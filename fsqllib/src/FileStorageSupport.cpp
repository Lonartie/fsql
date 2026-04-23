#include "FileStorageSupport.h"

#include "ColumnMetadata.h"
#include "SqlError.h"
#include "StorageFormatUtils.h"
#include "StringUtils.h"

#include <algorithm>
#include <sstream>
#include <utility>

namespace fsql
{
    namespace
    {
        constexpr std::string_view view_suffix = ".view.sql";

        RowGenerator scan_rows(std::vector<Row> rows)
        {
            for (const auto& row : rows)
            {
                co_yield row;
            }
        }

        RowGenerator scan_with_append_journal(RowGenerator base_rows,
                                             std::filesystem::path journal_path,
                                             std::size_t column_count,
                                             std::string table_name)
        {
            for (const auto& row : base_rows)
            {
                co_yield row;
            }

            std::ifstream input(journal_path);
            if (!input)
            {
                co_return;
            }

            std::string line;
            while (std::getline(input, line))
            {
                auto row = detail::parse_quoted_string_array(line, "Malformed appended row journal for table: " + table_name);
                if (row.size() != column_count)
                {
                    fail("Row column count mismatch in table: " + table_name);
                }
                co_yield row;
            }
        }

        std::string describe_ambiguous_candidates(const std::vector<std::filesystem::path>& candidates)
        {
            std::ostringstream output;
            for (std::size_t i = 0; i < candidates.size(); ++i)
            {
                if (i > 0)
                {
                    output << ", ";
                }
                output << candidates[i].string();
            }
            return output.str();
        }
    }

    FileStorageSupport::FileStorageSupport(std::filesystem::path root_directory,
                                           std::string default_extension,
                                           std::vector<std::string> supported_extensions)
        : root_directory_(std::move(root_directory)),
          default_extension_(std::move(default_extension)),
          supported_extensions_(std::move(supported_extensions))
    {
        if (supported_extensions_.empty())
        {
            supported_extensions_.push_back(default_extension_);
        }
    }

    std::filesystem::path FileStorageSupport::table_path(const RelationReference& table_name) const
    {
        return resolve_table_path(table_name, false);
    }

    bool FileStorageSupport::has_table(const RelationReference& table_name) const
    {
        if (table_name.kind == RelationReference::Kind::FilePath)
        {
            return try_resolve_existing_table_path(rooted_path(table_name), false).has_value();
        }

        const auto candidates = existing_identifier_candidates(root_directory_ / table_name.name);
        if (candidates.size() > 1)
        {
            fail("Ambiguous table reference '" + table_name.name + "': " + describe_ambiguous_candidates(candidates));
        }
        return !candidates.empty();
    }

    bool FileStorageSupport::has_view(const RelationReference& view_name) const
    {
        if (view_name.kind == RelationReference::Kind::FilePath)
        {
            const auto raw_path = rooted_path(view_name);
            if (supports_extension(raw_path) && !is_view_path(raw_path))
            {
                return false;
            }
        }
        return std::filesystem::exists(resolve_view_path(view_name, false));
    }

    Table FileStorageSupport::load_table(const RelationReference& table_name, const TableReader& reader) const
    {
        const auto path = resolve_table_path(table_name, false);
        std::ifstream input(path, std::ios::binary);
        if (!input)
        {
            fail("Table does not exist: " + table_name.name);
        }

        auto table = reader(input, table_name.name);
        table.storage_path = path;
        append_journal_rows(table, path);
        return detail::validate_loaded_table(std::move(table));
    }

    Table FileStorageSupport::describe_table(const RelationReference& table_name, const TableReader& reader) const
    {
        const auto path = resolve_table_path(table_name, false);
        std::ifstream input(path, std::ios::binary);
        if (!input)
        {
            fail("Table does not exist: " + table_name.name);
        }

        auto table = reader(input, table_name.name);
        table.storage_path = path;
        table.rows.clear();
        return table;
    }

    RowGenerator FileStorageSupport::scan_table(const RelationReference& table_name,
                                                const TableReader& reader,
                                                const TableScanner& scanner) const
    {
        const auto path = resolve_table_path(table_name, false);
        std::ifstream metadata_input(path, std::ios::binary);
        if (!metadata_input)
        {
            fail("Table does not exist: " + table_name.name);
        }
        const auto table = reader(metadata_input, table_name.name);

        std::ifstream input(path, std::ios::binary);
        if (!input)
        {
            fail("Table does not exist: " + table_name.name);
        }
        return scan_with_append_journal(scanner(std::move(input), table_name.name),
                                        append_journal_path(path),
                                        table.columns.size(),
                                        table_name.name);
    }

    ViewDefinition FileStorageSupport::load_view(const RelationReference& view_name) const
    {
        const auto path = resolve_view_path(view_name, false);
        std::ifstream input(path);
        if (!input)
        {
            fail("View does not exist: " + view_name.name);
        }

        std::ostringstream buffer;
        buffer << input.rdbuf();
        const auto query = buffer.str();
        if (query.empty())
        {
            fail("View is empty or invalid: " + view_name.name);
        }

        ViewDefinition view;
        view.name = view_name.name;
        view.select_statement = query;
        if (view_name.kind == RelationReference::Kind::FilePath)
        {
            view.storage_path = path;
        }
        return view;
    }

    void FileStorageSupport::save_table(const Table& table, const TableWriter& writer) const
    {
        const bool uses_explicit_path = table.storage_path.has_value();
        if (!uses_explicit_path)
        {
            const RelationReference reference{RelationReference::Kind::Identifier, table.name};
            if (has_view(reference))
            {
                fail("View already exists: " + table.name);
            }
        }

        const auto path = uses_explicit_path
            ? resolve_table_path({RelationReference::Kind::FilePath, table.storage_path->string()}, true)
            : std::filesystem::path(root_directory_ / (table.name + default_extension_));
        std::ofstream output(path, std::ios::trunc | std::ios::binary);
        if (!output)
        {
            fail("Unable to write table: " + table.name);
        }
        writer(output, table);

        std::error_code error;
        std::filesystem::remove(append_journal_path(path), error);
        rewrite_auto_increment_state(path, table);
    }

    bool FileStorageSupport::supports_append(const RelationReference& table_name) const
    {
        return has_table(table_name);
    }

    void FileStorageSupport::append_row(const RelationReference& table_name, const Table& table, const Row& row) const
    {
        if (row.size() != table.columns.size())
        {
            fail("INSERT value count does not match table column count");
        }

        const auto path = resolve_table_path(table_name, false);
        std::ofstream output(append_journal_path(path), std::ios::app | std::ios::binary);
        if (!output)
        {
            fail("Unable to append row to table: " + table.name);
        }
        output << detail::serialize_quoted_string_array(row) << '\n';
        advance_auto_increment_state(path, table, row);
    }

    std::optional<std::string> FileStorageSupport::read_next_auto_increment_value(const RelationReference& table_name) const
    {
        const auto path = resolve_table_path(table_name, false);
        std::ifstream input(auto_increment_state_path(path));
        if (!input)
        {
            return std::nullopt;
        }

        std::string value;
        std::getline(input, value);
        if (value.empty())
        {
            return std::nullopt;
        }
        return value;
    }

    void FileStorageSupport::save_view(const ViewDefinition& view) const
    {
        const bool uses_explicit_path = view.storage_path.has_value();
        if (uses_explicit_path)
        {
            const auto raw_path = view.storage_path->is_absolute() ? *view.storage_path : root_directory_ / *view.storage_path;
            if (supports_extension(raw_path) && !is_view_path(raw_path))
            {
                fail("Unsupported view path: " + view.name);
            }
        }
        if (!uses_explicit_path)
        {
            const RelationReference reference{RelationReference::Kind::Identifier, view.name};
            if (has_table(reference))
            {
                fail("Table already exists: " + view.name);
            }
        }

        const auto path = uses_explicit_path
            ? resolve_view_path({RelationReference::Kind::FilePath, view.storage_path->string()}, true)
            : resolve_view_path({RelationReference::Kind::Identifier, view.name}, true);
        std::ofstream output(path, std::ios::trunc);
        if (!output)
        {
            fail("Unable to write view: " + view.name);
        }

        output << view.select_statement;
    }

    void FileStorageSupport::delete_table(const RelationReference& table_name) const
    {
        const auto path = resolve_table_path(table_name, false);
        if (!std::filesystem::remove(path))
        {
            fail("Unable to delete table: " + table_name.name);
        }

        std::error_code error;
        std::filesystem::remove(append_journal_path(path), error);
        std::filesystem::remove(auto_increment_state_path(path), error);
    }

    void FileStorageSupport::delete_view(const RelationReference& view_name) const
    {
        if (view_name.kind == RelationReference::Kind::FilePath)
        {
            const auto raw_path = rooted_path(view_name);
            if (supports_extension(raw_path) && !is_view_path(raw_path))
            {
                fail("View does not exist: " + view_name.name);
            }
        }

        const auto path = resolve_view_path(view_name, false);
        if (!std::filesystem::remove(path))
        {
            fail("Unable to delete view: " + view_name.name);
        }
    }

    std::size_t FileStorageSupport::column_index(const Table& table, const std::string& column) const
    {
        for (std::size_t i = 0; i < table.columns.size(); ++i)
        {
            if (iequals(visible_column_name(table.columns[i]), column))
            {
                return i;
            }
        }
        fail("Unknown column '" + column + "' in table '" + table.name + "'");
    }

    const std::filesystem::path& FileStorageSupport::root_directory() const
    {
        return root_directory_;
    }

    const std::string& FileStorageSupport::default_extension() const
    {
        return default_extension_;
    }

    bool FileStorageSupport::supports_extension(const std::filesystem::path& path) const
    {
        const auto extension = path.extension().string();
        return std::any_of(supported_extensions_.begin(), supported_extensions_.end(), [&](const std::string& candidate)
        {
            return iequals(candidate, extension);
        });
    }

    bool FileStorageSupport::is_view_path(const std::filesystem::path& path)
    {
        const auto filename = path.filename().string();
        return filename.size() >= view_suffix.size() && filename.substr(filename.size() - view_suffix.size()) == view_suffix;
    }

    std::filesystem::path FileStorageSupport::resolve_view_source_path(std::filesystem::path path)
    {
        if (path.extension().empty())
        {
            const auto with_view_extension = path.string() + std::string(view_suffix);
            if (!std::filesystem::exists(path) && std::filesystem::exists(with_view_extension))
            {
                path = with_view_extension;
            }
        }
        return path;
    }

    std::string FileStorageSupport::view_logical_name_from_path(const std::filesystem::path& path)
    {
        const auto filename = path.filename().string();
        if (filename.size() >= view_suffix.size() && filename.substr(filename.size() - view_suffix.size()) == view_suffix)
        {
            return filename.substr(0, filename.size() - view_suffix.size());
        }
        return path.stem().string();
    }

    std::filesystem::path FileStorageSupport::rooted_path(const RelationReference& reference) const
    {
        auto path = std::filesystem::path(reference.name);
        if (path.is_absolute())
        {
            return path;
        }
        return root_directory_ / path;
    }

    std::optional<std::filesystem::path> FileStorageSupport::try_resolve_existing_table_path(std::filesystem::path path,
                                                                                              bool throw_on_unsupported_extension) const
    {
        if (is_view_path(path))
        {
            return std::nullopt;
        }
        if (path.extension().empty())
        {
            const auto candidates = existing_identifier_candidates(path);
            if (candidates.size() > 1)
            {
                fail("Ambiguous table reference '" + path.string() + "': " + describe_ambiguous_candidates(candidates));
            }
            if (candidates.empty())
            {
                return std::nullopt;
            }
            return candidates.front();
        }
        if (!supports_extension(path))
        {
            if (throw_on_unsupported_extension)
            {
                fail("Unsupported table format: " + path.string());
            }
            return std::nullopt;
        }
        if (!std::filesystem::exists(path))
        {
            return std::nullopt;
        }
        return path;
    }

    std::filesystem::path FileStorageSupport::resolve_table_path(const RelationReference& table_name, bool for_write) const
    {
        if (table_name.kind == RelationReference::Kind::FilePath)
        {
            auto path = rooted_path(table_name);
            if (for_write)
            {
                if (is_view_path(path))
                {
                    fail("Unsupported table format: " + table_name.name);
                }
                if (path.extension().empty())
                {
                    path += default_extension_;
                }
                else if (!supports_extension(path))
                {
                    fail("Unsupported table format: " + table_name.name);
                }
                return path;
            }

            if (const auto resolved_path = try_resolve_existing_table_path(std::move(path), true); resolved_path.has_value())
            {
                return *resolved_path;
            }
            fail("Table does not exist: " + table_name.name);
        }

        if (for_write)
        {
            return root_directory_ / (table_name.name + default_extension_);
        }
        return resolve_existing_identifier_table_path(table_name.name);
    }

    std::filesystem::path FileStorageSupport::resolve_view_path(const RelationReference& view_name, bool for_write) const
    {
        if (view_name.kind == RelationReference::Kind::FilePath)
        {
            auto path = rooted_path(view_name);
            if (for_write && path.extension().empty() && !std::filesystem::exists(path))
            {
                path += view_suffix;
            }
            return for_write ? path : resolve_view_source_path(std::move(path));
        }
        return root_directory_ / (view_name.name + std::string(view_suffix));
    }

    std::filesystem::path FileStorageSupport::resolve_existing_identifier_table_path(const std::string& table_name) const
    {
        const auto candidates = existing_identifier_candidates(root_directory_ / table_name);
        if (candidates.empty())
        {
            fail("Table does not exist: " + table_name);
        }
        if (candidates.size() > 1)
        {
            fail("Ambiguous table reference '" + table_name + "': " + describe_ambiguous_candidates(candidates));
        }
        return candidates.front();
    }

    std::vector<std::filesystem::path> FileStorageSupport::existing_identifier_candidates(const std::filesystem::path& base_path) const
    {
        std::vector<std::filesystem::path> candidates;
        for (const auto& extension : supported_extensions_)
        {
            const auto candidate = std::filesystem::path(base_path.string() + extension);
            if (std::filesystem::exists(candidate))
            {
                candidates.push_back(candidate);
            }
        }
        return candidates;
    }

    std::filesystem::path FileStorageSupport::append_journal_path(const std::filesystem::path& table_path) const
    {
        return std::filesystem::path(table_path.string() + ".fsql.append");
    }

    std::filesystem::path FileStorageSupport::auto_increment_state_path(const std::filesystem::path& table_path) const
    {
        return std::filesystem::path(table_path.string() + ".fsql.autoincrement");
    }

    void FileStorageSupport::append_journal_rows(Table& table, const std::filesystem::path& table_path) const
    {
        std::ifstream input(append_journal_path(table_path));
        if (!input)
        {
            return;
        }

        std::string line;
        while (std::getline(input, line))
        {
            auto row = detail::parse_quoted_string_array(line, "Malformed appended row journal for table: " + table.name);
            if (row.size() != table.columns.size())
            {
                fail("Row column count mismatch in table: " + table.name);
            }
            table.rows.push_back(std::move(row));
        }
    }

    void FileStorageSupport::rewrite_auto_increment_state(const std::filesystem::path& table_path, const Table& table) const
    {
        std::error_code error;
        if (const auto index = auto_increment_column_index(table); index.has_value())
        {
            std::ofstream output(auto_increment_state_path(table_path), std::ios::trunc);
            if (!output)
            {
                fail("Unable to write AUTO_INCREMENT state for table: " + table.name);
            }
            output << next_auto_increment_value(table, *index);
            return;
        }
        std::filesystem::remove(auto_increment_state_path(table_path), error);
    }

    void FileStorageSupport::advance_auto_increment_state(const std::filesystem::path& table_path,
                                                          const Table& table,
                                                          const Row& row) const
    {
        if (const auto index = auto_increment_column_index(table); index.has_value())
        {
            if (row.size() <= *index || row[*index].empty())
            {
                return;
            }
            try
            {
                std::ofstream output(auto_increment_state_path(table_path), std::ios::trunc);
                if (!output)
                {
                    fail("Unable to write AUTO_INCREMENT state for table: " + table.name);
                }
                output << (std::stoll(row[*index]) + 1);
                return;
            }
            catch (const std::exception&)
            {
                fail("AUTO_INCREMENT column requires numeric existing values");
            }
        }
    }
}

