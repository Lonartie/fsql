#include "FileStorageBase.h"

#include "ColumnMetadata.h"
#include "SqlError.h"
#include "StringUtils.h"

#include <algorithm>
#include <fstream>
#include <optional>
#include <sstream>
#include <utility>

namespace fsql
{
    namespace
    {
        constexpr std::string_view view_suffix = ".view.sql";

        RowGenerator scan_materialized_rows(std::vector<Row> rows)
        {
            for (const auto& row : rows)
            {
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

    FileStorageBase::FileStorageBase(std::filesystem::path root_directory,
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

    std::filesystem::path FileStorageBase::table_path(const RelationReference& table_name) const
    {
        return resolve_table_path(table_name, false);
    }

    bool FileStorageBase::has_table(const RelationReference& table_name) const
    {
        if (table_name.kind == RelationReference::Kind::FilePath)
        {
            if (const auto resolved_path = try_resolve_existing_table_path(rooted_path(table_name), false); resolved_path.has_value())
            {
                return true;
            }
            return false;
        }

        const auto candidates = existing_identifier_candidates(root_directory_ / table_name.name);
        if (candidates.size() > 1)
        {
            fail("Ambiguous table reference '" + table_name.name + "': " + describe_ambiguous_candidates(candidates));
        }
        return !candidates.empty();
    }

    bool FileStorageBase::has_view(const RelationReference& view_name) const
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

    Table FileStorageBase::load_table(const RelationReference& table_name) const
    {
        const auto path = resolve_table_path(table_name, false);
        auto table = load_table_from_file(path, table_name.name);
        table.storage_path = path;
        return table;
    }

    Table FileStorageBase::describe_table(const RelationReference& table_name) const
    {
        auto table = load_table(table_name);
        table.rows.clear();
        return table;
    }

    RowGenerator FileStorageBase::scan_table(const RelationReference& table_name) const
    {
        const auto path = resolve_table_path(table_name, false);
        return scan_table_from_file(path, table_name.name);
    }

    ViewDefinition FileStorageBase::load_view(const RelationReference& view_name) const
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

    void FileStorageBase::save_table(const Table& table)
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

        output << serialize_table_text(table);
    }

    void FileStorageBase::save_view(const ViewDefinition& view)
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

    void FileStorageBase::delete_table(const RelationReference& table_name)
    {
        const auto path = resolve_table_path(table_name, false);
        if (!std::filesystem::remove(path))
        {
            fail("Unable to delete table: " + table_name.name);
        }
    }

    void FileStorageBase::delete_view(const RelationReference& view_name)
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

    std::size_t FileStorageBase::column_index(const Table& table, const std::string& column) const
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

    const std::string& FileStorageBase::default_extension() const
    {
        return default_extension_;
    }

    const std::filesystem::path& FileStorageBase::root_directory() const
    {
        return root_directory_;
    }

    bool FileStorageBase::supports_extension(const std::filesystem::path& path) const
    {
        const auto extension = path.extension().string();
        return std::any_of(supported_extensions_.begin(), supported_extensions_.end(), [&](const std::string& candidate)
        {
            return iequals(candidate, extension);
        });
    }

    RowGenerator FileStorageBase::scan_table_from_file(std::filesystem::path path, std::string table_name) const
    {
        const auto table = load_table_from_file(path, table_name);
        return scan_materialized_rows(table.rows);
    }

    std::filesystem::path FileStorageBase::rooted_path(const RelationReference& reference) const
    {
        auto path = std::filesystem::path(reference.name);
        if (path.is_absolute())
        {
            return path;
        }
        return root_directory_ / path;
    }

    std::optional<std::filesystem::path> FileStorageBase::try_resolve_existing_table_path(std::filesystem::path path,
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

    std::filesystem::path FileStorageBase::resolve_table_path(const RelationReference& table_name, bool for_write) const
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

    std::filesystem::path FileStorageBase::resolve_view_path(const RelationReference& view_name, bool for_write) const
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

    std::filesystem::path FileStorageBase::resolve_existing_identifier_table_path(const std::string& table_name) const
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

    bool FileStorageBase::is_view_path(const std::filesystem::path& path)
    {
        const auto filename = path.filename().string();
        return filename.size() >= view_suffix.size() && filename.substr(filename.size() - view_suffix.size()) == view_suffix;
    }

    std::filesystem::path FileStorageBase::resolve_view_source_path(std::filesystem::path path)
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

    std::string FileStorageBase::view_logical_name_from_path(const std::filesystem::path& path)
    {
        const auto filename = path.filename().string();
        if (filename.size() >= view_suffix.size() && filename.substr(filename.size() - view_suffix.size()) == view_suffix)
        {
            return filename.substr(0, filename.size() - view_suffix.size());
        }
        return path.stem().string();
    }

    std::vector<std::filesystem::path> FileStorageBase::existing_identifier_candidates(const std::filesystem::path& base_path) const
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

    Table FileStorageBase::load_table_from_file(const std::filesystem::path& path, const std::string& table_name) const
    {
        std::ifstream input(path, std::ios::binary);
        if (!input)
        {
            fail("Table does not exist: " + table_name);
        }

        std::ostringstream buffer;
        buffer << input.rdbuf();
        return parse_table_text(buffer.str(), table_name);
    }
}

