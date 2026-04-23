#include "CsvStorage.h"

#include "SqlError.h"

#include <utility>

namespace fsql
{
    namespace
    {
        constexpr std::string_view csv_extension = ".csv";

        RowGenerator scan_with_owned_storage(std::shared_ptr<IStorage> storage, RelationReference reference)
        {
            auto rows = storage->scan_table(reference);
            for (const auto& row : rows)
            {
                co_yield row;
            }
        }

        std::string table_logical_name_from_path(const std::filesystem::path& path)
        {
            return path.stem().string();
        }
    }

    CsvStorage::CsvStorage()
        : storage_support_(std::filesystem::current_path(), ".csv")
    {
    }

    CsvStorage::CsvStorage(std::filesystem::path root_directory)
        : storage_support_(std::move(root_directory), ".csv")
    {
    }

    std::filesystem::path CsvStorage::table_path(const RelationReference& table_name) const
    {
        return storage_support_.table_path(table_name);
    }

    bool CsvStorage::has_table(const RelationReference& table_name) const
    {
        return storage_support_.has_table(table_name);
    }

    bool CsvStorage::has_view(const RelationReference& view_name) const
    {
        return storage_support_.has_view(view_name);
    }

    Table CsvStorage::load_table(const RelationReference& table_name) const
    {
        return storage_support_.load_table(table_name, load_table_from_stream);
    }

    Table CsvStorage::describe_table(const RelationReference& table_name) const
    {
        return storage_support_.describe_table(table_name, describe_table_from_stream);
    }

    RowGenerator CsvStorage::scan_table(const RelationReference& table_name) const
    {
        return storage_support_.scan_table(table_name, scan_table_from_file);
    }

    ViewDefinition CsvStorage::load_view(const RelationReference& view_name) const
    {
        return storage_support_.load_view(view_name);
    }

    void CsvStorage::save_table(const Table& table)
    {
        storage_support_.save_table(table, write_table_to_stream);
    }

    void CsvStorage::save_view(const ViewDefinition& view)
    {
        storage_support_.save_view(view);
    }

    void CsvStorage::delete_table(const RelationReference& table_name)
    {
        storage_support_.delete_table(table_name);
    }

    void CsvStorage::delete_view(const RelationReference& view_name)
    {
        storage_support_.delete_view(view_name);
    }

    std::size_t CsvStorage::column_index(const Table& table, const std::string& column) const
    {
        return storage_support_.column_index(table, column);
    }

    std::filesystem::path CsvStorage::resolve_table_source_path(std::filesystem::path path)
    {
        if (path.extension().empty())
        {
            const auto with_extension = std::filesystem::path(path.string() + std::string(csv_extension));
            if (std::filesystem::exists(with_extension))
            {
                return with_extension;
            }
        }
        else if (path.extension() != csv_extension)
        {
            fail("Unsupported table format: " + path.string());
        }

        if (!std::filesystem::exists(path))
        {
            fail("Table file does not exist: " + path.string());
        }
        return path;
    }

    std::filesystem::path CsvStorage::resolve_view_source_path(std::filesystem::path path)
    {
        return FileStorageSupport::resolve_view_source_path(std::move(path));
    }

    Table CsvStorage::load_table_from_path(std::filesystem::path path)
    {
        const auto resolved_path = resolve_table_source_path(std::move(path));
        auto storage = std::make_shared<CsvStorage>(std::filesystem::current_path());
        auto table = storage->load_table({RelationReference::Kind::FilePath, resolved_path.string()});
        table.name = table_logical_name_from_path(resolved_path);
        table.storage_path = resolved_path;
        return table;
    }

    Table CsvStorage::describe_table_from_path(std::filesystem::path path)
    {
        const auto resolved_path = resolve_table_source_path(std::move(path));
        auto storage = std::make_shared<CsvStorage>(std::filesystem::current_path());
        auto table = storage->describe_table({RelationReference::Kind::FilePath, resolved_path.string()});
        table.name = table_logical_name_from_path(resolved_path);
        table.storage_path = resolved_path;
        return table;
    }

    RowGenerator CsvStorage::scan_table_from_path(std::filesystem::path path)
    {
        const auto resolved_path = resolve_table_source_path(std::move(path));
        auto storage = std::make_shared<CsvStorage>(std::filesystem::current_path());
        return scan_with_owned_storage(storage, {RelationReference::Kind::FilePath, resolved_path.string()});
    }

    ViewDefinition CsvStorage::load_view_from_path(std::filesystem::path path)
    {
        const auto resolved_path = resolve_view_source_path(std::move(path));
        auto storage = std::make_shared<CsvStorage>(std::filesystem::current_path());
        auto view = storage->load_view({RelationReference::Kind::FilePath, resolved_path.string()});
        view.name = FileStorageSupport::view_logical_name_from_path(resolved_path);
        view.storage_path = resolved_path;
        return view;
    }

    Table CsvStorage::describe_table_from_stream(std::istream& input, const std::string& table_name)
    {
        Table table;
        table.name = table_name;

        std::string line;
        if (!std::getline(input, line))
        {
            fail("Table is empty or invalid: " + table_name);
        }

        table.columns = parse_csv_line(line);
        if (table.columns.empty())
        {
            fail("Table has no columns: " + table_name);
        }
        return table;
    }

    Table CsvStorage::load_table_from_stream(std::istream& input, const std::string& table_name)
    {
        auto table = describe_table_from_stream(input, table_name);
        std::string line;
        while (std::getline(input, line))
        {
            auto row = parse_csv_line(line);
            if (row.size() != table.columns.size())
            {
                fail("Row column count mismatch in table: " + table_name);
            }
            table.rows.push_back(std::move(row));
        }
        return table;
    }

    RowGenerator CsvStorage::scan_table_from_file(std::ifstream input, std::string table_name)
    {
        const auto table = describe_table_from_stream(input, table_name);
        std::string line;
        while (std::getline(input, line))
        {
            auto row = parse_csv_line(line);
            if (row.size() != table.columns.size())
            {
                fail("Row column count mismatch in table: " + table_name);
            }
            co_yield row;
        }
    }

    void CsvStorage::write_table_to_stream(std::ostream& output, const Table& table)
    {
        for (std::size_t i = 0; i < table.columns.size(); ++i)
        {
            if (i > 0)
            {
                output << ',';
            }
            output << escape_csv(table.columns[i]);
        }
        output << '\n';

        for (const auto& row : table.rows)
        {
            for (std::size_t i = 0; i < row.size(); ++i)
            {
                if (i > 0)
                {
                    output << ',';
                }
                output << escape_csv(row[i]);
            }
            output << '\n';
        }
    }

    std::string CsvStorage::escape_csv(const std::string& value)
    {
        const bool needs_quotes = value.find_first_of(",\"\n\r") != std::string::npos;
        if (!needs_quotes)
        {
            return value;
        }

        std::string escaped = "\"";
        for (char ch : value)
        {
            if (ch == '"')
            {
                escaped += "\"\"";
            }
            else
            {
                escaped += ch;
            }
        }
        escaped += '"';
        return escaped;
    }

    std::vector<std::string> CsvStorage::parse_csv_line(const std::string& line)
    {
        std::vector<std::string> fields;
        std::string current;
        bool in_quotes = false;

        for (std::size_t i = 0; i < line.size(); ++i)
        {
            const char ch = line[i];
            if (in_quotes)
            {
                if (ch == '"')
                {
                    if (i + 1 < line.size() && line[i + 1] == '"')
                    {
                        current += '"';
                        ++i;
                    }
                    else
                    {
                        in_quotes = false;
                    }
                }
                else
                {
                    current += ch;
                }
            }
            else if (ch == ',')
            {
                fields.push_back(current);
                current.clear();
            }
            else if (ch == '"')
            {
                in_quotes = true;
            }
            else
            {
                current += ch;
            }
        }

        if (in_quotes)
        {
            fail("Unterminated quoted CSV field");
        }

        fields.push_back(current);
        return fields;
    }
}
