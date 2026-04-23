#include "TomlStorage.h"

#include "SqlError.h"
#include "StorageFormatUtils.h"
#include "StringUtils.h"

#include <utility>

namespace fsql
{
    namespace
    {
        Table read_toml_table(std::istream& input, const std::string& table_name, bool include_rows)
        {
            Table table;
            table.name = table_name;

            std::string line;
            bool saw_columns = false;
            bool saw_rows = false;
            bool inside_rows = false;
            while (std::getline(input, line))
            {
                const auto trimmed = trim(line);
                if (trimmed.empty() || trimmed[0] == '#')
                {
                    continue;
                }
                if (inside_rows)
                {
                    if (trimmed == "]")
                    {
                        inside_rows = false;
                        saw_rows = true;
                        continue;
                    }
                    auto row_text = trimmed;
                    if (!row_text.empty() && row_text.back() == ',')
                    {
                        row_text.pop_back();
                    }
                    auto row = detail::parse_quoted_string_array(row_text, "Expected TOML row array in table: " + table_name);
                    if (!table.columns.empty() && row.size() != table.columns.size())
                    {
                        fail("Row column count mismatch in table: " + table_name);
                    }
                    if (include_rows)
                    {
                        table.rows.push_back(std::move(row));
                    }
                    continue;
                }
                if (trimmed.rfind("columns", 0) == 0)
                {
                    const auto equals = trimmed.find('=');
                    if (equals == std::string::npos)
                    {
                        fail("Expected '=' after columns in TOML table: " + table_name);
                    }
                    table.columns = detail::parse_quoted_string_array(trim(trimmed.substr(equals + 1)),
                                                                      "Expected TOML columns array in table: " + table_name);
                    saw_columns = true;
                    continue;
                }
                if (trimmed.rfind("rows", 0) == 0)
                {
                    const auto equals = trimmed.find('=');
                    if (equals == std::string::npos)
                    {
                        fail("Expected '=' after rows in TOML table: " + table_name);
                    }
                    const auto value = trim(trimmed.substr(equals + 1));
                    if (value == "[]")
                    {
                        saw_rows = true;
                        continue;
                    }
                    if (value != "[")
                    {
                        fail("Expected '[' after rows = in TOML table: " + table_name);
                    }
                    inside_rows = true;
                    continue;
                }
                fail("Unsupported TOML table content: " + table_name);
            }

            if (inside_rows)
            {
                fail("Unterminated rows array in TOML table: " + table_name);
            }
            if (!saw_columns || !saw_rows)
            {
                fail("TOML table must contain columns and rows: " + table_name);
            }
            return include_rows ? detail::validate_loaded_table(std::move(table)) : std::move(table);
        }
    }

    TomlStorage::TomlStorage()
        : storage_support_(std::filesystem::current_path(), ".toml")
    {
    }

    TomlStorage::TomlStorage(std::filesystem::path root_directory)
        : storage_support_(std::move(root_directory), ".toml")
    {
    }

    std::filesystem::path TomlStorage::table_path(const RelationReference& table_name) const
    {
        return storage_support_.table_path(table_name);
    }

    bool TomlStorage::has_table(const RelationReference& table_name) const
    {
        return storage_support_.has_table(table_name);
    }

    bool TomlStorage::has_view(const RelationReference& view_name) const
    {
        return storage_support_.has_view(view_name);
    }

    Table TomlStorage::load_table(const RelationReference& table_name) const
    {
        return storage_support_.load_table(table_name, load_table_from_stream);
    }

    Table TomlStorage::describe_table(const RelationReference& table_name) const
    {
        return storage_support_.describe_table(table_name, describe_table_from_stream);
    }

    RowGenerator TomlStorage::scan_table(const RelationReference& table_name) const
    {
        return storage_support_.scan_table(table_name, scan_table_from_file);
    }

    ViewDefinition TomlStorage::load_view(const RelationReference& view_name) const
    {
        return storage_support_.load_view(view_name);
    }

    void TomlStorage::save_table(const Table& table)
    {
        storage_support_.save_table(table, write_table_to_stream);
    }

    void TomlStorage::save_view(const ViewDefinition& view)
    {
        storage_support_.save_view(view);
    }

    void TomlStorage::delete_table(const RelationReference& table_name)
    {
        storage_support_.delete_table(table_name);
    }

    void TomlStorage::delete_view(const RelationReference& view_name)
    {
        storage_support_.delete_view(view_name);
    }

    std::size_t TomlStorage::column_index(const Table& table, const std::string& column) const
    {
        return storage_support_.column_index(table, column);
    }

    Table TomlStorage::load_table_from_stream(std::istream& input, const std::string& table_name)
    {
        return read_toml_table(input, table_name, true);
    }

    Table TomlStorage::describe_table_from_stream(std::istream& input, const std::string& table_name)
    {
        return read_toml_table(input, table_name, false);
    }

    RowGenerator TomlStorage::scan_table_from_file(std::ifstream input, std::string table_name)
    {
        std::string line;
        std::size_t column_count = 0;
        bool saw_columns = false;
        bool saw_rows = false;
        bool inside_rows = false;
        while (std::getline(input, line))
        {
            const auto trimmed = trim(line);
            if (trimmed.empty() || trimmed[0] == '#')
            {
                continue;
            }
            if (inside_rows)
            {
                if (trimmed == "]")
                {
                    inside_rows = false;
                    saw_rows = true;
                    continue;
                }
                auto row_text = trimmed;
                if (!row_text.empty() && row_text.back() == ',')
                {
                    row_text.pop_back();
                }
                auto row = detail::parse_quoted_string_array(row_text, "Expected TOML row array in table: " + table_name);
                if (saw_columns && row.size() != column_count)
                {
                    fail("Row column count mismatch in table: " + table_name);
                }
                co_yield row;
                continue;
            }
            if (trimmed.rfind("columns", 0) == 0)
            {
                const auto equals = trimmed.find('=');
                if (equals == std::string::npos)
                {
                    fail("Expected '=' after columns in TOML table: " + table_name);
                }
                column_count = detail::parse_quoted_string_array(trim(trimmed.substr(equals + 1)),
                                                                 "Expected TOML columns array in table: " + table_name).size();
                saw_columns = true;
                continue;
            }
            if (trimmed.rfind("rows", 0) == 0)
            {
                const auto equals = trimmed.find('=');
                if (equals == std::string::npos)
                {
                    fail("Expected '=' after rows in TOML table: " + table_name);
                }
                const auto value = trim(trimmed.substr(equals + 1));
                if (value == "[]")
                {
                    saw_rows = true;
                    continue;
                }
                if (value != "[")
                {
                    fail("Expected '[' after rows = in TOML table: " + table_name);
                }
                inside_rows = true;
                continue;
            }
            fail("Unsupported TOML table content: " + table_name);
        }

        if (inside_rows)
        {
            fail("Unterminated rows array in TOML table: " + table_name);
        }
        if (!saw_columns || !saw_rows)
        {
            fail("TOML table must contain columns and rows: " + table_name);
        }
    }

    void TomlStorage::write_table_to_stream(std::ostream& output, const Table& table)
    {
        output << "columns = [";
        for (std::size_t i = 0; i < table.columns.size(); ++i)
        {
            if (i > 0)
            {
                output << ", ";
            }
            output << '"' << detail::quoted_string_body(table.columns[i]) << '"';
        }
        output << "]\n";
        output << "rows = [\n";
        for (const auto& row : table.rows)
        {
            output << "  [";
            for (std::size_t i = 0; i < row.size(); ++i)
            {
                if (i > 0)
                {
                    output << ", ";
                }
                output << '"' << detail::quoted_string_body(row[i]) << '"';
            }
            output << "],\n";
        }
        output << "]\n";
    }
}
