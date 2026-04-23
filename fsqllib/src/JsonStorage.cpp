#include "JsonStorage.h"

#include "SqlError.h"
#include "StorageFormatUtils.h"
#include "StringUtils.h"

#include <utility>

namespace fsql
{
    namespace
    {
        std::string extract_array_text(const std::string& text, const std::string& message)
        {
            const auto start = text.find('[');
            const auto end = text.rfind(']');
            if (start == std::string::npos || end == std::string::npos || end < start)
            {
                fail(message);
            }
            return text.substr(start, end - start + 1);
        }

        Table read_json_table(std::istream& input, const std::string& table_name, bool include_rows)
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
                if (trimmed.empty() || trimmed == "{" || trimmed == "}" || trimmed == "},")
                {
                    continue;
                }
                if (!saw_columns && trimmed.find("\"columns\"") != std::string::npos)
                {
                    table.columns = detail::parse_quoted_string_array(extract_array_text(trimmed, "Expected JSON columns array in table: " + table_name),
                                                                      "Expected JSON columns array in table: " + table_name);
                    saw_columns = true;
                    continue;
                }
                if (trimmed.find("\"rows\"") != std::string::npos)
                {
                    saw_rows = true;
                    inside_rows = true;
                    continue;
                }
                if (!inside_rows)
                {
                    continue;
                }
                if (!trimmed.empty() && trimmed.front() == ']')
                {
                    inside_rows = false;
                    continue;
                }

                auto row = detail::parse_quoted_string_array(extract_array_text(trimmed, "Expected JSON row array in table: " + table_name),
                                                             "Expected JSON row array in table: " + table_name);
                if (!table.columns.empty() && row.size() != table.columns.size())
                {
                    fail("Row column count mismatch in table: " + table_name);
                }
                if (include_rows)
                {
                    table.rows.push_back(std::move(row));
                }
            }

            if (!saw_columns || !saw_rows)
            {
                fail("JSON table must contain columns and rows: " + table_name);
            }
            return include_rows ? detail::validate_loaded_table(std::move(table)) : std::move(table);
        }
    }

    JsonStorage::JsonStorage()
        : storage_support_(std::filesystem::current_path(), ".json")
    {
    }

    JsonStorage::JsonStorage(std::filesystem::path root_directory)
        : storage_support_(std::move(root_directory), ".json")
    {
    }

    std::filesystem::path JsonStorage::table_path(const RelationReference& table_name) const
    {
        return storage_support_.table_path(table_name);
    }

    bool JsonStorage::has_table(const RelationReference& table_name) const
    {
        return storage_support_.has_table(table_name);
    }

    bool JsonStorage::has_view(const RelationReference& view_name) const
    {
        return storage_support_.has_view(view_name);
    }

    Table JsonStorage::load_table(const RelationReference& table_name) const
    {
        return storage_support_.load_table(table_name, load_table_from_stream);
    }

    Table JsonStorage::describe_table(const RelationReference& table_name) const
    {
        return storage_support_.describe_table(table_name, describe_table_from_stream);
    }

    RowGenerator JsonStorage::scan_table(const RelationReference& table_name) const
    {
        return storage_support_.scan_table(table_name, scan_table_from_file);
    }

    ViewDefinition JsonStorage::load_view(const RelationReference& view_name) const
    {
        return storage_support_.load_view(view_name);
    }

    void JsonStorage::save_table(const Table& table)
    {
        storage_support_.save_table(table, write_table_to_stream);
    }

    void JsonStorage::save_view(const ViewDefinition& view)
    {
        storage_support_.save_view(view);
    }

    void JsonStorage::delete_table(const RelationReference& table_name)
    {
        storage_support_.delete_table(table_name);
    }

    void JsonStorage::delete_view(const RelationReference& view_name)
    {
        storage_support_.delete_view(view_name);
    }

    std::size_t JsonStorage::column_index(const Table& table, const std::string& column) const
    {
        return storage_support_.column_index(table, column);
    }

    Table JsonStorage::load_table_from_stream(std::istream& input, const std::string& table_name)
    {
        return read_json_table(input, table_name, true);
    }

    Table JsonStorage::describe_table_from_stream(std::istream& input, const std::string& table_name)
    {
        return read_json_table(input, table_name, false);
    }

    RowGenerator JsonStorage::scan_table_from_file(std::ifstream input, std::string table_name)
    {
        std::string line;
        std::size_t column_count = 0;
        bool saw_columns = false;
        bool saw_rows = false;
        bool inside_rows = false;
        while (std::getline(input, line))
        {
            const auto trimmed = trim(line);
            if (trimmed.empty() || trimmed == "{" || trimmed == "}" || trimmed == "},")
            {
                continue;
            }
            if (!saw_columns && trimmed.find("\"columns\"") != std::string::npos)
            {
                column_count = detail::parse_quoted_string_array(extract_array_text(trimmed, "Expected JSON columns array in table: " + table_name),
                                                                 "Expected JSON columns array in table: " + table_name).size();
                saw_columns = true;
                continue;
            }
            if (trimmed.find("\"rows\"") != std::string::npos)
            {
                saw_rows = true;
                inside_rows = true;
                continue;
            }
            if (!inside_rows)
            {
                continue;
            }
            if (!trimmed.empty() && trimmed.front() == ']')
            {
                inside_rows = false;
                continue;
            }

            auto row = detail::parse_quoted_string_array(extract_array_text(trimmed, "Expected JSON row array in table: " + table_name),
                                                         "Expected JSON row array in table: " + table_name);
            if (saw_columns && row.size() != column_count)
            {
                fail("Row column count mismatch in table: " + table_name);
            }
            co_yield row;
        }

        if (!saw_columns || !saw_rows)
        {
            fail("JSON table must contain columns and rows: " + table_name);
        }
    }

    void JsonStorage::write_table_to_stream(std::ostream& output, const Table& table)
    {
        output << "{\n";
        output << "  \"columns\": [";
        for (std::size_t i = 0; i < table.columns.size(); ++i)
        {
            if (i > 0)
            {
                output << ", ";
            }
            output << '"' << detail::quoted_string_body(table.columns[i]) << '"';
        }
        output << "],\n";
        output << "  \"rows\": [";
        if (!table.rows.empty())
        {
            output << '\n';
            for (std::size_t i = 0; i < table.rows.size(); ++i)
            {
                output << "    [";
                for (std::size_t j = 0; j < table.rows[i].size(); ++j)
                {
                    if (j > 0)
                    {
                        output << ", ";
                    }
                    output << '"' << detail::quoted_string_body(table.rows[i][j]) << '"';
                }
                output << ']';
                if (i + 1 < table.rows.size())
                {
                    output << ',';
                }
                output << '\n';
            }
            output << "  ";
        }
        output << "]\n}";
    }
}
