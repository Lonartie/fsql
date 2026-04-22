#include "CsvStorage.h"

#include "SqlError.h"
#include "StringUtils.h"

#include <fstream>
#include <utility>

namespace sql
{
    namespace
    {
        std::string visible_column_name(const std::string& stored_name)
        {
            std::string visible_name = stored_name;

            constexpr std::string_view default_marker = " DEFAULT(";
            const auto default_position = visible_name.find(default_marker);
            if (default_position != std::string::npos)
            {
                visible_name = visible_name.substr(0, default_position);
            }

            constexpr std::string_view auto_increment_marker = " AUTO_INCREMENT";
            const auto auto_increment_position = visible_name.find(auto_increment_marker);
            if (auto_increment_position != std::string::npos)
            {
                visible_name = visible_name.substr(0, auto_increment_position);
            }

            return visible_name;
        }
    }

    CsvStorage::CsvStorage() : root_directory_(std::filesystem::current_path())
    {
    }

    CsvStorage::CsvStorage(std::filesystem::path root_directory) : root_directory_(std::move(root_directory))
    {
    }

    std::filesystem::path CsvStorage::table_path(const std::string& table_name) const
    {
        return root_directory_ / (table_name + ".csv");
    }

    Table CsvStorage::load_table(const std::string& table_name) const
    {
        const auto path = table_path(table_name);
        std::ifstream input(path);
        if (!input)
        {
            fail("Table does not exist: " + table_name);
        }

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

    void CsvStorage::save_table(const Table& table)
    {
        const auto path = table_path(table.name);
        std::ofstream output(path, std::ios::trunc);
        if (!output)
        {
            fail("Unable to write table: " + table.name);
        }

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

    void CsvStorage::delete_table(const std::string& table_name)
    {
        const auto path = table_path(table_name);
        if (!std::filesystem::exists(path))
        {
            fail("Table does not exist: " + table_name);
        }

        if (!std::filesystem::remove(path))
        {
            fail("Unable to delete table: " + table_name);
        }
    }

    std::size_t CsvStorage::column_index(const Table& table, const std::string& column) const
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
            if (ch == '\"')
            {
                escaped += "\"\"";
            }
            else
            {
                escaped += ch;
            }
        }
        escaped += '\"';
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
                if (ch == '\"')
                {
                    if (i + 1 < line.size() && line[i + 1] == '\"')
                    {
                        current += '\"';
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
            else if (ch == '\"')
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
