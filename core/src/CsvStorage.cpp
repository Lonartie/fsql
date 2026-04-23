#include "CsvStorage.h"

#include "ColumnMetadata.h"
#include "SqlError.h"
#include "StringUtils.h"

#include <fstream>
#include <sstream>
#include <utility>

namespace sql
{
    namespace
    {

        Table describe_table_from_stream(std::istream& input, const std::string& table_name)
        {
            Table table;
            table.name = table_name;

            std::string line;
            if (!std::getline(input, line))
            {
                fail("Table is empty or invalid: " + table_name);
            }

            table.columns = CsvStorage::parse_csv_line(line);
            if (table.columns.empty())
            {
                fail("Table has no columns: " + table_name);
            }

            return table;
        }

        Table load_table_from_stream(std::istream& input, const std::string& table_name)
        {
            Table table = describe_table_from_stream(input, table_name);

            std::string line;
            while (std::getline(input, line))
            {
                auto row = CsvStorage::parse_csv_line(line);
                if (row.size() != table.columns.size())
                {
                    fail("Row column count mismatch in table: " + table_name);
                }
                table.rows.push_back(std::move(row));
            }

            return table;
        }

        RowGenerator scan_table_from_stream(std::filesystem::path path,
                                            std::string table_name,
                                            std::string missing_message)
        {
            std::ifstream input(path);
            if (!input)
            {
                fail(std::move(missing_message));
            }

            const auto table = describe_table_from_stream(input, table_name);

            std::string line;
            while (std::getline(input, line))
            {
                auto row = CsvStorage::parse_csv_line(line);
                if (row.size() != table.columns.size())
                {
                    fail("Row column count mismatch in table: " + table_name);
                }
                co_yield row;
            }
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

    std::filesystem::path CsvStorage::view_path(const std::string& view_name) const
    {
        return root_directory_ / (view_name + ".view.sql");
    }

    bool CsvStorage::has_table(const std::string& table_name) const
    {
        return std::filesystem::exists(table_path(table_name));
    }

    bool CsvStorage::has_view(const std::string& view_name) const
    {
        return std::filesystem::exists(view_path(view_name));
    }

    Table CsvStorage::load_table(const std::string& table_name) const
    {
        const auto path = table_path(table_name);
        std::ifstream input(path);
        if (!input)
        {
            fail("Table does not exist: " + table_name);
        }

        return load_table_from_stream(input, table_name);
    }

    Table CsvStorage::describe_table(const std::string& table_name) const
    {
        const auto path = table_path(table_name);
        std::ifstream input(path);
        if (!input)
        {
            fail("Table does not exist: " + table_name);
        }

        return describe_table_from_stream(input, table_name);
    }

    RowGenerator CsvStorage::scan_table(const std::string& table_name) const
    {
        return scan_table_from_stream(table_path(table_name), table_name, "Table does not exist: " + table_name);
    }

    std::filesystem::path CsvStorage::resolve_table_source_path(std::filesystem::path path)
    {
        if (path.extension().empty())
        {
            const auto with_csv_extension = path.string() + ".csv";
            if (!std::filesystem::exists(path) && std::filesystem::exists(with_csv_extension))
            {
                path = with_csv_extension;
            }
        }
        return path;
    }

    Table CsvStorage::load_table_from_path(std::filesystem::path path)
    {
        path = resolve_table_source_path(std::move(path));
        std::ifstream input(path);
        if (!input)
        {
            fail("Table file does not exist: " + path.string());
        }

        return load_table_from_stream(input, path.stem().string());
    }

    Table CsvStorage::describe_table_from_path(std::filesystem::path path)
    {
        path = resolve_table_source_path(std::move(path));
        std::ifstream input(path);
        if (!input)
        {
            fail("Table file does not exist: " + path.string());
        }

        return describe_table_from_stream(input, path.stem().string());
    }

    RowGenerator CsvStorage::scan_table_from_path(std::filesystem::path path)
    {
        path = resolve_table_source_path(std::move(path));
        return scan_table_from_stream(path, path.stem().string(), "Table file does not exist: " + path.string());
    }

    ViewDefinition CsvStorage::load_view(const std::string& view_name) const
    {
        const auto path = view_path(view_name);
        std::ifstream input(path);
        if (!input)
        {
            fail("View does not exist: " + view_name);
        }

        std::ostringstream buffer;
        buffer << input.rdbuf();
        const auto query = buffer.str();
        if (query.empty())
        {
            fail("View is empty or invalid: " + view_name);
        }

        return {view_name, query};
    }

    void CsvStorage::save_table(const Table& table)
    {
        if (has_view(table.name))
        {
            fail("View already exists: " + table.name);
        }

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

    void CsvStorage::save_view(const ViewDefinition& view)
    {
        if (has_table(view.name))
        {
            fail("Table already exists: " + view.name);
        }

        const auto path = view_path(view.name);
        std::ofstream output(path, std::ios::trunc);
        if (!output)
        {
            fail("Unable to write view: " + view.name);
        }

        output << view.select_statement;
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

    void CsvStorage::delete_view(const std::string& view_name)
    {
        const auto path = view_path(view_name);
        if (!std::filesystem::exists(path))
        {
            fail("View does not exist: " + view_name);
        }

        if (!std::filesystem::remove(path))
        {
            fail("Unable to delete view: " + view_name);
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
