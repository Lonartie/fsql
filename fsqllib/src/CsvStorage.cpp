#include "CsvStorage.h"

#include "ColumnMetadata.h"
#include "SqlError.h"
#include "StringUtils.h"

#include <fstream>
#include <sstream>
#include <utility>

namespace fsql
{
    namespace
    {
        constexpr std::string_view view_suffix = ".view.sql";

        bool ends_with(std::string_view text, std::string_view suffix)
        {
            return text.size() >= suffix.size() && text.substr(text.size() - suffix.size()) == suffix;
        }

        std::string view_logical_name_from_path(const std::filesystem::path& path)
        {
            const auto filename = path.filename().string();
            if (ends_with(filename, view_suffix))
            {
                return filename.substr(0, filename.size() - view_suffix.size());
            }
            return path.stem().string();
        }

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

    std::filesystem::path CsvStorage::rooted_path(const RelationReference& reference) const
    {
        auto path = std::filesystem::path(reference.name);
        if (path.is_absolute())
        {
            return path;
        }
        return root_directory_ / path;
    }

    std::filesystem::path CsvStorage::explicit_table_path(const RelationReference& table_name, bool for_write) const
    {
        auto path = rooted_path(table_name);
        if (for_write && path.extension().empty() && !std::filesystem::exists(path))
        {
            path += ".csv";
        }
        return for_write ? path : resolve_table_source_path(std::move(path));
    }

    std::filesystem::path CsvStorage::explicit_view_path(const RelationReference& view_name, bool for_write) const
    {
        auto path = rooted_path(view_name);
        if (for_write && path.extension().empty() && !std::filesystem::exists(path))
        {
            path += view_suffix;
        }
        return for_write ? path : resolve_view_source_path(std::move(path));
    }

    std::filesystem::path CsvStorage::table_path(const RelationReference& table_name) const
    {
        if (table_name.kind == RelationReference::Kind::FilePath)
        {
            return explicit_table_path(table_name, false);
        }
        return root_directory_ / (table_name.name + ".csv");
    }

    std::filesystem::path CsvStorage::view_path(const RelationReference& view_name) const
    {
        if (view_name.kind == RelationReference::Kind::FilePath)
        {
            return explicit_view_path(view_name, false);
        }
        return root_directory_ / (view_name.name + ".view.sql");
    }

    bool CsvStorage::has_table(const RelationReference& table_name) const
    {
        return std::filesystem::exists(table_path(table_name));
    }

    bool CsvStorage::has_view(const RelationReference& view_name) const
    {
        return std::filesystem::exists(view_path(view_name));
    }

    Table CsvStorage::load_table(const RelationReference& table_name) const
    {
        const auto path = table_path(table_name);
        std::ifstream input(path);
        if (!input)
        {
            fail("Table does not exist: " + table_name.name);
        }

        auto table = load_table_from_stream(input, table_name.name);
        if (table_name.kind == RelationReference::Kind::FilePath)
        {
            table.storage_path = path;
        }
        return table;
    }

    Table CsvStorage::describe_table(const RelationReference& table_name) const
    {
        const auto path = table_path(table_name);
        std::ifstream input(path);
        if (!input)
        {
            fail("Table does not exist: " + table_name.name);
        }

        auto table = describe_table_from_stream(input, table_name.name);
        if (table_name.kind == RelationReference::Kind::FilePath)
        {
            table.storage_path = path;
        }
        return table;
    }

    RowGenerator CsvStorage::scan_table(const RelationReference& table_name) const
    {
        return scan_table_from_stream(table_path(table_name), table_name.name, "Table does not exist: " + table_name.name);
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

    std::filesystem::path CsvStorage::resolve_view_source_path(std::filesystem::path path)
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

    Table CsvStorage::load_table_from_path(std::filesystem::path path)
    {
        path = resolve_table_source_path(std::move(path));
        std::ifstream input(path);
        if (!input)
        {
            fail("Table file does not exist: " + path.string());
        }

        auto table = load_table_from_stream(input, path.stem().string());
        table.storage_path = path;
        return table;
    }

    Table CsvStorage::describe_table_from_path(std::filesystem::path path)
    {
        path = resolve_table_source_path(std::move(path));
        std::ifstream input(path);
        if (!input)
        {
            fail("Table file does not exist: " + path.string());
        }

        auto table = describe_table_from_stream(input, path.stem().string());
        table.storage_path = path;
        return table;
    }

    RowGenerator CsvStorage::scan_table_from_path(std::filesystem::path path)
    {
        path = resolve_table_source_path(std::move(path));
        return scan_table_from_stream(path, path.stem().string(), "Table file does not exist: " + path.string());
    }

    ViewDefinition CsvStorage::load_view(const RelationReference& view_name) const
    {
        const auto path = view_path(view_name);
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

    ViewDefinition CsvStorage::load_view_from_path(std::filesystem::path path)
    {
        path = resolve_view_source_path(std::move(path));
        std::ifstream input(path);
        if (!input)
        {
            fail("View file does not exist: " + path.string());
        }

        std::ostringstream buffer;
        buffer << input.rdbuf();
        const auto query = buffer.str();
        if (query.empty())
        {
            fail("View is empty or invalid: " + path.string());
        }

        ViewDefinition view;
        view.name = view_logical_name_from_path(path);
        view.storage_path = path;
        view.select_statement = query;
        return view;
    }

    void CsvStorage::save_table(const Table& table)
    {
        const bool uses_explicit_path = table.storage_path.has_value();
        if (!uses_explicit_path)
        {
            RelationReference reference;
            reference.name = table.name;
            if (has_view(reference))
            {
                fail("View already exists: " + table.name);
            }
        }

        const auto path = uses_explicit_path
            ? explicit_table_path({RelationReference::Kind::FilePath, table.storage_path->string()}, true)
            : table_path({RelationReference::Kind::Identifier, table.name});
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
        const bool uses_explicit_path = view.storage_path.has_value();
        if (!uses_explicit_path)
        {
            RelationReference reference;
            reference.name = view.name;
            if (has_table(reference))
            {
                fail("Table already exists: " + view.name);
            }
        }

        const auto path = uses_explicit_path
            ? explicit_view_path({RelationReference::Kind::FilePath, view.storage_path->string()}, true)
            : view_path({RelationReference::Kind::Identifier, view.name});
        std::ofstream output(path, std::ios::trunc);
        if (!output)
        {
            fail("Unable to write view: " + view.name);
        }

        output << view.select_statement;
    }

    void CsvStorage::delete_table(const RelationReference& table_name)
    {
        const auto path = table_path(table_name);
        if (!std::filesystem::exists(path))
        {
            fail("Table does not exist: " + table_name.name);
        }

        if (!std::filesystem::remove(path))
        {
            fail("Unable to delete table: " + table_name.name);
        }
    }

    void CsvStorage::delete_view(const RelationReference& view_name)
    {
        const auto path = view_path(view_name);
        if (!std::filesystem::exists(path))
        {
            fail("View does not exist: " + view_name.name);
        }

        if (!std::filesystem::remove(path))
        {
            fail("Unable to delete view: " + view_name.name);
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
