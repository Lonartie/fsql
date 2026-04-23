#include "YamlStorage.h"

#include "ColumnMetadata.h"
#include "SqlError.h"
#include "StorageFormatUtils.h"
#include "StringUtils.h"

#include <utility>

namespace fsql
{
    namespace
    {
        Table read_yaml_table(std::istream& input, const std::string& table_name, bool include_rows)
        {
            Table table;
            table.name = table_name;

            std::string line;
            enum class Section
            {
                None,
                Columns,
                Rows
            } section = Section::None;

            bool saw_columns = false;
            bool saw_rows = false;
            while (std::getline(input, line))
            {
                const auto trimmed = trim(line);
                if (trimmed.empty() || trimmed[0] == '#')
                {
                    continue;
                }
                if (trimmed == "columns:")
                {
                    section = Section::Columns;
                    saw_columns = true;
                    continue;
                }
                if (trimmed == "rows:")
                {
                    section = Section::Rows;
                    saw_rows = true;
                    continue;
                }
                if (trimmed.rfind("- ", 0) != 0)
                {
                    fail("Unsupported YAML table content: " + table_name);
                }

                const auto value = trim(trimmed.substr(2));
                if (section == Section::Columns)
                {
                    auto parsed = detail::parse_quoted_string_array("[" + value + "]", "Expected YAML quoted column name in table: " + table_name);
                    if (parsed.size() != 1)
                    {
                        fail("Expected YAML quoted column name in table: " + table_name);
                    }
                    table.columns.push_back(std::move(parsed.front()));
                    continue;
                }
                if (section == Section::Rows)
                {
                    auto row = detail::parse_quoted_string_array(value, "Expected YAML row array in table: " + table_name);
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
                fail("YAML table entries must appear under columns: or rows: in table: " + table_name);
            }

            if (!saw_columns || !saw_rows)
            {
                fail("YAML table must contain columns and rows: " + table_name);
            }
            return include_rows ? detail::validate_loaded_table(std::move(table)) : std::move(table);
        }
    }

    YamlStorage::YamlStorage()
        : storage_support_(std::filesystem::current_path(), ".yaml", {".yaml", ".yml"})
    {
    }

    YamlStorage::YamlStorage(std::filesystem::path root_directory)
        : storage_support_(std::move(root_directory), ".yaml", {".yaml", ".yml"})
    {
    }

    std::filesystem::path YamlStorage::table_path(const RelationReference& table_name) const
    {
        return storage_support_.table_path(table_name);
    }

    bool YamlStorage::has_table(const RelationReference& table_name) const
    {
        return storage_support_.has_table(table_name);
    }

    bool YamlStorage::has_view(const RelationReference& view_name) const
    {
        return storage_support_.has_view(view_name);
    }

    Table YamlStorage::load_table(const RelationReference& table_name) const
    {
        return storage_support_.load_table(table_name, load_table_from_stream);
    }

    Table YamlStorage::describe_table(const RelationReference& table_name) const
    {
        return storage_support_.describe_table(table_name, describe_table_from_stream);
    }

    RowGenerator YamlStorage::scan_table(const RelationReference& table_name) const
    {
        return storage_support_.scan_table(table_name, describe_table_from_stream, scan_table_from_file);
    }

    ViewDefinition YamlStorage::load_view(const RelationReference& view_name) const
    {
        return storage_support_.load_view(view_name);
    }

    void YamlStorage::save_table(const Table& table)
    {
        storage_support_.save_table(table, write_table_to_stream);
    }

    bool YamlStorage::supports_append(const RelationReference& table_name) const
    {
        return storage_support_.supports_append(table_name);
    }

    void YamlStorage::append_row(const RelationReference& table_name, const Table& table, const Row& row)
    {
        storage_support_.append_row(table_name, table, row);
    }

    std::string YamlStorage::next_auto_increment_value_for_insert(const RelationReference& table_name,
                                                                  const Table& table,
                                                                  std::size_t index) const
    {
        if (const auto next_value = storage_support_.read_next_auto_increment_value(table_name); next_value.has_value())
        {
            return *next_value;
        }
        return fsql::next_auto_increment_value(load_table(table_name), index);
    }

    void YamlStorage::save_view(const ViewDefinition& view)
    {
        storage_support_.save_view(view);
    }

    void YamlStorage::delete_table(const RelationReference& table_name)
    {
        storage_support_.delete_table(table_name);
    }

    void YamlStorage::delete_view(const RelationReference& view_name)
    {
        storage_support_.delete_view(view_name);
    }

    std::size_t YamlStorage::column_index(const Table& table, const std::string& column) const
    {
        return storage_support_.column_index(table, column);
    }

    Table YamlStorage::load_table_from_stream(std::istream& input, const std::string& table_name)
    {
        return read_yaml_table(input, table_name, true);
    }

    Table YamlStorage::describe_table_from_stream(std::istream& input, const std::string& table_name)
    {
        return read_yaml_table(input, table_name, false);
    }

    RowGenerator YamlStorage::scan_table_from_file(std::ifstream input, std::string table_name)
    {
        std::string line;
        enum class Section
        {
            None,
            Columns,
            Rows
        } section = Section::None;
        std::size_t column_count = 0;
        bool saw_columns = false;
        bool saw_rows = false;
        while (std::getline(input, line))
        {
            const auto trimmed = trim(line);
            if (trimmed.empty() || trimmed[0] == '#')
            {
                continue;
            }
            if (trimmed == "columns:")
            {
                section = Section::Columns;
                saw_columns = true;
                continue;
            }
            if (trimmed == "rows:")
            {
                section = Section::Rows;
                saw_rows = true;
                continue;
            }
            if (trimmed.rfind("- ", 0) != 0)
            {
                fail("Unsupported YAML table content: " + table_name);
            }

            const auto value = trim(trimmed.substr(2));
            if (section == Section::Columns)
            {
                ++column_count;
                continue;
            }
            if (section == Section::Rows)
            {
                auto row = detail::parse_quoted_string_array(value, "Expected YAML row array in table: " + table_name);
                if (saw_columns && row.size() != column_count)
                {
                    fail("Row column count mismatch in table: " + table_name);
                }
                co_yield row;
                continue;
            }
            fail("YAML table entries must appear under columns: or rows: in table: " + table_name);
        }

        if (!saw_columns || !saw_rows)
        {
            fail("YAML table must contain columns and rows: " + table_name);
        }
    }

    void YamlStorage::write_table_to_stream(std::ostream& output, const Table& table)
    {
        output << "columns:\n";
        for (const auto& column : table.columns)
        {
            output << "  - \"" << detail::quoted_string_body(column) << "\"\n";
        }
        output << "rows:\n";
        for (const auto& row : table.rows)
        {
            output << "  - [";
            for (std::size_t i = 0; i < row.size(); ++i)
            {
                if (i > 0)
                {
                    output << ", ";
                }
                output << '"' << detail::quoted_string_body(row[i]) << '"';
            }
            output << "]\n";
        }
    }
}
