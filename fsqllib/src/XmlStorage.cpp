#include "XmlStorage.h"

#include "ColumnMetadata.h"
#include "SqlError.h"
#include "StorageFormatUtils.h"
#include "StringUtils.h"

#include <utility>

namespace fsql
{
    namespace
    {
        std::vector<std::string> parse_row_values(const std::string& row_text, const std::string& table_name)
        {
            std::vector<std::string> row;
            std::size_t position = 0;
            while (true)
            {
                const auto value_open = row_text.find("<value>", position);
                if (value_open == std::string::npos)
                {
                    return row;
                }
                const auto value_start = value_open + 7;
                const auto value_close = row_text.find("</value>", value_start);
                if (value_close == std::string::npos)
                {
                    fail("Malformed XML value entry in table: " + table_name);
                }
                row.push_back(detail::xml_unescape(row_text.substr(value_start, value_close - value_start)));
                position = value_close + 8;
            }
        }

        Table read_xml_table(std::istream& input, const std::string& table_name, bool include_rows)
        {
            Table table;
            table.name = table_name;

            std::string line;
            bool saw_columns = false;
            bool saw_rows_section = false;
            while (std::getline(input, line))
            {
                const auto trimmed = trim(line);
                if (trimmed.empty())
                {
                    continue;
                }
                if (trimmed == "<rows>" || trimmed == "</rows>")
                {
                    saw_rows_section = true;
                    continue;
                }
                if (trimmed.find("<column>") != std::string::npos)
                {
                    const auto start = trimmed.find("<column>") + 8;
                    const auto end = trimmed.find("</column>", start);
                    if (end == std::string::npos)
                    {
                        fail("Malformed XML column entry in table: " + table_name);
                    }
                    table.columns.push_back(detail::xml_unescape(trimmed.substr(start, end - start)));
                    saw_columns = true;
                    continue;
                }
                if (trimmed.find("<row>") != std::string::npos)
                {
                    const auto start = trimmed.find("<row>") + 5;
                    const auto end = trimmed.find("</row>", start);
                    if (end == std::string::npos)
                    {
                        fail("Malformed XML row entry in table: " + table_name);
                    }
                    auto row = parse_row_values(trimmed.substr(start, end - start), table_name);
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
            }

            if (!saw_columns || !saw_rows_section)
            {
                fail("XML table must contain columns and rows: " + table_name);
            }
            return include_rows ? detail::validate_loaded_table(std::move(table)) : std::move(table);
        }
    }

    XmlStorage::XmlStorage()
        : storage_support_(std::filesystem::current_path(), ".xml")
    {
    }

    XmlStorage::XmlStorage(std::filesystem::path root_directory)
        : storage_support_(std::move(root_directory), ".xml")
    {
    }

    std::filesystem::path XmlStorage::table_path(const RelationReference& table_name) const
    {
        return storage_support_.table_path(table_name);
    }

    bool XmlStorage::has_table(const RelationReference& table_name) const
    {
        return storage_support_.has_table(table_name);
    }

    bool XmlStorage::has_view(const RelationReference& view_name) const
    {
        return storage_support_.has_view(view_name);
    }

    Table XmlStorage::load_table(const RelationReference& table_name) const
    {
        return storage_support_.load_table(table_name, load_table_from_stream);
    }

    Table XmlStorage::describe_table(const RelationReference& table_name) const
    {
        return storage_support_.describe_table(table_name, describe_table_from_stream);
    }

    RowGenerator XmlStorage::scan_table(const RelationReference& table_name) const
    {
        return storage_support_.scan_table(table_name, describe_table_from_stream, scan_table_from_file);
    }

    ViewDefinition XmlStorage::load_view(const RelationReference& view_name) const
    {
        return storage_support_.load_view(view_name);
    }

    void XmlStorage::save_table(const Table& table)
    {
        storage_support_.save_table(table, write_table_to_stream);
    }

    bool XmlStorage::supports_append(const RelationReference& table_name) const
    {
        return storage_support_.supports_append(table_name);
    }

    void XmlStorage::append_row(const RelationReference& table_name, const Table& table, const Row& row)
    {
        storage_support_.append_row(table_name, table, row);
    }

    std::string XmlStorage::next_auto_increment_value_for_insert(const RelationReference& table_name,
                                                                 const Table& table,
                                                                 std::size_t index) const
    {
        if (const auto next_value = storage_support_.read_next_auto_increment_value(table_name); next_value.has_value())
        {
            return *next_value;
        }
        return fsql::next_auto_increment_value(load_table(table_name), index);
    }

    void XmlStorage::save_view(const ViewDefinition& view)
    {
        storage_support_.save_view(view);
    }

    void XmlStorage::delete_table(const RelationReference& table_name)
    {
        storage_support_.delete_table(table_name);
    }

    void XmlStorage::delete_view(const RelationReference& view_name)
    {
        storage_support_.delete_view(view_name);
    }

    std::size_t XmlStorage::column_index(const Table& table, const std::string& column) const
    {
        return storage_support_.column_index(table, column);
    }

    Table XmlStorage::load_table_from_stream(std::istream& input, const std::string& table_name)
    {
        return read_xml_table(input, table_name, true);
    }

    Table XmlStorage::describe_table_from_stream(std::istream& input, const std::string& table_name)
    {
        return read_xml_table(input, table_name, false);
    }

    RowGenerator XmlStorage::scan_table_from_file(std::ifstream input, std::string table_name)
    {
        std::string line;
        std::size_t column_count = 0;
        bool saw_columns = false;
        bool saw_rows_section = false;
        while (std::getline(input, line))
        {
            const auto trimmed = trim(line);
            if (trimmed.empty())
            {
                continue;
            }
            if (trimmed == "<rows>" || trimmed == "</rows>")
            {
                saw_rows_section = true;
                continue;
            }
            if (trimmed.find("<column>") != std::string::npos)
            {
                ++column_count;
                saw_columns = true;
                continue;
            }
            if (trimmed.find("<row>") != std::string::npos)
            {
                const auto start = trimmed.find("<row>") + 5;
                const auto end = trimmed.find("</row>", start);
                if (end == std::string::npos)
                {
                    fail("Malformed XML row entry in table: " + table_name);
                }
                auto row = parse_row_values(trimmed.substr(start, end - start), table_name);
                if (saw_columns && row.size() != column_count)
                {
                    fail("Row column count mismatch in table: " + table_name);
                }
                co_yield row;
            }
        }

        if (!saw_columns || !saw_rows_section)
        {
            fail("XML table must contain columns and rows: " + table_name);
        }
    }

    void XmlStorage::write_table_to_stream(std::ostream& output, const Table& table)
    {
        output << "<table>\n";
        output << "  <columns>\n";
        for (const auto& column : table.columns)
        {
            output << "    <column>" << detail::xml_escape(column) << "</column>\n";
        }
        output << "  </columns>\n";
        output << "  <rows>\n";
        for (const auto& row : table.rows)
        {
            output << "    <row>";
            for (const auto& value : row)
            {
                output << "<value>" << detail::xml_escape(value) << "</value>";
            }
            output << "</row>\n";
        }
        output << "  </rows>\n";
        output << "</table>\n";
    }
}
