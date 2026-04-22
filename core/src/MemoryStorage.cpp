#include "MemoryStorage.h"

#include "SqlError.h"
#include "StringUtils.h"

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

    std::filesystem::path MemoryStorage::table_path(const std::string& table_name) const
    {
        return std::filesystem::path(table_name + ".csv");
    }

    bool MemoryStorage::has_table(const std::string& table_name) const
    {
        return tables_.contains(table_name);
    }

    bool MemoryStorage::has_view(const std::string& view_name) const
    {
        return views_.contains(view_name);
    }

    Table MemoryStorage::load_table(const std::string& table_name) const
    {
        const auto it = tables_.find(table_name);
        if (it == tables_.end())
        {
            fail("Table does not exist: " + table_name);
        }

        return it->second;
    }

    ViewDefinition MemoryStorage::load_view(const std::string& view_name) const
    {
        const auto it = views_.find(view_name);
        if (it == views_.end())
        {
            fail("View does not exist: " + view_name);
        }

        return it->second;
    }

    void MemoryStorage::save_table(const Table& table)
    {
        if (has_view(table.name))
        {
            fail("View already exists: " + table.name);
        }
        tables_[table.name] = table;
    }

    void MemoryStorage::save_view(const ViewDefinition& view)
    {
        if (has_table(view.name))
        {
            fail("Table already exists: " + view.name);
        }
        views_[view.name] = view;
    }

    void MemoryStorage::delete_table(const std::string& table_name)
    {
        if (tables_.erase(table_name) == 0)
        {
            fail("Table does not exist: " + table_name);
        }
    }

    void MemoryStorage::delete_view(const std::string& view_name)
    {
        if (views_.erase(view_name) == 0)
        {
            fail("View does not exist: " + view_name);
        }
    }

    std::size_t MemoryStorage::column_index(const Table& table, const std::string& column) const
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
}
