#include "MemoryStorage.h"

#include "ColumnMetadata.h"
#include "SqlError.h"
#include "StringUtils.h"

namespace fsql
{

    std::filesystem::path MemoryStorage::table_path(const RelationReference& table_name) const
    {
        return std::filesystem::path(table_name.name + ".csv");
    }

    bool MemoryStorage::has_table(const RelationReference& table_name) const
    {
        return tables_.contains(table_name.name);
    }

    bool MemoryStorage::has_view(const RelationReference& view_name) const
    {
        return views_.contains(view_name.name);
    }

    Table MemoryStorage::load_table(const RelationReference& table_name) const
    {
        const auto it = tables_.find(table_name.name);
        if (it == tables_.end())
        {
            fail("Table does not exist: " + table_name.name);
        }

        return it->second;
    }

    Table MemoryStorage::describe_table(const RelationReference& table_name) const
    {
        auto table = load_table(table_name);
        table.rows.clear();
        return table;
    }

    RowGenerator MemoryStorage::scan_table(const RelationReference& table_name) const
    {
        const auto it = tables_.find(table_name.name);
        if (it == tables_.end())
        {
            fail("Table does not exist: " + table_name.name);
        }

        for (const auto& row : it->second.rows)
        {
            co_yield row;
        }
    }

    ViewDefinition MemoryStorage::load_view(const RelationReference& view_name) const
    {
        const auto it = views_.find(view_name.name);
        if (it == views_.end())
        {
            fail("View does not exist: " + view_name.name);
        }

        return it->second;
    }

    void MemoryStorage::save_table(const Table& table)
    {
        if (has_view({RelationReference::Kind::Identifier, table.name}))
        {
            fail("View already exists: " + table.name);
        }
        tables_[table.name] = table;
    }

    void MemoryStorage::save_view(const ViewDefinition& view)
    {
        if (has_table({RelationReference::Kind::Identifier, view.name}))
        {
            fail("Table already exists: " + view.name);
        }
        views_[view.name] = view;
    }

    void MemoryStorage::delete_table(const RelationReference& table_name)
    {
        if (tables_.erase(table_name.name) == 0)
        {
            fail("Table does not exist: " + table_name.name);
        }
    }

    void MemoryStorage::delete_view(const RelationReference& view_name)
    {
        if (views_.erase(view_name.name) == 0)
        {
            fail("View does not exist: " + view_name.name);
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
