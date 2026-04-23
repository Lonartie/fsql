#pragma once

#include "CoroTypes.h"
#include "ColumnMetadata.h"
#include "SqlTypes.h"

#include <cstddef>
#include <filesystem>
#include <string>

namespace fsql
{
    /// @brief Defines the storage contract used by the SQL executor.
    class IStorage
    {
    public:
        /// @brief Virtual destructor.
        virtual ~IStorage() = default;

        /// @brief Resolves the file path for a table.
        /// @param table_name Logical table name or explicit file path.
        /// @return Storage path for the table.
        virtual std::filesystem::path table_path(const RelationReference& table_name) const = 0;

        /// @brief Checks whether a table exists.
        /// @param table_name Logical table name or explicit file path.
        /// @return `true` when the table exists.
        virtual bool has_table(const RelationReference& table_name) const = 0;

        /// @brief Checks whether a view exists.
        /// @param view_name Logical view name or explicit file path.
        /// @return `true` when the view exists.
        virtual bool has_view(const RelationReference& view_name) const = 0;

        /// @brief Loads a table from storage.
        /// @param table_name Logical table name or explicit file path.
        /// @return Loaded table.
        virtual Table load_table(const RelationReference& table_name) const = 0;

        /// @brief Loads table metadata without materializing table rows.
        /// @param table_name Logical table name or explicit file path.
        /// @return Table metadata containing the logical name and columns.
        virtual Table describe_table(const RelationReference& table_name) const = 0;

        /// @brief Streams table rows from storage.
        /// @param table_name Logical table name or explicit file path.
        /// @return Reopenable row stream for the table contents.
        virtual RowGenerator scan_table(const RelationReference& table_name) const = 0;

        /// @brief Loads a view definition from storage.
        /// @param view_name Logical view name or explicit file path.
        /// @return Loaded view definition.
        virtual ViewDefinition load_view(const RelationReference& view_name) const = 0;

        /// @brief Saves a table to storage.
        /// @param table Table to persist.
        virtual void save_table(const Table& table) = 0;

        /// @brief Returns whether the storage can append rows without rewriting the whole table.
        /// @param table_name Logical table name or explicit file path.
        /// @return `true` when append-based inserts are supported.
        virtual bool supports_append(const RelationReference& table_name) const
        {
            static_cast<void>(table_name);
            return false;
        }

        /// @brief Appends a row to a persisted table without materializing the whole table when supported.
        /// @param table_name Logical table name or explicit file path.
        /// @param table Table schema metadata.
        /// @param row Row to append.
        virtual void append_row(const RelationReference& table_name, const Table& table, const Row& row)
        {
            auto loaded = load_table(table_name);
            loaded.rows.push_back(row);
            save_table(loaded);
        }

        /// @brief Returns the next AUTO_INCREMENT value for a table column.
        /// @param table_name Logical table name or explicit file path.
        /// @param table Table schema metadata.
        /// @param index AUTO_INCREMENT column index.
        /// @return Next generated value.
        virtual std::string next_auto_increment_value_for_insert(const RelationReference& table_name,
                                                                 const Table& table,
                                                                 std::size_t index) const
        {
            static_cast<void>(table);
            return next_auto_increment_value(load_table(table_name), index);
        }

        /// @brief Saves a view definition to storage.
        /// @param view View to persist.
        virtual void save_view(const ViewDefinition& view) = 0;

        /// @brief Deletes a table from storage.
        /// @param table_name Logical table name or explicit file path.
        virtual void delete_table(const RelationReference& table_name) = 0;

        /// @brief Deletes a view from storage.
        /// @param view_name Logical view name or explicit file path.
        virtual void delete_view(const RelationReference& view_name) = 0;

        /// @brief Resolves a column index by name.
        /// @param table Table containing the column.
        /// @param column Column name.
        /// @return Zero-based column index.
        virtual std::size_t column_index(const Table& table, const std::string& column) const = 0;
    };
}
