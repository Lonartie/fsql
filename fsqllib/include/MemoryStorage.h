#pragma once

#include "IStorage.h"

#include <unordered_map>

namespace fsql
{
    /// @brief In-memory storage implementation used primarily for tests.
    class MemoryStorage final : public IStorage
    {
    public:
        std::filesystem::path table_path(const std::string& table_name) const
        {
            return table_path({RelationReference::Kind::Identifier, table_name});
        }

        /// @brief Resolves a synthetic path for a table.
        /// @param table_name Logical table name.
        /// @return Synthetic path ending in `.csv`.
        std::filesystem::path table_path(const RelationReference& table_name) const override;

        bool has_table(const std::string& table_name) const
        {
            return has_table({RelationReference::Kind::Identifier, table_name});
        }

        /// @brief Checks whether a table exists in memory.
        /// @param table_name Logical table name.
        /// @return `true` when the table exists.
        bool has_table(const RelationReference& table_name) const override;

        bool has_view(const std::string& view_name) const
        {
            return has_view({RelationReference::Kind::Identifier, view_name});
        }

        /// @brief Checks whether a view exists in memory.
        /// @param view_name Logical view name.
        /// @return `true` when the view exists.
        bool has_view(const RelationReference& view_name) const override;

        Table load_table(const std::string& table_name) const
        {
            return load_table({RelationReference::Kind::Identifier, table_name});
        }

        /// @brief Loads a table from memory.
        /// @param table_name Logical table name.
        /// @return Loaded table.
        Table load_table(const RelationReference& table_name) const override;

        Table describe_table(const std::string& table_name) const
        {
            return describe_table({RelationReference::Kind::Identifier, table_name});
        }

        /// @brief Loads table metadata from memory without materializing rows.
        /// @param table_name Logical table name.
        /// @return Table metadata containing the logical name and columns.
        Table describe_table(const RelationReference& table_name) const override;

        RowGenerator scan_table(const std::string& table_name) const
        {
            return scan_table({RelationReference::Kind::Identifier, table_name});
        }

        /// @brief Streams table rows from memory.
        /// @param table_name Logical table name.
        /// @return Row stream for the table contents.
        RowGenerator scan_table(const RelationReference& table_name) const override;

        ViewDefinition load_view(const std::string& view_name) const
        {
            return load_view({RelationReference::Kind::Identifier, view_name});
        }

        /// @brief Loads a view definition from memory.
        /// @param view_name Logical view name.
        /// @return Loaded view definition.
        ViewDefinition load_view(const RelationReference& view_name) const override;

        /// @brief Saves a table into memory.
        /// @param table Table to persist.
        void save_table(const Table& table) override;

        bool supports_append(const RelationReference& table_name) const override;
        void append_row(const RelationReference& table_name, const Table& table, const Row& row) override;
        std::string next_auto_increment_value_for_insert(const RelationReference& table_name,
                                                         const Table& table,
                                                         std::size_t index) const override;

        /// @brief Saves a view definition into memory.
        /// @param view View definition to persist.
        void save_view(const ViewDefinition& view) override;

        void delete_table(const std::string& table_name)
        {
            delete_table({RelationReference::Kind::Identifier, table_name});
        }

        /// @brief Deletes a table from memory.
        /// @param table_name Logical table name.
        void delete_table(const RelationReference& table_name) override;

        void delete_view(const std::string& view_name)
        {
            delete_view({RelationReference::Kind::Identifier, view_name});
        }

        /// @brief Deletes a view from memory.
        /// @param view_name Logical view name.
        void delete_view(const RelationReference& view_name) override;

        /// @brief Resolves a column index by name.
        /// @param table Table containing the column.
        /// @param column Column name.
        /// @return Zero-based column index.
        std::size_t column_index(const Table& table, const std::string& column) const override;

    private:
        /// @brief In-memory table store keyed by logical table name.
        std::unordered_map<std::string, Table> tables_;

        /// @brief In-memory view store keyed by logical view name.
        std::unordered_map<std::string, ViewDefinition> views_;
    };
}
