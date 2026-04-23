#pragma once

#include "IStorage.h"

#include <unordered_map>

namespace sql
{
    /// @brief In-memory storage implementation used primarily for tests.
    class MemoryStorage final : public IStorage
    {
    public:
        /// @brief Resolves a synthetic path for a table.
        /// @param table_name Logical table name.
        /// @return Synthetic path ending in `.csv`.
        std::filesystem::path table_path(const std::string& table_name) const override;

        /// @brief Checks whether a table exists in memory.
        /// @param table_name Logical table name.
        /// @return `true` when the table exists.
        bool has_table(const std::string& table_name) const override;

        /// @brief Checks whether a view exists in memory.
        /// @param view_name Logical view name.
        /// @return `true` when the view exists.
        bool has_view(const std::string& view_name) const override;

        /// @brief Loads a table from memory.
        /// @param table_name Logical table name.
        /// @return Loaded table.
        Table load_table(const std::string& table_name) const override;

        /// @brief Loads table metadata from memory without materializing rows.
        /// @param table_name Logical table name.
        /// @return Table metadata containing the logical name and columns.
        Table describe_table(const std::string& table_name) const override;

        /// @brief Streams table rows from memory.
        /// @param table_name Logical table name.
        /// @return Row stream for the table contents.
        RowGenerator scan_table(const std::string& table_name) const override;

        /// @brief Loads a view definition from memory.
        /// @param view_name Logical view name.
        /// @return Loaded view definition.
        ViewDefinition load_view(const std::string& view_name) const override;

        /// @brief Saves a table into memory.
        /// @param table Table to persist.
        void save_table(const Table& table) override;

        /// @brief Saves a view definition into memory.
        /// @param view View definition to persist.
        void save_view(const ViewDefinition& view) override;

        /// @brief Deletes a table from memory.
        /// @param table_name Logical table name.
        void delete_table(const std::string& table_name) override;

        /// @brief Deletes a view from memory.
        /// @param view_name Logical view name.
        void delete_view(const std::string& view_name) override;

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
