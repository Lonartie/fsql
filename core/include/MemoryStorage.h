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

        /// @brief Loads a table from memory.
        /// @param table_name Logical table name.
        /// @return Loaded table.
        Table load_table(const std::string& table_name) const override;

        /// @brief Saves a table into memory.
        /// @param table Table to persist.
        void save_table(const Table& table) override;

        /// @brief Deletes a table from memory.
        /// @param table_name Logical table name.
        void delete_table(const std::string& table_name) override;

        /// @brief Resolves a column index by name.
        /// @param table Table containing the column.
        /// @param column Column name.
        /// @return Zero-based column index.
        std::size_t column_index(const Table& table, const std::string& column) const override;

    private:
        /// @brief In-memory table store keyed by logical table name.
        std::unordered_map<std::string, Table> tables_;
    };
}
