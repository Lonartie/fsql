#pragma once

#include "SqlTypes.h"

#include <cstddef>
#include <filesystem>
#include <string>

namespace sql
{
    /// @brief Defines the storage contract used by the SQL executor.
    class IStorage
    {
    public:
        /// @brief Virtual destructor.
        virtual ~IStorage() = default;

        /// @brief Resolves the file path for a table.
        /// @param table_name Logical table name.
        /// @return Storage path for the table.
        virtual std::filesystem::path table_path(const std::string& table_name) const = 0;

        /// @brief Loads a table from storage.
        /// @param table_name Logical table name.
        /// @return Loaded table.
        virtual Table load_table(const std::string& table_name) const = 0;

        /// @brief Saves a table to storage.
        /// @param table Table to persist.
        virtual void save_table(const Table& table) = 0;

        /// @brief Deletes a table from storage.
        /// @param table_name Logical table name.
        virtual void delete_table(const std::string& table_name) = 0;

        /// @brief Resolves a column index by name.
        /// @param table Table containing the column.
        /// @param column Column name.
        /// @return Zero-based column index.
        virtual std::size_t column_index(const Table& table, const std::string& column) const = 0;
    };
}
