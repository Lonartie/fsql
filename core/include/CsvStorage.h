#pragma once

#include "IStorage.h"

#include <cstddef>
#include <filesystem>
#include <string>
#include <vector>

namespace sql
{
    /// @brief Provides CSV-backed table persistence.
    class CsvStorage final : public IStorage
    {
    public:
        /// @brief Initializes storage rooted at the current working directory.
        CsvStorage();

        /// @brief Initializes storage rooted at a specific directory.
        /// @param root_directory Directory containing CSV tables.
        explicit CsvStorage(std::filesystem::path root_directory);

        /// @brief Resolves the file path for a table.
        /// @param table_name Logical table name.
        /// @return Full CSV file path.
        std::filesystem::path table_path(const std::string& table_name) const override;

        /// @brief Loads a table from disk.
        /// @param table_name Logical table name.
        /// @return Loaded table.
        Table load_table(const std::string& table_name) const override;

        /// @brief Saves a table to disk.
        /// @param table Table to persist.
        void save_table(const Table& table) override;

        /// @brief Deletes a table from disk.
        /// @param table_name Logical table name.
        void delete_table(const std::string& table_name) override;

        /// @brief Resolves a column index by name.
        /// @param table Table containing the column.
        /// @param column Column name.
        /// @return Zero-based column index.
        std::size_t column_index(const Table& table, const std::string& column) const override;

        /// @brief Escapes a value for CSV output.
        /// @param value Raw field value.
        /// @return Escaped CSV field.
        static std::string escape_csv(const std::string& value);

        /// @brief Parses a single CSV line.
        /// @param line CSV line.
        /// @return Parsed fields.
        static std::vector<std::string> parse_csv_line(const std::string& line);

    private:
        /// @brief Root directory containing CSV files.
        std::filesystem::path root_directory_;
    };
}
