#pragma once

#include "IStorage.h"

#include <cstddef>
#include <filesystem>
#include <string>
#include <vector>

namespace fsql
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

        std::filesystem::path table_path(const std::string& table_name) const
        {
            return table_path({RelationReference::Kind::Identifier, table_name});
        }

        /// @brief Resolves the file path for a table.
        /// @param table_name Logical table name.
        /// @return Full CSV file path.
        std::filesystem::path table_path(const RelationReference& table_name) const override;

        bool has_table(const std::string& table_name) const
        {
            return has_table({RelationReference::Kind::Identifier, table_name});
        }

        /// @brief Checks whether a CSV-backed table exists.
        /// @param table_name Logical table name.
        /// @return `true` when the table exists.
        bool has_table(const RelationReference& table_name) const override;

        bool has_view(const std::string& view_name) const
        {
            return has_view({RelationReference::Kind::Identifier, view_name});
        }

        /// @brief Checks whether a persisted view exists.
        /// @param view_name Logical view name.
        /// @return `true` when the view exists.
        bool has_view(const RelationReference& view_name) const override;

        Table load_table(const std::string& table_name) const
        {
            return load_table({RelationReference::Kind::Identifier, table_name});
        }

        /// @brief Loads a table from disk.
        /// @param table_name Logical table name.
        /// @return Loaded table.
        Table load_table(const RelationReference& table_name) const override;

        Table describe_table(const std::string& table_name) const
        {
            return describe_table({RelationReference::Kind::Identifier, table_name});
        }

        /// @brief Loads table metadata from disk without materializing rows.
        /// @param table_name Logical table name.
        /// @return Table metadata containing the logical name and columns.
        Table describe_table(const RelationReference& table_name) const override;

        RowGenerator scan_table(const std::string& table_name) const
        {
            return scan_table({RelationReference::Kind::Identifier, table_name});
        }

        /// @brief Streams table rows from disk.
        /// @param table_name Logical table name.
        /// @return Row stream for the table contents.
        RowGenerator scan_table(const RelationReference& table_name) const override;

        /// @brief Resolves a direct CSV file source path, optionally appending `.csv`.
        /// @param path Candidate file path.
        /// @return Resolved file path.
        static std::filesystem::path resolve_table_source_path(std::filesystem::path path);

        /// @brief Resolves a direct view definition file path, optionally appending `.view.sql`.
        /// @param path Candidate file path.
        /// @return Resolved file path.
        static std::filesystem::path resolve_view_source_path(std::filesystem::path path);

        /// @brief Loads a table directly from a CSV file path.
        /// @param path CSV file path with optional `.csv` extension omitted.
        /// @return Loaded table.
        static Table load_table_from_path(std::filesystem::path path);

        /// @brief Loads table metadata directly from a CSV file path.
        /// @param path CSV file path with optional `.csv` extension omitted.
        /// @return Table metadata containing the logical name and columns.
        static Table describe_table_from_path(std::filesystem::path path);

        /// @brief Streams rows directly from a CSV file path.
        /// @param path CSV file path with optional `.csv` extension omitted.
        /// @return Row stream for the file contents.
        static RowGenerator scan_table_from_path(std::filesystem::path path);

        /// @brief Loads a view definition from disk.
        /// @param view_name Logical view name.
        /// @return Loaded view definition.
        ViewDefinition load_view(const RelationReference& view_name) const override;

        ViewDefinition load_view(const std::string& view_name) const
        {
            return load_view({RelationReference::Kind::Identifier, view_name});
        }

        /// @brief Loads a view definition directly from a file path.
        /// @param path View definition file path with optional `.view.sql` extension omitted.
        /// @return Loaded view definition.
        static ViewDefinition load_view_from_path(std::filesystem::path path);

        /// @brief Saves a table to disk.
        /// @param table Table to persist.
        void save_table(const Table& table) override;

        /// @brief Saves a view definition to disk.
        /// @param view View definition to persist.
        void save_view(const ViewDefinition& view) override;

        /// @brief Deletes a table from disk.
        /// @param table_name Logical table name.
        void delete_table(const RelationReference& table_name) override;

        void delete_table(const std::string& table_name)
        {
            delete_table({RelationReference::Kind::Identifier, table_name});
        }

        /// @brief Deletes a view from disk.
        /// @param view_name Logical view name.
        void delete_view(const RelationReference& view_name) override;

        void delete_view(const std::string& view_name)
        {
            delete_view({RelationReference::Kind::Identifier, view_name});
        }

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
        /// @brief Resolves the file path for a view definition.
        /// @param view_name Logical view name.
        /// @return Full view definition path.
        std::filesystem::path view_path(const RelationReference& view_name) const;

        /// @brief Resolves a relation reference against the storage root when needed.
        /// @param reference Relation reference.
        /// @return Rooted filesystem path.
        std::filesystem::path rooted_path(const RelationReference& reference) const;

        /// @brief Resolves the file path for a path-addressed table.
        /// @param table_name File-path table reference.
        /// @param for_write Indicates whether the result will be written.
        /// @return Full CSV file path.
        std::filesystem::path explicit_table_path(const RelationReference& table_name, bool for_write) const;

        /// @brief Resolves the file path for a path-addressed view.
        /// @param view_name File-path view reference.
        /// @param for_write Indicates whether the result will be written.
        /// @return Full view definition file path.
        std::filesystem::path explicit_view_path(const RelationReference& view_name, bool for_write) const;

        /// @brief Root directory containing CSV files.
        std::filesystem::path root_directory_;
    };
}
