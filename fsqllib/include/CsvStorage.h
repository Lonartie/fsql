#pragma once

#include "FileStorageSupport.h"
#include "StorageRegistry.h"

#include <cstddef>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace fsql
{
    /// @brief Persists tables in CSV files.
    class CsvStorage final : public IStorage
    {
    public:
        CsvStorage();
        explicit CsvStorage(std::filesystem::path root_directory);

        std::filesystem::path table_path(const RelationReference& table_name) const override;
        bool has_table(const RelationReference& table_name) const override;
        bool has_view(const RelationReference& view_name) const override;
        Table load_table(const RelationReference& table_name) const override;
        Table describe_table(const RelationReference& table_name) const override;
        RowGenerator scan_table(const RelationReference& table_name) const override;
        ViewDefinition load_view(const RelationReference& view_name) const override;
        void save_table(const Table& table) override;
        bool supports_append(const RelationReference& table_name) const override;
        void append_row(const RelationReference& table_name, const Table& table, const Row& row) override;
        std::string next_auto_increment_value_for_insert(const RelationReference& table_name,
                                                         const Table& table,
                                                         std::size_t index) const override;
        void save_view(const ViewDefinition& view) override;
        void delete_table(const RelationReference& table_name) override;
        void delete_view(const RelationReference& view_name) override;
        std::size_t column_index(const Table& table, const std::string& column) const override;

        static std::filesystem::path resolve_table_source_path(std::filesystem::path path);
        static std::filesystem::path resolve_view_source_path(std::filesystem::path path);
        static Table load_table_from_path(std::filesystem::path path);
        static Table describe_table_from_path(std::filesystem::path path);
        static RowGenerator scan_table_from_path(std::filesystem::path path);
        static ViewDefinition load_view_from_path(std::filesystem::path path);
        static std::string escape_csv(const std::string& value);
        static std::vector<std::string> parse_csv_line(const std::string& line);

    private:
        static Table load_table_from_stream(std::istream& input, const std::string& table_name);
        static Table describe_table_from_stream(std::istream& input, const std::string& table_name);
        static RowGenerator scan_table_from_file(std::ifstream input, std::string table_name);
        static void write_table_to_stream(std::ostream& output, const Table& table);

        FileStorageSupport storage_support_;
        FSQL_AUTO_REGISTER_STORAGE(CsvStorage, ".csv");
    };
}
