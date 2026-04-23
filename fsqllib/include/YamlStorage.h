#pragma once

#include "FileStorageSupport.h"
#include "StorageRegistry.h"

#include <fstream>

namespace fsql
{
    /// @brief Persists tables in YAML files.
    class YamlStorage final : public IStorage
    {
    public:
        YamlStorage();
        explicit YamlStorage(std::filesystem::path root_directory);

        std::filesystem::path table_path(const RelationReference& table_name) const override;
        bool has_table(const RelationReference& table_name) const override;
        bool has_view(const RelationReference& view_name) const override;
        Table load_table(const RelationReference& table_name) const override;
        Table describe_table(const RelationReference& table_name) const override;
        RowGenerator scan_table(const RelationReference& table_name) const override;
        ViewDefinition load_view(const RelationReference& view_name) const override;
        void save_table(const Table& table) override;
        void save_view(const ViewDefinition& view) override;
        void delete_table(const RelationReference& table_name) override;
        void delete_view(const RelationReference& view_name) override;
        std::size_t column_index(const Table& table, const std::string& column) const override;

    private:
        static Table load_table_from_stream(std::istream& input, const std::string& table_name);
        static Table describe_table_from_stream(std::istream& input, const std::string& table_name);
        static RowGenerator scan_table_from_file(std::ifstream input, std::string table_name);
        static void write_table_to_stream(std::ostream& output, const Table& table);

        FileStorageSupport storage_support_;
        FSQL_AUTO_REGISTER_STORAGE(YamlStorage, ".yaml", ".yml");
    };
}
