#pragma once

#include "IStorage.h"

#include <filesystem>

namespace fsql
{
    /// @brief Composite file-backed storage that routes table operations by registered extension.
    class FileStorage final : public IStorage
    {
    public:
        FileStorage();
        explicit FileStorage(std::filesystem::path root_directory);

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

        /// @brief Resolves a direct table file source path, optionally appending or auto-detecting a supported extension.
        /// @param path Candidate file path.
        /// @return Resolved file path.
        static std::filesystem::path resolve_table_source_path(std::filesystem::path path);

        /// @brief Resolves a direct view definition file path, optionally appending `.view.sql`.
        /// @param path Candidate file path.
        /// @return Resolved file path.
        static std::filesystem::path resolve_view_source_path(std::filesystem::path path);

        /// @brief Loads a table directly from a supported file path.
        /// @param path Table file path with an optional supported extension omitted.
        /// @return Loaded table.
        static Table load_table_from_path(std::filesystem::path path);

        /// @brief Loads table metadata directly from a supported file path.
        /// @param path Table file path with an optional supported extension omitted.
        /// @return Table metadata containing the logical name and columns.
        static Table describe_table_from_path(std::filesystem::path path);

        /// @brief Streams rows directly from a supported file path.
        /// @param path Table file path with an optional supported extension omitted.
        /// @return Row stream for the file contents.
        static RowGenerator scan_table_from_path(std::filesystem::path path);

        /// @brief Loads a view definition directly from a file path.
        /// @param path View definition file path with optional `.view.sql` extension omitted.
        /// @return Loaded view definition.
        static ViewDefinition load_view_from_path(std::filesystem::path path);

    private:
        std::filesystem::path root_directory_;
    };
}

