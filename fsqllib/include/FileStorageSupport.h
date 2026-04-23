#pragma once

#include "IStorage.h"

#include <filesystem>
#include <fstream>
#include <functional>
#include <optional>
#include <string>
#include <vector>

namespace fsql
{
    /// @brief Shared helper for single-format file-backed storages.
    class FileStorageSupport
    {
    public:
        using TableReader = std::function<Table(std::istream&, const std::string&)>;
        using TableWriter = std::function<void(std::ostream&, const Table&)>;
        using TableScanner = std::function<RowGenerator(std::ifstream, std::string)>;

        explicit FileStorageSupport(std::filesystem::path root_directory,
                                    std::string default_extension,
                                    std::vector<std::string> supported_extensions = {});

        std::filesystem::path table_path(const RelationReference& table_name) const;
        bool has_table(const RelationReference& table_name) const;
        bool has_view(const RelationReference& view_name) const;
        Table load_table(const RelationReference& table_name, const TableReader& reader) const;
        Table describe_table(const RelationReference& table_name, const TableReader& reader) const;
        RowGenerator scan_table(const RelationReference& table_name,
                                const TableReader& reader,
                                const TableScanner& scanner) const;
        ViewDefinition load_view(const RelationReference& view_name) const;
        void save_table(const Table& table, const TableWriter& writer) const;
        bool supports_append(const RelationReference& table_name) const;
        void append_row(const RelationReference& table_name, const Table& table, const Row& row) const;
        std::optional<std::string> read_next_auto_increment_value(const RelationReference& table_name) const;
        void save_view(const ViewDefinition& view) const;
        void delete_table(const RelationReference& table_name) const;
        void delete_view(const RelationReference& view_name) const;
        std::size_t column_index(const Table& table, const std::string& column) const;

        const std::filesystem::path& root_directory() const;
        const std::string& default_extension() const;
        bool supports_extension(const std::filesystem::path& path) const;

        static bool is_view_path(const std::filesystem::path& path);
        static std::filesystem::path resolve_view_source_path(std::filesystem::path path);
        static std::string view_logical_name_from_path(const std::filesystem::path& path);

    private:
        std::filesystem::path append_journal_path(const std::filesystem::path& table_path) const;
        std::filesystem::path auto_increment_state_path(const std::filesystem::path& table_path) const;
        void append_journal_rows(Table& table, const std::filesystem::path& table_path) const;
        void rewrite_auto_increment_state(const std::filesystem::path& table_path, const Table& table) const;
        void advance_auto_increment_state(const std::filesystem::path& table_path, const Table& table, const Row& row) const;
        std::filesystem::path rooted_path(const RelationReference& reference) const;
        std::optional<std::filesystem::path> try_resolve_existing_table_path(std::filesystem::path path,
                                                                             bool throw_on_unsupported_extension) const;
        std::filesystem::path resolve_table_path(const RelationReference& table_name, bool for_write) const;
        std::filesystem::path resolve_view_path(const RelationReference& view_name, bool for_write) const;
        std::filesystem::path resolve_existing_identifier_table_path(const std::string& table_name) const;
        std::vector<std::filesystem::path> existing_identifier_candidates(const std::filesystem::path& base_path) const;

        std::filesystem::path root_directory_;
        std::string default_extension_;
        std::vector<std::string> supported_extensions_;
    };
}

