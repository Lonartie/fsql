#pragma once

#include "IStorage.h"

#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace fsql
{
    /// @brief Base implementation for file-backed storages that persist one table format.
    class FileStorageBase : public IStorage
    {
    public:
        explicit FileStorageBase(std::filesystem::path root_directory,
                                 std::string default_extension,
                                 std::vector<std::string> supported_extensions = {});

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

        /// @brief Returns whether a path names a persisted view definition.
        /// @param path Candidate filesystem path.
        /// @return `true` when the path ends with `.view.sql`.
        static bool is_view_path(const std::filesystem::path& path);

        /// @brief Resolves a direct view file source path, optionally appending `.view.sql`.
        /// @param path Candidate file path.
        /// @return Resolved file path.
        static std::filesystem::path resolve_view_source_path(std::filesystem::path path);

        /// @brief Returns the logical view name implied by a resolved view path.
        /// @param path Resolved view path.
        /// @return Logical view name.
        static std::string view_logical_name_from_path(const std::filesystem::path& path);

    protected:
        /// @brief Returns the default extension used when writing identifier-addressed tables.
        /// @return Extension including the leading `.`.
        const std::string& default_extension() const;

        /// @brief Returns the root directory used for relative table and view paths.
        /// @return Root directory.
        const std::filesystem::path& root_directory() const;

        /// @brief Returns whether a path extension is handled by this storage.
        /// @param path Candidate path.
        /// @return `true` when the extension is supported.
        bool supports_extension(const std::filesystem::path& path) const;

        /// @brief Parses a persisted table payload.
        /// @param text Serialized table content.
        /// @param table_name Logical or path-based table name.
        /// @return Materialized table.
        virtual Table parse_table_text(const std::string& text, const std::string& table_name) const = 0;

        /// @brief Serializes a table payload.
        /// @param table Table to persist.
        /// @return Serialized representation.
        virtual std::string serialize_table_text(const Table& table) const = 0;

        /// @brief Streams rows from a persisted table file.
        /// @param path Resolved table path.
        /// @param table_name Logical or path-based table name.
        /// @return Reopenable row stream.
        virtual RowGenerator scan_table_from_file(std::filesystem::path path, std::string table_name) const;

        /// @brief Resolves a relation reference against the storage root when needed.
        /// @param reference Relation reference.
        /// @return Rooted filesystem path.
        std::filesystem::path rooted_path(const RelationReference& reference) const;

        /// @brief Resolves a direct table file source path for this storage.
        /// @param path Candidate file path.
        /// @param throw_on_unsupported_extension Whether unsupported extensions should fail instead of returning no path.
        /// @return Resolved table path when found.
        std::optional<std::filesystem::path> try_resolve_existing_table_path(std::filesystem::path path,
                                                                             bool throw_on_unsupported_extension) const;

        /// @brief Resolves a direct table file path for reads or writes.
        /// @param table_name Logical table name or explicit file path.
        /// @param for_write Indicates whether the resolved path will be written.
        /// @return Resolved table path.
        std::filesystem::path resolve_table_path(const RelationReference& table_name, bool for_write) const;

        /// @brief Resolves a direct view definition file path for reads or writes.
        /// @param view_name Logical view name or explicit file path.
        /// @param for_write Indicates whether the resolved path will be written.
        /// @return Resolved view definition path.
        std::filesystem::path resolve_view_path(const RelationReference& view_name, bool for_write) const;

        /// @brief Resolves an existing identifier-addressed table path within this storage.
        /// @param table_name Logical table name.
        /// @return Resolved table path.
        std::filesystem::path resolve_existing_identifier_table_path(const std::string& table_name) const;


    private:
        std::vector<std::filesystem::path> existing_identifier_candidates(const std::filesystem::path& base_path) const;
        Table load_table_from_file(const std::filesystem::path& path, const std::string& table_name) const;

        std::filesystem::path root_directory_;
        std::string default_extension_;
        std::vector<std::string> supported_extensions_;
    };
}

