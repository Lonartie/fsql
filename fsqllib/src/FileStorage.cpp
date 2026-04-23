#include "FileStorage.h"

#include "CsvStorage.h"
#include "FileStorageSupport.h"
#include "JsonStorage.h"
#include "StorageRegistry.h"
#include "SqlError.h"
#include "TomlStorage.h"
#include "XmlStorage.h"
#include "YamlStorage.h"

#include <algorithm>
#include <cctype>
#include <optional>
#include <sstream>
#include <utility>

namespace fsql
{
    namespace
    {
        struct ResolvedTableStorage
        {
            std::shared_ptr<IStorage> storage;
            std::filesystem::path path;
        };

        RowGenerator scan_with_owned_storage(std::shared_ptr<IStorage> storage, RelationReference reference)
        {
            auto rows = storage->scan_table(reference);
            for (const auto& row : rows)
            {
                co_yield row;
            }
        }

        std::string normalize_extension(const std::filesystem::path& path)
        {
            auto extension = path.extension().string();
            std::transform(extension.begin(), extension.end(), extension.begin(), [](unsigned char ch)
            {
                return static_cast<char>(std::tolower(ch));
            });
            return extension;
        }

        std::string table_logical_name_from_path(const std::filesystem::path& path)
        {
            return path.stem().string();
        }

        bool is_explicit_table_file_path(const RelationReference& reference, const std::filesystem::path& root_directory)
        {
            if (reference.kind != RelationReference::Kind::FilePath)
            {
                return false;
            }

            const auto path = std::filesystem::path(reference.name).is_absolute()
                ? std::filesystem::path(reference.name)
                : root_directory / reference.name;
            return !FileStorageSupport::is_view_path(path)
                && !path.extension().empty()
                && StorageRegistry::instance().supports_extension(normalize_extension(path));
        }

        std::string describe_ambiguous_candidates(const std::vector<ResolvedTableStorage>& candidates)
        {
            std::ostringstream output;
            for (std::size_t i = 0; i < candidates.size(); ++i)
            {
                if (i > 0)
                {
                    output << ", ";
                }
                output << candidates[i].path.string();
            }
            return output.str();
        }

        std::shared_ptr<IStorage> default_storage(std::filesystem::path root_directory)
        {
            return StorageRegistry::instance().create_storage(".csv", std::move(root_directory));
        }

        std::shared_ptr<IStorage> storage_for_path(const std::filesystem::path& path, std::filesystem::path root_directory)
        {
            return StorageRegistry::instance().create_storage(normalize_extension(path), std::move(root_directory));
        }

        std::vector<ResolvedTableStorage> existing_table_candidates(const std::filesystem::path& base_path,
                                                                    const std::filesystem::path& root_directory)
        {
            std::vector<ResolvedTableStorage> candidates;
            for (const auto& extension : StorageRegistry::instance().extensions())
            {
                const auto candidate_path = std::filesystem::path(base_path.string() + extension);
                if (!std::filesystem::exists(candidate_path))
                {
                    continue;
                }
                candidates.push_back({StorageRegistry::instance().create_storage(extension, root_directory), candidate_path});
            }
            return candidates;
        }

        ResolvedTableStorage resolve_existing_table_storage(const std::filesystem::path& base_path,
                                                            const std::string& display_name,
                                                            const std::filesystem::path& root_directory)
        {
            const auto candidates = existing_table_candidates(base_path, root_directory);
            if (candidates.empty())
            {
                fail("Table does not exist: " + display_name);
            }
            if (candidates.size() > 1)
            {
                fail("Ambiguous table reference '" + display_name + "': " + describe_ambiguous_candidates(candidates));
            }
            return candidates.front();
        }

        std::optional<ResolvedTableStorage> try_resolve_existing_explicit_table_storage(const std::filesystem::path& path,
                                                                                         const std::string& display_name,
                                                                                         const std::filesystem::path& root_directory,
                                                                                         bool throw_on_unsupported_extension)
        {
            if (FileStorageSupport::is_view_path(path))
            {
                return std::nullopt;
            }
            if (path.extension().empty())
            {
                return resolve_existing_table_storage(path, display_name, root_directory);
            }
            if (!StorageRegistry::instance().supports_extension(normalize_extension(path)))
            {
                if (throw_on_unsupported_extension)
                {
                    fail("Unsupported table format: " + display_name);
                }
                return std::nullopt;
            }
            if (!std::filesystem::exists(path))
            {
                return std::nullopt;
            }
            return ResolvedTableStorage{storage_for_path(path, root_directory), path};
        }

        ResolvedTableStorage resolve_existing_storage(const RelationReference& table_name, const std::filesystem::path& root_directory)
        {
            if (table_name.kind == RelationReference::Kind::FilePath)
            {
                if (const auto resolved = try_resolve_existing_explicit_table_storage(
                    std::filesystem::path(table_name.name).is_absolute() ? std::filesystem::path(table_name.name) : root_directory / table_name.name,
                    table_name.name,
                    root_directory,
                    true);
                    resolved.has_value())
                {
                    return *resolved;
                }
                fail("Table does not exist: " + table_name.name);
            }
            return resolve_existing_table_storage(root_directory / table_name.name, table_name.name, root_directory);
        }

        ResolvedTableStorage resolve_writable_storage(const std::filesystem::path& path, const std::filesystem::path& root_directory)
        {
            if (FileStorageSupport::is_view_path(path))
            {
                fail("Unsupported table format: " + path.string());
            }
            if (path.extension().empty())
            {
                const auto resolved_path = std::filesystem::path(path.string() + ".csv");
                return {default_storage(root_directory), resolved_path};
            }
            if (!StorageRegistry::instance().supports_extension(normalize_extension(path)))
            {
                fail("Unsupported table format: " + path.string());
            }
            return {storage_for_path(path, root_directory), path};
        }
    }

    FileStorage::FileStorage() : root_directory_(std::filesystem::current_path())
    {
    }

    FileStorage::FileStorage(std::filesystem::path root_directory) : root_directory_(std::move(root_directory))
    {
    }

    std::filesystem::path FileStorage::table_path(const RelationReference& table_name) const
    {
        return resolve_existing_storage(table_name, root_directory_).path;
    }

    bool FileStorage::has_table(const RelationReference& table_name) const
    {
        if (table_name.kind == RelationReference::Kind::FilePath)
        {
            const auto path = std::filesystem::path(table_name.name).is_absolute() ? std::filesystem::path(table_name.name) : root_directory_ / table_name.name;
            if (FileStorageSupport::is_view_path(path))
            {
                return false;
            }
            if (path.extension().empty())
            {
                const auto candidates = existing_table_candidates(path, root_directory_);
                if (candidates.size() > 1)
                {
                    fail("Ambiguous table reference '" + table_name.name + "': " + describe_ambiguous_candidates(candidates));
                }
                return !candidates.empty();
            }
            if (!StorageRegistry::instance().supports_extension(normalize_extension(path)))
            {
                return false;
            }
            return std::filesystem::exists(path);
        }

        const auto candidates = existing_table_candidates(root_directory_ / table_name.name, root_directory_);
        if (candidates.size() > 1)
        {
            fail("Ambiguous table reference '" + table_name.name + "': " + describe_ambiguous_candidates(candidates));
        }
        return !candidates.empty();
    }

    bool FileStorage::has_view(const RelationReference& view_name) const
    {
        if (is_explicit_table_file_path(view_name, root_directory_))
        {
            return false;
        }
        return default_storage(root_directory_)->has_view(view_name);
    }

    Table FileStorage::load_table(const RelationReference& table_name) const
    {
        const auto resolved_storage = resolve_existing_storage(table_name, root_directory_);
        return resolved_storage.storage->load_table(table_name);
    }

    Table FileStorage::describe_table(const RelationReference& table_name) const
    {
        const auto resolved_storage = resolve_existing_storage(table_name, root_directory_);
        return resolved_storage.storage->describe_table(table_name);
    }

    RowGenerator FileStorage::scan_table(const RelationReference& table_name) const
    {
        const auto resolved_storage = resolve_existing_storage(table_name, root_directory_);
        return resolved_storage.storage->scan_table(table_name);
    }

    ViewDefinition FileStorage::load_view(const RelationReference& view_name) const
    {
        if (is_explicit_table_file_path(view_name, root_directory_))
        {
            fail("View does not exist: " + view_name.name);
        }
        return default_storage(root_directory_)->load_view(view_name);
    }

    void FileStorage::save_table(const Table& table)
    {
        if (table.storage_path.has_value())
        {
            const auto path = table.storage_path->is_absolute() ? *table.storage_path : root_directory_ / *table.storage_path;
            auto resolved_storage = resolve_writable_storage(path, root_directory_);
            auto persisted_table = table;
            persisted_table.storage_path = resolved_storage.path;
            resolved_storage.storage->save_table(persisted_table);
            return;
        }

        default_storage(root_directory_)->save_table(table);
    }

    bool FileStorage::supports_append(const RelationReference& table_name) const
    {
        const auto resolved_storage = resolve_existing_storage(table_name, root_directory_);
        return resolved_storage.storage->supports_append(table_name);
    }

    void FileStorage::append_row(const RelationReference& table_name, const Table& table, const Row& row)
    {
        const auto resolved_storage = resolve_existing_storage(table_name, root_directory_);
        resolved_storage.storage->append_row(table_name, table, row);
    }

    std::string FileStorage::next_auto_increment_value_for_insert(const RelationReference& table_name,
                                                                  const Table& table,
                                                                  std::size_t index) const
    {
        const auto resolved_storage = resolve_existing_storage(table_name, root_directory_);
        return resolved_storage.storage->next_auto_increment_value_for_insert(table_name, table, index);
    }

    void FileStorage::save_view(const ViewDefinition& view)
    {
        if (view.storage_path.has_value())
        {
            const auto path = view.storage_path->is_absolute() ? *view.storage_path : root_directory_ / *view.storage_path;
            if (!FileStorageSupport::is_view_path(path)
                && !path.extension().empty()
                && StorageRegistry::instance().supports_extension(normalize_extension(path)))
            {
                fail("Unsupported view path: " + view.name);
            }
        }
        default_storage(root_directory_)->save_view(view);
    }

    void FileStorage::delete_table(const RelationReference& table_name)
    {
        const auto resolved_storage = resolve_existing_storage(table_name, root_directory_);
        resolved_storage.storage->delete_table(table_name);
    }

    void FileStorage::delete_view(const RelationReference& view_name)
    {
        if (is_explicit_table_file_path(view_name, root_directory_))
        {
            fail("View does not exist: " + view_name.name);
        }
        default_storage(root_directory_)->delete_view(view_name);
    }

    std::size_t FileStorage::column_index(const Table& table, const std::string& column) const
    {
        return default_storage(root_directory_)->column_index(table, column);
    }

    std::filesystem::path FileStorage::resolve_table_source_path(std::filesystem::path path)
    {
        return resolve_existing_storage({RelationReference::Kind::FilePath, path.string()}, std::filesystem::current_path()).path;
    }

    std::filesystem::path FileStorage::resolve_view_source_path(std::filesystem::path path)
    {
        return FileStorageSupport::resolve_view_source_path(std::move(path));
    }

    Table FileStorage::load_table_from_path(std::filesystem::path path)
    {
        const auto resolved_path = resolve_table_source_path(std::move(path));
        auto table = storage_for_path(resolved_path, std::filesystem::current_path())->load_table({RelationReference::Kind::FilePath, resolved_path.string()});
        table.name = table_logical_name_from_path(resolved_path);
        table.storage_path = resolved_path;
        return table;
    }

    Table FileStorage::describe_table_from_path(std::filesystem::path path)
    {
        const auto resolved_path = resolve_table_source_path(std::move(path));
        auto table = storage_for_path(resolved_path, std::filesystem::current_path())->describe_table({RelationReference::Kind::FilePath, resolved_path.string()});
        table.name = table_logical_name_from_path(resolved_path);
        table.storage_path = resolved_path;
        return table;
    }

    RowGenerator FileStorage::scan_table_from_path(std::filesystem::path path)
    {
        const auto resolved_path = resolve_table_source_path(std::move(path));
        auto storage = storage_for_path(resolved_path, std::filesystem::current_path());
        return scan_with_owned_storage(storage, {RelationReference::Kind::FilePath, resolved_path.string()});
    }

    ViewDefinition FileStorage::load_view_from_path(std::filesystem::path path)
    {
        if (!FileStorageSupport::is_view_path(path)
            && !path.extension().empty()
            && StorageRegistry::instance().supports_extension(normalize_extension(path)))
        {
            fail("View file does not exist: " + path.string());
        }
        const auto resolved_path = resolve_view_source_path(std::move(path));
        auto view = default_storage(std::filesystem::current_path())->load_view({RelationReference::Kind::FilePath, resolved_path.string()});
        view.name = FileStorageSupport::view_logical_name_from_path(resolved_path);
        view.storage_path = resolved_path;
        return view;
    }
}

