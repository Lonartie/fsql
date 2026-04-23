#pragma once

#include "IStorage.h"

#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace fsql
{
    /// @brief Registers file-backed table storage implementations by extension.
    class StorageRegistry
    {
    public:
        using Factory = std::function<std::shared_ptr<IStorage>(std::filesystem::path)>;

        struct Registration
        {
            std::vector<std::string> extensions;
            Factory factory;
        };

        /// @brief Returns the process-wide storage registry instance.
        /// @return Shared registry instance.
        static StorageRegistry& instance();

        /// @brief Registers a storage factory for one or more extensions.
        /// @param extensions Supported normalized extensions including the leading `.`.
        /// @param factory Factory used to instantiate the storage implementation for a root directory.
        void register_storage(std::vector<std::string> extensions, Factory factory);

        /// @brief Returns whether an extension is registered.
        /// @param extension Extension including the leading `.`.
        /// @return `true` when a storage implementation handles the extension.
        bool supports_extension(std::string_view extension) const;

        /// @brief Creates a storage instance for an extension.
        /// @param extension Extension including the leading `.`.
        /// @param root_directory Root directory supplied to the storage implementation.
        /// @return Newly created storage backend.
        std::shared_ptr<IStorage> create_storage(std::string_view extension, std::filesystem::path root_directory) const;

        /// @brief Returns all registered extensions.
        /// @return Registered extensions in registration order.
        std::vector<std::string> extensions() const;

    private:
        std::vector<Registration> registrations_;
    };
}

#define FSQL_AUTO_REGISTER_STORAGE(StorageType, ...) \
    inline static const bool storage_registered_ = []() \
    { \
        ::fsql::StorageRegistry::instance().register_storage({__VA_ARGS__}, [](std::filesystem::path root_directory) \
        { \
            return std::make_shared<::fsql::StorageType>(std::move(root_directory)); \
        }); \
        return true; \
    }()

