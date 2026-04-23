#include "StorageRegistry.h"

#include "SqlError.h"

#include <algorithm>
#include <cctype>
#include <utility>

namespace fsql
{
    namespace
    {
        std::string normalize_extension(std::string extension)
        {
            std::transform(extension.begin(), extension.end(), extension.begin(), [](unsigned char ch)
            {
                return static_cast<char>(std::tolower(ch));
            });
            return extension;
        }
    }

    StorageRegistry& StorageRegistry::instance()
    {
        static StorageRegistry registry;
        return registry;
    }

    void StorageRegistry::register_storage(std::vector<std::string> extensions, Factory factory)
    {
        if (extensions.empty())
        {
            fail("Storage registration requires at least one extension");
        }

        for (auto& extension : extensions)
        {
            extension = normalize_extension(std::move(extension));
            if (extension.empty() || extension.front() != '.')
            {
                fail("Storage registration requires extensions beginning with '.'");
            }
            if (supports_extension(extension))
            {
                fail("Storage already registered for extension: " + extension);
            }
        }

        registrations_.push_back({std::move(extensions), std::move(factory)});
    }

    bool StorageRegistry::supports_extension(std::string_view extension) const
    {
        const auto normalized_extension = normalize_extension(std::string(extension));
        return std::any_of(registrations_.begin(), registrations_.end(), [&](const Registration& registration)
        {
            return std::any_of(registration.extensions.begin(), registration.extensions.end(), [&](const std::string& registered_extension)
            {
                return registered_extension == normalized_extension;
            });
        });
    }

    std::shared_ptr<IStorage> StorageRegistry::create_storage(std::string_view extension, std::filesystem::path root_directory) const
    {
        const auto normalized_extension = normalize_extension(std::string(extension));
        for (const auto& registration : registrations_)
        {
            if (std::any_of(registration.extensions.begin(), registration.extensions.end(), [&](const std::string& registered_extension)
            {
                return registered_extension == normalized_extension;
            }))
            {
                return registration.factory(std::move(root_directory));
            }
        }

        fail("Unsupported table format: " + normalized_extension);
    }

    std::vector<std::string> StorageRegistry::extensions() const
    {
        std::vector<std::string> registered_extensions;
        for (const auto& registration : registrations_)
        {
            registered_extensions.insert(registered_extensions.end(), registration.extensions.begin(), registration.extensions.end());
        }
        return registered_extensions;
    }
}

