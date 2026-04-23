#include "BuiltinStorageRegistration.h"

namespace fsql
{
    void register_csv_storage_backend();
    void register_json_storage_backend();
    void register_toml_storage_backend();
    void register_yaml_storage_backend();
    void register_xml_storage_backend();

    void register_builtin_storages()
    {
        static const bool registered = []()
        {
            register_csv_storage_backend();
            register_json_storage_backend();
            register_toml_storage_backend();
            register_yaml_storage_backend();
            register_xml_storage_backend();
            return true;
        }();
        static_cast<void>(registered);
    }
}

