#include "doctest.h"

#include "JsonStorage.h"
#include "TomlStorage.h"
#include "XmlStorage.h"
#include "YamlStorage.h"
#include "test_support.h"

#include <functional>
#include <memory>
#include <string>
#include <vector>

TEST_SUITE_BEGIN("ConcreteStorage");

namespace
{
    struct StorageCase
    {
        std::string name;
        std::string default_extension;
        std::function<std::shared_ptr<fsql::IStorage>(const std::filesystem::path&)> create;
    };

    std::vector<fsql::Row> materialize_rows(fsql::IStorage& storage, const fsql::RelationReference& table_name)
    {
        std::vector<fsql::Row> rows;
        const fsql::SerialCoroExecutor coro_executor;
        coro_executor.drive_rows(storage.scan_table(table_name), [&](const fsql::Row& row)
        {
            rows.push_back(row);
            return true;
        });
        return rows;
    }

    const std::vector<StorageCase> storage_cases = {
        {"csv", ".csv", [](const std::filesystem::path& root)
        {
            return std::make_shared<fsql::CsvStorage>(root);
        }},
        {"json", ".json", [](const std::filesystem::path& root)
        {
            return std::make_shared<fsql::JsonStorage>(root);
        }},
        {"toml", ".toml", [](const std::filesystem::path& root)
        {
            return std::make_shared<fsql::TomlStorage>(root);
        }},
        {"yaml", ".yaml", [](const std::filesystem::path& root)
        {
            return std::make_shared<fsql::YamlStorage>(root);
        }},
        {"xml", ".xml", [](const std::filesystem::path& root)
        {
            return std::make_shared<fsql::XmlStorage>(root);
        }}
    };
}

TEST_CASE("concrete storage implementations persist tables directly")
{
    for (const auto& storage_case : storage_cases)
    {
        SUBCASE(storage_case.name.c_str())
        {
            fsql_test::TemporaryDirectory temp_directory;
            auto storage = storage_case.create(temp_directory.path);

            fsql::Table table;
            table.name = "tasks";
            table.columns = {"title", "done"};
            table.rows = {{"Patch release", "false"}, {"Write docs", "true"}};

            storage->save_table(table);

            const fsql::RelationReference reference{fsql::RelationReference::Kind::Identifier, "tasks"};
            CHECK(storage->has_table(reference));
            CHECK_EQ(storage->table_path(reference).extension().string(), storage_case.default_extension);
            CHECK(std::filesystem::exists(temp_directory.path / ("tasks" + storage_case.default_extension)));

            const auto described = storage->describe_table(reference);
            CHECK_EQ(described.name, "tasks");
            REQUIRE_EQ(described.columns.size(), 2U);
            CHECK(described.rows.empty());
            CHECK_EQ(described.columns[0], "title");
            CHECK_EQ(described.columns[1], "done");

            const auto loaded = storage->load_table(reference);
            REQUIRE(loaded.storage_path.has_value());
            CHECK_EQ(loaded.storage_path->extension().string(), storage_case.default_extension);
            REQUIRE_EQ(loaded.rows.size(), 2U);
            CHECK_EQ(loaded.rows[0][0], "Patch release");
            CHECK_EQ(loaded.rows[0][1], "false");
            CHECK_EQ(loaded.rows[1][0], "Write docs");
            CHECK_EQ(loaded.rows[1][1], "true");

            const auto scanned_rows = materialize_rows(*storage, reference);
            REQUIRE_EQ(scanned_rows.size(), 2U);
            CHECK_EQ(scanned_rows[0][0], "Patch release");
            CHECK_EQ(scanned_rows[1][0], "Write docs");

            CHECK_EQ(storage->column_index(loaded, "DONE"), 1U);

            storage->delete_table(reference);
            CHECK_FALSE(storage->has_table(reference));
            CHECK_FALSE(std::filesystem::exists(temp_directory.path / ("tasks" + storage_case.default_extension)));
        }
    }
}

TEST_CASE("concrete storage implementations persist views directly")
{
    for (const auto& storage_case : storage_cases)
    {
        SUBCASE(storage_case.name.c_str())
        {
            fsql_test::TemporaryDirectory temp_directory;
            auto storage = storage_case.create(temp_directory.path);

            fsql::ViewDefinition view;
            view.name = "open_tasks";
            view.select_statement = "SELECT title FROM tasks WHERE done = false";

            storage->save_view(view);

            const fsql::RelationReference reference{fsql::RelationReference::Kind::Identifier, "open_tasks"};
            CHECK(storage->has_view(reference));
            CHECK(std::filesystem::exists(temp_directory.path / "open_tasks.view.sql"));

            const auto loaded = storage->load_view(reference);
            CHECK_EQ(loaded.name, "open_tasks");
            CHECK_EQ(loaded.select_statement, "SELECT title FROM tasks WHERE done = false");

            storage->delete_view(reference);
            CHECK_FALSE(storage->has_view(reference));
            CHECK_FALSE(std::filesystem::exists(temp_directory.path / "open_tasks.view.sql"));
        }
    }
}

TEST_CASE("yaml storage supports explicit yml file paths")
{
    fsql_test::TemporaryDirectory temp_directory;
    fsql::YamlStorage storage(temp_directory.path);

    fsql::Table table;
    table.name = "tasks";
    table.storage_path = temp_directory.path / "tasks.yml";
    table.columns = {"title", "done"};
    table.rows = {{"Patch release", "false"}};

    storage.save_table(table);

    const fsql::RelationReference reference{fsql::RelationReference::Kind::FilePath, (temp_directory.path / "tasks.yml").string()};
    CHECK(storage.has_table(reference));
    const auto loaded = storage.load_table(reference);
    REQUIRE(loaded.storage_path.has_value());
    CHECK_EQ(loaded.storage_path->extension().string(), ".yml");
    REQUIRE_EQ(loaded.rows.size(), 1U);
    CHECK_EQ(loaded.rows[0][0], "Patch release");
}

TEST_SUITE_END();

