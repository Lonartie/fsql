#include "doctest.h"

#include "test_support.h"

TEST_SUITE_BEGIN("CsvStorage::load_table");

TEST_CASE("rejects missing tables without writing files")
{
    const auto root = std::filesystem::temp_directory_path() / "fsql_missing_storage_root";
    fsql::CsvStorage storage(root);

    CHECK_THROWS_AS(storage.load_table({fsql::RelationReference::Kind::Identifier, "todos"}), std::runtime_error);
}

TEST_CASE("loads a table from an explicit file path with optional csv extension")
{
    const auto fixture = fsql_test::fixture_path("file_source_tasks.csv");
    auto without_extension = fixture;
    without_extension.replace_extension();

    const auto loaded = fsql::FileStorage::load_table_from_path(without_extension);
    REQUIRE_EQ(loaded.columns.size(), 3U);
    REQUIRE_EQ(loaded.rows.size(), 3U);
    CHECK_EQ(loaded.name, "file_source_tasks");
    CHECK_EQ(loaded.rows[0][0], "Patch release");
    CHECK_EQ(loaded.rows[1][0], "Write docs");
    CHECK_EQ(loaded.rows[2][0], "Archive logs");
}

TEST_CASE("loads a view from an explicit file path with optional view suffix")
{
    fsql_test::TemporaryDirectory temp_directory;
    std::filesystem::create_directories(temp_directory.path / "tables");
    std::filesystem::create_directories(temp_directory.path / "views");

    fsql::FileStorage storage(temp_directory.path);
    fsql::Executor executor(std::make_shared<fsql::FileStorage>(temp_directory.path));
    const auto table_path = temp_directory.path / "tables" / "tasks";
    const auto view_path = temp_directory.path / "views" / "open_tasks";

    const auto create_table = executor.execute(fsql_test::parse_statement("CREATE TABLE '" + table_path.string() + "' (title, done);"));
    REQUIRE(create_table.success);
    REQUIRE(executor.execute(fsql_test::parse_statement("INSERT INTO '" + table_path.string() + "' VALUES ('Patch release', false);")).success);
    REQUIRE(executor.execute(fsql_test::parse_statement("CREATE VIEW '" + view_path.string() + "' AS SELECT title FROM '" + table_path.string() + "';")).success);

    const auto loaded = fsql::FileStorage::load_view_from_path(view_path);
    CHECK_EQ(loaded.name, "open_tasks");
    CHECK(loaded.select_statement.find("SELECT title FROM '") != std::string::npos);

    auto without_extension = view_path;
    without_extension.replace_extension();
    without_extension = without_extension.parent_path() / without_extension.stem();
    const auto loaded_without_suffix = storage.load_view({fsql::RelationReference::Kind::FilePath, without_extension.string()});
    CHECK_EQ(loaded_without_suffix.name, without_extension.string());
    CHECK(loaded_without_suffix.select_statement.find("SELECT title FROM '") != std::string::npos);
}

TEST_CASE("loads supported non-csv table formats from explicit paths without extensions")
{
    const std::vector<std::string> extensions = {".json", ".toml", ".yaml", ".xml"};

    for (const auto& extension : extensions)
    {
        SUBCASE(extension.c_str())
        {
            fsql_test::TemporaryDirectory temp_directory;
            auto storage = std::make_shared<fsql::FileStorage>(temp_directory.path);
            fsql::Executor executor(storage);

            const auto table_name = "tasks" + extension;
            auto create_result = executor.execute(fsql_test::parse_statement("CREATE TABLE " + table_name + " (title, done);"));
            REQUIRE(create_result.success);
            REQUIRE(executor.execute(fsql_test::parse_statement("INSERT INTO tasks VALUES ('Patch release', false);")).success);
            REQUIRE(executor.execute(fsql_test::parse_statement("INSERT INTO tasks VALUES ('Write docs', true);")).success);

            const auto loaded = fsql::FileStorage::load_table_from_path(temp_directory.path / "tasks");
            REQUIRE_EQ(loaded.columns.size(), 2U);
            REQUIRE_EQ(loaded.rows.size(), 2U);
            CHECK_EQ(loaded.name, "tasks");
            REQUIRE(loaded.storage_path.has_value());
            CHECK_EQ(loaded.storage_path->extension().string(), extension);
            CHECK_EQ(loaded.rows[0][0], "Patch release");
            CHECK_EQ(loaded.rows[1][0], "Write docs");
        }
    }
}

TEST_CASE("errors when table format auto-detection is ambiguous")
{
    fsql_test::TemporaryDirectory temp_directory;
    auto storage = std::make_shared<fsql::FileStorage>(temp_directory.path);
    fsql::Executor executor(storage);

    REQUIRE(executor.execute(fsql_test::parse_statement("CREATE TABLE tasks.json (title);")) .success);
    REQUIRE(executor.execute(fsql_test::parse_statement("CREATE TABLE tasks.yaml (title);")) .success);

    CHECK_THROWS_AS(storage->load_table({fsql::RelationReference::Kind::Identifier, "tasks"}), std::runtime_error);
    CHECK_THROWS_AS(fsql::FileStorage::load_table_from_path(temp_directory.path / "tasks"), std::runtime_error);
}

TEST_CASE("explicit table file paths with extensions are not treated as views")
{
    const std::vector<std::string> extensions = {".csv", ".json", ".toml", ".yaml", ".xml"};

    for (const auto& extension : extensions)
    {
        SUBCASE(extension.c_str())
        {
            fsql_test::TemporaryDirectory temp_directory;
            auto storage = std::make_shared<fsql::FileStorage>(temp_directory.path);
            fsql::Executor executor(storage);

            const auto table_path = temp_directory.path / ("tasks" + extension);
            REQUIRE(executor.execute(fsql_test::parse_statement("CREATE TABLE '" + table_path.string() + "' (title);" )).success);

            const fsql::RelationReference reference{fsql::RelationReference::Kind::FilePath, table_path.string()};
            CHECK(storage->has_table(reference));
            CHECK_FALSE(storage->has_view(reference));
            CHECK_THROWS_AS(storage->load_view(reference), std::runtime_error);
        }
    }
}

TEST_SUITE_END();

