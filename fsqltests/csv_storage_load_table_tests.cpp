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

    const auto loaded = fsql::CsvStorage::load_table_from_path(without_extension);
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

    fsql::CsvStorage storage(temp_directory.path);
    fsql::Executor executor(std::make_shared<fsql::CsvStorage>(temp_directory.path));
    const auto table_path = temp_directory.path / "tables" / "tasks";
    const auto view_path = temp_directory.path / "views" / "open_tasks";

    const auto create_table = executor.execute(fsql_test::parse_statement("CREATE TABLE '" + table_path.string() + "' (title, done);"));
    REQUIRE(create_table.success);
    REQUIRE(executor.execute(fsql_test::parse_statement("INSERT INTO '" + table_path.string() + "' VALUES ('Patch release', false);")).success);
    REQUIRE(executor.execute(fsql_test::parse_statement("CREATE VIEW '" + view_path.string() + "' AS SELECT title FROM '" + table_path.string() + "';")).success);

    const auto loaded = fsql::CsvStorage::load_view_from_path(view_path);
    CHECK_EQ(loaded.name, "open_tasks");
    CHECK(loaded.select_statement.find("SELECT title FROM '") != std::string::npos);

    auto without_extension = view_path;
    without_extension.replace_extension();
    without_extension = without_extension.parent_path() / without_extension.stem();
    const auto loaded_without_suffix = storage.load_view({fsql::RelationReference::Kind::FilePath, without_extension.string()});
    CHECK_EQ(loaded_without_suffix.name, without_extension.string());
    CHECK(loaded_without_suffix.select_statement.find("SELECT title FROM '") != std::string::npos);
}

TEST_SUITE_END();

