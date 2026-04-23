#include "doctest.h"

#include "test_support.h"

TEST_SUITE_BEGIN("CsvStorage::load_table");

TEST_CASE("rejects missing tables without writing files")
{
    const auto root = std::filesystem::temp_directory_path() / "csv_sql_missing_storage_root";
    sql::CsvStorage storage(root);

    CHECK_THROWS_AS(storage.load_table("todos"), std::runtime_error);
}

TEST_CASE("loads a table from an explicit file path with optional csv extension")
{
    const auto fixture = sql_test::fixture_path("file_source_tasks.csv");
    auto without_extension = fixture;
    without_extension.replace_extension();

    const auto loaded = sql::CsvStorage::load_table_from_path(without_extension);
    REQUIRE_EQ(loaded.columns.size(), 3U);
    REQUIRE_EQ(loaded.rows.size(), 3U);
    CHECK_EQ(loaded.name, "file_source_tasks");
    CHECK_EQ(loaded.rows[0][0], "Patch release");
    CHECK_EQ(loaded.rows[1][0], "Write docs");
    CHECK_EQ(loaded.rows[2][0], "Archive logs");
}

TEST_SUITE_END();

