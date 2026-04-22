#include "doctest.h"

#include "test_support.h"

TEST_SUITE_BEGIN("CsvStorage::load_table");

TEST_CASE("round-trips a table on disk")
{
    const sql_test::TempDirectoryGuard temp_dir("sql_doctest_roundtrip");

    sql::CsvStorage storage(temp_dir.path);
    sql::Table table{"todos", {"title", "text"}, {{"Buy milk", "hello, world"}}};
    storage.save_table(table);

    const auto loaded = storage.load_table("todos");
    REQUIRE_EQ(loaded.rows.size(), 1U);
    CHECK_EQ(loaded.rows[0][0], "Buy milk");
    CHECK_EQ(loaded.rows[0][1], "hello, world");
}

TEST_SUITE_END();

