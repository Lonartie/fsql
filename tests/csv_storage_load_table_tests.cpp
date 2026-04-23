#include "doctest.h"

#include "test_support.h"

TEST_SUITE_BEGIN("CsvStorage::load_table");

TEST_CASE("rejects missing tables without writing files")
{
    const auto root = std::filesystem::temp_directory_path() / "csv_sql_missing_storage_root";
    sql::CsvStorage storage(root);

    CHECK_THROWS_AS(storage.load_table("todos"), std::runtime_error);
}

TEST_SUITE_END();

