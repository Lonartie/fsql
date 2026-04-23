#include "doctest.h"

#include "test_support.h"

TEST_SUITE_BEGIN("CsvStorage::parse_csv_line");

TEST_CASE("parses quoted values")
{
    const auto fields = fsql::CsvStorage::parse_csv_line("\"hello, \"\"world\"\"\",done");
    REQUIRE_EQ(fields.size(), 2U);
    CHECK_EQ(fields[0], "hello, \"world\"");
    CHECK_EQ(fields[1], "done");
}

TEST_CASE("rejects unterminated quoted fields")
{
    CHECK_THROWS_AS(fsql::CsvStorage::parse_csv_line("\"unterminated"), std::runtime_error);
}

TEST_SUITE_END();
