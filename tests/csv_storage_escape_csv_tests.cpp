#include "doctest.h"

#include "test_support.h"

TEST_SUITE_BEGIN("CsvStorage::escape_csv");

TEST_CASE("escapes quoted values")
{
    const auto escaped = sql::CsvStorage::escape_csv("hello, \"world\"");
    CHECK_EQ(escaped, "\"hello, \"\"world\"\"\"");
}

TEST_SUITE_END();

