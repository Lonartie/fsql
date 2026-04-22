#include "doctest.h"

#include "test_support.h"

#include <chrono>
#include <thread>

TEST_SUITE_BEGIN("Executor::execute");

TEST_CASE("auto increments omitted id column")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (id AUTO_INCREMENT, title, done);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos (title, done) VALUES ('Buy milk', false);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos (title, done) VALUES ('Write docs', true);"));

    const auto table = context.storage->load_table("todos");
    REQUIRE_EQ(table.rows.size(), 2U);
    CHECK_EQ(table.rows[0][0], "1");
    CHECK_EQ(table.rows[1][0], "2");
}

TEST_CASE("applies literal default values on insert")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE dates (creation_date = 'today', label = 'new');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO dates VALUES ();"));

    const auto table = context.storage->load_table("dates");
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(table.rows[0][0], "today");
    CHECK_EQ(table.rows[0][1], "new");
}

TEST_CASE("applies NOW default values on insert")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE dates (creation_date = NOW(), label = 'new');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO dates (label) VALUES ('custom');"));

    const auto table = context.storage->load_table("dates");
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_FALSE(table.rows[0][0].empty());
    CHECK_EQ(table.rows[0][1], "custom");
}

TEST_CASE("stores NOW default as expression and evaluates it on insert")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE dates (creation_date = NOW(), label = 'new');"));
    const auto created_table = context.storage->load_table("dates");
    CHECK(created_table.columns[0].find("DEFAULT(NOW())") != std::string::npos);

    std::this_thread::sleep_for(std::chrono::seconds(1));
    context.executor.execute(sql_test::parse_statement("INSERT INTO dates (label) VALUES ('custom');"));

    const auto inserted_table = context.storage->load_table("dates");
    REQUIRE_EQ(inserted_table.rows.size(), 1U);
    CHECK_FALSE(inserted_table.rows[0][0].empty());
    CHECK_NE(inserted_table.rows[0][0], "NOW()");
    CHECK_EQ(inserted_table.rows[0][1], "custom");
}

TEST_CASE("supports SELECT subqueries in default expressions")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE defaults (value);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO defaults VALUES ('fallback');"));
    context.executor.execute(sql_test::parse_statement("CREATE TABLE dest (value = (SELECT value FROM defaults));"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO dest VALUES ();"));

    const auto table = context.storage->load_table("dest");
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(table.rows[0][0], "fallback");
}

TEST_SUITE_END();

