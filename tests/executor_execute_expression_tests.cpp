#include "doctest.h"

#include "test_support.h"

TEST_SUITE_BEGIN("Executor::execute");

TEST_CASE("supports NOW() in insert values")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE events (id, created_at, text);"));
    CHECK_NOTHROW(context.executor.execute(sql_test::parse_statement("INSERT INTO events VALUES (1, NOW(), 'created');")));

    const auto table = context.storage->load_table("events");
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_FALSE(table.rows[0][1].empty());
    CHECK_EQ(table.rows[0][2], "created");
}

TEST_CASE("supports arithmetic expressions in WHERE")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE nums (a, b, label);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO nums VALUES (2, 3, 'match');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO nums VALUES (2, 2, 'miss');"));
    context.reset_output();

    context.executor.execute(sql_test::parse_statement("SELECT label FROM nums WHERE a + b * 2 = 8;"));

    const auto text = context.output.str();
    CHECK(text.find("match") != std::string::npos);
    CHECK(text.find("miss") == std::string::npos);
}

TEST_CASE("supports comparison and logical expressions in WHERE")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE nums (a, b, label);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO nums VALUES (5, 1, 'left');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO nums VALUES (1, 5, 'right');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO nums VALUES (1, 1, 'none');"));
    context.reset_output();

    context.executor.execute(sql_test::parse_statement("SELECT label FROM nums WHERE (a > b && a >= 5) || (b > a && b >= 5);"));

    const auto text = context.output.str();
    CHECK(text.find("left") != std::string::npos);
    CHECK(text.find("right") != std::string::npos);
    CHECK(text.find("none") == std::string::npos);
}

TEST_CASE("supports bitwise expressions in WHERE")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE flags (value, label);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO flags VALUES (6, 'has-bit-2');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO flags VALUES (1, 'no-bit-2');"));
    context.reset_output();

    context.executor.execute(sql_test::parse_statement("SELECT label FROM flags WHERE (value & 2) = 2;"));

    const auto text = context.output.str();
    CHECK(text.find("has-bit-2") != std::string::npos);
    CHECK(text.find("no-bit-2") == std::string::npos);
}

TEST_CASE("supports SELECT subqueries in WHERE expressions")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, category);"));
    context.executor.execute(sql_test::parse_statement("CREATE TABLE defaults (value);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('Buy milk', 'home');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('Write docs', 'work');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO defaults VALUES ('work');"));
    context.reset_output();

    context.executor.execute(sql_test::parse_statement("SELECT title FROM todos WHERE category = (SELECT value FROM defaults);"));

    const auto text = context.output.str();
    CHECK(text.find("Write docs") != std::string::npos);
    CHECK(text.find("Buy milk") == std::string::npos);
    CHECK(text.find("1 row(s) selected") != std::string::npos);
}

TEST_CASE("supports SELECT subqueries in INSERT values")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE source (value);"));
    context.executor.execute(sql_test::parse_statement("CREATE TABLE dest (value);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO source VALUES ('copied');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO dest VALUES ((SELECT value FROM source));"));

    const auto table = context.storage->load_table("dest");
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(table.rows[0][0], "copied");
}

TEST_CASE("supports SELECT subqueries in UPDATE assignments")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, category);"));
    context.executor.execute(sql_test::parse_statement("CREATE TABLE defaults (value);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('Buy milk', 'home');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO defaults VALUES ('done');"));
    context.executor.execute(sql_test::parse_statement("UPDATE todos SET category = (SELECT value FROM defaults) WHERE title = 'Buy milk';"));

    const auto table = context.storage->load_table("todos");
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(table.rows[0][1], "done");
}

TEST_CASE("rejects SELECT subqueries returning no rows")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE source (value);"));
    context.executor.execute(sql_test::parse_statement("CREATE TABLE dest (value);"));

    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("INSERT INTO dest VALUES ((SELECT value FROM source));")), std::runtime_error);
}

TEST_CASE("rejects SELECT subqueries returning multiple rows")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE source (value);"));
    context.executor.execute(sql_test::parse_statement("CREATE TABLE dest (value);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO source VALUES ('a');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO source VALUES ('b');"));

    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("INSERT INTO dest VALUES ((SELECT value FROM source));")), std::runtime_error);
}

TEST_CASE("rejects SELECT subqueries returning multiple columns")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE source (value, label);"));
    context.executor.execute(sql_test::parse_statement("CREATE TABLE dest (value);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO source VALUES ('a', 'b');"));

    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("INSERT INTO dest VALUES ((SELECT value, label FROM source));")), std::runtime_error);
}

TEST_SUITE_END();

