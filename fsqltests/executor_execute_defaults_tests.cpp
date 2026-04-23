#include "doctest.h"

#include "test_support.h"

#include <chrono>
#include <thread>

TEST_SUITE_BEGIN("Executor::execute");

TEST_CASE("auto increments omitted id column")
{
    fsql_test::ExecutorContext context;

    context.executor.execute(fsql_test::parse_statement("CREATE TABLE todos (id AUTO_INCREMENT, title, done);"));
    context.executor.execute(fsql_test::parse_statement("INSERT INTO todos (title, done) VALUES ('Buy milk', false);"));
    context.executor.execute(fsql_test::parse_statement("INSERT INTO todos (title, done) VALUES ('Write docs', true);"));

    const auto table = context.storage->load_table("todos");
    REQUIRE_EQ(table.rows.size(), 2U);
    CHECK_EQ(table.rows[0][0], "1");
    CHECK_EQ(table.rows[1][0], "2");
}

TEST_CASE("applies literal default values on insert")
{
    fsql_test::ExecutorContext context;

    context.executor.execute(fsql_test::parse_statement("CREATE TABLE dates (creation_date = 'today', label = 'new');"));
    context.executor.execute(fsql_test::parse_statement("INSERT INTO dates VALUES ();"));

    const auto table = context.storage->load_table("dates");
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(table.rows[0][0], "today");
    CHECK_EQ(table.rows[0][1], "new");
}

TEST_CASE("applies NOW default values on insert")
{
    fsql_test::ExecutorContext context;

    context.executor.execute(fsql_test::parse_statement("CREATE TABLE dates (creation_date = NOW(), label = 'new');"));
    context.executor.execute(fsql_test::parse_statement("INSERT INTO dates (label) VALUES ('custom');"));

    const auto table = context.storage->load_table("dates");
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_FALSE(table.rows[0][0].empty());
    CHECK_EQ(table.rows[0][1], "custom");
}

TEST_CASE("stores NOW default as expression and evaluates it on insert")
{
    fsql_test::ExecutorContext context;

    context.executor.execute(fsql_test::parse_statement("CREATE TABLE dates (creation_date = NOW(), label = 'new');"));
    const auto created_table = context.storage->load_table("dates");
    CHECK(created_table.columns[0].find("DEFAULT(NOW())") != std::string::npos);

    context.executor.execute(fsql_test::parse_statement("INSERT INTO dates (label) VALUES ('custom');"));

    const auto inserted_table = context.storage->load_table("dates");
    REQUIRE_EQ(inserted_table.rows.size(), 1U);
    CHECK_FALSE(inserted_table.rows[0][0].empty());
    CHECK_NE(inserted_table.rows[0][0], "NOW()");
    CHECK_EQ(inserted_table.rows[0][1], "custom");
}

TEST_CASE("supports SELECT subqueries in default expressions")
{
    fsql_test::ExecutorContext context;

    context.executor.execute(fsql_test::parse_statement("CREATE TABLE defaults (value);"));
    context.executor.execute(fsql_test::parse_statement("INSERT INTO defaults VALUES ('fallback');"));
    context.executor.execute(fsql_test::parse_statement("CREATE TABLE dest (value = (SELECT value FROM defaults));"));
    context.executor.execute(fsql_test::parse_statement("INSERT INTO dest VALUES ();"));

    const auto table = context.storage->load_table("dest");
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(table.rows[0][0], "fallback");
}

TEST_CASE("alter table add column backfills existing rows with default values")
{
    fsql_test::ExecutorContext context;

    context.executor.execute(fsql_test::parse_statement("CREATE TABLE todos (title);"));
    context.executor.execute(fsql_test::parse_statement("INSERT INTO todos VALUES ('Buy milk');"));
    context.executor.execute(fsql_test::parse_statement("INSERT INTO todos VALUES ('Write docs');"));
    context.executor.execute(fsql_test::parse_statement("ALTER TABLE todos ADD COLUMN category = 'backlog';"));

    const auto table = context.storage->load_table("todos");
    REQUIRE_EQ(table.rows.size(), 2U);
    CHECK_EQ(table.rows[0][1], "backlog");
    CHECK_EQ(table.rows[1][1], "backlog");
}

TEST_CASE("alter table set and drop default affects future inserts")
{
    fsql_test::ExecutorContext context;

    context.executor.execute(fsql_test::parse_statement("CREATE TABLE todos (title, category);"));
    context.executor.execute(fsql_test::parse_statement("ALTER TABLE todos ALTER COLUMN category SET DEFAULT 'backlog';"));
    context.executor.execute(fsql_test::parse_statement("INSERT INTO todos (title) VALUES ('Buy milk');"));
    context.executor.execute(fsql_test::parse_statement("ALTER TABLE todos ALTER COLUMN category DROP DEFAULT;"));
    context.executor.execute(fsql_test::parse_statement("INSERT INTO todos (title, category) VALUES ('Write docs', 'docs');"));

    const auto table = context.storage->load_table("todos");
    REQUIRE_EQ(table.rows.size(), 2U);
    CHECK_EQ(table.rows[0][1], "backlog");
    CHECK_EQ(table.rows[1][1], "docs");
    CHECK(table.columns[1].find("DEFAULT(") == std::string::npos);
}

TEST_CASE("alter table can enable auto increment on an existing column")
{
    fsql_test::ExecutorContext context;

    context.executor.execute(fsql_test::parse_statement("CREATE TABLE todos (id, title);"));
    context.executor.execute(fsql_test::parse_statement("INSERT INTO todos VALUES ('', 'Buy milk');"));
    context.executor.execute(fsql_test::parse_statement("INSERT INTO todos VALUES (5, 'Write docs');"));
    context.executor.execute(fsql_test::parse_statement("ALTER TABLE todos ALTER COLUMN id SET AUTO_INCREMENT;"));
    context.executor.execute(fsql_test::parse_statement("INSERT INTO todos (title) VALUES ('Patch release');"));

    const auto table = context.storage->load_table("todos");
    REQUIRE_EQ(table.rows.size(), 3U);
    CHECK_EQ(table.rows[0][0], "6");
    CHECK_EQ(table.rows[1][0], "5");
    CHECK_EQ(table.rows[2][0], "7");
}

TEST_SUITE_END();

