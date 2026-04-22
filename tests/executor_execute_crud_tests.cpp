#include "doctest.h"

#include "test_support.h"

TEST_SUITE_BEGIN("Executor::execute");

TEST_CASE("insert with explicit columns fills unspecified values with empty strings")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, category, text, done);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos (title, done) VALUES ('Buy milk', true);"));

    const auto table = context.storage->load_table("todos");
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(table.rows[0][0], "Buy milk");
    CHECK_EQ(table.rows[0][1], "");
    CHECK_EQ(table.rows[0][2], "");
    CHECK_EQ(table.rows[0][3], "true");
}

TEST_CASE("update modifies only matching rows")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, category, text, done);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('Buy milk', 'home', '2 liters', 'false');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('Write docs', 'work', 'API docs', 'false');"));
    context.executor.execute(sql_test::parse_statement("UPDATE todos SET done = true WHERE title = 'Buy milk';"));

    const auto table = context.storage->load_table("todos");
    REQUIRE_EQ(table.rows.size(), 2U);
    CHECK_EQ(table.rows[0][3], "true");
    CHECK_EQ(table.rows[1][3], "false");
    CHECK(context.output.str().find("Updated 1 row(s) in 'todos'") != std::string::npos);
}

TEST_CASE("update without where modifies all rows")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, done);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('A', false);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('B', false);"));
    context.executor.execute(sql_test::parse_statement("UPDATE todos SET done = true;"));

    const auto table = context.storage->load_table("todos");
    REQUIRE_EQ(table.rows.size(), 2U);
    CHECK_EQ(table.rows[0][1], "true");
    CHECK_EQ(table.rows[1][1], "true");
    CHECK(context.output.str().find("Updated 2 row(s) in 'todos'") != std::string::npos);
}

TEST_CASE("drops table from memory storage")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, done);"));
    context.executor.execute(sql_test::parse_statement("DROP TABLE todos;"));

    CHECK(context.output.str().find("Dropped table 'todos'") != std::string::npos);
    CHECK_THROWS_AS(context.storage->load_table("todos"), std::runtime_error);
}

TEST_CASE("rejects duplicate table creation")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, done);"));
    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, done);")), std::runtime_error);
}

TEST_CASE("rejects unknown columns")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, done);"));
    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("SELECT missing FROM todos;")), std::runtime_error);
    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("UPDATE todos SET missing = true;")), std::runtime_error);
    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("INSERT INTO todos (missing) VALUES (true);")), std::runtime_error);
}

TEST_CASE("rejects mismatched insert value counts")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, done);"));
    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('Buy milk');")), std::runtime_error);
    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("INSERT INTO todos (title) VALUES ('Buy milk', true);")), std::runtime_error);
}

TEST_CASE("rejects operations on missing tables")
{
    sql_test::ExecutorContext context;

    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("SELECT * FROM missing;")), std::runtime_error);
    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("INSERT INTO missing VALUES ('x');")), std::runtime_error);
    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("UPDATE missing SET value = 1;")), std::runtime_error);
}

TEST_CASE("rejects dropping missing table")
{
    sql_test::ExecutorContext context;

    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("DROP TABLE missing;")), std::runtime_error);
}

TEST_CASE("deletes matching rows with where")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, done);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('Buy milk', true);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('Write docs', false);"));
    context.executor.execute(sql_test::parse_statement("DELETE FROM todos WHERE done = true;"));

    const auto table = context.storage->load_table("todos");
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(table.rows[0][0], "Write docs");
    CHECK(context.output.str().find("Deleted 1 row(s) from 'todos'") != std::string::npos);
}

TEST_CASE("deletes all rows without where")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, done);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('Buy milk', true);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('Write docs', false);"));
    context.executor.execute(sql_test::parse_statement("DELETE FROM todos;"));

    const auto table = context.storage->load_table("todos");
    CHECK(table.rows.empty());
    CHECK(context.output.str().find("Deleted 2 row(s) from 'todos'") != std::string::npos);
}

TEST_CASE("drops csv-backed table files")
{
    const sql_test::TempDirectoryGuard temp_dir("sql_doctest_drop");
    const auto csv_path = temp_dir.path / "todos.csv";

    auto storage = std::make_shared<sql::CsvStorage>(temp_dir.path);
    std::ostringstream output;
    sql::Executor executor(storage, output);

    executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, done);"));
    REQUIRE(std::filesystem::exists(csv_path));
    executor.execute(sql_test::parse_statement("DROP TABLE todos;"));
    CHECK_FALSE(std::filesystem::exists(csv_path));
}

TEST_SUITE_END();

