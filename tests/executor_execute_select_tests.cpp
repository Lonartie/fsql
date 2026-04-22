#include "doctest.h"

#include "test_support.h"

TEST_SUITE_BEGIN("Executor::execute");

TEST_CASE("create insert and select workflow")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, category, text, done);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('Buy milk', 'home', '2 liters', 'false');"));
    context.executor.execute(sql_test::parse_statement("SELECT category, text FROM todos;"));

    const auto text = context.output.str();
    CHECK(text.find("Created table 'todos'") != std::string::npos);
    CHECK(text.find("Inserted 1 row into 'todos'") != std::string::npos);
    CHECK(text.find("home") != std::string::npos);
    CHECK(text.find("2 liters") != std::string::npos);
    CHECK(text.find("1 row(s) selected") != std::string::npos);
}

TEST_CASE("select where filters rows")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, category, text, done);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('Buy milk', 'home', '2 liters', 'false');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('Write docs', 'work', 'API docs', 'false');"));
    context.reset_output();

    context.executor.execute(sql_test::parse_statement("SELECT title FROM todos WHERE category = 'work';"));

    const auto text = context.output.str();
    CHECK(text.find("Write docs") != std::string::npos);
    CHECK(text.find("Buy milk") == std::string::npos);
    CHECK(text.find("1 row(s) selected") != std::string::npos);
}

TEST_CASE("select all returns all columns")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, category);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('Buy milk', 'home');"));
    context.reset_output();

    context.executor.execute(sql_test::parse_statement("SELECT * FROM todos;"));

    const auto text = context.output.str();
    CHECK(text.find("title") != std::string::npos);
    CHECK(text.find("category") != std::string::npos);
    CHECK(text.find("Buy milk") != std::string::npos);
    CHECK(text.find("home") != std::string::npos);
}

TEST_CASE("select escapes csv output")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, text);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('Buy milk', 'hello, world');"));
    context.reset_output();

    context.executor.execute(sql_test::parse_statement("SELECT text FROM todos;"));

    CHECK(context.output.str().find("hello, world") != std::string::npos);
}

TEST_CASE("pretty formats select output")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, category);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('Buy milk', 'home');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('Write docs', 'work');"));
    context.reset_output();

    context.executor.execute(sql_test::parse_statement("SELECT title, category FROM todos;"));

    const auto text = context.output.str();
    CHECK(text.find("+------------+----------+") != std::string::npos);
    CHECK(text.find("| title      | category |") != std::string::npos);
    CHECK(text.find("| Buy milk   | home     |") != std::string::npos);
    CHECK(text.find("| Write docs | work     |") != std::string::npos);
    CHECK(text.find("| title|") == std::string::npos);
    CHECK(text.find("| Buy milk|") == std::string::npos);
    CHECK(text.find("2 row(s) selected") != std::string::npos);
}

TEST_CASE("supports typical aggregate functions on filtered rows")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE metrics (category, value, label);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO metrics VALUES ('ops', 10, 'alpha');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO metrics VALUES ('ops', 20, 'beta');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO metrics VALUES ('ops', '', 'gamma');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO metrics VALUES ('docs', 7, 'delta');"));
    context.reset_output();

    context.executor.execute(sql_test::parse_statement("SELECT COUNT(*), COUNT(value), SUM(value), AVG(value), MIN(value), MAX(value) FROM metrics WHERE category = 'ops';"));

    const auto text = context.output.str();
    CHECK(text.find("COUNT(*)") != std::string::npos);
    CHECK(text.find("COUNT(value)") != std::string::npos);
    CHECK(text.find("SUM(value)") != std::string::npos);
    CHECK(text.find("AVG(value)") != std::string::npos);
    CHECK(text.find("MIN(value)") != std::string::npos);
    CHECK(text.find("MAX(value)") != std::string::npos);
    CHECK(text.find("| 3        | 2            | 30         | 15         | 10         | 20         |") != std::string::npos);
    CHECK(text.find("1 row(s) selected") != std::string::npos);
}

TEST_CASE("supports MIN and MAX on text projections")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE metrics (category, label);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO metrics VALUES ('ops', 'zulu');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO metrics VALUES ('ops', 'alpha');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO metrics VALUES ('ops', 'mango');"));
    context.reset_output();

    context.executor.execute(sql_test::parse_statement("SELECT MIN(label), MAX(label) FROM metrics WHERE category = 'ops';"));

    const auto text = context.output.str();
    CHECK(text.find("| alpha      | zulu       |") != std::string::npos);
    CHECK(text.find("1 row(s) selected") != std::string::npos);
}

TEST_CASE("rejects mixing aggregate and non aggregate projections without grouping")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE metrics (category, value);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO metrics VALUES ('ops', 10);"));

    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("SELECT category, COUNT(*) FROM metrics;")), std::runtime_error);
}

TEST_SUITE_END();

