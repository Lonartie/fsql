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

TEST_CASE("select supports multiple sources with qualified column references")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE tasks (id, title, team_id);"));
    context.executor.execute(sql_test::parse_statement("CREATE TABLE teams (id, name);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES (1, 'Patch release', 10);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES (2, 'Write docs', 20);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO teams VALUES (10, 'ops');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO teams VALUES (20, 'docs');"));
    context.reset_output();

    context.executor.execute(sql_test::parse_statement(
        "SELECT tasks.title, teams.name FROM tasks, teams WHERE tasks.team_id = teams.id ORDER BY tasks.title ASC;"));

    const auto text = context.output.str();
    CHECK(text.find("Patch release") != std::string::npos);
    CHECK(text.find("ops") != std::string::npos);
    CHECK(text.find("Write docs") != std::string::npos);
    CHECK(text.find("docs") != std::string::npos);
    CHECK(text.find("2 row(s) selected") != std::string::npos);
}

TEST_CASE("select star qualifies headers for multiple sources")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE tasks (id, team_id);"));
    context.executor.execute(sql_test::parse_statement("CREATE TABLE teams (id, name);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES (1, 10);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO teams VALUES (10, 'ops');"));
    context.reset_output();

    context.executor.execute(sql_test::parse_statement("SELECT * FROM tasks, teams WHERE tasks.team_id = teams.id;"));

    const auto text = context.output.str();
    CHECK(text.find("tasks.id") != std::string::npos);
    CHECK(text.find("tasks.team_id") != std::string::npos);
    CHECK(text.find("teams.id") != std::string::npos);
    CHECK(text.find("teams.name") != std::string::npos);
    CHECK(text.find("1 row(s) selected") != std::string::npos);
}

TEST_CASE("select supports source subqueries with aliases")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE tasks (title, team_id);"));
    context.executor.execute(sql_test::parse_statement("CREATE TABLE teams (id, name);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Patch release', 10);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Write docs', 20);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO teams VALUES (10, 'ops');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO teams VALUES (20, 'docs');"));
    context.reset_output();

    context.executor.execute(sql_test::parse_statement(
        "SELECT t.title, lookup.name FROM tasks t, (SELECT id, name FROM teams) lookup WHERE t.team_id = lookup.id ORDER BY t.title;"));

    const auto text = context.output.str();
    CHECK(text.find("Patch release") != std::string::npos);
    CHECK(text.find("Write docs") != std::string::npos);
    CHECK(text.find("ops") != std::string::npos);
    CHECK(text.find("docs") != std::string::npos);
    CHECK(text.find("2 row(s) selected") != std::string::npos);
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

TEST_CASE("orders selected rows by multiple expressions")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE tasks (title, priority, effort);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Bravo', 5, 1);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Alpha', 5, 2);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Delta', 3, 9);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Charlie', 8, 1);"));
    context.reset_output();

    context.executor.execute(sql_test::parse_statement("SELECT title, priority FROM tasks ORDER BY priority DESC, title ASC;"));

    const auto text = context.output.str();
    const auto charlie = text.find("| Charlie |");
    const auto alpha = text.find("| Alpha   |");
    const auto bravo = text.find("| Bravo   |");
    const auto delta = text.find("| Delta   |");
    REQUIRE(charlie != std::string::npos);
    REQUIRE(alpha != std::string::npos);
    REQUIRE(bravo != std::string::npos);
    REQUIRE(delta != std::string::npos);
    CHECK(charlie < alpha);
    CHECK(alpha < bravo);
    CHECK(bravo < delta);
    CHECK(text.find("4 row(s) selected") != std::string::npos);
}

TEST_CASE("select distinct removes duplicate projected rows")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE tasks (title, category, priority);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Patch release', 'ops', 8);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Night watch', 'ops', 4);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Write docs', 'docs', 3);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('API review', 'docs', 6);"));
    context.reset_output();

    context.executor.execute(sql_test::parse_statement("SELECT DISTINCT category FROM tasks ORDER BY category ASC;"));

    const auto text = context.output.str();
    CHECK(text.find("| docs     |") != std::string::npos);
    CHECK(text.find("| ops      |") != std::string::npos);
    CHECK(text.find("2 row(s) selected") != std::string::npos);
}

TEST_CASE("select applies offset and limit after ordering")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE tasks (title, priority);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('First', 9);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Second', 7);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Third', 5);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Fourth', 3);"));
    context.reset_output();

    context.executor.execute(sql_test::parse_statement("SELECT title, priority FROM tasks ORDER BY priority DESC LIMIT 2 OFFSET 1;"));

    const auto text = context.output.str();
    CHECK(text.find("| Second |") != std::string::npos);
    CHECK(text.find("| Third  |") != std::string::npos);
    CHECK(text.find("| First  |") == std::string::npos);
    CHECK(text.find("| Fourth |") == std::string::npos);
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

TEST_CASE("groups rows and filters groups with HAVING")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE tasks (team, owner, points, done);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('ops', 'alice', 8, false);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('ops', 'bob', 5, false);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('docs', 'cara', 3, false);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('docs', 'dave', 4, true);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('sec', 'erin', 9, false);"));
    context.reset_output();

    context.executor.execute(sql_test::parse_statement(
        "SELECT team, COUNT(*), SUM(points), AVG(points) "
        "FROM tasks WHERE done = false "
        "GROUP BY team HAVING COUNT(*) >= 2 "
        "ORDER BY SUM(points) DESC, team ASC;"));

    const auto text = context.output.str();
    CHECK(text.find("| ops  | 2        | 13          | 6.5         |") != std::string::npos);
    CHECK(text.find("| docs |") == std::string::npos);
    CHECK(text.find("| sec  |") == std::string::npos);
    CHECK(text.find("1 row(s) selected") != std::string::npos);
}

TEST_CASE("supports grouped expressions built from grouping columns")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE tasks (team, points);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('ops', 4);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('ops', 6);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('docs', 3);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('docs', 7);"));
    context.reset_output();

    context.executor.execute(sql_test::parse_statement(
        "SELECT team + '-total', SUM(points) "
        "FROM tasks GROUP BY team "
        "ORDER BY SUM(points) DESC, team ASC;"));

    const auto text = context.output.str();
    const auto ops = text.find("ops-total");
    const auto docs = text.find("docs-total");
    REQUIRE(ops != std::string::npos);
    REQUIRE(docs != std::string::npos);
    CHECK(docs < ops);
    CHECK(text.find("10") != std::string::npos);
    CHECK(text.find("2 row(s) selected") != std::string::npos);
}

TEST_CASE("rejects non grouped columns in grouped queries")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE tasks (team, owner, points);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('ops', 'alice', 5);"));

    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("SELECT team, owner, COUNT(*) FROM tasks GROUP BY team;")), std::runtime_error);
}

TEST_CASE("rejects unsupported group by expressions and HAVING without grouping")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE tasks (team, points);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('ops', 5);"));

    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("SELECT team, COUNT(*) FROM tasks GROUP BY team + '!';")), std::runtime_error);
    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("SELECT team FROM tasks HAVING COUNT(*) > 0;")), std::runtime_error);
}

TEST_CASE("rejects mixing aggregate and non aggregate projections without grouping")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE metrics (category, value);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO metrics VALUES ('ops', 10);"));

    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("SELECT category, COUNT(*) FROM metrics;")), std::runtime_error);
}

TEST_CASE("rejects ambiguous and invalid multi source column references")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE tasks (id, team_id);"));
    context.executor.execute(sql_test::parse_statement("CREATE TABLE teams (id, name);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES (1, 10);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO teams VALUES (10, 'ops');"));

    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("SELECT id FROM tasks, teams WHERE tasks.team_id = teams.id;")), std::runtime_error);
    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("SELECT missing.name FROM tasks, teams;")), std::runtime_error);
    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("SELECT title FROM tasks, (SELECT id FROM teams);")), std::runtime_error);
}

TEST_SUITE_END();

