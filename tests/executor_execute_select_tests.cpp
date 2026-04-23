#include "doctest.h"

#include "test_support.h"

TEST_SUITE_BEGIN("Executor::execute");

TEST_CASE("returns a structured result for select statements")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    sql::Executor executor(storage);

    const auto create_result = executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, category);"));
    REQUIRE(create_result.success);
    CHECK_EQ(static_cast<int>(create_result.kind), static_cast<int>(sql::ExecutionResultKind::Create));
    CHECK_EQ(create_result.message, "Created table 'todos'");

    const auto insert_result = executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('Buy milk', 'home');"));
    REQUIRE(insert_result.success);
    CHECK_EQ(insert_result.affected_rows, 1U);

    const auto select_result = executor.execute(sql_test::parse_statement("SELECT title, category FROM todos;"));
    REQUIRE(select_result.success);
    CHECK_EQ(static_cast<int>(select_result.kind), static_cast<int>(sql::ExecutionResultKind::Select));
    CHECK_EQ(select_result.affected_rows, 1U);
    const auto& table = sql_test::require_table(select_result);
    REQUIRE_EQ(table.column_names.size(), 2U);
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(table.column_names[0], "title");
    CHECK_EQ(table.column_names[1], "category");
    CHECK_EQ(table.rows[0][0], "Buy milk");
    CHECK_EQ(table.rows[0][1], "home");
    CHECK_EQ(select_result.message, "1 row(s) selected");
}

TEST_CASE("create insert and select workflow")
{
    sql_test::ExecutorContext context;

    const auto create_result = context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, category, text, done);"));
    const auto insert_result = context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('Buy milk', 'home', '2 liters', 'false');"));
    const auto select_result = context.executor.execute(sql_test::parse_statement("SELECT category, text FROM todos;"));

    CHECK_EQ(create_result.message, "Created table 'todos'");
    CHECK_EQ(insert_result.message, "Inserted 1 row into 'todos'");
    const auto& table = sql_test::require_table(select_result);
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(table.rows[0][0], "home");
    CHECK_EQ(table.rows[0][1], "2 liters");
    CHECK_EQ(select_result.message, "1 row(s) selected");
}

TEST_CASE("select where filters rows")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, category, text, done);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('Buy milk', 'home', '2 liters', 'false');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('Write docs', 'work', 'API docs', 'false');"));

    const auto result = context.executor.execute(sql_test::parse_statement("SELECT title FROM todos WHERE category = 'work';"));

    const auto& table = sql_test::require_table(result);
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(table.rows[0][0], "Write docs");
    CHECK_EQ(result.message, "1 row(s) selected");
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

    const auto result = context.executor.execute(sql_test::parse_statement(
        "SELECT tasks.title, teams.name FROM tasks, teams WHERE tasks.team_id = teams.id ORDER BY tasks.title ASC;"));

    const auto& table = sql_test::require_table(result);
    REQUIRE_EQ(table.rows.size(), 2U);
    CHECK_EQ(table.rows[0][0], "Patch release");
    CHECK_EQ(table.rows[0][1], "ops");
    CHECK_EQ(table.rows[1][0], "Write docs");
    CHECK_EQ(table.rows[1][1], "docs");
    CHECK_EQ(result.message, "2 row(s) selected");
}

TEST_CASE("select star qualifies headers for multiple sources")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE tasks (id, team_id);"));
    context.executor.execute(sql_test::parse_statement("CREATE TABLE teams (id, name);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES (1, 10);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO teams VALUES (10, 'ops');"));

    const auto result = context.executor.execute(sql_test::parse_statement("SELECT * FROM tasks, teams WHERE tasks.team_id = teams.id;"));

    const auto& table = sql_test::require_table(result);
    REQUIRE_EQ(table.column_names.size(), 4U);
    CHECK_EQ(table.column_names[0], "tasks.id");
    CHECK_EQ(table.column_names[1], "tasks.team_id");
    CHECK_EQ(table.column_names[2], "teams.id");
    CHECK_EQ(table.column_names[3], "teams.name");
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(result.message, "1 row(s) selected");
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

    const auto result = context.executor.execute(sql_test::parse_statement(
        "SELECT t.title, lookup.name FROM tasks t, (SELECT id, name FROM teams) lookup WHERE t.team_id = lookup.id ORDER BY t.title;"));

    const auto& table = sql_test::require_table(result);
    REQUIRE_EQ(table.rows.size(), 2U);
    CHECK_EQ(table.rows[0][0], "Patch release");
    CHECK_EQ(table.rows[0][1], "ops");
    CHECK_EQ(table.rows[1][0], "Write docs");
    CHECK_EQ(table.rows[1][1], "docs");
    CHECK_EQ(result.message, "2 row(s) selected");
}

TEST_CASE("select reads rows from created views")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE tasks (title, team_id, done);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Patch release', 10, false);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Archive logs', 10, true);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Write docs', 20, false);"));
    context.executor.execute(sql_test::parse_statement("CREATE VIEW open_tasks AS SELECT title, team_id FROM tasks WHERE done = false;"));

    const auto result = context.executor.execute(sql_test::parse_statement("SELECT title FROM open_tasks ORDER BY title ASC;"));

    const auto& table = sql_test::require_table(result);
    REQUIRE_EQ(table.rows.size(), 2U);
    CHECK_EQ(table.rows[0][0], "Patch release");
    CHECK_EQ(table.rows[1][0], "Write docs");
    CHECK_EQ(result.message, "2 row(s) selected");
}

TEST_CASE("select supports views built on other views")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE tasks (title, team_id, done);"));
    context.executor.execute(sql_test::parse_statement("CREATE TABLE teams (id, name);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Patch release', 10, false);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Archive logs', 10, true);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO teams VALUES (10, 'ops');"));
    context.executor.execute(sql_test::parse_statement("CREATE VIEW open_tasks AS SELECT title, team_id FROM tasks WHERE done = false;"));
    context.executor.execute(sql_test::parse_statement(
        "CREATE VIEW open_task_lookup AS SELECT open_tasks.title, teams.name FROM open_tasks, teams WHERE open_tasks.team_id = teams.id;"));

    const auto result = context.executor.execute(sql_test::parse_statement("SELECT * FROM open_task_lookup;"));

    const auto& table = sql_test::require_table(result);
    REQUIRE_EQ(table.column_names.size(), 2U);
    CHECK_EQ(table.column_names[0], "open_tasks.title");
    CHECK_EQ(table.column_names[1], "teams.name");
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(table.rows[0][0], "Patch release");
    CHECK_EQ(table.rows[0][1], "ops");
    CHECK_EQ(result.message, "1 row(s) selected");
}

TEST_CASE("select all returns all columns")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, category);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('Buy milk', 'home');"));

    const auto result = context.executor.execute(sql_test::parse_statement("SELECT * FROM todos;"));

    const auto& table = sql_test::require_table(result);
    REQUIRE_EQ(table.column_names.size(), 2U);
    CHECK_EQ(table.column_names[0], "title");
    CHECK_EQ(table.column_names[1], "category");
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(table.rows[0][0], "Buy milk");
    CHECK_EQ(table.rows[0][1], "home");
}

TEST_CASE("select preserves commas inside cell values")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, text);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('Buy milk', 'hello, world');"));

    const auto result = context.executor.execute(sql_test::parse_statement("SELECT text FROM todos;"));

    const auto& table = sql_test::require_table(result);
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(table.rows[0][0], "hello, world");
}

TEST_CASE("select returns structured rows and headers")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, category);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('Buy milk', 'home');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('Write docs', 'work');"));

    const auto result = context.executor.execute(sql_test::parse_statement("SELECT title, category FROM todos;"));

    const auto& table = sql_test::require_table(result);
    REQUIRE_EQ(table.column_names.size(), 2U);
    CHECK_EQ(table.column_names[0], "title");
    CHECK_EQ(table.column_names[1], "category");
    REQUIRE_EQ(table.rows.size(), 2U);
    CHECK_EQ(table.rows[0][0], "Buy milk");
    CHECK_EQ(table.rows[0][1], "home");
    CHECK_EQ(table.rows[1][0], "Write docs");
    CHECK_EQ(table.rows[1][1], "work");
    CHECK_EQ(result.message, "2 row(s) selected");
}

TEST_CASE("orders selected rows by multiple expressions")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE tasks (title, priority, effort);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Bravo', 5, 1);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Alpha', 5, 2);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Delta', 3, 9);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Charlie', 8, 1);"));

    const auto result = context.executor.execute(sql_test::parse_statement("SELECT title, priority FROM tasks ORDER BY priority DESC, title ASC;"));

    const auto& table = sql_test::require_table(result);
    REQUIRE_EQ(table.rows.size(), 4U);
    CHECK_EQ(table.rows[0][0], "Charlie");
    CHECK_EQ(table.rows[1][0], "Alpha");
    CHECK_EQ(table.rows[2][0], "Bravo");
    CHECK_EQ(table.rows[3][0], "Delta");
    CHECK_EQ(result.message, "4 row(s) selected");
}

TEST_CASE("select distinct removes duplicate projected rows")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE tasks (title, category, priority);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Patch release', 'ops', 8);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Night watch', 'ops', 4);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Write docs', 'docs', 3);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('API review', 'docs', 6);"));

    const auto result = context.executor.execute(sql_test::parse_statement("SELECT DISTINCT category FROM tasks ORDER BY category ASC;"));

    const auto& table = sql_test::require_table(result);
    REQUIRE_EQ(table.rows.size(), 2U);
    CHECK_EQ(table.rows[0][0], "docs");
    CHECK_EQ(table.rows[1][0], "ops");
    CHECK_EQ(result.message, "2 row(s) selected");
}

TEST_CASE("select applies offset and limit after ordering")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE tasks (title, priority);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('First', 9);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Second', 7);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Third', 5);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Fourth', 3);"));

    const auto result = context.executor.execute(sql_test::parse_statement("SELECT title, priority FROM tasks ORDER BY priority DESC LIMIT 2 OFFSET 1;"));

    const auto& table = sql_test::require_table(result);
    REQUIRE_EQ(table.rows.size(), 2U);
    CHECK_EQ(table.rows[0][0], "Second");
    CHECK_EQ(table.rows[1][0], "Third");
    CHECK_EQ(result.message, "2 row(s) selected");
}

TEST_CASE("supports typical aggregate functions on filtered rows")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE metrics (category, value, label);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO metrics VALUES ('ops', 10, 'alpha');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO metrics VALUES ('ops', 20, 'beta');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO metrics VALUES ('ops', '', 'gamma');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO metrics VALUES ('docs', 7, 'delta');"));

    const auto result = context.executor.execute(sql_test::parse_statement("SELECT COUNT(*), COUNT(value), SUM(value), AVG(value), MIN(value), MAX(value) FROM metrics WHERE category = 'ops';"));

    const auto& table = sql_test::require_table(result);
    REQUIRE_EQ(table.column_names.size(), 6U);
    CHECK_EQ(table.column_names[0], "COUNT(*)");
    CHECK_EQ(table.column_names[1], "COUNT(value)");
    CHECK_EQ(table.column_names[2], "SUM(value)");
    CHECK_EQ(table.column_names[3], "AVG(value)");
    CHECK_EQ(table.column_names[4], "MIN(value)");
    CHECK_EQ(table.column_names[5], "MAX(value)");
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(table.rows[0][0], "3");
    CHECK_EQ(table.rows[0][1], "2");
    CHECK_EQ(table.rows[0][2], "30");
    CHECK_EQ(table.rows[0][3], "15");
    CHECK_EQ(table.rows[0][4], "10");
    CHECK_EQ(table.rows[0][5], "20");
    CHECK_EQ(result.message, "1 row(s) selected");
}

TEST_CASE("supports MIN and MAX on text projections")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE metrics (category, label);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO metrics VALUES ('ops', 'zulu');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO metrics VALUES ('ops', 'alpha');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO metrics VALUES ('ops', 'mango');"));

    const auto result = context.executor.execute(sql_test::parse_statement("SELECT MIN(label), MAX(label) FROM metrics WHERE category = 'ops';"));

    const auto& table = sql_test::require_table(result);
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(table.rows[0][0], "alpha");
    CHECK_EQ(table.rows[0][1], "zulu");
    CHECK_EQ(result.message, "1 row(s) selected");
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

    const auto result = context.executor.execute(sql_test::parse_statement(
        "SELECT team, COUNT(*), SUM(points), AVG(points) "
        "FROM tasks WHERE done = false "
        "GROUP BY team HAVING COUNT(*) >= 2 "
        "ORDER BY SUM(points) DESC, team ASC;"));

    const auto& table = sql_test::require_table(result);
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(table.rows[0][0], "ops");
    CHECK_EQ(table.rows[0][1], "2");
    CHECK_EQ(table.rows[0][2], "13");
    CHECK_EQ(table.rows[0][3], "6.5");
    CHECK_EQ(result.message, "1 row(s) selected");
}

TEST_CASE("supports grouped expressions built from grouping columns")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE tasks (team, points);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('ops', 4);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('ops', 6);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('docs', 3);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('docs', 7);"));

    const auto result = context.executor.execute(sql_test::parse_statement(
        "SELECT team + '-total', SUM(points) "
        "FROM tasks GROUP BY team "
        "ORDER BY SUM(points) DESC, team ASC;"));

    const auto& table = sql_test::require_table(result);
    REQUIRE_EQ(table.rows.size(), 2U);
    CHECK_EQ(table.rows[0][0], "docs-total");
    CHECK_EQ(table.rows[0][1], "10");
    CHECK_EQ(table.rows[1][0], "ops-total");
    CHECK_EQ(table.rows[1][1], "10");
    CHECK_EQ(result.message, "2 row(s) selected");
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

