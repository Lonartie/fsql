#include "doctest.h"

#include "test_support.h"

TEST_SUITE_BEGIN("Executor::execute");

TEST_CASE("returns failed execution results without throwing from the core executor")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    sql::Executor executor(storage);

    const auto result = executor.execute(sql_test::parse_statement("SELECT * FROM missing;"));

    CHECK_FALSE(result.success);
    CHECK_EQ(static_cast<int>(result.kind), static_cast<int>(sql::ExecutionResultKind::None));
    CHECK(result.table == std::nullopt);
    CHECK(result.error.find("missing") != std::string::npos);
}

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
    const auto result = context.executor.execute(sql_test::parse_statement("UPDATE todos SET done = true WHERE title = 'Buy milk';"));

    const auto table = context.storage->load_table("todos");
    REQUIRE_EQ(table.rows.size(), 2U);
    CHECK_EQ(table.rows[0][3], "true");
    CHECK_EQ(table.rows[1][3], "false");
    CHECK_EQ(result.affected_rows, 1U);
    CHECK_EQ(result.message, "Updated 1 row(s) in 'todos'");
}

TEST_CASE("update without where modifies all rows")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, done);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('A', false);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('B', false);"));
    const auto result = context.executor.execute(sql_test::parse_statement("UPDATE todos SET done = true;"));

    const auto table = context.storage->load_table("todos");
    REQUIRE_EQ(table.rows.size(), 2U);
    CHECK_EQ(table.rows[0][1], "true");
    CHECK_EQ(table.rows[1][1], "true");
    CHECK_EQ(result.affected_rows, 2U);
    CHECK_EQ(result.message, "Updated 2 row(s) in 'todos'");
}

TEST_CASE("drops table from memory storage")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, done);"));
    const auto result = context.executor.execute(sql_test::parse_statement("DROP TABLE todos;"));

    CHECK_EQ(result.message, "Dropped table 'todos'");
    CHECK_THROWS_AS(context.storage->load_table("todos"), std::runtime_error);
}

TEST_CASE("rejects duplicate table creation")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, done);"));
    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, done);")), std::runtime_error);
}

TEST_CASE("rejects name collisions between tables and views")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE tasks (title);"));
    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("CREATE VIEW tasks AS SELECT title FROM tasks;")), std::runtime_error);

    context.executor.execute(sql_test::parse_statement("CREATE VIEW open_tasks AS SELECT title FROM tasks;"));
    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("CREATE TABLE open_tasks (title);")), std::runtime_error);
}

TEST_CASE("rejects unknown columns")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, done);"));
    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("SELECT missing FROM todos;")), std::runtime_error);
    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("UPDATE todos SET missing = true;")), std::runtime_error);
    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("INSERT INTO todos (missing) VALUES (true);")), std::runtime_error);
}

TEST_CASE("alter table rename column keeps data accessible under new name")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, archived_at);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('Buy milk', '2026-04-22');"));
    context.executor.execute(sql_test::parse_statement("ALTER TABLE todos RENAME COLUMN archived_at TO closed_at;"));

    const auto result = context.executor.execute(sql_test::parse_statement("SELECT closed_at FROM todos;"));

    const auto& table = sql_test::require_table(result);
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(table.rows[0][0], "2026-04-22");
    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("SELECT archived_at FROM todos;")), std::runtime_error);
}

TEST_CASE("alter table drop column removes stored values")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, category, done);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('Buy milk', 'home', true);"));
    context.executor.execute(sql_test::parse_statement("ALTER TABLE todos DROP COLUMN category;"));

    const auto table = context.storage->load_table("todos");
    REQUIRE_EQ(table.columns.size(), 2U);
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(table.rows[0][0], "Buy milk");
    CHECK_EQ(table.rows[0][1], "true");
}

TEST_CASE("rejects mismatched insert value counts")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, done);"));
    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('Buy milk');")), std::runtime_error);
    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("INSERT INTO todos (title) VALUES ('Buy milk', true);")), std::runtime_error);
}

TEST_CASE("rejects invalid alter table operations")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (id AUTO_INCREMENT, title);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES (1, 'Buy milk');"));
    context.executor.execute(sql_test::parse_statement("CREATE TABLE notes (id, title);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO notes VALUES ('x', 'bad');"));
    context.executor.execute(sql_test::parse_statement("CREATE TABLE single (only_col);"));

    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("ALTER TABLE todos ADD COLUMN title;")), std::runtime_error);
    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("ALTER TABLE single DROP COLUMN only_col;")), std::runtime_error);
    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("ALTER TABLE todos ALTER COLUMN title SET AUTO_INCREMENT;")), std::runtime_error);
    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("ALTER TABLE notes ALTER COLUMN id SET AUTO_INCREMENT;")), std::runtime_error);
}

TEST_CASE("rejects operations on missing tables")
{
    sql_test::ExecutorContext context;

    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("SELECT * FROM missing;")), std::runtime_error);
    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("INSERT INTO missing VALUES ('x');")), std::runtime_error);
    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("UPDATE missing SET value = 1;")), std::runtime_error);
}

TEST_CASE("alter view replaces stored definition")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE tasks (title, done);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Patch release', false);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Archive logs', true);"));
    context.executor.execute(sql_test::parse_statement("CREATE VIEW open_tasks AS SELECT title FROM tasks WHERE done = false;"));
    context.executor.execute(sql_test::parse_statement("ALTER VIEW open_tasks AS SELECT title FROM tasks WHERE done = true;"));

    const auto result = context.executor.execute(sql_test::parse_statement("SELECT title FROM open_tasks;"));

    const auto& table = sql_test::require_table(result);
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(table.rows[0][0], "Archive logs");
    CHECK_EQ(result.message, "1 row(s) selected");
}

TEST_CASE("rejects write operations and mismatched drop commands against views")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE tasks (title, done);"));
    context.executor.execute(sql_test::parse_statement("CREATE VIEW open_tasks AS SELECT title FROM tasks WHERE done = false;"));

    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("INSERT INTO open_tasks VALUES ('Patch release');")), std::runtime_error);
    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("UPDATE open_tasks SET title = 'Done';")), std::runtime_error);
    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("DELETE FROM open_tasks;")), std::runtime_error);
    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("ALTER TABLE open_tasks ADD COLUMN team;")), std::runtime_error);
    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("DROP TABLE open_tasks;")), std::runtime_error);
    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("DROP VIEW tasks;")), std::runtime_error);
}

TEST_CASE("rejects cyclic view definitions and rolls them back")
{
    sql_test::ExecutorContext context;

    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("CREATE VIEW loop_view AS SELECT * FROM loop_view;")), std::runtime_error);
    CHECK_FALSE(context.storage->has_view("loop_view"));
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
    const auto result = context.executor.execute(sql_test::parse_statement("DELETE FROM todos WHERE done = true;"));

    const auto table = context.storage->load_table("todos");
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(table.rows[0][0], "Write docs");
    CHECK_EQ(result.affected_rows, 1U);
    CHECK_EQ(result.message, "Deleted 1 row(s) from 'todos'");
}

TEST_CASE("qualified WHERE predicates behave consistently across select update and delete")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE tasks (title, team_id, priority, done);"));
    context.executor.execute(sql_test::parse_statement("CREATE TABLE teams (id, name);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Patch release', 10, 8, false);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Write docs', 20, 8, false);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Archive logs', 10, 3, false);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO teams VALUES (10, 'ops');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO teams VALUES (20, 'docs');"));

    const auto select_result = context.executor.execute(sql_test::parse_statement(
        "SELECT title FROM tasks WHERE tasks.team_id IN (SELECT id FROM teams WHERE name = 'ops') AND tasks.priority >= 5 AND NOT tasks.done;"));

    const auto& selected = sql_test::require_table(select_result);
    REQUIRE_EQ(selected.rows.size(), 1U);
    CHECK_EQ(selected.rows[0][0], "Patch release");

    const auto update_result = context.executor.execute(sql_test::parse_statement(
        "UPDATE tasks SET done = true WHERE tasks.team_id IN (SELECT id FROM teams WHERE name = 'ops') AND tasks.priority >= 5 AND NOT tasks.done;"));
    CHECK_EQ(update_result.affected_rows, 1U);

    const auto updated = context.storage->load_table("tasks");
    REQUIRE_EQ(updated.rows.size(), 3U);
    CHECK_EQ(updated.rows[0][3], "true");
    CHECK_EQ(updated.rows[1][3], "false");
    CHECK_EQ(updated.rows[2][3], "false");

    const auto delete_result = context.executor.execute(sql_test::parse_statement(
        "DELETE FROM tasks WHERE tasks.team_id IN (SELECT id FROM teams WHERE name = 'ops') AND tasks.done = true;"));
    CHECK_EQ(delete_result.affected_rows, 1U);

    const auto remaining = context.storage->load_table("tasks");
    REQUIRE_EQ(remaining.rows.size(), 2U);
    CHECK_EQ(remaining.rows[0][0], "Write docs");
    CHECK_EQ(remaining.rows[1][0], "Archive logs");
}

TEST_CASE("single table update and delete reject unknown qualified WHERE identifiers")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE tasks (title, done);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Patch release', false);"));

    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("UPDATE tasks SET done = true WHERE missing.done = false;")), std::runtime_error);
    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("DELETE FROM tasks WHERE missing.done = false;")), std::runtime_error);
}

TEST_CASE("update and delete support inline IN list predicates")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE tasks (id, title, done);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES (1, 'Patch release', false);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES (2, 'Write docs', false);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES (3, 'Rotate keys', false);"));

    const auto update_result = context.executor.execute(sql_test::parse_statement(
        "UPDATE tasks SET done = true WHERE id IN (1, 3) AND title IN ('Patch release', 'Rotate keys');"));
    CHECK_EQ(update_result.affected_rows, 2U);

    auto table = context.storage->load_table("tasks");
    REQUIRE_EQ(table.rows.size(), 3U);
    CHECK_EQ(table.rows[0][2], "true");
    CHECK_EQ(table.rows[1][2], "false");
    CHECK_EQ(table.rows[2][2], "true");

    const auto delete_result = context.executor.execute(sql_test::parse_statement(
        "DELETE FROM tasks WHERE id IN (3) OR title IN ('Write docs');"));
    CHECK_EQ(delete_result.affected_rows, 2U);

    table = context.storage->load_table("tasks");
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(table.rows[0][0], "1");
    CHECK_EQ(table.rows[0][1], "Patch release");
}

TEST_CASE("update and delete support NOT IN predicates")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE tasks (id, title, team_id, done);"));
    context.executor.execute(sql_test::parse_statement("CREATE TABLE teams (id, name);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES (1, 'Patch release', 10, false);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES (2, 'Write docs', 20, false);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES (3, 'Rotate keys', 30, false);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO teams VALUES (10, 'ops');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO teams VALUES (30, 'sec');"));

    const auto update_result = context.executor.execute(sql_test::parse_statement(
        "UPDATE tasks SET done = true WHERE id NOT IN (1, 3) AND team_id NOT IN (SELECT id FROM teams WHERE name = 'ops');"));
    CHECK_EQ(update_result.affected_rows, 1U);

    auto table = context.storage->load_table("tasks");
    REQUIRE_EQ(table.rows.size(), 3U);
    CHECK_EQ(table.rows[0][3], "false");
    CHECK_EQ(table.rows[1][3], "true");
    CHECK_EQ(table.rows[2][3], "false");

    const auto delete_result = context.executor.execute(sql_test::parse_statement(
        "DELETE FROM tasks WHERE id NOT IN (1, 2) OR title NOT IN ('Patch release', 'Write docs');"));
    CHECK_EQ(delete_result.affected_rows, 1U);

    table = context.storage->load_table("tasks");
    REQUIRE_EQ(table.rows.size(), 2U);
    CHECK_EQ(table.rows[0][0], "1");
    CHECK_EQ(table.rows[1][0], "2");
}

TEST_CASE("update can assign NULL values")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, archived_at);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('Buy milk', '2026-04-22');"));
    context.executor.execute(sql_test::parse_statement("UPDATE todos SET archived_at = NULL WHERE title = 'Buy milk';"));

    const auto result = context.executor.execute(sql_test::parse_statement("SELECT title FROM todos WHERE archived_at IS NULL;"));

    const auto& table = sql_test::require_table(result);
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(table.rows[0][0], "Buy milk");
    CHECK_EQ(result.message, "1 row(s) selected");
}

TEST_CASE("deletes all rows without where")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE todos (title, done);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('Buy milk', true);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO todos VALUES ('Write docs', false);"));
    const auto result = context.executor.execute(sql_test::parse_statement("DELETE FROM todos;"));

    const auto table = context.storage->load_table("todos");
    CHECK(table.rows.empty());
    CHECK_EQ(result.affected_rows, 2U);
    CHECK_EQ(result.message, "Deleted 2 row(s) from 'todos'");
}

TEST_CASE("drops views from memory storage")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE tasks (title, done);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('Patch release', false);"));
    context.executor.execute(sql_test::parse_statement("CREATE VIEW open_tasks AS SELECT title FROM tasks WHERE done = false;"));
    CHECK(context.storage->has_view("open_tasks"));

    const auto result = context.executor.execute(sql_test::parse_statement("DROP VIEW open_tasks;"));
    CHECK_EQ(result.message, "Dropped view 'open_tasks'");
    CHECK_FALSE(context.storage->has_view("open_tasks"));
    CHECK_THROWS_AS(context.storage->load_view("open_tasks"), std::runtime_error);
}

TEST_CASE("csv storage supports quoted file paths for table statements")
{
    sql_test::TemporaryDirectory temp_directory;
    std::filesystem::create_directories(temp_directory.path / "data");

    auto storage = std::make_shared<sql::CsvStorage>(temp_directory.path);
    sql::Executor executor(storage);
    const auto table_path = temp_directory.path / "data" / "tasks";
    const auto table_path_text = table_path.string();

    auto run = [&](const std::string& query)
    {
        const auto result = executor.execute(sql_test::parse_statement(query));
        REQUIRE(result.success);
        return result;
    };

    const auto create_result = run("CREATE TABLE '" + table_path_text + "' (id AUTO_INCREMENT, title, done = false);");
    CHECK_EQ(create_result.message, "Created table '" + table_path_text + "'");
    CHECK(std::filesystem::exists(table_path_text + ".csv"));

    const auto insert_result = run("INSERT INTO '" + table_path_text + "' (title) VALUES ('Patch release');");
    CHECK_EQ(insert_result.message, "Inserted 1 row into '" + table_path_text + "'");

    const auto update_result = run("UPDATE '" + table_path_text + "' SET done = true WHERE title = 'Patch release';");
    CHECK_EQ(update_result.message, "Updated 1 row(s) in '" + table_path_text + "'");

    const auto loaded = storage->load_table({sql::RelationReference::Kind::FilePath, table_path_text});
    REQUIRE_EQ(loaded.rows.size(), 1U);
    CHECK_EQ(loaded.rows[0][0], "1");
    CHECK_EQ(loaded.rows[0][1], "Patch release");
    CHECK_EQ(loaded.rows[0][2], "true");

    const auto delete_result = run("DELETE FROM '" + table_path_text + "' WHERE done = true;");
    CHECK_EQ(delete_result.message, "Deleted 1 row(s) from '" + table_path_text + "'");
    CHECK(storage->load_table({sql::RelationReference::Kind::FilePath, table_path_text}).rows.empty());

    const auto drop_result = run("DROP TABLE '" + table_path_text + "';");
    CHECK_EQ(drop_result.message, "Dropped table '" + table_path_text + "'");
    CHECK_FALSE(std::filesystem::exists(table_path_text + ".csv"));
}

TEST_CASE("csv storage supports quoted file paths for view statements")
{
    sql_test::TemporaryDirectory temp_directory;
    std::filesystem::create_directories(temp_directory.path / "tables");
    std::filesystem::create_directories(temp_directory.path / "views");

    auto storage = std::make_shared<sql::CsvStorage>(temp_directory.path);
    sql::Executor executor(storage);
    const auto table_path = temp_directory.path / "tables" / "tasks";
    const auto view_path = temp_directory.path / "views" / "open_tasks";
    const auto table_path_text = table_path.string();
    const auto view_path_text = view_path.string();

    auto run = [&](const std::string& query)
    {
        const auto result = executor.execute(sql_test::parse_statement(query));
        REQUIRE(result.success);
        return result;
    };

    run("CREATE TABLE '" + table_path_text + "' (title, done);");
    run("INSERT INTO '" + table_path_text + "' VALUES ('Patch release', false);");
    run("INSERT INTO '" + table_path_text + "' VALUES ('Archive logs', true);");

    const auto create_result = run("CREATE VIEW '" + view_path_text + "' AS SELECT title FROM '" + table_path_text + "' WHERE done = false;");
    CHECK_EQ(create_result.message, "Created view '" + view_path_text + "'");
    CHECK(std::filesystem::exists(view_path_text + ".view.sql"));

    const auto alter_result = run("ALTER VIEW '" + view_path_text + "' AS SELECT title FROM '" + table_path_text + "' WHERE done = true;");
    CHECK_EQ(alter_result.message, "Altered view '" + view_path_text + "'");

    const auto view = storage->load_view({sql::RelationReference::Kind::FilePath, view_path_text});
    CHECK(view.select_statement.find("done = true") != std::string::npos);

    const auto drop_result = run("DROP VIEW '" + view_path_text + "';");
    CHECK_EQ(drop_result.message, "Dropped view '" + view_path_text + "'");
    CHECK_FALSE(std::filesystem::exists(view_path_text + ".view.sql"));
}

TEST_SUITE_END();

