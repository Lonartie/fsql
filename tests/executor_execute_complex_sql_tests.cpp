#include "doctest.h"

#include "test_support.h"

TEST_SUITE_BEGIN("Executor::execute");

TEST_CASE("handles deeply nested workflow with defaults subqueries updates selects and deletes")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE settings (id, default_category, default_done, priority_limit, required_mask);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO settings VALUES (1, 'ops', false, 10, 6);"));
    context.executor.execute(sql_test::parse_statement(
        "CREATE TABLE work_items ("
        "id AUTO_INCREMENT, "
        "title, "
        "category = (SELECT default_category FROM settings WHERE id = 1), "
        "done = (SELECT default_done FROM settings WHERE id = 1), "
        "priority = 0, "
        "effort = 0, "
        "flags = 0"
        ");"));

    context.executor.execute(sql_test::parse_statement("INSERT INTO work_items (title, priority, effort, flags) VALUES ('Patch release', 8, 2, 6);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO work_items (title, category, done, priority, effort, flags) VALUES ('Write docs', 'docs', true, 3, 1, 2);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO work_items (title, priority, effort, flags) VALUES ('Night watch', 4, 1, 4);"));

    context.executor.execute(sql_test::parse_statement(
        "UPDATE work_items "
        "SET done = true, category = (SELECT default_category FROM settings WHERE id = 1) "
        "WHERE ((priority + effort * 2) >= (SELECT priority_limit FROM settings WHERE id = 1) "
        "&& ((flags & (SELECT required_mask FROM settings WHERE id = 1)) = (SELECT required_mask FROM settings WHERE id = 1))) "
        "|| title = 'Night watch';"));

    const auto updated = context.storage->load_table("work_items");
    REQUIRE_EQ(updated.rows.size(), 3U);
    CHECK_EQ(updated.rows[0][0], "1");
    CHECK_EQ(updated.rows[0][2], "ops");
    CHECK_EQ(updated.rows[0][3], "true");
    CHECK_EQ(updated.rows[1][0], "2");
    CHECK_EQ(updated.rows[1][2], "docs");
    CHECK_EQ(updated.rows[1][3], "true");
    CHECK_EQ(updated.rows[2][0], "3");
    CHECK_EQ(updated.rows[2][2], "ops");
    CHECK_EQ(updated.rows[2][3], "true");

    context.reset_output();
    context.executor.execute(sql_test::parse_statement(
        "SELECT title, category FROM work_items "
        "WHERE !(done = true && category = 'docs') "
        "&& (((flags | 1) > 0) || (~priority < 0));"));

    const auto select_text = context.output.str();
    CHECK(select_text.find("Patch release") != std::string::npos);
    CHECK(select_text.find("Night watch") != std::string::npos);
    CHECK(select_text.find("Write docs") == std::string::npos);
    CHECK(select_text.find("2 row(s) selected") != std::string::npos);

    context.executor.execute(sql_test::parse_statement("DELETE FROM work_items WHERE done = true && ((flags & 2) = 0);"));

    const auto remaining = context.storage->load_table("work_items");
    REQUIRE_EQ(remaining.rows.size(), 2U);
    CHECK_EQ(remaining.rows[0][1], "Patch release");
    CHECK_EQ(remaining.rows[1][1], "Write docs");
}

TEST_CASE("re evaluates multiple subquery defaults after configuration changes")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE settings (id, default_category, default_done, default_priority);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO settings VALUES (1, 'backlog', false, 5);"));
    context.executor.execute(sql_test::parse_statement(
        "CREATE TABLE tickets ("
        "id AUTO_INCREMENT, "
        "title, "
        "category = (SELECT default_category FROM settings WHERE id = 1), "
        "done = (SELECT default_done FROM settings WHERE id = 1), "
        "priority = (SELECT default_priority FROM settings WHERE id = 1)"
        ");"));

    context.executor.execute(sql_test::parse_statement("INSERT INTO tickets (title) VALUES ('First import');"));
    context.executor.execute(sql_test::parse_statement("UPDATE settings SET default_category = 'expedite', default_done = true, default_priority = 9 WHERE id = 1;"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tickets (title) VALUES ('Second import');"));

    const auto table = context.storage->load_table("tickets");
    REQUIRE_EQ(table.rows.size(), 2U);
    CHECK_EQ(table.rows[0][0], "1");
    CHECK_EQ(table.rows[0][1], "First import");
    CHECK_EQ(table.rows[0][2], "backlog");
    CHECK_EQ(table.rows[0][3], "false");
    CHECK_EQ(table.rows[0][4], "5");
    CHECK_EQ(table.rows[1][0], "2");
    CHECK_EQ(table.rows[1][1], "Second import");
    CHECK_EQ(table.rows[1][2], "expedite");
    CHECK_EQ(table.rows[1][3], "true");
    CHECK_EQ(table.rows[1][4], "9");
}

TEST_CASE("rejects complex defaults once configuration queries stop being scalar")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE settings (id, default_category);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO settings VALUES (1, 'ops');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO settings VALUES (2, 'infra');"));
    context.executor.execute(sql_test::parse_statement(
        "CREATE TABLE broken_items ("
        "title, "
        "category = (SELECT default_category FROM settings WHERE id >= 1)"
        ");"));

    CHECK_THROWS_AS(context.executor.execute(sql_test::parse_statement("INSERT INTO broken_items (title) VALUES ('ambiguous');")), std::runtime_error);
}

TEST_CASE("supports aggregate subqueries inside larger maintenance workflow")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE tasks (id AUTO_INCREMENT, team, points, done, flags);"));
    context.executor.execute(sql_test::parse_statement("CREATE TABLE summaries (team, total_points, avg_points, open_count, max_points, min_points);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks (team, points, done, flags) VALUES ('ops', 5, false, 3);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks (team, points, done, flags) VALUES ('ops', 8, true, 7);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks (team, points, done, flags) VALUES ('ops', 13, false, 6);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks (team, points, done, flags) VALUES ('docs', 2, false, 1);"));
    context.executor.execute(sql_test::parse_statement(
        "INSERT INTO summaries VALUES ("
        "'ops', "
        "(SELECT SUM(points) FROM tasks WHERE team = 'ops'), "
        "(SELECT AVG(points) FROM tasks WHERE team = 'ops'), "
        "(SELECT COUNT(*) FROM tasks WHERE team = 'ops' && done = false), "
        "(SELECT MAX(points) FROM tasks WHERE team = 'ops'), "
        "(SELECT MIN(points) FROM tasks WHERE team = 'ops')"
        ");"));

    auto summaries = context.storage->load_table("summaries");
    REQUIRE_EQ(summaries.rows.size(), 1U);
    CHECK_EQ(summaries.rows[0][1], "26");
    CHECK_EQ(summaries.rows[0][2], "8.66667");
    CHECK_EQ(summaries.rows[0][3], "2");
    CHECK_EQ(summaries.rows[0][4], "13");
    CHECK_EQ(summaries.rows[0][5], "5");

    context.executor.execute(sql_test::parse_statement(
        "UPDATE tasks SET done = true "
        "WHERE team = 'ops' && points < (SELECT AVG(points) FROM tasks WHERE team = 'ops');"));

    const auto tasks = context.storage->load_table("tasks");
    REQUIRE_EQ(tasks.rows.size(), 4U);
    CHECK_EQ(tasks.rows[0][3], "true");
    CHECK_EQ(tasks.rows[1][3], "true");
    CHECK_EQ(tasks.rows[2][3], "false");

    context.reset_output();
    context.executor.execute(sql_test::parse_statement(
        "SELECT COUNT(*), SUM(points), AVG(points), MIN(points), MAX(points) "
        "FROM tasks WHERE team = 'ops' && ((flags & 2) = 2 || done = true);"));

    const auto text = context.output.str();
    CHECK(text.find("| 3        | 26          | 8.66667     | 5           | 13          |") != std::string::npos);
    CHECK(text.find("1 row(s) selected") != std::string::npos);
}

TEST_CASE("combines distinct ordering and pagination in a larger review workflow")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE deployments (service, environment, risk, owner, status, retries);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO deployments VALUES ('billing', 'prod', 9, 'ops', 'pending', 1);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO deployments VALUES ('search', 'stage', 4, 'ops', 'pending', 2);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO deployments VALUES ('auth', 'prod', 7, 'sec', 'blocked', 0);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO deployments VALUES ('docs', 'prod', 2, 'docs', 'pending', 3);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO deployments VALUES ('cdn', 'prod', 9, 'ops', 'pending', 0);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO deployments VALUES ('auth', 'stage', 7, 'sec', 'pending', 2);"));

    context.executor.execute(sql_test::parse_statement(
        "UPDATE deployments SET status = 'priority' "
        "WHERE environment = 'prod' && (risk + retries) >= 9;"));

    const auto table = context.storage->load_table("deployments");
    REQUIRE_EQ(table.rows.size(), 6U);
    CHECK_EQ(table.rows[0][4], "priority");
    CHECK_EQ(table.rows[4][4], "priority");

    context.reset_output();
    context.executor.execute(sql_test::parse_statement(
        "SELECT DISTINCT owner, status FROM deployments "
        "WHERE (risk >= 4 && environment = 'prod') || retries > 1 "
        "ORDER BY status DESC, owner ASC LIMIT 2 OFFSET 1;"));

    const auto text = context.output.str();
    CHECK(text.find("| docs  | pending |") != std::string::npos);
    CHECK(text.find("| ops   | pending |") != std::string::npos);
    CHECK(text.find("| sec   | pending |") == std::string::npos);
    CHECK(text.find("| sec   | blocked |") == std::string::npos);
    CHECK(text.find("2 row(s) selected") != std::string::npos);
}

TEST_CASE("summarizes changing workloads with grouping having ordering and pagination")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE incidents (service, team, severity, hours, resolved, escalations);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO incidents VALUES ('billing', 'ops', 9, 5, false, 2);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO incidents VALUES ('search', 'ops', 7, 4, false, 1);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO incidents VALUES ('docs', 'docs', 3, 2, false, 0);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO incidents VALUES ('auth', 'sec', 8, 6, false, 3);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO incidents VALUES ('cdn', 'ops', 4, 1, true, 0);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO incidents VALUES ('portal', 'docs', 6, 5, false, 2);"));

    context.executor.execute(sql_test::parse_statement(
        "UPDATE incidents SET resolved = true "
        "WHERE severity BETWEEN 8 AND 9 OR escalations >= 3;"));

    const auto after_update = context.storage->load_table("incidents");
    REQUIRE_EQ(after_update.rows.size(), 6U);
    CHECK_EQ(after_update.rows[0][4], "true");
    CHECK_EQ(after_update.rows[3][4], "true");
    CHECK_EQ(after_update.rows[5][4], "false");

    context.reset_output();
    context.executor.execute(sql_test::parse_statement(
        "SELECT team, COUNT(*), SUM(hours), MAX(severity) "
        "FROM incidents WHERE resolved = false "
        "GROUP BY team HAVING SUM(hours) >= 5 "
        "ORDER BY SUM(hours) DESC, team ASC LIMIT 2 OFFSET 0;"));

    const auto text = context.output.str();
    CHECK(text.find("| docs | 2        | 7          | 6             |") != std::string::npos);
    CHECK(text.find("| ops  | 1        | 4          | 7             |") == std::string::npos);
    CHECK(text.find("| sec  |") == std::string::npos);
    CHECK(text.find("1 row(s) selected") != std::string::npos);
}

TEST_SUITE_END();

