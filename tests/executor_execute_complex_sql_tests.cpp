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

    const auto select_result = context.executor.execute(sql_test::parse_statement(
        "SELECT title, category FROM work_items "
        "WHERE !(done = true && category = 'docs') "
        "&& (((flags | 1) > 0) || (~priority < 0));"));

    const auto& select_table = sql_test::require_table(select_result);
    REQUIRE_EQ(select_table.rows.size(), 2U);
    CHECK_EQ(select_table.rows[0][0], "Patch release");
    CHECK_EQ(select_table.rows[1][0], "Night watch");
    CHECK_EQ(select_result.message, "2 row(s) selected");

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

    const auto result = context.executor.execute(sql_test::parse_statement(
        "SELECT COUNT(*), SUM(points), AVG(points), MIN(points), MAX(points) "
        "FROM tasks WHERE team = 'ops' && ((flags & 2) = 2 || done = true);"));

    const auto& table = sql_test::require_table(result);
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(table.rows[0][0], "3");
    CHECK_EQ(table.rows[0][1], "26");
    CHECK_EQ(table.rows[0][2], "8.66667");
    CHECK_EQ(table.rows[0][3], "5");
    CHECK_EQ(table.rows[0][4], "13");
    CHECK_EQ(result.message, "1 row(s) selected");
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

    const auto result = context.executor.execute(sql_test::parse_statement(
        "SELECT DISTINCT owner, status FROM deployments "
        "WHERE (risk >= 4 && environment = 'prod') || retries > 1 "
        "ORDER BY status DESC, owner ASC LIMIT 2 OFFSET 1;"));

    const auto& result_table = sql_test::require_table(result);
    REQUIRE_EQ(result_table.rows.size(), 2U);
    CHECK_EQ(result_table.rows[0][0], "docs");
    CHECK_EQ(result_table.rows[0][1], "pending");
    CHECK_EQ(result_table.rows[1][0], "ops");
    CHECK_EQ(result_table.rows[1][1], "pending");
    CHECK_EQ(result.message, "2 row(s) selected");
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

    const auto result = context.executor.execute(sql_test::parse_statement(
        "SELECT team, COUNT(*), SUM(hours), MAX(severity) "
        "FROM incidents WHERE resolved = false "
        "GROUP BY team HAVING SUM(hours) >= 5 "
        "ORDER BY SUM(hours) DESC, team ASC LIMIT 2 OFFSET 0;"));

    const auto& table = sql_test::require_table(result);
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(table.rows[0][0], "docs");
    CHECK_EQ(table.rows[0][1], "2");
    CHECK_EQ(table.rows[0][2], "7");
    CHECK_EQ(table.rows[0][3], "6");
    CHECK_EQ(result.message, "1 row(s) selected");
}

TEST_CASE("combines multiple select sources and derived tables in a reporting workflow")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE services (id, name, team_id, active);"));
    context.executor.execute(sql_test::parse_statement("CREATE TABLE teams (id, name, region);"));
    context.executor.execute(sql_test::parse_statement("CREATE TABLE incidents (service_id, severity, open);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO services VALUES (1, 'billing', 10, true);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO services VALUES (2, 'search', 10, true);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO services VALUES (3, 'docs', 20, false);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO teams VALUES (10, 'ops', 'eu');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO teams VALUES (20, 'docs', 'us');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO incidents VALUES (1, 9, true);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO incidents VALUES (1, 5, false);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO incidents VALUES (2, 7, true);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO incidents VALUES (3, 3, true);"));

    const auto result = context.executor.execute(sql_test::parse_statement(
        "SELECT svc.name, teams.name, incident_rows.open "
        "FROM services svc, teams, "
        "(SELECT service_id, open FROM incidents WHERE open = true) incident_rows "
        "WHERE svc.team_id = teams.id AND svc.id = incident_rows.service_id AND svc.active = true "
        "ORDER BY svc.name ASC;"));

    const auto& table = sql_test::require_table(result);
    REQUIRE_EQ(table.rows.size(), 2U);
    CHECK_EQ(table.rows[0][0], "billing");
    CHECK_EQ(table.rows[0][1], "ops");
    CHECK_EQ(table.rows[1][0], "search");
    CHECK_EQ(table.rows[1][1], "ops");
    CHECK_EQ(result.message, "2 row(s) selected");
}

TEST_CASE("uses EXISTS and IN predicate subqueries in a larger triage workflow")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE tasks (id, title, team_id, status, severity);"));
    context.executor.execute(sql_test::parse_statement("CREATE TABLE teams (id, name, on_call);"));
    context.executor.execute(sql_test::parse_statement("CREATE TABLE escalations (task_id, active);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES (1, 'Patch release', 10, 'open', 9);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES (2, 'Write docs', 20, 'open', 3);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES (3, 'Rotate keys', 30, 'open', 8);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO teams VALUES (10, 'ops', true);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO teams VALUES (20, 'docs', false);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO teams VALUES (30, 'sec', true);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO escalations VALUES (1, true);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO escalations VALUES (3, true);"));

    context.executor.execute(sql_test::parse_statement(
        "UPDATE tasks SET status = 'priority' "
        "WHERE EXISTS (SELECT id FROM teams WHERE on_call = true) "
        "AND team_id IN (SELECT id FROM teams WHERE on_call = true) "
        "AND id IN (SELECT task_id FROM escalations WHERE active = true);"));

    const auto updated = context.storage->load_table("tasks");
    REQUIRE_EQ(updated.rows.size(), 3U);
    CHECK_EQ(updated.rows[0][3], "priority");
    CHECK_EQ(updated.rows[1][3], "open");
    CHECK_EQ(updated.rows[2][3], "priority");

    const auto result = context.executor.execute(sql_test::parse_statement(
        "SELECT title, status FROM tasks "
        "WHERE id IN (SELECT task_id FROM escalations WHERE active = true) "
        "OR (EXISTS (SELECT id FROM teams WHERE name = 'docs') AND team_id IN (SELECT id FROM teams WHERE name = 'docs')) "
        "ORDER BY title;"));

    const auto& table = sql_test::require_table(result);
    REQUIRE_EQ(table.rows.size(), 3U);
    CHECK_EQ(table.rows[0][0], "Patch release");
    CHECK_EQ(table.rows[1][0], "Rotate keys");
    CHECK_EQ(table.rows[2][0], "Write docs");
    CHECK_EQ(result.message, "3 row(s) selected");
}

TEST_CASE("uses ANY and ALL quantified subqueries in a larger escalation workflow")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE tasks (id, title, severity, team_id, status);"));
    context.executor.execute(sql_test::parse_statement("CREATE TABLE thresholds (level);"));
    context.executor.execute(sql_test::parse_statement("CREATE TABLE on_call (team_id);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES (1, 'Patch release', 9, 10, 'open');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES (2, 'Write docs', 3, 20, 'open');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES (3, 'Rotate keys', 8, 30, 'open');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES (4, 'Renew cert', 5, 10, 'open');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO thresholds VALUES (4);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO thresholds VALUES (6);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO on_call VALUES (10);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO on_call VALUES (30);"));

    context.executor.execute(sql_test::parse_statement(
        "UPDATE tasks SET status = 'priority' "
        "WHERE severity > ALL (SELECT level FROM thresholds) "
        "AND team_id = ANY (SELECT team_id FROM on_call);"));

    const auto updated = context.storage->load_table("tasks");
    REQUIRE_EQ(updated.rows.size(), 4U);
    CHECK_EQ(updated.rows[0][4], "priority");
    CHECK_EQ(updated.rows[1][4], "open");
    CHECK_EQ(updated.rows[2][4], "priority");
    CHECK_EQ(updated.rows[3][4], "open");

    const auto result = context.executor.execute(sql_test::parse_statement(
        "SELECT title, status FROM tasks "
        "WHERE severity >= ANY (SELECT level FROM thresholds) "
        "AND severity <= ALL (SELECT level + 2 FROM thresholds WHERE level >= 6) "
        "AND team_id = ANY (SELECT team_id FROM on_call) "
        "ORDER BY title;"));

    const auto& table = sql_test::require_table(result);
    REQUIRE_EQ(table.rows.size(), 2U);
    CHECK_EQ(table.rows[0][0], "Renew cert");
    CHECK_EQ(table.rows[1][0], "Rotate keys");
    CHECK_EQ(result.message, "2 row(s) selected");
}

TEST_CASE("uses NULL values in a larger archival workflow")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE tasks (id, title, archived_at, owner, closed_at);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES (1, 'Patch release', NULL, 'ops', NULL);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES (2, 'Write docs', '2026-04-01', 'docs', '2026-04-02');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES (3, 'Rotate keys', NULL, 'sec', '2026-04-10');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES (4, 'Renew cert', '', 'ops', NULL);"));

    context.executor.execute(sql_test::parse_statement(
        "UPDATE tasks SET archived_at = '2026-04-22' WHERE closed_at IS NOT NULL AND archived_at IS NULL;"));
    context.executor.execute(sql_test::parse_statement(
        "UPDATE tasks SET archived_at = NULL, closed_at = NULL WHERE title = 'Write docs';"));

    const auto updated = context.storage->load_table("tasks");
    REQUIRE_EQ(updated.rows.size(), 4U);

    const auto result = context.executor.execute(sql_test::parse_statement(
        "SELECT title, archived_at FROM tasks WHERE archived_at IS NULL ORDER BY title;"));

    const auto& table = sql_test::require_table(result);
    REQUIRE_EQ(table.rows.size(), 2U);
    CHECK_EQ(table.rows[0][0], "Patch release");
    CHECK_EQ(table.rows[0][1], "NULL");
    CHECK_EQ(table.rows[1][0], "Write docs");
    CHECK_EQ(table.rows[1][1], "NULL");
    CHECK_EQ(result.message, "2 row(s) selected");
}

TEST_CASE("uses multiple alter table actions in a staged schema evolution workflow")
{
    sql_test::ExecutorContext context;

    context.executor.execute(sql_test::parse_statement("CREATE TABLE tasks (id, title);"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES (1, 'Patch release');"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks VALUES ('', 'Write docs');"));

    context.executor.execute(sql_test::parse_statement("ALTER TABLE tasks ALTER COLUMN id SET AUTO_INCREMENT;"));
    context.executor.execute(sql_test::parse_statement("ALTER TABLE tasks ADD COLUMN category = 'backlog';"));
    context.executor.execute(sql_test::parse_statement("ALTER TABLE tasks ADD COLUMN archived_at = NULL;"));
    context.executor.execute(sql_test::parse_statement("ALTER TABLE tasks RENAME COLUMN archived_at TO closed_at;"));
    context.executor.execute(sql_test::parse_statement("ALTER TABLE tasks ALTER COLUMN category SET DEFAULT 'general';"));
    context.executor.execute(sql_test::parse_statement("ALTER TABLE tasks DROP COLUMN closed_at;"));
    context.executor.execute(sql_test::parse_statement("INSERT INTO tasks (title) VALUES ('Rotate keys');"));

    const auto table = context.storage->load_table("tasks");
    REQUIRE_EQ(table.columns.size(), 3U);
    REQUIRE_EQ(table.rows.size(), 3U);
    CHECK_EQ(table.rows[0][0], "1");
    CHECK_EQ(table.rows[1][0], "2");
    CHECK_EQ(table.rows[2][0], "3");
    CHECK_EQ(table.rows[0][2], "backlog");
    CHECK_EQ(table.rows[1][2], "backlog");
    CHECK_EQ(table.rows[2][2], "general");

    const auto result = context.executor.execute(sql_test::parse_statement("SELECT id, title, category FROM tasks ORDER BY id;"));

    const auto& result_table = sql_test::require_table(result);
    REQUIRE_EQ(result_table.rows.size(), 3U);
    CHECK_EQ(result_table.rows[0][1], "Patch release");
    CHECK_EQ(result_table.rows[1][1], "Write docs");
    CHECK_EQ(result_table.rows[2][1], "Rotate keys");
    CHECK_EQ(result_table.rows[2][2], "general");
    CHECK_EQ(result.message, "3 row(s) selected");
}

TEST_SUITE_END();

