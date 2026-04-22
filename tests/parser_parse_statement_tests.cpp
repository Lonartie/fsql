#include "doctest.h"

#include "test_support.h"

TEST_SUITE_BEGIN("Parser::parse_statement");

TEST_CASE("parses create statement")
{
    const auto statement = sql_test::parse_statement("CREATE TABLE todos (title, category, text, done);");

    CHECK_EQ(static_cast<int>(statement.kind), static_cast<int>(sql::Statement::Kind::Create));
    CHECK_EQ(statement.create.table_name, "todos");
    REQUIRE_EQ(statement.create.columns.size(), 4U);
    CHECK_EQ(statement.create.columns[0].name, "title");
    CHECK_FALSE(statement.create.columns[0].auto_increment);
    CHECK_EQ(statement.create.columns[3].name, "done");
}

TEST_CASE("parses insert with explicit columns")
{
    const auto statement = sql_test::parse_statement("INSERT INTO todos (title, done) VALUES ('Buy milk', true);");

    CHECK_EQ(static_cast<int>(statement.kind), static_cast<int>(sql::Statement::Kind::Insert));
    CHECK(statement.insert.columns.has_value());
    REQUIRE(statement.insert.columns.has_value());
    CHECK_EQ(statement.insert.table_name, "todos");
    CHECK_EQ(statement.insert.columns->size(), 2U);
    CHECK_EQ((*statement.insert.columns)[0], "title");
    REQUIRE_EQ(statement.insert.values.size(), 2U);
    CHECK(statement.insert.values[0] != nullptr);
    CHECK_EQ(static_cast<int>(statement.insert.values[0]->kind), static_cast<int>(sql::ExpressionKind::Literal));
    CHECK_EQ(statement.insert.values[0]->text, "Buy milk");
    CHECK_EQ(static_cast<int>(statement.insert.values[1]->kind), static_cast<int>(sql::ExpressionKind::Identifier));
    CHECK_EQ(statement.insert.values[1]->text, "true");
}

TEST_CASE("parses select star with where clause")
{
    const auto statement = sql_test::parse_statement("SELECT * FROM todos WHERE category = 'home';");

    CHECK_EQ(static_cast<int>(statement.kind), static_cast<int>(sql::Statement::Kind::Select));
    CHECK(statement.select.select_all);
    REQUIRE_EQ(statement.select.sources.size(), 1U);
    CHECK_EQ(static_cast<int>(statement.select.sources[0].kind), static_cast<int>(sql::SelectSource::Kind::Table));
    CHECK_EQ(statement.select.sources[0].name, "todos");
    REQUIRE(statement.select.where != nullptr);
    CHECK_EQ(static_cast<int>(statement.select.where->kind), static_cast<int>(sql::ExpressionKind::Binary));
    CHECK_EQ(static_cast<int>(statement.select.where->binary_operator), static_cast<int>(sql::BinaryOperator::Equal));
}

TEST_CASE("parses update assignments")
{
    const auto statement = sql_test::parse_statement("UPDATE todos SET done = true, category = 'done' WHERE title = 'Buy milk';");

    CHECK_EQ(static_cast<int>(statement.kind), static_cast<int>(sql::Statement::Kind::Update));
    CHECK_EQ(statement.update.table_name, "todos");
    REQUIRE_EQ(statement.update.assignments.size(), 2U);
    CHECK_EQ(statement.update.assignments[0].first, "done");
    CHECK(statement.update.assignments[0].second != nullptr);
    CHECK_EQ(statement.update.assignments[0].second->text, "true");
    CHECK_EQ(statement.update.assignments[1].first, "category");
    CHECK(statement.update.assignments[1].second != nullptr);
    CHECK_EQ(statement.update.assignments[1].second->text, "done");
    REQUIRE(statement.update.where != nullptr);
    CHECK_EQ(static_cast<int>(statement.update.where->kind), static_cast<int>(sql::ExpressionKind::Binary));
}

TEST_CASE("accepts statements without semicolons")
{
    const auto statement = sql_test::parse_statement("SELECT title FROM todos");
    CHECK_EQ(static_cast<int>(statement.kind), static_cast<int>(sql::Statement::Kind::Select));
    REQUIRE_EQ(statement.select.projections.size(), 1U);
    REQUIRE(statement.select.projections[0] != nullptr);
    CHECK_EQ(static_cast<int>(statement.select.projections[0]->kind), static_cast<int>(sql::ExpressionKind::Identifier));
    CHECK_EQ(statement.select.projections[0]->text, "title");
}

TEST_CASE("parses aggregate select projections")
{
    const auto statement = sql_test::parse_statement("SELECT COUNT(*), SUM(amount), AVG(amount), MIN(label), MAX(label) FROM metrics WHERE amount > 0;");

    CHECK_EQ(static_cast<int>(statement.kind), static_cast<int>(sql::Statement::Kind::Select));
    CHECK_FALSE(statement.select.select_all);
    REQUIRE_EQ(statement.select.projections.size(), 5U);
    REQUIRE(statement.select.projections[0] != nullptr);
    CHECK_EQ(static_cast<int>(statement.select.projections[0]->kind), static_cast<int>(sql::ExpressionKind::FunctionCall));
    CHECK_EQ(statement.select.projections[0]->text, "COUNT");
    CHECK(statement.select.projections[0]->function_uses_star);
    CHECK(statement.select.projections[0]->arguments.empty());
    REQUIRE(statement.select.projections[1] != nullptr);
    CHECK_EQ(statement.select.projections[1]->text, "SUM");
    REQUIRE_EQ(statement.select.projections[1]->arguments.size(), 1U);
    CHECK_EQ(statement.select.projections[1]->arguments[0]->text, "amount");
    CHECK(statement.select.where != nullptr);
}

TEST_CASE("parses select result shaping clauses")
{
    const auto statement = sql_test::parse_statement("SELECT DISTINCT category, priority + 1 FROM todos WHERE done = false ORDER BY category DESC, priority + 1 ASC LIMIT 5 OFFSET 2;");

    CHECK_EQ(static_cast<int>(statement.kind), static_cast<int>(sql::Statement::Kind::Select));
    CHECK(statement.select.distinct);
    CHECK_FALSE(statement.select.select_all);
    REQUIRE_EQ(statement.select.projections.size(), 2U);
    REQUIRE_EQ(statement.select.order_by.size(), 2U);
    REQUIRE(statement.select.order_by[0].expression != nullptr);
    CHECK_EQ(statement.select.order_by[0].expression->text, "category");
    CHECK(statement.select.order_by[0].descending);
    REQUIRE(statement.select.order_by[1].expression != nullptr);
    CHECK_EQ(static_cast<int>(statement.select.order_by[1].expression->kind), static_cast<int>(sql::ExpressionKind::Binary));
    CHECK_FALSE(statement.select.order_by[1].descending);
    REQUIRE(statement.select.limit.has_value());
    CHECK_EQ(*statement.select.limit, 5U);
    REQUIRE(statement.select.offset.has_value());
    CHECK_EQ(*statement.select.offset, 2U);
}

TEST_CASE("parses group by and having clauses")
{
    const auto statement = sql_test::parse_statement("SELECT team, COUNT(*), SUM(points) FROM tasks WHERE done = false GROUP BY team HAVING COUNT(*) >= 2 ORDER BY SUM(points) DESC;");

    CHECK_EQ(static_cast<int>(statement.kind), static_cast<int>(sql::Statement::Kind::Select));
    CHECK_FALSE(statement.select.select_all);
    REQUIRE_EQ(statement.select.projections.size(), 3U);
    REQUIRE_EQ(statement.select.group_by.size(), 1U);
    REQUIRE(statement.select.group_by[0] != nullptr);
    CHECK_EQ(static_cast<int>(statement.select.group_by[0]->kind), static_cast<int>(sql::ExpressionKind::Identifier));
    CHECK_EQ(statement.select.group_by[0]->text, "team");
    REQUIRE(statement.select.having != nullptr);
    CHECK_EQ(static_cast<int>(statement.select.having->kind), static_cast<int>(sql::ExpressionKind::Binary));
    CHECK_EQ(static_cast<int>(statement.select.having->binary_operator), static_cast<int>(sql::BinaryOperator::GreaterEqual));
    REQUIRE_EQ(statement.select.order_by.size(), 1U);
    REQUIRE(statement.select.order_by[0].expression != nullptr);
    CHECK_EQ(static_cast<int>(statement.select.order_by[0].expression->kind), static_cast<int>(sql::ExpressionKind::FunctionCall));
    CHECK_EQ(statement.select.order_by[0].expression->text, "SUM");
    CHECK(statement.select.order_by[0].descending);
}

TEST_CASE("parses multiple select sources and qualified identifiers")
{
    const auto statement = sql_test::parse_statement("SELECT tasks.title, teams.name FROM tasks, teams WHERE tasks.team_id = teams.id;");

    CHECK_EQ(static_cast<int>(statement.kind), static_cast<int>(sql::Statement::Kind::Select));
    REQUIRE_EQ(statement.select.sources.size(), 2U);
    CHECK_EQ(statement.select.sources[0].name, "tasks");
    CHECK_EQ(statement.select.sources[1].name, "teams");
    REQUIRE_EQ(statement.select.projections.size(), 2U);
    CHECK_EQ(statement.select.projections[0]->text, "tasks.title");
    CHECK_EQ(statement.select.projections[1]->text, "teams.name");
    REQUIRE(statement.select.where != nullptr);
    CHECK_EQ(static_cast<int>(statement.select.where->kind), static_cast<int>(sql::ExpressionKind::Binary));
    CHECK_EQ(static_cast<int>(statement.select.where->binary_operator), static_cast<int>(sql::BinaryOperator::Equal));
}

TEST_CASE("parses select source subqueries with aliases")
{
    const auto statement = sql_test::parse_statement("SELECT t.title, defaults.category FROM tasks t, (SELECT category FROM settings) defaults WHERE t.category = defaults.category;");

    REQUIRE_EQ(statement.select.sources.size(), 2U);
    CHECK_EQ(statement.select.sources[0].name, "tasks");
    REQUIRE(statement.select.sources[0].alias.has_value());
    CHECK_EQ(*statement.select.sources[0].alias, "t");
    CHECK_EQ(static_cast<int>(statement.select.sources[1].kind), static_cast<int>(sql::SelectSource::Kind::Subquery));
    REQUIRE(statement.select.sources[1].subquery != nullptr);
    REQUIRE(statement.select.sources[1].alias.has_value());
    CHECK_EQ(*statement.select.sources[1].alias, "defaults");
    REQUIRE_EQ(statement.select.sources[1].subquery->sources.size(), 1U);
    CHECK_EQ(statement.select.sources[1].subquery->sources[0].name, "settings");
}

TEST_CASE("parses UNIQUE synonym for DISTINCT")
{
    const auto statement = sql_test::parse_statement("SELECT UNIQUE category FROM todos ORDER BY category;");

    CHECK_EQ(static_cast<int>(statement.kind), static_cast<int>(sql::Statement::Kind::Select));
    CHECK(statement.select.distinct);
    REQUIRE_EQ(statement.select.order_by.size(), 1U);
    REQUIRE(statement.select.order_by[0].expression != nullptr);
    CHECK_EQ(statement.select.order_by[0].expression->text, "category");
}

TEST_CASE("parses SQL keyword predicates inside WHERE clause")
{
    const auto statement = sql_test::parse_statement("SELECT title FROM tasks WHERE NOT done AND priority BETWEEN 3 AND 9 AND owner LIKE 'op%' OR title REGEXP '^Patch';");

    CHECK_EQ(static_cast<int>(statement.kind), static_cast<int>(sql::Statement::Kind::Select));
    REQUIRE(statement.select.where != nullptr);
    CHECK_EQ(static_cast<int>(statement.select.where->kind), static_cast<int>(sql::ExpressionKind::Binary));
    CHECK_EQ(static_cast<int>(statement.select.where->binary_operator), static_cast<int>(sql::BinaryOperator::LogicalOr));
    REQUIRE(statement.select.where->right != nullptr);
    CHECK_EQ(static_cast<int>(statement.select.where->right->kind), static_cast<int>(sql::ExpressionKind::Binary));
    CHECK_EQ(static_cast<int>(statement.select.where->right->binary_operator), static_cast<int>(sql::BinaryOperator::Regexp));
}

TEST_CASE("parses EXISTS and IN subquery predicates inside WHERE clause")
{
    const auto statement = sql_test::parse_statement("SELECT title FROM tasks WHERE EXISTS (SELECT id FROM teams WHERE name = 'ops') OR team_id IN (SELECT id FROM teams WHERE name = 'ops');");

    CHECK_EQ(static_cast<int>(statement.kind), static_cast<int>(sql::Statement::Kind::Select));
    REQUIRE(statement.select.where != nullptr);
    CHECK_EQ(static_cast<int>(statement.select.where->kind), static_cast<int>(sql::ExpressionKind::Binary));
    CHECK_EQ(static_cast<int>(statement.select.where->binary_operator), static_cast<int>(sql::BinaryOperator::LogicalOr));
    REQUIRE(statement.select.where->left != nullptr);
    CHECK_EQ(static_cast<int>(statement.select.where->left->kind), static_cast<int>(sql::ExpressionKind::Exists));
    REQUIRE(statement.select.where->right != nullptr);
    CHECK_EQ(static_cast<int>(statement.select.where->right->kind), static_cast<int>(sql::ExpressionKind::Binary));
    CHECK_EQ(static_cast<int>(statement.select.where->right->binary_operator), static_cast<int>(sql::BinaryOperator::In));
}

TEST_CASE("parses ANY and ALL quantified subquery predicates inside WHERE clause")
{
    const auto statement = sql_test::parse_statement("SELECT title FROM tasks WHERE severity > ALL (SELECT threshold FROM critical_rules) AND team_id = ANY (SELECT id FROM teams WHERE on_call = true);");

    CHECK_EQ(static_cast<int>(statement.kind), static_cast<int>(sql::Statement::Kind::Select));
    REQUIRE(statement.select.where != nullptr);
    CHECK_EQ(static_cast<int>(statement.select.where->kind), static_cast<int>(sql::ExpressionKind::Binary));
    CHECK_EQ(static_cast<int>(statement.select.where->binary_operator), static_cast<int>(sql::BinaryOperator::LogicalAnd));
    REQUIRE(statement.select.where->left != nullptr);
    CHECK_EQ(static_cast<int>(statement.select.where->left->binary_operator), static_cast<int>(sql::BinaryOperator::Greater));
    CHECK_EQ(static_cast<int>(statement.select.where->left->subquery_quantifier), static_cast<int>(sql::SubqueryQuantifier::All));
    REQUIRE(statement.select.where->right != nullptr);
    CHECK_EQ(static_cast<int>(statement.select.where->right->binary_operator), static_cast<int>(sql::BinaryOperator::Equal));
    CHECK_EQ(static_cast<int>(statement.select.where->right->subquery_quantifier), static_cast<int>(sql::SubqueryQuantifier::Any));
}

TEST_CASE("parses NULL literals and IS NULL predicates inside statements")
{
    const auto select_statement = sql_test::parse_statement("SELECT title FROM tasks WHERE archived_at IS NULL OR deleted_at IS NOT NULL;");

    REQUIRE(select_statement.select.where != nullptr);
    CHECK_EQ(static_cast<int>(select_statement.select.where->binary_operator), static_cast<int>(sql::BinaryOperator::LogicalOr));
    REQUIRE(select_statement.select.where->left != nullptr);
    CHECK_EQ(static_cast<int>(select_statement.select.where->left->binary_operator), static_cast<int>(sql::BinaryOperator::Is));
    REQUIRE(select_statement.select.where->right != nullptr);
    CHECK_EQ(static_cast<int>(select_statement.select.where->right->binary_operator), static_cast<int>(sql::BinaryOperator::IsNot));

    const auto insert_statement = sql_test::parse_statement("INSERT INTO tasks (title, archived_at) VALUES ('Patch release', NULL);");
    REQUIRE_EQ(insert_statement.insert.values.size(), 2U);
    REQUIRE(insert_statement.insert.values[1] != nullptr);
    CHECK_EQ(static_cast<int>(insert_statement.insert.values[1]->kind), static_cast<int>(sql::ExpressionKind::Null));
}

TEST_CASE("rejects unsupported statements")
{
    CHECK_THROWS_AS(sql_test::parse_statement("MERGE INTO todos;"), std::runtime_error);
}

TEST_CASE("rejects trailing tokens")
{
    CHECK_THROWS_AS(sql_test::parse_statement("SELECT title FROM todos WHERE done = false extra"), std::runtime_error);
}

TEST_CASE("accepts AUTO_INCREMENT in create table")
{
    const auto statement = sql_test::parse_statement("CREATE TABLE todos (id AUTO_INCREMENT, title, done);");
    REQUIRE_EQ(statement.create.columns.size(), 3U);
    CHECK_EQ(statement.create.columns[0].name, "id");
    CHECK(statement.create.columns[0].auto_increment);
}

TEST_CASE("parses drop table statement")
{
    const auto statement = sql_test::parse_statement("DROP TABLE todos;");
    CHECK_EQ(static_cast<int>(statement.kind), static_cast<int>(sql::Statement::Kind::Drop));
    CHECK_EQ(statement.drop.table_name, "todos");
}

TEST_CASE("parses complex WHERE expressions")
{
    const auto statement = sql_test::parse_statement("SELECT * FROM nums WHERE !(a + 1 < b) && ((a ^ b) > 0 || ~a < 0);");
    CHECK_EQ(static_cast<int>(statement.kind), static_cast<int>(sql::Statement::Kind::Select));
    CHECK(statement.select.where != nullptr);
}

TEST_CASE("parses WHERE clause containing SELECT subquery")
{
    const auto statement = sql_test::parse_statement("SELECT title FROM todos WHERE category = (SELECT value FROM defaults);");

    REQUIRE(statement.select.where != nullptr);
    CHECK_EQ(static_cast<int>(statement.select.where->kind), static_cast<int>(sql::ExpressionKind::Binary));
    REQUIRE(statement.select.where->right != nullptr);
    CHECK_EQ(static_cast<int>(statement.select.where->right->kind), static_cast<int>(sql::ExpressionKind::Select));
    REQUIRE(statement.select.where->right->select != nullptr);
    REQUIRE_EQ(statement.select.where->right->select->sources.size(), 1U);
    CHECK_EQ(statement.select.where->right->select->sources[0].name, "defaults");
}

TEST_CASE("parses column default expressions")
{
    const auto statement = sql_test::parse_statement("CREATE TABLE dates (creation_date = NOW(), label = 'new');");
    REQUIRE_EQ(statement.create.columns.size(), 2U);
    REQUIRE(statement.create.columns[0].default_value != nullptr);
    CHECK_EQ(static_cast<int>(statement.create.columns[0].default_value->kind), static_cast<int>(sql::ExpressionKind::FunctionCall));
    CHECK_EQ(statement.create.columns[0].default_value->text, "NOW");
    REQUIRE(statement.create.columns[1].default_value != nullptr);
    CHECK_EQ(static_cast<int>(statement.create.columns[1].default_value->kind), static_cast<int>(sql::ExpressionKind::Literal));
    CHECK_EQ(statement.create.columns[1].default_value->text, "new");
}

TEST_CASE("parses delete statement with where")
{
    const auto statement = sql_test::parse_statement("DELETE FROM todos WHERE done = true;");
    CHECK_EQ(static_cast<int>(statement.kind), static_cast<int>(sql::Statement::Kind::Delete));
    CHECK_EQ(statement.delete_statement.table_name, "todos");
    CHECK(statement.delete_statement.where != nullptr);
}

TEST_SUITE_END();

