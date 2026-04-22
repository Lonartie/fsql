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
    CHECK_EQ(statement.select.table_name, "todos");
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

TEST_CASE("rejects unsupported statements")
{
    CHECK_THROWS_AS(sql_test::parse_statement("MERGE INTO todos;"), std::runtime_error);
}

TEST_CASE("rejects trailing tokens")
{
    CHECK_THROWS_AS(sql_test::parse_statement("SELECT title FROM todos extra"), std::runtime_error);
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
    CHECK_EQ(statement.select.where->right->select->table_name, "defaults");
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

