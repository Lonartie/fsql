#include "doctest.h"

#include "test_support.h"

TEST_SUITE_BEGIN("Parser::parse_expression");

TEST_CASE("parses SELECT subquery as expression")
{
    const auto expression = sql_test::parse_expression("(SELECT category FROM defaults WHERE id = 1)");

    REQUIRE(expression != nullptr);
    CHECK_EQ(static_cast<int>(expression->kind), static_cast<int>(sql::ExpressionKind::Select));
    REQUIRE(expression->select != nullptr);
    REQUIRE_EQ(expression->select->sources.size(), 1U);
    CHECK_EQ(expression->select->sources[0].name, "defaults");
    REQUIRE_EQ(expression->select->projections.size(), 1U);
    REQUIRE(expression->select->projections[0] != nullptr);
    CHECK_EQ(static_cast<int>(expression->select->projections[0]->kind), static_cast<int>(sql::ExpressionKind::Identifier));
    CHECK_EQ(expression->select->projections[0]->text, "category");
    CHECK(expression->select->where != nullptr);
}

TEST_CASE("parses SELECT subquery expression with multiple sources")
{
    const auto expression = sql_test::parse_expression("(SELECT tasks.title FROM tasks, teams WHERE tasks.team_id = teams.id AND teams.name = 'ops')");

    REQUIRE(expression != nullptr);
    REQUIRE(expression->select != nullptr);
    REQUIRE_EQ(expression->select->sources.size(), 2U);
    CHECK_EQ(expression->select->sources[0].name, "tasks");
    CHECK_EQ(expression->select->sources[1].name, "teams");
    REQUIRE_EQ(expression->select->projections.size(), 1U);
    CHECK_EQ(expression->select->projections[0]->text, "tasks.title");
    REQUIRE(expression->select->where != nullptr);
}

TEST_CASE("parses SQL keyword boolean operators")
{
    const auto expression = sql_test::parse_expression("NOT active OR score > 5 AND team = 'ops'");

    REQUIRE(expression != nullptr);
    CHECK_EQ(static_cast<int>(expression->kind), static_cast<int>(sql::ExpressionKind::Binary));
    CHECK_EQ(static_cast<int>(expression->binary_operator), static_cast<int>(sql::BinaryOperator::LogicalOr));
    REQUIRE(expression->left != nullptr);
    CHECK_EQ(static_cast<int>(expression->left->kind), static_cast<int>(sql::ExpressionKind::Unary));
    CHECK_EQ(static_cast<int>(expression->left->unary_operator), static_cast<int>(sql::UnaryOperator::LogicalNot));
    REQUIRE(expression->right != nullptr);
    CHECK_EQ(static_cast<int>(expression->right->kind), static_cast<int>(sql::ExpressionKind::Binary));
    CHECK_EQ(static_cast<int>(expression->right->binary_operator), static_cast<int>(sql::BinaryOperator::LogicalAnd));
}

TEST_CASE("parses BETWEEN LIKE and REGEXP expressions")
{
    const auto expression = sql_test::parse_expression("priority BETWEEN 3 AND 7 AND owner LIKE 'op%' OR title REGEXP '^Patch'");

    REQUIRE(expression != nullptr);
    CHECK_EQ(static_cast<int>(expression->kind), static_cast<int>(sql::ExpressionKind::Binary));
    CHECK_EQ(static_cast<int>(expression->binary_operator), static_cast<int>(sql::BinaryOperator::LogicalOr));
    REQUIRE(expression->left != nullptr);
    CHECK_EQ(static_cast<int>(expression->left->kind), static_cast<int>(sql::ExpressionKind::Binary));
    CHECK_EQ(static_cast<int>(expression->left->binary_operator), static_cast<int>(sql::BinaryOperator::LogicalAnd));
    REQUIRE(expression->left->left != nullptr);
    CHECK_EQ(static_cast<int>(expression->left->left->kind), static_cast<int>(sql::ExpressionKind::Binary));
    CHECK_EQ(static_cast<int>(expression->left->left->binary_operator), static_cast<int>(sql::BinaryOperator::LogicalAnd));
    REQUIRE(expression->right != nullptr);
    CHECK_EQ(static_cast<int>(expression->right->kind), static_cast<int>(sql::ExpressionKind::Binary));
    CHECK_EQ(static_cast<int>(expression->right->binary_operator), static_cast<int>(sql::BinaryOperator::Regexp));
}

TEST_CASE("parses EXISTS and IN subquery predicates")
{
    const auto expression = sql_test::parse_expression("EXISTS (SELECT id FROM tasks WHERE done = false) AND team_id IN (SELECT id FROM teams)");

    REQUIRE(expression != nullptr);
    CHECK_EQ(static_cast<int>(expression->kind), static_cast<int>(sql::ExpressionKind::Binary));
    CHECK_EQ(static_cast<int>(expression->binary_operator), static_cast<int>(sql::BinaryOperator::LogicalAnd));
    REQUIRE(expression->left != nullptr);
    CHECK_EQ(static_cast<int>(expression->left->kind), static_cast<int>(sql::ExpressionKind::Exists));
    REQUIRE(expression->left->select != nullptr);
    REQUIRE(expression->right != nullptr);
    CHECK_EQ(static_cast<int>(expression->right->kind), static_cast<int>(sql::ExpressionKind::Binary));
    CHECK_EQ(static_cast<int>(expression->right->binary_operator), static_cast<int>(sql::BinaryOperator::In));
    REQUIRE(expression->right->right != nullptr);
    CHECK_EQ(static_cast<int>(expression->right->right->kind), static_cast<int>(sql::ExpressionKind::Select));
}

TEST_CASE("parses inline IN list predicates")
{
    const auto expression = sql_test::parse_expression("team_id IN (10, 20, 30) AND owner IN ('ops', 'docs')");

    REQUIRE(expression != nullptr);
    CHECK_EQ(static_cast<int>(expression->kind), static_cast<int>(sql::ExpressionKind::Binary));
    CHECK_EQ(static_cast<int>(expression->binary_operator), static_cast<int>(sql::BinaryOperator::LogicalAnd));
    REQUIRE(expression->left != nullptr);
    CHECK_EQ(static_cast<int>(expression->left->binary_operator), static_cast<int>(sql::BinaryOperator::In));
    REQUIRE(expression->left->right != nullptr);
    CHECK_EQ(static_cast<int>(expression->left->right->kind), static_cast<int>(sql::ExpressionKind::List));
    REQUIRE_EQ(expression->left->right->arguments.size(), 3U);
    CHECK_EQ(expression->left->right->arguments[0]->text, "10");
    REQUIRE(expression->right != nullptr);
    CHECK_EQ(static_cast<int>(expression->right->binary_operator), static_cast<int>(sql::BinaryOperator::In));
    REQUIRE(expression->right->right != nullptr);
    CHECK_EQ(static_cast<int>(expression->right->right->kind), static_cast<int>(sql::ExpressionKind::List));
    REQUIRE_EQ(expression->right->right->arguments.size(), 2U);
    CHECK_EQ(expression->right->right->arguments[0]->text, "ops");
}

TEST_CASE("parses NOT IN list and subquery predicates")
{
    const auto expression = sql_test::parse_expression("team_id NOT IN (10, 20, 30) OR owner NOT IN (SELECT name FROM teams)");

    REQUIRE(expression != nullptr);
    CHECK_EQ(static_cast<int>(expression->kind), static_cast<int>(sql::ExpressionKind::Binary));
    CHECK_EQ(static_cast<int>(expression->binary_operator), static_cast<int>(sql::BinaryOperator::LogicalOr));
    REQUIRE(expression->left != nullptr);
    CHECK_EQ(static_cast<int>(expression->left->binary_operator), static_cast<int>(sql::BinaryOperator::NotIn));
    REQUIRE(expression->left->right != nullptr);
    CHECK_EQ(static_cast<int>(expression->left->right->kind), static_cast<int>(sql::ExpressionKind::List));
    REQUIRE_EQ(expression->left->right->arguments.size(), 3U);
    REQUIRE(expression->right != nullptr);
    CHECK_EQ(static_cast<int>(expression->right->binary_operator), static_cast<int>(sql::BinaryOperator::NotIn));
    REQUIRE(expression->right->right != nullptr);
    CHECK_EQ(static_cast<int>(expression->right->right->kind), static_cast<int>(sql::ExpressionKind::Select));
}

TEST_CASE("parses ANY and ALL quantified subquery predicates")
{
    const auto expression = sql_test::parse_expression("score >= ALL (SELECT min_score FROM rules) OR score = ANY (SELECT exact_score FROM overrides)");

    REQUIRE(expression != nullptr);
    CHECK_EQ(static_cast<int>(expression->kind), static_cast<int>(sql::ExpressionKind::Binary));
    CHECK_EQ(static_cast<int>(expression->binary_operator), static_cast<int>(sql::BinaryOperator::LogicalOr));
    REQUIRE(expression->left != nullptr);
    CHECK_EQ(static_cast<int>(expression->left->kind), static_cast<int>(sql::ExpressionKind::Binary));
    CHECK_EQ(static_cast<int>(expression->left->binary_operator), static_cast<int>(sql::BinaryOperator::GreaterEqual));
    CHECK_EQ(static_cast<int>(expression->left->subquery_quantifier), static_cast<int>(sql::SubqueryQuantifier::All));
    REQUIRE(expression->right != nullptr);
    CHECK_EQ(static_cast<int>(expression->right->kind), static_cast<int>(sql::ExpressionKind::Binary));
    CHECK_EQ(static_cast<int>(expression->right->binary_operator), static_cast<int>(sql::BinaryOperator::Equal));
    CHECK_EQ(static_cast<int>(expression->right->subquery_quantifier), static_cast<int>(sql::SubqueryQuantifier::Any));
}

TEST_CASE("parses NULL literals and IS NULL predicates")
{
    const auto expression = sql_test::parse_expression("value IS NULL OR NULL IS NOT NULL");

    REQUIRE(expression != nullptr);
    CHECK_EQ(static_cast<int>(expression->kind), static_cast<int>(sql::ExpressionKind::Binary));
    CHECK_EQ(static_cast<int>(expression->binary_operator), static_cast<int>(sql::BinaryOperator::LogicalOr));
    REQUIRE(expression->left != nullptr);
    CHECK_EQ(static_cast<int>(expression->left->binary_operator), static_cast<int>(sql::BinaryOperator::Is));
    REQUIRE(expression->left->right != nullptr);
    CHECK_EQ(static_cast<int>(expression->left->right->kind), static_cast<int>(sql::ExpressionKind::Null));
    REQUIRE(expression->right != nullptr);
    CHECK_EQ(static_cast<int>(expression->right->binary_operator), static_cast<int>(sql::BinaryOperator::IsNot));
    REQUIRE(expression->right->left != nullptr);
    CHECK_EQ(static_cast<int>(expression->right->left->kind), static_cast<int>(sql::ExpressionKind::Null));
}

TEST_SUITE_END();

