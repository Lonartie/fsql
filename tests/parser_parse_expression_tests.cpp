#include "doctest.h"

#include "test_support.h"

TEST_SUITE_BEGIN("Parser::parse_expression");

TEST_CASE("parses SELECT subquery as expression")
{
    const auto expression = sql_test::parse_expression("(SELECT category FROM defaults WHERE id = 1)");

    REQUIRE(expression != nullptr);
    CHECK_EQ(static_cast<int>(expression->kind), static_cast<int>(sql::ExpressionKind::Select));
    REQUIRE(expression->select != nullptr);
    CHECK_EQ(expression->select->table_name, "defaults");
    REQUIRE_EQ(expression->select->projections.size(), 1U);
    REQUIRE(expression->select->projections[0] != nullptr);
    CHECK_EQ(static_cast<int>(expression->select->projections[0]->kind), static_cast<int>(sql::ExpressionKind::Identifier));
    CHECK_EQ(expression->select->projections[0]->text, "category");
    CHECK(expression->select->where != nullptr);
}

TEST_SUITE_END();

