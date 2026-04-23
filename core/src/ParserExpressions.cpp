#include "Parser.h"

#include "SqlError.h"

#include <limits>
#include <utility>

namespace sql
{
    ExpressionPtr Parser::parse_expression()
    {
        return parse_logical_or();
    }

    ExpressionPtr Parser::parse_logical_or()
    {
        auto expression = parse_logical_and();
        while (consume_optional(TokenType::DoublePipe) || match_keyword("OR"))
        {
            expression = make_binary(BinaryOperator::LogicalOr, expression, parse_logical_and());
        }
        return expression;
    }

    ExpressionPtr Parser::parse_logical_and()
    {
        auto expression = parse_bitwise_or();
        while (consume_optional(TokenType::DoubleAmpersand) || match_keyword("AND"))
        {
            expression = make_binary(BinaryOperator::LogicalAnd, expression, parse_bitwise_or());
        }
        return expression;
    }

    ExpressionPtr Parser::parse_bitwise_or()
    {
        auto expression = parse_bitwise_xor();
        while (consume_optional(TokenType::Pipe))
        {
            expression = make_binary(BinaryOperator::BitwiseOr, expression, parse_bitwise_xor());
        }
        return expression;
    }

    ExpressionPtr Parser::parse_bitwise_xor()
    {
        auto expression = parse_bitwise_and();
        while (consume_optional(TokenType::Caret))
        {
            expression = make_binary(BinaryOperator::BitwiseXor, expression, parse_bitwise_and());
        }
        return expression;
    }

    ExpressionPtr Parser::parse_bitwise_and()
    {
        auto expression = parse_equality();
        while (consume_optional(TokenType::Ampersand))
        {
            expression = make_binary(BinaryOperator::BitwiseAnd, expression, parse_equality());
        }
        return expression;
    }

    ExpressionPtr Parser::parse_equality()
    {
        auto expression = parse_relational();
        while (true)
        {
            if (consume_optional(TokenType::DoubleEqual) || consume_optional(TokenType::Equal))
            {
                std::optional<SubqueryQuantifier> quantifier;
                if (match_keyword("ANY"))
                {
                    quantifier = SubqueryQuantifier::Any;
                }
                else if (match_keyword("ALL"))
                {
                    quantifier = SubqueryQuantifier::All;
                }

                if (quantifier.has_value())
                {
                    expect(TokenType::LParen, "Expected '(' after ANY/ALL");
                    if (!match_keyword("SELECT"))
                    {
                        fail("ANY and ALL currently require a SELECT subquery");
                    }
                    auto statement = parse_select_statement();
                    expect(TokenType::RParen, "Expected ')' after ANY/ALL subquery");
                    expression = make_quantified_binary(BinaryOperator::Equal, expression, std::move(statement), *quantifier);
                }
                else
                {
                    expression = make_binary(BinaryOperator::Equal, expression, parse_relational());
                }
            }
            else if (consume_optional(TokenType::NotEqual))
            {
                std::optional<SubqueryQuantifier> quantifier;
                if (match_keyword("ANY"))
                {
                    quantifier = SubqueryQuantifier::Any;
                }
                else if (match_keyword("ALL"))
                {
                    quantifier = SubqueryQuantifier::All;
                }

                if (quantifier.has_value())
                {
                    expect(TokenType::LParen, "Expected '(' after ANY/ALL");
                    if (!match_keyword("SELECT"))
                    {
                        fail("ANY and ALL currently require a SELECT subquery");
                    }
                    auto statement = parse_select_statement();
                    expect(TokenType::RParen, "Expected ')' after ANY/ALL subquery");
                    expression = make_quantified_binary(BinaryOperator::NotEqual, expression, std::move(statement), *quantifier);
                }
                else
                {
                    expression = make_binary(BinaryOperator::NotEqual, expression, parse_relational());
                }
            }
            else
            {
                break;
            }
        }
        return expression;
    }

    ExpressionPtr Parser::parse_relational()
    {
        auto expression = parse_additive();
        while (true)
        {
            auto parse_quantified = [&](BinaryOperator op, auto&& parse_rhs) -> ExpressionPtr
            {
                std::optional<SubqueryQuantifier> quantifier;
                if (match_keyword("ANY"))
                {
                    quantifier = SubqueryQuantifier::Any;
                }
                else if (match_keyword("ALL"))
                {
                    quantifier = SubqueryQuantifier::All;
                }

                if (quantifier.has_value())
                {
                    expect(TokenType::LParen, "Expected '(' after ANY/ALL");
                    if (!match_keyword("SELECT"))
                    {
                        fail("ANY and ALL currently require a SELECT subquery");
                    }
                    auto statement = parse_select_statement();
                    expect(TokenType::RParen, "Expected ')' after ANY/ALL subquery");
                    return make_quantified_binary(op, expression, std::move(statement), *quantifier);
                }
                return make_binary(op, expression, parse_rhs());
            };

            if (consume_optional(TokenType::Less))
            {
                expression = parse_quantified(BinaryOperator::Less, [&]() { return parse_additive(); });
            }
            else if (consume_optional(TokenType::LessEqual))
            {
                expression = parse_quantified(BinaryOperator::LessEqual, [&]() { return parse_additive(); });
            }
            else if (consume_optional(TokenType::Greater))
            {
                expression = parse_quantified(BinaryOperator::Greater, [&]() { return parse_additive(); });
            }
            else if (consume_optional(TokenType::GreaterEqual))
            {
                expression = parse_quantified(BinaryOperator::GreaterEqual, [&]() { return parse_additive(); });
            }
            else if (match_keyword("IS"))
            {
                const bool negated = match_keyword("NOT");
                if (!match_keyword("NULL"))
                {
                    fail("IS currently only supports NULL");
                }
                expression = make_binary(negated ? BinaryOperator::IsNot : BinaryOperator::Is, expression, make_null());
            }
            else if (match_keyword("IN"))
            {
                expect(TokenType::LParen, "Expected '(' after IN");
                if (match_keyword("SELECT"))
                {
                    auto statement = parse_select_statement();
                    expect(TokenType::RParen, "Expected ')' after IN subquery");
                    expression = make_binary(BinaryOperator::In, expression, make_select(std::move(statement)));
                }
                else
                {
                    auto items = parse_expression_list();
                    expect(TokenType::RParen, "Expected ')' after IN list");
                    expression = make_binary(BinaryOperator::In, expression, make_list(std::move(items)));
                }
            }
            else if (match_keyword("NOT"))
            {
                if (!match_keyword("IN"))
                {
                    fail("Expected IN after NOT");
                }

                expect(TokenType::LParen, "Expected '(' after NOT IN");
                if (match_keyword("SELECT"))
                {
                    auto statement = parse_select_statement();
                    expect(TokenType::RParen, "Expected ')' after NOT IN subquery");
                    expression = make_binary(BinaryOperator::NotIn, expression, make_select(std::move(statement)));
                }
                else
                {
                    auto items = parse_expression_list();
                    expect(TokenType::RParen, "Expected ')' after NOT IN list");
                    expression = make_binary(BinaryOperator::NotIn, expression, make_list(std::move(items)));
                }
            }
            else if (match_keyword("BETWEEN"))
            {
                const auto lower = parse_additive();
                expect_keyword("AND");
                const auto upper = parse_additive();
                expression = make_binary(BinaryOperator::LogicalAnd,
                                         make_binary(BinaryOperator::GreaterEqual, expression, lower),
                                         make_binary(BinaryOperator::LessEqual, expression, upper));
            }
            else if (match_keyword("LIKE"))
            {
                expression = make_binary(BinaryOperator::Like, expression, parse_additive());
            }
            else if (match_keyword("REGEXP"))
            {
                expression = make_binary(BinaryOperator::Regexp, expression, parse_additive());
            }
            else
            {
                break;
            }
        }
        return expression;
    }

    ExpressionPtr Parser::parse_additive()
    {
        auto expression = parse_multiplicative();
        while (true)
        {
            if (consume_optional(TokenType::Plus))
            {
                expression = make_binary(BinaryOperator::Add, expression, parse_multiplicative());
            }
            else if (consume_optional(TokenType::Minus))
            {
                expression = make_binary(BinaryOperator::Subtract, expression, parse_multiplicative());
            }
            else
            {
                break;
            }
        }
        return expression;
    }

    ExpressionPtr Parser::parse_multiplicative()
    {
        auto expression = parse_unary();
        while (true)
        {
            if (consume_optional(TokenType::Star))
            {
                expression = make_binary(BinaryOperator::Multiply, expression, parse_unary());
            }
            else if (consume_optional(TokenType::Slash))
            {
                expression = make_binary(BinaryOperator::Divide, expression, parse_unary());
            }
            else if (consume_optional(TokenType::Percent))
            {
                expression = make_binary(BinaryOperator::Modulo, expression, parse_unary());
            }
            else
            {
                break;
            }
        }
        return expression;
    }

    ExpressionPtr Parser::parse_unary()
    {
        if (consume_optional(TokenType::Plus))
        {
            return make_unary(UnaryOperator::Plus, parse_unary());
        }
        if (consume_optional(TokenType::Minus))
        {
            return make_unary(UnaryOperator::Minus, parse_unary());
        }
        if (consume_optional(TokenType::Exclamation) || match_keyword("NOT"))
        {
            return make_unary(UnaryOperator::LogicalNot, parse_unary());
        }
        if (consume_optional(TokenType::Tilde))
        {
            return make_unary(UnaryOperator::BitwiseNot, parse_unary());
        }
        return parse_primary();
    }

    ExpressionPtr Parser::parse_primary()
    {
        if (match_keyword("EXISTS"))
        {
            expect(TokenType::LParen, "Expected '(' after EXISTS");
            if (!match_keyword("SELECT"))
            {
                fail("EXISTS requires a SELECT subquery");
            }
            auto statement = parse_select_statement();
            expect(TokenType::RParen, "Expected ')' after EXISTS subquery");
            return make_exists(std::move(statement));
        }

        if (consume_optional(TokenType::LParen))
        {
            if (match_keyword("SELECT"))
            {
                auto statement = parse_select_statement();
                expect(TokenType::RParen, "Expected ')' after SELECT expression");
                return make_select(std::move(statement));
            }
            auto expression = parse_expression();
            expect(TokenType::RParen, "Expected ')' after expression");
            return expression;
        }

        if (check(TokenType::String) || check(TokenType::Number))
        {
            return make_literal(advance().text);
        }
        if (match_keyword("NULL"))
        {
            return make_null();
        }
        if (check(TokenType::Identifier))
        {
            const std::string text = parse_identifier_reference("Expected identifier");
            if (consume_optional(TokenType::LParen))
            {
                std::vector<ExpressionPtr> arguments;
                bool uses_star = false;
                if (consume_optional(TokenType::Star))
                {
                    uses_star = true;
                }
                else if (!check(TokenType::RParen))
                {
                    arguments = parse_expression_list();
                }
                expect(TokenType::RParen, "Expected ')' after function call");
                return make_function(text, std::move(arguments), uses_star);
            }
            return make_identifier(text);
        }
        fail("Expected expression");
    }

    ExpressionPtr Parser::make_literal(std::string text) const
    {
        auto expression = std::make_shared<Expression>();
        expression->kind = ExpressionKind::Literal;
        expression->text = std::move(text);
        return expression;
    }

    ExpressionPtr Parser::make_null() const
    {
        auto expression = std::make_shared<Expression>();
        expression->kind = ExpressionKind::Null;
        return expression;
    }

    ExpressionPtr Parser::make_identifier(std::string text) const
    {
        auto expression = std::make_shared<Expression>();
        expression->kind = ExpressionKind::Identifier;
        expression->text = std::move(text);
        return expression;
    }

    ExpressionPtr Parser::make_function(std::string text, std::vector<ExpressionPtr> arguments, bool uses_star) const
    {
        auto expression = std::make_shared<Expression>();
        expression->kind = ExpressionKind::FunctionCall;
        expression->text = std::move(text);
        expression->arguments = std::move(arguments);
        expression->function_uses_star = uses_star;
        return expression;
    }

    ExpressionPtr Parser::make_select(SelectStatement statement) const
    {
        auto expression = std::make_shared<Expression>();
        expression->kind = ExpressionKind::Select;
        expression->select = std::make_shared<SelectStatement>(std::move(statement));
        return expression;
    }

    ExpressionPtr Parser::make_list(std::vector<ExpressionPtr> arguments) const
    {
        auto expression = std::make_shared<Expression>();
        expression->kind = ExpressionKind::List;
        expression->arguments = std::move(arguments);
        return expression;
    }

    ExpressionPtr Parser::make_exists(SelectStatement statement) const
    {
        auto expression = std::make_shared<Expression>();
        expression->kind = ExpressionKind::Exists;
        expression->select = std::make_shared<SelectStatement>(std::move(statement));
        return expression;
    }

    ExpressionPtr Parser::make_unary(UnaryOperator op, ExpressionPtr operand) const
    {
        auto expression = std::make_shared<Expression>();
        expression->kind = ExpressionKind::Unary;
        expression->unary_operator = op;
        expression->left = std::move(operand);
        return expression;
    }

    ExpressionPtr Parser::make_binary(BinaryOperator op, ExpressionPtr left, ExpressionPtr right) const
    {
        auto expression = std::make_shared<Expression>();
        expression->kind = ExpressionKind::Binary;
        expression->binary_operator = op;
        expression->left = std::move(left);
        expression->right = std::move(right);
        return expression;
    }

    ExpressionPtr Parser::make_quantified_binary(BinaryOperator op, ExpressionPtr left, SelectStatement statement, SubqueryQuantifier quantifier) const
    {
        auto expression = std::make_shared<Expression>();
        expression->kind = ExpressionKind::Binary;
        expression->binary_operator = op;
        expression->subquery_quantifier = quantifier;
        expression->left = std::move(left);
        expression->right = make_select(std::move(statement));
        return expression;
    }

    std::size_t Parser::parse_non_negative_integer(const std::string& message)
    {
        if (!check(TokenType::Number))
        {
            fail(message);
        }

        const auto token = advance();
        if (!token.text.empty() && token.text.front() == '-')
        {
            fail(message);
        }

        try
        {
            const auto value = std::stoull(token.text);
            if (value > std::numeric_limits<std::size_t>::max())
            {
                fail(message);
            }
            return static_cast<std::size_t>(value);
        }
        catch (const std::exception&)
        {
            fail(message);
        }
    }
}

