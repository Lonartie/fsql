#include "Parser.h"

#include "SqlError.h"
#include "StringUtils.h"

#include <limits>
#include <utility>

namespace sql
{
    Parser::Parser(std::vector<Token> tokens) : tokens_(std::move(tokens))
    {
    }

    Statement Parser::parse_statement()
    {
        if (match_keyword("CREATE"))
        {
            return parse_create();
        }
        if (match_keyword("DROP"))
        {
            return parse_drop();
        }
        if (match_keyword("DELETE"))
        {
            return parse_delete();
        }
        if (match_keyword("INSERT"))
        {
            return parse_insert();
        }
        if (match_keyword("SELECT"))
        {
            return parse_select();
        }
        if (match_keyword("UPDATE"))
        {
            return parse_update();
        }

        fail("Expected CREATE, DROP, DELETE, INSERT, SELECT, or UPDATE");
    }

    Statement Parser::parse_create()
    {
        expect_keyword("TABLE");

        CreateStatement stmt;
        stmt.table_name = expect_identifier("Expected table name");
        expect(TokenType::LParen, "Expected '(' after table name");
        stmt.columns = parse_column_definition_list();
        expect(TokenType::RParen, "Expected ')' after column list");
        consume_optional(TokenType::Semicolon);
        expect(TokenType::End, "Unexpected tokens after CREATE TABLE");

        Statement statement;
        statement.kind = Statement::Kind::Create;
        statement.create = std::move(stmt);
        return statement;
    }

    Statement Parser::parse_drop()
    {
        expect_keyword("TABLE");

        DropStatement stmt;
        stmt.table_name = expect_identifier("Expected table name");
        consume_optional(TokenType::Semicolon);
        expect(TokenType::End, "Unexpected tokens after DROP TABLE");

        Statement statement;
        statement.kind = Statement::Kind::Drop;
        statement.drop = std::move(stmt);
        return statement;
    }

    Statement Parser::parse_delete()
    {
        expect_keyword("FROM");

        DeleteStatement stmt;
        stmt.table_name = expect_identifier("Expected table name");
        if (match_keyword("WHERE"))
        {
            stmt.where = parse_expression();
        }
        consume_optional(TokenType::Semicolon);
        expect(TokenType::End, "Unexpected tokens after DELETE FROM");

        Statement statement;
        statement.kind = Statement::Kind::Delete;
        statement.delete_statement = std::move(stmt);
        return statement;
    }

    Statement Parser::parse_insert()
    {
        expect_keyword("INTO");

        InsertStatement stmt;
        stmt.table_name = expect_identifier("Expected table name");
        if (check(TokenType::LParen))
        {
            advance();
            stmt.columns = parse_identifier_list();
            expect(TokenType::RParen, "Expected ')' after insert column list");
        }

        expect_keyword("VALUES");
        expect(TokenType::LParen, "Expected '(' before VALUES list");
        if (!check(TokenType::RParen))
        {
            stmt.values = parse_value_list();
        }
        expect(TokenType::RParen, "Expected ')' after VALUES list");
        consume_optional(TokenType::Semicolon);
        expect(TokenType::End, "Unexpected tokens after INSERT");

        Statement statement;
        statement.kind = Statement::Kind::Insert;
        statement.insert = std::move(stmt);
        return statement;
    }

    Statement Parser::parse_select()
    {
        Statement statement;
        statement.kind = Statement::Kind::Select;
        statement.select = parse_select_statement();

        consume_optional(TokenType::Semicolon);
        expect(TokenType::End, "Unexpected tokens after SELECT");
        return statement;
    }

    SelectStatement Parser::parse_select_statement()
    {
        SelectStatement stmt;
        if (match_keyword("DISTINCT") || match_keyword("UNIQUE"))
        {
            stmt.distinct = true;
        }

        if (check(TokenType::Star))
        {
            advance();
            stmt.select_all = true;
        }
        else
        {
            stmt.projections = parse_expression_list();
        }

        expect_keyword("FROM");
        stmt.table_name = expect_identifier("Expected table name");
        if (match_keyword("WHERE"))
        {
            stmt.where = parse_expression();
        }

        if (match_keyword("ORDER"))
        {
            expect_keyword("BY");
            stmt.order_by = parse_order_by_list();
        }

        if (match_keyword("LIMIT"))
        {
            stmt.limit = parse_non_negative_integer("Expected non-negative integer after LIMIT");
        }

        if (match_keyword("OFFSET"))
        {
            stmt.offset = parse_non_negative_integer("Expected non-negative integer after OFFSET");
        }

        return stmt;
    }

    Statement Parser::parse_update()
    {
        UpdateStatement stmt;
        stmt.table_name = expect_identifier("Expected table name");
        expect_keyword("SET");
        while (true)
        {
            const std::string column = expect_identifier("Expected column name in assignment");
            expect(TokenType::Equal, "Expected '=' in assignment");
            stmt.assignments.emplace_back(column, parse_expression());
            if (!consume_optional(TokenType::Comma))
            {
                break;
            }
        }

        if (match_keyword("WHERE"))
        {
            stmt.where = parse_expression();
        }

        consume_optional(TokenType::Semicolon);
        expect(TokenType::End, "Unexpected tokens after UPDATE");

        Statement statement;
        statement.kind = Statement::Kind::Update;
        statement.update = std::move(stmt);
        return statement;
    }

    std::vector<std::string> Parser::parse_identifier_list()
    {
        std::vector<std::string> values;
        values.push_back(expect_identifier("Expected identifier"));
        while (consume_optional(TokenType::Comma))
        {
            values.push_back(expect_identifier("Expected identifier after ','"));
        }
        return values;
    }

    std::vector<ColumnDefinition> Parser::parse_column_definition_list()
    {
        std::vector<ColumnDefinition> values;
        values.push_back(parse_column_definition());
        while (consume_optional(TokenType::Comma))
        {
            values.push_back(parse_column_definition());
        }
        return values;
    }

    ColumnDefinition Parser::parse_column_definition()
    {
        ColumnDefinition definition;
        definition.name = expect_identifier("Expected column name");
        if (match_keyword("AUTO_INCREMENT"))
        {
            definition.auto_increment = true;
        }
        if (consume_optional(TokenType::Equal))
        {
            definition.default_value = parse_expression();
        }
        return definition;
    }

    std::vector<ExpressionPtr> Parser::parse_value_list()
    {
        return parse_expression_list();
    }

    std::vector<ExpressionPtr> Parser::parse_expression_list()
    {
        std::vector<ExpressionPtr> values;
        values.push_back(parse_expression());
        while (consume_optional(TokenType::Comma))
        {
            values.push_back(parse_expression());
        }
        return values;
    }

    std::vector<SelectOrderBy> Parser::parse_order_by_list()
    {
        std::vector<SelectOrderBy> values;
        while (true)
        {
            SelectOrderBy order_by;
            order_by.expression = parse_expression();
            if (match_keyword("DESC"))
            {
                order_by.descending = true;
            }
            else
            {
                match_keyword("ASC");
            }
            values.push_back(std::move(order_by));
            if (!consume_optional(TokenType::Comma))
            {
                break;
            }
        }
        return values;
    }

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
                expression = make_binary(BinaryOperator::Equal, expression, parse_relational());
            }
            else if (consume_optional(TokenType::NotEqual))
            {
                expression = make_binary(BinaryOperator::NotEqual, expression, parse_relational());
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
            if (consume_optional(TokenType::Less))
            {
                expression = make_binary(BinaryOperator::Less, expression, parse_additive());
            }
            else if (consume_optional(TokenType::LessEqual))
            {
                expression = make_binary(BinaryOperator::LessEqual, expression, parse_additive());
            }
            else if (consume_optional(TokenType::Greater))
            {
                expression = make_binary(BinaryOperator::Greater, expression, parse_additive());
            }
            else if (consume_optional(TokenType::GreaterEqual))
            {
                expression = make_binary(BinaryOperator::GreaterEqual, expression, parse_additive());
            }
            else if (match_keyword("BETWEEN"))
            {
                const auto lower = parse_additive();
                expect_keyword("AND");
                const auto upper = parse_additive();
                expression = make_binary(
                    BinaryOperator::LogicalAnd,
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
        if (consume_optional(TokenType::Exclamation))
        {
            return make_unary(UnaryOperator::LogicalNot, parse_unary());
        }
        if (match_keyword("NOT"))
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

        if (check(TokenType::Identifier))
        {
            const std::string text = advance().text;
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

    std::string Parser::expect_identifier(const std::string& message)
    {
        if (!check(TokenType::Identifier))
        {
            fail(message);
        }

        return advance().text;
    }

    void Parser::expect_keyword(const std::string& keyword)
    {
        if (!match_keyword(keyword))
        {
            fail("Expected keyword " + keyword);
        }
    }

    bool Parser::match_keyword(const std::string& keyword)
    {
        if (check(TokenType::Identifier) && iequals(peek().text, keyword))
        {
            advance();
            return true;
        }

        return false;
    }

    bool Parser::consume_optional(TokenType type)
    {
        if (check(type))
        {
            advance();
            return true;
        }

        return false;
    }

    void Parser::expect(TokenType type, const std::string& message)
    {
        if (!check(type))
        {
            fail(message);
        }

        advance();
    }

    bool Parser::check(TokenType type) const
    {
        return peek().type == type;
    }

    const Token& Parser::peek() const
    {
        return tokens_[position_];
    }

    Token Parser::advance()
    {
        return tokens_[position_++];
    }
}
