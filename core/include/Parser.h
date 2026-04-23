#pragma once

#include "SqlTypes.h"

#include <cstddef>
#include <string>
#include <vector>

namespace sql
{
    /// @brief Parses tokens into executable SQL statements.
    class Parser
    {
    public:
        /// @brief Initializes the parser with a token stream.
        /// @param tokens Tokens produced by the tokenizer.
        explicit Parser(std::vector<Token> tokens);

        /// @brief Parses a single SQL statement.
        /// @return Parsed statement.
        Statement parse_statement();

        /// @brief Parses a full expression.
        /// @return Parsed expression tree.
        ExpressionPtr parse_expression();

    private:
        /// @brief Parses a `CREATE TABLE` or `CREATE VIEW` statement.
        Statement parse_create();

        /// @brief Parses an `ALTER TABLE` or `ALTER VIEW` statement.
        Statement parse_alter();

        /// @brief Parses a `DROP TABLE` or `DROP VIEW` statement.
        Statement parse_drop();

        /// @brief Parses a `DELETE FROM` statement.
        Statement parse_delete();

        /// @brief Parses an `INSERT INTO` statement.
        Statement parse_insert();

        /// @brief Parses a `SELECT` statement.
        Statement parse_select();

        /// @brief Parses the body of a `SELECT` statement after the `SELECT` keyword.
        /// @return Parsed select payload.
        SelectStatement parse_select_statement();

        /// @brief Parses an `UPDATE` statement.
        Statement parse_update();

        /// @brief Parses a comma-separated identifier list.
        /// @return Parsed identifiers.
        std::vector<std::string> parse_identifier_list();

        /// @brief Parses a comma-separated column definition list.
        /// @return Parsed column definitions.
        std::vector<ColumnDefinition> parse_column_definition_list();

        /// @brief Parses a single column definition.
        /// @return Parsed column definition.
        ColumnDefinition parse_column_definition();

        /// @brief Parses a comma-separated value list.
        /// @return Parsed values.
        std::vector<ExpressionPtr> parse_value_list();

        /// @brief Parses a comma-separated expression list.
        /// @return Parsed expressions.
        std::vector<ExpressionPtr> parse_expression_list();

        /// @brief Parses a comma-separated ORDER BY list.
        /// @return Parsed ORDER BY terms.
        std::vector<SelectOrderBy> parse_order_by_list();

        /// @brief Parses a comma-separated FROM source list.
        /// @return Parsed SELECT sources.
        std::vector<SelectSource> parse_select_source_list();

        /// @brief Parses a single FROM source.
        /// @return Parsed source.
        SelectSource parse_select_source();

        /// @brief Parses logical OR expressions.
        /// @return Parsed expression tree.
        ExpressionPtr parse_logical_or();

        /// @brief Parses logical AND expressions.
        /// @return Parsed expression tree.
        ExpressionPtr parse_logical_and();

        /// @brief Parses bitwise OR expressions.
        /// @return Parsed expression tree.
        ExpressionPtr parse_bitwise_or();

        /// @brief Parses bitwise XOR expressions.
        /// @return Parsed expression tree.
        ExpressionPtr parse_bitwise_xor();

        /// @brief Parses bitwise AND expressions.
        /// @return Parsed expression tree.
        ExpressionPtr parse_bitwise_and();

        /// @brief Parses equality expressions.
        /// @return Parsed expression tree.
        ExpressionPtr parse_equality();

        /// @brief Parses relational expressions.
        /// @return Parsed expression tree.
        ExpressionPtr parse_relational();

        /// @brief Parses additive expressions.
        /// @return Parsed expression tree.
        ExpressionPtr parse_additive();

        /// @brief Parses multiplicative expressions.
        /// @return Parsed expression tree.
        ExpressionPtr parse_multiplicative();

        /// @brief Parses unary expressions.
        /// @return Parsed expression tree.
        ExpressionPtr parse_unary();

        /// @brief Parses primary expressions.
        /// @return Parsed expression tree.
        ExpressionPtr parse_primary();

        /// @brief Creates a literal expression node.
        /// @param text Literal text.
        /// @return Expression node.
        ExpressionPtr make_literal(std::string text) const;

        /// @brief Creates a NULL literal expression node.
        /// @return Expression node.
        ExpressionPtr make_null() const;

        /// @brief Creates an identifier expression node.
        /// @param text Identifier text.
        /// @return Expression node.
        ExpressionPtr make_identifier(std::string text) const;

        /// @brief Creates a function call expression node.
        /// @param text Function name.
        /// @param arguments Function arguments.
        /// @param uses_star Indicates whether the function call used `*`.
        /// @return Expression node.
        ExpressionPtr make_function(std::string text, std::vector<ExpressionPtr> arguments = {}, bool uses_star = false) const;

        /// @brief Creates a select expression node.
        /// @param statement Nested select statement.
        /// @return Expression node.
        ExpressionPtr make_select(SelectStatement statement) const;

        /// @brief Creates an inline expression-list node.
        /// @param arguments List items.
        /// @return Expression node.
        ExpressionPtr make_list(std::vector<ExpressionPtr> arguments) const;

        /// @brief Creates an EXISTS expression node.
        /// @param statement Nested select statement.
        /// @return Expression node.
        ExpressionPtr make_exists(SelectStatement statement) const;

        /// @brief Creates a unary expression node.
        /// @param op Unary operator.
        /// @param operand Operand expression.
        /// @return Expression node.
        ExpressionPtr make_unary(UnaryOperator op, ExpressionPtr operand) const;

        /// @brief Creates a binary expression node.
        /// @param op Binary operator.
        /// @param left Left operand.
        /// @param right Right operand.
        /// @return Expression node.
        ExpressionPtr make_binary(BinaryOperator op, ExpressionPtr left, ExpressionPtr right) const;

        /// @brief Creates a quantified binary expression node such as `= ANY (SELECT ...)`.
        /// @param op Comparison operator.
        /// @param left Left operand.
        /// @param statement Nested select statement.
        /// @param quantifier Subquery quantifier.
        /// @return Expression node.
        ExpressionPtr make_quantified_binary(BinaryOperator op, ExpressionPtr left, SelectStatement statement, SubqueryQuantifier quantifier) const;

        /// @brief Parses a non-negative integer literal.
        /// @param message Error message for invalid values.
        /// @return Parsed integer value.
        std::size_t parse_non_negative_integer(const std::string& message);

        /// @brief Consumes and returns an identifier.
        /// @param message Error message if the next token is not an identifier.
        /// @return Identifier text.
        std::string expect_identifier(const std::string& message);

        /// @brief Consumes and returns a table/view identifier or quoted file path.
        /// @param message Error message if the next token is neither an identifier nor a string.
        /// @return Parsed relation reference.
        RelationReference expect_relation_reference(const std::string& message);

        /// @brief Parses a possibly qualified identifier reference such as `source.column`.
        /// @param message Error message if the next token is not an identifier.
        /// @return Identifier path.
        std::string parse_identifier_reference(const std::string& message);

        /// @brief Consumes a keyword.
        /// @param keyword Expected keyword.
        void expect_keyword(const std::string& keyword);

        /// @brief Matches a keyword case-insensitively.
        /// @param keyword Keyword to match.
        /// @return `true` if matched; otherwise `false`.
        bool match_keyword(const std::string& keyword);

        /// @brief Consumes a token if it matches the requested type.
        /// @param type Token type to consume.
        /// @return `true` if consumed; otherwise `false`.
        bool consume_optional(TokenType type);

        /// @brief Consumes a token of the requested type.
        /// @param type Expected token type.
        /// @param message Error message if the token does not match.
        void expect(TokenType type, const std::string& message);

        /// @brief Checks the current token type.
        /// @param type Token type to test.
        /// @return `true` if the current token matches; otherwise `false`.
        bool check(TokenType type) const;

        /// @brief Returns the current token.
        /// @return Current token reference.
        const Token& peek() const;

        /// @brief Consumes and returns the current token.
        /// @return Consumed token.
        Token advance();

        /// @brief Token stream.
        std::vector<Token> tokens_;

        /// @brief Current token index.
        std::size_t position_ = 0;
    };
}
