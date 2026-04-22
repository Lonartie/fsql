#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace sql
{
    /// @brief Represents a single CSV row.
    using Row = std::vector<std::string>;

    /// @brief Represents a loaded CSV-backed table.
    /// @details The first row of the CSV file is treated as the header and stored
    /// in @ref columns. Remaining rows are stored in @ref rows.
    struct Table
    {
        /// @brief Logical table name without the `.csv` extension.
        std::string name;

        /// @brief Ordered list of column names.
        std::vector<std::string> columns;

        /// @brief Table data rows.
        std::vector<Row> rows;
    };

    /// @brief Supported token kinds produced by the tokenizer.
    enum class TokenType
    {
        Identifier,
        String,
        Number,
        Comma,
        Dot,
        LParen,
        RParen,
        Semicolon,
        Equal,
        DoubleEqual,
        NotEqual,
        Less,
        LessEqual,
        Greater,
        GreaterEqual,
        Plus,
        Minus,
        Slash,
        Percent,
        Ampersand,
        Pipe,
        Caret,
        Tilde,
        Exclamation,
        DoubleAmpersand,
        DoublePipe,
        Star,
        End
    };

    /// @brief Represents a lexical token.
    struct Token
    {
        /// @brief Token category.
        TokenType type{};

        /// @brief Original or normalized token text.
        std::string text;
    };

    /// @brief Supported expression node kinds.
    enum class ExpressionKind
    {
        Literal,
        Identifier,
        Select,
        FunctionCall,
        Unary,
        Binary
    };

    /// @brief Supported unary operators.
    enum class UnaryOperator
    {
        Plus,
        Minus,
        LogicalNot,
        BitwiseNot
    };

    /// @brief Supported binary operators.
    enum class BinaryOperator
    {
        Multiply,
        Divide,
        Modulo,
        Add,
        Subtract,
        Less,
        LessEqual,
        Greater,
        GreaterEqual,
        Like,
        Regexp,
        Equal,
        NotEqual,
        BitwiseAnd,
        BitwiseXor,
        BitwiseOr,
        LogicalAnd,
        LogicalOr
    };

    struct Expression;
    struct SelectStatement;

    /// @brief Shared pointer type for expression nodes.
    using ExpressionPtr = std::shared_ptr<Expression>;

    /// @brief Shared pointer type for select statement nodes embedded in expressions.
    using SelectStatementPtr = std::shared_ptr<SelectStatement>;

    /// @brief Represents an expression tree node.
    struct Expression
    {
        /// @brief Expression node kind.
        ExpressionKind kind = ExpressionKind::Literal;

        /// @brief Literal text, identifier name, or function name.
        std::string text;

        /// @brief Nested select statement when @ref kind is `Select`.
        SelectStatementPtr select;

        /// @brief Function arguments when @ref kind is `FunctionCall`.
        std::vector<ExpressionPtr> arguments;

        /// @brief Indicates whether a function call used `*` as its argument list.
        bool function_uses_star = false;

        /// @brief Unary operator when @ref kind is `Unary`.
        UnaryOperator unary_operator = UnaryOperator::Plus;

        /// @brief Binary operator when @ref kind is `Binary`.
        BinaryOperator binary_operator = BinaryOperator::Add;

        /// @brief Left operand for unary and binary expressions.
        ExpressionPtr left;

        /// @brief Right operand for binary expressions.
        ExpressionPtr right;
    };

    /// @brief Represents a declared table column.
    struct ColumnDefinition
    {
        /// @brief Column name.
        std::string name;

        /// @brief Indicates whether the column auto-increments.
        bool auto_increment = false;

        /// @brief Optional default expression applied when no value is provided.
        ExpressionPtr default_value;
    };

    /// @brief Parsed `CREATE TABLE` statement.
    struct CreateStatement
    {
        /// @brief Target table name.
        std::string table_name;

        /// @brief Declared columns and attributes.
        std::vector<ColumnDefinition> columns;
    };

    /// @brief Parsed `DROP TABLE` statement.
    struct DropStatement
    {
        /// @brief Target table name.
        std::string table_name;
    };

    /// @brief Parsed `DELETE FROM` statement.
    struct DeleteStatement
    {
        /// @brief Target table name.
        std::string table_name;

        /// @brief Optional filter expression.
        ExpressionPtr where;
    };

    /// @brief Parsed `INSERT INTO` statement.
    struct InsertStatement
    {
        /// @brief Target table name.
        std::string table_name;

        /// @brief Optional explicit target columns.
        std::optional<std::vector<std::string>> columns;

        /// @brief Values to insert.
        std::vector<ExpressionPtr> values;
    };

    /// @brief Parsed `SELECT` statement.
    struct SelectOrderBy
    {
        /// @brief Expression used for ordering.
        ExpressionPtr expression;

        /// @brief Indicates descending order when `true`.
        bool descending = false;
    };

    /// @brief Parsed SELECT source.
    struct SelectSource
    {
        /// @brief Supported source kinds.
        enum class Kind
        {
            Table,
            Subquery
        } kind = Kind::Table;

        /// @brief Table name when @ref kind is `Table`.
        std::string name;

        /// @brief Nested select statement when @ref kind is `Subquery`.
        SelectStatementPtr subquery;

        /// @brief Optional source alias.
        std::optional<std::string> alias;
    };

    /// @brief Parsed `SELECT` statement.
    struct SelectStatement
    {
        /// @brief Source tables or subqueries.
        std::vector<SelectSource> sources;

        /// @brief Indicates whether duplicate projected rows should be removed.
        bool distinct = false;

        /// @brief Indicates whether `*` was used.
        bool select_all = false;

        /// @brief Explicitly selected projections.
        std::vector<ExpressionPtr> projections;

        /// @brief Optional filter expression.
        ExpressionPtr where;

        /// @brief Optional GROUP BY expressions.
        std::vector<ExpressionPtr> group_by;

        /// @brief Optional HAVING filter applied after grouping.
        ExpressionPtr having;

        /// @brief Optional ORDER BY terms.
        std::vector<SelectOrderBy> order_by;

        /// @brief Optional LIMIT clause.
        std::optional<std::size_t> limit;

        /// @brief Optional OFFSET clause.
        std::optional<std::size_t> offset;
    };

    /// @brief Parsed `UPDATE` statement.
    struct UpdateStatement
    {
        /// @brief Target table name.
        std::string table_name;

        /// @brief Column/value assignments.
        std::vector<std::pair<std::string, ExpressionPtr>> assignments;

        /// @brief Optional filter expression.
        ExpressionPtr where;
    };

    /// @brief Represents a parsed SQL statement.
    struct Statement
    {
        /// @brief Supported statement kinds.
        enum class Kind
        {
            Create,
            Drop,
            Delete,
            Insert,
            Select,
            Update
        } kind{};

        /// @brief `CREATE TABLE` payload.
        CreateStatement create;

        /// @brief `DROP TABLE` payload.
        DropStatement drop;

        /// @brief `DELETE FROM` payload.
        DeleteStatement delete_statement;

        /// @brief `INSERT INTO` payload.
        InsertStatement insert;

        /// @brief `SELECT` payload.
        SelectStatement select;

        /// @brief `UPDATE` payload.
        UpdateStatement update;
    };
}
