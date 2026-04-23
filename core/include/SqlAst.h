#pragma once

#include "SqlData.h"

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace sql
{
    /// @brief Supported expression node kinds.
    enum class ExpressionKind
    {
        Literal,
        Null,
        Identifier,
        Select,
        Exists,
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
        Is,
        IsNot,
        In,
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

    /// @brief Quantifier applied to a SELECT subquery predicate.
    enum class SubqueryQuantifier
    {
        None,
        Any,
        All
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

        /// @brief Optional subquery quantifier for quantified comparisons.
        SubqueryQuantifier subquery_quantifier = SubqueryQuantifier::None;

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

    /// @brief Represents a table or view reference used by parsed statements.
    struct RelationReference
    {
        /// @brief Supported relation reference kinds.
        enum class Kind
        {
            Identifier,
            FilePath
        } kind = Kind::Identifier;

        /// @brief Logical identifier text or explicit file path.
        std::string name;
    };

    /// @brief Parsed `CREATE TABLE` or `CREATE VIEW` statement.
    struct CreateStatement
    {
        /// @brief Target schema object kind.
        SchemaObjectKind object_kind = SchemaObjectKind::Table;

        /// @brief Target table or view name.
        RelationReference table_name;

        /// @brief Declared columns and attributes for table creation.
        std::vector<ColumnDefinition> columns;

        /// @brief Stored query for view creation.
        SelectStatementPtr view_query;
    };

    /// @brief Parsed `DROP TABLE` or `DROP VIEW` statement.
    struct DropStatement
    {
        /// @brief Target schema object kind.
        SchemaObjectKind object_kind = SchemaObjectKind::Table;

        /// @brief Target table or view name.
        RelationReference table_name;
    };

    /// @brief Parsed `DELETE FROM` statement.
    struct DeleteStatement
    {
        /// @brief Target table name.
        RelationReference table_name;

        /// @brief Optional filter expression.
        ExpressionPtr where;
    };

    /// @brief Parsed `INSERT INTO` statement.
    struct InsertStatement
    {
        /// @brief Target table name.
        RelationReference table_name;

        /// @brief Optional explicit target columns.
        std::optional<std::vector<std::string>> columns;

        /// @brief Values to insert.
        std::vector<ExpressionPtr> values;
    };

    /// @brief Parsed `SELECT` ORDER BY term.
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
            FilePath,
            Subquery
        } kind = Kind::Table;

        /// @brief Table name or file path when @ref kind is `Table` or `FilePath`.
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
        RelationReference table_name;

        /// @brief Column/value assignments.
        std::vector<std::pair<std::string, ExpressionPtr>> assignments;

        /// @brief Optional filter expression.
        ExpressionPtr where;
    };

    /// @brief Supported ALTER TABLE actions.
    enum class AlterAction
    {
        AddColumn,
        DropColumn,
        RenameColumn,
        SetDefault,
        DropDefault,
        SetAutoIncrement,
        DropAutoIncrement,
        SetViewQuery
    };

    /// @brief Parsed `ALTER TABLE` or `ALTER VIEW` statement.
    struct AlterStatement
    {
        /// @brief Target schema object kind.
        SchemaObjectKind object_kind = SchemaObjectKind::Table;

        /// @brief Target table or view name.
        RelationReference table_name;

        /// @brief Requested alter action.
        AlterAction action = AlterAction::AddColumn;

        /// @brief Target column name for non-add actions.
        std::string column_name;

        /// @brief Replacement or added column definition.
        ColumnDefinition column;

        /// @brief New column name for rename actions.
        std::string new_name;

        /// @brief Replacement query for `ALTER VIEW`.
        SelectStatementPtr view_query;
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
            Update,
            Alter
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

        /// @brief `ALTER TABLE` payload.
        AlterStatement alter;
    };
}

