#pragma once

#include "SqlTypes.h"

#include <string>

namespace sql
{
    /// @brief Quotes a string as a SQL string literal.
    /// @param value Raw string value.
    /// @return SQL string literal.
    std::string quote_string(const std::string& value);

    /// @brief Serializes an expression AST back into SQL-like text.
    /// @param expression Expression to serialize.
    /// @return Serialized expression text.
    std::string serialize_expression(const ExpressionPtr& expression);

    /// @brief Serializes a SELECT source.
    /// @param source Source to serialize.
    /// @return Serialized source text.
    std::string serialize_select_source(const SelectSource& source);

    /// @brief Serializes a SELECT statement AST back into SQL-like text.
    /// @param statement Statement to serialize.
    /// @return Serialized statement text.
    std::string serialize_select_statement(const SelectStatement& statement);
}

