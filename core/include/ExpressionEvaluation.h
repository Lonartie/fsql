#pragma once

#include "ExecutionTypes.h"
#include "IStorage.h"

#include <string>

namespace sql
{
    inline constexpr std::string_view null_storage_marker = "__CSV_SQL_NULL__";

    bool is_stored_null(const std::string& value);
    std::string visible_value_text(const std::string& value);
    bool is_integer_like(double value);
    std::string format_number(double value);
    bool try_parse_number(const std::string& text, double& value);
    bool to_bool(const EvaluatedValue& value);
    long long to_integer(const EvaluatedValue& value);
    EvaluatedValue make_numeric(double value);
    EvaluatedValue make_null();
    EvaluatedValue make_text(std::string value);
    EvaluatedValue apply_unary_operator(UnaryOperator op, const EvaluatedValue& operand);
    EvaluatedValue apply_binary_operator(BinaryOperator op, const EvaluatedValue& left, const EvaluatedValue& right);
    bool evaluate_quantified_comparison(int comparison, BinaryOperator op);
    bool contains_aggregate_function(const ExpressionPtr& expression);
    int compare_values(const EvaluatedValue& left, const EvaluatedValue& right);
    double require_numeric_argument(const EvaluatedValue& value, const std::string& function_name);
    EvaluatedValue evaluate_function(const Expression& expression);
    std::size_t resolve_select_column_index(const ResolvedSelectTable& table, const std::string& identifier);
    EvaluatedValue evaluate_select_row_expression(const ExpressionPtr& expression, const ResolvedSelectTable& table, const Row& row, const IStorage& storage);
    EvaluatedValue evaluate_grouped_expression(const ExpressionPtr& expression, const ResolvedSelectTable& table, const SelectGroupState& group, const IStorage& storage);
    EvaluatedValue evaluate_expression(const ExpressionPtr& expression, const Table& table, const Row& row, const IStorage& storage);
    std::string evaluate_value(const ExpressionPtr& expression, const Table& table, const Row& row, const IStorage& storage);
    std::string evaluate_value(const ExpressionPtr& expression, const IStorage& storage);
}

