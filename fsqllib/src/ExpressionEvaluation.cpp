#include "ExpressionEvaluation.h"

#include "SelectExecution.h"
#include "SerialCoroExecutor.h"
#include "SqlError.h"
#include "SqlSerialization.h"
#include "StringUtils.h"

namespace fsql
{
    namespace
    {
        const ICoroExecutor& default_coro_executor()
        {
            static const SerialCoroExecutor executor;
            return executor;
        }

        ValueGenerator open_execution_values(const ExecutionTable& table)
        {
            for (const auto& row : table.rows())
            {
                if (row.empty())
                {
                    fail("SELECT expression row is missing projected value");
                }
                co_yield row[0];
            }
        }

        EvaluatedValue evaluate_select_expression(const SelectStatement& stmt, const IStorage& storage)
        {
            const auto result = run_select_statement(stmt, storage);
            if (result.column_names.size() != 1)
            {
                fail("SELECT expression must return exactly one column");
            }

            std::optional<std::string> first_value;
            bool saw_multiple_rows = false;
            default_coro_executor().drive_values(open_execution_values(result), [&](const std::string& value)
            {
                if (!first_value.has_value())
                {
                    first_value = value;
                    return true;
                }
                saw_multiple_rows = true;
                return false;
            });

            if (!first_value.has_value())
            {
                fail("SELECT expression returned no rows");
            }
            if (saw_multiple_rows)
            {
                fail("SELECT expression returned more than one row");
            }
            return make_text(*first_value);
        }

        bool evaluate_exists_expression(const SelectStatement& stmt, const IStorage& storage)
        {
            const auto result = run_select_statement(stmt, storage);
            return default_coro_executor().drive_rows(result.rows(), [](const Row& row)
            {
                static_cast<void>(row);
                return false;
            }) > 0;
        }

        EvaluatedValue evaluate_in_subquery(const EvaluatedValue& left,
                                            const ExpressionPtr& right,
                                            const IStorage& storage,
                                            bool negated = false)
        {
            if (!right)
            {
                fail("IN requires a SELECT subquery or inline list");
            }
            if (right->kind != ExpressionKind::Select || !right->select)
            {
                fail("IN requires a SELECT subquery or inline list");
            }

            const auto result = run_select_statement(*right->select, storage);
            if (result.column_names.size() != 1)
            {
                fail("IN subquery must return exactly one column");
            }
            if (left.is_null)
            {
                return make_numeric(0.0);
            }

            bool found_match = false;
            default_coro_executor().drive_values(open_execution_values(result), [&](const std::string& value)
            {
                const auto candidate = make_text(value);
                if (candidate.is_null)
                {
                    return true;
                }
                if (compare_values(left, candidate) == 0)
                {
                    found_match = true;
                    return false;
                }
                return true;
            });
            return make_numeric(found_match != negated ? 1.0 : 0.0);
        }

        EvaluatedValue evaluate_in_values(const EvaluatedValue& left,
                                          const std::vector<EvaluatedValue>& values,
                                          bool negated = false)
        {
            if (left.is_null)
            {
                return make_numeric(0.0);
            }

            for (const auto& candidate : values)
            {
                if (!candidate.is_null && compare_values(left, candidate) == 0)
                {
                    return make_numeric(negated ? 0.0 : 1.0);
                }
            }
            return make_numeric(negated ? 1.0 : 0.0);
        }

        EvaluatedValue evaluate_quantified_subquery(const EvaluatedValue& left,
                                                    BinaryOperator op,
                                                    SubqueryQuantifier quantifier,
                                                    const ExpressionPtr& right,
                                                    const IStorage& storage)
        {
            if (quantifier == SubqueryQuantifier::None)
            {
                fail("Missing ANY/ALL quantifier");
            }
            if (!right || right->kind != ExpressionKind::Select || !right->select)
            {
                fail("ANY and ALL currently require a SELECT subquery");
            }

            const auto result = run_select_statement(*right->select, storage);
            if (result.column_names.size() != 1)
            {
                fail("ANY/ALL subquery must return exactly one column");
            }
            if (left.is_null)
            {
                return make_numeric(0.0);
            }

            if (quantifier == SubqueryQuantifier::Any)
            {
                bool matched = false;
                default_coro_executor().drive_values(open_execution_values(result), [&](const std::string& value)
                {
                    const auto candidate = make_text(value);
                    if (candidate.is_null)
                    {
                        return true;
                    }
                    if (evaluate_quantified_comparison(compare_values(left, candidate), op))
                    {
                        matched = true;
                        return false;
                    }
                    return true;
                });
                return make_numeric(matched ? 1.0 : 0.0);
            }

            bool all_match = true;
            default_coro_executor().drive_values(open_execution_values(result), [&](const std::string& value)
            {
                const auto candidate = make_text(value);
                if (candidate.is_null)
                {
                    return true;
                }
                if (!evaluate_quantified_comparison(compare_values(left, candidate), op))
                {
                    all_match = false;
                    return false;
                }
                return true;
            });
            return make_numeric(all_match ? 1.0 : 0.0);
        }

        EvaluatedValue evaluate_select_identifier(const std::string& identifier, const ResolvedSelectTable& table, const Row& row)
        {
            const auto dot = identifier.find('.');
            std::optional<std::string> source_name;
            std::string column_name = identifier;
            if (dot != std::string::npos)
            {
                source_name = identifier.substr(0, dot);
                column_name = identifier.substr(dot + 1);
            }

            std::optional<std::size_t> resolved_index;
            for (std::size_t i = 0; i < table.columns.size(); ++i)
            {
                const auto& column = table.columns[i];
                if (column.column_name != column_name)
                {
                    continue;
                }
                if (source_name.has_value() && column.source_name != *source_name)
                {
                    continue;
                }
                if (resolved_index.has_value())
                {
                    fail((source_name.has_value() ? "Ambiguous qualified column '" : "Ambiguous column '") + identifier + "'");
                }
                resolved_index = i;
            }

            if (!resolved_index.has_value())
            {
                if (source_name.has_value())
                {
                    fail("Unknown column '" + identifier + "' in SELECT sources");
                }
                return make_text(identifier);
            }
            return make_text(row[*resolved_index]);
        }

        EvaluatedValue evaluate_table_identifier(const std::string& identifier, const Table& table, const Row& row, const IStorage& storage)
        {
            const auto dot = identifier.find('.');
            if (dot == std::string::npos)
            {
                try
                {
                    return make_text(row[storage.column_index(table, identifier)]);
                }
                catch (const std::runtime_error&)
                {
                    return make_text(identifier);
                }
            }

            const auto source_name = identifier.substr(0, dot);
            const auto column_name = identifier.substr(dot + 1);
            if (!iequals(source_name, table.name))
            {
                fail("Unknown column '" + identifier + "' in table '" + table.name + "'");
            }

            try
            {
                return make_text(row[storage.column_index(table, column_name)]);
            }
            catch (const std::runtime_error&)
            {
                fail("Unknown column '" + identifier + "' in table '" + table.name + "'");
            }
        }
    }

    std::size_t resolve_select_column_index(const ResolvedSelectTable& table, const std::string& identifier)
    {
        const auto dot = identifier.find('.');
        std::optional<std::string> source_name;
        std::string column_name = identifier;
        if (dot != std::string::npos)
        {
            source_name = identifier.substr(0, dot);
            column_name = identifier.substr(dot + 1);
        }

        std::optional<std::size_t> resolved_index;
        for (std::size_t i = 0; i < table.columns.size(); ++i)
        {
            const auto& column = table.columns[i];
            if (column.column_name != column_name)
            {
                continue;
            }
            if (source_name.has_value() && column.source_name != *source_name)
            {
                continue;
            }
            if (resolved_index.has_value())
            {
                fail((source_name.has_value() ? "Ambiguous qualified column '" : "Ambiguous column '") + identifier + "'");
            }
            resolved_index = i;
        }
        if (!resolved_index.has_value())
        {
            fail("Unknown column '" + identifier + "' in SELECT sources");
        }
        return *resolved_index;
    }

    EvaluatedValue evaluate_select_row_expression(const ExpressionPtr& expression, const ResolvedSelectTable& table, const Row& row, const IStorage& storage)
    {
        if (!expression)
        {
            return make_text("");
        }
        switch (expression->kind)
        {
        case ExpressionKind::Literal:
            return make_text(expression->text);
        case ExpressionKind::Null:
            return make_null();
        case ExpressionKind::Identifier:
            return evaluate_select_identifier(expression->text, table, row);
        case ExpressionKind::List:
            fail("Inline expression lists can only be used with IN");
        case ExpressionKind::Select:
            if (!expression->select) fail("Missing SELECT expression payload");
            return evaluate_select_expression(*expression->select, storage);
        case ExpressionKind::Exists:
            if (!expression->select) fail("Missing EXISTS expression payload");
            return make_numeric(evaluate_exists_expression(*expression->select, storage) ? 1.0 : 0.0);
        case ExpressionKind::FunctionCall:
            return evaluate_function(*expression);
        case ExpressionKind::Unary:
            return apply_unary_operator(expression->unary_operator, evaluate_select_row_expression(expression->left, table, row, storage));
        case ExpressionKind::Binary:
            if (expression->subquery_quantifier != SubqueryQuantifier::None)
            {
                return evaluate_quantified_subquery(evaluate_select_row_expression(expression->left, table, row, storage), expression->binary_operator, expression->subquery_quantifier, expression->right, storage);
            }
            if (expression->binary_operator == BinaryOperator::In || expression->binary_operator == BinaryOperator::NotIn)
            {
                const bool negated = expression->binary_operator == BinaryOperator::NotIn;
                if (expression->right && expression->right->kind == ExpressionKind::List)
                {
                    std::vector<EvaluatedValue> values;
                    values.reserve(expression->right->arguments.size());
                    for (const auto& item : expression->right->arguments)
                    {
                        values.push_back(evaluate_select_row_expression(item, table, row, storage));
                    }
                    return evaluate_in_values(evaluate_select_row_expression(expression->left, table, row, storage), values, negated);
                }
                return evaluate_in_subquery(evaluate_select_row_expression(expression->left, table, row, storage), expression->right, storage, negated);
            }
            return apply_binary_operator(expression->binary_operator,
                                         evaluate_select_row_expression(expression->left, table, row, storage),
                                         evaluate_select_row_expression(expression->right, table, row, storage));
        }
        fail("Unsupported select expression");
    }

    EvaluatedValue evaluate_grouped_expression(const ExpressionPtr& expression, const ResolvedSelectTable& table, const SelectGroupState& group, const IStorage& storage)
    {
        if (!expression)
        {
            return make_text("");
        }
        switch (expression->kind)
        {
        case ExpressionKind::Literal:
            return make_text(expression->text);
        case ExpressionKind::Null:
            return make_null();
        case ExpressionKind::Identifier:
            return evaluate_select_identifier(expression->text, table, group.representative_row);
        case ExpressionKind::List:
            fail("Inline expression lists can only be used with IN");
        case ExpressionKind::Select:
            if (!expression->select) fail("Missing SELECT expression payload");
            return evaluate_select_expression(*expression->select, storage);
        case ExpressionKind::Exists:
            if (!expression->select) fail("Missing EXISTS expression payload");
            return make_numeric(evaluate_exists_expression(*expression->select, storage) ? 1.0 : 0.0);
        case ExpressionKind::FunctionCall:
        {
            const auto aggregate = group.aggregates.find(serialize_expression(expression));
            if (aggregate != group.aggregates.end())
            {
                const auto& state = aggregate->second;
                if (!state.expression)
                {
                    fail("Missing aggregate state for expression " + serialize_expression(expression));
                }
                if (state.expression->text == "COUNT" || state.expression->text == "count") return make_numeric(static_cast<double>(state.count));
                if (state.expression->text == "SUM" || state.expression->text == "sum") return make_numeric(state.total);
                if (state.expression->text == "AVG" || state.expression->text == "avg")
                {
                    return make_numeric(state.numeric_count == 0 ? 0.0 : (state.total / static_cast<double>(state.numeric_count)));
                }
                return state.has_best ? make_text(state.best.text) : make_text("");
            }
            return evaluate_function(*expression);
        }
        case ExpressionKind::Unary:
            return apply_unary_operator(expression->unary_operator, evaluate_grouped_expression(expression->left, table, group, storage));
        case ExpressionKind::Binary:
            if (expression->subquery_quantifier != SubqueryQuantifier::None)
            {
                return evaluate_quantified_subquery(evaluate_grouped_expression(expression->left, table, group, storage), expression->binary_operator, expression->subquery_quantifier, expression->right, storage);
            }
            if (expression->binary_operator == BinaryOperator::In || expression->binary_operator == BinaryOperator::NotIn)
            {
                const bool negated = expression->binary_operator == BinaryOperator::NotIn;
                if (expression->right && expression->right->kind == ExpressionKind::List)
                {
                    std::vector<EvaluatedValue> values;
                    values.reserve(expression->right->arguments.size());
                    for (const auto& item : expression->right->arguments)
                    {
                        values.push_back(evaluate_grouped_expression(item, table, group, storage));
                    }
                    return evaluate_in_values(evaluate_grouped_expression(expression->left, table, group, storage), values, negated);
                }
                return evaluate_in_subquery(evaluate_grouped_expression(expression->left, table, group, storage), expression->right, storage, negated);
            }
            return apply_binary_operator(expression->binary_operator,
                                         evaluate_grouped_expression(expression->left, table, group, storage),
                                         evaluate_grouped_expression(expression->right, table, group, storage));
        }
        fail("Unsupported grouped expression");
    }

    EvaluatedValue evaluate_expression(const ExpressionPtr& expression, const Table& table, const Row& row, const IStorage& storage)
    {
        if (!expression)
        {
            return make_text("");
        }
        switch (expression->kind)
        {
        case ExpressionKind::Literal:
            return make_text(expression->text);
        case ExpressionKind::Null:
            return make_null();
        case ExpressionKind::Identifier:
            return evaluate_table_identifier(expression->text, table, row, storage);
        case ExpressionKind::List:
            fail("Inline expression lists can only be used with IN");
        case ExpressionKind::Select:
            if (!expression->select) fail("Missing SELECT expression payload");
            return evaluate_select_expression(*expression->select, storage);
        case ExpressionKind::Exists:
            if (!expression->select) fail("Missing EXISTS expression payload");
            return make_numeric(evaluate_exists_expression(*expression->select, storage) ? 1.0 : 0.0);
        case ExpressionKind::FunctionCall:
            return evaluate_function(*expression);
        case ExpressionKind::Unary:
            return apply_unary_operator(expression->unary_operator, evaluate_expression(expression->left, table, row, storage));
        case ExpressionKind::Binary:
            if (expression->subquery_quantifier != SubqueryQuantifier::None)
            {
                return evaluate_quantified_subquery(evaluate_expression(expression->left, table, row, storage), expression->binary_operator, expression->subquery_quantifier, expression->right, storage);
            }
            if (expression->binary_operator == BinaryOperator::In || expression->binary_operator == BinaryOperator::NotIn)
            {
                const bool negated = expression->binary_operator == BinaryOperator::NotIn;
                if (expression->right && expression->right->kind == ExpressionKind::List)
                {
                    std::vector<EvaluatedValue> values;
                    values.reserve(expression->right->arguments.size());
                    for (const auto& item : expression->right->arguments)
                    {
                        values.push_back(evaluate_expression(item, table, row, storage));
                    }
                    return evaluate_in_values(evaluate_expression(expression->left, table, row, storage), values, negated);
                }
                return evaluate_in_subquery(evaluate_expression(expression->left, table, row, storage), expression->right, storage, negated);
            }
            return apply_binary_operator(expression->binary_operator,
                                         evaluate_expression(expression->left, table, row, storage),
                                         evaluate_expression(expression->right, table, row, storage));
        }
        fail("Unsupported expression");
    }

    std::string evaluate_value(const ExpressionPtr& expression, const Table& table, const Row& row, const IStorage& storage)
    {
        return evaluate_expression(expression, table, row, storage).text;
    }

    std::string evaluate_value(const ExpressionPtr& expression, const IStorage& storage)
    {
        const Table empty_table{};
        const Row empty_row{};
        return evaluate_expression(expression, empty_table, empty_row, storage).text;
    }
}

