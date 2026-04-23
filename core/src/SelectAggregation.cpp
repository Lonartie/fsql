#include "SelectExecutionDetail.h"

#include "ExpressionEvaluation.h"
#include "SqlError.h"
#include "SqlSerialization.h"
#include "StringUtils.h"

#include <map>

namespace sql::detail
{
    bool is_aggregate_function_name(const std::string& name)
    {
        return iequals(name, "COUNT") || iequals(name, "SUM") || iequals(name, "AVG") || iequals(name, "MIN") || iequals(name, "MAX");
    }

    void update_aggregate_function(AggregateFunctionState& state, const ResolvedSelectTable& table, const Row& row, const IStorage& storage)
    {
        if (!state.expression || state.expression->kind != ExpressionKind::FunctionCall || !is_aggregate_function_name(state.expression->text))
        {
            fail("Invalid aggregate projection");
        }

        if (iequals(state.expression->text, "COUNT"))
        {
            if (state.expression->function_uses_star)
            {
                if (!state.expression->arguments.empty())
                {
                    fail("COUNT(*) does not accept additional arguments");
                }
                ++state.count;
                return;
            }
            if (state.expression->arguments.size() != 1)
            {
                fail("COUNT requires exactly one argument or *");
            }
            const auto value = evaluate_select_row_expression(state.expression->arguments[0], table, row, storage);
            if (!value.is_null && !value.text.empty())
            {
                ++state.count;
            }
            return;
        }

        if (state.expression->function_uses_star)
        {
            fail("Only COUNT supports '*' as an argument");
        }
        if (state.expression->arguments.size() != 1)
        {
            fail("Aggregate function " + state.expression->text + " requires exactly one argument");
        }

        const auto value = evaluate_select_row_expression(state.expression->arguments[0], table, row, storage);
        if (value.is_null || value.text.empty())
        {
            return;
        }
        if (iequals(state.expression->text, "SUM") || iequals(state.expression->text, "AVG"))
        {
            state.total += require_numeric_argument(value, state.expression->text);
            ++state.numeric_count;
            return;
        }
        if (!state.has_best)
        {
            state.best = value;
            state.has_best = true;
            return;
        }

        const bool prefer_numeric = value.numeric && state.best.numeric;
        const bool should_replace = iequals(state.expression->text, "MIN")
            ? (prefer_numeric ? value.number < state.best.number : value.text < state.best.text)
            : (prefer_numeric ? value.number > state.best.number : value.text > state.best.text);
        if (should_replace)
        {
            state.best = value;
        }
    }

    EvaluatedValue finalize_aggregate_function(const AggregateFunctionState& state)
    {
        if (!state.expression)
        {
            fail("Missing aggregate state");
        }
        if (iequals(state.expression->text, "COUNT"))
        {
            return make_numeric(static_cast<double>(state.count));
        }
        if (iequals(state.expression->text, "SUM"))
        {
            return make_numeric(state.total);
        }
        if (iequals(state.expression->text, "AVG"))
        {
            return make_numeric(state.numeric_count == 0 ? 0.0 : (state.total / static_cast<double>(state.numeric_count)));
        }
        return state.has_best ? make_text(state.best.text) : make_text("");
    }

    namespace
    {
        void collect_aggregate_definitions_from_expression(const ExpressionPtr& expression, std::map<std::string, ExpressionPtr>& definitions)
        {
            if (!expression)
            {
                return;
            }
            switch (expression->kind)
            {
            case ExpressionKind::FunctionCall:
                if (is_aggregate_function_name(expression->text))
                {
                    definitions.emplace(serialize_expression(expression), expression);
                    return;
                }
                for (const auto& argument : expression->arguments)
                {
                    collect_aggregate_definitions_from_expression(argument, definitions);
                }
                return;
            case ExpressionKind::List:
                for (const auto& argument : expression->arguments)
                {
                    collect_aggregate_definitions_from_expression(argument, definitions);
                }
                return;
            case ExpressionKind::Unary:
                collect_aggregate_definitions_from_expression(expression->left, definitions);
                return;
            case ExpressionKind::Binary:
                collect_aggregate_definitions_from_expression(expression->left, definitions);
                collect_aggregate_definitions_from_expression(expression->right, definitions);
                return;
            case ExpressionKind::Literal:
            case ExpressionKind::Null:
            case ExpressionKind::Identifier:
            case ExpressionKind::Select:
            case ExpressionKind::Exists:
                return;
            }
        }
    }

    std::map<std::string, ExpressionPtr> collect_aggregate_definitions(const SelectStatement& stmt)
    {
        std::map<std::string, ExpressionPtr> definitions;
        for (const auto& projection : stmt.projections)
        {
            collect_aggregate_definitions_from_expression(projection, definitions);
        }
        for (const auto& order_by : stmt.order_by)
        {
            collect_aggregate_definitions_from_expression(order_by.expression, definitions);
        }
        collect_aggregate_definitions_from_expression(stmt.having, definitions);
        return definitions;
    }

    std::map<std::string, AggregateFunctionState> make_aggregate_state_map(const std::map<std::string, ExpressionPtr>& definitions)
    {
        std::map<std::string, AggregateFunctionState> states;
        for (const auto& [key, expression] : definitions)
        {
            states.emplace(key, AggregateFunctionState{expression});
        }
        return states;
    }

    void validate_select_projection(const ExpressionPtr& expression, const ResolvedSelectTable& table)
    {
        if (!expression)
        {
            return;
        }
        switch (expression->kind)
        {
        case ExpressionKind::Identifier:
            static_cast<void>(resolve_select_column_index(table, expression->text));
            return;
        case ExpressionKind::Exists:
            return;
        case ExpressionKind::List:
            for (const auto& argument : expression->arguments)
            {
                validate_select_projection(argument, table);
            }
            return;
        case ExpressionKind::FunctionCall:
            for (const auto& argument : expression->arguments)
            {
                validate_select_projection(argument, table);
            }
            return;
        case ExpressionKind::Unary:
            validate_select_projection(expression->left, table);
            return;
        case ExpressionKind::Binary:
            validate_select_projection(expression->left, table);
            validate_select_projection(expression->right, table);
            return;
        case ExpressionKind::Select:
        case ExpressionKind::Literal:
        case ExpressionKind::Null:
            return;
        }
    }

    std::set<std::size_t> collect_group_by_column_indexes(const SelectStatement& stmt, const ResolvedSelectTable& table)
    {
        std::set<std::size_t> identifiers;
        for (const auto& expression : stmt.group_by)
        {
            if (!expression || expression->kind != ExpressionKind::Identifier)
            {
                fail("GROUP BY currently only supports column identifiers");
            }
            identifiers.insert(resolve_select_column_index(table, expression->text));
        }
        return identifiers;
    }

    void validate_grouped_expression(const ExpressionPtr& expression,
                                     const ResolvedSelectTable& table,
                                     const std::set<std::size_t>& group_by_identifiers,
                                     bool inside_aggregate)
    {
        if (!expression)
        {
            return;
        }
        switch (expression->kind)
        {
        case ExpressionKind::Literal:
        case ExpressionKind::Null:
        case ExpressionKind::Exists:
        case ExpressionKind::Select:
            return;
        case ExpressionKind::List:
            for (const auto& argument : expression->arguments)
            {
                validate_grouped_expression(argument, table, group_by_identifiers, inside_aggregate);
            }
            return;
        case ExpressionKind::Identifier:
        {
            const auto index = resolve_select_column_index(table, expression->text);
            if (!inside_aggregate && !group_by_identifiers.contains(index))
            {
                fail("Grouped query references non-grouped column '" + expression->text + "'");
            }
            return;
        }
        case ExpressionKind::FunctionCall:
            if (is_aggregate_function_name(expression->text))
            {
                for (const auto& argument : expression->arguments)
                {
                    validate_grouped_expression(argument, table, group_by_identifiers, true);
                }
                return;
            }
            for (const auto& argument : expression->arguments)
            {
                validate_grouped_expression(argument, table, group_by_identifiers, inside_aggregate);
            }
            return;
        case ExpressionKind::Unary:
            validate_grouped_expression(expression->left, table, group_by_identifiers, inside_aggregate);
            return;
        case ExpressionKind::Binary:
            validate_grouped_expression(expression->left, table, group_by_identifiers, inside_aggregate);
            validate_grouped_expression(expression->right, table, group_by_identifiers, inside_aggregate);
            return;
        }
    }

    std::vector<SelectGroupState> build_select_groups(const SelectStatement& stmt,
                                                      const ResolvedSelectTable& table,
                                                      const IStorage& storage,
                                                      const std::map<std::string, ExpressionPtr>& aggregate_definitions)
    {
        std::vector<SelectGroupState> groups;
        std::map<Row, std::size_t> key_to_group_index;
        for (const auto& row : table.rows())
        {
            if (stmt.where && !to_bool(evaluate_select_row_expression(stmt.where, table, row, storage)))
            {
                continue;
            }

            Row key;
            key.reserve(stmt.group_by.size());
            for (const auto& expression : stmt.group_by)
            {
                key.push_back(evaluate_select_row_expression(expression, table, row, storage).text);
            }

            const auto [iterator, inserted] = key_to_group_index.emplace(key, groups.size());
            if (inserted)
            {
                SelectGroupState group;
                group.representative_row = row;
                group.aggregates = make_aggregate_state_map(aggregate_definitions);
                groups.push_back(std::move(group));
            }

            for (auto& [aggregate_name, state] : groups[iterator->second].aggregates)
            {
                static_cast<void>(aggregate_name);
                update_aggregate_function(state, table, row, storage);
            }
        }
        return groups;
    }
}


