#pragma once

#include "CoroTypes.h"
#include "SqlTypes.h"

#include <functional>
#include <map>
#include <string>
#include <vector>

namespace sql
{
    /// @brief Evaluated scalar value used during execution.
    struct EvaluatedValue
    {
        std::string text;
        bool numeric = false;
        double number = 0.0;
        bool is_null = false;
    };

    struct ProjectedSelectRow
    {
        Row values;
        std::vector<EvaluatedValue> order_values;
    };

    struct ResolvedSelectColumn
    {
        std::string source_name;
        std::string column_name;
    };

    struct ResolvedSelectTable
    {
        std::vector<ResolvedSelectColumn> columns;
        std::function<RowGenerator()> rows;
        std::size_t source_count = 0;
    };

    struct SelectSourcePlan
    {
        std::string source_name;
        std::vector<std::string> column_names;
        std::function<RowGenerator()> rows;
    };

    struct AggregateFunctionState
    {
        ExpressionPtr expression;
        std::size_t count = 0;
        double total = 0.0;
        std::size_t numeric_count = 0;
        EvaluatedValue best;
        bool has_best = false;
    };

    struct SelectGroupState
    {
        Row representative_row;
        std::map<std::string, AggregateFunctionState> aggregates;
    };
}

