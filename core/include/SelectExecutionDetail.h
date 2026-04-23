#pragma once

#include "ExecutionResult.h"
#include "ExecutionTypes.h"
#include "ForkJoinScheduler.h"
#include "IStorage.h"

#include <map>
#include <set>
#include <string>
#include <vector>

namespace sql::detail
{
    bool is_aggregate_function_name(const std::string& name);
    std::string select_star_column_name(const ResolvedSelectTable& table, std::size_t index);

    ExecutionTable run_view_statement(const RelationReference& view_name, const IStorage& storage);
    ResolvedSelectTable materialize_select_table(const SelectStatement& stmt, const IStorage& storage, const ForkJoinScheduler* scheduler);

    void apply_select_modifiers(const SelectStatement& stmt, std::vector<ProjectedSelectRow>& rows);
    ExecutionTable make_buffered_execution_table(std::vector<std::string> column_names, std::vector<ProjectedSelectRow> projected_rows);
    RowGenerator open_joined_rows(std::shared_ptr<const std::vector<SelectSourcePlan>> sources, std::size_t index, Row prefix);
    RowGenerator open_select_all_rows(ResolvedSelectTable table, SelectStatement stmt, const IStorage* storage);
    RowGenerator open_projected_rows(ResolvedSelectTable table, SelectStatement stmt, const IStorage* storage);

    void validate_select_projection(const ExpressionPtr& expression, const ResolvedSelectTable& table);
    std::set<std::size_t> collect_group_by_column_indexes(const SelectStatement& stmt, const ResolvedSelectTable& table);
    void validate_grouped_expression(const ExpressionPtr& expression,
                                     const ResolvedSelectTable& table,
                                     const std::set<std::size_t>& group_by_identifiers,
                                     bool inside_aggregate = false);

    void update_aggregate_function(AggregateFunctionState& state, const ResolvedSelectTable& table, const Row& row, const IStorage& storage);
    EvaluatedValue finalize_aggregate_function(const AggregateFunctionState& state);
    std::map<std::string, ExpressionPtr> collect_aggregate_definitions(const SelectStatement& stmt);
    std::map<std::string, AggregateFunctionState> make_aggregate_state_map(const std::map<std::string, ExpressionPtr>& definitions);
    std::vector<SelectGroupState> build_select_groups(const SelectStatement& stmt,
                                                      const ResolvedSelectTable& table,
                                                      const IStorage& storage,
                                                      const std::map<std::string, ExpressionPtr>& aggregate_definitions);
}


