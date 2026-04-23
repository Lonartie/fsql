#include "SelectExecution.h"

#include "ExpressionEvaluation.h"
#include "SelectExecutionDetail.h"
#include "SqlError.h"
#include "SqlSerialization.h"

#include <algorithm>
#include <memory>
#include <set>
#include <utility>
#include <vector>

namespace fsql
{
    namespace
    {
        RowGenerator open_visible_rows(std::function<RowGenerator()> open_rows)
        {
            for (auto row : open_rows())
            {
                for (auto& value : row)
                {
                    value = visible_value_text(value);
                }
                co_yield row;
            }
        }

        RowGenerator open_materialized_rows(std::shared_ptr<const std::vector<Row>> rows)
        {
            for (const auto& row : *rows)
            {
                co_yield row;
            }
        }

        std::string select_projection_column_name(const SelectProjection& projection)
        {
            return projection.alias.value_or(serialize_expression(projection.expression));
        }

        std::vector<ProjectedSelectRow> materialize_shaped_source_rows(const SelectStatement& stmt,
                                                                       const ResolvedSelectTable& table,
                                                                       const IStorage& storage)
        {
            std::vector<ProjectedSelectRow> rows;
            for (const auto& row : table.rows())
            {
                if (stmt.where && !to_bool(evaluate_select_row_expression(stmt.where, table, row, storage)))
                {
                    continue;
                }

                ProjectedSelectRow projected_row;
                projected_row.values = row;
                projected_row.order_values.reserve(stmt.order_by.size());
                for (const auto& order_by : stmt.order_by)
                {
                    projected_row.order_values.push_back(evaluate_select_row_expression(order_by.expression, table, row, storage));
                }
                rows.push_back(std::move(projected_row));
            }
            detail::apply_select_modifiers(stmt, rows);
            return rows;
        }
    }

    namespace detail
    {
        RowGenerator open_joined_rows(std::shared_ptr<const std::vector<SelectSourcePlan>> sources, std::size_t index, Row prefix)
        {
            if (index >= sources->size())
            {
                co_yield prefix;
                co_return;
            }
            for (const auto& source_row : (*sources)[index].rows())
            {
                Row combined_row = prefix;
                combined_row.insert(combined_row.end(), source_row.begin(), source_row.end());
                for (auto row : open_joined_rows(sources, index + 1, std::move(combined_row)))
                {
                    co_yield row;
                }
            }
        }

        void apply_select_modifiers(const SelectStatement& stmt, std::vector<ProjectedSelectRow>& rows)
        {
            if (!stmt.order_by.empty())
            {
                std::stable_sort(rows.begin(), rows.end(), [&stmt](const ProjectedSelectRow& left, const ProjectedSelectRow& right)
                {
                    for (std::size_t i = 0; i < stmt.order_by.size(); ++i)
                    {
                        const int comparison = compare_values(left.order_values[i], right.order_values[i]);
                        if (comparison == 0)
                        {
                            continue;
                        }
                        return stmt.order_by[i].descending ? (comparison > 0) : (comparison < 0);
                    }
                    return false;
                });
            }

            if (stmt.distinct)
            {
                std::set<Row> seen_rows;
                auto unique_begin = std::remove_if(rows.begin(), rows.end(), [&](const ProjectedSelectRow& row)
                {
                    return !seen_rows.insert(row.values).second;
                });
                rows.erase(unique_begin, rows.end());
            }

            const std::size_t offset = stmt.offset.value_or(0);
            if (offset >= rows.size())
            {
                rows.clear();
                return;
            }
            if (offset > 0)
            {
                rows.erase(rows.begin(), rows.begin() + static_cast<std::ptrdiff_t>(offset));
            }
            if (stmt.limit.has_value() && rows.size() > *stmt.limit)
            {
                rows.erase(rows.begin() + static_cast<std::ptrdiff_t>(*stmt.limit), rows.end());
            }
        }

        ExecutionTable make_buffered_execution_table(std::vector<std::string> column_names, std::vector<ProjectedSelectRow> projected_rows)
        {
            auto rows = std::make_shared<std::vector<Row>>();
            rows->reserve(projected_rows.size());
            for (auto& projected_row : projected_rows)
            {
                rows->push_back(std::move(projected_row.values));
            }

            ExecutionTable result;
            result.column_names = std::move(column_names);
            result.rows = [rows]()
            {
                return open_materialized_rows(rows);
            };
            return result;
        }

        RowGenerator open_select_all_rows(ResolvedSelectTable table, SelectStatement stmt, const IStorage* storage)
        {
            const auto offset = stmt.offset.value_or(0);
            const auto limit = stmt.limit.value_or(static_cast<std::size_t>(-1));
            std::size_t skipped = 0;
            std::size_t emitted = 0;
            for (const auto& row : table.rows())
            {
                if (stmt.where && !to_bool(evaluate_select_row_expression(stmt.where, table, row, *storage)))
                {
                    continue;
                }
                if (skipped < offset)
                {
                    ++skipped;
                    continue;
                }
                if (emitted >= limit)
                {
                    co_return;
                }
                co_yield row;
                ++emitted;
            }
        }

        RowGenerator open_projected_rows(ResolvedSelectTable table, SelectStatement stmt, const IStorage* storage)
        {
            const auto offset = stmt.offset.value_or(0);
            const auto limit = stmt.limit.value_or(static_cast<std::size_t>(-1));
            std::size_t skipped = 0;
            std::size_t emitted = 0;
            for (const auto& row : table.rows())
            {
                if (stmt.where && !to_bool(evaluate_select_row_expression(stmt.where, table, row, *storage)))
                {
                    continue;
                }
                if (skipped < offset)
                {
                    ++skipped;
                    continue;
                }
                if (emitted >= limit)
                {
                    co_return;
                }

                Row projected_row;
                projected_row.reserve(stmt.projections.size());
                for (const auto& projection : stmt.projections)
                {
                    projected_row.push_back(evaluate_select_row_expression(projection.expression, table, row, *storage).text);
                }
                co_yield projected_row;
                ++emitted;
            }
        }
    }

    ExecutionTable make_visible_execution_table(ExecutionTable table)
    {
        const auto open_rows = table.rows;
        table.rows = [open_rows]()
        {
            return open_visible_rows(open_rows);
        };
        return table;
    }

    std::size_t count_execution_rows(const ExecutionTable& table, const ICoroExecutor& coro_executor)
    {
        return coro_executor.drive_rows(table.rows(), [](const Row& row)
        {
            static_cast<void>(row);
            return true;
        });
    }

    ExecutionTable run_select_statement(const SelectStatement& stmt, const IStorage& storage, const ForkJoinScheduler* scheduler)
    {
        const auto table = detail::materialize_select_table(stmt, storage, scheduler);
        if (stmt.having && stmt.group_by.empty())
        {
            fail("HAVING requires GROUP BY");
        }

        if (!stmt.group_by.empty())
        {
            if (stmt.select_all)
            {
                fail("SELECT * is not supported with GROUP BY");
            }

            const auto group_by_identifiers = detail::collect_group_by_column_indexes(stmt, table);
            ExecutionTable result;
            for (const auto& projection : stmt.projections)
            {
                detail::validate_grouped_expression(projection.expression, table, group_by_identifiers);
                result.column_names.push_back(select_projection_column_name(projection));
            }
            for (const auto& order_by : stmt.order_by)
            {
                detail::validate_grouped_expression(order_by.expression, table, group_by_identifiers);
            }
            if (stmt.having)
            {
                detail::validate_grouped_expression(stmt.having, table, group_by_identifiers);
            }

            const auto aggregate_definitions = detail::collect_aggregate_definitions(stmt);
            std::vector<ProjectedSelectRow> projected_rows;
            const auto groups = detail::build_select_groups(stmt, table, storage, aggregate_definitions);
            projected_rows.reserve(groups.size());
            for (const auto& group : groups)
            {
                if (stmt.having && !to_bool(evaluate_grouped_expression(stmt.having, table, group, storage)))
                {
                    continue;
                }

                ProjectedSelectRow projected_row;
                projected_row.values.reserve(stmt.projections.size());
                for (const auto& projection : stmt.projections)
                {
                    projected_row.values.push_back(evaluate_grouped_expression(projection.expression, table, group, storage).text);
                }
                projected_row.order_values.reserve(stmt.order_by.size());
                for (const auto& order_by : stmt.order_by)
                {
                    projected_row.order_values.push_back(evaluate_grouped_expression(order_by.expression, table, group, storage));
                }
                projected_rows.push_back(std::move(projected_row));
            }
            detail::apply_select_modifiers(stmt, projected_rows);
            return detail::make_buffered_execution_table(std::move(result.column_names), std::move(projected_rows));
        }

        for (const auto& order_by : stmt.order_by)
        {
            detail::validate_select_projection(order_by.expression, table);
        }

        if (stmt.select_all)
        {
            ExecutionTable result;
            for (std::size_t i = 0; i < table.columns.size(); ++i)
            {
                result.column_names.push_back(detail::select_star_column_name(table, i));
            }
            if (stmt.order_by.empty() && !stmt.distinct)
            {
                result.rows = [table, stmt, storage_ptr = &storage]()
                {
                    return detail::open_select_all_rows(table, stmt, storage_ptr);
                };
                return result;
            }

            std::vector<ProjectedSelectRow> projected_rows;
            for (const auto& row : table.rows())
            {
                if (stmt.where && !to_bool(evaluate_select_row_expression(stmt.where, table, row, storage)))
                {
                    continue;
                }
                ProjectedSelectRow projected_row;
                projected_row.values = row;
                projected_row.order_values.reserve(stmt.order_by.size());
                for (const auto& order_by : stmt.order_by)
                {
                    projected_row.order_values.push_back(evaluate_select_row_expression(order_by.expression, table, row, storage));
                }
                projected_rows.push_back(std::move(projected_row));
            }
            detail::apply_select_modifiers(stmt, projected_rows);
            return detail::make_buffered_execution_table(std::move(result.column_names), std::move(projected_rows));
        }

        ExecutionTable result;
        bool has_aggregate_projection = false;
        for (const auto& projection : stmt.projections)
        {
            detail::validate_select_projection(projection.expression, table);
            has_aggregate_projection = has_aggregate_projection || contains_aggregate_function(projection.expression);
            result.column_names.push_back(select_projection_column_name(projection));
        }

        std::vector<ProjectedSelectRow> projected_rows;
        if (has_aggregate_projection)
        {
            const auto aggregate_definitions = detail::collect_aggregate_definitions(stmt);
            auto aggregate_states = detail::make_aggregate_state_map(aggregate_definitions);
            const auto shaped_rows = materialize_shaped_source_rows(stmt, table, storage);
            for (const auto& projected_row : shaped_rows)
            {
                for (auto& [aggregate_name, state] : aggregate_states)
                {
                    static_cast<void>(aggregate_name);
                    detail::update_aggregate_function(state, table, projected_row.values, storage);
                }
            }

            ProjectedSelectRow aggregate_row;
            aggregate_row.values.reserve(stmt.projections.size());
            for (const auto& projection : stmt.projections)
            {
                if (!projection.expression
                    || projection.expression->kind != ExpressionKind::FunctionCall
                    || !detail::is_aggregate_function_name(projection.expression->text))
                {
                    fail("Aggregate queries only support aggregate functions in SELECT projections");
                }
                const auto state = aggregate_states.find(serialize_expression(projection.expression));
                if (state == aggregate_states.end())
                {
                    fail("Missing aggregate state for expression " + serialize_expression(projection.expression));
                }
                aggregate_row.values.push_back(detail::finalize_aggregate_function(state->second).text);
            }
            projected_rows.push_back(std::move(aggregate_row));
            return detail::make_buffered_execution_table(std::move(result.column_names), std::move(projected_rows));
        }

        if (stmt.order_by.empty() && !stmt.distinct)
        {
            result.rows = [table, stmt, storage_ptr = &storage]()
            {
                return detail::open_projected_rows(table, stmt, storage_ptr);
            };
            return result;
        }

        for (const auto& row : table.rows())
        {
            if (stmt.where && !to_bool(evaluate_select_row_expression(stmt.where, table, row, storage)))
            {
                continue;
            }
            ProjectedSelectRow projected_row;
            projected_row.values.reserve(stmt.projections.size());
            for (const auto& projection : stmt.projections)
            {
                projected_row.values.push_back(evaluate_select_row_expression(projection.expression, table, row, storage).text);
            }
            projected_row.order_values.reserve(stmt.order_by.size());
            for (const auto& order_by : stmt.order_by)
            {
                projected_row.order_values.push_back(evaluate_select_row_expression(order_by.expression, table, row, storage));
            }
            projected_rows.push_back(std::move(projected_row));
        }
        detail::apply_select_modifiers(stmt, projected_rows);
        return detail::make_buffered_execution_table(std::move(result.column_names), std::move(projected_rows));
    }
}


