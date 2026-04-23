#pragma once

#include "ExecutionResult.h"
#include "ExecutionTypes.h"
#include "ForkJoinScheduler.h"
#include "ICoroExecutor.h"
#include "IStorage.h"

namespace fsql
{
    /// @brief Wraps result rows so stored NULL markers become visible `NULL` text.
    /// @param table Raw execution table.
    /// @return Table exposing visible values.
    ExecutionTable make_visible_execution_table(ExecutionTable table);

    /// @brief Counts rows in an execution table using the configured coroutine executor.
    /// @param table Table to count.
    /// @param coro_executor Coroutine executor used to drive the row stream.
    /// @return Row count.
    std::size_t count_execution_rows(const ExecutionTable& table, const ICoroExecutor& coro_executor);

    /// @brief Executes a SELECT statement.
    /// @param stmt Statement to execute.
    /// @param storage Storage backend.
    /// @param scheduler Optional scheduler for independent planning work.
    /// @return Reopenable execution table.
    ExecutionTable run_select_statement(const SelectStatement& stmt, const IStorage& storage, const ForkJoinScheduler* scheduler = nullptr);

    /// @brief Validates that a persisted view can be executed successfully.
    /// @param view_name View to validate.
    /// @param storage Storage backend.
    /// @param coro_executor Coroutine executor used to fully consume the result.
    void validate_view_definition(const RelationReference& view_name, const IStorage& storage, const ICoroExecutor& coro_executor);
}

