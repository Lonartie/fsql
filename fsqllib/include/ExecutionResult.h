#pragma once

#include "CoroTypes.h"

#include <functional>
#include <optional>
#include <string>
#include <vector>

namespace fsql
{
    /// @brief Tabular data returned by a `SELECT` statement.
    struct ExecutionTable
    {
        /// @brief Ordered result column names.
        std::vector<std::string> column_names;

        /// @brief Reopenable row stream for the result.
        std::function<RowGenerator()> rows;
    };

    /// @brief Supported structured executor result kinds.
    enum class ExecutionResultKind
    {
        None,
        Create,
        Alter,
        Drop,
        Delete,
        Insert,
        Select,
        Update
    };

    /// @brief Structured outcome of executing a statement.
    struct ExecutionResult
    {
        /// @brief Indicates whether execution completed successfully.
        bool success = false;

        /// @brief Statement/result category.
        ExecutionResultKind kind = ExecutionResultKind::None;

        /// @brief Number of affected rows or selected rows when applicable.
        std::size_t affected_rows = 0;

        /// @brief Human-readable success summary.
        std::string message;

        /// @brief Optional tabular result for `SELECT` statements.
        std::optional<ExecutionTable> table;

        /// @brief Human-readable failure description when @ref success is `false`.
        std::string error;
    };
}

