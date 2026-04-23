#pragma once

#include "CsvStorage.h"
#include "CoroTypes.h"
#include "ICoroExecutor.h"
#include "IStorage.h"
#include "SqlTypes.h"

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace sql
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

    /// @brief Executes parsed SQL statements against CSV storage.
    class Executor
    {
    public:
        /// @brief Initializes the executor.
        /// @param storage Storage backend.
        /// @param coro_executor Coroutine executor used to drive streamed result consumption.
        explicit Executor(std::shared_ptr<IStorage> storage, std::shared_ptr<ICoroExecutor> coro_executor = nullptr);

        /// @brief Executes a parsed statement.
        /// @param statement Statement to execute.
        /// @return Structured execution result.
        ExecutionResult execute(const Statement& statement);

    private:
        /// @brief Executes `CREATE TABLE` or `CREATE VIEW`.
        ExecutionResult execute_create(const CreateStatement& stmt);

        /// @brief Executes `ALTER TABLE` or `ALTER VIEW`.
        ExecutionResult execute_alter(const AlterStatement& stmt);

        /// @brief Executes `DROP TABLE` or `DROP VIEW`.
        ExecutionResult execute_drop(const DropStatement& stmt);

        /// @brief Executes `DELETE FROM`.
        ExecutionResult execute_delete(const DeleteStatement& stmt);

        /// @brief Executes `INSERT INTO`.
        ExecutionResult execute_insert(const InsertStatement& stmt);

        /// @brief Executes `SELECT`.
        ExecutionResult execute_select(const SelectStatement& stmt);

        /// @brief Executes `UPDATE`.
        ExecutionResult execute_update(const UpdateStatement& stmt);

        /// @brief Storage backend.
        std::shared_ptr<IStorage> storage_;

        /// @brief Coroutine executor used to drive streamed top-level result consumption.
        std::shared_ptr<ICoroExecutor> coro_executor_;
    };
}
