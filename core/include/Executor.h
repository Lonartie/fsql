#pragma once

#include "CsvStorage.h"
#include "IStorage.h"
#include "SqlTypes.h"

#include <iosfwd>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace sql
{
    /// @brief Executes parsed SQL statements against CSV storage.
    class Executor
    {
    public:
        /// @brief Initializes the executor.
        /// @param storage Storage backend.
        /// @param output Output stream for results.
        explicit Executor(std::shared_ptr<IStorage> storage, std::ostream& output);

        /// @brief Executes a parsed statement.
        /// @param statement Statement to execute.
        void execute(const Statement& statement);

    private:
        /// @brief Executes `CREATE TABLE`.
        void execute_create(const CreateStatement& stmt);

        /// @brief Executes `DROP TABLE`.
        void execute_drop(const DropStatement& stmt);

        /// @brief Executes `DELETE FROM`.
        void execute_delete(const DeleteStatement& stmt);

        /// @brief Executes `INSERT INTO`.
        void execute_insert(const InsertStatement& stmt);

        /// @brief Executes `SELECT`.
        void execute_select(const SelectStatement& stmt);

        /// @brief Executes `UPDATE`.
        void execute_update(const UpdateStatement& stmt);

        /// @brief Storage backend.
        std::shared_ptr<IStorage> storage_;

        /// @brief Output stream for command results.
        std::ostream& output_;
    };
}
