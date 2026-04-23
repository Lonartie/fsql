#pragma once

#include "CsvStorage.h"
#include "Executor.h"
#include "MemoryStorage.h"
#include "Parser.h"
#include "SerialCoroExecutor.h"
#include "Tokenizer.h"

#include <filesystem>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>

namespace sql_test
{
    inline sql::Statement parse_statement(const std::string& query)
    {
        sql::Tokenizer tokenizer(query);
        sql::Parser parser(tokenizer.tokenize());
        return parser.parse_statement();
    }

    inline sql::ExpressionPtr parse_expression(const std::string& query)
    {
        sql::Tokenizer tokenizer(query);
        sql::Parser parser(tokenizer.tokenize());
        return parser.parse_expression();
    }

    inline std::filesystem::path fixture_path(const std::string& name)
    {
        return std::filesystem::path(__FILE__).parent_path() / "fixtures" / name;
    }

    struct TemporaryDirectory
    {
        std::filesystem::path path;

        TemporaryDirectory()
        {
            path = std::filesystem::temp_directory_path()
                / ("csv_sql_" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
            std::filesystem::create_directories(path);
        }

        ~TemporaryDirectory()
        {
            std::error_code error;
            std::filesystem::remove_all(path, error);
        }
    };

    struct ExecutorContext
    {
        struct ThrowingExecutor
        {
            sql::Executor inner;

            explicit ThrowingExecutor(std::shared_ptr<sql::IStorage> storage)
                : inner(std::move(storage))
            {
            }

            sql::ExecutionResult execute(const sql::Statement& statement)
            {
                const auto result = inner.execute(statement);
                if (!result.success)
                {
                    throw std::runtime_error(result.error);
                }
                return result;
            }
        };

        std::shared_ptr<sql::MemoryStorage> storage;
        ThrowingExecutor executor;

        ExecutorContext() : storage(std::make_shared<sql::MemoryStorage>()), executor(storage)
        {
        }
    };


    struct MaterializedExecutionTable
    {
        std::vector<std::string> column_names;
        std::vector<sql::Row> rows;
    };


    inline MaterializedExecutionTable require_table(const sql::ExecutionResult& result)
    {
        if (!result.success)
        {
            throw std::runtime_error("Expected successful result but got error: " + result.error);
        }
        if (!result.table.has_value())
        {
            throw std::runtime_error("Expected result table to be present");
        }

        MaterializedExecutionTable table;
        table.column_names = result.table->column_names;
        const sql::SerialCoroExecutor coro_executor;
        coro_executor.drive_rows(result.table->rows(), [&](const sql::Row& row)
        {
            table.rows.push_back(row);
            return true;
        });
        return table;
    }

}

