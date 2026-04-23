#pragma once

#include "CsvStorage.h"
#include "Executor.h"
#include "FileStorage.h"
#include "MemoryStorage.h"
#include "Parser.h"
#include "SerialCoroExecutor.h"
#include "Tokenizer.h"

#include <filesystem>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>

namespace fsql_test
{
    inline fsql::Statement parse_statement(const std::string& query)
    {
        fsql::Tokenizer tokenizer(query);
        fsql::Parser parser(tokenizer.tokenize());
        return parser.parse_statement();
    }

    inline fsql::ExpressionPtr parse_expression(const std::string& query)
    {
        fsql::Tokenizer tokenizer(query);
        fsql::Parser parser(tokenizer.tokenize());
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
                / ("fsql_" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
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
            fsql::Executor inner;

            explicit ThrowingExecutor(std::shared_ptr<fsql::IStorage> storage)
                : inner(std::move(storage))
            {
            }

            fsql::ExecutionResult execute(const fsql::Statement& statement)
            {
                const auto result = inner.execute(statement);
                if (!result.success)
                {
                    throw std::runtime_error(result.error);
                }
                return result;
            }
        };

        std::shared_ptr<fsql::MemoryStorage> storage;
        ThrowingExecutor executor;

        ExecutorContext() : storage(std::make_shared<fsql::MemoryStorage>()), executor(storage)
        {
        }
    };


    struct MaterializedExecutionTable
    {
        std::vector<std::string> column_names;
        std::vector<fsql::Row> rows;
    };


    inline MaterializedExecutionTable require_table(const fsql::ExecutionResult& result)
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
        const fsql::SerialCoroExecutor coro_executor;
        coro_executor.drive_rows(result.table->rows(), [&](const fsql::Row& row)
        {
            table.rows.push_back(row);
            return true;
        });
        return table;
    }

}

