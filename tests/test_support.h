#pragma once

#include "CsvStorage.h"
#include "Executor.h"
#include "MemoryStorage.h"
#include "Parser.h"
#include "Tokenizer.h"

#include <filesystem>
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


    inline const sql::ExecutionTable& require_table(const sql::ExecutionResult& result)
    {
        if (!result.success)
        {
            throw std::runtime_error("Expected successful result but got error: " + result.error);
        }
        if (!result.table.has_value())
        {
            throw std::runtime_error("Expected result table to be present");
        }
        return *result.table;
    }

}

