#pragma once

#include "CsvStorage.h"
#include "Executor.h"
#include "MemoryStorage.h"
#include "Parser.h"
#include "Tokenizer.h"

#include <chrono>
#include <filesystem>
#include <memory>
#include <sstream>
#include <string>
#include <system_error>

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

    struct ExecutorContext
    {
        std::shared_ptr<sql::MemoryStorage> storage;
        std::ostringstream output;
        sql::Executor executor;

        ExecutorContext() : storage(std::make_shared<sql::MemoryStorage>()), executor(storage, output)
        {
        }

        void reset_output()
        {
            output.str("");
            output.clear();
        }
    };

    struct TempDirectoryGuard
    {
        std::filesystem::path path;

        explicit TempDirectoryGuard(const std::string& prefix)
            : path(std::filesystem::temp_directory_path() / (prefix + "_" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count())))
        {
            std::filesystem::remove_all(path);
            std::filesystem::create_directories(path);
        }

        ~TempDirectoryGuard()
        {
            std::error_code error;
            std::filesystem::remove_all(path, error);
        }
    };
}

