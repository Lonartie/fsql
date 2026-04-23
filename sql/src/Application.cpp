#include "Application.h"

#include "ConsoleOutputWriter.h"
#include "CsvStorage.h"
#include "Executor.h"
#include "ParallelCoroExecutor.h"
#include "Parser.h"
#include "QueryInput.h"
#include "Tokenizer.h"

#include <exception>
#include <iostream>
#include <memory>
#include <string>

namespace sql
{
    int Application::run(int argc, char** argv)
    {
        try
        {
            const std::string query = QueryInput::read(argc, argv, std::cin);
            if (query.empty())
            {
                std::cerr << "Usage: sql <SQL statement>\n";
                return 1;
            }

            Tokenizer tokenizer(query);
            Parser parser(tokenizer.tokenize());
            auto coro_executor = std::make_shared<ParallelCoroExecutor>();
            Executor executor(std::make_shared<CsvStorage>(), coro_executor);
            ConsoleOutputWriter writer(coro_executor);

            const auto result = executor.execute(parser.parse_statement());
            if (!result.success)
            {
                std::cerr << "Error: " << result.error << '\n';
                return 1;
            }

            writer.write(std::cout, result);
            return 0;
        }
        catch (const std::exception& ex)
        {
            std::cerr << "Error: " << ex.what() << '\n';
            return 1;
        }
    }
}
