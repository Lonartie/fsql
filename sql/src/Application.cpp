#include "Application.h"

#include "CsvStorage.h"
#include "Executor.h"
#include "Parser.h"
#include "StringUtils.h"
#include "Tokenizer.h"

#include <exception>
#include <filesystem>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>

namespace sql
{
    namespace
    {
        bool is_wrapper_command(const std::string& command)
        {
            return iequals(command, "select") ||
                iequals(command, "insert") ||
                iequals(command, "update") ||
                iequals(command, "delete") ||
                iequals(command, "create") ||
                iequals(command, "drop");
        }
    }

    int Application::run(int argc, char** argv)
    {
        try
        {
            std::string query;
            if (argc > 1)
            {
                query = join_arguments(argc, argv);

                const std::string executable_name = std::filesystem::path(argv[0]).stem().string();
                if (is_wrapper_command(executable_name))
                {
                    query = executable_name + " " + query;
                }
            }
            else
            {
                std::ostringstream buffer;
                buffer << std::cin.rdbuf();
                query = trim(buffer.str());
            }

            if (query.empty())
            {
                std::cerr << "Usage: sql <SQL statement>\n";
                return 1;
            }

            Tokenizer tokenizer(query);
            Parser parser(tokenizer.tokenize());
            Executor executor(std::make_shared<CsvStorage>(), std::cout);
            executor.execute(parser.parse_statement());
            return 0;
        }
        catch (const std::exception& ex)
        {
            std::cerr << "Error: " << ex.what() << '\n';
            return 1;
        }
    }

    std::string Application::join_arguments(int argc, char** argv)
    {
        std::ostringstream builder;
        for (int i = 1; i < argc; ++i)
        {
            if (i > 1)
            {
                builder << ' ';
            }
            builder << argv[i];
        }
        return builder.str();
    }
}
