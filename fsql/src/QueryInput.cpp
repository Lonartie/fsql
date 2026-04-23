#include "QueryInput.h"

#include "StringUtils.h"

#include <filesystem>
#include <sstream>

namespace fsql
{
    namespace
    {
        std::string join_arguments(int argc, char** argv)
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

    std::string QueryInput::executable_name(const char* argv0)
    {
        if (argv0 == nullptr)
        {
            return "fsql";
        }
        return std::filesystem::path(argv0).stem().string();
    }

    bool QueryInput::is_wrapper_command(const std::string& command)
    {
        return iequals(command, "select")
            || iequals(command, "insert")
            || iequals(command, "update")
            || iequals(command, "delete")
            || iequals(command, "create")
            || iequals(command, "drop");
    }

    std::string QueryInput::read(int argc, char** argv, std::istream& input)
    {
        std::string query;
        if (argc > 1)
        {
            query = join_arguments(argc, argv);
            const auto command = executable_name(argc > 0 ? argv[0] : nullptr);
            if (is_wrapper_command(command))
            {
                query = command + " " + query;
            }
            return query;
        }

        std::ostringstream buffer;
        buffer << input.rdbuf();
        return trim(buffer.str());
    }
}

