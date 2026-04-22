#pragma once

#include <string>

namespace sql
{
    /// @brief Coordinates command-line input, parsing, execution, and error handling.
    class Application
    {
    public:
        /// @brief Runs the application.
        /// @param argc Argument count.
        /// @param argv Argument vector.
        /// @return Process exit code.
        int run(int argc, char** argv);

    private:
        /// @brief Builds a SQL query string from command-line arguments.
        /// @param argc Argument count.
        /// @param argv Argument vector.
        /// @return Combined SQL query.
        static std::string join_arguments(int argc, char** argv);
    };
}
