#pragma once

#include <string>

namespace fsql
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
    };
}
