#pragma once

#include <iosfwd>
#include <string>

namespace sql
{
    /// @brief Builds executable/query input for the CLI runtime.
    class QueryInput
    {
    public:
        /// @brief Returns the executable stem derived from argv[0].
        /// @param argv0 Raw executable path.
        /// @return Executable stem.
        static std::string executable_name(const char* argv0);

        /// @brief Checks whether a command name is implemented as a SQL wrapper.
        /// @param command Executable stem.
        /// @return `true` when the executable wraps a SQL keyword.
        static bool is_wrapper_command(const std::string& command);

        /// @brief Builds a SQL query from argv or stdin.
        /// @param argc Argument count.
        /// @param argv Argument vector.
        /// @param input Input stream used when no explicit argv query is present.
        /// @return Trimmed SQL query.
        static std::string read(int argc, char** argv, std::istream& input);
    };
}

