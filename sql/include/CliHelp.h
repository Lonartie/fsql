#pragma once

#include <iosfwd>
#include <string_view>

namespace sql
{
    /// @brief Renders CLI help for the main binary and wrapper commands.
    class CliHelp
    {
    public:
        /// @brief Checks whether the current invocation requests help.
        /// @param argc Argument count.
        /// @param argv Argument vector.
        /// @return `true` when help should be displayed.
        static bool should_show(int argc, char** argv);

        /// @brief Writes help for a specific program name.
        /// @param output Destination stream.
        /// @param program_name Executable stem or alias.
        static void write(std::ostream& output, std::string_view program_name);
    };
}

