#pragma once

#include "Executor.h"

#include <iosfwd>

namespace fsql
{
    /// @brief Defines how structured execution results are rendered for humans.
    class IOutputWriter
    {
    public:
        /// @brief Virtual destructor.
        virtual ~IOutputWriter() = default;

        /// @brief Writes a structured execution result to an output stream.
        /// @param output Output stream receiving the rendered result.
        /// @param result Structured execution result.
        virtual void write(std::ostream& output, const ExecutionResult& result) const = 0;
    };
}

