#pragma once

#include "IOutputWriter.h"

namespace sql
{
    /// @brief Renders execution results using the console table/status format.
    class ConsoleOutputWriter final : public IOutputWriter
    {
    public:
        /// @brief Writes a structured execution result to an output stream.
        /// @param output Output stream receiving the rendered result.
        /// @param result Structured execution result.
        void write(std::ostream& output, const ExecutionResult& result) const override;
    };
}

