#pragma once

#include "ICoroExecutor.h"
#include "IOutputWriter.h"

#include <memory>

namespace sql
{
    /// @brief Renders execution results using the console table/status format.
    class ConsoleOutputWriter final : public IOutputWriter
    {
    public:
        /// @brief Initializes the writer with a coroutine executor.
        /// @param coro_executor Coroutine executor used to drive streamed rows.
        explicit ConsoleOutputWriter(std::shared_ptr<ICoroExecutor> coro_executor = nullptr);

        /// @brief Writes a structured execution result to an output stream.
        /// @param output Output stream receiving the rendered result.
        /// @param result Structured execution result.
        void write(std::ostream& output, const ExecutionResult& result) const override;

    private:
        std::shared_ptr<ICoroExecutor> coro_executor_;
    };
}

