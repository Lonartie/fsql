#pragma once

#include "CoroTypes.h"

#include <cstddef>
#include <functional>
#include <string>

namespace fsql
{
    /// @brief Defines how coroutine streams are driven from start to finish.
    class ICoroExecutor
    {
    public:
        /// @brief Virtual destructor.
        virtual ~ICoroExecutor() = default;

        /// @brief Drives a row stream and forwards each yielded row to a consumer.
        /// @param rows Row stream.
        /// @param consumer Consumer invoked for each yielded row. Return `false` to stop consumption early.
        /// @return Number of consumed rows.
        virtual std::size_t drive_rows(RowGenerator rows, const std::function<bool(const Row&)>& consumer) const = 0;

        /// @brief Drives a scalar value stream and forwards each yielded value to a consumer.
        /// @param values Value stream.
        /// @param consumer Consumer invoked for each yielded value. Return `false` to stop consumption early.
        /// @return Number of consumed values.
        virtual std::size_t drive_values(ValueGenerator values, const std::function<bool(const std::string&)>& consumer) const = 0;
    };
}

