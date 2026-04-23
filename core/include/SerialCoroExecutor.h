#pragma once

#include "ICoroExecutor.h"

namespace sql
{
    /// @brief Drives coroutine streams serially from start to finish.
    class SerialCoroExecutor final : public ICoroExecutor
    {
    public:
        /// @brief Drives a row stream serially.
        /// @param rows Row stream.
        /// @param consumer Consumer invoked for each yielded row. Return `false` to stop consumption early.
        /// @return Number of consumed rows.
        std::size_t drive_rows(RowGenerator rows, const std::function<bool(const Row&)>& consumer) const override
        {
            std::size_t count = 0;
            for (const auto& row : rows)
            {
                ++count;
                if (!consumer(row))
                {
                    break;
                }
            }
            return count;
        }

        /// @brief Drives a scalar value stream serially.
        /// @param values Value stream.
        /// @param consumer Consumer invoked for each yielded value. Return `false` to stop consumption early.
        /// @return Number of consumed values.
        std::size_t drive_values(ValueGenerator values, const std::function<bool(const std::string&)>& consumer) const override
        {
            std::size_t count = 0;
            for (const auto& value : values)
            {
                ++count;
                if (!consumer(value))
                {
                    break;
                }
            }
            return count;
        }
    };
}

