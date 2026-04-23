#pragma once

#include <stdexcept>
#include <string>

namespace fsql
{
    /// @brief Throws a runtime error with the provided message.
    /// @param message Error description.
    /// @throws std::runtime_error Always thrown.
    [[noreturn]] inline void fail(const std::string& message)
    {
        throw std::runtime_error(message);
    }
}
