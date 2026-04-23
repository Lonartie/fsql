#pragma once

#include <algorithm>
#include <cctype>
#include <string>

namespace fsql
{
    /// @brief Trims leading and trailing whitespace.
    /// @param value Input string.
    /// @return Trimmed string.
    inline std::string trim(const std::string& value)
    {
        const auto begin = value.find_first_not_of(" \t\r\n");
        if (begin == std::string::npos)
        {
            return {};
        }

        const auto end = value.find_last_not_of(" \t\r\n");
        return value.substr(begin, end - begin + 1);
    }

    /// @brief Converts a string to uppercase.
    /// @param value Input string.
    /// @return Uppercase copy.
    inline std::string to_upper(std::string value)
    {
        std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
            return static_cast<char>(std::toupper(ch));
        });
        return value;
    }

    /// @brief Performs a case-insensitive equality comparison.
    /// @param left Left operand.
    /// @param right Right operand.
    /// @return `true` if equal ignoring case; otherwise `false`.
    inline bool iequals(const std::string& left, const std::string& right)
    {
        return to_upper(left) == to_upper(right);
    }

    /// @brief Determines whether a character can start an identifier.
    /// @param ch Character to test.
    /// @return `true` if valid; otherwise `false`.
    inline bool is_identifier_start(char ch)
    {
        return std::isalpha(static_cast<unsigned char>(ch)) || ch == '_';
    }

    /// @brief Determines whether a character can appear inside an identifier.
    /// @param ch Character to test.
    /// @return `true` if valid; otherwise `false`.
    inline bool is_identifier_part(char ch)
    {
        return std::isalnum(static_cast<unsigned char>(ch)) || ch == '_';
    }
}
