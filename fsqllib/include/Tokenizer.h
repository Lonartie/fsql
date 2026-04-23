#pragma once

#include "SqlTypes.h"

#include <cstddef>
#include <string_view>
#include <vector>

namespace fsql
{
    /// @brief Converts SQL text into a sequence of lexical tokens.
    class Tokenizer
    {
    public:
        /// @brief Initializes the tokenizer.
        /// @param input SQL input text.
        explicit Tokenizer(std::string_view input);

        /// @brief Tokenizes the configured input.
        /// @return Ordered token sequence terminated by `TokenType::End`.
        std::vector<Token> tokenize();

    private:
        /// @brief Skips whitespace characters.
        void skip_whitespace();

        /// @brief Reads an identifier token.
        /// @return Parsed token.
        Token read_identifier();

        /// @brief Reads a numeric token.
        /// @return Parsed token.
        Token read_number();

        /// @brief Reads a quoted string token.
        /// @return Parsed token.
        Token read_string();

        /// @brief Source SQL text.
        std::string_view input_;

        /// @brief Current read position.
        std::size_t position_ = 0;
    };
}
