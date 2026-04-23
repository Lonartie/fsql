#pragma once

#include <string>

namespace fsql
{
    /// @brief Supported token kinds produced by the tokenizer.
    enum class TokenType
    {
        Identifier,
        String,
        Number,
        Comma,
        Dot,
        LParen,
        RParen,
        Semicolon,
        Equal,
        DoubleEqual,
        NotEqual,
        Less,
        LessEqual,
        Greater,
        GreaterEqual,
        Plus,
        Minus,
        Slash,
        Percent,
        Ampersand,
        Pipe,
        Caret,
        Tilde,
        Exclamation,
        DoubleAmpersand,
        DoublePipe,
        Star,
        End
    };

    /// @brief Represents a lexical token.
    struct Token
    {
        /// @brief Token category.
        TokenType type{};

        /// @brief Original or normalized token text.
        std::string text;
    };
}

