#include "Tokenizer.h"

#include "SqlError.h"
#include "StringUtils.h"

#include <cctype>
#include <string>

namespace sql
{
    Tokenizer::Tokenizer(std::string_view input) : input_(input)
    {
    }

    std::vector<Token> Tokenizer::tokenize()
    {
        std::vector<Token> tokens;
        while (true)
        {
            skip_whitespace();
            if (position_ >= input_.size())
            {
                tokens.push_back({TokenType::End, {}});
                return tokens;
            }

            const char ch = input_[position_];
            if (is_identifier_start(ch))
            {
                tokens.push_back(read_identifier());
            }
            else if (std::isdigit(static_cast<unsigned char>(ch)) ||
                ((ch == '-' || ch == '+') && position_ + 1 < input_.size() && std::isdigit(static_cast<unsigned char>(input_[position_ + 1]))))
            {
                tokens.push_back(read_number());
            }
            else if (ch == '\'' || ch == '"')
            {
                tokens.push_back(read_string());
            }
            else
            {
                ++position_;
                switch (ch)
                {
                case ',': tokens.push_back({TokenType::Comma, ","}); break;
                case '.': tokens.push_back({TokenType::Dot, "."}); break;
                case '(': tokens.push_back({TokenType::LParen, "("}); break;
                case ')': tokens.push_back({TokenType::RParen, ")"}); break;
                case ';': tokens.push_back({TokenType::Semicolon, ";"}); break;
                case '+': tokens.push_back({TokenType::Plus, "+"}); break;
                case '-': tokens.push_back({TokenType::Minus, "-"}); break;
                case '*': tokens.push_back({TokenType::Star, "*"}); break;
                case '/': tokens.push_back({TokenType::Slash, "/"}); break;
                case '%': tokens.push_back({TokenType::Percent, "%"}); break;
                case '^': tokens.push_back({TokenType::Caret, "^"}); break;
                case '~': tokens.push_back({TokenType::Tilde, "~"}); break;
                case '=':
                    if (position_ < input_.size() && input_[position_] == '=')
                    {
                        ++position_;
                        tokens.push_back({TokenType::DoubleEqual, "=="});
                    }
                    else
                    {
                        tokens.push_back({TokenType::Equal, "="});
                    }
                    break;
                case '!':
                    if (position_ < input_.size() && input_[position_] == '=')
                    {
                        ++position_;
                        tokens.push_back({TokenType::NotEqual, "!="});
                    }
                    else
                    {
                        tokens.push_back({TokenType::Exclamation, "!"});
                    }
                    break;
                case '<':
                    if (position_ < input_.size() && input_[position_] == '=')
                    {
                        ++position_;
                        tokens.push_back({TokenType::LessEqual, "<="});
                    }
                    else
                    {
                        tokens.push_back({TokenType::Less, "<"});
                    }
                    break;
                case '>':
                    if (position_ < input_.size() && input_[position_] == '=')
                    {
                        ++position_;
                        tokens.push_back({TokenType::GreaterEqual, ">="});
                    }
                    else
                    {
                        tokens.push_back({TokenType::Greater, ">"});
                    }
                    break;
                case '&':
                    if (position_ < input_.size() && input_[position_] == '&')
                    {
                        ++position_;
                        tokens.push_back({TokenType::DoubleAmpersand, "&&"});
                    }
                    else
                    {
                        tokens.push_back({TokenType::Ampersand, "&"});
                    }
                    break;
                case '|':
                    if (position_ < input_.size() && input_[position_] == '|')
                    {
                        ++position_;
                        tokens.push_back({TokenType::DoublePipe, "||"});
                    }
                    else
                    {
                        tokens.push_back({TokenType::Pipe, "|"});
                    }
                    break;
                default: fail(std::string("Unexpected character: ") + ch);
                }
            }
        }
    }

    void Tokenizer::skip_whitespace()
    {
        while (position_ < input_.size() && std::isspace(static_cast<unsigned char>(input_[position_])))
        {
            ++position_;
        }
    }

    Token Tokenizer::read_identifier()
    {
        const std::size_t start = position_;
        ++position_;
        while (position_ < input_.size() && is_identifier_part(input_[position_]))
        {
            ++position_;
        }

        return {TokenType::Identifier, std::string(input_.substr(start, position_ - start))};
    }

    Token Tokenizer::read_number()
    {
        const std::size_t start = position_;
        if (input_[position_] == '+' || input_[position_] == '-')
        {
            ++position_;
        }

        while (position_ < input_.size() &&
            (std::isdigit(static_cast<unsigned char>(input_[position_])) || input_[position_] == '.'))
        {
            ++position_;
        }

        return {TokenType::Number, std::string(input_.substr(start, position_ - start))};
    }

    Token Tokenizer::read_string()
    {
        const char quote = input_[position_++];
        std::string value;
        while (position_ < input_.size())
        {
            const char ch = input_[position_++];
            if (ch == quote)
            {
                if (position_ < input_.size() && input_[position_] == quote)
                {
                    value += quote;
                    ++position_;
                    continue;
                }

                return {TokenType::String, value};
            }

            value += ch;
        }

        fail("Unterminated string literal");
    }
}
