#include "StorageFormatUtils.h"

#include "SqlError.h"

#include <array>

namespace fsql::detail
{
    namespace
    {
        void skip_whitespace(const std::string& text, std::size_t& position)
        {
            while (position < text.size() && std::isspace(static_cast<unsigned char>(text[position])))
            {
                ++position;
            }
        }

        void expect_char(const std::string& text, std::size_t& position, char expected, const std::string& message)
        {
            skip_whitespace(text, position);
            if (position >= text.size() || text[position] != expected)
            {
                fail(message);
            }
            ++position;
        }

        bool consume_char(const std::string& text, std::size_t& position, char expected)
        {
            skip_whitespace(text, position);
            if (position < text.size() && text[position] == expected)
            {
                ++position;
                return true;
            }
            return false;
        }

        std::string parse_quoted_string(const std::string& text, std::size_t& position, const std::string& message)
        {
            skip_whitespace(text, position);
            if (position >= text.size() || text[position] != '"')
            {
                fail(message);
            }
            ++position;

            std::string value;
            while (position < text.size())
            {
                const char ch = text[position++];
                if (ch == '"')
                {
                    return value;
                }
                if (ch != '\\')
                {
                    value += ch;
                    continue;
                }
                if (position >= text.size())
                {
                    fail("Unterminated escape sequence");
                }

                const char escaped = text[position++];
                switch (escaped)
                {
                case '\\': value += '\\'; break;
                case '"': value += '"'; break;
                case 'n': value += '\n'; break;
                case 'r': value += '\r'; break;
                case 't': value += '\t'; break;
                default:
                    fail("Unsupported escape sequence");
                }
            }

            fail("Unterminated quoted string");
        }
    }

    std::string quoted_string_body(const std::string& value)
    {
        std::string escaped;
        for (const unsigned char ch : value)
        {
            switch (ch)
            {
            case '\\': escaped += "\\\\"; break;
            case '"': escaped += "\\\""; break;
            case '\n': escaped += "\\n"; break;
            case '\r': escaped += "\\r"; break;
            case '\t': escaped += "\\t"; break;
            default:
                escaped += static_cast<char>(ch);
                break;
            }
        }
        return escaped;
    }

    std::vector<std::string> parse_quoted_string_array(const std::string& text, const std::string& message)
    {
        std::vector<std::string> values;
        std::size_t position = 0;
        expect_char(text, position, '[', message);
        skip_whitespace(text, position);
        if (consume_char(text, position, ']'))
        {
            skip_whitespace(text, position);
            if (position != text.size())
            {
                fail(message);
            }
            return values;
        }

        while (true)
        {
            values.push_back(parse_quoted_string(text, position, "Expected quoted string inside array"));
            skip_whitespace(text, position);
            if (consume_char(text, position, ']'))
            {
                skip_whitespace(text, position);
                if (position != text.size())
                {
                    fail(message);
                }
                return values;
            }
            expect_char(text, position, ',', "Expected ',' or ']' inside array");
        }
    }

    std::string xml_escape(const std::string& value)
    {
        std::string escaped;
        for (const char ch : value)
        {
            switch (ch)
            {
            case '&': escaped += "&amp;"; break;
            case '<': escaped += "&lt;"; break;
            case '>': escaped += "&gt;"; break;
            case '"': escaped += "&quot;"; break;
            case '\'': escaped += "&apos;"; break;
            default:
                escaped += ch;
                break;
            }
        }
        return escaped;
    }

    std::string xml_unescape(std::string value)
    {
        const std::array<std::pair<std::string_view, std::string_view>, 5> entities = {{
            {"&amp;", "&"},
            {"&lt;", "<"},
            {"&gt;", ">"},
            {"&quot;", "\""},
            {"&apos;", "'"}
        }};

        for (const auto& [entity, replacement] : entities)
        {
            std::size_t position = 0;
            while ((position = value.find(entity, position)) != std::string::npos)
            {
                value.replace(position, entity.size(), replacement);
                position += replacement.size();
            }
        }
        return value;
    }

    Table validate_loaded_table(Table table)
    {
        if (table.columns.empty())
        {
            fail("Table has no columns: " + table.name);
        }
        for (const auto& row : table.rows)
        {
            if (row.size() != table.columns.size())
            {
                fail("Row column count mismatch in table: " + table.name);
            }
        }
        return table;
    }
}
