#include "Parser.h"

#include "SqlError.h"
#include "StringUtils.h"

#include <utility>

namespace fsql
{
    Parser::Parser(std::vector<Token> tokens) : tokens_(std::move(tokens))
    {
    }

    Statement Parser::parse_statement()
    {
        if (match_keyword("ALTER"))
        {
            return parse_alter();
        }
        if (match_keyword("CREATE"))
        {
            return parse_create();
        }
        if (match_keyword("DROP"))
        {
            return parse_drop();
        }
        if (match_keyword("DELETE"))
        {
            return parse_delete();
        }
        if (match_keyword("INSERT"))
        {
            return parse_insert();
        }
        if (match_keyword("SELECT"))
        {
            return parse_select();
        }
        if (match_keyword("UPDATE"))
        {
            return parse_update();
        }

        fail("Expected ALTER, CREATE, DROP, DELETE, INSERT, SELECT, or UPDATE");
    }

    std::string Parser::expect_identifier(const std::string& message)
    {
        if (!check(TokenType::Identifier))
        {
            fail(message);
        }
        return advance().text;
    }

    RelationReference Parser::expect_relation_reference(const std::string& message)
    {
        RelationReference reference;
        if (check(TokenType::Identifier))
        {
            reference.name = advance().text;
            return reference;
        }
        if (check(TokenType::String))
        {
            reference.kind = RelationReference::Kind::FilePath;
            reference.name = advance().text;
            return reference;
        }
        fail(message);
    }

    std::string Parser::parse_identifier_reference(const std::string& message)
    {
        std::string identifier = expect_identifier(message);
        while (consume_optional(TokenType::Dot))
        {
            identifier += "." + expect_identifier("Expected identifier after '.'");
        }
        return identifier;
    }

    void Parser::expect_keyword(const std::string& keyword)
    {
        if (!match_keyword(keyword))
        {
            fail("Expected keyword " + keyword);
        }
    }

    bool Parser::match_keyword(const std::string& keyword)
    {
        if (check(TokenType::Identifier) && iequals(peek().text, keyword))
        {
            advance();
            return true;
        }
        return false;
    }

    bool Parser::consume_optional(TokenType type)
    {
        if (check(type))
        {
            advance();
            return true;
        }
        return false;
    }

    void Parser::expect(TokenType type, const std::string& message)
    {
        if (!check(type))
        {
            fail(message);
        }
        advance();
    }

    bool Parser::check(TokenType type) const
    {
        return peek().type == type;
    }

    const Token& Parser::peek() const
    {
        return tokens_[position_];
    }

    Token Parser::advance()
    {
        return tokens_[position_++];
    }
}
