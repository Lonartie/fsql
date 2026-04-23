#include "doctest.h"

#include "test_support.h"

TEST_SUITE_BEGIN("Tokenizer::tokenize");

TEST_CASE("parses identifiers strings and punctuation")
{
    fsql::Tokenizer tokenizer("SELECT title, text FROM todos WHERE done = 'false';");
    const auto tokens = tokenizer.tokenize();

    REQUIRE_GE(tokens.size(), 11U);
    CHECK_EQ(static_cast<int>(tokens[0].type), static_cast<int>(fsql::TokenType::Identifier));
    CHECK_EQ(tokens[0].text, "SELECT");
    CHECK_EQ(tokens[1].text, "title");
    CHECK_EQ(static_cast<int>(tokens[2].type), static_cast<int>(fsql::TokenType::Comma));
    CHECK_EQ(tokens[3].text, "text");
    CHECK_EQ(tokens[4].text, "FROM");
    CHECK_EQ(tokens[5].text, "todos");
    CHECK_EQ(tokens[6].text, "WHERE");
    CHECK_EQ(tokens[7].text, "done");
    CHECK_EQ(static_cast<int>(tokens[8].type), static_cast<int>(fsql::TokenType::Equal));
    CHECK_EQ(static_cast<int>(tokens[9].type), static_cast<int>(fsql::TokenType::String));
    CHECK_EQ(tokens[9].text, "false");
    CHECK_EQ(static_cast<int>(tokens[10].type), static_cast<int>(fsql::TokenType::Semicolon));
}

TEST_CASE("parses signed numbers")
{
    fsql::Tokenizer tokenizer("INSERT INTO metrics VALUES (-12.5, +7);");
    const auto tokens = tokenizer.tokenize();

    CHECK_EQ(static_cast<int>(tokens[5].type), static_cast<int>(fsql::TokenType::Number));
    CHECK_EQ(tokens[5].text, "-12.5");
    CHECK_EQ(static_cast<int>(tokens[7].type), static_cast<int>(fsql::TokenType::Number));
    CHECK_EQ(tokens[7].text, "+7");
}

TEST_CASE("supports doubled quotes inside strings")
{
    fsql::Tokenizer tokenizer("INSERT INTO todos VALUES ('It''s done');");
    const auto tokens = tokenizer.tokenize();

    CHECK_EQ(static_cast<int>(tokens[5].type), static_cast<int>(fsql::TokenType::String));
    CHECK_EQ(tokens[5].text, "It's done");
}

TEST_CASE("tokenizes qualified identifiers and source dots")
{
    fsql::Tokenizer tokenizer("SELECT tasks.title FROM tasks, teams WHERE tasks.team_id = teams.id;");
    const auto tokens = tokenizer.tokenize();

    CHECK_EQ(tokens[1].text, "tasks");
    CHECK_EQ(static_cast<int>(tokens[2].type), static_cast<int>(fsql::TokenType::Dot));
    CHECK_EQ(tokens[3].text, "title");
    CHECK_EQ(tokens[5].text, "tasks");
    CHECK_EQ(static_cast<int>(tokens[6].type), static_cast<int>(fsql::TokenType::Comma));
    CHECK_EQ(tokens[7].text, "teams");
}

TEST_CASE("rejects unterminated strings")
{
    fsql::Tokenizer tokenizer("SELECT 'unterminated");
    CHECK_THROWS_AS(tokenizer.tokenize(), std::runtime_error);
}

TEST_SUITE_END();

