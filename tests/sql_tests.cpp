#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include "doctest.h"

#include "CsvStorage.h"
#include "Executor.h"
#include "MemoryStorage.h"
#include "Parser.h"
#include "SqlTypes.h"
#include "Tokenizer.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace
{
    sql::Statement parse_statement(const std::string& query)
    {
        sql::Tokenizer tokenizer(query);
        sql::Parser parser(tokenizer.tokenize());
        return parser.parse_statement();
    }

    sql::ExpressionPtr parse_expression(const std::string& query)
    {
        sql::Tokenizer tokenizer(query);
        sql::Parser parser(tokenizer.tokenize());
        return parser.parse_expression();
    }
}

TEST_CASE("Tokenizer parses identifiers strings and punctuation")
{
    sql::Tokenizer tokenizer("SELECT title, text FROM todos WHERE done = 'false';");
    const auto tokens = tokenizer.tokenize();

    REQUIRE_GE(tokens.size(), 11U);
    CHECK_EQ(static_cast<int>(tokens[0].type), static_cast<int>(sql::TokenType::Identifier));
    CHECK_EQ(tokens[0].text, "SELECT");
    CHECK_EQ(tokens[1].text, "title");
    CHECK_EQ(static_cast<int>(tokens[2].type), static_cast<int>(sql::TokenType::Comma));
    CHECK_EQ(tokens[3].text, "text");
    CHECK_EQ(tokens[4].text, "FROM");
    CHECK_EQ(tokens[5].text, "todos");
    CHECK_EQ(tokens[6].text, "WHERE");
    CHECK_EQ(tokens[7].text, "done");
    CHECK_EQ(static_cast<int>(tokens[8].type), static_cast<int>(sql::TokenType::Equal));
    CHECK_EQ(static_cast<int>(tokens[9].type), static_cast<int>(sql::TokenType::String));
    CHECK_EQ(tokens[9].text, "false");
    CHECK_EQ(static_cast<int>(tokens[10].type), static_cast<int>(sql::TokenType::Semicolon));
}

TEST_CASE("Tokenizer parses signed numbers")
{
    sql::Tokenizer tokenizer("INSERT INTO metrics VALUES (-12.5, +7);");
    const auto tokens = tokenizer.tokenize();

    CHECK_EQ(static_cast<int>(tokens[5].type), static_cast<int>(sql::TokenType::Number));
    CHECK_EQ(tokens[5].text, "-12.5");
    CHECK_EQ(static_cast<int>(tokens[7].type), static_cast<int>(sql::TokenType::Number));
    CHECK_EQ(tokens[7].text, "+7");
}

TEST_CASE("Tokenizer supports doubled quotes inside strings")
{
    sql::Tokenizer tokenizer("INSERT INTO todos VALUES ('It''s done');");
    const auto tokens = tokenizer.tokenize();

    CHECK_EQ(static_cast<int>(tokens[5].type), static_cast<int>(sql::TokenType::String));
    CHECK_EQ(tokens[5].text, "It's done");
}

TEST_CASE("Tokenizer rejects unterminated strings")
{
    sql::Tokenizer tokenizer("SELECT 'unterminated");
    CHECK_THROWS_AS(tokenizer.tokenize(), std::runtime_error);
}

TEST_CASE("Parser parses create statement")
{
    const auto statement = parse_statement("CREATE TABLE todos (title, category, text, done);");

    CHECK_EQ(static_cast<int>(statement.kind), static_cast<int>(sql::Statement::Kind::Create));
    CHECK_EQ(statement.create.table_name, "todos");
    REQUIRE_EQ(statement.create.columns.size(), 4U);
    CHECK_EQ(statement.create.columns[0].name, "title");
    CHECK_FALSE(statement.create.columns[0].auto_increment);
    CHECK_EQ(statement.create.columns[3].name, "done");
}

TEST_CASE("Parser parses insert with explicit columns")
{
    const auto statement = parse_statement("INSERT INTO todos (title, done) VALUES ('Buy milk', true);");

    CHECK_EQ(static_cast<int>(statement.kind), static_cast<int>(sql::Statement::Kind::Insert));
    CHECK(statement.insert.columns.has_value());
    REQUIRE(statement.insert.columns.has_value());
    CHECK_EQ(statement.insert.table_name, "todos");
    CHECK_EQ(statement.insert.columns->size(), 2U);
    CHECK_EQ((*statement.insert.columns)[0], "title");
    REQUIRE_EQ(statement.insert.values.size(), 2U);
    CHECK(statement.insert.values[0] != nullptr);
    CHECK_EQ(static_cast<int>(statement.insert.values[0]->kind), static_cast<int>(sql::ExpressionKind::Literal));
    CHECK_EQ(statement.insert.values[0]->text, "Buy milk");
    CHECK_EQ(static_cast<int>(statement.insert.values[1]->kind), static_cast<int>(sql::ExpressionKind::Identifier));
    CHECK_EQ(statement.insert.values[1]->text, "true");
}

TEST_CASE("Parser parses select star with where clause")
{
    const auto statement = parse_statement("SELECT * FROM todos WHERE category = 'home';");

    CHECK_EQ(static_cast<int>(statement.kind), static_cast<int>(sql::Statement::Kind::Select));
    CHECK(statement.select.select_all);
    CHECK_EQ(statement.select.table_name, "todos");
    REQUIRE(statement.select.where != nullptr);
    CHECK_EQ(static_cast<int>(statement.select.where->kind), static_cast<int>(sql::ExpressionKind::Binary));
    CHECK_EQ(static_cast<int>(statement.select.where->binary_operator), static_cast<int>(sql::BinaryOperator::Equal));
}

TEST_CASE("Parser parses update assignments")
{
    const auto statement = parse_statement("UPDATE todos SET done = true, category = 'done' WHERE title = 'Buy milk';");

    CHECK_EQ(static_cast<int>(statement.kind), static_cast<int>(sql::Statement::Kind::Update));
    CHECK_EQ(statement.update.table_name, "todos");
    REQUIRE_EQ(statement.update.assignments.size(), 2U);
    CHECK_EQ(statement.update.assignments[0].first, "done");
    CHECK(statement.update.assignments[0].second != nullptr);
    CHECK_EQ(statement.update.assignments[0].second->text, "true");
    CHECK_EQ(statement.update.assignments[1].first, "category");
    CHECK(statement.update.assignments[1].second != nullptr);
    CHECK_EQ(statement.update.assignments[1].second->text, "done");
    REQUIRE(statement.update.where != nullptr);
    CHECK_EQ(static_cast<int>(statement.update.where->kind), static_cast<int>(sql::ExpressionKind::Binary));
}

TEST_CASE("Parser accepts statements without semicolons")
{
    const auto statement = parse_statement("SELECT title FROM todos");
    CHECK_EQ(static_cast<int>(statement.kind), static_cast<int>(sql::Statement::Kind::Select));
    CHECK_EQ(statement.select.columns[0], "title");
}

TEST_CASE("Parser rejects unsupported statements")
{
    CHECK_THROWS_AS(parse_statement("MERGE INTO todos;"), std::runtime_error);
}

TEST_CASE("Parser rejects trailing tokens")
{
    CHECK_THROWS_AS(parse_statement("SELECT title FROM todos extra"), std::runtime_error);
}

TEST_CASE("MemoryStorage stores and loads tables")
{
    sql::MemoryStorage storage;
    sql::Table table{"todos", {"title", "done"}, {{"Buy milk", "false"}}};

    storage.save_table(table);
    const auto loaded = storage.load_table("todos");

    CHECK_EQ(loaded.name, "todos");
    REQUIRE_EQ(loaded.columns.size(), 2U);
    REQUIRE_EQ(loaded.rows.size(), 1U);
    CHECK_EQ(loaded.rows[0][0], "Buy milk");
    CHECK_EQ(storage.column_index(loaded, "DONE"), 1U);
}

TEST_CASE("MemoryStorage rejects missing tables")
{
    sql::MemoryStorage storage;
    CHECK_THROWS_AS(storage.load_table("missing"), std::runtime_error);
}

TEST_CASE("CsvStorage escapes and parses quoted values")
{
    const auto escaped = sql::CsvStorage::escape_csv("hello, \"world\"");
    CHECK_EQ(escaped, "\"hello, \"\"world\"\"\"");

    const auto fields = sql::CsvStorage::parse_csv_line("\"hello, \"\"world\"\"\",done");
    REQUIRE_EQ(fields.size(), 2U);
    CHECK_EQ(fields[0], "hello, \"world\"");
    CHECK_EQ(fields[1], "done");
}

TEST_CASE("CsvStorage rejects unterminated quoted fields")
{
    CHECK_THROWS_AS(sql::CsvStorage::parse_csv_line("\"unterminated"), std::runtime_error);
}

TEST_CASE("Executor create insert and select workflow")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    std::ostringstream output;
    sql::Executor executor(storage, output);

    executor.execute(parse_statement("CREATE TABLE todos (title, category, text, done);"));
    executor.execute(parse_statement("INSERT INTO todos VALUES ('Buy milk', 'home', '2 liters', 'false');"));
    executor.execute(parse_statement("SELECT category, text FROM todos;"));

    const auto text = output.str();
    CHECK(text.find("Created table 'todos'") != std::string::npos);
    CHECK(text.find("Inserted 1 row into 'todos'") != std::string::npos);
    CHECK(text.find("home") != std::string::npos);
    CHECK(text.find("2 liters") != std::string::npos);
    CHECK(text.find("1 row(s) selected") != std::string::npos);
}

TEST_CASE("Executor insert with explicit columns fills unspecified values with empty strings")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    std::ostringstream output;
    sql::Executor executor(storage, output);

    executor.execute(parse_statement("CREATE TABLE todos (title, category, text, done);"));
    executor.execute(parse_statement("INSERT INTO todos (title, done) VALUES ('Buy milk', true);"));

    const auto table = storage->load_table("todos");
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(table.rows[0][0], "Buy milk");
    CHECK_EQ(table.rows[0][1], "");
    CHECK_EQ(table.rows[0][2], "");
    CHECK_EQ(table.rows[0][3], "true");
}

TEST_CASE("Executor update modifies only matching rows")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    std::ostringstream output;
    sql::Executor executor(storage, output);

    executor.execute(parse_statement("CREATE TABLE todos (title, category, text, done);"));
    executor.execute(parse_statement("INSERT INTO todos VALUES ('Buy milk', 'home', '2 liters', 'false');"));
    executor.execute(parse_statement("INSERT INTO todos VALUES ('Write docs', 'work', 'API docs', 'false');"));
    executor.execute(parse_statement("UPDATE todos SET done = true WHERE title = 'Buy milk';"));

    const auto table = storage->load_table("todos");
    REQUIRE_EQ(table.rows.size(), 2U);
    CHECK_EQ(table.rows[0][3], "true");
    CHECK_EQ(table.rows[1][3], "false");
    CHECK(output.str().find("Updated 1 row(s) in 'todos'") != std::string::npos);
}

TEST_CASE("Executor update without where modifies all rows")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    std::ostringstream output;
    sql::Executor executor(storage, output);

    executor.execute(parse_statement("CREATE TABLE todos (title, done);"));
    executor.execute(parse_statement("INSERT INTO todos VALUES ('A', false);"));
    executor.execute(parse_statement("INSERT INTO todos VALUES ('B', false);"));
    executor.execute(parse_statement("UPDATE todos SET done = true;"));

    const auto table = storage->load_table("todos");
    REQUIRE_EQ(table.rows.size(), 2U);
    CHECK_EQ(table.rows[0][1], "true");
    CHECK_EQ(table.rows[1][1], "true");
    CHECK(output.str().find("Updated 2 row(s) in 'todos'") != std::string::npos);
}

TEST_CASE("Executor select where filters rows")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    std::ostringstream output;
    sql::Executor executor(storage, output);

    executor.execute(parse_statement("CREATE TABLE todos (title, category, text, done);"));
    executor.execute(parse_statement("INSERT INTO todos VALUES ('Buy milk', 'home', '2 liters', 'false');"));
    executor.execute(parse_statement("INSERT INTO todos VALUES ('Write docs', 'work', 'API docs', 'false');"));
    output.str("");
    output.clear();

    executor.execute(parse_statement("SELECT title FROM todos WHERE category = 'work';"));

    const auto text = output.str();
    CHECK(text.find("Write docs") != std::string::npos);
    CHECK(text.find("Buy milk") == std::string::npos);
    CHECK(text.find("1 row(s) selected") != std::string::npos);
}

TEST_CASE("Executor select all returns all columns")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    std::ostringstream output;
    sql::Executor executor(storage, output);

    executor.execute(parse_statement("CREATE TABLE todos (title, category);"));
    executor.execute(parse_statement("INSERT INTO todos VALUES ('Buy milk', 'home');"));
    output.str("");
    output.clear();

    executor.execute(parse_statement("SELECT * FROM todos;"));

    const auto text = output.str();
    CHECK(text.find("title") != std::string::npos);
    CHECK(text.find("category") != std::string::npos);
    CHECK(text.find("Buy milk") != std::string::npos);
    CHECK(text.find("home") != std::string::npos);
}

TEST_CASE("Executor select escapes csv output")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    std::ostringstream output;
    sql::Executor executor(storage, output);

    executor.execute(parse_statement("CREATE TABLE todos (title, text);"));
    executor.execute(parse_statement("INSERT INTO todos VALUES ('Buy milk', 'hello, world');"));
    output.str("");
    output.clear();

    executor.execute(parse_statement("SELECT text FROM todos;"));

    CHECK(output.str().find("hello, world") != std::string::npos);
}

TEST_CASE("Executor rejects duplicate table creation")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    std::ostringstream output;
    sql::Executor executor(storage, output);

    executor.execute(parse_statement("CREATE TABLE todos (title, done);"));
    CHECK_THROWS_AS(executor.execute(parse_statement("CREATE TABLE todos (title, done);")), std::runtime_error);
}

TEST_CASE("Executor rejects unknown columns")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    std::ostringstream output;
    sql::Executor executor(storage, output);

    executor.execute(parse_statement("CREATE TABLE todos (title, done);"));
    CHECK_THROWS_AS(executor.execute(parse_statement("SELECT missing FROM todos;")), std::runtime_error);
    CHECK_THROWS_AS(executor.execute(parse_statement("UPDATE todos SET missing = true;")), std::runtime_error);
    CHECK_THROWS_AS(executor.execute(parse_statement("INSERT INTO todos (missing) VALUES (true);")), std::runtime_error);
}

TEST_CASE("Executor rejects mismatched insert value counts")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    std::ostringstream output;
    sql::Executor executor(storage, output);

    executor.execute(parse_statement("CREATE TABLE todos (title, done);"));
    CHECK_THROWS_AS(executor.execute(parse_statement("INSERT INTO todos VALUES ('Buy milk');")), std::runtime_error);
    CHECK_THROWS_AS(executor.execute(parse_statement("INSERT INTO todos (title) VALUES ('Buy milk', true);")), std::runtime_error);
}

TEST_CASE("Executor rejects operations on missing tables")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    std::ostringstream output;
    sql::Executor executor(storage, output);

    CHECK_THROWS_AS(executor.execute(parse_statement("SELECT * FROM missing;")), std::runtime_error);
    CHECK_THROWS_AS(executor.execute(parse_statement("INSERT INTO missing VALUES ('x');")), std::runtime_error);
    CHECK_THROWS_AS(executor.execute(parse_statement("UPDATE missing SET value = 1;")), std::runtime_error);
}

TEST_CASE("CsvStorage round-trips a table on disk")
{
    const auto temp_dir = std::filesystem::temp_directory_path() / "sql_doctest_roundtrip";
    std::filesystem::create_directories(temp_dir);
    const auto csv_path = temp_dir / "todos.csv";
    std::filesystem::remove(csv_path);

    sql::CsvStorage storage(temp_dir);
    sql::Table table{"todos", {"title", "text"}, {{"Buy milk", "hello, world"}}};
    storage.save_table(table);

    const auto loaded = storage.load_table("todos");
    REQUIRE_EQ(loaded.rows.size(), 1U);
    CHECK_EQ(loaded.rows[0][0], "Buy milk");
    CHECK_EQ(loaded.rows[0][1], "hello, world");

    std::filesystem::remove(csv_path);
    std::filesystem::remove(temp_dir);
}

TEST_CASE("Executor supports NOW() in insert values")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    std::ostringstream output;
    sql::Executor executor(storage, output);

    executor.execute(parse_statement("CREATE TABLE events (id, created_at, text);"));
    CHECK_NOTHROW(executor.execute(parse_statement("INSERT INTO events VALUES (1, NOW(), 'created');")));

    const auto table = storage->load_table("events");
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_FALSE(table.rows[0][1].empty());
    CHECK_EQ(table.rows[0][2], "created");
}

TEST_CASE("Parser accepts AUTO_INCREMENT in create table")
{
    const auto statement = parse_statement("CREATE TABLE todos (id AUTO_INCREMENT, title, done);");
    REQUIRE_EQ(statement.create.columns.size(), 3U);
    CHECK_EQ(statement.create.columns[0].name, "id");
    CHECK(statement.create.columns[0].auto_increment);
}

TEST_CASE("Executor auto increments omitted id column")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    std::ostringstream output;
    sql::Executor executor(storage, output);

    executor.execute(parse_statement("CREATE TABLE todos (id AUTO_INCREMENT, title, done);"));
    executor.execute(parse_statement("INSERT INTO todos (title, done) VALUES ('Buy milk', false);"));
    executor.execute(parse_statement("INSERT INTO todos (title, done) VALUES ('Write docs', true);"));

    const auto table = storage->load_table("todos");
    REQUIRE_EQ(table.rows.size(), 2U);
    CHECK_EQ(table.rows[0][0], "1");
    CHECK_EQ(table.rows[1][0], "2");
}

TEST_CASE("Executor pretty formats select output")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    std::ostringstream output;
    sql::Executor executor(storage, output);

    executor.execute(parse_statement("CREATE TABLE todos (title, category);"));
    executor.execute(parse_statement("INSERT INTO todos VALUES ('Buy milk', 'home');"));
    executor.execute(parse_statement("INSERT INTO todos VALUES ('Write docs', 'work');"));
    output.str("");
    output.clear();

    executor.execute(parse_statement("SELECT title, category FROM todos;"));

    const auto text = output.str();
    CHECK(text.find("+------------+----------+") != std::string::npos);
    CHECK(text.find("| title      | category |") != std::string::npos);
    CHECK(text.find("| Buy milk   | home     |") != std::string::npos);
    CHECK(text.find("| Write docs | work     |") != std::string::npos);
    CHECK(text.find("| title|") == std::string::npos);
    CHECK(text.find("| Buy milk|") == std::string::npos);
    CHECK(text.find("2 row(s) selected") != std::string::npos);
}

TEST_CASE("Parser parses drop table statement")
{
    const auto statement = parse_statement("DROP TABLE todos;");
    CHECK_EQ(static_cast<int>(statement.kind), static_cast<int>(sql::Statement::Kind::Drop));
    CHECK_EQ(statement.drop.table_name, "todos");
}

TEST_CASE("Executor drops table from memory storage")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    std::ostringstream output;
    sql::Executor executor(storage, output);

    executor.execute(parse_statement("CREATE TABLE todos (title, done);"));
    executor.execute(parse_statement("DROP TABLE todos;"));

    CHECK(output.str().find("Dropped table 'todos'") != std::string::npos);
    CHECK_THROWS_AS(storage->load_table("todos"), std::runtime_error);
}

TEST_CASE("Executor rejects dropping missing table")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    std::ostringstream output;
    sql::Executor executor(storage, output);

    CHECK_THROWS_AS(executor.execute(parse_statement("DROP TABLE missing;")), std::runtime_error);
}

TEST_CASE("CsvStorage drop removes csv file")
{
    const auto temp_dir = std::filesystem::temp_directory_path() / "sql_doctest_drop";
    std::filesystem::create_directories(temp_dir);
    const auto csv_path = temp_dir / "todos.csv";
    std::filesystem::remove(csv_path);

    auto storage = std::make_shared<sql::CsvStorage>(temp_dir);
    std::ostringstream output;
    sql::Executor executor(storage, output);

    executor.execute(parse_statement("CREATE TABLE todos (title, done);"));
    REQUIRE(std::filesystem::exists(csv_path));
    executor.execute(parse_statement("DROP TABLE todos;"));
    CHECK_FALSE(std::filesystem::exists(csv_path));

    std::filesystem::remove(temp_dir);
}

TEST_CASE("Executor supports arithmetic expressions in WHERE")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    std::ostringstream output;
    sql::Executor executor(storage, output);

    executor.execute(parse_statement("CREATE TABLE nums (a, b, label);"));
    executor.execute(parse_statement("INSERT INTO nums VALUES (2, 3, 'match');"));
    executor.execute(parse_statement("INSERT INTO nums VALUES (2, 2, 'miss');"));
    output.str("");
    output.clear();

    executor.execute(parse_statement("SELECT label FROM nums WHERE a + b * 2 = 8;"));

    const auto text = output.str();
    CHECK(text.find("match") != std::string::npos);
    CHECK(text.find("miss") == std::string::npos);
}

TEST_CASE("Executor supports comparison and logical expressions in WHERE")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    std::ostringstream output;
    sql::Executor executor(storage, output);

    executor.execute(parse_statement("CREATE TABLE nums (a, b, label);"));
    executor.execute(parse_statement("INSERT INTO nums VALUES (5, 1, 'left');"));
    executor.execute(parse_statement("INSERT INTO nums VALUES (1, 5, 'right');"));
    executor.execute(parse_statement("INSERT INTO nums VALUES (1, 1, 'none');"));
    output.str("");
    output.clear();

    executor.execute(parse_statement("SELECT label FROM nums WHERE (a > b && a >= 5) || (b > a && b >= 5);"));

    const auto text = output.str();
    CHECK(text.find("left") != std::string::npos);
    CHECK(text.find("right") != std::string::npos);
    CHECK(text.find("none") == std::string::npos);
}

TEST_CASE("Executor supports bitwise expressions in WHERE")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    std::ostringstream output;
    sql::Executor executor(storage, output);

    executor.execute(parse_statement("CREATE TABLE flags (value, label);"));
    executor.execute(parse_statement("INSERT INTO flags VALUES (6, 'has-bit-2');"));
    executor.execute(parse_statement("INSERT INTO flags VALUES (1, 'no-bit-2');"));
    output.str("");
    output.clear();

    executor.execute(parse_statement("SELECT label FROM flags WHERE (value & 2) = 2;"));

    const auto text = output.str();
    CHECK(text.find("has-bit-2") != std::string::npos);
    CHECK(text.find("no-bit-2") == std::string::npos);
}

TEST_CASE("Executor supports SELECT subqueries in WHERE expressions")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    std::ostringstream output;
    sql::Executor executor(storage, output);

    executor.execute(parse_statement("CREATE TABLE todos (title, category);"));
    executor.execute(parse_statement("CREATE TABLE defaults (value);"));
    executor.execute(parse_statement("INSERT INTO todos VALUES ('Buy milk', 'home');"));
    executor.execute(parse_statement("INSERT INTO todos VALUES ('Write docs', 'work');"));
    executor.execute(parse_statement("INSERT INTO defaults VALUES ('work');"));
    output.str("");
    output.clear();

    executor.execute(parse_statement("SELECT title FROM todos WHERE category = (SELECT value FROM defaults);"));

    const auto text = output.str();
    CHECK(text.find("Write docs") != std::string::npos);
    CHECK(text.find("Buy milk") == std::string::npos);
    CHECK(text.find("1 row(s) selected") != std::string::npos);
}

TEST_CASE("Executor supports SELECT subqueries in INSERT values")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    std::ostringstream output;
    sql::Executor executor(storage, output);

    executor.execute(parse_statement("CREATE TABLE source (value);"));
    executor.execute(parse_statement("CREATE TABLE dest (value);"));
    executor.execute(parse_statement("INSERT INTO source VALUES ('copied');"));
    executor.execute(parse_statement("INSERT INTO dest VALUES ((SELECT value FROM source));"));

    const auto table = storage->load_table("dest");
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(table.rows[0][0], "copied");
}

TEST_CASE("Executor supports SELECT subqueries in UPDATE assignments")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    std::ostringstream output;
    sql::Executor executor(storage, output);

    executor.execute(parse_statement("CREATE TABLE todos (title, category);"));
    executor.execute(parse_statement("CREATE TABLE defaults (value);"));
    executor.execute(parse_statement("INSERT INTO todos VALUES ('Buy milk', 'home');"));
    executor.execute(parse_statement("INSERT INTO defaults VALUES ('done');"));
    executor.execute(parse_statement("UPDATE todos SET category = (SELECT value FROM defaults) WHERE title = 'Buy milk';"));

    const auto table = storage->load_table("todos");
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(table.rows[0][1], "done");
}

TEST_CASE("Executor supports SELECT subqueries in default expressions")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    std::ostringstream output;
    sql::Executor executor(storage, output);

    executor.execute(parse_statement("CREATE TABLE defaults (value);"));
    executor.execute(parse_statement("INSERT INTO defaults VALUES ('fallback');"));
    executor.execute(parse_statement("CREATE TABLE dest (value = (SELECT value FROM defaults));"));
    executor.execute(parse_statement("INSERT INTO dest VALUES ();"));

    const auto table = storage->load_table("dest");
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(table.rows[0][0], "fallback");
}

TEST_CASE("Executor rejects SELECT subqueries returning no rows")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    std::ostringstream output;
    sql::Executor executor(storage, output);

    executor.execute(parse_statement("CREATE TABLE source (value);"));
    executor.execute(parse_statement("CREATE TABLE dest (value);"));

    CHECK_THROWS_AS(executor.execute(parse_statement("INSERT INTO dest VALUES ((SELECT value FROM source));")), std::runtime_error);
}

TEST_CASE("Executor rejects SELECT subqueries returning multiple rows")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    std::ostringstream output;
    sql::Executor executor(storage, output);

    executor.execute(parse_statement("CREATE TABLE source (value);"));
    executor.execute(parse_statement("CREATE TABLE dest (value);"));
    executor.execute(parse_statement("INSERT INTO source VALUES ('a');"));
    executor.execute(parse_statement("INSERT INTO source VALUES ('b');"));

    CHECK_THROWS_AS(executor.execute(parse_statement("INSERT INTO dest VALUES ((SELECT value FROM source));")), std::runtime_error);
}

TEST_CASE("Executor rejects SELECT subqueries returning multiple columns")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    std::ostringstream output;
    sql::Executor executor(storage, output);

    executor.execute(parse_statement("CREATE TABLE source (value, label);"));
    executor.execute(parse_statement("CREATE TABLE dest (value);"));
    executor.execute(parse_statement("INSERT INTO source VALUES ('a', 'b');"));

    CHECK_THROWS_AS(executor.execute(parse_statement("INSERT INTO dest VALUES ((SELECT value, label FROM source));")), std::runtime_error);
}

TEST_CASE("Parser parses complex WHERE expressions")
{
    const auto statement = parse_statement("SELECT * FROM nums WHERE !(a + 1 < b) && ((a ^ b) > 0 || ~a < 0);");
    CHECK_EQ(static_cast<int>(statement.kind), static_cast<int>(sql::Statement::Kind::Select));
    CHECK(statement.select.where != nullptr);
}

TEST_CASE("Parser parses SELECT subquery as expression")
{
    const auto expression = parse_expression("(SELECT category FROM defaults WHERE id = 1)");

    REQUIRE(expression != nullptr);
    CHECK_EQ(static_cast<int>(expression->kind), static_cast<int>(sql::ExpressionKind::Select));
    REQUIRE(expression->select != nullptr);
    CHECK_EQ(expression->select->table_name, "defaults");
    REQUIRE_EQ(expression->select->columns.size(), 1U);
    CHECK_EQ(expression->select->columns[0], "category");
    CHECK(expression->select->where != nullptr);
}

TEST_CASE("Parser parses WHERE clause containing SELECT subquery")
{
    const auto statement = parse_statement("SELECT title FROM todos WHERE category = (SELECT value FROM defaults);");

    REQUIRE(statement.select.where != nullptr);
    CHECK_EQ(static_cast<int>(statement.select.where->kind), static_cast<int>(sql::ExpressionKind::Binary));
    REQUIRE(statement.select.where->right != nullptr);
    CHECK_EQ(static_cast<int>(statement.select.where->right->kind), static_cast<int>(sql::ExpressionKind::Select));
    REQUIRE(statement.select.where->right->select != nullptr);
    CHECK_EQ(statement.select.where->right->select->table_name, "defaults");
}

TEST_CASE("Parser parses column default expressions")
{
    const auto statement = parse_statement("CREATE TABLE dates (creation_date = NOW(), label = 'new');");
    REQUIRE_EQ(statement.create.columns.size(), 2U);
    REQUIRE(statement.create.columns[0].default_value != nullptr);
    CHECK_EQ(static_cast<int>(statement.create.columns[0].default_value->kind), static_cast<int>(sql::ExpressionKind::FunctionCall));
    CHECK_EQ(statement.create.columns[0].default_value->text, "NOW");
    REQUIRE(statement.create.columns[1].default_value != nullptr);
    CHECK_EQ(static_cast<int>(statement.create.columns[1].default_value->kind), static_cast<int>(sql::ExpressionKind::Literal));
    CHECK_EQ(statement.create.columns[1].default_value->text, "new");
}

TEST_CASE("Executor applies literal default values on insert")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    std::ostringstream output;
    sql::Executor executor(storage, output);

    executor.execute(parse_statement("CREATE TABLE dates (creation_date = 'today', label = 'new');"));
    executor.execute(parse_statement("INSERT INTO dates VALUES ();"));

    const auto table = storage->load_table("dates");
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(table.rows[0][0], "today");
    CHECK_EQ(table.rows[0][1], "new");
}

TEST_CASE("Executor applies NOW default values on insert")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    std::ostringstream output;
    sql::Executor executor(storage, output);

    executor.execute(parse_statement("CREATE TABLE dates (creation_date = NOW(), label = 'new');"));
    executor.execute(parse_statement("INSERT INTO dates (label) VALUES ('custom');"));

    const auto table = storage->load_table("dates");
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_FALSE(table.rows[0][0].empty());
    CHECK_EQ(table.rows[0][1], "custom");
}

TEST_CASE("Executor stores NOW default as expression and evaluates it on insert")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    std::ostringstream output;
    sql::Executor executor(storage, output);

    executor.execute(parse_statement("CREATE TABLE dates (creation_date = NOW(), label = 'new');"));
    const auto created_table = storage->load_table("dates");
    CHECK(created_table.columns[0].find("DEFAULT(NOW())") != std::string::npos);

    std::this_thread::sleep_for(std::chrono::seconds(1));
    executor.execute(parse_statement("INSERT INTO dates (label) VALUES ('custom');"));

    const auto inserted_table = storage->load_table("dates");
    REQUIRE_EQ(inserted_table.rows.size(), 1U);
    CHECK_FALSE(inserted_table.rows[0][0].empty());
    CHECK_NE(inserted_table.rows[0][0], "NOW()");
    CHECK_EQ(inserted_table.rows[0][1], "custom");
}

TEST_CASE("Parser parses delete statement with where")
{
    const auto statement = parse_statement("DELETE FROM todos WHERE done = true;");
    CHECK_EQ(static_cast<int>(statement.kind), static_cast<int>(sql::Statement::Kind::Delete));
    CHECK_EQ(statement.delete_statement.table_name, "todos");
    CHECK(statement.delete_statement.where != nullptr);
}

TEST_CASE("Executor deletes matching rows with where")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    std::ostringstream output;
    sql::Executor executor(storage, output);

    executor.execute(parse_statement("CREATE TABLE todos (title, done);"));
    executor.execute(parse_statement("INSERT INTO todos VALUES ('Buy milk', true);"));
    executor.execute(parse_statement("INSERT INTO todos VALUES ('Write docs', false);"));
    executor.execute(parse_statement("DELETE FROM todos WHERE done = true;"));

    const auto table = storage->load_table("todos");
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(table.rows[0][0], "Write docs");
    CHECK(output.str().find("Deleted 1 row(s) from 'todos'") != std::string::npos);
}

TEST_CASE("Executor deletes all rows without where")
{
    auto storage = std::make_shared<sql::MemoryStorage>();
    std::ostringstream output;
    sql::Executor executor(storage, output);

    executor.execute(parse_statement("CREATE TABLE todos (title, done);"));
    executor.execute(parse_statement("INSERT INTO todos VALUES ('Buy milk', true);"));
    executor.execute(parse_statement("INSERT INTO todos VALUES ('Write docs', false);"));
    executor.execute(parse_statement("DELETE FROM todos;"));

    const auto table = storage->load_table("todos");
    CHECK(table.rows.empty());
    CHECK(output.str().find("Deleted 2 row(s) from 'todos'") != std::string::npos);
}
