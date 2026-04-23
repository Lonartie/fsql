#include "Parser.h"

#include "SqlError.h"
#include "StringUtils.h"

namespace fsql
{
    namespace
    {
        bool is_select_source_terminator(const Token& token)
        {
            return token.type == TokenType::Comma
                || token.type == TokenType::Semicolon
                || token.type == TokenType::End
                || token.type == TokenType::RParen
                || iequals(token.text, "WHERE")
                || iequals(token.text, "GROUP")
                || iequals(token.text, "HAVING")
                || iequals(token.text, "ORDER")
                || iequals(token.text, "LIMIT")
                || iequals(token.text, "OFFSET");
        }
    }

    Statement Parser::parse_alter()
    {
        AlterStatement stmt;
        if (match_keyword("TABLE"))
        {
            stmt.object_kind = SchemaObjectKind::Table;
            stmt.table_name = expect_relation_reference("Expected table name");

            if (match_keyword("ADD"))
            {
                expect_keyword("COLUMN");
                stmt.action = AlterAction::AddColumn;
                stmt.column = parse_column_definition();
            }
            else if (match_keyword("DROP"))
            {
                expect_keyword("COLUMN");
                stmt.action = AlterAction::DropColumn;
                stmt.column_name = expect_identifier("Expected column name");
            }
            else if (match_keyword("RENAME"))
            {
                expect_keyword("COLUMN");
                stmt.action = AlterAction::RenameColumn;
                stmt.column_name = expect_identifier("Expected column name");
                expect_keyword("TO");
                stmt.new_name = expect_identifier("Expected new column name");
            }
            else if (match_keyword("ALTER"))
            {
                expect_keyword("COLUMN");
                stmt.column_name = expect_identifier("Expected column name");
                if (match_keyword("SET"))
                {
                    if (match_keyword("DEFAULT"))
                    {
                        stmt.action = AlterAction::SetDefault;
                        stmt.column.default_value = parse_expression();
                    }
                    else if (match_keyword("AUTO_INCREMENT"))
                    {
                        stmt.action = AlterAction::SetAutoIncrement;
                    }
                    else
                    {
                        fail("Expected DEFAULT or AUTO_INCREMENT after ALTER COLUMN ... SET");
                    }
                }
                else if (match_keyword("DROP"))
                {
                    if (match_keyword("DEFAULT"))
                    {
                        stmt.action = AlterAction::DropDefault;
                    }
                    else if (match_keyword("AUTO_INCREMENT"))
                    {
                        stmt.action = AlterAction::DropAutoIncrement;
                    }
                    else
                    {
                        fail("Expected DEFAULT or AUTO_INCREMENT after ALTER COLUMN ... DROP");
                    }
                }
                else
                {
                    fail("Expected SET or DROP after ALTER COLUMN");
                }
            }
            else
            {
                fail("Expected ADD COLUMN, DROP COLUMN, RENAME COLUMN, or ALTER COLUMN");
            }

            consume_optional(TokenType::Semicolon);
            expect(TokenType::End, "Unexpected tokens after ALTER TABLE");
        }
        else if (match_keyword("VIEW"))
        {
            stmt.object_kind = SchemaObjectKind::View;
            stmt.table_name = expect_relation_reference("Expected view name");
            expect_keyword("AS");
            if (!match_keyword("SELECT"))
            {
                fail("Expected SELECT after ALTER VIEW ... AS");
            }
            stmt.action = AlterAction::SetViewQuery;
            stmt.view_query = std::make_shared<SelectStatement>(parse_select_statement());

            consume_optional(TokenType::Semicolon);
            expect(TokenType::End, "Unexpected tokens after ALTER VIEW");
        }
        else
        {
            fail("Expected TABLE or VIEW after ALTER");
        }

        Statement statement;
        statement.kind = Statement::Kind::Alter;
        statement.alter = std::move(stmt);
        return statement;
    }

    Statement Parser::parse_create()
    {
        CreateStatement stmt;
        if (match_keyword("TABLE"))
        {
            stmt.object_kind = SchemaObjectKind::Table;
            stmt.table_name = expect_relation_reference("Expected table name");
            expect(TokenType::LParen, "Expected '(' after table name");
            stmt.columns = parse_column_definition_list();
            expect(TokenType::RParen, "Expected ')' after column list");
            consume_optional(TokenType::Semicolon);
            expect(TokenType::End, "Unexpected tokens after CREATE TABLE");
        }
        else if (match_keyword("VIEW"))
        {
            stmt.object_kind = SchemaObjectKind::View;
            stmt.table_name = expect_relation_reference("Expected view name");
            expect_keyword("AS");
            if (!match_keyword("SELECT"))
            {
                fail("Expected SELECT after CREATE VIEW ... AS");
            }
            stmt.view_query = std::make_shared<SelectStatement>(parse_select_statement());
            consume_optional(TokenType::Semicolon);
            expect(TokenType::End, "Unexpected tokens after CREATE VIEW");
        }
        else
        {
            fail("Expected TABLE or VIEW after CREATE");
        }

        Statement statement;
        statement.kind = Statement::Kind::Create;
        statement.create = std::move(stmt);
        return statement;
    }

    Statement Parser::parse_drop()
    {
        DropStatement stmt;
        if (match_keyword("TABLE"))
        {
            stmt.object_kind = SchemaObjectKind::Table;
            stmt.table_name = expect_relation_reference("Expected table name");
            consume_optional(TokenType::Semicolon);
            expect(TokenType::End, "Unexpected tokens after DROP TABLE");
        }
        else if (match_keyword("VIEW"))
        {
            stmt.object_kind = SchemaObjectKind::View;
            stmt.table_name = expect_relation_reference("Expected view name");
            consume_optional(TokenType::Semicolon);
            expect(TokenType::End, "Unexpected tokens after DROP VIEW");
        }
        else
        {
            fail("Expected TABLE or VIEW after DROP");
        }

        Statement statement;
        statement.kind = Statement::Kind::Drop;
        statement.drop = std::move(stmt);
        return statement;
    }

    Statement Parser::parse_delete()
    {
        DeleteStatement stmt;
        expect_keyword("FROM");
        stmt.table_name = expect_relation_reference("Expected table name");
        if (match_keyword("WHERE"))
        {
            stmt.where = parse_expression();
        }
        consume_optional(TokenType::Semicolon);
        expect(TokenType::End, "Unexpected tokens after DELETE");

        Statement statement;
        statement.kind = Statement::Kind::Delete;
        statement.delete_statement = std::move(stmt);
        return statement;
    }

    Statement Parser::parse_insert()
    {
        InsertStatement stmt;
        expect_keyword("INTO");
        stmt.table_name = expect_relation_reference("Expected table name");
        if (check(TokenType::LParen))
        {
            advance();
            stmt.columns = parse_identifier_list();
            expect(TokenType::RParen, "Expected ')' after column list");
        }
        expect_keyword("VALUES");
        expect(TokenType::LParen, "Expected '(' before VALUES list");
        if (!check(TokenType::RParen))
        {
            stmt.values = parse_value_list();
        }
        expect(TokenType::RParen, "Expected ')' after VALUES list");
        consume_optional(TokenType::Semicolon);
        expect(TokenType::End, "Unexpected tokens after INSERT");

        Statement statement;
        statement.kind = Statement::Kind::Insert;
        statement.insert = std::move(stmt);
        return statement;
    }

    Statement Parser::parse_select()
    {
        Statement statement;
        statement.kind = Statement::Kind::Select;
        statement.select = parse_select_statement();
        consume_optional(TokenType::Semicolon);
        expect(TokenType::End, "Unexpected tokens after SELECT");
        return statement;
    }

    SelectStatement Parser::parse_select_statement()
    {
        SelectStatement stmt;
        stmt.distinct = match_keyword("DISTINCT") || match_keyword("UNIQUE");

        if (consume_optional(TokenType::Star))
        {
            stmt.select_all = true;
        }
        else
        {
            stmt.projections = parse_select_projection_list();
        }

        expect_keyword("FROM");
        stmt.sources = parse_select_source_list();
        if (match_keyword("WHERE"))
        {
            stmt.where = parse_expression();
        }
        if (match_keyword("GROUP"))
        {
            expect_keyword("BY");
            stmt.group_by = parse_expression_list();
        }
        if (match_keyword("HAVING"))
        {
            stmt.having = parse_expression();
        }
        if (match_keyword("ORDER"))
        {
            expect_keyword("BY");
            stmt.order_by = parse_order_by_list();
        }
        if (match_keyword("LIMIT"))
        {
            stmt.limit = parse_non_negative_integer("Expected non-negative LIMIT value");
        }
        if (match_keyword("OFFSET"))
        {
            stmt.offset = parse_non_negative_integer("Expected non-negative OFFSET value");
        }
        return stmt;
    }

    Statement Parser::parse_update()
    {
        UpdateStatement stmt;
        stmt.table_name = expect_relation_reference("Expected table name");
        expect_keyword("SET");
        do
        {
            const std::string column = expect_identifier("Expected column name in assignment");
            expect(TokenType::Equal, "Expected '=' in assignment");
            stmt.assignments.emplace_back(column, parse_expression());
        }
        while (consume_optional(TokenType::Comma));

        if (match_keyword("WHERE"))
        {
            stmt.where = parse_expression();
        }
        consume_optional(TokenType::Semicolon);
        expect(TokenType::End, "Unexpected tokens after UPDATE");

        Statement statement;
        statement.kind = Statement::Kind::Update;
        statement.update = std::move(stmt);
        return statement;
    }

    std::vector<std::string> Parser::parse_identifier_list()
    {
        std::vector<std::string> values;
        values.push_back(expect_identifier("Expected identifier"));
        while (consume_optional(TokenType::Comma))
        {
            values.push_back(expect_identifier("Expected identifier after ','"));
        }
        return values;
    }

    std::vector<SelectSource> Parser::parse_select_source_list()
    {
        std::vector<SelectSource> sources;
        sources.push_back(parse_select_source());
        while (consume_optional(TokenType::Comma))
        {
            sources.push_back(parse_select_source());
        }
        return sources;
    }

    SelectSource Parser::parse_select_source()
    {
        SelectSource source;
        if (consume_optional(TokenType::LParen))
        {
            source.kind = SelectSource::Kind::Subquery;
            if (!match_keyword("SELECT"))
            {
                fail("Expected SELECT after '(' in FROM source");
            }
            source.subquery = std::make_shared<SelectStatement>(parse_select_statement());
            expect(TokenType::RParen, "Expected ')' after FROM subquery");
            if (match_keyword("AS"))
            {
                source.alias = expect_identifier("Expected alias after AS");
            }
            else if (check(TokenType::Identifier) && !is_select_source_terminator(peek()))
            {
                source.alias = advance().text;
            }
            return source;
        }

        const auto relation = expect_relation_reference("Expected table name");
        source.kind = relation.kind == RelationReference::Kind::FilePath
            ? SelectSource::Kind::FilePath
            : SelectSource::Kind::Table;
        source.name = relation.name;
        if (match_keyword("AS"))
        {
            source.alias = expect_identifier("Expected alias after AS");
        }
        else if (check(TokenType::Identifier) && !is_select_source_terminator(peek()))
        {
            source.alias = advance().text;
        }
        return source;
    }

    std::vector<ColumnDefinition> Parser::parse_column_definition_list()
    {
        std::vector<ColumnDefinition> columns;
        columns.push_back(parse_column_definition());
        while (consume_optional(TokenType::Comma))
        {
            columns.push_back(parse_column_definition());
        }
        return columns;
    }

    ColumnDefinition Parser::parse_column_definition()
    {
        ColumnDefinition column;
        column.name = expect_identifier("Expected column name");
        if (match_keyword("AUTO_INCREMENT"))
        {
            column.auto_increment = true;
        }
        if (consume_optional(TokenType::Equal))
        {
            column.default_value = parse_expression();
        }
        return column;
    }

    std::vector<ExpressionPtr> Parser::parse_value_list()
    {
        return parse_expression_list();
    }

    std::vector<ExpressionPtr> Parser::parse_expression_list()
    {
        std::vector<ExpressionPtr> expressions;
        expressions.push_back(parse_expression());
        while (consume_optional(TokenType::Comma))
        {
            expressions.push_back(parse_expression());
        }
        return expressions;
    }

    SelectProjection Parser::parse_select_projection()
    {
        SelectProjection projection;
        projection.expression = parse_expression();
        if (match_keyword("AS"))
        {
            if (check(TokenType::Identifier) || check(TokenType::String))
            {
                projection.alias = advance().text;
            }
            else
            {
                fail("Expected alias after AS");
            }
        }
        return projection;
    }

    std::vector<SelectProjection> Parser::parse_select_projection_list()
    {
        std::vector<SelectProjection> projections;
        projections.push_back(parse_select_projection());
        while (consume_optional(TokenType::Comma))
        {
            projections.push_back(parse_select_projection());
        }
        return projections;
    }

    std::vector<SelectOrderBy> Parser::parse_order_by_list()
    {
        std::vector<SelectOrderBy> order_by;
        do
        {
            SelectOrderBy term;
            term.expression = parse_expression();
            if (match_keyword("DESC"))
            {
                term.descending = true;
            }
            else
            {
                match_keyword("ASC");
            }
            order_by.push_back(std::move(term));
        }
        while (consume_optional(TokenType::Comma));
        return order_by;
    }
}


