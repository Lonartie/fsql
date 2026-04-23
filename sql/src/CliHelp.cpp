#include "CliHelp.h"

#include <ostream>
#include <string>

namespace sql
{
    namespace
    {
        std::string version()
        {
            return "sql version 1.0.0";
        }

        void write_common_options(std::ostream& output)
        {
            output
                << "Options:\n"
                << "  --help -h    Show this help message\n\n";
        }

        void write_sql_syntax_overview(std::ostream& output)
        {
            output
                << "Syntax overview:\n"
                << "  Statements generally look like: KEYWORD ...;\n"
                << "  String literals use single quotes: 'text'\n"
                << "  File path sources in SELECT use quoted paths: SELECT * FROM '/path/to/file';\n"
                << "  Qualified columns use source.column: tasks.title\n"
                << "  Boolean-style expressions are used in WHERE/HAVING: done = false, score >= 10\n"
                << "  Subqueries are written in parentheses: WHERE id IN (SELECT team_id FROM tasks)\n"
                << "  GROUP BY / HAVING / ORDER BY / LIMIT / OFFSET follow the SELECT body in that order\n\n";
        }

        void write_select_syntax(std::ostream& output)
        {
            output
                << "SELECT syntax:\n"
                << "  SELECT * FROM source;\n"
                << "  SELECT expr1, expr2 FROM source1[, source2, ...]\n"
                << "    [WHERE condition]\n"
                << "    [GROUP BY expr1[, expr2, ...]]\n"
                << "    [HAVING condition]\n"
                << "    [ORDER BY expr [ASC|DESC][, ...]]\n"
                << "    [LIMIT n]\n"
                << "    [OFFSET n];\n\n";
        }

        void write_insert_syntax(std::ostream& output)
        {
            output
                << "INSERT syntax:\n"
                << "  INSERT INTO table VALUES (value1, value2, ...);\n"
                << "  INSERT INTO table (column1, column2) VALUES (value1, value2);\n"
                << "  Missing columns use their configured defaults when available.\n\n";
        }

        void write_update_syntax(std::ostream& output)
        {
            output
                << "UPDATE syntax:\n"
                << "  UPDATE table SET column1 = expr1[, column2 = expr2, ...] [WHERE condition];\n\n";
        }

        void write_delete_syntax(std::ostream& output)
        {
            output
                << "DELETE syntax:\n"
                << "  DELETE FROM table [WHERE condition];\n\n";
        }

        void write_create_syntax(std::ostream& output)
        {
            output
                << "CREATE syntax:\n"
                << "  CREATE TABLE table_name (column1, column2, ...);\n"
                << "  CREATE TABLE table_name (id AUTO_INCREMENT, title, done = false);\n"
                << "  CREATE VIEW view_name AS SELECT ...;\n\n";
        }

        void write_drop_syntax(std::ostream& output)
        {
            output
                << "DROP syntax:\n"
                << "  DROP TABLE table_name;\n"
                << "  DROP VIEW view_name;\n\n";
        }

        void write_sql_help(std::ostream& output)
        {
            output
                << version() << '\n'
                << "Usage: sql [options] [SQL statement]\n\n";
            write_common_options(output);
            write_sql_syntax_overview(output);
            output
                << "Examples:\n"
                << "  sql \"SELECT * FROM todos;\"\n"
                << "  sql \"SELECT title FROM todos WHERE done = false ORDER BY title;\"\n"
                << "  sql \"SELECT team, COUNT(*) FROM tasks GROUP BY team HAVING COUNT(*) >= 2;\"\n"
                << "  sql \"SELECT src.title FROM '/tmp/tasks.csv' src WHERE src.done = false;\"\n"
                << "  sql \"INSERT INTO todos (title, done) VALUES ('Buy milk', false);\"\n"
                << "  sql \"UPDATE todos SET done = true WHERE title = 'Buy milk';\"\n"
                << "  sql \"DELETE FROM todos WHERE done = true;\"\n"
                << "  sql \"CREATE TABLE todos (id AUTO_INCREMENT, title, done = false);\"\n"
                << "  sql \"CREATE VIEW open_todos AS SELECT title FROM todos WHERE done = false;\"\n"
                << "  sql \"DROP VIEW open_todos;\"\n";
        }

        void write_select_help(std::ostream& output)
        {
            output
                << version() << '\n'
                << "Usage: select [options] [SELECT body]\n\n";
            write_common_options(output);
            write_select_syntax(output);
            output
                << "Examples:\n"
                << "  select \"* FROM todos;\"\n"
                << "  select \"title, done FROM todos WHERE done = false ORDER BY title ASC;\"\n"
                << "  select \"team, COUNT(*) FROM tasks GROUP BY team HAVING COUNT(*) > 1;\"\n"
                << "  select \"tasks.title, teams.name FROM tasks, teams WHERE tasks.team_id = teams.id;\"\n"
                << "  select \"src.title FROM '/tmp/tasks.csv' src WHERE src.done = false LIMIT 10 OFFSET 5;\"\n";
        }

        void write_insert_help(std::ostream& output)
        {
            output
                << version() << '\n'
                << "Usage: insert [options] [INSERT body]\n\n";
            write_common_options(output);
            write_insert_syntax(output);
            output
                << "Examples:\n"
                << "  insert \"INTO todos VALUES ('Buy milk', false);\"\n"
                << "  insert \"INTO todos (title, done) VALUES ('Write docs', true);\"\n"
                << "  insert \"INTO todos (title) VALUES ('Uses default done value');\"\n"
                << "  insert \"INTO events VALUES (NOW(), 'created');\"\n";
        }

        void write_update_help(std::ostream& output)
        {
            output
                << version() << '\n'
                << "Usage: update [options] [UPDATE body]\n\n";
            write_common_options(output);
            write_update_syntax(output);
            output
                << "Examples:\n"
                << "  update \"todos SET done = true WHERE title = 'Buy milk';\"\n"
                << "  update \"todos SET done = true, category = 'done' WHERE done = false;\"\n"
                << "  update \"tasks SET priority = priority + 1 WHERE team = 'ops';\"\n";
        }

        void write_delete_help(std::ostream& output)
        {
            output
                << version() << '\n'
                << "Usage: delete [options] [DELETE body]\n\n";
            write_common_options(output);
            write_delete_syntax(output);
            output
                << "Examples:\n"
                << "  delete \"FROM todos WHERE done = true;\"\n"
                << "  delete \"FROM logs WHERE created_at < '2026-01-01';\"\n"
                << "  delete \"FROM tasks;\"\n";
        }

        void write_create_help(std::ostream& output)
        {
            output
                << version() << '\n'
                << "Usage: create [options] [CREATE body]\n\n";
            write_common_options(output);
            write_create_syntax(output);
            output
                << "Examples:\n"
                << "  create \"TABLE todos (title, done = false);\"\n"
                << "  create \"TABLE tasks (id AUTO_INCREMENT, title, team, created_at = NOW());\"\n"
                << "  create \"VIEW open_tasks AS SELECT title FROM tasks WHERE done = false;\"\n";
        }

        void write_drop_help(std::ostream& output)
        {
            output
                << version() << '\n'
                << "Usage: drop [options] [DROP body]\n\n";
            write_common_options(output);
            write_drop_syntax(output);
            output
                << "Examples:\n"
                << "  drop \"TABLE todos;\"\n"
                << "  drop \"VIEW open_tasks;\"\n";
        }

        void write_program_index(std::ostream& output)
        {
            output
                << version() << '\n'
                << "Usage: [program] [options]\n"
                << "Programs:\n"
                << "  sql           Main SQL application\n"
                << "  select        Run or inspect SELECT statements\n"
                << "  insert        Run or inspect INSERT statements\n"
                << "  update        Run or inspect UPDATE statements\n"
                << "  delete        Run or inspect DELETE statements\n"
                << "  create        Run or inspect CREATE TABLE / CREATE VIEW statements\n"
                << "  drop          Run or inspect DROP TABLE / DROP VIEW statements\n\n";
            write_common_options(output);
            output
                << "Tip:\n"
                << "  Wrapper commands prepend their keyword automatically.\n"
                << "  Example: select \"title FROM todos WHERE done = false;\"\n";
        }
    }

    bool CliHelp::should_show(int argc, char** argv)
    {
        return argc <= 1 || (std::string(argv[1]) == "--help" || std::string(argv[1]) == "-h");
    }

    void CliHelp::write(std::ostream& output, std::string_view program_name)
    {
        if (program_name == "sql")
        {
            write_sql_help(output);
        }
        else if (program_name == "select")
        {
            write_select_help(output);
        }
        else if (program_name == "insert")
        {
            write_insert_help(output);
        }
        else if (program_name == "update")
        {
            write_update_help(output);
        }
        else if (program_name == "delete")
        {
            write_delete_help(output);
        }
        else if (program_name == "create" || program_name == "create-table")
        {
            write_create_help(output);
        }
        else if (program_name == "drop" || program_name == "drop-table")
        {
            write_drop_help(output);
        }
        else
        {
            write_program_index(output);
        }
    }
}

