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

        void write_sql_help(std::ostream& output)
        {
            output
                << version() << '\n'
                << "Usage: sql [options] [sql pattern]\n"
                << "Options:\n"
                << "  --help -h    Show this help message\n\n"
                << "Patterns:\n"
                << "  SELECT * FROM table;\n"
                << "  INSERT INTO table (column1, column2) VALUES (value1, value2);\n"
                << "  UPDATE table SET column1 = value1 WHERE condition;\n"
                << "  DELETE FROM table WHERE condition;\n"
                << "  CREATE TABLE table_name (column1 datatype, column2 datatype, ...);\n"
                << "  DROP TABLE table_name;\n";
        }

        void write_select_help(std::ostream& output)
        {
            output
                << version() << '\n'
                << "Usage: select [options] [sql pattern]\n"
                << "Options:\n"
                << "  --help -h    Show this help message\n\n"
                << "Patterns:\n"
                << "  SELECT * FROM table;\n"
                << "  SELECT column1, column2 FROM table WHERE condition;\n"
                << "  SELECT column1, column2 FROM table ORDER BY column1;\n"
                << "  SELECT column1, column2 FROM table GROUP BY column1;\n";
        }

        void write_insert_help(std::ostream& output)
        {
            output
                << version() << '\n'
                << "Usage: insert [options] [sql pattern]\n"
                << "Options:\n"
                << "  --help -h    Show this help message\n\n"
                << "Patterns:\n"
                << "  INSERT INTO table (column1, column2) VALUES (value1, value2);\n"
                << "  INSERT INTO table (column1, column2) SELECT column1, column2 FROM another_table;\n";
        }

        void write_update_help(std::ostream& output)
        {
            output
                << version() << '\n'
                << "Usage: update [options] [sql pattern]\n"
                << "Options:\n"
                << "  --help -h    Show this help message\n\n"
                << "Patterns:\n"
                << "  UPDATE table SET column1 = value1 WHERE condition;\n"
                << "  UPDATE table SET column1 = value1, column2 = value2 WHERE condition;\n";
        }

        void write_delete_help(std::ostream& output)
        {
            output
                << version() << '\n'
                << "Usage: delete [options] [sql pattern]\n"
                << "Options:\n"
                << "  --help -h    Show this help message\n\n"
                << "Patterns:\n"
                << "  DELETE FROM table WHERE condition;\n";
        }

        void write_create_help(std::ostream& output)
        {
            output
                << version() << '\n'
                << "Usage: create-table [options] [sql pattern]\n"
                << "Options:\n"
                << "  --help -h    Show this help message\n\n"
                << "Patterns:\n"
                << "  CREATE TABLE table_name (column1 datatype, column2 datatype, ...);\n";
        }

        void write_drop_help(std::ostream& output)
        {
            output
                << version() << '\n'
                << "Usage: drop-table [options] [sql pattern]\n"
                << "Options:\n"
                << "  --help -h    Show this help message\n\n"
                << "Patterns:\n"
                << "  DROP TABLE table_name;\n";
        }

        void write_program_index(std::ostream& output)
        {
            output
                << version() << '\n'
                << "Usage: [program] [options]\n"
                << "Programs:\n"
                << "  sql           Main SQL application\n"
                << "  select        Help for SELECT statements\n"
                << "  insert        Help for INSERT statements\n"
                << "  update        Help for UPDATE statements\n"
                << "  delete        Help for DELETE statements\n"
                << "  create-table  Help for CREATE TABLE statements\n"
                << "  drop-table    Help for DROP TABLE statements\n\n"
                << "Options:\n"
                << "  --help -h     Show this help message\n";
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
        else if (program_name == "create-table")
        {
            write_create_help(output);
        }
        else if (program_name == "drop-table")
        {
            write_drop_help(output);
        }
        else
        {
            write_program_index(output);
        }
    }
}

