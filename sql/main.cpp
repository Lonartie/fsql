#include "Application.h"

#include <iostream>

std::string version() { return "sql version 1.0.0"; }

void sqlHelp() {
  std::cout
      << version() << "\n"
      << "Usage: sql [options] [sql pattern]\n"
      << "Options:\n"
      << "  --help -h    Show this help message\n"
      << "\n"
      << "Patterns:\n"
      << "  SELECT * FROM table;\n"
      << "  INSERT INTO table (column1, column2) VALUES (value1, value2);\n"
      << "  UPDATE table SET column1 = value1 WHERE condition;\n"
      << "  DELETE FROM table WHERE condition;\n"
      << "  CREATE TABLE table_name (column1 datatype, column2 datatype, "
         "...);\n"
      << "  DROP TABLE table_name;\n";
}

void selectHelp() {
  std::cout << version() << "\n"
            << "Usage: select [options] [sql pattern]\n"
            << "Options:\n"
            << "  --help -h    Show this help message\n"
            << "\n"
            << "Patterns:\n"
            << "  SELECT * FROM table;\n"
            << "  SELECT column1, column2 FROM table WHERE condition;\n"
            << "  SELECT column1, column2 FROM table ORDER BY column1;\n"
            << "  SELECT column1, column2 FROM table GROUP BY column1;\n";
}

void insertHelp() {
  std::cout
      << version() << "\n"
      << "Usage: insert [options] [sql pattern]\n"
      << "Options:\n"
      << "  --help -h    Show this help message\n"
      << "\n"
      << "Patterns:\n"
      << "  INSERT INTO table (column1, column2) VALUES (value1, value2);\n"
      << "  INSERT INTO table (column1, column2) SELECT column1, column2 "
         "FROM another_table;\n";
}

void updateHelp() {
  std::cout << version() << "\n"
            << "Usage: update [options] [sql pattern]\n"
            << "Options:\n"
            << "  --help -h    Show this help message\n"
            << "\n"
            << "Patterns:\n"
            << "  UPDATE table SET column1 = value1 WHERE condition;\n"
            << "  UPDATE table SET column1 = value1, column2 = value2 WHERE "
               "condition;\n";
}

void deleteHelp() {
  std::cout << version() << "\n"
            << "Usage: delete [options] [sql pattern]\n"
            << "Options:\n"
            << "  --help -h    Show this help message\n"
            << "\n"
            << "Patterns:\n"
            << "  DELETE FROM table WHERE condition;\n";
}

void createTableHelp() {
  std::cout << version() << "\n"
            << "Usage: create-table [options] [sql pattern]\n"
            << "Options:\n"
            << "  --help -h    Show this help message\n"
            << "\n"
            << "Patterns:\n"
            << "  CREATE TABLE table_name (column1 datatype, column2 datatype, "
               "...);\n";
}

void dropTableHelp() {
  std::cout << version() << "\n"
            << "Usage: drop-table [options] [sql pattern]\n"
            << "Options:\n"
            << "  --help -h    Show this help message\n"
            << "\n"
            << "Patterns:\n"
            << "  DROP TABLE table_name;\n";
}

void help(std::string programName) {
  if (programName == "sql") {
    sqlHelp();
  } else if (programName == "select") {
    selectHelp();
  } else if (programName == "insert") {
    insertHelp();
  } else if (programName == "update") {
    updateHelp();
  } else if (programName == "delete") {
    deleteHelp();
  } else if (programName == "create-table") {
    createTableHelp();
  } else if (programName == "drop-table") {
    dropTableHelp();
  } else {
    std::cout << version() << "\n"
              << "Usage: [program] [options]\n"
              << "Programs:\n"
              << "  sql           Main SQL application\n"
              << "  select        Help for SELECT statements\n"
              << "  insert        Help for INSERT statements\n"
              << "  update        Help for UPDATE statements\n"
              << "  delete        Help for DELETE statements\n"
              << "  create-table  Help for CREATE TABLE statements\n"
              << "  drop-table    Help for DROP TABLE statements\n"
              << "\n"
              << "Options:\n"
              << "  --help -h     Show this help message\n";
  }
}

int main(int argc, char **argv) {
  bool showHelp = argc <= 1 || (std::string(argv[1]) == "--help" ||
                                std::string(argv[1]) == "-h");
  if (showHelp) {
    std::string programName = argc > 0 ? argv[0] : "sql";
    help(programName);
    return 0;
  }

  sql::Application application;
  return application.run(argc, argv);
}