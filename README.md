# csv_sql

`csv_sql` is a small C++ SQL-like engine that stores tables as CSV files.
It can parse and execute a focused subset of SQL statements, making it useful as a learning project, a lightweight toy database, or a compact playground for parser / executor work.

The project is built with CMake, uses a reusable core library, provides a command-line application, and includes an automated test suite.

> This is **not** a full SQLite replacement. It implements a compact SQL dialect on top of CSV-backed storage.

## What the project does

At a high level, `csv_sql`:

- tokenizes and parses SQL-like input
- executes statements against in-memory or CSV-backed storage
- persists tables as `.csv` files
- prints query results in a readable table format
- includes tests for parsing, execution, CSV handling, defaults, expressions, aggregates, and more complex workflows

## Current feature set

The implementation currently supports a focused subset of SQL functionality, including:

- `CREATE TABLE`
- `DROP TABLE`
- `INSERT INTO ... VALUES (...)`
- `SELECT`
- `UPDATE`
- `DELETE`

Additional supported behavior includes:

- column default expressions
- `AUTO_INCREMENT`
- arithmetic, logical, comparison, and bitwise expressions
- scalar `SELECT` sub-expressions
- aggregate functions such as:
  - `COUNT`
  - `SUM`
  - `AVG`
  - `MIN`
  - `MAX`
- single-table `SELECT` result shaping:
  - `DISTINCT` / `UNIQUE`
  - `ORDER BY`
  - `LIMIT`
  - `OFFSET`

For planned and completed work, see [`progress.md`](./progress.md).

## How storage works

The command-line application uses CSV-backed storage rooted at the **current working directory**.
That means a table such as:

```sql
CREATE TABLE todos (title, done);
```

will be stored as a CSV file like:

```text
todos.csv
```

in the directory where you run the executable.

## Build requirements

Known from the project files:

- CMake `3.12` or newer
- a C++20-capable compiler

The project is organized as three CMake subprojects:

- `core/` - parser, tokenizer, executor, storage
- `sql/` - command-line application
- `tests/` - doctest-based test suite

## Build

From the repository root:

```sh
cmake -S . -B build
cmake --build build
```

This builds:

- the core library
- the main CLI executable
- command wrappers such as `select`, `insert`, `update`, `delete`, `create`, and `drop`
- the `sql_tests` test binary

## Run

The main executable is built from the `sql/` subproject.
Depending on your generator and platform, the binary will typically be located under the build tree, for example:

```sh
./build/sql/sql "CREATE TABLE todos (title, category, done);"
./build/sql/sql "INSERT INTO todos VALUES ('Buy milk', 'home', false);"
./build/sql/sql "SELECT * FROM todos ORDER BY title;"
```

You can also pipe a statement into the program:

```sh
echo "SELECT * FROM todos;" | ./build/sql/sql
```

### Wrapper commands

The project also builds small wrapper executables for common statement types. These prepend the statement keyword automatically.
Examples:

```sh
./build/sql/create "TABLE todos (title, done);"
./build/sql/insert "INTO todos VALUES ('Ship release', true);"
./build/sql/select "* FROM todos;"
```

## Example session

```sh
./build/sql/sql "CREATE TABLE tasks (id AUTO_INCREMENT, title, category = 'general', done = false);"
./build/sql/sql "INSERT INTO tasks (title) VALUES ('Write README');"
./build/sql/sql "INSERT INTO tasks (title, category) VALUES ('Fix parser bug', 'dev');"
./build/sql/sql "SELECT title, category, done FROM tasks ORDER BY category, title;"
```

## Run the tests

The repository already enables testing via CMake.
After building, run:

```sh
ctest --test-dir build --output-on-failure
```

You can also run the test executable directly if you want more control:

```sh
./build/tests/sql_tests
```

## Project layout

```text
core/   Core SQL engine: tokenizer, parser, executor, storage
sql/    Command-line application
tests/  Automated tests
```

More specifically:

- `core/include/` contains public headers
- `core/src/` contains the engine implementation
- `sql/src/Application.cpp` wires command-line input to the parser and executor
- `tests/` contains method-grouped test suites

## Development notes

This project is best understood as a compact SQL engine with a deliberately limited scope.
It is a good base for experimenting with:

- parsing and AST design
- query execution
- CSV-backed persistence
- incremental language feature development
- test-driven implementation of SQL features

If you want to see what is done and what is planned next, check [`progress.md`](./progress.md).

