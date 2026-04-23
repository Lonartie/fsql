# fsql

> A compact, AI-built C++ SQL-like engine for file-backed tabular data, designed to work both as a standalone CLI tool and as an embeddable library.

`fsql` is a small but ambitious C++20 project that parses and executes a focused SQL-like dialect over file-backed and in-memory tables. CSV remains the default on-disk format, and JSON, TOML, YAML, and XML table files are also supported. It exposes a reusable core library (`fsqllib`), a command-line application (`fsql`), command wrappers such as `fselect` and `finsert`, and an automated doctest suite.

It is intentionally opinionated: it aims to be understandable and embeddable rather than fully SQL-compatible.

---

## Table of contents

1. [What is fsql?](#1-what-is-fsql)
2. [What is it not?](#2-what-is-it-not)
3. [How to build fsql?](#3-how-to-build-fsql)
4. [How to use it as a CLI tool?](#4-how-to-use-it-as-a-cli-tool)
5. [How to use it as a library?](#5-how-to-use-it-as-a-library)
6. [SQL-like syntax explained](#6-sql-like-syntax-explained)
   - [6.1 Statements overview](#61-statements-overview)
   - [6.2 Literals, identifiers, and values](#62-literals-identifiers-and-values)
   - [6.3 SELECT sources](#63-select-sources)
   - [6.4 Expressions and operators](#64-expressions-and-operators)
   - [6.5 Grouping and aggregation](#65-grouping-and-aggregation)
   - [6.6 Subqueries](#66-subqueries)
   - [6.7 Views](#67-views)
   - [6.8 ALTER TABLE and schema metadata](#68-alter-table-and-schema-metadata)
   - [6.9 NULL, defaults, and AUTO_INCREMENT](#69-null-defaults-and-auto_increment)
   - [6.10 Dialect differences and current limitations](#610-dialect-differences-and-current-limitations)
7. [Execution model explained](#7-execution-model-explained)
   - [7.1 High-level pipeline](#71-high-level-pipeline)
   - [7.2 Storage model](#72-storage-model)
   - [7.3 Structured results instead of direct printing](#73-structured-results-instead-of-direct-printing)
   - [7.4 Streaming result model](#74-streaming-result-model)
   - [7.5 Coroutine executors](#75-coroutine-executors)
   - [7.6 Where buffering still happens](#76-where-buffering-still-happens)
   - [7.7 Error handling model](#77-error-handling-model)
8. [Project layout](#8-project-layout)
9. [Running the tests](#9-running-the-tests)
10. [Roadmap and further reading](#10-roadmap-and-further-reading)

---

## 1. What is fsql?

`fsql` is a **SQL-like execution engine** with two main use cases:

- **Standalone CLI tool** for querying and mutating file-backed data from the terminal
- **Reusable C++ library** for applications that want parsing, execution, storage abstraction, streaming results, and output formatting without owning all of that logic themselves

At a high level, `fsql` can:

- tokenize SQL-like input into tokens
- parse tokens into an AST
- execute statements against an abstract storage layer
- use either **file-backed storage** or **in-memory storage**
- stream rows with coroutine-backed generators
- return **structured execution results** instead of printing directly
- optionally render those results through a core-owned output writer

### Supported capabilities today

The currently implemented feature set includes:

- Schema operations:
  - `CREATE TABLE`
  - `DROP TABLE`
  - `ALTER TABLE`
  - `CREATE VIEW`
  - `DROP VIEW`
  - `ALTER VIEW`
- Data operations:
  - `INSERT INTO ... VALUES (...)`
  - `UPDATE`
  - `DELETE`
  - `SELECT`
- Query shaping:
  - `DISTINCT` / `UNIQUE`
  - `ORDER BY`
  - `LIMIT`
  - `OFFSET`
  - `AS` aliases in the `SELECT` list
- Expressions and predicates:
  - arithmetic operators
  - logical operators: `AND`, `OR`, `NOT`
  - bitwise operators
  - comparisons
  - `BETWEEN`
  - `LIKE`
  - `REGEXP`
- Aggregation:
  - `COUNT`
  - `SUM`
  - `AVG`
  - `MIN`
  - `MAX`
  - `GROUP BY`
  - `HAVING`
- Subqueries:
  - scalar `SELECT` expressions
  - `EXISTS (SELECT ...)`
  - `IN (SELECT ...)`
  - quantified comparisons with `ANY` / `ALL`
- Data model features:
  - `NULL`
  - column default expressions
  - `AUTO_INCREMENT`
- Multi-source `SELECT`:
  - comma-separated sources in `FROM`
  - subquery sources in `FROM`
  - file-path sources and targets wherever table/view names are accepted
  - qualified columns such as `tasks.title`
- Streamed execution:
  - coroutine row/value generators
  - serial and parallel coroutine executors
  - structured `ExecutionResult` objects

---

## 2. What is it not?

This project is **not**:

- a SQLite replacement
- a standards-complete SQL engine
- an ACID database
- a query optimizer
- a networked database server
- a persistent engine with indexes, transactions, or concurrent write isolation

### Important disclaimer

`fsql` intentionally implements a **custom SQL-like dialect**.

Some syntax is SQL-inspired, but not identical to mainstream engines. For example:

- column defaults currently use **`column = expression`** in `CREATE TABLE`, not standard SQL `DEFAULT expression`
- multi-source queries are currently expressed with **comma-separated `FROM` sources**, not `JOIN` syntax
- quoted file paths can be used anywhere the dialect expects a table or view name
- some operators and behaviors are deliberately simplified

If you want exact SQL compatibility, this project is the wrong tool.
If you want a small, readable, embeddable engine that behaves *like SQL in many places*, this project is exactly that.

---

## 3. How to build fsql?

### Requirements

From the project files, the known requirements are:

- **CMake 3.12+**
- a **C++20-capable compiler**

### Build from the repository root

```sh
cmake -S . -B build
cmake --build build
```

### CMake targets

The project currently builds the following important targets:

| Target       | Type | Purpose |
|--------------|---|---|
| `fsqllib`    | static library | core parsing / execution / storage / streaming engine |
| `fsqlapplib` | static library | CLI-specific support code |
| `fsql`       | executable | main command-line application |
| `fselect`    | executable | wrapper around `fsql` for `SELECT` |
| `finsert`    | executable | wrapper around `fsql` for `INSERT` |
| `fupdate`    | executable | wrapper around `fsql` for `UPDATE` |
| `fdelete`    | executable | wrapper around `fsql` for `DELETE` |
| `fcreate`    | executable | wrapper around `fsql` for `CREATE` |
| `fdrop`      | executable | wrapper around `fsql` for `DROP` |
| `fsqltests`  | executable | doctest-based test suite |

### Typical build output locations

Depending on your generator/platform, the binaries typically end up under the build tree. In this repository layout, common paths are:

```text
build/fsql/fsql
build/fsql/fselect
build/fsql/finsert
build/fsqltests/fsqltests
```

---

## 4. How to use it as a CLI tool?

The CLI uses `CsvStorage`, rooted at the **current working directory**.
That means running the CLI in some folder makes that folder the backing “database directory” for table/view files.

For tables, `.csv` is the default write format when no extension is specified. Existing `.json`, `.toml`, `.yaml` / `.yml`, and `.xml` tables are also detected automatically when a table name is resolved without an explicit extension.

### 4.1 Main executable

Use the main executable when you want to pass complete statements yourself.

```sh
./build/fsql/fsql "CREATE TABLE todos (id AUTO_INCREMENT, title, done = false);"
./build/fsql/fsql "INSERT INTO todos (title) VALUES ('Buy milk');"
./build/fsql/fsql "SELECT * FROM todos ORDER BY id;"
```

You can also pipe a statement through standard input:

```sh
echo "SELECT * FROM todos;" | ./build/fsql/fsql
```

### 4.2 Wrapper executables

Wrapper executables prepend the statement keyword automatically.
This is convenient when you are repeatedly working with a single statement family.

```sh
./build/fsql/fcreate "TABLE todos (title, done = false);"
./build/fsql/finsert "INTO todos VALUES ('Ship release', true);"
./build/fsql/fselect "* FROM todos WHERE done = false;"
./build/fsql/fupdate "todos SET done = true WHERE title = 'Ship release';"
./build/fsql/fdelete "FROM todos WHERE done = true;"
./build/fsql/fdrop "TABLE todos;"
```

### 4.3 Help output

```sh
./build/fsql/fsql --help
./build/fsql/fselect --help
./build/fsql/fcreate --help
```

### 4.4 CLI session example

```sh
./build/fsql/fsql "CREATE TABLE tasks (id AUTO_INCREMENT, title, category = 'general', done = false);"
./build/fsql/fsql "INSERT INTO tasks (title) VALUES ('Write README');"
./build/fsql/fsql "INSERT INTO tasks (title, category) VALUES ('Fix parser bug', 'dev');"
./build/fsql/fsql "SELECT id, title, category, done FROM tasks ORDER BY category, title;"
./build/fsql/fsql "UPDATE tasks SET done = true WHERE title = 'Write README';"
./build/fsql/fsql "SELECT title FROM tasks WHERE NOT done OR category = 'dev';"
```

### 4.5 Working with table files directly

Quoted file paths can be used anywhere the dialect would normally accept a table or view identifier.
That includes `CREATE TABLE`, `CREATE VIEW`, `INSERT INTO`, `UPDATE`, `DELETE FROM`, `DROP TABLE`, `DROP VIEW`, and `SELECT ... FROM ...`.

```sh
./build/fsql/fsql "CREATE TABLE '/tmp/tasks' (id AUTO_INCREMENT, title, done = false);"
./build/fsql/fsql "INSERT INTO '/tmp/tasks' (title) VALUES ('Write README');"
./build/fsql/fsql "SELECT src.title FROM '/tmp/tasks.csv' src WHERE src.done = false ORDER BY src.title;"
./build/fsql/fsql "CREATE VIEW '/tmp/open_tasks' AS SELECT title FROM '/tmp/tasks' WHERE done = false;"
./build/fsql/fsql "SELECT * FROM '/tmp/open_tasks';"
```

For table files, `.csv` is the default when writing without an explicit extension.
When reading or mutating an existing table, the engine can auto-detect `.csv`, `.json`, `.toml`, `.yaml` / `.yml`, and `.xml` files when the extension is omitted.
If multiple table files share the same base name, the command fails with an ambiguity error.

```sh
./build/fsql/fsql "CREATE TABLE tasks.json (id AUTO_INCREMENT, title, done = false);"
./build/fsql/fsql "INSERT INTO tasks (title) VALUES ('Write README');"
./build/fsql/fsql "SELECT title FROM tasks WHERE done = false ORDER BY title;"
```

For view files, the `.view.sql` suffix is optional when it can be resolved correctly.
Relative quoted paths are resolved from the active `CsvStorage` root, which for the CLI is the current working directory.

### 4.6 How CLI persistence works

If you run:

```sh
./build/fsql/fsql "CREATE TABLE todos (title, done);"
```

from a directory called `workspace/`, the CLI will create something like:

```text
workspace/todos.csv
```

Views are persisted separately as `.view.sql` files.

---

## 5. How to use it as a library?

The main library target is **`fsqllib`**.

### 5.1 Typical embedding strategy

If you vendor this repository into another CMake project, the simplest approach is:

```cmake
add_subdirectory(fsql)

target_link_libraries(my_app PRIVATE fsqllib)
```

`fsqllib` exports its public include directory, so consumers can include the headers from `fsqllib/include/`.

### 5.2 The main library concepts

Most embedders will work with these types:

| Type | Role |
|---|---|
| `fsql::Tokenizer` | turns input text into `Token`s |
| `fsql::Parser` | turns tokens into a parsed `Statement` or `Expression` |
| `fsql::Executor` | executes parsed statements against an `IStorage` |
| `fsql::ExecutionResult` | structured execution outcome |
| `fsql::ExecutionTable` | reopenable streamed result rows for `SELECT` |
| `fsql::IStorage` | abstraction for table/view storage |
| `fsql::CsvStorage` | CSV-backed implementation of `IStorage` |
| `fsql::MemoryStorage` | in-memory implementation of `IStorage` |
| `fsql::ICoroExecutor` | driver for row/value coroutine streams |
| `fsql::SerialCoroExecutor` | serial stream driver |
| `fsql::ParallelCoroExecutor` | overlapped/parallel stream driver |
| `fsql::IOutputWriter` | rendering abstraction for `ExecutionResult` |
| `fsql::ConsoleOutputWriter` | terminal-friendly result formatter |

### 5.3 Minimal library example

```cpp
#include "ConsoleOutputWriter.h"
#include "Executor.h"
#include "MemoryStorage.h"
#include "ParallelCoroExecutor.h"
#include "Parser.h"
#include "Tokenizer.h"

#include <iostream>
#include <memory>
#include <string>

int main()
{
    auto storage = std::make_shared<fsql::MemoryStorage>();
    auto coro = std::make_shared<fsql::ParallelCoroExecutor>();
    fsql::Executor executor(storage, coro);
    fsql::ConsoleOutputWriter writer(coro);

    auto run = [&](const std::string& query)
    {
        fsql::Tokenizer tokenizer(query);
        fsql::Parser parser(tokenizer.tokenize());
        const auto result = executor.execute(parser.parse_statement());
        if (!result.success)
        {
            std::cerr << "Error: " << result.error << '\n';
            return;
        }
        writer.write(std::cout, result);
    };

    run("CREATE TABLE todos (title, done = false);");
    run("INSERT INTO todos (title) VALUES ('Buy milk');");
    run("SELECT title, done FROM todos;");
}
```

### 5.4 Using the library without console rendering

A key design goal of `fsql` is that the core executor **does not need to print**.
You can inspect `ExecutionResult` directly.

```cpp
fsql::Tokenizer tokenizer("SELECT title FROM todos WHERE done = false;");
fsql::Parser parser(tokenizer.tokenize());
const auto result = executor.execute(parser.parse_statement());

if (!result.success)
{
    throw std::runtime_error(result.error);
}

if (result.table.has_value())
{
    const auto& table = *result.table;
    coro->drive_rows(table.rows(), [](const fsql::Row& row)
    {
        std::cout << row[0] << '\n';
        return true;
    });
}
```

### 5.5 Choosing a storage backend

#### In-memory storage

Use `MemoryStorage` when:

- embedding in tests
- embedding in tools that do not want filesystem side effects
- creating ephemeral databases

#### CSV-backed storage

Use `CsvStorage` when:

- you want durable CSV-backed tables and persisted views
- you want CLI-like behavior from inside another program
- you want direct access to CSV files in a folder

You can root `CsvStorage` in any directory:

```cpp
auto storage = std::make_shared<fsql::CsvStorage>("/path/to/data");
```

### 5.6 Rendering human-readable output

If you want CLI-style formatted output from library code, use `ConsoleOutputWriter`.

If you do not, ignore it and handle `ExecutionResult` yourself.

### 5.7 Serialization helpers

`SqlSerialization` functions are also public and useful if you want to:

- turn expressions/statements back into the project’s SQL-like text
- inspect persisted view definitions
- build tooling around the AST

---

## 6. SQL-like syntax explained

This section documents the *current* implemented dialect.
It is intentionally practical rather than pretending to be full ANSI SQL.

### 6.1 Statements overview

Currently supported top-level statement families are:

- `CREATE TABLE`
- `CREATE VIEW ... AS SELECT ...`
- `ALTER TABLE`
- `ALTER VIEW ... AS SELECT ...`
- `DROP TABLE`
- `DROP VIEW`
- `INSERT INTO ... VALUES (...)`
- `SELECT`
- `UPDATE`
- `DELETE FROM`

Examples:

```sql
CREATE TABLE todos (id AUTO_INCREMENT, title, done = false);
CREATE VIEW open_todos AS SELECT title FROM todos WHERE done = false;
ALTER TABLE todos ADD COLUMN category = 'backlog';
ALTER VIEW open_todos AS SELECT id, title FROM todos WHERE done = false;
DROP VIEW open_todos;
DROP TABLE todos;
```

### 6.2 Literals, identifiers, and values

#### String literals

Use single quotes:

```sql
'Ship release'
'ops'
'2026-04-23'
```

#### Numbers

Numbers are parsed as numeric literals when possible:

```sql
1
42
3.14
-7
```

#### Booleans

This dialect commonly uses text tokens such as `true` and `false` in expressions.

```sql
done = true
flag = false
```

#### NULL

`NULL` is supported explicitly:

```sql
INSERT INTO tasks VALUES ('Patch release', NULL);
SELECT title FROM tasks WHERE archived_at IS NULL;
```

#### Identifiers

Unquoted identifiers are used for table names, view names, column names, and function names.

Qualified identifiers are written as:

```sql
source.column
```

Example:

```sql
SELECT tasks.title, teams.name FROM tasks, teams WHERE tasks.team_id = teams.id;
```

The same expression grammar is used in `WHERE` clauses for `SELECT`, `UPDATE`, and `DELETE`.
That means you can reuse predicates such as:

```sql
SELECT title FROM tasks WHERE tasks.done = false;
UPDATE tasks SET done = true WHERE tasks.done = false;
DELETE FROM tasks WHERE tasks.done = true;
```

In single-table `UPDATE` and `DELETE`, qualified references use the target table name because those statements do not currently support table aliases.

### 6.3 SELECT sources

`SELECT` sources are more flexible than in many minimal SQL toys.

#### Table sources

```sql
SELECT * FROM todos;
```

#### Multiple sources

Multiple comma-separated sources are supported:

```sql
SELECT tasks.title, teams.name
FROM tasks, teams
WHERE tasks.team_id = teams.id;
```

This is **not** SQL `JOIN` syntax. It is a multi-source `FROM` with predicate-based matching in `WHERE`.

#### Subquery sources

You can use a `SELECT` subquery in `FROM`.

```sql
SELECT lookup.title
FROM (SELECT title FROM todos WHERE done = false) lookup;
```

Aliases are supported for subquery sources and are usually the clearest choice when you want to reference projected columns explicitly.

#### Projection aliases

`SELECT` projections can be renamed with `AS`. The alias may be a bare identifier or a quoted string when you want spaces or punctuation in the output header.

```sql
SELECT title AS task_title, priority + 1 AS next_priority
FROM tasks;

SELECT title AS 'Task title', priority + 1 AS "next priority"
FROM tasks;

SELECT derived.task_title
FROM (SELECT title AS task_title FROM tasks WHERE done = false) derived;
```

#### File path sources

You can quote a path anywhere a table or view name would normally appear.
In `FROM`, that means a quoted path may resolve to either a CSV table file or a persisted `.view.sql` file.

```sql
CREATE TABLE '/tmp/tasks' (id AUTO_INCREMENT, title, done = false);
INSERT INTO '/tmp/tasks' (title) VALUES ('Patch release');
SELECT src.title FROM '/tmp/tasks.csv' src;
SELECT * FROM '/tmp/tasks';
CREATE VIEW '/tmp/open_tasks' AS SELECT title FROM '/tmp/tasks' WHERE done = false;
SELECT * FROM '/tmp/open_tasks';
UPDATE '/tmp/tasks' SET done = true WHERE title = 'Patch release';
DELETE FROM '/tmp/tasks' WHERE done = true;
DROP VIEW '/tmp/open_tasks';
DROP TABLE '/tmp/tasks';
```

Table paths may omit `.csv` when resolution is unambiguous.
View paths may omit `.view.sql` when resolution is unambiguous.
If both a table file and a view file match the same quoted path, the reference is ambiguous and execution fails.

### 6.4 Expressions and operators

Supported expression categories include:

These expression forms are shared across row-filtering `WHERE` clauses in `SELECT`, `UPDATE`, and `DELETE`, and they also work inside subqueries used by those clauses.

#### Arithmetic

```sql
priority + 1
hours * rate
(a + b) / 2
```

#### Logical

```sql
NOT done
owner = 'ops' AND priority >= 5
team = 'ops' OR team = 'sec'
```

#### Comparison

```sql
priority = 10
priority != 10
priority < 10
priority <= 10
priority > 10
priority >= 10
```

#### Bitwise

```sql
flags & 2
flags | 1
flags ^ 8
~flags
```

#### SQL-style predicates

```sql
title LIKE 'Patch%'
owner REGEXP '^op'
priority BETWEEN 5 AND 10
archived_at IS NULL
archived_at IS NOT NULL
```

#### Functions

The currently important built-in function is:

```sql
NOW()
```

Aggregate functions are also supported in `SELECT` projections:

```sql
COUNT(*)
SUM(points)
AVG(points)
MIN(points)
MAX(points)
```

### 6.5 Grouping and aggregation

Grouped aggregation is supported for single-table grouped queries.

```sql
SELECT team, COUNT(*), SUM(hours)
FROM incidents
WHERE resolved = false
GROUP BY team
HAVING SUM(hours) >= 5
ORDER BY SUM(hours) DESC;
```

Notes:

- `HAVING` requires `GROUP BY`
- aggregate expressions are supported in grouped projections/order/having paths
- grouped queries do **not** support `SELECT *`
- for aggregate-only queries without `GROUP BY`, input-row shaping (`WHERE`, `ORDER BY`, `DISTINCT`, `LIMIT`, `OFFSET`) is applied before aggregate functions are finalized

### 6.6 Subqueries

Several subquery forms are implemented.

#### Scalar subqueries

```sql
SELECT title
FROM todos
WHERE category = (SELECT value FROM defaults);
```

#### EXISTS

```sql
SELECT title
FROM tasks
WHERE EXISTS (SELECT id FROM teams WHERE name = 'ops');
```

#### IN

```sql
SELECT title
FROM tasks
WHERE team_id IN (SELECT id FROM teams WHERE on_call = true);
```

#### ANY / ALL

```sql
SELECT title
FROM tasks
WHERE severity > ALL (SELECT value FROM thresholds);
```

### 6.7 Views

Views are stored as serialized `SELECT` statements and behave as readonly virtual tables.

```sql
CREATE VIEW open_tasks AS
SELECT title, team
FROM tasks
WHERE done = false;

SELECT * FROM open_tasks;

ALTER VIEW open_tasks AS
SELECT id, title
FROM tasks
WHERE done = false;

DROP VIEW open_tasks;
```

### 6.8 ALTER TABLE and schema metadata

Supported `ALTER TABLE` actions include:

- `ADD COLUMN`
- `DROP COLUMN`
- `RENAME COLUMN`
- `ALTER COLUMN ... SET DEFAULT`
- `ALTER COLUMN ... DROP DEFAULT`
- `ALTER COLUMN ... SET AUTO_INCREMENT`
- `ALTER COLUMN ... DROP AUTO_INCREMENT`

Examples:

```sql
ALTER TABLE todos ADD COLUMN category = 'backlog';
ALTER TABLE todos DROP COLUMN category;
ALTER TABLE todos RENAME COLUMN title TO summary;
ALTER TABLE todos ALTER COLUMN category SET DEFAULT 'general';
ALTER TABLE todos ALTER COLUMN category DROP DEFAULT;
ALTER TABLE todos ALTER COLUMN id SET AUTO_INCREMENT;
ALTER TABLE todos ALTER COLUMN id DROP AUTO_INCREMENT;
```

### 6.9 NULL, defaults, and AUTO_INCREMENT

#### Defaults

Defaults are currently declared with `=` in `CREATE TABLE`.

```sql
CREATE TABLE tasks (
    id AUTO_INCREMENT,
    title,
    category = 'general',
    created_at = NOW(),
    archived_at = NULL
);
```

Defaults can also be subqueries:

```sql
CREATE TABLE work_items (
    category = (SELECT value FROM defaults)
);
```

#### AUTO_INCREMENT

Only one `AUTO_INCREMENT` column is supported per table.

```sql
CREATE TABLE todos (id AUTO_INCREMENT, title);
```

#### NULL behavior

Currently implemented:

- `NULL` literal
- `IS NULL`
- `IS NOT NULL`
- basic null-aware predicate behavior

### 6.10 Dialect differences and current limitations

This project deliberately differs from mainstream SQL in several ways.

#### Important non-standard / limited areas

- defaults use `column = expr` in table definitions
- no explicit `JOIN` syntax yet
- no `UNION`, `INTERSECT`, or `EXCEPT`
- no transactions
- no indexes
- no type system beyond string/numeric/null-style evaluation rules
- not all SQL functions are implemented
- this is not a full standards-compliant SQL parser

#### Example of a currently unsupported style

```sql
SELECT *
FROM tasks
INNER JOIN teams ON tasks.team_id = teams.id;
```

Use multi-source `FROM` plus `WHERE` instead:

```sql
SELECT tasks.title, teams.name
FROM tasks, teams
WHERE tasks.team_id = teams.id;
```

---

## 7. Execution model explained

### 7.1 High-level pipeline

The engine follows a clear pipeline:

1. **input text**
2. `Tokenizer`
3. `Parser`
4. AST / `Statement`
5. `Executor`
6. `ExecutionResult`
7. optional rendering through an `IOutputWriter`

This separation is intentional and makes the library reusable outside the CLI.

### 7.2 Storage model

Execution is abstracted behind `IStorage`.

That means the same parser/executor works with:

- `CsvStorage`
- `MemoryStorage`
- any custom storage implementation you provide

The storage contract separates:

- metadata access (`describe_table`)
- full table materialization (`load_table`)
- streaming row scans (`scan_table`)
- view persistence (`load_view`, `save_view`)

### 7.3 Structured results instead of direct printing

The executor returns a structured `ExecutionResult`.
That result contains:

- `success`
- `kind`
- `affected_rows`
- `message`
- optional `table`
- `error`

This is important for library consumers because it means:

- you can inspect results programmatically
- you do not need the CLI
- you do not need to depend on console formatting

### 7.4 Streaming result model

For `SELECT`, the returned `ExecutionTable` does **not** necessarily hold rows in a fully materialized vector.
Instead it exposes:

```cpp
std::function<RowGenerator()> rows;
```

This means result rows are reopenable and streamable.
Consumers can drive them directly with a coroutine executor.

### 7.5 Coroutine executors

The streaming engine uses coroutine-backed generators (`RowGenerator`, `ValueGenerator`) plus an execution driver interface:

- `ICoroExecutor`
- `SerialCoroExecutor`
- `ParallelCoroExecutor`

#### `SerialCoroExecutor`

Consumes rows/values serially in order.
Good for deterministic simple execution.

#### `ParallelCoroExecutor`

Overlaps loading and consumption where safe.
It preserves ordered consumer callbacks while allowing independent work to be scheduled more aggressively.

### 7.6 Where buffering still happens

The engine prefers streaming, but some query features still require buffering or reshaping.
Typical buffering cases include:

- `ORDER BY`
- `DISTINCT`
- grouped aggregation
- aggregate result shaping
- some derived / buffered subquery paths

In other words:

- simple streamable `SELECT` paths can stay streamed
- result-shaping operations that fundamentally require all relevant rows may materialize intermediate state

### 7.7 Error handling model

`Executor::execute(...)` catches exceptions and returns failed `ExecutionResult` values.

That means library consumers can choose either of these styles:

- inspect `result.success` / `result.error`
- wrap execution with their own throwing helper if they prefer exception-style control flow

The test suite includes an example of the latter pattern.

---

## 8. Project layout

```text
fsqllib/   Core SQL engine: tokenizer, parser, executor, storage, streaming
fsql/      Command-line application and wrapper support
fsqltests/ Automated tests
```

More specifically:

- `fsqllib/include/` contains public headers
- `fsqllib/src/` contains the engine implementation
- `fsql/src/` contains CLI input/help/application logic
- `fsqltests/` contains grouped doctest suites for parser, executor, and storage behavior

---

## 9. Running the tests

After building, run the full suite with CTest:

```sh
ctest --test-dir build --output-on-failure
```

Or run the doctest binary directly:

```sh
./build/fsqltests/fsqltests
```

---

## 10. Roadmap and further reading

To see planned and completed work, check:

- [`progress.md`](PROGRESS.md)

That file tracks both implemented features and explicitly planned follow-up work such as:

- SQL standard alignment improvements
- more functions
- set operations
- richer join syntax
- deeper documentation (`SYNTAX.md` is planned)

---

If you use `fsql` as a library, the most important takeaway is this:

> the parser, executor, storage abstraction, streaming result model, and output formatting are intentionally decoupled, so you can embed only the parts you need.
