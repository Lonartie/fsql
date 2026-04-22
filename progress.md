# Features

## Planned
- ALTER TABLE command
- CREATE VIEW / DROP VIEW command
- JOINs
- UNION, UNION ALL, INTERSECT, EXCEPT
- EXISTS, IN, ANY, ALL
- IS NULL, IS NOT NULL (whole null-semantics)
- Further align operator semantics with SQL standard beyond the implemented keyword operators
  - Standardize remaining non-SQL operators / aliases and edge-case semantics
- All missing aggregate functions:
  - String functions: 
    - length, substr, trim, replace, instr, lower, upper, concat, etc.
  - Date/Time functions:
    - date, time, datetime, strftime, julianday, unixepoch, etc.
  - Numeric functions:
    - round, abs, coalesce, nullif, etc.
  - JSON functions:
    - json_array, json_object, json_array_length, json_array_length, json_array_length, json_array_length, etc.
- Align default value syntax with SQL standard (e.g. DEFAULT 'value' instead of = 'value')
- More powerful subqueries returning lists of values
  - IN (SELECT ...), EXISTS (SELECT ...), etc.

## In Progress

## Completed
- SELECT result shaping for single-table queries:
  - ORDER BY
  - LIMIT
  - OFFSET
  - DISTINCT / UNIQUE
- SQL keyword logical operators and predicates in expressions:
  - AND
  - OR
  - NOT
  - BETWEEN
  - LIKE
  - REGEXP
- Single-table grouped aggregation:
  - GROUP BY
  - HAVING
- Multi-source SELECTs:
  - Multiple table sources in `FROM`
  - Subquery sources in `FROM`
  - Addressing columns by source name (e.g. `table1.column1`) to avoid ambiguity
