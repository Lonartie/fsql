# Features

## Planned
- ALTER TABLE command
- CREATE VIEW / DROP VIEW command
- SELECT from multiple sources (tables and subqueries)
    - Addressing columns by table name (e.g. table1.column1) to avoid ambiguity
- JOINs, GROUP BY, HAVING, ORDER BY
- LIMIT, OFFSET, DISTINCT, UNIQUE
- UNION, UNION ALL, INTERSECT, EXCEPT
- EXISTS, IN, ANY, ALL
- BETWEEN, LIKE, REGEXP
- IS NULL, IS NOT NULL (whole null-semantics)
- Align operator semantics with SQL standard (e.g. AND instead of && and many more)
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