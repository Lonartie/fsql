#pragma once

#include <string>
#include <vector>

namespace sql
{
    /// @brief Represents a single CSV row.
    using Row = std::vector<std::string>;

    /// @brief Represents a loaded CSV-backed table.
    struct Table
    {
        /// @brief Logical table name without the `.csv` extension.
        std::string name;

        /// @brief Ordered list of column names.
        std::vector<std::string> columns;

        /// @brief Table data rows.
        std::vector<Row> rows;
    };

    /// @brief Represents a stored readonly view definition.
    struct ViewDefinition
    {
        /// @brief Logical view name.
        std::string name;

        /// @brief Serialized `SELECT` statement backing the view.
        std::string select_statement;
    };

    /// @brief Supported schema object kinds.
    enum class SchemaObjectKind
    {
        Table,
        View
    };
}

