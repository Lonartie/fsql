#include "ColumnMetadata.h"

#include "ExpressionEvaluation.h"
#include "SqlError.h"

#include <algorithm>
#include <string_view>

namespace fsql
{
    ColumnMetadata parse_column_metadata(const std::string& stored_name)
    {
        ColumnMetadata metadata;
        metadata.visible_name = stored_name;

        constexpr std::string_view auto_increment_marker = " AUTO_INCREMENT";
        constexpr std::string_view default_marker = " DEFAULT(";

        const auto default_position = metadata.visible_name.find(default_marker);
        if (default_position != std::string::npos)
        {
            const auto expression_start = default_position + default_marker.size();
            const auto expression_end = metadata.visible_name.rfind(')');
            if (expression_end == std::string::npos || expression_end < expression_start)
            {
                fail("Invalid stored default expression metadata");
            }
            metadata.default_expression = metadata.visible_name.substr(expression_start, expression_end - expression_start);
            metadata.visible_name = metadata.visible_name.substr(0, default_position);
        }

        const auto auto_increment_position = metadata.visible_name.find(auto_increment_marker);
        if (auto_increment_position != std::string::npos)
        {
            metadata.auto_increment = true;
            metadata.visible_name = metadata.visible_name.substr(0, auto_increment_position);
        }

        return metadata;
    }

    std::string serialize_column_metadata(const ColumnMetadata& metadata)
    {
        std::string stored_name = metadata.visible_name;
        if (metadata.auto_increment)
        {
            stored_name += " AUTO_INCREMENT";
        }
        if (!metadata.default_expression.empty())
        {
            stored_name += " DEFAULT(" + metadata.default_expression + ")";
        }
        return stored_name;
    }

    std::string visible_column_name(const std::string& stored_name)
    {
        return parse_column_metadata(stored_name).visible_name;
    }

    bool has_visible_column_name(const Table& table, const std::string& column_name, std::optional<std::size_t> skip_index)
    {
        for (std::size_t i = 0; i < table.columns.size(); ++i)
        {
            if (skip_index.has_value() && *skip_index == i)
            {
                continue;
            }
            if (visible_column_name(table.columns[i]) == column_name)
            {
                return true;
            }
        }
        return false;
    }

    void ensure_single_auto_increment_column(const Table& table, std::optional<std::size_t> skip_index)
    {
        std::size_t count = 0;
        for (std::size_t i = 0; i < table.columns.size(); ++i)
        {
            if (skip_index.has_value() && *skip_index == i)
            {
                continue;
            }
            if (parse_column_metadata(table.columns[i]).auto_increment)
            {
                ++count;
            }
        }
        if (count > 1)
        {
            fail("Only one AUTO_INCREMENT column is supported");
        }
    }

    void backfill_auto_increment_column(Table& table, std::size_t index)
    {
        long long maximum = 0;
        for (const auto& row : table.rows)
        {
            if (row[index].empty() || row[index] == null_storage_marker)
            {
                continue;
            }
            try
            {
                maximum = std::max(maximum, std::stoll(row[index]));
            }
            catch (const std::exception&)
            {
                fail("AUTO_INCREMENT column requires numeric existing values");
            }
        }

        for (auto& row : table.rows)
        {
            if (!row[index].empty() && row[index] != null_storage_marker)
            {
                continue;
            }
            row[index] = std::to_string(++maximum);
        }
    }

    std::optional<std::size_t> auto_increment_column_index(const Table& table)
    {
        for (std::size_t i = 0; i < table.columns.size(); ++i)
        {
            if (parse_column_metadata(table.columns[i]).auto_increment)
            {
                return i;
            }
        }
        return std::nullopt;
    }

    std::string next_auto_increment_value(const Table& table, std::size_t index)
    {
        long long maximum = 0;
        for (const auto& row : table.rows)
        {
            if (row[index].empty() || row[index] == null_storage_marker)
            {
                continue;
            }
            maximum = std::max(maximum, std::stoll(row[index]));
        }
        return std::to_string(maximum + 1);
    }
}


