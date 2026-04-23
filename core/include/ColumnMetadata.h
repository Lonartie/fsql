#pragma once

#include "SqlTypes.h"

#include <cstddef>
#include <optional>
#include <string>

namespace sql
{
    /// @brief Parsed metadata encoded into a stored column name.
    struct ColumnMetadata
    {
        /// @brief User-visible column name.
        std::string visible_name;

        /// @brief Whether the column auto-increments.
        bool auto_increment = false;

        /// @brief Serialized default expression, when present.
        std::string default_expression;
    };

    /// @brief Decodes stored column metadata.
    /// @param stored_name Stored column descriptor.
    /// @return Parsed column metadata.
    ColumnMetadata parse_column_metadata(const std::string& stored_name);

    /// @brief Encodes stored column metadata.
    /// @param metadata Column metadata to encode.
    /// @return Stored column descriptor.
    std::string serialize_column_metadata(const ColumnMetadata& metadata);

    /// @brief Returns the visible column name from encoded metadata.
    /// @param stored_name Stored column descriptor.
    /// @return Visible column name.
    std::string visible_column_name(const std::string& stored_name);

    /// @brief Checks whether a table already contains a visible column name.
    /// @param table Table to inspect.
    /// @param column_name Visible column name.
    /// @param skip_index Optional column index to ignore.
    /// @return `true` when the visible name already exists.
    bool has_visible_column_name(const Table& table, const std::string& column_name, std::optional<std::size_t> skip_index = std::nullopt);

    /// @brief Ensures there is at most one AUTO_INCREMENT column.
    /// @param table Table to inspect.
    /// @param skip_index Optional column index to ignore.
    void ensure_single_auto_increment_column(const Table& table, std::optional<std::size_t> skip_index = std::nullopt);

    /// @brief Backfills empty values in an AUTO_INCREMENT column.
    /// @param table Table to mutate.
    /// @param index AUTO_INCREMENT column index.
    void backfill_auto_increment_column(Table& table, std::size_t index);

    /// @brief Finds the AUTO_INCREMENT column index, if present.
    /// @param table Table to inspect.
    /// @return Column index when present.
    std::optional<std::size_t> auto_increment_column_index(const Table& table);

    /// @brief Computes the next AUTO_INCREMENT value for a column.
    /// @param table Table to inspect.
    /// @param index AUTO_INCREMENT column index.
    /// @return Next generated value.
    std::string next_auto_increment_value(const Table& table, std::size_t index);
}

