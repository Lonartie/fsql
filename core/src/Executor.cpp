#include "Executor.h"

#include "Parser.h"
#include "SqlError.h"
#include "StringUtils.h"
#include "Tokenizer.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <ctime>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <regex>
#include <sstream>
#include <utility>
#include <vector>

namespace sql
{
    namespace
    {
        struct EvaluatedValue
        {
            std::string text;
            bool numeric = false;
            double number = 0.0;
        };

        struct SelectResult
        {
            std::vector<std::string> column_names;
            std::vector<Row> rows;
        };

        struct ProjectedSelectRow
        {
            Row values;
            std::vector<EvaluatedValue> order_values;
        };

        struct ParsedColumnMetadata
        {
            std::string visible_name;
            bool auto_increment = false;
            std::string default_expression;
        };

        bool is_integer_like(double value)
        {
            return std::fabs(value - std::round(value)) < 1e-9;
        }

        std::string format_number(double value)
        {
            if (is_integer_like(value))
            {
                return std::to_string(static_cast<long long>(std::llround(value)));
            }

            std::ostringstream stream;
            stream << value;
            return stream.str();
        }

        bool try_parse_number(const std::string& text, double& value)
        {
            if (text.empty())
            {
                return false;
            }

            char* end = nullptr;
            value = std::strtod(text.c_str(), &end);
            return end != nullptr && *end == '\0';
        }

        bool to_bool(const EvaluatedValue& value)
        {
            if (value.numeric)
            {
                return std::fabs(value.number) > 1e-9;
            }
            return !value.text.empty() && !iequals(value.text, "false") && value.text != "0";
        }

        long long to_integer(const EvaluatedValue& value)
        {
            if (value.numeric)
            {
                return static_cast<long long>(std::llround(value.number));
            }

            double parsed = 0.0;
            if (try_parse_number(value.text, parsed))
            {
                return static_cast<long long>(std::llround(parsed));
            }

            fail("Expected numeric value but got '" + value.text + "'");
        }

        EvaluatedValue make_numeric(double value)
        {
            return {format_number(value), true, value};
        }

        EvaluatedValue make_text(std::string value)
        {
            double parsed = 0.0;
            if (try_parse_number(value, parsed))
            {
                return {std::move(value), true, parsed};
            }
            return {std::move(value), false, 0.0};
        }

        std::string quote_string(const std::string& value)
        {
            std::string quoted = "'";
            for (const char ch : value)
            {
                quoted += ch;
                if (ch == '\'')
                {
                    quoted += '\'';
                }
            }
            quoted += '\'';
            return quoted;
        }

        bool like_matches_at(const std::string& value, const std::string& pattern, std::size_t value_index, std::size_t pattern_index)
        {
            while (pattern_index < pattern.size())
            {
                if (pattern[pattern_index] == '%')
                {
                    ++pattern_index;
                    if (pattern_index == pattern.size())
                    {
                        return true;
                    }

                    for (std::size_t i = value_index; i <= value.size(); ++i)
                    {
                        if (like_matches_at(value, pattern, i, pattern_index))
                        {
                            return true;
                        }
                    }
                    return false;
                }

                if (value_index >= value.size())
                {
                    return false;
                }

                if (pattern[pattern_index] == '_')
                {
                    ++value_index;
                    ++pattern_index;
                    continue;
                }

                if (value[value_index] != pattern[pattern_index])
                {
                    return false;
                }

                ++value_index;
                ++pattern_index;
            }

            return value_index == value.size();
        }

        bool like_matches(const std::string& value, const std::string& pattern)
        {
            return like_matches_at(value, pattern, 0, 0);
        }

        std::string serialize_expression(const ExpressionPtr& expression);

        EvaluatedValue evaluate_expression(const ExpressionPtr& expression, const Table& table, const Row& row, const IStorage& storage);

        std::string serialize_select_statement(const SelectStatement& statement)
        {
            std::ostringstream stream;
            stream << "SELECT ";
            if (statement.distinct)
            {
                stream << "DISTINCT ";
            }
            if (statement.select_all)
            {
                stream << '*';
            }
            else
            {
                for (std::size_t i = 0; i < statement.projections.size(); ++i)
                {
                    if (i > 0)
                    {
                        stream << ", ";
                    }
                    stream << serialize_expression(statement.projections[i]);
                }
            }

            stream << " FROM " << statement.table_name;
            if (statement.where)
            {
                stream << " WHERE " << serialize_expression(statement.where);
            }

            if (!statement.order_by.empty())
            {
                stream << " ORDER BY ";
                for (std::size_t i = 0; i < statement.order_by.size(); ++i)
                {
                    if (i > 0)
                    {
                        stream << ", ";
                    }
                    stream << serialize_expression(statement.order_by[i].expression);
                    if (statement.order_by[i].descending)
                    {
                        stream << " DESC";
                    }
                }
            }

            if (statement.limit.has_value())
            {
                stream << " LIMIT " << *statement.limit;
            }

            if (statement.offset.has_value())
            {
                stream << " OFFSET " << *statement.offset;
            }

            return stream.str();
        }

        std::string serialize_expression(const ExpressionPtr& expression)
        {
            if (!expression)
            {
                return {};
            }

            switch (expression->kind)
            {
            case ExpressionKind::Literal:
            {
                double parsed = 0.0;
                if (try_parse_number(expression->text, parsed))
                {
                    return expression->text;
                }
                return quote_string(expression->text);
            }
            case ExpressionKind::Identifier:
                return expression->text;
            case ExpressionKind::Select:
                if (!expression->select)
                {
                    fail("Missing SELECT expression payload");
                }
                return "(" + serialize_select_statement(*expression->select) + ")";
            case ExpressionKind::FunctionCall:
            {
                std::ostringstream stream;
                stream << expression->text << '(';
                if (expression->function_uses_star)
                {
                    stream << '*';
                }
                else
                {
                    for (std::size_t i = 0; i < expression->arguments.size(); ++i)
                    {
                        if (i > 0)
                        {
                            stream << ", ";
                        }
                        stream << serialize_expression(expression->arguments[i]);
                    }
                }
                stream << ')';
                return stream.str();
            }
            case ExpressionKind::Unary:
            {
                std::string op;
                switch (expression->unary_operator)
                {
                case UnaryOperator::Plus: op = "+"; break;
                case UnaryOperator::Minus: op = "-"; break;
                case UnaryOperator::LogicalNot: op = "!"; break;
                case UnaryOperator::BitwiseNot: op = "~"; break;
                }
                return op + "(" + serialize_expression(expression->left) + ")";
            }
            case ExpressionKind::Binary:
            {
                std::string op;
                switch (expression->binary_operator)
                {
                case BinaryOperator::Multiply: op = "*"; break;
                case BinaryOperator::Divide: op = "/"; break;
                case BinaryOperator::Modulo: op = "%"; break;
                case BinaryOperator::Add: op = "+"; break;
                case BinaryOperator::Subtract: op = "-"; break;
                case BinaryOperator::Less: op = "<"; break;
                case BinaryOperator::LessEqual: op = "<="; break;
                case BinaryOperator::Greater: op = ">"; break;
                case BinaryOperator::GreaterEqual: op = ">="; break;
                case BinaryOperator::Like: op = "LIKE"; break;
                case BinaryOperator::Regexp: op = "REGEXP"; break;
                case BinaryOperator::Equal: op = "="; break;
                case BinaryOperator::NotEqual: op = "!="; break;
                case BinaryOperator::BitwiseAnd: op = "&"; break;
                case BinaryOperator::BitwiseXor: op = "^"; break;
                case BinaryOperator::BitwiseOr: op = "|"; break;
                case BinaryOperator::LogicalAnd: op = "&&"; break;
                case BinaryOperator::LogicalOr: op = "||"; break;
                }
                return "(" + serialize_expression(expression->left) + " " + op + " " + serialize_expression(expression->right) + ")";
            }
            }

            fail("Unsupported expression serialization");
        }

        bool is_aggregate_function_name(const std::string& name)
        {
            return iequals(name, "COUNT") || iequals(name, "SUM") || iequals(name, "AVG") || iequals(name, "MIN") || iequals(name, "MAX");
        }

        EvaluatedValue evaluate_function(const Expression& expression)
        {
            if (iequals(expression.text, "NOW"))
            {
                if (expression.function_uses_star || !expression.arguments.empty())
                {
                    fail("Unsupported function: NOW(...)");
                }
                const auto now = std::chrono::system_clock::now();
                const std::time_t time = std::chrono::system_clock::to_time_t(now);
                std::tm local_time{};
#ifdef _WIN32
                localtime_s(&local_time, &time);
#else
                localtime_r(&time, &local_time);
#endif
                std::ostringstream stream;
                stream << std::put_time(&local_time, "%Y-%m-%d %H:%M:%S");
                return make_text(stream.str());
            }

            if (is_aggregate_function_name(expression.text))
            {
                fail("Aggregate function '" + expression.text + "' can only be used in SELECT projections");
            }

            fail("Unsupported function: " + expression.text + "()");
        }

        ParsedColumnMetadata parse_column_metadata(const std::string& stored_name)
        {
            ParsedColumnMetadata metadata;
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

        std::string visible_column_name(const std::string& stored_name)
        {
            return parse_column_metadata(stored_name).visible_name;
        }

        std::string next_auto_increment_value(const Table& table, std::size_t index)
        {
            long long maximum = 0;
            for (const auto& row : table.rows)
            {
                if (row[index].empty())
                {
                    continue;
                }
                maximum = std::max(maximum, std::stoll(row[index]));
            }
            return std::to_string(maximum + 1);
        }

        std::string make_separator(const std::vector<std::size_t>& widths)
        {
            std::ostringstream stream;
            stream << '+';
            for (const auto width : widths)
            {
                stream << std::string(width + 2, '-') << '+';
            }
            return stream.str();
        }

        void write_row(std::ostream& output, const std::vector<std::string>& values, const std::vector<std::size_t>& widths)
        {
            output << '|';
            for (std::size_t i = 0; i < values.size(); ++i)
            {
                output << ' ' << std::left << std::setw(static_cast<int>(widths[i])) << values[i] << ' ' << '|';
            }
            output << '\n';
        }

        bool contains_aggregate_function(const ExpressionPtr& expression)
        {
            if (!expression)
            {
                return false;
            }

            switch (expression->kind)
            {
            case ExpressionKind::FunctionCall:
                if (is_aggregate_function_name(expression->text))
                {
                    return true;
                }
                for (const auto& argument : expression->arguments)
                {
                    if (contains_aggregate_function(argument))
                    {
                        return true;
                    }
                }
                return false;
            case ExpressionKind::Unary:
                return contains_aggregate_function(expression->left);
            case ExpressionKind::Binary:
                return contains_aggregate_function(expression->left) || contains_aggregate_function(expression->right);
            case ExpressionKind::Select:
            case ExpressionKind::Literal:
            case ExpressionKind::Identifier:
                return false;
            }

            return false;
        }

        int compare_values(const EvaluatedValue& left, const EvaluatedValue& right)
        {
            if (left.numeric && right.numeric)
            {
                if (left.number < right.number)
                {
                    return -1;
                }
                if (left.number > right.number)
                {
                    return 1;
                }
                return 0;
            }

            if (left.text < right.text)
            {
                return -1;
            }
            if (left.text > right.text)
            {
                return 1;
            }
            return 0;
        }

        void apply_select_modifiers(const SelectStatement& stmt, std::vector<ProjectedSelectRow>& rows)
        {
            if (!stmt.order_by.empty())
            {
                std::stable_sort(rows.begin(), rows.end(), [&stmt](const ProjectedSelectRow& left, const ProjectedSelectRow& right)
                {
                    for (std::size_t i = 0; i < stmt.order_by.size(); ++i)
                    {
                        const int comparison = compare_values(left.order_values[i], right.order_values[i]);
                        if (comparison == 0)
                        {
                            continue;
                        }
                        return stmt.order_by[i].descending ? (comparison > 0) : (comparison < 0);
                    }
                    return false;
                });
            }

            if (stmt.distinct)
            {
                rows.erase(std::unique(rows.begin(), rows.end(), [](const ProjectedSelectRow& left, const ProjectedSelectRow& right)
                {
                    return left.values == right.values;
                }), rows.end());
            }

            const std::size_t offset = stmt.offset.value_or(0);
            if (offset >= rows.size())
            {
                rows.clear();
                return;
            }
            if (offset > 0)
            {
                rows.erase(rows.begin(), rows.begin() + static_cast<std::ptrdiff_t>(offset));
            }

            if (stmt.limit.has_value() && rows.size() > *stmt.limit)
            {
                rows.erase(rows.begin() + static_cast<std::ptrdiff_t>(*stmt.limit), rows.end());
            }
        }

        void validate_select_projection(const ExpressionPtr& expression, const Table& table, const IStorage& storage)
        {
            if (!expression)
            {
                return;
            }

            switch (expression->kind)
            {
            case ExpressionKind::Identifier:
                static_cast<void>(storage.column_index(table, expression->text));
                return;
            case ExpressionKind::FunctionCall:
                for (const auto& argument : expression->arguments)
                {
                    validate_select_projection(argument, table, storage);
                }
                return;
            case ExpressionKind::Unary:
                validate_select_projection(expression->left, table, storage);
                return;
            case ExpressionKind::Binary:
                validate_select_projection(expression->left, table, storage);
                validate_select_projection(expression->right, table, storage);
                return;
            case ExpressionKind::Select:
            case ExpressionKind::Literal:
                return;
            }
        }

        std::vector<const Row*> filter_rows(const Table& table, const ExpressionPtr& where, const IStorage& storage)
        {
            std::vector<const Row*> rows;
            rows.reserve(table.rows.size());
            for (const auto& row : table.rows)
            {
                if (where && !to_bool(evaluate_expression(where, table, row, storage)))
                {
                    continue;
                }
                rows.push_back(&row);
            }
            return rows;
        }

        double require_numeric_argument(const EvaluatedValue& value, const std::string& function_name)
        {
            if (value.numeric)
            {
                return value.number;
            }

            double parsed = 0.0;
            if (try_parse_number(value.text, parsed))
            {
                return parsed;
            }

            fail("Aggregate function " + function_name + " requires numeric values");
        }

        EvaluatedValue evaluate_aggregate_function(const ExpressionPtr& expression, const Table& table, const std::vector<const Row*>& rows, const IStorage& storage)
        {
            if (!expression || expression->kind != ExpressionKind::FunctionCall || !is_aggregate_function_name(expression->text))
            {
                fail("Invalid aggregate projection");
            }

            if (iequals(expression->text, "COUNT"))
            {
                if (expression->function_uses_star)
                {
                    if (!expression->arguments.empty())
                    {
                        fail("COUNT(*) does not accept additional arguments");
                    }
                    return make_numeric(static_cast<double>(rows.size()));
                }

                if (expression->arguments.size() != 1)
                {
                    fail("COUNT requires exactly one argument or *");
                }

                std::size_t count = 0;
                for (const auto* row : rows)
                {
                    const auto value = evaluate_expression(expression->arguments[0], table, *row, storage);
                    if (!value.text.empty())
                    {
                        ++count;
                    }
                }
                return make_numeric(static_cast<double>(count));
            }

            if (expression->function_uses_star)
            {
                fail("Only COUNT supports '*' as an argument");
            }
            if (expression->arguments.size() != 1)
            {
                fail("Aggregate function " + expression->text + " requires exactly one argument");
            }

            if (iequals(expression->text, "SUM") || iequals(expression->text, "AVG"))
            {
                double total = 0.0;
                std::size_t count = 0;
                for (const auto* row : rows)
                {
                    const auto value = evaluate_expression(expression->arguments[0], table, *row, storage);
                    if (value.text.empty())
                    {
                        continue;
                    }
                    total += require_numeric_argument(value, expression->text);
                    ++count;
                }

                if (iequals(expression->text, "SUM"))
                {
                    return make_numeric(total);
                }
                return make_numeric(count == 0 ? 0.0 : (total / static_cast<double>(count)));
            }

            EvaluatedValue best;
            bool has_best = false;
            for (const auto* row : rows)
            {
                const auto value = evaluate_expression(expression->arguments[0], table, *row, storage);
                if (value.text.empty())
                {
                    continue;
                }

                if (!has_best)
                {
                    best = value;
                    has_best = true;
                    continue;
                }

                const bool prefer_numeric = value.numeric && best.numeric;
                const bool should_replace = iequals(expression->text, "MIN")
                    ? (prefer_numeric ? value.number < best.number : value.text < best.text)
                    : (prefer_numeric ? value.number > best.number : value.text > best.text);
                if (should_replace)
                {
                    best = value;
                }
            }

            return has_best ? make_text(best.text) : make_text("");
        }

        SelectResult run_select_statement(const SelectStatement& stmt, const IStorage& storage)
        {
            const Table table = storage.load_table(stmt.table_name);
            const auto rows = filter_rows(table, stmt.where, storage);

            for (const auto& order_by : stmt.order_by)
            {
                validate_select_projection(order_by.expression, table, storage);
            }

            std::vector<ProjectedSelectRow> projected_rows;
            projected_rows.reserve(rows.size());

            if (stmt.select_all)
            {
                SelectResult result;
                for (std::size_t i = 0; i < table.columns.size(); ++i)
                {
                    result.column_names.push_back(visible_column_name(table.columns[i]));
                }

                for (const auto* row : rows)
                {
                    ProjectedSelectRow projected_row;
                    projected_row.values = *row;
                    projected_row.order_values.reserve(stmt.order_by.size());
                    for (const auto& order_by : stmt.order_by)
                    {
                        projected_row.order_values.push_back(evaluate_expression(order_by.expression, table, *row, storage));
                    }
                    projected_rows.push_back(std::move(projected_row));
                }

                apply_select_modifiers(stmt, projected_rows);
                result.rows.reserve(projected_rows.size());
                for (auto& projected_row : projected_rows)
                {
                    result.rows.push_back(std::move(projected_row.values));
                }
                return result;
            }

            SelectResult result;
            bool has_aggregate_projection = false;
            for (const auto& projection : stmt.projections)
            {
                validate_select_projection(projection, table, storage);
                has_aggregate_projection = has_aggregate_projection || contains_aggregate_function(projection);
                result.column_names.push_back(serialize_expression(projection));
            }

            if (has_aggregate_projection)
            {
                ProjectedSelectRow aggregate_row;
                aggregate_row.values.reserve(stmt.projections.size());
                for (const auto& projection : stmt.projections)
                {
                    if (!projection || projection->kind != ExpressionKind::FunctionCall || !is_aggregate_function_name(projection->text))
                    {
                        fail("Aggregate queries only support aggregate functions in SELECT projections");
                    }
                    aggregate_row.values.push_back(evaluate_aggregate_function(projection, table, rows, storage).text);
                }
                projected_rows.push_back(std::move(aggregate_row));
                apply_select_modifiers(stmt, projected_rows);
                result.rows.reserve(projected_rows.size());
                for (auto& projected_row : projected_rows)
                {
                    result.rows.push_back(std::move(projected_row.values));
                }
                return result;
            }

            for (const auto* row : rows)
            {
                ProjectedSelectRow projected_row;
                projected_row.values.reserve(stmt.projections.size());
                for (const auto& projection : stmt.projections)
                {
                    projected_row.values.push_back(evaluate_expression(projection, table, *row, storage).text);
                }
                projected_row.order_values.reserve(stmt.order_by.size());
                for (const auto& order_by : stmt.order_by)
                {
                    projected_row.order_values.push_back(evaluate_expression(order_by.expression, table, *row, storage));
                }
                projected_rows.push_back(std::move(projected_row));
            }

            apply_select_modifiers(stmt, projected_rows);
            result.rows.reserve(projected_rows.size());
            for (auto& projected_row : projected_rows)
            {
                result.rows.push_back(std::move(projected_row.values));
            }

            return result;
        }

        EvaluatedValue evaluate_select_expression(const SelectStatement& stmt, const IStorage& storage)
        {
            const auto result = run_select_statement(stmt, storage);
            if (result.column_names.size() != 1)
            {
                fail("SELECT expression must return exactly one column");
            }
            if (result.rows.empty())
            {
                fail("SELECT expression returned no rows");
            }
            if (result.rows.size() != 1)
            {
                fail("SELECT expression returned more than one row");
            }
            return make_text(result.rows[0][0]);
        }

        EvaluatedValue evaluate_expression(const ExpressionPtr& expression, const Table& table, const Row& row, const IStorage& storage)
        {
            if (!expression)
            {
                return make_text("");
            }

            switch (expression->kind)
            {
            case ExpressionKind::Literal:
                return make_text(expression->text);
            case ExpressionKind::Identifier:
            {
                try
                {
                    const auto index = storage.column_index(table, expression->text);
                    return make_text(row[index]);
                }
                catch (const std::runtime_error&)
                {
                    return make_text(expression->text);
                }
            }
            case ExpressionKind::Select:
                if (!expression->select)
                {
                    fail("Missing SELECT expression payload");
                }
                return evaluate_select_expression(*expression->select, storage);
            case ExpressionKind::FunctionCall:
                return evaluate_function(*expression);
            case ExpressionKind::Unary:
            {
                const auto operand = evaluate_expression(expression->left, table, row, storage);
                switch (expression->unary_operator)
                {
                case UnaryOperator::Plus:
                    return make_numeric(operand.numeric ? operand.number : std::stod(operand.text));
                case UnaryOperator::Minus:
                    return make_numeric(-(operand.numeric ? operand.number : std::stod(operand.text)));
                case UnaryOperator::LogicalNot:
                    return make_numeric(to_bool(operand) ? 0.0 : 1.0);
                case UnaryOperator::BitwiseNot:
                    return make_numeric(static_cast<double>(~to_integer(operand)));
                }
                break;
            }
            case ExpressionKind::Binary:
            {
                const auto left = evaluate_expression(expression->left, table, row, storage);
                const auto right = evaluate_expression(expression->right, table, row, storage);
                switch (expression->binary_operator)
                {
                case BinaryOperator::Multiply: return make_numeric((left.numeric ? left.number : std::stod(left.text)) * (right.numeric ? right.number : std::stod(right.text)));
                case BinaryOperator::Divide: return make_numeric((left.numeric ? left.number : std::stod(left.text)) / (right.numeric ? right.number : std::stod(right.text)));
                case BinaryOperator::Modulo: return make_numeric(static_cast<double>(to_integer(left) % to_integer(right)));
                case BinaryOperator::Add:
                    if (left.numeric && right.numeric)
                    {
                        return make_numeric(left.number + right.number);
                    }
                    return make_text(left.text + right.text);
                case BinaryOperator::Subtract: return make_numeric((left.numeric ? left.number : std::stod(left.text)) - (right.numeric ? right.number : std::stod(right.text)));
                case BinaryOperator::Less:
                    return make_numeric(((left.numeric && right.numeric) ? (left.number < right.number) : (left.text < right.text)) ? 1.0 : 0.0);
                case BinaryOperator::LessEqual:
                    return make_numeric(((left.numeric && right.numeric) ? (left.number <= right.number) : (left.text <= right.text)) ? 1.0 : 0.0);
                case BinaryOperator::Greater:
                    return make_numeric(((left.numeric && right.numeric) ? (left.number > right.number) : (left.text > right.text)) ? 1.0 : 0.0);
                case BinaryOperator::GreaterEqual:
                    return make_numeric(((left.numeric && right.numeric) ? (left.number >= right.number) : (left.text >= right.text)) ? 1.0 : 0.0);
                case BinaryOperator::Like:
                    return make_numeric(like_matches(left.text, right.text) ? 1.0 : 0.0);
                case BinaryOperator::Regexp:
                    try
                    {
                        return make_numeric(std::regex_search(left.text, std::regex(right.text)) ? 1.0 : 0.0);
                    }
                    catch (const std::regex_error&)
                    {
                        fail("Invalid REGEXP pattern '" + right.text + "'");
                    }
                case BinaryOperator::Equal:
                    return make_numeric(((left.numeric && right.numeric) ? (std::fabs(left.number - right.number) < 1e-9) : (left.text == right.text)) ? 1.0 : 0.0);
                case BinaryOperator::NotEqual:
                    return make_numeric(((left.numeric && right.numeric) ? (std::fabs(left.number - right.number) >= 1e-9) : (left.text != right.text)) ? 1.0 : 0.0);
                case BinaryOperator::BitwiseAnd: return make_numeric(static_cast<double>(to_integer(left) & to_integer(right)));
                case BinaryOperator::BitwiseXor: return make_numeric(static_cast<double>(to_integer(left) ^ to_integer(right)));
                case BinaryOperator::BitwiseOr: return make_numeric(static_cast<double>(to_integer(left) | to_integer(right)));
                case BinaryOperator::LogicalAnd: return make_numeric((to_bool(left) && to_bool(right)) ? 1.0 : 0.0);
                case BinaryOperator::LogicalOr: return make_numeric((to_bool(left) || to_bool(right)) ? 1.0 : 0.0);
                }
                break;
            }
            }

            fail("Unsupported expression");
        }

        std::string evaluate_value(const ExpressionPtr& expression, const Table& table, const Row& row, const IStorage& storage)
        {
            return evaluate_expression(expression, table, row, storage).text;
        }

        std::string evaluate_value(const ExpressionPtr& expression, const IStorage& storage)
        {
            const Table empty_table{};
            const Row empty_row{};
            return evaluate_expression(expression, empty_table, empty_row, storage).text;
        }
    }

    Executor::Executor(std::shared_ptr<IStorage> storage, std::ostream& output)
        : storage_(std::move(storage)), output_(output)
    {
        if (!storage_)
        {
            fail("Storage backend must not be null");
        }
    }

    void Executor::execute(const Statement& statement)
    {
        switch (statement.kind)
        {
        case Statement::Kind::Create:
            execute_create(statement.create);
            break;
        case Statement::Kind::Drop:
            execute_drop(statement.drop);
            break;
        case Statement::Kind::Delete:
            execute_delete(statement.delete_statement);
            break;
        case Statement::Kind::Insert:
            execute_insert(statement.insert);
            break;
        case Statement::Kind::Select:
            execute_select(statement.select);
            break;
        case Statement::Kind::Update:
            execute_update(statement.update);
            break;
        }
    }

    void Executor::execute_create(const CreateStatement& stmt)
    {
        if (stmt.columns.empty())
        {
            fail("CREATE TABLE requires at least one column");
        }

        const auto path = storage_->table_path(stmt.table_name);
        if (std::filesystem::exists(path))
        {
            fail("Table already exists: " + stmt.table_name);
        }

        try
        {
            static_cast<void>(storage_->load_table(stmt.table_name));
        }
        catch (const std::runtime_error& error)
        {
            if (error.what() != std::string("Table does not exist: ") + stmt.table_name)
            {
                throw;
            }

            Table table;
            table.name = stmt.table_name;
            table.columns.reserve(stmt.columns.size());
            for (const auto& column : stmt.columns)
            {
                std::string stored_name = column.name;
                if (column.auto_increment)
                {
                    stored_name += " AUTO_INCREMENT";
                }
                if (column.default_value)
                {
                    stored_name += " DEFAULT(" + serialize_expression(column.default_value) + ")";
                }
                table.columns.push_back(std::move(stored_name));
            }
            storage_->save_table(table);
            output_ << "Created table '" << stmt.table_name << "'\n";
            return;
        }

        fail("Table already exists: " + stmt.table_name);
    }

    void Executor::execute_drop(const DropStatement& stmt)
    {
        storage_->delete_table(stmt.table_name);
        output_ << "Dropped table '" << stmt.table_name << "'\n";
    }

    void Executor::execute_delete(const DeleteStatement& stmt)
    {
        Table table = storage_->load_table(stmt.table_name);
        const auto original_size = table.rows.size();

        table.rows.erase(
            std::remove_if(
                table.rows.begin(),
                table.rows.end(),
                [&](const Row& row)
                {
                    if (!stmt.where)
                    {
                        return true;
                    }
                    return to_bool(evaluate_expression(stmt.where, table, row, *storage_));
                }),
            table.rows.end());

        const auto deleted = original_size - table.rows.size();
        storage_->save_table(table);
        output_ << "Deleted " << deleted << " row(s) from '" << stmt.table_name << "'\n";
    }

    void Executor::execute_insert(const InsertStatement& stmt)
    {
        Table table = storage_->load_table(stmt.table_name);
        Row row(table.columns.size());
        std::vector<bool> assigned(table.columns.size(), false);

        if (stmt.columns.has_value())
        {
            if (stmt.columns->size() != stmt.values.size())
            {
                fail("INSERT column count does not match value count");
            }

            for (std::size_t i = 0; i < stmt.columns->size(); ++i)
            {
                const auto index = storage_->column_index(table, (*stmt.columns)[i]);
                row[index] = evaluate_value(stmt.values[i], *storage_);
                assigned[index] = true;
            }
        }
        else
        {
            if (stmt.values.size() > table.columns.size())
            {
                fail("INSERT value count does not match table column count");
            }

            for (std::size_t i = 0; i < stmt.values.size(); ++i)
            {
                row[i] = evaluate_value(stmt.values[i], *storage_);
                assigned[i] = true;
            }
        }

        std::size_t missing_without_default = 0;
        for (std::size_t i = 0; i < row.size(); ++i)
        {
            if (assigned[i])
            {
                continue;
            }

            const auto metadata = parse_column_metadata(table.columns[i]);
            if (!metadata.default_expression.empty())
            {
                Tokenizer tokenizer(metadata.default_expression);
                Parser parser(tokenizer.tokenize());
                row[i] = evaluate_value(parser.parse_expression(), *storage_);
            }
            else
            {
                row[i].clear();
                ++missing_without_default;
            }
        }

        if (!stmt.columns.has_value() && !stmt.values.empty() && missing_without_default > 0)
        {
            fail("INSERT value count does not match table column count");
        }

        if (const auto auto_increment_index = auto_increment_column_index(table); auto_increment_index.has_value() && row[*auto_increment_index].empty())
        {
            row[*auto_increment_index] = next_auto_increment_value(table, *auto_increment_index);
        }

        table.rows.push_back(std::move(row));
        storage_->save_table(table);
        output_ << "Inserted 1 row into '" << stmt.table_name << "'\n";
    }

    void Executor::execute_select(const SelectStatement& stmt)
    {
        const auto result = run_select_statement(stmt, *storage_);

        std::vector<std::size_t> widths(result.column_names.size(), 0);
        for (std::size_t i = 0; i < result.column_names.size(); ++i)
        {
            widths[i] = result.column_names[i].size();
        }
        for (const auto& row : result.rows)
        {
            for (std::size_t i = 0; i < row.size(); ++i)
            {
                widths[i] = std::max(widths[i], row[i].size());
            }
        }

        const auto separator = make_separator(widths);
        output_ << separator << '\n';
        write_row(output_, result.column_names, widths);
        output_ << separator << '\n';
        for (const auto& row : result.rows)
        {
            write_row(output_, row, widths);
        }
        output_ << separator << '\n';
        output_ << result.rows.size() << " row(s) selected\n";
    }

    void Executor::execute_update(const UpdateStatement& stmt)
    {
        Table table = storage_->load_table(stmt.table_name);
        std::vector<std::pair<std::size_t, ExpressionPtr>> assignments;
        assignments.reserve(stmt.assignments.size());
        for (const auto& [column, value] : stmt.assignments)
        {
            assignments.emplace_back(storage_->column_index(table, column), value);
        }

        std::size_t updated = 0;
        for (auto& row : table.rows)
        {
            if (stmt.where && !to_bool(evaluate_expression(stmt.where, table, row, *storage_)))
            {
                continue;
            }

            for (const auto& [index, value] : assignments)
            {
                row[index] = evaluate_value(value, table, row, *storage_);
            }
            ++updated;
        }

        storage_->save_table(table);
        output_ << "Updated " << updated << " row(s) in '" << stmt.table_name << "'\n";
    }
}
