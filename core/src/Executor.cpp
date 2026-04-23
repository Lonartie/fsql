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
#include <map>
#include <regex>
#include <set>
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
            bool is_null = false;
        };

        constexpr std::string_view null_storage_marker = "__CSV_SQL_NULL__";

        bool is_stored_null(const std::string& value)
        {
            return value == null_storage_marker;
        }

        std::string visible_value_text(const std::string& value)
        {
            return is_stored_null(value) ? "NULL" : value;
        }

        ExecutionTable make_visible_execution_table(ExecutionTable table)
        {
            for (auto& row : table.rows)
            {
                for (auto& value : row)
                {
                    value = visible_value_text(value);
                }
            }
            return table;
        }

        ExecutionResult make_success_result(ExecutionResultKind kind,
                                            std::size_t affected_rows,
                                            std::string message,
                                            std::optional<ExecutionTable> table = std::nullopt)
        {
            ExecutionResult result;
            result.success = true;
            result.kind = kind;
            result.affected_rows = affected_rows;
            result.message = std::move(message);
            result.table = std::move(table);
            return result;
        }

        struct ProjectedSelectRow
        {
            Row values;
            std::vector<EvaluatedValue> order_values;
        };

        struct SelectGroup
        {
            const Row* representative = nullptr;
            std::vector<const Row*> rows;
        };

        struct ResolvedSelectColumn
        {
            std::string source_name;
            std::string column_name;
        };

        struct ResolvedSelectTable
        {
            std::vector<ResolvedSelectColumn> columns;
            std::vector<Row> rows;
            std::size_t source_count = 0;
        };

        struct MaterializedSelectSource
        {
            std::string source_name;
            std::vector<std::string> column_names;
            std::vector<Row> rows;
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
            if (value.is_null)
            {
                return false;
            }
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

        EvaluatedValue make_null()
        {
            return {std::string(null_storage_marker), false, 0.0, true};
        }

        EvaluatedValue make_text(std::string value)
        {
            if (is_stored_null(value))
            {
                return make_null();
            }

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

        EvaluatedValue apply_unary_operator(UnaryOperator op, const EvaluatedValue& operand)
        {
            switch (op)
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

            fail("Unsupported unary operator");
        }

        EvaluatedValue apply_binary_operator(BinaryOperator op, const EvaluatedValue& left, const EvaluatedValue& right)
        {
            switch (op)
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
                if (left.is_null || right.is_null)
                {
                    return make_numeric(0.0);
                }
                return make_numeric(((left.numeric && right.numeric) ? (left.number < right.number) : (left.text < right.text)) ? 1.0 : 0.0);
            case BinaryOperator::LessEqual:
                if (left.is_null || right.is_null)
                {
                    return make_numeric(0.0);
                }
                return make_numeric(((left.numeric && right.numeric) ? (left.number <= right.number) : (left.text <= right.text)) ? 1.0 : 0.0);
            case BinaryOperator::Greater:
                if (left.is_null || right.is_null)
                {
                    return make_numeric(0.0);
                }
                return make_numeric(((left.numeric && right.numeric) ? (left.number > right.number) : (left.text > right.text)) ? 1.0 : 0.0);
            case BinaryOperator::GreaterEqual:
                if (left.is_null || right.is_null)
                {
                    return make_numeric(0.0);
                }
                return make_numeric(((left.numeric && right.numeric) ? (left.number >= right.number) : (left.text >= right.text)) ? 1.0 : 0.0);
            case BinaryOperator::Is:
                return make_numeric((left.is_null && right.is_null) ? 1.0 : 0.0);
            case BinaryOperator::IsNot:
                return make_numeric((left.is_null && right.is_null) ? 0.0 : 1.0);
            case BinaryOperator::In:
                fail("IN subqueries require dedicated evaluation");
            case BinaryOperator::Like:
                if (left.is_null || right.is_null)
                {
                    return make_numeric(0.0);
                }
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
                if (left.is_null || right.is_null)
                {
                    return make_numeric(0.0);
                }
                return make_numeric(((left.numeric && right.numeric) ? (std::fabs(left.number - right.number) < 1e-9) : (left.text == right.text)) ? 1.0 : 0.0);
            case BinaryOperator::NotEqual:
                if (left.is_null || right.is_null)
                {
                    return make_numeric(0.0);
                }
                return make_numeric(((left.numeric && right.numeric) ? (std::fabs(left.number - right.number) >= 1e-9) : (left.text != right.text)) ? 1.0 : 0.0);
            case BinaryOperator::BitwiseAnd: return make_numeric(static_cast<double>(to_integer(left) & to_integer(right)));
            case BinaryOperator::BitwiseXor: return make_numeric(static_cast<double>(to_integer(left) ^ to_integer(right)));
            case BinaryOperator::BitwiseOr: return make_numeric(static_cast<double>(to_integer(left) | to_integer(right)));
            case BinaryOperator::LogicalAnd: return make_numeric((to_bool(left) && to_bool(right)) ? 1.0 : 0.0);
            case BinaryOperator::LogicalOr: return make_numeric((to_bool(left) || to_bool(right)) ? 1.0 : 0.0);
            }

            fail("Unsupported binary operator");
        }

        bool evaluate_quantified_comparison(int comparison, BinaryOperator op)
        {
            switch (op)
            {
            case BinaryOperator::Less:
                return comparison < 0;
            case BinaryOperator::LessEqual:
                return comparison <= 0;
            case BinaryOperator::Greater:
                return comparison > 0;
            case BinaryOperator::GreaterEqual:
                return comparison >= 0;
            case BinaryOperator::Equal:
                return comparison == 0;
            case BinaryOperator::NotEqual:
                return comparison != 0;
            default:
                fail("ANY and ALL require comparison operators");
            }
        }

        std::string serialize_expression(const ExpressionPtr& expression);

        std::string serialize_select_statement(const SelectStatement& statement);

        EvaluatedValue evaluate_expression(const ExpressionPtr& expression, const Table& table, const Row& row, const IStorage& storage);

        EvaluatedValue evaluate_select_expression(const SelectStatement& stmt, const IStorage& storage);

        bool evaluate_exists_expression(const SelectStatement& stmt, const IStorage& storage);

        EvaluatedValue evaluate_in_subquery(const EvaluatedValue& left, const ExpressionPtr& right, const IStorage& storage);

        EvaluatedValue evaluate_quantified_subquery(const EvaluatedValue& left,
                                                    BinaryOperator op,
                                                    SubqueryQuantifier quantifier,
                                                    const ExpressionPtr& right,
                                                    const IStorage& storage);

        ExecutionTable run_select_statement(const SelectStatement& stmt, const IStorage& storage);

        thread_local std::vector<std::string> active_view_stack;

        bool has_active_view(const std::string& view_name)
        {
            return std::any_of(active_view_stack.begin(), active_view_stack.end(), [&](const auto& active_name)
            {
                return iequals(active_name, view_name);
            });
        }

        std::string describe_view_cycle(const std::string& view_name)
        {
            std::ostringstream stream;
            for (std::size_t i = 0; i < active_view_stack.size(); ++i)
            {
                if (i > 0)
                {
                    stream << " -> ";
                }
                stream << active_view_stack[i];
            }
            if (!active_view_stack.empty())
            {
                stream << " -> ";
            }
            stream << view_name;
            return stream.str();
        }

        class ActiveViewGuard
        {
        public:
            explicit ActiveViewGuard(std::string view_name) : view_name_(std::move(view_name))
            {
                if (has_active_view(view_name_))
                {
                    fail("Cyclic view reference detected: " + describe_view_cycle(view_name_));
                }
                active_view_stack.push_back(view_name_);
            }

            ~ActiveViewGuard()
            {
                if (!active_view_stack.empty())
                {
                    active_view_stack.pop_back();
                }
            }

        private:
            std::string view_name_;
        };

        SelectStatement parse_view_statement(const ViewDefinition& view)
        {
            Tokenizer tokenizer(view.select_statement);
            Parser parser(tokenizer.tokenize());
            const auto statement = parser.parse_statement();
            if (statement.kind != Statement::Kind::Select)
            {
                fail("View definition must contain a SELECT statement: " + view.name);
            }
            return statement.select;
        }

        ExecutionTable run_view_statement(const std::string& view_name, const IStorage& storage)
        {
            ActiveViewGuard guard(view_name);
            return run_select_statement(parse_view_statement(storage.load_view(view_name)), storage);
        }

        void validate_view_definition(const std::string& view_name, const IStorage& storage)
        {
            static_cast<void>(run_view_statement(view_name, storage));
        }

        std::string serialize_select_source(const SelectSource& source)
        {
            std::ostringstream stream;
            if (source.kind == SelectSource::Kind::Table)
            {
                stream << source.name;
            }
            else
            {
                if (!source.subquery)
                {
                    fail("Missing SELECT source payload");
                }
                stream << '(' << serialize_select_statement(*source.subquery) << ')';
            }

            if (source.alias.has_value())
            {
                stream << ' ' << *source.alias;
            }

            return stream.str();
        }

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

            stream << " FROM ";
            for (std::size_t i = 0; i < statement.sources.size(); ++i)
            {
                if (i > 0)
                {
                    stream << ", ";
                }
                stream << serialize_select_source(statement.sources[i]);
            }
            if (statement.where)
            {
                stream << " WHERE " << serialize_expression(statement.where);
            }

            if (!statement.group_by.empty())
            {
                stream << " GROUP BY ";
                for (std::size_t i = 0; i < statement.group_by.size(); ++i)
                {
                    if (i > 0)
                    {
                        stream << ", ";
                    }
                    stream << serialize_expression(statement.group_by[i]);
                }
            }

            if (statement.having)
            {
                stream << " HAVING " << serialize_expression(statement.having);
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
            case ExpressionKind::Null:
                return "NULL";
            case ExpressionKind::Identifier:
                return expression->text;
            case ExpressionKind::Select:
                if (!expression->select)
                {
                    fail("Missing SELECT expression payload");
                }
                return "(" + serialize_select_statement(*expression->select) + ")";
            case ExpressionKind::Exists:
                if (!expression->select)
                {
                    fail("Missing EXISTS expression payload");
                }
                return "EXISTS (" + serialize_select_statement(*expression->select) + ")";
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
                case BinaryOperator::Is: op = "IS"; break;
                case BinaryOperator::IsNot: op = "IS NOT"; break;
                case BinaryOperator::In: op = "IN"; break;
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

                if (expression->subquery_quantifier != SubqueryQuantifier::None)
                {
                    if (!expression->right || expression->right->kind != ExpressionKind::Select)
                    {
                        fail("Quantified comparisons require a SELECT subquery");
                    }

                    const auto quantifier = expression->subquery_quantifier == SubqueryQuantifier::Any ? "ANY" : "ALL";
                    return "(" + serialize_expression(expression->left) + " " + op + " " + quantifier + " " + serialize_expression(expression->right) + ")";
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

        std::string serialize_column_metadata(const ParsedColumnMetadata& metadata)
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

        bool has_visible_column_name(const Table& table, const std::string& column_name, std::optional<std::size_t> skip_index = std::nullopt)
        {
            for (std::size_t i = 0; i < table.columns.size(); ++i)
            {
                if (skip_index.has_value() && *skip_index == i)
                {
                    continue;
                }
                if (iequals(parse_column_metadata(table.columns[i]).visible_name, column_name))
                {
                    return true;
                }
            }
            return false;
        }

        void ensure_single_auto_increment_column(const Table& table, std::optional<std::size_t> skip_index = std::nullopt)
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
                if (row[index].empty() || is_stored_null(row[index]))
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
                if (!row[index].empty() && !is_stored_null(row[index]))
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

        std::string visible_column_name(const std::string& stored_name)
        {
            return parse_column_metadata(stored_name).visible_name;
        }

        std::string next_auto_increment_value(const Table& table, std::size_t index)
        {
            long long maximum = 0;
            for (const auto& row : table.rows)
            {
                if (row[index].empty() || is_stored_null(row[index]))
                {
                    continue;
                }
                maximum = std::max(maximum, std::stoll(row[index]));
            }
            return std::to_string(maximum + 1);
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
            case ExpressionKind::Exists:
            case ExpressionKind::Select:
            case ExpressionKind::Literal:
            case ExpressionKind::Null:
            case ExpressionKind::Identifier:
                return false;
            }

            return false;
        }

        int compare_values(const EvaluatedValue& left, const EvaluatedValue& right)
        {
            if (left.is_null && right.is_null)
            {
                return 0;
            }
            if (left.is_null)
            {
                return -1;
            }
            if (right.is_null)
            {
                return 1;
            }

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

        std::size_t resolve_select_column_index(const ResolvedSelectTable& table, const std::string& identifier)
        {
            const auto dot = identifier.find('.');
            std::optional<std::string> source_name;
            std::string column_name = identifier;
            if (dot != std::string::npos)
            {
                source_name = identifier.substr(0, dot);
                column_name = identifier.substr(dot + 1);
            }

            std::optional<std::size_t> resolved_index;
            for (std::size_t i = 0; i < table.columns.size(); ++i)
            {
                const auto& column = table.columns[i];
                if (!iequals(column.column_name, column_name))
                {
                    continue;
                }
                if (source_name.has_value() && !iequals(column.source_name, *source_name))
                {
                    continue;
                }
                if (resolved_index.has_value())
                {
                    fail((source_name.has_value() ? "Ambiguous qualified column '" : "Ambiguous column '") + identifier + "'");
                }
                resolved_index = i;
            }

            if (!resolved_index.has_value())
            {
                fail("Unknown column '" + identifier + "' in SELECT sources");
            }
            return *resolved_index;
        }

        EvaluatedValue evaluate_select_identifier(const std::string& identifier, const ResolvedSelectTable& table, const Row& row)
        {
            const auto dot = identifier.find('.');
            std::optional<std::string> source_name;
            std::string column_name = identifier;
            if (dot != std::string::npos)
            {
                source_name = identifier.substr(0, dot);
                column_name = identifier.substr(dot + 1);
            }

            std::optional<std::size_t> resolved_index;
            for (std::size_t i = 0; i < table.columns.size(); ++i)
            {
                const auto& column = table.columns[i];
                if (!iequals(column.column_name, column_name))
                {
                    continue;
                }
                if (source_name.has_value() && !iequals(column.source_name, *source_name))
                {
                    continue;
                }
                if (resolved_index.has_value())
                {
                    fail((source_name.has_value() ? "Ambiguous qualified column '" : "Ambiguous column '") + identifier + "'");
                }
                resolved_index = i;
            }

            if (!resolved_index.has_value())
            {
                if (source_name.has_value())
                {
                    fail("Unknown column '" + identifier + "' in SELECT sources");
                }
                return make_text(identifier);
            }
            return make_text(row[*resolved_index]);
        }

        std::string select_star_column_name(const ResolvedSelectTable& table, std::size_t index)
        {
            if (table.source_count > 1)
            {
                return table.columns[index].source_name + "." + table.columns[index].column_name;
            }
            return table.columns[index].column_name;
        }

        MaterializedSelectSource materialize_select_source(const SelectSource& source, const IStorage& storage)
        {
            MaterializedSelectSource materialized;
            if (source.kind == SelectSource::Kind::Table)
            {
                const bool has_table = storage.has_table(source.name);
                const bool has_view = storage.has_view(source.name);
                if (has_table && has_view)
                {
                    fail("Name collision between table and view: " + source.name);
                }

                materialized.source_name = source.alias.value_or(source.name);
                if (has_view)
                {
                    const auto result = run_view_statement(source.name, storage);
                    materialized.column_names = result.column_names;
                    materialized.rows = result.rows;
                    return materialized;
                }

                const Table table = storage.load_table(source.name);
                materialized.column_names.reserve(table.columns.size());
                for (const auto& column : table.columns)
                {
                    materialized.column_names.push_back(visible_column_name(column));
                }
                materialized.rows = table.rows;
                return materialized;
            }

            if (!source.subquery)
            {
                fail("Missing SELECT source payload");
            }
            if (!source.alias.has_value())
            {
                fail("SELECT subquery sources require an alias");
            }

            const auto result = run_select_statement(*source.subquery, storage);
            materialized.source_name = *source.alias;
            materialized.column_names = result.column_names;
            materialized.rows = result.rows;
            return materialized;
        }

        ResolvedSelectTable materialize_select_table(const SelectStatement& stmt, const IStorage& storage)
        {
            if (stmt.sources.empty())
            {
                fail("SELECT requires at least one source");
            }

            std::vector<MaterializedSelectSource> sources;
            sources.reserve(stmt.sources.size());
            for (const auto& source : stmt.sources)
            {
                auto materialized = materialize_select_source(source, storage);
                for (const auto& existing : sources)
                {
                    if (iequals(existing.source_name, materialized.source_name))
                    {
                        fail("Duplicate SELECT source name '" + materialized.source_name + "'");
                    }
                }
                sources.push_back(std::move(materialized));
            }

            ResolvedSelectTable resolved;
            resolved.source_count = sources.size();
            for (const auto& source : sources)
            {
                for (const auto& column_name : source.column_names)
                {
                    resolved.columns.push_back({source.source_name, column_name});
                }
            }

            resolved.rows.push_back({});
            for (const auto& source : sources)
            {
                if (source.rows.empty())
                {
                    resolved.rows.clear();
                    break;
                }

                std::vector<Row> next_rows;
                next_rows.reserve(resolved.rows.size() * source.rows.size());
                for (const auto& prefix : resolved.rows)
                {
                    for (const auto& source_row : source.rows)
                    {
                        Row row = prefix;
                        row.insert(row.end(), source_row.begin(), source_row.end());
                        next_rows.push_back(std::move(row));
                    }
                }
                resolved.rows = std::move(next_rows);
            }

            return resolved;
        }

        void validate_select_projection(const ExpressionPtr& expression, const ResolvedSelectTable& table)
        {
            if (!expression)
            {
                return;
            }

            switch (expression->kind)
            {
            case ExpressionKind::Identifier:
                static_cast<void>(resolve_select_column_index(table, expression->text));
                return;
            case ExpressionKind::Exists:
                return;
            case ExpressionKind::FunctionCall:
                for (const auto& argument : expression->arguments)
                {
                    validate_select_projection(argument, table);
                }
                return;
            case ExpressionKind::Unary:
                validate_select_projection(expression->left, table);
                return;
            case ExpressionKind::Binary:
                validate_select_projection(expression->left, table);
                validate_select_projection(expression->right, table);
                return;
            case ExpressionKind::Select:
            case ExpressionKind::Literal:
            case ExpressionKind::Null:
                return;
            }
        }

        std::set<std::size_t> collect_group_by_column_indexes(const SelectStatement& stmt, const ResolvedSelectTable& table)
        {
            std::set<std::size_t> identifiers;
            for (const auto& expression : stmt.group_by)
            {
                if (!expression || expression->kind != ExpressionKind::Identifier)
                {
                    fail("GROUP BY currently only supports column identifiers");
                }

                identifiers.insert(resolve_select_column_index(table, expression->text));
            }
            return identifiers;
        }

        void validate_grouped_expression(const ExpressionPtr& expression,
                                         const ResolvedSelectTable& table,
                                         const std::set<std::size_t>& group_by_identifiers,
                                         bool inside_aggregate = false)
        {
            if (!expression)
            {
                return;
            }

            switch (expression->kind)
            {
            case ExpressionKind::Literal:
            case ExpressionKind::Null:
            case ExpressionKind::Exists:
            case ExpressionKind::Select:
                return;
            case ExpressionKind::Identifier:
            {
                const auto index = resolve_select_column_index(table, expression->text);
                if (!inside_aggregate && !group_by_identifiers.contains(index))
                {
                    fail("Grouped query references non-grouped column '" + expression->text + "'");
                }
                return;
            }
            case ExpressionKind::FunctionCall:
                if (is_aggregate_function_name(expression->text))
                {
                    for (const auto& argument : expression->arguments)
                    {
                        validate_grouped_expression(argument, table, group_by_identifiers, true);
                    }
                    return;
                }

                for (const auto& argument : expression->arguments)
                {
                    validate_grouped_expression(argument, table, group_by_identifiers, inside_aggregate);
                }
                return;
            case ExpressionKind::Unary:
                validate_grouped_expression(expression->left, table, group_by_identifiers, inside_aggregate);
                return;
            case ExpressionKind::Binary:
                validate_grouped_expression(expression->left, table, group_by_identifiers, inside_aggregate);
                validate_grouped_expression(expression->right, table, group_by_identifiers, inside_aggregate);
                return;
            }
        }

        EvaluatedValue evaluate_select_row_expression(const ExpressionPtr& expression, const ResolvedSelectTable& table, const Row& row, const IStorage& storage)
        {
            if (!expression)
            {
                return make_text("");
            }

            switch (expression->kind)
            {
            case ExpressionKind::Literal:
                return make_text(expression->text);
            case ExpressionKind::Null:
                return make_null();
            case ExpressionKind::Identifier:
                return evaluate_select_identifier(expression->text, table, row);
            case ExpressionKind::Select:
                if (!expression->select)
                {
                    fail("Missing SELECT expression payload");
                }
                return evaluate_select_expression(*expression->select, storage);
            case ExpressionKind::Exists:
                if (!expression->select)
                {
                    fail("Missing EXISTS expression payload");
                }
                return make_numeric(evaluate_exists_expression(*expression->select, storage) ? 1.0 : 0.0);
            case ExpressionKind::FunctionCall:
                return evaluate_function(*expression);
            case ExpressionKind::Unary:
                return apply_unary_operator(expression->unary_operator, evaluate_select_row_expression(expression->left, table, row, storage));
            case ExpressionKind::Binary:
                if (expression->subquery_quantifier != SubqueryQuantifier::None)
                {
                    return evaluate_quantified_subquery(evaluate_select_row_expression(expression->left, table, row, storage),
                                                        expression->binary_operator,
                                                        expression->subquery_quantifier,
                                                        expression->right,
                                                        storage);
                }
                if (expression->binary_operator == BinaryOperator::In)
                {
                    return evaluate_in_subquery(evaluate_select_row_expression(expression->left, table, row, storage), expression->right, storage);
                }
                return apply_binary_operator(expression->binary_operator,
                                             evaluate_select_row_expression(expression->left, table, row, storage),
                                             evaluate_select_row_expression(expression->right, table, row, storage));
            }

            fail("Unsupported select expression");
        }

        std::vector<const Row*> filter_rows(const ResolvedSelectTable& table, const ExpressionPtr& where, const IStorage& storage)
        {
            std::vector<const Row*> rows;
            rows.reserve(table.rows.size());
            for (const auto& row : table.rows)
            {
                if (where && !to_bool(evaluate_select_row_expression(where, table, row, storage)))
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

        EvaluatedValue evaluate_aggregate_function(const ExpressionPtr& expression, const ResolvedSelectTable& table, const std::vector<const Row*>& rows, const IStorage& storage)
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
                    const auto value = evaluate_select_row_expression(expression->arguments[0], table, *row, storage);
                    if (!value.is_null && !value.text.empty())
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
                    const auto value = evaluate_select_row_expression(expression->arguments[0], table, *row, storage);
                    if (value.is_null || value.text.empty())
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
                const auto value = evaluate_select_row_expression(expression->arguments[0], table, *row, storage);
                if (value.is_null || value.text.empty())
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

        EvaluatedValue evaluate_grouped_expression(const ExpressionPtr& expression,
                                                   const ResolvedSelectTable& table,
                                                   const Row& representative_row,
                                                   const std::vector<const Row*>& rows,
                                                   const IStorage& storage)
        {
            if (!expression)
            {
                return make_text("");
            }

            switch (expression->kind)
            {
            case ExpressionKind::Literal:
                return make_text(expression->text);
            case ExpressionKind::Null:
                return make_null();
            case ExpressionKind::Identifier:
                return evaluate_select_identifier(expression->text, table, representative_row);
            case ExpressionKind::Select:
                if (!expression->select)
                {
                    fail("Missing SELECT expression payload");
                }
                return evaluate_select_expression(*expression->select, storage);
            case ExpressionKind::Exists:
                if (!expression->select)
                {
                    fail("Missing EXISTS expression payload");
                }
                return make_numeric(evaluate_exists_expression(*expression->select, storage) ? 1.0 : 0.0);
            case ExpressionKind::FunctionCall:
                if (is_aggregate_function_name(expression->text))
                {
                    return evaluate_aggregate_function(expression, table, rows, storage);
                }
                return evaluate_function(*expression);
            case ExpressionKind::Unary:
                return apply_unary_operator(expression->unary_operator, evaluate_grouped_expression(expression->left, table, representative_row, rows, storage));
            case ExpressionKind::Binary:
                if (expression->subquery_quantifier != SubqueryQuantifier::None)
                {
                    return evaluate_quantified_subquery(evaluate_grouped_expression(expression->left, table, representative_row, rows, storage),
                                                        expression->binary_operator,
                                                        expression->subquery_quantifier,
                                                        expression->right,
                                                        storage);
                }
                if (expression->binary_operator == BinaryOperator::In)
                {
                    return evaluate_in_subquery(evaluate_grouped_expression(expression->left, table, representative_row, rows, storage), expression->right, storage);
                }
                return apply_binary_operator(expression->binary_operator,
                                             evaluate_grouped_expression(expression->left, table, representative_row, rows, storage),
                                             evaluate_grouped_expression(expression->right, table, representative_row, rows, storage));
            }

            fail("Unsupported grouped expression");
        }

        std::vector<SelectGroup> build_select_groups(const SelectStatement& stmt,
                                                     const ResolvedSelectTable& table,
                                                     const std::vector<const Row*>& rows,
                                                     const IStorage& storage)
        {
            std::vector<SelectGroup> groups;
            std::map<Row, std::size_t> key_to_group_index;

            for (const auto* row : rows)
            {
                Row key;
                key.reserve(stmt.group_by.size());
                for (const auto& expression : stmt.group_by)
                {
                    key.push_back(evaluate_select_row_expression(expression, table, *row, storage).text);
                }

                const auto [iterator, inserted] = key_to_group_index.emplace(key, groups.size());
                if (inserted)
                {
                    SelectGroup group;
                    group.representative = row;
                    groups.push_back(std::move(group));
                }
                groups[iterator->second].rows.push_back(row);
            }

            return groups;
        }

        ExecutionTable run_select_statement(const SelectStatement& stmt, const IStorage& storage)
        {
            const auto table = materialize_select_table(stmt, storage);
            const auto rows = filter_rows(table, stmt.where, storage);

            if (stmt.having && stmt.group_by.empty())
            {
                fail("HAVING requires GROUP BY");
            }

            if (!stmt.group_by.empty())
            {
                if (stmt.select_all)
                {
                    fail("SELECT * is not supported with GROUP BY");
                }

                const auto group_by_identifiers = collect_group_by_column_indexes(stmt, table);
                ExecutionTable result;
                for (const auto& projection : stmt.projections)
                {
                    validate_grouped_expression(projection, table, group_by_identifiers);
                    result.column_names.push_back(serialize_expression(projection));
                }

                for (const auto& order_by : stmt.order_by)
                {
                    validate_grouped_expression(order_by.expression, table, group_by_identifiers);
                }

                if (stmt.having)
                {
                    validate_grouped_expression(stmt.having, table, group_by_identifiers);
                }

                std::vector<ProjectedSelectRow> projected_rows;
                const auto groups = build_select_groups(stmt, table, rows, storage);
                projected_rows.reserve(groups.size());

                for (const auto& group : groups)
                {
                    if (stmt.having && !to_bool(evaluate_grouped_expression(stmt.having, table, *group.representative, group.rows, storage)))
                    {
                        continue;
                    }

                    ProjectedSelectRow projected_row;
                    projected_row.values.reserve(stmt.projections.size());
                    for (const auto& projection : stmt.projections)
                    {
                        projected_row.values.push_back(evaluate_grouped_expression(projection, table, *group.representative, group.rows, storage).text);
                    }
                    projected_row.order_values.reserve(stmt.order_by.size());
                    for (const auto& order_by : stmt.order_by)
                    {
                        projected_row.order_values.push_back(evaluate_grouped_expression(order_by.expression, table, *group.representative, group.rows, storage));
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

            for (const auto& order_by : stmt.order_by)
            {
                validate_select_projection(order_by.expression, table);
            }

            std::vector<ProjectedSelectRow> projected_rows;
            projected_rows.reserve(rows.size());

            if (stmt.select_all)
            {
                ExecutionTable result;
                for (std::size_t i = 0; i < table.columns.size(); ++i)
                {
                    result.column_names.push_back(select_star_column_name(table, i));
                }

                for (const auto* row : rows)
                {
                    ProjectedSelectRow projected_row;
                    projected_row.values = *row;
                    projected_row.order_values.reserve(stmt.order_by.size());
                    for (const auto& order_by : stmt.order_by)
                    {
                        projected_row.order_values.push_back(evaluate_select_row_expression(order_by.expression, table, *row, storage));
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

            ExecutionTable result;
            bool has_aggregate_projection = false;
            for (const auto& projection : stmt.projections)
            {
                validate_select_projection(projection, table);
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
                    projected_row.values.push_back(evaluate_select_row_expression(projection, table, *row, storage).text);
                }
                projected_row.order_values.reserve(stmt.order_by.size());
                for (const auto& order_by : stmt.order_by)
                {
                    projected_row.order_values.push_back(evaluate_select_row_expression(order_by.expression, table, *row, storage));
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

        bool evaluate_exists_expression(const SelectStatement& stmt, const IStorage& storage)
        {
            const auto result = run_select_statement(stmt, storage);
            return !result.rows.empty();
        }

        EvaluatedValue evaluate_in_subquery(const EvaluatedValue& left, const ExpressionPtr& right, const IStorage& storage)
        {
            if (!right || right->kind != ExpressionKind::Select || !right->select)
            {
                fail("IN currently requires a SELECT subquery");
            }

            const auto result = run_select_statement(*right->select, storage);
            if (result.column_names.size() != 1)
            {
                fail("IN subquery must return exactly one column");
            }

            if (left.is_null)
            {
                return make_numeric(0.0);
            }

            for (const auto& row : result.rows)
            {
                if (row.empty())
                {
                    continue;
                }
                const auto candidate = make_text(row[0]);
                if (candidate.is_null)
                {
                    continue;
                }
                if (compare_values(left, candidate) == 0)
                {
                    return make_numeric(1.0);
                }
            }

            return make_numeric(0.0);
        }

        EvaluatedValue evaluate_quantified_subquery(const EvaluatedValue& left,
                                                    BinaryOperator op,
                                                    SubqueryQuantifier quantifier,
                                                    const ExpressionPtr& right,
                                                    const IStorage& storage)
        {
            if (quantifier == SubqueryQuantifier::None)
            {
                fail("Missing ANY/ALL quantifier");
            }
            if (!right || right->kind != ExpressionKind::Select || !right->select)
            {
                fail("ANY and ALL currently require a SELECT subquery");
            }

            const auto result = run_select_statement(*right->select, storage);
            if (result.column_names.size() != 1)
            {
                fail("ANY/ALL subquery must return exactly one column");
            }

            if (left.is_null)
            {
                return make_numeric(0.0);
            }

            if (quantifier == SubqueryQuantifier::Any)
            {
                for (const auto& row : result.rows)
                {
                    if (row.empty())
                    {
                        continue;
                    }
                    const auto candidate = make_text(row[0]);
                    if (candidate.is_null)
                    {
                        continue;
                    }
                    if (evaluate_quantified_comparison(compare_values(left, candidate), op))
                    {
                        return make_numeric(1.0);
                    }
                }
                return make_numeric(0.0);
            }

            for (const auto& row : result.rows)
            {
                if (row.empty())
                {
                    continue;
                }
                const auto candidate = make_text(row[0]);
                if (candidate.is_null)
                {
                    continue;
                }
                if (!evaluate_quantified_comparison(compare_values(left, candidate), op))
                {
                    return make_numeric(0.0);
                }
            }

            return make_numeric(1.0);
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
            case ExpressionKind::Null:
                return make_null();
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
            case ExpressionKind::Exists:
                if (!expression->select)
                {
                    fail("Missing EXISTS expression payload");
                }
                return make_numeric(evaluate_exists_expression(*expression->select, storage) ? 1.0 : 0.0);
            case ExpressionKind::FunctionCall:
                return evaluate_function(*expression);
            case ExpressionKind::Unary:
                return apply_unary_operator(expression->unary_operator, evaluate_expression(expression->left, table, row, storage));
            case ExpressionKind::Binary:
                if (expression->subquery_quantifier != SubqueryQuantifier::None)
                {
                    return evaluate_quantified_subquery(evaluate_expression(expression->left, table, row, storage),
                                                        expression->binary_operator,
                                                        expression->subquery_quantifier,
                                                        expression->right,
                                                        storage);
                }
                if (expression->binary_operator == BinaryOperator::In)
                {
                    return evaluate_in_subquery(evaluate_expression(expression->left, table, row, storage), expression->right, storage);
                }
                return apply_binary_operator(expression->binary_operator,
                                             evaluate_expression(expression->left, table, row, storage),
                                             evaluate_expression(expression->right, table, row, storage));
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

    Executor::Executor(std::shared_ptr<IStorage> storage)
        : storage_(std::move(storage))
    {
        if (!storage_)
        {
            fail("Storage backend must not be null");
        }
    }

    ExecutionResult Executor::execute(const Statement& statement)
    {
        try
        {
            switch (statement.kind)
            {
            case Statement::Kind::Alter:
                return execute_alter(statement.alter);
            case Statement::Kind::Create:
                return execute_create(statement.create);
            case Statement::Kind::Drop:
                return execute_drop(statement.drop);
            case Statement::Kind::Delete:
                return execute_delete(statement.delete_statement);
            case Statement::Kind::Insert:
                return execute_insert(statement.insert);
            case Statement::Kind::Select:
                return execute_select(statement.select);
            case Statement::Kind::Update:
                return execute_update(statement.update);
            }
        }
        catch (const std::exception& ex)
        {
            ExecutionResult result;
            result.success = false;
            result.error = ex.what();
            return result;
        }

        ExecutionResult result;
        result.success = false;
        result.error = "Unsupported statement kind";
        return result;
    }

    ExecutionResult Executor::execute_alter(const AlterStatement& stmt)
    {
        const bool has_table = storage_->has_table(stmt.table_name);
        const bool has_view = storage_->has_view(stmt.table_name);
        if (has_table && has_view)
        {
            fail("Name collision between table and view: " + stmt.table_name);
        }

        if (stmt.object_kind == SchemaObjectKind::View)
        {
            if (stmt.action != AlterAction::SetViewQuery)
            {
                fail("Unsupported ALTER VIEW action");
            }
            if (!stmt.view_query)
            {
                fail("ALTER VIEW requires a SELECT statement");
            }
            if (has_table)
            {
                fail("Cannot ALTER VIEW because '" + stmt.table_name + "' is a table");
            }

            const auto previous_view = storage_->load_view(stmt.table_name);

            ViewDefinition view;
            view.name = stmt.table_name;
            view.select_statement = serialize_select_statement(*stmt.view_query);
            storage_->save_view(view);

            try
            {
                validate_view_definition(stmt.table_name, *storage_);
            }
            catch (...)
            {
                storage_->save_view(previous_view);
                throw;
            }

            return make_success_result(ExecutionResultKind::Alter, 0, "Altered view '" + stmt.table_name + "'");
        }

        if (has_view)
        {
            fail("Cannot ALTER TABLE on view: " + stmt.table_name);
        }

        Table table = storage_->load_table(stmt.table_name);

        auto find_column_index = [&]() -> std::size_t
        {
            return storage_->column_index(table, stmt.column_name);
        };

        switch (stmt.action)
        {
        case AlterAction::AddColumn:
        {
            if (stmt.column.name.empty())
            {
                fail("ALTER TABLE ADD COLUMN requires a column name");
            }
            if (has_visible_column_name(table, stmt.column.name))
            {
                fail("Column already exists: " + stmt.column.name);
            }

            ParsedColumnMetadata metadata;
            metadata.visible_name = stmt.column.name;
            metadata.auto_increment = stmt.column.auto_increment;
            if (stmt.column.default_value)
            {
                metadata.default_expression = serialize_expression(stmt.column.default_value);
            }

            table.columns.push_back(serialize_column_metadata(metadata));
            for (auto& row : table.rows)
            {
                if (stmt.column.default_value)
                {
                    row.push_back(evaluate_value(stmt.column.default_value, *storage_));
                }
                else
                {
                    row.emplace_back();
                }
            }

            if (metadata.auto_increment)
            {
                ensure_single_auto_increment_column(table);
                backfill_auto_increment_column(table, table.columns.size() - 1);
            }

            storage_->save_table(table);
            return make_success_result(ExecutionResultKind::Alter, 0, "Altered table '" + stmt.table_name + "'");
        }
        case AlterAction::DropColumn:
        {
            if (table.columns.size() <= 1)
            {
                fail("ALTER TABLE DROP COLUMN cannot remove the last column");
            }

            const auto index = find_column_index();
            table.columns.erase(table.columns.begin() + static_cast<std::ptrdiff_t>(index));
            for (auto& row : table.rows)
            {
                row.erase(row.begin() + static_cast<std::ptrdiff_t>(index));
            }

            storage_->save_table(table);
            return make_success_result(ExecutionResultKind::Alter, 0, "Altered table '" + stmt.table_name + "'");
        }
        case AlterAction::RenameColumn:
        {
            const auto index = find_column_index();
            if (stmt.new_name.empty())
            {
                fail("ALTER TABLE RENAME COLUMN requires a new column name");
            }
            if (has_visible_column_name(table, stmt.new_name, index))
            {
                fail("Column already exists: " + stmt.new_name);
            }

            auto metadata = parse_column_metadata(table.columns[index]);
            metadata.visible_name = stmt.new_name;
            table.columns[index] = serialize_column_metadata(metadata);

            storage_->save_table(table);
            return make_success_result(ExecutionResultKind::Alter, 0, "Altered table '" + stmt.table_name + "'");
        }
        case AlterAction::SetDefault:
        {
            const auto index = find_column_index();
            auto metadata = parse_column_metadata(table.columns[index]);
            if (!stmt.column.default_value)
            {
                fail("ALTER COLUMN SET DEFAULT requires an expression");
            }
            metadata.default_expression = serialize_expression(stmt.column.default_value);
            table.columns[index] = serialize_column_metadata(metadata);

            storage_->save_table(table);
            return make_success_result(ExecutionResultKind::Alter, 0, "Altered table '" + stmt.table_name + "'");
        }
        case AlterAction::DropDefault:
        {
            const auto index = find_column_index();
            auto metadata = parse_column_metadata(table.columns[index]);
            metadata.default_expression.clear();
            table.columns[index] = serialize_column_metadata(metadata);

            storage_->save_table(table);
            return make_success_result(ExecutionResultKind::Alter, 0, "Altered table '" + stmt.table_name + "'");
        }
        case AlterAction::SetAutoIncrement:
        {
            const auto index = find_column_index();
            auto metadata = parse_column_metadata(table.columns[index]);
            metadata.auto_increment = true;
            table.columns[index] = serialize_column_metadata(metadata);
            ensure_single_auto_increment_column(table);
            backfill_auto_increment_column(table, index);

            storage_->save_table(table);
            return make_success_result(ExecutionResultKind::Alter, 0, "Altered table '" + stmt.table_name + "'");
        }
        case AlterAction::DropAutoIncrement:
        {
            const auto index = find_column_index();
            auto metadata = parse_column_metadata(table.columns[index]);
            metadata.auto_increment = false;
            table.columns[index] = serialize_column_metadata(metadata);

            storage_->save_table(table);
            return make_success_result(ExecutionResultKind::Alter, 0, "Altered table '" + stmt.table_name + "'");
        }
        case AlterAction::SetViewQuery:
            fail("ALTER VIEW action reached ALTER TABLE handler");
        }

        fail("Unsupported ALTER action");
    }

    ExecutionResult Executor::execute_create(const CreateStatement& stmt)
    {
        if (stmt.object_kind == SchemaObjectKind::View)
        {
            if (!stmt.view_query)
            {
                fail("CREATE VIEW requires a SELECT statement");
            }
            if (storage_->has_table(stmt.table_name))
            {
                fail("Table already exists: " + stmt.table_name);
            }
            if (storage_->has_view(stmt.table_name))
            {
                fail("View already exists: " + stmt.table_name);
            }

            ViewDefinition view;
            view.name = stmt.table_name;
            view.select_statement = serialize_select_statement(*stmt.view_query);
            storage_->save_view(view);

            try
            {
                validate_view_definition(stmt.table_name, *storage_);
            }
            catch (...)
            {
                storage_->delete_view(stmt.table_name);
                throw;
            }

            return make_success_result(ExecutionResultKind::Create, 0, "Created view '" + stmt.table_name + "'");
        }

        if (stmt.columns.empty())
        {
            fail("CREATE TABLE requires at least one column");
        }

        if (storage_->has_table(stmt.table_name))
        {
            fail("Table already exists: " + stmt.table_name);
        }
        if (storage_->has_view(stmt.table_name))
        {
            fail("View already exists: " + stmt.table_name);
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
        return make_success_result(ExecutionResultKind::Create, 0, "Created table '" + stmt.table_name + "'");
    }

    ExecutionResult Executor::execute_drop(const DropStatement& stmt)
    {
        const bool has_table = storage_->has_table(stmt.table_name);
        const bool has_view = storage_->has_view(stmt.table_name);
        if (has_table && has_view)
        {
            fail("Name collision between table and view: " + stmt.table_name);
        }

        if (stmt.object_kind == SchemaObjectKind::View)
        {
            if (has_table)
            {
                fail("Cannot DROP VIEW because '" + stmt.table_name + "' is a table");
            }
            storage_->delete_view(stmt.table_name);
            return make_success_result(ExecutionResultKind::Drop, 0, "Dropped view '" + stmt.table_name + "'");
        }

        if (has_view)
        {
            fail("Cannot DROP TABLE on view: " + stmt.table_name);
        }

        storage_->delete_table(stmt.table_name);
        return make_success_result(ExecutionResultKind::Drop, 0, "Dropped table '" + stmt.table_name + "'");
    }

    ExecutionResult Executor::execute_delete(const DeleteStatement& stmt)
    {
        if (storage_->has_view(stmt.table_name))
        {
            fail("Cannot DELETE FROM view: " + stmt.table_name);
        }

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
        return make_success_result(ExecutionResultKind::Delete,
                                   deleted,
                                   "Deleted " + std::to_string(deleted) + " row(s) from '" + stmt.table_name + "'");
    }

    ExecutionResult Executor::execute_insert(const InsertStatement& stmt)
    {
        if (storage_->has_view(stmt.table_name))
        {
            fail("Cannot INSERT INTO view: " + stmt.table_name);
        }

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
        return make_success_result(ExecutionResultKind::Insert, 1, "Inserted 1 row into '" + stmt.table_name + "'");
    }

    ExecutionResult Executor::execute_select(const SelectStatement& stmt)
    {
        auto table = run_select_statement(stmt, *storage_);
        const auto row_count = table.rows.size();
        return make_success_result(ExecutionResultKind::Select,
                                   row_count,
                                   std::to_string(row_count) + " row(s) selected",
                                   make_visible_execution_table(std::move(table)));
    }

    ExecutionResult Executor::execute_update(const UpdateStatement& stmt)
    {
        if (storage_->has_view(stmt.table_name))
        {
            fail("Cannot UPDATE view: " + stmt.table_name);
        }

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
        return make_success_result(ExecutionResultKind::Update,
                                   updated,
                                   "Updated " + std::to_string(updated) + " row(s) in '" + stmt.table_name + "'");
    }
}
