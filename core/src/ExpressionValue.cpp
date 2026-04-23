#include "ExpressionEvaluation.h"

#include "SqlError.h"

#include <chrono>
#include <cmath>
#include <ctime>
#include <cstdlib>
#include <iomanip>
#include <regex>
#include <sstream>

namespace sql
{
    namespace
    {
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

        bool is_aggregate_function_name(const std::string& name)
        {
            return name == "COUNT" || name == "count"
                || name == "SUM" || name == "sum"
                || name == "AVG" || name == "avg"
                || name == "MIN" || name == "min"
                || name == "MAX" || name == "max";
        }
    }

    bool is_stored_null(const std::string& value)
    {
        return value == null_storage_marker;
    }

    std::string visible_value_text(const std::string& value)
    {
        return is_stored_null(value) ? "NULL" : value;
    }

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
        return !value.text.empty() && value.text != "0" && value.text != "false" && value.text != "FALSE";
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
            if (left.is_null || right.is_null) return make_numeric(0.0);
            return make_numeric(((left.numeric && right.numeric) ? (left.number < right.number) : (left.text < right.text)) ? 1.0 : 0.0);
        case BinaryOperator::LessEqual:
            if (left.is_null || right.is_null) return make_numeric(0.0);
            return make_numeric(((left.numeric && right.numeric) ? (left.number <= right.number) : (left.text <= right.text)) ? 1.0 : 0.0);
        case BinaryOperator::Greater:
            if (left.is_null || right.is_null) return make_numeric(0.0);
            return make_numeric(((left.numeric && right.numeric) ? (left.number > right.number) : (left.text > right.text)) ? 1.0 : 0.0);
        case BinaryOperator::GreaterEqual:
            if (left.is_null || right.is_null) return make_numeric(0.0);
            return make_numeric(((left.numeric && right.numeric) ? (left.number >= right.number) : (left.text >= right.text)) ? 1.0 : 0.0);
        case BinaryOperator::Is:
            return make_numeric((left.is_null && right.is_null) ? 1.0 : 0.0);
        case BinaryOperator::IsNot:
            return make_numeric((left.is_null && right.is_null) ? 0.0 : 1.0);
        case BinaryOperator::In:
        case BinaryOperator::NotIn:
            fail("IN and NOT IN require dedicated evaluation");
        case BinaryOperator::Like:
            if (left.is_null || right.is_null) return make_numeric(0.0);
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
            if (left.is_null || right.is_null) return make_numeric(0.0);
            return make_numeric(((left.numeric && right.numeric) ? (std::fabs(left.number - right.number) < 1e-9) : (left.text == right.text)) ? 1.0 : 0.0);
        case BinaryOperator::NotEqual:
            if (left.is_null || right.is_null) return make_numeric(0.0);
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
        case BinaryOperator::Less: return comparison < 0;
        case BinaryOperator::LessEqual: return comparison <= 0;
        case BinaryOperator::Greater: return comparison > 0;
        case BinaryOperator::GreaterEqual: return comparison >= 0;
        case BinaryOperator::Equal: return comparison == 0;
        case BinaryOperator::NotEqual: return comparison != 0;
        default: fail("ANY and ALL require comparison operators");
        }
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
        case ExpressionKind::List:
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
        if (left.is_null && right.is_null) return 0;
        if (left.is_null) return -1;
        if (right.is_null) return 1;
        if (left.numeric && right.numeric)
        {
            if (left.number < right.number) return -1;
            if (left.number > right.number) return 1;
            return 0;
        }
        if (left.text < right.text) return -1;
        if (left.text > right.text) return 1;
        return 0;
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

    EvaluatedValue evaluate_function(const Expression& expression)
    {
        if (expression.text == "NOW" || expression.text == "now")
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
}

