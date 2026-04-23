#include "SqlSerialization.h"

#include "SqlError.h"
#include "StringUtils.h"

#include <sstream>

namespace fsql
{
    namespace
    {
        bool is_numeric_literal(const std::string& text)
        {
            if (text.empty())
            {
                return false;
            }

            char* end = nullptr;
            std::strtod(text.c_str(), &end);
            return end != nullptr && *end == '\0';
        }

        bool is_simple_alias_identifier(const std::string& text)
        {
            if (text.empty() || !is_identifier_start(text.front()))
            {
                return false;
            }

            for (const char ch : text)
            {
                if (!is_identifier_part(ch))
                {
                    return false;
                }
            }

            return true;
        }

        std::string serialize_projection_alias(const std::string& alias)
        {
            return is_simple_alias_identifier(alias) ? alias : quote_string(alias);
        }
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

    std::string serialize_select_source(const SelectSource& source)
    {
        std::ostringstream stream;
        if (source.kind == SelectSource::Kind::Table)
        {
            stream << source.name;
        }
        else if (source.kind == SelectSource::Kind::FilePath)
        {
            stream << quote_string(source.name);
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
                stream << serialize_expression(statement.projections[i].expression);
                if (statement.projections[i].alias.has_value())
                {
                    stream << " AS " << serialize_projection_alias(*statement.projections[i].alias);
                }
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
            return is_numeric_literal(expression->text) ? expression->text : quote_string(expression->text);
        case ExpressionKind::Null:
            return "NULL";
        case ExpressionKind::Identifier:
            return expression->text;
        case ExpressionKind::List:
        {
            std::ostringstream stream;
            stream << '(';
            for (std::size_t i = 0; i < expression->arguments.size(); ++i)
            {
                if (i > 0)
                {
                    stream << ", ";
                }
                stream << serialize_expression(expression->arguments[i]);
            }
            stream << ')';
            return stream.str();
        }
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
            case BinaryOperator::NotIn: op = "NOT IN"; break;
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
            if ((expression->binary_operator == BinaryOperator::In || expression->binary_operator == BinaryOperator::NotIn)
                && expression->right && expression->right->kind == ExpressionKind::List)
            {
                return "(" + serialize_expression(expression->left) + " " + op + " " + serialize_expression(expression->right) + ")";
            }
            return "(" + serialize_expression(expression->left) + " " + op + " " + serialize_expression(expression->right) + ")";
        }
        }

        fail("Unsupported expression serialization");
    }
}

