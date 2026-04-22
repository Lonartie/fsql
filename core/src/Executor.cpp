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
            case ExpressionKind::FunctionCall:
                return expression->text + "()";
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

        EvaluatedValue evaluate_function(const std::string& name)
        {
            if (iequals(name, "NOW"))
            {
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

            fail("Unsupported function: " + name + "()");
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
            case ExpressionKind::FunctionCall:
                return evaluate_function(expression->text);
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

        std::string evaluate_value(const ExpressionPtr& expression)
        {
            const Table empty_table{};
            const Row empty_row{};
            struct DummyStorage final : IStorage
            {
                std::filesystem::path table_path(const std::string&) const override { return {}; }
                Table load_table(const std::string&) const override { fail("No table context available"); }
                void save_table(const Table&) override { fail("No table context available"); }
                void delete_table(const std::string&) override { fail("No table context available"); }
                std::size_t column_index(const Table&, const std::string&) const override { fail("No table context available"); }
            } storage;
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
                row[index] = evaluate_value(stmt.values[i]);
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
                row[i] = evaluate_value(stmt.values[i]);
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
                row[i] = evaluate_value(parser.parse_expression());
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
        const Table table = storage_->load_table(stmt.table_name);
        std::vector<std::size_t> selected_indexes;
        std::vector<std::string> selected_names;

        if (stmt.select_all)
        {
            for (std::size_t i = 0; i < table.columns.size(); ++i)
            {
                selected_indexes.push_back(i);
                selected_names.push_back(visible_column_name(table.columns[i]));
            }
        }
        else
        {
            for (const auto& column : stmt.columns)
            {
                const auto index = storage_->column_index(table, column);
                selected_indexes.push_back(index);
                selected_names.push_back(visible_column_name(table.columns[index]));
            }
        }

        std::vector<Row> result_rows;
        for (const auto& row : table.rows)
        {
            if (stmt.where && !to_bool(evaluate_expression(stmt.where, table, row, *storage_)))
            {
                continue;
            }

            Row projected_row;
            projected_row.reserve(selected_indexes.size());
            for (const auto index : selected_indexes)
            {
                projected_row.push_back(row[index]);
            }
            result_rows.push_back(std::move(projected_row));
        }

        std::vector<std::size_t> widths(selected_names.size(), 0);
        for (std::size_t i = 0; i < selected_names.size(); ++i)
        {
            widths[i] = selected_names[i].size();
        }
        for (const auto& row : result_rows)
        {
            for (std::size_t i = 0; i < row.size(); ++i)
            {
                widths[i] = std::max(widths[i], row[i].size());
            }
        }

        const auto separator = make_separator(widths);
        output_ << separator << '\n';
        write_row(output_, selected_names, widths);
        output_ << separator << '\n';
        for (const auto& row : result_rows)
        {
            write_row(output_, row, widths);
        }
        output_ << separator << '\n';
        output_ << result_rows.size() << " row(s) selected\n";
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
