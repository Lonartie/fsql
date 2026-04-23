#include "Executor.h"

#include "ColumnMetadata.h"
#include "ExpressionEvaluation.h"
#include "Parser.h"
#include "ParallelCoroExecutor.h"
#include "SelectExecution.h"
#include "SerialCoroExecutor.h"
#include "SqlError.h"
#include "SqlSerialization.h"
#include "Tokenizer.h"

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

namespace sql
{
    namespace
    {
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

        std::string relation_name(const RelationReference& reference)
        {
            return reference.name;
        }

        std::optional<std::filesystem::path> relation_storage_path(const RelationReference& reference)
        {
            if (reference.kind != RelationReference::Kind::FilePath)
            {
                return std::nullopt;
            }
            return std::filesystem::path(reference.name);
        }
    }

    Executor::Executor(std::shared_ptr<IStorage> storage, std::shared_ptr<ICoroExecutor> coro_executor)
        : storage_(std::move(storage)),
          coro_executor_(std::move(coro_executor))
    {
        if (!storage_)
        {
            fail("Storage backend must not be null");
        }
        if (!coro_executor_)
        {
            coro_executor_ = std::make_shared<SerialCoroExecutor>();
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
            fail("Name collision between table and view: " + relation_name(stmt.table_name));
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
                fail("Cannot ALTER VIEW because '" + relation_name(stmt.table_name) + "' is a table");
            }

            const auto previous_view = storage_->load_view(stmt.table_name);

            ViewDefinition view;
            view.name = relation_name(stmt.table_name);
            view.storage_path = relation_storage_path(stmt.table_name);
            view.select_statement = serialize_select_statement(*stmt.view_query);
            storage_->save_view(view);

            try
            {
                validate_view_definition(stmt.table_name, *storage_, *coro_executor_);
            }
            catch (...)
            {
                storage_->save_view(previous_view);
                throw;
            }

            return make_success_result(ExecutionResultKind::Alter, 0, "Altered view '" + relation_name(stmt.table_name) + "'");
        }

        if (has_view)
        {
            fail("Cannot ALTER TABLE on view: " + relation_name(stmt.table_name));
        }

        Table table = storage_->load_table(stmt.table_name);
        auto find_column_index = [&]()
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

            ColumnMetadata metadata;
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
            return make_success_result(ExecutionResultKind::Alter, 0, "Altered table '" + relation_name(stmt.table_name) + "'");
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
            return make_success_result(ExecutionResultKind::Alter, 0, "Altered table '" + relation_name(stmt.table_name) + "'");
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
            return make_success_result(ExecutionResultKind::Alter, 0, "Altered table '" + relation_name(stmt.table_name) + "'");
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
            return make_success_result(ExecutionResultKind::Alter, 0, "Altered table '" + relation_name(stmt.table_name) + "'");
        }
        case AlterAction::DropDefault:
        {
            const auto index = find_column_index();
            auto metadata = parse_column_metadata(table.columns[index]);
            metadata.default_expression.clear();
            table.columns[index] = serialize_column_metadata(metadata);
            storage_->save_table(table);
            return make_success_result(ExecutionResultKind::Alter, 0, "Altered table '" + relation_name(stmt.table_name) + "'");
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
            return make_success_result(ExecutionResultKind::Alter, 0, "Altered table '" + relation_name(stmt.table_name) + "'");
        }
        case AlterAction::DropAutoIncrement:
        {
            const auto index = find_column_index();
            auto metadata = parse_column_metadata(table.columns[index]);
            metadata.auto_increment = false;
            table.columns[index] = serialize_column_metadata(metadata);
            storage_->save_table(table);
            return make_success_result(ExecutionResultKind::Alter, 0, "Altered table '" + relation_name(stmt.table_name) + "'");
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
                fail("Table already exists: " + relation_name(stmt.table_name));
            }
            if (storage_->has_view(stmt.table_name))
            {
                fail("View already exists: " + relation_name(stmt.table_name));
            }

            ViewDefinition view;
            view.name = relation_name(stmt.table_name);
            view.storage_path = relation_storage_path(stmt.table_name);
            view.select_statement = serialize_select_statement(*stmt.view_query);
            storage_->save_view(view);

            try
            {
                validate_view_definition(stmt.table_name, *storage_, *coro_executor_);
            }
            catch (...)
            {
                storage_->delete_view(stmt.table_name);
                throw;
            }

            return make_success_result(ExecutionResultKind::Create, 0, "Created view '" + relation_name(stmt.table_name) + "'");
        }

        if (stmt.columns.empty())
        {
            fail("CREATE TABLE requires at least one column");
        }
        if (storage_->has_table(stmt.table_name))
        {
            fail("Table already exists: " + relation_name(stmt.table_name));
        }
        if (storage_->has_view(stmt.table_name))
        {
            fail("View already exists: " + relation_name(stmt.table_name));
        }

        Table table;
        table.name = relation_name(stmt.table_name);
        table.storage_path = relation_storage_path(stmt.table_name);
        table.columns.reserve(stmt.columns.size());
        for (const auto& column : stmt.columns)
        {
            ColumnMetadata metadata;
            metadata.visible_name = column.name;
            metadata.auto_increment = column.auto_increment;
            if (column.default_value)
            {
                metadata.default_expression = serialize_expression(column.default_value);
            }
            table.columns.push_back(serialize_column_metadata(metadata));
        }
        storage_->save_table(table);
        return make_success_result(ExecutionResultKind::Create, 0, "Created table '" + relation_name(stmt.table_name) + "'");
    }

    ExecutionResult Executor::execute_drop(const DropStatement& stmt)
    {
        const bool has_table = storage_->has_table(stmt.table_name);
        const bool has_view = storage_->has_view(stmt.table_name);
        if (has_table && has_view)
        {
            fail("Name collision between table and view: " + relation_name(stmt.table_name));
        }

        if (stmt.object_kind == SchemaObjectKind::View)
        {
            if (has_table)
            {
                fail("Cannot DROP VIEW because '" + relation_name(stmt.table_name) + "' is a table");
            }
            storage_->delete_view(stmt.table_name);
            return make_success_result(ExecutionResultKind::Drop, 0, "Dropped view '" + relation_name(stmt.table_name) + "'");
        }

        if (has_view)
        {
            fail("Cannot DROP TABLE on view: " + relation_name(stmt.table_name));
        }
        storage_->delete_table(stmt.table_name);
        return make_success_result(ExecutionResultKind::Drop, 0, "Dropped table '" + relation_name(stmt.table_name) + "'");
    }

    ExecutionResult Executor::execute_delete(const DeleteStatement& stmt)
    {
        if (storage_->has_view(stmt.table_name))
        {
            fail("Cannot DELETE FROM view: " + relation_name(stmt.table_name));
        }

        Table table = storage_->load_table(stmt.table_name);
        const auto original_size = table.rows.size();
        table.rows.erase(std::remove_if(table.rows.begin(), table.rows.end(), [&](const Row& row)
        {
            if (!stmt.where)
            {
                return true;
            }
            return to_bool(evaluate_expression(stmt.where, table, row, *storage_));
        }), table.rows.end());

        const auto deleted = original_size - table.rows.size();
        storage_->save_table(table);
        return make_success_result(ExecutionResultKind::Delete,
                                   deleted,
                                   "Deleted " + std::to_string(deleted) + " row(s) from '" + relation_name(stmt.table_name) + "'");
    }

    ExecutionResult Executor::execute_insert(const InsertStatement& stmt)
    {
        if (storage_->has_view(stmt.table_name))
        {
            fail("Cannot INSERT INTO view: " + relation_name(stmt.table_name));
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
        return make_success_result(ExecutionResultKind::Insert, 1, "Inserted 1 row into '" + relation_name(stmt.table_name) + "'");
    }

    ExecutionResult Executor::execute_select(const SelectStatement& stmt)
    {
        std::shared_ptr<ForkJoinScheduler> scheduler;
        if (const auto parallel_coro_executor = std::dynamic_pointer_cast<ParallelCoroExecutor>(coro_executor_))
        {
            scheduler = parallel_coro_executor->scheduler();
        }

        auto table = run_select_statement(stmt, *storage_, scheduler.get());
        const auto row_count = count_execution_rows(table, *coro_executor_);
        return make_success_result(ExecutionResultKind::Select,
                                   row_count,
                                   std::to_string(row_count) + " row(s) selected",
                                   make_visible_execution_table(std::move(table)));
    }

    ExecutionResult Executor::execute_update(const UpdateStatement& stmt)
    {
        if (storage_->has_view(stmt.table_name))
        {
            fail("Cannot UPDATE view: " + relation_name(stmt.table_name));
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
                                   "Updated " + std::to_string(updated) + " row(s) in '" + relation_name(stmt.table_name) + "'");
    }
}
