#include "SelectExecution.h"

#include "ColumnMetadata.h"
#include "CsvStorage.h"
#include "Parser.h"
#include "SelectExecutionDetail.h"
#include "SqlError.h"
#include "Tokenizer.h"
#include "StringUtils.h"

#include <algorithm>
#include <sstream>
#include <utility>
#include <vector>

namespace sql::detail
{
    std::string select_star_column_name(const ResolvedSelectTable& table, std::size_t index)
    {
        if (table.source_count > 1)
        {
            return table.columns[index].source_name + "." + table.columns[index].column_name;
        }
        return table.columns[index].column_name;
    }

    namespace
    {
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

        std::string default_select_source_name(const SelectSource& source)
        {
            if (source.kind == SelectSource::Kind::FilePath)
            {
                return CsvStorage::resolve_table_source_path(source.name).stem().string();
            }
            return source.name;
        }

        SelectSourcePlan materialize_select_source(const SelectSource& source, const IStorage& storage)
        {
            SelectSourcePlan materialized;
            if (source.kind == SelectSource::Kind::FilePath)
            {
                const auto table = CsvStorage::describe_table_from_path(source.name);
                materialized.source_name = source.alias.value_or(default_select_source_name(source));
                materialized.column_names.reserve(table.columns.size());
                for (const auto& column : table.columns)
                {
                    materialized.column_names.push_back(visible_column_name(column));
                }
                materialized.rows = [path = source.name]()
                {
                    return CsvStorage::scan_table_from_path(path);
                };
                return materialized;
            }

            if (source.kind == SelectSource::Kind::Table)
            {
                const bool has_table = storage.has_table(source.name);
                const bool has_view = storage.has_view(source.name);
                if (has_table && has_view)
                {
                    fail("Name collision between table and view: " + source.name);
                }

                materialized.source_name = source.alias.value_or(default_select_source_name(source));
                if (has_view)
                {
                    const auto result = run_view_statement(source.name, storage);
                    materialized.column_names = result.column_names;
                    materialized.rows = result.rows;
                    return materialized;
                }

                const Table table = storage.describe_table(source.name);
                materialized.column_names.reserve(table.columns.size());
                for (const auto& column : table.columns)
                {
                    materialized.column_names.push_back(visible_column_name(column));
                }
                materialized.rows = [storage_ptr = &storage, table_name = source.name]()
                {
                    return storage_ptr->scan_table(table_name);
                };
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

        bool can_parallelize_select_source_materialization(const SelectSource& source, const IStorage& storage)
        {
            if (source.kind == SelectSource::Kind::FilePath)
            {
                return true;
            }
            if (source.kind != SelectSource::Kind::Table)
            {
                return false;
            }
            return !storage.has_view(source.name);
        }
    }

    ExecutionTable run_view_statement(const std::string& view_name, const IStorage& storage)
    {
        ActiveViewGuard guard(view_name);
        return run_select_statement(parse_view_statement(storage.load_view(view_name)), storage);
    }

    ResolvedSelectTable materialize_select_table(const SelectStatement& stmt, const IStorage& storage, const ForkJoinScheduler* scheduler)
    {
        if (stmt.sources.empty())
        {
            fail("SELECT requires at least one source");
        }

        std::vector<SelectSourcePlan> sources;
        if (scheduler != nullptr && stmt.sources.size() > 1 && std::all_of(stmt.sources.begin(), stmt.sources.end(), [&](const SelectSource& source)
        {
            return can_parallelize_select_source_materialization(source, storage);
        }))
        {
            std::vector<std::function<SelectSourcePlan()>> tasks;
            tasks.reserve(stmt.sources.size());
            for (const auto& source : stmt.sources)
            {
                tasks.push_back([source, storage_ptr = &storage]()
                {
                    return materialize_select_source(source, *storage_ptr);
                });
            }
            sources = scheduler->fork_join(tasks);
        }
        else
        {
            sources.reserve(stmt.sources.size());
            for (const auto& source : stmt.sources)
            {
                sources.push_back(materialize_select_source(source, storage));
            }
        }

        for (std::size_t i = 0; i < sources.size(); ++i)
        {
            for (std::size_t j = i + 1; j < sources.size(); ++j)
            {
                if (iequals(sources[i].source_name, sources[j].source_name))
                {
                    fail("Duplicate SELECT source name '" + sources[j].source_name + "'");
                }
            }
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

        auto shared_sources = std::make_shared<std::vector<SelectSourcePlan>>(std::move(sources));
        resolved.rows = [shared_sources]()
        {
            return open_joined_rows(shared_sources, 0, {});
        };

        return resolved;
    }
}

namespace sql
{
    void validate_view_definition(const std::string& view_name, const IStorage& storage, const ICoroExecutor& coro_executor)
    {
        const auto result = detail::run_view_statement(view_name, storage);
        coro_executor.drive_rows(result.rows(), [](const Row& row)
        {
            static_cast<void>(row);
            return true;
        });
    }
}



