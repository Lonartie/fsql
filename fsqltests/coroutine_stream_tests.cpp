#include "doctest.h"

#include "ForkJoinScheduler.h"
#include "ParallelCoroExecutor.h"
#include "test_support.h"

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <set>
#include <stdexcept>
#include <thread>

namespace
{
    fsql::RowGenerator make_test_rows()
    {
        co_yield {"first"};
        co_yield {"second"};
        co_yield {"third"};
    }

    fsql::ValueGenerator make_test_values()
    {
        co_yield "alpha";
        co_yield "beta";
        co_yield "gamma";
    }

    class StreamingOnlyStorage final : public fsql::IStorage
    {
    public:
        std::filesystem::path table_path(const fsql::RelationReference& table_name) const override
        {
            return std::filesystem::path(table_name.name + ".csv");
        }

        bool has_table(const fsql::RelationReference& table_name) const override
        {
            return table_name.name == table_.name;
        }

        bool has_view(const fsql::RelationReference& view_name) const override
        {
            static_cast<void>(view_name);
            return false;
        }

        fsql::Table load_table(const fsql::RelationReference& table_name) const override
        {
            ++load_table_calls;
            throw std::runtime_error("load_table should not be called for streamed SELECT paths: " + table_name.name);
        }

        fsql::Table describe_table(const fsql::RelationReference& table_name) const override
        {
            if (!has_table(table_name))
            {
                throw std::runtime_error("Unknown table: " + table_name.name);
            }

            ++describe_table_calls;
            return {table_.name, std::nullopt, table_.columns, {}};
        }

        fsql::RowGenerator scan_table(const fsql::RelationReference& table_name) const override
        {
            if (!has_table(table_name))
            {
                throw std::runtime_error("Unknown table: " + table_name.name);
            }

            ++scan_table_calls;
            for (const auto& row : table_.rows)
            {
                co_yield row;
            }
        }

        fsql::ViewDefinition load_view(const fsql::RelationReference& view_name) const override
        {
            throw std::runtime_error("Unknown view: " + view_name.name);
        }

        void save_table(const fsql::Table& table) override
        {
            table_ = table;
        }

        void save_view(const fsql::ViewDefinition& view) override
        {
            static_cast<void>(view);
            throw std::runtime_error("Views are not supported in this test storage");
        }

        void delete_table(const fsql::RelationReference& table_name) override
        {
            static_cast<void>(table_name);
            throw std::runtime_error("DELETE TABLE is not supported in this test storage");
        }

        void delete_view(const fsql::RelationReference& view_name) override
        {
            static_cast<void>(view_name);
            throw std::runtime_error("DELETE VIEW is not supported in this test storage");
        }

        std::size_t column_index(const fsql::Table& table, const std::string& column) const override
        {
            for (std::size_t i = 0; i < table.columns.size(); ++i)
            {
                if (table.columns[i] == column)
                {
                    return i;
                }
            }
            throw std::runtime_error("Unknown column: " + column);
        }

        mutable std::size_t load_table_calls = 0;
        mutable std::size_t describe_table_calls = 0;
        mutable std::size_t scan_table_calls = 0;

    private:
        fsql::Table table_{"todos", std::nullopt, {"title", "done"}, {{"Buy milk", "false"}, {"Archive logs", "true"}}};
    };

    class ParallelDescribeStorage final : public fsql::IStorage
    {
    public:
        std::filesystem::path table_path(const fsql::RelationReference& table_name) const override
        {
            return std::filesystem::path(table_name.name + ".csv");
        }

        bool has_table(const fsql::RelationReference& table_name) const override
        {
            return table_name.name == tasks_.name || table_name.name == teams_.name;
        }

        bool has_view(const fsql::RelationReference& view_name) const override
        {
            static_cast<void>(view_name);
            return false;
        }

        fsql::Table load_table(const fsql::RelationReference& table_name) const override
        {
            throw std::runtime_error("load_table should not be called in this test: " + table_name.name);
        }

        fsql::Table describe_table(const fsql::RelationReference& table_name) const override
        {
            {
                std::unique_lock lock(mutex_);
                describe_thread_ids_.insert(std::this_thread::get_id());
                ++active_describes_;
                max_active_describes_ = std::max(max_active_describes_, active_describes_);
                if (active_describes_ < 2)
                {
                    condition_.wait_for(lock, std::chrono::milliseconds(250), [&]()
                    {
                        return active_describes_ >= 2;
                    });
                }
                else
                {
                    condition_.notify_all();
                }
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(25));

            {
                std::scoped_lock lock(mutex_);
                --active_describes_;
            }
            condition_.notify_all();

            if (table_name.name == tasks_.name)
            {
                return {tasks_.name, std::nullopt, tasks_.columns, {}};
            }
            if (table_name.name == teams_.name)
            {
                return {teams_.name, std::nullopt, teams_.columns, {}};
            }

            throw std::runtime_error("Unknown table: " + table_name.name);
        }

        fsql::RowGenerator scan_table(const fsql::RelationReference& table_name) const override
        {
            if (table_name.name == tasks_.name)
            {
                for (const auto& row : tasks_.rows)
                {
                    co_yield row;
                }
                co_return;
            }
            if (table_name.name == teams_.name)
            {
                for (const auto& row : teams_.rows)
                {
                    co_yield row;
                }
                co_return;
            }

            throw std::runtime_error("Unknown table: " + table_name.name);
        }

        fsql::ViewDefinition load_view(const fsql::RelationReference& view_name) const override
        {
            throw std::runtime_error("Unknown view: " + view_name.name);
        }

        void save_table(const fsql::Table& table) override
        {
            static_cast<void>(table);
            throw std::runtime_error("save_table is not supported in this test storage");
        }

        void save_view(const fsql::ViewDefinition& view) override
        {
            static_cast<void>(view);
            throw std::runtime_error("save_view is not supported in this test storage");
        }

        void delete_table(const fsql::RelationReference& table_name) override
        {
            static_cast<void>(table_name);
            throw std::runtime_error("delete_table is not supported in this test storage");
        }

        void delete_view(const fsql::RelationReference& view_name) override
        {
            static_cast<void>(view_name);
            throw std::runtime_error("delete_view is not supported in this test storage");
        }

        std::size_t column_index(const fsql::Table& table, const std::string& column) const override
        {
            for (std::size_t i = 0; i < table.columns.size(); ++i)
            {
                if (table.columns[i] == column)
                {
                    return i;
                }
            }
            throw std::runtime_error("Unknown column: " + column);
        }

        std::size_t max_active_describes() const
        {
            std::scoped_lock lock(mutex_);
            return max_active_describes_;
        }

        std::size_t unique_describe_threads() const
        {
            std::scoped_lock lock(mutex_);
            return describe_thread_ids_.size();
        }

    private:
        mutable std::mutex mutex_;
        mutable std::condition_variable condition_;
        mutable std::size_t active_describes_ = 0;
        mutable std::size_t max_active_describes_ = 0;
        mutable std::set<std::thread::id> describe_thread_ids_;

        fsql::Table tasks_{"tasks", std::nullopt, {"title", "team_id"}, {{"Patch release", "10"}, {"Write docs", "20"}}};
        fsql::Table teams_{"teams", std::nullopt, {"id", "name"}, {{"10", "ops"}, {"20", "docs"}}};
    };
}

TEST_SUITE_BEGIN("Coroutines");

TEST_CASE("serial coroutine executor can stop row streams early")
{
    fsql::SerialCoroExecutor executor;
    std::vector<fsql::Row> consumed;

    const auto count = executor.drive_rows(make_test_rows(), [&](const fsql::Row& row)
    {
        consumed.push_back(row);
        return consumed.size() < 2;
    });

    CHECK_EQ(count, 2U);
    REQUIRE_EQ(consumed.size(), 2U);
    CHECK_EQ(consumed[0][0], "first");
    CHECK_EQ(consumed[1][0], "second");
}

TEST_CASE("serial coroutine executor can stop value streams early")
{
    fsql::SerialCoroExecutor executor;
    std::vector<std::string> consumed;

    const auto count = executor.drive_values(make_test_values(), [&](const std::string& value)
    {
        consumed.push_back(value);
        return value != "beta";
    });

    CHECK_EQ(count, 2U);
    REQUIRE_EQ(consumed.size(), 2U);
    CHECK_EQ(consumed[0], "alpha");
    CHECK_EQ(consumed[1], "beta");
}

TEST_CASE("fork join scheduler preserves order across nested tasks")
{
    fsql::ForkJoinScheduler scheduler(2);

    std::vector<std::function<int()>> tasks;
    tasks.push_back([]
    {
        return 1;
    });
    tasks.push_back([&scheduler]
    {
        std::vector<std::function<int()>> nested_tasks;
        nested_tasks.push_back([]
        {
            return 20;
        });
        nested_tasks.push_back([]
        {
            return 21;
        });

        const auto nested_results = scheduler.fork_join(nested_tasks);
        return nested_results[0] + nested_results[1];
    });
    tasks.push_back([]
    {
        return 3;
    });

    const auto results = scheduler.fork_join(tasks);
    REQUIRE_EQ(results.size(), 3U);
    CHECK_EQ(results[0], 1);
    CHECK_EQ(results[1], 41);
    CHECK_EQ(results[2], 3);
}

TEST_CASE("parallel coroutine executor preserves row order and supports early stop")
{
    auto scheduler = std::make_shared<fsql::ForkJoinScheduler>(2);
    fsql::ParallelCoroExecutor executor(scheduler, 1, 1);
    std::vector<fsql::Row> consumed;

    const auto count = executor.drive_rows(make_test_rows(), [&](const fsql::Row& row)
    {
        consumed.push_back(row);
        return consumed.size() < 2;
    });

    CHECK_EQ(count, 2U);
    REQUIRE_EQ(consumed.size(), 2U);
    CHECK_EQ(consumed[0][0], "first");
    CHECK_EQ(consumed[1][0], "second");
}

TEST_CASE("parallel coroutine executor preserves value order and supports early stop")
{
    auto scheduler = std::make_shared<fsql::ForkJoinScheduler>(2);
    fsql::ParallelCoroExecutor executor(scheduler, 1, 1);
    std::vector<std::string> consumed;

    const auto count = executor.drive_values(make_test_values(), [&](const std::string& value)
    {
        consumed.push_back(value);
        return value != "beta";
    });

    CHECK_EQ(count, 2U);
    REQUIRE_EQ(consumed.size(), 2U);
    CHECK_EQ(consumed[0], "alpha");
    CHECK_EQ(consumed[1], "beta");
}

TEST_CASE("simple select streams rows from scan_table without load_table")
{
    auto storage = std::make_shared<StreamingOnlyStorage>();
    fsql::Executor executor(storage);

    const auto result = executor.execute(fsql_test::parse_statement("SELECT title FROM todos WHERE done = false;"));

    REQUIRE(result.success);
    CHECK_EQ(storage->load_table_calls, 0U);
    CHECK_EQ(storage->describe_table_calls, 1U);
    CHECK_EQ(storage->scan_table_calls, 1U);
    CHECK_EQ(result.affected_rows, 1U);
    CHECK_EQ(result.message, "1 row(s) selected");

    const auto& table = fsql_test::require_table(result);
    CHECK_EQ(storage->load_table_calls, 0U);
    CHECK_EQ(storage->scan_table_calls, 2U);
    REQUIRE_EQ(table.rows.size(), 1U);
    CHECK_EQ(table.rows[0][0], "Buy milk");
}

TEST_CASE("parallel executor materializes independent select sources through fork join scheduling")
{
    auto storage = std::make_shared<ParallelDescribeStorage>();
    auto coro_executor = std::make_shared<fsql::ParallelCoroExecutor>(std::make_shared<fsql::ForkJoinScheduler>(2), 1, 1);
    fsql::Executor executor(storage, coro_executor);

    const auto result = executor.execute(fsql_test::parse_statement(
        "SELECT tasks.title, teams.name FROM tasks, teams WHERE tasks.team_id = teams.id ORDER BY tasks.title;"));

    REQUIRE(result.success);
    const auto& table = fsql_test::require_table(result);
    REQUIRE_EQ(table.rows.size(), 2U);
    CHECK_EQ(table.rows[0][0], "Patch release");
    CHECK_EQ(table.rows[0][1], "ops");
    CHECK_EQ(table.rows[1][0], "Write docs");
    CHECK_EQ(table.rows[1][1], "docs");
    CHECK_GE(storage->max_active_describes(), 2U);
    CHECK_GE(storage->unique_describe_threads(), 2U);
}

TEST_SUITE_END();

