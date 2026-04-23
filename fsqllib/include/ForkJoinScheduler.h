#pragma once

#include <algorithm>
#include <cstddef>
#include <deque>
#include <future>
#include <functional>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace fsql
{
    /// @brief Runs independent tasks using an ordered fork/join strategy.
    class ForkJoinScheduler
    {
    public:
        /// @brief Initializes the scheduler.
        /// @param max_parallelism Maximum number of concurrently forked tasks.
        explicit ForkJoinScheduler(std::size_t max_parallelism = std::thread::hardware_concurrency())
            : max_parallelism_(std::max<std::size_t>(1, max_parallelism == 0 ? 1 : max_parallelism))
        {
        }

        /// @brief Returns the configured parallelism limit.
        /// @return Maximum concurrently forked tasks.
        std::size_t max_parallelism() const
        {
            return max_parallelism_;
        }

        /// @brief Forks a single task asynchronously.
        /// @tparam Task Callable task type.
        /// @param task Task to execute.
        /// @return Future representing the asynchronous task.
        template <typename Task>
        auto fork(Task&& task) const -> std::future<std::invoke_result_t<std::decay_t<Task>>>
        {
            return std::async(std::launch::async, std::forward<Task>(task));
        }

        /// @brief Forks independent tasks and joins them in input order.
        /// @tparam T Task result type.
        /// @param tasks Tasks to execute.
        /// @return Ordered task results.
        template <typename T>
        std::vector<T> fork_join(const std::vector<std::function<T()>>& tasks) const
        {
            std::vector<T> results;
            results.reserve(tasks.size());
            if (tasks.empty())
            {
                return results;
            }
            if (max_parallelism_ <= 1 || tasks.size() == 1)
            {
                for (const auto& task : tasks)
                {
                    results.push_back(task());
                }
                return results;
            }

            std::deque<std::future<T>> active_tasks;
            const auto concurrency = std::min(max_parallelism_, tasks.size());
            std::size_t next_task_index = 0;
            for (; next_task_index < concurrency; ++next_task_index)
            {
                active_tasks.push_back(fork(tasks[next_task_index]));
            }

            std::size_t joined_task_count = 0;
            while (joined_task_count < tasks.size())
            {
                results.push_back(active_tasks.front().get());
                active_tasks.pop_front();
                ++joined_task_count;

                if (next_task_index < tasks.size())
                {
                    active_tasks.push_back(fork(tasks[next_task_index]));
                    ++next_task_index;
                }
            }

            return results;
        }

        /// @brief Forks independent void tasks and joins them in input order.
        /// @param tasks Tasks to execute.
        void fork_join(const std::vector<std::function<void()>>& tasks) const
        {
            if (tasks.empty())
            {
                return;
            }
            if (max_parallelism_ <= 1 || tasks.size() == 1)
            {
                for (const auto& task : tasks)
                {
                    task();
                }
                return;
            }

            std::deque<std::future<void>> active_tasks;
            const auto concurrency = std::min(max_parallelism_, tasks.size());
            std::size_t next_task_index = 0;
            for (; next_task_index < concurrency; ++next_task_index)
            {
                active_tasks.push_back(fork(tasks[next_task_index]));
            }

            std::size_t joined_task_count = 0;
            while (joined_task_count < tasks.size())
            {
                active_tasks.front().get();
                active_tasks.pop_front();
                ++joined_task_count;

                if (next_task_index < tasks.size())
                {
                    active_tasks.push_back(fork(tasks[next_task_index]));
                    ++next_task_index;
                }
            }
        }

    private:
        std::size_t max_parallelism_ = 1;
    };
}

