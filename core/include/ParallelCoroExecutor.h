#pragma once

#include "ForkJoinScheduler.h"
#include "SerialCoroExecutor.h"

#include <algorithm>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

namespace sql
{
    /// @brief Drives coroutine streams by overlapping loading and consumption when safe.
    class ParallelCoroExecutor final : public ICoroExecutor
    {
    public:
        /// @brief Initializes the parallel coroutine executor.
        /// @param scheduler Scheduler used to fork producer stages.
        /// @param chunk_size Number of yielded items buffered per chunk.
        /// @param buffered_chunk_count Maximum queued chunk count.
        explicit ParallelCoroExecutor(std::shared_ptr<ForkJoinScheduler> scheduler = nullptr,
                                      std::size_t chunk_size = 64,
                                      std::size_t buffered_chunk_count = 2)
            : scheduler_(std::move(scheduler)),
              chunk_size_(std::max<std::size_t>(1, chunk_size)),
              buffered_chunk_count_(std::max<std::size_t>(1, buffered_chunk_count))
        {
            if (!scheduler_)
            {
                scheduler_ = std::make_shared<ForkJoinScheduler>();
            }
        }

        /// @brief Returns the scheduler used for fork/join work.
        /// @return Shared scheduler instance.
        const std::shared_ptr<ForkJoinScheduler>& scheduler() const
        {
            return scheduler_;
        }

        /// @brief Drives a row stream with overlapped loading and serial ordered consumption.
        /// @param rows Row stream.
        /// @param consumer Consumer invoked for each yielded row. Return `false` to stop consumption early.
        /// @return Number of consumed rows.
        std::size_t drive_rows(RowGenerator rows, const std::function<bool(const Row&)>& consumer) const override
        {
            return drive_stream<Row>(std::move(rows),
                                     consumer,
                                     [this](RowGenerator generator, const std::function<bool(const Row&)>& serial_consumer)
                                     {
                                         return serial_executor_.drive_rows(std::move(generator), serial_consumer);
                                     });
        }

        /// @brief Drives a scalar value stream with overlapped loading and serial ordered consumption.
        /// @param values Value stream.
        /// @param consumer Consumer invoked for each yielded value. Return `false` to stop consumption early.
        /// @return Number of consumed values.
        std::size_t drive_values(ValueGenerator values, const std::function<bool(const std::string&)>& consumer) const override
        {
            return drive_stream<std::string>(std::move(values),
                                             consumer,
                                             [this](ValueGenerator generator, const std::function<bool(const std::string&)>& serial_consumer)
                                             {
                                                 return serial_executor_.drive_values(std::move(generator), serial_consumer);
                                             });
        }

    private:
        template <typename T>
        struct StreamState
        {
            std::mutex mutex;
            std::condition_variable condition;
            std::deque<std::vector<T>> chunks;
            std::exception_ptr exception;
            bool done = false;
            bool stop = false;
        };

        template <typename T>
        void publish_chunk(const std::shared_ptr<StreamState<T>>& state, std::vector<T> chunk) const
        {
            if (chunk.empty())
            {
                return;
            }

            std::unique_lock lock(state->mutex);
            state->condition.wait(lock, [&]()
            {
                return state->stop || state->chunks.size() < buffered_chunk_count_;
            });
            if (state->stop)
            {
                return;
            }

            state->chunks.push_back(std::move(chunk));
            lock.unlock();
            state->condition.notify_all();
        }

        template <typename T, typename GeneratorType, typename SerialDriver>
        std::size_t drive_stream(GeneratorType generator,
                                 const std::function<bool(const T&)>& consumer,
                                 SerialDriver&& serial_driver) const
        {
            if (scheduler_->max_parallelism() <= 1)
            {
                return serial_driver(std::move(generator), consumer);
            }

            auto state = std::make_shared<StreamState<T>>();
            auto producer = scheduler_->fork([state, generator = std::move(generator), this]() mutable
            {
                try
                {
                    std::vector<T> chunk;
                    chunk.reserve(chunk_size_);
                    for (const auto& value : generator)
                    {
                        {
                            std::scoped_lock lock(state->mutex);
                            if (state->stop)
                            {
                                break;
                            }
                        }

                        chunk.push_back(value);
                        if (chunk.size() >= chunk_size_)
                        {
                            publish_chunk(state, std::move(chunk));
                            chunk = std::vector<T>();
                            chunk.reserve(chunk_size_);
                        }
                    }

                    publish_chunk(state, std::move(chunk));
                }
                catch (...)
                {
                    std::scoped_lock lock(state->mutex);
                    state->exception = std::current_exception();
                }

                {
                    std::scoped_lock lock(state->mutex);
                    state->done = true;
                }
                state->condition.notify_all();
            });

            std::size_t count = 0;
            try
            {
                while (true)
                {
                    std::vector<T> chunk;
                    {
                        std::unique_lock lock(state->mutex);
                        state->condition.wait(lock, [&]()
                        {
                            return !state->chunks.empty() || state->done;
                        });

                        if (state->chunks.empty())
                        {
                            break;
                        }

                        chunk = std::move(state->chunks.front());
                        state->chunks.pop_front();
                    }
                    state->condition.notify_all();

                    for (const auto& value : chunk)
                    {
                        ++count;
                        if (!consumer(value))
                        {
                            {
                                std::scoped_lock lock(state->mutex);
                                state->stop = true;
                            }
                            state->condition.notify_all();
                            producer.get();
                            if (state->exception)
                            {
                                std::rethrow_exception(state->exception);
                            }
                            return count;
                        }
                    }
                }
            }
            catch (...)
            {
                {
                    std::scoped_lock lock(state->mutex);
                    state->stop = true;
                }
                state->condition.notify_all();
                producer.get();
                throw;
            }

            producer.get();
            if (state->exception)
            {
                std::rethrow_exception(state->exception);
            }
            return count;
        }

        std::shared_ptr<ForkJoinScheduler> scheduler_;
        std::size_t chunk_size_ = 64;
        std::size_t buffered_chunk_count_ = 2;
        SerialCoroExecutor serial_executor_;
    };
}



