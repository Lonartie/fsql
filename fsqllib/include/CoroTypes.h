#pragma once

#include "SqlTypes.h"

#include <coroutine>
#include <exception>
#include <optional>
#include <utility>

namespace fsql
{
    /// @brief Coroutine-backed generator for yielding values one at a time.
    template <typename T>
    class Generator
    {
    public:
        struct promise_type;
        using handle_type = std::coroutine_handle<promise_type>;

        /// @brief Promise type for the generator coroutine.
        struct promise_type
        {
            /// @brief Current yielded value.
            std::optional<T> current_value;

            /// @brief Captured coroutine exception.
            std::exception_ptr exception;

            /// @brief Builds the generator wrapper.
            /// @return Generator instance.
            Generator get_return_object()
            {
                return Generator(handle_type::from_promise(*this));
            }

            /// @brief Suspends before first execution.
            /// @return Awaitable suspend object.
            static std::suspend_always initial_suspend() noexcept
            {
                return {};
            }

            /// @brief Suspends at the end for orderly destruction.
            /// @return Awaitable suspend object.
            static std::suspend_always final_suspend() noexcept
            {
                return {};
            }

            /// @brief Stores a yielded value.
            /// @param value Yielded value.
            /// @return Awaitable suspend object.
            std::suspend_always yield_value(T value) noexcept
            {
                current_value = std::move(value);
                return {};
            }

            /// @brief Completes the coroutine without a final value.
            void return_void()
            {
            }

            /// @brief Captures an unhandled exception.
            void unhandled_exception()
            {
                exception = std::current_exception();
            }
        };

        /// @brief Iterator over yielded generator values.
        class iterator
        {
        public:
            /// @brief Initializes an end iterator.
            iterator() = default;

            /// @brief Initializes an iterator from a coroutine handle.
            /// @param handle Coroutine handle.
            explicit iterator(handle_type handle) : handle_(handle)
            {
            }

            /// @brief Advances to the next yielded value.
            /// @return Iterator reference.
            iterator& operator++()
            {
                resume();
                return *this;
            }

            /// @brief Accesses the current yielded value.
            /// @return Current value reference.
            const T& operator*() const
            {
                return *handle_.promise().current_value;
            }

            /// @brief Accesses the current yielded value pointer.
            /// @return Current value pointer.
            const T* operator->() const
            {
                return &(*handle_.promise().current_value);
            }

            /// @brief Compares against the end sentinel.
            /// @param sentinel End sentinel.
            /// @return `true` when iteration has finished.
            bool operator==(std::default_sentinel_t sentinel) const
            {
                static_cast<void>(sentinel);
                return handle_ == nullptr;
            }

        private:
            /// @brief Resumes the coroutine and handles completion.
            void resume()
            {
                if (!handle_)
                {
                    return;
                }

                handle_.resume();
                if (!handle_.done())
                {
                    return;
                }

                if (handle_.promise().exception)
                {
                    std::rethrow_exception(handle_.promise().exception);
                }
                handle_ = nullptr;
            }

            handle_type handle_ = nullptr;

            friend class Generator;
        };

        /// @brief Initializes an empty generator.
        Generator() = default;

        /// @brief Initializes a generator from a coroutine handle.
        /// @param handle Coroutine handle.
        explicit Generator(handle_type handle) : handle_(handle)
        {
        }

        Generator(const Generator&) = delete;
        Generator& operator=(const Generator&) = delete;

        /// @brief Moves the generator.
        /// @param other Source generator.
        Generator(Generator&& other) noexcept : handle_(std::exchange(other.handle_, nullptr))
        {
        }

        /// @brief Move-assigns the generator.
        /// @param other Source generator.
        /// @return Generator reference.
        Generator& operator=(Generator&& other) noexcept
        {
            if (this == &other)
            {
                return *this;
            }
            if (handle_)
            {
                handle_.destroy();
            }
            handle_ = std::exchange(other.handle_, nullptr);
            return *this;
        }

        /// @brief Destroys the coroutine if still active.
        ~Generator()
        {
            if (handle_)
            {
                handle_.destroy();
            }
        }

        /// @brief Begins iteration over yielded values.
        /// @return Iterator positioned at the first value or end.
        iterator begin()
        {
            if (!handle_)
            {
                return iterator();
            }

            handle_.resume();
            if (!handle_.done())
            {
                return iterator(handle_);
            }

            if (handle_.promise().exception)
            {
                std::rethrow_exception(handle_.promise().exception);
            }
            return iterator();
        }

        /// @brief Returns the end sentinel.
        /// @return End sentinel.
        std::default_sentinel_t end() const noexcept
        {
            return {};
        }

    private:
        handle_type handle_ = nullptr;
    };

    /// @brief Coroutine stream yielding rows.
    using RowGenerator = Generator<Row>;

    /// @brief Coroutine stream yielding scalar values.
    using ValueGenerator = Generator<std::string>;
}

