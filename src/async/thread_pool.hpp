/**
 * @file thread_pool.hpp
 * @brief Thread pool with std::jthread and configurable concurrency.
 */

#pragma once

#include <concepts>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <stop_token>
#include <type_traits>

namespace surge::async {

/**
 * A work-stealing thread pool built on std::jthread.
 *
 * Supports submitting arbitrary callables and receiving results via
 * std::future. Threads are cooperatively cancellable through std::stop_token.
 *
 * Usage:
 * @code
 *   ThreadPool pool(4);
 *   auto f = pool.submit([] { return expensive_computation(); });
 *   auto result = f.get();
 * @endcode
 */
class ThreadPool {
public:
    /**
     * Construct a thread pool.
     * @param thread_count Number of worker threads. 0 = hardware_concurrency().
     */
    explicit ThreadPool(int32_t thread_count = 0);

    /**
     * Destroy the pool. Requests all threads to stop and joins them.
     * Pending tasks that have not started are abandoned.
     */
    ~ThreadPool();

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    /**
     * Submit a callable for asynchronous execution.
     * @param task Any callable (function, lambda, std::function, etc.).
     * @return A std::future holding the result of the callable.
     */
    template <typename F>
        requires std::invocable<F>
    auto submit(F&& task) -> std::future<std::invoke_result_t<F>> {
        using R = std::invoke_result_t<F>;
        auto promise = std::make_shared<std::promise<R>>();
        auto future = promise->get_future();

        enqueue([p = std::move(promise), t = std::forward<F>(task)]() mutable {
            try {
                if constexpr (std::is_void_v<R>) {
                    t();
                    p->set_value();
                } else {
                    p->set_value(t());
                }
            } catch (...) {
                p->set_exception(std::current_exception());
            }
        });

        return future;
    }

    /**
     * Submit a callable that accepts a std::stop_token for cooperative
     * cancellation.
     * @param task Callable accepting a std::stop_token as its first argument.
     * @return A std::future holding the result.
     */
    template <typename F>
        requires std::invocable<F, std::stop_token>
    auto submit_cancellable(F&& task) -> std::future<std::invoke_result_t<F, std::stop_token>> {
        using R = std::invoke_result_t<F, std::stop_token>;
        auto promise = std::make_shared<std::promise<R>>();
        auto future = promise->get_future();

        enqueue_cancellable([p = std::move(promise), t = std::forward<F>(task)](std::stop_token st) mutable {
            try {
                if constexpr (std::is_void_v<R>) {
                    t(st);
                    p->set_value();
                } else {
                    p->set_value(t(st));
                }
            } catch (...) {
                p->set_exception(std::current_exception());
            }
        });

        return future;
    }

    /** Return the number of worker threads. */
    int32_t thread_count() const;

    /** Return the approximate number of queued tasks. */
    int64_t pending_count() const;

    /** Request all threads to stop (cooperative cancellation). */
    void request_stop();

    /** Wait for all currently queued tasks to complete. */
    void wait_all();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;

    /** Enqueue a type-erased task for execution. */
    void enqueue(std::function<void()> task);

    /** Enqueue a type-erased cancellable task. */
    void enqueue_cancellable(std::function<void(std::stop_token)> task);
};

}  // namespace surge::async
