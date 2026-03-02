#include "async/thread_pool.hpp"
#include <queue>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <atomic>
#include <spdlog/spdlog.h>

namespace surge::async {

struct ThreadPool::Impl {
    std::vector<std::jthread> workers;
    std::queue<std::function<void()>> tasks;
    std::queue<std::function<void(std::stop_token)>> cancellable_tasks;
    mutable std::mutex mutex;
    std::condition_variable_any cv;
    std::condition_variable_any idle_cv;
    std::atomic<int64_t> active_count{0};
    bool shutdown = false;
    int32_t num_threads = 0;

    explicit Impl(int32_t count) : num_threads(count) {
        spdlog::debug("Creating thread pool with {} threads", count);
        workers.reserve(static_cast<size_t>(count));
        for (int32_t i = 0; i < count; ++i) {
            workers.emplace_back([this](std::stop_token stop_token) {
                worker_loop(stop_token);
            });
        }
    }

    ~Impl() {
        do_shutdown();
    }

    void worker_loop(std::stop_token stop_token) {
        while (!stop_token.stop_requested()) {
            std::function<void()> task;
            std::function<void(std::stop_token)> cancellable_task;

            {
                std::unique_lock lock(mutex);
                cv.wait(lock, stop_token, [this] {
                    return shutdown || !tasks.empty() || !cancellable_tasks.empty();
                });

                if (tasks.empty() && cancellable_tasks.empty()) {
                    if (shutdown || stop_token.stop_requested()) return;
                    continue;
                }

                if (!tasks.empty()) {
                    task = std::move(tasks.front());
                    tasks.pop();
                } else if (!cancellable_tasks.empty()) {
                    cancellable_task = std::move(cancellable_tasks.front());
                    cancellable_tasks.pop();
                }
            }

            active_count.fetch_add(1);

            try {
                if (task) {
                    task();
                } else if (cancellable_task) {
                    cancellable_task(stop_token);
                }
            } catch (const std::exception& e) {
                spdlog::error("Thread pool task threw exception: {}", e.what());
            } catch (...) {
                spdlog::error("Thread pool task threw unknown exception");
            }

            active_count.fetch_sub(1);
            idle_cv.notify_all();
        }
    }

    void do_shutdown() {
        {
            std::unique_lock lock(mutex);
            if (shutdown) return;
            shutdown = true;
        }
        cv.notify_all();

        for (auto& w : workers) {
            w.request_stop();
        }
        cv.notify_all();

        for (auto& w : workers) {
            if (w.joinable()) {
                w.join();
            }
        }
        workers.clear();
        spdlog::debug("Thread pool shut down");
    }
};

ThreadPool::ThreadPool(int32_t thread_count) {
    int32_t count = thread_count;
    if (count <= 0) {
        count = static_cast<int32_t>(
            std::max(1u, std::thread::hardware_concurrency()));
    }
    impl_ = std::make_unique<Impl>(count);
}

ThreadPool::~ThreadPool() = default;

void ThreadPool::enqueue(std::function<void()> task) {
    {
        std::unique_lock lock(impl_->mutex);
        if (impl_->shutdown) {
            throw std::runtime_error("Cannot enqueue on a shut down thread pool");
        }
        impl_->tasks.push(std::move(task));
    }
    impl_->cv.notify_one();
}

void ThreadPool::enqueue_cancellable(std::function<void(std::stop_token)> task) {
    {
        std::unique_lock lock(impl_->mutex);
        if (impl_->shutdown) {
            throw std::runtime_error("Cannot enqueue on a shut down thread pool");
        }
        impl_->cancellable_tasks.push(std::move(task));
    }
    impl_->cv.notify_one();
}

int32_t ThreadPool::thread_count() const {
    return impl_->num_threads;
}

int64_t ThreadPool::pending_count() const {
    std::unique_lock lock(impl_->mutex);
    return static_cast<int64_t>(impl_->tasks.size() + impl_->cancellable_tasks.size());
}

void ThreadPool::request_stop() {
    for (auto& w : impl_->workers) {
        w.request_stop();
    }
    impl_->cv.notify_all();
}

void ThreadPool::wait_all() {
    std::unique_lock lock(impl_->mutex);
    impl_->idle_cv.wait(lock, [this] {
        return impl_->tasks.empty() &&
               impl_->cancellable_tasks.empty() &&
               impl_->active_count.load() == 0;
    });
}

} // namespace surge::async
