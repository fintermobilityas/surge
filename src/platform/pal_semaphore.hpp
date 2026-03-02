/**
 * @file pal_semaphore.hpp
 * @brief Platform Abstraction Layer -- named semaphore for single-instance enforcement.
 */

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace surge::platform {

/**
 * Cross-platform named semaphore for enforcing single-instance constraints.
 *
 * On Linux: uses POSIX named semaphores (sem_open).
 * On macOS: uses POSIX named semaphores (sem_open).
 * On Windows: uses CreateSemaphore with a Global\\ namespace name.
 *
 * Usage:
 * @code
 *   NamedSemaphore sem("com.example.app.update");
 *   if (sem.try_acquire()) {
 *       // we are the only instance running the update
 *       ...
 *       sem.release();
 *   }
 * @endcode
 */
class NamedSemaphore {
public:
    /**
     * Open or create a named semaphore.
     * @param name          System-wide unique name.
     * @param initial_count Initial semaphore count (default 1 for mutex-like behavior).
     */
    explicit NamedSemaphore(std::string_view name, int32_t initial_count = 1);
    ~NamedSemaphore();

    NamedSemaphore(const NamedSemaphore&) = delete;
    NamedSemaphore& operator=(const NamedSemaphore&) = delete;

    /**
     * Try to acquire the semaphore without blocking.
     * @return true if acquired.
     */
    bool try_acquire();

    /**
     * Acquire the semaphore, blocking up to @p timeout_ms milliseconds.
     * @param timeout_ms Maximum wait time (-1 = infinite).
     * @return true if acquired within the timeout.
     */
    bool acquire(int32_t timeout_ms = -1);

    /**
     * Release the semaphore.
     * @return true on success.
     */
    bool release();

    /** Return true if the semaphore is currently held by this instance. */
    bool is_held() const;

    /** Return the semaphore name. */
    const std::string& name() const;

    /**
     * Unlink / destroy the system semaphore object.
     * Only call this when you are certain no other process is using it.
     */
    static void unlink(std::string_view name);

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace surge::platform
