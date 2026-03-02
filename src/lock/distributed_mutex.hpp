/**
 * @file distributed_mutex.hpp
 * @brief Distributed mutex using a remote lock server for coordinating
 *        concurrent release operations.
 */

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace surge {
class Context;
}

namespace surge::lock {

/**
 * A distributed mutex that coordinates access to shared resources (e.g. the
 * release index) across multiple machines using a remote lock server.
 *
 * Supports RAII via DistributedLockGuard.
 */
class DistributedMutex {
public:
    /**
     * Construct a distributed mutex.
     * @param ctx  Surge context with lock-server configuration.
     * @param name Lock name (unique identifier for the resource).
     */
    DistributedMutex(Context& ctx, std::string name);
    ~DistributedMutex();

    DistributedMutex(const DistributedMutex&) = delete;
    DistributedMutex& operator=(const DistributedMutex&) = delete;

    /**
     * Attempt to acquire the lock, blocking up to @p timeout_seconds.
     * @param timeout_seconds Maximum time to wait.
     * @return true if the lock was acquired.
     */
    bool try_acquire(int32_t timeout_seconds = 30);

    /**
     * Release a previously acquired lock.
     * @return true on success, false if the lock was not held or the
     *         server rejected the release.
     */
    bool try_release();

    /** Return true if this mutex currently holds the lock. */
    bool is_locked() const;

    /** Return the lock name. */
    const std::string& name() const;

    /** Return the challenge token (valid only while lock is held). */
    std::optional<std::string> challenge() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

/**
 * RAII guard that acquires a DistributedMutex on construction and releases
 * it on destruction.
 *
 * Usage:
 * @code
 *   DistributedMutex mtx(ctx, "releases-index");
 *   {
 *       DistributedLockGuard guard(mtx, 30);
 *       if (guard.owns_lock()) {
 *           // safe to modify shared resource
 *       }
 *   }
 * @endcode
 */
class DistributedLockGuard {
public:
    /**
     * Construct and attempt to acquire the lock.
     * @param mutex           Mutex to lock.
     * @param timeout_seconds Maximum time to wait for acquisition.
     */
    explicit DistributedLockGuard(DistributedMutex& mutex, int32_t timeout_seconds = 30);
    ~DistributedLockGuard();

    DistributedLockGuard(const DistributedLockGuard&) = delete;
    DistributedLockGuard& operator=(const DistributedLockGuard&) = delete;

    /** Return true if the lock is currently held. */
    bool owns_lock() const;

private:
    DistributedMutex& mutex_;
    bool locked_ = false;
};

}  // namespace surge::lock
