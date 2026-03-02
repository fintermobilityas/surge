/**
 * @file context.hpp
 * @brief Core context object holding configuration, cancellation, and error state.
 */

#pragma once

#include "surge/surge_api.h"

#include <memory>
#include <optional>
#include <stop_token>
#include <string>

namespace surge {

/* ----- configuration structs ----- */

struct StorageConfig {
    surge_storage_provider provider = SURGE_STORAGE_S3;
    std::string bucket;
    std::string region;
    std::string access_key;
    std::string secret_key;
    std::string endpoint;
    std::string prefix;
};

struct LockConfig {
    std::string server_url;
};

/* ----- context ----- */

class Context {
public:
    Context();
    ~Context();

    Context(const Context&) = delete;
    Context& operator=(const Context&) = delete;
    Context(Context&&) noexcept;
    Context& operator=(Context&&) noexcept;

    /** Record an error on this context. Thread-safe. */
    void set_last_error(int32_t code, std::string message);

    /** Retrieve the most recent error, or nullptr if none. */
    const surge_error* last_error() const;

    /** Clear the stored error. */
    void clear_error();

    /* -- storage -- */
    void set_storage_config(StorageConfig config);
    const StorageConfig& storage_config() const;

    /* -- lock server -- */
    void set_lock_config(LockConfig config);
    const LockConfig& lock_config() const;

    /* -- resource budget -- */
    void set_resource_budget(surge_resource_budget budget);
    const surge_resource_budget& resource_budget() const;

    /* -- cancellation -- */
    std::stop_source& stop_source();
    std::stop_token stop_token() const;

    /** Request cancellation of all operations tied to this context. */
    void cancel();

    /** Return true if cancellation has been requested. */
    bool is_cancelled() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace surge
