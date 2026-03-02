/**
 * @file distributed_mutex.cpp
 * @brief Distributed lock using HTTP API (port from DistributedMutex.cs).
 */

#include "lock/distributed_mutex.hpp"

#include "core/context.hpp"

#include <chrono>
#include <cstring>
#include <curl/curl.h>
#include <fmt/format.h>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <thread>

namespace surge::lock {

namespace {

size_t write_string_callback(char* ptr, size_t size, size_t nmemb, void* userdata) {
    auto* str = static_cast<std::string*>(userdata);
    size_t total = size * nmemb;
    str->append(ptr, total);
    return total;
}

}  // anonymous namespace

struct DistributedMutex::Impl {
    std::string server_url;
    std::string name;
    std::string challenge_token;
    bool acquired = false;

    int32_t http_post_json(const std::string& url, const nlohmann::json& body, std::string& response) {
        auto* curl = curl_easy_init();
        if (!curl)
            return SURGE_ERROR;

        std::string json_str = body.dump();

        struct curl_slist* headers = nullptr;
        headers = curl_slist_append(headers, "Content-Type: application/json; charset=utf-8");

        response.clear();
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_str.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, static_cast<long>(json_str.size()));
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_string_callback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);

        CURLcode res = curl_easy_perform(curl);
        long http_code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);

        if (res != CURLE_OK) {
            spdlog::error("Lock HTTP request failed: {}", curl_easy_strerror(res));
            return SURGE_ERROR;
        }
        if (http_code < 200 || http_code >= 300) {
            spdlog::error("Lock HTTP {}: {}", http_code, response);
            return SURGE_ERROR;
        }
        return SURGE_OK;
    }

    int32_t http_delete_json(const std::string& url, const nlohmann::json& body, std::string& response) {
        auto* curl = curl_easy_init();
        if (!curl)
            return SURGE_ERROR;

        std::string json_str = body.dump();

        struct curl_slist* headers = nullptr;
        headers = curl_slist_append(headers, "Content-Type: application/json; charset=utf-8");

        response.clear();
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_str.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, static_cast<long>(json_str.size()));
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_string_callback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);

        CURLcode res = curl_easy_perform(curl);
        long http_code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);

        if (res != CURLE_OK)
            return SURGE_ERROR;
        if (http_code < 200 || http_code >= 300)
            return SURGE_ERROR;
        return SURGE_OK;
    }
};

DistributedMutex::DistributedMutex(Context& ctx, std::string name) : impl_(std::make_unique<Impl>()) {
    impl_->server_url = ctx.lock_config().server_url;
    impl_->name = std::move(name);
}

DistributedMutex::~DistributedMutex() {
    if (impl_ && impl_->acquired) {
        // Best-effort release on destruction
        try_release();
    }
}

bool DistributedMutex::try_acquire(int32_t timeout_seconds) {
    if (impl_->acquired) {
        spdlog::error("Mutex already acquired: {}", impl_->name);
        return false;
    }

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_seconds);

    while (std::chrono::steady_clock::now() < deadline) {
        spdlog::info("Attempting to acquire mutex: {}", impl_->name);

        nlohmann::json body;
        body["name"] = impl_->name;
        body["duration"] = "24:00:00";  // 24 hours

        std::string response;
        std::string lock_url = impl_->server_url + "/lock";
        auto rc = impl_->http_post_json(lock_url, body, response);

        if (rc == SURGE_OK && !response.empty()) {
            impl_->challenge_token = response;
            // Strip quotes if JSON-encoded string
            if (impl_->challenge_token.front() == '"' && impl_->challenge_token.back() == '"') {
                impl_->challenge_token = impl_->challenge_token.substr(1, impl_->challenge_token.size() - 2);
            }
            impl_->acquired = true;
            spdlog::info("Successfully acquired mutex: {}", impl_->name);
            return true;
        }

        // Wait before retry
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    spdlog::error("Failed to acquire mutex within {} seconds: {}", timeout_seconds, impl_->name);
    return false;
}

bool DistributedMutex::try_release() {
    if (!impl_->acquired)
        return false;

    spdlog::info("Attempting to release mutex: {}", impl_->name);

    nlohmann::json body;
    body["name"] = impl_->name;
    body["challenge"] = impl_->challenge_token;
    body["breakPeriod"] = "00:00:00";  // immediate

    std::string response;
    std::string unlock_url = impl_->server_url + "/unlock";
    auto rc = impl_->http_delete_json(unlock_url, body, response);

    if (rc == SURGE_OK) {
        impl_->acquired = false;
        spdlog::info("Successfully released mutex: {}", impl_->name);
        return true;
    }

    spdlog::error("Failed to release mutex: {}", impl_->name);
    return false;
}

bool DistributedMutex::is_locked() const {
    return impl_->acquired;
}

const std::string& DistributedMutex::name() const {
    return impl_->name;
}

std::optional<std::string> DistributedMutex::challenge() const {
    if (impl_->acquired) {
        return impl_->challenge_token;
    }
    return std::nullopt;
}

// ----- DistributedLockGuard -----

DistributedLockGuard::DistributedLockGuard(DistributedMutex& mutex, int32_t timeout_seconds) : mutex_(mutex) {
    locked_ = mutex_.try_acquire(timeout_seconds);
}

DistributedLockGuard::~DistributedLockGuard() {
    if (locked_) {
        mutex_.try_release();
    }
}

bool DistributedLockGuard::owns_lock() const {
    return locked_;
}

}  // namespace surge::lock
