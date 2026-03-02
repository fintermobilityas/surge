/**
 * @file distributed_mutex.cpp
 * @brief Distributed lock using HTTP API (port from DistributedMutex.cs).
 */

#include "lock/distributed_mutex.hpp"
#include <curl/curl.h>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <fmt/format.h>
#include <chrono>
#include <cstring>
#include <thread>

namespace surge::lock {

namespace {

size_t write_string_callback(char* ptr, size_t size, size_t nmemb, void* userdata) {
    auto* str = static_cast<std::string*>(userdata);
    size_t total = size * nmemb;
    str->append(ptr, total);
    return total;
}

} // anonymous namespace

class DistributedMutex::Impl {
public:
    std::string server_url;
    std::string name;
    std::string challenge;
    bool acquired = false;
    bool disposed = false;
    bool release_on_destroy = true;
    std::stop_token stop_token;

    int32_t http_post_json(const std::string& url, const nlohmann::json& body,
                            std::string& response) {
        auto* curl = curl_easy_init();
        if (!curl) return SURGE_ERROR;

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

    int32_t http_delete_json(const std::string& url, const nlohmann::json& body,
                              std::string& response) {
        auto* curl = curl_easy_init();
        if (!curl) return SURGE_ERROR;

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

        if (res != CURLE_OK) return SURGE_ERROR;
        if (http_code < 200 || http_code >= 300) return SURGE_ERROR;
        return SURGE_OK;
    }
};

DistributedMutex::DistributedMutex(const std::string& server_url,
                                   const std::string& name,
                                   bool release_on_destroy,
                                   std::stop_token stop_token)
    : impl_(std::make_unique<Impl>())
{
    impl_->server_url = server_url;
    impl_->name = name;
    impl_->release_on_destroy = release_on_destroy;
    impl_->stop_token = std::move(stop_token);
}

DistributedMutex::~DistributedMutex() {
    if (impl_ && impl_->acquired && impl_->release_on_destroy && !impl_->disposed) {
        spdlog::info("Disposing mutex: {}", impl_->name);
        // Best-effort release on destruction
        try_release();
    }
}

DistributedMutex::DistributedMutex(DistributedMutex&&) noexcept = default;
DistributedMutex& DistributedMutex::operator=(DistributedMutex&&) noexcept = default;

bool DistributedMutex::try_acquire(std::chrono::milliseconds retry_delay, int retries) {
    if (impl_->disposed) {
        spdlog::error("Cannot acquire disposed mutex: {}", impl_->name);
        return false;
    }
    if (impl_->acquired) {
        spdlog::error("Mutex already acquired: {}", impl_->name);
        return false;
    }

    retries = std::max(0, retries);
    int attempt = 0;

    while (true) {
        if (impl_->stop_token.stop_requested()) {
            spdlog::info("Lock acquisition cancelled: {}", impl_->name);
            return false;
        }

        spdlog::info("Attempting to acquire mutex: {} (attempt {})", impl_->name, attempt + 1);

        nlohmann::json body;
        body["name"] = impl_->name;
        body["duration"] = "24:00:00"; // 24 hours

        std::string response;
        std::string lock_url = impl_->server_url + "/lock";
        auto rc = impl_->http_post_json(lock_url, body, response);

        if (rc == SURGE_OK && !response.empty()) {
            impl_->challenge = response;
            // Strip quotes if JSON-encoded string
            if (impl_->challenge.front() == '"' && impl_->challenge.back() == '"') {
                impl_->challenge = impl_->challenge.substr(1, impl_->challenge.size() - 2);
            }
            impl_->acquired = true;
            spdlog::info("Successfully acquired mutex: {}", impl_->name);
            return true;
        }

        attempt++;
        if (attempt > retries) {
            spdlog::error("Failed to acquire mutex after {} attempts: {}", attempt, impl_->name);
            return false;
        }

        spdlog::info("Retrying lock acquisition in {}ms", retry_delay.count());
        std::this_thread::sleep_for(retry_delay);
    }
}

bool DistributedMutex::try_release() {
    if (impl_->disposed || !impl_->acquired) return false;

    spdlog::info("Attempting to release mutex: {}", impl_->name);

    nlohmann::json body;
    body["name"] = impl_->name;
    body["challenge"] = impl_->challenge;
    body["breakPeriod"] = "00:00:00"; // immediate

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

bool DistributedMutex::is_acquired() const {
    return impl_->acquired;
}

const std::string& DistributedMutex::name() const {
    return impl_->name;
}

const std::string& DistributedMutex::challenge() const {
    return impl_->challenge;
}

void DistributedMutex::dispose() {
    if (impl_->disposed) return;

    if (impl_->acquired && impl_->release_on_destroy) {
        // Retry release up to 3 times
        for (int i = 0; i < 3; ++i) {
            if (try_release()) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
    }

    impl_->disposed = true;
}

} // namespace surge::lock
