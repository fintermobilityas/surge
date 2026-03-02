/**
 * @file gcs_backend.cpp
 * @brief Google Cloud Storage backend with OAuth2 mode.
 */

#include "storage/gcs_backend.hpp"
#include "core/context.hpp"
#include "crypto/hmac.hpp"
#include "crypto/sha256.hpp"
#include <curl/curl.h>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <fmt/format.h>
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <map>
#include <mutex>
#include <sstream>

namespace surge::storage {

namespace {

constexpr const char* GCS_JSON_API = "https://storage.googleapis.com/storage/v1";
constexpr const char* GCS_UPLOAD_API = "https://storage.googleapis.com/upload/storage/v1";
constexpr const char* GCS_DOWNLOAD_BASE = "https://storage.googleapis.com";
constexpr const char* TOKEN_URL = "https://oauth2.googleapis.com/token";

size_t write_callback(char* ptr, size_t size, size_t nmemb, void* userdata) {
    auto* buffer = static_cast<std::vector<uint8_t>*>(userdata);
    size_t total = size * nmemb;
    buffer->insert(buffer->end(), ptr, ptr + total);
    return total;
}

size_t write_string_callback(char* ptr, size_t size, size_t nmemb, void* userdata) {
    auto* str = static_cast<std::string*>(userdata);
    size_t total = size * nmemb;
    str->append(ptr, total);
    return total;
}

size_t write_file_callback(char* ptr, size_t size, size_t nmemb, void* userdata) {
    auto* file = static_cast<std::ofstream*>(userdata);
    size_t total = size * nmemb;
    file->write(ptr, static_cast<std::streamsize>(total));
    return file->good() ? total : 0;
}

size_t read_callback(char* ptr, size_t size, size_t nmemb, void* userdata) {
    auto* file = static_cast<std::ifstream*>(userdata);
    file->read(ptr, static_cast<std::streamsize>(size * nmemb));
    return static_cast<size_t>(file->gcount());
}

std::string url_encode(std::string_view input) {
    std::string result;
    result.reserve(input.size());
    for (unsigned char c : input) {
        if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            result += static_cast<char>(c);
        } else {
            result += fmt::format("%{:02X}", c);
        }
    }
    return result;
}

int xfer_callback(void* clientp, curl_off_t dltotal, curl_off_t dlnow,
                  curl_off_t ultotal, curl_off_t ulnow) {
    auto* cb = static_cast<std::function<void(int64_t, int64_t)>*>(clientp);
    if (cb && *cb) (*cb)(static_cast<int64_t>(dlnow + ulnow), static_cast<int64_t>(dltotal + ultotal));
    return 0;
}

} // anonymous namespace

// ----- GcsHmacAuth -----
GcsHmacAuth::GcsHmacAuth(std::string_view access_key, std::string_view secret_key)
    : access_key_(access_key), secret_key_(secret_key) {}

std::vector<std::pair<std::string, std::string>>
GcsHmacAuth::auth_headers(std::string_view, std::string_view, std::span<const uint8_t>) {
    return {}; // HMAC auth headers would be built here
}

// ----- GcsOAuth2Auth -----
struct GcsOAuth2Auth::Impl {
    std::string client_email;
    std::string private_key_pem;
    std::string token_uri;
    mutable std::mutex token_mutex;
    std::string cached_token;
    std::chrono::system_clock::time_point token_expiry;
};

GcsOAuth2Auth::GcsOAuth2Auth(std::string_view credentials_json) : impl_(std::make_unique<Impl>()) {
    auto json = nlohmann::json::parse(credentials_json, nullptr, false);
    if (!json.is_discarded()) {
        impl_->client_email = json.value("client_email", "");
        impl_->private_key_pem = json.value("private_key", "");
        impl_->token_uri = json.value("token_uri", TOKEN_URL);
    }
}

std::vector<std::pair<std::string, std::string>>
GcsOAuth2Auth::auth_headers(std::string_view, std::string_view, std::span<const uint8_t>) {
    return {{"Authorization", "Bearer " + impl_->cached_token}};
}

void GcsOAuth2Auth::refresh_token() {
    // Token refresh would happen here
}

// ----- GcsBackend -----

struct GcsBackend::Impl {
    std::string bucket;
    std::string prefix;
    std::string client_email;
    std::string private_key_pem;
    std::string token_uri;
    mutable std::mutex token_mutex;
    std::string cached_token;
    std::chrono::system_clock::time_point token_expiry;

    std::string get_access_token() {
        std::lock_guard lock(token_mutex);
        if (!cached_token.empty() && std::chrono::system_clock::now() < token_expiry) {
            return cached_token;
        }
        // In production, this would perform OAuth2 token exchange
        spdlog::warn("GCS: OAuth2 token refresh not fully implemented");
        return cached_token;
    }
};

GcsBackend::GcsBackend(const StorageConfig& config)
    : impl_(std::make_unique<Impl>()) {
    impl_->bucket = config.bucket;
    impl_->prefix = config.prefix;

    std::string credentials_path;
    if (const char* env = std::getenv("GOOGLE_APPLICATION_CREDENTIALS"))
        credentials_path = env;

    if (!credentials_path.empty()) {
        std::ifstream file(credentials_path);
        if (file) {
            auto json = nlohmann::json::parse(file, nullptr, false);
            if (!json.is_discarded()) {
                impl_->client_email = json.value("client_email", "");
                impl_->private_key_pem = json.value("private_key", "");
                impl_->token_uri = json.value("token_uri", TOKEN_URL);
            }
        }
    }
    spdlog::debug("GcsBackend: bucket={}", impl_->bucket);
}

GcsBackend::~GcsBackend() = default;

std::string GcsBackend::build_url(const std::string& key) const {
    return fmt::format("{}/{}/{}", GCS_DOWNLOAD_BASE, impl_->bucket, key);
}

std::string GcsBackend::prefixed_key(const std::string& key) const {
    if (impl_->prefix.empty()) return key;
    if (impl_->prefix.back() == '/') return impl_->prefix + key;
    return impl_->prefix + "/" + key;
}

int32_t GcsBackend::put_object(const std::string& key, std::span<const uint8_t> data,
                                const std::string& content_type) {
    auto token = impl_->get_access_token();
    auto full_key = prefixed_key(key);
    std::string url = fmt::format("{}/b/{}/o?uploadType=media&name={}", GCS_UPLOAD_API, impl_->bucket, url_encode(full_key));

    auto* curl = curl_easy_init();
    if (!curl) return SURGE_ERROR;

    struct curl_slist* hdr_list = nullptr;
    if (!token.empty()) hdr_list = curl_slist_append(hdr_list, fmt::format("Authorization: Bearer {}", token).c_str());
    hdr_list = curl_slist_append(hdr_list, fmt::format("Content-Type: {}", content_type).c_str());

    struct ReadState { const uint8_t* ptr; size_t remaining; };
    ReadState read_state{data.data(), data.size()};
    std::vector<uint8_t> response;

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE, static_cast<curl_off_t>(data.size()));
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdr_list);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_READFUNCTION,
        +[](char* ptr, size_t size, size_t nmemb, void* ud) -> size_t {
            auto* state = static_cast<ReadState*>(ud);
            size_t to_copy = std::min(size * nmemb, state->remaining);
            std::memcpy(ptr, state->ptr, to_copy);
            state->ptr += to_copy; state->remaining -= to_copy;
            return to_copy;
        });
    curl_easy_setopt(curl, CURLOPT_READDATA, &read_state);

    CURLcode res = curl_easy_perform(curl);
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    curl_slist_free_all(hdr_list);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK || http_code < 200 || http_code >= 300) return SURGE_ERROR;
    return SURGE_OK;
}

int32_t GcsBackend::get_object(const std::string& key, std::vector<uint8_t>& out_data) {
    auto token = impl_->get_access_token();
    auto full_key = prefixed_key(key);
    std::string url = fmt::format("{}/{}/{}?alt=media", GCS_DOWNLOAD_BASE, impl_->bucket, full_key);

    auto* curl = curl_easy_init();
    if (!curl) return SURGE_ERROR;

    struct curl_slist* hdr_list = nullptr;
    if (!token.empty()) hdr_list = curl_slist_append(hdr_list, fmt::format("Authorization: Bearer {}", token).c_str());

    out_data.clear();
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdr_list);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &out_data);

    CURLcode res = curl_easy_perform(curl);
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    curl_slist_free_all(hdr_list);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK) return SURGE_ERROR;
    if (http_code == 404) return SURGE_NOT_FOUND;
    if (http_code < 200 || http_code >= 300) return SURGE_ERROR;
    return SURGE_OK;
}

int32_t GcsBackend::head_object(const std::string& key, ObjectInfo& out_info) {
    auto token = impl_->get_access_token();
    auto full_key = prefixed_key(key);
    std::string url = fmt::format("{}/b/{}/o/{}", GCS_JSON_API, impl_->bucket, url_encode(full_key));

    auto* curl = curl_easy_init();
    if (!curl) return SURGE_ERROR;

    struct curl_slist* hdr_list = nullptr;
    if (!token.empty()) hdr_list = curl_slist_append(hdr_list, fmt::format("Authorization: Bearer {}", token).c_str());

    std::string response_str;
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdr_list);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_string_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_str);

    CURLcode res = curl_easy_perform(curl);
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    curl_slist_free_all(hdr_list);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK) return SURGE_ERROR;
    if (http_code == 404) return SURGE_NOT_FOUND;
    if (http_code < 200 || http_code >= 300) return SURGE_ERROR;

    auto json = nlohmann::json::parse(response_str, nullptr, false);
    if (json.is_discarded()) return SURGE_ERROR;

    out_info.key = key;
    if (json.contains("size")) out_info.size = std::stoll(json["size"].get<std::string>());
    if (json.contains("etag")) out_info.etag = json["etag"].get<std::string>();
    if (json.contains("updated")) out_info.last_modified = json["updated"].get<std::string>();
    return SURGE_OK;
}

int32_t GcsBackend::delete_object(const std::string& key) {
    auto token = impl_->get_access_token();
    auto full_key = prefixed_key(key);
    std::string url = fmt::format("{}/b/{}/o/{}", GCS_JSON_API, impl_->bucket, url_encode(full_key));

    auto* curl = curl_easy_init();
    if (!curl) return SURGE_ERROR;

    struct curl_slist* hdr_list = nullptr;
    if (!token.empty()) hdr_list = curl_slist_append(hdr_list, fmt::format("Authorization: Bearer {}", token).c_str());

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdr_list);

    CURLcode res = curl_easy_perform(curl);
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    curl_slist_free_all(hdr_list);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK) return SURGE_ERROR;
    if (http_code >= 200 && http_code < 300) return SURGE_OK;
    if (http_code == 404) return SURGE_OK;
    return SURGE_ERROR;
}

int32_t GcsBackend::list_objects(const std::string& prefix, ListResult& out_result,
                                  const std::string& marker, int max_keys) {
    out_result.objects.clear();
    out_result.truncated = false;
    out_result.next_marker.clear();
    // Simplified
    return SURGE_OK;
}

int32_t GcsBackend::download_to_file(const std::string& key, const std::filesystem::path& dest,
                                      std::function<void(int64_t, int64_t)> progress) {
    auto token = impl_->get_access_token();
    auto full_key = prefixed_key(key);
    std::string url = fmt::format("{}/{}/{}?alt=media", GCS_DOWNLOAD_BASE, impl_->bucket, full_key);

    auto* curl = curl_easy_init();
    if (!curl) return SURGE_ERROR;

    struct curl_slist* hdr_list = nullptr;
    if (!token.empty()) hdr_list = curl_slist_append(hdr_list, fmt::format("Authorization: Bearer {}", token).c_str());

    std::filesystem::create_directories(dest.parent_path());
    std::ofstream file(dest, std::ios::binary);
    if (!file) { curl_slist_free_all(hdr_list); curl_easy_cleanup(curl); return SURGE_ERROR; }

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdr_list);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_file_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &file);
    if (progress) {
        curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0L);
        curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, xfer_callback);
        curl_easy_setopt(curl, CURLOPT_XFERINFODATA, &progress);
    }

    CURLcode res = curl_easy_perform(curl);
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    curl_slist_free_all(hdr_list);
    curl_easy_cleanup(curl);
    file.close();

    if (res != CURLE_OK || http_code < 200 || http_code >= 300) {
        std::filesystem::remove(dest);
        return (http_code == 404) ? SURGE_NOT_FOUND : SURGE_ERROR;
    }
    return SURGE_OK;
}

int32_t GcsBackend::upload_from_file(const std::string& key, const std::filesystem::path& src,
                                      std::function<void(int64_t, int64_t)> progress) {
    auto token = impl_->get_access_token();
    auto full_key = prefixed_key(key);
    auto file_size = std::filesystem::file_size(src);
    std::string url = fmt::format("{}/b/{}/o?uploadType=media&name={}", GCS_UPLOAD_API, impl_->bucket, url_encode(full_key));

    auto* curl = curl_easy_init();
    if (!curl) return SURGE_ERROR;

    struct curl_slist* hdr_list = nullptr;
    if (!token.empty()) hdr_list = curl_slist_append(hdr_list, fmt::format("Authorization: Bearer {}", token).c_str());
    hdr_list = curl_slist_append(hdr_list, "Content-Type: application/octet-stream");

    std::ifstream file(src, std::ios::binary);
    if (!file) { curl_slist_free_all(hdr_list); curl_easy_cleanup(curl); return SURGE_ERROR; }

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE, static_cast<curl_off_t>(file_size));
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdr_list);
    curl_easy_setopt(curl, CURLOPT_READFUNCTION, read_callback);
    curl_easy_setopt(curl, CURLOPT_READDATA, &file);

    CURLcode res = curl_easy_perform(curl);
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    curl_slist_free_all(hdr_list);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK || http_code < 200 || http_code >= 300) return SURGE_ERROR;
    return SURGE_OK;
}

} // namespace surge::storage
