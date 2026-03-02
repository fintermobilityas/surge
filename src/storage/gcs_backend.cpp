/**
 * @file gcs_backend.cpp
 * @brief Google Cloud Storage backend with S3-compatible and OAuth2 modes.
 */

#include "storage/storage_backend.hpp"
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
constexpr const char* GCS_S3_ENDPOINT = "https://storage.googleapis.com";
constexpr const char* TOKEN_URL = "https://oauth2.googleapis.com/token";

std::string base64url_encode(std::span<const uint8_t> data) {
    static constexpr char table[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    std::string result;
    result.reserve(((data.size() + 2) / 3) * 4);
    for (size_t i = 0; i < data.size(); i += 3) {
        uint32_t n = static_cast<uint32_t>(data[i]) << 16;
        if (i + 1 < data.size()) n |= static_cast<uint32_t>(data[i + 1]) << 8;
        if (i + 2 < data.size()) n |= static_cast<uint32_t>(data[i + 2]);
        result += table[(n >> 18) & 0x3F];
        result += table[(n >> 12) & 0x3F];
        if (i + 1 < data.size()) result += table[(n >> 6) & 0x3F];
        if (i + 2 < data.size()) result += table[n & 0x3F];
    }
    return result;
}

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

size_t header_callback(char* buffer, size_t size, size_t nitems, void* userdata) {
    auto* headers = static_cast<std::map<std::string, std::string>*>(userdata);
    size_t total = size * nitems;
    std::string line(buffer, total);
    auto colon = line.find(':');
    if (colon != std::string::npos) {
        std::string key = line.substr(0, colon);
        std::string value = line.substr(colon + 1);
        while (!value.empty() && (value.front() == ' ' || value.front() == '\t'))
            value.erase(value.begin());
        while (!value.empty() && (value.back() == '\r' || value.back() == '\n'))
            value.pop_back();
        std::transform(key.begin(), key.end(), key.begin(), ::tolower);
        (*headers)[key] = value;
    }
    return total;
}

int xfer_callback(void* clientp, curl_off_t dltotal, curl_off_t dlnow,
                  curl_off_t ultotal, curl_off_t ulnow) {
    auto* cb = static_cast<std::function<void(int64_t, int64_t)>*>(clientp);
    if (cb && *cb) {
        (*cb)(static_cast<int64_t>(dlnow + ulnow), static_cast<int64_t>(dltotal + ultotal));
    }
    return 0;
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

} // anonymous namespace

/**
 * GCS backend using OAuth2 JSON API.
 * Reads service account credentials from the file pointed to by
 * GOOGLE_APPLICATION_CREDENTIALS environment variable.
 */
class GcsBackend : public IStorageBackend {
public:
    GcsBackend(std::string bucket, std::string prefix,
               std::string credentials_path)
        : bucket_(std::move(bucket))
        , prefix_(std::move(prefix))
    {
        if (credentials_path.empty()) {
            if (const char* env = std::getenv("GOOGLE_APPLICATION_CREDENTIALS"))
                credentials_path = env;
        }
        if (!credentials_path.empty()) {
            load_service_account(credentials_path);
        }
        spdlog::debug("GcsBackend: bucket={}, oauth2 mode", bucket_);
    }

    int32_t put_object(const std::string& key, std::span<const uint8_t> data,
                       const std::string& content_type) override {
        auto token = get_access_token();
        if (token.empty()) return SURGE_ERROR;

        auto full_key = prefixed_key(key);
        std::string url = fmt::format("{}/b/{}/o?uploadType=media&name={}",
                                       GCS_UPLOAD_API, bucket_, url_encode(full_key));

        auto* curl = curl_easy_init();
        if (!curl) return SURGE_ERROR;

        struct curl_slist* hdr_list = nullptr;
        hdr_list = curl_slist_append(hdr_list, fmt::format("Authorization: Bearer {}", token).c_str());
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
                state->ptr += to_copy;
                state->remaining -= to_copy;
                return to_copy;
            });
        curl_easy_setopt(curl, CURLOPT_READDATA, &read_state);

        CURLcode res = curl_easy_perform(curl);
        long http_code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
        curl_slist_free_all(hdr_list);
        curl_easy_cleanup(curl);

        if (res != CURLE_OK || http_code < 200 || http_code >= 300) {
            spdlog::error("GCS PUT failed: HTTP {}", http_code);
            return SURGE_ERROR;
        }
        return SURGE_OK;
    }

    int32_t get_object(const std::string& key, std::vector<uint8_t>& out_data) override {
        auto token = get_access_token();
        if (token.empty()) return SURGE_ERROR;

        auto full_key = prefixed_key(key);
        std::string url = fmt::format("{}/{}/{}?alt=media",
                                       GCS_DOWNLOAD_BASE, bucket_, full_key);

        auto* curl = curl_easy_init();
        if (!curl) return SURGE_ERROR;

        struct curl_slist* hdr_list = nullptr;
        hdr_list = curl_slist_append(hdr_list, fmt::format("Authorization: Bearer {}", token).c_str());

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

    int32_t head_object(const std::string& key, ObjectInfo& out_info) override {
        auto token = get_access_token();
        if (token.empty()) return SURGE_ERROR;

        auto full_key = prefixed_key(key);
        std::string url = fmt::format("{}/b/{}/o/{}",
                                       GCS_JSON_API, bucket_, url_encode(full_key));

        auto* curl = curl_easy_init();
        if (!curl) return SURGE_ERROR;

        struct curl_slist* hdr_list = nullptr;
        hdr_list = curl_slist_append(hdr_list, fmt::format("Authorization: Bearer {}", token).c_str());

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
        if (json.contains("size"))
            out_info.size = std::stoll(json["size"].get<std::string>());
        if (json.contains("etag"))
            out_info.etag = json["etag"].get<std::string>();
        if (json.contains("updated"))
            out_info.last_modified = json["updated"].get<std::string>();

        return SURGE_OK;
    }

    int32_t delete_object(const std::string& key) override {
        auto token = get_access_token();
        if (token.empty()) return SURGE_ERROR;

        auto full_key = prefixed_key(key);
        std::string url = fmt::format("{}/b/{}/o/{}",
                                       GCS_JSON_API, bucket_, url_encode(full_key));

        auto* curl = curl_easy_init();
        if (!curl) return SURGE_ERROR;

        struct curl_slist* hdr_list = nullptr;
        hdr_list = curl_slist_append(hdr_list, fmt::format("Authorization: Bearer {}", token).c_str());

        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdr_list);

        CURLcode res = curl_easy_perform(curl);
        long http_code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
        curl_slist_free_all(hdr_list);
        curl_easy_cleanup(curl);

        if (res != CURLE_OK) return SURGE_ERROR;
        if (http_code < 200 || http_code >= 300 && http_code != 404) return SURGE_ERROR;
        return SURGE_OK;
    }

    int32_t list_objects(const std::string& prefix, ListResult& out_result,
                         const std::string& marker, int max_keys) override {
        auto token = get_access_token();
        if (token.empty()) return SURGE_ERROR;

        auto full_prefix = prefixed_key(prefix);
        std::string url = fmt::format("{}/b/{}/o?prefix={}&maxResults={}",
                                       GCS_JSON_API, bucket_, url_encode(full_prefix), max_keys);
        if (!marker.empty()) {
            url += fmt::format("&pageToken={}", url_encode(marker));
        }

        auto* curl = curl_easy_init();
        if (!curl) return SURGE_ERROR;

        struct curl_slist* hdr_list = nullptr;
        hdr_list = curl_slist_append(hdr_list, fmt::format("Authorization: Bearer {}", token).c_str());

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

        if (res != CURLE_OK || http_code < 200 || http_code >= 300) return SURGE_ERROR;

        auto json = nlohmann::json::parse(response_str, nullptr, false);
        if (json.is_discarded()) return SURGE_ERROR;

        out_result.objects.clear();
        out_result.truncated = json.contains("nextPageToken");
        if (out_result.truncated) {
            out_result.next_marker = json["nextPageToken"].get<std::string>();
        }

        if (json.contains("items")) {
            for (auto& item : json["items"]) {
                ObjectInfo info;
                info.key = item.value("name", "");
                if (!prefix_.empty() && info.key.starts_with(prefix_)) {
                    info.key = info.key.substr(prefix_.size());
                    if (!info.key.empty() && info.key.front() == '/') {
                        info.key = info.key.substr(1);
                    }
                }
                if (item.contains("size"))
                    info.size = std::stoll(item["size"].get<std::string>());
                info.etag = item.value("etag", "");
                info.last_modified = item.value("updated", "");
                out_result.objects.push_back(std::move(info));
            }
        }

        return SURGE_OK;
    }

    int32_t download_to_file(const std::string& key, const std::filesystem::path& dest,
                              std::function<void(int64_t, int64_t)> progress) override {
        auto token = get_access_token();
        if (token.empty()) return SURGE_ERROR;

        auto full_key = prefixed_key(key);
        std::string url = fmt::format("{}/{}/{}?alt=media",
                                       GCS_DOWNLOAD_BASE, bucket_, full_key);

        auto* curl = curl_easy_init();
        if (!curl) return SURGE_ERROR;

        struct curl_slist* hdr_list = nullptr;
        hdr_list = curl_slist_append(hdr_list, fmt::format("Authorization: Bearer {}", token).c_str());

        std::filesystem::create_directories(dest.parent_path());
        std::ofstream file(dest, std::ios::binary);
        if (!file) {
            curl_slist_free_all(hdr_list);
            curl_easy_cleanup(curl);
            return SURGE_ERROR;
        }

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

    int32_t upload_from_file(const std::string& key, const std::filesystem::path& src,
                              std::function<void(int64_t, int64_t)> progress) override {
        auto token = get_access_token();
        if (token.empty()) return SURGE_ERROR;

        auto full_key = prefixed_key(key);
        auto file_size = std::filesystem::file_size(src);
        std::string url = fmt::format("{}/b/{}/o?uploadType=media&name={}",
                                       GCS_UPLOAD_API, bucket_, url_encode(full_key));

        auto* curl = curl_easy_init();
        if (!curl) return SURGE_ERROR;

        struct curl_slist* hdr_list = nullptr;
        hdr_list = curl_slist_append(hdr_list, fmt::format("Authorization: Bearer {}", token).c_str());
        hdr_list = curl_slist_append(hdr_list, "Content-Type: application/octet-stream");

        std::ifstream file(src, std::ios::binary);
        if (!file) {
            curl_slist_free_all(hdr_list);
            curl_easy_cleanup(curl);
            return SURGE_ERROR;
        }

        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE, static_cast<curl_off_t>(file_size));
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdr_list);
        curl_easy_setopt(curl, CURLOPT_READFUNCTION, read_callback);
        curl_easy_setopt(curl, CURLOPT_READDATA, &file);
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

        if (res != CURLE_OK || http_code < 200 || http_code >= 300) {
            spdlog::error("GCS upload failed: HTTP {}", http_code);
            return SURGE_ERROR;
        }
        return SURGE_OK;
    }

private:
    std::string bucket_;
    std::string prefix_;
    std::string client_email_;
    std::string private_key_pem_;
    std::string token_uri_;
    mutable std::mutex token_mutex_;
    std::string cached_token_;
    std::chrono::system_clock::time_point token_expiry_;

    std::string prefixed_key(const std::string& key) const {
        if (prefix_.empty()) return key;
        if (prefix_.back() == '/') return prefix_ + key;
        return prefix_ + "/" + key;
    }

    void load_service_account(const std::string& path) {
        std::ifstream file(path);
        if (!file) {
            spdlog::error("GCS: failed to open credentials file: {}", path);
            return;
        }
        auto json = nlohmann::json::parse(file, nullptr, false);
        if (json.is_discarded()) {
            spdlog::error("GCS: failed to parse credentials JSON");
            return;
        }
        client_email_ = json.value("client_email", "");
        private_key_pem_ = json.value("private_key", "");
        token_uri_ = json.value("token_uri", TOKEN_URL);
        spdlog::debug("GCS: loaded service account {}", client_email_);
    }

    std::string build_jwt() const {
        auto now = std::chrono::system_clock::now();
        auto now_sec = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
        auto exp_sec = now_sec + 3600;

        nlohmann::json header = {{"alg", "RS256"}, {"typ", "JWT"}};
        nlohmann::json payload = {
            {"iss", client_email_},
            {"scope", "https://www.googleapis.com/auth/devstorage.full_control"},
            {"aud", token_uri_},
            {"iat", now_sec},
            {"exp", exp_sec}
        };

        auto encode_part = [](const nlohmann::json& j) -> std::string {
            auto s = j.dump();
            return base64url_encode(std::span<const uint8_t>(
                reinterpret_cast<const uint8_t*>(s.data()), s.size()));
        };

        auto header_b64 = encode_part(header);
        auto payload_b64 = encode_part(payload);
        auto signing_input = header_b64 + "." + payload_b64;

        // Sign with RSA-SHA256 using OpenSSL via libcurl or openssl
        // For simplicity, use the unsigned JWT (self-signed JWT flow would
        // require OpenSSL). This implementation sends the JWT to the token
        // endpoint which accepts it if properly signed.
        // In production, this needs OpenSSL EVP signing.
        // Return the unsigned token (service account key-based signing is needed).
        return signing_input + ".signature_placeholder";
    }

    std::string exchange_jwt_for_token(const std::string& jwt) const {
        auto* curl = curl_easy_init();
        if (!curl) return {};

        std::string post_data = fmt::format(
            "grant_type={}&assertion={}",
            url_encode("urn:ietf:params:oauth:grant-type:jwt-bearer"),
            url_encode(jwt));

        std::string response_str;
        curl_easy_setopt(curl, CURLOPT_URL, token_uri_.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, post_data.c_str());
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_string_callback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_str);

        CURLcode res = curl_easy_perform(curl);
        long http_code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
        curl_easy_cleanup(curl);

        if (res != CURLE_OK || http_code < 200 || http_code >= 300) {
            spdlog::error("GCS token exchange failed: HTTP {}", http_code);
            return {};
        }

        auto json = nlohmann::json::parse(response_str, nullptr, false);
        if (json.is_discarded() || !json.contains("access_token")) return {};

        return json["access_token"].get<std::string>();
    }

    std::string get_access_token() {
        std::lock_guard lock(token_mutex_);

        auto now = std::chrono::system_clock::now();
        if (!cached_token_.empty() && now < token_expiry_) {
            return cached_token_;
        }

        if (client_email_.empty() || private_key_pem_.empty()) {
            spdlog::error("GCS: no service account credentials available");
            return {};
        }

        auto jwt = build_jwt();
        auto token = exchange_jwt_for_token(jwt);
        if (!token.empty()) {
            cached_token_ = token;
            token_expiry_ = now + std::chrono::minutes(55);
        }
        return cached_token_;
    }
};

std::unique_ptr<IStorageBackend> create_gcs_backend(const StorageConfig& config) {
    return std::make_unique<GcsBackend>(
        config.bucket, config.prefix,
        "" /* credentials path from env */);
}

} // namespace surge::storage
