/**
 * @file s3_backend.cpp
 * @brief S3 storage backend with AWS Signature V4 authentication.
 */

#include "storage/storage_backend.hpp"
#include "crypto/hmac.hpp"
#include "crypto/sha256.hpp"
#include <curl/curl.h>
#include <spdlog/spdlog.h>
#include <fmt/format.h>
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <map>
#include <sstream>
#include <string_view>

namespace surge::storage {

namespace {

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

std::string to_hex(std::span<const uint8_t> data) {
    std::string result;
    result.reserve(data.size() * 2);
    for (auto b : data) {
        result += fmt::format("{:02x}", b);
    }
    return result;
}

std::string utc_date() {
    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
    gmtime_r(&time, &tm);
    char buf[16];
    std::strftime(buf, sizeof(buf), "%Y%m%d", &tm);
    return buf;
}

std::string utc_datetime() {
    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
    gmtime_r(&time, &tm);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y%m%dT%H%M%SZ", &tm);
    return buf;
}

size_t write_callback(char* ptr, size_t size, size_t nmemb, void* userdata) {
    auto* buffer = static_cast<std::vector<uint8_t>*>(userdata);
    size_t total = size * nmemb;
    buffer->insert(buffer->end(), ptr, ptr + total);
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
        // Trim whitespace
        while (!value.empty() && (value.front() == ' ' || value.front() == '\t'))
            value.erase(value.begin());
        while (!value.empty() && (value.back() == '\r' || value.back() == '\n'))
            value.pop_back();
        // Lowercase key for consistent lookup
        std::transform(key.begin(), key.end(), key.begin(), ::tolower);
        (*headers)[key] = value;
    }
    return total;
}

struct CurlProgressData {
    std::function<void(int64_t, int64_t)> callback;
    bool is_upload;
};

int xfer_callback(void* clientp, curl_off_t dltotal, curl_off_t dlnow,
                  curl_off_t ultotal, curl_off_t ulnow) {
    auto* data = static_cast<CurlProgressData*>(clientp);
    if (data && data->callback) {
        if (data->is_upload) {
            data->callback(static_cast<int64_t>(ulnow), static_cast<int64_t>(ultotal));
        } else {
            data->callback(static_cast<int64_t>(dlnow), static_cast<int64_t>(dltotal));
        }
    }
    return 0;
}

} // anonymous namespace

class S3Backend : public IStorageBackend {
public:
    S3Backend(std::string bucket, std::string region,
              std::string access_key, std::string secret_key,
              std::string endpoint, std::string prefix)
        : bucket_(std::move(bucket))
        , region_(std::move(region))
        , access_key_(std::move(access_key))
        , secret_key_(std::move(secret_key))
        , endpoint_(std::move(endpoint))
        , prefix_(std::move(prefix))
    {
        // Fall back to environment variables for credentials
        if (access_key_.empty()) {
            if (const char* env = std::getenv("SURGE_ACCESS_KEY"))
                access_key_ = env;
            else if (const char* env = std::getenv("AWS_ACCESS_KEY_ID"))
                access_key_ = env;
        }
        if (secret_key_.empty()) {
            if (const char* env = std::getenv("SURGE_SECRET_KEY"))
                secret_key_ = env;
            else if (const char* env = std::getenv("AWS_SECRET_ACCESS_KEY"))
                secret_key_ = env;
        }
        if (region_.empty()) {
            if (const char* env = std::getenv("AWS_DEFAULT_REGION"))
                region_ = env;
            else
                region_ = "us-east-1";
        }

        if (endpoint_.empty()) {
            endpoint_ = fmt::format("https://{}.s3.{}.amazonaws.com", bucket_, region_);
            path_style_ = false;
        } else {
            // MinIO / custom endpoint uses path-style
            path_style_ = true;
        }
        spdlog::debug("S3Backend: endpoint={}, bucket={}, region={}, path_style={}",
                       endpoint_, bucket_, region_, path_style_);
    }

    int32_t put_object(const std::string& key, std::span<const uint8_t> data,
                       const std::string& content_type) override {
        auto full_key = prefixed_key(key);
        auto url = build_url(full_key);
        auto payload_hash = crypto::sha256_hex(data);
        auto datetime = utc_datetime();
        auto date = datetime.substr(0, 8);

        std::map<std::string, std::string> headers;
        headers["host"] = host_for_signing();
        headers["x-amz-content-sha256"] = payload_hash;
        headers["x-amz-date"] = datetime;
        headers["content-type"] = content_type;
        headers["content-length"] = std::to_string(data.size());

        auto auth = build_authorization("PUT", "/" + uri_path(full_key), "", headers, payload_hash, date, datetime);
        headers["authorization"] = auth;

        auto* curl = curl_easy_init();
        if (!curl) return SURGE_ERROR;

        struct curl_slist* hdr_list = nullptr;
        for (auto& [k, v] : headers) {
            if (k == "host") continue;
            hdr_list = curl_slist_append(hdr_list, fmt::format("{}: {}", k, v).c_str());
        }

        std::vector<uint8_t> response;
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
        curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, static_cast<curl_off_t>(data.size()));
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdr_list);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);

        // Use a read callback over a memory span
        struct ReadState { const uint8_t* ptr; size_t remaining; };
        ReadState read_state{data.data(), data.size()};

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

        if (res != CURLE_OK) {
            spdlog::error("S3 PUT failed: {}", curl_easy_strerror(res));
            return SURGE_ERROR;
        }
        if (http_code < 200 || http_code >= 300) {
            spdlog::error("S3 PUT HTTP {}: {}", http_code, std::string(response.begin(), response.end()));
            return SURGE_ERROR;
        }
        return SURGE_OK;
    }

    int32_t get_object(const std::string& key, std::vector<uint8_t>& out_data) override {
        auto full_key = prefixed_key(key);
        auto url = build_url(full_key);
        auto payload_hash = crypto::sha256_hex(std::span<const uint8_t>{});
        auto datetime = utc_datetime();
        auto date = datetime.substr(0, 8);

        std::map<std::string, std::string> headers;
        headers["host"] = host_for_signing();
        headers["x-amz-content-sha256"] = payload_hash;
        headers["x-amz-date"] = datetime;

        auto auth = build_authorization("GET", "/" + uri_path(full_key), "", headers, payload_hash, date, datetime);
        headers["authorization"] = auth;

        auto* curl = curl_easy_init();
        if (!curl) return SURGE_ERROR;

        struct curl_slist* hdr_list = nullptr;
        for (auto& [k, v] : headers) {
            if (k == "host") continue;
            hdr_list = curl_slist_append(hdr_list, fmt::format("{}: {}", k, v).c_str());
        }

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

        if (res != CURLE_OK) {
            spdlog::error("S3 GET failed: {}", curl_easy_strerror(res));
            return SURGE_ERROR;
        }
        if (http_code == 404) return SURGE_NOT_FOUND;
        if (http_code < 200 || http_code >= 300) {
            spdlog::error("S3 GET HTTP {}", http_code);
            return SURGE_ERROR;
        }
        return SURGE_OK;
    }

    int32_t head_object(const std::string& key, ObjectInfo& out_info) override {
        auto full_key = prefixed_key(key);
        auto url = build_url(full_key);
        auto payload_hash = crypto::sha256_hex(std::span<const uint8_t>{});
        auto datetime = utc_datetime();
        auto date = datetime.substr(0, 8);

        std::map<std::string, std::string> sign_headers;
        sign_headers["host"] = host_for_signing();
        sign_headers["x-amz-content-sha256"] = payload_hash;
        sign_headers["x-amz-date"] = datetime;

        auto auth = build_authorization("HEAD", "/" + uri_path(full_key), "", sign_headers, payload_hash, date, datetime);
        sign_headers["authorization"] = auth;

        auto* curl = curl_easy_init();
        if (!curl) return SURGE_ERROR;

        struct curl_slist* hdr_list = nullptr;
        for (auto& [k, v] : sign_headers) {
            if (k == "host") continue;
            hdr_list = curl_slist_append(hdr_list, fmt::format("{}: {}", k, v).c_str());
        }

        std::map<std::string, std::string> resp_headers;
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdr_list);
        curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, header_callback);
        curl_easy_setopt(curl, CURLOPT_HEADERDATA, &resp_headers);

        CURLcode res = curl_easy_perform(curl);
        long http_code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
        curl_slist_free_all(hdr_list);
        curl_easy_cleanup(curl);

        if (res != CURLE_OK) return SURGE_ERROR;
        if (http_code == 404) return SURGE_NOT_FOUND;
        if (http_code < 200 || http_code >= 300) return SURGE_ERROR;

        out_info.key = key;
        if (auto it = resp_headers.find("content-length"); it != resp_headers.end())
            out_info.size = std::stoll(it->second);
        if (auto it = resp_headers.find("etag"); it != resp_headers.end())
            out_info.etag = it->second;
        if (auto it = resp_headers.find("last-modified"); it != resp_headers.end())
            out_info.last_modified = it->second;

        return SURGE_OK;
    }

    int32_t delete_object(const std::string& key) override {
        auto full_key = prefixed_key(key);
        auto url = build_url(full_key);
        auto payload_hash = crypto::sha256_hex(std::span<const uint8_t>{});
        auto datetime = utc_datetime();
        auto date = datetime.substr(0, 8);

        std::map<std::string, std::string> headers;
        headers["host"] = host_for_signing();
        headers["x-amz-content-sha256"] = payload_hash;
        headers["x-amz-date"] = datetime;

        auto auth = build_authorization("DELETE", "/" + uri_path(full_key), "", headers, payload_hash, date, datetime);
        headers["authorization"] = auth;

        auto* curl = curl_easy_init();
        if (!curl) return SURGE_ERROR;

        struct curl_slist* hdr_list = nullptr;
        for (auto& [k, v] : headers) {
            if (k == "host") continue;
            hdr_list = curl_slist_append(hdr_list, fmt::format("{}: {}", k, v).c_str());
        }

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
        auto full_prefix = prefixed_key(prefix);
        std::string query = fmt::format("list-type=2&prefix={}&max-keys={}", url_encode(full_prefix), max_keys);
        if (!marker.empty()) {
            query += fmt::format("&continuation-token={}", url_encode(marker));
        }

        std::string uri_p = path_style_ ? fmt::format("/{}", bucket_) : "/";
        auto url = build_url("") + "?" + query;

        auto payload_hash = crypto::sha256_hex(std::span<const uint8_t>{});
        auto datetime = utc_datetime();
        auto date = datetime.substr(0, 8);

        std::map<std::string, std::string> headers;
        headers["host"] = host_for_signing();
        headers["x-amz-content-sha256"] = payload_hash;
        headers["x-amz-date"] = datetime;

        auto auth = build_authorization("GET", uri_p, query, headers, payload_hash, date, datetime);
        headers["authorization"] = auth;

        auto* curl = curl_easy_init();
        if (!curl) return SURGE_ERROR;

        struct curl_slist* hdr_list = nullptr;
        for (auto& [k, v] : headers) {
            if (k == "host") continue;
            hdr_list = curl_slist_append(hdr_list, fmt::format("{}: {}", k, v).c_str());
        }

        std::vector<uint8_t> response;
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdr_list);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);

        CURLcode res = curl_easy_perform(curl);
        long http_code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
        curl_slist_free_all(hdr_list);
        curl_easy_cleanup(curl);

        if (res != CURLE_OK || http_code < 200 || http_code >= 300) return SURGE_ERROR;

        // Simple XML parsing for ListBucketResult
        std::string xml(response.begin(), response.end());
        out_result.objects.clear();
        out_result.truncated = false;
        out_result.next_marker.clear();

        // Parse IsTruncated
        if (auto pos = xml.find("<IsTruncated>true</IsTruncated>"); pos != std::string::npos) {
            out_result.truncated = true;
        }

        // Parse NextContinuationToken
        if (auto start = xml.find("<NextContinuationToken>"); start != std::string::npos) {
            start += std::strlen("<NextContinuationToken>");
            auto end = xml.find("</NextContinuationToken>", start);
            if (end != std::string::npos) {
                out_result.next_marker = xml.substr(start, end - start);
            }
        }

        // Parse Contents entries
        size_t search_from = 0;
        while (true) {
            auto contents_start = xml.find("<Contents>", search_from);
            if (contents_start == std::string::npos) break;
            auto contents_end = xml.find("</Contents>", contents_start);
            if (contents_end == std::string::npos) break;
            search_from = contents_end + std::strlen("</Contents>");

            auto block = xml.substr(contents_start, contents_end - contents_start);
            ObjectInfo info;

            auto extract = [&](const std::string& tag) -> std::string {
                auto open = fmt::format("<{}>", tag);
                auto close = fmt::format("</{}>", tag);
                auto s = block.find(open);
                if (s == std::string::npos) return {};
                s += open.size();
                auto e = block.find(close, s);
                if (e == std::string::npos) return {};
                return block.substr(s, e - s);
            };

            info.key = extract("Key");
            // Strip the prefix so callers see the relative key
            if (!prefix_.empty() && info.key.starts_with(prefix_)) {
                info.key = info.key.substr(prefix_.size());
                if (!info.key.empty() && info.key.front() == '/') {
                    info.key = info.key.substr(1);
                }
            }
            auto size_str = extract("Size");
            if (!size_str.empty()) info.size = std::stoll(size_str);
            info.etag = extract("ETag");
            info.last_modified = extract("LastModified");

            out_result.objects.push_back(std::move(info));
        }

        return SURGE_OK;
    }

    int32_t download_to_file(const std::string& key, const std::filesystem::path& dest,
                              std::function<void(int64_t, int64_t)> progress) override {
        auto full_key = prefixed_key(key);
        auto url = build_url(full_key);
        auto payload_hash = crypto::sha256_hex(std::span<const uint8_t>{});
        auto datetime = utc_datetime();
        auto date = datetime.substr(0, 8);

        std::map<std::string, std::string> headers;
        headers["host"] = host_for_signing();
        headers["x-amz-content-sha256"] = payload_hash;
        headers["x-amz-date"] = datetime;

        auto auth = build_authorization("GET", "/" + uri_path(full_key), "", headers, payload_hash, date, datetime);
        headers["authorization"] = auth;

        auto* curl = curl_easy_init();
        if (!curl) return SURGE_ERROR;

        struct curl_slist* hdr_list = nullptr;
        for (auto& [k, v] : headers) {
            if (k == "host") continue;
            hdr_list = curl_slist_append(hdr_list, fmt::format("{}: {}", k, v).c_str());
        }

        std::filesystem::create_directories(dest.parent_path());
        std::ofstream file(dest, std::ios::binary);
        if (!file) {
            curl_slist_free_all(hdr_list);
            curl_easy_cleanup(curl);
            return SURGE_ERROR;
        }

        CurlProgressData prog_data{progress, false};
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdr_list);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_file_callback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &file);
        if (progress) {
            curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0L);
            curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, xfer_callback);
            curl_easy_setopt(curl, CURLOPT_XFERINFODATA, &prog_data);
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
        auto full_key = prefixed_key(key);
        auto url = build_url(full_key);
        auto file_size = std::filesystem::file_size(src);

        // Hash the file for payload signing
        auto payload_hash = crypto::sha256_hex_file(src);
        auto datetime = utc_datetime();
        auto date = datetime.substr(0, 8);

        std::map<std::string, std::string> headers;
        headers["host"] = host_for_signing();
        headers["x-amz-content-sha256"] = payload_hash;
        headers["x-amz-date"] = datetime;
        headers["content-type"] = "application/octet-stream";
        headers["content-length"] = std::to_string(file_size);

        auto auth = build_authorization("PUT", "/" + uri_path(full_key), "", headers, payload_hash, date, datetime);
        headers["authorization"] = auth;

        auto* curl = curl_easy_init();
        if (!curl) return SURGE_ERROR;

        struct curl_slist* hdr_list = nullptr;
        for (auto& [k, v] : headers) {
            if (k == "host") continue;
            hdr_list = curl_slist_append(hdr_list, fmt::format("{}: {}", k, v).c_str());
        }

        std::ifstream file(src, std::ios::binary);
        if (!file) {
            curl_slist_free_all(hdr_list);
            curl_easy_cleanup(curl);
            return SURGE_ERROR;
        }

        CurlProgressData prog_data{progress, true};
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
        curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, static_cast<curl_off_t>(file_size));
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdr_list);
        curl_easy_setopt(curl, CURLOPT_READFUNCTION, read_callback);
        curl_easy_setopt(curl, CURLOPT_READDATA, &file);
        if (progress) {
            curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0L);
            curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, xfer_callback);
            curl_easy_setopt(curl, CURLOPT_XFERINFODATA, &prog_data);
        }

        CURLcode res = curl_easy_perform(curl);
        long http_code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
        curl_slist_free_all(hdr_list);
        curl_easy_cleanup(curl);

        if (res != CURLE_OK || http_code < 200 || http_code >= 300) {
            spdlog::error("S3 upload HTTP {}: {}", http_code, curl_easy_strerror(res));
            return SURGE_ERROR;
        }
        return SURGE_OK;
    }

private:
    std::string bucket_;
    std::string region_;
    std::string access_key_;
    std::string secret_key_;
    std::string endpoint_;
    std::string prefix_;
    bool path_style_ = false;

    std::string prefixed_key(const std::string& key) const {
        if (prefix_.empty()) return key;
        if (prefix_.back() == '/') return prefix_ + key;
        return prefix_ + "/" + key;
    }

    std::string host_for_signing() const {
        // Extract host from endpoint URL
        std::string_view ep = endpoint_;
        if (ep.starts_with("https://")) ep.remove_prefix(8);
        else if (ep.starts_with("http://")) ep.remove_prefix(7);
        auto slash = ep.find('/');
        if (slash != std::string_view::npos) ep = ep.substr(0, slash);
        return std::string(ep);
    }

    std::string uri_path(const std::string& key) const {
        if (path_style_) {
            return fmt::format("{}/{}", bucket_, key);
        }
        return key;
    }

    std::string build_url(const std::string& key) const {
        if (path_style_) {
            if (key.empty()) return fmt::format("{}/{}", endpoint_, bucket_);
            return fmt::format("{}/{}/{}", endpoint_, bucket_, key);
        }
        if (key.empty()) return endpoint_;
        return fmt::format("{}/{}", endpoint_, key);
    }

    std::string build_canonical_request(
        const std::string& method,
        const std::string& uri,
        const std::string& query_string,
        const std::map<std::string, std::string>& headers,
        const std::string& payload_hash) const
    {
        // Canonical URI
        std::string canonical_uri = uri.empty() ? "/" : uri;

        // Canonical query string (already sorted if single param set)
        // Split, sort, and rejoin
        std::map<std::string, std::string> sorted_params;
        if (!query_string.empty()) {
            std::istringstream qs(query_string);
            std::string pair;
            while (std::getline(qs, pair, '&')) {
                auto eq = pair.find('=');
                if (eq != std::string::npos) {
                    sorted_params[pair.substr(0, eq)] = pair.substr(eq + 1);
                } else {
                    sorted_params[pair] = "";
                }
            }
        }
        std::string canonical_qs;
        for (auto& [k, v] : sorted_params) {
            if (!canonical_qs.empty()) canonical_qs += '&';
            canonical_qs += k + "=" + v;
        }

        // Canonical headers (sorted by lowercase key)
        std::string canonical_headers;
        std::string signed_headers;
        for (auto& [k, v] : headers) {
            canonical_headers += fmt::format("{}:{}\n", k, v);
            if (!signed_headers.empty()) signed_headers += ';';
            signed_headers += k;
        }

        return fmt::format("{}\n{}\n{}\n{}\n{}\n{}",
                           method, canonical_uri, canonical_qs,
                           canonical_headers, signed_headers, payload_hash);
    }

    std::vector<uint8_t> derive_signing_key(const std::string& date) const {
        auto key_bytes = [](const std::string& s) -> std::vector<uint8_t> {
            return {s.begin(), s.end()};
        };

        auto k_secret = key_bytes("AWS4" + secret_key_);
        auto k_date = crypto::hmac_sha256(k_secret, std::vector<uint8_t>(date.begin(), date.end()));
        auto k_region = crypto::hmac_sha256(k_date, std::vector<uint8_t>(region_.begin(), region_.end()));
        std::string service = "s3";
        auto k_service = crypto::hmac_sha256(k_region, std::vector<uint8_t>(service.begin(), service.end()));
        std::string request = "aws4_request";
        return crypto::hmac_sha256(k_service, std::vector<uint8_t>(request.begin(), request.end()));
    }

    std::string build_authorization(
        const std::string& method,
        const std::string& uri,
        const std::string& query_string,
        const std::map<std::string, std::string>& headers,
        const std::string& payload_hash,
        const std::string& date,
        const std::string& datetime) const
    {
        auto canonical_request = build_canonical_request(method, uri, query_string, headers, payload_hash);
        auto canonical_hash = crypto::sha256_hex(
            std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(canonical_request.data()),
                                     canonical_request.size()));

        std::string credential_scope = fmt::format("{}/{}/s3/aws4_request", date, region_);
        std::string string_to_sign = fmt::format("AWS4-HMAC-SHA256\n{}\n{}\n{}", datetime, credential_scope, canonical_hash);

        auto signing_key = derive_signing_key(date);
        auto signature = crypto::hmac_sha256_hex(signing_key,
            std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(string_to_sign.data()),
                                     string_to_sign.size()));

        // Build signed headers list
        std::string signed_headers;
        for (auto& [k, v] : headers) {
            if (!signed_headers.empty()) signed_headers += ';';
            signed_headers += k;
        }

        return fmt::format("AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
                           access_key_, credential_scope, signed_headers, signature);
    }
};

// Factory registration helper - used by create_storage_backend
std::unique_ptr<IStorageBackend> create_s3_backend(const StorageConfig& config) {
    return std::make_unique<S3Backend>(
        config.bucket, config.region,
        config.access_key, config.secret_key,
        config.endpoint, config.prefix);
}

} // namespace surge::storage
