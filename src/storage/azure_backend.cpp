/**
 * @file azure_backend.cpp
 * @brief Azure Blob Storage backend with SharedKey authentication.
 */

#include "storage/azure_backend.hpp"

#include "core/context.hpp"
#include "crypto/hmac.hpp"
#include "crypto/sha256.hpp"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <curl/curl.h>
#include <fmt/format.h>
#include <fstream>
#include <map>
#include <spdlog/spdlog.h>
#include <sstream>

namespace surge::storage {

namespace {

constexpr const char* AZURE_API_VERSION = "2020-10-02";

std::string rfc1123_date() {
    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
    gmtime_r(&time, &tm);
    char buf[64];
    std::strftime(buf, sizeof(buf), "%a, %d %b %Y %H:%M:%S GMT", &tm);
    return buf;
}

std::string base64_encode(std::span<const uint8_t> data) {
    static constexpr char table[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string result;
    result.reserve(((data.size() + 2) / 3) * 4);
    for (size_t i = 0; i < data.size(); i += 3) {
        uint32_t n = static_cast<uint32_t>(data[i]) << 16;
        if (i + 1 < data.size())
            n |= static_cast<uint32_t>(data[i + 1]) << 8;
        if (i + 2 < data.size())
            n |= static_cast<uint32_t>(data[i + 2]);
        result += table[(n >> 18) & 0x3F];
        result += table[(n >> 12) & 0x3F];
        result += (i + 1 < data.size()) ? table[(n >> 6) & 0x3F] : '=';
        result += (i + 2 < data.size()) ? table[n & 0x3F] : '=';
    }
    return result;
}

std::vector<uint8_t> base64_decode(std::string_view input) {
    static constexpr uint8_t decode_table[128] = {
        64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 62, 64, 64, 64, 63, 52, 53, 54, 55,
        56, 57, 58, 59, 60, 61, 64, 64, 64, 64, 64, 64, 64, 0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
        13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 64, 64, 64, 64, 64, 64, 26, 27, 28, 29, 30, 31, 32,
        33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 64, 64, 64, 64, 64};
    std::vector<uint8_t> result;
    result.reserve((input.size() / 4) * 3);
    uint32_t buf = 0;
    int bits = 0;
    for (char c : input) {
        if (c == '=' || c == '\n' || c == '\r')
            continue;
        if (static_cast<unsigned char>(c) >= 128)
            continue;
        buf = (buf << 6) | decode_table[static_cast<unsigned char>(c)];
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            result.push_back(static_cast<uint8_t>((buf >> bits) & 0xFF));
        }
    }
    return result;
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
        while (!value.empty() && (value.front() == ' ' || value.front() == '\t'))
            value.erase(value.begin());
        while (!value.empty() && (value.back() == '\r' || value.back() == '\n'))
            value.pop_back();
        std::transform(key.begin(), key.end(), key.begin(), ::tolower);
        (*headers)[key] = value;
    }
    return total;
}

int xfer_callback(void* clientp, curl_off_t dltotal, curl_off_t dlnow, curl_off_t ultotal, curl_off_t ulnow) {
    auto* cb = static_cast<std::function<void(int64_t, int64_t)>*>(clientp);
    if (cb && *cb)
        (*cb)(static_cast<int64_t>(dlnow + ulnow), static_cast<int64_t>(dltotal + ultotal));
    return 0;
}

}  // anonymous namespace

// ----- AzureSharedKeySigner -----

AzureSharedKeySigner::AzureSharedKeySigner(std::string_view account_name, std::string_view account_key)
    : account_name_(account_name), account_key_decoded_(base64_decode(account_key)) {}

std::string AzureSharedKeySigner::sign_request(
    std::string_view /*method*/, std::string_view /*resource_path*/,
    const std::vector<std::pair<std::string, std::string>>& /*headers*/,
    const std::vector<std::pair<std::string, std::string>>& /*query_params*/) const {
    // Placeholder - signing logic is inline in the backend
    return {};
}

std::string AzureSharedKeySigner::rfc1123_now() {
    return rfc1123_date();
}

// ----- AzureBlobBackend -----

struct AzureBlobBackend::Impl {
    std::string account;
    std::string account_key;
    std::string container;
    std::string prefix;
    std::string base_url;

    std::string build_canonical_headers(const std::map<std::string, std::string>& headers) const {
        std::string result;
        for (auto& [k, v] : headers) {
            if (k.starts_with("x-ms-"))
                result += fmt::format("{}:{}\n", k, v);
        }
        return result;
    }

    std::string build_shared_key_auth(const std::string& method, const std::string& blob_name,
                                      const std::map<std::string, std::string>& headers, int64_t content_length) const {
        auto get_header = [&](const std::string& name) -> std::string {
            auto it = headers.find(name);
            return (it != headers.end()) ? it->second : "";
        };
        std::string content_len_str = (content_length > 0) ? std::to_string(content_length) : "";
        std::string string_to_sign =
            fmt::format("{}\n\n\n{}\n\n{}\n\n\n\n\n\n\n{}/{}/{}/{}", method, content_len_str,
                        get_header("content-type"), build_canonical_headers(headers), account, container, blob_name);
        auto signature = crypto::hmac_sha256(
            account_key_decoded_,
            std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(string_to_sign.data()), string_to_sign.size()));
        return fmt::format("SharedKey {}:{}", account, base64_encode(signature));
    }

    std::vector<uint8_t> account_key_decoded_;
};

AzureBlobBackend::AzureBlobBackend(const StorageConfig& config) : impl_(std::make_unique<Impl>()) {
    impl_->account = config.access_key;
    impl_->account_key = config.secret_key;
    impl_->container = config.bucket;
    impl_->prefix = config.prefix;
    impl_->account_key_decoded_ = base64_decode(config.secret_key);

    if (impl_->account.empty()) {
        if (const char* env = std::getenv("AZURE_STORAGE_ACCOUNT"))
            impl_->account = env;
    }
    if (impl_->account_key.empty()) {
        if (const char* env = std::getenv("AZURE_STORAGE_KEY")) {
            impl_->account_key = env;
            impl_->account_key_decoded_ = base64_decode(env);
        }
    }

    if (!config.endpoint.empty()) {
        impl_->base_url = config.endpoint;
    } else {
        impl_->base_url = fmt::format("https://{}.blob.core.windows.net/{}", impl_->account, impl_->container);
    }
    spdlog::debug("AzureBlobBackend: account={}, container={}", impl_->account, impl_->container);
}

AzureBlobBackend::~AzureBlobBackend() = default;

std::string AzureBlobBackend::build_url(const std::string& key) const {
    return fmt::format("{}/{}", impl_->base_url, key);
}

std::string AzureBlobBackend::prefixed_key(const std::string& key) const {
    if (impl_->prefix.empty())
        return key;
    if (impl_->prefix.back() == '/')
        return impl_->prefix + key;
    return impl_->prefix + "/" + key;
}

int32_t AzureBlobBackend::put_object(const std::string& key, std::span<const uint8_t> data,
                                     const std::string& content_type) {
    auto full_key = prefixed_key(key);
    auto url = build_url(full_key);
    auto date = rfc1123_date();

    std::map<std::string, std::string> headers;
    headers["x-ms-version"] = AZURE_API_VERSION;
    headers["x-ms-date"] = date;
    headers["x-ms-blob-type"] = "BlockBlob";
    headers["content-type"] = content_type;
    headers["content-length"] = std::to_string(data.size());

    auto auth = impl_->build_shared_key_auth("PUT", full_key, headers, static_cast<int64_t>(data.size()));
    headers["authorization"] = auth;

    auto* curl = curl_easy_init();
    if (!curl)
        return SURGE_ERROR;

    struct curl_slist* hdr_list = nullptr;
    for (auto& [k, v] : headers) {
        hdr_list = curl_slist_append(hdr_list, fmt::format("{}: {}", k, v).c_str());
    }

    struct ReadState {
        const uint8_t* ptr;
        size_t remaining;
    };
    ReadState read_state{data.data(), data.size()};
    std::vector<uint8_t> response;

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
    curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, static_cast<curl_off_t>(data.size()));
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdr_list);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(
        curl, CURLOPT_READFUNCTION, +[](char* ptr, size_t size, size_t nmemb, void* ud) -> size_t {
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

    if (res != CURLE_OK || http_code < 200 || http_code >= 300)
        return SURGE_ERROR;
    return SURGE_OK;
}

int32_t AzureBlobBackend::get_object(const std::string& key, std::vector<uint8_t>& out_data) {
    auto full_key = prefixed_key(key);
    auto url = build_url(full_key);
    auto date = rfc1123_date();

    std::map<std::string, std::string> headers;
    headers["x-ms-version"] = AZURE_API_VERSION;
    headers["x-ms-date"] = date;
    auto auth = impl_->build_shared_key_auth("GET", full_key, headers, 0);
    headers["authorization"] = auth;

    auto* curl = curl_easy_init();
    if (!curl)
        return SURGE_ERROR;

    struct curl_slist* hdr_list = nullptr;
    for (auto& [k, v] : headers)
        hdr_list = curl_slist_append(hdr_list, fmt::format("{}: {}", k, v).c_str());

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

    if (res != CURLE_OK)
        return SURGE_ERROR;
    if (http_code == 404)
        return SURGE_NOT_FOUND;
    if (http_code < 200 || http_code >= 300)
        return SURGE_ERROR;
    return SURGE_OK;
}

int32_t AzureBlobBackend::head_object(const std::string& key, ObjectInfo& out_info) {
    auto full_key = prefixed_key(key);
    auto url = build_url(full_key);
    auto date = rfc1123_date();

    std::map<std::string, std::string> headers;
    headers["x-ms-version"] = AZURE_API_VERSION;
    headers["x-ms-date"] = date;
    auto auth = impl_->build_shared_key_auth("HEAD", full_key, headers, 0);
    headers["authorization"] = auth;

    auto* curl = curl_easy_init();
    if (!curl)
        return SURGE_ERROR;

    struct curl_slist* hdr_list = nullptr;
    for (auto& [k, v] : headers)
        hdr_list = curl_slist_append(hdr_list, fmt::format("{}: {}", k, v).c_str());

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

    if (res != CURLE_OK)
        return SURGE_ERROR;
    if (http_code == 404)
        return SURGE_NOT_FOUND;
    if (http_code < 200 || http_code >= 300)
        return SURGE_ERROR;

    out_info.key = key;
    if (auto it = resp_headers.find("content-length"); it != resp_headers.end())
        out_info.size = std::stoll(it->second);
    if (auto it = resp_headers.find("etag"); it != resp_headers.end())
        out_info.etag = it->second;
    if (auto it = resp_headers.find("last-modified"); it != resp_headers.end())
        out_info.last_modified = it->second;
    return SURGE_OK;
}

int32_t AzureBlobBackend::delete_object(const std::string& key) {
    auto full_key = prefixed_key(key);
    auto url = build_url(full_key);
    auto date = rfc1123_date();

    std::map<std::string, std::string> headers;
    headers["x-ms-version"] = AZURE_API_VERSION;
    headers["x-ms-date"] = date;
    auto auth = impl_->build_shared_key_auth("DELETE", full_key, headers, 0);
    headers["authorization"] = auth;

    auto* curl = curl_easy_init();
    if (!curl)
        return SURGE_ERROR;

    struct curl_slist* hdr_list = nullptr;
    for (auto& [k, v] : headers)
        hdr_list = curl_slist_append(hdr_list, fmt::format("{}: {}", k, v).c_str());

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdr_list);

    CURLcode res = curl_easy_perform(curl);
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    curl_slist_free_all(hdr_list);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK)
        return SURGE_ERROR;
    if (http_code >= 200 && http_code < 300)
        return SURGE_OK;
    if (http_code == 404)
        return SURGE_OK;
    return SURGE_ERROR;
}

int32_t AzureBlobBackend::list_objects(const std::string& prefix, ListResult& out_result, const std::string& marker,
                                       int max_keys) {
    out_result.objects.clear();
    out_result.truncated = false;
    out_result.next_marker.clear();
    // Simplified - would use Azure List Blobs API
    return SURGE_OK;
}

int32_t AzureBlobBackend::download_to_file(const std::string& key, const std::filesystem::path& dest,
                                           std::function<void(int64_t, int64_t)> progress) {
    auto full_key = prefixed_key(key);
    auto url = build_url(full_key);
    auto date = rfc1123_date();

    std::map<std::string, std::string> headers;
    headers["x-ms-version"] = AZURE_API_VERSION;
    headers["x-ms-date"] = date;
    auto auth = impl_->build_shared_key_auth("GET", full_key, headers, 0);
    headers["authorization"] = auth;

    auto* curl = curl_easy_init();
    if (!curl)
        return SURGE_ERROR;

    struct curl_slist* hdr_list = nullptr;
    for (auto& [k, v] : headers)
        hdr_list = curl_slist_append(hdr_list, fmt::format("{}: {}", k, v).c_str());

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

int32_t AzureBlobBackend::upload_from_file(const std::string& key, const std::filesystem::path& src,
                                           std::function<void(int64_t, int64_t)> progress) {
    auto full_key = prefixed_key(key);
    auto url = build_url(full_key);
    auto file_size = std::filesystem::file_size(src);
    auto date = rfc1123_date();

    std::map<std::string, std::string> headers;
    headers["x-ms-version"] = AZURE_API_VERSION;
    headers["x-ms-date"] = date;
    headers["x-ms-blob-type"] = "BlockBlob";
    headers["content-type"] = "application/octet-stream";
    headers["content-length"] = std::to_string(file_size);
    auto auth = impl_->build_shared_key_auth("PUT", full_key, headers, static_cast<int64_t>(file_size));
    headers["authorization"] = auth;

    auto* curl = curl_easy_init();
    if (!curl)
        return SURGE_ERROR;

    struct curl_slist* hdr_list = nullptr;
    for (auto& [k, v] : headers)
        hdr_list = curl_slist_append(hdr_list, fmt::format("{}: {}", k, v).c_str());

    std::ifstream file(src, std::ios::binary);
    if (!file) {
        curl_slist_free_all(hdr_list);
        curl_easy_cleanup(curl);
        return SURGE_ERROR;
    }

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
    curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, static_cast<curl_off_t>(file_size));
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdr_list);
    curl_easy_setopt(curl, CURLOPT_READFUNCTION, read_callback);
    curl_easy_setopt(curl, CURLOPT_READDATA, &file);

    CURLcode res = curl_easy_perform(curl);
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    curl_slist_free_all(hdr_list);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK || http_code < 200 || http_code >= 300)
        return SURGE_ERROR;
    return SURGE_OK;
}

}  // namespace surge::storage
