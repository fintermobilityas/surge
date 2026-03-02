/**
 * @file azure_backend.cpp
 * @brief Azure Blob Storage backend with SharedKey authentication.
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
#include <ctime>
#include <fstream>
#include <map>
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
    static constexpr char table[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string result;
    result.reserve(((data.size() + 2) / 3) * 4);

    for (size_t i = 0; i < data.size(); i += 3) {
        uint32_t n = static_cast<uint32_t>(data[i]) << 16;
        if (i + 1 < data.size()) n |= static_cast<uint32_t>(data[i + 1]) << 8;
        if (i + 2 < data.size()) n |= static_cast<uint32_t>(data[i + 2]);

        result += table[(n >> 18) & 0x3F];
        result += table[(n >> 12) & 0x3F];
        result += (i + 1 < data.size()) ? table[(n >> 6) & 0x3F] : '=';
        result += (i + 2 < data.size()) ? table[n & 0x3F] : '=';
    }
    return result;
}

std::vector<uint8_t> base64_decode(std::string_view input) {
    static constexpr uint8_t decode_table[128] = {
        64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,
        64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,64,
        64,64,64,64,64,64,64,64,64,64,64,62,64,64,64,63,
        52,53,54,55,56,57,58,59,60,61,64,64,64,64,64,64,
        64, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13,14,
        15,16,17,18,19,20,21,22,23,24,25,64,64,64,64,64,
        64,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,
        41,42,43,44,45,46,47,48,49,50,51,64,64,64,64,64
    };

    std::vector<uint8_t> result;
    result.reserve((input.size() / 4) * 3);
    uint32_t buf = 0;
    int bits = 0;
    for (char c : input) {
        if (c == '=' || c == '\n' || c == '\r') continue;
        if (static_cast<unsigned char>(c) >= 128) continue;
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

int xfer_callback(void* clientp, curl_off_t dltotal, curl_off_t dlnow,
                  curl_off_t ultotal, curl_off_t ulnow) {
    auto* cb = static_cast<std::function<void(int64_t, int64_t)>*>(clientp);
    if (cb && *cb) {
        (*cb)(static_cast<int64_t>(dlnow + ulnow), static_cast<int64_t>(dltotal + ultotal));
    }
    return 0;
}

} // anonymous namespace

class AzureBlobBackend : public IStorageBackend {
public:
    AzureBlobBackend(std::string account, std::string key,
                     std::string container, std::string sas_token,
                     std::string prefix)
        : account_(std::move(account))
        , account_key_(std::move(key))
        , container_(std::move(container))
        , sas_token_(std::move(sas_token))
        , prefix_(std::move(prefix))
    {
        // Try environment variables if not provided
        if (account_.empty()) {
            if (const char* env = std::getenv("AZURE_STORAGE_ACCOUNT"))
                account_ = env;
        }
        if (account_key_.empty()) {
            if (const char* env = std::getenv("AZURE_STORAGE_KEY"))
                account_key_ = env;
        }

        base_url_ = fmt::format("https://{}.blob.core.windows.net/{}", account_, container_);
        spdlog::debug("AzureBlobBackend: account={}, container={}", account_, container_);
    }

    int32_t put_object(const std::string& key, std::span<const uint8_t> data,
                       const std::string& content_type) override {
        auto full_key = prefixed_key(key);
        auto url = build_url(full_key);
        auto date = rfc1123_date();

        std::map<std::string, std::string> headers;
        headers["x-ms-version"] = AZURE_API_VERSION;
        headers["x-ms-date"] = date;
        headers["x-ms-blob-type"] = "BlockBlob";
        headers["content-type"] = content_type;
        headers["content-length"] = std::to_string(data.size());

        if (!sas_token_.empty()) {
            // SAS auth: no signing needed, token is appended to URL
        } else {
            auto auth = build_shared_key_auth("PUT", full_key, headers, data.size());
            headers["authorization"] = auth;
        }

        auto* curl = curl_easy_init();
        if (!curl) return SURGE_ERROR;

        struct curl_slist* hdr_list = nullptr;
        for (auto& [k, v] : headers) {
            hdr_list = curl_slist_append(hdr_list, fmt::format("{}: {}", k, v).c_str());
        }

        struct ReadState { const uint8_t* ptr; size_t remaining; };
        ReadState read_state{data.data(), data.size()};

        std::vector<uint8_t> response;
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
        curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, static_cast<curl_off_t>(data.size()));
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
            spdlog::error("Azure PUT failed: HTTP {} - {}", http_code,
                          std::string(response.begin(), response.end()));
            return SURGE_ERROR;
        }
        return SURGE_OK;
    }

    int32_t get_object(const std::string& key, std::vector<uint8_t>& out_data) override {
        auto full_key = prefixed_key(key);
        auto url = build_url(full_key);
        auto date = rfc1123_date();

        std::map<std::string, std::string> headers;
        headers["x-ms-version"] = AZURE_API_VERSION;
        headers["x-ms-date"] = date;

        if (sas_token_.empty()) {
            auto auth = build_shared_key_auth("GET", full_key, headers, 0);
            headers["authorization"] = auth;
        }

        auto* curl = curl_easy_init();
        if (!curl) return SURGE_ERROR;

        struct curl_slist* hdr_list = nullptr;
        for (auto& [k, v] : headers) {
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

        if (res != CURLE_OK) return SURGE_ERROR;
        if (http_code == 404) return SURGE_NOT_FOUND;
        if (http_code < 200 || http_code >= 300) return SURGE_ERROR;
        return SURGE_OK;
    }

    int32_t head_object(const std::string& key, ObjectInfo& out_info) override {
        auto full_key = prefixed_key(key);
        auto url = build_url(full_key);
        auto date = rfc1123_date();

        std::map<std::string, std::string> headers;
        headers["x-ms-version"] = AZURE_API_VERSION;
        headers["x-ms-date"] = date;

        if (sas_token_.empty()) {
            auto auth = build_shared_key_auth("HEAD", full_key, headers, 0);
            headers["authorization"] = auth;
        }

        auto* curl = curl_easy_init();
        if (!curl) return SURGE_ERROR;

        struct curl_slist* hdr_list = nullptr;
        for (auto& [k, v] : headers) {
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
        auto date = rfc1123_date();

        std::map<std::string, std::string> headers;
        headers["x-ms-version"] = AZURE_API_VERSION;
        headers["x-ms-date"] = date;

        if (sas_token_.empty()) {
            auto auth = build_shared_key_auth("DELETE", full_key, headers, 0);
            headers["authorization"] = auth;
        }

        auto* curl = curl_easy_init();
        if (!curl) return SURGE_ERROR;

        struct curl_slist* hdr_list = nullptr;
        for (auto& [k, v] : headers) {
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
        std::string url = fmt::format("{}?restype=container&comp=list&prefix={}&maxresults={}",
                                       base_url_, full_prefix, max_keys);
        if (!marker.empty()) {
            url += fmt::format("&marker={}", marker);
        }
        if (!sas_token_.empty()) {
            url += "&" + sas_token_;
        }

        auto date = rfc1123_date();
        std::map<std::string, std::string> headers;
        headers["x-ms-version"] = AZURE_API_VERSION;
        headers["x-ms-date"] = date;

        if (sas_token_.empty()) {
            // For list operations the resource path is the container with comp=list
            auto auth = build_shared_key_auth_list("GET", headers);
            headers["authorization"] = auth;
        }

        auto* curl = curl_easy_init();
        if (!curl) return SURGE_ERROR;

        struct curl_slist* hdr_list = nullptr;
        for (auto& [k, v] : headers) {
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

        // Parse XML response
        std::string xml(response.begin(), response.end());
        out_result.objects.clear();
        out_result.truncated = false;
        out_result.next_marker.clear();

        // Parse NextMarker
        if (auto start = xml.find("<NextMarker>"); start != std::string::npos) {
            start += std::strlen("<NextMarker>");
            auto end = xml.find("</NextMarker>", start);
            if (end != std::string::npos) {
                out_result.next_marker = xml.substr(start, end - start);
                out_result.truncated = !out_result.next_marker.empty();
            }
        }

        // Parse Blob entries
        size_t search_from = 0;
        while (true) {
            auto blob_start = xml.find("<Blob>", search_from);
            if (blob_start == std::string::npos) break;
            auto blob_end = xml.find("</Blob>", blob_start);
            if (blob_end == std::string::npos) break;
            search_from = blob_end + std::strlen("</Blob>");

            auto block = xml.substr(blob_start, blob_end - blob_start);
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

            info.key = extract("Name");
            if (!prefix_.empty() && info.key.starts_with(prefix_)) {
                info.key = info.key.substr(prefix_.size());
                if (!info.key.empty() && info.key.front() == '/') {
                    info.key = info.key.substr(1);
                }
            }
            auto size_str = extract("Content-Length");
            if (!size_str.empty()) info.size = std::stoll(size_str);
            info.etag = extract("Etag");
            info.last_modified = extract("Last-Modified");

            out_result.objects.push_back(std::move(info));
        }

        return SURGE_OK;
    }

    int32_t download_to_file(const std::string& key, const std::filesystem::path& dest,
                              std::function<void(int64_t, int64_t)> progress) override {
        auto full_key = prefixed_key(key);
        auto url = build_url(full_key);
        auto date = rfc1123_date();

        std::map<std::string, std::string> headers;
        headers["x-ms-version"] = AZURE_API_VERSION;
        headers["x-ms-date"] = date;

        if (sas_token_.empty()) {
            auto auth = build_shared_key_auth("GET", full_key, headers, 0);
            headers["authorization"] = auth;
        }

        auto* curl = curl_easy_init();
        if (!curl) return SURGE_ERROR;

        struct curl_slist* hdr_list = nullptr;
        for (auto& [k, v] : headers) {
            hdr_list = curl_slist_append(hdr_list, fmt::format("{}: {}", k, v).c_str());
        }

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

        if (sas_token_.empty()) {
            auto auth = build_shared_key_auth("PUT", full_key, headers, file_size);
            headers["authorization"] = auth;
        }

        auto* curl = curl_easy_init();
        if (!curl) return SURGE_ERROR;

        struct curl_slist* hdr_list = nullptr;
        for (auto& [k, v] : headers) {
            hdr_list = curl_slist_append(hdr_list, fmt::format("{}: {}", k, v).c_str());
        }

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
            spdlog::error("Azure upload failed: HTTP {}", http_code);
            return SURGE_ERROR;
        }
        return SURGE_OK;
    }

private:
    std::string account_;
    std::string account_key_;
    std::string container_;
    std::string sas_token_;
    std::string prefix_;
    std::string base_url_;

    std::string prefixed_key(const std::string& key) const {
        if (prefix_.empty()) return key;
        if (prefix_.back() == '/') return prefix_ + key;
        return prefix_ + "/" + key;
    }

    std::string build_url(const std::string& key) const {
        std::string url = fmt::format("{}/{}", base_url_, key);
        if (!sas_token_.empty()) {
            url += "?" + sas_token_;
        }
        return url;
    }

    std::string build_canonical_headers(const std::map<std::string, std::string>& headers) const {
        std::string result;
        for (auto& [k, v] : headers) {
            if (k.starts_with("x-ms-")) {
                result += fmt::format("{}:{}\n", k, v);
            }
        }
        return result;
    }

    std::string build_shared_key_auth(
        const std::string& method,
        const std::string& blob_name,
        const std::map<std::string, std::string>& headers,
        int64_t content_length) const
    {
        auto get_header = [&](const std::string& name) -> std::string {
            auto it = headers.find(name);
            return (it != headers.end()) ? it->second : "";
        };

        std::string content_len_str = (content_length > 0) ? std::to_string(content_length) : "";

        // StringToSign format for Blob service
        std::string string_to_sign = fmt::format(
            "{}\n"    // VERB
            "\n"      // Content-Encoding
            "\n"      // Content-Language
            "{}\n"    // Content-Length
            "\n"      // Content-MD5
            "{}\n"    // Content-Type
            "\n"      // Date
            "\n"      // If-Modified-Since
            "\n"      // If-Match
            "\n"      // If-None-Match
            "\n"      // If-Unmodified-Since
            "\n"      // Range
            "{}"      // CanonicalizedHeaders
            "/{}/{}/{}", // CanonicalizedResource
            method, content_len_str, get_header("content-type"),
            build_canonical_headers(headers),
            account_, container_, blob_name);

        auto key_bytes = base64_decode(account_key_);
        auto signature = crypto::hmac_sha256(key_bytes,
            std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(string_to_sign.data()),
                                     string_to_sign.size()));

        return fmt::format("SharedKey {}:{}", account_, base64_encode(signature));
    }

    std::string build_shared_key_auth_list(
        const std::string& method,
        const std::map<std::string, std::string>& headers) const
    {
        std::string string_to_sign = fmt::format(
            "{}\n\n\n\n\n\n\n\n\n\n\n\n{}/{}/{}\ncomp:list\nrestype:container",
            method, build_canonical_headers(headers), account_, container_);

        auto key_bytes = base64_decode(account_key_);
        auto signature = crypto::hmac_sha256(key_bytes,
            std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(string_to_sign.data()),
                                     string_to_sign.size()));

        return fmt::format("SharedKey {}:{}", account_, base64_encode(signature));
    }
};

std::unique_ptr<IStorageBackend> create_azure_backend(const StorageConfig& config) {
    return std::make_unique<AzureBlobBackend>(
        config.access_key,  // account name
        config.secret_key,  // account key
        config.bucket,      // container name
        config.endpoint,    // SAS token (or empty)
        config.prefix);
}

} // namespace surge::storage
