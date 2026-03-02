/**
 * @file s3_backend.cpp
 * @brief S3 storage backend with AWS Signature V4 authentication.
 */

#include "storage/s3_backend.hpp"
#include "core/context.hpp"
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
        while (!value.empty() && (value.front() == ' ' || value.front() == '\t'))
            value.erase(value.begin());
        while (!value.empty() && (value.back() == '\r' || value.back() == '\n'))
            value.pop_back();
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

// ----- AwsSigV4Signer -----

AwsSigV4Signer::AwsSigV4Signer(std::string_view access_key,
                                 std::string_view secret_key,
                                 std::string_view region,
                                 std::string_view service)
    : access_key_(access_key)
    , secret_key_(secret_key)
    , region_(region)
    , service_(service) {}

std::string AwsSigV4Signer::sign_request(
    std::string_view /*method*/,
    std::string_view /*uri*/,
    std::string_view /*query_string*/,
    const std::vector<std::pair<std::string, std::string>>& /*headers*/,
    std::span<const uint8_t> /*payload*/) const {
    // Placeholder - full SigV4 signing implemented inline in the backend methods
    return {};
}

std::string AwsSigV4Signer::payload_hash(std::span<const uint8_t> payload) {
    return crypto::sha256_hex(payload);
}

std::string AwsSigV4Signer::iso8601_now() { return utc_datetime(); }
std::string AwsSigV4Signer::datestamp_now() { return utc_date(); }

std::vector<uint8_t> AwsSigV4Signer::derive_signing_key(std::string_view datestamp) const {
    auto key_bytes = [](const std::string& s) -> std::vector<uint8_t> {
        return {s.begin(), s.end()};
    };

    auto k_secret = key_bytes("AWS4" + secret_key_);
    auto date_bytes = std::vector<uint8_t>(datestamp.begin(), datestamp.end());
    auto k_date = crypto::hmac_sha256(k_secret, date_bytes);
    auto region_bytes = std::vector<uint8_t>(region_.begin(), region_.end());
    auto k_region = crypto::hmac_sha256(k_date, region_bytes);
    auto service_bytes = std::vector<uint8_t>(service_.begin(), service_.end());
    auto k_service = crypto::hmac_sha256(k_region, service_bytes);
    std::string request = "aws4_request";
    auto request_bytes = std::vector<uint8_t>(request.begin(), request.end());
    return crypto::hmac_sha256(k_service, request_bytes);
}

// ----- S3StorageBackend -----

struct S3StorageBackend::Impl {
    std::string bucket;
    std::string region;
    std::string access_key;
    std::string secret_key;
    std::string endpoint;
    std::string prefix;
    bool path_style = false;

    std::string host_for_signing() const {
        std::string_view ep = endpoint;
        if (ep.starts_with("https://")) ep.remove_prefix(8);
        else if (ep.starts_with("http://")) ep.remove_prefix(7);
        auto slash = ep.find('/');
        if (slash != std::string_view::npos) ep = ep.substr(0, slash);
        return std::string(ep);
    }

    std::string uri_path(const std::string& key) const {
        if (path_style) return fmt::format("{}/{}", bucket, key);
        return key;
    }

    std::string build_authorization(
        const std::string& method, const std::string& uri,
        const std::string& query_string,
        const std::map<std::string, std::string>& headers,
        const std::string& payload_hash_val,
        const std::string& date, const std::string& datetime) const {

        // Canonical headers
        std::string canonical_headers;
        std::string signed_headers;
        for (auto& [k, v] : headers) {
            canonical_headers += fmt::format("{}:{}\n", k, v);
            if (!signed_headers.empty()) signed_headers += ';';
            signed_headers += k;
        }

        std::string canonical_uri = uri.empty() ? "/" : uri;

        // Sort query string
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

        auto canonical_request = fmt::format("{}\n{}\n{}\n{}\n{}\n{}",
            method, canonical_uri, canonical_qs,
            canonical_headers, signed_headers, payload_hash_val);

        auto canonical_hash = crypto::sha256_hex(
            std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(canonical_request.data()),
                                     canonical_request.size()));

        std::string credential_scope = fmt::format("{}/{}/s3/aws4_request", date, region);
        std::string string_to_sign = fmt::format("AWS4-HMAC-SHA256\n{}\n{}\n{}",
            datetime, credential_scope, canonical_hash);

        // Derive signing key
        auto key_bytes = [](const std::string& s) -> std::vector<uint8_t> {
            return {s.begin(), s.end()};
        };
        auto k_secret = key_bytes("AWS4" + secret_key);
        auto k_date = crypto::hmac_sha256(k_secret, std::vector<uint8_t>(date.begin(), date.end()));
        auto k_region = crypto::hmac_sha256(k_date, std::vector<uint8_t>(region.begin(), region.end()));
        std::string svc = "s3";
        auto k_service = crypto::hmac_sha256(k_region, std::vector<uint8_t>(svc.begin(), svc.end()));
        std::string req = "aws4_request";
        auto signing_key = crypto::hmac_sha256(k_service, std::vector<uint8_t>(req.begin(), req.end()));

        auto signature = crypto::hmac_sha256_hex(signing_key,
            std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(string_to_sign.data()),
                                     string_to_sign.size()));

        return fmt::format("AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
                           access_key, credential_scope, signed_headers, signature);
    }
};

S3StorageBackend::S3StorageBackend(const StorageConfig& config)
    : impl_(std::make_unique<Impl>()) {
    impl_->bucket = config.bucket;
    impl_->region = config.region;
    impl_->access_key = config.access_key;
    impl_->secret_key = config.secret_key;
    impl_->endpoint = config.endpoint;
    impl_->prefix = config.prefix;

    if (impl_->access_key.empty()) {
        if (const char* env = std::getenv("AWS_ACCESS_KEY_ID"))
            impl_->access_key = env;
    }
    if (impl_->secret_key.empty()) {
        if (const char* env = std::getenv("AWS_SECRET_ACCESS_KEY"))
            impl_->secret_key = env;
    }
    if (impl_->region.empty()) {
        if (const char* env = std::getenv("AWS_DEFAULT_REGION"))
            impl_->region = env;
        else
            impl_->region = "us-east-1";
    }
    if (impl_->endpoint.empty()) {
        impl_->endpoint = fmt::format("https://{}.s3.{}.amazonaws.com",
                                       impl_->bucket, impl_->region);
        impl_->path_style = false;
    } else {
        impl_->path_style = true;
    }
    spdlog::debug("S3StorageBackend: endpoint={}, bucket={}, region={}",
                   impl_->endpoint, impl_->bucket, impl_->region);
}

S3StorageBackend::~S3StorageBackend() = default;

std::string S3StorageBackend::build_url(const std::string& key) const {
    if (impl_->path_style) {
        if (key.empty()) return fmt::format("{}/{}", impl_->endpoint, impl_->bucket);
        return fmt::format("{}/{}/{}", impl_->endpoint, impl_->bucket, key);
    }
    if (key.empty()) return impl_->endpoint;
    return fmt::format("{}/{}", impl_->endpoint, key);
}

std::string S3StorageBackend::prefixed_key(const std::string& key) const {
    if (impl_->prefix.empty()) return key;
    if (impl_->prefix.back() == '/') return impl_->prefix + key;
    return impl_->prefix + "/" + key;
}

int32_t S3StorageBackend::put_object(const std::string& key, std::span<const uint8_t> data,
                                      const std::string& content_type) {
    auto full_key = prefixed_key(key);
    auto url = build_url(full_key);
    auto ph = crypto::sha256_hex(data);
    auto datetime = utc_datetime();
    auto date = datetime.substr(0, 8);

    std::map<std::string, std::string> headers;
    headers["host"] = impl_->host_for_signing();
    headers["x-amz-content-sha256"] = ph;
    headers["x-amz-date"] = datetime;
    headers["content-type"] = content_type;
    headers["content-length"] = std::to_string(data.size());

    auto auth = impl_->build_authorization("PUT", "/" + impl_->uri_path(full_key), "", headers, ph, date, datetime);
    headers["authorization"] = auth;

    auto* curl = curl_easy_init();
    if (!curl) return SURGE_ERROR;

    struct curl_slist* hdr_list = nullptr;
    for (auto& [k, v] : headers) {
        if (k == "host") continue;
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

    if (res != CURLE_OK || http_code < 200 || http_code >= 300) return SURGE_ERROR;
    return SURGE_OK;
}

int32_t S3StorageBackend::get_object(const std::string& key, std::vector<uint8_t>& out_data) {
    auto full_key = prefixed_key(key);
    auto url = build_url(full_key);
    auto ph = crypto::sha256_hex(std::span<const uint8_t>{});
    auto datetime = utc_datetime();
    auto date = datetime.substr(0, 8);

    std::map<std::string, std::string> headers;
    headers["host"] = impl_->host_for_signing();
    headers["x-amz-content-sha256"] = ph;
    headers["x-amz-date"] = datetime;

    auto auth = impl_->build_authorization("GET", "/" + impl_->uri_path(full_key), "", headers, ph, date, datetime);
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

    if (res != CURLE_OK) return SURGE_ERROR;
    if (http_code == 404) return SURGE_NOT_FOUND;
    if (http_code < 200 || http_code >= 300) return SURGE_ERROR;
    return SURGE_OK;
}

int32_t S3StorageBackend::head_object(const std::string& key, ObjectInfo& out_info) {
    auto full_key = prefixed_key(key);
    auto url = build_url(full_key);
    auto ph = crypto::sha256_hex(std::span<const uint8_t>{});
    auto datetime = utc_datetime();
    auto date = datetime.substr(0, 8);

    std::map<std::string, std::string> sign_headers;
    sign_headers["host"] = impl_->host_for_signing();
    sign_headers["x-amz-content-sha256"] = ph;
    sign_headers["x-amz-date"] = datetime;

    auto auth = impl_->build_authorization("HEAD", "/" + impl_->uri_path(full_key), "", sign_headers, ph, date, datetime);
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

int32_t S3StorageBackend::delete_object(const std::string& key) {
    auto full_key = prefixed_key(key);
    auto url = build_url(full_key);
    auto ph = crypto::sha256_hex(std::span<const uint8_t>{});
    auto datetime = utc_datetime();
    auto date = datetime.substr(0, 8);

    std::map<std::string, std::string> headers;
    headers["host"] = impl_->host_for_signing();
    headers["x-amz-content-sha256"] = ph;
    headers["x-amz-date"] = datetime;

    auto auth = impl_->build_authorization("DELETE", "/" + impl_->uri_path(full_key), "", headers, ph, date, datetime);
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
    if (http_code >= 200 && http_code < 300) return SURGE_OK;
    if (http_code == 404) return SURGE_OK;
    return SURGE_ERROR;
}

int32_t S3StorageBackend::list_objects(const std::string& prefix, ListResult& out_result,
                                        const std::string& marker, int max_keys) {
    // Simplified list implementation
    out_result.objects.clear();
    out_result.truncated = false;
    out_result.next_marker.clear();

    auto full_prefix = prefixed_key(prefix);
    std::string query = fmt::format("list-type=2&prefix={}&max-keys={}", url_encode(full_prefix), max_keys);
    if (!marker.empty()) {
        query += fmt::format("&continuation-token={}", url_encode(marker));
    }

    auto url = build_url("") + "?" + query;
    auto ph = crypto::sha256_hex(std::span<const uint8_t>{});
    auto datetime = utc_datetime();
    auto date = datetime.substr(0, 8);

    std::map<std::string, std::string> headers;
    headers["host"] = impl_->host_for_signing();
    headers["x-amz-content-sha256"] = ph;
    headers["x-amz-date"] = datetime;

    std::string uri_p = impl_->path_style ? fmt::format("/{}", impl_->bucket) : "/";
    auto auth = impl_->build_authorization("GET", uri_p, query, headers, ph, date, datetime);
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

    // Simple XML parsing
    std::string xml(response.begin(), response.end());
    if (xml.find("<IsTruncated>true</IsTruncated>") != std::string::npos)
        out_result.truncated = true;

    // Parse Contents entries (simplified)
    size_t search_from = 0;
    while (true) {
        auto cs = xml.find("<Contents>", search_from);
        if (cs == std::string::npos) break;
        auto ce = xml.find("</Contents>", cs);
        if (ce == std::string::npos) break;
        search_from = ce + 11;

        auto block = xml.substr(cs, ce - cs);
        ObjectInfo info;
        auto extract = [&](const std::string& tag) -> std::string {
            auto open = "<" + tag + ">";
            auto close = "</" + tag + ">";
            auto s = block.find(open);
            if (s == std::string::npos) return {};
            s += open.size();
            auto e = block.find(close, s);
            if (e == std::string::npos) return {};
            return block.substr(s, e - s);
        };

        info.key = extract("Key");
        auto size_str = extract("Size");
        if (!size_str.empty()) info.size = std::stoll(size_str);
        info.etag = extract("ETag");
        info.last_modified = extract("LastModified");
        out_result.objects.push_back(std::move(info));
    }

    return SURGE_OK;
}

int32_t S3StorageBackend::download_to_file(const std::string& key, const std::filesystem::path& dest,
                                            std::function<void(int64_t, int64_t)> progress) {
    auto full_key = prefixed_key(key);
    auto url = build_url(full_key);
    auto ph = crypto::sha256_hex(std::span<const uint8_t>{});
    auto datetime = utc_datetime();
    auto date = datetime.substr(0, 8);

    std::map<std::string, std::string> headers;
    headers["host"] = impl_->host_for_signing();
    headers["x-amz-content-sha256"] = ph;
    headers["x-amz-date"] = datetime;

    auto auth = impl_->build_authorization("GET", "/" + impl_->uri_path(full_key), "", headers, ph, date, datetime);
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

int32_t S3StorageBackend::upload_from_file(const std::string& key, const std::filesystem::path& src,
                                            std::function<void(int64_t, int64_t)> progress) {
    auto full_key = prefixed_key(key);
    auto url = build_url(full_key);
    auto file_size = std::filesystem::file_size(src);
    auto ph = crypto::sha256_hex_file(src);
    auto datetime = utc_datetime();
    auto date = datetime.substr(0, 8);

    std::map<std::string, std::string> headers;
    headers["host"] = impl_->host_for_signing();
    headers["x-amz-content-sha256"] = ph;
    headers["x-amz-date"] = datetime;
    headers["content-type"] = "application/octet-stream";
    headers["content-length"] = std::to_string(file_size);

    auto auth = impl_->build_authorization("PUT", "/" + impl_->uri_path(full_key), "", headers, ph, date, datetime);
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

    if (res != CURLE_OK || http_code < 200 || http_code >= 300) return SURGE_ERROR;
    return SURGE_OK;
}

} // namespace surge::storage
