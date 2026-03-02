/**
 * @file gcs_backend.hpp
 * @brief Google Cloud Storage backend with HMAC / OAuth2 authentication.
 */

#pragma once

#include "storage_backend.hpp"

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace surge {
struct StorageConfig;
}

namespace surge::storage {

/**
 * Authentication strategy for GCS.
 *
 * Supports both HMAC-based (S3-interop) and OAuth2 bearer-token approaches.
 */
class GcsAuthProvider {
public:
    virtual ~GcsAuthProvider() = default;

    /** Return headers to attach to the HTTP request for authentication. */
    virtual std::vector<std::pair<std::string, std::string>> auth_headers(std::string_view method,
                                                                          std::string_view resource,
                                                                          std::span<const uint8_t> payload) = 0;
};

/** HMAC-based auth compatible with the S3-interop XML API. */
class GcsHmacAuth final : public GcsAuthProvider {
public:
    GcsHmacAuth(std::string_view access_key, std::string_view secret_key);

    std::vector<std::pair<std::string, std::string>> auth_headers(std::string_view method, std::string_view resource,
                                                                  std::span<const uint8_t> payload) override;

private:
    std::string access_key_;
    std::string secret_key_;
};

/** OAuth2 bearer-token auth using service-account credentials. */
class GcsOAuth2Auth final : public GcsAuthProvider {
public:
    explicit GcsOAuth2Auth(std::string_view credentials_json);

    std::vector<std::pair<std::string, std::string>> auth_headers(std::string_view method, std::string_view resource,
                                                                  std::span<const uint8_t> payload) override;

    /** Force-refresh the bearer token. */
    void refresh_token();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

/**
 * Google Cloud Storage backend.
 *
 * Uses the JSON or XML API depending on the authentication method.
 * StorageConfig mapping:
 *   - bucket     -> GCS bucket name
 *   - access_key -> HMAC access ID or empty for OAuth2
 *   - secret_key -> HMAC secret or path to service-account JSON
 *   - endpoint   -> custom endpoint (defaults to https://storage.googleapis.com)
 */
class GcsBackend final : public IStorageBackend {
public:
    explicit GcsBackend(const StorageConfig& config);
    ~GcsBackend() override;

    GcsBackend(const GcsBackend&) = delete;
    GcsBackend& operator=(const GcsBackend&) = delete;

    int32_t put_object(const std::string& key, std::span<const uint8_t> data,
                       const std::string& content_type = "application/octet-stream") override;

    int32_t get_object(const std::string& key, std::vector<uint8_t>& out_data) override;

    int32_t head_object(const std::string& key, ObjectInfo& out_info) override;

    int32_t delete_object(const std::string& key) override;

    int32_t list_objects(const std::string& prefix, ListResult& out_result, const std::string& marker = "",
                         int max_keys = 1000) override;

    int32_t download_to_file(const std::string& key, const std::filesystem::path& dest,
                             std::function<void(int64_t, int64_t)> progress = nullptr) override;

    int32_t upload_from_file(const std::string& key, const std::filesystem::path& src,
                             std::function<void(int64_t, int64_t)> progress = nullptr) override;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;

    std::string build_url(const std::string& key) const;
    std::string prefixed_key(const std::string& key) const;
};

}  // namespace surge::storage
