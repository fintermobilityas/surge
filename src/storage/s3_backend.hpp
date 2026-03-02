/**
 * @file s3_backend.hpp
 * @brief Amazon S3 storage backend with AWS Signature V4 authentication.
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
 * AWS Signature V4 signing helper.
 *
 * Produces Authorization headers compliant with the AWS SigV4 spec for
 * S3-compatible APIs (AWS, MinIO, DigitalOcean Spaces, etc.).
 */
class AwsSigV4Signer {
public:
    AwsSigV4Signer(std::string_view access_key, std::string_view secret_key, std::string_view region,
                   std::string_view service = "s3");

    /** Build the full Authorization header value for a request. */
    std::string sign_request(std::string_view method, std::string_view uri, std::string_view query_string,
                             const std::vector<std::pair<std::string, std::string>>& headers,
                             std::span<const uint8_t> payload) const;

    /** Compute the SHA-256 hash of the payload for the x-amz-content-sha256 header. */
    static std::string payload_hash(std::span<const uint8_t> payload);

    /** Return the current UTC timestamp in ISO 8601 basic format (YYYYMMDD'T'HHMMSS'Z'). */
    static std::string iso8601_now();

    /** Return the current UTC date stamp (YYYYMMDD). */
    static std::string datestamp_now();

private:
    std::string access_key_;
    std::string secret_key_;
    std::string region_;
    std::string service_;

    std::vector<uint8_t> derive_signing_key(std::string_view datestamp) const;
};

/**
 * S3-compatible storage backend.
 *
 * Supports AWS S3, MinIO, DigitalOcean Spaces, and any endpoint that
 * speaks the S3 REST API with SigV4 authentication.
 */
class S3StorageBackend final : public IStorageBackend {
public:
    explicit S3StorageBackend(const StorageConfig& config);
    ~S3StorageBackend() override;

    S3StorageBackend(const S3StorageBackend&) = delete;
    S3StorageBackend& operator=(const S3StorageBackend&) = delete;

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

    /** Build the full URL for an object key, accounting for prefix/endpoint. */
    std::string build_url(const std::string& key) const;

    /** Prefix the key with the configured storage prefix. */
    std::string prefixed_key(const std::string& key) const;
};

}  // namespace surge::storage
