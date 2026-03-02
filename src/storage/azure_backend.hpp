/**
 * @file azure_backend.hpp
 * @brief Azure Blob Storage backend with SharedKey authentication.
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
 * Azure SharedKey authentication signer.
 *
 * Produces Authorization headers per the Azure Storage SharedKey spec.
 */
class AzureSharedKeySigner {
public:
    AzureSharedKeySigner(std::string_view account_name, std::string_view account_key);

    /** Build the Authorization header for a Blob Service request. */
    std::string sign_request(std::string_view method, std::string_view resource_path,
                             const std::vector<std::pair<std::string, std::string>>& headers,
                             const std::vector<std::pair<std::string, std::string>>& query_params) const;

    /** Format the current UTC time in RFC 1123 format for the x-ms-date header. */
    static std::string rfc1123_now();

private:
    std::string account_name_;
    std::vector<uint8_t> account_key_decoded_;
};

/**
 * Azure Blob Storage backend.
 *
 * Uses the Blob Service REST API with SharedKey authentication.
 * The StorageConfig fields are mapped as:
 *   - bucket     -> container name
 *   - access_key -> account name
 *   - secret_key -> account key (Base64-encoded)
 *   - endpoint   -> custom endpoint URL (defaults to https://<account>.blob.core.windows.net)
 */
class AzureBlobBackend final : public IStorageBackend {
public:
    explicit AzureBlobBackend(const StorageConfig& config);
    ~AzureBlobBackend() override;

    AzureBlobBackend(const AzureBlobBackend&) = delete;
    AzureBlobBackend& operator=(const AzureBlobBackend&) = delete;

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
