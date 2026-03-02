/**
 * @file storage_backend.hpp
 * @brief Abstract storage interface and factory for cloud / local backends.
 */

#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace surge {
struct StorageConfig; // forward from core/context.hpp
}

namespace surge::storage {

/** Metadata about a stored object. */
struct ObjectInfo {
    std::string key;
    int64_t     size = 0;
    std::string etag;
    std::string last_modified;
};

/** Paginated listing result. */
struct ListResult {
    std::vector<ObjectInfo> objects;
    std::string             next_marker;
    bool                    truncated = false;
};

/**
 * Abstract base for all storage backends (S3, Azure Blob, GCS, filesystem).
 *
 * All methods return 0 (SURGE_OK) on success and a negative error code on
 * failure.
 */
class IStorageBackend {
public:
    virtual ~IStorageBackend() = default;

    /** Upload an in-memory buffer. */
    virtual int32_t put_object(
        const std::string& key,
        std::span<const uint8_t> data,
        const std::string& content_type = "application/octet-stream") = 0;

    /** Download an object into a memory buffer. */
    virtual int32_t get_object(
        const std::string& key,
        std::vector<uint8_t>& out_data) = 0;

    /** Retrieve metadata for an object without downloading its body. */
    virtual int32_t head_object(
        const std::string& key,
        ObjectInfo& out_info) = 0;

    /** Delete a single object. */
    virtual int32_t delete_object(const std::string& key) = 0;

    /** List objects under a prefix with optional pagination. */
    virtual int32_t list_objects(
        const std::string& prefix,
        ListResult& out_result,
        const std::string& marker = "",
        int max_keys = 1000) = 0;

    /** Download an object directly to a file on disk. */
    virtual int32_t download_to_file(
        const std::string& key,
        const std::filesystem::path& dest,
        std::function<void(int64_t, int64_t)> progress = nullptr) = 0;

    /** Upload a file from disk. */
    virtual int32_t upload_from_file(
        const std::string& key,
        const std::filesystem::path& src,
        std::function<void(int64_t, int64_t)> progress = nullptr) = 0;
};

/**
 * Factory: create a storage backend from configuration.
 * @param config Storage configuration from the context.
 * @return Owning pointer to the concrete backend, or nullptr on invalid config.
 */
std::unique_ptr<IStorageBackend> create_storage_backend(
    const StorageConfig& config);

} // namespace surge::storage
