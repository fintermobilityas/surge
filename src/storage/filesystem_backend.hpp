/**
 * @file filesystem_backend.hpp
 * @brief Local filesystem storage backend using std::filesystem.
 */

#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <span>
#include <string>
#include <vector>

#include "storage_backend.hpp"

namespace surge {
struct StorageConfig;
}

namespace surge::storage {

/**
 * Filesystem-based storage backend.
 *
 * Stores objects as plain files under a root directory. Useful for local
 * testing and air-gapped deployments.
 *
 * StorageConfig mapping:
 *   - bucket -> root directory path
 *   - prefix -> subdirectory prefix within root
 */
class FilesystemBackend final : public IStorageBackend {
public:
    explicit FilesystemBackend(const StorageConfig& config);
    ~FilesystemBackend() override;

    FilesystemBackend(const FilesystemBackend&) = delete;
    FilesystemBackend& operator=(const FilesystemBackend&) = delete;

    int32_t put_object(
        const std::string& key,
        std::span<const uint8_t> data,
        const std::string& content_type = "application/octet-stream") override;

    int32_t get_object(
        const std::string& key,
        std::vector<uint8_t>& out_data) override;

    int32_t head_object(
        const std::string& key,
        ObjectInfo& out_info) override;

    int32_t delete_object(const std::string& key) override;

    int32_t list_objects(
        const std::string& prefix,
        ListResult& out_result,
        const std::string& marker = "",
        int max_keys = 1000) override;

    int32_t download_to_file(
        const std::string& key,
        const std::filesystem::path& dest,
        std::function<void(int64_t, int64_t)> progress = nullptr) override;

    int32_t upload_from_file(
        const std::string& key,
        const std::filesystem::path& src,
        std::function<void(int64_t, int64_t)> progress = nullptr) override;

private:
    std::filesystem::path root_dir_;
    std::string prefix_;

    /** Resolve a key to a full filesystem path. */
    std::filesystem::path resolve_path(const std::string& key) const;
};

} // namespace surge::storage
