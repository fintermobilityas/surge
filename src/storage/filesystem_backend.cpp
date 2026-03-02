/**
 * @file filesystem_backend.cpp
 * @brief Local filesystem storage backend.
 */

#include "storage/storage_backend.hpp"
#include <spdlog/spdlog.h>
#include <fmt/format.h>
#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>

namespace surge::storage {

namespace fs = std::filesystem;

class FilesystemBackend : public IStorageBackend {
public:
    explicit FilesystemBackend(std::string root_path, std::string prefix)
        : root_(std::move(root_path))
        , prefix_(std::move(prefix))
    {
        if (!prefix_.empty()) {
            root_ = (fs::path(root_) / prefix_).string();
        }
        spdlog::debug("FilesystemBackend: root={}", root_);
    }

    int32_t put_object(const std::string& key, std::span<const uint8_t> data,
                       [[maybe_unused]] const std::string& content_type) override {
        auto path = key_to_path(key);
        std::error_code ec;
        fs::create_directories(path.parent_path(), ec);
        if (ec) {
            spdlog::error("Failed to create directories for {}: {}", path.string(), ec.message());
            return SURGE_ERROR;
        }

        std::ofstream file(path, std::ios::binary | std::ios::trunc);
        if (!file) {
            spdlog::error("Failed to open file for writing: {}", path.string());
            return SURGE_ERROR;
        }
        file.write(reinterpret_cast<const char*>(data.data()),
                    static_cast<std::streamsize>(data.size()));
        if (!file) {
            spdlog::error("Failed to write to file: {}", path.string());
            return SURGE_ERROR;
        }
        return SURGE_OK;
    }

    int32_t get_object(const std::string& key, std::vector<uint8_t>& out_data) override {
        auto path = key_to_path(key);
        if (!fs::exists(path)) return SURGE_NOT_FOUND;

        std::ifstream file(path, std::ios::binary | std::ios::ate);
        if (!file) return SURGE_ERROR;

        auto size = file.tellg();
        file.seekg(0);
        out_data.resize(static_cast<size_t>(size));
        file.read(reinterpret_cast<char*>(out_data.data()),
                  static_cast<std::streamsize>(size));
        if (!file) return SURGE_ERROR;
        return SURGE_OK;
    }

    int32_t head_object(const std::string& key, ObjectInfo& out_info) override {
        auto path = key_to_path(key);
        std::error_code ec;
        auto status = fs::status(path, ec);
        if (ec || !fs::exists(status)) return SURGE_NOT_FOUND;

        out_info.key = key;
        out_info.size = static_cast<int64_t>(fs::file_size(path, ec));
        if (ec) return SURGE_ERROR;

        auto ftime = fs::last_write_time(path, ec);
        if (!ec) {
            auto sctp = std::chrono::clock_cast<std::chrono::system_clock>(ftime);
            auto time = std::chrono::system_clock::to_time_t(sctp);
            std::tm tm{};
            gmtime_r(&time, &tm);
            char buf[64];
            std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm);
            out_info.last_modified = buf;
        }
        out_info.etag = "";
        return SURGE_OK;
    }

    int32_t delete_object(const std::string& key) override {
        auto path = key_to_path(key);
        std::error_code ec;
        if (!fs::exists(path, ec)) return SURGE_OK;
        fs::remove(path, ec);
        if (ec) {
            spdlog::error("Failed to delete {}: {}", path.string(), ec.message());
            return SURGE_ERROR;
        }
        return SURGE_OK;
    }

    int32_t list_objects(const std::string& prefix, ListResult& out_result,
                         [[maybe_unused]] const std::string& marker, int max_keys) override {
        auto dir = key_to_path(prefix);
        out_result.objects.clear();
        out_result.truncated = false;
        out_result.next_marker.clear();

        std::error_code ec;
        if (!fs::exists(dir, ec)) return SURGE_OK;

        // If prefix points to a directory, list its contents recursively
        // If it points to something else or doesn't exist, try the parent
        fs::path list_dir = fs::is_directory(dir, ec) ? dir : dir.parent_path();
        std::string prefix_filter = fs::is_directory(dir, ec) ? "" : dir.filename().string();

        int count = 0;
        for (auto& entry : fs::recursive_directory_iterator(list_dir, ec)) {
            if (ec) break;
            if (!entry.is_regular_file()) continue;

            std::string relative = fs::relative(entry.path(), fs::path(root_), ec).string();
            if (ec) continue;

            // Replace backslashes with forward slashes for consistency
            std::replace(relative.begin(), relative.end(), '\\', '/');

            // Apply prefix filter if needed
            if (!prefix_filter.empty() && !relative.starts_with(prefix_filter))
                continue;

            ObjectInfo info;
            info.key = relative;
            info.size = static_cast<int64_t>(entry.file_size(ec));
            if (ec) continue;

            auto ftime = entry.last_write_time(ec);
            if (!ec) {
                auto sctp = std::chrono::clock_cast<std::chrono::system_clock>(ftime);
                auto time = std::chrono::system_clock::to_time_t(sctp);
                std::tm tm{};
                gmtime_r(&time, &tm);
                char buf[64];
                std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm);
                info.last_modified = buf;
            }

            out_result.objects.push_back(std::move(info));
            if (++count >= max_keys) {
                out_result.truncated = true;
                break;
            }
        }

        return SURGE_OK;
    }

    int32_t download_to_file(const std::string& key, const std::filesystem::path& dest,
                              std::function<void(int64_t, int64_t)> progress) override {
        auto src_path = key_to_path(key);
        std::error_code ec;
        if (!fs::exists(src_path, ec)) return SURGE_NOT_FOUND;

        fs::create_directories(dest.parent_path(), ec);
        if (ec) return SURGE_ERROR;

        auto file_size = static_cast<int64_t>(fs::file_size(src_path, ec));
        if (ec) return SURGE_ERROR;

        std::ifstream src_file(src_path, std::ios::binary);
        std::ofstream dst_file(dest, std::ios::binary | std::ios::trunc);
        if (!src_file || !dst_file) return SURGE_ERROR;

        constexpr size_t BUFFER_SIZE = 65536;
        char buffer[BUFFER_SIZE];
        int64_t bytes_copied = 0;

        while (src_file) {
            src_file.read(buffer, BUFFER_SIZE);
            auto bytes_read = src_file.gcount();
            if (bytes_read > 0) {
                dst_file.write(buffer, bytes_read);
                if (!dst_file) return SURGE_ERROR;
                bytes_copied += bytes_read;
                if (progress) progress(bytes_copied, file_size);
            }
        }

        return SURGE_OK;
    }

    int32_t upload_from_file(const std::string& key, const std::filesystem::path& src,
                              std::function<void(int64_t, int64_t)> progress) override {
        auto dest_path = key_to_path(key);
        std::error_code ec;
        fs::create_directories(dest_path.parent_path(), ec);
        if (ec) return SURGE_ERROR;

        auto file_size = static_cast<int64_t>(fs::file_size(src, ec));
        if (ec) return SURGE_ERROR;

        std::ifstream src_file(src, std::ios::binary);
        std::ofstream dst_file(dest_path, std::ios::binary | std::ios::trunc);
        if (!src_file || !dst_file) return SURGE_ERROR;

        constexpr size_t BUFFER_SIZE = 65536;
        char buffer[BUFFER_SIZE];
        int64_t bytes_copied = 0;

        while (src_file) {
            src_file.read(buffer, BUFFER_SIZE);
            auto bytes_read = src_file.gcount();
            if (bytes_read > 0) {
                dst_file.write(buffer, bytes_read);
                if (!dst_file) return SURGE_ERROR;
                bytes_copied += bytes_read;
                if (progress) progress(bytes_copied, file_size);
            }
        }

        return SURGE_OK;
    }

private:
    std::string root_;
    std::string prefix_;

    fs::path key_to_path(const std::string& key) const {
        return fs::path(root_) / key;
    }
};

std::unique_ptr<IStorageBackend> create_filesystem_backend(const StorageConfig& config) {
    return std::make_unique<FilesystemBackend>(config.bucket, config.prefix);
}

// Storage backend factory
std::unique_ptr<IStorageBackend> create_storage_backend(const StorageConfig& config) {
    switch (config.provider) {
        case SURGE_STORAGE_S3: {
            extern std::unique_ptr<IStorageBackend> create_s3_backend(const StorageConfig&);
            return create_s3_backend(config);
        }
        case SURGE_STORAGE_AZURE_BLOB: {
            extern std::unique_ptr<IStorageBackend> create_azure_backend(const StorageConfig&);
            return create_azure_backend(config);
        }
        case SURGE_STORAGE_GCS: {
            extern std::unique_ptr<IStorageBackend> create_gcs_backend(const StorageConfig&);
            return create_gcs_backend(config);
        }
        case SURGE_STORAGE_FILESYSTEM:
            return create_filesystem_backend(config);
        default:
            spdlog::error("Unknown storage provider: {}", static_cast<int>(config.provider));
            return nullptr;
    }
}

} // namespace surge::storage
