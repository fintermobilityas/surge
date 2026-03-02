/**
 * @file download_manager.hpp
 * @brief Parallel download manager using libcurl multi-handle.
 */

#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <span>
#include <stop_token>
#include <string>
#include <vector>

namespace surge {
class Context;
}

namespace surge::async {

/** Description of a single file to download. */
struct DownloadRequest {
    /** URL to download from. */
    std::string url;

    /** Local destination file path. */
    std::filesystem::path dest_path;

    /** Expected SHA-256 hex digest (empty = skip verification). */
    std::string expected_sha256;

    /** Expected file size in bytes (-1 = unknown). */
    int64_t expected_size = -1;
};

/** Result of a single download. */
struct DownloadResult {
    /** Index into the original request vector. */
    int32_t index = 0;

    /** 0 on success, negative error code on failure. */
    int32_t status = 0;

    /** HTTP status code (0 if the request never reached the server). */
    int32_t http_status = 0;

    /** Number of bytes downloaded. */
    int64_t bytes_downloaded = 0;

    /** Computed SHA-256 hex digest of the downloaded file. */
    std::string sha256;

    /** Human-readable error message (empty on success). */
    std::string error_message;
};

/** Aggregate progress for all downloads. */
struct DownloadProgress {
    int64_t total_bytes_done = 0;
    int64_t total_bytes_total = 0;
    int32_t files_done = 0;
    int32_t files_total = 0;
    double speed_bytes_per_sec = 0.0;
};

/** Progress callback type. */
using DownloadProgressCallback = std::function<void(const DownloadProgress&)>;

/**
 * Manages parallel file downloads using libcurl's multi-handle interface.
 *
 * Supports:
 *   - Configurable concurrency (max simultaneous transfers).
 *   - Per-file SHA-256 verification.
 *   - Bandwidth throttling.
 *   - Cooperative cancellation via std::stop_token.
 *
 * Usage:
 * @code
 *   DownloadManager dm(ctx);
 *   std::vector<DownloadRequest> reqs = { ... };
 *   auto results = dm.download(reqs, [](const DownloadProgress& p) {
 *       spdlog::info("{}%", (p.total_bytes_done * 100) / p.total_bytes_total);
 *   });
 * @endcode
 */
class DownloadManager {
public:
    /**
     * Construct a download manager.
     * @param ctx Surge context (provides resource budget, cancellation).
     */
    explicit DownloadManager(Context& ctx);
    ~DownloadManager();

    DownloadManager(const DownloadManager&) = delete;
    DownloadManager& operator=(const DownloadManager&) = delete;

    /**
     * Download all files in @p requests.
     * @param requests List of downloads to perform.
     * @param progress Optional progress callback.
     * @return One DownloadResult per request, in the same order.
     */
    std::vector<DownloadResult> download(std::span<const DownloadRequest> requests,
                                         DownloadProgressCallback progress = nullptr);

    /**
     * Download all files with explicit cancellation support.
     * @param requests   List of downloads.
     * @param stop_token Token for cooperative cancellation.
     * @param progress   Optional progress callback.
     * @return One DownloadResult per request.
     */
    std::vector<DownloadResult> download(std::span<const DownloadRequest> requests, std::stop_token stop_token,
                                         DownloadProgressCallback progress = nullptr);

    /** Set the maximum number of concurrent downloads. */
    void set_max_concurrent(int32_t max_concurrent);

    /** Set the maximum download speed in bytes per second (0 = unlimited). */
    void set_max_speed(int64_t bytes_per_sec);

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace surge::async
