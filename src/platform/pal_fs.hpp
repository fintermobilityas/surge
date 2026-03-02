/**
 * @file pal_fs.hpp
 * @brief Platform Abstraction Layer -- filesystem utilities.
 */

#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace surge::platform {

/**
 * Atomically rename a file or directory, falling back to copy+delete on
 * cross-device moves.
 * @return true on success.
 */
bool atomic_rename(const std::filesystem::path& src,
                   const std::filesystem::path& dst);

/**
 * Copy a file with progress reporting.
 * @param src      Source file.
 * @param dst      Destination file (created or overwritten).
 * @param progress Callback receiving (bytes_done, bytes_total).
 * @return true on success.
 */
bool copy_file_with_progress(
    const std::filesystem::path& src,
    const std::filesystem::path& dst,
    std::function<void(int64_t, int64_t)> progress = nullptr);

/**
 * Recursively copy a directory tree.
 * @return true on success.
 */
bool copy_directory(const std::filesystem::path& src,
                    const std::filesystem::path& dst);

/**
 * Recursively delete a directory. Retries on transient lock errors (Windows).
 * @return true on success.
 */
bool remove_directory(const std::filesystem::path& dir);

/**
 * Create a temporary directory with a unique name.
 * @param prefix Prefix for the directory name.
 * @return Path to the created directory.
 */
std::filesystem::path create_temp_dir(std::string_view prefix = "surge-");

/**
 * Set POSIX file permissions. No-op on Windows.
 * @param path File or directory.
 * @param mode POSIX mode bits (e.g. 0755).
 * @return true on success (or Windows).
 */
bool set_permissions(const std::filesystem::path& path, uint32_t mode);

/**
 * Make a file executable (chmod +x). No-op on Windows.
 * @return true on success.
 */
bool make_executable(const std::filesystem::path& path);

/**
 * Read an entire file into a byte vector.
 * @return File contents, or std::nullopt on failure.
 */
std::optional<std::vector<uint8_t>> read_file(
    const std::filesystem::path& path);

/**
 * Write bytes to a file atomically (write to temp + rename).
 * @return true on success.
 */
bool write_file_atomic(const std::filesystem::path& path,
                       const std::vector<uint8_t>& data);

/**
 * List immediate child directories of a path, sorted by name.
 */
std::vector<std::filesystem::path> list_directories(
    const std::filesystem::path& parent);

/**
 * Compute the total size of all files under a directory tree.
 */
int64_t directory_size(const std::filesystem::path& dir);

} // namespace surge::platform
