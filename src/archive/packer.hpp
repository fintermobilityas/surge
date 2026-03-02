/**
 * @file packer.hpp
 * @brief Archive creation using tar + zstd compression.
 */

#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <sys/types.h>

namespace surge::archive {

/** Options for the archive packer. */
struct PackerOptions {
    /** Zstandard compression level (1-22, default 9). */
    int zstd_level = 9;

    /** Progress callback: (files_done, files_total). */
    std::function<void(int64_t files_done, int64_t files_total)> progress;
};

/**
 * Creates compressed tar archives (tar.zst) for release packaging.
 *
 * Usage:
 * @code
 *   ArchivePacker packer("/tmp/release-1.0.0.tar.zst", {.zstd_level = 12});
 *   packer.add_directory("/build/output");
 *   packer.add_buffer("manifest.yml", yaml_bytes);
 *   packer.finalize();
 * @endcode
 */
class ArchivePacker {
public:
    /**
     * Construct a packer that writes to @p output_path.
     * @param output_path Destination file (will be created / overwritten).
     * @param options     Compression and progress options.
     */
    explicit ArchivePacker(const std::filesystem::path& output_path,
                           const PackerOptions& options = {});
    ~ArchivePacker();

    ArchivePacker(const ArchivePacker&) = delete;
    ArchivePacker& operator=(const ArchivePacker&) = delete;

    /**
     * Add a single file to the archive.
     * @param source       Path to the file on disk.
     * @param archive_path Relative path inside the archive.
     */
    void add_file(const std::filesystem::path& source,
                  const std::string& archive_path);

    /**
     * Recursively add a directory to the archive.
     * @param source_dir     Directory to walk.
     * @param archive_prefix Optional prefix prepended to all entries.
     */
    void add_directory(const std::filesystem::path& source_dir,
                       const std::string& archive_prefix = "");

    /**
     * Add an in-memory buffer as a file entry.
     * @param archive_path Relative path inside the archive.
     * @param data         Buffer contents.
     * @param permissions  POSIX permissions (default 0644).
     */
    void add_buffer(const std::string& archive_path,
                    std::span<const uint8_t> data,
                    mode_t permissions = 0644);

    /**
     * Flush and close the archive. Must be called before the file is usable.
     * Calling finalize() more than once is a no-op.
     */
    void finalize();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace surge::archive
