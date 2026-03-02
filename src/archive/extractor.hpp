/**
 * @file extractor.hpp
 * @brief Archive extraction for tar.zst packages.
 */

#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace surge::archive {

/** Options for the archive extractor. */
struct ExtractorOptions {
    /**
     * Progress callback: (files_done, files_total, bytes_done, bytes_total).
     * bytes_total may be 0 if the total is not known up-front.
     */
    std::function<void(int64_t files_done, int64_t files_total,
                       int64_t bytes_done, int64_t bytes_total)> progress;

    /** If true, read and verify the embedded manifest.yml checksums. */
    bool verify_manifest = false;
};

/**
 * Extracts compressed tar archives (tar.zst) produced by ArchivePacker.
 *
 * Usage:
 * @code
 *   ArchiveExtractor ex("/tmp/release-1.0.0.tar.zst", {.verify_manifest = true});
 *   ex.extract_to("/opt/myapp/app-1.0.0");
 * @endcode
 */
class ArchiveExtractor {
public:
    /**
     * Open an archive for reading.
     * @param archive_path Path to the .tar.zst file.
     * @param options      Extraction options.
     */
    explicit ArchiveExtractor(const std::filesystem::path& archive_path,
                              const ExtractorOptions& options = {});
    ~ArchiveExtractor();

    ArchiveExtractor(const ArchiveExtractor&) = delete;
    ArchiveExtractor& operator=(const ArchiveExtractor&) = delete;

    /**
     * Extract all entries to @p dest_dir.
     * Creates the directory if it does not exist.
     * @throws std::runtime_error on I/O or decompression errors.
     */
    void extract_to(const std::filesystem::path& dest_dir);

    /**
     * Read a single archive entry into memory.
     * @param entry_path Relative path within the archive.
     * @return File contents, or an empty vector if the entry is not found.
     */
    std::vector<uint8_t> read_entry(const std::string& entry_path);

    /**
     * List all entries in the archive.
     * @return Sorted list of relative paths.
     */
    std::vector<std::string> list_entries();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace surge::archive
