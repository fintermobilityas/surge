/**
 * @file extractor.cpp
 * @brief Archive extractor using libarchive.
 */

#include "archive/extractor.hpp"
#include <archive.h>
#include <archive_entry.h>
#include <spdlog/spdlog.h>
#include <fmt/format.h>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <stdexcept>

namespace surge::archive {

namespace fs = std::filesystem;

struct ArchiveExtractor::Impl {
    struct archive* archive = nullptr;
    fs::path archive_path;
    bool opened = false;
};

ArchiveExtractor::ArchiveExtractor(const std::filesystem::path& archive_path)
    : impl_(std::make_unique<Impl>())
{
    impl_->archive_path = archive_path;
    impl_->archive = archive_read_new();
    if (!impl_->archive) {
        throw std::runtime_error("Failed to create archive reader");
    }

    archive_read_support_filter_zstd(impl_->archive);
    archive_read_support_filter_all(impl_->archive);
    archive_read_support_format_tar(impl_->archive);
    archive_read_support_format_all(impl_->archive);

    if (archive_read_open_filename(impl_->archive, archive_path.string().c_str(), 65536) != ARCHIVE_OK) {
        auto err = archive_error_string(impl_->archive);
        archive_read_free(impl_->archive);
        impl_->archive = nullptr;
        throw std::runtime_error(fmt::format("Failed to open archive {}: {}",
                                              archive_path.string(), err ? err : "unknown error"));
    }
    impl_->opened = true;
    spdlog::debug("ArchiveExtractor: opened {}", archive_path.string());
}

ArchiveExtractor::~ArchiveExtractor() {
    if (impl_ && impl_->archive) {
        if (impl_->opened) {
            archive_read_close(impl_->archive);
        }
        archive_read_free(impl_->archive);
    }
}

ArchiveExtractor::ArchiveExtractor(ArchiveExtractor&&) noexcept = default;
ArchiveExtractor& ArchiveExtractor::operator=(ArchiveExtractor&&) noexcept = default;

int32_t ArchiveExtractor::extract_all(
    const std::filesystem::path& dest_dir,
    std::function<void(int64_t items_done, int64_t items_total, const std::string& current_file)> progress)
{
    if (!impl_->opened) return SURGE_ERROR;

    fs::create_directories(dest_dir);

    // First pass: count entries (for progress)
    int64_t total_entries = 0;
    {
        struct archive* count_archive = archive_read_new();
        archive_read_support_filter_all(count_archive);
        archive_read_support_format_all(count_archive);
        if (archive_read_open_filename(count_archive, impl_->archive_path.string().c_str(), 65536) == ARCHIVE_OK) {
            struct archive_entry* entry;
            while (archive_read_next_header(count_archive, &entry) == ARCHIVE_OK) {
                total_entries++;
                archive_read_data_skip(count_archive);
            }
            archive_read_close(count_archive);
        }
        archive_read_free(count_archive);
    }

    // Reopen for actual extraction
    archive_read_close(impl_->archive);
    archive_read_free(impl_->archive);
    impl_->archive = archive_read_new();
    archive_read_support_filter_all(impl_->archive);
    archive_read_support_format_all(impl_->archive);
    if (archive_read_open_filename(impl_->archive, impl_->archive_path.string().c_str(), 65536) != ARCHIVE_OK) {
        return SURGE_ERROR;
    }
    impl_->opened = true;

    // Extract entries
    struct archive_entry* entry;
    int64_t items_done = 0;

    while (archive_read_next_header(impl_->archive, &entry) == ARCHIVE_OK) {
        const char* pathname = archive_entry_pathname(entry);
        if (!pathname) continue;

        fs::path dest_path = dest_dir / pathname;

        // Security: prevent path traversal
        auto canonical_dest = fs::weakly_canonical(dest_dir);
        auto canonical_file = fs::weakly_canonical(dest_path);
        auto [dest_end, file_end] = std::mismatch(
            canonical_dest.begin(), canonical_dest.end(),
            canonical_file.begin(), canonical_file.end());
        if (dest_end != canonical_dest.end()) {
            spdlog::warn("Skipping path-traversal entry: {}", pathname);
            archive_read_data_skip(impl_->archive);
            continue;
        }

        auto entry_type = archive_entry_filetype(entry);

        if (entry_type == AE_IFDIR) {
            fs::create_directories(dest_path);
        } else if (entry_type == AE_IFREG) {
            fs::create_directories(dest_path.parent_path());

            std::ofstream file(dest_path, std::ios::binary | std::ios::trunc);
            if (!file) {
                spdlog::error("Failed to create file: {}", dest_path.string());
                return SURGE_ERROR;
            }

            constexpr size_t BUFFER_SIZE = 65536;
            char buffer[BUFFER_SIZE];
            la_ssize_t bytes_read;
            while ((bytes_read = archive_read_data(impl_->archive, buffer, BUFFER_SIZE)) > 0) {
                file.write(buffer, bytes_read);
                if (!file) {
                    spdlog::error("Write failed for: {}", dest_path.string());
                    return SURGE_ERROR;
                }
            }
            if (bytes_read < 0) {
                spdlog::error("Error reading archive data for {}: {}",
                               pathname, archive_error_string(impl_->archive));
                return SURGE_ERROR;
            }
            file.close();

            // Restore permissions
            auto perm = archive_entry_perm(entry);
            if (perm != 0) {
                std::error_code ec;
                fs::permissions(dest_path, static_cast<fs::perms>(perm),
                                fs::perm_options::replace, ec);
            }
        } else {
            archive_read_data_skip(impl_->archive);
        }

        items_done++;
        if (progress) {
            progress(items_done, total_entries, pathname);
        }
    }

    spdlog::info("Extracted {} entries to {}", items_done, dest_dir.string());
    return SURGE_OK;
}

int32_t ArchiveExtractor::read_entry(const std::string& entry_path,
                                      std::vector<uint8_t>& out_data) {
    // Reopen archive to search from beginning
    archive_read_close(impl_->archive);
    archive_read_free(impl_->archive);
    impl_->archive = archive_read_new();
    archive_read_support_filter_all(impl_->archive);
    archive_read_support_format_all(impl_->archive);
    if (archive_read_open_filename(impl_->archive, impl_->archive_path.string().c_str(), 65536) != ARCHIVE_OK) {
        impl_->opened = false;
        return SURGE_ERROR;
    }
    impl_->opened = true;

    struct archive_entry* entry;
    while (archive_read_next_header(impl_->archive, &entry) == ARCHIVE_OK) {
        const char* pathname = archive_entry_pathname(entry);
        if (!pathname) continue;

        if (entry_path == pathname) {
            auto entry_size = archive_entry_size(entry);
            out_data.clear();
            if (entry_size > 0) {
                out_data.resize(static_cast<size_t>(entry_size));
                la_ssize_t total_read = 0;
                while (total_read < entry_size) {
                    auto bytes_read = archive_read_data(
                        impl_->archive,
                        out_data.data() + total_read,
                        static_cast<size_t>(entry_size - total_read));
                    if (bytes_read < 0) {
                        spdlog::error("Error reading entry {}: {}",
                                       entry_path, archive_error_string(impl_->archive));
                        return SURGE_ERROR;
                    }
                    if (bytes_read == 0) break;
                    total_read += bytes_read;
                }
                out_data.resize(static_cast<size_t>(total_read));
            } else {
                // Unknown size: read incrementally
                constexpr size_t BUFFER_SIZE = 65536;
                char buffer[BUFFER_SIZE];
                la_ssize_t bytes_read;
                while ((bytes_read = archive_read_data(impl_->archive, buffer, BUFFER_SIZE)) > 0) {
                    out_data.insert(out_data.end(), buffer, buffer + bytes_read);
                }
                if (bytes_read < 0) return SURGE_ERROR;
            }
            return SURGE_OK;
        }
        archive_read_data_skip(impl_->archive);
    }

    return SURGE_NOT_FOUND;
}

std::vector<std::string> ArchiveExtractor::list_entries() {
    std::vector<std::string> entries;

    // Reopen archive
    archive_read_close(impl_->archive);
    archive_read_free(impl_->archive);
    impl_->archive = archive_read_new();
    archive_read_support_filter_all(impl_->archive);
    archive_read_support_format_all(impl_->archive);
    if (archive_read_open_filename(impl_->archive, impl_->archive_path.string().c_str(), 65536) != ARCHIVE_OK) {
        impl_->opened = false;
        return entries;
    }
    impl_->opened = true;

    struct archive_entry* entry;
    while (archive_read_next_header(impl_->archive, &entry) == ARCHIVE_OK) {
        const char* pathname = archive_entry_pathname(entry);
        if (pathname) {
            entries.emplace_back(pathname);
        }
        archive_read_data_skip(impl_->archive);
    }

    return entries;
}

} // namespace surge::archive
