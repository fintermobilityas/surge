/**
 * @file packer.cpp
 * @brief Archive packer using libarchive (tar.zst format).
 */

#include "archive/packer.hpp"
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

struct ArchivePacker::Impl {
    struct archive* archive = nullptr;
    fs::path output_path;
    PackerOptions options;
    int64_t files_added = 0;
    int64_t total_bytes = 0;
    bool finalized = false;
};

ArchivePacker::ArchivePacker(const std::filesystem::path& output_path, const PackerOptions& opts)
    : impl_(std::make_unique<Impl>())
{
    impl_->output_path = output_path;
    impl_->options = opts;

    // Ensure parent directory exists
    fs::create_directories(output_path.parent_path());

    impl_->archive = archive_write_new();
    if (!impl_->archive) {
        throw std::runtime_error("Failed to create archive writer");
    }

    // Set zstd compression
    if (archive_write_add_filter_zstd(impl_->archive) != ARCHIVE_OK) {
        archive_write_free(impl_->archive);
        throw std::runtime_error("Failed to set zstd compression filter");
    }

    // Use PAX restricted (POSIX tar) format
    if (archive_write_set_format_pax_restricted(impl_->archive) != ARCHIVE_OK) {
        archive_write_free(impl_->archive);
        throw std::runtime_error("Failed to set archive format");
    }

    // Set compression level
    auto level_str = std::to_string(opts.zstd_level);
    archive_write_set_filter_option(impl_->archive, "zstd", "compression-level", level_str.c_str());

    // Set thread count if specified
    if (opts.threads > 0) {
        auto threads_str = std::to_string(opts.threads);
        archive_write_set_filter_option(impl_->archive, "zstd", "threads", threads_str.c_str());
    }

    if (archive_write_open_filename(impl_->archive, output_path.string().c_str()) != ARCHIVE_OK) {
        auto err = archive_error_string(impl_->archive);
        archive_write_free(impl_->archive);
        throw std::runtime_error(fmt::format("Failed to open archive: {}", err ? err : "unknown error"));
    }

    spdlog::debug("ArchivePacker: created {} with zstd level {}", output_path.string(), opts.zstd_level);
}

ArchivePacker::~ArchivePacker() {
    if (impl_ && impl_->archive) {
        if (!impl_->finalized) {
            archive_write_close(impl_->archive);
        }
        archive_write_free(impl_->archive);
    }
}

ArchivePacker::ArchivePacker(ArchivePacker&&) noexcept = default;
ArchivePacker& ArchivePacker::operator=(ArchivePacker&&) noexcept = default;

int32_t ArchivePacker::add_file(const std::filesystem::path& source,
                                 const std::string& archive_path) {
    if (impl_->finalized) {
        spdlog::error("Cannot add files to finalized archive");
        return SURGE_ERROR;
    }

    std::error_code ec;
    if (!fs::exists(source, ec)) {
        spdlog::error("Source file does not exist: {}", source.string());
        return SURGE_ERROR;
    }

    auto file_size = static_cast<int64_t>(fs::file_size(source, ec));
    if (ec) {
        spdlog::error("Failed to get file size: {}", source.string());
        return SURGE_ERROR;
    }

    auto* entry = archive_entry_new();
    if (!entry) return SURGE_ERROR;

    archive_entry_set_pathname(entry, archive_path.c_str());
    archive_entry_set_size(entry, file_size);
    archive_entry_set_filetype(entry, AE_IFREG);

    // Preserve file permissions
    auto perms = fs::status(source, ec).permissions();
    if (!ec) {
        archive_entry_set_perm(entry, static_cast<mode_t>(perms));
    } else {
        archive_entry_set_perm(entry, 0644);
    }

    // Set modification time
    auto ftime = fs::last_write_time(source, ec);
    if (!ec) {
        auto sctp = std::chrono::clock_cast<std::chrono::system_clock>(ftime);
        auto time = std::chrono::system_clock::to_time_t(sctp);
        archive_entry_set_mtime(entry, time, 0);
    }

    if (archive_write_header(impl_->archive, entry) != ARCHIVE_OK) {
        spdlog::error("Failed to write header for {}: {}",
                       archive_path, archive_error_string(impl_->archive));
        archive_entry_free(entry);
        return SURGE_ERROR;
    }

    // Write file contents
    std::ifstream file(source, std::ios::binary);
    if (!file) {
        archive_entry_free(entry);
        return SURGE_ERROR;
    }

    constexpr size_t BUFFER_SIZE = 65536;
    char buffer[BUFFER_SIZE];
    while (file) {
        file.read(buffer, BUFFER_SIZE);
        auto bytes_read = file.gcount();
        if (bytes_read > 0) {
            if (archive_write_data(impl_->archive, buffer,
                                   static_cast<size_t>(bytes_read)) < 0) {
                spdlog::error("Failed to write data for {}: {}",
                               archive_path, archive_error_string(impl_->archive));
                archive_entry_free(entry);
                return SURGE_ERROR;
            }
        }
    }

    archive_entry_free(entry);
    impl_->files_added++;
    impl_->total_bytes += file_size;
    return SURGE_OK;
}

int32_t ArchivePacker::add_directory(const std::filesystem::path& source_dir,
                                      const std::string& archive_prefix) {
    if (impl_->finalized) return SURGE_ERROR;

    std::error_code ec;
    if (!fs::is_directory(source_dir, ec)) {
        spdlog::error("Not a directory: {}", source_dir.string());
        return SURGE_ERROR;
    }

    for (auto& entry : fs::recursive_directory_iterator(source_dir, ec)) {
        if (ec) {
            spdlog::error("Error iterating directory: {}", ec.message());
            return SURGE_ERROR;
        }

        if (!entry.is_regular_file()) continue;

        auto relative = fs::relative(entry.path(), source_dir, ec);
        if (ec) continue;

        std::string archive_path = archive_prefix.empty()
            ? relative.string()
            : archive_prefix + "/" + relative.string();

        // Normalize path separators
        std::replace(archive_path.begin(), archive_path.end(), '\\', '/');

        auto result = add_file(entry.path(), archive_path);
        if (result != SURGE_OK) return result;
    }

    return SURGE_OK;
}

int32_t ArchivePacker::add_buffer(const std::string& archive_path,
                                   std::span<const uint8_t> data,
                                   mode_t permissions) {
    if (impl_->finalized) return SURGE_ERROR;

    auto* entry = archive_entry_new();
    if (!entry) return SURGE_ERROR;

    archive_entry_set_pathname(entry, archive_path.c_str());
    archive_entry_set_size(entry, static_cast<int64_t>(data.size()));
    archive_entry_set_filetype(entry, AE_IFREG);
    archive_entry_set_perm(entry, permissions);

    auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    archive_entry_set_mtime(entry, now, 0);

    if (archive_write_header(impl_->archive, entry) != ARCHIVE_OK) {
        spdlog::error("Failed to write header for {}: {}",
                       archive_path, archive_error_string(impl_->archive));
        archive_entry_free(entry);
        return SURGE_ERROR;
    }

    if (!data.empty()) {
        if (archive_write_data(impl_->archive, data.data(), data.size()) < 0) {
            spdlog::error("Failed to write buffer data for {}: {}",
                           archive_path, archive_error_string(impl_->archive));
            archive_entry_free(entry);
            return SURGE_ERROR;
        }
    }

    archive_entry_free(entry);
    impl_->files_added++;
    impl_->total_bytes += static_cast<int64_t>(data.size());
    return SURGE_OK;
}

int32_t ArchivePacker::finalize() {
    if (impl_->finalized) return SURGE_OK;

    if (archive_write_close(impl_->archive) != ARCHIVE_OK) {
        spdlog::error("Failed to close archive: {}",
                       archive_error_string(impl_->archive));
        return SURGE_ERROR;
    }

    impl_->finalized = true;
    spdlog::info("Archive finalized: {} files, {} bytes total, output: {}",
                  impl_->files_added, impl_->total_bytes, impl_->output_path.string());
    return SURGE_OK;
}

int64_t ArchivePacker::files_added() const {
    return impl_->files_added;
}

int64_t ArchivePacker::total_bytes() const {
    return impl_->total_bytes;
}

} // namespace surge::archive
