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

ArchivePacker::ArchivePacker(const std::filesystem::path& output_path, const PackerOptions& options)
    : impl_(std::make_unique<Impl>())
{
    impl_->output_path = output_path;
    impl_->options = options;

    // Ensure parent directory exists
    fs::create_directories(output_path.parent_path());

    impl_->archive = archive_write_new();
    if (!impl_->archive) {
        throw std::runtime_error("Failed to create archive writer");
    }

    // Set zstd compression (ARCHIVE_WARN is acceptable — libarchive returns it
    // when using its built-in zstd support rather than an external program)
    int zstd_ret = archive_write_add_filter_zstd(impl_->archive);
    if (zstd_ret != ARCHIVE_OK && zstd_ret != ARCHIVE_WARN) {
        auto err = archive_error_string(impl_->archive);
        archive_write_free(impl_->archive);
        throw std::runtime_error(
            fmt::format("Failed to set zstd compression filter: {}", err ? err : "unknown error"));
    }

    // Use PAX restricted (POSIX tar) format
    if (archive_write_set_format_pax_restricted(impl_->archive) != ARCHIVE_OK) {
        archive_write_free(impl_->archive);
        throw std::runtime_error("Failed to set archive format");
    }

    // Set compression level — use NULL for filter name to let libarchive
    // route to the correct filter regardless of internal naming
    auto level_str = std::to_string(options.zstd_level);
    archive_write_set_filter_option(impl_->archive, NULL, "compression-level", level_str.c_str());

    if (archive_write_open_filename(impl_->archive, output_path.string().c_str()) != ARCHIVE_OK) {
        auto err = archive_error_string(impl_->archive);
        archive_write_free(impl_->archive);
        throw std::runtime_error(fmt::format("Failed to open archive: {}", err ? err : "unknown error"));
    }

    spdlog::debug("ArchivePacker: created {} with zstd level {}", output_path.string(), options.zstd_level);
}

ArchivePacker::~ArchivePacker() {
    if (impl_ && impl_->archive) {
        if (!impl_->finalized) {
            archive_write_close(impl_->archive);
        }
        archive_write_free(impl_->archive);
    }
}

void ArchivePacker::add_file(const std::filesystem::path& source,
                              const std::string& archive_path) {
    if (impl_->finalized) {
        throw std::runtime_error("Cannot add files to finalized archive");
    }

    std::error_code ec;
    if (!fs::exists(source, ec)) {
        throw std::runtime_error(
            fmt::format("Source file does not exist: {}", source.string()));
    }

    auto file_size = static_cast<int64_t>(fs::file_size(source, ec));
    if (ec) {
        throw std::runtime_error(
            fmt::format("Failed to get file size: {}", source.string()));
    }

    auto* entry = archive_entry_new();
    if (!entry) {
        throw std::runtime_error("Failed to create archive entry");
    }

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
        throw std::runtime_error(
            fmt::format("Failed to write archive header for {}", archive_path));
    }

    // Write file contents
    std::ifstream file(source, std::ios::binary);
    if (!file) {
        archive_entry_free(entry);
        throw std::runtime_error(
            fmt::format("Failed to open source file: {}", source.string()));
    }

    constexpr size_t BUFFER_SIZE = 65536;
    char buffer[BUFFER_SIZE];
    while (file) {
        file.read(buffer, BUFFER_SIZE);
        auto bytes_read = file.gcount();
        if (bytes_read > 0) {
            if (archive_write_data(impl_->archive, buffer,
                                   static_cast<size_t>(bytes_read)) < 0) {
                archive_entry_free(entry);
                throw std::runtime_error(
                    fmt::format("Failed to write data for {}", archive_path));
            }
        }
    }

    archive_entry_free(entry);
    impl_->files_added++;
    impl_->total_bytes += file_size;

    if (impl_->options.progress) {
        impl_->options.progress(impl_->files_added, -1);
    }
}

void ArchivePacker::add_directory(const std::filesystem::path& source_dir,
                                   const std::string& archive_prefix) {
    if (impl_->finalized) {
        throw std::runtime_error("Cannot add files to finalized archive");
    }

    std::error_code ec;
    if (!fs::is_directory(source_dir, ec)) {
        throw std::runtime_error(
            fmt::format("Not a directory: {}", source_dir.string()));
    }

    for (auto& entry : fs::recursive_directory_iterator(source_dir, ec)) {
        if (ec) {
            throw std::runtime_error(
                fmt::format("Error iterating directory: {}", ec.message()));
        }

        if (!entry.is_regular_file()) continue;

        auto relative = fs::relative(entry.path(), source_dir, ec);
        if (ec) continue;

        std::string archive_path = archive_prefix.empty()
            ? relative.string()
            : archive_prefix + "/" + relative.string();

        // Normalize path separators
        std::replace(archive_path.begin(), archive_path.end(), '\\', '/');

        add_file(entry.path(), archive_path);
    }
}

void ArchivePacker::add_buffer(const std::string& archive_path,
                                std::span<const uint8_t> data,
                                mode_t permissions) {
    if (impl_->finalized) {
        throw std::runtime_error("Cannot add files to finalized archive");
    }

    auto* entry = archive_entry_new();
    if (!entry) {
        throw std::runtime_error("Failed to create archive entry");
    }

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
        throw std::runtime_error(
            fmt::format("Failed to write archive header for {}", archive_path));
    }

    if (!data.empty()) {
        if (archive_write_data(impl_->archive, data.data(), data.size()) < 0) {
            archive_entry_free(entry);
            throw std::runtime_error(
                fmt::format("Failed to write buffer data for {}", archive_path));
        }
    }

    archive_entry_free(entry);
    impl_->files_added++;
    impl_->total_bytes += static_cast<int64_t>(data.size());
}

void ArchivePacker::finalize() {
    if (impl_->finalized) return;

    if (archive_write_close(impl_->archive) != ARCHIVE_OK) {
        throw std::runtime_error(
            fmt::format("Failed to close archive: {}",
                         archive_error_string(impl_->archive)));
    }

    impl_->finalized = true;
    spdlog::info("Archive finalized: {} files, {} bytes total, output: {}",
                  impl_->files_added, impl_->total_bytes, impl_->output_path.string());
}

} // namespace surge::archive
