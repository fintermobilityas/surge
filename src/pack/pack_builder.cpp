/**
 * @file pack_builder.cpp
 * @brief Package builder: creates full and delta packages, updates release index.
 */

#include "pack/pack_builder.hpp"
#include "archive/packer.hpp"
#include "crypto/sha256.hpp"
#include "diff/bsdiff_wrapper.hpp"
#include "releases/release_manifest.hpp"
#include "storage/storage_backend.hpp"
#include "config/constants.hpp"
#include <spdlog/spdlog.h>
#include <fmt/format.h>
#include <yaml-cpp/yaml.h>
#include <filesystem>
#include <fstream>

namespace surge::pack {

namespace fs = std::filesystem;

struct PackBuilder::Impl {
    std::shared_ptr<storage::IStorageBackend> storage;
    std::string app_id;
    std::string rid;
    std::string version;
    std::string channel;
    fs::path artifacts_dir;
    fs::path output_dir;
    surge_resource_budget budget{};

    releases::ReleaseIndex release_index;
    releases::ReleaseEntry built_entry;
    fs::path full_package_path;
    fs::path delta_package_path;
    bool has_delta = false;

    std::vector<releases::FileChecksum> compute_file_checksums(const fs::path& dir) {
        std::vector<releases::FileChecksum> checksums;
        std::error_code ec;

        for (auto& entry : fs::recursive_directory_iterator(dir, ec)) {
            if (!entry.is_regular_file()) continue;

            auto relative = fs::relative(entry.path(), dir, ec);
            if (ec) continue;

            std::string rel_path = relative.string();
            std::replace(rel_path.begin(), rel_path.end(), '\\', '/');

            releases::FileChecksum fc;
            fc.path = rel_path;
            fc.sha256 = crypto::sha256_hex_file(entry.path());
            fc.size = static_cast<int64_t>(entry.file_size(ec));

            checksums.push_back(std::move(fc));
        }

        return checksums;
    }
};

PackBuilder::PackBuilder(std::shared_ptr<storage::IStorageBackend> storage,
                         const std::string& app_id,
                         const std::string& rid,
                         const std::string& version,
                         const std::filesystem::path& artifacts_dir,
                         const std::filesystem::path& output_dir,
                         const surge_resource_budget& budget)
    : impl_(std::make_unique<Impl>())
{
    impl_->storage = std::move(storage);
    impl_->app_id = app_id;
    impl_->rid = rid;
    impl_->version = version;
    impl_->artifacts_dir = artifacts_dir;
    impl_->output_dir = output_dir;
    impl_->budget = budget;

    fs::create_directories(output_dir);
    spdlog::debug("PackBuilder: app_id={}, version={}, rid={}", app_id, version, rid);
}

PackBuilder::~PackBuilder() = default;
PackBuilder::PackBuilder(PackBuilder&&) noexcept = default;
PackBuilder& PackBuilder::operator=(PackBuilder&&) noexcept = default;

int32_t PackBuilder::build_full_package(surge_progress_callback progress_cb,
                                         void* user_data) {
    spdlog::info("Building full package for {} v{}", impl_->app_id, impl_->version);

    // Compute file checksums for all artifacts
    auto checksums = impl_->compute_file_checksums(impl_->artifacts_dir);
    if (checksums.empty()) {
        spdlog::error("No files found in artifacts directory: {}", impl_->artifacts_dir.string());
        return SURGE_ERROR;
    }
    spdlog::info("Found {} files in artifacts", checksums.size());

    // Create embedded manifest.yml
    YAML::Emitter manifest_out;
    manifest_out << YAML::BeginMap;
    manifest_out << YAML::Key << "version" << YAML::Value << impl_->version;
    manifest_out << YAML::Key << "app_id" << YAML::Value << impl_->app_id;
    manifest_out << YAML::Key << "rid" << YAML::Value << impl_->rid;
    manifest_out << YAML::Key << "files" << YAML::Value << YAML::BeginSeq;
    for (auto& fc : checksums) {
        manifest_out << YAML::BeginMap;
        manifest_out << YAML::Key << "path" << YAML::Value << fc.path;
        manifest_out << YAML::Key << "sha256" << YAML::Value << fc.sha256;
        manifest_out << YAML::Key << "size" << YAML::Value << fc.size;
        manifest_out << YAML::EndMap;
    }
    manifest_out << YAML::EndSeq;
    manifest_out << YAML::EndMap;

    std::string manifest_str = manifest_out.c_str();
    std::vector<uint8_t> manifest_bytes(manifest_str.begin(), manifest_str.end());

    // Build the tar.zst archive
    auto package_name = fmt::format("{}-{}-{}-full.tar.zst", impl_->app_id, impl_->version, impl_->rid);
    impl_->full_package_path = impl_->output_dir / package_name;

    int zstd_level = (impl_->budget.zstd_compression_level > 0)
        ? impl_->budget.zstd_compression_level : 3;

    archive::PackerOptions opts;
    opts.zstd_level = zstd_level;
    opts.threads = impl_->budget.max_threads;

    archive::ArchivePacker packer(impl_->full_package_path, opts);

    // Add the embedded manifest first
    auto rc = packer.add_buffer(constants::ARCHIVE_MANIFEST_FILE, manifest_bytes, 0644);
    if (rc != SURGE_OK) return rc;

    // Add all artifact files
    int64_t files_done = 0;
    int64_t files_total = static_cast<int64_t>(checksums.size());

    for (auto& fc : checksums) {
        auto src_path = impl_->artifacts_dir / fc.path;
        rc = packer.add_file(src_path, fc.path);
        if (rc != SURGE_OK) {
            spdlog::error("Failed to add file to archive: {}", fc.path);
            return rc;
        }
        files_done++;
        if (progress_cb) {
            int pct = static_cast<int>(files_done * 100 / files_total);
            surge_progress prog{};
            prog.phase = SURGE_PHASE_FINALIZE;
            prog.phase_percent = pct;
            prog.total_percent = pct;
            prog.items_done = files_done;
            prog.items_total = files_total;
            progress_cb(&prog, user_data);
        }
    }

    rc = packer.finalize();
    if (rc != SURGE_OK) return rc;

    // Compute package checksum
    auto package_sha256 = crypto::sha256_hex_file(impl_->full_package_path);
    auto package_size = static_cast<int64_t>(fs::file_size(impl_->full_package_path));

    // Build release entry
    impl_->built_entry.version = impl_->version;
    impl_->built_entry.channel = impl_->channel;
    impl_->built_entry.is_genesis = false; // will be set by push logic
    impl_->built_entry.is_delta = false;
    impl_->built_entry.full.filename = package_name;
    impl_->built_entry.full.size = package_size;
    impl_->built_entry.full.sha256 = package_sha256;
    impl_->built_entry.files = checksums;

    spdlog::info("Full package built: {} ({} bytes, sha256={})",
                  package_name, package_size, package_sha256);
    return SURGE_OK;
}

int32_t PackBuilder::build_delta_package(const std::filesystem::path& previous_artifacts_dir,
                                          const std::string& base_version,
                                          surge_progress_callback progress_cb,
                                          void* user_data) {
    spdlog::info("Building delta package: {} -> {}", base_version, impl_->version);

    if (!fs::exists(previous_artifacts_dir)) {
        spdlog::error("Previous artifacts directory not found: {}", previous_artifacts_dir.string());
        return SURGE_ERROR;
    }

    auto old_checksums = impl_->compute_file_checksums(previous_artifacts_dir);
    auto new_checksums = impl_->compute_file_checksums(impl_->artifacts_dir);

    // Build lookup for old files
    std::map<std::string, releases::FileChecksum> old_map;
    for (auto& fc : old_checksums) {
        old_map[fc.path] = fc;
    }

    // Determine changed files
    struct DeltaFile {
        std::string path;
        bool is_new;        // true = new file, false = changed file (needs bsdiff)
    };
    std::vector<DeltaFile> delta_files;

    for (auto& fc : new_checksums) {
        auto it = old_map.find(fc.path);
        if (it == old_map.end()) {
            delta_files.push_back({fc.path, true});
        } else if (it->second.sha256 != fc.sha256) {
            delta_files.push_back({fc.path, false});
        }
    }

    if (delta_files.empty()) {
        spdlog::info("No changes detected between versions");
        return SURGE_OK;
    }

    spdlog::info("{} files changed/added in delta", delta_files.size());

    // Create delta archive
    auto delta_name = fmt::format("{}-{}-{}-delta-{}.tar.zst",
                                   impl_->app_id, impl_->version, impl_->rid, base_version);
    impl_->delta_package_path = impl_->output_dir / delta_name;

    int zstd_level = (impl_->budget.zstd_compression_level > 0)
        ? impl_->budget.zstd_compression_level : 3;

    archive::PackerOptions opts;
    opts.zstd_level = zstd_level;
    opts.threads = impl_->budget.max_threads;

    archive::ArchivePacker packer(impl_->delta_package_path, opts);

    int64_t files_done = 0;
    int64_t files_total = static_cast<int64_t>(delta_files.size());

    for (auto& df : delta_files) {
        if (df.is_new) {
            // New file: add directly
            auto src = impl_->artifacts_dir / df.path;
            auto rc = packer.add_file(src, df.path);
            if (rc != SURGE_OK) return rc;
        } else {
            // Changed file: compute bsdiff and add as .bsdiff
            auto old_path = previous_artifacts_dir / df.path;
            auto new_path = impl_->artifacts_dir / df.path;

            // Read old file
            std::ifstream old_stream(old_path, std::ios::binary | std::ios::ate);
            auto old_size = old_stream.tellg();
            old_stream.seekg(0);
            std::vector<uint8_t> old_data(static_cast<size_t>(old_size));
            old_stream.read(reinterpret_cast<char*>(old_data.data()),
                            static_cast<std::streamsize>(old_size));

            // Read new file
            std::ifstream new_stream(new_path, std::ios::binary | std::ios::ate);
            auto new_size = new_stream.tellg();
            new_stream.seekg(0);
            std::vector<uint8_t> new_data(static_cast<size_t>(new_size));
            new_stream.read(reinterpret_cast<char*>(new_data.data()),
                            static_cast<std::streamsize>(new_size));

            auto result = diff::bsdiff_create(old_data, new_data);
            if (!result.success) {
                spdlog::error("Failed to create bsdiff for: {}", df.path);
                return SURGE_ERROR;
            }

            // Add patch as .bsdiff file
            auto patch_path = df.path + ".bsdiff";
            auto rc = packer.add_buffer(patch_path, result.patch_data, 0644);
            if (rc != SURGE_OK) return rc;
        }

        files_done++;
        if (progress_cb) {
            int pct = static_cast<int>(files_done * 100 / files_total);
            surge_progress prog{};
            prog.phase = SURGE_PHASE_APPLY_DELTA;
            prog.phase_percent = pct;
            prog.total_percent = pct;
            prog.items_done = files_done;
            prog.items_total = files_total;
            progress_cb(&prog, user_data);
        }
    }

    auto rc = packer.finalize();
    if (rc != SURGE_OK) return rc;

    auto delta_sha256 = crypto::sha256_hex_file(impl_->delta_package_path);
    auto delta_size = static_cast<int64_t>(fs::file_size(impl_->delta_package_path));

    impl_->built_entry.is_delta = true;
    impl_->built_entry.delta.filename = delta_name;
    impl_->built_entry.delta.size = delta_size;
    impl_->built_entry.delta.sha256 = delta_sha256;
    impl_->built_entry.delta.base_version = base_version;
    impl_->has_delta = true;

    spdlog::info("Delta package built: {} ({} bytes)", delta_name, delta_size);
    return SURGE_OK;
}

int32_t PackBuilder::push(const std::string& channel,
                           surge_progress_callback progress_cb,
                           void* user_data) {
    spdlog::info("Pushing packages for {} v{} to channel '{}'",
                  impl_->app_id, impl_->version, channel);

    impl_->built_entry.channel = channel;

    // Download current release index (or create new)
    std::string index_key = fmt::format("{}/{}", impl_->app_id, constants::RELEASES_FILE_COMPRESSED);
    std::vector<uint8_t> compressed;
    auto rc = impl_->storage->get_object(index_key, compressed);

    releases::ReleaseIndex index;
    if (rc == SURGE_OK) {
        auto yaml_data = releases::decompress_release_index(compressed);
        index = releases::parse_release_index(yaml_data);
    } else {
        index.app_id = impl_->app_id;
        index.schema = constants::MANIFEST_SCHEMA_VERSION;
    }

    // Determine if this is a genesis release (first release on this channel)
    bool is_genesis = true;
    for (auto& rel : index.releases) {
        if (rel.channel == channel) {
            is_genesis = false;
            break;
        }
    }
    impl_->built_entry.is_genesis = is_genesis;

    // Ensure channel exists in index
    if (std::find(index.channels.begin(), index.channels.end(), channel) == index.channels.end()) {
        index.channels.push_back(channel);
    }

    // Upload full package
    std::string full_key = fmt::format("{}/{}", impl_->app_id, impl_->built_entry.full.filename);
    auto upload_progress = [&](int64_t done, int64_t total) {
        if (progress_cb) {
            int pct = (total > 0) ? static_cast<int>(done * 100 / total) : 0;
            surge_progress prog{};
            prog.phase = SURGE_PHASE_DOWNLOAD; // reuse for upload indication
            prog.phase_percent = pct;
            prog.total_percent = pct / 2;
            prog.bytes_done = done;
            prog.bytes_total = total;
            progress_cb(&prog, user_data);
        }
    };

    rc = impl_->storage->upload_from_file(full_key, impl_->full_package_path, upload_progress);
    if (rc != SURGE_OK) {
        spdlog::error("Failed to upload full package");
        return rc;
    }

    // Upload delta package if it exists
    if (impl_->has_delta && fs::exists(impl_->delta_package_path)) {
        std::string delta_key = fmt::format("{}/{}", impl_->app_id, impl_->built_entry.delta.filename);
        rc = impl_->storage->upload_from_file(delta_key, impl_->delta_package_path, nullptr);
        if (rc != SURGE_OK) {
            spdlog::error("Failed to upload delta package");
            return rc;
        }
    }

    // Add release entry to index
    index.releases.push_back(impl_->built_entry);

    // Serialize, compress, and upload updated index
    auto yaml_data = releases::serialize_release_index(index);
    auto compressed_index = releases::compress_release_index(yaml_data);

    rc = impl_->storage->put_object(index_key, compressed_index, "application/octet-stream");
    if (rc != SURGE_OK) {
        spdlog::error("Failed to upload release index");
        return rc;
    }

    // Upload checksum
    auto index_hash = crypto::sha256_hex(compressed_index);
    std::string checksum_key = fmt::format("{}/{}", impl_->app_id, constants::RELEASES_CHECKSUM_FILE);
    std::vector<uint8_t> hash_bytes(index_hash.begin(), index_hash.end());
    rc = impl_->storage->put_object(checksum_key, hash_bytes, "text/plain");
    if (rc != SURGE_OK) {
        spdlog::warn("Failed to upload release checksum (non-fatal)");
    }

    if (progress_cb) {
        surge_progress prog{};
        prog.phase = SURGE_PHASE_FINALIZE;
        prog.phase_percent = 100;
        prog.total_percent = 100;
        progress_cb(&prog, user_data);
    }

    spdlog::info("Successfully pushed {} v{} to channel '{}'",
                  impl_->app_id, impl_->version, channel);
    return SURGE_OK;
}

} // namespace surge::pack
