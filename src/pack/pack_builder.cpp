/**
 * @file pack_builder.cpp
 * @brief Package builder: creates full and delta packages, updates release index.
 */

#include "pack/pack_builder.hpp"

#include "archive/packer.hpp"
#include "config/constants.hpp"
#include "core/context.hpp"
#include "crypto/sha256.hpp"
#include "diff/bsdiff_wrapper.hpp"
#include "releases/release_manifest.hpp"
#include "storage/storage_backend.hpp"

#include <filesystem>
#include <fmt/format.h>
#include <fstream>
#include <spdlog/spdlog.h>
#include <yaml-cpp/yaml.h>

namespace surge::pack {

namespace fs = std::filesystem;

struct PackBuilder::Impl {
    Context& ctx;
    std::shared_ptr<storage::IStorageBackend> storage;
    fs::path manifest_path;
    std::string app_id;
    std::string rid;
    std::string version;
    fs::path artifacts_dir;
    fs::path output_dir;

    std::vector<PackageArtifact> artifacts;
    releases::ReleaseIndex release_index;

    explicit Impl(Context& c) : ctx(c) {}
};

PackBuilder::PackBuilder(Context& ctx, std::filesystem::path manifest_path, std::string app_id, std::string rid,
                         std::string version, std::filesystem::path artifacts_dir)
    : impl_(std::make_unique<Impl>(ctx)) {
    impl_->manifest_path = std::move(manifest_path);
    impl_->app_id = std::move(app_id);
    impl_->rid = std::move(rid);
    impl_->version = std::move(version);
    impl_->artifacts_dir = std::move(artifacts_dir);
    impl_->output_dir = impl_->artifacts_dir.parent_path() / "packages";
    impl_->storage = std::shared_ptr<storage::IStorageBackend>(storage::create_storage_backend(ctx.storage_config()));

    fs::create_directories(impl_->output_dir);
    spdlog::debug("PackBuilder: app_id={}, version={}, rid={}", impl_->app_id, impl_->version, impl_->rid);
}

PackBuilder::~PackBuilder() = default;

int32_t PackBuilder::build(ProgressCallback progress) {
    spdlog::info("Building full package for {} v{}", impl_->app_id, impl_->version);

    impl_->artifacts.clear();

    // Validate artifacts directory
    if (!fs::exists(impl_->artifacts_dir) || !fs::is_directory(impl_->artifacts_dir)) {
        spdlog::error("Artifacts directory not found: {}", impl_->artifacts_dir.string());
        return SURGE_ERROR;
    }

    // Count files
    int64_t total_files = 0;
    std::error_code ec;
    for (auto& entry : fs::recursive_directory_iterator(impl_->artifacts_dir, ec)) {
        if (entry.is_regular_file())
            total_files++;
    }

    if (total_files == 0) {
        spdlog::error("No files found in artifacts directory: {}", impl_->artifacts_dir.string());
        return SURGE_ERROR;
    }
    spdlog::info("Found {} files in artifacts", total_files);

    // Build the tar.zst archive
    auto package_name = fmt::format("{}-{}-{}-full.tar.zst", impl_->app_id, impl_->version, impl_->rid);
    auto full_package_path = impl_->output_dir / package_name;

    archive::PackerOptions opts;
    opts.zstd_level = 9;

    int64_t files_done = 0;
    if (progress) {
        opts.progress = [&](int64_t done, int64_t /*total*/) {
            files_done = done;
            surge_progress p{};
            p.phase = SURGE_PHASE_FINALIZE;
            p.phase_percent = static_cast<int32_t>(files_done * 100 / total_files);
            p.total_percent = p.phase_percent;
            p.items_done = files_done;
            p.items_total = total_files;
            progress(p);
        };
    }

    archive::ArchivePacker packer(full_package_path, opts);

    // Add all artifact files
    packer.add_directory(impl_->artifacts_dir);

    packer.finalize();

    // Compute package checksum
    auto package_sha256 = crypto::sha256_hex_file(full_package_path);
    auto package_size = static_cast<int64_t>(fs::file_size(full_package_path));

    PackageArtifact artifact;
    artifact.path = full_package_path;
    artifact.filename = package_name;
    artifact.size = package_size;
    artifact.sha256 = package_sha256;
    artifact.is_delta = false;
    impl_->artifacts.push_back(std::move(artifact));

    spdlog::info("Full package built: {} ({} bytes, sha256={})", package_name, package_size, package_sha256);
    return SURGE_OK;
}

int32_t PackBuilder::push(const std::string& channel, ProgressCallback progress) {
    spdlog::info("Pushing packages for {} v{} to channel '{}'", impl_->app_id, impl_->version, channel);

    if (impl_->artifacts.empty()) {
        spdlog::error("No packages to push. Call build() first.");
        return SURGE_ERROR;
    }

    // Download current release index (or create new)
    std::string index_key = fmt::format("{}/{}", impl_->app_id, constants::RELEASES_FILE_COMPRESSED);
    std::vector<uint8_t> compressed;
    auto rc = impl_->storage->get_object(index_key, compressed);

    releases::ReleaseIndex index;
    if (rc == SURGE_OK) {
        try {
            index = releases::decompress_release_index(compressed);
        } catch (const std::exception& e) {
            spdlog::warn("Failed to parse existing index, creating new: {}", e.what());
            index.app_id = impl_->app_id;
            index.schema = constants::MANIFEST_SCHEMA_VERSION;
        }
    } else {
        index.app_id = impl_->app_id;
        index.schema = constants::MANIFEST_SCHEMA_VERSION;
    }

    // Determine if this is a genesis release
    bool is_genesis = true;
    for (auto& rel : index.releases) {
        for (auto& ch : rel.channels) {
            if (ch == channel) {
                is_genesis = false;
                break;
            }
        }
        if (!is_genesis)
            break;
    }

    // Upload packages
    for (auto& artifact : impl_->artifacts) {
        std::string key = fmt::format("{}/{}", impl_->app_id, artifact.filename);
        auto upload_progress = [&](int64_t done, int64_t total) {
            if (progress) {
                int pct = (total > 0) ? static_cast<int>(done * 100 / total) : 0;
                surge_progress p{};
                p.phase = SURGE_PHASE_DOWNLOAD;  // reuse for upload indication
                p.phase_percent = pct;
                p.total_percent = pct / 2;
                p.bytes_done = done;
                p.bytes_total = total;
                progress(p);
            }
        };

        rc = impl_->storage->upload_from_file(key, artifact.path, upload_progress);
        if (rc != SURGE_OK) {
            spdlog::error("Failed to upload package: {}", artifact.filename);
            return rc;
        }
    }

    // Build release entry using the header's ReleaseEntry struct
    releases::ReleaseEntry entry;
    entry.version = impl_->version;
    entry.channels = {channel};
    entry.rid = impl_->rid;
    entry.is_genesis = is_genesis;

    // Use the full package artifact
    for (auto& art : impl_->artifacts) {
        if (!art.is_delta) {
            entry.full_filename = art.filename;
            entry.full_size = art.size;
            entry.full_sha256 = art.sha256;
        } else {
            entry.delta_filename = art.filename;
            entry.delta_size = art.size;
            entry.delta_sha256 = art.sha256;
        }
    }

    index.releases.push_back(std::move(entry));

    // Serialize, compress, and upload updated index
    auto compressed_index = releases::compress_release_index(index);

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

    if (progress) {
        surge_progress p{};
        p.phase = SURGE_PHASE_FINALIZE;
        p.phase_percent = 100;
        p.total_percent = 100;
        progress(p);
    }

    spdlog::info("Successfully pushed {} v{} to channel '{}'", impl_->app_id, impl_->version, channel);
    return SURGE_OK;
}

const std::vector<PackageArtifact>& PackBuilder::artifacts() const {
    return impl_->artifacts;
}

const std::string& PackBuilder::version() const {
    return impl_->version;
}

const std::string& PackBuilder::app_id() const {
    return impl_->app_id;
}

const std::string& PackBuilder::rid() const {
    return impl_->rid;
}

}  // namespace surge::pack
