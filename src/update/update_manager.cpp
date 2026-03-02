/**
 * @file update_manager.cpp
 * @brief Core update manager: check, download, verify, extract, apply deltas.
 */

#include "update/update_manager.hpp"
#include "core/context.hpp"
#include "storage/storage_backend.hpp"
#include "releases/release_manifest.hpp"
#include "archive/extractor.hpp"
#include "diff/bsdiff_wrapper.hpp"
#include "crypto/sha256.hpp"
#include "config/constants.hpp"
#include <spdlog/spdlog.h>
#include <fmt/format.h>
#include <filesystem>
#include <fstream>

namespace surge::update {

namespace fs = std::filesystem;

struct UpdateManager::Impl {
    Context& ctx;
    std::shared_ptr<storage::IStorageBackend> storage;
    std::string app_id;
    std::string current_version;
    std::string channel;
    fs::path install_dir;

    releases::ReleaseIndex cached_index;
    bool index_loaded = false;

    explicit Impl(Context& c) : ctx(c) {}
};

UpdateManager::UpdateManager(Context& ctx,
                              std::string app_id,
                              std::string current_version,
                              std::string channel,
                              std::filesystem::path install_dir)
    : impl_(std::make_unique<Impl>(ctx))
{
    impl_->app_id = std::move(app_id);
    impl_->current_version = std::move(current_version);
    impl_->channel = std::move(channel);
    impl_->install_dir = std::move(install_dir);
    impl_->storage = std::shared_ptr<storage::IStorageBackend>(
        storage::create_storage_backend(ctx.storage_config()));

    spdlog::debug("UpdateManager: app_id={}, current={}, channel={}, install_dir={}",
                   impl_->app_id, impl_->current_version, impl_->channel,
                   impl_->install_dir.string());
}

UpdateManager::~UpdateManager() = default;

std::optional<UpdateInfo> UpdateManager::check_for_updates() {
    spdlog::info("Checking for updates (current: {})", impl_->current_version);

    // Download releases.yml.zst
    std::string index_key = fmt::format("{}/{}", impl_->app_id, constants::RELEASES_FILE_COMPRESSED);
    std::vector<uint8_t> compressed;
    auto rc = impl_->storage->get_object(index_key, compressed);
    if (rc == SURGE_NOT_FOUND) {
        spdlog::info("No release index found");
        return std::nullopt;
    }
    if (rc != SURGE_OK) {
        spdlog::error("Failed to download release index");
        return std::nullopt;
    }

    // Decompress and parse
    try {
        impl_->cached_index = releases::decompress_release_index(compressed);
        impl_->index_loaded = true;
    } catch (const std::exception& e) {
        spdlog::error("Failed to decompress/parse release index: {}", e.what());
        return std::nullopt;
    }

    // Find newer releases
    auto newer = releases::get_releases_newer_than(
        impl_->cached_index, impl_->current_version, impl_->channel);

    if (newer.empty()) {
        spdlog::info("Already up to date");
        return std::nullopt;
    }

    UpdateInfo info;
    info.available_releases = std::move(newer);
    info.latest_version = info.available_releases.back().version;

    // Check if delta path exists
    auto delta_chain = releases::get_delta_chain(
        impl_->cached_index, impl_->current_version, info.latest_version, impl_->channel);
    info.delta_available = !delta_chain.empty();

    // Calculate download size
    if (info.delta_available) {
        for (auto& rel : delta_chain) {
            info.download_size += (rel.delta_size > 0) ? rel.delta_size : rel.full_size;
        }
    } else if (!info.available_releases.empty()) {
        info.download_size = info.available_releases.back().full_size;
    }

    spdlog::info("Found {} updates available, latest: {}",
                  info.available_releases.size(), info.latest_version);
    return info;
}

int32_t UpdateManager::download_and_apply(const UpdateInfo& info,
                                            ProgressCallback progress) {
    if (info.available_releases.empty()) {
        spdlog::warn("No releases to apply");
        return SURGE_NOT_FOUND;
    }

    if (impl_->ctx.is_cancelled()) return SURGE_CANCELLED;

    auto& target = info.available_releases.back();
    spdlog::info("Downloading and applying update to {}", target.version);

    // Report check phase complete
    if (progress) {
        surge_progress p{};
        p.phase = SURGE_PHASE_CHECK;
        p.phase_percent = 100;
        p.total_percent = 5;
        progress(p);
    }

    if (impl_->ctx.is_cancelled()) return SURGE_CANCELLED;

    // Phase 2: Download
    auto packages_dir = impl_->install_dir / constants::PACKAGES_DIR;
    fs::create_directories(packages_dir);

    std::string package_filename = target.full_filename;
    std::string expected_sha256 = target.full_sha256;
    int64_t expected_size = target.full_size;

    std::string package_key = fmt::format("{}/{}", impl_->app_id, package_filename);
    auto dest_path = packages_dir / package_filename;

    auto download_progress = [&](int64_t done, int64_t total) {
        if (progress) {
            int pct = (total > 0) ? static_cast<int>(done * 100 / total) : 0;
            surge_progress p{};
            p.phase = SURGE_PHASE_DOWNLOAD;
            p.phase_percent = pct;
            p.total_percent = 5 + (pct * 60 / 100);
            p.bytes_done = done;
            p.bytes_total = total;
            progress(p);
        }
    };

    auto rc = impl_->storage->download_to_file(package_key, dest_path, download_progress);
    if (rc != SURGE_OK) {
        spdlog::error("Failed to download package: {}", package_filename);
        return rc;
    }

    if (impl_->ctx.is_cancelled()) {
        fs::remove(dest_path);
        return SURGE_CANCELLED;
    }

    // Phase 3: Verify SHA256
    spdlog::info("Verifying package checksum");
    auto actual_sha256 = crypto::sha256_hex_file(dest_path);
    if (!expected_sha256.empty() && actual_sha256 != expected_sha256) {
        spdlog::error("Checksum mismatch: expected={}, actual={}", expected_sha256, actual_sha256);
        fs::remove(dest_path);
        return SURGE_ERROR;
    }

    if (progress) {
        surge_progress p{};
        p.phase = SURGE_PHASE_VERIFY;
        p.phase_percent = 100;
        p.total_percent = 70;
        progress(p);
    }

    if (impl_->ctx.is_cancelled()) {
        fs::remove(dest_path);
        return SURGE_CANCELLED;
    }

    // Phase 4: Extract
    spdlog::info("Extracting package");
    auto app_dir_name = fmt::format("{}{}", constants::APP_DIR_PREFIX, target.version);
    auto app_dir = impl_->install_dir / app_dir_name;

    try {
        archive::ArchiveExtractor extractor(dest_path);
        extractor.extract_to(app_dir);
    } catch (const std::exception& e) {
        spdlog::error("Extraction error: {}", e.what());
        std::error_code ec;
        fs::remove_all(app_dir, ec);
        fs::remove(dest_path);
        return SURGE_ERROR;
    }

    if (progress) {
        surge_progress p{};
        p.phase = SURGE_PHASE_EXTRACT;
        p.phase_percent = 100;
        p.total_percent = 85;
        progress(p);
    }

    // Phase 5: Apply Deltas (skip for full packages)
    if (progress) {
        surge_progress p{};
        p.phase = SURGE_PHASE_APPLY_DELTA;
        p.phase_percent = 100;
        p.total_percent = 95;
        progress(p);
    }

    // Phase 6: Finalize
    spdlog::info("Finalizing update");
    fs::remove(dest_path);

    // Remove old app directories beyond retention limit
    constexpr int RETENTION_LIMIT = 1;
    struct VersionDir {
        fs::path path;
        std::string version;
    };
    std::vector<VersionDir> version_dirs;

    std::error_code ec;
    for (auto& entry : fs::directory_iterator(impl_->install_dir, ec)) {
        if (!entry.is_directory()) continue;
        auto dirname = entry.path().filename().string();
        if (!dirname.starts_with(constants::APP_DIR_PREFIX)) continue;
        auto ver = dirname.substr(std::strlen(constants::APP_DIR_PREFIX));
        if (ver == target.version) continue;
        version_dirs.push_back({entry.path(), ver});
    }

    std::sort(version_dirs.begin(), version_dirs.end(),
              [](const VersionDir& a, const VersionDir& b) {
                  return releases::compare_versions(a.version, b.version) < 0;
              });

    if (static_cast<int>(version_dirs.size()) > RETENTION_LIMIT) {
        int to_remove = static_cast<int>(version_dirs.size()) - RETENTION_LIMIT;
        for (int i = 0; i < to_remove; ++i) {
            spdlog::info("Removing old version directory: {}", version_dirs[i].path.string());
            fs::remove_all(version_dirs[i].path, ec);
        }
    }

    if (progress) {
        surge_progress p{};
        p.phase = SURGE_PHASE_FINALIZE;
        p.phase_percent = 100;
        p.total_percent = 100;
        progress(p);
    }

    spdlog::info("Update to {} completed successfully", target.version);
    return SURGE_OK;
}

const std::string& UpdateManager::current_version() const {
    return impl_->current_version;
}

const std::string& UpdateManager::app_id() const {
    return impl_->app_id;
}

const std::string& UpdateManager::channel() const {
    return impl_->channel;
}

const std::filesystem::path& UpdateManager::install_dir() const {
    return impl_->install_dir;
}

} // namespace surge::update
