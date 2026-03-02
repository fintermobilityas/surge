/**
 * @file update_manager.cpp
 * @brief Core update manager: check, download, verify, extract, apply deltas.
 */

#include "update/update_manager.hpp"
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

class UpdateManager::Impl {
public:
    std::shared_ptr<storage::IStorageBackend> storage;
    std::string app_id;
    std::string current_version;
    std::string channel;
    fs::path install_dir;
    surge_resource_budget budget{};
    std::stop_token stop_token;

    releases::ReleaseIndex cached_index;
    bool index_loaded = false;

    void report_progress(surge_progress_callback cb, void* user_data,
                         surge_progress_phase phase, int32_t phase_pct,
                         int64_t bytes_done = 0, int64_t bytes_total = 0) {
        if (!cb) return;

        // Phase weights: Check=5%, Download=60%, Verify=5%, Extract=15%, ApplyDelta=10%, Finalize=5%
        static constexpr int phase_starts[] = {0, 5, 65, 70, 85, 95};
        static constexpr int phase_widths[] = {5, 60, 5, 15, 10, 5};

        int idx = static_cast<int>(phase);
        int total_pct = phase_starts[idx] + (phase_widths[idx] * phase_pct / 100);

        surge_progress progress{};
        progress.phase = phase;
        progress.phase_percent = phase_pct;
        progress.total_percent = std::min(total_pct, 100);
        progress.bytes_done = bytes_done;
        progress.bytes_total = bytes_total;
        cb(&progress, user_data);
    }
};

UpdateManager::UpdateManager(std::shared_ptr<storage::IStorageBackend> storage,
                             const std::string& app_id,
                             const std::string& current_version,
                             const std::string& channel,
                             const std::filesystem::path& install_dir,
                             const surge_resource_budget& budget,
                             std::stop_token stop_token)
    : impl_(std::make_unique<Impl>())
{
    impl_->storage = std::move(storage);
    impl_->app_id = app_id;
    impl_->current_version = current_version;
    impl_->channel = channel;
    impl_->install_dir = install_dir;
    impl_->budget = budget;
    impl_->stop_token = std::move(stop_token);

    spdlog::debug("UpdateManager: app_id={}, current={}, channel={}, install_dir={}",
                   app_id, current_version, channel, install_dir.string());
}

UpdateManager::~UpdateManager() = default;
UpdateManager::UpdateManager(UpdateManager&&) noexcept = default;
UpdateManager& UpdateManager::operator=(UpdateManager&&) noexcept = default;

int32_t UpdateManager::check_for_updates(std::vector<releases::ReleaseEntry>& out_updates) {
    spdlog::info("Checking for updates (current: {})", impl_->current_version);

    // Download releases.yml.zst
    std::string index_key = fmt::format("{}/{}", impl_->app_id, constants::RELEASES_FILE_COMPRESSED);
    std::vector<uint8_t> compressed;
    auto rc = impl_->storage->get_object(index_key, compressed);
    if (rc == SURGE_NOT_FOUND) {
        spdlog::info("No release index found");
        return SURGE_NOT_FOUND;
    }
    if (rc != SURGE_OK) {
        spdlog::error("Failed to download release index");
        return rc;
    }

    // Decompress
    std::vector<uint8_t> yaml_data;
    try {
        yaml_data = releases::decompress_release_index(compressed);
    } catch (const std::exception& e) {
        spdlog::error("Failed to decompress release index: {}", e.what());
        return SURGE_ERROR;
    }

    // Parse
    try {
        impl_->cached_index = releases::parse_release_index(yaml_data);
        impl_->index_loaded = true;
    } catch (const std::exception& e) {
        spdlog::error("Failed to parse release index: {}", e.what());
        return SURGE_ERROR;
    }

    // Find newer releases
    out_updates = releases::get_releases_newer_than(
        impl_->cached_index, impl_->current_version, impl_->channel);

    if (out_updates.empty()) {
        spdlog::info("Already up to date");
        return SURGE_NOT_FOUND;
    }

    spdlog::info("Found {} updates available", out_updates.size());
    return SURGE_OK;
}

int32_t UpdateManager::download_and_apply(
    const releases::ReleaseEntry& target_release,
    surge_progress_callback progress_cb,
    void* user_data)
{
    spdlog::info("Downloading and applying update to {}", target_release.version);

    if (impl_->stop_token.stop_requested()) return SURGE_CANCELLED;

    // Phase 1: Check (5%)
    impl_->report_progress(progress_cb, user_data, SURGE_PHASE_CHECK, 100);

    if (impl_->stop_token.stop_requested()) return SURGE_CANCELLED;

    // Phase 2: Download (60%)
    auto packages_dir = impl_->install_dir / constants::PACKAGES_DIR;
    fs::create_directories(packages_dir);

    // Determine whether to use delta or full package
    bool use_delta = target_release.is_delta
                     && !target_release.delta.filename.empty()
                     && target_release.delta.base_version == impl_->current_version;

    std::string package_filename;
    std::string expected_sha256;
    int64_t expected_size;

    if (use_delta) {
        package_filename = target_release.delta.filename;
        expected_sha256 = target_release.delta.sha256;
        expected_size = target_release.delta.size;
        spdlog::info("Using delta package: {}", package_filename);
    } else {
        package_filename = target_release.full.filename;
        expected_sha256 = target_release.full.sha256;
        expected_size = target_release.full.size;
        spdlog::info("Using full package: {}", package_filename);
    }

    std::string package_key = fmt::format("{}/{}", impl_->app_id, package_filename);
    auto dest_path = packages_dir / package_filename;

    auto download_progress = [&](int64_t done, int64_t total) {
        int pct = (total > 0) ? static_cast<int>(done * 100 / total) : 0;
        impl_->report_progress(progress_cb, user_data, SURGE_PHASE_DOWNLOAD, pct, done, total);
    };

    auto rc = impl_->storage->download_to_file(package_key, dest_path, download_progress);
    if (rc != SURGE_OK) {
        spdlog::error("Failed to download package: {}", package_filename);
        return rc;
    }
    impl_->report_progress(progress_cb, user_data, SURGE_PHASE_DOWNLOAD, 100, expected_size, expected_size);

    if (impl_->stop_token.stop_requested()) {
        fs::remove(dest_path);
        return SURGE_CANCELLED;
    }

    // Phase 3: Verify SHA256 (5%)
    spdlog::info("Verifying package checksum");
    auto actual_sha256 = crypto::sha256_hex_file(dest_path);
    if (actual_sha256 != expected_sha256) {
        spdlog::error("Checksum mismatch: expected={}, actual={}", expected_sha256, actual_sha256);
        fs::remove(dest_path);
        return SURGE_ERROR;
    }
    impl_->report_progress(progress_cb, user_data, SURGE_PHASE_VERIFY, 100);

    if (impl_->stop_token.stop_requested()) {
        fs::remove(dest_path);
        return SURGE_CANCELLED;
    }

    // Phase 4: Extract (15%)
    spdlog::info("Extracting package");
    auto app_dir_name = fmt::format("{}{}", constants::APP_DIR_PREFIX, target_release.version);
    auto app_dir = impl_->install_dir / app_dir_name;

    try {
        archive::ArchiveExtractor extractor(dest_path);

        auto extract_progress = [&](int64_t items_done, int64_t items_total, const std::string& file) {
            int pct = (items_total > 0) ? static_cast<int>(items_done * 100 / items_total) : 0;
            impl_->report_progress(progress_cb, user_data, SURGE_PHASE_EXTRACT, pct);
        };

        rc = extractor.extract_all(app_dir, extract_progress);
        if (rc != SURGE_OK) {
            spdlog::error("Extraction failed");
            fs::remove_all(app_dir);
            fs::remove(dest_path);
            return rc;
        }
    } catch (const std::exception& e) {
        spdlog::error("Extraction error: {}", e.what());
        fs::remove_all(app_dir);
        fs::remove(dest_path);
        return SURGE_ERROR;
    }
    impl_->report_progress(progress_cb, user_data, SURGE_PHASE_EXTRACT, 100);

    if (impl_->stop_token.stop_requested()) {
        fs::remove_all(app_dir);
        fs::remove(dest_path);
        return SURGE_CANCELLED;
    }

    // Phase 5: Apply Deltas if needed (10%)
    if (use_delta) {
        spdlog::info("Applying binary deltas from base version {}", impl_->current_version);
        auto base_dir_name = fmt::format("{}{}", constants::APP_DIR_PREFIX, impl_->current_version);
        auto base_dir = impl_->install_dir / base_dir_name;

        if (!fs::exists(base_dir)) {
            spdlog::error("Base version directory not found: {}", base_dir.string());
            fs::remove_all(app_dir);
            fs::remove(dest_path);
            return SURGE_ERROR;
        }

        int64_t delta_files_done = 0;
        int64_t delta_files_total = 0;

        // Count delta files
        for (auto& entry : fs::recursive_directory_iterator(app_dir)) {
            if (entry.is_regular_file() && entry.path().extension() == ".bsdiff") {
                delta_files_total++;
            }
        }

        // Apply each delta
        std::error_code ec;
        for (auto& entry : fs::recursive_directory_iterator(app_dir)) {
            if (!entry.is_regular_file()) continue;
            if (entry.path().extension() != ".bsdiff") continue;

            if (impl_->stop_token.stop_requested()) {
                fs::remove_all(app_dir);
                fs::remove(dest_path);
                return SURGE_CANCELLED;
            }

            auto relative = fs::relative(entry.path(), app_dir, ec);
            if (ec) continue;

            // The target file path (strip .bsdiff extension)
            auto target_file = app_dir / relative.stem();
            auto base_file = base_dir / relative.stem();

            if (!fs::exists(base_file)) {
                spdlog::error("Base file not found for delta: {}", base_file.string());
                fs::remove_all(app_dir);
                fs::remove(dest_path);
                return SURGE_ERROR;
            }

            // Read base file
            std::ifstream base_stream(base_file, std::ios::binary | std::ios::ate);
            auto base_size = base_stream.tellg();
            base_stream.seekg(0);
            std::vector<uint8_t> base_data(static_cast<size_t>(base_size));
            base_stream.read(reinterpret_cast<char*>(base_data.data()),
                              static_cast<std::streamsize>(base_size));

            // Read patch file
            std::ifstream patch_stream(entry.path(), std::ios::binary | std::ios::ate);
            auto patch_size = patch_stream.tellg();
            patch_stream.seekg(0);
            std::vector<uint8_t> patch_data(static_cast<size_t>(patch_size));
            patch_stream.read(reinterpret_cast<char*>(patch_data.data()),
                               static_cast<std::streamsize>(patch_size));

            // Apply patch
            auto result = diff::bspatch_apply(base_data, patch_data);
            if (!result.success) {
                spdlog::error("Failed to apply delta for: {}", relative.string());
                fs::remove_all(app_dir);
                fs::remove(dest_path);
                return SURGE_ERROR;
            }

            // Write the patched file
            std::ofstream out_file(target_file, std::ios::binary | std::ios::trunc);
            out_file.write(reinterpret_cast<const char*>(result.new_data.data()),
                           static_cast<std::streamsize>(result.new_data.size()));
            out_file.close();

            // Remove the .bsdiff file
            fs::remove(entry.path());

            delta_files_done++;
            int pct = (delta_files_total > 0)
                ? static_cast<int>(delta_files_done * 100 / delta_files_total)
                : 100;
            impl_->report_progress(progress_cb, user_data, SURGE_PHASE_APPLY_DELTA, pct);
        }
    }
    impl_->report_progress(progress_cb, user_data, SURGE_PHASE_APPLY_DELTA, 100);

    // Phase 6: Finalize (5%)
    spdlog::info("Finalizing update");

    // Clean up downloaded package (unless it's a genesis release to retain)
    if (!target_release.is_genesis) {
        fs::remove(dest_path);
    }

    // Remove old app directories beyond retention limit
    constexpr int RETENTION_LIMIT = 1;
    struct VersionDir {
        fs::path path;
        std::string version;
    };
    std::vector<VersionDir> version_dirs;

    for (auto& entry : fs::directory_iterator(impl_->install_dir)) {
        if (!entry.is_directory()) continue;
        auto dirname = entry.path().filename().string();
        if (!dirname.starts_with(constants::APP_DIR_PREFIX)) continue;
        auto ver = dirname.substr(std::strlen(constants::APP_DIR_PREFIX));
        if (ver == target_release.version) continue;
        version_dirs.push_back({entry.path(), ver});
    }

    // Sort by version ascending
    std::sort(version_dirs.begin(), version_dirs.end(),
              [](const VersionDir& a, const VersionDir& b) {
                  return releases::compare_versions(a.version, b.version) < 0;
              });

    // Remove oldest directories exceeding retention limit
    if (static_cast<int>(version_dirs.size()) > RETENTION_LIMIT) {
        int to_remove = static_cast<int>(version_dirs.size()) - RETENTION_LIMIT;
        for (int i = 0; i < to_remove; ++i) {
            spdlog::info("Removing old version directory: {}", version_dirs[i].path.string());
            std::error_code ec;
            fs::remove_all(version_dirs[i].path, ec);
            if (ec) {
                spdlog::warn("Failed to remove {}: {}", version_dirs[i].path.string(), ec.message());
            }
        }
    }

    impl_->report_progress(progress_cb, user_data, SURGE_PHASE_FINALIZE, 100);

    spdlog::info("Update to {} completed successfully", target_release.version);
    return SURGE_OK;
}

std::string UpdateManager::current_version() const {
    return impl_->current_version;
}

std::string UpdateManager::channel() const {
    return impl_->channel;
}

} // namespace surge::update
