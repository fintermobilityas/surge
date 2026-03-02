/**
 * @file update_manager.hpp
 * @brief High-level update manager: check, download, verify, and apply updates.
 */

#pragma once

#include "surge/surge_api.h"

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace surge {
class Context;
}

namespace surge::releases {
struct ReleaseEntry;
struct ReleaseIndex;
}  // namespace surge::releases

namespace surge::update {

/** Information about available updates returned by check_for_updates(). */
struct UpdateInfo {
    /** All releases that are newer than the current version, oldest-first. */
    std::vector<releases::ReleaseEntry> available_releases;

    /** The latest version available. */
    std::string latest_version;

    /** True if a delta-update path exists from current to latest. */
    bool delta_available = false;

    /** Total download size in bytes (delta if available, otherwise full). */
    int64_t download_size = 0;
};

/** Progress callback for update operations. */
using ProgressCallback = std::function<void(const surge_progress&)>;

/**
 * Manages the full update lifecycle for a single application.
 *
 * Typical flow:
 * @code
 *   UpdateManager mgr(ctx, "com.example.app", "1.0.0", "stable", "/opt/myapp");
 *   auto info = mgr.check_for_updates();
 *   if (info) {
 *       mgr.download_and_apply(*info, [](const surge_progress& p) {
 *           std::cout << p.total_percent << "%" << std::endl;
 *       });
 *   }
 * @endcode
 */
class UpdateManager {
public:
    /**
     * Construct an update manager.
     * @param ctx             Surge context (must outlive the manager).
     * @param app_id          Application identifier.
     * @param current_version Currently installed version (semver).
     * @param channel         Release channel to track.
     * @param install_dir     Root installation directory.
     */
    UpdateManager(Context& ctx, std::string app_id, std::string current_version, std::string channel,
                  std::filesystem::path install_dir);
    ~UpdateManager();

    UpdateManager(const UpdateManager&) = delete;
    UpdateManager& operator=(const UpdateManager&) = delete;

    /**
     * Check for available updates by fetching and parsing the remote
     * release index.
     * @return UpdateInfo if updates are available, std::nullopt if up-to-date.
     */
    std::optional<UpdateInfo> check_for_updates();

    /**
     * Download and apply an update.
     *
     * Steps performed:
     *   1. Download packages (delta or full) from storage.
     *   2. Verify SHA-256 checksums.
     *   3. Extract / apply delta patches.
     *   4. Write the new app directory.
     *   5. Update local metadata.
     *
     * @param info     Update info from check_for_updates().
     * @param progress Optional progress callback.
     * @return 0 on success, negative error code on failure.
     */
    int32_t download_and_apply(const UpdateInfo& info, ProgressCallback progress = nullptr);

    /** Return the currently installed version. */
    const std::string& current_version() const;

    /** Return the application identifier. */
    const std::string& app_id() const;

    /** Return the tracked release channel. */
    const std::string& channel() const;

    /** Return the installation directory. */
    const std::filesystem::path& install_dir() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace surge::update
