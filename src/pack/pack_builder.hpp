/**
 * @file pack_builder.hpp
 * @brief Builds full and delta release packages from build artifacts.
 */

#pragma once

#include "surge/surge_api.h"

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace surge {
class Context;
}

namespace surge::releases {
struct ReleaseEntry;
struct ReleaseIndex;
}  // namespace surge::releases

namespace surge::pack {

/** Metadata about a built package. */
struct PackageArtifact {
    std::filesystem::path path;
    std::string filename;
    int64_t size = 0;
    std::string sha256;
    bool is_delta = false;
    std::string from_version;  // only for delta packages
};

/** Progress callback for pack operations. */
using ProgressCallback = std::function<void(const surge_progress&)>;

/**
 * Builds release packages (full archives + delta patches) from application
 * build artifacts and manages pushing them to storage.
 *
 * Usage:
 * @code
 *   PackBuilder builder(ctx, "surge.yml", "com.example.app", "linux-x64",
 *                       "2.0.0", "/build/output");
 *   builder.build([](const surge_progress& p) { ... });
 *   builder.push("stable", [](const surge_progress& p) { ... });
 * @endcode
 */
class PackBuilder {
public:
    /**
     * Construct a pack builder.
     * @param ctx           Surge context with storage configured.
     * @param manifest_path Path to the surge.yml manifest.
     * @param app_id        Application identifier.
     * @param rid           Runtime identifier (e.g. "linux-x64").
     * @param version       Version string for this release.
     * @param artifacts_dir Directory containing build artifacts.
     */
    PackBuilder(Context& ctx, std::filesystem::path manifest_path, std::string app_id, std::string rid,
                std::string version, std::filesystem::path artifacts_dir);
    ~PackBuilder();

    PackBuilder(const PackBuilder&) = delete;
    PackBuilder& operator=(const PackBuilder&) = delete;

    /**
     * Build full and delta packages.
     *
     * Steps:
     *   1. Read the manifest and validate configuration.
     *   2. Create the full archive (tar.zst) with a manifest.yml entry.
     *   3. Fetch the previous release from storage (if any).
     *   4. Create delta patches between previous and current.
     *   5. Compute SHA-256 checksums for all packages.
     *
     * @param progress Optional progress callback.
     * @return 0 on success, negative error code on failure.
     */
    int32_t build(ProgressCallback progress = nullptr);

    /**
     * Push built packages to the configured storage backend and update the
     * remote release index.
     *
     * @param channel  Target release channel.
     * @param progress Optional progress callback.
     * @return 0 on success, negative error code on failure.
     */
    int32_t push(const std::string& channel, ProgressCallback progress = nullptr);

    /** Return the list of packages produced by the last build(). */
    const std::vector<PackageArtifact>& artifacts() const;

    /** Return the version being built. */
    const std::string& version() const;

    /** Return the application identifier. */
    const std::string& app_id() const;

    /** Return the runtime identifier. */
    const std::string& rid() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace surge::pack
