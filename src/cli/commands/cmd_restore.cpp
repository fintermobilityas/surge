/**
 * @file cmd_restore.cpp
 * @brief `surge restore` - Reconstruct a full package from delta chain.
 */

#include <cxxopts.hpp>
#include <spdlog/spdlog.h>
#include <fmt/format.h>
#include <yaml-cpp/yaml.h>
#include <filesystem>
#include <iostream>
#include <vector>
#include <chrono>
#include "config/constants.hpp"
#include "config/manifest.hpp"

namespace fs = std::filesystem;

namespace {

fs::path find_manifest(const std::string& path_override) {
    if (!path_override.empty()) {
        return fs::path(path_override);
    }
    auto candidate = fs::path(surge::constants::SURGE_DIR) / surge::constants::MANIFEST_FILE;
    if (fs::exists(candidate)) {
        return candidate;
    }
    return {};
}

std::string format_size(int64_t bytes) {
    if (bytes < 1024) return fmt::format("{} B", bytes);
    if (bytes < 1024 * 1024) return fmt::format("{:.1f} KB", bytes / 1024.0);
    if (bytes < 1024 * 1024 * 1024) return fmt::format("{:.1f} MB", bytes / (1024.0 * 1024.0));
    return fmt::format("{:.2f} GB", bytes / (1024.0 * 1024.0 * 1024.0));
}

struct DeltaChainEntry {
    std::string version;
    std::string key;
    bool is_genesis;
};

} // anonymous namespace

int cmd_restore(int argc, char* argv[]) {
    cxxopts::Options options("surge restore", "Reconstruct a full package from delta chain");

    options.add_options()
        ("app", "Application ID", cxxopts::value<std::string>()->default_value(""))
        ("version", "Target version to restore (required)", cxxopts::value<std::string>())
        ("output", "Output directory for restored package", cxxopts::value<std::string>()->default_value("restored"))
        ("manifest", "Path to surge.yml", cxxopts::value<std::string>()->default_value(""))
        ("h,help", "Show help")
    ;

    auto result = options.parse(argc, argv);

    if (result.count("help")) {
        std::cout << options.help() << "\n";
        return 0;
    }

    if (!result.count("version")) {
        spdlog::error("--version is required. Specify the version to restore.");
        return 1;
    }

    const auto version = result["version"].as<std::string>();
    const auto output_dir = fs::path(result["output"].as<std::string>());

    // Locate manifest
    auto manifest_path = find_manifest(result["manifest"].as<std::string>());
    if (manifest_path.empty() || !fs::exists(manifest_path)) {
        spdlog::error("Cannot find {}. Run 'surge init' first or specify --manifest",
                       surge::constants::MANIFEST_FILE);
        return 1;
    }

    surge::config::SurgeManifest manifest;
    try {
        manifest = surge::config::parse_manifest(manifest_path);
    } catch (const std::exception& ex) {
        spdlog::error("Failed to parse manifest: {}", ex.what());
        return 1;
    }

    if (manifest.apps.empty()) {
        spdlog::error("No apps defined in manifest");
        return 1;
    }

    // Resolve app
    const auto app_id_override = result["app"].as<std::string>();
    const surge::config::AppConfig* target_app = nullptr;

    if (app_id_override.empty()) {
        target_app = &manifest.apps.front();
    } else {
        for (const auto& app : manifest.apps) {
            if (app.id == app_id_override) {
                target_app = &app;
                break;
            }
        }
        if (!target_app) {
            spdlog::error("App '{}' not found in manifest", app_id_override);
            return 1;
        }
    }

    spdlog::info("Restoring release:");
    spdlog::info("  App:     {}", target_app->id);
    spdlog::info("  Version: {}", version);
    spdlog::info("  Output:  {}", fs::absolute(output_dir).string());

    auto start_time = std::chrono::steady_clock::now();

    // Step 1: Download and parse release index
    spdlog::info("Downloading release index...");

    // TODO: Integrate with storage backend
    // auto storage = surge::storage::create_storage_backend(storage_config);
    // std::vector<uint8_t> index_data;
    // storage->get_object(index_key, index_data);

    // Step 2: Build delta chain from genesis to target version
    spdlog::info("Building delta chain to version {}...", version);

    // TODO: Walk the release index backwards from target version to find
    // the genesis package, then collect all delta packages in order.
    //
    // std::vector<DeltaChainEntry> chain;
    // Build chain: genesis -> v1 delta -> v2 delta -> ... -> target version
    //
    // The chain is ordered so that:
    //   chain[0] = genesis (full package)
    //   chain[1..n] = deltas in application order

    // Step 3: Download genesis package
    spdlog::info("Downloading genesis package...");

    // TODO: Download the genesis full package
    // storage->download_to_file(genesis_key, temp_dir / "genesis.pkg",
    //     [](int64_t done, int64_t total) {
    //         spdlog::info("  Download: {}/{}", format_size(done), format_size(total));
    //     });

    // Step 4: Download and apply delta chain
    // TODO: For each delta in the chain, download and apply using bspatch
    //
    // for (size_t i = 1; i < chain.size(); ++i) {
    //     spdlog::info("Applying delta {}/{}: {} -> {}",
    //                  i, chain.size() - 1, chain[i-1].version, chain[i].version);
    //
    //     // Download delta
    //     storage->download_to_file(chain[i].key, delta_path, progress);
    //
    //     // Apply delta using bspatch
    //     surge_bspatch_ctx patch_ctx = {};
    //     patch_ctx.older = current_data;
    //     patch_ctx.older_size = current_size;
    //     patch_ctx.patch = delta_data;
    //     patch_ctx.patch_size = delta_size;
    //     surge_bspatch(&patch_ctx);
    //
    //     // Current becomes the patched result
    //     current_data = patch_ctx.newer;
    //     current_size = patch_ctx.newer_size;
    // }

    // Step 5: Write reconstructed package to output directory
    fs::create_directories(output_dir);

    spdlog::info("Writing restored package to {}", output_dir.string());

    // TODO: Extract the reconstructed archive to the output directory

    auto elapsed = std::chrono::steady_clock::now() - start_time;
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();

    spdlog::info("Restore completed in {:.1f}s", elapsed_ms / 1000.0);
    spdlog::info("Output: {}", fs::absolute(output_dir).string());

    return 0;
}
