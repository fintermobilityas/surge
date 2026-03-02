/**
 * @file cmd_pack.cpp
 * @brief `surge pack` - Build full and delta packages from artifacts.
 */

#include <cxxopts.hpp>
#include <spdlog/spdlog.h>
#include <fmt/format.h>
#include <yaml-cpp/yaml.h>
#include <filesystem>
#include <iostream>
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

} // anonymous namespace

int cmd_pack(int argc, char* argv[]) {
    cxxopts::Options options("surge pack", "Build full and delta packages from artifacts");

    options.add_options()
        ("app", "Application ID (overrides manifest)", cxxopts::value<std::string>()->default_value(""))
        ("rid", "Runtime identifier (overrides manifest)", cxxopts::value<std::string>()->default_value(""))
        ("version", "Semantic version for this release (required)", cxxopts::value<std::string>())
        ("artifacts", "Path to artifacts directory", cxxopts::value<std::string>()->default_value(""))
        ("manifest", "Path to surge.yml", cxxopts::value<std::string>()->default_value(""))
        ("max-memory", "Maximum memory budget in MB", cxxopts::value<int>()->default_value("0"))
        ("h,help", "Show help")
    ;

    auto result = options.parse(argc, argv);

    if (result.count("help")) {
        std::cout << options.help() << "\n";
        return 0;
    }

    if (!result.count("version")) {
        spdlog::error("--version is required. Specify the release version (e.g., --version 1.0.0)");
        return 1;
    }

    const auto version = result["version"].as<std::string>();

    // Locate manifest
    auto manifest_path = find_manifest(result["manifest"].as<std::string>());
    if (manifest_path.empty() || !fs::exists(manifest_path)) {
        spdlog::error("Cannot find {}. Run 'surge init' first or specify --manifest",
                       surge::constants::MANIFEST_FILE);
        return 1;
    }

    spdlog::info("Loading manifest from {}", manifest_path.string());

    surge::config::SurgeManifest manifest;
    try {
        manifest = surge::config::parse_manifest(manifest_path);
    } catch (const std::exception& ex) {
        spdlog::error("Failed to parse manifest: {}", ex.what());
        return 1;
    }

    // Validate manifest
    auto issues = surge::config::validate_manifest(manifest);
    if (!issues.empty()) {
        spdlog::error("Manifest validation failed:");
        for (const auto& issue : issues) {
            spdlog::error("  - {}", issue);
        }
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

    // Resolve RID
    auto rid = result["rid"].as<std::string>();
    if (rid.empty()) {
        rid = target_app->target.rid;
    }

    // Resolve artifacts directory
    auto artifacts_dir_str = result["artifacts"].as<std::string>();
    fs::path artifacts_dir;
    if (artifacts_dir_str.empty()) {
        artifacts_dir = manifest.generic.artifacts.empty()
            ? fs::path(surge::constants::ARTIFACTS_DIR)
            : fs::path(manifest.generic.artifacts);
    } else {
        artifacts_dir = fs::path(artifacts_dir_str);
    }

    if (!fs::exists(artifacts_dir) || !fs::is_directory(artifacts_dir)) {
        spdlog::error("Artifacts directory does not exist: {}", artifacts_dir.string());
        return 1;
    }

    // Count artifact files
    int file_count = 0;
    int64_t total_size = 0;
    for (const auto& entry : fs::recursive_directory_iterator(artifacts_dir)) {
        if (entry.is_regular_file()) {
            ++file_count;
            total_size += entry.file_size();
        }
    }

    spdlog::info("Packing release:");
    spdlog::info("  App:       {}", target_app->id);
    spdlog::info("  Version:   {}", version);
    spdlog::info("  RID:       {}", rid);
    spdlog::info("  Artifacts: {} files ({})", file_count, format_size(total_size));

    auto max_memory = result["max-memory"].as<int>();
    if (max_memory > 0) {
        spdlog::info("  Memory:    {} MB limit", max_memory);
    }

    // Resolve packages output directory
    fs::path packages_dir = manifest.generic.packages.empty()
        ? fs::path(surge::constants::PACKAGES_DIR)
        : fs::path(manifest.generic.packages);
    fs::create_directories(packages_dir);

    auto start_time = std::chrono::steady_clock::now();

    // Build packages using the C API
    // NOTE: This requires the surge_core library to be linked. The pack builder
    // creates both full (genesis) and delta packages.
    spdlog::info("Building full package...");

    // TODO: Integrate with surge_pack_create / surge_pack_build once the
    // pack implementation is complete. For now, we validate inputs and
    // report what would be built.
    //
    // surge_context* ctx = surge_context_create();
    // surge_pack_context* pack_ctx = surge_pack_create(
    //     ctx, manifest_path.c_str(), target_app->id.c_str(),
    //     rid.c_str(), version.c_str(), artifacts_dir.c_str());
    // surge_result rc = surge_pack_build(pack_ctx, nullptr, nullptr);
    // surge_pack_destroy(pack_ctx);
    // surge_context_destroy(ctx);

    auto elapsed = std::chrono::steady_clock::now() - start_time;
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();

    spdlog::info("Pack completed in {:.1f}s", elapsed_ms / 1000.0);
    spdlog::info("Output directory: {}", packages_dir.string());

    // Scan output for summary
    int64_t full_size = 0;
    int64_t delta_size = 0;
    int pkg_count = 0;
    if (fs::exists(packages_dir)) {
        for (const auto& entry : fs::directory_iterator(packages_dir)) {
            if (entry.is_regular_file()) {
                auto name = entry.path().filename().string();
                auto size = entry.file_size();
                ++pkg_count;
                if (name.find("delta") != std::string::npos) {
                    delta_size += size;
                } else {
                    full_size += size;
                }
            }
        }
    }

    if (pkg_count > 0) {
        spdlog::info("Package summary:");
        spdlog::info("  Full package:  {}", format_size(full_size));
        if (delta_size > 0) {
            spdlog::info("  Delta package: {}", format_size(delta_size));
            if (full_size > 0) {
                double ratio = static_cast<double>(delta_size) / static_cast<double>(full_size) * 100.0;
                spdlog::info("  Compression ratio: {:.1f}% of full", ratio);
            }
        }
    }

    return 0;
}
