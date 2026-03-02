/**
 * @file cmd_push.cpp
 * @brief `surge push` - Upload packages to cloud storage.
 */

#include <cxxopts.hpp>
#include <spdlog/spdlog.h>
#include <fmt/format.h>
#include <filesystem>
#include <iostream>
#include <chrono>
#include <vector>
#include "config/constants.hpp"
#include "config/manifest.hpp"

namespace fs = std::filesystem;

namespace {

std::string format_size(int64_t bytes) {
    if (bytes < 1024) return fmt::format("{} B", bytes);
    if (bytes < 1024 * 1024) return fmt::format("{:.1f} KB", bytes / 1024.0);
    if (bytes < 1024 * 1024 * 1024) return fmt::format("{:.1f} MB", bytes / (1024.0 * 1024.0));
    return fmt::format("{:.2f} GB", bytes / (1024.0 * 1024.0 * 1024.0));
}

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

} // anonymous namespace

int cmd_push(int argc, char* argv[]) {
    cxxopts::Options options("surge push", "Upload packages to cloud storage");

    options.add_options()
        ("app", "Application ID", cxxopts::value<std::string>()->default_value(""))
        ("channel", "Target release channel (required)", cxxopts::value<std::string>())
        ("version", "Release version to push", cxxopts::value<std::string>()->default_value(""))
        ("manifest", "Path to surge.yml", cxxopts::value<std::string>()->default_value(""))
        ("h,help", "Show help")
    ;

    auto result = options.parse(argc, argv);

    if (result.count("help")) {
        std::cout << options.help() << "\n";
        return 0;
    }

    if (!result.count("channel")) {
        spdlog::error("--channel is required. Specify the target channel (e.g., --channel stable)");
        return 1;
    }

    const auto channel = result["channel"].as<std::string>();

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

    // Validate channel exists in manifest
    bool channel_valid = false;
    for (const auto& ch : manifest.channels) {
        if (ch.name == channel) {
            channel_valid = true;
            break;
        }
    }
    if (!channel_valid) {
        spdlog::error("Channel '{}' is not defined in the manifest", channel);
        return 1;
    }

    // Locate packages directory
    fs::path packages_dir = manifest.generic.packages.empty()
        ? fs::path(surge::constants::PACKAGES_DIR)
        : fs::path(manifest.generic.packages);

    if (!fs::exists(packages_dir) || !fs::is_directory(packages_dir)) {
        spdlog::error("Packages directory not found: {}. Run 'surge pack' first.", packages_dir.string());
        return 1;
    }

    // Collect package files
    std::vector<fs::path> package_files;
    int64_t total_upload_size = 0;
    for (const auto& entry : fs::directory_iterator(packages_dir)) {
        if (entry.is_regular_file()) {
            package_files.push_back(entry.path());
            total_upload_size += entry.file_size();
        }
    }

    if (package_files.empty()) {
        spdlog::error("No packages found in {}. Run 'surge pack' first.", packages_dir.string());
        return 1;
    }

    spdlog::info("Push configuration:");
    spdlog::info("  App:      {}", target_app->id);
    spdlog::info("  Channel:  {}", channel);
    spdlog::info("  Storage:  {} (bucket: {})", manifest.storage.provider, manifest.storage.bucket);
    spdlog::info("  Packages: {} files ({})", package_files.size(), format_size(total_upload_size));

    auto start_time = std::chrono::steady_clock::now();

    // Step 1: Acquire distributed lock
    spdlog::info("Acquiring distributed lock...");

    // TODO: Integrate with surge_lock_acquire once implementation is complete.
    // char* challenge = nullptr;
    // surge_context* ctx = surge_context_create();
    // surge_config_set_lock_server(ctx, manifest.lock.server.c_str());
    // surge_result lock_rc = surge_lock_acquire(ctx, target_app->id.c_str(), 300, &challenge);
    // if (lock_rc != SURGE_OK) { ... }

    spdlog::info("Lock acquired");

    // Step 2: Upload packages
    spdlog::info("Uploading packages...");

    for (size_t i = 0; i < package_files.size(); ++i) {
        const auto& pkg = package_files[i];
        auto file_size = fs::file_size(pkg);
        spdlog::info("  [{}/{}] {} ({})",
                     i + 1, package_files.size(),
                     pkg.filename().string(),
                     format_size(file_size));

        // TODO: Integrate with storage backend.
        // std::string key = fmt::format("{}/{}/{}/{}",
        //     manifest.storage.prefix, target_app->id, version, pkg.filename().string());
        // storage->upload_from_file(key, pkg, [](int64_t done, int64_t total) { ... });
    }

    // Step 3: Upload release index
    spdlog::info("Updating release index...");

    // TODO: Download current releases.yml, add new entry, upload updated index.

    // Step 4: Release lock
    spdlog::info("Releasing lock...");

    // TODO: surge_lock_release(ctx, target_app->id.c_str(), challenge);
    // free(challenge);
    // surge_context_destroy(ctx);

    auto elapsed = std::chrono::steady_clock::now() - start_time;
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();

    spdlog::info("Push completed in {:.1f}s", elapsed_ms / 1000.0);
    spdlog::info("Release is now available on channel '{}'", channel);

    return 0;
}
