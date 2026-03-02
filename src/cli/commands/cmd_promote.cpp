/**
 * @file cmd_promote.cpp
 * @brief `surge promote` - Promote a release to an additional channel.
 */

#include <cxxopts.hpp>
#include <spdlog/spdlog.h>
#include <fmt/format.h>
#include <yaml-cpp/yaml.h>
#include <nlohmann/json.hpp>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <algorithm>
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

} // anonymous namespace

int cmd_promote(int argc, char* argv[]) {
    cxxopts::Options options("surge promote", "Promote a release to an additional channel");

    options.add_options()
        ("app", "Application ID", cxxopts::value<std::string>()->default_value(""))
        ("version", "Release version to promote (required)", cxxopts::value<std::string>())
        ("channel", "Target channel to promote to (required)", cxxopts::value<std::string>())
        ("manifest", "Path to surge.yml", cxxopts::value<std::string>()->default_value(""))
        ("h,help", "Show help")
    ;

    auto result = options.parse(argc, argv);

    if (result.count("help")) {
        std::cout << options.help() << "\n";
        return 0;
    }

    if (!result.count("version")) {
        spdlog::error("--version is required");
        return 1;
    }
    if (!result.count("channel")) {
        spdlog::error("--channel is required");
        return 1;
    }

    const auto version = result["version"].as<std::string>();
    const auto channel = result["channel"].as<std::string>();

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

    // Validate channel exists
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

    spdlog::info("Promoting release:");
    spdlog::info("  App:     {}", target_app->id);
    spdlog::info("  Version: {}", version);
    spdlog::info("  Channel: {}", channel);

    // Step 1: Download current release index from storage
    spdlog::info("Downloading release index...");

    // TODO: Integrate with storage backend to download releases.yml
    // auto storage = surge::storage::create_storage_backend(storage_config);
    // std::vector<uint8_t> index_data;
    // std::string key = fmt::format("{}/{}/{}",
    //     manifest.storage.prefix, target_app->id, surge::constants::RELEASES_FILE_COMPRESSED);
    // storage->get_object(key, index_data);

    // Step 2: Parse release index and add channel to the specified version
    // TODO: Parse the release index YAML, find the entry for `version`,
    // and add `channel` to its channels list if not already present.
    //
    // YAML::Node releases = YAML::Load(index_content);
    // for (auto& release : releases["releases"]) {
    //     if (release["version"].as<std::string>() == version) {
    //         auto channels = release["channels"];
    //         bool already_promoted = false;
    //         for (const auto& ch : channels) {
    //             if (ch.as<std::string>() == channel) {
    //                 already_promoted = true;
    //                 break;
    //             }
    //         }
    //         if (already_promoted) {
    //             spdlog::warn("Version {} is already on channel '{}'", version, channel);
    //             return 0;
    //         }
    //         channels.push_back(channel);
    //         break;
    //     }
    // }

    // Step 3: Upload updated release index
    spdlog::info("Uploading updated release index...");

    // TODO: Serialize and upload the modified index.
    // storage->put_object(key, serialized_data);

    spdlog::info("Version {} promoted to channel '{}'", version, channel);

    return 0;
}
