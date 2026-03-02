/**
 * @file cmd_list.cpp
 * @brief `surge list` - List releases and their channels.
 */

#include <cxxopts.hpp>
#include <spdlog/spdlog.h>
#include <fmt/format.h>
#include <nlohmann/json.hpp>
#include <yaml-cpp/yaml.h>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>
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

std::string format_size(int64_t bytes) {
    if (bytes < 1024) return fmt::format("{} B", bytes);
    if (bytes < 1024 * 1024) return fmt::format("{:.1f} KB", bytes / 1024.0);
    if (bytes < 1024 * 1024 * 1024) return fmt::format("{:.1f} MB", bytes / (1024.0 * 1024.0));
    return fmt::format("{:.2f} GB", bytes / (1024.0 * 1024.0 * 1024.0));
}

struct ReleaseEntry {
    std::string version;
    std::vector<std::string> channels;
    int64_t full_size = 0;
    int64_t delta_size = 0;
    std::string created;
    std::string notes;
    bool is_genesis = false;
};

void print_table(const std::vector<ReleaseEntry>& releases) {
    // Column headers
    const std::string h_ver     = "Version";
    const std::string h_chan    = "Channels";
    const std::string h_full   = "Full Size";
    const std::string h_delta  = "Delta Size";
    const std::string h_date   = "Created";
    const std::string h_notes  = "Notes";

    // Calculate column widths
    size_t w_ver   = h_ver.size();
    size_t w_chan  = h_chan.size();
    size_t w_full  = h_full.size();
    size_t w_delta = h_delta.size();
    size_t w_date  = h_date.size();
    size_t w_notes = h_notes.size();

    for (const auto& r : releases) {
        w_ver = std::max(w_ver, r.version.size());

        std::string ch_str;
        for (size_t i = 0; i < r.channels.size(); ++i) {
            if (i > 0) ch_str += ", ";
            ch_str += r.channels[i];
        }
        w_chan = std::max(w_chan, ch_str.size());

        w_full  = std::max(w_full, format_size(r.full_size).size());
        w_delta = std::max(w_delta, r.delta_size > 0 ? format_size(r.delta_size).size() : size_t(1));
        w_date  = std::max(w_date, r.created.size());
        w_notes = std::max(w_notes, r.notes.size());
    }

    // Print header
    std::cout << fmt::format("  {:<{}}  {:<{}}  {:>{}}  {:>{}}  {:<{}}  {:<{}}",
                             h_ver, w_ver, h_chan, w_chan,
                             h_full, w_full, h_delta, w_delta,
                             h_date, w_date, h_notes, w_notes)
              << "\n";

    // Print separator
    std::cout << "  " << std::string(w_ver, '-')
              << "  " << std::string(w_chan, '-')
              << "  " << std::string(w_full, '-')
              << "  " << std::string(w_delta, '-')
              << "  " << std::string(w_date, '-')
              << "  " << std::string(w_notes, '-')
              << "\n";

    // Print rows
    for (const auto& r : releases) {
        std::string ch_str;
        for (size_t i = 0; i < r.channels.size(); ++i) {
            if (i > 0) ch_str += ", ";
            ch_str += r.channels[i];
        }

        std::string delta_str = r.delta_size > 0 ? format_size(r.delta_size) : "-";

        std::cout << fmt::format("  {:<{}}  {:<{}}  {:>{}}  {:>{}}  {:<{}}  {:<{}}",
                                 r.version, w_ver, ch_str, w_chan,
                                 format_size(r.full_size), w_full, delta_str, w_delta,
                                 r.created, w_date, r.notes, w_notes)
                  << "\n";
    }
}

void print_json(const std::vector<ReleaseEntry>& releases) {
    nlohmann::json json_releases = nlohmann::json::array();

    for (const auto& r : releases) {
        nlohmann::json entry;
        entry["version"] = r.version;
        entry["channels"] = r.channels;
        entry["fullSize"] = r.full_size;
        entry["deltaSize"] = r.delta_size;
        entry["created"] = r.created;
        entry["notes"] = r.notes;
        entry["isGenesis"] = r.is_genesis;
        json_releases.push_back(entry);
    }

    nlohmann::json output;
    output["releases"] = json_releases;

    std::cout << output.dump(2) << "\n";
}

} // anonymous namespace

int cmd_list(int argc, char* argv[]) {
    cxxopts::Options options("surge list", "List releases and their channels");

    options.add_options()
        ("app", "Application ID", cxxopts::value<std::string>()->default_value(""))
        ("channel", "Filter by channel", cxxopts::value<std::string>()->default_value(""))
        ("format", "Output format: table or json", cxxopts::value<std::string>()->default_value("table"))
        ("manifest", "Path to surge.yml", cxxopts::value<std::string>()->default_value(""))
        ("h,help", "Show help")
    ;

    auto result = options.parse(argc, argv);

    if (result.count("help")) {
        std::cout << options.help() << "\n";
        return 0;
    }

    const auto output_format = result["format"].as<std::string>();
    if (output_format != "table" && output_format != "json") {
        spdlog::error("Unknown format '{}'. Use 'table' or 'json'.", output_format);
        return 1;
    }

    const auto channel_filter = result["channel"].as<std::string>();

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

    spdlog::info("Fetching releases for app '{}'...", target_app->id);

    // TODO: Download release index from storage
    // auto storage = surge::storage::create_storage_backend(storage_config);
    // std::vector<uint8_t> index_data;
    // std::string key = fmt::format("{}/{}/{}",
    //     manifest.storage.prefix, target_app->id, surge::constants::RELEASES_FILE_COMPRESSED);
    // int32_t rc = storage->get_object(key, index_data);

    // TODO: Parse the release index and populate release entries
    std::vector<ReleaseEntry> releases;

    // Placeholder: parse releases from downloaded YAML
    // YAML::Node index = YAML::Load(decompressed_content);
    // for (const auto& node : index["releases"]) {
    //     ReleaseEntry entry;
    //     entry.version = node["version"].as<std::string>();
    //     for (const auto& ch : node["channels"]) {
    //         entry.channels.push_back(ch.as<std::string>());
    //     }
    //     entry.full_size = node["fullSize"].as<int64_t>(0);
    //     entry.delta_size = node["deltaSize"].as<int64_t>(0);
    //     entry.created = node["created"].as<std::string>("");
    //     entry.notes = node["notes"].as<std::string>("");
    //     entry.is_genesis = node["isGenesis"].as<bool>(false);
    //     releases.push_back(entry);
    // }

    // Apply channel filter
    if (!channel_filter.empty()) {
        releases.erase(
            std::remove_if(releases.begin(), releases.end(),
                [&channel_filter](const ReleaseEntry& r) {
                    return std::find(r.channels.begin(), r.channels.end(), channel_filter)
                           == r.channels.end();
                }),
            releases.end());
    }

    if (releases.empty()) {
        if (!channel_filter.empty()) {
            spdlog::info("No releases found on channel '{}'", channel_filter);
        } else {
            spdlog::info("No releases found for app '{}'", target_app->id);
        }
        return 0;
    }

    spdlog::info("Found {} release(s)", releases.size());

    if (output_format == "json") {
        print_json(releases);
    } else {
        print_table(releases);
    }

    return 0;
}
