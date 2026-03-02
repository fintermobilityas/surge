/**
 * @file cmd_migrate.cpp
 * @brief `surge migrate` - Migrate configuration from snapx to surge.
 */

#include "config/constants.hpp"
#include "config/manifest.hpp"

#include <cxxopts.hpp>
#include <filesystem>
#include <fmt/format.h>
#include <fstream>
#include <iostream>
#include <set>
#include <spdlog/spdlog.h>
#include <string>
#include <vector>
#include <yaml-cpp/yaml.h>

namespace fs = std::filesystem;

namespace {

// Fields from snapx that are dropped during migration (NuGet-specific)
const std::set<std::string> DROPPED_FIELDS = {
    "nuspec", "pushFeed", "updateFeed", "framework", "icon", "shortcuts", "installers",
};

struct MigrationSummary {
    int apps_migrated = 0;
    int channels_migrated = 0;
    std::vector<std::string> dropped_fields;
    std::vector<std::string> warnings;
};

surge::config::SurgeManifest migrate_snapx_manifest(const YAML::Node& snapx, MigrationSummary& summary) {
    surge::config::SurgeManifest surge_manifest;
    surge_manifest.schema = surge::constants::MANIFEST_SCHEMA_VERSION;

    // Migrate generic section
    if (snapx["generic"]) {
        auto generic = snapx["generic"];
        if (generic["token"]) {
            surge_manifest.generic.token = generic["token"].as<std::string>("");
        }
        if (generic["artifacts"]) {
            surge_manifest.generic.artifacts = generic["artifacts"].as<std::string>("");
        }
        if (generic["packages"]) {
            surge_manifest.generic.packages = generic["packages"].as<std::string>("");
        }
    }

    // Migrate channels
    if (snapx["channels"]) {
        for (const auto& ch : snapx["channels"]) {
            surge::config::ChannelConfig channel;
            if (ch.IsMap()) {
                channel.name = ch["name"].as<std::string>("");
            } else {
                channel.name = ch.as<std::string>("");
            }
            if (!channel.name.empty()) {
                surge_manifest.channels.push_back(channel);
                ++summary.channels_migrated;
            }
        }
    }

    // Storage: snapx uses NuGet feeds, surge uses cloud storage.
    // We cannot automatically map NuGet feeds to S3/Azure/GCS,
    // so we set up a template and warn the user.
    surge_manifest.storage.provider = "s3";
    surge_manifest.storage.bucket = "my-updates-bucket";
    surge_manifest.storage.region = "us-east-1";
    surge_manifest.storage.prefix = "releases";
    summary.warnings.push_back(
        "Storage configuration requires manual setup. "
        "snapx used NuGet feeds; surge uses cloud storage (S3/Azure/GCS).");

    // Lock server: not present in snapx
    surge_manifest.lock.server = "";

    // Migrate apps
    if (snapx["apps"]) {
        for (const auto& app_node : snapx["apps"]) {
            surge::config::AppConfig app;

            // Keep: id, main (exe), supervisorId, installDirectory
            app.id = app_node["id"].as<std::string>("");
            if (app_node["exe"]) {
                app.main = app_node["exe"].as<std::string>("");
            } else if (app_node["main"]) {
                app.main = app_node["main"].as<std::string>("");
            }
            app.supervisor_id = app_node["supervisorId"].as<std::string>("");
            app.install_directory = app_node["installDirectory"].as<std::string>("");

            // Migrate channels list
            if (app_node["channels"]) {
                for (const auto& ch : app_node["channels"]) {
                    if (ch.IsMap()) {
                        app.channels.push_back(ch["name"].as<std::string>(""));
                    } else {
                        app.channels.push_back(ch.as<std::string>(""));
                    }
                }
            }

            // Keep: target (os, rid, persistentAssets, environment)
            if (app_node["target"]) {
                auto target = app_node["target"];
                app.target.os = target["os"].as<std::string>("");
                app.target.rid = target["rid"].as<std::string>("");

                if (target["persistentAssets"]) {
                    for (const auto& asset : target["persistentAssets"]) {
                        app.target.persistent_assets.push_back(asset.as<std::string>(""));
                    }
                }

                if (target["environment"]) {
                    for (const auto& env : target["environment"]) {
                        app.target.environment[env.first.as<std::string>()] = env.second.as<std::string>("");
                    }
                }
            }

            // Migrate metadata if present
            if (app_node["description"]) {
                app.metadata.description = app_node["description"].as<std::string>("");
            }
            if (app_node["authors"]) {
                app.metadata.authors = app_node["authors"].as<std::string>("");
            }

            // Track dropped NuGet-specific fields
            for (const auto& field : DROPPED_FIELDS) {
                if (app_node[field]) {
                    summary.dropped_fields.push_back(fmt::format("apps[{}].{}", app.id, field));
                }
            }

            if (!app.id.empty()) {
                surge_manifest.apps.push_back(std::move(app));
                ++summary.apps_migrated;
            }
        }
    }

    // Check for top-level dropped fields
    for (const auto& field : DROPPED_FIELDS) {
        if (snapx[field]) {
            summary.dropped_fields.push_back(field);
        }
    }

    return surge_manifest;
}

}  // anonymous namespace

int cmd_migrate(int argc, char* argv[]) {
    cxxopts::Options options("surge migrate", "Migrate configuration from snapx to surge");

    options.add_options()("from-snapx", "Path to snapx.yml file (required)", cxxopts::value<std::string>())(
        "output", "Output path for surge.yml", cxxopts::value<std::string>()->default_value(""))("h,help", "Show help");

    auto result = options.parse(argc, argv);

    if (result.count("help")) {
        std::cout << options.help() << "\n";
        return 0;
    }

    if (!result.count("from-snapx")) {
        spdlog::error("--from-snapx is required. Specify the path to your snapx.yml file.");
        return 1;
    }

    const auto snapx_path = fs::path(result["from-snapx"].as<std::string>());
    if (!fs::exists(snapx_path)) {
        spdlog::error("snapx manifest not found: {}", snapx_path.string());
        return 1;
    }

    // Determine output path
    fs::path output_path;
    auto output_str = result["output"].as<std::string>();
    if (output_str.empty()) {
        output_path = fs::path(surge::constants::SURGE_DIR) / surge::constants::MANIFEST_FILE;
    } else {
        output_path = fs::path(output_str);
    }

    spdlog::info("Migrating from snapx:");
    spdlog::info("  Source: {}", snapx_path.string());
    spdlog::info("  Output: {}", output_path.string());

    // Parse snapx.yml
    YAML::Node snapx;
    try {
        snapx = YAML::LoadFile(snapx_path.string());
    } catch (const std::exception& ex) {
        spdlog::error("Failed to parse snapx manifest: {}", ex.what());
        return 1;
    }

    // Perform migration
    MigrationSummary summary;
    surge::config::SurgeManifest surge_manifest;
    try {
        surge_manifest = migrate_snapx_manifest(snapx, summary);
    } catch (const std::exception& ex) {
        spdlog::error("Migration failed: {}", ex.what());
        return 1;
    }

    // Validate the generated manifest
    auto issues = surge::config::validate_manifest(surge_manifest);
    if (!issues.empty()) {
        spdlog::warn("Generated manifest has validation issues:");
        for (const auto& issue : issues) {
            spdlog::warn("  - {}", issue);
        }
    }

    // Write output
    fs::create_directories(output_path.parent_path());
    try {
        surge::config::write_manifest(surge_manifest, output_path);
    } catch (const std::exception& ex) {
        spdlog::error("Failed to write surge manifest: {}", ex.what());
        return 1;
    }

    // Print summary
    std::cout << "\n"
              << "Migration summary:\n"
              << "  Apps migrated:     " << summary.apps_migrated << "\n"
              << "  Channels migrated: " << summary.channels_migrated << "\n";

    if (!summary.dropped_fields.empty()) {
        std::cout << "\n  Dropped fields (NuGet-specific, not applicable to surge):\n";
        for (const auto& field : summary.dropped_fields) {
            std::cout << "    - " << field << "\n";
        }
    }

    if (!summary.warnings.empty()) {
        std::cout << "\n  Warnings:\n";
        for (const auto& warning : summary.warnings) {
            std::cout << "    - " << warning << "\n";
        }
    }

    std::cout << "\n"
              << "Output written to: " << output_path.string() << "\n"
              << "\n"
              << "Next steps:\n"
              << "  1. Review and edit " << output_path.string() << "\n"
              << "  2. Configure cloud storage settings (S3, Azure Blob, or GCS)\n"
              << "  3. Configure lock server if using distributed locks\n"
              << "  4. Run 'surge pack --version <version>' to build your first surge packages\n";

    return 0;
}
