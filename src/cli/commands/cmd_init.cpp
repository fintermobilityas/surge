/**
 * @file cmd_init.cpp
 * @brief `surge init` - Initialize a new surge project.
 */

#include "config/constants.hpp"

#include <cxxopts.hpp>
#include <filesystem>
#include <fmt/format.h>
#include <fstream>
#include <iostream>
#include <spdlog/spdlog.h>
#include <string>

namespace fs = std::filesystem;

namespace {

std::string detect_os() {
#if defined(_WIN32)
    return "win";
#elif defined(__APPLE__)
    return "osx";
#else
    return "linux";
#endif
}

std::string detect_rid() {
    std::string os = detect_os();
#if defined(__x86_64__) || defined(_M_X64)
    return os + "-x64";
#elif defined(__aarch64__) || defined(_M_ARM64)
    return os + "-arm64";
#else
    return os + "-x64";
#endif
}

std::string generate_template_manifest(const std::string& app_id, const std::string& main_exe, const std::string& os,
                                       const std::string& rid) {
    return fmt::format(R"(# Surge manifest - https://github.com/user/surge
schema: {schema}

generic:
  token: "$SURGE_TOKEN"
  artifacts: artifacts
  packages: packages

storage:
  provider: s3
  bucket: my-updates-bucket
  region: us-east-1
  prefix: releases
  # endpoint: https://custom-s3-endpoint.example.com  # for S3-compatible providers

lock:
  server: ""

channels:
  - name: stable
  - name: staging
  - name: test

apps:
  - id: {app_id}
    main: {main}
    supervisorId: {app_id}_supervisor
    installDirectory: $HOME/.surge/apps/{app_id}
    channels:
      - test
      - staging
      - stable
    target:
      os: {os}
      rid: {rid}
      persistentAssets: []
      environment: {{}}
    metadata:
      description: ""
      authors: ""
)",
                       fmt::arg("schema", surge::constants::MANIFEST_SCHEMA_VERSION), fmt::arg("app_id", app_id),
                       fmt::arg("main", main_exe), fmt::arg("os", os), fmt::arg("rid", rid));
}

}  // anonymous namespace

int cmd_init(int argc, char* argv[]) {
    cxxopts::Options options("surge init", "Initialize a new surge project");

    options.add_options()("app-id", "Application identifier", cxxopts::value<std::string>()->default_value("myapp"))(
        "main", "Main executable name", cxxopts::value<std::string>()->default_value("myapp"))(
        "path", "Project directory (default: current directory)", cxxopts::value<std::string>()->default_value("."))(
        "h,help", "Show help");

    auto result = options.parse(argc, argv);

    if (result.count("help")) {
        std::cout << options.help() << "\n";
        return 0;
    }

    const auto app_id = result["app-id"].as<std::string>();
    const auto main_exe = result["main"].as<std::string>();
    const auto project_dir = fs::path(result["path"].as<std::string>());
    const auto surge_dir = project_dir / surge::constants::SURGE_DIR;
    const auto manifest_path = surge_dir / surge::constants::MANIFEST_FILE;

    // Check if already initialized
    if (fs::exists(manifest_path)) {
        spdlog::error("surge project already initialized at {}", surge_dir.string());
        spdlog::info("Edit {} to update configuration", manifest_path.string());
        return 1;
    }

    // Detect platform
    const auto os = detect_os();
    const auto rid = detect_rid();

    spdlog::info("Initializing surge project...");
    spdlog::info("  Platform: {} ({})", os, rid);
    spdlog::info("  App ID:   {}", app_id);
    spdlog::info("  Main:     {}", main_exe);

    // Create directories
    fs::create_directories(surge_dir);
    fs::create_directories(project_dir / surge::constants::ARTIFACTS_DIR);
    fs::create_directories(project_dir / surge::constants::PACKAGES_DIR);

    // Write template manifest
    const auto manifest_content = generate_template_manifest(app_id, main_exe, os, rid);
    {
        std::ofstream out(manifest_path);
        if (!out) {
            spdlog::error("Failed to write {}", manifest_path.string());
            return 1;
        }
        out << manifest_content;
    }

    spdlog::info("Created {}", manifest_path.string());

    std::cout << "\n"
              << "Surge project initialized successfully.\n"
              << "\n"
              << "Next steps:\n"
              << "  1. Edit " << manifest_path.string() << " to configure storage and app settings\n"
              << "  2. Place build artifacts in the '" << surge::constants::ARTIFACTS_DIR << "/' directory\n"
              << "  3. Run 'surge pack --version 1.0.0' to build packages\n"
              << "  4. Run 'surge push --channel test' to upload packages\n";

    return 0;
}
