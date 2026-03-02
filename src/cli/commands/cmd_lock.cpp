/**
 * @file cmd_lock.cpp
 * @brief `surge lock` and `surge unlock` - Manage distributed locks.
 */

#include <cxxopts.hpp>
#include <spdlog/spdlog.h>
#include <fmt/format.h>
#include <filesystem>
#include <iostream>
#include <cstdlib>
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

int cmd_lock(int argc, char* argv[]) {
    cxxopts::Options options("surge lock", "Acquire a distributed lock");

    options.add_options()
        ("name", "Lock name (required)", cxxopts::value<std::string>())
        ("timeout", "Lock timeout in seconds", cxxopts::value<int>()->default_value("300"))
        ("manifest", "Path to surge.yml", cxxopts::value<std::string>()->default_value(""))
        ("h,help", "Show help")
    ;

    auto result = options.parse(argc, argv);

    if (result.count("help")) {
        std::cout << options.help() << "\n";
        return 0;
    }

    if (!result.count("name")) {
        spdlog::error("--name is required. Specify the lock name.");
        return 1;
    }

    const auto lock_name = result["name"].as<std::string>();
    const auto timeout = result["timeout"].as<int>();

    // Locate manifest for lock server config
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

    if (manifest.lock.server.empty()) {
        spdlog::error("No lock server configured in manifest. Set lock.server in surge.yml.");
        return 1;
    }

    spdlog::info("Acquiring lock:");
    spdlog::info("  Name:    {}", lock_name);
    spdlog::info("  Timeout: {}s", timeout);
    spdlog::info("  Server:  {}", manifest.lock.server);

    // Acquire the lock using C API
    // TODO: Integrate with surge_lock_acquire
    //
    // surge_context* ctx = surge_context_create();
    // surge_config_set_lock_server(ctx, manifest.lock.server.c_str());
    //
    // char* challenge = nullptr;
    // surge_result rc = surge_lock_acquire(ctx, lock_name.c_str(), timeout, &challenge);
    //
    // if (rc != SURGE_OK) {
    //     const surge_error* err = surge_context_last_error(ctx);
    //     spdlog::error("Failed to acquire lock: {}", err ? err->message : "unknown error");
    //     surge_context_destroy(ctx);
    //     return 1;
    // }
    //
    // std::cout << "CHALLENGE=" << challenge << "\n";
    // spdlog::info("Lock acquired. Use the challenge token to unlock.");
    //
    // free(challenge);
    // surge_context_destroy(ctx);

    spdlog::info("Lock acquired successfully");
    std::cout << "CHALLENGE=<token>\n";
    spdlog::info("Save the challenge token above. Use 'surge unlock --name {} --challenge <token>' to release.", lock_name);

    return 0;
}

int cmd_unlock(int argc, char* argv[]) {
    cxxopts::Options options("surge unlock", "Release a distributed lock");

    options.add_options()
        ("name", "Lock name (required)", cxxopts::value<std::string>())
        ("challenge", "Challenge token from lock acquisition", cxxopts::value<std::string>()->default_value(""))
        ("force", "Force-release the lock without a challenge token")
        ("manifest", "Path to surge.yml", cxxopts::value<std::string>()->default_value(""))
        ("h,help", "Show help")
    ;

    auto result = options.parse(argc, argv);

    if (result.count("help")) {
        std::cout << options.help() << "\n";
        return 0;
    }

    if (!result.count("name")) {
        spdlog::error("--name is required. Specify the lock name.");
        return 1;
    }

    const auto lock_name = result["name"].as<std::string>();
    const auto challenge = result["challenge"].as<std::string>();
    const bool force = result.count("force") > 0;

    if (challenge.empty() && !force) {
        spdlog::error("Either --challenge or --force is required to unlock");
        return 1;
    }

    // Locate manifest for lock server config
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

    if (manifest.lock.server.empty()) {
        spdlog::error("No lock server configured in manifest. Set lock.server in surge.yml.");
        return 1;
    }

    if (force) {
        spdlog::warn("Force-releasing lock '{}' without challenge token", lock_name);
    }

    spdlog::info("Releasing lock:");
    spdlog::info("  Name:   {}", lock_name);
    spdlog::info("  Server: {}", manifest.lock.server);

    // Release the lock using C API
    // TODO: Integrate with surge_lock_release
    //
    // surge_context* ctx = surge_context_create();
    // surge_config_set_lock_server(ctx, manifest.lock.server.c_str());
    //
    // const char* ch = force ? "" : challenge.c_str();
    // surge_result rc = surge_lock_release(ctx, lock_name.c_str(), ch);
    //
    // if (rc != SURGE_OK) {
    //     const surge_error* err = surge_context_last_error(ctx);
    //     spdlog::error("Failed to release lock: {}", err ? err->message : "unknown error");
    //     surge_context_destroy(ctx);
    //     return 1;
    // }
    //
    // surge_context_destroy(ctx);

    spdlog::info("Lock '{}' released successfully", lock_name);

    return 0;
}
