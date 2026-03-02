/**
 * @file manifest.hpp
 * @brief Types and helpers for reading / writing surge.yml manifests.
 */

#pragma once

#include <cstdint>
#include <filesystem>
#include <map>
#include <string>
#include <vector>

namespace surge::config {

struct GenericConfig {
    std::string token;
    std::string artifacts;
    std::string packages;
};

struct StorageManifestConfig {
    std::string provider;
    std::string bucket;
    std::string region;
    std::string prefix;
    std::string endpoint;
};

struct LockManifestConfig {
    std::string server;
};

struct ChannelConfig {
    std::string name;
};

struct TargetConfig {
    std::string os;
    std::string rid;
    std::vector<std::string> persistent_assets;
    std::map<std::string, std::string> environment;
};

struct MetadataConfig {
    std::string description;
    std::string authors;
};

struct AppConfig {
    std::string id;
    std::string main;
    std::string supervisor_id;
    std::string install_directory;
    std::vector<std::string> channels;
    TargetConfig target;
    MetadataConfig metadata;
};

struct SurgeManifest {
    int schema = 1;
    GenericConfig generic;
    StorageManifestConfig storage;
    LockManifestConfig lock;
    std::vector<ChannelConfig> channels;
    std::vector<AppConfig> apps;
};

/**
 * Parse a surge.yml manifest from disk.
 * @throws std::runtime_error on parse failure.
 */
SurgeManifest parse_manifest(const std::filesystem::path& path);

/**
 * Serialize and write a manifest to disk.
 * @throws std::runtime_error on I/O failure.
 */
void write_manifest(const SurgeManifest& manifest, const std::filesystem::path& path);

/**
 * Validate a manifest and return a list of human-readable issues.
 * An empty list means the manifest is valid.
 */
std::vector<std::string> validate_manifest(const SurgeManifest& manifest);

}  // namespace surge::config
