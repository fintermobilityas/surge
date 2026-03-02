/**
 * @file release_manifest.cpp
 * @brief Release index (releases.yml) YAML handling with zstd compression.
 */

#include "releases/release_manifest.hpp"
#include "crypto/sha256.hpp"
#include <yaml-cpp/yaml.h>
#include <spdlog/spdlog.h>
#include <fmt/format.h>
#include <zstd.h>
#include <algorithm>
#include <charconv>
#include <sstream>
#include <stdexcept>

namespace surge::releases {

namespace {

std::vector<int> parse_version_parts(const std::string& version) {
    std::vector<int> parts;
    std::string_view sv = version;

    while (!sv.empty()) {
        int value = 0;
        auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), value);
        if (ec != std::errc{}) break;
        parts.push_back(value);
        sv = std::string_view(ptr, sv.data() + sv.size() - ptr);
        if (!sv.empty() && sv.front() == '.') {
            sv.remove_prefix(1);
        }
    }
    return parts;
}

} // anonymous namespace

int compare_versions(const std::string& a, const std::string& b) {
    auto parts_a = parse_version_parts(a);
    auto parts_b = parse_version_parts(b);

    auto max_len = std::max(parts_a.size(), parts_b.size());
    parts_a.resize(max_len, 0);
    parts_b.resize(max_len, 0);

    for (size_t i = 0; i < max_len; ++i) {
        if (parts_a[i] < parts_b[i]) return -1;
        if (parts_a[i] > parts_b[i]) return 1;
    }
    return 0;
}

ReleaseIndex parse_release_index(std::span<const uint8_t> yaml_bytes) {
    ReleaseIndex index;

    std::string yaml_str(reinterpret_cast<const char*>(yaml_bytes.data()), yaml_bytes.size());
    YAML::Node doc = YAML::Load(yaml_str);

    if (doc["app_id"]) index.app_id = doc["app_id"].as<std::string>();
    if (doc["schema"]) index.schema = doc["schema"].as<int>();

    if (doc["channels"] && doc["channels"].IsSequence()) {
        for (auto& ch : doc["channels"]) {
            index.channels.push_back(ch.as<std::string>());
        }
    }

    if (doc["releases"] && doc["releases"].IsSequence()) {
        for (auto& rel_node : doc["releases"]) {
            ReleaseEntry entry;
            if (rel_node["version"]) entry.version = rel_node["version"].as<std::string>();
            if (rel_node["channel"]) entry.channel = rel_node["channel"].as<std::string>();
            if (rel_node["is_genesis"]) entry.is_genesis = rel_node["is_genesis"].as<bool>();
            if (rel_node["is_delta"]) entry.is_delta = rel_node["is_delta"].as<bool>();

            if (rel_node["full"]) {
                auto& full = rel_node["full"];
                if (full["filename"]) entry.full.filename = full["filename"].as<std::string>();
                if (full["size"]) entry.full.size = full["size"].as<int64_t>();
                if (full["sha256"]) entry.full.sha256 = full["sha256"].as<std::string>();
            }

            if (rel_node["delta"]) {
                auto& delta = rel_node["delta"];
                if (delta["filename"]) entry.delta.filename = delta["filename"].as<std::string>();
                if (delta["size"]) entry.delta.size = delta["size"].as<int64_t>();
                if (delta["sha256"]) entry.delta.sha256 = delta["sha256"].as<std::string>();
                if (delta["base_version"]) entry.delta.base_version = delta["base_version"].as<std::string>();
            }

            if (rel_node["files"] && rel_node["files"].IsSequence()) {
                for (auto& f : rel_node["files"]) {
                    FileChecksum fc;
                    if (f["path"]) fc.path = f["path"].as<std::string>();
                    if (f["sha256"]) fc.sha256 = f["sha256"].as<std::string>();
                    if (f["size"]) fc.size = f["size"].as<int64_t>();
                    entry.files.push_back(std::move(fc));
                }
            }

            index.releases.push_back(std::move(entry));
        }
    }

    spdlog::debug("Parsed release index: app_id={}, {} releases", index.app_id, index.releases.size());
    return index;
}

std::vector<uint8_t> serialize_release_index(const ReleaseIndex& index) {
    YAML::Emitter out;
    out << YAML::BeginMap;
    out << YAML::Key << "schema" << YAML::Value << index.schema;
    out << YAML::Key << "app_id" << YAML::Value << index.app_id;

    out << YAML::Key << "channels" << YAML::Value << YAML::BeginSeq;
    for (auto& ch : index.channels) {
        out << ch;
    }
    out << YAML::EndSeq;

    out << YAML::Key << "releases" << YAML::Value << YAML::BeginSeq;
    for (auto& rel : index.releases) {
        out << YAML::BeginMap;
        out << YAML::Key << "version" << YAML::Value << rel.version;
        out << YAML::Key << "channel" << YAML::Value << rel.channel;
        out << YAML::Key << "is_genesis" << YAML::Value << rel.is_genesis;
        out << YAML::Key << "is_delta" << YAML::Value << rel.is_delta;

        out << YAML::Key << "full" << YAML::Value << YAML::BeginMap;
        out << YAML::Key << "filename" << YAML::Value << rel.full.filename;
        out << YAML::Key << "size" << YAML::Value << rel.full.size;
        out << YAML::Key << "sha256" << YAML::Value << rel.full.sha256;
        out << YAML::EndMap;

        if (rel.is_delta) {
            out << YAML::Key << "delta" << YAML::Value << YAML::BeginMap;
            out << YAML::Key << "filename" << YAML::Value << rel.delta.filename;
            out << YAML::Key << "size" << YAML::Value << rel.delta.size;
            out << YAML::Key << "sha256" << YAML::Value << rel.delta.sha256;
            out << YAML::Key << "base_version" << YAML::Value << rel.delta.base_version;
            out << YAML::EndMap;
        }

        if (!rel.files.empty()) {
            out << YAML::Key << "files" << YAML::Value << YAML::BeginSeq;
            for (auto& f : rel.files) {
                out << YAML::BeginMap;
                out << YAML::Key << "path" << YAML::Value << f.path;
                out << YAML::Key << "sha256" << YAML::Value << f.sha256;
                out << YAML::Key << "size" << YAML::Value << f.size;
                out << YAML::EndMap;
            }
            out << YAML::EndSeq;
        }

        out << YAML::EndMap;
    }
    out << YAML::EndSeq;
    out << YAML::EndMap;

    std::string yaml_str = out.c_str();
    return {yaml_str.begin(), yaml_str.end()};
}

std::vector<uint8_t> compress_release_index(std::span<const uint8_t> yaml_data, int level) {
    size_t bound = ZSTD_compressBound(yaml_data.size());
    std::vector<uint8_t> compressed(bound);

    size_t compressed_size = ZSTD_compress(
        compressed.data(), compressed.size(),
        yaml_data.data(), yaml_data.size(),
        level);

    if (ZSTD_isError(compressed_size)) {
        throw std::runtime_error(fmt::format("Zstd compression failed: {}",
                                              ZSTD_getErrorName(compressed_size)));
    }

    compressed.resize(compressed_size);
    spdlog::debug("Compressed release index: {} -> {} bytes ({:.1f}%)",
                   yaml_data.size(), compressed_size,
                   100.0 * static_cast<double>(compressed_size) / static_cast<double>(yaml_data.size()));
    return compressed;
}

std::vector<uint8_t> decompress_release_index(std::span<const uint8_t> compressed_data) {
    // Determine decompressed size
    auto decompressed_size = ZSTD_getFrameContentSize(compressed_data.data(), compressed_data.size());
    if (decompressed_size == ZSTD_CONTENTSIZE_UNKNOWN) {
        // Fallback: allocate a reasonable buffer and grow if needed
        decompressed_size = compressed_data.size() * 10;
    }
    if (decompressed_size == ZSTD_CONTENTSIZE_ERROR) {
        throw std::runtime_error("Invalid zstd compressed data");
    }

    std::vector<uint8_t> decompressed(decompressed_size);
    size_t actual = ZSTD_decompress(
        decompressed.data(), decompressed.size(),
        compressed_data.data(), compressed_data.size());

    if (ZSTD_isError(actual)) {
        throw std::runtime_error(fmt::format("Zstd decompression failed: {}",
                                              ZSTD_getErrorName(actual)));
    }

    decompressed.resize(actual);
    return decompressed;
}

std::vector<ReleaseEntry> get_releases_newer_than(
    const ReleaseIndex& index,
    const std::string& current_version,
    const std::string& channel)
{
    std::vector<ReleaseEntry> newer;

    for (auto& rel : index.releases) {
        if (!channel.empty() && rel.channel != channel) continue;
        if (compare_versions(rel.version, current_version) > 0) {
            newer.push_back(rel);
        }
    }

    // Sort by version ascending
    std::sort(newer.begin(), newer.end(),
              [](const ReleaseEntry& a, const ReleaseEntry& b) {
                  return compare_versions(a.version, b.version) < 0;
              });

    return newer;
}

std::vector<ReleaseEntry> get_delta_chain(
    const ReleaseIndex& index,
    const std::string& current_version,
    const std::string& target_version,
    const std::string& channel)
{
    std::vector<ReleaseEntry> chain;

    // Get all versions between current and target
    auto newer = get_releases_newer_than(index, current_version, channel);

    for (auto& rel : newer) {
        if (compare_versions(rel.version, target_version) > 0) break;

        // Prefer delta releases if they chain from the right base
        if (rel.is_delta) {
            std::string expected_base = chain.empty() ? current_version : chain.back().version;
            if (rel.delta.base_version == expected_base) {
                chain.push_back(rel);
                continue;
            }
        }

        // Fall back to full release
        chain.push_back(rel);

        if (rel.version == target_version) break;
    }

    return chain;
}

const ReleaseEntry* find_release(const ReleaseIndex& index,
                                  const std::string& version,
                                  const std::string& channel) {
    for (auto& rel : index.releases) {
        if (rel.version == version) {
            if (channel.empty() || rel.channel == channel) {
                return &rel;
            }
        }
    }
    return nullptr;
}

const ReleaseEntry* get_latest_release(const ReleaseIndex& index,
                                        const std::string& channel) {
    const ReleaseEntry* latest = nullptr;
    for (auto& rel : index.releases) {
        if (!channel.empty() && rel.channel != channel) continue;
        if (!latest || compare_versions(rel.version, latest->version) > 0) {
            latest = &rel;
        }
    }
    return latest;
}

} // namespace surge::releases
