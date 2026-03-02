/**
 * @file release_manifest.cpp
 * @brief Release index (releases.yml) YAML handling with zstd compression.
 */

#include "releases/release_manifest.hpp"

#include "crypto/sha256.hpp"

#include <algorithm>
#include <charconv>
#include <fmt/format.h>
#include <spdlog/spdlog.h>
#include <sstream>
#include <stdexcept>
#include <yaml-cpp/yaml.h>
#include <zstd.h>

namespace surge::releases {

namespace {

std::vector<int> parse_version_parts(const std::string& version) {
    std::vector<int> parts;
    std::string_view sv = version;

    while (!sv.empty()) {
        int value = 0;
        auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), value);
        if (ec != std::errc{})
            break;
        parts.push_back(value);
        sv = std::string_view(ptr, sv.data() + sv.size() - ptr);
        if (!sv.empty() && sv.front() == '.') {
            sv.remove_prefix(1);
        }
    }
    return parts;
}

}  // anonymous namespace

int compare_versions(const std::string& a, const std::string& b) {
    auto parts_a = parse_version_parts(a);
    auto parts_b = parse_version_parts(b);

    auto max_len = std::max(parts_a.size(), parts_b.size());
    parts_a.resize(max_len, 0);
    parts_b.resize(max_len, 0);

    for (size_t i = 0; i < max_len; ++i) {
        if (parts_a[i] < parts_b[i])
            return -1;
        if (parts_a[i] > parts_b[i])
            return 1;
    }
    return 0;
}

ReleaseIndex parse_release_index(const std::vector<uint8_t>& yaml_data) {
    ReleaseIndex index;

    std::string yaml_str(reinterpret_cast<const char*>(yaml_data.data()), yaml_data.size());
    YAML::Node doc = YAML::Load(yaml_str);

    if (doc["app_id"])
        index.app_id = doc["app_id"].as<std::string>();
    if (doc["pack_id"])
        index.pack_id = doc["pack_id"].as<std::string>();
    if (doc["schema"])
        index.schema = doc["schema"].as<int>();
    if (doc["last_write_utc"])
        index.last_write_utc = doc["last_write_utc"].as<std::string>();

    if (doc["releases"] && doc["releases"].IsSequence()) {
        for (const auto& rel_node : doc["releases"]) {
            ReleaseEntry entry;
            if (rel_node["version"])
                entry.version = rel_node["version"].as<std::string>();
            if (rel_node["os"])
                entry.os = rel_node["os"].as<std::string>();
            if (rel_node["rid"])
                entry.rid = rel_node["rid"].as<std::string>();
            if (rel_node["is_genesis"])
                entry.is_genesis = rel_node["is_genesis"].as<bool>();
            if (rel_node["created_utc"])
                entry.created_utc = rel_node["created_utc"].as<std::string>();
            if (rel_node["release_notes"])
                entry.release_notes = rel_node["release_notes"].as<std::string>();

            if (rel_node["channels"] && rel_node["channels"].IsSequence()) {
                for (const auto& ch : rel_node["channels"]) {
                    entry.channels.push_back(ch.as<std::string>());
                }
            }

            if (rel_node["full_filename"])
                entry.full_filename = rel_node["full_filename"].as<std::string>();
            if (rel_node["full_size"])
                entry.full_size = rel_node["full_size"].as<int64_t>();
            if (rel_node["full_sha256"])
                entry.full_sha256 = rel_node["full_sha256"].as<std::string>();

            if (rel_node["delta_filename"])
                entry.delta_filename = rel_node["delta_filename"].as<std::string>();
            if (rel_node["delta_size"])
                entry.delta_size = rel_node["delta_size"].as<int64_t>();
            if (rel_node["delta_sha256"])
                entry.delta_sha256 = rel_node["delta_sha256"].as<std::string>();

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
    out << YAML::Key << "pack_id" << YAML::Value << index.pack_id;
    out << YAML::Key << "last_write_utc" << YAML::Value << index.last_write_utc;

    out << YAML::Key << "releases" << YAML::Value << YAML::BeginSeq;
    for (auto& rel : index.releases) {
        out << YAML::BeginMap;
        out << YAML::Key << "version" << YAML::Value << rel.version;
        out << YAML::Key << "os" << YAML::Value << rel.os;
        out << YAML::Key << "rid" << YAML::Value << rel.rid;
        out << YAML::Key << "is_genesis" << YAML::Value << rel.is_genesis;
        out << YAML::Key << "created_utc" << YAML::Value << rel.created_utc;

        if (!rel.channels.empty()) {
            out << YAML::Key << "channels" << YAML::Value << YAML::BeginSeq;
            for (auto& ch : rel.channels) {
                out << ch;
            }
            out << YAML::EndSeq;
        }

        out << YAML::Key << "full_filename" << YAML::Value << rel.full_filename;
        out << YAML::Key << "full_size" << YAML::Value << rel.full_size;
        out << YAML::Key << "full_sha256" << YAML::Value << rel.full_sha256;

        if (!rel.delta_filename.empty()) {
            out << YAML::Key << "delta_filename" << YAML::Value << rel.delta_filename;
            out << YAML::Key << "delta_size" << YAML::Value << rel.delta_size;
            out << YAML::Key << "delta_sha256" << YAML::Value << rel.delta_sha256;
        }

        if (!rel.release_notes.empty()) {
            out << YAML::Key << "release_notes" << YAML::Value << rel.release_notes;
        }

        out << YAML::EndMap;
    }
    out << YAML::EndSeq;
    out << YAML::EndMap;

    std::string yaml_str = out.c_str();
    return {yaml_str.begin(), yaml_str.end()};
}

std::vector<uint8_t> compress_release_index(const ReleaseIndex& index, int zstd_level) {
    auto yaml_data = serialize_release_index(index);

    size_t bound = ZSTD_compressBound(yaml_data.size());
    std::vector<uint8_t> compressed(bound);

    size_t compressed_size =
        ZSTD_compress(compressed.data(), compressed.size(), yaml_data.data(), yaml_data.size(), zstd_level);

    if (ZSTD_isError(compressed_size)) {
        throw std::runtime_error(fmt::format("Zstd compression failed: {}", ZSTD_getErrorName(compressed_size)));
    }

    compressed.resize(compressed_size);
    spdlog::debug("Compressed release index: {} -> {} bytes ({:.1f}%)", yaml_data.size(), compressed_size,
                  100.0 * static_cast<double>(compressed_size) / static_cast<double>(yaml_data.size()));
    return compressed;
}

ReleaseIndex decompress_release_index(std::span<const uint8_t> compressed) {
    // Determine decompressed size
    auto decompressed_size = ZSTD_getFrameContentSize(compressed.data(), compressed.size());
    if (decompressed_size == ZSTD_CONTENTSIZE_UNKNOWN) {
        // Fallback: allocate a reasonable buffer and grow if needed
        decompressed_size = compressed.size() * 10;
    }
    if (decompressed_size == ZSTD_CONTENTSIZE_ERROR) {
        throw std::runtime_error("Invalid zstd compressed data");
    }

    std::vector<uint8_t> decompressed(decompressed_size);
    size_t actual = ZSTD_decompress(decompressed.data(), decompressed.size(), compressed.data(), compressed.size());

    if (ZSTD_isError(actual)) {
        throw std::runtime_error(fmt::format("Zstd decompression failed: {}", ZSTD_getErrorName(actual)));
    }

    decompressed.resize(actual);
    return parse_release_index(decompressed);
}

std::vector<ReleaseEntry> get_releases_newer_than(const ReleaseIndex& index, const std::string& version,
                                                  const std::string& channel) {
    std::vector<ReleaseEntry> newer;

    for (auto& rel : index.releases) {
        // Check channel match
        if (!channel.empty()) {
            bool on_channel = false;
            for (auto& ch : rel.channels) {
                if (ch == channel) {
                    on_channel = true;
                    break;
                }
            }
            if (!on_channel)
                continue;
        }
        if (compare_versions(rel.version, version) > 0) {
            newer.push_back(rel);
        }
    }

    // Sort by version ascending
    std::sort(newer.begin(), newer.end(),
              [](const ReleaseEntry& a, const ReleaseEntry& b) { return compare_versions(a.version, b.version) < 0; });

    return newer;
}

std::vector<ReleaseEntry> get_delta_chain(const ReleaseIndex& index, const std::string& from_version,
                                          const std::string& to_version, const std::string& channel) {
    std::vector<ReleaseEntry> chain;

    // Get all versions between current and target
    auto newer = get_releases_newer_than(index, from_version, channel);

    for (auto& rel : newer) {
        if (compare_versions(rel.version, to_version) > 0)
            break;

        chain.push_back(rel);

        if (rel.version == to_version)
            break;
    }

    return chain;
}

}  // namespace surge::releases
