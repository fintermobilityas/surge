/**
 * @file release_manifest.hpp
 * @brief Release index types, serialization, and version comparison.
 */

#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <vector>

namespace surge::releases {

/** A single release entry in the index. */
struct ReleaseEntry {
    std::string version;
    std::vector<std::string> channels;
    std::string os;
    std::string rid;
    bool is_genesis = false;

    std::string full_filename;
    int64_t full_size = 0;
    std::string full_sha256;

    std::string delta_filename;
    int64_t delta_size = 0;
    std::string delta_sha256;

    std::string created_utc;
    std::string release_notes;
};

/** Top-level release index (serialized as releases.yml). */
struct ReleaseIndex {
    int schema = 1;
    std::string app_id;
    std::string pack_id;
    std::string last_write_utc;
    std::vector<ReleaseEntry> releases;
};

/* ----- serialization ----- */

/**
 * Parse a YAML release index from raw bytes.
 * @throws std::runtime_error on parse failure.
 */
ReleaseIndex parse_release_index(const std::vector<uint8_t>& yaml_data);

/** Serialize a release index to YAML bytes. */
std::vector<uint8_t> serialize_release_index(const ReleaseIndex& index);

/**
 * Serialize and compress a release index with zstd.
 * @param index      Index to serialize.
 * @param zstd_level Compression level (1-22, default 9).
 */
std::vector<uint8_t> compress_release_index(const ReleaseIndex& index, int zstd_level = 9);

/**
 * Decompress a zstd-compressed release index and parse it.
 * @throws std::runtime_error on decompression or parse failure.
 */
ReleaseIndex decompress_release_index(std::span<const uint8_t> compressed);

/* ----- querying ----- */

/**
 * Return all releases newer than @p version on the given @p channel,
 * ordered from oldest to newest.
 */
std::vector<ReleaseEntry> get_releases_newer_than(const ReleaseIndex& index, const std::string& version,
                                                  const std::string& channel);

/**
 * Compute the optimal chain of delta patches from @p from_version to
 * @p to_version on the given @p channel.
 * @return Ordered list of releases whose delta patches should be applied
 *         sequentially. Empty if no delta chain exists.
 */
std::vector<ReleaseEntry> get_delta_chain(const ReleaseIndex& index, const std::string& from_version,
                                          const std::string& to_version, const std::string& channel);

/* ----- version comparison ----- */

/**
 * Compare two semantic version strings.
 * @return Negative if a < b, zero if a == b, positive if a > b.
 */
int compare_versions(const std::string& a, const std::string& b);

}  // namespace surge::releases
