/**
 * @file constants.hpp
 * @brief Compile-time constants for the Surge update framework.
 */

#pragma once

#include <cstdint>

namespace surge::constants {

inline constexpr const char* VERSION = "0.1.0";
inline constexpr const char* SURGE_DIR = ".surge";
inline constexpr const char* MANIFEST_FILE = "surge.yml";
inline constexpr const char* RELEASES_FILE = "releases.yml";
inline constexpr const char* RELEASES_FILE_COMPRESSED = "releases.yml.zst";
inline constexpr const char* RELEASES_CHECKSUM_FILE = "releases.yml.zst.sha256";
inline constexpr const char* ARCHIVE_MANIFEST_FILE = "manifest.yml";
inline constexpr const char* APP_DIR_PREFIX = "app-";
inline constexpr const char* PACKAGES_DIR = "packages";
inline constexpr const char* ARTIFACTS_DIR = "artifacts";

inline constexpr int MANIFEST_SCHEMA_VERSION = 1;

}  // namespace surge::constants
