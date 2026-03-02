/**
 * @file sha256.hpp
 * @brief SHA-256 hashing utilities.
 */

#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <span>
#include <string>
#include <vector>

namespace surge::crypto {

/**
 * Compute the SHA-256 hash of an in-memory buffer and return it as a
 * lowercase hex string (64 characters).
 */
std::string sha256_hex(std::span<const uint8_t> data);

/**
 * Compute the SHA-256 hash of a file on disk and return it as a lowercase
 * hex string. Reads the file in streaming fashion to handle large files.
 * @param path     Path to the file.
 * @param progress Optional callback receiving (bytes_done, bytes_total).
 */
std::string sha256_hex_file(
    const std::filesystem::path& path,
    std::function<void(int64_t bytes_done, int64_t bytes_total)> progress = nullptr);

/**
 * Compute the raw 32-byte SHA-256 hash of an in-memory buffer.
 */
std::vector<uint8_t> sha256_raw(std::span<const uint8_t> data);

} // namespace surge::crypto
