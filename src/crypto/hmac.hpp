/**
 * @file hmac.hpp
 * @brief HMAC-SHA256 utilities for authentication signing.
 */

#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <vector>

namespace surge::crypto {

/**
 * Compute HMAC-SHA256 and return the raw 32-byte MAC.
 * @param key  Secret key bytes.
 * @param data Message bytes.
 */
std::vector<uint8_t> hmac_sha256(std::span<const uint8_t> key,
                                  std::span<const uint8_t> data);

/**
 * Compute HMAC-SHA256 and return the result as a lowercase hex string.
 * @param key  Secret key bytes.
 * @param data Message bytes.
 */
std::string hmac_sha256_hex(std::span<const uint8_t> key,
                             std::span<const uint8_t> data);

} // namespace surge::crypto
