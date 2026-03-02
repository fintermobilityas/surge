#include "crypto/hmac.hpp"

#include <array>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <stdexcept>

namespace surge::crypto {

namespace {

constexpr size_t HMAC_SHA256_SIZE = 32;

std::string bytes_to_hex(const uint8_t* data, size_t len) {
    std::string result;
    result.reserve(len * 2);
    constexpr char hex_chars[] = "0123456789abcdef";
    for (size_t i = 0; i < len; ++i) {
        result.push_back(hex_chars[(data[i] >> 4) & 0x0f]);
        result.push_back(hex_chars[data[i] & 0x0f]);
    }
    return result;
}

}  // anonymous namespace

std::vector<uint8_t> hmac_sha256(std::span<const uint8_t> key, std::span<const uint8_t> data) {
    std::vector<uint8_t> result(HMAC_SHA256_SIZE);
    unsigned int result_len = 0;

    auto* out = HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()), data.data(), data.size(), result.data(),
                     &result_len);

    if (!out) {
        throw std::runtime_error("HMAC-SHA256 computation failed");
    }

    result.resize(result_len);
    return result;
}

std::string hmac_sha256_hex(std::span<const uint8_t> key, std::span<const uint8_t> data) {
    auto raw = hmac_sha256(key, data);
    return bytes_to_hex(raw.data(), raw.size());
}

}  // namespace surge::crypto
