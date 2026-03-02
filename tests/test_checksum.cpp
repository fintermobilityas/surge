/**
 * @file test_checksum.cpp
 * @brief SHA-256 and HMAC-SHA256 tests against known test vectors.
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "crypto/sha256.hpp"
#include "crypto/hmac.hpp"

namespace {

std::vector<uint8_t> to_bytes(const std::string& s) {
    return {s.begin(), s.end()};
}

std::vector<uint8_t> hex_to_bytes(const std::string& hex) {
    std::vector<uint8_t> result;
    result.reserve(hex.size() / 2);
    for (size_t i = 0; i < hex.size(); i += 2) {
        auto byte_str = hex.substr(i, 2);
        result.push_back(static_cast<uint8_t>(strtol(byte_str.c_str(), nullptr, 16)));
    }
    return result;
}

std::string bytes_to_hex(const std::vector<uint8_t>& data) {
    std::string result;
    result.reserve(data.size() * 2);
    for (uint8_t b : data) {
        char buf[3];
        snprintf(buf, sizeof(buf), "%02x", b);
        result += buf;
    }
    return result;
}

// --------------------------------------------------------------------------
// SHA-256 Tests
// --------------------------------------------------------------------------

TEST(SHA256, EmptyString) {
    auto data = to_bytes("");
    auto hash = surge::crypto::sha256_hex(data);
    EXPECT_EQ(hash, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
}

TEST(SHA256, Abc) {
    auto data = to_bytes("abc");
    auto hash = surge::crypto::sha256_hex(data);
    EXPECT_EQ(hash, "ba7816bf8f01cfea414140de5dae2223b0361a396177a9cb410ff61f20015ad");
}

TEST(SHA256, LongerMessage) {
    // "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"
    auto data = to_bytes("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq");
    auto hash = surge::crypto::sha256_hex(data);
    EXPECT_EQ(hash, "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1");
}

TEST(SHA256, RawReturns32Bytes) {
    auto data = to_bytes("test");
    auto raw = surge::crypto::sha256_raw(data);
    EXPECT_EQ(raw.size(), 32u);
}

TEST(SHA256, HexIs64Chars) {
    auto data = to_bytes("test");
    auto hex = surge::crypto::sha256_hex(data);
    EXPECT_EQ(hex.size(), 64u);
}

TEST(SHA256, RawAndHexConsistent) {
    auto data = to_bytes("consistency check");
    auto raw = surge::crypto::sha256_raw(data);
    auto hex = surge::crypto::sha256_hex(data);
    EXPECT_EQ(bytes_to_hex(raw), hex);
}

TEST(SHA256, DifferentInputsDifferentHashes) {
    auto hash1 = surge::crypto::sha256_hex(to_bytes("hello"));
    auto hash2 = surge::crypto::sha256_hex(to_bytes("world"));
    EXPECT_NE(hash1, hash2);
}

TEST(SHA256, SameInputSameHash) {
    auto data = to_bytes("deterministic");
    auto hash1 = surge::crypto::sha256_hex(data);
    auto hash2 = surge::crypto::sha256_hex(data);
    EXPECT_EQ(hash1, hash2);
}

// --------------------------------------------------------------------------
// HMAC-SHA256 Tests (RFC 4231 Test Vectors)
// --------------------------------------------------------------------------

TEST(HMAC_SHA256, RFC4231_TestCase1) {
    // Test Case 1: HMAC-SHA-256 with 20-byte key
    // Key  = 0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b (20 bytes)
    // Data = "Hi There"
    auto key = hex_to_bytes("0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b");
    auto data = to_bytes("Hi There");

    auto result = surge::crypto::hmac_sha256_hex(key, data);
    EXPECT_EQ(result, "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7");
}

TEST(HMAC_SHA256, RFC4231_TestCase2) {
    // Test Case 2: HMAC-SHA-256 with key "Jefe"
    // Key  = "Jefe"
    // Data = "what do ya want for nothing?"
    auto key = to_bytes("Jefe");
    auto data = to_bytes("what do ya want for nothing?");

    auto result = surge::crypto::hmac_sha256_hex(key, data);
    EXPECT_EQ(result, "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843");
}

TEST(HMAC_SHA256, RFC4231_TestCase3) {
    // Test Case 3: HMAC-SHA-256 with 20-byte key of 0xaa
    // Key  = aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (20 bytes of 0xaa)
    // Data = dddddddddd... (50 bytes of 0xdd)
    std::vector<uint8_t> key(20, 0xaa);
    std::vector<uint8_t> data(50, 0xdd);

    auto result = surge::crypto::hmac_sha256_hex(key, data);
    EXPECT_EQ(result, "773ea91e36800e46854db8ebd09181a72959098b3ef8c122d9635514ced565fe");
}

TEST(HMAC_SHA256, RFC4231_TestCase4) {
    // Test Case 4
    // Key  = 0102030405060708090a0b0c0d0e0f10111213141516171819 (25 bytes)
    // Data = cdcdcdcd... (50 bytes of 0xcd)
    auto key = hex_to_bytes("0102030405060708090a0b0c0d0e0f10111213141516171819");
    std::vector<uint8_t> data(50, 0xcd);

    auto result = surge::crypto::hmac_sha256_hex(key, data);
    EXPECT_EQ(result, "82558a389a443c0ea4cc819899f2083a85f0faa3e578f8077a2e3ff46729665b");
}

TEST(HMAC_SHA256, RawAndHexConsistent) {
    auto key = to_bytes("secret");
    auto data = to_bytes("message");

    auto raw = surge::crypto::hmac_sha256(key, data);
    auto hex = surge::crypto::hmac_sha256_hex(key, data);

    EXPECT_EQ(raw.size(), 32u);
    EXPECT_EQ(bytes_to_hex(raw), hex);
}

TEST(HMAC_SHA256, DifferentKeysDifferentResults) {
    auto data = to_bytes("same data");
    auto result1 = surge::crypto::hmac_sha256_hex(to_bytes("key1"), data);
    auto result2 = surge::crypto::hmac_sha256_hex(to_bytes("key2"), data);
    EXPECT_NE(result1, result2);
}

TEST(HMAC_SHA256, EmptyData) {
    auto key = to_bytes("key");
    auto data = to_bytes("");
    auto result = surge::crypto::hmac_sha256_hex(key, data);
    // Just verify it produces valid 64-char hex
    EXPECT_EQ(result.size(), 64u);
}

} // anonymous namespace
