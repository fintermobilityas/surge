/**
 * @file test_storage_gcs.cpp
 * @brief Google Cloud Storage HMAC signing tests.
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "crypto/hmac.hpp"
#include "crypto/sha256.hpp"

namespace {

std::vector<uint8_t> to_bytes(const std::string& s) {
    return {s.begin(), s.end()};
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

/**
 * GCS HMAC signing uses the same SigV4-like process as AWS S3 interop.
 * The primary difference is the service name and endpoint.
 */
namespace gcs_sigv4 {

std::string payload_hash(std::span<const uint8_t> payload) {
    return surge::crypto::sha256_hex(payload);
}

std::string make_canonical_request(
    const std::string& method,
    const std::string& uri,
    const std::string& query_string,
    const std::vector<std::pair<std::string, std::string>>& headers,
    const std::string& signed_headers,
    const std::string& payload_hash_hex)
{
    std::string result;
    result += method + "\n";
    result += uri + "\n";
    result += query_string + "\n";

    for (const auto& [name, value] : headers) {
        result += name + ":" + value + "\n";
    }
    result += "\n";

    result += signed_headers + "\n";
    result += payload_hash_hex;
    return result;
}

std::vector<uint8_t> derive_signing_key(
    const std::string& secret_key,
    const std::string& datestamp,
    const std::string& region,
    const std::string& service = "storage")
{
    // GCS HMAC uses the same key derivation as AWS SigV4
    auto k_date = surge::crypto::hmac_sha256(
        to_bytes("GOOG4" + secret_key), to_bytes(datestamp));
    auto k_region = surge::crypto::hmac_sha256(k_date, to_bytes(region));
    auto k_service = surge::crypto::hmac_sha256(k_region, to_bytes(service));
    auto k_signing = surge::crypto::hmac_sha256(k_service, to_bytes("goog4_request"));
    return k_signing;
}

std::string make_string_to_sign(
    const std::string& timestamp,
    const std::string& scope,
    const std::string& canonical_request_hash)
{
    return "GOOG4-HMAC-SHA256\n" + timestamp + "\n" + scope + "\n" + canonical_request_hash;
}

std::string make_authorization_header(
    const std::string& access_key,
    const std::string& scope,
    const std::string& signed_headers,
    const std::string& signature)
{
    return "GOOG4-HMAC-SHA256 Credential=" + access_key + "/" + scope +
           ", SignedHeaders=" + signed_headers +
           ", Signature=" + signature;
}

} // namespace gcs_sigv4

// --------------------------------------------------------------------------
// GCS HMAC Signing Tests
// --------------------------------------------------------------------------

TEST(GcsHmac, SigningKeyDerivation) {
    std::string secret_key = "EXAMPLE_GCS_SECRET_KEY_1234567890";
    std::string datestamp = "20240101";
    std::string region = "auto";

    auto key = gcs_sigv4::derive_signing_key(secret_key, datestamp, region);
    EXPECT_EQ(key.size(), 32u);

    // Verify determinism
    auto key2 = gcs_sigv4::derive_signing_key(secret_key, datestamp, region);
    EXPECT_EQ(key, key2);

    // Different date produces different key
    auto key3 = gcs_sigv4::derive_signing_key(secret_key, "20240102", region);
    EXPECT_NE(key, key3);
}

TEST(GcsHmac, CanonicalRequest_GetObject) {
    std::string p_hash = gcs_sigv4::payload_hash({});
    EXPECT_EQ(p_hash, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");

    std::vector<std::pair<std::string, std::string>> headers = {
        {"host", "storage.googleapis.com"},
        {"x-goog-content-sha256", p_hash},
        {"x-goog-date", "20240101T000000Z"},
    };

    auto canonical = gcs_sigv4::make_canonical_request(
        "GET",
        "/my-bucket/my-object",
        "",
        headers,
        "host;x-goog-content-sha256;x-goog-date",
        p_hash);

    EXPECT_TRUE(canonical.find("GET") == 0);
    EXPECT_TRUE(canonical.find("/my-bucket/my-object") != std::string::npos);
    EXPECT_TRUE(canonical.find("storage.googleapis.com") != std::string::npos);
}

TEST(GcsHmac, CanonicalRequest_PutObject) {
    auto data = to_bytes("file contents");
    std::string p_hash = gcs_sigv4::payload_hash(data);

    std::vector<std::pair<std::string, std::string>> headers = {
        {"content-type", "application/octet-stream"},
        {"host", "storage.googleapis.com"},
        {"x-goog-content-sha256", p_hash},
        {"x-goog-date", "20240101T120000Z"},
    };

    auto canonical = gcs_sigv4::make_canonical_request(
        "PUT",
        "/my-bucket/uploads/file.bin",
        "",
        headers,
        "content-type;host;x-goog-content-sha256;x-goog-date",
        p_hash);

    EXPECT_TRUE(canonical.find("PUT") == 0);
    EXPECT_TRUE(canonical.find("application/octet-stream") != std::string::npos);
}

TEST(GcsHmac, StringToSign) {
    std::string timestamp = "20240101T000000Z";
    std::string scope = "20240101/auto/storage/goog4_request";
    std::string canonical_hash = surge::crypto::sha256_hex(to_bytes("canonical request content"));

    auto sts = gcs_sigv4::make_string_to_sign(timestamp, scope, canonical_hash);

    EXPECT_TRUE(sts.find("GOOG4-HMAC-SHA256") == 0);
    EXPECT_TRUE(sts.find(timestamp) != std::string::npos);
    EXPECT_TRUE(sts.find(scope) != std::string::npos);
    EXPECT_TRUE(sts.find(canonical_hash) != std::string::npos);
}

TEST(GcsHmac, AuthorizationHeaderFormat) {
    std::string access_key = "GOOGEXAMPLEACCESSKEY";
    std::string scope = "20240101/auto/storage/goog4_request";
    std::string signed_headers = "host;x-goog-content-sha256;x-goog-date";
    std::string signature = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";

    auto auth = gcs_sigv4::make_authorization_header(
        access_key, scope, signed_headers, signature);

    EXPECT_TRUE(auth.find("GOOG4-HMAC-SHA256") == 0);
    EXPECT_TRUE(auth.find("Credential=GOOGEXAMPLEACCESSKEY/") != std::string::npos);
    EXPECT_TRUE(auth.find("SignedHeaders=host;x-goog-content-sha256;x-goog-date") != std::string::npos);
    EXPECT_TRUE(auth.find("Signature=") != std::string::npos);
}

TEST(GcsHmac, FullSigningFlow) {
    std::string access_key = "GOOGEXAMPLEACCESSKEY";
    std::string secret_key = "GOOGEXAMPLESECRETKEY1234567890AB";
    std::string datestamp = "20240101";
    std::string timestamp = "20240101T000000Z";
    std::string region = "auto";

    // 1. Payload hash
    auto p_hash = gcs_sigv4::payload_hash({});

    // 2. Canonical request
    std::vector<std::pair<std::string, std::string>> headers = {
        {"host", "storage.googleapis.com"},
        {"x-goog-content-sha256", p_hash},
        {"x-goog-date", timestamp},
    };
    std::string signed_headers = "host;x-goog-content-sha256;x-goog-date";

    auto canonical = gcs_sigv4::make_canonical_request(
        "GET", "/bucket/object.txt", "", headers, signed_headers, p_hash);
    auto canonical_hash = surge::crypto::sha256_hex(to_bytes(canonical));

    // 3. String to sign
    std::string scope = datestamp + "/" + region + "/storage/goog4_request";
    auto sts = gcs_sigv4::make_string_to_sign(timestamp, scope, canonical_hash);

    // 4. Signing key
    auto signing_key = gcs_sigv4::derive_signing_key(secret_key, datestamp, region);

    // 5. Signature
    auto signature = surge::crypto::hmac_sha256_hex(signing_key, to_bytes(sts));
    EXPECT_EQ(signature.size(), 64u);

    // 6. Authorization header
    auto auth = gcs_sigv4::make_authorization_header(
        access_key, scope, signed_headers, signature);
    EXPECT_TRUE(auth.find("GOOG4-HMAC-SHA256") == 0);
}

TEST(GcsHmac, DifferentRegions_DifferentKeys) {
    std::string secret = "test_secret_key_for_gcs";
    std::string date = "20240101";

    auto key_auto = gcs_sigv4::derive_signing_key(secret, date, "auto");
    auto key_us = gcs_sigv4::derive_signing_key(secret, date, "us-central1");

    EXPECT_NE(key_auto, key_us);
}

} // anonymous namespace
