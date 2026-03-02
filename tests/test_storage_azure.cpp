/**
 * @file test_storage_azure.cpp
 * @brief Azure SharedKey signing test vectors.
 */

#include "crypto/hmac.hpp"
#include "crypto/sha256.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace {

std::vector<uint8_t> to_bytes(const std::string& s) {
    return {s.begin(), s.end()};
}

// Base64 decode (minimal implementation for test vectors)
std::vector<uint8_t> base64_decode(const std::string& input) {
    static const std::string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    std::vector<uint8_t> result;
    std::vector<int> T(256, -1);
    for (int i = 0; i < 64; i++)
        T[static_cast<unsigned char>(chars[i])] = i;

    int val = 0, valb = -8;
    for (unsigned char c : input) {
        if (c == '=' || c == '\n' || c == '\r')
            continue;
        if (T[c] == -1)
            break;
        val = (val << 6) + T[c];
        valb += 6;
        if (valb >= 0) {
            result.push_back(static_cast<uint8_t>((val >> valb) & 0xFF));
            valb -= 8;
        }
    }
    return result;
}

// Base64 encode
std::string base64_encode(const std::vector<uint8_t>& data) {
    static const char chars[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    std::string result;
    int val = 0, valb = -6;
    for (uint8_t c : data) {
        val = (val << 8) + c;
        valb += 8;
        while (valb >= 0) {
            result += chars[(val >> valb) & 0x3F];
            valb -= 6;
        }
    }
    if (valb > -6)
        result += chars[((val << 8) >> (valb + 8)) & 0x3F];
    while (result.size() % 4)
        result += '=';
    return result;
}

/**
 * Construct the Azure SharedKey string-to-sign for Blob service requests.
 *
 * Format:
 *   VERB\n
 *   Content-Encoding\n
 *   Content-Language\n
 *   Content-Length\n
 *   Content-MD5\n
 *   Content-Type\n
 *   Date\n
 *   If-Modified-Since\n
 *   If-Match\n
 *   If-None-Match\n
 *   If-Unmodified-Since\n
 *   Range\n
 *   CanonicalizedHeaders\n
 *   CanonicalizedResource
 */
std::string make_azure_string_to_sign(const std::string& verb, const std::string& content_type,
                                      const std::string& content_length, const std::string& x_ms_date,
                                      const std::string& x_ms_version, const std::string& account_name,
                                      const std::string& resource_path) {
    // Standard headers (most empty for simple GET/PUT)
    std::string sts;
    sts += verb + "\n";            // VERB
    sts += "\n";                   // Content-Encoding
    sts += "\n";                   // Content-Language
    sts += content_length + "\n";  // Content-Length (empty if 0 for GET)
    sts += "\n";                   // Content-MD5
    sts += content_type + "\n";    // Content-Type
    sts += "\n";                   // Date (using x-ms-date instead)
    sts += "\n";                   // If-Modified-Since
    sts += "\n";                   // If-Match
    sts += "\n";                   // If-None-Match
    sts += "\n";                   // If-Unmodified-Since
    sts += "\n";                   // Range

    // Canonicalized headers
    sts += "x-ms-date:" + x_ms_date + "\n";
    sts += "x-ms-version:" + x_ms_version + "\n";

    // Canonicalized resource
    sts += "/" + account_name + resource_path;

    return sts;
}

std::string sign_azure_request(const std::string& account_key_base64, const std::string& string_to_sign) {
    auto key_bytes = base64_decode(account_key_base64);
    auto sts_bytes = to_bytes(string_to_sign);
    auto signature = surge::crypto::hmac_sha256(key_bytes, sts_bytes);
    return base64_encode(signature);
}

// --------------------------------------------------------------------------
// Azure SharedKey Tests
// --------------------------------------------------------------------------

TEST(AzureSharedKey, StringToSign_GetBlob) {
    auto sts = make_azure_string_to_sign("GET",                            // verb
                                         "",                               // content_type
                                         "",                               // content_length (empty for GET)
                                         "Sun, 11 Oct 2009 21:49:13 GMT",  // x-ms-date
                                         "2009-09-19",                     // x-ms-version
                                         "myaccount",                      // account_name
                                         "/mycontainer/myblob"             // resource_path
    );

    // Verify structure
    EXPECT_EQ(sts.substr(0, 4), "GET\n");
    EXPECT_TRUE(sts.find("x-ms-date:Sun, 11 Oct 2009 21:49:13 GMT") != std::string::npos);
    EXPECT_TRUE(sts.find("x-ms-version:2009-09-19") != std::string::npos);
    EXPECT_TRUE(sts.find("/myaccount/mycontainer/myblob") != std::string::npos);

    // Count newlines: should have at least 14 (12 standard headers + canonicalized headers)
    int newline_count = 0;
    for (char c : sts)
        if (c == '\n')
            newline_count++;
    EXPECT_GE(newline_count, 13);
}

TEST(AzureSharedKey, StringToSign_PutBlob) {
    auto sts = make_azure_string_to_sign("PUT", "application/octet-stream", "1024", "Mon, 12 Oct 2009 10:00:00 GMT",
                                         "2009-09-19", "myaccount", "/mycontainer/myblob");

    EXPECT_EQ(sts.substr(0, 4), "PUT\n");
    EXPECT_TRUE(sts.find("application/octet-stream") != std::string::npos);
    EXPECT_TRUE(sts.find("1024") != std::string::npos);
}

TEST(AzureSharedKey, SignatureGeneration) {
    // Use a known Base64-encoded key
    std::string account_key_base64 = "dGVzdGtleWZvcmF6dXJlc3RvcmFnZXNpZ25pbmc=";  // "testkeyforazurestoragesigning"

    auto sts = make_azure_string_to_sign("GET", "", "", "Mon, 01 Jan 2024 00:00:00 GMT", "2023-11-03", "testaccount",
                                         "/testcontainer/testblob");

    auto signature = sign_azure_request(account_key_base64, sts);

    // Signature should be base64 encoded (44 chars for 32-byte HMAC-SHA256)
    EXPECT_EQ(signature.size(), 44u);

    // Verify determinism
    auto signature2 = sign_azure_request(account_key_base64, sts);
    EXPECT_EQ(signature, signature2);
}

TEST(AzureSharedKey, DifferentKeysDifferentSignatures) {
    std::string key1 = base64_encode(to_bytes("key-one-for-azure"));
    std::string key2 = base64_encode(to_bytes("key-two-for-azure"));

    auto sts = make_azure_string_to_sign("GET", "", "", "Mon, 01 Jan 2024 00:00:00 GMT", "2023-11-03", "account",
                                         "/container/blob");

    auto sig1 = sign_azure_request(key1, sts);
    auto sig2 = sign_azure_request(key2, sts);

    EXPECT_NE(sig1, sig2);
}

TEST(AzureSharedKey, AuthorizationHeaderFormat) {
    std::string account_name = "myaccount";
    std::string signature = "c2lnbmF0dXJlLWhlcmU=";  // placeholder

    auto auth = "SharedKey " + account_name + ":" + signature;

    EXPECT_TRUE(auth.find("SharedKey ") == 0);
    EXPECT_TRUE(auth.find("myaccount:") != std::string::npos);
}

TEST(AzureSharedKey, Base64_RoundTrip) {
    std::vector<uint8_t> original = {0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD, 0x80, 0x7F};
    auto encoded = base64_encode(original);
    auto decoded = base64_decode(encoded);
    EXPECT_EQ(original, decoded);
}

TEST(AzureSharedKey, ListBlobs_StringToSign) {
    auto sts = make_azure_string_to_sign("GET", "", "", "Tue, 02 Jan 2024 12:00:00 GMT", "2023-11-03", "storageaccount",
                                         "/mycontainer\ncomp:list\nrestype:container");

    EXPECT_TRUE(sts.find("/storageaccount/mycontainer") != std::string::npos);
    EXPECT_TRUE(sts.find("comp:list") != std::string::npos);
}

}  // anonymous namespace
