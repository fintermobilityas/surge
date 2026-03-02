#include "crypto/sha256.hpp"

#include <array>
#include <fmt/format.h>
#include <fstream>
#include <iomanip>
#include <memory>
#include <openssl/evp.h>
#include <sstream>
#include <stdexcept>

namespace surge::crypto {

namespace {

struct EvpMdCtxDeleter {
    void operator()(EVP_MD_CTX* ctx) const {
        EVP_MD_CTX_free(ctx);
    }
};

using EvpMdCtxPtr = std::unique_ptr<EVP_MD_CTX, EvpMdCtxDeleter>;

constexpr size_t SHA256_DIGEST_SIZE = 32;
constexpr size_t FILE_READ_BUFFER_SIZE = 65536;  // 64KB

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

std::vector<uint8_t> compute_sha256(const uint8_t* data, size_t len) {
    EvpMdCtxPtr ctx(EVP_MD_CTX_new());
    if (!ctx) {
        throw std::runtime_error("Failed to create EVP_MD_CTX");
    }

    if (EVP_DigestInit_ex(ctx.get(), EVP_sha256(), nullptr) != 1) {
        throw std::runtime_error("Failed to initialize SHA-256 digest");
    }

    if (EVP_DigestUpdate(ctx.get(), data, len) != 1) {
        throw std::runtime_error("Failed to update SHA-256 digest");
    }

    std::vector<uint8_t> hash(SHA256_DIGEST_SIZE);
    unsigned int hash_len = 0;
    if (EVP_DigestFinal_ex(ctx.get(), hash.data(), &hash_len) != 1) {
        throw std::runtime_error("Failed to finalize SHA-256 digest");
    }

    hash.resize(hash_len);
    return hash;
}

}  // anonymous namespace

std::string sha256_hex(std::span<const uint8_t> data) {
    auto hash = compute_sha256(data.data(), data.size());
    return bytes_to_hex(hash.data(), hash.size());
}

std::string sha256_hex_file(const std::filesystem::path& path, std::function<void(int64_t, int64_t)> progress) {
    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) {
        throw std::runtime_error(fmt::format("Failed to open file for hashing: '{}'", path.string()));
    }

    // Get file size for progress reporting
    file.seekg(0, std::ios::end);
    const auto file_size = static_cast<int64_t>(file.tellg());
    file.seekg(0, std::ios::beg);

    EvpMdCtxPtr ctx(EVP_MD_CTX_new());
    if (!ctx) {
        throw std::runtime_error("Failed to create EVP_MD_CTX");
    }

    if (EVP_DigestInit_ex(ctx.get(), EVP_sha256(), nullptr) != 1) {
        throw std::runtime_error("Failed to initialize SHA-256 digest");
    }

    std::array<char, FILE_READ_BUFFER_SIZE> buffer{};
    int64_t bytes_read_total = 0;

    while (file.good()) {
        file.read(buffer.data(), buffer.size());
        auto bytes_read = file.gcount();
        if (bytes_read > 0) {
            if (EVP_DigestUpdate(ctx.get(), buffer.data(), static_cast<size_t>(bytes_read)) != 1) {
                throw std::runtime_error("Failed to update SHA-256 digest");
            }
            bytes_read_total += bytes_read;
            if (progress) {
                progress(bytes_read_total, file_size);
            }
        }
    }

    std::array<uint8_t, SHA256_DIGEST_SIZE> hash{};
    unsigned int hash_len = 0;
    if (EVP_DigestFinal_ex(ctx.get(), hash.data(), &hash_len) != 1) {
        throw std::runtime_error("Failed to finalize SHA-256 digest");
    }

    return bytes_to_hex(hash.data(), hash_len);
}

std::vector<uint8_t> sha256_raw(std::span<const uint8_t> data) {
    return compute_sha256(data.data(), data.size());
}

}  // namespace surge::crypto
