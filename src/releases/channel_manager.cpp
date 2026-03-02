/**
 * @file channel_manager.cpp
 * @brief Release channel management (promote / demote).
 */

#include "releases/channel_manager.hpp"
#include "releases/release_manifest.hpp"
#include "storage/storage_backend.hpp"
#include "core/context.hpp"
#include "config/constants.hpp"
#include "crypto/sha256.hpp"
#include <spdlog/spdlog.h>
#include <fmt/format.h>
#include <algorithm>
#include <stdexcept>

namespace surge::releases {

struct ChannelManager::Impl {
    Context& ctx;
    std::shared_ptr<storage::IStorageBackend> storage;
    std::string app_id;

    explicit Impl(Context& c) : ctx(c) {}

    int32_t load_index(ReleaseIndex& index) {
        std::string key = fmt::format("{}/{}", app_id, constants::RELEASES_FILE_COMPRESSED);

        std::vector<uint8_t> compressed;
        auto rc = storage->get_object(key, compressed);
        if (rc == SURGE_NOT_FOUND) {
            // No index yet - start fresh
            index.app_id = app_id;
            index.schema = constants::MANIFEST_SCHEMA_VERSION;
            return SURGE_OK;
        }
        if (rc != SURGE_OK) {
            spdlog::error("Failed to download release index");
            return rc;
        }

        index = decompress_release_index(compressed);
        return SURGE_OK;
    }

    int32_t save_index(const ReleaseIndex& index) {
        auto compressed = compress_release_index(index);

        std::string key = fmt::format("{}/{}", app_id, constants::RELEASES_FILE_COMPRESSED);
        auto rc = storage->put_object(key, compressed, "application/octet-stream");
        if (rc != SURGE_OK) {
            spdlog::error("Failed to upload release index");
            return rc;
        }

        // Also upload the checksum
        auto hash = crypto::sha256_hex(compressed);
        std::string checksum_key = fmt::format("{}/{}", app_id, constants::RELEASES_CHECKSUM_FILE);
        std::vector<uint8_t> hash_bytes(hash.begin(), hash.end());
        rc = storage->put_object(checksum_key, hash_bytes, "text/plain");
        if (rc != SURGE_OK) {
            spdlog::error("Failed to upload release checksum");
            return rc;
        }

        spdlog::debug("Saved release index: {} releases", index.releases.size());
        return SURGE_OK;
    }
};

ChannelManager::ChannelManager(Context& ctx, const std::string& app_id)
    : impl_(std::make_unique<Impl>(ctx))
{
    impl_->app_id = app_id;
    // Create storage backend from context config
    impl_->storage = std::shared_ptr<storage::IStorageBackend>(
        storage::create_storage_backend(ctx.storage_config()));
}

ChannelManager::~ChannelManager() = default;

int32_t ChannelManager::promote(const std::string& version,
                                 const std::string& source_channel,
                                 const std::string& target_channel) {
    spdlog::info("Promoting {} from channel '{}' to channel '{}'",
                  version, source_channel, target_channel);

    // Download and parse current release index
    ReleaseIndex index;
    auto rc = impl_->load_index(index);
    if (rc != SURGE_OK) return rc;

    // Find the release on source channel
    bool found = false;
    for (auto& rel : index.releases) {
        if (rel.version == version) {
            // Check if it's on the source channel
            bool on_source = source_channel.empty();
            for (auto& ch : rel.channels) {
                if (ch == source_channel) { on_source = true; break; }
            }
            if (!on_source) continue;

            // Check if already on target channel
            for (auto& ch : rel.channels) {
                if (ch == target_channel) {
                    spdlog::warn("Release {} is already on channel '{}'", version, target_channel);
                    return SURGE_OK;
                }
            }

            // Add target channel
            rel.channels.push_back(target_channel);
            found = true;
            break;
        }
    }

    if (!found) {
        spdlog::error("Release {} not found on channel '{}'", version, source_channel);
        return SURGE_NOT_FOUND;
    }

    // Save the updated index
    rc = impl_->save_index(index);
    if (rc != SURGE_OK) return rc;

    spdlog::info("Successfully promoted {} to channel '{}'", version, target_channel);
    return SURGE_OK;
}

int32_t ChannelManager::demote(const std::string& version,
                                const std::string& channel) {
    spdlog::info("Demoting {} from channel '{}'", version, channel);

    ReleaseIndex index;
    auto rc = impl_->load_index(index);
    if (rc != SURGE_OK) return rc;

    // Remove the channel from the release entry
    bool found = false;
    for (auto& rel : index.releases) {
        if (rel.version == version) {
            auto it = std::find(rel.channels.begin(), rel.channels.end(), channel);
            if (it != rel.channels.end()) {
                rel.channels.erase(it);
                found = true;
                break;
            }
        }
    }

    if (!found) {
        spdlog::warn("Release {} not found on channel '{}'", version, channel);
        return SURGE_NOT_FOUND;
    }

    rc = impl_->save_index(index);
    if (rc != SURGE_OK) return rc;

    spdlog::info("Successfully demoted {} from channel '{}'", version, channel);
    return SURGE_OK;
}

std::vector<std::string> ChannelManager::list_channels() {
    ReleaseIndex index;
    auto rc = impl_->load_index(index);
    if (rc != SURGE_OK) return {};

    std::vector<std::string> channels;
    for (auto& rel : index.releases) {
        for (auto& ch : rel.channels) {
            if (std::find(channels.begin(), channels.end(), ch) == channels.end()) {
                channels.push_back(ch);
            }
        }
    }

    std::sort(channels.begin(), channels.end());
    return channels;
}

std::vector<ReleaseEntry> ChannelManager::list_releases(const std::string& channel) {
    ReleaseIndex index;
    auto rc = impl_->load_index(index);
    if (rc != SURGE_OK) return {};

    std::vector<ReleaseEntry> result;
    for (auto& rel : index.releases) {
        bool on_channel = channel.empty();
        for (auto& ch : rel.channels) {
            if (ch == channel) { on_channel = true; break; }
        }
        if (on_channel) {
            result.push_back(rel);
        }
    }

    // Sort newest first
    std::sort(result.begin(), result.end(),
              [](const ReleaseEntry& a, const ReleaseEntry& b) {
                  return compare_versions(a.version, b.version) > 0;
              });

    return result;
}

ReleaseIndex ChannelManager::fetch_index() {
    ReleaseIndex index;
    impl_->load_index(index);
    return index;
}

} // namespace surge::releases
