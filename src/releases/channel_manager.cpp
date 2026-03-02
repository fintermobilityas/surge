/**
 * @file channel_manager.cpp
 * @brief Release channel management (promote / demote).
 */

#include "releases/channel_manager.hpp"
#include "releases/release_manifest.hpp"
#include "storage/storage_backend.hpp"
#include "config/constants.hpp"
#include "crypto/sha256.hpp"
#include <spdlog/spdlog.h>
#include <fmt/format.h>
#include <algorithm>
#include <stdexcept>

namespace surge::releases {

struct ChannelManager::Impl {
    std::shared_ptr<storage::IStorageBackend> storage;
    std::string app_id;
};

ChannelManager::ChannelManager(std::shared_ptr<storage::IStorageBackend> storage,
                               const std::string& app_id)
    : impl_(std::make_unique<Impl>())
{
    impl_->storage = std::move(storage);
    impl_->app_id = app_id;
}

ChannelManager::~ChannelManager() = default;
ChannelManager::ChannelManager(ChannelManager&&) noexcept = default;
ChannelManager& ChannelManager::operator=(ChannelManager&&) noexcept = default;

int32_t ChannelManager::promote(const std::string& version,
                                 const std::string& target_channel) {
    spdlog::info("Promoting {} to channel '{}'", version, target_channel);

    // Download and parse current release index
    ReleaseIndex index;
    auto rc = load_index(index);
    if (rc != SURGE_OK) return rc;

    // Validate the target channel exists
    auto ch_it = std::find(index.channels.begin(), index.channels.end(), target_channel);
    if (ch_it == index.channels.end()) {
        spdlog::error("Channel '{}' does not exist in release index", target_channel);
        return SURGE_ERROR;
    }

    // Find the release
    bool found = false;
    for (auto& rel : index.releases) {
        if (rel.version == version) {
            if (rel.channel == target_channel) {
                spdlog::warn("Release {} is already on channel '{}'", version, target_channel);
                return SURGE_OK;
            }
            // Create a new entry for this channel (copy the release to the new channel)
            ReleaseEntry promoted = rel;
            promoted.channel = target_channel;
            index.releases.push_back(std::move(promoted));
            found = true;
            break;
        }
    }

    if (!found) {
        spdlog::error("Release {} not found in index", version);
        return SURGE_NOT_FOUND;
    }

    // Save the updated index
    rc = save_index(index);
    if (rc != SURGE_OK) return rc;

    spdlog::info("Successfully promoted {} to channel '{}'", version, target_channel);
    return SURGE_OK;
}

int32_t ChannelManager::demote(const std::string& version,
                                const std::string& channel) {
    spdlog::info("Demoting {} from channel '{}'", version, channel);

    ReleaseIndex index;
    auto rc = load_index(index);
    if (rc != SURGE_OK) return rc;

    // Remove the release entry for this version+channel combination
    auto it = std::remove_if(index.releases.begin(), index.releases.end(),
        [&](const ReleaseEntry& rel) {
            return rel.version == version && rel.channel == channel;
        });

    if (it == index.releases.end()) {
        spdlog::warn("Release {} not found on channel '{}'", version, channel);
        return SURGE_NOT_FOUND;
    }

    index.releases.erase(it, index.releases.end());

    rc = save_index(index);
    if (rc != SURGE_OK) return rc;

    spdlog::info("Successfully demoted {} from channel '{}'", version, channel);
    return SURGE_OK;
}

int32_t ChannelManager::add_channel(const std::string& channel_name) {
    spdlog::info("Adding channel '{}'", channel_name);

    ReleaseIndex index;
    auto rc = load_index(index);
    if (rc != SURGE_OK) return rc;

    auto it = std::find(index.channels.begin(), index.channels.end(), channel_name);
    if (it != index.channels.end()) {
        spdlog::warn("Channel '{}' already exists", channel_name);
        return SURGE_OK;
    }

    index.channels.push_back(channel_name);

    rc = save_index(index);
    if (rc != SURGE_OK) return rc;

    spdlog::info("Added channel '{}'", channel_name);
    return SURGE_OK;
}

int32_t ChannelManager::remove_channel(const std::string& channel_name) {
    spdlog::info("Removing channel '{}'", channel_name);

    ReleaseIndex index;
    auto rc = load_index(index);
    if (rc != SURGE_OK) return rc;

    auto it = std::find(index.channels.begin(), index.channels.end(), channel_name);
    if (it == index.channels.end()) {
        spdlog::warn("Channel '{}' does not exist", channel_name);
        return SURGE_NOT_FOUND;
    }

    // Remove the channel and all releases on it
    index.channels.erase(it);
    std::erase_if(index.releases,
        [&](const ReleaseEntry& rel) { return rel.channel == channel_name; });

    rc = save_index(index);
    if (rc != SURGE_OK) return rc;

    spdlog::info("Removed channel '{}' and its releases", channel_name);
    return SURGE_OK;
}

int32_t ChannelManager::load_index(ReleaseIndex& index) {
    std::string key = fmt::format("{}/{}", impl_->app_id, constants::RELEASES_FILE_COMPRESSED);

    std::vector<uint8_t> compressed;
    auto rc = impl_->storage->get_object(key, compressed);
    if (rc == SURGE_NOT_FOUND) {
        // No index yet - start fresh
        index.app_id = impl_->app_id;
        index.schema = constants::MANIFEST_SCHEMA_VERSION;
        return SURGE_OK;
    }
    if (rc != SURGE_OK) {
        spdlog::error("Failed to download release index");
        return rc;
    }

    auto yaml_data = decompress_release_index(compressed);
    index = parse_release_index(yaml_data);
    return SURGE_OK;
}

int32_t ChannelManager::save_index(const ReleaseIndex& index) {
    auto yaml_data = serialize_release_index(index);
    auto compressed = compress_release_index(yaml_data);

    std::string key = fmt::format("{}/{}", impl_->app_id, constants::RELEASES_FILE_COMPRESSED);
    auto rc = impl_->storage->put_object(key, compressed, "application/octet-stream");
    if (rc != SURGE_OK) {
        spdlog::error("Failed to upload release index");
        return rc;
    }

    // Also upload the checksum
    auto hash = crypto::sha256_hex(compressed);
    std::string checksum_key = fmt::format("{}/{}", impl_->app_id, constants::RELEASES_CHECKSUM_FILE);
    std::vector<uint8_t> hash_bytes(hash.begin(), hash.end());
    rc = impl_->storage->put_object(checksum_key, hash_bytes, "text/plain");
    if (rc != SURGE_OK) {
        spdlog::error("Failed to upload release checksum");
        return rc;
    }

    spdlog::debug("Saved release index: {} releases", index.releases.size());
    return SURGE_OK;
}

} // namespace surge::releases
