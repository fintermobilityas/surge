/**
 * @file channel_manager.hpp
 * @brief Manages release channels (promote, demote, list).
 */

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "release_manifest.hpp"

namespace surge {
class Context;
}

namespace surge::releases {

/**
 * Manages release-channel operations on a release index stored in cloud
 * storage. Supports promoting releases between channels (e.g. beta -> stable)
 * and demoting / removing releases from channels.
 */
class ChannelManager {
public:
    /**
     * Construct a channel manager.
     * @param ctx    Surge context with storage and lock configuration.
     * @param app_id Application identifier to scope operations.
     */
    ChannelManager(Context& ctx, const std::string& app_id);
    ~ChannelManager();

    ChannelManager(const ChannelManager&) = delete;
    ChannelManager& operator=(const ChannelManager&) = delete;

    /**
     * Promote a release to an additional channel.
     * @param version         Version string of the release to promote.
     * @param source_channel  Channel where the release currently exists.
     * @param target_channel  Channel to promote to.
     * @return 0 on success, negative error code on failure.
     */
    int32_t promote(const std::string& version,
                    const std::string& source_channel,
                    const std::string& target_channel);

    /**
     * Remove a release from a channel.
     * @param version Version string.
     * @param channel Channel to remove the release from.
     * @return 0 on success, negative error code on failure.
     */
    int32_t demote(const std::string& version,
                   const std::string& channel);

    /**
     * List all distinct channels present in the release index.
     */
    std::vector<std::string> list_channels();

    /**
     * List all releases on a specific channel, ordered newest-first.
     */
    std::vector<ReleaseEntry> list_releases(const std::string& channel);

    /**
     * Fetch and return the current release index from storage.
     */
    ReleaseIndex fetch_index();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace surge::releases
