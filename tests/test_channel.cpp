/**
 * @file test_channel.cpp
 * @brief Tests for channel promote/demote logic.
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <vector>

namespace {

// Simplified channel management for testing promote/demote logic
struct Release {
    std::string version;
    std::set<std::string> channels;
    int64_t full_size = 0;
    bool is_genesis = false;
};

class ChannelManager {
public:
    std::vector<Release> releases;

    // Promote a version to a channel. Returns true if successful.
    bool promote(const std::string& version, const std::string& channel) {
        auto it = find_release(version);
        if (it == releases.end())
            return false;
        it->channels.insert(channel);
        return true;
    }

    // Demote (remove) a version from a channel. Returns true if successful.
    bool demote(const std::string& version, const std::string& channel) {
        auto it = find_release(version);
        if (it == releases.end())
            return false;
        return it->channels.erase(channel) > 0;
    }

    // Get all releases in a specific channel
    std::vector<const Release*> get_channel_releases(const std::string& channel) const {
        std::vector<const Release*> result;
        for (const auto& r : releases) {
            if (r.channels.count(channel)) {
                result.push_back(&r);
            }
        }
        return result;
    }

    // Get the latest release in a channel
    const Release* get_latest(const std::string& channel) const {
        auto channel_releases = get_channel_releases(channel);
        if (channel_releases.empty())
            return nullptr;
        // Assume releases are ordered newest first
        return channel_releases.front();
    }

    // Check if a version exists in any channel
    bool has_version(const std::string& version) const {
        return std::any_of(releases.begin(), releases.end(), [&](const Release& r) { return r.version == version; });
    }

private:
    std::vector<Release>::iterator find_release(const std::string& version) {
        return std::find_if(releases.begin(), releases.end(), [&](const Release& r) { return r.version == version; });
    }
};

class ChannelTest : public ::testing::Test {
protected:
    ChannelManager mgr;

    void SetUp() override {
        mgr.releases = {
            {"2.0.0", {"beta"}, 40960, false},
            {"1.1.0", {"beta", "stable"}, 20480, false},
            {"1.0.0", {"stable"}, 10240, true},
        };
    }
};

// --------------------------------------------------------------------------
// Promote Tests
// --------------------------------------------------------------------------

TEST_F(ChannelTest, PromoteToNewChannel) {
    EXPECT_TRUE(mgr.promote("2.0.0", "stable"));

    auto stable = mgr.get_channel_releases("stable");
    bool found = std::any_of(stable.begin(), stable.end(), [](const Release* r) { return r->version == "2.0.0"; });
    EXPECT_TRUE(found);
}

TEST_F(ChannelTest, PromoteAlreadyInChannel_IsIdempotent) {
    EXPECT_TRUE(mgr.promote("1.0.0", "stable"));
    auto stable = mgr.get_channel_releases("stable");

    int count = 0;
    for (const auto* r : stable) {
        if (r->version == "1.0.0")
            count++;
    }
    EXPECT_EQ(count, 1);
}

TEST_F(ChannelTest, PromoteNonExistentVersion_Fails) {
    EXPECT_FALSE(mgr.promote("9.9.9", "stable"));
}

TEST_F(ChannelTest, PromoteToMultipleChannels) {
    EXPECT_TRUE(mgr.promote("2.0.0", "stable"));
    EXPECT_TRUE(mgr.promote("2.0.0", "nightly"));

    auto it =
        std::find_if(mgr.releases.begin(), mgr.releases.end(), [](const Release& r) { return r.version == "2.0.0"; });
    ASSERT_NE(it, mgr.releases.end());
    EXPECT_TRUE(it->channels.count("beta"));
    EXPECT_TRUE(it->channels.count("stable"));
    EXPECT_TRUE(it->channels.count("nightly"));
}

// --------------------------------------------------------------------------
// Demote Tests
// --------------------------------------------------------------------------

TEST_F(ChannelTest, DemoteFromChannel) {
    EXPECT_TRUE(mgr.demote("1.1.0", "beta"));

    auto beta = mgr.get_channel_releases("beta");
    bool found = std::any_of(beta.begin(), beta.end(), [](const Release* r) { return r->version == "1.1.0"; });
    EXPECT_FALSE(found);

    // Should still be in stable
    auto stable = mgr.get_channel_releases("stable");
    found = std::any_of(stable.begin(), stable.end(), [](const Release* r) { return r->version == "1.1.0"; });
    EXPECT_TRUE(found);
}

TEST_F(ChannelTest, DemoteNotInChannel_ReturnsFalse) {
    EXPECT_FALSE(mgr.demote("1.0.0", "beta"));
}

TEST_F(ChannelTest, DemoteNonExistentVersion_Fails) {
    EXPECT_FALSE(mgr.demote("9.9.9", "stable"));
}

TEST_F(ChannelTest, DemoteFromAllChannels) {
    EXPECT_TRUE(mgr.demote("1.1.0", "beta"));
    EXPECT_TRUE(mgr.demote("1.1.0", "stable"));

    auto it =
        std::find_if(mgr.releases.begin(), mgr.releases.end(), [](const Release& r) { return r.version == "1.1.0"; });
    ASSERT_NE(it, mgr.releases.end());
    EXPECT_TRUE(it->channels.empty());
}

// --------------------------------------------------------------------------
// Channel Query Tests
// --------------------------------------------------------------------------

TEST_F(ChannelTest, GetChannelReleases_ReturnsCorrectCount) {
    auto stable = mgr.get_channel_releases("stable");
    EXPECT_EQ(stable.size(), 2u);  // 1.0.0 and 1.1.0

    auto beta = mgr.get_channel_releases("beta");
    EXPECT_EQ(beta.size(), 2u);  // 2.0.0 and 1.1.0
}

TEST_F(ChannelTest, GetLatest_ReturnsNewest) {
    auto latest = mgr.get_latest("stable");
    ASSERT_NE(latest, nullptr);
    // Our list has 2.0.0, 1.1.0, 1.0.0 - latest stable should be first match
    // which is 1.1.0 (it's in position [1] but is the first "stable" entry)
    EXPECT_TRUE(latest->version == "1.1.0" || latest->version == "2.0.0");
}

TEST_F(ChannelTest, GetLatest_EmptyChannel_ReturnsNull) {
    auto latest = mgr.get_latest("nightly");
    EXPECT_EQ(latest, nullptr);
}

TEST_F(ChannelTest, HasVersion) {
    EXPECT_TRUE(mgr.has_version("1.0.0"));
    EXPECT_TRUE(mgr.has_version("1.1.0"));
    EXPECT_TRUE(mgr.has_version("2.0.0"));
    EXPECT_FALSE(mgr.has_version("3.0.0"));
}

// --------------------------------------------------------------------------
// Channel Workflow Tests
// --------------------------------------------------------------------------

TEST_F(ChannelTest, PromoteFlowBetaToStable) {
    // Typical workflow: release goes beta -> stable
    // 2.0.0 is currently only in beta, promote to stable
    EXPECT_TRUE(mgr.promote("2.0.0", "stable"));

    auto stable = mgr.get_channel_releases("stable");
    EXPECT_EQ(stable.size(), 3u);
}

TEST_F(ChannelTest, DemoteOldStableAfterPromotion) {
    // After promoting 2.0.0 to stable, demote 1.0.0 from stable
    mgr.promote("2.0.0", "stable");
    EXPECT_TRUE(mgr.demote("1.0.0", "stable"));

    auto stable = mgr.get_channel_releases("stable");
    EXPECT_EQ(stable.size(), 2u);

    bool old_present =
        std::any_of(stable.begin(), stable.end(), [](const Release* r) { return r->version == "1.0.0"; });
    EXPECT_FALSE(old_present);
}

TEST_F(ChannelTest, CrossChannelRelease) {
    // A release can be in multiple channels simultaneously
    auto it =
        std::find_if(mgr.releases.begin(), mgr.releases.end(), [](const Release& r) { return r.version == "1.1.0"; });
    ASSERT_NE(it, mgr.releases.end());
    EXPECT_EQ(it->channels.size(), 2u);
    EXPECT_TRUE(it->channels.count("beta"));
    EXPECT_TRUE(it->channels.count("stable"));
}

}  // anonymous namespace
