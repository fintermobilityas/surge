/**
 * @file test_releases.cpp
 * @brief Tests for release index parsing, serialization, and version comparison.
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <string>
#include <unordered_map>
#include <vector>

namespace {

/**
 * Simple semver comparison. Returns:
 *   -1 if a < b
 *    0 if a == b
 *    1 if a > b
 */
struct SemVer {
    int major = 0;
    int minor = 0;
    int patch = 0;

    static SemVer parse(const std::string& s) {
        SemVer v;
        if (sscanf(s.c_str(), "%d.%d.%d", &v.major, &v.minor, &v.patch) < 1)
            return {};
        return v;
    }

    bool operator<(const SemVer& o) const {
        if (major != o.major)
            return major < o.major;
        if (minor != o.minor)
            return minor < o.minor;
        return patch < o.patch;
    }

    bool operator==(const SemVer& o) const {
        return major == o.major && minor == o.minor && patch == o.patch;
    }

    bool operator>(const SemVer& o) const {
        return o < *this;
    }
    bool operator<=(const SemVer& o) const {
        return !(o < *this);
    }
    bool operator>=(const SemVer& o) const {
        return !(*this < o);
    }
    bool operator!=(const SemVer& o) const {
        return !(*this == o);
    }
};

struct ReleaseEntry {
    std::string version;
    std::string channel;
    int64_t full_size = 0;
    int64_t delta_size = 0;
    bool is_genesis = false;
    std::string base_version;  // for delta chain
};

struct ReleaseIndex {
    std::vector<ReleaseEntry> releases;

    void sort_by_version_desc() {
        std::sort(releases.begin(), releases.end(), [](const ReleaseEntry& a, const ReleaseEntry& b) {
            return SemVer::parse(b.version) < SemVer::parse(a.version);
        });
    }

    std::vector<ReleaseEntry> filter_by_channel(const std::string& channel) const {
        std::vector<ReleaseEntry> result;
        for (const auto& r : releases) {
            if (r.channel == channel)
                result.push_back(r);
        }
        return result;
    }

    // Build delta chain from current_version to target_version.
    // Returns versions in apply order, or empty if no chain exists.
    std::vector<std::string> resolve_delta_chain(const std::string& current_version,
                                                 const std::string& target_version) const {
        // Build a map from version -> base_version
        std::unordered_map<std::string, std::string> base_map;
        for (const auto& r : releases) {
            if (!r.base_version.empty()) {
                base_map[r.version] = r.base_version;
            }
        }

        // Walk backwards from target to current
        std::vector<std::string> chain;
        std::string v = target_version;

        while (v != current_version) {
            chain.push_back(v);
            auto it = base_map.find(v);
            if (it == base_map.end())
                return {};  // broken chain
            v = it->second;
            // Safety: prevent infinite loops
            if (chain.size() > releases.size())
                return {};
        }

        std::reverse(chain.begin(), chain.end());
        return chain;
    }
};

TEST(VersionComparison, BasicOrdering) {
    auto v100 = SemVer::parse("1.0.0");
    auto v110 = SemVer::parse("1.1.0");
    auto v200 = SemVer::parse("2.0.0");

    EXPECT_TRUE(v100 < v110);
    EXPECT_TRUE(v110 < v200);
    EXPECT_TRUE(v100 < v200);
    EXPECT_FALSE(v200 < v100);
}

TEST(VersionComparison, EqualVersions) {
    auto a = SemVer::parse("3.2.1");
    auto b = SemVer::parse("3.2.1");
    EXPECT_EQ(a, b);
    EXPECT_FALSE(a < b);
    EXPECT_FALSE(a > b);
}

TEST(VersionComparison, PatchOrdering) {
    auto v100 = SemVer::parse("1.0.0");
    auto v101 = SemVer::parse("1.0.1");
    auto v102 = SemVer::parse("1.0.2");

    EXPECT_TRUE(v100 < v101);
    EXPECT_TRUE(v101 < v102);
}

TEST(VersionComparison, MajorTakesPrecedence) {
    auto v199 = SemVer::parse("1.99.99");
    auto v200 = SemVer::parse("2.0.0");
    EXPECT_TRUE(v199 < v200);
}

TEST(ReleaseIndex, FilterByChannel) {
    ReleaseIndex index;
    index.releases = {
        {"1.0.0", "stable", 1024, 0, true, ""},
        {"1.1.0", "stable", 2048, 512, false, "1.0.0"},
        {"1.1.0-beta.1", "beta", 2048, 0, true, ""},
        {"2.0.0", "stable", 4096, 1024, false, "1.1.0"},
        {"2.0.0-beta.1", "beta", 4096, 512, false, "1.1.0-beta.1"},
    };

    auto stable = index.filter_by_channel("stable");
    EXPECT_EQ(stable.size(), 3u);
    for (const auto& r : stable) {
        EXPECT_EQ(r.channel, "stable");
    }

    auto beta = index.filter_by_channel("beta");
    EXPECT_EQ(beta.size(), 2u);
    for (const auto& r : beta) {
        EXPECT_EQ(r.channel, "beta");
    }

    auto nightly = index.filter_by_channel("nightly");
    EXPECT_TRUE(nightly.empty());
}

TEST(ReleaseIndex, SortByVersionDescending) {
    ReleaseIndex index;
    index.releases = {
        {"1.0.0", "stable", 1024, 0, true, ""},
        {"3.0.0", "stable", 4096, 0, false, ""},
        {"2.0.0", "stable", 2048, 0, false, ""},
        {"1.5.0", "stable", 1536, 0, false, ""},
    };

    index.sort_by_version_desc();

    ASSERT_EQ(index.releases.size(), 4u);
    EXPECT_EQ(index.releases[0].version, "3.0.0");
    EXPECT_EQ(index.releases[1].version, "2.0.0");
    EXPECT_EQ(index.releases[2].version, "1.5.0");
    EXPECT_EQ(index.releases[3].version, "1.0.0");
}

TEST(ReleaseIndex, DeltaChainResolution) {
    ReleaseIndex index;
    index.releases = {
        {"1.0.0", "stable", 1024, 0, true, ""},
        {"1.1.0", "stable", 2048, 256, false, "1.0.0"},
        {"1.2.0", "stable", 3072, 512, false, "1.1.0"},
        {"2.0.0", "stable", 4096, 1024, false, "1.2.0"},
    };

    // Chain from 1.0.0 to 2.0.0 should be: 1.1.0 -> 1.2.0 -> 2.0.0
    auto chain = index.resolve_delta_chain("1.0.0", "2.0.0");
    ASSERT_EQ(chain.size(), 3u);
    EXPECT_EQ(chain[0], "1.1.0");
    EXPECT_EQ(chain[1], "1.2.0");
    EXPECT_EQ(chain[2], "2.0.0");
}

TEST(ReleaseIndex, DeltaChainSingleStep) {
    ReleaseIndex index;
    index.releases = {
        {"1.0.0", "stable", 1024, 0, true, ""},
        {"1.1.0", "stable", 2048, 256, false, "1.0.0"},
    };

    auto chain = index.resolve_delta_chain("1.0.0", "1.1.0");
    ASSERT_EQ(chain.size(), 1u);
    EXPECT_EQ(chain[0], "1.1.0");
}

TEST(ReleaseIndex, DeltaChainMissingVersion) {
    ReleaseIndex index;
    index.releases = {
        {"1.0.0", "stable", 1024, 0, true, ""},
        // 1.1.0 is missing from the chain
        {"1.2.0", "stable", 3072, 512, false, "1.1.0"},
    };

    auto chain = index.resolve_delta_chain("1.0.0", "1.2.0");
    EXPECT_TRUE(chain.empty()) << "Should fail when delta chain is broken";
}

TEST(ReleaseIndex, HandleMissingVersionsGracefully) {
    ReleaseIndex index;
    // Empty index
    EXPECT_TRUE(index.releases.empty());

    auto filtered = index.filter_by_channel("stable");
    EXPECT_TRUE(filtered.empty());

    auto chain = index.resolve_delta_chain("1.0.0", "2.0.0");
    EXPECT_TRUE(chain.empty());
}

TEST(ReleaseEntry, GenesisRelease) {
    ReleaseEntry genesis{"1.0.0", "stable", 10240, 0, true, ""};

    EXPECT_TRUE(genesis.is_genesis);
    EXPECT_EQ(genesis.delta_size, 0);
    EXPECT_TRUE(genesis.base_version.empty());
}

TEST(ReleaseEntry, DeltaRelease) {
    ReleaseEntry delta{"1.1.0", "stable", 10240, 2048, false, "1.0.0"};

    EXPECT_FALSE(delta.is_genesis);
    EXPECT_GT(delta.delta_size, 0);
    EXPECT_EQ(delta.base_version, "1.0.0");
    EXPECT_LT(delta.delta_size, delta.full_size);
}

}  // anonymous namespace
