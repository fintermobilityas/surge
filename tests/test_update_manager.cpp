/**
 * @file test_update_manager.cpp
 * @brief Tests for update manager version comparison, delta chain resolution,
 *        and mock storage backend update simulation.
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <map>
#include <span>
#include <string>
#include <vector>

#include "storage/storage_backend.hpp"

namespace {

// --------------------------------------------------------------------------
// Version comparison utilities
// --------------------------------------------------------------------------

struct SemVer {
    int major = 0;
    int minor = 0;
    int patch = 0;

    static SemVer parse(const std::string& s) {
        SemVer v;
        sscanf(s.c_str(), "%d.%d.%d", &v.major, &v.minor, &v.patch);
        return v;
    }

    int compare(const SemVer& other) const {
        if (major != other.major) return major < other.major ? -1 : 1;
        if (minor != other.minor) return minor < other.minor ? -1 : 1;
        if (patch != other.patch) return patch < other.patch ? -1 : 1;
        return 0;
    }

    bool operator<(const SemVer& o) const { return compare(o) < 0; }
    bool operator==(const SemVer& o) const { return compare(o) == 0; }
    bool operator>(const SemVer& o) const { return compare(o) > 0; }
    bool operator<=(const SemVer& o) const { return compare(o) <= 0; }
    bool operator>=(const SemVer& o) const { return compare(o) >= 0; }
    bool operator!=(const SemVer& o) const { return compare(o) != 0; }
};

// --------------------------------------------------------------------------
// Mock storage backend for update simulation
// --------------------------------------------------------------------------

class MockStorageBackend final : public surge::storage::IStorageBackend {
public:
    std::map<std::string, std::vector<uint8_t>> objects;
    int get_call_count = 0;
    int put_call_count = 0;

    int32_t put_object(
        const std::string& key,
        std::span<const uint8_t> data,
        const std::string& /*content_type*/) override
    {
        put_call_count++;
        objects[key] = {data.begin(), data.end()};
        return 0;
    }

    // Convenience overload for tests (avoids implicit vector->span conversion issues)
    int32_t put_object(const std::string& key, const std::vector<uint8_t>& data) {
        return put_object(key, std::span<const uint8_t>(data), "application/octet-stream");
    }

    int32_t get_object(
        const std::string& key,
        std::vector<uint8_t>& out_data) override
    {
        get_call_count++;
        auto it = objects.find(key);
        if (it == objects.end()) return -3; // SURGE_NOT_FOUND
        out_data = it->second;
        return 0;
    }

    int32_t head_object(
        const std::string& key,
        surge::storage::ObjectInfo& out_info) override
    {
        auto it = objects.find(key);
        if (it == objects.end()) return -3;
        out_info.key = key;
        out_info.size = static_cast<int64_t>(it->second.size());
        return 0;
    }

    int32_t delete_object(const std::string& key) override {
        objects.erase(key);
        return 0;
    }

    int32_t list_objects(
        const std::string& prefix,
        surge::storage::ListResult& out_result,
        const std::string& /*marker*/,
        int max_keys) override
    {
        out_result.objects.clear();
        int count = 0;
        for (const auto& [key, data] : objects) {
            if (key.substr(0, prefix.size()) == prefix) {
                out_result.objects.push_back({key, static_cast<int64_t>(data.size()), "", ""});
                if (++count >= max_keys) {
                    out_result.truncated = true;
                    break;
                }
            }
        }
        return 0;
    }

    int32_t download_to_file(
        const std::string& /*key*/,
        const std::filesystem::path& /*dest*/,
        std::function<void(int64_t, int64_t)> /*progress*/) override
    {
        return 0;
    }

    int32_t upload_from_file(
        const std::string& /*key*/,
        const std::filesystem::path& /*src*/,
        std::function<void(int64_t, int64_t)> /*progress*/) override
    {
        return 0;
    }
};

// --------------------------------------------------------------------------
// Version Comparison Tests
// --------------------------------------------------------------------------

TEST(VersionComparison, BasicLessThan) {
    EXPECT_TRUE(SemVer::parse("1.0.0") < SemVer::parse("2.0.0"));
    EXPECT_TRUE(SemVer::parse("1.0.0") < SemVer::parse("1.1.0"));
    EXPECT_TRUE(SemVer::parse("1.0.0") < SemVer::parse("1.0.1"));
}

TEST(VersionComparison, BasicGreaterThan) {
    EXPECT_TRUE(SemVer::parse("2.0.0") > SemVer::parse("1.0.0"));
    EXPECT_TRUE(SemVer::parse("1.1.0") > SemVer::parse("1.0.0"));
    EXPECT_TRUE(SemVer::parse("1.0.1") > SemVer::parse("1.0.0"));
}

TEST(VersionComparison, Equal) {
    EXPECT_EQ(SemVer::parse("1.2.3"), SemVer::parse("1.2.3"));
    EXPECT_EQ(SemVer::parse("0.0.0"), SemVer::parse("0.0.0"));
}

TEST(VersionComparison, CompareFunction) {
    auto v1 = SemVer::parse("1.2.3");
    auto v2 = SemVer::parse("1.2.4");
    EXPECT_EQ(v1.compare(v1), 0);
    EXPECT_LT(v1.compare(v2), 0);
    EXPECT_GT(v2.compare(v1), 0);
}

TEST(VersionComparison, TransitiveOrdering) {
    auto a = SemVer::parse("1.0.0");
    auto b = SemVer::parse("1.5.0");
    auto c = SemVer::parse("2.0.0");

    EXPECT_TRUE(a < b);
    EXPECT_TRUE(b < c);
    EXPECT_TRUE(a < c); // transitive
}

TEST(VersionComparison, LargeVersionNumbers) {
    auto a = SemVer::parse("100.200.300");
    auto b = SemVer::parse("100.200.301");
    EXPECT_TRUE(a < b);
}

// --------------------------------------------------------------------------
// Delta Chain Resolution Tests
// --------------------------------------------------------------------------

struct MockRelease {
    std::string version;
    std::string base_version;
    int64_t full_size;
    int64_t delta_size;
};

std::vector<std::string> resolve_delta_chain(
    const std::vector<MockRelease>& releases,
    const std::string& current_version,
    const std::string& target_version)
{
    std::map<std::string, std::string> base_map;
    for (const auto& r : releases) {
        if (!r.base_version.empty()) {
            base_map[r.version] = r.base_version;
        }
    }

    std::vector<std::string> chain;
    std::string v = target_version;

    while (v != current_version) {
        chain.push_back(v);
        auto it = base_map.find(v);
        if (it == base_map.end()) return {};
        v = it->second;
        if (chain.size() > releases.size()) return {};
    }

    std::reverse(chain.begin(), chain.end());
    return chain;
}

TEST(DeltaChain, LinearChain) {
    std::vector<MockRelease> releases = {
        {"1.0.0", "",      10000, 0},
        {"1.1.0", "1.0.0", 11000, 2000},
        {"1.2.0", "1.1.0", 12000, 3000},
        {"1.3.0", "1.2.0", 13000, 2500},
    };

    auto chain = resolve_delta_chain(releases, "1.0.0", "1.3.0");
    ASSERT_EQ(chain.size(), 3u);
    EXPECT_EQ(chain[0], "1.1.0");
    EXPECT_EQ(chain[1], "1.2.0");
    EXPECT_EQ(chain[2], "1.3.0");
}

TEST(DeltaChain, SingleStep) {
    std::vector<MockRelease> releases = {
        {"1.0.0", "",      10000, 0},
        {"1.1.0", "1.0.0", 11000, 1500},
    };

    auto chain = resolve_delta_chain(releases, "1.0.0", "1.1.0");
    ASSERT_EQ(chain.size(), 1u);
    EXPECT_EQ(chain[0], "1.1.0");
}

TEST(DeltaChain, BrokenChain_ReturnsEmpty) {
    std::vector<MockRelease> releases = {
        {"1.0.0", "",      10000, 0},
        // Gap: 1.1.0 is missing
        {"1.2.0", "1.1.0", 12000, 3000},
    };

    auto chain = resolve_delta_chain(releases, "1.0.0", "1.2.0");
    EXPECT_TRUE(chain.empty());
}

TEST(DeltaChain, AlreadyUpToDate) {
    std::vector<MockRelease> releases = {
        {"1.0.0", "", 10000, 0},
    };

    auto chain = resolve_delta_chain(releases, "1.0.0", "1.0.0");
    EXPECT_TRUE(chain.empty());
}

// --------------------------------------------------------------------------
// Mock Storage Backend Tests
// --------------------------------------------------------------------------

TEST(MockStorage, PutAndGet) {
    MockStorageBackend storage;

    std::string content = "releases index content";
    std::vector<uint8_t> data(content.begin(), content.end());

    auto result = storage.put_object("releases.yml", data);
    EXPECT_EQ(result, 0);
    EXPECT_EQ(storage.put_call_count, 1);

    std::vector<uint8_t> out;
    result = storage.get_object("releases.yml", out);
    EXPECT_EQ(result, 0);
    EXPECT_EQ(storage.get_call_count, 1);
    EXPECT_EQ(out, data);
}

TEST(MockStorage, GetNonExistent_ReturnsNotFound) {
    MockStorageBackend storage;

    std::vector<uint8_t> out;
    auto result = storage.get_object("nonexistent", out);
    EXPECT_EQ(result, -3); // SURGE_NOT_FOUND
}

TEST(MockStorage, HeadObject) {
    MockStorageBackend storage;

    std::vector<uint8_t> data = {1, 2, 3, 4, 5};
    storage.put_object("test-key", data);

    surge::storage::ObjectInfo info;
    auto result = storage.head_object("test-key", info);
    EXPECT_EQ(result, 0);
    EXPECT_EQ(info.key, "test-key");
    EXPECT_EQ(info.size, 5);
}

TEST(MockStorage, Delete) {
    MockStorageBackend storage;

    std::vector<uint8_t> data = {1, 2, 3};
    storage.put_object("to-delete", data);

    auto result = storage.delete_object("to-delete");
    EXPECT_EQ(result, 0);

    std::vector<uint8_t> out;
    result = storage.get_object("to-delete", out);
    EXPECT_EQ(result, -3);
}

TEST(MockStorage, ListObjects) {
    MockStorageBackend storage;

    std::vector<uint8_t> d1 = {1};
    std::vector<uint8_t> d2 = {2, 3};
    std::vector<uint8_t> d3 = {4};
    storage.put_object("app1/releases.yml", d1);
    storage.put_object("app1/packages/1.0.0.tar.zst", d2);
    storage.put_object("app2/releases.yml", d3);

    surge::storage::ListResult result;
    storage.list_objects("app1/", result);
    EXPECT_EQ(result.objects.size(), 2u);
}

TEST(MockStorage, ListObjects_WithMaxKeys) {
    MockStorageBackend storage;

    for (int i = 0; i < 10; ++i) {
        std::vector<uint8_t> val = {static_cast<uint8_t>(i)};
        storage.put_object("prefix/key" + std::to_string(i), val);
    }

    surge::storage::ListResult result;
    storage.list_objects("prefix/", result, "", 3);
    EXPECT_EQ(result.objects.size(), 3u);
    EXPECT_TRUE(result.truncated);
}

TEST(MockStorage, SimulateUpdateCheck) {
    MockStorageBackend storage;

    // Simulate releases index stored in cloud
    std::string releases_yaml =
        "releases:\n"
        "  - version: 1.0.0\n"
        "    channel: stable\n"
        "    is_genesis: true\n"
        "  - version: 1.1.0\n"
        "    channel: stable\n"
        "    base_version: 1.0.0\n";

    std::vector<uint8_t> data(releases_yaml.begin(), releases_yaml.end());
    storage.put_object("myapp/linux-x64/releases.yml", data);

    // Client checks for updates
    std::vector<uint8_t> fetched;
    auto result = storage.get_object("myapp/linux-x64/releases.yml", fetched);
    EXPECT_EQ(result, 0);

    std::string fetched_str(fetched.begin(), fetched.end());
    EXPECT_TRUE(fetched_str.find("1.1.0") != std::string::npos);
}

} // anonymous namespace
