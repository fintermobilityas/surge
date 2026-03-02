/**
 * @file test_bsdiff.cpp
 * @brief Tests for binary diff and patch round-trip operations.
 */

#include "surge/surge_api.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <numeric>
#include <vector>

namespace {

class BsdiffTest : public ::testing::Test {
protected:
    void verify_round_trip(const std::vector<uint8_t>& old_data, const std::vector<uint8_t>& new_data) {
        // Create diff
        surge_bsdiff_ctx diff_ctx{};
        diff_ctx.older = old_data.data();
        diff_ctx.older_size = static_cast<int64_t>(old_data.size());
        diff_ctx.newer = new_data.data();
        diff_ctx.newer_size = static_cast<int64_t>(new_data.size());

        auto diff_result = surge_bsdiff(&diff_ctx);
        ASSERT_EQ(diff_result, SURGE_OK) << "bsdiff failed with status: " << diff_ctx.status;
        ASSERT_NE(diff_ctx.patch, nullptr);
        ASSERT_GT(diff_ctx.patch_size, 0);

        // Apply patch
        surge_bspatch_ctx patch_ctx{};
        patch_ctx.older = old_data.data();
        patch_ctx.older_size = static_cast<int64_t>(old_data.size());
        patch_ctx.patch = diff_ctx.patch;
        patch_ctx.patch_size = diff_ctx.patch_size;

        auto patch_result = surge_bspatch(&patch_ctx);
        ASSERT_EQ(patch_result, SURGE_OK) << "bspatch failed with status: " << patch_ctx.status;
        ASSERT_NE(patch_ctx.newer, nullptr);
        ASSERT_EQ(patch_ctx.newer_size, static_cast<int64_t>(new_data.size()));

        // Verify output matches expected
        EXPECT_EQ(std::memcmp(patch_ctx.newer, new_data.data(), new_data.size()), 0)
            << "Patched output does not match expected new data";

        // Clean up
        surge_bspatch_free(&patch_ctx);
        surge_bsdiff_free(&diff_ctx);
    }
};

TEST_F(BsdiffTest, SmallFiles_RoundTrip) {
    std::vector<uint8_t> old_data = {'H', 'e', 'l', 'l', 'o', ' ', 'W', 'o', 'r', 'l', 'd'};
    std::vector<uint8_t> new_data = {'H', 'e', 'l', 'l', 'o', ' ', 'S', 'u', 'r', 'g', 'e'};

    verify_round_trip(old_data, new_data);
}

TEST_F(BsdiffTest, IdenticalFiles_RoundTrip) {
    std::vector<uint8_t> data = {'i', 'd', 'e', 'n', 't', 'i', 'c', 'a', 'l'};

    // Create diff
    surge_bsdiff_ctx diff_ctx{};
    diff_ctx.older = data.data();
    diff_ctx.older_size = static_cast<int64_t>(data.size());
    diff_ctx.newer = data.data();
    diff_ctx.newer_size = static_cast<int64_t>(data.size());

    auto diff_result = surge_bsdiff(&diff_ctx);
    ASSERT_EQ(diff_result, SURGE_OK);

    // Patch should be small for identical files
    ASSERT_NE(diff_ctx.patch, nullptr);

    // Apply patch and verify
    surge_bspatch_ctx patch_ctx{};
    patch_ctx.older = data.data();
    patch_ctx.older_size = static_cast<int64_t>(data.size());
    patch_ctx.patch = diff_ctx.patch;
    patch_ctx.patch_size = diff_ctx.patch_size;

    auto patch_result = surge_bspatch(&patch_ctx);
    ASSERT_EQ(patch_result, SURGE_OK);
    ASSERT_EQ(patch_ctx.newer_size, static_cast<int64_t>(data.size()));
    EXPECT_EQ(std::memcmp(patch_ctx.newer, data.data(), data.size()), 0);

    surge_bspatch_free(&patch_ctx);
    surge_bsdiff_free(&diff_ctx);
}

TEST_F(BsdiffTest, CompletelyDifferentFiles_RoundTrip) {
    // Old file: sequential bytes
    std::vector<uint8_t> old_data(256);
    std::iota(old_data.begin(), old_data.end(), 0);

    // New file: reversed bytes
    std::vector<uint8_t> new_data(old_data.rbegin(), old_data.rend());

    verify_round_trip(old_data, new_data);
}

TEST_F(BsdiffTest, LargerFiles_RoundTrip) {
    // 4KB old file with a pattern
    std::vector<uint8_t> old_data(4096);
    for (size_t i = 0; i < old_data.size(); ++i)
        old_data[i] = static_cast<uint8_t>(i % 256);

    // New file: same pattern but with modifications in the middle
    std::vector<uint8_t> new_data = old_data;
    for (size_t i = 1024; i < 2048; ++i)
        new_data[i] = static_cast<uint8_t>((i * 7 + 13) % 256);

    verify_round_trip(old_data, new_data);
}

TEST_F(BsdiffTest, SizeIncrease_RoundTrip) {
    std::vector<uint8_t> old_data = {'s', 'h', 'o', 'r', 't'};
    std::vector<uint8_t> new_data = {'m', 'u', 'c', 'h', ' ', 'l', 'o', 'n', 'g', 'e', 'r',
                                     ' ', 'd', 'a', 't', 'a', ' ', 'h', 'e', 'r', 'e'};

    verify_round_trip(old_data, new_data);
}

TEST_F(BsdiffTest, SizeDecrease_RoundTrip) {
    std::vector<uint8_t> old_data = {'l', 'o', 'n', 'g', 'e', 'r', ' ', 'o', 'r', 'i',
                                     'g', 'i', 'n', 'a', 'l', ' ', 'f', 'i', 'l', 'e'};
    std::vector<uint8_t> new_data = {'s', 'm', 'a', 'l', 'l'};

    verify_round_trip(old_data, new_data);
}

TEST_F(BsdiffTest, SingleByteFiles_RoundTrip) {
    std::vector<uint8_t> old_data = {0x41};
    std::vector<uint8_t> new_data = {0x42};

    verify_round_trip(old_data, new_data);
}

TEST_F(BsdiffTest, FreeNullContext_NoOp) {
    // Calling free on a zeroed context should not crash
    surge_bsdiff_ctx diff_ctx{};
    surge_bsdiff_free(&diff_ctx);

    surge_bspatch_ctx patch_ctx{};
    surge_bspatch_free(&patch_ctx);
}

}  // anonymous namespace
