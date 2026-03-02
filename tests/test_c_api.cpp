/**
 * @file test_c_api.cpp
 * @brief C API contract tests: create/destroy context, set config, verify error handling.
 */

#include <gtest/gtest.h>

#include "surge/surge_api.h"

namespace {

// --------------------------------------------------------------------------
// Context Lifecycle Tests
// --------------------------------------------------------------------------

TEST(CApi, ContextCreate_ReturnsNonNull) {
    surge_context* ctx = surge_context_create();
    ASSERT_NE(ctx, nullptr);
    surge_context_destroy(ctx);
}

TEST(CApi, ContextDestroy_NullIsNoOp) {
    // Passing NULL should not crash
    surge_context_destroy(nullptr);
}

TEST(CApi, ContextLastError_NullCtx_ReturnsNull) {
    const surge_error* err = surge_context_last_error(nullptr);
    EXPECT_EQ(err, nullptr);
}

TEST(CApi, ContextLastError_NewContext_ReturnsNull) {
    surge_context* ctx = surge_context_create();
    ASSERT_NE(ctx, nullptr);

    const surge_error* err = surge_context_last_error(ctx);
    EXPECT_EQ(err, nullptr);

    surge_context_destroy(ctx);
}

// --------------------------------------------------------------------------
// Configuration Tests
// --------------------------------------------------------------------------

TEST(CApi, ConfigSetStorage_ValidParams) {
    surge_context* ctx = surge_context_create();
    ASSERT_NE(ctx, nullptr);

    surge_result result = surge_config_set_storage(
        ctx,
        SURGE_STORAGE_FILESYSTEM,
        "/tmp/test-releases",
        nullptr,    // region
        nullptr,    // access_key
        nullptr,    // secret_key
        nullptr     // endpoint
    );
    EXPECT_EQ(result, SURGE_OK);

    surge_context_destroy(ctx);
}

TEST(CApi, ConfigSetStorage_S3Provider) {
    surge_context* ctx = surge_context_create();
    ASSERT_NE(ctx, nullptr);

    surge_result result = surge_config_set_storage(
        ctx,
        SURGE_STORAGE_S3,
        "my-bucket",
        "us-east-1",
        "AKIAEXAMPLE",
        "secret-key",
        nullptr
    );
    EXPECT_EQ(result, SURGE_OK);

    surge_context_destroy(ctx);
}

TEST(CApi, ConfigSetStorage_NullCtx_ReturnsError) {
    surge_result result = surge_config_set_storage(
        nullptr,
        SURGE_STORAGE_FILESYSTEM,
        "/tmp/test",
        nullptr, nullptr, nullptr, nullptr
    );
    EXPECT_EQ(result, SURGE_ERROR);
}

TEST(CApi, ConfigSetLockServer) {
    surge_context* ctx = surge_context_create();
    ASSERT_NE(ctx, nullptr);

    surge_result result = surge_config_set_lock_server(ctx, "https://lock.example.com");
    EXPECT_EQ(result, SURGE_OK);

    surge_context_destroy(ctx);
}

TEST(CApi, ConfigSetLockServer_NullCtx_ReturnsError) {
    surge_result result = surge_config_set_lock_server(nullptr, "https://lock.example.com");
    EXPECT_EQ(result, SURGE_ERROR);
}

TEST(CApi, ConfigSetResourceBudget) {
    surge_context* ctx = surge_context_create();
    ASSERT_NE(ctx, nullptr);

    surge_resource_budget budget = {};
    budget.max_memory_bytes = 512 * 1024 * 1024LL;
    budget.max_threads = 4;
    budget.max_concurrent_downloads = 2;
    budget.max_download_speed_bps = 10 * 1024 * 1024LL;
    budget.zstd_compression_level = 12;

    surge_result result = surge_config_set_resource_budget(ctx, &budget);
    EXPECT_EQ(result, SURGE_OK);

    surge_context_destroy(ctx);
}

TEST(CApi, ConfigSetResourceBudget_NullCtx_ReturnsError) {
    surge_resource_budget budget = {};
    surge_result result = surge_config_set_resource_budget(nullptr, &budget);
    EXPECT_EQ(result, SURGE_ERROR);
}

// --------------------------------------------------------------------------
// Update Manager Tests
// --------------------------------------------------------------------------

TEST(CApi, UpdateManagerCreate_WithValidParams) {
    surge_context* ctx = surge_context_create();
    ASSERT_NE(ctx, nullptr);

    // Configure storage first
    surge_config_set_storage(ctx, SURGE_STORAGE_FILESYSTEM, "/tmp/test", nullptr, nullptr, nullptr, nullptr);

    surge_update_manager* mgr = surge_update_manager_create(
        ctx, "testapp", "1.0.0", "stable", "/opt/testapp");
    EXPECT_NE(mgr, nullptr);

    surge_update_manager_destroy(mgr);
    surge_context_destroy(ctx);
}

TEST(CApi, UpdateManagerCreate_NullCtx_ReturnsNull) {
    surge_update_manager* mgr = surge_update_manager_create(
        nullptr, "testapp", "1.0.0", "stable", "/opt/testapp");
    EXPECT_EQ(mgr, nullptr);
}

TEST(CApi, UpdateManagerDestroy_NullIsNoOp) {
    surge_update_manager_destroy(nullptr);
}

// --------------------------------------------------------------------------
// Release Info Tests
// --------------------------------------------------------------------------

TEST(CApi, ReleasesDestroy_NullIsNoOp) {
    surge_releases_destroy(nullptr);
}

// --------------------------------------------------------------------------
// Cancellation Tests
// --------------------------------------------------------------------------

TEST(CApi, Cancel_ValidCtx) {
    surge_context* ctx = surge_context_create();
    ASSERT_NE(ctx, nullptr);

    surge_result result = surge_cancel(ctx);
    EXPECT_EQ(result, SURGE_OK);

    surge_context_destroy(ctx);
}

TEST(CApi, Cancel_NullCtx_ReturnsError) {
    surge_result result = surge_cancel(nullptr);
    EXPECT_EQ(result, SURGE_ERROR);
}

// --------------------------------------------------------------------------
// Enum Value Tests
// --------------------------------------------------------------------------

TEST(CApi, ResultCodes_HaveExpectedValues) {
    EXPECT_EQ(SURGE_OK, 0);
    EXPECT_EQ(SURGE_ERROR, -1);
    EXPECT_EQ(SURGE_CANCELLED, -2);
    EXPECT_EQ(SURGE_NOT_FOUND, -3);
}

TEST(CApi, ProgressPhases_HaveExpectedValues) {
    EXPECT_EQ(SURGE_PHASE_CHECK, 0);
    EXPECT_EQ(SURGE_PHASE_DOWNLOAD, 1);
    EXPECT_EQ(SURGE_PHASE_VERIFY, 2);
    EXPECT_EQ(SURGE_PHASE_EXTRACT, 3);
    EXPECT_EQ(SURGE_PHASE_APPLY_DELTA, 4);
    EXPECT_EQ(SURGE_PHASE_FINALIZE, 5);
}

TEST(CApi, StorageProviders_HaveExpectedValues) {
    EXPECT_EQ(SURGE_STORAGE_S3, 0);
    EXPECT_EQ(SURGE_STORAGE_AZURE_BLOB, 1);
    EXPECT_EQ(SURGE_STORAGE_GCS, 2);
    EXPECT_EQ(SURGE_STORAGE_FILESYSTEM, 3);
}

// --------------------------------------------------------------------------
// Struct Size/Layout Tests
// --------------------------------------------------------------------------

TEST(CApi, ProgressStruct_SizeIsReasonable) {
    // Ensure the struct has the expected minimum size
    EXPECT_GE(sizeof(surge_progress), 48u);
}

TEST(CApi, ResourceBudgetStruct_SizeIsReasonable) {
    EXPECT_GE(sizeof(surge_resource_budget), 24u);
}

TEST(CApi, ErrorStruct_ContainsCodeAndMessage) {
    surge_error err = {};
    err.code = -1;
    err.message = "test error";
    EXPECT_EQ(err.code, -1);
    EXPECT_STREQ(err.message, "test error");
}

// --------------------------------------------------------------------------
// Pack Context Tests
// --------------------------------------------------------------------------

TEST(CApi, PackDestroy_NullIsNoOp) {
    surge_pack_destroy(nullptr);
}

// --------------------------------------------------------------------------
// Bsdiff/Bspatch Free Tests
// --------------------------------------------------------------------------

TEST(CApi, BsdiffFree_ZeroedCtx_NoOp) {
    surge_bsdiff_ctx ctx = {};
    surge_bsdiff_free(&ctx);
}

TEST(CApi, BspatchFree_ZeroedCtx_NoOp) {
    surge_bspatch_ctx ctx = {};
    surge_bspatch_free(&ctx);
}

} // anonymous namespace
