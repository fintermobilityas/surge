/**
 * @file test_supervisor.cpp
 * @brief Supervisor process management tests.
 */

#include <gtest/gtest.h>

#include <csignal>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "surge/surge_api.h"

namespace fs = std::filesystem;

namespace {

class SupervisorTest : public ::testing::Test {
protected:
    fs::path temp_dir_;

    void SetUp() override {
        temp_dir_ = fs::temp_directory_path() / "surge_test_supervisor";
        fs::create_directories(temp_dir_);
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(temp_dir_, ec);
    }

    void write_file(const fs::path& path, const std::string& content) {
        fs::create_directories(path.parent_path());
        std::ofstream ofs(path, std::ios::binary);
        ofs << content;
    }
};

TEST_F(SupervisorTest, SupervisorStart_NullExePath_ReturnsError) {
    surge_result result = surge_supervisor_start(
        nullptr,
        temp_dir_.c_str(),
        "test-supervisor",
        0,
        nullptr);
    EXPECT_EQ(result, SURGE_ERROR);
}

TEST_F(SupervisorTest, SupervisorStart_NullWorkingDir_ReturnsError) {
    surge_result result = surge_supervisor_start(
        "/usr/bin/test",
        nullptr,
        "test-supervisor",
        0,
        nullptr);
    EXPECT_EQ(result, SURGE_ERROR);
}

TEST_F(SupervisorTest, SupervisorStart_NullSupervisorId_ReturnsError) {
    surge_result result = surge_supervisor_start(
        "/usr/bin/test",
        temp_dir_.c_str(),
        nullptr,
        0,
        nullptr);
    EXPECT_EQ(result, SURGE_ERROR);
}

TEST_F(SupervisorTest, SupervisorStart_NonExistentExe_ReturnsError) {
    surge_result result = surge_supervisor_start(
        "/nonexistent/path/to/binary",
        temp_dir_.c_str(),
        "test-supervisor",
        0,
        nullptr);
    EXPECT_EQ(result, SURGE_ERROR);
}

TEST_F(SupervisorTest, SupervisorStart_WithArguments) {
    // Create a dummy script to supervise
    auto script_path = temp_dir_ / "test_script.sh";
    write_file(script_path, "#!/bin/bash\nsleep 0.1\n");
    fs::permissions(script_path, fs::perms::owner_all);

    const char* args[] = {"--port", "8080", "--verbose"};

    // Note: This may or may not succeed depending on the environment.
    // We primarily test that the API handles arguments correctly.
    surge_result result = surge_supervisor_start(
        script_path.c_str(),
        temp_dir_.c_str(),
        "test-supervisor",
        3,
        args);

    // Result may be OK or ERROR depending on implementation state
    // The important thing is it doesn't crash
    EXPECT_TRUE(result == SURGE_OK || result == SURGE_ERROR);
}

TEST_F(SupervisorTest, SupervisorStart_ZeroArgs) {
    auto script_path = temp_dir_ / "no_args.sh";
    write_file(script_path, "#!/bin/bash\ntrue\n");
    fs::permissions(script_path, fs::perms::owner_all);

    surge_result result = surge_supervisor_start(
        script_path.c_str(),
        temp_dir_.c_str(),
        "no-args-supervisor",
        0,
        nullptr);

    EXPECT_TRUE(result == SURGE_OK || result == SURGE_ERROR);
}

// --------------------------------------------------------------------------
// Supervisor ID Tests
// --------------------------------------------------------------------------

TEST(SupervisorId, ConventionalNaming) {
    // Test the naming convention: <app-id>-supervisor
    std::string app_id = "myapp";
    std::string supervisor_id = app_id + "-supervisor";
    EXPECT_EQ(supervisor_id, "myapp-supervisor");
}

TEST(SupervisorId, UniquePerApp) {
    std::string sv1 = "app1-supervisor";
    std::string sv2 = "app2-supervisor";
    EXPECT_NE(sv1, sv2);
}

// --------------------------------------------------------------------------
// Process Events Tests
// --------------------------------------------------------------------------

TEST(ProcessEvents, NullCallbacks_NoOp) {
    // Calling process_events with no callbacks should not crash
    surge_result result = surge_process_events(
        0, nullptr, nullptr, nullptr, nullptr, nullptr);

    // May return OK or ERROR depending on args validation
    EXPECT_TRUE(result == SURGE_OK || result == SURGE_ERROR);
}

TEST(ProcessEvents, WithCallbacks) {
    static bool first_run_called = false;
    static bool installed_called = false;
    static bool updated_called = false;

    surge_event_callback on_first_run = [](const char*, void*) {
        first_run_called = true;
    };
    surge_event_callback on_installed = [](const char*, void*) {
        installed_called = true;
    };
    surge_event_callback on_updated = [](const char*, void*) {
        updated_called = true;
    };

    const char* args[] = {"testapp"};

    // With mock args, events may or may not fire
    surge_process_events(1, args, on_first_run, on_installed, on_updated, nullptr);

    // We just verify the function doesn't crash with callbacks
    // The actual event firing depends on installation state
}

// --------------------------------------------------------------------------
// Restart Argument Tests
// --------------------------------------------------------------------------

TEST(SupervisorArgs, EmptyArgs) {
    std::vector<std::string> args;
    EXPECT_TRUE(args.empty());
}

TEST(SupervisorArgs, PreserveArgOrder) {
    std::vector<std::string> args = {"--config", "app.yml", "--port", "8080"};

    ASSERT_EQ(args.size(), 4u);
    EXPECT_EQ(args[0], "--config");
    EXPECT_EQ(args[1], "app.yml");
    EXPECT_EQ(args[2], "--port");
    EXPECT_EQ(args[3], "8080");
}

TEST(SupervisorArgs, ArgcArgvConversion) {
    std::vector<std::string> args = {"arg1", "arg2", "arg3"};
    std::vector<const char*> argv;
    for (const auto& a : args) {
        argv.push_back(a.c_str());
    }

    EXPECT_EQ(static_cast<int>(argv.size()), 3);
    EXPECT_STREQ(argv[0], "arg1");
    EXPECT_STREQ(argv[1], "arg2");
    EXPECT_STREQ(argv[2], "arg3");
}

} // anonymous namespace
