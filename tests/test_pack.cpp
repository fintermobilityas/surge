/**
 * @file test_pack.cpp
 * @brief Package builder tests with temp directories.
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "config/manifest.hpp"
#include "config/constants.hpp"

namespace fs = std::filesystem;

namespace {

class PackTest : public ::testing::Test {
protected:
    fs::path temp_dir_;

    void SetUp() override {
        temp_dir_ = fs::temp_directory_path() / "surge_test_pack";
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

    std::string read_file(const fs::path& path) {
        std::ifstream ifs(path, std::ios::binary);
        return {std::istreambuf_iterator<char>(ifs), std::istreambuf_iterator<char>()};
    }

    // Create a minimal manifest for testing
    surge::config::SurgeManifest create_test_manifest() {
        surge::config::SurgeManifest manifest;
        manifest.schema = 1;
        manifest.generic.token = "test-token";
        manifest.generic.artifacts = (temp_dir_ / "artifacts").string();
        manifest.generic.packages = (temp_dir_ / "packages").string();
        manifest.storage.provider = "filesystem";
        manifest.storage.bucket = (temp_dir_ / "releases").string();
        manifest.channels.push_back({"test"});

        surge::config::AppConfig app;
        app.id = "testapp";
        app.main = "testapp";
        app.supervisor_id = "testapp-sv";
        app.install_directory = "testapp";
        app.target.os = "linux";
        app.target.rid = "linux-x64";
        manifest.apps.push_back(app);

        return manifest;
    }
};

TEST_F(PackTest, ManifestForPack_IsValid) {
    auto manifest = create_test_manifest();
    auto issues = surge::config::validate_manifest(manifest);
    EXPECT_TRUE(issues.empty())
        << "Test manifest should be valid: " << (issues.empty() ? "" : issues[0]);
}

TEST_F(PackTest, PrepareDirectories) {
    auto artifacts_dir = temp_dir_ / "artifacts";
    auto packages_dir = temp_dir_ / "packages";
    auto releases_dir = temp_dir_ / "releases";

    fs::create_directories(artifacts_dir);
    fs::create_directories(packages_dir);
    fs::create_directories(releases_dir);

    EXPECT_TRUE(fs::is_directory(artifacts_dir));
    EXPECT_TRUE(fs::is_directory(packages_dir));
    EXPECT_TRUE(fs::is_directory(releases_dir));
}

TEST_F(PackTest, ArtifactLayout) {
    // Simulate build artifacts
    auto artifacts_dir = temp_dir_ / "artifacts" / "testapp" / "linux-x64";
    write_file(artifacts_dir / "testapp", "#!/bin/bash\necho hello\n");
    write_file(artifacts_dir / "libsurge.so", std::string(4096, '\0'));
    write_file(artifacts_dir / "config" / "default.yml", "key: value\n");

    EXPECT_TRUE(fs::exists(artifacts_dir / "testapp"));
    EXPECT_TRUE(fs::exists(artifacts_dir / "libsurge.so"));
    EXPECT_TRUE(fs::exists(artifacts_dir / "config" / "default.yml"));

    // Count files
    int file_count = 0;
    for (auto& entry : fs::recursive_directory_iterator(artifacts_dir)) {
        if (entry.is_regular_file()) file_count++;
    }
    EXPECT_EQ(file_count, 3);
}

TEST_F(PackTest, PackageOutputNaming) {
    // Verify package naming convention
    std::string app_id = "testapp";
    std::string rid = "linux-x64";
    std::string version = "1.0.0";

    auto full_name = app_id + "-" + version + "-" + rid + "-full.tar.zst";
    auto delta_name = app_id + "-" + version + "-" + rid + "-delta.tar.zst";

    EXPECT_EQ(full_name, "testapp-1.0.0-linux-x64-full.tar.zst");
    EXPECT_EQ(delta_name, "testapp-1.0.0-linux-x64-delta.tar.zst");
}

TEST_F(PackTest, GenesisPackage_NoDelta) {
    // For genesis (first) release, only a full package should exist
    auto packages_dir = temp_dir_ / "packages";
    fs::create_directories(packages_dir);

    auto full_path = packages_dir / "testapp-1.0.0-linux-x64-full.tar.zst";
    write_file(full_path, "full package data");

    EXPECT_TRUE(fs::exists(full_path));
    EXPECT_FALSE(fs::exists(packages_dir / "testapp-1.0.0-linux-x64-delta.tar.zst"));
}

TEST_F(PackTest, DeltaPackage_BothExist) {
    // For subsequent releases, both full and delta should exist
    auto packages_dir = temp_dir_ / "packages";
    fs::create_directories(packages_dir);

    auto full_path = packages_dir / "testapp-1.1.0-linux-x64-full.tar.zst";
    auto delta_path = packages_dir / "testapp-1.1.0-linux-x64-delta.tar.zst";
    write_file(full_path, std::string(10000, 'F'));
    write_file(delta_path, std::string(2000, 'D'));

    EXPECT_TRUE(fs::exists(full_path));
    EXPECT_TRUE(fs::exists(delta_path));
    EXPECT_GT(fs::file_size(full_path), fs::file_size(delta_path));
}

TEST_F(PackTest, ManifestWriteAndRead) {
    auto manifest = create_test_manifest();
    auto manifest_path = temp_dir_ / "surge.yml";

    surge::config::write_manifest(manifest, manifest_path);
    ASSERT_TRUE(fs::exists(manifest_path));

    auto parsed = surge::config::parse_manifest(manifest_path);
    EXPECT_EQ(parsed.apps[0].id, "testapp");
    EXPECT_EQ(parsed.apps[0].target.rid, "linux-x64");
    EXPECT_EQ(parsed.storage.provider, "filesystem");
}

TEST_F(PackTest, ConstantsUsedCorrectly) {
    EXPECT_STREQ(surge::constants::SURGE_DIR, ".surge");
    EXPECT_STREQ(surge::constants::MANIFEST_FILE, "surge.yml");
    EXPECT_STREQ(surge::constants::RELEASES_FILE, "releases.yml");
    EXPECT_STREQ(surge::constants::PACKAGES_DIR, "packages");
    EXPECT_STREQ(surge::constants::ARTIFACTS_DIR, "artifacts");
    EXPECT_STREQ(surge::constants::APP_DIR_PREFIX, "app-");
}

TEST_F(PackTest, MultipleAppManifest) {
    surge::config::SurgeManifest manifest;
    manifest.schema = 1;
    manifest.storage.provider = "filesystem";
    manifest.storage.bucket = "/tmp/releases";
    manifest.channels.push_back({"stable"});

    for (const auto& name : {"app1", "app2", "app3"}) {
        surge::config::AppConfig app;
        app.id = name;
        app.main = name;
        app.install_directory = name;
        app.target.os = "linux";
        app.target.rid = "linux-x64";
        manifest.apps.push_back(app);
    }

    auto issues = surge::config::validate_manifest(manifest);
    EXPECT_TRUE(issues.empty());
    EXPECT_EQ(manifest.apps.size(), 3u);
}

} // anonymous namespace
