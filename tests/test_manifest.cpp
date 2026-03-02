/**
 * @file test_manifest.cpp
 * @brief Tests for surge.yml manifest parsing and validation.
 */

#include <gtest/gtest.h>

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>

#include "config/manifest.hpp"

namespace fs = std::filesystem;

namespace {

class ManifestTest : public ::testing::Test {
protected:
    fs::path temp_dir_;

    void SetUp() override {
        temp_dir_ = fs::temp_directory_path() / "surge_test_manifest";
        fs::create_directories(temp_dir_);
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(temp_dir_, ec);
    }

    fs::path write_yaml(const std::string& filename, const std::string& content) {
        auto path = temp_dir_ / filename;
        std::ofstream ofs(path);
        ofs << content;
        return path;
    }
};

TEST_F(ManifestTest, ParseValidManifest) {
    auto path = write_yaml("surge.yml", R"yaml(
schema: 1

generic:
  token: my-token
  artifacts: .surge/artifacts
  packages: .surge/packages

storage:
  provider: s3
  bucket: my-bucket
  region: us-east-1
  prefix: releases/
  endpoint: ""

lock:
  server: https://lock.example.com

channels:
  - name: stable
  - name: beta

apps:
  - id: myapp
    main: myapp
    supervisorId: myapp-supervisor
    installDirectory: myapp
    channels: []
    target:
      os: linux
      rid: linux-x64
      persistentAssets:
        - config
        - logs
      environment:
        MY_VAR: value
    metadata:
      description: My application
      authors: Test Author
)yaml");

    auto manifest = surge::config::parse_manifest(path);

    EXPECT_EQ(manifest.schema, 1);
    EXPECT_EQ(manifest.generic.token, "my-token");
    EXPECT_EQ(manifest.generic.artifacts, ".surge/artifacts");
    EXPECT_EQ(manifest.generic.packages, ".surge/packages");
    EXPECT_EQ(manifest.storage.provider, "s3");
    EXPECT_EQ(manifest.storage.bucket, "my-bucket");
    EXPECT_EQ(manifest.storage.region, "us-east-1");
    EXPECT_EQ(manifest.storage.prefix, "releases/");
    EXPECT_EQ(manifest.lock.server, "https://lock.example.com");

    ASSERT_EQ(manifest.channels.size(), 2u);
    EXPECT_EQ(manifest.channels[0].name, "stable");
    EXPECT_EQ(manifest.channels[1].name, "beta");

    ASSERT_EQ(manifest.apps.size(), 1u);
    const auto& app = manifest.apps[0];
    EXPECT_EQ(app.id, "myapp");
    EXPECT_EQ(app.main, "myapp");
    EXPECT_EQ(app.supervisor_id, "myapp-supervisor");
    EXPECT_EQ(app.install_directory, "myapp");
    EXPECT_EQ(app.target.os, "linux");
    EXPECT_EQ(app.target.rid, "linux-x64");
    ASSERT_EQ(app.target.persistent_assets.size(), 2u);
    EXPECT_EQ(app.target.persistent_assets[0], "config");
    EXPECT_EQ(app.target.persistent_assets[1], "logs");
    EXPECT_EQ(app.target.environment.at("MY_VAR"), "value");
    EXPECT_EQ(app.metadata.description, "My application");
    EXPECT_EQ(app.metadata.authors, "Test Author");
}

TEST_F(ManifestTest, ParseMinimalManifest) {
    auto path = write_yaml("minimal.yml", R"yaml(
schema: 1

generic:
  token: ""
  artifacts: ""
  packages: ""

storage:
  provider: filesystem
  bucket: /tmp/releases

channels:
  - name: test

apps:
  - id: testapp
    main: testapp
    installDirectory: testapp
    target:
      os: linux
      rid: linux-x64
    metadata:
      description: ""
      authors: ""
)yaml");

    auto manifest = surge::config::parse_manifest(path);

    EXPECT_EQ(manifest.schema, 1);
    EXPECT_EQ(manifest.storage.provider, "filesystem");
    EXPECT_EQ(manifest.storage.bucket, "/tmp/releases");
    ASSERT_EQ(manifest.channels.size(), 1u);
    ASSERT_EQ(manifest.apps.size(), 1u);
    EXPECT_EQ(manifest.apps[0].id, "testapp");
}

TEST_F(ManifestTest, DetectMissingRequiredFields) {
    // Manifest with missing app id
    surge::config::SurgeManifest manifest;
    manifest.schema = 1;
    manifest.channels.push_back({"stable"});

    surge::config::AppConfig app;
    app.main = "myapp";
    app.install_directory = "myapp";
    app.target.os = "linux";
    app.target.rid = "linux-x64";
    // id is intentionally empty
    manifest.apps.push_back(app);

    auto issues = surge::config::validate_manifest(manifest);
    EXPECT_FALSE(issues.empty());

    bool found_id_issue = false;
    for (const auto& issue : issues) {
        if (issue.find("id") != std::string::npos) {
            found_id_issue = true;
            break;
        }
    }
    EXPECT_TRUE(found_id_issue) << "Expected validation to flag missing app id";
}

TEST_F(ManifestTest, RoundTripWriteThenParse) {
    surge::config::SurgeManifest original;
    original.schema = 1;
    original.generic.token = "round-trip-token";
    original.generic.artifacts = ".surge/artifacts";
    original.generic.packages = ".surge/packages";
    original.storage.provider = "s3";
    original.storage.bucket = "test-bucket";
    original.storage.region = "eu-west-1";
    original.lock.server = "https://lock.test.com";
    original.channels.push_back({"stable"});
    original.channels.push_back({"beta"});

    surge::config::AppConfig app;
    app.id = "roundtrip";
    app.main = "roundtrip";
    app.supervisor_id = "roundtrip-sv";
    app.install_directory = "roundtrip";
    app.target.os = "linux";
    app.target.rid = "linux-x64";
    app.target.persistent_assets = {"data", "cache"};
    app.metadata.description = "Round-trip test app";
    app.metadata.authors = "Test";
    original.apps.push_back(app);

    auto path = temp_dir_ / "roundtrip.yml";
    surge::config::write_manifest(original, path);

    ASSERT_TRUE(fs::exists(path));

    auto parsed = surge::config::parse_manifest(path);

    EXPECT_EQ(parsed.schema, original.schema);
    EXPECT_EQ(parsed.generic.token, original.generic.token);
    EXPECT_EQ(parsed.storage.provider, original.storage.provider);
    EXPECT_EQ(parsed.storage.bucket, original.storage.bucket);
    EXPECT_EQ(parsed.storage.region, original.storage.region);
    EXPECT_EQ(parsed.lock.server, original.lock.server);

    ASSERT_EQ(parsed.channels.size(), original.channels.size());
    for (size_t i = 0; i < parsed.channels.size(); ++i) {
        EXPECT_EQ(parsed.channels[i].name, original.channels[i].name);
    }

    ASSERT_EQ(parsed.apps.size(), 1u);
    EXPECT_EQ(parsed.apps[0].id, "roundtrip");
    EXPECT_EQ(parsed.apps[0].target.rid, "linux-x64");
    ASSERT_EQ(parsed.apps[0].target.persistent_assets.size(), 2u);
    EXPECT_EQ(parsed.apps[0].target.persistent_assets[0], "data");
    EXPECT_EQ(parsed.apps[0].target.persistent_assets[1], "cache");
}

TEST_F(ManifestTest, YamlAnchorAliasSupport) {
    // YAML anchors (&) and aliases (*) are standard YAML features
    auto path = write_yaml("anchors.yml", R"yaml(
schema: 1

generic:
  token: anchor-test
  artifacts: .surge/artifacts
  packages: .surge/packages

storage:
  provider: filesystem
  bucket: /tmp/test

channels:
  - name: stable

apps:
  - id: app1
    main: app1
    installDirectory: app1
    target:
      os: linux
      rid: linux-x64
    metadata:
      description: App 1
      authors: Team
  - id: app2
    main: app2
    installDirectory: app2
    target:
      os: linux
      rid: linux-arm64
    metadata:
      description: App 2
      authors: Team
)yaml");

    auto manifest = surge::config::parse_manifest(path);

    ASSERT_EQ(manifest.apps.size(), 2u);
    EXPECT_EQ(manifest.apps[0].target.os, "linux");
    EXPECT_EQ(manifest.apps[0].target.rid, "linux-x64");
    EXPECT_EQ(manifest.apps[1].target.os, "linux");
    // The alias with override should produce linux-arm64
    EXPECT_EQ(manifest.apps[1].target.rid, "linux-arm64");
}

TEST_F(ManifestTest, ParseNonExistentFile_Throws) {
    EXPECT_THROW(
        surge::config::parse_manifest(temp_dir_ / "nonexistent.yml"),
        std::runtime_error);
}

TEST_F(ManifestTest, ParseInvalidYaml_Throws) {
    auto path = write_yaml("invalid.yml", "{{{{not valid yaml}}}}");
    EXPECT_THROW(
        surge::config::parse_manifest(path),
        std::runtime_error);
}

TEST_F(ManifestTest, ValidManifest_NoIssues) {
    surge::config::SurgeManifest manifest;
    manifest.schema = 1;
    manifest.storage.provider = "filesystem";
    manifest.storage.bucket = "/tmp/test";
    manifest.channels.push_back({"stable"});

    surge::config::AppConfig app;
    app.id = "valid";
    app.main = "valid";
    app.install_directory = "valid";
    app.target.os = "linux";
    app.target.rid = "linux-x64";
    manifest.apps.push_back(app);

    auto issues = surge::config::validate_manifest(manifest);
    EXPECT_TRUE(issues.empty()) << "Valid manifest should have no issues, got: "
                                 << (issues.empty() ? "" : issues[0]);
}

} // anonymous namespace
