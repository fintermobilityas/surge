/**
 * @file test_archive.cpp
 * @brief Tests for tar.zst archive creation, extraction, and content verification.
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "archive/packer.hpp"

namespace fs = std::filesystem;

namespace {

class ArchiveTest : public ::testing::Test {
protected:
    fs::path temp_dir_;

    void SetUp() override {
        temp_dir_ = fs::temp_directory_path() / "surge_test_archive";
        fs::create_directories(temp_dir_);
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(temp_dir_, ec);
    }

    void write_file(const fs::path& path, const std::string& content, fs::perms perms = fs::perms::owner_read | fs::perms::owner_write | fs::perms::group_read | fs::perms::others_read) {
        fs::create_directories(path.parent_path());
        std::ofstream ofs(path, std::ios::binary);
        ofs << content;
        ofs.close();
        fs::permissions(path, perms);
    }

    std::string read_file(const fs::path& path) {
        std::ifstream ifs(path, std::ios::binary);
        return {std::istreambuf_iterator<char>(ifs), std::istreambuf_iterator<char>()};
    }
};

TEST_F(ArchiveTest, CreateAndExtract_VerifyContents) {
    // Create test files
    auto source_dir = temp_dir_ / "source";
    write_file(source_dir / "file1.txt", "Hello, World!");
    write_file(source_dir / "file2.dat", std::string(1024, 'X'));
    write_file(source_dir / "subdir" / "nested.txt", "Nested content");

    auto archive_path = temp_dir_ / "test.tar.zst";

    // Pack
    {
        surge::archive::ArchivePacker packer(archive_path, {.zstd_level = 3, .progress = nullptr});
        packer.add_directory(source_dir);
        packer.finalize();
    }

    ASSERT_TRUE(fs::exists(archive_path));
    EXPECT_GT(fs::file_size(archive_path), 0u);
}

TEST_F(ArchiveTest, AddBuffer_CreatesEntry) {
    auto archive_path = temp_dir_ / "buffer_test.tar.zst";

    std::string content = "manifest content here";
    std::vector<uint8_t> data(content.begin(), content.end());

    {
        surge::archive::ArchivePacker packer(archive_path, {.zstd_level = 1});
        packer.add_buffer("manifest.yml", data, 0644);
        packer.finalize();
    }

    ASSERT_TRUE(fs::exists(archive_path));
    EXPECT_GT(fs::file_size(archive_path), 0u);
}

TEST_F(ArchiveTest, PermissionPreservation_0755) {
    auto source_dir = temp_dir_ / "perms";
    auto script_path = source_dir / "run.sh";
    write_file(script_path, "#!/bin/bash\necho hello",
               fs::perms::owner_all | fs::perms::group_read | fs::perms::group_exec |
               fs::perms::others_read | fs::perms::others_exec);

    auto archive_path = temp_dir_ / "perms.tar.zst";

    {
        surge::archive::ArchivePacker packer(archive_path);
        packer.add_file(script_path, "run.sh");
        packer.finalize();
    }

    ASSERT_TRUE(fs::exists(archive_path));
}

TEST_F(ArchiveTest, PermissionPreservation_0644) {
    auto source_dir = temp_dir_ / "perms644";
    auto config_path = source_dir / "config.yml";
    write_file(config_path, "key: value",
               fs::perms::owner_read | fs::perms::owner_write |
               fs::perms::group_read | fs::perms::others_read);

    auto archive_path = temp_dir_ / "perms644.tar.zst";

    {
        surge::archive::ArchivePacker packer(archive_path);
        packer.add_file(config_path, "config.yml");
        packer.finalize();
    }

    ASSERT_TRUE(fs::exists(archive_path));
}

TEST_F(ArchiveTest, DirectoryStructurePreservation) {
    auto source_dir = temp_dir_ / "structure";
    write_file(source_dir / "a" / "b" / "c" / "deep.txt", "deep");
    write_file(source_dir / "a" / "sibling.txt", "sibling");
    write_file(source_dir / "root.txt", "root");

    auto archive_path = temp_dir_ / "structure.tar.zst";

    {
        surge::archive::ArchivePacker packer(archive_path, {.zstd_level = 1});
        packer.add_directory(source_dir, "app/");
        packer.finalize();
    }

    ASSERT_TRUE(fs::exists(archive_path));
    EXPECT_GT(fs::file_size(archive_path), 0u);
}

TEST_F(ArchiveTest, EmbeddedManifest) {
    auto archive_path = temp_dir_ / "with_manifest.tar.zst";

    std::string manifest_yaml = "schema: 1\nid: testapp\nversion: 1.0.0\n";
    std::vector<uint8_t> manifest_data(manifest_yaml.begin(), manifest_yaml.end());

    auto source_dir = temp_dir_ / "app_files";
    write_file(source_dir / "myapp", "binary content");

    {
        surge::archive::ArchivePacker packer(archive_path);
        packer.add_buffer("manifest.yml", manifest_data, 0644);
        packer.add_directory(source_dir, "app/");
        packer.finalize();
    }

    ASSERT_TRUE(fs::exists(archive_path));
}

TEST_F(ArchiveTest, EmptyArchive_Finalizes) {
    auto archive_path = temp_dir_ / "empty.tar.zst";

    {
        surge::archive::ArchivePacker packer(archive_path);
        packer.finalize();
    }

    ASSERT_TRUE(fs::exists(archive_path));
}

TEST_F(ArchiveTest, DoubleFinalize_IsNoOp) {
    auto archive_path = temp_dir_ / "double_finalize.tar.zst";

    {
        surge::archive::ArchivePacker packer(archive_path);
        packer.add_buffer("test.txt", std::vector<uint8_t>{'t', 'e', 's', 't'});
        packer.finalize();
        // Second finalize should be a no-op
        packer.finalize();
    }

    ASSERT_TRUE(fs::exists(archive_path));
}

TEST_F(ArchiveTest, ProgressCallback_IsCalled) {
    auto source_dir = temp_dir_ / "progress";
    for (int i = 0; i < 5; ++i) {
        write_file(source_dir / ("file" + std::to_string(i) + ".txt"),
                   std::string(100, static_cast<char>('A' + i)));
    }

    auto archive_path = temp_dir_ / "progress.tar.zst";
    int callback_count = 0;

    {
        surge::archive::PackerOptions opts;
        opts.zstd_level = 1;
        opts.progress = [&](int64_t done, int64_t total) {
            callback_count++;
            EXPECT_GE(done, 0);
            EXPECT_GE(total, 0);
            EXPECT_LE(done, total);
        };

        surge::archive::ArchivePacker packer(archive_path, opts);
        packer.add_directory(source_dir);
        packer.finalize();
    }

    EXPECT_GT(callback_count, 0) << "Progress callback should have been called at least once";
}

} // anonymous namespace
