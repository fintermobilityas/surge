#include "platform/pal_fs.hpp"

#include <array>
#include <cstring>
#include <fmt/format.h>
#include <fstream>
#include <random>
#include <spdlog/spdlog.h>

#ifdef _WIN32
#include <io.h>
#include <windows.h>
#else
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace surge::platform {

bool atomic_rename(const std::filesystem::path& src, const std::filesystem::path& dst) {
    std::error_code ec;
    std::filesystem::rename(src, dst, ec);
    if (!ec)
        return true;

    // Fallback for cross-device: copy then remove
    spdlog::debug("atomic_rename: rename failed ({}), falling back to copy+remove", ec.message());
    if (!std::filesystem::copy_file(src, dst, std::filesystem::copy_options::overwrite_existing, ec)) {
        spdlog::error("atomic_rename: copy failed: {}", ec.message());
        return false;
    }
    std::filesystem::remove(src, ec);
    return true;
}

bool copy_file_with_progress(const std::filesystem::path& src, const std::filesystem::path& dst,
                             std::function<void(int64_t, int64_t)> progress) {
    std::ifstream in(src, std::ios::binary);
    if (!in.is_open()) {
        spdlog::error("copy_file_with_progress: cannot open source '{}'", src.string());
        return false;
    }

    // Create parent directory
    std::error_code ec;
    std::filesystem::create_directories(dst.parent_path(), ec);

    std::ofstream out(dst, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) {
        spdlog::error("copy_file_with_progress: cannot open dest '{}'", dst.string());
        return false;
    }

    auto total = static_cast<int64_t>(std::filesystem::file_size(src, ec));
    if (ec)
        total = -1;

    constexpr size_t buf_size = 65536;
    std::array<char, buf_size> buf{};
    int64_t done = 0;

    while (in.good()) {
        in.read(buf.data(), buf_size);
        auto n = in.gcount();
        if (n > 0) {
            out.write(buf.data(), n);
            if (out.fail()) {
                spdlog::error("copy_file_with_progress: write failed");
                return false;
            }
            done += n;
            if (progress) {
                progress(done, total);
            }
        }
    }

    return true;
}

bool copy_directory(const std::filesystem::path& src, const std::filesystem::path& dst) {
    std::error_code ec;
    std::filesystem::copy(
        src, dst, std::filesystem::copy_options::recursive | std::filesystem::copy_options::overwrite_existing, ec);
    if (ec) {
        spdlog::error("copy_directory: failed to copy '{}' -> '{}': {}", src.string(), dst.string(), ec.message());
        return false;
    }
    return true;
}

bool remove_directory(const std::filesystem::path& dir) {
    std::error_code ec;
    if (!std::filesystem::exists(dir, ec))
        return true;

#ifdef _WIN32
    // Windows may fail due to file locks; retry a few times
    for (int attempt = 0; attempt < 3; ++attempt) {
        ec.clear();
        std::filesystem::remove_all(dir, ec);
        if (!ec)
            return true;
        spdlog::debug("remove_directory: attempt {} failed: {}", attempt + 1, ec.message());
        Sleep(100);
    }
    spdlog::error("remove_directory: failed to remove '{}': {}", dir.string(), ec.message());
    return false;
#else
    std::filesystem::remove_all(dir, ec);
    if (ec) {
        spdlog::error("remove_directory: failed to remove '{}': {}", dir.string(), ec.message());
        return false;
    }
    return true;
#endif
}

std::filesystem::path create_temp_dir(std::string_view prefix) {
    auto base = std::filesystem::temp_directory_path();

#ifdef _WIN32
    // Generate random suffix
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dist(0, 15);
    constexpr char hex[] = "0123456789abcdef";
    std::string suffix;
    for (int i = 0; i < 8; ++i)
        suffix.push_back(hex[dist(gen)]);

    auto result = base / (std::string(prefix) + suffix);
    std::error_code ec;
    std::filesystem::create_directories(result, ec);
    if (ec) {
        throw std::runtime_error(fmt::format("Failed to create temp directory: {}", ec.message()));
    }
    return result;
#else
    std::string tmpl = (base / (std::string(prefix) + "XXXXXX")).string();
    char* result = mkdtemp(tmpl.data());
    if (!result) {
        throw std::runtime_error(fmt::format("mkdtemp failed: {}", strerror(errno)));
    }
    return std::filesystem::path(result);
#endif
}

bool set_permissions(const std::filesystem::path& path, uint32_t mode) {
#ifdef _WIN32
    // No-op on Windows
    (void)path;
    (void)mode;
    return true;
#else
    if (chmod(path.c_str(), static_cast<mode_t>(mode)) != 0) {
        spdlog::error("set_permissions: chmod failed on '{}': {}", path.string(), strerror(errno));
        return false;
    }
    return true;
#endif
}

bool make_executable(const std::filesystem::path& path) {
#ifdef _WIN32
    return true;  // No-op on Windows
#else
    struct stat st{};
    if (stat(path.c_str(), &st) != 0) {
        spdlog::error("make_executable: stat failed on '{}': {}", path.string(), strerror(errno));
        return false;
    }
    mode_t new_mode = st.st_mode | S_IXUSR | S_IXGRP | S_IXOTH;
    if (chmod(path.c_str(), new_mode) != 0) {
        spdlog::error("make_executable: chmod failed on '{}': {}", path.string(), strerror(errno));
        return false;
    }
    return true;
#endif
}

std::optional<std::vector<uint8_t>> read_file(const std::filesystem::path& path) {
    std::ifstream file(path, std::ios::binary | std::ios::ate);
    if (!file.is_open()) {
        spdlog::debug("read_file: cannot open '{}'", path.string());
        return std::nullopt;
    }
    auto size = file.tellg();
    file.seekg(0, std::ios::beg);
    std::vector<uint8_t> data(static_cast<size_t>(size));
    file.read(reinterpret_cast<char*>(data.data()), size);
    if (file.fail()) {
        spdlog::error("read_file: read error on '{}'", path.string());
        return std::nullopt;
    }
    return data;
}

bool write_file_atomic(const std::filesystem::path& path, const std::vector<uint8_t>& data) {
    // Write to temp file, then rename
    auto tmp = path;
    tmp += ".tmp";

    std::ofstream file(tmp, std::ios::binary | std::ios::trunc);
    if (!file.is_open()) {
        spdlog::error("write_file_atomic: cannot open temp file '{}'", tmp.string());
        return false;
    }
    file.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(data.size()));
    file.close();
    if (file.fail()) {
        spdlog::error("write_file_atomic: write failed to '{}'", tmp.string());
        std::error_code ec;
        std::filesystem::remove(tmp, ec);
        return false;
    }

    return atomic_rename(tmp, path);
}

std::vector<std::filesystem::path> list_directories(const std::filesystem::path& parent) {
    std::vector<std::filesystem::path> result;
    std::error_code ec;

    for (const auto& entry : std::filesystem::directory_iterator(parent, ec)) {
        if (entry.is_directory(ec)) {
            result.push_back(entry.path());
        }
    }

    std::sort(result.begin(), result.end());
    return result;
}

int64_t directory_size(const std::filesystem::path& dir) {
    int64_t total = 0;
    std::error_code ec;

    for (const auto& entry : std::filesystem::recursive_directory_iterator(dir, ec)) {
        if (entry.is_regular_file(ec)) {
            total += static_cast<int64_t>(entry.file_size(ec));
        }
    }

    return total;
}

}  // namespace surge::platform
