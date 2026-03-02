/**
 * @file stub_executable.cpp
 * @brief Stub executable that launches the highest-versioned app directory.
 */

#include "supervisor/stub_executable.hpp"
#include "config/constants.hpp"
#include "releases/release_manifest.hpp"
#include <spdlog/spdlog.h>
#include <fmt/format.h>

#include <algorithm>
#include <charconv>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#ifdef _WIN32
#include <windows.h>
#include <process.h>
#else
#include <unistd.h>
#include <sys/types.h>
#endif

namespace surge::supervisor {

namespace fs = std::filesystem;

struct StubExecutable::Impl {
    fs::path install_dir;
    std::string app_main;
};

StubExecutable::StubExecutable(std::filesystem::path install_dir,
                               std::string app_main)
    : impl_(std::make_unique<Impl>())
{
    impl_->install_dir = std::move(install_dir);
    impl_->app_main = std::move(app_main);
}

StubExecutable::~StubExecutable() = default;

std::optional<std::filesystem::path> StubExecutable::find_latest_app_dir() const {
    auto dirs = list_app_dirs();
    if (dirs.empty()) return std::nullopt;

    // dirs are sorted newest first
    auto exe_path = dirs.front() / impl_->app_main;
    if (fs::exists(exe_path)) {
        return exe_path;
    }

    // Try other versions
    for (auto& dir : dirs) {
        auto path = dir / impl_->app_main;
        if (fs::exists(path)) return path;
    }

    return std::nullopt;
}

int32_t StubExecutable::run(const std::vector<std::string>& args) {
    auto exe = find_latest_app_dir();
    if (!exe) {
        spdlog::error("Could not find any app-* directories with executable '{}' in: {}",
                       impl_->app_main, impl_->install_dir.string());
        return 1;
    }

    spdlog::info("Launching: {}", exe->string());

    auto app_dir = exe->parent_path();

#ifndef _WIN32
    // Build argv for execv
    std::vector<const char*> exec_argv;
    std::string exe_str = exe->string();
    exec_argv.push_back(exe_str.c_str());
    for (auto& arg : args) {
        exec_argv.push_back(arg.c_str());
    }
    exec_argv.push_back(nullptr);

    execv(exe_str.c_str(), const_cast<char* const*>(exec_argv.data()));
    // Only returns on failure
    spdlog::error("execv failed: {}", strerror(errno));
    return 127;

#else
    // Windows: use CreateProcess
    std::string cmd_line = exe->string();
    for (auto& arg : args) {
        cmd_line += " ";
        cmd_line += arg;
    }

    STARTUPINFOA si{};
    PROCESS_INFORMATION pi{};
    si.cb = sizeof(si);

    if (!CreateProcessA(
            exe->string().c_str(),
            cmd_line.data(),
            nullptr, nullptr,
            FALSE,
            0,
            nullptr,
            app_dir.string().c_str(),
            &si, &pi))
    {
        spdlog::error("CreateProcess failed: {}", GetLastError());
        return 1;
    }

    WaitForSingleObject(pi.hProcess, INFINITE);
    DWORD exit_code = 0;
    GetExitCodeProcess(pi.hProcess, &exit_code);
    CloseHandle(pi.hThread);
    CloseHandle(pi.hProcess);
    return static_cast<int32_t>(exit_code);
#endif
}

std::vector<std::filesystem::path> StubExecutable::list_app_dirs() const {
    std::vector<std::filesystem::path> result;
    std::error_code ec;

    for (auto& entry : fs::directory_iterator(impl_->install_dir, ec)) {
        if (!entry.is_directory()) continue;

        auto dirname = entry.path().filename().string();
        if (!dirname.starts_with(constants::APP_DIR_PREFIX)) continue;

        auto ver = version_from_dir_name(dirname);
        if (!ver) continue;

        result.push_back(entry.path());
    }

    // Sort newest first
    std::sort(result.begin(), result.end(),
        [](const fs::path& a, const fs::path& b) {
            auto va = StubExecutable::version_from_dir_name(a.filename().string());
            auto vb = StubExecutable::version_from_dir_name(b.filename().string());
            if (!va || !vb) return false;
            return releases::compare_versions(*va, *vb) > 0;
        });

    return result;
}

std::optional<std::string> StubExecutable::version_from_dir_name(
    const std::string& dir_name) {
    if (!dir_name.starts_with(constants::APP_DIR_PREFIX)) {
        return std::nullopt;
    }
    auto version = dir_name.substr(std::strlen(constants::APP_DIR_PREFIX));
    if (version.empty()) return std::nullopt;

    // Validate it looks like a version (starts with a digit)
    if (!std::isdigit(static_cast<unsigned char>(version.front()))) {
        return std::nullopt;
    }

    return version;
}

} // namespace surge::supervisor
