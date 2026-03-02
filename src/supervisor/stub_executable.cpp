/**
 * @file stub_executable.cpp
 * @brief Stub executable that launches the highest-versioned app directory.
 *
 * This is a modernized port of snapx's stubexecutable.cpp using std::filesystem.
 */

#include "supervisor/stub_executable.hpp"
#include "config/constants.hpp"
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

namespace {

/**
 * Parse a simple semver string "MAJOR.MINOR.PATCH" into comparable parts.
 * Returns empty vector on parse failure.
 */
std::vector<int> parse_semver(const std::string& version) {
    std::vector<int> parts;
    std::string_view sv = version;

    while (!sv.empty()) {
        int value = 0;
        auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), value);
        if (ec != std::errc{}) return {};
        parts.push_back(value);
        sv = std::string_view(ptr, sv.data() + sv.size() - ptr);
        if (!sv.empty() && sv.front() == '.') {
            sv.remove_prefix(1);
        } else if (!sv.empty()) {
            // Handle pre-release suffixes: stop parsing numeric parts
            break;
        }
    }
    return parts;
}

/**
 * Compare two semver version vectors. Returns -1, 0, or 1.
 */
int compare_semver(const std::vector<int>& a, const std::vector<int>& b) {
    auto max_len = std::max(a.size(), b.size());
    for (size_t i = 0; i < max_len; ++i) {
        int va = (i < a.size()) ? a[i] : 0;
        int vb = (i < b.size()) ? b[i] : 0;
        if (va < vb) return -1;
        if (va > vb) return 1;
    }
    return 0;
}

/**
 * Find the highest-versioned app directory in the given directory.
 * Looks for directories named "app-X.Y.Z".
 */
fs::path find_highest_version_dir(const fs::path& base_dir) {
    struct VersionDir {
        fs::path path;
        std::vector<int> version;
    };

    std::vector<VersionDir> app_dirs;
    std::error_code ec;

    for (auto& entry : fs::directory_iterator(base_dir, ec)) {
        if (!entry.is_directory()) continue;

        auto dirname = entry.path().filename().string();
        if (!dirname.starts_with(constants::APP_DIR_PREFIX)) continue;

        auto version_str = dirname.substr(std::strlen(constants::APP_DIR_PREFIX));
        auto version = parse_semver(version_str);
        if (version.empty()) {
            spdlog::warn("Could not parse version from directory: {}", dirname);
            continue;
        }

        app_dirs.push_back({entry.path(), version});
    }

    if (app_dirs.empty()) {
        return {};
    }

    auto best = std::max_element(app_dirs.begin(), app_dirs.end(),
        [](const VersionDir& a, const VersionDir& b) {
            return compare_semver(a.version, b.version) < 0;
        });

    return best->path;
}

/**
 * Get the process executable name (without path).
 */
std::string get_process_name() {
#ifdef _WIN32
    char buf[MAX_PATH];
    DWORD len = GetModuleFileNameA(nullptr, buf, MAX_PATH);
    if (len == 0) return {};
    fs::path p(std::string(buf, len));
    return p.filename().string();
#else
    // Read /proc/self/exe
    std::error_code ec;
    auto exe_path = fs::read_symlink("/proc/self/exe", ec);
    if (ec) {
        spdlog::error("Failed to read /proc/self/exe: {}", ec.message());
        return {};
    }
    return exe_path.filename().string();
#endif
}

/**
 * Get the directory containing the current executable.
 */
fs::path get_exe_directory() {
#ifdef _WIN32
    char buf[MAX_PATH];
    DWORD len = GetModuleFileNameA(nullptr, buf, MAX_PATH);
    if (len == 0) return {};
    return fs::path(std::string(buf, len)).parent_path();
#else
    std::error_code ec;
    auto exe_path = fs::read_symlink("/proc/self/exe", ec);
    if (ec) return fs::current_path();
    return exe_path.parent_path();
#endif
}

} // anonymous namespace

int stub_executable_main(int argc, char* argv[],
                          const std::map<std::string, std::string>& environment) {
    auto exe_dir = get_exe_directory();
    auto app_name = get_process_name();

    if (app_name.empty()) {
        spdlog::error("Failed to determine own executable name");
        return 1;
    }

    spdlog::debug("Stub executable: app_name={}, exe_dir={}", app_name, exe_dir.string());

    auto app_dir = find_highest_version_dir(exe_dir);
    if (app_dir.empty()) {
        spdlog::error("Could not find any app-* directories in: {}", exe_dir.string());
        return 1;
    }

    auto target_exe = app_dir / app_name;
    if (!fs::exists(target_exe)) {
        spdlog::error("Target executable not found: {}", target_exe.string());
        return 1;
    }

    spdlog::info("Launching: {}", target_exe.string());

    // Set environment variables
    for (auto& [key, value] : environment) {
#ifdef _WIN32
        SetEnvironmentVariableA(key.c_str(), value.c_str());
#else
        setenv(key.c_str(), value.c_str(), 1);
#endif
        spdlog::debug("Set environment: {}={}", key, value);
    }

#ifndef _WIN32
    // Build argv for execv
    std::vector<const char*> exec_argv;
    exec_argv.push_back(target_exe.c_str());
    for (int i = 1; i < argc; ++i) {
        exec_argv.push_back(argv[i]);
    }
    exec_argv.push_back(nullptr);

    // Fork and exec
    pid_t pid = fork();
    if (pid < 0) {
        spdlog::error("fork() failed: {}", strerror(errno));
        return 1;
    }

    if (pid == 0) {
        // Child: change to app directory and exec
        if (chdir(app_dir.string().c_str()) != 0) {
            _exit(127);
        }
        execv(target_exe.string().c_str(), const_cast<char* const*>(exec_argv.data()));
        _exit(127);
    }

    // Parent: daemonize - detach from child and exit
    spdlog::info("Process started with PID {}", pid);
    return 0;

#else
    // Windows: use CreateProcess to launch detached
    std::string cmd_line = target_exe.string();
    for (int i = 1; i < argc; ++i) {
        cmd_line += " ";
        cmd_line += argv[i];
    }

    STARTUPINFOA si{};
    PROCESS_INFORMATION pi{};
    si.cb = sizeof(si);

    if (!CreateProcessA(
            target_exe.string().c_str(),
            cmd_line.data(),
            nullptr, nullptr,
            FALSE,
            DETACHED_PROCESS,
            nullptr,
            app_dir.string().c_str(),
            &si, &pi))
    {
        spdlog::error("CreateProcess failed: {}", GetLastError());
        return 1;
    }

    spdlog::info("Process started with PID {}", pi.dwProcessId);
    CloseHandle(pi.hThread);
    CloseHandle(pi.hProcess);
    return 0;
#endif
}

} // namespace surge::supervisor
