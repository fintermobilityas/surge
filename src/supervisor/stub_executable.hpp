/**
 * @file stub_executable.hpp
 * @brief Stub executable logic for the Surge launcher / bootstrapper.
 *
 * The stub executable is a small program placed in a well-known location.
 * On launch it finds the latest installed app directory, then exec's into
 * the real application binary.
 */

#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

namespace surge::supervisor {

/**
 * Finds and launches the latest installed application version.
 *
 * The stub is typically compiled as the user-facing executable (e.g. "myapp")
 * and placed alongside the app-* version directories.
 */
class StubExecutable {
public:
    /**
     * Construct a stub executor.
     * @param install_dir  Root installation directory containing app-* dirs.
     * @param app_main     Relative path to the main executable within an app dir
     *                     (e.g. "bin/myapp").
     */
    StubExecutable(std::filesystem::path install_dir, std::string app_main);
    ~StubExecutable();

    StubExecutable(const StubExecutable&) = delete;
    StubExecutable& operator=(const StubExecutable&) = delete;

    /**
     * Scan the install directory for app-* directories and return the path
     * to the latest version's main executable.
     * @return Full path to the executable, or std::nullopt if none found.
     */
    std::optional<std::filesystem::path> find_latest_app_dir() const;

    /**
     * Find and execute the latest application version.
     *
     * On POSIX this uses execv() and does not return on success.
     * On Windows this spawns a child process and waits.
     *
     * @param args  Arguments to pass to the application.
     * @return Exit code. On POSIX, only returns on failure.
     */
    int32_t run(const std::vector<std::string>& args = {});

    /**
     * List all installed app directories, sorted by version (newest first).
     */
    std::vector<std::filesystem::path> list_app_dirs() const;

    /**
     * Extract the version string from an app directory name.
     * @param dir_name Directory name (e.g. "app-1.2.3").
     * @return Version string, or std::nullopt if the name is not valid.
     */
    static std::optional<std::string> version_from_dir_name(const std::string& dir_name);

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace surge::supervisor
