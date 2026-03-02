/**
 * @file pal_process.hpp
 * @brief Platform Abstraction Layer -- process management.
 */

#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

namespace surge::platform {

/** Handle to a running child process. */
struct ProcessHandle {
    int64_t pid = -1;
#ifdef _WIN32
    void* handle = nullptr;
#endif
};

/** Result of a completed process. */
struct ProcessResult {
    int exit_code = -1;
    bool timed_out = false;
};

/**
 * Spawn a child process.
 * @param exe_path    Path to the executable.
 * @param args        Command-line arguments (argv[1..]).
 * @param working_dir Working directory (empty = inherit).
 * @param env         Additional environment variables (key=value pairs).
 * @return Process handle, or std::nullopt on failure.
 */
std::optional<ProcessHandle> spawn_process(const std::filesystem::path& exe_path,
                                           const std::vector<std::string>& args = {},
                                           const std::filesystem::path& working_dir = {},
                                           const std::vector<std::string>& env = {});

/**
 * Wait for a process to exit.
 * @param handle     Process handle from spawn_process().
 * @param timeout_ms Maximum time to wait (-1 = infinite).
 * @return Process result.
 */
ProcessResult wait_for_process(const ProcessHandle& handle, int timeout_ms = -1);

/**
 * Send a graceful termination signal (SIGTERM / TerminateProcess).
 * @return true if the signal was sent successfully.
 */
bool terminate_process(const ProcessHandle& handle);

/**
 * Forcefully kill a process (SIGKILL / TerminateProcess with exit code 1).
 * @return true if the process was killed.
 */
bool kill_process(const ProcessHandle& handle);

/**
 * Check if a process is still running.
 */
bool is_process_running(const ProcessHandle& handle);

/**
 * Return the PID of the current process.
 */
int64_t current_pid();

/**
 * Replace the current process image with a new executable (POSIX execv).
 * On Windows, this spawns a new process and exits the current one.
 * @param exe_path Path to the new executable.
 * @param args     Arguments for the new process.
 * @return Only returns on failure; exit code in that case.
 */
int exec_replace(const std::filesystem::path& exe_path, const std::vector<std::string>& args);

}  // namespace surge::platform
