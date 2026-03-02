/**
 * @file supervisor.hpp
 * @brief Process supervisor for managed application lifecycle.
 */

#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace surge::supervisor {

/** State of the supervised process. */
enum class ProcessState {
    Stopped,
    Starting,
    Running,
    Stopping,
    Crashed,
};

/** Information about the supervised process. */
struct ProcessInfo {
    ProcessState state = ProcessState::Stopped;
    int          pid   = -1;
    int          exit_code = 0;
    std::string  version;
    std::filesystem::path exe_path;
    std::filesystem::path working_dir;
};

/** Callback invoked when the supervised process exits. */
using ExitCallback = std::function<void(int exit_code)>;

/**
 * Manages the lifecycle of a child application process.
 *
 * The supervisor starts, monitors, and optionally restarts the application.
 * During updates, it coordinates stopping the old process and starting the
 * new version.
 */
class Supervisor {
public:
    /**
     * Construct a supervisor.
     * @param supervisor_id Unique identifier for this supervisor instance.
     * @param install_dir   Root installation directory (contains app-* dirs).
     */
    Supervisor(std::string supervisor_id,
               std::filesystem::path install_dir);
    ~Supervisor();

    Supervisor(const Supervisor&) = delete;
    Supervisor& operator=(const Supervisor&) = delete;

    /**
     * Start the application.
     * @param exe_path    Path to the executable.
     * @param working_dir Working directory for the child.
     * @param args        Command-line arguments.
     * @return 0 on success, negative error code on failure.
     */
    int32_t start(const std::filesystem::path& exe_path,
                  const std::filesystem::path& working_dir,
                  const std::vector<std::string>& args = {});

    /**
     * Stop the supervised process gracefully.
     * @param timeout_ms  Maximum time to wait for graceful shutdown.
     * @return 0 on success, negative error code on failure.
     */
    int32_t stop(int timeout_ms = 5000);

    /**
     * Restart the supervised process, optionally with a new executable path.
     * @param new_exe_path If non-empty, switch to this executable.
     * @param new_args     If non-empty, use these arguments.
     * @return 0 on success, negative error code on failure.
     */
    int32_t restart(const std::filesystem::path& new_exe_path = {},
                    const std::vector<std::string>& new_args = {});

    /** Return true if the supervised process is currently running. */
    bool is_running() const;

    /** Return current process information. */
    ProcessInfo process_info() const;

    /** Register a callback for when the supervised process exits. */
    void on_exit(ExitCallback callback);

    /** Return the supervisor identifier. */
    const std::string& supervisor_id() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace surge::supervisor
