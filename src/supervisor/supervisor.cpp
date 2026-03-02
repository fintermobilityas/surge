/**
 * @file supervisor.cpp
 * @brief Process supervisor: monitor, restart, signal handling.
 */

#include "supervisor/supervisor.hpp"

#include "platform/pal_process.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstring>
#include <filesystem>
#include <fmt/format.h>
#include <fstream>
#include <spdlog/spdlog.h>
#include <string>
#include <thread>
#include <vector>

namespace surge::supervisor {

namespace fs = std::filesystem;

struct Supervisor::Impl {
    std::string supervisor_id;
    fs::path install_dir;

    // Current process state
    fs::path current_exe_path;
    fs::path current_working_dir;
    std::vector<std::string> current_args;

    platform::ProcessHandle process_handle;
    ProcessInfo info;
    ExitCallback exit_callback;
    bool running = false;
};

Supervisor::Supervisor(std::string supervisor_id, std::filesystem::path install_dir) : impl_(std::make_unique<Impl>()) {
    impl_->supervisor_id = std::move(supervisor_id);
    impl_->install_dir = std::move(install_dir);
    impl_->info.state = ProcessState::Stopped;
}

Supervisor::~Supervisor() {
    if (impl_ && impl_->running) {
        stop();
    }
}

int32_t Supervisor::start(const std::filesystem::path& exe_path, const std::filesystem::path& working_dir,
                          const std::vector<std::string>& args) {
    if (impl_->running) {
        spdlog::warn("Supervisor: process already running, stopping first");
        stop();
    }

    impl_->current_exe_path = exe_path;
    impl_->current_working_dir = working_dir;
    impl_->current_args = args;

    spdlog::info("Supervisor starting: id={}, exe={}", impl_->supervisor_id, exe_path.string());

    impl_->info.state = ProcessState::Starting;
    impl_->info.exe_path = exe_path;
    impl_->info.working_dir = working_dir;

    auto handle = platform::spawn_process(exe_path, args, working_dir);
    if (!handle) {
        spdlog::error("Failed to spawn child process: {}", exe_path.string());
        impl_->info.state = ProcessState::Crashed;
        return -1;
    }

    impl_->process_handle = *handle;
    impl_->info.pid = static_cast<int>(handle->pid);
    impl_->info.state = ProcessState::Running;
    impl_->running = true;

    spdlog::info("Launched child process with PID {}", handle->pid);
    return 0;
}

int32_t Supervisor::stop(int timeout_ms) {
    if (!impl_->running)
        return 0;

    spdlog::info("Stopping supervised process (PID {}, timeout {}ms)", impl_->info.pid, timeout_ms);

    impl_->info.state = ProcessState::Stopping;

    // Send graceful termination signal
    platform::terminate_process(impl_->process_handle);

    // Wait for process to exit
    auto result = platform::wait_for_process(impl_->process_handle, timeout_ms);

    if (result.timed_out) {
        spdlog::warn("Child process did not exit gracefully, force killing");
        platform::kill_process(impl_->process_handle);
        result = platform::wait_for_process(impl_->process_handle, 5000);
    }

    impl_->info.exit_code = result.exit_code;
    impl_->info.state = ProcessState::Stopped;
    impl_->running = false;

    if (impl_->exit_callback) {
        impl_->exit_callback(result.exit_code);
    }

    spdlog::info("Child process stopped with exit code {}", result.exit_code);
    return 0;
}

int32_t Supervisor::restart(const std::filesystem::path& new_exe_path, const std::vector<std::string>& new_args) {
    spdlog::info("Restarting supervised process");

    // Stop current process
    if (impl_->running) {
        stop();
    }

    // Use new exe path if provided, otherwise reuse current
    auto exe = new_exe_path.empty() ? impl_->current_exe_path : new_exe_path;
    auto args = new_args.empty() ? impl_->current_args : new_args;

    return start(exe, impl_->current_working_dir, args);
}

bool Supervisor::is_running() const {
    if (!impl_->running)
        return false;
    return platform::is_process_running(impl_->process_handle);
}

ProcessInfo Supervisor::process_info() const {
    return impl_->info;
}

void Supervisor::on_exit(ExitCallback callback) {
    impl_->exit_callback = std::move(callback);
}

const std::string& Supervisor::supervisor_id() const {
    return impl_->supervisor_id;
}

}  // namespace surge::supervisor
