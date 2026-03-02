/**
 * @file supervisor.cpp
 * @brief Process supervisor: monitor, restart, signal handling.
 */

#include "supervisor/supervisor.hpp"
#include <spdlog/spdlog.h>
#include <fmt/format.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#ifdef _WIN32
#include <windows.h>
#include <process.h>
#else
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace surge::supervisor {

namespace fs = std::filesystem;

namespace {

std::atomic<bool> g_shutdown_requested{false};
std::atomic<pid_t> g_child_pid{0};

#ifndef _WIN32
void signal_handler(int sig) {
    spdlog::info("Supervisor received signal {}", sig);
    g_shutdown_requested.store(true);

    pid_t child = g_child_pid.load();
    if (child > 0) {
        kill(child, SIGTERM);
    }
}
#endif

} // anonymous namespace

struct Supervisor::Impl {
    std::string exe_path;
    std::string working_dir;
    std::string supervisor_id;
    std::vector<std::string> args;
    fs::path pid_file_path;
    pid_t child_pid = 0;
    bool running = false;
    int max_restarts = 10;
    std::chrono::seconds restart_delay{2};
    std::chrono::seconds min_runtime{5};
};

Supervisor::Supervisor(const std::string& exe_path,
                       const std::string& working_dir,
                       const std::string& supervisor_id)
    : impl_(std::make_unique<Impl>())
{
    impl_->exe_path = exe_path;
    impl_->working_dir = working_dir;
    impl_->supervisor_id = supervisor_id;
    impl_->pid_file_path = fs::path(working_dir) / fmt::format(".surge-supervisor-{}.pid", supervisor_id);
}

Supervisor::~Supervisor() {
    stop();
}

Supervisor::Supervisor(Supervisor&&) noexcept = default;
Supervisor& Supervisor::operator=(Supervisor&&) noexcept = default;

int32_t Supervisor::start(int argc, const char** argv) {
    // Store arguments
    impl_->args.clear();
    for (int i = 0; i < argc; ++i) {
        impl_->args.emplace_back(argv[i]);
    }

    // Install signal handlers
#ifndef _WIN32
    struct sigaction sa{};
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGTERM, &sa, nullptr);
    sigaction(SIGINT, &sa, nullptr);
#endif

    g_shutdown_requested.store(false);

    spdlog::info("Supervisor starting: id={}, exe={}", impl_->supervisor_id, impl_->exe_path);

    int restarts = 0;

    while (!g_shutdown_requested.load()) {
        auto start_time = std::chrono::steady_clock::now();

        auto rc = launch_child();
        if (rc != SURGE_OK) {
            spdlog::error("Failed to launch child process");
            return rc;
        }

        // Write PID file
        write_pid_file();

        // Monitor the child process
        int exit_code = wait_for_child();

        auto elapsed = std::chrono::steady_clock::now() - start_time;

        remove_pid_file();

        if (g_shutdown_requested.load()) {
            spdlog::info("Supervisor shutting down (signal received)");
            break;
        }

        if (exit_code == 0) {
            spdlog::info("Child process exited cleanly (code 0)");
            break;
        }

        spdlog::warn("Child process exited with code {} after {:.1f}s",
                       exit_code,
                       std::chrono::duration<double>(elapsed).count());

        // If process ran for less than min_runtime, count it as a crash
        if (elapsed < impl_->min_runtime) {
            restarts++;
            if (restarts >= impl_->max_restarts) {
                spdlog::error("Max restart limit ({}) reached, giving up", impl_->max_restarts);
                return SURGE_ERROR;
            }
        } else {
            restarts = 0; // Reset counter if process ran for a reasonable time
        }

        spdlog::info("Restarting child process in {}s (restart {}/{})",
                       impl_->restart_delay.count(), restarts, impl_->max_restarts);
        std::this_thread::sleep_for(impl_->restart_delay);
    }

    return SURGE_OK;
}

int32_t Supervisor::stop() {
    if (!impl_->running) return SURGE_OK;

    spdlog::info("Stopping supervised process");
    g_shutdown_requested.store(true);

#ifndef _WIN32
    if (impl_->child_pid > 0) {
        kill(impl_->child_pid, SIGTERM);

        // Wait up to 10 seconds for graceful shutdown
        for (int i = 0; i < 100; ++i) {
            int status;
            pid_t result = waitpid(impl_->child_pid, &status, WNOHANG);
            if (result != 0) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        // Force kill if still running
        if (kill(impl_->child_pid, 0) == 0) {
            spdlog::warn("Child process did not exit gracefully, sending SIGKILL");
            kill(impl_->child_pid, SIGKILL);
            waitpid(impl_->child_pid, nullptr, 0);
        }
    }
#else
    // Windows: use TerminateProcess as fallback
    // (In practice, the child would be signaled through other means)
#endif

    impl_->running = false;
    remove_pid_file();
    return SURGE_OK;
}

bool Supervisor::is_running() const {
    return impl_->running;
}

pid_t Supervisor::child_pid() const {
    return impl_->child_pid;
}

int32_t Supervisor::launch_child() {
#ifndef _WIN32
    pid_t pid = fork();
    if (pid < 0) {
        spdlog::error("fork() failed: {}", strerror(errno));
        return SURGE_ERROR;
    }

    if (pid == 0) {
        // Child process
        if (!impl_->working_dir.empty()) {
            if (chdir(impl_->working_dir.c_str()) != 0) {
                _exit(127);
            }
        }

        // Build argv
        std::vector<const char*> argv;
        argv.push_back(impl_->exe_path.c_str());
        for (auto& arg : impl_->args) {
            argv.push_back(arg.c_str());
        }
        argv.push_back(nullptr);

        execv(impl_->exe_path.c_str(), const_cast<char* const*>(argv.data()));
        // execv only returns on error
        _exit(127);
    }

    // Parent process
    impl_->child_pid = pid;
    g_child_pid.store(pid);
    impl_->running = true;
    spdlog::info("Launched child process with PID {}", pid);
    return SURGE_OK;

#else
    // Windows implementation
    std::string cmd = impl_->exe_path;
    for (auto& arg : impl_->args) {
        cmd += " " + arg;
    }

    STARTUPINFOA si{};
    PROCESS_INFORMATION pi{};
    si.cb = sizeof(si);

    if (!CreateProcessA(
            nullptr,
            cmd.data(),
            nullptr, nullptr,
            FALSE, 0,
            nullptr,
            impl_->working_dir.empty() ? nullptr : impl_->working_dir.c_str(),
            &si, &pi))
    {
        spdlog::error("CreateProcess failed: {}", GetLastError());
        return SURGE_ERROR;
    }

    impl_->child_pid = static_cast<pid_t>(pi.dwProcessId);
    impl_->running = true;
    CloseHandle(pi.hThread);
    CloseHandle(pi.hProcess);
    spdlog::info("Launched child process with PID {}", impl_->child_pid);
    return SURGE_OK;
#endif
}

int Supervisor::wait_for_child() {
#ifndef _WIN32
    int status = 0;
    while (true) {
        pid_t result = waitpid(impl_->child_pid, &status, 0);
        if (result == impl_->child_pid) {
            impl_->running = false;
            if (WIFEXITED(status)) {
                return WEXITSTATUS(status);
            }
            if (WIFSIGNALED(status)) {
                return 128 + WTERMSIG(status);
            }
            return -1;
        }
        if (result < 0) {
            if (errno == EINTR) continue;
            impl_->running = false;
            return -1;
        }
    }
#else
    // Windows: wait for process
    HANDLE hProcess = OpenProcess(SYNCHRONIZE | PROCESS_QUERY_INFORMATION,
                                   FALSE, static_cast<DWORD>(impl_->child_pid));
    if (!hProcess) return -1;

    WaitForSingleObject(hProcess, INFINITE);
    DWORD exit_code = 0;
    GetExitCodeProcess(hProcess, &exit_code);
    CloseHandle(hProcess);
    impl_->running = false;
    return static_cast<int>(exit_code);
#endif
}

void Supervisor::write_pid_file() {
    std::ofstream file(impl_->pid_file_path);
    if (file) {
        file << impl_->child_pid;
        spdlog::debug("Wrote PID file: {}", impl_->pid_file_path.string());
    }
}

void Supervisor::remove_pid_file() {
    std::error_code ec;
    fs::remove(impl_->pid_file_path, ec);
}

} // namespace surge::supervisor
