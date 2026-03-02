#include "platform/pal_process.hpp"

#include <cerrno>
#include <cstring>
#include <spdlog/spdlog.h>

#ifdef _WIN32
#include <tlhelp32.h>
#include <windows.h>
#else
#include <signal.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>
extern char** environ;
#endif

namespace surge::platform {

std::optional<ProcessHandle> spawn_process(const std::filesystem::path& exe_path, const std::vector<std::string>& args,
                                           const std::filesystem::path& working_dir,
                                           const std::vector<std::string>& env) {
#ifdef _WIN32
    // Build command line
    std::wstring cmd_line = L"\"" + exe_path.wstring() + L"\"";
    for (const auto& arg : args) {
        cmd_line += L" ";
        std::wstring warg(arg.begin(), arg.end());
        cmd_line += L"\"" + warg + L"\"";
    }

    // Build environment block if needed
    std::wstring env_block;
    if (!env.empty()) {
        for (const auto& e : env) {
            std::wstring we(e.begin(), e.end());
            env_block += we;
            env_block += L'\0';
        }
        env_block += L'\0';
    }

    STARTUPINFOW si{};
    si.cb = sizeof(si);
    PROCESS_INFORMATION pi{};

    std::wstring work_dir;
    if (!working_dir.empty()) {
        work_dir = working_dir.wstring();
    }

    BOOL ok = CreateProcessW(
        nullptr, cmd_line.data(), nullptr, nullptr, FALSE, CREATE_NEW_PROCESS_GROUP | CREATE_UNICODE_ENVIRONMENT,
        env_block.empty() ? nullptr : env_block.data(), work_dir.empty() ? nullptr : work_dir.c_str(), &si, &pi);

    if (!ok) {
        spdlog::error("CreateProcess failed for '{}': error {}", exe_path.string(), GetLastError());
        return std::nullopt;
    }

    ProcessHandle handle;
    handle.pid = static_cast<int64_t>(pi.dwProcessId);
    handle.handle = pi.hProcess;
    CloseHandle(pi.hThread);

    spdlog::info("Spawned process '{}' with PID {}", exe_path.string(), handle.pid);
    return handle;

#else
    // Build argv
    std::vector<const char*> argv;
    std::string exe_str = exe_path.string();
    argv.push_back(exe_str.c_str());
    for (const auto& arg : args) {
        argv.push_back(arg.c_str());
    }
    argv.push_back(nullptr);

    // Set up file actions for working directory (posix_spawn doesn't support chdir
    // directly on all platforms, so we use fork+exec for working_dir)
    if (!working_dir.empty() || !env.empty()) {
        // Use fork+exec for full control
        pid_t pid = fork();
        if (pid < 0) {
            spdlog::error("fork() failed: {}", strerror(errno));
            return std::nullopt;
        }

        if (pid == 0) {
            // Child process
            if (!working_dir.empty()) {
                if (chdir(working_dir.c_str()) != 0) {
                    _exit(127);
                }
            }

            // Set extra environment variables
            for (const auto& e : env) {
                auto pos = e.find('=');
                if (pos != std::string::npos) {
                    setenv(e.substr(0, pos).c_str(), e.substr(pos + 1).c_str(), 1);
                }
            }

            // Create new session
            setsid();

            execv(exe_path.c_str(), const_cast<char* const*>(argv.data()));
            _exit(127);
        }

        ProcessHandle handle;
        handle.pid = static_cast<int64_t>(pid);
        spdlog::info("Spawned process '{}' with PID {}", exe_path.string(), handle.pid);
        return handle;
    }

    // Simple case: use posix_spawn
    pid_t pid = 0;
    int rc = posix_spawn(&pid, exe_path.c_str(), nullptr, nullptr, const_cast<char* const*>(argv.data()), environ);
    if (rc != 0) {
        spdlog::error("posix_spawn failed for '{}': {}", exe_path.string(), strerror(rc));
        return std::nullopt;
    }

    ProcessHandle handle;
    handle.pid = static_cast<int64_t>(pid);
    spdlog::info("Spawned process '{}' with PID {}", exe_path.string(), handle.pid);
    return handle;
#endif
}

ProcessResult wait_for_process(const ProcessHandle& handle, int timeout_ms) {
    ProcessResult result;

#ifdef _WIN32
    DWORD wait_time = timeout_ms < 0 ? INFINITE : static_cast<DWORD>(timeout_ms);
    DWORD wait_result = WaitForSingleObject(handle.handle, wait_time);

    if (wait_result == WAIT_OBJECT_0) {
        DWORD code = 0;
        GetExitCodeProcess(handle.handle, &code);
        result.exit_code = static_cast<int>(code);
        result.timed_out = false;
    } else if (wait_result == WAIT_TIMEOUT) {
        result.timed_out = true;
    } else {
        spdlog::error("WaitForSingleObject failed: error {}", GetLastError());
        result.exit_code = -1;
    }

#else
    auto pid = static_cast<pid_t>(handle.pid);

    if (timeout_ms < 0) {
        // Wait indefinitely
        int status = 0;
        pid_t rc = waitpid(pid, &status, 0);
        if (rc > 0) {
            if (WIFEXITED(status)) {
                result.exit_code = WEXITSTATUS(status);
            } else if (WIFSIGNALED(status)) {
                result.exit_code = 128 + WTERMSIG(status);
            }
        } else {
            result.exit_code = -1;
        }
        return result;
    }

    // Poll with timeout
    constexpr int poll_interval_ms = 50;
    int elapsed = 0;
    while (elapsed < timeout_ms) {
        int status = 0;
        pid_t rc = waitpid(pid, &status, WNOHANG);
        if (rc > 0) {
            if (WIFEXITED(status)) {
                result.exit_code = WEXITSTATUS(status);
            } else if (WIFSIGNALED(status)) {
                result.exit_code = 128 + WTERMSIG(status);
            }
            return result;
        }
        if (rc < 0 && errno == ECHILD) {
            // Process already reaped or not our child
            result.exit_code = -1;
            return result;
        }

        struct timespec ts{};
        ts.tv_nsec = poll_interval_ms * 1000000L;
        nanosleep(&ts, nullptr);
        elapsed += poll_interval_ms;
    }

    result.timed_out = true;
#endif

    return result;
}

bool terminate_process(const ProcessHandle& handle) {
#ifdef _WIN32
    // On Windows, there's no graceful signal, but we try GenerateConsoleCtrlEvent first
    if (GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, static_cast<DWORD>(handle.pid))) {
        return true;
    }
    return TerminateProcess(handle.handle, 1) != 0;
#else
    auto pid = static_cast<pid_t>(handle.pid);
    if (::kill(pid, SIGTERM) == 0) {
        spdlog::debug("Sent SIGTERM to PID {}", handle.pid);
        return true;
    }
    if (errno == ESRCH)
        return true;  // Already dead
    spdlog::warn("kill(SIGTERM) failed for PID {}: {}", handle.pid, strerror(errno));
    return false;
#endif
}

bool kill_process(const ProcessHandle& handle) {
#ifdef _WIN32
    if (!TerminateProcess(handle.handle, 1)) {
        spdlog::warn("TerminateProcess failed for PID {}: error {}", handle.pid, GetLastError());
        return false;
    }
    return true;
#else
    auto pid = static_cast<pid_t>(handle.pid);
    if (::kill(pid, SIGKILL) == 0) {
        spdlog::debug("Sent SIGKILL to PID {}", handle.pid);
        return true;
    }
    if (errno == ESRCH)
        return true;
    spdlog::warn("kill(SIGKILL) failed for PID {}: {}", handle.pid, strerror(errno));
    return false;
#endif
}

bool is_process_running(const ProcessHandle& handle) {
#ifdef _WIN32
    if (!handle.handle)
        return false;
    DWORD exit_code = 0;
    if (!GetExitCodeProcess(handle.handle, &exit_code))
        return false;
    return exit_code == STILL_ACTIVE;
#else
    auto pid = static_cast<pid_t>(handle.pid);
    if (pid <= 0)
        return false;
    if (::kill(pid, 0) == 0)
        return true;
    return errno != ESRCH;
#endif
}

int64_t current_pid() {
#ifdef _WIN32
    return static_cast<int64_t>(GetCurrentProcessId());
#else
    return static_cast<int64_t>(getpid());
#endif
}

int exec_replace(const std::filesystem::path& exe_path, const std::vector<std::string>& args) {
#ifdef _WIN32
    // Windows doesn't have exec - spawn new process and exit
    auto handle = spawn_process(exe_path, args);
    if (handle) {
        _exit(0);
    }
    return -1;
#else
    std::vector<const char*> argv;
    std::string exe_str = exe_path.string();
    argv.push_back(exe_str.c_str());
    for (const auto& arg : args) {
        argv.push_back(arg.c_str());
    }
    argv.push_back(nullptr);

    execv(exe_path.c_str(), const_cast<char* const*>(argv.data()));
    // Only returns on failure
    spdlog::error("execv failed for '{}': {}", exe_path.string(), strerror(errno));
    return -1;
#endif
}

}  // namespace surge::platform
