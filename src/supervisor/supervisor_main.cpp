#include "platform/pal.hpp"
#include "platform/pal_fs.hpp"
#include "platform/pal_process.hpp"
#include "supervisor/supervisor.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <fstream>
#include <iostream>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <thread>

namespace {

std::atomic<bool> g_shutdown_requested{false};

void signal_handler(int sig) {
    spdlog::info("Received signal {}, initiating shutdown", sig);
    g_shutdown_requested.store(true);
}

void setup_signals() {
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);
#ifndef _WIN32
    std::signal(SIGHUP, SIG_IGN);
#endif
}

void setup_logging(const std::filesystem::path& log_dir) {
    try {
        std::error_code ec;
        std::filesystem::create_directories(log_dir, ec);

        auto console_sink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
        auto file_sink =
            std::make_shared<spdlog::sinks::basic_file_sink_mt>((log_dir / "supervisor.log").string(), false);

        auto logger = std::make_shared<spdlog::logger>("supervisor", spdlog::sinks_init_list{console_sink, file_sink});

        logger->set_level(spdlog::level::debug);
        logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] [%t] %v");
        spdlog::set_default_logger(logger);
    } catch (const std::exception& e) {
        std::cerr << "Failed to setup logging: " << e.what() << std::endl;
    }
}

struct SupervisorConfig {
    std::filesystem::path install_dir;
    std::filesystem::path executable;
    std::filesystem::path working_dir;
    std::vector<std::string> args;
    std::string supervisor_id = "surge-supervisor";
    int max_restarts = 10;
    int restart_delay_seconds = 3;
    int restart_window_seconds = 60;
    std::filesystem::path log_dir;
    std::filesystem::path pid_file;
};

SupervisorConfig parse_args(int argc, char* argv[]) {
    SupervisorConfig config;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--install-dir" && i + 1 < argc) {
            config.install_dir = argv[++i];
        } else if (arg == "--exe" && i + 1 < argc) {
            config.executable = argv[++i];
        } else if (arg == "--working-dir" && i + 1 < argc) {
            config.working_dir = argv[++i];
        } else if (arg == "--id" && i + 1 < argc) {
            config.supervisor_id = argv[++i];
        } else if (arg == "--max-restarts" && i + 1 < argc) {
            config.max_restarts = std::stoi(argv[++i]);
        } else if (arg == "--restart-delay" && i + 1 < argc) {
            config.restart_delay_seconds = std::stoi(argv[++i]);
        } else if (arg == "--restart-window" && i + 1 < argc) {
            config.restart_window_seconds = std::stoi(argv[++i]);
        } else if (arg == "--log-dir" && i + 1 < argc) {
            config.log_dir = argv[++i];
        } else if (arg == "--pid-file" && i + 1 < argc) {
            config.pid_file = argv[++i];
        } else if (arg == "--") {
            for (int j = i + 1; j < argc; ++j) {
                config.args.emplace_back(argv[j]);
            }
            break;
        }
    }

    // Defaults
    if (config.install_dir.empty() && !config.executable.empty()) {
        config.install_dir = config.executable.parent_path();
    }
    if (config.working_dir.empty()) {
        config.working_dir = config.install_dir;
    }
    if (config.log_dir.empty() && !config.install_dir.empty()) {
        config.log_dir = config.install_dir / "logs";
    }
    if (config.pid_file.empty() && !config.install_dir.empty()) {
        config.pid_file = config.install_dir / "supervisor.pid";
    }

    return config;
}

void write_pid_file(const std::filesystem::path& path) {
    if (path.empty())
        return;
    try {
        std::ofstream f(path);
        f << surge::platform::current_pid();
    } catch (const std::exception& e) {
        spdlog::warn("Failed to write PID file: {}", e.what());
    }
}

void remove_pid_file(const std::filesystem::path& path) {
    if (path.empty())
        return;
    std::error_code ec;
    std::filesystem::remove(path, ec);
}

}  // anonymous namespace

int main(int argc, char* argv[]) {
    auto config = parse_args(argc, argv);

    if (config.executable.empty()) {
        std::cerr << "Usage: surge_supervisor --exe <path> [--install-dir <dir>] "
                     "[--working-dir <dir>] [--id <name>] "
                     "[--max-restarts N] [--restart-delay N] [--log-dir <dir>] "
                     "[--pid-file <path>] [-- child-args...]"
                  << std::endl;
        return 1;
    }

    if (!config.log_dir.empty()) {
        setup_logging(config.log_dir);
    }

    setup_signals();
    write_pid_file(config.pid_file);

    spdlog::info("Surge Supervisor starting");
    spdlog::info("  Executable:  {}", config.executable.string());
    spdlog::info("  Install dir: {}", config.install_dir.string());
    spdlog::info("  Supervisor ID: {}", config.supervisor_id);
    spdlog::info("  Max restarts: {}", config.max_restarts);
    spdlog::info("  Restart delay: {}s", config.restart_delay_seconds);

    surge::supervisor::Supervisor supervisor(config.supervisor_id, config.install_dir);

    int restart_count = 0;
    auto window_start = std::chrono::steady_clock::now();

    while (!g_shutdown_requested.load()) {
        // Reset restart count if outside the window
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - window_start).count();
        if (elapsed >= config.restart_window_seconds) {
            restart_count = 0;
            window_start = now;
        }

        if (restart_count >= config.max_restarts) {
            spdlog::error("Max restarts ({}) exceeded within {} seconds, giving up", config.max_restarts,
                          config.restart_window_seconds);
            remove_pid_file(config.pid_file);
            return 1;
        }

        // Start the supervised process
        // Supervisor::start(path, path, vector<string>)
        int32_t rc = supervisor.start(config.executable, config.working_dir, config.args);

        if (rc != 0) {
            spdlog::error("Failed to start child process '{}'", config.executable.string());
            restart_count++;
            std::this_thread::sleep_for(std::chrono::seconds(config.restart_delay_seconds));
            continue;
        }

        spdlog::info("Child process started");

        // Wait for child to exit
        while (!g_shutdown_requested.load() && supervisor.is_running()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        if (g_shutdown_requested.load()) {
            spdlog::info("Shutdown requested, stopping child");
            supervisor.stop(5000);  // Supervisor::stop(int timeout_ms = 5000)
            break;
        }

        auto pinfo = supervisor.process_info();  // Supervisor::process_info() const
        spdlog::warn("Child process exited with code {}", pinfo.exit_code);

        if (pinfo.exit_code == 0) {
            spdlog::info("Child exited cleanly, supervisor shutting down");
            break;
        }

        restart_count++;
        spdlog::info("Restarting child ({}/{} in current window)", restart_count, config.max_restarts);

        // Delay before restart
        for (int i = 0; i < config.restart_delay_seconds && !g_shutdown_requested.load(); ++i) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

    spdlog::info("Surge Supervisor shutting down");
    remove_pid_file(config.pid_file);
    return 0;
}
