/**
 * @file main.cpp
 * @brief CLI entry point for the Surge update framework.
 *
 * Dispatches to subcommands: init, pack, push, promote, demote, list,
 * restore, lock, unlock, migrate.
 */

#include "config/constants.hpp"

#include <cxxopts.hpp>
#include <functional>
#include <iostream>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <string>
#include <unordered_map>

// Forward declarations for command handlers
int cmd_init(int argc, char* argv[]);
int cmd_pack(int argc, char* argv[]);
int cmd_push(int argc, char* argv[]);
int cmd_promote(int argc, char* argv[]);
int cmd_demote(int argc, char* argv[]);
int cmd_list(int argc, char* argv[]);
int cmd_restore(int argc, char* argv[]);
int cmd_lock(int argc, char* argv[]);
int cmd_unlock(int argc, char* argv[]);
int cmd_migrate(int argc, char* argv[]);

namespace {

void print_usage() {
    std::cout << "surge " << surge::constants::VERSION << " - binary delta update framework\n"
              << "\n"
              << "Usage: surge <command> [options]\n"
              << "\n"
              << "Commands:\n"
              << "  init       Initialize a new surge project\n"
              << "  pack       Build full and delta packages from artifacts\n"
              << "  push       Upload packages to cloud storage\n"
              << "  promote    Add a release to a channel\n"
              << "  demote     Remove a release from a channel\n"
              << "  list       List releases and their channels\n"
              << "  restore    Reconstruct a full package from delta chain\n"
              << "  lock       Acquire a distributed lock\n"
              << "  unlock     Release a distributed lock\n"
              << "  migrate    Migrate configuration from snapx\n"
              << "\n"
              << "Options:\n"
              << "  --version  Show version information\n"
              << "  --help     Show this help message\n"
              << "\n"
              << "Run 'surge <command> --help' for command-specific options.\n";
}

void setup_logging(bool verbose) {
    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    auto logger = std::make_shared<spdlog::logger>("surge", console_sink);

    if (verbose) {
        logger->set_level(spdlog::level::debug);
    } else {
        logger->set_level(spdlog::level::info);
    }

    logger->set_pattern("[%^%l%$] %v");
    spdlog::set_default_logger(logger);
}

}  // anonymous namespace

int main(int argc, char* argv[]) {
    // Handle no-argument case
    if (argc < 2) {
        print_usage();
        return 1;
    }

    std::string first_arg = argv[1];

    // Handle top-level flags before command dispatch
    if (first_arg == "--version" || first_arg == "-V") {
        std::cout << "surge " << surge::constants::VERSION << "\n";
        return 0;
    }

    if (first_arg == "--help" || first_arg == "-h") {
        print_usage();
        return 0;
    }

    // Check for --verbose anywhere in the arguments
    bool verbose = false;
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "--verbose" || std::string(argv[i]) == "-v") {
            verbose = true;
            break;
        }
    }

    setup_logging(verbose);

    // Build dispatch table
    static const std::unordered_map<std::string, std::function<int(int, char*[])>> commands = {
        {"init", cmd_init},     {"pack", cmd_pack},       {"push", cmd_push},       {"promote", cmd_promote},
        {"demote", cmd_demote}, {"list", cmd_list},       {"restore", cmd_restore}, {"lock", cmd_lock},
        {"unlock", cmd_unlock}, {"migrate", cmd_migrate},
    };

    auto it = commands.find(first_arg);
    if (it == commands.end()) {
        std::cerr << "surge: unknown command '" << first_arg << "'\n\n";
        print_usage();
        return 1;
    }

    // Shift argv so subcommand sees itself as argv[0]
    // e.g., "surge pack --app foo" becomes "surge pack --app foo" with argc-1, argv+1
    try {
        return it->second(argc - 1, argv + 1);
    } catch (const std::exception& ex) {
        spdlog::error("{}", ex.what());
        return 1;
    }
}
