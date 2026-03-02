/**
 * @file pal.hpp
 * @brief Platform Abstraction Layer -- top-level header aggregating all PAL sub-modules.
 */

#pragma once

#include "pal_fs.hpp"
#include "pal_process.hpp"
#include "pal_semaphore.hpp"

namespace surge::platform {

/** Detected operating system. */
enum class OperatingSystem {
    Windows,
    Linux,
    MacOS,
    Unknown,
};

/** Detected CPU architecture. */
enum class Architecture {
    X86_64,
    Arm64,
    X86,
    Unknown,
};

/** Return the current operating system. */
OperatingSystem current_os() noexcept;

/** Return the current CPU architecture. */
Architecture current_arch() noexcept;

/** Return a runtime-identifier string (e.g. "linux-x64", "win-arm64"). */
std::string current_rid();

/** Return the number of available logical CPU cores. */
int cpu_count() noexcept;

/** Return available physical memory in bytes. */
int64_t available_memory() noexcept;

/** Return the system temporary directory. */
std::filesystem::path temp_dir();

/** Return the user's home directory. */
std::filesystem::path home_dir();

/**
 * Retrieve the value of an environment variable.
 * @return The value, or std::nullopt if the variable is not set.
 */
std::optional<std::string> get_env(std::string_view name);

/**
 * Set an environment variable for the current process.
 * @return true on success.
 */
bool set_env(std::string_view name, std::string_view value);

}  // namespace surge::platform
