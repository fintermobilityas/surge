#include "platform/pal.hpp"
#include <algorithm>
#include <cstdlib>
#include <optional>
#include <string>
#include <thread>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#include <sys/sysinfo.h>
#include <pwd.h>
#endif

namespace surge::platform {

OperatingSystem current_os() noexcept {
#if defined(_WIN32)
    return OperatingSystem::Windows;
#elif defined(__APPLE__)
    return OperatingSystem::MacOS;
#elif defined(__linux__)
    return OperatingSystem::Linux;
#else
    return OperatingSystem::Unknown;
#endif
}

Architecture current_arch() noexcept {
#if defined(__x86_64__) || defined(_M_X64)
    return Architecture::X86_64;
#elif defined(__aarch64__) || defined(_M_ARM64)
    return Architecture::Arm64;
#elif defined(__i386__) || defined(_M_IX86)
    return Architecture::X86;
#else
    return Architecture::Unknown;
#endif
}

std::string current_rid() {
    std::string os_part;
    switch (current_os()) {
    case OperatingSystem::Windows: os_part = "win";   break;
    case OperatingSystem::Linux:   os_part = "linux"; break;
    case OperatingSystem::MacOS:   os_part = "osx";   break;
    default:                       os_part = "unknown"; break;
    }

    std::string arch_part;
    switch (current_arch()) {
    case Architecture::X86_64: arch_part = "x64";   break;
    case Architecture::Arm64:  arch_part = "arm64"; break;
    case Architecture::X86:    arch_part = "x86";   break;
    default:                   arch_part = "unknown"; break;
    }

    return os_part + "-" + arch_part;
}

int cpu_count() noexcept {
    int count = static_cast<int>(std::thread::hardware_concurrency());
    return count > 0 ? count : 1;
}

int64_t available_memory() noexcept {
#ifdef _WIN32
    MEMORYSTATUSEX status;
    status.dwLength = sizeof(status);
    if (GlobalMemoryStatusEx(&status)) {
        return static_cast<int64_t>(status.ullAvailPhys);
    }
    return 0;
#else
    struct sysinfo info {};
    if (sysinfo(&info) == 0) {
        return static_cast<int64_t>(info.freeram) * static_cast<int64_t>(info.mem_unit);
    }
    return 0;
#endif
}

std::filesystem::path temp_dir() {
    std::error_code ec;
    auto tmp = std::filesystem::temp_directory_path(ec);
    if (ec) return "/tmp";
    return tmp;
}

std::filesystem::path home_dir() {
#ifdef _WIN32
    if (auto* val = std::getenv("USERPROFILE")) {
        return val;
    }
    // Fallback: combine HOMEDRIVE + HOMEPATH
    auto* drive = std::getenv("HOMEDRIVE");
    auto* path = std::getenv("HOMEPATH");
    if (drive && path) {
        return std::string(drive) + path;
    }
    return "C:\\";
#else
    if (auto* val = std::getenv("HOME")) {
        return val;
    }
    // Fallback to passwd entry
    auto* pw = getpwuid(getuid());
    if (pw && pw->pw_dir) {
        return pw->pw_dir;
    }
    return "/tmp";
#endif
}

std::optional<std::string> get_env(std::string_view name) {
    std::string name_str(name);
    auto* val = std::getenv(name_str.c_str());
    if (!val) return std::nullopt;
    return std::string(val);
}

bool set_env(std::string_view name, std::string_view value) {
    std::string name_str(name);
    std::string value_str(value);
#ifdef _WIN32
    return _putenv_s(name_str.c_str(), value_str.c_str()) == 0;
#else
    return setenv(name_str.c_str(), value_str.c_str(), 1) == 0;
#endif
}

} // namespace surge::platform
