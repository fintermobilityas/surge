#include "platform/pal_semaphore.hpp"

#include <fmt/format.h>
#include <spdlog/spdlog.h>
#include <stdexcept>

#ifdef _WIN32
#include <windows.h>
#else
#include <cerrno>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <semaphore.h>
#endif

namespace surge::platform {

struct NamedSemaphore::Impl {
    std::string name_;
    bool held_ = false;
#ifdef _WIN32
    HANDLE handle = nullptr;
#else
    sem_t* handle = SEM_FAILED;
#endif
};

NamedSemaphore::NamedSemaphore(std::string_view name, int32_t initial_count) : impl_(std::make_unique<Impl>()) {
    impl_->name_ = std::string(name);

#ifdef _WIN32
    std::string win_name = "Global\\" + impl_->name_;
    impl_->handle =
        CreateSemaphoreA(nullptr, static_cast<LONG>(initial_count), static_cast<LONG>(initial_count), win_name.c_str());
    if (!impl_->handle) {
        throw std::runtime_error(fmt::format("Failed to create semaphore '{}': error {}", name, GetLastError()));
    }
    spdlog::debug("Created Windows semaphore '{}' (count={})", name, initial_count);

#else
    std::string posix_name = "/" + impl_->name_;
    impl_->handle = sem_open(posix_name.c_str(), O_CREAT, 0644, static_cast<unsigned int>(initial_count));
    if (impl_->handle == SEM_FAILED) {
        throw std::runtime_error(fmt::format("Failed to create semaphore '{}': {}", name, strerror(errno)));
    }
    spdlog::debug("Created POSIX semaphore '{}' (count={})", name, initial_count);
#endif
}

NamedSemaphore::~NamedSemaphore() {
    if (!impl_)
        return;

    if (impl_->held_) {
        release();
    }

#ifdef _WIN32
    if (impl_->handle) {
        CloseHandle(impl_->handle);
        spdlog::debug("Closed Windows semaphore '{}'", impl_->name_);
    }
#else
    if (impl_->handle != SEM_FAILED) {
        sem_close(impl_->handle);
        spdlog::debug("Closed POSIX semaphore '{}'", impl_->name_);
    }
#endif
}

bool NamedSemaphore::try_acquire() {
#ifdef _WIN32
    DWORD result = WaitForSingleObject(impl_->handle, 0);
    if (result == WAIT_OBJECT_0) {
        impl_->held_ = true;
        spdlog::debug("Acquired semaphore '{}'", impl_->name_);
        return true;
    }
    return false;
#else
    if (sem_trywait(impl_->handle) == 0) {
        impl_->held_ = true;
        spdlog::debug("Acquired semaphore '{}'", impl_->name_);
        return true;
    }
    return false;
#endif
}

bool NamedSemaphore::acquire(int32_t timeout_ms) {
#ifdef _WIN32
    DWORD wait_time = timeout_ms < 0 ? INFINITE : static_cast<DWORD>(timeout_ms);
    DWORD result = WaitForSingleObject(impl_->handle, wait_time);
    if (result == WAIT_OBJECT_0) {
        impl_->held_ = true;
        spdlog::debug("Acquired semaphore '{}'", impl_->name_);
        return true;
    }
    if (result == WAIT_TIMEOUT) {
        spdlog::debug("Semaphore '{}' acquire timed out", impl_->name_);
        return false;
    }
    spdlog::error("WaitForSingleObject failed for '{}': error {}", impl_->name_, GetLastError());
    return false;

#else
    if (timeout_ms < 0) {
        // Wait indefinitely
        while (sem_wait(impl_->handle) != 0) {
            if (errno != EINTR) {
                spdlog::error("sem_wait failed for '{}': {}", impl_->name_, strerror(errno));
                return false;
            }
        }
        impl_->held_ = true;
        spdlog::debug("Acquired semaphore '{}'", impl_->name_);
        return true;
    }

    if (timeout_ms == 0) {
        return try_acquire();
    }

    // Timed wait
    struct timespec ts{};
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += timeout_ms / 1000;
    ts.tv_nsec += (timeout_ms % 1000) * 1000000L;
    if (ts.tv_nsec >= 1000000000L) {
        ts.tv_sec += 1;
        ts.tv_nsec -= 1000000000L;
    }

    while (sem_timedwait(impl_->handle, &ts) != 0) {
        if (errno == ETIMEDOUT) {
            spdlog::debug("Semaphore '{}' acquire timed out", impl_->name_);
            return false;
        }
        if (errno != EINTR) {
            spdlog::error("sem_timedwait failed for '{}': {}", impl_->name_, strerror(errno));
            return false;
        }
    }

    impl_->held_ = true;
    spdlog::debug("Acquired semaphore '{}'", impl_->name_);
    return true;
#endif
}

bool NamedSemaphore::release() {
#ifdef _WIN32
    if (!ReleaseSemaphore(impl_->handle, 1, nullptr)) {
        spdlog::error("ReleaseSemaphore failed for '{}': error {}", impl_->name_, GetLastError());
        return false;
    }
    impl_->held_ = false;
    spdlog::debug("Released semaphore '{}'", impl_->name_);
    return true;
#else
    if (sem_post(impl_->handle) != 0) {
        spdlog::error("sem_post failed for '{}': {}", impl_->name_, strerror(errno));
        return false;
    }
    impl_->held_ = false;
    spdlog::debug("Released semaphore '{}'", impl_->name_);
    return true;
#endif
}

bool NamedSemaphore::is_held() const {
    return impl_->held_;
}

const std::string& NamedSemaphore::name() const {
    return impl_->name_;
}

void NamedSemaphore::unlink(std::string_view name) {
#ifdef _WIN32
    // Windows semaphores are cleaned up when all handles are closed
    spdlog::debug("Windows semaphore '{}' will be cleaned up on handle close", name);
#else
    std::string posix_name = "/" + std::string(name);
    if (sem_unlink(posix_name.c_str()) != 0 && errno != ENOENT) {
        spdlog::warn("sem_unlink failed for '{}': {}", name, strerror(errno));
    } else {
        spdlog::debug("Unlinked POSIX semaphore '{}'", name);
    }
#endif
}

}  // namespace surge::platform
