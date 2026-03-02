#include "core/context.hpp"
#include <mutex>
#include <string>

namespace surge {

struct Context::Impl {
    mutable std::mutex mutex;
    surge_error last_error{0, nullptr};
    std::string last_error_message;
    StorageConfig storage_config;
    LockConfig lock_config;
    surge_resource_budget resource_budget{0, 0, 0, 0, 0};
    std::stop_source stop_src;
};

Context::Context() : impl_(std::make_unique<Impl>()) {}
Context::~Context() = default;
Context::Context(Context&&) noexcept = default;
Context& Context::operator=(Context&&) noexcept = default;

void Context::set_last_error(int32_t code, std::string message) {
    std::lock_guard lock(impl_->mutex);
    impl_->last_error_message = std::move(message);
    impl_->last_error.code = code;
    impl_->last_error.message = impl_->last_error_message.c_str();
}

const surge_error* Context::last_error() const {
    std::lock_guard lock(impl_->mutex);
    if (impl_->last_error.code == 0) return nullptr;
    return &impl_->last_error;
}

void Context::clear_error() {
    std::lock_guard lock(impl_->mutex);
    impl_->last_error.code = 0;
    impl_->last_error.message = nullptr;
    impl_->last_error_message.clear();
}

void Context::set_storage_config(StorageConfig config) {
    std::lock_guard lock(impl_->mutex);
    impl_->storage_config = std::move(config);
}

const StorageConfig& Context::storage_config() const {
    std::lock_guard lock(impl_->mutex);
    return impl_->storage_config;
}

void Context::set_lock_config(LockConfig config) {
    std::lock_guard lock(impl_->mutex);
    impl_->lock_config = std::move(config);
}

const LockConfig& Context::lock_config() const {
    std::lock_guard lock(impl_->mutex);
    return impl_->lock_config;
}

void Context::set_resource_budget(surge_resource_budget budget) {
    std::lock_guard lock(impl_->mutex);
    impl_->resource_budget = budget;
}

const surge_resource_budget& Context::resource_budget() const {
    std::lock_guard lock(impl_->mutex);
    return impl_->resource_budget;
}

std::stop_source& Context::stop_source() {
    return impl_->stop_src;
}

std::stop_token Context::stop_token() const {
    return impl_->stop_src.get_token();
}

void Context::cancel() {
    impl_->stop_src.request_stop();
}

bool Context::is_cancelled() const {
    return impl_->stop_src.get_token().stop_requested();
}

} // namespace surge
