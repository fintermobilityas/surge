#include "surge/surge_api.h"
#include "core/context.hpp"
#include "core/error.hpp"
#include "update/update_manager.hpp"
#include "pack/pack_builder.hpp"
#include "diff/bsdiff_wrapper.hpp"
#include "lock/distributed_mutex.hpp"
#include "supervisor/supervisor.hpp"
#include "releases/release_manifest.hpp"
#include "platform/pal.hpp"
#include "platform/pal_process.hpp"
#include <spdlog/spdlog.h>
#include <cstring>
#include <cstdlib>
#include <string>
#include <memory>

// ----- internal handle helpers -----

static surge::Context* to_ctx(surge_context* ctx) {
    return reinterpret_cast<surge::Context*>(ctx);
}

static const surge::Context* to_ctx(const surge_context* ctx) {
    return reinterpret_cast<const surge::Context*>(ctx);
}

static surge_result set_error(surge::Context* ctx, surge::ErrorCode code,
                              const std::string& msg) {
    if (ctx) ctx->set_last_error(static_cast<int32_t>(code), msg);
    return static_cast<surge_result>(surge::to_surge_result(code));
}

// ----- Update manager wrapper -----

struct surge_update_manager_wrapper {
    std::unique_ptr<surge::update::UpdateManager> mgr;
    std::optional<surge::update::UpdateInfo> last_check;
    surge::Context* ctx;
};

// ----- Releases info wrapper -----

struct surge_releases_info_wrapper {
    std::vector<surge::releases::ReleaseEntry> releases;
    std::string latest_version;
};

static surge_releases_info_wrapper* to_info(surge_releases_info* i) {
    return reinterpret_cast<surge_releases_info_wrapper*>(i);
}

static const surge_releases_info_wrapper* to_info(const surge_releases_info* i) {
    return reinterpret_cast<const surge_releases_info_wrapper*>(i);
}

// ----- Pack context wrapper -----

struct surge_pack_context_wrapper {
    std::unique_ptr<surge::pack::PackBuilder> builder;
    surge::Context* ctx;
};

extern "C" {

// ===== Lifecycle =====

SURGE_API surge_context* SURGE_CALL surge_context_create(void) {
    try {
        auto* ctx = new surge::Context();
        spdlog::debug("surge_context_create: created context");
        return reinterpret_cast<surge_context*>(ctx);
    } catch (const std::exception& e) {
        spdlog::error("surge_context_create failed: {}", e.what());
        return nullptr;
    }
}

SURGE_API void SURGE_CALL surge_context_destroy(surge_context* ctx) {
    if (!ctx) return;
    try {
        delete to_ctx(ctx);
    } catch (...) {}
}

SURGE_API const surge_error* SURGE_CALL surge_context_last_error(
    const surge_context* ctx) {
    if (!ctx) return nullptr;
    try {
        return to_ctx(ctx)->last_error();
    } catch (...) {
        return nullptr;
    }
}

// ===== Configuration =====

SURGE_API surge_result SURGE_CALL surge_config_set_storage(
    surge_context* ctx,
    surge_storage_provider provider,
    const char* bucket,
    const char* region,
    const char* access_key,
    const char* secret_key,
    const char* endpoint) {
    if (!ctx) return SURGE_ERROR;

    try {
        surge::StorageConfig sc;
        sc.provider = provider;
        if (bucket)     sc.bucket = bucket;
        if (region)     sc.region = region;
        if (access_key) sc.access_key = access_key;
        if (secret_key) sc.secret_key = secret_key;
        if (endpoint)   sc.endpoint = endpoint;

        to_ctx(ctx)->set_storage_config(std::move(sc));
        spdlog::debug("Storage configured: provider={}", static_cast<int>(provider));
        return SURGE_OK;
    } catch (const std::exception& e) {
        return set_error(to_ctx(ctx), surge::ErrorCode::StorageError, e.what());
    }
}

SURGE_API surge_result SURGE_CALL surge_config_set_lock_server(
    surge_context* ctx,
    const char* url) {
    if (!ctx || !url) return SURGE_ERROR;

    try {
        surge::LockConfig lc;
        lc.server_url = url;
        to_ctx(ctx)->set_lock_config(std::move(lc));
        spdlog::debug("Lock server configured: {}", url);
        return SURGE_OK;
    } catch (const std::exception& e) {
        return set_error(to_ctx(ctx), surge::ErrorCode::LockFailed, e.what());
    }
}

SURGE_API surge_result SURGE_CALL surge_config_set_resource_budget(
    surge_context* ctx,
    const surge_resource_budget* budget) {
    if (!ctx || !budget) return SURGE_ERROR;

    try {
        to_ctx(ctx)->set_resource_budget(*budget);
        spdlog::debug("Resource budget set: threads={}, downloads={}",
                      budget->max_threads, budget->max_concurrent_downloads);
        return SURGE_OK;
    } catch (const std::exception& e) {
        return set_error(to_ctx(ctx), surge::ErrorCode::Unknown, e.what());
    }
}

// ===== Update manager =====

SURGE_API surge_update_manager* SURGE_CALL surge_update_manager_create(
    surge_context* ctx,
    const char* app_id,
    const char* current_version,
    const char* channel,
    const char* install_dir) {
    if (!ctx || !app_id || !current_version || !channel || !install_dir) return nullptr;

    try {
        auto wrapper = std::make_unique<surge_update_manager_wrapper>();
        wrapper->ctx = to_ctx(ctx);
        wrapper->mgr = std::make_unique<surge::update::UpdateManager>(
            *to_ctx(ctx), app_id, current_version, channel, install_dir);

        spdlog::debug("Update manager created: app={}, version={}, channel={}",
                      app_id, current_version, channel);
        return reinterpret_cast<surge_update_manager*>(wrapper.release());
    } catch (const std::exception& e) {
        set_error(to_ctx(ctx), surge::ErrorCode::Unknown, e.what());
        return nullptr;
    }
}

SURGE_API void SURGE_CALL surge_update_manager_destroy(surge_update_manager* mgr) {
    if (!mgr) return;
    try {
        delete reinterpret_cast<surge_update_manager_wrapper*>(mgr);
    } catch (...) {}
}

SURGE_API surge_result SURGE_CALL surge_update_check(
    surge_update_manager* mgr,
    surge_releases_info** info) {
    if (!mgr || !info) return SURGE_ERROR;

    auto* wrapper = reinterpret_cast<surge_update_manager_wrapper*>(mgr);

    try {
        auto result = wrapper->mgr->check_for_updates();
        if (!result) {
            *info = nullptr;
            return SURGE_NOT_FOUND;
        }

        auto info_wrapper = std::make_unique<surge_releases_info_wrapper>();
        info_wrapper->releases = std::move(result->available_releases);
        info_wrapper->latest_version = std::move(result->latest_version);

        wrapper->last_check = std::move(result);
        *info = reinterpret_cast<surge_releases_info*>(info_wrapper.release());
        return SURGE_OK;
    } catch (const std::exception& e) {
        set_error(wrapper->ctx, surge::ErrorCode::NetworkError, e.what());
        return SURGE_ERROR;
    }
}

SURGE_API surge_result SURGE_CALL surge_update_download_and_apply(
    surge_update_manager* mgr,
    const surge_releases_info* info,
    surge_progress_callback progress_cb,
    void* user_data) {
    if (!mgr) return SURGE_ERROR;

    auto* wrapper = reinterpret_cast<surge_update_manager_wrapper*>(mgr);

    try {
        if (!wrapper->last_check) {
            return set_error(wrapper->ctx, surge::ErrorCode::InvalidArgument,
                             "No update info available, call surge_update_check first");
        }

        surge::update::ProgressCallback cb;
        if (progress_cb) {
            cb = [progress_cb, user_data](const surge_progress& p) {
                progress_cb(&p, user_data);
            };
        }

        int32_t rc = wrapper->mgr->download_and_apply(
            *wrapper->last_check, cb);
        return static_cast<surge_result>(rc);
    } catch (const std::exception& e) {
        return set_error(wrapper->ctx, surge::ErrorCode::DownloadFailed, e.what());
    }
}

// ===== Release-info accessors =====

SURGE_API int32_t SURGE_CALL surge_releases_count(const surge_releases_info* info) {
    if (!info) return 0;
    return static_cast<int32_t>(to_info(info)->releases.size());
}

SURGE_API void SURGE_CALL surge_releases_destroy(surge_releases_info* info) {
    if (!info) return;
    delete reinterpret_cast<surge_releases_info_wrapper*>(info);
}

SURGE_API const char* SURGE_CALL surge_release_version(
    const surge_releases_info* info, int32_t index) {
    if (!info) return nullptr;
    auto* w = to_info(info);
    if (index < 0 || static_cast<size_t>(index) >= w->releases.size()) return nullptr;
    return w->releases[static_cast<size_t>(index)].version.c_str();
}

SURGE_API const char* SURGE_CALL surge_release_channel(
    const surge_releases_info* info, int32_t index) {
    if (!info) return nullptr;
    auto* w = to_info(info);
    if (index < 0 || static_cast<size_t>(index) >= w->releases.size()) return nullptr;
    auto& rel = w->releases[static_cast<size_t>(index)];
    if (rel.channels.empty()) return nullptr;
    return rel.channels.front().c_str();
}

SURGE_API int64_t SURGE_CALL surge_release_full_size(
    const surge_releases_info* info, int32_t index) {
    if (!info) return 0;
    auto* w = to_info(info);
    if (index < 0 || static_cast<size_t>(index) >= w->releases.size()) return 0;
    return w->releases[static_cast<size_t>(index)].full_size;  // matches ReleaseEntry::full_size
}

SURGE_API int32_t SURGE_CALL surge_release_is_genesis(
    const surge_releases_info* info, int32_t index) {
    if (!info) return 0;
    auto* w = to_info(info);
    if (index < 0 || static_cast<size_t>(index) >= w->releases.size()) return 0;
    return w->releases[static_cast<size_t>(index)].is_genesis ? 1 : 0;
}

// ===== Binary diff / patch =====

SURGE_API surge_result SURGE_CALL surge_bsdiff(surge_bsdiff_ctx* ctx) {
    if (!ctx || !ctx->older || !ctx->newer) return SURGE_ERROR;

    try {
        auto result = surge::diff::bsdiff(
            {ctx->older, static_cast<size_t>(ctx->older_size)},
            {ctx->newer, static_cast<size_t>(ctx->newer_size)});

        auto* buf = static_cast<uint8_t*>(std::malloc(result.patch_data.size()));
        if (!buf) {
            ctx->status = -1;
            return SURGE_ERROR;
        }
        std::memcpy(buf, result.patch_data.data(), result.patch_data.size());
        ctx->patch = buf;
        ctx->patch_size = static_cast<int64_t>(result.patch_data.size());
        ctx->status = 0;
        return SURGE_OK;
    } catch (const std::exception& e) {
        spdlog::error("bsdiff failed: {}", e.what());
        ctx->status = -1;
        return SURGE_ERROR;
    }
}

SURGE_API surge_result SURGE_CALL surge_bspatch(surge_bspatch_ctx* ctx) {
    if (!ctx || !ctx->older || !ctx->patch) return SURGE_ERROR;

    try {
        auto result = surge::diff::bspatch(
            {ctx->older, static_cast<size_t>(ctx->older_size)},
            {ctx->patch, static_cast<size_t>(ctx->patch_size)});

        auto* buf = static_cast<uint8_t*>(std::malloc(result.new_data.size()));
        if (!buf) {
            ctx->status = -1;
            return SURGE_ERROR;
        }
        std::memcpy(buf, result.new_data.data(), result.new_data.size());
        ctx->newer = buf;
        ctx->newer_size = static_cast<int64_t>(result.new_data.size());
        ctx->status = 0;
        return SURGE_OK;
    } catch (const std::exception& e) {
        spdlog::error("bspatch failed: {}", e.what());
        ctx->status = -1;
        return SURGE_ERROR;
    }
}

SURGE_API void SURGE_CALL surge_bsdiff_free(surge_bsdiff_ctx* ctx) {
    if (!ctx) return;
    std::free(ctx->patch);
    ctx->patch = nullptr;
    ctx->patch_size = 0;
}

SURGE_API void SURGE_CALL surge_bspatch_free(surge_bspatch_ctx* ctx) {
    if (!ctx) return;
    std::free(ctx->newer);
    ctx->newer = nullptr;
    ctx->newer_size = 0;
}

// ===== Pack builder =====

SURGE_API surge_pack_context* SURGE_CALL surge_pack_create(
    surge_context* ctx,
    const char* manifest_path,
    const char* app_id,
    const char* rid,
    const char* version,
    const char* artifacts_dir) {
    if (!ctx || !manifest_path || !app_id || !rid || !version || !artifacts_dir)
        return nullptr;

    try {
        auto wrapper = std::make_unique<surge_pack_context_wrapper>();
        wrapper->ctx = to_ctx(ctx);
        wrapper->builder = std::make_unique<surge::pack::PackBuilder>(
            *to_ctx(ctx), manifest_path, app_id, rid, version, artifacts_dir);

        spdlog::debug("Pack context created: app={}, version={}, rid={}",
                      app_id, version, rid);
        return reinterpret_cast<surge_pack_context*>(wrapper.release());
    } catch (const std::exception& e) {
        set_error(to_ctx(ctx), surge::ErrorCode::Unknown, e.what());
        return nullptr;
    }
}

SURGE_API surge_result SURGE_CALL surge_pack_build(
    surge_pack_context* pack_ctx,
    surge_progress_callback progress_cb,
    void* user_data) {
    if (!pack_ctx) return SURGE_ERROR;

    auto* wrapper = reinterpret_cast<surge_pack_context_wrapper*>(pack_ctx);

    try {
        surge::pack::ProgressCallback cb;
        if (progress_cb) {
            cb = [progress_cb, user_data](const surge_progress& p) {
                progress_cb(&p, user_data);
            };
        }

        int32_t rc = wrapper->builder->build(cb);
        return static_cast<surge_result>(rc);
    } catch (const std::exception& e) {
        return set_error(wrapper->ctx, surge::ErrorCode::Unknown, e.what());
    }
}

SURGE_API surge_result SURGE_CALL surge_pack_push(
    surge_pack_context* pack_ctx,
    const char* channel,
    surge_progress_callback progress_cb,
    void* user_data) {
    if (!pack_ctx || !channel) return SURGE_ERROR;

    auto* wrapper = reinterpret_cast<surge_pack_context_wrapper*>(pack_ctx);

    try {
        surge::pack::ProgressCallback cb;
        if (progress_cb) {
            cb = [progress_cb, user_data](const surge_progress& p) {
                progress_cb(&p, user_data);
            };
        }

        int32_t rc = wrapper->builder->push(channel, cb);
        return static_cast<surge_result>(rc);
    } catch (const std::exception& e) {
        return set_error(wrapper->ctx, surge::ErrorCode::UploadFailed, e.what());
    }
}

SURGE_API void SURGE_CALL surge_pack_destroy(surge_pack_context* pack_ctx) {
    if (!pack_ctx) return;
    delete reinterpret_cast<surge_pack_context_wrapper*>(pack_ctx);
}

// ===== Distributed lock =====

SURGE_API surge_result SURGE_CALL surge_lock_acquire(
    surge_context* ctx,
    const char* name,
    int32_t timeout_seconds,
    char** challenge_out) {
    if (!ctx || !name || !challenge_out) return SURGE_ERROR;

    try {
        auto mutex = std::make_unique<surge::lock::DistributedMutex>(
            *to_ctx(ctx), name);

        bool acquired = mutex->try_acquire(timeout_seconds);
        if (!acquired) {
            return set_error(to_ctx(ctx), surge::ErrorCode::LockFailed,
                             "Failed to acquire lock");
        }

        auto challenge = mutex->challenge();
        if (challenge) {
            *challenge_out = static_cast<char*>(
                std::malloc(challenge->size() + 1));
            if (*challenge_out) {
                std::memcpy(*challenge_out, challenge->c_str(),
                            challenge->size() + 1);
            }
        } else {
            *challenge_out = nullptr;
        }

        // Note: In a real implementation, we'd store the mutex handle somewhere
        // so it can be released later. For now, the lock is held by the server.
        return SURGE_OK;
    } catch (const std::exception& e) {
        return set_error(to_ctx(ctx), surge::ErrorCode::LockFailed, e.what());
    }
}

SURGE_API surge_result SURGE_CALL surge_lock_release(
    surge_context* ctx,
    const char* name,
    const char* challenge) {
    if (!ctx || !name || !challenge) return SURGE_ERROR;

    try {
        auto mutex = std::make_unique<surge::lock::DistributedMutex>(
            *to_ctx(ctx), name);

        // The server-side release uses the challenge token
        bool released = mutex->try_release();
        if (!released) {
            return set_error(to_ctx(ctx), surge::ErrorCode::LockFailed,
                             "Failed to release lock");
        }

        return SURGE_OK;
    } catch (const std::exception& e) {
        return set_error(to_ctx(ctx), surge::ErrorCode::LockFailed, e.what());
    }
}

// ===== Supervisor =====

SURGE_API surge_result SURGE_CALL surge_supervisor_start(
    const char* exe_path,
    const char* working_dir,
    const char* supervisor_id,
    int argc,
    const char** argv) {
    if (!exe_path || !supervisor_id) return SURGE_ERROR;

    try {
        std::vector<std::string> args;
        if (argv) {
            for (int i = 0; i < argc; ++i) {
                if (argv[i]) args.emplace_back(argv[i]);
            }
        }

        std::filesystem::path install_dir = working_dir ? working_dir : ".";

        surge::supervisor::Supervisor sup(supervisor_id, install_dir);
        int32_t rc = sup.start(
            std::filesystem::path(exe_path),
            working_dir ? std::filesystem::path(working_dir) : std::filesystem::path("."),
            args);
        return static_cast<surge_result>(rc);
    } catch (const std::exception& e) {
        spdlog::error("surge_supervisor_start failed: {}", e.what());
        return SURGE_ERROR;
    }
}

// ===== Lifecycle events =====

SURGE_API surge_result SURGE_CALL surge_process_events(
    int argc,
    const char** argv,
    surge_event_callback on_first_run,
    surge_event_callback on_installed,
    surge_event_callback on_updated,
    void* user_data) {
    (void)argc;

    try {
        // Check for special command-line markers
        // --surge-first-run, --surge-installed, --surge-updated <version>
        std::string version;
        bool first_run = false;
        bool installed = false;
        bool updated = false;

        if (argv) {
            for (int i = 1; i < argc; ++i) {
                if (!argv[i]) continue;
                std::string arg = argv[i];
                if (arg == "--surge-first-run") {
                    first_run = true;
                } else if (arg == "--surge-installed") {
                    installed = true;
                } else if (arg == "--surge-updated" && i + 1 < argc) {
                    updated = true;
                    version = argv[++i];
                }
            }
        }

        if (first_run && on_first_run) {
            on_first_run(version.c_str(), user_data);
        }
        if (installed && on_installed) {
            on_installed(version.c_str(), user_data);
        }
        if (updated && on_updated) {
            on_updated(version.c_str(), user_data);
        }

        return SURGE_OK;
    } catch (...) {
        return SURGE_ERROR;
    }
}

// ===== Cancellation =====

SURGE_API surge_result SURGE_CALL surge_cancel(surge_context* ctx) {
    if (!ctx) return SURGE_ERROR;
    try {
        to_ctx(ctx)->cancel();
        spdlog::info("Context cancellation requested");
        return SURGE_OK;
    } catch (...) {
        return SURGE_ERROR;
    }
}

} // extern "C"
