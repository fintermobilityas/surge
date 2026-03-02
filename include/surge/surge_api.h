/**
 * @file surge_api.h
 * @brief Public C API for the Surge update framework.
 *
 * This header defines the complete C interface for integrating Surge into
 * applications. All types use opaque pointers and plain C types for maximum
 * ABI compatibility across compilers and languages.
 */

#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* -------------------------------------------------------------------------- */
/*  Export / calling-convention macros                                         */
/* -------------------------------------------------------------------------- */

#ifdef _WIN32
  #ifdef SURGE_BUILDING
    #define SURGE_API __declspec(dllexport)
  #else
    #define SURGE_API __declspec(dllimport)
  #endif
  #define SURGE_CALL __cdecl
#else
  #define SURGE_API __attribute__((visibility("default")))
  #define SURGE_CALL
#endif

/* -------------------------------------------------------------------------- */
/*  Opaque handle types                                                       */
/* -------------------------------------------------------------------------- */

typedef struct surge_context           surge_context;
typedef struct surge_update_manager    surge_update_manager;
typedef struct surge_releases_info     surge_releases_info;
typedef struct surge_pack_context      surge_pack_context;

/* -------------------------------------------------------------------------- */
/*  Enumerations                                                              */
/* -------------------------------------------------------------------------- */

/** Result codes returned by most API functions. */
typedef enum surge_result {
    SURGE_OK        =  0,
    SURGE_ERROR     = -1,
    SURGE_CANCELLED = -2,
    SURGE_NOT_FOUND = -3
} surge_result;

/** Phases reported through progress callbacks. */
typedef enum surge_progress_phase {
    SURGE_PHASE_CHECK       = 0,
    SURGE_PHASE_DOWNLOAD    = 1,
    SURGE_PHASE_VERIFY      = 2,
    SURGE_PHASE_EXTRACT     = 3,
    SURGE_PHASE_APPLY_DELTA = 4,
    SURGE_PHASE_FINALIZE    = 5
} surge_progress_phase;

/** Cloud / local storage providers. */
typedef enum surge_storage_provider {
    SURGE_STORAGE_S3          = 0,
    SURGE_STORAGE_AZURE_BLOB  = 1,
    SURGE_STORAGE_GCS         = 2,
    SURGE_STORAGE_FILESYSTEM  = 3
} surge_storage_provider;

/* -------------------------------------------------------------------------- */
/*  Plain-data structures                                                     */
/* -------------------------------------------------------------------------- */

/** Error information returned by surge_context_last_error(). */
typedef struct surge_error {
    int32_t      code;
    const char*  message;
} surge_error;

/** Progress snapshot delivered to progress callbacks. */
typedef struct surge_progress {
    surge_progress_phase phase;
    int32_t              phase_percent;
    int32_t              total_percent;
    int64_t              bytes_done;
    int64_t              bytes_total;
    int64_t              items_done;
    int64_t              items_total;
    double               speed_bytes_per_sec;
} surge_progress;

/** Tunables for memory, CPU, and network usage. */
typedef struct surge_resource_budget {
    int64_t  max_memory_bytes;
    int32_t  max_threads;
    int32_t  max_concurrent_downloads;
    int64_t  max_download_speed_bps;
    int32_t  zstd_compression_level;
} surge_resource_budget;

/** Input / output context for bsdiff (binary diff creation). */
typedef struct surge_bsdiff_ctx {
    const uint8_t*  older;
    int64_t         older_size;
    const uint8_t*  newer;
    int64_t         newer_size;
    uint8_t*        patch;
    int64_t         patch_size;
    int32_t         status;
} surge_bsdiff_ctx;

/** Input / output context for bspatch (binary patch application). */
typedef struct surge_bspatch_ctx {
    const uint8_t*  older;
    int64_t         older_size;
    uint8_t*        newer;
    int64_t         newer_size;
    const uint8_t*  patch;
    int64_t         patch_size;
    int32_t         status;
} surge_bspatch_ctx;

/* -------------------------------------------------------------------------- */
/*  Callback typedefs                                                         */
/* -------------------------------------------------------------------------- */

/** Called repeatedly during long-running operations to report progress. */
typedef void (SURGE_CALL *surge_progress_callback)(
    const surge_progress* progress,
    void*                 user_data);

/** Called for lifecycle events (first-run, installed, updated). */
typedef void (SURGE_CALL *surge_event_callback)(
    const char* version,
    void*       user_data);

/* -------------------------------------------------------------------------- */
/*  Lifecycle                                                                 */
/* -------------------------------------------------------------------------- */

/**
 * Create a new Surge context. Must be destroyed with surge_context_destroy().
 * @return A new context handle, or NULL on allocation failure.
 */
SURGE_API surge_context* SURGE_CALL surge_context_create(void);

/**
 * Destroy a Surge context and release all associated resources.
 * @param ctx Context handle (may be NULL).
 */
SURGE_API void SURGE_CALL surge_context_destroy(surge_context* ctx);

/**
 * Retrieve the last error that occurred on @p ctx.
 * @return Pointer to an internal surge_error struct, or NULL if no error.
 *         The pointer is valid until the next API call on the same context.
 */
SURGE_API const surge_error* SURGE_CALL surge_context_last_error(
    const surge_context* ctx);

/* -------------------------------------------------------------------------- */
/*  Configuration                                                             */
/* -------------------------------------------------------------------------- */

/**
 * Configure the cloud or local storage backend.
 * @param ctx        Context handle.
 * @param provider   One of the surge_storage_provider values.
 * @param bucket     Bucket / container / root path.
 * @param region     Cloud region (may be NULL for filesystem).
 * @param access_key Access key or account name (may be NULL).
 * @param secret_key Secret key (may be NULL).
 * @param endpoint   Custom endpoint URL (may be NULL for default).
 * @return SURGE_OK on success.
 */
SURGE_API surge_result SURGE_CALL surge_config_set_storage(
    surge_context*        ctx,
    surge_storage_provider provider,
    const char*           bucket,
    const char*           region,
    const char*           access_key,
    const char*           secret_key,
    const char*           endpoint);

/**
 * Configure the distributed lock server URL.
 * @param ctx Context handle.
 * @param url Lock server URL (e.g. "https://lock.example.com").
 * @return SURGE_OK on success.
 */
SURGE_API surge_result SURGE_CALL surge_config_set_lock_server(
    surge_context* ctx,
    const char*    url);

/**
 * Set resource budget limits (memory, threads, bandwidth).
 * @param ctx    Context handle.
 * @param budget Pointer to a filled surge_resource_budget struct.
 * @return SURGE_OK on success.
 */
SURGE_API surge_result SURGE_CALL surge_config_set_resource_budget(
    surge_context*              ctx,
    const surge_resource_budget* budget);

/* -------------------------------------------------------------------------- */
/*  Update manager                                                            */
/* -------------------------------------------------------------------------- */

/**
 * Create an update manager bound to a specific application.
 * @param ctx             Context handle (must outlive the manager).
 * @param app_id          Application identifier.
 * @param current_version Current installed version string (semver).
 * @param channel         Release channel (e.g. "stable", "beta").
 * @param install_dir     Root installation directory.
 * @return A new update-manager handle, or NULL on failure.
 */
SURGE_API surge_update_manager* SURGE_CALL surge_update_manager_create(
    surge_context* ctx,
    const char*    app_id,
    const char*    current_version,
    const char*    channel,
    const char*    install_dir);

/**
 * Destroy an update manager.
 * @param mgr Manager handle (may be NULL).
 */
SURGE_API void SURGE_CALL surge_update_manager_destroy(
    surge_update_manager* mgr);

/**
 * Check for available updates.
 * @param mgr  Manager handle.
 * @param info [out] Receives a pointer to release information.
 *             Must be freed with surge_releases_destroy().
 * @return SURGE_OK if updates are available, SURGE_NOT_FOUND if up-to-date.
 */
SURGE_API surge_result SURGE_CALL surge_update_check(
    surge_update_manager*  mgr,
    surge_releases_info**  info);

/**
 * Download and apply an update described by @p info.
 * @param mgr         Manager handle.
 * @param info        Release information from surge_update_check().
 * @param progress_cb Optional progress callback (may be NULL).
 * @param user_data   Opaque pointer forwarded to @p progress_cb.
 * @return SURGE_OK on success.
 */
SURGE_API surge_result SURGE_CALL surge_update_download_and_apply(
    surge_update_manager*   mgr,
    const surge_releases_info* info,
    surge_progress_callback progress_cb,
    void*                   user_data);

/* -------------------------------------------------------------------------- */
/*  Release-info accessors                                                    */
/* -------------------------------------------------------------------------- */

/** Return the number of releases in @p info. */
SURGE_API int32_t SURGE_CALL surge_releases_count(
    const surge_releases_info* info);

/** Free a releases-info structure returned by surge_update_check(). */
SURGE_API void SURGE_CALL surge_releases_destroy(
    surge_releases_info* info);

/** Return the version string for release at @p index. */
SURGE_API const char* SURGE_CALL surge_release_version(
    const surge_releases_info* info,
    int32_t                    index);

/** Return the channel string for release at @p index. */
SURGE_API const char* SURGE_CALL surge_release_channel(
    const surge_releases_info* info,
    int32_t                    index);

/** Return the full-package size in bytes for release at @p index. */
SURGE_API int64_t SURGE_CALL surge_release_full_size(
    const surge_releases_info* info,
    int32_t                    index);

/** Return non-zero if release at @p index is a genesis (initial) release. */
SURGE_API int32_t SURGE_CALL surge_release_is_genesis(
    const surge_releases_info* info,
    int32_t                    index);

/* -------------------------------------------------------------------------- */
/*  Binary diff / patch (bsdiff / bspatch)                                    */
/* -------------------------------------------------------------------------- */

/**
 * Create a binary diff patch.
 * @param ctx Filled bsdiff context. On success, ctx->patch and ctx->patch_size
 *            are set. Free the patch buffer with surge_bsdiff_free().
 * @return SURGE_OK on success.
 */
SURGE_API surge_result SURGE_CALL surge_bsdiff(surge_bsdiff_ctx* ctx);

/**
 * Apply a binary diff patch.
 * @param ctx Filled bspatch context. On success, ctx->newer and ctx->newer_size
 *            are set. Free the output buffer with surge_bspatch_free().
 * @return SURGE_OK on success.
 */
SURGE_API surge_result SURGE_CALL surge_bspatch(surge_bspatch_ctx* ctx);

/** Free memory allocated by surge_bsdiff(). */
SURGE_API void SURGE_CALL surge_bsdiff_free(surge_bsdiff_ctx* ctx);

/** Free memory allocated by surge_bspatch(). */
SURGE_API void SURGE_CALL surge_bspatch_free(surge_bspatch_ctx* ctx);

/* -------------------------------------------------------------------------- */
/*  Pack builder                                                              */
/* -------------------------------------------------------------------------- */

/**
 * Create a new pack context for building release packages.
 * @param ctx           Surge context.
 * @param manifest_path Path to the surge.yml manifest file.
 * @param app_id        Application identifier.
 * @param rid           Runtime identifier (e.g. "linux-x64").
 * @param version       Version string for this release.
 * @param artifacts_dir Directory containing build artifacts.
 * @return A new pack context, or NULL on failure.
 */
SURGE_API surge_pack_context* SURGE_CALL surge_pack_create(
    surge_context* ctx,
    const char*    manifest_path,
    const char*    app_id,
    const char*    rid,
    const char*    version,
    const char*    artifacts_dir);

/**
 * Build release packages (full + delta).
 * @param pack_ctx    Pack context from surge_pack_create().
 * @param progress_cb Optional progress callback.
 * @param user_data   Opaque pointer forwarded to @p progress_cb.
 * @return SURGE_OK on success.
 */
SURGE_API surge_result SURGE_CALL surge_pack_build(
    surge_pack_context*     pack_ctx,
    surge_progress_callback progress_cb,
    void*                   user_data);

/**
 * Push built packages to the configured storage backend.
 * @param pack_ctx    Pack context from surge_pack_create().
 * @param channel     Target release channel.
 * @param progress_cb Optional progress callback.
 * @param user_data   Opaque pointer forwarded to @p progress_cb.
 * @return SURGE_OK on success.
 */
SURGE_API surge_result SURGE_CALL surge_pack_push(
    surge_pack_context*     pack_ctx,
    const char*             channel,
    surge_progress_callback progress_cb,
    void*                   user_data);

/** Destroy a pack context. */
SURGE_API void SURGE_CALL surge_pack_destroy(surge_pack_context* pack_ctx);

/* -------------------------------------------------------------------------- */
/*  Distributed lock                                                          */
/* -------------------------------------------------------------------------- */

/**
 * Acquire a named distributed lock.
 * @param ctx             Surge context (must have lock server configured).
 * @param name            Lock name.
 * @param timeout_seconds Maximum time to wait for the lock.
 * @param challenge_out   [out] Receives an opaque challenge string required
 *                        to release the lock. Caller must free with free().
 * @return SURGE_OK on success.
 */
SURGE_API surge_result SURGE_CALL surge_lock_acquire(
    surge_context* ctx,
    const char*    name,
    int32_t        timeout_seconds,
    char**         challenge_out);

/**
 * Release a previously acquired distributed lock.
 * @param ctx       Surge context.
 * @param name      Lock name (must match the acquire call).
 * @param challenge Challenge string from surge_lock_acquire().
 * @return SURGE_OK on success.
 */
SURGE_API surge_result SURGE_CALL surge_lock_release(
    surge_context* ctx,
    const char*    name,
    const char*    challenge);

/* -------------------------------------------------------------------------- */
/*  Supervisor                                                                */
/* -------------------------------------------------------------------------- */

/**
 * Start a supervised process.
 * @param exe_path      Path to the executable.
 * @param working_dir   Working directory for the child process.
 * @param supervisor_id Identifier for this supervisor instance.
 * @param argc          Argument count for the child process.
 * @param argv          Argument array for the child process.
 * @return SURGE_OK on success.
 */
SURGE_API surge_result SURGE_CALL surge_supervisor_start(
    const char*  exe_path,
    const char*  working_dir,
    const char*  supervisor_id,
    int          argc,
    const char** argv);

/* -------------------------------------------------------------------------- */
/*  Lifecycle events                                                          */
/* -------------------------------------------------------------------------- */

/**
 * Process application lifecycle events. Call early in main() to handle
 * first-run, post-install, and post-update hooks.
 * @param argc          main() argc.
 * @param argv          main() argv.
 * @param on_first_run  Callback for first run (may be NULL).
 * @param on_installed  Callback for fresh install (may be NULL).
 * @param on_updated    Callback for post-update (may be NULL).
 * @param user_data     Opaque pointer forwarded to all callbacks.
 * @return SURGE_OK on success.
 */
SURGE_API surge_result SURGE_CALL surge_process_events(
    int                  argc,
    const char**         argv,
    surge_event_callback on_first_run,
    surge_event_callback on_installed,
    surge_event_callback on_updated,
    void*                user_data);

/* -------------------------------------------------------------------------- */
/*  Cancellation                                                              */
/* -------------------------------------------------------------------------- */

/**
 * Request cancellation of any in-progress operation on @p ctx.
 * Thread-safe: may be called from any thread.
 * @param ctx Context handle.
 * @return SURGE_OK on success.
 */
SURGE_API surge_result SURGE_CALL surge_cancel(surge_context* ctx);

#ifdef __cplusplus
} /* extern "C" */
#endif
