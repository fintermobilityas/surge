#![deny(unsafe_op_in_unsafe_fn)]
#![allow(
    clippy::borrow_as_ptr,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::manual_let_else,
    clippy::similar_names,
    clippy::single_match_else
)]

//! C API (`cdylib`) for the Surge update framework.
//!
//! This crate produces `libsurge.so` / `surge.dll` / `libsurge.dylib` and
//! exports every function declared in `surge_api.h`.  All public symbols
//! use `#[no_mangle] pub unsafe extern "C"` and catch panics at the boundary.

mod context;
mod diff;
mod handles;
mod pack;
mod releases;
mod shared;
mod update;
mod utils;

use std::collections::BTreeMap;
use std::ffi::{c_char, c_int, c_void};
use std::path::Path;
use std::ptr;

use surge_core::lock::mutex::DistributedMutex;
use surge_core::supervisor::state::{
    supervisor_pid_file, supervisor_stop_file, write_restart_args, write_supervisor_exe_path,
};
use surge_core::update::status::{UpdateConvergenceState, mark_restart_handoff_converged, read_update_status};

pub use crate::context::{
    surge_config_set_lock_server, surge_config_set_resource_budget, surge_config_set_storage, surge_context_create,
    surge_context_destroy, surge_context_last_error,
};
pub use crate::diff::{surge_bsdiff, surge_bsdiff_free, surge_bspatch, surge_bspatch_free};
use crate::handles::SurgeContextHandle;
pub use crate::pack::{surge_pack_build, surge_pack_create, surge_pack_destroy, surge_pack_push};
pub use crate::releases::{
    surge_release_channel, surge_release_full_size, surge_release_is_genesis, surge_release_version,
    surge_releases_count, surge_releases_destroy,
};
use crate::shared::{
    SURGE_CANCELLED, SURGE_ERROR, SURGE_OK, SurgeEventCallback, catch_ffi, collect_argv, cstr_to_string, ffi_trace,
    libc_malloc, set_ctx_error,
};
pub use crate::update::{
    surge_update_check, surge_update_download_and_apply, surge_update_manager_create, surge_update_manager_destroy,
    surge_update_manager_set_artifact_retention_policy, surge_update_manager_set_channel,
    surge_update_manager_set_current_version, surge_update_manager_set_release_retention_limit,
    surge_update_status_read_json,
};
use crate::utils::to_lossy_cstring;

// ---------------------------------------------------------------------------
//  #[repr(C)] structs matching surge_api.h
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct SurgeProgressFfi {
    pub phase: i32,
    pub phase_percent: i32,
    pub total_percent: i32,
    pub bytes_done: i64,
    pub bytes_total: i64,
    pub items_done: i64,
    pub items_total: i64,
    pub speed_bytes_per_sec: f64,
}

#[repr(C)]
pub struct SurgeResourceBudgetFfi {
    pub max_memory_bytes: i64,
    pub max_threads: i32,
    pub max_concurrent_downloads: i32,
    pub max_download_speed_bps: i64,
    pub zstd_compression_level: i32,
}

#[repr(C)]
pub struct SurgeBsdiffCtxFfi {
    pub older: *const u8,
    pub older_size: i64,
    pub newer: *const u8,
    pub newer_size: i64,
    pub patch: *mut u8,
    pub patch_size: i64,
    pub status: i32,
}

#[repr(C)]
pub struct SurgeBspatchCtxFfi {
    pub older: *const u8,
    pub older_size: i64,
    pub newer: *mut u8,
    pub newer_size: i64,
    pub patch: *const u8,
    pub patch_size: i64,
    pub status: i32,
}

// =========================================================================
//  7. Distributed lock (2 functions)
// =========================================================================

/// Acquire a named distributed lock.
///
/// On success, `*challenge_out` receives a C string that must be passed to
/// `surge_lock_release`. The caller must free it with `free()`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_lock_acquire(
    ctx: *mut SurgeContextHandle,
    name: *const c_char,
    timeout_seconds: i32,
    challenge_out: *mut *mut c_char,
) -> i32 {
    if ctx.is_null() || name.is_null() || challenge_out.is_null() {
        return SURGE_ERROR;
    }

    catch_ffi(std::panic::AssertUnwindSafe(|| {
        // SAFETY: `ctx`/`challenge_out` are checked non-null above. The out
        // pointer is cleared immediately to avoid stale outputs on failure.
        let handle = unsafe {
            *challenge_out = ptr::null_mut();
            &*ctx
        };
        handle.clear_last_error();

        if handle.ctx.is_cancelled() {
            return SURGE_CANCELLED;
        }

        // SAFETY: `name` follows the nullable C string contract.
        let name_s = unsafe { cstr_to_string(name) };

        let mut mutex = DistributedMutex::new(handle.ctx.clone(), &name_s);
        let result = handle.runtime.block_on(mutex.try_acquire(timeout_seconds));

        match result {
            Ok(true) => {
                let token = mutex.challenge().unwrap_or("");
                let c_challenge = to_lossy_cstring(token);
                let len = c_challenge.as_bytes_with_nul().len();
                let buf = libc_malloc(len).cast::<c_char>();
                if buf.is_null() {
                    let e = surge_core::error::SurgeError::Other("malloc failed".into());
                    return set_ctx_error(handle, &e);
                }
                // SAFETY: `buf` points to `len` writable bytes allocated by
                // malloc above and `c_challenge` contains exactly `len`
                // initialized bytes including terminator.
                unsafe {
                    ptr::copy_nonoverlapping(c_challenge.as_ptr(), buf, len);
                    *challenge_out = buf;
                }
                SURGE_OK
            }
            Ok(false) => {
                let e = surge_core::error::SurgeError::Lock("Lock is held by another process".into());
                set_ctx_error(handle, &e)
            }
            Err(e) => set_ctx_error(handle, &e),
        }
    }))
}

/// Release a previously acquired distributed lock.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_lock_release(
    ctx: *mut SurgeContextHandle,
    name: *const c_char,
    challenge: *const c_char,
) -> i32 {
    if ctx.is_null() || name.is_null() || challenge.is_null() {
        return SURGE_ERROR;
    }

    catch_ffi(std::panic::AssertUnwindSafe(|| {
        // SAFETY: `ctx` is checked non-null above.
        let handle = unsafe { &*ctx };
        handle.clear_last_error();

        // SAFETY: `name` and `challenge` follow the nullable C string
        // contract.
        let (name_s, challenge_s) = unsafe { (cstr_to_string(name), cstr_to_string(challenge)) };

        let mut mutex = DistributedMutex::new(handle.ctx.clone(), &name_s);
        mutex.set_challenge(challenge_s);

        let result = handle.runtime.block_on(mutex.try_release());

        match result {
            Ok(()) => SURGE_OK,
            Err(e) => set_ctx_error(handle, &e),
        }
    }))
}

// =========================================================================
//  8. Supervisor (1 function)
// =========================================================================

/// Start a supervised process.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_supervisor_start(
    exe_path: *const c_char,
    install_dir: *const c_char,
    supervisor_id: *const c_char,
    argc: c_int,
    argv: *const *const c_char,
) -> i32 {
    ffi_trace("surge_supervisor_start: enter");
    catch_ffi(std::panic::AssertUnwindSafe(|| {
        if exe_path.is_null() || install_dir.is_null() || supervisor_id.is_null() {
            ffi_trace("surge_supervisor_start: null input");
            return SURGE_ERROR;
        }

        // SAFETY: pointer inputs satisfy this API's FFI contract.
        let (exe_s, install_dir_s, sup_id, args_owned) = unsafe {
            (
                cstr_to_string(exe_path),
                cstr_to_string(install_dir),
                cstr_to_string(supervisor_id),
                collect_argv(argc, argv),
            )
        };
        if exe_s.trim().is_empty() || install_dir_s.trim().is_empty() || sup_id.trim().is_empty() {
            ffi_trace("surge_supervisor_start: missing required input");
            return SURGE_ERROR;
        }

        let install_dir = Path::new(&install_dir_s);
        let exe = Path::new(&exe_s);
        let supervisor_path = exe
            .parent()
            .unwrap_or(install_dir)
            .join(surge_core::platform::process::supervisor_binary_name());
        if !supervisor_path.is_file() {
            tracing::error!(
                "supervisor_start failed: supervisor binary not found at {}",
                supervisor_path.display()
            );
            ffi_trace("surge_supervisor_start: supervisor binary missing");
            return SURGE_ERROR;
        }

        ffi_trace("surge_supervisor_start: writing restart args");
        if let Err(e) = write_restart_args(install_dir, &sup_id, &args_owned) {
            tracing::error!("supervisor_start failed: {e}");
            ffi_trace("surge_supervisor_start: restart args failed");
            return SURGE_ERROR;
        }

        ffi_trace("surge_supervisor_start: writing exe state");
        if let Err(e) = write_supervisor_exe_path(install_dir, &sup_id, exe) {
            tracing::error!("supervisor_start failed: {e}");
            ffi_trace("surge_supervisor_start: exe state failed");
            return SURGE_ERROR;
        }

        let pid = surge_core::platform::process::current_pid().to_string();
        let mut args: Vec<&str> = vec!["watch", "--id", &sup_id, "--dir", &install_dir_s, "--pid", &pid];
        if !args_owned.is_empty() {
            args.push("--");
            args.extend(args_owned.iter().map(String::as_str));
        }

        ffi_trace("surge_supervisor_start: spawning supervisor");
        match surge_core::platform::process::spawn_detached(
            &supervisor_path,
            &args,
            Some(install_dir),
            &BTreeMap::new(),
        ) {
            Ok(_) => {
                ffi_trace("surge_supervisor_start: spawned supervisor");
                mark_self_supervised_runtime_converged(install_dir, exe.parent().unwrap_or(install_dir));
                SURGE_OK
            }
            Err(e) => {
                tracing::error!("supervisor_start failed: {e}");
                ffi_trace("surge_supervisor_start: spawn failed");
                SURGE_ERROR
            }
        }
    }))
}

fn mark_self_supervised_runtime_converged(install_dir: &Path, active_app_dir: &Path) {
    let record = match read_update_status(install_dir) {
        Ok(Some(record)) => record,
        Ok(None) => return,
        Err(e) => {
            tracing::warn!(
                install_root = %install_dir.display(),
                reason = %e,
                "Failed reading update status before self-supervisor convergence proof"
            );
            return;
        }
    };
    if !matches!(
        record.state,
        UpdateConvergenceState::InProgress | UpdateConvergenceState::PendingRestart
    ) {
        return;
    }

    let runtime_version = match surge_core::install::read_runtime_manifest_version(active_app_dir) {
        Ok(Some(version)) => version,
        Ok(None) => return,
        Err(e) => {
            tracing::warn!(
                install_root = %install_dir.display(),
                app_dir = %active_app_dir.display(),
                reason = %e,
                "Failed reading runtime manifest before self-supervisor convergence proof"
            );
            return;
        }
    };
    if runtime_version.trim() != record.target_version.trim() {
        return;
    }

    match mark_restart_handoff_converged(install_dir, &record.target_version) {
        Ok(Some(_)) => {
            tracing::info!(
                version = record.target_version,
                "Restart handoff converged after self-supervisor accepted the target runtime"
            );
        }
        Ok(None) => {}
        Err(e) => {
            tracing::warn!(
                install_root = %install_dir.display(),
                version = record.target_version,
                reason = %e,
                "Failed to record self-supervisor convergence proof"
            );
        }
    }
}

/// Stop a supervised process watcher.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_supervisor_stop(install_dir: *const c_char, supervisor_id: *const c_char) -> i32 {
    ffi_trace("surge_supervisor_stop: enter");
    catch_ffi(std::panic::AssertUnwindSafe(|| {
        if install_dir.is_null() || supervisor_id.is_null() {
            ffi_trace("surge_supervisor_stop: null input");
            return SURGE_ERROR;
        }

        // SAFETY: pointer inputs satisfy this API's FFI contract.
        let (install_dir_s, sup_id) = unsafe { (cstr_to_string(install_dir), cstr_to_string(supervisor_id)) };
        if install_dir_s.trim().is_empty() || sup_id.trim().is_empty() {
            ffi_trace("surge_supervisor_stop: missing required input");
            return SURGE_ERROR;
        }

        let install_dir = Path::new(&install_dir_s);
        let pid_file = supervisor_pid_file(install_dir, &sup_id);
        if !pid_file.is_file() {
            ffi_trace("surge_supervisor_stop: pid file missing");
            return SURGE_ERROR;
        }

        ffi_trace("surge_supervisor_stop: writing stop file");
        let stop_file = supervisor_stop_file(install_dir, &sup_id);
        if let Err(e) = std::fs::write(&stop_file, b"surge-stop") {
            tracing::error!("supervisor_stop failed: {e}");
            ffi_trace("surge_supervisor_stop: stop file failed");
            return SURGE_ERROR;
        }

        ffi_trace("surge_supervisor_stop: waiting for supervisor exit");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while pid_file.exists() {
            if std::time::Instant::now() >= deadline {
                tracing::error!(
                    "supervisor_stop failed: timed out waiting for supervisor '{}' to exit",
                    sup_id
                );
                ffi_trace("surge_supervisor_stop: timed out");
                return SURGE_ERROR;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        let _ = std::fs::remove_file(&stop_file);
        ffi_trace("surge_supervisor_stop: success");
        SURGE_OK
    }))
}

// =========================================================================
//  9. Lifecycle events (1 function)
// =========================================================================

/// Process application lifecycle events.
///
/// Call early in `main()` to handle first-run, post-install, and post-update
/// hooks.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_process_events(
    argc: c_int,
    argv: *const *const c_char,
    on_first_run: SurgeEventCallback,
    on_installed: SurgeEventCallback,
    on_updated: SurgeEventCallback,
    user_data: *mut c_void,
) -> i32 {
    catch_ffi(std::panic::AssertUnwindSafe(|| {
        const ZERO_VERSION: &str = "0.0.0";

        // Collect argv for inspection.
        // SAFETY: `argv` follows this API's FFI contract.
        let args = unsafe { collect_argv(argc, argv) };

        let emit_event = |callback: SurgeEventCallback, version: &str| {
            if let Some(cb) = callback {
                let version = if version.trim().is_empty() {
                    ZERO_VERSION
                } else {
                    version.trim()
                };
                let version_c = to_lossy_cstring(version);
                cb(version_c.as_ptr(), user_data);
            }
        };

        // Parse lifecycle flags similarly to Snapx:
        // `--surge-installed <version>` and `--surge-first-run <version>`.
        let mut index = 0usize;
        while index < args.len() {
            let arg = args[index].as_str();

            let mut consumed_next = false;
            let next_version = args.get(index + 1).and_then(|candidate| {
                if candidate.starts_with("--") {
                    None
                } else {
                    Some(candidate.as_str())
                }
            });

            match arg {
                "--surge-first-run" => {
                    emit_event(on_first_run, next_version.unwrap_or(ZERO_VERSION));
                    consumed_next = next_version.is_some();
                }
                "--surge-installed" => {
                    emit_event(on_installed, next_version.unwrap_or(ZERO_VERSION));
                    consumed_next = next_version.is_some();
                }
                "--surge-updated" => {
                    emit_event(on_updated, next_version.unwrap_or(ZERO_VERSION));
                    consumed_next = next_version.is_some();
                }
                _ => {
                    if let Some(version) = arg.strip_prefix("--surge-updated=") {
                        emit_event(on_updated, version);
                    }
                }
            }

            index += 1;
            if consumed_next {
                index += 1;
            }
        }

        SURGE_OK
    }))
}

// =========================================================================
//  10. Cancellation (1 function)
// =========================================================================

/// Request cancellation of any in-progress operation on `ctx`.
///
/// Thread-safe: may be called from any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_cancel(ctx: *mut SurgeContextHandle) -> i32 {
    if ctx.is_null() {
        return SURGE_ERROR;
    }

    catch_ffi(std::panic::AssertUnwindSafe(|| {
        let handle = unsafe { &*ctx };
        handle.ctx.cancel();
        SURGE_OK
    }))
}

// =========================================================================
//  11. Allocator (1 function)
// =========================================================================

/// Free a NUL-terminated C string that was returned by a Surge FFI call which
/// documents its output as `free()`-owned (for example
/// `surge_update_status_read_json`). Safe to call with NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_free_cstring(ptr: *mut c_char) {
    // SAFETY: caller guarantees `ptr` was returned by a Surge FFI function
    // documented as `free()`-owned, or is null.
    unsafe { crate::shared::libc_free(ptr.cast::<c_void>()) };
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    struct TestInstallDir {
        path: PathBuf,
    }

    impl TestInstallDir {
        fn new(name: &str) -> Self {
            let unique = format!(
                "{name}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            );
            let path = std::env::temp_dir().join(unique);
            std::fs::create_dir(&path).unwrap();
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestInstallDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    fn write_runtime_manifest(app_dir: &Path, version: &str) {
        let runtime_manifest = app_dir.join(surge_core::install::RUNTIME_MANIFEST_RELATIVE_PATH);
        std::fs::create_dir_all(runtime_manifest.parent().unwrap()).unwrap();
        std::fs::write(
            &runtime_manifest,
            format!("id: demo-app\nversion: {version}\nchannel: test\n"),
        )
        .unwrap();
    }

    #[test]
    fn self_supervisor_converges_pending_status_when_runtime_manifest_matches() {
        let tmp = TestInstallDir::new("surge-ffi-self-supervisor-converges");
        let app_dir = tmp.path().join("app");
        write_runtime_manifest(&app_dir, "2.0.0");
        let pending = surge_core::update::status::UpdateStatusRecord::pending_restart_with_failure_phase(
            "demo-app",
            "2.0.0",
            "2.0.0",
            "test",
            "2026-05-21T10:00:00Z".to_string(),
            "2026-05-21T10:00:01Z".to_string(),
            "waiting for old child",
            surge_core::update::status::RESTART_HANDOFF_WAITING_FOR_OLD_CHILD_PHASE,
        );
        surge_core::update::status::write_update_status(tmp.path(), &pending).unwrap();

        super::mark_self_supervised_runtime_converged(tmp.path(), &app_dir);

        let status = surge_core::update::status::read_update_status(tmp.path())
            .unwrap()
            .expect("status should remain present");
        assert_eq!(
            status.state,
            surge_core::update::status::UpdateConvergenceState::Converged
        );
        assert!(status.supervisor_restart_confirmed);
        assert_eq!(status.failure_phase, None);
        assert_eq!(status.reason, None);
    }

    #[test]
    fn self_supervisor_converges_stale_in_progress_status_when_runtime_manifest_matches() {
        let tmp = TestInstallDir::new("surge-ffi-self-supervisor-converges-in-progress");
        let app_dir = tmp.path().join("app");
        write_runtime_manifest(&app_dir, "2.0.0");
        let in_progress = surge_core::update::status::UpdateStatusRecord::in_progress(
            "demo-app",
            "1.0.0",
            "2.0.0",
            "test",
            "2026-05-21T10:00:00Z".to_string(),
        )
        .with_current_phase_at("package apply started", "2026-05-21T10:00:01Z".to_string());
        surge_core::update::status::write_update_status(tmp.path(), &in_progress).unwrap();

        super::mark_self_supervised_runtime_converged(tmp.path(), &app_dir);

        let status = surge_core::update::status::read_update_status(tmp.path())
            .unwrap()
            .expect("status should remain present");
        assert_eq!(
            status.state,
            surge_core::update::status::UpdateConvergenceState::Converged
        );
        assert_eq!(status.installed_version, "2.0.0");
        assert_eq!(status.target_version, "2.0.0");
        assert!(status.supervisor_restart_confirmed);
        assert_eq!(status.current_phase, None);
        assert_eq!(status.failure_phase, None);
        assert_eq!(status.reason, None);
    }
}
