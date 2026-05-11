//! Phase-substep progress emission for the finalize phase.
//!
//! The finalize phase of an update runs several substeps (supervisor
//! shutdown, atomic directory swaps, persistent asset copy, cache pruning,
//! post-update hook, supervisor restart) that historically reported nothing
//! to the user once the bytes/items counter hit 100%. This module owns the
//! helper that emits `ProgressInfo` with a `phase_label` for each substep,
//! mirrors that label into the persisted in-progress
//! [`UpdateStatusRecord`], and provides a heartbeat helper for substeps
//! that can block silently for many seconds.
//!
//! Substep labels live in [`labels`] so the manager and tests reference the
//! same canonical strings.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tracing::warn;

use super::progress::{ProgressInfo, emit_progress};
use crate::update::status::{self, UpdateStatusRecord};

/// Substep labels for the finalize phase. These appear in `ProgressInfo`
/// events and on the persisted `current_phase` field of in-progress update
/// status records so operators can tell a stuck "swapping app directory"
/// apart from a stuck "starting supervisor".
pub(crate) mod labels {
    pub const STOPPING_SUPERVISOR: &str = "stopping supervisor";
    pub const PREPARING_SWAP: &str = "preparing app swap";
    pub const SWAPPING_APP_DIRECTORY: &str = "swapping app directory";
    pub const COPYING_PERSISTENT_ASSETS: &str = "copying persistent assets";
    pub const WRITING_RUNTIME_MANIFEST: &str = "writing runtime metadata";
    pub const INSTALLING_SHORTCUTS: &str = "installing shortcuts";
    pub const PRUNING_OLD_VERSIONS: &str = "pruning old versions and caches";
    pub const POST_UPDATE_HOOK: &str = "running post-update hook";
    pub const RESTARTING_SUPERVISOR: &str = "restarting supervisor";
}

/// Periodic re-emit interval for finalize substeps whose underlying work
/// can block silently for many seconds (supervisor shutdown, post-update
/// hook). Operators watching CLI output use this to distinguish "stuck"
/// from "still running".
pub(super) const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

/// Bundle of state used to emit phase progress and persist the current
/// substep label to the update status record.
pub(super) struct PhaseProgressEmitter<'a, F>
where
    F: Fn(ProgressInfo) + Send + Sync,
{
    pub(super) progress: Option<&'a Arc<F>>,
    pub(super) install_dir: &'a Path,
    pub(super) in_progress_template: &'a UpdateStatusRecord,
}

impl<F> PhaseProgressEmitter<'_, F>
where
    F: Fn(ProgressInfo) + Send + Sync,
{
    /// Run a future while periodically re-emitting the substep label so
    /// observers see "still working" beats during long-running phases that
    /// don't themselves report progress (supervisor shutdown, post-update
    /// hook, supervisor restart). The label is emitted once up front, then
    /// every `interval` until the future resolves.
    pub(super) async fn run_with_heartbeat<Fut, T>(
        &self,
        phase: i32,
        label: &'static str,
        total_percent: i32,
        interval: Duration,
        future: Fut,
    ) -> T
    where
        Fut: std::future::Future<Output = T>,
    {
        self.emit_substep(phase, label, total_percent);
        tokio::pin!(future);
        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await; // skip the immediate tick; we just emitted via emit_substep
        loop {
            tokio::select! {
                result = &mut future => return result,
                _ = ticker.tick() => {
                    emit_progress(
                        self.progress,
                        ProgressInfo {
                            phase,
                            phase_label: label,
                            total_percent,
                            ..ProgressInfo::default()
                        },
                    );
                }
            }
        }
    }

    /// Emit a progress event and best-effort persist the current substep
    /// label to the in-progress status record.
    pub(super) fn emit_substep(&self, phase: i32, label: &'static str, total_percent: i32) {
        emit_progress(
            self.progress,
            ProgressInfo {
                phase,
                phase_label: label,
                total_percent,
                ..ProgressInfo::default()
            },
        );
        let record = self.in_progress_template.clone().with_current_phase(label);
        if let Err(e) = status::write_update_status(self.install_dir, &record) {
            warn!(error = %e, phase = label, "Failed to persist in-progress substep status (continuing)");
        }
    }
}
