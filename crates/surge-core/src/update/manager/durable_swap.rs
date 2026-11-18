//! Flushes that make the app directory swap survive a host reset.
//!
//! The payload verification reads the payload back from the page cache, so it
//! passes whether or not the data has reached the disk. Finalize therefore
//! flushes the payload before the swap and the activated directory after it.

use std::path::Path;

use crate::error::{Result, SurgeError};
use crate::platform::durability::{sync_directory, sync_tree};

use super::progress::ProgressInfo;
use super::progress_substep::{HEARTBEAT_INTERVAL, PhaseProgressEmitter, labels};

/// Flush the verified payload while the previous version is still running:
/// once it is swapped in, a host reset must find the new files complete, not
/// 0 bytes long.
pub(super) async fn flush_payload<F>(progress_emitter: &PhaseProgressEmitter<'_, F>, payload_dir: &Path) -> Result<()>
where
    F: Fn(ProgressInfo) + Send + Sync,
{
    let payload_dir = payload_dir.to_path_buf();
    progress_emitter
        .run_with_heartbeat(
            6,
            labels::FLUSHING_PAYLOAD,
            90,
            HEARTBEAT_INTERVAL,
            flush_blocking(move || sync_tree(&payload_dir)),
        )
        .await
}

/// Flush the swap itself (the renames, the persistent assets and the runtime
/// manifest) before the new version is started, the previous one is removed
/// and the update is reported done.
pub(super) async fn flush_activated_app<F>(
    progress_emitter: &PhaseProgressEmitter<'_, F>,
    active_app_dir: &Path,
    install_dir: &Path,
) -> Result<()>
where
    F: Fn(ProgressInfo) + Send + Sync,
{
    progress_emitter.emit_substep(6, labels::FLUSHING_ACTIVATED_APP, 95);
    let active_app_dir = active_app_dir.to_path_buf();
    let install_dir = install_dir.to_path_buf();
    flush_blocking(move || {
        sync_tree(&active_app_dir)?;
        sync_directory(&install_dir)
    })
    .await
}

/// Run a blocking flush off the async executor so heartbeats keep firing while
/// a slow disk catches up.
async fn flush_blocking<F>(flush: F) -> Result<()>
where
    F: FnOnce() -> Result<()> + Send + 'static,
{
    tokio::task::spawn_blocking(flush)
        .await
        .map_err(|e| SurgeError::Update(format!("Flushing application files to disk was interrupted: {e}")))?
}
