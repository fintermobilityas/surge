use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::Instant;

use surge_core::config::manifest::SurgeManifest;
use surge_core::error::{Result, SurgeError};
use surge_core::storage::{self, StorageBackend};
use surge_core::storage_config::build_storage_config;

use super::progress::{upload_heartbeat_interval, upload_progress_message};

pub(super) fn build_installer_upload_backend(manifest: &SurgeManifest) -> Result<Box<dyn StorageBackend>> {
    let storage_config = build_storage_config(manifest)?;
    super::super::ensure_mutating_storage_access(&storage_config, "upload installers")?;
    storage::create_storage_backend(&storage_config)
}

pub(super) async fn upload_installers_to_storage(
    backend: &dyn StorageBackend,
    installer_paths: &[PathBuf],
) -> Result<()> {
    for installer_path in installer_paths {
        let filename = installer_path
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .ok_or_else(|| {
                SurgeError::Pack(format!(
                    "Invalid installer path (missing filename): {}",
                    installer_path.display()
                ))
            })?;
        let key = format!("installers/{filename}");
        upload_installer_with_feedback(backend, &key, installer_path).await?;
    }

    Ok(())
}

async fn upload_installer_with_feedback(backend: &dyn StorageBackend, key: &str, source_path: &Path) -> Result<u64> {
    let total_bytes = std::fs::metadata(source_path)?.len();
    crate::logline::subtle(&format!(
        "  Uploading installer {key} ({})",
        crate::formatters::format_bytes(total_bytes)
    ));

    let started = Instant::now();
    let upload_running = Arc::new(AtomicBool::new(true));
    let bytes_done = Arc::new(AtomicU64::new(0));

    let upload_running_for_heartbeat = Arc::clone(&upload_running);
    let bytes_done_for_heartbeat = Arc::clone(&bytes_done);
    let key_for_heartbeat = key.to_string();
    let heartbeat = thread::spawn(move || {
        while upload_running_for_heartbeat.load(Ordering::Relaxed) {
            thread::sleep(upload_heartbeat_interval());
            if !upload_running_for_heartbeat.load(Ordering::Relaxed) {
                break;
            }

            let uploaded = bytes_done_for_heartbeat.load(Ordering::Relaxed).min(total_bytes);
            let progress = upload_progress_message(uploaded, total_bytes, started);
            crate::logline::subtle(&format!("      {key_for_heartbeat}: {progress}"));
        }
    });

    let bytes_done_for_progress = Arc::clone(&bytes_done);
    let progress = move |done: u64, _total: u64| {
        bytes_done_for_progress.store(done.min(total_bytes), Ordering::Relaxed);
    };

    let upload_result = backend.upload_from_file(key, source_path, Some(&progress)).await;
    bytes_done.store(total_bytes, Ordering::Relaxed);
    upload_running.store(false, Ordering::Relaxed);
    let _ = heartbeat.join();
    upload_result?;

    crate::logline::subtle(&format!(
        "      {key}: {} in {}",
        crate::formatters::format_byte_progress(total_bytes, total_bytes, "uploaded"),
        crate::formatters::format_duration(started.elapsed())
    ));

    Ok(total_bytes)
}
