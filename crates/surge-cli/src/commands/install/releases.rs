use std::path::Path;
use std::sync::{Arc, Mutex};

use indicatif::ProgressBar;

use crate::logline;
use surge_core::config::constants::RELEASES_FILE_COMPRESSED;
use surge_core::error::{Result, SurgeError};
use surge_core::releases::artifact_cache::{CacheFetchOutcome, fetch_or_reuse_file};
use surge_core::releases::manifest::{ReleaseEntry, ReleaseIndex, decompress_release_index};
use surge_core::releases::restore::{RestoreOptions, RestoreProgress, restore_full_archive_for_version_with_options};
use surge_core::releases::version::compare_versions;
use surge_core::storage::{StorageBackend, TransferProgress};

use super::selection::infer_os_from_rid;
use super::{make_progress_bar, make_spinner};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ArchiveAcquisition {
    ReusedLocal,
    Downloaded,
    Reconstructed,
}

pub(super) async fn fetch_release_index(backend: &dyn StorageBackend) -> Result<(ReleaseIndex, bool)> {
    match backend.get_object(RELEASES_FILE_COMPRESSED).await {
        Ok(data) => Ok((decompress_release_index(&data)?, true)),
        Err(SurgeError::NotFound(_)) => Ok((ReleaseIndex::default(), false)),
        Err(e) => Err(e),
    }
}

pub(super) async fn download_release_archive(
    backend: &dyn StorageBackend,
    index: &ReleaseIndex,
    release: &ReleaseEntry,
    rid_candidates: &[String],
    full_filename: &str,
    destination: &Path,
) -> Result<ArchiveAcquisition> {
    struct FetchProgressUi {
        verify_spinner: Option<ProgressBar>,
        transfer_bar: Option<ProgressBar>,
    }

    let expected_sha256 = release.full_sha256.trim();
    let ui_state = Arc::new(Mutex::new(FetchProgressUi {
        verify_spinner: if destination.is_file() && !expected_sha256.is_empty() {
            make_spinner("Verifying cached package integrity")
        } else {
            None
        },
        transfer_bar: None,
    }));
    let ui_state_for_progress = Arc::clone(&ui_state);
    let total_hint = u64::try_from(release.full_size.max(0)).unwrap_or(0);
    let transfer_progress: Box<TransferProgress> = Box::new(move |done: u64, total: u64| {
        let mut ui = ui_state_for_progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(spinner) = ui.verify_spinner.take() {
            spinner.finish_and_clear();
        }
        if ui.transfer_bar.is_none() {
            let initial_total = if total > 0 { total } else { total_hint };
            ui.transfer_bar = make_progress_bar("Fetching full package", initial_total);
        }
        if let Some(bar) = ui.transfer_bar.as_ref() {
            if total > 0 {
                bar.set_length(total);
            }
            bar.set_position(done);
        }
    });
    let fetch_result = fetch_or_reuse_file(
        backend,
        full_filename,
        destination,
        &release.full_sha256,
        Some(transfer_progress.as_ref()),
    )
    .await;
    let (verify_spinner, direct_fetch_bar) = {
        let mut ui = ui_state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        (ui.verify_spinner.take(), ui.transfer_bar.take())
    };
    if let Some(spinner) = verify_spinner {
        spinner.finish_and_clear();
    }
    if let Some(bar) = direct_fetch_bar {
        bar.finish_and_clear();
    }

    match fetch_result {
        Ok(CacheFetchOutcome::ReusedLocal) => Ok(ArchiveAcquisition::ReusedLocal),
        Ok(CacheFetchOutcome::DownloadedFresh | CacheFetchOutcome::DownloadedAfterInvalidLocal) => {
            Ok(ArchiveAcquisition::Downloaded)
        }
        Err(SurgeError::NotFound(_)) => {
            let restore_rid = if release.rid.trim().is_empty() {
                rid_candidates.first().map_or("", String::as_str)
            } else {
                release.rid.as_str()
            };
            let restore_bar = make_progress_bar("Rebuilding full package from release graph", 0);
            let restore_bar_for_progress = restore_bar.clone();
            let progress = |p: RestoreProgress| {
                if let Some(bar) = &restore_bar_for_progress {
                    if p.bytes_total > 0 {
                        bar.set_length(u64::try_from(p.bytes_total).unwrap_or(0));
                        bar.set_position(u64::try_from(p.bytes_done).unwrap_or(0));
                    } else if p.items_total > 0 {
                        bar.set_length(u64::try_from(p.items_total).unwrap_or(0));
                        bar.set_position(u64::try_from(p.items_done).unwrap_or(0));
                    }
                    bar.set_message(format!(
                        "Rebuilding full package from release graph ({}/{})",
                        p.items_done, p.items_total
                    ));
                } else {
                    logline::subtle(&format!(
                        "  Rebuilding full package from release graph [{}/{}] {} / {} bytes",
                        p.items_done, p.items_total, p.bytes_done, p.bytes_total
                    ));
                }
            };
            let rebuilt = restore_full_archive_for_version_with_options(
                backend,
                index,
                restore_rid,
                &release.version,
                RestoreOptions {
                    cache_dir: destination.parent(),
                    progress: Some(&progress),
                    ..RestoreOptions::default()
                },
            )
            .await?;
            if let Some(bar) = &restore_bar {
                bar.finish_and_clear();
            }
            std::fs::write(destination, rebuilt)?;
            Ok(ArchiveAcquisition::Reconstructed)
        }
        Err(e) => Err(e),
    }
}

pub(super) fn select_release<'a>(
    releases: &'a [ReleaseEntry],
    channel: &str,
    version: Option<&str>,
    rid_candidates: &[String],
    selected_os: Option<&str>,
) -> Option<&'a ReleaseEntry> {
    let mut eligible: Vec<&ReleaseEntry> = releases
        .iter()
        .filter(|release| release.channels.iter().any(|c| c == channel))
        .collect();

    if let Some(version) = version.map(str::trim).filter(|v| !v.is_empty()) {
        eligible.retain(|release| release.version == version);
    }

    if let Some(os) = selected_os.map(str::trim).filter(|value| !value.is_empty()) {
        let os = os.to_ascii_lowercase();
        eligible.retain(|release| release_os(release).is_some_and(|release_os| release_os == os));
    }

    if eligible.is_empty() {
        return None;
    }

    for rid in rid_candidates {
        let mut by_rid: Vec<&ReleaseEntry> = eligible.iter().copied().filter(|release| release.rid == *rid).collect();
        by_rid.sort_by(|a, b| compare_versions(&b.version, &a.version));
        if let Some(best) = by_rid.first() {
            return Some(*best);
        }
    }

    let mut generic: Vec<&ReleaseEntry> = eligible
        .iter()
        .copied()
        .filter(|release| release.rid.trim().is_empty())
        .collect();
    generic.sort_by(|a, b| compare_versions(&b.version, &a.version));
    generic.first().copied()
}

fn release_os(release: &ReleaseEntry) -> Option<String> {
    if let Some(os) = normalize_release_os(&release.os) {
        return Some(os.to_string());
    }
    infer_os_from_rid(&release.rid)
}

fn normalize_release_os(raw: &str) -> Option<&'static str> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "linux" => Some("linux"),
        "win" | "windows" => Some("windows"),
        "osx" | "macos" | "darwin" => Some("macos"),
        _ => None,
    }
}
