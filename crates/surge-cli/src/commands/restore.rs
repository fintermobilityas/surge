#![allow(clippy::too_many_lines)]

use std::path::Path;
use std::time::Instant;

use crate::formatters::{format_byte_progress, format_duration};
use crate::logline;
use crate::ui::UiTheme;
use surge_core::config::constants::RELEASES_FILE_COMPRESSED;
use surge_core::config::manifest::{AppConfig, SurgeManifest};
use surge_core::error::{Result, SurgeError};
use surge_core::releases::artifact_cache::{CacheFetchOutcome, fetch_or_reuse_file};
use surge_core::releases::manifest::{ReleaseEntry, decompress_release_index};
use surge_core::releases::restore::{RestoreOptions, restore_full_archive_for_version_with_options};
use surge_core::releases::version::compare_versions;
use surge_core::storage::{self, StorageBackend};

/// Restore releases from a local packages directory to storage.
pub async fn execute(
    manifest_path: &Path,
    app_id: Option<&str>,
    rid: Option<&str>,
    version: Option<&str>,
    packages_dir: &Path,
) -> Result<()> {
    const TOTAL_STAGES: usize = 5;

    let theme = UiTheme::global();
    let started = Instant::now();
    let requested_version = version.map(str::trim).filter(|value| !value.is_empty());

    print_stage(theme, 1, TOTAL_STAGES, "Resolving manifest and storage backend");
    let manifest = SurgeManifest::from_file(manifest_path)?;
    let app_id = super::resolve_app_id_with_rid_hint(&manifest, app_id, rid)?;
    let rid = super::resolve_rid(&manifest, &app_id, rid)?;
    let (app, _) = manifest
        .find_app_with_target(&app_id, &rid)
        .ok_or_else(|| SurgeError::Config(format!("No target {rid} found for app {app_id}")))?;
    let default_channel = default_channel_for_app(&manifest, app);
    let storage_config = super::build_app_scoped_storage_config(&manifest, &app_id)?;
    let backend = storage::create_storage_backend(&storage_config)?;

    if packages_dir.exists() && !packages_dir.is_dir() {
        return Err(SurgeError::Storage(format!(
            "Packages path is not a directory: {}",
            packages_dir.display()
        )));
    }

    if !packages_dir.exists() {
        return restore_release_from_storage(
            &*backend,
            packages_dir,
            &app_id,
            &rid,
            requested_version,
            &default_channel,
            theme,
            started,
        )
        .await;
    }

    print_stage_done(
        theme,
        1,
        TOTAL_STAGES,
        &format!(
            "Target: {app_id}/{rid}{}",
            requested_version.map_or(String::new(), |value| format!(" v{value}"))
        ),
    );

    print_stage(
        theme,
        2,
        TOTAL_STAGES,
        &format!("Scanning packages directory {}", packages_dir.display()),
    );
    let files = walkdir(packages_dir)?;
    let mut summary = RestoreSummary {
        scanned: files.len(),
        ..RestoreSummary::default()
    };
    print_stage_done(
        theme,
        2,
        TOTAL_STAGES,
        &format!("Discovered {} file(s)", summary.scanned),
    );

    print_stage(theme, 3, TOTAL_STAGES, "Filtering files for selected app/rid/version");
    let candidates = collect_restore_candidates(&files, packages_dir, &app_id, &rid, requested_version)?;
    summary.matched = candidates.len();
    summary.total_bytes = candidates.iter().map(|candidate| candidate.size_bytes).sum();
    let skipped = summary.skipped();
    if summary.matched == 0 {
        print_stage_done(
            theme,
            3,
            TOTAL_STAGES,
            &format!("No matching files found (skipped {skipped})"),
        );
    } else {
        print_stage_done(
            theme,
            3,
            TOTAL_STAGES,
            &format!("Matched {} file(s) (skipped {skipped})", summary.matched),
        );
    }

    print_stage(
        theme,
        4,
        TOTAL_STAGES,
        &format!("Uploading {} object(s)", summary.matched),
    );
    for (index, candidate) in candidates.iter().enumerate() {
        logline::subtle(&format!("  [{}/{}] {}", index + 1, summary.matched, candidate.key));
        backend
            .upload_from_file(&candidate.key, &candidate.source_path, None)
            .await?;
        summary.restored += 1;
        summary.uploaded_bytes = summary.uploaded_bytes.saturating_add(candidate.size_bytes);
        logline::subtle(&format!(
            "      {}",
            format_byte_progress(summary.uploaded_bytes, summary.total_bytes, "uploaded")
        ));
    }

    if summary.restored == 0 {
        print_stage_done(theme, 4, TOTAL_STAGES, "No objects uploaded");
    } else {
        print_stage_done(
            theme,
            4,
            TOTAL_STAGES,
            &format!("Uploaded {} object(s)", summary.restored),
        );
    }

    print_stage(theme, 5, TOTAL_STAGES, "Finalize restore summary");
    let duration = format_duration(started.elapsed());
    print_stage_done(
        theme,
        5,
        TOTAL_STAGES,
        &format!(
            "Completed in {duration} (scanned: {}, matched: {}, uploaded: {}, skipped: {}, {})",
            summary.scanned,
            summary.matched,
            summary.restored,
            summary.skipped(),
            format_byte_progress(summary.uploaded_bytes, summary.total_bytes, "uploaded")
        ),
    );

    Ok(())
}

#[derive(Debug, Default)]
struct RestoreSummary {
    scanned: usize,
    matched: usize,
    restored: usize,
    total_bytes: u64,
    uploaded_bytes: u64,
}

impl RestoreSummary {
    fn skipped(&self) -> usize {
        self.scanned.saturating_sub(self.matched)
    }
}

#[derive(Debug, Clone)]
struct RestoreCandidate {
    source_path: std::path::PathBuf,
    key: String,
    size_bytes: u64,
}

fn collect_restore_candidates(
    entries: &[std::path::PathBuf],
    packages_dir: &Path,
    app_id: &str,
    rid: &str,
    version: Option<&str>,
) -> Result<Vec<RestoreCandidate>> {
    let mut candidates = Vec::new();
    for entry in entries {
        let rel_path = entry
            .strip_prefix(packages_dir)
            .map_err(|e| SurgeError::Io(std::io::Error::other(e)))?;
        let key = normalize_key(rel_path);
        if !is_restore_match(&key, app_id, rid, version) {
            continue;
        }
        let size_bytes = std::fs::metadata(entry)?.len();
        candidates.push(RestoreCandidate {
            source_path: entry.clone(),
            key,
            size_bytes,
        });
    }
    Ok(candidates)
}

fn normalize_key(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn is_restore_match(key: &str, app_id: &str, rid: &str, version: Option<&str>) -> bool {
    if key == RELEASES_FILE_COMPRESSED {
        return true;
    }

    if !key.starts_with(&format!("{app_id}-")) || !key.contains(&format!("-{rid}-")) {
        return false;
    }

    version.is_none_or(|value| key.contains(&format!("-{value}-")))
}

fn print_stage(theme: UiTheme, stage: usize, total: usize, text: &str) {
    let _ = theme;
    logline::info(&format!("[{stage}/{total}] {text}"));
}

fn print_stage_done(theme: UiTheme, stage: usize, total: usize, text: &str) {
    let _ = theme;
    logline::success(&format!("[{stage}/{total}] {text}"));
}

fn file_size_label(path: &Path) -> String {
    match std::fs::metadata(path) {
        Ok(meta) => crate::formatters::format_bytes(meta.len()),
        Err(_) => "unknown size".to_string(),
    }
}

async fn restore_release_from_storage(
    backend: &dyn StorageBackend,
    packages_dir: &Path,
    app_id: &str,
    rid: &str,
    requested_version: Option<&str>,
    channel: &str,
    theme: UiTheme,
    started: Instant,
) -> Result<()> {
    const TOTAL_STAGES: usize = 5;

    print_stage_done(
        theme,
        1,
        TOTAL_STAGES,
        &format!(
            "Target: {app_id}/{rid}{} (restoring from storage, channel: {channel})",
            requested_version.map_or(String::new(), |value| format!(" v{value}"))
        ),
    );

    print_stage(theme, 2, TOTAL_STAGES, "Fetching release index from storage");
    let index_bytes = backend.get_object(RELEASES_FILE_COMPRESSED).await?;
    let index = decompress_release_index(&index_bytes)?;
    if !index.app_id.is_empty() && index.app_id != app_id {
        return Err(SurgeError::NotFound(format!(
            "Release index belongs to app '{}' not '{}'",
            index.app_id, app_id
        )));
    }
    let release = select_release_for_restore(&index.releases, channel, requested_version, rid).ok_or_else(|| {
        SurgeError::NotFound(format!(
            "No release found for app '{app_id}' rid '{rid}' on channel '{channel}'{}",
            requested_version.map_or_else(String::new, |value| format!(" and version '{value}'"))
        ))
    })?;
    print_stage_done(
        theme,
        2,
        TOTAL_STAGES,
        &format!("Selected release version {}", release.version),
    );

    print_stage(theme, 3, TOTAL_STAGES, "Preparing local package cache");
    std::fs::create_dir_all(packages_dir)?;
    std::fs::write(packages_dir.join(RELEASES_FILE_COMPRESSED), index_bytes)?;
    print_stage_done(
        theme,
        3,
        TOTAL_STAGES,
        &format!("Wrote {}", packages_dir.join(RELEASES_FILE_COMPRESSED).display()),
    );

    let full_key = release.full_filename.trim();
    if full_key.is_empty() {
        return Err(SurgeError::NotFound(format!(
            "Selected release {} for {app_id}/{rid} does not define a full package filename",
            release.version
        )));
    }
    let local_full_name = Path::new(full_key)
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .ok_or_else(|| SurgeError::Storage(format!("Invalid full package key: {full_key}")))?;
    let full_package_path = packages_dir.join(local_full_name);

    print_stage(theme, 4, TOTAL_STAGES, "Restoring full package");
    match fetch_or_reuse_file(backend, full_key, &full_package_path, &release.full_sha256, None).await {
        Ok(CacheFetchOutcome::ReusedLocal) => {
            print_stage_done(
                theme,
                4,
                TOTAL_STAGES,
                &format!(
                    "Using local package {} ({})",
                    full_package_path.display(),
                    file_size_label(&full_package_path)
                ),
            );
        }
        Ok(CacheFetchOutcome::DownloadedFresh | CacheFetchOutcome::DownloadedAfterInvalidLocal) => {
            print_stage_done(
                theme,
                4,
                TOTAL_STAGES,
                &format!(
                    "Downloaded {} ({})",
                    full_package_path.display(),
                    file_size_label(&full_package_path)
                ),
            );
        }
        Err(SurgeError::NotFound(_)) => {
            let rebuilt = restore_full_archive_for_version_with_options(
                backend,
                &index,
                rid,
                &release.version,
                RestoreOptions {
                    cache_dir: Some(packages_dir),
                    progress: None,
                },
            )
            .await?;
            std::fs::write(&full_package_path, rebuilt)?;
            print_stage_done(
                theme,
                4,
                TOTAL_STAGES,
                &format!(
                    "Rebuilt {} from release graph ({})",
                    full_package_path.display(),
                    file_size_label(&full_package_path)
                ),
            );
        }
        Err(e) => return Err(e),
    }

    print_stage(theme, 5, TOTAL_STAGES, "Finalize restore summary");
    print_stage_done(
        theme,
        5,
        TOTAL_STAGES,
        &format!("Completed in {}", format_duration(started.elapsed())),
    );
    Ok(())
}

fn select_release_for_restore(
    releases: &[ReleaseEntry],
    channel: &str,
    version: Option<&str>,
    rid: &str,
) -> Option<ReleaseEntry> {
    let mut eligible: Vec<&ReleaseEntry> = releases
        .iter()
        .filter(|release| release.channels.iter().any(|candidate| candidate == channel))
        .collect();

    if let Some(requested) = version.map(str::trim).filter(|value| !value.is_empty()) {
        eligible.retain(|release| release.version == requested);
    }

    if eligible.is_empty() {
        return None;
    }

    let mut by_rid: Vec<&ReleaseEntry> = eligible.iter().copied().filter(|release| release.rid == rid).collect();
    by_rid.sort_by(|a, b| compare_versions(&b.version, &a.version));
    if let Some(release) = by_rid.first() {
        return Some((*release).clone());
    }

    let mut generic: Vec<&ReleaseEntry> = eligible
        .iter()
        .copied()
        .filter(|release| release.rid.trim().is_empty())
        .collect();
    generic.sort_by(|a, b| compare_versions(&b.version, &a.version));
    generic.first().map(|release| (*release).clone())
}

fn default_channel_for_app(manifest: &SurgeManifest, app: &AppConfig) -> String {
    app.channels
        .first()
        .cloned()
        .or_else(|| manifest.channels.first().map(|channel| channel.name.clone()))
        .unwrap_or_else(|| "stable".to_string())
}

/// Recursively list all files in a directory.
fn walkdir(dir: &Path) -> Result<Vec<std::path::PathBuf>> {
    let mut files = Vec::new();
    walk_recursive(dir, &mut files)?;
    Ok(files)
}

fn walk_recursive(dir: &Path, files: &mut Vec<std::path::PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            walk_recursive(&path, files)?;
        } else {
            files.push(path);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use surge_core::config::constants::DEFAULT_ZSTD_LEVEL;
    use surge_core::config::constants::RELEASES_FILE_COMPRESSED;
    use surge_core::crypto::sha256::sha256_hex;
    use surge_core::diff::wrapper::bsdiff_buffers;
    use surge_core::platform::detect::current_rid;
    use surge_core::releases::manifest::{DeltaArtifact, ReleaseEntry, ReleaseIndex, compress_release_index};

    fn write_manifest(path: &Path, store_dir: &Path, app_id: &str, rid: &str) {
        let yaml = format!(
            r"schema: 1
storage:
  provider: filesystem
  bucket: {bucket}
apps:
  - id: {app_id}
    target:
      rid: {rid}
",
            bucket = store_dir.display()
        );
        std::fs::write(path, yaml).expect("manifest should be written");
    }

    fn make_release_entry(app_id: &str, rid: &str, version: &str) -> ReleaseEntry {
        ReleaseEntry {
            version: version.to_string(),
            channels: vec!["stable".to_string()],
            os: "linux".to_string(),
            rid: rid.to_string(),
            is_genesis: false,
            full_filename: format!("{app_id}-{version}-{rid}-full.tar.zst"),
            full_size: 0,
            full_sha256: String::new(),
            deltas: Vec::new(),
            preferred_delta_id: String::new(),
            created_utc: String::new(),
            release_notes: String::new(),
            name: String::new(),
            main_exe: "demo".to_string(),
            install_directory: "demo".to_string(),
            supervisor_id: String::new(),
            icon: String::new(),
            shortcuts: Vec::new(),
            persistent_assets: Vec::new(),
            installers: Vec::new(),
            environment: std::collections::BTreeMap::new(),
        }
    }

    #[test]
    fn restore_match_always_keeps_release_index() {
        assert!(is_restore_match(
            RELEASES_FILE_COMPRESSED,
            "demo",
            "linux-x64",
            Some("1.0.0")
        ));
    }

    #[test]
    fn restore_match_filters_app_rid_and_optional_version() {
        assert!(is_restore_match(
            "demo-1.0.0-linux-x64-full.tar.zst",
            "demo",
            "linux-x64",
            None
        ));
        assert!(!is_restore_match(
            "other-1.0.0-linux-x64-full.tar.zst",
            "demo",
            "linux-x64",
            None
        ));
        assert!(!is_restore_match(
            "demo-1.0.0-linux-arm64-full.tar.zst",
            "demo",
            "linux-x64",
            None
        ));
        assert!(is_restore_match(
            "demo-1.0.0-linux-x64-full.tar.zst",
            "demo",
            "linux-x64",
            Some("1.0.0")
        ));
        assert!(!is_restore_match(
            "demo-1.1.0-linux-x64-full.tar.zst",
            "demo",
            "linux-x64",
            Some("1.0.0")
        ));
    }

    #[tokio::test]
    async fn execute_restores_only_matching_files_and_release_index() {
        let temp_dir = tempfile::tempdir().expect("temp dir should be created");
        let store_dir = temp_dir.path().join("store");
        let packages_dir = temp_dir.path().join("packages");
        let manifest_path = temp_dir.path().join("surge.yml");
        let app_id = "demo";
        let version = "1.0.0";
        let rid = current_rid();

        std::fs::create_dir_all(&store_dir).expect("store dir should be created");
        std::fs::create_dir_all(&packages_dir).expect("packages dir should be created");
        write_manifest(&manifest_path, &store_dir, app_id, &rid);

        let matching_full = format!("{app_id}-{version}-{rid}-full.tar.zst");
        let matching_delta = format!("{app_id}-{version}-{rid}-delta.tar.zst");
        let wrong_version = format!("{app_id}-2.0.0-{rid}-full.tar.zst");
        let wrong_app = format!("other-{version}-{rid}-full.tar.zst");

        std::fs::write(packages_dir.join(RELEASES_FILE_COMPRESSED), b"index").expect("release index should be written");
        std::fs::write(packages_dir.join(&matching_full), b"full").expect("matching full should be written");
        std::fs::write(packages_dir.join(&matching_delta), b"delta").expect("matching delta should be written");
        std::fs::write(packages_dir.join(&wrong_version), b"wrong version").expect("wrong version should be written");
        std::fs::write(packages_dir.join(&wrong_app), b"wrong app").expect("wrong app should be written");

        execute(&manifest_path, Some(app_id), Some(&rid), Some(version), &packages_dir)
            .await
            .expect("restore should succeed");

        assert!(store_dir.join(RELEASES_FILE_COMPRESSED).is_file());
        assert!(store_dir.join(&matching_full).is_file());
        assert!(store_dir.join(&matching_delta).is_file());
        assert!(!store_dir.join(&wrong_version).is_file());
        assert!(!store_dir.join(&wrong_app).is_file());
    }

    #[tokio::test]
    async fn execute_restores_requested_release_from_storage_when_packages_dir_is_missing() {
        let temp_dir = tempfile::tempdir().expect("temp dir should be created");
        let store_dir = temp_dir.path().join("store");
        let packages_dir = temp_dir.path().join("packages");
        let manifest_path = temp_dir.path().join("surge.yml");
        let app_id = "demo";
        let rid = current_rid();
        let v1 = "1.0.0";
        let v2 = "1.1.0";

        std::fs::create_dir_all(&store_dir).expect("store dir should be created");
        write_manifest(&manifest_path, &store_dir, app_id, &rid);

        let full_v1 = b"full-v1".to_vec();
        let full_v2 = b"full-v2".to_vec();
        let patch_v2 = bsdiff_buffers(&full_v1, &full_v2).expect("delta patch should build");
        let delta_v2 = zstd::encode_all(patch_v2.as_slice(), 3).expect("delta should compress");

        let mut release_v1 = make_release_entry(app_id, &rid, v1);
        release_v1.set_primary_delta(None);
        release_v1.full_size = full_v1.len() as i64;
        release_v1.full_sha256 = sha256_hex(&full_v1);

        let mut release_v2 = make_release_entry(app_id, &rid, v2);
        release_v2.full_size = full_v2.len() as i64;
        release_v2.full_sha256 = sha256_hex(&full_v2);
        release_v2.set_primary_delta(Some(DeltaArtifact::bsdiff_zstd(
            "primary",
            v1,
            &format!("{app_id}-{v2}-{rid}-delta.tar.zst"),
            delta_v2.len() as i64,
            &sha256_hex(&delta_v2),
        )));

        let index = ReleaseIndex {
            app_id: app_id.to_string(),
            releases: vec![release_v1, release_v2],
            ..ReleaseIndex::default()
        };
        let index_bytes = compress_release_index(&index, DEFAULT_ZSTD_LEVEL).expect("index should compress");
        std::fs::write(store_dir.join(RELEASES_FILE_COMPRESSED), &index_bytes)
            .expect("release index should be written");
        std::fs::write(store_dir.join(format!("{app_id}-{v1}-{rid}-full.tar.zst")), &full_v1)
            .expect("base full should be written");
        std::fs::write(store_dir.join(format!("{app_id}-{v2}-{rid}-delta.tar.zst")), &delta_v2)
            .expect("delta should be written");

        execute(&manifest_path, Some(app_id), Some(&rid), Some(v2), &packages_dir)
            .await
            .expect("restore should succeed");

        assert!(packages_dir.is_dir(), "restore should create the package cache");
        assert_eq!(
            std::fs::read(packages_dir.join(RELEASES_FILE_COMPRESSED)).expect("release index should be restored"),
            index_bytes
        );
        assert_eq!(
            std::fs::read(packages_dir.join(format!("{app_id}-{v2}-{rid}-full.tar.zst")))
                .expect("rebuilt full package should be written"),
            full_v2
        );
    }
}
