#![allow(clippy::too_many_lines)]

use std::path::Path;
use std::time::Instant;

use crate::formatters::{format_byte_progress, format_duration};
use crate::logline;
use crate::ui::UiTheme;
use surge_core::config::constants::RELEASES_FILE_COMPRESSED;
use surge_core::config::manifest::SurgeManifest;
use surge_core::error::{Result, SurgeError};
use surge_core::storage;

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
    let storage_config = super::build_app_scoped_storage_config(&manifest, &app_id)?;
    let backend = storage::create_storage_backend(&storage_config)?;

    if !packages_dir.is_dir() {
        return Err(SurgeError::Storage(format!(
            "Packages directory does not exist: {}",
            packages_dir.display()
        )));
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
    use surge_core::config::constants::RELEASES_FILE_COMPRESSED;
    use surge_core::platform::detect::current_rid;

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
}
