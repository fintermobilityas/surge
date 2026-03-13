use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::logline;
use surge_core::config::constants::RELEASES_FILE_COMPRESSED;
use surge_core::config::installer::InstallerManifest;
use surge_core::error::{Result, SurgeError};
use surge_core::install::{self as core_install, InstallProfile};
use surge_core::platform::paths::default_install_root;
use surge_core::releases::artifact_cache::{cache_path_for_key, cached_artifact_matches, prune_cached_artifacts};
use surge_core::releases::manifest::{ReleaseEntry, ReleaseIndex, decompress_release_index};
use surge_core::releases::restore::{
    RestoreOptions, RestoreProgress, required_artifacts_for_index, restore_full_archive_for_version_with_options,
};
use surge_core::storage::{self, StorageBackend};

const PACKAGE_PROGRESS_LOG_INTERVAL: Duration = Duration::from_secs(10);
const PACKAGE_PROGRESS_PERCENT_STEP: u64 = 10;

#[derive(Debug)]
struct ProgressReporter {
    label: &'static str,
    last_logged_at: Instant,
    last_value: u64,
    next_percent: u64,
}

impl ProgressReporter {
    fn new(label: &'static str) -> Self {
        Self {
            label,
            last_logged_at: Instant::now(),
            last_value: 0,
            next_percent: PACKAGE_PROGRESS_PERCENT_STEP,
        }
    }

    fn observe_bytes(&mut self, done: u64, total: u64) -> Option<String> {
        self.observe_bytes_at(Instant::now(), done, total)
    }

    fn observe_bytes_at(&mut self, now: Instant, done: u64, total: u64) -> Option<String> {
        let total = total.max(done);
        let percent = percent(done, total);
        let crossed_percent = total > 0 && percent >= self.next_percent;
        let timed_out =
            now.duration_since(self.last_logged_at) >= PACKAGE_PROGRESS_LOG_INTERVAL && done > self.last_value;
        let finished = total > 0 && done >= total && done > self.last_value;

        if !crossed_percent && !timed_out && !finished {
            return None;
        }

        self.last_logged_at = now;
        self.last_value = done;
        while self.next_percent <= percent && self.next_percent <= 100 {
            self.next_percent = self.next_percent.saturating_add(PACKAGE_PROGRESS_PERCENT_STEP);
        }

        Some(format!(
            "{} {} / {} ({}%)",
            self.label,
            crate::formatters::format_bytes(done),
            crate::formatters::format_bytes(total),
            percent
        ))
    }

    fn observe_restore(&mut self, progress: RestoreProgress) -> Option<String> {
        let bytes_total = u64::try_from(progress.bytes_total.max(0)).unwrap_or(0);
        let bytes_done = u64::try_from(progress.bytes_done.max(0)).unwrap_or(0);
        if bytes_total > 0 {
            return self.observe_bytes(bytes_done, bytes_total);
        }

        let items_total = u64::try_from(progress.items_total.max(0)).unwrap_or(0);
        let items_done = u64::try_from(progress.items_done.max(0)).unwrap_or(0);
        let percent = percent(items_done, items_total);
        let now = Instant::now();
        let crossed_percent = items_total > 0 && percent >= self.next_percent;
        let timed_out =
            now.duration_since(self.last_logged_at) >= PACKAGE_PROGRESS_LOG_INTERVAL && items_done > self.last_value;
        let finished = items_total > 0 && items_done >= items_total && items_done > self.last_value;

        if !crossed_percent && !timed_out && !finished {
            return None;
        }

        self.last_logged_at = now;
        self.last_value = items_done;
        while self.next_percent <= percent && self.next_percent <= 100 {
            self.next_percent = self.next_percent.saturating_add(PACKAGE_PROGRESS_PERCENT_STEP);
        }

        Some(format!("{} step {items_done}/{items_total} ({}%)", self.label, percent))
    }
}

fn percent(done: u64, total: u64) -> u64 {
    if total == 0 {
        return 0;
    }

    done.saturating_mul(100).saturating_div(total).min(100)
}

/// Execute setup from an extracted installer directory.
///
/// This is called either directly via `surge setup [dir]` or auto-detected when
/// warp extracts the bundle and runs `surge` with no arguments.
pub async fn execute(dir: &Path, no_start: bool) -> Result<()> {
    let manifest_path = dir.join("installer.yml");
    if !manifest_path.is_file() {
        return Err(SurgeError::Config(format!(
            "No installer.yml found in '{}'",
            dir.display()
        )));
    }

    let manifest_bytes = std::fs::read(&manifest_path)?;
    let manifest: InstallerManifest = serde_yaml::from_slice(&manifest_bytes)?;

    logline::info(&format!(
        "Setting up {} v{} ({}/{})",
        manifest.runtime.name, manifest.version, manifest.app_id, manifest.rid
    ));

    let install_root = default_install_root(&manifest.app_id, &manifest.runtime.install_directory)?;

    if let Err(e) = super::stop_supervisor(&install_root, &manifest.runtime.supervisor_id).await {
        logline::warn(&format!("Could not stop supervisor: {e}"));
    }
    stop_running_app(&install_root, &manifest.runtime.main_exe);

    let package = resolve_package(dir, &manifest, &install_root).await?;

    let profile = InstallProfile::from_installer_manifest(&manifest, &manifest.runtime.shortcuts);

    core_install::install_package_locally_at_root(&profile, package.path(), &install_root)?;
    let active_app_dir = install_root.join("app");
    let runtime_manifest = core_install::RuntimeManifestMetadata::new(
        &manifest.version,
        &manifest.channel,
        &manifest.storage.provider,
        &manifest.storage.bucket,
        &manifest.storage.region,
        &manifest.storage.endpoint,
    );
    core_install::write_runtime_manifest(&active_app_dir, &profile, &runtime_manifest)?;

    if let Some(required_artifacts) = package.required_artifacts.as_ref() {
        match prune_install_artifact_cache(&install_root, required_artifacts, &manifest.release.full_filename) {
            Ok(0) => {}
            Ok(pruned) => {
                logline::info(&format!(
                    "Pruned {pruned} stale artifact cache entr{}.",
                    if pruned == 1 { "y" } else { "ies" }
                ));
            }
            Err(e) => {
                logline::warn(&format!("Artifact cache pruning failed: {e}"));
            }
        }
    }

    logline::success(&format!(
        "Installed '{}' to '{}'",
        manifest.app_id,
        install_root.display()
    ));

    if !no_start {
        match core_install::auto_start_after_install_sequence(
            &profile,
            &install_root,
            &active_app_dir,
            &manifest.version,
        ) {
            Ok(pid) => {
                logline::success(&format!("Started '{}' (pid {pid})", manifest.runtime.name));
            }
            Err(e) => {
                logline::warn(&format!("Auto-start failed: {e}"));
            }
        }
    }

    Ok(())
}

/// Resolve the full package: prefer bundled payload, then the persistent
/// artifact cache, then release-graph reconstruction/download into that cache.
async fn resolve_package(dir: &Path, manifest: &InstallerManifest, install_root: &Path) -> Result<ResolvedPackage> {
    let full_filename = manifest.release.full_filename.trim();
    if full_filename.is_empty() {
        return Err(SurgeError::Config(
            "Installer manifest has no full_filename in release section".to_string(),
        ));
    }

    let payload_path = dir.join("payload").join(full_filename);
    if payload_path.is_file() {
        logline::info(&format!("Using bundled payload: {}", payload_path.display()));
        return Ok(ResolvedPackage {
            path: payload_path,
            required_artifacts: None,
        });
    }

    let artifact_cache_dir = install_artifact_cache_dir(install_root);
    std::fs::create_dir_all(&artifact_cache_dir)?;
    let cached_package_path = cache_path_for_key(&artifact_cache_dir, full_filename)?;
    let storage_config = build_storage_config_from_manifest(dir, manifest)?;
    let backend = storage::create_storage_backend(&storage_config)?;
    let index = match fetch_release_index(&*backend, manifest).await {
        Ok(index) => index,
        Err(error) if cached_package_path.is_file() => {
            logline::warn(&format!(
                "Could not fetch release index; using cached package '{}' without verification: {error}",
                cached_package_path.display()
            ));
            return Ok(ResolvedPackage {
                path: cached_package_path,
                required_artifacts: None,
            });
        }
        Err(error) => return Err(error),
    };
    let required_artifacts = index.as_ref().map(required_artifacts_for_index);

    if let Some(index) = index.as_ref()
        && let Some(release) = find_release_for_installer(index, manifest)
    {
        if cached_artifact_matches(&cached_package_path, &release.full_sha256)? {
            logline::info(&format!(
                "Using cached package from artifact cache: {}",
                cached_package_path.display()
            ));
            return Ok(ResolvedPackage {
                path: cached_package_path,
                required_artifacts,
            });
        }

        logline::info(&format!("Preparing package '{full_filename}' in artifact cache"));
        let progress = Mutex::new(ProgressReporter::new("Preparing package..."));
        let restored = restore_full_archive_for_version_with_options(
            &*backend,
            index,
            &manifest.rid,
            &manifest.version,
            RestoreOptions {
                cache_dir: Some(&artifact_cache_dir),
                progress: Some(&|update| {
                    let mut reporter = progress.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                    if let Some(message) = reporter.observe_restore(update) {
                        logline::subtle(&message);
                    }
                }),
            },
        )
        .await?;
        std::fs::write(&cached_package_path, restored)?;
        logline::success(&format!(
            "Prepared '{}' in artifact cache ({})",
            full_filename,
            file_size_label(&cached_package_path)
        ));
        return Ok(ResolvedPackage {
            path: cached_package_path,
            required_artifacts,
        });
    }

    if cached_package_path.is_file() {
        logline::warn(&format!(
            "Release metadata for '{}' was not found; using cached package '{}'.",
            full_filename,
            cached_package_path.display()
        ));
        return Ok(ResolvedPackage {
            path: cached_package_path,
            required_artifacts,
        });
    }

    logline::info(&format!("Downloading package '{full_filename}' into artifact cache"));
    let progress = Mutex::new(ProgressReporter::new("Downloading package..."));

    backend
        .download_to_file(
            full_filename,
            &cached_package_path,
            Some(&|done, total| {
                let mut reporter = progress.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(message) = reporter.observe_bytes(done, total) {
                    logline::subtle(&message);
                }
            }),
        )
        .await?;

    logline::success(&format!(
        "Downloaded '{}' to artifact cache ({})",
        full_filename,
        file_size_label(&cached_package_path)
    ));

    Ok(ResolvedPackage {
        path: cached_package_path,
        required_artifacts,
    })
}

fn build_storage_config_from_manifest(
    dir: &Path,
    manifest: &InstallerManifest,
) -> Result<surge_core::context::StorageConfig> {
    super::build_storage_config_from_installer_manifest(manifest, dir)
}

fn install_artifact_cache_dir(install_root: &Path) -> PathBuf {
    install_root.join(".surge-cache").join("artifacts")
}

async fn fetch_release_index(
    backend: &dyn StorageBackend,
    manifest: &InstallerManifest,
) -> Result<Option<ReleaseIndex>> {
    let key = manifest.release_index_key.trim();
    let key = if key.is_empty() { RELEASES_FILE_COMPRESSED } else { key };
    match backend.get_object(key).await {
        Ok(bytes) => {
            let index = decompress_release_index(&bytes)?;
            if !index.app_id.is_empty() && index.app_id != manifest.app_id {
                return Err(SurgeError::NotFound(format!(
                    "Release index belongs to app '{}' not '{}'",
                    index.app_id, manifest.app_id
                )));
            }
            Ok(Some(index))
        }
        Err(SurgeError::NotFound(_)) => Ok(None),
        Err(e) => Err(e),
    }
}

fn find_release_for_installer<'a>(index: &'a ReleaseIndex, manifest: &InstallerManifest) -> Option<&'a ReleaseEntry> {
    index.releases.iter().find(|release| {
        release.version == manifest.version
            && release.full_filename.trim() == manifest.release.full_filename.trim()
            && (release.rid.is_empty() || manifest.rid.is_empty() || release.rid == manifest.rid)
    })
}

fn prune_install_artifact_cache(
    install_root: &Path,
    required_artifacts: &BTreeSet<String>,
    warm_full_filename: &str,
) -> Result<usize> {
    let mut retained_artifacts = required_artifacts.clone();
    let warm_full_filename = warm_full_filename.trim();
    if !warm_full_filename.is_empty() {
        retained_artifacts.insert(warm_full_filename.to_string());
    }
    prune_cached_artifacts(&install_artifact_cache_dir(install_root), &retained_artifacts)
}

/// Kill any running process whose executable lives in the app directory.
/// This catches orphaned app processes that outlived their supervisor.
fn stop_running_app(install_root: &Path, main_exe: &str) {
    let main_exe = main_exe.trim();
    if main_exe.is_empty() {
        return;
    }

    let exe_path = install_root.join("app").join(main_exe);
    let exe_name = exe_path.to_string_lossy();

    #[cfg(unix)]
    {
        let status = std::process::Command::new("pkill").args(["-f", &*exe_name]).status();
        if matches!(status, Ok(s) if s.success()) {
            logline::info(&format!("Stopped running app process '{main_exe}'."));
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
    }

    #[cfg(windows)]
    {
        let _ = std::process::Command::new("taskkill")
            .args(["/F", "/FI", &format!("IMAGENAME eq {main_exe}")])
            .status();
    }

    let _ = &exe_name;
}

fn file_size_label(path: &Path) -> String {
    match std::fs::metadata(path) {
        Ok(meta) => crate::formatters::format_bytes(meta.len()),
        Err(_) => "unknown size".to_string(),
    }
}

struct ResolvedPackage {
    path: PathBuf,
    required_artifacts: Option<BTreeSet<String>>,
}

impl ResolvedPackage {
    fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use surge_core::archive::packer::ArchivePacker;
    use surge_core::config::constants::RELEASES_FILE_COMPRESSED;
    use surge_core::config::installer::{InstallerRelease, InstallerRuntime, InstallerStorage, InstallerUi};
    use surge_core::crypto::sha256::sha256_hex;
    use surge_core::diff::wrapper::bsdiff_buffers;
    use surge_core::platform::detect::current_rid;
    use surge_core::releases::manifest::{DeltaArtifact, ReleaseEntry, ReleaseIndex, compress_release_index};

    fn make_manifest(
        install_root: &Path,
        store_root: &Path,
        full_filename: &str,
        installer_type: &str,
    ) -> InstallerManifest {
        InstallerManifest {
            schema: 1,
            format: "surge-installer-v1".to_string(),
            ui: InstallerUi::Console,
            installer_type: installer_type.to_string(),
            app_id: "demo-app".to_string(),
            rid: current_rid(),
            version: "1.2.3".to_string(),
            channel: "stable".to_string(),
            generated_utc: chrono::Utc::now().to_rfc3339(),
            headless_default_if_no_display: true,
            release_index_key: RELEASES_FILE_COMPRESSED.to_string(),
            storage: InstallerStorage {
                provider: "filesystem".to_string(),
                bucket: store_root.to_string_lossy().to_string(),
                region: String::new(),
                endpoint: String::new(),
                prefix: String::new(),
            },
            release: InstallerRelease {
                full_filename: full_filename.to_string(),
                delta_filename: String::new(),
                delta_algorithm: String::new(),
                delta_patch_format: String::new(),
                delta_compression: String::new(),
            },
            runtime: InstallerRuntime {
                name: "Demo App".to_string(),
                main_exe: "demoapp".to_string(),
                install_directory: install_root.to_string_lossy().to_string(),
                supervisor_id: String::new(),
                icon: String::new(),
                shortcuts: Vec::new(),
                persistent_assets: Vec::new(),
                installers: Vec::new(),
                environment: BTreeMap::new(),
            },
        }
    }

    fn write_archive(path: &Path, payload: &[u8]) {
        let mut packer = ArchivePacker::new(3).expect("archive packer");
        packer
            .add_buffer("demoapp", b"#!/bin/sh\necho demo\n", 0o755)
            .expect("demoapp entry");
        packer.add_buffer("payload.txt", payload, 0o644).expect("payload entry");
        packer.finalize_to_file(path).expect("archive file");
    }

    fn write_release_index(store_root: &Path, manifest: &InstallerManifest, archive_path: &Path) {
        let archive = std::fs::read(archive_path).expect("archive bytes");
        let release = ReleaseEntry {
            version: manifest.version.clone(),
            channels: vec![manifest.channel.clone()],
            os: "linux".to_string(),
            rid: manifest.rid.clone(),
            is_genesis: false,
            full_filename: manifest.release.full_filename.clone(),
            full_size: i64::try_from(archive.len()).expect("archive len fits i64"),
            full_sha256: sha256_hex(&archive),
            deltas: Vec::new(),
            preferred_delta_id: String::new(),
            created_utc: manifest.generated_utc.clone(),
            release_notes: String::new(),
            name: manifest.runtime.name.clone(),
            main_exe: manifest.runtime.main_exe.clone(),
            install_directory: manifest.runtime.install_directory.clone(),
            supervisor_id: manifest.runtime.supervisor_id.clone(),
            icon: manifest.runtime.icon.clone(),
            shortcuts: manifest.runtime.shortcuts.clone(),
            persistent_assets: manifest.runtime.persistent_assets.clone(),
            installers: manifest.runtime.installers.clone(),
            environment: manifest.runtime.environment.clone(),
        };
        write_release_index_entries(store_root, &manifest.app_id, vec![release]);
    }

    fn write_release_index_entries(store_root: &Path, app_id: &str, releases: Vec<ReleaseEntry>) {
        let index = ReleaseIndex {
            app_id: app_id.to_string(),
            releases,
            ..ReleaseIndex::default()
        };
        let compressed = compress_release_index(&index, 3).expect("release index");
        std::fs::write(store_root.join(RELEASES_FILE_COMPRESSED), compressed).expect("write release index");
    }

    #[test]
    fn progress_reporter_logs_each_percent_bucket_once() {
        let base = Instant::now();
        let mut reporter = ProgressReporter::new("Preparing package...");

        assert!(reporter.observe_bytes_at(base, 9, 100).is_none());
        assert_eq!(
            reporter.observe_bytes_at(base, 10, 100),
            Some("Preparing package... 10 B / 100 B (10%)".to_string())
        );
        assert!(reporter.observe_bytes_at(base, 19, 100).is_none());
        assert_eq!(
            reporter.observe_bytes_at(base, 20, 100),
            Some("Preparing package... 20 B / 100 B (20%)".to_string())
        );
    }

    #[test]
    fn progress_reporter_emits_time_based_update_between_percent_buckets() {
        let base = Instant::now();
        let mut reporter = ProgressReporter::new("Downloading package...");

        assert!(reporter.observe_bytes_at(base, 5, 100).is_none());
        assert_eq!(
            reporter.observe_bytes_at(base + PACKAGE_PROGRESS_LOG_INTERVAL + Duration::from_millis(1), 6, 100),
            Some("Downloading package... 6 B / 100 B (6%)".to_string())
        );
    }

    #[tokio::test]
    async fn execute_installs_bundled_payload_and_writes_runtime_manifest() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let installer_dir = temp_dir.path().join("installer");
        let payload_dir = installer_dir.join("payload");
        let install_root = temp_dir.path().join("installed-app");
        let store_root = temp_dir.path().join("store");
        let full_filename = "demo-app-1.2.3-full.tar.zst";

        std::fs::create_dir_all(&payload_dir).expect("payload dir");
        std::fs::create_dir_all(&store_root).expect("store dir");

        let manifest = make_manifest(&install_root, &store_root, full_filename, "offline");
        let installer_yaml = serde_yaml::to_string(&manifest).expect("installer yaml");
        std::fs::write(installer_dir.join("installer.yml"), installer_yaml).expect("installer manifest");
        write_archive(&payload_dir.join(full_filename), b"bundled payload");

        execute(&installer_dir, true).await.expect("setup should succeed");

        let active_app_dir = install_root.join("app");
        assert_eq!(
            std::fs::read_to_string(active_app_dir.join("payload.txt")).expect("payload file"),
            "bundled payload"
        );
        assert!(active_app_dir.join("demoapp").is_file());
        assert!(!install_root.join(".surge-app-next").exists());
        assert!(!install_root.join(".surge-app-prev").exists());

        let runtime_manifest = active_app_dir.join(surge_core::install::RUNTIME_MANIFEST_RELATIVE_PATH);
        let runtime_yaml = std::fs::read_to_string(runtime_manifest).expect("runtime manifest");
        assert!(runtime_yaml.contains("id: demo-app"));
        assert!(runtime_yaml.contains("version: 1.2.3"));
        assert!(runtime_yaml.contains("channel: stable"));
    }

    #[tokio::test]
    async fn resolve_package_downloads_when_bundled_payload_is_missing() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let installer_dir = temp_dir.path().join("installer");
        let install_root = temp_dir.path().join("installed-app");
        let store_root = temp_dir.path().join("store");
        let full_filename = "demo-app-1.2.3-full.tar.zst";
        let stored_archive = store_root.join(full_filename);

        std::fs::create_dir_all(&installer_dir).expect("installer dir");
        std::fs::create_dir_all(&store_root).expect("store dir");
        write_archive(&stored_archive, b"downloaded payload");

        let manifest = make_manifest(&install_root, &store_root, full_filename, "online");
        write_release_index(&store_root, &manifest, &stored_archive);
        let package = resolve_package(&installer_dir, &manifest, &install_root)
            .await
            .expect("downloaded package");

        assert!(package.path.is_file());
        assert_eq!(
            std::fs::read(&package.path).expect("downloaded bytes"),
            std::fs::read(stored_archive).expect("stored bytes")
        );
        assert!(package.required_artifacts.is_some());
    }

    #[tokio::test]
    async fn execute_prunes_stale_artifact_cache_entries_after_online_setup() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let installer_dir = temp_dir.path().join("installer");
        let install_root = temp_dir.path().join("installed-app");
        let store_root = temp_dir.path().join("store");
        let full_filename = "demo-app-1.2.3-full.tar.zst";
        let stored_archive = store_root.join(full_filename);
        let stale_path = install_root.join(".surge-cache").join("artifacts").join("stale.bin");

        std::fs::create_dir_all(&installer_dir).expect("installer dir");
        std::fs::create_dir_all(&store_root).expect("store dir");
        std::fs::create_dir_all(stale_path.parent().expect("stale parent")).expect("cache dir");
        std::fs::write(&stale_path, b"stale").expect("stale cache entry");
        write_archive(&stored_archive, b"cached payload");

        let manifest = make_manifest(&install_root, &store_root, full_filename, "online");
        write_release_index(&store_root, &manifest, &stored_archive);
        let installer_yaml = serde_yaml::to_string(&manifest).expect("installer yaml");
        std::fs::write(installer_dir.join("installer.yml"), installer_yaml).expect("installer manifest");

        execute(&installer_dir, true).await.expect("setup should succeed");

        assert!(!stale_path.exists(), "stale cache entry should be pruned");
        assert!(
            install_root
                .join(".surge-cache")
                .join("artifacts")
                .join(full_filename)
                .is_file(),
            "resolved package should remain in cache"
        );
    }

    #[tokio::test]
    async fn execute_retains_installed_full_in_artifact_cache_when_release_graph_prunes_it() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let installer_dir = temp_dir.path().join("installer");
        let install_root = temp_dir.path().join("installed-app");
        let store_root = temp_dir.path().join("store");
        let rid = current_rid();
        let base_full_filename = "demo-app-1.0.0-full.tar.zst";
        let target_full_filename = "demo-app-1.2.3-full.tar.zst";
        let delta_filename = "demo-app-1.2.3-delta.tar.zst";
        let base_archive_path = store_root.join(base_full_filename);
        let target_archive_path = temp_dir.path().join(target_full_filename);
        let delta_path = store_root.join(delta_filename);

        std::fs::create_dir_all(&installer_dir).expect("installer dir");
        std::fs::create_dir_all(&store_root).expect("store dir");
        write_archive(&base_archive_path, b"base payload");
        write_archive(&target_archive_path, b"target payload");

        let base_archive = std::fs::read(&base_archive_path).expect("base archive");
        let target_archive = std::fs::read(&target_archive_path).expect("target archive");
        let patch = bsdiff_buffers(&base_archive, &target_archive).expect("delta patch");
        let delta_bytes = zstd::encode_all(patch.as_slice(), 3).expect("delta bytes");
        std::fs::write(&delta_path, &delta_bytes).expect("write delta");

        let manifest = make_manifest(&install_root, &store_root, target_full_filename, "online");
        let installer_yaml = serde_yaml::to_string(&manifest).expect("installer yaml");
        std::fs::write(installer_dir.join("installer.yml"), installer_yaml).expect("installer manifest");

        let mut base_release = ReleaseEntry {
            version: "1.0.0".to_string(),
            channels: vec![manifest.channel.clone()],
            os: "linux".to_string(),
            rid: rid.clone(),
            is_genesis: false,
            full_filename: base_full_filename.to_string(),
            full_size: i64::try_from(base_archive.len()).expect("base size"),
            full_sha256: sha256_hex(&base_archive),
            deltas: Vec::new(),
            preferred_delta_id: String::new(),
            created_utc: manifest.generated_utc.clone(),
            release_notes: String::new(),
            name: manifest.runtime.name.clone(),
            main_exe: manifest.runtime.main_exe.clone(),
            install_directory: manifest.runtime.install_directory.clone(),
            supervisor_id: manifest.runtime.supervisor_id.clone(),
            icon: manifest.runtime.icon.clone(),
            shortcuts: manifest.runtime.shortcuts.clone(),
            persistent_assets: manifest.runtime.persistent_assets.clone(),
            installers: manifest.runtime.installers.clone(),
            environment: manifest.runtime.environment.clone(),
        };
        base_release.set_primary_delta(None);

        let mut target_release = ReleaseEntry {
            version: manifest.version.clone(),
            channels: vec![manifest.channel.clone()],
            os: "linux".to_string(),
            rid,
            is_genesis: false,
            full_filename: target_full_filename.to_string(),
            full_size: i64::try_from(target_archive.len()).expect("target size"),
            full_sha256: sha256_hex(&target_archive),
            deltas: Vec::new(),
            preferred_delta_id: String::new(),
            created_utc: manifest.generated_utc.clone(),
            release_notes: String::new(),
            name: manifest.runtime.name.clone(),
            main_exe: manifest.runtime.main_exe.clone(),
            install_directory: manifest.runtime.install_directory.clone(),
            supervisor_id: manifest.runtime.supervisor_id.clone(),
            icon: manifest.runtime.icon.clone(),
            shortcuts: manifest.runtime.shortcuts.clone(),
            persistent_assets: manifest.runtime.persistent_assets.clone(),
            installers: manifest.runtime.installers.clone(),
            environment: manifest.runtime.environment.clone(),
        };
        target_release.set_primary_delta(Some(DeltaArtifact::bsdiff_zstd(
            "primary",
            "1.0.0",
            delta_filename,
            i64::try_from(delta_bytes.len()).expect("delta size"),
            &sha256_hex(&delta_bytes),
        )));

        write_release_index_entries(&store_root, &manifest.app_id, vec![base_release, target_release]);

        execute(&installer_dir, true).await.expect("setup should succeed");

        let artifact_cache = install_root.join(".surge-cache").join("artifacts");
        assert!(
            artifact_cache.join(base_full_filename).is_file(),
            "base full should remain because the release graph still needs it"
        );
        assert!(
            artifact_cache.join(delta_filename).is_file(),
            "delta should remain because the release graph still needs it"
        );
        assert!(
            artifact_cache.join(target_full_filename).is_file(),
            "installed target full should remain as a warm cache entry"
        );
    }
}
