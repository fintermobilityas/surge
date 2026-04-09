use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::logline;
use surge_core::config::installer::InstallerManifest;
use surge_core::error::{Result, SurgeError};
use surge_core::install::{self as core_install, InstallProfile};
use surge_core::installer_package::{
    InstallerPackageAcquisition, ResolveInstallerPackageOptions, ResolvedInstallerPackage, install_artifact_cache_dir,
    prune_install_artifact_cache, resolve_installer_package,
};
use surge_core::platform::fs::make_executable;
use surge_core::platform::paths::default_install_root;
use surge_core::releases::artifact_cache::cache_path_for_key;
use surge_core::releases::restore::RestoreProgress;

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
pub async fn execute(dir: &Path, no_start: bool, stage: bool) -> Result<()> {
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

    if stage {
        let package = resolve_package(dir, &manifest, &install_root).await?;
        ensure_stage_cache_entry(&package, &manifest, &install_root)?;
        persist_staged_installer_cache(dir, &manifest, &install_root)?;
        logline::success(&format!(
            "Staged '{}' v{} in artifact cache at '{}'",
            manifest.app_id,
            manifest.version,
            install_root.display()
        ));
        return Ok(());
    }

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
    let download_progress = Mutex::new(ProgressReporter::new("Downloading package..."));
    let restore_progress = Mutex::new(ProgressReporter::new("Preparing package..."));

    let package = resolve_installer_package(
        dir,
        manifest,
        install_root,
        ResolveInstallerPackageOptions {
            download_progress: Some(&|done, total| {
                let mut reporter = download_progress
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(message) = reporter.observe_bytes(done, total) {
                    logline::subtle(&message);
                }
            }),
            restore_progress: Some(&|update| {
                let mut reporter = restore_progress
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(message) = reporter.observe_restore(update) {
                    logline::subtle(&message);
                }
            }),
            stage: None,
        },
    )
    .await?;

    match package.acquisition {
        InstallerPackageAcquisition::BundledPayload => {
            logline::info(&format!("Using bundled payload: {}", package.path().display()));
        }
        InstallerPackageAcquisition::ArtifactCache => {
            logline::info(&format!(
                "Using cached package from artifact cache: {}",
                package.path().display()
            ));
        }
        InstallerPackageAcquisition::PreparedArtifactCache => {
            logline::success(&format!(
                "Prepared '{}' in artifact cache ({})",
                full_filename,
                file_size_label(package.path())
            ));
        }
        InstallerPackageAcquisition::ArtifactCacheFallback => {
            logline::warn(&format!(
                "Using cached package from artifact cache without release-index verification: {}",
                package.path().display()
            ));
        }
        InstallerPackageAcquisition::Downloaded => {
            logline::success(&format!(
                "Downloaded '{}' to artifact cache ({})",
                full_filename,
                file_size_label(package.path())
            ));
        }
    }

    Ok(package)
}

fn ensure_stage_cache_entry(
    package: &ResolvedPackage,
    manifest: &InstallerManifest,
    install_root: &Path,
) -> Result<()> {
    if package.acquisition != InstallerPackageAcquisition::BundledPayload {
        return Ok(());
    }

    let artifact_cache_dir = install_artifact_cache_dir(install_root);
    std::fs::create_dir_all(&artifact_cache_dir)?;
    let cached_package_path = cache_path_for_key(&artifact_cache_dir, manifest.release.full_filename.trim())?;
    std::fs::copy(package.path(), &cached_package_path)?;
    logline::info(&format!(
        "Copied bundled payload into artifact cache: {}",
        cached_package_path.display()
    ));
    Ok(())
}

fn persist_staged_installer_cache(dir: &Path, manifest: &InstallerManifest, install_root: &Path) -> Result<()> {
    if !manifest.installer_type.trim().eq_ignore_ascii_case("online") {
        return Ok(());
    }

    let surge_binary_name = staged_installer_binary_name();
    let surge_binary_path = dir.join(surge_binary_name);
    if !surge_binary_path.is_file() {
        return Err(SurgeError::Config(format!(
            "Online stage cache is missing embedded surge binary '{}'",
            surge_binary_path.display()
        )));
    }

    let installer_manifest_path = dir.join("installer.yml");
    if !installer_manifest_path.is_file() {
        return Err(SurgeError::Config(format!(
            "Online stage cache is missing installer manifest '{}'",
            installer_manifest_path.display()
        )));
    }

    let staged_installer_dir = install_root.join(".surge-cache").join("staged-installer");
    if staged_installer_dir.exists() {
        std::fs::remove_dir_all(&staged_installer_dir)?;
    }
    std::fs::create_dir_all(&staged_installer_dir)?;

    let cached_surge_binary = staged_installer_dir.join(surge_binary_name);
    std::fs::copy(&surge_binary_path, &cached_surge_binary)?;
    make_executable(&cached_surge_binary)?;
    std::fs::copy(&installer_manifest_path, staged_installer_dir.join("installer.yml"))?;

    let staged_identity = serde_json::json!({
        "app_id": manifest.app_id.trim(),
        "version": manifest.version.trim(),
        "channel": manifest.channel.trim(),
        "rid": manifest.rid.trim(),
        "full_filename": manifest.release.full_filename.trim(),
        "full_sha256": manifest.release.full_sha256.trim(),
        "install_directory": manifest.runtime.install_directory.trim(),
        "supervisor_id": manifest.runtime.supervisor_id.trim(),
        "storage_provider": manifest.storage.provider.trim(),
        "storage_bucket": manifest.storage.bucket.trim(),
        "storage_region": manifest.storage.region.trim(),
        "storage_endpoint": manifest.storage.endpoint.trim(),
    });
    std::fs::write(
        staged_installer_dir.join(".surge-staged-release.json"),
        serde_json::to_vec(&staged_identity)
            .map_err(|e| SurgeError::Config(format!("Failed to serialize staged installer identity: {e}")))?,
    )?;

    logline::info(&format!(
        "Persisted staged installer support files in '{}'.",
        staged_installer_dir.display()
    ));

    Ok(())
}

fn staged_installer_binary_name() -> &'static str {
    if cfg!(windows) { "surge.exe" } else { "surge" }
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

type ResolvedPackage = ResolvedInstallerPackage;

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
                full_sha256: String::new(),
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

        execute(&installer_dir, true, false)
            .await
            .expect("setup should succeed");

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
    async fn execute_preserves_persistent_assets_and_prunes_stale_snapshots() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let installer_dir = temp_dir.path().join("installer");
        let payload_dir = installer_dir.join("payload");
        let install_root = temp_dir.path().join("installed-app");
        let active_app_dir = install_root.join("app");
        let store_root = temp_dir.path().join("store");
        let full_filename = "demo-app-1.2.3-full.tar.zst";

        std::fs::create_dir_all(active_app_dir.join("state")).expect("state dir should exist");
        std::fs::create_dir_all(payload_dir.parent().expect("payload parent")).expect("payload parent should exist");
        std::fs::create_dir_all(install_root.join("app-1.0.0")).expect("stale snapshot should exist");
        std::fs::create_dir_all(install_root.join("app-0.9.0")).expect("stale snapshot should exist");
        std::fs::write(active_app_dir.join("settings.json"), "persisted settings").expect("settings should exist");
        std::fs::write(active_app_dir.join("state").join("cache.bin"), "persisted cache").expect("state should exist");
        std::fs::write(active_app_dir.join("old.txt"), "remove me").expect("old file should exist");

        let mut manifest = make_manifest(&install_root, &store_root, full_filename, "offline");
        manifest.runtime.persistent_assets = vec!["settings.json".to_string(), "state".to_string()];
        let installer_yaml = serde_yaml::to_string(&manifest).expect("installer yaml");
        std::fs::write(installer_dir.join("installer.yml"), installer_yaml).expect("installer manifest");

        let mut packer = ArchivePacker::new(3).expect("archive packer");
        packer
            .add_buffer("demoapp", b"#!/bin/sh\necho demo\n", 0o755)
            .expect("demoapp entry");
        packer
            .add_buffer("settings.json", b"packaged settings", 0o644)
            .expect("settings entry");
        packer
            .add_buffer("state/cache.bin", b"packaged cache", 0o644)
            .expect("state entry");
        packer
            .add_buffer("payload.txt", b"bundled payload", 0o644)
            .expect("payload entry");
        packer
            .finalize_to_file(&payload_dir.join(full_filename))
            .expect("archive file");

        execute(&installer_dir, true, false)
            .await
            .expect("setup should succeed");

        let active_app_dir = install_root.join("app");
        assert_eq!(
            std::fs::read_to_string(active_app_dir.join("settings.json")).expect("settings should exist"),
            "persisted settings"
        );
        assert_eq!(
            std::fs::read_to_string(active_app_dir.join("state").join("cache.bin")).expect("state should exist"),
            "persisted cache"
        );
        assert_eq!(
            std::fs::read_to_string(active_app_dir.join("payload.txt")).expect("payload should exist"),
            "bundled payload"
        );
        assert!(
            !active_app_dir.join("old.txt").exists(),
            "undeclared assets should be removed"
        );
        assert!(
            !install_root.join("app-1.0.0").exists(),
            "stale snapshot should be pruned"
        );
        assert!(
            !install_root.join("app-0.9.0").exists(),
            "stale snapshot should be pruned"
        );
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

        assert!(package.path().is_file());
        assert_eq!(
            std::fs::read(package.path()).expect("downloaded bytes"),
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

        execute(&installer_dir, true, false)
            .await
            .expect("setup should succeed");

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

        execute(&installer_dir, true, false)
            .await
            .expect("setup should succeed");

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

    #[tokio::test]
    async fn execute_stage_persists_bundled_payload_in_artifact_cache_without_installing() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let installer_dir = temp_dir.path().join("installer");
        let payload_dir = installer_dir.join("payload");
        let install_root = temp_dir.path().join("installed-app");
        let store_root = temp_dir.path().join("store");
        let full_filename = "demo-app-1.2.3-full.tar.zst";
        let bundled_payload = payload_dir.join(full_filename);

        std::fs::create_dir_all(&payload_dir).expect("payload dir");
        std::fs::create_dir_all(&store_root).expect("store dir");

        let manifest = make_manifest(&install_root, &store_root, full_filename, "offline");
        let installer_yaml = serde_yaml::to_string(&manifest).expect("installer yaml");
        std::fs::write(installer_dir.join("installer.yml"), installer_yaml).expect("installer manifest");
        write_archive(&bundled_payload, b"bundled payload");
        let bundled_archive = std::fs::read(&bundled_payload).expect("bundled payload should exist");

        execute(&installer_dir, true, true)
            .await
            .expect("setup stage should succeed");

        let cached_package = install_root.join(".surge-cache").join("artifacts").join(full_filename);
        assert!(
            cached_package.is_file(),
            "bundled payload should be copied into the artifact cache"
        );
        assert_eq!(
            std::fs::read(&cached_package).expect("cached package should exist"),
            bundled_archive
        );
        assert!(
            !install_root.join("app").exists(),
            "stage mode should not activate the install"
        );
    }

    #[tokio::test]
    async fn execute_stage_persists_online_installer_cache_without_installing() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let installer_dir = temp_dir.path().join("installer");
        let install_root = temp_dir.path().join("installed-app");
        let store_root = temp_dir.path().join("store");
        let full_filename = "demo-app-1.2.3-full.tar.zst";
        let stored_archive = store_root.join(full_filename);

        std::fs::create_dir_all(&installer_dir).expect("installer dir");
        std::fs::create_dir_all(&store_root).expect("store dir");
        write_archive(&stored_archive, b"downloaded payload");

        let mut manifest = make_manifest(&install_root, &store_root, full_filename, "online");
        manifest.release.full_sha256 = sha256_hex(&std::fs::read(&stored_archive).expect("stored bytes"));
        write_release_index(&store_root, &manifest, &stored_archive);
        let installer_yaml = serde_yaml::to_string(&manifest).expect("installer yaml");
        std::fs::write(installer_dir.join("installer.yml"), installer_yaml).expect("installer manifest");
        let surge_binary = installer_dir.join(staged_installer_binary_name());
        std::fs::write(&surge_binary, b"#!/bin/sh\nexit 0\n").expect("surge stub");
        make_executable(&surge_binary).expect("surge stub should be executable");

        execute(&installer_dir, true, true)
            .await
            .expect("setup stage should succeed");

        let staged_installer_dir = install_root.join(".surge-cache").join("staged-installer");
        assert!(
            staged_installer_dir.join(staged_installer_binary_name()).is_file(),
            "online stage should persist the surge helper"
        );
        assert!(
            staged_installer_dir.join("installer.yml").is_file(),
            "online stage should persist the installer manifest"
        );
        let staged_identity = std::fs::read_to_string(staged_installer_dir.join(".surge-staged-release.json"))
            .expect("staged identity should exist");
        assert!(staged_identity.contains("\"version\":\"1.2.3\""));
        assert!(staged_identity.contains(&manifest.release.full_sha256));
        assert!(
            !install_root.join("app").exists(),
            "stage mode should not activate the install"
        );
    }
}
