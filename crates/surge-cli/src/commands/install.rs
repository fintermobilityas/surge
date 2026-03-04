use std::path::Path;

use crate::logline;
use tokio::process::Command;

use surge_core::config::constants::RELEASES_FILE_COMPRESSED;
use surge_core::config::manifest::SurgeManifest;
use surge_core::error::{Result, SurgeError};
use surge_core::install::{self as core_install, InstallProfile};
use surge_core::releases::manifest::{ReleaseEntry, ReleaseIndex, decompress_release_index};
use surge_core::releases::restore::restore_full_archive_for_version;
use surge_core::releases::version::compare_versions;
use surge_core::storage::{self, StorageBackend};

#[derive(Debug, Clone, Copy, Default)]
pub struct StorageOverrides<'a> {
    pub provider: Option<&'a str>,
    pub bucket: Option<&'a str>,
    pub region: Option<&'a str>,
    pub endpoint: Option<&'a str>,
    pub prefix: Option<&'a str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RuntimeProfile {
    os: String,
    arch: String,
    gpu: String,
}

impl RuntimeProfile {
    fn has_nvidia_gpu(&self) -> bool {
        let gpu = self.gpu.trim().to_ascii_lowercase();
        gpu == "nvidia" || gpu == "true" || gpu == "yes"
    }
}

enum InstallTarget {
    Local,
    Tailscale { ssh_target: String, file_target: String },
}

pub async fn execute(
    manifest_path: &Path,
    application_manifest_path: &Path,
    node: Option<&str>,
    ssh_user: Option<&str>,
    app_id: Option<&str>,
    channel: &str,
    rid: Option<&str>,
    version: Option<&str>,
    plan_only: bool,
    no_start: bool,
    download_dir: &Path,
    overrides: StorageOverrides<'_>,
) -> Result<()> {
    let manifest = load_install_manifest(application_manifest_path, manifest_path)?;
    let app_id = super::resolve_app_id_with_rid_hint(&manifest, app_id, rid)?;

    let install_target = match node.map(str::trim).filter(|value| !value.is_empty()) {
        Some(node) => {
            let (ssh_target, file_target) = resolve_tailscale_targets(node, ssh_user)?;
            InstallTarget::Tailscale {
                ssh_target,
                file_target,
            }
        }
        None => InstallTarget::Local,
    };

    let (rid_candidates, profile) = if let Some(requested_rid) = rid.map(str::trim).filter(|value| !value.is_empty()) {
        (vec![requested_rid.to_string()], None::<RuntimeProfile>)
    } else {
        let detected = match &install_target {
            InstallTarget::Local => detect_local_profile(),
            InstallTarget::Tailscale { ssh_target, .. } => detect_remote_profile(ssh_target).await?,
        };
        let base_rid = derive_base_rid(&detected).ok_or_else(|| {
            SurgeError::Platform(format!(
                "Unable to map profile to a RID (os='{}', arch='{}'). Use --rid to override.",
                detected.os, detected.arch
            ))
        })?;
        (
            build_rid_candidates(&base_rid, detected.has_nvidia_gpu()),
            Some(detected),
        )
    };

    let storage_config = build_storage_config_with_overrides(&manifest, &app_id, overrides)?;
    let backend = storage::create_storage_backend(&storage_config)?;
    let index = fetch_release_index(&*backend).await?;
    if !index.app_id.is_empty() && index.app_id != app_id {
        return Err(SurgeError::NotFound(format!(
            "Release index belongs to app '{}' not '{}'",
            index.app_id, app_id
        )));
    }

    let release = select_release(&index.releases, channel, version, &rid_candidates).ok_or_else(|| {
        let version_suffix = version.map_or_else(String::new, |v| format!(" and version '{v}'"));
        SurgeError::NotFound(format!(
            "No release found on channel '{channel}' for RID candidates [{}]{version_suffix}",
            rid_candidates.join(", ")
        ))
    })?;

    let selected_rid = if release.rid.is_empty() {
        "<generic>".to_string()
    } else {
        release.rid.clone()
    };

    if let Some(profile) = profile {
        match &install_target {
            InstallTarget::Local => logline::info(&format!(
                "Local profile: os={}, arch={}, gpu={}",
                profile.os, profile.arch, profile.gpu
            )),
            InstallTarget::Tailscale { ssh_target, .. } => logline::info(&format!(
                "Remote profile for {ssh_target}: os={}, arch={}, gpu={}",
                profile.os, profile.arch, profile.gpu
            )),
        }
    }
    logline::info(&format!("RID candidates: {}", rid_candidates.join(", ")));
    logline::success(&format!(
        "Selected release: app={} version={} rid={} channels={} full_package={}",
        app_id,
        release.version,
        selected_rid,
        if release.channels.is_empty() {
            "-".to_string()
        } else {
            release.channels.join(",")
        },
        release.full_filename
    ));

    let full_filename = release.full_filename.trim();
    if full_filename.is_empty() {
        return Err(SurgeError::NotFound(format!(
            "Release {} ({selected_rid}) has no full package filename",
            release.version
        )));
    }

    if plan_only {
        match &install_target {
            InstallTarget::Local => {
                logline::warn("Plan only mode: no download performed. Remove --plan-only to fetch the package.");
            }
            InstallTarget::Tailscale { file_target, .. } => logline::warn(&format!(
                "Plan only mode: no transfer performed. Remove --plan-only to download and copy package to {file_target}."
            )),
        }
        return Ok(());
    }

    std::fs::create_dir_all(download_dir)?;
    let local_package = download_dir.join(Path::new(full_filename).file_name().unwrap_or_default());
    logline::info(&format!(
        "Downloading package '{}' to '{}'",
        full_filename,
        local_package.display()
    ));
    let reconstructed = download_release_archive(
        &*backend,
        &index,
        release,
        &rid_candidates,
        full_filename,
        &local_package,
    )
    .await?;
    if reconstructed {
        logline::warn(&format!(
            "Direct full package '{}' missing in backend; reconstructed from retained release artifacts.",
            Path::new(full_filename).display()
        ));
    }

    match &install_target {
        InstallTarget::Local => {
            let install_root = install_package_locally(&app_id, release, &local_package)?;
            let active_app_dir = install_root.join("app");
            logline::success(&format!(
                "Downloaded '{}' to '{}'.",
                Path::new(full_filename).display(),
                local_package.display()
            ));
            logline::success(&format!(
                "Installed '{}' to '{}' (active app: '{}').",
                app_id,
                install_root.display(),
                active_app_dir.display()
            ));

            if !no_start && !plan_only {
                let display_name = release.display_name(&app_id);
                match auto_start_after_install(release, &app_id, &install_root, &active_app_dir) {
                    Ok(pid) => {
                        logline::success(&format!("Started '{display_name}' (pid {pid})."));
                    }
                    Err(e) => {
                        logline::warn(&format!("Auto-start failed: {e}"));
                    }
                }
            }
        }
        InstallTarget::Tailscale { file_target, .. } => {
            copy_file_to_tailscale_node(file_target, &local_package).await?;
            logline::success(&format!(
                "Copied '{}' to node '{}' via tailscale file sharing.",
                local_package.display(),
                file_target
            ));
            logline::subtle(&format!(
                "Install hint on node {}: extract '{}' into the install directory for app '{}'.",
                file_target,
                Path::new(full_filename).display(),
                app_id
            ));
        }
    }

    Ok(())
}

fn resolve_tailscale_targets(node: &str, ssh_user: Option<&str>) -> Result<(String, String)> {
    let node = node.trim();
    if node.is_empty() {
        return Err(SurgeError::Config(
            "Tailscale node cannot be empty. Provide --node <node>.".to_string(),
        ));
    }

    if let Some((user_part, host_part)) = node.split_once('@') {
        if user_part.trim().is_empty() || host_part.trim().is_empty() {
            return Err(SurgeError::Config(format!(
                "Invalid --node value '{node}'. Expected '<node>' or '<user>@<node>'."
            )));
        }
        return Ok((node.to_string(), host_part.to_string()));
    }

    if let Some(user) = ssh_user.map(str::trim).filter(|value| !value.is_empty()) {
        Ok((format!("{user}@{node}"), node.to_string()))
    } else {
        Ok((node.to_string(), node.to_string()))
    }
}

fn load_install_manifest(application_manifest_path: &Path, fallback_manifest_path: &Path) -> Result<SurgeManifest> {
    if application_manifest_path.is_file() {
        return SurgeManifest::from_file(application_manifest_path);
    }
    SurgeManifest::from_file(fallback_manifest_path)
}

fn release_install_profile<'a>(app_id: &'a str, release: &'a ReleaseEntry) -> InstallProfile<'a> {
    InstallProfile {
        app_id,
        display_name: release.display_name(app_id),
        main_exe: &release.main_exe,
        install_directory: &release.install_directory,
        supervisor_id: &release.supervisor_id,
        icon: &release.icon,
        shortcuts: &release.shortcuts,
        environment: &release.environment,
    }
}

fn install_package_locally(app_id: &str, release: &ReleaseEntry, package_path: &Path) -> Result<std::path::PathBuf> {
    let profile = release_install_profile(app_id, release);
    core_install::install_package_locally(&profile, package_path)
}

fn auto_start_after_install(
    release: &ReleaseEntry,
    app_id: &str,
    install_root: &std::path::Path,
    active_app_dir: &std::path::Path,
) -> Result<u32> {
    let profile = release_install_profile(app_id, release);
    core_install::auto_start_after_install(&profile, install_root, active_app_dir)
}

fn build_storage_config_with_overrides(
    manifest: &SurgeManifest,
    app_id: &str,
    overrides: StorageOverrides<'_>,
) -> Result<surge_core::context::StorageConfig> {
    let mut config = super::build_app_scoped_storage_config(manifest, app_id)?;

    if let Some(provider) = overrides.provider.map(str::trim).filter(|value| !value.is_empty()) {
        config.provider = Some(super::parse_storage_provider(provider)?);
    }
    if let Some(bucket) = overrides.bucket.map(str::trim).filter(|value| !value.is_empty()) {
        config.bucket = bucket.to_string();
    }
    if let Some(region) = overrides.region.map(str::trim).filter(|value| !value.is_empty()) {
        config.region = region.to_string();
    }
    if let Some(endpoint) = overrides.endpoint.map(str::trim).filter(|value| !value.is_empty()) {
        config.endpoint = endpoint.to_string();
    }
    if let Some(prefix) = overrides.prefix.map(str::trim).filter(|value| !value.is_empty()) {
        config.prefix = prefix.to_string();
    }

    Ok(config)
}

async fn fetch_release_index(backend: &dyn StorageBackend) -> Result<ReleaseIndex> {
    match backend.get_object(RELEASES_FILE_COMPRESSED).await {
        Ok(data) => decompress_release_index(&data),
        Err(SurgeError::NotFound(_)) => Ok(ReleaseIndex::default()),
        Err(e) => Err(e),
    }
}

async fn download_release_archive(
    backend: &dyn StorageBackend,
    index: &ReleaseIndex,
    release: &ReleaseEntry,
    rid_candidates: &[String],
    full_filename: &str,
    destination: &Path,
) -> Result<bool> {
    match backend.download_to_file(full_filename, destination, None).await {
        Ok(()) => Ok(false),
        Err(SurgeError::NotFound(_)) => {
            let restore_rid = if release.rid.trim().is_empty() {
                rid_candidates.first().map_or("", String::as_str)
            } else {
                release.rid.as_str()
            };
            let rebuilt = restore_full_archive_for_version(backend, index, restore_rid, &release.version).await?;
            std::fs::write(destination, rebuilt)?;
            Ok(true)
        }
        Err(e) => Err(e),
    }
}

fn select_release<'a>(
    releases: &'a [ReleaseEntry],
    channel: &str,
    version: Option<&str>,
    rid_candidates: &[String],
) -> Option<&'a ReleaseEntry> {
    let mut eligible: Vec<&ReleaseEntry> = releases
        .iter()
        .filter(|release| release.channels.iter().any(|c| c == channel))
        .collect();

    if let Some(version) = version.map(str::trim).filter(|v| !v.is_empty()) {
        eligible.retain(|release| release.version == version);
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

fn detect_local_profile() -> RuntimeProfile {
    let os = std::env::consts::OS.to_string();
    let arch = std::env::consts::ARCH.to_string();
    let gpu = if has_local_nvidia_gpu() {
        "nvidia".to_string()
    } else {
        "none".to_string()
    };
    RuntimeProfile { os, arch, gpu }
}

fn has_local_nvidia_gpu() -> bool {
    std::process::Command::new("nvidia-smi")
        .arg("-L")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

async fn detect_remote_profile(node: &str) -> Result<RuntimeProfile> {
    let unix_probe = r#"os=$(uname -s 2>/dev/null || echo unknown); arch=$(uname -m 2>/dev/null || echo unknown); gpu=none; if command -v nvidia-smi >/dev/null 2>&1; then gpu=nvidia; fi; printf "os=%s\narch=%s\ngpu=%s\n" "$os" "$arch" "$gpu""#;
    let unix_failure = match run_tailscale_capture(&["ssh", node, "sh", "-lc", unix_probe]).await {
        Ok(stdout) => {
            if let Some(profile) = parse_remote_profile(&stdout) {
                return Ok(profile);
            }
            "profile probe succeeded but output was not parseable".to_string()
        }
        Err(unix_err) => unix_err.to_string(),
    };

    let windows_probe = "$os='Windows'; $arch=[System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString(); $gpu='none'; if (Get-CimInstance Win32_VideoController | Where-Object { $_.Name -match 'NVIDIA' } | Select-Object -First 1) { $gpu='nvidia' }; Write-Output \"os=$os\"; Write-Output \"arch=$arch\"; Write-Output \"gpu=$gpu\"";
    match run_tailscale_capture(&[
        "ssh",
        node,
        "powershell",
        "-NoProfile",
        "-NonInteractive",
        "-Command",
        windows_probe,
    ])
    .await
    {
        Ok(stdout) => parse_remote_profile(&stdout).ok_or_else(|| {
            SurgeError::Platform(format!(
                "Unable to parse remote profile from node '{node}' (unix probe failed: {unix_failure}). Use --rid to override."
            ))
        }),
        Err(windows_err) => Err(SurgeError::Platform(format!(
            "Unable to detect remote profile for '{node}'. Unix probe failed ({unix_failure}) and Windows probe failed ({windows_err}). Use --rid to override."
        ))),
    }
}

fn parse_remote_profile(raw: &str) -> Option<RuntimeProfile> {
    let mut os = String::new();
    let mut arch = String::new();
    let mut gpu = String::from("none");

    for line in raw.lines() {
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let value = value.trim();
        match key.trim() {
            "os" => os = value.to_string(),
            "arch" => arch = value.to_string(),
            "gpu" => gpu = value.to_string(),
            _ => {}
        }
    }

    if os.is_empty() || arch.is_empty() {
        return None;
    }

    Some(RuntimeProfile { os, arch, gpu })
}

fn derive_base_rid(profile: &RuntimeProfile) -> Option<String> {
    let os = normalize_os(&profile.os)?;
    let arch = normalize_arch(&profile.arch)?;
    Some(format!("{os}-{arch}"))
}

fn normalize_os(raw: &str) -> Option<&'static str> {
    let os = raw.trim().to_ascii_lowercase();
    if os.contains("linux") {
        Some("linux")
    } else if os.contains("darwin") || os.contains("mac") {
        Some("osx")
    } else if os.contains("windows") || os.contains("mingw") || os.contains("msys") {
        Some("win")
    } else {
        None
    }
}

fn normalize_arch(raw: &str) -> Option<&'static str> {
    let arch = raw.trim().to_ascii_lowercase();
    if arch == "x86_64" || arch == "amd64" || arch == "x64" {
        Some("x64")
    } else if arch == "aarch64" || arch == "arm64" {
        Some("arm64")
    } else if arch == "x86" || arch == "i386" || arch == "i686" {
        Some("x86")
    } else {
        None
    }
}

fn build_rid_candidates(base_rid: &str, nvidia_gpu: bool) -> Vec<String> {
    let mut candidates: Vec<String> = Vec::new();
    let mut push_unique = |candidate: String| {
        if !candidates.iter().any(|existing| existing == &candidate) {
            candidates.push(candidate);
        }
    };

    if nvidia_gpu {
        push_unique(format!("{base_rid}-nvidia"));
        push_unique(format!("{base_rid}-cuda"));
        push_unique(format!("{base_rid}-gpu"));
    }
    push_unique(base_rid.to_string());
    if !nvidia_gpu {
        push_unique(format!("{base_rid}-cpu"));
    }

    candidates
}

async fn copy_file_to_tailscale_node(node: &str, local_file: &Path) -> Result<()> {
    let source = local_file.display().to_string();
    let target = format!("{node}:");
    let args = ["file", "cp", source.as_str(), target.as_str()];
    run_tailscale_capture(&args).await?;
    Ok(())
}

async fn run_tailscale_capture(args: &[&str]) -> Result<String> {
    let output = Command::new("tailscale")
        .args(args)
        .output()
        .await
        .map_err(|e| SurgeError::Platform(format!("Failed to run tailscale command: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let cmd = format!("tailscale {}", args.join(" "));
        let msg = if stderr.is_empty() {
            format!("Command failed: {cmd}")
        } else {
            format!("Command failed: {cmd}: {stderr}")
        };
        return Err(SurgeError::Platform(msg));
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::Path;

    use super::*;
    use surge_core::archive::packer::ArchivePacker;
    use surge_core::config::manifest::ShortcutLocation;
    use surge_core::config::manifest::SurgeManifest;
    use surge_core::crypto::sha256::sha256_hex;
    use surge_core::diff::wrapper::bsdiff_buffers;
    use surge_core::releases::manifest::DeltaArtifact;
    use surge_core::storage::filesystem::FilesystemBackend;

    fn release(version: &str, channel: &str, rid: &str, full: &str) -> ReleaseEntry {
        ReleaseEntry {
            version: version.to_string(),
            channels: vec![channel.to_string()],
            os: "linux".to_string(),
            rid: rid.to_string(),
            is_genesis: false,
            full_filename: full.to_string(),
            full_size: 1,
            full_sha256: "x".to_string(),
            deltas: Vec::new(),
            preferred_delta_id: String::new(),
            created_utc: String::new(),
            release_notes: String::new(),
            name: String::new(),
            main_exe: String::new(),
            install_directory: String::new(),
            supervisor_id: String::new(),
            icon: String::new(),
            shortcuts: vec![ShortcutLocation::Desktop],
            persistent_assets: Vec::new(),
            installers: Vec::new(),
            environment: BTreeMap::new(),
        }
    }

    #[test]
    fn parse_profile_output() {
        let raw = "os=Linux\narch=x86_64\ngpu=nvidia\n";
        let profile = parse_remote_profile(raw).expect("profile should parse");
        assert_eq!(profile.os, "Linux");
        assert_eq!(profile.arch, "x86_64");
        assert!(profile.has_nvidia_gpu());
    }

    #[test]
    fn resolve_targets_plain_node_without_user() {
        let (ssh_target, file_target) = resolve_tailscale_targets("edge-node", None).expect("targets");
        assert_eq!(ssh_target, "edge-node");
        assert_eq!(file_target, "edge-node");
    }

    #[test]
    fn resolve_targets_plain_node_with_ssh_user() {
        let (ssh_target, file_target) = resolve_tailscale_targets("edge-node", Some("operator")).expect("targets");
        assert_eq!(ssh_target, "operator@edge-node");
        assert_eq!(file_target, "edge-node");
    }

    #[test]
    fn resolve_targets_user_at_node_keeps_file_target_host_only() {
        let (ssh_target, file_target) = resolve_tailscale_targets("alice@edge-node", Some("ignored")).expect("targets");
        assert_eq!(ssh_target, "alice@edge-node");
        assert_eq!(file_target, "edge-node");
    }

    #[test]
    fn install_package_locally_creates_expected_app_layout() {
        let tmp = tempfile::tempdir().expect("temp dir should exist");
        let install_root = tmp.path().join("install-root");
        let package_path = tmp.path().join("package.tar.zst");

        let mut packer = ArchivePacker::new(3).expect("archive packer should be created");
        packer
            .add_buffer("youpark", b"#!/bin/sh\necho ok\n", 0o755)
            .expect("main executable should be added");
        packer
            .add_buffer(".surge/surge.yml", b"schema: 1\n", 0o644)
            .expect("manifest should be added");
        let package_bytes = packer.finalize().expect("archive should be finalized");
        std::fs::write(&package_path, package_bytes).expect("archive should be written");

        let mut entry = release("1.2.3", "test", "linux-x64-cuda", "youpark-full.tar.zst");
        entry.main_exe = "youpark".to_string();
        entry.install_directory = "youpark".to_string();
        entry.shortcuts = Vec::new();

        let profile = release_install_profile("youpark", &entry);
        core_install::install_package_locally_at_root(&profile, &package_path, &install_root)
            .expect("local install should succeed");

        assert!(install_root.join("app").join("youpark").is_file());
        assert!(install_root.join("app").join(".surge").join("surge.yml").is_file());
        assert!(!install_root.join(".surge-app-next").exists());
        assert!(!install_root.join(".surge-app-prev").exists());
    }

    #[test]
    fn install_package_locally_replaces_existing_app_directory() {
        let tmp = tempfile::tempdir().expect("temp dir should exist");
        let install_root = tmp.path().join("install-root");
        let existing_app_dir = install_root.join("app");
        std::fs::create_dir_all(&existing_app_dir).expect("existing app dir should exist");
        std::fs::write(existing_app_dir.join("old.txt"), b"old").expect("old file should be written");

        let package_path = tmp.path().join("package.tar.zst");
        let mut packer = ArchivePacker::new(3).expect("archive packer should be created");
        packer
            .add_buffer("new.txt", b"new", 0o644)
            .expect("new payload should be added");
        let package_bytes = packer.finalize().expect("archive should be finalized");
        std::fs::write(&package_path, package_bytes).expect("archive should be written");

        let mut entry = release("1.2.3", "test", "linux-x64-cuda", "youpark-full.tar.zst");
        entry.main_exe = "youpark".to_string();
        entry.install_directory = "youpark".to_string();
        entry.shortcuts = Vec::new();

        let profile = release_install_profile("youpark", &entry);
        core_install::install_package_locally_at_root(&profile, &package_path, &install_root)
            .expect("local install should succeed");

        assert!(install_root.join("app").join("new.txt").is_file());
        assert!(!install_root.join("app").join("old.txt").exists());
        assert!(!install_root.join(".surge-app-prev").exists());
    }

    #[tokio::test]
    async fn download_release_archive_reconstructs_missing_full_from_deltas() {
        let tmp = tempfile::tempdir().expect("temp dir should exist");
        let backend = FilesystemBackend::new(tmp.path().to_str().expect("temp path should be utf-8"), "");

        let full_v1 = b"payload-v1".to_vec();
        let full_v2 = b"payload-v2".to_vec();
        let patch_v2 = bsdiff_buffers(&full_v1, &full_v2).expect("patch should build");
        let delta_v2 = zstd::encode_all(patch_v2.as_slice(), 3).expect("delta should encode");

        let mut v1 = release("1.0.0", "test", "linux-x64", "demo-1.0.0-linux-x64-full.tar.zst");
        v1.full_sha256 = sha256_hex(&full_v1);
        v1.set_primary_delta(None);

        let mut v2 = release("1.1.0", "test", "linux-x64", "demo-1.1.0-linux-x64-full.tar.zst");
        v2.full_sha256 = sha256_hex(&full_v2);
        v2.set_primary_delta(Some(DeltaArtifact::bsdiff_zstd(
            "primary",
            "1.0.0",
            "demo-1.1.0-linux-x64-delta.tar.zst",
            delta_v2.len() as i64,
            &sha256_hex(&delta_v2),
        )));

        backend
            .put_object(&v1.full_filename, &full_v1, "application/octet-stream")
            .await
            .expect("v1 full should upload");
        let v2_delta = v2
            .selected_delta()
            .expect("v2 should include descriptor delta")
            .filename;
        backend
            .put_object(&v2_delta, &delta_v2, "application/octet-stream")
            .await
            .expect("v2 delta should upload");

        let index = ReleaseIndex {
            app_id: "demo".to_string(),
            releases: vec![v1.clone(), v2.clone()],
            ..ReleaseIndex::default()
        };

        let destination = tmp.path().join("downloaded-full.tar.zst");
        let rebuilt = download_release_archive(
            &backend,
            &index,
            &v2,
            &[String::from("linux-x64")],
            &v2.full_filename,
            &destination,
        )
        .await
        .expect("fallback restore should succeed");

        assert!(rebuilt, "archive should be reconstructed from deltas");
        assert_eq!(
            std::fs::read(destination).expect("rebuilt archive should be readable"),
            full_v2
        );
    }

    fn load_reference_manifest_bytes() -> Vec<u8> {
        let source = Path::new("/home/peters/github/youpark/.snapx/snapx.yml");
        if source.is_file() {
            std::fs::read(source).expect("failed to read /home/peters/github/youpark/.snapx/snapx.yml")
        } else {
            br"schema: 2
channels:
  - name: test
  - name: production
apps:
  - id: quasar-ubuntu24.04-linux-x64-cpu
    channels: [test, production]
    target:
      rid: linux-x64
  - id: quasar-ubuntu24.04-linux-x64-cuda
    channels: [test, production]
    target:
      rid: linux-x64
  - id: quasar-jetpack4.6-linux-arm64
    channels: [test, production]
    target:
      rid: linux-arm64
  - id: quasar-jetpack5.0-linux-arm64
    channels: [test, production]
    target:
      rid: linux-arm64
  - id: quasar-jetpack5.1-linux-arm64
    channels: [test, production]
    target:
      rid: linux-arm64
"
            .to_vec()
        }
    }

    #[test]
    fn parse_profile_output_crlf() {
        let raw = "os=Windows\r\narch=AMD64\r\ngpu=none\r\n";
        let profile = parse_remote_profile(raw).expect("profile should parse");
        assert_eq!(profile.os, "Windows");
        assert_eq!(profile.arch, "AMD64");
        assert!(!profile.has_nvidia_gpu());
        assert_eq!(derive_base_rid(&profile), Some("win-x64".to_string()));
    }

    #[test]
    fn derive_rid_candidates_gpu() {
        let profile = RuntimeProfile {
            os: "Linux".to_string(),
            arch: "amd64".to_string(),
            gpu: "nvidia".to_string(),
        };
        let base = derive_base_rid(&profile).expect("base rid should resolve");
        let candidates = build_rid_candidates(&base, true);
        assert!(candidates.contains(&"linux-x64-nvidia".to_string()));
        assert!(candidates.contains(&"linux-x64-cuda".to_string()));
        assert!(candidates.contains(&"linux-x64-gpu".to_string()));
        assert!(candidates.contains(&"linux-x64".to_string()));
    }

    #[test]
    fn derive_rid_candidates_cover_youpark_variants() {
        let x64_cpu = build_rid_candidates("linux-x64", false);
        assert!(x64_cpu.contains(&"linux-x64".to_string()));
        assert!(x64_cpu.contains(&"linux-x64-cpu".to_string()));

        let x64_gpu = build_rid_candidates("linux-x64", true);
        assert!(x64_gpu.contains(&"linux-x64-cuda".to_string()));

        let arm64 = build_rid_candidates("linux-arm64", true);
        assert!(arm64.contains(&"linux-arm64".to_string()));
    }

    #[test]
    fn derive_rid_candidates_cover_reference_manifest_targets() {
        let manifest = SurgeManifest::parse(&load_reference_manifest_bytes()).expect("manifest should parse");
        let mut rids = manifest
            .app_ids()
            .into_iter()
            .flat_map(|app_id| manifest.target_rids(&app_id))
            .collect::<Vec<_>>();
        rids.sort();
        rids.dedup();

        assert!(rids.contains(&"linux-x64".to_string()));
        assert!(rids.contains(&"linux-arm64".to_string()));

        let cpu_candidates = build_rid_candidates("linux-x64", false);
        let gpu_candidates = build_rid_candidates("linux-x64", true);
        let arm_candidates = build_rid_candidates("linux-arm64", true);

        assert!(cpu_candidates.contains(&"linux-x64-cpu".to_string()));
        assert!(gpu_candidates.contains(&"linux-x64-cuda".to_string()));
        assert!(arm_candidates.contains(&"linux-arm64".to_string()));
    }

    #[test]
    fn select_release_prefers_first_matching_candidate() {
        let releases = vec![
            release("1.1.0", "stable", "linux-x64", "cpu-1.1.0"),
            release("1.0.0", "stable", "linux-x64-gpu", "gpu-1.0.0"),
            release("1.2.0", "stable", "", "generic-1.2.0"),
        ];

        let candidates = vec![
            "linux-x64-gpu".to_string(),
            "linux-x64".to_string(),
            "linux-x64-cpu".to_string(),
        ];

        let selected = select_release(&releases, "stable", None, &candidates).expect("release should resolve");
        assert_eq!(selected.full_filename, "gpu-1.0.0");
    }

    #[test]
    fn select_release_falls_back_to_generic() {
        let releases = vec![release("1.3.0", "stable", "", "generic-1.3.0")];
        let candidates = vec!["linux-arm64".to_string()];

        let selected = select_release(&releases, "stable", None, &candidates).expect("release should resolve");
        assert_eq!(selected.full_filename, "generic-1.3.0");
    }

    #[test]
    fn select_release_supports_youpark_style_cpu_cuda_variants() {
        let releases = vec![
            release("1.0.0", "production", "linux-x64-cpu", "cpu"),
            release("1.0.0", "production", "linux-x64-cuda", "cuda"),
            release("1.0.0", "production", "linux-arm64", "arm"),
        ];

        let gpu_candidates = build_rid_candidates("linux-x64", true);
        let gpu = select_release(&releases, "production", None, &gpu_candidates).expect("gpu release should resolve");
        assert_eq!(gpu.full_filename, "cuda");

        let cpu_candidates = build_rid_candidates("linux-x64", false);
        let cpu = select_release(&releases, "production", None, &cpu_candidates).expect("cpu release should resolve");
        assert_eq!(cpu.full_filename, "cpu");
    }
}
