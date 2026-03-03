use std::path::Path;

use tokio::process::Command;

use surge_core::config::constants::RELEASES_FILE_COMPRESSED;
use surge_core::config::manifest::SurgeManifest;
use surge_core::context::{Context, StorageConfig, StorageProvider};
use surge_core::error::{Result, SurgeError};
use surge_core::releases::manifest::{ReleaseEntry, ReleaseIndex, decompress_release_index};
use surge_core::releases::version::compare_versions;
use surge_core::storage::{self, StorageBackend};

#[derive(Debug, Clone, PartialEq, Eq)]
struct RemoteProfile {
    os: String,
    arch: String,
    gpu: String,
}

impl RemoteProfile {
    fn has_nvidia_gpu(&self) -> bool {
        let gpu = self.gpu.trim().to_ascii_lowercase();
        gpu == "nvidia" || gpu == "true" || gpu == "yes"
    }
}

pub async fn install_execute(
    manifest_path: &Path,
    node: &str,
    app_id: Option<&str>,
    channel: &str,
    rid: Option<&str>,
    version: Option<&str>,
    plan_only: bool,
    download_dir: &Path,
) -> Result<()> {
    let manifest = SurgeManifest::from_file(manifest_path)?;
    let app_id = super::resolve_app_id(&manifest, app_id)?;

    let (rid_candidates, profile) = if let Some(requested_rid) = rid.map(str::trim).filter(|r| !r.is_empty()) {
        (vec![requested_rid.to_string()], None)
    } else {
        let detected = detect_remote_profile(node).await?;
        let base_rid = derive_base_rid(&detected).ok_or_else(|| {
            SurgeError::Platform(format!(
                "Unable to map remote profile to a RID (os='{}', arch='{}'). Use --rid to override.",
                detected.os, detected.arch
            ))
        })?;
        (
            build_rid_candidates(&base_rid, detected.has_nvidia_gpu()),
            Some(detected),
        )
    };

    let storage_config = build_storage_config(&manifest)?;
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

    if let Some(profile) = &profile {
        println!(
            "Remote profile for {node}: os={}, arch={}, gpu={}",
            profile.os, profile.arch, profile.gpu
        );
    }
    println!("RID candidates: {}", rid_candidates.join(", "));
    println!(
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
    );

    let full_filename = release.full_filename.trim();
    if full_filename.is_empty() {
        return Err(SurgeError::NotFound(format!(
            "Release {} ({selected_rid}) has no full package filename",
            release.version
        )));
    }

    if plan_only {
        println!("Plan only mode: no transfer performed. Remove --plan-only to download and copy package to {node}.");
        return Ok(());
    }

    std::fs::create_dir_all(download_dir)?;
    let local_package = download_dir.join(Path::new(full_filename).file_name().unwrap_or_default());
    tracing::info!(
        "Downloading package '{}' to '{}'",
        full_filename,
        local_package.display()
    );
    backend.download_to_file(full_filename, &local_package, None).await?;

    copy_file_to_tailscale_node(node, &local_package).await?;
    println!(
        "Copied '{}' to node '{}' via tailscale file sharing.",
        local_package.display(),
        node
    );
    println!(
        "Install hint on node {}: extract '{}' into the install directory for app '{}'.",
        node,
        Path::new(full_filename).display(),
        app_id
    );

    Ok(())
}

async fn fetch_release_index(backend: &dyn StorageBackend) -> Result<ReleaseIndex> {
    match backend.get_object(RELEASES_FILE_COMPRESSED).await {
        Ok(data) => decompress_release_index(&data),
        Err(SurgeError::NotFound(_)) => Ok(ReleaseIndex::default()),
        Err(e) => Err(e),
    }
}

fn build_storage_config(manifest: &SurgeManifest) -> Result<StorageConfig> {
    let provider = match manifest.storage.provider.to_lowercase().as_str() {
        "s3" => StorageProvider::S3,
        "azure" => StorageProvider::AzureBlob,
        "gcs" => StorageProvider::Gcs,
        "filesystem" => StorageProvider::Filesystem,
        "github" | "github_releases" | "github-releases" => StorageProvider::GitHubReleases,
        other => return Err(SurgeError::Config(format!("Unknown storage provider: {other}"))),
    };

    let ctx = Context::new();
    ctx.set_storage(
        provider,
        &manifest.storage.bucket,
        &manifest.storage.region,
        "",
        "",
        &manifest.storage.endpoint,
    );
    let mut cfg = ctx.storage_config();
    cfg.prefix.clone_from(&manifest.storage.prefix);
    Ok(cfg)
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

async fn detect_remote_profile(node: &str) -> Result<RemoteProfile> {
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

fn parse_remote_profile(raw: &str) -> Option<RemoteProfile> {
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

    Some(RemoteProfile { os, arch, gpu })
}

fn derive_base_rid(profile: &RemoteProfile) -> Option<String> {
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
    use surge_core::config::manifest::ShortcutLocation;
    use surge_core::config::manifest::SurgeManifest;

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
            delta_filename: String::new(),
            delta_size: 0,
            delta_sha256: String::new(),
            created_utc: String::new(),
            release_notes: String::new(),
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
        let profile = RemoteProfile {
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
