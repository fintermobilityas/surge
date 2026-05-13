use super::activation::{
    build_remote_app_copy_activation_script, remote_linux_shortcut_icon_path, stage_remote_linux_shortcuts,
};
use super::execution::{
    detect_remote_home_directory, run_tailscale_capture, run_tailscale_streaming,
    run_tailscale_streaming_with_status_watchdog, stream_directory_to_tailscale_node_with_command,
};
use super::published_installer::build_remote_runtime_environment;
use super::state::{check_remote_staged_payload_identity, remote_staged_payload_identity};
use super::types::RemoteLaunchEnvironment;
use super::{
    ArchiveAcquisition, Path, PathBuf, ReleaseEntry, ReleaseIndex, RemoteSetupWatchdog, Result, StorageBackend,
    SurgeError, cache_path_for_key, core_install, download_release_archive, logline, release_install_profile,
    release_runtime_manifest_metadata, shell_single_quote,
};

pub(super) fn remote_install_root(home: &Path, app_id: &str, install_directory: &str) -> Result<PathBuf> {
    let name = if install_directory.trim().is_empty() {
        app_id.trim()
    } else {
        install_directory.trim()
    };
    if name.is_empty() {
        return Err(SurgeError::Config(
            "App id or install directory is required for remote install".to_string(),
        ));
    }

    let candidate = Path::new(name);
    if candidate.is_absolute() {
        Ok(candidate.to_path_buf())
    } else {
        Ok(home.join(".local/share").join(candidate))
    }
}

pub(crate) fn select_latest_remote_legacy_app_dir<I, S>(install_root: &Path, entries: I) -> Option<PathBuf>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut best: Option<(String, PathBuf)> = None;

    for entry in entries {
        let name = entry.as_ref().trim();
        let Some(version) = remote_legacy_snapshot_version(name) else {
            continue;
        };

        if best.as_ref().is_none_or(|(best_version, _)| {
            super::compare_versions(version, best_version) == std::cmp::Ordering::Greater
        }) {
            best = Some((version.to_string(), install_root.join(name)));
        }
    }

    best.map(|(_, path)| path)
}

fn remote_legacy_snapshot_version(dir_name: &str) -> Option<&str> {
    let version = dir_name.strip_prefix("app-")?;
    if version.is_empty() || !version.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        return None;
    }
    Some(version)
}

async fn detect_remote_legacy_app_dir(ssh_node: &str, install_root: &Path) -> Result<Option<PathBuf>> {
    let probe = format!(
        "install_root={}; \
if [ -d \"$install_root\" ]; then \
  for path in \"$install_root\"/app-[0-9]*; do \
    if [ -d \"$path\" ]; then \
      basename \"$path\"; \
    fi; \
  done; \
fi",
        shell_single_quote(&install_root.to_string_lossy()),
    );
    let command = format!("sh -c {}", shell_single_quote(&probe));
    let output = run_tailscale_capture(&["ssh", ssh_node, command.as_str()]).await?;

    Ok(select_latest_remote_legacy_app_dir(
        install_root,
        output.lines().map(str::trim).filter(|line| !line.is_empty()),
    ))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn deploy_remote_app_copy_for_tailscale(
    backend: &dyn StorageBackend,
    index: &ReleaseIndex,
    download_dir: &Path,
    ssh_target: &str,
    file_target: &str,
    app_id: &str,
    _rid: &str,
    release: &ReleaseEntry,
    channel: &str,
    storage_config: &surge_core::context::StorageConfig,
    launch_env: &RemoteLaunchEnvironment,
    rid_candidates: &[String],
    full_filename: &str,
    no_start: bool,
    stage: bool,
) -> Result<()> {
    let remote_home = detect_remote_home_directory(ssh_target).await?;
    let install_root = remote_install_root(&remote_home, app_id, &release.install_directory)?;
    let active_app_dir = install_root.join("app");
    let runtime_environment = build_remote_runtime_environment(release, launch_env);
    let staged_payload_identity = remote_staged_payload_identity(app_id, release, channel, storage_config);
    let main_exe_name = if release.main_exe.trim().is_empty() {
        app_id
    } else {
        release.main_exe.trim()
    };

    if !stage
        && let Some(remote_staged_payload) = check_remote_staged_payload_identity(ssh_target, &install_root).await
        && remote_staged_payload == staged_payload_identity
    {
        logline::success(&format!(
            "Using pre-staged payload for '{app_id}' v{} on '{file_target}'.",
            release.version
        ));
        stop_remote_supervisor_if_running(ssh_target, &install_root, &release.supervisor_id).await?;
        let legacy_app_dir = if release.persistent_assets.is_empty() {
            None
        } else {
            detect_remote_legacy_app_dir(ssh_target, &install_root).await?
        };
        let activation_script = build_remote_app_copy_activation_script(
            &install_root,
            main_exe_name,
            &release.version,
            &runtime_environment,
            &release.persistent_assets,
            legacy_app_dir.as_deref(),
            no_start,
        )?;
        let ssh_command = format!("sh -lc {}", shell_single_quote(&activation_script));
        logline::info(&format!("Activating pre-staged install on '{file_target}'..."));
        return run_tailscale_streaming(&["ssh", ssh_target, ssh_command.as_str()], "remote").await;
    }

    std::fs::create_dir_all(download_dir)?;
    let local_package = download_dir.join(Path::new(full_filename).file_name().unwrap_or_default());
    let acquisition =
        download_release_archive(backend, index, release, rid_candidates, full_filename, &local_package).await?;
    match acquisition {
        ArchiveAcquisition::ReusedLocal => {
            logline::success(&format!(
                "Using cached package '{}' at '{}'.",
                Path::new(full_filename).display(),
                local_package.display()
            ));
        }
        ArchiveAcquisition::Downloaded => {
            logline::success(&format!(
                "Downloaded '{}' to '{}'.",
                Path::new(full_filename).display(),
                local_package.display()
            ));
        }
        ArchiveAcquisition::Reconstructed => {
            logline::warn(&format!(
                "Direct full package '{}' missing in backend; reconstructed from retained release artifacts.",
                Path::new(full_filename).display()
            ));
        }
    }

    let staging_dir =
        tempfile::tempdir().map_err(|e| SurgeError::Platform(format!("Failed to create staging directory: {e}")))?;
    let stage_root = staging_dir.path().join("remote-stage");
    let stage_app_dir = stage_root.join("app");
    surge_core::archive::extractor::extract_file_to(&local_package, &stage_app_dir)?;

    let install_profile = release_install_profile(app_id, release);
    let runtime_manifest = release_runtime_manifest_metadata(release, channel, storage_config);
    core_install::write_runtime_manifest(&stage_app_dir, &install_profile, &runtime_manifest)?;
    std::fs::write(
        stage_root.join(".surge-staged-release.json"),
        serde_json::to_vec(&staged_payload_identity)
            .map_err(|e| SurgeError::Config(format!("Failed to serialize remote staged payload identity: {e}")))?,
    )?;
    let legacy_app_dir = if release.persistent_assets.is_empty() {
        None
    } else {
        detect_remote_legacy_app_dir(ssh_target, &install_root).await?
    };

    if !release.shortcuts.is_empty() {
        let icon_path =
            remote_linux_shortcut_icon_path(&stage_app_dir, &active_app_dir, app_id, main_exe_name, &release.icon);
        let rendered = surge_core::platform::shortcuts::render_linux_shortcut_files(
            app_id,
            release.display_name(app_id),
            &active_app_dir.join(main_exe_name),
            &icon_path,
            &release.supervisor_id,
            &install_root,
            &release.shortcuts,
            &runtime_environment,
        );
        stage_remote_linux_shortcuts(&stage_root, &rendered)?;
    }

    let transfer_command = format!(
        "command -v tar >/dev/null 2>&1 || {{ echo 'Remote host is missing tar' >&2; exit 1; }}; \
install_root={}; stage_dir=\"$install_root/.surge-transfer-stage\"; \
mkdir -p \"$install_root\"; rm -rf \"$stage_dir\"; mkdir -p \"$stage_dir\"; tar -C \"$stage_dir\" -xf -",
        shell_single_quote(&install_root.to_string_lossy())
    );
    logline::info(&format!(
        "Streaming extracted app payload to '{file_target}' for host-mismatch remote deployment..."
    ));
    stream_directory_to_tailscale_node_with_command(ssh_target, &stage_root, &transfer_command).await?;

    if stage {
        return Ok(());
    }

    stop_remote_supervisor_if_running(ssh_target, &install_root, &release.supervisor_id).await?;
    let activation_script = build_remote_app_copy_activation_script(
        &install_root,
        main_exe_name,
        &release.version,
        &runtime_environment,
        &release.persistent_assets,
        legacy_app_dir.as_deref(),
        no_start,
    )?;
    let ssh_command = format!("sh -lc {}", shell_single_quote(&activation_script));
    logline::info(&format!("Activating remote install on '{file_target}'..."));
    run_tailscale_streaming(&["ssh", ssh_target, ssh_command.as_str()], "remote").await
}

pub(crate) async fn run_remote_staged_installer_setup(
    ssh_node: &str,
    file_target: &str,
    app_id: &str,
    release: &ReleaseEntry,
    no_start: bool,
) -> Result<()> {
    let remote_home = detect_remote_home_directory(ssh_node).await?;
    let install_root = remote_install_root(&remote_home, app_id, &release.install_directory)?;
    let setup_command = build_remote_staged_installer_setup_command(&install_root, no_start);
    let ssh_command = format!("sh -lc {}", shell_single_quote(&setup_command));
    logline::info(&format!(
        "Using pre-staged installer cache for '{app_id}' v{} on '{file_target}'.",
        release.version
    ));
    let watchdog = RemoteSetupWatchdog::new(ssh_node, &install_root);
    run_tailscale_streaming_with_status_watchdog(&["ssh", ssh_node, ssh_command.as_str()], "remote", watchdog).await
}

pub(crate) async fn warn_if_remote_stage_cleanup_fails(ssh_node: &str, app_id: &str, release: &ReleaseEntry) {
    if let Err(error) = cleanup_remote_staged_payload(ssh_node, app_id, release).await {
        logline::warn(&format!("Could not remove stale remote staged payload: {error}"));
    }
}

pub(crate) async fn cleanup_remote_staged_payload(ssh_node: &str, app_id: &str, release: &ReleaseEntry) -> Result<()> {
    let remote_home = detect_remote_home_directory(ssh_node).await?;
    let install_root = remote_install_root(&remote_home, app_id, &release.install_directory)?;
    let cleanup_command = build_remote_stage_cleanup_command(&install_root);
    let ssh_command = format!("sh -lc {}", shell_single_quote(&cleanup_command));
    run_tailscale_streaming(&["ssh", ssh_node, ssh_command.as_str()], "remote").await
}

pub(crate) async fn stop_remote_supervisor_if_running(
    ssh_node: &str,
    install_root: &Path,
    supervisor_id: &str,
) -> Result<()> {
    let Some(stop_command) = build_remote_stop_supervisor_command(install_root, supervisor_id) else {
        return Ok(());
    };

    logline::info(&format!(
        "Stopping remote supervisor '{}' before activation...",
        supervisor_id.trim()
    ));
    let ssh_command = format!("sh -lc {}", shell_single_quote(&stop_command));
    run_tailscale_streaming(&["ssh", ssh_node, ssh_command.as_str()], "remote").await
}

pub(crate) fn build_remote_stage_cleanup_command(install_root: &Path) -> String {
    format!(
        "install_root={}; rm -rf \"$install_root/.surge-transfer-stage\"",
        shell_single_quote(&install_root.to_string_lossy())
    )
}

pub(crate) async fn remote_staged_app_copy_files_exist(ssh_node: &str, install_root: &Path) -> Result<bool> {
    let stage_root = install_root.join(".surge-transfer-stage");
    let marker = stage_root.join(".surge-staged-release.json");
    let app_dir = stage_root.join("app");
    remote_paths_exist(ssh_node, &[app_dir.as_path()], &[marker.as_path()]).await
}

pub(crate) async fn remote_staged_installer_cache_files_exist(
    ssh_node: &str,
    install_root: &Path,
    release: &ReleaseEntry,
) -> Result<bool> {
    let stage_dir = install_root.join(".surge-cache").join("staged-installer");
    let marker = stage_dir.join(".surge-staged-release.json");
    let installer_manifest = stage_dir.join("installer.yml");
    let surge_bin = stage_dir.join("surge");
    let artifact_cache_dir = install_root.join(".surge-cache").join("artifacts");
    let cached_package = cache_path_for_key(&artifact_cache_dir, release.full_filename.trim())?;
    remote_paths_exist(
        ssh_node,
        &[stage_dir.as_path()],
        &[
            marker.as_path(),
            installer_manifest.as_path(),
            surge_bin.as_path(),
            cached_package.as_path(),
        ],
    )
    .await
}

async fn remote_paths_exist(ssh_node: &str, required_dirs: &[&Path], required_files: &[&Path]) -> Result<bool> {
    let probe = build_remote_paths_exist_probe(required_dirs, required_files);
    let command = format!("sh -c {}", shell_single_quote(&probe));
    Ok(run_tailscale_capture(&["ssh", ssh_node, command.as_str()])
        .await?
        .trim()
        == "ready")
}

pub(crate) fn build_remote_paths_exist_probe(required_dirs: &[&Path], required_files: &[&Path]) -> String {
    let mut checks = Vec::new();
    for path in required_dirs {
        checks.push(format!("[ -d {} ]", shell_single_quote(&path.to_string_lossy())));
    }
    for path in required_files {
        checks.push(format!("[ -f {} ]", shell_single_quote(&path.to_string_lossy())));
    }
    if checks.is_empty() {
        "printf 'ready'".to_string()
    } else {
        format!(
            "if {}; then printf 'ready'; else printf 'missing'; fi",
            checks.join(" && ")
        )
    }
}

pub(crate) fn build_remote_stop_supervisor_command(install_root: &Path, supervisor_id: &str) -> Option<String> {
    let supervisor_id = supervisor_id.trim();
    if supervisor_id.is_empty() {
        return None;
    }

    Some(format!(
        "install_root={}; supervisor_id={}; pid_file=\"$install_root/.surge-supervisor-$supervisor_id.pid\"; \
if [ ! -d \"$install_root\" ] || [ ! -f \"$pid_file\" ]; then exit 0; fi; \
pid=\"$(tr -d '[:space:]' < \"$pid_file\")\"; \
case \"$pid\" in ''|*[!0-9]*) echo \"Invalid PID in supervisor PID file: $pid_file\" >&2; exit 1 ;; esac; \
pid_stat() {{ if command -v ps >/dev/null 2>&1; then ps -o stat= -p \"$pid\" 2>/dev/null | tr -d '[:space:]' || true; elif kill -0 \"$pid\" 2>/dev/null; then printf R; fi; }}; \
clear_if_stale() {{ stat=\"$(pid_stat)\"; if [ -z \"$stat\" ] || [ \"${{stat#Z}}\" != \"$stat\" ]; then rm -f \"$pid_file\"; exit 0; fi; }}; \
clear_if_stale; \
if ! kill \"$pid\" 2>/dev/null; then clear_if_stale; echo \"Failed to stop supervisor '$supervisor_id' (pid $pid)\" >&2; exit 1; fi; \
i=0; \
while [ -f \"$pid_file\" ]; do \
  clear_if_stale; \
  if [ \"$i\" -ge 200 ]; then \
    kill -KILL \"$pid\" 2>/dev/null || true; \
    sleep 0.5; \
    clear_if_stale; \
    echo \"Timed out waiting for supervisor '$supervisor_id' to exit\" >&2; exit 1; \
  fi; \
  sleep 0.1; \
  i=$((i + 1)); \
done",
        shell_single_quote(&install_root.to_string_lossy()),
        shell_single_quote(supervisor_id)
    ))
}

pub(crate) fn build_remote_staged_installer_setup_command(install_root: &Path, no_start: bool) -> String {
    let no_start_flag = if no_start { " --no-start" } else { "" };
    format!(
        "install_root={}; \
stage_dir=\"$install_root/.surge-cache/staged-installer\"; \
surge_bin=\"$stage_dir/surge\"; \
if [ ! -d \"$stage_dir\" ] || [ ! -f \"$stage_dir/installer.yml\" ] || [ ! -f \"$surge_bin\" ]; then \
  echo \"Remote staged installer cache is missing required files\" >&2; \
  exit 1; \
fi; \
chmod +x \"$surge_bin\" || true; \
cd \"$stage_dir\"; \
\"$surge_bin\" setup \"$stage_dir\"{no_start_flag}",
        shell_single_quote(&install_root.to_string_lossy())
    )
}
