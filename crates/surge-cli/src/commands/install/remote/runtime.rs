use std::collections::BTreeMap;
use std::path::Path;

use super::{
    ReleaseEntry, Result, SurgeError, activation, check_remote_install_state, execution, logline, published_installer,
    run_tailscale_streaming, shell_single_quote, staging, types,
};

pub(crate) async fn converge_current_remote_runtime(
    ssh_target: &str,
    file_target: &str,
    app_id: &str,
    release: &ReleaseEntry,
    launch_env: &types::RemoteLaunchEnvironment,
) -> Result<()> {
    logline::info(&format!(
        "'{app_id}' v{} is package-current on '{file_target}'; verifying active runtime because --force was supplied.",
        release.version
    ));

    match verify_remote_started_process(ssh_target, file_target, app_id, release).await {
        Ok(()) => {
            logline::success(&format!(
                "Remote package-current/runtime-current proof confirmed on '{file_target}' for v{}.",
                release.version
            ));
            Ok(())
        }
        Err(error) => {
            logline::warn(&format!(
                "Remote runtime proof is missing on '{file_target}'; restarting current install: {error}"
            ));
            restart_current_remote_runtime(ssh_target, file_target, app_id, release, launch_env).await?;
            verify_remote_started_process(ssh_target, file_target, app_id, release).await?;
            logline::success(&format!(
                "Remote restart-performed/restart-confirmed proof succeeded on '{file_target}' for v{}.",
                release.version
            ));
            Ok(())
        }
    }
}

async fn restart_current_remote_runtime(
    ssh_target: &str,
    file_target: &str,
    app_id: &str,
    release: &ReleaseEntry,
    launch_env: &types::RemoteLaunchEnvironment,
) -> Result<()> {
    let remote_home = execution::detect_remote_home_directory(ssh_target).await?;
    let install_root = staging::remote_install_root(&remote_home, app_id, &release.install_directory)?;
    if !release.supervisor_id.trim().is_empty() {
        staging::stop_remote_supervisor_if_running(ssh_target, &install_root, &release.supervisor_id).await?;
    }

    let main_exe = if release.main_exe.trim().is_empty() {
        app_id
    } else {
        release.main_exe.trim()
    };
    let runtime_environment = published_installer::build_remote_runtime_environment(release, launch_env);
    let restart_command = build_remote_runtime_start_command(
        &install_root,
        main_exe,
        &release.supervisor_id,
        &release.version,
        &runtime_environment,
    );
    let ssh_command = format!("sh -lc {}", shell_single_quote(&restart_command));
    logline::info(&format!("Restarting remote runtime on '{file_target}'..."));
    run_tailscale_streaming(&["ssh", ssh_target, ssh_command.as_str()], "remote").await
}

pub(crate) fn build_remote_runtime_start_command(
    install_root: &Path,
    main_exe: &str,
    supervisor_id: &str,
    version: &str,
    environment: &BTreeMap<String, String>,
) -> String {
    let install_root = shell_single_quote(&install_root.to_string_lossy());
    let main_exe = shell_single_quote(main_exe);
    let supervisor_id = shell_single_quote(supervisor_id.trim());
    let version = shell_single_quote(version.trim());
    let exports = activation::shell_export_lines(environment);

    format!(
        "set -eu\n\
install_root={install_root}\n\
active_app_dir=\"$install_root/app\"\n\
main_exe={main_exe}\n\
supervisor_id={supervisor_id}\n\
version={version}\n\
active_exe=\"$active_app_dir/$main_exe\"\n\
{exports}\
if [ ! -f \"$active_exe\" ]; then\n\
  echo \"application executable missing at $active_exe\" >&2\n\
  exit 1\n\
fi\n\
if [ ! -x \"$active_exe\" ]; then\n\
  chmod +x \"$active_exe\" || true\n\
fi\n\
kill_matching() {{\n\
  pattern=\"$1\"\n\
  if ! command -v pgrep >/dev/null 2>&1; then\n\
    return 0\n\
  fi\n\
  for pid in $(pgrep -u \"$(id -u)\" -f \"$pattern\" 2>/dev/null || true); do\n\
    case \"$pid\" in \"$$\"|\"$PPID\") continue ;; esac\n\
    kill \"$pid\" 2>/dev/null || true\n\
  done\n\
}}\n\
kill_matching \"$active_exe\"\n\
kill_matching \"$install_root/app-\"\n\
kill_matching \"$install_root/app/\"\n\
cd \"$install_root\"\n\
if [ -n \"$supervisor_id\" ]; then\n\
  kill_matching \"surge-supervisor.*--id $supervisor_id\"\n\
  supervisor_bin=\"$active_app_dir/surge-supervisor\"\n\
  pid_file=\"$install_root/.surge-supervisor-$supervisor_id.pid\"\n\
  stop_file=\"$install_root/.surge-supervisor-$supervisor_id.stop\"\n\
  if [ ! -f \"$supervisor_bin\" ]; then\n\
    echo \"supervisor binary missing at $supervisor_bin\" >&2\n\
    exit 1\n\
  fi\n\
  if [ ! -x \"$supervisor_bin\" ]; then\n\
    chmod +x \"$supervisor_bin\" || true\n\
  fi\n\
  rm -f \"$stop_file\"\n\
  if [ -n \"$version\" ]; then\n\
    nohup \"$supervisor_bin\" run --id \"$supervisor_id\" --dir \"$install_root\" --exe \"$active_exe\" -- --surge-first-run \"$version\" >/dev/null 2>&1 &\n\
  else\n\
    nohup \"$supervisor_bin\" run --id \"$supervisor_id\" --dir \"$install_root\" --exe \"$active_exe\" -- --surge-first-run >/dev/null 2>&1 &\n\
  fi\n\
  i=0\n\
  while [ ! -f \"$pid_file\" ]; do\n\
    if [ \"$i\" -ge 50 ]; then\n\
      echo \"supervisor restart was not confirmed: $pid_file did not appear\" >&2\n\
      exit 1\n\
    fi\n\
    sleep 0.1\n\
    i=$((i + 1))\n\
  done\n\
  echo \"supervisor restart confirmed\"\n\
else\n\
  if [ -n \"$version\" ]; then\n\
    nohup \"$active_exe\" --surge-first-run \"$version\" >/dev/null 2>&1 &\n\
  else\n\
    nohup \"$active_exe\" --surge-first-run >/dev/null 2>&1 &\n\
  fi\n\
  echo \"application restart requested\"\n\
fi\n"
    )
}

pub(crate) async fn verify_remote_runtime_after_install(
    ssh_target: &str,
    file_target: &str,
    install_dir: &str,
    app_id: &str,
    release: &ReleaseEntry,
    channel: &str,
    storage_config: &surge_core::context::StorageConfig,
    verify_started_process: bool,
) -> Result<()> {
    let main_exe_name = if release.main_exe.trim().is_empty() {
        app_id
    } else {
        release.main_exe.trim()
    };
    let state = check_remote_install_state(ssh_target, install_dir, main_exe_name)
        .await?
        .ok_or_else(|| {
            SurgeError::Update(format!(
                "Remote runtime verification failed on '{file_target}': no runtime metadata found"
            ))
        })?;
    if state.app_identity_matches(app_id)
        && state.version.trim() == release.version.trim()
        && state.metadata_matches(channel, storage_config)
    {
        logline::success(&format!(
            "Verified remote runtime on '{file_target}': '{app_id}' v{} ({channel}).",
            release.version,
        ));
        if verify_started_process {
            verify_remote_started_process(ssh_target, file_target, app_id, release).await?;
        }
        return Ok(());
    }

    Err(SurgeError::Update(format!(
        "Remote runtime verification failed on '{file_target}': found app id {:?} v{} channel {:?}, expected '{app_id}' v{} channel '{}'.",
        state.app_id, state.version, state.channel, release.version, channel
    )))
}

async fn verify_remote_started_process(
    ssh_target: &str,
    file_target: &str,
    app_id: &str,
    release: &ReleaseEntry,
) -> Result<()> {
    let remote_home = execution::detect_remote_home_directory(ssh_target).await?;
    let install_root = staging::remote_install_root(&remote_home, app_id, &release.install_directory)?;
    let main_exe = if release.main_exe.trim().is_empty() {
        app_id
    } else {
        release.main_exe.trim()
    };
    let probe =
        build_remote_process_verification_probe(&install_root, main_exe, &release.supervisor_id, &release.version);
    let command = format!("sh -c {}", shell_single_quote(&probe));
    let mut last_result = String::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let output = execution::run_tailscale_capture(&["ssh", ssh_target, command.as_str()]).await?;
        let result = output.trim();
        if result == "ready" {
            logline::success(&format!(
                "Verified remote process on '{file_target}' for v{}.",
                release.version
            ));
            return Ok(());
        }
        last_result.clear();
        last_result.push_str(result);
        if std::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }

    Err(SurgeError::Update(format!(
        "Remote process verification failed on '{file_target}': {last_result}"
    )))
}

pub(crate) fn build_remote_process_verification_probe(
    install_root: &Path,
    main_exe: &str,
    supervisor_id: &str,
    version: &str,
) -> String {
    format!(
        r#"install_root={}; main_exe={}; supervisor_id={}; version={};
active_exe="$install_root/app/$main_exe"; status_file="$install_root/.surge-update-status.json"; app_seen=0; target_app_seen=0; stale_app_seen=0; supervisor_seen=0; stale_supervisor_seen=0; waiting_supervisor_seen=0; target_supervisor_seen=0; target_app_pids=""; watched_pids=""; status_converged=0;
if [ -r "$status_file" ]; then
  status_compact="$(tr -d '[:space:]' < "$status_file" 2>/dev/null || true)";
  status_has_state=0; status_has_installed=0; status_has_target=0;
  case "$status_compact" in *'"state":"converged"'*) status_has_state=1 ;; esac;
  case "$status_compact" in *'"installed_version":"'"$version"'"'*) status_has_installed=1 ;; esac;
  case "$status_compact" in *'"target_version":"'"$version"'"'*) status_has_target=1 ;; esac;
  if [ "$status_has_state" -eq 1 ] && [ "$status_has_installed" -eq 1 ] && [ "$status_has_target" -eq 1 ]; then status_converged=1; fi;
fi;
contains_target_first_run() {{ cmd_tokens=" $1 "; case "$cmd_tokens" in *" --surge-first-run $version "*|*" $version --surge-first-run "*) return 0 ;; esac; return 1; }}
contains_target_version_arg() {{ cmd_tokens=" $1 "; case "$cmd_tokens" in *" $version "*) return 0 ;; esac; return 1; }}
contains_target_proof() {{ contains_target_first_run "$1" || contains_target_version_arg "$1"; }}
process_exe_matches_active() {{ actual="$(readlink "/proc/$1/exe" 2>/dev/null || true)"; [ "$actual" = "$active_exe" ]; }}
extract_watched_pid() {{ case "$1" in *" watch "*" --pid "*) rest="${{1#* --pid }}"; watched_pid="${{rest%% *}}"; case "$watched_pid" in ""|*[!0-9]*) return 1 ;; esac; printf '%s\n' "$watched_pid"; return 0 ;; esac; return 1; }}
for cmdline in /proc/[0-9]*/cmdline; do
  [ -r "$cmdline" ] || continue;
  pid="${{cmdline%/cmdline}}"; pid="${{pid##*/}}";
  case "$pid" in "$$"|"$PPID") continue ;; esac;
  cmd="$(tr '\0' ' ' < "$cmdline" 2>/dev/null || true)";
  [ -n "$cmd" ] || continue;
  case "$cmd" in *"surge-supervisor"*) ;; *"$active_exe"*) app_seen=1; if contains_target_proof "$cmd" || {{ [ "$status_converged" -eq 1 ] && process_exe_matches_active "$pid"; }}; then target_app_seen=1; target_app_pids="${{target_app_pids}}${{pid}} "; else stale_app_seen=1; fi ;; esac;
  if [ -n "$supervisor_id" ]; then
    case "$cmd" in *"surge-supervisor"*"--id $supervisor_id"*)
      supervisor_seen=1;
      if watched_pid="$(extract_watched_pid "$cmd")"; then watched_pids="${{watched_pids}}${{watched_pid}} "; fi;
      case " $cmd " in *" --surge-first-run "*) if contains_target_first_run "$cmd"; then target_supervisor_seen=1; else stale_supervisor_seen=1; fi ;; esac
    ;; esac;
  fi;
done;
for watched_pid in $watched_pids; do
  case " $target_app_pids " in *" $watched_pid "*) target_supervisor_seen=1 ;; *) if kill -0 "$watched_pid" 2>/dev/null; then waiting_supervisor_seen=1; fi ;; esac;
done;
if [ "$target_app_seen" -ne 1 ]; then
  if [ "$app_seen" -eq 1 ]; then echo "app process for $active_exe is running without target proof for $version"; else echo "app process for $active_exe was not found"; fi;
  exit 0;
fi;
if [ "$stale_app_seen" -eq 1 ]; then echo "stale app process for $active_exe is still running without target proof for $version"; exit 0; fi;
if [ -n "$supervisor_id" ] && [ "$waiting_supervisor_seen" -eq 1 ]; then echo "supervisor process '$supervisor_id' is still waiting for the previous child"; exit 0; fi;
if [ -n "$supervisor_id" ] && [ "$stale_supervisor_seen" -eq 1 ]; then echo "supervisor process '$supervisor_id' is running with stale first-run proof"; exit 0; fi;
if [ -n "$supervisor_id" ] && [ "$supervisor_seen" -ne 1 ]; then echo "supervisor process '$supervisor_id' was not found"; exit 0; fi;
if [ -n "$supervisor_id" ] && [ "$target_supervisor_seen" -ne 1 ]; then echo "supervisor process '$supervisor_id' is not watching target app process for $version"; exit 0; fi;
echo ready"#,
        shell_single_quote(&install_root.to_string_lossy()),
        shell_single_quote(main_exe),
        shell_single_quote(supervisor_id.trim()),
        shell_single_quote(version.trim())
    )
}
