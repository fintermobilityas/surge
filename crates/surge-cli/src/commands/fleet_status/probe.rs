use std::collections::BTreeMap;
use std::process::Stdio;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::process::Command;

use crate::commands::install::shell_single_quote;

const PROBE_BEGIN: &str = "__SURGE_FLEET_STATUS_PROBE_BEGIN__";
const PROBE_END: &str = "__SURGE_FLEET_STATUS_PROBE_END__";
const RUNTIME_BEGIN: &str = "__SURGE_FLEET_STATUS_RUNTIME_BEGIN__";
const RUNTIME_END: &str = "__SURGE_FLEET_STATUS_RUNTIME_END__";
const UPDATE_BEGIN: &str = "__SURGE_FLEET_STATUS_UPDATE_BEGIN__";
const UPDATE_END: &str = "__SURGE_FLEET_STATUS_UPDATE_END__";

#[derive(Debug, Clone)]
pub(super) struct RemoteProbe {
    pub(super) missing: bool,
    pub(super) runtime_yaml: Option<String>,
    pub(super) update_status: Option<RemoteUpdateStatus>,
    pub(super) process: ProcessSummary,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct RemoteUpdateStatus {
    #[serde(default)]
    pub(super) state: String,
    #[serde(default)]
    pub(super) installed_version: String,
    #[serde(default)]
    pub(super) target_version: String,
    #[serde(default)]
    pub(super) channel: String,
    #[serde(default)]
    pub(super) app_id: String,
    pub(super) current_phase: Option<String>,
    pub(super) last_progress_at_utc: Option<String>,
    pub(super) reason: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct ProcessSummary {
    pub(super) proc_available: bool,
    #[serde(flatten)]
    pub(super) app: AppProcessSummary,
    #[serde(flatten)]
    pub(super) supervisor: SupervisorProcessSummary,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct AppProcessSummary {
    pub(super) app_process_running: bool,
    pub(super) target_app_process_running: bool,
    pub(super) stale_app_process_running: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct SupervisorProcessSummary {
    pub(super) supervisor_configured: bool,
    pub(super) supervisor_process_running: bool,
    #[serde(flatten)]
    pub(super) handoff: SupervisorHandoffSummary,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct SupervisorHandoffSummary {
    pub(super) supervisor_waiting_for_previous_child: bool,
    pub(super) stale_supervisor_process_running: bool,
}

impl ProcessSummary {
    pub(super) fn app_process_running(&self) -> bool {
        self.app.app_process_running
    }

    pub(super) fn target_app_process_running(&self) -> bool {
        self.app.target_app_process_running
    }

    pub(super) fn supervisor_configured(&self) -> bool {
        self.supervisor.supervisor_configured
    }

    pub(super) fn supervisor_process_running(&self) -> bool {
        self.supervisor.supervisor_process_running
    }

    pub(super) fn supervisor_waiting_for_previous_child(&self) -> bool {
        self.supervisor.handoff.supervisor_waiting_for_previous_child
    }

    pub(super) fn stale_supervisor_process_running(&self) -> bool {
        self.supervisor.handoff.stale_supervisor_process_running
    }
}

pub(super) async fn run_tailscale_capture_timeout(
    ssh_target: &str,
    remote_command: &str,
    timeout: Duration,
) -> std::result::Result<String, String> {
    let mut command = Command::new("tailscale");
    command
        .args(["ssh", ssh_target, remote_command])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let output = match tokio::time::timeout(timeout, command.output()).await {
        Ok(Ok(output)) => output,
        Ok(Err(error)) => return Err(format!("failed to run tailscale command: {error}")),
        Err(_) => {
            return Err(format!(
                "timed out after {}s running tailscale ssh {}",
                timeout.as_secs(),
                ssh_target
            ));
        }
    };

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        if stderr.is_empty() {
            return Err(format!("tailscale ssh {ssh_target} exited with {}", output.status));
        }
        return Err(stderr);
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

pub(super) fn parse_remote_probe(output: &str) -> std::result::Result<RemoteProbe, String> {
    let probe =
        section_between(output, PROBE_BEGIN, PROBE_END).ok_or_else(|| "missing probe section markers".to_string())?;
    let values = parse_key_values(&probe);
    let missing = parse_bool(values.get("missing").map(String::as_str));
    let runtime_yaml = section_between(output, RUNTIME_BEGIN, RUNTIME_END);
    let update_status = parse_update_status(section_between(output, UPDATE_BEGIN, UPDATE_END).as_deref())?;
    Ok(RemoteProbe {
        missing,
        runtime_yaml,
        update_status,
        process: ProcessSummary {
            proc_available: parse_bool(values.get("proc_available").map(String::as_str)),
            app: AppProcessSummary {
                app_process_running: parse_bool(values.get("app_process_running").map(String::as_str)),
                target_app_process_running: parse_bool(values.get("target_app_process_running").map(String::as_str)),
                stale_app_process_running: parse_bool(values.get("stale_app_process_running").map(String::as_str)),
            },
            supervisor: SupervisorProcessSummary {
                supervisor_configured: parse_bool(values.get("supervisor_configured").map(String::as_str)),
                supervisor_process_running: parse_bool(values.get("supervisor_process_running").map(String::as_str)),
                handoff: SupervisorHandoffSummary {
                    supervisor_waiting_for_previous_child: parse_bool(
                        values.get("supervisor_waiting_for_previous_child").map(String::as_str),
                    ),
                    stale_supervisor_process_running: parse_bool(
                        values.get("stale_supervisor_process_running").map(String::as_str),
                    ),
                },
            },
        },
    })
}

fn parse_update_status(raw: Option<&str>) -> std::result::Result<Option<RemoteUpdateStatus>, String> {
    let Some(raw) = raw.map(str::trim).filter(|raw| !raw.is_empty()) else {
        return Ok(None);
    };
    serde_json::from_str(raw).map(Some).map_err(|error| error.to_string())
}

fn parse_key_values(raw: &str) -> BTreeMap<String, String> {
    raw.lines()
        .filter_map(|line| {
            let (key, value) = line.split_once('=')?;
            Some((key.trim().to_string(), value.trim().to_string()))
        })
        .collect()
}

fn parse_bool(value: Option<&str>) -> bool {
    matches!(value.map(str::trim), Some("true" | "1" | "yes"))
}

fn section_between(output: &str, begin: &str, end: &str) -> Option<String> {
    let (_, after_begin) = output.split_once(begin)?;
    let (section, _) = after_begin.split_once(end)?;
    Some(section.trim_matches('\n').to_string())
}

pub(super) fn build_remote_probe_script(app_id: &str, target_version: &str) -> String {
    let app_id = shell_single_quote(app_id);
    let target_version = shell_single_quote(target_version);
    format!(
        r#"set -eu
app_id={app_id}
target_version={target_version}
base="$HOME/.local/share"
manifest=""
direct="$base/$app_id/app/.surge/runtime.yml"
if [ -f "$direct" ]; then
  manifest="$direct"
else
  for candidate in "$base"/*/app/.surge/runtime.yml; do
    [ -f "$candidate" ] || continue
    candidate_id="$(sed -n 's/^id:[[:space:]]*//p' "$candidate" | head -n1)"
    if [ "$candidate_id" = "$app_id" ]; then
      manifest="$candidate"
      break
    fi
  done
fi
printf '{PROBE_BEGIN}\n'
if [ -z "$manifest" ]; then
  printf 'missing=true\n'
  printf 'proc_available=false\n'
  printf 'app_process_running=false\n'
  printf 'target_app_process_running=false\n'
  printf 'stale_app_process_running=false\n'
  printf 'supervisor_configured=false\n'
  printf 'supervisor_process_running=false\n'
  printf 'supervisor_waiting_for_previous_child=false\n'
  printf 'stale_supervisor_process_running=false\n'
  printf '{PROBE_END}\n'
  exit 0
fi
app_dir="${{manifest%/.surge/runtime.yml}}"
install_root="${{app_dir%/app}}"
status_file="$install_root/.surge-update-status.json"
supervisor_id="$(sed -n 's/^supervisorId:[[:space:]]*//p' "$manifest" | head -n1)"
proc_available=false
app_process_running=false
target_app_process_running=false
stale_app_process_running=false
supervisor_process_running=false
supervisor_waiting_for_previous_child=false
stale_supervisor_process_running=false
contains_target_first_run() {{
  cmd_tokens=" $1 "
  case "$cmd_tokens" in
    *" --surge-first-run $target_version "*|*" $target_version --surge-first-run "*) return 0 ;;
  esac
  return 1
}}
watched_pid_is_running() {{
  case "$1" in
    *" watch "*" --pid "*)
      rest="${{1#* --pid }}"
      watched_pid="${{rest%% *}}"
      case "$watched_pid" in ""|*[!0-9]*) return 1 ;; esac
      kill -0 "$watched_pid" 2>/dev/null
      return $?
    ;;
  esac
  return 1
}}
if [ -d /proc ]; then
  proc_available=true
  for cmdline in /proc/[0-9]*/cmdline; do
    [ -r "$cmdline" ] || continue
    pid="${{cmdline%/cmdline}}"
    pid="${{pid##*/}}"
    case "$pid" in "$$"|"$PPID") continue ;; esac
    cmd="$(tr '\0' ' ' < "$cmdline" 2>/dev/null || true)"
    [ -n "$cmd" ] || continue
    case "$cmd" in
      *"$install_root/app/"*)
        case "$cmd" in
          *"surge-supervisor"*) ;;
          *)
            app_process_running=true
            if contains_target_first_run "$cmd"; then
              target_app_process_running=true
            else
              stale_app_process_running=true
            fi
          ;;
        esac
      ;;
    esac
    if [ -n "$supervisor_id" ]; then
      case "$cmd" in
        *"surge-supervisor"*"--id $supervisor_id"*)
          supervisor_process_running=true
          if watched_pid_is_running "$cmd"; then
            supervisor_waiting_for_previous_child=true
          fi
          case " $cmd " in
            *" --surge-first-run "*) if ! contains_target_first_run "$cmd"; then stale_supervisor_process_running=true; fi ;;
          esac
        ;;
      esac
    fi
  done
fi
printf 'missing=false\n'
printf 'manifest=%s\n' "$manifest"
printf 'install_root=%s\n' "$install_root"
printf 'proc_available=%s\n' "$proc_available"
printf 'app_process_running=%s\n' "$app_process_running"
printf 'target_app_process_running=%s\n' "$target_app_process_running"
printf 'stale_app_process_running=%s\n' "$stale_app_process_running"
if [ -n "$supervisor_id" ]; then printf 'supervisor_configured=true\n'; else printf 'supervisor_configured=false\n'; fi
printf 'supervisor_process_running=%s\n' "$supervisor_process_running"
printf 'supervisor_waiting_for_previous_child=%s\n' "$supervisor_waiting_for_previous_child"
printf 'stale_supervisor_process_running=%s\n' "$stale_supervisor_process_running"
printf '{PROBE_END}\n'
printf '{RUNTIME_BEGIN}\n'
cat "$manifest"
printf '\n{RUNTIME_END}\n'
printf '{UPDATE_BEGIN}\n'
if [ -f "$status_file" ]; then
  cat "$status_file"
fi
printf '\n{UPDATE_END}\n'
"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_remote_probe_reads_sections() {
        let output = format!(
            "{PROBE_BEGIN}\nmissing=false\nproc_available=true\napp_process_running=true\ntarget_app_process_running=true\nstale_app_process_running=false\nsupervisor_configured=false\nsupervisor_process_running=false\nsupervisor_waiting_for_previous_child=false\nstale_supervisor_process_running=false\n{PROBE_END}\n{RUNTIME_BEGIN}\nid: sample-app\nversion: 1.2.3\nchannel: sample-channel\ninstallDirectory: sample-app\n{RUNTIME_END}\n{UPDATE_BEGIN}\n{UPDATE_END}\n",
        );

        let probe = parse_remote_probe(&output).expect("probe should parse");

        assert!(!probe.missing);
        assert!(probe.process.app_process_running());
        assert!(probe.runtime_yaml.expect("runtime yaml").contains("sample-app"));
    }
}
