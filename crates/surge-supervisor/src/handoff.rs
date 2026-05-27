use std::path::Path;
use std::process::ExitStatus;

use surge_core::update::status::{
    RESTART_HANDOFF_TARGET_CHILD_EXITED_PHASE, mark_restart_handoff_converged, mark_restart_handoff_pending,
};

pub(super) fn pending_restart_handoff_version(
    _watched_pid: Option<u32>,
    handoff_version: Option<&str>,
    args: &[String],
) -> Option<String> {
    handoff_version
        .map(str::trim)
        .filter(|version| !version.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| first_run_version(args))
}

fn first_run_version(args: &[String]) -> Option<String> {
    args.iter().enumerate().find_map(|(index, arg)| {
        if arg != "--surge-first-run" {
            return None;
        }
        args.get(index + 1)
            .filter(|candidate| !candidate.starts_with("--"))
            .or_else(|| {
                index
                    .checked_sub(1)
                    .and_then(|prev| args.get(prev))
                    .filter(|candidate| !candidate.starts_with("--"))
            })
            .map(|version| version.trim().to_string())
            .filter(|version| !version.is_empty())
    })
}

pub(super) fn without_lifecycle_args(args: &[String]) -> Vec<String> {
    let mut retained = Vec::with_capacity(args.len());
    let mut index = 0usize;
    while index < args.len() {
        let arg = &args[index];
        if is_lifecycle_arg(arg) {
            index += 1;
            if lifecycle_arg_takes_value(arg) && args.get(index).is_some_and(|candidate| !candidate.starts_with("--")) {
                index += 1;
            }
            continue;
        }

        retained.push(arg.clone());
        index += 1;
    }

    retained
}

fn is_lifecycle_arg(arg: &str) -> bool {
    matches!(arg, "--surge-first-run" | "--surge-installed" | "--surge-updated") || arg.starts_with("--surge-updated=")
}

fn lifecycle_arg_takes_value(arg: &str) -> bool {
    matches!(arg, "--surge-first-run" | "--surge-installed" | "--surge-updated")
}

pub(super) fn record_restart_handoff_converged(install_dir: &Path, version: &str) {
    match mark_restart_handoff_converged(install_dir, version) {
        Ok(Some(_)) => {
            tracing::info!(version, "Restart handoff converged after target child startup");
        }
        Ok(None) => {
            tracing::warn!(
                install_root = %install_dir.display(),
                version,
                reason = "no matching pending restart handoff status record",
                "Failed to record restart handoff convergence"
            );
        }
        Err(e) => {
            tracing::warn!(
                install_root = %install_dir.display(),
                version,
                reason = %e,
                "Failed to record restart handoff convergence"
            );
        }
    }
}

pub(super) fn record_restart_handoff_child_exited(install_dir: &Path, version: &str, status: ExitStatus) {
    let reason = format!("target child exited with {status} before restart handoff completed");
    match mark_restart_handoff_pending(install_dir, version, &reason, RESTART_HANDOFF_TARGET_CHILD_EXITED_PHASE) {
        Ok(Some(_)) => {
            tracing::warn!(version, %status, "Restart handoff target child exited before startup proof completed");
        }
        Ok(None) => {
            tracing::warn!(
                install_root = %install_dir.display(),
                version,
                reason = "no matching pending restart handoff status record",
                "Failed to record restart handoff child exit"
            );
        }
        Err(e) => {
            tracing::warn!(
                install_root = %install_dir.display(),
                version,
                reason = %e,
                "Failed to record restart handoff child exit"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_handoff_version_uses_explicit_watch_target() {
        let args = vec!["--app-mode".to_string()];

        let version = pending_restart_handoff_version(Some(42), Some(" 2.0.0 "), &args);

        assert_eq!(version.as_deref(), Some("2.0.0"));
    }

    #[test]
    fn pending_handoff_version_falls_back_to_first_run_arg() {
        let args = vec!["--surge-first-run".to_string(), "2.0.0".to_string()];

        let version = pending_restart_handoff_version(Some(42), None, &args);

        assert_eq!(version.as_deref(), Some("2.0.0"));
    }

    #[test]
    fn pending_handoff_version_accepts_first_run_without_watched_pid() {
        let args = vec!["--surge-first-run".to_string(), "2.0.0".to_string()];

        let version = pending_restart_handoff_version(None, None, &args);

        assert_eq!(version.as_deref(), Some("2.0.0"));
    }

    #[test]
    fn restart_args_drop_lifecycle_flag_value_pairs() {
        let args = vec![
            "--app-mode".to_string(),
            "service".to_string(),
            "--surge-first-run".to_string(),
            "2.0.0".to_string(),
            "--surge-updated=2.0.0".to_string(),
            "--tail".to_string(),
        ];

        let retained = without_lifecycle_args(&args);

        assert_eq!(retained, vec!["--app-mode", "service", "--tail"]);
    }
}
