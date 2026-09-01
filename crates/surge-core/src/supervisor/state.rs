use std::path::{Path, PathBuf};

use crate::error::{Result, SurgeError};

#[cfg(unix)]
mod takeover;
#[cfg(unix)]
pub use takeover::{
    SupervisorTakeoverAcknowledgement, SupervisorTakeoverCancellation, SupervisorTakeoverCommit,
    SupervisorTakeoverHandoff, SupervisorTakeoverInstance, SupervisorTakeoverRequest,
    accept_supervisor_takeover_request, cancel_supervisor_takeover_request, clear_supervisor_takeover_exchange,
    clear_supervisor_takeover_instance_if_owned, clear_supervisor_takeover_request, read_accepted_supervisor_takeover,
    read_supervisor_takeover_acknowledgement, read_supervisor_takeover_commit, read_supervisor_takeover_instance,
    read_supervisor_takeover_request, supervisor_takeover_acknowledgement_file, supervisor_takeover_request_file,
    take_accepted_supervisor_takeover, write_supervisor_takeover_acknowledgement, write_supervisor_takeover_commit,
    write_supervisor_takeover_instance, write_supervisor_takeover_request,
};

fn normalized_supervisor_id(supervisor_id: &str) -> &str {
    supervisor_id.trim()
}

fn supervisor_state_path(install_dir: &Path, supervisor_id: &str, suffix: &str) -> PathBuf {
    install_dir.join(format!(
        ".surge-supervisor-{}{suffix}",
        normalized_supervisor_id(supervisor_id)
    ))
}

#[must_use]
pub fn supervisor_pid_file(install_dir: &Path, supervisor_id: &str) -> PathBuf {
    supervisor_state_path(install_dir, supervisor_id, ".pid")
}

#[must_use]
pub fn supervisor_stop_file(install_dir: &Path, supervisor_id: &str) -> PathBuf {
    supervisor_state_path(install_dir, supervisor_id, ".stop")
}

#[must_use]
pub fn supervisor_restart_args_file(install_dir: &Path, supervisor_id: &str) -> PathBuf {
    supervisor_state_path(install_dir, supervisor_id, ".args.json")
}

#[must_use]
pub fn supervisor_exe_file(install_dir: &Path, supervisor_id: &str) -> PathBuf {
    supervisor_state_path(install_dir, supervisor_id, ".exe")
}

#[must_use]
#[cfg(unix)]
pub fn supervisor_takeover_pid_file(install_dir: &Path, supervisor_id: &str) -> PathBuf {
    supervisor_state_path(install_dir, supervisor_id, ".takeover.pid")
}

#[cfg(unix)]
pub fn write_supervisor_takeover_pid(install_dir: &Path, supervisor_id: &str, pid: u32) -> Result<()> {
    std::fs::write(
        supervisor_takeover_pid_file(install_dir, supervisor_id),
        pid.to_string(),
    )?;
    Ok(())
}

#[must_use]
#[cfg(unix)]
pub fn take_supervisor_takeover_pid(install_dir: &Path, supervisor_id: &str) -> Option<u32> {
    let path = supervisor_takeover_pid_file(install_dir, supervisor_id);
    let pid = std::fs::read_to_string(&path)
        .ok()
        .and_then(|contents| contents.trim().parse::<u32>().ok())
        .filter(|pid| *pid != 0);
    let _ = std::fs::remove_file(path);
    pid
}

#[cfg(unix)]
pub fn clear_supervisor_takeover_pid(install_dir: &Path, supervisor_id: &str) {
    if let Err(error) = try_clear_supervisor_takeover_pid(install_dir, supervisor_id) {
        tracing::warn!(supervisor_id, %error, "Failed to clean up legacy supervisor takeover PID");
    }
}

#[cfg(unix)]
pub fn try_clear_supervisor_takeover_pid(install_dir: &Path, supervisor_id: &str) -> Result<()> {
    match std::fs::remove_file(supervisor_takeover_pid_file(install_dir, supervisor_id)) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

/// Persist the supervised executable path so the spawning side can omit it from
/// the supervisor's argv. Keeping the app path out of argv stops an external
/// `pkill -f <app-path>` from also matching the supervisor process.
pub fn write_supervisor_exe_path(install_dir: &Path, supervisor_id: &str, exe_path: &Path) -> Result<()> {
    let supervisor_id = normalized_supervisor_id(supervisor_id);
    if supervisor_id.is_empty() {
        return Ok(());
    }

    std::fs::write(
        supervisor_exe_file(install_dir, supervisor_id),
        exe_path.to_string_lossy().as_bytes(),
    )?;
    Ok(())
}

#[must_use]
pub fn read_supervisor_exe_path(install_dir: &Path, supervisor_id: &str) -> Option<PathBuf> {
    let supervisor_id = normalized_supervisor_id(supervisor_id);
    if supervisor_id.is_empty() {
        return None;
    }

    let exe_path = supervisor_exe_file(install_dir, supervisor_id);
    let contents = std::fs::read_to_string(exe_path).ok()?;
    let trimmed = contents.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(PathBuf::from(trimmed))
}

pub fn write_restart_args(install_dir: &Path, supervisor_id: &str, args: &[String]) -> Result<()> {
    let supervisor_id = normalized_supervisor_id(supervisor_id);
    if supervisor_id.is_empty() {
        return Ok(());
    }

    let encoded = serde_json::to_vec(args)
        .map_err(|e| SurgeError::Config(format!("Failed to encode supervisor restart args: {e}")))?;
    std::fs::write(supervisor_restart_args_file(install_dir, supervisor_id), encoded)?;
    Ok(())
}

pub fn read_restart_args(install_dir: &Path, supervisor_id: &str) -> Result<Vec<String>> {
    let supervisor_id = normalized_supervisor_id(supervisor_id);
    if supervisor_id.is_empty() {
        return Ok(Vec::new());
    }

    let args_path = supervisor_restart_args_file(install_dir, supervisor_id);
    if !args_path.is_file() {
        return Ok(Vec::new());
    }

    let raw = std::fs::read(&args_path)?;
    serde_json::from_slice(&raw)
        .map_err(|e| SurgeError::Config(format!("Failed to decode supervisor restart args: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restart_args_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let args = vec!["--headless".to_string(), "--profile=test".to_string()];

        write_restart_args(dir.path(), "demo-supervisor", &args).unwrap();

        let restored = read_restart_args(dir.path(), "demo-supervisor").unwrap();
        assert_eq!(restored, args);
    }

    #[test]
    fn restart_args_missing_file_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let restored = read_restart_args(dir.path(), "demo-supervisor").unwrap();
        assert!(restored.is_empty());
    }

    #[test]
    fn supervisor_exe_path_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let exe = dir.path().join("app").join("demo-app");

        write_supervisor_exe_path(dir.path(), "demo-supervisor", &exe).unwrap();

        assert_eq!(read_supervisor_exe_path(dir.path(), "demo-supervisor"), Some(exe));
    }

    #[test]
    fn supervisor_exe_path_missing_file_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(read_supervisor_exe_path(dir.path(), "demo-supervisor"), None);
    }

    #[test]
    fn supervisor_exe_path_blank_contents_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(supervisor_exe_file(dir.path(), "demo-supervisor"), "   \n").unwrap();
        assert_eq!(read_supervisor_exe_path(dir.path(), "demo-supervisor"), None);
    }

    #[test]
    #[cfg(unix)]
    fn supervisor_takeover_pid_is_consumed_once() {
        let dir = tempfile::tempdir().unwrap();

        write_supervisor_takeover_pid(dir.path(), "demo-supervisor", 42).unwrap();

        assert_eq!(take_supervisor_takeover_pid(dir.path(), "demo-supervisor"), Some(42));
        assert_eq!(take_supervisor_takeover_pid(dir.path(), "demo-supervisor"), None);
    }

    #[test]
    #[cfg(unix)]
    fn supervisor_takeover_pid_cleanup_reports_failures() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(supervisor_takeover_pid_file(dir.path(), "demo-supervisor")).unwrap();

        assert!(try_clear_supervisor_takeover_pid(dir.path(), "demo-supervisor").is_err());
    }
}
