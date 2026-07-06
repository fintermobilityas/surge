use std::path::{Path, PathBuf};

use crate::error::{Result, SurgeError};

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
}
