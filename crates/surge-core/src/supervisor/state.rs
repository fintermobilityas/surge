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
}
