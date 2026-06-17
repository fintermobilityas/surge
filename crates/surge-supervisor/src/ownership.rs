use std::path::Path;

pub(crate) fn current_supervisor_owns_pid_file(pid_file: &Path, own_pid: u32) -> bool {
    let expected = own_pid.to_string();
    std::fs::read_to_string(pid_file).is_ok_and(|contents| contents.trim() == expected)
}

pub(crate) fn supervisor_was_superseded(pid_file: &Path, own_pid: u32) -> bool {
    if current_supervisor_owns_pid_file(pid_file, own_pid) {
        return false;
    }

    tracing::info!(
        "Supervisor pid file {} no longer belongs to PID {own_pid}; exiting without restarting children",
        pid_file.display()
    );
    true
}

pub(crate) fn remove_owned_supervisor_state(pid_file: &Path, stop_file: &Path, own_pid: u32) {
    if !current_supervisor_owns_pid_file(pid_file, own_pid) {
        tracing::debug!(
            "Skipping supervisor state cleanup because {} no longer belongs to PID {own_pid}",
            pid_file.display()
        );
        return;
    }

    if stop_file.exists() {
        let _ = std::fs::remove_file(stop_file);
    }
    if current_supervisor_owns_pid_file(pid_file, own_pid) {
        let _ = std::fs::remove_file(pid_file);
    }
}
