use std::path::Path;
use std::time::Duration;

use crate::error::{Result, SurgeError};
use crate::platform::process::{ProcessHandle, spawn_detached};

use super::InstallProfile;

/// Start the installed application, using the supervisor if configured.
pub fn auto_start_after_install(
    profile: &InstallProfile<'_>,
    install_root: &Path,
    active_app_dir: &Path,
) -> Result<u32> {
    start_installed_application(
        profile,
        install_root,
        active_app_dir,
        &["--surge-installed"],
        true,
        false,
    )
}

/// Run the post-install lifecycle sequence:
/// 1) invoke `--surge-installed <version>` and wait for it to finish (up to 15s),
/// 2) invoke `--surge-first-run <version>` and return that process id.
///
/// This mirrors Snapx's install behavior where install hooks run in a short-lived
/// process and first-run continues in a second process.
pub fn auto_start_after_install_sequence(
    profile: &InstallProfile<'_>,
    install_root: &Path,
    active_app_dir: &Path,
    version: &str,
) -> Result<u32> {
    let installed_args = lifecycle_args("--surge-installed", version);
    let mut installed_handle =
        spawn_installed_application(profile, install_root, active_app_dir, &installed_args, false)?;
    wait_for_process_exit_or_timeout(&mut installed_handle, Duration::from_secs(15));

    let first_run_args = lifecycle_args("--surge-first-run", version);
    let first_run_handle = spawn_installed_application(profile, install_root, active_app_dir, &first_run_args, false)?;
    Ok(first_run_handle.pid())
}

/// Launch the installed application for user-facing "Launch" actions.
///
/// Unlike `auto_start_after_install`, this does not pass the `--surge-installed`
/// lifecycle argument and should keep GUI apps open for immediate use.
pub fn launch_installed_application(
    profile: &InstallProfile<'_>,
    install_root: &Path,
    active_app_dir: &Path,
) -> Result<u32> {
    start_installed_application(profile, install_root, active_app_dir, &[], false, true)
}

fn start_installed_application(
    profile: &InstallProfile<'_>,
    install_root: &Path,
    active_app_dir: &Path,
    app_args: &[&str],
    prefer_supervisor: bool,
    verify_running: bool,
) -> Result<u32> {
    let mut handle = spawn_installed_application(profile, install_root, active_app_dir, app_args, prefer_supervisor)?;
    let pid = handle.pid();
    if verify_running {
        let process_label = if prefer_supervisor && !profile.supervisor_id.trim().is_empty() {
            "supervisor"
        } else {
            "application"
        };
        verify_process_stays_running(&mut handle, process_label)?;
    }
    Ok(pid)
}

fn spawn_installed_application(
    profile: &InstallProfile<'_>,
    install_root: &Path,
    active_app_dir: &Path,
    app_args: &[&str],
    prefer_supervisor: bool,
) -> Result<ProcessHandle> {
    let main_exe = profile.main_exe.trim();
    if main_exe.is_empty() {
        return Err(SurgeError::Config(
            "Cannot auto-start: no main executable in release metadata".to_string(),
        ));
    }

    let exe_path = active_app_dir.join(main_exe);

    let supervisor_id = profile.supervisor_id.trim();
    if prefer_supervisor && !supervisor_id.is_empty() {
        let supervisor_path = active_app_dir.join(crate::platform::process::supervisor_binary_name());

        let install_root_str = install_root.to_string_lossy();
        let exe_path_str = exe_path.to_string_lossy();
        let mut args: Vec<&str> = vec![
            "run",
            "--id",
            supervisor_id,
            "--dir",
            &install_root_str,
            "--exe",
            &exe_path_str,
        ];
        if !app_args.is_empty() {
            args.push("--");
            args.extend_from_slice(app_args);
        }
        return spawn_detached(&supervisor_path, &args, Some(install_root), profile.environment);
    }

    spawn_detached(&exe_path, app_args, Some(install_root), profile.environment)
}

fn lifecycle_args<'a>(flag: &'a str, version: &'a str) -> Vec<&'a str> {
    let mut args = vec![flag];
    let version = version.trim();
    if !version.is_empty() {
        args.push(version);
    }
    args
}

fn wait_for_process_exit_or_timeout(handle: &mut ProcessHandle, timeout: Duration) {
    let check_interval = Duration::from_millis(100);
    let checks = (timeout.as_millis() / check_interval.as_millis()) as usize;

    for _ in 0..checks {
        if !handle.poll_running() {
            let _ = handle.wait();
            return;
        }
        std::thread::sleep(check_interval);
    }

    if handle.poll_running() {
        let _ = handle.kill();
        let _ = handle.wait();
    }
}

fn verify_process_stays_running(handle: &mut ProcessHandle, process_label: &str) -> Result<()> {
    let check_interval = Duration::from_millis(200);
    let total_wait = Duration::from_secs(4);
    let checks = (total_wait.as_millis() / check_interval.as_millis()) as usize;

    for _ in 0..checks {
        std::thread::sleep(check_interval);
        if !handle.poll_running() {
            let result = handle.wait()?;
            return Err(SurgeError::Platform(format!(
                "Failed to launch {process_label}: process exited shortly after start with code {}",
                result.exit_code
            )));
        }
    }
    Ok(())
}
