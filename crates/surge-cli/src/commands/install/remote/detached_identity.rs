use super::detached::REMOTE_INSTALLER_PID_PATH;

pub(super) fn process_identity_function() -> &'static str {
    r#"process_identity() {
  boot="$(cat /proc/sys/kernel/random/boot_id 2>/dev/null)" || return 1;
  stat="$(cat "/proc/$1/stat" 2>/dev/null)" || return 1;
  ticks="$(printf '%s\n' "${stat##*) }" | awk '{print $20}')";
  case "$ticks" in ''|*[!0-9]*) return 1 ;; esac;
  [ -n "$boot" ] || return 1;
  printf '%s:%s\n' "$boot" "$ticks";
}"#
}

pub(super) fn installer_process_probe() -> String {
    format!(
        r#"{};
pid=''; alive=no; unverified=no;
if [ -f {REMOTE_INSTALLER_PID_PATH} ]; then pid="$(tr -d '[:space:]' < {REMOTE_INSTALLER_PID_PATH})"; fi;
case "$pid" in
  ''|*[!0-9]*) : ;;
  *) if kill -0 "$pid" 2>/dev/null; then
       expected_identity="$(cat /tmp/.surge-installer.identity 2>/dev/null || true)";
       actual_identity="$(process_identity "$pid" || true)";
       if [ -z "$expected_identity" ]; then unverified=yes;
       elif [ -n "$actual_identity" ] && [ "$actual_identity" = "$expected_identity" ]; then alive=yes;
       fi;
     fi ;;
esac"#,
        process_identity_function()
    )
}

#[cfg(all(test, target_os = "linux"))]
pub(super) fn current_process_identity() -> String {
    let output = std::process::Command::new("sh")
        .args([
            "-c",
            &format!(
                "{}; process_identity {}",
                process_identity_function(),
                std::process::id()
            ),
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::super::detached::{build_remote_detached_install_probe_command, parse_remote_detached_install_probe};
    use super::*;
    use std::fs;
    use std::process::Command;

    #[test]
    fn probe_distinguishes_current_reused_and_unverified_pids() {
        let temp = tempfile::tempdir().unwrap();
        let prefix = temp.path().join(".surge-installer");
        let pid = prefix.with_extension("pid");
        let identity = prefix.with_extension("identity");
        fs::write(&pid, std::process::id().to_string()).unwrap();
        let script =
            build_remote_detached_install_probe_command().replace("/tmp/.surge-installer", &prefix.to_string_lossy());
        let probe = || {
            let output = Command::new("sh").args(["-c", &script]).output().unwrap();
            assert!(output.status.success());
            parse_remote_detached_install_probe(&String::from_utf8(output.stdout).unwrap()).unwrap()
        };
        assert!(probe().unverified_alive);
        let current = current_process_identity();
        fs::write(&identity, &current).unwrap();
        assert!(probe().alive);
        assert!(!probe().unverified_alive);
        let (boot, ticks) = current.split_once(':').unwrap();
        for stale in [
            format!("old-boot:{ticks}"),
            format!("{boot}:{}", ticks.parse::<u64>().unwrap() + 1),
        ] {
            fs::write(&identity, stale).unwrap();
            let result = probe();
            assert!(!result.alive);
            assert!(!result.unverified_alive);
        }
        fs::remove_file(pid).unwrap();
        assert!(!probe().alive);
        assert!(!probe().unverified_alive);
    }
}
