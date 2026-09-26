use std::process::Stdio;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};

use super::{Result, SurgeError, shell_single_quote};

pub(super) struct RemoteInstallerLock {
    connection: Child,
}

// Stale-transfer cleanup matches .surge-installer in process command lines.
fn lock_script() -> &'static str {
    "exec 9>/tmp/.surge-remote-control.lock; flock -n 9 || exit 75; printf 'locked\\n'; cat >/dev/null"
}

impl RemoteInstallerLock {
    pub(super) async fn acquire(ssh_target: &str) -> Result<Self> {
        let script = format!("sh -c {}", shell_single_quote(lock_script()));
        let mut connection = Command::new("tailscale")
            .args(["ssh", ssh_target, &script])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| SurgeError::Platform(format!("Could not acquire remote installer lock: {error}")))?;
        let stdout = connection
            .stdout
            .take()
            .ok_or_else(|| SurgeError::Platform("Missing installer lock handshake pipe".to_string()))?;
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        let read = tokio::time::timeout(Duration::from_secs(30), reader.read_line(&mut line)).await;
        if !matches!(read, Ok(Ok(_))) || line.trim() != "locked" {
            return Err(SurgeError::Platform("Could not acquire the node-local installer lock; another controller may be preparing or watching an install. Retry when it has finished.".to_string()));
        }
        Ok(Self { connection })
    }

    pub(super) fn ensure_held(&mut self) -> Result<()> {
        match self.connection.try_wait() {
            Ok(None) => Ok(()),
            Ok(Some(_)) => Err(SurgeError::Platform(
                "Remote installer lock connection closed; refusing further installer mutations".to_string(),
            )),
            Err(error) => Err(SurgeError::Platform(format!(
                "Could not check remote installer lock: {error}"
            ))),
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::io::BufRead;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn node_local_lock_serializes_controllers_and_releases_on_disconnect() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(".surge-remote-control.lock");
        let script = lock_script().replace("/tmp/.surge-remote-control.lock", &path.to_string_lossy());
        let mut owner = std::process::Command::new("sh")
            .args(["-c", &script])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut reader = std::io::BufReader::new(owner.stdout.take().unwrap());
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert_eq!(line, "locked\n");
        let bin = temp.path().join("bin");
        std::fs::create_dir(&bin).unwrap();
        let ps = bin.join("ps");
        std::fs::write(
            &ps,
            format!("#!/bin/sh\nexec /bin/ps -p {} -o pid=,ppid=,args=\n", owner.id()),
        )
        .unwrap();
        std::fs::set_permissions(&ps, std::fs::Permissions::from_mode(0o755)).unwrap();
        let cleanup = super::super::installer_stage::build_remote_installer_transfer_cleanup_command().replace(
            "/tmp/.surge-installer",
            &temp.path().join(".surge-installer").to_string_lossy(),
        );
        let cleanup_result = std::process::Command::new("sh")
            .args(["-c", &cleanup])
            .env("PATH", format!("{}:/usr/bin:/bin", bin.display()))
            .output()
            .unwrap();
        assert!(cleanup_result.status.success());
        assert!(
            owner.try_wait().unwrap().is_none(),
            "transfer cleanup must preserve the controller lock"
        );
        let contender = std::process::Command::new("sh")
            .args(["-c", &script])
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert_eq!(contender.status.code(), Some(75));
        assert!(contender.stdout.is_empty());
        drop(owner.stdin.take());
        assert!(owner.wait().unwrap().success());
        let next = std::process::Command::new("sh")
            .args(["-c", &script])
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert!(next.status.success());
        assert_eq!(next.stdout, b"locked\n");
    }
}
