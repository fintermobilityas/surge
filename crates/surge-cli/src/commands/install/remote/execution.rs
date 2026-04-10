use super::{
    AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader, Command, Instant, Path, Result, Stdio,
    SurgeError, logline, make_progress_bar, make_spinner, shell_single_quote,
};

pub(crate) fn resolve_tailscale_targets(node: &str, node_user: Option<&str>) -> Result<(String, String)> {
    let node = node.trim();
    if node.is_empty() {
        return Err(SurgeError::Config(
            "Tailscale node cannot be empty. Provide --node <node>.".to_string(),
        ));
    }

    if let Some((user_part, host_part)) = node.split_once('@') {
        if user_part.trim().is_empty() || host_part.trim().is_empty() {
            return Err(SurgeError::Config(format!(
                "Invalid --node value '{node}'. Expected '<node>' or '<user>@<node>'."
            )));
        }
        return Ok((node.to_string(), host_part.to_string()));
    }

    if let Some(user) = node_user.map(str::trim).filter(|value| !value.is_empty()) {
        Ok((format!("{user}@{node}"), node.to_string()))
    } else {
        Ok((node.to_string(), node.to_string()))
    }
}

pub(crate) async fn detect_remote_home_directory(ssh_node: &str) -> Result<std::path::PathBuf> {
    let command = format!("sh -c {}", shell_single_quote("printf %s \"$HOME\""));
    let output = run_tailscale_capture(&["ssh", ssh_node, command.as_str()]).await?;
    let home = output.trim();
    if home.is_empty() {
        return Err(SurgeError::Platform(format!(
            "Failed to determine HOME directory on remote node '{ssh_node}'"
        )));
    }
    Ok(std::path::PathBuf::from(home))
}

pub(crate) async fn stream_directory_to_tailscale_node_with_command(
    node: &str,
    local_dir: &Path,
    remote_command: &str,
) -> Result<()> {
    let ssh_command = format!("sh -lc {}", shell_single_quote(remote_command));
    let local_dir_str = local_dir.to_string_lossy().to_string();
    let mut tar_child = Command::new("tar")
        .args(["-C", local_dir_str.as_str(), "-cf", "-", "."])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| SurgeError::Platform(format!("Failed to archive '{}' for transfer: {e}", local_dir.display())))?;
    let mut remote_child = Command::new("tailscale")
        .args(["ssh", node, ssh_command.as_str()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| SurgeError::Platform(format!("Failed to run tailscale ssh stream copy: {e}")))?;

    let mut tar_stdout = tar_child
        .stdout
        .take()
        .ok_or_else(|| SurgeError::Platform("Failed to capture local tar stdout".to_string()))?;
    let mut remote_stdin = remote_child
        .stdin
        .take()
        .ok_or_else(|| SurgeError::Platform("Failed to capture tailscale ssh stdin".to_string()))?;

    let transfer_message = format!("Streaming '{}' to '{node}'", local_dir.display());
    let transfer_spinner = make_spinner(&transfer_message);
    let transfer_result: Result<()> = async {
        let mut buffer = vec![0_u8; 128 * 1024];
        loop {
            let read_bytes = tar_stdout.read(&mut buffer).await.map_err(|e| {
                SurgeError::Platform(format!(
                    "Failed to read archived directory '{}' for transfer: {e}",
                    local_dir.display()
                ))
            })?;
            if read_bytes == 0 {
                break;
            }
            remote_stdin.write_all(&buffer[..read_bytes]).await.map_err(|e| {
                SurgeError::Platform(format!("Failed to stream '{}' to '{node}': {e}", local_dir.display()))
            })?;
            if let Some(spinner) = transfer_spinner.as_ref() {
                spinner.tick();
            }
        }
        remote_stdin.flush().await.map_err(|e| {
            SurgeError::Platform(format!(
                "Failed to flush transfer stream to '{node}' for '{}': {e}",
                local_dir.display()
            ))
        })?;
        Ok(())
    }
    .await;
    drop(remote_stdin);

    if let Some(spinner) = &transfer_spinner {
        spinner.finish_and_clear();
    }

    if let Err(err) = transfer_result {
        let _ = tar_child.kill().await;
        let _ = remote_child.kill().await;
        return Err(err);
    }

    let tar_output = tar_child
        .wait_with_output()
        .await
        .map_err(|e| SurgeError::Platform(format!("Failed to wait for local tar process: {e}")))?;
    if !tar_output.status.success() {
        let stderr = String::from_utf8_lossy(&tar_output.stderr).trim().to_string();
        return Err(SurgeError::Platform(if stderr.is_empty() {
            format!("Command failed: tar -C '{}' -cf - .", local_dir.display())
        } else {
            format!("Command failed: tar -C '{}' -cf - .: {stderr}", local_dir.display())
        }));
    }

    let remote_output = remote_child
        .wait_with_output()
        .await
        .map_err(|e| SurgeError::Platform(format!("Failed to wait for tailscale ssh stream copy: {e}")))?;
    if !remote_output.status.success() {
        let stderr = String::from_utf8_lossy(&remote_output.stderr).trim().to_string();
        return Err(SurgeError::Platform(if stderr.is_empty() {
            format!("Command failed: tailscale ssh {node} sh -lc <stream-copy>")
        } else {
            format!("Command failed: tailscale ssh {node} sh -lc <stream-copy>: {stderr}")
        }));
    }

    Ok(())
}

pub(crate) async fn stream_file_to_tailscale_node_with_command(
    node: &str,
    local_file: &Path,
    remote_command: &str,
) -> Result<()> {
    let ssh_command = format!("sh -lc {}", shell_single_quote(remote_command));
    let mut child = Command::new("tailscale")
        .args(["ssh", node, ssh_command.as_str()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| SurgeError::Platform(format!("Failed to run tailscale ssh stream copy: {e}")))?;

    let mut local_reader = tokio::fs::File::open(local_file)
        .await
        .map_err(|e| SurgeError::Platform(format!("Failed to open '{}' for transfer: {e}", local_file.display())))?;

    let transfer_total_bytes = tokio::fs::metadata(local_file).await.map_or(0, |meta| meta.len());
    let transfer_message = format!("Streaming '{}' to '{node}'", local_file.display());
    let transfer_bar = if transfer_total_bytes > 0 {
        make_progress_bar(&transfer_message, transfer_total_bytes)
    } else {
        make_spinner(&transfer_message)
    };
    let mut last_transfer_log = Instant::now();

    let mut child_stdin = child
        .stdin
        .take()
        .ok_or_else(|| SurgeError::Platform("Failed to capture tailscale ssh stdin".to_string()))?;

    let mut transferred_bytes = 0_u64;
    let mut buffer = vec![0_u8; 128 * 1024];
    loop {
        let read_bytes = local_reader.read(&mut buffer).await.map_err(|e| {
            SurgeError::Platform(format!("Failed to read '{}' for transfer: {e}", local_file.display()))
        })?;
        if read_bytes == 0 {
            break;
        }
        child_stdin.write_all(&buffer[..read_bytes]).await.map_err(|e| {
            SurgeError::Platform(format!("Failed to stream '{}' to '{node}': {e}", local_file.display()))
        })?;
        transferred_bytes = transferred_bytes.saturating_add(u64::try_from(read_bytes).unwrap_or(0));

        if let Some(bar) = transfer_bar.as_ref() {
            if transfer_total_bytes > 0 {
                bar.set_position(transferred_bytes);
            } else {
                bar.tick();
                bar.set_message(format!("{transfer_message} ({transferred_bytes} bytes transferred)"));
            }
        } else if last_transfer_log.elapsed() >= std::time::Duration::from_secs(5) {
            if transfer_total_bytes > 0 {
                let pct = (transferred_bytes as f64 / transfer_total_bytes as f64) * 100.0;
                logline::subtle(&format!(
                    "Streaming package to '{node}'... {transferred_bytes}/{transfer_total_bytes} bytes ({pct:.0}%)"
                ));
            } else {
                logline::subtle(&format!(
                    "Streaming package to '{node}'... {transferred_bytes} bytes transferred"
                ));
            }
            last_transfer_log = Instant::now();
        }
    }

    child_stdin.flush().await.map_err(|e| {
        SurgeError::Platform(format!(
            "Failed to flush transfer stream to '{node}' for '{}': {e}",
            local_file.display()
        ))
    })?;
    drop(child_stdin);

    if let Some(bar) = &transfer_bar {
        bar.finish_and_clear();
    } else {
        logline::subtle(&format!(
            "Completed stream upload to '{node}' ({transferred_bytes} bytes)."
        ));
    }

    let finalize_spinner = make_spinner("Waiting for remote copy confirmation");
    if finalize_spinner.is_none() {
        logline::subtle("Waiting for remote copy confirmation...");
    }

    let output = child.wait_with_output().await;
    if let Some(spinner) = finalize_spinner {
        spinner.finish_and_clear();
    }
    let output =
        output.map_err(|e| SurgeError::Platform(format!("Failed to wait for tailscale ssh stream copy: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let msg = if stderr.is_empty() {
            format!("Command failed: tailscale ssh {node} sh -lc <stream-copy>")
        } else {
            format!("Command failed: tailscale ssh {node} sh -lc <stream-copy>: {stderr}")
        };
        return Err(SurgeError::Platform(msg));
    }

    Ok(())
}

pub(crate) async fn run_tailscale_capture(args: &[&str]) -> Result<String> {
    let output = Command::new("tailscale")
        .args(args)
        .output()
        .await
        .map_err(|e| SurgeError::Platform(format!("Failed to run tailscale command: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let cmd = format!("tailscale {}", args.join(" "));
        let msg = if stderr.is_empty() {
            format!("Command failed: {cmd}")
        } else {
            format!("Command failed: {cmd}: {stderr}")
        };
        return Err(SurgeError::Platform(msg));
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

pub(crate) async fn run_tailscale_streaming(args: &[&str], prefix: &str) -> Result<()> {
    let mut child = Command::new("tailscale")
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| SurgeError::Platform(format!("Failed to run tailscale command: {e}")))?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| SurgeError::Platform("Failed to capture tailscale stdout".to_string()))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| SurgeError::Platform("Failed to capture tailscale stderr".to_string()))?;

    let stdout_task = tokio::spawn(relay_tailscale_output(stdout, prefix.to_string()));
    let stderr_task = tokio::spawn(relay_tailscale_output(stderr, prefix.to_string()));

    let status = child
        .wait()
        .await
        .map_err(|e| SurgeError::Platform(format!("Failed to wait for tailscale command: {e}")))?;
    let stdout = stdout_task
        .await
        .map_err(|e| SurgeError::Platform(format!("Failed to read tailscale stdout: {e}")))?
        .map_err(|e| SurgeError::Platform(format!("Failed to read tailscale stdout: {e}")))?;
    let stderr = stderr_task
        .await
        .map_err(|e| SurgeError::Platform(format!("Failed to read tailscale stderr: {e}")))?
        .map_err(|e| SurgeError::Platform(format!("Failed to read tailscale stderr: {e}")))?;

    if !status.success() {
        let cmd = format!("tailscale {}", args.join(" "));
        let message = stderr
            .lines()
            .rev()
            .find(|line| !line.trim().is_empty())
            .or_else(|| stdout.lines().rev().find(|line| !line.trim().is_empty()));
        let msg = if let Some(message) = message {
            format!("Command failed: {cmd}: {}", message.trim())
        } else {
            format!("Command failed: {cmd}")
        };
        return Err(SurgeError::Platform(msg));
    }

    Ok(())
}

async fn relay_tailscale_output<R>(reader: R, prefix: String) -> std::io::Result<String>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let mut reader = BufReader::new(reader);
    let mut buffer = Vec::new();
    let mut captured = String::new();

    loop {
        buffer.clear();
        let read = reader.read_until(b'\n', &mut buffer).await?;
        if read == 0 {
            break;
        }

        let chunk = String::from_utf8_lossy(&buffer);
        let trimmed = chunk.trim();
        if !trimmed.is_empty() {
            logline::subtle(&format!("{prefix}: {trimmed}"));
        }
        captured.push_str(&chunk);
    }

    Ok(captured)
}
