use std::path::Path;

use super::execution::{
    REMOTE_INSTALLER_FINAL_PATH, REMOTE_INSTALLER_PARTIAL_PATH, run_tailscale_capture,
    stream_file_to_tailscale_node_with_command_from_offset,
};
use super::{Result, SurgeError, logline, shell_single_quote};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InstallerStageState {
    Complete,
    Missing,
    Partial(u64),
    Discard,
}

pub(crate) async fn stage_installer_file_for_tailscale(
    ssh_target: &str,
    file_target: &str,
    installer_path: &Path,
    expected_size: u64,
    expected_sha256: &str,
) -> Result<()> {
    let state = probe_remote_installer_stage(ssh_target, expected_size, expected_sha256).await?;
    match state {
        InstallerStageState::Complete => {
            logline::success(&format!(
                "Verified staged installer already present on '{file_target}'; skipping upload."
            ));
            Ok(())
        }
        InstallerStageState::Partial(offset) => {
            logline::info(&format!(
                "Resuming staged installer upload to '{file_target}' from byte {offset} of {expected_size}."
            ));
            let command = build_remote_installer_resume_command(expected_size, expected_sha256, offset);
            stream_file_to_tailscale_node_with_command_from_offset(ssh_target, installer_path, &command, offset).await
        }
        InstallerStageState::Missing => {
            logline::info(&format!("Creating remote installer stage on '{file_target}'."));
            let command = build_remote_installer_resume_command(expected_size, expected_sha256, 0);
            stream_file_to_tailscale_node_with_command_from_offset(ssh_target, installer_path, &command, 0).await
        }
        InstallerStageState::Discard => {
            logline::warn(&format!(
                "Discarding incompatible remote installer stage on '{file_target}' and uploading from the beginning."
            ));
            let command = build_remote_installer_resume_command(expected_size, expected_sha256, 0);
            stream_file_to_tailscale_node_with_command_from_offset(ssh_target, installer_path, &command, 0).await
        }
    }
}

async fn probe_remote_installer_stage(
    ssh_target: &str,
    expected_size: u64,
    expected_sha256: &str,
) -> Result<InstallerStageState> {
    let command = format!(
        "sh -c {}",
        shell_single_quote(&build_remote_installer_stage_probe_command(
            expected_size,
            expected_sha256
        ))
    );
    let output = run_tailscale_capture(&["ssh", ssh_target, command.as_str()]).await?;
    parse_installer_stage_probe(output.trim())
}

fn parse_installer_stage_probe(output: &str) -> Result<InstallerStageState> {
    if output == "complete" {
        return Ok(InstallerStageState::Complete);
    }
    if output == "missing" {
        return Ok(InstallerStageState::Missing);
    }
    if output == "discard" {
        return Ok(InstallerStageState::Discard);
    }
    if let Some(size) = output.strip_prefix("partial ") {
        let size = size
            .trim()
            .parse::<u64>()
            .map_err(|e| SurgeError::Platform(format!("Remote installer stage returned invalid partial size: {e}")))?;
        return Ok(InstallerStageState::Partial(size));
    }
    Err(SurgeError::Platform(format!(
        "Remote installer stage probe returned unexpected output: {output}"
    )))
}

fn build_remote_installer_stage_probe_command(expected_size: u64, expected_sha256: &str) -> String {
    let expected_sha256 = expected_sha256.trim();
    format!(
        "set -eu; \
final_path={REMOTE_INSTALLER_FINAL_PATH}; partial={REMOTE_INSTALLER_PARTIAL_PATH}; \
expected_size='{expected_size}'; expected_sha256='{expected_sha256}'; \
hash_file() {{ if command -v sha256sum >/dev/null 2>&1; then sha256sum \"$1\" | awk '{{print $1}}'; \
elif command -v shasum >/dev/null 2>&1; then shasum -a 256 \"$1\" | awk '{{print $1}}'; \
else return 1; fi; }}; \
if [ -f \"$final_path\" ]; then \
  final_size=\"$(wc -c < \"$final_path\" | tr -d '[:space:]')\"; \
  if [ \"$final_size\" = \"$expected_size\" ] && [ \"$(hash_file \"$final_path\")\" = \"$expected_sha256\" ]; then echo complete; exit 0; fi; \
fi; \
if [ -f \"$partial\" ]; then \
  partial_size=\"$(wc -c < \"$partial\" | tr -d '[:space:]')\"; \
  if [ \"$partial_size\" -gt \"$expected_size\" ]; then echo discard; exit 0; fi; \
  echo \"partial $partial_size\"; exit 0; \
fi; \
echo missing"
    )
}

fn build_remote_installer_resume_command(expected_size: u64, expected_sha256: &str, offset: u64) -> String {
    let expected_sha256 = expected_sha256.trim();
    format!(
        "set -eu; \
final_path={REMOTE_INSTALLER_FINAL_PATH}; partial={REMOTE_INSTALLER_PARTIAL_PATH}; \
expected_size='{expected_size}'; expected_sha256='{expected_sha256}'; expected_offset='{offset}'; \
if [ \"$expected_offset\" = 0 ]; then rm -f \"$partial\"; fi; \
actual_offset=0; if [ -f \"$partial\" ]; then actual_offset=\"$(wc -c < \"$partial\" | tr -d '[:space:]')\"; fi; \
if [ \"$actual_offset\" != \"$expected_offset\" ]; then \
  echo \"remote installer partial changed before resume: expected $expected_offset bytes, got $actual_offset bytes\" >&2; exit 1; \
fi; \
cat >> \"$partial\"; \
actual_size=\"$(wc -c < \"$partial\" | tr -d '[:space:]')\"; \
if [ \"$actual_size\" != \"$expected_size\" ]; then \
  echo \"remote installer size mismatch at $partial: expected $expected_size bytes, got $actual_size bytes\" >&2; exit 1; \
fi; \
if command -v sha256sum >/dev/null 2>&1; then actual_sha256=\"$(sha256sum \"$partial\" | awk '{{print $1}}')\"; \
elif command -v shasum >/dev/null 2>&1; then actual_sha256=\"$(shasum -a 256 \"$partial\" | awk '{{print $1}}')\"; \
else echo 'remote host has no sha256sum or shasum command available to verify installer transfer' >&2; exit 1; fi; \
if [ \"$actual_sha256\" != \"$expected_sha256\" ]; then \
  echo \"remote installer sha256 mismatch at $partial: expected $expected_sha256, got $actual_sha256\" >&2; rm -f \"$partial\"; exit 1; \
fi; \
chmod +x \"$partial\"; mv \"$partial\" \"$final_path\""
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_installer_stage_probe_reads_states() {
        assert_eq!(
            parse_installer_stage_probe("complete").unwrap(),
            InstallerStageState::Complete
        );
        assert_eq!(
            parse_installer_stage_probe("missing").unwrap(),
            InstallerStageState::Missing
        );
        assert_eq!(
            parse_installer_stage_probe("discard").unwrap(),
            InstallerStageState::Discard
        );
        assert_eq!(
            parse_installer_stage_probe("partial 42").unwrap(),
            InstallerStageState::Partial(42)
        );
    }

    #[test]
    fn resume_command_appends_and_verifies_partial() {
        let command = build_remote_installer_resume_command(100, "abc123", 40);

        assert!(command.contains("expected_offset='40'"));
        assert!(command.contains("cat >> \"$partial\""));
        assert!(command.contains("sha256sum"));
        assert!(command.contains("shasum -a 256"));
        assert!(command.contains("mv \"$partial\" \"$final_path\""));
    }
}
