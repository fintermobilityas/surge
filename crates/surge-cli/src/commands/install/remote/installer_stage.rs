use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::Duration;

use super::execution::{
    REMOTE_INSTALLER_FINAL_PATH, REMOTE_INSTALLER_PARTIAL_PATH, run_tailscale_capture,
    stream_file_to_tailscale_node_with_command_from_offset_timeout,
};
use super::{Result, SurgeError, logline, shell_single_quote};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InstallerStageState {
    Complete,
    Missing,
    Partial(u64),
    Discard,
}

const INSTALLER_STAGE_CHUNK_BYTES: u64 = 256 * 1024;
const INSTALLER_STAGE_CONFIRMATION_TIMEOUT: Duration = Duration::from_secs(90);

pub(crate) async fn stage_installer_file_for_tailscale(
    ssh_target: &str,
    file_target: &str,
    installer_path: &Path,
    expected_size: u64,
    expected_sha256: &str,
) -> Result<()> {
    cleanup_remote_installer_transfer_helpers(ssh_target, file_target).await?;
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
            stream_installer_chunks_for_tailscale(
                ssh_target,
                file_target,
                installer_path,
                expected_size,
                expected_sha256,
                offset,
            )
            .await
        }
        InstallerStageState::Missing => {
            logline::info(&format!("Creating remote installer stage on '{file_target}'."));
            stream_installer_chunks_for_tailscale(
                ssh_target,
                file_target,
                installer_path,
                expected_size,
                expected_sha256,
                0,
            )
            .await
        }
        InstallerStageState::Discard => {
            logline::warn(&format!(
                "Discarding incompatible remote installer stage on '{file_target}' and uploading from the beginning."
            ));
            stream_installer_chunks_for_tailscale(
                ssh_target,
                file_target,
                installer_path,
                expected_size,
                expected_sha256,
                0,
            )
            .await
        }
    }
}

async fn stream_installer_chunks_for_tailscale(
    ssh_target: &str,
    file_target: &str,
    installer_path: &Path,
    expected_size: u64,
    expected_sha256: &str,
    mut offset: u64,
) -> Result<()> {
    while offset < expected_size {
        let next_offset = offset.saturating_add(INSTALLER_STAGE_CHUNK_BYTES).min(expected_size);
        let chunk_len = next_offset.saturating_sub(offset);
        let chunk = create_installer_chunk(installer_path, offset, chunk_len)?;
        let command = build_remote_installer_chunk_command(expected_size, expected_sha256, offset, next_offset);
        logline::subtle(&format!(
            "Uploading staged installer chunk to '{file_target}' ({next_offset}/{expected_size} bytes)."
        ));
        stream_file_to_tailscale_node_with_command_from_offset_timeout(
            ssh_target,
            chunk.path(),
            &command,
            0,
            INSTALLER_STAGE_CONFIRMATION_TIMEOUT,
        )
        .await?;
        offset = next_offset;
    }
    Ok(())
}

fn create_installer_chunk(installer_path: &Path, offset: u64, len: u64) -> Result<tempfile::NamedTempFile> {
    let mut source = std::fs::File::open(installer_path)?;
    source.seek(SeekFrom::Start(offset))?;
    let len = usize::try_from(len)
        .map_err(|_| SurgeError::Platform(format!("Installer chunk length is too large: {len} bytes")))?;
    let mut bytes = vec![0_u8; len];
    source.read_exact(&mut bytes)?;

    let mut chunk = tempfile::NamedTempFile::new()?;
    chunk.write_all(&bytes)?;
    chunk.flush()?;
    Ok(chunk)
}

async fn cleanup_remote_installer_transfer_helpers(ssh_target: &str, file_target: &str) -> Result<()> {
    let command = format!(
        "sh -c {}",
        shell_single_quote(&build_remote_installer_transfer_cleanup_command())
    );
    let output = run_tailscale_capture(&["ssh", ssh_target, command.as_str()]).await?;
    if output.trim() == "cleaned" {
        logline::subtle(&format!(
            "Cleared stale remote installer transfer helpers on '{file_target}'."
        ));
    }
    Ok(())
}

fn build_remote_installer_transfer_cleanup_command() -> String {
    "set -eu; \
partial=/tmp/.surge-installer.partial; \
pattern='[.]surge-installer|[.]surge-transfer-stage'; \
pids=\"$(ps -eo pid=,ppid=,args= | awk -v self=\"$$\" -v parent=\"$PPID\" -v pattern=\"$pattern\" '$1 != self && $1 != parent && $0 ~ pattern { print $1 }')\"; \
if [ -n \"$pids\" ]; then kill $pids 2>/dev/null || true; cleaned=1; else cleaned=0; fi; \
rm -f \"$partial\".chunk.*; \
if [ \"$cleaned\" = 1 ]; then echo cleaned; else echo none; fi"
        .to_string()
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
final_path={REMOTE_INSTALLER_FINAL_PATH}; partial={REMOTE_INSTALLER_PARTIAL_PATH}; meta=\"$partial.meta\"; \
expected_size='{expected_size}'; expected_sha256='{expected_sha256}'; \
hash_file() {{ if command -v sha256sum >/dev/null 2>&1; then sha256sum \"$1\" | awk '{{print $1}}'; \
elif command -v shasum >/dev/null 2>&1; then shasum -a 256 \"$1\" | awk '{{print $1}}'; \
else return 1; fi; }}; \
if [ -f \"$final_path\" ]; then \
  final_size=\"$(wc -c < \"$final_path\" | tr -d '[:space:]')\"; \
  if [ \"$final_size\" = \"$expected_size\" ] && [ \"$(hash_file \"$final_path\")\" = \"$expected_sha256\" ]; then echo complete; exit 0; fi; \
fi; \
if [ -f \"$partial\" ]; then \
  if [ ! -f \"$meta\" ]; then echo discard; exit 0; fi; \
  meta_size=\"$(awk '{{print $1}}' \"$meta\")\"; meta_sha=\"$(awk '{{print $2}}' \"$meta\")\"; \
  if [ \"$meta_size\" != \"$expected_size\" ] || [ \"$meta_sha\" != \"$expected_sha256\" ]; then echo discard; exit 0; fi; \
  partial_size=\"$(wc -c < \"$partial\" | tr -d '[:space:]')\"; \
  if [ \"$partial_size\" -gt \"$expected_size\" ]; then echo discard; exit 0; fi; \
  echo \"partial $partial_size\"; exit 0; \
fi; \
echo missing"
    )
}

fn build_remote_installer_chunk_command(
    expected_size: u64,
    expected_sha256: &str,
    expected_offset: u64,
    expected_next_offset: u64,
) -> String {
    let expected_sha256 = expected_sha256.trim();
    let expected_chunk_size = expected_next_offset.saturating_sub(expected_offset);
    let reset = if expected_offset == 0 {
        "rm -f \"$partial\" \"$final_path\" \"$meta\"; printf '%s\t%s\n' \"$expected_size\" \"$expected_sha256\" > \"$meta\"; "
    } else {
        "if [ ! -f \"$meta\" ]; then echo 'remote installer partial metadata is missing' >&2; exit 1; fi; \
meta_size=\"$(awk '{print $1}' \"$meta\")\"; meta_sha=\"$(awk '{print $2}' \"$meta\")\"; \
if [ \"$meta_size\" != \"$expected_size\" ] || [ \"$meta_sha\" != \"$expected_sha256\" ]; then \
  echo 'remote installer partial metadata does not match this installer' >&2; rm -f \"$partial\" \"$meta\"; exit 1; \
fi; "
    };
    let finalize = if expected_next_offset == expected_size {
        format!(
            "if command -v sha256sum >/dev/null 2>&1; then actual_sha256=\"$(sha256sum \"$partial\" | awk '{{print $1}}')\"; \
elif command -v shasum >/dev/null 2>&1; then actual_sha256=\"$(shasum -a 256 \"$partial\" | awk '{{print $1}}')\"; \
else echo 'remote host has no sha256sum or shasum command available to verify installer transfer' >&2; exit 1; fi; \
if [ \"$actual_sha256\" != '{expected_sha256}' ]; then \
  echo \"remote installer sha256 mismatch at $partial: expected {expected_sha256}, got $actual_sha256\" >&2; rm -f \"$partial\"; exit 1; \
fi; \
chmod +x \"$partial\"; mv \"$partial\" \"$final_path\"; rm -f \"$meta\""
        )
    } else {
        "echo chunk".to_string()
    };

    format!(
        "set -eu; \
final_path={REMOTE_INSTALLER_FINAL_PATH}; partial={REMOTE_INSTALLER_PARTIAL_PATH}; meta=\"$partial.meta\"; \
expected_size='{expected_size}'; expected_sha256='{expected_sha256}'; expected_offset='{expected_offset}'; expected_next_offset='{expected_next_offset}'; expected_chunk_size='{expected_chunk_size}'; \
chunk=\"$partial.chunk.$expected_offset.$$\"; \
trap 'rm -f \"$chunk\"' EXIT; \
{reset}actual_offset=0; if [ -f \"$partial\" ]; then actual_offset=\"$(wc -c < \"$partial\" | tr -d '[:space:]')\"; fi; \
if [ \"$actual_offset\" != \"$expected_offset\" ]; then \
  echo \"remote installer partial changed before chunk append: expected $expected_offset bytes, got $actual_offset bytes\" >&2; exit 1; \
fi; \
cat > \"$chunk\"; \
chunk_size=\"$(wc -c < \"$chunk\" | tr -d '[:space:]')\"; \
if [ \"$chunk_size\" != \"$expected_chunk_size\" ]; then \
  echo \"remote installer chunk size mismatch at $chunk: expected $expected_chunk_size bytes, got $chunk_size bytes\" >&2; exit 1; \
fi; \
cat \"$chunk\" >> \"$partial\"; \
actual_size=\"$(wc -c < \"$partial\" | tr -d '[:space:]')\"; \
if [ \"$actual_size\" != \"$expected_next_offset\" ]; then \
  echo \"remote installer chunk size mismatch at $partial: expected $expected_next_offset bytes, got $actual_size bytes\" >&2; rm -f \"$partial\"; exit 1; \
fi; \
{finalize}"
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
    fn chunk_command_appends_expected_range_and_finalizes_last_chunk() {
        let command = build_remote_installer_chunk_command(100, "abc123", 40, 80);

        assert!(command.contains("expected_offset='40'"));
        assert!(command.contains("expected_next_offset='80'"));
        assert!(command.contains("cat > \"$chunk\""));
        assert!(command.contains("cat \"$chunk\" >> \"$partial\""));
        assert!(!command.contains("mv \"$partial\" \"$final_path\""));

        let final_command = build_remote_installer_chunk_command(100, "abc123", 80, 100);

        assert!(final_command.contains("sha256sum"));
        assert!(final_command.contains("mv \"$partial\" \"$final_path\""));
    }

    #[test]
    fn cleanup_command_uses_self_safe_transfer_patterns() {
        let command = build_remote_installer_transfer_cleanup_command();

        assert!(command.contains("pattern='[.]surge-installer|[.]surge-transfer-stage'"));
        assert!(command.contains("$1 != self && $1 != parent"));
        assert!(command.contains("kill $pids"));
    }
}
