use std::path::{Path, PathBuf};

use super::{Result, SurgeError};

pub(crate) const REMOTE_STAGE_MANIFEST_FILE: &str = ".surge-stage-manifest.tsv";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemoteStageManifest {
    entries: Vec<RemoteStageEntry>,
}

impl RemoteStageManifest {
    pub(crate) fn build(stage_root: &Path) -> Result<Self> {
        let mut entries = Vec::new();
        collect_entries(stage_root, stage_root, &mut entries)?;
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(Self { entries })
    }

    pub(crate) fn write_to_stage_root(&self, stage_root: &Path) -> Result<PathBuf> {
        let path = stage_root.join(REMOTE_STAGE_MANIFEST_FILE);
        std::fs::write(&path, self.to_tsv())?;
        Ok(path)
    }

    pub(crate) fn entries(&self) -> &[RemoteStageEntry] {
        &self.entries
    }

    fn to_tsv(&self) -> String {
        let mut lines = vec!["# surge remote stage manifest v1".to_string()];
        for entry in &self.entries {
            lines.push(format!("{}\t{}\t{}", entry.sha256, entry.size, entry.path));
        }
        lines.push(String::new());
        lines.join("\n")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemoteStageEntry {
    pub(crate) path: String,
    size: u64,
    sha256: String,
}

fn collect_entries(root: &Path, dir: &Path, entries: &mut Vec<RemoteStageEntry>) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            collect_entries(root, &path, entries)?;
            continue;
        }
        if !file_type.is_file() {
            continue;
        }
        let rel = path.strip_prefix(root).map_err(|e| {
            SurgeError::Platform(format!(
                "Failed to relativize staged path '{}' under '{}': {e}",
                path.display(),
                root.display()
            ))
        })?;
        if rel == Path::new(REMOTE_STAGE_MANIFEST_FILE) {
            continue;
        }
        let rel = rel.to_string_lossy().replace('\\', "/");
        if rel.contains('\n') || rel.contains('\t') {
            return Err(SurgeError::Config(format!(
                "Remote stage path '{rel}' cannot contain tabs or newlines"
            )));
        }
        let metadata = entry.metadata()?;
        entries.push(RemoteStageEntry {
            path: rel,
            size: metadata.len(),
            sha256: surge_core::crypto::sha256::sha256_hex_file(&path)?,
        });
    }
    Ok(())
}

pub(crate) fn build_remote_stage_verify_command(install_root: &Path, expected_manifest_sha256: &str) -> String {
    let install_root = super::shell_single_quote(&install_root.to_string_lossy());
    let expected_manifest_sha256 = expected_manifest_sha256.trim();
    format!(
        "set -eu; \
install_root={install_root}; \
stage_dir=\"$install_root/.surge-transfer-stage\"; \
manifest=\"$stage_dir/{REMOTE_STAGE_MANIFEST_FILE}\"; \
if [ ! -d \"$stage_dir\" ] || [ ! -f \"$manifest\" ]; then echo STAGE_MISSING; exit 0; fi; \
if command -v sha256sum >/dev/null 2>&1; then hash_cmd='sha256sum'; \
elif command -v shasum >/dev/null 2>&1; then hash_cmd='shasum -a 256'; \
else echo STAGE_UNVERIFIABLE; exit 0; fi; \
manifest_sha=\"$($hash_cmd \"$manifest\" | awk '{{print $1}}')\"; \
if [ \"$manifest_sha\" != '{expected_manifest_sha256}' ]; then echo STAGE_MISSING; exit 0; fi; \
missing=0; \
while IFS='	' read -r expected_sha expected_size rel; do \
  case \"$expected_sha\" in ''|'#'*) continue ;; esac; \
  path=\"$stage_dir/$rel\"; \
  if [ ! -f \"$path\" ]; then echo \"MISSING	$rel\"; missing=1; continue; fi; \
  actual_size=\"$(wc -c < \"$path\" | tr -d '[:space:]')\"; \
  if [ \"$actual_size\" != \"$expected_size\" ]; then echo \"INVALID	$rel\"; missing=1; continue; fi; \
  actual_sha=\"$($hash_cmd \"$path\" | awk '{{print $1}}')\"; \
  if [ \"$actual_sha\" != \"$expected_sha\" ]; then echo \"INVALID	$rel\"; missing=1; continue; fi; \
done < \"$manifest\"; \
if [ \"$missing\" = 0 ]; then echo STAGE_READY; fi"
    )
}

pub(crate) fn build_remote_stage_prepare_command(install_root: &Path, discard_existing: bool) -> String {
    let install_root = super::shell_single_quote(&install_root.to_string_lossy());
    let cleanup = if discard_existing {
        "rm -rf \"$stage_dir\"; "
    } else {
        ""
    };
    format!(
        "set -eu; \
command -v tar >/dev/null 2>&1 || {{ echo 'Remote host is missing tar' >&2; exit 1; }}; \
install_root={install_root}; \
stage_dir=\"$install_root/.surge-transfer-stage\"; \
mkdir -p \"$install_root\"; {cleanup}mkdir -p \"$stage_dir\"; \
tar -C \"$stage_dir\" -xf -"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stage_manifest_lists_files_with_hashes() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let root = temp_dir.path();
        std::fs::create_dir_all(root.join("app")).expect("app dir");
        std::fs::write(root.join("app").join("demo"), b"demo").expect("demo file");
        std::fs::write(root.join(".surge-staged-release.json"), b"{}").expect("marker");
        std::fs::write(root.join(REMOTE_STAGE_MANIFEST_FILE), b"old").expect("old manifest");

        let manifest = RemoteStageManifest::build(root).expect("manifest");
        let paths: Vec<_> = manifest.entries().iter().map(|entry| entry.path.as_str()).collect();

        assert_eq!(paths, vec![".surge-staged-release.json", "app/demo"]);
    }

    #[test]
    fn verify_command_reports_ready_or_missing_entries() {
        let command = build_remote_stage_verify_command(Path::new("/tmp/demo app"), "abc123");

        assert!(command.contains(".surge-transfer-stage"));
        assert!(command.contains(REMOTE_STAGE_MANIFEST_FILE));
        assert!(command.contains("abc123"));
        assert!(command.contains("STAGE_READY"));
        assert!(command.contains("MISSING"));
        assert!(command.contains("INVALID"));
        assert!(command.contains("sha256sum"));
        assert!(command.contains("shasum -a 256"));
    }
}
