use std::collections::BTreeSet;
use std::path::Path;

use crate::archive::packer::ArchivePacker;
use crate::crypto::sha256::sha256_hex;
use crate::error::{Result, SurgeError};

use super::staging::materialize_canonical_pack_root;
use super::toolchain::{
    find_supervisor_binary, resolve_surge_dotnet_native_runtime_bundle, supervisor_binary_name_for_rid,
};
use super::{BundledArtifact, PackBuilder, PackageArtifact};

impl PackBuilder {
    pub(super) fn build_full_package(&mut self) -> Result<PackageArtifact> {
        let budget = self.ctx.resource_budget();
        let filename = format!("{}-{}-{}-full.tar.zst", self.app_id, self.version, self.rid);
        let n_workers = budget.effective_zstd_workers();
        let mut bundled = Vec::new();

        if !self.supervisor_id.trim().is_empty() {
            let supervisor_name = supervisor_binary_name_for_rid(&self.rid);
            if !self.artifacts_dir.join(supervisor_name).is_file() {
                bundled.push(BundledArtifact {
                    source: find_supervisor_binary(supervisor_name)?,
                    archive_name: supervisor_name.to_string(),
                });
            }
        }

        if let Some(native_runtime) = resolve_surge_dotnet_native_runtime_bundle(&self.artifacts_dir, &self.rid)? {
            bundled.push(native_runtime);
        }

        let staging_root = if bundled.is_empty() {
            None
        } else {
            Some(materialize_canonical_pack_root(&self.artifacts_dir, &bundled)?)
        };
        let pack_root = staging_root
            .as_ref()
            .map_or(self.artifacts_dir.as_path(), tempfile::TempDir::path);

        let compression_level = budget.zstd_compression_level;
        let mut packer = ArchivePacker::with_threads(compression_level, n_workers)?;
        let executable_paths = self.executable_archive_paths();
        if executable_paths.is_empty() {
            packer.add_directory(pack_root, "")?;
        } else {
            packer.add_directory_with_executable_overrides(pack_root, "", &executable_paths)?;
        }

        let archive_bytes = packer.finalize()?;
        let sha256 = sha256_hex(&archive_bytes);
        let size = i64::try_from(archive_bytes.len())
            .map_err(|_| SurgeError::Archive(format!("Archive is too large: {} bytes", archive_bytes.len())))?;

        Ok(PackageArtifact {
            filename,
            size,
            sha256,
            is_delta: false,
            from_version: String::new(),
            patch_format: String::new(),
            zstd_compression_level: compression_level,
            zstd_workers: n_workers,
            bytes: archive_bytes,
        })
    }

    fn executable_archive_paths(&self) -> BTreeSet<String> {
        if !rid_uses_unix_executable_bits(&self.rid) {
            return BTreeSet::new();
        }

        let mut paths = BTreeSet::new();
        let main_exe = self.main_exe.trim();
        if !main_exe.is_empty() && self.artifacts_dir.join(main_exe).is_file() {
            paths.insert(archive_path_string(main_exe));
        }

        if !self.supervisor_id.trim().is_empty() {
            let supervisor_name = supervisor_binary_name_for_rid(&self.rid);
            if self.artifacts_dir.join(supervisor_name).is_file() {
                paths.insert(archive_path_string(supervisor_name));
            }
        }

        paths
    }
}

fn rid_uses_unix_executable_bits(rid: &str) -> bool {
    let rid = rid.to_ascii_lowercase();
    rid.starts_with("linux-") || rid.starts_with("osx-") || rid.starts_with("macos-")
}

fn archive_path_string(path: &str) -> String {
    Path::new(path).to_string_lossy().replace('\\', "/")
}
