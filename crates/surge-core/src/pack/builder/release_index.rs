use crate::config::constants::{RELEASES_FILE_COMPRESSED, SCHEMA_VERSION};
use crate::error::{Result, SurgeError};
use crate::releases::manifest::{
    DeltaArtifact, PATCH_FORMAT_SPARSE_FILE_OPS_V1, ReleaseEntry, ReleaseIndex, compress_release_index,
    decompress_release_index,
};

use super::PackBuilder;

impl PackBuilder {
    pub(super) async fn update_release_index(&self, channel: &str) -> Result<()> {
        let mut index = match self.storage.get_object(RELEASES_FILE_COMPRESSED).await {
            Ok(data) => decompress_release_index(&data)?,
            Err(SurgeError::NotFound(_)) => ReleaseIndex {
                schema: SCHEMA_VERSION,
                app_id: self.app_id.clone(),
                pack_id: String::new(),
                last_write_utc: String::new(),
                releases: Vec::new(),
            },
            Err(e) => return Err(e),
        };

        let full = self.artifacts.iter().find(|a| !a.is_delta);
        let delta = self.artifacts.iter().find(|a| a.is_delta);

        let entry = ReleaseEntry {
            version: self.version.clone(),
            channels: vec![channel.to_string()],
            os: detect_os_from_rid(&self.rid),
            rid: self.rid.clone(),
            is_genesis: index.releases.is_empty(),
            full_filename: full.map_or(String::new(), |a| a.filename.clone()),
            full_size: full.map_or(0, |a| a.size),
            full_sha256: full.map_or(String::new(), |a| a.sha256.clone()),
            deltas: Vec::new(),
            preferred_delta_id: String::new(),
            created_utc: chrono::Utc::now().to_rfc3339(),
            release_notes: String::new(),
            name: self.name.clone(),
            main_exe: self.main_exe.clone(),
            install_directory: self.install_directory.clone(),
            supervisor_id: self.supervisor_id.clone(),
            icon: self.icon.clone(),
            shortcuts: self.shortcuts.clone(),
            persistent_assets: self.persistent_assets.clone(),
            installers: self.installers.clone(),
            environment: self.environment.clone(),
        };
        let mut entry = entry;
        let primary_delta = delta.map(|artifact| {
            if artifact
                .patch_format
                .eq_ignore_ascii_case(PATCH_FORMAT_SPARSE_FILE_OPS_V1)
            {
                DeltaArtifact::sparse_file_ops_zstd(
                    "primary",
                    &artifact.from_version,
                    &artifact.filename,
                    artifact.size,
                    &artifact.sha256,
                )
            } else {
                DeltaArtifact::with_patch_format(
                    "primary",
                    &artifact.from_version,
                    &artifact.patch_format,
                    &artifact.filename,
                    artifact.size,
                    &artifact.sha256,
                )
            }
        });
        entry.set_primary_delta(primary_delta);

        index
            .releases
            .retain(|r| !(r.version == self.version && r.rid == self.rid));
        index.releases.push(entry);

        index.last_write_utc = chrono::Utc::now().to_rfc3339();

        let budget = self.ctx.resource_budget();
        let compressed = compress_release_index(&index, budget.zstd_compression_level)?;
        self.storage
            .put_object(RELEASES_FILE_COMPRESSED, &compressed, "application/octet-stream")
            .await?;

        Ok(())
    }
}

pub(super) fn detect_os_from_rid(rid: &str) -> String {
    rid.split('-').next().unwrap_or("unknown").to_string()
}
