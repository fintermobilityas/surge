//! Channel management for release promotion and demotion.

use std::sync::Arc;

use crate::config::constants::RELEASES_FILE_COMPRESSED;
use crate::context::Context;
use crate::error::{Result, SurgeError};
use crate::releases::manifest::{ReleaseEntry, ReleaseIndex, compress_release_index, decompress_release_index};

/// Manages release channels: fetching, saving, promoting, and demoting releases.
pub struct ChannelManager {
    ctx: Arc<Context>,
    storage: Box<dyn crate::storage::StorageBackend>,
}

impl ChannelManager {
    /// Create a new channel manager.
    pub fn new(ctx: Arc<Context>, storage: Box<dyn crate::storage::StorageBackend>) -> Self {
        Self { ctx, storage }
    }

    /// Download and decompress the release index from storage.
    pub async fn fetch_index(&self) -> Result<ReleaseIndex> {
        self.ctx.check_cancelled()?;

        let data = self.storage.get_object(RELEASES_FILE_COMPRESSED).await?;
        decompress_release_index(&data)
    }

    /// Compress and upload the release index to storage.
    pub async fn save_index(&self, index: &ReleaseIndex) -> Result<()> {
        self.ctx.check_cancelled()?;

        let budget = self.ctx.resource_budget();
        let compressed = compress_release_index(index, budget.zstd_compression_level)?;
        self.storage
            .put_object(RELEASES_FILE_COMPRESSED, &compressed, "application/octet-stream")
            .await?;
        Ok(())
    }

    /// Promote a release version from one channel to another.
    ///
    /// Adds `target_channel` to the release's channel list if it is currently
    /// on `source_channel`. Returns an error if the version is not found or
    /// not on the source channel.
    pub async fn promote(&self, version: &str, source_channel: &str, target_channel: &str) -> Result<()> {
        self.ctx.check_cancelled()?;

        let mut index = self.fetch_index().await?;
        let mut found = false;

        for release in &mut index.releases {
            if release.version == version {
                if !release.channels.iter().any(|c| c == source_channel) {
                    return Err(SurgeError::Update(format!(
                        "Version {version} is not on channel '{source_channel}'"
                    )));
                }
                if !release.channels.iter().any(|c| c == target_channel) {
                    release.channels.push(target_channel.to_string());
                }
                found = true;
                break;
            }
        }

        if !found {
            return Err(SurgeError::NotFound(format!(
                "Version {version} not found in release index"
            )));
        }

        index.last_write_utc = chrono::Utc::now().to_rfc3339();
        self.save_index(&index).await?;
        Ok(())
    }

    /// Demote a release version by removing it from a channel.
    ///
    /// Removes `channel` from the release's channel list. Returns an error
    /// if the version is not found or not on the channel.
    pub async fn demote(&self, version: &str, channel: &str) -> Result<()> {
        self.ctx.check_cancelled()?;

        let mut index = self.fetch_index().await?;
        let mut found = false;

        for release in &mut index.releases {
            if release.version == version {
                let before_len = release.channels.len();
                release.channels.retain(|c| c != channel);
                if release.channels.len() == before_len {
                    return Err(SurgeError::Update(format!(
                        "Version {version} is not on channel '{channel}'"
                    )));
                }
                found = true;
                break;
            }
        }

        if !found {
            return Err(SurgeError::NotFound(format!(
                "Version {version} not found in release index"
            )));
        }

        index.last_write_utc = chrono::Utc::now().to_rfc3339();
        self.save_index(&index).await?;
        Ok(())
    }

    /// Get a list of all distinct channels across all releases in the index.
    #[must_use]
    pub fn list_channels(index: &ReleaseIndex) -> Vec<String> {
        let mut channels: Vec<String> = index.releases.iter().flat_map(|r| r.channels.iter().cloned()).collect();
        channels.sort();
        channels.dedup();
        channels
    }

    /// Get all releases on a specific channel, sorted by version (oldest first).
    #[must_use]
    pub fn list_releases<'a>(index: &'a ReleaseIndex, channel: &str) -> Vec<&'a ReleaseEntry> {
        use crate::releases::version::compare_versions;

        let mut releases: Vec<&ReleaseEntry> = index
            .releases
            .iter()
            .filter(|r| r.channels.iter().any(|c| c == channel))
            .collect();

        releases.sort_by(|a, b| compare_versions(&a.version, &b.version));
        releases
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::releases::manifest::ReleaseEntry;

    fn make_entry(version: &str, channels: &[&str]) -> ReleaseEntry {
        ReleaseEntry {
            version: version.to_string(),
            channels: channels.iter().map(|s| (*s).to_string()).collect(),
            os: "linux".to_string(),
            rid: "linux-x64".to_string(),
            is_genesis: false,
            full_filename: format!("app-{version}-full.tar.zst"),
            full_size: 1000,
            full_sha256: "abc123".to_string(),
            delta_filename: String::new(),
            delta_size: 0,
            delta_sha256: String::new(),
            created_utc: "2025-01-01T00:00:00Z".to_string(),
            release_notes: String::new(),
            name: String::new(),
            main_exe: "test-app".to_string(),
            install_directory: "test-app".to_string(),
            supervisor_id: String::new(),
            icon: String::new(),
            shortcuts: Vec::new(),
            persistent_assets: Vec::new(),
            installers: Vec::new(),
            environment: std::collections::BTreeMap::new(),
        }
    }

    fn make_index(entries: Vec<ReleaseEntry>) -> ReleaseIndex {
        ReleaseIndex {
            schema: 1,
            app_id: "test-app".to_string(),
            pack_id: "test-pack".to_string(),
            last_write_utc: "2025-01-01T00:00:00Z".to_string(),
            releases: entries,
        }
    }

    #[test]
    fn test_list_channels() {
        let index = make_index(vec![
            make_entry("1.0.0", &["stable"]),
            make_entry("1.1.0", &["stable", "beta"]),
            make_entry("2.0.0", &["beta", "alpha"]),
        ]);

        let channels = ChannelManager::list_channels(&index);
        assert_eq!(channels, vec!["alpha", "beta", "stable"]);
    }

    #[test]
    fn test_list_channels_empty() {
        let index = make_index(vec![]);
        let channels = ChannelManager::list_channels(&index);
        assert!(channels.is_empty());
    }

    #[test]
    fn test_list_releases_on_channel() {
        let index = make_index(vec![
            make_entry("1.2.0", &["stable"]),
            make_entry("1.0.0", &["stable"]),
            make_entry("2.0.0", &["beta"]),
            make_entry("1.1.0", &["stable", "beta"]),
        ]);

        let stable = ChannelManager::list_releases(&index, "stable");
        assert_eq!(stable.len(), 3);
        assert_eq!(stable[0].version, "1.0.0");
        assert_eq!(stable[1].version, "1.1.0");
        assert_eq!(stable[2].version, "1.2.0");

        let beta = ChannelManager::list_releases(&index, "beta");
        assert_eq!(beta.len(), 2);
        assert_eq!(beta[0].version, "1.1.0");
        assert_eq!(beta[1].version, "2.0.0");
    }

    #[test]
    fn test_list_releases_unknown_channel() {
        let index = make_index(vec![make_entry("1.0.0", &["stable"])]);
        let releases = ChannelManager::list_releases(&index, "nightly");
        assert!(releases.is_empty());
    }
}
