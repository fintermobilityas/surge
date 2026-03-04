//! Release index types and operations.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::config::manifest::ShortcutLocation;
use crate::error::{Result, SurgeError};
use crate::releases::version::compare_versions;

/// A single release entry in the release index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseEntry {
    pub version: String,
    #[serde(default)]
    pub channels: Vec<String>,
    #[serde(default)]
    pub os: String,
    #[serde(default)]
    pub rid: String,
    #[serde(default)]
    pub is_genesis: bool,

    #[serde(default)]
    pub full_filename: String,
    #[serde(default)]
    pub full_size: i64,
    #[serde(default)]
    pub full_sha256: String,

    #[serde(default)]
    pub delta_filename: String,
    #[serde(default)]
    pub delta_size: i64,
    #[serde(default)]
    pub delta_sha256: String,

    #[serde(default)]
    pub created_utc: String,
    #[serde(default)]
    pub release_notes: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub main_exe: String,
    #[serde(default)]
    pub install_directory: String,
    #[serde(default)]
    pub supervisor_id: String,
    #[serde(default)]
    pub icon: String,
    #[serde(default)]
    pub shortcuts: Vec<ShortcutLocation>,
    #[serde(default)]
    pub persistent_assets: Vec<String>,
    #[serde(default)]
    pub installers: Vec<String>,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
}

impl ReleaseEntry {
    /// Returns the best display name, falling back through `name → main_exe → app_id`.
    #[must_use]
    pub fn display_name<'a>(&'a self, app_id: &'a str) -> &'a str {
        if !self.name.is_empty() {
            return &self.name;
        }
        if !self.main_exe.is_empty() {
            return &self.main_exe;
        }
        app_id
    }
}

/// The top-level release index (releases.yml).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseIndex {
    #[serde(default = "default_schema")]
    pub schema: i32,
    #[serde(default)]
    pub app_id: String,
    #[serde(default)]
    pub pack_id: String,
    #[serde(default)]
    pub last_write_utc: String,
    #[serde(default)]
    pub releases: Vec<ReleaseEntry>,
}

fn default_schema() -> i32 {
    crate::config::constants::SCHEMA_VERSION
}

impl Default for ReleaseIndex {
    fn default() -> Self {
        Self {
            schema: default_schema(),
            app_id: String::new(),
            pack_id: String::new(),
            last_write_utc: String::new(),
            releases: Vec::new(),
        }
    }
}

/// Parse a release index from YAML bytes.
pub fn parse_release_index(data: &[u8]) -> Result<ReleaseIndex> {
    let index: ReleaseIndex = serde_yaml::from_slice(data)?;
    Ok(index)
}

/// Serialize a release index to YAML bytes.
pub fn serialize_release_index(index: &ReleaseIndex) -> Result<Vec<u8>> {
    let yaml = serde_yaml::to_string(index)?;
    Ok(yaml.into_bytes())
}

/// Serialize a release index and compress with zstd.
pub fn compress_release_index(index: &ReleaseIndex, level: i32) -> Result<Vec<u8>> {
    let yaml_bytes = serialize_release_index(index)?;
    let compressed = zstd::encode_all(yaml_bytes.as_slice(), level)
        .map_err(|e| SurgeError::Archive(format!("Failed to compress release index: {e}")))?;
    Ok(compressed)
}

/// Decompress zstd data and parse as a release index.
pub fn decompress_release_index(data: &[u8]) -> Result<ReleaseIndex> {
    let decompressed =
        zstd::decode_all(data).map_err(|e| SurgeError::Archive(format!("Failed to decompress release index: {e}")))?;
    parse_release_index(&decompressed)
}

/// Get all releases newer than the given version on the specified channel,
/// sorted from oldest to newest.
pub fn get_releases_newer_than<'a>(index: &'a ReleaseIndex, version: &str, channel: &str) -> Vec<&'a ReleaseEntry> {
    let mut newer: Vec<&ReleaseEntry> = index
        .releases
        .iter()
        .filter(|r| {
            r.channels.iter().any(|c| c == channel)
                && compare_versions(&r.version, version) == std::cmp::Ordering::Greater
        })
        .collect();

    newer.sort_by(|a, b| compare_versions(&a.version, &b.version));
    newer
}

/// Find a chain of delta releases from `from` version to `to` version on the
/// given channel. Returns `None` if no valid contiguous delta chain exists.
///
/// The chain is ordered from oldest to newest, where:
/// - Each entry has a non-empty `delta_filename`
/// - Entries are contiguous versions on the channel between `from` and `to`
pub fn get_delta_chain<'a>(
    index: &'a ReleaseIndex,
    from: &str,
    to: &str,
    channel: &str,
) -> Option<Vec<&'a ReleaseEntry>> {
    if compare_versions(from, to) != std::cmp::Ordering::Less {
        return None;
    }

    // Get all releases on this channel, sorted by version
    let mut on_channel: Vec<&ReleaseEntry> = index
        .releases
        .iter()
        .filter(|r| r.channels.iter().any(|c| c == channel))
        .collect();

    on_channel.sort_by(|a, b| compare_versions(&a.version, &b.version));

    // Find releases in range (from, to], i.e., versions strictly greater than
    // `from` and less than or equal to `to`.
    let chain: Vec<&ReleaseEntry> = on_channel
        .into_iter()
        .filter(|r| {
            compare_versions(&r.version, from) == std::cmp::Ordering::Greater
                && compare_versions(&r.version, to) != std::cmp::Ordering::Greater
        })
        .collect();

    if chain.is_empty() {
        return None;
    }

    // Verify the last entry in the chain matches the target version
    if let Some(last) = chain.last()
        && compare_versions(&last.version, to) != std::cmp::Ordering::Equal
    {
        return None;
    }

    // Delta chains require an actual delta file for each step.
    let all_have_deltas = chain.iter().all(|r| !r.delta_filename.is_empty());
    if !all_have_deltas {
        return None;
    }

    Some(chain)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(version: &str, channels: &[&str], has_delta: bool) -> ReleaseEntry {
        ReleaseEntry {
            version: version.to_string(),
            channels: channels.iter().map(|s| (*s).to_string()).collect(),
            os: "linux".to_string(),
            rid: "linux-x64".to_string(),
            is_genesis: false,
            full_filename: format!("app-{version}-full.tar.zst"),
            full_size: 1000,
            full_sha256: "abc123".to_string(),
            delta_filename: if has_delta {
                format!("app-{version}-delta.tar.zst")
            } else {
                String::new()
            },
            delta_size: if has_delta { 200 } else { 0 },
            delta_sha256: if has_delta { "def456".to_string() } else { String::new() },
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
            environment: BTreeMap::new(),
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
    fn test_parse_and_serialize_roundtrip() {
        let index = make_index(vec![
            make_entry("1.0.0", &["stable"], false),
            make_entry("1.1.0", &["stable", "beta"], true),
        ]);

        let yaml = serialize_release_index(&index).unwrap();
        let parsed = parse_release_index(&yaml).unwrap();

        assert_eq!(parsed.app_id, "test-app");
        assert_eq!(parsed.releases.len(), 2);
        assert_eq!(parsed.releases[0].version, "1.0.0");
        assert_eq!(parsed.releases[1].version, "1.1.0");
    }

    #[test]
    fn test_compress_decompress_roundtrip() {
        let index = make_index(vec![make_entry("1.0.0", &["stable"], false)]);

        let compressed = compress_release_index(&index, crate::config::constants::DEFAULT_ZSTD_LEVEL).unwrap();
        assert!(!compressed.is_empty());

        let decompressed = decompress_release_index(&compressed).unwrap();
        assert_eq!(decompressed.app_id, "test-app");
        assert_eq!(decompressed.releases.len(), 1);
    }

    #[test]
    fn test_get_releases_newer_than() {
        let index = make_index(vec![
            make_entry("1.0.0", &["stable"], false),
            make_entry("1.1.0", &["stable"], true),
            make_entry("1.2.0", &["stable"], true),
            make_entry("2.0.0", &["beta"], true),
        ]);

        let newer = get_releases_newer_than(&index, "1.0.0", "stable");
        assert_eq!(newer.len(), 2);
        assert_eq!(newer[0].version, "1.1.0");
        assert_eq!(newer[1].version, "1.2.0");
    }

    #[test]
    fn test_get_releases_newer_than_no_results() {
        let index = make_index(vec![
            make_entry("1.0.0", &["stable"], false),
            make_entry("1.1.0", &["stable"], true),
        ]);

        let newer = get_releases_newer_than(&index, "1.1.0", "stable");
        assert!(newer.is_empty());
    }

    #[test]
    fn test_get_delta_chain_success() {
        let index = make_index(vec![
            make_entry("1.0.0", &["stable"], false),
            make_entry("1.1.0", &["stable"], true),
            make_entry("1.2.0", &["stable"], true),
        ]);

        let chain = get_delta_chain(&index, "1.0.0", "1.2.0", "stable");
        assert!(chain.is_some());
        let chain = chain.unwrap();
        assert_eq!(chain.len(), 2);
        assert_eq!(chain[0].version, "1.1.0");
        assert_eq!(chain[1].version, "1.2.0");
    }

    #[test]
    fn test_get_delta_chain_missing_delta() {
        let index = make_index(vec![
            make_entry("1.0.0", &["stable"], false),
            make_entry("1.1.0", &["stable"], false), // no delta
            make_entry("1.2.0", &["stable"], true),
        ]);

        let chain = get_delta_chain(&index, "1.0.0", "1.2.0", "stable");
        assert!(chain.is_none());
    }

    #[test]
    fn test_get_delta_chain_wrong_direction() {
        let index = make_index(vec![
            make_entry("1.0.0", &["stable"], false),
            make_entry("1.1.0", &["stable"], true),
        ]);

        let chain = get_delta_chain(&index, "1.1.0", "1.0.0", "stable");
        assert!(chain.is_none());
    }

    #[test]
    fn test_get_delta_chain_different_channel() {
        let index = make_index(vec![
            make_entry("1.0.0", &["stable"], false),
            make_entry("1.1.0", &["beta"], true),
            make_entry("1.2.0", &["stable"], true),
        ]);

        // Chain exists on "stable": just [1.2.0] which has a delta.
        // 1.1.0 is excluded because it's not on "stable".
        let chain = get_delta_chain(&index, "1.0.0", "1.2.0", "stable");
        assert!(chain.is_some());
        let chain = chain.unwrap();
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0].version, "1.2.0");
    }

    #[test]
    fn test_get_delta_chain_genesis_without_delta_is_invalid() {
        let mut genesis = make_entry("1.1.0", &["stable"], false);
        genesis.is_genesis = true;

        let index = make_index(vec![genesis]);
        let chain = get_delta_chain(&index, "1.0.0", "1.1.0", "stable");
        assert!(chain.is_none());
    }
}
