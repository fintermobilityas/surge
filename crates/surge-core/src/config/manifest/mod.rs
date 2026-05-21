mod lookup;
mod normalize;
mod types;
mod validate;

use std::path::Path;

use serde_yaml::Value;

use crate::error::{Result, SurgeError};

pub use self::types::{
    AppConfig, CacheManifestConfig, ChannelManifestConfig, GpuCompatibilityConfig, InstallArtifactCacheManifestConfig,
    InstallArtifactCachePolicy, InstallArtifactCacheRetention, InstallerType, LockManifestConfig,
    OsReleaseCompatibilityConfig, PackCompressionFormat, PackCompressionManifestConfig, PackDeltaManifestConfig,
    PackDeltaStrategy, PackManifestConfig, PackPolicy, PackRetentionManifestConfig, ShortcutLocation,
    StorageManifestConfig, SurgeManifest, TargetCompatibilityConfig, TargetConfig,
};

impl SurgeManifest {
    pub fn parse(data: &[u8]) -> Result<Self> {
        let raw: Value = serde_yaml::from_slice(data)?;
        validate::reject_embedded_storage_credentials(&raw)?;
        let mut manifest: Self = serde_yaml::from_value(raw)?;
        manifest.normalize();
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn from_file(path: &Path) -> Result<Self> {
        let data = std::fs::read(path)
            .map_err(|e| SurgeError::Config(format!("Failed to read manifest '{}': {e}", path.display())))?;
        Self::parse(&data)
    }

    pub fn to_yaml(&self) -> Result<Vec<u8>> {
        let manifest_yaml = serde_yaml::to_string(self)?;
        Ok(manifest_yaml.into_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::{InstallArtifactCacheRetention, PackDeltaStrategy, SurgeManifest};
    use crate::config::constants::{
        PACK_DEFAULT_CHECKPOINT_EVERY, PACK_DEFAULT_KEEP_LATEST_FULLS, PACK_DEFAULT_MAX_CHAIN_LENGTH,
        PACK_DEFAULT_ZSTD_LEVEL,
    };

    #[test]
    fn parse_derives_single_target_id_when_missing() {
        let yaml = br"schema: 1
storage:
  provider: filesystem
  bucket: /tmp/store
apps:
  - main: demoapp
    target:
      rid: linux-x64
      distro: ubuntu24.04
      variant: cuda
";

        let manifest = SurgeManifest::parse(yaml).expect("manifest should parse");
        assert_eq!(manifest.apps.len(), 1);
        assert_eq!(manifest.apps[0].id, "demoapp-ubuntu24.04-linux-x64-cuda");
    }

    #[test]
    fn parse_preserves_explicit_single_target_id() {
        let yaml = br"schema: 1
storage:
  provider: filesystem
  bucket: /tmp/store
apps:
  - id: explicit-id
    main: demoapp
    target:
      rid: linux-x64
      distro: ubuntu24.04
";

        let manifest = SurgeManifest::parse(yaml).expect("manifest should parse");
        assert_eq!(manifest.apps.len(), 1);
        assert_eq!(manifest.apps[0].id, "explicit-id");
    }

    #[test]
    fn parse_target_compatibility_rules() {
        let yaml = br#"schema: 1
storage:
  provider: filesystem
  bucket: /tmp/store
apps:
  - id: demo
    target:
      rid: linux-x64
      compatibility:
        os-release:
          id: ubuntu
          version-id: "24.04"
        gpu:
          vendor: nvidia
        files:
          /etc/example_runtime_release: "R35.*"
        packages:
          example-dnn-runtime: "8.4.*"
"#;

        let manifest = SurgeManifest::parse(yaml).expect("manifest should parse");
        let target = manifest
            .find_target("demo", "linux-x64")
            .expect("target should resolve");
        let compatibility = target.compatibility.expect("compatibility should parse");

        assert_eq!(
            compatibility
                .os_release
                .as_ref()
                .and_then(|os_release| os_release.id.as_deref()),
            Some("ubuntu")
        );
        assert_eq!(
            compatibility
                .files
                .get("/etc/example_runtime_release")
                .map(String::as_str),
            Some("R35.*")
        );
        assert_eq!(
            compatibility.packages.get("example-dnn-runtime").map(String::as_str),
            Some("8.4.*")
        );
    }

    #[test]
    fn effective_pack_policy_uses_defaults_when_pack_is_omitted() {
        let yaml = br"schema: 1
storage:
  provider: filesystem
  bucket: /tmp/store
apps:
  - id: demoapp
    target:
      rid: linux-x64
";

        let manifest = SurgeManifest::parse(yaml).expect("manifest should parse");
        let policy = manifest.effective_pack_policy();

        assert_eq!(policy.delta_strategy, PackDeltaStrategy::SparseFileOps);
        assert_eq!(policy.compression_level, PACK_DEFAULT_ZSTD_LEVEL);
        assert_eq!(policy.max_chain_length, PACK_DEFAULT_MAX_CHAIN_LENGTH);
        assert_eq!(policy.keep_latest_fulls, PACK_DEFAULT_KEEP_LATEST_FULLS);
        assert_eq!(policy.checkpoint_every, PACK_DEFAULT_CHECKPOINT_EVERY);
    }

    #[test]
    fn parse_accepts_pack_policy_override() {
        let yaml = br"schema: 1
storage:
  provider: filesystem
  bucket: /tmp/store
pack:
  delta:
    strategy: archive-bsdiff
    max_chain_length: 4
  compression:
    format: zstd
    level: 5
  retention:
    keep_latest_fulls: 3
    checkpoint_every: 9
apps:
  - id: demoapp
    target:
      rid: linux-x64
";

        let manifest = SurgeManifest::parse(yaml).expect("manifest should parse");
        let policy = manifest.effective_pack_policy();

        assert_eq!(policy.delta_strategy, PackDeltaStrategy::ArchiveBsdiff);
        assert_eq!(policy.compression_level, 5);
        assert_eq!(policy.max_chain_length, 4);
        assert_eq!(policy.keep_latest_fulls, 3);
        assert_eq!(policy.checkpoint_every, 9);
    }

    #[test]
    fn parse_accepts_install_artifact_cache_policy_override() {
        let yaml = br"schema: 1
storage:
  provider: filesystem
  bucket: /tmp/store
cache:
  installArtifacts:
    retention: latest_full
    keepFullCount: 1
apps:
  - id: demoapp
    target:
      rid: linux-x64
";

        let manifest = SurgeManifest::parse(yaml).expect("manifest should parse");
        let policy = manifest.effective_install_artifact_cache_policy();

        assert_eq!(policy.retention, InstallArtifactCacheRetention::LatestFull);
        assert_eq!(policy.keep_full_count, 1);
    }

    #[test]
    fn parse_accepts_bounded_install_artifact_cache_policies() {
        for (retention, expected) in [
            ("just_installed", InstallArtifactCacheRetention::JustInstalled),
            ("none", InstallArtifactCacheRetention::None),
        ] {
            let yaml = format!(
                r"schema: 1
storage:
  provider: filesystem
  bucket: /tmp/store
cache:
  installArtifacts:
    retention: {retention}
apps:
  - id: demoapp
    target:
      rid: linux-x64
"
            );

            let manifest = SurgeManifest::parse(yaml.as_bytes()).expect("manifest should parse");
            let policy = manifest.effective_install_artifact_cache_policy();
            assert_eq!(policy.retention, expected);
        }
    }

    #[test]
    fn parse_rejects_zero_install_artifact_cache_keep_full_count() {
        let yaml = br"schema: 1
storage:
  provider: filesystem
  bucket: /tmp/store
cache:
  installArtifacts:
    retention: latest_full
    keepFullCount: 0
apps:
  - id: demoapp
    target:
      rid: linux-x64
";

        let err = SurgeManifest::parse(yaml).expect_err("manifest should be rejected");
        assert!(err.to_string().contains("cache.installArtifacts.keepFullCount"));
    }

    #[test]
    fn parse_rejects_invalid_pack_compression_level() {
        let yaml = br"schema: 1
storage:
  provider: filesystem
  bucket: /tmp/store
pack:
  compression:
    level: 23
apps:
  - id: demoapp
    target:
      rid: linux-x64
";

        let err = SurgeManifest::parse(yaml).expect_err("manifest should be rejected");
        assert!(err.to_string().contains("pack.compression.level"));
    }
}
