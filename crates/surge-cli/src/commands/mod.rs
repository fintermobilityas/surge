pub mod compact;
pub mod demote;
pub mod init;
pub mod install;
pub mod list;
pub mod lock;
pub mod migrate;
pub mod pack;
pub mod promote;
pub mod push;
pub mod restore;
pub mod setup;
pub mod tune;

pub(crate) use surge_core::storage_config::{
    append_prefix, build_app_scoped_storage_config, build_app_scoped_storage_context, parse_storage_provider,
};

#[allow(unused_imports)]
pub(crate) use crate::prompts::{resolve_app_id, resolve_app_id_with_rid_hint, resolve_rid};

use surge_core::context::{StorageConfig, StorageProvider};
use surge_core::error::{Result, SurgeError};

pub(crate) fn ensure_mutating_storage_access(config: &StorageConfig, action: &str) -> Result<()> {
    let provider = config
        .provider
        .ok_or_else(|| SurgeError::Config(format!("Cannot {action}: storage provider is not configured")))?;
    let access_present = !config.access_key.trim().is_empty();
    let secret_present = !config.secret_key.trim().is_empty();

    match provider {
        StorageProvider::Filesystem => Ok(()),
        StorageProvider::S3 | StorageProvider::AzureBlob if access_present && secret_present => Ok(()),
        StorageProvider::S3 => Err(SurgeError::Config(format!(
            "Cannot {action}: S3 write access requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"
        ))),
        StorageProvider::AzureBlob => Err(SurgeError::Config(format!(
            "Cannot {action}: Azure Blob write access requires AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY"
        ))),
        StorageProvider::Gcs if secret_present => Ok(()),
        StorageProvider::Gcs => Err(SurgeError::Config(format!(
            "Cannot {action}: GCS write access requires GOOGLE_ACCESS_TOKEN or HMAC credentials"
        ))),
        StorageProvider::GitHubReleases if access_present || secret_present => Ok(()),
        StorageProvider::GitHubReleases => Err(SurgeError::Config(format!(
            "Cannot {action}: GitHub Releases write access requires GITHUB_TOKEN or GH_TOKEN"
        ))),
    }
}

/// Stop a running supervisor by sending SIGTERM (or taskkill on Windows) and
/// waiting for its PID file to disappear. No-op if the supervisor is not running.
pub(crate) async fn stop_supervisor(install_dir: &std::path::Path, supervisor_id: &str) -> Result<()> {
    let supervisor_id = supervisor_id.trim();
    if supervisor_id.is_empty() {
        return Ok(());
    }

    if !install_dir.is_dir() {
        return Ok(());
    }

    let pid_file = install_dir.join(format!(".surge-supervisor-{supervisor_id}.pid"));
    if !pid_file.is_file() {
        return Ok(());
    }

    let pid_str = tokio::fs::read_to_string(&pid_file)
        .await
        .map_err(|e| SurgeError::Config(format!("Failed to read supervisor PID file: {e}")))?;
    let pid: u32 = pid_str
        .trim()
        .parse()
        .map_err(|e| SurgeError::Config(format!("Invalid PID in supervisor PID file: {e}")))?;

    crate::logline::info(&format!(
        "Stopping supervisor '{supervisor_id}' (pid {pid}) before install..."
    ));

    let output = tokio::process::Command::new(if cfg!(unix) { "kill" } else { "taskkill" })
        .args(if cfg!(unix) {
            vec![pid.to_string()]
        } else {
            vec!["/PID".to_string(), pid.to_string()]
        })
        .output()
        .await
        .map_err(|e| {
            SurgeError::Platform(format!(
                "Failed to send terminate signal to supervisor (pid {pid}): {e}"
            ))
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(SurgeError::Platform(format!(
            "Failed to stop supervisor '{supervisor_id}' (pid {pid}): {stderr}"
        )));
    }

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    while pid_file.exists() {
        if tokio::time::Instant::now() >= deadline {
            return Err(SurgeError::Platform(format!(
                "Timed out waiting for supervisor '{supervisor_id}' to exit"
            )));
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    crate::logline::success("Supervisor stopped.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::Path;

    use surge_core::archive::extractor::{list_entries_from_bytes, read_entry};
    use surge_core::archive::packer::ArchivePacker;
    use surge_core::config::constants::{DEFAULT_ZSTD_LEVEL, RELEASES_FILE_COMPRESSED};
    use surge_core::config::manifest::{ShortcutLocation, SurgeManifest};
    use surge_core::context::{StorageConfig, StorageProvider};
    use surge_core::diff::chunked::{ChunkedDiffOptions, chunked_bsdiff};
    use surge_core::diff::wrapper::bsdiff_buffers;
    use surge_core::installer_bundle::read_embedded_payload;
    use surge_core::platform::detect::current_rid;
    use surge_core::platform::fs::make_executable;
    use surge_core::releases::delta::build_archive_chunked_patch;
    use surge_core::releases::manifest::{
        PATCH_FORMAT_CHUNKED_BSDIFF_ARCHIVE_V2, PATCH_FORMAT_CHUNKED_BSDIFF_V1, ReleaseEntry, ReleaseIndex,
        compress_release_index, decompress_release_index,
    };

    fn write_manifest(path: &Path, store_dir: &Path, app_id: &str, rid: &str) {
        let yaml = format!(
            r"schema: 1
storage:
  provider: filesystem
  bucket: {bucket}
apps:
  - id: {app_id}
    name: Test App
    main_exe: demoapp
    channels:
      - stable
      - test
    targets:
      - rid: {rid}
        icon: icon.png
        shortcuts:
          - desktop
          - startup
        installers:
          - online
          - offline
",
            bucket = store_dir.display()
        );
        std::fs::write(path, yaml).unwrap();
    }

    fn write_multi_app_manifest(path: &Path, store_dir: &Path, app_a: &str, app_b: &str, rid: &str) {
        let yaml = format!(
            r"schema: 1
storage:
  provider: filesystem
  bucket: {bucket}
apps:
  - id: {app_a}
    name: App A
    main_exe: demoapp
    targets:
      - rid: {rid}
  - id: {app_b}
    name: App B
    main_exe: demoapp
    targets:
      - rid: {rid}
",
            bucket = store_dir.display()
        );
        std::fs::write(path, yaml).unwrap();
    }

    fn read_index(store_dir: &Path) -> ReleaseIndex {
        let data = std::fs::read(store_dir.join(RELEASES_FILE_COMPRESSED)).unwrap();
        decompress_release_index(&data).unwrap()
    }

    #[test]
    fn test_resolve_app_and_rid_defaults_single_target() {
        let yaml = br"schema: 1
storage:
  provider: filesystem
  bucket: /tmp/releases
apps:
  - id: demoapp
    target:
      rid: linux-x64
";
        let manifest = SurgeManifest::parse(yaml).unwrap();

        assert_eq!(super::resolve_app_id(&manifest, None).unwrap(), "demoapp");
        assert_eq!(super::resolve_rid(&manifest, "demoapp", None).unwrap(), "linux-x64");
    }

    #[test]
    fn test_resolve_app_requires_explicit_value_when_manifest_is_ambiguous() {
        let yaml = br"schema: 1
storage:
  provider: filesystem
  bucket: /tmp/releases
apps:
  - id: demoapp
    target:
      rid: linux-x64
  - id: otherapp
    target:
      rid: linux-x64
";
        let manifest = SurgeManifest::parse(yaml).unwrap();

        let err = super::resolve_app_id(&manifest, None).unwrap_err();
        assert!(err.to_string().contains("Provide --app-id"));
    }

    #[test]
    fn test_resolve_app_with_rid_hint_picks_unique_matching_app() {
        let yaml = br"schema: 1
storage:
  provider: filesystem
  bucket: /tmp/releases
apps:
  - id: app-a
    target:
      rid: linux-x64
  - id: app-b
    target:
      rid: linux-arm64
";
        let manifest = SurgeManifest::parse(yaml).unwrap();

        let app_id = super::resolve_app_id_with_rid_hint(&manifest, None, Some("linux-arm64")).unwrap();
        assert_eq!(app_id, "app-b");
    }

    #[test]
    fn test_resolve_app_with_rid_hint_rejects_ambiguous_match() {
        let yaml = br"schema: 1
storage:
  provider: filesystem
  bucket: /tmp/releases
apps:
  - id: app-a
    target:
      rid: linux-arm64
  - id: app-b
    target:
      rid: linux-arm64
";
        let manifest = SurgeManifest::parse(yaml).unwrap();

        let err = super::resolve_app_id_with_rid_hint(&manifest, None, Some("linux-arm64")).unwrap_err();
        assert!(err.to_string().contains("matches multiple apps"));
        assert!(err.to_string().contains("Provide --app-id"));
    }

    #[test]
    fn test_resolve_app_with_rid_hint_rejects_missing_rid_in_multi_app_manifest() {
        let yaml = br"schema: 1
storage:
  provider: filesystem
  bucket: /tmp/releases
apps:
  - id: app-a
    target:
      rid: linux-x64
  - id: app-b
    target:
      rid: linux-arm64
";
        let manifest = SurgeManifest::parse(yaml).unwrap();

        let err = super::resolve_app_id_with_rid_hint(&manifest, None, Some("win-x64")).unwrap_err();
        assert!(err.to_string().contains("No app in manifest defines target RID"));
        assert!(err.to_string().contains("Provide --app-id"));
    }

    #[test]
    fn test_parse_storage_provider_supports_aliases() {
        use surge_core::context::StorageProvider;
        assert_eq!(
            super::parse_storage_provider("azure_blob").unwrap(),
            StorageProvider::AzureBlob
        );
        assert_eq!(
            super::parse_storage_provider("github-releases").unwrap(),
            StorageProvider::GitHubReleases
        );
        assert_eq!(
            super::parse_storage_provider("fs").unwrap(),
            StorageProvider::Filesystem
        );
    }

    #[test]
    fn test_storage_credentials_resolve_s3_keys() {
        use surge_core::context::StorageProvider;
        use surge_core::storage_config::storage_credentials_from_lookup;
        let mut env = BTreeMap::new();
        env.insert("AWS_ACCESS_KEY_ID".to_string(), "access".to_string());
        env.insert("AWS_SECRET_ACCESS_KEY".to_string(), "secret".to_string());
        let creds = storage_credentials_from_lookup(StorageProvider::S3, |key| env.get(key).cloned());
        assert_eq!(creds.access_key, "access");
        assert_eq!(creds.secret_key, "secret");
    }

    #[test]
    fn test_storage_credentials_resolve_azure_keys() {
        use surge_core::context::StorageProvider;
        use surge_core::storage_config::storage_credentials_from_lookup;
        let mut env = BTreeMap::new();
        env.insert("AZURE_STORAGE_ACCOUNT_NAME".to_string(), "account".to_string());
        env.insert("AZURE_STORAGE_ACCOUNT_KEY".to_string(), "key".to_string());
        let creds = storage_credentials_from_lookup(StorageProvider::AzureBlob, |key| env.get(key).cloned());
        assert_eq!(creds.access_key, "account");
        assert_eq!(creds.secret_key, "key");
    }

    #[test]
    fn test_storage_credentials_resolve_github_token_to_secret_key() {
        use surge_core::context::StorageProvider;
        use surge_core::storage_config::storage_credentials_from_lookup;
        let mut env = BTreeMap::new();
        env.insert("GITHUB_TOKEN".to_string(), "ghp_test".to_string());
        let creds = storage_credentials_from_lookup(StorageProvider::GitHubReleases, |key| env.get(key).cloned());
        assert!(creds.access_key.is_empty());
        assert_eq!(creds.secret_key, "ghp_test");
    }

    #[test]
    fn test_ensure_mutating_storage_access_accepts_azure_account_and_key() {
        let config = StorageConfig {
            provider: Some(StorageProvider::AzureBlob),
            access_key: "account".to_string(),
            secret_key: "key".to_string(),
            ..StorageConfig::default()
        };

        assert!(super::ensure_mutating_storage_access(&config, "compact releases").is_ok());
    }

    #[test]
    fn test_ensure_mutating_storage_access_rejects_azure_without_key() {
        let config = StorageConfig {
            provider: Some(StorageProvider::AzureBlob),
            access_key: "account".to_string(),
            ..StorageConfig::default()
        };

        let err = super::ensure_mutating_storage_access(&config, "compact releases").unwrap_err();
        assert!(err.to_string().contains("AZURE_STORAGE_ACCOUNT_NAME"));
        assert!(err.to_string().contains("AZURE_STORAGE_ACCOUNT_KEY"));
    }

    #[test]
    fn test_ensure_mutating_storage_access_accepts_gcs_oauth_token() {
        let config = StorageConfig {
            provider: Some(StorageProvider::Gcs),
            secret_key: "ya29.token".to_string(),
            ..StorageConfig::default()
        };

        assert!(super::ensure_mutating_storage_access(&config, "compact releases").is_ok());
    }

    #[test]
    fn test_build_app_scoped_storage_config_scopes_only_for_multi_app_manifest() {
        let single_yaml = br"schema: 1
storage:
  provider: filesystem
  bucket: /tmp/releases
  prefix: base
apps:
  - id: app-a
    target:
      rid: linux-x64
";
        let single = SurgeManifest::parse(single_yaml).unwrap();
        let single_cfg = super::build_app_scoped_storage_config(&single, "app-a").unwrap();
        assert_eq!(single_cfg.prefix, "base");

        let multi_yaml = br"schema: 1
storage:
  provider: filesystem
  bucket: /tmp/releases
  prefix: base
apps:
  - id: app-a
    target:
      rid: linux-x64
  - id: app-b
    target:
      rid: linux-x64
";
        let multi = SurgeManifest::parse(multi_yaml).unwrap();
        let multi_cfg = super::build_app_scoped_storage_config(&multi, "app-a").unwrap();
        assert_eq!(multi_cfg.prefix, "base/app-a");
    }

    fn create_stub_installer_launcher(dir: &Path, rid: &str) -> std::path::PathBuf {
        let ext = if rid.starts_with("win-") { ".exe" } else { "" };
        let stub_path = dir.join(format!("surge-installer{ext}"));
        std::fs::write(&stub_path, b"stub-launcher-bytes").unwrap();
        make_executable(&stub_path).unwrap();
        stub_path
    }

    fn read_installer_payload(path: &Path) -> Vec<u8> {
        read_embedded_payload(path).unwrap()
    }

    #[tokio::test]
    async fn test_pack_push_promote_demote_smoke() {
        let tmp = tempfile::tempdir().unwrap();
        let rid = current_rid();
        let stub = create_stub_installer_launcher(tmp.path(), &rid);
        super::pack::set_surge_installer_launcher_override_for_test(&stub);

        let store_dir = tmp.path().join("store");
        let artifacts_dir = tmp.path().join("artifacts");
        let packages_dir = tmp.path().join("packages");
        let manifest_path = tmp.path().join("surge.yml");
        let app_id = "smoke-app";
        let version = "1.0.0";

        std::fs::create_dir_all(&store_dir).unwrap();
        std::fs::create_dir_all(&artifacts_dir).unwrap();
        std::fs::create_dir_all(&packages_dir).unwrap();
        std::fs::write(artifacts_dir.join("payload.txt"), b"smoke payload").unwrap();
        std::fs::write(artifacts_dir.join("demoapp"), b"#!/bin/sh\necho smoke\n").unwrap();
        make_executable(&artifacts_dir.join("demoapp")).unwrap();
        std::fs::write(artifacts_dir.join("icon.png"), b"icon").unwrap();
        write_manifest(&manifest_path, &store_dir, app_id, &rid);

        super::pack::execute(
            &manifest_path,
            Some(app_id),
            version,
            Some(&rid),
            Some(&artifacts_dir),
            &packages_dir,
        )
        .await
        .unwrap();

        let full_package = packages_dir.join(format!("{app_id}-{version}-{rid}-full.tar.zst"));
        assert!(full_package.exists());
        let installers_dir = packages_dir
            .parent()
            .unwrap()
            .join("installers")
            .join(app_id)
            .join(&rid);
        let installer_ext = if rid.starts_with("win-") { "exe" } else { "bin" };
        let online_installer = installers_dir.join(format!("Setup-{rid}-{app_id}-stable-online.{installer_ext}"));
        let offline_installer = installers_dir.join(format!("Setup-{rid}-{app_id}-stable-offline.{installer_ext}"));
        assert!(online_installer.exists());
        assert!(offline_installer.exists());

        let online_data = read_installer_payload(&online_installer);
        let online_entries = list_entries_from_bytes(&online_data).unwrap();
        assert!(
            online_entries
                .iter()
                .any(|entry| entry.path.to_string_lossy().contains("installer.yml"))
        );
        let online_manifest = String::from_utf8(read_entry(&online_data, "installer.yml").unwrap()).unwrap();
        assert!(online_manifest.contains("installer_type: online"));
        assert!(online_manifest.contains("ui: console"));
        assert!(online_manifest.contains("headless_default_if_no_display: true"));

        let offline_data = read_installer_payload(&offline_installer);
        let offline_manifest = String::from_utf8(read_entry(&offline_data, "installer.yml").unwrap()).unwrap();
        assert!(offline_manifest.contains("installer_type: offline"));

        super::push::execute(
            &manifest_path,
            Some(app_id),
            version,
            Some(&rid),
            "stable",
            &packages_dir,
        )
        .await
        .unwrap();

        let index = read_index(&store_dir);
        assert_eq!(index.app_id, app_id);
        assert_eq!(index.releases.len(), 1);
        assert_eq!(index.releases[0].version, version);
        assert_eq!(index.releases[0].rid, rid);
        assert_eq!(index.releases[0].channels, vec!["stable"]);
        assert_eq!(index.releases[0].name, "Test App");
        assert_eq!(index.releases[0].main_exe, "demoapp");
        assert_eq!(index.releases[0].icon, "icon.png");
        assert_eq!(
            index.releases[0].shortcuts,
            vec![ShortcutLocation::Desktop, ShortcutLocation::Startup]
        );

        super::promote::execute(&manifest_path, Some(app_id), version, Some(&rid), "beta")
            .await
            .unwrap();
        let index = read_index(&store_dir);
        assert_eq!(index.releases[0].channels, vec!["beta", "stable"]);

        super::demote::execute(&manifest_path, Some(app_id), version, Some(&rid), "beta")
            .await
            .unwrap();
        let index = read_index(&store_dir);
        assert_eq!(index.releases[0].channels, vec!["stable"]);

        super::list::execute(&manifest_path, Some(app_id), Some(&rid), None)
            .await
            .unwrap();
        super::list::execute(&manifest_path, Some(app_id), Some(&rid), Some("beta"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_push_multi_app_uses_app_scoped_release_index() {
        let tmp = tempfile::tempdir().unwrap();
        let store_dir = tmp.path().join("store");
        let packages_dir = tmp.path().join("packages");
        let manifest_path = tmp.path().join("surge.yml");
        let rid = current_rid();
        let app_a = "multi-app-a";
        let app_b = "multi-app-b";
        let version = "1.0.0";

        std::fs::create_dir_all(&store_dir).unwrap();
        std::fs::create_dir_all(&packages_dir).unwrap();
        write_multi_app_manifest(&manifest_path, &store_dir, app_a, app_b, &rid);

        // Seed unscoped index with another app to ensure scoped push does not collide.
        let root_index = ReleaseIndex {
            app_id: app_b.to_string(),
            releases: vec![ReleaseEntry {
                version: "0.9.0".to_string(),
                channels: vec!["stable".to_string()],
                os: "linux".to_string(),
                rid: rid.clone(),
                is_genesis: true,
                full_filename: format!("{app_b}-0.9.0-{rid}-full.tar.zst"),
                full_size: 1,
                full_sha256: String::new(),
                deltas: Vec::new(),
                preferred_delta_id: String::new(),
                created_utc: String::new(),
                release_notes: String::new(),
                name: String::new(),
                main_exe: "demoapp".to_string(),
                install_directory: "demoapp".to_string(),
                supervisor_id: String::new(),
                icon: String::new(),
                shortcuts: Vec::new(),
                persistent_assets: Vec::new(),
                installers: Vec::new(),
                environment: BTreeMap::new(),
            }],
            ..ReleaseIndex::default()
        };
        let root_bytes = compress_release_index(&root_index, DEFAULT_ZSTD_LEVEL).unwrap();
        std::fs::write(store_dir.join(RELEASES_FILE_COMPRESSED), root_bytes).unwrap();

        let full_package = packages_dir.join(format!("{app_a}-{version}-{rid}-full.tar.zst"));
        std::fs::write(&full_package, b"full-package").unwrap();

        super::push::execute(&manifest_path, Some(app_a), version, Some(&rid), "test", &packages_dir)
            .await
            .unwrap();

        let root_after = read_index(&store_dir);
        assert_eq!(root_after.app_id, app_b);

        let scoped_data = std::fs::read(store_dir.join(app_a).join(RELEASES_FILE_COMPRESSED)).unwrap();
        let scoped_index = decompress_release_index(&scoped_data).unwrap();
        assert_eq!(scoped_index.app_id, app_a);
        assert_eq!(scoped_index.releases.len(), 1);
        assert_eq!(scoped_index.releases[0].version, version);
        assert_eq!(scoped_index.releases[0].channels, vec!["test"]);
        assert_eq!(scoped_index.releases[0].name, "App A");
    }

    #[tokio::test]
    async fn test_push_uploads_delta_only_after_first_full_for_rid() {
        let tmp = tempfile::tempdir().unwrap();
        let store_dir = tmp.path().join("store");
        let packages_dir = tmp.path().join("packages");
        let manifest_path = tmp.path().join("surge.yml");
        let rid = current_rid();
        let app_id = "delta-only-app";
        let v1 = "1.0.0";
        let v2 = "1.0.1";

        std::fs::create_dir_all(&store_dir).unwrap();
        std::fs::create_dir_all(&packages_dir).unwrap();
        write_manifest(&manifest_path, &store_dir, app_id, &rid);

        let v1_full_key = format!("{app_id}-{v1}-{rid}-full.tar.zst");
        std::fs::write(packages_dir.join(&v1_full_key), b"full-v1").unwrap();
        super::push::execute(&manifest_path, Some(app_id), v1, Some(&rid), "stable", &packages_dir)
            .await
            .unwrap();

        let v2_full_key = format!("{app_id}-{v2}-{rid}-full.tar.zst");
        let v2_delta_key = format!("{app_id}-{v2}-{rid}-delta.tar.zst");
        std::fs::write(packages_dir.join(&v2_full_key), b"full-v2").unwrap();
        std::fs::write(packages_dir.join(&v2_delta_key), b"delta-v2").unwrap();
        super::push::execute(&manifest_path, Some(app_id), v2, Some(&rid), "stable", &packages_dir)
            .await
            .unwrap();

        assert!(
            store_dir.join(&v1_full_key).is_file(),
            "base full should remain in storage"
        );
        assert!(
            store_dir.join(&v2_delta_key).is_file(),
            "delta package should be uploaded for subsequent release"
        );
        assert!(
            !store_dir.join(&v2_full_key).exists(),
            "new full package should not be uploaded once a base full exists"
        );

        let index = read_index(&store_dir);
        let v2_entry = index
            .releases
            .iter()
            .find(|release| release.version == v2 && release.rid == rid)
            .expect("v2 release should exist in index");
        assert_eq!(v2_entry.full_filename, v2_full_key);
        assert_eq!(
            v2_entry.selected_delta().expect("v2 should include delta").filename,
            v2_delta_key
        );
    }

    #[tokio::test]
    async fn test_push_records_chunked_delta_patch_format() {
        let tmp = tempfile::tempdir().unwrap();
        let store_dir = tmp.path().join("store");
        let packages_dir = tmp.path().join("packages");
        let manifest_path = tmp.path().join("surge.yml");
        let rid = current_rid();
        let app_id = "chunked-push-app";
        let v1 = "1.0.0";
        let v2 = "1.0.1";

        std::fs::create_dir_all(&store_dir).unwrap();
        std::fs::create_dir_all(&packages_dir).unwrap();
        write_manifest(&manifest_path, &store_dir, app_id, &rid);

        let v1_full_key = format!("{app_id}-{v1}-{rid}-full.tar.zst");
        let v2_full_key = format!("{app_id}-{v2}-{rid}-full.tar.zst");
        let v2_delta_key = format!("{app_id}-{v2}-{rid}-delta.tar.zst");

        let v1_full = b"full-v1".to_vec();
        let v2_full = b"full-v2-but-different".to_vec();
        let v2_patch = chunked_bsdiff(&v1_full, &v2_full, &ChunkedDiffOptions::default()).unwrap();
        let v2_delta = zstd::encode_all(v2_patch.as_slice(), 3).unwrap();

        std::fs::write(packages_dir.join(&v1_full_key), &v1_full).unwrap();
        super::push::execute(&manifest_path, Some(app_id), v1, Some(&rid), "stable", &packages_dir)
            .await
            .unwrap();

        std::fs::write(packages_dir.join(&v2_full_key), &v2_full).unwrap();
        std::fs::write(packages_dir.join(&v2_delta_key), &v2_delta).unwrap();
        super::push::execute(&manifest_path, Some(app_id), v2, Some(&rid), "stable", &packages_dir)
            .await
            .unwrap();

        let index = read_index(&store_dir);
        let v2_entry = index
            .releases
            .iter()
            .find(|release| release.version == v2 && release.rid == rid)
            .expect("v2 release should exist in index");
        let delta = v2_entry.selected_delta().expect("v2 should include delta");
        assert_eq!(delta.filename, v2_delta_key);
        assert_eq!(delta.patch_format, PATCH_FORMAT_CHUNKED_BSDIFF_V1);
    }

    #[tokio::test]
    async fn test_push_records_archive_chunked_delta_patch_format() {
        let tmp = tempfile::tempdir().unwrap();
        let store_dir = tmp.path().join("store");
        let packages_dir = tmp.path().join("packages");
        let manifest_path = tmp.path().join("surge.yml");
        let rid = current_rid();
        let app_id = "archive-chunked-push-app";
        let v1 = "1.0.0";
        let v2 = "1.0.1";

        std::fs::create_dir_all(&store_dir).unwrap();
        std::fs::create_dir_all(&packages_dir).unwrap();
        write_manifest(&manifest_path, &store_dir, app_id, &rid);

        let v1_full_key = format!("{app_id}-{v1}-{rid}-full.tar.zst");
        let v2_full_key = format!("{app_id}-{v2}-{rid}-full.tar.zst");
        let v2_delta_key = format!("{app_id}-{v2}-{rid}-delta.tar.zst");

        let mut packer_v1 = ArchivePacker::new(7).unwrap();
        packer_v1
            .add_buffer("Program.cs", b"Console.WriteLine(\"v1\");\n", 0o644)
            .unwrap();
        packer_v1
            .add_buffer("payload.bin", &vec![b'X'; 1024 * 1024], 0o644)
            .unwrap();
        let v1_full = packer_v1.finalize().unwrap();

        let mut packer_v2 = ArchivePacker::new(7).unwrap();
        packer_v2
            .add_buffer("Program.cs", b"Console.WriteLine(\"v2\");\n", 0o644)
            .unwrap();
        packer_v2
            .add_buffer("payload.bin", &vec![b'X'; 1024 * 1024], 0o644)
            .unwrap();
        let v2_full = packer_v2.finalize().unwrap();

        let v2_patch = build_archive_chunked_patch(&v1_full, &v2_full, 7, &ChunkedDiffOptions::default()).unwrap();
        let v2_delta = zstd::encode_all(v2_patch.as_slice(), 3).unwrap();

        std::fs::write(packages_dir.join(&v1_full_key), &v1_full).unwrap();
        super::push::execute(&manifest_path, Some(app_id), v1, Some(&rid), "stable", &packages_dir)
            .await
            .unwrap();

        std::fs::write(packages_dir.join(&v2_full_key), &v2_full).unwrap();
        std::fs::write(packages_dir.join(&v2_delta_key), &v2_delta).unwrap();
        super::push::execute(&manifest_path, Some(app_id), v2, Some(&rid), "stable", &packages_dir)
            .await
            .unwrap();

        let index = read_index(&store_dir);
        let v2_entry = index
            .releases
            .iter()
            .find(|release| release.version == v2 && release.rid == rid)
            .expect("v2 release should exist in index");
        let delta = v2_entry.selected_delta().expect("v2 should include delta");
        assert_eq!(delta.filename, v2_delta_key);
        assert_eq!(delta.patch_format, PATCH_FORMAT_CHUNKED_BSDIFF_ARCHIVE_V2);
    }

    #[tokio::test]
    async fn test_compact_materializes_latest_full_and_preserves_other_channels() {
        let tmp = tempfile::tempdir().unwrap();
        let store_dir = tmp.path().join("store");
        let packages_dir = tmp.path().join("packages");
        let manifest_path = tmp.path().join("surge.yml");
        let rid = current_rid();
        let app_id = "compact-app";
        let v1 = "1.0.0";
        let v2 = "1.1.0";

        std::fs::create_dir_all(&store_dir).unwrap();
        std::fs::create_dir_all(&packages_dir).unwrap();
        write_manifest(&manifest_path, &store_dir, app_id, &rid);

        let v1_full_key = format!("{app_id}-{v1}-{rid}-full.tar.zst");
        let v2_full_key = format!("{app_id}-{v2}-{rid}-full.tar.zst");
        let v2_delta_key = format!("{app_id}-{v2}-{rid}-delta.tar.zst");

        let v1_full = b"compact-v1-full".to_vec();
        let v2_full = b"compact-v2-full-with-delta".to_vec();
        let v2_patch = bsdiff_buffers(&v1_full, &v2_full).unwrap();
        let v2_delta = zstd::encode_all(v2_patch.as_slice(), 3).unwrap();

        std::fs::write(packages_dir.join(&v1_full_key), &v1_full).unwrap();
        super::push::execute(&manifest_path, Some(app_id), v1, Some(&rid), "stable", &packages_dir)
            .await
            .unwrap();
        super::promote::execute(&manifest_path, Some(app_id), v1, Some(&rid), "test")
            .await
            .unwrap();

        std::fs::write(packages_dir.join(&v2_full_key), &v2_full).unwrap();
        std::fs::write(packages_dir.join(&v2_delta_key), &v2_delta).unwrap();
        super::push::execute(&manifest_path, Some(app_id), v2, Some(&rid), "test", &packages_dir)
            .await
            .unwrap();

        assert!(
            !store_dir.join(&v2_full_key).exists(),
            "delta-only release should not upload its full archive before compaction"
        );
        assert!(
            store_dir.join(&v2_delta_key).is_file(),
            "delta artifact should be present before compaction"
        );

        super::compact::execute(&manifest_path, Some(app_id), Some(&rid), "test")
            .await
            .unwrap();

        let index = read_index(&store_dir);
        assert_eq!(index.releases.len(), 2);

        let v1_entry = index
            .releases
            .iter()
            .find(|release| release.version == v1 && release.rid == rid)
            .expect("v1 release should remain for stable");
        assert_eq!(v1_entry.channels, vec!["stable"]);

        let v2_entry = index
            .releases
            .iter()
            .find(|release| release.version == v2 && release.rid == rid)
            .expect("v2 release should remain for test");
        assert_eq!(v2_entry.channels, vec!["test"]);
        assert!(
            v2_entry.selected_delta().is_none(),
            "compacted latest release should be full-only"
        );

        assert!(
            store_dir.join(&v1_full_key).is_file(),
            "stable baseline full should remain in storage"
        );
        assert!(
            store_dir.join(&v2_full_key).is_file(),
            "compaction should materialize the latest full artifact"
        );
        assert!(
            !store_dir.join(&v2_delta_key).exists(),
            "latest delta should be pruned after compaction"
        );
        assert_eq!(std::fs::read(store_dir.join(&v2_full_key)).unwrap(), v2_full);
    }
}
