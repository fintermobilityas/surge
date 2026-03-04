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

use surge_core::config::manifest::SurgeManifest;
use surge_core::context::{Context, StorageConfig, StorageProvider};
use surge_core::error::{Result, SurgeError};

pub(crate) struct StorageCredentials {
    pub access_key: String,
    pub secret_key: String,
}

pub(crate) fn resolve_app_id(manifest: &SurgeManifest, requested_app_id: Option<&str>) -> Result<String> {
    if let Some(app_id) = requested_app_id.map(str::trim).filter(|value| !value.is_empty()) {
        return Ok(app_id.to_string());
    }

    let app_ids = manifest.app_ids();
    match app_ids.as_slice() {
        [single] => Ok(single.clone()),
        [] => Err(SurgeError::Config(
            "Manifest has no apps. Provide --app-id explicitly.".to_string(),
        )),
        _ => Err(SurgeError::Config(format!(
            "Manifest contains multiple apps ({}). Provide --app-id.",
            app_ids.join(", ")
        ))),
    }
}

pub(crate) fn resolve_app_id_with_rid_hint(
    manifest: &SurgeManifest,
    requested_app_id: Option<&str>,
    requested_rid: Option<&str>,
) -> Result<String> {
    if let Some(app_id) = requested_app_id.map(str::trim).filter(|value| !value.is_empty()) {
        return Ok(app_id.to_string());
    }

    let requested_rid = requested_rid.map(str::trim).filter(|value| !value.is_empty());
    if let Some(rid) = requested_rid {
        let mut candidates: Vec<String> = manifest
            .app_ids()
            .into_iter()
            .filter(|app_id| manifest.target_rids(app_id).iter().any(|target_rid| target_rid == rid))
            .collect();
        candidates.sort();
        candidates.dedup();

        return match candidates.as_slice() {
            [single] => Ok(single.clone()),
            [] => {
                if manifest.apps.len() > 1 {
                    Err(SurgeError::Config(format!(
                        "No app in manifest defines target RID '{rid}'. Provide --app-id."
                    )))
                } else {
                    resolve_app_id(manifest, None)
                }
            }
            _ => Err(SurgeError::Config(format!(
                "RID '{rid}' matches multiple apps ({}). Provide --app-id.",
                candidates.join(", ")
            ))),
        };
    }

    resolve_app_id(manifest, None)
}

pub(crate) fn resolve_rid(manifest: &SurgeManifest, app_id: &str, requested_rid: Option<&str>) -> Result<String> {
    if let Some(rid) = requested_rid.map(str::trim).filter(|value| !value.is_empty()) {
        return Ok(rid.to_string());
    }

    let rids = manifest.target_rids(app_id);
    match rids.as_slice() {
        [single] => Ok(single.clone()),
        [] => Err(SurgeError::Config(format!(
            "App '{app_id}' has no targets. Provide --rid explicitly."
        ))),
        _ => Err(SurgeError::Config(format!(
            "App '{app_id}' has multiple targets ({}). Provide --rid.",
            rids.join(", ")
        ))),
    }
}

pub(crate) fn build_storage_context(manifest: &SurgeManifest) -> Result<Context> {
    let provider = parse_storage_provider(&manifest.storage.provider)?;
    let creds = storage_credentials_from_env(provider);

    let ctx = Context::new();
    ctx.set_storage(
        provider,
        &manifest.storage.bucket,
        &manifest.storage.region,
        &creds.access_key,
        &creds.secret_key,
        &manifest.storage.endpoint,
    );
    ctx.set_storage_prefix(&manifest.storage.prefix);
    Ok(ctx)
}

pub(crate) fn build_storage_config(manifest: &SurgeManifest) -> Result<StorageConfig> {
    Ok(build_storage_context(manifest)?.storage_config())
}

/// Build a storage config for an app.
///
/// For multi-app manifests, storage is scoped by app id to avoid release index
/// collisions. Single-app manifests keep their existing storage prefix.
pub(crate) fn build_app_scoped_storage_config(manifest: &SurgeManifest, app_id: &str) -> Result<StorageConfig> {
    let mut config = build_storage_config(manifest)?;
    if manifest.apps.len() > 1 {
        config.prefix = append_prefix(&config.prefix, app_id);
    }
    Ok(config)
}

/// Build a storage context for an app.
///
/// For multi-app manifests, storage is scoped by app id to avoid release index
/// collisions. Single-app manifests keep their existing storage prefix.
pub(crate) fn build_app_scoped_storage_context(manifest: &SurgeManifest, app_id: &str) -> Result<Context> {
    let ctx = build_storage_context(manifest)?;
    if manifest.apps.len() > 1 {
        let base_prefix = ctx.storage_config().prefix;
        ctx.set_storage_prefix(&append_prefix(&base_prefix, app_id));
    }
    Ok(ctx)
}

pub(crate) fn append_prefix(prefix: &str, segment: &str) -> String {
    let prefix = prefix.trim().trim_matches('/');
    let segment = segment.trim().trim_matches('/');

    if prefix.is_empty() {
        segment.to_string()
    } else if segment.is_empty() {
        prefix.to_string()
    } else {
        format!("{prefix}/{segment}")
    }
}

pub(crate) fn parse_storage_provider(raw: &str) -> Result<StorageProvider> {
    let normalized = raw.trim().to_ascii_lowercase().replace('-', "_");
    let provider = match normalized.as_str() {
        "s3" => StorageProvider::S3,
        "azure" | "azure_blob" | "azureblob" => StorageProvider::AzureBlob,
        "gcs" => StorageProvider::Gcs,
        "filesystem" | "fs" => StorageProvider::Filesystem,
        "github" | "github_releases" | "githubreleases" => StorageProvider::GitHubReleases,
        "" => return Err(SurgeError::Config("Storage provider is required".to_string())),
        other => return Err(SurgeError::Config(format!("Unknown storage provider: {other}"))),
    };
    Ok(provider)
}

pub(crate) fn storage_credentials_from_env(provider: StorageProvider) -> StorageCredentials {
    storage_credentials_from_lookup(provider, |name| std::env::var(name).ok())
}

pub(crate) fn storage_credentials_from_lookup<F>(provider: StorageProvider, mut lookup: F) -> StorageCredentials
where
    F: FnMut(&str) -> Option<String>,
{
    match provider {
        StorageProvider::S3 => StorageCredentials {
            access_key: first_non_empty_env(&mut lookup, &["AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY"]),
            secret_key: first_non_empty_env(&mut lookup, &["AWS_SECRET_ACCESS_KEY", "AWS_SECRET_KEY"]),
        },
        StorageProvider::AzureBlob => StorageCredentials {
            access_key: first_non_empty_env(&mut lookup, &["AZURE_STORAGE_ACCOUNT_NAME", "AZURE_STORAGE_ACCOUNT"]),
            secret_key: first_non_empty_env(&mut lookup, &["AZURE_STORAGE_ACCOUNT_KEY"]),
        },
        StorageProvider::Gcs => StorageCredentials {
            access_key: first_non_empty_env(&mut lookup, &["GCS_ACCESS_KEY_ID", "GCS_ACCESS_KEY"]),
            secret_key: first_non_empty_env(
                &mut lookup,
                &["GCS_SECRET_ACCESS_KEY", "GCS_SECRET_KEY", "GOOGLE_ACCESS_TOKEN"],
            ),
        },
        StorageProvider::GitHubReleases => StorageCredentials {
            access_key: String::new(),
            secret_key: first_non_empty_env(&mut lookup, &["GITHUB_TOKEN", "GH_TOKEN"]),
        },
        StorageProvider::Filesystem => StorageCredentials {
            access_key: String::new(),
            secret_key: String::new(),
        },
    }
}

fn first_non_empty_env<F>(lookup: &mut F, keys: &[&str]) -> String
where
    F: FnMut(&str) -> Option<String>,
{
    keys.iter()
        .filter_map(|key| lookup(key))
        .map(|value| value.trim().to_string())
        .find(|value| !value.is_empty())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::Path;

    use surge_core::archive::extractor::{list_entries_from_bytes, read_entry};
    use surge_core::config::constants::{DEFAULT_ZSTD_LEVEL, RELEASES_FILE_COMPRESSED};
    use surge_core::config::manifest::{ShortcutLocation, SurgeManifest};
    use surge_core::installer_bundle::read_embedded_payload;
    use surge_core::platform::detect::current_rid;
    use surge_core::platform::fs::make_executable;
    use surge_core::releases::manifest::{
        ReleaseEntry, ReleaseIndex, compress_release_index, decompress_release_index,
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
          - web
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
        assert_eq!(
            super::parse_storage_provider("azure_blob").unwrap(),
            super::StorageProvider::AzureBlob
        );
        assert_eq!(
            super::parse_storage_provider("github-releases").unwrap(),
            super::StorageProvider::GitHubReleases
        );
        assert_eq!(
            super::parse_storage_provider("fs").unwrap(),
            super::StorageProvider::Filesystem
        );
    }

    #[test]
    fn test_storage_credentials_resolve_s3_keys() {
        let mut env = BTreeMap::new();
        env.insert("AWS_ACCESS_KEY_ID".to_string(), "access".to_string());
        env.insert("AWS_SECRET_ACCESS_KEY".to_string(), "secret".to_string());
        let creds = super::storage_credentials_from_lookup(super::StorageProvider::S3, |key| env.get(key).cloned());
        assert_eq!(creds.access_key, "access");
        assert_eq!(creds.secret_key, "secret");
    }

    #[test]
    fn test_storage_credentials_resolve_azure_keys() {
        let mut env = BTreeMap::new();
        env.insert("AZURE_STORAGE_ACCOUNT_NAME".to_string(), "account".to_string());
        env.insert("AZURE_STORAGE_ACCOUNT_KEY".to_string(), "key".to_string());
        let creds =
            super::storage_credentials_from_lookup(super::StorageProvider::AzureBlob, |key| env.get(key).cloned());
        assert_eq!(creds.access_key, "account");
        assert_eq!(creds.secret_key, "key");
    }

    #[test]
    fn test_storage_credentials_resolve_github_token_to_secret_key() {
        let mut env = BTreeMap::new();
        env.insert("GITHUB_TOKEN".to_string(), "ghp_test".to_string());
        let creds =
            super::storage_credentials_from_lookup(super::StorageProvider::GitHubReleases, |key| env.get(key).cloned());
        assert!(creds.access_key.is_empty());
        assert_eq!(creds.secret_key, "ghp_test");
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
        let web_installer = installers_dir.join(format!("Setup-{rid}-{app_id}-stable-web.{installer_ext}"));
        let offline_installer = installers_dir.join(format!("Setup-{rid}-{app_id}-stable-offline.{installer_ext}"));
        assert!(web_installer.exists());
        assert!(offline_installer.exists());

        let web_data = read_installer_payload(&web_installer);
        let web_entries = list_entries_from_bytes(&web_data).unwrap();
        assert!(
            web_entries
                .iter()
                .any(|entry| entry.path.to_string_lossy().contains("installer.yml"))
        );
        let web_manifest = String::from_utf8(read_entry(&web_data, "installer.yml").unwrap()).unwrap();
        assert!(web_manifest.contains("installer_type: web"));
        assert!(web_manifest.contains("ui: imgui"));
        assert!(web_manifest.contains("headless_default_if_no_display: true"));

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
                delta_filename: String::new(),
                delta_size: 0,
                delta_sha256: String::new(),
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
}
