pub mod demote;
pub mod init;
pub mod list;
pub mod lock;
pub mod migrate;
pub mod pack;
pub mod promote;
pub mod push;
pub mod restore;
pub mod tailscale;

use surge_core::config::manifest::SurgeManifest;
use surge_core::error::{Result, SurgeError};

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

#[cfg(test)]
mod tests {
    use std::path::Path;

    use surge_core::archive::extractor::{list_entries_from_bytes, read_entry};
    use surge_core::config::constants::RELEASES_FILE_COMPRESSED;
    use surge_core::config::manifest::{ShortcutLocation, SurgeManifest};
    use surge_core::platform::detect::current_rid;
    use surge_core::platform::fs::make_executable;
    use surge_core::releases::manifest::{ReleaseIndex, decompress_release_index};

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

    #[tokio::test]
    async fn test_pack_push_promote_demote_smoke() {
        let tmp = tempfile::tempdir().unwrap();
        let store_dir = tmp.path().join("store");
        let artifacts_dir = tmp.path().join("artifacts");
        let packages_dir = tmp.path().join("packages");
        let manifest_path = tmp.path().join("surge.yml");
        let app_id = "smoke-app";
        let version = "1.0.0";
        let rid = current_rid();

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
            &artifacts_dir,
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
        let web_installer = installers_dir.join(format!("Setup-{rid}-{app_id}-stable-web.surge-installer.tar.zst"));
        let offline_installer =
            installers_dir.join(format!("Setup-{rid}-{app_id}-stable-offline.surge-installer.tar.zst"));
        assert!(web_installer.exists());
        assert!(offline_installer.exists());

        let web_data = std::fs::read(&web_installer).unwrap();
        let web_entries = list_entries_from_bytes(&web_data).unwrap();
        assert!(
            web_entries
                .iter()
                .any(|entry| entry.path == std::path::Path::new("installer.yml"))
        );
        assert!(!web_entries.iter().any(|entry| entry.path.starts_with("payload/")));
        let web_manifest = String::from_utf8(read_entry(&web_data, "installer.yml").unwrap()).unwrap();
        assert!(web_manifest.contains("installer_type: web"));
        assert!(web_manifest.contains("ui: imgui"));
        assert!(web_manifest.contains("headless_default_if_no_display: true"));

        let offline_data = std::fs::read(&offline_installer).unwrap();
        let offline_entries = list_entries_from_bytes(&offline_data).unwrap();
        assert!(
            offline_entries
                .iter()
                .any(|entry| entry.path == std::path::PathBuf::from("payload").join(full_package.file_name().unwrap()))
        );
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
}
