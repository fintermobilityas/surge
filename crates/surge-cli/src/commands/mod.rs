pub mod demote;
pub mod init;
pub mod list;
pub mod lock;
pub mod migrate;
pub mod pack;
pub mod promote;
pub mod push;
pub mod restore;

#[cfg(test)]
mod tests {
    use std::path::Path;

    use surge_core::config::constants::RELEASES_FILE_COMPRESSED;
    use surge_core::platform::detect::current_rid;
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
        write_manifest(&manifest_path, &store_dir, app_id, &rid);

        super::pack::execute(&manifest_path, app_id, version, &rid, &artifacts_dir, &packages_dir)
            .await
            .unwrap();

        let full_package = packages_dir.join(format!("{app_id}-{version}-{rid}-full.tar.zst"));
        assert!(full_package.exists());

        super::push::execute(&manifest_path, app_id, version, &rid, "stable", &packages_dir)
            .await
            .unwrap();

        let index = read_index(&store_dir);
        assert_eq!(index.app_id, app_id);
        assert_eq!(index.releases.len(), 1);
        assert_eq!(index.releases[0].version, version);
        assert_eq!(index.releases[0].rid, rid);
        assert_eq!(index.releases[0].channels, vec!["stable"]);

        super::promote::execute(&manifest_path, app_id, version, &rid, "beta")
            .await
            .unwrap();
        let index = read_index(&store_dir);
        assert_eq!(index.releases[0].channels, vec!["beta", "stable"]);

        super::demote::execute(&manifest_path, app_id, version, &rid, "beta")
            .await
            .unwrap();
        let index = read_index(&store_dir);
        assert_eq!(index.releases[0].channels, vec!["stable"]);

        super::list::execute(&manifest_path, app_id, &rid, None).await.unwrap();
        super::list::execute(&manifest_path, app_id, &rid, Some("beta"))
            .await
            .unwrap();
    }
}
