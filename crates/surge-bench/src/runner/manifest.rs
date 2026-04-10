use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use surge_core::config::constants::RELEASES_FILE_COMPRESSED;
use surge_core::config::installer::{
    InstallerManifest, InstallerRelease, InstallerRuntime, InstallerStorage, InstallerUi,
};
use surge_core::error::Result;

use super::BENCH_APP_NAME;

pub(super) fn installer_manifest(
    store_dir: &Path,
    app_id: &str,
    rid: &str,
    version: &str,
    full_filename: &str,
    installer_type: &str,
    install_root: &Path,
) -> InstallerManifest {
    InstallerManifest {
        schema: 1,
        format: "surge-installer-v1".to_string(),
        ui: InstallerUi::Console,
        installer_type: installer_type.to_string(),
        app_id: app_id.to_string(),
        rid: rid.to_string(),
        version: version.to_string(),
        channel: "stable".to_string(),
        generated_utc: "1970-01-01T00:00:00Z".to_string(),
        headless_default_if_no_display: true,
        release_index_key: RELEASES_FILE_COMPRESSED.to_string(),
        storage: InstallerStorage {
            provider: "filesystem".to_string(),
            bucket: store_dir.to_string_lossy().to_string(),
            region: String::new(),
            endpoint: String::new(),
            prefix: String::new(),
        },
        release: InstallerRelease {
            full_filename: full_filename.to_string(),
            full_sha256: String::new(),
            delta_filename: String::new(),
            delta_algorithm: String::new(),
            delta_patch_format: String::new(),
            delta_compression: String::new(),
        },
        runtime: InstallerRuntime {
            name: BENCH_APP_NAME.to_string(),
            main_exe: "app.main.dll".to_string(),
            install_directory: install_root.to_string_lossy().to_string(),
            supervisor_id: String::new(),
            icon: String::new(),
            shortcuts: Vec::new(),
            persistent_assets: Vec::new(),
            installers: vec![installer_type.to_string()],
            environment: BTreeMap::new(),
        },
    }
}

pub(super) fn write_bench_manifest(
    path: &Path,
    store_dir: &Path,
    app_id: &str,
    rid: &str,
    pack_zstd_level: i32,
) -> Result<()> {
    let manifest = format!(
        r"schema: 1
storage:
  provider: filesystem
  bucket: {bucket}
pack:
  delta:
    strategy: archive-chunked-bsdiff
  compression:
    format: zstd
    level: {pack_zstd_level}
apps:
  - id: {app_id}
    name: Benchmark App
    main: app.main.dll
    channels:
      - stable
    target:
      rid: {rid}
",
        bucket = store_dir.display()
    );
    fs::write(path, manifest)?;
    Ok(())
}

pub(super) fn version_label(index: usize) -> String {
    format!("1.0.{}", index.saturating_sub(1))
}
