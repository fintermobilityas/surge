use std::path::Path;
use std::process::{Command, Output};

use surge_core::config::constants::RELEASES_FILE_COMPRESSED;
use surge_core::platform::detect::current_rid;
use surge_core::releases::manifest::decompress_release_index;

const CANONICAL_APP_ID: &str = "quasar-ubuntu24.04";

// Expected source migration manifest (readable, snapx-compatible naming style).
const SOURCE_MIGRATION_YML: &str = r"schema: 1
storage:
  provider: filesystem
  bucket: __SRC_STORE__
apps:
  - id: __SOURCE_APP_ID__
    main: quasar
    installDirectory: quasar
    target:
      rid: __RID__
      icon: icon.svg
      shortcuts:
        - desktop
        - startup
      persistentAssets:
        - assets
        - logging
      installers:
        - web
        - offline
";

// Expected destination migration manifest.
const DEST_MIGRATION_YML: &str = r"schema: 1
storage:
  provider: filesystem
  bucket: __DST_STORE__
apps:
  - id: quasar-ubuntu24.04
    main: quasar
    installDirectory: quasar
    target:
      rid: __RID__
";

fn run(args: &[&str], cwd: &Path) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_surge"));
    cmd.current_dir(cwd).args(args);
    if std::env::var("SURGE_INSTALLER_BINARY").is_err() {
        cmd.env("SURGE_INSTALLER_BINARY", env!("CARGO_BIN_EXE_surge"));
    }
    if std::env::var("SURGE_INSTALLER_LAUNCHER").is_err() {
        cmd.env("SURGE_INSTALLER_LAUNCHER", env!("CARGO_BIN_EXE_surge"));
    }
    cmd.output().expect("failed to run surge")
}

fn debug_output(output: &Output) -> String {
    format!(
        "status={:?}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[test]
fn migrate_snapx_style_app_copies_full_and_delta_artifacts() {
    let tmp = tempfile::tempdir().unwrap();
    let rid = current_rid();
    let source_app_id = format!("quasar-ubuntu24.04-{rid}");
    let src_store = tmp.path().join("src-store");
    let dst_store = tmp.path().join("dst-store");
    let packages = tmp.path().join("packages");
    let artifacts_v1 = tmp.path().join("artifacts-v1");
    let artifacts_v2 = tmp.path().join("artifacts-v2");
    std::fs::create_dir_all(&src_store).unwrap();
    std::fs::create_dir_all(&dst_store).unwrap();
    std::fs::create_dir_all(&packages).unwrap();
    std::fs::create_dir_all(&artifacts_v1).unwrap();
    std::fs::create_dir_all(&artifacts_v2).unwrap();

    let source_manifest = SOURCE_MIGRATION_YML
        .replace("__SRC_STORE__", &src_store.to_string_lossy())
        .replace("__SOURCE_APP_ID__", &source_app_id)
        .replace("__RID__", &rid);
    let source_manifest_path = tmp.path().join("source.yml");
    std::fs::write(&source_manifest_path, source_manifest).unwrap();

    let destination_manifest = DEST_MIGRATION_YML
        .replace("__DST_STORE__", &dst_store.to_string_lossy())
        .replace("__RID__", &rid);
    let destination_manifest_path = tmp.path().join("dest.yml");
    std::fs::write(&destination_manifest_path, destination_manifest).unwrap();

    std::fs::write(artifacts_v1.join("quasar"), b"#!/bin/sh\necho v1\n").unwrap();
    std::fs::write(artifacts_v1.join("icon.svg"), b"<svg></svg>").unwrap();
    std::fs::write(artifacts_v1.join("payload.txt"), b"payload-v1").unwrap();

    std::fs::write(artifacts_v2.join("quasar"), b"#!/bin/sh\necho v2\n").unwrap();
    std::fs::write(artifacts_v2.join("icon.svg"), b"<svg></svg>").unwrap();
    std::fs::write(artifacts_v2.join("payload.txt"), b"payload-v2").unwrap();

    let source_manifest_path_str = source_manifest_path.to_string_lossy().into_owned();
    let destination_manifest_path_str = destination_manifest_path.to_string_lossy().into_owned();
    let packages_str = packages.to_string_lossy().into_owned();
    let artifacts_v1_str = artifacts_v1.to_string_lossy().into_owned();
    let artifacts_v2_str = artifacts_v2.to_string_lossy().into_owned();

    let out = run(
        &[
            "-m",
            &source_manifest_path_str,
            "pack",
            "--app-id",
            &source_app_id,
            "--rid",
            &rid,
            "--version",
            "1.0.0",
            "--artifacts-dir",
            &artifacts_v1_str,
            "--output-dir",
            &packages_str,
        ],
        tmp.path(),
    );
    assert!(out.status.success(), "{}", debug_output(&out));

    let out = run(
        &[
            "-m",
            &source_manifest_path_str,
            "push",
            "--app-id",
            &source_app_id,
            "--rid",
            &rid,
            "--version",
            "1.0.0",
            "--channel",
            "stable",
            "--packages-dir",
            &packages_str,
        ],
        tmp.path(),
    );
    assert!(out.status.success(), "{}", debug_output(&out));

    let out = run(
        &[
            "-m",
            &source_manifest_path_str,
            "pack",
            "--app-id",
            &source_app_id,
            "--rid",
            &rid,
            "--version",
            "1.1.0",
            "--artifacts-dir",
            &artifacts_v2_str,
            "--output-dir",
            &packages_str,
        ],
        tmp.path(),
    );
    assert!(out.status.success(), "{}", debug_output(&out));

    let out = run(
        &[
            "-m",
            &source_manifest_path_str,
            "push",
            "--app-id",
            &source_app_id,
            "--rid",
            &rid,
            "--version",
            "1.1.0",
            "--channel",
            "stable",
            "--packages-dir",
            &packages_str,
        ],
        tmp.path(),
    );
    assert!(out.status.success(), "{}", debug_output(&out));

    let out = run(
        &[
            "-m",
            &source_manifest_path_str,
            "migrate",
            "--app-id",
            CANONICAL_APP_ID,
            "--rid",
            &rid,
            "--dest-manifest",
            &destination_manifest_path_str,
        ],
        tmp.path(),
    );
    assert!(out.status.success(), "{}", debug_output(&out));

    let expected_full_v1 = dst_store.join(format!("{CANONICAL_APP_ID}-1.0.0-{rid}-full.tar.zst"));
    let expected_full_v2 = dst_store.join(format!("{CANONICAL_APP_ID}-1.1.0-{rid}-full.tar.zst"));
    let expected_delta_v2 = dst_store.join(format!("{CANONICAL_APP_ID}-1.1.0-{rid}-delta.tar.zst"));
    let expected_index = dst_store.join(RELEASES_FILE_COMPRESSED);

    assert!(expected_full_v1.exists(), "missing {}", expected_full_v1.display());
    assert!(expected_full_v2.exists(), "missing {}", expected_full_v2.display());
    assert!(expected_delta_v2.exists(), "missing {}", expected_delta_v2.display());
    assert!(expected_index.exists(), "missing {}", expected_index.display());

    let index_data = std::fs::read(expected_index).unwrap();
    let index = decompress_release_index(&index_data).unwrap();
    assert_eq!(index.app_id, CANONICAL_APP_ID);
    assert_eq!(index.releases.len(), 2);
    assert!(
        index
            .releases
            .iter()
            .any(|release| release.version == "1.0.0" && release.rid == rid)
    );
    assert!(
        index
            .releases
            .iter()
            .any(|release| release.version == "1.1.0" && release.rid == rid && release.selected_delta().is_some())
    );
}
