use surge_core::config::manifest::{ShortcutLocation, SurgeManifest};

const SNAPX_COMPAT_YAML: &[u8] = br#"
schema: 2

generic:
  token: REPLACEME

channels:
  - name: test
    pushFeed: !nuget
      name: sample-publish
    updateFeed: !http
      source: https://example.com/api/application/quasar/snapx.json
  - name: production
    pushFeed: !nuget
      name: sample-publish
    updateFeed: !http
      source: https://example.com/api/application/quasar/snapx.json

apps:
  - id: quasar-ubuntu24.04-linux-x64-cpu
    main: quasar
    installDirectory: quasar
    supervisorid: 223033af-20bb-4c4c-9639-589f57d58444
    channels:
      - test
      - production
    target:
      os: linux
      framework: net10.0
      rid: linux-x64
      icon: .snapx/assets/icons/quasar.svg
      persistentAssets:
        - application.v2.json
        - assets
        - logging
      shortcuts:
        - desktop
        - startup
      installers:
        - web
        - offline
      environment:
        OPENCV_FFMPEG_CAPTURE_OPTIONS: "rtsp_transport;tcp"

  - id: quasar-ubuntu24.04-linux-x64-cuda
    main: quasar
    installDirectory: quasar
    supervisorid: 223033af-20bb-4c4c-9639-589f57d58444
    channels:
      - test
      - production
    target:
      os: linux
      framework: net10.0
      rid: linux-x64
      icon: .snapx/assets/icons/quasar.svg
      persistentAssets:
        - application.v2.json
        - assets
        - logging
      shortcuts:
        - desktop
        - startup
      installers:
        - web
      environment:
        OPENCV_FFMPEG_CAPTURE_OPTIONS: "rtsp_transport;tcp"

  - id: quasar-ubuntu24.04-linux-arm64
    main: quasar
    installDirectory: quasar
    supervisorid: 223033af-20bb-4c4c-9639-589f57d58444
    channels:
      - test
      - production
    target:
      os: linux
      framework: net10.0
      rid: linux-arm64
      icon: .snapx/assets/icons/quasar.svg
      persistentAssets:
        - application.v2.json
        - assets
        - logging
      shortcuts:
        - desktop
        - startup
      installers:
        - web
        - offline
      environment:
        OPENCV_FFMPEG_CAPTURE_OPTIONS: "rtsp_transport;tcp"
        GST_DEBUG: "1"
"#;

#[test]
fn parse_snapx_compat_manifest() {
    let manifest = SurgeManifest::parse(SNAPX_COMPAT_YAML).expect("snapx-compatible manifest should parse");

    assert_eq!(manifest.schema, 2);
    assert_eq!(manifest.apps.len(), 3);

    let app = manifest
        .find_app("quasar-ubuntu24.04-linux-arm64")
        .expect("expected app entry");
    assert_eq!(app.effective_main_exe(), "quasar");
    assert_eq!(app.effective_install_directory(), "quasar");

    let target = manifest
        .find_target("quasar-ubuntu24.04-linux-arm64", "linux-arm64")
        .expect("expected target");
    assert_eq!(
        target.shortcuts,
        vec![ShortcutLocation::Desktop, ShortcutLocation::Startup]
    );
    assert!(target.persistent_assets.contains(&"assets".to_string()));
    assert_eq!(target.installers, vec!["web".to_string(), "offline".to_string()]);
    assert_eq!(target.environment.get("GST_DEBUG").map(String::as_str), Some("1"));
}

#[test]
fn defaults_main_and_install_directory_to_id() {
    let yaml = br"
schema: 1
storage:
  provider: filesystem
  bucket: /tmp/releases
apps:
  - id: quasar
    target:
      rid: linux-x64
";

    let manifest = SurgeManifest::parse(yaml).unwrap();
    let app = manifest.find_app("quasar").unwrap();
    assert_eq!(app.effective_main_exe(), "quasar");
    assert_eq!(app.effective_install_directory(), "quasar");
}

#[test]
fn inherits_target_defaults_from_app_level() {
    let yaml = br#"
schema: 1
storage:
  provider: filesystem
  bucket: /tmp/releases
apps:
  - id: quasar
    main: quasar
    installDirectory: quasar
    icon: icon.svg
    shortcuts: [desktop]
    persistentAssets: [assets, config.json]
    installers: [web, offline]
    environment:
      A: "1"
    targets:
      - rid: linux-x64
      - rid: linux-arm64
        environment:
          B: "2"
"#;

    let manifest = SurgeManifest::parse(yaml).unwrap();

    // Multi-target apps with `id` get expanded: quasar-linux-x64, quasar-linux-arm64
    let linux_x64 = manifest.find_target("quasar-linux-x64", "linux-x64").unwrap();
    assert_eq!(linux_x64.icon, "icon.svg");
    assert_eq!(linux_x64.shortcuts, vec![ShortcutLocation::Desktop]);
    assert_eq!(
        linux_x64.persistent_assets,
        vec!["assets".to_string(), "config.json".to_string()]
    );
    assert_eq!(linux_x64.installers, vec!["web".to_string(), "offline".to_string()]);
    assert_eq!(linux_x64.environment.get("A").map(String::as_str), Some("1"));

    // Verify expanded child inherits name/main_exe/install_directory from parent id
    let app_x64 = manifest.find_app("quasar-linux-x64").unwrap();
    assert_eq!(app_x64.effective_name(), "quasar");
    assert_eq!(app_x64.effective_main_exe(), "quasar");
    assert_eq!(app_x64.effective_install_directory(), "quasar");

    let linux_arm64 = manifest.find_target("quasar-linux-arm64", "linux-arm64").unwrap();
    assert_eq!(linux_arm64.environment.get("A").map(String::as_str), Some("1"));
    assert_eq!(linux_arm64.environment.get("B").map(String::as_str), Some("2"));
}

#[test]
fn rejects_persistent_asset_parent_traversal() {
    let yaml = br"
schema: 1
storage:
  provider: filesystem
  bucket: /tmp/releases
apps:
  - id: bad
    target:
      rid: linux-x64
      persistentAssets:
        - ../secret
";

    let err = SurgeManifest::parse(yaml).unwrap_err();
    assert!(err.to_string().contains("cannot traverse parent/root"));
}

#[test]
fn rejects_embedded_storage_credentials() {
    let yaml = br"
schema: 1
storage:
  provider: s3
  bucket: my-bucket
  secret_key: should-not-be-here
apps:
  - id: demo
    target:
      rid: linux-x64
";

    let err = SurgeManifest::parse(yaml).unwrap_err();
    assert!(err.to_string().contains("Credentials are not allowed in manifests"));
}

#[test]
fn rejects_unknown_installer_type() {
    let yaml = br"
schema: 1
storage:
  provider: filesystem
  bucket: /tmp/releases
apps:
  - id: demo
    target:
      rid: linux-x64
      installers:
        - web
        - usb
";

    let err = SurgeManifest::parse(yaml).unwrap_err();
    assert!(err.to_string().contains("Unsupported installer"));
}
