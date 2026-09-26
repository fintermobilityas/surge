use super::*;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;

#[test]
fn stage_monitor_uses_job_result_and_exact_cache_in_isolated_transport() {
    let temp = tempfile::tempdir().unwrap();
    let transport = temp.path().join("tailscale");
    fs::write(
        &transport,
        r"#!/usr/bin/python3
import os, sys
assert sys.argv[1:3] == ['ssh', 'fixture']
command = sys.argv[3].replace('/tmp/.surge-', os.environ['SURGE_STAGE_TEST_ROOT'] + '/.surge-')
os.execv('/bin/sh', ['sh', '-c', command])
",
    )
    .unwrap();
    fs::set_permissions(&transport, fs::Permissions::from_mode(0o755)).unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "commands::install::remote::detached::stage_tests::isolated_stage_monitor_worker",
            "--nocapture",
        ])
        .env("SURGE_STAGE_TEST_ROOT", temp.path())
        .env("HOME", temp.path())
        .env("PATH", format!("{}:/usr/bin:/bin", temp.path().display()))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn current_fixture_release() -> ReleaseEntry {
    ReleaseEntry {
        version: "1.2.3".to_string(),
        rid: "linux-x64".to_string(),
        full_filename: "demoapp-1.2.3.tar.zst".to_string(),
        install_directory: "demoapp".to_string(),
        main_exe: "demoapp".to_string(),
        ..ReleaseEntry::default()
    }
}

fn current_fixture_storage(root: &Path) -> StorageConfig {
    StorageConfig {
        provider: Some(surge_core::context::StorageProvider::Filesystem),
        bucket: root.to_str().unwrap().to_string(),
        ..StorageConfig::default()
    }
}

async fn invoke_current_fixture_install(root: &Path, plan_only: bool, force: bool, stage: bool) -> Result<()> {
    use crate::commands::install::{InstallBehavior, InstallMode};
    let release = current_fixture_release();
    let storage = current_fixture_storage(root);
    let backend = surge_core::storage::filesystem::FilesystemBackend::new(root.to_str().unwrap(), "");
    super::super::install_release_via_tailscale(
        None,
        &backend,
        &super::super::ReleaseIndex::default(),
        &root.join("downloads"),
        "fixture",
        "fixture",
        "demoapp",
        "linux-x64",
        &["linux-x64".to_string()],
        &release,
        "test",
        &storage,
        &release.full_filename,
        InstallBehavior {
            plan_only,
            force,
            no_start: true,
            mode: if stage {
                InstallMode::StageOnly
            } else {
                InstallMode::Install
            },
            ..InstallBehavior::default()
        },
    )
    .await
}

#[tokio::test]
async fn isolated_stage_monitor_worker() {
    let Ok(root) = std::env::var("SURGE_STAGE_TEST_ROOT") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let mut controller_lock = super::super::lock::RemoteInstallerLock::acquire("fixture")
        .await
        .unwrap();
    controller_lock.ensure_held().unwrap();
    assert!(
        super::super::lock::RemoteInstallerLock::acquire("fixture")
            .await
            .is_err()
    );
    let app = root.join(".local/share/demoapp/app");
    fs::create_dir_all(app.join(".surge")).unwrap();
    fs::write(app.join("demoapp"), "fixture").unwrap();
    fs::write(
        app.join(".surge/runtime.yml"),
        format!(
            "id: demoapp\nversion: 1.2.3\nchannel: test\nprovider: filesystem\nbucket: {}\n",
            root.display()
        ),
    )
    .unwrap();
    for force in [false, true] {
        let result = invoke_current_fixture_install(&root, false, force, false).await;
        assert!(result.unwrap_err().to_string().contains("node-local installer lock"));
    }
    invoke_current_fixture_install(&root, true, false, false).await.unwrap();
    drop(controller_lock);
    invoke_current_fixture_install(&root, false, false, false)
        .await
        .unwrap();
    for force in [false, true] {
        assert!(
            invoke_current_fixture_install(&root, false, force, true).await.is_err(),
            "already-installed metadata must not report stage success without its archive"
        );
    }
    let controller_lock = super::super::lock::RemoteInstallerLock::acquire("fixture")
        .await
        .unwrap();
    let pending = root.join(".surge-installer.operation");
    let installer = root.join(".surge-installer");
    fs::write(&pending, "starting-operation").unwrap();
    fs::write(&installer, "preserve-starting-installer").unwrap();
    drop(controller_lock);
    let mut controller_lock = super::super::lock::RemoteInstallerLock::acquire("fixture")
        .await
        .unwrap();
    let result = probe_remote_install_before_transfer("fixture", &mut controller_lock, Duration::ZERO).await;
    assert!(result.is_err());
    assert_eq!(fs::read_to_string(&pending).unwrap(), "starting-operation");
    assert_eq!(fs::read_to_string(&installer).unwrap(), "preserve-starting-installer");
    let publish_root = root.clone();
    let publisher = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        fs::write(
            publish_root.join(".surge-installer.identity"),
            super::super::detached_identity::current_process_identity(),
        )
        .unwrap();
        fs::write(
            publish_root.join(".surge-installer.pid"),
            std::process::id().to_string(),
        )
        .unwrap();
    });
    let probe = probe_remote_install_before_transfer("fixture", &mut controller_lock, Duration::from_secs(5))
        .await
        .unwrap();
    publisher.join().unwrap();
    assert!(probe.alive);
    assert_eq!(probe.operation.as_deref(), Some("starting-operation"));
    assert_eq!(fs::read_to_string(&installer).unwrap(), "preserve-starting-installer");
    drop(controller_lock);
    for force in [false, true] {
        assert!(
            invoke_current_fixture_install(&root, false, force, false)
                .await
                .is_err(),
            "a live detached job must prevent package-current early success"
        );
    }
    assert_eq!(fs::read_to_string(&installer).unwrap(), "preserve-starting-installer");
    let request = super::super::operation::request_fingerprint(
        "demoapp",
        "linux-x64",
        &current_fixture_release(),
        "test",
        &current_fixture_storage(&root),
        crate::commands::install::InstallBehavior {
            no_start: true,
            ..Default::default()
        },
    )
    .unwrap();
    fs::write(&pending, request).unwrap();
    let finish_root = root.clone();
    let finisher = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(200));
        fs::write(
            finish_root.join(".local/share/demoapp/.surge-update-status.json"),
            r#"{"state":"converged","installed_version":"1.2.3","target_version":"1.2.3"}"#,
        )
        .unwrap();
        fs::write(finish_root.join("job-finished"), "complete").unwrap();
        fs::write(finish_root.join(".surge-installer.result"), "0").unwrap();
        fs::remove_file(finish_root.join(".surge-installer.pid")).unwrap();
    });
    let completion = invoke_current_fixture_install(&root, false, false, false).await;
    let finished_before_return = root.join("job-finished").exists();
    finisher.join().unwrap();
    completion.unwrap();
    assert!(
        finished_before_return,
        "current metadata must not bypass the matching detached job"
    );
    assert!(
        !installer.exists(),
        "successful reattachment must clean up the completed owned helper"
    );
    let _controller_lock = super::super::lock::RemoteInstallerLock::acquire("fixture")
        .await
        .unwrap();
    let install = root.join(".local/share/demoapp");
    fs::create_dir_all(&install).unwrap();
    let release = ReleaseEntry {
        version: "1.2.3".to_string(),
        rid: "linux-x64".to_string(),
        full_filename: "demoapp-1.2.3.tar.zst".to_string(),
        full_sha256: "expected-hash".to_string(),
        ..ReleaseEntry::default()
    };
    let storage = StorageConfig {
        provider: Some(surge_core::context::StorageProvider::S3),
        bucket: "fixture".to_string(),
        ..StorageConfig::default()
    };
    let target = RemoteInstallTarget {
        is_stage: true,
        app_id: "demoapp",
        rid: "linux-x64",
        release: &release,
        channel: "test",
        storage: &storage,
    };
    let operation = "fixture-stage";
    fs::write(root.join(".surge-installer.operation"), operation).unwrap();
    fs::write(root.join(".surge-installer.pid"), std::process::id().to_string()).unwrap();
    fs::write(
        root.join(".surge-installer.identity"),
        super::super::detached_identity::current_process_identity(),
    )
    .unwrap();
    let mut offset = 0;
    let mut progress = Instant::now();
    for state in ["converged", "failed", "in_progress"] {
        let previous = format!(r#"{{"state":"{state}","installed_version":"1.2.2","target_version":"1.2.2"}}"#);
        fs::write(install.join(".surge-update-status.json"), &previous).unwrap();
        let outcome = poll_remote_detached_install_once(
            "fixture",
            "fixture",
            &install,
            &mut offset,
            &mut progress,
            &target,
            operation,
        )
        .await
        .unwrap();
        assert_eq!(
            outcome,
            WatchOutcome::InProgress,
            "old app status {state} must not describe staging"
        );
        assert_eq!(
            fs::read_to_string(install.join(".surge-update-status.json")).unwrap(),
            previous
        );
    }
    let install_target = RemoteInstallTarget {
        is_stage: false,
        ..target
    };
    for state in ["converged", "failed"] {
        for version in ["1.2.2", "1.2.3"] {
            fs::write(
                install.join(".surge-update-status.json"),
                format!(r#"{{"state":"{state}","installed_version":"{version}","target_version":"{version}"}}"#),
            )
            .unwrap();
            assert_eq!(
                poll_remote_detached_install_once(
                    "fixture",
                    "fixture",
                    &install,
                    &mut offset,
                    &mut progress,
                    &install_target,
                    operation
                )
                .await
                .unwrap(),
                WatchOutcome::InProgress
            );
        }
    }
    fs::write(root.join(".surge-installer.operation"), "another-operation").unwrap();
    assert!(
        poll_remote_detached_install_once(
            "fixture",
            "fixture",
            &install,
            &mut offset,
            &mut progress,
            &target,
            operation
        )
        .await
        .is_err()
    );
    fs::write(root.join(".surge-installer.operation"), operation).unwrap();
    fs::remove_file(root.join(".surge-installer.pid")).unwrap();
    assert!(
        poll_remote_detached_install_once(
            "fixture",
            "fixture",
            &install,
            &mut offset,
            &mut progress,
            &target,
            operation
        )
        .await
        .is_err()
    );
    fs::write(root.join(".surge-installer.result"), "0").unwrap();
    assert!(matches!(
        poll_remote_detached_install_once(
            "fixture",
            "fixture",
            &install,
            &mut offset,
            &mut progress,
            &target,
            operation
        )
        .await,
        Err(SurgeError::NotFound(_))
    ));

    let cache = install.join(".surge-cache/staged-installer");
    fs::create_dir_all(&cache).unwrap();
    let identity = super::super::state::remote_staged_payload_identity("demoapp", &release, "test", &storage);
    fs::write(
        cache.join(".surge-staged-release.json"),
        serde_json::to_vec(&identity).unwrap(),
    )
    .unwrap();
    fs::write(cache.join("installer.yml"), "fixture").unwrap();
    fs::write(cache.join("surge"), "fixture").unwrap();
    assert!(matches!(
        poll_remote_detached_install_once(
            "fixture",
            "fixture",
            &install,
            &mut offset,
            &mut progress,
            &target,
            operation
        )
        .await,
        Err(SurgeError::NotFound(_))
    ));
    let artifact =
        super::super::cache_path_for_key(&install.join(".surge-cache/artifacts"), &release.full_filename).unwrap();
    fs::create_dir_all(artifact.parent().unwrap()).unwrap();
    fs::write(&artifact, "fixture").unwrap();
    assert_eq!(
        poll_remote_detached_install_once(
            "fixture",
            "fixture",
            &install,
            &mut offset,
            &mut progress,
            &target,
            operation
        )
        .await
        .unwrap(),
        WatchOutcome::Converged
    );
    super::super::state::verify_remote_stage_readiness(
        "fixture",
        "fixture",
        "demoapp",
        "linux-x64",
        &release,
        "test",
        &storage,
    )
    .await
    .unwrap();
    fs::write(
        install.join(".surge-update-status.json"),
        r#"{"state":"converged","installed_version":"1.2.3","target_version":"1.2.3"}"#,
    )
    .unwrap();
    assert_eq!(
        poll_remote_detached_install_once(
            "fixture",
            "fixture",
            &install,
            &mut offset,
            &mut progress,
            &install_target,
            operation
        )
        .await
        .unwrap(),
        WatchOutcome::Converged
    );
    fs::write(
        install.join(".surge-update-status.json"),
        r#"{"state":"converged","installed_version":"1.2.2","target_version":"1.2.2"}"#,
    )
    .unwrap();
    assert!(
        poll_remote_detached_install_once(
            "fixture",
            "fixture",
            &install,
            &mut offset,
            &mut progress,
            &install_target,
            operation
        )
        .await
        .is_err()
    );
    let wrong_target = RemoteInstallTarget {
        channel: "another-channel",
        ..target
    };
    assert!(matches!(
        poll_remote_detached_install_once(
            "fixture",
            "fixture",
            &install,
            &mut offset,
            &mut progress,
            &wrong_target,
            operation
        )
        .await,
        Err(SurgeError::NotFound(_))
    ));
}
