use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use surge_core::archive::packer::ArchivePacker;
use surge_core::config::constants::{DEFAULT_ZSTD_LEVEL, RELEASES_FILE_COMPRESSED};
use surge_core::context::{Context, StorageProvider};
use surge_core::crypto::sha256::sha256_hex_file;
use surge_core::install::{InstallProfile, RuntimeManifestMetadata, write_runtime_manifest};
use surge_core::platform::detect::current_rid;
use surge_core::platform::process::supervisor_binary_name;
use surge_core::releases::manifest::{ReleaseEntry, ReleaseIndex, compress_release_index};
use surge_core::supervisor::state::write_restart_args;
use surge_core::update::manager::{ProgressInfo, UpdateApplyOutcome, UpdateManager};
use surge_core::update::status::{UpdateConvergenceState, read_update_status};

const APP_ID: &str = "external-finalizer-fixture";
const SUPERVISOR_ID: &str = "external-finalizer-supervisor";
const CURRENT_VERSION: &str = "1.0.0";
const TARGET_VERSION: &str = "2.0.0";
const MODE_ENV: &str = "SURGE_EXTERNAL_FINALIZE_MODE";
const UPDATER_MODE: &str = "updater";
const TARGET_MODE: &str = "target";
const TARGET_ENV_VALUE: &str = "target-environment-preserved";
const RESTART_ARGS: [&str; 2] = ["--fixture-restart", "value with spaces"];

#[cfg(windows)]
const UPDATER_EXE: &str = "fixture-updater.exe";
#[cfg(not(windows))]
const UPDATER_EXE: &str = "fixture-updater";
#[cfg(windows)]
const TARGET_EXE: &str = "fixture-target.exe";
#[cfg(not(windows))]
const TARGET_EXE: &str = "fixture-target";

fn main() {
    match std::env::var(MODE_ENV).as_deref() {
        Ok(UPDATER_MODE) => run_self_update(),
        Ok(TARGET_MODE) => run_target(),
        _ => verify_self_hosted_update(),
    }
}

fn run_self_update() {
    let store_root = required_path("SURGE_EXTERNAL_FINALIZE_STORE");
    let install_root = required_path("SURGE_EXTERNAL_FINALIZE_INSTALL");
    let returned_marker = required_path("SURGE_EXTERNAL_FINALIZE_RETURNED");
    let allow_exit_marker = required_path("SURGE_EXTERNAL_FINALIZE_ALLOW_EXIT");
    let context = Arc::new(Context::new());
    context.set_storage(
        StorageProvider::Filesystem,
        store_root.to_str().unwrap(),
        "",
        "",
        "",
        "",
    );
    let mut manager =
        UpdateManager::new(context, APP_ID, CURRENT_VERSION, "test", install_root.to_str().unwrap()).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async move {
        let update = manager.check_for_updates().await.unwrap().unwrap();
        let outcome = manager
            .download_and_apply(&update, None::<fn(ProgressInfo)>)
            .await
            .unwrap();
        assert_eq!(outcome, UpdateApplyOutcome::ExternalFinalizeScheduled);
    });
    std::fs::write(returned_marker, "scheduled").unwrap();
    assert!(wait_until(Duration::from_secs(10), || allow_exit_marker.is_file()));
}

fn run_target() {
    let install_root = required_path("SURGE_EXTERNAL_FINALIZE_INSTALL");
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let status = read_update_status(&install_root)
        .unwrap()
        .expect("target must observe update status");
    let observed_status = format!(
        "{}\n{}\n{}\n",
        status.state.as_str(),
        status.installed_version,
        status.target_version
    );

    if args
        .iter()
        .any(|arg| arg == "--surge-updated" || arg.starts_with("--surge-updated="))
    {
        std::fs::write(
            required_path("SURGE_EXTERNAL_FINALIZE_TARGET_HOOK_STATUS"),
            observed_status,
        )
        .unwrap();
        return;
    }

    let started_marker = required_path("SURGE_EXTERNAL_FINALIZE_TARGET_STARTED");
    let status_marker = required_path("SURGE_EXTERNAL_FINALIZE_TARGET_STATUS");
    let args_marker = required_path("SURGE_EXTERNAL_FINALIZE_TARGET_ARGS");
    let environment_marker = required_path("SURGE_EXTERNAL_FINALIZE_TARGET_ENVIRONMENT");
    let executable_marker = required_path("SURGE_EXTERNAL_FINALIZE_TARGET_EXECUTABLE");
    let pid_path = required_path("SURGE_EXTERNAL_FINALIZE_TARGET_PID");
    let stop_path = required_path("SURGE_EXTERNAL_FINALIZE_TARGET_STOP");

    std::fs::write(status_marker, observed_status).unwrap();
    std::fs::write(args_marker, args.join("\n")).unwrap();
    std::fs::write(
        environment_marker,
        std::env::var("SURGE_EXTERNAL_FINALIZE_TARGET_VALUE").unwrap_or_default(),
    )
    .unwrap();
    std::fs::write(
        executable_marker,
        std::fs::canonicalize(std::env::current_exe().unwrap())
            .unwrap()
            .to_string_lossy()
            .as_bytes(),
    )
    .unwrap();
    std::fs::write(pid_path, std::process::id().to_string()).unwrap();
    std::fs::write(started_marker, "started").unwrap();
    assert!(wait_until(Duration::from_mins(1), || stop_path.is_file()));
}

fn verify_self_hosted_update() {
    #[cfg(windows)]
    if !detached_helper_launch_is_supported() {
        return;
    }

    let temp = tempfile::tempdir().unwrap();
    let store_root = temp.path().join("store");
    let install_root = temp.path().join("install");
    let active_app_dir = install_root.join("app");
    let updater_path = active_app_dir.join(UPDATER_EXE);
    let returned_marker = temp.path().join("update-returned");
    let allow_exit_marker = temp.path().join("allow-updater-exit");
    let target_started_marker = temp.path().join("target-started");
    let target_hook_status_marker = temp.path().join("target-hook-status");
    let target_status_marker = temp.path().join("target-status");
    let target_args_marker = temp.path().join("target-args");
    let target_environment_marker = temp.path().join("target-environment");
    let target_executable_marker = temp.path().join("target-executable");
    let target_pid_path = temp.path().join("target.pid");
    let target_stop_path = temp.path().join("target.stop");
    std::fs::create_dir_all(&active_app_dir).unwrap();

    let fixture_exe = std::env::current_exe().unwrap();
    std::fs::copy(&fixture_exe, &updater_path).unwrap();
    make_executable(&updater_path);
    std::fs::copy(
        Path::new(env!("CARGO_BIN_EXE_surge-supervisor")),
        active_app_dir.join(supervisor_binary_name()),
    )
    .unwrap();
    make_executable(&active_app_dir.join(supervisor_binary_name()));
    write_current_runtime_manifest(&active_app_dir);
    write_restart_args(
        &install_root,
        SUPERVISOR_ID,
        &RESTART_ARGS.iter().map(ToString::to_string).collect::<Vec<_>>(),
    )
    .unwrap();
    write_target_release(
        &store_root,
        &fixture_exe,
        &install_root,
        &target_started_marker,
        &target_hook_status_marker,
        &target_status_marker,
        &target_args_marker,
        &target_environment_marker,
        &target_executable_marker,
        &target_pid_path,
        &target_stop_path,
    );

    let mut updater = std::process::Command::new(&updater_path)
        .env(MODE_ENV, UPDATER_MODE)
        .env("SURGE_EXTERNAL_FINALIZE_STORE", &store_root)
        .env("SURGE_EXTERNAL_FINALIZE_INSTALL", &install_root)
        .env("SURGE_EXTERNAL_FINALIZE_RETURNED", &returned_marker)
        .env("SURGE_EXTERNAL_FINALIZE_ALLOW_EXIT", &allow_exit_marker)
        .spawn()
        .unwrap();

    assert!(wait_until(Duration::from_secs(20), || returned_marker.is_file()));
    assert!(updater.try_wait().unwrap().is_none());
    assert!(active_app_dir.join(UPDATER_EXE).is_file());
    assert!(!active_app_dir.join(TARGET_EXE).exists());
    assert!(!target_started_marker.exists());

    std::fs::write(&allow_exit_marker, "exit").unwrap();
    assert!(
        updater.wait().unwrap().success(),
        "self-hosted updater should hand off successfully"
    );
    assert!(
        wait_until(Duration::from_secs(20), || target_started_marker.is_file()),
        "target did not start; status: {:?}; active entries: {:?}",
        read_update_status(&install_root),
        std::fs::read_dir(&active_app_dir).map(|entries| entries
            .filter_map(std::result::Result::ok)
            .map(|entry| entry.file_name())
            .collect::<Vec<_>>()),
    );

    assert_eq!(
        std::fs::read_to_string(&target_hook_status_marker).unwrap(),
        "pending_restart\n2.0.0\n2.0.0\n"
    );
    assert_eq!(
        std::fs::read_to_string(&target_status_marker).unwrap(),
        "pending_restart\n2.0.0\n2.0.0\n"
    );
    assert_eq!(
        std::fs::read_to_string(&target_args_marker).unwrap(),
        RESTART_ARGS.join("\n")
    );
    assert_eq!(
        std::fs::read_to_string(&target_environment_marker).unwrap(),
        TARGET_ENV_VALUE
    );
    assert_eq!(
        PathBuf::from(std::fs::read_to_string(&target_executable_marker).unwrap()),
        std::fs::canonicalize(active_app_dir.join(TARGET_EXE)).unwrap()
    );

    assert!(wait_until(Duration::from_secs(15), || {
        read_update_status(&install_root).ok().flatten().is_some_and(|status| {
            status.state == UpdateConvergenceState::Converged
                && status.installed_version == TARGET_VERSION
                && status.supervisor_restart_confirmed
        })
    }));
    assert!(
        install_root
            .join(format!("app-{CURRENT_VERSION}"))
            .join(UPDATER_EXE)
            .is_file()
    );
    assert!(
        install_root
            .join(".surge-tools")
            .join(supervisor_binary_name())
            .is_file()
    );
    assert_eq!(
        surge_core::install::read_runtime_manifest_version(&active_app_dir)
            .unwrap()
            .as_deref(),
        Some(TARGET_VERSION)
    );

    stop_fixture_processes(&install_root, &target_stop_path, &target_pid_path);
}

#[cfg(windows)]
fn detached_helper_launch_is_supported() -> bool {
    let environment = BTreeMap::new();
    let mut probe = match surge_core::platform::process::spawn_detached(
        Path::new("cmd.exe"),
        &["/d", "/c", "exit", "0"],
        None,
        &environment,
    ) {
        Ok(probe) => probe,
        Err(error) if error.to_string().contains("Access is denied") => {
            eprintln!("Windows job denied safe helper breakaway; Surge failed closed before handoff");
            return false;
        }
        Err(error) => panic!("detached helper capability probe failed unexpectedly: {error}"),
    };
    assert_eq!(probe.wait().unwrap().exit_code, 0);
    true
}

fn required_path(name: &str) -> PathBuf {
    PathBuf::from(std::env::var_os(name).unwrap_or_else(|| panic!("missing {name}")))
}

fn write_current_runtime_manifest(active_app_dir: &Path) {
    let shortcuts = [];
    let persistent_assets = [];
    let environment = BTreeMap::new();
    let profile = InstallProfile::new(
        APP_ID,
        "External finalizer fixture",
        UPDATER_EXE,
        APP_ID,
        SUPERVISOR_ID,
        "",
        &shortcuts,
        &persistent_assets,
        &environment,
    );
    let metadata = RuntimeManifestMetadata::new(CURRENT_VERSION, "test", "filesystem", ".", "", "");
    write_runtime_manifest(active_app_dir, &profile, &metadata).unwrap();
}

#[allow(clippy::too_many_arguments)]
fn write_target_release(
    store_root: &Path,
    fixture_exe: &Path,
    install_root: &Path,
    target_started_marker: &Path,
    target_hook_status_marker: &Path,
    target_status_marker: &Path,
    target_args_marker: &Path,
    target_environment_marker: &Path,
    target_executable_marker: &Path,
    target_pid_path: &Path,
    target_stop_path: &Path,
) {
    let app_store = store_root.join(APP_ID);
    std::fs::create_dir_all(&app_store).unwrap();
    let full_filename = format!("{APP_ID}-{TARGET_VERSION}-full.tar.zst");
    let full_path = app_store.join(&full_filename);
    let mut packer = ArchivePacker::new(DEFAULT_ZSTD_LEVEL).unwrap();
    packer.add_file(fixture_exe, TARGET_EXE).unwrap();
    packer
        .add_file(
            Path::new(env!("CARGO_BIN_EXE_surge-supervisor")),
            supervisor_binary_name(),
        )
        .unwrap();
    packer.finalize_to_file(&full_path).unwrap();

    let environment = BTreeMap::from([
        (MODE_ENV.to_string(), TARGET_MODE.to_string()),
        (
            "SURGE_EXTERNAL_FINALIZE_INSTALL".to_string(),
            install_root.to_string_lossy().into_owned(),
        ),
        (
            "SURGE_EXTERNAL_FINALIZE_TARGET_STARTED".to_string(),
            target_started_marker.to_string_lossy().into_owned(),
        ),
        (
            "SURGE_EXTERNAL_FINALIZE_TARGET_HOOK_STATUS".to_string(),
            target_hook_status_marker.to_string_lossy().into_owned(),
        ),
        (
            "SURGE_EXTERNAL_FINALIZE_TARGET_STATUS".to_string(),
            target_status_marker.to_string_lossy().into_owned(),
        ),
        (
            "SURGE_EXTERNAL_FINALIZE_TARGET_ARGS".to_string(),
            target_args_marker.to_string_lossy().into_owned(),
        ),
        (
            "SURGE_EXTERNAL_FINALIZE_TARGET_ENVIRONMENT".to_string(),
            target_environment_marker.to_string_lossy().into_owned(),
        ),
        (
            "SURGE_EXTERNAL_FINALIZE_TARGET_EXECUTABLE".to_string(),
            target_executable_marker.to_string_lossy().into_owned(),
        ),
        (
            "SURGE_EXTERNAL_FINALIZE_TARGET_PID".to_string(),
            target_pid_path.to_string_lossy().into_owned(),
        ),
        (
            "SURGE_EXTERNAL_FINALIZE_TARGET_STOP".to_string(),
            target_stop_path.to_string_lossy().into_owned(),
        ),
        (
            "SURGE_EXTERNAL_FINALIZE_TARGET_VALUE".to_string(),
            TARGET_ENV_VALUE.to_string(),
        ),
    ]);
    let release = ReleaseEntry {
        version: TARGET_VERSION.to_string(),
        channels: vec!["test".to_string()],
        rid: current_rid(),
        is_genesis: true,
        full_filename,
        full_size: i64::try_from(std::fs::metadata(&full_path).unwrap().len()).unwrap(),
        full_sha256: sha256_hex_file(&full_path).unwrap(),
        name: "External finalizer fixture".to_string(),
        main_exe: TARGET_EXE.to_string(),
        install_directory: APP_ID.to_string(),
        supervisor_id: SUPERVISOR_ID.to_string(),
        environment,
        ..ReleaseEntry::default()
    };
    let index = ReleaseIndex {
        app_id: APP_ID.to_string(),
        releases: vec![release],
        ..ReleaseIndex::default()
    };
    std::fs::write(
        app_store.join(RELEASES_FILE_COMPRESSED),
        compress_release_index(&index, DEFAULT_ZSTD_LEVEL).unwrap(),
    )
    .unwrap();
}

#[cfg(unix)]
fn make_executable(path: &Path) {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = std::fs::metadata(path).unwrap().permissions();
    permissions.set_mode(permissions.mode() | 0o111);
    std::fs::set_permissions(path, permissions).unwrap();
}

#[cfg(not(unix))]
fn make_executable(_path: &Path) {}

fn wait_until(timeout: Duration, mut condition: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if condition() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    condition()
}

fn stop_fixture_processes(install_root: &Path, target_stop_path: &Path, target_pid_path: &Path) {
    let supervisor_stop = install_root.join(format!(".surge-supervisor-{SUPERVISOR_ID}.stop"));
    let supervisor_pid = install_root.join(format!(".surge-supervisor-{SUPERVISOR_ID}.pid"));
    std::fs::write(&supervisor_stop, "test cleanup").unwrap();
    assert!(wait_until(Duration::from_secs(5), || !supervisor_pid.exists()));
    std::fs::write(target_stop_path, "stop").unwrap();

    let target_pid = std::fs::read_to_string(target_pid_path)
        .unwrap()
        .trim()
        .parse::<u32>()
        .unwrap();
    assert!(wait_until(Duration::from_secs(5), || {
        !surge_core::platform::process::is_pid_alive(target_pid)
    }));
}
