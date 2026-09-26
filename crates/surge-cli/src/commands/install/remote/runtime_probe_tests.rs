use super::build_remote_process_verification_probe;
use std::fs;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Processes(Vec<Child>);

impl Drop for Processes {
    fn drop(&mut self) {
        for child in &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn spawn(command: &mut Command) -> Child {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match command.spawn() {
            Ok(child) => return child,
            Err(error) if error.kind() == std::io::ErrorKind::ExecutableFileBusy && Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("Could not start fixture process: {error}"),
        }
    }
}

fn run_probe(root: &Path) -> String {
    let probe = build_remote_process_verification_probe(root, "demoapp", "demo-supervisor", "1.2.3");
    let output = Command::new("sh").args(["-c", &probe]).output().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

fn fixture() -> tempfile::TempDir {
    let root = tempfile::tempdir().unwrap();
    let app = root.path().join("app");
    fs::create_dir(&app).unwrap();
    fs::copy("/bin/sleep", app.join("demoapp")).unwrap();
    fs::copy("/bin/sh", app.join("surge-supervisor")).unwrap();
    fs::write(
        root.path().join(".surge-update-status.json"),
        r#"{"state":"converged","installed_version":"1.2.3","target_version":"1.2.3"}"#,
    )
    .unwrap();
    root
}

fn spawn_supervisor(root: &Path, id: &str, watched: u32, processes: &mut Processes) -> u32 {
    spawn_supervisor_with_args(root, id, watched, &[], processes)
}

fn spawn_supervisor_with_args(
    root: &Path,
    id: &str,
    watched: u32,
    child_args: &[&str],
    processes: &mut Processes,
) -> u32 {
    let child_pid = root.join("child.pid");
    let child = spawn(
        Command::new(root.join("app/surge-supervisor"))
            .arg("-c")
            .arg(r#"bash -c 'shopt -s execfail; for attempt in {1..200}; do exec "$1" 30; sleep 0.01; done; exit 1' child "$1" & child=$!; echo "$child" > "$2"; wait "$child""#)
            .arg("watch")
            .arg(fs::canonicalize(root.join("app/demoapp")).unwrap())
            .arg(&child_pid)
            .args(["--id", id, "--pid", &watched.to_string()])
            .args(child_args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    );
    let pid = child.id();
    processes.0.push(child);
    let deadline = Instant::now() + Duration::from_secs(5);
    let expected_exe = fs::canonicalize(root.join("app/demoapp")).unwrap();
    loop {
        let ready = fs::read_to_string(&child_pid)
            .ok()
            .and_then(|value| value.trim().parse::<u32>().ok())
            .and_then(|pid| fs::read_link(format!("/proc/{pid}/exe")).ok())
            .is_some_and(|exe| exe == expected_exe);
        if ready {
            break;
        }
        assert!(Instant::now() < deadline, "child did not exec the fixture app");
        std::thread::sleep(Duration::from_millis(10));
    }
    pid
}

fn stop_spawned_child(root: &Path) {
    let pid = fs::read_to_string(root.join("child.pid")).unwrap();
    let _ = Command::new("kill").arg(pid.trim()).status();
}

#[test]
fn process_probe_accepts_current_child_after_watched_process_exits() {
    let root = fixture();
    let mut processes = Processes(Vec::new());
    let old = Command::new("true").spawn().unwrap();
    let old_pid = old.id();
    processes.0.push(old);
    processes.0[0].wait().unwrap();
    spawn_supervisor(root.path(), "demo-supervisor", old_pid, &mut processes);
    let result = run_probe(root.path());
    stop_spawned_child(root.path());
    assert_eq!(result, "ready");
}

#[test]
fn process_probe_rejects_child_of_another_supervisor() {
    let root = fixture();
    let mut processes = Processes(Vec::new());
    spawn_supervisor(root.path(), "other-supervisor", 999_999, &mut processes);
    let result = run_probe(root.path());
    stop_spawned_child(root.path());
    assert_ne!(result, "ready");
}

#[test]
fn process_probe_rejects_unrelated_app_with_matching_supervisor_arguments() {
    let root = fixture();
    let mut processes = Processes(Vec::new());
    let unrelated = spawn(Command::new(root.path().join("app/demoapp")).arg("30"));
    processes.0.push(unrelated);
    let supervisor = spawn(
        Command::new(root.path().join("app/surge-supervisor"))
            .args([
                "-c",
                "read answer",
                "watch",
                "--id",
                "demo-supervisor",
                "--pid",
                "999999",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    );
    processes.0.push(supervisor);
    assert_ne!(run_probe(root.path()), "ready");
}

#[test]
fn process_probe_rejects_current_child_without_target_version_proof() {
    let root = fixture();
    let mut processes = Processes(Vec::new());
    fs::write(
        root.path().join(".surge-update-status.json"),
        r#"{"state":"converged","installed_version":"1.2.2","target_version":"1.2.2"}"#,
    )
    .unwrap();
    spawn_supervisor(root.path(), "demo-supervisor", 999_999, &mut processes);
    let result = run_probe(root.path());
    stop_spawned_child(root.path());
    assert_ne!(result, "ready");
}

#[test]
fn process_probe_rejects_superseded_app_alongside_current_child() {
    let root = fixture();
    let mut processes = Processes(Vec::new());
    let retained = root.path().join("app-1.2.2");
    fs::create_dir(&retained).unwrap();
    fs::copy("/bin/sleep", retained.join("demoapp")).unwrap();
    processes
        .0
        .push(spawn(Command::new(retained.join("demoapp")).arg("30")));
    spawn_supervisor(root.path(), "demo-supervisor", 999_999, &mut processes);
    let result = run_probe(root.path());
    stop_spawned_child(root.path());
    assert_ne!(result, "ready");
}

#[test]
fn process_probe_rejects_supervisor_id_prefix_match() {
    let root = fixture();
    let mut processes = Processes(Vec::new());
    spawn_supervisor(root.path(), "demo-supervisor-other", 999_999, &mut processes);
    let result = run_probe(root.path());
    stop_spawned_child(root.path());
    assert_ne!(result, "ready");
}

#[test]
fn process_probe_rejects_supervisor_id_in_forwarded_child_arguments() {
    let root = fixture();
    let mut processes = Processes(Vec::new());
    spawn_supervisor_with_args(
        root.path(),
        "other-supervisor",
        999_999,
        &["--", "--id", "demo-supervisor"],
        &mut processes,
    );
    let result = run_probe(root.path());
    stop_spawned_child(root.path());
    assert_ne!(result, "ready");
}

#[test]
fn process_probe_rejects_unrelated_supervisor_with_target_first_run_argument() {
    let root = fixture();
    let mut processes = Processes(Vec::new());
    processes
        .0
        .push(spawn(Command::new(root.path().join("app/demoapp")).arg("30")));
    processes.0.push(spawn(
        Command::new(root.path().join("app/surge-supervisor"))
            .args([
                "-c",
                "read answer",
                "run",
                "--id",
                "demo-supervisor",
                "--",
                "--surge-first-run",
                "1.2.3",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    ));
    assert_ne!(run_probe(root.path()), "ready");
}

fn probe_original_watched_app(valid_start_time: Option<bool>) -> String {
    let root = fixture();
    let mut processes = Processes(Vec::new());
    let app = spawn(Command::new(root.path().join("app/demoapp")).arg("30"));
    let app_pid = app.id();
    processes.0.push(app);
    let mut supervisor = Command::new(root.path().join("app/surge-supervisor"));
    supervisor.args([
        "-c",
        "read answer",
        "watch",
        "--id",
        "demo-supervisor",
        "--pid",
        &app_pid.to_string(),
    ]);
    if let Some(valid) = valid_start_time {
        let start = surge_core::platform::process::process_start_time(app_pid).unwrap();
        supervisor.args(["--pid-start-time", &(if valid { start } else { start + 1 }).to_string()]);
    }
    processes.0.push(spawn(
        supervisor
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    ));
    run_probe(root.path())
}

#[test]
fn process_probe_accepts_original_watched_app() {
    assert_eq!(probe_original_watched_app(Some(true)), "ready");
}

#[test]
fn process_probe_rejects_reused_watched_pid_identity() {
    assert_ne!(probe_original_watched_app(Some(false)), "ready");
}

#[test]
fn process_probe_requires_parent_proof_for_legacy_watch_without_start_time() {
    assert!(probe_original_watched_app(None).contains("lacks start-time identity"));
}

#[test]
fn process_probe_accepts_in_tree_symlink_after_respawn() {
    let root = fixture();
    let app = root.path().join("app");
    fs::create_dir(app.join("bin")).unwrap();
    fs::rename(app.join("demoapp"), app.join("bin/actual-app")).unwrap();
    std::os::unix::fs::symlink("bin/actual-app", app.join("demoapp")).unwrap();
    let mut processes = Processes(Vec::new());
    spawn_supervisor(root.path(), "demo-supervisor", 999_999, &mut processes);
    let result = run_probe(root.path());
    stop_spawned_child(root.path());
    assert_eq!(result, "ready");
}

#[test]
fn process_probe_rejects_out_of_tree_symlink() {
    let root = fixture();
    let app = root.path().join("app");
    fs::rename(app.join("demoapp"), root.path().join("outside-app")).unwrap();
    std::os::unix::fs::symlink("../outside-app", app.join("demoapp")).unwrap();
    let mut processes = Processes(Vec::new());
    spawn_supervisor(root.path(), "demo-supervisor", 999_999, &mut processes);
    let result = run_probe(root.path());
    stop_spawned_child(root.path());
    assert_ne!(result, "ready");
}

#[test]
fn process_probe_rejects_retained_symlink_target_changed_between_releases() {
    let root = fixture();
    let app = root.path().join("app");
    let retained = root.path().join("app-1.2.2/lib");
    fs::create_dir(app.join("bin")).unwrap();
    fs::create_dir_all(&retained).unwrap();
    fs::rename(app.join("demoapp"), app.join("bin/actual-app")).unwrap();
    fs::copy("/bin/sleep", retained.join("old-app")).unwrap();
    std::os::unix::fs::symlink("lib/old-app", root.path().join("app-1.2.2/demoapp")).unwrap();
    std::os::unix::fs::symlink("bin/actual-app", app.join("demoapp")).unwrap();
    let mut processes = Processes(Vec::new());
    processes
        .0
        .push(spawn(Command::new(retained.join("old-app")).arg("30")));
    spawn_supervisor(root.path(), "demo-supervisor", 999_999, &mut processes);
    let result = run_probe(root.path());
    stop_spawned_child(root.path());
    assert_ne!(result, "ready");
}

#[test]
fn process_probe_rejects_watched_process_with_spoofed_app_arguments() {
    let root = fixture();
    let mut processes = Processes(Vec::new());
    let unrelated = spawn(
        Command::new("/bin/sh")
            .args(["-c", "read answer"])
            .arg(root.path().join("app/demoapp"))
            .args(["--surge-first-run", "1.2.3"])
            .stdin(Stdio::piped()),
    );
    let watched = unrelated.id().to_string();
    processes.0.push(unrelated);
    processes.0.push(spawn(
        Command::new(root.path().join("app/surge-supervisor"))
            .args([
                "-c",
                "read answer",
                "watch",
                "--id",
                "demo-supervisor",
                "--pid",
                &watched,
            ])
            .stdin(Stdio::piped()),
    ));
    assert_ne!(run_probe(root.path()), "ready");
}

#[test]
fn process_probe_rejects_process_using_replaced_active_executable() {
    let root = fixture();
    let mut processes = Processes(Vec::new());
    spawn_supervisor(root.path(), "demo-supervisor", 999_999, &mut processes);
    fs::remove_file(root.path().join("app/demoapp")).unwrap();
    fs::copy("/bin/sleep", root.path().join("app/demoapp")).unwrap();
    let result = run_probe(root.path());
    stop_spawned_child(root.path());
    assert_ne!(result, "ready");
}
