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
    let child_pid = root.join("child.pid");
    let child = spawn(
        Command::new(root.join("app/surge-supervisor"))
            .arg("-c")
            .arg(r#""$1" 30 & child=$!; echo "$child" > "$2"; wait "$child""#)
            .arg("watch")
            .arg(root.join("app/demoapp"))
            .arg(&child_pid)
            .args(["--id", id, "--pid", &watched.to_string()])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    );
    let pid = child.id();
    processes.0.push(child);
    let deadline = Instant::now() + Duration::from_secs(5);
    while !child_pid.exists() {
        assert!(Instant::now() < deadline, "child did not start");
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
