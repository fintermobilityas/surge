use std::process::{Child, Command, Stdio};
use std::time::Instant;

use super::*;

struct TestApplication {
    child: Child,
    identity: ProcessIdentity,
    directory: tempfile::TempDir,
}

impl TestApplication {
    fn start() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let executable = directory.path().join("threaded-application");
        std::fs::copy(std::env::current_exe().unwrap(), &executable).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        let child = loop {
            match Command::new(&executable)
                .args([
                    "--exact",
                    "update::manager::external_finalize::quiesce::linux_tests::threaded_application_helper",
                    "--nocapture",
                ])
                .env("SURGE_QUIESCE_TEST_DIRECTORY", directory.path())
                .stdout(Stdio::null())
                .spawn()
            {
                Ok(child) => break child,
                Err(error) if error.raw_os_error() == Some(nix::libc::ETXTBSY) && Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("failed to start threaded application: {error}"),
            }
        };
        let identity = ProcessIdentity {
            pid: child.id(),
            start_time: process_start_time(child.id()).unwrap(),
            executable,
        };
        let app = Self {
            child,
            identity,
            directory,
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        while !app.directory.path().join("ready").exists() {
            assert!(Instant::now() < deadline, "threaded application did not start");
            std::thread::sleep(Duration::from_millis(10));
        }
        app
    }

    fn request_exit(&self) {
        std::fs::write(self.directory.path().join("exit"), []).unwrap();
    }
}

impl Drop for TestApplication {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn threaded_application_helper() {
    let Some(directory) = std::env::var_os("SURGE_QUIESCE_TEST_DIRECTORY") else {
        return;
    };
    let directory = PathBuf::from(directory);
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let exit = directory.join("exit");
            std::thread::spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(30);
                while !exit.exists() && Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(1));
                }
            })
        })
        .collect();
    std::fs::write(directory.join("ready"), []).unwrap();
    for thread in threads {
        thread.join().unwrap();
    }
}

#[test]
fn enumeration_excludes_threads_and_quiescence_waits_for_application_exit() {
    let mut app = TestApplication::start();
    let mut unrelated = TestApplication::start();
    assert_eq!(
        matching_processes(&app.identity.executable).unwrap(),
        [app.identity.clone()]
    );
    assert!(identity_is_running(&app.identity).unwrap());

    app.request_exit();
    quiesce_updating_application_with_timeouts(
        app.identity.pid,
        app.identity.start_time,
        &app.identity.executable,
        Duration::from_secs(5),
        Duration::from_secs(2),
        Duration::from_secs(1),
    )
    .unwrap();

    assert!(app.child.wait().unwrap().success());
    assert!(!identity_is_running(&app.identity).unwrap());
    assert!(matching_processes(&app.identity.executable).unwrap().is_empty());
    assert!(unrelated.child.try_wait().unwrap().is_none());
}

#[test]
fn metadata_errors_during_exit_are_revalidated_before_failing() {
    for reap in [false, true] {
        let mut app = TestApplication::start();
        let identity = app.identity.clone();
        let running = identity_is_running_with_metadata(&identity, || {
            app.request_exit();
            if reap {
                assert!(app.child.wait().unwrap().success());
            } else {
                let deadline = Instant::now() + Duration::from_secs(5);
                while probe_process_identity(identity.pid, identity.start_time) != PidLiveness::Dead {
                    assert!(Instant::now() < deadline, "application did not exit");
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
            Err(SurgeError::Supervisor("metadata vanished during exit".into()))
        })
        .unwrap();
        assert!(!running);
    }
}

#[test]
fn unverifiable_live_metadata_fails_closed_without_signalling() {
    let mut app = TestApplication::start();
    let error = identity_is_running_with_metadata(&app.identity, || {
        Err(SurgeError::Supervisor("live metadata unavailable".into()))
    })
    .unwrap_err();
    assert!(error.to_string().contains("live metadata unavailable"));
    assert!(app.child.try_wait().unwrap().is_none());
}

#[test]
fn repeated_scans_tolerate_multithreaded_shutdown() {
    let mut app = TestApplication::start();
    app.request_exit();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let processes = matching_processes(&app.identity.executable).unwrap();
        assert!(processes.iter().all(|process| process.pid == app.identity.pid));
        if !identity_is_running(&app.identity).unwrap() {
            break;
        }
        assert!(Instant::now() < deadline, "application did not exit");
    }
    assert!(app.child.wait().unwrap().success());
}

#[test]
fn exited_leader_with_live_worker_is_force_stopped_before_activation() {
    use std::os::unix::process::ExitStatusExt;

    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("exited_leader.c");
    let executable = directory.path().join("exited-leader");
    std::fs::write(&source, include_str!("exited_leader.c")).unwrap();
    let compile = Command::new("cc")
        .arg("-pthread")
        .arg(&source)
        .arg("-o")
        .arg(&executable)
        .output()
        .unwrap();
    assert!(compile.status.success(), "{}", String::from_utf8_lossy(&compile.stderr));
    let child = Command::new(&executable).spawn().unwrap();
    let identity = ProcessIdentity {
        pid: child.id(),
        start_time: process_start_time(child.id()).unwrap(),
        executable,
    };
    let mut app = TestApplication {
        child,
        identity,
        directory,
    };
    let mut unrelated = TestApplication::start();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let stat = std::fs::read_to_string(format!("/proc/{}/stat", app.identity.pid)).unwrap();
        if stat.rsplit_once(')').unwrap().1.trim_start().starts_with('Z') {
            break;
        }
        assert!(Instant::now() < deadline, "leader did not exit");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        probe_process_identity(app.identity.pid, app.identity.start_time),
        PidLiveness::Alive
    );
    assert!(identity_is_running(&app.identity).unwrap());
    assert_eq!(
        matching_processes(&app.identity.executable).unwrap(),
        [app.identity.clone()]
    );
    quiesce_updating_application_with_timeouts(
        app.identity.pid,
        app.identity.start_time,
        &app.identity.executable,
        Duration::ZERO,
        Duration::from_millis(100),
        Duration::from_secs(2),
    )
    .unwrap();
    assert_eq!(app.child.wait().unwrap().signal(), Some(nix::libc::SIGKILL));
    assert!(unrelated.child.try_wait().unwrap().is_none());
}
