//! Fixtures for tests that run a system program after renaming or linking it.
//!
//! Multicall coreutils dispatch on `argv[0]`, so a fixture that copies `sleep` under another
//! name only works when the source is a standalone program.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// Programs that may provide a standalone `sleep`, most likely first.
const SLEEP_CANDIDATES: [&str; 3] = ["/bin/sleep", "/usr/bin/sleep", "/usr/bin/gnusleep"];

/// Foreign file name used to prove a candidate runs without its own `argv[0]`.
const PROBE_NAME: &str = "surge-fixture-sleep-probe";

const SPAWN_RETRY_LIMIT: Duration = Duration::from_secs(2);

struct SleepFixture {
    _directory: Option<tempfile::TempDir>,
    path: PathBuf,
}

/// A `sleep` program that keeps working when copied, renamed or symlinked into a fixture.
pub(crate) fn standalone_sleep_binary() -> &'static Path {
    static SLEEP: OnceLock<SleepFixture> = OnceLock::new();
    &SLEEP.get_or_init(resolve_standalone_sleep).path
}

fn resolve_standalone_sleep() -> SleepFixture {
    let standalone = SLEEP_CANDIDATES
        .iter()
        .map(Path::new)
        .find(|candidate| candidate.is_file() && runs_under_foreign_name(candidate));
    match standalone {
        Some(source) => SleepFixture {
            _directory: None,
            path: source.to_path_buf(),
        },
        None => compile_sleep_fixture().unwrap_or_else(|| {
            panic!(
                "no standalone sleep program is available: none of [{}] still runs when copied to \
                 `{PROBE_NAME}`, and compiling a replacement with `cc` failed",
                SLEEP_CANDIDATES.join(", ")
            )
        }),
    }
}

fn runs_under_foreign_name(source: &Path) -> bool {
    let Ok(directory) = tempfile::tempdir() else {
        return false;
    };
    let probe = directory.path().join(PROBE_NAME);
    let copied = std::fs::copy(source, &probe).is_ok();
    copied && run_with_retry(Command::new(&probe).arg("0")).is_some_and(|status| status.success())
}

fn compile_sleep_fixture() -> Option<SleepFixture> {
    const SOURCE: &str = "\
#include <stdlib.h>
#include <unistd.h>

int main(int argc, char **argv) {
    sleep(argc > 1 ? (unsigned)atoi(argv[1]) : 30u);
    return 0;
}
";

    let directory = tempfile::tempdir().ok()?;
    let source_path = directory.path().join("standalone-sleep.c");
    std::fs::write(&source_path, SOURCE).ok()?;
    let binary = directory.path().join("standalone-sleep");
    let mut compile = Command::new("cc");
    compile.args(["-O0", "-o"]).arg(&binary).arg(&source_path);
    let status = run_with_retry(&mut compile)?;
    (status.success() && binary.is_file()).then(|| SleepFixture {
        _directory: Some(directory),
        path: binary,
    })
}

fn run_with_retry(command: &mut Command) -> Option<ExitStatus> {
    let deadline = Instant::now() + SPAWN_RETRY_LIMIT;
    loop {
        match command.stdin(Stdio::null()).output() {
            Ok(output) => return Some(output.status),
            Err(error) if error.kind() == ErrorKind::ExecutableFileBusy && Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(_) => return None,
        }
    }
}
