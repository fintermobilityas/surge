use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use tracing::warn;

use crate::platform::fs::atomic_rename;
use crate::releases::manifest::ReleaseEntry;

use super::lifecycle::{self, SupervisorRestartOutcome};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RecoveredGeneration {
    Previous,
    Target,
}

pub(super) struct Guard {
    install_dir: PathBuf,
    current_app_dir: Option<PathBuf>,
    active_app_dir: PathBuf,
    next_app_dir: PathBuf,
    previous_swap_dir: PathBuf,
    previous: ReleaseEntry,
    target: ReleaseEntry,
    previous_moved: bool,
    target_staged: bool,
    target_active: bool,
    armed: bool,
}

impl Guard {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        install_dir: &Path,
        current_app_dir: Option<&Path>,
        active_app_dir: &Path,
        next_app_dir: &Path,
        previous_swap_dir: &Path,
        current_version: &str,
        current_main_exe: &str,
        current_supervisor_id: &str,
        current_environment: Option<&BTreeMap<String, String>>,
        target: &ReleaseEntry,
    ) -> Self {
        Self {
            install_dir: install_dir.to_path_buf(),
            current_app_dir: current_app_dir.map(Path::to_path_buf),
            active_app_dir: active_app_dir.to_path_buf(),
            next_app_dir: next_app_dir.to_path_buf(),
            previous_swap_dir: previous_swap_dir.to_path_buf(),
            previous: ReleaseEntry {
                version: current_version.to_string(),
                main_exe: current_main_exe.to_string(),
                supervisor_id: current_supervisor_id.to_string(),
                environment: current_environment.cloned().unwrap_or_default(),
                ..ReleaseEntry::default()
            },
            target: target.clone(),
            previous_moved: false,
            target_staged: false,
            target_active: false,
            armed: true,
        }
    }

    pub(super) fn mark_target_staged(&mut self) {
        self.target_staged = true;
    }

    pub(super) fn mark_previous_moved(&mut self) {
        self.previous_moved = true;
    }

    pub(super) fn mark_target_active(&mut self) {
        self.target_staged = false;
        self.target_active = true;
    }

    pub(super) fn complete_supervisor_restart(&mut self) {
        self.armed = false;
    }

    pub(super) fn recover(&mut self) -> Option<RecoveredGeneration> {
        if !self.armed {
            return None;
        }
        self.armed = false;

        warn!("Finalize failed after application quiescence; recovering application supervision");
        let Some(start) = self.recovery_start() else {
            warn!("Finalize recovery could not find a usable previous or target application directory");
            return None;
        };
        let release = match start.generation {
            RecoveredGeneration::Previous => &self.previous,
            RecoveredGeneration::Target => &self.target,
        };
        let supervisor_outcome = lifecycle::restart_supervisor_immediately(&self.install_dir, &start.app_dir, release);
        match &supervisor_outcome {
            SupervisorRestartOutcome::NotApplicable => {
                warn!(app = %start.app_dir.display(), "Finalize recovery has no configured supervisor to restart");
            }
            SupervisorRestartOutcome::PendingRestart { reason, failure_phase } => {
                warn!(
                    app = %start.app_dir.display(),
                    target = matches!(start.generation, RecoveredGeneration::Target),
                    reason,
                    failure_phase,
                    "Finalize recovery requested application supervision"
                );
            }
        }
        Some(start.generation)
    }

    fn recovery_start(&mut self) -> Option<RecoveryStart> {
        if self.target_active && self.active_app_dir.is_dir() && !self.move_target_aside() {
            return Some(RecoveryStart::target(self.active_app_dir.clone()));
        }

        if self.previous_moved && self.previous_swap_dir.is_dir() {
            if !self.active_app_dir.exists() {
                match atomic_rename(&self.previous_swap_dir, &self.active_app_dir) {
                    Ok(()) => return Some(RecoveryStart::previous(self.active_app_dir.clone())),
                    Err(error) => {
                        warn!(
                            previous = %self.previous_swap_dir.display(),
                            active = %self.active_app_dir.display(),
                            %error,
                            "Failed to restore the previous application directory after finalize failed"
                        );
                        if let Some(target) = self.restore_target_to_canonical_path() {
                            return Some(target);
                        }
                    }
                }
            }
            return Some(RecoveryStart::previous(self.previous_swap_dir.clone()));
        }

        if let Some(current_app_dir) = &self.current_app_dir
            && current_app_dir.is_dir()
        {
            return Some(RecoveryStart::previous(current_app_dir.clone()));
        }

        if self.target_staged || self.next_app_dir.is_dir() {
            return self.restore_target_to_canonical_path().or_else(|| {
                self.next_app_dir
                    .is_dir()
                    .then(|| RecoveryStart::target(self.next_app_dir.clone()))
            });
        }

        if self.target_active && self.active_app_dir.is_dir() {
            return Some(RecoveryStart::target(self.active_app_dir.clone()));
        }
        None
    }

    fn move_target_aside(&mut self) -> bool {
        if self.next_app_dir.exists() {
            warn!(
                target = %self.active_app_dir.display(),
                next = %self.next_app_dir.display(),
                "Cannot move the failed target application aside because the next directory already exists"
            );
            return false;
        }
        match atomic_rename(&self.active_app_dir, &self.next_app_dir) {
            Ok(()) => {
                self.target_active = false;
                self.target_staged = true;
                true
            }
            Err(error) => {
                warn!(
                    target = %self.active_app_dir.display(),
                    next = %self.next_app_dir.display(),
                    %error,
                    "Failed to move the target application aside during finalize recovery"
                );
                false
            }
        }
    }

    fn restore_target_to_canonical_path(&mut self) -> Option<RecoveryStart> {
        if self.active_app_dir.is_dir() {
            return self
                .target_active
                .then(|| RecoveryStart::target(self.active_app_dir.clone()));
        }
        if !self.next_app_dir.is_dir() {
            return None;
        }
        match atomic_rename(&self.next_app_dir, &self.active_app_dir) {
            Ok(()) => {
                self.target_staged = false;
                self.target_active = true;
                Some(RecoveryStart::target(self.active_app_dir.clone()))
            }
            Err(error) => {
                warn!(
                    target = %self.next_app_dir.display(),
                    active = %self.active_app_dir.display(),
                    %error,
                    "Failed to restore the target application to the canonical directory during finalize recovery"
                );
                None
            }
        }
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        let _ = self.recover();
    }
}

struct RecoveryStart {
    app_dir: PathBuf,
    generation: RecoveredGeneration,
}

impl RecoveryStart {
    fn previous(app_dir: PathBuf) -> Self {
        Self {
            app_dir,
            generation: RecoveredGeneration::Previous,
        }
    }

    fn target(app_dir: PathBuf) -> Self {
        Self {
            app_dir,
            generation: RecoveredGeneration::Target,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    #[test]
    fn pre_swap_failure_restarts_previous_supervisor_with_environment() {
        let fixture = RecoveryFixture::new();
        write_supervised_app(fixture.install_dir(), &fixture.active_app_dir, "old-app", "old");
        let previous_environment = BTreeMap::from([("RECOVERY_MARKER".to_string(), "preserved".to_string())]);
        let mut guard = fixture.guard(Some(&previous_environment));

        let recovery = guard.recover().unwrap();

        assert_eq!(recovery, RecoveredGeneration::Previous);
        assert_eq!(fixture.recovery_command(), "run\n");
        assert_eq!(fixture.recovery_environment(), "preserved\n");
    }

    #[test]
    fn post_swap_failure_rolls_back_and_restarts_previous_supervisor() {
        let fixture = RecoveryFixture::new();
        write_supervised_app(fixture.install_dir(), &fixture.active_app_dir, "target-app", "target");
        write_supervised_app(fixture.install_dir(), &fixture.previous_swap_dir, "old-app", "previous");
        let mut guard = fixture.guard(None);
        guard.mark_target_staged();
        guard.mark_previous_moved();
        guard.mark_target_active();

        let recovery = guard.recover().unwrap();

        assert_eq!(recovery, RecoveredGeneration::Previous);
        assert_eq!(
            std::fs::read_to_string(fixture.active_app_dir.join("generation")).unwrap(),
            "previous"
        );
        assert_eq!(
            std::fs::read_to_string(fixture.next_app_dir.join("generation")).unwrap(),
            "target"
        );
        assert_eq!(fixture.recovery_command(), "run\n");
    }

    #[test]
    fn first_install_failure_restores_staged_target_to_canonical_path() {
        let fixture = RecoveryFixture::new_without_current();
        write_supervised_app(fixture.install_dir(), &fixture.next_app_dir, "target-app", "target");
        let mut guard = fixture.guard(None);
        guard.mark_target_staged();

        let recovery = guard.recover().unwrap();

        assert_eq!(recovery, RecoveredGeneration::Target);
        assert_eq!(
            std::fs::read_to_string(fixture.active_app_dir.join("generation")).unwrap(),
            "target"
        );
        assert!(!fixture.next_app_dir.exists());
    }

    #[test]
    fn unexpected_active_directory_does_not_override_known_previous_swap() {
        let fixture = RecoveryFixture::new();
        write_supervised_app(fixture.install_dir(), &fixture.active_app_dir, "unknown-app", "unknown");
        write_supervised_app(fixture.install_dir(), &fixture.previous_swap_dir, "old-app", "previous");
        let mut guard = fixture.guard(None);
        guard.mark_previous_moved();

        let recovery = guard.recover().unwrap();

        assert_eq!(recovery, RecoveredGeneration::Previous);
        assert_eq!(
            crate::supervisor::state::read_supervisor_exe_path(fixture.install_dir(), "demo-supervisor").as_deref(),
            Some(fixture.previous_swap_dir.join("old-app").as_path())
        );
    }

    #[test]
    fn completed_target_restart_does_not_roll_back_pending_handoff() {
        let fixture = RecoveryFixture::new();
        write_supervised_app(fixture.install_dir(), &fixture.active_app_dir, "target-app", "target");
        write_supervised_app(fixture.install_dir(), &fixture.previous_swap_dir, "old-app", "previous");
        let mut guard = fixture.guard(None);
        guard.mark_previous_moved();
        guard.mark_target_active();
        guard.complete_supervisor_restart();

        drop(guard);

        assert_eq!(
            std::fs::read_to_string(fixture.active_app_dir.join("generation")).unwrap(),
            "target"
        );
        assert!(!fixture.next_app_dir.exists());
    }

    struct RecoveryFixture {
        temp: tempfile::TempDir,
        current_app_dir: Option<PathBuf>,
        active_app_dir: PathBuf,
        next_app_dir: PathBuf,
        previous_swap_dir: PathBuf,
    }

    impl RecoveryFixture {
        fn new() -> Self {
            Self::new_with_current(true)
        }

        fn new_without_current() -> Self {
            Self::new_with_current(false)
        }

        fn new_with_current(has_current: bool) -> Self {
            let temp = tempfile::tempdir().unwrap();
            let active_app_dir = temp.path().join("app");
            Self {
                current_app_dir: has_current.then(|| active_app_dir.clone()),
                next_app_dir: temp.path().join(".surge-app-next"),
                previous_swap_dir: temp.path().join(".surge-app-prev"),
                active_app_dir,
                temp,
            }
        }

        fn install_dir(&self) -> &Path {
            self.temp.path()
        }

        fn guard(&self, environment: Option<&BTreeMap<String, String>>) -> Guard {
            Guard::new(
                self.install_dir(),
                self.current_app_dir.as_deref(),
                &self.active_app_dir,
                &self.next_app_dir,
                &self.previous_swap_dir,
                "1.0.0",
                "old-app",
                "demo-supervisor",
                environment,
                &release("2.0.0", "target-app"),
            )
        }

        fn recovery_command(&self) -> String {
            std::fs::read_to_string(self.install_dir().join("recovery-command")).unwrap()
        }

        fn recovery_environment(&self) -> String {
            std::fs::read_to_string(self.install_dir().join("recovery-environment")).unwrap()
        }
    }

    fn release(version: &str, main_exe: &str) -> ReleaseEntry {
        ReleaseEntry {
            version: version.to_string(),
            main_exe: main_exe.to_string(),
            supervisor_id: "demo-supervisor".to_string(),
            ..ReleaseEntry::default()
        }
    }

    fn write_supervised_app(install_dir: &Path, app_dir: &Path, main_exe: &str, generation: &str) {
        std::fs::create_dir_all(app_dir).unwrap();
        std::fs::write(app_dir.join("generation"), generation).unwrap();
        let main_exe_path = app_dir.join(main_exe);
        std::fs::write(&main_exe_path, "#!/bin/sh\nexit 0\n").unwrap();
        make_executable(&main_exe_path);

        let supervisor_path = app_dir.join(crate::platform::process::supervisor_binary_name());
        std::fs::write(
            &supervisor_path,
            format!(
                r#"#!/bin/sh
command="$1"
shift
id=""
dir=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --id) id="$2"; shift 2 ;;
    --dir) dir="$2"; shift 2 ;;
    *) shift ;;
  esac
done
echo "$command" > '{}'
echo "${{RECOVERY_MARKER:-missing}}" > '{}'
echo $$ > "$dir/.surge-supervisor-$id.pid"
"#,
                install_dir.join("recovery-command").display(),
                install_dir.join("recovery-environment").display()
            ),
        )
        .unwrap();
        make_executable(&supervisor_path);
    }

    fn make_executable(path: &Path) {
        let mut permissions = std::fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(path, permissions).unwrap();
    }
}
