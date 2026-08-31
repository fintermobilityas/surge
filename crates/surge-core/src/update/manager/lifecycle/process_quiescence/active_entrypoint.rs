use std::ffi::OsStr;
use std::path::{Path, PathBuf};

use crate::error::{Result, SurgeError};

pub(super) struct Identity {
    path: PathBuf,
    pub(super) resolved: PathBuf,
    require_entrypoint_argument: bool,
}

impl Identity {
    pub(super) fn resolve(active_app_dir: &Path, main_exe: &str) -> Result<Option<Self>> {
        let active_app_root = match std::fs::canonicalize(active_app_dir) {
            Ok(path) => path,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(SurgeError::Platform(format!(
                    "Failed to resolve active application directory before swap: {e}"
                )));
            }
        };
        let configured_path = active_app_root.join(main_exe);
        let resolved = match std::fs::canonicalize(&configured_path) {
            Ok(path) => path,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(SurgeError::Platform(format!(
                    "Failed to resolve active application executable before swap: {e}"
                )));
            }
        };

        Ok(Some(Self {
            require_entrypoint_argument: !resolved.starts_with(&active_app_root),
            path: configured_path,
            resolved,
        }))
    }

    pub(super) fn matches_executable(&self, executable: &Path) -> bool {
        !self.require_entrypoint_argument
            && (executable == self.resolved
                || cfg!(target_os = "macos")
                    && std::fs::canonicalize(executable).is_ok_and(|resolved| resolved == self.resolved))
    }

    pub(super) fn matches_argument(&self, argument: &OsStr, cwd: Option<&Path>) -> bool {
        if self.require_entrypoint_argument {
            argument_preserves_entrypoint(argument, cwd, &self.path)
        } else {
            argument_resolves_to(argument, cwd, &self.resolved)
        }
    }

    #[cfg(test)]
    pub(super) fn requires_argument(&self) -> bool {
        self.require_entrypoint_argument
    }
}

fn argument_preserves_entrypoint(argument: &OsStr, cwd: Option<&Path>, expected: &Path) -> bool {
    let Some(candidate) = absolute_argument(argument, cwd) else {
        return false;
    };
    candidate == expected
}

fn argument_resolves_to(argument: &OsStr, cwd: Option<&Path>, expected: &Path) -> bool {
    let Some(candidate) = absolute_argument(argument, cwd) else {
        return false;
    };
    std::fs::canonicalize(candidate).is_ok_and(|resolved| resolved == expected)
}

fn absolute_argument(argument: &OsStr, cwd: Option<&Path>) -> Option<PathBuf> {
    let argument = Path::new(argument);
    if argument.as_os_str().is_empty() {
        return None;
    }
    if argument.is_absolute() {
        Some(argument.to_path_buf())
    } else {
        cwd.map(|cwd| cwd.join(argument))
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::symlink;

    use super::*;

    #[test]
    fn nested_parent_symlink_preserves_configured_launch_identity() {
        let tmp = tempfile::tempdir().unwrap();
        let active_app_dir = tmp.path().join("app");
        let shared_dir = tmp.path().join("shared");
        std::fs::create_dir_all(&active_app_dir).unwrap();
        std::fs::create_dir_all(&shared_dir).unwrap();
        std::fs::write(shared_dir.join("demo"), "fixture").unwrap();
        symlink(&shared_dir, active_app_dir.join("bin")).unwrap();

        let identity = Identity::resolve(&active_app_dir, "bin/demo").unwrap().unwrap();

        assert!(identity.requires_argument());
        assert!(identity.matches_argument(active_app_dir.join("bin/demo").as_os_str(), None));
        assert!(!identity.matches_argument(shared_dir.join("demo").as_os_str(), None));
    }
}
