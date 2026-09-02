use std::ffi::OsStr;
use std::path::{Path, PathBuf};

use crate::error::{Result, SurgeError};
use crate::update::manager::current_install::safe_relative_path;

pub(super) struct Identity {
    configured_launch_path: PathBuf,
    path: PathBuf,
    pub(super) resolved: PathBuf,
    require_entrypoint_argument: bool,
}

impl Identity {
    pub(super) fn resolve(active_app_dir: &Path, main_exe: &str) -> Result<Self> {
        if safe_relative_path(Path::new(main_exe)).is_none() {
            return Err(SurgeError::Platform(
                "Active application executable must be a safe relative path".to_string(),
            ));
        }
        let configured_launch_path = std::path::absolute(active_app_dir.join(main_exe)).map_err(|e| {
            SurgeError::Platform(format!(
                "Failed to resolve configured active application path before swap: {e}"
            ))
        })?;
        let active_app_root = std::fs::canonicalize(active_app_dir).map_err(|e| {
            SurgeError::Platform(format!(
                "Failed to resolve active application directory before swap: {e}"
            ))
        })?;
        let configured_path = active_app_root.join(main_exe);
        let resolved = std::fs::canonicalize(&configured_path).map_err(|e| {
            SurgeError::Platform(format!(
                "Failed to resolve active application executable before swap: {e}"
            ))
        })?;

        Ok(Self {
            configured_launch_path,
            require_entrypoint_argument: configured_path != resolved,
            path: configured_path,
            resolved,
        })
    }

    pub(super) fn matches_executable(&self, executable: &Path) -> bool {
        !self.require_entrypoint_argument && self.matches_resolved_executable(executable)
    }

    pub(super) fn matches_resolved_executable(&self, executable: &Path) -> bool {
        executable == self.resolved
            || cfg!(target_os = "macos")
                && std::fs::canonicalize(executable).is_ok_and(|resolved| resolved == self.resolved)
    }

    pub(super) fn matches_argument(&self, argument: &OsStr, cwd: Option<&Path>) -> Result<bool> {
        if self.require_entrypoint_argument {
            argument_preserves_entrypoint(argument, cwd, &self.path, &self.configured_launch_path)
        } else {
            argument_resolves_to(argument, cwd, &self.resolved)
        }
    }

    #[cfg(test)]
    pub(super) fn requires_argument(&self) -> bool {
        self.require_entrypoint_argument
    }
}

fn argument_preserves_entrypoint(
    argument: &OsStr,
    cwd: Option<&Path>,
    expected: &Path,
    configured_launch_path: &Path,
) -> Result<bool> {
    match absolute_argument(argument, cwd) {
        ArgumentPath::Missing => return Ok(false),
        ArgumentPath::Ambiguous => return ambiguous_relative_argument(argument, expected),
        ArgumentPath::Absolute(candidate) => {
            if candidate == expected || candidate == configured_launch_path {
                return Ok(true);
            }
        }
    }

    ambiguous_relative_argument(argument, expected)
}

fn argument_resolves_to(argument: &OsStr, cwd: Option<&Path>, expected: &Path) -> Result<bool> {
    match absolute_argument(argument, cwd) {
        ArgumentPath::Missing => Ok(false),
        ArgumentPath::Ambiguous => ambiguous_relative_argument(argument, expected),
        ArgumentPath::Absolute(candidate) => {
            if candidate == expected || std::fs::canonicalize(candidate).is_ok_and(|resolved| resolved == expected) {
                Ok(true)
            } else {
                ambiguous_relative_argument(argument, expected)
            }
        }
    }
}

fn ambiguous_relative_argument(argument: &OsStr, expected: &Path) -> Result<bool> {
    let argument = Path::new(argument);
    if argument.is_relative() && argument.file_name() == expected.file_name() {
        return Err(SurgeError::Platform(format!(
            "Cannot establish the launch directory for relative active application argument '{}'; refusing to swap while its process identity is ambiguous",
            argument.display()
        )));
    }

    Ok(false)
}

enum ArgumentPath {
    Missing,
    Absolute(PathBuf),
    Ambiguous,
}

fn absolute_argument(argument: &OsStr, cwd: Option<&Path>) -> ArgumentPath {
    let argument = Path::new(argument);
    if argument.as_os_str().is_empty() {
        return ArgumentPath::Missing;
    }
    if argument.is_absolute() {
        ArgumentPath::Absolute(argument.to_path_buf())
    } else if let Some(cwd) = cwd {
        ArgumentPath::Absolute(cwd.join(argument))
    } else {
        ArgumentPath::Ambiguous
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

        let identity = Identity::resolve(&active_app_dir, "bin/demo").unwrap();

        assert!(identity.requires_argument());
        assert!(
            identity
                .matches_argument(active_app_dir.join("bin/demo").as_os_str(), None)
                .unwrap()
        );
        assert!(
            !identity
                .matches_argument(shared_dir.join("demo").as_os_str(), None)
                .unwrap()
        );
    }

    #[test]
    fn entrypoint_symlink_within_app_preserves_configured_launch_identity() {
        let tmp = tempfile::tempdir().unwrap();
        let active_app_dir = tmp.path().join("app");
        let shared_dir = active_app_dir.join("shared");
        std::fs::create_dir_all(&shared_dir).unwrap();
        std::fs::write(shared_dir.join("demo"), "fixture").unwrap();
        symlink("shared/demo", active_app_dir.join("demo")).unwrap();

        let identity = Identity::resolve(&active_app_dir, "demo").unwrap();

        assert!(identity.requires_argument());
        assert!(
            identity
                .matches_argument(active_app_dir.join("demo").as_os_str(), None)
                .unwrap()
        );
        assert!(
            !identity
                .matches_argument(shared_dir.join("demo").as_os_str(), None)
                .unwrap()
        );
    }

    #[test]
    fn active_directory_alias_preserves_configured_launch_identity() {
        let tmp = tempfile::tempdir().unwrap();
        let active_app_dir = tmp.path().join("app");
        let active_app_alias = tmp.path().join("app-alias");
        let shared_dir = active_app_dir.join("shared");
        std::fs::create_dir_all(&shared_dir).unwrap();
        std::fs::write(shared_dir.join("demo"), "fixture").unwrap();
        symlink("shared/demo", active_app_dir.join("demo")).unwrap();
        symlink(&active_app_dir, &active_app_alias).unwrap();

        let identity = Identity::resolve(&active_app_alias, "demo").unwrap();

        assert!(identity.requires_argument());
        assert!(
            identity
                .matches_argument(active_app_alias.join("demo").as_os_str(), None)
                .unwrap()
        );
        assert!(
            !identity
                .matches_argument(shared_dir.join("demo").as_os_str(), None)
                .unwrap()
        );
    }

    #[test]
    fn resolve_accepts_dot_prefixed_entrypoint() {
        let tmp = tempfile::tempdir().unwrap();
        let active_app_dir = tmp.path().join("app");
        std::fs::create_dir_all(&active_app_dir).unwrap();
        std::fs::write(active_app_dir.join("demo"), "fixture").unwrap();

        let identity = Identity::resolve(&active_app_dir, "./demo").unwrap();

        assert_eq!(
            identity.resolved,
            std::fs::canonicalize(active_app_dir.join("demo")).unwrap()
        );
    }

    #[test]
    fn resolve_rejects_entrypoints_outside_the_active_app() {
        let tmp = tempfile::tempdir().unwrap();
        let active_app_dir = tmp.path().join("app");
        std::fs::create_dir_all(&active_app_dir).unwrap();

        for main_exe in ["../other-app", "/opt/other-app", "."] {
            let error = Identity::resolve(&active_app_dir, main_exe).err().unwrap();
            assert!(error.to_string().contains("must be a safe relative path"));
        }
    }
}
