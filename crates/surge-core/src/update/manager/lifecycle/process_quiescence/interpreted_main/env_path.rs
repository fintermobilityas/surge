use std::ffi::OsStr;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use nix::errno::Errno;
use nix::unistd::{AccessFlags, access};

use crate::error::{Result, SurgeError};

pub(super) fn select_path_executable(
    search_path: &OsStr,
    program: &Path,
    observed_executable: Option<&Path>,
) -> Result<Option<PathBuf>> {
    select_path_executable_with(search_path, program, observed_executable, |_| {})
}

fn select_path_executable_with<F>(
    search_path: &OsStr,
    program: &Path,
    observed_executable: Option<&Path>,
    mut after_metadata: F,
) -> Result<Option<PathBuf>>
where
    F: FnMut(&Path),
{
    for directory in std::env::split_paths(search_path) {
        if directory.as_os_str().is_empty() || directory.is_relative() {
            return Err(SurgeError::Platform(format!(
                "Cannot safely resolve env interpreter '{}' through a relative PATH entry before swap",
                program.display()
            )));
        }
        let candidate = directory.join(program);
        let metadata = match std::fs::metadata(&candidate) {
            Ok(metadata) => metadata,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
                ) =>
            {
                continue;
            }
            Err(error) => return Err(candidate_inspection_error(&candidate, error)),
        };
        if !metadata.is_file() {
            continue;
        }

        after_metadata(&candidate);
        let resolved = std::fs::canonicalize(&candidate).map_err(|error| candidate_changed_error(&candidate, error))?;
        ensure_candidate_identity(&candidate, &metadata)?;

        if observed_executable.is_some_and(|observed| paths_resolve_to_same_executable(observed, &resolved)) {
            return Ok(Some(resolved));
        }

        match access(&candidate, AccessFlags::X_OK) {
            Ok(()) => {}
            Err(Errno::EACCES) => {
                ensure_candidate_identity(&candidate, &metadata)?;
                continue;
            }
            Err(error @ (Errno::ENOENT | Errno::ENOTDIR)) => {
                return Err(candidate_changed_error(&candidate, error));
            }
            Err(error) => {
                return Err(SurgeError::Platform(format!(
                    "Cannot safely inspect env interpreter candidate '{}': {error}",
                    candidate.display()
                )));
            }
        }

        ensure_candidate_identity(&candidate, &metadata)?;
        return Ok(Some(resolved));
    }
    Ok(None)
}

fn ensure_candidate_identity(candidate: &Path, expected: &std::fs::Metadata) -> Result<()> {
    let current_resolved =
        std::fs::canonicalize(candidate).map_err(|error| candidate_changed_error(candidate, error))?;
    let current_metadata =
        std::fs::metadata(&current_resolved).map_err(|error| candidate_changed_error(candidate, error))?;
    if expected.dev() != current_metadata.dev() || expected.ino() != current_metadata.ino() {
        return Err(SurgeError::Platform(format!(
            "Env interpreter candidate '{}' changed during inspection before application swap",
            candidate.display()
        )));
    }
    Ok(())
}

fn candidate_inspection_error(candidate: &Path, error: std::io::Error) -> SurgeError {
    SurgeError::Platform(format!(
        "Cannot safely inspect env interpreter candidate '{}': {error}",
        candidate.display()
    ))
}

fn candidate_changed_error(candidate: &Path, error: impl std::fmt::Display) -> SurgeError {
    SurgeError::Platform(format!(
        "Env interpreter candidate '{}' changed during inspection before application swap: {error}",
        candidate.display()
    ))
}

pub(super) fn paths_resolve_to_same_executable(actual: &Path, expected: &Path) -> bool {
    if actual == expected {
        return true;
    }
    if std::fs::canonicalize(actual)
        .ok()
        .zip(std::fs::canonicalize(expected).ok())
        .is_some_and(|(actual, expected)| actual == expected)
    {
        return true;
    }

    std::fs::metadata(actual)
        .ok()
        .zip(std::fs::metadata(expected).ok())
        .is_some_and(|(actual, expected)| actual.dev() == expected.dev() && actual.ino() == expected.ino())
        || macos_system_shell_identity_matches(actual, expected)
}

#[cfg(target_os = "macos")]
fn macos_system_shell_identity_matches(actual: &Path, expected: &Path) -> bool {
    // macOS reports a stable /bin/sh script process as /bin/bash while retaining /bin/sh as argv[0].
    actual == Path::new("/bin/bash") && expected == Path::new("/bin/sh")
}

#[cfg(not(target_os = "macos"))]
fn macos_system_shell_identity_matches(_actual: &Path, _expected: &Path) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::{PermissionsExt, symlink};

    use super::*;

    #[test]
    fn observed_candidate_is_selected_after_execute_permission_is_removed() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let observed = first.path().join("demo-interpreter");
        std::fs::copy(std::env::current_exe().unwrap(), &observed).unwrap();
        std::fs::set_permissions(&observed, std::fs::Permissions::from_mode(0o644)).unwrap();
        symlink("/bin/sh", second.path().join("demo-interpreter")).unwrap();
        let search_path = std::env::join_paths([first.path(), second.path()]).unwrap();

        let selected = select_path_executable(&search_path, Path::new("demo-interpreter"), Some(&observed))
            .unwrap()
            .unwrap();

        assert!(paths_resolve_to_same_executable(&selected, &observed));
    }

    #[test]
    fn candidate_disappearance_after_metadata_fails_closed() {
        let directory = tempfile::tempdir().unwrap();
        let candidate = directory.path().join("demo-interpreter");
        std::fs::copy(std::env::current_exe().unwrap(), &candidate).unwrap();
        let mut removed = false;

        let error = select_path_executable_with(
            directory.path().as_os_str(),
            Path::new("demo-interpreter"),
            None,
            |inspected| {
                if !removed {
                    std::fs::remove_file(inspected).unwrap();
                    removed = true;
                }
            },
        )
        .unwrap_err();

        assert!(error.to_string().contains("changed during inspection"));
    }

    #[test]
    fn non_executable_replacement_after_metadata_fails_closed() {
        let directory = tempfile::tempdir().unwrap();
        let candidate = directory.path().join("demo-interpreter");
        let replacement = directory.path().join("replacement");
        std::fs::copy(std::env::current_exe().unwrap(), &candidate).unwrap();
        std::fs::copy(std::env::current_exe().unwrap(), &replacement).unwrap();
        std::fs::set_permissions(&replacement, std::fs::Permissions::from_mode(0o644)).unwrap();
        let mut replaced = false;

        let error = select_path_executable_with(
            directory.path().as_os_str(),
            Path::new("demo-interpreter"),
            None,
            |inspected| {
                if !replaced {
                    std::fs::rename(&replacement, inspected).unwrap();
                    replaced = true;
                }
            },
        )
        .unwrap_err();

        assert!(error.to_string().contains("changed during inspection"));
    }
}
