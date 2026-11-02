//! Verification of a materialized update payload before it is activated.
//!
//! The staged archive is SHA-256 verified before extraction, but nothing used to
//! verify the tree the extractor actually produced. A payload whose entries all
//! extract as 0-byte files (a truncated or mis-packed archive) therefore looked
//! like a successful update, and finalize swapped it into `app/` after stopping
//! the supervisor. These helpers re-read the materialized tree and refuse to
//! activate a payload that cannot possibly run.

use std::path::{Path, PathBuf};

use crate::error::{Result, SurgeError};

/// Counts observed while walking a materialized payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PayloadSurvey {
    pub files: u64,
    pub zero_byte_files: u64,
    pub directories: u64,
    pub symlinks: u64,
    pub bytes: u64,
}

/// Walk `root` without following symlinks and count what it contains.
///
/// Uses `symlink_metadata` so a link is counted as a link instead of as its
/// target, and an explicit stack instead of recursion so a deep tree cannot
/// overflow the stack.
pub fn survey_payload(root: &Path) -> Result<PayloadSurvey> {
    let mut survey = PayloadSurvey::default();
    let mut pending = vec![root.to_path_buf()];

    while let Some(directory) = pending.pop() {
        let entries = std::fs::read_dir(&directory).map_err(|e| {
            SurgeError::Update(format!(
                "Failed to inspect update payload directory '{}': {e}",
                directory.display()
            ))
        })?;
        for entry in entries {
            let entry = entry.map_err(|e| {
                SurgeError::Update(format!(
                    "Failed to inspect update payload directory '{}': {e}",
                    directory.display()
                ))
            })?;
            let path: PathBuf = entry.path();
            let metadata = std::fs::symlink_metadata(&path).map_err(|e| {
                SurgeError::Update(format!(
                    "Failed to inspect update payload entry '{}': {e}",
                    path.display()
                ))
            })?;
            let file_type = metadata.file_type();
            if file_type.is_symlink() {
                survey.symlinks += 1;
            } else if file_type.is_dir() {
                survey.directories += 1;
                pending.push(path);
            } else if file_type.is_file() {
                let size = metadata.len();
                survey.files += 1;
                survey.bytes += size;
                if size == 0 {
                    survey.zero_byte_files += 1;
                }
            }
        }
    }

    Ok(survey)
}

/// Verify that `root` holds an activatable application payload.
///
/// Every check is deterministic: a payload is rejected only when it is missing
/// its main executable, ships an empty main executable or supervisor binary, or
/// contains no file with any content at all. A ratio heuristic (for example
/// "reject when most files are empty") is deliberately not used because it
/// false-positives on legitimate packages.
pub fn verify_payload_for_activation(
    root: &Path,
    main_exe: &str,
    supervisor_binary: Option<&str>,
) -> Result<PayloadSurvey> {
    if !root.is_dir() {
        return Err(SurgeError::Update(format!(
            "Update payload directory '{}' is missing",
            root.display()
        )));
    }

    let main_exe = main_exe.trim();
    if !main_exe.is_empty() {
        let main_exe_path = root.join(main_exe);
        let main_exe_size = std::fs::metadata(&main_exe_path)
            .ok()
            .filter(std::fs::Metadata::is_file);
        let Some(main_exe_size) = main_exe_size else {
            return Err(SurgeError::Update(format!(
                "Update payload is missing its main executable '{main_exe}'"
            )));
        };
        if main_exe_size.len() == 0 {
            return Err(SurgeError::Update(format!(
                "Update payload main executable '{main_exe}' is 0 bytes; refusing to activate a truncated payload"
            )));
        }
    }

    if let Some(supervisor_binary) = supervisor_binary.map(str::trim).filter(|name| !name.is_empty()) {
        let supervisor_path = root.join(supervisor_binary);
        let supervisor_is_empty =
            std::fs::metadata(&supervisor_path).is_ok_and(|metadata| metadata.is_file() && metadata.len() == 0);
        if supervisor_is_empty {
            return Err(SurgeError::Update(format!(
                "Update payload supervisor binary '{supervisor_binary}' is 0 bytes; refusing to activate a truncated payload"
            )));
        }
    }

    let survey = survey_payload(root)?;
    if survey.bytes == 0 {
        return Err(SurgeError::Update(format!(
            "Update payload under '{}' contains {} files and no file with any content ({} are 0 bytes); refusing to activate a truncated payload",
            root.display(),
            survey.files,
            survey.zero_byte_files
        )));
    }

    Ok(survey)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_file(root: &Path, relative: &str, contents: &[u8]) {
        let path = root.join(relative);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("payload directory should be created");
        }
        std::fs::write(&path, contents).expect("payload file should be written");
    }

    fn healthy_payload(root: &Path) {
        write_file(root, "sampleapp", b"#!/bin/sh\n");
        write_file(root, "lib/Some.Dependency.dll", &vec![0x11_u8; 1024]);
        write_file(root, "empty.log", b"");
    }

    #[test]
    fn healthy_payload_passes_and_survey_counts_files_and_bytes() {
        let tmp = tempfile::tempdir().expect("temp dir should exist");
        let root = tmp.path();
        healthy_payload(root);

        let survey =
            verify_payload_for_activation(root, "sampleapp", Some("surge-supervisor")).expect("payload passes");

        assert_eq!(survey.files, 3);
        assert_eq!(survey.zero_byte_files, 1);
        assert_eq!(survey.directories, 1);
        assert_eq!(survey.symlinks, 0);
        assert_eq!(survey.bytes, 10 + 1024);
    }

    #[test]
    fn payload_with_no_content_is_rejected() {
        let tmp = tempfile::tempdir().expect("temp dir should exist");
        let root = tmp.path();
        for name in ["sampleapp", "lib/Some.Dependency.dll", "lib/Other.Dependency.dll"] {
            write_file(root, name, b"");
        }

        let err = verify_payload_for_activation(root, "sampleapp", None).expect_err("empty payload is rejected");
        assert!(
            matches!(&err, SurgeError::Update(message) if message.contains("main executable 'sampleapp' is 0 bytes")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn zero_byte_main_executable_is_rejected_even_when_other_files_have_content() {
        let tmp = tempfile::tempdir().expect("temp dir should exist");
        let root = tmp.path();
        write_file(root, "sampleapp", b"");
        write_file(root, "lib/Some.Dependency.dll", &vec![0x22_u8; 512]);

        let err =
            verify_payload_for_activation(root, "sampleapp", None).expect_err("truncated main executable is rejected");
        assert!(
            matches!(
                &err,
                SurgeError::Update(message)
                    if message.contains("main executable 'sampleapp' is 0 bytes")
                        && message.contains("refusing to activate a truncated payload")
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn legitimately_empty_sidecar_file_does_not_fail_a_healthy_payload() {
        let tmp = tempfile::tempdir().expect("temp dir should exist");
        let root = tmp.path();
        write_file(root, "sampleapp", b"binary");
        write_file(root, "logs/session.log", b"");

        let survey = verify_payload_for_activation(root, "sampleapp", None).expect("payload passes");
        assert_eq!(survey.files, 2);
        assert_eq!(survey.zero_byte_files, 1);
    }

    #[test]
    fn missing_main_executable_is_rejected() {
        let tmp = tempfile::tempdir().expect("temp dir should exist");
        let root = tmp.path();
        write_file(root, "lib/Some.Dependency.dll", b"content");

        let err =
            verify_payload_for_activation(root, "sampleapp", None).expect_err("missing main executable is rejected");
        assert!(
            matches!(&err, SurgeError::Update(message) if message.contains("missing its main executable 'sampleapp'")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn empty_main_exe_name_skips_executable_checks_but_still_requires_content() {
        let tmp = tempfile::tempdir().expect("temp dir should exist");
        let root = tmp.path();
        write_file(root, "payload.txt", b"content");

        verify_payload_for_activation(root, "", None).expect("empty main exe name skips the executable checks");

        let empty_root = tmp.path().join("empty");
        write_file(&empty_root, "payload.txt", b"");
        let err = verify_payload_for_activation(&empty_root, "", None).expect_err("contentless payload is rejected");
        assert!(
            matches!(
                &err,
                SurgeError::Update(message)
                    if message.contains("contains 1 files and no file with any content (1 are 0 bytes)")
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn missing_payload_directory_is_rejected() {
        let tmp = tempfile::tempdir().expect("temp dir should exist");
        let root = tmp.path().join("absent");

        let err = verify_payload_for_activation(&root, "sampleapp", None).expect_err("missing payload is rejected");
        assert!(
            matches!(&err, SurgeError::Update(message) if message.contains("is missing")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn empty_supervisor_binary_is_rejected_but_absent_one_is_accepted() {
        let tmp = tempfile::tempdir().expect("temp dir should exist");
        let root = tmp.path();
        healthy_payload(root);
        write_file(root, "surge-supervisor", b"");

        let err = verify_payload_for_activation(root, "sampleapp", Some("surge-supervisor"))
            .expect_err("empty supervisor is rejected");
        assert!(
            matches!(
                &err,
                SurgeError::Update(message) if message.contains("supervisor binary 'surge-supervisor' is 0 bytes")
            ),
            "unexpected error: {err}"
        );

        let without_supervisor = tmp.path().join("without-supervisor");
        healthy_payload(&without_supervisor);
        verify_payload_for_activation(&without_supervisor, "sampleapp", Some("surge-supervisor"))
            .expect("an absent supervisor binary is not an error");
    }

    #[test]
    #[cfg(unix)]
    fn symlinks_are_counted_and_never_followed() {
        use std::os::unix::fs::symlink;

        let tmp = tempfile::tempdir().expect("temp dir should exist");
        let root = tmp.path().join("payload");
        std::fs::create_dir_all(&root).expect("payload directory should be created");
        write_file(&root, "sampleapp", b"binary");
        let outside = tmp.path().join("outside");
        std::fs::create_dir_all(&outside).expect("outside directory should be created");
        std::fs::write(outside.join("huge.bin"), vec![0x33_u8; 4096]).expect("outside file should be written");
        symlink(&outside, root.join("linked-dir")).expect("directory symlink should be created");
        symlink(outside.join("huge.bin"), root.join("linked-file")).expect("file symlink should be created");

        let survey = verify_payload_for_activation(&root, "sampleapp", None).expect("payload passes");

        assert_eq!(survey.files, 1);
        assert_eq!(survey.bytes, 6);
        assert_eq!(survey.symlinks, 2);
        assert_eq!(survey.directories, 0);
    }
}
