use std::fs;
use std::path::{Path, PathBuf};

use surge_core::crypto::sha256;
use surge_core::error::{Result, SurgeError};
use surge_core::install::{LEGACY_RUNTIME_MANIFEST_RELATIVE_PATH, RUNTIME_MANIFEST_RELATIVE_PATH};

pub(super) fn dir_size(dir: &Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Ok(meta) = entry.metadata()
                && meta.is_file()
            {
                total += meta.len();
            }
        }
    }
    total
}

pub(super) fn dir_size_recursive(dir: &Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Ok(meta) = entry.metadata() {
                if meta.is_file() {
                    total += meta.len();
                } else if meta.is_dir() {
                    total += dir_size_recursive(&entry.path());
                }
            }
        }
    }
    total
}

pub(super) fn assert_directories_match(actual: &Path, expected: &Path) -> Result<()> {
    let mut actual_files = collect_relative_files(actual, actual)?;
    let mut expected_files = collect_relative_files(expected, expected)?;
    actual_files.sort();
    expected_files.sort();

    if actual_files != expected_files {
        return Err(SurgeError::Update(
            "Installed files do not match the expected payload".to_string(),
        ));
    }

    for relative in actual_files {
        let actual_hash = sha256::sha256_hex_file(&actual.join(&relative))?;
        let expected_hash = sha256::sha256_hex_file(&expected.join(&relative))?;
        if actual_hash != expected_hash {
            return Err(SurgeError::Update(format!(
                "Installed file differs from expected payload: {}",
                relative.display()
            )));
        }
    }

    Ok(())
}

fn collect_relative_files(root: &Path, current: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            files.extend(collect_relative_files(root, &path)?);
        } else if metadata.is_file() {
            let relative = path
                .strip_prefix(root)
                .map_err(|e| SurgeError::Update(format!("Failed to collect file list: {e}")))?;
            if relative != Path::new(RUNTIME_MANIFEST_RELATIVE_PATH)
                && relative != Path::new(LEGACY_RUNTIME_MANIFEST_RELATIVE_PATH)
            {
                files.push(relative.to_path_buf());
            }
        }
    }
    Ok(files)
}
