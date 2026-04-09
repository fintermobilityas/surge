use std::path::{Path, PathBuf};

use crate::error::{Result, SurgeError};

pub(super) fn resolve_target_path(app_dir: &Path, relative_or_absolute: &str) -> Result<PathBuf> {
    let input_path = Path::new(relative_or_absolute);
    let path = if input_path.is_absolute() {
        input_path.to_path_buf()
    } else {
        app_dir.join(input_path)
    };

    if !path.exists() {
        return Err(SurgeError::Platform(format!(
            "Shortcut target path does not exist: {}",
            path.display()
        )));
    }

    Ok(path)
}

pub(super) fn sanitize_file_stem(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            result.push(ch);
        } else {
            result.push('-');
        }
    }

    if result.is_empty() {
        "surge-app".to_string()
    } else {
        result
    }
}
