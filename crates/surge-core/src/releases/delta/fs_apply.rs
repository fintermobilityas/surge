use std::fs;
use std::path::{Component, Path, PathBuf};

use crate::crypto::sha256::sha256_hex_file;
use crate::diff::chunked::chunked_bspatch_file;
use crate::error::{Result, SurgeError};

use super::sparse_ops::SparseFileOp;

pub(super) fn apply_sparse_file_ops(root: &Path, ops: &[SparseFileOp], payloads: &[u8]) -> Result<()> {
    for op in ops {
        match op {
            SparseFileOp::Delete { path } => {
                let target = resolve_relative_path(root, path)?;
                remove_path_if_exists(&target)?;
            }
            SparseFileOp::EnsureDir { path, mode } => {
                let target = resolve_relative_path(root, path)?;
                fs::create_dir_all(&target)?;
                set_mode(&target, *mode)?;
            }
            SparseFileOp::SetMode { path, mode } => {
                let target = resolve_relative_path(root, path)?;
                set_mode(&target, *mode)?;
            }
            SparseFileOp::WriteFile {
                path,
                mode,
                payload_offset,
                payload_len,
                sha256,
            } => {
                let target = resolve_relative_path(root, path)?;
                let payload = payload_slice(payloads, *payload_offset, *payload_len)?;
                remove_path_if_exists(&target)?;
                if let Some(parent) = target.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(&target, payload)?;
                set_mode(&target, *mode)?;
                verify_file_sha256(&target, sha256)?;
            }
            SparseFileOp::PatchFile {
                path,
                mode,
                payload_offset,
                payload_len,
                basis_sha256,
                sha256,
            } => {
                let target = resolve_relative_path(root, path)?;
                verify_file_sha256(&target, basis_sha256)?;
                let patch_bytes = payload_slice(payloads, *payload_offset, *payload_len)?;
                let temp_path = patched_temp_path(&target);
                if temp_path.exists() {
                    fs::remove_file(&temp_path)?;
                }
                chunked_bspatch_file(&target, patch_bytes, &temp_path)?;
                fs::remove_file(&target)?;
                fs::rename(&temp_path, &target)?;
                set_mode(&target, *mode)?;
                verify_file_sha256(&target, sha256)?;
            }
            SparseFileOp::WriteSymlink { path, target } => {
                let link_path = resolve_relative_path(root, path)?;
                remove_path_if_exists(&link_path)?;
                if let Some(parent) = link_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                create_symlink(target, &link_path)?;
            }
        }
    }
    Ok(())
}

fn resolve_relative_path(root: &Path, relative: &str) -> Result<PathBuf> {
    let mut resolved = PathBuf::from(root);
    for component in Path::new(relative).components() {
        match component {
            Component::Normal(segment) => resolved.push(segment),
            _ => {
                return Err(SurgeError::Update(format!("Invalid sparse delta path '{relative}'")));
            }
        }
    }
    Ok(resolved)
}

fn payload_slice(payloads: &[u8], offset: u64, len: u64) -> Result<&[u8]> {
    let start = usize::try_from(offset)
        .map_err(|_| SurgeError::Update("Sparse delta payload offset exceeds platform limits".to_string()))?;
    let len = usize::try_from(len)
        .map_err(|_| SurgeError::Update("Sparse delta payload length exceeds platform limits".to_string()))?;
    let end = start
        .checked_add(len)
        .ok_or_else(|| SurgeError::Update("Sparse delta payload range overflows".to_string()))?;
    payloads
        .get(start..end)
        .ok_or_else(|| SurgeError::Update("Sparse delta payload range is invalid".to_string()))
}

fn verify_file_sha256(path: &Path, expected_sha256: &str) -> Result<()> {
    let expected = expected_sha256.trim();
    if expected.is_empty() {
        return Ok(());
    }
    let actual = sha256_hex_file(path)?;
    if actual != expected {
        return Err(SurgeError::Update(format!(
            "Sparse delta file hash mismatch for '{}': expected {expected}, got {actual}",
            path.display()
        )));
    }
    Ok(())
}

fn remove_path_if_exists(path: &Path) -> Result<()> {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return Ok(());
    };
    let file_type = metadata.file_type();
    if file_type.is_dir() {
        fs::remove_dir_all(path)?;
    } else {
        fs::remove_file(path)?;
    }
    Ok(())
}

fn patched_temp_path(target: &Path) -> PathBuf {
    let file_name = target
        .file_name()
        .map_or_else(|| "patched".to_string(), |name| name.to_string_lossy().into_owned());
    target.with_file_name(format!(".{file_name}.surge-patch"))
}

#[cfg(unix)]
fn set_mode(path: &Path, mode: u32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = fs::metadata(path)?.permissions();
    permissions.set_mode(mode);
    fs::set_permissions(path, permissions)?;
    Ok(())
}

#[cfg(not(unix))]
fn set_mode(_path: &Path, _mode: u32) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn create_symlink(target: &str, link_path: &Path) -> Result<()> {
    std::os::unix::fs::symlink(target, link_path)?;
    Ok(())
}

#[cfg(windows)]
fn create_symlink(target: &str, link_path: &Path) -> Result<()> {
    std::os::windows::fs::symlink_file(target, link_path)?;
    Ok(())
}
