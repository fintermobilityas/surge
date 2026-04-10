use std::fs;
use std::io;
use std::path::Path;

use super::specs::{FilePattern, FileSpec, scale_size};
use super::synthetic::write_synthetic_file;

pub(super) fn reset_directory(dir: &Path) -> io::Result<()> {
    if dir.exists() {
        fs::remove_dir_all(dir)?;
    }
    fs::create_dir_all(dir)
}

pub(super) fn copy_flat_directory(from: &Path, to: &Path) -> io::Result<()> {
    reset_directory(to)?;
    for entry in fs::read_dir(from)? {
        let entry = entry?;
        fs::copy(entry.path(), to.join(entry.file_name()))?;
    }
    Ok(())
}

pub(super) fn rewrite_large_release_files(
    specs: &[FileSpec],
    dir: &Path,
    seed: u64,
    version_index: usize,
) -> io::Result<()> {
    let rewrite_count = (specs.len() / 20).max(1);
    let offset = version_index.saturating_sub(2) % specs.len().max(1);
    let version_seed = seed.wrapping_add((version_index as u64).wrapping_mul(1_000));

    for step in 0..rewrite_count {
        let spec = &specs[(offset + step) % specs.len()];
        write_synthetic_file(version_seed, spec, &dir.join(&spec.name))?;
    }

    Ok(())
}

pub(super) fn mutate_nativesdk(dir: &Path, seed: u64, version_index: usize) -> io::Result<()> {
    let nativesdk_path = dir.join("nativesdk.so");
    if !nativesdk_path.exists() {
        return Ok(());
    }

    let mut data = fs::read(&nativesdk_path)?;
    if data.is_empty() {
        return Ok(());
    }

    let page_span = 4096usize;
    let page_index = version_index.saturating_sub(1) % 64;
    let offset = (page_index * page_span).min(data.len().saturating_sub(1));
    let patch_end = (offset + page_span).min(data.len());
    let mut patch_rng = super::rng::Xorshift64::new(seed.wrapping_add((version_index as u64).wrapping_mul(2_000)));
    patch_rng.fill_bytes(&mut data[offset..patch_end]);
    fs::write(&nativesdk_path, &data)
}

pub(super) fn write_feature_files(dir: &Path, scale: f64, seed: u64, version_index: usize) -> io::Result<()> {
    let feature_binary = FileSpec {
        name: "app.feature.dll".to_string(),
        size: scale_size(411_136, scale),
        pattern: FilePattern::Binary,
    };
    let feature_config = FileSpec {
        name: "app.feature.config.json".to_string(),
        size: scale_size(12_000, scale),
        pattern: FilePattern::Text,
    };

    write_synthetic_file(
        seed.wrapping_add((version_index as u64).wrapping_mul(3_000)),
        &feature_binary,
        &dir.join(&feature_binary.name),
    )?;
    write_synthetic_file(
        seed.wrapping_add((version_index as u64).wrapping_mul(4_000)),
        &feature_config,
        &dir.join(&feature_config.name),
    )
}

pub(super) fn remove_rotating_config(dir: &Path, version_index: usize) -> io::Result<()> {
    const CANDIDATES: &[&str] = &[
        "generated-config-007.json",
        "generated-config-014.json",
        "generated-config-021.json",
        "generated-config-028.json",
    ];
    let candidate = CANDIDATES[version_index.saturating_sub(2) % CANDIDATES.len()];
    match fs::remove_file(dir.join(candidate)) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

pub(super) fn dir_size(dir: &Path) -> io::Result<u64> {
    let mut total = 0;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let meta = entry.metadata()?;
        if meta.is_file() {
            total += meta.len();
        }
    }
    Ok(total)
}
