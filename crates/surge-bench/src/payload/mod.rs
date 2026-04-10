#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]

mod mutations;
mod rng;
mod specs;
mod synthetic;

use std::io;
use std::path::{Path, PathBuf};

use self::mutations::{
    copy_flat_directory, dir_size, mutate_nativesdk, remove_rotating_config, reset_directory,
    rewrite_large_release_files, write_feature_files,
};
use self::rng::Xorshift64;
use self::specs::{FileSpec, build_file_specs};
use self::synthetic::write_synthetic_file;

pub struct GeneratedPayload {
    pub v1_dir: PathBuf,
    pub v2_dir: PathBuf,
    pub total_files: usize,
    pub total_size_v1: u64,
    pub total_size_v2: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScenarioProfile {
    FullRelease,
    SdkOnly,
}

impl ScenarioProfile {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FullRelease => "full_release",
            Self::SdkOnly => "sdk_only",
        }
    }
}

pub struct PayloadTemplate {
    scale: f64,
    specs: Vec<FileSpec>,
    pub total_files: usize,
}

impl PayloadTemplate {
    #[must_use]
    pub fn new(scale: f64, seed: u64) -> Self {
        let mut rng = Xorshift64::new(seed);
        let specs = build_file_specs(&mut rng, scale);
        let total_files = specs.len();
        Self {
            scale,
            specs,
            total_files,
        }
    }

    pub fn write_base(&self, dir: &Path, seed: u64) -> io::Result<u64> {
        reset_directory(dir)?;
        for spec in &self.specs {
            write_synthetic_file(seed, spec, &dir.join(&spec.name))?;
        }
        dir_size(dir)
    }

    pub fn mutate_version(
        &self,
        dir: &Path,
        seed: u64,
        version_index: usize,
        scenario: ScenarioProfile,
    ) -> io::Result<u64> {
        match scenario {
            ScenarioProfile::FullRelease => {
                rewrite_large_release_files(&self.specs, dir, seed, version_index)?;
                mutate_nativesdk(dir, seed, version_index)?;
                write_feature_files(dir, self.scale, seed, version_index)?;
                remove_rotating_config(dir, version_index)?;
            }
            ScenarioProfile::SdkOnly => {
                mutate_nativesdk(dir, seed, version_index)?;
            }
        }

        dir_size(dir)
    }
}

pub fn generate(work_dir: &Path, scale: f64, seed: u64, scenario: ScenarioProfile) -> io::Result<GeneratedPayload> {
    let v1_dir = work_dir.join("v1");
    let v2_dir = work_dir.join("v2");
    let template = PayloadTemplate::new(scale, seed);
    let total_files = template.total_files;
    let total_size_v1 = template.write_base(&v1_dir, seed)?;
    copy_flat_directory(&v1_dir, &v2_dir)?;
    let total_size_v2 = template.mutate_version(&v2_dir, seed, 2, scenario)?;

    Ok(GeneratedPayload {
        v1_dir,
        v2_dir,
        total_files,
        total_size_v1,
        total_size_v2,
    })
}
