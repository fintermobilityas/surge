use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::config::manifest::InstallArtifactCachePolicy;
use crate::context::{Context, StorageConfig, StorageProvider};
use crate::error::{Result, SurgeError};
use crate::platform::fs::write_file_atomic;
use crate::platform::process::{current_pid, supervisor_binary_name};
use crate::releases::manifest::{ReleaseEntry, ReleaseIndex};
use crate::update::status::UpdateStatusRecord;

use super::super::current_install::ReleaseIdentity;
use super::super::{UpdateManager, current_install};

const EXTERNAL_FINALIZE_SCHEMA: u32 = 1;
pub(super) const EXTERNAL_FINALIZE_DIR: &str = ".surge-finalize";
pub(super) const PLAN_FILE_NAME: &str = "plan.json";
pub(super) const READY_FILE_NAME: &str = "ready";
pub(super) const ARMED_FILE_NAME: &str = "armed";
pub(super) const ACCEPTED_FILE_NAME: &str = "accepted";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExternalStorageConfig {
    provider: i32,
    bucket: String,
    region: String,
    endpoint: String,
    prefix: String,
}

impl ExternalStorageConfig {
    fn from_storage_config(config: &StorageConfig) -> Result<Self> {
        let provider = config
            .provider
            .ok_or_else(|| SurgeError::Config("External finalizer requires a storage provider".to_string()))?;
        Ok(Self {
            provider: provider as i32,
            bucket: config.bucket.clone(),
            region: config.region.clone(),
            endpoint: config.endpoint.clone(),
            prefix: config.prefix.clone(),
        })
    }

    fn apply_to_context(&self, context: &Context) -> Result<()> {
        let provider = StorageProvider::from_i32(self.provider).ok_or_else(|| {
            SurgeError::Config(format!(
                "External finalizer plan uses unknown storage provider {}",
                self.provider
            ))
        })?;
        context.set_storage(provider, &self.bucket, &self.region, "", "", &self.endpoint);
        context.set_storage_prefix(&self.prefix);
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ExternalFinalizePlan {
    pub(super) schema: u32,
    pub(super) operation_id: String,
    pub(super) install_dir: PathBuf,
    pub(super) updater_pid: u32,
    pub(super) updater_exe: PathBuf,
    pub(super) current_app_dir: PathBuf,
    pub(super) app_id: String,
    pub(super) current_version: String,
    pub(super) channel: String,
    pub(super) release_retention_limit: usize,
    pub(super) artifact_retention_policy: InstallArtifactCachePolicy,
    storage: ExternalStorageConfig,
    pub(super) release_index: ReleaseIndex,
    pub(super) current_release_identity: ReleaseIdentity,
    pub(super) latest: ReleaseEntry,
    pub(super) in_progress_template: UpdateStatusRecord,
    pub(super) previous_attempt_status: Option<UpdateStatusRecord>,
}

impl ExternalFinalizePlan {
    pub(super) fn from_manager(
        manager: &UpdateManager,
        latest: &ReleaseEntry,
        updater_exe: PathBuf,
        current_app_dir: PathBuf,
        in_progress_template: &UpdateStatusRecord,
        previous_attempt_status: Option<UpdateStatusRecord>,
    ) -> Result<Self> {
        let release_index = manager.cached_index.clone().ok_or_else(|| {
            SurgeError::Update(
                "External finalization requires the release index captured during update check".to_string(),
            )
        })?;
        let current_release_identity = manager.current_release_identity.clone().ok_or_else(|| {
            SurgeError::Update("External finalization requires the installed application identity".to_string())
        })?;
        Ok(Self {
            schema: EXTERNAL_FINALIZE_SCHEMA,
            operation_id: Uuid::new_v4().to_string(),
            install_dir: manager.install_dir.clone(),
            updater_pid: current_pid(),
            updater_exe,
            current_app_dir,
            app_id: manager.app_id.clone(),
            current_version: manager.current_version.clone(),
            channel: manager.channel.clone(),
            release_retention_limit: manager.release_retention_limit,
            artifact_retention_policy: manager.artifact_retention_policy,
            storage: ExternalStorageConfig::from_storage_config(&manager.ctx.storage_config())?,
            release_index,
            current_release_identity,
            latest: latest.clone(),
            in_progress_template: in_progress_template.clone(),
            previous_attempt_status,
        })
    }

    pub(super) fn operation_dir(&self) -> PathBuf {
        self.operations_dir().join(&self.operation_id)
    }

    pub(super) fn operations_dir(&self) -> PathBuf {
        self.install_dir.join(EXTERNAL_FINALIZE_DIR)
    }

    pub(super) fn plan_path(&self) -> PathBuf {
        self.operation_dir().join(PLAN_FILE_NAME)
    }

    pub(super) fn ready_path(&self) -> PathBuf {
        self.operation_dir().join(READY_FILE_NAME)
    }

    pub(super) fn armed_path(&self) -> PathBuf {
        self.operation_dir().join(ARMED_FILE_NAME)
    }

    pub(super) fn accepted_path(&self) -> PathBuf {
        self.operation_dir().join(ACCEPTED_FILE_NAME)
    }

    pub(super) fn staging_dir(&self) -> PathBuf {
        self.install_dir.join(".surge-staging")
    }

    pub(super) fn extracted_final_dir(&self) -> PathBuf {
        self.staging_dir().join("extracted")
    }

    pub(super) fn artifact_cache_dir(&self) -> PathBuf {
        self.install_dir.join(".surge-cache").join("artifacts")
    }
}

pub(super) fn write_plan(plan: &ExternalFinalizePlan) -> Result<()> {
    let json = serde_json::to_vec_pretty(plan)
        .map_err(|error| SurgeError::Config(format!("Failed to encode external finalizer plan: {error}")))?;
    write_file_atomic(&plan.plan_path(), &json)
}

pub(super) fn read_and_validate_plan(plan_path: &Path) -> Result<ExternalFinalizePlan> {
    let raw = std::fs::read(plan_path)?;
    let plan: ExternalFinalizePlan = serde_json::from_slice(&raw)
        .map_err(|error| SurgeError::Config(format!("Failed to decode external finalizer plan: {error}")))?;
    if plan.schema != EXTERNAL_FINALIZE_SCHEMA {
        return Err(SurgeError::Config(format!(
            "Unsupported external finalizer plan schema {}",
            plan.schema
        )));
    }
    Uuid::parse_str(&plan.operation_id)
        .map_err(|error| SurgeError::Config(format!("Invalid external finalizer operation id: {error}")))?;
    let bound_install_dir = super::super::bind_install_dir(&plan.install_dir.to_string_lossy())?;
    if bound_install_dir != plan.install_dir {
        return Err(SurgeError::Config(
            "External finalizer plan install directory is not canonically bound".to_string(),
        ));
    }
    let absolute_plan_path = std::path::absolute(plan_path)?;
    if absolute_plan_path != plan.plan_path() {
        return Err(SurgeError::Config(format!(
            "External finalizer plan path '{}' does not match operation '{}'",
            absolute_plan_path.display(),
            plan.operation_id
        )));
    }
    if plan.updater_pid == 0 || plan.updater_pid == current_pid() {
        return Err(SurgeError::Config(
            "External finalizer plan has an invalid updating process id".to_string(),
        ));
    }
    let stable_app_dir = plan.install_dir.join("app");
    let is_legacy_snapshot = plan.current_app_dir.parent() == Some(plan.install_dir.as_path())
        && plan
            .current_app_dir
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("app-") && name.len() > 4);
    if plan.current_app_dir != stable_app_dir && !is_legacy_snapshot {
        return Err(SurgeError::Config(
            "External finalizer plan current application directory is outside the supported install layout".to_string(),
        ));
    }
    if plan.app_id.trim().is_empty()
        || plan.current_version.trim().is_empty()
        || plan.channel.trim().is_empty()
        || plan.latest.version.trim().is_empty()
    {
        return Err(SurgeError::Config(
            "External finalizer plan is missing application identity".to_string(),
        ));
    }
    if plan.latest.version != plan.in_progress_template.target_version
        || plan.app_id != plan.in_progress_template.app_id
        || plan.channel != plan.in_progress_template.channel
        || plan.current_version != plan.in_progress_template.installed_version
    {
        return Err(SurgeError::Config(
            "External finalizer plan does not match its update status transaction".to_string(),
        ));
    }
    Ok(plan)
}

pub(super) fn manager_from_plan(plan: &ExternalFinalizePlan) -> Result<UpdateManager> {
    let context = Arc::new(Context::new());
    plan.storage.apply_to_context(&context)?;
    let mut manager = UpdateManager::new(
        context,
        &plan.app_id,
        &plan.current_version,
        &plan.channel,
        &plan.install_dir.to_string_lossy(),
    )?;
    manager.release_retention_limit = plan.release_retention_limit;
    manager.artifact_retention_policy = plan.artifact_retention_policy;
    manager.cached_index = Some(plan.release_index.clone());
    manager.current_release_identity = Some(plan.current_release_identity.clone());
    Ok(manager)
}

pub(super) fn validate_materialized_target(plan: &ExternalFinalizePlan, manager: &UpdateManager) -> Result<()> {
    let actual_identity = current_install::load(manager)?;
    if actual_identity.as_ref() != Some(&plan.current_release_identity) {
        return Err(SurgeError::Update(
            "Installed application identity changed before the external finalizer became ready".to_string(),
        ));
    }
    let extracted = plan.extracted_final_dir();
    if !extracted.is_dir() {
        return Err(SurgeError::Update(format!(
            "External finalizer payload is missing at '{}'",
            extracted.display()
        )));
    }
    let main_exe = extracted.join(&plan.latest.main_exe);
    if !main_exe.is_file() {
        return Err(SurgeError::Update(format!(
            "External finalizer target executable is missing at '{}'",
            main_exe.display()
        )));
    }
    let supervisor = extracted.join(supervisor_binary_name());
    if !supervisor.is_file() {
        return Err(SurgeError::Update(format!(
            "External finalizer target supervisor is missing at '{}'",
            supervisor.display()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn handshake_marker_paths_are_scoped_to_the_operation() {
        let install_dir = PathBuf::from("/tmp/demo-install");
        let operation_id = Uuid::new_v4().to_string();
        let plan = ExternalFinalizePlan {
            schema: EXTERNAL_FINALIZE_SCHEMA,
            operation_id: operation_id.clone(),
            install_dir: install_dir.clone(),
            updater_pid: 42,
            updater_exe: install_dir.join("app/demo"),
            current_app_dir: install_dir.join("app"),
            app_id: "demo".to_string(),
            current_version: "1.0.0".to_string(),
            channel: "test".to_string(),
            release_retention_limit: 1,
            artifact_retention_policy: InstallArtifactCachePolicy::default(),
            storage: ExternalStorageConfig {
                provider: StorageProvider::Filesystem as i32,
                bucket: String::new(),
                region: String::new(),
                endpoint: String::new(),
                prefix: String::new(),
            },
            release_index: ReleaseIndex::default(),
            current_release_identity: ReleaseIdentity {
                version: "1.0.0".to_string(),
                main_exe: "demo".to_string(),
                supervisor_id: "demo-supervisor".to_string(),
                environment: BTreeMap::default(),
            },
            latest: ReleaseEntry::default(),
            in_progress_template: UpdateStatusRecord::in_progress(
                "demo",
                "1.0.0",
                "2.0.0",
                "test",
                "2026-09-02T00:00:00Z".to_string(),
            ),
            previous_attempt_status: None,
        };

        assert_eq!(
            plan.plan_path(),
            install_dir
                .join(EXTERNAL_FINALIZE_DIR)
                .join(operation_id)
                .join(PLAN_FILE_NAME)
        );
        assert_eq!(
            plan.ready_path().file_name().and_then(|name| name.to_str()),
            Some("ready")
        );
        assert_eq!(
            plan.armed_path().file_name().and_then(|name| name.to_str()),
            Some("armed")
        );
        assert_eq!(
            plan.accepted_path().file_name().and_then(|name| name.to_str()),
            Some("accepted")
        );
    }
}
