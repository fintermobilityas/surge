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
    schema: u32,
    pub(super) operation_id: String,
    pub(super) install_dir: PathBuf,
    pub(super) updater_pid: u32,
    pub(super) updater_start_time: u64,
    pub(super) updater_exe: PathBuf,
    pub(super) app_id: String,
    pub(super) current_version: String,
    pub(super) channel: String,
    pub(super) release_retention_limit: usize,
    pub(super) artifact_retention_policy: InstallArtifactCachePolicy,
    storage: ExternalStorageConfig,
    pub(super) release_index: ReleaseIndex,
    pub(super) current_release_identity: ReleaseIdentity,
    pub(super) target_release: ReleaseEntry,
    pub(super) restart_args: Vec<String>,
    pub(super) in_progress_template: UpdateStatusRecord,
    pub(super) previous_attempt_status: Option<UpdateStatusRecord>,
}

impl ExternalFinalizePlan {
    pub(super) fn from_manager(
        manager: &UpdateManager,
        target_release: &ReleaseEntry,
        updater_exe: PathBuf,
        updater_start_time: u64,
        restart_args: Vec<String>,
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
        if current_release_identity.supervisor_id.trim().is_empty() {
            return Err(SurgeError::Update(
                "External finalization requires a supervised current release".to_string(),
            ));
        }
        if target_release.supervisor_id.trim().is_empty() {
            return Err(SurgeError::Update(
                "External finalization requires a supervised target release".to_string(),
            ));
        }

        Ok(Self {
            schema: EXTERNAL_FINALIZE_SCHEMA,
            operation_id: Uuid::new_v4().to_string(),
            install_dir: manager.install_dir.clone(),
            updater_pid: current_pid(),
            updater_start_time,
            updater_exe,
            app_id: manager.app_id.clone(),
            current_version: manager.current_version.clone(),
            channel: manager.channel.clone(),
            release_retention_limit: manager.release_retention_limit,
            artifact_retention_policy: manager.artifact_retention_policy,
            storage: ExternalStorageConfig::from_storage_config(&manager.ctx.storage_config())?,
            release_index,
            current_release_identity,
            target_release: target_release.clone(),
            restart_args,
            in_progress_template: in_progress_template.clone(),
            previous_attempt_status,
        })
    }

    pub(super) fn operations_dir(&self) -> PathBuf {
        self.install_dir.join(EXTERNAL_FINALIZE_DIR)
    }

    pub(super) fn operation_dir(&self) -> PathBuf {
        self.operations_dir().join(&self.operation_id)
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

    pub(super) fn active_app_dir(&self) -> PathBuf {
        self.install_dir.join("app")
    }

    pub(super) fn previous_version_dir(&self) -> PathBuf {
        self.install_dir.join(format!("app-{}", self.current_version))
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
    if std::path::absolute(plan_path)? != plan.plan_path() {
        return Err(SurgeError::Config(
            "External finalizer plan path does not match its operation id".to_string(),
        ));
    }
    if plan.updater_pid == 0 || plan.updater_pid == current_pid() || plan.updater_start_time == 0 {
        return Err(SurgeError::Config(
            "External finalizer plan has an invalid updating process identity".to_string(),
        ));
    }
    if plan.app_id.trim().is_empty()
        || plan.current_version.trim().is_empty()
        || plan.channel.trim().is_empty()
        || plan.target_release.version.trim().is_empty()
        || plan.current_release_identity.supervisor_id.trim().is_empty()
        || plan.target_release.supervisor_id.trim().is_empty()
    {
        return Err(SurgeError::Config(
            "External finalizer plan is missing supervised application identity".to_string(),
        ));
    }
    if plan.target_release.version != plan.in_progress_template.target_version
        || plan.app_id != plan.in_progress_template.app_id
        || plan.channel != plan.in_progress_template.channel
        || plan.current_version != plan.in_progress_template.installed_version
    {
        return Err(SurgeError::Config(
            "External finalizer plan does not match its update status transaction".to_string(),
        ));
    }
    let expected_updater = plan.active_app_dir().join(&plan.current_release_identity.main_exe);
    if plan.updater_exe != std::fs::canonicalize(&expected_updater)? {
        return Err(SurgeError::Config(
            "External finalizer updater is not the stable active application executable".to_string(),
        ));
    }
    if !plan.release_index.releases.iter().any(|release| {
        release.version == plan.target_release.version
            && release.rid == plan.target_release.rid
            && release.full_sha256 == plan.target_release.full_sha256
    }) {
        return Err(SurgeError::Config(
            "External finalizer target does not match its captured release index".to_string(),
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
    manager.release_retention_limit = plan.release_retention_limit.max(1);
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
    if !plan.extracted_final_dir().is_dir() {
        return Err(SurgeError::Update("External finalizer payload is missing".to_string()));
    }
    if !plan.extracted_final_dir().join(&plan.target_release.main_exe).is_file() {
        return Err(SurgeError::Update(
            "External finalizer target executable is missing".to_string(),
        ));
    }
    if !plan.extracted_final_dir().join(supervisor_binary_name()).is_file() {
        return Err(SurgeError::Update(
            "External finalizer target supervisor is missing".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan_fixture() -> (tempfile::TempDir, UpdateManager, ReleaseEntry, UpdateStatusRecord) {
        let temp = tempfile::tempdir().unwrap();
        let context = Arc::new(Context::new());
        context.set_storage(
            StorageProvider::Filesystem,
            &temp.path().to_string_lossy(),
            "",
            "",
            "",
            "",
        );
        let mut manager = UpdateManager::new(
            Arc::clone(&context),
            "demo",
            "1.0.0",
            "test",
            &temp.path().to_string_lossy(),
        )
        .unwrap();
        context.set_storage(
            StorageProvider::Filesystem,
            &temp.path().to_string_lossy(),
            "",
            "secret-access-key",
            "secret-private-key",
            "",
        );
        manager.current_release_identity = Some(ReleaseIdentity {
            version: "1.0.0".to_string(),
            main_exe: "demo".to_string(),
            supervisor_id: "demo-supervisor".to_string(),
            environment: std::collections::BTreeMap::default(),
        });
        let target = ReleaseEntry {
            version: "2.0.0".to_string(),
            rid: "test-rid".to_string(),
            full_sha256: "target-sha".to_string(),
            main_exe: "demo".to_string(),
            supervisor_id: "demo-supervisor".to_string(),
            ..ReleaseEntry::default()
        };
        manager.cached_index = Some(ReleaseIndex {
            app_id: "demo".to_string(),
            releases: vec![target.clone()],
            ..ReleaseIndex::default()
        });
        let status =
            UpdateStatusRecord::in_progress("demo", "1.0.0", "2.0.0", "test", "2026-01-01T00:00:00Z".to_string());
        (temp, manager, target, status)
    }

    #[test]
    fn external_plan_rejects_unsupervised_current_or_target_release() {
        let (_temp, mut manager, mut target, status) = plan_fixture();
        manager.current_release_identity.as_mut().unwrap().supervisor_id.clear();

        let current_error = ExternalFinalizePlan::from_manager(
            &manager,
            &target,
            PathBuf::from("updater"),
            1,
            Vec::new(),
            &status,
            None,
        )
        .unwrap_err();
        assert!(current_error.to_string().contains("supervised current release"));

        manager.current_release_identity.as_mut().unwrap().supervisor_id = "demo-supervisor".to_string();
        target.supervisor_id.clear();
        let target_error = ExternalFinalizePlan::from_manager(
            &manager,
            &target,
            PathBuf::from("updater"),
            1,
            Vec::new(),
            &status,
            None,
        )
        .unwrap_err();
        assert!(target_error.to_string().contains("supervised target release"));
    }

    #[test]
    fn external_plan_does_not_serialize_storage_credentials() {
        let (_temp, manager, target, status) = plan_fixture();
        let plan = ExternalFinalizePlan::from_manager(
            &manager,
            &target,
            PathBuf::from("updater"),
            1,
            Vec::new(),
            &status,
            None,
        )
        .unwrap();

        let json = serde_json::to_string(&plan).unwrap();
        assert!(!json.contains("secret-access-key"));
        assert!(!json.contains("secret-private-key"));
    }
}
