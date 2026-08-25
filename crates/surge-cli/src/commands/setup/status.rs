use std::path::{Path, PathBuf};

use serde::Deserialize;
use surge_core::config::installer::InstallerManifest;
use surge_core::install::RUNTIME_MANIFEST_RELATIVE_PATH;
use surge_core::update::status::{self as update_status, FailureContext, UpdateStatusRecord, write_update_status};

pub(super) const PHASE_STAGE_RECEIVED: &str = "stage received";
pub(super) const PHASE_RELEASE_RESOLVED: &str = "release or delta resolved";
pub(super) const PHASE_PACKAGE_DOWNLOADING: &str = "package downloading";
pub(super) const PHASE_PACKAGE_DOWNLOADED: &str = "package downloaded";
pub(super) const PHASE_SUPERVISOR_STOP_REQUESTED: &str = "supervisor stop requested";
pub(super) const PHASE_APP_SWAP_STARTED: &str = "app swap started";
pub(super) const PHASE_PERSISTENT_ASSETS_COPIED: &str = "persistent assets copied";
pub(super) const PHASE_SUPERVISOR_RESTART_REQUESTED: &str = "supervisor restart requested";
pub(super) const PHASE_SUPERVISOR_RESTART_CONFIRMED: &str = "supervisor restart confirmed";

const NOT_INSTALLED_VERSION: &str = "not_installed";

#[derive(Debug)]
pub(super) struct SetupStatus {
    install_root: PathBuf,
    app_id: String,
    installed_version: String,
    target_version: String,
    channel: String,
    attempted_at_utc: String,
    previous_attempt_record: Option<UpdateStatusRecord>,
}

impl SetupStatus {
    pub(super) fn new(manifest: &InstallerManifest, install_root: &Path) -> Self {
        let app_id = manifest.app_id.trim().to_string();
        let target_version = manifest.version.trim().to_string();
        Self {
            install_root: install_root.to_path_buf(),
            previous_attempt_record: current_record(install_root, &app_id, &target_version),
            app_id,
            installed_version: installed_version(install_root),
            target_version,
            channel: manifest.channel.trim().to_string(),
            attempted_at_utc: update_status::now_utc_rfc3339(),
        }
    }

    pub(super) fn record_phase(&self, phase: &'static str) {
        let now = update_status::now_utc_rfc3339();
        let mut record = self.in_progress_record().with_current_phase_at(phase, now);
        if let Some(existing) = self.current_record() {
            record.last_completed_phase = existing.last_completed_phase;
        }
        self.write(&record);
    }

    pub(super) fn record_completed_phase(&self, phase: &'static str) {
        let record = self
            .in_progress_record()
            .with_completed_phase_at(phase, update_status::now_utc_rfc3339());
        self.write(&record);
    }

    pub(super) fn record_converged(&self, supervisor_restart_confirmed: bool) {
        self.write(&UpdateStatusRecord::converged(
            &self.app_id,
            &self.target_version,
            &self.channel,
            Some(self.attempted_at_utc.clone()),
            update_status::now_utc_rfc3339(),
            supervisor_restart_confirmed,
        ));
    }

    pub(super) fn record_pending_restart(&self, reason: &str) {
        self.write(&UpdateStatusRecord::pending_restart(
            &self.app_id,
            &self.target_version,
            &self.target_version,
            &self.channel,
            self.attempted_at_utc.clone(),
            update_status::now_utc_rfc3339(),
            reason,
        ));
    }

    pub(super) fn record_failed(&self, reason: &str) {
        let current = self.current_record();
        let schedule = update_status::retry_schedule(self.previous_attempt_record.as_ref(), &self.target_version);
        let record = UpdateStatusRecord::failed_with_context(
            &self.app_id,
            &self.installed_version,
            &self.target_version,
            &self.channel,
            self.attempted_at_utc.clone(),
            reason,
            FailureContext::from_record(current.as_ref(), true),
        )
        .with_retry_schedule_at(
            &schedule,
            update_status::next_retry_timestamp(chrono::Utc::now(), &schedule),
        );
        self.write(&record);
    }

    fn in_progress_record(&self) -> UpdateStatusRecord {
        UpdateStatusRecord::in_progress(
            &self.app_id,
            &self.installed_version,
            &self.target_version,
            &self.channel,
            self.attempted_at_utc.clone(),
        )
    }

    fn current_record(&self) -> Option<UpdateStatusRecord> {
        current_record(&self.install_root, &self.app_id, &self.target_version)
    }

    fn write(&self, record: &UpdateStatusRecord) {
        let _ = write_update_status(&self.install_root, record);
    }
}

fn current_record(install_root: &Path, app_id: &str, target_version: &str) -> Option<UpdateStatusRecord> {
    update_status::read_update_status(install_root)
        .ok()
        .flatten()
        .filter(|record| record.app_id == app_id && record.target_version == target_version)
}

#[derive(Debug, Deserialize)]
struct RuntimeManifest {
    #[serde(default)]
    version: String,
}

fn installed_version(install_root: &Path) -> String {
    let path = install_root.join("app").join(RUNTIME_MANIFEST_RELATIVE_PATH);
    let Ok(bytes) = std::fs::read(path) else {
        return NOT_INSTALLED_VERSION.to_string();
    };
    let Ok(manifest) = serde_yaml::from_slice::<RuntimeManifest>(&bytes) else {
        return NOT_INSTALLED_VERSION.to_string();
    };
    let version = manifest.version.trim();
    if version.is_empty() {
        NOT_INSTALLED_VERSION.to_string()
    } else {
        version.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_failed_uses_pre_attempt_failed_record_for_backoff() {
        let install_root = tempfile::tempdir().expect("temp install root");
        let manifest: InstallerManifest = serde_yaml::from_str(
            r#"
schema: 1
format: surge-installer-v1
ui: console
installer_type: online
app_id: demo-app
rid: linux-x64
version: "1.2.3"
channel: stable
generated_utc: "2026-05-11T14:00:00Z"
release_index_key: releases.zstd
storage:
  provider: filesystem
  bucket: /tmp/store
release:
  full_filename: demo-full.tar.zst
runtime:
  name: Demo
  main_exe: demo
"#,
        )
        .expect("manifest parses");

        let previous_failed = UpdateStatusRecord::failed(
            "demo-app",
            "1.2.2",
            "1.2.3",
            "stable",
            "2026-05-11T14:00:00Z".to_string(),
            "previous failure",
        )
        .with_retry_schedule_at(
            &update_status::RetrySchedule::base(),
            "2026-05-11T14:05:00Z".to_string(),
        );
        write_update_status(install_root.path(), &previous_failed).expect("seed previous failure status");

        let setup = SetupStatus::new(&manifest, install_root.path());
        setup.record_phase(PHASE_STAGE_RECEIVED);
        setup.record_failed("current failure");

        let failed = update_status::read_update_status(install_root.path())
            .expect("status read succeeds")
            .expect("status exists");
        assert_eq!(failed.retry_count, Some(2));
    }
}
