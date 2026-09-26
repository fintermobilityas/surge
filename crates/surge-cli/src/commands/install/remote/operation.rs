use super::{InstallBehavior, ReleaseEntry, Result};
use surge_core::context::StorageConfig;

pub(super) fn request_fingerprint(
    app_id: &str,
    rid: &str,
    release: &ReleaseEntry,
    channel: &str,
    storage: &StorageConfig,
    behavior: InstallBehavior,
) -> Result<String> {
    let intent = serde_json::to_vec(&(
        super::state::remote_staged_payload_identity(app_id, release, channel, storage),
        rid,
        (
            &release.main_exe,
            &release.environment,
            &release.persistent_assets,
            &release.shortcuts,
            &release.icon,
            &release.name,
        ),
        &storage.prefix,
        behavior.no_start,
        behavior.mode.is_stage(),
        behavior.force,
    ))?;
    Ok(surge_core::crypto::sha256::sha256_hex(&intent))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::install::InstallMode;

    #[test]
    fn fingerprint_tracks_request_intent_and_ignores_credential_rotation() {
        let mut release = ReleaseEntry {
            version: "1.2.3".into(),
            full_sha256: "archive-hash".into(),
            ..ReleaseEntry::default()
        };
        let mut storage = StorageConfig {
            bucket: "fixture".into(),
            ..StorageConfig::default()
        };
        let behavior = InstallBehavior::default();
        let fingerprint = |release: &ReleaseEntry, storage: &StorageConfig, behavior| {
            request_fingerprint("demoapp", "linux-x64", release, "test", storage, behavior).unwrap()
        };
        let initial = fingerprint(&release, &storage, behavior);
        storage.access_key = "rotated-access".into();
        storage.secret_key = "rotated-secret".into();
        release.channels.push("another-channel".into());
        release.release_notes = "updated notes".into();
        assert_eq!(fingerprint(&release, &storage, behavior), initial);
        for changed in [
            InstallBehavior {
                force: true,
                ..behavior
            },
            InstallBehavior {
                no_start: true,
                ..behavior
            },
            InstallBehavior {
                mode: InstallMode::StageOnly,
                ..behavior
            },
        ] {
            assert_ne!(fingerprint(&release, &storage, changed), initial);
        }
        storage.prefix = "other-scope".into();
        assert_ne!(fingerprint(&release, &storage, behavior), initial);
        storage.prefix.clear();
        release.full_sha256 = "different-archive".into();
        assert_ne!(fingerprint(&release, &storage, behavior), initial);
    }
}
