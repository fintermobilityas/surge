use std::path::{Component, Path};

use crate::error::{Result, SurgeError};
use crate::install::read_runtime_manifest_identity;
use crate::supervisor::state::read_supervisor_exe_path;

use super::{UpdateManager, apply};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ReleaseIdentity {
    pub(super) main_exe: String,
    pub(super) supervisor_id: String,
}

pub(super) fn load(manager: &UpdateManager) -> Result<Option<ReleaseIdentity>> {
    let Some(active_app_dir) = apply::find_previous_app_dir(&manager.install_dir, &manager.current_version) else {
        return Ok(None);
    };
    let Some(manifest) = read_runtime_manifest_identity(&active_app_dir)? else {
        return Ok(None);
    };

    if manifest.app_id != manager.app_id {
        return Err(SurgeError::Update(format!(
            "Installed runtime manifest app '{}' does not match update app '{}'",
            manifest.app_id, manager.app_id
        )));
    }
    if manifest.version != manager.current_version {
        return Err(SurgeError::Update(format!(
            "Installed runtime manifest version '{}' does not exactly match update version '{}'",
            manifest.version, manager.current_version
        )));
    }

    let main_exe = if manifest.main_exe.is_empty() {
        legacy_main_exe(manager, &active_app_dir, &manifest.app_id, &manifest.supervisor_id)
    } else {
        Some(manifest.main_exe)
    };

    Ok(main_exe.map(|main_exe| ReleaseIdentity {
        main_exe,
        supervisor_id: manifest.supervisor_id,
    }))
}

fn legacy_main_exe(
    manager: &UpdateManager,
    active_app_dir: &Path,
    app_id: &str,
    supervisor_id: &str,
) -> Option<String> {
    if let Some(supervisor_exe) = read_supervisor_exe_path(&manager.install_dir, supervisor_id) {
        let stable_app_dir = manager.install_dir.join("app");
        let versioned_app_dir = manager.install_dir.join(format!("app-{}", manager.current_version));
        for root in [active_app_dir, stable_app_dir.as_path(), versioned_app_dir.as_path()] {
            if let Ok(relative) = supervisor_exe.strip_prefix(root)
                && let Some(main_exe) = safe_relative_path(relative)
            {
                return Some(main_exe);
            }
        }

        if supervisor_exe.is_relative() {
            for root in [active_app_dir, stable_app_dir.as_path(), versioned_app_dir.as_path()] {
                let candidate = root.join(&supervisor_exe);
                if candidate.exists()
                    && let Some(main_exe) = safe_relative_path(&supervisor_exe)
                {
                    return Some(main_exe);
                }
            }
        }
    }

    let app_id_path = Path::new(app_id);
    safe_relative_path(app_id_path).filter(|main_exe| {
        let candidate = active_app_dir.join(main_exe);
        candidate.is_file()
            || std::fs::symlink_metadata(candidate).is_ok_and(|metadata| metadata.file_type().is_symlink())
    })
}

fn safe_relative_path(path: &Path) -> Option<String> {
    if path.as_os_str().is_empty()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return None;
    }
    path.to_str().map(ToString::to_string)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::context::{Context, StorageProvider};
    use crate::supervisor::state::write_supervisor_exe_path;

    use super::*;

    #[test]
    fn safe_relative_path_rejects_escape_and_absolute_paths() {
        assert_eq!(safe_relative_path(Path::new("bin/demo")).as_deref(), Some("bin/demo"));
        assert_eq!(safe_relative_path(Path::new("../demo")), None);
        assert_eq!(safe_relative_path(Path::new("/opt/demo")), None);
    }

    #[test]
    fn legacy_manifest_uses_local_supervisor_executable_identity() {
        let tmp = tempfile::tempdir().unwrap();
        let store_dir = tmp.path().join("store");
        let install_dir = tmp.path().join("install");
        let active_app_dir = install_dir.join("app");
        std::fs::create_dir_all(&store_dir).unwrap();
        std::fs::create_dir_all(active_app_dir.join(".surge")).unwrap();
        std::fs::write(active_app_dir.join("demo-entrypoint"), "fixture").unwrap();
        std::fs::write(
            active_app_dir.join(crate::install::RUNTIME_MANIFEST_RELATIVE_PATH),
            "id: demo\nversion: 1.0.0\nsupervisorId: demo-supervisor\n",
        )
        .unwrap();
        write_supervisor_exe_path(&install_dir, "demo-supervisor", &active_app_dir.join("demo-entrypoint")).unwrap();

        let ctx = Arc::new(Context::new());
        ctx.set_storage(StorageProvider::Filesystem, store_dir.to_str().unwrap(), "", "", "", "");
        let manager = UpdateManager::new(ctx, "demo", "1.0.0", "stable", install_dir.to_str().unwrap()).unwrap();

        let identity = load(&manager).unwrap().unwrap();

        assert_eq!(identity.main_exe, "demo-entrypoint");
        assert_eq!(identity.supervisor_id, "demo-supervisor");
    }
}
