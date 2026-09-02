use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};

use crate::error::{Result, SurgeError};
use crate::install::read_runtime_manifest_identity;
use crate::releases::version::compare_versions;
use crate::supervisor::state::read_supervisor_exe_path;

use super::{UpdateManager, apply};

#[derive(Debug, Clone)]
pub(super) struct ReleaseIdentity {
    pub(super) version: String,
    pub(super) main_exe: String,
    pub(super) supervisor_id: String,
    pub(super) environment: BTreeMap<String, String>,
}

impl PartialEq for ReleaseIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.version == other.version
            && self.main_exe == other.main_exe
            && self.supervisor_id == other.supervisor_id
            && self.environment == other.environment
    }
}

impl Eq for ReleaseIdentity {}

pub(super) fn load(manager: &UpdateManager) -> Result<Option<ReleaseIdentity>> {
    let Some(active_app_dir) = apply::find_previous_app_dir(&manager.install_dir, &manager.current_version) else {
        return Ok(None);
    };

    load_from_app_dir(manager, &active_app_dir, Some(&manager.current_version))
}

#[cfg(unix)]
pub(super) fn load_previous_swap(manager: &UpdateManager, app_dir: &Path) -> Result<Option<ReleaseIdentity>> {
    load_from_app_dir(manager, app_dir, None)
}

fn load_from_app_dir(
    manager: &UpdateManager,
    app_dir: &Path,
    expected_version: Option<&str>,
) -> Result<Option<ReleaseIdentity>> {
    let Some(manifest) = read_runtime_manifest_identity(app_dir)? else {
        return Ok(None);
    };

    if manifest.app_id != manager.app_id {
        return Err(SurgeError::Update(format!(
            "Installed runtime manifest app '{}' does not match update app '{}'",
            manifest.app_id, manager.app_id
        )));
    }
    if let Some(expected_version) = expected_version
        && compare_versions(&manifest.version, expected_version) != Ordering::Equal
    {
        return Err(SurgeError::Update(format!(
            "Installed runtime manifest version '{}' does not match update version '{}'",
            manifest.version, expected_version
        )));
    }

    let main_exe = if manifest.main_exe.is_empty() {
        legacy_main_exe(
            manager,
            app_dir,
            &manifest.version,
            &manifest.app_id,
            &manifest.supervisor_id,
        )
        .ok_or_else(|| {
            SurgeError::Update(format!(
                "Installed runtime manifest for '{}' does not declare mainExe and no local executable identity could be inferred",
                manifest.app_id
            ))
        })?
    } else {
        safe_relative_path(Path::new(&manifest.main_exe)).ok_or_else(|| {
            SurgeError::Update("Installed runtime manifest mainExe must be a safe relative path".to_string())
        })?
    };

    Ok(Some(ReleaseIdentity {
        version: manifest.version,
        main_exe,
        supervisor_id: manifest.supervisor_id,
        environment: manifest.environment,
    }))
}

fn legacy_main_exe(
    manager: &UpdateManager,
    active_app_dir: &Path,
    installed_version: &str,
    app_id: &str,
    supervisor_id: &str,
) -> Option<String> {
    if let Some(supervisor_exe) = read_supervisor_exe_path(&manager.install_dir, supervisor_id) {
        let bound_supervisor_exe = bind_absolute_parent(&supervisor_exe);
        let stable_app_dir = manager.install_dir.join("app");
        let versioned_app_dir = manager.install_dir.join(format!("app-{installed_version}"));
        for root in [active_app_dir, stable_app_dir.as_path(), versioned_app_dir.as_path()] {
            if let Ok(relative) = bound_supervisor_exe.strip_prefix(root)
                && is_executable_entry(&bound_supervisor_exe)
                && let Some(main_exe) = safe_relative_path(relative)
            {
                return Some(main_exe);
            }
        }

        if supervisor_exe.is_relative() {
            for root in [active_app_dir, stable_app_dir.as_path(), versioned_app_dir.as_path()] {
                let candidate = root.join(&supervisor_exe);
                if is_executable_entry(&candidate)
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
        is_executable_entry(&candidate)
    })
}

fn is_executable_entry(path: &Path) -> bool {
    path.is_file() || std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_symlink())
}

fn bind_absolute_parent(path: &Path) -> PathBuf {
    if !path.is_absolute() {
        return path.to_path_buf();
    }

    let Some(parent) = path.parent() else {
        return path.to_path_buf();
    };
    let Some(file_name) = path.file_name() else {
        return path.to_path_buf();
    };

    std::fs::canonicalize(parent).map_or_else(|_| path.to_path_buf(), |bound_parent| bound_parent.join(file_name))
}

pub(super) fn safe_relative_path(path: &Path) -> Option<String> {
    let mut has_normal_component = false;
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(_) => has_normal_component = true,
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }

    if !has_normal_component {
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
        assert_eq!(safe_relative_path(Path::new("./demo")).as_deref(), Some("./demo"));
        assert_eq!(safe_relative_path(Path::new(".")), None);
        assert_eq!(safe_relative_path(Path::new("")), None);
        assert_eq!(safe_relative_path(Path::new("../demo")), None);
        assert_eq!(safe_relative_path(Path::new("/opt/demo")), None);
    }

    #[cfg(unix)]
    #[test]
    fn bind_absolute_parent_resolves_parent_alias_without_resolving_final_symlink() {
        use std::os::unix::fs::symlink;

        let tmp = tempfile::tempdir().unwrap();
        let physical_parent = tmp.path().join("physical");
        let aliased_parent = tmp.path().join("alias");
        std::fs::create_dir_all(&physical_parent).unwrap();
        std::fs::write(physical_parent.join("target"), "fixture").unwrap();
        symlink("target", physical_parent.join("entrypoint")).unwrap();
        symlink(&physical_parent, &aliased_parent).unwrap();

        let bound = bind_absolute_parent(&aliased_parent.join("entrypoint"));
        let bound_physical_parent = std::fs::canonicalize(&physical_parent).unwrap();

        assert_eq!(bound, bound_physical_parent.join("entrypoint"));
        assert_ne!(
            bound,
            std::fs::canonicalize(physical_parent.join("entrypoint")).unwrap()
        );
        assert!(is_executable_entry(&bound));
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

        assert_eq!(identity.version, "1.0.0");
        assert_eq!(identity.main_exe, "demo-entrypoint");
        assert_eq!(identity.supervisor_id, "demo-supervisor");
    }

    #[test]
    fn runtime_manifest_rejects_main_exe_outside_the_install() {
        let tmp = tempfile::tempdir().unwrap();
        let store_dir = tmp.path().join("store");
        let install_dir = tmp.path().join("install");
        let active_app_dir = install_dir.join("app");
        std::fs::create_dir_all(&store_dir).unwrap();
        std::fs::create_dir_all(active_app_dir.join(".surge")).unwrap();
        std::fs::write(
            active_app_dir.join(crate::install::RUNTIME_MANIFEST_RELATIVE_PATH),
            "id: demo\nversion: 1.0.0\nmainExe: ../other-app\n",
        )
        .unwrap();

        let ctx = Arc::new(Context::new());
        ctx.set_storage(StorageProvider::Filesystem, store_dir.to_str().unwrap(), "", "", "", "");
        let manager = UpdateManager::new(ctx, "demo", "1.0.0", "stable", install_dir.to_str().unwrap()).unwrap();

        let error = load(&manager).unwrap_err();

        assert!(error.to_string().contains("mainExe must be a safe relative path"));
    }

    #[test]
    fn legacy_manifest_without_resolvable_main_exe_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let store_dir = tmp.path().join("store");
        let install_dir = tmp.path().join("install");
        let active_app_dir = install_dir.join("app");
        std::fs::create_dir_all(&store_dir).unwrap();
        std::fs::create_dir_all(active_app_dir.join(".surge")).unwrap();
        std::fs::write(active_app_dir.join("different-entrypoint"), "fixture").unwrap();
        std::fs::write(
            active_app_dir.join(crate::install::RUNTIME_MANIFEST_RELATIVE_PATH),
            "id: demo\nversion: 1.0.0\nsupervisorId: missing-supervisor\nenvironment:\n  MODE: original\n",
        )
        .unwrap();

        let ctx = Arc::new(Context::new());
        ctx.set_storage(StorageProvider::Filesystem, store_dir.to_str().unwrap(), "", "", "", "");
        let manager = UpdateManager::new(ctx, "demo", "1.0.0", "stable", install_dir.to_str().unwrap()).unwrap();

        let error = load(&manager).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("no local executable identity could be inferred")
        );
    }

    #[test]
    fn legacy_manifest_rejects_non_executable_supervisor_paths() {
        let tmp = tempfile::tempdir().unwrap();
        let store_dir = tmp.path().join("store");
        let install_dir = tmp.path().join("install");
        let active_app_dir = install_dir.join("app");
        std::fs::create_dir_all(&store_dir).unwrap();
        std::fs::create_dir_all(active_app_dir.join("directory-entrypoint")).unwrap();
        std::fs::create_dir_all(active_app_dir.join(".surge")).unwrap();
        std::fs::write(
            active_app_dir.join(crate::install::RUNTIME_MANIFEST_RELATIVE_PATH),
            "id: demo\nversion: 1.0.0\nsupervisorId: demo-supervisor\n",
        )
        .unwrap();

        let ctx = Arc::new(Context::new());
        ctx.set_storage(StorageProvider::Filesystem, store_dir.to_str().unwrap(), "", "", "", "");
        let manager = UpdateManager::new(ctx, "demo", "1.0.0", "stable", install_dir.to_str().unwrap()).unwrap();

        for supervisor_exe in [
            active_app_dir.join("missing-entrypoint"),
            PathBuf::from("directory-entrypoint"),
        ] {
            write_supervisor_exe_path(&install_dir, "demo-supervisor", &supervisor_exe).unwrap();

            let error = load(&manager).unwrap_err();

            assert!(
                error
                    .to_string()
                    .contains("no local executable identity could be inferred"),
                "unexpected result for {}",
                supervisor_exe.display()
            );
        }
    }
}
