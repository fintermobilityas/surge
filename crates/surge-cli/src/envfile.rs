use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::Mutex;
use std::sync::{OnceLock, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use surge_core::error::{Result, SurgeError};

const ENV_FILENAME: &str = ".env.surge";
const APP_ENV_FILENAME_PREFIX: &str = ".env.surge.";

#[derive(Default)]
struct ScopedStorageEnvState {
    global: BTreeMap<String, String>,
    per_app: BTreeMap<String, BTreeMap<String, String>>,
}

#[derive(Default)]
struct StorageEnvState {
    scopes: BTreeMap<PathBuf, ScopedStorageEnvState>,
}

static STORAGE_ENV_STATE: OnceLock<RwLock<StorageEnvState>> = OnceLock::new();

fn storage_env_state() -> &'static RwLock<StorageEnvState> {
    STORAGE_ENV_STATE.get_or_init(|| RwLock::new(StorageEnvState::default()))
}

fn read_state() -> RwLockReadGuard<'static, StorageEnvState> {
    storage_env_state().read().unwrap_or_else(PoisonError::into_inner)
}

fn write_state() -> RwLockWriteGuard<'static, StorageEnvState> {
    storage_env_state().write().unwrap_or_else(PoisonError::into_inner)
}

pub(crate) fn candidate_paths_for_manifest(manifest_path: &Path) -> Vec<PathBuf> {
    let Some(parent) = manifest_path.parent() else {
        return Vec::new();
    };

    let mut candidates = Vec::new();
    if parent.file_name().is_some_and(|name| name == ".surge")
        && let Some(project_root) = parent.parent()
    {
        candidates.push(project_root.join(ENV_FILENAME));
    }
    candidates.push(parent.join(ENV_FILENAME));
    candidates
}

pub(crate) fn candidate_paths_for_setup(dir: &Path) -> Vec<PathBuf> {
    vec![dir.join(ENV_FILENAME)]
}

pub(crate) fn load_storage_env_files(scope: &Path, candidates: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut state = ScopedStorageEnvState::default();
    let mut loaded = Vec::new();
    let mut seen_files = BTreeSet::new();
    let mut scanned_dirs = BTreeSet::new();

    for path in candidates {
        if seen_files.insert(path.clone()) && path.is_file() {
            apply_storage_env_file(path, &mut state.global)?;
            loaded.push(path.clone());
        }

        let Some(directory) = scan_directory_for_candidate(path) else {
            continue;
        };
        if !scanned_dirs.insert(directory.clone()) {
            continue;
        }

        for (app_id, app_path) in app_env_files_for_directory(&directory)? {
            if !seen_files.insert(app_path.clone()) || !app_path.is_file() {
                continue;
            }

            let overlay = state.per_app.entry(app_id).or_default();
            apply_storage_env_file(&app_path, overlay)?;
            loaded.push(app_path);
        }
    }

    write_state().scopes.insert(storage_env_scope_key(scope), state);
    Ok(loaded)
}

pub(crate) fn storage_env_lookup(name: &str, scope: &Path, app_id: Option<&str>) -> Option<String> {
    if let Ok(value) = std::env::var(name)
        && !value.trim().is_empty()
    {
        return Some(value);
    }

    let state = read_state();
    let scoped = state.scopes.get(&storage_env_scope_key(scope))?;
    if let Some(value) = app_id
        .and_then(|id| scoped.per_app.get(id))
        .and_then(|overlay| overlay.get(name))
        .filter(|value| !value.trim().is_empty())
    {
        return Some(value.clone());
    }

    scoped
        .global
        .get(name)
        .cloned()
        .filter(|value| !value.trim().is_empty())
}

fn apply_storage_env_file(path: &Path, overlay: &mut BTreeMap<String, String>) -> Result<()> {
    let contents = std::fs::read_to_string(path)
        .map_err(|e| SurgeError::Config(format!("Failed to read {}: {e}", path.display())))?;

    for (line_index, raw_line) in contents.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let line = strip_export_prefix(line);
        let (raw_key, raw_value) = line.split_once('=').ok_or_else(|| {
            SurgeError::Config(format!(
                "Invalid {} entry at {}:{}: expected KEY=VALUE",
                ENV_FILENAME,
                path.display(),
                line_index + 1
            ))
        })?;

        let key = raw_key.trim();
        if !is_valid_env_key(key) {
            return Err(SurgeError::Config(format!(
                "Invalid {} key '{}' at {}:{}",
                ENV_FILENAME,
                key,
                path.display(),
                line_index + 1
            )));
        }

        let value = parse_env_value(raw_value).map_err(|message| {
            SurgeError::Config(format!(
                "Invalid {} value for '{}' at {}:{}: {}",
                ENV_FILENAME,
                key,
                path.display(),
                line_index + 1,
                message
            ))
        })?;

        overlay.insert(key.to_string(), value);
    }

    Ok(())
}

fn app_env_files_for_directory(directory: &Path) -> Result<Vec<(String, PathBuf)>> {
    if !directory.is_dir() {
        return Ok(Vec::new());
    }

    let mut files = Vec::new();
    let entries = std::fs::read_dir(directory)
        .map_err(|e| SurgeError::Config(format!("Failed to read {}: {e}", directory.display())))?;

    for entry in entries {
        let entry = entry.map_err(|e| SurgeError::Config(format!("Failed to read {}: {e}", directory.display())))?;
        let path = entry.path();
        let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };

        let Some(app_id) = file_name.strip_prefix(APP_ENV_FILENAME_PREFIX) else {
            continue;
        };
        if app_id.trim().is_empty() {
            continue;
        }

        files.push((app_id.to_string(), path));
    }

    files.sort_by(|left, right| left.1.cmp(&right.1));
    Ok(files)
}

fn storage_env_scope_key(scope: &Path) -> PathBuf {
    if scope.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        scope.to_path_buf()
    }
}

fn scan_directory_for_candidate(path: &Path) -> Option<PathBuf> {
    let directory = path.parent()?;
    if directory.as_os_str().is_empty() {
        Some(PathBuf::from("."))
    } else {
        Some(directory.to_path_buf())
    }
}

fn strip_export_prefix(line: &str) -> &str {
    if let Some(rest) = line.strip_prefix("export")
        && rest.starts_with(char::is_whitespace)
    {
        return rest.trim_start();
    }

    line
}

fn is_valid_env_key(key: &str) -> bool {
    let mut chars = key.chars();
    let Some(first) = chars.next() else {
        return false;
    };

    (first == '_' || first.is_ascii_alphabetic()) && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn parse_env_value(raw_value: &str) -> std::result::Result<String, &'static str> {
    let trimmed = raw_value.trim();
    if trimmed.is_empty() {
        return Ok(String::new());
    }

    match trimmed.chars().next() {
        Some('"') => parse_quoted_env_value(trimmed, '"'),
        Some('\'') => parse_quoted_env_value(trimmed, '\''),
        _ => Ok(trimmed.to_string()),
    }
}

fn parse_quoted_env_value(value: &str, quote: char) -> std::result::Result<String, &'static str> {
    let mut result = String::new();
    let mut chars = value.chars();
    let Some(opening_quote) = chars.next() else {
        return Err("value is empty");
    };
    if opening_quote != quote {
        return Err("value starts with the wrong quote");
    }

    let mut escaped = false;
    let mut closed = false;

    for ch in chars {
        if quote == '"' && escaped {
            result.push(match ch {
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                '\\' => '\\',
                '"' => '"',
                other => other,
            });
            escaped = false;
            continue;
        }

        if quote == '"' && ch == '\\' {
            escaped = true;
            continue;
        }

        if ch == quote {
            closed = true;
            break;
        }

        result.push(ch);
    }

    if escaped {
        return Err("value ends with an unfinished escape sequence");
    }
    if !closed {
        return Err("missing closing quote");
    }

    let consumed = if quote == '"' {
        format!("\"{}\"", escape_double_quoted_value(&result))
    } else {
        format!("'{}'", result)
    };
    let remainder = value
        .strip_prefix(&consumed)
        .ok_or("quoted value could not be parsed")?
        .trim();
    if !remainder.is_empty() {
        return Err("unexpected trailing characters after closing quote");
    }

    Ok(result)
}

fn escape_double_quoted_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
        .replace('"', "\\\"")
}

#[cfg(test)]
pub(crate) fn with_storage_env_state_for_test<T>(
    scope: &Path,
    global: BTreeMap<String, String>,
    per_app: BTreeMap<String, BTreeMap<String, String>>,
    test: impl FnOnce() -> T,
) -> T {
    static TEST_STORAGE_ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    let _guard = TEST_STORAGE_ENV_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    *write_state() = StorageEnvState {
        scopes: BTreeMap::from([(storage_env_scope_key(scope), ScopedStorageEnvState { global, per_app })]),
    };

    struct ResetOverlayOnDrop;

    impl Drop for ResetOverlayOnDrop {
        fn drop(&mut self) {
            *write_state() = StorageEnvState::default();
        }
    }

    let _reset = ResetOverlayOnDrop;
    test()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidate_paths_include_project_root_when_manifest_lives_in_dot_surge() {
        let manifest = Path::new("/tmp/demo/.surge/surge.yml");
        let paths = candidate_paths_for_manifest(manifest);
        assert_eq!(
            paths,
            vec![
                PathBuf::from("/tmp/demo/.env.surge"),
                PathBuf::from("/tmp/demo/.surge/.env.surge")
            ]
        );
    }

    #[test]
    fn candidate_paths_use_manifest_directory_for_custom_manifest_locations() {
        let manifest = Path::new("/tmp/demo/custom/surge.yml");
        let paths = candidate_paths_for_manifest(manifest);
        assert_eq!(paths, vec![PathBuf::from("/tmp/demo/custom/.env.surge")]);
    }

    #[test]
    fn load_storage_env_files_applies_later_candidates_after_earlier_ones() {
        let scope = Path::new("/tmp/demo/.surge/surge.yml");
        with_storage_env_state_for_test(scope, BTreeMap::new(), BTreeMap::new(), || {
            let temp_dir = tempfile::tempdir().expect("temp dir");
            let project_env = temp_dir.path().join(".env.surge");
            let surge_dir = temp_dir.path().join(".surge");
            let local_env = surge_dir.join(".env.surge");
            std::fs::create_dir_all(&surge_dir).expect("surge dir");
            std::fs::write(&project_env, "AZURE_STORAGE_ACCOUNT_NAME=project\n").expect("project env");
            std::fs::write(&local_env, "AZURE_STORAGE_ACCOUNT_NAME=local\n").expect("local env");

            let loaded =
                load_storage_env_files(scope, &[project_env.clone(), local_env.clone()]).expect("load env files");

            assert_eq!(loaded, vec![project_env, local_env]);
            assert_eq!(
                storage_env_lookup("AZURE_STORAGE_ACCOUNT_NAME", scope, None).as_deref(),
                Some("local")
            );
        });
    }

    #[test]
    fn load_storage_env_files_parses_export_and_quoted_values() {
        let scope = Path::new("/tmp/demo/surge.yml");
        with_storage_env_state_for_test(scope, BTreeMap::new(), BTreeMap::new(), || {
            let temp_dir = tempfile::tempdir().expect("temp dir");
            let env_path = temp_dir.path().join(".env.surge");
            std::fs::write(
                &env_path,
                "export AZURE_STORAGE_ACCOUNT_NAME = demo-account\nAZURE_STORAGE_ACCOUNT_KEY=\"line\\nvalue\"\n",
            )
            .expect("env file");

            load_storage_env_files(scope, &[env_path]).expect("load env");

            assert_eq!(
                storage_env_lookup("AZURE_STORAGE_ACCOUNT_NAME", scope, None).as_deref(),
                Some("demo-account")
            );
            assert_eq!(
                storage_env_lookup("AZURE_STORAGE_ACCOUNT_KEY", scope, None).as_deref(),
                Some("line\nvalue")
            );
        });
    }

    #[test]
    fn load_storage_env_files_supports_per_app_overlays() {
        let scope = Path::new("/tmp/demo/surge.yml");
        with_storage_env_state_for_test(scope, BTreeMap::new(), BTreeMap::new(), || {
            let temp_dir = tempfile::tempdir().expect("temp dir");
            let shared_env = temp_dir.path().join(".env.surge");
            let app_env = temp_dir.path().join(".env.surge.app-a");
            std::fs::write(&shared_env, "AZURE_STORAGE_ACCOUNT_NAME=shared\n").expect("shared env");
            std::fs::write(
                &app_env,
                "AZURE_STORAGE_ACCOUNT_NAME=app-specific\nAZURE_STORAGE_ACCOUNT_KEY=app-key\n",
            )
            .expect("app env");

            let loaded = load_storage_env_files(scope, std::slice::from_ref(&shared_env)).expect("load env files");

            assert_eq!(loaded, vec![shared_env, app_env]);
            assert_eq!(
                storage_env_lookup("AZURE_STORAGE_ACCOUNT_NAME", scope, None).as_deref(),
                Some("shared")
            );
            assert_eq!(
                storage_env_lookup("AZURE_STORAGE_ACCOUNT_NAME", scope, Some("app-a")).as_deref(),
                Some("app-specific")
            );
            assert_eq!(
                storage_env_lookup("AZURE_STORAGE_ACCOUNT_KEY", scope, Some("app-a")).as_deref(),
                Some("app-key")
            );
        });
    }

    #[test]
    fn scan_directory_for_relative_candidates_uses_current_directory() {
        assert_eq!(
            scan_directory_for_candidate(Path::new(".env.surge")),
            Some(PathBuf::from("."))
        );
    }

    #[test]
    fn app_env_files_for_missing_directory_returns_empty() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let missing_dir = temp_dir.path().join("missing");
        let files = app_env_files_for_directory(&missing_dir).expect("missing directories should be skipped");
        assert!(files.is_empty());
    }

    #[test]
    fn load_storage_env_files_keeps_scopes_isolated() {
        let default_scope = Path::new("/tmp/default/surge.yml");
        with_storage_env_state_for_test(default_scope, BTreeMap::new(), BTreeMap::new(), || {
            let temp_dir = tempfile::tempdir().expect("temp dir");
            let source_dir = temp_dir.path().join("source");
            let dest_dir = temp_dir.path().join("dest");
            std::fs::create_dir_all(&source_dir).expect("source dir");
            std::fs::create_dir_all(&dest_dir).expect("dest dir");

            let source_scope = source_dir.join("surge.yml");
            let dest_scope = dest_dir.join("surge.yml");
            let source_env = source_dir.join(".env.surge");
            let dest_env = dest_dir.join(".env.surge");
            std::fs::write(&source_env, "AZURE_STORAGE_ACCOUNT_NAME=source\n").expect("source env");
            std::fs::write(&dest_env, "AZURE_STORAGE_ACCOUNT_NAME=dest\n").expect("dest env");

            load_storage_env_files(&source_scope, std::slice::from_ref(&source_env)).expect("load source env");
            load_storage_env_files(&dest_scope, std::slice::from_ref(&dest_env)).expect("load dest env");

            assert_eq!(
                storage_env_lookup("AZURE_STORAGE_ACCOUNT_NAME", &source_scope, None).as_deref(),
                Some("source")
            );
            assert_eq!(
                storage_env_lookup("AZURE_STORAGE_ACCOUNT_NAME", &dest_scope, None).as_deref(),
                Some("dest")
            );
        });
    }
}
