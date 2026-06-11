use std::collections::BTreeMap;

use super::{Path, PathBuf, Result, core_install, shell_single_quote};

pub(super) fn remote_linux_shortcut_icon_path(
    staged_app_dir: &Path,
    remote_app_dir: &Path,
    app_id: &str,
    main_exe_name: &str,
    configured_icon: &str,
) -> PathBuf {
    let configured_icon = configured_icon.trim();
    if !configured_icon.is_empty() {
        let candidate = Path::new(configured_icon);
        return if candidate.is_absolute() {
            candidate.to_path_buf()
        } else {
            remote_app_dir.join(candidate)
        };
    }

    let mut candidates = Vec::new();
    for stem in [main_exe_name.trim(), app_id.trim(), "icon", "logo"] {
        if stem.is_empty() {
            continue;
        }
        for ext in ["svg", "png", "xpm"] {
            candidates.push(PathBuf::from(format!("{stem}.{ext}")));
            candidates.push(Path::new(".surge").join(format!("{stem}.{ext}")));
        }
    }

    for candidate in candidates {
        if staged_app_dir.join(&candidate).is_file() {
            return remote_app_dir.join(candidate);
        }
    }

    remote_app_dir.join(main_exe_name)
}

pub(super) fn stage_remote_linux_shortcuts(
    stage_root: &Path,
    rendered: &[surge_core::platform::shortcuts::LinuxShortcutFile],
) -> Result<()> {
    for shortcut in rendered {
        let target_dir = match shortcut.location {
            surge_core::config::manifest::ShortcutLocation::Desktop
            | surge_core::config::manifest::ShortcutLocation::StartMenu => {
                stage_root.join("shortcuts").join("applications")
            }
            surge_core::config::manifest::ShortcutLocation::Startup => stage_root.join("shortcuts").join("autostart"),
        };
        std::fs::create_dir_all(&target_dir)?;
        std::fs::write(target_dir.join(&shortcut.file_name), shortcut.content.as_bytes())?;
    }
    Ok(())
}

pub(super) fn shell_export_lines(environment: &BTreeMap<String, String>) -> String {
    let mut lines = String::new();
    for (key, value) in environment {
        lines.push_str("export ");
        lines.push_str(key);
        lines.push('=');
        lines.push_str(&shell_single_quote(value));
        lines.push('\n');
    }
    lines
}

pub(crate) fn build_remote_app_copy_activation_script(
    install_root: &Path,
    main_exe: &str,
    version: &str,
    environment: &BTreeMap<String, String>,
    persistent_assets: &[String],
    legacy_app_dir: Option<&Path>,
    no_start: bool,
) -> Result<String> {
    let install_root_quoted = shell_single_quote(&install_root.to_string_lossy());
    let main_exe_quoted = shell_single_quote(main_exe);
    let version_quoted = shell_single_quote(version);
    let exports = shell_export_lines(environment);
    let legacy_app_dir_quoted =
        legacy_app_dir.map_or_else(|| "''".to_string(), |path| shell_single_quote(&path.to_string_lossy()));
    let runtime_manifest_relative_path = core_install::RUNTIME_MANIFEST_RELATIVE_PATH;
    let legacy_runtime_manifest_relative_path = core_install::LEGACY_RUNTIME_MANIFEST_RELATIVE_PATH;
    let persistent_asset_commands = persistent_assets
        .iter()
        .map(|asset| {
            core_install::validate_relative_persistent_asset_path(asset).map(|relative| {
                format!(
                    "  copy_persistent_asset {}\n\\\n",
                    shell_single_quote(&relative.to_string_lossy())
                )
            })
        })
        .collect::<Result<Vec<_>>>()?
        .join("");
    let persistent_asset_block = format!(
        "legacy_app_dir={legacy_app_dir_quoted}\n\\\n\
active_runtime_manifest=\"$active_app_dir/{runtime_manifest_relative_path}\"\n\\\n\
active_legacy_runtime_manifest=\"$active_app_dir/{legacy_runtime_manifest_relative_path}\"\n\\\n\
runtime_manifest_backup=\"$stage_dir/.surge-runtime-next.yml\"\n\\\n\
legacy_runtime_manifest_backup=\"$stage_dir/.surge-surge-next.yml\"\n\\\n\
\n\\\n\
copy_persistent_asset() {{\n\\\n\
  relative_path=\"$1\"\n\\\n\
  source=\"$persistent_source_dir/$relative_path\"\n\\\n\
  destination=\"$active_app_dir/$relative_path\"\n\\\n\
  if [ ! -e \"$source\" ]; then\n\\\n\
    return 0\n\\\n\
  fi\n\\\n\
  if [ -d \"$source\" ]; then\n\\\n\
    rm -rf \"$destination\"\n\\\n\
    mkdir -p \"$(dirname \"$destination\")\"\n\\\n\
    cp -a \"$source\" \"$destination\"\n\\\n\
  else\n\\\n\
    mkdir -p \"$(dirname \"$destination\")\"\n\\\n\
    if [ -d \"$destination\" ]; then\n\\\n\
      rm -rf \"$destination\"\n\\\n\
    fi\n\\\n\
    cp -a \"$source\" \"$destination\"\n\\\n\
  fi\n\\\n\
}}\n\\\n\
\n\\\n",
    );

    let mut script = format!(
        "set -eu\n\
install_root={install_root_quoted}\n\
stage_dir=\"$install_root/.surge-transfer-stage\"\n\
next_app_dir=\"$install_root/.surge-app-next\"\n\
active_app_dir=\"$install_root/app\"\n\
previous_app_dir=\"$install_root/.surge-app-prev\"\n\
applications_dir=\"$HOME/.local/share/applications\"\n\
autostart_dir=\"$HOME/.config/autostart\"\n\
main_exe={main_exe_quoted}\n\
version={version_quoted}\n\
{persistent_asset_block}\
\n\
kill_matching() {{\n\
  pattern=\"$1\"\n\
  if ! command -v pgrep >/dev/null 2>&1; then\n\
    return 0\n\
  fi\n\
  for pid in $(pgrep -u \"$(id -u)\" -f \"$pattern\" 2>/dev/null || true); do\n\
    case \"$pid\" in\n\
      \"$$\"|\"$PPID\")\n\
        continue\n\
        ;;\n\
    esac\n\
    kill \"$pid\" 2>/dev/null || true\n\
  done\n\
}}\n\
\n\
stale_app_pids() {{\n\
  for exe_link in /proc/[0-9]*/exe; do\n\
    [ -e \"$exe_link\" ] || continue\n\
    pid=\"${{exe_link%/exe}}\"\n\
    pid=\"${{pid##*/}}\"\n\
    case \"$pid\" in \"$$\"|\"$PPID\") continue ;; esac\n\
    actual=\"$(readlink \"$exe_link\" 2>/dev/null || true)\"\n\
    case \"$actual\" in *\" (deleted)\") actual=\"${{actual% (deleted)}}\" ;; esac\n\
    case \"$actual\" in \"$install_root\"/app-*/\"$main_exe\"|\"$install_root\"/.surge-app-prev/\"$main_exe\"|\"$install_root\"/\"$main_exe\") printf '%s ' \"$pid\" ;; esac\n\
  done\n\
}}\n\
\n\
terminate_stale_app_processes() {{\n\
  pids=\"$(stale_app_pids)\"\n\
  [ -n \"$pids\" ] || return 0\n\
  kill $pids 2>/dev/null || true\n\
  i=0\n\
  while [ \"$i\" -lt 50 ]; do\n\
    pids=\"$(stale_app_pids)\"\n\
    [ -z \"$pids\" ] && return 0\n\
    sleep 0.1\n\
    i=$((i + 1))\n\
  done\n\
  kill -KILL $pids 2>/dev/null || true\n\
  i=0\n\
  while [ \"$i\" -lt 20 ]; do\n\
    pids=\"$(stale_app_pids)\"\n\
    [ -z \"$pids\" ] && return 0\n\
    sleep 0.1\n\
    i=$((i + 1))\n\
  done\n\
  echo \"stale app process for $main_exe is still running from a superseded install directory\" >&2\n\
  return 1\n\
}}\n\
\n\
kill_matching \"$install_root/$main_exe\"\n\
kill_matching \"$install_root/app-\"\n\
kill_matching \"$install_root/app/\"\n\
terminate_stale_app_processes\n\
rm -rf \"$next_app_dir\" \"$previous_app_dir\"\n\
if [ ! -d \"$stage_dir/app\" ]; then\n\
  echo \"Remote install stage is missing app payload\" >&2\n\
  exit 1\n\
fi\n\
mv \"$stage_dir/app\" \"$next_app_dir\"\n\
if [ -d \"$active_app_dir\" ]; then\n\
  mv \"$active_app_dir\" \"$previous_app_dir\"\n\
fi\n\
mv \"$next_app_dir\" \"$active_app_dir\"\n\
terminate_stale_app_processes\n\
\n\
if [ -n \"${{legacy_app_dir:-}}\" ] && [ -d \"$legacy_app_dir\" ] && [ ! -d \"$previous_app_dir\" ]; then\n\
  persistent_source_dir=\"$legacy_app_dir\"\n\
elif [ -d \"$previous_app_dir\" ]; then\n\
  persistent_source_dir=\"$previous_app_dir\"\n\
else\n\
  persistent_source_dir=\"\"\n\
fi\n\
\n\
if [ -n \"${{persistent_source_dir:-}}\" ]; then\n\
  if [ -f \"$active_runtime_manifest\" ]; then\n\
    cp \"$active_runtime_manifest\" \"$runtime_manifest_backup\"\n\
  fi\n\
  if [ -f \"$active_legacy_runtime_manifest\" ]; then\n\
    cp \"$active_legacy_runtime_manifest\" \"$legacy_runtime_manifest_backup\"\n\
  fi\n\
{persistent_asset_commands}\
  if [ -f \"$runtime_manifest_backup\" ]; then\n\
    mkdir -p \"$(dirname \"$active_runtime_manifest\")\"\n\
    cp \"$runtime_manifest_backup\" \"$active_runtime_manifest\"\n\
  fi\n\
  if [ -f \"$legacy_runtime_manifest_backup\" ]; then\n\
    mkdir -p \"$(dirname \"$active_legacy_runtime_manifest\")\"\n\
    cp \"$legacy_runtime_manifest_backup\" \"$active_legacy_runtime_manifest\"\n\
  fi\n\
fi\n\
\n\
rm -rf \"$previous_app_dir\"\n\
\n\
for snapshot_dir in \"$install_root\"/app-[0-9]*; do\n\
  [ -d \"$snapshot_dir\" ] || continue\n\
  rm -rf \"$snapshot_dir\"\n\
done\n\
\n\
if [ -d \"$stage_dir/shortcuts/applications\" ]; then\n\
  mkdir -p \"$applications_dir\"\n\
  cp \"$stage_dir/shortcuts/applications/\"*.desktop \"$applications_dir/\" 2>/dev/null || true\n\
  chmod +x \"$applications_dir/\"*.desktop 2>/dev/null || true\n\
fi\n\
if [ -d \"$stage_dir/shortcuts/autostart\" ]; then\n\
  mkdir -p \"$autostart_dir\"\n\
  cp \"$stage_dir/shortcuts/autostart/\"*.desktop \"$autostart_dir/\" 2>/dev/null || true\n\
  chmod +x \"$autostart_dir/\"*.desktop 2>/dev/null || true\n\
fi\n\
rm -rf \"$stage_dir\"\n\
{exports}\
if [ ! -x \"$active_app_dir/$main_exe\" ] && [ -f \"$active_app_dir/$main_exe\" ]; then\n\
  chmod +x \"$active_app_dir/$main_exe\" || true\n\
fi\n"
    );

    if !no_start {
        script.push_str(
            "cd \"$install_root\"\n\
if [ -n \"$version\" ]; then\n\
  \"$active_app_dir/$main_exe\" --surge-installed \"$version\" >/dev/null 2>&1 || true\n\
  nohup \"$active_app_dir/$main_exe\" --surge-first-run \"$version\" >/dev/null 2>&1 &\n\
else\n\
  \"$active_app_dir/$main_exe\" --surge-installed >/dev/null 2>&1 || true\n\
  nohup \"$active_app_dir/$main_exe\" --surge-first-run >/dev/null 2>&1 &\n\
fi\n",
        );
    }

    Ok(script)
}
