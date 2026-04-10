use std::collections::HashSet;

use super::types::{AppConfig, ChannelManifestConfig, SurgeManifest, TargetConfig};

impl SurgeManifest {
    pub(super) fn normalize(&mut self) {
        let mut expanded = Vec::new();
        for app in std::mem::take(&mut self.apps) {
            let targets: Vec<TargetConfig> = app.iter_targets().cloned().collect();

            if targets.len() <= 1 {
                let mut app = app;
                if app.id.trim().is_empty()
                    && let Some(target) = targets.first()
                {
                    let base = if app.main_exe.trim().is_empty() {
                        app.name.as_str()
                    } else {
                        app.main_exe.as_str()
                    };
                    if !base.trim().is_empty() {
                        app.id = derive_target_app_id(base, target);
                    }
                }
                expanded.push(app);
                continue;
            }

            let base = if !app.id.trim().is_empty() {
                app.id.as_str()
            } else if !app.main_exe.trim().is_empty() {
                app.main_exe.as_str()
            } else {
                app.name.as_str()
            };

            let has_id = !app.id.is_empty();
            let child_name = if app.name.is_empty() && has_id {
                app.id.clone()
            } else {
                app.name.clone()
            };
            let child_main_exe = if app.main_exe.is_empty() && has_id {
                app.id.clone()
            } else {
                app.main_exe.clone()
            };
            let child_install_dir = if app.install_directory.is_empty() && has_id {
                app.id.clone()
            } else {
                app.install_directory.clone()
            };

            for target in targets {
                expanded.push(AppConfig {
                    id: derive_target_app_id(base, &target),
                    name: child_name.clone(),
                    main_exe: child_main_exe.clone(),
                    install_directory: child_install_dir.clone(),
                    supervisor_id: app.supervisor_id.clone(),
                    channels: app.channels.clone(),
                    os: app.os.clone(),
                    icon: app.icon.clone(),
                    shortcuts: app.shortcuts.clone(),
                    persistent_assets: app.persistent_assets.clone(),
                    installers: app.installers.clone(),
                    environment: app.environment.clone(),
                    targets: vec![target],
                    target: None,
                });
            }
        }
        self.apps = expanded;

        if self.channels.is_empty() {
            let mut seen = HashSet::new();
            for app in &self.apps {
                for channel in &app.channels {
                    if seen.insert(channel.clone()) {
                        self.channels.push(ChannelManifestConfig { name: channel.clone() });
                    }
                }
            }
        }
    }
}

fn derive_target_app_id(base: &str, target: &TargetConfig) -> String {
    let base = base.trim();
    let mut derived = if target.distro.trim().is_empty() {
        format!("{base}-{}", target.rid)
    } else {
        format!("{base}-{}-{}", target.distro, target.rid)
    };
    if !target.variant.trim().is_empty() {
        derived.push('-');
        derived.push_str(&target.variant);
    }
    derived
}
