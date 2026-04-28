use super::types::{
    AppConfig, InstallArtifactCachePolicy, PackCompressionFormat, PackDeltaStrategy, PackPolicy, SurgeManifest,
    TargetConfig,
};
use super::validate::canonicalize_installers;

impl SurgeManifest {
    #[must_use]
    pub fn effective_pack_policy(&self) -> PackPolicy {
        let mut policy = PackPolicy::default();

        if let Some(pack) = &self.pack {
            if let Some(delta) = &pack.delta {
                if let Some(strategy) = delta.strategy.as_deref().and_then(PackDeltaStrategy::parse) {
                    policy.delta_strategy = strategy;
                }
                if let Some(max_chain_length) = delta.max_chain_length {
                    policy.max_chain_length = max_chain_length;
                }
            }

            if let Some(compression) = &pack.compression {
                if let Some(format) = compression.format.as_deref().and_then(PackCompressionFormat::parse) {
                    policy.compression_format = format;
                }
                if let Some(level) = compression.level {
                    policy.compression_level = level;
                }
            }

            if let Some(retention) = &pack.retention {
                if let Some(keep_latest_fulls) = retention.keep_latest_fulls {
                    policy.keep_latest_fulls = keep_latest_fulls;
                }
                if let Some(checkpoint_every) = retention.checkpoint_every {
                    policy.checkpoint_every = checkpoint_every;
                }
            }
        }

        policy
    }

    #[must_use]
    pub fn effective_install_artifact_cache_policy(&self) -> InstallArtifactCachePolicy {
        self.cache.map_or_else(InstallArtifactCachePolicy::default, |cache| {
            cache.effective_install_artifact_cache_policy()
        })
    }

    pub fn find_app(&self, app_id: &str) -> Option<&AppConfig> {
        self.apps.iter().find(|app| app.id == app_id)
    }

    pub fn find_app_with_target(&self, app_id: &str, rid: &str) -> Option<(&AppConfig, TargetConfig)> {
        self.apps
            .iter()
            .filter(|app| app.id == app_id)
            .find_map(|app| app.find_target(rid).map(|target| (app, app.resolve_target(target))))
    }

    pub fn find_target(&self, app_id: &str, rid: &str) -> Option<TargetConfig> {
        self.find_app_with_target(app_id, rid).map(|(_, target)| target)
    }

    #[must_use]
    pub fn app_ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.apps.iter().map(|app| app.id.clone()).collect();
        ids.sort();
        ids.dedup();
        ids
    }

    #[must_use]
    pub fn target_rids(&self, app_id: &str) -> Vec<String> {
        let mut rids: Vec<String> = self
            .apps
            .iter()
            .filter(|app| app.id == app_id)
            .flat_map(|app| app.iter_targets().map(|target| target.rid.clone()))
            .filter(|rid| !rid.is_empty())
            .collect();
        rids.sort();
        rids.dedup();
        rids
    }
}

impl AppConfig {
    pub(super) fn iter_targets(&self) -> impl Iterator<Item = &TargetConfig> {
        self.target.iter().chain(self.targets.iter())
    }

    pub(super) fn find_target(&self, rid: &str) -> Option<&TargetConfig> {
        self.iter_targets().find(|target| target.rid == rid)
    }

    #[must_use]
    pub fn effective_name(&self) -> String {
        if !self.name.trim().is_empty() {
            return self.name.clone();
        }
        if !self.main_exe.trim().is_empty() {
            return self.main_exe.clone();
        }
        self.id.clone()
    }

    #[must_use]
    pub fn effective_main_exe(&self) -> String {
        if self.main_exe.trim().is_empty() {
            self.id.clone()
        } else {
            self.main_exe.clone()
        }
    }

    #[must_use]
    pub fn effective_install_directory(&self) -> String {
        if !self.install_directory.trim().is_empty() {
            return self.install_directory.clone();
        }
        if !self.main_exe.trim().is_empty() {
            return self.main_exe.clone();
        }
        self.id.clone()
    }

    #[must_use]
    pub fn resolve_target(&self, target: &TargetConfig) -> TargetConfig {
        let mut resolved = target.clone();
        if resolved.os.is_empty() {
            resolved.os.clone_from(&self.os);
        }
        if resolved.icon.is_empty() {
            resolved.icon.clone_from(&self.icon);
        }
        if resolved.shortcuts.is_empty() {
            resolved.shortcuts.clone_from(&self.shortcuts);
        }
        if resolved.persistent_assets.is_empty() {
            resolved.persistent_assets.clone_from(&self.persistent_assets);
        }
        if resolved.installers.is_empty() {
            resolved.installers.clone_from(&self.installers);
        }
        resolved.installers = canonicalize_installers(&resolved.installers);
        if !self.environment.is_empty() {
            let mut merged = self.environment.clone();
            for (key, value) in &resolved.environment {
                merged.insert(key.clone(), value.clone());
            }
            resolved.environment = merged;
        }
        resolved
    }
}
