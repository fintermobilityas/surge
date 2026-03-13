use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_yaml::{Mapping, Value};

use crate::formatters::{format_bytes, format_duration};
use crate::logline;
use surge_core::config::manifest::{PackDeltaStrategy, SurgeManifest};
use surge_core::error::{Result, SurgeError};
use surge_core::pack::builder::PackBuilder;
use surge_core::platform::fs::write_file_atomic;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TuneCandidate {
    delta_strategy: PackDeltaStrategy,
    compression_level: i32,
}

#[derive(Debug, Clone)]
struct PackTuneResult {
    candidate: TuneCandidate,
    duration: Duration,
    full_size: u64,
    delta_size: Option<u64>,
}

impl PackTuneResult {
    fn total_size(&self) -> u64 {
        self.full_size.saturating_add(self.delta_size.unwrap_or(0))
    }
}

pub async fn execute_pack(
    manifest_path: &Path,
    app_id: Option<&str>,
    version: &str,
    rid: Option<&str>,
    artifacts_dir: Option<&Path>,
    zstd_levels: &[i32],
    delta_strategies: &[String],
    write_manifest: bool,
) -> Result<()> {
    let raw_manifest = std::fs::read(manifest_path)?;
    let manifest = SurgeManifest::parse(&raw_manifest)?;
    let app_id = super::resolve_app_id_with_rid_hint(&manifest, app_id, rid)?;
    let rid = super::resolve_rid(&manifest, &app_id, rid)?;
    let artifacts_dir = artifacts_dir.map_or_else(
        || super::pack::default_artifacts_dir(manifest_path, &app_id, &rid, version),
        PathBuf::from,
    );

    if !artifacts_dir.is_dir() {
        return Err(SurgeError::Pack(format!(
            "Artifacts directory does not exist: {}. Use --artifacts-dir to override.",
            artifacts_dir.display(),
        )));
    }

    let candidates = build_candidates(zstd_levels, delta_strategies)?;
    if candidates.is_empty() {
        return Err(SurgeError::Config(
            "No tune candidates were generated. Provide at least one zstd level.".to_string(),
        ));
    }

    logline::title("Pack Tune");
    logline::info(&format!(
        "Target: {app_id}/{rid} v{version} | Artifacts: {}",
        artifacts_dir.display()
    ));

    let mut results = Vec::with_capacity(candidates.len());
    for candidate in &candidates {
        logline::subtle(&format!(
            "Benchmarking strategy={} zstd={}",
            candidate.delta_strategy.as_str(),
            candidate.compression_level
        ));
        let result = benchmark_candidate(
            manifest_path,
            &raw_manifest,
            &app_id,
            version,
            &rid,
            &artifacts_dir,
            *candidate,
        )
        .await?;
        logline::plain(&format!(
            "  {:24} zstd={:<2} build {:>7} | full {:>8} | delta {:>8}",
            result.candidate.delta_strategy.as_str(),
            result.candidate.compression_level,
            format_duration(result.duration),
            format_bytes(result.full_size),
            result.delta_size.map_or_else(|| "-".to_string(), format_bytes),
        ));
        results.push(result);
    }

    let recommended =
        choose_recommended(&results).ok_or_else(|| SurgeError::Pack("No tune results were produced".to_string()))?;
    logline::success(&format!(
        "Recommended: strategy={} zstd={} (build {}, total artifacts {})",
        recommended.candidate.delta_strategy.as_str(),
        recommended.candidate.compression_level,
        format_duration(recommended.duration),
        format_bytes(recommended.total_size())
    ));

    if write_manifest {
        let yaml = build_manifest_with_policy(
            &raw_manifest,
            recommended.candidate.delta_strategy,
            recommended.candidate.compression_level,
        )?;
        write_file_atomic(manifest_path, &yaml)?;
        logline::success(&format!(
            "Updated {} with pack.delta.strategy={} and pack.compression.level={}",
            manifest_path.display(),
            recommended.candidate.delta_strategy.as_str(),
            recommended.candidate.compression_level
        ));
    }

    Ok(())
}

fn build_candidates(zstd_levels: &[i32], delta_strategies: &[String]) -> Result<Vec<TuneCandidate>> {
    let mut levels = zstd_levels.to_vec();
    levels.sort_unstable();
    levels.dedup();

    if let Some(level) = levels.iter().copied().find(|level| !(1..=22).contains(level)) {
        return Err(SurgeError::Config(format!(
            "Unsupported zstd level {level}. Supported values are 1 through 22."
        )));
    }

    let mut strategies = Vec::new();
    for raw in delta_strategies {
        let strategy = PackDeltaStrategy::parse(raw).ok_or_else(|| {
            SurgeError::Config(format!(
                "Unsupported delta strategy '{raw}'. Supported values: archive-chunked-bsdiff, archive-bsdiff"
            ))
        })?;
        if !strategies.contains(&strategy) {
            strategies.push(strategy);
        }
    }

    let mut candidates = Vec::new();
    for strategy in strategies {
        for level in &levels {
            candidates.push(TuneCandidate {
                delta_strategy: strategy,
                compression_level: *level,
            });
        }
    }

    Ok(candidates)
}

async fn benchmark_candidate(
    manifest_path: &Path,
    raw_manifest: &[u8],
    app_id: &str,
    version: &str,
    rid: &str,
    artifacts_dir: &Path,
    candidate: TuneCandidate,
) -> Result<PackTuneResult> {
    let tempdir = tempfile::tempdir()?;
    let candidate_manifest_path = tempdir.path().join("surge.yml");
    let candidate_manifest_yaml =
        build_manifest_with_policy(raw_manifest, candidate.delta_strategy, candidate.compression_level)?;
    write_file_atomic(&candidate_manifest_path, &candidate_manifest_yaml)?;
    let candidate_manifest = SurgeManifest::parse(&candidate_manifest_yaml)?;
    let ctx = Arc::new(configure_benchmark_context(manifest_path, &candidate_manifest, app_id)?);

    let manifest_path = candidate_manifest_path.to_str().ok_or_else(|| {
        SurgeError::Config(format!(
            "Manifest path is not valid UTF-8: {}",
            candidate_manifest_path.display()
        ))
    })?;
    let artifacts_dir = artifacts_dir.to_str().ok_or_else(|| {
        SurgeError::Config(format!(
            "Artifacts directory is not valid UTF-8: {}",
            artifacts_dir.display()
        ))
    })?;

    let mut builder = PackBuilder::new(ctx, manifest_path, app_id, rid, version, artifacts_dir)?;
    let started = Instant::now();
    builder.build(None).await?;
    let duration = started.elapsed();

    let full_size = builder
        .artifacts()
        .iter()
        .find(|artifact| !artifact.is_delta)
        .map_or(0, |artifact| u64::try_from(artifact.size).ok().unwrap_or(0));
    let delta_size = builder
        .artifacts()
        .iter()
        .find(|artifact| artifact.is_delta)
        .and_then(|artifact| u64::try_from(artifact.size).ok());

    Ok(PackTuneResult {
        candidate,
        duration,
        full_size,
        delta_size,
    })
}

fn configure_benchmark_context(
    manifest_path: &Path,
    manifest: &SurgeManifest,
    app_id: &str,
) -> Result<surge_core::context::Context> {
    super::pack::configure_context(manifest_path, manifest, app_id)
}

fn choose_recommended(results: &[PackTuneResult]) -> Option<&PackTuneResult> {
    let smallest_total = results.iter().map(PackTuneResult::total_size).min()?;
    let size_budget = smallest_total.saturating_add((smallest_total.saturating_mul(5)) / 100);

    results
        .iter()
        .filter(|result| result.total_size() <= size_budget)
        .min_by_key(|result| (result.duration, result.total_size(), result.candidate.compression_level))
        .or_else(|| {
            results
                .iter()
                .min_by_key(|result| (result.duration, result.total_size()))
        })
}

fn build_manifest_with_policy(
    raw_manifest: &[u8],
    delta_strategy: PackDeltaStrategy,
    compression_level: i32,
) -> Result<Vec<u8>> {
    let mut root: Value = serde_yaml::from_slice(raw_manifest)?;
    set_pack_policy(&mut root, delta_strategy, compression_level)?;
    let yaml = serde_yaml::to_string(&root)?.into_bytes();
    SurgeManifest::parse(&yaml)?;
    Ok(yaml)
}

fn set_pack_policy(root: &mut Value, delta_strategy: PackDeltaStrategy, compression_level: i32) -> Result<()> {
    let root_map = root
        .as_mapping_mut()
        .ok_or_else(|| SurgeError::Config("Manifest root must be a mapping".to_string()))?;
    let pack_map = ensure_child_mapping(root_map, "pack")?;
    let delta_map = ensure_child_mapping(pack_map, "delta")?;
    set_value(
        delta_map,
        "strategy",
        Value::String(delta_strategy.as_str().to_string()),
    );
    let compression_map = ensure_child_mapping(pack_map, "compression")?;
    set_value(compression_map, "format", Value::String("zstd".to_string()));
    set_value(
        compression_map,
        "level",
        serde_yaml::to_value(compression_level).map_err(|e| SurgeError::Config(format!("{e}")))?,
    );
    Ok(())
}

fn ensure_child_mapping<'a>(mapping: &'a mut Mapping, key: &str) -> Result<&'a mut Mapping> {
    let key_value = Value::String(key.to_string());
    let value = mapping
        .entry(key_value)
        .or_insert_with(|| Value::Mapping(Mapping::new()));
    value
        .as_mapping_mut()
        .ok_or_else(|| SurgeError::Config(format!("Manifest field '{key}' must be a mapping")))
}

fn set_value(mapping: &mut Mapping, key: &str, value: Value) {
    mapping.insert(Value::String(key.to_string()), value);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use surge_core::platform::detect::current_rid;
    use surge_core::platform::fs::make_executable;

    #[test]
    fn choose_recommended_prefers_fast_candidate_within_size_budget() {
        let slow_small = PackTuneResult {
            candidate: TuneCandidate {
                delta_strategy: PackDeltaStrategy::ArchiveChunkedBsdiff,
                compression_level: 5,
            },
            duration: Duration::from_secs(10),
            full_size: 100,
            delta_size: Some(10),
        };
        let fast_close = PackTuneResult {
            candidate: TuneCandidate {
                delta_strategy: PackDeltaStrategy::ArchiveChunkedBsdiff,
                compression_level: 3,
            },
            duration: Duration::from_secs(8),
            full_size: 103,
            delta_size: Some(12),
        };
        let fast_large = PackTuneResult {
            candidate: TuneCandidate {
                delta_strategy: PackDeltaStrategy::ArchiveChunkedBsdiff,
                compression_level: 1,
            },
            duration: Duration::from_secs(7),
            full_size: 140,
            delta_size: Some(20),
        };

        let results = [slow_small, fast_close, fast_large];
        let best = choose_recommended(&results).expect("best candidate");
        assert_eq!(best.candidate.compression_level, 3);
    }

    #[test]
    fn build_manifest_with_policy_updates_pack_block() {
        let yaml = br"schema: 1
storage:
  provider: filesystem
  bucket: /tmp/store
apps:
  - id: demoapp
    target:
      rid: linux-x64
";

        let rendered =
            build_manifest_with_policy(yaml, PackDeltaStrategy::ArchiveBsdiff, 5).expect("manifest should render");
        let rendered = String::from_utf8(rendered).expect("utf-8");
        assert!(rendered.contains("strategy: archive-bsdiff"));
        assert!(rendered.contains("level: 5"));
    }

    #[test]
    fn configure_benchmark_context_uses_original_manifest_scope_for_env_lookup() {
        let manifest_scope = Path::new("/tmp/demo/surge.yml");
        crate::envfile::with_storage_env_state_for_test(
            manifest_scope,
            BTreeMap::new(),
            BTreeMap::from([(
                "demoapp".to_string(),
                BTreeMap::from([
                    ("AZURE_STORAGE_ACCOUNT_NAME".to_string(), "overlay-account".to_string()),
                    ("AZURE_STORAGE_ACCOUNT_KEY".to_string(), "overlay-key".to_string()),
                ]),
            )]),
            || {
                let manifest_yaml = br"schema: 1
storage:
  provider: azure
  bucket: demo
apps:
  - id: demoapp
    target:
      rid: linux-x64
";
                let manifest = SurgeManifest::parse(manifest_yaml).expect("manifest should parse");
                let ctx = configure_benchmark_context(manifest_scope, &manifest, "demoapp")
                    .expect("context should use original manifest scope");
                let storage_config = ctx.storage_config();

                assert_eq!(storage_config.access_key, "overlay-account");
                assert_eq!(storage_config.secret_key, "overlay-key");
            },
        );
    }

    #[tokio::test]
    async fn execute_pack_writes_recommended_policy_to_manifest() {
        let temp_dir = tempfile::tempdir().expect("temp dir should be created");
        let store_dir = temp_dir.path().join("store");
        let artifacts_dir = temp_dir.path().join("artifacts");
        let manifest_path = temp_dir.path().join("surge.yml");
        let app_id = "demoapp";
        let rid = current_rid();
        let version = "1.0.0";

        std::fs::create_dir_all(&store_dir).expect("store dir should be created");
        std::fs::create_dir_all(&artifacts_dir).expect("artifacts dir should be created");
        std::fs::write(
            &manifest_path,
            format!(
                "schema: 1\nstorage:\n  provider: filesystem\n  bucket: {}\napps:\n  - id: {app_id}\n    main: demoapp\n    installDirectory: demoapp\n    target:\n      rid: {rid}\n",
                store_dir.display()
            ),
        )
        .expect("manifest should be written");

        std::fs::write(artifacts_dir.join("demoapp"), b"#!/bin/sh\necho tuned\n")
            .expect("main executable should be written");
        make_executable(&artifacts_dir.join("demoapp")).expect("main executable should be executable");
        std::fs::write(artifacts_dir.join("payload.txt"), b"payload").expect("payload should be written");

        execute_pack(
            &manifest_path,
            Some(app_id),
            version,
            Some(&rid),
            Some(&artifacts_dir),
            &[3],
            &[PackDeltaStrategy::ArchiveChunkedBsdiff.as_str().to_string()],
            true,
        )
        .await
        .expect("tune pack should succeed");

        let rendered = std::fs::read_to_string(&manifest_path).expect("rendered manifest");
        assert!(rendered.contains("strategy: archive-chunked-bsdiff"));
        assert!(rendered.contains("level: 3"));
    }
}
