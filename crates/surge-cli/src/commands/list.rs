use std::path::Path;

use crate::ui::UiTheme;
use chrono::{DateTime, Utc};
use surge_core::config::constants::RELEASES_FILE_COMPRESSED;
use surge_core::config::manifest::SurgeManifest;
use surge_core::context::StorageConfig;
use surge_core::error::{Result, SurgeError};
use surge_core::releases::manifest::{ReleaseEntry, ReleaseIndex, decompress_release_index};
use surge_core::releases::version::compare_versions;
use surge_core::storage::{self};

#[derive(Debug, Clone)]
struct OverviewRow {
    app_id: String,
    rid: String,
    total: String,
    latest: String,
    published: String,
    channel_statuses: Vec<String>,
}

/// List release status with overview semantics.
///
/// - With a single-app manifest (or `--app-id`), shows target/rid status rows.
/// - With a multi-app manifest and no `--app-id`, shows a full app overview table.
pub async fn execute(
    manifest_path: &Path,
    app_id: Option<&str>,
    rid: Option<&str>,
    channel: Option<&str>,
) -> Result<()> {
    let manifest = SurgeManifest::from_file(manifest_path)?;

    let requested_app_id = app_id.map(str::trim).filter(|value| !value.is_empty());
    let requested_rid = rid.map(str::trim).filter(|value| !value.is_empty());
    let requested_channel = channel.map(str::trim).filter(|value| !value.is_empty());

    let app_ids = if let Some(app) = requested_app_id {
        vec![app.to_string()]
    } else {
        manifest.app_ids()
    };

    if app_ids.is_empty() {
        return Err(SurgeError::Config(
            "Manifest has no apps. Cannot list releases.".to_string(),
        ));
    }

    if requested_rid.is_some() && requested_app_id.is_none() && app_ids.len() > 1 {
        return Err(SurgeError::Config(
            "--rid requires --app-id when manifest contains multiple apps".to_string(),
        ));
    }

    let channel_headers = if let Some(ch) = requested_channel {
        vec![ch.to_string()]
    } else {
        default_channel_headers(&manifest)
    };

    let mut rows = Vec::new();

    for app in &app_ids {
        let mut target_rids = if let Some(requested) = requested_rid {
            vec![requested.to_string()]
        } else {
            let rids = manifest.target_rids(app);
            if rids.is_empty() { vec![String::new()] } else { rids }
        };

        target_rids.sort();
        target_rids.dedup();

        let index = fetch_release_index_for_app(&manifest, app).await?;
        for target_rid in target_rids {
            rows.push(build_overview_row(app, &target_rid, index.as_ref(), &channel_headers));
        }
    }

    rows.sort_by(|a, b| a.app_id.cmp(&b.app_id).then_with(|| a.rid.cmp(&b.rid)));
    print_overview_table(&channel_headers, &rows);

    Ok(())
}

fn default_channel_headers(manifest: &SurgeManifest) -> Vec<String> {
    let mut channels = Vec::new();
    for channel in &manifest.channels {
        if !channel.name.trim().is_empty() && !channels.iter().any(|c| c == &channel.name) {
            channels.push(channel.name.clone());
        }
    }

    if channels.is_empty() {
        channels.push("stable".to_string());
    }

    channels
}

fn build_overview_row(
    app_id: &str,
    rid: &str,
    index: Option<&ReleaseIndex>,
    channel_headers: &[String],
) -> OverviewRow {
    let Some(index) = index else {
        return empty_overview_row(app_id, rid, channel_headers.len());
    };

    if !index.app_id.is_empty() && index.app_id != app_id {
        return empty_overview_row(app_id, rid, channel_headers.len());
    }

    let relevant: Vec<&ReleaseEntry> = index
        .releases
        .iter()
        .filter(|release| release_matches_rid(release, rid))
        .collect();

    if relevant.is_empty() {
        return empty_overview_row(app_id, rid, channel_headers.len());
    }

    let latest_release = relevant.iter().max_by(|a, b| compare_versions(&a.version, &b.version));

    let latest = latest_release
        .map(|release| release.version.clone())
        .unwrap_or_else(|| "-".to_string());

    let published_raw = latest_release
        .and_then(|release| {
            let created = release.created_utc.trim();
            if created.is_empty() {
                None
            } else {
                Some(created.to_string())
            }
        })
        .or_else(|| {
            let last_write = index.last_write_utc.trim();
            if last_write.is_empty() {
                None
            } else {
                Some(last_write.to_string())
            }
        })
        .unwrap_or_else(|| "-".to_string());
    let published = humanize_publish_timestamp(&published_raw);

    let mut channel_statuses = Vec::with_capacity(channel_headers.len());
    for channel in channel_headers {
        let latest_for_channel = latest_release_for_channel(&relevant, channel);
        channel_statuses.push(
            latest_for_channel
                .map(format_release_cell)
                .unwrap_or_else(|| "-".to_string()),
        );
    }

    OverviewRow {
        app_id: app_id.to_string(),
        rid: display_rid(rid),
        total: relevant.len().to_string(),
        latest,
        published,
        channel_statuses,
    }
}

fn empty_overview_row(app_id: &str, rid: &str, channel_count: usize) -> OverviewRow {
    OverviewRow {
        app_id: app_id.to_string(),
        rid: display_rid(rid),
        total: "0".to_string(),
        latest: "-".to_string(),
        published: "-".to_string(),
        channel_statuses: vec!["-".to_string(); channel_count],
    }
}

fn display_rid(rid: &str) -> String {
    let rid = rid.trim();
    if rid.is_empty() {
        "<generic>".to_string()
    } else {
        rid.to_string()
    }
}

fn release_matches_rid(release: &ReleaseEntry, rid: &str) -> bool {
    let rid = rid.trim();
    let release_rid = release.rid.trim();

    if rid.is_empty() {
        return release_rid.is_empty();
    }

    release_rid.is_empty() || release_rid == rid
}

fn latest_release_for_channel<'a>(releases: &'a [&ReleaseEntry], channel: &str) -> Option<&'a ReleaseEntry> {
    releases
        .iter()
        .copied()
        .filter(|release| release.channels.iter().any(|existing| existing == channel))
        .max_by(|a, b| compare_versions(&a.version, &b.version))
}

fn format_release_cell(release: &ReleaseEntry) -> String {
    let has_delta = !release.delta_filename.trim().is_empty() && release.delta_size > 0;
    if has_delta {
        format!("{} (delta {})", release.version, format_bytes(release.delta_size))
    } else {
        format!("{} (full {})", release.version, format_bytes(release.full_size))
    }
}

fn format_bytes(bytes: i64) -> String {
    if bytes < 0 {
        return "-".to_string();
    }

    let units = ["B", "KB", "MB", "GB", "TB"];
    let mut unit_idx = 0usize;
    let mut value = bytes as f64;

    while value >= 1024.0 && unit_idx < units.len() - 1 {
        value /= 1024.0;
        unit_idx += 1;
    }

    if unit_idx == 0 {
        format!("{}{}", bytes, units[unit_idx])
    } else {
        format!("{value:.1}{}", units[unit_idx])
    }
}

fn humanize_publish_timestamp(raw: &str) -> String {
    humanize_publish_timestamp_at(raw, Utc::now())
}

fn humanize_publish_timestamp_at(raw: &str, now: DateTime<Utc>) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed == "-" {
        return "-".to_string();
    }

    let parsed = match DateTime::parse_from_rfc3339(trimmed) {
        Ok(ts) => ts.with_timezone(&Utc),
        Err(_) => return trimmed.to_string(),
    };

    let absolute = parsed.format("%Y-%m-%d %H:%M UTC");
    let relative = humanize_relative_delta(parsed, now);
    format!("{absolute} ({relative})")
}

fn humanize_relative_delta(published: DateTime<Utc>, now: DateTime<Utc>) -> String {
    let is_future = published > now;
    let seconds = if is_future {
        (published - now).num_seconds()
    } else {
        (now - published).num_seconds()
    };

    let amount = seconds.max(0);

    if amount < 60 {
        return if is_future {
            "in <1m".to_string()
        } else {
            "just now".to_string()
        };
    }

    let (value, unit) = if amount < 3_600 {
        (amount / 60, "m")
    } else if amount < 86_400 {
        (amount / 3_600, "h")
    } else if amount < 604_800 {
        (amount / 86_400, "d")
    } else if amount < 2_592_000 {
        (amount / 604_800, "w")
    } else if amount < 31_536_000 {
        (amount / 2_592_000, "mo")
    } else {
        (amount / 31_536_000, "y")
    };

    if is_future {
        format!("in {value}{unit}")
    } else {
        format!("{value}{unit} ago")
    }
}

fn print_overview_table(channel_headers: &[String], rows: &[OverviewRow]) {
    let theme = UiTheme::global();
    if rows.is_empty() {
        println!("{}", theme.warning("No releases found."));
        return;
    }

    let mut headers = vec![
        "app".to_string(),
        "rid".to_string(),
        "total".to_string(),
        "latest".to_string(),
        "published".to_string(),
    ];
    headers.extend(channel_headers.iter().cloned());

    let mut table_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let mut cells = vec![
            row.app_id.clone(),
            row.rid.clone(),
            row.total.clone(),
            row.latest.clone(),
            row.published.clone(),
        ];
        cells.extend(row.channel_statuses.clone());
        table_rows.push(cells);
    }

    let mut widths: Vec<usize> = headers.iter().map(String::len).collect();
    for row in &table_rows {
        for (idx, cell) in row.iter().enumerate() {
            widths[idx] = widths[idx].max(cell.len());
        }
    }

    let title = format!("Status overview ({} row(s))", table_rows.len());
    println!("{}", theme.title(&title));
    println!();

    print_table_header_row(&headers, &widths, theme);
    print_table_separator(&widths, theme);
    for row in &table_rows {
        print_table_row(row, &widths, theme);
    }
}

fn print_table_header_row(cells: &[String], widths: &[usize], theme: UiTheme) {
    let line = cells
        .iter()
        .enumerate()
        .map(|(idx, cell)| format!("{cell:<width$}", width = widths[idx]))
        .collect::<Vec<_>>()
        .join("  ");
    println!("{}", theme.title(&line));
}

fn print_table_row(cells: &[String], widths: &[usize], theme: UiTheme) {
    let mut styled = Vec::with_capacity(cells.len());
    for (idx, cell) in cells.iter().enumerate() {
        let padded = format!("{cell:<width$}", width = widths[idx]);
        styled.push(style_data_cell(idx, cell, &padded, theme));
    }
    println!("{}", styled.join("  "));
}

fn style_data_cell(idx: usize, value: &str, padded: &str, theme: UiTheme) -> String {
    match idx {
        0 => theme.bold(&theme.blue(padded)),
        1 => {
            if value == "<generic>" {
                theme.magenta(padded)
            } else {
                padded.to_string()
            }
        }
        2 => theme.dim(padded),
        3 => {
            if value == "-" {
                theme.dim(padded)
            } else {
                theme.green(padded)
            }
        }
        4 => theme.dim(padded),
        _ => style_channel_cell(value, padded, theme),
    }
}

fn style_channel_cell(value: &str, padded: &str, theme: UiTheme) -> String {
    if value == "-" {
        return theme.dim(padded);
    }

    if value.contains("(delta ") {
        return theme.yellow(padded);
    }

    theme.green(padded)
}

fn print_table_separator(widths: &[usize], theme: UiTheme) {
    let line = widths
        .iter()
        .map(|width| "-".repeat(*width))
        .collect::<Vec<_>>()
        .join("  ");
    println!("{}", theme.subtle(&line));
}

async fn fetch_release_index_for_app(manifest: &SurgeManifest, app_id: &str) -> Result<Option<ReleaseIndex>> {
    let configs = build_storage_configs_for_app(manifest, app_id)?;

    for config in configs {
        let backend = storage::create_storage_backend(&config)?;
        match backend.get_object(RELEASES_FILE_COMPRESSED).await {
            Ok(data) => {
                let index = decompress_release_index(&data)?;
                if index.app_id.is_empty() || index.app_id == app_id {
                    return Ok(Some(index));
                }
            }
            Err(SurgeError::NotFound(_)) => continue,
            Err(e) => return Err(e),
        }
    }

    Ok(None)
}

fn build_storage_configs_for_app(manifest: &SurgeManifest, app_id: &str) -> Result<Vec<StorageConfig>> {
    let base = build_storage_config(manifest)?;

    if manifest.apps.len() <= 1 {
        return Ok(vec![base]);
    }

    let mut configs = Vec::new();
    let per_app_prefix = append_prefix(&base.prefix, app_id);
    if per_app_prefix != base.prefix {
        let mut scoped = base.clone();
        scoped.prefix = per_app_prefix;
        configs.push(scoped);
    }
    configs.push(base);

    Ok(configs)
}

fn append_prefix(prefix: &str, segment: &str) -> String {
    let prefix = prefix.trim().trim_matches('/');
    let segment = segment.trim().trim_matches('/');

    if prefix.is_empty() {
        segment.to_string()
    } else if segment.is_empty() {
        prefix.to_string()
    } else {
        format!("{prefix}/{segment}")
    }
}

fn build_storage_config(manifest: &SurgeManifest) -> Result<StorageConfig> {
    super::build_storage_config(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::TimeZone;
    use surge_core::config::constants::DEFAULT_ZSTD_LEVEL;
    use surge_core::releases::manifest::compress_release_index;

    fn release(version: &str, channels: &[&str], rid: &str, full: i64, delta: i64) -> ReleaseEntry {
        ReleaseEntry {
            version: version.to_string(),
            channels: channels.iter().map(|channel| (*channel).to_string()).collect(),
            os: "linux".to_string(),
            rid: rid.to_string(),
            is_genesis: false,
            full_filename: format!("{version}-full"),
            full_size: full,
            full_sha256: String::new(),
            delta_filename: if delta > 0 {
                format!("{version}-delta")
            } else {
                String::new()
            },
            delta_size: delta,
            delta_sha256: String::new(),
            created_utc: "2026-03-03T14:00:00Z".to_string(),
            release_notes: String::new(),
            main_exe: String::new(),
            install_directory: String::new(),
            supervisor_id: String::new(),
            icon: String::new(),
            shortcuts: Vec::new(),
            persistent_assets: Vec::new(),
            installers: Vec::new(),
            environment: std::collections::BTreeMap::new(),
        }
    }

    #[test]
    fn append_prefix_handles_empty_values() {
        assert_eq!(append_prefix("", "app-a"), "app-a");
        assert_eq!(append_prefix("/releases/", "/app-a/"), "releases/app-a");
        assert_eq!(append_prefix("releases", ""), "releases");
    }

    #[test]
    fn build_overview_row_picks_latest_per_channel() {
        let index = ReleaseIndex {
            app_id: "app-a".to_string(),
            last_write_utc: "2026-03-03T14:59:00Z".to_string(),
            releases: vec![
                release("1.0.0", &["test"], "linux-x64", 100, 0),
                release("1.1.0", &["test"], "linux-x64", 120, 50),
                release("1.0.5", &["production"], "linux-x64", 200, 0),
            ],
            ..ReleaseIndex::default()
        };

        let row = build_overview_row(
            "app-a",
            "linux-x64",
            Some(&index),
            &["test".to_string(), "production".to_string()],
        );

        assert_eq!(row.app_id, "app-a");
        assert_eq!(row.rid, "linux-x64");
        assert_eq!(row.total, "3");
        assert_eq!(row.latest, "1.1.0");
        assert!(row.published.starts_with("2026-03-03 14:00 UTC ("));
        assert!(row.published.ends_with(')'));
        assert!(row.channel_statuses[0].contains("1.1.0"));
        assert!(row.channel_statuses[0].contains("delta"));
        assert!(row.channel_statuses[1].contains("1.0.5"));
    }

    #[test]
    fn humanize_publish_timestamp_formats_date_and_relative_age() {
        let now = Utc
            .with_ymd_and_hms(2026, 3, 3, 15, 0, 0)
            .single()
            .expect("valid datetime");
        let formatted = humanize_publish_timestamp_at("2026-03-03T14:55:00Z", now);
        assert_eq!(formatted, "2026-03-03 14:55 UTC (5m ago)");
    }

    #[test]
    fn humanize_publish_timestamp_keeps_invalid_values() {
        let now = Utc
            .with_ymd_and_hms(2026, 3, 3, 15, 0, 0)
            .single()
            .expect("valid datetime");
        let formatted = humanize_publish_timestamp_at("not-a-date", now);
        assert_eq!(formatted, "not-a-date");
    }

    #[tokio::test]
    async fn fetch_release_index_for_app_prefers_scoped_prefix_on_multi_app_manifest() {
        let tmp = tempfile::tempdir().expect("temp dir should be created");
        let bucket = tmp.path().join("store");
        std::fs::create_dir_all(&bucket).expect("store directory should be created");

        let manifest_yaml = format!(
            r"schema: 1
storage:
  provider: filesystem
  bucket: {bucket}
  prefix: releases
apps:
  - id: app-a
    target:
      rid: linux-x64
  - id: app-b
    target:
      rid: linux-x64
",
            bucket = bucket.display()
        );
        let manifest = SurgeManifest::parse(manifest_yaml.as_bytes()).expect("manifest should parse");

        let scoped_index = ReleaseIndex {
            app_id: "app-a".to_string(),
            releases: vec![release("2.0.0", &["stable"], "linux-x64", 10, 0)],
            ..ReleaseIndex::default()
        };
        let scoped_data = compress_release_index(&scoped_index, DEFAULT_ZSTD_LEVEL).expect("index compression");

        let scoped_dir = bucket.join("releases").join("app-a");
        std::fs::create_dir_all(&scoped_dir).expect("scoped dir should be created");
        std::fs::write(scoped_dir.join(RELEASES_FILE_COMPRESSED), scoped_data).expect("scoped index should be written");

        let fetched = fetch_release_index_for_app(&manifest, "app-a")
            .await
            .expect("fetch should succeed")
            .expect("index should exist");
        assert_eq!(fetched.app_id, "app-a");
        assert_eq!(fetched.releases.len(), 1);
        assert_eq!(fetched.releases[0].version, "2.0.0");
    }
}
