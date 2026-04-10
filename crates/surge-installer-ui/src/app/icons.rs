use std::path::Path;

use eframe::egui;

use surge_core::config::installer::InstallerManifest;

pub(crate) fn load_window_icon(staging_dir: &Path, icon_name: &str) -> egui::IconData {
    load_app_icon(staging_dir, icon_name).unwrap_or_else(default_surge_icon)
}

pub(crate) fn load_app_logo(staging_dir: &Path, icon_name: &str) -> Option<egui::IconData> {
    Some(load_app_icon(staging_dir, icon_name).unwrap_or_else(default_surge_icon))
}

pub(crate) fn window_app_id(manifest: &InstallerManifest) -> String {
    let preferred = manifest.runtime.install_directory.trim();
    let fallback_name = manifest.runtime.name.trim();
    let fallback_id = manifest.app_id.trim();

    let raw = if !preferred.is_empty() {
        preferred
    } else if !fallback_name.is_empty() {
        fallback_name
    } else {
        fallback_id
    };

    let mut normalized = String::with_capacity(raw.len());
    for c in raw.chars() {
        if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' {
            normalized.push(c.to_ascii_lowercase());
        } else {
            normalized.push('-');
        }
    }

    let cleaned = normalized.trim_matches(['-', '.'].as_ref());
    if cleaned.is_empty() {
        "surge-installer".to_string()
    } else {
        cleaned.to_string()
    }
}

fn load_app_icon(staging_dir: &Path, icon_name: &str) -> Option<egui::IconData> {
    let trimmed = icon_name.trim();
    if trimmed.is_empty() {
        return None;
    }

    let icon_rel = Path::new(trimmed);
    let assets_dir = staging_dir.join("assets");
    let mut candidates = vec![assets_dir.join(icon_rel)];
    if let Some(file_name) = icon_rel.file_name() {
        candidates.push(assets_dir.join(file_name));
    }

    let icon_path = candidates.into_iter().find(|candidate| candidate.is_file())?;
    let bytes = std::fs::read(&icon_path).ok()?;
    decode_icon(&bytes, icon_path.extension().and_then(std::ffi::OsStr::to_str))
}

fn default_surge_icon() -> egui::IconData {
    decode_icon(include_bytes!("../../assets/logo.svg"), Some("svg")).unwrap_or_default()
}

fn decode_icon(bytes: &[u8], extension: Option<&str>) -> Option<egui::IconData> {
    if extension.is_some_and(|ext| ext.eq_ignore_ascii_case("svg")) || bytes.starts_with(b"<svg") {
        return decode_svg_icon(bytes);
    }

    let img = image::load_from_memory(bytes).ok()?;
    let rgba = img.to_rgba8();
    Some(egui::IconData {
        rgba: rgba.as_raw().clone(),
        width: rgba.width(),
        height: rgba.height(),
    })
}

fn decode_svg_icon(bytes: &[u8]) -> Option<egui::IconData> {
    let options = resvg::usvg::Options::default();
    let tree = resvg::usvg::Tree::from_data(bytes, &options).ok()?;
    let size = tree.size();

    const TARGET_SIZE: u32 = 128;
    let max_dim = size.width().max(size.height());
    if max_dim <= 0.0 {
        return None;
    }

    let scale = (TARGET_SIZE as f32) / max_dim;
    let width = (size.width() * scale).round().max(1.0) as u32;
    let height = (size.height() * scale).round().max(1.0) as u32;
    let mut pixmap = resvg::tiny_skia::Pixmap::new(width, height)?;
    let transform = resvg::tiny_skia::Transform::from_scale(scale, scale);
    resvg::render(&tree, transform, &mut pixmap.as_mut());

    Some(egui::IconData {
        rgba: pixmap.data().to_vec(),
        width,
        height,
    })
}
