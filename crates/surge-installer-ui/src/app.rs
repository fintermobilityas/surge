#![forbid(unsafe_code)]
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::items_after_statements,
    clippy::unnecessary_wraps
)]

use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};

use egui::{Align2, Color32, CornerRadius, FontId, Id, Pos2, RichText, Shape, Stroke, StrokeKind, Vec2};

use surge_core::config::installer::InstallerManifest;
use surge_core::install as core_install;
use surge_core::install::InstallProfile;

use crate::install::{self, ProgressUpdate};

// ---------------------------------------------------------------------------
// Theme
// ---------------------------------------------------------------------------

const BG: Color32 = Color32::from_rgb(13, 13, 20);
const SURFACE: Color32 = Color32::from_rgb(24, 24, 37);
const ACCENT: Color32 = Color32::from_rgb(99, 102, 241);
const ACCENT_HOVER: Color32 = Color32::from_rgb(129, 140, 248);
const ACCENT_PRESSED: Color32 = Color32::from_rgb(67, 56, 202);
const SUCCESS: Color32 = Color32::from_rgb(34, 197, 94);
const ERROR: Color32 = Color32::from_rgb(239, 68, 68);
const TEXT_PRIMARY: Color32 = Color32::from_rgb(248, 250, 252);
const TEXT_SECONDARY: Color32 = Color32::from_rgb(148, 163, 184);
const TEXT_MUTED: Color32 = Color32::from_rgb(100, 116, 139);
const PROGRESS_TRACK: Color32 = Color32::from_rgb(30, 30, 46);

pub fn configure_theme(ctx: &egui::Context) {
    let mut visuals = egui::Visuals::dark();
    visuals.panel_fill = BG;
    visuals.window_fill = BG;
    visuals.extreme_bg_color = SURFACE;
    visuals.faint_bg_color = SURFACE;
    visuals.override_text_color = Some(TEXT_PRIMARY);

    visuals.widgets.inactive.bg_fill = SURFACE;
    visuals.widgets.inactive.weak_bg_fill = SURFACE;
    visuals.widgets.inactive.fg_stroke = Stroke::new(1.0, TEXT_SECONDARY);
    visuals.widgets.inactive.corner_radius = CornerRadius::same(8);

    visuals.widgets.hovered.bg_fill = Color32::from_rgb(35, 35, 50);
    visuals.widgets.hovered.weak_bg_fill = Color32::from_rgb(35, 35, 50);
    visuals.widgets.hovered.fg_stroke = Stroke::new(1.0, TEXT_PRIMARY);
    visuals.widgets.hovered.corner_radius = CornerRadius::same(8);

    visuals.widgets.active.bg_fill = Color32::from_rgb(40, 40, 55);
    visuals.widgets.active.weak_bg_fill = Color32::from_rgb(40, 40, 55);
    visuals.widgets.active.corner_radius = CornerRadius::same(8);

    visuals.selection.bg_fill = ACCENT_PRESSED;
    visuals.selection.stroke = Stroke::new(1.0, ACCENT);

    ctx.set_visuals(visuals);
}

// ---------------------------------------------------------------------------
// Screens
// ---------------------------------------------------------------------------

enum Screen {
    Welcome,
    Installing { progress: f32, status: String },
    Complete { install_root: PathBuf },
    Error(String),
}

// ---------------------------------------------------------------------------
// App
// ---------------------------------------------------------------------------

pub struct InstallerApp {
    manifest: InstallerManifest,
    staging_dir: PathBuf,
    app_logo: Option<egui::IconData>,
    app_logo_texture: Option<egui::TextureHandle>,
    simulator: bool,
    install_error: Arc<Mutex<Option<String>>>,
    screen: Screen,
    progress_rx: Option<Receiver<ProgressUpdate>>,
}

impl InstallerApp {
    pub fn new(
        manifest: InstallerManifest,
        staging_dir: PathBuf,
        app_logo: Option<egui::IconData>,
        simulator: bool,
        install_error: Arc<Mutex<Option<String>>>,
    ) -> Self {
        Self {
            manifest,
            staging_dir,
            app_logo,
            app_logo_texture: None,
            simulator,
            install_error,
            screen: Screen::Welcome,
            progress_rx: None,
        }
    }

    fn logo_texture(&mut self, ctx: &egui::Context) -> Option<egui::TextureHandle> {
        if self.app_logo_texture.is_none() {
            let logo = self.app_logo.as_ref()?;
            let color_image =
                egui::ColorImage::from_rgba_unmultiplied([logo.width as usize, logo.height as usize], &logo.rgba);
            self.app_logo_texture =
                Some(ctx.load_texture("installer-app-logo", color_image, egui::TextureOptions::LINEAR));
        }

        self.app_logo_texture.clone()
    }

    fn draw_brand_mark(&mut self, ui: &mut egui::Ui, size: f32, pulsing: bool) {
        if let Some(texture) = self.logo_texture(ui.ctx()) {
            let mut image = egui::Image::from_texture(&texture).fit_to_exact_size(Vec2::splat(size));
            if pulsing {
                let time = ui.input(|i| i.time);
                let pulse = ((time * 1.8).sin() as f32 * 0.5 + 0.5) * 0.25 + 0.75;
                image = image.tint(Color32::WHITE.linear_multiply(pulse));
            }
            ui.add(image);
        } else if pulsing {
            let time = ui.input(|i| i.time);
            let pulse = ((time * 1.8).sin() as f32 * 0.5 + 0.5) * 0.3 + 0.7;
            draw_bolt(
                ui,
                size,
                ACCENT.linear_multiply(pulse),
                ACCENT_PRESSED.linear_multiply(pulse),
            );
        } else {
            draw_bolt(ui, size, ACCENT, ACCENT_PRESSED);
        }
    }

    fn start_install(&mut self, ctx: &egui::Context) {
        let (tx, rx): (Sender<ProgressUpdate>, Receiver<ProgressUpdate>) = channel();
        self.progress_rx = Some(rx);
        self.screen = Screen::Installing {
            progress: 0.0,
            status: "Preparing\u{2026}".to_string(),
        };

        let manifest = self.manifest.clone();
        let staging_dir = self.staging_dir.clone();
        let shortcuts = manifest.runtime.shortcuts.clone();
        let simulator = self.simulator;
        let ctx_clone = ctx.clone();

        std::thread::spawn(move || {
            install::run_install(&manifest, &staging_dir, None, &shortcuts, &tx, &ctx_clone, simulator);
        });
    }

    fn poll_progress(&mut self) {
        if let Some(rx) = &self.progress_rx {
            while let Ok(update) = rx.try_recv() {
                match update {
                    ProgressUpdate::Status(status) => {
                        if let Screen::Installing { status: ref mut s, .. } = self.screen {
                            *s = status;
                        }
                    }
                    ProgressUpdate::Progress(p) => {
                        if let Screen::Installing { ref mut progress, .. } = self.screen {
                            *progress = p;
                        }
                    }
                    ProgressUpdate::Complete(root) => {
                        *self
                            .install_error
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
                        self.screen = Screen::Complete { install_root: root };
                        self.progress_rx = None;
                        return;
                    }
                    ProgressUpdate::Error(msg) => {
                        *self
                            .install_error
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(msg.clone());
                        self.screen = Screen::Error(msg);
                        self.progress_rx = None;
                        return;
                    }
                }
            }
        }
    }

    // -- Welcome ---------------------------------------------------------------

    fn render_welcome(&mut self, ctx: &egui::Context) {
        egui::CentralPanel::default()
            .frame(egui::Frame::NONE.fill(BG))
            .show(ctx, |ui| {
                let h = ui.available_height();
                ui.vertical_centered(|ui| {
                    ui.add_space(h * 0.10);

                    self.draw_brand_mark(ui, 80.0, false);
                    ui.add_space(20.0);

                    // App name
                    ui.label(
                        RichText::new(&self.manifest.runtime.name)
                            .font(FontId::proportional(26.0))
                            .color(TEXT_PRIMARY)
                            .strong(),
                    );
                    ui.add_space(4.0);

                    // Version
                    ui.label(
                        RichText::new(format!(
                            "v{}  \u{00b7}  {}",
                            self.manifest.version, self.manifest.channel
                        ))
                        .font(FontId::proportional(13.0))
                        .color(TEXT_MUTED),
                    );

                    ui.add_space(40.0);

                    // Install button
                    if accent_button(ui, "Install", 220.0, 46.0).clicked() {
                        self.start_install(ui.ctx());
                    }

                    ui.add_space(16.0);

                    // Install location hint
                    if let Ok(path) = surge_core::platform::paths::default_install_root(
                        &self.manifest.app_id,
                        &self.manifest.runtime.install_directory,
                    ) {
                        ui.label(
                            RichText::new(format!("Installs to {}", path.display()))
                                .font(FontId::proportional(11.0))
                                .color(TEXT_MUTED),
                        );
                    }
                });

                // Footer
                let bottom = ui.max_rect().bottom() - 20.0;
                let center_x = ui.max_rect().center().x;
                ui.painter().text(
                    Pos2::new(center_x, bottom),
                    Align2::CENTER_CENTER,
                    "Powered by Surge",
                    FontId::proportional(11.0),
                    TEXT_MUTED,
                );
            });
    }

    // -- Installing ------------------------------------------------------------

    fn render_installing(&mut self, ctx: &egui::Context) {
        let (target, status_text) = match &self.screen {
            Screen::Installing { progress, status } => (*progress, status.clone()),
            _ => return,
        };
        let name = self.manifest.runtime.name.clone();

        let animated = ctx.animate_value_with_time(Id::new("install_progress"), target, 0.3);

        egui::CentralPanel::default()
            .frame(egui::Frame::NONE.fill(BG))
            .show(ctx, |ui| {
                let h = ui.available_height();
                ui.vertical_centered(|ui| {
                    ui.add_space(h * 0.18);

                    self.draw_brand_mark(ui, 52.0, true);
                    ui.add_space(20.0);

                    ui.label(
                        RichText::new(format!("Installing {name}"))
                            .font(FontId::proportional(20.0))
                            .color(TEXT_PRIMARY)
                            .strong(),
                    );
                    ui.add_space(28.0);

                    draw_progress_bar(ui, animated, 300.0, 5.0);
                    ui.add_space(14.0);

                    ui.label(
                        RichText::new(format!("{}%", (animated * 100.0) as u32))
                            .font(FontId::proportional(14.0))
                            .color(TEXT_SECONDARY),
                    );
                    ui.add_space(6.0);

                    ui.label(
                        RichText::new(status_text.as_str())
                            .font(FontId::proportional(12.0))
                            .color(TEXT_MUTED),
                    );
                });
            });

        ctx.request_repaint();
    }

    // -- Complete --------------------------------------------------------------

    fn render_complete(&mut self, ctx: &egui::Context) {
        let install_root = match &self.screen {
            Screen::Complete { install_root } => install_root.clone(),
            _ => return,
        };

        egui::CentralPanel::default()
            .frame(egui::Frame::NONE.fill(BG))
            .show(ctx, |ui| {
                let h = ui.available_height();
                ui.vertical_centered(|ui| {
                    ui.add_space(h * 0.13);

                    draw_checkmark(ui, 30.0);
                    ui.add_space(20.0);

                    ui.label(
                        RichText::new("Ready to go!")
                            .font(FontId::proportional(24.0))
                            .color(TEXT_PRIMARY)
                            .strong(),
                    );
                    ui.add_space(8.0);

                    ui.label(
                        RichText::new(format!(
                            "{} v{} has been installed",
                            self.manifest.runtime.name, self.manifest.version
                        ))
                        .font(FontId::proportional(14.0))
                        .color(TEXT_SECONDARY),
                    );
                    ui.add_space(4.0);

                    ui.label(
                        RichText::new(install_root.to_string_lossy())
                            .font(FontId::monospace(11.0))
                            .color(TEXT_MUTED),
                    );

                    ui.add_space(40.0);

                    // Buttons
                    ui.horizontal(|ui| {
                        let total_w = 200.0 + 12.0 + 120.0;
                        ui.add_space((ui.available_width() - total_w) / 2.0);

                        if accent_button(ui, "Launch", 200.0, 44.0).clicked() {
                            let profile = InstallProfile::from_installer_manifest(
                                &self.manifest,
                                &self.manifest.runtime.shortcuts,
                            );
                            let active_app_dir = install_root.join("app");
                            match core_install::launch_installed_application(&profile, &install_root, &active_app_dir) {
                                Ok(_) => ui.ctx().send_viewport_cmd(egui::ViewportCommand::Close),
                                Err(error) => {
                                    self.screen = Screen::Error(format!(
                                        "Failed to launch {}: {error}",
                                        self.manifest.runtime.name
                                    ));
                                }
                            }
                        }

                        ui.add_space(12.0);

                        if ghost_button(ui, "Close", 120.0, 44.0).clicked() {
                            ui.ctx().send_viewport_cmd(egui::ViewportCommand::Close);
                        }
                    });
                });
            });
    }

    // -- Error -----------------------------------------------------------------

    fn render_error(&self, ctx: &egui::Context) {
        let Screen::Error(error_msg) = &self.screen else {
            return;
        };

        egui::CentralPanel::default()
            .frame(egui::Frame::NONE.fill(BG))
            .show(ctx, |ui| {
                let h = ui.available_height();
                ui.vertical_centered(|ui| {
                    ui.add_space(h * 0.12);

                    draw_x_mark(ui, 26.0);
                    ui.add_space(20.0);

                    ui.label(
                        RichText::new("Installation Failed")
                            .font(FontId::proportional(22.0))
                            .color(TEXT_PRIMARY)
                            .strong(),
                    );
                    ui.add_space(16.0);

                    // Error detail in a dark box
                    egui::Frame::NONE
                        .fill(SURFACE)
                        .corner_radius(8)
                        .inner_margin(16.0)
                        .show(ui, |ui| {
                            ui.set_max_width(380.0);
                            egui::ScrollArea::vertical().max_height(120.0).show(ui, |ui| {
                                ui.label(
                                    RichText::new(error_msg.as_str())
                                        .font(FontId::monospace(11.0))
                                        .color(ERROR),
                                );
                            });
                        });

                    ui.add_space(32.0);

                    if ghost_button(ui, "Close", 120.0, 44.0).clicked() {
                        ui.ctx().send_viewport_cmd(egui::ViewportCommand::Close);
                    }
                });
            });
    }
}

impl eframe::App for InstallerApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.poll_progress();

        match &self.screen {
            Screen::Welcome => self.render_welcome(ctx),
            Screen::Installing { .. } => self.render_installing(ctx),
            Screen::Complete { .. } => self.render_complete(ctx),
            Screen::Error(_) => self.render_error(ctx),
        }
    }
}

// ---------------------------------------------------------------------------
// Custom widgets
// ---------------------------------------------------------------------------

fn accent_button(ui: &mut egui::Ui, text: &str, width: f32, height: f32) -> egui::Response {
    let (rect, response) = ui.allocate_exact_size(Vec2::new(width, height), egui::Sense::click());
    let hovered = response.hovered();
    let pressed = response.is_pointer_button_down_on();

    if ui.is_rect_visible(rect) {
        let painter = ui.painter();
        let rounding = CornerRadius::same(10);

        let bg = if pressed {
            ACCENT_PRESSED
        } else if hovered {
            ACCENT_HOVER
        } else {
            ACCENT
        };

        painter.rect_filled(rect, rounding, bg);

        // Glow on hover
        if hovered && !pressed {
            painter.rect_stroke(
                rect.expand(1.5),
                CornerRadius::same(12),
                Stroke::new(1.0, ACCENT.linear_multiply(0.35)),
                StrokeKind::Outside,
            );
        }

        painter.text(
            rect.center(),
            Align2::CENTER_CENTER,
            text,
            FontId::proportional(15.0),
            Color32::WHITE,
        );
    }

    response.on_hover_cursor(egui::CursorIcon::PointingHand)
}

fn ghost_button(ui: &mut egui::Ui, text: &str, width: f32, height: f32) -> egui::Response {
    let (rect, response) = ui.allocate_exact_size(Vec2::new(width, height), egui::Sense::click());
    let hovered = response.hovered();

    if ui.is_rect_visible(rect) {
        let painter = ui.painter();
        let rounding = CornerRadius::same(10);

        if hovered {
            painter.rect_filled(rect, rounding, Color32::from_rgba_premultiplied(255, 255, 255, 6));
        }

        let border = if hovered { TEXT_SECONDARY } else { TEXT_MUTED };
        let text_c = if hovered { TEXT_PRIMARY } else { TEXT_SECONDARY };

        painter.rect_stroke(rect, rounding, Stroke::new(1.0, border), StrokeKind::Outside);
        painter.text(
            rect.center(),
            Align2::CENTER_CENTER,
            text,
            FontId::proportional(14.0),
            text_c,
        );
    }

    response.on_hover_cursor(egui::CursorIcon::PointingHand)
}

// ---------------------------------------------------------------------------
// Custom drawing
// ---------------------------------------------------------------------------

fn draw_progress_bar(ui: &mut egui::Ui, progress: f32, width: f32, height: f32) {
    let (rect, _) = ui.allocate_exact_size(Vec2::new(width, height), egui::Sense::hover());
    if !ui.is_rect_visible(rect) {
        return;
    }

    let painter = ui.painter();
    let r = CornerRadius::same((height / 2.0) as u8);

    // Track
    painter.rect_filled(rect, r, PROGRESS_TRACK);

    // Fill
    let fill_w = rect.width() * progress.clamp(0.0, 1.0);
    if fill_w > 1.0 {
        let fill = egui::Rect::from_min_size(rect.min, Vec2::new(fill_w, height));
        painter.rect_filled(fill, r, ACCENT);

        // Subtle highlight on the top half for a glossy look
        let hi = egui::Rect::from_min_size(rect.min, Vec2::new(fill_w, height * 0.45));
        painter.rect_filled(hi, r, ACCENT_HOVER.linear_multiply(0.18));
    }
}

fn draw_checkmark(ui: &mut egui::Ui, radius: f32) {
    let size = Vec2::splat(radius * 2.0 + 8.0);
    let (rect, _) = ui.allocate_exact_size(size, egui::Sense::hover());
    if !ui.is_rect_visible(rect) {
        return;
    }

    let painter = ui.painter();
    let center = rect.center();

    painter.circle_filled(center, radius, SUCCESS.linear_multiply(0.12));
    painter.circle_stroke(center, radius, Stroke::new(2.0, SUCCESS));

    let half = radius * 0.45;
    let p1 = Pos2::new(center.x - half, center.y);
    let p2 = Pos2::new(center.x - half * 0.3, center.y + half * 0.7);
    let p3 = Pos2::new(center.x + half, center.y - half * 0.5);
    painter.line_segment([p1, p2], Stroke::new(2.5, SUCCESS));
    painter.line_segment([p2, p3], Stroke::new(2.5, SUCCESS));
}

/// Draws the Surge lightning bolt from the logo SVG, decomposed into 4 triangles.
fn draw_bolt(ui: &mut egui::Ui, height: f32, fill: Color32, outline: Color32) {
    let w = height * 0.5;
    let (rect, _) = ui.allocate_exact_size(Vec2::new(w, height), egui::Sense::hover());
    if !ui.is_rect_visible(rect) {
        return;
    }
    let painter = ui.painter();
    let center = rect.center();
    let scale = height / 180.0;
    let pt = |x: f32, y: f32| Pos2::new(center.x + (x - 100.0) * scale, center.y + (y - 100.0) * scale);
    let verts = [
        pt(110.0, 10.0),
        pt(60.0, 95.0),
        pt(95.0, 95.0),
        pt(80.0, 190.0),
        pt(140.0, 105.0),
        pt(105.0, 105.0),
    ];

    // Glow
    painter.circle_filled(center, height * 0.38, fill.linear_multiply(0.08));
    painter.circle_filled(center, height * 0.28, fill.linear_multiply(0.06));

    // Bolt (4 triangles for concave polygon)
    let no_stroke = Stroke::NONE;
    painter.add(Shape::convex_polygon(
        vec![verts[0], verts[1], verts[5]],
        fill,
        no_stroke,
    ));
    painter.add(Shape::convex_polygon(
        vec![verts[1], verts[2], verts[5]],
        fill,
        no_stroke,
    ));
    painter.add(Shape::convex_polygon(
        vec![verts[2], verts[3], verts[4]],
        fill,
        no_stroke,
    ));
    painter.add(Shape::convex_polygon(
        vec![verts[2], verts[4], verts[5]],
        fill,
        no_stroke,
    ));

    // Outline
    painter.add(Shape::closed_line(verts.to_vec(), Stroke::new(1.5, outline)));
}

fn draw_x_mark(ui: &mut egui::Ui, radius: f32) {
    let size = Vec2::splat(radius * 2.0 + 8.0);
    let (rect, _) = ui.allocate_exact_size(size, egui::Sense::hover());
    if !ui.is_rect_visible(rect) {
        return;
    }

    let painter = ui.painter();
    let c = rect.center();

    painter.circle_filled(c, radius, ERROR.linear_multiply(0.12));
    painter.circle_stroke(c, radius, Stroke::new(2.0, ERROR));

    let s = radius * 0.38;
    painter.line_segment(
        [Pos2::new(c.x - s, c.y - s), Pos2::new(c.x + s, c.y + s)],
        Stroke::new(2.5, ERROR),
    );
    painter.line_segment(
        [Pos2::new(c.x + s, c.y - s), Pos2::new(c.x - s, c.y + s)],
        Stroke::new(2.5, ERROR),
    );
}

// ---------------------------------------------------------------------------
// Icon helpers
// ---------------------------------------------------------------------------

pub fn load_window_icon(staging_dir: &Path, icon_name: &str) -> egui::IconData {
    load_app_icon(staging_dir, icon_name).unwrap_or_else(default_surge_icon)
}

pub fn load_app_logo(staging_dir: &Path, icon_name: &str) -> Option<egui::IconData> {
    Some(load_app_icon(staging_dir, icon_name).unwrap_or_else(default_surge_icon))
}

pub fn window_app_id(manifest: &InstallerManifest) -> String {
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
    decode_icon(include_bytes!("../../../assets/logo.svg"), Some("svg")).unwrap_or_default()
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
