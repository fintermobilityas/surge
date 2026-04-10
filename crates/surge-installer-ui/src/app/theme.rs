use eframe::egui::{self, Color32, CornerRadius, Stroke};

pub(super) const BG: Color32 = Color32::from_rgb(13, 13, 20);
pub(super) const SURFACE: Color32 = Color32::from_rgb(24, 24, 37);
pub(super) const ACCENT: Color32 = Color32::from_rgb(99, 102, 241);
pub(super) const ACCENT_HOVER: Color32 = Color32::from_rgb(129, 140, 248);
pub(super) const ACCENT_PRESSED: Color32 = Color32::from_rgb(67, 56, 202);
pub(super) const SUCCESS: Color32 = Color32::from_rgb(34, 197, 94);
pub(super) const ERROR: Color32 = Color32::from_rgb(239, 68, 68);
pub(super) const TEXT_PRIMARY: Color32 = Color32::from_rgb(248, 250, 252);
pub(super) const TEXT_SECONDARY: Color32 = Color32::from_rgb(148, 163, 184);
pub(super) const TEXT_MUTED: Color32 = Color32::from_rgb(100, 116, 139);
pub(super) const PROGRESS_TRACK: Color32 = Color32::from_rgb(30, 30, 46);

pub(crate) fn configure_theme(ctx: &egui::Context) {
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
