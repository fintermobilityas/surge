use eframe::egui::{self, Align2, Color32, FontId, Id, Pos2, RichText, Vec2};

use surge_core::install as core_install;
use surge_core::install::InstallProfile;

use super::theme::{ACCENT, ACCENT_PRESSED, BG, ERROR, SURFACE, TEXT_MUTED, TEXT_PRIMARY, TEXT_SECONDARY};
use super::widgets::{accent_button, draw_bolt, draw_checkmark, draw_progress_bar, draw_x_mark, ghost_button};
use super::{InstallerApp, Screen};

impl InstallerApp {
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

    pub(super) fn render_welcome(&mut self, ui: &mut egui::Ui) {
        egui::CentralPanel::default()
            .frame(egui::Frame::NONE.fill(BG))
            .show(ui, |ui| {
                let available_height = ui.available_height();
                ui.vertical_centered(|ui| {
                    ui.add_space(available_height * 0.10);

                    self.draw_brand_mark(ui, 80.0, false);
                    ui.add_space(20.0);

                    ui.label(
                        RichText::new(&self.manifest.runtime.name)
                            .font(FontId::proportional(26.0))
                            .color(TEXT_PRIMARY)
                            .strong(),
                    );
                    ui.add_space(4.0);

                    ui.label(
                        RichText::new(format!(
                            "v{}  \u{00b7}  {}",
                            self.manifest.version, self.manifest.channel
                        ))
                        .font(FontId::proportional(13.0))
                        .color(TEXT_MUTED),
                    );

                    ui.add_space(40.0);

                    if accent_button(ui, "Install", 220.0, 46.0).clicked() {
                        self.start_install(ui.ctx());
                    }

                    ui.add_space(16.0);

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

    pub(super) fn render_installing(&mut self, ui: &mut egui::Ui) {
        let (target, status_text) = match &self.screen {
            Screen::Installing { progress, status } => (*progress, status.clone()),
            _ => return,
        };
        let name = self.manifest.runtime.name.clone();

        let animated = ui
            .ctx()
            .animate_value_with_time(Id::new("install_progress"), target, 0.3);

        egui::CentralPanel::default()
            .frame(egui::Frame::NONE.fill(BG))
            .show(ui, |ui| {
                let available_height = ui.available_height();
                ui.vertical_centered(|ui| {
                    ui.add_space(available_height * 0.18);

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

        ui.ctx().request_repaint();
    }

    pub(super) fn render_complete(&mut self, ui: &mut egui::Ui) {
        let install_root = match &self.screen {
            Screen::Complete { install_root } => install_root.clone(),
            _ => return,
        };

        egui::CentralPanel::default()
            .frame(egui::Frame::NONE.fill(BG))
            .show(ui, |ui| {
                let available_height = ui.available_height();
                ui.vertical_centered(|ui| {
                    ui.add_space(available_height * 0.13);

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

                    ui.horizontal(|ui| {
                        let total_width = 200.0 + 12.0 + 120.0;
                        ui.add_space((ui.available_width() - total_width) / 2.0);

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

    pub(super) fn render_error(&self, ui: &mut egui::Ui) {
        let Screen::Error(error_msg) = &self.screen else {
            return;
        };

        egui::CentralPanel::default()
            .frame(egui::Frame::NONE.fill(BG))
            .show(ui, |ui| {
                let available_height = ui.available_height();
                ui.vertical_centered(|ui| {
                    ui.add_space(available_height * 0.12);

                    draw_x_mark(ui, 26.0);
                    ui.add_space(20.0);

                    ui.label(
                        RichText::new("Installation Failed")
                            .font(FontId::proportional(22.0))
                            .color(TEXT_PRIMARY)
                            .strong(),
                    );
                    ui.add_space(16.0);

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
