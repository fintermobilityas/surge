#![forbid(unsafe_code)]
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::items_after_statements,
    clippy::unnecessary_wraps
)]

mod icons;
mod screens;
mod theme;
mod widgets;

use std::path::PathBuf;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};

use eframe::egui;

use surge_core::config::installer::InstallerManifest;

use crate::install::{self, ProgressUpdate};

pub(crate) use icons::{load_app_logo, load_window_icon, window_app_id};
pub(crate) use theme::configure_theme;

enum Screen {
    Welcome,
    Installing { progress: f32, status: String },
    Complete { install_root: PathBuf },
    Error(String),
}

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
}

impl eframe::App for InstallerApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        self.poll_progress();

        match &self.screen {
            Screen::Welcome => self.render_welcome(ui),
            Screen::Installing { .. } => self.render_installing(ui),
            Screen::Complete { .. } => self.render_complete(ui),
            Screen::Error(_) => self.render_error(ui),
        }
    }
}
