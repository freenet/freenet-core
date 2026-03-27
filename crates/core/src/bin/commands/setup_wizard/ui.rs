//! NWG-based setup wizard dialog for Windows.
//!
//! Shows a native Windows dialog with the Freenet logo, welcome text,
//! and Install/Run buttons. During installation, shows a progress bar
//! with status updates from the background installer thread.

#[cfg(target_os = "windows")]
mod platform {
    use super::super::installer::{self, InstallProgress};
    use native_windows_derive as nwd;
    use native_windows_gui as nwg;
    use nwg::NativeUi;
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    /// Result of the setup dialog interaction.
    #[derive(Debug, Clone, PartialEq, Default)]
    pub enum SetupResult {
        /// User clicked "Install" and installation completed successfully.
        Installed,
        /// User clicked "Run without installing".
        RunWithout,
        /// User closed the dialog window.
        #[default]
        Cancelled,
    }

    /// The main setup wizard window.
    #[derive(Default, nwd::NwgUi)]
    pub struct SetupWizard {
        // ── Window ──
        #[nwg_control(
            size: (480, 440),
            center: true,
            title: "Freenet Setup",
            flags: "WINDOW|VISIBLE"
        )]
        #[nwg_events(OnWindowClose: [SetupWizard::on_close])]
        window: nwg::Window,

        // ── Logo ──
        #[nwg_resource(source_bin: Some(include_bytes!("../assets/freenet_logo_256.png")))]
        logo_bitmap: nwg::Bitmap,

        #[nwg_control(
            size: (128, 128),
            position: (176, 20),
            bitmap: Some(&data.logo_bitmap)
        )]
        logo_frame: nwg::ImageFrame,

        // ── Welcome text ──
        #[nwg_resource(family: "Segoe UI", size: 22, weight: 600)]
        heading_font: nwg::Font,

        #[nwg_resource(family: "Segoe UI", size: 16)]
        body_font: nwg::Font,

        #[nwg_control(
            text: "Welcome to Freenet",
            size: (300, 32),
            position: (90, 160),
            font: Some(&data.heading_font),
            flags: "VISIBLE"
        )]
        heading_label: nwg::Label,

        #[nwg_control(
            text: "A decentralized platform for free communication.\nClick Install to get started.",
            size: (400, 40),
            position: (40, 200),
            font: Some(&data.body_font),
            flags: "VISIBLE"
        )]
        desc_label: nwg::Label,

        // ── Buttons ──
        #[nwg_resource(family: "Segoe UI", size: 17, weight: 600)]
        button_font: nwg::Font,

        #[nwg_resource(family: "Segoe UI", size: 15)]
        link_font: nwg::Font,

        #[nwg_control(
            text: "Install Freenet",
            size: (220, 44),
            position: (130, 260),
            font: Some(&data.button_font),
            flags: "VISIBLE"
        )]
        #[nwg_events(OnButtonClick: [SetupWizard::on_install])]
        install_btn: nwg::Button,

        #[nwg_control(
            text: "Run without installing",
            size: (220, 30),
            position: (130, 315),
            font: Some(&data.link_font),
            flags: "VISIBLE"
        )]
        #[nwg_events(OnButtonClick: [SetupWizard::on_run_only])]
        run_only_btn: nwg::Button,

        // ── Progress (hidden until install starts) ──
        #[nwg_control(size: (400, 22), position: (40, 270), range: 0..7)]
        progress_bar: nwg::ProgressBar,

        #[nwg_control(text: "", size: (400, 24), position: (40, 300), font: Some(&data.body_font))]
        status_label: nwg::Label,

        // ── Error display (hidden until error) ──
        #[nwg_control(text: "", size: (400, 60), position: (40, 340), font: Some(&data.body_font))]
        error_label: nwg::Label,

        // ── Cross-thread notification ──
        #[nwg_control]
        #[nwg_events(OnNotice: [SetupWizard::on_progress_update])]
        progress_notice: nwg::Notice,

        // ── State ──
        result: RefCell<SetupResult>,
        progress_queue: Arc<Mutex<VecDeque<InstallProgress>>>,
        install_step: RefCell<u32>,
        /// Set to true after install completes or errors — changes button to "Close"
        finished: RefCell<bool>,
        /// Guard against double-click spawning multiple installer threads
        installing: RefCell<bool>,
    }

    impl SetupWizard {
        fn on_install(&self) {
            // If install already finished (success or error), button acts as "Close"
            if *self.finished.borrow() {
                nwg::stop_thread_dispatch();
                return;
            }

            // Guard against double-click race
            if *self.installing.borrow() {
                return;
            }
            *self.installing.borrow_mut() = true;

            // Hide buttons, show progress elements
            self.install_btn.set_visible(false);
            self.run_only_btn.set_visible(false);
            self.progress_bar.set_visible(true);
            self.status_label.set_visible(true);
            self.desc_label.set_text("Installing...");
            self.progress_bar.set_pos(0);

            let queue = self.progress_queue.clone();
            let sender = self.progress_notice.sender();

            std::thread::spawn(move || {
                let result = installer::run_install(|progress| {
                    if let Ok(mut q) = queue.lock() {
                        q.push_back(progress);
                    }
                    sender.notice();
                });

                if let Err(e) = result {
                    if let Ok(mut q) = queue.lock() {
                        q.push_back(InstallProgress::Error(format!("{e:#}")));
                    }
                    sender.notice();
                }
            });
        }

        fn on_run_only(&self) {
            *self.result.borrow_mut() = SetupResult::RunWithout;
            nwg::stop_thread_dispatch();
        }

        fn on_close(&self) {
            *self.result.borrow_mut() = SetupResult::Cancelled;
            nwg::stop_thread_dispatch();
        }

        fn on_progress_update(&self) {
            let updates: Vec<InstallProgress> = {
                let Ok(mut q) = self.progress_queue.lock() else {
                    return;
                };
                q.drain(..).collect()
            };

            for update in updates {
                let (text, step) = match &update {
                    InstallProgress::StoppingExisting => ("Stopping existing service...", 0),
                    InstallProgress::CopyingBinary => ("Copying files...", 1),
                    InstallProgress::DownloadingFdev => ("Downloading developer tools...", 2),
                    InstallProgress::FdevSkipped(_) => ("Developer tools skipped (optional)", 2),
                    InstallProgress::AddingToPath => ("Configuring system PATH...", 3),
                    InstallProgress::InstallingService => ("Installing background service...", 4),
                    InstallProgress::LaunchingService => ("Starting Freenet...", 5),
                    InstallProgress::OpeningDashboard => ("Opening dashboard...", 6),
                    InstallProgress::Complete => ("Installation complete!", 7),
                    InstallProgress::Error(msg) => {
                        self.error_label.set_visible(true);
                        self.error_label.set_text(&format!("Error: {msg}"));
                        self.install_btn.set_text("Close");
                        self.install_btn.set_visible(true);
                        *self.finished.borrow_mut() = true;
                        return;
                    }
                };

                self.status_label.set_text(text);
                self.progress_bar.set_pos(step);
                *self.install_step.borrow_mut() = step;

                if matches!(update, InstallProgress::Complete) {
                    self.desc_label
                        .set_text("Freenet is running! Look for the icon in your system tray.");
                    self.install_btn.set_text("Close");
                    self.install_btn.set_visible(true);
                    *self.finished.borrow_mut() = true;
                    *self.result.borrow_mut() = SetupResult::Installed;
                }
            }
        }
    }

    /// Show the setup wizard dialog and block until the user makes a choice.
    pub fn show_setup_dialog() -> anyhow::Result<SetupResult> {
        nwg::init().map_err(|e| anyhow::anyhow!("Failed to initialize NWG: {e}"))?;

        // Enable high-DPI awareness
        nwg::Font::set_global_family("Segoe UI")
            .map_err(|e| anyhow::anyhow!("Failed to set font: {e}"))?;

        let wizard = SetupWizard::build_ui(Default::default())
            .map_err(|e| anyhow::anyhow!("Failed to build setup UI: {e}"))?;

        nwg::dispatch_thread_events();

        Ok(wizard.result.borrow().clone())
    }
}

#[cfg(not(target_os = "windows"))]
mod platform {
    /// Stub result type for non-Windows platforms.
    #[derive(Debug, Clone, PartialEq)]
    pub enum SetupResult {
        Installed,
        RunWithout,
        Cancelled,
    }

    /// No-op on non-Windows platforms.
    pub fn show_setup_dialog() -> anyhow::Result<SetupResult> {
        Ok(SetupResult::RunWithout)
    }
}

#[rustfmt::skip]  // Import order differs between local and CI rustfmt versions
pub use platform::{SetupResult, show_setup_dialog};
