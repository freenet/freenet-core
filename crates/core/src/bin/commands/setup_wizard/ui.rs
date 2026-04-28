//! WebView2-based setup wizard dialog for Windows.
//!
//! Renders a modern HTML/CSS interface via Microsoft Edge WebView2 (pre-installed
//! on Windows 10+). The installer backend runs on a background thread and sends
//! progress updates to the UI via the tao event loop proxy.

/// Embedded HTML/CSS/JS for the setup wizard UI.
///
/// Defined at module scope (not inside the Windows-gated `platform` mod) so
/// content-level unit tests run on every CI platform — Windows-only tests
/// don't run on this project's Linux CI matrix and the windows_check job
/// only does `cargo check`. Keeping the constant here means the tests below
/// run wherever `cargo test -p freenet --bin freenet` runs. The constant
/// is unused on non-Windows release builds (the `platform` module is the
/// only consumer), hence the dead-code allow.
#[cfg_attr(not(target_os = "windows"), allow(dead_code))]
const SETUP_HTML: &str = include_str!("setup.html");

#[cfg(target_os = "windows")]
mod platform {
    use super::super::installer::{self, InstallProgress};
    use super::SETUP_HTML;
    use tao::event::{Event, WindowEvent};
    use tao::event_loop::{ControlFlow, EventLoopBuilder};
    use tao::platform::run_return::EventLoopExtRunReturn;
    use tao::window::WindowBuilder;
    use wry::WebViewBuilder;

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

    /// Custom events sent from background threads to the event loop.
    #[derive(Debug)]
    enum UiEvent {
        /// Installation progress update.
        Progress(InstallProgress),
        /// User action received from the webview IPC handler.
        Action(String),
    }

    /// Show the setup wizard dialog and block until the user makes a choice.
    ///
    /// Uses `run_return` instead of `run` so the event loop returns control
    /// to the caller when the dialog closes (tao's `run` calls `process::exit`).
    pub fn show_setup_dialog() -> anyhow::Result<SetupResult> {
        let mut event_loop = EventLoopBuilder::<UiEvent>::with_user_event().build();
        let proxy = event_loop.create_proxy();

        let window = WindowBuilder::new()
            .with_title("Freenet Setup")
            .with_inner_size(tao::dpi::LogicalSize::new(520.0, 440.0))
            .with_resizable(false)
            .with_decorations(true)
            .build(&event_loop)
            .map_err(|e| anyhow::anyhow!("Failed to create window: {e}"))?;

        // Center the window on screen
        if let Some(monitor) = window.current_monitor() {
            let screen = monitor.size();
            let win_size = window.outer_size();
            let x = (screen.width.saturating_sub(win_size.width)) / 2;
            let y = (screen.height.saturating_sub(win_size.height)) / 2;
            window.set_outer_position(tao::dpi::PhysicalPosition::new(x, y));
        }

        // Use a temp directory for WebView2 user data instead of the default
        // (which creates <exe_name>.WebView2/ next to the executable). See #3740.
        let webview2_data_dir = std::env::temp_dir().join("freenet-setup-webview2");
        let mut web_context = wry::WebContext::new(Some(webview2_data_dir.clone()));

        let ipc_proxy = proxy.clone();
        let webview = WebViewBuilder::new_with_web_context(&mut web_context)
            .with_html(SETUP_HTML)
            .with_ipc_handler(move |msg| {
                let _ = ipc_proxy.send_event(UiEvent::Action(msg.body().to_string()));
            })
            .with_devtools(false)
            .build(&window)
            .map_err(|e| anyhow::anyhow!("Failed to create WebView2: {e}"))?;

        let mut result = SetupResult::Cancelled;
        // Guard against double-click spawning multiple installer threads.
        let mut installing = false;
        // Track the installer thread so we can join it before exiting.
        let mut install_handle: Option<std::thread::JoinHandle<()>> = None;

        event_loop.run_return(|event, _, control_flow| {
            *control_flow = ControlFlow::Wait;

            match event {
                Event::WindowEvent {
                    event: WindowEvent::CloseRequested,
                    ..
                } => {
                    // Don't join the installer thread — it may be blocked on a
                    // subprocess, which would hang the window. The detached
                    // service wrapper survives parent exit.
                    drop(install_handle.take());
                    *control_flow = ControlFlow::Exit;
                }

                Event::UserEvent(UiEvent::Action(action)) => match action.as_str() {
                    "install" => {
                        if installing {
                            return;
                        }
                        installing = true;
                        let install_proxy = proxy.clone();
                        let handle = std::thread::spawn(move || {
                            let cb_result = installer::run_install(|progress| {
                                let _ = install_proxy.send_event(UiEvent::Progress(progress));
                            });
                            if let Err(e) = cb_result {
                                let _ = install_proxy.send_event(UiEvent::Progress(
                                    InstallProgress::Error(format!("{e:#}")),
                                ));
                            }
                        });
                        install_handle = Some(handle);
                    }
                    "run_without" => {
                        result = SetupResult::RunWithout;
                        *control_flow = ControlFlow::Exit;
                    }
                    "close" => {
                        *control_flow = ControlFlow::Exit;
                    }
                    _ => {}
                },

                Event::UserEvent(UiEvent::Progress(progress)) => {
                    let js = match &progress {
                        InstallProgress::StoppingExisting => {
                            "updateProgress(7, 'Stopping existing service...')".to_string()
                        }
                        InstallProgress::CopyingBinary => {
                            "updateProgress(21, 'Copying files...')".to_string()
                        }
                        InstallProgress::DownloadingFdev => {
                            "updateProgress(35, 'Downloading developer tools...')".to_string()
                        }
                        InstallProgress::FdevSkipped(_) => {
                            "updateProgress(42, 'Developer tools skipped')".to_string()
                        }
                        InstallProgress::AddingToPath => {
                            "updateProgress(50, 'Configuring system PATH...')".to_string()
                        }
                        InstallProgress::InstallingService => {
                            "updateProgress(65, 'Installing background service...')".to_string()
                        }
                        InstallProgress::LaunchingService => {
                            "updateProgress(80, 'Starting Freenet...')".to_string()
                        }
                        InstallProgress::OpeningDashboard => {
                            "updateProgress(92, 'Opening dashboard...')".to_string()
                        }
                        InstallProgress::Complete => {
                            result = SetupResult::Installed;
                            "showComplete()".to_string()
                        }
                        InstallProgress::Error(msg) => {
                            let escaped = escape_for_js(msg);
                            format!("showError('{escaped}')")
                        }
                    };
                    let _ = webview.evaluate_script(&js);
                }

                _ => {}
            }
        });

        // Drop the webview so WebView2 releases file locks on the data dir
        drop(webview);
        // Clean up the temporary WebView2 data directory
        let _ = std::fs::remove_dir_all(&webview2_data_dir);

        Ok(result)
    }

    /// Escape a string for safe embedding in a JS single-quoted string literal.
    fn escape_for_js(s: &str) -> String {
        s.replace('\\', "\\\\")
            .replace('\'', "\\'")
            .replace('\n', "\\n")
            .replace('\r', "\\r")
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_escape_for_js_special_chars() {
            assert_eq!(escape_for_js("hello"), "hello");
            assert_eq!(escape_for_js("it's"), "it\\'s");
            assert_eq!(escape_for_js("line1\nline2"), "line1\\nline2");
            assert_eq!(escape_for_js("path\\to\\file"), "path\\\\to\\\\file");
            assert_eq!(
                escape_for_js("err: can't\ndo it\\now"),
                "err: can\\'t\\ndo it\\\\now"
            );
        }
    }
}

#[cfg(not(target_os = "windows"))]
mod platform {
    /// Stub result type for non-Windows platforms.
    #[derive(Debug, Clone, PartialEq)]
    #[allow(dead_code)] // re-exported for parity with the Windows variant
    pub enum SetupResult {
        Installed,
        RunWithout,
        Cancelled,
    }

    /// No-op on non-Windows platforms.
    #[allow(dead_code)] // re-exported for parity with the Windows variant
    pub fn show_setup_dialog() -> anyhow::Result<SetupResult> {
        Ok(SetupResult::RunWithout)
    }
}

#[rustfmt::skip]  // Import order differs between local and CI rustfmt versions
#[cfg_attr(not(target_os = "windows"), allow(unused_imports))]
pub use platform::{SetupResult, show_setup_dialog};

#[cfg(test)]
mod html_tests {
    use super::SETUP_HTML;

    /// The success screen used to silently auto-close after 5s with no
    /// indicator, so users saw the dialog vanish on its own. The fix wires
    /// up a visible countdown ("Closing in Ns...") and renames the button
    /// to "Close now". The HTML/JS is embedded in a string and never
    /// touched at runtime by Rust, so a future refactor could trivially
    /// delete the countdown wiring without breaking compilation. Pin the
    /// user-visible elements that drive the countdown so a regression at
    /// least trips a unit test on every CI platform.
    #[test]
    fn test_setup_html_success_screen_has_visible_countdown() {
        assert!(
            SETUP_HTML.contains("id=\"countdown\""),
            "the success screen must render a #countdown element",
        );
        assert!(
            SETUP_HTML.contains("Closing in "),
            "countdown text must say 'Closing in Ns...' so the user sees what is happening",
        );
        assert!(
            SETUP_HTML.contains("setInterval("),
            "countdown must use setInterval to update the visible label every second",
        );
        assert!(
            SETUP_HTML.contains("Close now"),
            "manual-close button label must read 'Close now' (not the old 'Close')",
        );
        assert!(
            SETUP_HTML.contains("clearInterval("),
            "doClose must clearInterval so a manual close cancels the pending tick",
        );
    }
}
