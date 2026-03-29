//! WebView2-based setup wizard dialog for Windows.
//!
//! Renders a modern HTML/CSS interface via Microsoft Edge WebView2 (pre-installed
//! on Windows 10+). The installer backend runs on a background thread and sends
//! progress updates to the UI via the tao event loop proxy.

#[cfg(target_os = "windows")]
mod platform {
    use super::super::installer::{self, InstallProgress};
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

        let ipc_proxy = proxy.clone();
        let webview = WebViewBuilder::new()
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
                    // If the installer is still running, wait for it to finish
                    // before closing so we don't abandon a half-done install.
                    if let Some(handle) = install_handle.take() {
                        let _ = webview
                            .evaluate_script("updateProgress(95, 'Finishing installation...')");
                        handle.join().ok();
                    }
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

    /// Embedded HTML/CSS/JS for the setup wizard UI.
    const SETUP_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }

  body {
    font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
    background: #0c1219;
    color: #d4dde8;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    height: 100vh;
    overflow: hidden;
    user-select: none;
    -webkit-user-select: none;
  }

  .logo-container {
    width: 88px;
    height: 88px;
    margin-bottom: 28px;
  }

  .logo-container svg {
    width: 100%;
    height: 100%;
    filter: drop-shadow(0 2px 12px rgba(0, 110, 255, 0.15));
  }

  h1 {
    font-size: 22px;
    font-weight: 600;
    color: #f0f4f8;
    margin-bottom: 8px;
    letter-spacing: -0.3px;
  }

  .subtitle {
    color: #7b8da0;
    font-size: 13.5px;
    margin-bottom: 40px;
  }

  #welcome { text-align: center; }

  .btn-primary {
    display: inline-block;
    background: linear-gradient(135deg, #0070f3 0%, #0058cc 100%);
    color: #fff;
    border: none;
    padding: 13px 52px;
    border-radius: 8px;
    font-size: 14.5px;
    font-weight: 600;
    cursor: pointer;
    transition: all 0.15s ease;
    letter-spacing: 0.2px;
  }

  .btn-primary:hover {
    background: linear-gradient(135deg, #1a82ff 0%, #0066ee 100%);
    transform: translateY(-1px);
    box-shadow: 0 4px 16px rgba(0, 112, 243, 0.35);
  }

  .btn-primary:active {
    transform: translateY(0);
    box-shadow: 0 2px 8px rgba(0, 112, 243, 0.25);
  }

  .btn-secondary {
    display: inline-block;
    background: transparent;
    color: #5a7089;
    border: none;
    padding: 10px 24px;
    font-size: 12.5px;
    cursor: pointer;
    transition: color 0.15s ease;
    margin-top: 14px;
  }

  .btn-secondary:hover { color: #8ba3ba; }

  #progress {
    display: none;
    width: 340px;
    text-align: center;
  }

  .progress-track {
    width: 100%;
    height: 3px;
    background: #1a2635;
    border-radius: 2px;
    overflow: hidden;
    margin-bottom: 18px;
  }

  .progress-fill {
    height: 100%;
    background: linear-gradient(90deg, #0070f3, #00b4d8);
    border-radius: 2px;
    transition: width 0.6s cubic-bezier(0.4, 0, 0.2, 1);
    width: 0%;
  }

  .progress-text {
    color: #7b8da0;
    font-size: 12.5px;
  }

  #complete {
    display: none;
    text-align: center;
  }

  .complete-icon {
    width: 48px;
    height: 48px;
    margin: 0 auto 16px;
    border-radius: 50%;
    background: rgba(16, 185, 129, 0.1);
    display: flex;
    align-items: center;
    justify-content: center;
  }

  .complete-icon svg {
    width: 24px;
    height: 24px;
    stroke: #10b981;
    fill: none;
    stroke-width: 2.5;
    stroke-linecap: round;
    stroke-linejoin: round;
  }

  .complete-text {
    color: #c8d5e2;
    font-size: 14px;
    margin-bottom: 6px;
  }

  .complete-hint {
    color: #5a7089;
    font-size: 12px;
    margin-bottom: 28px;
  }

  #error {
    display: none;
    text-align: center;
    margin-top: 20px;
  }

  .error-text {
    color: #f87171;
    font-size: 12.5px;
    margin-bottom: 20px;
    line-height: 1.5;
    max-width: 380px;
  }

  .btn-close {
    display: inline-block;
    background: rgba(255, 255, 255, 0.06);
    color: #8ba3ba;
    border: 1px solid rgba(255, 255, 255, 0.08);
    padding: 10px 40px;
    border-radius: 8px;
    font-size: 13px;
    font-weight: 500;
    cursor: pointer;
    transition: all 0.15s ease;
  }

  .btn-close:hover {
    background: rgba(255, 255, 255, 0.1);
    color: #c8d5e2;
  }
</style>
</head>
<body>

<div class="logo-container">
  <svg viewBox="100 30 430 400" xmlns="http://www.w3.org/2000/svg">
    <defs>
      <linearGradient id="g" x1="0%" y1="0%" x2="100%" y2="100%">
        <stop offset="0%" stop-color="#0070f3"/>
        <stop offset="100%" stop-color="#00b4d8"/>
      </linearGradient>
    </defs>
    <path fill="url(#g)" d="M358.864 40.470C358.605 40.728 354.143 42.467 348.947 44.334C284.621 67.446 232.573 113.729 201.443 175.500C193.895 190.478 184.375 213.708 185.375 214.708C185.621 214.954 187.715 211.857 190.030 207.827C211.190 170.984 229.863 146.093 255.968 119.933C274.854 101.008 282.998 94.207 302.034 81.466C334.671 59.621 367.531 47.376 401.250 44.492L409.000 43.829 408.984 47.165C408.958 52.704 405.255 68.515 401.010 81.213C382.392 136.898 338.799 184.709 277.000 217.224C271.225 220.263 263.913 223.788 260.750 225.058C254.629 227.517 254.126 228.307 256.511 231.712C258.282 234.241 258.484 234.089 249.500 237.002C226.868 244.341 200.420 256.771 183.918 267.825C173.918 274.522 156.961 289.225 158.000 290.296C158.275 290.579 163.450 287.694 169.500 283.883C175.550 280.073 186.125 274.083 193.000 270.573C264.905 233.856 345.414 226.155 422.387 248.633C434.634 252.210 468.194 264.823 465.830 264.961C465.461 264.982 459.741 263.440 453.118 261.534C422.666 252.769 376.068 246.967 347.500 248.384C320.590 249.719 284.052 255.527 283.798 258.510C283.694 259.727 291.796 264.541 298.477 267.231C306.163 270.326 319.360 270.612 338.812 268.103C385.602 262.070 433.627 269.250 469.963 287.712C475.721 290.638 480.874 293.474 481.416 294.016C482.061 294.661 476.225 295.004 464.450 295.010C407.520 295.043 349.853 308.084 300.500 332.086C290.412 336.992 272.833 346.834 271.147 348.519C269.278 350.389 273.301 349.263 283.966 344.933C302.548 337.389 332.479 327.629 351.000 323.074C386.266 314.400 413.893 311.393 450.000 312.298C474.559 312.914 491.602 315.535 509.306 321.421C520.784 325.236 519.954 325.640 504.924 323.552C419.615 311.701 330.506 332.225 238.000 385.033C224.991 392.460 221.855 394.386 200.762 407.913C184.591 418.282 178.817 420.978 172.750 420.990C167.060 421.002 164.441 418.869 163.413 413.387C160.912 400.055 178.394 366.762 202.136 339.644L206.387 334.788 197.443 335.644C183.073 337.019 158.519 336.519 150.152 334.681C132.833 330.876 120.785 321.947 117.439 310.437C112.326 292.850 123.492 270.717 146.912 252.015C154.528 245.934 155.702 244.562 156.798 240.464C158.983 232.296 168.599 206.900 174.208 194.482C184.044 172.710 197.989 150.083 213.332 131.000C229.597 110.770 255.612 87.415 277.277 73.590C286.990 67.393 310.855 55.436 323.062 50.653C335.623 45.730 360.750 38.583 358.864 40.470M375.000 209.596C430.712 216.655 477.541 237.609 509.241 269.661C514.049 274.523 518.765 279.625 519.721 281.000C521.404 283.419 521.347 283.408 517.980 280.669C500.484 266.434 486.556 257.516 468.000 248.668C452.323 241.193 438.766 236.261 424.000 232.663C395.297 225.667 374.955 223.024 349.705 223.010C333.212 223.000 332.865 222.956 330.455 220.545C329.105 219.195 328.000 217.046 328.000 215.768C328.000 212.845 330.558 209.125 333.357 207.978C336.189 206.817 360.536 207.763 375.000 209.596"/>
  </svg>
</div>

<h1 id="heading">Welcome to Freenet</h1>
<p class="subtitle" id="subtitle">Decentralized communication for everyone</p>

<div id="welcome">
  <button class="btn-primary" onclick="doInstall()">Install Freenet</button><br>
  <button class="btn-secondary" onclick="doRunWithout()">Run without installing</button>
</div>

<div id="progress">
  <div class="progress-track">
    <div class="progress-fill" id="bar"></div>
  </div>
  <p class="progress-text" id="status">Preparing...</p>
</div>

<div id="complete">
  <div class="complete-icon">
    <svg viewBox="0 0 24 24"><polyline points="20 6 9 17 4 12"/></svg>
  </div>
  <p class="complete-text">Freenet is running</p>
  <p class="complete-hint">Look for the icon in your system tray</p>
  <button class="btn-close" onclick="doClose()">Close</button>
</div>

<div id="error">
  <p class="error-text" id="error-msg"></p>
  <button class="btn-close" onclick="doClose()">Close</button>
</div>

<script>
  function doInstall() {
    document.getElementById('welcome').style.display = 'none';
    document.getElementById('progress').style.display = 'block';
    window.ipc.postMessage('install');
  }
  function doRunWithout() { window.ipc.postMessage('run_without'); }
  function doClose() { window.ipc.postMessage('close'); }

  function updateProgress(pct, text) {
    document.getElementById('bar').style.width = pct + '%';
    document.getElementById('status').textContent = text;
  }

  function showComplete() {
    document.getElementById('progress').style.display = 'none';
    document.getElementById('heading').textContent = 'Installation complete';
    document.getElementById('subtitle').style.display = 'none';
    document.getElementById('complete').style.display = 'block';
    // Auto-close after 5 seconds so the user doesn't have to click Close
    setTimeout(function() { window.ipc.postMessage('close'); }, 5000);
  }

  function showError(msg) {
    document.getElementById('progress').style.display = 'none';
    document.getElementById('heading').textContent = 'Installation failed';
    document.getElementById('subtitle').style.display = 'none';
    document.getElementById('error').style.display = 'block';
    document.getElementById('error-msg').textContent = msg;
  }
</script>

</body>
</html>"##;
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
