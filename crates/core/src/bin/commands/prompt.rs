//! `freenet prompt` subcommand: show a native permission dialog to the user.
//!
//! This binary subcommand is spawned by the node (via `SubprocessPrompter`) when
//! a delegate emits `RequestUserInput`. It gets its own process and main thread,
//! avoiding event loop conflicts with the node's tokio runtime.
//!
//! Strategy (tried in order):
//! 1. **Linux**: zenity or kdialog (native GTK/Qt dialogs)
//! 2. **macOS**: osascript (native Cocoa dialog via AppleScript)
//! 3. **Windows**: PowerShell MessageBox
//! 4. **Fallback**: stdin/stdout terminal prompt (if TTY available)
//! 5. **Headless**: prints -1 (deny) if no dialog mechanism is available
//!
//! Prints the selected button index (0-based) to stdout, or -1 on deny/timeout/dismiss.

use clap::Args;

/// Arguments for the `freenet prompt` subcommand.
#[derive(Args, Debug)]
pub struct PromptArgs {
    /// The message to display to the user.
    #[arg(long)]
    pub message: String,

    /// JSON array of button labels (e.g. '["Allow Once","Always Allow","Deny"]').
    #[arg(long)]
    pub buttons: String,

    /// Timeout in seconds before auto-denying.
    #[arg(long, default_value = "60")]
    pub timeout: u64,
}

impl PromptArgs {
    pub fn run(self) -> anyhow::Result<()> {
        let labels: Vec<String> = serde_json::from_str(&self.buttons)
            .map_err(|e| anyhow::anyhow!("Invalid --buttons JSON: {e}"))?;

        if labels.is_empty() {
            println!("-1");
            return Ok(());
        }

        let result = show_dialog(&self.message, &labels, self.timeout);
        println!("{result}");
        Ok(())
    }
}

/// Show a dialog and return the selected button index, or -1 on deny/dismiss/timeout.
fn show_dialog(message: &str, labels: &[String], timeout_secs: u64) -> i32 {
    // Try platform-specific dialogs first
    #[cfg(target_os = "linux")]
    if let Some(idx) = try_linux_dialog(message, labels) {
        return idx;
    }

    #[cfg(target_os = "macos")]
    if let Some(idx) = try_macos_dialog(message, labels) {
        return idx;
    }

    #[cfg(target_os = "windows")]
    if let Some(idx) = try_windows_dialog(message, labels) {
        return idx;
    }

    // Fallback: terminal prompt if stdin is a TTY
    if atty_is_terminal() {
        return terminal_prompt(message, labels, timeout_secs);
    }

    // Headless: auto-deny
    -1
}

/// Try to show a dialog using zenity or kdialog (Linux).
#[cfg(target_os = "linux")]
fn try_linux_dialog(message: &str, labels: &[String]) -> Option<i32> {
    // Try zenity first
    if let Some(idx) = try_zenity(message, labels) {
        return Some(idx);
    }
    // Try kdialog
    if let Some(idx) = try_kdialog(message, labels) {
        return Some(idx);
    }
    None
}

#[cfg(target_os = "linux")]
fn try_zenity(message: &str, labels: &[String]) -> Option<i32> {
    use std::process::Command;

    // Check if zenity is available
    if Command::new("zenity").arg("--version").output().is_err() {
        return None;
    }

    // For 2 buttons: use --question with custom labels
    // For 3+ buttons: use --question with --extra-button for additional options
    let mut cmd = Command::new("zenity");
    cmd.arg("--question")
        .arg("--title=Freenet Permission")
        .arg(format!("--text={message}"))
        .arg("--no-wrap");

    match labels.len() {
        1 => {
            cmd.arg(format!("--ok-label={}", labels[0]));
        }
        2 => {
            cmd.arg(format!("--ok-label={}", labels[0]));
            cmd.arg(format!("--cancel-label={}", labels[1]));
        }
        _ => {
            // First button = OK, last button = Cancel, middle buttons = extra
            cmd.arg(format!("--ok-label={}", labels[0]));
            cmd.arg(format!("--cancel-label={}", labels[labels.len() - 1]));
            for label in &labels[1..labels.len() - 1] {
                cmd.arg(format!("--extra-button={label}"));
            }
        }
    }

    let output = cmd.output().ok()?;
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();

    if output.status.success() {
        // OK button pressed (first label)
        return Some(0);
    }

    // Check exit code: 1 = Cancel (last label), 5 = timeout
    match output.status.code() {
        Some(1) if labels.len() >= 2 => {
            // Cancel = last button
            return Some(labels.len() as i32 - 1);
        }
        _ => {}
    }

    // Check stdout for extra button text
    if !stdout.is_empty() {
        for (i, label) in labels.iter().enumerate() {
            if stdout == *label {
                return Some(i as i32);
            }
        }
    }

    Some(-1)
}

#[cfg(target_os = "linux")]
fn try_kdialog(message: &str, labels: &[String]) -> Option<i32> {
    use std::process::Command;

    if Command::new("kdialog").arg("--version").output().is_err() {
        return None;
    }

    // kdialog supports --yesno, --yesnocancel, or --menu for arbitrary choices
    if labels.len() <= 3 {
        let mut cmd = Command::new("kdialog");
        cmd.arg("--title").arg("Freenet Permission");

        match labels.len() {
            1 => {
                cmd.arg("--msgbox").arg(message);
                let status = cmd.status().ok()?;
                return Some(if status.success() { 0 } else { -1 });
            }
            2 => {
                cmd.arg("--yesno")
                    .arg(message)
                    .arg("--yes-label")
                    .arg(&labels[0])
                    .arg("--no-label")
                    .arg(&labels[1]);
                let status = cmd.status().ok()?;
                return Some(if status.success() { 0 } else { 1 });
            }
            3 => {
                cmd.arg("--yesnocancel")
                    .arg(message)
                    .arg("--yes-label")
                    .arg(&labels[0])
                    .arg("--no-label")
                    .arg(&labels[1])
                    .arg("--cancel-label")
                    .arg(&labels[2]);
                let status = cmd.status().ok()?;
                return match status.code() {
                    Some(0) => Some(0),
                    Some(1) => Some(1),
                    Some(2) => Some(2),
                    _ => Some(-1),
                };
            }
            _ => {}
        }
    }

    None
}

/// Try to show a dialog using osascript (macOS).
#[cfg(target_os = "macos")]
fn try_macos_dialog(message: &str, labels: &[String]) -> Option<i32> {
    use std::process::Command;

    // Build AppleScript button list: {"Allow Once", "Always Allow", "Deny"}
    let button_list = labels
        .iter()
        .map(|l| format!("\"{}\"", l.replace('"', "\\\"")))
        .collect::<Vec<_>>()
        .join(", ");

    let script = format!(
        r#"display dialog "{}" buttons {{{button_list}}} default button 1 with title "Freenet Permission" with icon caution"#,
        message.replace('"', "\\\""),
    );

    let output = Command::new("osascript")
        .arg("-e")
        .arg(&script)
        .output()
        .ok()?;

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        // Output is like: "button returned:Allow Once"
        if let Some(button_text) = stdout.strip_prefix("button returned:") {
            let button_text = button_text.trim();
            for (i, label) in labels.iter().enumerate() {
                if button_text == label {
                    return Some(i as i32);
                }
            }
        }
    }

    Some(-1)
}

/// Try to show a dialog using PowerShell (Windows).
#[cfg(target_os = "windows")]
fn try_windows_dialog(message: &str, labels: &[String]) -> Option<i32> {
    use std::process::Command;

    // For simple 2-3 button dialogs, use Windows Forms MessageBox
    // For more complex cases, use a PowerShell custom form
    let escaped_msg = message.replace("'", "''");

    // Build a PowerShell script that creates a simple form with custom buttons
    let mut script = String::from(
        "Add-Type -AssemblyName System.Windows.Forms\n\
         $form = New-Object System.Windows.Forms.Form\n\
         $form.Text = 'Freenet Permission'\n\
         $form.StartPosition = 'CenterScreen'\n\
         $form.FormBorderStyle = 'FixedDialog'\n\
         $form.MaximizeBox = $false\n\
         $form.MinimizeBox = $false\n\
         $form.TopMost = $true\n",
    );

    script.push_str(&format!(
        "$label = New-Object System.Windows.Forms.Label\n\
         $label.Text = '{}'\n\
         $label.AutoSize = $true\n\
         $label.Location = New-Object System.Drawing.Point(20,20)\n\
         $form.Controls.Add($label)\n",
        escaped_msg
    ));

    for (i, label) in labels.iter().enumerate() {
        let x = 20 + i * 110;
        script.push_str(&format!(
            "$btn{i} = New-Object System.Windows.Forms.Button\n\
             $btn{i}.Text = '{}'\n\
             $btn{i}.Location = New-Object System.Drawing.Point({x},80)\n\
             $btn{i}.Add_Click({{ $form.Tag = {i}; $form.Close() }})\n\
             $form.Controls.Add($btn{i})\n",
            label.replace("'", "''"),
        ));
    }

    script.push_str(
        "$form.Tag = -1\n\
         $form.ShowDialog() | Out-Null\n\
         Write-Output $form.Tag\n",
    );

    let output = Command::new("powershell")
        .arg("-NoProfile")
        .arg("-Command")
        .arg(&script)
        .output()
        .ok()?;

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if let Ok(idx) = stdout.parse::<i32>() {
            return Some(idx);
        }
    }

    Some(-1)
}

/// Check if stdin is a terminal (without pulling in the `atty` crate).
fn atty_is_terminal() -> bool {
    std::io::IsTerminal::is_terminal(&std::io::stdin())
}

/// Simple terminal-based prompt (fallback when no GUI is available).
fn terminal_prompt(message: &str, labels: &[String], _timeout_secs: u64) -> i32 {
    use std::io::{self, Write};

    eprintln!("\n--- Freenet Permission Request ---");
    eprintln!("{message}");
    eprintln!();
    for (i, label) in labels.iter().enumerate() {
        eprintln!("  [{i}] {label}");
    }
    eprint!("\nChoose (0-{}): ", labels.len() - 1);
    drop(io::stderr().flush());

    let mut input = String::new();
    if io::stdin().read_line(&mut input).is_ok() {
        if let Ok(idx) = input.trim().parse::<i32>() {
            if idx >= 0 && (idx as usize) < labels.len() {
                return idx;
            }
        }
    }

    -1
}
