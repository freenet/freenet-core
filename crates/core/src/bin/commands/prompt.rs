//! `freenet prompt` subcommand: show a native permission dialog to the user.
//!
//! This binary subcommand is spawned by the node (via `SubprocessPrompter`) when
//! a delegate emits `RequestUserInput`. It gets its own process and main thread,
//! avoiding event loop conflicts with the node's tokio runtime.
//!
//! Strategy per platform:
//! - **Linux**: Native GTK3 dialog (dynamically links to libgtk-3, near-universal on desktops)
//! - **macOS**: osascript (native Cocoa dialog via AppleScript, always available)
//! - **Windows**: PowerShell WinForms (always available)
//! - **Fallback**: stdin/stdout terminal prompt (if TTY available)
//! - **Headless**: prints -1 (deny) if no dialog mechanism is available
//!
//! Prints the selected button index (0-based) to stdout, or -1 on deny/timeout/dismiss.
//!
//! # Security
//!
//! The message and button labels originate from untrusted delegate WASM code.
//! All platform dialog implementations sanitize inputs to prevent injection:
//! - **Linux**: GTK API takes strings directly (no shell/script interpretation)
//! - **macOS**: Message piped via stdin to avoid AppleScript injection
//! - **Windows**: Message and labels written to a temp file read by PowerShell

use clap::Args;

/// Maximum message length. 2KB is enough for a detailed permission description
/// while preventing a malicious delegate from filling the screen or exhausting
/// CLI argument space (~128KB on most OSes).
const MAX_MESSAGE_LEN: usize = 2048;

/// Maximum button label length. 64 chars fits comfortably in a dialog button
/// on all platforms without truncation.
const MAX_LABEL_LEN: usize = 64;

/// Maximum number of response buttons. 10 keeps the dialog usable; more choices
/// would overwhelm the user and may not fit in dialog layouts.
const MAX_LABELS: usize = 10;

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

        // Sanitize inputs from untrusted delegate WASM
        let message = sanitize_message(&self.message);
        let labels: Vec<String> = labels
            .into_iter()
            .take(MAX_LABELS)
            .map(|l| sanitize_label(&l))
            .collect();

        let result = show_dialog(&message, &labels, self.timeout);
        println!("{result}");
        Ok(())
    }
}

/// Sanitize a message string from untrusted delegate code.
/// Truncates to MAX_MESSAGE_LEN and strips control characters.
fn sanitize_message(msg: &str) -> String {
    msg.chars()
        .take(MAX_MESSAGE_LEN)
        .filter(|c| !c.is_control() || *c == '\n')
        .collect()
}

/// Sanitize a button label from untrusted delegate code.
/// Truncates to MAX_LABEL_LEN and strips control characters.
fn sanitize_label(label: &str) -> String {
    label
        .chars()
        .take(MAX_LABEL_LEN)
        .filter(|c| !c.is_control())
        .collect()
}

/// Show a dialog and return the selected button index, or -1 on deny/dismiss/timeout.
fn show_dialog(message: &str, labels: &[String], timeout_secs: u64) -> i32 {
    #[cfg(target_os = "linux")]
    if let Some(idx) = try_gtk_dialog(message, labels) {
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
    if stdin_is_terminal() {
        return terminal_prompt(message, labels, timeout_secs);
    }

    // Headless: auto-deny
    -1
}

/// Show a native GTK3 dialog with custom buttons (Linux).
///
/// GTK3 is dynamically linked -- `libgtk-3-0` must be installed at runtime.
/// This is near-universal on Linux desktops (dependency of Firefox, Chrome,
/// GIMP, and most GNOME/XFCE apps; KDE desktops also typically have it for
/// cross-toolkit app support).
///
/// Returns `None` if GTK initialization fails (headless, no DISPLAY, etc.).
#[cfg(target_os = "linux")]
fn try_gtk_dialog(message: &str, labels: &[String]) -> Option<i32> {
    use gtk::prelude::*;
    use gtk::{ButtonsType, DialogFlags, MessageDialog, MessageType, ResponseType, Window};

    if gtk::init().is_err() {
        return None;
    }

    let dialog = MessageDialog::new(
        None::<&Window>,
        DialogFlags::MODAL,
        MessageType::Question,
        ButtonsType::None,
        message,
    );
    dialog.set_title("Freenet Permission");

    // Add custom buttons with response IDs matching their index
    for (i, label) in labels.iter().enumerate() {
        dialog.add_button(label, ResponseType::Other(i as u16));
    }

    // Set the first button as default
    dialog.set_default_response(ResponseType::Other(0));

    let response = dialog.run();
    dialog.close();

    // Process pending GTK events to ensure the dialog is fully destroyed
    while gtk::events_pending() {
        gtk::main_iteration();
    }

    match response {
        ResponseType::Other(idx) => Some(idx as i32),
        _ => Some(-1), // Dialog closed without clicking a button
    }
}

/// Show a dialog via osascript (macOS).
///
/// The message is passed via stdin to avoid AppleScript injection. Button labels
/// are sanitized (alphanumeric + spaces only) before interpolation.
#[cfg(target_os = "macos")]
fn try_macos_dialog(message: &str, labels: &[String]) -> Option<i32> {
    use std::io::Write;
    use std::process::{Command, Stdio};

    // Sanitize labels for AppleScript: only allow safe characters
    let safe_labels: Vec<String> = labels
        .iter()
        .map(|l| {
            l.chars()
                .filter(|c| c.is_alphanumeric() || *c == ' ' || *c == '-' || *c == '_')
                .collect::<String>()
        })
        .collect();

    let button_list = safe_labels
        .iter()
        .map(|l| format!("\"{l}\""))
        .collect::<Vec<_>>()
        .join(", ");

    // Read the message from stdin to avoid injection via string interpolation.
    let script = format!(
        r#"set msg to do shell script "cat"
display dialog msg buttons {{{button_list}}} default button 1 with title "Freenet Permission" with icon caution"#,
    );

    let mut child = Command::new("osascript")
        .arg("-e")
        .arg(&script)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .ok()?;

    if let Some(mut stdin) = child.stdin.take() {
        drop(stdin.write_all(message.as_bytes()));
    }

    let output = child.wait_with_output().ok()?;

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        if let Some(button_text) = stdout.strip_prefix("button returned:") {
            let button_text = button_text.trim();
            for (i, label) in safe_labels.iter().enumerate() {
                if button_text == label {
                    return Some(i as i32);
                }
            }
        }
    }

    Some(-1)
}

/// Show a dialog via PowerShell (Windows).
///
/// The message and labels are passed via a temporary JSON file to avoid
/// PowerShell injection through string interpolation.
#[cfg(target_os = "windows")]
fn try_windows_dialog(message: &str, labels: &[String]) -> Option<i32> {
    use std::io::Write;
    use std::process::Command;

    let data = serde_json::json!({
        "message": message,
        "labels": labels,
    });

    let temp_dir = std::env::temp_dir();
    let data_file = temp_dir.join(format!("freenet-prompt-{}.json", std::process::id()));
    let mut f = std::fs::File::create(&data_file).ok()?;
    drop(f.write_all(data.to_string().as_bytes()));
    drop(f);

    let script = format!(
        r#"
Add-Type -AssemblyName System.Windows.Forms
$data = Get-Content '{}' | ConvertFrom-Json
$form = New-Object System.Windows.Forms.Form
$form.Text = 'Freenet Permission'
$form.StartPosition = 'CenterScreen'
$form.FormBorderStyle = 'FixedDialog'
$form.MaximizeBox = $false
$form.MinimizeBox = $false
$form.TopMost = $true
$form.AutoSize = $true
$form.AutoSizeMode = 'GrowOnly'
$label = New-Object System.Windows.Forms.Label
$label.Text = $data.message
$label.AutoSize = $true
$label.MaximumSize = New-Object System.Drawing.Size(400,0)
$label.Location = New-Object System.Drawing.Point(20,20)
$form.Controls.Add($label)
$i = 0
foreach ($btnLabel in $data.labels) {{
    $btn = New-Object System.Windows.Forms.Button
    $btn.Text = $btnLabel
    $btn.Location = New-Object System.Drawing.Point((20 + $i * 110), 80)
    $btn.Tag = $i
    $btn.Add_Click({{ $form.Tag = $this.Tag; $form.Close() }})
    $form.Controls.Add($btn)
    $i++
}}
$form.Tag = -1
$form.ShowDialog() | Out-Null
Write-Output $form.Tag
"#,
        data_file.display()
    );

    let output = Command::new("powershell")
        .arg("-NoProfile")
        .arg("-Command")
        .arg(&script)
        .output()
        .ok();

    drop(std::fs::remove_file(&data_file));

    let output = output?;

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if let Ok(idx) = stdout.parse::<i32>() {
            return Some(idx);
        }
    }

    Some(-1)
}

fn stdin_is_terminal() -> bool {
    std::io::IsTerminal::is_terminal(&std::io::stdin())
}

/// Simple terminal-based prompt (fallback when no GUI is available).
///
/// Note: `_timeout_secs` is not enforced here; the parent process
/// (`SubprocessPrompter`) kills this subprocess after the timeout.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_message_preserves_normal_text() {
        assert_eq!(sanitize_message("Hello world"), "Hello world");
    }

    #[test]
    fn test_sanitize_message_preserves_newlines() {
        assert_eq!(sanitize_message("Line 1\nLine 2"), "Line 1\nLine 2");
    }

    #[test]
    fn test_sanitize_message_strips_control_chars() {
        assert_eq!(sanitize_message("Hello\x00\x07\x1b world"), "Hello world");
    }

    #[test]
    fn test_sanitize_message_truncates_at_limit() {
        let long_msg = "A".repeat(MAX_MESSAGE_LEN + 500);
        let result = sanitize_message(&long_msg);
        assert_eq!(result.len(), MAX_MESSAGE_LEN);
    }

    #[test]
    fn test_sanitize_label_strips_all_control_chars_including_newlines() {
        assert_eq!(sanitize_label("Allow\nOnce"), "AllowOnce");
        assert_eq!(sanitize_label("OK\x00\x07"), "OK");
    }

    #[test]
    fn test_sanitize_label_truncates_at_limit() {
        let long_label = "B".repeat(MAX_LABEL_LEN + 100);
        let result = sanitize_label(&long_label);
        assert_eq!(result.len(), MAX_LABEL_LEN);
    }

    #[test]
    fn test_sanitize_label_empty() {
        assert_eq!(sanitize_label(""), "");
    }

    #[test]
    fn test_sanitize_message_empty() {
        assert_eq!(sanitize_message(""), "");
    }
}
