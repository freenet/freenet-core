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
//!
//! # Security
//!
//! The message and button labels originate from untrusted delegate WASM code.
//! All platform dialog implementations sanitize inputs to prevent command injection:
//! - **Linux**: Arguments passed via `Command::arg()` (no shell interpretation)
//! - **macOS**: Message piped via stdin to avoid AppleScript injection
//! - **Windows**: Message and labels written to a temp file read by PowerShell

use clap::Args;

/// Maximum length for message and button labels to prevent abuse.
const MAX_MESSAGE_LEN: usize = 2048;
const MAX_LABEL_LEN: usize = 64;
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
    if stdin_is_terminal() {
        return terminal_prompt(message, labels, timeout_secs);
    }

    // Headless: auto-deny
    -1
}

/// Try to show a dialog using zenity or kdialog (Linux).
#[cfg(target_os = "linux")]
fn try_linux_dialog(message: &str, labels: &[String]) -> Option<i32> {
    if let Some(idx) = try_zenity(message, labels) {
        return Some(idx);
    }
    if let Some(idx) = try_kdialog(message, labels) {
        return Some(idx);
    }
    None
}

/// Show a dialog via zenity. All arguments passed via `Command::arg()` which
/// uses execvp -- no shell interpretation, so no injection risk.
#[cfg(target_os = "linux")]
fn try_zenity(message: &str, labels: &[String]) -> Option<i32> {
    use std::process::Command;

    if Command::new("zenity").arg("--version").output().is_err() {
        return None;
    }

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
        return Some(0);
    }

    // Extra buttons: zenity exits with code 1 AND writes the button text to stdout.
    // Check stdout for extra button text BEFORE falling back to Cancel interpretation,
    // since both extra buttons and Cancel use exit code 1.
    if !stdout.is_empty() {
        for (i, label) in labels.iter().enumerate() {
            if stdout == *label {
                return Some(i as i32);
            }
        }
    }

    // Exit code 1 with no stdout = Cancel button (mapped to last label)
    if output.status.code() == Some(1) && labels.len() >= 2 {
        return Some(labels.len() as i32 - 1);
    }

    Some(-1)
}

#[cfg(target_os = "linux")]
fn try_kdialog(message: &str, labels: &[String]) -> Option<i32> {
    use std::process::Command;

    if Command::new("kdialog").arg("--version").output().is_err() {
        return None;
    }

    // kdialog supports --yesno (2 buttons) and --yesnocancel (3 buttons).
    // For 4+ buttons, falls through to terminal prompt.
    let mut cmd = Command::new("kdialog");
    cmd.arg("--title").arg("Freenet Permission");

    match labels.len() {
        1 => {
            cmd.arg("--msgbox").arg(message);
            let status = cmd.status().ok()?;
            Some(if status.success() { 0 } else { -1 })
        }
        2 => {
            cmd.arg("--yesno")
                .arg(message)
                .arg("--yes-label")
                .arg(&labels[0])
                .arg("--no-label")
                .arg(&labels[1]);
            let status = cmd.status().ok()?;
            Some(if status.success() { 0 } else { 1 })
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
            match status.code() {
                Some(0) => Some(0),
                Some(1) => Some(1),
                Some(2) => Some(2),
                _ => Some(-1),
            }
        }
        _ => None,
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
    // The AppleScript reads stdin, so the message never appears in the script text.
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

    // Write message and labels to a temp file to avoid PowerShell injection
    let data = serde_json::json!({
        "message": message,
        "labels": labels,
    });

    let temp_dir = std::env::temp_dir();
    let data_file = temp_dir.join(format!("freenet-prompt-{}.json", std::process::id()));
    let mut f = std::fs::File::create(&data_file).ok()?;
    drop(f.write_all(data.to_string().as_bytes()));
    drop(f);

    // PowerShell script reads from the JSON file -- no user-controlled string interpolation
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

    // Clean up temp file
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
