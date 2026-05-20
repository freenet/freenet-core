use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result};
use semver::Version;
use tokio::process::Command;

#[derive(Clone, Debug)]
pub struct Updater {
    pub command: PathBuf,
    pub dry_run: bool,
    /// The privilege-escalation prefix. In production this is `"sudo"`
    /// (resolved via PATH and constrained by sudoers). Tests can point
    /// this at a fake-sudo script under the test's tempdir to exercise
    /// the spawn pipeline without needing real root.
    pub sudo_command: PathBuf,
}

impl Updater {
    pub fn new_with_sudo(command: PathBuf, dry_run: bool) -> Self {
        Self {
            command,
            dry_run,
            sudo_command: PathBuf::from("sudo"),
        }
    }
}

/// How long to wait synchronously after spawning the update script before
/// returning success to the caller. Legitimate updates take tens of seconds
/// (download + service restart); immediate failures (sudoers misconfig,
/// script not executable, sudo rejection) surface within milliseconds. One
/// second is a comfortable separator that lets the caller see a 5xx for the
/// failure case instead of a falsely-successful 202.
const EARLY_EXIT_PROBE: Duration = Duration::from_secs(1);

impl Updater {
    /// Invoke the update script for `target_version`, or log the intent if
    /// dry-run. Returns `Ok` once the script has either started running
    /// (legitimate path) or exited 0 within the early-exit probe window.
    /// An immediate non-zero exit (sudo rejection, etc.) is surfaced as
    /// `Err` so the HTTP layer can return a non-2xx and the caller can
    /// retry rather than be falsely rate-limited.
    pub async fn run(&self, target_version: &Version) -> Result<()> {
        let target_arg = format!("v{target_version}");

        if self.dry_run {
            tracing::info!(
                target = %target_version,
                command = %self.command.display(),
                "dry-run: would invoke `sudo --non-interactive {} --force --target-version {}`",
                self.command.display(),
                target_arg
            );
            return Ok(());
        }

        tracing::info!(
            target = %target_version,
            command = %self.command.display(),
            "spawning update command"
        );

        // stdout/stderr inherit the agent's fds, which under systemd point
        // at journald — so the script's logs end up in the agent's journal
        // unit. Previously they went to /dev/null, which was a debugging
        // blind spot when the script failed.
        let mut child = Command::new(&self.sudo_command)
            .arg("--non-interactive")
            .arg(&self.command)
            .arg("--force")
            .arg("--target-version")
            .arg(&target_arg)
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .kill_on_drop(false)
            .spawn()
            .context("spawn update command")?;

        match tokio::time::timeout(EARLY_EXIT_PROBE, child.wait()).await {
            Ok(Ok(status)) if !status.success() => {
                anyhow::bail!("update command exited early with {status}");
            }
            Ok(Err(e)) => {
                anyhow::bail!("wait() on update command failed: {e}");
            }
            Ok(Ok(_)) => {
                // Exited 0 within the probe window — unusual but not an
                // error (script could have decided it was already
                // up-to-date even with --force, depending on its checks).
            }
            Err(_timeout) => {
                // Still running — the expected path. Hand off to a
                // background task that logs the eventual exit; the caller
                // polls /version to confirm the new binary is installed.
                let cmd_path = self.command.clone();
                let target = target_version.to_string();
                tokio::spawn(async move {
                    match child.wait().await {
                        Ok(status) => {
                            tracing::info!(
                                %status,
                                target,
                                command = %cmd_path.display(),
                                "update command exited"
                            );
                        }
                        Err(e) => {
                            tracing::error!(
                                error = %e,
                                target,
                                command = %cmd_path.display(),
                                "wait() on update command failed"
                            );
                        }
                    }
                });
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dry_run_does_not_execute() {
        // Point at a file that doesn't exist. If dry_run is honoured, run()
        // succeeds without touching the filesystem; if not, spawn() would
        // fail and the test would catch the regression.
        let updater = Updater::new_with_sudo(
            PathBuf::from("/nonexistent/path/that/should/never/exist"),
            true,
        );
        updater
            .run(&Version::new(0, 2, 56))
            .await
            .expect("dry_run must not invoke the command");
    }

    // Live-spawn coverage lives in the integration test
    // `tests/update_handler.rs`, which exercises the full request →
    // validation → spawn pipeline against a stub script (no sudo).
}
