use std::path::PathBuf;
use std::process::Stdio;

use anyhow::{Context, Result};
use tokio::process::Command;

#[derive(Clone, Debug)]
pub struct Updater {
    pub command: PathBuf,
    pub dry_run: bool,
}

impl Updater {
    pub async fn run(&self) -> Result<()> {
        if self.dry_run {
            tracing::info!(
                command = %self.command.display(),
                "dry-run: would invoke `sudo --non-interactive {} --force`",
                self.command.display()
            );
            return Ok(());
        }

        // Spawn detached: the gateway-auto-update.sh process restarts the
        // freenet-gateway systemd unit, which is independent of the agent's
        // process tree. We don't await completion — the caller polls
        // /version to confirm the new binary is installed.
        let mut child = Command::new("sudo")
            .arg("--non-interactive")
            .arg(&self.command)
            .arg("--force")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(false)
            .spawn()
            .with_context(|| {
                format!(
                    "spawn `sudo --non-interactive {} --force`",
                    self.command.display()
                )
            })?;

        let cmd_path = self.command.clone();
        tokio::spawn(async move {
            match child.wait().await {
                Ok(status) => {
                    tracing::info!(%status, command = %cmd_path.display(), "update command exited");
                }
                Err(e) => {
                    tracing::error!(error = %e, command = %cmd_path.display(), "wait() on update command failed");
                }
            }
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[tokio::test]
    async fn dry_run_does_not_execute() {
        // Point at a file that doesn't exist. If dry_run is honoured, run()
        // succeeds without touching the filesystem; if not, spawn() would
        // fail and the test would catch the regression.
        let updater = Updater {
            command: PathBuf::from("/nonexistent/path/that/should/never/exist"),
            dry_run: true,
        };
        updater
            .run()
            .await
            .expect("dry_run must not invoke the command");
    }
}
