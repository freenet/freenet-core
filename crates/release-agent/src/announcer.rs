//! River announcement runner.
//!
//! Posts a release announcement to the Freenet Official River room by
//! invoking a small `announce-to-river.sh` shell script via `sudo -n`.
//! The script lives on nova alongside the room owner signing key — both
//! resources are owned by the `ian` user and never leave the box. The
//! agent passes the announcement text on argv; the script is responsible
//! for converting it into a `riverctl` invocation with the right paths.
//!
//! This module is structurally parallel to [`crate::updater::Updater`]:
//! same `dry_run` short-circuit, same sudo-wrapping, same 1-second
//! early-exit probe so an immediate sudo rejection surfaces as `Err`.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::process::Command;

#[derive(Clone, Debug)]
pub struct Announcer {
    /// Path to the river-announce script (e.g.
    /// `/usr/local/bin/announce-to-river.sh`). Empty path means the
    /// endpoint is disabled (returns 503).
    pub command: PathBuf,
    pub dry_run: bool,
    /// Privilege-escalation wrapper. Production: `sudo`. Tests inject a
    /// fake-sudo script that records argv.
    pub sudo_command: PathBuf,
    /// The user under which `riverctl` must run (typically `ian` on
    /// nova, since the signing key + rooms.json live in their home dir).
    pub run_as_user: String,
}

const EARLY_EXIT_PROBE: Duration = Duration::from_secs(1);

impl Announcer {
    pub fn new_with_sudo(command: PathBuf, dry_run: bool, run_as_user: impl Into<String>) -> Self {
        Self {
            command,
            dry_run,
            sudo_command: PathBuf::from("sudo"),
            run_as_user: run_as_user.into(),
        }
    }

    pub fn is_configured(&self) -> bool {
        !self.command.as_os_str().is_empty()
    }

    /// Invoke the announce script with `message` on argv. Returns `Ok` if
    /// the script either kept running past the 1s probe or exited 0 within
    /// it; returns `Err` for immediate non-zero exit so the HTTP layer
    /// returns 500 and the caller can retry.
    ///
    /// `target_version`, when `Some`, is the release version this
    /// announcement is for (bare semver, no leading `v`). It is appended to the
    /// script's argv as `--target-version <v>` (see below for why a flag rather
    /// than an env var) so `announce-to-river.sh` can gate the post on the
    /// RESTARTED node reporting that version — the deterministic fix for the
    /// #4496 announce/update race. `None` (a caller that predates the plumbing)
    /// omits the flag and the script falls back to its read-streak heuristic.
    pub async fn run(&self, message: &str, target_version: Option<&str>) -> Result<()> {
        if self.dry_run {
            tracing::info!(
                command = %self.command.display(),
                len = message.len(),
                target_version = target_version.unwrap_or("<none>"),
                "dry-run: would invoke `sudo -n -u {} {} <message>`",
                self.run_as_user,
                self.command.display()
            );
            return Ok(());
        }

        tracing::info!(
            command = %self.command.display(),
            len = message.len(),
            target_version = target_version.unwrap_or("<none>"),
            "spawning announce command"
        );

        let mut cmd = Command::new(&self.sudo_command);
        cmd.arg("--non-interactive")
            .arg("-u")
            .arg(&self.run_as_user);
        // The target version is forwarded as a `--target-version <v>` flag on
        // the SCRIPT's own argv, not via env. The sudoers rule pins the
        // command to the script path with a trailing `*` wildcard that matches
        // the whole argv, so extra script arguments are allowed — but a
        // `sudo env VAR=... script` prefix would change the matched command to
        // `env` and break the sudoers match, and sudo strips the process
        // environment by default so a plain `.env()` would not survive either.
        // Passing it as a script flag keeps the sudoers match on the script
        // and reaches the shell as a normal positional (no shell eval — each
        // `.arg` is its own execve slot).
        cmd.arg(&self.command).arg(message);
        if let Some(v) = target_version {
            cmd.arg("--target-version").arg(v);
        }
        let mut child = cmd
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .kill_on_drop(false)
            .spawn()
            .context("spawn announce command")?;

        match tokio::time::timeout(EARLY_EXIT_PROBE, child.wait()).await {
            Ok(Ok(status)) if !status.success() => {
                anyhow::bail!("announce command exited early with {status}");
            }
            Ok(Err(e)) => {
                anyhow::bail!("wait() on announce command failed: {e}");
            }
            Ok(Ok(_)) => {}
            Err(_timeout) => {
                let cmd_path = self.command.clone();
                tokio::spawn(async move {
                    match child.wait().await {
                        Ok(status) => tracing::info!(
                            %status,
                            command = %cmd_path.display(),
                            "announce command exited"
                        ),
                        Err(e) => tracing::error!(
                            error = %e,
                            command = %cmd_path.display(),
                            "wait() on announce command failed"
                        ),
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
        let a = Announcer::new_with_sudo(PathBuf::from("/nonexistent/announce.sh"), true, "nobody");
        a.run("test message", Some("0.2.90"))
            .await
            .expect("dry_run must not invoke");
    }

    #[test]
    fn empty_command_is_unconfigured() {
        let a = Announcer::new_with_sudo(PathBuf::new(), false, "ian");
        assert!(!a.is_configured());
    }
}
