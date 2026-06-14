use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
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

/// RAII guard proving an update is currently in flight (its `stop`/`restart`
/// of the gateway service has been kicked off and has not yet completed).
///
/// The release-agent holds exactly one of these per managed gateway for the
/// **whole** duration of an update — from the moment the update script is
/// spawned until the child process exits (or a bounded max-hold timeout
/// fires). A second `POST /update` that arrives while a guard is alive cannot
/// acquire one, so the handler rejects it with `409 Conflict` rather than
/// spawning a second `systemctl stop/restart`.
///
/// WHY: during the 0.2.65 rollout, two update commands arrived ~1s apart.
/// The first spawned a restart whose *old* process took ~90s to die (past the
/// 45s final-SIGTERM timeout); the second update's `systemctl stop` then
/// SIGKILLed the freshly-started *new* process, leaving nova's gateway DOWN
/// until a manual `systemctl start`. The 1s early-exit probe in
/// [`Updater::run`] is far too short to cover that restart window, and the
/// rate-limit mutex only gates *timing*, not *overlap*. This guard closes the
/// overlap window. See issue #4271.
///
/// The boolean is reset to `false` on `Drop`, so the guard is released on
/// completion, on early/immediate failure, AND if the owning task panics —
/// a crashed update can never wedge the agent permanently.
#[derive(Debug)]
pub struct InFlightGuard {
    flag: Arc<AtomicBool>,
}

impl InFlightGuard {
    /// Try to claim the in-flight slot. Returns `Some(guard)` if no update was
    /// in flight (the slot was `false` and is now `true`), or `None` if an
    /// update is already running and the caller must back off.
    ///
    /// Uses a compare-and-swap so two concurrent callers can never both win.
    pub fn try_acquire(flag: &Arc<AtomicBool>) -> Option<Self> {
        match flag.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => Some(Self {
                flag: Arc::clone(flag),
            }),
            Err(_) => None,
        }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
    }
}

/// How long to wait synchronously after spawning the update script before
/// returning success to the caller. Legitimate updates take tens of seconds
/// (download + service restart); immediate failures (sudoers misconfig,
/// script not executable, sudo rejection) surface within milliseconds. One
/// second is a comfortable separator that lets the caller see a 5xx for the
/// failure case instead of a falsely-successful 202.
const EARLY_EXIT_PROBE: Duration = Duration::from_secs(1);

/// Absolute ceiling on how long the [`InFlightGuard`] is held while waiting
/// for the spawned update child to exit. The guard MUST outlive the real
/// `systemctl restart` window (old process drain + new process startup),
/// which the nova outage showed can run past 90s — but it must NOT be
/// unbounded, or a hung/zombied update would wedge the agent against all
/// future updates forever. This mirrors the project rule that any lock/GC
/// exemption be time-bounded.
///
/// 5 minutes comfortably covers `TimeoutStopSec` (45s final SIGTERM) plus the
/// observed ~90s drain plus new-process startup, with generous headroom,
/// while still guaranteeing the slot frees within a bounded window if the
/// update never reports completion.
const MAX_UPDATE_HOLD: Duration = Duration::from_secs(300);

impl Updater {
    /// Invoke the update script for `target_version`, or log the intent if
    /// dry-run. Returns `Ok` once the script has either started running
    /// (legitimate path) or exited 0 within the early-exit probe window.
    /// An immediate non-zero exit (sudo rejection, etc.) is surfaced as
    /// `Err` so the HTTP layer can return a non-2xx and the caller can
    /// retry rather than be falsely rate-limited.
    ///
    /// `in_flight` is the caller's [`InFlightGuard`], proving no other update
    /// is running. On the legitimate "still running" path it is moved into the
    /// background wait task so the in-flight slot stays claimed for the WHOLE
    /// restart window (bounded by [`MAX_UPDATE_HOLD`]), not just the 1s probe —
    /// this is what prevents a second update from race-killing the freshly
    /// started process (issue #4271). On dry-run, immediate exit, or spawn
    /// failure the guard drops here, freeing the slot right away.
    pub async fn run(&self, target_version: &Version, in_flight: InFlightGuard) -> Result<()> {
        let target_arg = format!("v{target_version}");

        if self.dry_run {
            tracing::info!(
                target = %target_version,
                command = %self.command.display(),
                "dry-run: would invoke `sudo --non-interactive {} --force --target-version {}`",
                self.command.display(),
                target_arg
            );
            // Drop `in_flight` explicitly: a dry-run does nothing, so the slot
            // must free immediately rather than linger.
            drop(in_flight);
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
                //
                // The in-flight guard is MOVED into this task so the slot
                // stays claimed for the whole restart window — any second
                // /update arriving meanwhile is rejected with 409 instead of
                // spawning a racing `systemctl stop` (issue #4271). The wait
                // is bounded by MAX_UPDATE_HOLD so a hung child can't pin the
                // slot forever; the guard drops when this task ends, whether
                // by clean exit, timeout, or panic.
                let cmd_path = self.command.clone();
                let target = target_version.to_string();
                tokio::spawn(async move {
                    let _in_flight = in_flight;
                    match tokio::time::timeout(MAX_UPDATE_HOLD, child.wait()).await {
                        Ok(Ok(status)) => {
                            tracing::info!(
                                %status,
                                target,
                                command = %cmd_path.display(),
                                "update command exited"
                            );
                        }
                        Ok(Err(e)) => {
                            tracing::error!(
                                error = %e,
                                target,
                                command = %cmd_path.display(),
                                "wait() on update command failed"
                            );
                        }
                        Err(_elapsed) => {
                            tracing::warn!(
                                target,
                                command = %cmd_path.display(),
                                max_hold_secs = MAX_UPDATE_HOLD.as_secs(),
                                "update command still running past max-hold; \
                                 releasing in-flight slot so future updates aren't blocked"
                            );
                        }
                    }
                    // `_in_flight` drops here, freeing the slot.
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
        let flag = Arc::new(AtomicBool::new(false));
        let guard = InFlightGuard::try_acquire(&flag).expect("slot is free");
        updater
            .run(&Version::new(0, 2, 56), guard)
            .await
            .expect("dry_run must not invoke the command");
        assert!(
            !flag.load(Ordering::Acquire),
            "dry-run must release the in-flight slot immediately"
        );
    }

    #[tokio::test]
    async fn in_flight_guard_is_single_acquire() {
        // Second acquire while the first guard is alive must fail; once the
        // first drops, the slot frees again.
        let flag = Arc::new(AtomicBool::new(false));
        let first = InFlightGuard::try_acquire(&flag).expect("first acquire succeeds");
        assert!(
            InFlightGuard::try_acquire(&flag).is_none(),
            "second concurrent acquire must be rejected"
        );
        drop(first);
        assert!(
            InFlightGuard::try_acquire(&flag).is_some(),
            "slot must free once the guard drops"
        );
    }

    // Live-spawn coverage lives in the integration test
    // `tests/update_handler.rs`, which exercises the full request →
    // validation → spawn pipeline against a stub script (no sudo).
}
