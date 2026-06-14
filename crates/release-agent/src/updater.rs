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
    ///
    /// The success ordering is `Acquire` (not `AcqRel`): the meaningful
    /// publish — making the slot available again — happens on `Drop` via the
    /// `Release` store below, so the CAS only needs to *observe* the prior
    /// release, not itself publish anything. `Acquire` pairs with that
    /// `Release` and is sufficient.
    pub fn try_acquire(flag: &Arc<AtomicBool>) -> Option<Self> {
        match flag.compare_exchange(false, true, Ordering::Acquire, Ordering::Acquire) {
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
///
/// Public so config validation can warn when `rate_limit_seconds` is set below
/// this value, which would remove the rate-limit backstop that covers the
/// residual re-open window if an update runs past the max-hold (see the timeout
/// arm in [`Updater::run`] and `Config::warn_if_rate_limit_below_max_hold`).
pub const MAX_UPDATE_HOLD: Duration = Duration::from_secs(300);

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
                    // LOAD-BEARING: this binding keeps the InFlightGuard alive
                    // for the WHOLE `child.wait()` below. Its scope IS the
                    // in-flight window — the slot stays claimed until this task
                    // ends (clean exit, timeout, or panic), at which point the
                    // guard drops and releases the slot. Do NOT narrow its scope
                    // (e.g. `drop(in_flight)` early, or move the wait out from
                    // under it): that would re-open the #4271 overlap window,
                    // letting a second POST acquire the slot and spawn a racing
                    // `systemctl stop` while this update's restart is still in
                    // progress.
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
                            // RESIDUAL RE-OPEN WINDOW (documented intentionally):
                            // we free the in-flight slot here even though the
                            // child is STILL running (kill_on_drop=false leaves
                            // it alive). If a real update genuinely runs past
                            // MAX_UPDATE_HOLD, a second POST could now acquire the
                            // slot and spawn a racing `systemctl stop` — the exact
                            // #4271 double-stop. The time-bound is mandatory (an
                            // unbounded exemption would let a hung/zombied update
                            // wedge the agent against ALL future updates forever,
                            // violating the project rule that lock/GC exemptions
                            // be time-bounded), so we accept this narrow residual
                            // window as the lesser evil.
                            //
                            // SECOND BACKSTOP: the rate-limit window
                            // (`rate_limit_seconds`, prod default 600s) gates how
                            // OFTEN updates may start. With the default it is >
                            // MAX_UPDATE_HOLD (300s), so a second POST in this
                            // window is rejected by the rate-limiter (429) before
                            // it can reach the in-flight check — closing the gap.
                            // Config load emits a `warn!` if an operator sets
                            // rate_limit_seconds < MAX_UPDATE_HOLD, which would
                            // remove this backstop. See `Config::warn_if_*`.
                            //
                            // WHY 300s: it comfortably covers TimeoutStopSec (45s
                            // final SIGTERM) + the observed ~90s nova drain + new-
                            // process startup, with generous headroom — a >300s
                            // update is an operational anomaly, not a normal slow
                            // restart, hence the `error!` level below.
                            tracing::error!(
                                target,
                                command = %cmd_path.display(),
                                max_hold_secs = MAX_UPDATE_HOLD.as_secs(),
                                "update command still running past max-hold; \
                                 releasing in-flight slot so future updates aren't \
                                 blocked. If rate_limit_seconds < max-hold the \
                                 overlap guard is no longer backstopped — a second \
                                 update could now race this one's restart (#4271)"
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

    /// Write an executable stub that, when invoked, signals via `started_marker`
    /// that `/bin/sh` is already running the script body, then becomes a real
    /// subprocess which sleeps far past `MAX_UPDATE_HOLD` so the spawned child
    /// NEVER exits within the hold window. Returns the stub's path (used as
    /// `sudo_command`).
    ///
    /// The wall-clock sleep is intentional: under `start_paused` a real OS
    /// child's sleep does NOT honour virtual time, so the child stays alive
    /// across `tokio::time::advance`. That is exactly the condition the
    /// MAX_UPDATE_HOLD timeout arm exists for — the tokio timer fires on the
    /// virtual-time advance while the child is still running.
    ///
    /// The `started_marker` write is the teardown-noise guard: once it exists,
    /// `/bin/sh` has already opened and read this (tiny) script into memory and
    /// `exec`'d into `sleep`, so the test can drop its TempDir without the
    /// detached child (`kill_on_drop=false`) ever re-opening the now-deleted
    /// script and logging `No such file or directory` to stderr.
    fn write_never_exiting_stub(
        dir: &std::path::Path,
        started_marker: &std::path::Path,
    ) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let bin = dir.join("fake-sudo-sleep");
        // The agent always prepends `--non-interactive`; ignore argv. Touch the
        // started marker, then exec `sleep` for 30 REAL wall-clock seconds. The
        // test advances virtual time instantly and finishes in milliseconds of
        // wall-clock, so 30s keeps the child alive across the (instant) virtual
        // MAX_UPDATE_HOLD = 300s advance, yet self-reaps shortly after so the
        // detached child doesn't linger as a long-lived orphan.
        let script = format!(
            "#!/bin/sh\n: > {}\nexec sleep 30\n",
            started_marker.display()
        );
        std::fs::write(&bin, script).unwrap();
        let mut perms = std::fs::metadata(&bin).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&bin, perms).unwrap();
        bin
    }

    /// The MAX_UPDATE_HOLD timeout arm in [`Updater::run`] frees the in-flight
    /// slot even while the spawned child is STILL running — the documented
    /// "residual re-open window". This is the only path that releases the guard
    /// without the child exiting, and the comment itself flags it as the spot
    /// that can re-enable the #4271 overlap bug, so it must be covered.
    ///
    /// Virtual time (`start_paused`) lets us fire the timeout deterministically:
    /// the stub child sleeps in real wall-clock (a real subprocess does NOT
    /// honour paused time), so it never exits during the test, while
    /// `tokio::time::advance(MAX_UPDATE_HOLD + …)` trips the `tokio::time::timeout`
    /// that wraps `child.wait()` in the background task — exactly the
    /// `Err(_elapsed)` arm under test.
    ///
    /// LOAD-BEARING: the assertion is that the flag returns to `false` AFTER the
    /// advance. If the timeout-release were removed (the guard held until the
    /// child exits, which never happens here), the flag would stay `true` and
    /// this test would hang at the poll deadline and fail — proving the arm is
    /// what frees the slot.
    #[tokio::test(start_paused = true)]
    async fn max_update_hold_timeout_releases_in_flight_slot() {
        let tmp = tempfile::tempdir().unwrap();
        let started_marker = tmp.path().join("child-started");
        let sudo = write_never_exiting_stub(tmp.path(), &started_marker);

        let updater = Updater {
            // `command` is only ever an argv element here (the stub ignores it),
            // so any path works; the never-exiting behaviour comes from `sudo`.
            command: tmp.path().join("update.sh"),
            dry_run: false,
            sudo_command: sudo,
        };

        let flag = Arc::new(AtomicBool::new(false));
        let guard = InFlightGuard::try_acquire(&flag).expect("slot is free");

        // run() spawns the real (sleeping) child. The 1s EARLY_EXIT_PROBE timer
        // auto-fires under paused time (the child never exits, so the only
        // pending work is the timer), so run() takes the "still running" branch:
        // it moves the guard into the background wait task and returns Ok.
        updater
            .run(&Version::new(0, 2, 56), guard)
            .await
            .expect("spawn of the sleeping stub must succeed");

        // The slot is still claimed: the background task holds the guard while
        // it waits (bounded by MAX_UPDATE_HOLD) for the child that never exits.
        assert!(
            flag.load(Ordering::Acquire),
            "slot must stay claimed while the update is in flight"
        );

        // Let the background wait task get scheduled and park on its
        // `tokio::time::timeout(MAX_UPDATE_HOLD, child.wait())` so its timer is
        // registered before we advance virtual time.
        tokio::task::yield_now().await;

        // Fire the MAX_UPDATE_HOLD timeout: advance virtual time just past the
        // hold. The real child keeps sleeping (wall-clock), so the only way the
        // slot can free is the `Err(_elapsed)` timeout arm dropping the guard.
        tokio::time::advance(MAX_UPDATE_HOLD + Duration::from_millis(1)).await;

        // Let the background task observe the elapsed timeout and drop the guard.
        // Bounded poll so the test fails (rather than hangs forever) if the
        // timeout-release is ever removed. `sleep` cooperates with paused-time
        // auto-advance and yields to the background task between checks.
        let mut released = false;
        for _ in 0..1_000 {
            if !flag.load(Ordering::Acquire) {
                released = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(
            released,
            "MAX_UPDATE_HOLD timeout arm must release the in-flight slot even \
             though the child is still running (residual re-open window, #4271)"
        );

        // And the slot is now genuinely free: a fresh acquire succeeds, i.e. a
        // follow-up update would be accepted rather than 409-rejected.
        assert!(
            InFlightGuard::try_acquire(&flag).is_some(),
            "after the timeout fires the slot must be re-acquirable"
        );

        // Teardown-noise guard: wait until the stub child has `exec`'d into
        // `sleep` (it writes `started_marker` immediately before doing so) so
        // dropping `tmp` below doesn't delete the script out from under a child
        // that hasn't opened it yet — which would log `No such file or
        // directory` to stderr. The child runs on the OS independently of the
        // paused runtime; a busy `yield_now` poll lets it make progress without
        // depending on (paused) tokio time. Bounded so a genuinely stuck child
        // fails the test rather than hanging.
        let mut child_running = false;
        for _ in 0..100_000 {
            if started_marker.exists() {
                child_running = true;
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            child_running,
            "stub child should have started (and written its marker) by now"
        );
        drop(tmp);
    }

    // Live-spawn coverage lives in the integration test
    // `tests/update_handler.rs`, which exercises the full request →
    // validation → spawn pipeline against a stub script (no sudo).
}
