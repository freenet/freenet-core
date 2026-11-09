//! The QUIC crash-loop breaker on Windows: count short runs that panicked in
//! QUIC code (#6031).
//!
//! The unix breaker asks whether a run crashed. Windows gives no reliable way
//! to answer that from the outside (a logoff, a Fast Startup shutdown, a
//! session end and a `taskkill` all look alike), so this one asks the run
//! itself: the failure the breaker exists for, a panic inside quinn's driver
//! or in the runtime adapter quinn calls under its lock, always panics before
//! quinn's destructors abort the process. A panic hook ([`record_quic_panics`])
//! records a panic located in quinn, quinn-proto or this crate's QUIC
//! transport ([`quic_attributed`]) in the run's marker. A run with QUIC
//! enabled counts only if it lasted less than [`SHORT_RUN`] and recorded such
//! a panic, and [`SHORT_RUNS_TO_TRIP`] of them in a row keep QUIC off for
//! [`QUIC_BREAKER_COOLDOWN`]. A short run that recorded no QUIC panic (any
//! ordinary stop) starts the count again. A run that started more than the
//! cooldown after the one before it starts the count again too.
//!
//! The first version (#6101) counted every short run, whatever ended it.
//! Replayed over 2.2 days of telemetry from 634 Windows peers, that would
//! have kept QUIC off for a day on 1.3% of them, for ordinary short sessions.
//!
//! This is weaker than the unix breaker in one way: a process death not
//! preceded by a panic (a stack overflow, an out-of-memory abort, an access
//! violation) is not counted. The quinn failure this exists for panics first.
//! And as before, a crash loop whose runs each last [`SHORT_RUN`] or more
//! never trips, and neither does a fault that crashes once a day. See
//! [`super`].
//!
//! # The marker
//!
//! [`Marker::ShortRuns`] in the unix marker's file: the short runs in a row
//! before this run, when this run started, a nonce, and whether this run has
//! recorded a QUIC panic.
//!
//! - **Written** once the endpoint is up, as on unix, so a start that cannot
//!   bind (a second node on the same port) changes nothing.
//! - **Removed** after the run has lasted [`SHORT_RUN`], by a task registered
//!   as [`TIMER_TASK`] ([`start_clear_timer`]), or by a clean exit
//!   ([`super::quic_breaker_clean_exit`]). The clean exit is the update exit
//!   (code 42 from `run_network_node`) and a Ctrl+C. An abort or a kill does
//!   not reach it. A panic in the update-check task does, as on unix: the run
//!   then ends `Ok`. The Ctrl+C path is untested in a real process on Windows:
//!   a test would have to send the console event, which can reach the runner.
//! - **Marked** by the panic hook on the first panic attributed to QUIC code,
//!   but only while the marker is still this run's: after [`SHORT_RUN`] the
//!   timer has removed it, so a later panic finds nothing to mark. The timer
//!   and the clean exit announce the removal first ([`note_marker_cleared`]),
//!   so a panic racing them takes its mark back out.
//! - **Still there at the next start** means the last run lasted less than
//!   [`SHORT_RUN`] and did not reach a clean exit. [`decide`] counts it if it
//!   recorded a QUIC panic and started less than the cooldown ago.
//!
//! # The panic hook
//!
//! Process-global, installed once, when the first Windows marker is written.
//! It calls the hook that was installed before it, so the default panic output
//! (or any other hook) still runs. A panic counts as QUIC's if the thread is
//! inside a [`QuicCode`] guard (quinn's driver polls and the socket adapter's
//! calls, so a panic in a dependency they call counts), or failing that, if
//! its location is in QUIC code ([`quic_attributed`]). It does no logging and takes no lock: it
//! may run on a thread that holds one. It counts every attributed panic, a
//! caught one included (the socket adapter catches its own panics and turns
//! QUIC off for the run, #6027): a fault that recurs at every start is what
//! the breaker is for, whether or not it ends the process.
//!
//! [`decide`] is a pure function of what was read and the clock. Everything
//! here compiles and is tested on every platform: the one thing that is
//! Windows only is the `cfg!(not(unix))` in [`super::gate`] that calls
//! [`gate`].
//!
//! # Where the timer lives
//!
//! In its own task, started when the marker is written, because the run's
//! clock starts there and because it may touch the disk, which belongs in no
//! event loop. The transport passes the node's `LateRegistrar`, as it does for
//! the QUIC accept loop: the monitor is already waiting by the time the
//! transport opens. The monitor reads any end of a registered task as a node
//! failure, so the task parks once its work is done.
//!
//! # Failing open
//!
//! A marker that does not parse, cannot be read, or comes from another
//! platform or a later release counts as no marker: QUIC runs as configured
//! and the count starts again. A clock that has gone back past a marker's time
//! does the same. A marker that cannot be written lets QUIC run with a WARN.
//! The breaker only ever chooses whether QUIC runs, so nothing here can stop a
//! node from starting.
//!
//! An older release's binary does not read the marker on Windows, so a
//! rollback ignores it. An older release on unix would read the new state as
//! a later release's and replace it.
//!
//! # Other inputs
//!
//! - **`quic-ignore-crash-breaker`** skips the trip and the cooldown. Runs are
//!   still counted and cleared, so the count stands when the override is
//!   removed and a start then trips if it has reached the limit. A start under
//!   the override replaces a trip's
//!   marker, so it erases the trip: removing the override does not bring the
//!   cooldown back.
//! - **QUIC off by config**: no marker is read or written. A marker left by an
//!   earlier run stays, and is counted if QUIC is turned on again within the
//!   cooldown of that run's start.
//! - **Fast Startup and sleep.** Fast Startup needs no handling: this breaker
//!   uses no boot identity, so that shutdown is a stop like any other. A laptop
//!   suspended during a run may read as a short run, or may clear the count
//!   early, depending on whether the timer's clock counts suspended time. That
//!   is unverified, and it costs at most a day on UDP.
//! - **The marker path** is the node's own data directory. The start that
//!   reads the marker and the run that writes it are the same process type
//!   with the same configuration, so `freenet service`, which starts
//!   `freenet network` as the logged-on user from an `HKCU` Run key, needs no
//!   agreement with any other process and the log directory plays no part.
//!
//! # Decisions and known limits
//!
//! - **The clean exit clears the count, it does not only skip one.** An update
//!   puts a different binary in place, so earlier short runs say nothing about
//!   it. If the update then fails, the same binary starts with the count at
//!   zero, which delays a trip by the runs before that exit. A failed update
//!   does not repeat at will: `claim_update_attempt` limits it.
//! - **A fatal listener or storage exit counts.** It also exits 42 under the
//!   wrapper, but through `std::process::exit` in `p2p_impl.rs`, which does not
//!   reach the clean exit.
//! - **Known false trips** (one cooldown with QUIC off each): three short runs
//!   in a row that each recorded a QUIC panic which did not stop the node and
//!   were then stopped on purpose. Also a second node on the same data
//!   directory that reaches [`gate`] while the first is under [`SHORT_RUN`]
//!   with a recorded panic and the count one short of a trip. The preflight in
//!   `run_network` exits 43 before that when a live node answers.
//! - **A marker left behind counts once**: a rollback to a release that does
//!   not clear it, followed by this release again within the cooldown, reads
//!   it as one short run.
//! - **Known missed trips**: a crash not preceded by a panic attributed to QUIC
//!   code (a stack overflow, an out-of-memory abort, an access violation, or
//!   a panic in a dependency on a thread outside the guarded calls, such as a
//!   task this crate spawns that calls into `bytes`); a panic whose mark could not be written; a crash loop whose runs
//!   each last [`SHORT_RUN`] or more never trips; a fault that crashes once per cooldown or less often
//!   never trips, because the gap resets the count; a crash before the marker
//!   is written is not counted; a crash in a run whose marker could not be
//!   written is not counted.
//! - **A trip that cannot be written repeats** at the next start, with QUIC off
//!   throughout, until the marker it could not replace is a cooldown old. Each
//!   start logs a WARN.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use super::{
    BreakerOutcome, Gated, Marker, PENDING, Previous, QUIC_BREAKER_COOLDOWN, Verdict,
    clear_if_owned, cooldown_end, marker_path, read_previous, unix_secs, write_marker,
};
use crate::config::{GlobalExecutor, GlobalRng};
use crate::node::background_task_monitor::LateRegistrar;
use crate::simulation::{RealTime, TimeSource};

/// A run shorter than this counts toward a trip.
pub(super) const SHORT_RUN: Duration = Duration::from_secs(10 * 60);

/// The number of short runs in a row that keep QUIC off.
pub(super) const SHORT_RUNS_TO_TRIP: u32 = 3;

/// How many times the timer tries to clear the count, [`SHORT_RUN`] apart. A
/// scanner holding the file open makes a removal fail on Windows, and the
/// failure only costs one count.
const CLEAR_ATTEMPTS: u32 = 3;

/// The name the timer is registered under.
pub(super) const TIMER_TASK: &str = "quic_short_run_timer";

/// What a start decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Decision {
    pub verdict: Verdict,
    /// The short runs in a row so far, the last run included.
    pub short_runs: u32,
}

/// The whole decision, as described in the module docs. Only
/// [`Verdict::Arm`], [`Verdict::Trip`] and [`Verdict::Cool`] come out of it. A
/// run counts only if it recorded a QUIC panic and started within the
/// cooldown, so an ordinary stop, a clock gone back past its start and a run
/// from long ago all start the count again.
pub(super) fn decide(previous: &Previous, now_secs: u64, ignore_breaker: bool) -> Decision {
    let arm = |short_runs| Decision {
        verdict: Verdict::Arm,
        short_runs,
    };
    match previous {
        Previous::Marker(Marker::ShortRuns {
            short_runs,
            started_at_secs,
            quic_panicked: true,
            ..
        }) if within_cooldown(*started_at_secs, now_secs) => {
            let short_runs = short_runs.saturating_add(1);
            if short_runs >= SHORT_RUNS_TO_TRIP && !ignore_breaker {
                Decision {
                    verdict: Verdict::Trip,
                    short_runs,
                }
            } else {
                arm(short_runs)
            }
        }
        Previous::Marker(Marker::Tripped { tripped_at_secs })
            if !ignore_breaker && within_cooldown(*tripped_at_secs, now_secs) =>
        {
            Decision {
                verdict: Verdict::Cool {
                    tripped_at_secs: *tripped_at_secs,
                },
                short_runs: 0,
            }
        }
        Previous::Nothing
        | Previous::Unreadable
        | Previous::Marker(
            Marker::Armed { .. }
            | Marker::ShortRuns { .. }
            | Marker::Tripped { .. }
            | Marker::Unknown,
        ) => arm(0),
    }
}

/// Whether `at_secs` is not ahead of `now_secs` and less than a cooldown before
/// it. Stricter than the unix `cooling`: a time ahead of the clock does not
/// count at all.
fn within_cooldown(at_secs: u64, now_secs: u64) -> bool {
    at_secs <= now_secs && now_secs < at_secs.saturating_add(QUIC_BREAKER_COOLDOWN.as_secs())
}

/// The marker a decision leaves behind; `None` leaves the file as it is.
fn marker_after(decision: Decision, now_secs: u64, nonce: u64) -> Option<Marker> {
    match decision.verdict {
        Verdict::Arm => Some(Marker::ShortRuns {
            short_runs: decision.short_runs,
            started_at_secs: now_secs,
            nonce,
            quic_panicked: false,
        }),
        Verdict::Trip => Some(Marker::Tripped {
            tripped_at_secs: now_secs,
        }),
        Verdict::RunUnarmed | Verdict::Cool { .. } | Verdict::Unreadable => None,
    }
}

/// [`super::gate`] on Windows. Reads the marker only with QUIC requested.
pub(super) fn gate<T>(
    settings: Option<T>,
    data_dir: &Path,
    ignore_breaker: bool,
    now: SystemTime,
) -> Gated<T> {
    if settings.is_none() {
        return Gated {
            settings,
            breaker: BreakerOutcome::NotConsulted,
        };
    }
    let path = marker_path(data_dir);
    let previous = read_previous(&path);
    if previous == Previous::Unreadable {
        tracing::warn!(
            path = %path.display(),
            "Could not read the QUIC crash-breaker marker; QUIC runs as configured and the \
             count of short runs starts again"
        );
    }
    let now_secs = unix_secs(now);
    let decision = decide(&previous, now_secs, ignore_breaker);
    let marker = marker_after(decision, now_secs, GlobalRng::random_u64());
    let metrics = &crate::transport::TRANSPORT_METRICS;
    let until_utc = |tripped_at_secs: u64| {
        chrono::DateTime::<chrono::Utc>::from(cooldown_end(tripped_at_secs)).to_rfc3339()
    };
    match decision.verdict {
        Verdict::Arm => {
            if decision.short_runs > 0 {
                tracing::info!(
                    short_runs = decision.short_runs,
                    trip_at = SHORT_RUNS_TO_TRIP,
                    "The previous run with QUIC enabled lasted less than the short-run limit"
                );
            }
            PENDING.lock().marker = marker.map(|marker| (path, marker));
        }
        Verdict::Trip => {
            // Off even if the write fails: the evidence is in hand now.
            if let Some(Err(error)) = marker.as_ref().map(|marker| write_marker(&path, marker)) {
                tracing::warn!(
                    %error,
                    path = %path.display(),
                    "Could not record the QUIC crash-breaker trip; the next start decides again"
                );
            }
            metrics.record_quic_breaker_trip();
            metrics.set_quic_breaker_suppressed(true);
            tracing::warn!(
                until = %until_utc(now_secs),
                short_runs = decision.short_runs,
                short_run_minutes = SHORT_RUN.as_secs() / 60,
                "Several runs in a row with QUIC enabled ended early, so QUIC stays off until \
                 the cooldown ends. Set `quic-ignore-crash-breaker = true` to run QUIC anyway"
            );
        }
        Verdict::Cool { tripped_at_secs } => {
            metrics.set_quic_breaker_suppressed(true);
            tracing::warn!(
                until = %until_utc(tripped_at_secs),
                "QUIC stays off: several runs in a row with QUIC enabled ended early and the \
                 cooldown has not ended. Set `quic-ignore-crash-breaker = true` to run QUIC \
                 anyway"
            );
        }
        Verdict::RunUnarmed | Verdict::Unreadable => {}
    }
    Gated {
        settings: settings.filter(|_| decision.verdict.quic_enabled()),
        breaker: BreakerOutcome::Decided(decision.verdict),
    }
}

/// Start the task that clears the count once this run has lasted
/// [`SHORT_RUN`]. See the module docs for why it lives here.
pub(super) fn start_clear_timer(registrar: Option<&LateRegistrar>, path: PathBuf, nonce: u64) {
    let Some(registrar) = registrar else {
        tracing::warn!(
            "No task registrar for the QUIC short-run timer, so this run will count as short"
        );
        return;
    };
    let handle = GlobalExecutor::spawn(async move {
        clear_after_short_run(&RealTime::new(), path, nonce).await;
        // The monitor reads any end of a registered task as a node failure.
        std::future::pending::<()>().await;
    });
    registrar.register(TIMER_TASK, handle);
}

/// Wait [`SHORT_RUN`] on `time`, then clear the count, trying again
/// [`SHORT_RUN`] later if that fails, [`CLEAR_ATTEMPTS`] times in all. Does not
/// touch a marker with another nonce: a later run or a trip owns it.
async fn clear_after_short_run(time: &impl TimeSource, path: PathBuf, nonce: u64) {
    for attempt in 1..=CLEAR_ATTEMPTS {
        time.sleep(SHORT_RUN).await;
        let target = path.clone();
        note_marker_cleared();
        // The disk can be slow on Windows; this task must not hold a worker.
        let outcome = tokio::task::spawn_blocking(move || clear_if_owned(&target, nonce)).await;
        match outcome {
            Ok(Ok(true)) => {
                tracing::info!("The run has passed the QUIC short-run limit; the count is clear");
                return;
            }
            Ok(Ok(false)) => {
                tracing::info!(
                    "The run has passed the QUIC short-run limit; the marker is no longer this \
                     run's, so it stays"
                );
                return;
            }
            Ok(Err(error)) => tracing::warn!(
                %error,
                attempt,
                path = %path.display(),
                "Could not clear the QUIC short-run count"
            ),
            Err(error) => tracing::warn!(%error, attempt, "The QUIC short-run clear did not run"),
        }
    }
}

/// Whether a panic at `file` (a panic location's path) is in QUIC code: quinn
/// or quinn-proto (a crate directory named `quinn` or `quinn-proto`, or
/// starting `quinn-`, as cargo names a registry or versioned vendored crate),
/// or this crate's QUIC transport. Paths use either separator, since a Windows
/// build reports backslashes. The fallback to [`QuicCode`]: a panic in
/// QUIC code on a thread that is not inside a guarded call.
pub(super) fn quic_attributed(file: &str) -> bool {
    let file = file.replace('\\', "/");
    file.split('/')
        .any(|part| part == "quinn" || part == "quinn-proto" || part.starts_with("quinn-"))
        || file.contains("src/transport/quic/")
        || file.ends_with("src/transport/quic.rs")
}

thread_local! {
    /// How many [`QuicCode`] guards this thread is inside.
    static IN_QUIC_CODE: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// While alive, a panic on this thread counts as a panic in QUIC code,
/// wherever it is located: inside quinn's driver polls (`runtime.rs`) and the
/// socket adapter's calls (`socket.rs`), a panic in a dependency they call
/// (`bytes`, `tokio`, `rustls`, the egress cap) is the same QUIC fault. The
/// count is restored on drop, which during an unwind runs after the hook.
pub(crate) struct QuicCode(());

impl QuicCode {
    pub(crate) fn enter() -> Self {
        IN_QUIC_CODE.with(|depth| depth.set(depth.get().saturating_add(1)));
        Self(())
    }
}

impl Drop for QuicCode {
    fn drop(&mut self) {
        IN_QUIC_CODE.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

/// Whether this thread is inside a [`QuicCode`] guard. `false` if the
/// thread-local is gone (a panic during thread teardown).
fn in_quic_code() -> bool {
    IN_QUIC_CODE
        .try_with(|depth| depth.get() > 0)
        .unwrap_or(false)
}

/// Set by a clean exit or the short-run timer before it removes the marker,
/// so a panic racing it cannot write the marker back
/// ([`record_quic_panic_at`]).
static MARKER_CLEARED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Called before this run's marker is removed on purpose (the short-run
/// timer, a clean exit).
pub(super) fn note_marker_cleared() {
    MARKER_CLEARED.store(true, std::sync::atomic::Ordering::SeqCst);
}

/// Mark the marker at `path` as having recorded a QUIC panic, if it is still
/// the run's with `nonce` and not marked yet. Returns whether it wrote. Takes
/// no lock and logs nothing: it runs inside the panic hook.
fn mark_quic_panic(path: &Path, nonce: u64) -> std::io::Result<bool> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    match serde_json::from_slice::<Marker>(&bytes) {
        Ok(Marker::ShortRuns {
            short_runs,
            started_at_secs,
            nonce: owner,
            quic_panicked: false,
        }) if owner == nonce => {
            let marked = Marker::ShortRuns {
                short_runs,
                started_at_secs,
                nonce,
                quic_panicked: true,
            };
            write_marker(path, &marked).map(|()| true)
        }
        Ok(_) | Err(_) => Ok(false),
    }
}

/// The marker the panic hook marks: the first one this process wrote.
static PANIC_TARGET: std::sync::OnceLock<(PathBuf, u64)> = std::sync::OnceLock::new();
/// Set by the first attributed panic, so only that one touches the disk.
static PANIC_RECORDED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static HOOK_INSTALLED: std::sync::Once = std::sync::Once::new();

/// Install the panic hook (once per process) that records a panic in QUIC
/// code in the marker at `path` with `nonce`. See the module docs.
pub(super) fn record_quic_panics(path: PathBuf, nonce: u64) {
    // One node per process in production; a second keeps the first.
    let _first_wins = PANIC_TARGET.set((path, nonce));
    HOOK_INSTALLED.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            on_panic(
                info.location().map(|location| location.file()),
                in_quic_code(),
            );
            previous(info);
        }));
    });
}

/// What the hook does before calling the hook it replaced: `file` is the
/// panic's location, `in_quic` whether the panicking thread is inside a
/// [`QuicCode`] guard.
fn on_panic(file: Option<&str>, in_quic: bool) {
    if !(in_quic || file.is_some_and(quic_attributed)) {
        return;
    }
    let Some((path, nonce)) = PANIC_TARGET.get() else {
        return;
    };
    if PANIC_RECORDED.swap(true, std::sync::atomic::Ordering::AcqRel) {
        return;
    }
    record_quic_panic_at(path, *nonce, || {
        MARKER_CLEARED.load(std::sync::atomic::Ordering::SeqCst)
    });
}

/// Mark the marker, unless `cleared` says the run removed it on purpose, and
/// if that happened while the mark was being written, remove the marker
/// again: the clearer sets `cleared` before it removes the file, so either it
/// removes the marked file or this sees `cleared` and removes it. Best effort:
/// a mark that cannot be written leaves the run uncounted.
fn record_quic_panic_at(path: &Path, nonce: u64, cleared: impl Fn() -> bool) {
    if cleared() {
        return;
    }
    if let Ok(true) = mark_quic_panic(path, nonce) {
        if cleared() {
            let _best_effort = clear_if_owned(path, nonce);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::write_atomic;
    use super::*;
    use crate::simulation::VirtualTime;

    const T0: u64 = 1_800_000_000;
    const NONCE: u64 = 7;

    fn cooldown() -> u64 {
        QUIC_BREAKER_COOLDOWN.as_secs()
    }

    fn running(short_runs: u32, started_at_secs: u64) -> Previous {
        Previous::Marker(Marker::ShortRuns {
            short_runs,
            started_at_secs,
            nonce: NONCE,
            quic_panicked: false,
        })
    }

    /// A short run that recorded a QUIC panic: the kind that counts.
    fn panicked(short_runs: u32, started_at_secs: u64) -> Previous {
        Previous::Marker(Marker::ShortRuns {
            short_runs,
            started_at_secs,
            nonce: NONCE,
            quic_panicked: true,
        })
    }

    fn tripped(tripped_at_secs: u64) -> Previous {
        Previous::Marker(Marker::Tripped { tripped_at_secs })
    }

    fn arm(short_runs: u32) -> Decision {
        Decision {
            verdict: Verdict::Arm,
            short_runs,
        }
    }

    /// One start against the file at `path`, doing what [`gate`] and the arm
    /// do with its decision. The run it starts is left running: the marker
    /// stays until a test clears it.
    fn start(path: &Path, now_secs: u64, ignore: bool) -> Verdict {
        let decision = decide(&read_previous(path), now_secs, ignore);
        if let Some(marker) = marker_after(decision, now_secs, NONCE) {
            write_marker(path, &marker).unwrap();
        }
        decision.verdict
    }

    /// [`start`], then a panic in QUIC code during the run (what the hook
    /// does with it).
    fn start_panicking(path: &Path, now_secs: u64, ignore: bool) -> Verdict {
        let verdict = start(path, now_secs, ignore);
        if verdict == Verdict::Arm {
            assert!(mark_quic_panic(path, NONCE).unwrap(), "marked");
        }
        verdict
    }

    /// Three short runs that panicked in QUIC code trip, not two: the fourth
    /// start is the first one with QUIC off.
    #[test]
    fn three_short_runs_in_a_row_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = marker_path(dir.path());
        for run in 0..SHORT_RUNS_TO_TRIP {
            let now = T0 + u64::from(run);
            assert_eq!(start(&path, now, false), Verdict::Arm, "start {run}");
            assert_eq!(
                read_previous(&path),
                running(run, now),
                "the marker records the runs so far and this start"
            );
            assert!(mark_quic_panic(&path, NONCE).unwrap());
            assert_eq!(read_previous(&path), panicked(run, now));
        }
        let now = T0 + u64::from(SHORT_RUNS_TO_TRIP);
        assert_eq!(start(&path, now, false), Verdict::Trip);
        assert_eq!(read_previous(&path), tripped(now));
    }

    /// A run that passes [`SHORT_RUN`], or reaches a clean exit, clears the
    /// count, so the next three runs are needed for a trip.
    #[test]
    fn a_long_run_or_a_clean_exit_between_resets_the_count() {
        let dir = tempfile::tempdir().unwrap();
        let path = marker_path(dir.path());
        for run in 0..2 {
            assert_eq!(start_panicking(&path, T0 + run, false), Verdict::Arm);
        }
        assert_eq!(read_previous(&path), panicked(1, T0 + 1));
        clear_if_owned(&path, NONCE).unwrap();
        for run in 0..SHORT_RUNS_TO_TRIP {
            assert_eq!(
                start_panicking(&path, T0 + 10, false),
                Verdict::Arm,
                "start {run}"
            );
        }
        assert_eq!(read_previous(&path), panicked(2, T0 + 10));
    }

    #[test]
    fn suppression_ends_after_the_cooldown() {
        let dir = tempfile::tempdir().unwrap();
        let path = marker_path(dir.path());
        write_marker(
            &path,
            &Marker::Tripped {
                tripped_at_secs: T0,
            },
        )
        .unwrap();
        let cool = Verdict::Cool {
            tripped_at_secs: T0,
        };
        assert_eq!(start(&path, T0, false), cool);
        assert_eq!(start(&path, T0 + cooldown() - 1, false), cool);
        assert_eq!(
            read_previous(&path),
            tripped(T0),
            "a start inside the cooldown does not move the trip"
        );
        assert_eq!(start(&path, T0 + cooldown(), false), Verdict::Arm);
        assert_eq!(
            read_previous(&path),
            running(0, T0 + cooldown()),
            "the count starts again"
        );
    }

    /// Damaged, unreadable, foreign and later-release markers all run QUIC
    /// with the count at zero.
    #[test]
    fn a_marker_that_is_not_evidence_fails_open() {
        let unix_marker = Previous::Marker(Marker::Armed {
            pid: 1,
            boot_id: None,
            nonce: NONCE,
            armed_at_secs: T0,
        });
        for previous in [
            Previous::Nothing,
            Previous::Unreadable,
            Previous::Marker(Marker::Unknown),
            unix_marker,
        ] {
            assert_eq!(decide(&previous, T0, false), arm(0), "{previous:?}");
        }

        let dir = tempfile::tempdir().unwrap();
        let path = marker_path(dir.path());
        for damaged in [
            &b"not json"[..],
            b"",
            br#"{"state":"short_runs"}"#,
            br#"{"state":"short_runs","short_runs":"x","started_at_secs":1,"nonce":1}"#,
            br#"{"state":"short_runs","short_runs":-1,"started_at_secs":1,"nonce":1}"#,
            br#"{"state":"from_a_later_release","x":1}"#,
        ] {
            std::fs::write(&path, damaged).unwrap();
            assert_eq!(start(&path, T0, false), Verdict::Arm, "{damaged:?}");
            assert_eq!(read_previous(&path), running(0, T0), "replaced");
        }

        // A directory in its place: reads as unreadable, and arming fails.
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        let decision = decide(&read_previous(&path), T0, false);
        assert_eq!(decision, arm(0));
        assert!(decision.verdict.quic_enabled());
    }

    /// A clock set back past a marker's time runs QUIC as configured.
    #[test]
    fn a_clock_going_back_fails_open() {
        // A running marker from the future: not counted, whatever it says.
        assert_eq!(decide(&panicked(2, T0 + 1), T0, false), arm(0));
        assert_eq!(decide(&panicked(2, T0), T0, false).verdict, Verdict::Trip);
        // Same second counts: restarts can be that fast.
        assert_eq!(decide(&panicked(0, T0), T0, false), arm(1));

        // A trip from the future holds nothing off, however close.
        assert_eq!(decide(&tripped(T0 + 1), T0, false), arm(0));
        assert_eq!(decide(&tripped(T0 + 2 * cooldown()), T0, false), arm(0));
        // A clock that cannot be read is 0 (`unix_secs`).
        assert_eq!(decide(&tripped(T0), 0, false), arm(0));
        assert_eq!(decide(&panicked(2, T0), 0, false), arm(0));
        // A trip time at the top of the range does not wrap into a cooldown.
        assert_eq!(decide(&tripped(u64::MAX), u64::MAX, false), arm(0));
    }

    /// A run counts only if it started less than a cooldown ago.
    #[test]
    fn a_run_from_a_cooldown_ago_starts_the_count_again() {
        assert_eq!(
            decide(&panicked(2, T0), T0 + cooldown() - 1, false).verdict,
            Verdict::Trip,
            "one second inside"
        );
        assert_eq!(decide(&panicked(0, T0), T0 + cooldown() - 1, false), arm(1));
        assert_eq!(decide(&panicked(2, T0), T0 + cooldown(), false), arm(0));
        assert_eq!(
            decide(&panicked(2, T0), T0 + 30 * cooldown(), false),
            arm(0)
        );
        assert_eq!(decide(&panicked(2, T0), T0 + cooldown(), true), arm(0));
    }

    /// Three short sessions a week apart never trip, and neither does a crash
    /// once a day. Two a day are close enough to add up.
    #[test]
    fn short_runs_do_not_add_up_across_gaps_of_a_cooldown() {
        let dir = tempfile::tempdir().unwrap();
        let path = marker_path(dir.path());
        for gap in [7 * cooldown(), cooldown()] {
            for run in 0..10 {
                let now = T0 + run * gap;
                assert_eq!(
                    start_panicking(&path, now, false),
                    Verdict::Arm,
                    "gap {gap} run {run}"
                );
                assert_eq!(read_previous(&path), panicked(0, now));
            }
            std::fs::remove_file(&path).unwrap();
        }

        let half_a_day = cooldown() / 2;
        for run in 0..SHORT_RUNS_TO_TRIP {
            let now = T0 + u64::from(run) * half_a_day;
            assert_eq!(start_panicking(&path, now, false), Verdict::Arm);
        }
        let now = T0 + u64::from(SHORT_RUNS_TO_TRIP) * half_a_day;
        assert_eq!(start(&path, now, false), Verdict::Trip);
    }

    /// The override skips the trip and the cooldown, and the count goes on.
    #[test]
    fn the_override_runs_quic_and_keeps_counting() {
        assert_eq!(decide(&panicked(2, T0), T0, true), arm(3));
        assert_eq!(decide(&panicked(9, T0), T0, true), arm(10));
        assert_eq!(decide(&tripped(T0), T0, true), arm(0));
        assert_eq!(
            marker_after(decide(&tripped(T0), T0, true), T0, NONCE),
            Some(Marker::ShortRuns {
                short_runs: 0,
                started_at_secs: T0,
                nonce: NONCE,
                quic_panicked: false,
            }),
            "the override's start replaces the trip, so the trip is gone"
        );
        assert_eq!(
            decide(&panicked(3, T0), T0, false).verdict,
            Verdict::Trip,
            "once removed, the last run trips"
        );
        assert_eq!(
            decide(&panicked(u32::MAX, T0), T0, true),
            arm(u32::MAX),
            "the count saturates"
        );
    }

    /// An update exit goes through the clean exit, so it counts for nothing:
    /// the runs after it need all three.
    #[test]
    fn an_update_exit_does_not_count() {
        let dir = tempfile::tempdir().unwrap();
        let path = marker_path(dir.path());
        for updates in 0..10 {
            assert_eq!(start(&path, T0 + updates, false), Verdict::Arm);
            assert_eq!(read_previous(&path), running(0, T0 + updates));
            clear_if_owned(&path, NONCE).unwrap(); // `quic_breaker_clean_exit`
        }
    }

    /// With QUIC off nothing is read or written, and a marker from an earlier
    /// run is left exactly as it was.
    #[test]
    fn quic_off_does_not_touch_the_marker() {
        let dir = tempfile::tempdir().unwrap();
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(T0);
        let empty = gate(None::<u8>, dir.path(), false, now);
        assert_eq!(empty.settings, None);
        assert_eq!(empty.breaker, BreakerOutcome::NotConsulted);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);

        let path = marker_path(dir.path());
        std::fs::create_dir(&path).unwrap();
        let gated = gate(None::<u8>, dir.path(), false, now);
        assert_eq!(gated.breaker, BreakerOutcome::NotConsulted);
    }

    /// The trip keeps QUIC off through the settings as well as the verdict,
    /// with the trip written before the endpoint is built.
    #[test]
    fn a_trip_and_a_cooldown_take_the_settings_away() {
        let dir = tempfile::tempdir().unwrap();
        let path = marker_path(dir.path());
        write_marker(
            &path,
            &Marker::ShortRuns {
                short_runs: SHORT_RUNS_TO_TRIP - 1,
                started_at_secs: T0,
                nonce: NONCE,
                quic_panicked: true,
            },
        )
        .unwrap();
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(T0 + 1);
        let gated = gate(Some(7u8), dir.path(), false, now);
        assert_eq!(gated.settings, None);
        assert_eq!(gated.breaker, BreakerOutcome::Decided(Verdict::Trip));
        assert_eq!(read_previous(&path), tripped(T0 + 1));

        let cooling = gate(Some(7u8), dir.path(), false, now);
        assert_eq!(cooling.settings, None);
        assert_eq!(
            cooling.breaker,
            BreakerOutcome::Decided(Verdict::Cool {
                tripped_at_secs: T0 + 1
            })
        );
    }

    /// A start that cannot write its trip keeps QUIC off for this run.
    #[test]
    fn a_trip_that_cannot_be_written_still_keeps_quic_off() {
        let dir = tempfile::tempdir().unwrap();
        // A readable marker that cannot be replaced: a directory holds the
        // temporary file's name.
        let path = marker_path(dir.path());
        write_marker(
            &path,
            &Marker::ShortRuns {
                short_runs: SHORT_RUNS_TO_TRIP - 1,
                started_at_secs: T0,
                nonce: NONCE,
                quic_panicked: true,
            },
        )
        .unwrap();
        let tmp = dir.path().join(format!(
            ".{}.{}.tmp",
            super::super::MARKER_FILE,
            std::process::id()
        ));
        std::fs::create_dir(tmp).unwrap();
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(T0 + 1);
        let gated = gate(Some(7u8), dir.path(), false, now);
        assert_eq!(gated.settings, None);
        assert_eq!(
            read_previous(&path),
            panicked(SHORT_RUNS_TO_TRIP - 1, T0),
            "the marker stays, so the next start decides again"
        );
    }

    /// Run the timer against `time`.
    fn run_timer(time: &VirtualTime, path: &Path) -> tokio::task::JoinHandle<()> {
        let (time, path) = (time.clone(), path.to_path_buf());
        tokio::spawn(async move { clear_after_short_run(&time, path, NONCE).await })
    }

    /// Wait until the timer has gone to sleep on `time`, so that an advance
    /// reaches it. Bounded, so a timer that never sleeps fails the test.
    async fn until_sleeping(time: &VirtualTime) {
        tokio::time::timeout(Duration::from_secs(30), async {
            while time.pending_wakeup_count() == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the timer never went to sleep");
    }

    /// The timer clears the count at [`SHORT_RUN`], not before, and leaves a
    /// marker another run or a trip has replaced.
    #[tokio::test]
    async fn the_timer_clears_only_its_own_marker_after_a_short_run() {
        let dir = tempfile::tempdir().unwrap();
        let path = marker_path(dir.path());

        let time = VirtualTime::new();
        write_marker(&path, &marker_after(arm(1), T0, NONCE).unwrap()).unwrap();
        let timer = run_timer(&time, &path);
        until_sleeping(&time).await;
        assert_eq!(
            time.next_wakeup_deadline(),
            Some(SHORT_RUN.as_nanos() as u64),
            "it wakes at the limit, not before"
        );
        assert!(path.exists());
        time.advance(SHORT_RUN);
        timer.await.unwrap();
        assert!(!path.exists(), "the run has lasted the limit");

        for marker in [
            marker_after(arm(1), T0, NONCE + 1).unwrap(),
            Marker::Tripped {
                tripped_at_secs: T0,
            },
        ] {
            let time = VirtualTime::new();
            write_marker(&path, &marker).unwrap();
            let timer = run_timer(&time, &path);
            until_sleeping(&time).await;
            time.advance(SHORT_RUN);
            timer.await.unwrap();
            assert!(path.exists(), "{marker:?} is not this run's");
        }
    }

    /// The real timer clears the count and then stays alive: the monitor reads
    /// any end of a registered task as a node failure.
    #[tokio::test(start_paused = true)]
    async fn the_registered_timer_clears_the_count_and_does_not_end() {
        let dir = tempfile::tempdir().unwrap();
        let path = marker_path(dir.path());
        write_marker(&path, &marker_after(arm(1), T0, NONCE).unwrap()).unwrap();
        let monitor = crate::node::background_task_monitor::BackgroundTaskMonitor::new();
        start_clear_timer(Some(&monitor.late_registrar()), path.clone(), NONCE);
        let ended = tokio::time::timeout(100 * SHORT_RUN, monitor.wait_for_any_exit()).await;
        assert!(ended.is_err(), "the monitor saw the timer end: {ended:?}");
        assert!(!path.exists());
    }

    /// A removal that fails is tried again, and the tries are bounded.
    #[tokio::test]
    async fn the_timer_retries_a_failed_clear_a_bounded_number_of_times() {
        let dir = tempfile::tempdir().unwrap();
        // A marker path that is a directory: reading it fails.
        let path = marker_path(dir.path());
        std::fs::create_dir(&path).unwrap();
        let time = VirtualTime::new();
        let timer = run_timer(&time, &path);
        for _ in 0..CLEAR_ATTEMPTS {
            until_sleeping(&time).await;
            time.advance(SHORT_RUN);
        }
        tokio::time::timeout(Duration::from_secs(30), timer)
            .await
            .expect("the tries are bounded")
            .unwrap();

        // A failure, then the path is repaired: the second try clears it.
        let time = VirtualTime::new();
        let timer = run_timer(&time, &path);
        until_sleeping(&time).await;
        time.advance(SHORT_RUN);
        until_sleeping(&time).await;
        std::fs::remove_dir(&path).unwrap();
        write_marker(&path, &marker_after(arm(0), T0, NONCE).unwrap()).unwrap();
        time.advance(SHORT_RUN);
        timer.await.unwrap();
        assert!(!path.exists());
    }

    /// The format is plain JSON with a `state` tag; pinned so a later release
    /// can read what this one wrote.
    #[test]
    fn marker_format() {
        assert_eq!(
            serde_json::to_value(marker_after(arm(2), T0, NONCE).unwrap()).unwrap(),
            serde_json::json!({
                "state": "short_runs", "short_runs": 2, "started_at_secs": T0, "nonce": NONCE,
                "quic_panicked": false
            })
        );
        // A marker from the release before the field (#6101) reads as a run
        // that recorded no QUIC panic, so it does not count.
        let earlier: Marker = serde_json::from_value(serde_json::json!({
            "state": "short_runs", "short_runs": 2, "started_at_secs": T0, "nonce": NONCE
        }))
        .unwrap();
        assert_eq!(Previous::Marker(earlier), running(2, T0));
        assert_eq!(decide(&running(2, T0), T0, false), arm(0));
    }

    /// `write_atomic` is what stands between a crash and a half-written
    /// marker; a marker cut short counts as no marker.
    #[test]
    fn a_truncated_marker_counts_as_no_marker() {
        let dir = tempfile::tempdir().unwrap();
        let path = marker_path(dir.path());
        let full = serde_json::to_vec(&marker_after(arm(2), T0, NONCE).unwrap()).unwrap();
        write_atomic(&path, &full[..full.len() / 2]).unwrap();
        assert_eq!(read_previous(&path), Previous::Nothing);
    }

    /// The unix decision treats this marker as no evidence, so a data
    /// directory carried from Windows starts normally.
    #[test]
    fn the_unix_decision_ignores_a_short_run_marker() {
        let previous = running(2, T0);
        let got = super::super::verdict(&super::super::Facts {
            previous: &previous,
            boot_id: Some("boot"),
            armed_pid_alive: false,
            stopped: &[],
            now_secs: T0,
            ignore_breaker: false,
        });
        assert_eq!(got, Verdict::Arm);
    }

    /// A short run that recorded no QUIC panic (a logoff, a shutdown, a
    /// `taskkill`, a tray Quit) starts the count again, however many in a row.
    #[test]
    fn an_ordinary_short_run_does_not_count() {
        let dir = tempfile::tempdir().unwrap();
        let path = marker_path(dir.path());
        for run in 0..10 {
            assert_eq!(start(&path, T0 + run, false), Verdict::Arm, "start {run}");
            assert_eq!(read_previous(&path), running(0, T0 + run));
        }
        assert_eq!(
            decide(&running(SHORT_RUNS_TO_TRIP - 1, T0), T0, false),
            arm(0)
        );
        // Two panicked runs, then an ordinary stop: the count starts again.
        std::fs::remove_file(&path).unwrap();
        for run in 0..2 {
            assert_eq!(start_panicking(&path, T0 + run, false), Verdict::Arm);
        }
        assert_eq!(start(&path, T0 + 2, false), Verdict::Arm);
        assert_eq!(read_previous(&path), running(2, T0 + 2));
        assert_eq!(start(&path, T0 + 3, false), Verdict::Arm);
        assert_eq!(read_previous(&path), running(0, T0 + 3));
    }

    /// Attribution is by the panic's file: quinn and quinn-proto, under any
    /// cargo source directory and either separator, and this crate's QUIC
    /// transport. Nothing else.
    #[test]
    fn a_panic_is_attributed_to_quic_by_its_location() {
        for quic in [
            "/home/u/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/quinn-0.11.12/src/endpoint.rs",
            "C:\\Users\\u\\.cargo\\registry\\src\\index.crates.io-1949cf8c6b5b557f\\quinn-proto-0.11.18\\src\\connection\\mod.rs",
            "/build/vendor/quinn-udp-0.5.14/src/lib.rs",
            "/build/vendor/quinn/src/endpoint.rs",
            "/build/vendor/quinn-proto/src/connection/mod.rs",
            "crates/core/src/transport/quic/runtime.rs",
            "crates\\core\\src\\transport\\quic\\path\\route.rs",
            "crates/core/src/transport/quic.rs",
        ] {
            assert!(quic_attributed(quic), "{quic}");
        }
        for other in [
            "crates/core/src/transport/peer_connection.rs",
            "crates/core/src/transport/quick.rs",
            "crates/core/src/node/p2p_impl.rs",
            "/home/u/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/tokio-1.47.1/src/runtime/task/mod.rs",
            "/rustc/1159e78c4747b02ef996e55082b704c09b970588/library/core/src/option.rs",
            "/home/u/.cargo/registry/src/x/myquinn-1.0.0/src/lib.rs",
            "",
        ] {
            assert!(!quic_attributed(other), "{other}");
        }
    }

    /// A panic marks only this run's marker, once: not another run's, not a
    /// trip, and not a marker the timer has already removed, so a panic after
    /// the run has lasted [`SHORT_RUN`] cannot make it count.
    #[test]
    fn a_panic_marks_only_this_short_run() {
        let dir = tempfile::tempdir().unwrap();
        let path = marker_path(dir.path());
        assert!(!mark_quic_panic(&path, NONCE).unwrap(), "no marker");

        write_marker(&path, &marker_after(arm(1), T0, NONCE + 1).unwrap()).unwrap();
        assert!(!mark_quic_panic(&path, NONCE).unwrap(), "another run's");
        assert_eq!(
            read_previous(&path),
            Previous::Marker(marker_after(arm(1), T0, NONCE + 1).unwrap())
        );

        write_marker(
            &path,
            &Marker::Tripped {
                tripped_at_secs: T0,
            },
        )
        .unwrap();
        assert!(!mark_quic_panic(&path, NONCE).unwrap(), "a trip");
        assert_eq!(read_previous(&path), tripped(T0));

        write_marker(&path, &marker_after(arm(1), T0, NONCE).unwrap()).unwrap();
        assert!(mark_quic_panic(&path, NONCE).unwrap());
        assert_eq!(read_previous(&path), panicked(1, T0));
        assert!(!mark_quic_panic(&path, NONCE).unwrap(), "once");

        // The run passed the limit: the timer removed it; a panic now finds
        // nothing to mark and the next start counts nothing.
        write_marker(&path, &marker_after(arm(1), T0, NONCE).unwrap()).unwrap();
        assert!(clear_if_owned(&path, NONCE).unwrap());
        assert!(!mark_quic_panic(&path, NONCE).unwrap());
        assert!(!path.exists());
        assert_eq!(decide(&read_previous(&path), T0 + 1, false), arm(0));
    }

    /// Set in the child process that runs [`the_panic_hook_marks_a_quic_panic_and_chains`]'s body.
    const HOOK_CHILD_ENV: &str = "FREENET_TEST_QUIC_PANIC_HOOK_CHILD";

    /// The real hook, installed in a child process (the hook is
    /// process-global, so it must not reach other tests; see the
    /// cross-test-interference row in bug-prevention-patterns): a panic in
    /// this file, which is QUIC code, marks the run, the hook installed
    /// before it still runs, and the default panic output still reaches
    /// stderr.
    #[test]
    fn the_panic_hook_marks_a_quic_panic_and_chains() {
        let name = "transport::quic::breaker::short_runs::tests::the_panic_hook_marks_a_quic_panic_and_chains";
        if std::env::var_os(HOOK_CHILD_ENV).is_some() {
            hook_child_body();
            return;
        }
        let exe = if cfg!(target_os = "linux") {
            PathBuf::from("/proc/self/exe")
        } else {
            std::env::current_exe().expect("test binary path")
        };
        let output = std::process::Command::new(exe)
            .args(["--exact", "--test-threads=1", "--nocapture", name])
            .env(HOOK_CHILD_ENV, "1")
            .output()
            .expect("re-run the test binary");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "the child failed ({}).\nstdout:\n{stdout}\nstderr:\n{stderr}",
            output.status
        );
        // libtest exits 0 when its filter matches nothing: a rename must fail.
        assert!(
            stdout.contains("1 passed"),
            "the child did not run the test; was it renamed?\nstdout:\n{stdout}"
        );
        assert!(
            stderr.contains("injected QUIC panic"),
            "the default panic output was swallowed:\n{stderr}"
        );
    }

    fn hook_child_body() {
        use std::sync::atomic::{AtomicU32, Ordering};
        static EARLIER_HOOK_RUNS: AtomicU32 = AtomicU32::new(0);
        let default = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            EARLIER_HOOK_RUNS.fetch_add(1, Ordering::SeqCst);
            default(info);
        }));

        let dir = tempfile::tempdir().unwrap();
        let path = marker_path(dir.path());
        write_marker(&path, &marker_after(arm(1), T0, NONCE).unwrap()).unwrap();
        record_quic_panics(path.clone(), NONCE);

        // Not QUIC's: located elsewhere, or with no location, outside a
        // guard. Nothing is marked and nothing latches.
        on_panic(Some("crates/core/src/node/p2p_impl.rs"), false);
        on_panic(None, false);
        assert_eq!(read_previous(&path), running(1, T0), "not QUIC's");
        // A real panic located in a dependency (alloc), outside a guard.
        let caught = std::panic::catch_unwind(|| {
            let v: Vec<u64> = Vec::with_capacity(std::hint::black_box(usize::MAX));
            v.len()
        });
        assert!(caught.is_err());
        assert_eq!(EARLIER_HOOK_RUNS.load(Ordering::SeqCst), 1, "chained");
        assert_eq!(
            read_previous(&path),
            running(1, T0),
            "a dependency's own panic"
        );

        // The same dependency panic inside a guard (as in quinn's driver or
        // the socket adapter) is QUIC's.
        let caught = std::panic::catch_unwind(|| {
            let _quic = QuicCode::enter();
            let v: Vec<u64> = Vec::with_capacity(std::hint::black_box(usize::MAX));
            v.len()
        });
        assert!(caught.is_err());
        assert!(!in_quic_code(), "the guard is released by the unwind");
        assert_eq!(EARLIER_HOOK_RUNS.load(Ordering::SeqCst), 2, "chained");
        assert_eq!(read_previous(&path), panicked(1, T0), "marked");

        // A later panic in QUIC code does not touch the disk again.
        write_marker(&path, &marker_after(arm(1), T0, NONCE).unwrap()).unwrap();
        let caught = std::panic::catch_unwind(|| panic!("injected QUIC panic"));
        assert!(caught.is_err());
        assert_eq!(EARLIER_HOOK_RUNS.load(Ordering::SeqCst), 3, "still chained");
        assert_eq!(read_previous(&path), running(1, T0), "marked once only");
    }

    /// A panic racing the run's own removal of its marker (the short-run
    /// timer, a clean exit) does not write it back: nothing is written once
    /// the removal is announced, and a mark written while it is announced is
    /// taken out again.
    #[test]
    fn a_panic_racing_the_clear_does_not_resurrect_the_marker() {
        let dir = tempfile::tempdir().unwrap();
        let path = marker_path(dir.path());
        write_marker(&path, &marker_after(arm(1), T0, NONCE).unwrap()).unwrap();
        record_quic_panic_at(&path, NONCE, || true);
        assert_eq!(
            read_previous(&path),
            running(1, T0),
            "announced: not written"
        );

        // Announced between the check and the write: the mark is removed.
        let asks = std::cell::Cell::new(0);
        record_quic_panic_at(&path, NONCE, || {
            asks.set(asks.get() + 1);
            asks.get() > 1
        });
        assert_eq!(asks.get(), 2);
        assert!(!path.exists(), "the clearer's removal stands");

        // Not announced: marked.
        write_marker(&path, &marker_after(arm(1), T0, NONCE).unwrap()).unwrap();
        record_quic_panic_at(&path, NONCE, || false);
        assert_eq!(read_previous(&path), panicked(1, T0));
    }

    /// The guard nests and is released in order.
    #[test]
    fn the_quic_code_guard_nests() {
        assert!(!in_quic_code());
        let outer = QuicCode::enter();
        {
            let _inner = QuicCode::enter();
            assert!(in_quic_code());
        }
        assert!(in_quic_code());
        drop(outer);
        assert!(!in_quic_code());
    }

    /// Pins the guarded call sites, which no unit test reaches through a real
    /// quinn driver: every driver future quinn spawns is polled inside a
    /// guard, and so is every catch_unwind call in the socket adapter.
    #[test]
    fn quinn_drivers_and_the_socket_adapter_run_inside_the_guard() {
        let flat = |src: &str| src.split_whitespace().collect::<String>();
        let runtime = flat(include_str!("../runtime.rs"));
        let spawn = runtime
            .find("fnspawn(&self,future:")
            .expect("Runtime::spawn");
        let body = &runtime[spawn..];
        let wrap = body
            .find("letfuture=Box::pin(InQuicCode(future));")
            .expect("wrapped");
        let first_spawn = body.find("GlobalExecutor::spawn(").expect("spawned");
        assert!(
            wrap < first_spawn,
            "the driver is wrapped before it is spawned"
        );
        assert!(
            runtime.contains("let_quic=QuicCode::enter();self.0.as_mut().poll(cx)"),
            "InQuicCode polls inside the guard"
        );
        let socket = flat(include_str!("../socket.rs"));
        let unwinds = socket.matches("std::panic::catch_unwind(").count();
        let guarded = socket
            .matches("let_quic=super::breaker::QuicCode::enter();matchstd::panic::catch_unwind(")
            .count();
        assert!(unwinds >= 3, "the adapter's catch_unwind calls: {unwinds}");
        assert_eq!(guarded, unwinds, "every adapter catch_unwind is guarded");
    }
}
