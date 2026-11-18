//! The QUIC crash-loop breaker (#5940).
//!
//! When runs with QUIC enabled keep ending in a QUIC fault, QUIC stays off
//! for [`QUIC_BREAKER_COOLDOWN`] (24 h) on a peer and for
//! [`GATEWAY_QUIC_BREAKER_COOLDOWN`] (15 min, provisional) on a gateway
//! ([`BreakerRole`]). Some failures inside quinn's own code end the process
//! rather than a task, and the service manager restarts the node into the
//! same failure; the breaker bounds those restarts with QUIC per cooldown.
//!
//! Two rules decide it ([`decides_by_short_runs`]). Every peer, a gateway
//! on Windows, and a gateway on a unix platform with no boot identity
//! (`platform::boot_id`: anything but Linux and macOS), count short runs
//! that panicked in QUIC code ([`short_runs`]). A gateway on Linux or macOS
//! asks how its last run ended, which the rest of this doc describes. With no
//! UDP to fall back to ([`UDP_FALLBACK`] false) every node counts short runs,
//! a gateway included, because only that rule can back off. Peers on unix used
//! that second rule until #6151; it still runs on them as a shadow ([`legacy_shadow`]) that only
//! telemetry reads, until UDP is removed.
//!
//! # Gateways
//!
//! The cooldown is decided once, at startup ([`gate`]). So that a gateway is
//! never QUIC-off for longer than its cooldown, a gateway whose service unit
//! marks it supervised (`FREENET_SUPERVISED`, `liveness::gateway_quic_restarts`)
//! and whose run starts with QUIC kept off by a trip or a cooldown exits when
//! the cooldown ends ([`quic_restore_at`]), and its supervisor restarts it
//! with QUIC on. The same gateway exits when its QUIC endpoint dies, once the
//! exit is recorded in the marker ([`note_endpoint_down_exit`]). Both exits
//! use code 42 at any uptime, never the fast-crash code 45, which the
//! post-update crash-loop rollback counts. systemd's start limit counts every
//! start, 42 included (5 in 120 s under a `freenet service install` unit);
//! these restarts stay under it, because only the first endpoint death in a
//! day restarts at once and every other restart waits out a cooldown of at
//! least 15 minutes or the 90 s floor (pinned against the unit by the
//! binary's `gateway_quic_restarts_stay_under_the_unit_start_limit`). Under
//! that unit, 42 also runs `freenet update`, which changes nothing when
//! there is no newer release. The first endpoint death in
//! [`ENDPOINT_DOWN_WINDOW`] restarts QUIC at once; the next trips, and so
//! does every one while a recent trip counts, so the restarts per day are
//! bounded whatever the death rate ([`ENDPOINT_DOWN_EXITS_TO_TRIP`]). Under
//! `quic-ignore-crash-breaker` a gateway does not exit for an endpoint
//! death at all: the next start would ignore the record, so it would never
//! stop restarting. The cooldown restart comes at
//! least `QUIC_RESTORE_MIN_UPTIME` (90 s) after the start, after the
//! startup update check. A gateway's cooldown doubles on each repeat trip
//! (a trip less than [`GATEWAY_TRIP_STREAK_RESET`], a day, after the last
//! cooldown ended), up to [`GATEWAY_QUIC_BREAKER_COOLDOWN_MAX`] (4 h), so a
//! fault that recurs at every start does not restart it every 15 minutes
//! forever; the markers carry the last trip ([`PriorTrip`]), and a clean
//! stop, which removes the marker, starts the count again.
//!
//! A gateway also replaces a marker it cannot read, and runs armed, instead
//! of staying off. Two cases keep QUIC off on a gateway until it restarts,
//! with an ERROR: a marker that can be neither read nor removed, and a trip
//! that cannot be written; in both the data directory is not writable, and a
//! restart would find the same marker. A gateway without the supervisor
//! marker logs that it does not restart itself (a WARN under systemd without
//! `FREENET_SUPERVISED`, an ERROR naming the cooldown end when QUIC is off).
//! An older release reads no role: rolling a gateway back to one brings the
//! peer cooldown back.
//!
//! **This module doc describes a gateway on Linux or macOS.** Windows does not reliably
//! tell a process that it is being stopped: every design that tried to tell a crash
//! from a normal stop there (a logoff, a Fast Startup shutdown, a session end)
//! got one of them wrong. So Windows does not ask how a run ended. A panic
//! hook records a panic located in QUIC code (the failure this breaker is for
//! always panics before it aborts), and runs that ended within ten minutes
//! after recording one, three in a row (two on a peer while UDP exists),
//! each starting within 24 h of the one before, keep QUIC off for the role's cooldown, with the same state and
//! telemetry. [`short_runs`] describes it. Peers on every platform use it
//! too (#6151): once UDP is removed a peer has no transport to fall back to,
//! so the breaker must not trip on anything but a QUIC fault, and the exit
//! rule's known false trips (a kill at stop, a closed terminal) cost a peer a
//! day of QUIC already.
//!
//! **The short-run rule is weaker than the exit rule.** A crash not preceded
//! by a panic in QUIC code is not counted, a crash loop whose runs each last
//! ten minutes or more never trips, and neither does a fault that crashes
//! once a day. QUIC became on by default on Windows (shadow, now `prefer`)
//! after this rule was confirmed on a real Windows machine (#6031), with
//! those limits weighed. On a unix peer the shadow of the exit rule measures
//! what they cost: the `quic_breaker_start` event reports what it would have
//! decided ([`StartReport`]).
//!
//! # Without UDP (#6151; off until UDP is removed)
//!
//! Turning QUIC off is a fallback only while UDP exists. [`UDP_FALLBACK`]
//! says whether it does; the UDP-removal release sets it to `false`, and
//! until then everything below runs only in tests. With no UDP:
//!
//! - a peer trips at three short runs again
//!   ([`short_runs::short_runs_to_trip`]);
//! - a trip keeps QUIC on, on a gateway as on a peer ([`Verdict::Backoff`];
//!   QUIC off would leave a gateway with no transport for 15 minutes to 4
//!   hours, and every unix gateway counts short runs for this, see
//!   [`decides_by_short_runs`]): its transport opens
//!   only after a delay, 1 minute doubling per further counted run up to 30
//!   ([`backoff_delay`], waited out in [`wait_out_crash_loop_backoff`] just
//!   before the transport opens, and cut short by a stop), the marker records
//!   the step at once with no QUIC panic (so a stop or crash during the wait
//!   ends the backoff), and a run that outlives the short-run limit clears it.
//!   The update probation (`rollback::COMMIT_HEALTHY_UPTIME_SECS`) does not
//!   run while the wait does ([`wait_for_transport_open`]), so a release that
//!   crashes the moment QUIC opens is still rolled back;
//! - the binary's startup update check runs after a jitter of under 5
//!   seconds instead of its 0-60 s one ([`quic_crash_loop_update_now`]), through the same claim, so the
//!   known-bad pin, the lockout, a release cooling down and the download
//!   budget still apply, and the backoff spaces the checks to about one per
//!   30 minutes;
//! - an earlier release's QUIC-off trip is dropped with a WARN;
//! - a supervised peer exits when its QUIC endpoint dies, as a gateway does
//!   (`liveness::gateway_quic_restarts`), under the same endpoint-death rule.
//!   On unix this is a change in today's behaviour for a gateway with no boot
//!   identity: [`note_endpoint_down_exit`] now writes its marker again after the
//!   short-run timer cleared it, where it used to find none and keep running.
//!
//! An unsupervised peer whose endpoint dies is not restarted: it keeps
//! running, with no transport once UDP is gone. Whether it should exit
//! instead is left to the UDP-removal release.
//!
//! # The marker
//!
//! [`MARKER_FILE`] in the node's data directory, written atomically:
//!
//! - **Armed** once the node has bound its UDP socket, built its QUIC
//!   endpoint and installed its SIGTERM handler ([`arm_if_quic_up`] and
//!   [`quic_breaker_signals_ready`], whichever comes last). It records the
//!   process id, the machine's boot identity and a random nonce.
//! - **Removed** by [`quic_breaker_clean_exit`], which the `freenet` binary
//!   calls on its two clean exits (an operator stop and the auto-update exit),
//!   and only while the marker still carries this process's nonce.
//! - **Tripped**, with the time, when a start finds that the run which armed
//!   it crashed. QUIC then stays off until the cooldown ends, and the marker
//!   is not rewritten.
//!
//! # Was it a crash?
//!
//! [`verdict`] is a pure function of the marker and the evidence. An Armed
//! marker is a crash when the machine has not rebooted since it was armed,
//! the armed pid is no longer alive, and the stop note does not list it.
//!
//! - A different boot identity is never a crash: a reboot ends every process.
//!   If either side's boot identity is unknown, it is not a crash either; the
//!   start logs one WARN, since the breaker then cannot trip.
//! - If the armed pid is still alive on the same boot (a node still draining
//!   after SIGTERM, or a second node on the same data directory), the start
//!   waits for it to exit, up to the configured `shutdown-drain-secs` plus
//!   [`HOLDER_EXIT_MARGIN`] (at most [`HOLDER_EXIT_MAX`]), then reads the
//!   marker again,
//!   and decides on what it reads then: a holder that exited cleanly has
//!   removed its marker, and one that crashed during the wait has left it. If
//!   the holder is still alive, the start arms over its marker without
//!   tripping.
//! - **The stop note** lists pids that the macOS tray wrapper (`freenet
//!   service run-wrapper` in Freenet.app) ended with a hard kill on purpose
//!   (its tray Restart, Stop, Quit and update), recorded before the kill
//!   ([`record_deliberate_stop`]) in the file it passes to its node as
//!   [`STOP_NOTE_ENV_VAR`]. Every start, QUIC on or off, removes an Armed
//!   marker whose pid the note lists and then deletes the note, so an entry
//!   cannot wait for a later start. A node started without that variable
//!   (from a terminal, say) reads the same file at its fixed place in the
//!   default log directory; that file may be meant for another node, so such
//!   a start takes out only the pid that matched its own marker.
//!
//! # Other inputs
//!
//! - **A marker that exists but cannot be read** keeps QUIC off for that run,
//!   with a WARN, unless the override is set; then QUIC runs without arming.
//!   A marker that does not parse, or has a state from a later release, is
//!   not evidence and is replaced.
//! - **Override.** `quic-ignore-crash-breaker` skips the trip and the
//!   cooldown. The marker is still armed and cleared as usual (when it can be
//!   read), so once the override is removed only a crash under it trips. A
//!   start under the override replaces a trip's marker, so it erases the trip:
//!   removing the override does not bring the cooldown back. Like
//!   every other setting, a value given on the command line or in the
//!   environment is saved to `config.toml`, and stays until it is removed
//!   there.
//! - The breaker is off unless the `freenet` binary enables it
//!   ([`enable_quic_crash_breaker`]), so tests and embedders that build nodes
//!   in-process never see it. A start with QUIC off reconciles and deletes
//!   the note, and otherwise does not read or write the marker.
//!
//! # Decisions and known limits
//!
//! - **Any abnormal exit trips, not only one caused by QUIC** (a forced exit
//!   after a fatal listener or storage error, an out-of-memory kill), so
//!   `quic_breaker_trips` counts abnormal ends of QUIC-enabled runs. The
//!   exception is a gateway's exit on endpoint death, which it notes in the
//!   marker first ([`note_endpoint_down_exit`]): that restarts QUIC the first
//!   time in [`ENDPOINT_DOWN_WINDOW`] and trips after that
//!   ([`endpoint_down_verdict`]). The breaker does not
//!   change exit codes, so it and the post-update crash-loop
//!   rollback (#4073) act independently.
//! - **Known false trips** (one cooldown with QUIC off each): a stop ended
//!   with SIGKILL, whether by the supervisor after its grace period (systemd
//!   45 s, launchd 20 s, Kubernetes 30 s, `docker stop` 10 s, against a drain
//!   of 30 s by default) or
//!   by this repo's own escalations (the systemd unit's `ExecStartPre` reaper
//!   of a leftover node, `freenet service` self-heal and purge, and the macOS
//!   launch script's stale-process kill); a terminal closed on a foreground
//!   node (SIGHUP is not handled); an error during teardown after a SIGTERM,
//!   which makes the run end with an error (exit 1) rather than as a clean
//!   stop; a Ctrl+C in the moment between arming and
//!   the SIGINT handler's first use (only SIGTERM is waited for); a hard kill
//!   whose note could not be written.
//! - **Known missed trips** (QUIC stays on, as every node runs today): a
//!   crash followed by a reboot; a crashed pid that the OS already gave to
//!   another process that outlives the wait; on macOS, a step of the system
//!   clock between arming and the next start, which moves `kern.boottime` and
//!   so reads as a reboot; a start that wipes the data directory and with it
//!   the marker (a node run with `--id`, and debug builds' default
//!   directories).
//! - **The nonce narrows, but does not close, the overlap between an old node
//!   and a new one.** The clean exit reads the marker and then removes it, so
//!   a newer node arming between the two can lose its marker and then run
//!   unprotected.
//! - **A panic in the update-check task counts as clean.** The run then ends
//!   `Ok` (exit 0, as before this change), and the breaker follows the exit
//!   code.
//! - **A trip that cannot be written repeats** at the next start, with QUIC
//!   off throughout. A marker that cannot be armed lets QUIC run with a WARN.
//! - **QUIC turned off right after a crash, then back on later**, trips at
//!   that later start if nothing else intervened: a start with QUIC off leaves
//!   the marker alone.
//!
//! The binary's wiring is tested through the real process in
//! `tests/graceful_shutdown_exit_code.rs`.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

use crate::node::background_task_monitor::LateRegistrar;

mod short_runs;
pub use short_runs::PEER_SHORT_RUNS_TO_TRIP_WITH_UDP;
pub(crate) use short_runs::QuicCode;

/// Whether UDP is still there to fall back to when the breaker trips (#6151).
/// While it is, a trip keeps QUIC off for a cooldown and the node runs over
/// UDP. The UDP-removal release (#5940 phase D) sets this to `false`: a
/// peer's trip then keeps QUIC on and delays the start instead
/// ([`Verdict::Backoff`]), runs the update check within seconds, and a supervised
/// peer exits when its QUIC endpoint dies, as a gateway does; and a peer
/// trips at [`short_runs::SHORT_RUNS_TO_TRIP`] again. Everything that reads
/// it takes it as a parameter, so tests cover both values; production reads
/// it through [`udp_fallback`].
pub const UDP_FALLBACK: bool = true;

/// [`UDP_FALLBACK`], or `false` in a test build of the binary whose
/// environment sets `FREENET_TEST_BREAKER_NO_UDP_FALLBACK` (the
/// `quic-panic-injection` feature, which cannot be built without debug
/// assertions), so a real-process test can run the post-UDP path.
pub(crate) fn udp_fallback() -> bool {
    #[cfg(feature = "quic-panic-injection")]
    if std::env::var_os("FREENET_TEST_BREAKER_NO_UDP_FALLBACK").is_some() {
        return false;
    }
    UDP_FALLBACK
}

/// How many short runs in a row trip the crash-loop breaker for a node, with
/// or without UDP to fall back to. Read by the binary's test of the order
/// between the breaker and the post-update rollback.
#[doc(hidden)]
pub fn short_runs_to_trip(is_gateway: bool, udp_fallback: bool) -> u32 {
    short_runs::short_runs_to_trip(BreakerRole::from_is_gateway(is_gateway), udp_fallback)
}

/// The first start delay of a peer's crash loop with no UDP to fall back to
/// ([`Verdict::Backoff`]); each further counted run doubles it.
pub(crate) const BACKOFF_FIRST: Duration = Duration::from_secs(60);

/// The longest start delay ([`backoff_delay`]).
pub(crate) const BACKOFF_MAX: Duration = Duration::from_secs(30 * 60);

/// How long a start at backoff `step` (1 for the first) waits before it
/// opens the transport: [`BACKOFF_FIRST`] doubled for each step after the
/// first, at most [`BACKOFF_MAX`]. A run that outlives
/// [`short_runs::SHORT_RUN`] clears the marker, and with it the step.
pub(crate) fn backoff_delay(step: u32) -> Duration {
    backoff_delay_from(step, backoff_first())
}

fn backoff_delay_from(step: u32, first: Duration) -> Duration {
    let doublings = step.max(1) - 1;
    first
        .checked_mul(2u32.saturating_pow(doublings))
        .unwrap_or(BACKOFF_MAX)
        .min(BACKOFF_MAX)
}

/// [`BACKOFF_FIRST`], or the seconds `FREENET_TEST_BREAKER_BACKOFF_SECS`
/// gives in a `quic-panic-injection` test build.
fn backoff_first() -> Duration {
    #[cfg(feature = "quic-panic-injection")]
    if let Some(secs) = std::env::var("FREENET_TEST_BREAKER_BACKOFF_SECS")
        .ok()
        .and_then(|secs| secs.parse().ok())
    {
        return Duration::from_secs(secs);
    }
    BACKOFF_FIRST
}

/// The start delay [`gate`] decided for this run, until the transport waits
/// it out ([`wait_out_crash_loop_backoff`]).
static BACKOFF: parking_lot::Mutex<Option<Duration>> = parking_lot::Mutex::new(None);

/// Set when this start is in a crash-loop backoff, so the binary's startup
/// update check runs after a few seconds of jitter instead of its 0-60 s one
/// ([`quic_crash_loop_update_now`]).
static UPDATE_NOW: AtomicBool = AtomicBool::new(false);

fn note_backoff(delay: Duration) {
    *BACKOFF.lock() = Some(delay);
    BACKOFF_PENDING.store(true, Ordering::SeqCst);
    UPDATE_NOW.store(true, Ordering::Relaxed);
}

/// Whether this start found the node in a QUIC crash loop with no UDP to fall
/// back to (#6151), so the startup update check should run after a few
/// seconds of jitter rather than its 0-60 s one: the node is broken, and a fixed release is
/// what ends the loop. Read by the binary after the node is built (which runs
/// the breaker) and before the update check starts. The check still goes
/// through the same claim: a release pinned bad, the lockout, a release
/// cooling down and the download budget all still refuse it.
pub fn quic_crash_loop_update_now() -> bool {
    UPDATE_NOW.load(Ordering::Relaxed)
}

/// Wait out this start's crash-loop backoff, if [`gate`] decided one, before
/// the transport opens. Returns early once `shutting_down` is set, so a stop
/// during the wait is prompt. The marker the wait leaves on disk records the
/// step and no QUIC panic, so a start after a stop during the wait does not
/// wait again. Real time: only the binary sets a backoff. Returns true when a
/// stop ended the wait: the caller must not open the transport.
pub(crate) async fn wait_out_crash_loop_backoff(shutting_down: &AtomicBool) -> bool {
    let Some(delay) = BACKOFF.lock().take() else {
        return false;
    };
    use crate::simulation::TimeSource;
    let time = crate::simulation::RealTime::new();
    let waited = wait_unless(
        delay,
        HOLDER_POLL,
        || shutting_down.load(Ordering::SeqCst),
        |d| time.sleep(d),
    )
    .await;
    let stopped = shutting_down.load(Ordering::SeqCst);
    BACKOFF_PENDING.store(false, Ordering::SeqCst);
    TRANSPORT_WAIT_OVER.notify_waiters();
    if stopped {
        tracing::info!(
            waited_secs = waited.as_secs(),
            "Stopped during the QUIC crash-loop backoff; the transport is not opened"
        );
    } else {
        tracing::info!(
            waited_secs = waited.as_secs(),
            "QUIC crash-loop backoff over; opening the transport"
        );
    }
    stopped
}

/// Set from [`note_backoff`] until [`wait_out_crash_loop_backoff`] ends.
static BACKOFF_PENDING: AtomicBool = AtomicBool::new(false);

/// Wakes [`wait_for_transport_open`] when the backoff wait ends.
static TRANSPORT_WAIT_OVER: tokio::sync::Notify = tokio::sync::Notify::const_new();

/// Return once no crash-loop backoff wait is pending: at once when this start
/// has none, otherwise when the wait ends. The binary's update probation
/// commit (`rollback::commit_probation`) starts its healthy-uptime window from
/// here, so a backoff start, whose transport is closed for the wait, does not
/// count the wait as healthy uptime: a release that crashes as soon as QUIC
/// opens is still rolled back, and a rollback that is retrying
/// (`RollbackUnavailable` leaves its marker for a later crash) is not cleared
/// by the node's own idle time.
///
/// Returns whether it had to wait. The caller starts its window again when it
/// did, and calls this only after a first window, by when the breaker (which
/// runs early in the node's build) has decided.
pub async fn wait_for_transport_open() -> bool {
    wait_while_pending(&BACKOFF_PENDING, &TRANSPORT_WAIT_OVER).await
}

async fn wait_while_pending(pending: &AtomicBool, over: &tokio::sync::Notify) -> bool {
    let mut waited = false;
    loop {
        let notified = over.notified();
        tokio::pin!(notified);
        // Registered before the flag is read, so an end between the read and
        // the await is not missed.
        notified.as_mut().enable();
        if !pending.load(Ordering::SeqCst) {
            return waited;
        }
        waited = true;
        notified.await;
    }
}

/// Sleep `delay` in steps of `poll` with `sleep`, stopping early once
/// `stop` returns true. Returns how long it slept.
async fn wait_unless<Sleep: std::future::Future<Output = ()>>(
    delay: Duration,
    poll: Duration,
    stop: impl Fn() -> bool,
    sleep: impl Fn(Duration) -> Sleep,
) -> Duration {
    let mut slept = Duration::ZERO;
    while slept < delay && !stop() {
        let step = poll.min(delay - slept);
        sleep(step).await;
        slept += step;
    }
    slept
}

/// How long QUIC stays off on a peer after a run with QUIC enabled ended
/// abnormally.
pub(crate) const QUIC_BREAKER_COOLDOWN: Duration = Duration::from_secs(24 * 60 * 60);

/// How long QUIC stays off on a gateway after a run with QUIC enabled ended
/// abnormally. Provisional value (a proposal, not measured).
///
/// A gateway never takes the peer cooldown: once joiners dial a gateway's
/// QUIC endpoint directly (#5940 phase B), a gateway with QUIC off for a day
/// would be unreachable over QUIC for that day. A short cooldown still bounds
/// a crash loop to one restart with QUIC per cooldown.
#[doc(hidden)]
pub const GATEWAY_QUIC_BREAKER_COOLDOWN: Duration = Duration::from_secs(15 * 60);

/// The longest a gateway's cooldown grows to on repeated trips
/// ([`BreakerRole::cooldown`]). Provisional value (a proposal, not measured).
pub(crate) const GATEWAY_QUIC_BREAKER_COOLDOWN_MAX: Duration = Duration::from_secs(4 * 60 * 60);

/// A gateway's trip counts as a repeat of the one before, and doubles the
/// cooldown, when it comes less than this long after that one's cooldown
/// ended; a clean stretch this long starts the count again. Provisional.
pub(crate) const GATEWAY_TRIP_STREAK_RESET: Duration = Duration::from_secs(24 * 60 * 60);

/// A gateway that exits on purpose because its QUIC endpoint died (see
/// `liveness::gateway_quic_restarts`) is restarted by its supervisor with
/// QUIC on the first time within [`ENDPOINT_DOWN_WINDOW`]. Its exit number
/// this many in the window, and any while a trip of the last
/// [`GATEWAY_TRIP_STREAK_RESET`] still counts ([`PriorTrip`]), trips the
/// breaker, so every further death advances the doubling cooldown and the
/// restarts per day are bounded whatever the death rate: one free restart,
/// then two restarts (the death and the cooldown's end) per trip, with
/// cooldowns of 15, 30, 60, 120 and then 240 minutes, at most about 20
/// restarts in the first day and 12 a day after. Provisional value.
#[doc(hidden)]
pub const ENDPOINT_DOWN_EXITS_TO_TRIP: usize = 2;

/// The window [`ENDPOINT_DOWN_EXITS_TO_TRIP`] counts in. Provisional value.
pub(crate) const ENDPOINT_DOWN_WINDOW: Duration = Duration::from_secs(24 * 60 * 60);

/// Which cooldown applies: the node's role at this start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BreakerRole {
    /// A peer: a trip holds for [`QUIC_BREAKER_COOLDOWN`].
    Peer,
    /// A gateway: a trip holds for [`GATEWAY_QUIC_BREAKER_COOLDOWN`],
    /// doubling on each repeat trip up to
    /// [`GATEWAY_QUIC_BREAKER_COOLDOWN_MAX`].
    Gateway,
}

impl BreakerRole {
    pub(crate) fn from_is_gateway(is_gateway: bool) -> Self {
        if is_gateway {
            Self::Gateway
        } else {
            Self::Peer
        }
    }

    /// How long QUIC stays off after the `trips`-th trip in a row (0 and 1
    /// both mean the first). A peer always waits [`QUIC_BREAKER_COOLDOWN`].
    /// A gateway waits [`GATEWAY_QUIC_BREAKER_COOLDOWN`] doubled for each
    /// repeat, at most [`GATEWAY_QUIC_BREAKER_COOLDOWN_MAX`], so a fault that
    /// recurs at every start does not restart a gateway every 15 minutes
    /// forever.
    pub(crate) fn cooldown(self, trips: u32) -> Duration {
        match self {
            Self::Peer => QUIC_BREAKER_COOLDOWN,
            Self::Gateway => {
                let doublings = trips.max(1) - 1;
                GATEWAY_QUIC_BREAKER_COOLDOWN
                    .checked_mul(2u32.saturating_pow(doublings))
                    .unwrap_or(GATEWAY_QUIC_BREAKER_COOLDOWN_MAX)
                    .min(GATEWAY_QUIC_BREAKER_COOLDOWN_MAX)
            }
        }
    }
}

/// A gateway's most recent trip, carried in the markers that follow it so a
/// repeat trip can grow the cooldown ([`BreakerRole::cooldown`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PriorTrip {
    pub tripped_at_secs: u64,
    /// The trips in a row it ended, itself included.
    pub trips: u32,
}

impl PriorTrip {
    /// Whether a trip at `now_secs` repeats this one: it comes less than
    /// [`GATEWAY_TRIP_STREAK_RESET`] after this one's cooldown ended. A trip
    /// time further ahead of the clock than the longest cooldown is ignored.
    fn repeats_at(self, now_secs: u64) -> bool {
        let cooldown_end = self
            .tripped_at_secs
            .saturating_add(BreakerRole::Gateway.cooldown(self.trips).as_secs());
        self.tripped_at_secs <= now_secs.saturating_add(GATEWAY_QUIC_BREAKER_COOLDOWN_MAX.as_secs())
            && now_secs < cooldown_end.saturating_add(GATEWAY_TRIP_STREAK_RESET.as_secs())
    }
}

/// The gateway trip a new marker carries from `previous`, if it still counts
/// at `now_secs`. Always `None` on a peer, whose markers stay as before.
pub(crate) fn prior_trip(
    previous: &Previous,
    now_secs: u64,
    role: BreakerRole,
) -> Option<PriorTrip> {
    if role != BreakerRole::Gateway {
        return None;
    }
    let prior = match previous {
        Previous::Marker(Marker::Tripped {
            tripped_at_secs,
            trips,
        }) => Some(PriorTrip {
            tripped_at_secs: *tripped_at_secs,
            trips: (*trips).max(1),
        }),
        Previous::Marker(
            Marker::Armed { prior_trip, .. } | Marker::ShortRuns { prior_trip, .. },
        ) => *prior_trip,
        Previous::Nothing | Previous::Unreadable | Previous::Marker(Marker::Unknown) => None,
    };
    prior.filter(|prior| prior.repeats_at(now_secs))
}

/// Which trip in a row a trip at `now_secs` after `previous` is: one more
/// than a prior gateway trip that still counts, otherwise the first.
pub(crate) fn trip_number(previous: &Previous, now_secs: u64, role: BreakerRole) -> u32 {
    prior_trip(previous, now_secs, role).map_or(1, |prior| prior.trips.saturating_add(1))
}

/// The trips-in-a-row count of a Tripped marker in `previous` (1 otherwise).
pub(crate) fn tripped_trips(previous: &Previous) -> u32 {
    match previous {
        Previous::Marker(Marker::Tripped { trips, .. }) => (*trips).max(1),
        Previous::Nothing
        | Previous::Unreadable
        | Previous::Marker(Marker::Armed { .. } | Marker::ShortRuns { .. } | Marker::Unknown) => 1,
    }
}

/// Serde: a field left out when 0.
fn is_zero(value: &u32) -> bool {
    *value == 0
}

/// Serde: a first trip is written as before, without the count.
fn is_first_trip(trips: &u32) -> bool {
    *trips <= 1
}

/// Serde: a marker without the count records a first trip.
fn first_trip() -> u32 {
    1
}

/// How much longer than the configured shutdown drain a start waits for a
/// live process holding the marker to exit: the 2 s pause after the drain
/// and a margin.
pub(crate) const HOLDER_EXIT_MARGIN: Duration = Duration::from_secs(10);

/// The longest a start waits for a live holder, whatever the configured
/// drain. When it expires the start arms over the marker without tripping.
pub(crate) const HOLDER_EXIT_MAX: Duration = Duration::from_secs(120);

/// How often the start checks whether that process has exited.
const HOLDER_POLL: Duration = Duration::from_millis(250);

/// The marker's file name in the node's data directory.
pub(crate) const MARKER_FILE: &str = "quic-crash-breaker.json";

/// The legacy exit-status rule's own marker on a unix peer
/// ([`legacy_shadow`]), next to [`MARKER_FILE`]. Only telemetry reads what it
/// decides.
pub(crate) const LEGACY_SHADOW_FILE: &str = "quic-crash-breaker-legacy.json";

/// Environment variable the macOS tray wrapper sets on its child: the path of
/// the note where it records deliberate hard kills.
pub const STOP_NOTE_ENV_VAR: &str = "FREENET_QUIC_BREAKER_STOP_NOTE";

/// The note's file name. The wrapper puts it in its log directory; a node
/// started without [`STOP_NOTE_ENV_VAR`] looks for it in the default log
/// directory.
pub const STOP_NOTE_FILE: &str = "quic-breaker-stop-note";

/// The note keeps at most this many pids, the most recent ones.
const MAX_NOTE_PIDS: usize = 32;

/// Set by [`enable_quic_crash_breaker`]; off in tests and embedders.
static ENABLED: AtomicBool = AtomicBool::new(false);

/// The marker [`gate`] decided to arm, and what has happened since. Written
/// once both the endpoint is up and the signal handlers are installed.
static PENDING: parking_lot::Mutex<Pending> = parking_lot::Mutex::new(Pending {
    marker: None,
    shadow: None,
    quic_up: false,
    signals_ready: false,
    registrar: None,
});

/// Set by [`gate`] when `quic-ignore-crash-breaker` overrode the breaker for
/// this run. Then an endpoint death is not recorded and the node does not
/// exit for it: the next start would ignore the record, so a death at every
/// start would restart the node without bound.
static OVERRIDDEN: AtomicBool = AtomicBool::new(false);

/// Orders exactly two writers of this run's marker: the Windows short-run
/// clear (`short_runs::clear_short_run`) and the endpoint-death record
/// ([`note_endpoint_down_exit_with`]). Each holds it across its
/// read-and-change, so in either order the death record survives: a clear
/// that comes first is followed by the record written again, and a record
/// written first makes the clear leave the marker. Pinned by
/// `the_marker_update_lock_is_taken_by_the_clear_and_the_record`.
///
/// The other writers do not take it, and it does not order them:
/// - arming ([`arm`]) runs under [`PENDING`], as does the record of a death
///   before arming, so those two are ordered by that lock instead, and the
///   clear timer starts only after arming;
/// - the Windows panic hook (`short_runs::mark_quic_panic`) takes no lock,
///   since it runs inside a panic. A caught socket-adapter panic marks the
///   marker before the endpoint goes down, so its write lands before the
///   death record; an unrelated QUIC panic on another thread at the same
///   moment could overwrite the record, which then costs one free restart;
/// - a clean exit's removal ([`clear_if_owned`]) does not run on a death,
///   which ends the process without a clean exit.
pub(super) static MARKER_UPDATE: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

/// Set by [`gate`] when this start decides by short runs
/// ([`decides_by_short_runs`]), whose marker the short-run timer clears, so
/// an endpoint death after that writes it again ([`note_endpoint_down_exit`]).
static DECIDED_BY_SHORT_RUNS: AtomicBool = AtomicBool::new(false);

/// The marker this process armed, as armed, for a short-run start that must
/// write it again after clearing it ([`note_endpoint_down_exit`]).
static ARMED_MARKER: parking_lot::Mutex<Option<Marker>> = parking_lot::Mutex::new(None);

/// On a gateway whose run starts with QUIC kept off by a trip or a cooldown:
/// when that cooldown ends. Read by the transport, which restarts a
/// supervised gateway then ([`quic_restore_at`]).
static QUIC_RESTORE_AT: parking_lot::Mutex<Option<SystemTime>> = parking_lot::Mutex::new(None);

/// When the cooldown that keeps this gateway's QUIC off for the run ends, if
/// one does. `None` on a peer, when QUIC runs, and when QUIC is off for a
/// reason a restart would not cure (a marker that cannot be read or written).
pub(crate) fn quic_restore_at() -> Option<SystemTime> {
    *QUIC_RESTORE_AT.lock()
}

/// Record the cooldown end a gateway restarts at; nothing on a peer.
fn set_quic_restore_at(role: BreakerRole, tripped_at_secs: u64, trips: u32) {
    if role == BreakerRole::Gateway {
        *QUIC_RESTORE_AT.lock() = Some(cooldown_end(tripped_at_secs, role.cooldown(trips)));
    }
}

/// What a trip or cooldown WARN says about when QUIC comes back: a peer at
/// its next start after the cooldown; a gateway that restarts itself
/// (`liveness::gateway_quic_restarts`) at the cooldown's end; any other
/// gateway at a restart after it.
fn restart_note(role: BreakerRole) -> &'static str {
    restart_note_for(
        role,
        super::liveness::gateway_quic_restarts(role == BreakerRole::Gateway),
    )
}

fn restart_note_for(role: BreakerRole, restarts_itself: bool) -> &'static str {
    match (role, restarts_itself) {
        (BreakerRole::Peer, _) => "The first start after the cooldown ends runs QUIC again.",
        (BreakerRole::Gateway, true) => {
            "This gateway restarts itself when the cooldown ends, to turn QUIC back on."
        }
        (BreakerRole::Gateway, false) => {
            "This gateway does not restart itself (it is not run with FREENET_SUPERVISED=1): \
             restart it after the cooldown ends to turn QUIC back on."
        }
    }
}

/// The marker this process armed and its nonce, for the clean exit.
static ARMED: OnceLock<(PathBuf, u64)> = OnceLock::new();

/// The legacy shadow marker this process armed and its nonce, for the clean
/// exit ([`legacy_shadow`]).
static LEGACY_ARMED: OnceLock<(PathBuf, u64)> = OnceLock::new();

/// Turn the breaker on for this process. Called once by the `freenet` binary
/// before the node is built, alongside the other process-level switches
/// (`enable_abort_on_fatal_listener_exit`). A caller must also call
/// [`quic_breaker_signals_ready`] once its SIGTERM handler is installed, or
/// the marker is never armed.
pub fn enable_quic_crash_breaker() {
    ENABLED.store(true, Ordering::Relaxed);
}

/// The `freenet` binary has installed its SIGTERM handler, so a stop from now
/// on reaches the clean exit. Arms a pending marker if the endpoint is
/// already up.
pub fn quic_breaker_signals_ready() {
    let mut pending = PENDING.lock();
    pending.signals_ready = true;
    arm_when_ready(&mut pending);
}

/// Remove the marker this process armed, if it still carries this process's
/// nonce. The `freenet` binary calls this on its clean exits (an operator stop
/// and the auto-update exit) and on no other.
pub fn quic_breaker_clean_exit() {
    if let Some((path, nonce)) = ARMED.get() {
        short_runs::note_marker_cleared();
        if let Err(error) = clear_if_owned(path, *nonce) {
            tracing::warn!(
                %error,
                path = %path.display(),
                "Could not remove the QUIC crash-breaker marker on a clean exit"
            );
        }
    }
    if let Some((path, nonce)) = LEGACY_ARMED.get() {
        if let Err(error) = clear_legacy_if_owned(path, *nonce) {
            tracing::debug!(%error, "Could not remove the legacy QUIC breaker shadow's marker");
        }
    }
}

/// Record that process `pid` is about to be stopped on purpose with a hard
/// kill, so the breaker does not read it as a crash. Best effort.
pub fn record_deliberate_stop(note: &Path, pid: u32) {
    let mut pids = std::fs::read_to_string(note)
        .map(|content| parse_note(&content))
        .unwrap_or_default();
    pids.push(pid);
    let keep_from = pids.len().saturating_sub(MAX_NOTE_PIDS);
    let lines: Vec<String> = pids[keep_from..].iter().map(u32::to_string).collect();
    let _best_effort = write_atomic(note, lines.join("\n").as_bytes());
}

/// The marker's contents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub(crate) enum Marker {
    /// A process with QUIC enabled is running, or ended without a clean exit.
    Armed {
        pid: u32,
        /// The machine's boot identity when armed; `None` if unknown.
        #[serde(default)]
        boot_id: Option<String>,
        nonce: u64,
        armed_at_secs: u64,
        /// When this node recently exited on purpose because its QUIC
        /// endpoint died ([`note_endpoint_down_exit`]), oldest first, at most
        /// [`ENDPOINT_DOWN_EXITS_TO_TRIP`] entries within
        /// [`ENDPOINT_DOWN_WINDOW`]. Carried from run to run. Absent in a
        /// marker from an earlier release, and an earlier release ignores it.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        endpoint_down_secs: Vec<u64>,
        /// A gateway's last trip, while a repeat of it would still grow the
        /// cooldown ([`PriorTrip`]). Absent otherwise, on a peer, and in a
        /// marker from an earlier release, which ignores it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prior_trip: Option<PriorTrip>,
    },
    /// The breaker tripped at this time (seconds since the Unix epoch).
    Tripped {
        tripped_at_secs: u64,
        /// The gateway trips in a row this one ends (1 for a first trip, and
        /// on a peer; then absent, as in a marker from an earlier release,
        /// which ignores it). Sets the cooldown ([`BreakerRole::cooldown`]).
        #[serde(default = "first_trip", skip_serializing_if = "is_first_trip")]
        trips: u32,
    },
    /// A peer, or a gateway on Windows: a run with QUIC enabled started at
    /// this time, after `short_runs` short runs in a row. See
    /// [`short_runs`].
    ShortRuns {
        short_runs: u32,
        started_at_secs: u64,
        nonce: u64,
        /// The run recorded a panic in QUIC code (quinn, quinn-proto or this
        /// crate's QUIC transport) while it was still short. Only such a run
        /// counts. Absent in a marker from an earlier release.
        #[serde(default)]
        quic_panicked: bool,
        /// Where that panic was (`file:line`), for telemetry. Absent when
        /// none was recorded, and in a marker from an earlier release, which
        /// ignores it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        quic_panic_at: Option<String>,
        /// A peer with no UDP to fall back to (#6151): the start-delay step
        /// this run started at after its crash loop was detected
        /// ([`Verdict::Backoff`]); 0 when it was not. Absent when 0, and in a
        /// marker from an earlier release, which ignores it.
        #[serde(default, skip_serializing_if = "is_zero")]
        backoff_step: u32,
        /// As in [`Marker::Armed`].
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        endpoint_down_secs: Vec<u64>,
        /// As in [`Marker::Armed`].
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prior_trip: Option<PriorTrip>,
    },
    /// A state this binary does not know, written by a later release.
    #[serde(other)]
    Unknown,
}

/// What a start found in the data directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Previous {
    /// No marker, or one that is not evidence (damaged, a later release).
    Nothing,
    /// A marker exists but could not be read.
    Unreadable,
    Marker(Marker),
}

/// The inputs to [`verdict`].
pub(crate) struct Facts<'a> {
    pub previous: &'a Previous,
    /// This boot's identity; `None` if unknown.
    pub boot_id: Option<&'a str>,
    /// Whether the pid in an Armed marker is still alive (after the wait).
    pub armed_pid_alive: bool,
    /// The pids the stop note lists.
    pub stopped: &'a [u32],
    pub now_secs: u64,
    pub ignore_breaker: bool,
    /// Decides how long a trip holds.
    pub role: BreakerRole,
}

/// What a start does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// QUIC on; arm the marker once the endpoint and signal handlers are up.
    Arm,
    /// QUIC on without arming: the override is set and the marker cannot be
    /// read.
    RunUnarmed,
    /// Crash evidence: record the trip, QUIC off.
    Trip,
    /// Crash evidence on a peer with no UDP to fall back to (#6151): QUIC
    /// stays on, but the transport opens only after [`backoff_delay`] for
    /// this step, the update check runs within seconds, and the marker records the
    /// step so the next counted run goes one step further.
    Backoff { step: u32 },
    /// QUIC off until the earlier trip's cooldown ends.
    Cool { tripped_at_secs: u64 },
    /// The marker cannot be read: QUIC off for this run.
    Unreadable,
}

impl Verdict {
    /// Whether QUIC runs.
    pub(crate) fn quic_enabled(self) -> bool {
        matches!(self, Self::Arm | Self::RunUnarmed | Self::Backoff { .. })
    }

    /// The name telemetry reports ([`StartReport`]).
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Arm => "arm",
            Self::RunUnarmed => "run_unarmed",
            Self::Trip => "trip",
            Self::Backoff { .. } => "backoff",
            Self::Cool { .. } => "cool",
            Self::Unreadable => "unreadable",
        }
    }
}

/// What [`gate`] returns: the settings to open the endpoint with, and what
/// the breaker decided. The upgrade policy reads the decision, so a refusal
/// it causes can name the breaker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Gated<T> {
    /// `None` when QUIC is off by config or kept off by the breaker.
    pub settings: Option<T>,
    pub breaker: BreakerOutcome,
}

/// What the breaker decided at this start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BreakerOutcome {
    /// The breaker did not run: it is not enabled (tests and embedders) or
    /// `transport-quic` is off.
    NotConsulted,
    /// The breaker ran and decided this.
    Decided(Verdict),
}

impl BreakerOutcome {
    /// The verdict, when the breaker ran and keeps QUIC off for this run.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn suppressing_verdict(self) -> Option<Verdict> {
        match self {
            Self::NotConsulted => None,
            Self::Decided(verdict) => (!verdict.quic_enabled()).then_some(verdict),
        }
    }
}

/// The breaker's whole decision, as described in the module docs.
pub(crate) fn verdict(facts: &Facts<'_>) -> Verdict {
    match facts.previous {
        Previous::Unreadable if facts.ignore_breaker => Verdict::RunUnarmed,
        Previous::Unreadable => Verdict::Unreadable,
        Previous::Marker(Marker::Armed {
            pid,
            boot_id,
            armed_at_secs,
            endpoint_down_secs,
            ..
        }) if !facts.ignore_breaker
            && same_boot(boot_id.as_deref(), facts.boot_id)
            && !facts.armed_pid_alive
            && !facts.stopped.contains(pid) =>
        {
            endpoint_down_verdict(
                endpoint_down_secs,
                *armed_at_secs,
                facts.previous,
                facts.now_secs,
                facts.role,
            )
            .unwrap_or(Verdict::Trip)
        }
        Previous::Marker(Marker::Tripped {
            tripped_at_secs,
            trips,
        }) if !facts.ignore_breaker
            && cooling(
                *tripped_at_secs,
                facts.now_secs,
                facts.role.cooldown(*trips),
            ) =>
        {
            Verdict::Cool {
                tripped_at_secs: *tripped_at_secs,
            }
        }
        Previous::Nothing
        | Previous::Marker(
            Marker::Armed { .. }
            | Marker::Tripped { .. }
            | Marker::ShortRuns { .. }
            | Marker::Unknown,
        ) => Verdict::Arm,
    }
}

/// The entries of `endpoint_down_secs` within [`ENDPOINT_DOWN_WINDOW`] before
/// `now_secs` (a time ahead of the clock does not count), oldest first, at
/// most [`ENDPOINT_DOWN_EXITS_TO_TRIP`].
pub(crate) fn recent_endpoint_down_exits(endpoint_down_secs: &[u64], now_secs: u64) -> Vec<u64> {
    let window = ENDPOINT_DOWN_WINDOW.as_secs();
    let recent: Vec<u64> = endpoint_down_secs
        .iter()
        .copied()
        .filter(|at| *at <= now_secs && now_secs < at.saturating_add(window))
        .collect();
    let skip = recent.len().saturating_sub(ENDPOINT_DOWN_EXITS_TO_TRIP);
    recent[skip..].to_vec()
}

/// The verdict on a run that started at `run_started_secs` and recorded
/// `endpoint_down_secs` in its marker (`previous`), or `None` if it did not
/// end by exiting for an endpoint death. Such an exit restarts QUIC the first
/// time in [`ENDPOINT_DOWN_WINDOW`] and trips after that, or at once while a
/// recent trip still counts, so each further death doubles the cooldown
/// (see [`ENDPOINT_DOWN_EXITS_TO_TRIP`] for the bound this gives).
pub(crate) fn endpoint_down_verdict(
    endpoint_down_secs: &[u64],
    run_started_secs: u64,
    previous: &Previous,
    now_secs: u64,
    role: BreakerRole,
) -> Option<Verdict> {
    let exited_on_endpoint_down = endpoint_down_secs
        .last()
        .is_some_and(|at| *at >= run_started_secs);
    if !exited_on_endpoint_down {
        return None;
    }
    let repeated = recent_endpoint_down_exits(endpoint_down_secs, now_secs).len()
        >= ENDPOINT_DOWN_EXITS_TO_TRIP
        || prior_trip(previous, now_secs, role).is_some();
    Some(if repeated {
        Verdict::Trip
    } else {
        Verdict::Arm
    })
}

/// The endpoint-down exits a new marker carries from `previous`.
pub(crate) fn carried_endpoint_down_exits(previous: &Previous, now_secs: u64) -> Vec<u64> {
    match previous {
        Previous::Marker(
            Marker::Armed {
                endpoint_down_secs, ..
            }
            | Marker::ShortRuns {
                endpoint_down_secs, ..
            },
        ) => recent_endpoint_down_exits(endpoint_down_secs, now_secs),
        Previous::Nothing
        | Previous::Unreadable
        | Previous::Marker(Marker::Tripped { .. } | Marker::Unknown) => Vec::new(),
    }
}

/// What [`note_endpoint_down_exit`] did.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum EndpointDownNote {
    /// Recorded: the node exits, and the next start counts it.
    Recorded,
    /// `quic-ignore-crash-breaker` overrode the breaker for this run.
    Overridden,
    /// No marker of this run to record it in (the breaker is off, the
    /// marker could not be written at arming, or another run replaced it).
    NoMarker,
}

/// Record in this process's marker that the node is about to exit on
/// purpose because its QUIC endpoint died, so the next start can count it
/// ([`endpoint_down_verdict`]). A marker not written yet (the death came
/// before arming) is written now, with the exit noted; on Windows a marker
/// the run already cleared after [`short_runs::SHORT_RUN`] is written again.
/// Returns whether it was recorded, logging why not. The node does not exit
/// for an unrecorded death: the next start could not count it, so a death at
/// every start would restart the node without bound.
pub(crate) fn note_endpoint_down_exit(now: SystemTime) -> bool {
    let recreate = if DECIDED_BY_SHORT_RUNS.load(Ordering::Relaxed) {
        ARMED_MARKER.lock().clone()
    } else {
        None
    };
    let noted = note_endpoint_down_exit_with(
        ARMED.get(),
        &PENDING,
        OVERRIDDEN.load(Ordering::Relaxed),
        unix_secs(now),
        recreate.as_ref(),
    );
    let keeps_running = "this node does not restart itself for it and keeps running, over UDP \
                         while UDP exists and with no transport once it is gone, with QUIC off \
                         until it is restarted";
    match noted {
        Ok(EndpointDownNote::Recorded) => return true,
        Ok(EndpointDownNote::Overridden) => tracing::warn!(
            "The QUIC endpoint died. The crash-loop breaker is overridden \
             (quic-ignore-crash-breaker), so {keeps_running}"
        ),
        Ok(EndpointDownNote::NoMarker) => tracing::warn!(
            "The QUIC endpoint died. This run has no crash-breaker marker to record the exit \
             in, so {keeps_running}"
        ),
        Err((path, error)) => tracing::warn!(
            %error,
            path = %path.display(),
            "The QUIC endpoint died. Recording the exit in the crash-breaker marker failed, \
             so {keeps_running}"
        ),
    }
    false
}

/// [`note_endpoint_down_exit`] with the armed marker `armed` (the static
/// [`ARMED`] outside tests), the not-yet-written marker in `pending`, whether
/// the breaker was `overridden`, and on Windows the marker as armed, to
/// write again if the run cleared it (`recreate`).
fn note_endpoint_down_exit_with(
    armed: Option<&(PathBuf, u64)>,
    pending: &parking_lot::Mutex<Pending>,
    overridden: bool,
    now_secs: u64,
    recreate: Option<&Marker>,
) -> Result<EndpointDownNote, (PathBuf, std::io::Error)> {
    if overridden {
        return Ok(EndpointDownNote::Overridden);
    }
    let _update = MARKER_UPDATE.lock();
    let recorded = |recorded: bool| {
        if recorded {
            EndpointDownNote::Recorded
        } else {
            EndpointDownNote::NoMarker
        }
    };
    if let Some((path, nonce)) = armed {
        return note_endpoint_down_exit_at(path, *nonce, now_secs, recreate)
            .map(recorded)
            .map_err(|error| (path.clone(), error));
    }
    let mut pending = pending.lock();
    let Some((path, marker)) = pending.marker.as_mut() else {
        return Ok(EndpointDownNote::NoMarker);
    };
    if !note_in(marker, now_secs) {
        return Ok(EndpointDownNote::NoMarker);
    }
    write_marker(path, marker)
        .map(|()| EndpointDownNote::Recorded)
        .map_err(|error| (path.clone(), error))
}

/// Note an endpoint-down exit at `now_secs` in `marker`; whether it could.
fn note_in(marker: &mut Marker, now_secs: u64) -> bool {
    match marker {
        Marker::Armed {
            endpoint_down_secs, ..
        }
        | Marker::ShortRuns {
            endpoint_down_secs, ..
        } => {
            endpoint_down_secs.push(now_secs);
            *endpoint_down_secs = recent_endpoint_down_exits(endpoint_down_secs, now_secs);
            true
        }
        Marker::Tripped { .. } | Marker::Unknown => false,
    }
}

/// [`note_endpoint_down_exit`] for the marker at `path` armed with `nonce`.
/// With `recreate` (Windows: the marker as armed), a marker the run already
/// cleared after [`short_runs::SHORT_RUN`] is written again from it, with
/// its carried exits and trip history, so the exit is still counted.
fn note_endpoint_down_exit_at(
    path: &Path,
    nonce: u64,
    now_secs: u64,
    recreate: Option<&Marker>,
) -> std::io::Result<bool> {
    let mut marker = match (read_previous(path), recreate) {
        (Previous::Marker(marker), _) => marker,
        (Previous::Nothing, Some(armed)) => armed.clone(),
        (Previous::Nothing, None) => return Ok(false),
        (Previous::Unreadable, _) => {
            return Err(std::io::Error::other("the marker cannot be read"));
        }
    };
    let owner = match &marker {
        Marker::Armed { nonce, .. } | Marker::ShortRuns { nonce, .. } => *nonce,
        Marker::Tripped { .. } | Marker::Unknown => return Ok(false),
    };
    if owner != nonce || !note_in(&mut marker, now_secs) {
        return Ok(false);
    }
    write_marker(path, &marker).map(|()| true)
}

/// Whether two boot identities are both known and equal.
fn same_boot(then: Option<&str>, now: Option<&str>) -> bool {
    matches!((then, now), (Some(then), Some(now)) if then == now)
}

/// The pid in an Armed marker that the start should treat as a possible live
/// holder: one from this boot (or of unknown boot), and neither this process
/// nor 0. `None` when there is none.
pub(crate) fn holder_of(previous: &Previous, boot_id: Option<&str>, own_pid: u32) -> Option<u32> {
    let Previous::Marker(Marker::Armed {
        pid,
        boot_id: armed_boot,
        ..
    }) = previous
    else {
        return None;
    };
    let rebooted = matches!(
        (armed_boot.as_deref(), boot_id),
        (Some(then), Some(now)) if then != now
    );
    (*pid != own_pid && *pid != 0 && !rebooted).then_some(*pid)
}

/// After the wait for a holder: read the marker again and check whether its
/// holder, if any, is alive now. A holder that exited cleanly has removed its
/// marker; one that crashed during the wait has left it with a dead pid.
pub(crate) fn reread_after_wait(
    path: &Path,
    boot_id: Option<&str>,
    own_pid: u32,
    pid_alive: impl Fn(u32) -> bool,
) -> (Previous, bool) {
    let previous = read_previous(path);
    let alive = holder_of(&previous, boot_id, own_pid).is_some_and(pid_alive);
    (previous, alive)
}

/// What a start read before deciding.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Observed {
    pub previous: Previous,
    pub stopped: Vec<u32>,
}

/// Reconcile the stop note with the marker, then, when QUIC is requested,
/// read the marker. An Armed marker whose pid the note lists was ended on
/// purpose, so it is removed first. A note passed by the wrapper is then
/// deleted (always, once any removal has succeeded, so a failed removal leaves
/// it for the next start). A note found at the default path (`shared`) may be
/// meant for another node, so only the pid that matched this node's marker is
/// taken out of it. `None` when QUIC is not requested.
pub(crate) fn observe(
    quic_requested: bool,
    data_dir: &Path,
    note_path: Option<&Path>,
    shared: bool,
) -> Option<Observed> {
    observe_with(
        quic_requested,
        &marker_path(data_dir),
        note_path,
        shared,
        read_previous,
        remove_if_exists,
    )
}

/// [`observe`] for the marker at `marker` (the breaker's own, or the legacy
/// shadow's on a unix peer, [`legacy_shadow`]) read with `read`, with the
/// marker removal injected, so a test can make it fail.
fn observe_with(
    quic_requested: bool,
    marker: &Path,
    note_path: Option<&Path>,
    shared: bool,
    read: impl Fn(&Path) -> Previous,
    remove_marker: impl Fn(&Path) -> std::io::Result<()>,
) -> Option<Observed> {
    let stopped = note_path
        .and_then(|note| std::fs::read_to_string(note).ok())
        .map(|content| parse_note(&content))
        .unwrap_or_default();
    if let Some(note) = note_path.filter(|_| !stopped.is_empty()) {
        let noted = match read(marker) {
            Previous::Marker(Marker::Armed { pid, .. }) if stopped.contains(&pid) => Some(pid),
            Previous::Nothing | Previous::Unreadable | Previous::Marker(_) => None,
        };
        let removed = noted.is_some_and(|_| remove_marker(marker).is_ok());
        if shared {
            if let Some(pid) = noted.filter(|_| removed) {
                let rest: Vec<String> = stopped
                    .iter()
                    .filter(|p| **p != pid)
                    .map(u32::to_string)
                    .collect();
                let _best_effort = if rest.is_empty() {
                    std::fs::remove_file(note)
                } else {
                    write_atomic(note, rest.join("\n").as_bytes())
                };
            }
        } else if noted.is_none() || removed {
            let _consumed = std::fs::remove_file(note);
        }
    }
    quic_requested.then(|| Observed {
        previous: read(marker),
        stopped,
    })
}

/// The production entry point: apply the breaker to the QUIC `settings`
/// chosen by the config. The returned settings are `None` when the breaker
/// keeps QUIC off, and [`Gated::breaker`] says what it decided.
/// Logs the outcome and updates the transport telemetry. `now` is the wall
/// clock of the caller's `TimeSource`. May wait up to `drain_secs` plus
/// [`HOLDER_EXIT_MARGIN`] for a previous node to exit. Windows decides by
/// [`short_runs::gate`] instead, and does not wait. `role` picks the
/// cooldown ([`BreakerRole::cooldown`]).
pub(crate) async fn gate<T>(
    settings: Option<T>,
    data_dir: &Path,
    ignore_breaker: bool,
    drain_secs: u64,
    now: SystemTime,
    role: BreakerRole,
) -> Gated<T> {
    let not_consulted = |settings| Gated {
        settings,
        breaker: BreakerOutcome::NotConsulted,
    };
    if !ENABLED.load(Ordering::Relaxed) {
        return not_consulted(settings);
    }
    OVERRIDDEN.store(ignore_breaker, Ordering::Relaxed);
    let by_short_runs = decides_by_short_runs(
        cfg!(unix),
        role,
        platform::boot_id().is_some(),
        udp_fallback(),
    );
    DECIDED_BY_SHORT_RUNS.store(by_short_runs, Ordering::Relaxed);
    if by_short_runs {
        // The legacy shadow reads its marker first: on the first start after
        // an upgrade it takes the earlier release's marker as its evidence.
        let legacy = if cfg!(unix) {
            legacy_shadow(settings.is_some(), data_dir, ignore_breaker, now)
        } else {
            None
        };
        let (gated, counted) = short_runs::gate(
            settings,
            data_dir,
            ignore_breaker,
            now,
            role,
            udp_fallback(),
        );
        if let BreakerOutcome::Decided(verdict) = gated.breaker {
            set_start_report(StartReport {
                role,
                rule: Rule::ShortRuns,
                verdict,
                ignore_breaker,
                short_runs: Some(counted.short_runs),
                quic_panic_at: counted.quic_panic_at,
                legacy_verdict: legacy,
            });
        }
        return gated;
    }
    let (note_path, shared) = stop_note();
    let Some(Observed {
        mut previous,
        stopped,
    }) = observe(settings.is_some(), data_dir, note_path.as_deref(), shared)
    else {
        return not_consulted(settings);
    };
    let path = marker_path(data_dir);
    let own_pid = std::process::id();
    let now_secs = unix_secs(now);
    let boot_id = platform::boot_id();
    if boot_id.is_none() {
        tracing::warn!(
            "Could not read this machine's boot identity, so the QUIC crash-loop breaker \
             cannot trip on this start"
        );
    }
    let mut armed_pid_alive = false;
    if let Some(holder) = holder_of(&previous, boot_id.as_deref(), own_pid) {
        if platform::pid_alive(holder) {
            use crate::simulation::TimeSource;
            let wait = holder_exit_wait(drain_secs);
            tracing::info!(
                pid = holder,
                wait_secs = wait.as_secs(),
                "Process {holder} holds the QUIC crash-breaker marker; waiting for it to exit"
            );
            // Real time: only the `freenet` binary reaches here (see `ENABLED`).
            let time = crate::simulation::RealTime::new();
            wait_for_exit(holder, wait, HOLDER_POLL, platform::pid_alive, |d| {
                time.sleep(d)
            })
            .await;
            (previous, armed_pid_alive) =
                reread_after_wait(&path, boot_id.as_deref(), own_pid, platform::pid_alive);
            if armed_pid_alive {
                tracing::warn!(
                    "Another process still holds the QUIC crash-breaker marker after the \
                     wait; arming over it"
                );
            }
        }
    }

    let mut decided = verdict(&Facts {
        previous: &previous,
        boot_id: boot_id.as_deref(),
        armed_pid_alive,
        stopped: &stopped,
        now_secs,
        ignore_breaker,
        role,
    });
    // A gateway does not keep QUIC off over a marker it cannot read: it
    // replaces the marker and runs armed, so a real crash loop still trips.
    // If the marker cannot be removed either, QUIC stays off for the run
    // and no restart is scheduled (one would find the same marker).
    if role == BreakerRole::Gateway && decided == Verdict::Unreadable {
        match remove_if_exists(&path) {
            Ok(()) => {
                tracing::warn!(
                    path = %path.display(),
                    "Could not read the QUIC crash-breaker marker; this gateway replaced it \
                     and runs QUIC with a new one"
                );
                previous = Previous::Nothing;
                decided = Verdict::Arm;
            }
            Err(error) => tracing::error!(
                %error,
                path = %path.display(),
                "Could not read or replace the QUIC crash-breaker marker, so QUIC stays off \
                 on this gateway until it restarts. Fix the file's permissions or remove it"
            ),
        }
    }

    let metrics = &crate::transport::TRANSPORT_METRICS;
    let until_utc = |tripped_at_secs: u64, trips: u32| {
        chrono::DateTime::<chrono::Utc>::from(cooldown_end(tripped_at_secs, role.cooldown(trips)))
            .to_rfc3339()
    };
    match decided {
        Verdict::Arm => {
            let endpoint_down_secs = carried_endpoint_down_exits(&previous, now_secs);
            if !endpoint_down_secs.is_empty() {
                tracing::warn!(
                    recent_exits = endpoint_down_secs.len(),
                    trip_at = ENDPOINT_DOWN_EXITS_TO_TRIP,
                    window_hours = ENDPOINT_DOWN_WINDOW.as_secs() / 3600,
                    "This node recently exited because its QUIC endpoint died; QUIC runs \
                     again, and the next such exit within the window trips the breaker"
                );
            }
            PENDING.lock().marker = Some((
                path,
                Marker::Armed {
                    pid: own_pid,
                    boot_id,
                    nonce: crate::config::GlobalRng::random_u64(),
                    armed_at_secs: now_secs,
                    endpoint_down_secs,
                    prior_trip: prior_trip(&previous, now_secs, role),
                },
            ));
        }
        Verdict::RunUnarmed => tracing::warn!(
            path = %path.display(),
            "The QUIC crash-breaker marker cannot be read; QUIC runs without the breaker \
             because quic-ignore-crash-breaker is set"
        ),
        Verdict::Trip => {
            let trips = trip_number(&previous, now_secs, role);
            // Off even if the write fails: the evidence is in hand now.
            let written = write_marker(
                &path,
                &Marker::Tripped {
                    tripped_at_secs: now_secs,
                    trips,
                },
            );
            metrics.record_quic_breaker_trip();
            metrics.set_quic_breaker_suppressed(true);
            tracing::warn!(
                until = %until_utc(now_secs, trips),
                cooldown_minutes = role.cooldown(trips).as_secs() / 60,
                trips,
                ?role,
                "The previous run with QUIC enabled ended abnormally, so QUIC is off for \
                 this run. {} Set `quic-ignore-crash-breaker = true` to run QUIC anyway",
                restart_note(role)
            );
            match written {
                Ok(()) => set_quic_restore_at(role, now_secs, trips),
                // A restart would find the old marker and trip again, so a
                // gateway does not restart for a trip it could not record.
                Err(error) if role == BreakerRole::Gateway => tracing::error!(
                    %error,
                    path = %path.display(),
                    "Could not record the QUIC crash-breaker trip, so QUIC stays off on this \
                     gateway until it restarts. Fix the data directory's permissions"
                ),
                Err(_) => {}
            }
        }
        Verdict::Cool { tripped_at_secs } => {
            let trips = tripped_trips(&previous);
            metrics.set_quic_breaker_suppressed(true);
            tracing::warn!(
                until = %until_utc(tripped_at_secs, trips),
                trips,
                ?role,
                "QUIC stays off: a run with QUIC enabled ended abnormally and the \
                 cooldown has not ended. {} Set `quic-ignore-crash-breaker = true` to run \
                 QUIC anyway",
                restart_note(role)
            );
            set_quic_restore_at(role, tripped_at_secs, trips);
        }
        // The exit-status rule never backs off: only `short_runs::decide`
        // decides a backoff, and `decides_by_short_runs` sends every node
        // there when there is no UDP.
        Verdict::Backoff { .. } => {}
        Verdict::Unreadable => {
            metrics.set_quic_breaker_suppressed(true);
            tracing::warn!(
                path = %path.display(),
                "Could not read the QUIC crash-breaker marker, so QUIC stays off for this \
                 run. Fix the file's permissions or remove it. Set \
                 `quic-ignore-crash-breaker = true` to run QUIC anyway"
            );
        }
    }
    set_start_report(StartReport {
        role,
        rule: Rule::ExitStatus,
        verdict: decided,
        ignore_breaker,
        short_runs: None,
        quic_panic_at: None,
        legacy_verdict: None,
    });
    Gated {
        settings: settings.filter(|_| decided.quic_enabled()),
        breaker: BreakerOutcome::Decided(decided),
    }
}

/// Whether a start decides by short runs that panicked in QUIC code
/// ([`short_runs`]): every peer (#6151), a gateway on Windows, a gateway
/// on a `unix` platform with no boot identity (`boot_id_known` false), where
/// [`verdict`] cannot tell a reboot from a crash and would never trip, and
/// every node when there is no UDP to fall back to (`udp_fallback` false),
/// because [`verdict`] can only trip to QUIC off and never backs off. A
/// gateway on `unix` with a boot identity and UDP decides by how its last
/// run ended ([`verdict`]).
pub(crate) fn decides_by_short_runs(
    unix: bool,
    role: BreakerRole,
    boot_id_known: bool,
    udp_fallback: bool,
) -> bool {
    !unix || role == BreakerRole::Peer || !boot_id_known || !udp_fallback
}

/// The stop note: the wrapper's when it passed one; otherwise the note at
/// its fixed place, which may be meant for another node (`shared`).
fn stop_note() -> (Option<PathBuf>, bool) {
    match std::env::var_os(STOP_NOTE_ENV_VAR) {
        Some(path) => (Some(PathBuf::from(path)), false),
        None => (
            crate::tracing::tracer::get_log_dir().map(|dir| dir.join(STOP_NOTE_FILE)),
            true,
        ),
    }
}

/// The exit-status rule, the unix peer's breaker before #6151, run on a unix
/// peer as a shadow that never changes whether QUIC runs. It keeps its own
/// marker ([`LEGACY_SHADOW_FILE`]) with the old states and decides on it with
/// [`verdict`], so telemetry can say what the switch to short runs changed:
/// a trip the old rule would have made and the new one did not (a crash
/// loop with no QUIC panic, or a stop the old rule misread as a crash), and
/// the reverse. Returns what the old rule decides, `None` with QUIC not
/// requested (the stop note is still reconciled, as the old rule did).
///
/// Differences from the old rule, all in the shadow only: it does not wait
/// for a live holder of its marker to exit (a holder still alive counts as no
/// crash, as it did after the wait); it arms only when this run's QUIC
/// endpoint comes up, so while the new rule keeps QUIC off it does not arm;
/// and on the first start after the switch it reads the earlier release's
/// marker as its own. Removed with UDP (#6151).
fn legacy_shadow(
    quic_requested: bool,
    data_dir: &Path,
    ignore_breaker: bool,
    now: SystemTime,
) -> Option<Verdict> {
    let (note_path, shared) = stop_note();
    legacy_shadow_at(
        quic_requested,
        data_dir,
        note_path.as_deref(),
        shared,
        LegacyStart {
            boot_id: platform::boot_id(),
            own_pid: std::process::id(),
            stopped: Vec::new(),
            now_secs: unix_secs(now),
            ignore_breaker,
            nonce: crate::config::GlobalRng::random_u64(),
        },
        platform::pid_alive,
        &PENDING,
    )
}

/// [`legacy_shadow`] with the stop note, this start's facts, the platform's
/// pid probe and the pending marker passed in, so a test can drive it. An
/// Armed marker goes into `pending`, to be written once QUIC is up; a
/// Tripped one is written now, and a failed write only loses the shadow's
/// record (the next start decides again on what it finds).
///
/// The Armed marker it leaves is bounded like the breaker's own: a clean
/// exit removes it, and the next start decides on it and replaces it. A
/// process that stays up without QUIC never writes it.
fn legacy_shadow_at(
    quic_requested: bool,
    data_dir: &Path,
    note_path: Option<&Path>,
    shared: bool,
    mut start: LegacyStart,
    pid_alive: impl Fn(u32) -> bool,
    pending: &parking_lot::Mutex<Pending>,
) -> Option<Verdict> {
    let path = legacy_shadow_path(data_dir);
    let version = crate::config::PCK_VERSION;
    let boot_id = start.boot_id.clone();
    let Observed { previous, stopped } = observe_with(
        quic_requested,
        &path,
        note_path,
        shared,
        |path| read_legacy_shadow(path, version, boot_id.as_deref()),
        remove_if_exists,
    )?;
    start.stopped = stopped;
    let (decided, marker) = legacy_shadow_decide(
        previous,
        || read_previous(&marker_path(data_dir)),
        &start,
        pid_alive,
    );
    let stamped = |marker| LegacyShadowFile {
        version: version.to_string(),
        boot_id: start.boot_id.clone(),
        marker,
    };
    match marker {
        Some(armed @ Marker::Armed { .. }) => pending.lock().shadow = Some((path, stamped(armed))),
        Some(tripped) => {
            if let Err(error) = write_legacy_shadow(&path, &stamped(tripped)) {
                tracing::debug!(%error, "Could not record the legacy QUIC breaker shadow's trip");
            }
        }
        None => {}
    }
    Some(decided)
}

/// The legacy shadow's file: its marker, stamped with the release and the
/// machine boot it was written under ([`read_legacy_shadow`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LegacyShadowFile {
    pub version: String,
    #[serde(default)]
    pub boot_id: Option<String>,
    pub marker: Marker,
}

/// The legacy shadow's marker at `path`, as evidence for this start of
/// release `version` on boot `boot_id`. A file from another release is no
/// evidence: after a rollback and a roll forward, an Armed marker left by
/// the earlier run of this release would read as a crash that never
/// happened. An Armed marker from another boot is no evidence either (the
/// old rule never read one as a crash); a Tripped one still holds, as the
/// old rule's trip did across a reboot. A file that does not parse is no
/// evidence; one that cannot be read is [`Previous::Unreadable`].
///
/// Known inaccuracy, telemetry only: a rollback to an earlier release and a
/// return to this same release on the same boot is not detected. An Armed
/// marker this release left before the rollback then passes both checks,
/// and the next start reports a legacy trip for that run, whether or not it
/// crashed. It is rare (a rollback and return within one boot) and changes
/// nothing but `legacy_verdict`. Tying the file to the run before it would
/// need the main marker to outlive a long run, which the short-run timer
/// deletes on purpose.
fn read_legacy_shadow(path: &Path, version: &str, boot_id: Option<&str>) -> Previous {
    match std::fs::read(path) {
        Ok(bytes) => match serde_json::from_slice::<LegacyShadowFile>(&bytes) {
            Ok(file) if file.version != version => Previous::Nothing,
            Ok(LegacyShadowFile {
                marker: Marker::Armed { .. },
                boot_id: stamped,
                ..
            }) if !same_boot(stamped.as_deref(), boot_id) => Previous::Nothing,
            Ok(LegacyShadowFile {
                marker: Marker::Unknown,
                ..
            })
            | Err(_) => Previous::Nothing,
            Ok(file) => Previous::Marker(file.marker),
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Previous::Nothing,
        Err(_) => Previous::Unreadable,
    }
}

fn write_legacy_shadow(path: &Path, file: &LegacyShadowFile) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(file).map_err(std::io::Error::other)?;
    write_atomic(path, &bytes)
}

/// Remove the legacy shadow's file if it holds the Armed marker with
/// `nonce`; the clean exit's counterpart of [`clear_if_owned`].
fn clear_legacy_if_owned(path: &Path, nonce: u64) -> std::io::Result<bool> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    match serde_json::from_slice::<LegacyShadowFile>(&bytes) {
        Ok(LegacyShadowFile {
            marker: Marker::Armed { nonce: owner, .. },
            ..
        }) if owner == nonce => remove_if_exists(path).map(|()| true),
        Ok(_) | Err(_) => Ok(false),
    }
}

/// What [`legacy_shadow_decide`] reads besides the markers.
struct LegacyStart {
    boot_id: Option<String>,
    own_pid: u32,
    /// The pids the stop note lists.
    stopped: Vec<u32>,
    now_secs: u64,
    ignore_breaker: bool,
    /// For the marker it arms.
    nonce: u64,
}

/// The legacy shadow's decision on its own marker `shadow`, or, when it has
/// none, on the breaker's marker (`main`, read only then), and the marker it
/// leaves: Armed, to write once QUIC is up, or Tripped, to write now.
/// `pid_alive` is the platform probe.
fn legacy_shadow_decide(
    shadow: Previous,
    main: impl FnOnce() -> Previous,
    start: &LegacyStart,
    pid_alive: impl Fn(u32) -> bool,
) -> (Verdict, Option<Marker>) {
    let previous = match shadow {
        Previous::Nothing => legacy_evidence(main()),
        shadow @ (Previous::Unreadable | Previous::Marker(_)) => shadow,
    };
    let armed_pid_alive =
        holder_of(&previous, start.boot_id.as_deref(), start.own_pid).is_some_and(pid_alive);
    let decided = verdict(&Facts {
        previous: &previous,
        boot_id: start.boot_id.as_deref(),
        armed_pid_alive,
        stopped: &start.stopped,
        now_secs: start.now_secs,
        ignore_breaker: start.ignore_breaker,
        role: BreakerRole::Peer,
    });
    let marker = match decided {
        Verdict::Arm => Some(Marker::Armed {
            pid: start.own_pid,
            boot_id: start.boot_id.clone(),
            nonce: start.nonce,
            armed_at_secs: start.now_secs,
            endpoint_down_secs: Vec::new(),
            prior_trip: None,
        }),
        Verdict::Trip => Some(Marker::Tripped {
            tripped_at_secs: start.now_secs,
            trips: 1,
        }),
        Verdict::RunUnarmed
        | Verdict::Backoff { .. }
        | Verdict::Cool { .. }
        | Verdict::Unreadable => None,
    };
    (decided, marker)
}

/// The breaker's own marker, read as the legacy shadow's evidence when the
/// shadow has none (the first start after the upgrade from 0.2.159): only an
/// Armed marker, which on a peer only 0.2.159's exit rule writes. A Tripped
/// marker there may be the short-run rule's own trip, which the shadow must
/// not take as the old rule's; anything else is no evidence.
fn legacy_evidence(main: Previous) -> Previous {
    match main {
        Previous::Marker(Marker::Armed { .. }) => main,
        Previous::Nothing
        | Previous::Unreadable
        | Previous::Marker(Marker::Tripped { .. } | Marker::ShortRuns { .. } | Marker::Unknown) => {
            Previous::Nothing
        }
    }
}

pub(crate) fn legacy_shadow_path(data_dir: &Path) -> PathBuf {
    data_dir.join(LEGACY_SHADOW_FILE)
}

/// Which rule decided a start ([`StartReport`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Rule {
    /// [`short_runs`]: runs that ended soon after a panic in QUIC code.
    ShortRuns,
    /// [`verdict`]: how the last run ended (a gateway on Linux or macOS).
    ExitStatus,
}

/// What the breaker decided at this start, for the `quic_breaker_start`
/// telemetry event, which the node sends once per start
/// ([`take_start_report`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StartReport {
    pub role: BreakerRole,
    pub rule: Rule,
    pub verdict: Verdict,
    pub ignore_breaker: bool,
    /// [`Rule::ShortRuns`]: the short runs in a row so far, the last run
    /// included.
    pub short_runs: Option<u32>,
    /// [`Rule::ShortRuns`]: where the last run's QUIC panic was, when it
    /// recorded one and counted.
    pub quic_panic_at: Option<String>,
    /// A unix peer: what the exit-status rule would have decided
    /// ([`legacy_shadow`]).
    pub legacy_verdict: Option<Verdict>,
}

impl StartReport {
    /// The event's payload.
    pub(crate) fn to_json(&self) -> serde_json::Value {
        let mut event = serde_json::json!({
            "role": match self.role {
                BreakerRole::Peer => "peer",
                BreakerRole::Gateway => "gateway",
            },
            "rule": match self.rule {
                Rule::ShortRuns => "short_runs",
                Rule::ExitStatus => "exit_status",
            },
            "verdict": self.verdict.label(),
            "quic_off": !self.verdict.quic_enabled(),
            "overridden": self.ignore_breaker,
        });
        let fields = event
            .as_object_mut()
            .expect("the payload is an object literal");
        if let Some(short_runs) = self.short_runs {
            fields.insert("short_runs".into(), short_runs.into());
        }
        if let Verdict::Backoff { step } = self.verdict {
            fields.insert("backoff_step".into(), step.into());
            fields.insert(
                "backoff_secs".into(),
                backoff_delay_from(step, BACKOFF_FIRST).as_secs().into(),
            );
        }
        if let Some(at) = &self.quic_panic_at {
            fields.insert("quic_panic_at".into(), at.as_str().into());
        }
        if let Some(legacy) = self.legacy_verdict {
            fields.insert("legacy_verdict".into(), legacy.label().into());
            fields.insert("legacy_quic_off".into(), (!legacy.quic_enabled()).into());
        }
        event
    }
}

/// This start's [`StartReport`], until the node sends it.
static START_REPORT: parking_lot::Mutex<Option<StartReport>> = parking_lot::Mutex::new(None);

/// Record this start's report, and log it once at INFO with the payload
/// the event carries, so an operator (and a real-process test, which runs
/// with telemetry off) can read it.
fn set_start_report(report: StartReport) {
    tracing::info!(
        report = %report.to_json(),
        "QUIC crash-loop breaker start report"
    );
    *START_REPORT.lock() = Some(report);
}

/// Take this start's report, to send as the `quic_breaker_start` event.
/// `None` when the breaker did not run, or the report was already taken.
pub(crate) fn take_start_report() -> Option<StartReport> {
    START_REPORT.lock().take()
}

/// How long a start waits for a live holder: the configured drain plus
/// [`HOLDER_EXIT_MARGIN`], at most [`HOLDER_EXIT_MAX`].
fn holder_exit_wait(drain_secs: u64) -> Duration {
    Duration::from_secs(drain_secs)
        .saturating_add(HOLDER_EXIT_MARGIN)
        .min(HOLDER_EXIT_MAX)
}

/// Wait up to `wait` for `pid` to exit, checking every `poll`. Returns
/// whether it exited. `alive` and `sleep` are the platform probe and the
/// clock, injected so a test can run the loop.
async fn wait_for_exit<Sleep: std::future::Future<Output = ()>>(
    pid: u32,
    wait: Duration,
    poll: Duration,
    alive: impl Fn(u32) -> bool,
    sleep: impl Fn(Duration) -> Sleep,
) -> bool {
    let polls = (wait.as_millis() / poll.as_millis().max(1)).max(1);
    for _ in 0..polls {
        if !alive(pid) {
            return true;
        }
        sleep(poll).await;
    }
    !alive(pid)
}

/// The transport is up; `quic_up` says whether its QUIC endpoint was built.
/// Arms a pending marker if the signal handlers are already installed. A
/// start that fails to bind, or whose endpoint could not be built, leaves no
/// marker. `registrar` registers the Windows short-run timer.
pub(crate) fn arm_if_quic_up(quic_up: bool, registrar: Option<&LateRegistrar>) {
    let mut pending = PENDING.lock();
    note_transport_up(&mut pending, quic_up, registrar);
    arm_when_ready(&mut pending);
}

/// Record that the transport is up. `registrar` is kept only for a Windows
/// marker that will be written, and only until it is: a clone left in the
/// static [`PENDING`] would keep the monitor's late tasks alive, and an
/// in-memory simulation node relies on dropping its last registrar to abort
/// them.
fn note_transport_up(pending: &mut Pending, quic_up: bool, registrar: Option<&LateRegistrar>) {
    pending.quic_up = quic_up;
    let needs_timer = quic_up && matches!(pending.marker, Some((_, Marker::ShortRuns { .. })));
    pending.registrar = registrar.filter(|_| needs_timer).cloned();
}

/// What arming waits for.
struct Pending {
    marker: Option<(PathBuf, Marker)>,
    /// The legacy shadow's Armed marker, on a unix peer ([`legacy_shadow`]).
    shadow: Option<(PathBuf, LegacyShadowFile)>,
    quic_up: bool,
    signals_ready: bool,
    registrar: Option<LateRegistrar>,
}

/// Write the pending marker once the endpoint is up and the signal handlers
/// are installed. Called under the `PENDING` lock from both events, so
/// whichever comes second writes it, exactly once.
fn arm_when_ready(pending: &mut Pending) {
    arm_when_ready_into(
        pending,
        &ARMED,
        &LEGACY_ARMED,
        short_runs::record_quic_panics,
    );
}

/// [`arm_when_ready`], recording what was written in `armed` and the legacy
/// shadow's marker in `legacy_armed` (the statics [`ARMED`] and
/// [`LEGACY_ARMED`] outside tests), and installing the short-run panic hook
/// with `record_panics`, which tests replace: the real hook is
/// process-global, so only a test in a child process may install it.
fn arm_when_ready_into(
    pending: &mut Pending,
    armed: &OnceLock<(PathBuf, u64)>,
    legacy_armed: &OnceLock<(PathBuf, u64)>,
    record_panics: fn(PathBuf, u64),
) {
    if !(pending.quic_up && pending.signals_ready) {
        return;
    }
    if let Some((
        path,
        file @ LegacyShadowFile {
            marker: Marker::Armed { nonce, .. },
            ..
        },
    )) = pending.shadow.take()
    {
        match write_legacy_shadow(&path, &file) {
            Ok(()) => {
                let _first_wins = legacy_armed.set((path, nonce));
            }
            Err(error) => tracing::debug!(
                %error,
                "Could not write the legacy QUIC breaker shadow's marker"
            ),
        }
    }
    let registrar = pending.registrar.take();
    let counts_short_runs = matches!(pending.marker, Some((_, Marker::ShortRuns { .. })));
    let as_armed = pending.marker.as_ref().map(|(_, marker)| marker.clone());
    if let Some((path, nonce)) = arm(pending.marker.take()) {
        *ARMED_MARKER.lock() = as_armed;
        if counts_short_runs {
            short_runs::start_clear_timer(registrar.as_ref(), path.clone(), nonce);
            record_panics(path.clone(), nonce);
        }
        // Test builds only: start an injected QUIC panic's countdown now that
        // the marker (and on Windows the panic hook) is in place.
        #[cfg(feature = "quic-panic-injection")]
        super::panic_injection::inject_from_env();
        // One node per process in production; a second keeps the first.
        let _first_wins = armed.set((path, nonce));
    }
}

/// Write `pending`. Returns the marker's path and nonce when it was written.
fn arm(pending: Option<(PathBuf, Marker)>) -> Option<(PathBuf, u64)> {
    let (path, marker) = pending?;
    let (Marker::Armed { nonce, .. } | Marker::ShortRuns { nonce, .. }) = marker else {
        return None;
    };
    match write_marker(&path, &marker) {
        Ok(()) => Some((path, nonce)),
        Err(error) => {
            tracing::warn!(
                %error,
                path = %path.display(),
                "Could not write the QUIC crash-breaker marker; QUIC runs without it"
            );
            None
        }
    }
}

pub(crate) fn marker_path(data_dir: &Path) -> PathBuf {
    data_dir.join(MARKER_FILE)
}

fn read_previous(path: &Path) -> Previous {
    match std::fs::read(path) {
        Ok(bytes) => match serde_json::from_slice::<Marker>(&bytes) {
            Ok(Marker::Unknown) | Err(_) => Previous::Nothing,
            Ok(marker) => Previous::Marker(marker),
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Previous::Nothing,
        Err(_) => Previous::Unreadable,
    }
}

fn unix_secs(time: SystemTime) -> u64 {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn cooldown_end(tripped_at_secs: u64, cooldown: Duration) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(tripped_at_secs.saturating_add(cooldown.as_secs()))
}

/// Whether a trip at `tripped_at_secs` still holds at `now_secs` for
/// `cooldown`. A trip time more than a cooldown in the future (a clock set
/// back a long way) is not honoured, so it cannot hold QUIC off indefinitely.
fn cooling(tripped_at_secs: u64, now_secs: u64, cooldown: Duration) -> bool {
    let cooldown = cooldown.as_secs();
    now_secs < tripped_at_secs.saturating_add(cooldown)
        && tripped_at_secs <= now_secs.saturating_add(cooldown)
}

/// Remove the marker at `path` if it is this run's, with `nonce`; otherwise
/// leave it. Returns whether it removed one. See the module docs for the window
/// between the read and the removal.
fn clear_if_owned(path: &Path, nonce: u64) -> std::io::Result<bool> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    match serde_json::from_slice::<Marker>(&bytes) {
        Ok(Marker::Armed { nonce: owner, .. } | Marker::ShortRuns { nonce: owner, .. })
            if owner == nonce =>
        {
            remove_if_exists(path).map(|()| true)
        }
        _ => Ok(false),
    }
}

fn remove_if_exists(path: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}

fn parse_note(content: &str) -> Vec<u32> {
    content
        .split_whitespace()
        .filter_map(|pid| pid.parse().ok())
        .collect()
}

fn write_marker(path: &Path, marker: &Marker) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(marker).map_err(std::io::Error::other)?;
    write_atomic(path, &bytes)
}

/// Write `bytes` to a sibling temporary file named for this process, then
/// rename it into place, so two writers never share a temporary file.
fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let dir = path
        .parent()
        .ok_or_else(|| std::io::Error::other("path has no parent"))?;
    std::fs::create_dir_all(dir)?;
    let name = path
        .file_name()
        .ok_or_else(|| std::io::Error::other("path has no file name"))?
        .to_string_lossy();
    let tmp = dir.join(format!(".{name}.{}.tmp", std::process::id()));
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path).inspect_err(|_| {
        let _cleanup = std::fs::remove_file(&tmp);
    })
}

/// Thin platform bindings for the evidence. Only their results are decided
/// on; the decision is tested on every platform.
mod platform {
    /// The boot identity: stable for one boot, different after a reboot.
    #[cfg(target_os = "linux")]
    pub(super) fn boot_id() -> Option<String> {
        let id = std::fs::read_to_string("/proc/sys/kernel/random/boot_id").ok()?;
        Some(id.trim().to_string()).filter(|id| !id.is_empty())
    }

    #[cfg(target_os = "macos")]
    pub(super) fn boot_id() -> Option<String> {
        let mut boot = libc::timeval {
            tv_sec: 0,
            tv_usec: 0,
        };
        let mut len = std::mem::size_of::<libc::timeval>();
        // SAFETY: `kern.boottime` is a `struct timeval`; `boot` and `len`
        // describe a buffer of exactly that size.
        let rc = unsafe {
            libc::sysctlbyname(
                c"kern.boottime".as_ptr(),
                (&mut boot as *mut libc::timeval).cast(),
                &mut len,
                std::ptr::null_mut(),
                0,
            )
        };
        (rc == 0 && boot.tv_sec > 0).then(|| format!("{}.{}", boot.tv_sec, boot.tv_usec))
    }

    /// Unknown on other platforms (and unused on Windows, which counts short
    /// runs instead).
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    pub(super) fn boot_id() -> Option<String> {
        None
    }

    /// Whether `pid` is a live process. `EPERM` means it exists. Pid 0 and
    /// pids that do not fit `pid_t` are never alive (`kill(0, 0)` would
    /// signal this process group).
    #[cfg(unix)]
    pub(super) fn pid_alive(pid: u32) -> bool {
        let Ok(pid) = libc::pid_t::try_from(pid) else {
            return false;
        };
        if pid <= 0 {
            return false;
        }
        // SAFETY: signal 0 only checks that `pid` exists and may be signalled.
        let rc = unsafe { libc::kill(pid, 0) };
        rc == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
    }

    /// Unused: Windows counts short runs that panicked in QUIC code instead.
    #[cfg(not(unix))]
    pub(super) fn pid_alive(_pid: u32) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PID: u32 = 4242;
    const OWN: u32 = 99;
    const T0: u64 = 1_800_000_000;
    const BOOT: &str = "boot-a";

    /// The panic-hook installer for tests: the real hook is process-global,
    /// so only the child-process test in `short_runs.rs` installs it.
    fn no_hook(_path: PathBuf, _nonce: u64) {}

    fn armed(pid: u32, boot: Option<&str>) -> Previous {
        Previous::Marker(Marker::Armed {
            prior_trip: None,
            pid,
            boot_id: boot.map(str::to_string),
            nonce: 7,
            armed_at_secs: T0,
            endpoint_down_secs: Vec::new(),
        })
    }

    fn marker_of(previous: Previous) -> Marker {
        let Previous::Marker(marker) = previous else {
            unreachable!()
        };
        marker
    }

    fn facts<'a>(previous: &'a Previous, boot_id: Option<&'a str>) -> Facts<'a> {
        Facts {
            previous,
            boot_id,
            armed_pid_alive: false,
            stopped: &[],
            now_secs: T0 + 30,
            ignore_breaker: false,
            role: BreakerRole::Peer,
        }
    }

    /// Every combination for an Armed marker: it trips only on the same known
    /// boot, with the pid dead, not noted, and no override.
    #[test]
    fn an_armed_marker_trips_only_on_same_boot_dead_pid_and_no_note() {
        for marker_boot in [Some(BOOT), Some("boot-b"), None] {
            for current_boot in [Some(BOOT), None] {
                for alive in [false, true] {
                    for noted in [false, true] {
                        for ignore in [false, true] {
                            let previous = armed(PID, marker_boot);
                            let stopped: &[u32] = if noted { &[1, PID] } else { &[1] };
                            let got = verdict(&Facts {
                                armed_pid_alive: alive,
                                stopped,
                                ignore_breaker: ignore,
                                ..facts(&previous, current_boot)
                            });
                            let same_boot = marker_boot.is_some() && marker_boot == current_boot;
                            let expected = if same_boot && !alive && !noted && !ignore {
                                Verdict::Trip
                            } else {
                                Verdict::Arm
                            };
                            assert_eq!(
                                got, expected,
                                "marker boot {marker_boot:?}, boot {current_boot:?}, \
                                 alive {alive}, noted {noted}, ignore {ignore}"
                            );
                        }
                    }
                }
            }
        }
    }

    /// Which pid is a possible live holder: an Armed marker's, unless it is
    /// this process, 0, or from an earlier boot; none for other markers.
    #[test]
    fn the_holder_of_a_marker() {
        let holder = |previous: &Previous, boot: Option<&str>| holder_of(previous, boot, OWN);
        assert_eq!(holder(&armed(PID, Some(BOOT)), Some(BOOT)), Some(PID));
        assert_eq!(
            holder(&armed(PID, None), Some(BOOT)),
            Some(PID),
            "unknown boot"
        );
        assert_eq!(
            holder(&armed(PID, Some(BOOT)), None),
            Some(PID),
            "unknown boot"
        );
        assert_eq!(
            holder(&armed(PID, Some("boot-b")), Some(BOOT)),
            None,
            "rebooted"
        );
        assert_eq!(
            holder(&armed(OWN, Some(BOOT)), Some(BOOT)),
            None,
            "this process"
        );
        assert_eq!(holder(&armed(0, Some(BOOT)), Some(BOOT)), None, "pid 0");
        let tripped = Previous::Marker(Marker::Tripped {
            trips: 1,
            tripped_at_secs: T0,
        });
        for previous in [Previous::Nothing, Previous::Unreadable, tripped] {
            assert_eq!(holder(&previous, Some(BOOT)), None, "{previous:?}");
        }
    }

    /// After the wait the decision is made on the marker as it is then: a
    /// holder that exited cleanly removed it (arm), one that crashed during
    /// the wait left it with a dead pid (trip), and one still running holds
    /// it (arm over it).
    #[test]
    fn the_marker_is_read_again_after_the_wait() {
        let dir = tempfile::tempdir().unwrap();
        let path = marker_path(dir.path());
        let decide = |previous: &Previous, alive: bool| {
            verdict(&Facts {
                armed_pid_alive: alive,
                ..facts(previous, Some(BOOT))
            })
        };
        let holder = marker_of(armed(PID, Some(BOOT)));

        // Exited cleanly during the wait: its marker is gone.
        let (previous, alive) = reread_after_wait(&path, Some(BOOT), OWN, |_| false);
        assert_eq!((&previous, alive), (&Previous::Nothing, false));
        assert_eq!(decide(&previous, alive), Verdict::Arm);

        // Crashed during the wait: its marker stays, its pid is dead.
        write_marker(&path, &holder).unwrap();
        let (previous, alive) = reread_after_wait(&path, Some(BOOT), OWN, |_| false);
        assert!(!alive);
        assert_eq!(decide(&previous, alive), Verdict::Trip);

        // Still running at the end of the wait.
        let (previous, alive) = reread_after_wait(&path, Some(BOOT), OWN, |pid| pid == PID);
        assert!(alive);
        assert_eq!(decide(&previous, alive), Verdict::Arm);
    }

    fn armed_with_downs(armed_at_secs: u64, downs: &[u64]) -> Previous {
        Previous::Marker(Marker::Armed {
            prior_trip: None,
            pid: PID,
            boot_id: Some(BOOT.to_string()),
            nonce: 7,
            armed_at_secs,
            endpoint_down_secs: downs.to_vec(),
        })
    }

    /// A run that recorded an endpoint-down exit restarts with QUIC on the
    /// first time in the window; the second in the window trips, and so does
    /// any while a recent trip still counts, so each further death doubles
    /// the cooldown. A crash of a run that recorded none still trips.
    #[test]
    fn an_endpoint_down_exit_restarts_quic_once_then_trips() {
        let w = ENDPOINT_DOWN_WINDOW.as_secs();
        let now = T0 + 30;
        let at = |previous: &Previous, now_secs: u64| {
            verdict(&Facts {
                now_secs,
                role: BreakerRole::Gateway,
                ..facts(previous, Some(BOOT))
            })
        };
        assert_eq!(at(&armed_with_downs(T0, &[T0 + 10]), now), Verdict::Arm);
        assert_eq!(
            at(&armed_with_downs(T0, &[T0 - 3600 * 20, T0 + 10]), now),
            Verdict::Trip,
            "the second in the window trips, even many hours later"
        );
        assert_eq!(
            at(&armed_with_downs(T0, &[T0 - w - 1, T0 + 10]), now),
            Verdict::Arm,
            "one outside the window does not count"
        );
        let after_a_trip = Previous::Marker(Marker::Armed {
            pid: PID,
            boot_id: Some(BOOT.to_string()),
            nonce: 7,
            armed_at_secs: T0,
            endpoint_down_secs: vec![T0 + 10],
            prior_trip: Some(PriorTrip {
                tripped_at_secs: T0 - 3600,
                trips: 1,
            }),
        });
        assert_eq!(
            at(&after_a_trip, now),
            Verdict::Trip,
            "while a recent trip counts, every death trips"
        );
        assert_eq!(
            at(&armed_with_downs(T0, &[T0 - 600]), now),
            Verdict::Trip,
            "this run recorded no endpoint-down exit: a crash"
        );
        assert_eq!(
            at(&armed_with_downs(T0, &[]), now),
            Verdict::Trip,
            "no record: a crash"
        );
        // Only the crash conditions decide whether it is evidence at all.
        assert_eq!(
            verdict(&Facts {
                armed_pid_alive: true,
                ..facts(&armed_with_downs(T0, &[T0 + 10]), Some(BOOT))
            }),
            Verdict::Arm
        );
    }

    /// The restarts per day are bounded whatever the death rate: simulate a
    /// gateway whose endpoint dies the moment QUIC comes up, through the real
    /// verdicts and markers, and count the restarts. One free restart, then
    /// two per trip (the death and the cooldown's end), with cooldowns
    /// doubling to the 4 h cap: at most 20 in the first day, 12 a day after.
    #[test]
    fn endpoint_death_restarts_per_day_are_bounded() {
        let gw = BreakerRole::Gateway;
        let decide = |previous: &Previous, now_secs: u64| {
            verdict(&Facts {
                now_secs,
                role: gw,
                ..facts(previous, Some(BOOT))
            })
        };
        // Each start: decide, then either run QUIC and die at once (an
        // endpoint-down exit), or sit out the cooldown and restart at its end.
        let mut previous = Previous::Nothing;
        let mut now = T0;
        let mut restarts: Vec<u64> = Vec::new();
        while now < T0 + 3 * 24 * 3600 {
            match decide(&previous, now) {
                Verdict::Arm => {
                    let mut marker = Marker::Armed {
                        pid: PID,
                        boot_id: Some(BOOT.to_string()),
                        nonce: 7,
                        armed_at_secs: now,
                        endpoint_down_secs: carried_endpoint_down_exits(&previous, now),
                        prior_trip: prior_trip(&previous, now, gw),
                    };
                    now += 1;
                    assert!(note_in(&mut marker, now));
                    previous = Previous::Marker(marker);
                }
                Verdict::Trip => {
                    let trips = trip_number(&previous, now, gw);
                    previous = Previous::Marker(Marker::Tripped {
                        tripped_at_secs: now,
                        trips,
                    });
                    now += gw.cooldown(trips).as_secs().max(90);
                }
                Verdict::Cool { .. }
                | Verdict::Backoff { .. }
                | Verdict::RunUnarmed
                | Verdict::Unreadable => {
                    panic!("every start in this loop comes after its cooldown")
                }
            }
            restarts.push(now);
        }
        let per_day = |day: u64| {
            restarts
                .iter()
                .filter(|at| (T0 + day * 86_400..T0 + (day + 1) * 86_400).contains(*at))
                .count()
        };
        assert!(per_day(0) <= 20, "first day: {}", per_day(0));
        assert!(per_day(1) <= 12, "second day: {}", per_day(1));
        assert!(per_day(2) <= 12, "third day: {}", per_day(2));
        assert!(
            per_day(1) >= 10,
            "the cap is reached, not undercut: {}",
            per_day(1)
        );
    }

    #[test]
    fn recent_endpoint_down_exits_keeps_the_window_and_the_newest() {
        let w = ENDPOINT_DOWN_WINDOW.as_secs();
        assert_eq!(
            recent_endpoint_down_exits(&[T0 - w, T0 - w + 1, T0, T0 + 1], T0),
            vec![T0 - w + 1, T0],
            "outside the window or ahead of the clock does not count"
        );
        assert_eq!(
            recent_endpoint_down_exits(&[T0 - 4, T0 - 3, T0 - 2, T0 - 1], T0),
            vec![T0 - 2, T0 - 1],
            "at most ENDPOINT_DOWN_EXITS_TO_TRIP, the newest"
        );
        assert_eq!(ENDPOINT_DOWN_EXITS_TO_TRIP, 2);
        assert!(
            recent_endpoint_down_exits(&[u64::MAX], u64::MAX).is_empty(),
            "saturation ends the window"
        );
    }

    /// The note is added only to this process's own Armed marker, and the
    /// next start carries the recent ones into its new marker.
    #[test]
    fn an_endpoint_down_exit_is_noted_only_in_the_owners_marker() {
        let dir = tempfile::tempdir().unwrap();
        let path = marker_path(dir.path());
        assert!(
            !note_endpoint_down_exit_at(&path, 7, T0, None).unwrap(),
            "no marker"
        );

        write_marker(&path, &marker_of(armed_with_downs(T0, &[T0 - 30]))).unwrap();
        assert!(
            !note_endpoint_down_exit_at(&path, 8, T0 + 10, None).unwrap(),
            "not ours"
        );
        assert_eq!(read_previous(&path), armed_with_downs(T0, &[T0 - 30]));

        assert!(note_endpoint_down_exit_at(&path, 7, T0 + 10, None).unwrap());
        let noted = read_previous(&path);
        assert_eq!(noted, armed_with_downs(T0, &[T0 - 30, T0 + 10]));
        assert_eq!(
            carried_endpoint_down_exits(&noted, T0 + 20),
            vec![T0 - 30, T0 + 10]
        );
        assert_eq!(
            carried_endpoint_down_exits(&noted, T0 + 10 + ENDPOINT_DOWN_WINDOW.as_secs()),
            Vec::<u64>::new(),
            "all aged out"
        );

        write_marker(
            &path,
            &Marker::Tripped {
                trips: 1,
                tripped_at_secs: T0,
            },
        )
        .unwrap();
        assert!(
            !note_endpoint_down_exit_at(&path, 7, T0, None).unwrap(),
            "not armed"
        );
    }

    /// The trip and cooldown WARNs say when QUIC comes back, and promise a
    /// restart only to a gateway that restarts itself.
    #[test]
    fn the_restart_note_promises_a_restart_only_when_one_comes() {
        assert!(restart_note_for(BreakerRole::Gateway, true).contains("restarts itself"));
        let unsupervised = restart_note_for(BreakerRole::Gateway, false);
        assert!(unsupervised.contains("does not restart itself"));
        assert!(unsupervised.contains("FREENET_SUPERVISED=1"));
        for restarts in [true, false] {
            assert!(restart_note_for(BreakerRole::Peer, restarts).contains("first start"));
        }
        // Nothing in the unit-test binary enables restarts.
        assert_eq!(
            restart_note(BreakerRole::Gateway),
            restart_note_for(BreakerRole::Gateway, false)
        );
    }

    /// A rollback reads this release's markers: the marker type of v0.2.159
    /// and of main before this change (identical; copied verbatim below,
    /// docs dropped) parses every new-format marker, extra fields ignored,
    /// as the same state with the same values. A Tripped marker with a count
    /// reads as a plain trip, which the older release holds for its 24 h.
    /// A unix peer now writes short-run markers (#6151); 0.2.159 reads one
    /// as a known state and replaces it at its next start. The other
    /// direction (old markers into the new type) is
    /// `this_release_reads_the_markers_of_0_2_159`,
    /// `the_trip_count_fields_are_compatible_both_ways` and
    /// `the_endpoint_down_field_is_compatible_both_ways`.
    #[test]
    fn a_release_before_this_change_reads_the_new_markers() {
        #[derive(Debug, PartialEq, Eq, Deserialize)]
        #[serde(tag = "state", rename_all = "snake_case")]
        enum MarkerV0_2_159 {
            Armed {
                pid: u32,
                #[serde(default)]
                boot_id: Option<String>,
                nonce: u64,
                armed_at_secs: u64,
            },
            Tripped {
                tripped_at_secs: u64,
            },
            ShortRuns {
                short_runs: u32,
                started_at_secs: u64,
                nonce: u64,
                #[serde(default)]
                quic_panicked: bool,
            },
            #[serde(other)]
            Unknown,
        }
        let prior_trip = Some(PriorTrip {
            tripped_at_secs: 1,
            trips: 3,
        });
        let cases = [
            (
                Marker::Armed {
                    pid: 5,
                    boot_id: Some("b".into()),
                    nonce: 7,
                    armed_at_secs: 9,
                    endpoint_down_secs: vec![8, 9],
                    prior_trip,
                },
                MarkerV0_2_159::Armed {
                    pid: 5,
                    boot_id: Some("b".into()),
                    nonce: 7,
                    armed_at_secs: 9,
                },
            ),
            (
                Marker::Tripped {
                    tripped_at_secs: 3,
                    trips: 4,
                },
                MarkerV0_2_159::Tripped { tripped_at_secs: 3 },
            ),
            (
                Marker::ShortRuns {
                    endpoint_down_secs: vec![3, 4],
                    short_runs: 2,
                    started_at_secs: 4,
                    nonce: 7,
                    quic_panicked: true,
                    quic_panic_at: Some("quinn-0.11.12/src/endpoint.rs:403".into()),
                    backoff_step: 0,
                    prior_trip,
                },
                MarkerV0_2_159::ShortRuns {
                    short_runs: 2,
                    started_at_secs: 4,
                    nonce: 7,
                    quic_panicked: true,
                },
            ),
            // A marker at a backoff step: the older release ignores the step.
            (
                Marker::ShortRuns {
                    endpoint_down_secs: vec![3, 4],
                    short_runs: 4,
                    started_at_secs: 4,
                    nonce: 7,
                    quic_panicked: false,
                    quic_panic_at: None,
                    backoff_step: 3,
                    prior_trip,
                },
                MarkerV0_2_159::ShortRuns {
                    short_runs: 4,
                    started_at_secs: 4,
                    nonce: 7,
                    quic_panicked: false,
                },
            ),
        ];
        for (new, old) in cases {
            let json = serde_json::to_vec(&new).unwrap();
            assert_eq!(
                serde_json::from_slice::<MarkerV0_2_159>(&json).unwrap(),
                old,
                "{}",
                String::from_utf8_lossy(&json)
            );
        }
    }

    /// Both writers [`MARKER_UPDATE`] orders take it before they read the
    /// marker. Deleting either lock leaves every behaviour test green (a
    /// single-threaded test cannot interleave them), so this pins the lines.
    #[test]
    fn the_marker_update_lock_is_taken_by_the_clear_and_the_record() {
        let flat = |src: &str, signature: &str| -> String {
            production_fn_body(src, signature)
                .lines()
                .map(|line| line.split_once("//").map_or(line, |(code, _)| code))
                .collect::<String>()
                .split_whitespace()
                .collect()
        };
        let clear = flat(include_str!("breaker/short_runs.rs"), "fn clear_short_run(");
        let lock = clear
            .find("let_update=super::MARKER_UPDATE.lock();")
            .expect("the clear takes the lock");
        let read = clear.find("read_previous(path)").expect("the read");
        assert!(lock < read, "before it reads the marker: {clear}");
        assert!(clear[lock..].contains("clear_if_owned(path,nonce)"));

        let record = flat(
            include_str!("breaker.rs"),
            "fn note_endpoint_down_exit_with(",
        );
        let lock = record
            .find("let_update=MARKER_UPDATE.lock();")
            .expect("the record takes the lock");
        let armed = record
            .find("ifletSome((path,nonce))=armed{")
            .expect("the armed-marker path");
        let pending = record
            .find("letmutpending=pending.lock();")
            .expect("the pending-marker path");
        assert!(
            lock < armed && lock < pending,
            "before either write path: {record}"
        );
    }

    /// The Windows rewrite of a cleared marker is wired end to end: arming
    /// keeps the marker as armed, and the endpoint-death record passes it on
    /// off unix. Neither runs in a unix unit test (arming writes through
    /// statics and the branch is Windows-only), so this pins the lines;
    /// `an_endpoint_down_exit_is_recorded_in_every_marker_kind` tests the
    /// rewrite itself.
    #[test]
    fn the_windows_marker_rewrite_is_wired() {
        let src = include_str!("breaker.rs");
        let flat = |signature: &str| -> String {
            production_fn_body(src, signature)
                .lines()
                .map(|line| line.split_once("//").map_or(line, |(code, _)| code))
                .collect::<String>()
                .split_whitespace()
                .collect()
        };
        let arming = flat("fn arm_when_ready_into(");
        let kept = arming
            .find("letas_armed=pending.marker.as_ref().map(|(_,marker)|marker.clone());")
            .expect("arming keeps the marker before writing it");
        let written = arming
            .find(
                "ifletSome((path,nonce))=arm(pending.marker.take()){*ARMED_MARKER.lock()=as_armed;",
            )
            .expect("and stores it once written");
        assert!(kept < written);
        let note = flat("pub(crate) fn note_endpoint_down_exit(");
        assert!(
            note.contains(
                "letrecreate=ifDECIDED_BY_SHORT_RUNS.load(Ordering::Relaxed){ARMED_MARKER.lock().clone()}else{None};"
            ),
            "off unix the record gets the marker as armed: {note}"
        );
        assert!(
            note.contains("unix_secs(now),recreate.as_ref(),);"),
            "and passes it on: {note}"
        );
    }

    /// A death before arming writes the pending marker with the exit noted,
    /// so the next start counts it; with nothing pending (the breaker off or
    /// overridden) nothing is recorded, and an armed marker is used as is.
    #[test]
    fn a_death_before_arming_writes_the_pending_marker() {
        let dir = tempfile::tempdir().unwrap();
        let path = marker_path(dir.path());
        let pending = |marker| {
            parking_lot::Mutex::new(Pending {
                marker,
                shadow: None,
                quic_up: false,
                signals_ready: false,
                registrar: None,
            })
        };
        let nothing = pending(None);
        assert_eq!(
            note_endpoint_down_exit_with(None, &nothing, false, T0, None).unwrap(),
            EndpointDownNote::NoMarker
        );
        assert_eq!(read_previous(&path), Previous::Nothing);

        let armed_marker = marker_of(armed_with_downs(T0, &[]));
        let waiting = pending(Some((path.clone(), armed_marker)));
        assert_eq!(
            note_endpoint_down_exit_with(None, &waiting, true, T0 + 3, None).unwrap(),
            EndpointDownNote::Overridden,
            "under the override nothing is recorded"
        );
        assert_eq!(read_previous(&path), Previous::Nothing);
        assert_eq!(
            note_endpoint_down_exit_with(None, &waiting, false, T0 + 3, None).unwrap(),
            EndpointDownNote::Recorded
        );
        assert_eq!(read_previous(&path), armed_with_downs(T0, &[T0 + 3]));
        assert_eq!(
            verdict(&Facts {
                now_secs: T0 + 30,
                ..facts(&read_previous(&path), Some(BOOT))
            }),
            Verdict::Arm,
            "the next start counts it as an endpoint-down exit, not a crash"
        );

        let owner = (path.clone(), 7);
        assert_eq!(
            note_endpoint_down_exit_with(Some(&owner), &nothing, true, T0 + 9, None).unwrap(),
            EndpointDownNote::Overridden
        );
        assert_eq!(
            note_endpoint_down_exit_with(Some(&owner), &nothing, false, T0 + 9, None).unwrap(),
            EndpointDownNote::Recorded
        );
        assert_eq!(
            read_previous(&path),
            armed_with_downs(T0, &[T0 + 3, T0 + 9])
        );
    }

    /// A gateway's cooldown doubles per repeat trip up to the cap; a peer's
    /// never changes.
    #[test]
    fn a_gateway_cooldown_doubles_up_to_the_cap() {
        let min = |m: u64| Duration::from_secs(m * 60);
        let gw = |trips| BreakerRole::Gateway.cooldown(trips);
        assert_eq!(gw(0), min(15));
        assert_eq!(gw(1), min(15));
        assert_eq!(gw(2), min(30));
        assert_eq!(gw(3), min(60));
        assert_eq!(gw(4), min(120));
        assert_eq!(gw(5), min(240));
        assert_eq!(gw(6), min(240), "capped");
        assert_eq!(
            gw(u32::MAX),
            GATEWAY_QUIC_BREAKER_COOLDOWN_MAX,
            "no overflow"
        );
        assert_eq!(GATEWAY_QUIC_BREAKER_COOLDOWN_MAX, min(240));
        for trips in [0, 1, 2, 9, u32::MAX] {
            assert_eq!(BreakerRole::Peer.cooldown(trips), QUIC_BREAKER_COOLDOWN);
        }
    }

    /// A trip counts as a repeat while it comes less than a day after the
    /// previous trip's cooldown ended; the markers in between carry that
    /// trip; a peer never carries one.
    #[test]
    fn a_repeat_trip_is_one_within_a_day_of_the_last_cooldown() {
        let day = GATEWAY_TRIP_STREAK_RESET.as_secs();
        let tripped = |trips| {
            Previous::Marker(Marker::Tripped {
                tripped_at_secs: T0,
                trips,
            })
        };
        let gw = BreakerRole::Gateway;
        // The second trip's cooldown, 30 min, ends at T0 + 1800.
        let end2 = T0 + 1800;
        assert_eq!(trip_number(&tripped(2), end2 + day - 1, gw), 3);
        assert_eq!(trip_number(&tripped(2), end2 + day, gw), 1, "a clean day");
        assert_eq!(
            trip_number(&tripped(0), T0 + 60, gw),
            2,
            "0 reads as a first trip"
        );
        assert_eq!(trip_number(&tripped(2), T0 + 60, BreakerRole::Peer), 1);
        assert_eq!(prior_trip(&tripped(2), T0 + 60, BreakerRole::Peer), None);
        assert_eq!(trip_number(&Previous::Nothing, T0, gw), 1);
        // Carried through the markers that follow, until it stops counting.
        let prior = PriorTrip {
            tripped_at_secs: T0,
            trips: 2,
        };
        assert_eq!(prior_trip(&tripped(2), T0 + 3600, gw), Some(prior));
        let armed = Previous::Marker(Marker::Armed {
            pid: PID,
            boot_id: None,
            nonce: 7,
            armed_at_secs: T0 + 3600,
            endpoint_down_secs: Vec::new(),
            prior_trip: Some(prior),
        });
        assert_eq!(trip_number(&armed, T0 + 7200, gw), 3);
        assert_eq!(prior_trip(&armed, end2 + day, gw), None);
        let short_runs = Previous::Marker(Marker::ShortRuns {
            endpoint_down_secs: Vec::new(),
            short_runs: 1,
            started_at_secs: T0 + 3600,
            nonce: 7,
            quic_panicked: true,
            quic_panic_at: None,
            backoff_step: 0,
            prior_trip: Some(prior),
        });
        assert_eq!(trip_number(&short_runs, T0 + 7200, gw), 3);
        // A trip time far ahead of the clock is not honoured.
        let ahead = GATEWAY_QUIC_BREAKER_COOLDOWN_MAX.as_secs() + 1;
        assert_eq!(trip_number(&tripped(2), T0 - ahead, gw), 1);
        // The streak sets how long a Tripped marker cools.
        let facts_at = |previous: &Previous, now_secs| {
            verdict(&Facts {
                now_secs,
                role: gw,
                ..facts(previous, Some(BOOT))
            })
        };
        assert_eq!(
            facts_at(&tripped(2), T0 + 1799),
            Verdict::Cool {
                tripped_at_secs: T0
            }
        );
        assert_eq!(facts_at(&tripped(2), T0 + 1800), Verdict::Arm);
        assert_eq!(tripped_trips(&tripped(3)), 3);
        assert_eq!(tripped_trips(&tripped(0)), 1);
        assert_eq!(tripped_trips(&Previous::Nothing), 1);
    }

    /// A first trip, and a marker with no prior trip, are written exactly as
    /// an earlier release writes them, and its markers parse.
    #[test]
    fn the_trip_count_fields_are_compatible_both_ways() {
        let first = Marker::Tripped {
            tripped_at_secs: 3,
            trips: 1,
        };
        let old = r#"{"state":"tripped","tripped_at_secs":3}"#;
        assert_eq!(serde_json::to_string(&first).unwrap(), old);
        assert_eq!(
            serde_json::from_str::<Marker>(old).unwrap(),
            first,
            "a marker without the count reads back as the same first trip"
        );
        let repeat = Marker::Tripped {
            tripped_at_secs: 3,
            trips: 2,
        };
        let json = serde_json::to_string(&repeat).unwrap();
        assert_eq!(json, r#"{"state":"tripped","tripped_at_secs":3,"trips":2}"#);
        assert_eq!(serde_json::from_str::<Marker>(&json).unwrap(), repeat);
        let armed = Marker::Armed {
            pid: 5,
            boot_id: Some("b".into()),
            nonce: 7,
            armed_at_secs: 9,
            endpoint_down_secs: Vec::new(),
            prior_trip: Some(PriorTrip {
                tripped_at_secs: 1,
                trips: 2,
            }),
        };
        let json = serde_json::to_string(&armed).unwrap();
        assert_eq!(serde_json::from_str::<Marker>(&json).unwrap(), armed);
    }

    /// An endpoint-down exit is recorded the same way in a unix and a
    /// Windows marker; on Windows a marker the run already cleared is written
    /// again from the marker as armed, keeping its carried exits and trip
    /// history, so the exit still counts.
    #[test]
    fn an_endpoint_down_exit_is_recorded_in_every_marker_kind() {
        let mut armed = marker_of(armed_with_downs(T0, &[]));
        assert!(note_in(&mut armed, T0 + 5));
        assert_eq!(armed, marker_of(armed_with_downs(T0, &[T0 + 5])));
        let prior_trip = Some(PriorTrip {
            tripped_at_secs: T0 - 3600,
            trips: 2,
        });
        let short_runs = |endpoint_down_secs: Vec<u64>| Marker::ShortRuns {
            short_runs: 1,
            started_at_secs: T0,
            nonce: 7,
            quic_panicked: false,
            quic_panic_at: None,
            backoff_step: 0,
            endpoint_down_secs,
            prior_trip,
        };
        let mut marker = short_runs(vec![]);
        assert!(note_in(&mut marker, T0 + 5));
        assert_eq!(marker, short_runs(vec![T0 + 5]));
        let mut tripped = Marker::Tripped {
            tripped_at_secs: T0,
            trips: 1,
        };
        assert!(!note_in(&mut tripped, T0 + 5));

        let dir = tempfile::tempdir().unwrap();
        let path = marker_path(dir.path());
        assert!(
            !note_endpoint_down_exit_at(&path, 7, T0, None).unwrap(),
            "unix: a cleared marker is not recreated"
        );
        let as_armed = short_runs(vec![T0 - 7200]);
        assert!(note_endpoint_down_exit_at(&path, 7, T0 + 900, Some(&as_armed)).unwrap());
        assert_eq!(
            read_previous(&path),
            Previous::Marker(short_runs(vec![T0 - 7200, T0 + 900])),
            "Windows: written again from the marker as armed"
        );
        assert!(
            !note_endpoint_down_exit_at(&path, 8, T0, Some(&as_armed)).unwrap(),
            "not ours"
        );
    }

    /// A marker from an earlier release (no field) still parses, and a marker
    /// with no endpoint-down exits is written exactly as before, so a
    /// rollback reads it.
    #[test]
    fn the_endpoint_down_field_is_compatible_both_ways() {
        let old = r#"{"state":"armed","pid":5,"boot_id":"b","nonce":7,"armed_at_secs":9}"#;
        let parsed: Marker = serde_json::from_str(old).unwrap();
        assert_eq!(
            parsed,
            Marker::Armed {
                prior_trip: None,
                pid: 5,
                boot_id: Some("b".into()),
                nonce: 7,
                armed_at_secs: 9,
                endpoint_down_secs: Vec::new(),
            }
        );
        assert_eq!(serde_json::to_string(&parsed).unwrap(), old);
    }

    /// A gateway's trip holds for the gateway cooldown, never the peer's
    /// 24 h; a peer's trip still holds for the full peer cooldown.
    #[test]
    fn a_gateway_trip_holds_only_for_the_gateway_cooldown() {
        let g = GATEWAY_QUIC_BREAKER_COOLDOWN.as_secs();
        assert!(g < QUIC_BREAKER_COOLDOWN.as_secs());
        let previous = Previous::Marker(Marker::Tripped {
            trips: 1,
            tripped_at_secs: T0,
        });
        let at = |now_secs: u64, role: BreakerRole| {
            verdict(&Facts {
                now_secs,
                role,
                ..facts(&previous, Some(BOOT))
            })
        };
        let cool = Verdict::Cool {
            tripped_at_secs: T0,
        };
        assert_eq!(at(T0 + g - 1, BreakerRole::Gateway), cool);
        assert_eq!(
            at(T0 + g, BreakerRole::Gateway),
            Verdict::Arm,
            "a gateway runs QUIC again one gateway cooldown later"
        );
        assert_eq!(
            at(T0 + g, BreakerRole::Peer),
            cool,
            "a peer still waits out the peer cooldown"
        );
        assert_eq!(
            at(T0 + QUIC_BREAKER_COOLDOWN.as_secs() - 1, BreakerRole::Peer),
            cool
        );
        assert_eq!(
            at(T0 - g - 1, BreakerRole::Gateway),
            Verdict::Arm,
            "a gateway does not honour a trip further ahead than its cooldown"
        );
        // A crash on a gateway is still a trip: only how long it holds differs.
        let armed = armed(PID, Some(BOOT));
        assert_eq!(
            verdict(&Facts {
                role: BreakerRole::Gateway,
                ..facts(&armed, Some(BOOT))
            }),
            Verdict::Trip
        );
        assert_eq!(BreakerRole::from_is_gateway(true), BreakerRole::Gateway);
        assert_eq!(BreakerRole::from_is_gateway(false), BreakerRole::Peer);
    }

    #[test]
    fn a_trip_holds_for_the_cooldown_unless_overridden() {
        let c = QUIC_BREAKER_COOLDOWN.as_secs();
        let previous = Previous::Marker(Marker::Tripped {
            trips: 1,
            tripped_at_secs: T0,
        });
        let at = |now_secs: u64, ignore_breaker: bool| {
            verdict(&Facts {
                now_secs,
                ignore_breaker,
                ..facts(&previous, Some(BOOT))
            })
        };
        let cool = Verdict::Cool {
            tripped_at_secs: T0,
        };
        assert_eq!(at(T0, false), cool);
        assert_eq!(at(T0 + c - 1, false), cool);
        assert_eq!(
            at(T0 + c, false),
            Verdict::Arm,
            "exactly one cooldown later"
        );
        assert_eq!(at(T0 - c, false), cool, "a trip one cooldown ahead holds");
        assert_eq!(
            at(T0 - c - 1, false),
            Verdict::Arm,
            "further ahead does not"
        );
        assert_eq!(at(T0, true), Verdict::Arm, "the override");
        assert!(
            !cooling(u64::MAX, 0, QUIC_BREAKER_COOLDOWN),
            "saturation ends the cooldown"
        );
    }

    #[test]
    fn other_previous_states() {
        let check = |previous: Previous, ignore_breaker: bool, expected: Verdict| {
            let got = verdict(&Facts {
                ignore_breaker,
                ..facts(&previous, Some(BOOT))
            });
            assert_eq!(got, expected, "{previous:?} ignore {ignore_breaker}");
        };
        check(Previous::Nothing, false, Verdict::Arm);
        check(Previous::Nothing, true, Verdict::Arm);
        check(Previous::Unreadable, false, Verdict::Unreadable);
        check(Previous::Unreadable, true, Verdict::RunUnarmed);
        check(Previous::Marker(Marker::Unknown), false, Verdict::Arm);
        assert!(!Verdict::Trip.quic_enabled());
        assert!(!Verdict::Unreadable.quic_enabled());
        assert!(!Verdict::Cool { tripped_at_secs: 0 }.quic_enabled());
        assert!(Verdict::Arm.quic_enabled() && Verdict::RunUnarmed.quic_enabled());
    }

    /// Every start, QUIC on or off, removes an Armed marker whose pid the note
    /// lists and then deletes the note; the marker is read only with QUIC on.
    #[test]
    fn observe_reconciles_the_note_on_every_start() {
        let dir = tempfile::tempdir().unwrap();
        let path = marker_path(dir.path());
        let note = dir.path().join("note");

        // QUIC off: the noted marker is removed, then the note.
        write_marker(&path, &marker_of(armed(PID, Some(BOOT)))).unwrap();
        record_deliberate_stop(&note, PID);
        assert_eq!(observe(false, dir.path(), Some(&note), false), None);
        assert!(!path.exists(), "the noted marker is removed with QUIC off");
        assert!(!note.exists(), "then the note");

        // A marker the note does not name is kept, and reported.
        let other = marker_of(armed(PID + 1, Some(BOOT)));
        write_marker(&path, &other).unwrap();
        record_deliberate_stop(&note, PID);
        assert_eq!(
            observe(true, dir.path(), Some(&note), false),
            Some(Observed {
                previous: Previous::Marker(other),
                stopped: vec![PID],
            })
        );
        assert!(!note.exists());

        // A removal that fails keeps the note for the next start.
        write_marker(&path, &marker_of(armed(PID, Some(BOOT)))).unwrap();
        record_deliberate_stop(&note, PID);
        let failing = |_: &Path| Err(std::io::Error::other("cannot remove"));
        observe_with(
            false,
            &marker_path(dir.path()),
            Some(&note),
            false,
            read_previous,
            failing,
        );
        assert!(path.exists(), "removal failed");
        assert!(note.exists(), "so the note is kept");
        observe(false, dir.path(), Some(&note), false);
        assert!(
            !path.exists() && !note.exists(),
            "the next start finishes it"
        );

        // No note: nothing to reconcile.
        assert_eq!(
            observe(true, dir.path().join("empty").as_path(), None, false).map(|o| o.stopped),
            Some(Vec::new())
        );
    }

    /// A note found at the default path may be meant for another node: a
    /// start takes out only the pid that matched its own marker, and leaves a
    /// note that names no pid of its own untouched.
    #[test]
    fn observe_keeps_other_nodes_entries_in_a_shared_note() {
        let dir = tempfile::tempdir().unwrap();
        let path = marker_path(dir.path());
        let note = dir.path().join("note");

        // Nothing of ours in it: left as it is, QUIC on or off.
        record_deliberate_stop(&note, PID + 1);
        observe(false, dir.path(), Some(&note), true);
        observe(true, dir.path(), Some(&note), true);
        assert_eq!(
            std::fs::read_to_string(&note).unwrap(),
            (PID + 1).to_string()
        );

        // Our marker's pid in it: the marker goes, and only that pid.
        write_marker(&path, &marker_of(armed(PID, Some(BOOT)))).unwrap();
        record_deliberate_stop(&note, PID);
        observe(true, dir.path(), Some(&note), true);
        assert!(!path.exists(), "the noted marker is removed");
        assert_eq!(
            parse_note(&std::fs::read_to_string(&note).unwrap()),
            vec![PID + 1]
        );

        // A note left empty is deleted.
        std::fs::remove_file(&note).unwrap();
        write_marker(&path, &marker_of(armed(PID, Some(BOOT)))).unwrap();
        record_deliberate_stop(&note, PID);
        observe(false, dir.path(), Some(&note), true);
        assert!(!path.exists() && !note.exists());
    }

    #[test]
    fn reading_the_previous_marker() {
        let dir = tempfile::tempdir().unwrap();
        let path = marker_path(dir.path());
        assert_eq!(read_previous(&path), Previous::Nothing);
        std::fs::write(&path, b"not json").unwrap();
        assert_eq!(read_previous(&path), Previous::Nothing, "damaged");
        std::fs::write(&path, br#"{"state":"something_new","x":1}"#).unwrap();
        assert_eq!(read_previous(&path), Previous::Nothing, "a later release");
        let marker = Marker::Tripped {
            trips: 1,
            tripped_at_secs: 3,
        };
        write_marker(&path, &marker).unwrap();
        assert_eq!(read_previous(&path), Previous::Marker(marker));
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        assert_eq!(read_previous(&path), Previous::Unreadable, "a directory");
    }

    #[test]
    fn a_clean_exit_removes_only_the_marker_with_its_nonce() {
        let dir = tempfile::tempdir().unwrap();
        let path = marker_path(dir.path());
        assert!(!clear_if_owned(&path, 7).unwrap(), "absent: fine");
        write_marker(&path, &marker_of(armed(PID, Some(BOOT)))).unwrap();
        assert!(!clear_if_owned(&path, 8).unwrap());
        assert!(path.exists(), "another nonce is left in place");
        assert!(clear_if_owned(&path, 7).unwrap());
        assert!(!path.exists());
        write_marker(
            &path,
            &Marker::Tripped {
                trips: 1,
                tripped_at_secs: 1,
            },
        )
        .unwrap();
        assert!(!clear_if_owned(&path, 7).unwrap());
        assert!(path.exists(), "a trip is never removed by a clean exit");
    }

    #[test]
    fn the_note_is_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let note = dir.path().join("note");
        for pid in 1..=MAX_NOTE_PIDS as u32 + 3 {
            record_deliberate_stop(&note, pid);
        }
        let pids = parse_note(&std::fs::read_to_string(&note).unwrap());
        assert_eq!(pids.len(), MAX_NOTE_PIDS, "bounded");
        assert_eq!(pids.first(), Some(&4), "the oldest are dropped");
    }

    /// The marker is written once both the endpoint is up and the signal
    /// handlers are installed, whichever comes second, and only with the
    /// endpoint up.
    #[test]
    fn arming_waits_for_the_endpoint_and_the_signal_handlers() {
        let dir = tempfile::tempdir().unwrap();
        let path = marker_path(dir.path());
        let marker = marker_of(armed(PID, Some(BOOT)));
        let pending = |quic_up, signals_ready| Pending {
            marker: Some((path.clone(), marker.clone())),
            shadow: None,
            quic_up,
            signals_ready,
            registrar: None,
        };
        for (quic_up, signals_ready) in [(false, false), (true, false), (false, true)] {
            let mut p = pending(quic_up, signals_ready);
            arm_when_ready(&mut p);
            assert!(!path.exists(), "up {quic_up}, signals {signals_ready}");
            assert!(p.marker.is_some(), "still pending");
        }
        let mut p = pending(true, true);
        arm_when_ready(&mut p);
        assert_eq!(read_previous(&path), Previous::Marker(marker));
        assert!(p.marker.is_none(), "written once");
        assert_eq!(arm(None), None, "nothing pending");
    }

    /// The clean exit finds a Windows marker through what arming recorded, so
    /// a marker that was written must be recorded with its path and nonce, and
    /// one that could not be written must not.
    #[test]
    fn arming_a_short_run_marker_records_it_for_the_clean_exit() {
        let dir = tempfile::tempdir().unwrap();
        let path = marker_path(dir.path());
        let marker = Marker::ShortRuns {
            endpoint_down_secs: Vec::new(),
            prior_trip: None,
            short_runs: 1,
            started_at_secs: T0,
            nonce: 7,
            quic_panicked: false,
            quic_panic_at: None,
            backoff_step: 0,
        };
        let pending = |path: &Path| Pending {
            marker: Some((path.to_path_buf(), marker.clone())),
            shadow: None,
            quic_up: true,
            signals_ready: true,
            registrar: None,
        };

        let armed = OnceLock::new();
        arm_when_ready_into(&mut pending(&path), &armed, &OnceLock::new(), no_hook);
        assert_eq!(armed.get(), Some(&(path.clone(), 7)));
        assert!(clear_if_owned(&path, armed.get().unwrap().1).unwrap());

        let blocker = dir.path().join("a-file");
        std::fs::write(&blocker, b"").unwrap();
        let armed = OnceLock::new();
        arm_when_ready_into(
            &mut pending(&marker_path(&blocker)),
            &armed,
            &OnceLock::new(),
            no_hook,
        );
        assert_eq!(armed.get(), None, "the write failed");
    }

    /// The registrar stays in the static only for a Windows marker about to be
    /// written. Anywhere else it would keep the monitor's late tasks alive past
    /// their in-memory node, so they would never be aborted.
    #[tokio::test]
    async fn the_registrar_is_kept_only_for_a_short_run_marker_until_it_is_written() {
        let monitor = crate::node::background_task_monitor::BackgroundTaskMonitor::new();
        let registrar = monitor.late_registrar();
        let dir = tempfile::tempdir().unwrap();
        let path = marker_path(dir.path());
        let short_run_marker = Marker::ShortRuns {
            endpoint_down_secs: Vec::new(),
            prior_trip: None,
            short_runs: 0,
            started_at_secs: T0,
            nonce: 7,
            quic_panicked: false,
            quic_panic_at: None,
            backoff_step: 0,
        };
        let pending = |marker: Option<Marker>, signals_ready| Pending {
            marker: marker.map(|marker| (path.clone(), marker)),
            shadow: None,
            quic_up: false,
            signals_ready,
            registrar: None,
        };

        for marker in [None, Some(marker_of(armed(PID, Some(BOOT))))] {
            let mut p = pending(marker.clone(), false);
            note_transport_up(&mut p, true, Some(&registrar));
            assert!(p.registrar.is_none(), "{marker:?}");
        }

        let mut p = pending(Some(short_run_marker.clone()), false);
        note_transport_up(&mut p, false, Some(&registrar));
        assert!(p.registrar.is_none(), "no endpoint, so no marker to write");

        // Held while the marker waits for the signal handlers, gone after.
        note_transport_up(&mut p, true, Some(&registrar));
        assert!(p.registrar.is_some());
        let written = OnceLock::new();
        arm_when_ready_into(&mut p, &written, &OnceLock::new(), no_hook);
        assert!(p.registrar.is_some(), "still waiting");
        p.signals_ready = true;
        arm_when_ready_into(&mut p, &written, &OnceLock::new(), no_hook);
        assert!(p.registrar.is_none());
    }

    /// A Windows marker gets the timer that clears it, registered with the
    /// monitor, when it is written; a unix marker does not.
    #[tokio::test]
    async fn only_a_short_run_marker_starts_the_timer() {
        let monitor = crate::node::background_task_monitor::BackgroundTaskMonitor::new();
        let registrar = monitor.late_registrar();
        let dir = tempfile::tempdir().unwrap();
        let path = marker_path(dir.path());
        let short_run_marker = Marker::ShortRuns {
            endpoint_down_secs: Vec::new(),
            prior_trip: None,
            short_runs: 1,
            started_at_secs: T0,
            nonce: 7,
            quic_panicked: false,
            quic_panic_at: None,
            backoff_step: 0,
        };
        let written = OnceLock::new();
        let arm_now = |marker: Marker, registrar: Option<LateRegistrar>| {
            arm_when_ready_into(
                &mut Pending {
                    marker: Some((path.clone(), marker)),
                    shadow: None,
                    quic_up: true,
                    signals_ready: true,
                    registrar,
                },
                &written,
                &OnceLock::new(),
                no_hook,
            )
        };

        arm_now(marker_of(armed(PID, Some(BOOT))), Some(registrar.clone()));
        assert!(registrar.registered().is_empty());

        arm_now(short_run_marker.clone(), Some(registrar.clone()));
        assert_eq!(registrar.registered(), vec![short_runs::TIMER_TASK]);

        // Without a registrar the run goes on, with no timer.
        std::fs::remove_file(&path).unwrap();
        arm_now(short_run_marker.clone(), None);
        assert_eq!(read_previous(&path), Previous::Marker(short_run_marker));
        assert_eq!(registrar.registered().len(), 1);
    }

    /// `gate` does nothing while the breaker is not enabled, which is every
    /// test and embedder.
    #[tokio::test]
    async fn gate_is_inert_unless_enabled() {
        let _globals = BACKOFF_GLOBALS.lock().await;
        let dir = tempfile::tempdir().unwrap();
        // Nothing in this test binary enables the breaker: it is process-wide
        // and would act on every node a test builds. A test that needs it
        // enabled must run in a child process (see the real-process tests in
        // `tests/graceful_shutdown_exit_code.rs`).
        assert!(!ENABLED.load(Ordering::Relaxed));
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(T0);
        assert_eq!(
            gate(Some(7u8), dir.path(), false, 30, now, BreakerRole::Peer).await,
            Gated {
                settings: Some(7),
                breaker: BreakerOutcome::NotConsulted,
            }
        );
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
        assert!(PENDING.lock().marker.is_none());
    }

    /// The outcome suppresses QUIC exactly when the breaker ran and its
    /// verdict keeps QUIC off; a breaker that did not run suppresses nothing.
    #[test]
    fn outcome_suppresses_only_a_verdict_that_keeps_quic_off() {
        assert_eq!(BreakerOutcome::NotConsulted.suppressing_verdict(), None);
        for (verdict, suppresses) in [
            (Verdict::Arm, false),
            (Verdict::RunUnarmed, false),
            (Verdict::Trip, true),
            (Verdict::Cool { tripped_at_secs: 1 }, true),
            (Verdict::Unreadable, true),
        ] {
            assert_eq!(
                BreakerOutcome::Decided(verdict).suppressing_verdict(),
                suppresses.then_some(verdict),
                "{verdict:?}"
            );
        }
    }

    /// `gate` decides on the marker as read after the wait: the call that
    /// re-reads it directly follows the wait. (`gate` itself is inert in unit
    /// tests; `the_marker_is_read_again_after_the_wait` tests the re-read.)
    #[test]
    fn gate_reads_the_marker_again_after_the_wait() {
        let flat: String =
            production_fn_body(include_str!("breaker.rs"), "pub(crate) async fn gate<T>(")
                .split_whitespace()
                .collect();
        let wait = flat.find("wait_for_exit(holder,").expect("the wait");
        let after = wait + flat[wait..].find(".await;").expect("awaited") + ".await;".len();
        assert!(
            flat[after..].starts_with("(previous,armed_pid_alive)=reread_after_wait(&path,"),
            "gate must re-read the marker right after waiting for its holder"
        );
    }

    /// Every peer, a gateway on Windows and a gateway on a unix platform with
    /// no boot identity decide by short runs (those that
    /// panicked in QUIC code), and only when the breaker is enabled. `gate` is
    /// inert in unit tests, so this pins the lines: the enabled check, then
    /// the dispatch on [`decides_by_short_runs`] for this platform and role,
    /// which runs the legacy shadow on unix before the short-run decision.
    /// The real-process tests are `tests/quic_breaker_windows.rs` and, for a
    /// unix peer, `tests/graceful_shutdown_exit_code.rs`.
    #[test]
    fn gate_sends_peers_and_windows_to_the_short_run_decision_after_the_enabled_check() {
        let flat: String =
            production_fn_body(include_str!("breaker.rs"), "pub(crate) async fn gate<T>(")
                .split_whitespace()
                .collect();
        let at = |needle: &str| {
            flat.find(needle)
                .unwrap_or_else(|| panic!("not found: {needle}"))
        };
        let enabled = at("if!ENABLED.load(Ordering::Relaxed){returnnot_consulted(settings);}");
        let decided = at(
            "letby_short_runs=decides_by_short_runs(cfg!(unix),role,platform::boot_id().is_some(),udp_fallback(),);",
        );
        // The endpoint-death record reads which rule decided
        // (`note_endpoint_down_exit`), so it is stored before either runs.
        let stored = at("DECIDED_BY_SHORT_RUNS.store(by_short_runs,Ordering::Relaxed);");
        let dispatch = at("ifby_short_runs{");
        assert!(decided < stored && stored < dispatch);
        let shadow = at(
            "letlegacy=ifcfg!(unix){legacy_shadow(settings.is_some(),data_dir,ignore_breaker,now)}else{None};",
        );
        let short_runs = at(
            "let(gated,counted)=short_runs::gate(settings,data_dir,ignore_breaker,now,role,udp_fallback(),);",
        );
        let returned = at("returngated;}");
        assert!(enabled < dispatch && dispatch < shadow && shadow < short_runs);
        assert!(short_runs < returned);
    }

    /// The body of the production function whose definition starts with
    /// `signature` in this file's source. Only the part before the test
    /// module is searched, so this test's own literals cannot match, and the
    /// signature must occur there exactly once (AGENTS.md, source-scrape
    /// pins). The test module is the one line that is exactly `mod tests {`,
    /// which holds with CRLF line endings too.
    fn production_fn_body<'a>(src: &'a str, signature: &str) -> &'a str {
        let module_starts: Vec<usize> = src
            .match_indices("mod tests {")
            .map(|(i, _)| i)
            .filter(|&i| i == 0 || src.as_bytes()[i - 1] == b'\n')
            .collect();
        assert_eq!(module_starts.len(), 1, "expected one test module");
        let production = &src[..module_starts[0]];
        assert_eq!(
            production.matches(signature).count(),
            1,
            "`{signature}` must occur exactly once before the test module"
        );
        let start = production.find(signature).expect("counted above");
        let open = start + production[start..].find('{').expect("a body");
        let mut depth = 0usize;
        for (i, byte) in production.bytes().enumerate().skip(open) {
            match byte {
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        return &production[open..=i];
                    }
                }
                _ => {}
            }
        }
        panic!("unbalanced braces in `{signature}`");
    }

    /// The wait loop: it returns as soon as the holder is gone, and gives up
    /// after `wait` with the holder still alive.
    #[tokio::test]
    async fn the_wait_loop_ends_when_the_holder_exits_or_at_the_deadline() {
        use std::cell::Cell;
        let polls = &Cell::new(0u32);
        let sleeps = &Cell::new(0u32);
        let sleep = |_: Duration| {
            sleeps.set(sleeps.get() + 1);
            std::future::ready(())
        };
        let wait = Duration::from_secs(4);
        let poll = Duration::from_secs(1);

        // Exits after the third check.
        let exits_after = |n: u32| {
            move |_: u32| {
                polls.set(polls.get() + 1);
                polls.get() < n
            }
        };
        assert!(wait_for_exit(PID, wait, poll, exits_after(3), sleep).await);
        assert_eq!((polls.get(), sleeps.get()), (3, 2), "stops at once");

        // Never exits: four sleeps of `poll` fill `wait`, then it gives up.
        polls.set(0);
        sleeps.set(0);
        assert!(!wait_for_exit(PID, wait, poll, |_| true, sleep).await);
        assert_eq!(sleeps.get(), 4);

        // The wait follows the configured drain.
        assert_eq!(holder_exit_wait(30), Duration::from_secs(40));
        assert_eq!(holder_exit_wait(100), Duration::from_secs(110));
        assert_eq!(holder_exit_wait(300), HOLDER_EXIT_MAX, "clamped");
        assert_eq!(holder_exit_wait(u64::MAX), HOLDER_EXIT_MAX, "a huge drain");

        // Already gone: no sleep.
        sleeps.set(0);
        assert!(wait_for_exit(PID, wait, poll, |_| false, sleep).await);
        assert_eq!(sleeps.get(), 0);
    }

    /// The transport arms only when its endpoint is up: the call passes the
    /// endpoint's availability, not a constant.
    #[test]
    fn the_transport_arms_only_with_the_endpoint_up() {
        let src: String = include_str!("../connection_handler.rs")
            .split_whitespace()
            .collect();
        assert!(
            src.contains("QuicAvailability::Up,task_registrar.as_ref(),"),
            "the transport must pass the node's registrar for the short-run timer"
        );
        assert_eq!(
            src.matches("arm_if_quic_up(").count(),
            1,
            "one call to arm_if_quic_up"
        );
        assert!(
            src.contains(
                "arm_if_quic_up(outbound.quic_availability().get()==super::quic::liveness::QuicAvailability::Up,"
            ),
            "the transport must pass whether its endpoint is up"
        );
    }

    /// The probes on the host: a stable boot identity, this process and a
    /// live child alive, a reaped child, pid 0 and `u32::MAX` not.
    #[cfg(unix)]
    #[test]
    fn platform_probes() {
        assert_eq!(platform::boot_id(), platform::boot_id());
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        assert!(platform::boot_id().is_some());
        assert!(platform::pid_alive(std::process::id()));
        let mut child = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .unwrap();
        assert!(platform::pid_alive(child.id()), "a live child is alive");
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(!platform::pid_alive(child.id()), "a reaped child is not");
        assert!(!platform::pid_alive(0), "pid 0 is never alive");
        assert!(!platform::pid_alive(u32::MAX));
    }

    /// #6151: every peer decides by short runs, and so does a gateway off
    /// unix or on a unix platform with no boot identity; a gateway with a boot
    /// identity decides by how its last run ended.
    #[test]
    fn who_decides_by_short_runs() {
        // (unix, role, boot identity known, udp fallback)
        assert!(decides_by_short_runs(true, BreakerRole::Peer, true, true));
        assert!(decides_by_short_runs(true, BreakerRole::Peer, false, true));
        assert!(!decides_by_short_runs(
            true,
            BreakerRole::Gateway,
            true,
            true
        ));
        assert!(decides_by_short_runs(false, BreakerRole::Peer, false, true));
        assert!(decides_by_short_runs(
            false,
            BreakerRole::Gateway,
            false,
            true
        ));
        // A unix gateway with no boot identity cannot tell a reboot from a
        // crash, so it counts short runs rather than never tripping.
        assert!(decides_by_short_runs(
            true,
            BreakerRole::Gateway,
            false,
            true
        ));
        // With no UDP every node counts short runs, a unix gateway with a
        // boot identity included: only that rule can back off (#6151).
        assert!(decides_by_short_runs(
            true,
            BreakerRole::Gateway,
            true,
            false
        ));
    }

    fn legacy_start(stopped: &[u32], ignore_breaker: bool) -> LegacyStart {
        LegacyStart {
            boot_id: Some(BOOT.to_string()),
            own_pid: OWN,
            stopped: stopped.to_vec(),
            now_secs: T0 + 60,
            ignore_breaker,
            nonce: 11,
        }
    }

    /// The marker the legacy shadow arms at `T0 + 60`.
    fn legacy_armed() -> Option<Marker> {
        Some(Marker::Armed {
            pid: OWN,
            boot_id: Some(BOOT.to_string()),
            nonce: 11,
            armed_at_secs: T0 + 60,
            endpoint_down_secs: Vec::new(),
            prior_trip: None,
        })
    }

    /// The legacy shadow decides as the exit-status rule did on a unix peer:
    /// a run that died on this boot without a clean exit or a note trips,
    /// for the peer cooldown; anything else arms its own marker.
    #[test]
    fn the_legacy_shadow_decides_as_the_exit_status_rule() {
        let no_main = || -> Previous { panic!("a shadow of its own is decided on alone") };
        let dead = |_| false;
        let start = legacy_start(&[], false);

        let (crash, marker) = legacy_shadow_decide(armed(PID, Some(BOOT)), no_main, &start, dead);
        assert_eq!(crash, Verdict::Trip);
        assert_eq!(
            marker,
            Some(Marker::Tripped {
                tripped_at_secs: T0 + 60,
                trips: 1
            })
        );

        let not_crashes = [
            (
                "noted",
                armed(PID, Some(BOOT)),
                legacy_start(&[PID], false),
                false,
            ),
            (
                "rebooted",
                armed(PID, Some("boot-b")),
                legacy_start(&[], false),
                false,
            ),
            (
                "alive",
                armed(PID, Some(BOOT)),
                legacy_start(&[], false),
                true,
            ),
            (
                "override",
                armed(PID, Some(BOOT)),
                legacy_start(&[], true),
                false,
            ),
        ];
        for (case, shadow, start, alive) in not_crashes {
            let decided = legacy_shadow_decide(shadow, no_main, &start, |_| alive);
            assert_eq!(decided, (Verdict::Arm, legacy_armed()), "{case}");
        }

        let tripped = Previous::Marker(Marker::Tripped {
            tripped_at_secs: T0,
            trips: 1,
        });
        assert_eq!(
            legacy_shadow_decide(tripped, no_main, &start, dead),
            (
                Verdict::Cool {
                    tripped_at_secs: T0
                },
                None
            ),
            "a trip holds for the peer cooldown and leaves its marker"
        );
        let expired = Previous::Marker(Marker::Tripped {
            tripped_at_secs: T0 + 60 - QUIC_BREAKER_COOLDOWN.as_secs(),
            trips: 1,
        });
        assert_eq!(
            legacy_shadow_decide(expired, no_main, &start, dead),
            (Verdict::Arm, legacy_armed())
        );
        assert_eq!(
            legacy_shadow_decide(Previous::Unreadable, no_main, &start, dead),
            (Verdict::Unreadable, None)
        );
    }

    /// With no marker of its own, the shadow decides on the one the earlier
    /// release left in the breaker's file, so a crash just before the upgrade
    /// is not lost; a short-run marker there is no evidence.
    #[test]
    fn the_legacy_shadow_starts_from_the_earlier_releases_marker() {
        let dead = |_| false;
        let start = legacy_start(&[], false);
        let decide =
            |main: Previous| legacy_shadow_decide(Previous::Nothing, || main, &start, dead);
        assert_eq!(decide(armed(PID, Some(BOOT))).0, Verdict::Trip);
        // A Tripped marker there may be the short-run rule's own trip.
        let tripped = Previous::Marker(Marker::Tripped {
            tripped_at_secs: T0,
            trips: 1,
        });
        let short_runs = Previous::Marker(Marker::ShortRuns {
            short_runs: 2,
            started_at_secs: T0,
            nonce: 3,
            quic_panicked: true,
            quic_panic_at: None,
            backoff_step: 0,
            endpoint_down_secs: Vec::new(),
            prior_trip: None,
        });
        for main in [
            tripped,
            short_runs,
            Previous::Nothing,
            Previous::Unreadable,
            Previous::Marker(Marker::Unknown),
        ] {
            assert_eq!(
                decide(main.clone()),
                (Verdict::Arm, legacy_armed()),
                "{main:?}"
            );
        }
    }

    /// `marker` as this release writes the legacy shadow's file on boot
    /// [`BOOT`].
    fn stamped(marker: Marker) -> LegacyShadowFile {
        LegacyShadowFile {
            version: crate::config::PCK_VERSION.to_string(),
            boot_id: Some(BOOT.to_string()),
            marker,
        }
    }

    /// The shadow's file is evidence only for the release that wrote it: a
    /// file from another release (a rollback and a roll forward) is none, so
    /// a stale Armed marker cannot produce a legacy trip. An Armed marker
    /// from another boot is none either; a Tripped one holds across a
    /// reboot, as the old rule's trip did.
    #[test]
    fn the_legacy_shadow_reads_only_its_own_releases_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = legacy_shadow_path(dir.path());
        let read = || read_legacy_shadow(&path, crate::config::PCK_VERSION, Some(BOOT));
        let armed_marker = marker_of(armed(PID, Some(BOOT)));
        let tripped = Marker::Tripped {
            tripped_at_secs: T0,
            trips: 1,
        };
        assert_eq!(read(), Previous::Nothing, "no file");

        write_legacy_shadow(&path, &stamped(armed_marker.clone())).unwrap();
        assert_eq!(read(), Previous::Marker(armed_marker.clone()));

        let mut other_release = stamped(armed_marker.clone());
        other_release.version = "0.0.1".into();
        write_legacy_shadow(&path, &other_release).unwrap();
        assert_eq!(read(), Previous::Nothing, "another release's file");

        let mut other_boot = stamped(armed_marker);
        other_boot.boot_id = Some("boot-b".into());
        write_legacy_shadow(&path, &other_boot).unwrap();
        assert_eq!(
            read(),
            Previous::Nothing,
            "an Armed marker from another boot"
        );
        other_boot.marker = tripped.clone();
        write_legacy_shadow(&path, &other_boot).unwrap();
        assert_eq!(
            read(),
            Previous::Marker(tripped),
            "a trip holds across a reboot"
        );

        // The breaker's own format is not the shadow's.
        write_marker(&path, &marker_of(armed(PID, Some(BOOT)))).unwrap();
        assert_eq!(read(), Previous::Nothing);
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        assert_eq!(read(), Previous::Unreadable);
    }

    /// On disk, at the first start with no shadow file: a Tripped marker in
    /// the breaker's own file (which may be the short-run rule's own trip)
    /// is not the shadow's evidence, so the shadow arms rather than cooling
    /// or tripping, and the breaker's file is left as it was; an Armed one
    /// there (0.2.159's) is, and trips the shadow.
    #[test]
    fn the_legacy_shadow_ignores_a_tripped_main_marker_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let main = marker_path(dir.path());
        let shadow = legacy_shadow_path(dir.path());
        let pending = parking_lot::Mutex::new(Pending {
            marker: None,
            shadow: None,
            quic_up: false,
            signals_ready: false,
            registrar: None,
        });
        let dead = |_| false;
        let tripped = Marker::Tripped {
            tripped_at_secs: T0 + 30,
            trips: 1,
        };
        write_marker(&main, &tripped).unwrap();
        let decided = legacy_shadow_at(
            true,
            dir.path(),
            None,
            false,
            legacy_start(&[], false),
            dead,
            &pending,
        );
        assert_eq!(decided, Some(Verdict::Arm));
        assert!(!shadow.exists(), "nothing written before QUIC is up");
        assert_eq!(
            pending.lock().shadow.take(),
            Some((shadow.clone(), stamped(legacy_armed().unwrap())))
        );
        assert_eq!(read_previous(&main), Previous::Marker(tripped));

        write_marker(&main, &marker_of(armed(PID, Some(BOOT)))).unwrap();
        let decided = legacy_shadow_at(
            true,
            dir.path(),
            None,
            false,
            legacy_start(&[], false),
            dead,
            &pending,
        );
        assert_eq!(decided, Some(Verdict::Trip));
        assert_eq!(
            read_legacy_shadow(&shadow, crate::config::PCK_VERSION, Some(BOOT)),
            Previous::Marker(Marker::Tripped {
                tripped_at_secs: T0 + 60,
                trips: 1
            })
        );
    }

    /// The shadow with QUIC off decides nothing and writes nothing, but still
    /// reconciles the stop note, as the old rule did on every start; with
    /// QUIC on, a crash it finds trips it, and a trip it cannot write is lost
    /// without stopping the start.
    #[test]
    fn the_legacy_shadow_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = legacy_shadow_path(dir.path());
        let note = dir.path().join("note");
        let pending = parking_lot::Mutex::new(Pending {
            marker: None,
            shadow: None,
            quic_up: false,
            signals_ready: false,
            registrar: None,
        });
        let start = || legacy_start(&[], false);
        let dead = |_| false;

        // QUIC off: the noted pid's marker is removed and the note consumed.
        write_legacy_shadow(&path, &stamped(marker_of(armed(PID, Some(BOOT))))).unwrap();
        std::fs::write(&note, PID.to_string()).unwrap();
        let decided = legacy_shadow_at(
            false,
            dir.path(),
            Some(&note),
            false,
            start(),
            dead,
            &pending,
        );
        assert_eq!(decided, None);
        assert!(!path.exists(), "the noted run's marker is removed");
        assert!(!note.exists(), "the note is consumed");
        assert!(pending.lock().shadow.is_none());

        // QUIC on, a crash: tripped and written at once.
        write_legacy_shadow(&path, &stamped(marker_of(armed(PID, Some(BOOT))))).unwrap();
        let decided = legacy_shadow_at(true, dir.path(), None, false, start(), dead, &pending);
        assert_eq!(decided, Some(Verdict::Trip));
        assert_eq!(
            read_legacy_shadow(&path, crate::config::PCK_VERSION, Some(BOOT)),
            Previous::Marker(Marker::Tripped {
                tripped_at_secs: T0 + 60,
                trips: 1
            })
        );
        assert!(pending.lock().shadow.is_none());

        // A trip that cannot be written: the start goes on, the file stays.
        write_legacy_shadow(&path, &stamped(marker_of(armed(PID, Some(BOOT))))).unwrap();
        let tmp = dir
            .path()
            .join(format!(".{LEGACY_SHADOW_FILE}.{}.tmp", std::process::id()));
        std::fs::create_dir(&tmp).unwrap();
        let decided = legacy_shadow_at(true, dir.path(), None, false, start(), dead, &pending);
        assert_eq!(decided, Some(Verdict::Trip));
        assert_eq!(
            read_legacy_shadow(&path, crate::config::PCK_VERSION, Some(BOOT)),
            Previous::Marker(marker_of(armed(PID, Some(BOOT))))
        );
        std::fs::remove_dir(&tmp).unwrap();

        // No evidence: an Armed marker for this run waits in `pending`.
        std::fs::remove_file(&path).unwrap();
        let decided = legacy_shadow_at(true, dir.path(), None, false, start(), dead, &pending);
        assert_eq!(decided, Some(Verdict::Arm));
        assert_eq!(
            pending.lock().shadow,
            Some((path.clone(), stamped(legacy_armed().unwrap())))
        );
        assert!(!path.exists(), "written only once QUIC is up");
    }

    /// Arming writes the legacy shadow's marker beside the breaker's, records
    /// it for the clean exit, and a shadow that cannot be written changes
    /// nothing else; the clean exit's removal takes only that nonce.
    #[test]
    fn arming_writes_the_legacy_shadow_beside_the_marker() {
        let dir = tempfile::tempdir().unwrap();
        let path = marker_path(dir.path());
        let shadow = legacy_shadow_path(dir.path());
        let marker = Marker::ShortRuns {
            short_runs: 0,
            started_at_secs: T0,
            nonce: 7,
            quic_panicked: false,
            quic_panic_at: None,
            backoff_step: 0,
            endpoint_down_secs: Vec::new(),
            prior_trip: None,
        };
        let pending = |shadow_path: &Path| Pending {
            marker: Some((path.clone(), marker.clone())),
            shadow: Some((shadow_path.to_path_buf(), stamped(legacy_armed().unwrap()))),
            quic_up: true,
            signals_ready: true,
            registrar: None,
        };
        let (armed, legacy) = (OnceLock::new(), OnceLock::new());
        arm_when_ready_into(&mut pending(&shadow), &armed, &legacy, no_hook);
        assert_eq!(armed.get(), Some(&(path.clone(), 7)));
        assert_eq!(legacy.get(), Some(&(shadow.clone(), 11)));
        assert_eq!(
            read_legacy_shadow(&shadow, crate::config::PCK_VERSION, Some(BOOT)),
            Previous::Marker(legacy_armed().unwrap())
        );
        assert!(
            !clear_legacy_if_owned(&shadow, 7).unwrap(),
            "the breaker's nonce"
        );
        assert!(clear_legacy_if_owned(&shadow, 11).unwrap());
        assert!(!shadow.exists());

        std::fs::remove_file(&path).unwrap();
        let blocker = dir.path().join("a-file");
        std::fs::write(&blocker, b"").unwrap();
        let (armed, legacy) = (OnceLock::new(), OnceLock::new());
        arm_when_ready_into(
            &mut pending(&legacy_shadow_path(&blocker)),
            &armed,
            &legacy,
            no_hook,
        );
        assert_eq!(legacy.get(), None, "the shadow's write failed");
        assert_eq!(armed.get(), Some(&(path.clone(), 7)), "the breaker armed");
        assert_eq!(read_previous(&path), Previous::Marker(marker));
    }

    /// The `quic_breaker_start` payload, pinned: the dashboard and the release
    /// watch read these names.
    #[test]
    fn the_start_report_payload() {
        let peer = StartReport {
            role: BreakerRole::Peer,
            rule: Rule::ShortRuns,
            verdict: Verdict::Trip,
            ignore_breaker: false,
            short_runs: Some(3),
            quic_panic_at: Some("quinn-proto-0.11.9/src/connection/mod.rs:42".into()),
            legacy_verdict: Some(Verdict::Arm),
        };
        assert_eq!(
            peer.to_json(),
            serde_json::json!({
                "role": "peer", "rule": "short_runs", "verdict": "trip",
                "quic_off": true, "overridden": false, "short_runs": 3,
                "quic_panic_at": "quinn-proto-0.11.9/src/connection/mod.rs:42",
                "legacy_verdict": "arm", "legacy_quic_off": false
            })
        );
        let gateway = StartReport {
            role: BreakerRole::Gateway,
            rule: Rule::ExitStatus,
            verdict: Verdict::Cool { tripped_at_secs: 1 },
            ignore_breaker: true,
            short_runs: None,
            quic_panic_at: None,
            legacy_verdict: None,
        };
        assert_eq!(
            gateway.to_json(),
            serde_json::json!({
                "role": "gateway", "rule": "exit_status", "verdict": "cool",
                "quic_off": true, "overridden": true
            })
        );
        for verdict in [
            Verdict::Arm,
            Verdict::RunUnarmed,
            Verdict::Trip,
            Verdict::Cool { tripped_at_secs: 0 },
            Verdict::Unreadable,
        ] {
            assert!(!verdict.label().is_empty());
        }
    }

    /// This release reads every marker 0.2.159 wrote, as written: a unix
    /// peer upgrading with an Armed or Tripped marker keeps that evidence
    /// (the legacy shadow takes the Armed one, the short-run decision holds
    /// the Tripped one for its cooldown). The other direction is
    /// `a_release_before_this_change_reads_the_new_markers`.
    #[test]
    fn this_release_reads_the_markers_of_0_2_159() {
        let cases = [
            (
                r#"{"state":"armed","pid":5,"boot_id":"b","nonce":7,"armed_at_secs":9}"#,
                Marker::Armed {
                    pid: 5,
                    boot_id: Some("b".into()),
                    nonce: 7,
                    armed_at_secs: 9,
                    endpoint_down_secs: Vec::new(),
                    prior_trip: None,
                },
            ),
            (
                r#"{"state":"tripped","tripped_at_secs":3}"#,
                Marker::Tripped {
                    tripped_at_secs: 3,
                    trips: 1,
                },
            ),
            (
                r#"{"state":"short_runs","short_runs":2,"started_at_secs":4,"nonce":7,"quic_panicked":true}"#,
                Marker::ShortRuns {
                    short_runs: 2,
                    started_at_secs: 4,
                    nonce: 7,
                    quic_panicked: true,
                    quic_panic_at: None,
                    backoff_step: 0,
                    endpoint_down_secs: Vec::new(),
                    prior_trip: None,
                },
            ),
        ];
        for (json, marker) in cases {
            assert_eq!(
                serde_json::from_str::<Marker>(json).unwrap(),
                marker,
                "{json}"
            );
        }
    }

    /// The backoff doubles from a minute to its 30-minute cap.
    #[test]
    fn the_backoff_doubles_up_to_its_cap() {
        let minutes = |step| backoff_delay_from(step, BACKOFF_FIRST).as_secs() / 60;
        assert_eq!(
            [0, 1, 2, 3, 4, 5, 6, 7, 40, u32::MAX].map(minutes),
            [1, 1, 2, 4, 8, 16, 30, 30, 30, 30]
        );
        assert_eq!(backoff_delay(3), backoff_delay_from(3, BACKOFF_FIRST));
    }

    /// The wait sleeps in steps until the delay is over, and stops at the
    /// first check after a stop is requested.
    #[tokio::test]
    async fn the_backoff_wait_ends_early_on_a_stop() {
        use std::sync::atomic::AtomicU32;
        let slept = AtomicU32::new(0);
        let sleep = |d: Duration| {
            slept.fetch_add(u32::try_from(d.as_millis()).unwrap(), Ordering::SeqCst);
            std::future::ready(())
        };
        let full = wait_unless(
            Duration::from_secs(2),
            Duration::from_millis(300),
            || false,
            sleep,
        )
        .await;
        assert_eq!(full, Duration::from_secs(2));
        assert_eq!(slept.load(Ordering::SeqCst), 2000);

        let polls = AtomicU32::new(0);
        let stopped = wait_unless(
            Duration::from_secs(60),
            Duration::from_millis(250),
            || polls.fetch_add(1, Ordering::SeqCst) >= 3,
            |_| std::future::ready(()),
        )
        .await;
        assert_eq!(
            stopped,
            Duration::from_millis(750),
            "three steps, then the stop"
        );
    }

    /// GitHub checks in a day of a peer's QUIC crash loop with no UDP to
    /// fall back to: one startup check per start (as today; the crash loop
    /// only shortens the jitter), each run panicking in QUIC 5 s after it opens
    /// and the supervisor restarting it 10 s later, with `delay` as the start
    /// delay of a backoff step.
    fn checks_per_day(delay: impl Fn(u32) -> Duration) -> u32 {
        const DAY: u64 = 24 * 60 * 60;
        let mut previous = Previous::Nothing;
        let mut now = T0;
        let mut checks = 0;
        while now < T0 + DAY {
            checks += 1;
            let decision = short_runs::decide(&previous, now, false, BreakerRole::Peer, false);
            let wait = match decision.verdict {
                Verdict::Backoff { step } => delay(step),
                Verdict::Arm => Duration::ZERO,
                other @ (Verdict::RunUnarmed
                | Verdict::Trip
                | Verdict::Cool { .. }
                | Verdict::Unreadable) => panic!("a peer with no UDP never {other:?}"),
            };
            let Some(Marker::ShortRuns {
                short_runs,
                started_at_secs,
                nonce,
                backoff_step,
                ..
            }) = short_runs::marker_after(decision, &previous, now, 7, BreakerRole::Peer)
            else {
                panic!("a start with QUIC on leaves a short-run marker");
            };
            previous = Previous::Marker(Marker::ShortRuns {
                short_runs,
                started_at_secs,
                nonce,
                quic_panicked: true,
                quic_panic_at: None,
                backoff_step,
                endpoint_down_secs: Vec::new(),
                prior_trip: None,
            });
            now += wait.as_secs() + 5 + 10;
        }
        checks
    }

    /// #6151: a tripped peer cannot hammer GitHub. Its startup check runs at
    /// once, but the backoff spaces its starts, so a day of crash loop asks
    /// GitHub about once per 30 minutes (49 at most from the cap, plus the
    /// first steps); without the backoff it would ask every 15 s. Each
    /// check also still passes the update claim (known-bad pin, lockout,
    /// release cooldown, download budget), which this does not model.
    #[test]
    fn a_tripped_peer_checks_for_updates_a_bounded_number_of_times_a_day() {
        let bounded = checks_per_day(backoff_delay_from_first);
        assert!(bounded <= 60, "{bounded} checks a day");
        let unbounded = checks_per_day(|_| Duration::ZERO);
        assert!(unbounded > 5_000, "without the backoff: {unbounded}");
    }

    fn backoff_delay_from_first(step: u32) -> Duration {
        backoff_delay_from(step, BACKOFF_FIRST)
    }

    /// The start report says how long a backoff waits.
    #[test]
    fn the_start_report_gives_the_backoff() {
        let report = StartReport {
            role: BreakerRole::Peer,
            rule: Rule::ShortRuns,
            verdict: Verdict::Backoff { step: 3 },
            ignore_breaker: false,
            short_runs: Some(5),
            quic_panic_at: None,
            legacy_verdict: None,
        };
        assert_eq!(
            report.to_json(),
            serde_json::json!({
                "role": "peer", "rule": "short_runs", "verdict": "backoff",
                "quic_off": false, "overridden": false, "short_runs": 5,
                "backoff_step": 3, "backoff_secs": 240
            })
        );
    }

    /// A marker with a backoff step is one 0.2.159 reads as a short-run
    /// marker, the step ignored; without a step the field is left out.
    #[test]
    fn the_backoff_step_field_is_compatible() {
        let marker = |backoff_step| Marker::ShortRuns {
            short_runs: 3,
            started_at_secs: 4,
            nonce: 7,
            quic_panicked: false,
            quic_panic_at: None,
            backoff_step,
            endpoint_down_secs: Vec::new(),
            prior_trip: None,
        };
        let json = serde_json::to_value(marker(0)).unwrap();
        assert!(json.get("backoff_step").is_none(), "{json}");
        let json = serde_json::to_value(marker(2)).unwrap();
        assert_eq!(json["backoff_step"], 2);
        assert_eq!(serde_json::from_value::<Marker>(json).unwrap(), marker(2));
    }

    /// The format is plain JSON with a `state` tag; pinned so a later release
    /// can read what this one wrote.
    #[test]
    fn marker_format() {
        assert_eq!(
            serde_json::to_value(marker_of(armed(PID, Some(BOOT)))).unwrap(),
            serde_json::json!({
                "state": "armed", "pid": PID, "boot_id": BOOT, "nonce": 7,
                "armed_at_secs": T0
            })
        );
        assert_eq!(
            serde_json::to_value(Marker::Tripped {
                trips: 1,
                tripped_at_secs: 3
            })
            .unwrap(),
            serde_json::json!({"state": "tripped", "tripped_at_secs": 3})
        );
    }

    /// Held by the tests that read or write the process-global marker slot and
    /// backoff state (`PENDING`, `BACKOFF`, `UPDATE_NOW`), which run on threads
    /// of one process under plain `cargo test`.
    static BACKOFF_GLOBALS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// A start the short-run decision backs off, end to end through `gate`
    /// (#6151): QUIC stays on, the start delay and the update-now flag are set,
    /// the update probation is held until the wait ends, and a stop during the
    /// wait returns without opening the transport and releases the probation.
    /// Peer and gateway alike. FALSIFY: delete the `UPDATE_NOW.store` in
    /// `note_backoff` (the update check keeps its full jitter), the
    /// `BACKOFF_PENDING` store (the probation commits during the wait), or
    /// the `BACKOFF_PENDING` clear after the wait.
    ///
    /// Process globals, so one test owns them all and resets them.
    #[tokio::test]
    async fn a_backed_off_start_sets_the_delay_the_update_flag_and_holds_the_probation() {
        let _globals = BACKOFF_GLOBALS.lock().await;
        for role in [BreakerRole::Peer, BreakerRole::Gateway] {
            let dir = tempfile::tempdir().unwrap();
            write_marker(
                &marker_path(dir.path()),
                &Marker::ShortRuns {
                    short_runs: 2,
                    started_at_secs: T0 - 10,
                    nonce: 7,
                    quic_panicked: true,
                    quic_panic_at: None,
                    backoff_step: 0,
                    endpoint_down_secs: Vec::new(),
                    prior_trip: None,
                },
            )
            .unwrap();
            let now = SystemTime::UNIX_EPOCH + Duration::from_secs(T0);
            assert!(!quic_crash_loop_update_now());
            let (gated, _) = short_runs::gate(Some(7u8), dir.path(), false, now, role, false);
            assert_eq!(gated.settings, Some(7), "{role:?}: QUIC stays on");
            assert!(
                quic_crash_loop_update_now(),
                "{role:?}: the update check does not wait for its full jitter"
            );
            assert_eq!(*BACKOFF.lock(), Some(backoff_delay(1)), "{role:?}");
            assert!(BACKOFF_PENDING.load(Ordering::SeqCst), "{role:?}");
            // The probation is held while the wait is pending.
            assert!(
                tokio::time::timeout(Duration::from_millis(50), wait_for_transport_open())
                    .await
                    .is_err(),
                "{role:?}: the probation window must not start during the wait"
            );
            // A stop during the wait ends it at once, without opening the
            // transport, and releases the probation.
            let stopping = AtomicBool::new(true);
            assert!(wait_out_crash_loop_backoff(&stopping).await, "{role:?}");
            assert!(!BACKOFF_PENDING.load(Ordering::SeqCst), "{role:?}");
            assert!(BACKOFF.lock().is_none(), "{role:?}");
            assert!(
                !wait_for_transport_open().await,
                "{role:?}: nothing to wait for"
            );
            UPDATE_NOW.store(false, Ordering::Relaxed);
            // `gate` left the marker pending; other tests read `PENDING`.
            PENDING.lock().marker = None;
        }
        // With nothing decided, the wait is a no-op that opens the transport.
        assert!(!wait_out_crash_loop_backoff(&AtomicBool::new(false)).await);
    }

    /// `wait_while_pending` returns at once when nothing is pending, and
    /// otherwise only after the wait ends, reporting that it waited.
    #[tokio::test(start_paused = true)]
    async fn the_transport_open_wait_returns_only_after_the_backoff_ends() {
        let pending = std::sync::Arc::new(AtomicBool::new(false));
        let over = std::sync::Arc::new(tokio::sync::Notify::new());
        assert!(!wait_while_pending(&pending, &over).await);
        pending.store(true, Ordering::SeqCst);
        let waiter = {
            let (pending, over) = (pending.clone(), over.clone());
            tokio::spawn(async move { wait_while_pending(&pending, &over).await })
        };
        tokio::time::sleep(Duration::from_secs(3600)).await;
        assert!(!waiter.is_finished(), "returned during the backoff");
        pending.store(false, Ordering::SeqCst);
        over.notify_waiters();
        assert!(waiter.await.unwrap(), "reports that it waited");
    }

    /// Reverting `gateway_quic_restart`'s first argument at the transport's
    /// open from `restarts_for_quic(is_gateway)` to `is_gateway` leaves a
    /// supervised peer with no UDP running without a transport when its QUIC
    /// endpoint dies (#6151). The helpers are tested apart; this pins the
    /// call site.
    #[test]
    fn the_transport_asks_restarts_for_quic_not_is_gateway() {
        let src = include_str!("../connection_handler.rs");
        let open = src
            .split("let restart = gateway_quic_restart(")
            .nth(1)
            .expect("the call site")
            .split(");")
            .next()
            .unwrap();
        let squeezed: String = open.split_whitespace().collect();
        assert!(
            squeezed.starts_with("super::quic::liveness::restarts_for_quic(is_gateway),"),
            "the first argument must be restarts_for_quic(is_gateway): {squeezed}"
        );
    }

    /// An unix node that decided by short runs (every peer, and a gateway with
    /// no boot identity or no UDP) writes its marker again when the run's
    /// short-run timer already cleared it, so an endpoint death is still
    /// counted; with no marker to write again it is not recorded. This is the
    /// same record the Windows test pins, on the unix path
    /// (`note_endpoint_down_exit_with`).
    #[cfg(unix)]
    #[test]
    fn a_unix_short_run_marker_cleared_by_the_timer_is_written_again_for_an_endpoint_death() {
        let dir = tempfile::tempdir().unwrap();
        let path = marker_path(dir.path());
        let as_armed = Marker::ShortRuns {
            short_runs: 0,
            started_at_secs: T0,
            nonce: 7,
            quic_panicked: false,
            quic_panic_at: None,
            backoff_step: 0,
            endpoint_down_secs: Vec::new(),
            prior_trip: None,
        };
        let armed = (path.clone(), 7u64);
        let pending = parking_lot::Mutex::new(Pending {
            marker: None,
            shadow: None,
            quic_up: false,
            signals_ready: false,
            registrar: None,
        });
        // The timer cleared the marker: written again from the armed copy.
        assert_eq!(
            note_endpoint_down_exit_with(Some(&armed), &pending, false, T0 + 900, Some(&as_armed))
                .unwrap(),
            EndpointDownNote::Recorded
        );
        assert!(matches!(
            read_previous(&path),
            Previous::Marker(Marker::ShortRuns { endpoint_down_secs, .. })
                if endpoint_down_secs == vec![T0 + 900]
        ));
        std::fs::remove_file(&path).unwrap();
        // No armed copy (the exit-status rule decided): nothing to write.
        assert_eq!(
            note_endpoint_down_exit_with(Some(&armed), &pending, false, T0 + 900, None).unwrap(),
            EndpointDownNote::NoMarker
        );
    }
}
