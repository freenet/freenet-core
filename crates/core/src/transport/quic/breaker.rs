//! The QUIC crash-loop breaker (#5940).
//!
//! If a run with QUIC enabled ends abnormally, QUIC stays off for
//! [`QUIC_BREAKER_COOLDOWN`] (24 h) on a peer and for
//! [`GATEWAY_QUIC_BREAKER_COOLDOWN`] (15 min, provisional) on a gateway
//! ([`BreakerRole`]). Some failures inside quinn's own code end the process
//! rather than a task, and the service manager restarts the node into the
//! same failure; the breaker bounds that to one restart with QUIC per
//! cooldown.
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
//! **This module doc describes unix.** Windows does not reliably tell a
//! process that it is being stopped: every design that tried to tell a crash
//! from a normal stop there (a logoff, a Fast Startup shutdown, a session end)
//! got one of them wrong. So Windows does not ask how a run ended. A panic
//! hook records a panic located in QUIC code (the failure this breaker is for
//! always panics before it aborts), and runs that ended within ten minutes
//! after recording one, three in a row, each starting within 24 h of the one
//! before, keep QUIC off for the role's cooldown, with the same state and
//! telemetry. [`short_runs`] describes it.
//!
//! **Windows is weaker than unix.** There, a crash not preceded by a panic in
//! QUIC code is not counted, a crash loop whose runs each last ten minutes or
//! more never trips, and neither does a fault that crashes once a day. QUIC
//! became on by default on Windows (shadow, now `prefer`) after this breaker
//! was confirmed on a real Windows machine (#6031), with those limits weighed.
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
pub(crate) use short_runs::QuicCode;

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
    quic_up: false,
    signals_ready: false,
    registrar: None,
});

/// Set by [`gate`] when `quic-ignore-crash-breaker` overrode the breaker for
/// this run. Then an endpoint death is not recorded and the node does not
/// exit for it: the next start would ignore the record, so a death at every
/// start would restart the node without bound.
static OVERRIDDEN: AtomicBool = AtomicBool::new(false);

/// Held across each read-and-change of this run's marker by the Windows
/// short-run clear ([`short_runs`]) and by [`note_endpoint_down_exit`], so
/// in either order the death record survives: a clear that comes first is
/// followed by the record written again, and a record written first makes
/// the clear leave the marker.
pub(super) static MARKER_UPDATE: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

/// The marker this process armed, as armed, for a Windows run that must
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
    /// Windows: a run with QUIC enabled started at this time, after
    /// `short_runs` short runs in a row. See [`short_runs`].
    ShortRuns {
        short_runs: u32,
        started_at_secs: u64,
        nonce: u64,
        /// The run recorded a panic in QUIC code (quinn, quinn-proto or this
        /// crate's QUIC transport) while it was still short. Only such a run
        /// counts. Absent in a marker from an earlier release.
        #[serde(default)]
        quic_panicked: bool,
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
    /// QUIC off until the earlier trip's cooldown ends.
    Cool { tripped_at_secs: u64 },
    /// The marker cannot be read: QUIC off for this run.
    Unreadable,
}

impl Verdict {
    /// Whether QUIC runs.
    pub(crate) fn quic_enabled(self) -> bool {
        matches!(self, Self::Arm | Self::RunUnarmed)
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
    let recreate = if cfg!(unix) {
        None
    } else {
        ARMED_MARKER.lock().clone()
    };
    let noted = note_endpoint_down_exit_with(
        ARMED.get(),
        &PENDING,
        OVERRIDDEN.load(Ordering::Relaxed),
        unix_secs(now),
        recreate.as_ref(),
    );
    let keeps_running = "this gateway does not restart itself for it and keeps running over \
                         UDP, with QUIC off until it is restarted";
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
        data_dir,
        note_path,
        shared,
        remove_if_exists,
    )
}

/// [`observe`] with the marker removal injected, so a test can make it fail.
fn observe_with(
    quic_requested: bool,
    data_dir: &Path,
    note_path: Option<&Path>,
    shared: bool,
    remove_marker: impl Fn(&Path) -> std::io::Result<()>,
) -> Option<Observed> {
    let marker = marker_path(data_dir);
    let stopped = note_path
        .and_then(|note| std::fs::read_to_string(note).ok())
        .map(|content| parse_note(&content))
        .unwrap_or_default();
    if let Some(note) = note_path.filter(|_| !stopped.is_empty()) {
        let noted = match read_previous(&marker) {
            Previous::Marker(Marker::Armed { pid, .. }) if stopped.contains(&pid) => Some(pid),
            Previous::Nothing | Previous::Unreadable | Previous::Marker(_) => None,
        };
        let removed = noted.is_some_and(|_| remove_marker(&marker).is_ok());
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
        previous: read_previous(&marker),
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
    if cfg!(not(unix)) {
        return short_runs::gate(settings, data_dir, ignore_breaker, now, role);
    }
    // The wrapper's note when it passed one; otherwise the note at its fixed
    // place, which may be meant for another node (`shared`).
    let (note_path, shared) = match std::env::var_os(STOP_NOTE_ENV_VAR) {
        Some(path) => (Some(PathBuf::from(path)), false),
        None => (
            crate::tracing::tracer::get_log_dir().map(|dir| dir.join(STOP_NOTE_FILE)),
            true,
        ),
    };
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
    Gated {
        settings: settings.filter(|_| decided.quic_enabled()),
        breaker: BreakerOutcome::Decided(decided),
    }
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
    quic_up: bool,
    signals_ready: bool,
    registrar: Option<LateRegistrar>,
}

/// Write the pending marker once the endpoint is up and the signal handlers
/// are installed. Called under the `PENDING` lock from both events, so
/// whichever comes second writes it, exactly once.
fn arm_when_ready(pending: &mut Pending) {
    arm_when_ready_into(pending, &ARMED, short_runs::record_quic_panics);
}

/// [`arm_when_ready`], recording what was written in `armed`, which is the
/// static [`ARMED`] outside tests, and installing the Windows panic hook with
/// `record_panics`, which tests replace: the real hook is process-global, so
/// only a test in a child process may install it.
fn arm_when_ready_into(
    pending: &mut Pending,
    armed: &OnceLock<(PathBuf, u64)>,
    record_panics: fn(PathBuf, u64),
) {
    if !(pending.quic_up && pending.signals_ready) {
        return;
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
                Verdict::Cool { .. } | Verdict::RunUnarmed | Verdict::Unreadable => {
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
    /// The other direction (old markers into the new type) is
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
                    prior_trip,
                },
                MarkerV0_2_159::ShortRuns {
                    short_runs: 2,
                    started_at_secs: 4,
                    nonce: 7,
                    quic_panicked: true,
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
            note.contains("letrecreate=ifcfg!(unix){None}else{ARMED_MARKER.lock().clone()};"),
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
        observe_with(false, dir.path(), Some(&note), false, failing);
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
        };
        let pending = |path: &Path| Pending {
            marker: Some((path.to_path_buf(), marker.clone())),
            quic_up: true,
            signals_ready: true,
            registrar: None,
        };

        let armed = OnceLock::new();
        arm_when_ready_into(&mut pending(&path), &armed, no_hook);
        assert_eq!(armed.get(), Some(&(path.clone(), 7)));
        assert!(clear_if_owned(&path, armed.get().unwrap().1).unwrap());

        let blocker = dir.path().join("a-file");
        std::fs::write(&blocker, b"").unwrap();
        let armed = OnceLock::new();
        arm_when_ready_into(&mut pending(&marker_path(&blocker)), &armed, no_hook);
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
        };
        let pending = |marker: Option<Marker>, signals_ready| Pending {
            marker: marker.map(|marker| (path.clone(), marker)),
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
        arm_when_ready_into(&mut p, &written, no_hook);
        assert!(p.registrar.is_some(), "still waiting");
        p.signals_ready = true;
        arm_when_ready_into(&mut p, &written, no_hook);
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
        };
        let written = OnceLock::new();
        let arm_now = |marker: Marker, registrar: Option<LateRegistrar>| {
            arm_when_ready_into(
                &mut Pending {
                    marker: Some((path.clone(), marker)),
                    quic_up: true,
                    signals_ready: true,
                    registrar,
                },
                &written,
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

    /// Windows is the one platform that decides by short runs (those that
    /// panicked in QUIC code), and it does so
    /// only when the breaker is enabled. `gate` is inert in unit tests and the
    /// branch cannot be taken on unix, so this pins the two lines; the
    /// real-process test is `tests/quic_breaker_windows.rs`.
    #[test]
    fn gate_sends_windows_to_the_short_run_decision_after_the_enabled_check() {
        let flat: String =
            production_fn_body(include_str!("breaker.rs"), "pub(crate) async fn gate<T>(")
                .split_whitespace()
                .collect();
        let enabled = flat
            .find("if!ENABLED.load(Ordering::Relaxed){returnnot_consulted(settings);}")
            .expect("the enabled check");
        let windows = flat
            .find(
                "ifcfg!(not(unix)){returnshort_runs::gate(settings,data_dir,ignore_breaker,now,role);}",
            )
            .expect("the Windows branch");
        assert!(enabled < windows);
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
}
