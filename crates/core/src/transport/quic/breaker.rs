//! The QUIC crash-loop breaker (#5940).
//!
//! If a run with QUIC enabled ends abnormally, QUIC stays off for
//! [`QUIC_BREAKER_COOLDOWN`]. Some failures inside quinn's own code end the
//! process rather than a task, and the service manager restarts the node into
//! the same failure; the breaker bounds that to one restart with QUIC per
//! cooldown.
//!
//! **Unix only.** On Windows the breaker is off: QUIC runs exactly as
//! configured and no marker is read or written. Every design tried for
//! Windows read a normal stop (a logoff, a Fast Startup shutdown, a session
//! end) as a crash or missed a real one, and none could be checked on a
//! Windows machine; see the tracking issue linked from #6029.
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
//!   read), so once the override is removed only a crash under it trips. Like
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
//!   breaker does not change exit codes, so it and the post-update crash-loop
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

/// How long QUIC stays off after a run with QUIC enabled ended abnormally.
pub(crate) const QUIC_BREAKER_COOLDOWN: Duration = Duration::from_secs(24 * 60 * 60);

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
});

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
    },
    /// The breaker tripped at this time (seconds since the Unix epoch).
    Tripped { tripped_at_secs: u64 },
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
    /// The breaker did not run: it is not enabled (tests and embedders), the
    /// platform is not unix, or `transport-quic` is off.
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
        Previous::Marker(Marker::Armed { pid, boot_id, .. })
            if !facts.ignore_breaker
                && same_boot(boot_id.as_deref(), facts.boot_id)
                && !facts.armed_pid_alive
                && !facts.stopped.contains(pid) =>
        {
            Verdict::Trip
        }
        Previous::Marker(Marker::Tripped { tripped_at_secs })
            if !facts.ignore_breaker && cooling(*tripped_at_secs, facts.now_secs) =>
        {
            Verdict::Cool {
                tripped_at_secs: *tripped_at_secs,
            }
        }
        Previous::Nothing
        | Previous::Marker(Marker::Armed { .. } | Marker::Tripped { .. } | Marker::Unknown) => {
            Verdict::Arm
        }
    }
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
/// [`HOLDER_EXIT_MARGIN`] for a previous node to exit. On Windows it returns
/// `settings` unchanged.
pub(crate) async fn gate<T>(
    settings: Option<T>,
    data_dir: &Path,
    ignore_breaker: bool,
    drain_secs: u64,
    now: SystemTime,
) -> Gated<T> {
    let not_consulted = |settings| Gated {
        settings,
        breaker: BreakerOutcome::NotConsulted,
    };
    if !ENABLED.load(Ordering::Relaxed) || cfg!(not(unix)) {
        return not_consulted(settings);
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

    let decided = verdict(&Facts {
        previous: &previous,
        boot_id: boot_id.as_deref(),
        armed_pid_alive,
        stopped: &stopped,
        now_secs,
        ignore_breaker,
    });

    let metrics = &crate::transport::TRANSPORT_METRICS;
    let until_utc = |tripped_at_secs: u64| {
        chrono::DateTime::<chrono::Utc>::from(cooldown_end(tripped_at_secs)).to_rfc3339()
    };
    match decided {
        Verdict::Arm => {
            PENDING.lock().marker = Some((
                path,
                Marker::Armed {
                    pid: own_pid,
                    boot_id,
                    nonce: crate::config::GlobalRng::random_u64(),
                    armed_at_secs: now_secs,
                },
            ));
        }
        Verdict::RunUnarmed => tracing::warn!(
            path = %path.display(),
            "The QUIC crash-breaker marker cannot be read; QUIC runs without the breaker \
             because quic-ignore-crash-breaker is set"
        ),
        Verdict::Trip => {
            // Off even if the write fails: the evidence is in hand now.
            let _best_effort = write_marker(
                &path,
                &Marker::Tripped {
                    tripped_at_secs: now_secs,
                },
            );
            metrics.record_quic_breaker_trip();
            metrics.set_quic_breaker_suppressed(true);
            tracing::warn!(
                until = %until_utc(now_secs),
                cooldown_hours = QUIC_BREAKER_COOLDOWN.as_secs() / 3600,
                "The previous run with QUIC enabled ended abnormally, so QUIC stays off \
                 until the cooldown ends. Set `quic-ignore-crash-breaker = true` to run \
                 QUIC anyway"
            );
        }
        Verdict::Cool { tripped_at_secs } => {
            metrics.set_quic_breaker_suppressed(true);
            tracing::warn!(
                until = %until_utc(tripped_at_secs),
                "QUIC stays off: a run with QUIC enabled ended abnormally and the \
                 cooldown has not ended. Set `quic-ignore-crash-breaker = true` to run \
                 QUIC anyway"
            );
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
/// marker.
pub(crate) fn arm_if_quic_up(quic_up: bool) {
    let mut pending = PENDING.lock();
    pending.quic_up = quic_up;
    arm_when_ready(&mut pending);
}

/// What arming waits for.
struct Pending {
    marker: Option<(PathBuf, Marker)>,
    quic_up: bool,
    signals_ready: bool,
}

/// Write the pending marker once the endpoint is up and the signal handlers
/// are installed. Called under the `PENDING` lock from both events, so
/// whichever comes second writes it, exactly once.
fn arm_when_ready(pending: &mut Pending) {
    if !(pending.quic_up && pending.signals_ready) {
        return;
    }
    if let Some(armed) = arm(pending.marker.take()) {
        // One node per process in production; a second keeps the first.
        let _first_wins = ARMED.set(armed);
    }
}

/// Write `pending`. Returns the marker's path and nonce when it was written.
fn arm(pending: Option<(PathBuf, Marker)>) -> Option<(PathBuf, u64)> {
    let (path, marker) = pending?;
    let Marker::Armed { nonce, .. } = marker else {
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

fn cooldown_end(tripped_at_secs: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH
        + Duration::from_secs(tripped_at_secs.saturating_add(QUIC_BREAKER_COOLDOWN.as_secs()))
}

/// Whether a trip at `tripped_at_secs` still holds at `now_secs`. A trip time
/// more than a cooldown in the future (a clock set back a long way) is not
/// honoured, so it cannot hold QUIC off indefinitely.
fn cooling(tripped_at_secs: u64, now_secs: u64) -> bool {
    let cooldown = QUIC_BREAKER_COOLDOWN.as_secs();
    now_secs < tripped_at_secs.saturating_add(cooldown)
        && tripped_at_secs <= now_secs.saturating_add(cooldown)
}

/// Remove the marker at `path` if it is Armed with `nonce`; otherwise leave
/// it. See the module docs for the window between the read and the removal.
fn clear_if_owned(path: &Path, nonce: u64) -> std::io::Result<()> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    match serde_json::from_slice::<Marker>(&bytes) {
        Ok(Marker::Armed { nonce: owner, .. }) if owner == nonce => remove_if_exists(path),
        _ => Ok(()),
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

    /// Unknown on other platforms (and unused on Windows, where the breaker
    /// is off).
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

    /// Unused: the breaker is off where this applies.
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

    fn armed(pid: u32, boot: Option<&str>) -> Previous {
        Previous::Marker(Marker::Armed {
            pid,
            boot_id: boot.map(str::to_string),
            nonce: 7,
            armed_at_secs: T0,
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

    #[test]
    fn a_trip_holds_for_the_cooldown_unless_overridden() {
        let c = QUIC_BREAKER_COOLDOWN.as_secs();
        let previous = Previous::Marker(Marker::Tripped {
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
        assert!(!cooling(u64::MAX, 0), "saturation ends the cooldown");
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
        let marker = Marker::Tripped { tripped_at_secs: 3 };
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
        clear_if_owned(&path, 7).unwrap(); // absent: fine
        write_marker(&path, &marker_of(armed(PID, Some(BOOT)))).unwrap();
        clear_if_owned(&path, 8).unwrap();
        assert!(path.exists(), "another nonce is left in place");
        clear_if_owned(&path, 7).unwrap();
        assert!(!path.exists());
        write_marker(&path, &Marker::Tripped { tripped_at_secs: 1 }).unwrap();
        clear_if_owned(&path, 7).unwrap();
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
            gate(Some(7u8), dir.path(), false, 30, now).await,
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
            serde_json::to_value(Marker::Tripped { tripped_at_secs: 3 }).unwrap(),
            serde_json::json!({"state": "tripped", "tripped_at_secs": 3})
        );
    }
}
