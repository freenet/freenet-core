//! Opt-in, local-only recorder of the router's raw inputs, for offline
//! evaluation of routing predictors against real traffic (#4485).
//!
//! # Why this exists
//!
//! Every question about which failure predictor is better — the legacy blend,
//! the residual correction, or a design not yet written — is ultimately a
//! question about real traffic, and a synthetic harness can only answer it for
//! the structure it was built to contain. The router's inputs are small: one
//! [`RouteEvent`](super::RouteEvent) per observed outcome. Recording that
//! stream with timestamps lets any candidate predictor be replayed
//! prequentially against the traffic a real node saw, without redeploying a
//! node per candidate.
//!
//! Alongside each outcome it records what every existing failure layer
//! forecast *before* ingesting it, so the recording also carries the in-process
//! prequential scores as a cross-check on any replay.
//!
//! # What it is and is not
//!
//! - It is a **prediction-accuracy** dataset: the outcome for the peer that was
//!   actually tried, not the whole candidate set a routing decision chose from,
//!   so it cannot answer "what if a different peer had been picked".
//! - It covers the router's `add_event` stream only. The separate CONNECT
//!   forward-acceptance estimator is out of scope.
//! - Events are tagged `originator` or `relay`: relay hops record outcomes under
//!   their own conventions (see `record_relay_route_event`, and
//!   `operations::route_attempt` for how failures of either origin are
//!   labelled since #5657), and replays may need
//!   to treat the two populations differently. `originator` means the node
//!   started the operation, which includes sub-operations it starts while
//!   serving someone else, not only its own clients' requests.
//! - A run may append after an earlier run's `truncated` line if that run
//!   stopped with room to spare (a large record did not fit); split on `start`.
//! - `t_ms` is absolute. The predictor's time feature is relative to process
//!   start, so a replay re-bases on the `start` line that opens each run.
//!
//! Peer attributes (connection age, protocol version, gateway status, transfer
//! volume, routing health) are recorded as periodic snapshots rather than per
//! event, because they live behind connection-manager locks that must not be
//! taken under the router's write lock. They join to events offline on `peer`
//! and time. Their distinctive value is population coverage — they include
//! connected peers that were never routed to and so never appear as events. The
//! routing counts are all-contract, originator-only aggregates; per-contract
//! density comes from the event stream itself.
//!
//! # Off by default, and local
//!
//! Nothing is recorded unless the operator sets `FREENET_ROUTING_DATASET` to a
//! file path. The file is written locally and never transmitted. It is not
//! anonymised: peers are keyed by a hash of their public key (linkable by anyone
//! who holds the key), and ring locations are derived from masked IP addresses.
//! Treat it like a node log.
//!
//! Give each node process its own path. The cap is enforced per process, and
//! two writers appending to one file can interleave lines longer than the write
//! buffer — so two gateways on one host must not share the variable's value.
//!
//! # Never blocks the caller
//!
//! Records are handed to a dedicated writer thread through a bounded channel
//! with `try_send`; the caller — which holds the router's write lock — never
//! waits on disk. A full channel drops the record and counts it, and the count
//! is written INTO the file (`"kind":"dropped"`), so an incomplete recording
//! says so itself rather than reading as a quiet period. The byte cap works the
//! same way: a record that would cross it is replaced by a final
//! `"kind":"truncated"` line and recording stops. A write error cannot be
//! recorded in the file it failed to write, so it is logged with its cause.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{
    Receiver, RecvTimeoutError, SyncSender, TryRecvError, TrySendError, sync_channel,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::ring::PeerKeyLocation;
use crate::transport::TransportPublicKey;

/// Environment variable naming the file to record into. Unset → no recording.
pub(crate) const DATASET_PATH_ENV: &str = "FREENET_ROUTING_DATASET";
/// Environment variable overriding [`DEFAULT_MAX_BYTES`], in plain bytes.
pub(crate) const DATASET_MAX_BYTES_ENV: &str = "FREENET_ROUTING_DATASET_MAX_BYTES";

/// Default size cap, so a forgotten setting cannot fill a disk.
pub(crate) const DEFAULT_MAX_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// Channel capacity. Route records are a few hundred bytes; a peer snapshot is
/// larger (one entry per connection) but arrives once a minute.
const CHANNEL_CAPACITY: usize = 8192;

/// How long the writer waits for a record before re-checking for drops.
const IDLE_WAKE: Duration = Duration::from_secs(5);

/// Records written between flushes when the channel never drains. Flushing
/// whenever the channel empties bounds loss on a crash to what was in flight,
/// even on a node busy enough that it is never idle.
const MAX_RECORDS_PER_FLUSH: usize = 1024;

/// Bytes at the top of the cap kept free for the truncation marker. Records
/// stop at `max_bytes - MARKER_RESERVE`; the marker may use the reserve but
/// never the byte beyond it, so the file never exceeds `max_bytes` however many
/// times the node restarts onto it.
const MARKER_RESERVE: u64 = 512;

/// The single clock every record is stamped with.
///
/// Host wall clock, deliberately not `TimeSource`: the predictor this data
/// exists to replay derives its own time feature from the host wall clock
/// (`routing_predictor::wall_clock_hours`), so a replay needs records on that
/// same clock. Route events and peer snapshots MUST share this function, or they
/// stop joining the moment either side's clock is changed.
pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

/// Stable 64-bit identity for a peer: FNV-1a over the full 32-byte public key.
///
/// Stable across builds and processes (unlike `DefaultHasher`), so events and
/// peer snapshots from different runs join on the same value.
pub(crate) fn peer_hash(peer: &PeerKeyLocation) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in peer.pub_key().as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

/// Which code path observed the outcome.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RouteSource {
    /// The node that started the operation: `Ring::routing_finished`, and
    /// since #5657 also the originator's per-attempt labels and hop-credited
    /// successes (`Ring::record_route_event_router_only`).
    Originator,
    /// A relay hop observing its downstream peer (`record_relay_route_event`).
    Relay,
}

/// What every failure layer forecast for an event, before ingesting it.
#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
pub(crate) struct FailureForecasts {
    /// The global isotonic distance curve.
    pub global: f64,
    /// The global curve with the per-peer EWMA adjustment.
    pub adjusted: f64,
    /// The legacy fixed-weight blend of `adjusted` with Renegade.
    pub blended: f64,
    /// The residual correction composed on the global curve.
    pub corrected: f64,
    /// Shrinkage applied to the correction, when one was formed.
    pub lambda: Option<f64>,
    /// Effective evidence behind the correction, when one was formed.
    pub n_eff: Option<f64>,
    /// The hierarchical empirical-Bayes estimator (#4485), once it has a curve.
    pub hierarchical: Option<f64>,
    /// FLOORED AT 1 ms: every `log_response_time_*` field is `ln(max(t, 0.001))`,
    /// while routing acts on the unfloored value and `time_to_response_start_s`
    /// is recorded raw. Floor `time_to_response_start_s` the same way before any
    /// log-scale scoring against these, or a 0 s outcome becomes `-inf`.
    ///
    /// `ln(seconds)` to response start the legacy stack would act on (including
    /// the residual correction when that flag is on), forecast for every event
    /// whether or not it turns out to be timed, so timing can be scored offline
    /// on the timed subset. `None` without a timing estimate, and recorded only
    /// while the hierarchical estimator is computed.
    pub log_response_time_legacy: Option<f64>,
    /// The same forecast from the hierarchical estimator: `ln E[T]`, the value
    /// routing would act on, not the log-scale location.
    pub log_response_time_hierarchical: Option<f64>,
    /// `ln(bytes/s)` of the transfer speed the legacy stack would act on.
    pub log_transfer_speed_legacy: Option<f64>,
    /// `ln(bytes/s)` of the hierarchical estimator's effective speed (so that
    /// `bytes / speed` is its expected transfer time).
    pub log_transfer_speed_hierarchical: Option<f64>,
}

/// One observed routing outcome.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct RouteRecord {
    pub t_ms: u64,
    pub source: RouteSource,
    pub peer: String,
    pub peer_location: Option<f64>,
    pub contract_location: f64,
    /// `None` when the peer's location is unknown. (The router itself falls
    /// back to 0.5 in that case; the recording does not pretend it knew.)
    pub distance: Option<f64>,
    pub op: Option<&'static str>,
    /// `success`, `success_untimed` or `failure`.
    pub outcome: &'static str,
    pub time_to_response_start_s: Option<f64>,
    pub payload_bytes: Option<usize>,
    pub payload_transfer_s: Option<f64>,
    /// Failure events the global curve had absorbed before this one — how
    /// warmed-up the forecasts below were.
    pub prior_failure_events: usize,
    /// `None` until the estimators can produce a forecast at all.
    pub forecasts: Option<FailureForecasts>,
}

/// Attributes of one connected peer at snapshot time.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct PeerAttributes {
    pub peer: String,
    pub location: Option<f64>,
    pub connected_s: f64,
    /// `None` when the configured gateway list was unavailable.
    pub is_configured_gateway: Option<bool>,
    pub version: Option<String>,
    pub routing_successes: Option<u64>,
    pub routing_failures: Option<u64>,
    /// Transport counters, which are LRU-bounded per address and so can reset
    /// for a peer that has been evicted from tracking.
    pub bytes_sent: Option<u64>,
    pub bytes_received: Option<u64>,
}

/// The inputs a peer snapshot is assembled from, gathered by the caller so
/// that assembly itself takes no locks and can be tested directly.
pub(crate) struct PeerSnapshotInputs<'a> {
    /// Each connected peer with how long it has been connected, in seconds.
    pub connections: &'a [(PeerKeyLocation, f64)],
    pub gateways: Option<&'a [TransportPublicKey]>,
    pub versions: &'a HashMap<SocketAddr, (u8, u8, u16)>,
    /// `(successes, failures)` per address.
    pub health: &'a HashMap<SocketAddr, (u64, u64)>,
    /// `(sent, received)` per address.
    pub transfer: &'a HashMap<SocketAddr, (u64, u64)>,
}

/// Assemble peer attributes. Every per-address join is a keyed lookup, so no
/// field can be attributed to the wrong peer by an ordering assumption.
pub(crate) fn peer_attributes(inputs: &PeerSnapshotInputs<'_>) -> Vec<PeerAttributes> {
    inputs
        .connections
        .iter()
        .map(|(peer, connected_s)| {
            let addr = peer.socket_addr();
            let lookup =
                |map: &HashMap<SocketAddr, (u64, u64)>| addr.and_then(|a| map.get(&a).copied());
            let health = lookup(inputs.health);
            let transfer = lookup(inputs.transfer);
            PeerAttributes {
                peer: peer_hash(peer),
                location: peer.location().map(|location| location.as_f64()),
                connected_s: *connected_s,
                is_configured_gateway: inputs
                    .gateways
                    .map(|gateways| gateways.contains(peer.pub_key())),
                version: addr
                    .and_then(|a| inputs.versions.get(&a))
                    .map(|(major, minor, patch)| format!("{major}.{minor}.{patch}")),
                routing_successes: health.map(|(successes, _)| successes),
                routing_failures: health.map(|(_, failures)| failures),
                bytes_sent: transfer.map(|(sent, _)| sent),
                bytes_received: transfer.map(|(_, received)| received),
            }
        })
        .collect()
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Line<'a> {
    Start {
        t_ms: u64,
        version: &'static str,
    },
    Route(&'a RouteRecord),
    Peers {
        t_ms: u64,
        peers: &'a [PeerAttributes],
    },
    Dropped {
        t_ms: u64,
        total: u64,
    },
    Truncated {
        t_ms: u64,
        max_bytes: u64,
        dropped_total: u64,
    },
}

enum Record {
    // Boxed: the forecasts make a route record several times the size of a
    // peers record, and every slot of the bounded channel is sized to the
    // largest variant.
    Route(Box<RouteRecord>),
    Peers {
        t_ms: u64,
        peers: Vec<PeerAttributes>,
    },
}

/// Handle to a running recorder. Cheap to call from hot paths.
pub(crate) struct RoutingDataset {
    tx: SyncSender<Record>,
    dropped: Arc<AtomicU64>,
    /// Set by the writer when it exits, so callers stop building records that
    /// could only ever be dropped.
    stopped: Arc<AtomicBool>,
}

impl std::fmt::Debug for RoutingDataset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoutingDataset")
            .field("dropped", &self.dropped.load(Ordering::Relaxed))
            .field("stopped", &self.stopped.load(Ordering::Relaxed))
            .finish()
    }
}

impl RoutingDataset {
    /// Open `path` for appending and start the writer thread.
    pub(crate) fn open(path: &Path, max_bytes: u64) -> std::io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let (dataset, rx) = Self::unstarted();
        let dropped = dataset.dropped.clone();
        let stopped = dataset.stopped.clone();
        // A plain OS thread, not a runtime task: its whole job is blocking file
        // I/O, and it lives exactly as long as the process. It exits when every
        // sender is gone, the cap is reached, or a write fails.
        std::thread::Builder::new()
            .name("routing-dataset".into())
            .spawn(move || write_loop(rx, file, max_bytes, &dropped, &stopped))?;
        Ok(dataset)
    }

    /// A handle that has already stopped, as after its byte cap or a write
    /// error, for tests of callers that must notice.
    #[cfg(test)]
    pub(crate) fn stopped_for_test() -> Self {
        let (dataset, _rx) = Self::unstarted();
        dataset.stopped.store(true, Ordering::Relaxed);
        dataset
    }

    /// A handle whose writer has not been started; the caller owns the receiver.
    fn unstarted() -> (Self, Receiver<Record>) {
        let (tx, rx) = sync_channel(CHANNEL_CAPACITY);
        let dataset = Self {
            tx,
            dropped: Arc::new(AtomicU64::new(0)),
            stopped: Arc::new(AtomicBool::new(false)),
        };
        (dataset, rx)
    }

    /// Whether records are still being written. Callers check this before
    /// building a record, so a stopped recorder costs nothing further.
    pub(crate) fn is_recording(&self) -> bool {
        !self.stopped.load(Ordering::Relaxed)
    }

    pub(crate) fn record_route(&self, record: RouteRecord) {
        self.send(Record::Route(Box::new(record)));
    }

    pub(crate) fn record_peers(&self, t_ms: u64, peers: Vec<PeerAttributes>) {
        self.send(Record::Peers { t_ms, peers });
    }

    fn send(&self, record: Record) {
        match self.tx.try_send(record) {
            Ok(()) => {}
            // Full: drop and count — never wait under the router lock.
            // Disconnected: the writer has stopped; counting keeps the total
            // honest for anything still in flight when it did.
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

enum WriteError {
    /// The line would cross the byte cap; nothing was written.
    Cap,
    Io(std::io::Error),
}

struct Writer {
    out: BufWriter<File>,
    written: u64,
    max_bytes: u64,
    reported_dropped: u64,
}

impl Writer {
    /// Write one ordinary line, unless it would enter the marker reserve.
    fn line(&mut self, line: &Line<'_>) -> Result<(), WriteError> {
        self.line_within(line, self.max_bytes.saturating_sub(MARKER_RESERVE))
    }

    fn line_within(&mut self, line: &Line<'_>, limit: u64) -> Result<(), WriteError> {
        let Ok(mut bytes) = serde_json::to_vec(line) else {
            // Cannot happen for these types; skipping beats stopping.
            return Ok(());
        };
        bytes.push(b'\n');
        let len = bytes.len() as u64;
        if self.written.saturating_add(len) > limit {
            return Err(WriteError::Cap);
        }
        self.out.write_all(&bytes).map_err(WriteError::Io)?;
        self.written += len;
        Ok(())
    }

    fn report_drops(&mut self, dropped: &AtomicU64) -> Result<(), WriteError> {
        let total = dropped.load(Ordering::Relaxed);
        if total == self.reported_dropped {
            return Ok(());
        }
        self.line(&Line::Dropped {
            t_ms: now_ms(),
            total,
        })?;
        self.reported_dropped = total;
        Ok(())
    }

    /// Write a record. One the cap turns away is counted as dropped, like any
    /// other record that never reaches the file.
    fn record(&mut self, record: &Record, dropped: &AtomicU64) -> Result<(), WriteError> {
        let result = match record {
            Record::Route(route) => self.line(&Line::Route(route)),
            Record::Peers { t_ms, peers } => self.line(&Line::Peers { t_ms: *t_ms, peers }),
        };
        if let Err(WriteError::Cap) = result {
            dropped.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    fn flush(&mut self) -> Result<(), WriteError> {
        self.out.flush().map_err(WriteError::Io)
    }
}

fn write_loop(
    rx: Receiver<Record>,
    file: File,
    max_bytes: u64,
    dropped: &AtomicU64,
    stopped: &AtomicBool,
) {
    let outcome = run_writer(&rx, file, max_bytes, dropped, stopped);
    // Mark stopped before the receiver drops, so callers stop building records.
    stopped.store(true, Ordering::Relaxed);
    // Anything that slipped into the queue after the final drain is counted
    // here, so the in-memory total stays exact even where the file's cannot.
    dropped.fetch_add(rx.try_iter().count() as u64, Ordering::Relaxed);
    match outcome {
        Ok(()) => {}
        Err(WriteError::Cap) => tracing::info!(
            max_bytes,
            "routing dataset: size cap reached; recording stopped"
        ),
        Err(WriteError::Io(error)) => tracing::warn!(
            %error,
            dropped = dropped.load(Ordering::Relaxed),
            "routing dataset: write failed; recording stopped"
        ),
    }
}

fn run_writer(
    rx: &Receiver<Record>,
    file: File,
    max_bytes: u64,
    dropped: &AtomicU64,
    stopped: &AtomicBool,
) -> Result<(), WriteError> {
    // Appending to an existing recording counts its bytes against the cap. A
    // file without room for a start line gets nothing at all, so a restart loop
    // cannot grow it past the cap.
    let written = file.metadata().map(|meta| meta.len()).unwrap_or(0);
    let mut writer = Writer {
        out: BufWriter::new(file),
        written,
        max_bytes,
        reported_dropped: 0,
    };
    writer.line(&Line::Start {
        t_ms: now_ms(),
        version: env!("CARGO_PKG_VERSION"),
    })?;
    writer.flush()?;

    let result = drain(rx, &mut writer, dropped);
    if let Err(WriteError::Cap) = result {
        // Stop producers FIRST, then count what is still queued, so the marker's
        // total covers every record that will never be written. The one residual
        // window is a sender that checked `is_recording` just before this store
        // and enqueues just after the drain: its record is missing from the
        // marker, though `write_loop` still adds it to the in-memory total.
        stopped.store(true, Ordering::Relaxed);
        let queued = rx.try_iter().count() as u64;
        dropped.fetch_add(queued, Ordering::Relaxed);
        // The marker may use the reserve. If even that is gone, an earlier run
        // already marked the file and this one adds nothing.
        let marker = Line::Truncated {
            t_ms: now_ms(),
            max_bytes,
            dropped_total: dropped.load(Ordering::Relaxed),
        };
        match writer.line_within(&marker, max_bytes) {
            Ok(()) | Err(WriteError::Cap) => {}
            Err(error) => return Err(error),
        }
    } else {
        // Whatever ended the loop, a drop count not yet written is written now.
        if let Err(WriteError::Io(error)) = writer.report_drops(dropped) {
            return Err(WriteError::Io(error));
        }
    }
    writer.flush()?;
    result
}

fn drain(
    rx: &Receiver<Record>,
    writer: &mut Writer,
    dropped: &AtomicU64,
) -> Result<(), WriteError> {
    loop {
        let first = match rx.recv_timeout(IDLE_WAKE) {
            Ok(record) => Some(record),
            Err(RecvTimeoutError::Timeout) => None,
            Err(RecvTimeoutError::Disconnected) => return Ok(()),
        };
        writer.report_drops(dropped)?;
        if let Some(record) = first {
            writer.record(&record, dropped)?;
            // Take whatever else is already queued, then flush: a busy node is
            // flushed every batch, not only when it happens to go idle.
            for _ in 1..MAX_RECORDS_PER_FLUSH {
                match rx.try_recv() {
                    Ok(record) => writer.record(&record, dropped)?,
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        writer.flush()?;
                        return Ok(());
                    }
                }
            }
        }
        writer.flush()?;
    }
}

/// Parse the max-bytes override. Anything unparsable falls back to the
/// default with a warning, so `4G` is not silently read as something else.
fn parse_max_bytes(value: Option<&str>) -> u64 {
    let Some(value) = value else {
        return DEFAULT_MAX_BYTES;
    };
    match value.trim().parse::<u64>() {
        Ok(bytes) => bytes,
        Err(_) => {
            tracing::warn!(
                value,
                default = DEFAULT_MAX_BYTES,
                "routing dataset: {DATASET_MAX_BYTES_ENV} must be a plain byte count; using the default"
            );
            DEFAULT_MAX_BYTES
        }
    }
}

/// The process-wide recorder, if the operator enabled one.
///
/// Resolved once. A path that cannot be opened disables recording with a
/// warning rather than failing the node: this is a diagnostic, not a
/// dependency.
///
/// Always `None` in unit tests: a process-global resolved once is decided by
/// whichever test touches it first, and a developer's exported environment
/// variable would otherwise make every router test write to their file. Tests
/// supply a recorder explicitly instead.
#[cfg(not(test))]
pub(crate) fn global() -> Option<&'static RoutingDataset> {
    static DATASET: std::sync::OnceLock<Option<RoutingDataset>> = std::sync::OnceLock::new();
    DATASET
        .get_or_init(|| {
            let path = std::path::PathBuf::from(std::env::var_os(DATASET_PATH_ENV)?);
            let max_bytes = parse_max_bytes(std::env::var(DATASET_MAX_BYTES_ENV).ok().as_deref());
            match RoutingDataset::open(&path, max_bytes) {
                Ok(dataset) => {
                    tracing::info!(
                        path = %path.display(),
                        max_bytes,
                        "routing dataset: recording route events"
                    );
                    Some(dataset)
                }
                Err(error) => {
                    tracing::warn!(
                        path = %path.display(),
                        %error,
                        "routing dataset: cannot open; recording disabled"
                    );
                    None
                }
            }
        })
        .as_ref()
}

#[cfg(test)]
pub(crate) fn global() -> Option<&'static RoutingDataset> {
    None
}

/// Whether `FREENET_ROUTING_DATASET` is set, so a missing recorder means it
/// failed to open rather than was never asked for.
#[cfg(not(test))]
pub(crate) fn configured() -> bool {
    std::env::var_os(DATASET_PATH_ENV).is_some()
}

#[cfg(test)]
pub(crate) fn configured() -> bool {
    false
}

/// Read a recording once the asynchronous writer has produced what `want`
/// describes — polling the artifact rather than sleeping a fixed amount.
#[cfg(test)]
pub(crate) fn lines_eventually(
    path: &Path,
    want: impl Fn(&[serde_json::Value]) -> bool,
) -> Vec<serde_json::Value> {
    for _ in 0..200 {
        let lines = read_lines(path);
        if want(&lines) {
            return lines;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!(
        "expected lines never appeared in {}: {:?}",
        path.display(),
        std::fs::read_to_string(path)
    );
}

#[cfg(test)]
fn read_lines(path: &Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::TransportKeypair;

    fn route(peer: &str) -> RouteRecord {
        RouteRecord {
            t_ms: 1,
            source: RouteSource::Relay,
            peer: peer.to_string(),
            peer_location: Some(0.25),
            contract_location: 0.5,
            distance: Some(0.25),
            op: Some("GET"),
            outcome: "failure",
            time_to_response_start_s: None,
            payload_bytes: None,
            payload_transfer_s: None,
            prior_failure_events: 3,
            forecasts: Some(FailureForecasts {
                global: 0.1,
                adjusted: 0.2,
                blended: 0.15,
                corrected: 0.12,
                lambda: Some(0.5),
                n_eff: Some(4.0),
                hierarchical: Some(0.11),
                log_response_time_legacy: Some(-1.5),
                log_response_time_hierarchical: None,
                log_transfer_speed_legacy: Some(9.0),
                log_transfer_speed_hierarchical: None,
            }),
        }
    }

    fn peer_at(addr: &str) -> PeerKeyLocation {
        PeerKeyLocation::new(
            TransportKeypair::new().public().clone(),
            addr.parse().unwrap(),
        )
    }

    /// Run the writer synchronously over records already queued.
    fn write_queued(path: &Path, max_bytes: u64, records: Vec<RouteRecord>) -> RoutingDataset {
        let (dataset, rx) = RoutingDataset::unstarted();
        for record in records {
            dataset.record_route(record);
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        let (dropped, stopped) = (dataset.dropped.clone(), dataset.stopped.clone());
        // Disconnect the channel so the writer returns once drained, while the
        // counters stay observable through the clones.
        let RoutingDataset { tx, .. } = dataset;
        drop(tx);
        write_loop(rx, file, max_bytes, &dropped, &stopped);
        let (tx, _rx) = sync_channel(0);
        RoutingDataset {
            tx,
            dropped,
            stopped,
        }
    }

    #[test]
    fn records_round_trip_as_tagged_json_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("routing.jsonl");
        let dataset = RoutingDataset::open(&path, DEFAULT_MAX_BYTES).unwrap();
        assert!(dataset.is_recording());

        dataset.record_route(route("00000000000000aa"));
        dataset.record_peers(
            7,
            vec![PeerAttributes {
                peer: "00000000000000aa".into(),
                location: Some(0.25),
                connected_s: 12.5,
                is_configured_gateway: Some(true),
                version: Some("0.2.135".into()),
                routing_successes: Some(4),
                routing_failures: Some(1),
                bytes_sent: Some(10),
                bytes_received: Some(20),
            }],
        );
        drop(dataset);

        let lines = lines_eventually(&path, |lines| lines.len() >= 3);
        assert_eq!(lines[0]["kind"], "start");
        assert_eq!(lines[1]["kind"], "route");
        assert_eq!(lines[1]["source"], "relay");
        assert_eq!(lines[1]["peer"], "00000000000000aa");
        assert_eq!(lines[1]["outcome"], "failure");
        assert_eq!(lines[1]["forecasts"]["corrected"], 0.12);
        assert_eq!(lines[1]["forecasts"]["hierarchical"], 0.11);
        assert_eq!(lines[1]["forecasts"]["log_response_time_legacy"], -1.5);
        assert_eq!(lines[1]["forecasts"]["log_transfer_speed_legacy"], 9.0);
        assert!(lines[1]["forecasts"]["log_transfer_speed_hierarchical"].is_null());
        assert!(
            lines[1]["forecasts"]["log_response_time_hierarchical"].is_null(),
            "an absent forecast must be recorded as null, not omitted or zero"
        );
        assert_eq!(lines[2]["kind"], "peers");
        assert_eq!(lines[2]["t_ms"], 7);
        assert_eq!(lines[2]["peers"][0]["is_configured_gateway"], true);
    }

    #[test]
    fn a_record_that_would_cross_the_cap_is_replaced_by_the_truncation_marker() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("routing.jsonl");
        // Room for the start line and one and a half route records.
        let one_route = serde_json::to_vec(&Line::Route(&route("00000000000000aa")))
            .unwrap()
            .len() as u64
            + 1;
        let start_len = 80;
        let cap = MARKER_RESERVE + start_len + one_route + one_route / 2;
        let dataset = write_queued(
            &path,
            cap,
            vec![
                route("00000000000000aa"),
                route("00000000000000bb"),
                route("00000000000000cc"),
            ],
        );

        let lines = read_lines(&path);
        let kinds: Vec<&str> = lines
            .iter()
            .map(|line| line["kind"].as_str().unwrap())
            .collect();
        assert_eq!(kinds, ["start", "route", "truncated"], "{lines:?}");
        assert_eq!(lines[1]["peer"], "00000000000000aa");
        assert_eq!(
            lines[2]["dropped_total"], 2,
            "the marker counts the record the cap turned away and the one still queued"
        );
        assert_eq!(dataset.dropped(), 2);
        let content_before_marker: u64 = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .take(2)
            .map(|line| line.len() as u64 + 1)
            .sum();
        assert!(
            content_before_marker <= cap - MARKER_RESERVE,
            "records must stay out of the marker reserve"
        );
        assert!(std::fs::metadata(&path).unwrap().len() <= cap);
        assert!(
            !dataset.is_recording(),
            "a capped writer reports itself stopped"
        );
    }

    #[test]
    fn reopening_a_file_at_the_cap_does_not_grow_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("routing.jsonl");
        // Room for a start line but not a route record: the first run writes a
        // start line and a truncation marker.
        let cap = MARKER_RESERVE + 100;
        write_queued(&path, cap, vec![route("00000000000000aa"); 4]);
        let kinds: Vec<String> = read_lines(&path)
            .iter()
            .map(|line| line["kind"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(kinds, ["start", "truncated"]);
        let size = std::fs::metadata(&path).unwrap().len();

        // A restart loop onto the same file must not grow it at all.
        for _ in 0..5 {
            let dataset = write_queued(&path, cap, vec![route("00000000000000bb")]);
            assert!(!dataset.is_recording());
            assert_eq!(std::fs::metadata(&path).unwrap().len(), size);
        }
        assert!(size <= cap);
    }

    #[test]
    fn a_full_channel_counts_drops_and_the_file_carries_the_count() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("routing.jsonl");
        const OVERFLOW: usize = 5;
        // Writer not started while sending, so the channel genuinely fills: the
        // saturated state, reached deterministically rather than by racing a
        // disk. Every send must return even though nothing is draining.
        let dataset = write_queued(
            &path,
            DEFAULT_MAX_BYTES,
            vec![route("00000000000000aa"); CHANNEL_CAPACITY + OVERFLOW],
        );
        assert_eq!(dataset.dropped(), OVERFLOW as u64);

        let lines = read_lines(&path);
        let routes = lines.iter().filter(|line| line["kind"] == "route").count();
        assert_eq!(routes, CHANNEL_CAPACITY, "every queued record is written");
        let reported: Vec<u64> = lines
            .iter()
            .filter(|line| line["kind"] == "dropped")
            .filter_map(|line| line["total"].as_u64())
            .collect();
        assert_eq!(
            reported,
            vec![OVERFLOW as u64],
            "the recording must say it is incomplete, exactly once"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_write_error_stops_the_writer_and_later_records_count_as_dropped() {
        // /dev/full accepts the open and fails every write with ENOSPC.
        let dataset = RoutingDataset::open(Path::new("/dev/full"), DEFAULT_MAX_BYTES).unwrap();
        for _ in 0..200 {
            if !dataset.is_recording() {
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        assert!(
            !dataset.is_recording(),
            "a failed write must stop the writer"
        );
        for _ in 0..200 {
            dataset.record_route(route("00000000000000aa"));
            if dataset.dropped() > 0 {
                return;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        panic!("records sent to a stopped writer must be counted as dropped");
    }

    #[test]
    fn opening_an_unwritable_path_fails_instead_of_recording_nowhere() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("no-such-dir").join("routing.jsonl");
        assert!(RoutingDataset::open(&missing, DEFAULT_MAX_BYTES).is_err());
    }

    #[test]
    fn max_bytes_parsing_accepts_plain_counts_and_falls_back_otherwise() {
        assert_eq!(parse_max_bytes(None), DEFAULT_MAX_BYTES);
        assert_eq!(parse_max_bytes(Some(" 1048576 ")), 1_048_576);
        assert_eq!(parse_max_bytes(Some("0")), 0);
        assert_eq!(parse_max_bytes(Some("1G")), DEFAULT_MAX_BYTES);
        assert_eq!(parse_max_bytes(Some("")), DEFAULT_MAX_BYTES);
    }

    #[test]
    fn peer_attributes_join_every_field_by_address() {
        let gateway = peer_at("192.0.2.1:31337");
        let ordinary = peer_at("192.0.2.2:31338");
        let never_seen = peer_at("192.0.2.3:31339");
        let connections = [
            (gateway.clone(), 10.0),
            (ordinary.clone(), 20.0),
            (never_seen.clone(), 30.0),
        ];
        let gateways = [gateway.pub_key().clone()];
        let addr = |peer: &PeerKeyLocation| peer.socket_addr().unwrap();
        // Map insertion order deliberately differs from connection order, so an
        // ordering-based join would misattribute.
        let versions = HashMap::from([
            (addr(&ordinary), (0, 2, 135)),
            (addr(&gateway), (0, 2, 134)),
        ]);
        let health = HashMap::from([(addr(&ordinary), (7, 3)), (addr(&gateway), (100, 1))]);
        let transfer = HashMap::from([(addr(&gateway), (5, 6))]);

        let attributes = peer_attributes(&PeerSnapshotInputs {
            connections: &connections,
            gateways: Some(&gateways),
            versions: &versions,
            health: &health,
            transfer: &transfer,
        });

        assert_eq!(attributes.len(), 3);
        let [gw, ord, unseen] = [&attributes[0], &attributes[1], &attributes[2]];
        assert_eq!(gw.peer, peer_hash(&gateway));
        assert_eq!(gw.is_configured_gateway, Some(true));
        assert_eq!(gw.version.as_deref(), Some("0.2.134"));
        assert_eq!(
            (gw.routing_successes, gw.routing_failures),
            (Some(100), Some(1))
        );
        assert_eq!((gw.bytes_sent, gw.bytes_received), (Some(5), Some(6)));
        assert_eq!(gw.connected_s, 10.0);

        assert_eq!(ord.peer, peer_hash(&ordinary));
        assert_eq!(ord.is_configured_gateway, Some(false));
        assert_eq!(ord.version.as_deref(), Some("0.2.135"));
        assert_eq!(
            (ord.routing_successes, ord.routing_failures),
            (Some(7), Some(3))
        );
        assert_eq!((ord.bytes_sent, ord.bytes_received), (None, None));

        assert_eq!(unseen.version, None);
        assert_eq!(
            (unseen.routing_successes, unseen.routing_failures),
            (None, None)
        );
        assert_eq!(unseen.location, never_seen.location().map(|l| l.as_f64()));

        let unknown = peer_attributes(&PeerSnapshotInputs {
            connections: &connections,
            gateways: None,
            versions: &versions,
            health: &health,
            transfer: &transfer,
        });
        assert!(
            unknown
                .iter()
                .all(|peer| peer.is_configured_gateway.is_none()),
            "an unavailable gateway list must read as unknown, not as 'not a gateway'"
        );
    }

    #[test]
    fn operator_facing_variable_names_are_stable() {
        // These names are what an operator's service unit sets; renaming one
        // silently turns recording off on every node that uses it.
        assert_eq!(DATASET_PATH_ENV, "FREENET_ROUTING_DATASET");
        assert_eq!(DATASET_MAX_BYTES_ENV, "FREENET_ROUTING_DATASET_MAX_BYTES");
    }

    #[test]
    fn peer_hash_is_stable_and_uses_the_whole_key() {
        // Distinct keys on purpose: `PeerKeyLocation::random()` reuses one cached
        // key per thread, so two of those differ only in address — which this
        // hash must ignore.
        let key = TransportKeypair::new().public().clone();
        let a = PeerKeyLocation::new(key.clone(), "192.0.2.1:31337".parse().unwrap());
        let b = peer_at("192.0.2.1:31337");
        assert_eq!(
            peer_hash(&a),
            peer_hash(&PeerKeyLocation::new(
                key,
                "192.0.2.2:31338".parse().unwrap()
            )),
            "identity is the key; a peer that changes address is the same peer"
        );
        assert_ne!(peer_hash(&a), peer_hash(&b));
        assert_eq!(peer_hash(&a).len(), 16);
    }
}
