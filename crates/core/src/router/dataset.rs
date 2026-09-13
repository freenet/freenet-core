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
//! Peer attributes (connection age, protocol version, gateway status, transfer
//! volume) are recorded as periodic snapshots rather than per event, because
//! they live behind connection-manager locks that must not be taken under the
//! router's write lock. They join to events offline on `peer` and time.
//!
//! # Off by default, and nothing leaves the machine
//!
//! Nothing is recorded unless the operator sets `FREENET_ROUTING_DATASET` to a
//! file path. The file is written locally and never transmitted. Peers are
//! identified by a 64-bit hash of their public key, not by address.
//!
//! # Never blocks the caller
//!
//! Records are handed to a dedicated writer thread through a bounded channel
//! with `try_send`; the caller — which holds the router's write lock — never
//! waits on disk. A full channel drops the record and counts it, and the count
//! is written INTO the file (`"kind":"dropped"`), so an incomplete recording
//! says so itself rather than reading as a quiet period. The same holds for the
//! byte cap: when it is reached, a final `"kind":"truncated"` line is written
//! and recording stops.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TrySendError, sync_channel};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::ring::PeerKeyLocation;

/// Environment variable naming the file to record into. Unset → no recording.
pub(crate) const DATASET_PATH_ENV: &str = "FREENET_ROUTING_DATASET";
/// Environment variable overriding [`DEFAULT_MAX_BYTES`].
pub(crate) const DATASET_MAX_BYTES_ENV: &str = "FREENET_ROUTING_DATASET_MAX_BYTES";

/// Default size cap: large enough for days of gateway traffic, small enough
/// that a forgotten setting cannot fill a disk.
pub(crate) const DEFAULT_MAX_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// Channel capacity. At a few hundred bytes per record this bounds the queued
/// memory to a few MiB.
const CHANNEL_CAPACITY: usize = 8192;

/// How often the writer flushes, and reports new drops, when records are
/// arriving slowly.
const FLUSH_INTERVAL: Duration = Duration::from_secs(5);

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

fn unix_millis(now: SystemTime) -> u64 {
    now.duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
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
}

/// One observed routing outcome.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct RouteRecord {
    pub t_ms: u64,
    pub peer: String,
    pub peer_location: Option<f64>,
    pub contract_location: f64,
    pub distance: f64,
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
    pub is_configured_gateway: bool,
    pub version: Option<String>,
    pub routing_successes: Option<u64>,
    pub routing_failures: Option<u64>,
    pub bytes_sent: Option<u64>,
    pub bytes_received: Option<u64>,
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
    Route(RouteRecord),
    Peers {
        t_ms: u64,
        peers: Vec<PeerAttributes>,
    },
}

/// Handle to a running recorder. Cheap to call from hot paths.
pub(crate) struct RoutingDataset {
    tx: SyncSender<Record>,
    dropped: std::sync::Arc<AtomicU64>,
}

impl std::fmt::Debug for RoutingDataset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoutingDataset")
            .field("dropped", &self.dropped.load(Ordering::Relaxed))
            .finish()
    }
}

impl RoutingDataset {
    /// Open `path` for appending and start the writer thread.
    pub(crate) fn open(path: &Path, max_bytes: u64) -> std::io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let (dataset, rx) = Self::unstarted();
        let dropped = dataset.dropped.clone();
        // A plain OS thread, not a runtime task: its whole job is blocking file
        // I/O, and it lives exactly as long as the process. It exits when every
        // sender is gone.
        std::thread::Builder::new()
            .name("routing-dataset".into())
            .spawn(move || write_loop(rx, file, max_bytes, dropped))?;
        Ok(dataset)
    }

    /// A handle whose writer has not been started; the caller owns the receiver.
    fn unstarted() -> (Self, Receiver<Record>) {
        let (tx, rx) = sync_channel(CHANNEL_CAPACITY);
        let dataset = Self {
            tx,
            dropped: std::sync::Arc::new(AtomicU64::new(0)),
        };
        (dataset, rx)
    }

    pub(crate) fn record_route(&self, record: RouteRecord) {
        self.send(Record::Route(record));
    }

    pub(crate) fn record_peers(&self, now: SystemTime, peers: Vec<PeerAttributes>) {
        self.send(Record::Peers {
            t_ms: unix_millis(now),
            peers,
        });
    }

    fn send(&self, record: Record) {
        match self.tx.try_send(record) {
            Ok(()) => {}
            // Full: drop and count — never wait under the router lock.
            // Disconnected: the writer stopped (byte cap or I/O error) and has
            // already said so in the file; counting keeps the total honest.
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

/// Write one line. `false` means the file is unusable and the writer must stop.
fn emit(out: &mut BufWriter<File>, written: &mut u64, line: &Line<'_>) -> bool {
    let Ok(mut bytes) = serde_json::to_vec(line) else {
        // Unserializable (cannot happen for these types): skip, keep going.
        return true;
    };
    bytes.push(b'\n');
    if out.write_all(&bytes).is_err() {
        return false;
    }
    *written += bytes.len() as u64;
    true
}

fn report_drops(
    out: &mut BufWriter<File>,
    written: &mut u64,
    dropped: &AtomicU64,
    reported: &mut u64,
) -> bool {
    let total = dropped.load(Ordering::Relaxed);
    if total == *reported {
        return true;
    }
    *reported = total;
    emit(
        out,
        written,
        &Line::Dropped {
            t_ms: unix_millis(SystemTime::now()),
            total,
        },
    )
}

fn write_loop(
    rx: Receiver<Record>,
    file: File,
    max_bytes: u64,
    dropped: std::sync::Arc<AtomicU64>,
) {
    // Appending to an existing recording counts its bytes against the cap, so a
    // restart loop cannot grow the file past it.
    let mut written = file.metadata().map(|meta| meta.len()).unwrap_or(0);
    let mut out = BufWriter::new(file);
    let mut reported_dropped = 0u64;

    let start = Line::Start {
        t_ms: unix_millis(SystemTime::now()),
        version: env!("CARGO_PKG_VERSION"),
    };
    if !emit(&mut out, &mut written, &start) {
        tracing::warn!("routing dataset: write failed; recording disabled");
        return;
    }

    loop {
        let record = match rx.recv_timeout(FLUSH_INTERVAL) {
            Ok(record) => Some(record),
            Err(RecvTimeoutError::Timeout) => None,
            Err(RecvTimeoutError::Disconnected) => break,
        };

        if !report_drops(&mut out, &mut written, &dropped, &mut reported_dropped) {
            break;
        }

        let Some(record) = record else {
            if out.flush().is_err() {
                break;
            }
            continue;
        };

        if written >= max_bytes {
            let line = Line::Truncated {
                t_ms: unix_millis(SystemTime::now()),
                max_bytes,
                dropped_total: dropped.load(Ordering::Relaxed),
            };
            let marked = emit(&mut out, &mut written, &line);
            if !marked || out.flush().is_err() {
                tracing::warn!("routing dataset: could not write the truncation marker");
            }
            tracing::info!(
                max_bytes,
                "routing dataset: size cap reached; recording stopped"
            );
            // Returning drops the receiver, so later sends count as drops.
            return;
        }

        let ok = match &record {
            Record::Route(route) => emit(&mut out, &mut written, &Line::Route(route)),
            Record::Peers { t_ms, peers } => {
                emit(&mut out, &mut written, &Line::Peers { t_ms: *t_ms, peers })
            }
        };
        if !ok {
            break;
        }
    }
    let _ = report_drops(&mut out, &mut written, &dropped, &mut reported_dropped);
    if out.flush().is_err() {
        tracing::warn!("routing dataset: final flush failed");
    }
    tracing::warn!("routing dataset: writer stopped");
}

/// The process-wide recorder, if the operator enabled one.
///
/// Resolved once. A path that cannot be opened disables recording with a
/// warning rather than failing the node: this is a diagnostic, not a
/// dependency.
pub(crate) fn global() -> Option<&'static RoutingDataset> {
    static DATASET: OnceLock<Option<RoutingDataset>> = OnceLock::new();
    DATASET
        .get_or_init(|| {
            let path = PathBuf::from(std::env::var_os(DATASET_PATH_ENV)?);
            let max_bytes = std::env::var(DATASET_MAX_BYTES_ENV)
                .ok()
                .and_then(|value| value.trim().parse::<u64>().ok())
                .unwrap_or(DEFAULT_MAX_BYTES);
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

/// Read a recording once the asynchronous writer has produced what `want`
/// describes — polling the artifact rather than sleeping a fixed amount.
#[cfg(test)]
pub(crate) fn lines_eventually(
    path: &Path,
    want: impl Fn(&[serde_json::Value]) -> bool,
) -> Vec<serde_json::Value> {
    // The writer is asynchronous by design; poll the artifact rather than
    // sleeping a fixed amount.
    for _ in 0..200 {
        let lines: Vec<serde_json::Value> = std::fs::read_to_string(path)
            .unwrap_or_default()
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect();
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
mod tests {
    use super::*;

    fn route(peer: &str) -> RouteRecord {
        RouteRecord {
            t_ms: 1,
            peer: peer.to_string(),
            peer_location: Some(0.25),
            contract_location: 0.5,
            distance: 0.25,
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
            }),
        }
    }

    #[test]
    fn records_round_trip_as_tagged_json_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("routing.jsonl");
        let dataset = RoutingDataset::open(&path, DEFAULT_MAX_BYTES).unwrap();

        dataset.record_route(route("00000000000000aa"));
        dataset.record_peers(
            UNIX_EPOCH + Duration::from_millis(7),
            vec![PeerAttributes {
                peer: "00000000000000aa".into(),
                location: Some(0.25),
                connected_s: 12.5,
                is_configured_gateway: true,
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
        assert_eq!(lines[1]["peer"], "00000000000000aa");
        assert_eq!(lines[1]["outcome"], "failure");
        assert_eq!(lines[1]["forecasts"]["corrected"], 0.12);
        assert_eq!(lines[2]["kind"], "peers");
        assert_eq!(lines[2]["t_ms"], 7);
        assert_eq!(lines[2]["peers"][0]["is_configured_gateway"], true);
    }

    #[test]
    fn byte_cap_writes_a_truncation_marker_and_stops() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("routing.jsonl");
        // The start line alone exceeds this, so the first record hits the cap.
        let dataset = RoutingDataset::open(&path, 10).unwrap();
        dataset.record_route(route("00000000000000aa"));

        let lines = lines_eventually(&path, |lines| {
            lines.iter().any(|line| line["kind"] == "truncated")
        });
        assert!(
            lines.iter().all(|line| line["kind"] != "route"),
            "no record may be written past the cap: {lines:?}"
        );

        // Once the writer has stopped, further records are counted as drops
        // rather than silently vanishing.
        for _ in 0..200 {
            dataset.record_route(route("00000000000000bb"));
            if dataset.dropped() > 0 {
                return;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        panic!("records sent after the writer stopped must be counted as dropped");
    }

    #[test]
    fn a_full_channel_counts_drops_and_the_file_carries_the_count() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("routing.jsonl");
        // Writer not started yet, so the channel genuinely fills: this is the
        // saturated state, reached deterministically rather than by racing a
        // disk.
        let (dataset, rx) = RoutingDataset::unstarted();
        const OVERFLOW: usize = 5;
        for _ in 0..(CHANNEL_CAPACITY + OVERFLOW) {
            // Must return immediately even though nothing is draining.
            dataset.record_route(route("00000000000000aa"));
        }
        assert_eq!(dataset.dropped(), OVERFLOW as u64);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .unwrap();
        let dropped = dataset.dropped.clone();
        drop(dataset);
        write_loop(rx, file, DEFAULT_MAX_BYTES, dropped);

        let lines = lines_eventually(&path, |_| true);
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

    #[test]
    fn peer_hash_is_stable_and_uses_the_whole_key() {
        // Distinct keys on purpose: `PeerKeyLocation::random()` reuses one cached
        // key per thread, so two of those differ only in address — which this
        // hash must ignore.
        use crate::transport::TransportKeypair;
        let addr: std::net::SocketAddr = "192.0.2.1:31337".parse().unwrap();
        let other_addr: std::net::SocketAddr = "192.0.2.2:31338".parse().unwrap();
        let key = TransportKeypair::new().public().clone();
        let a = PeerKeyLocation::new(key.clone(), addr);
        let b = PeerKeyLocation::new(TransportKeypair::new().public().clone(), addr);
        assert_eq!(
            peer_hash(&a),
            peer_hash(&PeerKeyLocation::new(key, other_addr)),
            "identity is the key; a peer that changes address is the same peer"
        );
        assert_eq!(peer_hash(&a), peer_hash(&a));
        assert_ne!(peer_hash(&a), peer_hash(&b));
        assert_eq!(peer_hash(&a).len(), 16);
    }
}
