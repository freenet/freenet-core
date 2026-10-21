//! Transport layer metrics collection for periodic telemetry snapshots.
//!
//! Instead of emitting telemetry events per-transfer (which could flood the server),
//! we accumulate metrics and emit periodic snapshots every N seconds.
//!
//! Metrics always accumulate (negligible overhead from atomic ops). Snapshots
//! are only taken by TelemetryWorker when telemetry is enabled - if telemetry
//! is disabled, no TelemetryWorker runs and `take_snapshot()` is never called.
//!
//! # Usage
//!
//! ```ignore
//! use freenet::transport::metrics::TRANSPORT_METRICS;
//!
//! // Record a completed transfer
//! TRANSPORT_METRICS.record_transfer_completed(&stats);
//!
//! // Periodically take snapshots (done by TelemetryWorker when telemetry enabled)
//! if let Some(snapshot) = TRANSPORT_METRICS.take_snapshot() {
//!     // Send to telemetry
//! }
//! ```

use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use tokio::sync::mpsc;

use crate::tracing::{TransferDirection, TransferEvent};

/// Global transport metrics instance.
///
/// All peer connections report to this single instance, which aggregates
/// metrics for periodic telemetry snapshots.
pub static TRANSPORT_METRICS: LazyLock<TransportMetrics> = LazyLock::new(TransportMetrics::new);

/// Declares a telemetry label enum with `COUNT`, `ALL` and `label` generated
/// from the one variant list, so a variant cannot be added without growing the
/// per-variant counter arrays (indexed by `variant as usize`). Each enum's
/// tests then fail to compile until the new variant has a snapshot field.
macro_rules! labelled_counters {
    (
        $(#[$emeta:meta])*
        $vis:vis enum $name:ident {
            $($(#[$meta:meta])* $variant:ident => $label:literal,)+
        }
    ) => {
        $(#[$emeta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        $vis enum $name {
            $($(#[$meta])* $variant,)+
        }

        // Not every enum reads every item outside tests.
        #[allow(dead_code)]
        impl $name {
            /// Every variant, in declaration order, so `ALL[i] as usize == i`.
            $vis const ALL: [Self; Self::COUNT] = [$(Self::$variant),+];
            $vis const COUNT: usize = [$(Self::$variant),+].len();

            /// Metric label.
            $vis fn label(self) -> &'static str {
                match self {
                    $(Self::$variant => $label,)+
                }
            }
        }
    };
}

labelled_counters! {
    /// Why an inbound stream was refused or evicted. Each has its own
    /// counter because each has a different remedy.
    pub(crate) enum InboundStreamOutcome {
        /// Refused: declared length 0 or above `MAX_STREAM_TOTAL_BYTES`.
        RefusedInvalidLength => "refused_invalid_length",
        /// Refused: the connection's byte allowance was full.
        RefusedConnectionBytes => "refused_connection_bytes",
        /// Refused: the connection's stream count was full.
        RefusedConnectionStreams => "refused_connection_streams",
        /// Connection closed: a refused stream could not be remembered, because
        /// the connection already had as many refused streams outstanding as are
        /// remembered.
        ClosedTooManyRefusals => "closed_too_many_refusals",
        /// Evicted: no new data within the idle timeout.
        EvictedIdle => "evicted_idle",
        /// Evicted: past its age limit.
        EvictedAge => "evicted_age",
    }
}

labelled_counters! {
    /// Why an outbound NAT traversal was started. Internal only (never on the
    /// wire). Exported per period as `nat_attempts_<label>` and
    /// `nat_established_<label>`, and on each `NatFailureSample`. A dial to a
    /// configured gateway is `Gateway` whichever path started it.
    pub(crate) enum NatAttemptOrigin {
        /// A CONNECT joiner dialling an acceptor that already started punching.
        ConnectJoiner => "connect_joiner",
        /// A CONNECT acceptor punching towards the joiner it just accepted.
        ConnectAcceptor => "connect_acceptor",
        /// A dial to a configured gateway (bootstrap, or any other path).
        Gateway => "gateway",
        /// The event loop had a message for a non-gateway peer that the ring
        /// still lists but has no transport to (the connection was lost), and
        /// dialled it to send the message. The remote is usually not
        /// expecting us, so this works only if it still holds our session.
        /// Cached-peer joins never dial at all (#5954), so they are not here.
        /// Also the tag for callers that use the transport directly (tests,
        /// benchmarks), which have no origin.
        RingPeerNoTransport => "ring_peer_no_transport",
    }
}

labelled_counters! {
    /// An intro packet a non-gateway dropped because no connection or
    /// traversal exists for its exact address. Exactly one per dropped intro;
    /// exported per period as `stray_intro_<label>`.
    pub(crate) enum StrayIntro {
        /// The same address had an outbound traversal time out (failure kind
        /// `no_response`, `undecryptable` or `incomplete`) in the last
        /// `STRAY_INTRO_WINDOW` (10 s): the remote punched after we gave up.
        /// Other failures (version mismatch, send error, ...) are not
        /// remembered, even when their attempt ran to the deadline. Checked
        /// first, as the more specific match. If more than
        /// `MAX_RECENT_TRAVERSAL_FAILURES` failed within the window, the
        /// oldest were forgotten early and their late intros count as another
        /// kind; `stray_intro_failures_evicted` counts how often.
        AfterDeadline => "after_deadline",
        /// A traversal to the same IP on another port is running. Either the
        /// remote's NAT mapped it to a port we are not punching
        /// (endpoint-dependent mapping) and our NAT let the packet through, or
        /// a DIFFERENT peer shares that IP (CGNAT, several peers behind one
        /// NAT). This counter cannot tell the two apart, so it is not direct
        /// evidence of endpoint-dependent mapping on its own.
        SameIpOtherPort => "same_ip_other_port",
        /// Neither.
        Other => "other",
    }
}

labelled_counters! {
    /// How long a CONNECT joiner held an acceptor in its dial queue between
    /// the acceptor's Response arriving and the dial being dispatched. The
    /// acceptor starts punching when it accepts and gives up after
    /// `NAT_TRAVERSAL_DEADLINE` (3 s), so a wait past that is a missed window.
    /// Exported per period as `dial_queue_wait_<label>`.
    pub(crate) enum DialQueueWait {
        /// Under 100 ms (dialled at once).
        Lt100Ms => "lt_100ms",
        /// 100 ms to under 1 s.
        Lt1S => "lt_1s",
        /// 1 s to under 3 s.
        Lt3S => "lt_3s",
        /// 3 s or more.
        Ge3S => "ge_3s",
    }
}

impl DialQueueWait {
    /// The bucket for a wait of `wait`.
    pub(crate) fn bucket(wait: std::time::Duration) -> Self {
        match wait.as_millis() {
            0..100 => Self::Lt100Ms,
            100..1000 => Self::Lt1S,
            1000..3000 => Self::Lt3S,
            _ => Self::Ge3S,
        }
    }
}

/// Accumulates transport metrics for periodic reporting.
///
/// All operations are lock-free using atomics. Metrics are accumulated
/// until `take_snapshot()` is called, which resets the counters.
///
/// # Thread Safety
///
/// This struct is designed for concurrent updates from multiple connections.
/// The snapshot operation is not atomic across all fields, but this is
/// acceptable for telemetry purposes.
///
/// # When Telemetry is Disabled
///
/// Metrics still accumulate (negligible overhead), but `take_snapshot()`
/// is never called since TelemetryWorker doesn't run.
#[derive(Debug)]
pub struct TransportMetrics {
    // Transfer counters (reset each snapshot)
    transfers_completed: AtomicU32,
    transfers_failed: AtomicU32,
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,

    // Cumulative wire-byte counters (never reset, used by the local dashboard).
    //
    // `cumulative_bytes_sent` is updated at the UDP socket layer for every
    // successful `send_to`, so it reflects every byte we put on the wire
    // including keep-alives, ACKs, NAT-traversal probes, and small control
    // messages.
    //
    // `cumulative_bytes_received` is updated post-authentication (see
    // `record_packet_received`), so it counts bytes from packets that pass
    // `try_decrypt_sym` against a symmetric session key (including a
    // handshake's completion ACK), plus intro packets that decrypt with our
    // static key on an existing connection (peer restart). The intro packet
    // that opens a connection is not counted.
    // Counting at the raw socket would let an attacker spoofing UDP source
    // addresses inflate this counter arbitrarily (#3999).
    cumulative_bytes_sent: AtomicU64,
    cumulative_bytes_received: AtomicU64,

    // Cumulative wire-packet counters (never reset). Metered at exactly the
    // same two call sites as the cumulative byte counters above, so the same
    // send-at-socket / receive-post-auth asymmetry applies. Kept here rather
    // than incremented as an OTel counter at the call site because these fire
    // once per UDP datagram — an observable counter reading these costs
    // nothing on the hot path.
    cumulative_packets_sent: AtomicU64,
    cumulative_packets_received: AtomicU64,

    // Cumulative outcome counters (never reset), incremented alongside the
    // period counters they shadow. They exist because the period counters are
    // `swap`ped to zero by `take_snapshot` for the legacy telemetry worker, so
    // an observable OTel counter reading those directly would report a
    // non-monotonic series whenever `telemetry-enabled` is also on.
    //
    // Deliberately a second counter rather than an `otel::record_*` call at
    // each site: a hand-placed mirror rots silently (#4009, #4010 —
    // `.claude/rules/bug-prevention-patterns.md`), while a counter the same
    // function already increments cannot drift from itself.
    cumulative_transfers_completed: AtomicU64,
    cumulative_transfers_failed: AtomicU64,
    cumulative_nat_attempts: AtomicU64,
    cumulative_nat_established: AtomicU64,
    cumulative_nat_failed_error: AtomicU64,
    cumulative_nat_failed_version: AtomicU64,
    /// Inbound streams refused admission or evicted, by reason (never reset).
    /// See [`InboundStreamOutcome`].
    cumulative_inbound_stream_outcomes: [AtomicU64; InboundStreamOutcome::COUNT],

    // Timing accumulators (for computing averages)
    total_transfer_time_ms: AtomicU64,

    // Throughput tracking
    peak_throughput_bps: AtomicU64,

    // LEDBAT stats (peak values during period)
    peak_cwnd_bytes: AtomicU32,
    min_cwnd_bytes: AtomicU32,
    cwnd_sum: AtomicU64,     // For computing average
    cwnd_samples: AtomicU32, // Number of samples

    // Slowdowns during period
    slowdowns_triggered: AtomicU32,

    // NAT hole-punch traversal counters (reset each snapshot).
    /// Outbound NAT traversal attempts started during the period.
    nat_traversal_attempts: AtomicU32,
    /// NAT traversal attempts that established a connection during the period.
    nat_traversal_established: AtomicU32,
    /// NAT traversal attempts that failed for a non-version reason during the
    /// period (unreachable / symmetric-NAT / generic transport error bucket).
    nat_traversal_failed_error: AtomicU32,
    /// NAT traversal attempts that failed due to a protocol version mismatch
    /// during the period.
    nat_traversal_failed_version: AtomicU32,

    // Per-peer wire-byte stats (bounded to MAX_TRACKED_PEERS entries).
    //
    // Receive side is metered post-authentication in
    // `transport::peer_connection::PeerConnection::recv` — only packets that
    // pass `try_decrypt_sym` for an established connection contribute. Send
    // side is metered at the socket layer because outbound targets are
    // controlled by us, not influenced by remote peers.
    //
    // When the table is full a new entry evicts the least-recently-updated
    // entry (LRU on `last_seen_tick`).
    per_peer_stats: DashMap<SocketAddr, PeerTransferStats>,

    // Monotonically increasing tick stamped onto each per-peer entry on
    // update. Used purely for LRU ordering — no wall-clock semantics, so
    // simulation determinism is unaffected.
    per_peer_tick: AtomicU64,

    // RTT tracking (in microseconds for precision)
    min_rtt_us: AtomicU64,
    max_rtt_us: AtomicU64,
    rtt_sum_us: AtomicU64,
    rtt_samples: AtomicU32,

    /// Loss / retransmission / ACK-overhead counters (reset each snapshot).
    /// Every write is a single relaxed `fetch_add`; see [`LossCounters`].
    loss: LossCounters,

    /// NAT hole-punch failure sub-reasons and the NAT-mapping signal (reset
    /// each snapshot). Written once per handshake, never per packet.
    nat_detail: NatDetailCounters,

    /// Stream transfers whose every fragment was acknowledged, and the
    /// time from stream start to the final ACK (reset each snapshot).
    transfers_acked: AtomicU32,
    transfer_ack_time_sum_ms: AtomicU64,
    transfer_ack_time_max_ms: AtomicU64,

    /// Listener-to-connection inbound packet channel: depth high-water mark
    /// and packets dropped because the channel was full (reset each snapshot).
    inbound_queue_depth_hwm: AtomicU32,
    inbound_queue_drops: AtomicU64,

    /// Inbound streams refused or evicted this period, by reason (reset each
    /// snapshot). The never-reset twin is `cumulative_inbound_stream_outcomes`,
    /// which is what the OTel instrument observes.
    inbound_stream_outcomes: [AtomicU32; InboundStreamOutcome::COUNT],

    /// Outbound NAT traversals started / established this period, by
    /// [`NatAttemptOrigin`] (reset each snapshot). Each sums to its untagged
    /// `nat_traversal_*` twin; the cumulative totals are untagged.
    nat_attempts_by_origin: [AtomicU32; NatAttemptOrigin::COUNT],
    nat_established_by_origin: [AtomicU32; NatAttemptOrigin::COUNT],

    /// Intro packets a non-gateway dropped as unknown, by [`StrayIntro`]
    /// (reset each snapshot).
    stray_intros: [AtomicU32; StrayIntro::COUNT],
    /// Recent traversal failures forgotten early because the listener's
    /// table was full (reset each snapshot). Each can turn a later
    /// `AfterDeadline` intro into another kind.
    stray_intro_failures_evicted: AtomicU32,

    /// CONNECT joiner acceptor dials by time spent queued, by
    /// [`DialQueueWait`] (reset each snapshot).
    dial_queue_wait: [AtomicU32; DialQueueWait::COUNT],
}

/// Period counters for packet loss, retransmission and standalone-ACK
/// overhead on established connections.
#[derive(Debug, Default)]
struct LossCounters {
    /// UDP datagrams handed to the socket (all traffic, metered at the same
    /// call site as `cumulative_packets_sent`).
    packets_sent: AtomicU64,
    /// Retransmissions (RTO resends and TLP probes) successfully handed to
    /// the socket.
    packets_retransmitted: AtomicU64,
    /// Packets given up on after `MAX_PACKET_RETRANSMITS` with no ACK.
    packets_abandoned: AtomicU64,
    /// Per-packet RTO expiries (each one triggers a resend).
    rto_expiries: AtomicU64,
    /// Tail-loss probes fired.
    tlp_probes: AtomicU64,
    /// Standalone ack-only `NoOp` packets sent, and their on-wire bytes.
    standalone_ack_packets: AtomicU64,
    standalone_ack_bytes: AtomicU64,
}

/// Period counters splitting failed outbound NAT traversals by cause, plus
/// the endpoint-dependent-mapping signal derived from the external address
/// other nodes report back to us.
#[derive(Debug, Default)]
struct NatDetailCounters {
    no_response: AtomicU32,
    undecryptable: AtomicU32,
    version_incompatible: AtomicU32,
    incomplete: AtomicU32,
    send_error: AtomicU32,
    other: AtomicU32,
    observed_addr_reports: AtomicU32,
    observed_addr_port_changes: AtomicU32,
    observed_addr_ip_changes: AtomicU32,
    /// Most recent `(reporter, observed)` pair per address family (index 0 =
    /// IPv4, 1 = IPv6). Only touched when a remote reports our address during
    /// a handshake, never per packet, so a mutex is fine here. Never
    /// exported: only the change counts above leave the process.
    last_observed_addr: parking_lot::Mutex<[Option<(SocketAddr, SocketAddr)>; 2]>,
    /// Up to [`MAX_NAT_FAILURE_SAMPLES`] per-attempt failure records for the
    /// period, and how many failures did not fit. Written once per failed
    /// handshake (never per packet), so a mutex is fine.
    samples: parking_lot::Mutex<Vec<NatFailureSample>>,
    samples_dropped: AtomicU32,
    /// Test-only log of `(remote, kind, origin)` so handler-path tests can
    /// find THEIR attempt among concurrent tests' (bounded).
    #[cfg(test)]
    failures_by_remote: parking_lot::Mutex<
        std::collections::VecDeque<(SocketAddr, NatFailureKind, NatAttemptOrigin)>,
    >,
    /// Test-only log of admitted attempts `(remote, origin)` (bounded).
    #[cfg(test)]
    attempts_by_remote:
        parking_lot::Mutex<std::collections::VecDeque<(SocketAddr, NatAttemptOrigin)>>,
    /// Test-only log of established attempts `(remote, origin)` (bounded).
    #[cfg(test)]
    established_by_remote:
        parking_lot::Mutex<std::collections::VecDeque<(SocketAddr, NatAttemptOrigin)>>,
    /// Test-only log of joiner dial queue waits `(acceptor, bucket)`
    /// (bounded).
    #[cfg(test)]
    dial_queue_waits_by_remote:
        parking_lot::Mutex<std::collections::VecDeque<(SocketAddr, DialQueueWait)>>,
    /// Test-only log of dropped stray intros `(remote, kind)` (bounded).
    #[cfg(test)]
    stray_intros_by_remote:
        parking_lot::Mutex<std::collections::VecDeque<(SocketAddr, StrayIntro)>>,
    /// Test-only log of observed-address reporters (bounded).
    #[cfg(test)]
    observed_reporters: parking_lot::Mutex<std::collections::VecDeque<SocketAddr>>,
}

/// Why an outbound NAT traversal attempt failed. Exactly one is recorded per
/// failed attempt, so the six counters sum to
/// `nat_traversal_failed_error + nat_traversal_failed_version` (with a
/// different split between version and non-version causes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NatFailureKind {
    /// Deadline expired and no packet at all arrived from the remote.
    NoResponse,
    /// Packets arrived from the remote but none could be decrypted.
    Undecryptable,
    /// The remote's protocol version is incompatible with ours.
    VersionIncompatible,
    /// Packets were decrypted but the handshake did not complete in time.
    Incomplete,
    /// Our sends to the remote were failing.
    SendError,
    /// Anything else (channel closed, malformed ack, ...).
    Other,
}

impl NatFailureKind {
    /// Stable telemetry label; matches the `nat_fail_*` snapshot field suffix.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::NoResponse => "no_response",
            Self::Undecryptable => "undecryptable",
            Self::VersionIncompatible => "version_incompatible",
            Self::Incomplete => "incomplete",
            Self::SendError => "send_error",
            Self::Other => "other",
        }
    }
}

/// Test-only: append to a bounded log, dropping the oldest entry when full,
/// so a long single-process test run never stops recording.
#[cfg(test)]
fn push_test_log<T>(log: &parking_lot::Mutex<std::collections::VecDeque<T>>, entry: T) {
    let mut log = log.lock();
    if log.len() >= 4096 {
        log.pop_front();
    }
    log.push_back(entry);
}

/// What a failed outbound NAT traversal saw, as recorded on its
/// [`NatFailureSample`].
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct NatAttemptSeen {
    /// Any packet arrived from the remote.
    pub received_any: bool,
    /// A packet from the remote was decrypted.
    pub decrypted_any: bool,
    /// The attempt ran to its deadline.
    pub deadline_reached: bool,
}

/// Per-period cap on [`NatFailureSample`]s carried by `transport_snapshot`.
/// Failures beyond it are counted in `nat_fail_samples_dropped`; the
/// `nat_fail_*` counters still count every failure.
const MAX_NAT_FAILURE_SAMPLES: usize = 8;

/// What one failed outbound NAT traversal saw. Carried in the periodic
/// `transport_snapshot` (bounded per period) rather than as an event per
/// failure, so a burst of failures costs no telemetry budget, and so the
/// samples can be read alongside the same period's `observed_addr_*`
/// mapping signal. No remote address is recorded.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub struct NatFailureSample {
    /// [`NatFailureKind`] label, e.g. `no_response`.
    pub sub_reason: String,
    /// Any packet arrived from the remote.
    pub received_any: bool,
    /// A packet from the remote was decrypted.
    pub decrypted_any: bool,
    /// The attempt ran to its deadline.
    pub deadline_reached: bool,
    /// [`NatAttemptOrigin`] label, e.g. `connect_joiner`.
    pub origin: String,
}

/// Nearest-rank percentile of an ascending slice; `None` when empty. These are
/// exact order statistics over one value per connection, so they are
/// meaningful at small N (the connection count is reported alongside).
fn percentile(sorted: &[u64], pct: usize) -> Option<u64> {
    if sorted.is_empty() {
        return None;
    }
    // Nearest-rank percentile: ceil(pct/100 * N), 1-based.
    let rank = (pct * sorted.len()).div_ceil(100).max(1);
    sorted.get(rank - 1).copied()
}

/// Per-connection RTT distribution across live connections, computed from
/// one value per connection (not per sample).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ConnRttDistribution {
    pub connections: u32,
    pub srtt_p50_us: Option<u64>,
    pub srtt_p90_us: Option<u64>,
    pub srtt_max_us: Option<u64>,
    pub min_rtt_p50_us: Option<u64>,
    pub min_rtt_p90_us: Option<u64>,
    pub min_rtt_max_us: Option<u64>,
}

impl ConnRttDistribution {
    /// Build from `(srtt_us, min_rtt_us)` pairs, one per connection that has
    /// at least one RTT sample.
    pub(crate) fn from_samples(mut srtt: Vec<u64>, mut min_rtt: Vec<u64>) -> Self {
        srtt.sort_unstable();
        min_rtt.sort_unstable();
        Self {
            connections: srtt.len().max(min_rtt.len()) as u32,
            srtt_p50_us: percentile(&srtt, 50),
            srtt_p90_us: percentile(&srtt, 90),
            srtt_max_us: srtt.last().copied(),
            min_rtt_p50_us: percentile(&min_rtt, 50),
            min_rtt_p90_us: percentile(&min_rtt, 90),
            min_rtt_max_us: min_rtt.last().copied(),
        }
    }
}

#[cfg(test)]
thread_local! {
    /// Test-only per-address byte totals returned by
    /// [`TransportMetrics::connection_peer_bytes`] in place of the global table.
    pub(crate) static PEER_BYTES_OVERRIDE: std::cell::RefCell<
        std::collections::HashMap<SocketAddr, (u64, u64)>,
    > = std::cell::RefCell::new(std::collections::HashMap::new());
}

/// Maximum number of peers tracked for per-peer transfer stats.
const MAX_TRACKED_PEERS: usize = 256;

/// Per-peer cumulative transfer statistics.
#[derive(Debug)]
pub struct PeerTransferStats {
    pub bytes_sent: AtomicU64,
    pub bytes_received: AtomicU64,
    /// Monotonic tick of the most recent update; smallest tick = oldest entry,
    /// used for LRU eviction when the table is full.
    last_seen_tick: AtomicU64,
}

impl Default for PeerTransferStats {
    fn default() -> Self {
        Self {
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            last_seen_tick: AtomicU64::new(0),
        }
    }
}

impl Default for TransportMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl TransportMetrics {
    /// Create a new metrics collector.
    pub fn new() -> Self {
        Self {
            transfers_completed: AtomicU32::new(0),
            transfers_failed: AtomicU32::new(0),
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            cumulative_bytes_sent: AtomicU64::new(0),
            cumulative_bytes_received: AtomicU64::new(0),
            cumulative_packets_sent: AtomicU64::new(0),
            cumulative_packets_received: AtomicU64::new(0),
            cumulative_transfers_completed: AtomicU64::new(0),
            cumulative_transfers_failed: AtomicU64::new(0),
            cumulative_nat_attempts: AtomicU64::new(0),
            cumulative_nat_established: AtomicU64::new(0),
            cumulative_nat_failed_error: AtomicU64::new(0),
            cumulative_nat_failed_version: AtomicU64::new(0),
            cumulative_inbound_stream_outcomes: Default::default(),
            total_transfer_time_ms: AtomicU64::new(0),
            peak_throughput_bps: AtomicU64::new(0),
            peak_cwnd_bytes: AtomicU32::new(0),
            min_cwnd_bytes: AtomicU32::new(u32::MAX),
            cwnd_sum: AtomicU64::new(0),
            cwnd_samples: AtomicU32::new(0),
            slowdowns_triggered: AtomicU32::new(0),
            nat_traversal_attempts: AtomicU32::new(0),
            nat_traversal_established: AtomicU32::new(0),
            nat_traversal_failed_error: AtomicU32::new(0),
            nat_traversal_failed_version: AtomicU32::new(0),
            min_rtt_us: AtomicU64::new(u64::MAX),
            max_rtt_us: AtomicU64::new(0),
            rtt_sum_us: AtomicU64::new(0),
            rtt_samples: AtomicU32::new(0),
            per_peer_stats: DashMap::new(),
            per_peer_tick: AtomicU64::new(0),
            loss: LossCounters::default(),
            nat_detail: NatDetailCounters::default(),
            transfers_acked: AtomicU32::new(0),
            transfer_ack_time_sum_ms: AtomicU64::new(0),
            transfer_ack_time_max_ms: AtomicU64::new(0),
            inbound_queue_depth_hwm: AtomicU32::new(0),
            inbound_queue_drops: AtomicU64::new(0),
            inbound_stream_outcomes: Default::default(),
            nat_attempts_by_origin: Default::default(),
            nat_established_by_origin: Default::default(),
            stray_intros: Default::default(),
            stray_intro_failures_evicted: AtomicU32::new(0),
            dial_queue_wait: Default::default(),
        }
    }

    /// A per-packet RTO fired on an established connection.
    pub(crate) fn record_rto_expiry(&self) {
        self.loss.rto_expiries.fetch_add(1, Ordering::Relaxed);
    }

    /// A tail-loss probe fired on an established connection.
    pub(crate) fn record_tlp_probe(&self) {
        self.loss.tlp_probes.fetch_add(1, Ordering::Relaxed);
    }

    /// A retransmission (RTO resend or TLP probe) was handed to the socket.
    pub(crate) fn record_retransmission_sent(&self) {
        self.loss
            .packets_retransmitted
            .fetch_add(1, Ordering::Relaxed);
    }

    /// A packet was abandoned after `MAX_PACKET_RETRANSMITS` with no ACK.
    pub(crate) fn record_packet_abandoned(&self) {
        self.loss.packets_abandoned.fetch_add(1, Ordering::Relaxed);
    }

    /// A standalone ack-only `NoOp` of `bytes` on-wire bytes was sent.
    pub(crate) fn record_standalone_ack(&self, bytes: u64) {
        self.loss
            .standalone_ack_packets
            .fetch_add(1, Ordering::Relaxed);
        self.loss
            .standalone_ack_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record one failed outbound NAT traversal to `remote`: its
    /// classification counter and, while the period has room, a per-attempt
    /// sample. `remote` is never exported (tests use it to find their own
    /// attempt).
    pub(crate) fn record_nat_failure(
        &self,
        remote: SocketAddr,
        kind: NatFailureKind,
        seen: NatAttemptSeen,
        origin: NatAttemptOrigin,
    ) {
        #[cfg(test)]
        push_test_log(&self.nat_detail.failures_by_remote, (remote, kind, origin));
        #[cfg(not(test))]
        let _ = remote;
        let NatAttemptSeen {
            received_any,
            decrypted_any,
            deadline_reached,
        } = seen;
        self.record_nat_failure_kind(kind);
        let mut samples = self.nat_detail.samples.lock();
        if samples.len() < MAX_NAT_FAILURE_SAMPLES {
            samples.push(NatFailureSample {
                sub_reason: kind.as_str().to_string(),
                received_any,
                decrypted_any,
                deadline_reached,
                origin: origin.label().to_string(),
            });
        } else {
            self.nat_detail
                .samples_dropped
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Test-only: the failure kinds recorded for `remote`.
    #[cfg(test)]
    pub(crate) fn nat_failures_for(&self, remote: SocketAddr) -> Vec<NatFailureKind> {
        self.nat_detail
            .failures_by_remote
            .lock()
            .iter()
            .filter(|(addr, _, _)| *addr == remote)
            .map(|(_, kind, _)| *kind)
            .collect()
    }

    /// Test-only: the origins of the failures recorded for `remote`.
    #[cfg(test)]
    pub(crate) fn nat_failure_origins_for(&self, remote: SocketAddr) -> Vec<NatAttemptOrigin> {
        self.nat_detail
            .failures_by_remote
            .lock()
            .iter()
            .filter(|(addr, _, _)| *addr == remote)
            .map(|(_, _, origin)| *origin)
            .collect()
    }

    /// Test-only: the origins of the traversals to `remote` that
    /// established.
    #[cfg(test)]
    pub(crate) fn nat_established_origins_for(&self, remote: SocketAddr) -> Vec<NatAttemptOrigin> {
        self.nat_detail
            .established_by_remote
            .lock()
            .iter()
            .filter(|(addr, _)| *addr == remote)
            .map(|(_, origin)| *origin)
            .collect()
    }

    /// Test-only: the origins of the attempts admitted to `remote`.
    #[cfg(test)]
    pub(crate) fn nat_attempt_origins_for(&self, remote: SocketAddr) -> Vec<NatAttemptOrigin> {
        self.nat_detail
            .attempts_by_remote
            .lock()
            .iter()
            .filter(|(addr, _)| *addr == remote)
            .map(|(_, origin)| *origin)
            .collect()
    }

    /// Test-only: the dial queue wait buckets recorded for `acceptor`.
    #[cfg(test)]
    pub(crate) fn dial_queue_waits_for(&self, acceptor: SocketAddr) -> Vec<DialQueueWait> {
        self.nat_detail
            .dial_queue_waits_by_remote
            .lock()
            .iter()
            .filter(|(addr, _)| *addr == acceptor)
            .map(|(_, bucket)| *bucket)
            .collect()
    }

    /// Test-only: the stray intros recorded from `remote`.
    #[cfg(test)]
    pub(crate) fn stray_intros_from(&self, remote: SocketAddr) -> Vec<StrayIntro> {
        self.nat_detail
            .stray_intros_by_remote
            .lock()
            .iter()
            .filter(|(addr, _)| *addr == remote)
            .map(|(_, kind)| *kind)
            .collect()
    }

    /// Test-only: whether `reporter` has reported our external address.
    #[cfg(test)]
    pub(crate) fn observed_addr_reported_by(&self, reporter: SocketAddr) -> bool {
        self.nat_detail
            .observed_reporters
            .lock()
            .contains(&reporter)
    }

    /// Classify one failed outbound NAT traversal (counter only).
    fn record_nat_failure_kind(&self, kind: NatFailureKind) {
        let d = &self.nat_detail;
        let counter = match kind {
            NatFailureKind::NoResponse => &d.no_response,
            NatFailureKind::Undecryptable => &d.undecryptable,
            NatFailureKind::VersionIncompatible => &d.version_incompatible,
            NatFailureKind::Incomplete => &d.incomplete,
            NatFailureKind::SendError => &d.send_error,
            NatFailureKind::Other => &d.other,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Remote `reporter` told us the external address it sees us at
    /// (`observed`).
    ///
    /// Compared with the latest report from a DIFFERENT reporter in the same
    /// address family. Every connection leaves through one local socket, so
    /// with an endpoint-independent NAT mapping all reporters see the same
    /// `ip:port`; a different port on the same IP is the signature of an
    /// endpoint-dependent (symmetric) mapping. A different IP is counted
    /// separately (multi-homing, address change). Repeat reports from the
    /// same reporter, and reports in the other address family, are never
    /// counted as changes, so several gateways or a dual-stack host do not
    /// produce false positives. An IPv4-mapped IPv6 address is treated as
    /// IPv4. Residual false positive: a NAT that rebinds our mapping between
    /// two different reporters' reports looks the same as an
    /// endpoint-dependent mapping. Addresses are never exported.
    pub(crate) fn record_observed_external_addr(&self, reporter: SocketAddr, observed: SocketAddr) {
        let d = &self.nat_detail;
        d.observed_addr_reports.fetch_add(1, Ordering::Relaxed);
        #[cfg(test)]
        {
            let mut log = d.observed_reporters.lock();
            if log.len() >= 4096 {
                log.pop_front();
            }
            log.push_back(reporter);
        }
        let observed = SocketAddr::new(observed.ip().to_canonical(), observed.port());
        let family = usize::from(observed.is_ipv6());
        let previous = d.last_observed_addr.lock()[family].replace((reporter, observed));
        let Some((prev_reporter, prev_observed)) = previous else {
            return;
        };
        if prev_reporter == reporter {
            return;
        }
        if prev_observed.ip() != observed.ip() {
            d.observed_addr_ip_changes.fetch_add(1, Ordering::Relaxed);
        } else if prev_observed.port() != observed.port() {
            d.observed_addr_port_changes.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Every fragment of an outbound stream was acknowledged `elapsed_ms`
    /// after the stream started (compare `avg_transfer_time_ms`, which stops
    /// at the hand-off of the last fragment to the socket).
    pub(crate) fn record_transfer_acked(&self, elapsed_ms: u64) {
        self.transfer_ack_time_sum_ms
            .fetch_add(elapsed_ms, Ordering::Relaxed);
        self.transfer_ack_time_max_ms
            .fetch_max(elapsed_ms, Ordering::Relaxed);
        self.transfers_acked.fetch_add(1, Ordering::Relaxed);
    }

    /// Observed depth of a listener-to-connection inbound channel right after
    /// a successful `try_send`.
    pub(crate) fn record_inbound_queue_depth(&self, depth: u32) {
        self.inbound_queue_depth_hwm
            .fetch_max(depth, Ordering::Relaxed);
    }

    /// The listener dropped an inbound packet because the connection's
    /// channel was full.
    pub(crate) fn record_inbound_queue_drop(&self) {
        self.inbound_queue_drops.fetch_add(1, Ordering::Relaxed);
    }

    /// Per-peer wire bytes as read by the ring's per-connection bookkeeping
    /// (baseline at connect, totals at disconnect). Same as
    /// [`Self::peer_bytes`]; tests can substitute values for an address via
    /// [`PEER_BYTES_OVERRIDE`] so they do not depend on the shared, bounded
    /// per-peer table.
    pub(crate) fn connection_peer_bytes(&self, addr: SocketAddr) -> Option<(u64, u64)> {
        #[cfg(test)]
        if let Some(bytes) = PEER_BYTES_OVERRIDE.with(|o| o.borrow().get(&addr).copied()) {
            return Some(bytes);
        }
        self.peer_bytes(addr)
    }

    /// Wire bytes `(sent, received)` recorded for `addr` while it has been
    /// tracked in the per-peer table. `None` if the peer is not tracked (it
    /// was evicted by LRU or already removed).
    pub fn peer_bytes(&self, addr: SocketAddr) -> Option<(u64, u64)> {
        self.per_peer_stats.get(&addr).map(|s| {
            (
                s.bytes_sent.load(Ordering::Relaxed),
                s.bytes_received.load(Ordering::Relaxed),
            )
        })
    }

    /// Record a completed outbound transfer.
    pub fn record_transfer_completed(&self, stats: &super::TransferStats) {
        self.cumulative_transfers_completed
            .fetch_add(1, Ordering::Relaxed);
        // Use saturating arithmetic to prevent overflow (though extremely unlikely
        // in practice - would require billions of transfers or exabytes of data)
        self.transfers_completed
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_add(1))
            })
            .ok();
        // Note: `bytes_sent` here aggregates stream-transfer payload sizes for
        // the periodic telemetry snapshot. Wire-byte counters
        // (`cumulative_bytes_sent`, per-peer `bytes_sent`) are updated at the
        // socket layer in `record_packet_sent` and intentionally not touched
        // here.
        self.bytes_sent
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_add(stats.bytes_transferred))
            })
            .ok();
        self.total_transfer_time_ms
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_add(stats.elapsed.as_millis() as u64))
            })
            .ok();

        // Update throughput peak
        let throughput = stats.avg_throughput_bps();
        self.peak_throughput_bps
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                if throughput > current {
                    Some(throughput)
                } else {
                    None
                }
            })
            .ok();

        // Update cwnd stats. A controller without a bounded window (FixedRate)
        // reports `None`; recording its effectively-infinite window would make
        // the snapshot's avg/peak/min cwnd a sentinel rather than a value.
        if let Some(final_cwnd) = stats.final_cwnd_bytes {
            self.record_cwnd_sample(final_cwnd);
        }
        if let Some(peak_cwnd) = stats.peak_cwnd_bytes {
            self.peak_cwnd_bytes.fetch_max(peak_cwnd, Ordering::Relaxed);
        }

        self.slowdowns_triggered
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_add(stats.slowdowns_triggered))
            })
            .ok();

        // Record RTT sample from base_delay
        let rtt_us = stats.base_delay.as_micros() as u64;
        if rtt_us > 0 {
            self.record_rtt_sample(rtt_us);
        }
    }

    /// Record a failed outbound transfer.
    ///
    /// The failure sibling of [`Self::record_transfer_completed`]. Together the
    /// two count every outbound stream that resolves: `send_stream` /
    /// `pipe_stream` either run to completion (completed) or return early from
    /// one of their error sites (failed), never both.
    ///
    /// Call this once per failed stream, from the OUTBOUND-stream arms of
    /// `PeerConnection::recv` — the place every outbound stream task's result
    /// is observed. (`recv` also observes `inbound_stream_futures`; those are
    /// not transfers this counter tracks.)
    ///
    /// It mirrors the `emit_transfer_failed` event for every stream whose
    /// result is observed there, but the two are NOT unconditionally 1:1: the
    /// event is emitted inside the stream task, while this counter increments
    /// only at the observation site, so a task that panics or is orphaned by
    /// connection teardown diverges. See the call sites in `peer_connection.rs`
    /// for both gaps and for the classification rationale.
    ///
    /// Unlike the success path this records ONLY the counter: a failed transfer
    /// has no meaningful duration, throughput, or payload size to aggregate,
    /// and the bytes it did put on the wire are already counted at the socket
    /// layer by `record_packet_sent`.
    pub fn record_transfer_failed(&self) {
        self.cumulative_transfers_failed
            .fetch_add(1, Ordering::Relaxed);
        // Saturating, matching `record_transfer_completed` — a pinned u32::MAX
        // is a better failure mode than wrapping to 0 and reporting a healthy
        // node.
        self.transfers_failed
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_add(1))
            })
            .ok();
    }

    /// Record the start of an outbound NAT traversal attempt to `remote`
    /// (`remote` is never exported; tests use it to find their own attempt).
    pub(crate) fn record_nat_traversal_attempt(
        &self,
        remote: SocketAddr,
        origin: NatAttemptOrigin,
    ) {
        #[cfg(test)]
        push_test_log(&self.nat_detail.attempts_by_remote, (remote, origin));
        #[cfg(not(test))]
        let _ = remote;
        self.cumulative_nat_attempts.fetch_add(1, Ordering::Relaxed);
        self.nat_traversal_attempts.fetch_add(1, Ordering::Relaxed);
        self.nat_attempts_by_origin[origin as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// Record a NAT traversal attempt to `remote` that established a
    /// connection (`remote` is never exported).
    pub(crate) fn record_nat_traversal_established(
        &self,
        remote: SocketAddr,
        origin: NatAttemptOrigin,
    ) {
        #[cfg(test)]
        push_test_log(&self.nat_detail.established_by_remote, (remote, origin));
        #[cfg(not(test))]
        let _ = remote;
        self.cumulative_nat_established
            .fetch_add(1, Ordering::Relaxed);
        self.nat_traversal_established
            .fetch_add(1, Ordering::Relaxed);
        self.nat_established_by_origin[origin as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// Record an intro packet from `remote` that a non-gateway dropped as
    /// unknown (`remote` is never exported).
    pub(crate) fn record_stray_intro(&self, remote: SocketAddr, kind: StrayIntro) {
        #[cfg(test)]
        push_test_log(&self.nat_detail.stray_intros_by_remote, (remote, kind));
        #[cfg(not(test))]
        let _ = remote;
        self.stray_intros[kind as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// Record `n` recent traversal failures the listener forgot early
    /// because its table was full.
    pub(crate) fn record_stray_intro_failures_evicted(&self, n: u32) {
        if n > 0 {
            self.stray_intro_failures_evicted
                .fetch_add(n, Ordering::Relaxed);
        }
    }

    /// Record how long a CONNECT joiner held `acceptor` before dialling it
    /// (`acceptor` is never exported).
    pub(crate) fn record_dial_queue_wait(&self, acceptor: SocketAddr, wait: std::time::Duration) {
        let bucket = DialQueueWait::bucket(wait);
        #[cfg(test)]
        push_test_log(
            &self.nat_detail.dial_queue_waits_by_remote,
            (acceptor, bucket),
        );
        #[cfg(not(test))]
        let _ = acceptor;
        self.dial_queue_wait[bucket as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// Record a NAT traversal attempt that failed for a non-version reason
    /// (unreachable / symmetric-NAT / generic transport error).
    pub fn record_nat_traversal_failed_error(&self) {
        self.cumulative_nat_failed_error
            .fetch_add(1, Ordering::Relaxed);
        self.nat_traversal_failed_error
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a NAT traversal attempt that failed due to a protocol version
    /// mismatch.
    pub fn record_nat_traversal_failed_version(&self) {
        self.cumulative_nat_failed_version
            .fetch_add(1, Ordering::Relaxed);
        self.nat_traversal_failed_version
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cwnd sample (called periodically or on transfer completion).
    pub(crate) fn record_cwnd_sample(&self, cwnd_bytes: u32) {
        crate::tracing::otel::record_cwnd(cwnd_bytes as u64);
        self.cwnd_sum
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_add(cwnd_bytes as u64))
            })
            .ok();
        self.cwnd_samples
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_add(1))
            })
            .ok();

        // Update min
        self.min_cwnd_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                if cwnd_bytes < current {
                    Some(cwnd_bytes)
                } else {
                    None
                }
            })
            .ok();
    }

    /// Record an RTT sample in microseconds.
    ///
    /// Called both from [`Self::record_transfer_completed`] (LEDBAT
    /// `base_delay` at stream completion) and from the ping/pong keep-alive
    /// cycle in `peer_connection.rs` (round-trip of a keep-alive Ping). The
    /// keep-alive path is what keeps RTT statistics populated for quiet,
    /// long-lived connections that rarely complete a stream transfer (#4000).
    pub(crate) fn record_rtt_sample(&self, rtt_us: u64) {
        crate::tracing::otel::record_rtt_ms(rtt_us as f64 / 1000.0);
        self.rtt_sum_us
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_add(rtt_us))
            })
            .ok();
        self.rtt_samples
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_add(1))
            })
            .ok();

        // Update min
        self.min_rtt_us
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                if rtt_us < current { Some(rtt_us) } else { None }
            })
            .ok();

        // Update max
        self.max_rtt_us
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                if rtt_us > current { Some(rtt_us) } else { None }
            })
            .ok();
    }

    /// Read cumulative bytes uploaded without resetting counters.
    ///
    /// Used by the local dashboard to display lifetime transfer stats
    /// without interfering with the periodic telemetry snapshots.
    pub fn cumulative_bytes_sent(&self) -> u64 {
        self.cumulative_bytes_sent.load(Ordering::Relaxed)
    }

    /// Current period count of fully acknowledged transfers (tests only).
    #[cfg(test)]
    pub(crate) fn transfers_acked_count(&self) -> u32 {
        self.transfers_acked.load(Ordering::Relaxed)
    }

    /// Record a completed inbound stream transfer.
    ///
    /// Aggregates stream-payload bytes into the per-snapshot `bytes_received`
    /// counter, which pairs with `transfers_completed` for periodic telemetry.
    /// Wire-byte counters (`cumulative_bytes_received`, per-peer
    /// `bytes_received`) are owned by [`Self::record_packet_received`] at the
    /// socket layer.
    pub fn record_inbound_completed(&self, bytes: u64) {
        self.bytes_received.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record an outbound UDP packet at the socket layer.
    ///
    /// Updates the cumulative dashboard counter and the per-peer counter so
    /// keep-alives, ACKs, and other small control messages all show up on the
    /// local dashboard, not just large stream transfers.
    ///
    /// Outbound is metered at the socket layer because send targets are
    /// controlled by us — there is no spoof vector — and we want to surface
    /// every byte we put on the wire (including NAT-traversal probes that
    /// never produce a stream).
    ///
    /// `remote_addr` MUST be the canonical (un-mapped) form used elsewhere in
    /// the system — the same form `recv_from` returns after
    /// `normalize_mapped_addr`. Mismatched callers (e.g. passing a
    /// `::ffff:x.x.x.x` mapped target on a dual-stack socket) would create a
    /// duplicate per-peer entry that the dashboard cannot join against
    /// `connected_peers[].address`.
    pub fn record_packet_sent(&self, remote_addr: SocketAddr, bytes: u64) {
        self.cumulative_bytes_sent
            .fetch_add(bytes, Ordering::Relaxed);
        self.cumulative_packets_sent.fetch_add(1, Ordering::Relaxed);
        self.loss.packets_sent.fetch_add(1, Ordering::Relaxed);
        self.record_per_peer(remote_addr, bytes, |s| &s.bytes_sent);
    }

    /// Record an inbound UDP packet that has passed authentication.
    ///
    /// MUST only be called for packets that have been successfully decrypted:
    /// against a symmetric session key (the call site in
    /// [`crate::transport::peer_connection::PeerConnection::recv`], and the
    /// handshake completion ACKs in `connection_handler`), or, on the
    /// peer-restart path in `recv`, as an intro packet with our static key. Calling
    /// this on raw `recv_from` output would let an attacker inflate the
    /// dashboard by spoofing UDP packets from many source IPs (#3999).
    ///
    /// See [`Self::record_packet_sent`] for the canonical-address contract on
    /// `remote_addr`.
    pub fn record_packet_received(&self, remote_addr: SocketAddr, bytes: u64) {
        self.cumulative_bytes_received
            .fetch_add(bytes, Ordering::Relaxed);
        self.cumulative_packets_received
            .fetch_add(1, Ordering::Relaxed);
        self.record_per_peer(remote_addr, bytes, |s| &s.bytes_received);
    }

    /// Read cumulative bytes downloaded without resetting counters.
    pub fn cumulative_bytes_received(&self) -> u64 {
        self.cumulative_bytes_received.load(Ordering::Relaxed)
    }

    /// Read cumulative packets sent/received without resetting counters.
    pub(crate) fn cumulative_packets(&self) -> (u64, u64) {
        (
            self.cumulative_packets_sent.load(Ordering::Relaxed),
            self.cumulative_packets_received.load(Ordering::Relaxed),
        )
    }

    /// Cumulative stream transfers `(completed, failed)`, never reset — the
    /// monotonic twin of the period counters `take_snapshot` consumes.
    pub(crate) fn cumulative_transfers(&self) -> (u64, u64) {
        (
            self.cumulative_transfers_completed.load(Ordering::Relaxed),
            self.cumulative_transfers_failed.load(Ordering::Relaxed),
        )
    }

    /// Record an inbound stream refused or evicted.
    pub(crate) fn record_inbound_stream_outcome(&self, outcome: InboundStreamOutcome) {
        self.cumulative_inbound_stream_outcomes[outcome as usize].fetch_add(1, Ordering::Relaxed);
        self.inbound_stream_outcomes[outcome as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// Cumulative inbound stream outcomes (refusals and evictions), never
    /// reset, one entry per [`InboundStreamOutcome`] in declaration order.
    pub(crate) fn cumulative_inbound_stream_outcomes(
        &self,
    ) -> [(InboundStreamOutcome, u64); InboundStreamOutcome::COUNT] {
        InboundStreamOutcome::ALL.map(|outcome| {
            (
                outcome,
                self.cumulative_inbound_stream_outcomes[outcome as usize].load(Ordering::Relaxed),
            )
        })
    }

    /// Cumulative NAT traversal outcomes, never reset, in the order
    /// `(attempt, established, failed_error, failed_version)`.
    pub(crate) fn cumulative_nat_traversal(&self) -> (u64, u64, u64, u64) {
        (
            self.cumulative_nat_attempts.load(Ordering::Relaxed),
            self.cumulative_nat_established.load(Ordering::Relaxed),
            self.cumulative_nat_failed_error.load(Ordering::Relaxed),
            self.cumulative_nat_failed_version.load(Ordering::Relaxed),
        )
    }

    /// Record per-peer bytes for the given direction.
    ///
    /// Bounded to [`MAX_TRACKED_PEERS`] entries with LRU eviction: when the
    /// table is full, the entry whose `last_seen_tick` is smallest is
    /// removed before the new entry is inserted. This keeps the dashboard's
    /// per-peer view current as peers come and go without unbounded growth,
    /// and combined with the post-authentication call site in
    /// [`Self::record_packet_received`] closes the spoof inflation vector
    /// described in #3999.
    fn record_per_peer(
        &self,
        addr: SocketAddr,
        bytes: u64,
        field: impl Fn(&PeerTransferStats) -> &AtomicU64,
    ) {
        let tick = self.per_peer_tick.fetch_add(1, Ordering::Relaxed) + 1;

        if let Some(entry) = self.per_peer_stats.get(&addr) {
            field(&entry).fetch_add(bytes, Ordering::Relaxed);
            entry.last_seen_tick.store(tick, Ordering::Relaxed);
            return;
        }

        // The capacity check / eviction / insert isn't atomic across threads,
        // so the table may transiently hold up to MAX_TRACKED_PEERS + a small
        // burst — acceptable for a dashboard counter.
        if self.per_peer_stats.len() >= MAX_TRACKED_PEERS {
            self.evict_oldest_peer();
        }
        let entry = self.per_peer_stats.entry(addr).or_default();
        field(&entry).fetch_add(bytes, Ordering::Relaxed);
        entry.last_seen_tick.store(tick, Ordering::Relaxed);
    }

    /// Remove the per-peer entry whose `last_seen_tick` is smallest.
    ///
    /// O(MAX_TRACKED_PEERS) but called only on insert into a full table.
    fn evict_oldest_peer(&self) {
        let oldest = self
            .per_peer_stats
            .iter()
            .min_by_key(|entry| entry.value().last_seen_tick.load(Ordering::Relaxed))
            .map(|entry| *entry.key());
        if let Some(addr) = oldest {
            self.per_peer_stats.remove(&addr);
        }
    }

    /// Remove a peer's per-peer stats.
    ///
    /// Called when the connection to `addr` is torn down so the slot becomes
    /// available for a new peer immediately, instead of relying on LRU
    /// eviction to clean up after the connection-level state is gone.
    pub fn remove_peer(&self, addr: SocketAddr) {
        self.per_peer_stats.remove(&addr);
    }

    /// Read (and, when `reset`, consume) the period counters added for the
    /// transport-redesign baseline. Swapped as a group; like the rest of the
    /// snapshot this is not atomic across fields, which is fine for telemetry.
    fn period_extras(&self, reset: bool) -> SnapshotExtras {
        let get64 = |a: &AtomicU64| {
            if reset {
                a.swap(0, Ordering::Relaxed)
            } else {
                a.load(Ordering::Relaxed)
            }
        };
        let get32 = |a: &AtomicU32| {
            if reset {
                a.swap(0, Ordering::Relaxed)
            } else {
                a.load(Ordering::Relaxed)
            }
        };
        let l = &self.loss;
        let d = &self.nat_detail;
        // Sum before count, mirroring `record_transfer_acked` (count last),
        // so a concurrent record never yields a count with no sum.
        let ack_sum = get64(&self.transfer_ack_time_sum_ms);
        let ack_max = get64(&self.transfer_ack_time_max_ms);
        let acked = get32(&self.transfers_acked);
        let attempts = &self.nat_attempts_by_origin;
        let established = &self.nat_established_by_origin;
        let stray = &self.stray_intros;
        let wait = &self.dial_queue_wait;
        SnapshotExtras {
            packets_sent: get64(&l.packets_sent),
            packets_retransmitted: get64(&l.packets_retransmitted),
            packets_abandoned: get64(&l.packets_abandoned),
            rto_expiries: get64(&l.rto_expiries),
            tlp_probes: get64(&l.tlp_probes),
            standalone_ack_packets: get64(&l.standalone_ack_packets),
            standalone_ack_bytes: get64(&l.standalone_ack_bytes),
            nat_fail_no_response: get32(&d.no_response),
            nat_fail_undecryptable: get32(&d.undecryptable),
            nat_fail_version_incompatible: get32(&d.version_incompatible),
            nat_fail_incomplete: get32(&d.incomplete),
            nat_fail_send_error: get32(&d.send_error),
            nat_fail_other: get32(&d.other),
            observed_addr_reports: get32(&d.observed_addr_reports),
            observed_addr_port_changes: get32(&d.observed_addr_port_changes),
            observed_addr_ip_changes: get32(&d.observed_addr_ip_changes),
            transfers_acked: acked,
            avg_transfer_ack_time_ms: (acked > 0).then(|| ack_sum / acked as u64),
            max_transfer_ack_time_ms: (acked > 0).then_some(ack_max),
            inbound_queue_depth_hwm: get32(&self.inbound_queue_depth_hwm),
            inbound_queue_drops: get64(&self.inbound_queue_drops),
            nat_fail_samples: if reset {
                std::mem::take(&mut *d.samples.lock())
            } else {
                d.samples.lock().clone()
            },
            nat_fail_samples_dropped: get32(&d.samples_dropped),
            inbound_stream_refused_invalid_length: get32(
                &self.inbound_stream_outcomes[InboundStreamOutcome::RefusedInvalidLength as usize],
            ),
            inbound_stream_refused_connection_bytes: get32(
                &self.inbound_stream_outcomes
                    [InboundStreamOutcome::RefusedConnectionBytes as usize],
            ),
            inbound_stream_refused_connection_streams: get32(
                &self.inbound_stream_outcomes
                    [InboundStreamOutcome::RefusedConnectionStreams as usize],
            ),
            inbound_stream_closed_too_many_refusals: get32(
                &self.inbound_stream_outcomes[InboundStreamOutcome::ClosedTooManyRefusals as usize],
            ),
            inbound_stream_evicted_idle: get32(
                &self.inbound_stream_outcomes[InboundStreamOutcome::EvictedIdle as usize],
            ),
            inbound_stream_evicted_age: get32(
                &self.inbound_stream_outcomes[InboundStreamOutcome::EvictedAge as usize],
            ),
            nat_attempts_connect_joiner: get32(&attempts[NatAttemptOrigin::ConnectJoiner as usize]),
            nat_attempts_connect_acceptor: get32(
                &attempts[NatAttemptOrigin::ConnectAcceptor as usize],
            ),
            nat_attempts_gateway: get32(&attempts[NatAttemptOrigin::Gateway as usize]),
            nat_attempts_ring_peer_no_transport: get32(
                &attempts[NatAttemptOrigin::RingPeerNoTransport as usize],
            ),
            nat_established_connect_joiner: get32(
                &established[NatAttemptOrigin::ConnectJoiner as usize],
            ),
            nat_established_connect_acceptor: get32(
                &established[NatAttemptOrigin::ConnectAcceptor as usize],
            ),
            nat_established_gateway: get32(&established[NatAttemptOrigin::Gateway as usize]),
            nat_established_ring_peer_no_transport: get32(
                &established[NatAttemptOrigin::RingPeerNoTransport as usize],
            ),
            stray_intro_after_deadline: get32(&stray[StrayIntro::AfterDeadline as usize]),
            stray_intro_same_ip_other_port: get32(&stray[StrayIntro::SameIpOtherPort as usize]),
            stray_intro_other: get32(&stray[StrayIntro::Other as usize]),
            stray_intro_failures_evicted: get32(&self.stray_intro_failures_evicted),
            dial_queue_wait_lt_100ms: get32(&wait[DialQueueWait::Lt100Ms as usize]),
            dial_queue_wait_lt_1s: get32(&wait[DialQueueWait::Lt1S as usize]),
            dial_queue_wait_lt_3s: get32(&wait[DialQueueWait::Lt3S as usize]),
            dial_queue_wait_ge_3s: get32(&wait[DialQueueWait::Ge3S as usize]),
        }
    }

    /// Read-only snapshot for the local dashboard. Does NOT reset counters
    /// (unlike `take_snapshot` which is consumed by the telemetry worker).
    ///
    /// **Telemetry interaction**: `transfers_completed`, `transfers_failed`,
    /// `peak_throughput_bps`, `avg_cwnd_bytes`, `avg_rtt_us`, and
    /// `slowdowns_triggered` are period accumulators that `take_snapshot`
    /// resets every `transport_snapshot_interval_secs` (default 30s).  Between
    /// resets these reflect recent activity; the dashboard sees the current
    /// window, not a lifetime aggregate.
    /// `cumulative_bytes_sent/received` are never reset and reflect totals.
    pub fn read_snapshot(&self) -> TransportSnapshot {
        let transfers_completed = self.transfers_completed.load(Ordering::Relaxed);
        let transfers_failed = self.transfers_failed.load(Ordering::Relaxed);
        let total_transfer_time_ms = self.total_transfer_time_ms.load(Ordering::Relaxed);
        let peak_throughput_bps = self.peak_throughput_bps.load(Ordering::Relaxed);
        let peak_cwnd_bytes = self.peak_cwnd_bytes.load(Ordering::Relaxed);
        let min_cwnd_bytes = self.min_cwnd_bytes.load(Ordering::Relaxed);
        let cwnd_sum = self.cwnd_sum.load(Ordering::Relaxed);
        let cwnd_samples = self.cwnd_samples.load(Ordering::Relaxed);
        let slowdowns_triggered = self.slowdowns_triggered.load(Ordering::Relaxed);
        let nat_traversal_attempts = self.nat_traversal_attempts.load(Ordering::Relaxed);
        let nat_traversal_established = self.nat_traversal_established.load(Ordering::Relaxed);
        let nat_traversal_failed_error = self.nat_traversal_failed_error.load(Ordering::Relaxed);
        let nat_traversal_failed_version =
            self.nat_traversal_failed_version.load(Ordering::Relaxed);
        let min_rtt_us = self.min_rtt_us.load(Ordering::Relaxed);
        let max_rtt_us = self.max_rtt_us.load(Ordering::Relaxed);
        let rtt_sum_us = self.rtt_sum_us.load(Ordering::Relaxed);
        let rtt_samples = self.rtt_samples.load(Ordering::Relaxed);

        let avg_cwnd_bytes = (cwnd_samples > 0).then(|| (cwnd_sum / cwnd_samples as u64) as u32);
        let avg_transfer_time_ms = if transfers_completed > 0 {
            total_transfer_time_ms / transfers_completed as u64
        } else {
            0
        };
        let avg_rtt_us = if rtt_samples > 0 {
            rtt_sum_us / rtt_samples as u64
        } else {
            0
        };
        let extras = self.period_extras(false);
        let conn_rtt = super::rolling_rtt_stats::live_connection_rtt_distribution();

        TransportSnapshot {
            transfers_completed,
            transfers_failed,
            // Delta fields not applicable to non-resetting reads —
            // `take_snapshot` computes these from period accumulators,
            // but a read-only snapshot has no meaningful interval.
            bytes_sent: 0,
            bytes_received: 0,
            avg_transfer_time_ms,
            peak_throughput_bps,
            avg_cwnd_bytes,
            peak_cwnd_bytes,
            min_cwnd_bytes: if min_cwnd_bytes == u32::MAX {
                0
            } else {
                min_cwnd_bytes
            },
            slowdowns_triggered,
            nat_traversal_attempts,
            nat_traversal_established,
            nat_traversal_failed_error,
            nat_traversal_failed_version,
            avg_rtt_us,
            min_rtt_us: if min_rtt_us == u64::MAX {
                0
            } else {
                min_rtt_us
            },
            max_rtt_us,
            ..Default::default()
        }
        .with_extras(conn_rtt.into(), extras)
    }

    /// Snapshot per-peer transfer stats: `(addr, bytes_sent, bytes_received)`.
    pub fn per_peer_snapshot(&self) -> Vec<(SocketAddr, u64, u64)> {
        self.per_peer_stats
            .iter()
            .map(|entry| {
                let addr = *entry.key();
                let sent = entry.value().bytes_sent.load(Ordering::Relaxed);
                let recv = entry.value().bytes_received.load(Ordering::Relaxed);
                (addr, sent, recv)
            })
            .collect()
    }

    /// Take a snapshot and reset all counters.
    ///
    /// Returns `None` if there was no activity during the period.
    pub fn take_snapshot(&self) -> Option<TransportSnapshot> {
        // Read and reset counters
        let transfers_completed = self.transfers_completed.swap(0, Ordering::Relaxed);
        let transfers_failed = self.transfers_failed.swap(0, Ordering::Relaxed);
        // NAT hole-punch counters mirror transfers_*: swapped-to-reset at the
        // top and treated as activity inputs to the snapshot gate below.
        let nat_traversal_attempts = self.nat_traversal_attempts.swap(0, Ordering::Relaxed);
        let nat_traversal_established = self.nat_traversal_established.swap(0, Ordering::Relaxed);
        let nat_traversal_failed_error = self.nat_traversal_failed_error.swap(0, Ordering::Relaxed);
        let nat_traversal_failed_version =
            self.nat_traversal_failed_version.swap(0, Ordering::Relaxed);
        // RTT samples count as activity in their own right: quiet connections
        // that only exchange keep-alive ping/pong traffic record RTT samples
        // but complete no transfers (#4000). Gating snapshot emission solely on
        // transfers would discard those samples, so the telemetry worker would
        // never see RTT data for a quiet node — the exact under-counting this is
        // meant to fix.
        //
        // The activity check `load`s the count (peek, no reset) instead of
        // swapping it, so the count is consumed in exactly one place — grouped
        // with the RTT accumulator swaps below. Resetting the count up here
        // separately would create a window where a concurrent
        // `record_rtt_sample` has its sum/min/max consumed by this snapshot
        // while its count carries into the next, producing a bogus
        // avg/min/max = 0 next tick.
        let rtt_samples_pending = self.rtt_samples.load(Ordering::Relaxed);

        // NAT traversal attempts/outcomes count as activity in their own
        // right: a period whose only event is a NAT hole-punch (e.g. a failed
        // traversal on a bootstrapping node with no other connections, hence no
        // RTT keep-alive samples) must still emit a snapshot — otherwise the NAT
        // visibility counters would be silently dropped, the exact
        // under-counting this feature exists to fix. The counters were already
        // swap-consumed above, so this is a pure gate check on the swapped values.
        let nat_activity = nat_traversal_attempts != 0
            || nat_traversal_established != 0
            || nat_traversal_failed_error != 0
            || nat_traversal_failed_version != 0;

        // The redesign-baseline counters are consumed here, before the gate,
        // like the NAT counters above: any packet sent, loss event, NAT
        // failure detail, acked transfer or queue drop is activity in its own
        // right, and a period with only those must still produce a snapshot.
        let extras = self.period_extras(true);

        // No activity = no snapshot
        if transfers_completed == 0
            && transfers_failed == 0
            && rtt_samples_pending == 0
            && !nat_activity
            && !extras.has_activity()
        {
            // Still reset other counters to prevent stale data. The RTT
            // accumulators (`rtt_samples`, `rtt_sum_us`, `min_rtt_us`,
            // `max_rtt_us`) are deliberately left untouched: they are only ever
            // advanced together by `record_rtt_sample`, so in the genuine
            // no-activity case they already hold their identity values, and
            // leaving them keeps the count and accumulators mutually consistent
            // if a sample lands concurrently with this reset.
            self.bytes_sent.store(0, Ordering::Relaxed);
            self.bytes_received.store(0, Ordering::Relaxed);
            self.total_transfer_time_ms.store(0, Ordering::Relaxed);
            self.peak_throughput_bps.store(0, Ordering::Relaxed);
            self.peak_cwnd_bytes.store(0, Ordering::Relaxed);
            self.min_cwnd_bytes.store(u32::MAX, Ordering::Relaxed);
            self.cwnd_sum.store(0, Ordering::Relaxed);
            self.cwnd_samples.store(0, Ordering::Relaxed);
            self.slowdowns_triggered.store(0, Ordering::Relaxed);
            return None;
        }

        let bytes_sent = self.bytes_sent.swap(0, Ordering::Relaxed);
        let bytes_received = self.bytes_received.swap(0, Ordering::Relaxed);
        let total_transfer_time_ms = self.total_transfer_time_ms.swap(0, Ordering::Relaxed);
        let peak_throughput_bps = self.peak_throughput_bps.swap(0, Ordering::Relaxed);
        let peak_cwnd_bytes = self.peak_cwnd_bytes.swap(0, Ordering::Relaxed);
        let min_cwnd_bytes = self.min_cwnd_bytes.swap(u32::MAX, Ordering::Relaxed);
        let cwnd_sum = self.cwnd_sum.swap(0, Ordering::Relaxed);
        let cwnd_samples = self.cwnd_samples.swap(0, Ordering::Relaxed);
        let slowdowns_triggered = self.slowdowns_triggered.swap(0, Ordering::Relaxed);
        // Swap the RTT accumulators and the sample count together so the count
        // is consumed in lockstep with the sum/min/max it summarizes. Order
        // mirrors `record_rtt_sample` (sum, then count) to minimize the chance
        // of a concurrent sample being split across two snapshots.
        let rtt_sum_us = self.rtt_sum_us.swap(0, Ordering::Relaxed);
        let rtt_samples = self.rtt_samples.swap(0, Ordering::Relaxed);
        let min_rtt_us = self.min_rtt_us.swap(u64::MAX, Ordering::Relaxed);
        let max_rtt_us = self.max_rtt_us.swap(0, Ordering::Relaxed);

        // Compute averages. `None` (JSON null) when no bounded cwnd was
        // sampled, e.g. every transfer ran under FixedRate.
        let avg_cwnd_bytes = (cwnd_samples > 0).then(|| (cwnd_sum / cwnd_samples as u64) as u32);

        let avg_transfer_time_ms = if transfers_completed > 0 {
            total_transfer_time_ms / transfers_completed as u64
        } else {
            0
        };

        let avg_rtt_us = if rtt_samples > 0 {
            rtt_sum_us / rtt_samples as u64
        } else {
            0
        };

        Some(
            TransportSnapshot {
                transfers_completed,
                transfers_failed,
                bytes_sent,
                bytes_received,
                avg_transfer_time_ms,
                peak_throughput_bps,
                avg_cwnd_bytes,
                peak_cwnd_bytes,
                min_cwnd_bytes: if min_cwnd_bytes == u32::MAX {
                    0
                } else {
                    min_cwnd_bytes
                },
                slowdowns_triggered,
                nat_traversal_attempts,
                nat_traversal_established,
                nat_traversal_failed_error,
                nat_traversal_failed_version,
                avg_rtt_us,
                min_rtt_us: if min_rtt_us == u64::MAX {
                    0
                } else {
                    min_rtt_us
                },
                max_rtt_us,
                ..Default::default()
            }
            .with_extras(
                super::rolling_rtt_stats::live_connection_rtt_distribution().into(),
                extras,
            ),
        )
    }
}

/// Periodic transport metrics snapshot.
///
/// Emitted every N seconds to provide aggregate transport layer statistics
/// without flooding the telemetry server with per-transfer events.
///
/// # Sentinel Values
///
/// - `min_cwnd_bytes`: 0 indicates no cwnd samples were recorded
/// - `min_rtt_us`: 0 indicates no RTT samples were recorded
/// - Other fields: 0 is a valid value indicating no activity for that metric
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub struct TransportSnapshot {
    /// Number of stream transfers completed successfully.
    pub transfers_completed: u32,
    /// Number of stream transfers that failed.
    pub transfers_failed: u32,
    /// Total bytes sent during the period.
    pub bytes_sent: u64,
    /// Total bytes received during the period.
    pub bytes_received: u64,
    /// Average transfer time in milliseconds (0 if no transfers).
    pub avg_transfer_time_ms: u64,
    /// Peak throughput observed (bytes per second).
    pub peak_throughput_bps: u64,
    /// Average congestion window size in bytes. `None` (JSON null) if no
    /// bounded cwnd was sampled, e.g. when every transfer ran under the
    /// FixedRate controller, which has no meaningful window.
    ///
    /// Type change: this was a `u32` that read `0` (or, under FixedRate,
    /// a `u32::MAX`-derived sentinel) with no samples. A typed consumer must
    /// now accept null.
    pub avg_cwnd_bytes: Option<u32>,
    /// Peak congestion window size (bytes).
    pub peak_cwnd_bytes: u32,
    /// Minimum congestion window size in bytes (0 if no samples recorded).
    pub min_cwnd_bytes: u32,
    /// Number of LEDBAT slowdowns triggered.
    pub slowdowns_triggered: u32,
    /// Outbound NAT traversal attempts started during the period.
    pub nat_traversal_attempts: u32,
    /// NAT traversal attempts that established a connection during the period.
    pub nat_traversal_established: u32,
    /// NAT traversal attempts that failed for a non-version reason during the
    /// period (unreachable / symmetric-NAT / generic transport error).
    pub nat_traversal_failed_error: u32,
    /// NAT traversal attempts that failed due to a protocol version mismatch
    /// during the period.
    pub nat_traversal_failed_version: u32,
    /// Average RTT in microseconds (0 if no samples).
    pub avg_rtt_us: u64,
    /// Minimum RTT in microseconds (0 if no samples recorded).
    pub min_rtt_us: u64,
    /// Maximum RTT in microseconds.
    pub max_rtt_us: u64,
    // ---- Per-connection RTT distribution across live connections at
    // snapshot time: one value per connection, not per sample. SRTT is the
    // connection's RFC 6298 estimate from data-ACK samples (the sent-packet
    // tracker's SRTT); a connection that has only exchanged keep-alives
    // contributes an SRTT smoothed the same way from its ping/pong samples.
    // Minimum RTT covers both sample sources. `None` (JSON null) when no live
    // connection has a sample.
    /// Live connections with at least one RTT sample.
    pub conn_rtt_connections: u32,
    pub conn_srtt_p50_us: Option<u64>,
    pub conn_srtt_p90_us: Option<u64>,
    pub conn_srtt_max_us: Option<u64>,
    pub conn_min_rtt_p50_us: Option<u64>,
    pub conn_min_rtt_p90_us: Option<u64>,
    pub conn_min_rtt_max_us: Option<u64>,
    // ---- Period counters added to baseline the transport redesign. All
    // are reset by `take_snapshot`.
    /// UDP datagrams handed to the socket (all traffic).
    pub packets_sent: u64,
    /// Retransmissions (RTO resends + TLP probes) handed to the socket on
    /// established connections.
    pub packets_retransmitted: u64,
    /// Packets abandoned after the maximum number of retransmissions.
    pub packets_abandoned: u64,
    /// Per-packet retransmission-timeout expiries.
    pub rto_expiries: u64,
    /// Tail-loss probes fired.
    pub tlp_probes: u64,
    /// Standalone ack-only `NoOp` packets sent, and their on-wire bytes.
    pub standalone_ack_packets: u64,
    pub standalone_ack_bytes: u64,
    /// Failed outbound NAT traversals by cause. Together these sum to
    /// `nat_traversal_failed_error + nat_traversal_failed_version`, but the
    /// split differs: an incompatible version seen in the remote's intro is
    /// `nat_fail_version_incompatible` here and `failed_error` there.
    pub nat_fail_no_response: u32,
    pub nat_fail_undecryptable: u32,
    pub nat_fail_version_incompatible: u32,
    pub nat_fail_incomplete: u32,
    pub nat_fail_send_error: u32,
    pub nat_fail_other: u32,
    /// Reports of our external address received from remotes (each
    /// handshake ack that carries it), and how many differed from the latest
    /// report by a DIFFERENT reporter in the same address family, in port
    /// only (endpoint-dependent NAT mapping signal) or in IP.
    pub observed_addr_reports: u32,
    pub observed_addr_port_changes: u32,
    pub observed_addr_ip_changes: u32,
    /// Outbound stream transfers whose every fragment was acknowledged.
    /// At most 64 streams per connection are timed at once; on a connection
    /// with more concurrent streams the excess is not counted, which biases
    /// the timing toward less loaded connections.
    pub transfers_acked: u32,
    /// Mean / max time from stream start to the final ACK, in ms. Compare
    /// `avg_transfer_time_ms`, which stops at socket hand-off of the last
    /// fragment. `None` if no transfer was fully acknowledged.
    pub avg_transfer_ack_time_ms: Option<u64>,
    pub max_transfer_ack_time_ms: Option<u64>,
    /// High-water mark of any listener-to-connection inbound packet channel.
    pub inbound_queue_depth_hwm: u32,
    /// Inbound packets the listener dropped because that channel was full.
    pub inbound_queue_drops: u64,
    /// Per-attempt records of the FIRST 8 failed outbound NAT traversals in
    /// the period (not a representative sample of a busy period);
    /// `nat_fail_samples_dropped` counts the rest. Read with the same
    /// period's `observed_addr_port_changes` to relate failures to an
    /// endpoint-dependent NAT mapping. Samples and counters are reset
    /// separately, so a failure racing the snapshot can land its counter and
    /// its sample in adjacent periods. Adding this `Vec` changes the bincode
    /// layout of the local event log again (older records are skipped).
    pub nat_fail_samples: Vec<NatFailureSample>,
    pub nat_fail_samples_dropped: u32,
    /// Inbound streams refused or evicted during the period, by reason
    /// (see [`InboundStreamOutcome`]; the field suffix is its label). Counts
    /// only, no addresses. The never-reset twin feeds the OTel metric.
    pub inbound_stream_refused_invalid_length: u32,
    pub inbound_stream_refused_connection_bytes: u32,
    pub inbound_stream_refused_connection_streams: u32,
    pub inbound_stream_closed_too_many_refusals: u32,
    pub inbound_stream_evicted_idle: u32,
    pub inbound_stream_evicted_age: u32,
    /// Outbound NAT traversals started during the period, by
    /// [`NatAttemptOrigin`] (the field suffix is its label). They sum to
    /// `nat_traversal_attempts`, up to a snapshot race.
    pub nat_attempts_connect_joiner: u32,
    pub nat_attempts_connect_acceptor: u32,
    pub nat_attempts_gateway: u32,
    pub nat_attempts_ring_peer_no_transport: u32,
    /// Outbound NAT traversals established during the period, by origin.
    /// They sum to `nat_traversal_established`, up to a snapshot race.
    pub nat_established_connect_joiner: u32,
    pub nat_established_connect_acceptor: u32,
    pub nat_established_gateway: u32,
    pub nat_established_ring_peer_no_transport: u32,
    /// Intro packets a non-gateway dropped during the period because no
    /// connection or traversal exists for their exact address, by
    /// [`StrayIntro`]. Counts only, no addresses.
    pub stray_intro_after_deadline: u32,
    pub stray_intro_same_ip_other_port: u32,
    pub stray_intro_other: u32,
    /// Recent traversal failures the listener forgot before the 10 s window
    /// because its table (256 entries) was full; a later intro from one of
    /// them is counted as another kind. Non-zero means the
    /// `stray_intro_*` split under-counts `after_deadline`.
    pub stray_intro_failures_evicted: u32,
    /// CONNECT joiner acceptor dials during the period by how long the
    /// acceptor waited in the joiner's dial queue, by [`DialQueueWait`].
    pub dial_queue_wait_lt_100ms: u32,
    pub dial_queue_wait_lt_1s: u32,
    pub dial_queue_wait_lt_3s: u32,
    pub dial_queue_wait_ge_3s: u32,
}

/// Per-connection RTT distribution fields of [`TransportSnapshot`]; see the
/// field docs there.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ConnRttSnapshot {
    /// Live connections with at least one RTT sample.
    pub conn_rtt_connections: u32,
    pub conn_srtt_p50_us: Option<u64>,
    pub conn_srtt_p90_us: Option<u64>,
    pub conn_srtt_max_us: Option<u64>,
    pub conn_min_rtt_p50_us: Option<u64>,
    pub conn_min_rtt_p90_us: Option<u64>,
    pub conn_min_rtt_max_us: Option<u64>,
}

impl TransportSnapshot {
    /// Copy the per-connection RTT distribution and the redesign-baseline
    /// period counters into this snapshot.
    fn with_extras(mut self, conn_rtt: ConnRttSnapshot, extras: SnapshotExtras) -> Self {
        let ConnRttSnapshot {
            conn_rtt_connections,
            conn_srtt_p50_us,
            conn_srtt_p90_us,
            conn_srtt_max_us,
            conn_min_rtt_p50_us,
            conn_min_rtt_p90_us,
            conn_min_rtt_max_us,
        } = conn_rtt;
        let SnapshotExtras {
            packets_sent,
            packets_retransmitted,
            packets_abandoned,
            rto_expiries,
            tlp_probes,
            standalone_ack_packets,
            standalone_ack_bytes,
            nat_fail_no_response,
            nat_fail_undecryptable,
            nat_fail_version_incompatible,
            nat_fail_incomplete,
            nat_fail_send_error,
            nat_fail_other,
            observed_addr_reports,
            observed_addr_port_changes,
            observed_addr_ip_changes,
            transfers_acked,
            avg_transfer_ack_time_ms,
            max_transfer_ack_time_ms,
            inbound_queue_depth_hwm,
            inbound_queue_drops,
            nat_fail_samples,
            nat_fail_samples_dropped,
            inbound_stream_refused_invalid_length,
            inbound_stream_refused_connection_bytes,
            inbound_stream_refused_connection_streams,
            inbound_stream_closed_too_many_refusals,
            inbound_stream_evicted_idle,
            inbound_stream_evicted_age,
            nat_attempts_connect_joiner,
            nat_attempts_connect_acceptor,
            nat_attempts_gateway,
            nat_attempts_ring_peer_no_transport,
            nat_established_connect_joiner,
            nat_established_connect_acceptor,
            nat_established_gateway,
            nat_established_ring_peer_no_transport,
            stray_intro_after_deadline,
            stray_intro_same_ip_other_port,
            stray_intro_other,
            stray_intro_failures_evicted,
            dial_queue_wait_lt_100ms,
            dial_queue_wait_lt_1s,
            dial_queue_wait_lt_3s,
            dial_queue_wait_ge_3s,
        } = extras;
        self.conn_rtt_connections = conn_rtt_connections;
        self.conn_srtt_p50_us = conn_srtt_p50_us;
        self.conn_srtt_p90_us = conn_srtt_p90_us;
        self.conn_srtt_max_us = conn_srtt_max_us;
        self.conn_min_rtt_p50_us = conn_min_rtt_p50_us;
        self.conn_min_rtt_p90_us = conn_min_rtt_p90_us;
        self.conn_min_rtt_max_us = conn_min_rtt_max_us;
        self.packets_sent = packets_sent;
        self.packets_retransmitted = packets_retransmitted;
        self.packets_abandoned = packets_abandoned;
        self.rto_expiries = rto_expiries;
        self.tlp_probes = tlp_probes;
        self.standalone_ack_packets = standalone_ack_packets;
        self.standalone_ack_bytes = standalone_ack_bytes;
        self.nat_fail_no_response = nat_fail_no_response;
        self.nat_fail_undecryptable = nat_fail_undecryptable;
        self.nat_fail_version_incompatible = nat_fail_version_incompatible;
        self.nat_fail_incomplete = nat_fail_incomplete;
        self.nat_fail_send_error = nat_fail_send_error;
        self.nat_fail_other = nat_fail_other;
        self.observed_addr_reports = observed_addr_reports;
        self.observed_addr_port_changes = observed_addr_port_changes;
        self.observed_addr_ip_changes = observed_addr_ip_changes;
        self.transfers_acked = transfers_acked;
        self.avg_transfer_ack_time_ms = avg_transfer_ack_time_ms;
        self.max_transfer_ack_time_ms = max_transfer_ack_time_ms;
        self.inbound_queue_depth_hwm = inbound_queue_depth_hwm;
        self.inbound_queue_drops = inbound_queue_drops;
        self.nat_fail_samples = nat_fail_samples;
        self.nat_fail_samples_dropped = nat_fail_samples_dropped;
        self.inbound_stream_refused_invalid_length = inbound_stream_refused_invalid_length;
        self.inbound_stream_refused_connection_bytes = inbound_stream_refused_connection_bytes;
        self.inbound_stream_refused_connection_streams = inbound_stream_refused_connection_streams;
        self.inbound_stream_closed_too_many_refusals = inbound_stream_closed_too_many_refusals;
        self.inbound_stream_evicted_idle = inbound_stream_evicted_idle;
        self.inbound_stream_evicted_age = inbound_stream_evicted_age;
        self.nat_attempts_connect_joiner = nat_attempts_connect_joiner;
        self.nat_attempts_connect_acceptor = nat_attempts_connect_acceptor;
        self.nat_attempts_gateway = nat_attempts_gateway;
        self.nat_attempts_ring_peer_no_transport = nat_attempts_ring_peer_no_transport;
        self.nat_established_connect_joiner = nat_established_connect_joiner;
        self.nat_established_connect_acceptor = nat_established_connect_acceptor;
        self.nat_established_gateway = nat_established_gateway;
        self.nat_established_ring_peer_no_transport = nat_established_ring_peer_no_transport;
        self.stray_intro_after_deadline = stray_intro_after_deadline;
        self.stray_intro_same_ip_other_port = stray_intro_same_ip_other_port;
        self.stray_intro_other = stray_intro_other;
        self.stray_intro_failures_evicted = stray_intro_failures_evicted;
        self.dial_queue_wait_lt_100ms = dial_queue_wait_lt_100ms;
        self.dial_queue_wait_lt_1s = dial_queue_wait_lt_1s;
        self.dial_queue_wait_lt_3s = dial_queue_wait_lt_3s;
        self.dial_queue_wait_ge_3s = dial_queue_wait_ge_3s;
        self
    }
}

impl From<ConnRttDistribution> for ConnRttSnapshot {
    fn from(d: ConnRttDistribution) -> Self {
        Self {
            conn_rtt_connections: d.connections,
            conn_srtt_p50_us: d.srtt_p50_us,
            conn_srtt_p90_us: d.srtt_p90_us,
            conn_srtt_max_us: d.srtt_max_us,
            conn_min_rtt_p50_us: d.min_rtt_p50_us,
            conn_min_rtt_p90_us: d.min_rtt_p90_us,
            conn_min_rtt_max_us: d.min_rtt_max_us,
        }
    }
}

/// Period counters of [`TransportSnapshot`] added to baseline the transport
/// redesign; see the field docs there.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct SnapshotExtras {
    /// UDP datagrams handed to the socket (all traffic).
    pub packets_sent: u64,
    /// Retransmissions (RTO resends + TLP probes) handed to the socket on
    /// established connections.
    pub packets_retransmitted: u64,
    /// Packets abandoned after the maximum number of retransmissions.
    pub packets_abandoned: u64,
    /// Per-packet retransmission-timeout expiries.
    pub rto_expiries: u64,
    /// Tail-loss probes fired.
    pub tlp_probes: u64,
    /// Standalone ack-only `NoOp` packets sent, and their on-wire bytes.
    pub standalone_ack_packets: u64,
    pub standalone_ack_bytes: u64,
    /// Failed outbound NAT traversals by cause. Together these sum to
    /// `nat_traversal_failed_error + nat_traversal_failed_version`, but the
    /// split differs: an incompatible version seen in the remote's intro is
    /// `nat_fail_version_incompatible` here and `failed_error` there.
    pub nat_fail_no_response: u32,
    pub nat_fail_undecryptable: u32,
    pub nat_fail_version_incompatible: u32,
    pub nat_fail_incomplete: u32,
    pub nat_fail_send_error: u32,
    pub nat_fail_other: u32,
    /// Reports of our external address received from remotes (each
    /// handshake ack that carries it), and how many differed from the latest
    /// report by a DIFFERENT reporter in the same address family, in port
    /// only (endpoint-dependent NAT mapping signal) or in IP.
    pub observed_addr_reports: u32,
    pub observed_addr_port_changes: u32,
    pub observed_addr_ip_changes: u32,
    /// Outbound stream transfers whose every fragment was acknowledged.
    /// At most 64 streams per connection are timed at once; on a connection
    /// with more concurrent streams the excess is not counted, which biases
    /// the timing toward less loaded connections.
    pub transfers_acked: u32,
    /// Mean / max time from stream start to the final ACK, in ms. Compare
    /// `avg_transfer_time_ms`, which stops at socket hand-off of the last
    /// fragment. `None` if no transfer was fully acknowledged.
    pub avg_transfer_ack_time_ms: Option<u64>,
    pub max_transfer_ack_time_ms: Option<u64>,
    /// High-water mark of any listener-to-connection inbound packet channel.
    pub inbound_queue_depth_hwm: u32,
    /// Inbound packets the listener dropped because that channel was full.
    pub inbound_queue_drops: u64,
    /// Per-attempt records of the FIRST 8 failed outbound NAT traversals in
    /// the period (not a representative sample of a busy period);
    /// `nat_fail_samples_dropped` counts the rest. Read with the same
    /// period's `observed_addr_port_changes` to relate failures to an
    /// endpoint-dependent NAT mapping. Samples and counters are reset
    /// separately, so a failure racing the snapshot can land its counter and
    /// its sample in adjacent periods. Adding this `Vec` changes the bincode
    /// layout of the local event log again (older records are skipped).
    pub nat_fail_samples: Vec<NatFailureSample>,
    pub nat_fail_samples_dropped: u32,
    /// Inbound streams refused or evicted during the period, by reason
    /// (see [`InboundStreamOutcome`]; the field suffix is its label). Counts
    /// only, no addresses. The never-reset twin feeds the OTel metric.
    pub inbound_stream_refused_invalid_length: u32,
    pub inbound_stream_refused_connection_bytes: u32,
    pub inbound_stream_refused_connection_streams: u32,
    pub inbound_stream_closed_too_many_refusals: u32,
    pub inbound_stream_evicted_idle: u32,
    pub inbound_stream_evicted_age: u32,
    /// Outbound NAT traversals started during the period, by
    /// [`NatAttemptOrigin`] (the field suffix is its label). They sum to
    /// `nat_traversal_attempts`, up to a snapshot race.
    pub nat_attempts_connect_joiner: u32,
    pub nat_attempts_connect_acceptor: u32,
    pub nat_attempts_gateway: u32,
    pub nat_attempts_ring_peer_no_transport: u32,
    /// Outbound NAT traversals established during the period, by origin.
    /// They sum to `nat_traversal_established`, up to a snapshot race.
    pub nat_established_connect_joiner: u32,
    pub nat_established_connect_acceptor: u32,
    pub nat_established_gateway: u32,
    pub nat_established_ring_peer_no_transport: u32,
    /// Intro packets a non-gateway dropped during the period because no
    /// connection or traversal exists for their exact address, by
    /// [`StrayIntro`]. Counts only, no addresses.
    pub stray_intro_after_deadline: u32,
    pub stray_intro_same_ip_other_port: u32,
    pub stray_intro_other: u32,
    /// Recent traversal failures the listener forgot before the 10 s window
    /// because its table (256 entries) was full; a later intro from one of
    /// them is counted as another kind. Non-zero means the
    /// `stray_intro_*` split under-counts `after_deadline`.
    pub stray_intro_failures_evicted: u32,
    /// CONNECT joiner acceptor dials during the period by how long the
    /// acceptor waited in the joiner's dial queue, by [`DialQueueWait`].
    pub dial_queue_wait_lt_100ms: u32,
    pub dial_queue_wait_lt_1s: u32,
    pub dial_queue_wait_lt_3s: u32,
    pub dial_queue_wait_ge_3s: u32,
}

impl SnapshotExtras {
    fn has_activity(&self) -> bool {
        // `inbound_queue_depth_hwm` alone is not activity: any delivered
        // packet sets it, and `packets_sent` already covers a live node.
        self.packets_sent != 0
            || self.packets_retransmitted != 0
            || self.packets_abandoned != 0
            || self.rto_expiries != 0
            || self.tlp_probes != 0
            || self.standalone_ack_packets != 0
            || self.nat_fail_no_response != 0
            || self.nat_fail_undecryptable != 0
            || self.nat_fail_version_incompatible != 0
            || self.nat_fail_incomplete != 0
            || self.nat_fail_send_error != 0
            || self.nat_fail_other != 0
            || self.observed_addr_reports != 0
            || self.transfers_acked != 0
            || self.inbound_queue_drops != 0
            || !self.nat_fail_samples.is_empty()
            || self.inbound_stream_refused_invalid_length != 0
            || self.inbound_stream_refused_connection_bytes != 0
            || self.inbound_stream_refused_connection_streams != 0
            || self.inbound_stream_closed_too_many_refusals != 0
            || self.inbound_stream_evicted_idle != 0
            || self.inbound_stream_evicted_age != 0
            || self.nat_attempts_connect_joiner != 0
            || self.nat_attempts_connect_acceptor != 0
            || self.nat_attempts_gateway != 0
            || self.nat_attempts_ring_peer_no_transport != 0
            || self.nat_established_connect_joiner != 0
            || self.nat_established_connect_acceptor != 0
            || self.nat_established_gateway != 0
            || self.nat_established_ring_peer_no_transport != 0
            || self.stray_intro_after_deadline != 0
            || self.stray_intro_same_ip_other_port != 0
            || self.stray_intro_other != 0
            || self.stray_intro_failures_evicted != 0
            || self.dial_queue_wait_lt_100ms != 0
            || self.dial_queue_wait_lt_1s != 0
            || self.dial_queue_wait_lt_3s != 0
            || self.dial_queue_wait_ge_3s != 0
    }
}

/// Channel capacity for transfer events.
/// Using a bounded channel to prevent memory issues if telemetry is slow.
const TRANSFER_EVENT_CHANNEL_CAPACITY: usize = 1000;

/// Global sender for transfer events.
/// This is set once during telemetry initialization.
static TRANSFER_EVENT_SENDER: LazyLock<parking_lot::RwLock<Option<mpsc::Sender<TransferEvent>>>> =
    LazyLock::new(|| parking_lot::RwLock::new(None));

/// Initialize the transfer event channel.
/// Returns a receiver that the TelemetryWorker should poll.
/// Should be called once during telemetry initialization.
pub fn init_transfer_event_channel() -> mpsc::Receiver<TransferEvent> {
    let (tx, rx) = mpsc::channel(TRANSFER_EVENT_CHANNEL_CAPACITY);
    *TRANSFER_EVENT_SENDER.write() = Some(tx);
    rx
}

/// Emit a transfer started event.
/// Returns immediately if telemetry is not enabled (sender not initialized).
pub fn emit_transfer_started(
    stream_id: u64,
    peer_addr: SocketAddr,
    expected_bytes: u64,
    direction: TransferDirection,
) {
    let sender_guard = TRANSFER_EVENT_SENDER.read();
    if let Some(sender) = sender_guard.as_ref() {
        let event = TransferEvent::Started {
            stream_id,
            peer_addr,
            expected_bytes,
            direction,
            tx_id: None, // Transaction ID not available at transport layer
            timestamp: crate::tracing::telemetry::current_timestamp_ms(),
        };
        // Fire-and-forget: try_send avoids blocking the transport layer; channel full means telemetry drops
        #[allow(clippy::let_underscore_must_use)]
        let _ = sender.try_send(event);
    }
}

/// Emit a transfer completed event.
/// Returns immediately if telemetry is not enabled (sender not initialized).
#[allow(clippy::too_many_arguments)]
pub fn emit_transfer_completed(
    stream_id: u64,
    peer_addr: SocketAddr,
    bytes_transferred: u64,
    elapsed_ms: u64,
    avg_throughput_bps: u64,
    peak_cwnd_bytes: Option<u32>,
    final_cwnd_bytes: Option<u32>,
    slowdowns_triggered: Option<u32>,
    final_srtt_ms: Option<u32>,
    final_ssthresh_bytes: Option<u32>,
    min_ssthresh_floor_bytes: Option<u32>,
    total_timeouts: Option<u32>,
    min_rtt_ms: Option<u32>,
    peak_flightsize_bytes: Option<u32>,
    direction: TransferDirection,
) {
    let sender_guard = TRANSFER_EVENT_SENDER.read();
    if let Some(sender) = sender_guard.as_ref() {
        let event = TransferEvent::Completed {
            stream_id,
            peer_addr,
            bytes_transferred,
            elapsed_ms,
            avg_throughput_bps,
            peak_cwnd_bytes,
            final_cwnd_bytes,
            slowdowns_triggered,
            final_srtt_ms,
            final_ssthresh_bytes,
            min_ssthresh_floor_bytes,
            total_timeouts,
            min_rtt_ms,
            peak_flightsize_bytes,
            direction,
            timestamp: crate::tracing::telemetry::current_timestamp_ms(),
        };
        // Fire-and-forget: try_send avoids blocking the transport layer; channel full means telemetry drops
        #[allow(clippy::let_underscore_must_use)]
        let _ = sender.try_send(event);
    }
}

/// Emit a transfer failed event.
/// Returns immediately if telemetry is not enabled (sender not initialized).
pub fn emit_transfer_failed(
    stream_id: u64,
    peer_addr: SocketAddr,
    bytes_transferred: u64,
    reason: String,
    elapsed_ms: u64,
    direction: TransferDirection,
) {
    let sender_guard = TRANSFER_EVENT_SENDER.read();
    if let Some(sender) = sender_guard.as_ref() {
        let event = TransferEvent::Failed {
            stream_id,
            peer_addr,
            bytes_transferred,
            reason,
            elapsed_ms,
            direction,
            timestamp: crate::tracing::telemetry::current_timestamp_ms(),
        };
        // Fire-and-forget: try_send avoids blocking the transport layer; channel full means telemetry drops
        #[allow(clippy::let_underscore_must_use)]
        let _ = sender.try_send(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// A remote for unit tests that record per-remote counters.
    const TEST_REMOTE: SocketAddr = SocketAddr::V4(std::net::SocketAddrV4::new(
        std::net::Ipv4Addr::new(192, 0, 2, 99),
        9,
    ));

    /// `cumulative_inbound_stream_outcomes` indexes its counters by
    /// `outcome as usize`, which is only sound while `ALL` lists the
    /// outcomes in declaration order, and labels are distinct.
    #[test]
    fn inbound_stream_outcomes_are_listed_in_declaration_order() {
        for (i, outcome) in InboundStreamOutcome::ALL.iter().enumerate() {
            assert_eq!(*outcome as usize, i, "{outcome:?}");
        }
        let mut labels: Vec<_> = InboundStreamOutcome::ALL
            .iter()
            .map(|o| o.label())
            .collect();
        labels.sort_unstable();
        labels.dedup();
        assert_eq!(labels.len(), InboundStreamOutcome::COUNT);
    }

    #[test]
    fn test_empty_snapshot_returns_none() {
        let metrics = TransportMetrics::new();
        assert!(metrics.take_snapshot().is_none());
    }

    #[test]
    fn test_snapshot_after_transfer() {
        let metrics = TransportMetrics::new();

        let stats = crate::transport::TransferStats {
            stream_id: 1,
            remote_addr: "127.0.0.1:8080".parse().unwrap(),
            bytes_transferred: 1024,
            elapsed: Duration::from_millis(100),
            peak_cwnd_bytes: Some(50000),
            final_cwnd_bytes: Some(40000),
            slowdowns_triggered: 1,
            base_delay: Duration::from_millis(10),
            final_ssthresh_bytes: 100000,
            min_ssthresh_floor_bytes: 5696,
            total_timeouts: 0,
            final_flightsize: 0,
            configured_rate: 0,
        };

        metrics.record_transfer_completed(&stats);

        let snapshot = metrics.take_snapshot().expect("should have snapshot");
        assert_eq!(snapshot.transfers_completed, 1);
        assert_eq!(snapshot.bytes_sent, 1024);
        assert_eq!(snapshot.peak_cwnd_bytes, 50000);
        assert_eq!(snapshot.slowdowns_triggered, 1);
    }

    /// Regression test for #4000: a quiet connection that records RTT samples
    /// from the ping/pong keep-alive cycle but completes no transfers must
    /// still produce a telemetry snapshot. Without counting RTT samples as
    /// activity, `take_snapshot` would early-return `None` and reset the RTT
    /// counters, silently discarding exactly the quiet-connection RTT data the
    /// keep-alive sampling path is meant to surface.
    #[test]
    fn test_snapshot_emitted_for_rtt_only_activity() {
        let metrics = TransportMetrics::new();

        // No transfers — only a keep-alive RTT sample, as a quiet connection
        // would produce.
        metrics.record_rtt_sample(42_000);

        let snapshot = metrics
            .take_snapshot()
            .expect("RTT-only activity must still produce a snapshot");
        assert_eq!(snapshot.transfers_completed, 0);
        assert_eq!(snapshot.transfers_failed, 0);
        assert_eq!(snapshot.avg_rtt_us, 42_000);
        assert_eq!(snapshot.min_rtt_us, 42_000);
        assert_eq!(snapshot.max_rtt_us, 42_000);

        // Counters reset: a subsequent snapshot with no new activity is None.
        assert!(metrics.take_snapshot().is_none());
    }

    /// NAT hole-punch counters are period accumulators that also count as
    /// snapshot activity: a period whose only event is a NAT traversal (e.g. a
    /// failed hole-punch on a bootstrapping node with no transfers and no RTT
    /// keep-alive samples) must still emit a snapshot, otherwise the NAT
    /// visibility counters would be silently dropped by the no-activity gate.
    #[test]
    fn test_snapshot_emitted_for_nat_only_activity() {
        let metrics = TransportMetrics::new();

        metrics.record_nat_traversal_attempt(TEST_REMOTE, NatAttemptOrigin::Gateway);
        metrics.record_nat_traversal_established(TEST_REMOTE, NatAttemptOrigin::Gateway);
        metrics.record_nat_traversal_failed_error();
        metrics.record_nat_traversal_failed_version();

        let snapshot = metrics
            .take_snapshot()
            .expect("NAT-only activity must still produce a snapshot");
        assert_eq!(snapshot.transfers_completed, 0);
        assert_eq!(snapshot.transfers_failed, 0);
        assert_eq!(snapshot.nat_traversal_attempts, 1);
        assert_eq!(snapshot.nat_traversal_established, 1);
        assert_eq!(snapshot.nat_traversal_failed_error, 1);
        assert_eq!(snapshot.nat_traversal_failed_version, 1);

        // Counters reset after the snapshot: a subsequent snapshot with no new
        // activity is None.
        assert!(metrics.take_snapshot().is_none());
    }

    /// Failed transfers count as snapshot activity in their own right: a period
    /// whose ONLY activity is failures (a sick node whose every transfer dies,
    /// with no completions and no RTT keep-alive samples) must still emit a
    /// snapshot, otherwise `take_snapshot` returns `None`, the telemetry worker
    /// uploads nothing, and the fail counter reads a permanent 0 — exactly the
    /// #4827 symptom this fix exists to surface.
    ///
    /// The `&& transfers_failed == 0` conjunct in the no-activity gate was DEAD
    /// until #4827 gave the counter a writer, so this is its first coverage.
    /// Sibling pins: `test_snapshot_emitted_for_rtt_only_activity` (#4000) and
    /// `test_snapshot_emitted_for_nat_only_activity`.
    ///
    /// Also the only coverage that a failure reaches `take_snapshot` at all —
    /// the value telemetry actually uploads, distinct from the `read_snapshot`
    /// the `peer_connection` wiring tests read.
    #[test]
    fn test_snapshot_emitted_for_failed_transfers_only() {
        let metrics = TransportMetrics::new();

        // No completions, no RTT samples, no NAT activity — only failures.
        metrics.record_transfer_failed();
        metrics.record_transfer_failed();
        metrics.record_transfer_failed();

        let snapshot = metrics
            .take_snapshot()
            .expect("failure-only activity must still produce a snapshot");
        assert_eq!(snapshot.transfers_failed, 3);
        assert_eq!(snapshot.transfers_completed, 0);
        // No completion means no measured duration: the average is the sentinel
        // 0, not a real timing. `cards.rs` must not render it as one.
        assert_eq!(snapshot.avg_transfer_time_ms, 0);

        // Counters reset after the snapshot: a subsequent snapshot with no new
        // activity is None. Without the reset a single failure would re-emit a
        // stale snapshot every period forever.
        assert!(metrics.take_snapshot().is_none());
    }

    /// `read_snapshot` (non-resetting dashboard read) reflects the NAT counters
    /// without consuming them, so a following `take_snapshot` still sees them.
    #[test]
    fn read_snapshot_reflects_nat_counters() {
        let metrics = TransportMetrics::new();
        metrics.record_nat_traversal_attempt(TEST_REMOTE, NatAttemptOrigin::Gateway);
        metrics.record_nat_traversal_attempt(TEST_REMOTE, NatAttemptOrigin::Gateway);
        metrics.record_nat_traversal_failed_version();

        let snap = metrics.read_snapshot();
        assert_eq!(snap.nat_traversal_attempts, 2);
        assert_eq!(snap.nat_traversal_failed_version, 1);
        assert_eq!(snap.nat_traversal_established, 0);
        assert_eq!(snap.nat_traversal_failed_error, 0);

        // read_snapshot must NOT reset — take_snapshot still sees the counts.
        let taken = metrics
            .take_snapshot()
            .expect("counts still present after a non-resetting read");
        assert_eq!(taken.nat_traversal_attempts, 2);
    }

    #[test]
    fn test_snapshot_resets_counters() {
        let metrics = TransportMetrics::new();

        let stats = crate::transport::TransferStats {
            stream_id: 1,
            remote_addr: "127.0.0.1:8080".parse().unwrap(),
            bytes_transferred: 1024,
            elapsed: Duration::from_millis(100),
            peak_cwnd_bytes: Some(50000),
            final_cwnd_bytes: Some(40000),
            slowdowns_triggered: 1,
            base_delay: Duration::from_millis(10),
            final_ssthresh_bytes: 100000,
            min_ssthresh_floor_bytes: 5696,
            total_timeouts: 0,
            final_flightsize: 0,
            configured_rate: 0,
        };

        metrics.record_transfer_completed(&stats);
        let _ = metrics.take_snapshot();

        // Second snapshot should be None (no new activity)
        assert!(metrics.take_snapshot().is_none());
    }

    #[test]
    fn test_multiple_transfers_aggregate() {
        let metrics = TransportMetrics::new();

        for i in 0..5 {
            let stats = crate::transport::TransferStats {
                stream_id: i,
                remote_addr: "127.0.0.1:8080".parse().unwrap(),
                bytes_transferred: 1000,
                elapsed: Duration::from_millis(100),
                peak_cwnd_bytes: Some(40000 + (i as u32 * 1000)),
                final_cwnd_bytes: Some(35000),
                slowdowns_triggered: 1,
                base_delay: Duration::from_millis(10),
                final_ssthresh_bytes: 100000,
                min_ssthresh_floor_bytes: 5696,
                total_timeouts: 0,
                final_flightsize: 0,
                configured_rate: 0,
            };
            metrics.record_transfer_completed(&stats);
        }

        let snapshot = metrics.take_snapshot().expect("should have snapshot");
        assert_eq!(snapshot.transfers_completed, 5);
        assert_eq!(snapshot.bytes_sent, 5000);
        assert_eq!(snapshot.peak_cwnd_bytes, 44000); // Max of all peaks
        assert_eq!(snapshot.slowdowns_triggered, 5);
    }

    #[test]
    fn test_inbound_completed_updates_only_snapshot_bytes() {
        // record_inbound_completed must NOT touch the cumulative or per-peer
        // wire-byte counters — those are owned by record_packet_received and
        // updated at the socket layer. This test pins that contract so the
        // double-counting bug (counted both at socket layer and at stream
        // completion) cannot regress.
        let metrics = TransportMetrics::new();

        metrics.record_inbound_completed(2048);
        metrics.record_inbound_completed(1024);

        assert_eq!(
            metrics.cumulative_bytes_received(),
            0,
            "cumulative is wire-byte only; stream completion must not touch it"
        );
        assert!(
            metrics.per_peer_snapshot().is_empty(),
            "per-peer is wire-byte only; stream completion must not touch it"
        );

        // The snapshot bytes_received field still aggregates transfer payloads
        // alongside transfers_completed for telemetry.
        let stats = crate::transport::TransferStats {
            stream_id: 1,
            remote_addr: "10.0.0.1:5000".parse().unwrap(),
            bytes_transferred: 100,
            elapsed: Duration::from_millis(10),
            peak_cwnd_bytes: Some(1000),
            final_cwnd_bytes: Some(1000),
            slowdowns_triggered: 0,
            base_delay: Duration::from_millis(5),
            final_ssthresh_bytes: 100000,
            min_ssthresh_floor_bytes: 5696,
            total_timeouts: 0,
            final_flightsize: 0,
            configured_rate: 0,
        };
        metrics.record_transfer_completed(&stats);
        let snapshot = metrics.take_snapshot().unwrap();
        assert_eq!(snapshot.bytes_received, 3072);
    }

    /// The cumulative outcome counters must survive `take_snapshot`.
    ///
    /// They exist only to be read as OTel observable COUNTERS, which must be
    /// monotonic. Their period twins are `swap`ped to zero here for the legacy
    /// telemetry worker, so observing those instead would make every metric
    /// sawtooth the moment `telemetry-enabled` is also on — silently, and only
    /// on nodes running both pipelines.
    #[test]
    fn cumulative_outcome_counters_survive_a_snapshot_reset() {
        let metrics = TransportMetrics::new();
        metrics.record_transfer_failed();
        metrics.record_nat_traversal_attempt(TEST_REMOTE, NatAttemptOrigin::Gateway);
        metrics.record_nat_traversal_established(TEST_REMOTE, NatAttemptOrigin::Gateway);
        metrics.record_nat_traversal_failed_error();
        metrics.record_nat_traversal_failed_version();

        assert_eq!(metrics.cumulative_transfers(), (0, 1));
        assert_eq!(metrics.cumulative_nat_traversal(), (1, 1, 1, 1));

        let snapshot = metrics.take_snapshot().expect("activity was recorded");
        assert_eq!(snapshot.transfers_failed, 1, "period counter was consumed");

        assert_eq!(
            metrics.cumulative_transfers(),
            (0, 1),
            "a snapshot must not reset the cumulative counters"
        );
        assert_eq!(metrics.cumulative_nat_traversal(), (1, 1, 1, 1));

        // ...and they keep climbing across the reset boundary.
        metrics.record_transfer_failed();
        metrics.record_nat_traversal_attempt(TEST_REMOTE, NatAttemptOrigin::Gateway);
        assert_eq!(metrics.cumulative_transfers(), (0, 2));
        assert_eq!(metrics.cumulative_nat_traversal(), (2, 1, 1, 1));
    }

    #[test]
    fn test_packet_sent_updates_cumulative_and_per_peer() {
        let metrics = TransportMetrics::new();
        let addr: std::net::SocketAddr = "10.0.0.1:5000".parse().unwrap();

        // Simulate small control packets that never trigger streaming —
        // historically these were invisible to the dashboard. After the fix
        // they must show up.
        metrics.record_packet_sent(addr, 64);
        metrics.record_packet_sent(addr, 200);

        assert_eq!(metrics.cumulative_bytes_sent(), 264);

        let peers = metrics.per_peer_snapshot();
        let peer = peers.iter().find(|(a, _, _)| *a == addr).expect("tracked");
        assert_eq!(peer.1, 264, "per-peer sent must reflect packet bytes");
        assert_eq!(peer.2, 0);
    }

    #[test]
    fn test_packet_received_updates_cumulative_and_per_peer() {
        let metrics = TransportMetrics::new();
        let addr: std::net::SocketAddr = "10.0.0.1:5000".parse().unwrap();

        metrics.record_packet_received(addr, 128);
        metrics.record_packet_received(addr, 256);

        assert_eq!(metrics.cumulative_bytes_received(), 384);

        let peers = metrics.per_peer_snapshot();
        let peer = peers.iter().find(|(a, _, _)| *a == addr).expect("tracked");
        assert_eq!(peer.1, 0);
        assert_eq!(peer.2, 384);
    }

    #[test]
    fn test_packet_metrics_survive_snapshot_reset() {
        let metrics = TransportMetrics::new();
        let addr: std::net::SocketAddr = "10.0.0.1:5000".parse().unwrap();

        metrics.record_packet_sent(addr, 500);
        metrics.record_packet_received(addr, 700);

        // Force a snapshot via a transfer completion.
        let stats = crate::transport::TransferStats {
            stream_id: 1,
            remote_addr: addr,
            bytes_transferred: 100,
            elapsed: Duration::from_millis(10),
            peak_cwnd_bytes: Some(1000),
            final_cwnd_bytes: Some(1000),
            slowdowns_triggered: 0,
            base_delay: Duration::from_millis(5),
            final_ssthresh_bytes: 100000,
            min_ssthresh_floor_bytes: 5696,
            total_timeouts: 0,
            final_flightsize: 0,
            configured_rate: 0,
        };
        metrics.record_transfer_completed(&stats);
        let _ = metrics.take_snapshot();

        // Cumulative + per-peer wire bytes survive the snapshot reset.
        assert_eq!(metrics.cumulative_bytes_sent(), 500);
        assert_eq!(metrics.cumulative_bytes_received(), 700);
        let peers = metrics.per_peer_snapshot();
        let peer = peers.iter().find(|(a, _, _)| *a == addr).expect("tracked");
        assert_eq!(peer.1, 500);
        assert_eq!(peer.2, 700);
    }

    #[test]
    fn test_per_peer_capacity_bound() {
        let metrics = TransportMetrics::new();

        // Fill to capacity
        for i in 0..MAX_TRACKED_PEERS {
            let addr: std::net::SocketAddr = format!("10.0.{}.{}:{}", i / 256, i % 256, 5000 + i)
                .parse()
                .unwrap();
            metrics.record_packet_received(addr, 100);
        }

        assert_eq!(metrics.per_peer_snapshot().len(), MAX_TRACKED_PEERS);

        // Beyond capacity: cumulative still counts AND the new peer is now
        // tracked because LRU evicts the oldest entry — that's the #3999
        // behavior change. Pre-fix the new peer would have been silently
        // dropped from the per-peer view.
        let extra: std::net::SocketAddr = "192.168.1.1:9999".parse().unwrap();
        metrics.record_packet_received(extra, 500);

        assert_eq!(metrics.per_peer_snapshot().len(), MAX_TRACKED_PEERS);
        assert!(
            metrics
                .per_peer_snapshot()
                .iter()
                .any(|(a, _, _)| *a == extra),
            "new peer must be tracked after LRU evicts the oldest entry"
        );

        let total: u64 = (MAX_TRACKED_PEERS as u64 * 100) + 500;
        assert_eq!(metrics.cumulative_bytes_received(), total);
    }

    #[test]
    fn test_per_peer_lru_evicts_oldest() {
        // Fill the table, then record activity on the second-oldest entry to
        // refresh its tick. The first-inserted entry must be the one evicted
        // when a brand-new peer is recorded. This pins the LRU ordering
        // (least-recently-updated, not least-recently-inserted).
        let metrics = TransportMetrics::new();

        let oldest: std::net::SocketAddr = "10.0.0.1:1000".parse().unwrap();
        metrics.record_packet_received(oldest, 1);

        let next_oldest: std::net::SocketAddr = "10.0.0.2:1000".parse().unwrap();
        metrics.record_packet_received(next_oldest, 1);

        // Fill the rest of the table.
        for i in 2..MAX_TRACKED_PEERS {
            let addr: std::net::SocketAddr = format!("10.0.{}.{}:{}", i / 256, i % 256, 5000 + i)
                .parse()
                .unwrap();
            metrics.record_packet_received(addr, 1);
        }
        assert_eq!(metrics.per_peer_snapshot().len(), MAX_TRACKED_PEERS);

        // Refresh `next_oldest` so it is no longer the LRU candidate.
        metrics.record_packet_received(next_oldest, 1);

        // Insert a new peer. LRU eviction should remove `oldest`.
        let newcomer: std::net::SocketAddr = "192.168.7.7:9999".parse().unwrap();
        metrics.record_packet_received(newcomer, 1);

        let peers = metrics.per_peer_snapshot();
        assert_eq!(peers.len(), MAX_TRACKED_PEERS);
        assert!(
            peers.iter().all(|(a, _, _)| *a != oldest),
            "the least-recently-updated entry must be evicted"
        );
        assert!(
            peers.iter().any(|(a, _, _)| *a == next_oldest),
            "the recently-refreshed entry must survive eviction"
        );
        assert!(
            peers.iter().any(|(a, _, _)| *a == newcomer),
            "the new entry must be tracked after eviction"
        );
    }

    #[test]
    fn test_remove_peer_frees_slot() {
        // Disconnecting a peer must immediately free its per-peer slot so
        // the bounded table doesn't accumulate stale entries on long-running
        // gateways.
        let metrics = TransportMetrics::new();

        let addr: std::net::SocketAddr = "10.0.0.5:5000".parse().unwrap();
        metrics.record_packet_received(addr, 100);
        assert_eq!(metrics.per_peer_snapshot().len(), 1);

        metrics.remove_peer(addr);
        assert!(
            metrics.per_peer_snapshot().is_empty(),
            "remove_peer must drop the entry"
        );

        // Cumulative counters are wire-byte history and intentionally do
        // NOT roll back when a peer entry is removed.
        assert_eq!(metrics.cumulative_bytes_received(), 100);
    }

    #[test]
    fn test_remove_peer_is_idempotent() {
        // Removing a non-existent peer is a no-op (e.g. when
        // record_peer_disconnected fires for a peer whose stats slot was
        // already evicted by LRU).
        let metrics = TransportMetrics::new();
        let addr: std::net::SocketAddr = "10.0.0.99:9999".parse().unwrap();
        metrics.remove_peer(addr); // must not panic / corrupt state
        assert!(metrics.per_peer_snapshot().is_empty());

        metrics.record_packet_received(addr, 50);
        assert_eq!(metrics.per_peer_snapshot().len(), 1);
        metrics.remove_peer(addr);
        metrics.remove_peer(addr); // second remove must be a no-op
        assert!(metrics.per_peer_snapshot().is_empty());
    }

    /// Sentinel values (`u32::MAX` / `u64::MAX`) must be mapped to 0 in the
    /// read-snapshot — the dashboard displays them as "0ms" / "0 B", not
    /// garbage. No samples are ever recorded, so the sentinels survive.
    #[test]
    fn read_snapshot_sentinels_map_to_zero() {
        let metrics = TransportMetrics::new();
        let snap = metrics.read_snapshot();
        assert_eq!(snap.min_cwnd_bytes, 0, "no cwnd samples → min should be 0");
        assert_eq!(snap.min_rtt_us, 0, "no RTT samples → min should be 0");
    }

    /// Happy-path: record known cwnd samples, verify read_snapshot
    /// returns correct average.
    #[test]
    fn read_snapshot_happy_path() {
        let metrics = TransportMetrics::new();
        metrics.record_cwnd_sample(4000);
        metrics.record_cwnd_sample(2000);
        metrics.record_cwnd_sample(6000);
        let snap = metrics.read_snapshot();
        assert_eq!(snap.avg_cwnd_bytes, Some(4000));
    }

    /// The loss / retransmit / standalone-ACK counters reach the snapshot
    /// that telemetry uploads, count as activity on their own, and reset.
    #[test]
    fn loss_counters_reach_snapshot_and_reset() {
        let metrics = TransportMetrics::new();
        metrics.record_rto_expiry();
        metrics.record_rto_expiry();
        metrics.record_tlp_probe();
        metrics.record_retransmission_sent();
        metrics.record_retransmission_sent();
        metrics.record_retransmission_sent();
        metrics.record_packet_abandoned();
        metrics.record_standalone_ack(60);
        metrics.record_standalone_ack(40);

        let snap = metrics
            .take_snapshot()
            .expect("loss events alone must produce a snapshot");
        assert_eq!(snap.rto_expiries, 2);
        assert_eq!(snap.tlp_probes, 1);
        assert_eq!(snap.packets_retransmitted, 3);
        assert_eq!(snap.packets_abandoned, 1);
        assert_eq!(snap.standalone_ack_packets, 2);
        assert_eq!(snap.standalone_ack_bytes, 100);
        assert!(metrics.take_snapshot().is_none(), "counters must reset");
    }

    /// `packets_sent` is metered by `record_packet_sent`, alongside the
    /// cumulative counter, and is a period counter.
    #[test]
    fn packets_sent_is_a_period_counter() {
        let metrics = TransportMetrics::new();
        let addr: SocketAddr = "10.0.0.1:5000".parse().unwrap();
        metrics.record_packet_sent(addr, 10);
        metrics.record_packet_sent(addr, 10);
        let snap = metrics.take_snapshot().expect("sends are activity");
        assert_eq!(snap.packets_sent, 2);
        metrics.record_packet_sent(addr, 10);
        assert_eq!(metrics.take_snapshot().expect("activity").packets_sent, 1);
        assert_eq!(metrics.cumulative_packets().0, 3, "cumulative is not reset");
    }

    /// A FixedRate transfer has no bounded cwnd. It must not feed the cwnd
    /// accumulators, and `avg_cwnd_bytes` must then be null, not a sentinel.
    #[test]
    fn unbounded_cwnd_transfer_leaves_avg_cwnd_null() {
        let metrics = TransportMetrics::new();
        let stats = crate::transport::TransferStats {
            stream_id: 1,
            remote_addr: "127.0.0.1:8080".parse().unwrap(),
            bytes_transferred: 1024,
            elapsed: Duration::from_millis(100),
            peak_cwnd_bytes: None,
            final_cwnd_bytes: None,
            slowdowns_triggered: 0,
            base_delay: Duration::ZERO,
            final_ssthresh_bytes: 0,
            min_ssthresh_floor_bytes: 0,
            total_timeouts: 0,
            final_flightsize: 0,
            configured_rate: 0,
        };
        metrics.record_transfer_completed(&stats);
        let snap = metrics.take_snapshot().expect("a transfer completed");
        assert_eq!(snap.avg_cwnd_bytes, None);
        assert_eq!(snap.peak_cwnd_bytes, 0);
        assert_eq!(snap.min_cwnd_bytes, 0);
        let json = serde_json::to_value(&snap).unwrap();
        assert!(json["avg_cwnd_bytes"].is_null(), "{json}");
    }

    /// Final-ACK timing: mean and max over fully acknowledged transfers,
    /// null when none.
    #[test]
    fn transfer_ack_time_avg_and_max() {
        let metrics = TransportMetrics::new();
        metrics.record_transfer_acked(100);
        metrics.record_transfer_acked(300);
        let snap = metrics
            .take_snapshot()
            .expect("acked transfers are activity");
        assert_eq!(snap.transfers_acked, 2);
        assert_eq!(snap.avg_transfer_ack_time_ms, Some(200));
        assert_eq!(snap.max_transfer_ack_time_ms, Some(300));

        metrics.record_packet_sent("10.0.0.1:1".parse().unwrap(), 1);
        let snap = metrics.take_snapshot().expect("activity");
        assert_eq!(snap.transfers_acked, 0);
        assert_eq!(snap.avg_transfer_ack_time_ms, None);
        assert_eq!(snap.max_transfer_ack_time_ms, None);
    }

    /// Inbound queue gauge keeps the high-water mark; drops are counted.
    #[test]
    fn inbound_queue_hwm_and_drops() {
        let metrics = TransportMetrics::new();
        metrics.record_inbound_queue_depth(3);
        metrics.record_inbound_queue_depth(17);
        metrics.record_inbound_queue_depth(5);
        metrics.record_inbound_queue_drop();
        let snap = metrics.take_snapshot().expect("a drop is activity");
        assert_eq!(snap.inbound_queue_depth_hwm, 17);
        assert_eq!(snap.inbound_queue_drops, 1);
    }

    /// The snapshot field that carries `outcome`'s period count. Exhaustive
    /// on purpose: a new `InboundStreamOutcome` fails to compile here until it
    /// is given a `TransportSnapshot` field.
    fn snapshot_count(snap: &TransportSnapshot, outcome: InboundStreamOutcome) -> u32 {
        match outcome {
            InboundStreamOutcome::RefusedInvalidLength => {
                snap.inbound_stream_refused_invalid_length
            }
            InboundStreamOutcome::RefusedConnectionBytes => {
                snap.inbound_stream_refused_connection_bytes
            }
            InboundStreamOutcome::RefusedConnectionStreams => {
                snap.inbound_stream_refused_connection_streams
            }
            InboundStreamOutcome::ClosedTooManyRefusals => {
                snap.inbound_stream_closed_too_many_refusals
            }
            InboundStreamOutcome::EvictedIdle => snap.inbound_stream_evicted_idle,
            InboundStreamOutcome::EvictedAge => snap.inbound_stream_evicted_age,
        }
    }

    /// Recorded outcomes reach the snapshot per reason; `take_snapshot` resets
    /// the period counts but never the cumulative ones the OTel instrument
    /// reads.
    #[test]
    fn inbound_stream_outcomes_snapshot_and_reset() {
        let metrics = TransportMetrics::new();
        // Outcome `i` is recorded `i + 1` times so every reason has a
        // distinct count and a swapped field is caught.
        for (i, outcome) in InboundStreamOutcome::ALL.into_iter().enumerate() {
            for _ in 0..=i {
                metrics.record_inbound_stream_outcome(outcome);
            }
        }
        let read = metrics.read_snapshot();
        for (i, outcome) in InboundStreamOutcome::ALL.into_iter().enumerate() {
            assert_eq!(snapshot_count(&read, outcome), i as u32 + 1, "{outcome:?}");
        }
        let snap = metrics.take_snapshot().expect("outcomes are activity");
        for (i, outcome) in InboundStreamOutcome::ALL.into_iter().enumerate() {
            assert_eq!(snapshot_count(&snap, outcome), i as u32 + 1, "{outcome:?}");
        }

        metrics.record_packet_sent("10.0.0.1:1".parse().unwrap(), 1);
        let snap = metrics.take_snapshot().expect("activity");
        for outcome in InboundStreamOutcome::ALL {
            assert_eq!(snapshot_count(&snap, outcome), 0, "{outcome:?} must reset");
        }
        for (i, (outcome, total)) in metrics
            .cumulative_inbound_stream_outcomes()
            .into_iter()
            .enumerate()
        {
            assert_eq!(total, i as u64 + 1, "{outcome:?} cumulative must not reset");
        }
    }

    /// Each reason alone, on a fresh collector, counts as activity: an
    /// outcome missing from `has_activity` would be dropped whenever it is
    /// the only thing that happened in a period.
    #[test]
    fn each_inbound_stream_outcome_alone_is_activity() {
        for outcome in InboundStreamOutcome::ALL {
            let metrics = TransportMetrics::new();
            metrics.record_inbound_stream_outcome(outcome);
            let snap = metrics
                .take_snapshot()
                .unwrap_or_else(|| panic!("{outcome:?} alone must be activity"));
            assert_eq!(snapshot_count(&snap, outcome), 1, "{outcome:?}");
        }
    }

    /// Every reason's snapshot field is uploaded as `inbound_stream_<label>`.
    #[test]
    fn inbound_stream_outcome_json_keys_match_labels() {
        let json = serde_json::to_value(TransportSnapshot::default()).unwrap();
        for outcome in InboundStreamOutcome::ALL {
            let key = format!("inbound_stream_{}", outcome.label());
            assert!(json.get(&key).is_some(), "missing snapshot key {key}");
        }
    }

    /// Every labelled enum's `ALL` lists its variants in declaration order
    /// (the counter arrays are indexed by `variant as usize`), and its labels
    /// are distinct (each is a snapshot field suffix).
    #[test]
    fn labelled_counter_enums_are_in_declaration_order_with_distinct_labels() {
        fn check<T: Copy + std::fmt::Debug>(
            all: &[T],
            index: fn(T) -> usize,
            label: fn(T) -> &'static str,
        ) {
            for (i, v) in all.iter().enumerate() {
                assert_eq!(index(*v), i, "{v:?}");
            }
            let mut labels: Vec<_> = all.iter().map(|v| label(*v)).collect();
            labels.sort_unstable();
            labels.dedup();
            assert_eq!(labels.len(), all.len());
        }
        check(
            &NatAttemptOrigin::ALL,
            |v| v as usize,
            NatAttemptOrigin::label,
        );
        check(&StrayIntro::ALL, |v| v as usize, StrayIntro::label);
        check(&DialQueueWait::ALL, |v| v as usize, DialQueueWait::label);
    }

    /// The snapshot fields carrying an origin's period attempts and
    /// establishments. Exhaustive on purpose: a new `NatAttemptOrigin` fails
    /// to compile here until it is given `TransportSnapshot` fields.
    fn origin_counts(snap: &TransportSnapshot, origin: NatAttemptOrigin) -> (u32, u32) {
        match origin {
            NatAttemptOrigin::ConnectJoiner => (
                snap.nat_attempts_connect_joiner,
                snap.nat_established_connect_joiner,
            ),
            NatAttemptOrigin::ConnectAcceptor => (
                snap.nat_attempts_connect_acceptor,
                snap.nat_established_connect_acceptor,
            ),
            NatAttemptOrigin::Gateway => (snap.nat_attempts_gateway, snap.nat_established_gateway),
            NatAttemptOrigin::RingPeerNoTransport => (
                snap.nat_attempts_ring_peer_no_transport,
                snap.nat_established_ring_peer_no_transport,
            ),
        }
    }

    /// The snapshot field carrying a stray-intro kind (exhaustive, as above).
    fn stray_count(snap: &TransportSnapshot, kind: StrayIntro) -> u32 {
        match kind {
            StrayIntro::AfterDeadline => snap.stray_intro_after_deadline,
            StrayIntro::SameIpOtherPort => snap.stray_intro_same_ip_other_port,
            StrayIntro::Other => snap.stray_intro_other,
        }
    }

    /// The snapshot field carrying a dial-queue-wait bucket (exhaustive).
    fn wait_count(snap: &TransportSnapshot, bucket: DialQueueWait) -> u32 {
        match bucket {
            DialQueueWait::Lt100Ms => snap.dial_queue_wait_lt_100ms,
            DialQueueWait::Lt1S => snap.dial_queue_wait_lt_1s,
            DialQueueWait::Lt3S => snap.dial_queue_wait_lt_3s,
            DialQueueWait::Ge3S => snap.dial_queue_wait_ge_3s,
        }
    }

    /// Attempts and establishments reach the snapshot per origin, summing to
    /// the untagged counters; `take_snapshot` resets them, but never the
    /// cumulative totals the OTel instrument reads.
    #[test]
    fn nat_origin_counts_snapshot_and_reset() {
        let metrics = TransportMetrics::new();
        // Origin `i` is attempted `i + 1` times and established once fewer,
        // so every field has a distinct count and a swapped one is caught.
        for (i, origin) in NatAttemptOrigin::ALL.into_iter().enumerate() {
            for _ in 0..=i {
                metrics.record_nat_traversal_attempt(TEST_REMOTE, origin);
            }
            for _ in 0..i {
                metrics.record_nat_traversal_established(TEST_REMOTE, origin);
            }
        }
        let n = NatAttemptOrigin::COUNT as u32;
        let attempts = n * (n + 1) / 2;
        let established = n * (n - 1) / 2;
        let read = metrics.read_snapshot();
        let snap = metrics.take_snapshot().expect("attempts are activity");
        for s in [&read, &snap] {
            for (i, origin) in NatAttemptOrigin::ALL.into_iter().enumerate() {
                assert_eq!(
                    origin_counts(s, origin),
                    (i as u32 + 1, i as u32),
                    "{origin:?}"
                );
            }
            assert_eq!(s.nat_traversal_attempts, attempts);
            assert_eq!(s.nat_traversal_established, established);
        }

        metrics.record_packet_sent("10.0.0.1:1".parse().unwrap(), 1);
        let snap = metrics.take_snapshot().expect("activity");
        for origin in NatAttemptOrigin::ALL {
            assert_eq!(
                origin_counts(&snap, origin),
                (0, 0),
                "{origin:?} must reset"
            );
        }
        let (cum_attempts, cum_established, _, _) = metrics.cumulative_nat_traversal();
        assert_eq!(
            (cum_attempts, cum_established),
            (u64::from(attempts), u64::from(established)),
            "cumulative totals must not reset"
        );
    }

    /// Stray intros reach the snapshot per kind and reset with it.
    #[test]
    fn stray_intro_counts_snapshot_and_reset() {
        let metrics = TransportMetrics::new();
        for (i, kind) in StrayIntro::ALL.into_iter().enumerate() {
            for _ in 0..=i {
                metrics.record_stray_intro(TEST_REMOTE, kind);
            }
        }
        let read = metrics.read_snapshot();
        let snap = metrics.take_snapshot().expect("stray intros are activity");
        for s in [&read, &snap] {
            for (i, kind) in StrayIntro::ALL.into_iter().enumerate() {
                assert_eq!(stray_count(s, kind), i as u32 + 1, "{kind:?}");
            }
        }
        assert!(metrics.take_snapshot().is_none(), "stray intros must reset");
    }

    /// Evictions from the recent-failure table reach the snapshot, count as
    /// activity on their own, and reset.
    #[test]
    fn stray_intro_failures_evicted_snapshot_and_reset() {
        let metrics = TransportMetrics::new();
        metrics.record_stray_intro_failures_evicted(0);
        assert!(metrics.take_snapshot().is_none(), "zero is not activity");
        metrics.record_stray_intro_failures_evicted(2);
        metrics.record_stray_intro_failures_evicted(1);
        assert_eq!(metrics.read_snapshot().stray_intro_failures_evicted, 3);
        let snap = metrics.take_snapshot().expect("evictions are activity");
        assert_eq!(snap.stray_intro_failures_evicted, 3);
        assert!(metrics.take_snapshot().is_none(), "evictions must reset");
    }

    /// Bucket boundaries: each bound belongs to the bucket above it.
    #[test]
    fn dial_queue_wait_bucket_boundaries() {
        let ms = Duration::from_millis;
        for (wait, bucket) in [
            (ms(0), DialQueueWait::Lt100Ms),
            (ms(99), DialQueueWait::Lt100Ms),
            (ms(100), DialQueueWait::Lt1S),
            (ms(999), DialQueueWait::Lt1S),
            (ms(1000), DialQueueWait::Lt3S),
            (ms(2999), DialQueueWait::Lt3S),
            (ms(3000), DialQueueWait::Ge3S),
            (Duration::MAX, DialQueueWait::Ge3S),
        ] {
            assert_eq!(DialQueueWait::bucket(wait), bucket, "{wait:?}");
        }
    }

    /// Dial queue waits reach the snapshot per bucket and reset with it.
    #[test]
    fn dial_queue_wait_counts_snapshot_and_reset() {
        let metrics = TransportMetrics::new();
        let waits = [0, 150, 150, 1500, 1500, 1500, 4000, 4000, 4000, 4000];
        for wait in waits {
            metrics.record_dial_queue_wait(TEST_REMOTE, Duration::from_millis(wait));
        }
        let snap = metrics.take_snapshot().expect("waits are activity");
        for (i, bucket) in DialQueueWait::ALL.into_iter().enumerate() {
            assert_eq!(wait_count(&snap, bucket), i as u32 + 1, "{bucket:?}");
        }
        assert!(metrics.take_snapshot().is_none(), "waits must reset");
    }

    /// Each new counter alone, on a fresh collector, is activity: one
    /// missing from `has_activity` would be dropped whenever it is the only
    /// thing that happened in a period.
    #[test]
    fn each_new_counter_alone_is_activity() {
        for origin in NatAttemptOrigin::ALL {
            let metrics = TransportMetrics::new();
            metrics.nat_established_by_origin[origin as usize].fetch_add(1, Ordering::Relaxed);
            let snap = metrics
                .take_snapshot()
                .unwrap_or_else(|| panic!("established {origin:?} alone must be activity"));
            assert_eq!(origin_counts(&snap, origin), (0, 1), "{origin:?}");
            let metrics = TransportMetrics::new();
            metrics.nat_attempts_by_origin[origin as usize].fetch_add(1, Ordering::Relaxed);
            let snap = metrics
                .take_snapshot()
                .unwrap_or_else(|| panic!("attempt {origin:?} alone must be activity"));
            assert_eq!(origin_counts(&snap, origin), (1, 0), "{origin:?}");
        }
        for kind in StrayIntro::ALL {
            let metrics = TransportMetrics::new();
            metrics.record_stray_intro(TEST_REMOTE, kind);
            let snap = metrics
                .take_snapshot()
                .unwrap_or_else(|| panic!("{kind:?} alone must be activity"));
            assert_eq!(stray_count(&snap, kind), 1, "{kind:?}");
        }
        for bucket in DialQueueWait::ALL {
            let metrics = TransportMetrics::new();
            metrics.dial_queue_wait[bucket as usize].fetch_add(1, Ordering::Relaxed);
            let snap = metrics
                .take_snapshot()
                .unwrap_or_else(|| panic!("{bucket:?} alone must be activity"));
            assert_eq!(wait_count(&snap, bucket), 1, "{bucket:?}");
        }
    }

    /// Every new field is uploaded under `<prefix>_<label>`.
    #[test]
    fn new_counter_json_keys_match_labels() {
        let json = serde_json::to_value(TransportSnapshot::default()).unwrap();
        let mut keys = Vec::new();
        for origin in NatAttemptOrigin::ALL {
            keys.push(format!("nat_attempts_{}", origin.label()));
            keys.push(format!("nat_established_{}", origin.label()));
        }
        for kind in StrayIntro::ALL {
            keys.push(format!("stray_intro_{}", kind.label()));
        }
        for bucket in DialQueueWait::ALL {
            keys.push(format!("dial_queue_wait_{}", bucket.label()));
        }
        for key in keys {
            assert!(json.get(&key).is_some(), "missing snapshot key {key}");
        }
    }

    /// Each NAT failure kind lands in its own counter.
    #[test]
    fn nat_failure_kinds_are_partitioned() {
        let metrics = TransportMetrics::new();
        for kind in [
            NatFailureKind::NoResponse,
            NatFailureKind::NoResponse,
            NatFailureKind::Undecryptable,
            NatFailureKind::VersionIncompatible,
            NatFailureKind::Incomplete,
            NatFailureKind::SendError,
            NatFailureKind::Other,
        ] {
            metrics.record_nat_failure_kind(kind);
        }
        let snap = metrics.take_snapshot().expect("NAT failures are activity");
        assert_eq!(
            (
                snap.nat_fail_no_response,
                snap.nat_fail_undecryptable,
                snap.nat_fail_version_incompatible,
                snap.nat_fail_incomplete,
                snap.nat_fail_send_error,
                snap.nat_fail_other,
            ),
            (2, 1, 1, 1, 1, 1)
        );
    }

    /// Per-attempt NAT failure samples are bounded per period, the overflow
    /// is counted, and both reset with the snapshot.
    #[test]
    fn nat_failure_samples_are_bounded_and_reset() {
        let metrics = TransportMetrics::new();
        for _ in 0..(MAX_NAT_FAILURE_SAMPLES + 3) {
            metrics.record_nat_failure(
                TEST_REMOTE,
                NatFailureKind::Undecryptable,
                NatAttemptSeen {
                    received_any: true,
                    decrypted_any: false,
                    deadline_reached: true,
                },
                NatAttemptOrigin::ConnectJoiner,
            );
        }
        let snap = metrics.take_snapshot().expect("failures are activity");
        assert_eq!(
            snap.nat_fail_undecryptable as usize,
            MAX_NAT_FAILURE_SAMPLES + 3
        );
        assert_eq!(snap.nat_fail_samples.len(), MAX_NAT_FAILURE_SAMPLES);
        assert_eq!(snap.nat_fail_samples_dropped, 3);
        assert_eq!(
            snap.nat_fail_samples[0],
            NatFailureSample {
                sub_reason: "undecryptable".into(),
                received_any: true,
                decrypted_any: false,
                deadline_reached: true,
                origin: "connect_joiner".into(),
            }
        );
        let json = serde_json::to_value(&snap).unwrap();
        assert_eq!(json["nat_fail_samples"][0]["sub_reason"], "undecryptable");
        assert_eq!(json["nat_fail_samples"][0]["origin"], "connect_joiner");
        assert!(metrics.take_snapshot().is_none(), "samples must reset");
    }

    /// Same IP with a different port is the endpoint-dependent mapping
    /// signal; a different IP is counted separately; the first report and a
    /// repeat of the same address change nothing.
    #[test]
    fn observed_external_addr_change_classification() {
        let metrics = TransportMetrics::new();
        let gw1: SocketAddr = "192.0.2.1:31337".parse().unwrap();
        let gw2: SocketAddr = "192.0.2.2:31337".parse().unwrap();
        let peer: SocketAddr = "192.0.2.3:5000".parse().unwrap();
        let v6_gw: SocketAddr = "[2001:db8::1]:31337".parse().unwrap();
        // Same mapping seen by two gateways: no change (no false positive).
        metrics.record_observed_external_addr(gw1, "203.0.113.5:40000".parse().unwrap());
        metrics.record_observed_external_addr(gw2, "203.0.113.5:40000".parse().unwrap());
        // Dual-stack: a v6 report is never compared with v4 ones.
        metrics.record_observed_external_addr(v6_gw, "[2001:db8::5]:40000".parse().unwrap());
        // The same reporter seeing a new port later is a rebinding, not EDM.
        metrics.record_observed_external_addr(gw2, "203.0.113.5:40001".parse().unwrap());
        // An IPv4-mapped IPv6 report is compared as IPv4 (same mapping: no change).
        metrics.record_observed_external_addr(gw1, "[::ffff:203.0.113.5]:40001".parse().unwrap());
        // A different reporter, same IP, different port: EDM signal.
        metrics.record_observed_external_addr(peer, "203.0.113.5:40123".parse().unwrap());
        // A different reporter, different IP.
        metrics.record_observed_external_addr(gw1, "198.51.100.9:40123".parse().unwrap());
        let snap = metrics.take_snapshot().expect("reports are activity");
        assert_eq!(snap.observed_addr_reports, 7);
        assert_eq!(snap.observed_addr_port_changes, 1);
        assert_eq!(snap.observed_addr_ip_changes, 1);
    }

    /// Percentiles are nearest-rank over one value per connection.
    #[test]
    fn conn_rtt_distribution_percentiles() {
        let d = ConnRttDistribution::from_samples(
            vec![50, 10, 40, 20, 30, 100, 90, 80, 70, 60],
            vec![5, 1],
        );
        assert_eq!(d.connections, 10);
        assert_eq!(d.srtt_p50_us, Some(50));
        assert_eq!(d.srtt_p90_us, Some(90));
        assert_eq!(d.srtt_max_us, Some(100));
        assert_eq!(d.min_rtt_p50_us, Some(1));
        assert_eq!(d.min_rtt_p90_us, Some(5));
        assert_eq!(d.min_rtt_max_us, Some(5));

        let single = ConnRttDistribution::from_samples(vec![7], vec![7]);
        assert_eq!((single.srtt_p50_us, single.srtt_p90_us), (Some(7), Some(7)));

        let empty = ConnRttDistribution::from_samples(vec![], vec![]);
        assert_eq!(empty.connections, 0);
        assert_eq!(empty.srtt_p50_us, None);
        assert_eq!(empty.min_rtt_max_us, None);
    }

    /// The uploaded JSON is additive: every pre-existing key is still
    /// present, and the new keys are there too.
    #[test]
    fn snapshot_json_keeps_old_keys_and_adds_new_ones() {
        let json = serde_json::to_value(TransportSnapshot::default()).unwrap();
        let obj = json.as_object().unwrap();
        for key in [
            "transfers_completed",
            "transfers_failed",
            "bytes_sent",
            "bytes_received",
            "avg_transfer_time_ms",
            "peak_throughput_bps",
            "avg_cwnd_bytes",
            "peak_cwnd_bytes",
            "min_cwnd_bytes",
            "slowdowns_triggered",
            "nat_traversal_attempts",
            "nat_traversal_established",
            "nat_traversal_failed_error",
            "nat_traversal_failed_version",
            "avg_rtt_us",
            "min_rtt_us",
            "max_rtt_us",
            "packets_sent",
            "packets_retransmitted",
            "packets_abandoned",
            "rto_expiries",
            "tlp_probes",
            "standalone_ack_packets",
            "standalone_ack_bytes",
            "conn_rtt_connections",
            "conn_srtt_p50_us",
            "conn_srtt_p90_us",
            "conn_srtt_max_us",
            "conn_min_rtt_p50_us",
            "conn_min_rtt_p90_us",
            "conn_min_rtt_max_us",
            "nat_fail_no_response",
            "nat_fail_undecryptable",
            "nat_fail_version_incompatible",
            "nat_fail_incomplete",
            "nat_fail_send_error",
            "nat_fail_other",
            "observed_addr_reports",
            "observed_addr_port_changes",
            "observed_addr_ip_changes",
            "transfers_acked",
            "avg_transfer_ack_time_ms",
            "max_transfer_ack_time_ms",
            "inbound_queue_depth_hwm",
            "inbound_queue_drops",
            "nat_fail_samples",
            "nat_fail_samples_dropped",
            "inbound_stream_refused_invalid_length",
            "inbound_stream_refused_connection_bytes",
            "inbound_stream_refused_connection_streams",
            "inbound_stream_closed_too_many_refusals",
            "inbound_stream_evicted_idle",
            "inbound_stream_evicted_age",
            "nat_attempts_connect_joiner",
            "nat_attempts_connect_acceptor",
            "nat_attempts_gateway",
            "nat_attempts_ring_peer_no_transport",
            "nat_established_connect_joiner",
            "nat_established_connect_acceptor",
            "nat_established_gateway",
            "nat_established_ring_peer_no_transport",
            "stray_intro_after_deadline",
            "stray_intro_same_ip_other_port",
            "stray_intro_other",
            "stray_intro_failures_evicted",
            "dial_queue_wait_lt_100ms",
            "dial_queue_wait_lt_1s",
            "dial_queue_wait_lt_3s",
            "dial_queue_wait_ge_3s",
        ] {
            assert!(obj.contains_key(key), "missing key {key}");
        }
        // The snapshot is also bincode-encoded into the local event log, so
        // it must round-trip there (no `serde(flatten)` / untagged tricks).
        let snap = TransportSnapshot {
            rto_expiries: 4,
            conn_srtt_p50_us: Some(12),
            avg_cwnd_bytes: None,
            ..Default::default()
        };
        let bytes = bincode::serialize(&snap).unwrap();
        let back: TransportSnapshot = bincode::deserialize(&bytes).unwrap();
        assert_eq!(back.rto_expiries, 4);
        assert_eq!(back.conn_srtt_p50_us, Some(12));
    }
}
