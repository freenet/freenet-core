use std::cell::Cell;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::time::Duration;

use parking_lot::RwLock;

use crate::config::GlobalExecutor;
use crate::simulation::{RealTime, TimeSource, TimeSourceInterval};
use crate::transport::connection_handler::NAT_TRAVERSAL_MAX_ATTEMPTS;
use crate::transport::crypto::TransportSecretKey;
use crate::transport::packet_data::UnknownEncryption;
use crate::transport::sent_packet_tracker::MESSAGE_CONFIRMATION_TIMEOUT;
use aes_gcm::Aes128Gcm;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{Instrument, instrument, span};

mod inbound_stream;
mod outbound_stream;
pub(crate) mod piped_stream;
pub(crate) mod streaming;

/// Lock-free streaming buffer implementation.
/// Public when bench feature is enabled for benchmarking.
#[cfg(feature = "bench")]
pub mod streaming_buffer;
#[cfg(not(feature = "bench"))]
pub(crate) mod streaming_buffer;

use super::{
    TransportError,
    bbr::DeliveryRateToken,
    congestion_control::{CongestionControl, CongestionController},
    connection_handler::SerializedMessage,
    global_bandwidth::GlobalBandwidthManager,
    packet_data::{self, PacketData},
    received_packet_tracker::ReceivedPacketTracker,
    received_packet_tracker::ReportResult,
    sent_packet_tracker::{PacketStream, ResendAction, SentPacketTracker},
    symmetric_message::{self, SymmetricMessage, SymmetricMessagePayload},
    token_bucket::TokenBucket,
};
use crate::operations::orphan_streams::OrphanStreamRegistry;
use crate::util::time_source::InstantTimeSrc;

type Result<T = (), E = TransportError> = std::result::Result<T, E>;

/// Result type for outbound stream transfers, includes transfer statistics.
type OutboundStreamResult = std::result::Result<super::TransferStats, TransportError>;

/// The max payload we can send in a single fragment, this MUST be less than packet_data::MAX_DATA_SIZE
/// since we need to account for the space overhead of SymmetricMessage::StreamFragment metadata.
/// Measured overhead: 41 bytes (see symmetric_message::stream_fragment_overhead())
/// The extra byte vs. the original 40 comes from the Option discriminant of `metadata_bytes`.
const MAX_DATA_SIZE: usize = packet_data::MAX_DATA_SIZE - 41;

/// How often to check for pending ACKs and send them proactively.
/// This prevents ACKs from being delayed when there's no outgoing traffic to piggyback on.
///
/// Set to MAX_CONFIRMATION_DELAY (100ms), which is the documented expectation from
/// `ReceivedPacketTracker`. The sender's actual timeout is MESSAGE_CONFIRMATION_TIMEOUT
/// (600ms = 100ms + 500ms network allowance), so 100ms provides ample margin.
///
/// Note: 50ms was tried initially but caused issues in Docker NAT test environments
/// due to increased timer overhead. 100ms provides the right balance between
/// responsiveness and system load.
///
/// Without this timer, ACKs would only be sent when:
/// 1. The receipt buffer fills up (20 packets)
/// 2. MESSAGE_CONFIRMATION_TIMEOUT (600ms) expires on packet arrival
///
/// This caused ~600ms delays per hop for streams larger than 20 packets.
const ACK_CHECK_INTERVAL: Duration = Duration::from_millis(100);

/// Relaxed timer intervals for simulation mode (direct runner with `start_paused(true)`).
///
/// In the direct runner, the transport layer uses `RealTime` but time is virtual via
/// tokio's paused clock. With 500 nodes × ~30 connections = ~15K connections, the default
/// timer intervals create ~900K timer firings per second of virtual time, which dominates
/// wall-clock runtime. These relaxed intervals reduce that to ~180K/sec (5x improvement).
///
/// Safety: SimulationSocket uses in-memory delivery with no real packet loss, so slower
/// ACK/retransmit checking doesn't cause correctness issues — it only affects simulated
/// throughput slightly.
const SIMULATION_ACK_CHECK_INTERVAL: Duration = Duration::from_millis(500);
const SIMULATION_RESEND_CHECK_INTERVAL: Duration = Duration::from_millis(50);
const SIMULATION_RESEND_YIELD_DELAY: Duration = Duration::from_millis(10);

#[must_use]
pub(crate) struct RemoteConnection<S = super::UdpSocket, T: TimeSource = RealTime> {
    pub(super) outbound_symmetric_key: Aes128Gcm,
    pub(super) remote_addr: SocketAddr,
    pub(super) sent_tracker: Arc<parking_lot::Mutex<SentPacketTracker<T>>>,
    pub(super) last_packet_id: Arc<AtomicU32>,
    pub(super) inbound_packet_recv: mpsc::Receiver<PacketData<UnknownEncryption>>,
    pub(super) inbound_symmetric_key: Aes128Gcm,
    pub(super) inbound_symmetric_key_bytes: [u8; 16],
    #[allow(dead_code)]
    pub(super) my_address: Option<SocketAddr>,
    /// Remote peer's negotiated protocol version, captured during the handshake.
    /// `None` on the joiner->gateway path (the gateway's AckConnection payload
    /// carries no version) — see connection_handler.rs traverse_nat AckConnection arm.
    pub(super) remote_protoc_version: Option<(u8, u8, u16)>,
    pub(super) transport_secret_key: TransportSecretKey,
    /// Congestion controller (BBR by default) - adapts to network conditions
    pub(super) congestion_controller: Arc<CongestionController<T>>,
    /// Token bucket rate limiter - smooths packet pacing based on congestion controller rate
    pub(super) token_bucket: Arc<TokenBucket<T>>,
    /// Socket for direct packet sending (bypasses centralized rate limiter)
    pub(super) socket: Arc<S>,
    /// Global bandwidth manager for fair sharing across connections.
    /// When Some, the token_bucket rate is periodically updated based on connection count.
    pub(super) global_bandwidth: Option<Arc<GlobalBandwidthManager>>,
    /// Shadow per-peer rolling RTT stats (issue #4074, Phase 1).
    /// Auto-registers with the global shadow registry on construction
    /// and removes itself on drop. Written here on each non-retransmitted
    /// ACK; the cross-connection aggregator reads via the registry.
    /// Does not feed back into congestion control.
    pub(crate) rolling_rtt_stats: super::rolling_rtt_stats::RollingRttStatsHandle<T>,
    /// Time source for deterministic simulation support
    pub(super) time_source: T,
}

impl<S, T: TimeSource> Drop for RemoteConnection<S, T> {
    fn drop(&mut self) {
        // Unregister from global bandwidth manager so other connections can use the freed bandwidth
        if let Some(ref global) = self.global_bandwidth {
            global.unregister_connection();
            tracing::debug!(
                peer_addr = %self.remote_addr,
                remaining_connections = global.connection_count(),
                "Unregistered connection from global bandwidth pool"
            );
        }
    }
}

/// Unique identifier for a streaming transfer.
///
/// Used to correlate streaming metadata messages with their data streams.
/// StreamIds are unique within a node's lifetime and are generated atomically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct StreamId(u32);

const STREAM_ID_BLOCK: u32 = 100_000;

thread_local! {
    static STREAM_ID_COUNTER: Cell<u32> = {
        let idx = crate::config::GlobalRng::thread_index();
        Cell::new((idx as u32) * STREAM_ID_BLOCK)
    };
}

impl StreamId {
    /// Marker bit that distinguishes operations-level streams from transport-level streams.
    /// When set, the receiver skips the legacy `InboundStream` reassembly path and only
    /// routes fragments through the orphan registry / streaming handles.
    const OPS_STREAM_BIT: u32 = 0x8000_0000;

    pub fn next() -> Self {
        Self(STREAM_ID_COUNTER.with(|c| {
            let v = c.get();
            c.set(v + 1);
            v
        }))
    }

    /// Generate a new StreamId for operations-level streaming.
    /// The high bit is set so the receiver can distinguish these from transport-level streams
    /// and skip the legacy `InboundStream` decode path (which would fail because the bytes
    /// are a streaming payload, not a `NetMessage`).
    pub fn next_operations() -> Self {
        let id = STREAM_ID_COUNTER.with(|c| {
            let v = c.get();
            c.set(v + 1);
            v
        });
        Self(id | Self::OPS_STREAM_BIT)
    }

    /// Returns true if this stream was created for operations-level streaming.
    pub fn is_operations_stream(&self) -> bool {
        self.0 & Self::OPS_STREAM_BIT != 0
    }

    /// Reset the stream ID counter to initial state for this thread.
    /// Thread-local, so safe for parallel test execution.
    pub fn reset_counter() {
        let idx = crate::config::GlobalRng::thread_index();
        STREAM_ID_COUNTER.with(|c| c.set((idx as u32) * STREAM_ID_BLOCK));
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
impl<'a> arbitrary::Arbitrary<'a> for StreamId {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self(u.arbitrary()?))
    }
}

type InboundStreamResult = Result<(StreamId, SerializedMessage), StreamId>;

/// The `PeerConnection` struct is responsible for managing the connection with a remote peer.
/// It provides methods for sending and receiving messages to and from the remote peer.
///
/// The `PeerConnection` struct maintains the state of the connection, including the remote
/// connection details, trackers for received and sent packets, and futures for inbound and
/// outbound streams.
///
/// The `send` method is used to send serialized data to the remote peer. If the data size
/// exceeds the maximum allowed size, it is sent as a stream; otherwise, it is sent as a
/// short message.
///
/// The `recv` method is used to receive incoming packets from the remote peer. It listens for
/// incoming packets or receipts, and resends packets if necessary.
///
/// The `process_inbound` method is used to process incoming payloads based on their type.
///
/// The `noop`, `outbound_short_message`, and `outbound_stream` methods are used internally for
/// sending different types of messages.
///
/// The `packet_sending` function is a helper function used to send packets to the remote peer.
#[must_use = "call await on the `recv` function to start listening for incoming messages"]
pub struct PeerConnection<S = super::UdpSocket, T: TimeSource = RealTime> {
    remote_conn: RemoteConnection<S, T>,
    received_tracker: ReceivedPacketTracker<InstantTimeSrc>,
    inbound_streams: HashMap<StreamId, mpsc::Sender<(u32, bytes::Bytes)>>,
    inbound_stream_futures: FuturesUnordered<JoinHandle<InboundStreamResult>>,
    outbound_stream_futures: FuturesUnordered<JoinHandle<OutboundStreamResult>>,
    failure_count: usize,
    /// First failure time as nanoseconds since time_source epoch
    first_failure_time_nanos: Option<u64>,
    /// Last packet report time as nanoseconds since time_source epoch
    last_packet_report_time_nanos: u64,
    /// Last rate update time as nanoseconds since time_source epoch.
    /// Used to implement RTT-adaptive rate updates (update approximately once per RTT)
    last_rate_update_nanos: Option<u64>,
    /// Timestamp (nanoseconds from time_source epoch) of the last received inbound packet.
    ///
    /// Persisted across `recv()` calls to survive cancellation. When the outer
    /// `peer_connection_listener` select picks the outbound branch, `recv()` is
    /// cancelled and re-called. Without persisting this across calls, the idle
    /// timeout window resets on every cancellation, preventing dead-peer detection
    /// when there is outbound traffic (see #3369).
    last_received_nanos: u64,
    keep_alive_handle: Option<JoinHandle<()>>,
    /// Tracks pending ping probes awaiting pong responses.
    /// Maps ping sequence number -> send timestamp (nanoseconds since time_source epoch).
    /// Used for bidirectional liveness detection.
    pending_pings: Arc<RwLock<BTreeMap<u64, u64>>>,
    /// Registry for streaming inbound streams.
    /// Maps stream IDs to streaming handles for incremental fragment access.
    streaming_registry: Arc<streaming::StreamRegistry>,
    /// Streaming handles for active streams (parallels inbound_streams).
    /// Used for pushing fragments to streaming consumers.
    ///
    /// Each entry pairs a `StreamHandle` with `last_activity_nanos` (in the
    /// `time_source` epoch) — the time of the most recent successful interaction
    /// with this stream (either initial registration or a subsequent fragment
    /// arrival). The periodic sweep on `timeout_check` ticks drops entries idle
    /// for longer than `streaming_handle_idle_timeout()`; without that bound, a
    /// stalled upstream sender (after the orphan registry GC has already
    /// cancelled the handle, or after the driver claimed-then-timed-out) leaks
    /// the underlying `Arc<LockFreeStreamBuffer>` until the entire connection
    /// drops. See issue #4079.
    streaming_handles: HashMap<StreamId, (streaming::StreamHandle, u64)>,
    /// Time source for deterministic simulation support
    time_source: T,
    /// Optional orphan stream registry for handling race conditions between
    /// stream fragments and metadata messages (RequestStreaming/ResponseStreaming).
    /// Set by the node layer after connection establishment.
    orphan_stream_registry:
        Option<std::sync::Arc<crate::operations::orphan_streams::OrphanStreamRegistry>>,
    /// LRU cache of recently dispatched metadata hashes (capacity 1000), used to dedup
    /// embedded-metadata-in-fragment-#1 against the separate ShortMessage.
    ///
    /// Uses `DefaultHasher` (not cryptographic). A hash collision would cause
    /// a silent metadata drop, leading to a 60-second operation timeout for
    /// the affected stream -- acceptable given the small working set and
    /// negligible collision probability. See issue #3317.
    dispatched_msg_hashes: lru::LruCache<u64, ()>,
}

impl<S, T: TimeSource> std::fmt::Debug for PeerConnection<S, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerConnection")
            .field("remote_conn", &self.remote_conn.remote_addr)
            .finish()
    }
}

impl<S, T: TimeSource> Drop for PeerConnection<S, T> {
    fn drop(&mut self) {
        // Cancel all active streaming handles so that any operation blocked on
        // stream_handle.assemble().await will receive StreamError::Cancelled
        // instead of waiting forever for fragments that will never arrive.
        let stream_count = self.streaming_handles.len();
        if stream_count > 0 {
            tracing::debug!(
                peer_addr = %self.remote_conn.remote_addr,
                stream_count,
                "Cancelling streaming handles on connection drop"
            );
            for (handle, _) in self.streaming_handles.values() {
                handle.cancel();
            }
        }

        if let Some(handle) = self.keep_alive_handle.take() {
            tracing::debug!(
                peer_addr = %self.remote_conn.remote_addr,
                "Cancelling keep-alive task"
            );
            handle.abort();
        }
    }
}

/// Interval between keep-alive ping packets.
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(5);

/// Maximum keepalive interval after backoff (caps the exponential growth).
const MAX_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(60);

/// Returns the keepalive interval for the given number of pending (unanswered) pings.
///
/// At or below `MAX_UNANSWERED_PINGS`: returns `KEEP_ALIVE_INTERVAL` (5s).
/// Above that threshold: doubles per additional unanswered ping, capped at 60s.
fn keepalive_interval_for_pending(pending_count: usize) -> Duration {
    if pending_count > MAX_UNANSWERED_PINGS {
        let extra = (pending_count - MAX_UNANSWERED_PINGS).min(4) as u32;
        KEEP_ALIVE_INTERVAL
            .saturating_mul(2u32.pow(extra))
            .min(MAX_KEEPALIVE_INTERVAL)
    } else {
        KEEP_ALIVE_INTERVAL
    }
}

/// Maximum number of unanswered pings before considering the connection dead.
///
/// With 5-second ping interval, 5 unanswered pings means 25 seconds of no response.
/// This is intentionally shorter than KILL_CONNECTION_AFTER (120s) because:
/// 1. We're actively probing, so we can detect failures faster
/// 2. Operations have their own timeouts, but routing to a dead peer wastes time
/// 3. 25 seconds is long enough to tolerate network hiccups and CI load spikes
/// 4. Bandwidth is negligible (~20 bytes/sec per connection)
///
/// Note: Was previously 2 (10 seconds) but caused flaky test failures due to
/// pong responses being delayed under CI load (issue: premature connection closure).
const MAX_UNANSWERED_PINGS: usize = 5;

/// Maximum number of metadata hashes to track for dedup.
const DEDUP_CACHE_CAPACITY: NonZeroUsize = match NonZeroUsize::new(1000) {
    Some(v) => v,
    None => panic!("DEDUP_CACHE_CAPACITY must be non-zero"),
};

/// Production idle threshold for sweeping `streaming_handles` entries.
///
/// A streaming handle that has not received a fragment in this long is treated
/// as a stalled stream: cancel the handle, drop the entry from the map, and
/// drop the matching `streaming_registry` entry. This releases the two clones
/// of `Arc<LockFreeStreamBuffer>` that `PeerConnection` owns; the buffer
/// itself is freed only when the LAST clone drops, which for the #4079 leak
/// scenarios is the same moment (no orphan-registry clone survives at sweep
/// time, see "Why no orphan-registry sweep here" below).
///
/// Sized at 6 × `streaming::STREAM_INACTIVITY_TIMEOUT` — comfortably past
/// the 5s point at which any in-flight `assemble()` would have already failed
/// with `InactivityTimeout`, but well under the 120s connection idle timeout
/// and the 60s orphan registry timeout. Production data (2026-04) puts p95
/// successful transfer at 1.8s, so a 30s gap is conclusive evidence the
/// stream is dead. Derived at compile time so a future change to
/// `STREAM_INACTIVITY_TIMEOUT` propagates.
///
/// Worst-case eviction latency (production) is `STREAMING_HANDLE_IDLE_TIMEOUT_PROD + the
/// timeout_check tick interval` (= 30s + 5s = 35s) because the sweep
/// piggybacks on the existing `timeout_check` tick in `recv()`. Even on a
/// connection with zero inbound traffic the tick still fires (it is
/// interval-based, not packet-driven), so the sweep runs.
///
/// ### Why no orphan-registry sweep here
///
/// This sweep does NOT reach into `OrphanStreamRegistry` to remove its
/// parallel clone of the handle. That's intentional: the orphan registry's
/// own 60s GC handles its residual clones, and we don't carry the
/// `(peer_addr, stream_id)` key path back into `OrphanStreamRegistry` from
/// here. For the two leak scenarios in #4079 — orphan-registry-already-GC'd,
/// or driver-claimed-then-dropped — no orphan-registry clone survives at
/// sweep time, so the buffer Arc drops immediately. For other paths a brief
/// (<60s) transient retention via the orphan registry is acceptable; the
/// permanent leak is closed.
///
/// See issue #4079.
const STREAMING_HANDLE_IDLE_TIMEOUT_PROD: Duration =
    Duration::from_secs(streaming::STREAM_INACTIVITY_TIMEOUT.as_secs() * 6);

/// Simulation idle threshold for sweeping `streaming_handles` entries.
///
/// Under the direct simulation runner with `start_paused(true)`, virtual time
/// can jump past 30s when tasks await `spawn_blocking` (WASM execution) —
/// this is the same constraint that pushed `connection_idle_timeout()` to
/// 24h in simulation mode (see `simulation/time.rs:215-226`). Reusing the
/// same horizon here prevents a single WASM-induced time jump from
/// falsely reaping an in-flight streaming relay's handle.
///
/// Activated when `SimulationIdleTimeout::is_enabled()` returns true — same
/// gate as the connection idle timeout's simulation accommodation.
const STREAMING_HANDLE_IDLE_TIMEOUT_SIM: Duration = Duration::from_secs(86_400);

/// Returns the active idle threshold for sweeping `streaming_handles` entries,
/// switching between the production and simulation values based on the same
/// `SimulationIdleTimeout` flag that gates `connection_idle_timeout()`.
fn streaming_handle_idle_timeout() -> Duration {
    if crate::config::SimulationIdleTimeout::is_enabled() {
        STREAMING_HANDLE_IDLE_TIMEOUT_SIM
    } else {
        STREAMING_HANDLE_IDLE_TIMEOUT_PROD
    }
}

/// Drop entries from `streaming_handles` (and their companion
/// `streaming_registry` rows) that have been idle for longer than `threshold`.
///
/// The threshold is passed in (rather than read from
/// `streaming_handle_idle_timeout()`) so unit tests can pin the boundary
/// precisely without depending on the `SimulationIdleTimeout` global flag.
/// Production calls `sweep_idle_streaming_handles` which reads the active
/// threshold from `streaming_handle_idle_timeout()`.
///
/// Factored out of `PeerConnection::sweep_idle_streaming_handles` so the
/// logic can be unit-tested against a plain map without constructing a
/// full `PeerConnection`. See issue #4079 for the leak scenarios.
fn sweep_streaming_handles_inner(
    streaming_handles: &mut HashMap<StreamId, (streaming::StreamHandle, u64)>,
    streaming_registry: &streaming::StreamRegistry,
    now_nanos: u64,
    threshold: Duration,
    peer_addr: SocketAddr,
) {
    let threshold_nanos = threshold.as_nanos() as u64;
    let mut swept_count: usize = 0;
    streaming_handles.retain(|&stream_id, (handle, last_activity_nanos)| {
        let idle_nanos = now_nanos.saturating_sub(*last_activity_nanos);
        if idle_nanos > threshold_nanos {
            tracing::debug!(
                %peer_addr,
                stream_id = %stream_id,
                idle_secs = Duration::from_nanos(idle_nanos).as_secs_f64(),
                "Sweeping idle streaming handle (issue #4079)"
            );
            handle.cancel();
            streaming_registry.remove(stream_id);
            swept_count += 1;
            false
        } else {
            true
        }
    });
    if swept_count > 0 {
        tracing::info!(
            %peer_addr,
            swept_count,
            remaining = streaming_handles.len(),
            "Swept idle streaming handles"
        );
    }
}

#[allow(private_bounds)]
impl<S: super::Socket, T: TimeSource> PeerConnection<S, T> {
    pub(super) fn new(remote_conn: RemoteConnection<S, T>) -> Self {
        // Start the keep-alive task before creating Self
        let remote_addr = remote_conn.remote_addr;
        let socket = remote_conn.socket.clone();
        let outbound_key = remote_conn.outbound_symmetric_key.clone();
        let last_packet_id = remote_conn.last_packet_id.clone();
        let time_source = remote_conn.time_source.clone();

        // Shared state for bidirectional liveness detection
        // Uses u64 nanoseconds for deterministic simulation support
        let pending_pings: Arc<RwLock<BTreeMap<u64, u64>>> = Arc::new(RwLock::new(BTreeMap::new()));
        let pending_pings_for_task = pending_pings.clone();

        let task_time_source = time_source.clone();
        let keepalive_enabled = time_source.supports_keepalive();
        let keep_alive_handle = GlobalExecutor::spawn(async move {
            // Exit immediately if keepalive is disabled for this time source.
            // VirtualTime disables keepalive because auto-advance + keepalive timing
            // causes spurious connection closures. See issue #2695.
            if !keepalive_enabled {
                tracing::debug!(
                    target: "freenet_core::transport::keepalive_lifecycle",
                    remote = ?remote_addr,
                    "Keep-alive task SKIPPED (time source does not support keepalive)"
                );
                return;
            }

            tracing::info!(
                target: "freenet_core::transport::keepalive_lifecycle",
                remote = ?remote_addr,
                "Keep-alive task STARTED for connection"
            );

            let mut tick_count = 0u64;
            let mut ping_seq = 0u64;
            let task_start_nanos = task_time_source.now_nanos();

            loop {
                // Back off keepalive interval when pings go unanswered (#3252).
                let pending_count = pending_pings_for_task.read().len();
                let wait_duration = keepalive_interval_for_pending(pending_count);
                task_time_source.sleep(wait_duration).await;
                tick_count += 1;

                let now_nanos = task_time_source.now_nanos();
                let elapsed_since_start_nanos = now_nanos.saturating_sub(task_start_nanos);

                // Use local ping sequence counter
                let current_ping_seq = ping_seq;
                ping_seq += 1;

                tracing::debug!(
                    target: "freenet_core::transport::keepalive_lifecycle",
                    remote = ?remote_addr,
                    tick_count,
                    ping_sequence = current_ping_seq,
                    elapsed_since_start_secs = Duration::from_nanos(elapsed_since_start_nanos).as_secs_f64(),
                    wait_interval_secs = wait_duration.as_secs_f64(),
                    unanswered_pings = pending_count,
                    "Keep-alive tick - sending Ping"
                );

                // Create a Ping packet for bidirectional liveness detection
                let packet_id = last_packet_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let ping_packet = match SymmetricMessage::serialize_msg_to_packet_data(
                    packet_id,
                    SymmetricMessagePayload::Ping {
                        sequence: current_ping_seq,
                    },
                    &outbound_key,
                    vec![], // No receipts for keep-alive
                ) {
                    Ok(packet) => packet.prepared_send(),
                    Err(e) => {
                        tracing::error!(?e, "Failed to create keep-alive Ping packet");
                        break;
                    }
                };

                // Record the pending ping before sending (using nanoseconds)
                {
                    let mut pending = pending_pings_for_task.write();
                    pending.insert(current_ping_seq, task_time_source.now_nanos());
                }

                // Send the keep-alive Ping packet directly to socket
                tracing::debug!(
                    target: "freenet_core::transport::keepalive_lifecycle",
                    remote = ?remote_addr,
                    packet_id,
                    ping_sequence = current_ping_seq,
                    "Sending keep-alive Ping packet"
                );

                match socket.send_to(&ping_packet, remote_addr).await {
                    Ok(_) => {
                        // Phase 1.6 (#4074): keep-alive Ping bypasses
                        // `packet_sending`; count it as must-flow here.
                        // Observation only.
                        crate::transport::shadow_demand::record_outbound(
                            crate::transport::shadow_demand::OutboundClass::MustFlow,
                            ping_packet.len(),
                        );
                        tracing::debug!(
                            target: "freenet_core::transport::keepalive_lifecycle",
                            remote = ?remote_addr,
                            packet_id,
                            ping_sequence = current_ping_seq,
                            "Keep-alive Ping packet sent successfully"
                        );
                    }
                    Err(e) => {
                        let elapsed = Duration::from_nanos(
                            task_time_source
                                .now_nanos()
                                .saturating_sub(task_start_nanos),
                        );
                        tracing::warn!(
                            target: "freenet_core::transport::keepalive_lifecycle",
                            remote = ?remote_addr,
                            error = ?e,
                            elapsed_since_start_secs = elapsed.as_secs_f64(),
                            total_ticks = tick_count,
                            "Keep-alive task STOPPING - socket error"
                        );
                        break;
                    }
                }
            }

            let elapsed = Duration::from_nanos(
                task_time_source
                    .now_nanos()
                    .saturating_sub(task_start_nanos),
            );
            tracing::warn!(
                target: "freenet_core::transport::keepalive_lifecycle",
                remote = ?remote_addr,
                total_lifetime_secs = elapsed.as_secs_f64(),
                total_ticks = tick_count,
                "Keep-alive task EXITING"
            );
        });

        tracing::debug!(
            peer_addr = %remote_addr,
            "PeerConnection created with persistent keep-alive task"
        );

        let now_nanos = time_source.now_nanos();
        Self {
            remote_conn,
            received_tracker: ReceivedPacketTracker::new(),
            inbound_streams: HashMap::new(),
            inbound_stream_futures: FuturesUnordered::new(),
            outbound_stream_futures: FuturesUnordered::new(),
            failure_count: 0,
            first_failure_time_nanos: None,
            last_packet_report_time_nanos: now_nanos,
            last_rate_update_nanos: None,
            last_received_nanos: now_nanos,
            keep_alive_handle: Some(keep_alive_handle),
            pending_pings,
            streaming_registry: Arc::new(streaming::StreamRegistry::new()),
            streaming_handles: HashMap::new(),
            time_source,
            orphan_stream_registry: None,
            dispatched_msg_hashes: lru::LruCache::new(DEDUP_CACHE_CAPACITY),
        }
    }

    /// Compute a fast hash of the given bytes for dedup tracking.
    fn msg_hash(bytes: &[u8]) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        bytes.hash(&mut h);
        h.finish()
    }

    /// Returns true if this message was already dispatched (is a duplicate).
    /// Only used on the embedded-metadata-in-fragment-#1 path to suppress
    /// duplicates when the same metadata was already dispatched as a ShortMessage.
    fn is_duplicate_dispatch(&mut self, bytes: &[u8]) -> bool {
        self.dispatched_msg_hashes
            .put(Self::msg_hash(bytes), ())
            .is_some()
    }

    /// Sets the orphan stream registry for handling race conditions between
    /// stream fragments and metadata messages.
    ///
    /// This should be called by the node layer after connection establishment,
    /// before any messages are processed.
    pub fn set_orphan_stream_registry(
        &mut self,
        registry: std::sync::Arc<crate::operations::orphan_streams::OrphanStreamRegistry>,
    ) {
        self.orphan_stream_registry = Some(registry);
    }

    /// Sweep `streaming_handles` of entries that have not seen activity for
    /// longer than `streaming_handle_idle_timeout()`.
    ///
    /// Each swept entry has its handle cancelled (so any consumer blocked on
    /// `assemble()` / `stream().next()` wakes with `StreamError::Cancelled`)
    /// and its companion `streaming_registry` entry removed, releasing the
    /// underlying `Arc<LockFreeStreamBuffer>` pre-allocation.
    ///
    /// Called from the existing `timeout_check` tick (5 s cadence) in `recv()`.
    /// See issue #4079 for the leak scenarios this closes.
    fn sweep_idle_streaming_handles(&mut self) {
        let now_nanos = self.time_source.now_nanos();
        sweep_streaming_handles_inner(
            &mut self.streaming_handles,
            &self.streaming_registry,
            now_nanos,
            streaming_handle_idle_timeout(),
            self.remote_conn.remote_addr,
        );
    }

    #[instrument(name = "peer_connection", skip_all)]
    pub async fn send<D>(&mut self, data: D) -> Result
    where
        D: Serialize + Send + std::fmt::Debug,
    {
        // Serialize directly on async runtime - bincode is fast enough (<1ms for typical messages)
        // that spawn_blocking overhead isn't worth it. Using spawn_blocking here caused chronic
        // blocking pool thread churn under load (see issue #2310).
        let data = bincode::serialize(&data)?;
        if data.len() + SymmetricMessage::short_message_overhead() > MAX_DATA_SIZE {
            tracing::trace!(
                peer_addr = %self.remote_conn.remote_addr,
                total_size_bytes = data.len(),
                "Sending as stream"
            );
            self.outbound_stream(data).await;
        } else {
            tracing::trace!(
                peer_addr = %self.remote_conn.remote_addr,
                "Sending as short message"
            );
            self.outbound_short_message(data).await?;
        }
        Ok(())
    }

    #[instrument(name = "peer_connection", skip(self))]
    pub async fn recv(&mut self) -> Result<Vec<u8>> {
        // When SimulationTransportOpt is enabled, use relaxed timer intervals to reduce
        // scheduling overhead from ~900K to ~180K timer firings/sec across all connections.
        let in_simulation = crate::config::SimulationTransportOpt::is_enabled();
        let ack_interval = if in_simulation {
            SIMULATION_ACK_CHECK_INTERVAL
        } else {
            ACK_CHECK_INTERVAL
        };
        let resend_initial = if in_simulation {
            SIMULATION_RESEND_CHECK_INTERVAL
        } else {
            Duration::from_millis(10)
        };
        let resend_yield = if in_simulation {
            SIMULATION_RESEND_YIELD_DELAY
        } else {
            Duration::from_millis(2)
        };

        // listen for incoming messages or receipts or wait until is time to do anything else again
        let mut resend_check_sleep: Option<
            std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
        > = Some(self.time_source.sleep(resend_initial));

        let kill_connection_after = self.time_source.connection_idle_timeout();
        let kill_connection_after_nanos = kill_connection_after.as_nanos() as u64;

        // Check for timeout periodically
        let mut timeout_check =
            TimeSourceInterval::new(self.time_source.clone(), Duration::from_secs(5));

        // Background ACK timer - sends pending ACKs proactively
        // This prevents delays when there's no outgoing traffic to piggyback ACKs on
        // Use interval_at to delay the first tick - unlike the keep-alive task which can
        // block to skip its first tick, we're inside a select! loop so we delay instead
        let ack_start_nanos = self.time_source.now_nanos() + ack_interval.as_nanos() as u64;
        let mut ack_check =
            TimeSourceInterval::new_at(self.time_source.clone(), ack_start_nanos, ack_interval);

        // Rate update timer - updates TokenBucket rate based on BBR cwnd
        // This allows the token bucket to adapt to network conditions dynamically
        let rate_start_nanos = self.time_source.now_nanos() + ack_interval.as_nanos() as u64;
        let mut rate_update_check =
            TimeSourceInterval::new_at(self.time_source.clone(), rate_start_nanos, ack_interval);

        const FAILURE_TIME_WINDOW: Duration = Duration::from_secs(30);
        const FAILURE_TIME_WINDOW_NANOS: u64 = FAILURE_TIME_WINDOW.as_nanos() as u64;
        loop {
            // If resend_check_sleep was consumed by a previous select iteration
            // (via .take()) but the resend branch didn't win, refill with a short
            // delay. This prevents the resend branch from being immediately Ready
            // on every iteration during retransmission storms, which would starve
            // inbound packet processing and create an ACK-drop feedback loop.
            if resend_check_sleep.is_none() {
                resend_check_sleep = Some(self.time_source.sleep(resend_yield));
            }

            // tracing::trace!(remote = ?self.remote_conn.remote_addr, "waiting for inbound messages");
            // DST: Use deterministic_select! for fair but deterministic branch ordering
            crate::deterministic_select! {
                inbound = self.remote_conn.inbound_packet_recv.recv() => {
                    let packet_data = inbound.ok_or_else(|| TransportError::ConnectionClosed(self.remote_addr()))?;
                    self.last_received_nanos = self.time_source.now_nanos();

                    // Debug logging for intro packets
                    if packet_data.is_intro_packet() {
                        tracing::debug!(
                            peer_addr = %self.remote_conn.remote_addr,
                            packet_bytes = ?&packet_data.data()[..std::cmp::min(32, packet_data.data().len())], // First 32 bytes
                            packet_len = packet_data.data().len(),
                            "Received intro packet"
                        );
                    }

                    let Ok(decrypted) = packet_data.try_decrypt_sym(&self.remote_conn.inbound_symmetric_key).inspect_err(|error| {
                        tracing::debug!(
                            error = %error,
                            peer_addr = %self.remote_conn.remote_addr,
                            inbound_key = ?self.remote_conn.inbound_symmetric_key_bytes,
                            packet_len = packet_data.data().len(),
                            packet_first_bytes = ?&packet_data.data()[..std::cmp::min(32, packet_data.data().len())],
                            "Failed to decrypt packet, might be an intro packet or a partial packet"
                        );
                    }) else {
                        // Check if this is an intro packet (X25519 encrypted) by examining packet type
                        if packet_data.is_intro_packet() {
                            tracing::debug!(
                                peer_addr = %self.remote_conn.remote_addr,
                                "Attempting to decrypt intro packet"
                            );

                            // Try to decrypt as intro packet
                            match self.remote_conn.transport_secret_key.decrypt(packet_data.data()) {
                                Ok(_decrypted_intro) => {
                                    // Authenticated by asymmetric decryption (peer
                                    // restart-detection path). Meter as a normal
                                    // received packet — bytes still represent real
                                    // authenticated traffic on the wire (#3999).
                                    super::TRANSPORT_METRICS.record_packet_received(
                                        self.remote_conn.remote_addr,
                                        packet_data.data().len() as u64,
                                    );
                                    tracing::debug!(
                                        peer_addr = %self.remote_conn.remote_addr,
                                        "Successfully decrypted intro packet, sending ACK"
                                    );

                                    // Send ACK response for intro packet
                                    let ack_packet = SymmetricMessage::ack_ok(
                                        &self.remote_conn.outbound_symmetric_key,
                                        self.remote_conn.inbound_symmetric_key_bytes,
                                        self.remote_conn.remote_addr,
                                    );

                                    if let Ok(ack) = ack_packet {
                                        if let Err(send_err) = self.remote_conn
                                            .socket
                                            .send_to(ack.data(), self.remote_conn.remote_addr)
                                            .await
                                        {
                                            tracing::warn!(
                                                peer_addr = %self.remote_conn.remote_addr,
                                                error = ?send_err,
                                                "Failed to send ACK for intro packet"
                                            );
                                        } else {
                                            tracing::debug!(
                                                peer_addr = %self.remote_conn.remote_addr,
                                                "Successfully sent ACK for intro packet"
                                            );
                                        }
                                    } else {
                                        tracing::warn!(
                                            peer_addr = %self.remote_conn.remote_addr,
                                            "Failed to create ACK packet for intro"
                                        );
                                    }

                                    // Continue to next packet
                                    continue;
                                }
                                Err(decrypt_err) => {
                                    tracing::trace!(
                                        peer_addr = %self.remote_conn.remote_addr,
                                        error = ?decrypt_err,
                                        "Packet with intro type marker failed decryption"
                                    );
                                }
                            }
                        }
                        let now_nanos = self.time_source.now_nanos();
                        if let Some(first_failure_time_nanos) = self.first_failure_time_nanos {
                            if now_nanos.saturating_sub(first_failure_time_nanos) <= FAILURE_TIME_WINDOW_NANOS {
                                self.failure_count += 1;
                            } else {
                                // Reset the failure count and time window
                                self.failure_count = 1;
                                self.first_failure_time_nanos = Some(now_nanos);
                            }
                        } else {
                            // Initialize the failure count and time window
                            self.failure_count = 1;
                            self.first_failure_time_nanos = Some(now_nanos);
                        }

                        if self.failure_count > NAT_TRAVERSAL_MAX_ATTEMPTS {
                            tracing::warn!(
                                peer_addr = %self.remote_conn.remote_addr,
                                failure_count = self.failure_count,
                                max_attempts = NAT_TRAVERSAL_MAX_ATTEMPTS,
                                "Dropping connection due to repeated decryption failures"
                            );
                            // Drop the connection (implement the logic to drop the connection here)
                            return Err(TransportError::ConnectionClosed(self.remote_addr()));
                        }

                        tracing::trace!(
                            peer_addr = %self.remote_conn.remote_addr,
                            "Ignoring packet"
                        );
                        continue;
                    };
                    // Post-authentication wire-byte accounting for the local
                    // dashboard. Doing this before the symmetric-decrypt
                    // gate would let an attacker spoof UDP packets from many
                    // source IPs and inflate the cumulative + per-peer
                    // counters, see #3999.
                    super::TRANSPORT_METRICS.record_packet_received(
                        self.remote_conn.remote_addr,
                        packet_data.data().len() as u64,
                    );
                    let msg = SymmetricMessage::deser(decrypted.data()).unwrap();
                    let SymmetricMessage {
                        packet_id,
                        confirm_receipt,
                        payload,
                    } = msg;

                    // Log and handle keep-alive packets specifically
                    match &payload {
                        SymmetricMessagePayload::NoOp => {
                            if confirm_receipt.is_empty() {
                                let elapsed_nanos = self.time_source.now_nanos().saturating_sub(self.last_received_nanos);
                                tracing::debug!(
                                    target: "freenet_core::transport::keepalive_received",
                                    remote = ?self.remote_conn.remote_addr,
                                    packet_id,
                                    time_since_last_received_ms = Duration::from_nanos(elapsed_nanos).as_millis(),
                                    "Received NoOp keep-alive packet (no receipts)"
                                );
                            } else {
                                tracing::debug!(
                                    target: "freenet_core::transport::keepalive_received",
                                    remote = ?self.remote_conn.remote_addr,
                                    packet_id,
                                    receipt_count = confirm_receipt.len(),
                                    "Received NoOp receipt packet"
                                );
                            }
                        }
                        SymmetricMessagePayload::Ping { sequence } => {
                            tracing::debug!(
                                target: "freenet_core::transport::keepalive_received",
                                remote = ?self.remote_conn.remote_addr,
                                packet_id,
                                ping_sequence = sequence,
                                "Received Ping, sending Pong response"
                            );
                            // Immediately respond with Pong
                            if let Err(e) = self.send_pong(*sequence).await {
                                tracing::warn!(
                                    target: "freenet_core::transport::keepalive_received",
                                    remote = ?self.remote_conn.remote_addr,
                                    ping_sequence = sequence,
                                    error = ?e,
                                    "Failed to send Pong response"
                                );
                            }
                        }
                        SymmetricMessagePayload::Pong { sequence } => {
                            tracing::debug!(
                                target: "freenet_core::transport::keepalive_received",
                                remote = ?self.remote_conn.remote_addr,
                                packet_id,
                                pong_sequence = sequence,
                                "Received Pong, confirming bidirectional liveness"
                            );
                            // Remove the corresponding ping from pending set and,
                            // if found, sample the round-trip time. The keep-alive
                            // cycle runs on every connection regardless of stream
                            // traffic, so this keeps RTT statistics populated for
                            // quiet, long-lived connections that rarely (or never)
                            // complete a stream transfer (#4000). Without it,
                            // RTT was only sampled at stream completion and quiet
                            // connections contributed zero samples.
                            let removed_send_nanos = {
                                let mut pending = self.pending_pings.write();
                                let removed = pending.remove(sequence);
                                if removed.is_some() {
                                    tracing::trace!(
                                        target: "freenet_core::transport::keepalive_received",
                                        remote = ?self.remote_conn.remote_addr,
                                        pong_sequence = sequence,
                                        remaining_pending = pending.len(),
                                        "Removed acknowledged ping from pending set"
                                    );
                                }
                                removed
                            };
                            if let Some(send_nanos) = removed_send_nanos {
                                // The ping timestamp was recorded with this same
                                // `time_source` (the keep-alive task clones it), so
                                // the subtraction is over one monotonic clock.
                                // `saturating_sub` is defensive against any skew
                                // yielding a 0 sample rather than underflowing.
                                let rtt_nanos = self
                                    .time_source
                                    .now_nanos()
                                    .saturating_sub(send_nanos);
                                let rtt_us = rtt_nanos / 1_000;
                                if rtt_us > 0 {
                                    super::TRANSPORT_METRICS.record_rtt_sample(rtt_us);
                                }
                            }
                        }
                        SymmetricMessagePayload::AckConnection { .. } | SymmetricMessagePayload::ShortMessage { .. } | SymmetricMessagePayload::StreamFragment { .. } => {}
                    }

                    {
                        tracing::trace!(
                            peer_addr = %self.remote_conn.remote_addr,
                            packet_id,
                            confirm_receipts_count = confirm_receipt.len(),
                            "Received inbound packet with confirmations"
                        );
                    }

                    let current_time_nanos = self.time_source.now_nanos();
                    let message_confirmation_timeout_nanos = MESSAGE_CONFIRMATION_TIMEOUT.as_nanos() as u64;
                    let should_send_receipts = if current_time_nanos > self.last_packet_report_time_nanos + message_confirmation_timeout_nanos {
                        let elapsed_nanos = current_time_nanos.saturating_sub(self.last_packet_report_time_nanos);
                        tracing::trace!(
                            peer_addr = %self.remote_conn.remote_addr,
                            elapsed_ms = Duration::from_nanos(elapsed_nanos).as_millis(),
                            timeout_ms = MESSAGE_CONFIRMATION_TIMEOUT.as_millis(),
                            "Timeout reached, should send receipts"
                        );
                        self.last_packet_report_time_nanos = current_time_nanos;
                        true
                    } else {
                        false
                    };

                    // Process ACKs and update BBR congestion controller
                    let (ack_info, _loss_rate) = self.remote_conn
                        .sent_tracker
                        .lock()
                        .report_received_receipts(&confirm_receipt);

                    // Feed ACK info to congestion controller for window adjustment
                    // All ACKs decrement flightsize, but only non-retransmitted packets
                    // update RTT estimation (Karn's algorithm)
                    // For BBR: passing the delivery token enables accurate bandwidth estimation
                    for (rtt_sample_opt, packet_size, token) in ack_info {
                        match rtt_sample_opt {
                            Some(rtt_sample) => {
                                // Shadow telemetry (issue #4074): record the RTT sample
                                // into the rolling per-peer window before feeding the
                                // congestion controller. Recording is observation-only
                                // and does not affect congestion control behavior.
                                self.remote_conn.rolling_rtt_stats.record(rtt_sample);
                                // Normal packet: full RTT processing + flightsize decrement
                                // For BBR: token enables accurate delivery rate computation
                                self.remote_conn.congestion_controller.on_ack_with_token(
                                    rtt_sample,
                                    packet_size,
                                    token,
                                );
                            }
                            None => {
                                // Retransmitted packet: only decrement flightsize (no RTT update)
                                self.remote_conn.congestion_controller.on_ack_without_rtt(packet_size);
                            }
                        }
                    }

                    let report_result = self.received_tracker.report_received_packet(packet_id);
                    match (report_result, should_send_receipts) {
                        (ReportResult::QueueFull, _) | (_, true) => {
                            let receipts = self.received_tracker.get_receipts();
                            if !receipts.is_empty() {
                                if let Err(e) = self.noop(receipts).await {
                                    if e.is_transient_send_failure() {
                                        tracing::warn!(
                                            peer_addr = %self.remote_conn.remote_addr,
                                            "ACK noop send failed, will retry"
                                        );
                                    } else {
                                        return Err(e);
                                    }
                                }
                            }
                        },
                        (ReportResult::Ok, _) => {}
                        (ReportResult::AlreadyReceived, _) => {
                            tracing::trace!(
                                peer_addr = %self.remote_conn.remote_addr,
                                packet_id,
                                "Already received packet"
                            );
                            continue;
                        }
                    }
                    if let Some(msg) = self.process_inbound(payload).await.map_err(|error| {
                        tracing::error!(
                            error = %error,
                            packet_id,
                            peer_addr = %self.remote_conn.remote_addr,
                            "Error processing inbound packet"
                        );
                        error
                    })? {
                        // Record inbound bytes for non-streamed (short) messages
                        super::TRANSPORT_METRICS.record_inbound_completed(msg.len() as u64);
                        tracing::trace!(
                            peer_addr = %self.remote_conn.remote_addr,
                            packet_id,
                            "Returning full stream message"
                        );
                        return Ok(msg);
                    }
                },
                inbound_stream = self.inbound_stream_futures.next(), if !self.inbound_stream_futures.is_empty() => {
                    let Some(res) = inbound_stream else {
                        tracing::error!(
                            peer_addr = %self.remote_conn.remote_addr,
                            "Unexpected no-stream from ongoing_inbound_streams"
                        );
                        continue
                    };
                    let Ok((stream_id, msg)) = res.map_err(|e| TransportError::Other(e.into()))? else {
                        tracing::error!(
                            peer_addr = %self.remote_conn.remote_addr,
                            "Unexpected error from ongoing_inbound_streams"
                        );
                        // TODO: may leave orphan stream recvs hanging around in this case
                        continue;
                    };
                    self.inbound_streams.remove(&stream_id);
                    // Also clean up streaming handle and registry
                    self.streaming_handles.remove(&stream_id);
                    self.streaming_registry.remove(stream_id);
                    // Record inbound bytes for dashboard metrics
                    let bytes_received = msg.len() as u64;
                    super::TRANSPORT_METRICS.record_inbound_completed(bytes_received);
                    tracing::trace!(
                        peer_addr = %self.remote_conn.remote_addr,
                        stream_id = %stream_id,
                        bytes = bytes_received,
                        "Stream finished"
                    );
                    return Ok(msg);
                },
                outbound_stream = self.outbound_stream_futures.next(), if !self.outbound_stream_futures.is_empty() => {
                    let Some(res) = outbound_stream else {
                        tracing::error!(
                            peer_addr = %self.remote_conn.remote_addr,
                            "Unexpected no-stream from ongoing_outbound_streams"
                        );
                        continue
                    };
                    // Handle task join error
                    let transfer_result = res.map_err(|e| TransportError::Other(e.into()))?;
                    // Handle transfer error or get stats
                    match transfer_result {
                        Ok(stats) => {
                            tracing::trace!(
                                peer_addr = %self.remote_conn.remote_addr,
                                stream_id = stats.stream_id,
                                bytes = stats.bytes_transferred,
                                elapsed_ms = stats.elapsed.as_millis(),
                                throughput_kbps = stats.avg_throughput_bps() / 1024,
                                "Outbound stream completed with stats"
                            );
                            // Report to global metrics for periodic telemetry snapshots
                            super::TRANSPORT_METRICS.record_transfer_completed(&stats);
                        }
                        Err(e) if e.is_transient_send_failure() => {
                            tracing::warn!(
                                peer_addr = %self.remote_conn.remote_addr,
                                error = %e,
                                "Outbound stream send failed, operation layer will timeout and retry"
                            );
                        }
                        Err(e) => return Err(e),
                    }
                },
                _ = timeout_check.tick() => {
                    let now_nanos = self.time_source.now_nanos();
                    let elapsed_nanos = now_nanos.saturating_sub(self.last_received_nanos);
                    let elapsed = Duration::from_nanos(elapsed_nanos);

                    // Check for traditional timeout (no packets received)
                    if elapsed_nanos > kill_connection_after_nanos {
                        tracing::warn!(
                            target: "freenet_core::transport::keepalive_timeout",
                            remote = ?self.remote_conn.remote_addr,
                            elapsed_seconds = elapsed.as_secs_f64(),
                            timeout_threshold_secs = kill_connection_after.as_secs(),
                            "CONNECTION TIMEOUT - no packets received for {:.8}s",
                            elapsed.as_secs_f64()
                        );

                        // Diagnostic: check keepalive task state at timeout.
                        // "still running" is the normal case — pings are being sent
                        // but the remote isn't responding (NAT mapping expired, peer
                        // crashed, etc.). "already finished" means the socket errored
                        // before the idle timeout fired (rare).
                        if let Some(ref handle) = self.keep_alive_handle {
                            let task_state = if handle.is_finished() { "finished" } else { "running" };
                            tracing::debug!(
                                target: "freenet_core::transport::keepalive_timeout",
                                remote = ?self.remote_conn.remote_addr,
                                keepalive_task = task_state,
                                "Connection timed out, keepalive task was {task_state}"
                            );
                        }

                        return Err(TransportError::ConnectionClosed(self.remote_addr()));
                    }

                    // Clean up stale pings older than kill_connection_after to prevent unbounded growth.
                    // This handles edge cases where pong responses are lost but connection stays alive.
                    {
                        let mut pending = self.pending_pings.write();
                        let stale_threshold_nanos = now_nanos.saturating_sub(kill_connection_after_nanos);
                        pending.retain(|_, sent_at_nanos| *sent_at_nanos > stale_threshold_nanos);
                    }

                    // Re-read count after cleanup
                    let pending_ping_count = self.pending_pings.read().len();

                    // Log unanswered pings for diagnostics but do NOT kill the connection.
                    // The idle timeout (connection_idle_timeout, typically 120s) is the sole
                    // connection liveness check. A separate bidirectional liveness check was
                    // previously here but caused premature connection drops in Docker NAT
                    // environments where small ping/pong UDP packets are lost while larger
                    // data packets succeed, or where gateway connections become idle because
                    // data routes through the P2P mesh instead.
                    if pending_ping_count > MAX_UNANSWERED_PINGS {
                        tracing::debug!(
                            target: "freenet_core::transport::keepalive_health",
                            remote = ?self.remote_conn.remote_addr,
                            pending_pings = pending_ping_count,
                            max_unanswered = MAX_UNANSWERED_PINGS,
                            time_since_last_received_secs = elapsed.as_secs_f64(),
                            "Many unanswered pings ({} > {}), relying on idle timeout for liveness",
                            pending_ping_count,
                            MAX_UNANSWERED_PINGS
                        );
                    }

                    let remaining_nanos = kill_connection_after_nanos.saturating_sub(elapsed_nanos);
                    tracing::trace!(
                        target: "freenet_core::transport::keepalive_health",
                        remote = ?self.remote_conn.remote_addr,
                        elapsed_seconds = elapsed.as_secs_f64(),
                        remaining_seconds = Duration::from_nanos(remaining_nanos).as_secs_f64(),
                        pending_pings = pending_ping_count,
                        "Connection health check - still alive"
                    );

                    // Drop streaming handles idle past streaming_handle_idle_timeout().
                    // Without this sweep, a stalled upstream sender (#4079)
                    // pins the per-stream Arc<LockFreeStreamBuffer> for the
                    // life of the connection. Reactive eviction at the
                    // fragment-arrival sites cannot fire when no more
                    // fragments are coming.
                    self.sweep_idle_streaming_handles();
                },
                // The .take() consumes the sleep future; the unwrap_or(ready()) fallback
                // should be unreachable since the top-of-loop guard always refills it,
                // but is kept as a defensive measure.
                _ = async { resend_check_sleep.take().unwrap_or_else(|| Box::pin(std::future::ready(()))).await } => {
                    // Bound retransmissions per iteration to prevent monopolizing the
                    // select loop. Remaining resends are deferred by a short sleep
                    // (see top of loop) so inbound branches can interleave.
                    const MAX_RESENDS_PER_ITERATION: usize = 4;
                    let mut resend_count = 0;
                    loop {
                        tracing::trace!(
                            peer_addr = %self.remote_conn.remote_addr,
                            "Checking for resends"
                        );
                        let maybe_resend = self.remote_conn
                            .sent_tracker
                            .lock()
                            .get_resend();
                        // Extract (idx, packet) from the resend action, applying
                        // action-specific side effects (congestion notification, logging).
                        // Abandon releases flight size and is handled inline (it neither
                        // re-sends nor re-registers). See #4345.
                        let (idx, packet) = match maybe_resend {
                            ResendAction::WaitUntil(deadline_nanos) => {
                                resend_check_sleep = Some(self.time_source.sleep_until(deadline_nanos));
                                break;
                            }
                            ResendAction::Abandon { packet_id, payload_len } => {
                                // The packet was retransmitted MAX_PACKET_RETRANSMITS times
                                // with no ACK and is now permanently lost. Release its bytes
                                // from flight size — this is the give-up that drains a flight
                                // size pinned by a never-ACKed packet (issue #4345). The
                                // tracker already dropped it, so we neither re-send nor
                                // re-register; continue draining the resend queue.
                                self.remote_conn
                                    .congestion_controller
                                    .release_flightsize(payload_len);
                                tracing::debug!(
                                    peer_addr = %self.remote_conn.remote_addr,
                                    packet_id,
                                    payload_len,
                                    "Resend abandoned packet — released flight size (#4345)"
                                );
                                resend_count += 1;
                                if resend_count >= MAX_RESENDS_PER_ITERATION {
                                    resend_check_sleep = Some(self.time_source.sleep(resend_yield));
                                    break;
                                }
                                continue;
                            }
                            ResendAction::Resend(idx, packet) => {
                                // Notify congestion controller of packet loss (timeout-based
                                // retransmission). The packet stays in flight — it is
                                // immediately re-sent and re-registered below — so flight size
                                // is unchanged; on_timeout only applies the cwnd loss response.
                                self.remote_conn.congestion_controller.on_timeout();
                                (idx, packet)
                            }
                            ResendAction::TlpProbe(idx, packet) => {
                                // TLP (Tail Loss Probe) - send probe to detect tail loss earlier.
                                // Unlike RTO, TLP does NOT call on_timeout() because it's speculative.
                                // If the probe gets an ACK, no loss occurred. If not, RTO will fire later.
                                tracing::trace!(
                                    peer_addr = %self.remote_conn.remote_addr,
                                    packet_id = idx,
                                    "Sending TLP probe"
                                );
                                (idx, packet)
                            }
                        };

                        // Common path for both Resend and TlpProbe: send the packet,
                        // re-register for ACK tracking, and enforce per-iteration limit.
                        match self.remote_conn
                            .socket
                            .send_to(&packet, self.remote_conn.remote_addr)
                            .await
                        {
                            Ok(_) => {
                                // Refresh the packet's send timestamp to the actual
                                // post-send instant. Since #4345 get_resend KEEPS the
                                // packet in the tracker across a resend, so this is an
                                // in-place refresh — BUT only if the packet is still
                                // tracked. A concurrent drop_stream (spawned abort task)
                                // may have removed and released it while we were awaiting
                                // send_to; refresh_sent_packet then no-ops instead of
                                // resurrecting it as a Control zombie (which a later
                                // ACK/abandon would double-release). When the packet IS
                                // still tracked, flight size is unchanged: it never left
                                // flight (on_timeout did not decrement), so no on_send /
                                // re-add runs. See refresh_sent_packet's rustdoc.
                                //
                                // `false` (the deliberate no-op-on-drop) is the
                                // resurrection-safe path, not an error — surface it at
                                // trace level rather than silently discarding the bool.
                                let still_tracked = self
                                    .remote_conn
                                    .sent_tracker
                                    .lock()
                                    .refresh_sent_packet(idx, packet, None);
                                if !still_tracked {
                                    tracing::trace!(
                                        peer_addr = %self.remote_conn.remote_addr,
                                        packet_id = idx,
                                        "Resent packet was dropped (stream aborted) before \
                                         re-registration — refresh no-op (#4345)"
                                    );
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    peer_addr = %self.remote_conn.remote_addr,
                                    packet_id = idx,
                                    error = %e,
                                    "Resend send failed, will retry on next RTO"
                                );
                                // Refresh only if still tracked — RTO then retries it.
                                // If a concurrent drop_stream already removed it, this
                                // no-ops (the stream is being torn down anyway), avoiding
                                // a resurrected zombie. See refresh_sent_packet's rustdoc.
                                // The bool is intentionally discarded: on this error path
                                // we're breaking out regardless of whether the packet was
                                // still tracked.
                                let _ = self
                                    .remote_conn
                                    .sent_tracker
                                    .lock()
                                    .refresh_sent_packet(idx, packet, None);
                                break;
                            }
                        }
                        resend_count += 1;
                        if resend_count >= MAX_RESENDS_PER_ITERATION {
                            resend_check_sleep = Some(self.time_source.sleep(resend_yield));
                            break;
                        }
                    }
                },
                // Background ACK timer - proactively send pending ACKs
                // This prevents ACK delays when there's no outgoing traffic to piggyback on
                _ = ack_check.tick() => {
                    let receipts = self.received_tracker.get_receipts();
                    if !receipts.is_empty() {
                        tracing::trace!(
                            peer_addr = %self.remote_conn.remote_addr,
                            receipt_count = receipts.len(),
                            "Background ACK timer: sending pending receipts"
                        );
                        if let Err(e) = self.noop(receipts).await {
                            if e.is_transient_send_failure() {
                                tracing::warn!(
                                    peer_addr = %self.remote_conn.remote_addr,
                                    "Background ACK send failed, will retry next tick"
                                );
                            } else {
                                return Err(e);
                            }
                        }
                    }
                },
                // Rate update timer - update TokenBucket rate based on BBR cwnd
                // RTT-adaptive: only update if at least one RTT has elapsed since last update
                _ = rate_update_check.tick() => {
                    let now_nanos = self.time_source.now_nanos();

                    // Use congestion controller's base delay for rate calculation
                    // Fallback to min_rtt only if base_delay is not yet established
                    let base_delay = self.remote_conn.congestion_controller.base_delay();
                    let rtt = if base_delay.is_zero() {
                        self.remote_conn.sent_tracker.lock().min_rtt()
                    } else {
                        base_delay
                    };

                    // RTT-adaptive update: only update if at least one RTT has elapsed since last update
                    // This prevents updating too frequently at high RTT or too slowly at low RTT
                    let should_update = match self.last_rate_update_nanos {
                        None => true, // First update
                        Some(last_update_nanos) => {
                            let elapsed_nanos = now_nanos.saturating_sub(last_update_nanos);
                            // Update if at least one RTT has passed, with bounds (50ms min, 500ms max)
                            let min_interval = rtt.max(Duration::from_millis(50)).min(Duration::from_millis(500));
                            elapsed_nanos >= min_interval.as_nanos() as u64
                        }
                    };

                    if should_update {
                        let cc_rate = self.remote_conn.congestion_controller.current_rate(rtt) as u64;
                        let cwnd = self.remote_conn.congestion_controller.current_cwnd();
                        let queuing_delay = self.remote_conn.congestion_controller.queuing_delay();

                        // Apply global bandwidth limit if configured
                        // Take minimum of congestion controller rate and global fair-share rate
                        let (new_rate, global_limit) = if let Some(ref global) =
                            self.remote_conn.global_bandwidth
                        {
                            let global_rate = global.current_per_connection_rate() as u64;
                            (cc_rate.min(global_rate), Some(global_rate))
                        } else {
                            (cc_rate, None)
                        };

                        // Calculate time since last update for debugging RTT-adaptive timing
                        let since_last_update_ms = self.last_rate_update_nanos
                            .map(|last| Duration::from_nanos(now_nanos.saturating_sub(last)).as_millis())
                            .unwrap_or(0);

                        self.remote_conn.token_bucket.set_rate(new_rate as usize);
                        self.last_rate_update_nanos = Some(now_nanos);

                        tracing::debug!(
                            peer_addr = %self.remote_conn.remote_addr,
                            // Rate control
                            new_rate_bytes_per_sec = new_rate,
                            new_rate_mbps = (new_rate as f64) / 1_000_000.0,
                            cc_rate_mbps = (cc_rate as f64) / 1_000_000.0,
                            global_limit_mbps = global_limit.map(|r| (r as f64) / 1_000_000.0),
                            // Congestion controller state
                            cwnd_bytes = cwnd,
                            cwnd_packets = cwnd / MAX_DATA_SIZE,
                            // Delay measurements
                            base_delay_ms = base_delay.as_millis(),
                            rtt_ms = rtt.as_millis(),
                            queuing_delay_ms = queuing_delay.as_millis(),
                            // Derived metrics
                            since_last_update_ms = since_last_update_ms,
                            "Congestion control metrics (RTT-adaptive rate update)"
                        );
                    }
                }
            }
        }
    }

    /// Returns the external address of the peer holding this connection.
    #[allow(dead_code)]
    pub fn my_address(&self) -> Option<SocketAddr> {
        self.remote_conn.my_address
    }

    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_conn.remote_addr
    }

    /// Remote peer's negotiated protocol version, if known (None on joiner->gateway connections).
    pub fn remote_version(&self) -> Option<(u8, u8, u16)> {
        self.remote_conn.remote_protoc_version
    }

    /// Returns a handle for accessing an inbound stream incrementally.
    ///
    /// This provides a `futures::Stream` interface for consuming fragments
    /// as they arrive, without waiting for complete reassembly. Useful for
    /// large transfers where you want to process data incrementally.
    ///
    /// # Arguments
    ///
    /// * `stream_id` - The ID of the stream to access
    ///
    /// # Returns
    ///
    /// * `Some(StreamHandle)` - If the stream exists
    /// * `None` - If no stream with that ID is registered
    ///
    /// # Example
    ///
    /// ```ignore
    /// if let Some(handle) = peer_conn.recv_stream_handle(stream_id) {
    ///     let mut stream = handle.stream();
    ///     while let Some(chunk) = stream.next().await {
    ///         process_chunk(chunk?);
    ///     }
    /// }
    /// ```
    #[allow(dead_code)]
    pub(crate) fn recv_stream_handle(
        &self,
        stream_id: StreamId,
    ) -> Option<streaming::StreamHandle> {
        self.streaming_handles
            .get(&stream_id)
            .map(|(handle, _)| handle.clone())
    }

    /// Returns the streaming registry for this connection.
    ///
    /// The registry allows external code to:
    /// - Look up streams by ID
    /// - Subscribe to new stream notifications
    /// - Get handles for incremental stream consumption
    pub fn streaming_registry(&self) -> Arc<streaming::StreamRegistry> {
        Arc::clone(&self.streaming_registry)
    }

    async fn process_inbound(
        &mut self,
        payload: SymmetricMessagePayload,
    ) -> Result<Option<Vec<u8>>> {
        use SymmetricMessagePayload::*;
        match payload {
            ShortMessage { payload } => {
                let bytes = payload.to_vec();
                // Record this message's hash so that if the same metadata arrives
                // embedded in a stream fragment, it will be suppressed as a duplicate.
                // We always dispatch ShortMessages — dedup only suppresses the
                // redundant embedded-metadata copy, never the ShortMessage itself.
                self.dispatched_msg_hashes.put(Self::msg_hash(&bytes), ());
                Ok(Some(bytes))
            }
            AckConnection { result: Err(cause) } => {
                Err(TransportError::ConnectionEstablishmentFailure { cause })
            }
            AckConnection { result: Ok(_) } => {
                let packet = SymmetricMessage::ack_ok(
                    &self.remote_conn.outbound_symmetric_key,
                    self.remote_conn.inbound_symmetric_key_bytes,
                    self.remote_conn.remote_addr,
                )?;
                if let Err(e) = self
                    .remote_conn
                    .socket
                    .send_to(packet.data(), self.remote_conn.remote_addr)
                    .await
                {
                    tracing::warn!(
                        peer_addr = %self.remote_conn.remote_addr,
                        error = %e,
                        "AckOk send failed, peer will retransmit if needed"
                    );
                }
                Ok(None)
            }
            StreamFragment {
                stream_id,
                total_length_bytes,
                fragment_number,
                payload,
                metadata_bytes,
            } => {
                let now_nanos = self.time_source.now_nanos();
                // Push to streaming handle for incremental access (Phase 1 streaming)
                if let Some((streaming_handle, last_activity_nanos)) =
                    self.streaming_handles.get_mut(&stream_id)
                {
                    // Existing streaming handle - push fragment.
                    let push_result =
                        streaming_handle.push_fragment(fragment_number, payload.clone());
                    // Refresh `last_activity_nanos` ONLY on real forward
                    // progress (`Ok(true)` = new fragment inserted).
                    // Duplicates (`Ok(false)`) and invalid fragments
                    // (`Err(InvalidFragment)`) MUST NOT extend the sweep
                    // deadline; otherwise a peer can keep a dead stream alive
                    // indefinitely by replaying the same fragment every <30s,
                    // defeating the bound issue #4079 enforces. This honors
                    // "Cleanup exemptions MUST be time-bounded" — the
                    // exemption must not be refreshable by no-progress traffic.
                    if matches!(push_result, Ok(true)) {
                        *last_activity_nanos = now_nanos;
                    }
                    if let Err(e) = push_result {
                        if matches!(e, streaming::StreamError::Cancelled) {
                            // Stream was cancelled (e.g., transaction timeout). Remove handle
                            // to stop processing further fragments for this stream.
                            self.streaming_handles.remove(&stream_id);
                            self.streaming_registry.remove(stream_id);
                            tracing::debug!(
                                peer_addr = %self.remote_conn.remote_addr,
                                stream_id = %stream_id,
                                fragment_number,
                                "Stream cancelled, removed from handles and registry"
                            );
                        } else {
                            tracing::warn!(
                                peer_addr = %self.remote_conn.remote_addr,
                                stream_id = %stream_id,
                                fragment_number,
                                error = %e,
                                "Failed to push fragment to streaming handle"
                            );
                        }
                    }
                } else {
                    // New stream - register with streaming registry
                    let streaming_handle = self
                        .streaming_registry
                        .register(stream_id, total_length_bytes);
                    if let Err(e) = streaming_handle.push_fragment(fragment_number, payload.clone())
                    {
                        if matches!(e, streaming::StreamError::Cancelled) {
                            // Stream was already cancelled, don't register it
                            tracing::debug!(
                                peer_addr = %self.remote_conn.remote_addr,
                                stream_id = %stream_id,
                                fragment_number,
                                "New stream already cancelled, not registering"
                            );
                            // Skip orphan registration and handle insertion
                        } else {
                            tracing::warn!(
                                peer_addr = %self.remote_conn.remote_addr,
                                stream_id = %stream_id,
                                fragment_number,
                                error = %e,
                                "Failed to push first fragment to streaming handle"
                            );
                            // Still register the handle for non-cancellation errors
                            if let Some(orphan_registry) = &self.orphan_stream_registry {
                                orphan_registry.register_orphan(
                                    self.remote_conn.remote_addr,
                                    stream_id,
                                    streaming_handle.clone(),
                                );
                                tracing::trace!(
                                    peer_addr = %self.remote_conn.remote_addr,
                                    stream_id = %stream_id,
                                    "Registered stream as orphan for operations layer"
                                );
                            } else if stream_id.is_operations_stream() {
                                tracing::error!(
                                    peer_addr = %self.remote_conn.remote_addr,
                                    stream_id = %stream_id,
                                    "Operations stream fragment arrived but orphan_stream_registry is None! \
                                     This will cause claim_or_wait() to timeout. Check connection setup."
                                );
                            }
                            self.streaming_handles
                                .insert(stream_id, (streaming_handle, now_nanos));
                        }
                    } else {
                        // Phase 4: Register with orphan stream registry for race condition handling.
                        // This allows operations layer to claim the stream when metadata arrives,
                        // even if the stream fragments arrived first.
                        if let Some(orphan_registry) = &self.orphan_stream_registry {
                            orphan_registry.register_orphan(
                                self.remote_conn.remote_addr,
                                stream_id,
                                streaming_handle.clone(),
                            );
                            tracing::trace!(
                                peer_addr = %self.remote_conn.remote_addr,
                                stream_id = %stream_id,
                                "Registered stream as orphan for operations layer"
                            );
                        } else if stream_id.is_operations_stream() {
                            // CRITICAL: Operations-level streams require orphan registry for claim_or_wait().
                            // If registry is None, the stream won't be registered and the operations
                            // layer will timeout waiting for it. This indicates a bug in connection setup.
                            tracing::error!(
                                peer_addr = %self.remote_conn.remote_addr,
                                stream_id = %stream_id,
                                "Operations stream fragment arrived but orphan_stream_registry is None! \
                                 This will cause claim_or_wait() to timeout. Check connection setup."
                            );
                        }

                        self.streaming_handles
                            .insert(stream_id, (streaming_handle, now_nanos));
                    }
                }

                // Operations-level streams skip the legacy InboundStream path.
                // Their bytes are a streaming payload (e.g. GetStreamingPayload), not a
                // NetMessage, so decode_msg() would fail and close the connection.
                if stream_id.is_operations_stream() {
                    // If fragment #1 carries embedded metadata bytes (fix #2757),
                    // dispatch them as if they were a ShortMessage so the operations
                    // layer processes the metadata even if the separate metadata
                    // message was lost over UDP.
                    if let Some(meta) = metadata_bytes {
                        let bytes = meta.to_vec();
                        if self.is_duplicate_dispatch(&bytes) {
                            tracing::debug!(
                                peer_addr = %self.remote_conn.remote_addr,
                                stream_id = %stream_id,
                                "Suppressing duplicate embedded metadata (already dispatched via ShortMessage)"
                            );
                        } else {
                            tracing::debug!(
                                peer_addr = %self.remote_conn.remote_addr,
                                stream_id = %stream_id,
                                meta_len = bytes.len(),
                                "Dispatching embedded metadata from fragment #1"
                            );
                            return Ok(Some(bytes));
                        }
                    }
                    tracing::trace!(
                        peer_addr = %self.remote_conn.remote_addr,
                        stream_id = %stream_id,
                        fragment_number,
                        "Operations stream fragment - skipping legacy InboundStream path"
                    );
                    return Ok(None);
                }

                // Legacy path: push to mpsc channel for existing recv() behavior.
                //
                // This is one of the rare in-tree exceptions to the
                // `.send().await`-in-recv-loop ban from
                // `.claude/rules/channel-safety.md`. The receiver here is
                // the freenet-spawned `recv_stream` reassembly task in
                // `inbound_stream_futures` — it is NOT an external client
                // consumer, and it has no upstream dependency on this
                // recv loop. The cascading-backpressure deadlock pattern
                // the rule prevents (slow external consumer stalls our
                // event loop, which feeds another channel the consumer
                // is waiting on) cannot form here because:
                //   - the consumer is internal and same-runtime, with
                //     bounded per-fragment work (BTreeMap insert + Vec extend),
                //   - blocking is scoped per-connection (each peer has
                //     its own recv loop), so it cannot starve other peers,
                //   - the only effect of transient backpressure is brief
                //     ACK/keepalive delay for THIS peer until `recv_stream`
                //     drains.
                //
                // Switching to `try_send` was attempted in #3961 and reverted
                // on review (Codex P1, skeptical reviewer M2): turning
                // transient executor pressure into `ConnectionClosed` would
                // abort otherwise-healthy large transfers whenever more than
                // 64 fragments back up before `recv_stream` is scheduled.
                if let Some(sender) = self.inbound_streams.get(&stream_id) {
                    sender
                        .send((fragment_number, payload))
                        .await
                        .map_err(|_| TransportError::ConnectionClosed(self.remote_addr()))?;
                    tracing::trace!(
                        peer_addr = %self.remote_conn.remote_addr,
                        stream_id = %stream_id,
                        fragment_number,
                        "Fragment pushed to existing stream"
                    );
                } else {
                    let (sender, receiver) = mpsc::channel(64);
                    tracing::trace!(
                        peer_addr = %self.remote_conn.remote_addr,
                        stream_id = %stream_id,
                        fragment_number,
                        "New stream"
                    );
                    self.inbound_streams.insert(stream_id, sender);
                    let mut stream = inbound_stream::InboundStream::new(total_length_bytes);
                    if let Some(msg) = stream.push_fragment(fragment_number, payload) {
                        self.inbound_streams.remove(&stream_id);
                        // Also remove from streaming handles and registry when complete
                        self.streaming_handles.remove(&stream_id);
                        self.streaming_registry.remove(stream_id);
                        tracing::trace!(
                            peer_addr = %self.remote_conn.remote_addr,
                            stream_id = %stream_id,
                            fragment_number,
                            "Stream finished"
                        );
                        return Ok(Some(msg));
                    }
                    self.inbound_stream_futures.push(GlobalExecutor::spawn(
                        inbound_stream::recv_stream(stream_id, receiver, stream),
                    ));
                }
                Ok(None)
            }
            NoOp => Ok(None),
            // Ping and Pong are handled earlier in recv() before process_inbound is called
            Ping { .. } | Pong { .. } => Ok(None),
        }
    }

    #[inline]
    async fn noop(&mut self, receipts: Vec<u32>) -> Result<()> {
        // Get token before sending (captures send-time state for BBR)
        // Estimate noop packet size (~50 bytes typically)
        let token = self
            .remote_conn
            .congestion_controller
            .on_send_with_token(50);
        packet_sending(
            self.remote_conn.remote_addr,
            &self.remote_conn.socket,
            self.remote_conn
                .last_packet_id
                .fetch_add(1, std::sync::atomic::Ordering::Release),
            &self.remote_conn.outbound_symmetric_key,
            receipts,
            (),
            &self.remote_conn.sent_tracker,
            50,
            token,
            PacketStream::Control,
        )
        .await
    }

    /// Send a Pong response to a received Ping.
    /// This confirms bidirectional liveness to the remote peer.
    async fn send_pong(&mut self, sequence: u64) -> Result<()> {
        let packet_id = self
            .remote_conn
            .last_packet_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let pong_packet = SymmetricMessage::serialize_msg_to_packet_data(
            packet_id,
            SymmetricMessagePayload::Pong { sequence },
            &self.remote_conn.outbound_symmetric_key,
            vec![], // No receipts for pong
        )?
        .prepared_send();

        match self
            .remote_conn
            .socket
            .send_to(&pong_packet, self.remote_conn.remote_addr)
            .await
        {
            Ok(_) => {
                // Phase 1.6 (#4074): Pong bypasses `packet_sending`, so
                // classify it as must-flow here. Observation only.
                super::shadow_demand::record_outbound(
                    super::shadow_demand::OutboundClass::MustFlow,
                    pong_packet.len(),
                );
                tracing::trace!(
                    peer_addr = %self.remote_conn.remote_addr,
                    packet_id,
                    pong_sequence = sequence,
                    "Pong packet sent"
                );
            }
            Err(e) => {
                tracing::warn!(
                    peer_addr = %self.remote_conn.remote_addr,
                    error = %e,
                    pong_sequence = sequence,
                    "Pong send failed, keepalive will handle liveness"
                );
            }
        }

        Ok(())
    }

    #[inline]
    pub(crate) async fn outbound_short_message(&mut self, data: SerializedMessage) -> Result<()> {
        let receipts = self.received_tracker.get_receipts();
        let packet_id = self
            .remote_conn
            .last_packet_id
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        // Get token before sending (captures send-time state for BBR)
        // Use actual data length plus overhead estimate
        let packet_size = data.len() + 40; // ShortMessage header overhead
        let token = self
            .remote_conn
            .congestion_controller
            .on_send_with_token(packet_size);
        packet_sending(
            self.remote_conn.remote_addr,
            &self.remote_conn.socket,
            packet_id,
            &self.remote_conn.outbound_symmetric_key,
            receipts,
            symmetric_message::ShortMessage(data.into()),
            &self.remote_conn.sent_tracker,
            packet_size,
            token,
            PacketStream::Control,
        )
        .await?;
        Ok(())
    }

    async fn outbound_stream(&mut self, data: SerializedMessage) {
        let stream_id = StreamId::next();
        self.outbound_stream_with_id(stream_id, data.into(), None, None)
            .await;
    }

    /// Send stream data with a caller-provided StreamId.
    /// Used by operations-level streaming where the StreamId is communicated
    /// in the metadata message and must match the data stream.
    ///
    /// If `completion_tx` is provided, it will be signaled when the stream
    /// transfer completes, carrying a [`BroadcastDeliveryOutcome`] that
    /// distinguishes delivery from a drop (#4235). Used by the broadcast queue
    /// to hold a semaphore permit until the actual transfer finishes.
    async fn outbound_stream_with_id(
        &mut self,
        stream_id: StreamId,
        data: bytes::Bytes,
        metadata: Option<bytes::Bytes>,
        completion_tx: Option<tokio::sync::oneshot::Sender<super::BroadcastDeliveryOutcome>>,
    ) {
        let task = GlobalExecutor::spawn(
            outbound_stream::send_stream(
                stream_id,
                self.remote_conn.last_packet_id.clone(),
                self.remote_conn.socket.clone(),
                self.remote_conn.remote_addr,
                data,
                self.remote_conn.outbound_symmetric_key.clone(),
                self.remote_conn.sent_tracker.clone(),
                self.remote_conn.token_bucket.clone(),
                self.remote_conn.congestion_controller.clone(),
                self.time_source.clone(),
                metadata,
                completion_tx,
            )
            .instrument(span!(tracing::Level::DEBUG, "outbound_stream")),
        );
        self.outbound_stream_futures.push(task);
    }

    /// Pipe an inbound stream to an outbound connection.
    ///
    /// This forwards fragments from `inbound_handle` to the remote peer as they arrive,
    /// without waiting for full reassembly. Used by intermediate nodes for low-latency
    /// stream forwarding.
    async fn pipe_stream_to_remote(
        &mut self,
        outbound_stream_id: StreamId,
        inbound_handle: streaming::StreamHandle,
        metadata: Option<bytes::Bytes>,
    ) {
        let task = GlobalExecutor::spawn(
            outbound_stream::pipe_stream(
                inbound_handle,
                outbound_stream_id,
                self.remote_conn.last_packet_id.clone(),
                self.remote_conn.socket.clone(),
                self.remote_conn.remote_addr,
                self.remote_conn.outbound_symmetric_key.clone(),
                self.remote_conn.sent_tracker.clone(),
                self.remote_conn.token_bucket.clone(),
                self.remote_conn.congestion_controller.clone(),
                self.time_source.clone(),
                metadata,
            )
            .instrument(span!(tracing::Level::DEBUG, "pipe_stream")),
        );
        self.outbound_stream_futures.push(task);
    }

    /// Sends a single stream fragment to the remote peer.
    ///
    /// This is the low-level API for piped forwarding. Unlike `send()` which
    /// serializes and fragments automatically, this sends a pre-existing fragment
    /// directly. Used by intermediate nodes to forward fragments immediately.
    ///
    /// # Arguments
    ///
    /// * `fragment` - The fragment to send (from `PipedStream::push_fragment()`)
    ///
    /// # Congestion Control
    ///
    /// This method applies both BBR congestion control (cwnd) and token bucket
    /// rate limiting, ensuring forwarded fragments don't overwhelm the network.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Forward fragments as they become ready
    /// for fragment in piped_stream.push_fragment(num, payload)? {
    ///     peer_connection.send_fragment(fragment).await?;
    /// }
    /// ```
    #[allow(dead_code)] // Phase 2 infrastructure - not yet integrated
    pub(crate) async fn send_fragment(
        &mut self,
        fragment: piped_stream::ForwardFragment,
    ) -> Result<()> {
        let packet_size = fragment.payload.len();

        // BBR congestion control - wait for cwnd space
        let mut cwnd_wait_iterations = 0;
        loop {
            let flightsize = self.remote_conn.congestion_controller.flightsize();
            let cwnd = self.remote_conn.congestion_controller.current_cwnd();

            if flightsize + packet_size <= cwnd {
                break;
            }

            cwnd_wait_iterations += 1;
            if cwnd_wait_iterations == 1 {
                tracing::trace!(
                    stream_id = %fragment.stream_id.0,
                    fragment_number = fragment.fragment_number,
                    flightsize_kb = flightsize / 1024,
                    cwnd_kb = cwnd / 1024,
                    "Waiting for cwnd space in send_fragment"
                );
            }

            // Exponential backoff
            if cwnd_wait_iterations <= 10 {
                tokio::task::yield_now().await;
            } else if cwnd_wait_iterations <= 100 {
                tokio::time::sleep(Duration::from_micros(100)).await;
            } else {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }

        // Token bucket rate limiting
        let wait_time = self.remote_conn.token_bucket.reserve(packet_size);
        if !wait_time.is_zero() {
            tracing::trace!(
                stream_id = %fragment.stream_id.0,
                fragment_number = fragment.fragment_number,
                wait_time_ms = wait_time.as_millis(),
                "Rate limiting fragment send"
            );
            tokio::time::sleep(wait_time).await;
        }

        // Get receipts and packet ID
        let receipts = self.received_tracker.get_receipts();
        let packet_id = self
            .remote_conn
            .last_packet_id
            .fetch_add(1, std::sync::atomic::Ordering::Release);

        // Get token before sending (captures send-time state for BBR)
        // This also updates the congestion controller's flightsize
        let token = self
            .remote_conn
            .congestion_controller
            .on_send_with_token(packet_size);

        // Send the fragment
        packet_sending(
            self.remote_conn.remote_addr,
            &self.remote_conn.socket,
            packet_id,
            &self.remote_conn.outbound_symmetric_key,
            receipts,
            symmetric_message::StreamFragment {
                stream_id: fragment.stream_id,
                total_length_bytes: fragment.total_bytes,
                fragment_number: fragment.fragment_number,
                payload: fragment.payload,
                metadata_bytes: None,
            },
            &self.remote_conn.sent_tracker,
            packet_size,
            token,
            PacketStream::Stream(fragment.stream_id),
        )
        .await?;

        tracing::trace!(
            stream_id = %fragment.stream_id.0,
            fragment_number = fragment.fragment_number,
            packet_id,
            "Fragment sent"
        );

        Ok(())
    }
}

/// Register a freshly-sent packet with the tracker, tagging it with its owning
/// stream for flight-size accounting (issue #4345).
///
/// `Stream(id)` first sends route to `report_sent_stream_packet_with_size`
/// (which records the tag); `Control` sends route to
/// `report_sent_packet_with_token_and_size` (which preserves any existing tag,
/// so resend re-registration of a stream fragment keeps its stream, and
/// genuinely-new control packets stay `Control`).
fn report_sent_tagged<T: crate::simulation::TimeSource>(
    tracker: &mut SentPacketTracker<T>,
    packet_id: u32,
    payload: Box<[u8]>,
    packet_size: usize,
    delivery_token: Option<DeliveryRateToken>,
    stream: PacketStream,
) {
    match stream {
        PacketStream::Stream(stream_id) => {
            tracker.report_sent_stream_packet_with_size(
                packet_id,
                payload,
                delivery_token,
                stream_id,
                packet_size,
            );
        }
        PacketStream::Control => {
            tracker.report_sent_packet_with_token_and_size(
                packet_id,
                payload,
                delivery_token,
                packet_size,
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn packet_sending<S: super::Socket, T: crate::simulation::TimeSource>(
    remote_addr: SocketAddr,
    socket: &Arc<S>,
    packet_id: u32,
    outbound_sym_key: &Aes128Gcm,
    confirm_receipt: Vec<u32>,
    payload: impl Into<SymmetricMessagePayload>,
    sent_tracker: &parking_lot::Mutex<SentPacketTracker<T>>,
    packet_size: usize,
    delivery_token: Option<DeliveryRateToken>,
    // Owning stream for flight-size accounting (issue #4345). `Stream(id)` for
    // `StreamFragment` sends so an aborted stream's bytes can be released
    // atomically via `SentPacketTracker::drop_stream`; `Control` for everything
    // else (handshake, NoOp, short message).
    stream: PacketStream,
) -> Result<()> {
    let start_time = tokio::time::Instant::now();
    tracing::trace!(
        peer_addr = %remote_addr,
        packet_id,
        "Attempting to send packet"
    );

    // Phase 1.6 shadow telemetry (#4074): classify this outbound payload
    // (must-flow / short / bulk) before it is consumed by serialization,
    // so the must-flow-vs-bulk split can be measured. Converting to the
    // concrete payload here is a no-op for callers that already pass one
    // and a cheap `Into` for the rest; `try_serialize_msg_to_packet_data`
    // still accepts it via `Into<Self>`. Observation only — see
    // `transport/shadow_demand.rs` and `.claude/rules/transport.md`.
    let payload: SymmetricMessagePayload = payload.into();
    let outbound_class = super::shadow_demand::classify(&payload);

    match SymmetricMessage::try_serialize_msg_to_packet_data(
        packet_id,
        payload,
        outbound_sym_key,
        confirm_receipt,
    )? {
        either::Either::Left(packet) => {
            let on_wire_packet_size = packet.data().len();
            tracing::trace!(
                peer_addr = %remote_addr,
                packet_id,
                packet_size,
                on_wire_packet_size,
                "Sending single packet"
            );
            let packet_data = packet.prepared_send();
            match socket.send_to(&packet_data, remote_addr).await {
                Ok(_) => {
                    let elapsed = start_time.elapsed();
                    tracing::trace!(
                        peer_addr = %remote_addr,
                        packet_id,
                        elapsed_ms = elapsed.as_millis(),
                        "Successfully sent packet"
                    );
                    // Record the classified on-wire bytes once the packet
                    // is actually sent (not on the error path).
                    super::shadow_demand::record_outbound(outbound_class, packet_data.len());
                    report_sent_tagged(
                        &mut sent_tracker.lock(),
                        packet_id,
                        packet_data,
                        packet_size,
                        delivery_token,
                        stream,
                    );
                    Ok(())
                }
                Err(e) => {
                    tracing::warn!(
                        peer_addr = %remote_addr,
                        packet_id,
                        error = %e,
                        "Failed to send packet (transient)"
                    );
                    Err(TransportError::SendFailed(remote_addr, e.kind()))
                }
            }
        }
        either::Either::Right((payload, mut confirm_receipt)) => {
            tracing::trace!(
                peer_addr = %remote_addr,
                packet_id,
                "Sending multi-packet message"
            );
            // Accumulate on-wire bytes across all packets of this multi-part
            // message so the Phase 1.6 class counters see the full cost,
            // attributed to the primary payload's class. Two accepted
            // accounting nits (observation-only, see shadow_demand.rs): the
            // trailing NoOp confirm-receipt packets are counted under the
            // primary class rather than as must-flow, and on a mid-burst
            // `send_to` failure the `?` below returns before the
            // `record_outbound` call, so the class split can undercount the
            // already-sent bytes relative to `cumulative_bytes_sent` (which
            // is incremented per packet at the socket layer). Both are tiny
            // and only affect the rarely-hit multi-packet path.
            let mut sent_on_wire = 0usize;
            macro_rules! send {
                ($packets:ident) => {{
                    for packet in $packets {
                        let packet_data = packet.prepared_send();
                        socket
                            .send_to(&packet_data, remote_addr)
                            .await
                            .map_err(|e| TransportError::SendFailed(remote_addr, e.kind()))?;
                        sent_on_wire += packet_data.len();
                        report_sent_tagged(
                            &mut sent_tracker.lock(),
                            packet_id,
                            packet_data,
                            packet_size,
                            delivery_token,
                            stream,
                        );
                    }
                }};
            }

            let max_num = SymmetricMessage::max_num_of_confirm_receipts_of_noop_message();
            let packet = SymmetricMessage::serialize_msg_to_packet_data(
                packet_id,
                payload,
                outbound_sym_key,
                vec![],
            )?;

            if max_num > confirm_receipt.len() {
                let packets = [
                    packet,
                    SymmetricMessage::serialize_msg_to_packet_data(
                        packet_id,
                        SymmetricMessagePayload::NoOp,
                        outbound_sym_key,
                        confirm_receipt,
                    )?,
                ];

                send!(packets);
                super::shadow_demand::record_outbound(outbound_class, sent_on_wire);
                return Ok(());
            }

            let mut packets = Vec::with_capacity(8);
            packets.push(packet);

            while !confirm_receipt.is_empty() {
                let len = confirm_receipt.len();

                if len <= max_num {
                    packets.push(SymmetricMessage::serialize_msg_to_packet_data(
                        packet_id,
                        SymmetricMessagePayload::NoOp,
                        outbound_sym_key,
                        confirm_receipt,
                    )?);
                    break;
                }

                let receipts = confirm_receipt.split_off(max_num);
                packets.push(SymmetricMessage::serialize_msg_to_packet_data(
                    packet_id,
                    SymmetricMessagePayload::NoOp,
                    outbound_sym_key,
                    receipts,
                )?);
            }

            send!(packets);
            super::shadow_demand::record_outbound(outbound_class, sent_on_wire);
            Ok(())
        }
    }
}

// =============================================================================
// PeerConnectionApi implementation for type erasure
// =============================================================================

impl<S: super::Socket> super::PeerConnectionApi for PeerConnection<S> {
    fn remote_addr(&self) -> std::net::SocketAddr {
        self.remote_conn.remote_addr
    }

    fn remote_version(&self) -> Option<(u8, u8, u16)> {
        PeerConnection::remote_version(self)
    }

    fn send_message(
        &mut self,
        msg: crate::message::NetMessage,
    ) -> std::pin::Pin<
        Box<dyn futures::Future<Output = Result<(), super::TransportError>> + Send + '_>,
    > {
        Box::pin(async move { self.send(msg).await })
    }

    fn recv(
        &mut self,
    ) -> std::pin::Pin<
        Box<dyn futures::Future<Output = Result<Vec<u8>, super::TransportError>> + Send + '_>,
    > {
        Box::pin(async move { PeerConnection::recv(self).await })
    }

    fn set_orphan_stream_registry(&mut self, registry: std::sync::Arc<OrphanStreamRegistry>) {
        PeerConnection::set_orphan_stream_registry(self, registry);
    }

    fn send_stream_data(
        &mut self,
        stream_id: StreamId,
        data: bytes::Bytes,
        metadata: Option<bytes::Bytes>,
        completion_tx: Option<tokio::sync::oneshot::Sender<super::BroadcastDeliveryOutcome>>,
    ) -> std::pin::Pin<
        Box<dyn futures::Future<Output = Result<(), super::TransportError>> + Send + '_>,
    > {
        Box::pin(async move {
            self.outbound_stream_with_id(stream_id, data, metadata, completion_tx)
                .await;
            Ok(())
        })
    }

    fn pipe_stream_data(
        &mut self,
        outbound_stream_id: StreamId,
        inbound_handle: streaming::StreamHandle,
        metadata: Option<bytes::Bytes>,
    ) -> std::pin::Pin<
        Box<dyn futures::Future<Output = Result<(), super::TransportError>> + Send + '_>,
    > {
        Box::pin(async move {
            self.pipe_stream_to_remote(outbound_stream_id, inbound_handle, metadata)
                .await;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use aes_gcm::KeyInit;
    use futures::TryFutureExt;
    use std::net::Ipv4Addr;

    use super::{
        inbound_stream::{InboundStream, recv_stream},
        outbound_stream::send_stream,
        *,
    };
    use crate::transport::packet_data::MAX_PACKET_SIZE;
    use crate::transport::received_packet_tracker::MAX_PENDING_RECEIPTS;
    use crate::transport::sent_packet_tracker::MAX_CONFIRMATION_DELAY;

    /// Simple test socket that writes to a channel
    struct TestSocket {
        sender: mpsc::Sender<(SocketAddr, Arc<[u8]>)>,
    }

    impl TestSocket {
        fn new(sender: mpsc::Sender<(SocketAddr, Arc<[u8]>)>) -> Self {
            Self { sender }
        }
    }

    impl crate::transport::Socket for TestSocket {
        async fn bind(_addr: SocketAddr) -> std::io::Result<Self> {
            unimplemented!()
        }

        async fn recv_from(&self, _buf: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
            unimplemented!()
        }

        async fn send_to(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
            self.sender
                .send((target, buf.into()))
                .await
                .map_err(|_| std::io::ErrorKind::ConnectionAborted)?;
            Ok(buf.len())
        }

        fn send_to_blocking(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
            self.sender
                .blocking_send((target, buf.into()))
                .map_err(|_| std::io::ErrorKind::ConnectionAborted)?;
            Ok(buf.len())
        }
    }

    /// Test socket with configurable send failures for testing transient error resilience.
    struct FailableTestSocket {
        sender: mpsc::Sender<(SocketAddr, Arc<[u8]>)>,
        fail_sends: Arc<std::sync::atomic::AtomicBool>,
    }

    impl FailableTestSocket {
        fn new(
            sender: mpsc::Sender<(SocketAddr, Arc<[u8]>)>,
            fail_sends: Arc<std::sync::atomic::AtomicBool>,
        ) -> Self {
            Self { sender, fail_sends }
        }
    }

    impl crate::transport::Socket for FailableTestSocket {
        async fn bind(_addr: SocketAddr) -> std::io::Result<Self> {
            unimplemented!()
        }

        async fn recv_from(&self, _buf: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
            unimplemented!()
        }

        async fn send_to(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
            if self.fail_sends.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NetworkUnreachable,
                    "simulated ENETUNREACH",
                ));
            }
            self.sender
                .send((target, buf.into()))
                .await
                .map_err(|_| std::io::ErrorKind::ConnectionAborted)?;
            Ok(buf.len())
        }

        fn send_to_blocking(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
            if self.fail_sends.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NetworkUnreachable,
                    "simulated ENETUNREACH",
                ));
            }
            self.sender
                .blocking_send((target, buf.into()))
                .map_err(|_| std::io::ErrorKind::ConnectionAborted)?;
            Ok(buf.len())
        }
    }

    /// Verify that packet_sending returns SendFailed (not ConnectionClosed) on send errors,
    /// and that SendFailed is classified as a transient failure.
    #[test]
    fn send_failure_returns_transient_error() {
        let fail_flag = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let (tx, _rx) = mpsc::channel(16);
        let socket = Arc::new(FailableTestSocket::new(tx, fail_flag));
        let remote_addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let outbound_key = {
                use aes_gcm::KeyInit;
                Aes128Gcm::new(&[0u8; 16].into())
            };
            let sent_tracker = Arc::new(parking_lot::Mutex::new(
                crate::transport::sent_packet_tracker::tests::mock_sent_packet_tracker(),
            ));

            let result = packet_sending(
                remote_addr,
                &socket,
                1,
                &outbound_key,
                vec![],
                (),
                &sent_tracker,
                50,
                None,
                PacketStream::Control,
            )
            .await;

            let err = result.expect_err("should fail");
            assert!(
                err.is_transient_send_failure(),
                "expected SendFailed, got: {err:?}"
            );
            assert!(
                !matches!(err, TransportError::ConnectionClosed(_)),
                "should NOT be ConnectionClosed"
            );
        });
    }

    /// Regression guard for issue #3318/#3321 at the `packet_sending` layer:
    /// a transient send failure must map to a recoverable, transient error and
    /// must not leave the send path in a poisoned state.
    ///
    /// The production incident saw a brief ENETUNREACH blip tear down all 11
    /// ring connections in under two seconds because every failed
    /// `socket.send_to()` surfaced as `ConnectionClosed`.  This drives the
    /// exact same `packet_sending` path through a transient failure and then
    /// asserts that, once the blip clears, the *same* socket and key send
    /// successfully again.
    ///
    /// Scope note: `packet_sending` is a stateless free function, so this locks
    /// only its error-mapping (blip → transient, not `ConnectionClosed`) and
    /// its statelessness/recoverability — a failed datagram leaves nothing
    /// behind that blocks the next send on the same socket+key. It does NOT
    /// exercise connection teardown: the decision to keep or drop the
    /// connection lives in the callers that consume this error
    /// (`is_transient_send_failure` dispatch sites), which this free function
    /// does not drive. That residual caller-level gap is the #3321 follow-up.
    #[test]
    fn transient_send_failure_then_recovery_succeeds() {
        let fail_flag = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let (tx, mut rx) = mpsc::channel(16);
        let socket = Arc::new(FailableTestSocket::new(tx, fail_flag.clone()));
        let remote_addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let outbound_key = {
                use aes_gcm::KeyInit;
                Aes128Gcm::new(&[0u8; 16].into())
            };
            let sent_tracker = Arc::new(parking_lot::Mutex::new(
                crate::transport::sent_packet_tracker::tests::mock_sent_packet_tracker(),
            ));

            // ---- blip: send fails transiently ----
            let err = packet_sending(
                remote_addr,
                &socket,
                1,
                &outbound_key,
                vec![],
                (),
                &sent_tracker,
                50,
                None,
                PacketStream::Control,
            )
            .await
            .expect_err("send during blip should fail");
            assert!(
                err.is_transient_send_failure(),
                "blip must surface as a transient failure callers can swallow, got: {err:?}"
            );
            assert!(
                !matches!(err, TransportError::ConnectionClosed(_)),
                "a single failed datagram must NOT report the connection closed — issue #3321"
            );

            // ---- blip clears: the same socket + key recover ----
            fail_flag.store(false, std::sync::atomic::Ordering::Relaxed);
            packet_sending(
                remote_addr,
                &socket,
                2,
                &outbound_key,
                vec![],
                (),
                &sent_tracker,
                50,
                None,
                PacketStream::Control,
            )
            .await
            .expect("send after the blip clears must succeed on the same connection");

            let (target, _) = rx.recv().await.expect("recovered packet must be sent");
            assert_eq!(
                target, remote_addr,
                "recovered packet must go to the same peer"
            );
        });
    }

    /// All transient send error kinds observed in production (the ENETUNREACH
    /// family and friends) must classify as transient so the p2p error
    /// handlers swallow them instead of closing the connection.  This locks
    /// the `is_transient_send_failure` contract the #3321 fix relies on.
    ///
    /// Classification is deliberately kind-agnostic: `is_transient_send_failure`
    /// matches `SendFailed(..)` on the *variant*, not the inner `ErrorKind`, so
    /// every send error is connection-local regardless of errno. The loop below
    /// therefore pins "every `SendFailed` is transient"; the explicit negative
    /// assertion on a non-`SendFailed` variant pins the *boundary* — a genuine
    /// `ConnectionClosed` must NOT be swallowed as transient — so the test
    /// nails down both sides of the predicate rather than only the true case.
    #[test]
    fn send_failed_kinds_classify_as_transient() {
        use std::io::ErrorKind;
        let remote_addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        for kind in [
            ErrorKind::NetworkUnreachable,
            ErrorKind::HostUnreachable,
            ErrorKind::ConnectionReset,
            ErrorKind::WouldBlock,
            ErrorKind::TimedOut,
        ] {
            let err = TransportError::SendFailed(remote_addr, kind);
            assert!(
                err.is_transient_send_failure(),
                "SendFailed({kind:?}) must be transient so the connection survives — issue #3321"
            );
        }

        // Boundary: a real connection-teardown error must NOT be classified as
        // a transient send blip, otherwise the swallow path would mask genuine
        // closures. `ConnectionClosed` and `ChannelClosed` are the canonical
        // non-transient variants.
        assert!(
            !TransportError::ConnectionClosed(remote_addr).is_transient_send_failure(),
            "ConnectionClosed must NOT be swallowed as a transient send failure — issue #3321"
        );
        assert!(
            !TransportError::ChannelClosed.is_transient_send_failure(),
            "ChannelClosed must NOT be swallowed as a transient send failure — issue #3321"
        );
    }

    #[test]
    fn packet_sending_tracks_caller_flight_size_for_metadata_stream_fragment() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let outbound_key = {
                use aes_gcm::KeyInit;
                Aes128Gcm::new(&[0u8; 16].into())
            };
            let sent_tracker = Arc::new(parking_lot::Mutex::new(
                crate::transport::sent_packet_tracker::tests::mock_sent_packet_tracker(),
            ));
            let remote_addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
            let ok_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let (tx, _rx) = mpsc::channel(16);
            let socket = Arc::new(FailableTestSocket::new(tx, ok_flag));
            let stream_id = StreamId::next();
            let packet_size = 128;

            packet_sending(
                remote_addr,
                &socket,
                1,
                &outbound_key,
                vec![],
                SymmetricMessagePayload::StreamFragment {
                    stream_id,
                    total_length_bytes: 200,
                    fragment_number: 1,
                    payload: bytes::Bytes::from(vec![9u8; 80]),
                    metadata_bytes: Some(bytes::Bytes::from(vec![7u8; 32])),
                },
                &sent_tracker,
                packet_size,
                None,
                PacketStream::Stream(stream_id),
            )
            .await
            .expect("metadata-bearing stream fragment send should succeed");

            let released = sent_tracker.lock().drop_stream(stream_id);
            assert_eq!(
                released, packet_size as u64,
                "drop_stream must release the exact flight-size bytes passed by the caller"
            );
        });
    }

    /// Phase 1.6 (#4074): the hot-path instrumentation must feed the
    /// classified byte counters, and only on a successful send. A
    /// `ShortMessage` advances `short`, a `StreamFragment` advances `bulk`,
    /// and a failing send records nothing. The class counters are
    /// process-global and monotonic, so the success assertions are framed
    /// as lower bounds (delta ≥ payload) and the failure assertion as an
    /// upper bound well below the payload size — both robust under
    /// shared-process (`cargo test`) execution where another test might
    /// also touch the same statics.
    #[test]
    fn packet_sending_feeds_classified_counters_on_success_only() {
        use crate::transport::shadow_demand::outbound_counters_snapshot;

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let outbound_key = {
                use aes_gcm::KeyInit;
                Aes128Gcm::new(&[0u8; 16].into())
            };
            let sent_tracker = Arc::new(parking_lot::Mutex::new(
                crate::transport::sent_packet_tracker::tests::mock_sent_packet_tracker(),
            ));
            let remote_addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();

            // ---- success: ShortMessage advances `short` ----
            let ok_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let (tx, _rx) = mpsc::channel(16);
            let socket = Arc::new(FailableTestSocket::new(tx, ok_flag));

            let (_, short_before, bulk_before) = outbound_counters_snapshot();
            packet_sending(
                remote_addr,
                &socket,
                1,
                &outbound_key,
                vec![],
                SymmetricMessagePayload::ShortMessage {
                    payload: bytes::Bytes::from(vec![7u8; 200]),
                },
                &sent_tracker,
                240,
                None,
                PacketStream::Control,
            )
            .await
            .expect("short send should succeed");
            let (_, short_after, _) = outbound_counters_snapshot();
            assert!(
                short_after - short_before >= 200,
                "ShortMessage send must add >= its 200-byte payload to `short` (delta {})",
                short_after - short_before
            );

            // ---- success: StreamFragment advances `bulk` ----
            let frag_stream_id = StreamId::next();
            packet_sending(
                remote_addr,
                &socket,
                2,
                &outbound_key,
                vec![],
                SymmetricMessagePayload::StreamFragment {
                    stream_id: frag_stream_id,
                    total_length_bytes: 300,
                    fragment_number: 0,
                    payload: bytes::Bytes::from(vec![9u8; 300]),
                    metadata_bytes: None,
                },
                &sent_tracker,
                300,
                None,
                PacketStream::Stream(frag_stream_id),
            )
            .await
            .expect("stream fragment send should succeed");
            let (_, _, bulk_after) = outbound_counters_snapshot();
            assert!(
                bulk_after - bulk_before >= 300,
                "StreamFragment send must add >= its 300-byte payload to `bulk` (delta {})",
                bulk_after - bulk_before
            );

            // ---- failure: a failing send records nothing ----
            let fail_flag = Arc::new(std::sync::atomic::AtomicBool::new(true));
            let (tx2, _rx2) = mpsc::channel(16);
            let failing = Arc::new(FailableTestSocket::new(tx2, fail_flag));
            let (_, short_pre_fail, _) = outbound_counters_snapshot();
            let res = packet_sending(
                remote_addr,
                &failing,
                3,
                &outbound_key,
                vec![],
                SymmetricMessagePayload::ShortMessage {
                    payload: bytes::Bytes::from(vec![3u8; 600]),
                },
                &sent_tracker,
                640,
                None,
                PacketStream::Control,
            )
            .await;
            assert!(res.is_err(), "failing socket must return Err");
            let (_, short_post_fail, _) = outbound_counters_snapshot();
            // A wrongly-recorded error path would jump `short` by >= 600;
            // concurrent unit tests only add tiny amounts, so a delta well
            // under 600 proves nothing was recorded on the failure path.
            assert!(
                short_post_fail - short_pre_fail < 600,
                "failing send must not record its 600-byte payload (delta {})",
                short_post_fail - short_pre_fail
            );
        });
    }

    /// Verify that ACK_CHECK_INTERVAL is properly configured relative to MAX_CONFIRMATION_DELAY.
    /// The ACK timer should ensure ACKs are sent within the sender's expected confirmation window.
    #[test]
    fn ack_check_interval_is_within_confirmation_window() {
        // ACK_CHECK_INTERVAL should not exceed MAX_CONFIRMATION_DELAY
        // to ensure receipts are sent within the allowed window
        assert!(
            ACK_CHECK_INTERVAL <= MAX_CONFIRMATION_DELAY,
            "ACK_CHECK_INTERVAL ({:?}) must not exceed MAX_CONFIRMATION_DELAY ({:?})",
            ACK_CHECK_INTERVAL,
            MAX_CONFIRMATION_DELAY
        );
    }

    /// Verify that the ACK timer interval is reasonable (not too fast or too slow).
    #[test]
    fn ack_check_interval_is_reasonable() {
        // Should not be too fast (would cause excessive CPU usage)
        assert!(
            ACK_CHECK_INTERVAL >= Duration::from_millis(10),
            "ACK_CHECK_INTERVAL ({:?}) should be at least 10ms to avoid excessive CPU usage",
            ACK_CHECK_INTERVAL
        );

        // Should not be too slow (would cause unnecessary delays)
        assert!(
            ACK_CHECK_INTERVAL <= Duration::from_millis(100),
            "ACK_CHECK_INTERVAL ({:?}) should be at most 100ms to ensure timely ACK delivery",
            ACK_CHECK_INTERVAL
        );
    }

    /// Test that MAX_PENDING_RECEIPTS is appropriate for typical stream sizes.
    /// A 40KB stream splits into ~28 packets, so buffer should be able to handle
    /// at least one batch before triggering an ACK send.
    #[test]
    fn pending_receipts_buffer_size_documented() {
        // Document the current buffer size - this affects when buffer-full ACKs are sent
        // With MAX_PENDING_RECEIPTS = 20 and typical streams of ~28 packets:
        // - First 20 packets: buffer fills, ACK sent
        // - Remaining 8 packets: rely on timer for ACK delivery
        // The background ACK timer ensures these remaining packets get ACKed within 100ms
        assert_eq!(
            MAX_PENDING_RECEIPTS, 20,
            "MAX_PENDING_RECEIPTS changed - verify ACK timing behavior is still correct"
        );
    }

    #[tokio::test]
    async fn test_inbound_outbound_interaction() -> Result<(), Box<dyn std::error::Error>> {
        const MSG_LEN: usize = 1000;
        let (sender, mut receiver) = mpsc::channel(1);
        let remote_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 8080);
        let mut message = vec![0u8; MSG_LEN];
        crate::config::GlobalRng::fill_bytes(&mut message);
        let mut key = [0u8; 16];
        crate::config::GlobalRng::fill_bytes(&mut key);
        let cipher = Aes128Gcm::new(&key.into());

        // Initialize with VirtualTime for deterministic testing
        // Token bucket has enough capacity (10KB) for 1KB message without sleeping
        let time_source = crate::simulation::VirtualTime::new();
        let sent_tracker = Arc::new(parking_lot::Mutex::new(
            SentPacketTracker::new_with_time_source(time_source.clone()),
        ));
        let congestion_controller =
            crate::transport::congestion_control::CongestionControlConfig::default()
                .with_initial_cwnd(2928)
                .with_min_cwnd(2928)
                .with_max_cwnd(1_000_000_000)
                .build_arc_with_time_source(time_source.clone());
        let token_bucket = Arc::new(TokenBucket::new_with_time_source(
            10_000,
            10_000_000,
            time_source.clone(),
        ));

        let stream_id = StreamId::next();
        // Send a long message using the outbound stream
        let outbound = GlobalExecutor::spawn(send_stream(
            stream_id,
            Arc::new(AtomicU32::new(0)),
            Arc::new(TestSocket::new(sender)),
            remote_addr,
            bytes::Bytes::from(message.clone()),
            cipher.clone(),
            sent_tracker,
            token_bucket,
            congestion_controller,
            time_source,
            None,
            None,
        ))
        .map_err(|e| e.into());

        let inbound = async {
            // need to take care of decrypting and deserializing the inbound data before collecting into the message
            let (tx, rx) = mpsc::channel(1);
            let stream = InboundStream::new(MSG_LEN as u64);
            let inbound_msg = GlobalExecutor::spawn(recv_stream(stream_id, rx, stream));
            while let Some((_, network_packet)) = receiver.recv().await {
                let decrypted = PacketData::<_, MAX_PACKET_SIZE>::from_buf(&network_packet)
                    .try_decrypt_sym(&cipher)
                    .map_err(|e| e.to_string())?;
                let SymmetricMessage {
                    payload:
                        SymmetricMessagePayload::StreamFragment {
                            fragment_number,
                            payload,
                            ..
                        },
                    ..
                } = SymmetricMessage::deser(decrypted.data()).expect("symmetric message")
                else {
                    return Err("unexpected message".into());
                };
                tx.send((fragment_number, payload)).await?;
            }
            let (_, msg) = inbound_msg
                .await?
                .map_err(|_| anyhow::anyhow!("stream failed"))?;
            Ok::<_, Box<dyn std::error::Error>>(msg)
        };

        let (out_res, inbound_msg) = tokio::try_join!(outbound, inbound)?;
        out_res?;
        assert_eq!(message, inbound_msg);
        Ok(())
    }

    /// Verify that bincode serialization is fast enough to run on async runtime.
    ///
    /// This test documents the assumption behind removing spawn_blocking from send().
    /// Bincode serialization of typical messages should complete in < 1ms, making
    /// spawn_blocking overhead unnecessary and actually harmful (causes thread churn).
    ///
    /// See issue #2310 for details on the thread explosion this caused.
    #[test]
    fn bincode_serialization_is_fast_enough_for_async() {
        use crate::message::{NeighborHostingMessage, NetMessage, NetMessageV1};
        use freenet_stdlib::prelude::ContractInstanceId;
        use std::time::Instant;

        // Helper to time serialization and assert it's fast enough
        fn assert_fast_serialize<T: serde::Serialize>(name: &str, value: &T) {
            let start = Instant::now();
            let serialized = bincode::serialize(value).expect("serialization failed");
            let elapsed = start.elapsed();

            // Serialization should complete in well under 10ms (typically < 1ms)
            // We use 10ms as a generous upper bound to avoid flaky tests on slow CI
            assert!(
                elapsed.as_millis() < 10,
                "{} serialization ({} bytes) took {:?}, expected < 10ms. \
                 If this fails consistently, reconsider whether spawn_blocking is needed.",
                name,
                serialized.len(),
                elapsed
            );
        }

        // Test 1: Simple byte payloads at various sizes (baseline)
        for size in [100, 1000, MAX_DATA_SIZE / 2, MAX_DATA_SIZE] {
            let payload: Vec<u8> = (0..size).map(|i| i as u8).collect();
            assert_fast_serialize(&format!("Vec<u8>[{}]", size), &payload);
        }

        // Test 2: Actual network message types used in production
        // These are the types that go through peer_connection.send()

        // NeighborHosting messages (common during connection setup)
        let cache_msg = NetMessage::V1(NetMessageV1::NeighborHosting {
            message: NeighborHostingMessage::HostingAnnounce {
                added: vec![ContractInstanceId::new([1u8; 32])],
                removed: vec![],
                is_response: false,
            },
        });
        assert_fast_serialize("NeighborHostingMessage", &cache_msg);

        // Large hosting state response (worst case for NeighborHosting)
        let large_cache = NetMessage::V1(NetMessageV1::NeighborHosting {
            message: NeighborHostingMessage::HostingStateResponse {
                contracts: (0..100)
                    .map(|i| ContractInstanceId::new([i as u8; 32]))
                    .collect(),
            },
        });
        assert_fast_serialize("Large HostingStateResponse", &large_cache);
    }

    /// Test that flightsize is properly accounted for retransmitted packets.
    ///
    /// This test verifies the fix for the flightsize leak bug where retransmitted
    /// packet ACKs didn't decrement flightsize because Karn's algorithm skipped
    /// RTT calculation and the code also skipped returning packet_size entirely.
    ///
    /// The fix ensures that when a retransmitted packet is ACKed:
    /// 1. The packet size is returned (so flightsize can be decremented)
    /// 2. But no RTT sample is returned (Karn's algorithm - don't pollute RTT estimates)
    ///
    /// Without the fix, flightsize would leak on every retransmission, eventually
    /// causing flightsize > cwnd, blocking all sends indefinitely.
    #[test]
    fn test_flightsize_accounting_for_retransmitted_packets() {
        use crate::transport::bbr::{BbrConfig, BbrController};
        use crate::transport::sent_packet_tracker::SentPacketTracker;
        use std::sync::Arc;

        // Create BBR controller with realistic values
        // Initial cwnd = ~38KB, min cwnd = 2KB
        let congestion = Arc::new(BbrController::new(BbrConfig {
            initial_cwnd: 38_000,
            min_cwnd: 2_000,
            max_cwnd: 10_000_000,
            ..Default::default()
        }));

        // Create sent packet tracker
        let mut tracker = SentPacketTracker::new();

        // Simulate sending 5 packets of 1424 bytes each
        let packet_size = 1424;
        for packet_id in 0..5u32 {
            let payload: Box<[u8]> = vec![0u8; packet_size].into_boxed_slice();
            tracker.report_sent_packet(packet_id, payload);
            congestion.on_send(packet_size);
        }

        // Verify initial flightsize
        assert_eq!(
            congestion.flightsize(),
            5 * packet_size,
            "Initial flightsize should be 5 * packet_size"
        );

        // Simulate ACK for packets 0, 1, 2 (normal ACKs with RTT samples)
        let (ack_info, _) = tracker.report_received_receipts(&[0, 1, 2]);
        assert_eq!(ack_info.len(), 3, "Should have 3 ACK entries");

        // All 3 should have RTT samples (not retransmitted)
        for (rtt_opt, size, _token) in &ack_info {
            assert!(
                rtt_opt.is_some(),
                "Non-retransmitted packets should have RTT samples"
            );
            assert_eq!(*size, packet_size, "Packet size should match");
        }

        // Apply ACKs to BBR - this should decrement flightsize
        for (rtt_opt, size, _token) in ack_info {
            match rtt_opt {
                Some(rtt) => congestion.on_ack(rtt, size),
                None => congestion.on_ack_without_rtt(size),
            }
        }

        assert_eq!(
            congestion.flightsize(),
            2 * packet_size,
            "Flightsize should be decremented to 2 * packet_size"
        );

        // Now simulate a timeout and retransmission for packet 3
        // Mark it as retransmitted
        tracker.mark_retransmitted(3);

        // Re-register the packet (simulating retransmission)
        // In real code, this happens in check_resend_receipts
        let payload: Box<[u8]> = vec![0u8; packet_size].into_boxed_slice();
        tracker.report_sent_packet(3, payload);

        // Simulate ACK for retransmitted packet 3
        let (ack_info, _) = tracker.report_received_receipts(&[3]);
        assert_eq!(
            ack_info.len(),
            1,
            "Should have 1 ACK entry for retransmitted packet"
        );

        // THIS IS THE KEY TEST: Retransmitted packet should return size but NO RTT
        let (rtt_opt, size, _token) = &ack_info[0];
        assert!(
            rtt_opt.is_none(),
            "Retransmitted packets should NOT have RTT samples (Karn's algorithm)"
        );
        assert_eq!(
            *size, packet_size,
            "Retransmitted packet ACK MUST still return packet size for flightsize decrement"
        );

        // Apply the ACK - should call on_ack_without_rtt, NOT skip the call entirely
        for (rtt_opt, size, _token) in ack_info {
            match rtt_opt {
                Some(rtt) => congestion.on_ack(rtt, size),
                None => congestion.on_ack_without_rtt(size),
            }
        }

        // Verify flightsize was properly decremented
        assert_eq!(
            congestion.flightsize(),
            packet_size,
            "Flightsize should be decremented even for retransmitted packet ACKs"
        );

        // Also ACK the last packet (4) to verify normal flow still works
        let (ack_info, _) = tracker.report_received_receipts(&[4]);
        for (rtt_opt, size, _token) in ack_info {
            match rtt_opt {
                Some(rtt) => congestion.on_ack(rtt, size),
                None => congestion.on_ack_without_rtt(size),
            }
        }

        assert_eq!(
            congestion.flightsize(),
            0,
            "All packets ACKed, flightsize should be 0"
        );
    }

    /// Regression test for issue #4345: flight size must drain when a packet is
    /// retransmitted forever with no ACK — the real production recv-loop path.
    ///
    /// This drives the EXACT production sequence (not a synthetic give-up): for
    /// each fired RTO the recv loop calls `on_timeout()` (cwnd loss response
    /// only — flight size unchanged), successfully re-sends, and re-registers
    /// the packet, because with no ACK arriving the tracker keeps re-queueing
    /// it. Crucially, since `on_timeout()` never touches flight size, it would
    /// stay pinned forever WITHOUT a give-up mechanism. The fix is the
    /// tracker's bounded-retransmit abandonment: after `MAX_PACKET_RETRANSMITS`
    /// it returns `ResendAction::Abandon`, the recv loop calls
    /// `release_flightsize(len)`, and the packet is dropped — so flight size
    /// finally drains.
    ///
    /// This is the integration-level companion to the pure-controller unit
    /// tests (`bbr`/`fixed_rate`
    /// `test_issue_4345_release_flightsize_drains_abandoned_bytes`): it confirms
    /// the END-TO-END path through `SentPacketTracker` actually reaches the
    /// give-up branch in production, which the earlier draft missed (the recv
    /// loop always re-sends on RTO, so only abandonment drains the leak).
    ///
    /// Before this fix, no abandonment existed: a never-ACKed packet was
    /// retransmitted forever, its initial `on_send` bytes pinned flight size at
    /// `cwnd`, and every subsequent stream on the connection aborted with
    /// "cwnd wait timeout".
    #[test]
    fn test_issue_4345_flightsize_drains_via_bounded_retransmit() {
        use crate::simulation::VirtualTime;
        use crate::transport::bbr::{BbrConfig, BbrController};
        use crate::transport::sent_packet_tracker::{ResendAction, SentPacketTracker};
        use std::sync::Arc;

        let time = VirtualTime::new();
        let congestion = Arc::new(BbrController::new_with_time_source(
            BbrConfig {
                initial_cwnd: 60_000,
                min_cwnd: 2_000,
                max_cwnd: 10_000_000,
                ..Default::default()
            },
            time.clone(),
        ));
        let mut tracker = SentPacketTracker::new_with_time_source(time.clone());

        // Send a burst that will never be ACKed (dead reverse-ACK path).
        let packet_size = 1424usize;
        let num_packets = 30u32;
        for packet_id in 0..num_packets {
            let payload: Box<[u8]> = vec![0u8; packet_size].into_boxed_slice();
            tracker.report_sent_packet(packet_id, payload);
            congestion.on_send(packet_size);
        }
        assert_eq!(
            congestion.flightsize(),
            num_packets as usize * packet_size,
            "flightsize should account for the whole in-flight burst"
        );

        // Drive the REAL production recv-loop sequence: each RTO calls
        // on_timeout() (cwnd loss response only — the packet is immediately
        // re-sent and stays in flight, so flight size is UNCHANGED) and
        // re-registers, repeating (no ACK ever arrives). Because every re-send
        // leaves flight size untouched, it would stay pinned forever WITHOUT a
        // give-up. The fix: after MAX_PACKET_RETRANSMITS the tracker returns
        // Abandon, the recv loop calls release_flightsize(len), and flight size
        // finally drains.
        let mut abandoned = 0u32;
        let mut resends = 0u32;
        for _ in 0..20_000 {
            match tracker.get_resend() {
                ResendAction::Resend(id, packet) => {
                    // Production recv-loop Resend arm: cwnd loss response, re-send
                    // (succeeds here), re-register. Flight size unchanged.
                    congestion.on_timeout();
                    tracker.report_sent_packet(id, packet);
                    resends += 1;
                }
                ResendAction::Abandon { payload_len, .. } => {
                    // The give-up that actually drains the leak.
                    congestion.release_flightsize(payload_len);
                    abandoned += 1;
                }
                ResendAction::TlpProbe(_id, _packet) => {
                    // Speculative probe — no flight-size effect.
                }
                ResendAction::WaitUntil(_) => {
                    if abandoned >= num_packets {
                        break; // every packet abandoned
                    }
                    // Jump past the (backed-off) next deadline and retry.
                    time.advance(std::time::Duration::from_secs(120));
                }
            }
        }

        // Sanity: flight size stayed pinned across the net-zero re-send rounds,
        // i.e. on_timeout alone never released it — only abandonment does.
        assert!(
            resends >= num_packets,
            "issue #4345: expected the production re-send path to run before \
             abandonment (resends={resends}); the test did not exercise the \
             real recv-loop re-send sequence"
        );
        assert_eq!(
            abandoned, num_packets,
            "issue #4345: every never-ACKed packet must eventually be abandoned \
             (abandoned={abandoned}/{num_packets})"
        );
        assert!(
            congestion.flightsize() <= packet_size,
            "issue #4345: flight size did not drain after bounded-retransmit \
             abandonment — got {} bytes ({} packets still in flight). Re-sends \
             alone never release a never-ACKed packet (on_timeout leaves flight \
             size unchanged); the tracker MUST abandon it (ResendAction::Abandon) \
             so the recv loop calls release_flightsize().",
            congestion.flightsize(),
            congestion.flightsize() / packet_size,
        );
    }

    /// Keepalive interval backs off exponentially past the unanswered-ping threshold (#3252).
    #[test]
    fn keepalive_interval_backs_off_for_unanswered_pings() {
        use super::{
            KEEP_ALIVE_INTERVAL, MAX_KEEPALIVE_INTERVAL, MAX_UNANSWERED_PINGS,
            keepalive_interval_for_pending,
        };

        // At or below threshold: base interval (5s)
        for count in 0..=MAX_UNANSWERED_PINGS {
            assert_eq!(
                keepalive_interval_for_pending(count),
                KEEP_ALIVE_INTERVAL,
                "pending_count={count} should use base interval"
            );
        }

        // Above threshold: exponential backoff, capped at MAX_KEEPALIVE_INTERVAL
        let expected = [
            (1, Duration::from_secs(10)),  // 5s * 2^1
            (2, Duration::from_secs(20)),  // 5s * 2^2
            (3, Duration::from_secs(40)),  // 5s * 2^3
            (4, MAX_KEEPALIVE_INTERVAL),   // 5s * 2^4 = 80s, capped at 60s
            (100, MAX_KEEPALIVE_INTERVAL), // far beyond threshold, still capped
        ];
        for (above, interval) in expected {
            assert_eq!(
                keepalive_interval_for_pending(MAX_UNANSWERED_PINGS + above),
                interval,
                "pending_count={} should produce {:?}",
                MAX_UNANSWERED_PINGS + above,
                interval,
            );
        }
    }

    /// Helper to compute msg_hash the same way PeerConnection does.
    fn msg_hash(bytes: &[u8]) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        bytes.hash(&mut h);
        h.finish()
    }

    /// Helper that mirrors PeerConnection::is_duplicate_dispatch using LRU.
    fn is_duplicate_dispatch(cache: &mut lru::LruCache<u64, ()>, bytes: &[u8]) -> bool {
        cache.put(msg_hash(bytes), ()).is_some()
    }

    /// Create a dedup cache with the given capacity.
    fn dedup_cache(cap: usize) -> lru::LruCache<u64, ()> {
        lru::LruCache::new(NonZeroUsize::new(cap).unwrap())
    }

    // Superseded: dedup store replaced with LRU cache in #3418;
    // HashSet.insert() return-value semantics no longer apply.
    // Equivalent behavior now tested by duplicate_embedded_metadata_suppressed.
    #[ignore]
    #[test]
    fn dispatched_short_message_always_recorded() {
        let mut cache = dedup_cache(1000);
        let bytes = b"metadata-payload";
        assert!(
            cache.put(msg_hash(bytes), ()).is_none(),
            "first insert should return None (not present)"
        );
        assert!(
            cache.put(msg_hash(bytes), ()).is_some(),
            "second insert returns Some (was present)"
        );
    }

    #[test]
    fn duplicate_embedded_metadata_suppressed() {
        // Same bytes: first insert is new, second is a duplicate.
        let mut cache = dedup_cache(1000);
        let bytes = b"metadata-payload";
        assert!(
            !is_duplicate_dispatch(&mut cache, bytes),
            "first insert is not a duplicate"
        );
        assert!(
            is_duplicate_dispatch(&mut cache, bytes),
            "second insert is a duplicate"
        );
    }

    #[test]
    fn different_metadata_not_suppressed() {
        // Different metadata bytes must not be treated as duplicates.
        let mut cache = dedup_cache(1000);
        assert!(!is_duplicate_dispatch(&mut cache, b"first-metadata"));
        assert!(!is_duplicate_dispatch(&mut cache, b"different-metadata"));
    }

    #[test]
    fn lru_eviction_preserves_recent_entries() {
        // Regression test for #3317: LRU eviction only removes the oldest entry,
        // unlike the old HashSet cap-and-clear which cleared everything.
        let mut cache = dedup_cache(3);

        // Fill cache to capacity, then insert a 4th to evict the oldest
        for i in 1..=4 {
            cache.put(msg_hash(format!("msg-{i}").as_bytes()), ());
        }

        // Recent entries survive; oldest (msg-1) was evicted
        assert!(cache.contains(&msg_hash(b"msg-4")), "msg-4 should survive");
        assert!(cache.contains(&msg_hash(b"msg-3")), "msg-3 should survive");
        assert!(
            !cache.contains(&msg_hash(b"msg-1")),
            "msg-1 should be evicted"
        );
    }

    /// Regression test for #3369: `last_received_nanos` is a struct field, not a local.
    ///
    /// Before the fix, `last_received_nanos` was a local variable in `recv()`,
    /// initialized to `now()` on each call. When `peer_connection_listener`'s select
    /// picked the outbound branch, `recv()` was cancelled and re-called, resetting the
    /// timeout window indefinitely — the gateway could never detect a dead peer as long
    /// as outbound traffic existed.
    ///
    /// After the fix, `last_received_nanos` is a struct field initialized once in `new()`
    /// and only updated when an actual inbound packet arrives. This test verifies:
    /// 1. The field is initialized to the creation-time timestamp
    /// 2. The field persists and is not reset when time advances (unlike a local that
    ///    would be re-initialized to `now()` on each recv() entry)
    /// 3. Multiple time advances don't affect the stored value
    #[tokio::test]
    async fn last_received_nanos_is_persistent_field() {
        use crate::transport::crypto::TransportKeypair;
        use crate::util::time_source::SharedMockTimeSource;

        let time_source = SharedMockTimeSource::new();
        let (_inbound_tx, inbound_rx) = mpsc::channel(16);
        let remote_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 9999);

        let mut key = [0u8; 16];
        crate::config::GlobalRng::fill_bytes(&mut key);
        let cipher = Aes128Gcm::new(&key.into());

        let keypair = TransportKeypair::new();

        let sent_tracker = Arc::new(parking_lot::Mutex::new(
            SentPacketTracker::new_with_time_source(time_source.clone()),
        ));
        let congestion_controller =
            crate::transport::congestion_control::CongestionControlConfig::default()
                .build_arc_with_time_source(time_source.clone());
        let token_bucket = Arc::new(TokenBucket::new_with_time_source(
            10_000,
            10_000_000,
            time_source.clone(),
        ));

        let socket = Arc::new(TestSocket::new(
            mpsc::channel::<(SocketAddr, Arc<[u8]>)>(16).0,
        ));

        let rolling_rtt_stats = crate::transport::rolling_rtt_stats::RollingRttStatsHandle::new(
            remote_addr,
            time_source.clone(),
        );
        let remote_conn = RemoteConnection {
            outbound_symmetric_key: cipher.clone(),
            remote_addr,
            sent_tracker,
            last_packet_id: Arc::new(AtomicU32::new(0)),
            inbound_packet_recv: inbound_rx,
            inbound_symmetric_key: cipher,
            inbound_symmetric_key_bytes: key,
            my_address: None,
            remote_protoc_version: None,
            transport_secret_key: keypair.secret,
            congestion_controller,
            token_bucket,
            socket,
            global_bandwidth: None,
            rolling_rtt_stats,
            time_source: time_source.clone(),
        };

        let creation_time = time_source.now_nanos();
        let conn = PeerConnection::new(remote_conn);

        // Field is initialized to creation-time timestamp
        assert_eq!(
            conn.last_received_nanos, creation_time,
            "last_received_nanos should be set to creation time"
        );

        // Advance time and verify the field does NOT change.
        // Before the fix, recv() would reinitialize a local `last_received_nanos = now()`
        // on every call. As a struct field, it stays at the creation-time value until
        // an actual inbound packet updates it.
        for i in 0..5 {
            time_source.advance_time(Duration::from_secs(10));
            let now = time_source.now_nanos();
            assert_ne!(now, creation_time, "time should have advanced");
            assert_eq!(
                conn.last_received_nanos, creation_time,
                "last_received_nanos must not change without inbound packets \
                 (iteration {i}, bug #3369). A local variable in recv() would \
                 return now()={now} instead of the stored creation_time={creation_time}"
            );
        }
    }

    /// Regression test for #3999: an authenticated inbound packet (one that
    /// passes `try_decrypt_sym`) must update both the per-peer entry and the
    /// cumulative receive counter, AND the byte count must equal the
    /// encrypted (wire) packet size, not the decrypted payload size.
    ///
    /// Without an explicit test, a future refactor that drops the
    /// `record_packet_received` call from the post-auth hook in `recv` would
    /// rot the dashboard silently — the spoof regression test only proves
    /// the *absence* of pre-auth counting, not the *presence* of post-auth
    /// counting. This test pins both halves.
    #[tokio::test]
    async fn recv_records_packet_metrics_post_authentication() {
        use crate::transport::crypto::TransportKeypair;
        use crate::transport::packet_data::PacketData;
        use crate::transport::symmetric_message::SymmetricMessagePayload;
        use crate::util::time_source::SharedMockTimeSource;
        use bytes::Bytes;

        let time_source = SharedMockTimeSource::new();
        let (inbound_tx, inbound_rx) = mpsc::channel(16);
        // Use a deterministic, never-otherwise-used port so concurrent tests
        // can't accidentally observe our per-peer entry.
        let remote_addr = SocketAddr::new(Ipv4Addr::new(10, 99, 99, 1).into(), 50001);

        let mut key = [0u8; 16];
        crate::config::GlobalRng::fill_bytes(&mut key);
        let cipher = Aes128Gcm::new(&key.into());
        let keypair = TransportKeypair::new();

        let sent_tracker = Arc::new(parking_lot::Mutex::new(
            SentPacketTracker::new_with_time_source(time_source.clone()),
        ));
        let congestion_controller =
            crate::transport::congestion_control::CongestionControlConfig::default()
                .build_arc_with_time_source(time_source.clone());
        let token_bucket = Arc::new(TokenBucket::new_with_time_source(
            10_000,
            10_000_000,
            time_source.clone(),
        ));
        let socket = Arc::new(TestSocket::new(
            mpsc::channel::<(SocketAddr, Arc<[u8]>)>(16).0,
        ));

        let rolling_rtt_stats = crate::transport::rolling_rtt_stats::RollingRttStatsHandle::new(
            remote_addr,
            time_source.clone(),
        );
        let remote_conn = RemoteConnection {
            outbound_symmetric_key: cipher.clone(),
            remote_addr,
            sent_tracker,
            last_packet_id: Arc::new(AtomicU32::new(0)),
            inbound_packet_recv: inbound_rx,
            inbound_symmetric_key: cipher.clone(),
            inbound_symmetric_key_bytes: key,
            my_address: None,
            remote_protoc_version: None,
            transport_secret_key: keypair.secret,
            congestion_controller,
            token_bucket,
            socket,
            global_bandwidth: None,
            rolling_rtt_stats,
            time_source,
        };

        // Build an encrypted ShortMessage so `recv` returns immediately
        // after metering. Plaintext payload is 100 bytes; the encrypted
        // wire packet will be larger (nonce + AEAD tag + framing).
        let plaintext: Vec<u8> = (0..100u8).collect();
        let payload = SymmetricMessagePayload::ShortMessage {
            payload: Bytes::from(plaintext.clone()),
        };
        let encrypted = SymmetricMessage::serialize_msg_to_packet_data(1, payload, &cipher, vec![])
            .expect("encrypt");
        let encrypted_bytes = encrypted.data().to_vec();
        let wire_size = encrypted_bytes.len() as u64;
        assert!(
            wire_size > plaintext.len() as u64,
            "encrypted wire size ({wire_size}) must exceed plaintext size ({}) — \
             AEAD tag + nonce + framing always grows the packet",
            plaintext.len()
        );

        // Wrap as the unknown-encryption type the inbound channel actually
        // carries (the listener doesn't know yet whether decryption will
        // succeed).
        let packet = PacketData::<crate::transport::packet_data::UnknownEncryption>::from_buf(
            &encrypted_bytes,
        );
        inbound_tx.send(packet).await.expect("send");

        let metrics_before_recv = crate::transport::TRANSPORT_METRICS
            .per_peer_snapshot()
            .into_iter()
            .find(|(a, _, _)| *a == remote_addr);
        assert!(
            metrics_before_recv.is_none(),
            "test precondition: no entry for our unique remote_addr"
        );
        let cumulative_before = crate::transport::TRANSPORT_METRICS.cumulative_bytes_received();

        let mut conn = PeerConnection::new(remote_conn);
        let _msg = conn.recv().await.expect("recv");

        // Per-peer entry must reflect the wire (encrypted) size — pinning
        // the byte semantics so a future refactor using
        // `decrypted.data().len()` would silently regress this.
        let entry = crate::transport::TRANSPORT_METRICS
            .per_peer_snapshot()
            .into_iter()
            .find(|(a, _, _)| *a == remote_addr)
            .expect("post-auth recv must create a per-peer entry");
        assert_eq!(
            entry.2, wire_size,
            "per-peer bytes_received must equal the encrypted wire size"
        );

        // Cumulative counter must also have moved by at least wire_size.
        // Other concurrent tests may push it higher.
        let cumulative_after = crate::transport::TRANSPORT_METRICS.cumulative_bytes_received();
        assert!(
            cumulative_after >= cumulative_before + wire_size,
            "cumulative_bytes_received must include this packet (before={cumulative_before}, \
             after={cumulative_after}, wire_size={wire_size})"
        );

        // Cleanup so the singleton doesn't leak our address into other tests.
        crate::transport::TRANSPORT_METRICS.remove_peer(remote_addr);
    }

    /// End-to-end regression for the #3321 failure mode (Gap 3 of #3367): a
    /// *burst* of consecutive UDP send failures on a live `PeerConnection`
    /// must NOT tear the connection down. The original #3321 bug had a single
    /// send failure cascade into closing all 11 connections.
    ///
    /// The existing `send_failure_returns_transient_error` /
    /// `transient_send_failure_then_recovery_succeeds` /
    /// `send_failed_kinds_classify_as_transient` tests all drive the STATELESS
    /// `packet_sending` free function — they pin error *classification* but not
    /// that a real `PeerConnection`'s outbound entry point survives multiple
    /// consecutive `io::Error`s and stays usable afterwards. This test closes
    /// that gap by driving `PeerConnection::send` (the public outbound path:
    /// `send` -> `outbound_short_message` -> `packet_sending().await?`) through
    /// a `FailableTestSocket`.
    ///
    /// Survival is proven two ways:
    ///   1. Each failed send returns `Err` classified as transient
    ///      (`is_transient_send_failure()`, NOT `ConnectionClosed`), and the
    ///      `&mut PeerConnection` is neither consumed nor poisoned — we keep
    ///      calling `send` on the same object across the whole burst.
    ///   2. After flipping the failure flag off, the very same connection sends
    ///      successfully and the packet arrives on the rx side — proving the
    ///      connection was never torn down by the burst.
    ///
    /// If the transient-error contract regressed so that a send failure mapped
    /// to `ConnectionClosed` (or any non-transient variant), the per-send
    /// classification assertion below would fail before recovery is reached.
    #[tokio::test]
    async fn connection_survives_burst_of_transient_send_failures() {
        use crate::transport::crypto::TransportKeypair;
        use crate::util::time_source::SharedMockTimeSource;

        let time_source = SharedMockTimeSource::new();
        let (_inbound_tx, inbound_rx) = mpsc::channel(16);
        let remote_addr = SocketAddr::new(Ipv4Addr::new(10, 99, 99, 3).into(), 50003);

        let mut key = [0u8; 16];
        crate::config::GlobalRng::fill_bytes(&mut key);
        let cipher = Aes128Gcm::new(&key.into());
        let keypair = TransportKeypair::new();

        let sent_tracker = Arc::new(parking_lot::Mutex::new(
            SentPacketTracker::new_with_time_source(time_source.clone()),
        ));
        let congestion_controller =
            crate::transport::congestion_control::CongestionControlConfig::default()
                .build_arc_with_time_source(time_source.clone());
        let token_bucket = Arc::new(TokenBucket::new_with_time_source(
            10_000,
            10_000_000,
            time_source.clone(),
        ));

        // Drive the connection's outbound path through a socket whose sends we
        // can flip to failing on demand. The rx end lets us prove a post-burst
        // send actually leaves the connection.
        let (sock_tx, mut sock_rx) = mpsc::channel::<(SocketAddr, Arc<[u8]>)>(16);
        let fail_sends = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let socket = Arc::new(FailableTestSocket::new(sock_tx, fail_sends.clone()));

        let rolling_rtt_stats = crate::transport::rolling_rtt_stats::RollingRttStatsHandle::new(
            remote_addr,
            time_source.clone(),
        );
        let remote_conn = RemoteConnection {
            outbound_symmetric_key: cipher.clone(),
            remote_addr,
            sent_tracker,
            last_packet_id: Arc::new(AtomicU32::new(0)),
            inbound_packet_recv: inbound_rx,
            inbound_symmetric_key: cipher,
            inbound_symmetric_key_bytes: key,
            my_address: None,
            remote_protoc_version: None,
            transport_secret_key: keypair.secret,
            congestion_controller,
            token_bucket,
            socket,
            global_bandwidth: None,
            rolling_rtt_stats,
            time_source,
        };

        let mut conn = PeerConnection::new(remote_conn);

        // Burst phase: several consecutive io::Errors hit the same live
        // connection. Each must surface as a transient error and must NOT
        // consume/poison `conn` — we keep reusing the same `&mut conn`.
        const BURST: usize = 5;
        for attempt in 0..BURST {
            let err = conn
                .send(b"payload".to_vec())
                .await
                .expect_err("send must fail while the socket is failing");
            assert!(
                err.is_transient_send_failure(),
                "burst send #{attempt} must be a transient send failure so the \
                 connection survives (#3321/#3367), got: {err:?}"
            );
            assert!(
                !matches!(err, TransportError::ConnectionClosed(_)),
                "burst send #{attempt} must NOT be ConnectionClosed — a single \
                 send failure tearing down the connection is exactly the #3321 bug"
            );
        }
        // Nothing should have reached the wire during the burst.
        assert!(
            sock_rx.try_recv().is_err(),
            "no packet should leave the socket while sends are failing"
        );

        // Recovery phase: stop failing and send once more on the SAME
        // connection object. Success + an actual packet on the rx side proves
        // the burst never tore the connection down.
        fail_sends.store(false, std::sync::atomic::Ordering::Relaxed);
        conn.send(b"payload".to_vec())
            .await
            .expect("send must succeed once the transient failure clears");
        let (target, _bytes) = sock_rx
            .try_recv()
            .expect("the post-burst send must actually leave the connection");
        assert_eq!(
            target, remote_addr,
            "recovered send must be addressed to the connected peer"
        );
    }

    /// Regression test for #4000: a Pong received for a pending keep-alive
    /// Ping must record an RTT sample, so quiet connections that never
    /// complete a stream transfer still contribute to the RTT statistics.
    ///
    /// Before the fix, the Pong handler removed the matching ping timestamp
    /// from `pending_pings` and discarded it — `record_rtt_sample` was only
    /// reachable from `record_transfer_completed` (stream completion), so a
    /// connection exchanging only keep-alive traffic recorded zero RTT
    /// samples. This test seeds a pending ping, advances the (mock) clock,
    /// feeds an encrypted Pong, and asserts the global RTT sample counter
    /// moved by at least one.
    #[tokio::test]
    async fn pong_records_rtt_sample_from_keepalive_cycle() {
        use crate::transport::crypto::TransportKeypair;
        use crate::transport::packet_data::PacketData;
        use crate::transport::symmetric_message::SymmetricMessagePayload;
        use crate::util::time_source::SharedMockTimeSource;
        use bytes::Bytes;

        let time_source = SharedMockTimeSource::new();
        let (inbound_tx, inbound_rx) = mpsc::channel(16);
        let remote_addr = SocketAddr::new(Ipv4Addr::new(10, 99, 99, 2).into(), 50002);

        let mut key = [0u8; 16];
        crate::config::GlobalRng::fill_bytes(&mut key);
        let cipher = Aes128Gcm::new(&key.into());
        let keypair = TransportKeypair::new();

        let sent_tracker = Arc::new(parking_lot::Mutex::new(
            SentPacketTracker::new_with_time_source(time_source.clone()),
        ));
        let congestion_controller =
            crate::transport::congestion_control::CongestionControlConfig::default()
                .build_arc_with_time_source(time_source.clone());
        let token_bucket = Arc::new(TokenBucket::new_with_time_source(
            10_000,
            10_000_000,
            time_source.clone(),
        ));
        let socket = Arc::new(TestSocket::new(
            mpsc::channel::<(SocketAddr, Arc<[u8]>)>(16).0,
        ));

        let rolling_rtt_stats = crate::transport::rolling_rtt_stats::RollingRttStatsHandle::new(
            remote_addr,
            time_source.clone(),
        );
        let remote_conn = RemoteConnection {
            outbound_symmetric_key: cipher.clone(),
            remote_addr,
            sent_tracker,
            last_packet_id: Arc::new(AtomicU32::new(0)),
            inbound_packet_recv: inbound_rx,
            inbound_symmetric_key: cipher.clone(),
            inbound_symmetric_key_bytes: key,
            my_address: None,
            remote_protoc_version: None,
            transport_secret_key: keypair.secret,
            congestion_controller,
            token_bucket,
            socket,
            global_bandwidth: None,
            rolling_rtt_stats,
            time_source: time_source.clone(),
        };

        // Build the encrypted Pong (sequence 7) and a trailing ShortMessage.
        // The Pong handler does not make `recv()` return (it produces no
        // message), so the ShortMessage gives the loop a value to yield once
        // the Pong has been processed.
        let pong_seq = 7u64;
        let pong = SymmetricMessage::serialize_msg_to_packet_data(
            1,
            SymmetricMessagePayload::Pong { sequence: pong_seq },
            &cipher,
            vec![],
        )
        .expect("encrypt pong");
        let pong_packet =
            PacketData::<crate::transport::packet_data::UnknownEncryption>::from_buf(pong.data());

        let short = SymmetricMessage::serialize_msg_to_packet_data(
            2,
            SymmetricMessagePayload::ShortMessage {
                payload: Bytes::from_static(b"done"),
            },
            &cipher,
            vec![],
        )
        .expect("encrypt short");
        let short_packet =
            PacketData::<crate::transport::packet_data::UnknownEncryption>::from_buf(short.data());

        let mut conn = PeerConnection::new(remote_conn);

        // Advance past zero before seeding so the ping's send timestamp is
        // strictly positive. The recv loop's stale-ping cleanup retains only
        // pings with `sent_at_nanos > now_nanos - idle_timeout`; at small mock
        // times that threshold saturates to 0, so a ping stamped at exactly 0
        // would be pruned before the Pong arrives. Stamp at 10 ms, then
        // advance another 50 ms so the round-trip is a deterministic 50 ms.
        time_source.advance_time(Duration::from_millis(10));
        let send_nanos = time_source.now_nanos();
        conn.pending_pings.write().insert(pong_seq, send_nanos);
        time_source.advance_time(Duration::from_millis(50));

        let samples_before = crate::transport::TRANSPORT_METRICS.rtt_sample_count();

        inbound_tx.send(pong_packet).await.expect("send pong");
        inbound_tx.send(short_packet).await.expect("send short");

        let _msg = conn.recv().await.expect("recv");

        let samples_after = crate::transport::TRANSPORT_METRICS.rtt_sample_count();
        assert!(
            samples_after > samples_before,
            "Pong for a pending ping must record at least one RTT sample \
             (before={samples_before}, after={samples_after}). \
             The keep-alive RTT path (#4000) is not wired up."
        );

        // The matching ping must have been consumed so it isn't sampled twice
        // or treated as still-unanswered by the keep-alive backoff.
        assert!(
            !conn.pending_pings.read().contains_key(&pong_seq),
            "Pong must remove the matching pending ping"
        );

        crate::transport::TRANSPORT_METRICS.remove_peer(remote_addr);
    }

    /// Regression test for #4079.
    ///
    /// Reproduces both leak scenarios called out in the issue:
    ///
    /// 1. The orphan registry GC cancelled the handle on its 5 s tick but
    ///    didn't notify the owning `PeerConnection`, so the
    ///    `streaming_handles` entry stayed pinned (and with it the
    ///    `Arc<LockFreeStreamBuffer>` pre-allocation).
    /// 2. The operations driver claimed the orphan, hit an internal timeout,
    ///    and dropped its clone — but no inbound fragment was ever going to
    ///    arrive again to trip the `Cancelled` path that removes the entry.
    ///
    /// Both scenarios share the same observable state: a stale entry in
    /// `streaming_handles` with no further activity. The sweep treats them
    /// identically — drop entries whose `last_activity_nanos` is older than
    /// the configured `streaming_handle_idle_timeout()`.
    ///
    /// Without the fix `sweep_streaming_handles_inner` does not exist and the
    /// stale entry persists for the life of the connection. With the fix,
    /// stale entries are removed, the underlying handle is cancelled (so any
    /// late fragment short-circuits on `StreamError::Cancelled` instead of
    /// allocating), and the companion `streaming_registry` entry is dropped.
    #[test]
    fn sweep_streaming_handles_inner_drops_idle_entries() {
        use streaming::{StreamError, StreamRegistry};
        // Threshold from the production constant: 30 s. Build "stale" and
        // "fresh" timestamps relative to a synthetic `now` so we don't depend
        // on wall-clock or test runtime speed.
        let now_nanos = STREAMING_HANDLE_IDLE_TIMEOUT_PROD.as_nanos() as u64 * 4;
        let stale_nanos = 0u64; // idle window much greater than threshold
        let fresh_nanos = now_nanos.saturating_sub(Duration::from_secs(1).as_nanos() as u64);

        let registry = StreamRegistry::new();
        let stale_id = StreamId::next();
        let fresh_id = StreamId::next();
        // `register` returns a fresh handle and also stores a clone inside
        // the registry — mirroring the production code path at
        // `process_inbound` line 1383.
        let stale_handle = registry.register(stale_id, 64 * 1024);
        let fresh_handle = registry.register(fresh_id, 64 * 1024);

        // Keep our own clone of the stale handle so we can prove it was
        // cancelled by the sweep (push_fragment must return Cancelled after).
        let stale_clone = stale_handle.clone();

        let mut streaming_handles: HashMap<StreamId, (streaming::StreamHandle, u64)> =
            HashMap::new();
        streaming_handles.insert(stale_id, (stale_handle, stale_nanos));
        streaming_handles.insert(fresh_id, (fresh_handle, fresh_nanos));

        assert_eq!(streaming_handles.len(), 2);
        assert_eq!(registry.stream_count(), 2);

        let peer_addr = SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 9000);
        sweep_streaming_handles_inner(
            &mut streaming_handles,
            &registry,
            now_nanos,
            STREAMING_HANDLE_IDLE_TIMEOUT_PROD,
            peer_addr,
        );

        // Stale entry removed from BOTH maps; fresh entry untouched.
        assert_eq!(
            streaming_handles.len(),
            1,
            "stale entry should have been swept out of streaming_handles"
        );
        assert!(streaming_handles.contains_key(&fresh_id));
        assert!(!streaming_handles.contains_key(&stale_id));
        assert_eq!(
            registry.stream_count(),
            1,
            "stale entry should have been removed from streaming_registry"
        );

        // Stale handle was cancelled: any fragment that finally arrives short
        // circuits on Cancelled, which is what the existing `process_inbound`
        // line-1361 path uses to clean up.
        let push_result =
            stale_clone.push_fragment(1, bytes::Bytes::from_static(b"would-leak-without-fix"));
        assert!(
            matches!(push_result, Err(StreamError::Cancelled)),
            "stale handle should be cancelled; got {:?}",
            push_result
        );
    }

    /// A second, narrower assertion: when every entry is within the idle
    /// window the sweep is a strict no-op (no spurious cancellation of
    /// in-flight streams). Pins the lower bound of the threshold so a future
    /// reduction of `STREAMING_HANDLE_IDLE_TIMEOUT_PROD` can't accidentally
    /// reap an actively-receiving stream.
    #[test]
    fn sweep_streaming_handles_inner_keeps_recent_entries() {
        use streaming::StreamRegistry;
        let registry = StreamRegistry::new();
        let id = StreamId::next();
        let handle = registry.register(id, 64 * 1024);

        let now_nanos = Duration::from_secs(3600).as_nanos() as u64;
        // Last activity was JUST under the threshold — sweep must keep this entry.
        let last_activity_nanos =
            now_nanos.saturating_sub(STREAMING_HANDLE_IDLE_TIMEOUT_PROD.as_nanos() as u64) + 1;

        let mut streaming_handles: HashMap<StreamId, (streaming::StreamHandle, u64)> =
            HashMap::new();
        streaming_handles.insert(id, (handle.clone(), last_activity_nanos));

        let peer_addr = SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 9001);
        sweep_streaming_handles_inner(
            &mut streaming_handles,
            &registry,
            now_nanos,
            STREAMING_HANDLE_IDLE_TIMEOUT_PROD,
            peer_addr,
        );

        assert_eq!(
            streaming_handles.len(),
            1,
            "in-window entry must not be swept"
        );
        assert_eq!(registry.stream_count(), 1);
        // And the kept handle must still be usable for incoming fragments.
        let result = handle.push_fragment(1, bytes::Bytes::from_static(b"still-alive"));
        assert!(
            result.is_ok(),
            "in-window handle must not be cancelled by sweep, got {:?}",
            result
        );
    }

    /// Boundary pin: confirms the `idle_nanos > threshold_nanos` comparison
    /// (strict greater-than). An entry at exactly the threshold must be
    /// KEPT; one even a single nanosecond past must be DROPPED. Without this
    /// test, a future change to `>=` (or vice versa) would silently shift
    /// the eviction window and could either reap not-yet-stalled streams
    /// (`>=`) or leak by one tick (`>` with off-by-one in the body).
    #[test]
    fn sweep_streaming_handles_inner_threshold_boundary_is_strict_greater_than() {
        use streaming::StreamRegistry;
        let registry = StreamRegistry::new();
        let exact_id = StreamId::next();
        let past_id = StreamId::next();
        let exact_handle = registry.register(exact_id, 64 * 1024);
        let past_handle = registry.register(past_id, 64 * 1024);

        let threshold_nanos = STREAMING_HANDLE_IDLE_TIMEOUT_PROD.as_nanos() as u64;
        let now_nanos = threshold_nanos * 10;
        // idle = exactly threshold -> KEEP
        let exact_last_activity = now_nanos - threshold_nanos;
        // idle = threshold + 1 ns -> DROP
        let past_last_activity = now_nanos - threshold_nanos - 1;

        let mut streaming_handles: HashMap<StreamId, (streaming::StreamHandle, u64)> =
            HashMap::new();
        streaming_handles.insert(exact_id, (exact_handle, exact_last_activity));
        streaming_handles.insert(past_id, (past_handle, past_last_activity));

        let peer_addr = SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 9002);
        sweep_streaming_handles_inner(
            &mut streaming_handles,
            &registry,
            now_nanos,
            STREAMING_HANDLE_IDLE_TIMEOUT_PROD,
            peer_addr,
        );

        assert!(
            streaming_handles.contains_key(&exact_id),
            "entry at idle = threshold must be KEPT (strict greater-than)"
        );
        assert!(
            !streaming_handles.contains_key(&past_id),
            "entry at idle = threshold + 1 ns must be DROPPED"
        );
    }

    /// A second sweep over already-swept state must be a strict no-op and
    /// must not double-cancel any handle. `timeout_check` fires every 5 s
    /// for the life of the connection so the sweep runs many times after
    /// the entry is gone — that must remain harmless.
    #[test]
    fn sweep_streaming_handles_inner_is_idempotent_on_repeat() {
        use streaming::StreamRegistry;
        let registry = StreamRegistry::new();
        let id = StreamId::next();
        let handle = registry.register(id, 64 * 1024);
        let now_nanos = STREAMING_HANDLE_IDLE_TIMEOUT_PROD.as_nanos() as u64 * 4;

        let mut streaming_handles: HashMap<StreamId, (streaming::StreamHandle, u64)> =
            HashMap::new();
        streaming_handles.insert(id, (handle, 0));

        let peer_addr = SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 9003);
        sweep_streaming_handles_inner(
            &mut streaming_handles,
            &registry,
            now_nanos,
            STREAMING_HANDLE_IDLE_TIMEOUT_PROD,
            peer_addr,
        );
        assert_eq!(streaming_handles.len(), 0);
        assert_eq!(registry.stream_count(), 0);

        // A second sweep with the map already empty must not panic, must not
        // log spuriously, and must leave both maps at zero.
        sweep_streaming_handles_inner(
            &mut streaming_handles,
            &registry,
            now_nanos,
            STREAMING_HANDLE_IDLE_TIMEOUT_PROD,
            peer_addr,
        );
        assert_eq!(streaming_handles.len(), 0);
        assert_eq!(registry.stream_count(), 0);
    }

    /// The leak this PR fixes is fundamentally about the
    /// `Arc<LockFreeStreamBuffer>` (a pre-allocated slot array sized for the
    /// full stream) staying alive. The map-cardinality assertions above are
    /// observable proxies; this test asserts the actual invariant — after
    /// sweep, the only surviving Arc reference is the one the test itself
    /// holds. If a future change adds another internal owner (e.g., the
    /// orphan registry now holds a third clone), the cardinality tests
    /// would still pass but this one would fail.
    #[test]
    fn sweep_streaming_handles_inner_releases_buffer_arc() {
        use streaming::StreamRegistry;
        let registry = StreamRegistry::new();
        let id = StreamId::next();
        let handle = registry.register(id, 64 * 1024);

        // After register: registry holds 1 clone, our local `handle` holds 1.
        // The handle's internal Arc<LockFreeStreamBuffer> is shared by both,
        // so strong_count starts at 2.
        let initial_strong = handle.buffer_strong_count();
        assert_eq!(
            initial_strong, 2,
            "registry + local handle = 2 buffer clones at start"
        );

        let mut streaming_handles: HashMap<StreamId, (streaming::StreamHandle, u64)> =
            HashMap::new();
        streaming_handles.insert(id, (handle.clone(), 0));
        // Now: registry + local handle + map = 3 Arc clones of the buffer.
        assert_eq!(handle.buffer_strong_count(), 3);

        let now_nanos = STREAMING_HANDLE_IDLE_TIMEOUT_PROD.as_nanos() as u64 * 4;
        let peer_addr = SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 9004);
        sweep_streaming_handles_inner(
            &mut streaming_handles,
            &registry,
            now_nanos,
            STREAMING_HANDLE_IDLE_TIMEOUT_PROD,
            peer_addr,
        );

        // Sweep dropped the map clone and the registry clone — only our
        // local `handle` clone of the buffer remains.
        assert_eq!(
            handle.buffer_strong_count(),
            1,
            "post-sweep, only the test's local handle clone of \
             Arc<LockFreeStreamBuffer> should remain — production code \
             must release the slot-array allocation, see issue #4079"
        );
    }

    /// Mirrors production load: the per-peer `streaming_handles` map can
    /// hold hundreds of entries spanning a range of last-activity times
    /// (report `JTDY6J` showed peak 230 outstanding). One sweep pass must
    /// correctly partition them by the threshold in a single iteration.
    #[test]
    fn sweep_streaming_handles_inner_partitions_mixed_age_entries() {
        use streaming::StreamRegistry;
        let registry = StreamRegistry::new();
        let threshold_nanos = STREAMING_HANDLE_IDLE_TIMEOUT_PROD.as_nanos() as u64;
        let now_nanos = threshold_nanos * 100;

        let mut streaming_handles: HashMap<StreamId, (streaming::StreamHandle, u64)> =
            HashMap::new();
        let mut expected_to_keep = Vec::new();
        let mut expected_to_drop = Vec::new();
        for i in 0..10u64 {
            let id = StreamId::next();
            let handle = registry.register(id, 64 * 1024);
            // i = 0..5 -> within window (kept); i = 5..10 -> outside (dropped).
            let last_activity_nanos = if i < 5 {
                now_nanos - (threshold_nanos / 2) // 15 s idle
            } else {
                now_nanos - threshold_nanos * 2 // 60 s idle
            };
            streaming_handles.insert(id, (handle, last_activity_nanos));
            if i < 5 {
                expected_to_keep.push(id);
            } else {
                expected_to_drop.push(id);
            }
        }
        assert_eq!(streaming_handles.len(), 10);
        assert_eq!(registry.stream_count(), 10);

        let peer_addr = SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 9005);
        sweep_streaming_handles_inner(
            &mut streaming_handles,
            &registry,
            now_nanos,
            STREAMING_HANDLE_IDLE_TIMEOUT_PROD,
            peer_addr,
        );

        assert_eq!(streaming_handles.len(), 5, "5 fresh entries should survive");
        assert_eq!(
            registry.stream_count(),
            5,
            "5 fresh registry rows should survive"
        );
        for id in &expected_to_keep {
            assert!(streaming_handles.contains_key(id), "kept entry missing");
        }
        for id in &expected_to_drop {
            assert!(
                !streaming_handles.contains_key(id),
                "dropped entry still present"
            );
        }
    }

    /// Pin test: the production call site at `timeout_check.tick()` MUST
    /// invoke `sweep_idle_streaming_handles()`. Both unit tests above call
    /// `sweep_streaming_handles_inner` directly — they would still pass
    /// if a future refactor accidentally deleted the call from the `recv()`
    /// loop, silently re-introducing the #4079 leak with green CI. This
    /// pin catches that exact failure mode by greppting the source.
    #[test]
    fn sweep_idle_streaming_handles_is_invoked_from_timeout_check() {
        let source = include_str!("peer_connection.rs");
        assert!(
            source.contains("self.sweep_idle_streaming_handles();"),
            "PR #4083 / issue #4079: recv()'s timeout_check.tick() arm must \
             call self.sweep_idle_streaming_handles() — without it the \
             streaming_handles leak comes back. If you intentionally moved \
             the call, update this pin (and ensure the new site fires at \
             least as often as the 5 s timeout_check tick)."
        );
    }

    /// `streaming_handle_idle_timeout()` MUST honor the same simulation flag
    /// (`SimulationIdleTimeout`) that gates `connection_idle_timeout()`,
    /// otherwise the direct simulation runner — where virtual time can jump
    /// past 30s when a task awaits `spawn_blocking` (WASM execution) — will
    /// falsely reap in-flight streaming-relay handles, producing flaky CI
    /// failures. Pattern mirrors `simulation/time.rs::connection_idle_timeout`.
    #[test]
    fn streaming_handle_idle_timeout_switches_to_sim_value_when_flag_enabled() {
        use crate::config::SimulationIdleTimeout;

        // Test runs on its own thread (each `#[test]` is a fresh thread).
        // The flag is thread-local, so toggling here cannot disturb other tests.
        SimulationIdleTimeout::disable();
        assert_eq!(
            streaming_handle_idle_timeout(),
            STREAMING_HANDLE_IDLE_TIMEOUT_PROD,
            "production threshold required when flag is OFF"
        );

        SimulationIdleTimeout::enable();
        assert_eq!(
            streaming_handle_idle_timeout(),
            STREAMING_HANDLE_IDLE_TIMEOUT_SIM,
            "simulation threshold required when flag is ON"
        );

        // Restore so a later test inheriting this thread isn't surprised.
        SimulationIdleTimeout::disable();
    }

    /// Pins the `last_activity_nanos` refresh path in `process_inbound`
    /// against the only progress signal that should advance the sweep
    /// deadline. Background: the original fix refreshed the timestamp
    /// before checking `push_fragment`'s return value, which Codex flagged
    /// as a replay-extension vector (a peer can keep a dead stream alive
    /// indefinitely by replaying the same fragment every <30 s). The fix
    /// gates the refresh on `Ok(true)` — actual new fragment inserted.
    ///
    /// This test combines a behavioral exercise of `StreamHandle::push_fragment`
    /// (so the test breaks if the underlying API ever stops returning
    /// `Ok(false)` for duplicates or `Err(InvalidFragment)` for OOB indices)
    /// with a source-grep pin on the production guard. Without both, a
    /// regression that loosens the guard (e.g., to `Ok(_)`) or removes it
    /// entirely would silently re-open the replay vector while leaving all
    /// the sweep-mechanic tests green.
    #[test]
    fn last_activity_refresh_strictly_gated_on_ok_true() {
        use streaming::{StreamError, StreamRegistry};
        use streaming_buffer::FRAGMENT_PAYLOAD_SIZE;

        // --- Part 1: behavioral pin on push_fragment's return values ---
        let registry = StreamRegistry::new();
        let id = StreamId::next();
        // 2 fragments + 1 overflow slot = 3 valid indices; index 99 is OOB.
        let handle = registry.register(id, (FRAGMENT_PAYLOAD_SIZE * 2) as u64);

        let r_new = handle.push_fragment(1, bytes::Bytes::from(vec![1u8; FRAGMENT_PAYLOAD_SIZE]));
        assert!(
            matches!(r_new, Ok(true)),
            "first push of fragment 1 must be Ok(true); got {:?}",
            r_new
        );

        let r_dup = handle.push_fragment(1, bytes::Bytes::from(vec![1u8; FRAGMENT_PAYLOAD_SIZE]));
        assert!(
            matches!(r_dup, Ok(false)),
            "duplicate push of fragment 1 must be Ok(false); got {:?}",
            r_dup
        );

        let r_invalid = handle.push_fragment(99, bytes::Bytes::from_static(b"oob"));
        assert!(
            matches!(r_invalid, Err(StreamError::InvalidFragment { .. })),
            "out-of-range push must be Err(InvalidFragment); got {:?}",
            r_invalid
        );

        // --- Part 2: source pin on the production refresh guard ---
        let source = include_str!("peer_connection.rs");
        assert!(
            source.contains("if matches!(push_result, Ok(true)) {"),
            "PR #4083 / issue #4079: `last_activity_nanos` refresh in \
             process_inbound must be gated on Ok(true) ONLY. Loosening \
             this to Ok(_) or removing the guard re-opens the replay \
             vector that lets a peer keep a dead stream alive \
             indefinitely by replaying the same fragment every <30 s. \
             See the comment block above the guard in process_inbound."
        );
        assert!(
            source.contains("Refresh `last_activity_nanos` ONLY on real forward"),
            "PR #4083 / issue #4079: the production code block explaining \
             why the refresh is gated on Ok(true) must remain in place. \
             If you intentionally rewrote it, update this pin to match \
             the new comment."
        );
    }
}
