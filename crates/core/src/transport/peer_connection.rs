use std::collections::BTreeMap;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;

use crate::config::GlobalExecutor;
use crate::simulation::{RealTime, TimeSource, TimeSourceInterval};
use crate::transport::connection_handler::NAT_TRAVERSAL_MAX_ATTEMPTS;
use crate::transport::crypto::TransportSecretKey;
use crate::transport::fast_channel::{self, FastReceiver, FastSender};
use crate::transport::packet_data::UnknownEncryption;
use crate::transport::sent_packet_tracker::MESSAGE_CONFIRMATION_TIMEOUT;
use aes_gcm::Aes128Gcm;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tracing::{instrument, span, Instrument};

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
    connection_handler::SerializedMessage,
    global_bandwidth::GlobalBandwidthManager,
    ledbat::LedbatController,
    packet_data::{self, PacketData},
    received_packet_tracker::ReceivedPacketTracker,
    received_packet_tracker::ReportResult,
    sent_packet_tracker::{ResendAction, SentPacketTracker},
    symmetric_message::{self, SymmetricMessage, SymmetricMessagePayload},
    token_bucket::TokenBucket,
    TransportError,
};
use crate::util::time_source::InstantTimeSrc;

type Result<T = (), E = TransportError> = std::result::Result<T, E>;

/// Result type for outbound stream transfers, includes transfer statistics.
type OutboundStreamResult = std::result::Result<super::TransferStats, TransportError>;

/// The max payload we can send in a single fragment, this MUST be less than packet_data::MAX_DATA_SIZE
/// since we need to account for the space overhead of SymmetricMessage::StreamFragment metadata.
/// Measured overhead: 40 bytes (see symmetric_message::stream_fragment_overhead())
const MAX_DATA_SIZE: usize = packet_data::MAX_DATA_SIZE - 40;

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

#[must_use]
pub(crate) struct RemoteConnection<S = super::UdpSocket, T: TimeSource = RealTime> {
    pub(super) outbound_symmetric_key: Aes128Gcm,
    pub(super) remote_addr: SocketAddr,
    pub(super) sent_tracker: Arc<parking_lot::Mutex<SentPacketTracker<T>>>,
    pub(super) last_packet_id: Arc<AtomicU32>,
    pub(super) inbound_packet_recv: FastReceiver<PacketData<UnknownEncryption>>,
    pub(super) inbound_symmetric_key: Aes128Gcm,
    pub(super) inbound_symmetric_key_bytes: [u8; 16],
    #[allow(dead_code)]
    pub(super) my_address: Option<SocketAddr>,
    pub(super) transport_secret_key: TransportSecretKey,
    /// LEDBAT congestion controller (RFC 6817) - adapts to network conditions
    pub(super) ledbat: Arc<LedbatController<T>>,
    /// Token bucket rate limiter - smooths packet pacing based on LEDBAT rate
    pub(super) token_bucket: Arc<TokenBucket<T>>,
    /// Socket for direct packet sending (bypasses centralized rate limiter)
    pub(super) socket: Arc<S>,
    /// Global bandwidth manager for fair sharing across connections.
    /// When Some, the token_bucket rate is periodically updated based on connection count.
    pub(super) global_bandwidth: Option<Arc<GlobalBandwidthManager>>,
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

/// Static counter for StreamId generation - must be at module level for reset access
static STREAM_ID_COUNTER: AtomicU32 = AtomicU32::new(0);

impl StreamId {
    pub fn next() -> Self {
        Self(STREAM_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Release))
    }

    /// Reset the stream ID counter to initial state.
    /// Used for deterministic simulation testing.
    pub fn reset_counter() {
        STREAM_ID_COUNTER.store(0, std::sync::atomic::Ordering::SeqCst);
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
    inbound_streams: HashMap<StreamId, FastSender<(u32, bytes::Bytes)>>,
    inbound_stream_futures: FuturesUnordered<JoinHandle<InboundStreamResult>>,
    outbound_stream_futures: FuturesUnordered<JoinHandle<OutboundStreamResult>>,
    failure_count: usize,
    /// First failure time as nanoseconds since time_source epoch
    first_failure_time_nanos: Option<u64>,
    /// Last packet report time as nanoseconds since time_source epoch
    last_packet_report_time_nanos: u64,
    keep_alive_handle: Option<JoinHandle<()>>,
    /// Last rate update time as nanoseconds since time_source epoch
    /// Used to implement RTT-adaptive rate updates (update approximately once per RTT)
    last_rate_update_nanos: Option<u64>,
    /// Tracks pending ping probes awaiting pong responses.
    /// Maps ping sequence number -> send timestamp (nanoseconds since time_source epoch).
    /// Used for bidirectional liveness detection.
    pending_pings: Arc<RwLock<BTreeMap<u64, u64>>>,
    /// Registry for streaming inbound streams.
    /// Maps stream IDs to streaming handles for incremental fragment access.
    streaming_registry: Arc<streaming::StreamRegistry>,
    /// Streaming handles for active streams (parallels inbound_streams).
    /// Used for pushing fragments to streaming consumers.
    streaming_handles: HashMap<StreamId, streaming::StreamHandle>,
    /// Time source for deterministic simulation support
    time_source: T,
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
        if let Some(handle) = self.keep_alive_handle.take() {
            tracing::debug!(
                peer_addr = %self.remote_conn.remote_addr,
                "Cancelling keep-alive task"
            );
            handle.abort();
        }
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

#[allow(private_bounds)]
impl<S: super::Socket, T: TimeSource> PeerConnection<S, T> {
    pub(super) fn new(remote_conn: RemoteConnection<S, T>) -> Self {
        const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(5);

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
        let keep_alive_handle = GlobalExecutor::spawn(async move {
            tracing::info!(
                target: "freenet_core::transport::keepalive_lifecycle",
                remote = ?remote_addr,
                "Keep-alive task STARTED for connection"
            );

            let mut interval =
                TimeSourceInterval::new(task_time_source.clone(), KEEP_ALIVE_INTERVAL);

            // Skip the first immediate tick
            interval.tick().await;

            let mut tick_count = 0u64;
            let mut ping_seq = 0u64;
            let task_start_nanos = task_time_source.now_nanos();

            loop {
                let tick_start_nanos = task_time_source.now_nanos();
                interval.tick().await;
                tick_count += 1;

                let now_nanos = task_time_source.now_nanos();
                let elapsed_since_start_nanos = now_nanos.saturating_sub(task_start_nanos);
                let elapsed_since_last_tick_nanos = now_nanos.saturating_sub(tick_start_nanos);

                // Use local ping sequence counter
                let current_ping_seq = ping_seq;
                ping_seq += 1;

                tracing::debug!(
                    target: "freenet_core::transport::keepalive_lifecycle",
                    remote = ?remote_addr,
                    tick_count,
                    ping_sequence = current_ping_seq,
                    elapsed_since_start_secs = Duration::from_nanos(elapsed_since_start_nanos).as_secs_f64(),
                    tick_interval_ms = Duration::from_nanos(elapsed_since_last_tick_nanos).as_millis(),
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
            keep_alive_handle: Some(keep_alive_handle),
            last_rate_update_nanos: None,
            pending_pings,
            streaming_registry: Arc::new(streaming::StreamRegistry::new()),
            streaming_handles: HashMap::new(),
            time_source,
        }
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
        // listen for incoming messages or receipts or wait until is time to do anything else again
        let mut resend_check_sleep: Option<
            std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
        > = Some(self.time_source.sleep(Duration::from_millis(10)));

        let kill_connection_after = self.time_source.connection_idle_timeout();
        let kill_connection_after_nanos = kill_connection_after.as_nanos() as u64;
        let mut last_received_nanos = self.time_source.now_nanos();

        // Check for timeout periodically
        let mut timeout_check =
            TimeSourceInterval::new(self.time_source.clone(), Duration::from_secs(5));

        // Background ACK timer - sends pending ACKs proactively every 100ms
        // This prevents delays when there's no outgoing traffic to piggyback ACKs on
        // Use interval_at to delay the first tick - unlike the keep-alive task which can
        // block to skip its first tick, we're inside a select! loop so we delay instead
        let ack_start_nanos = self.time_source.now_nanos() + ACK_CHECK_INTERVAL.as_nanos() as u64;
        let mut ack_check = TimeSourceInterval::new_at(
            self.time_source.clone(),
            ack_start_nanos,
            ACK_CHECK_INTERVAL,
        );

        // Rate update timer - updates TokenBucket rate based on LEDBAT cwnd every 100ms
        // This allows the token bucket to adapt to network conditions dynamically
        let rate_start_nanos = self.time_source.now_nanos() + ACK_CHECK_INTERVAL.as_nanos() as u64;
        let mut rate_update_check = TimeSourceInterval::new_at(
            self.time_source.clone(),
            rate_start_nanos,
            ACK_CHECK_INTERVAL,
        );

        const FAILURE_TIME_WINDOW: Duration = Duration::from_secs(30);
        const FAILURE_TIME_WINDOW_NANOS: u64 = FAILURE_TIME_WINDOW.as_nanos() as u64;
        loop {
            // tracing::trace!(remote = ?self.remote_conn.remote_addr, "waiting for inbound messages");
            tokio::select! {
                inbound = self.remote_conn.inbound_packet_recv.recv_async() => {
                    let packet_data = inbound.map_err(|_| TransportError::ConnectionClosed(self.remote_addr()))?;
                    last_received_nanos = self.time_source.now_nanos();

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
                                let elapsed_nanos = self.time_source.now_nanos().saturating_sub(last_received_nanos);
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
                            // Remove the corresponding ping from pending set
                            let mut pending = self.pending_pings.write();
                            if pending.remove(sequence).is_some() {
                                tracing::trace!(
                                    target: "freenet_core::transport::keepalive_received",
                                    remote = ?self.remote_conn.remote_addr,
                                    pong_sequence = sequence,
                                    remaining_pending = pending.len(),
                                    "Removed acknowledged ping from pending set"
                                );
                            }
                        }
                        _ => {}
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

                    // Process ACKs and update LEDBAT congestion controller
                    let (ack_info, _loss_rate) = self.remote_conn
                        .sent_tracker
                        .lock()
                        .report_received_receipts(&confirm_receipt);

                    // Feed ACK info to LEDBAT for congestion window adjustment
                    // All ACKs decrement flightsize, but only non-retransmitted packets
                    // update RTT estimation (Karn's algorithm)
                    for (rtt_sample_opt, packet_size) in ack_info {
                        match rtt_sample_opt {
                            Some(rtt_sample) => {
                                // Normal packet: full RTT processing + flightsize decrement
                                self.remote_conn.ledbat.on_ack(rtt_sample, packet_size);
                            }
                            None => {
                                // Retransmitted packet: only decrement flightsize (no RTT update)
                                self.remote_conn.ledbat.on_ack_without_rtt(packet_size);
                            }
                        }
                    }

                    let report_result = self.received_tracker.report_received_packet(packet_id);
                    match (report_result, should_send_receipts) {
                        (ReportResult::QueueFull, _) | (_, true) => {
                            let receipts = self.received_tracker.get_receipts();
                            if !receipts.is_empty() {
                                self.noop(receipts).await?;
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
                        tracing::trace!(
                            peer_addr = %self.remote_conn.remote_addr,
                            packet_id,
                            "Returning full stream message"
                        );
                        return Ok(msg);
                    }
                }
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
                    tracing::trace!(
                        peer_addr = %self.remote_conn.remote_addr,
                        stream_id = %stream_id,
                        "Stream finished"
                    );
                    return Ok(msg);
                }
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
                        Err(e) => return Err(e),
                    }
                }
                _ = timeout_check.tick() => {
                    let now_nanos = self.time_source.now_nanos();
                    let elapsed_nanos = now_nanos.saturating_sub(last_received_nanos);
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

                        // Check if keep-alive task is still alive
                        if let Some(ref handle) = self.keep_alive_handle {
                            if !handle.is_finished() {
                                tracing::error!(
                                    target: "freenet_core::transport::keepalive_timeout",
                                    remote = ?self.remote_conn.remote_addr,
                                    "Keep-alive task is STILL RUNNING despite timeout!"
                                );
                            } else {
                                tracing::error!(
                                    target: "freenet_core::transport::keepalive_timeout",
                                    remote = ?self.remote_conn.remote_addr,
                                    "Keep-alive task has ALREADY FINISHED before timeout!"
                                );
                            }
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

                    // Check for bidirectional liveness failure (too many unanswered pings).
                    // This catches asymmetric failures where we receive packets from remote
                    // (so last_received is updated) but our packets don't reach the remote
                    // (so our pings never get ponged back).
                    if pending_ping_count > MAX_UNANSWERED_PINGS {
                        tracing::warn!(
                            target: "freenet_core::transport::keepalive_timeout",
                            remote = ?self.remote_conn.remote_addr,
                            pending_pings = pending_ping_count,
                            max_unanswered = MAX_UNANSWERED_PINGS,
                            time_since_last_received_secs = elapsed.as_secs_f64(),
                            "BIDIRECTIONAL LIVENESS FAILURE - {} pings unanswered (max {}), \
                             connection appears asymmetric",
                            pending_ping_count,
                            MAX_UNANSWERED_PINGS
                        );
                        return Err(TransportError::ConnectionClosed(self.remote_addr()));
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
                }
                _ = async { resend_check_sleep.take().unwrap_or(Box::pin(std::future::ready(()))).await } => {
                    loop {
                        tracing::trace!(
                            peer_addr = %self.remote_conn.remote_addr,
                            "Checking for resends"
                        );
                        let maybe_resend = self.remote_conn
                            .sent_tracker
                            .lock()
                            .get_resend();
                        match maybe_resend {
                            ResendAction::WaitUntil(deadline_nanos) => {
                                resend_check_sleep = Some(self.time_source.sleep_until(deadline_nanos));
                                break;
                            }
                            ResendAction::Resend(idx, packet) => {
                                // Notify LEDBAT of packet loss (timeout-based retransmission)
                                self.remote_conn.ledbat.on_timeout();

                                self.remote_conn
                                    .socket
                                    .send_to(&packet, self.remote_conn.remote_addr)
                                    .await
                                    .map_err(|_| TransportError::ConnectionClosed(self.remote_addr()))?;
                                // Re-register packet for ACK tracking. Note: on_send() is NOT called here
                                // because the bytes were already counted in flightsize during the initial send.
                                // When the retransmitted packet is ACKed, on_ack_without_rtt() will decrement
                                // flightsize once, maintaining correct accounting.
                                self.remote_conn.sent_tracker.lock().report_sent_packet(idx, packet);
                            }
                        }
                    }
                }
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
                        self.noop(receipts).await?;
                    }
                }
                // Rate update timer - update TokenBucket rate based on LEDBAT cwnd
                // RTT-adaptive: only update if at least one RTT has elapsed since last update
                _ = rate_update_check.tick() => {
                    let now_nanos = self.time_source.now_nanos();

                    // Use LEDBAT's base delay for rate calculation (consistent with its internal state)
                    // Fallback to min_rtt only if base_delay is not yet established
                    let base_delay = self.remote_conn.ledbat.base_delay();
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
                        let ledbat_rate = self.remote_conn.ledbat.current_rate(rtt);
                        let cwnd = self.remote_conn.ledbat.current_cwnd();
                        let queuing_delay = self.remote_conn.ledbat.queuing_delay();

                        // Apply global bandwidth limit if configured
                        // Take minimum of LEDBAT rate and global fair-share rate
                        let (new_rate, global_limit) = if let Some(ref global) =
                            self.remote_conn.global_bandwidth
                        {
                            let global_rate = global.current_per_connection_rate();
                            (ledbat_rate.min(global_rate), Some(global_rate))
                        } else {
                            (ledbat_rate, None)
                        };

                        // Calculate time since last update for debugging RTT-adaptive timing
                        let since_last_update_ms = self.last_rate_update_nanos
                            .map(|last| Duration::from_nanos(now_nanos.saturating_sub(last)).as_millis())
                            .unwrap_or(0);

                        self.remote_conn.token_bucket.set_rate(new_rate);
                        self.last_rate_update_nanos = Some(now_nanos);

                        tracing::debug!(
                            peer_addr = %self.remote_conn.remote_addr,
                            // Rate control
                            new_rate_bytes_per_sec = new_rate,
                            new_rate_mbps = (new_rate as f64) / 1_000_000.0,
                            ledbat_rate_mbps = (ledbat_rate as f64) / 1_000_000.0,
                            global_limit_mbps = global_limit.map(|r| (r as f64) / 1_000_000.0),
                            // LEDBAT state
                            cwnd_bytes = cwnd,
                            cwnd_packets = cwnd / MAX_DATA_SIZE,
                            // Delay measurements
                            base_delay_ms = base_delay.as_millis(),
                            rtt_ms = rtt.as_millis(),
                            queuing_delay_ms = queuing_delay.as_millis(),
                            target_delay_ms = 100, // TARGET constant
                            // Derived metrics
                            off_target_ms = (queuing_delay.as_millis() as i64) - 100,
                            since_last_update_ms = since_last_update_ms,
                            "LEDBAT metrics (RTT-adaptive rate update)"
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
        self.streaming_handles.get(&stream_id).cloned()
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
            ShortMessage { payload } => Ok(Some(payload.to_vec())),
            AckConnection { result: Err(cause) } => {
                Err(TransportError::ConnectionEstablishmentFailure { cause })
            }
            AckConnection { result: Ok(_) } => {
                let packet = SymmetricMessage::ack_ok(
                    &self.remote_conn.outbound_symmetric_key,
                    self.remote_conn.inbound_symmetric_key_bytes,
                    self.remote_conn.remote_addr,
                )?;
                self.remote_conn
                    .socket
                    .send_to(packet.data(), self.remote_conn.remote_addr)
                    .await
                    .map_err(|_| TransportError::ConnectionClosed(self.remote_addr()))?;
                Ok(None)
            }
            StreamFragment {
                stream_id,
                total_length_bytes,
                fragment_number,
                payload,
            } => {
                // Push to streaming handle for incremental access (Phase 1 streaming)
                if let Some(streaming_handle) = self.streaming_handles.get(&stream_id) {
                    // Existing streaming handle - push fragment
                    if let Err(e) = streaming_handle.push_fragment(fragment_number, payload.clone())
                    {
                        tracing::warn!(
                            peer_addr = %self.remote_conn.remote_addr,
                            stream_id = %stream_id,
                            fragment_number,
                            error = %e,
                            "Failed to push fragment to streaming handle"
                        );
                    }
                } else {
                    // New stream - register with streaming registry
                    let streaming_handle = self
                        .streaming_registry
                        .register(stream_id, total_length_bytes)
                        .await;
                    if let Err(e) = streaming_handle.push_fragment(fragment_number, payload.clone())
                    {
                        tracing::warn!(
                            peer_addr = %self.remote_conn.remote_addr,
                            stream_id = %stream_id,
                            fragment_number,
                            error = %e,
                            "Failed to push first fragment to streaming handle"
                        );
                    }
                    self.streaming_handles.insert(stream_id, streaming_handle);
                }

                // Legacy path: push to fast_channel for existing recv() behavior
                if let Some(sender) = self.inbound_streams.get(&stream_id) {
                    sender
                        .send_async((fragment_number, payload))
                        .await
                        .map_err(|_| TransportError::ConnectionClosed(self.remote_addr()))?;
                    tracing::trace!(
                        peer_addr = %self.remote_conn.remote_addr,
                        stream_id = %stream_id,
                        fragment_number,
                        "Fragment pushed to existing stream"
                    );
                } else {
                    let (sender, receiver) = fast_channel::bounded(64);
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

        self.remote_conn
            .socket
            .send_to(&pong_packet, self.remote_conn.remote_addr)
            .await
            .map_err(|_| TransportError::ConnectionClosed(self.remote_addr()))?;

        tracing::trace!(
            peer_addr = %self.remote_conn.remote_addr,
            packet_id,
            pong_sequence = sequence,
            "Pong packet sent"
        );

        Ok(())
    }

    #[inline]
    pub(crate) async fn outbound_short_message(&mut self, data: SerializedMessage) -> Result<()> {
        let receipts = self.received_tracker.get_receipts();
        let packet_id = self
            .remote_conn
            .last_packet_id
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        packet_sending(
            self.remote_conn.remote_addr,
            &self.remote_conn.socket,
            packet_id,
            &self.remote_conn.outbound_symmetric_key,
            receipts,
            symmetric_message::ShortMessage(data.into()),
            &self.remote_conn.sent_tracker,
        )
        .await?;
        Ok(())
    }

    async fn outbound_stream(&mut self, data: SerializedMessage) {
        let stream_id = StreamId::next();
        let task = GlobalExecutor::spawn(
            outbound_stream::send_stream(
                stream_id,
                self.remote_conn.last_packet_id.clone(),
                self.remote_conn.socket.clone(),
                self.remote_conn.remote_addr,
                data.into(),
                self.remote_conn.outbound_symmetric_key.clone(),
                self.remote_conn.sent_tracker.clone(),
                self.remote_conn.token_bucket.clone(),
                self.remote_conn.ledbat.clone(),
                self.time_source.clone(),
            )
            .instrument(span!(tracing::Level::DEBUG, "outbound_stream")),
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
    /// This method applies both LEDBAT congestion control (cwnd) and token bucket
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

        // LEDBAT congestion control - wait for cwnd space
        let mut cwnd_wait_iterations = 0;
        loop {
            let flightsize = self.remote_conn.ledbat.flightsize();
            let cwnd = self.remote_conn.ledbat.current_cwnd();

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
            },
            &self.remote_conn.sent_tracker,
        )
        .await?;

        // Track for LEDBAT
        self.remote_conn.ledbat.on_send(packet_size);

        tracing::trace!(
            stream_id = %fragment.stream_id.0,
            fragment_number = fragment.fragment_number,
            packet_id,
            "Fragment sent"
        );

        Ok(())
    }
}

async fn packet_sending<S: super::Socket, T: crate::simulation::TimeSource>(
    remote_addr: SocketAddr,
    socket: &Arc<S>,
    packet_id: u32,
    outbound_sym_key: &Aes128Gcm,
    confirm_receipt: Vec<u32>,
    payload: impl Into<SymmetricMessagePayload>,
    sent_tracker: &parking_lot::Mutex<SentPacketTracker<T>>,
) -> Result<()> {
    let start_time = tokio::time::Instant::now();
    tracing::trace!(
        peer_addr = %remote_addr,
        packet_id,
        "Attempting to send packet"
    );

    match SymmetricMessage::try_serialize_msg_to_packet_data(
        packet_id,
        payload,
        outbound_sym_key,
        confirm_receipt,
    )? {
        either::Either::Left(packet) => {
            let packet_size = packet.data().len();
            tracing::trace!(
                peer_addr = %remote_addr,
                packet_id,
                packet_size,
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
                    sent_tracker
                        .lock()
                        .report_sent_packet(packet_id, packet_data);
                    Ok(())
                }
                Err(e) => {
                    tracing::error!(
                        peer_addr = %remote_addr,
                        packet_id,
                        error = %e,
                        "Failed to send packet"
                    );
                    Err(TransportError::ConnectionClosed(remote_addr))
                }
            }
        }
        either::Either::Right((payload, mut confirm_receipt)) => {
            tracing::trace!(
                peer_addr = %remote_addr,
                packet_id,
                "Sending multi-packet message"
            );
            macro_rules! send {
                ($packets:ident) => {{
                    for packet in $packets {
                        let packet_data = packet.prepared_send();
                        socket
                            .send_to(&packet_data, remote_addr)
                            .await
                            .map_err(|_| TransportError::ConnectionClosed(remote_addr))?;
                        sent_tracker
                            .lock()
                            .report_sent_packet(packet_id, packet_data);
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
}

#[cfg(test)]
mod tests {
    use aes_gcm::KeyInit;
    use futures::TryFutureExt;
    use std::net::Ipv4Addr;

    use super::{
        inbound_stream::{recv_stream, InboundStream},
        outbound_stream::send_stream,
        *,
    };
    use crate::transport::packet_data::MAX_PACKET_SIZE;
    use crate::transport::received_packet_tracker::MAX_PENDING_RECEIPTS;
    use crate::transport::sent_packet_tracker::MAX_CONFIRMATION_DELAY;

    /// Simple test socket that writes to a channel
    struct TestSocket {
        sender: fast_channel::FastSender<(SocketAddr, Arc<[u8]>)>,
    }

    impl TestSocket {
        fn new(sender: fast_channel::FastSender<(SocketAddr, Arc<[u8]>)>) -> Self {
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
                .send_async((target, buf.into()))
                .await
                .map_err(|_| std::io::ErrorKind::ConnectionAborted)?;
            Ok(buf.len())
        }

        fn send_to_blocking(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
            self.sender
                .send((target, buf.into()))
                .map_err(|_| std::io::ErrorKind::ConnectionAborted)?;
            Ok(buf.len())
        }
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
        let (sender, receiver) = fast_channel::bounded(1);
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
        let ledbat = Arc::new(LedbatController::new_with_time_source(
            crate::transport::ledbat::LedbatConfig {
                initial_cwnd: 2928,
                min_cwnd: 2928,
                max_cwnd: 1_000_000_000,
                ..Default::default()
            },
            time_source.clone(),
        ));
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
            ledbat,
            time_source,
        ))
        .map_err(|e| e.into());

        let inbound = async {
            // need to take care of decrypting and deserializing the inbound data before collecting into the message
            let (tx, rx) = fast_channel::bounded(1);
            let stream = InboundStream::new(MSG_LEN as u64);
            let inbound_msg = GlobalExecutor::spawn(recv_stream(stream_id, rx, stream));
            while let Ok((_, network_packet)) = receiver.recv_async().await {
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
                tx.send_async((fragment_number, payload)).await?;
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
        use crate::message::{NetMessage, NetMessageV1, ProximityCacheMessage};
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

        // ProximityCache messages (common during connection setup)
        let cache_msg = NetMessage::V1(NetMessageV1::ProximityCache {
            message: ProximityCacheMessage::CacheAnnounce {
                added: vec![ContractInstanceId::new([1u8; 32])],
                removed: vec![],
            },
        });
        assert_fast_serialize("ProximityCacheMessage", &cache_msg);

        // Large cache state response (worst case for ProximityCache)
        let large_cache = NetMessage::V1(NetMessageV1::ProximityCache {
            message: ProximityCacheMessage::CacheStateResponse {
                contracts: (0..100)
                    .map(|i| ContractInstanceId::new([i as u8; 32]))
                    .collect(),
            },
        });
        assert_fast_serialize("Large CacheStateResponse", &large_cache);
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
        use crate::transport::ledbat::LedbatController;
        use crate::transport::sent_packet_tracker::SentPacketTracker;
        use std::sync::Arc;

        // Create LEDBAT controller with realistic values
        // Initial cwnd = ~38KB (IW26), min cwnd = 2KB (1*MSS)
        let ledbat = Arc::new(LedbatController::new(38_000, 2_000, 10_000_000));

        // Create sent packet tracker
        let mut tracker = SentPacketTracker::new();

        // Simulate sending 5 packets of 1424 bytes each
        let packet_size = 1424;
        for packet_id in 0..5u32 {
            let payload: Box<[u8]> = vec![0u8; packet_size].into_boxed_slice();
            tracker.report_sent_packet(packet_id, payload);
            ledbat.on_send(packet_size);
        }

        // Verify initial flightsize
        assert_eq!(
            ledbat.flightsize(),
            5 * packet_size,
            "Initial flightsize should be 5 * packet_size"
        );

        // Simulate ACK for packets 0, 1, 2 (normal ACKs with RTT samples)
        let (ack_info, _) = tracker.report_received_receipts(&[0, 1, 2]);
        assert_eq!(ack_info.len(), 3, "Should have 3 ACK entries");

        // All 3 should have RTT samples (not retransmitted)
        for (rtt_opt, size) in &ack_info {
            assert!(
                rtt_opt.is_some(),
                "Non-retransmitted packets should have RTT samples"
            );
            assert_eq!(*size, packet_size, "Packet size should match");
        }

        // Apply ACKs to LEDBAT - this should decrement flightsize
        for (rtt_opt, size) in ack_info {
            match rtt_opt {
                Some(rtt) => ledbat.on_ack(rtt, size),
                None => ledbat.on_ack_without_rtt(size),
            }
        }

        assert_eq!(
            ledbat.flightsize(),
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
        let (rtt_opt, size) = &ack_info[0];
        assert!(
            rtt_opt.is_none(),
            "Retransmitted packets should NOT have RTT samples (Karn's algorithm)"
        );
        assert_eq!(
            *size, packet_size,
            "Retransmitted packet ACK MUST still return packet size for flightsize decrement"
        );

        // Apply the ACK - should call on_ack_without_rtt, NOT skip the call entirely
        for (rtt_opt, size) in ack_info {
            match rtt_opt {
                Some(rtt) => ledbat.on_ack(rtt, size),
                None => ledbat.on_ack_without_rtt(size),
            }
        }

        // Verify flightsize was properly decremented
        assert_eq!(
            ledbat.flightsize(),
            packet_size,
            "Flightsize should be decremented even for retransmitted packet ACKs"
        );

        // Also ACK the last packet (4) to verify normal flow still works
        let (ack_info, _) = tracker.report_received_receipts(&[4]);
        for (rtt_opt, size) in ack_info {
            match rtt_opt {
                Some(rtt) => ledbat.on_ack(rtt, size),
                None => ledbat.on_ack_without_rtt(size),
            }
        }

        assert_eq!(
            ledbat.flightsize(),
            0,
            "All packets ACKed, flightsize should be 0"
        );
    }
}
