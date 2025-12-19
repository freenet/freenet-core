use std::net::SocketAddr;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashMap, time::Instant};

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

use super::{
    connection_handler::SerializedMessage,
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
pub(crate) struct RemoteConnection<S = super::UdpSocket> {
    pub(super) outbound_symmetric_key: Aes128Gcm,
    pub(super) remote_addr: SocketAddr,
    pub(super) sent_tracker: Arc<parking_lot::Mutex<SentPacketTracker<InstantTimeSrc>>>,
    pub(super) last_packet_id: Arc<AtomicU32>,
    pub(super) inbound_packet_recv: FastReceiver<PacketData<UnknownEncryption>>,
    pub(super) inbound_symmetric_key: Aes128Gcm,
    pub(super) inbound_symmetric_key_bytes: [u8; 16],
    #[allow(dead_code)]
    pub(super) my_address: Option<SocketAddr>,
    pub(super) transport_secret_key: TransportSecretKey,
    /// LEDBAT congestion controller (RFC 6817) - adapts to network conditions
    pub(super) ledbat: Arc<LedbatController>,
    /// Token bucket rate limiter - smooths packet pacing based on LEDBAT rate
    pub(super) token_bucket: Arc<TokenBucket>,
    /// Socket for direct packet sending (bypasses centralized rate limiter)
    pub(super) socket: Arc<S>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub(crate) struct StreamId(u32);

impl StreamId {
    pub fn next() -> Self {
        static NEXT_ID: AtomicU32 = AtomicU32::new(0);
        Self(NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Release))
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
pub struct PeerConnection<S = super::UdpSocket> {
    remote_conn: RemoteConnection<S>,
    received_tracker: ReceivedPacketTracker<InstantTimeSrc>,
    inbound_streams: HashMap<StreamId, FastSender<(u32, Vec<u8>)>>,
    inbound_stream_futures: FuturesUnordered<JoinHandle<InboundStreamResult>>,
    outbound_stream_futures: FuturesUnordered<JoinHandle<Result>>,
    failure_count: usize,
    first_failure_time: Option<std::time::Instant>,
    last_packet_report_time: Instant,
    keep_alive_handle: Option<JoinHandle<()>>,
    /// Last time we updated the TokenBucket rate from LEDBAT
    /// Used to implement RTT-adaptive rate updates (update approximately once per RTT)
    last_rate_update: Option<std::time::Instant>,
}

impl<S> std::fmt::Debug for PeerConnection<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerConnection")
            .field("remote_conn", &self.remote_conn.remote_addr)
            .finish()
    }
}

impl<S> Drop for PeerConnection<S> {
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

#[allow(private_bounds)]
impl<S: super::Socket> PeerConnection<S> {
    pub(super) fn new(remote_conn: RemoteConnection<S>) -> Self {
        const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(10);

        // Start the keep-alive task before creating Self
        let remote_addr = remote_conn.remote_addr;
        let socket = remote_conn.socket.clone();
        let outbound_key = remote_conn.outbound_symmetric_key.clone();
        let last_packet_id = remote_conn.last_packet_id.clone();

        let keep_alive_handle = tokio::spawn(async move {
            tracing::info!(
                target: "freenet_core::transport::keepalive_lifecycle",
                remote = ?remote_addr,
                "Keep-alive task STARTED for connection"
            );

            let mut interval = tokio::time::interval(KEEP_ALIVE_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            // Skip the first immediate tick
            interval.tick().await;

            let mut tick_count = 0u64;
            let task_start = std::time::Instant::now();

            loop {
                let tick_start = std::time::Instant::now();
                interval.tick().await;
                tick_count += 1;

                let elapsed_since_start = task_start.elapsed();
                let elapsed_since_last_tick = tick_start.elapsed();

                tracing::debug!(
                    target: "freenet_core::transport::keepalive_lifecycle",
                    remote = ?remote_addr,
                    tick_count,
                    elapsed_since_start_secs = elapsed_since_start.as_secs_f64(),
                    tick_interval_ms = elapsed_since_last_tick.as_millis(),
                    "Keep-alive tick - attempting to send NoOp"
                );

                // Create a NoOp packet
                let packet_id = last_packet_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let noop_packet = match SymmetricMessage::serialize_msg_to_packet_data(
                    packet_id,
                    SymmetricMessagePayload::NoOp,
                    &outbound_key,
                    vec![], // No receipts for keep-alive
                ) {
                    Ok(packet) => packet.prepared_send(),
                    Err(e) => {
                        tracing::error!(?e, "Failed to create keep-alive packet");
                        break;
                    }
                };

                // Send the keep-alive packet directly to socket
                tracing::debug!(
                    target: "freenet_core::transport::keepalive_lifecycle",
                    remote = ?remote_addr,
                    packet_id,
                    "Sending keep-alive NoOp packet"
                );

                match socket.send_to(&noop_packet, remote_addr).await {
                    Ok(_) => {
                        tracing::debug!(
                            target: "freenet_core::transport::keepalive_lifecycle",
                            remote = ?remote_addr,
                            packet_id,
                            "Keep-alive NoOp packet sent successfully"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            target: "freenet_core::transport::keepalive_lifecycle",
                            remote = ?remote_addr,
                            error = ?e,
                            elapsed_since_start_secs = task_start.elapsed().as_secs_f64(),
                            total_ticks = tick_count,
                            "Keep-alive task STOPPING - socket error"
                        );
                        break;
                    }
                }
            }

            tracing::warn!(
                target: "freenet_core::transport::keepalive_lifecycle",
                remote = ?remote_addr,
                total_lifetime_secs = task_start.elapsed().as_secs_f64(),
                total_ticks = tick_count,
                "Keep-alive task EXITING"
            );
        });

        tracing::debug!(
            peer_addr = %remote_addr,
            "PeerConnection created with persistent keep-alive task"
        );

        Self {
            remote_conn,
            received_tracker: ReceivedPacketTracker::new(),
            inbound_streams: HashMap::new(),
            inbound_stream_futures: FuturesUnordered::new(),
            outbound_stream_futures: FuturesUnordered::new(),
            failure_count: 0,
            first_failure_time: None,
            last_packet_report_time: Instant::now(),
            keep_alive_handle: Some(keep_alive_handle),
            last_rate_update: None,
        }
    }

    #[instrument(name = "peer_connection", skip_all)]
    pub async fn send<T>(&mut self, data: T) -> Result
    where
        T: Serialize + Send + std::fmt::Debug,
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
        let mut resend_check = Some(tokio::time::sleep(tokio::time::Duration::from_millis(10)));

        const KILL_CONNECTION_AFTER: Duration = Duration::from_secs(120);
        let mut last_received = std::time::Instant::now();

        // Check for timeout periodically
        let mut timeout_check = tokio::time::interval(Duration::from_secs(5));
        timeout_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Background ACK timer - sends pending ACKs proactively every 100ms
        // This prevents delays when there's no outgoing traffic to piggyback ACKs on
        // Use interval_at to delay the first tick - unlike the keep-alive task which can
        // block to skip its first tick, we're inside a select! loop so we delay instead
        let mut ack_check = tokio::time::interval_at(
            tokio::time::Instant::now() + ACK_CHECK_INTERVAL,
            ACK_CHECK_INTERVAL,
        );
        ack_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Rate update timer - updates TokenBucket rate based on LEDBAT cwnd every 100ms
        // This allows the token bucket to adapt to network conditions dynamically
        let mut rate_update_check = tokio::time::interval_at(
            tokio::time::Instant::now() + ACK_CHECK_INTERVAL,
            ACK_CHECK_INTERVAL,
        );
        rate_update_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        const FAILURE_TIME_WINDOW: Duration = Duration::from_secs(30);
        loop {
            // tracing::trace!(remote = ?self.remote_conn.remote_addr, "waiting for inbound messages");
            tokio::select! {
                inbound = self.remote_conn.inbound_packet_recv.recv_async() => {
                    let packet_data = inbound.map_err(|_| TransportError::ConnectionClosed(self.remote_addr()))?;
                    last_received = std::time::Instant::now();

                    // Debug logging for 256-byte packets
                    if packet_data.data().len() == 256 {
                        tracing::debug!(
                            peer_addr = %self.remote_conn.remote_addr,
                            packet_bytes = ?&packet_data.data()[..32], // First 32 bytes
                            packet_len = packet_data.data().len(),
                            "Received 256-byte packet"
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
                        // Check if this is a 256-byte RSA intro packet
                        if packet_data.data().len() == 256 {
                            tracing::debug!(
                                peer_addr = %self.remote_conn.remote_addr,
                                "Attempting to decrypt potential RSA intro packet"
                            );

                            // Try to decrypt as RSA intro packet
                            match self.remote_conn.transport_secret_key.decrypt(packet_data.data()) {
                                Ok(_decrypted_intro) => {
                                    tracing::debug!(
                                        peer_addr = %self.remote_conn.remote_addr,
                                        "Successfully decrypted RSA intro packet, sending ACK"
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
                                Err(rsa_err) => {
                                    tracing::trace!(
                                        peer_addr = %self.remote_conn.remote_addr,
                                        error = ?rsa_err,
                                        "256-byte packet is not a valid RSA intro packet"
                                    );
                                }
                            }
                        }
                        let now = Instant::now();
                        if let Some(first_failure_time) = self.first_failure_time {
                            if now.duration_since(first_failure_time) <= FAILURE_TIME_WINDOW {
                                self.failure_count += 1;
                            } else {
                                // Reset the failure count and time window
                                self.failure_count = 1;
                                self.first_failure_time = Some(now);
                            }
                        } else {
                            // Initialize the failure count and time window
                            self.failure_count = 1;
                            self.first_failure_time = Some(now);
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

                    // Log keep-alive packets specifically
                    if matches!(payload, SymmetricMessagePayload::NoOp) {
                        if confirm_receipt.is_empty() {
                            tracing::debug!(
                                target: "freenet_core::transport::keepalive_received",
                                remote = ?self.remote_conn.remote_addr,
                                packet_id,
                                time_since_last_received_ms = last_received.elapsed().as_millis(),
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

                    {
                        tracing::trace!(
                            peer_addr = %self.remote_conn.remote_addr,
                            packet_id,
                            confirm_receipts_count = confirm_receipt.len(),
                            "Received inbound packet with confirmations"
                        );
                    }

                    let current_time = Instant::now();
                    let should_send_receipts = if current_time > self.last_packet_report_time + MESSAGE_CONFIRMATION_TIMEOUT {
                        tracing::trace!(
                            peer_addr = %self.remote_conn.remote_addr,
                            elapsed_ms = current_time.duration_since(self.last_packet_report_time).as_millis(),
                            timeout_ms = MESSAGE_CONFIRMATION_TIMEOUT.as_millis(),
                            "Timeout reached, should send receipts"
                        );
                        self.last_packet_report_time = current_time;
                        true
                    } else {
                        false
                    };

                    // Process ACKs and update LEDBAT congestion controller
                    let (rtt_samples, _loss_rate) = self.remote_conn
                        .sent_tracker
                        .lock()
                        .report_received_receipts(&confirm_receipt);

                    // Feed RTT samples to LEDBAT for congestion window adjustment
                    for (rtt_sample, packet_size) in rtt_samples {
                        self.remote_conn.ledbat.on_ack(rtt_sample, packet_size);
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
                    res.map_err(|e| TransportError::Other(e.into()))??
                }
                _ = timeout_check.tick() => {
                    let elapsed = last_received.elapsed();
                    if elapsed > KILL_CONNECTION_AFTER {
                        tracing::warn!(
                            target: "freenet_core::transport::keepalive_timeout",
                            remote = ?self.remote_conn.remote_addr,
                            elapsed_seconds = elapsed.as_secs_f64(),
                            timeout_threshold_secs = KILL_CONNECTION_AFTER.as_secs(),
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
                    } else {
                        tracing::trace!(
                            target: "freenet_core::transport::keepalive_health",
                            remote = ?self.remote_conn.remote_addr,
                            elapsed_seconds = elapsed.as_secs_f64(),
                            remaining_seconds = (KILL_CONNECTION_AFTER - elapsed).as_secs_f64(),
                            "Connection health check - still alive"
                        );
                    }
                }
                _ = resend_check.take().unwrap_or(tokio::time::sleep(Duration::from_millis(10))) => {
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
                            ResendAction::WaitUntil(wait_until) => {
                                resend_check = Some(tokio::time::sleep_until(wait_until.into()));
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
                    let now = std::time::Instant::now();

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
                    let should_update = match self.last_rate_update {
                        None => true, // First update
                        Some(last_update) => {
                            let elapsed = now.duration_since(last_update);
                            // Update if at least one RTT has passed, with bounds (50ms min, 500ms max)
                            let min_interval = rtt.max(Duration::from_millis(50)).min(Duration::from_millis(500));
                            elapsed >= min_interval
                        }
                    };

                    if should_update {
                        let new_rate = self.remote_conn.ledbat.current_rate(rtt);
                        let cwnd = self.remote_conn.ledbat.current_cwnd();
                        let queuing_delay = self.remote_conn.ledbat.queuing_delay();

                        // Calculate time since last update for debugging RTT-adaptive timing
                        let since_last_update_ms = self.last_rate_update
                            .map(|last| now.duration_since(last).as_millis())
                            .unwrap_or(0);

                        self.remote_conn.token_bucket.set_rate(new_rate);
                        self.last_rate_update = Some(now);

                        tracing::debug!(
                            peer_addr = %self.remote_conn.remote_addr,
                            // Rate control
                            new_rate_bytes_per_sec = new_rate,
                            new_rate_mbps = (new_rate as f64) / 1_000_000.0,
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

    async fn process_inbound(
        &mut self,
        payload: SymmetricMessagePayload,
    ) -> Result<Option<Vec<u8>>> {
        use SymmetricMessagePayload::*;
        match payload {
            ShortMessage { payload } => Ok(Some(payload)),
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
                        tracing::trace!(
                            peer_addr = %self.remote_conn.remote_addr,
                            stream_id = %stream_id,
                            fragment_number,
                            "Stream finished"
                        );
                        return Ok(Some(msg));
                    }
                    self.inbound_stream_futures
                        .push(tokio::spawn(inbound_stream::recv_stream(
                            stream_id, receiver, stream,
                        )));
                }
                Ok(None)
            }
            NoOp => Ok(None),
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
            symmetric_message::ShortMessage(data),
            &self.remote_conn.sent_tracker,
        )
        .await?;
        Ok(())
    }

    async fn outbound_stream(&mut self, data: SerializedMessage) {
        let stream_id = StreamId::next();
        let task = tokio::spawn(
            outbound_stream::send_stream(
                stream_id,
                self.remote_conn.last_packet_id.clone(),
                self.remote_conn.socket.clone(),
                self.remote_conn.remote_addr,
                data,
                self.remote_conn.outbound_symmetric_key.clone(),
                self.remote_conn.sent_tracker.clone(),
                self.remote_conn.token_bucket.clone(),
                self.remote_conn.ledbat.clone(),
            )
            .instrument(span!(tracing::Level::DEBUG, "outbound_stream")),
        );
        self.outbound_stream_futures.push(task);
    }
}

async fn packet_sending<S: super::Socket>(
    remote_addr: SocketAddr,
    socket: &Arc<S>,
    packet_id: u32,
    outbound_sym_key: &Aes128Gcm,
    confirm_receipt: Vec<u32>,
    payload: impl Into<SymmetricMessagePayload>,
    sent_tracker: &parking_lot::Mutex<SentPacketTracker<InstantTimeSrc>>,
) -> Result<()> {
    let start_time = std::time::Instant::now();
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
        let message: Vec<_> = std::iter::repeat(0)
            .take(MSG_LEN)
            .map(|_| rand::random::<u8>())
            .collect();
        let key = rand::random::<[u8; 16]>();
        let cipher = Aes128Gcm::new(&key.into());
        let sent_tracker = Arc::new(parking_lot::Mutex::new(SentPacketTracker::new()));

        // Initialize LEDBAT and TokenBucket for test
        let ledbat = Arc::new(LedbatController::new(2928, 2928, 1_000_000_000));
        let token_bucket = Arc::new(TokenBucket::new(10_000, 10_000_000));

        let stream_id = StreamId::next();
        // Send a long message using the outbound stream
        let outbound = tokio::task::spawn(send_stream(
            stream_id,
            Arc::new(AtomicU32::new(0)),
            Arc::new(TestSocket::new(sender)),
            remote_addr,
            message.clone(),
            cipher.clone(),
            sent_tracker,
            token_bucket,
            ledbat,
        ))
        .map_err(|e| e.into());

        let inbound = async {
            // need to take care of decrypting and deserializing the inbound data before collecting into the message
            let (tx, rx) = fast_channel::bounded(1);
            let stream = InboundStream::new(MSG_LEN as u64);
            let inbound_msg = tokio::task::spawn(recv_stream(stream_id, rx, stream));
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
}
