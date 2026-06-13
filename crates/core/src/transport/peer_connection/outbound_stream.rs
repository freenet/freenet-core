use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::time::Duration;

use aes_gcm::Aes128Gcm;
use bytes::Bytes;

use tokio::sync::oneshot;

use crate::{
    simulation::TimeSource,
    tracing::TransferDirection,
    transport::{
        TransferStats, TransportError,
        congestion_control::{CongestionControl, CongestionController},
        metrics::{emit_transfer_completed, emit_transfer_failed, emit_transfer_started},
        packet_data,
        sent_packet_tracker::SentPacketTracker,
        symmetric_message::{self},
    },
};

use futures::StreamExt;

use super::StreamId;
use super::streaming::StreamHandle;

/// Maximum time to wait for congestion window space *per fragment* before aborting a stream
/// transfer. Resets for each fragment, so a slow-but-progressing transfer won't time out.
///
/// Must be shorter than STREAM_INACTIVITY_TIMEOUT (5s) so the sender fails first with a
/// diagnostic message rather than the receiver timing out silently.
///
/// With STREAM_INACTIVITY_TIMEOUT at 5s, a 3s cwnd wait gives the sender 2s headroom
/// to fail and report before the receiver's inactivity timeout fires.
const CWND_WAIT_TIMEOUT: Duration = Duration::from_secs(3);

// Compile-time guard: sender must fail before receiver so the failure is diagnostic.
const _: () = assert!(
    CWND_WAIT_TIMEOUT.as_secs() < super::streaming::STREAM_INACTIVITY_TIMEOUT.as_secs(),
    "CWND_WAIT_TIMEOUT must be shorter than STREAM_INACTIVITY_TIMEOUT"
);

/// Stream payload type using zero-copy Bytes for efficient fragmentation.
/// Using Bytes::slice() instead of Vec::split_off() eliminates per-fragment allocations.
pub(crate) type SerializedStream = Bytes;

/// The max payload we can send in a single fragment, this MUST be less than packet_data::MAX_DATA_SIZE
/// since we need to account for the space overhead of SymmetricMessage::StreamFragment metadata.
/// Measured overhead: 41 bytes (see symmetric_message::stream_fragment_overhead())
/// The extra byte vs. the original 40 comes from the Option discriminant of `metadata_bytes`.
const MAX_DATA_SIZE: usize = packet_data::MAX_DATA_SIZE - 41;

/// Drain `stream_id`'s in-flight packets from the tracker and release their bytes from
/// flight size atomically (#4345). Used by every stream-terminating early return so the
/// next stream on the same connection finds a clean flight size instead of inheriting
/// the dead stream's wedge.
///
/// The order matters: `drop_stream` removes entries first (so a late ACK or the per-packet
/// abandon path can't double-decrement), then `release_flightsize` adjusts the counter
/// once with the returned byte total. The tracker lock is released before touching the
/// congestion controller — the controller's `release_flightsize` is its own atomic, so
/// we avoid holding both locks simultaneously.
fn drain_stream_from_tracker<T: TimeSource>(
    sent_packet_tracker: &parking_lot::Mutex<SentPacketTracker<T>>,
    congestion_controller: &CongestionController<T>,
    stream_id: StreamId,
    site: &'static str,
) {
    let drained = sent_packet_tracker.lock().drop_stream(stream_id);
    if drained > 0 {
        congestion_controller.release_flightsize(drained);
        tracing::debug!(
            stream_id = %stream_id.0,
            drained_bytes = drained,
            site,
            "drained stream from tracker + released flight size on abort (#4345)"
        );
    }
}

// TODO: unit test
/// Handles sending a stream that is *not piped*. In the future this will be replaced by
/// piped streams which start forwarding before the stream has been received.
///
/// Returns `TransferStats` on success with BBR congestion control metrics
/// for telemetry purposes.
#[allow(clippy::too_many_arguments)]
pub(super) async fn send_stream<S: super::super::Socket, T: TimeSource>(
    stream_id: StreamId,
    last_packet_id: Arc<AtomicU32>,
    socket: Arc<S>,
    destination_addr: SocketAddr,
    mut stream_to_send: SerializedStream,
    outbound_symmetric_key: Aes128Gcm,
    sent_packet_tracker: Arc<parking_lot::Mutex<SentPacketTracker<T>>>,
    token_bucket: Arc<super::super::token_bucket::TokenBucket<T>>,
    congestion_controller: Arc<CongestionController<T>>,
    time_source: T,
    metadata: Option<Bytes>,
    completion_tx: Option<oneshot::Sender<()>>,
) -> Result<TransferStats, TransportError> {
    let start_time = time_source.now();
    let bytes_to_send = stream_to_send.len() as u64;

    // Emit transfer started telemetry event
    emit_transfer_started(
        stream_id.0 as u64,
        destination_addr,
        bytes_to_send,
        TransferDirection::Send,
    );

    tracing::debug!(
        stream_id = %stream_id.0,
        length_bytes = stream_to_send.len(),
        initial_rate_bytes_per_sec = token_bucket.rate(),
        cwnd = congestion_controller.current_cwnd(),
        "Sending stream"
    );
    let total_length_bytes = stream_to_send.len() as u32;
    // Calculate total_packets accounting for fragment #1's reduced payload when
    // metadata is embedded. Without this adjustment, the loop terminates too early
    // and the final bytes of the stream are never sent.
    let total_packets = if let Some(ref meta) = metadata {
        let meta_overhead = 1 + 8 + meta.len();
        let first_frag_capacity = MAX_DATA_SIZE.saturating_sub(meta_overhead);
        if stream_to_send.len() <= first_frag_capacity {
            1
        } else {
            let remaining = stream_to_send.len() - first_frag_capacity;
            1 + remaining.div_ceil(MAX_DATA_SIZE)
        }
    } else {
        stream_to_send.len().div_ceil(MAX_DATA_SIZE)
    };
    let mut sent_so_far = 0;
    let mut next_fragment_number = 1; // Fragment numbers are 1-indexed
    let mut pending_metadata = metadata;

    loop {
        if sent_so_far == total_packets {
            break;
        }

        let packet_size = stream_to_send.len().min(MAX_DATA_SIZE);

        // BBR congestion control - wait until cwnd has space for this packet.
        // This enforces the congestion window calculated by BBR's state machine.
        //
        // IMPORTANT: This loop requires that recv() is being called on this connection
        // to process incoming ACKs. ACKs reduce flightsize via on_ack(), which opens
        // cwnd space. If recv() is never called, flightsize never decreases and this
        // loop will block forever.
        //
        // Safety: the loop is bounded by CWND_WAIT_TIMEOUT (#3608).
        let cwnd_wait_start = time_source.now();
        let mut cwnd_wait_iterations = 0;
        loop {
            let flightsize = congestion_controller.flightsize();
            let cwnd = congestion_controller.current_cwnd();

            // Check if we have space in the congestion window
            if flightsize + packet_size <= cwnd {
                break; // Space available, proceed to send
            }

            cwnd_wait_iterations += 1;
            if cwnd_wait_iterations == 1 {
                tracing::trace!(
                    stream_id = %stream_id.0,
                    flightsize_kb = flightsize / 1024,
                    cwnd_kb = cwnd / 1024,
                    packet_size,
                    "Waiting for cwnd space (ensure recv() is being called to process ACKs)"
                );
            }

            // Timeout: if cwnd space never opens (ACKs stopped arriving),
            // fail the stream instead of blocking forever (#3608).
            let cwnd_elapsed = time_source.now().saturating_sub(cwnd_wait_start);
            if cwnd_elapsed >= CWND_WAIT_TIMEOUT {
                let elapsed = time_source.now().saturating_sub(start_time);
                tracing::warn!(
                    stream_id = %stream_id.0,
                    destination = %destination_addr,
                    sent_so_far,
                    total_packets,
                    flightsize_kb = flightsize / 1024,
                    cwnd_kb = cwnd / 1024,
                    cwnd_wait_ms = cwnd_elapsed.as_millis(),
                    elapsed_ms = elapsed.as_millis(),
                    "send_stream cwnd wait timed out — ACKs likely stopped arriving"
                );
                emit_transfer_failed(
                    stream_id.0 as u64,
                    destination_addr,
                    sent_so_far as u64,
                    format!(
                        "cwnd wait timeout after {}s (sent {sent_so_far}/{total_packets} packets, \
                         flightsize={flightsize}B, cwnd={cwnd}B)",
                        cwnd_elapsed.as_secs()
                    ),
                    elapsed.as_millis() as u64,
                    TransferDirection::Send,
                );
                // Fail only this stream, not the connection (#4345). A cwnd-wait
                // timeout means ACKs stopped arriving for this transfer; tearing
                // down the whole connection would kill every other operation
                // multiplexed on it. The op layer times out and retries against
                // another candidate; the idle timeout decides connection liveness.
                //
                // NOTE: `completion_tx` is intentionally NOT signaled here — it
                // is dropped by this early return. broadcast_queue awaits it with
                // a timeout and treats the resulting oneshot RecvError as
                // completion (broadcast_queue.rs: `Ok(Err(_))` arm), releasing the
                // permit immediately. Do NOT "fix" this by adding a blocking
                // `tx.send(())` here — a blocking send in this stream task is the
                // backpressure pattern channel-safety.md warns against.
                //
                // Atomic drain (#4345): remove this stream's in-flight packets from
                // the tracker AND release their bytes from flight size, in that order.
                // Doing both atomically here is what prevents the next stream on this
                // connection from inheriting a pinned flight size; relying on the
                // per-packet abandon path alone takes ~6s (12 × 500ms RTO floor),
                // which exceeds CWND_WAIT_TIMEOUT.
                drain_stream_from_tracker(
                    &sent_packet_tracker,
                    &congestion_controller,
                    stream_id,
                    "send_stream cwnd-wait abort",
                );
                return Err(TransportError::OutboundStreamFailed(destination_addr));
            }

            // Exponential backoff to balance responsiveness and CPU usage
            // First 10 attempts: immediate yield (context switch only)
            // Next 90 attempts: 100μs sleep
            // Beyond 100: 1ms sleep (graceful degradation)
            if cwnd_wait_iterations <= 10 {
                tokio::task::yield_now().await;
            } else if cwnd_wait_iterations <= 100 {
                time_source.sleep(Duration::from_micros(100)).await;
            } else {
                time_source.sleep(Duration::from_millis(1)).await;
            }
        }

        if cwnd_wait_iterations > 0 {
            tracing::trace!(
                stream_id = %stream_id.0,
                wait_iterations = cwnd_wait_iterations,
                "Acquired cwnd space"
            );
        }

        // Token bucket rate limiting - reserve tokens and wait if needed
        let wait_time = token_bucket.reserve(packet_size);
        if !wait_time.is_zero() {
            tracing::trace!(
                stream_id = %stream_id.0,
                wait_time_ms = wait_time.as_millis(),
                packet_size,
                "Rate limiting stream transmission"
            );
            time_source.sleep(wait_time).await;
        }

        // Embed metadata in fragment #1 for reliability (fix #2757).
        // If the separate metadata message is lost over UDP, the receiver
        // can reconstruct it from the embedded bytes.
        let metadata_bytes = if next_fragment_number == 1 {
            pending_metadata.take()
        } else {
            None
        };

        // Calculate available payload size for this fragment.
        // For fragment #1 with embedded metadata, reduce payload to make room
        // for the metadata bytes within the MAX_DATA_SIZE constraint.
        let available_payload = if let Some(ref meta) = metadata_bytes {
            // Reserve space for metadata: bincode serializes Option<Bytes> as:
            // - 1 byte for Some discriminant
            // - 8 bytes for length prefix
            // - N bytes for the actual data
            let meta_overhead = 1 + 8 + meta.len();
            MAX_DATA_SIZE.saturating_sub(meta_overhead)
        } else {
            MAX_DATA_SIZE
        };

        // Zero-copy fragmentation using Bytes::slice()
        // This avoids allocating a new Vec for each fragment
        let fragment = {
            if stream_to_send.len() > available_payload {
                let fragment = stream_to_send.slice(..available_payload);
                stream_to_send = stream_to_send.slice(available_payload..);
                fragment
            } else {
                std::mem::take(&mut stream_to_send)
            }
        };
        let packet_id = last_packet_id.fetch_add(1, std::sync::atomic::Ordering::Release);

        // Get token before sending (captures send-time state for BBR)
        // This also updates the congestion controller's flightsize
        let token = congestion_controller.on_send_with_token(packet_size);

        if let Err(e) = super::packet_sending(
            destination_addr,
            &socket,
            packet_id,
            &outbound_symmetric_key,
            vec![],
            symmetric_message::StreamFragment {
                stream_id,
                total_length_bytes: total_length_bytes as u64,
                fragment_number: next_fragment_number,
                payload: fragment,
                metadata_bytes,
            },
            sent_packet_tracker.as_ref(),
            token,
        )
        .await
        {
            // Emit transfer failed telemetry event
            let bytes_sent = (sent_so_far * MAX_DATA_SIZE) as u64;
            let elapsed = time_source.now().saturating_sub(start_time);
            emit_transfer_failed(
                stream_id.0 as u64,
                destination_addr,
                bytes_sent.min(bytes_to_send),
                e.to_string(),
                elapsed.as_millis() as u64,
                TransferDirection::Send,
            );
            // Signal completion (error path) so broadcast queue can release permit
            if let Some(tx) = completion_tx {
                // Receiver may be dropped if queue timed out; ignore the error.
                let _ignored = tx.send(());
            }
            // Drain earlier fragments of THIS stream from tracker (#4345). The failing
            // packet itself was never registered (`report_sent_packet` runs only on
            // socket success), but successfully-sent prior fragments are still in flight.
            // A same-connection retry would inherit their wedge without this drain.
            drain_stream_from_tracker(
                &sent_packet_tracker,
                &congestion_controller,
                stream_id,
                "send_stream send-failed abort",
            );
            return Err(e);
        }

        next_fragment_number += 1;
        sent_so_far += 1;
    }

    // Gather congestion control stats for telemetry
    // Use algorithm-specific stats when available for detailed metrics
    let generic_stats = congestion_controller.stats();
    let ledbat_stats = congestion_controller.ledbat_stats();
    let bbr_stats = congestion_controller.bbr_stats();
    let elapsed = time_source.now().saturating_sub(start_time);

    tracing::debug!(
        stream_id = %stream_id.0,
        total_packets = %sent_so_far,
        bytes = bytes_to_send,
        elapsed_ms = elapsed.as_millis(),
        peak_cwnd_kb = generic_stats.peak_cwnd / 1024,
        final_cwnd_kb = generic_stats.cwnd / 1024,
        slowdowns = ledbat_stats.as_ref().map(|s| s.periodic_slowdowns).unwrap_or(0),
        bbr_state = ?bbr_stats.as_ref().map(|s| s.state),
        "Stream sent"
    );

    // Emit transfer completed telemetry event
    emit_transfer_completed(
        stream_id.0 as u64,
        destination_addr,
        bytes_to_send,
        elapsed.as_millis() as u64,
        if elapsed.as_secs() > 0 {
            bytes_to_send / elapsed.as_secs()
        } else {
            bytes_to_send * 1000 / elapsed.as_millis().max(1) as u64
        },
        Some(generic_stats.peak_cwnd as u32),
        Some(generic_stats.cwnd as u32),
        ledbat_stats.as_ref().map(|s| s.periodic_slowdowns as u32),
        Some(generic_stats.base_delay.as_millis() as u32),
        Some(generic_stats.ssthresh as u32),
        ledbat_stats.as_ref().map(|s| s.min_ssthresh_floor as u32),
        Some(generic_stats.total_timeouts as u32),
        TransferDirection::Send,
    );

    // Signal completion (success path) so broadcast queue can release permit
    if let Some(tx) = completion_tx {
        // Receiver may be dropped if queue timed out; ignore the error.
        let _ignored = tx.send(());
    }

    Ok(TransferStats {
        stream_id: stream_id.0 as u64,
        remote_addr: destination_addr,
        bytes_transferred: bytes_to_send,
        elapsed,
        peak_cwnd_bytes: generic_stats.peak_cwnd as u32,
        final_cwnd_bytes: generic_stats.cwnd as u32,
        slowdowns_triggered: ledbat_stats
            .as_ref()
            .map(|s| s.periodic_slowdowns as u32)
            .unwrap_or(0),
        base_delay: generic_stats.base_delay,
        final_ssthresh_bytes: generic_stats.ssthresh as u32,
        min_ssthresh_floor_bytes: ledbat_stats
            .as_ref()
            .map(|s| s.min_ssthresh_floor as u32)
            .unwrap_or(0),
        total_timeouts: generic_stats.total_timeouts as u32,
        final_flightsize: generic_stats.flightsize as u32,
        configured_rate: congestion_controller.configured_rate() as u32,
    })
}

/// Pipes an inbound stream to an outbound connection, forwarding fragments as they arrive.
///
/// Unlike `send_stream` which takes complete data and fragments it, this reads
/// from a `StreamHandle` and forwards each fragment incrementally. This avoids
/// full reassembly at intermediate nodes, reducing latency.
///
/// The outbound stream uses a new `outbound_stream_id` so the receiver sees
/// a fresh stream. Fragments are sent with the same BBR congestion control
/// and token bucket rate limiting as `send_stream`.
#[allow(clippy::too_many_arguments)]
pub(super) async fn pipe_stream<S: super::super::Socket, T: TimeSource>(
    inbound_handle: StreamHandle,
    outbound_stream_id: StreamId,
    last_packet_id: Arc<AtomicU32>,
    socket: Arc<S>,
    destination_addr: SocketAddr,
    outbound_symmetric_key: Aes128Gcm,
    sent_packet_tracker: Arc<parking_lot::Mutex<SentPacketTracker<T>>>,
    token_bucket: Arc<super::super::token_bucket::TokenBucket<T>>,
    congestion_controller: Arc<CongestionController<T>>,
    time_source: T,
    metadata: Option<Bytes>,
) -> Result<TransferStats, TransportError> {
    let start_time = time_source.now();
    let total_bytes = inbound_handle.total_bytes();

    emit_transfer_started(
        outbound_stream_id.0 as u64,
        destination_addr,
        total_bytes,
        TransferDirection::Send,
    );

    tracing::debug!(
        stream_id = %outbound_stream_id.0,
        total_bytes,
        "Piping stream to next hop"
    );

    let mut stream = inbound_handle.stream();
    let mut sent_so_far = 0u64;
    let mut fragment_number = 1u32;
    let mut pending_metadata = metadata;

    // Inactivity timeout for piped streams. If no fragment arrives from the
    // inbound buffer within this duration, the pipe fails rather than hanging
    // indefinitely. This matches STREAM_INACTIVITY_TIMEOUT used by assemble().
    use super::streaming::STREAM_INACTIVITY_TIMEOUT;
    let inactivity_timeout = STREAM_INACTIVITY_TIMEOUT;

    loop {
        // Use tokio::select! with time_source.sleep() for DST compatibility.
        // tokio::time::timeout uses real timers which don't advance in
        // VirtualTime simulation tests.
        let next_fragment = tokio::select! {
            result = stream.next() => {
                match result {
                    Some(r) => r,
                    None => break, // Stream complete
                }
            }
            _ = time_source.sleep(inactivity_timeout) => {
                // No fragment arrived within the inactivity timeout
                let elapsed = time_source.now().saturating_sub(start_time);
                tracing::warn!(
                    stream_id = %outbound_stream_id.0,
                    destination = %destination_addr,
                    sent_so_far,
                    total_bytes,
                    fragment_number,
                    elapsed_ms = elapsed.as_millis(),
                    "pipe_stream stalled: no fragment received within {}s",
                    inactivity_timeout.as_secs()
                );
                emit_transfer_failed(
                    outbound_stream_id.0 as u64,
                    destination_addr,
                    sent_so_far,
                    format!(
                        "pipe stalled: no fragment for {}s (sent {sent_so_far}/{total_bytes} bytes)",
                        inactivity_timeout.as_secs()
                    ),
                    elapsed.as_millis() as u64,
                    TransferDirection::Send,
                );
                // Stall is in the UPSTREAM inbound feed, not the downstream
                // connection — fail only this stream so other ops multiplexed
                // on the downstream connection survive (#4345). The op layer
                // times out and retries; the idle timeout decides connection
                // liveness.
                drain_stream_from_tracker(
                    &sent_packet_tracker,
                    &congestion_controller,
                    outbound_stream_id,
                    "pipe_stream inactivity abort",
                );
                return Err(TransportError::OutboundStreamFailed(destination_addr));
            }
        };
        let payload = match next_fragment {
            Ok(data) => data,
            Err(e) => {
                let elapsed = time_source.now().saturating_sub(start_time);
                emit_transfer_failed(
                    outbound_stream_id.0 as u64,
                    destination_addr,
                    sent_so_far,
                    format!("inbound stream error: {e}"),
                    elapsed.as_millis() as u64,
                    TransferDirection::Send,
                );
                // Error is on the UPSTREAM inbound stream, not the downstream
                // connection — fail only this stream (#4345), same rationale as
                // the inactivity-stall arm above.
                drain_stream_from_tracker(
                    &sent_packet_tracker,
                    &congestion_controller,
                    outbound_stream_id,
                    "pipe_stream inbound-error abort",
                );
                return Err(TransportError::OutboundStreamFailed(destination_addr));
            }
        };

        let packet_size = payload.len();

        // BBR congestion control - wait until cwnd has space for this packet.
        //
        // IMPORTANT: This loop requires that recv() is being called on this connection
        // to process incoming ACKs. ACKs reduce flightsize via on_ack(), which opens
        // cwnd space. If recv() is never called, flightsize never decreases and this
        // loop will block forever — which is exactly what happened in #3608 when the
        // relay GET streaming path (#3586) lost ACKs mid-transfer.
        //
        // Safety: the loop is bounded by CWND_WAIT_TIMEOUT. If cwnd space doesn't
        // open within the timeout, the pipe fails with OutboundStreamFailed
        // (stream-scoped, connection survives — #4345) rather than hanging
        // indefinitely.
        let cwnd_wait_start = time_source.now();
        let mut cwnd_wait_iterations = 0;
        loop {
            let flightsize = congestion_controller.flightsize();
            let cwnd = congestion_controller.current_cwnd();

            if flightsize + packet_size <= cwnd {
                break;
            }

            cwnd_wait_iterations += 1;
            if cwnd_wait_iterations == 1 {
                tracing::trace!(
                    stream_id = %outbound_stream_id.0,
                    fragment_number,
                    flightsize_kb = flightsize / 1024,
                    cwnd_kb = cwnd / 1024,
                    "Waiting for cwnd space in pipe_stream"
                );
            }

            // Timeout: if cwnd space never opens (ACKs stopped arriving),
            // fail the pipe instead of blocking forever (#3608).
            let cwnd_elapsed = time_source.now().saturating_sub(cwnd_wait_start);
            if cwnd_elapsed >= CWND_WAIT_TIMEOUT {
                let elapsed = time_source.now().saturating_sub(start_time);
                tracing::warn!(
                    stream_id = %outbound_stream_id.0,
                    destination = %destination_addr,
                    fragment_number,
                    sent_so_far,
                    total_bytes,
                    flightsize_kb = flightsize / 1024,
                    cwnd_kb = cwnd / 1024,
                    cwnd_wait_ms = cwnd_elapsed.as_millis(),
                    elapsed_ms = elapsed.as_millis(),
                    "pipe_stream cwnd wait timed out — ACKs likely stopped arriving"
                );
                emit_transfer_failed(
                    outbound_stream_id.0 as u64,
                    destination_addr,
                    sent_so_far,
                    format!(
                        "cwnd wait timeout after {}s (sent {sent_so_far}/{total_bytes} bytes, \
                         flightsize={flightsize}B, cwnd={cwnd}B)",
                        cwnd_elapsed.as_secs()
                    ),
                    elapsed.as_millis() as u64,
                    TransferDirection::Send,
                );
                // Fail only this stream, not the connection (#4345). See the
                // matching site in send_stream for the rationale.
                drain_stream_from_tracker(
                    &sent_packet_tracker,
                    &congestion_controller,
                    outbound_stream_id,
                    "pipe_stream cwnd-wait abort",
                );
                return Err(TransportError::OutboundStreamFailed(destination_addr));
            }

            if cwnd_wait_iterations <= 10 {
                tokio::task::yield_now().await;
            } else if cwnd_wait_iterations <= 100 {
                time_source.sleep(Duration::from_micros(100)).await;
            } else {
                time_source.sleep(Duration::from_millis(1)).await;
            }
        }

        // Token bucket rate limiting
        let wait_time = token_bucket.reserve(packet_size);
        if !wait_time.is_zero() {
            time_source.sleep(wait_time).await;
        }

        // Embed metadata in fragment #1 for reliability (fix #2757).
        // For piped streams, only embed if it fits within MAX_DATA_SIZE without
        // exceeding the limit. If the fragment #1 payload is too large, skip
        // embedding and rely on the separate metadata message instead.
        let metadata_bytes = if fragment_number == 1 {
            if let Some(meta) = pending_metadata.take() {
                // Note: The 41-byte overhead already includes Option discriminant (1 byte)
                // and length prefix (8 bytes). Only add the metadata data length.
                let required_size = payload.len() + 41 + meta.len();
                if required_size <= packet_data::MAX_DATA_SIZE {
                    Some(meta)
                } else {
                    tracing::debug!(
                        stream_id = %outbound_stream_id.0,
                        payload_len = payload.len(),
                        meta_len = meta.len(),
                        required_size,
                        max_size = packet_data::MAX_DATA_SIZE,
                        "Skipping metadata embedding in piped fragment #1 - would exceed MAX_DATA_SIZE"
                    );
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        let packet_id = last_packet_id.fetch_add(1, std::sync::atomic::Ordering::Release);
        let token = congestion_controller.on_send_with_token(packet_size);

        if let Err(e) = super::packet_sending(
            destination_addr,
            &socket,
            packet_id,
            &outbound_symmetric_key,
            vec![],
            symmetric_message::StreamFragment {
                stream_id: outbound_stream_id,
                total_length_bytes: total_bytes,
                fragment_number,
                payload,
                metadata_bytes,
            },
            sent_packet_tracker.as_ref(),
            token,
        )
        .await
        {
            let elapsed = time_source.now().saturating_sub(start_time);
            emit_transfer_failed(
                outbound_stream_id.0 as u64,
                destination_addr,
                sent_so_far,
                e.to_string(),
                elapsed.as_millis() as u64,
                TransferDirection::Send,
            );
            // Drain earlier fragments of THIS stream (#4345); same rationale as the
            // matching site in send_stream.
            drain_stream_from_tracker(
                &sent_packet_tracker,
                &congestion_controller,
                outbound_stream_id,
                "pipe_stream send-failed abort",
            );
            return Err(e);
        }

        sent_so_far += packet_size as u64;
        fragment_number += 1;
    }

    let generic_stats = congestion_controller.stats();
    let ledbat_stats = congestion_controller.ledbat_stats();
    let elapsed = time_source.now().saturating_sub(start_time);

    tracing::debug!(
        stream_id = %outbound_stream_id.0,
        fragments = fragment_number - 1,
        bytes = sent_so_far,
        elapsed_ms = elapsed.as_millis(),
        "Pipe stream complete"
    );

    emit_transfer_completed(
        outbound_stream_id.0 as u64,
        destination_addr,
        sent_so_far,
        elapsed.as_millis() as u64,
        if elapsed.as_secs() > 0 {
            sent_so_far / elapsed.as_secs()
        } else {
            sent_so_far * 1000 / elapsed.as_millis().max(1) as u64
        },
        Some(generic_stats.peak_cwnd as u32),
        Some(generic_stats.cwnd as u32),
        ledbat_stats.as_ref().map(|s| s.periodic_slowdowns as u32),
        Some(generic_stats.base_delay.as_millis() as u32),
        Some(generic_stats.ssthresh as u32),
        ledbat_stats.as_ref().map(|s| s.min_ssthresh_floor as u32),
        Some(generic_stats.total_timeouts as u32),
        TransferDirection::Send,
    );

    Ok(TransferStats {
        stream_id: outbound_stream_id.0 as u64,
        remote_addr: destination_addr,
        bytes_transferred: sent_so_far,
        elapsed,
        peak_cwnd_bytes: generic_stats.peak_cwnd as u32,
        final_cwnd_bytes: generic_stats.cwnd as u32,
        slowdowns_triggered: ledbat_stats
            .as_ref()
            .map(|s| s.periodic_slowdowns as u32)
            .unwrap_or(0),
        base_delay: generic_stats.base_delay,
        final_ssthresh_bytes: generic_stats.ssthresh as u32,
        min_ssthresh_floor_bytes: ledbat_stats
            .as_ref()
            .map(|s| s.min_ssthresh_floor as u32)
            .unwrap_or(0),
        total_timeouts: generic_stats.total_timeouts as u32,
        final_flightsize: generic_stats.flightsize as u32,
        configured_rate: congestion_controller.configured_rate() as u32,
    })
}

#[cfg(test)]
mod tests {
    use aes_gcm::KeyInit;
    use std::net::Ipv4Addr;
    use tests::packet_data::MAX_PACKET_SIZE;
    use tracing::debug;

    use super::{
        symmetric_message::{SymmetricMessage, SymmetricMessagePayload},
        *,
    };
    use crate::config::GlobalExecutor;
    use crate::simulation::{RealTime, VirtualTime};
    use crate::transport::congestion_control::CongestionControlConfig;
    use crate::transport::ledbat::LedbatConfig;
    use crate::transport::packet_data::PacketData;
    use crate::transport::token_bucket::TokenBucket;
    use tokio::sync::mpsc;

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

    #[tokio::test]
    async fn test_send_stream_success() -> Result<(), Box<dyn std::error::Error>> {
        let (outbound_sender, mut outbound_receiver) = mpsc::channel(1);
        let remote_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 8080);
        let mut message = vec![0u8; 100_000];
        crate::config::GlobalRng::fill_bytes(&mut message);
        let cipher = {
            let mut key = [0u8; 16];
            crate::config::GlobalRng::fill_bytes(&mut key);
            Aes128Gcm::new(&key.into())
        };

        // Use VirtualTime for deterministic testing
        // Token bucket has enough capacity that no sleeping is needed
        let time_source = VirtualTime::new();
        let sent_tracker = Arc::new(parking_lot::Mutex::new(
            SentPacketTracker::new_with_time_source(time_source.clone()),
        ));

        // Initialize congestion controller and TokenBucket for test with VirtualTime
        // Use large cwnd since unit tests don't simulate ACKs to reduce flightsize
        let congestion_controller = CongestionControlConfig::from_ledbat_config(LedbatConfig {
            initial_cwnd: 1_000_000,
            min_cwnd: 1_000_000,
            max_cwnd: 1_000_000_000,
            ..Default::default()
        })
        .build_arc_with_time_source(time_source.clone());
        let token_bucket = Arc::new(TokenBucket::new_with_time_source(
            1_000_000,
            10_000_000,
            time_source.clone(),
        ));

        let background_task = GlobalExecutor::spawn(send_stream(
            StreamId::next(),
            Arc::new(AtomicU32::new(0)),
            Arc::new(TestSocket::new(outbound_sender)),
            remote_addr,
            Bytes::from(message.clone()),
            cipher.clone(),
            sent_tracker,
            token_bucket,
            congestion_controller,
            time_source,
            None,
            None,
        ));

        let mut inbound_bytes = Vec::with_capacity(message.len());
        while let Some((_, packet)) = outbound_receiver.recv().await {
            let decrypted_packet = PacketData::<_, MAX_PACKET_SIZE>::from_buf(packet.as_ref())
                .try_decrypt_sym(&cipher)
                .map_err(|e| e.to_string())?;
            let deserialized = SymmetricMessage::deser(decrypted_packet.data())?;
            let SymmetricMessagePayload::StreamFragment { payload, .. } = deserialized.payload
            else {
                panic!("Expected a StreamFragment, got {:?}", deserialized.payload);
            };
            inbound_bytes.extend_from_slice(payload.as_ref());
        }

        let result = background_task.await?;
        assert!(result.is_ok());
        assert_eq!(&message[..10], &inbound_bytes[..10]);
        assert_eq!(inbound_bytes.len(), 100_000);
        assert_eq!(&message[99_990..], &inbound_bytes[99_990..]);
        Ok(())
    }

    #[tokio::test]
    async fn test_send_stream_with_bandwidth_limit() -> Result<(), Box<dyn std::error::Error>> {
        let (outbound_sender, mut outbound_receiver) = mpsc::channel(100);
        let destination_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 1234));
        let key = Aes128Gcm::new_from_slice(&[0u8; 16])?;
        let last_packet_id = Arc::new(AtomicU32::new(0));
        let stream_id = StreamId::next();

        // Create a large message (10KB)
        let message = vec![0u8; 10_000];

        // Set bandwidth limit to 100KB/s (100,000 bytes/second)
        let bandwidth_limit = 100_000;

        // Use real time for integration testing with bandwidth limiting
        let time_source = RealTime::new();
        let sent_tracker = Arc::new(parking_lot::Mutex::new(
            SentPacketTracker::new_with_time_source(time_source.clone()),
        ));

        // Initialize congestion controller and TokenBucket for test
        // Use large cwnd since unit tests don't simulate ACKs to reduce flightsize
        // Use small burst capacity (1KB) to ensure rate limiting is observable
        let congestion_controller = CongestionControlConfig::from_ledbat_config(LedbatConfig {
            initial_cwnd: 1_000_000,
            min_cwnd: 1_000_000,
            max_cwnd: 1_000_000_000,
            ..Default::default()
        })
        .build_arc();
        let token_bucket = Arc::new(TokenBucket::new_with_time_source(
            1_000, // 1KB burst - ensures rate limiting kicks in
            bandwidth_limit,
            time_source.clone(),
        ));

        // Expected: 100KB/s with token bucket rate limiting
        // Should throttle appropriately

        let start_time = tokio::time::Instant::now();

        // Clone sender for receiver task termination
        let sender_clone: mpsc::Sender<(SocketAddr, Arc<[u8]>)> = outbound_sender.clone();
        let key_clone = key.clone();

        // Spawn receiver task to collect packets
        let receiver_task = GlobalExecutor::spawn(async move {
            let mut packet_count = 0;
            let mut total_bytes = 0;
            while let Some((addr, packet)) = outbound_receiver.recv().await {
                assert_eq!(addr, destination_addr);
                packet_count += 1;
                total_bytes += packet.len();

                // Decrypt and verify it's a stream fragment
                let packet_data = PacketData::<_, MAX_PACKET_SIZE>::from_buf(&packet);
                let decrypted = packet_data.try_decrypt_sym(&key_clone).unwrap();
                let msg = SymmetricMessage::deser(decrypted.data()).unwrap();
                match msg.payload {
                    SymmetricMessagePayload::StreamFragment { .. } => {
                        // Expected
                    }
                    SymmetricMessagePayload::AckConnection { .. }
                    | SymmetricMessagePayload::ShortMessage { .. }
                    | SymmetricMessagePayload::NoOp
                    | SymmetricMessagePayload::Ping { .. }
                    | SymmetricMessagePayload::Pong { .. } => panic!("Expected stream fragment"),
                }
            }
            (packet_count, total_bytes)
        });

        // Spawn the send_stream task
        let send_task = GlobalExecutor::spawn(send_stream(
            stream_id,
            last_packet_id.clone(),
            Arc::new(TestSocket::new(outbound_sender)),
            destination_addr,
            Bytes::from(message.clone()),
            key.clone(),
            sent_tracker.clone(),
            token_bucket,
            congestion_controller,
            time_source,
            None,
            None,
        ));

        // Wait for send task to complete
        send_task.await??;
        let elapsed = start_time.elapsed();

        // Drop the cloned sender to close the channel
        drop(sender_clone);

        // Get receiver results
        let (packet_count, _total_bytes) = receiver_task.await?;

        // Verify we sent the expected number of packets
        let expected_packets = message.len().div_ceil(MAX_DATA_SIZE);
        assert_eq!(packet_count, expected_packets);

        // Verify that rate limiting occurred
        // For 10KB at 100KB/s with 1KB burst:
        // - First 1KB sent immediately (burst)
        // - Remaining 9KB at 100KB/s = ~90ms
        // - Actual timing: ~50-60ms due to concurrent token refill
        debug!(
            "Transfer took: {elapsed:?}, packets sent: {packet_count}, expected: {expected_packets}"
        );
        debug!("Bytes per packet: ~{MAX_DATA_SIZE}");
        assert!(
            elapsed.as_millis() >= 50,
            "Transfer completed too quickly: {elapsed:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_send_stream_without_bandwidth_limit() -> Result<(), Box<dyn std::error::Error>> {
        let (outbound_sender, mut outbound_receiver) = mpsc::channel(100);
        let destination_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 1234));
        let key = Aes128Gcm::new_from_slice(&[0u8; 16])?;
        let last_packet_id = Arc::new(AtomicU32::new(0));
        let stream_id = StreamId::next();

        // Create a large message (10KB)
        let message = vec![0u8; 10_000];

        // Use real time for integration testing without bandwidth limiting
        let time_source = RealTime::new();
        let sent_tracker = Arc::new(parking_lot::Mutex::new(
            SentPacketTracker::new_with_time_source(time_source.clone()),
        ));

        // Initialize congestion controller and TokenBucket with very high rate (effectively unlimited)
        // Use large cwnd since unit tests don't simulate ACKs to reduce flightsize
        let congestion_controller = CongestionControlConfig::from_ledbat_config(LedbatConfig {
            initial_cwnd: 1_000_000,
            min_cwnd: 1_000_000,
            max_cwnd: 1_000_000_000,
            ..Default::default()
        })
        .build_arc();
        let token_bucket = Arc::new(TokenBucket::new_with_time_source(
            100_000,       // 100 KB burst capacity
            1_000_000_000, // 1 GB/s rate (effectively unlimited)
            time_source.clone(),
        ));

        let start_time = tokio::time::Instant::now();

        // Clone sender for receiver task termination
        let sender_clone: mpsc::Sender<(SocketAddr, Arc<[u8]>)> = outbound_sender.clone();

        // Spawn receiver task to collect packets
        let receiver_task = GlobalExecutor::spawn(async move {
            let mut packet_count = 0;
            while let Some((addr, _packet)) = outbound_receiver.recv().await {
                assert_eq!(addr, destination_addr);
                packet_count += 1;
            }
            packet_count
        });

        // Spawn the send_stream task
        let send_task = GlobalExecutor::spawn(send_stream(
            stream_id,
            last_packet_id.clone(),
            Arc::new(TestSocket::new(outbound_sender)),
            destination_addr,
            Bytes::from(message.clone()),
            key.clone(),
            sent_tracker.clone(),
            token_bucket,
            congestion_controller,
            time_source,
            None,
            None,
        ));

        // Wait for send task to complete
        send_task.await??;
        let elapsed = start_time.elapsed();

        // Drop the cloned sender to close the channel
        drop(sender_clone);

        // Get receiver results
        let packet_count = receiver_task.await?;

        // Verify we sent the expected number of packets
        let expected_packets = message.len().div_ceil(MAX_DATA_SIZE);
        assert_eq!(packet_count, expected_packets);

        // Without rate limiting, should complete very quickly (< 50ms)
        assert!(
            elapsed.as_millis() < 50,
            "Transfer took too long without rate limit: {elapsed:?}"
        );

        Ok(())
    }

    /// Test that send_stream aborts with OutboundStreamFailed when the cwnd
    /// wait exceeds CWND_WAIT_TIMEOUT. Simulates a dead outbound connection
    /// where cwnd is too small for any packet (#3608). Per #4345 this is a
    /// stream-scoped failure that must not tear down the connection.
    #[tokio::test(start_paused = true)]
    async fn test_send_stream_cwnd_wait_timeout() -> Result<(), Box<dyn std::error::Error>> {
        let (outbound_sender, _outbound_receiver) = mpsc::channel(100);
        let remote_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 8080);
        let message = vec![0u8; 10_000];
        let cipher = {
            let mut key = [0u8; 16];
            crate::config::GlobalRng::fill_bytes(&mut key);
            Aes128Gcm::new(&key.into())
        };

        // Use RealTime with tokio's paused time (start_paused = true) for
        // deterministic auto-advancing sleep. VirtualTime requires manual
        // advance() calls which don't interleave well with the cwnd wait loop.
        let time_source = RealTime::new();

        // Create a congestion controller with a tiny cwnd (1 byte).
        // Any real packet exceeds this, so the cwnd wait loop never breaks
        // and must hit the CWND_WAIT_TIMEOUT.
        let congestion_controller = CongestionControlConfig::from_ledbat_config(LedbatConfig {
            initial_cwnd: 1,
            min_cwnd: 1,
            max_cwnd: 1,
            ..Default::default()
        })
        .build_arc();

        let sent_tracker = Arc::new(parking_lot::Mutex::new(SentPacketTracker::new()));
        let token_bucket = Arc::new(TokenBucket::new(1_000_000, 100_000_000));

        let send_task = GlobalExecutor::spawn(send_stream(
            StreamId::next(),
            Arc::new(AtomicU32::new(0)),
            Arc::new(TestSocket::new(outbound_sender)),
            remote_addr,
            Bytes::from(message),
            cipher,
            sent_tracker,
            token_bucket,
            congestion_controller,
            time_source,
            None,
            None,
        ));

        // With start_paused=true, tokio auto-advances time through sleep calls.
        // The cwnd wait loop sleeps in 1ms increments, and the timeout is
        // CWND_WAIT_TIMEOUT (3s), so tokio auto-advances through the iterations
        // instantly.
        let result = send_task.await.expect("join error");
        assert!(
            matches!(result, Err(TransportError::OutboundStreamFailed(_))),
            "Expected OutboundStreamFailed after cwnd wait timeout, got: {result:?}",
        );

        Ok(())
    }

    /// Regression test for #4345: a cwnd-wait timeout must fail only the stream,
    /// NOT the connection.
    ///
    /// When an outbound stream's congestion-window wait times out, `send_stream`
    /// returns an error. Before this fix it returned
    /// `TransportError::ConnectionClosed`, which the connection recv loop in
    /// `peer_connection.rs` treats as fatal — tearing the whole connection down
    /// and killing every other operation multiplexed on it, forcing a
    /// re-handshake. The fix returns `TransportError::OutboundStreamFailed`,
    /// which `is_transient_send_failure()` classifies as non-fatal, so the recv
    /// loop logs and lets the op layer retry while the connection survives (the
    /// idle timeout remains the sole authority on connection liveness).
    ///
    /// This test drives the real `send_stream` to the cwnd-wait timeout with a
    /// 1-byte cwnd (no packet ever fits, so the wait loop never breaks) and
    /// asserts:
    ///   1. the error IS `OutboundStreamFailed` (not `ConnectionClosed`), and
    ///   2. `is_transient_send_failure()` is `true`, i.e. the recv loop takes
    ///      the connection-survival arm.
    ///
    /// Asserting on `is_transient_send_failure()` is the load-bearing check:
    /// the recv-loop classification (`peer_connection.rs`) is exactly
    /// `Err(e) if e.is_transient_send_failure() => /* connection survives */`
    /// vs the catch-all `Err(e) => return Err(e) /* connection torn down */`.
    /// `send_failure_returns_transient_error` in `peer_connection.rs` exercises
    /// the symmetric SendFailed case the same way. A full `PeerConnection::recv`
    /// teardown harness would require substantial new scaffolding that the
    /// stream-level unit harness here does not expose; the error-classification
    /// assertions below pin the behavior the recv loop branches on.
    #[tokio::test(start_paused = true)]
    async fn cwnd_wait_timeout_fails_stream_not_connection()
    -> Result<(), Box<dyn std::error::Error>> {
        let (outbound_sender, _outbound_receiver) = mpsc::channel(100);
        let remote_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 8080);
        let message = vec![0u8; 10_000];
        let cipher = {
            let mut key = [0u8; 16];
            crate::config::GlobalRng::fill_bytes(&mut key);
            Aes128Gcm::new(&key.into())
        };

        // RealTime under tokio's paused clock auto-advances through the cwnd
        // wait loop's sleeps deterministically (same pattern as
        // test_send_stream_cwnd_wait_timeout).
        let time_source = RealTime::new();

        // cwnd of 1 byte: every real packet exceeds it, so the cwnd wait loop
        // never breaks and must hit CWND_WAIT_TIMEOUT.
        let congestion_controller = CongestionControlConfig::from_ledbat_config(LedbatConfig {
            initial_cwnd: 1,
            min_cwnd: 1,
            max_cwnd: 1,
            ..Default::default()
        })
        .build_arc();

        let sent_tracker = Arc::new(parking_lot::Mutex::new(SentPacketTracker::new()));
        let token_bucket = Arc::new(TokenBucket::new(1_000_000, 100_000_000));

        let send_task = GlobalExecutor::spawn(send_stream(
            StreamId::next(),
            Arc::new(AtomicU32::new(0)),
            Arc::new(TestSocket::new(outbound_sender)),
            remote_addr,
            Bytes::from(message),
            cipher,
            sent_tracker,
            token_bucket,
            congestion_controller,
            time_source,
            None,
            None,
        ));

        let err = send_task
            .await
            .expect("join error")
            .expect_err("cwnd wait should time out");

        // (1) The new stream-scoped variant, carrying the destination addr.
        assert!(
            matches!(err, TransportError::OutboundStreamFailed(addr) if addr == remote_addr),
            "expected OutboundStreamFailed({remote_addr}), got: {err:?}"
        );

        // (2) It must NOT be ConnectionClosed — that is the fatal arm the recv
        // loop would tear the connection down on.
        assert!(
            !matches!(err, TransportError::ConnectionClosed(_)),
            "cwnd timeout must NOT be ConnectionClosed (would kill the connection): {err:?}"
        );

        // (3) The recv loop branches on is_transient_send_failure(); true means
        // it logs and lets the op layer retry while the connection survives.
        assert!(
            err.is_transient_send_failure(),
            "cwnd timeout must be classified transient so the connection survives: {err:?}"
        );

        Ok(())
    }

    /// #4345 regression: after a stream aborts on CWND_WAIT_TIMEOUT, the same
    /// tracker + congestion controller must be drained so a follow-up stream on
    /// the SAME connection (the op-layer retry from PR #4367) does not inherit
    /// the wedge. Pre-seed the tracker with stream A's in-flight bytes; with
    /// cwnd=1, send_stream's cwnd-wait loop times out; assert tracker and
    /// flightsize are clean and a fresh stream B can register.
    #[tokio::test(start_paused = true)]
    async fn cwnd_wait_abort_drains_tracker_and_unwedges_connection()
    -> Result<(), Box<dyn std::error::Error>> {
        let (outbound_sender, _outbound_receiver) = mpsc::channel(100);
        let remote_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 8080);
        let cipher = {
            let mut key = [0u8; 16];
            crate::config::GlobalRng::fill_bytes(&mut key);
            Aes128Gcm::new(&key.into())
        };

        let time_source = RealTime::new();
        let congestion_controller = CongestionControlConfig::from_ledbat_config(LedbatConfig {
            initial_cwnd: 1,
            min_cwnd: 1,
            max_cwnd: 1,
            ..Default::default()
        })
        .build_arc();
        let sent_tracker = Arc::new(parking_lot::Mutex::new(SentPacketTracker::new()));
        let token_bucket = Arc::new(TokenBucket::new(1_000_000, 100_000_000));

        // Stream A: pre-seeded in-flight bytes, will wedge on cwnd-wait.
        let stream_a_id = StreamId::next();
        {
            let mut t = sent_tracker.lock();
            t.report_sent_packet_with_token(201, Some(stream_a_id), vec![0u8; 1500].into(), None);
            t.report_sent_packet_with_token(202, Some(stream_a_id), vec![0u8; 1500].into(), None);
        }
        congestion_controller.on_send(3000);

        let send_a = GlobalExecutor::spawn(send_stream(
            stream_a_id,
            Arc::new(AtomicU32::new(0)),
            Arc::new(TestSocket::new(outbound_sender)),
            remote_addr,
            Bytes::from(vec![0u8; 10_000]),
            cipher,
            sent_tracker.clone(),
            token_bucket,
            congestion_controller.clone(),
            time_source,
            None,
            None,
        ));
        let err_a = send_a.await.expect("join").expect_err("A must wedge");
        assert!(matches!(err_a, TransportError::OutboundStreamFailed(_)));

        assert_eq!(sent_tracker.lock().pending_count(), 0);
        assert_eq!(congestion_controller.flightsize(), 0);

        // Stream B registers on the same tracker — proves no state inherited from A.
        let stream_b_id = StreamId::next();
        sent_tracker.lock().report_sent_packet_with_token(
            301,
            Some(stream_b_id),
            vec![0u8; 1].into(),
            None,
        );
        assert_eq!(sent_tracker.lock().pending_count(), 1);

        Ok(())
    }

    /// This is a regression test for the critical bug where adding metadata to
    /// fragment #1 caused size overflow, breaking all streaming operations >1422 bytes.
    #[test]
    fn test_fragment_1_with_metadata_respects_max_size() {
        use crate::transport::symmetric_message::{StreamFragment, SymmetricMessage};

        // Test Case 1: Typical metadata size (~200 bytes)
        let typical_metadata = bytes::Bytes::from(vec![0u8; 200]);
        let meta_overhead = 1 + 8 + typical_metadata.len(); // Option discriminant + length + data
        let available_payload = MAX_DATA_SIZE.saturating_sub(meta_overhead);

        let fragment_with_typical_meta = StreamFragment {
            stream_id: StreamId::next_operations(),
            total_length_bytes: 10000,
            fragment_number: 1,
            payload: bytes::Bytes::from(vec![0u8; available_payload]),
            metadata_bytes: Some(typical_metadata),
        };

        let msg = SymmetricMessage {
            packet_id: 1,
            confirm_receipt: vec![],
            payload: SymmetricMessagePayload::from(fragment_with_typical_meta),
        };

        let serialized = bincode::serialize(&msg).expect("serialization should succeed");
        assert!(
            serialized.len() <= packet_data::MAX_DATA_SIZE,
            "Fragment #1 with typical metadata ({} bytes) exceeds MAX_DATA_SIZE: {} > {}",
            200,
            serialized.len(),
            packet_data::MAX_DATA_SIZE
        );

        // Test Case 2: Large metadata (500 bytes) - stress test
        let large_metadata = bytes::Bytes::from(vec![0u8; 500]);
        let large_meta_overhead = 1 + 8 + large_metadata.len();
        let available_payload_large = MAX_DATA_SIZE.saturating_sub(large_meta_overhead);

        let fragment_with_large_meta = StreamFragment {
            stream_id: StreamId::next_operations(),
            total_length_bytes: 10000,
            fragment_number: 1,
            payload: bytes::Bytes::from(vec![0u8; available_payload_large]),
            metadata_bytes: Some(large_metadata),
        };

        let msg_large = SymmetricMessage {
            packet_id: 2,
            confirm_receipt: vec![],
            payload: SymmetricMessagePayload::from(fragment_with_large_meta),
        };

        let serialized_large =
            bincode::serialize(&msg_large).expect("serialization should succeed");
        assert!(
            serialized_large.len() <= packet_data::MAX_DATA_SIZE,
            "Fragment #1 with large metadata ({} bytes) exceeds MAX_DATA_SIZE: {} > {}",
            500,
            serialized_large.len(),
            packet_data::MAX_DATA_SIZE
        );

        // Test Case 3: Fragment #2 (no metadata) should use full MAX_DATA_SIZE
        let fragment_2 = StreamFragment {
            stream_id: StreamId::next_operations(),
            total_length_bytes: 10000,
            fragment_number: 2,
            payload: bytes::Bytes::from(vec![0u8; MAX_DATA_SIZE]),
            metadata_bytes: None,
        };

        let msg_frag2 = SymmetricMessage {
            packet_id: 3,
            confirm_receipt: vec![],
            payload: SymmetricMessagePayload::from(fragment_2),
        };

        let serialized_frag2 =
            bincode::serialize(&msg_frag2).expect("serialization should succeed");
        assert!(
            serialized_frag2.len() <= packet_data::MAX_DATA_SIZE,
            "Fragment #2 (no metadata, full payload) exceeds MAX_DATA_SIZE: {} > {}",
            serialized_frag2.len(),
            packet_data::MAX_DATA_SIZE
        );
    }

    /// Test that pipe_stream aborts with OutboundStreamFailed when the cwnd
    /// wait exceeds CWND_WAIT_TIMEOUT. This exercises the same timeout logic as
    /// send_stream but through the pipe_stream code path with different state
    /// variables (sent_so_far as u64 bytes, no completion_tx).
    #[tokio::test(start_paused = true)]
    async fn test_pipe_stream_cwnd_wait_timeout() -> Result<(), Box<dyn std::error::Error>> {
        use crate::transport::peer_connection::streaming::StreamHandle;

        let (outbound_sender, _outbound_receiver) = mpsc::channel(100);
        let remote_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 8080);
        let cipher = {
            let mut key = [0u8; 16];
            crate::config::GlobalRng::fill_bytes(&mut key);
            Aes128Gcm::new(&key.into())
        };

        let time_source = RealTime::new();

        // Create a StreamHandle with a fragment already buffered so the cwnd
        // wait loop has work to do. The fragment is never ACKed (no recv task),
        // so the cwnd wait (CWND_WAIT_TIMEOUT = 3s) fires before the inactivity
        // timeout (STREAM_INACTIVITY_TIMEOUT = 5s).
        // Fragment must be <= FRAGMENT_PAYLOAD_SIZE (1130 bytes).
        let stream_id = StreamId::next();
        let handle = StreamHandle::new(stream_id, 10_000);
        handle
            .push_fragment(1, Bytes::from(vec![0u8; 1000]))
            .unwrap();

        // LEDBAT controller with a 1-byte cwnd: every real packet exceeds it,
        // so the cwnd wait loop never breaks and must hit CWND_WAIT_TIMEOUT.
        // (The previous version used CongestionControlConfig::default(), which
        // is FixedRate with current_cwnd() == usize::MAX/2 — the cwnd loop
        // never blocked, so the test silently exercised the inactivity-timeout
        // path instead of the cwnd-wait path it claims to test.)
        let congestion_controller = CongestionControlConfig::from_ledbat_config(LedbatConfig {
            initial_cwnd: 1,
            min_cwnd: 1,
            max_cwnd: 1,
            ..Default::default()
        })
        .build_arc();

        let sent_tracker = Arc::new(parking_lot::Mutex::new(SentPacketTracker::new()));
        let token_bucket = Arc::new(TokenBucket::new(1_000_000, 100_000_000));

        let pipe_task = GlobalExecutor::spawn(pipe_stream(
            handle,
            StreamId::next(),
            Arc::new(AtomicU32::new(0)),
            Arc::new(TestSocket::new(outbound_sender)),
            remote_addr,
            cipher,
            sent_tracker,
            token_bucket,
            congestion_controller,
            time_source,
            None,
        ));

        let result = pipe_task.await.expect("join error");
        assert!(
            matches!(result, Err(TransportError::OutboundStreamFailed(_))),
            "Expected OutboundStreamFailed after pipe_stream cwnd wait timeout, got: {:?}",
            result
        );

        Ok(())
    }

    /// Regression test for #4345 (relay-pipe inactivity-stall path,
    /// outbound_stream.rs:456): when the UPSTREAM inbound feed produces no
    /// fragment within STREAM_INACTIVITY_TIMEOUT, pipe_stream must fail only
    /// THIS stream — not tear down the DOWNSTREAM connection that carries other
    /// multiplexed ops. Before this PR the site returned ConnectionClosed
    /// (is_transient_send_failure() == false → recv loop's fatal arm).
    ///
    /// Drives the stall by registering a StreamHandle with NO fragment ever
    /// pushed, so `stream.next()` stays Pending and the select!'s
    /// inactivity-timeout arm fires. Asserts the error is OutboundStreamFailed,
    /// is_transient_send_failure() is true, and it is NOT ConnectionClosed.
    #[tokio::test(start_paused = true)]
    async fn pipe_stream_inactivity_stall_fails_stream_not_connection()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::transport::peer_connection::streaming::StreamHandle;

        let (outbound_sender, _outbound_receiver) = mpsc::channel(100);
        let remote_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 8080);
        let cipher = {
            let mut key = [0u8; 16];
            crate::config::GlobalRng::fill_bytes(&mut key);
            Aes128Gcm::new(&key.into())
        };

        let time_source = RealTime::new();

        // StreamHandle with NO fragment pushed: stream.next() stays Pending, so
        // the inactivity timeout (STREAM_INACTIVITY_TIMEOUT) fires before any
        // fragment can be sent. The stall is reached before the cwnd loop, so
        // the congestion-control config is irrelevant; use the default.
        let stream_id = StreamId::next();
        let handle = StreamHandle::new(stream_id, 10_000);

        let congestion_controller = CongestionControlConfig::default().build_arc();
        let sent_tracker = Arc::new(parking_lot::Mutex::new(SentPacketTracker::new()));
        let token_bucket = Arc::new(TokenBucket::new(1_000_000, 100_000_000));

        let pipe_task = GlobalExecutor::spawn(pipe_stream(
            handle,
            StreamId::next(),
            Arc::new(AtomicU32::new(0)),
            Arc::new(TestSocket::new(outbound_sender)),
            remote_addr,
            cipher,
            sent_tracker,
            token_bucket,
            congestion_controller,
            time_source,
            None,
        ));

        let err = pipe_task
            .await
            .expect("join error")
            .expect_err("inactivity stall should fail the stream");

        assert!(
            matches!(err, TransportError::OutboundStreamFailed(addr) if addr == remote_addr),
            "expected OutboundStreamFailed({remote_addr}) on inactivity stall, got: {err:?}"
        );
        assert!(
            !matches!(err, TransportError::ConnectionClosed(_)),
            "inactivity stall must NOT be ConnectionClosed (would kill the connection): {err:?}"
        );
        assert!(
            err.is_transient_send_failure(),
            "inactivity stall must be classified transient so the connection survives: {err:?}"
        );

        Ok(())
    }

    /// Regression test for #4345 (relay-pipe inbound-stream-error path,
    /// outbound_stream.rs:471): when the UPSTREAM inbound stream yields an
    /// error, pipe_stream must fail only THIS stream — not the DOWNSTREAM
    /// connection. Before this PR the site returned ConnectionClosed
    /// (is_transient_send_failure() == false → recv loop's fatal arm).
    ///
    /// Drives the error by cancelling the StreamHandle so the inbound stream
    /// yields Err(StreamError::Cancelled). Asserts the error is
    /// OutboundStreamFailed, is_transient_send_failure() is true, and it is NOT
    /// ConnectionClosed.
    #[tokio::test(start_paused = true)]
    async fn pipe_stream_inbound_error_fails_stream_not_connection()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::transport::peer_connection::streaming::StreamHandle;

        let (outbound_sender, _outbound_receiver) = mpsc::channel(100);
        let remote_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 8080);
        let cipher = {
            let mut key = [0u8; 16];
            crate::config::GlobalRng::fill_bytes(&mut key);
            Aes128Gcm::new(&key.into())
        };

        let time_source = RealTime::new();

        // Cancel the handle so the inbound stream yields Err(Cancelled). The
        // poll_next cancelled-check fires before any cwnd wait, so the
        // congestion-control config is irrelevant; use the default.
        let stream_id = StreamId::next();
        let handle = StreamHandle::new(stream_id, 10_000);
        handle
            .push_fragment(1, Bytes::from(vec![0u8; 1000]))
            .unwrap();
        handle.cancel();

        let congestion_controller = CongestionControlConfig::default().build_arc();
        let sent_tracker = Arc::new(parking_lot::Mutex::new(SentPacketTracker::new()));
        let token_bucket = Arc::new(TokenBucket::new(1_000_000, 100_000_000));

        let pipe_task = GlobalExecutor::spawn(pipe_stream(
            handle,
            StreamId::next(),
            Arc::new(AtomicU32::new(0)),
            Arc::new(TestSocket::new(outbound_sender)),
            remote_addr,
            cipher,
            sent_tracker,
            token_bucket,
            congestion_controller,
            time_source,
            None,
        ));

        let err = pipe_task
            .await
            .expect("join error")
            .expect_err("inbound stream error should fail the stream");

        assert!(
            matches!(err, TransportError::OutboundStreamFailed(addr) if addr == remote_addr),
            "expected OutboundStreamFailed({remote_addr}) on inbound error, got: {err:?}"
        );
        assert!(
            !matches!(err, TransportError::ConnectionClosed(_)),
            "inbound error must NOT be ConnectionClosed (would kill the connection): {err:?}"
        );
        assert!(
            err.is_transient_send_failure(),
            "inbound error must be classified transient so the connection survives: {err:?}"
        );

        Ok(())
    }
}
