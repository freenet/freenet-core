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
        sent_packet_tracker::{PacketStream, SentPacketTracker},
        symmetric_message::{self},
    },
};

use futures::StreamExt;

use super::StreamId;

/// Maximum time to wait for congestion window space *per fragment* before aborting a stream
/// transfer. Resets for each fragment, so a slow-but-progressing transfer won't time out.
///
/// Must be shorter than STREAM_INACTIVITY_TIMEOUT (5s) so the sender fails first with a
/// diagnostic message rather than the receiver timing out silently.
///
/// With STREAM_INACTIVITY_TIMEOUT at 5s, a 3s cwnd wait gives the sender 2s headroom
/// to fail and report before the receiver's inactivity timeout fires.
const CWND_WAIT_TIMEOUT: Duration = Duration::from_secs(3);

/// Release the in-flight bytes of an aborting outbound stream from the
/// connection's flight size (issue #4345).
///
/// When a stream aborts (cwnd-wait timeout, upstream stall/error, or a mid-send
/// `packet_sending` failure) its already-sent, unacked fragments are still
/// counted in the connection-wide flight size. Without this they stay pinned
/// until each fragment is independently ACKed or ages out via
/// `MAX_PACKET_RETRANSMITS` (~6s) — and FixedRate's loss-pause caps cwnd at the
/// frozen flight size, so a single aborted stream starves every subsequent
/// stream on the connection (the #4345 symptom). `drop_stream` removes the
/// stream's packets from the tracker and returns their exact byte total, which
/// we hand to `release_flightsize` so the next stream sees an open cwnd.
///
/// # Locking
///
/// Mirrors the ACK path's ordering (`peer_connection.rs`): the tracker lock is
/// dropped BEFORE calling into the congestion controller, so the tracker mutex
/// is never held across a `release_flightsize` call (and never across an
/// `.await` — neither call awaits). This avoids any tracker↔controller
/// lock-ordering hazard.
///
/// Idempotent and cheap on the common path: a stream with no in-flight packets
/// (already fully ACKed, or zero fragments sent) drops nothing and releases 0.
fn release_aborted_stream_flightsize<T: TimeSource>(
    stream_id: StreamId,
    sent_packet_tracker: &parking_lot::Mutex<SentPacketTracker<T>>,
    congestion_controller: &CongestionController<T>,
) {
    let released = sent_packet_tracker.lock().drop_stream(stream_id);
    if released > 0 {
        // u64 -> usize: a connection's in-flight bytes never approach usize::MAX
        // on any supported platform; saturate defensively rather than wrap.
        congestion_controller.release_flightsize(released.min(usize::MAX as u64) as usize);
        tracing::debug!(
            stream_id = %stream_id.0,
            released_bytes = released,
            "Released aborted stream's in-flight bytes from flight size (#4345)"
        );
    }
}

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

/// A congestion window as reported in transfer telemetry: `None` when the
/// controller has no bounded window. FixedRate outside a loss pause reports
/// `usize::MAX / 2`, which used to surface as `u32::MAX` after truncation.
fn reported_cwnd(cwnd: usize) -> Option<u32> {
    if cwnd >= usize::MAX / 2 {
        return None;
    }
    u32::try_from(cwnd).ok()
}

/// Per-connection RTT and loss facts for a stream that has just handed its
/// last fragment to the socket, read from the connection's sent-packet
/// tracker (not the congestion controller, whose RTT/loss stats are zero
/// under FixedRate).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StreamEndTelemetry {
    /// Connection SRTT; `None` until it has a data-ACK RTT sample.
    final_srtt_ms: Option<u32>,
    /// Connection minimum RTT; `None` under the same condition. Together
    /// with `final_srtt_ms` it exposes queueing delay (loaded vs base RTT).
    min_rtt_ms: Option<u32>,
    /// RTO expiries on the connection while the transfer ran.
    rto_expiries: u32,
}

/// Collect [`StreamEndTelemetry`] and register the stream for final-ACK
/// timing. A stream whose fragments are all already acknowledged is
/// published to `TRANSPORT_METRICS` here, since no later receipt batch may
/// ever come to drain it.
fn finish_stream_telemetry<T: TimeSource>(
    sent_packet_tracker: &parking_lot::Mutex<SentPacketTracker<T>>,
    stream_id: StreamId,
    start_nanos: u64,
    rto_expiries_at_start: u64,
) -> StreamEndTelemetry {
    let to_ms = |d: Duration| d.as_millis().min(u32::MAX as u128) as u32;
    let (already_acked, telemetry) = {
        let mut tracker = sent_packet_tracker.lock();
        let already_acked = tracker.watch_stream_ack(stream_id, start_nanos);
        let srtt = tracker.smoothed_rtt();
        let telemetry = StreamEndTelemetry {
            final_srtt_ms: srtt.map(to_ms),
            // The tracker's min RTT holds a 100 ms placeholder until the
            // first sample, which arrives together with the SRTT.
            min_rtt_ms: srtt.map(|_| to_ms(tracker.min_rtt())),
            rto_expiries: tracker
                .rto_expiries()
                .saturating_sub(rto_expiries_at_start)
                .min(u32::MAX as u64) as u32,
        };
        (already_acked, telemetry)
    };
    if let Some(elapsed_nanos) = already_acked {
        crate::transport::TRANSPORT_METRICS.record_transfer_acked(elapsed_nanos / 1_000_000);
    }
    telemetry
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
    completion_tx: Option<oneshot::Sender<crate::transport::StreamSendOutcome>>,
    progress: Option<Arc<dyn crate::transport::StreamProgressObserver>>,
) -> Result<TransferStats, TransportError> {
    let start_time = time_source.now();
    let start_nanos = time_source.now_nanos();
    let rto_expiries_at_start = sent_packet_tracker.lock().rto_expiries();
    // Largest bytes-in-flight on the connection as this stream sent a
    // fragment (telemetry: queue build-up while it ran).
    let mut peak_flightsize: usize = 0;
    let bytes_to_send = stream_to_send.len() as u64;

    // Receivers admit only lengths that pass `check_declared_stream_length`.
    if let Err(rejection) = super::streaming::check_declared_stream_length(bytes_to_send) {
        tracing::warn!(
            stream_id = %stream_id.0,
            peer_addr = %destination_addr,
            length_bytes = bytes_to_send,
            max_stream_bytes = super::streaming::MAX_STREAM_TOTAL_BYTES,
            ?rejection,
            "Not sending stream: length outside the stream limits"
        );
        if let Some(tx) = completion_tx {
            let _ignored = tx.send(crate::transport::StreamSendOutcome::Dropped);
        }
        return Err(TransportError::OutboundStreamInvalidLength(
            destination_addr,
            bytes_to_send,
        ));
    }

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
    // The full stream length, as a u64.
    let total_length_bytes = bytes_to_send;
    // Fragment #1 must carry at least one payload byte; metadata too large to
    // leave room for one is not embedded (it is also sent separately).
    let metadata = metadata.filter(|meta| 1 + 8 + meta.len() < MAX_DATA_SIZE);
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
                // Aggregate sender-abort counter (Group B telemetry): the
                // cwnd-wait timed out (ACKs stopped arriving) and this stream is
                // being failed. Bucketed once here, never per-fragment.
                crate::node::network_status::record_stream_send_abort_cwnd();
                // Fail only this stream, not the connection (#4345). A cwnd-wait
                // timeout means ACKs stopped arriving for this transfer; tearing
                // down the whole connection would kill every other operation
                // multiplexed on it. The op layer times out and retries against
                // another candidate; the idle timeout decides connection liveness.
                //
                // Release this stream's already-sent in-flight bytes so the
                // frozen flight size doesn't starve the next stream (#4345).
                release_aborted_stream_flightsize(
                    stream_id,
                    sent_packet_tracker.as_ref(),
                    congestion_controller.as_ref(),
                );
                //
                // NOTE: `completion_tx` is intentionally NOT signaled here — it
                // is dropped by this early return. broadcast_queue awaits it with
                // a timeout and treats the resulting oneshot RecvError as a
                // *drop, not a delivery* (broadcast_queue.rs: `Ok(Err(_))` arm,
                // #4235), releasing the permit immediately without refreshing
                // peer interest. Do NOT "fix" this by adding a blocking
                // `tx.send(..)` here — a blocking send in this stream task is the
                // backpressure pattern channel-safety.md warns against.
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
        // Sampled after cwnd admission, the token-bucket wait and `on_send`,
        // so it is bytes actually in flight as this fragment goes out.
        peak_flightsize = peak_flightsize.max(congestion_controller.flightsize());

        if let Err(e) = super::packet_sending(
            destination_addr,
            &socket,
            packet_id,
            &outbound_symmetric_key,
            vec![],
            symmetric_message::StreamFragment {
                stream_id,
                total_length_bytes,
                fragment_number: next_fragment_number,
                payload: fragment,
                metadata_bytes,
            },
            sent_packet_tracker.as_ref(),
            packet_size,
            token,
            PacketStream::Stream(stream_id),
            &last_packet_id,
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
            // Release this stream's in-flight bytes (#4345). Two parts:
            // (1) drop_stream releases the already-sent, tracked fragments;
            // (2) the CURRENT fragment's on_send bytes were added to flight size
            //     just above (`on_send_with_token`) but the send failed, so the
            //     packet was never registered in the tracker — drop_stream can't
            //     see it. Release packet_size explicitly so those bytes don't leak.
            //
            // CORRECTNESS GUARD: step (2) is correct ONLY because a failed send
            // never registers the packet. `packet_sending` registers per
            // successfully-sent packet; with the empty `confirm_receipt` used
            // here it takes the single-packet path, so a send error means ZERO
            // registrations and drop_stream cannot have already released this
            // packet_id. If a future caller passes non-empty receipts (multi-
            // packet path) some sub-packets could register before a later one
            // fails — then drop_stream would release them AND this explicit call
            // would double-count. The debug_assert pins the invariant so that
            // change trips tests.
            debug_assert!(
                !sent_packet_tracker.lock().contains_packet(packet_id),
                "mid-send failure: failed packet {packet_id} must be absent from \
                 the tracker before explicit release (else double-release, #4345)"
            );
            release_aborted_stream_flightsize(
                stream_id,
                sent_packet_tracker.as_ref(),
                congestion_controller.as_ref(),
            );
            congestion_controller.release_flightsize(packet_size);
            // Signal completion (error path) so broadcast queue can release the
            // permit. The transfer failed mid-flight, so report a drop (#4235):
            // the queue must NOT treat this as a delivery.
            if let Some(tx) = completion_tx {
                // Receiver may be dropped if queue timed out; ignore the error.
                let _ignored = tx.send(crate::transport::StreamSendOutcome::Dropped);
            }
            return Err(e);
        }

        next_fragment_number += 1;
        sent_so_far += 1;
        if let Some(p) = &progress {
            // No timestamp is passed: the observer reads its OWN clock (the
            // originator's TimeSource), NOT this connection's time_source —
            // the two have different epochs and mixing them defeats the
            // inactivity timeout (#4001 single-epoch invariant; see
            // StreamProgressObserver).
            p.on_fragment_sent();
        }
    }

    // Gather congestion control stats for telemetry
    // Use algorithm-specific stats when available for detailed metrics
    let generic_stats = congestion_controller.stats();
    let ledbat_stats = congestion_controller.ledbat_stats();
    let bbr_stats = congestion_controller.bbr_stats();
    let elapsed = time_source.now().saturating_sub(start_time);
    let end = finish_stream_telemetry(
        &sent_packet_tracker,
        stream_id,
        start_nanos,
        rto_expiries_at_start,
    );
    let peak_cwnd = reported_cwnd(generic_stats.peak_cwnd);
    let final_cwnd = reported_cwnd(generic_stats.cwnd);

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
        peak_cwnd,
        final_cwnd,
        ledbat_stats.as_ref().map(|s| s.periodic_slowdowns as u32),
        end.final_srtt_ms,
        Some(generic_stats.ssthresh as u32),
        ledbat_stats.as_ref().map(|s| s.min_ssthresh_floor as u32),
        Some(end.rto_expiries),
        end.min_rtt_ms,
        Some(peak_flightsize.min(u32::MAX as usize) as u32),
        TransferDirection::Send,
    );

    // Signal completion (success path) so broadcast queue can release the
    // permit. The transfer completed end-to-end, so report a real delivery
    // (#4235) — only this case refreshes peer interest / caches the summary.
    if let Some(tx) = completion_tx {
        // Receiver may be dropped if queue timed out; ignore the error.
        let _ignored = tx.send(crate::transport::StreamSendOutcome::Delivered);
    }

    Ok(TransferStats {
        stream_id: stream_id.0 as u64,
        remote_addr: destination_addr,
        bytes_transferred: bytes_to_send,
        elapsed,
        peak_cwnd_bytes: peak_cwnd,
        final_cwnd_bytes: final_cwnd,
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
        total_timeouts: end.rto_expiries,
        final_flightsize: generic_stats.flightsize as u32,
        configured_rate: congestion_controller.configured_rate() as u32,
    })
}

/// Capacity of the pipe's outgoing fragment #1 when the first chunk is longer
/// than a fragment: room for this hop's own metadata header (#2757) when there
/// is any and at least one payload byte still fits, reserving exactly what
/// `send_stream` reserves (the metadata plus its 1-byte option tag and 8-byte
/// length); otherwise a full fragment.
fn first_fragment_capacity(metadata: Option<&Bytes>) -> usize {
    match metadata {
        Some(meta) if 1 + 8 + meta.len() < MAX_DATA_SIZE => MAX_DATA_SIZE - (1 + 8 + meta.len()),
        _ => MAX_DATA_SIZE,
    }
}

/// Re-chunks an inbound stream into the pipe's outgoing fragment payloads,
/// whatever chunk sizes the receiving transport delivered.
///
/// The receiver gives a stream `ceil(total / FRAGMENT_PAYLOAD_SIZE) + 1`
/// fragment slots and rejects empty fragments other than #1, so:
/// - fragment #1 is the first chunk as received if it fits in a fragment
///   (that keeps a UDP-received stream's boundaries exactly as they were,
///   including an empty #1 from an older sender); a longer first chunk is
///   cut at `first_capacity` (see [`first_fragment_capacity`]);
/// - every later fragment is exactly `FRAGMENT_PAYLOAD_SIZE` bytes except the
///   last, filled from as many chunks as it takes; empty chunks are dropped.
///
/// A UDP-received stream arrives as one chunk per fragment, full except
/// possibly #1 and the last, so its outgoing fragments are its incoming ones.
/// State lives in the struct, so dropping a `next()` future loses nothing.
/// After an `Err` the stream ends.
struct Rechunk<St> {
    inner: St,
    first_capacity: usize,
    first_sent: bool,
    buf: bytes::BytesMut,
    done: bool,
}

impl<St> Rechunk<St> {
    fn new(inner: St, first_capacity: usize) -> Self {
        Self {
            inner,
            first_capacity,
            first_sent: false,
            buf: bytes::BytesMut::new(),
            done: false,
        }
    }
}

impl<St> futures::Stream for Rechunk<St>
where
    St: futures::Stream<Item = Result<Bytes, super::streaming::StreamError>> + Unpin,
{
    type Item = Result<Bytes, super::streaming::StreamError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use super::streaming_buffer::FRAGMENT_PAYLOAD_SIZE;
        use std::task::Poll;
        let this = self.get_mut();
        loop {
            if this.done {
                return Poll::Ready(None);
            }
            if this.first_sent && this.buf.len() >= FRAGMENT_PAYLOAD_SIZE {
                return Poll::Ready(Some(Ok(this.buf.split_to(FRAGMENT_PAYLOAD_SIZE).freeze())));
            }
            match std::pin::Pin::new(&mut this.inner).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Err(e))) => {
                    this.done = true;
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(Some(Ok(chunk))) if !this.first_sent => {
                    this.first_sent = true;
                    if chunk.len() <= FRAGMENT_PAYLOAD_SIZE {
                        return Poll::Ready(Some(Ok(chunk)));
                    }
                    let mut chunk = chunk;
                    let first = chunk.split_to(this.first_capacity);
                    this.buf.extend_from_slice(&chunk);
                    return Poll::Ready(Some(Ok(first)));
                }
                Poll::Ready(Some(Ok(chunk))) => {
                    // A full chunk with nothing buffered is already a
                    // fragment (the common UDP case): pass it on uncopied.
                    if this.buf.is_empty() && chunk.len() == FRAGMENT_PAYLOAD_SIZE {
                        return Poll::Ready(Some(Ok(chunk)));
                    }
                    // Empty chunks are dropped: an empty fragment other
                    // than #1 is refused by the receiver.
                    this.buf.extend_from_slice(&chunk);
                }
                Poll::Ready(None) => {
                    this.done = true;
                    if !this.buf.is_empty() {
                        return Poll::Ready(Some(Ok(this.buf.split().freeze())));
                    }
                }
            }
        }
    }
}

/// Pipes an inbound stream to an outbound connection, forwarding fragments as they arrive.
///
/// Unlike `send_stream` which takes complete data and fragments it, this reads
/// an [`IncomingStream`](crate::transport::IncomingStream) chunk by chunk and
/// forwards it incrementally, re-chunked into fragments by [`Rechunk`]. This
/// avoids full reassembly at intermediate nodes, reducing latency.
///
/// The outbound stream uses a new `outbound_stream_id` so the receiver sees
/// a fresh stream. Fragments are sent with the same BBR congestion control
/// and token bucket rate limiting as `send_stream`.
#[allow(clippy::too_many_arguments)]
pub(super) async fn pipe_stream<S: super::super::Socket, T: TimeSource>(
    inbound_handle: crate::transport::IncomingStream,
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
    progress: Option<Arc<dyn crate::transport::StreamProgressObserver>>,
) -> Result<TransferStats, TransportError> {
    pipe_chunks(
        inbound_handle.total_bytes(),
        inbound_handle.chunks(),
        outbound_stream_id,
        last_packet_id,
        socket,
        destination_addr,
        outbound_symmetric_key,
        sent_packet_tracker,
        token_bucket,
        congestion_controller,
        time_source,
        metadata,
        progress,
    )
    .await
}

/// [`pipe_stream`] over any chunk stream of `total_bytes` bytes, re-chunked
/// into fragments by [`Rechunk`].
#[allow(clippy::too_many_arguments)]
async fn pipe_chunks<S, T, C>(
    total_bytes: u64,
    chunks: C,
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
    progress: Option<Arc<dyn crate::transport::StreamProgressObserver>>,
) -> Result<TransferStats, TransportError>
where
    S: super::super::Socket,
    T: TimeSource,
    C: futures::Stream<Item = Result<Bytes, super::streaming::StreamError>> + Unpin,
{
    let start_time = time_source.now();
    let start_nanos = time_source.now_nanos();
    let rto_expiries_at_start = sent_packet_tracker.lock().rto_expiries();
    // See `send_stream`.
    let mut peak_flightsize: usize = 0;

    // Inbound admission applies the same check; repeated here so a forwarded
    // stream's declared length is within the limits whatever its source.
    if let Err(rejection) = super::streaming::check_declared_stream_length(total_bytes) {
        tracing::warn!(
            stream_id = %outbound_stream_id.0,
            peer_addr = %destination_addr,
            total_bytes,
            max_stream_bytes = super::streaming::MAX_STREAM_TOTAL_BYTES,
            ?rejection,
            "Not piping stream: length outside the stream limits"
        );
        return Err(TransportError::OutboundStreamInvalidLength(
            destination_addr,
            total_bytes,
        ));
    }

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

    // Whatever transport received the stream, forward it as fragments the
    // receiver accepts (see `Rechunk`).
    let mut stream = Rechunk::new(chunks, first_fragment_capacity(metadata.as_ref()));
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
                // liveness. Release the bytes already forwarded for this stream
                // so the frozen flight size doesn't starve the next stream.
                release_aborted_stream_flightsize(
                    outbound_stream_id,
                    sent_packet_tracker.as_ref(),
                    congestion_controller.as_ref(),
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
                // the inactivity-stall arm above. Release already-forwarded bytes.
                release_aborted_stream_flightsize(
                    outbound_stream_id,
                    sent_packet_tracker.as_ref(),
                    congestion_controller.as_ref(),
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
                // matching site in send_stream for the rationale. Release the
                // bytes already forwarded for this stream so the frozen flight
                // size doesn't starve the next stream.
                release_aborted_stream_flightsize(
                    outbound_stream_id,
                    sent_packet_tracker.as_ref(),
                    congestion_controller.as_ref(),
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
                // The size of the fragment as it would be serialized with the
                // metadata embedded, not an estimate: the old
                // `payload + 41 + meta` left out the metadata's 8-byte length
                // and could approve an embed that then failed to serialize.
                let required_size = symmetric_message::SymmetricMessage::stream_fragment_size(
                    outbound_stream_id,
                    total_bytes,
                    fragment_number,
                    &payload,
                    Some(&meta),
                );
                if required_size <= packet_data::MAX_DATA_SIZE as u64 {
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
        // Sampled after cwnd admission, the token-bucket wait and `on_send`,
        // so it is bytes actually in flight as this fragment goes out.
        peak_flightsize = peak_flightsize.max(congestion_controller.flightsize());

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
            packet_size,
            token,
            PacketStream::Stream(outbound_stream_id),
            &last_packet_id,
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
            // Release this stream's in-flight bytes (#4345): drop_stream for the
            // already-forwarded tracked fragments, plus packet_size for the
            // CURRENT fragment whose on_send bytes were added just above but
            // whose send failed (so it was never registered in the tracker).
            //
            // CORRECTNESS GUARD (same as send_stream's mid-send site): the
            // explicit packet_size release is correct ONLY because a failed send
            // never registers the packet — the empty `confirm_receipt` here
            // forces the single-packet path, so a send error means ZERO
            // registrations and drop_stream cannot have already released this
            // packet_id. A future multi-packet wiring would break this; the
            // debug_assert pins the invariant.
            debug_assert!(
                !sent_packet_tracker.lock().contains_packet(packet_id),
                "mid-send failure: failed packet {packet_id} must be absent from \
                 the tracker before explicit release (else double-release, #4345)"
            );
            release_aborted_stream_flightsize(
                outbound_stream_id,
                sent_packet_tracker.as_ref(),
                congestion_controller.as_ref(),
            );
            congestion_controller.release_flightsize(packet_size);
            return Err(e);
        }

        sent_so_far += packet_size as u64;
        fragment_number += 1;
        if let Some(p) = &progress {
            // No timestamp is passed: the observer reads its OWN clock (the
            // originator's TimeSource), NOT this connection's time_source —
            // the two have different epochs and mixing them defeats the
            // inactivity timeout (#4001 single-epoch invariant; see
            // StreamProgressObserver).
            p.on_fragment_sent();
        }
    }

    // The inbound stream has ended. Forwarding fewer bytes than the stream
    // declared is a failed pipe, however cleanly the input ended: the
    // receiver would wait for the rest until its inactivity timeout. Like the
    // other input failures above, it fails only this stream (#4345).
    if sent_so_far != total_bytes {
        let elapsed = time_source.now().saturating_sub(start_time);
        tracing::warn!(
            stream_id = %outbound_stream_id.0,
            destination = %destination_addr,
            sent_so_far,
            total_bytes,
            "pipe_stream input ended short of its declared total"
        );
        emit_transfer_failed(
            outbound_stream_id.0 as u64,
            destination_addr,
            sent_so_far,
            format!("inbound stream ended short: forwarded {sent_so_far}/{total_bytes} bytes"),
            elapsed.as_millis() as u64,
            TransferDirection::Send,
        );
        release_aborted_stream_flightsize(
            outbound_stream_id,
            sent_packet_tracker.as_ref(),
            congestion_controller.as_ref(),
        );
        return Err(TransportError::OutboundStreamFailed(destination_addr));
    }

    let generic_stats = congestion_controller.stats();
    let ledbat_stats = congestion_controller.ledbat_stats();
    let elapsed = time_source.now().saturating_sub(start_time);
    let end = finish_stream_telemetry(
        &sent_packet_tracker,
        outbound_stream_id,
        start_nanos,
        rto_expiries_at_start,
    );
    let peak_cwnd = reported_cwnd(generic_stats.peak_cwnd);
    let final_cwnd = reported_cwnd(generic_stats.cwnd);

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
        peak_cwnd,
        final_cwnd,
        ledbat_stats.as_ref().map(|s| s.periodic_slowdowns as u32),
        end.final_srtt_ms,
        Some(generic_stats.ssthresh as u32),
        ledbat_stats.as_ref().map(|s| s.min_ssthresh_floor as u32),
        Some(end.rto_expiries),
        end.min_rtt_ms,
        Some(peak_flightsize.min(u32::MAX as usize) as u32),
        TransferDirection::Send,
    );

    Ok(TransferStats {
        stream_id: outbound_stream_id.0 as u64,
        remote_addr: destination_addr,
        bytes_transferred: sent_so_far,
        elapsed,
        peak_cwnd_bytes: peak_cwnd,
        final_cwnd_bytes: final_cwnd,
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
        total_timeouts: end.rto_expiries,
        final_flightsize: generic_stats.flightsize as u32,
        configured_rate: congestion_controller.configured_rate() as u32,
    })
}

#[cfg(test)]
mod telemetry_tests {
    use super::*;
    use crate::simulation::VirtualTime;

    /// FixedRate's unbounded window (`usize::MAX / 2`) is not a measurement
    /// and must not surface as `u32::MAX`.
    #[test]
    fn reported_cwnd_hides_unbounded_window() {
        assert_eq!(reported_cwnd(usize::MAX / 2), None);
        assert_eq!(reported_cwnd(usize::MAX), None);
        assert_eq!(reported_cwnd(64 * 1024), Some(64 * 1024));
        assert_eq!(reported_cwnd(0), Some(0));
    }

    /// Final SRTT comes from the connection's sent-packet tracker (so it is
    /// populated under FixedRate, whose controller reports zero RTT) and the
    /// timeout count is the RTO expiries during this transfer only.
    #[test]
    fn finish_stream_telemetry_uses_tracker_srtt_and_rto_delta() {
        let time = VirtualTime::new();
        let tracker =
            parking_lot::Mutex::new(SentPacketTracker::new_with_time_source(time.clone()));
        let none = finish_stream_telemetry(&tracker, StreamId::next_transport_internal(), 0, 0);
        assert_eq!(
            (none.final_srtt_ms, none.min_rtt_ms, none.rto_expiries),
            (None, None, 0),
            "no RTT sample yet: SRTT and min RTT must be null, not 0 / placeholder"
        );

        {
            let mut t = tracker.lock();
            t.report_sent_packet(1, vec![0u8; 8].into());
        }
        time.advance(Duration::from_millis(80));
        tracker.lock().report_received_receipts(&[1]);

        // Two RTO expiries before the transfer starts, one during it.
        let force_rto = |id: u32| {
            tracker.lock().report_sent_packet(id, vec![0u8; 8].into());
            time.advance(Duration::from_secs(5));
            loop {
                let action = tracker.lock().get_resend();
                match action {
                    ResendActionForTest::Resend(..) => break,
                    ResendActionForTest::TlpProbe(..) => continue,
                    other @ (ResendActionForTest::WaitUntil(_)
                    | ResendActionForTest::Abandon { .. }) => {
                        panic!("expected a resend, got {other:?}")
                    }
                }
            }
            tracker.lock().report_received_receipts(&[id]);
        };
        force_rto(2);
        force_rto(3);
        let at_start = tracker.lock().rto_expiries();
        force_rto(4);

        let end =
            finish_stream_telemetry(&tracker, StreamId::next_transport_internal(), 0, at_start);
        assert_eq!(end.final_srtt_ms, Some(80));
        assert_eq!(end.min_rtt_ms, Some(80));
        assert_eq!(end.rto_expiries, 1);
    }

    use crate::transport::sent_packet_tracker::ResendAction as ResendActionForTest;

    /// A stream whose fragments were all acknowledged before it registered
    /// its final-ACK watch is published at once, not left queued until a
    /// receipt batch that a quiet connection may never get.
    #[test]
    fn already_acked_stream_is_published_immediately() {
        let time = VirtualTime::new();
        let tracker =
            parking_lot::Mutex::new(SentPacketTracker::new_with_time_source(time.clone()));
        let stream = StreamId::next_transport_internal();
        {
            let mut t = tracker.lock();
            t.report_sent_stream_packet_with_size(1, vec![0u8; 8].into(), None, stream, 8);
            t.report_sent_stream_packet_with_size(2, vec![0u8; 8].into(), None, stream, 8);
        }
        time.advance(Duration::from_millis(20));
        tracker.lock().report_received_receipts(&[1, 2]);
        let before = crate::transport::TRANSPORT_METRICS.transfers_acked_count();
        finish_stream_telemetry(&tracker, stream, 0, 0);
        assert!(
            crate::transport::TRANSPORT_METRICS.transfers_acked_count() > before,
            "the completion must reach TRANSPORT_METRICS immediately"
        );
        assert!(tracker.lock().take_completed_stream_acks().is_empty());
    }
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

    /// A stream that ends before reaching its declared total is a failed
    /// pipe, not a successful one: the receiver would wait for the rest.
    #[tokio::test]
    async fn pipe_fails_a_stream_that_ends_short_of_its_total() {
        use super::super::streaming_buffer::FRAGMENT_PAYLOAD_SIZE as F;
        let chunks = futures::stream::iter(vec![
            Ok(Bytes::from(vec![1u8; F])),
            Ok(Bytes::from(vec![2u8; F])),
        ]);
        let (fragments, result) = pipe_and_capture_result(3 * F as u64, chunks, None).await;
        assert_eq!(fragments.len(), 2, "premise: what arrived was forwarded");
        assert!(
            matches!(result, Err(TransportError::OutboundStreamFailed(_))),
            "a short stream must fail the pipe: {result:?}"
        );
    }

    /// Metadata is embedded in a forwarded fragment #1 only when the real
    /// serialized fragment fits. Here payload + metadata leaves 5 bytes by
    /// the old `payload + 41 + meta` estimate, but the metadata's 8-byte
    /// length prefix does not fit: the pipe must skip the embed, not fail.
    #[tokio::test]
    async fn pipe_skips_metadata_that_would_overflow_fragment_one() {
        use super::super::streaming_buffer::FRAGMENT_PAYLOAD_SIZE as F;
        let meta = Bytes::from(vec![9u8; 40]);
        let first = F - meta.len() - 5;
        let expected = payload_of(first + F);
        let chunks = futures::stream::iter(vec![
            Ok(Bytes::copy_from_slice(&expected[..first])),
            Ok(Bytes::copy_from_slice(&expected[first..])),
        ]);
        let (fragments, result) =
            pipe_and_capture_result(expected.len() as u64, chunks, Some(meta)).await;
        assert!(result.is_ok(), "the pipe must not fail: {result:?}");
        assert!(
            !fragments[0].has_metadata,
            "metadata that does not fit is skipped"
        );
        assert_receiver_accepts(&fragments, &expected);
    }

    /// Yields its chunks with a `Pending` before each one (waking itself),
    /// so a reader that drops `next()` on `Pending` drops it often.
    struct HesitantChunks {
        chunks: std::collections::VecDeque<Bytes>,
        ready: bool,
    }

    impl futures::Stream for HesitantChunks {
        type Item = Result<Bytes, super::super::streaming::StreamError>;

        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            if !self.ready {
                self.ready = true;
                cx.waker().wake_by_ref();
                return std::task::Poll::Pending;
            }
            self.ready = false;
            std::task::Poll::Ready(self.chunks.pop_front().map(Ok))
        }
    }

    /// `Rechunk` read inside `select!`, with `next()` dropped every time its
    /// input is `Pending`, loses and duplicates nothing.
    #[tokio::test]
    async fn rechunk_survives_dropped_next_futures() {
        use super::super::streaming_buffer::FRAGMENT_PAYLOAD_SIZE as F;
        use futures::StreamExt;
        let sizes = [300, 2 * F + 5, 0, F, 100, F - 1, 7];
        let expected = payload_of(sizes.iter().sum());
        let mut at = 0;
        let mut chunks = std::collections::VecDeque::new();
        for len in sizes {
            chunks.push_back(Bytes::copy_from_slice(&expected[at..at + len]));
            at += len;
        }
        let mut rechunk = super::Rechunk::new(
            HesitantChunks {
                chunks,
                ready: false,
            },
            F,
        );
        let (mut out, mut dropped) = (Vec::new(), 0);
        loop {
            tokio::select! {
                biased;
                item = rechunk.next() => match item {
                    Some(chunk) => out.push(chunk.expect("no error")),
                    None => break,
                },
                _ = std::future::ready(()) => dropped += 1,
            }
        }
        assert!(dropped > 0, "premise: next() was dropped mid-read");
        let bytes: Vec<u8> = out.iter().flat_map(|b| b.to_vec()).collect();
        assert_eq!(bytes, expected, "nothing lost or duplicated");
        assert_eq!(out[0].len(), 300, "#1 is the first chunk");
        for piece in &out[1..out.len() - 1] {
            assert_eq!(piece.len(), F, "middle fragments are full");
        }
    }

    /// One outgoing fragment as decoded off the wire.
    #[derive(Debug)]
    struct WireFragment {
        number: u32,
        total: u64,
        payload: Bytes,
        has_metadata: bool,
    }

    /// Pipe `chunks` (declared `total` bytes) through `pipe_chunks` and decode
    /// every packet it sends.
    async fn pipe_and_capture<C>(
        total: u64,
        chunks: C,
        metadata: Option<Bytes>,
    ) -> Vec<WireFragment>
    where
        C: futures::Stream<Item = Result<Bytes, super::super::streaming::StreamError>>
            + Unpin
            + Send
            + 'static,
    {
        let (fragments, result) = pipe_and_capture_result(total, chunks, metadata).await;
        result.expect("pipe succeeds");
        fragments
    }

    /// [`pipe_and_capture`], returning the pipe's result instead of
    /// requiring success.
    async fn pipe_and_capture_result<C>(
        total: u64,
        chunks: C,
        metadata: Option<Bytes>,
    ) -> (Vec<WireFragment>, Result<TransferStats, TransportError>)
    where
        C: futures::Stream<Item = Result<Bytes, super::super::streaming::StreamError>>
            + Unpin
            + Send
            + 'static,
    {
        let (outbound_sender, mut outbound_receiver) = mpsc::channel(1);
        let time_source = VirtualTime::new();
        let (congestion_controller, token_bucket) = unthrottled(&time_source);
        let cipher = random_cipher();
        let task = GlobalExecutor::spawn(super::pipe_chunks(
            total,
            chunks,
            StreamId::next(),
            Arc::new(AtomicU32::new(0)),
            Arc::new(TestSocket::new(outbound_sender)),
            SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 8080),
            cipher.clone(),
            Arc::new(parking_lot::Mutex::new(
                SentPacketTracker::new_with_time_source(time_source.clone()),
            )),
            token_bucket,
            congestion_controller,
            time_source,
            metadata,
            None,
        ));
        let mut fragments = Vec::new();
        while let Some((_, packet)) = outbound_receiver.recv().await {
            let decrypted = PacketData::<_, MAX_PACKET_SIZE>::from_buf(packet.as_ref())
                .try_decrypt_sym(&cipher)
                .expect("decrypt");
            let message = SymmetricMessage::deser(decrypted.data()).expect("deser");
            let SymmetricMessagePayload::StreamFragment {
                fragment_number,
                total_length_bytes,
                payload,
                metadata_bytes,
                ..
            } = message.payload
            else {
                panic!("expected a StreamFragment");
            };
            fragments.push(WireFragment {
                number: fragment_number,
                total: total_length_bytes,
                payload,
                has_metadata: metadata_bytes.is_some(),
            });
        }
        let result = task.await.expect("join");
        (fragments, result)
    }

    /// What a receiver accepts: numbered from 1, within
    /// `ceil(total / F) + 1` slots, no empty fragment but #1, every fragment
    /// but #1 and the last exactly full, and the bytes in order equal to
    /// `expected`.
    fn assert_receiver_accepts(fragments: &[WireFragment], expected: &[u8]) {
        use super::super::streaming_buffer::FRAGMENT_PAYLOAD_SIZE as F;
        let total = expected.len() as u64;
        let slots = expected.len().div_ceil(F) + 1;
        assert!(
            fragments.len() <= slots,
            "{} fragments for {slots} slots",
            fragments.len()
        );
        for (i, f) in fragments.iter().enumerate() {
            assert_eq!(f.number as usize, i + 1, "fragment numbering");
            assert_eq!(f.total, total, "declared total on every fragment");
            assert!(f.payload.len() <= F, "fragment {} over capacity", f.number);
            if f.number > 1 {
                assert!(!f.payload.is_empty(), "empty fragment {}", f.number);
            }
            if f.number > 1 && i + 1 < fragments.len() {
                assert_eq!(
                    f.payload.len(),
                    F,
                    "non-final fragment {} not full",
                    f.number
                );
            }
        }
        let bytes: Vec<u8> = fragments.iter().flat_map(|f| f.payload.to_vec()).collect();
        assert_eq!(bytes.len() as u64, total, "byte total");
        assert_eq!(bytes, expected, "bytes in order");
    }

    /// An independent check: a real receiver-side `StreamHandle` for the
    /// declared total accepts every fragment and assembles `expected`.
    async fn assert_real_receiver_assembles(fragments: &[WireFragment], expected: &[u8]) {
        let receiver =
            super::super::streaming::StreamHandle::new(StreamId::next(), expected.len() as u64);
        for f in fragments {
            receiver
                .push_fragment(f.number, f.payload.clone())
                .unwrap_or_else(|e| panic!("receiver refused fragment {}: {e:?}", f.number));
        }
        let assembled = tokio::time::timeout(Duration::from_secs(5), receiver.assemble())
            .await
            .expect("receiver completes")
            .expect("receiver assembles");
        assert_eq!(assembled, expected, "receiver assembled the stream");
    }

    fn payload_of(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    /// A UDP-received stream is forwarded with exactly its incoming fragment
    /// boundaries, which is what the pipe did before it re-chunked: a short
    /// #1 (the origin embedded its metadata), full middles, a short last.
    #[tokio::test]
    async fn pipe_forwards_a_udp_received_stream_with_its_own_boundaries() {
        use super::super::streaming_buffer::FRAGMENT_PAYLOAD_SIZE as F;
        let sizes = [F - 50, F, F, 300];
        let expected = payload_of(sizes.iter().sum());
        let handle =
            super::super::streaming::StreamHandle::new(StreamId::next(), expected.len() as u64);
        let mut at = 0;
        for (n, len) in sizes.iter().enumerate() {
            handle
                .push_fragment(
                    n as u32 + 1,
                    Bytes::copy_from_slice(&expected[at..at + len]),
                )
                .unwrap();
            at += len;
        }
        let stream = crate::transport::IncomingStream::from(handle);
        let fragments = pipe_and_capture(
            stream.total_bytes(),
            stream.chunks(),
            Some(Bytes::from(vec![1u8; 40])),
        )
        .await;
        let out: Vec<_> = fragments.iter().map(|f| f.payload.len()).collect();
        assert_eq!(out, sizes, "outgoing boundaries equal the incoming ones");
        assert!(fragments[0].has_metadata, "#1 carries this hop's metadata");
        assert_receiver_accepts(&fragments, &expected);
        assert_real_receiver_assembles(&fragments, &expected).await;
    }

    /// An older sender's empty fragment #1 is forwarded as an empty #1, as
    /// before.
    #[tokio::test]
    async fn pipe_keeps_an_empty_first_fragment_from_an_older_sender() {
        use super::super::streaming_buffer::FRAGMENT_PAYLOAD_SIZE as F;
        let expected = payload_of(2 * F);
        let handle =
            super::super::streaming::StreamHandle::new(StreamId::next(), expected.len() as u64);
        handle.push_fragment(1, Bytes::new()).unwrap();
        handle
            .push_fragment(2, Bytes::copy_from_slice(&expected[..F]))
            .unwrap();
        handle
            .push_fragment(3, Bytes::copy_from_slice(&expected[F..]))
            .unwrap();
        let stream = crate::transport::IncomingStream::from(handle);
        let fragments = pipe_and_capture(stream.total_bytes(), stream.chunks(), None).await;
        let out: Vec<_> = fragments.iter().map(|f| f.payload.len()).collect();
        assert_eq!(out, [0, F, F]);
        assert_receiver_accepts(&fragments, &expected);
        assert_real_receiver_assembles(&fragments, &expected).await;
    }

    /// A stream received in chunks longer than a fragment (as another
    /// transport would deliver it) goes out as fragments the receiver
    /// accepts, with fragment #1 sized to carry this hop's metadata.
    #[tokio::test]
    async fn pipe_rechunks_long_chunks_into_full_fragments() {
        use super::super::streaming_buffer::FRAGMENT_PAYLOAD_SIZE as F;
        let expected = payload_of(3 * F);
        let handle =
            super::super::streaming::StreamHandle::new(StreamId::next(), expected.len() as u64);
        handle
            .push_fragment(1, Bytes::copy_from_slice(&expected[..2 * F]))
            .unwrap();
        handle
            .push_fragment(2, Bytes::copy_from_slice(&expected[2 * F..]))
            .unwrap();
        let stream = crate::transport::IncomingStream::from(handle);
        let metadata = Bytes::from(vec![1u8; 40]);
        let fragments =
            pipe_and_capture(stream.total_bytes(), stream.chunks(), Some(metadata)).await;
        assert_eq!(
            fragments[0].payload.len(),
            F - (1 + 8 + 40),
            "#1 leaves room for the metadata, as send_stream does"
        );
        assert!(fragments[0].has_metadata, "and carries it");
        assert_receiver_accepts(&fragments, &expected);
        assert_real_receiver_assembles(&fragments, &expected).await;
    }

    /// Many short chunks, some empty, are packed into full fragments.
    #[tokio::test]
    async fn pipe_packs_short_and_empty_chunks_into_full_fragments() {
        use super::super::streaming_buffer::FRAGMENT_PAYLOAD_SIZE as F;
        let expected = payload_of(2 * F + 77);
        let mut chunks = Vec::new();
        for (i, piece) in expected.chunks(100).enumerate() {
            chunks.push(Ok(Bytes::copy_from_slice(piece)));
            if i % 3 == 0 {
                chunks.push(Ok(Bytes::new()));
            }
        }
        let fragments =
            pipe_and_capture(expected.len() as u64, futures::stream::iter(chunks), None).await;
        assert_receiver_accepts(&fragments, &expected);
        assert_real_receiver_assembles(&fragments, &expected).await;
    }

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
            StreamId::next_transport_internal(),
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

    /// Outcome of one [`run_virtual_stream_transfer`] run.
    struct VirtualTransfer {
        /// Raw packets the receiver observed, in order.
        packets: Vec<Arc<[u8]>>,
        /// Virtual time that elapsed. Time only moves when the driver
        /// advances it, and the driver only does so when `send_stream` is
        /// parked on a `time_source.sleep`, so this is exactly the sum of
        /// the token-bucket waits and independent of host load.
        virtual_elapsed: Duration,
        /// Number of times `send_stream` parked on the clock (each is one
        /// token-bucket or cwnd wait).
        sleeps: usize,
    }

    /// Payload size used by [`run_virtual_stream_transfer`].
    const TRANSFER_LEN: usize = 10_000;

    /// Run a single 10KB stream transfer under [`VirtualTime`] with the given
    /// token bucket (burst capacity, rate bytes/s).
    ///
    /// Nothing here reads the wall clock: the driver advances virtual time
    /// to the next pending wakeup whenever the sender is waiting on it, so
    /// the result depends only on the token-bucket arithmetic (#4163).
    async fn run_virtual_stream_transfer(
        burst_capacity: usize,
        rate_bytes_per_sec: usize,
    ) -> Result<VirtualTransfer, Box<dyn std::error::Error>> {
        let (outbound_sender, mut outbound_receiver) = mpsc::channel(100);
        let destination_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 1234));
        let key = Aes128Gcm::new_from_slice(&[0u8; 16])?;

        let message = vec![0u8; TRANSFER_LEN];

        let time_source = VirtualTime::new();
        let sent_tracker = Arc::new(parking_lot::Mutex::new(
            SentPacketTracker::new_with_time_source(time_source.clone()),
        ));

        // Use large cwnd since unit tests don't simulate ACKs to reduce flightsize
        let congestion_controller = CongestionControlConfig::from_ledbat_config(LedbatConfig {
            initial_cwnd: 1_000_000,
            min_cwnd: 1_000_000,
            max_cwnd: 1_000_000_000,
            ..Default::default()
        })
        .build_arc_with_time_source(time_source.clone());
        let token_bucket = Arc::new(TokenBucket::new_with_time_source(
            burst_capacity,
            rate_bytes_per_sec,
            time_source.clone(),
        ));

        // Spawn receiver task to collect packets
        let receiver_task = GlobalExecutor::spawn(async move {
            let mut packets = Vec::new();
            while let Some((addr, packet)) = outbound_receiver.recv().await {
                assert_eq!(addr, destination_addr);
                packets.push(packet);
            }
            packets
        });

        let start_nanos = time_source.now_nanos();
        let send_task = GlobalExecutor::spawn(send_stream(
            StreamId::next_transport_internal(),
            Arc::new(AtomicU32::new(0)),
            Arc::new(TestSocket::new(outbound_sender)),
            destination_addr,
            Bytes::from(message.clone()),
            key,
            sent_tracker,
            token_bucket,
            congestion_controller,
            time_source.clone(),
            None,
            None,
            None,
        ));

        // Drive virtual time: only advance when the sender is parked on a
        // sleep, otherwise yield so the sender and receiver can make progress.
        //
        // This is deterministic because `VirtualTime::sleep` registers its
        // wakeup with a fixed deadline when the sleep future is created (not
        // when it is first polled), and this test runs on tokio's
        // `current_thread` runtime, so the sender only runs while the driver
        // is yielding. Whether the driver observes a pending wakeup before or
        // after the sender has parked, the deadline is the same, so the
        // virtual elapsed time is the same.
        //
        // Guard against a stalled sender: a healthy transfer needs a handful
        // of iterations per fragment, so exceeding the cap means the sender is
        // stuck, and should fail loudly instead of hanging CI.
        const MAX_DRIVER_ITERATIONS: usize = 100_000;
        let mut sleeps = 0;
        let mut iterations = 0;
        while !send_task.is_finished() {
            iterations += 1;
            assert!(
                iterations <= MAX_DRIVER_ITERATIONS,
                "send_stream did not finish after {MAX_DRIVER_ITERATIONS} driver iterations \
                 ({sleeps} virtual sleeps, virtual time {:?}): the sender is stalled",
                Duration::from_nanos(time_source.now_nanos() - start_nanos),
            );
            if time_source.advance_to_next_wakeup().is_some() {
                sleeps += 1;
            }
            tokio::task::yield_now().await;
        }
        send_task.await??;
        let virtual_elapsed = Duration::from_nanos(time_source.now_nanos() - start_nanos);

        // The sender (and its socket's channel sender) is dropped, so the
        // receiver task ends.
        let packets = receiver_task.await?;
        let expected_packets = message.len().div_ceil(MAX_DATA_SIZE);
        assert_eq!(packets.len(), expected_packets);

        Ok(VirtualTransfer {
            packets,
            virtual_elapsed,
            sleeps,
        })
    }

    /// Assert `elapsed` matches the token-bucket pacing of `TRANSFER_LEN`
    /// bytes after the initial `burst`, within +/-5%. Virtual time is exact, so
    /// the window only absorbs fragment granularity; a limiter that is 1.5x too
    /// fast or too slow fails.
    fn assert_throttled_duration(elapsed: Duration, burst: usize, rate: usize) {
        let expected = Duration::from_secs_f64((TRANSFER_LEN - burst) as f64 / rate as f64);
        let (lo, hi) = (expected.mul_f64(0.95), expected.mul_f64(1.05));
        assert!(
            (lo..=hi).contains(&elapsed),
            "throttled transfer took {elapsed:?}, expected {expected:?} (window {lo:?}..={hi:?})"
        );
    }

    #[tokio::test]
    async fn test_send_stream_with_bandwidth_limit() -> Result<(), Box<dyn std::error::Error>> {
        // The bucket starts with `burst` tokens and every payload byte is
        // reserved against it, so the remaining (TRANSFER_LEN - burst) bytes
        // are paced at `rate`: 9000 B at 100 KB/s = 90 ms of virtual time.
        let (burst, rate) = (1_000, 100_000);
        let transfer = run_virtual_stream_transfer(burst, rate).await?;

        let key = Aes128Gcm::new_from_slice(&[0u8; 16])?;
        for packet in &transfer.packets {
            // Decrypt and verify it's a stream fragment
            let packet_data = PacketData::<_, MAX_PACKET_SIZE>::from_buf(packet);
            let decrypted = packet_data.try_decrypt_sym(&key).unwrap();
            let msg = SymmetricMessage::deser(decrypted.data()).unwrap();
            match msg.payload {
                SymmetricMessagePayload::StreamFragment { .. } => {
                    // Expected
                }
                SymmetricMessagePayload::AckConnection { .. }
                | SymmetricMessagePayload::AckConnectionV2 { .. }
                | SymmetricMessagePayload::ShortMessage { .. }
                | SymmetricMessagePayload::NoOp
                | SymmetricMessagePayload::Ping { .. }
                | SymmetricMessagePayload::Pong { .. } => panic!("Expected stream fragment"),
            }
        }

        debug!(
            "Transfer took (virtual): {:?}, packets sent: {}, sleeps: {}",
            transfer.virtual_elapsed,
            transfer.packets.len(),
            transfer.sleeps
        );
        assert!(
            transfer.sleeps > 0,
            "rate limiting must have parked the sender on the token bucket"
        );
        assert_throttled_duration(transfer.virtual_elapsed, burst, rate);

        Ok(())
    }

    /// With a bucket that never throttles (100KB burst, 1GB/s) the sender must
    /// never wait on the clock at all. This asserts the mechanism (no
    /// token-bucket wait, zero virtual time) rather than comparing wall-clock
    /// durations, which flaked under CPU load (#4163). The rate-limited run is
    /// the control: the same harness must observe waits when throttled, so a
    /// harness that cannot see sleeps cannot pass this test vacuously.
    #[tokio::test]
    async fn test_send_stream_without_bandwidth_limit() -> Result<(), Box<dyn std::error::Error>> {
        let (burst, rate) = (1_000, 100_000);
        let limited = run_virtual_stream_transfer(burst, rate).await?;
        assert!(
            limited.sleeps > 0,
            "control: the rate-limited run must wait on the token bucket"
        );
        assert_throttled_duration(limited.virtual_elapsed, burst, rate);

        let unlimited = run_virtual_stream_transfer(100_000, 1_000_000_000).await?;
        assert_eq!(
            unlimited.sleeps, 0,
            "an unlimited token bucket must never make the sender wait"
        );
        assert_eq!(
            unlimited.virtual_elapsed,
            Duration::ZERO,
            "an unlimited transfer must consume no virtual time"
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
            StreamId::next_transport_internal(),
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
    /// which `is_transient()` classifies as non-fatal, so the recv
    /// loop logs and lets the op layer retry while the connection survives (the
    /// idle timeout remains the sole authority on connection liveness).
    ///
    /// This test drives the real `send_stream` to the cwnd-wait timeout with a
    /// 1-byte cwnd (no packet ever fits, so the wait loop never breaks) and
    /// asserts:
    ///   1. the error IS `OutboundStreamFailed` (not `ConnectionClosed`), and
    ///   2. `is_transient()` is `true`, i.e. the recv loop takes
    ///      the connection-survival arm.
    ///
    /// Asserting on `is_transient()` is the load-bearing check:
    /// the recv-loop classification (`peer_connection.rs`) is exactly
    /// `Err(e) if e.is_transient() => /* connection survives */`
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
            StreamId::next_transport_internal(),
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

        // (3) The recv loop branches on is_transient(); true means
        // it logs and lets the op layer retry while the connection survives.
        assert!(
            err.is_transient(),
            "cwnd timeout must be classified transient so the connection survives: {err:?}"
        );

        Ok(())
    }

    fn big_cwnd() -> Arc<CongestionController<RealTime>> {
        CongestionControlConfig::from_ledbat_config(LedbatConfig {
            initial_cwnd: 1_000_000,
            min_cwnd: 1_000_000,
            max_cwnd: 1_000_000_000,
            ..Default::default()
        })
        .build_arc()
    }

    fn random_cipher() -> Aes128Gcm {
        let mut key = [0u8; 16];
        crate::config::GlobalRng::fill_bytes(&mut key);
        Aes128Gcm::new(&key.into())
    }

    /// A message longer than `MAX_STREAM_TOTAL_BYTES` is not started: no
    /// fragment is sent, the broadcast queue is told it was dropped, and the
    /// error is stream-scoped but NOT transient (a retry fails the same way).
    #[tokio::test(start_paused = true)]
    async fn send_stream_refuses_a_stream_above_the_length_cap()
    -> Result<(), Box<dyn std::error::Error>> {
        use super::super::streaming::MAX_STREAM_TOTAL_BYTES;

        let (outbound_sender, mut outbound_receiver) = mpsc::channel(100);
        let remote_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 8080);
        // Zeroed and never written, so the pages are never touched.
        let message = Bytes::from(vec![0u8; MAX_STREAM_TOTAL_BYTES as usize + 1]);
        let (completion_tx, completion_rx) = oneshot::channel();

        let result = send_stream(
            StreamId::next_transport_internal(),
            Arc::new(AtomicU32::new(0)),
            Arc::new(TestSocket::new(outbound_sender)),
            remote_addr,
            message,
            random_cipher(),
            Arc::new(parking_lot::Mutex::new(SentPacketTracker::new())),
            Arc::new(TokenBucket::new(1_000_000, 100_000_000)),
            big_cwnd(),
            RealTime::new(),
            None,
            Some(completion_tx),
            None,
        )
        .await;

        let err = result.expect_err("an over-cap stream must not be sent");
        assert!(
            matches!(
                err,
                TransportError::OutboundStreamInvalidLength(addr, len)
                    if addr == remote_addr && len == MAX_STREAM_TOTAL_BYTES + 1
            ),
            "got: {err:?}"
        );
        assert!(!err.is_transient());
        assert!(
            outbound_receiver.try_recv().is_err(),
            "no fragment of a refused stream may be sent"
        );
        assert!(matches!(
            completion_rx.await,
            Ok(crate::transport::StreamSendOutcome::Dropped)
        ));
        Ok(())
    }

    /// A message of exactly `MAX_STREAM_TOTAL_BYTES` is sent, and its
    /// fragments declare the full length as a u64.
    #[tokio::test(start_paused = true)]
    async fn send_stream_sends_a_stream_at_the_length_cap() -> Result<(), Box<dyn std::error::Error>>
    {
        use super::super::streaming::MAX_STREAM_TOTAL_BYTES;

        let (outbound_sender, mut outbound_receiver) = mpsc::channel(4);
        let remote_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 8080);
        let message = Bytes::from(vec![0u8; MAX_STREAM_TOTAL_BYTES as usize]);
        let cipher = random_cipher();

        let task = GlobalExecutor::spawn(send_stream(
            StreamId::next_transport_internal(),
            Arc::new(AtomicU32::new(0)),
            Arc::new(TestSocket::new(outbound_sender)),
            remote_addr,
            message,
            cipher.clone(),
            Arc::new(parking_lot::Mutex::new(SentPacketTracker::new())),
            Arc::new(TokenBucket::new(1_000_000, 100_000_000)),
            big_cwnd(),
            RealTime::new(),
            None,
            None,
            None,
        ));

        let (_, packet) = outbound_receiver
            .recv()
            .await
            .expect("an at-cap stream is sent");
        let decrypted = PacketData::<_, MAX_PACKET_SIZE>::from_buf(packet.as_ref())
            .try_decrypt_sym(&cipher)
            .map_err(|e| e.to_string())?;
        let SymmetricMessagePayload::StreamFragment {
            total_length_bytes,
            fragment_number,
            ..
        } = SymmetricMessage::deser(decrypted.data())?.payload
        else {
            panic!("expected a stream fragment");
        };
        assert_eq!(fragment_number, 1);
        assert_eq!(total_length_bytes, MAX_STREAM_TOTAL_BYTES);
        task.abort();
        Ok(())
    }

    /// Metadata that would leave fragment #1 no payload is not embedded, so
    /// every fragment carries payload and the stream arrives intact.
    #[tokio::test(start_paused = true)]
    async fn send_stream_never_emits_an_empty_fragment() -> Result<(), Box<dyn std::error::Error>> {
        for meta_len in [MAX_DATA_SIZE - 10, MAX_DATA_SIZE - 9, MAX_DATA_SIZE] {
            let (outbound_sender, mut outbound_receiver) = mpsc::channel(100);
            let remote_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 8080);
            let message: Vec<u8> = (0..3_000u32).map(|i| i as u8).collect();
            let cipher = random_cipher();
            let task = GlobalExecutor::spawn(send_stream(
                StreamId::next(),
                Arc::new(AtomicU32::new(0)),
                Arc::new(TestSocket::new(outbound_sender)),
                remote_addr,
                Bytes::from(message.clone()),
                cipher.clone(),
                Arc::new(parking_lot::Mutex::new(SentPacketTracker::new())),
                Arc::new(TokenBucket::new(1_000_000, 100_000_000)),
                big_cwnd(),
                RealTime::new(),
                Some(Bytes::from(vec![9u8; meta_len])),
                None,
                None,
            ));
            task.await??;
            let mut received = Vec::new();
            while let Ok((_, packet)) = outbound_receiver.try_recv() {
                let decrypted = PacketData::<_, MAX_PACKET_SIZE>::from_buf(packet.as_ref())
                    .try_decrypt_sym(&cipher)
                    .map_err(|e| e.to_string())?;
                let SymmetricMessagePayload::StreamFragment {
                    payload,
                    metadata_bytes,
                    fragment_number,
                    ..
                } = SymmetricMessage::deser(decrypted.data())?.payload
                else {
                    panic!("expected a stream fragment");
                };
                assert!(
                    !payload.is_empty(),
                    "meta_len {meta_len}: fragment {fragment_number} is empty"
                );
                if let Some(meta) = metadata_bytes {
                    assert_eq!(fragment_number, 1);
                    assert!(1 + 8 + meta.len() < MAX_DATA_SIZE);
                }
                received.extend_from_slice(&payload);
            }
            assert_eq!(received, message, "meta_len {meta_len}");
        }
        Ok(())
    }

    /// `pipe_stream` applies the same length check to the stream it forwards.
    #[tokio::test(start_paused = true)]
    async fn pipe_stream_refuses_a_stream_with_an_invalid_length() {
        use crate::transport::peer_connection::streaming::StreamHandle;

        let (outbound_sender, mut outbound_receiver) = mpsc::channel(4);
        let remote_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 8080);
        let result = pipe_stream(
            StreamHandle::new(StreamId::next(), 0).into(),
            StreamId::next(),
            Arc::new(AtomicU32::new(0)),
            Arc::new(TestSocket::new(outbound_sender)),
            remote_addr,
            random_cipher(),
            Arc::new(parking_lot::Mutex::new(SentPacketTracker::new())),
            Arc::new(TokenBucket::new(1_000_000, 100_000_000)),
            big_cwnd(),
            RealTime::new(),
            None,
            None,
        )
        .await;
        let err = result.expect_err("an empty stream must not be forwarded");
        assert!(
            matches!(err, TransportError::OutboundStreamInvalidLength(addr, 0) if addr == remote_addr),
            "got: {err:?}"
        );
        assert!(!err.is_transient());
        assert!(outbound_receiver.try_recv().is_err());
    }

    /// Core regression for issue #4345: when an outbound stream aborts on a
    /// cwnd-wait timeout, the fragments it already put in flight MUST be released
    /// from the connection's flight size immediately — not left pinned until each
    /// ages out via `MAX_PACKET_RETRANSMITS` (~6s).
    ///
    /// Reproduces the stranded-flightsize symptom: a cwnd just large enough for a
    /// couple of fragments, a socket that accepts sends but NO ACKs ever arrive,
    /// so flight size climbs to ~cwnd and the next fragment's cwnd wait times out.
    /// Before the stage-2 wiring, `send_stream` returned the error while leaving
    /// those bytes in flight; FixedRate/LEDBAT then keep cwnd pinned at the frozen
    /// flight size and every subsequent stream starves. This test asserts:
    ///   1. flight size is > 0 at the moment of abort (fragments are stranded),
    ///   2. flight size is back to 0 once `send_stream` returns (abort released
    ///      the stream's in-flight bytes via drop_stream + release_flightsize).
    #[tokio::test(start_paused = true)]
    async fn cwnd_wait_timeout_releases_stranded_flightsize()
    -> Result<(), Box<dyn std::error::Error>> {
        // Generous channel + a drain task so sends never block; we never ACK.
        let (outbound_sender, mut outbound_receiver) = mpsc::channel(1024);
        let remote_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 8080);
        // ~7 fragments worth of data, but cwnd only admits a couple.
        let message = vec![0u8; 10_000];
        let cipher = {
            let mut key = [0u8; 16];
            crate::config::GlobalRng::fill_bytes(&mut key);
            Aes128Gcm::new(&key.into())
        };

        let time_source = RealTime::new();

        // cwnd admits ~2 fragments (2 * MAX_DATA_SIZE), so a few fragments go out
        // and fill flight size, then the next fragment's cwnd wait times out.
        let cwnd = 2 * MAX_DATA_SIZE;
        let congestion_controller = CongestionControlConfig::from_ledbat_config(LedbatConfig {
            initial_cwnd: cwnd,
            min_cwnd: cwnd,
            max_cwnd: cwnd,
            ..Default::default()
        })
        .build_arc();

        let sent_tracker = Arc::new(parking_lot::Mutex::new(SentPacketTracker::new()));
        let token_bucket = Arc::new(TokenBucket::new(1_000_000, 100_000_000));

        // Drain the socket so send_to never blocks; deliberately never ACK, so
        // flight size only ever grows until the abort releases it.
        let drain =
            GlobalExecutor::spawn(async move { while outbound_receiver.recv().await.is_some() {} });

        let cc_for_send = congestion_controller.clone();
        let send_task = GlobalExecutor::spawn(send_stream(
            StreamId::next_transport_internal(),
            Arc::new(AtomicU32::new(0)),
            Arc::new(TestSocket::new(outbound_sender)),
            remote_addr,
            Bytes::from(message),
            cipher,
            sent_tracker,
            token_bucket,
            cc_for_send,
            time_source,
            None,
            None,
            None,
        ));

        let err = send_task
            .await
            .expect("join error")
            .expect_err("cwnd wait should time out");
        assert!(
            matches!(err, TransportError::OutboundStreamFailed(addr) if addr == remote_addr),
            "expected OutboundStreamFailed, got: {err:?}"
        );

        // The stranded fragments must have been released on abort. With no ACKs,
        // the ONLY release path is the #4345 abort wiring; without it this reads
        // ~cwnd (pinned) instead of 0.
        assert_eq!(
            congestion_controller.flightsize(),
            0,
            "issue #4345: aborting the stream must release its in-flight bytes \
             (flight size should be drained, was {})",
            congestion_controller.flightsize()
        );

        drain.abort();
        Ok(())
    }

    /// Issue #4345 (stage 2): aborting a stream that has NO in-flight packets
    /// (cwnd too small for even the first fragment, so nothing was ever sent)
    /// must be a clean no-op — drop_stream releases 0 and flight size stays 0.
    /// Guards against the abort wiring panicking or mis-releasing on the empty
    /// case.
    #[tokio::test(start_paused = true)]
    async fn cwnd_wait_timeout_zero_inflight_is_noop() -> Result<(), Box<dyn std::error::Error>> {
        let (outbound_sender, _outbound_receiver) = mpsc::channel(100);
        let remote_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 8080);
        let message = vec![0u8; 10_000];
        let cipher = {
            let mut key = [0u8; 16];
            crate::config::GlobalRng::fill_bytes(&mut key);
            Aes128Gcm::new(&key.into())
        };

        let time_source = RealTime::new();

        // 1-byte cwnd: no fragment ever fits, so the FIRST cwnd wait times out
        // before anything is sent. Flight size is 0 the whole time.
        let congestion_controller = CongestionControlConfig::from_ledbat_config(LedbatConfig {
            initial_cwnd: 1,
            min_cwnd: 1,
            max_cwnd: 1,
            ..Default::default()
        })
        .build_arc();

        let sent_tracker = Arc::new(parking_lot::Mutex::new(SentPacketTracker::new()));
        let token_bucket = Arc::new(TokenBucket::new(1_000_000, 100_000_000));

        let cc_for_send = congestion_controller.clone();
        let send_task = GlobalExecutor::spawn(send_stream(
            StreamId::next_transport_internal(),
            Arc::new(AtomicU32::new(0)),
            Arc::new(TestSocket::new(outbound_sender)),
            remote_addr,
            Bytes::from(message),
            cipher,
            sent_tracker,
            token_bucket,
            cc_for_send,
            time_source,
            None,
            None,
            None,
        ));

        let err = send_task
            .await
            .expect("join error")
            .expect_err("cwnd wait should time out");
        assert!(
            matches!(err, TransportError::OutboundStreamFailed(_)),
            "expected OutboundStreamFailed, got: {err:?}"
        );
        assert_eq!(
            congestion_controller.flightsize(),
            0,
            "aborting a stream with zero in-flight packets must keep flight size 0"
        );
        Ok(())
    }

    /// Issue #4235 — producer-side emission: a `send_stream` that runs to a
    /// successful completion MUST signal `StreamSendOutcome::Delivered`
    /// on its `completion_tx`.
    ///
    /// The broadcast queue gates interest-refresh / summary-cache on this exact
    /// outcome. If the success arm regressed to emit `Dropped` (or stopped
    /// emitting), the queue would suppress interest refresh on real deliveries —
    /// the inverse of #4235's symptom — and silently stop re-converging peers.
    #[tokio::test]
    async fn send_stream_success_signals_delivered() -> Result<(), Box<dyn std::error::Error>> {
        let (outbound_sender, mut outbound_receiver) = mpsc::channel(1);
        let remote_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 8080);
        let mut message = vec![0u8; 100_000];
        crate::config::GlobalRng::fill_bytes(&mut message);
        let cipher = {
            let mut key = [0u8; 16];
            crate::config::GlobalRng::fill_bytes(&mut key);
            Aes128Gcm::new(&key.into())
        };

        // VirtualTime + a cwnd/token bucket large enough that no sleeping or
        // cwnd waiting is needed (mirrors test_send_stream_success).
        let time_source = VirtualTime::new();
        let sent_tracker = Arc::new(parking_lot::Mutex::new(
            SentPacketTracker::new_with_time_source(time_source.clone()),
        ));
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

        let (completion_tx, completion_rx) = oneshot::channel();
        let background_task = GlobalExecutor::spawn(send_stream(
            StreamId::next_transport_internal(),
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
            Some(completion_tx),
            None,
        ));

        // Drain the socket so the stream can run to completion.
        while outbound_receiver.recv().await.is_some() {}

        let result = background_task.await?;
        assert!(result.is_ok(), "send_stream should succeed: {result:?}");
        assert_eq!(
            completion_rx.await,
            Ok(crate::transport::StreamSendOutcome::Delivered),
            "a successful stream MUST signal Delivered (#4235)"
        );
        Ok(())
    }

    /// Issue #4235 — producer-side emission: a `send_stream` that fails mid-flight
    /// (a `packet_sending` error) MUST signal `StreamSendOutcome::Dropped`
    /// on its `completion_tx`, so the broadcast queue releases the permit WITHOUT
    /// refreshing interest or caching the summary.
    ///
    /// The failure is induced by dropping the outbound socket receiver before the
    /// stream starts: `TestSocket::send_to` then returns `ConnectionAborted`, so
    /// the first `packet_sending` call errors and the error arm runs. A large
    /// cwnd ensures the cwnd-wait early return (which drops the oneshot instead
    /// of signaling) is NOT taken — we want the explicit `Dropped` send.
    #[tokio::test]
    async fn send_stream_failure_signals_dropped() -> Result<(), Box<dyn std::error::Error>> {
        let (outbound_sender, outbound_receiver) = mpsc::channel(1);
        // Drop the receiver: every TestSocket::send_to now fails with
        // ConnectionAborted, so the first packet_sending() errors out.
        drop(outbound_receiver);

        let remote_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 8080);
        let message = vec![0u8; 10_000];
        let cipher = {
            let mut key = [0u8; 16];
            crate::config::GlobalRng::fill_bytes(&mut key);
            Aes128Gcm::new(&key.into())
        };

        let time_source = VirtualTime::new();
        let sent_tracker = Arc::new(parking_lot::Mutex::new(
            SentPacketTracker::new_with_time_source(time_source.clone()),
        ));
        // Large cwnd so we reach packet_sending immediately rather than the
        // cwnd-wait early return (which would drop the oneshot, not send Dropped).
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

        let (completion_tx, completion_rx) = oneshot::channel();
        let cc_handle = congestion_controller.clone();
        let background_task = GlobalExecutor::spawn(send_stream(
            StreamId::next_transport_internal(),
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
            Some(completion_tx),
            None,
        ));

        let result = background_task.await?;
        assert!(
            result.is_err(),
            "send_stream should fail when the socket is closed: {result:?}"
        );
        assert_eq!(
            completion_rx.await,
            Ok(crate::transport::StreamSendOutcome::Dropped),
            "a mid-flight send failure MUST signal Dropped (#4235)"
        );
        // #4345 mid-send-failure release: the first fragment's on_send bytes were
        // added to flight size, then its send failed (so it was NEVER registered
        // in the tracker). The mid-send error path must release exactly those
        // bytes — once, via the explicit release_flightsize(packet_size) — so
        // flight size returns to 0. A double-release would saturate to 0 too, but
        // a future multi-packet wiring (non-empty confirm_receipt) could register
        // a packet AND hit this path, so the dedicated single-packet guard at the
        // call site (debug_assert) protects against that; here we pin the net-0.
        assert_eq!(
            cc_handle.flightsize(),
            0,
            "send_stream mid-send failure must release the failed fragment's bytes (net 0)"
        );
        Ok(())
    }

    /// #4345 mid-send-failure release for the PIPE path (outbound_stream.rs:692).
    /// Mirror of `send_stream_failure_signals_dropped`'s flight-size assertion
    /// but driving `pipe_stream`: a fragment is buffered, the socket fails on
    /// send, and the mid-send error path must release the failed fragment's
    /// on_send bytes exactly once so flight size returns to 0.
    #[tokio::test]
    async fn pipe_stream_mid_send_failure_releases_flightsize()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::transport::peer_connection::streaming::StreamHandle;

        let (outbound_sender, outbound_receiver) = mpsc::channel(1);
        // Drop the receiver so TestSocket::send_to returns ConnectionAborted on
        // the first send — the mid-send error path runs.
        drop(outbound_receiver);

        let remote_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 8080);
        let cipher = {
            let mut key = [0u8; 16];
            crate::config::GlobalRng::fill_bytes(&mut key);
            Aes128Gcm::new(&key.into())
        };

        let time_source = RealTime::new();

        // One buffered fragment (<= FRAGMENT_PAYLOAD_SIZE, 1130 bytes).
        let stream_id = StreamId::next_transport_internal();
        let handle = StreamHandle::new(stream_id, 1000);
        handle
            .push_fragment(1, Bytes::from(vec![0u8; 1000]))
            .unwrap();

        // Large cwnd so the cwnd wait passes immediately and we reach
        // packet_sending (which then fails on the dead socket).
        let congestion_controller = CongestionControlConfig::from_ledbat_config(LedbatConfig {
            initial_cwnd: 1_000_000,
            min_cwnd: 1_000_000,
            max_cwnd: 1_000_000_000,
            ..Default::default()
        })
        .build_arc();
        let sent_tracker = Arc::new(parking_lot::Mutex::new(SentPacketTracker::new()));
        let token_bucket = Arc::new(TokenBucket::new(1_000_000, 100_000_000));

        let cc_handle = congestion_controller.clone();
        let pipe_task = GlobalExecutor::spawn(pipe_stream(
            handle.into(),
            StreamId::next_transport_internal(),
            Arc::new(AtomicU32::new(0)),
            Arc::new(TestSocket::new(outbound_sender)),
            remote_addr,
            cipher,
            sent_tracker,
            token_bucket,
            congestion_controller,
            time_source,
            None,
            None,
        ));

        let result = pipe_task.await.expect("join error");
        assert!(
            result.is_err(),
            "pipe_stream should fail when the socket is closed: {result:?}"
        );
        assert_eq!(
            cc_handle.flightsize(),
            0,
            "pipe_stream mid-send failure must release the failed fragment's bytes (net 0)"
        );
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
            stream_id: StreamId::next(),
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
            stream_id: StreamId::next(),
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
            stream_id: StreamId::next(),
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
        let stream_id = StreamId::next_transport_internal();
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

        let cc_handle = congestion_controller.clone();
        let pipe_task = GlobalExecutor::spawn(pipe_stream(
            handle.into(),
            StreamId::next_transport_internal(),
            Arc::new(AtomicU32::new(0)),
            Arc::new(TestSocket::new(outbound_sender)),
            remote_addr,
            cipher,
            sent_tracker,
            token_bucket,
            congestion_controller,
            time_source,
            None,
            None,
        ));

        let result = pipe_task.await.expect("join error");
        assert!(
            matches!(result, Err(TransportError::OutboundStreamFailed(_))),
            "Expected OutboundStreamFailed after pipe_stream cwnd wait timeout, got: {:?}",
            result
        );
        // #4345: the abort must leave flight size released. Here a 1-byte cwnd
        // means no fragment ever went out, so this guards that the abort's
        // drop_stream is a clean no-op (0) rather than mis-releasing or panicking.
        assert_eq!(
            cc_handle.flightsize(),
            0,
            "pipe_stream cwnd-wait abort must leave flight size at 0"
        );

        Ok(())
    }

    /// Regression test for #4345 (relay-pipe inactivity-stall path,
    /// outbound_stream.rs:456): when the UPSTREAM inbound feed produces no
    /// fragment within STREAM_INACTIVITY_TIMEOUT, pipe_stream must fail only
    /// THIS stream — not tear down the DOWNSTREAM connection that carries other
    /// multiplexed ops. Before this PR the site returned ConnectionClosed
    /// (is_transient() == false → recv loop's fatal arm).
    ///
    /// Drives the stall by registering a StreamHandle with NO fragment ever
    /// pushed, so `stream.next()` stays Pending and the select!'s
    /// inactivity-timeout arm fires. Asserts the error is OutboundStreamFailed,
    /// is_transient() is true, and it is NOT ConnectionClosed.
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
        let stream_id = StreamId::next_transport_internal();
        let handle = StreamHandle::new(stream_id, 10_000);

        let congestion_controller = CongestionControlConfig::default().build_arc();
        let sent_tracker = Arc::new(parking_lot::Mutex::new(SentPacketTracker::new()));
        let token_bucket = Arc::new(TokenBucket::new(1_000_000, 100_000_000));

        let cc_handle = congestion_controller.clone();
        let pipe_task = GlobalExecutor::spawn(pipe_stream(
            handle.into(),
            StreamId::next_transport_internal(),
            Arc::new(AtomicU32::new(0)),
            Arc::new(TestSocket::new(outbound_sender)),
            remote_addr,
            cipher,
            sent_tracker,
            token_bucket,
            congestion_controller,
            time_source,
            None,
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
            err.is_transient(),
            "inactivity stall must be classified transient so the connection survives: {err:?}"
        );
        // #4345: the stall fires before any fragment is sent, so the abort's
        // drop_stream is a clean no-op and flight size stays 0.
        assert_eq!(
            cc_handle.flightsize(),
            0,
            "pipe_stream inactivity-stall abort must leave flight size at 0"
        );

        Ok(())
    }

    /// Regression test for #4345 (relay-pipe inbound-stream-error path,
    /// outbound_stream.rs:471): when the UPSTREAM inbound stream yields an
    /// error, pipe_stream must fail only THIS stream — not the DOWNSTREAM
    /// connection. Before this PR the site returned ConnectionClosed
    /// (is_transient() == false → recv loop's fatal arm).
    ///
    /// Drives the error by cancelling the StreamHandle so the inbound stream
    /// yields Err(StreamError::Cancelled). Asserts the error is
    /// OutboundStreamFailed, is_transient() is true, and it is NOT
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
        let stream_id = StreamId::next_transport_internal();
        let handle = StreamHandle::new(stream_id, 10_000);
        handle
            .push_fragment(1, Bytes::from(vec![0u8; 1000]))
            .unwrap();
        handle.cancel();

        let congestion_controller = CongestionControlConfig::default().build_arc();
        let sent_tracker = Arc::new(parking_lot::Mutex::new(SentPacketTracker::new()));
        let token_bucket = Arc::new(TokenBucket::new(1_000_000, 100_000_000));

        let cc_handle = congestion_controller.clone();
        let pipe_task = GlobalExecutor::spawn(pipe_stream(
            handle.into(),
            StreamId::next_transport_internal(),
            Arc::new(AtomicU32::new(0)),
            Arc::new(TestSocket::new(outbound_sender)),
            remote_addr,
            cipher,
            sent_tracker,
            token_bucket,
            congestion_controller,
            time_source,
            None,
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
            err.is_transient(),
            "inbound error must be classified transient so the connection survives: {err:?}"
        );
        // #4345: the inbound error fires before any fragment is sent, so the
        // abort's drop_stream is a clean no-op and flight size stays 0.
        assert_eq!(
            cc_handle.flightsize(),
            0,
            "pipe_stream inbound-error abort must leave flight size at 0"
        );

        Ok(())
    }

    /// Counts `on_fragment_sent` calls.
    #[derive(Default)]
    struct CountingObserver(std::sync::atomic::AtomicUsize);

    impl crate::transport::StreamProgressObserver for CountingObserver {
        fn on_fragment_sent(&self) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    impl CountingObserver {
        fn count(&self) -> usize {
            self.0.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    /// Congestion controller and token bucket large enough that a test stream
    /// never waits on either.
    fn unthrottled(
        time_source: &VirtualTime,
    ) -> (
        Arc<CongestionController<VirtualTime>>,
        Arc<TokenBucket<VirtualTime>>,
    ) {
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
        (congestion_controller, token_bucket)
    }

    /// `send_stream` reports progress once per fragment it hands to the
    /// socket: the operations layer's inactivity timeout relies on it.
    #[tokio::test]
    async fn send_stream_reports_progress_once_per_fragment()
    -> Result<(), Box<dyn std::error::Error>> {
        let (outbound_sender, mut outbound_receiver) = mpsc::channel(1);
        let time_source = VirtualTime::new();
        let (congestion_controller, token_bucket) = unthrottled(&time_source);
        let observer = Arc::new(CountingObserver::default());
        let task = GlobalExecutor::spawn(send_stream(
            StreamId::next_transport_internal(),
            Arc::new(AtomicU32::new(0)),
            Arc::new(TestSocket::new(outbound_sender)),
            SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 8080),
            Bytes::from(vec![7u8; 20_000]),
            random_cipher(),
            Arc::new(parking_lot::Mutex::new(
                SentPacketTracker::new_with_time_source(time_source.clone()),
            )),
            token_bucket,
            congestion_controller,
            time_source,
            None,
            None,
            Some(observer.clone() as Arc<dyn crate::transport::StreamProgressObserver>),
        ));
        let mut packets = 0;
        while outbound_receiver.recv().await.is_some() {
            packets += 1;
        }
        task.await?.expect("send_stream succeeds");
        assert!(
            packets > 1,
            "premise: a multi-fragment stream, got {packets}"
        );
        assert_eq!(observer.count(), packets);
        Ok(())
    }

    /// `pipe_stream` reports progress once per fragment it forwards.
    #[tokio::test]
    async fn pipe_stream_reports_progress_once_per_fragment()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::transport::peer_connection::streaming::StreamHandle;
        use crate::transport::peer_connection::streaming_buffer::FRAGMENT_PAYLOAD_SIZE;

        const FRAGMENTS: usize = 4;
        let handle = StreamHandle::new(
            StreamId::next_transport_internal(),
            (FRAGMENTS * FRAGMENT_PAYLOAD_SIZE) as u64,
        );
        for n in 1..=FRAGMENTS {
            handle.push_fragment(n as u32, Bytes::from(vec![7u8; FRAGMENT_PAYLOAD_SIZE]))?;
        }

        let (outbound_sender, mut outbound_receiver) = mpsc::channel(1);
        let time_source = VirtualTime::new();
        let (congestion_controller, token_bucket) = unthrottled(&time_source);
        let observer = Arc::new(CountingObserver::default());
        let task = GlobalExecutor::spawn(pipe_stream(
            handle.into(),
            StreamId::next_transport_internal(),
            Arc::new(AtomicU32::new(0)),
            Arc::new(TestSocket::new(outbound_sender)),
            SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 8080),
            random_cipher(),
            Arc::new(parking_lot::Mutex::new(
                SentPacketTracker::new_with_time_source(time_source.clone()),
            )),
            token_bucket,
            congestion_controller,
            time_source,
            None,
            Some(observer.clone() as Arc<dyn crate::transport::StreamProgressObserver>),
        ));
        let mut packets = 0;
        while outbound_receiver.recv().await.is_some() {
            packets += 1;
        }
        task.await?.expect("pipe_stream succeeds");
        assert_eq!(packets, FRAGMENTS, "premise: one packet per fragment");
        assert_eq!(observer.count(), FRAGMENTS);
        Ok(())
    }
}
