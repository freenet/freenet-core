use super::PacketId;
use super::StreamId;
use super::bbr::DeliveryRateToken;
#[cfg(test)]
use crate::simulation::RealTime;
use crate::simulation::TimeSource;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;

/// Entry for pending packet receipts:
/// (encrypted_payload, sent_time_nanos, delivery_token, plaintext_packet_size)
type PendingReceiptEntry = (Box<[u8]>, u64, Option<DeliveryRateToken>, usize);

/// Identifies which outbound stream (if any) owns a sent packet's bytes.
///
/// Flight size is a single connection-wide counter, but a packet's bytes are
/// only credited back to it on ACK or abandonment. When an outbound stream
/// aborts (e.g. a cwnd-wait timeout in `outbound_stream.rs`), its in-flight
/// fragments are still tracked here and pin flight size until each one ages out
/// via `MAX_PACKET_RETRANSMITS` (~6s) — starving every later stream on the
/// connection (issue #4345). Tagging each pending packet with its owning stream
/// lets [`SentPacketTracker::drop_stream`] release that stranded flight size
/// atomically when the stream aborts.
///
/// `Control` is the explicit "no owning stream" sentinel for handshake packets,
/// keep-alive NoOps, short messages, and any other non-stream traffic;
/// `drop_stream` never touches `Control` packets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PacketStream {
    /// The packet carries a fragment of the given outbound stream.
    Stream(StreamId),
    /// The packet is not owned by any stream (handshake, NoOp, short message).
    Control,
}

/// Result of processing received receipts: (acked_packets_info, loss_proportion)
/// Each acked packet contains: (rtt_option, bytes_acked, delivery_token)
type AckProcessingResult = (
    Vec<(Option<Duration>, usize, Option<DeliveryRateToken>)>,
    Option<f64>,
);

const NETWORK_DELAY_ALLOWANCE: Duration = Duration::from_millis(500);

/// We can wait up to 100ms to confirm a message was received, this allows us to batch
/// receipts together and send them in a single message.
#[cfg(test)]
pub(crate) const MAX_CONFIRMATION_DELAY: Duration = Duration::from_millis(100);
#[cfg(not(test))]
const MAX_CONFIRMATION_DELAY: Duration = Duration::from_millis(100);

/// If we don't get a receipt for a message within 500ms, we assume the message was lost and
/// resend it. This must be significantly higher than MAX_CONFIRMATION_DELAY (100ms) to
/// account for network delay
pub(super) const MESSAGE_CONFIRMATION_TIMEOUT: Duration = {
    let millis: u128 = MAX_CONFIRMATION_DELAY.as_millis() + NETWORK_DELAY_ALLOWANCE.as_millis();

    // Check for overflow
    if millis > u64::MAX as u128 {
        panic!("Value too large for u64");
    }

    // Safe to convert now
    Duration::from_millis(millis as u64)
};

/// Maximum RTO backoff multiplier (RFC 6298 Section 5.5).
/// With a minimum/base RTO of 500ms and a max effective RTO cap of 60s, a backoff of 512 is
/// sufficient to ensure we can reach the 60s limit; for higher base RTO values, the cap is
/// reached with smaller backoff multipliers.
const MAX_RTO_BACKOFF: u32 = 512;

/// Maximum number of times a single packet is retransmitted before it is
/// abandoned (declared permanently lost).
///
/// Without an upper bound, a packet whose ACKs never arrive (e.g. a stream
/// whose sender task already gave up on the `CWND_WAIT_TIMEOUT`, or a starved
/// reverse-ACK channel) is retransmitted forever. Its bytes then stay counted
/// in the congestion controller's flight size for the life of the connection,
/// pinning flight size at `cwnd` and stalling every subsequent stream on that
/// connection (issue #4345).
///
/// On abandon, `get_resend` returns [`ResendAction::Abandon`] instead of
/// `Resend`; the recv loop releases the bytes from flight size via
/// `CongestionControl::release_flightsize(len)`, and the packet is dropped from
/// tracking.
///
/// Wall-clock to abandonment depends on whether OTHER packets are still being
/// ACKed, because `rto_backoff` is tracker-wide and resets to 1 on any ACK
/// while this count is per-packet:
/// - **Partial loss (the common multi-fragment case):** other fragments keep
///   ACKing, so `rto_backoff` stays at 1 and the black-holed fragment retries
///   at the 500ms `MIN_RTO` floor — abandoned after ~6s (12 × 500ms). That is
///   just above `STREAM_INACTIVITY_TIMEOUT` (5s), i.e. the receiver has already
///   given up on the stream; abandoning frees the connection's flight size for
///   the next stream.
/// - **Fully dead link (no ACKs at all):** `rto_backoff` escalates (1→512,
///   capped at a 60s effective RTO), so 12 retransmits span ~6 minutes; here
///   the 120s connection idle timeout tears the connection down first, so the
///   abandon path mainly serves the partial-loss case.
///
/// A still-progressing transfer is never abandoned: any ACK for the stuck
/// packet resets its count, and a 12-consecutive-RTO black hole on an otherwise
/// healthy path is a genuinely dead fragment.
const MAX_PACKET_RETRANSMITS: u32 = 12;

/// Minimum RTO after RTT samples have been collected.
///
/// RFC 6298 recommends 1 second, but Linux TCP uses 200ms. We use 500ms because it must
/// exceed RTT + ACK_CHECK_INTERVAL (100ms) to avoid spurious timeouts. With intercontinental
/// RTTs of 150-200ms and 100ms ACK batching, effective round-trip time can be 300-400ms.
///
/// History: 200ms caused 935 timeouts in 10 seconds (v0.1.92 regression) on high-latency paths.
///
/// Note: Initial RTO (before any RTT samples) remains 1 second per RFC 6298 Section 2.1.
const MIN_RTO: Duration = Duration::from_millis(500);

/// Minimum TLP timeout (PTO - Probe Timeout).
///
/// TLP sends a probe packet before RTO expires to detect tail loss earlier.
/// PTO = max(2 * SRTT, MIN_TLP_TIMEOUT).
///
/// RFC 8985 suggests a minimum of 10ms to prevent excessive probing on very fast networks.
const MIN_TLP_TIMEOUT: Duration = Duration::from_millis(10);

/// Determines the accuracy/sensitivity of the packet loss estimate. A lower value will result
/// in a more accurate estimate, but it will take longer to converge to the true value.
const PACKET_LOSS_DECAY_FACTOR: f64 = 1.0 / 1000.0;

/// This struct is responsible for tracking packets that have been sent but not yet acknowledged.
/// It is also responsible for deciding when to resend packets that have not been acknowledged.
///
/// The caller must report when packets are sent and when receipts are received using the
/// `report_sent_packet` and `report_received_receipts` functions. The caller must also call
/// `get_resend` periodically to check if any packets need to be resent.
///
/// The expectation is that get_resend will be called as part of a loop that looks something like
/// this:
///
/// ```ignore
/// let mut sent_packet_tracker = todo!();
/// loop {
///   match sent_packet_tracker.get_resend() {
///      ResendAction::WaitUntil(wait_until) => {
///        sleep_until(wait_until).await;
///      }
///      ResendAction::Resend(packet_id, packet) => {
///       // Send packet and then call report_sent_packet again with the same packet_id.
///      }
///   }
/// }
/// ```
pub(super) struct SentPacketTracker<T: TimeSource> {
    /// The list of packets that have been sent but not yet acknowledged.
    ///
    /// Stores the encrypted payload for resend, the send time for RTT
    /// calculation, the delivery token for BBR, and the plaintext packet size
    /// that was added to congestion-controller flight size.
    pending_receipts: HashMap<PacketId, PendingReceiptEntry>,

    resend_queue: VecDeque<ResendQueueEntry>,

    packet_loss_proportion: f64,

    pub(super) time_source: T,

    // RTT estimation fields (RFC 6298)
    /// Smoothed Round-Trip Time
    srtt: Option<Duration>,
    /// RTT variance
    rttvar: Duration,
    /// Minimum observed RTT (useful for BBR-style algorithms later)
    min_rtt: Duration,
    /// Retransmission Timeout
    rto: Duration,

    /// Track retransmitted packets for Karn's algorithm
    /// (exclude retransmissions from RTT estimation)
    retransmitted_packets: HashSet<PacketId>,

    /// Total packets sent (for statistics)
    total_packets_sent: usize,

    /// RTO backoff multiplier for exponential backoff (RFC 6298 Section 5.5)
    /// Doubles on each consecutive timeout, resets to 1 on valid ACK
    rto_backoff: u32,

    /// Track packets that have had TLP probes sent.
    /// TLP fires once per packet before RTO - if no ACK, RTO handles it.
    tlp_sent_packets: HashSet<PacketId>,

    /// Per-packet RTO retransmission count, used to abandon a packet after
    /// `MAX_PACKET_RETRANSMITS` (issue #4345). Incremented on each RTO fire,
    /// cleared on ACK. An entry is removed when its packet is ACKed or
    /// abandoned, so this map tracks only currently-unacked packets.
    retransmit_counts: HashMap<PacketId, u32>,

    /// Owning stream for each currently-tracked packet (issue #4345).
    ///
    /// Indexed by `PacketId`. Unlike `pending_receipts`, an entry here is NOT
    /// removed when `get_resend` pops a packet for retransmission — it persists
    /// across resends so the re-registration in the recv loop preserves the
    /// packet's stream tag. An entry is removed only when the packet leaves
    /// tracking for good: on ACK (`report_received_receipts`), on abandonment
    /// (`get_resend` Abandon), or when its stream is dropped (`drop_stream`).
    ///
    /// This is what makes `drop_stream` double-decrement-safe: after it removes
    /// a stream's packets from BOTH this map and `pending_receipts`, no later
    /// ACK or abandon can find those packets, so their bytes are released
    /// exactly once.
    packet_streams: HashMap<PacketId, PacketStream>,
}

impl<T: TimeSource> SentPacketTracker<T> {
    /// Create a new SentPacketTracker with a custom time source.
    pub(super) fn new_with_time_source(time_source: T) -> Self {
        SentPacketTracker {
            pending_receipts: HashMap::new(),
            resend_queue: VecDeque::new(),
            packet_loss_proportion: 0.0,
            time_source,
            // RFC 6298: Initial RTO = 1 second
            srtt: None,
            rttvar: Duration::from_secs(0),
            min_rtt: Duration::from_millis(100), // Reasonable default until real RTT samples arrive
            rto: Duration::from_secs(1),
            retransmitted_packets: HashSet::new(),
            total_packets_sent: 0,
            rto_backoff: 1, // No backoff initially
            tlp_sent_packets: HashSet::new(),
            retransmit_counts: HashMap::new(),
            packet_streams: HashMap::new(),
        }
    }

    /// Calculate PTO (Probe Timeout) for TLP.
    /// PTO = max(2 * SRTT, MIN_TLP_TIMEOUT)
    /// Returns None if no RTT samples yet (TLP disabled).
    fn probe_timeout(&self) -> Option<Duration> {
        self.srtt.map(|srtt| {
            let pto = srtt.saturating_mul(2);
            pto.max(MIN_TLP_TIMEOUT)
        })
    }
}

impl<T: TimeSource> SentPacketTracker<T> {
    pub(super) fn report_sent_packet(&mut self, packet_id: PacketId, payload: Box<[u8]>) {
        let packet_size = payload.len();
        self.report_sent_packet_with_token_and_size(packet_id, payload, None, packet_size);
    }

    /// Report a sent packet with an optional BBR delivery rate token and an
    /// explicit plaintext flight-size byte count.
    ///
    /// The token is used by BBR for accurate delivery rate estimation. When an ACK
    /// arrives, the token is returned alongside the RTT sample so it can be passed
    /// to `BbrController::on_ack_with_token`.
    ///
    /// The packet's stream tag is **preserved if already known** and otherwise
    /// defaults to [`PacketStream::Control`]. This is exactly the behaviour the
    /// recv loop's resend re-registration needs: `get_resend` removes the packet
    /// from `pending_receipts` but NOT from `packet_streams`, so re-sending a
    /// stream fragment through this method keeps its original stream tag. A
    /// genuinely new packet (monotonic `packet_id`, never colliding with an old
    /// tag) has no entry yet and is correctly tagged `Control`. First sends of
    /// stream fragments must use [`Self::report_sent_stream_packet_with_size`]
    /// instead so they are tagged with their owning stream.
    pub(super) fn report_sent_packet_with_token_and_size(
        &mut self,
        packet_id: PacketId,
        payload: Box<[u8]>,
        token: Option<DeliveryRateToken>,
        packet_size: usize,
    ) {
        let stream = self
            .packet_streams
            .get(&packet_id)
            .copied()
            .unwrap_or(PacketStream::Control);
        self.report_sent_packet_inner(packet_id, payload, token, stream, packet_size);
    }

    /// Report a sent packet that carries bytes owned by `stream`.
    ///
    /// Used for the first send of a `StreamFragment`. The packet is tagged with
    /// its owning stream so that, when the stream aborts, [`Self::drop_stream`]
    /// can release the packet's flight-size bytes atomically (issue #4345).
    /// Resends of the same packet re-register through
    /// [`Self::refresh_sent_packet`], which preserves this tag.
    #[cfg(test)]
    pub(super) fn report_sent_stream_packet(
        &mut self,
        packet_id: PacketId,
        payload: Box<[u8]>,
        token: Option<DeliveryRateToken>,
        stream_id: StreamId,
    ) {
        let packet_size = payload.len();
        self.report_sent_stream_packet_with_size(packet_id, payload, token, stream_id, packet_size);
    }

    /// Report a sent stream packet with an explicit plaintext flight-size byte
    /// count.
    pub(super) fn report_sent_stream_packet_with_size(
        &mut self,
        packet_id: PacketId,
        payload: Box<[u8]>,
        token: Option<DeliveryRateToken>,
        stream_id: StreamId,
        packet_size: usize,
    ) {
        self.report_sent_packet_inner(
            packet_id,
            payload,
            token,
            PacketStream::Stream(stream_id),
            packet_size,
        );
    }

    /// Refresh an already-tracked packet's payload / send-time / token in place
    /// after a resend, WITHOUT resurrecting it if it has been removed.
    ///
    /// Returns `true` if the packet was still tracked and was refreshed, `false`
    /// if it had already left `pending_receipts` (ACKed, abandoned, or dropped by
    /// [`Self::drop_stream`]) — in which case this is a no-op and `payload` is
    /// dropped.
    ///
    /// # Why this exists (issue #4345 — resurrection-safety)
    ///
    /// The recv loop's resend cycle releases the tracker lock across the UDP
    /// `send_to().await`, then re-registers the packet. A concurrent
    /// `drop_stream` (from the spawned outbound-stream abort task) can remove the
    /// packet in that window and release its bytes. If re-registration went
    /// through an insert-on-absent path (`report_sent_packet_*`), it would
    /// RESURRECT the packet as a `Control`-tagged zombie; a later ACK/abandon of
    /// that zombie would then release its bytes a SECOND time (flight-size
    /// under-count) and violate the "in `pending_receipts` iff in flight"
    /// invariant. This method only ever UPDATES an existing entry, never inserts,
    /// so a dropped packet stays dropped and its bytes are released exactly once.
    ///
    /// The recv loop MUST use this (not `report_sent_packet*`) for resend
    /// re-registration. `report_sent_packet*` remain insert-capable for first
    /// sends (and for test re-registration, which never races a `drop_stream`).
    pub(super) fn refresh_sent_packet(
        &mut self,
        packet_id: PacketId,
        payload: Box<[u8]>,
        token: Option<DeliveryRateToken>,
    ) -> bool {
        let sent_time_nanos = self.time_source.now_nanos();
        match self.pending_receipts.get_mut(&packet_id) {
            Some(slot) => {
                // In-place refresh. The packet is still tracked, still queued for
                // resend (get_resend re-queued it), and its stream tag in
                // `packet_streams` is untouched — so it is preserved across the
                // resend. `payload` is the same bytes the caller just sent; the
                // plaintext byte count is preserved because no new flight-size
                // bytes are added on resend.
                let packet_size = slot.3;
                *slot = (payload, sent_time_nanos, token, packet_size);
                true
            }
            None => {
                // Already removed (ACK / Abandon / drop_stream). Do NOT
                // re-insert — that would resurrect a dropped packet and enable a
                // double-release. Drop the payload and report the no-op.
                false
            }
        }
    }

    fn report_sent_packet_inner(
        &mut self,
        packet_id: PacketId,
        payload: Box<[u8]>,
        token: Option<DeliveryRateToken>,
        stream: PacketStream,
        packet_size: usize,
    ) {
        let sent_time_nanos = self.time_source.now_nanos();

        // NOTE (issue #4345 resurrection-safety): production resend
        // re-registration does NOT come through here — the recv loop uses
        // `refresh_sent_packet`, which never inserts, so a packet removed by a
        // concurrent `drop_stream` cannot be resurrected. This insert-or-refresh
        // path serves first sends (packet_sending / handshake) and test
        // re-registration (which never races a `drop_stream`). The refresh
        // branch keeps an already-present re-register idempotent (no duplicate
        // resend-queue entry, no `total_packets_sent` bump, stream tag
        // preserved); the `stream` argument is ignored when the entry exists.
        // Preserve the original plaintext byte count too, because flight size
        // was added only on the first send.
        if let Some(slot) = self.pending_receipts.get_mut(&packet_id) {
            let packet_size = slot.3;
            *slot = (payload, sent_time_nanos, token, packet_size);
            return;
        }

        self.pending_receipts
            .insert(packet_id, (payload, sent_time_nanos, token, packet_size));
        self.packet_streams.insert(packet_id, stream);
        self.resend_queue.push_back(ResendQueueEntry { packet_id });
        self.total_packets_sent += 1;
    }

    /// Remove every currently-tracked packet owned by `stream_id` and return the
    /// total payload byte count removed (issue #4345).
    ///
    /// Called when an outbound stream aborts (e.g. a cwnd-wait timeout) so the
    /// caller can release the aborted stream's stranded bytes from the
    /// congestion controller's flight size in one shot, instead of waiting for
    /// each fragment to age out via `MAX_PACKET_RETRANSMITS` (~6s) or be ACKed.
    /// The returned byte count is exactly what the caller must pass to
    /// `CongestionControl::release_flightsize`.
    ///
    /// # Double-decrement safety
    ///
    /// This removes the stream's packets from **all** tracking structures —
    /// `pending_receipts`, `packet_streams`, `retransmit_counts`,
    /// `retransmitted_packets`, `tlp_sent_packets`, and the `resend_queue`. Once
    /// removed, those packets cannot be released a second time by any other
    /// path:
    ///
    /// - A subsequent ACK in `report_received_receipts` finds no entry in
    ///   `pending_receipts`, so it produces no ack-info tuple and the caller
    ///   never subtracts its bytes again.
    /// - `get_resend` skips packet ids absent from `pending_receipts`, so it can
    ///   never emit a `Resend`/`TlpProbe`/`Abandon` for a dropped packet — in
    ///   particular it cannot `Abandon` it and trigger a second
    ///   `release_flightsize`.
    ///
    /// The byte count is therefore released exactly once: here.
    ///
    /// # Why the returned count is the stored plaintext packet size
    ///
    /// `pending_receipts` keeps both the encrypted on-wire payload (for resend)
    /// and the plaintext packet size that `on_send`/`on_send_with_token` added
    /// to flight size. `drop_stream` returns the stored plaintext size, not
    /// `payload.len()`, so the release exactly matches the earlier add.
    ///
    /// This is the same byte count the other two release paths use:
    ///
    /// - ACK: `report_received_receipts` returns the stored plaintext size and
    ///   the recv loop passes it to `on_ack*`.
    /// - Abandon: `ResendAction::Abandon { payload_len }` is the stored
    ///   plaintext size and the recv loop passes it to `release_flightsize`.
    ///
    /// So `drop_stream` releases exactly what an ACK or Abandon for those same
    /// packets would have released — it substitutes for the ACKs that will never
    /// arrive. All three release paths must remain on the same byte-count
    /// convention as `on_send`, or flight-size accounting will drift
    /// (issue #4402).
    ///
    /// Returns `0` for an unknown or already-drained stream, and never panics.
    ///
    /// Wired into the outbound-stream abort paths (`outbound_stream.rs`,
    /// `release_aborted_stream_flightsize`): on a cwnd-wait timeout, upstream
    /// stall/error, or mid-send failure, the abort calls this and passes the
    /// returned count to `CongestionControl::release_flightsize` so the aborted
    /// stream's stranded bytes are freed immediately instead of waiting out
    /// `MAX_PACKET_RETRANSMITS`.
    pub(super) fn drop_stream(&mut self, stream_id: StreamId) -> u64 {
        let target = PacketStream::Stream(stream_id);

        // Identify the owned packet ids first (one pass over the small
        // per-connection stream map), then remove from every structure.
        let owned: Vec<PacketId> = self
            .packet_streams
            .iter()
            .filter_map(|(&pid, &stream)| (stream == target).then_some(pid))
            .collect();

        if owned.is_empty() {
            return 0;
        }

        let mut released_bytes: u64 = 0;
        for pid in &owned {
            // Sum the payload bytes still pending (i.e. counted in flight size).
            // A packet whose tag is present but that is absent from
            // `pending_receipts` cannot occur: the tag is removed in lockstep on
            // ACK and abandon. The `if let` is a defensive no-panic guard.
            if let Some((_, _, _, packet_size)) = self.pending_receipts.remove(pid) {
                released_bytes = released_bytes.saturating_add(packet_size as u64);
            }
            self.packet_streams.remove(pid);
            self.retransmit_counts.remove(pid);
            self.retransmitted_packets.remove(pid);
            self.tlp_sent_packets.remove(pid);
        }

        // Drop the dropped packets' resend-queue entries. `get_resend` already
        // skips ids missing from `pending_receipts`, so this is not strictly
        // required for correctness, but it keeps the queue from growing with
        // dead entries across many stream drops.
        let dropped: HashSet<PacketId> = owned.into_iter().collect();
        self.resend_queue
            .retain(|entry| !dropped.contains(&entry.packet_id));

        released_bytes
    }

    /// Whether `packet_id` is currently tracked (present in `pending_receipts`,
    /// i.e. in flight). Used by the mid-send-failure abort sites to
    /// `debug_assert` that a packet whose send failed was never registered
    /// before they explicitly release its `on_send` bytes (issue #4345) — which
    /// would otherwise double-release if a future multi-packet send path
    /// registered the packet and then failed.
    pub(super) fn contains_packet(&self, packet_id: PacketId) -> bool {
        self.pending_receipts.contains_key(&packet_id)
    }

    /// Reports that receipts have been received for the given packet IDs.
    /// Returns: ((RTT sample option, packet size, delivery token) tuples, loss rate)
    ///
    /// RTT sample is Some for non-retransmitted packets (used for RTT estimation),
    /// None for retransmitted packets (Karn's algorithm - don't use for RTT).
    /// Packet size is ALWAYS returned so congestion controller can decrement flightsize.
    /// Delivery token is returned for BBR's accurate delivery rate estimation.
    pub(super) fn report_received_receipts(
        &mut self,
        packet_ids: &[PacketId],
    ) -> AckProcessingResult {
        let now_nanos = self.time_source.now_nanos();
        let mut ack_info = Vec::new();

        for packet_id in packet_ids {
            // Update loss proportion (ACK received = no loss)
            self.packet_loss_proportion = self.packet_loss_proportion
                * (1.0 - PACKET_LOSS_DECAY_FACTOR)
                + (PACKET_LOSS_DECAY_FACTOR * 0.0);

            // Get packet info before removing
            let is_retransmitted = self.retransmitted_packets.contains(packet_id);

            if let Some((_, sent_time_nanos, token, packet_size)) =
                self.pending_receipts.get(packet_id)
            {
                let packet_size = *packet_size;
                let token = *token; // Copy the token before removing

                if is_retransmitted {
                    // Retransmitted packet: return size but no RTT (Karn's algorithm)
                    // Note: token is still passed for BBR flightsize tracking
                    ack_info.push((None, packet_size, token));

                    // Full reset of backoff on retransmit ACKs (matching libutp behavior).
                    //
                    // Rationale: Receiving any ACK indicates the network is functioning, even
                    // if we can't use it for RTT estimation per Karn's algorithm. Karn's
                    // algorithm only addresses RTT measurement ambiguity, not backoff recovery.
                    //
                    // Prior art justification:
                    // - libutp (BitTorrent's canonical LEDBAT): full reset on any ACK
                    // - Linux TCP: full reset on any ACK
                    // - picotcp issue #69: identical death spiral bug, fixed with full reset
                    //
                    // The previous halving approach was more conservative than necessary.
                    // libutp has been battle-tested across millions of clients for 10+ years
                    // with full reset. The protection against congestion collapse comes from
                    // the timeout doubling itself, not from delayed recovery on ACK.
                    self.rto_backoff = 1;
                } else {
                    // Non-retransmitted: calculate RTT and update estimation
                    let rtt_nanos = now_nanos.saturating_sub(*sent_time_nanos);
                    let rtt_sample = Duration::from_nanos(rtt_nanos);
                    ack_info.push((Some(rtt_sample), packet_size, token));
                    self.update_rtt(rtt_sample);

                    // RFC 6298 Section 5.7: Reset backoff on valid ACK
                    // (only for non-retransmitted packets to be safe)
                    self.rto_backoff = 1;
                }
            }

            // Remove from pending, retransmitted, TLP, retransmit-count, and
            // stream tracking. Dropping the stream tag here is what keeps
            // `drop_stream` double-decrement-safe: once a packet is ACKed it is
            // no longer associated with any stream, so a later `drop_stream`
            // for that stream cannot release its bytes a second time (#4345).
            self.pending_receipts.remove(packet_id);
            self.retransmitted_packets.remove(packet_id);
            self.tlp_sent_packets.remove(packet_id);
            self.retransmit_counts.remove(packet_id);
            self.packet_streams.remove(packet_id);
        }

        let loss_rate = if self.total_packets_sent > 0 {
            Some(self.packet_loss_proportion)
        } else {
            None
        };

        (ack_info, loss_rate)
    }

    /// Updates RTT estimation based on a new sample using RFC 6298 algorithm
    fn update_rtt(&mut self, sample: Duration) {
        // RFC 6298 constants
        const ALPHA: f64 = 1.0 / 8.0; // Smoothing factor for SRTT
        const BETA: f64 = 1.0 / 4.0; // Smoothing factor for RTTVAR
        const K: u32 = 4; // RTO variance multiplier
        const G: Duration = Duration::from_millis(10); // Clock granularity

        match self.srtt {
            None => {
                // First RTT sample (RFC 6298 Section 2.2)
                self.srtt = Some(sample);
                self.rttvar = sample / 2;
                self.min_rtt = sample;
            }
            Some(srtt) => {
                // Subsequent RTT samples (RFC 6298 Section 2.3)
                // RTTVAR = (1 - BETA) * RTTVAR + BETA * |SRTT - R|
                let abs_diff = sample.abs_diff(srtt);
                self.rttvar = self.rttvar.mul_f64(1.0 - BETA) + abs_diff.mul_f64(BETA);

                // SRTT = (1 - ALPHA) * SRTT + ALPHA * R
                self.srtt = Some(srtt.mul_f64(1.0 - ALPHA) + sample.mul_f64(ALPHA));

                // Track minimum RTT
                self.min_rtt = self.min_rtt.min(sample);
            }
        }

        // RTO = SRTT + max(G, K * RTTVAR) (RFC 6298 Section 2.3)
        let rto_variance = self.rttvar * K;
        let max_variance = if rto_variance > G { rto_variance } else { G };
        self.rto = self.srtt.unwrap_or(Duration::from_secs(1)) + max_variance;

        // Clamp RTO to [500ms, 60s]
        // Note: RFC 6298 Section 2.4 recommends 1s minimum, but we use 500ms to
        // balance fast loss detection with ACK batching delay (ACK_CHECK_INTERVAL=100ms).
        if self.rto < MIN_RTO {
            self.rto = MIN_RTO;
        } else if self.rto > Duration::from_secs(60) {
            self.rto = Duration::from_secs(60);
        }
    }

    /// Mark a packet as retransmitted (for Karn's algorithm)
    pub(super) fn mark_retransmitted(&mut self, packet_id: PacketId) {
        self.retransmitted_packets.insert(packet_id);
    }

    /// Get the smoothed RTT estimate
    #[allow(dead_code)] // Used in tests, may be useful for debugging
    pub(super) fn smoothed_rtt(&self) -> Option<Duration> {
        self.srtt
    }

    /// Get the minimum observed RTT
    pub(super) fn min_rtt(&self) -> Duration {
        self.min_rtt
    }

    /// Get the base retransmission timeout (without backoff)
    #[allow(dead_code)] // Used in tests, may be useful for debugging
    pub(super) fn rto(&self) -> Duration {
        self.rto
    }

    /// Get the effective RTO with exponential backoff applied
    /// This is the actual timeout to use for retransmissions
    pub(super) fn effective_rto(&self) -> Duration {
        let backed_off = self.rto.saturating_mul(self.rto_backoff);
        // Cap at 60 seconds (RFC 6298 Section 2.5)
        backed_off.min(Duration::from_secs(60))
    }

    /// Called on retransmission timeout - doubles the RTO backoff (RFC 6298 Section 5.5)
    ///
    /// This implements the "back off the timer" requirement from RFC 6298:
    /// > (5.5) The host MUST set RTO <- RTO * 2 ("back off the timer")
    ///
    /// The backoff is reset when a valid ACK is received (in report_received_receipts).
    ///
    /// Design note: We use a separate backoff multiplier rather than modifying RTO directly.
    /// This preserves the base RTO for faster recovery when RTT improves, while still
    /// achieving RFC-compliant exponential backoff behavior.
    pub(super) fn on_timeout(&mut self) {
        // Double the backoff, capped at MAX_RTO_BACKOFF
        // With base RTO of 1s, 6 doublings (1→2→4→8→16→32→64) reach the 60s effective cap
        self.rto_backoff = (self.rto_backoff.saturating_mul(2)).min(MAX_RTO_BACKOFF);

        tracing::debug!(
            rto_backoff = self.rto_backoff,
            effective_rto_ms = self.effective_rto().as_millis(),
            "RTO backoff increased on timeout"
        );
    }

    /// Get the current backoff multiplier (for testing/debugging)
    #[allow(dead_code)]
    pub(super) fn rto_backoff(&self) -> u32 {
        self.rto_backoff
    }

    /// Either get a packet that needs to be resent, or how long the caller should wait until
    /// calling this function again.
    ///
    /// On `Resend`/`TlpProbe` the packet **stays in `pending_receipts`** — the
    /// returned `Box<[u8]>` is a clone of the still-tracked payload, and the
    /// packet's RTO timer is refreshed to "now" in place. The caller sends the
    /// bytes; it MAY call `report_sent_packet` again with the same packet_id to
    /// refresh the send timestamp to the post-`send_to` instant, but this is an
    /// idempotent in-place refresh, not a re-insert (issue #4345).
    ///
    /// WHY keep the packet across resend (issue #4345): `drop_stream` runs
    /// concurrently from the spawned outbound-stream abort path while the
    /// per-connection recv loop drives resends. If `get_resend` removed the
    /// packet from `pending_receipts` and relied on the caller to re-insert it
    /// after an `await`ed UDP send, a `drop_stream` landing in that gap would
    /// see no `pending_receipts` entry (release 0 bytes for a genuinely in-flight
    /// packet) and strip its stream tag — leaving the fragment's bytes pinned in
    /// flight size, the exact #4345 symptom. Keeping the entry makes the
    /// invariant total: a packet is in `pending_receipts` iff it is in flight, so
    /// `drop_stream` always sees and releases it. This is the same keep-the-entry
    /// shape the TLP branch below already uses.
    ///
    /// NOTE: this does NOT touch flight size. Flight size lives in the congestion
    /// controller; keeping the packet here is pure resend bookkeeping and does
    /// not reintroduce the forbidden decrement-on-RTO + re-add-on-resend pair
    /// (see `.claude/rules/transport.md` flight-size release invariant).
    ///
    /// TLP (Tail Loss Probe) fires before RTO to detect tail loss earlier.
    /// - TLP returns TlpProbe action (no backoff applied, speculative)
    /// - RTO returns Resend action (backoff applied, definite loss)
    pub(super) fn get_resend(&mut self) -> ResendAction {
        let now_nanos = self.time_source.now_nanos();
        let pto = self.probe_timeout();
        let effective_rto_nanos = self.effective_rto().as_nanos() as u64;

        while let Some(entry) = self.resend_queue.pop_front() {
            // Skip packets that have been ACKed
            if !self.pending_receipts.contains_key(&entry.packet_id) {
                continue;
            }

            // Get the packet's sent time - this may have been updated by re-registration
            let sent_time_nanos = self
                .pending_receipts
                .get(&entry.packet_id)
                .map(|(_, ts, _, _)| *ts)
                .unwrap_or(0);

            // Calculate RTO deadline from current sent_time (not stored value)
            // This handles re-registration after TLP correctly
            let rto_deadline = sent_time_nanos + effective_rto_nanos;

            // Calculate TLP deadline (if TLP is enabled and not already sent)
            let tlp_deadline = if let Some(pto) = pto {
                if !self.tlp_sent_packets.contains(&entry.packet_id) {
                    Some(sent_time_nanos + pto.as_nanos() as u64)
                } else {
                    None
                }
            } else {
                None
            };

            // Check if TLP should fire (before RTO)
            if let Some(tlp_at) = tlp_deadline {
                if now_nanos >= tlp_at && now_nanos < rto_deadline {
                    // TLP fires! Clone packet data for the probe
                    if let Some((packet, _, _, _)) = self.pending_receipts.get(&entry.packet_id) {
                        let packet_clone = packet.clone();
                        let packet_id = entry.packet_id;

                        // Mark TLP as sent for this packet (won't fire again)
                        self.tlp_sent_packets.insert(packet_id);

                        // Put entry back - RTO still pending
                        self.resend_queue.push_front(entry);

                        // Mark as retransmitted for Karn's algorithm
                        self.mark_retransmitted(packet_id);

                        // TLP does NOT apply backoff - it's speculative
                        return ResendAction::TlpProbe(packet_id, packet_clone);
                    }
                }
            }

            // Check if RTO should fire (only for a still-tracked packet —
            // get_resend keeps resent packets in pending_receipts since #4345).
            if now_nanos >= rto_deadline && self.pending_receipts.contains_key(&entry.packet_id) {
                // Update packet loss proportion for a lost packet
                self.packet_loss_proportion = self.packet_loss_proportion
                    * (1.0 - PACKET_LOSS_DECAY_FACTOR)
                    + PACKET_LOSS_DECAY_FACTOR;

                // Count this RTO. After MAX_PACKET_RETRANSMITS with no ACK,
                // abandon the packet so its bytes are released from flight
                // size instead of being retransmitted forever (issue #4345).
                let count = self.retransmit_counts.entry(entry.packet_id).or_insert(0);
                *count += 1;
                if *count > MAX_PACKET_RETRANSMITS {
                    // Abandonment is terminal: NOW remove the packet from
                    // every tracking structure (including pending_receipts).
                    // It will NOT be re-queued, so the recv loop stops
                    // retransmitting it. Removing the stream tag here keeps
                    // `drop_stream` double-decrement-safe: an abandoned
                    // packet's bytes are released once (by the recv loop, via
                    // the Abandon action), so a later `drop_stream` must not
                    // release them again (#4345).
                    let payload_len = self
                        .pending_receipts
                        .remove(&entry.packet_id)
                        .map(|(_, _, _, packet_size)| packet_size)
                        .unwrap_or(0);
                    self.retransmit_counts.remove(&entry.packet_id);
                    self.retransmitted_packets.remove(&entry.packet_id);
                    self.tlp_sent_packets.remove(&entry.packet_id);
                    self.packet_streams.remove(&entry.packet_id);
                    tracing::debug!(
                        packet_id = entry.packet_id,
                        retransmits = MAX_PACKET_RETRANSMITS,
                        payload_len,
                        "Abandoning packet after max retransmits — releasing flight size (#4345)"
                    );
                    return ResendAction::Abandon {
                        packet_id: entry.packet_id,
                        payload_len,
                    };
                }

                // Resend: KEEP the packet in pending_receipts (issue #4345 —
                // see the rustdoc above). Clone the payload for the caller,
                // refresh the send timestamp to now so the next RTO is
                // measured from this resend, and push the entry back so the
                // packet stays tracked for the next timeout.
                let packet = {
                    let slot = self
                        .pending_receipts
                        .get_mut(&entry.packet_id)
                        .expect("checked contains_key above");
                    slot.1 = now_nanos;
                    slot.0.clone()
                };
                self.resend_queue.push_back(ResendQueueEntry {
                    packet_id: entry.packet_id,
                });

                // Mark as retransmitted for Karn's algorithm
                self.mark_retransmitted(entry.packet_id);

                // RFC 6298 Section 5.5: Back off the timer on retransmission
                self.on_timeout();

                // Clean up TLP tracking for this packet
                self.tlp_sent_packets.remove(&entry.packet_id);

                return ResendAction::Resend(entry.packet_id, packet);
            }

            // Neither TLP nor RTO fired yet - calculate when to wake up
            let next_deadline = if let Some(tlp_at) = tlp_deadline {
                tlp_at.min(rto_deadline)
            } else {
                rto_deadline
            };

            self.resend_queue.push_front(entry);
            return ResendAction::WaitUntil(next_deadline);
        }

        // No pending packets - use effective RTO for next check
        let deadline_nanos = self.time_source.now_nanos() + self.effective_rto().as_nanos() as u64;
        ResendAction::WaitUntil(deadline_nanos)
    }
}

#[derive(Debug, PartialEq)]
pub enum ResendAction {
    /// Wait until the given deadline (nanoseconds since TimeSource epoch) before checking again.
    WaitUntil(u64),
    /// Full RTO timeout - resend the packet and apply backoff
    Resend(u32, Box<[u8]>),
    /// TLP (Tail Loss Probe) - send a probe to detect tail loss earlier than RTO.
    /// Unlike Resend, this doesn't apply backoff since it's speculative.
    TlpProbe(u32, Box<[u8]>),
    /// The packet has been retransmitted `MAX_PACKET_RETRANSMITS` times without
    /// an ACK and is declared permanently lost (issue #4345). It has been
    /// removed from tracking; the caller MUST release its `payload_len`
    /// plaintext bytes from the congestion controller's flight size via
    /// `CongestionControl::release_flightsize(payload_len)` and MUST NOT
    /// re-register or re-send it.
    Abandon { packet_id: u32, payload_len: usize },
}

struct ResendQueueEntry {
    packet_id: u32,
}

// Production constructor (backward-compatible, uses real time)
#[cfg(test)]
impl SentPacketTracker<RealTime> {
    /// Create a new SentPacketTracker with real time.
    ///
    /// This is the default constructor used in production code, matching TokenBucket::new().
    /// For testing with virtual time, use `new_with_time_source(virtual_time_instance)`.
    pub(super) fn new() -> Self {
        Self::new_with_time_source(RealTime::new())
    }
}

// Unit tests
#[cfg(test)]
pub(in crate::transport) mod tests {
    // These tests assert that `get_resend()` returns a specific `ResendAction`
    // variant and `panic!` on anything else (`match action { Expected => ..,
    // _ => panic!() }`). That wildcard-with-panic is the intended assertion
    // idiom — expanding it into a `pat | _` listing would only add churn and
    // would still need updating for any new variant. Scope the allow to the
    // test module rather than annotating each identical arm. Mirrors the
    // documented non_exhaustive wildcard pattern in
    // `.claude/rules/git-workflow.md`.
    #![allow(clippy::wildcard_enum_match_arm)]

    use super::*;
    use crate::simulation::VirtualTime;
    use rstest::rstest;

    pub(in crate::transport) fn mock_sent_packet_tracker() -> SentPacketTracker<VirtualTime> {
        let time_source = VirtualTime::new();
        SentPacketTracker::new_with_time_source(time_source)
    }

    #[rstest]
    #[case::single_packet(1, vec![1, 2, 3], 1, 1, 1)]
    #[case::multiple_packets_first(1, vec![1], 1, 1, 1)]
    fn test_report_sent_packet(
        #[case] packet_id: PacketId,
        #[case] payload: Vec<u8>,
        #[case] expected_pending: usize,
        #[case] expected_queue: usize,
        #[case] expected_total_sent: usize,
    ) {
        let mut tracker = mock_sent_packet_tracker();
        tracker.report_sent_packet(packet_id, payload.into());
        assert_eq!(tracker.pending_receipts.len(), expected_pending);
        assert_eq!(tracker.resend_queue.len(), expected_queue);
        assert_eq!(tracker.packet_loss_proportion, 0.0);
        assert_eq!(tracker.total_packets_sent, expected_total_sent);
    }

    #[test]
    fn test_report_received_receipts() {
        let mut tracker = mock_sent_packet_tracker();
        tracker.report_sent_packet(1, vec![1, 2, 3].into());
        let (ack_info, loss_rate) = tracker.report_received_receipts(&[1]);
        assert_eq!(tracker.pending_receipts.len(), 0);
        assert!(tracker.resend_queue.len() <= 1);
        assert_eq!(tracker.packet_loss_proportion, 0.0);
        assert_eq!(ack_info.len(), 1); // Should have one ACK entry
        assert!(ack_info[0].0.is_some()); // Non-retransmitted, so RTT should be Some
        assert_eq!(ack_info[0].1, 3); // Packet size
        assert!(loss_rate.is_some()); // Should have loss rate
    }

    #[test]
    fn test_packet_lost() {
        let mut tracker = mock_sent_packet_tracker();
        tracker.report_sent_packet(1, vec![1, 2, 3].into());
        // Packets now use effective_rto() which is 1s initially (not MESSAGE_CONFIRMATION_TIMEOUT)
        tracker.time_source.advance(tracker.effective_rto());
        let resend_action = tracker.get_resend();
        assert_eq!(resend_action, ResendAction::Resend(1, vec![1, 2, 3].into()));
        // Since #4345, a resent packet STAYS tracked (get_resend keeps it in
        // pending_receipts and re-queues it) so it remains visible to a
        // concurrent drop_stream; it is no longer removed on Resend. The payload
        // is returned as a clone.
        assert_eq!(tracker.pending_receipts.len(), 1);
        assert_eq!(tracker.resend_queue.len(), 1);
        assert_eq!(tracker.packet_loss_proportion, PACKET_LOSS_DECAY_FACTOR);
    }

    #[test]
    fn test_immediate_receipt_then_resend() {
        let mut tracker = mock_sent_packet_tracker();

        // Report two packets sent
        tracker.report_sent_packet(1, vec![1, 2, 3].into());
        tracker.report_sent_packet(2, vec![4, 5, 6].into());

        // Immediately report receipt for the first packet
        // This establishes SRTT ~ 0, so PTO = max(2*0, 10ms) = 10ms
        let _ = tracker.report_received_receipts(&[1]);

        // Simulate time just before TLP timeout (PTO = 10ms)
        tracker.time_source.advance(Duration::from_millis(9));

        // This should not trigger a resend yet (TLP fires at 10ms)
        match tracker.get_resend() {
            ResendAction::WaitUntil(_) => (),
            ResendAction::Resend(..)
            | ResendAction::TlpProbe(..)
            | ResendAction::Abandon { .. } => {
                panic!("Expected WaitUntil, got Resend/TlpProbe too early")
            }
        }

        // Now advance time to trigger TLP for packet 2
        tracker.time_source.advance(Duration::from_millis(2));

        // This should now trigger a TLP probe for packet 2
        match tracker.get_resend() {
            ResendAction::TlpProbe(packet_id, _) => {
                assert_eq!(packet_id, 2)
            }
            ResendAction::Resend(_, _) => panic!("Expected TlpProbe, got Resend"),
            ResendAction::WaitUntil(_) => panic!("Expected TlpProbe, got WaitUntil"),
            ResendAction::Abandon { .. } => panic!("Expected TlpProbe, got Abandon"),
        }
    }

    #[test]
    fn test_get_resend_with_pending_receipts() {
        let mut tracker = mock_sent_packet_tracker();

        tracker.report_sent_packet(0, Box::from(&[][..]));

        tracker.time_source.advance(Duration::from_millis(10));

        // Report second packet
        tracker.report_sent_packet(1, Box::from(&[][..]));

        // Acknowledge receipt of the first packet
        let _ = tracker.report_received_receipts(&[0]);

        // The next call to get_resend should calculate the wait time based on the second packet (id 1)
        match tracker.get_resend() {
            ResendAction::WaitUntil(wait_until_nanos) => {
                // With virtual time, the deadline should be in the future
                let now_nanos = tracker.time_source.now_nanos();
                assert!(
                    wait_until_nanos >= now_nanos,
                    "Wait deadline should be in the future"
                );
            }
            ResendAction::Resend(..)
            | ResendAction::TlpProbe(..)
            | ResendAction::Abandon { .. } => {
                panic!("Expected ResendAction::WaitUntil")
            }
        }
    }

    // RTT Estimation Tests (RFC 6298)

    #[rstest]
    #[case::fast_rtt(50, 50, 25, 50)]
    #[case::slow_rtt(100, 100, 50, 100)]
    #[case::very_fast_rtt(10, 10, 5, 10)]
    fn test_rtt_estimation_first_sample(
        #[case] delay_ms: u64,
        #[case] expected_srtt_ms: u64,
        #[case] expected_rttvar_ms: u64,
        #[case] expected_min_rtt_ms: u64,
    ) {
        let mut tracker = mock_sent_packet_tracker();

        // Send a packet
        tracker.report_sent_packet(1, vec![1, 2, 3].into());

        // Simulate delay
        tracker.time_source.advance(Duration::from_millis(delay_ms));

        // Receive ACK
        let (ack_info, _) = tracker.report_received_receipts(&[1]);

        // Verify first sample (RFC 6298 Section 2.2)
        assert_eq!(ack_info.len(), 1);
        assert_eq!(ack_info[0].0, Some(Duration::from_millis(delay_ms)));
        assert_eq!(ack_info[0].1, 3); // packet size
        assert_eq!(
            tracker.smoothed_rtt(),
            Some(Duration::from_millis(expected_srtt_ms))
        );
        assert_eq!(tracker.rttvar, Duration::from_millis(expected_rttvar_ms)); // R/2
        assert_eq!(
            tracker.min_rtt(),
            Duration::from_millis(expected_min_rtt_ms)
        );
    }

    #[test]
    fn test_rtt_estimation_exponential_smoothing() {
        let mut tracker = mock_sent_packet_tracker();

        // First RTT sample: 100ms
        tracker.report_sent_packet(1, vec![1].into());
        tracker.time_source.advance(Duration::from_millis(100));
        tracker.report_received_receipts(&[1]);

        // Second RTT sample: 200ms
        tracker.report_sent_packet(2, vec![2].into());
        tracker.time_source.advance(Duration::from_millis(200));
        let (ack_info, _) = tracker.report_received_receipts(&[2]);

        // Verify exponential smoothing (ALPHA = 1/8)
        // SRTT = (7/8 * 100) + (1/8 * 200) = 87.5 + 25 = 112.5ms
        assert_eq!(ack_info[0].0, Some(Duration::from_millis(200))); // .0 = Some(RTT), .1 = packet size
        let srtt = tracker.smoothed_rtt().unwrap();
        assert!((srtt.as_millis() as i64 - 112).abs() <= 1); // Allow 1ms tolerance
    }

    #[test]
    fn test_rtt_excludes_retransmissions() {
        let mut tracker = mock_sent_packet_tracker();

        // Send packet
        tracker.report_sent_packet(1, vec![1].into());

        // Mark as retransmitted
        tracker.mark_retransmitted(1);

        // Advance time and receive ACK
        tracker.time_source.advance(Duration::from_millis(50));
        let (ack_info, _) = tracker.report_received_receipts(&[1]);

        // Should have ACK info but with None RTT (Karn's algorithm)
        // Critically: packet_size is STILL returned so LEDBAT can decrement flightsize
        assert_eq!(ack_info.len(), 1);
        assert_eq!(ack_info[0].0, None); // No RTT for retransmitted packet
        assert_eq!(ack_info[0].1, 1); // But packet size IS returned
        assert_eq!(tracker.smoothed_rtt(), None); // No RTT samples yet
    }

    #[test]
    fn test_rto_clamping() {
        let mut tracker = mock_sent_packet_tracker();

        // Initial RTO should be 1 second (RFC 6298 Section 2.1)
        assert_eq!(tracker.rto(), Duration::from_secs(1));

        // After first RTT sample, RTO should be clamped
        tracker.report_sent_packet(1, vec![1].into());
        tracker.time_source.advance(Duration::from_millis(10)); // Very fast RTT
        tracker.report_received_receipts(&[1]);

        // RTO should be clamped to minimum 500ms (accounts for ACK_CHECK_INTERVAL)
        assert!(tracker.rto() >= Duration::from_millis(500));
        assert!(tracker.rto() <= Duration::from_secs(60));
    }

    #[rstest]
    #[case::three_samples(&[100, 50, 150], 50)]
    #[case::descending(&[200, 100, 50], 50)]
    #[case::ascending(&[50, 100, 150], 50)]
    #[case::single_sample(&[75], 75)]
    fn test_min_rtt_tracking(#[case] rtt_samples: &[u64], #[case] expected_min_rtt_ms: u64) {
        let mut tracker = mock_sent_packet_tracker();

        for (i, &rtt_ms) in rtt_samples.iter().enumerate() {
            let packet_id = (i + 1) as PacketId;
            tracker.report_sent_packet(packet_id, vec![packet_id as u8].into());
            tracker.time_source.advance(Duration::from_millis(rtt_ms));
            tracker.report_received_receipts(&[packet_id]);
        }

        assert_eq!(
            tracker.min_rtt(),
            Duration::from_millis(expected_min_rtt_ms)
        );
    }

    #[test]
    fn test_retransmitted_packet_marked_on_timeout() {
        let mut tracker = mock_sent_packet_tracker();

        // Send packet
        tracker.report_sent_packet(1, vec![1, 2, 3].into());

        // Wait for timeout (using effective_rto which is 1s initially)
        tracker.time_source.advance(tracker.effective_rto());

        // Get resend action
        match tracker.get_resend() {
            ResendAction::Resend(packet_id, _) | ResendAction::TlpProbe(packet_id, _) => {
                assert_eq!(packet_id, 1);
                // Packet should be marked as retransmitted
                assert!(tracker.retransmitted_packets.contains(&1));
            }
            _ => panic!("Expected Resend or TlpProbe action"),
        }
    }

    // RTO Exponential Backoff Tests (RFC 6298 Section 5.5)

    #[rstest]
    #[case::first_timeout(1, 2, 2)]
    #[case::second_timeout(2, 4, 4)]
    #[case::third_timeout(3, 8, 8)]
    #[case::fourth_timeout(4, 16, 16)]
    fn test_rto_backoff_doubles_on_timeout(
        #[case] timeout_count: u32,
        #[case] expected_backoff: u32,
        #[case] expected_rto_secs: u64,
    ) {
        let mut tracker = mock_sent_packet_tracker();

        // Initial backoff should be 1
        assert_eq!(tracker.rto_backoff(), 1);
        assert_eq!(tracker.effective_rto(), Duration::from_secs(1));

        for _ in 0..timeout_count {
            tracker.on_timeout();
        }

        assert_eq!(tracker.rto_backoff(), expected_backoff);
        assert_eq!(
            tracker.effective_rto(),
            Duration::from_secs(expected_rto_secs)
        );
    }

    #[test]
    fn test_rto_backoff_capped_at_60_seconds() {
        let mut tracker = mock_sent_packet_tracker();

        // Trigger many timeouts to exceed the 60s cap
        for _ in 0..10 {
            tracker.on_timeout();
        }

        // Effective RTO should be capped at 60 seconds
        assert_eq!(tracker.effective_rto(), Duration::from_secs(60));

        // Backoff multiplier should be capped at MAX_RTO_BACKOFF
        assert_eq!(tracker.rto_backoff(), MAX_RTO_BACKOFF);
    }

    #[rstest]
    #[case::non_retransmitted(false)]
    #[case::retransmitted(true)]
    fn test_rto_backoff_resets_on_ack(#[case] mark_retransmitted: bool) {
        let mut tracker = mock_sent_packet_tracker();

        // Trigger some timeouts
        tracker.on_timeout();
        tracker.on_timeout();
        assert_eq!(tracker.rto_backoff(), 4);

        // Send a new packet
        tracker.report_sent_packet(1, vec![1, 2, 3].into());

        if mark_retransmitted {
            tracker.mark_retransmitted(1);
        }

        // Advance time slightly and receive ACK
        tracker.time_source.advance(Duration::from_millis(50));
        tracker.report_received_receipts(&[1]);

        // Backoff should be reset to 1 for both cases
        // (matching libutp behavior - any ACK resets backoff)
        assert_eq!(tracker.rto_backoff(), 1);
        // With 50ms RTT, base RTO is clamped to 500ms minimum
        // For non-retransmitted: 50ms RTT -> RTO clamped to 500ms
        // For retransmitted: no RTT sample, but backoff reset to 1
        //   Note: retransmitted case keeps the previous RTO value (1s initial),
        //   since no RTT sample is taken for retransmits (Karn's algorithm)
        if mark_retransmitted {
            // No RTT sample taken, so base RTO stays at initial 1s
            assert_eq!(tracker.effective_rto(), Duration::from_secs(1));
        } else {
            // RTT sample of 50ms -> RTO clamped to 500ms minimum
            assert_eq!(tracker.effective_rto(), Duration::from_millis(500));
        }
    }

    #[test]
    fn test_death_spiral_recovery() {
        // Tests that the system can recover from a high backoff state through
        // a single retransmit ACK (matching libutp behavior)
        let mut tracker = mock_sent_packet_tracker();

        // Simulate death spiral: many timeouts leading to high backoff
        for _ in 0..6 {
            tracker.on_timeout();
        }
        assert_eq!(tracker.rto_backoff(), 64);

        // Single retransmit ACK should fully recover (matching libutp, Linux TCP)
        // This is the key fix: any ACK proves the network is working
        tracker.report_sent_packet(1, vec![1, 2, 3].into());
        tracker.mark_retransmitted(1);
        tracker.time_source.advance(Duration::from_millis(50));
        tracker.report_received_receipts(&[1]);

        // Immediate full recovery - no gradual halving needed
        assert_eq!(tracker.rto_backoff(), 1);
    }

    #[test]
    fn test_mixed_ack_batch_resets_on_fresh_packet() {
        // When a batch contains both retransmit and non-retransmit ACKs,
        // the non-retransmit ACK should fully reset backoff to 1
        let mut tracker = mock_sent_packet_tracker();

        // Build up backoff
        for _ in 0..4 {
            tracker.on_timeout();
        }
        assert_eq!(tracker.rto_backoff(), 16);

        // Send 3 packets: 1 and 2 will be retransmitted, 3 will be fresh
        tracker.report_sent_packet(1, vec![1].into());
        tracker.report_sent_packet(2, vec![2].into());
        tracker.report_sent_packet(3, vec![3].into());

        // Mark packets 1 and 2 as retransmitted
        tracker.mark_retransmitted(1);
        tracker.mark_retransmitted(2);

        tracker.time_source.advance(Duration::from_millis(50));

        // ACK all three in one batch - packet 3 is fresh, should reset to 1
        tracker.report_received_receipts(&[1, 2, 3]);

        // Fresh packet ACK should have reset backoff to 1
        assert_eq!(tracker.rto_backoff(), 1);
    }

    #[test]
    fn test_backoff_re_elevation_after_recovery() {
        // Verifies the system handles: timeout -> recovery -> timeout correctly
        let mut tracker = mock_sent_packet_tracker();

        // Phase 1: Build up high backoff
        for _ in 0..4 {
            tracker.on_timeout();
        }
        assert_eq!(tracker.rto_backoff(), 16);

        // Phase 2: Single retransmit ACK immediately recovers (libutp behavior)
        tracker.report_sent_packet(1, vec![1].into());
        tracker.mark_retransmitted(1);
        tracker.time_source.advance(Duration::from_millis(50));
        tracker.report_received_receipts(&[1]);
        assert_eq!(
            tracker.rto_backoff(),
            1,
            "Single retransmit ACK should fully recover"
        );

        // Phase 3: New timeout should elevate backoff again
        tracker.on_timeout();
        assert_eq!(
            tracker.rto_backoff(),
            2,
            "Timeout after recovery should work normally"
        );

        tracker.on_timeout();
        assert_eq!(
            tracker.rto_backoff(),
            4,
            "Subsequent timeout should continue doubling"
        );

        // Phase 4: Fresh ACK should reset again
        tracker.report_sent_packet(10, vec![10].into());
        tracker.time_source.advance(Duration::from_millis(50));
        tracker.report_received_receipts(&[10]);
        assert_eq!(tracker.rto_backoff(), 1, "Fresh ACK should reset to 1");
    }

    #[test]
    fn test_get_resend_triggers_backoff() {
        let mut tracker = mock_sent_packet_tracker();

        // Initial backoff
        assert_eq!(tracker.rto_backoff(), 1);
        let initial_rto = tracker.effective_rto();

        // Send packet
        tracker.report_sent_packet(1, vec![1, 2, 3].into());

        // Wait for timeout (using initial effective_rto)
        tracker.time_source.advance(initial_rto);

        // Get resend - this should trigger backoff (RTO, not TLP since no RTT samples)
        match tracker.get_resend() {
            ResendAction::Resend(packet_id, _) => {
                assert_eq!(packet_id, 1);
            }
            ResendAction::TlpProbe(_, _) => panic!("TLP shouldn't fire without RTT samples"),
            _ => panic!("Expected Resend action"),
        }

        // Backoff should have doubled
        assert_eq!(tracker.rto_backoff(), 2);
    }

    #[test]
    fn test_consecutive_resends_increase_backoff() {
        let mut tracker = mock_sent_packet_tracker();

        // Send packet - will timeout after effective_rto() = 1s
        // No RTT samples, so TLP is disabled, only RTO fires
        tracker.report_sent_packet(1, vec![1, 2, 3].into());
        assert_eq!(tracker.rto_backoff(), 1);

        // First timeout after 1s, resend triggers backoff to 2
        tracker.time_source.advance(Duration::from_secs(1));
        match tracker.get_resend() {
            ResendAction::Resend(_, payload) => {
                // Re-register the packet - now enqueued with effective_rto() = 2s
                tracker.report_sent_packet(1, payload);
            }
            ResendAction::TlpProbe(_, _) => panic!("TLP shouldn't fire without RTT samples"),
            _ => panic!("Expected Resend"),
        }
        assert_eq!(tracker.rto_backoff(), 2);
        assert_eq!(tracker.effective_rto(), Duration::from_secs(2));

        // Second timeout after 2s (not 1s!), resend triggers backoff to 4
        tracker.time_source.advance(Duration::from_secs(2));
        match tracker.get_resend() {
            ResendAction::Resend(_, payload) => {
                // Re-register - now enqueued with effective_rto() = 4s
                tracker.report_sent_packet(1, payload);
            }
            ResendAction::TlpProbe(_, _) => panic!("TLP shouldn't fire without RTT samples"),
            _ => panic!("Expected Resend"),
        }
        assert_eq!(tracker.rto_backoff(), 4);
        assert_eq!(tracker.effective_rto(), Duration::from_secs(4));

        // Third timeout after 4s (not 1s or 2s!), resend triggers backoff to 8
        tracker.time_source.advance(Duration::from_secs(4));
        match tracker.get_resend() {
            ResendAction::Resend(_, _) => {}
            ResendAction::TlpProbe(_, _) => panic!("TLP shouldn't fire without RTT samples"),
            _ => panic!("Expected Resend"),
        }
        assert_eq!(tracker.rto_backoff(), 8);
        assert_eq!(tracker.effective_rto(), Duration::from_secs(8));
    }

    // =========================================================================
    // Tests for 500ms minimum RTO
    // =========================================================================
    //
    // RFC 6298 recommends 1s minimum RTO, but Linux uses 200ms. We use 500ms
    // to account for ACK_CHECK_INTERVAL (100ms) batching delay - without this,
    // high-latency connections (150ms+ RTT) experience spurious timeouts since
    // RTT + ACK_CHECK_INTERVAL can exceed MIN_RTO.
    //
    // These tests verify the minimum RTO behavior.
    const EXPECTED_MIN_RTO: Duration = Duration::from_millis(500);

    #[test]
    fn test_min_rto_is_500ms() {
        let mut tracker = mock_sent_packet_tracker();

        // Simulate a very fast network with 10ms RTT
        tracker.report_sent_packet(1, vec![1].into());
        tracker.time_source.advance(Duration::from_millis(10));
        tracker.report_received_receipts(&[1]);

        // RTO should be clamped to 500ms minimum, not 1s
        // With 10ms RTT: RTO = SRTT + max(G, 4*RTTVAR) = 10ms + max(10ms, 4*5ms) = 30ms
        // But this is below minimum, so should be clamped to 500ms
        assert_eq!(
            tracker.rto(),
            EXPECTED_MIN_RTO,
            "RTO should be clamped to 500ms minimum for fast networks"
        );
    }

    #[test]
    fn test_min_rto_allows_higher_values() {
        let mut tracker = mock_sent_packet_tracker();

        // Simulate a slower network with 400ms RTT
        tracker.report_sent_packet(1, vec![1].into());
        tracker.time_source.advance(Duration::from_millis(400));
        tracker.report_received_receipts(&[1]);

        // RTO should be above 500ms minimum, so not clamped
        // With 400ms RTT: RTO = 400ms + max(10ms, 4*200ms) = 400ms + 800ms = 1200ms
        assert!(
            tracker.rto() > EXPECTED_MIN_RTO,
            "RTO should not be clamped when naturally above minimum"
        );
    }

    #[test]
    fn test_initial_rto_before_samples() {
        let tracker = mock_sent_packet_tracker();

        // Before any RTT samples, initial RTO should still be 1s (RFC 6298 Section 2.1)
        // This is intentional - we don't know the RTT yet, so we're conservative
        assert_eq!(
            tracker.rto(),
            Duration::from_secs(1),
            "Initial RTO before any samples should be 1s"
        );
    }

    #[test]
    fn test_effective_rto_with_backoff_respects_min() {
        let mut tracker = mock_sent_packet_tracker();

        // Get a fast RTT sample to set base RTO to 500ms
        tracker.report_sent_packet(1, vec![1].into());
        tracker.time_source.advance(Duration::from_millis(10));
        tracker.report_received_receipts(&[1]);

        // Base RTO should be 500ms (clamped)
        assert_eq!(tracker.rto(), EXPECTED_MIN_RTO);

        // Effective RTO with backoff of 1 should also be 500ms
        assert_eq!(tracker.effective_rto(), EXPECTED_MIN_RTO);

        // After timeout, backoff doubles to 2, effective RTO = 1000ms
        tracker.on_timeout();
        assert_eq!(tracker.effective_rto(), Duration::from_millis(1000));

        // After another timeout, backoff = 4, effective RTO = 2000ms
        tracker.on_timeout();
        assert_eq!(tracker.effective_rto(), Duration::from_millis(2000));
    }

    #[test]
    fn test_fast_loss_detection_with_500ms_rto() {
        let mut tracker = mock_sent_packet_tracker();

        // Establish fast RTT baseline
        tracker.report_sent_packet(1, vec![1].into());
        tracker.time_source.advance(Duration::from_millis(20));
        tracker.report_received_receipts(&[1]);

        // Send another packet
        tracker.report_sent_packet(2, vec![2].into());

        // With 20ms RTT:
        // - TLP (PTO) = max(2 * 20ms, 10ms) = 40ms
        // - RTO = 500ms (minimum)
        // TLP should fire first at ~40ms

        // At 39ms, should still be waiting
        tracker.time_source.advance(Duration::from_millis(39));
        match tracker.get_resend() {
            ResendAction::WaitUntil(_) => {} // Expected - still waiting
            ResendAction::Resend(..)
            | ResendAction::TlpProbe(..)
            | ResendAction::Abandon { .. } => {
                panic!("Should not fire before TLP timeout (40ms)")
            }
        }

        // At 41ms total, TLP should fire (faster than 500ms RTO!)
        tracker.time_source.advance(Duration::from_millis(2));
        match tracker.get_resend() {
            ResendAction::TlpProbe(id, _) => assert_eq!(id, 2, "TLP should probe packet 2"),
            ResendAction::Resend(id, _) => {
                assert_eq!(id, 2, "Or Resend if TLP not yet implemented")
            }
            ResendAction::WaitUntil(_) => panic!("Should have triggered TLP after 40ms"),
            ResendAction::Abandon { .. } => panic!("unexpected Abandon"),
        }
    }

    #[test]
    fn test_timeout_intervals_actually_increase() {
        let mut tracker = mock_sent_packet_tracker();

        // Initial state: 1s RTO, no backoff
        // No RTT samples, so TLP is disabled - only RTO fires
        assert_eq!(tracker.effective_rto(), Duration::from_secs(1));

        // Send packet
        tracker.report_sent_packet(1, vec![1, 2, 3].into());

        // Verify: if we wait less than 1s, we should NOT get a resend
        tracker.time_source.advance(Duration::from_millis(999));
        match tracker.get_resend() {
            ResendAction::WaitUntil(_) => {} // Expected
            ResendAction::TlpProbe(_, _) => panic!("TLP shouldn't fire without RTT samples"),
            ResendAction::Resend(_, _) => panic!("Should not resend before RTO expires"),
            ResendAction::Abandon { .. } => panic!("unexpected Abandon"),
        }

        // Advance 1 more ms to trigger timeout
        tracker.time_source.advance(Duration::from_millis(1));
        let payload = match tracker.get_resend() {
            ResendAction::Resend(id, payload) => {
                assert_eq!(id, 1);
                payload
            }
            ResendAction::TlpProbe(_, _) => panic!("TLP shouldn't fire without RTT samples"),
            _ => panic!("Expected Resend after 1s"),
        };

        // Backoff is now 2, re-register packet with 2s timeout
        assert_eq!(tracker.rto_backoff(), 2);
        tracker.report_sent_packet(1, payload);

        // Verify: if we wait less than 2s, we should NOT get a resend
        tracker.time_source.advance(Duration::from_millis(1999));
        match tracker.get_resend() {
            ResendAction::WaitUntil(_) => {} // Expected
            ResendAction::TlpProbe(_, _) => panic!("TLP shouldn't fire without RTT samples"),
            ResendAction::Resend(_, _) => panic!("Should not resend before backed-off RTO (2s)"),
            ResendAction::Abandon { .. } => panic!("unexpected Abandon"),
        }

        // Advance 1 more ms to trigger timeout
        tracker.time_source.advance(Duration::from_millis(1));
        let payload = match tracker.get_resend() {
            ResendAction::Resend(id, payload) => {
                assert_eq!(id, 1);
                payload
            }
            ResendAction::TlpProbe(_, _) => panic!("TLP shouldn't fire without RTT samples"),
            _ => panic!("Expected Resend after 2s"),
        };

        // Backoff is now 4, re-register packet with 4s timeout
        assert_eq!(tracker.rto_backoff(), 4);
        tracker.report_sent_packet(1, payload);

        // Verify: if we wait less than 4s, we should NOT get a resend
        tracker.time_source.advance(Duration::from_millis(3999));
        match tracker.get_resend() {
            ResendAction::WaitUntil(_) => {} // Expected
            ResendAction::TlpProbe(_, _) => panic!("TLP shouldn't fire without RTT samples"),
            ResendAction::Resend(_, _) => panic!("Should not resend before backed-off RTO (4s)"),
            ResendAction::Abandon { .. } => panic!("unexpected Abandon"),
        }

        // Advance 1 more ms to trigger timeout
        tracker.time_source.advance(Duration::from_millis(1));
        match tracker.get_resend() {
            ResendAction::Resend(id, _) => assert_eq!(id, 1),
            ResendAction::TlpProbe(_, _) => panic!("TLP shouldn't fire without RTT samples"),
            _ => panic!("Expected Resend after 4s"),
        }

        // Backoff is now 8
        assert_eq!(tracker.rto_backoff(), 8);
    }

    // =========================================================================
    // Tests for TLP (Tail Loss Probe) - RFC 8985
    // =========================================================================
    //
    // TLP sends a probe packet before the full RTO expires to detect tail loss
    // earlier. This is especially useful for the last packets of a transfer
    // where there are no subsequent packets to trigger fast retransmit.
    //
    // TLP timer (PTO) = 2 * SRTT (minimum 10ms)
    // TLP fires BEFORE RTO, and doesn't apply backoff (it's speculative)

    #[test]
    fn test_tlp_fires_before_rto() {
        let mut tracker = mock_sent_packet_tracker();

        // Establish RTT baseline of 50ms
        tracker.report_sent_packet(1, vec![1].into());
        tracker.time_source.advance(Duration::from_millis(50));
        tracker.report_received_receipts(&[1]);

        // SRTT = 50ms, so PTO = 2 * 50ms = 100ms
        // RTO = 500ms (minimum)
        // TLP should fire at 100ms, before RTO at 500ms

        // Send another packet
        tracker.report_sent_packet(2, vec![2].into());

        // At 99ms, should still be waiting
        tracker.time_source.advance(Duration::from_millis(99));
        match tracker.get_resend() {
            ResendAction::WaitUntil(_) => {} // Expected
            ResendAction::Resend(..)
            | ResendAction::TlpProbe(..)
            | ResendAction::Abandon { .. } => {
                panic!("Should not fire before TLP timeout")
            }
        }

        // At 101ms, TLP should fire (not full RTO)
        tracker.time_source.advance(Duration::from_millis(2));
        match tracker.get_resend() {
            ResendAction::TlpProbe(id, _) => assert_eq!(id, 2, "TLP should probe packet 2"),
            ResendAction::Resend(_, _) => panic!("Should be TLP probe, not full RTO resend"),
            ResendAction::WaitUntil(_) => panic!("TLP should have fired at 2*SRTT"),
            ResendAction::Abandon { .. } => panic!("unexpected Abandon"),
        }
    }

    #[test]
    fn test_tlp_does_not_apply_backoff() {
        let mut tracker = mock_sent_packet_tracker();

        // Establish RTT baseline
        tracker.report_sent_packet(1, vec![1].into());
        tracker.time_source.advance(Duration::from_millis(50));
        tracker.report_received_receipts(&[1]);

        assert_eq!(tracker.rto_backoff(), 1);

        // Send packet and wait for TLP
        tracker.report_sent_packet(2, vec![2].into());
        tracker.time_source.advance(Duration::from_millis(101)); // Past PTO = 100ms

        match tracker.get_resend() {
            ResendAction::TlpProbe(_, _) => {}
            ResendAction::WaitUntil(_)
            | ResendAction::Resend(..)
            | ResendAction::Abandon { .. } => panic!("Expected TLP probe"),
        }

        // Backoff should NOT have increased (TLP is speculative)
        assert_eq!(tracker.rto_backoff(), 1, "TLP should not increase backoff");
    }

    #[test]
    fn test_tlp_followed_by_rto_if_no_ack() {
        let mut tracker = mock_sent_packet_tracker();

        // Establish RTT baseline of 50ms
        tracker.report_sent_packet(1, vec![1].into());
        tracker.time_source.advance(Duration::from_millis(50));
        tracker.report_received_receipts(&[1]);

        // PTO = 100ms, RTO = 500ms

        // Send packet
        tracker.report_sent_packet(2, vec![2].into());

        // TLP fires at ~100ms
        tracker.time_source.advance(Duration::from_millis(101));
        let payload = match tracker.get_resend() {
            ResendAction::TlpProbe(id, payload) => {
                assert_eq!(id, 2);
                payload
            }
            ResendAction::WaitUntil(_)
            | ResendAction::Resend(..)
            | ResendAction::Abandon { .. } => panic!("Expected TLP probe"),
        };

        // Re-register for RTO tracking after TLP
        tracker.report_sent_packet(2, payload);

        // Now if still no ACK, full RTO should fire
        // RTO timer starts fresh after TLP, so wait another 500ms (MIN_RTO)
        tracker.time_source.advance(Duration::from_millis(499));
        match tracker.get_resend() {
            ResendAction::WaitUntil(_) => {} // Still waiting for RTO
            ResendAction::Resend(..)
            | ResendAction::TlpProbe(..)
            | ResendAction::Abandon { .. } => {
                panic!("Should still be waiting for RTO")
            }
        }

        tracker.time_source.advance(Duration::from_millis(2));
        match tracker.get_resend() {
            ResendAction::Resend(id, _) => assert_eq!(id, 2, "RTO should fire after TLP failed"),
            ResendAction::TlpProbe(_, _) => panic!("Should be RTO, not another TLP"),
            _ => panic!("RTO should have fired"),
        }

        // NOW backoff should have increased
        assert_eq!(tracker.rto_backoff(), 2, "RTO should increase backoff");
    }

    #[test]
    fn test_tlp_cancelled_by_ack() {
        let mut tracker = mock_sent_packet_tracker();

        // Establish RTT baseline
        tracker.report_sent_packet(1, vec![1].into());
        tracker.time_source.advance(Duration::from_millis(50));
        tracker.report_received_receipts(&[1]);

        // Send packet
        tracker.report_sent_packet(2, vec![2].into());

        // Advance partway to TLP timeout
        tracker.time_source.advance(Duration::from_millis(60));

        // ACK arrives before TLP fires
        tracker.report_received_receipts(&[2]);

        // Now get_resend should just wait, not fire TLP
        match tracker.get_resend() {
            ResendAction::WaitUntil(_) => {} // Good - no pending packets
            ResendAction::TlpProbe(_, _) => panic!("TLP should be cancelled by ACK"),
            ResendAction::Resend(_, _) => panic!("No resend needed, packet was ACKed"),
            ResendAction::Abandon { .. } => panic!("unexpected Abandon"),
        }
    }

    #[test]
    fn test_tlp_minimum_timeout() {
        let mut tracker = mock_sent_packet_tracker();

        // Very fast RTT of 2ms -> PTO would be 4ms, but minimum is 10ms
        tracker.report_sent_packet(1, vec![1].into());
        tracker.time_source.advance(Duration::from_millis(2));
        tracker.report_received_receipts(&[1]);

        // SRTT = 2ms, so PTO = max(2*2ms, 10ms) = 10ms

        tracker.report_sent_packet(2, vec![2].into());

        // At 9ms, should still wait
        tracker.time_source.advance(Duration::from_millis(9));
        match tracker.get_resend() {
            ResendAction::WaitUntil(_) => {}
            ResendAction::Resend(..)
            | ResendAction::TlpProbe(..)
            | ResendAction::Abandon { .. } => {
                panic!("Should not fire before minimum TLP timeout")
            }
        }

        // At 11ms, TLP should fire
        tracker.time_source.advance(Duration::from_millis(2));
        match tracker.get_resend() {
            ResendAction::TlpProbe(id, _) => assert_eq!(id, 2),
            ResendAction::WaitUntil(_)
            | ResendAction::Resend(..)
            | ResendAction::Abandon { .. } => {
                panic!("TLP should fire at minimum 10ms")
            }
        }
    }

    #[test]
    fn test_tlp_disabled_before_rtt_samples() {
        let mut tracker = mock_sent_packet_tracker();

        // No RTT samples yet - TLP should be disabled, only RTO
        // Initial RTO is 1s

        tracker.report_sent_packet(1, vec![1].into());

        // At 500ms, should still wait (no TLP without RTT baseline)
        tracker.time_source.advance(Duration::from_millis(500));
        match tracker.get_resend() {
            ResendAction::WaitUntil(_) => {}
            ResendAction::TlpProbe(_, _) => panic!("TLP should not fire without RTT samples"),
            ResendAction::Resend(_, _) => panic!("RTO is 1s, should not fire at 500ms"),
            ResendAction::Abandon { .. } => panic!("unexpected Abandon"),
        }

        // At 1001ms, RTO fires (no TLP)
        tracker.time_source.advance(Duration::from_millis(501));
        match tracker.get_resend() {
            ResendAction::Resend(id, _) => assert_eq!(id, 1),
            ResendAction::TlpProbe(_, _) => panic!("Should be RTO, not TLP"),
            _ => panic!("RTO should fire at 1s"),
        }
    }

    #[test]
    fn test_tlp_only_once_per_flight() {
        let mut tracker = mock_sent_packet_tracker();

        // Establish RTT baseline
        tracker.report_sent_packet(1, vec![1].into());
        tracker.time_source.advance(Duration::from_millis(50));
        tracker.report_received_receipts(&[1]);

        // Send multiple packets
        tracker.report_sent_packet(2, vec![2].into());
        tracker.report_sent_packet(3, vec![3].into());
        tracker.report_sent_packet(4, vec![4].into());

        // TLP fires once for the tail (last packet)
        tracker.time_source.advance(Duration::from_millis(101));
        match tracker.get_resend() {
            ResendAction::TlpProbe(id, _) => {
                // Should probe the oldest unacked packet
                assert_eq!(id, 2, "TLP should probe oldest unacked packet");
            }
            ResendAction::WaitUntil(_)
            | ResendAction::Resend(..)
            | ResendAction::Abandon { .. } => panic!("Expected TLP probe"),
        }
    }

    /// Regression test for issue #4345: a packet that is never ACKed is
    /// retransmitted at most `MAX_PACKET_RETRANSMITS` times, then abandoned —
    /// `get_resend` returns `Abandon` and the packet is dropped from tracking,
    /// so it is no longer re-queued (which would otherwise pin flight size
    /// forever). An ACK before the limit resets the count.
    #[test]
    fn test_issue_4345_packet_abandoned_after_max_retransmits() {
        let mut tracker = mock_sent_packet_tracker();
        let payload_len = 3usize;
        tracker.report_sent_packet(1, vec![1, 2, 3].into());

        // Drive RTOs, re-sending each time (production re-registers), with no ACK.
        let mut resends = 0u32;
        let mut abandoned = false;
        for _ in 0..1000 {
            // Jump past the (backed-off) RTO deadline.
            tracker.time_source.advance(Duration::from_secs(120));
            match tracker.get_resend() {
                ResendAction::Resend(id, packet) => {
                    assert_eq!(id, 1);
                    // Re-register exactly as the production recv loop does.
                    tracker.report_sent_packet(id, packet);
                    resends += 1;
                }
                ResendAction::Abandon {
                    packet_id,
                    payload_len: len,
                } => {
                    assert_eq!(packet_id, 1);
                    assert_eq!(len, payload_len);
                    abandoned = true;
                    break;
                }
                ResendAction::TlpProbe(..) => {}
                ResendAction::WaitUntil(_) => {}
            }
        }

        assert!(
            abandoned,
            "issue #4345: packet must be abandoned, never was"
        );
        assert_eq!(
            resends, MAX_PACKET_RETRANSMITS,
            "issue #4345: packet should be re-sent exactly MAX_PACKET_RETRANSMITS \
             times before abandonment (got {resends})"
        );
        // After abandonment the packet is gone from tracking — no further resend.
        assert!(
            tracker.pending_receipts.is_empty(),
            "abandoned packet must be removed from pending_receipts"
        );
        assert!(
            tracker.retransmit_counts.is_empty(),
            "abandoned packet's retransmit count must be cleared"
        );
        tracker.time_source.advance(Duration::from_secs(120));
        assert!(
            matches!(tracker.get_resend(), ResendAction::WaitUntil(_)),
            "abandoned packet must NOT be re-queued for resend"
        );
    }

    /// An ACK arriving before the retransmit limit resets the count, so a
    /// flaky-but-recovering packet is never abandoned.
    #[test]
    fn test_issue_4345_ack_resets_retransmit_count() {
        let mut tracker = mock_sent_packet_tracker();
        tracker.report_sent_packet(1, vec![1, 2, 3].into());

        // Fire a few RTOs (re-sending each), short of the limit.
        for _ in 0..(MAX_PACKET_RETRANSMITS - 2) {
            tracker.time_source.advance(Duration::from_secs(120));
            match tracker.get_resend() {
                ResendAction::Resend(id, packet) => tracker.report_sent_packet(id, packet),
                other => panic!("expected Resend, got {other:?}"),
            }
        }
        assert_eq!(
            tracker.retransmit_counts.get(&1).copied(),
            Some(MAX_PACKET_RETRANSMITS - 2)
        );

        // ACK arrives → count cleared.
        tracker.report_received_receipts(&[1]);
        assert!(
            tracker.retransmit_counts.is_empty(),
            "ACK must clear the retransmit count so the packet can't be abandoned later"
        );
    }

    /// Issue #4345: a flaky-but-progressing packet (intermittent loss with an
    /// ACK before every `MAX_PACKET_RETRANSMITS`-th retransmit) is NEVER
    /// abandoned — the per-packet count is reset by each ACK, so a slow but
    /// alive transfer survives indefinitely.
    #[test]
    fn test_issue_4345_flaky_but_alive_packet_never_abandoned() {
        let mut tracker = mock_sent_packet_tracker();

        // Run many more than MAX_PACKET_RETRANSMITS total RTO fires, but slip an
        // ACK in before the limit each time. The packet must never be abandoned.
        let mut id = 1u32;
        tracker.report_sent_packet(id, vec![1, 2, 3].into());
        for cycle in 0..(MAX_PACKET_RETRANSMITS as usize * 5) {
            tracker.time_source.advance(Duration::from_secs(120));
            match tracker.get_resend() {
                ResendAction::Resend(rid, packet) => {
                    tracker.report_sent_packet(rid, packet);
                    // Every (MAX-1) retransmits, an ACK lands for this packet,
                    // resetting its count — then we "send" a fresh packet id so
                    // the flow keeps making progress.
                    if (cycle + 1) % (MAX_PACKET_RETRANSMITS as usize - 1) == 0 {
                        tracker.report_received_receipts(&[rid]);
                        id += 1;
                        tracker.report_sent_packet(id, vec![1, 2, 3].into());
                    }
                }
                ResendAction::Abandon { .. } => {
                    panic!(
                        "issue #4345: a flaky-but-alive packet was abandoned at \
                         cycle {cycle} — ACKs before the limit must keep it alive"
                    );
                }
                ResendAction::TlpProbe(_, _) | ResendAction::WaitUntil(_) => {}
            }
        }
    }

    /// Issue #4345: after a packet is abandoned, a late ACK for it must NOT
    /// release flight size a second time. Abandon removes the packet from
    /// `pending_receipts`, so `report_received_receipts` returns no entry for it
    /// (no `bytes_acked` for the caller to subtract) — guarding against a
    /// double-decrement that would under-count flight size.
    #[test]
    fn test_issue_4345_late_ack_after_abandon_is_noop() {
        let mut tracker = mock_sent_packet_tracker();
        tracker.report_sent_packet(7, vec![9, 9, 9].into());

        // Drive RTOs (re-sending) until the packet is abandoned.
        let mut abandoned = false;
        for _ in 0..1000 {
            tracker.time_source.advance(Duration::from_secs(120));
            match tracker.get_resend() {
                ResendAction::Resend(id, packet) => tracker.report_sent_packet(id, packet),
                ResendAction::Abandon { packet_id, .. } => {
                    assert_eq!(packet_id, 7);
                    abandoned = true;
                    break;
                }
                ResendAction::TlpProbe(_, _) | ResendAction::WaitUntil(_) => {}
            }
        }
        assert!(abandoned, "packet should have been abandoned");

        // A late ACK for the abandoned packet must produce NO ack entry — so the
        // caller does not call release/decrement a second time.
        let (ack_info, _) = tracker.report_received_receipts(&[7]);
        assert!(
            ack_info.is_empty(),
            "issue #4345: a late ACK for an abandoned packet must be a no-op \
             (no double release of flight size); got {ack_info:?}"
        );
    }

    // =========================================================================
    // Tests for drop_stream (issue #4345, stage 1)
    // =========================================================================
    //
    // drop_stream releases the in-flight bytes of an aborted outbound stream in
    // one shot. The defining safety property is NO DOUBLE-DECREMENT: after a
    // stream's packets are dropped, no later ACK or abandon may release their
    // bytes a second time. These tests pin both the exact byte total and the
    // absence of any second release path.

    /// Drive the tracker through RTOs (re-registering each resent packet, as the
    /// production recv loop does) until `packet_id` is abandoned. Any OTHER
    /// packet that resends is re-registered too; if any other packet is
    /// abandoned first the helper panics, since callers rely on the target being
    /// the one that ages out.
    fn drive_to_abandon(
        tracker: &mut SentPacketTracker<VirtualTime>,
        packet_id: PacketId,
    ) -> usize {
        for _ in 0..1000 {
            tracker.time_source.advance(Duration::from_secs(120));
            match tracker.get_resend() {
                ResendAction::Resend(id, packet) => {
                    // Re-register exactly as the production recv loop does. The
                    // Control-default path must preserve a stream packet's tag.
                    tracker.report_sent_packet(id, packet);
                }
                ResendAction::Abandon {
                    packet_id: id,
                    payload_len,
                } => {
                    assert_eq!(
                        id, packet_id,
                        "an unexpected packet ({id}) abandoned before the target ({packet_id})"
                    );
                    return payload_len;
                }
                ResendAction::TlpProbe(..) | ResendAction::WaitUntil(_) => {}
            }
        }
        panic!("packet {packet_id} was never abandoned");
    }

    fn report_sent_stream_packets_with_accounting(
        tracker: &mut SentPacketTracker<VirtualTime>,
        flight_size: &mut isize,
        stream: StreamId,
        packets: &[(PacketId, usize, usize)],
    ) {
        for &(packet_id, plaintext_size, encrypted_size) in packets {
            assert!(
                encrypted_size > plaintext_size,
                "test must model encryption/framing overhead"
            );
            *flight_size += plaintext_size as isize;
            tracker.report_sent_stream_packet_with_size(
                packet_id,
                vec![0u8; encrypted_size].into(),
                None,
                stream,
                plaintext_size,
            );
        }
    }

    fn release_flight_size(flight_size: &mut isize, payload_len: usize) {
        *flight_size -= payload_len as isize;
    }

    fn release_acks(
        flight_size: &mut isize,
        ack_info: Vec<(Option<Duration>, usize, Option<DeliveryRateToken>)>,
    ) {
        for (_, packet_size, _) in ack_info {
            release_flight_size(flight_size, packet_size);
        }
    }

    /// Issue #4402: every release path must return the same plaintext byte count
    /// that `on_send` added to flight size, even though the tracker stores the
    /// encrypted on-wire payload for resend.
    #[test]
    fn test_issue_4402_plaintext_flightsize_releases_balance_all_paths() {
        // Fully ACKed stream.
        let mut tracker = mock_sent_packet_tracker();
        let stream = StreamId::next();
        let mut flight_size = 0isize;
        report_sent_stream_packets_with_accounting(
            &mut tracker,
            &mut flight_size,
            stream,
            &[(1, 10, 26), (2, 14, 30)],
        );

        let (ack_info, _) = tracker.report_received_receipts(&[1, 2]);
        release_acks(&mut flight_size, ack_info);
        assert_eq!(
            flight_size, 0,
            "ACK path must release the plaintext byte count that on_send added"
        );

        // Fully abandoned stream.
        let mut tracker = mock_sent_packet_tracker();
        let stream = StreamId::next();
        let mut flight_size = 0isize;
        report_sent_stream_packets_with_accounting(
            &mut tracker,
            &mut flight_size,
            stream,
            &[(10, 11, 27), (11, 13, 29)],
        );

        let released = drive_to_abandon(&mut tracker, 10);
        release_flight_size(&mut flight_size, released);
        let released = drive_to_abandon(&mut tracker, 11);
        release_flight_size(&mut flight_size, released);
        assert_eq!(
            flight_size, 0,
            "Abandon path must release the plaintext byte count that on_send added"
        );

        // Fully dropped stream.
        let mut tracker = mock_sent_packet_tracker();
        let stream = StreamId::next();
        let mut flight_size = 0isize;
        report_sent_stream_packets_with_accounting(
            &mut tracker,
            &mut flight_size,
            stream,
            &[(20, 17, 33), (21, 19, 35)],
        );

        let released = tracker.drop_stream(stream);
        release_flight_size(&mut flight_size, released as usize);
        assert_eq!(
            flight_size, 0,
            "drop_stream path must release the plaintext byte count that on_send added"
        );

        // Mixed stream: one packet ACKed, remainder dropped.
        let mut tracker = mock_sent_packet_tracker();
        let stream = StreamId::next();
        let mut flight_size = 0isize;
        report_sent_stream_packets_with_accounting(
            &mut tracker,
            &mut flight_size,
            stream,
            &[(30, 23, 39), (31, 25, 41), (32, 27, 43)],
        );

        let (ack_info, _) = tracker.report_received_receipts(&[30]);
        release_acks(&mut flight_size, ack_info);
        let released = tracker.drop_stream(stream);
        release_flight_size(&mut flight_size, released as usize);
        assert_eq!(
            flight_size, 0,
            "mixed ACK/drop paths must not over-release encrypted payload overhead"
        );
    }

    /// drop_stream returns the EXACT byte total of the dropped stream's packets
    /// and removes them from every tracking structure.
    #[test]
    fn test_drop_stream_returns_exact_byte_total() {
        let mut tracker = mock_sent_packet_tracker();
        let stream = StreamId::next();

        // Three fragments of distinct sizes: 3 + 5 + 7 = 15 bytes.
        tracker.report_sent_stream_packet(1, vec![0u8; 3].into(), None, stream);
        tracker.report_sent_stream_packet(2, vec![0u8; 5].into(), None, stream);
        tracker.report_sent_stream_packet(3, vec![0u8; 7].into(), None, stream);

        let released = tracker.drop_stream(stream);
        assert_eq!(
            released, 15,
            "must return the sum of dropped payload lengths"
        );

        // Every tracking structure is now empty for those packets.
        assert!(tracker.pending_receipts.is_empty());
        assert!(tracker.packet_streams.is_empty());
        assert!(tracker.retransmit_counts.is_empty());
        // resend_queue entries for the dropped packets are gone.
        assert!(
            tracker.resend_queue.is_empty(),
            "dropped packets must be purged from the resend queue"
        );
    }

    /// After drop_stream, a later ACK for a dropped packet does NOT produce an
    /// ack entry — so the caller never subtracts its bytes from flight size a
    /// second time (no double-credit on the ACK path).
    #[test]
    fn test_drop_stream_then_ack_is_noop() {
        let mut tracker = mock_sent_packet_tracker();
        let stream = StreamId::next();

        tracker.report_sent_stream_packet(10, vec![1, 2, 3, 4].into(), None, stream);
        tracker.report_sent_stream_packet(11, vec![5, 6].into(), None, stream);

        let released = tracker.drop_stream(stream);
        assert_eq!(released, 6);

        // A late ACK for both dropped packets must yield NO ack-info entries.
        let (ack_info, _) = tracker.report_received_receipts(&[10, 11]);
        assert!(
            ack_info.is_empty(),
            "issue #4345: ACK after drop_stream must not release bytes again; got {ack_info:?}"
        );
    }

    /// After drop_stream, a dropped packet can never reach the Abandon path
    /// (get_resend skips ids absent from pending_receipts), so abandonment can
    /// not release its bytes a second time (no double-credit on the abandon
    /// path).
    #[test]
    fn test_drop_stream_then_abandon_path_is_noop() {
        let mut tracker = mock_sent_packet_tracker();
        let stream = StreamId::next();

        tracker.report_sent_stream_packet(20, vec![0u8; 8].into(), None, stream);

        let released = tracker.drop_stream(stream);
        assert_eq!(released, 8);

        // The dropped packet is gone from pending_receipts, so get_resend never
        // re-emits it (no Resend, no Abandon) no matter how long we wait.
        for _ in 0..(MAX_PACKET_RETRANSMITS + 5) {
            tracker.time_source.advance(Duration::from_secs(120));
            match tracker.get_resend() {
                ResendAction::WaitUntil(_) => {}
                other => {
                    panic!("issue #4345: a dropped packet must never resend/abandon; got {other:?}")
                }
            }
        }
    }

    /// Interleaving: dropping stream A leaves stream B's accounting fully
    /// intact — B's bytes are still pending, still ACKable, and B's byte total
    /// is unaffected by A's drop.
    #[test]
    fn test_drop_stream_leaves_other_stream_intact() {
        let mut tracker = mock_sent_packet_tracker();
        let stream_a = StreamId::next();
        let stream_b = StreamId::next();

        // Interleave A and B packets in send order.
        tracker.report_sent_stream_packet(1, vec![0u8; 3].into(), None, stream_a);
        tracker.report_sent_stream_packet(2, vec![0u8; 100].into(), None, stream_b);
        tracker.report_sent_stream_packet(3, vec![0u8; 3].into(), None, stream_a);
        tracker.report_sent_stream_packet(4, vec![0u8; 200].into(), None, stream_b);

        // Dropping A releases only A's 6 bytes.
        assert_eq!(tracker.drop_stream(stream_a), 6);

        // B's two packets are untouched.
        assert!(tracker.pending_receipts.contains_key(&2));
        assert!(tracker.pending_receipts.contains_key(&4));
        assert_eq!(
            tracker.packet_streams.get(&2),
            Some(&PacketStream::Stream(stream_b))
        );
        assert_eq!(
            tracker.packet_streams.get(&4),
            Some(&PacketStream::Stream(stream_b))
        );
        // A's packets are gone.
        assert!(!tracker.pending_receipts.contains_key(&1));
        assert!(!tracker.pending_receipts.contains_key(&3));

        // B can still be ACKed normally and reports its full size.
        let (ack_info, _) = tracker.report_received_receipts(&[2, 4]);
        let acked_bytes: usize = ack_info.iter().map(|(_, size, _)| *size).sum();
        assert_eq!(
            acked_bytes, 300,
            "stream B's bytes must still be ACK-creditable"
        );

        // Dropping B now releases exactly B's 300 bytes (nothing double-counted).
        assert_eq!(
            tracker.drop_stream(stream_b),
            0,
            "stream B was already fully ACKed, so nothing left to release"
        );
    }

    /// Dropping a stream does NOT touch control (non-stream) packets that share
    /// the connection.
    #[test]
    fn test_drop_stream_leaves_control_packets_intact() {
        let mut tracker = mock_sent_packet_tracker();
        let stream = StreamId::next();

        // A control packet (e.g. a NoOp/handshake) and a stream fragment.
        tracker.report_sent_packet(1, vec![0u8; 50].into()); // Control (default)
        tracker.report_sent_stream_packet(2, vec![0u8; 9].into(), None, stream);

        assert_eq!(tracker.drop_stream(stream), 9);

        // The control packet survives and is still ACKable.
        assert!(tracker.pending_receipts.contains_key(&1));
        assert_eq!(tracker.packet_streams.get(&1), Some(&PacketStream::Control));
        let (ack_info, _) = tracker.report_received_receipts(&[1]);
        assert_eq!(ack_info.len(), 1);
        assert_eq!(ack_info[0].1, 50);
    }

    /// Empty / unknown stream id → returns 0, no panic. Also covers a stream
    /// whose packets were all already ACKed (drained).
    #[test]
    fn test_drop_stream_unknown_or_drained_returns_zero() {
        let mut tracker = mock_sent_packet_tracker();

        // Never-seen stream → 0.
        assert_eq!(tracker.drop_stream(StreamId::next()), 0);

        // A stream whose only packet was already ACKed → 0 (already drained).
        let stream = StreamId::next();
        tracker.report_sent_stream_packet(1, vec![0u8; 5].into(), None, stream);
        tracker.report_received_receipts(&[1]);
        assert_eq!(
            tracker.drop_stream(stream),
            0,
            "a fully-ACKed stream has no bytes left to release"
        );

        // Dropping again is still a no-op (idempotent, no panic).
        assert_eq!(tracker.drop_stream(stream), 0);
    }

    /// Boundary: a single-packet stream releases exactly that packet's bytes.
    #[test]
    fn test_drop_stream_single_packet() {
        let mut tracker = mock_sent_packet_tracker();
        let stream = StreamId::next();
        tracker.report_sent_stream_packet(1, vec![0u8; 42].into(), None, stream);
        assert_eq!(tracker.drop_stream(stream), 42);
        assert!(tracker.pending_receipts.is_empty());
        assert!(tracker.packet_streams.is_empty());
    }

    /// Boundary: many packets in one stream are all released in a single call.
    #[test]
    fn test_drop_stream_many_packets() {
        let mut tracker = mock_sent_packet_tracker();
        let stream = StreamId::next();
        let n = 500u32;
        let per_packet = 4usize;
        for id in 0..n {
            tracker.report_sent_stream_packet(id, vec![0u8; per_packet].into(), None, stream);
        }
        assert_eq!(
            tracker.drop_stream(stream),
            (n as u64) * (per_packet as u64)
        );
        assert!(tracker.pending_receipts.is_empty());
        assert!(tracker.packet_streams.is_empty());
        assert!(tracker.resend_queue.is_empty());
    }

    /// drop_stream sweeps packets that have already been retransmitted (so their
    /// resend-queue entry was re-pushed and their Karn/retransmit/TLP state is
    /// populated) as well as fresh pending packets — i.e. packets in BOTH the
    /// pending set and the resend machinery.
    #[test]
    fn test_drop_stream_sweeps_retransmitted_and_pending() {
        let mut tracker = mock_sent_packet_tracker();
        let stream = StreamId::next();

        // Packet 1: send, let it RTO once, and re-register (as the recv loop
        // does). The resend re-registration goes through report_sent_packet,
        // which must PRESERVE the stream tag.
        tracker.report_sent_stream_packet(1, vec![0u8; 10].into(), None, stream);
        tracker.time_source.advance(Duration::from_secs(2));
        match tracker.get_resend() {
            ResendAction::Resend(id, packet) => {
                assert_eq!(id, 1);
                // Re-register exactly as production does (Control-default path),
                // which must keep packet 1 tagged with `stream`.
                tracker.report_sent_packet(id, packet);
            }
            other => panic!("expected Resend for packet 1, got {other:?}"),
        }
        assert_eq!(
            tracker.packet_streams.get(&1),
            Some(&PacketStream::Stream(stream)),
            "resend re-registration must preserve the stream tag"
        );
        assert!(tracker.retransmitted_packets.contains(&1));

        // Packet 2: fresh, never retransmitted.
        tracker.report_sent_stream_packet(2, vec![0u8; 20].into(), None, stream);

        // Drop the stream: both the retransmitted packet 1 and the fresh
        // packet 2 are released, 10 + 20 = 30 bytes.
        assert_eq!(tracker.drop_stream(stream), 30);
        assert!(tracker.pending_receipts.is_empty());
        assert!(tracker.packet_streams.is_empty());
        assert!(
            tracker.retransmitted_packets.is_empty(),
            "retransmitted-packet state for dropped packets must be cleared"
        );
        assert!(tracker.retransmit_counts.is_empty());
    }

    /// A packet that was ABANDONED (its bytes already released via the Abandon
    /// action) is gone from tracking, so a subsequent drop_stream of its stream
    /// must NOT release those bytes again — the abandon and drop_stream paths
    /// don't double-credit each other.
    #[test]
    fn test_drop_stream_does_not_double_release_abandoned_packet() {
        let mut tracker = mock_sent_packet_tracker();
        let stream = StreamId::next();

        // Abandon packet 1 first (its 4 bytes are released by the Abandon
        // action). Sending packet 2 only AFTER abandonment keeps it out of the
        // shared RTO timeline, so the only abandonment is packet 1's.
        tracker.report_sent_stream_packet(1, vec![0u8; 4].into(), None, stream);
        drive_to_abandon(&mut tracker, 1);

        // Packet 1 is gone from tracking after abandonment.
        assert!(!tracker.packet_streams.contains_key(&1));
        assert!(!tracker.pending_receipts.contains_key(&1));

        // Now a fresh fragment of the SAME stream is still in flight.
        tracker.report_sent_stream_packet(2, vec![0u8; 6].into(), None, stream);

        // drop_stream now releases ONLY packet 2's 6 bytes, NOT packet 1's
        // already-released 4 bytes.
        assert_eq!(
            tracker.drop_stream(stream),
            6,
            "issue #4345: drop_stream must not re-release bytes already released by Abandon"
        );
    }

    /// Saturating-arithmetic / zero correctness: dropping a stream whose only
    /// packet is empty returns 0 and removes the packet (no panic, no underflow).
    #[test]
    fn test_drop_stream_zero_length_packet() {
        let mut tracker = mock_sent_packet_tracker();
        let stream = StreamId::next();
        tracker.report_sent_stream_packet(1, Box::from(&[][..]), None, stream);
        assert_eq!(tracker.drop_stream(stream), 0);
        assert!(tracker.pending_receipts.is_empty());
        assert!(tracker.packet_streams.is_empty());
    }

    /// Resend-gap race (issue #4345, stage 2): a packet that `get_resend` has
    /// just handed out for retransmission — but that the recv loop has NOT yet
    /// re-registered (it is mid `send_to().await`) — must STILL be released by a
    /// concurrent `drop_stream`.
    ///
    /// `drop_stream` runs from the spawned outbound-stream abort task while the
    /// per-connection recv loop drives resends; both share the tracker mutex.
    /// Before the keep-the-entry-on-resend fix, `get_resend` removed the packet
    /// from `pending_receipts` and relied on the recv loop to re-insert it after
    /// the await. A `drop_stream` landing in that window would see no
    /// `pending_receipts` entry — release 0 bytes for a genuinely in-flight
    /// packet AND strip its stream tag — leaving the fragment pinned in flight
    /// size (partial defeat of the fix). Now the packet stays in
    /// `pending_receipts` across the resend, so this test asserts it is released.
    ///
    /// This is the exact interleaving the gap describes: Resend handed out, NO
    /// re-registration yet, then drop_stream.
    #[test]
    fn test_drop_stream_releases_packet_out_for_resend() {
        let mut tracker = mock_sent_packet_tracker();
        let stream = StreamId::next();

        tracker.report_sent_stream_packet(1, vec![0u8; 13].into(), None, stream);

        // Fire the RTO so the packet is "out for resend": get_resend returns it
        // to the (recv-loop) caller. We deliberately do NOT re-register it,
        // modelling the recv task being suspended on its UDP send_to().await.
        tracker.time_source.advance(tracker.effective_rto());
        match tracker.get_resend() {
            ResendAction::Resend(id, _packet) => assert_eq!(id, 1),
            other => panic!("expected Resend for the out-for-resend packet, got {other:?}"),
        }

        // The packet is still tracked (keep-the-entry fix) and still tagged with
        // its stream, so a concurrent drop_stream in this gap releases its bytes.
        assert!(tracker.pending_receipts.contains_key(&1));
        assert_eq!(
            tracker.packet_streams.get(&1),
            Some(&PacketStream::Stream(stream))
        );
        assert_eq!(
            tracker.drop_stream(stream),
            13,
            "issue #4345: a packet out-for-resend must still be released by drop_stream"
        );

        // And it is fully gone afterwards (no later double-release).
        assert!(tracker.pending_receipts.is_empty());
        assert!(tracker.packet_streams.is_empty());
        let (ack_info, _) = tracker.report_received_receipts(&[1]);
        assert!(
            ack_info.is_empty(),
            "a late ACK after the gap-drop must not release bytes again"
        );
    }

    /// MUST-FIX regression (issue #4345, stage-2 review): the MIRROR of the
    /// resend-gap test. The recv loop's resend re-registration, when it runs
    /// AFTER a concurrent `drop_stream` already removed and released the packet,
    /// must NOT resurrect it — otherwise a later ACK/abandon of the resurrected
    /// "zombie" releases its bytes a SECOND time (flight-size under-count) and
    /// breaks the "in pending_receipts iff in flight" invariant.
    ///
    /// Models the race tail: get_resend hands the packet out, the abort task
    /// runs drop_stream (releases the bytes once), THEN the recv loop resumes and
    /// re-registers via `refresh_sent_packet` — which must be a no-op because the
    /// entry is gone.
    ///
    /// Asserts (a) the packet is NOT resurrected into any tracking structure, and
    /// (b) a later ACK and a later abandon attempt each release ZERO.
    ///
    /// This FAILS without the fix: the old recv-loop path called
    /// `report_sent_packet`, whose insert-on-absent branch resurrects the packet
    /// as a `Control` zombie (see `test_report_sent_packet_resurrects_*` which
    /// pins that footgun). `refresh_sent_packet` closes it.
    #[test]
    fn test_refresh_after_drop_stream_does_not_resurrect() {
        let mut tracker = mock_sent_packet_tracker();
        let stream = StreamId::next();

        tracker.report_sent_stream_packet(1, vec![0u8; 13].into(), None, stream);

        // get_resend hands the packet out for resend (recv loop now "awaiting
        // send_to"). Capture the payload as the recv loop would.
        tracker.time_source.advance(tracker.effective_rto());
        let resent_payload = match tracker.get_resend() {
            ResendAction::Resend(id, packet) => {
                assert_eq!(id, 1);
                packet
            }
            other => panic!("expected Resend, got {other:?}"),
        };

        // Concurrent abort task drops the stream — releases the 13 bytes ONCE.
        assert_eq!(tracker.drop_stream(stream), 13);
        assert!(!tracker.pending_receipts.contains_key(&1));

        // recv loop resumes and re-registers via the production path. Must no-op.
        let refreshed = tracker.refresh_sent_packet(1, resent_payload, None);
        assert!(
            !refreshed,
            "refresh_sent_packet must report no-op for a dropped packet"
        );

        // (a) NOT resurrected into any structure.
        assert!(
            !tracker.pending_receipts.contains_key(&1),
            "dropped packet must NOT be resurrected into pending_receipts"
        );
        assert!(!tracker.packet_streams.contains_key(&1));
        assert!(!tracker.retransmit_counts.contains_key(&1));

        // (b1) A later ACK releases ZERO (no ack-info entry → caller decrements
        // nothing).
        let (ack_info, _) = tracker.report_received_receipts(&[1]);
        assert!(
            ack_info.is_empty(),
            "ACK of a dropped-then-refreshed packet must release zero bytes; got {ack_info:?}"
        );

        // (b2) A later abandon attempt also releases ZERO: the packet is absent
        // from the resend queue / pending, so get_resend never emits Abandon for
        // it no matter how long we wait.
        for _ in 0..(MAX_PACKET_RETRANSMITS + 5) {
            tracker.time_source.advance(Duration::from_secs(120));
            match tracker.get_resend() {
                ResendAction::WaitUntil(_) => {}
                other => panic!(
                    "dropped packet must never resend/abandon after refresh no-op; got {other:?}"
                ),
            }
        }
    }

    /// Pins the FOOTGUN that `refresh_sent_packet` exists to avoid: the
    /// insert-capable `report_sent_packet` DOES resurrect a dropped packet. This
    /// documents why the recv loop must NOT use `report_sent_packet` for resend
    /// re-registration (issue #4345 stage-2 review). It is the "without the fix"
    /// behavior, asserted directly so a future refactor that points the recv loop
    /// back at `report_sent_packet` is caught by `test_refresh_after_drop_stream_
    /// does_not_resurrect` failing.
    #[test]
    fn test_report_sent_packet_resurrects_dropped_packet_footgun() {
        let mut tracker = mock_sent_packet_tracker();
        let stream = StreamId::next();
        tracker.report_sent_stream_packet(1, vec![0u8; 13].into(), None, stream);
        assert_eq!(tracker.drop_stream(stream), 13);

        // The insert-on-absent path resurrects the packet (as Control) — exactly
        // the bug. refresh_sent_packet is what the production recv loop uses to
        // avoid this; report_sent_packet remains insert-capable for first sends.
        tracker.report_sent_packet(1, vec![0u8; 13].into());
        assert!(
            tracker.pending_receipts.contains_key(&1),
            "report_sent_packet IS insert-capable (resurrects) — this is why the \
             recv loop must use refresh_sent_packet instead"
        );
        assert_eq!(
            tracker.packet_streams.get(&1),
            Some(&PacketStream::Control),
            "the resurrected packet is mis-tagged Control (the double-release footgun)"
        );
    }
}
