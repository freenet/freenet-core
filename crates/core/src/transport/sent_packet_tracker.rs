use super::bbr::DeliveryRateToken;
use super::PacketId;
#[cfg(test)]
use crate::simulation::RealTime;
use crate::simulation::TimeSource;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;

/// Entry for pending packet receipts: (payload, sent_time_nanos, delivery_token)
type PendingReceiptEntry = (Box<[u8]>, u64, Option<DeliveryRateToken>);

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
    /// The list of packets that have been sent but not yet acknowledged
    /// Stores (payload, sent_time_nanos, delivery_token) for RTT calculation and BBR
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
        self.report_sent_packet_with_token(packet_id, payload, None);
    }

    /// Report a sent packet with an optional BBR delivery rate token.
    ///
    /// The token is used by BBR for accurate delivery rate estimation. When an ACK
    /// arrives, the token is returned alongside the RTT sample so it can be passed
    /// to `BbrController::on_ack_with_token`.
    pub(super) fn report_sent_packet_with_token(
        &mut self,
        packet_id: PacketId,
        payload: Box<[u8]>,
        token: Option<DeliveryRateToken>,
    ) {
        let sent_time_nanos = self.time_source.now_nanos();
        self.pending_receipts
            .insert(packet_id, (payload, sent_time_nanos, token));
        self.resend_queue.push_back(ResendQueueEntry { packet_id });
        self.total_packets_sent += 1;
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

            if let Some((payload, sent_time_nanos, token)) = self.pending_receipts.get(packet_id) {
                let packet_size = payload.len();
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

            // Remove from pending, retransmitted, and TLP tracking
            self.pending_receipts.remove(packet_id);
            self.retransmitted_packets.remove(packet_id);
            self.tlp_sent_packets.remove(packet_id);
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
                let abs_diff = if sample > srtt {
                    sample - srtt
                } else {
                    srtt - sample
                };
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
    /// calling this function again. If a packet is resent you **must** call
    /// `report_sent_packet` again with the same packet_id.
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
                .map(|(_, ts, _)| *ts)
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
                    if let Some((packet, _, _)) = self.pending_receipts.get(&entry.packet_id) {
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

            // Check if RTO should fire
            if now_nanos >= rto_deadline {
                if let Some((packet, _sent_time_nanos, _token)) =
                    self.pending_receipts.remove(&entry.packet_id)
                {
                    // Update packet loss proportion for a lost packet
                    self.packet_loss_proportion = self.packet_loss_proportion
                        * (1.0 - PACKET_LOSS_DECAY_FACTOR)
                        + PACKET_LOSS_DECAY_FACTOR;

                    // Mark as retransmitted for Karn's algorithm
                    self.mark_retransmitted(entry.packet_id);

                    // RFC 6298 Section 5.5: Back off the timer on retransmission
                    self.on_timeout();

                    // Clean up TLP tracking for this packet
                    self.tlp_sent_packets.remove(&entry.packet_id);

                    return ResendAction::Resend(entry.packet_id, packet);
                }
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
        assert_eq!(tracker.pending_receipts.len(), 0);
        assert_eq!(tracker.resend_queue.len(), 0);
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
            _ => panic!("Expected WaitUntil, got Resend/TlpProbe too early"),
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
            _ => panic!("Expected ResendAction::WaitUntil"),
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
            _ => panic!("Should not fire before TLP timeout (40ms)"),
        }

        // At 41ms total, TLP should fire (faster than 500ms RTO!)
        tracker.time_source.advance(Duration::from_millis(2));
        match tracker.get_resend() {
            ResendAction::TlpProbe(id, _) => assert_eq!(id, 2, "TLP should probe packet 2"),
            ResendAction::Resend(id, _) => {
                assert_eq!(id, 2, "Or Resend if TLP not yet implemented")
            }
            ResendAction::WaitUntil(_) => panic!("Should have triggered TLP after 40ms"),
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
            _ => panic!("Should not fire before TLP timeout"),
        }

        // At 101ms, TLP should fire (not full RTO)
        tracker.time_source.advance(Duration::from_millis(2));
        match tracker.get_resend() {
            ResendAction::TlpProbe(id, _) => assert_eq!(id, 2, "TLP should probe packet 2"),
            ResendAction::Resend(_, _) => panic!("Should be TLP probe, not full RTO resend"),
            ResendAction::WaitUntil(_) => panic!("TLP should have fired at 2*SRTT"),
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
            _ => panic!("Expected TLP probe"),
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
            _ => panic!("Expected TLP probe"),
        };

        // Re-register for RTO tracking after TLP
        tracker.report_sent_packet(2, payload);

        // Now if still no ACK, full RTO should fire
        // RTO timer starts fresh after TLP, so wait another 500ms (MIN_RTO)
        tracker.time_source.advance(Duration::from_millis(499));
        match tracker.get_resend() {
            ResendAction::WaitUntil(_) => {} // Still waiting for RTO
            _ => panic!("Should still be waiting for RTO"),
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
            _ => panic!("Should not fire before minimum TLP timeout"),
        }

        // At 11ms, TLP should fire
        tracker.time_source.advance(Duration::from_millis(2));
        match tracker.get_resend() {
            ResendAction::TlpProbe(id, _) => assert_eq!(id, 2),
            _ => panic!("TLP should fire at minimum 10ms"),
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
            _ => panic!("Expected TLP probe"),
        }
    }
}
