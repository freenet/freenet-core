use super::PacketId;
use crate::util::time_source::{InstantTimeSrc, TimeSource};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

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
/// With a minimum/base RTO of 1s and a max effective RTO cap of 60s, a backoff of 64 is
/// sufficient to ensure we can reach the 60s limit; for higher base RTO values, the cap is
/// reached with smaller backoff multipliers.
const MAX_RTO_BACKOFF: u32 = 64;

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
    /// Changed to store (payload, sent_time) for RTT calculation
    pending_receipts: HashMap<PacketId, (Box<[u8]>, Instant)>,

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
}

impl SentPacketTracker<InstantTimeSrc> {
    pub(super) fn new() -> Self {
        SentPacketTracker {
            pending_receipts: HashMap::new(),
            resend_queue: VecDeque::new(),
            packet_loss_proportion: 0.0,
            time_source: InstantTimeSrc::new(),
            // RFC 6298: Initial RTO = 1 second
            srtt: None,
            rttvar: Duration::from_secs(0),
            min_rtt: Duration::from_millis(100), // Reasonable default until real RTT samples arrive
            rto: Duration::from_secs(1),
            retransmitted_packets: HashSet::new(),
            total_packets_sent: 0,
            rto_backoff: 1, // No backoff initially
        }
    }
}

impl<T: TimeSource> SentPacketTracker<T> {
    pub(super) fn report_sent_packet(&mut self, packet_id: PacketId, payload: Box<[u8]>) {
        let sent_time = self.time_source.now();
        self.pending_receipts
            .insert(packet_id, (payload, sent_time));
        // Use effective_rto() which includes exponential backoff (RFC 6298 Section 5.5)
        // This ensures retransmitted packets wait progressively longer (1s → 2s → 4s → ...)
        self.resend_queue.push_back(ResendQueueEntry {
            timeout_at: sent_time + self.effective_rto(),
            packet_id,
        });
        self.total_packets_sent += 1;
    }

    /// Reports that receipts have been received for the given packet IDs.
    /// Returns: ((RTT sample, packet size) pairs, loss rate)
    pub(super) fn report_received_receipts(
        &mut self,
        packet_ids: &[PacketId],
    ) -> (Vec<(Duration, usize)>, Option<f64>) {
        let now = self.time_source.now();
        let mut rtt_samples = Vec::new();

        for packet_id in packet_ids {
            // Update loss proportion (ACK received = no loss)
            self.packet_loss_proportion = self.packet_loss_proportion
                * (1.0 - PACKET_LOSS_DECAY_FACTOR)
                + (PACKET_LOSS_DECAY_FACTOR * 0.0);

            // Calculate RTT if this packet wasn't retransmitted (Karn's algorithm)
            if !self.retransmitted_packets.contains(packet_id) {
                if let Some((payload, sent_time)) = self.pending_receipts.get(packet_id) {
                    let rtt_sample = now.duration_since(*sent_time);
                    let packet_size = payload.len();
                    rtt_samples.push((rtt_sample, packet_size));
                    self.update_rtt(rtt_sample);

                    // RFC 6298 Section 5.7: Reset backoff on valid ACK
                    // (only for non-retransmitted packets to be safe)
                    self.rto_backoff = 1;
                }
            }

            // Remove from pending and retransmitted tracking
            self.pending_receipts.remove(packet_id);
            self.retransmitted_packets.remove(packet_id);
        }

        let loss_rate = if self.total_packets_sent > 0 {
            Some(self.packet_loss_proportion)
        } else {
            None
        };

        (rtt_samples, loss_rate)
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

        // Clamp RTO to [1s, 60s] (RFC 6298 Section 2.4 & 2.5)
        if self.rto < Duration::from_secs(1) {
            self.rto = Duration::from_secs(1);
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
    pub(super) fn get_resend(&mut self) -> ResendAction {
        let now = self.time_source.now();

        while let Some(entry) = self.resend_queue.pop_front() {
            if entry.timeout_at > now {
                if !self.pending_receipts.contains_key(&entry.packet_id) {
                    continue;
                }
                let wait_until = entry.timeout_at;
                self.resend_queue.push_front(entry);
                return ResendAction::WaitUntil(wait_until);
            } else if let Some((packet, _sent_time)) =
                self.pending_receipts.remove(&entry.packet_id)
            {
                // Update packet loss proportion for a lost packet
                // Resend logic
                self.packet_loss_proportion = self.packet_loss_proportion
                    * (1.0 - PACKET_LOSS_DECAY_FACTOR)
                    + PACKET_LOSS_DECAY_FACTOR;

                // Mark as retransmitted for Karn's algorithm
                self.mark_retransmitted(entry.packet_id);

                // RFC 6298 Section 5.5: Back off the timer on retransmission
                self.on_timeout();

                return ResendAction::Resend(entry.packet_id, packet);
            }
            // If the packet is no longer in pending_receipts, it means its receipt has been received.
            // No action needed, continue to check the next entry in the queue.
        }

        // Use effective RTO (with backoff) for the wait time
        ResendAction::WaitUntil(now + self.effective_rto())
    }
}

#[derive(Debug, PartialEq)]
pub enum ResendAction {
    WaitUntil(Instant),
    Resend(u32, Box<[u8]>),
}

struct ResendQueueEntry {
    timeout_at: Instant,
    packet_id: u32,
}

// Unit tests
#[cfg(test)]
pub(in crate::transport) mod tests {
    use super::*;
    use crate::util::time_source::MockTimeSource;

    pub(in crate::transport) fn mock_sent_packet_tracker() -> SentPacketTracker<MockTimeSource> {
        let time_source = MockTimeSource::new(Instant::now());

        SentPacketTracker {
            pending_receipts: HashMap::new(),
            resend_queue: VecDeque::new(),
            packet_loss_proportion: 0.0,
            time_source,
            srtt: None,
            rttvar: Duration::from_secs(0),
            min_rtt: Duration::from_millis(100),
            rto: Duration::from_secs(1),
            retransmitted_packets: HashSet::new(),
            total_packets_sent: 0,
            rto_backoff: 1,
        }
    }

    #[test]
    fn test_report_sent_packet() {
        let mut tracker = mock_sent_packet_tracker();
        tracker.report_sent_packet(1, vec![1, 2, 3].into());
        assert_eq!(tracker.pending_receipts.len(), 1);
        assert_eq!(tracker.resend_queue.len(), 1);
        assert_eq!(tracker.packet_loss_proportion, 0.0);
        assert_eq!(tracker.total_packets_sent, 1);
    }

    #[test]
    fn test_report_received_receipts() {
        let mut tracker = mock_sent_packet_tracker();
        tracker.report_sent_packet(1, vec![1, 2, 3].into());
        let (rtt_samples, loss_rate) = tracker.report_received_receipts(&[1]);
        assert_eq!(tracker.pending_receipts.len(), 0);
        assert!(tracker.resend_queue.len() <= 1);
        assert_eq!(tracker.packet_loss_proportion, 0.0);
        assert_eq!(rtt_samples.len(), 1); // Should have one RTT sample
        assert!(loss_rate.is_some()); // Should have loss rate
    }

    #[test]
    fn test_packet_lost() {
        let mut tracker = mock_sent_packet_tracker();
        tracker.report_sent_packet(1, vec![1, 2, 3].into());
        // Packets now use effective_rto() which is 1s initially (not MESSAGE_CONFIRMATION_TIMEOUT)
        tracker.time_source.advance_time(tracker.effective_rto());
        let resend_action = tracker.get_resend();
        assert_eq!(resend_action, ResendAction::Resend(1, vec![1, 2, 3].into()));
        assert_eq!(tracker.pending_receipts.len(), 0);
        assert_eq!(tracker.resend_queue.len(), 0);
        assert_eq!(tracker.packet_loss_proportion, PACKET_LOSS_DECAY_FACTOR);
    }

    #[test]
    fn test_immediate_receipt_then_resend() {
        let mut tracker = mock_sent_packet_tracker();
        let initial_rto = tracker.effective_rto();

        // Report two packets sent
        tracker.report_sent_packet(1, vec![1, 2, 3].into());
        tracker.report_sent_packet(2, vec![4, 5, 6].into());

        // Immediately report receipt for the first packet
        let _ = tracker.report_received_receipts(&[1]);

        // Simulate time just before the resend time for packet 2
        tracker
            .time_source
            .advance_time(initial_rto - Duration::from_millis(1));

        // This should not trigger a resend yet
        match tracker.get_resend() {
            ResendAction::WaitUntil(_) => (),
            _ => panic!("Expected WaitUntil, got Resend too early"),
        }

        // Now advance time to trigger resend for packet 2
        tracker.time_source.advance_time(Duration::from_millis(2));

        // This should now trigger a resend for packet 2
        match tracker.get_resend() {
            ResendAction::Resend(packet_id, _) => assert_eq!(packet_id, 2),
            _ => panic!("Expected Resend for message ID 2"),
        }
    }

    #[test]
    fn test_get_resend_with_pending_receipts() {
        let mut tracker = mock_sent_packet_tracker();

        tracker.report_sent_packet(0, Box::from(&[][..]));

        tracker.time_source.advance_time(Duration::from_millis(10));

        // Capture the effective RTO at the time packet 1 is sent
        let effective_rto_at_send = tracker.effective_rto();
        tracker.report_sent_packet(1, Box::from(&[][..]));

        let packet_1_timeout = tracker.time_source.now() + effective_rto_at_send;

        // Acknowledge receipt of the first packet
        let _ = tracker.report_received_receipts(&[0]);

        // The next call to get_resend should calculate the wait time based on the second packet (id 1)
        match tracker.get_resend() {
            ResendAction::WaitUntil(wait_until) => {
                assert_eq!(
                    wait_until, packet_1_timeout,
                    "Wait time does not match expected for second packet"
                );
            }
            _ => panic!("Expected ResendAction::WaitUntil"),
        }
    }

    // RTT Estimation Tests (RFC 6298)

    #[test]
    fn test_rtt_estimation_first_sample() {
        let mut tracker = mock_sent_packet_tracker();

        // Send a packet
        tracker.report_sent_packet(1, vec![1, 2, 3].into());

        // Simulate 50ms delay
        tracker.time_source.advance_time(Duration::from_millis(50));

        // Receive ACK
        let (rtt_samples, _) = tracker.report_received_receipts(&[1]);

        // Verify first sample (RFC 6298 Section 2.2)
        assert_eq!(rtt_samples.len(), 1);
        assert_eq!(rtt_samples[0].0, Duration::from_millis(50)); // .0 = RTT, .1 = packet size
        assert_eq!(tracker.smoothed_rtt(), Some(Duration::from_millis(50)));
        assert_eq!(tracker.rttvar, Duration::from_millis(25)); // R/2
        assert_eq!(tracker.min_rtt(), Duration::from_millis(50));
    }

    #[test]
    fn test_rtt_estimation_exponential_smoothing() {
        let mut tracker = mock_sent_packet_tracker();

        // First RTT sample: 100ms
        tracker.report_sent_packet(1, vec![1].into());
        tracker.time_source.advance_time(Duration::from_millis(100));
        tracker.report_received_receipts(&[1]);

        // Second RTT sample: 200ms
        tracker.report_sent_packet(2, vec![2].into());
        tracker.time_source.advance_time(Duration::from_millis(200));
        let (rtt_samples, _) = tracker.report_received_receipts(&[2]);

        // Verify exponential smoothing (ALPHA = 1/8)
        // SRTT = (7/8 * 100) + (1/8 * 200) = 87.5 + 25 = 112.5ms
        assert_eq!(rtt_samples[0].0, Duration::from_millis(200)); // .0 = RTT, .1 = packet size
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
        tracker.time_source.advance_time(Duration::from_millis(50));
        let (rtt_samples, _) = tracker.report_received_receipts(&[1]);

        // Should NOT include RTT sample (Karn's algorithm)
        assert_eq!(rtt_samples.len(), 0);
        assert_eq!(tracker.smoothed_rtt(), None); // No RTT samples yet
    }

    #[test]
    fn test_rto_clamping() {
        let mut tracker = mock_sent_packet_tracker();

        // Initial RTO should be 1 second (RFC 6298 Section 2.1)
        assert_eq!(tracker.rto(), Duration::from_secs(1));

        // After first RTT sample, RTO should be clamped
        tracker.report_sent_packet(1, vec![1].into());
        tracker.time_source.advance_time(Duration::from_millis(10)); // Very fast RTT
        tracker.report_received_receipts(&[1]);

        // RTO should be clamped to minimum 1s (RFC 6298 Section 2.4)
        assert!(tracker.rto() >= Duration::from_secs(1));
        assert!(tracker.rto() <= Duration::from_secs(60));
    }

    #[test]
    fn test_min_rtt_tracking() {
        let mut tracker = mock_sent_packet_tracker();

        // Send three packets with different RTTs
        tracker.report_sent_packet(1, vec![1].into());
        tracker.time_source.advance_time(Duration::from_millis(100));
        tracker.report_received_receipts(&[1]);

        tracker.report_sent_packet(2, vec![2].into());
        tracker.time_source.advance_time(Duration::from_millis(50)); // Minimum
        tracker.report_received_receipts(&[2]);

        tracker.report_sent_packet(3, vec![3].into());
        tracker.time_source.advance_time(Duration::from_millis(150));
        tracker.report_received_receipts(&[3]);

        // min_rtt should be 50ms
        assert_eq!(tracker.min_rtt(), Duration::from_millis(50));
    }

    #[test]
    fn test_retransmitted_packet_marked_on_timeout() {
        let mut tracker = mock_sent_packet_tracker();

        // Send packet
        tracker.report_sent_packet(1, vec![1, 2, 3].into());

        // Wait for timeout (using effective_rto which is 1s initially)
        tracker.time_source.advance_time(tracker.effective_rto());

        // Get resend action
        match tracker.get_resend() {
            ResendAction::Resend(packet_id, _) => {
                assert_eq!(packet_id, 1);
                // Packet should be marked as retransmitted
                assert!(tracker.retransmitted_packets.contains(&1));
            }
            _ => panic!("Expected Resend action"),
        }
    }

    // RTO Exponential Backoff Tests (RFC 6298 Section 5.5)

    #[test]
    fn test_rto_backoff_doubles_on_timeout() {
        let mut tracker = mock_sent_packet_tracker();

        // Initial backoff should be 1
        assert_eq!(tracker.rto_backoff(), 1);
        assert_eq!(tracker.effective_rto(), Duration::from_secs(1));

        // First timeout - backoff doubles to 2
        tracker.on_timeout();
        assert_eq!(tracker.rto_backoff(), 2);
        assert_eq!(tracker.effective_rto(), Duration::from_secs(2));

        // Second timeout - backoff doubles to 4
        tracker.on_timeout();
        assert_eq!(tracker.rto_backoff(), 4);
        assert_eq!(tracker.effective_rto(), Duration::from_secs(4));

        // Third timeout - backoff doubles to 8
        tracker.on_timeout();
        assert_eq!(tracker.rto_backoff(), 8);
        assert_eq!(tracker.effective_rto(), Duration::from_secs(8));
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

    #[test]
    fn test_rto_backoff_resets_on_valid_ack() {
        let mut tracker = mock_sent_packet_tracker();

        // Trigger some timeouts
        tracker.on_timeout();
        tracker.on_timeout();
        assert_eq!(tracker.rto_backoff(), 4);

        // Send a new packet
        tracker.report_sent_packet(1, vec![1, 2, 3].into());

        // Advance time slightly and receive ACK
        tracker.time_source.advance_time(Duration::from_millis(50));
        tracker.report_received_receipts(&[1]);

        // Backoff should be reset to 1
        assert_eq!(tracker.rto_backoff(), 1);
        assert_eq!(tracker.effective_rto(), Duration::from_secs(1));
    }

    #[test]
    fn test_rto_backoff_not_reset_on_retransmitted_ack() {
        let mut tracker = mock_sent_packet_tracker();

        // Trigger some timeouts
        tracker.on_timeout();
        tracker.on_timeout();
        assert_eq!(tracker.rto_backoff(), 4);

        // Send a packet and mark it as retransmitted
        tracker.report_sent_packet(1, vec![1, 2, 3].into());
        tracker.mark_retransmitted(1);

        // Advance time slightly and receive ACK
        tracker.time_source.advance_time(Duration::from_millis(50));
        tracker.report_received_receipts(&[1]);

        // Backoff should NOT be reset for retransmitted packets (Karn's algorithm)
        assert_eq!(tracker.rto_backoff(), 4);
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
        tracker.time_source.advance_time(initial_rto);

        // Get resend - this should trigger backoff
        match tracker.get_resend() {
            ResendAction::Resend(packet_id, _) => {
                assert_eq!(packet_id, 1);
            }
            _ => panic!("Expected Resend action"),
        }

        // Backoff should have doubled
        assert_eq!(tracker.rto_backoff(), 2);
    }

    #[test]
    fn test_consecutive_resends_increase_backoff() {
        let mut tracker = mock_sent_packet_tracker();

        // Send packet - will timeout after effective_rto() = 1s
        tracker.report_sent_packet(1, vec![1, 2, 3].into());
        assert_eq!(tracker.rto_backoff(), 1);

        // First timeout after 1s, resend triggers backoff to 2
        tracker.time_source.advance_time(Duration::from_secs(1));
        match tracker.get_resend() {
            ResendAction::Resend(_, payload) => {
                // Re-register the packet - now enqueued with effective_rto() = 2s
                tracker.report_sent_packet(1, payload);
            }
            _ => panic!("Expected Resend"),
        }
        assert_eq!(tracker.rto_backoff(), 2);
        assert_eq!(tracker.effective_rto(), Duration::from_secs(2));

        // Second timeout after 2s (not 1s!), resend triggers backoff to 4
        tracker.time_source.advance_time(Duration::from_secs(2));
        match tracker.get_resend() {
            ResendAction::Resend(_, payload) => {
                // Re-register - now enqueued with effective_rto() = 4s
                tracker.report_sent_packet(1, payload);
            }
            _ => panic!("Expected Resend"),
        }
        assert_eq!(tracker.rto_backoff(), 4);
        assert_eq!(tracker.effective_rto(), Duration::from_secs(4));

        // Third timeout after 4s (not 1s or 2s!), resend triggers backoff to 8
        tracker.time_source.advance_time(Duration::from_secs(4));
        match tracker.get_resend() {
            ResendAction::Resend(_, _) => {}
            _ => panic!("Expected Resend"),
        }
        assert_eq!(tracker.rto_backoff(), 8);
        assert_eq!(tracker.effective_rto(), Duration::from_secs(8));
    }

    /// End-to-end test verifying that timeout intervals actually increase.
    /// This is the critical test that validates the fix works correctly.
    #[test]
    fn test_timeout_intervals_actually_increase() {
        let mut tracker = mock_sent_packet_tracker();

        // Initial state: 1s RTO, no backoff
        assert_eq!(tracker.effective_rto(), Duration::from_secs(1));

        // Send packet
        tracker.report_sent_packet(1, vec![1, 2, 3].into());

        // Verify: if we wait less than 1s, we should NOT get a resend
        tracker.time_source.advance_time(Duration::from_millis(999));
        match tracker.get_resend() {
            ResendAction::WaitUntil(_) => {} // Expected
            ResendAction::Resend(_, _) => panic!("Should not resend before RTO expires"),
        }

        // Advance 1 more ms to trigger timeout
        tracker.time_source.advance_time(Duration::from_millis(1));
        let payload = match tracker.get_resend() {
            ResendAction::Resend(id, payload) => {
                assert_eq!(id, 1);
                payload
            }
            _ => panic!("Expected Resend after 1s"),
        };

        // Backoff is now 2, re-register packet with 2s timeout
        assert_eq!(tracker.rto_backoff(), 2);
        tracker.report_sent_packet(1, payload);

        // Verify: if we wait less than 2s, we should NOT get a resend
        tracker
            .time_source
            .advance_time(Duration::from_millis(1999));
        match tracker.get_resend() {
            ResendAction::WaitUntil(_) => {} // Expected
            ResendAction::Resend(_, _) => panic!("Should not resend before backed-off RTO (2s)"),
        }

        // Advance 1 more ms to trigger timeout
        tracker.time_source.advance_time(Duration::from_millis(1));
        let payload = match tracker.get_resend() {
            ResendAction::Resend(id, payload) => {
                assert_eq!(id, 1);
                payload
            }
            _ => panic!("Expected Resend after 2s"),
        };

        // Backoff is now 4, re-register packet with 4s timeout
        assert_eq!(tracker.rto_backoff(), 4);
        tracker.report_sent_packet(1, payload);

        // Verify: if we wait less than 4s, we should NOT get a resend
        tracker
            .time_source
            .advance_time(Duration::from_millis(3999));
        match tracker.get_resend() {
            ResendAction::WaitUntil(_) => {} // Expected
            ResendAction::Resend(_, _) => panic!("Should not resend before backed-off RTO (4s)"),
        }

        // Advance 1 more ms to trigger timeout
        tracker.time_source.advance_time(Duration::from_millis(1));
        match tracker.get_resend() {
            ResendAction::Resend(id, _) => assert_eq!(id, 1),
            _ => panic!("Expected Resend after 4s"),
        }

        // Backoff is now 8
        assert_eq!(tracker.rto_backoff(), 8);
    }
}
