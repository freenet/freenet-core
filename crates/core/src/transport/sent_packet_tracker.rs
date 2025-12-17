use super::PacketId;
use crate::util::time_source::{InstantTimeSrc, TimeSource};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
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
    pending_receipts: HashMap<PacketId, (Arc<[u8]>, Instant)>,

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
        }
    }
}

impl<T: TimeSource> SentPacketTracker<T> {
    pub(super) fn report_sent_packet(&mut self, packet_id: PacketId, payload: Arc<[u8]>) {
        let sent_time = self.time_source.now();
        self.pending_receipts
            .insert(packet_id, (payload, sent_time));
        self.resend_queue.push_back(ResendQueueEntry {
            timeout_at: sent_time + MESSAGE_CONFIRMATION_TIMEOUT,
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
    pub(super) fn smoothed_rtt(&self) -> Option<Duration> {
        self.srtt
    }

    /// Get the minimum observed RTT
    pub(super) fn min_rtt(&self) -> Duration {
        self.min_rtt
    }

    /// Get the current retransmission timeout
    pub(super) fn rto(&self) -> Duration {
        self.rto
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

                return ResendAction::Resend(entry.packet_id, packet);
            }
            // If the packet is no longer in pending_receipts, it means its receipt has been received.
            // No action needed, continue to check the next entry in the queue.
        }

        ResendAction::WaitUntil(now + MESSAGE_CONFIRMATION_TIMEOUT)
    }
}

#[derive(Debug, PartialEq)]
pub enum ResendAction {
    WaitUntil(Instant),
    Resend(u32, Arc<[u8]>),
}

struct ResendQueueEntry {
    timeout_at: Instant,
    packet_id: u32,
}

// Unit tests
#[cfg(test)]
pub(in crate::transport) mod tests {
    use super::*;
    use crate::transport::MessagePayload;
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
        tracker
            .time_source
            .advance_time(MESSAGE_CONFIRMATION_TIMEOUT);
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
        let _ = tracker.report_received_receipts(&[1]);

        // Simulate time just before the resend time for packet 2
        tracker
            .time_source
            .advance_time(MESSAGE_CONFIRMATION_TIMEOUT - Duration::from_millis(1));

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

        tracker.report_sent_packet(0, MessagePayload::new().into());

        tracker.time_source.advance_time(Duration::from_millis(10));

        tracker.report_sent_packet(1, MessagePayload::new().into());

        let packet_1_timeout = tracker.time_source.now() + MESSAGE_CONFIRMATION_TIMEOUT;

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

        // Wait for timeout
        tracker
            .time_source
            .advance_time(MESSAGE_CONFIRMATION_TIMEOUT);

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
}
