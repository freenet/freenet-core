use super::{MessageId, MessagePayload, MAX_CONFIRMATION_DELAY};
use crate::util::{CachingSystemTimeSrc, TimeSource};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};
use tracing::warn;

const NETWORK_DELAY_ALLOWANCE: Duration = Duration::from_millis(500);

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
/// ```
/// let mut sent_packet_tracker = todo!();
/// loop {
///   match sent_packet_tracker.get_resend() {
///      ResendAction::WaitUntil(wait_until) => {
///        sleep_until(wait_until).await;
///      }
///      ResendAction::Resend(message_id, packet) => {
///       // Send packet and then call report_sent_packet again with the same message_id.
///      }
///   }
/// }
/// ```
pub(super) struct SentPacketTracker<T: TimeSource> {
    /// The list of packets that have been sent but not yet acknowledged
    pending_receipts: HashMap<MessageId, MessagePayload>,

    resend_queue: VecDeque<ResendQueueEntry>,

    packet_loss_proportion: f64,

    pub(super) time_source: T,
}

impl SentPacketTracker<CachingSystemTimeSrc> {
    pub(super) fn new() -> Self {
        SentPacketTracker {
            pending_receipts: HashMap::new(),
            resend_queue: VecDeque::new(),
            packet_loss_proportion: 0.0,
            time_source: CachingSystemTimeSrc::new(),
        }
    }
}

impl<T: TimeSource> SentPacketTracker<T> {
    /// Get an estimate of the proportion of outbound packets that were lost This is a value
    /// between 0.0 and 1.0, where 0.0 means no packets are lost and 1.0 means all packets are
    /// lost. This estimate will be biased towards 0.0 initially, and will converge to the
    /// true value over time. It's accuracy will be approximately
    /// `PACKET_LOSS_DECAY_FACTOR` (0.001).
    pub(super) fn get_recent_packet_loss(&self) -> f64 {
        self.packet_loss_proportion
    }

    pub(super) fn report_sent_packet(&mut self, message_id: u32, payload: MessagePayload) {
        self.pending_receipts.insert(message_id, payload);
        self.resend_queue.push_back(ResendQueueEntry {
            timeout_at: self.time_source.now() + MESSAGE_CONFIRMATION_TIMEOUT,
            message_id,
        });
    }

    pub(super) fn report_received_receipts(&mut self, message_ids: &[MessageId]) {
        for message_id in message_ids {
            // This can be simplified but I'm leaving it like this for readability.
            self.packet_loss_proportion = self.packet_loss_proportion
                * (1.0 - PACKET_LOSS_DECAY_FACTOR)
                + (PACKET_LOSS_DECAY_FACTOR * 0.0);
            self.pending_receipts.remove(message_id);
        }
    }

    /// Either get a packet that needs to be resent, or how long the caller should wait until
    /// calling this function again. If a packet is resent you **must** call
    /// `report_sent_packet` again with the same message_id.
    pub(super) fn get_resend(&mut self) -> ResendAction {
        let now = self.time_source.now();

        while let Some(entry) = self.resend_queue.pop_front() {
            if entry.timeout_at > now {
                if !self.pending_receipts.contains_key(&entry.message_id) {
                    continue;
                }
                let wait_until = entry.timeout_at;
                self.resend_queue.push_front(entry);
                return ResendAction::WaitUntil(wait_until);
            } else if let Some(packet) = self.pending_receipts.remove(&entry.message_id) {
                // Update packet loss proportion for a lost packet
                // Resend logic
                self.packet_loss_proportion = self.packet_loss_proportion
                    * (1.0 - PACKET_LOSS_DECAY_FACTOR)
                    + PACKET_LOSS_DECAY_FACTOR;

                return ResendAction::Resend(entry.message_id, packet);
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
    Resend(u32, MessagePayload),
}

struct ResendQueueEntry {
    timeout_at: Instant,
    message_id: u32,
}

// Unit tests
#[cfg(test)]
pub(in crate::transport) mod tests {
    use super::*;
    use crate::util::MockTimeSource;

    pub(in crate::transport) fn mock_sent_packet_tracker() -> SentPacketTracker<MockTimeSource> {
        let time_source = MockTimeSource::new(Instant::now());

        SentPacketTracker {
            pending_receipts: HashMap::new(),
            resend_queue: VecDeque::new(),
            packet_loss_proportion: 0.0,
            time_source,
        }
    }

    #[test]
    fn test_report_sent_packet() {
        let mut tracker = mock_sent_packet_tracker();
        tracker.report_sent_packet(1, vec![1, 2, 3]);
        assert_eq!(tracker.pending_receipts.len(), 1);
        assert_eq!(tracker.resend_queue.len(), 1);
        assert_eq!(tracker.packet_loss_proportion, 0.0);
    }

    #[test]
    fn test_report_received_receipts() {
        let mut tracker = mock_sent_packet_tracker();
        tracker.report_sent_packet(1, vec![1, 2, 3]);
        tracker.report_received_receipts(&[1]);
        assert_eq!(tracker.pending_receipts.len(), 0);
        assert!(tracker.resend_queue.len() <= 1);
        assert_eq!(tracker.packet_loss_proportion, 0.0);
    }

    #[test]
    fn test_packet_lost() {
        let mut tracker = mock_sent_packet_tracker();
        tracker.report_sent_packet(1, vec![1, 2, 3]);
        tracker
            .time_source
            .advance_time(MESSAGE_CONFIRMATION_TIMEOUT);
        let resend_action = tracker.get_resend();
        assert_eq!(resend_action, ResendAction::Resend(1, vec![1, 2, 3]));
        assert_eq!(tracker.pending_receipts.len(), 0);
        assert_eq!(tracker.resend_queue.len(), 0);
        assert_eq!(tracker.packet_loss_proportion, PACKET_LOSS_DECAY_FACTOR);
    }

    #[test]
    fn test_immediate_receipt_then_resend() {
        let mut tracker = mock_sent_packet_tracker();

        // Report two packets sent
        tracker.report_sent_packet(1, vec![1, 2, 3]);
        tracker.report_sent_packet(2, vec![4, 5, 6]);

        // Immediately report receipt for the first packet
        tracker.report_received_receipts(&[1]);

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
            ResendAction::Resend(message_id, _) => assert_eq!(message_id, 2),
            _ => panic!("Expected Resend for message ID 2"),
        }
    }

    #[test]
    fn test_get_resend_with_pending_receipts() {
        let mut tracker = mock_sent_packet_tracker();

        tracker.report_sent_packet(0, MessagePayload::new());

        tracker.time_source.advance_time(Duration::from_millis(10));

        tracker.report_sent_packet(1, MessagePayload::new());

        let packet_1_timeout = tracker.time_source.now() + MESSAGE_CONFIRMATION_TIMEOUT;

        // Acknowledge receipt of the first packet
        tracker.report_received_receipts(&[0]);

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
}
