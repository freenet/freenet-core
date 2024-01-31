use crate::transport::MessageId;
use crate::util::{SystemTime, TimeSource};
use std::collections::{HashMap, VecDeque};
use std::mem;
use std::time::{Duration, Instant};

const RETAIN_TIME: Duration = Duration::from_secs(60);
const MAX_PENDING_RECEIPTS: usize = 20;

pub(super) struct ReceivedPacketTracker<T: TimeSource> {
    pending_receipts: Vec<MessageId>,
    message_id_time: VecDeque<(MessageId, Instant)>,
    time_by_message_id: HashMap<MessageId, Instant>,
    time_source: T,
}

impl ReceivedPacketTracker<SystemTime> {
    pub(super) fn new() -> Self {
        ReceivedPacketTracker {
            pending_receipts: Vec::new(),
            message_id_time: VecDeque::new(),
            time_by_message_id: HashMap::new(),
            time_source: SystemTime,
        }
    }
}

impl<T: TimeSource> ReceivedPacketTracker<T> {
    pub(super) fn report_received_packets(&mut self, message_id: MessageId) -> ReportResult {
        self.cleanup();
        if self.time_by_message_id.contains_key(&message_id) {
            ReportResult::AlreadyReceived
        } else {
            self.time_by_message_id
                .insert(message_id, self.time_source.now());
            self.message_id_time
                .push_back((message_id, self.time_source.now()));

            self.pending_receipts.push(message_id);

            if self.pending_receipts.len() < MAX_PENDING_RECEIPTS {
                ReportResult::Ok
            } else {
                ReportResult::QueueFull
            }
        }
    }

    /// Returns a list of packets that have been received since the last call to this function.
    /// This should be called every time a packet is sent to ensure that receipts are sent
    /// promptly. Every `MAX_CONFIRMATION_DELAY` (50ms) this should be called and if the returned
    /// list is not empty, the list should be sent as receipts immediately in a noop packet.
    pub(super) fn get_receipts(&mut self) -> Vec<MessageId> {
        self.cleanup();

        mem::take(self.pending_receipts.as_mut())
    }

    /// This function cleans up the `message_id_time` and `time_by_message_id` data structures.
    /// It removes entries that are older than `RETAIN_TIME`.
    fn cleanup(&mut self) {
        let remove_before = self.time_source.now() - RETAIN_TIME;
        while self
            .message_id_time
            .front()
            .map_or(false, |&(_, time)| time < remove_before)
        {
            let expired = self.message_id_time.pop_front();
            if let Some((message_id, _)) = expired {
                self.time_by_message_id.remove(&message_id);
            }
        }
        // Note: We deliberately don't clean up the pending_receipts list because it will
        // be emptied every time get_receipts is called.
    }
}

#[derive(Debug, PartialEq)]
pub(super) enum ReportResult {
    /// Packet was received for the first time and recorded
    Ok,

    /// The packet has already been received, it will be re-acknowledged but
    /// should otherwise be ignored
    AlreadyReceived,

    /// The queue is full and receipts must be sent immediately
    QueueFull,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::MockTimeSource;

    #[test]
    fn test_initialization() {
        let mut tracker = ReceivedPacketTracker::new();
        assert_eq!(tracker.get_receipts().len(), 0);
        assert_eq!(tracker.pending_receipts.len(), 0);
        assert_eq!(tracker.time_by_message_id.len(), 0);
    }

    #[test]
    fn test_report_receipt_ok() {
        let mut tracker = ReceivedPacketTracker::new();
        assert_eq!(tracker.report_received_packets(0), ReportResult::Ok);
        assert_eq!(tracker.pending_receipts.len(), 1);
        assert_eq!(tracker.time_by_message_id.len(), 1);
    }

    #[test]
    fn test_report_receipt_already_received() {
        let mut tracker = ReceivedPacketTracker::new();
        assert_eq!(tracker.report_received_packets(0), ReportResult::Ok);
        assert_eq!(
            tracker.report_received_packets(0),
            ReportResult::AlreadyReceived
        );
        assert_eq!(tracker.pending_receipts.len(), 1);
        assert_eq!(tracker.time_by_message_id.len(), 1);
    }

    #[test]
    fn test_report_receipt_queue_full() {
        let mut tracker = ReceivedPacketTracker::new();
        for i in 0..(MAX_PENDING_RECEIPTS - 1) {
            assert_eq!(
                tracker.report_received_packets(i as MessageId),
                ReportResult::Ok
            );
        }
        assert_eq!(
            tracker.report_received_packets((MAX_PENDING_RECEIPTS as MessageId) + 1),
            ReportResult::QueueFull
        );
        assert_eq!(tracker.pending_receipts.len(), MAX_PENDING_RECEIPTS);
        assert_eq!(tracker.time_by_message_id.len(), MAX_PENDING_RECEIPTS);
    }

    #[test]
    fn test_cleanup() {
        let time_source = MockTimeSource::new(Instant::now());

        let mut tracker = ReceivedPacketTracker {
            pending_receipts: Vec::new(),
            message_id_time: VecDeque::new(),
            time_by_message_id: HashMap::new(),
            time_source: time_source.clone(),
        };

        for i in 0..10 {
            assert_eq!(tracker.report_received_packets(i), ReportResult::Ok);
        }
        assert_eq!(tracker.time_by_message_id.len(), 10);
        assert_eq!(tracker.message_id_time.len(), 10);

        time_source.advance_time(RETAIN_TIME + Duration::from_secs(1));

        tracker.cleanup();
        assert_eq!(tracker.time_by_message_id.len(), 0);
        assert_eq!(tracker.message_id_time.len(), 0);
    }
}
