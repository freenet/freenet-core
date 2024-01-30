use crate::util::{SystemTime, TimeSource};
use std::collections::{HashMap, VecDeque};
use std::mem;
use time::{Duration, Instant};

const RETAIN_TIME: Duration = Duration::seconds(10);
const MAX_PENDING_RECEIPTS: usize = 20;

pub(super) struct ReceiptTracker<T: TimeSource> {
    pending_receipts: Vec<u32>,
    message_id_time: VecDeque<(u32, Instant)>,
    time_by_message_id: HashMap<u32, Instant>,
    time_source: T,
}

impl ReceiptTracker<SystemTime> {
    pub(super) fn new() -> Self {
        ReceiptTracker {
            pending_receipts: Vec::new(),
            message_id_time: VecDeque::new(),
            time_by_message_id: HashMap::new(),
            time_source: SystemTime,
        }
    }
}

impl<T: TimeSource> ReceiptTracker<T> {
    pub(super) fn report_received_packets(&mut self, message_id: u32) -> ReportResult {
        self.cleanup();
        if self.time_by_message_id.contains_key(&message_id) {
            ReportResult::AlreadyReceived
        } else {
            self.time_by_message_id
                .insert(message_id, self.time_source.now());
            self.message_id_time
                .push_back((message_id, self.time_source.now()));

            if self.pending_receipts.len() < MAX_PENDING_RECEIPTS {
                self.pending_receipts.push(message_id);
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
    pub(super) fn get_receipts(&mut self) -> Vec<u32> {
        self.cleanup();

        mem::take(self.pending_receipts.as_mut())
    }

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
    }
}

#[derive(Debug, PartialEq)]
enum ReportResult {
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
        let mut tracker = ReceiptTracker::new();
        assert_eq!(tracker.get_receipts().len(), 0);
        assert_eq!(tracker.pending_receipts.len(), 0);
        assert_eq!(tracker.time_by_message_id.len(), 0);
    }

    #[test]
    fn test_report_receipt_ok() {
        let mut tracker = ReceiptTracker::new();
        assert_eq!(tracker.report_received_packets(0), ReportResult::Ok);
        assert_eq!(tracker.pending_receipts.len(), 1);
        assert_eq!(tracker.time_by_message_id.len(), 1);
    }
}
