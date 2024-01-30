use crate::util::{SystemTime, TimeSource};
use std::collections::{HashMap, VecDeque};
use std::mem;
use time::{Duration, Instant};

const RETAIN_TIME: Duration = Duration::seconds(10);
const MAX_PENDING_RECEIPTS: usize = 20;

pub(super) struct ReceiptTracker<T: TimeSource> {
    pending_receipts: Vec<u16>,
    packet_id_time: VecDeque<(u16, Instant)>,
    time_by_packet: HashMap<u16, Instant>,
    time_source: T,
}

impl ReceiptTracker<SystemTime> {
    pub(super) fn new() -> Self {
        ReceiptTracker {
            pending_receipts: Vec::new(),
            packet_id_time: VecDeque::new(),
            time_by_packet: HashMap::new(),
            time_source: SystemTime,
        }
    }
}

impl<T: TimeSource> ReceiptTracker<T> {
    pub(super) fn report_received_packets(&mut self, packet_id: u16) -> ReportResult {
        self.cleanup();
        if self.time_by_packet.contains_key(&packet_id) {
            ReportResult::AlreadyReceived
        } else {
            self.time_by_packet
                .insert(packet_id, self.time_source.now());
            self.packet_id_time
                .push_back((packet_id, self.time_source.now()));

            if self.pending_receipts.len() < MAX_PENDING_RECEIPTS {
                self.pending_receipts.push(packet_id);
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
    pub(super) fn get_receipts(&mut self) -> Vec<u16> {
        self.cleanup();

        mem::take(self.pending_receipts.as_mut())
    }

    fn cleanup(&mut self) {
        let remove_before = self.time_source.now() - RETAIN_TIME;
        while self
            .packet_id_time
            .front()
            .map_or(false, |&(_, time)| time < remove_before)
        {
            let expired = self.packet_id_time.pop_front();
            if let Some((packet_id, _)) = expired {
                self.time_by_packet.remove(&packet_id);
            }
        }
    }
}

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
mod tests {}
