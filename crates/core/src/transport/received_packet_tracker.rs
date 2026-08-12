use crate::transport::PacketId;
use crate::util::time_source::{InstantTimeSrc, TimeSource};
use std::collections::{HashMap, VecDeque};
use std::mem;
use std::time::Duration;
use tokio::time::Instant;

/// How long to retain packets in case they need to be retransmitted
const RETAIN_TIME: Duration = Duration::from_secs(60);

/// Maximum number of pending receipts before forcing an ACK send.
/// When this many packets have been received without sending ACKs,
/// the receiver will send a NOOP with all pending receipts.
#[cfg(test)]
pub(crate) const MAX_PENDING_RECEIPTS: usize = 20;
#[cfg(not(test))]
const MAX_PENDING_RECEIPTS: usize = 20;

/// This struct is responsible for tracking received packets and deciding when to send receipts
/// from/to a specific peer.
///
/// The caller must report when packets are received using the `report_received_packets` method.
/// The caller must also call `get_receipts` periodically to check if any receipts need to be sent.
///
/// `get_receipts` should be called whenever a packet is sent.
///
/// `get_receipts` must **also** be called every `MAX_CONFIRMATION_DELAY` (100ms) and
/// if the returned list is not empty, the list should be sent as receipts immediately in a noop.
/// This may look something like this:
///
/// ```ignore
/// use super::MAX_CONFIRMATION_DELAY;
/// use std::thread::sleep;
/// let mut received_packet_tracker = todo!();
/// loop {
///    let receipts = received_packet_tracker.get_receipts();
///     if !receipts.is_empty() {
///        // Send receipts in a noop message
///     }
///     sleep(MAX_CONFIRMATION_DELAY);
///  }
/// ```
pub(super) struct ReceivedPacketTracker<T: TimeSource> {
    pending_receipts: Vec<PacketId>,
    packet_id_time: VecDeque<(PacketId, Instant)>,
    time_by_packet_id: HashMap<PacketId, Instant>,
    time_source: T,
}

impl ReceivedPacketTracker<InstantTimeSrc> {
    pub(super) fn new() -> Self {
        ReceivedPacketTracker {
            pending_receipts: Vec::new(),
            packet_id_time: VecDeque::new(),
            time_by_packet_id: HashMap::new(),
            time_source: InstantTimeSrc::new(),
        }
    }
}

impl<T: TimeSource> ReceivedPacketTracker<T> {
    pub(super) fn report_received_packet(&mut self, packet_id: PacketId) -> ReportResult {
        self.cleanup();
        let current_time = self.time_source.now();

        match self.time_by_packet_id.entry(packet_id) {
            std::collections::hash_map::Entry::Occupied(_) => {
                // Re-acknowledge. A duplicate almost always means the sender
                // retransmitted because our original receipt never made it back:
                // receipts ride on noop/piggybacked packets, which are themselves
                // unreliable and are never retransmitted. Staying silent here left
                // the sender with no way to ever learn the packet arrived, so its
                // bytes stayed in flight until the retransmit budget ran out. With
                // a full congestion window that is fatal well before then: the
                // stream blocks in cwnd wait and aborts after CWND_WAIT_TIMEOUT
                // (3s), which the receiver sees as a transfer that simply stopped
                // mid-stream. Re-acknowledging is exactly what the retransmit is
                // asking for, and it is the only signal that can drain the window.
                //
                // Deduplicated because a burst of retransmits for the same id would
                // otherwise queue the same receipt repeatedly; the sender only needs
                // to be told once per flush.
                if !self.pending_receipts.contains(&packet_id) {
                    self.pending_receipts.push(packet_id);
                }
                // Deliberately still `AlreadyReceived` rather than `QueueFull`: the
                // caller relies on this variant to skip re-processing a payload it
                // has already handled. The re-queued receipt goes out on the next
                // background ACK tick or the next packet that flushes receipts.
                ReportResult::AlreadyReceived
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(current_time);
                self.packet_id_time.push_back((packet_id, current_time));
                self.pending_receipts.push(packet_id);

                if self.pending_receipts.len() < MAX_PENDING_RECEIPTS {
                    ReportResult::Ok
                } else {
                    ReportResult::QueueFull
                }
            }
        }
    }

    /// Returns a list of packets that have been received since the last call to this function.
    /// This should be called every time a packet is sent to ensure that receipts are sent
    /// promptly. Every `MAX_CONFIRMATION_DELAY` (100ms) this should be called and if the returned
    /// list is not empty, the list should be sent as receipts immediately in a noop packet.
    pub(super) fn get_receipts(&mut self) -> Vec<PacketId> {
        self.cleanup();

        mem::take(self.pending_receipts.as_mut())
    }

    /// This function cleans up the `packet_id_time` and `time_by_packet_id` data structures.
    /// It removes entries that are older than `RETAIN_TIME`.
    fn cleanup(&mut self) {
        let remove_before = self.time_source.now() - RETAIN_TIME;
        while self
            .packet_id_time
            .front()
            .is_some_and(|&(_, time)| time < remove_before)
        {
            let expired = self.packet_id_time.pop_front();
            if let Some((packet_id, _)) = expired {
                self.time_by_packet_id.remove(&packet_id);
            }
        }
        // Note: We deliberately don't clean up the pending_receipts list because it will
        // be emptied every time get_receipts is called.
    }
}

#[must_use]
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
pub(in crate::transport) mod tests {
    use super::*;
    use crate::util::time_source::MockTimeSource;

    pub(in crate::transport) fn mock_received_packet_tracker()
    -> ReceivedPacketTracker<MockTimeSource> {
        ReceivedPacketTracker {
            pending_receipts: Vec::new(),
            packet_id_time: VecDeque::new(),
            time_by_packet_id: HashMap::new(),
            time_source: MockTimeSource::new(Instant::now()),
        }
    }

    #[test]
    fn test_initialization() {
        let mut tracker = ReceivedPacketTracker {
            pending_receipts: Vec::new(),
            packet_id_time: VecDeque::new(),
            time_by_packet_id: HashMap::new(),
            time_source: MockTimeSource::new(Instant::now()),
        };

        assert_eq!(tracker.get_receipts().len(), 0);
        assert_eq!(tracker.pending_receipts.len(), 0);
        assert_eq!(tracker.time_by_packet_id.len(), 0);
    }

    #[test]
    fn test_report_receipt_ok() {
        let mut tracker = ReceivedPacketTracker {
            pending_receipts: Vec::new(),
            packet_id_time: VecDeque::new(),
            time_by_packet_id: HashMap::new(),
            time_source: MockTimeSource::new(Instant::now()),
        };

        assert_eq!(tracker.report_received_packet(0), ReportResult::Ok);
        assert_eq!(tracker.pending_receipts.len(), 1);
        assert_eq!(tracker.time_by_packet_id.len(), 1);
    }

    #[test]
    fn test_report_receipt_already_received() {
        let mut tracker = mock_received_packet_tracker();

        assert_eq!(tracker.report_received_packet(0), ReportResult::Ok);
        assert_eq!(
            tracker.report_received_packet(0),
            ReportResult::AlreadyReceived
        );
        // Still one pending receipt, not two: the duplicate must not queue a
        // second copy of a receipt that has not been sent yet.
        assert_eq!(tracker.pending_receipts.len(), 1);
        assert_eq!(tracker.time_by_packet_id.len(), 1);
    }

    /// A retransmitted packet must be acknowledged again, even though its
    /// payload is ignored.
    ///
    /// This is the production scenario, not a synthetic one. The sender only
    /// retransmits because it saw no receipt, and the overwhelmingly likely
    /// reason is that the receipt itself was lost -- receipts travel on noop /
    /// piggybacked packets, which are never retransmitted. If the duplicate is
    /// met with silence, that packet's bytes stay in the sender's flight
    /// accounting with no remaining way to clear them. Once flight reaches the
    /// congestion window the sender blocks, and `CWND_WAIT_TIMEOUT` (3s) aborts
    /// the whole stream. Downstream this surfaces as a multi-fragment GET dying
    /// partway through with a stream-assembly inactivity timeout.
    ///
    /// Note the drain between the two reports: that is what makes this test
    /// discriminating. Without it the receipt from the *first* report is still
    /// queued, so the assertion would pass whether or not the duplicate
    /// re-acknowledges anything.
    #[test]
    fn retransmitted_packet_is_reacknowledged_after_its_receipt_was_sent() {
        let mut tracker = mock_received_packet_tracker();

        assert_eq!(tracker.report_received_packet(7), ReportResult::Ok);
        // The receipt is handed to the transport, which puts it on the wire --
        // where it is lost. Nothing is pending any more.
        assert_eq!(tracker.get_receipts(), vec![7]);
        assert!(tracker.pending_receipts.is_empty());

        // The sender, having heard nothing, retransmits.
        assert_eq!(
            tracker.report_received_packet(7),
            ReportResult::AlreadyReceived
        );

        assert_eq!(
            tracker.get_receipts(),
            vec![7],
            "a retransmit must be re-acknowledged, otherwise the sender can \
             never drain this packet from its congestion window"
        );
    }

    /// A burst of retransmits for the same packet must not queue the same
    /// receipt several times over -- one flush tells the sender everything it
    /// needs to know, and the receipt list is a fixed-capacity signal.
    #[test]
    fn repeated_retransmits_queue_one_receipt_per_flush() {
        let mut tracker = mock_received_packet_tracker();

        assert_eq!(tracker.report_received_packet(3), ReportResult::Ok);
        assert_eq!(tracker.get_receipts(), vec![3]);

        for _ in 0..5 {
            assert_eq!(
                tracker.report_received_packet(3),
                ReportResult::AlreadyReceived
            );
        }

        assert_eq!(tracker.get_receipts(), vec![3]);
    }

    #[test]
    fn test_report_receipt_queue_full() {
        let mut tracker = ReceivedPacketTracker {
            pending_receipts: Vec::new(),
            packet_id_time: VecDeque::new(),
            time_by_packet_id: HashMap::new(),
            time_source: MockTimeSource::new(Instant::now()),
        };

        for i in 0..(MAX_PENDING_RECEIPTS - 1) {
            assert_eq!(
                tracker.report_received_packet(i as PacketId),
                ReportResult::Ok
            );
        }
        assert_eq!(
            tracker.report_received_packet((MAX_PENDING_RECEIPTS as PacketId) + 1),
            ReportResult::QueueFull
        );
        assert_eq!(tracker.pending_receipts.len(), MAX_PENDING_RECEIPTS);
        assert_eq!(tracker.time_by_packet_id.len(), MAX_PENDING_RECEIPTS);
    }

    #[test]
    fn test_cleanup() {
        let mut tracker = ReceivedPacketTracker {
            pending_receipts: Vec::new(),
            packet_id_time: VecDeque::new(),
            time_by_packet_id: HashMap::new(),
            time_source: MockTimeSource::new(Instant::now()),
        };

        for i in 0..10 {
            assert_eq!(tracker.report_received_packet(i), ReportResult::Ok);
        }
        assert_eq!(tracker.time_by_packet_id.len(), 10);
        assert_eq!(tracker.packet_id_time.len(), 10);

        tracker
            .time_source
            .advance_time(RETAIN_TIME + Duration::from_secs(1));

        tracker.cleanup();
        assert_eq!(tracker.time_by_packet_id.len(), 0);
        assert_eq!(tracker.packet_id_time.len(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_many_trackers() {
        let mut trackers = vec![];
        for _ in 1..100 {
            trackers.push(ReceivedPacketTracker::new());
        }
    }
}
