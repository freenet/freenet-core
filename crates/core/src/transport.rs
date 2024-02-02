#![allow(dead_code)] // TODO: Remove before integration
//! Freenet Transport protocol implementation.
//!
//! Please see `docs/architecture/transport.md` for more information.

mod bw;
mod connection_handler;
mod crypto;
mod packet_data;
mod peer_connection;
mod received_packet_tracker;
mod sent_packet_tracker;
mod symmetric_message;

type MessagePayload = Vec<u8>;
type MessageId = u32;

use self::packet_data::PacketData;
use std::time::Duration;

/// We can wait up to 100ms to confirm a message was received, this allows us to batch
/// receipts together and send them in a single message.
const MAX_CONFIRMATION_DELAY: Duration = Duration::from_millis(100);

struct BytesPerSecond(f64);

impl BytesPerSecond {
    pub fn new(bytes_per_second: f64) -> Self {
        assert!(bytes_per_second >= 0.0);
        Self(bytes_per_second)
    }

    pub fn as_f64(&self) -> f64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::received_packet_tracker::ReportResult;
    use crate::transport::sent_packet_tracker::{ResendAction, MESSAGE_CONFIRMATION_TIMEOUT};

    #[test]
    fn test_packet_send_receive_acknowledge_flow() {
        let mut sent_tracker = sent_packet_tracker::tests::mock_sent_packet_tracker();
        let mut received_tracker = received_packet_tracker::tests::mock_received_packet_tracker();

        // Simulate sending packets
        for id in 1..=5 {
            sent_tracker.report_sent_packet(id, vec![id as u8]);
        }

        // Simulate receiving some packets
        for id in [1, 3, 5] {
            assert_eq!(
                received_tracker.report_received_packet(id),
                ReportResult::Ok
            );
        }

        // Get receipts and simulate acknowledging them
        let receipts = received_tracker.get_receipts();
        assert_eq!(receipts, vec![1, 3, 5]);
        sent_tracker.report_received_receipts(&receipts);

        // Check resend action for lost packets
        sent_tracker
            .time_source
            .advance_time(MESSAGE_CONFIRMATION_TIMEOUT);
        for id in [2, 4] {
            match sent_tracker.get_resend() {
                ResendAction::Resend(message_id, packet) => {
                    assert_eq!(message_id, id);
                    // Simulate resending packet
                    sent_tracker.report_sent_packet(id, packet);
                }
                _ => panic!("Expected resend action for packet {}", id),
            }
        }
    }
}
