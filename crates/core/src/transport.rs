#![allow(dead_code)] // TODO: Remove before integration
//! Freenet Transport protocol implementation.
//!
//! Please see `docs/architecture/transport.md` for more information.
//!
use std::{borrow::Cow, io, net::SocketAddr};

use futures::Future;
use tokio::net::UdpSocket;

mod connection_handler;
mod crypto;
mod packet_data;
mod peer_connection;
mod rate_limiter;
// todo: optimize trackers
mod received_packet_tracker;
mod sent_packet_tracker;
mod symmetric_message;

type MessagePayload = Vec<u8>;

type PacketId = u32;

use self::{packet_data::PacketData, peer_connection::StreamId};

pub use self::crypto::TransportKeypair;
pub(crate) use self::{
    connection_handler::{create_connection_handler, OutboundConnectionHandler},
    crypto::TransportPublicKey,
    peer_connection::PeerConnection,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum TransportError {
    #[error("transport handler channel closed, socket likely closed")]
    ChannelClosed,
    #[error("connection to remote closed")]
    ConnectionClosed(SocketAddr),
    #[error("failed while establishing connection, reason: {cause}")]
    ConnectionEstablishmentFailure { cause: Cow<'static, str> },
    #[error("incomplete inbound stream: {0}")]
    IncompleteInboundStream(StreamId),
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
    #[error("{0}")]
    PrivateKeyDecryptionError(aes_gcm::aead::Error),
    #[error(transparent)]
    PubKeyDecryptionError(#[from] rsa::errors::Error),
    #[error(transparent)]
    Serialization(#[from] bincode::Error),
    #[error("received unexpected message from remote: {0}")]
    UnexpectedMessage(Cow<'static, str>),
}

/// Make connection handler more testable
pub(crate) trait Socket: Sized + Send + Sync + 'static {
    fn bind(addr: SocketAddr) -> impl Future<Output = io::Result<Self>> + Send;
    fn recv_from(
        &self,
        buf: &mut [u8],
    ) -> impl Future<Output = io::Result<(usize, SocketAddr)>> + Send;
    fn send_to(
        &self,
        buf: &[u8],
        target: SocketAddr,
    ) -> impl Future<Output = io::Result<usize>> + Send;
}

impl Socket for UdpSocket {
    async fn bind(addr: SocketAddr) -> io::Result<Self> {
        Self::bind(addr).await
    }

    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.recv_from(buf).await
    }

    async fn send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        self.send_to(buf, target).await
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
            sent_tracker.report_sent_packet(id, vec![id as u8].into());
        }

        // Simulate receiving some packets
        for id in [1u32, 3, 5] {
            assert_eq!(
                received_tracker.report_received_packet(id),
                ReportResult::Ok
            );
        }

        // Get receipts and simulate acknowledging them
        let receipts = received_tracker.get_receipts();
        assert_eq!(receipts, vec![1u32, 3, 5]);
        sent_tracker.report_received_receipts(&receipts);

        // Check resend action for lost packets
        sent_tracker
            .time_source
            .advance_time(MESSAGE_CONFIRMATION_TIMEOUT);
        for id in [2, 4] {
            match sent_tracker.get_resend() {
                ResendAction::Resend(packet_id, packet) => {
                    assert_eq!(packet_id, id);
                    // Simulate resending packet
                    sent_tracker.report_sent_packet(id, packet);
                }
                _ => panic!("Expected resend action for packet {}", id),
            }
        }
    }
}
