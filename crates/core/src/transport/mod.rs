//! Freenet Transport protocol implementation.
//!
//! Please see `docs/architecture/transport.md` for more information.
//!
//! Provides the low-level network transport abstraction (e.g., UDP).
//! Handles the raw sending and receiving of byte packets over the network.
//! See `architecture.md`.

use std::{borrow::Cow, io, net::SocketAddr};

use futures::Future;
use tokio::net::UdpSocket;

pub mod connection_handler;
mod crypto;

/// Mock transport infrastructure for testing and benchmarking.
/// Provides MockSocket and helper functions to create mock peer connections
/// without real network I/O.
#[cfg(any(test, feature = "bench"))]
pub use connection_handler::mock_transport;
/// High-performance channels for transport layer.
pub mod fast_channel;
mod packet_data;
pub mod peer_connection;
// todo: optimize trackers
mod received_packet_tracker;

pub mod global_bandwidth;
pub(crate) mod ledbat;
mod sent_packet_tracker;
mod symmetric_message;
pub(crate) mod token_bucket;

// Re-export LEDBAT stats for telemetry
pub use ledbat::LedbatStats;

use std::time::Duration;

/// Statistics from a completed stream transfer.
///
/// Provides metrics for telemetry including LEDBAT congestion control data.
/// Used to emit TransferEvent telemetry when streams complete.
#[derive(Debug, Clone)]
pub struct TransferStats {
    /// Unique stream identifier.
    pub stream_id: u64,
    /// Remote peer address.
    pub remote_addr: SocketAddr,
    /// Total bytes transferred.
    pub bytes_transferred: u64,
    /// Time elapsed from start to completion.
    pub elapsed: Duration,
    /// Peak congestion window during transfer (bytes).
    pub peak_cwnd_bytes: u32,
    /// Final congestion window at completion (bytes).
    pub final_cwnd_bytes: u32,
    /// Number of LEDBAT slowdowns triggered during transfer.
    pub slowdowns_triggered: u32,
    /// Minimum observed RTT (base delay) at completion.
    pub base_delay: Duration,
}

impl TransferStats {
    /// Calculate average throughput in bytes per second.
    pub fn avg_throughput_bps(&self) -> u64 {
        if self.elapsed.is_zero() {
            return 0;
        }
        (self.bytes_transferred as f64 / self.elapsed.as_secs_f64()) as u64
    }
}

type MessagePayload = bytes::Bytes;

type PacketId = u32;

/// A wrapper around SocketAddr that represents an address observed at the transport layer.
/// This is the "ground truth" for NAT scenarios - it's the actual address we see
/// at the network layer, not what the peer claims in protocol messages.
///
/// Using a newtype instead of raw `SocketAddr` makes the address semantics explicit
/// and prevents accidental confusion with advertised/claimed addresses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ObservedAddr(SocketAddr);

impl ObservedAddr {
    /// Get the underlying socket address.
    pub fn socket_addr(&self) -> SocketAddr {
        self.0
    }
}

impl std::fmt::Display for ObservedAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<SocketAddr> for ObservedAddr {
    fn from(addr: SocketAddr) -> Self {
        Self(addr)
    }
}

pub(crate) use self::connection_handler::create_connection_handler;
pub use self::crypto::{TransportKeypair, TransportPublicKey};
pub use self::{
    connection_handler::{InboundConnectionHandler, OutboundConnectionHandler},
    peer_connection::PeerConnection,
};

// Streaming infrastructure (Phase 1)
pub use self::peer_connection::{
    streaming::{StreamError, StreamHandle, StreamRegistry, StreamingInboundStream},
    streaming_buffer::{InsertError, LockFreeStreamBuffer},
};

#[derive(Debug, thiserror::Error)]
#[cfg_attr(feature = "bench", allow(dead_code))]
pub enum TransportError {
    #[error("transport handler channel closed, socket likely closed")]
    ChannelClosed,
    #[error("connection to remote closed")]
    ConnectionClosed(SocketAddr),
    #[error("failed while establishing connection, reason: {cause}")]
    ConnectionEstablishmentFailure { cause: Cow<'static, str> },
    #[error("Version incompatibility with gateway\n  Your client version: {actual}\n  Gateway version: {expected}\n  \n  To fix this, update your Freenet client:\n    cargo install --force freenet --version {expected}\n  \n  Or if building from source:\n    git pull && cargo install --path crates/core")]
    ProtocolVersionMismatch {
        expected: String,
        actual: &'static str,
    },
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
    #[error(transparent)]
    PubKeyDecryptionError(#[from] rsa::errors::Error),
    #[error(transparent)]
    Serialization(#[from] bincode::Error),
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

    /// Synchronous send for use in blocking contexts (e.g., spawn_blocking).
    /// For UDP, this typically succeeds immediately since there's no flow control.
    /// Used by MockSocket implementations in tests.
    #[allow(dead_code)]
    fn send_to_blocking(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize>;
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

    fn send_to_blocking(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        // try_send_to is synchronous and for UDP typically succeeds immediately.
        // However, under high load the kernel buffer might be full, returning WouldBlock.
        // In that case, we retry with exponential backoff since we're in a blocking context.
        let mut backoff_us = 1u64; // Start at 1μs
        const MAX_BACKOFF_US: u64 = 1000; // Cap at 1ms

        loop {
            match self.try_send_to(buf, target) {
                Ok(n) => return Ok(n),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // Kernel buffer full - exponential backoff
                    std::thread::sleep(std::time::Duration::from_micros(backoff_us));
                    backoff_us = (backoff_us * 2).min(MAX_BACKOFF_US);
                }
                Err(e) => return Err(e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::received_packet_tracker::ReportResult;
    use crate::transport::sent_packet_tracker::ResendAction;

    #[test]
    fn test_packet_send_receive_acknowledge_flow() {
        let mut sent_tracker = sent_packet_tracker::tests::mock_sent_packet_tracker();
        let mut received_tracker = received_packet_tracker::tests::mock_received_packet_tracker();

        // Capture effective RTO before sending (packets use this for timeout)
        let effective_rto = sent_tracker.effective_rto();

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
        let _ = sent_tracker.report_received_receipts(&receipts);

        // Check resend action for lost packets
        // Packets now use effective_rto() for timeout (not MESSAGE_CONFIRMATION_TIMEOUT)
        sent_tracker.time_source.advance_time(effective_rto);
        for id in [2, 4] {
            match sent_tracker.get_resend() {
                ResendAction::Resend(packet_id, packet) => {
                    assert_eq!(packet_id, id);
                    // Simulate resending packet
                    sent_tracker.report_sent_packet(id, packet);
                }
                _ => panic!("Expected resend action for packet {id}"),
            }
        }
    }
}
