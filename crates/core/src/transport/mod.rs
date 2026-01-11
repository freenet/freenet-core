//! Freenet Transport protocol implementation.
//!
//! Please see `docs/architecture/transport.md` for more information.
//!
//! Provides the low-level network transport abstraction (e.g., UDP).
//! Handles the raw sending and receiving of byte packets over the network.
//! See `architecture.md`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::{borrow::Cow, io, net::SocketAddr};

use futures::Future;
use tokio::net::UdpSocket;

// =============================================================================
// Auto-update version mismatch detection
// =============================================================================
//
// When a peer detects a version mismatch with another peer (typically the gateway),
// it sets this flag. The node's run loop checks this periodically and triggers
// a GitHub check to verify if a newer version is actually available.
//
// This is temporary alpha-testing infrastructure to reduce the burden of
// frequent updates during rapid development.

/// Global flag set by transport layer when a version mismatch is detected.
static VERSION_MISMATCH_DETECTED: AtomicBool = AtomicBool::new(false);

/// Signal that a version mismatch was detected with another peer.
/// Called from the transport layer when a connection fails due to
/// protocol version incompatibility.
pub fn signal_version_mismatch() {
    VERSION_MISMATCH_DETECTED.store(true, Ordering::SeqCst);
}

/// Check if there's a pending version mismatch that should trigger an update check.
pub fn has_version_mismatch() -> bool {
    VERSION_MISMATCH_DETECTED.load(Ordering::SeqCst)
}

/// Clear the version mismatch flag (called after checking for updates).
pub fn clear_version_mismatch() {
    VERSION_MISMATCH_DETECTED.store(false, Ordering::SeqCst);
}

pub mod connection_handler;
mod crypto;
pub mod in_memory_socket;

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

pub mod congestion_control;
pub mod global_bandwidth;
pub(crate) mod ledbat;
pub mod metrics;
mod sent_packet_tracker;
mod symmetric_message;
pub(crate) mod token_bucket;

// Re-export LEDBAT stats for telemetry
pub use ledbat::LedbatStats;
// Re-export congestion control interface
pub use congestion_control::{
    AlgorithmConfig, CongestionControl, CongestionControlAlgorithm, CongestionControlConfig,
    CongestionControlStats, CongestionController,
};
// Re-export transport metrics for periodic telemetry snapshots
pub use metrics::{TransportMetrics, TransportSnapshot, TRANSPORT_METRICS};
// Re-export reset functions for deterministic simulation testing
pub use packet_data::reset_nonce_counter;
pub use peer_connection::StreamId;

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
    /// Final slow start threshold at completion (bytes).
    /// Key diagnostic for death spiral: if ssthresh collapses to min_cwnd,
    /// slow start can't recover useful throughput.
    pub final_ssthresh_bytes: u32,
    /// Effective minimum ssthresh floor (bytes).
    /// This floor prevents ssthresh from collapsing too low during timeouts.
    pub min_ssthresh_floor_bytes: u32,
    /// Total retransmission timeouts (RTO events) during transfer.
    /// High values indicate severe congestion or path issues.
    pub total_timeouts: u32,
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
pub(crate) use self::connection_handler::ExpectedInboundTracker;
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
    PubKeyDecryptionError(#[from] crypto::DecryptionError),
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

// =============================================================================
// Type-erased peer connection trait
// =============================================================================
//
// This trait abstracts over `PeerConnection<S>` to allow the event loop to work
// with connections without being generic over the socket type. This enables
// using the same event loop code for both production (UdpSocket) and testing
// (InMemorySocket) without propagating generics through the entire codebase.

use crate::message::NetMessage;

/// Type-erased interface for peer connections.
///
/// This trait provides the minimal interface needed by `peer_connection_listener`
/// and related event loop code to communicate with a peer. By boxing connections
/// as `Box<dyn PeerConnectionApi>`, we avoid making the event loop generic over
/// the socket type.
pub(crate) trait PeerConnectionApi: Send {
    /// Returns the remote peer's socket address.
    fn remote_addr(&self) -> SocketAddr;

    /// Sends a network message to the remote peer.
    ///
    /// The message is serialized and sent over the transport connection.
    fn send_message(
        &mut self,
        msg: NetMessage,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>>;

    /// Receives raw bytes from the remote peer.
    ///
    /// Returns the deserialized message bytes. The caller is responsible for
    /// deserializing into the appropriate message type.
    fn recv(
        &mut self,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Vec<u8>, TransportError>> + Send + '_>>;
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
        sent_tracker.time_source.advance(effective_rto);
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
