//! Freenet Transport protocol implementation.
//!
//! Please see `docs/architecture/transport.md` for more information.
//!
//! Provides the low-level network transport abstraction (e.g., UDP).
//! Handles the raw sending and receiving of byte packets over the network.
//! See `architecture.md`.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
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

/// Generation counter incremented on each `signal_version_mismatch()` call.
/// The update check task uses this to detect fresh mismatches and reset backoff.
static VERSION_MISMATCH_GENERATION: AtomicU64 = AtomicU64::new(0);

/// Signal that a version mismatch was detected with another peer.
/// Called from the transport layer when a connection fails due to
/// protocol version incompatibility.
pub fn signal_version_mismatch() {
    VERSION_MISMATCH_DETECTED.store(true, Ordering::SeqCst);
    VERSION_MISMATCH_GENERATION.fetch_add(1, Ordering::SeqCst);
}

/// Get the current mismatch generation counter.
/// Each call to `signal_version_mismatch()` increments this.
pub fn version_mismatch_generation() -> u64 {
    VERSION_MISMATCH_GENERATION.load(Ordering::SeqCst)
}

/// Check if there's a pending version mismatch that should trigger an update check.
pub fn has_version_mismatch() -> bool {
    VERSION_MISMATCH_DETECTED.load(Ordering::SeqCst)
}

/// Clear the version mismatch flag (called after checking for updates).
pub fn clear_version_mismatch() {
    VERSION_MISMATCH_DETECTED.store(false, Ordering::SeqCst);
}

// =============================================================================
// Decentralized version discovery
// =============================================================================
//
// Peers learn about newer versions from successful handshakes with neighbors.
// HIGHEST_SEEN_VERSION tracks the highest version seen in the network.
// URGENT_UPDATE_NEEDED is set when a remote's min_compatible > our version
// (meaning we MUST update to remain connected).

/// Highest version observed from any peer during handshake, with the number
/// of handshakes that reported it.
static HIGHEST_SEEN_VERSION: Mutex<Option<HighestSeenVersion>> = Mutex::new(None);

/// Set when a remote peer's min_compatible version exceeds our version,
/// meaning we cannot connect to that peer without updating.
static URGENT_UPDATE_NEEDED: AtomicBool = AtomicBool::new(false);

/// Minimum number of handshakes that must report a version before we
/// trust it for the stagger timer. This is a lightweight defense against
/// a single transient bad report — it does not track peer identity, so a
/// persistent attacker reconnecting can bypass it. The real protection is
/// that GitHub verification is always required before exit-42.
const MIN_VERSION_REPORTERS: usize = 2;

#[derive(Clone, Copy)]
struct HighestSeenVersion {
    version: (u8, u8, u16),
    reporter_count: usize,
}

/// Called on every successful handshake to track the highest version seen.
///
/// Lightweight defense: at least `MIN_VERSION_REPORTERS` handshakes must
/// report the same version before `get_highest_seen_version()` returns it.
/// This catches single transient bad reports. A persistent attacker can
/// bypass this by reconnecting, but the real protection is that the update
/// check task always verifies against GitHub before triggering exit-42.
pub fn report_peer_version(version: (u8, u8, u16)) {
    // Recover from mutex poisoning — version tracking is best-effort and
    // must not permanently break if another thread panicked while holding it.
    let mut guard = HIGHEST_SEEN_VERSION
        .lock()
        .unwrap_or_else(|e| e.into_inner());

    match *guard {
        None => {
            *guard = Some(HighestSeenVersion {
                version,
                reporter_count: 1,
            });
        }
        Some(ref mut current) => {
            if version > current.version {
                // New highest — reset count since this is a different version.
                *current = HighestSeenVersion {
                    version,
                    reporter_count: 1,
                };
            } else if version == current.version {
                current.reporter_count = current.reporter_count.saturating_add(1);
            }
            // version < current: ignore
        }
    }
}

/// Get the highest version seen from peers, if enough peers have reported it.
///
/// Returns `None` until at least `MIN_VERSION_REPORTERS` peers have reported
/// the same highest version. This prevents a single malicious peer from
/// triggering update checks by advertising a fake high version.
pub fn get_highest_seen_version() -> Option<(u8, u8, u16)> {
    let guard = HIGHEST_SEEN_VERSION
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    guard.and_then(|hsv| {
        if hsv.reporter_count >= MIN_VERSION_REPORTERS {
            Some(hsv.version)
        } else {
            None
        }
    })
}

/// Reset version discovery state (test only).
#[cfg(test)]
fn reset_version_discovery() {
    let mut guard = HIGHEST_SEEN_VERSION
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    *guard = None;
}

/// Called when a version mismatch indicates we are too old
/// (remote's min_compatible > our version). This is a breaking change
/// that requires immediate update.
pub fn signal_urgent_update() {
    URGENT_UPDATE_NEEDED.store(true, Ordering::SeqCst);
    // Also trigger the legacy mismatch path
    signal_version_mismatch();
}

/// Check if an urgent (breaking) update is needed.
pub fn is_urgent_update() -> bool {
    URGENT_UPDATE_NEEDED.load(Ordering::SeqCst)
}

/// Clear the urgent update flag.
pub fn clear_urgent_update() {
    URGENT_UPDATE_NEEDED.store(false, Ordering::SeqCst);
}

/// Global counter of open ring connections, updated by `connection_maintenance`.
/// The update check task reads this to decide whether to trust a gateway
/// version mismatch signal when the GitHub check keeps failing.
static OPEN_CONNECTION_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Update the global open connection count (called from `connection_maintenance`).
pub fn set_open_connection_count(count: usize) {
    OPEN_CONNECTION_COUNT.store(count, Ordering::SeqCst);
}

/// Get the current open connection count.
pub fn get_open_connection_count() -> usize {
    OPEN_CONNECTION_COUNT.load(Ordering::SeqCst)
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

pub(crate) mod bbr;
pub mod congestion_control;
pub(crate) mod fixed_rate;
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
/// Provides metrics for telemetry including congestion control data.
/// Used to emit TransferEvent telemetry when streams complete.
#[derive(Debug, Clone)]
pub struct TransferStats {
    /// Unique stream identifier.
    pub stream_id: u64,
    /// Remote peer address.
    pub remote_addr: SocketAddr,
    /// Total bytes transferred.
    pub bytes_transferred: u64,
    /// Time elapsed from start to transmission completion.
    /// Note: This is when we finished SENDING, not when all ACKs arrived.
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
    /// Bytes still in flight (sent but not yet ACKed) when transmission completed.
    /// High values relative to bytes_transferred indicate ACK lag.
    /// This helps estimate the gap between transmission end and final ACK.
    pub final_flightsize: u32,
    /// Configured transmission rate (bytes/sec) for fixed-rate controller.
    /// Zero for adaptive algorithms (BBR, LEDBAT).
    pub configured_rate: u32,
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

pub use self::connection_handler::create_connection_handler;
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
    ProtocolVersionMismatch { expected: String, actual: String },
    #[error("send to {0} failed: {1}")]
    SendFailed(SocketAddr, std::io::ErrorKind),
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
    #[error(transparent)]
    PubKeyDecryptionError(#[from] crypto::DecryptionError),
    #[error(transparent)]
    Serialization(#[from] bincode::Error),
}

impl TransportError {
    /// Returns true if this error is a transient UDP send failure that should
    /// not kill the connection. The idle timeout is the sole authority on
    /// connection liveness.
    pub fn is_transient_send_failure(&self) -> bool {
        matches!(self, TransportError::SendFailed(..))
    }
}

/// Socket trait for abstracting UDP communication.
///
/// This trait allows the transport layer to work with both real UDP sockets
/// and mock sockets for testing.
pub trait Socket: Sized + Send + Sync + 'static {
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
        // Use socket2 to configure dual-stack before binding, then convert
        // to a tokio UdpSocket. This ensures IPv6 sockets accept IPv4 too.
        let domain = if addr.is_ipv6() {
            socket2::Domain::IPV6
        } else {
            socket2::Domain::IPV4
        };
        let sock = socket2::Socket::new(domain, socket2::Type::DGRAM, Some(socket2::Protocol::UDP))
            .map_err(|e| io::Error::new(e.kind(), format!("Failed to create UDP socket: {e}")))?;
        if addr.is_ipv6() {
            // Dual-stack: accept both IPv4 (mapped as ::ffff:x.x.x.x) and IPv6
            sock.set_only_v6(false)?;
        }
        sock.set_nonblocking(true)?;
        sock.bind(&addr.into()).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("Failed to bind UDP socket to {addr}: {e}"),
            )
        })?;
        let std_socket: std::net::UdpSocket = sock.into();
        Self::from_std(std_socket)
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

    /// Sets the orphan stream registry for handling race conditions between
    /// stream fragments and metadata messages (RequestStreaming/ResponseStreaming).
    ///
    /// This should be called by the node layer after connection establishment,
    /// before any messages are processed.
    fn set_orphan_stream_registry(
        &mut self,
        registry: std::sync::Arc<crate::operations::orphan_streams::OrphanStreamRegistry>,
    );

    /// Sends raw stream data to the remote peer using the given operations-level StreamId.
    ///
    /// The data is fragmented and sent via the transport's outbound stream mechanism,
    /// using the provided `stream_id` (which should have the operations marker bit set)
    /// so that the receiver routes fragments through the orphan registry instead of
    /// the legacy InboundStream decode path.
    ///
    /// If `completion_tx` is provided, it will be signaled when the stream transfer
    /// completes (success or failure). Used by the broadcast queue to track when the
    /// actual data transfer finishes, not just when the send is enqueued.
    fn send_stream_data(
        &mut self,
        stream_id: crate::transport::peer_connection::StreamId,
        data: bytes::Bytes,
        metadata: Option<bytes::Bytes>,
        completion_tx: Option<tokio::sync::oneshot::Sender<()>>,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>>;

    /// Pipes an inbound stream to the remote peer, forwarding fragments as they arrive.
    ///
    /// Unlike `send_stream_data` which takes complete data and fragments it, this reads
    /// from an existing `StreamHandle` and forwards each fragment incrementally without
    /// waiting for full reassembly. This enables low-latency forwarding at intermediate nodes.
    ///
    /// The `outbound_stream_id` should be created via `StreamId::next_operations()` for
    /// operations-level routing at the receiver.
    fn pipe_stream_data(
        &mut self,
        outbound_stream_id: crate::transport::peer_connection::StreamId,
        inbound_handle: crate::transport::peer_connection::streaming::StreamHandle,
        metadata: Option<bytes::Bytes>,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>>;
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
                ResendAction::WaitUntil(_) | ResendAction::TlpProbe(..) => {
                    panic!("Expected resend action for packet {id}")
                }
            }
        }
    }
}

#[cfg(test)]
mod version_discovery_tests {
    use super::*;

    // These tests mutate global statics and must run sequentially.
    // Use `cargo test -- --test-threads=1 version_discovery` if running standalone.

    #[test]
    fn single_reporter_not_trusted() {
        reset_version_discovery();
        report_peer_version((0, 1, 153));
        // Only 1 reporter — should not be trusted yet
        assert_eq!(get_highest_seen_version(), None);
    }

    #[test]
    fn two_reporters_trusted() {
        reset_version_discovery();
        report_peer_version((0, 1, 153));
        report_peer_version((0, 1, 153));
        assert_eq!(get_highest_seen_version(), Some((0, 1, 153)));
    }

    #[test]
    fn higher_version_resets_count() {
        reset_version_discovery();
        report_peer_version((0, 1, 153));
        report_peer_version((0, 1, 153)); // count=2, now trusted
        assert_eq!(get_highest_seen_version(), Some((0, 1, 153)));

        // New higher version resets count
        report_peer_version((0, 1, 154));
        // Only 1 reporter for 154 — not trusted yet
        assert_eq!(get_highest_seen_version(), None);

        report_peer_version((0, 1, 154));
        assert_eq!(get_highest_seen_version(), Some((0, 1, 154)));
    }

    #[test]
    fn major_minor_bumps_accepted() {
        reset_version_discovery();
        report_peer_version((1, 0, 0));
        report_peer_version((1, 0, 0));
        assert_eq!(get_highest_seen_version(), Some((1, 0, 0)));
    }
}

#[cfg(test)]
mod dual_stack_tests {
    use super::*;
    use std::net::{Ipv6Addr, SocketAddr};

    /// Verify that binding to [::]:0 creates a dual-stack UDP socket that
    /// can receive IPv4 traffic (via IPv4-mapped addresses).
    #[tokio::test]
    async fn udp_dual_stack_accepts_ipv4() {
        let dual_addr = SocketAddr::new(std::net::IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0);
        let dual_sock = <UdpSocket as Socket>::bind(dual_addr)
            .await
            .expect("bind to [::]:0 should succeed");
        let bound_port = dual_sock.local_addr().unwrap().port();

        // Send a packet from an IPv4 socket to the dual-stack socket
        let v4_sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target = SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            bound_port,
        );
        v4_sender
            .send_to(b"hello", target)
            .await
            .expect("send from IPv4 should succeed");

        let mut buf = [0u8; 16];
        let (len, _src) = dual_sock.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..len], b"hello");
    }

    /// Verify that binding to [::]:0 also accepts IPv6 traffic.
    #[tokio::test]
    async fn udp_dual_stack_accepts_ipv6() {
        let dual_addr = SocketAddr::new(std::net::IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0);
        let dual_sock = <UdpSocket as Socket>::bind(dual_addr)
            .await
            .expect("bind to [::]:0 should succeed");
        let bound_port = dual_sock.local_addr().unwrap().port();

        let v6_sender = UdpSocket::bind("[::1]:0").await.unwrap();
        let target = SocketAddr::new(std::net::IpAddr::V6(Ipv6Addr::LOCALHOST), bound_port);
        v6_sender
            .send_to(b"world", target)
            .await
            .expect("send from IPv6 should succeed");

        let mut buf = [0u8; 16];
        let (len, _src) = dual_sock.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..len], b"world");
    }
}
