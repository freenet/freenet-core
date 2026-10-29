//! Freenet Transport protocol implementation.
//!
//! Please see `docs/architecture/transport.md` for more information.
//!
//! Provides the low-level network transport abstraction (e.g., UDP).
//! Handles the raw sending and receiving of byte packets over the network.
//! See `architecture.md`.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::{borrow::Cow, io, net::SocketAddr};

use futures::Future;
// This is the one legitimate tokio::net::UdpSocket import in crates/core/src/:
// transport.rs is the module that implements the Socket trait abstraction over
// UdpSocket. New code outside this file must use the Socket trait instead.
use tokio::net::UdpSocket; // rule-lint: ok — Socket trait implementation site

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

mod boundary;
pub mod connection_handler;
mod crypto;
pub(crate) mod demux;
pub(crate) mod egress_cap;
pub mod in_memory_socket;
pub(crate) mod quic;

/// Mock transport infrastructure for testing and benchmarking.
/// Provides MockSocket and helper functions to create mock peer connections
/// without real network I/O.
#[cfg(any(test, feature = "bench"))]
pub use connection_handler::mock_transport;
/// Size-aware WAN link model (bottleneck queue, delay, loss, spikes) that
/// `MockSocket` can route packets through.
#[cfg(any(test, feature = "bench"))]
pub mod link_model;
mod packet_data;
pub mod peer_connection;
// todo: optimize trackers
mod received_packet_tracker;

pub(crate) mod bandwidth_history;
pub(crate) mod bbr;
pub mod congestion_control;
pub(crate) mod fixed_rate;
pub mod global_bandwidth;
pub(crate) mod ledbat;
pub mod metrics;
pub(crate) mod reference_ping;
pub(crate) mod rolling_rtt_stats;
pub(crate) mod shadow_demand;
pub(crate) mod shadow_iface_tx;
pub(crate) mod shadow_stats;

// Type alias for the in-crate concrete `Socket` impl so callers in
// this crate can obtain a real UDP socket via a stable alias. The
// rule-lint check on the literal `tokio::net::UdpSocket` path exists
// to push new code through the `Socket` trait abstraction (which
// `reference_ping` uses); this alias is how callers that legitimately
// need to construct the production socket type name it without
// re-introducing the raw path. `UdpSocket` is in scope via the
// pre-existing import above.
pub(crate) type DefaultSocket = UdpSocket;
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
#[cfg(any(test, feature = "testing"))]
pub use metrics::conn_identity_totals;
pub(crate) use metrics::{ConnIdentity, NatAttemptOrigin};
pub use metrics::{TRANSPORT_METRICS, TransportMetrics, TransportSnapshot};
// Re-export reset functions for deterministic simulation testing
pub use self::boundary::StreamId;
pub use packet_data::reset_nonce_counter;

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
    /// Peak congestion window during transfer (bytes). `None` when the
    /// controller has no bounded window (FixedRate outside a loss pause
    /// reports an effectively infinite cwnd, which is not a measurement).
    pub peak_cwnd_bytes: Option<u32>,
    /// Final congestion window at completion (bytes). `None` under the same
    /// condition as `peak_cwnd_bytes`.
    pub final_cwnd_bytes: Option<u32>,
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
    /// Retransmission timeouts (per-packet RTO expiries) on this connection
    /// while the transfer ran, counted by the connection's sent-packet
    /// tracker so it is populated whatever the congestion controller.
    /// High values indicate severe congestion or path issues.
    ///
    /// This is a per-CONNECTION delta, not a per-stream count: an RTO of any
    /// packet on the connection during the transfer is included, so
    /// concurrent transfers on one connection each count the same expiries
    /// and summing across transfers double-counts.
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

pub(crate) use self::boundary::TransportHandle;
pub(crate) use self::boundary::{
    CloseReason, InboundAcceptor, InboundExpectations, InboundStreamRegistry, ListenerExit,
    NextConnection, OutboundConnector, PeerConnectionApi, PendingConnection, ReceiptStamp,
    StreamProgressObserver, StreamSendOutcome,
};
pub use self::boundary::{IncomingChunks, IncomingStream, StreamIdentity};
#[cfg(test)]
pub(crate) use self::connection_handler::ExpectedInboundTracker;
pub use self::connection_handler::create_connection_handler;
pub(crate) use self::connection_handler::{UdpTransportConfig, open_udp_transport};
pub use self::crypto::{TransportKeypair, TransportPublicKey};
/// Returned (wrapped around the socket type) by [`create_connection_handler`].
pub use self::demux::DemuxSocket;
/// Named by [`Socket::egress_cap`] and [`Socket::send_to_paced`]; only the
/// transport itself can construct or use them.
#[doc(hidden)]
pub use self::egress_cap::{EgressCap, EgressTake};
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
    #[error(
        "Version incompatibility with gateway\n  Your client version: {actual}\n  Gateway version: {expected}\n  \n  To fix this, update your Freenet client:\n    cargo install --force freenet --version {expected}\n  \n  Or if building from source:\n    git pull && cargo install --path crates/core"
    )]
    ProtocolVersionMismatch { expected: String, actual: String },
    #[error("outbound stream to {0} failed (stream-scoped, connection survives)")]
    OutboundStreamFailed(SocketAddr),
    /// An outbound stream was not started because its length is outside
    /// what a receiver admits (`streaming::check_declared_stream_length`).
    /// Stream-scoped like [`Self::OutboundStreamFailed`], but not transient:
    /// the same message fails the same way on every attempt.
    #[error("outbound stream to {0} not sent: length {1} is outside the stream limits")]
    OutboundStreamInvalidLength(SocketAddr, u64),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
    /// An error only the UDP transport produces (a socket send or I/O
    /// failure, a handshake decryption failure, its own wire encoding).
    /// Opaque above the transport: classify it with [`Self::scope`].
    #[error(transparent)]
    Udp(UdpError),
    /// An error only the QUIC path produces (#5940): a quinn connection,
    /// stream or endpoint error, or a framing error, mapped to a stable
    /// kind. Opaque above the transport like [`Self::Udp`]: classify it with
    /// [`Self::scope`]. Nothing produces one while `transport-quic` carries
    /// no traffic.
    #[error(transparent)]
    Quic(QuicError),
}

/// An error only the UDP transport produces, inside [`TransportError::Udp`].
/// Its kind is private to the transport; its Display is the kind's, exactly
/// as the variants it replaced displayed.
#[derive(thiserror::Error)]
#[error(transparent)]
pub struct UdpError(pub(in crate::transport) UdpErrorKind);

impl std::fmt::Debug for UdpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}

/// What went wrong in a [`UdpError`].
#[derive(Debug, thiserror::Error)]
pub(in crate::transport) enum UdpErrorKind {
    /// A UDP `send_to` to this address failed with this error kind. On an
    /// established connection it is transient: one packet failed and the
    /// connection survives (see [`TransportError::is_transient`]). When a
    /// handshake returns it, it is fatal: the connection attempt is over
    /// (#5871).
    #[error("send to {0} failed: {1}")]
    SendFailed(SocketAddr, std::io::ErrorKind),
    #[error(transparent)]
    IO(std::io::Error),
    #[error(transparent)]
    PubKeyDecryptionError(crypto::DecryptionError),
    #[error(transparent)]
    Serialization(bincode::Error),
}

/// An error only the QUIC path produces, inside [`TransportError::Quic`].
/// Its kind is private to the transport; `quic::error` maps quinn's and the
/// framing's errors to it.
#[derive(Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(transparent)]
pub struct QuicError(pub(in crate::transport) QuicErrorKind);

impl std::fmt::Debug for QuicError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}

/// What went wrong in a [`QuicError`]. A small, stable set: quinn has many
/// more error variants, and `quic::error` decides which kind each is.
///
/// Scope (see [`TransportError::scope`]): `Reset` and `Stopped` are
/// transient (a retry may succeed) unless their code refuses the message
/// itself, then stream scope like `InvalidFrame` and `StreamClosed`; the
/// rest end the QUIC connection (or the attempt to make one). A connection-scope kind ends
/// the QUIC path only: the peer's UDP session stays up, and e5 falls back
/// to it inside the transport, so a connection-scope QUIC error is never
/// surfaced to the operations above it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(in crate::transport) enum QuicErrorKind {
    /// The connection ended: the peer closed it with
    /// `close_code::NODE_DROPPED_PEER` or a code this node does not know,
    /// this node closed it, the peer's QUIC stack closed it without an
    /// error, or the peer answered with a stateless reset (it lost the
    /// connection, usually by restarting). Connection scope; handled inside
    /// the transport (fallback to UDP), never surfaced to operations.
    #[error("QUIC connection lost")]
    ConnectionLost,
    /// The stream was reset with this code: by the peer while this node was
    /// reading it, or by this node, which abandoned a send whose source
    /// failed (`reset_code::SENDER_ABORTED`). Transient or stream scope by
    /// code (`quic::error::reset_is_retryable`).
    #[error("QUIC stream reset with code {code}")]
    Reset { code: u64 },
    /// The peer stopped the stream (STOP_SENDING) with this code
    /// (`stop_code`). Transient or stream scope by code
    /// (`quic::error::stop_is_retryable`).
    #[error("QUIC stream stopped by the peer with code {code}")]
    Stopped { code: u64 },
    /// The connection was refused, or ended by a refusal. Connection scope.
    #[error("QUIC connection refused: {reason}")]
    Refused { reason: QuicRefusal },
    /// Nothing was heard from the peer for the idle timeout. Connection
    /// scope.
    #[error("QUIC connection timed out")]
    TimedOut,
    /// The node's QUIC endpoint is gone: its driver ended, or it was
    /// closed. Connection scope, for every connection of that endpoint
    /// (one per node), whose `QuicAvailability` reports it.
    #[error("the QUIC endpoint is down")]
    EndpointDown,
    /// The peer broke the QUIC protocol (a QUIC transport error code other
    /// than a TLS alert, sent by the peer or detected here). Connection
    /// scope.
    #[error("QUIC protocol error {code:#x}")]
    Protocol { code: u64 },
    /// A message could not be framed: its header would be refused
    /// (`framing::HeaderError`). Stream scope, and the same message fails
    /// the same way on every attempt.
    #[error("QUIC frame refused before sending")]
    InvalidFrame,
    /// The stream was already closed on this side (a misuse of quinn's
    /// stream API; not expected). Stream scope.
    #[error("QUIC stream already closed")]
    StreamClosed,
}

/// Why a QUIC connection was refused ([`QuicErrorKind::Refused`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(in crate::transport) enum QuicRefusal {
    /// The TLS handshake failed (a TLS alert, QUIC crypto error 0x100 to
    /// 0x1ff, either side): a key other than the expected one, a foreign
    /// key type, another ALPN.
    #[error("the TLS handshake failed")]
    Handshake,
    /// The peer does not speak QUIC version 1.
    #[error("no common QUIC version")]
    Version,
    /// The peer's QUIC stack refused the connection (transport error
    /// CONNECTION_REFUSED), for example because it was not accepting.
    #[error("the peer's QUIC stack refused the connection")]
    ConnectionRefused,
    /// The peer closed with `close_code::DOWNGRADE`: fall back to UDP.
    #[error("the peer asked to fall back to UDP")]
    Downgrade,
    /// The peer closed with `close_code::IDENTITY_MISMATCH`: the key this
    /// node proved is not the one the peer has on record for it.
    #[error("the peer reported an identity mismatch")]
    IdentityMismatch,
    /// The demux socket's allow-set is full.
    #[error("the QUIC allow-set is full")]
    AllowSetFull,
    /// The address is already allowed for another key.
    #[error("the address is already allowed for another key")]
    KeyConflict,
    /// The peer's transport key cannot be a QUIC identity.
    #[error("the peer's key is not usable for QUIC")]
    UnusableKey,
    /// This node could not start the connection: its TLS or quinn
    /// configuration refused it (an invalid address, no connection IDs
    /// left).
    #[error("this node could not start the connection")]
    Local,
}

impl From<QuicErrorKind> for TransportError {
    fn from(kind: QuicErrorKind) -> Self {
        TransportError::Quic(QuicError(kind))
    }
}

impl From<UdpErrorKind> for TransportError {
    fn from(kind: UdpErrorKind) -> Self {
        TransportError::Udp(UdpError(kind))
    }
}

impl From<std::io::Error> for TransportError {
    fn from(error: std::io::Error) -> Self {
        UdpErrorKind::IO(error).into()
    }
}

impl From<crypto::DecryptionError> for TransportError {
    fn from(error: crypto::DecryptionError) -> Self {
        UdpErrorKind::PubKeyDecryptionError(error).into()
    }
}

impl From<bincode::Error> for TransportError {
    fn from(error: bincode::Error) -> Self {
        UdpErrorKind::Serialization(error).into()
    }
}

impl TransportError {
    /// A UDP `send_to` to `addr` failed with `kind`.
    pub(in crate::transport) fn send_failed(addr: SocketAddr, kind: std::io::ErrorKind) -> Self {
        UdpErrorKind::SendFailed(addr, kind).into()
    }
}

/// What a [`TransportError`] ends, as seen from above the transport.
///
/// The node classifies transport errors only through
/// [`TransportError::closes_connection`] (and [`TransportError::scope`]),
/// never by matching variants, so a transport with different error variants
/// needs only to classify its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorScope {
    /// One send or one stream failed, and a retry may succeed. The connection
    /// survives: the idle timeout remains the sole authority on its liveness.
    Transient,
    /// One stream cannot be sent, and a retry fails the same way. A
    /// transport must handle these itself (the UDP transport does, in its
    /// recv loop); if one reaches the node, the node treats it as non-fatal
    /// and keeps the connection.
    Stream,
    /// The connection (or the attempt to make one) is over.
    Connection,
}

impl TransportError {
    /// Classify this error. See [`ErrorScope`].
    ///
    /// The match is exhaustive on purpose: a new variant must be classified
    /// here, not fall into a default.
    pub fn scope(&self) -> ErrorScope {
        match self {
            TransportError::OutboundStreamFailed(..)
            | TransportError::Udp(UdpError(UdpErrorKind::SendFailed(..))) => ErrorScope::Transient,
            TransportError::Quic(QuicError(QuicErrorKind::Stopped { code }))
                if quic::error::stop_is_retryable(*code) =>
            {
                ErrorScope::Transient
            }
            TransportError::Quic(QuicError(QuicErrorKind::Reset { code }))
                if quic::error::reset_is_retryable(*code) =>
            {
                ErrorScope::Transient
            }
            TransportError::OutboundStreamInvalidLength(..)
            | TransportError::Quic(QuicError(
                QuicErrorKind::Reset { .. }
                | QuicErrorKind::Stopped { .. }
                | QuicErrorKind::InvalidFrame
                | QuicErrorKind::StreamClosed,
            )) => ErrorScope::Stream,
            TransportError::Quic(QuicError(
                QuicErrorKind::ConnectionLost
                | QuicErrorKind::Refused { .. }
                | QuicErrorKind::TimedOut
                | QuicErrorKind::EndpointDown
                | QuicErrorKind::Protocol { .. },
            )) => ErrorScope::Connection,
            TransportError::ChannelClosed
            | TransportError::ConnectionClosed(_)
            | TransportError::ConnectionEstablishmentFailure { .. }
            | TransportError::ProtocolVersionMismatch { .. }
            | TransportError::Other(_)
            | TransportError::Udp(UdpError(
                UdpErrorKind::IO(_)
                | UdpErrorKind::PubKeyDecryptionError(_)
                | UdpErrorKind::Serialization(_),
            )) => ErrorScope::Connection,
        }
    }

    /// Whether this error ends the connection ([`ErrorScope::Connection`]).
    /// This is the node's only decision on a transport error: anything
    /// else, transient or stream-scoped, leaves the connection up.
    pub fn closes_connection(&self) -> bool {
        self.scope() == ErrorScope::Connection
    }

    /// Former name of the transient check, kept for one release.
    #[deprecated(note = "use closes_connection()/scope()")]
    pub fn is_transient_send_failure(&self) -> bool {
        self.scope() == ErrorScope::Transient
    }

    /// Returns true if this error is a transient failure that must NOT
    /// kill the connection ([`ErrorScope::Transient`]). The transport's own
    /// recv loop uses it to tell transient failures from stream-scoped ones
    /// ([`ErrorScope::Stream`]), which it accounts for differently. The idle timeout
    /// remains the sole authority on connection liveness; a single stalled
    /// or failed stream only fails that stream, and the operation layer
    /// times out and retries against another candidate.
    ///
    /// Covers:
    /// - `UdpErrorKind::SendFailed`: a transient UDP send error (e.g.
    ///   ENETUNREACH) on a single packet of an established connection.
    ///   The handshake also returns `SendFailed` (#5871), and there it is
    ///   FATAL: it ends the connection attempt. Do not apply this predicate
    ///   to the result of a connect; it describes established connections.
    /// - [`TransportError::OutboundStreamFailed`]: any stream-scoped failure
    ///   in `outbound_stream::{send_stream, pipe_stream}` — the
    ///   congestion-window wait exceeding `CWND_WAIT_TIMEOUT`, or (for the
    ///   relay-pipe path) the upstream inbound feed stalling past
    ///   `STREAM_INACTIVITY_TIMEOUT` or yielding an error. All three are
    ///   problems with one transfer or its inbound feed, not with the
    ///   downstream connection. Tearing down the whole connection here would
    ///   kill every other operation multiplexed on it and force a
    ///   re-handshake (#4345). The connection stays up and the idle timeout
    ///   decides liveness instead; the op layer times out and retries against
    ///   another candidate.
    pub(crate) fn is_transient(&self) -> bool {
        self.scope() == ErrorScope::Transient
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

    /// The node-wide egress cap this socket charges, if any. Only
    /// [`DemuxSocket`] can have one (when `total-bandwidth-limit` is set);
    /// every other socket returns `None`. Pacing points take from it through
    /// `egress_cap::try_take`.
    #[doc(hidden)]
    fn egress_cap(&self) -> Option<&EgressCap> {
        None
    }

    /// Send a datagram whose bytes a pacing point already took from
    /// [`Self::egress_cap`]. The socket returns what the datagram did not use
    /// instead of charging it a second time.
    ///
    /// A take exists only for a socket with a cap, so the default (for
    /// sockets without one) just returns the take and sends.
    #[doc(hidden)]
    fn send_to_paced(
        &self,
        buf: &[u8],
        target: SocketAddr,
        take: EgressTake<'_>,
    ) -> impl Future<Output = io::Result<usize>> + Send {
        drop(take);
        self.send_to(buf, target)
    }

    /// Send a datagram without waiting: when the socket cannot take it now,
    /// return `WouldBlock` instead of sleeping or awaiting. For a sender that
    /// must never block, such as a QUIC endpoint's driver (#5940). Charged
    /// like [`Self::send_to`] once sent; a datagram that was not sent is not
    /// charged.
    ///
    /// The default returns `Unsupported`; the UDP sockets, the in-memory
    /// sockets and [`DemuxSocket`] implement it.
    #[doc(hidden)]
    fn try_send_to(&self, _buf: &[u8], _target: SocketAddr) -> io::Result<usize> {
        Err(io::Error::from(io::ErrorKind::Unsupported))
    }

    /// Poll whether the socket can take a send now. `Ready` means a
    /// [`Self::try_send_to`] may succeed (it can still return `WouldBlock`);
    /// `Pending` arms `cx`'s waker for when the socket becomes writable.
    ///
    /// The default is always ready, which is right for sockets that never
    /// return `WouldBlock`.
    #[doc(hidden)]
    fn poll_send_ready(&self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    /// Read one datagram, saying whether it is for the caller
    /// ([`SocketRead::Datagram`]) or was consumed by the socket
    /// ([`SocketRead::Diverted`]: [`DemuxSocket`] counted it and dropped it
    /// or passed it to its QUIC ingress). The listener reads through this so
    /// it sees every datagram the socket received, of any class, for its
    /// socket-health signal.
    ///
    /// Cancellation-safe when [`Self::recv_from`] is: the default is one
    /// `recv_from`.
    #[doc(hidden)]
    fn recv_classified(
        &self,
        buf: &mut [u8],
    ) -> impl Future<Output = io::Result<SocketRead>> + Send {
        async move {
            let (len, from) = self.recv_from(buf).await?;
            Ok(SocketRead::Datagram(len, from))
        }
    }
}

/// What one [`Socket::recv_classified`] read produced.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SocketRead {
    /// A datagram for the caller: its length in the buffer and its source.
    Datagram(usize, SocketAddr),
    /// The socket received a datagram and consumed it itself.
    Diverted,
}

/// Normalize an IPv4-mapped IPv6 address (`::ffff:x.x.x.x`) to plain IPv4.
///
/// Dual-stack sockets report IPv4 peers as `::ffff:x.x.x.x`. Without
/// normalization, the same peer gets different ring locations, connection map
/// entries, and backoff tracking depending on whether the local socket is
/// IPv4-only or dual-stack. Normalizing at the transport boundary ensures a
/// single canonical representation throughout the system.
pub fn normalize_mapped_addr(addr: SocketAddr) -> SocketAddr {
    if let SocketAddr::V6(v6) = &addr {
        if let Some(v4) = v6.ip().to_ipv4_mapped() {
            return SocketAddr::new(std::net::IpAddr::V4(v4), v6.port());
        }
    }
    addr
}

/// Convert a plain IPv4 address to IPv4-mapped IPv6 for sending on a dual-stack socket.
///
/// An AF_INET6 socket cannot `sendto()` a plain AF_INET address — the kernel
/// rejects the address family mismatch with EINVAL. This function converts
/// `x.x.x.x` to `::ffff:x.x.x.x` when the local socket is IPv6.
fn map_addr_for_send(local_is_ipv6: bool, target: SocketAddr) -> SocketAddr {
    if local_is_ipv6 {
        if let SocketAddr::V4(v4) = &target {
            let mapped = v4.ip().to_ipv6_mapped();
            return SocketAddr::new(std::net::IpAddr::V6(mapped), v4.port());
        }
    }
    target
}

/// Create, tune and bind the production UDP socket. Shared by the
/// `Socket` impls for [`UdpSocket`] and [`UdpTransportSocket`].
async fn bind_udp(addr: SocketAddr) -> io::Result<UdpSocket> {
    {
        // Use socket2 to configure dual-stack before binding, then convert
        // to a tokio UdpSocket. This ensures IPv6 sockets accept IPv4 too.
        let is_ipv6 = addr.is_ipv6();
        let domain = if is_ipv6 {
            socket2::Domain::IPV6
        } else {
            socket2::Domain::IPV4
        };
        let sock = socket2::Socket::new(domain, socket2::Type::DGRAM, Some(socket2::Protocol::UDP))
            .map_err(|e| io::Error::new(e.kind(), format!("Failed to create UDP socket: {e}")))?;
        if is_ipv6 {
            // Dual-stack: accept both IPv4 (mapped as ::ffff:x.x.x.x) and IPv6
            sock.set_only_v6(false)?;
        }
        sock.set_nonblocking(true)?;
        // Request a generous UDP socket buffer. Stock Linux net.core.rmem_default
        // is only ~208 KiB, which is inadequate for a gateway under realistic
        // load and causes kernel-level drops visible as UdpRcvbufErrors in
        // /proc/net/snmp. Production gateways were silently losing ~15x the
        // packets they delivered until operators raised the sysctls manually
        // (see #3844). The kernel silently clamps to net.core.rmem_max, so we
        // also surface the actual size to give operators actionable diagnostics.
        //
        // Best-effort: failures log a warn and bind proceeds with whatever
        // buffer the kernel had — we don't fail bind over a tuning hint.
        const DESIRED_UDP_BUF_BYTES: usize = 16 * 1024 * 1024; // 16 MiB
        if let Err(e) = sock.set_recv_buffer_size(DESIRED_UDP_BUF_BYTES) {
            tracing::warn!(error = %e, "failed to request UDP recv buffer size");
        }
        if let Err(e) = sock.set_send_buffer_size(DESIRED_UDP_BUF_BYTES) {
            tracing::warn!(error = %e, "failed to request UDP send buffer size");
        }
        sock.bind(&addr.into()).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("Failed to bind UDP socket to {addr}: {e}"),
            )
        })?;
        // Only emit the post-tuning diagnostics after a successful bind so we
        // don't log buffer sizes (and clamp warnings) once per AddrInUse retry
        // inside `bind_socket_with_retry`. The buffer-tuning calls above still
        // run for every attempt, but their kernel side-effects belong to the
        // socket fd that gets dropped on retry — what we want operators to see
        // is the configuration of the fd that survives.
        //
        // `ok()` (not `unwrap_or(0)`) so a getsockopt failure doesn't
        // masquerade as a 0-byte buffer and trip the clamp warning below.
        let actual_rcv = sock.recv_buffer_size().ok();
        let actual_snd = sock.send_buffer_size().ok();
        // Linux's stock rmem_max ≈ 208 KiB is the documented production problem.
        // setsockopt(SO_RCVBUF) on Linux sets sk_rcvbuf = min(val, rmem_max) * 2,
        // so an unclamped 16 MiB request returns ~32 MiB. macOS/BSD default
        // `kern.ipc.maxsockbuf` is 4 MiB and they don't double — already ~20x
        // stock Linux, so a clamp there isn't worth alerting on, and the
        // "raise net.core.rmem_max" advice in this warning is Linux-specific.
        #[cfg(target_os = "linux")]
        {
            if let Some(rcv) = actual_rcv {
                if rcv < DESIRED_UDP_BUF_BYTES / 2 {
                    tracing::warn!(
                        requested = DESIRED_UDP_BUF_BYTES,
                        actual_rcv = rcv,
                        "UDP recv buffer smaller than requested — raise net.core.rmem_max (kernel clamped the request)"
                    );
                }
            }
            if let Some(snd) = actual_snd {
                if snd < DESIRED_UDP_BUF_BYTES / 2 {
                    tracing::warn!(
                        requested = DESIRED_UDP_BUF_BYTES,
                        actual_snd = snd,
                        "UDP send buffer smaller than requested — raise net.core.wmem_max (kernel clamped the request)"
                    );
                }
            }
        }
        tracing::info!(actual_rcv = ?actual_rcv, actual_snd = ?actual_snd, "UDP socket buffer sizes");
        let std_socket: std::net::UdpSocket = sock.into();
        UdpSocket::from_std(std_socket)
    }
}

impl Socket for UdpSocket {
    async fn bind(addr: SocketAddr) -> io::Result<Self> {
        bind_udp(addr).await
    }

    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let (len, addr) = self.recv_from(buf).await?;
        // Normalize ::ffff:x.x.x.x → plain IPv4 so the rest of the system
        // sees a consistent address regardless of socket type.
        let addr = normalize_mapped_addr(addr);
        // Receive-side dashboard metering happens post-authentication — see
        // `peer_connection::PeerConnection::recv` after `try_decrypt_sym`
        // succeeds. Doing it here would let an attacker spoofing UDP source
        // addresses inflate the dashboard counters and crowd legitimate
        // peers out of the bounded per-peer table (#3999).
        Ok((len, addr))
    }

    async fn send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        // Convert plain IPv4 targets to mapped form when sending on an IPv6 socket,
        // since AF_INET6 sockets reject AF_INET addresses with EINVAL.
        let local_is_ipv6 = self.local_addr().map(|a| a.is_ipv6()).unwrap_or(false);
        let mapped_target = map_addr_for_send(local_is_ipv6, target);
        let result = self.send_to(buf, mapped_target).await;
        if let Ok(bytes) = result {
            // Record against the un-mapped target so per-peer keys match the
            // normalized form used everywhere else in the system.
            metrics::TRANSPORT_METRICS.record_packet_sent(target, bytes as u64);
        }
        result
    }

    fn send_to_blocking(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        let local_is_ipv6 = self.local_addr().map(|a| a.is_ipv6()).unwrap_or(false);
        send_to_blocking_mapped(self, buf, target, local_is_ipv6)
    }

    fn try_send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        let local_is_ipv6 = self.local_addr().map(|a| a.is_ipv6()).unwrap_or(false);
        try_send_to_mapped(self, buf, target, local_is_ipv6)
    }

    fn poll_send_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        UdpSocket::poll_send_ready(self, cx)
    }
}

/// One non-blocking send, metered like `send_to` when it succeeds. Returns
/// `WouldBlock` when the kernel send buffer is full.
fn try_send_to_mapped(
    sock: &UdpSocket,
    buf: &[u8],
    target: SocketAddr,
    local_is_ipv6: bool,
) -> io::Result<usize> {
    let sent = sock.try_send_to(buf, map_addr_for_send(local_is_ipv6, target))?;
    metrics::TRANSPORT_METRICS.record_packet_sent(target, sent as u64);
    Ok(sent)
}

/// Blocking send shared by the `Socket` impls.
///
/// `try_send_to` is synchronous and for UDP typically succeeds immediately.
/// However, under high load the kernel buffer might be full, returning
/// WouldBlock. In that case, we retry with exponential backoff since we're in
/// a blocking context.
fn send_to_blocking_mapped(
    sock: &UdpSocket,
    buf: &[u8],
    target: SocketAddr,
    local_is_ipv6: bool,
) -> io::Result<usize> {
    let mut backoff_us = 1u64; // Start at 1μs
    const MAX_BACKOFF_US: u64 = 1000; // Cap at 1ms

    loop {
        match try_send_to_mapped(sock, buf, target, local_is_ipv6) {
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

/// Production UDP socket: a `tokio::net::UdpSocket` plus its address family,
/// captured once at bind time.
///
/// The plain `Socket for UdpSocket` impl above has to ask the kernel
/// (`local_addr()`, a `getsockname` syscall) on every send to learn whether the
/// socket is AF_INET6. That cost 0.6-3.6% of node CPU on a production peer
/// sending ~2,250 pps. The family of a socket never changes after `bind` and
/// equals the family of the bind address (a dual-stack socket bound to `[::]`
/// is AF_INET6), so this type caches it. The node binds its transport socket
/// as this type; `UdpSocket` keeps its original impl so the public `Socket`
/// surface is unchanged.
///
/// Deliberately no `Deref` to the tokio socket: the `Socket` trait methods
/// would shadow the inherent ones, making it unclear whether a call is the
/// metered trait `send_to` or the raw one.
#[derive(Debug)]
pub struct UdpTransportSocket {
    sock: UdpSocket,
    is_ipv6: bool,
}

impl UdpTransportSocket {
    /// Local address the socket is bound to.
    #[allow(dead_code)]
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.sock.local_addr()
    }

    /// Whether the socket is AF_INET6 (cached at bind; no syscall).
    #[allow(dead_code)]
    pub fn is_ipv6(&self) -> bool {
        self.is_ipv6
    }

    /// Unwrap the underlying tokio socket.
    #[allow(dead_code)]
    pub fn into_inner(self) -> UdpSocket {
        self.sock
    }
}

impl Socket for UdpTransportSocket {
    async fn bind(addr: SocketAddr) -> io::Result<Self> {
        Ok(Self {
            sock: bind_udp(addr).await?,
            is_ipv6: addr.is_ipv6(),
        })
    }

    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        Socket::recv_from(&self.sock, buf).await
    }

    async fn send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        let mapped_target = map_addr_for_send(self.is_ipv6, target);
        let result = self.sock.send_to(buf, mapped_target).await;
        if let Ok(bytes) = result {
            // Record against the un-mapped target so per-peer keys match the
            // normalized form used everywhere else in the system.
            metrics::TRANSPORT_METRICS.record_packet_sent(target, bytes as u64);
        }
        result
    }

    fn send_to_blocking(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        // Same as the `UdpSocket` impl, minus the per-call getsockname.
        send_to_blocking_mapped(&self.sock, buf, target, self.is_ipv6)
    }

    fn try_send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        try_send_to_mapped(&self.sock, buf, target, self.is_ipv6)
    }

    fn poll_send_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.sock.poll_send_ready(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::received_packet_tracker::ReportResult;
    use crate::transport::sent_packet_tracker::ResendAction;

    /// The production socket's non-blocking send: a fresh socket becomes
    /// ready to send, the datagram goes out from `try_send_to` with no await, and
    /// arrives. (Its `WouldBlock` path needs a full kernel send buffer,
    /// which loopback cannot be made to produce reliably; `DemuxSocket`'s
    /// tests cover passing `WouldBlock` through.)
    #[tokio::test]
    async fn udp_transport_socket_try_send_to_sends_without_waiting() {
        let localhost: SocketAddr = (std::net::Ipv4Addr::LOCALHOST, 0).into();
        let sender = <UdpTransportSocket as Socket>::bind(localhost)
            .await
            .unwrap();
        let receiver = <UdpTransportSocket as Socket>::bind(localhost)
            .await
            .unwrap();
        let to = receiver.local_addr().unwrap();
        // Tokio reports writable once its reactor has seen it, so the first
        // poll may be Pending; it must become Ready with the waker it armed.
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            std::future::poll_fn(|cx| sender.poll_send_ready(cx)),
        )
        .await
        .expect("a fresh socket becomes writable")
        .unwrap();
        assert_eq!(Socket::try_send_to(&sender, &[0x01, 2, 3], to).unwrap(), 3);
        let mut buf = [0u8; 16];
        let (len, from) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            Socket::recv_from(&receiver, &mut buf),
        )
        .await
        .expect("the datagram arrives")
        .unwrap();
        assert_eq!(&buf[..len], &[0x01, 2, 3]);
        assert_eq!(from, sender.local_addr().unwrap());
    }

    /// Each UDP-only error displays exactly as the `TransportError` variant
    /// it replaced did, and classifies the same way.
    #[test]
    fn udp_errors_display_and_classify_as_before() {
        let addr: SocketAddr = "192.0.2.1:4000".parse().unwrap();
        let kind = io::ErrorKind::NetworkUnreachable;
        let send = TransportError::send_failed(addr, kind);
        assert_eq!(send.to_string(), format!("send to {addr} failed: {kind}"));
        assert_eq!(send.scope(), ErrorScope::Transient);

        let io_error = TransportError::from(io::Error::other("socket gone"));
        assert_eq!(io_error.to_string(), "socket gone");
        assert_eq!(io_error.scope(), ErrorScope::Connection);

        let decrypt = TransportError::from(crypto::DecryptionError::DecryptionFailed);
        assert_eq!(
            decrypt.to_string(),
            crypto::DecryptionError::DecryptionFailed.to_string()
        );
        assert_eq!(decrypt.scope(), ErrorScope::Connection);

        let serialization = bincode::deserialize::<u64>(&[]).unwrap_err();
        let expected = serialization.to_string();
        let encoded = TransportError::from(serialization);
        assert_eq!(encoded.to_string(), expected);
        assert_eq!(encoded.scope(), ErrorScope::Connection);

        for error in [&send, &io_error, &decrypt, &encoded] {
            assert!(matches!(error, TransportError::Udp(_)), "{error:?}");
        }
    }

    /// Every variant keeps the decision the node made before errors were
    /// classified through `scope()`: the node continued on `SendFailed` and
    /// `OutboundStreamFailed` and closed the connection on everything else.
    ///
    /// `expected` is written independently of `scope()`, one row per variant.
    /// Its match has no wildcard, so adding a variant without a row here does
    /// not compile.
    #[test]
    fn error_scope_preserves_the_transient_decision_for_every_variant() {
        /// (scope, old `is_transient_send_failure`, closes the connection)
        fn expected(err: &TransportError) -> (ErrorScope, bool, bool) {
            match err {
                TransportError::ChannelClosed => (ErrorScope::Connection, false, true),
                TransportError::ConnectionClosed(_) => (ErrorScope::Connection, false, true),
                TransportError::ConnectionEstablishmentFailure { .. } => {
                    (ErrorScope::Connection, false, true)
                }
                TransportError::ProtocolVersionMismatch { .. } => {
                    (ErrorScope::Connection, false, true)
                }
                TransportError::Udp(UdpError(UdpErrorKind::SendFailed(..))) => {
                    (ErrorScope::Transient, true, false)
                }
                TransportError::OutboundStreamFailed(_) => (ErrorScope::Transient, true, false),
                TransportError::OutboundStreamInvalidLength(..) => {
                    (ErrorScope::Stream, false, false)
                }
                TransportError::Udp(UdpError(UdpErrorKind::IO(_))) => {
                    (ErrorScope::Connection, false, true)
                }
                TransportError::Other(_) => (ErrorScope::Connection, false, true),
                TransportError::Udp(UdpError(UdpErrorKind::PubKeyDecryptionError(_))) => {
                    (ErrorScope::Connection, false, true)
                }
                TransportError::Udp(UdpError(UdpErrorKind::Serialization(_))) => {
                    (ErrorScope::Connection, false, true)
                }
                TransportError::Quic(QuicError(kind)) => match kind {
                    // Codes that refuse the message itself, written out per
                    // namespace: stops 0x11 HEADER_REFUSED, 0x15 OVERLONG,
                    // 0x18 FRAGMENT_REFUSED; resets 0x22 LENGTH_MISMATCH.
                    // Every other code in each namespace, known or not, is
                    // retryable.
                    QuicErrorKind::Stopped { code } if ![0x11, 0x15, 0x18].contains(code) => {
                        (ErrorScope::Transient, true, false)
                    }
                    QuicErrorKind::Reset { code } if *code != 0x22 => {
                        (ErrorScope::Transient, true, false)
                    }
                    QuicErrorKind::Reset { .. }
                    | QuicErrorKind::Stopped { .. }
                    | QuicErrorKind::InvalidFrame
                    | QuicErrorKind::StreamClosed => (ErrorScope::Stream, false, false),
                    QuicErrorKind::ConnectionLost
                    | QuicErrorKind::Refused { .. }
                    | QuicErrorKind::TimedOut
                    | QuicErrorKind::EndpointDown
                    | QuicErrorKind::Protocol { .. } => (ErrorScope::Connection, false, true),
                },
            }
        }

        let addr: SocketAddr = "192.0.2.1:4000".parse().unwrap();
        let cases = [
            TransportError::ChannelClosed,
            TransportError::ConnectionClosed(addr),
            TransportError::ConnectionEstablishmentFailure { cause: "x".into() },
            TransportError::ProtocolVersionMismatch {
                expected: "1".into(),
                actual: "2".into(),
            },
            TransportError::send_failed(addr, io::ErrorKind::NetworkUnreachable),
            TransportError::OutboundStreamFailed(addr),
            TransportError::OutboundStreamInvalidLength(addr, 0),
            TransportError::from(io::Error::other("x")),
            TransportError::Other(anyhow::anyhow!("x")),
            TransportError::from(crypto::DecryptionError::DecryptionFailed),
            TransportError::from(bincode::deserialize::<u64>(&[]).unwrap_err()),
            TransportError::from(QuicErrorKind::ConnectionLost),
            TransportError::from(QuicErrorKind::Reset { code: 0x21 }),
            TransportError::from(QuicErrorKind::Stopped { code: 0x12 }),
            TransportError::from(QuicErrorKind::Stopped { code: 0x14 }),
            TransportError::from(QuicErrorKind::Stopped { code: 0x16 }),
            TransportError::from(QuicErrorKind::Stopped { code: 0x11 }),
            TransportError::from(QuicErrorKind::Reset { code: 0x15 }),
            TransportError::from(QuicErrorKind::Stopped { code: 0x13 }),
            TransportError::from(QuicErrorKind::Stopped { code: 0x17 }),
            TransportError::from(QuicErrorKind::Stopped { code: 0x18 }),
            TransportError::from(QuicErrorKind::Reset { code: 0x22 }),
            TransportError::from(QuicErrorKind::Stopped { code: 0x1f }),
            // Cross-namespace values: a reset carrying a deterministic stop
            // value, and a stop carrying the deterministic reset value.
            TransportError::from(QuicErrorKind::Reset { code: 0x11 }),
            TransportError::from(QuicErrorKind::Reset { code: 0x18 }),
            TransportError::from(QuicErrorKind::Stopped { code: 0x22 }),
            TransportError::from(QuicErrorKind::Refused {
                reason: QuicRefusal::Downgrade,
            }),
            TransportError::from(QuicErrorKind::TimedOut),
            TransportError::from(QuicErrorKind::EndpointDown),
            TransportError::from(QuicErrorKind::Protocol { code: 0xa }),
            TransportError::from(QuicErrorKind::InvalidFrame),
            TransportError::from(QuicErrorKind::StreamClosed),
        ];
        for err in &cases {
            let (scope, old_transient, closes) = expected(err);
            assert_eq!(err.scope(), scope, "{err:?}");
            assert_eq!(err.is_transient(), old_transient, "{err:?}");
            #[allow(deprecated)]
            let alias = err.is_transient_send_failure();
            assert_eq!(alias, old_transient, "{err:?}");
            assert_eq!(err.closes_connection(), closes, "{err:?}");
        }
    }

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
                ResendAction::WaitUntil(_)
                | ResendAction::TlpProbe(..)
                | ResendAction::Abandon { .. } => {
                    panic!("Expected resend action for packet {id}")
                }
            }
        }
    }
}

#[cfg(test)]
mod version_discovery_tests {
    use super::*;
    use serial_test::serial;

    // These tests mutate the process-global version-discovery statics
    // (`HIGHEST_SEEN_VERSION` et al.), so they MUST NOT run concurrently with
    // each other — an interleaved `report_peer_version` from a sibling test
    // corrupts another's `get_highest_seen_version` read. `#[serial(...)]`
    // enforces that (the module comment previously only *asked* for
    // `--test-threads=1`, which the default multi-threaded runner ignores,
    // making these tests flaky in the full suite).

    #[test]
    #[serial(version_discovery)]
    fn single_reporter_not_trusted() {
        reset_version_discovery();
        report_peer_version((0, 1, 153));
        // Only 1 reporter — should not be trusted yet
        assert_eq!(get_highest_seen_version(), None);
    }

    #[test]
    #[serial(version_discovery)]
    fn two_reporters_trusted() {
        reset_version_discovery();
        report_peer_version((0, 1, 153));
        report_peer_version((0, 1, 153));
        assert_eq!(get_highest_seen_version(), Some((0, 1, 153)));
    }

    #[test]
    #[serial(version_discovery)]
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
    #[serial(version_discovery)]
    fn major_minor_bumps_accepted() {
        reset_version_discovery();
        report_peer_version((1, 0, 0));
        report_peer_version((1, 0, 0));
        assert_eq!(get_highest_seen_version(), Some((1, 0, 0)));
    }
}

#[cfg(test)]
mod dual_stack_tests {
    //! These tests use real UdpSocket (not SimulationSocket) because they validate
    //! OS-level dual-stack behavior: IPv4-mapped address normalization, kernel
    //! address-family mapping in sendto(), and IPV6_V6ONLY socket option effects.
    //! SimulationSocket cannot exercise any of these kernel-level behaviors.

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
        let v4_addr = SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0);
        let v4_sender = <UdpSocket as Socket>::bind(v4_addr).await.unwrap();
        let target = SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            bound_port,
        );
        <UdpSocket as Socket>::send_to(&v4_sender, b"hello", target)
            .await
            .expect("send from IPv4 should succeed");

        let mut buf = [0u8; 16];
        let (len, src) = <UdpSocket as Socket>::recv_from(&dual_sock, &mut buf)
            .await
            .unwrap();
        assert_eq!(&buf[..len], b"hello");
        // recv_from must normalize ::ffff:127.0.0.1 → 127.0.0.1
        assert!(
            src.is_ipv4(),
            "IPv4 source should be normalized to plain IPv4, got {src}"
        );
    }

    /// Verify that binding to [::]:0 also accepts IPv6 traffic.
    #[tokio::test]
    async fn udp_dual_stack_accepts_ipv6() {
        let dual_addr = SocketAddr::new(std::net::IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0);
        let dual_sock = <UdpSocket as Socket>::bind(dual_addr)
            .await
            .expect("bind to [::]:0 should succeed");
        let bound_port = dual_sock.local_addr().unwrap().port();

        let v6_addr = SocketAddr::new(std::net::IpAddr::V6(Ipv6Addr::LOCALHOST), 0);
        let v6_sender = <UdpSocket as Socket>::bind(v6_addr).await.unwrap();
        let target = SocketAddr::new(std::net::IpAddr::V6(Ipv6Addr::LOCALHOST), bound_port);
        <UdpSocket as Socket>::send_to(&v6_sender, b"world", target)
            .await
            .expect("send from IPv6 should succeed");

        let mut buf = [0u8; 16];
        let (len, _src) = <UdpSocket as Socket>::recv_from(&dual_sock, &mut buf)
            .await
            .unwrap();
        assert_eq!(&buf[..len], b"world");
    }

    #[test]
    fn normalize_mapped_addr_converts_ipv4_mapped() {
        // ::ffff:127.0.0.1 → 127.0.0.1
        let mapped: SocketAddr = "[::ffff:127.0.0.1]:1234".parse().unwrap();
        let normalized = normalize_mapped_addr(mapped);
        assert_eq!(normalized, "127.0.0.1:1234".parse::<SocketAddr>().unwrap());
    }

    #[test]
    fn normalize_mapped_addr_preserves_native_ipv4() {
        let v4: SocketAddr = "1.2.3.4:5678".parse().unwrap();
        assert_eq!(normalize_mapped_addr(v4), v4);
    }

    #[test]
    fn normalize_mapped_addr_preserves_native_ipv6() {
        let v6: SocketAddr = "[2001:db8::1]:9999".parse().unwrap();
        assert_eq!(normalize_mapped_addr(v6), v6);
    }

    #[test]
    fn map_addr_for_send_maps_ipv4_on_ipv6_socket() {
        let v4: SocketAddr = "1.2.3.4:5678".parse().unwrap();
        let mapped = map_addr_for_send(true, v4);
        assert!(mapped.is_ipv6());
        if let SocketAddr::V6(v6) = mapped {
            assert_eq!(v6.ip().to_ipv4_mapped(), Some("1.2.3.4".parse().unwrap()));
            assert_eq!(v6.port(), 5678);
        }
    }

    #[test]
    fn map_addr_for_send_noop_on_ipv4_socket() {
        let v4: SocketAddr = "1.2.3.4:5678".parse().unwrap();
        assert_eq!(map_addr_for_send(false, v4), v4);
    }

    /// Verify round-trip: dual-stack socket can send back to an IPv4 peer
    /// whose address was learned via recv_from (normalized to plain IPv4).
    #[tokio::test]
    async fn udp_dual_stack_roundtrip_ipv4() {
        let dual_addr = SocketAddr::new(std::net::IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0);
        let dual_sock = <UdpSocket as Socket>::bind(dual_addr)
            .await
            .expect("bind to [::]:0 should succeed");
        let dual_port = dual_sock.local_addr().unwrap().port();

        let v4_addr = SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0);
        let v4_sock = <UdpSocket as Socket>::bind(v4_addr).await.unwrap();
        let v4_port = v4_sock.local_addr().unwrap().port();

        // IPv4 → dual-stack
        let target = SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            dual_port,
        );
        <UdpSocket as Socket>::send_to(&v4_sock, b"ping", target)
            .await
            .unwrap();

        let mut buf = [0u8; 16];
        let (len, src) = <UdpSocket as Socket>::recv_from(&dual_sock, &mut buf)
            .await
            .unwrap();
        assert_eq!(&buf[..len], b"ping");
        assert!(src.is_ipv4(), "source should be normalized to IPv4");

        // dual-stack → IPv4 (using the normalized address from recv_from)
        let reply_target = SocketAddr::new(src.ip(), v4_port);
        <UdpSocket as Socket>::send_to(&dual_sock, b"pong", reply_target)
            .await
            .expect("sending to normalized IPv4 addr from IPv6 socket should work");

        let (len, _) = <UdpSocket as Socket>::recv_from(&v4_sock, &mut buf)
            .await
            .unwrap();
        assert_eq!(&buf[..len], b"pong");
    }

    /// The address family cached at bind time (used per-send in place of a
    /// `getsockname` syscall) must agree with what the kernel reports, for
    /// plain v4, plain v6, and a dual-stack `[::]` bind.
    #[tokio::test]
    async fn cached_family_matches_local_addr() {
        for (addr, expect_ipv6) in [
            ("127.0.0.1:0", false),
            ("0.0.0.0:0", false),
            ("[::1]:0", true),
            ("[::]:0", true),
        ] {
            let addr: SocketAddr = addr.parse().unwrap();
            let sock = match <UdpTransportSocket as Socket>::bind(addr).await {
                Ok(s) => s,
                // Sandboxes without IPv6 cannot bind v6 addresses; skip only
                // for that, not for any error.
                Err(e)
                    if addr.is_ipv6()
                        && matches!(
                            e.kind(),
                            io::ErrorKind::AddrNotAvailable | io::ErrorKind::Unsupported
                        ) =>
                {
                    eprintln!("skipping {addr}: IPv6 unavailable ({e})");
                    continue;
                }
                Err(e) => panic!("bind {addr} failed: {e}"),
            };
            let local = sock.local_addr().unwrap();
            assert_eq!(sock.is_ipv6(), local.is_ipv6(), "bound to {addr}");
            assert_eq!(sock.is_ipv6(), expect_ipv6, "bound to {addr}");
        }
    }

    /// The cached-family socket must behave like the plain impl: v4 targets
    /// reach a dual-stack receiver, and sends update the packet metrics.
    #[tokio::test]
    async fn transport_socket_sends_to_dual_stack_and_records_metrics() {
        let dual_addr: SocketAddr = "[::]:0".parse().unwrap();
        let Ok(dual) = <UdpTransportSocket as Socket>::bind(dual_addr).await else {
            eprintln!("skipping: IPv6 unavailable");
            return;
        };
        let v4 = <UdpTransportSocket as Socket>::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let target: SocketAddr = format!("127.0.0.1:{}", dual.local_addr().unwrap().port())
            .parse()
            .unwrap();
        <UdpTransportSocket as Socket>::send_to(&v4, b"hi", target)
            .await
            .unwrap();
        let mut buf = [0u8; 16];
        let (len, src) = <UdpTransportSocket as Socket>::recv_from(&dual, &mut buf)
            .await
            .unwrap();
        assert_eq!(&buf[..len], b"hi");
        assert!(src.is_ipv4(), "source must be normalized, got {src}");
        // Metrics are keyed by the un-mapped target.
        assert!(
            crate::transport::metrics::TRANSPORT_METRICS
                .per_peer_snapshot()
                .iter()
                .any(|(a, sent, _)| *a == target && *sent >= 2),
            "send_to must record per-peer bytes_sent for {target}"
        );
        // Reply from the dual-stack socket to a plain v4 peer (needs mapping).
        let reply = SocketAddr::new(
            "127.0.0.1".parse().unwrap(),
            v4.local_addr().unwrap().port(),
        );
        <UdpTransportSocket as Socket>::send_to_blocking(&dual, b"yo", reply).unwrap();
        let (len, _) = <UdpTransportSocket as Socket>::recv_from(&v4, &mut buf)
            .await
            .unwrap();
        assert_eq!(&buf[..len], b"yo");
    }

    /// Regression: every successful send_to on `UdpSocket` (and, via
    /// `transport_socket_sends_to_dual_stack_and_records_metrics`, on
    /// `UdpTransportSocket`) must update the cumulative + per-peer counters used by the local
    /// dashboard. Before #3996, those counters only moved on stream-transfer
    /// completion, so a node connected for hours could legitimately show "—"
    /// for SENT despite real keep-alive traffic flowing.
    ///
    /// Receive-side accounting is owned by the post-authentication hook in
    /// `peer_connection.rs` (see #3999), so this test only asserts send
    /// behavior — `recv_from` deliberately does NOT bump any counter.
    #[tokio::test]
    async fn udp_socket_records_packet_metrics() {
        use crate::transport::metrics::TRANSPORT_METRICS;

        let v4_addr = SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0);
        let sender = <UdpSocket as Socket>::bind(v4_addr).await.unwrap();
        let receiver = <UdpSocket as Socket>::bind(v4_addr).await.unwrap();
        let receiver_addr = receiver.local_addr().unwrap();
        let sender_addr = sender.local_addr().unwrap();

        let cumulative_sent_before = TRANSPORT_METRICS.cumulative_bytes_sent();

        let payload = b"keep-alive-sized-control-packet";
        <UdpSocket as Socket>::send_to(&sender, payload, receiver_addr)
            .await
            .unwrap();

        let mut buf = [0u8; 64];
        let (len, _) = <UdpSocket as Socket>::recv_from(&receiver, &mut buf)
            .await
            .unwrap();
        assert_eq!(len, payload.len());

        // Send-side cumulative counter must move at least by the payload.
        // Other tests running in parallel may push it higher, hence the >=.
        assert!(
            TRANSPORT_METRICS.cumulative_bytes_sent()
                >= cumulative_sent_before + payload.len() as u64,
            "cumulative_bytes_sent must include the payload"
        );

        // Per-peer entry for the send target must exist with bytes_sent
        // covering the payload. The reverse-direction entry (keyed by the
        // sender's address) must NOT be created by `recv_from` alone —
        // recv-side accounting moved to the post-auth hook in #3999, so
        // an unauthenticated packet contributes nothing to per-peer state.
        let peers = TRANSPORT_METRICS.per_peer_snapshot();
        let receiver_entry = peers
            .iter()
            .find(|(a, _, _)| *a == receiver_addr)
            .expect("per-peer entry for send target must exist");
        assert!(
            receiver_entry.1 >= payload.len() as u64,
            "per-peer bytes_sent must include the payload (got {})",
            receiver_entry.1
        );
        assert!(
            peers.iter().all(|(a, _, _)| *a != sender_addr),
            "recv_from must not create a per-peer entry pre-authentication"
        );
    }

    /// Regression: failed sends must not bump the metrics. Without this,
    /// a future refactor could move the increment before the result check
    /// and silently inflate the counters on every error.
    #[tokio::test]
    async fn udp_socket_failed_send_does_not_record_metrics() {
        use crate::transport::metrics::TRANSPORT_METRICS;

        let v4_addr = SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0);
        let sender = <UdpSocket as Socket>::bind(v4_addr).await.unwrap();

        // Connect the socket to a known-bad address so subsequent sends
        // synchronously fail with ECONNREFUSED on Linux. This is the
        // simplest way to force a Send error from `send_to`.
        sender
            .connect("127.0.0.1:1") // port 1 is reserved; nothing listens
            .await
            .ok();

        let unbound: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let cumulative_sent_before = TRANSPORT_METRICS.cumulative_bytes_sent();

        // Capture per-peer state before — the exact behavior depends on the
        // OS (some kernels still accept the sendto), so we tolerate either
        // outcome but assert that *if* the send fails, no metric moves.
        let result = <UdpSocket as Socket>::send_to(&sender, b"x", unbound).await;
        if result.is_err() {
            assert_eq!(
                TRANSPORT_METRICS.cumulative_bytes_sent(),
                cumulative_sent_before,
                "failed send must not bump cumulative_bytes_sent"
            );
            // Per-peer entry for the unbound address should also not exist
            // due to this send. (Other tests may have created it; this is
            // why we don't assert absence outright.)
        }
    }

    /// Verify that `send_to_blocking` (the synchronous code path used in
    /// `spawn_blocking` contexts) also records metrics on success. Same
    /// dashboard-correctness guarantee as the async path. Runs inside a
    /// `spawn_blocking` so the call is in a real blocking context with the
    /// tokio reactor still active for `try_send_to` polling.
    #[tokio::test]
    async fn udp_socket_blocking_send_records_packet_metrics() {
        use crate::transport::metrics::TRANSPORT_METRICS;
        use std::sync::Arc;

        let v4_addr = SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0);
        let sender = Arc::new(<UdpSocket as Socket>::bind(v4_addr).await.unwrap());
        let receiver = <UdpSocket as Socket>::bind(v4_addr).await.unwrap();
        let receiver_addr = receiver.local_addr().unwrap();

        let cumulative_sent_before = TRANSPORT_METRICS.cumulative_bytes_sent();
        let payload: &'static [u8] = b"blocking-send-payload";
        let sender_clone = sender.clone();

        tokio::task::spawn_blocking(move || {
            <UdpSocket as Socket>::send_to_blocking(&sender_clone, payload, receiver_addr)
                .expect("blocking send should succeed on localhost UDP");
        })
        .await
        .unwrap();

        // Drain the receiver so the kernel buffer doesn't leak across tests.
        let mut buf = [0u8; 64];
        let _drain = <UdpSocket as Socket>::recv_from(&receiver, &mut buf).await;

        assert!(
            TRANSPORT_METRICS.cumulative_bytes_sent()
                >= cumulative_sent_before + payload.len() as u64,
            "send_to_blocking must record cumulative_bytes_sent"
        );
        let peers = TRANSPORT_METRICS.per_peer_snapshot();
        let entry = peers
            .iter()
            .find(|(a, _, _)| *a == receiver_addr)
            .expect("per-peer entry must exist after blocking send");
        assert!(
            entry.1 >= payload.len() as u64,
            "send_to_blocking must record per-peer bytes_sent"
        );
    }

    /// On a dual-stack IPv6 socket sending to an IPv4 target, the kernel
    /// receives the AF_INET6-mapped form, but the per-peer entry MUST be
    /// keyed by the canonical (un-mapped) IPv4 form. Otherwise the dashboard
    /// would split a single peer across two map entries when the local
    /// socket happens to be IPv6.
    #[tokio::test]
    async fn udp_socket_dual_stack_send_keys_per_peer_by_unmapped_addr() {
        use crate::transport::metrics::TRANSPORT_METRICS;

        let dual_addr = SocketAddr::new(std::net::IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0);
        let dual_sock = <UdpSocket as Socket>::bind(dual_addr).await.unwrap();
        let v4_addr = SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0);
        let v4_recv = <UdpSocket as Socket>::bind(v4_addr).await.unwrap();
        let v4_recv_port = v4_recv.local_addr().unwrap().port();

        let target = SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            v4_recv_port,
        );
        assert!(target.is_ipv4(), "test precondition");

        <UdpSocket as Socket>::send_to(&dual_sock, b"dual-stack", target)
            .await
            .unwrap();

        let peers = TRANSPORT_METRICS.per_peer_snapshot();
        let entry = peers
            .iter()
            .find(|(a, _, _)| *a == target)
            .expect("per-peer entry must be keyed by un-mapped IPv4 target");
        assert!(
            entry.0.is_ipv4(),
            "per-peer key must be plain IPv4, not ::ffff:x.x.x.x"
        );
        // Same target in mapped form must NOT have its own entry.
        let mapped: SocketAddr = format!("[::ffff:127.0.0.1]:{v4_recv_port}")
            .parse()
            .unwrap();
        assert!(
            peers.iter().all(|(a, _, _)| *a != mapped),
            "mapped form must not produce a duplicate per-peer entry"
        );
    }

    /// Regression for #3999: an attacker that fabricates UDP packets from
    /// many spoofed source IPs MUST NOT be able to inflate the dashboard
    /// cumulative_bytes_received counter or fill the bounded per-peer
    /// table. `recv_from` is the kernel handoff point that sees every
    /// packet regardless of authentication, so the fix is "don't meter
    /// here" — receive-side metering moved to the post-`try_decrypt_sym`
    /// hook in `peer_connection::PeerConnection::recv`.
    #[tokio::test]
    async fn udp_socket_recv_from_does_not_record_pre_authentication() {
        use crate::transport::metrics::TRANSPORT_METRICS;

        let v4_addr = SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0);
        let attacker = <UdpSocket as Socket>::bind(v4_addr).await.unwrap();
        let victim = <UdpSocket as Socket>::bind(v4_addr).await.unwrap();
        let victim_addr = victim.local_addr().unwrap();
        let attacker_addr = attacker.local_addr().unwrap();

        // Simulate a forged UDP packet arriving at the listening port: the
        // "attacker" socket stands in for any unauthenticated sender.
        let payload = vec![0xAB; 1024];
        attacker.send_to(&payload, victim_addr).await.unwrap();

        let mut buf = [0u8; 2048];
        let (len, src) = <UdpSocket as Socket>::recv_from(&victim, &mut buf)
            .await
            .unwrap();
        assert_eq!(len, payload.len());
        assert_eq!(src, attacker_addr);

        // The packet never reached `try_decrypt_sym`, so it must not have
        // created a per-peer entry. (Cumulative is process-global and may
        // be moved by concurrent authenticated paths; only per-peer
        // absence is reliably observable from this test.)
        let peers = TRANSPORT_METRICS.per_peer_snapshot();
        assert!(
            peers.iter().all(|(a, _, _)| *a != attacker_addr),
            "spoofed (un-authenticated) source MUST NOT create a per-peer entry"
        );
    }

    /// Bind a control socket via raw `tokio::net::UdpSocket::bind` and a tuned
    /// socket via our `Socket::bind` impl at the same address family, read
    /// back SO_RCVBUF / SO_SNDBUF on both, and enforce the regression
    /// invariants below.
    ///
    /// **Weak invariant (all platforms):** the tuned socket must never end up
    /// with a smaller buffer than the untuned control. setsockopt either
    /// succeeds, gets clamped, or fails — none of those paths should produce
    /// a smaller buffer than the un-tuned default.
    ///
    /// **Strict invariant (Linux):** `tuned > control`. Linux's
    /// `setsockopt(SO_RCVBUF, val)` sets `sk_rcvbuf = min(val, rmem_max) * 2`,
    /// but the un-tuned socket inherits `rmem_default` *without* the
    /// doubling. So on stock Ubuntu (rmem_max = rmem_default = 212992) the
    /// tuned socket lands at 425984 while the control sits at 212992 —
    /// strictly larger. Deleting `set_recv_buffer_size` / `set_send_buffer_size`
    /// collapses that gap and the strict assertion fires.
    async fn assert_tuned_bind_grows_buffers(addr: SocketAddr) {
        // Control: raw bind, no buffer-size hints applied.
        let control = tokio::net::UdpSocket::bind(addr)
            .await
            .expect("control bind should succeed");
        let control_sock2 = socket2::Socket::from(control.into_std().expect("into_std"));
        let control_rcv = control_sock2
            .recv_buffer_size()
            .expect("control recv_buffer_size");
        let control_snd = control_sock2
            .send_buffer_size()
            .expect("control send_buffer_size");

        // Tuned: bind via our Socket::bind, which should request 16 MiB
        // SO_RCVBUF / SO_SNDBUF before bind.
        let tuned = <UdpSocket as Socket>::bind(addr)
            .await
            .expect("tuned bind should succeed");
        let tuned_sock2 = socket2::Socket::from(tuned.into_std().expect("into_std"));
        let tuned_rcv = tuned_sock2
            .recv_buffer_size()
            .expect("tuned recv_buffer_size");
        let tuned_snd = tuned_sock2
            .send_buffer_size()
            .expect("tuned send_buffer_size");

        assert!(
            tuned_rcv >= control_rcv,
            "tuned recv buffer ({tuned_rcv}) must be at least as large as control ({control_rcv})"
        );
        assert!(
            tuned_snd >= control_snd,
            "tuned send buffer ({tuned_snd}) must be at least as large as control ({control_snd})"
        );

        #[cfg(target_os = "linux")]
        {
            assert!(
                tuned_rcv > control_rcv,
                "Linux: tuned SO_RCVBUF ({tuned_rcv}) must strictly exceed \
                 untuned control ({control_rcv}) — the fix did not take effect"
            );
            assert!(
                tuned_snd > control_snd,
                "Linux: tuned SO_SNDBUF ({tuned_snd}) must strictly exceed \
                 untuned control ({control_snd}) — the fix did not take effect"
            );
        }
    }

    /// Regression for #3844 (IPv4 path): `Socket::bind` must request a generous
    /// UDP socket buffer so production gateways aren't silently dropping
    /// packets to the kernel's stock ~208 KiB default. Both nova and vega
    /// were accumulating UdpRcvbufErrors at 15× the delivered-datagram rate
    /// until operators raised `net.core.rmem_max` manually.
    #[tokio::test]
    async fn udp_socket_bind_requests_large_buffers_ipv4() {
        let v4_addr = SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0);
        assert_tuned_bind_grows_buffers(v4_addr).await;
    }

    /// Regression for #3844 (IPv6 path): the tuning must apply equally to the
    /// dual-stack `[::]:port` bind that gateways actually use in production.
    /// `Ipv6Addr::UNSPECIFIED` exercises the same code path (the `is_ipv6`
    /// branch + `set_only_v6(false)` + the new setsockopt calls).
    #[tokio::test]
    async fn udp_socket_bind_requests_large_buffers_ipv6() {
        let v6_addr = SocketAddr::new(std::net::IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED), 0);
        assert_tuned_bind_grows_buffers(v6_addr).await;
    }
}
