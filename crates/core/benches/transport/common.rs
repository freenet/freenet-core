//! Common utilities for transport benchmarks
//!
//! This module provides shared helpers to reduce duplication across benchmark files.
//! Key abstractions:
//! - `ConnectedPeerPair`: Encapsulates peer creation + connection establishment
//! - Runtime/throughput/formatting utilities
//!
//! ## Usage
//!
//! ```rust,ignore
//! use common::{create_connected_peers, ConnectedPeerPair};
//!
//! // Simple case - instant connection
//! let ConnectedPeerPair { mut conn_a, mut conn_b, .. } = create_connected_peers().await;
//!
//! // With delay
//! let mut peers = create_connected_peers_with_delay(Duration::from_millis(2)).await;
//! peers.warmup(5, 1024).await; // 5 warmup transfers of 1KB each
//! ```

use dashmap::DashMap;
use freenet::transport::mock_transport::{
    create_mock_peer, create_mock_peer_with_delay, Channels, MockSocket, PacketDelayPolicy,
    PacketDropPolicy,
};
use freenet::transport::{OutboundConnectionHandler, PeerConnection, TransportPublicKey};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

// =============================================================================
// Runtime Creation
// =============================================================================

/// Create a multi-threaded tokio runtime for benchmarks
///
/// Uses 4 worker threads which is suitable for most benchmark scenarios.
pub fn create_benchmark_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("Failed to create benchmark runtime")
}

// =============================================================================
// Peer Creation & Connection
// =============================================================================

/// Unconnected peer pair
///
/// Created by `create_peer_pair()` functions. Call `.connect()` to establish
/// bidirectional connections and get a `ConnectedPeerPair`.
pub struct PeerPair {
    pub peer_a_pub: TransportPublicKey,
    pub peer_a: OutboundConnectionHandler<MockSocket>,
    pub peer_a_addr: SocketAddr,
    pub peer_b_pub: TransportPublicKey,
    pub peer_b: OutboundConnectionHandler<MockSocket>,
    pub peer_b_addr: SocketAddr,
}

/// Connected peer pair ready for data transfer
///
/// Encapsulates both connections and their peer handlers. The peer handlers
/// MUST be kept alive - dropping them closes the `inbound_packet_sender`
/// channel which causes `ConnectionClosed` errors.
///
/// ## Example
///
/// ```rust,ignore
/// let ConnectedPeerPair { mut conn_a, mut conn_b, .. } = create_connected_peers().await;
/// conn_a.send(message).await.unwrap();
/// let received: Vec<u8> = conn_b.recv().await.unwrap();
/// ```
pub struct ConnectedPeerPair {
    pub conn_a: PeerConnection<MockSocket>,
    pub conn_b: PeerConnection<MockSocket>,
    /// Must keep peer_a alive - it holds the inbound_packet_sender channel
    #[allow(dead_code)]
    peer_a: OutboundConnectionHandler<MockSocket>,
    /// Must keep peer_b alive - it holds the inbound_packet_sender channel
    #[allow(dead_code)]
    peer_b: OutboundConnectionHandler<MockSocket>,
}

impl PeerPair {
    /// Establish bidirectional connections between peers
    ///
    /// This performs the connection handshake and returns a `ConnectedPeerPair`
    /// ready for data transfer.
    pub async fn connect(mut self) -> ConnectedPeerPair {
        let (conn_a_inner, conn_b_inner) = futures::join!(
            self.peer_a.connect(self.peer_b_pub, self.peer_b_addr),
            self.peer_b.connect(self.peer_a_pub, self.peer_a_addr),
        );
        let (conn_a, conn_b) = futures::join!(conn_a_inner, conn_b_inner);
        let (conn_a, conn_b) = (conn_a.expect("connect A"), conn_b.expect("connect B"));

        ConnectedPeerPair {
            conn_a,
            conn_b,
            peer_a: self.peer_a,
            peer_b: self.peer_b,
        }
    }
}

impl ConnectedPeerPair {
    /// Warmup the connection with N transfers to stabilize LEDBAT cwnd
    ///
    /// Performs `iterations` send/receive cycles to allow LEDBAT congestion
    /// control to reach steady state before measurements begin.
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// let mut peers = create_connected_peers().await;
    /// peers.warmup(5, 65536).await; // 5 x 64KB warmup transfers
    /// ```
    pub async fn warmup(&mut self, iterations: usize, message_size: usize) {
        for _ in 0..iterations {
            let msg = vec![0xABu8; message_size];
            self.conn_a.send(msg).await.expect("warmup send");
            let _: Vec<u8> = self.conn_b.recv().await.expect("warmup recv");
        }
    }

    /// Get mutable references to both connections
    ///
    /// Useful when you need to pass connections separately to different tasks.
    pub fn connections_mut(
        &mut self,
    ) -> (
        &mut PeerConnection<MockSocket>,
        &mut PeerConnection<MockSocket>,
    ) {
        (&mut self.conn_a, &mut self.conn_b)
    }
}

/// Create a new pair of mock peers with no delay
pub async fn create_peer_pair(channels: Channels) -> PeerPair {
    let (peer_a_pub, peer_a, peer_a_addr) =
        create_mock_peer(PacketDropPolicy::ReceiveAll, channels.clone())
            .await
            .expect("create peer A");

    let (peer_b_pub, peer_b, peer_b_addr) =
        create_mock_peer(PacketDropPolicy::ReceiveAll, channels)
            .await
            .expect("create peer B");

    PeerPair {
        peer_a_pub,
        peer_a,
        peer_a_addr,
        peer_b_pub,
        peer_b,
        peer_b_addr,
    }
}

/// Create a new pair of mock peers with fixed delay
pub async fn create_peer_pair_with_delay(channels: Channels, delay: Duration) -> PeerPair {
    let (peer_a_pub, peer_a, peer_a_addr) = create_mock_peer_with_delay(
        PacketDropPolicy::ReceiveAll,
        PacketDelayPolicy::Fixed(delay),
        channels.clone(),
    )
    .await
    .expect("create peer A");

    let (peer_b_pub, peer_b, peer_b_addr) = create_mock_peer_with_delay(
        PacketDropPolicy::ReceiveAll,
        PacketDelayPolicy::Fixed(delay),
        channels,
    )
    .await
    .expect("create peer B");

    PeerPair {
        peer_a_pub,
        peer_a,
        peer_a_addr,
        peer_b_pub,
        peer_b,
        peer_b_addr,
    }
}

// =============================================================================
// Convenience Functions
// =============================================================================

/// Create a connected peer pair with no delay (most common case)
///
/// This is the simplest way to get two connected peers for benchmarking.
/// Creates fresh channels and establishes connections in one call.
///
/// ## Example
///
/// ```rust,ignore
/// let ConnectedPeerPair { mut conn_a, mut conn_b, .. } = create_connected_peers().await;
/// conn_a.send(vec![0u8; 1024]).await.unwrap();
/// let data: Vec<u8> = conn_b.recv().await.unwrap();
/// ```
pub async fn create_connected_peers() -> ConnectedPeerPair {
    let channels: Channels = Arc::new(DashMap::new());
    create_peer_pair(channels).await.connect().await
}

/// Create a connected peer pair with fixed delay
///
/// Use this when testing with simulated network latency (e.g., 2ms for LAN).
pub async fn create_connected_peers_with_delay(delay: Duration) -> ConnectedPeerPair {
    let channels: Channels = Arc::new(DashMap::new());
    create_peer_pair_with_delay(channels, delay)
        .await
        .connect()
        .await
}

/// Create a fresh channels map
///
/// Used when you need to share channels across multiple peer pairs
/// (e.g., for concurrent stream tests).
pub fn new_channels() -> Channels {
    Arc::new(DashMap::new())
}

// =============================================================================
// Throughput Utilities
// =============================================================================

/// Calculate throughput in Mbps from bytes transferred and duration
///
/// ## Example
///
/// ```rust
/// use std::time::Duration;
/// # use freenet::benches::transport::common::calculate_throughput_mbps;
///
/// let bytes = 1_000_000; // 1 MB
/// let duration = Duration::from_secs(1);
/// let mbps = calculate_throughput_mbps(bytes, duration);
/// assert_eq!(mbps, 8.0); // 1 MB/s = 8 Mbps
/// ```
pub fn calculate_throughput_mbps(bytes: usize, elapsed: Duration) -> f64 {
    let total_bytes = bytes as f64;
    (total_bytes * 8.0) / elapsed.as_secs_f64() / 1_000_000.0
}

/// Format duration for display
///
/// Uses milliseconds for durations >= 1ms, microseconds otherwise.
pub fn format_duration(d: Duration) -> String {
    if d.as_millis() > 0 {
        format!("{:.2}ms", d.as_secs_f64() * 1000.0)
    } else {
        format!("{:.2}µs", d.as_micros())
    }
}

/// Format throughput for display
///
/// Uses Mbps for throughput >= 1 Mbps, Kbps otherwise.
pub fn format_throughput(mbps: f64) -> String {
    if mbps >= 1.0 {
        format!("{:.2} Mbps", mbps)
    } else {
        format!("{:.2} Kbps", mbps * 1000.0)
    }
}

// =============================================================================
// Standard Test Configurations
// =============================================================================

/// Standard small message sizes for benchmarks (bytes, label)
///
/// Use these for consistency across benchmarks: 1KB, 4KB, 16KB
pub const SMALL_SIZES: &[(usize, &str)] = &[(1024, "1kb"), (4096, "4kb"), (16384, "16kb")];

/// Standard large message sizes for streaming benchmarks
///
/// Use these for multi-packet transfer tests: 4KB, 16KB, 64KB
pub const STREAM_SIZES: &[usize] = &[4096, 16384, 65536];

/// Standard delay values in microseconds for slow-start testing
///
/// 500µs and 2ms provide good coverage of LEDBAT dynamics
pub const DELAY_MICROS: &[u64] = &[500, 2000];

/// Standard transfer sizes in KB for throughput tests
///
/// 256KB and 1MB are good sizes for measuring sustained throughput
pub const TRANSFER_SIZES_KB: &[usize] = &[256, 1024];
