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

use criterion::measurement::{Measurement, ValueFormatter};
use dashmap::DashMap;
use freenet::simulation::{RealTime, TimeSource, VirtualTime};
use freenet::transport::mock_transport::{
    create_mock_peer, create_mock_peer_with_delay, create_mock_peer_with_virtual_time, Channels,
    MockSocket, PacketDelayPolicy, PacketDropPolicy,
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
///
/// Generic over `TS: TimeSource` to support both RealTime (wall-clock) and
/// VirtualTime (instant simulation) timing modes.
pub struct PeerPair<TS: TimeSource = RealTime> {
    pub peer_a_pub: TransportPublicKey,
    pub peer_a: OutboundConnectionHandler<MockSocket, TS>,
    pub peer_a_addr: SocketAddr,
    pub peer_b_pub: TransportPublicKey,
    pub peer_b: OutboundConnectionHandler<MockSocket, TS>,
    pub peer_b_addr: SocketAddr,
    /// Keep channels alive - dropping this closes MockSocket inbound channels
    #[allow(dead_code)]
    channels: Channels,
}

/// Connected peer pair ready for data transfer
///
/// Encapsulates both connections and their peer handlers. The channels map
/// MUST be kept alive - dropping it closes the MockSocket inbound channels
/// which causes the listener tasks to exit and `ConnectionClosed` errors.
///
/// Generic over `TS: TimeSource` to support both RealTime (wall-clock) and
/// VirtualTime (instant simulation) timing modes.
///
/// ## Example
///
/// ```rust,ignore
/// let ConnectedPeerPair { mut conn_a, mut conn_b, .. } = create_connected_peers().await;
/// conn_a.send(message).await.unwrap();
/// let received: Vec<u8> = conn_b.recv().await.unwrap();
/// ```
pub struct ConnectedPeerPair<TS: TimeSource = RealTime> {
    pub conn_a: PeerConnection<MockSocket, TS>,
    pub conn_b: PeerConnection<MockSocket, TS>,
    /// Must keep peer_a alive for send_queue channel
    #[allow(dead_code)]
    peer_a: OutboundConnectionHandler<MockSocket, TS>,
    /// Must keep peer_b alive for send_queue channel
    #[allow(dead_code)]
    peer_b: OutboundConnectionHandler<MockSocket, TS>,
    /// Keep channels alive - dropping this closes MockSocket inbound channels
    #[allow(dead_code)]
    channels: Channels,
}

impl PeerPair<RealTime> {
    /// Establish bidirectional connections between peers (RealTime variant)
    ///
    /// This performs the connection handshake and returns a `ConnectedPeerPair`
    /// ready for data transfer.
    pub async fn connect(mut self) -> ConnectedPeerPair<RealTime> {
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
            channels: self.channels,
        }
    }
}

impl PeerPair<VirtualTime> {
    /// Establish bidirectional connections between peers (VirtualTime variant)
    ///
    /// This performs the connection handshake and returns a `ConnectedPeerPair`
    /// ready for data transfer. Uses VirtualTime for instant delay simulation.
    pub async fn connect(mut self) -> ConnectedPeerPair<VirtualTime> {
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
            channels: self.channels,
        }
    }
}

impl<TS: TimeSource> ConnectedPeerPair<TS> {
    /// Warmup the connection with N transfers to stabilize LEDBAT cwnd
    ///
    /// Performs `iterations` send/receive cycles to allow LEDBAT congestion
    /// control to reach steady state before measurements begin.
    ///
    /// Returns the number of successful warmup iterations. If warmup fails
    /// early, benchmarks can still proceed (the connection may be less stable).
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// let mut peers = create_connected_peers().await;
    /// let completed = peers.warmup(5, 65536).await; // 5 x 64KB warmup transfers
    /// ```
    pub async fn warmup(&mut self, iterations: usize, message_size: usize) -> usize {
        let mut completed = 0;
        for i in 0..iterations {
            let msg = vec![0xABu8; message_size];
            if let Err(e) = self.conn_a.send(msg).await {
                eprintln!("Warmup send {} failed: {:?}", i, e);
                break;
            }
            match self.conn_b.recv().await {
                Ok(_) => completed += 1,
                Err(e) => {
                    eprintln!("Warmup recv {} failed: {:?}", i, e);
                    break;
                }
            }
        }
        completed
    }

    /// Get mutable references to both connections
    ///
    /// Useful when you need to pass connections separately to different tasks.
    pub fn connections_mut(
        &mut self,
    ) -> (
        &mut PeerConnection<MockSocket, TS>,
        &mut PeerConnection<MockSocket, TS>,
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
        create_mock_peer(PacketDropPolicy::ReceiveAll, channels.clone())
            .await
            .expect("create peer B");

    PeerPair {
        peer_a_pub,
        peer_a,
        peer_a_addr,
        peer_b_pub,
        peer_b,
        peer_b_addr,
        channels,
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
        channels.clone(),
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
        channels,
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
// VirtualTime Support
// =============================================================================

/// Spawn an auto-advance monitoring task for VirtualTime.
///
/// This task periodically checks for blocked VirtualSleep futures and advances
/// time to the next wakeup deadline when needed. This prevents deadlocks when
/// all tasks are waiting on timeouts.
///
/// **Must be called from within a tokio runtime context.**
///
/// ## Usage
///
/// ```rust,ignore
/// let time_source = VirtualTime::new();
/// let _auto_advance_handle = spawn_auto_advance_task(time_source.clone());
///
/// // Now VirtualTime benchmarks won't deadlock
/// let peers = create_peer_pair_with_virtual_time(channels, delay, time_source).await;
/// ```
///
/// ## Returns
///
/// A `JoinHandle` for the monitoring task. Call `.abort()` to stop it.
pub fn spawn_auto_advance_task(time_source: VirtualTime) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            // Yield to let protocol tasks run first
            tokio::task::yield_now().await;

            // Advance time in small steps (10ms) to avoid triggering protocol timeouts
            // The connection idle timeout is 120s, so we need to advance slowly enough
            // that packets can be exchanged between advances
            if time_source
                .try_auto_advance_bounded(Duration::from_millis(10))
                .is_some()
            {
                // Time advanced - continue to let tasks process
                continue;
            }

            // No pending wakeups - sleep briefly to avoid busy-loop
            tokio::time::sleep(Duration::from_micros(50)).await;
        }
    })
}

/// Create a pair of mock peers with VirtualTime support for instant delay simulation.
///
/// When using VirtualTime, delays advance the virtual clock instead of waiting real time.
/// This enables instant simulation of high-RTT scenarios (e.g., 300ms RTT completes in ~1ms).
///
/// The entire transport stack (MockSocket delays AND protocol timers) uses VirtualTime,
/// ensuring consistent timing behavior for benchmarks.
pub async fn create_peer_pair_with_virtual_time(
    channels: Channels,
    delay: Duration,
    time_source: VirtualTime,
) -> PeerPair<VirtualTime> {
    let (peer_a_pub, peer_a, peer_a_addr) = create_mock_peer_with_virtual_time(
        PacketDropPolicy::ReceiveAll,
        PacketDelayPolicy::Fixed(delay),
        channels.clone(),
        time_source.clone(),
    )
    .await
    .expect("create peer A");

    let (peer_b_pub, peer_b, peer_b_addr) = create_mock_peer_with_virtual_time(
        PacketDropPolicy::ReceiveAll,
        PacketDelayPolicy::Fixed(delay),
        channels.clone(),
        time_source,
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
        channels,
    }
}

/// Create a connected peer pair with VirtualTime for instant high-RTT simulation.
///
/// ## Example
///
/// ```rust,ignore
/// let time_source = VirtualTime::new();
/// let peers = create_connected_peers_with_virtual_time(
///     Duration::from_millis(100), // Simulated 100ms RTT
///     time_source.clone(),
/// ).await;
///
/// // Transfer completes in ~1ms wall time but simulates 100ms network delay
/// peers.conn_a.send(message).await.unwrap();
/// let received = peers.conn_b.recv().await.unwrap();
///
/// // Check virtual time elapsed
/// let elapsed = Duration::from_nanos(time_source.now_nanos());
/// ```
pub async fn create_connected_peers_with_virtual_time(
    delay: Duration,
    time_source: VirtualTime,
) -> ConnectedPeerPair<VirtualTime> {
    let channels: Channels = Arc::new(DashMap::new());
    create_peer_pair_with_virtual_time(channels, delay, time_source)
        .await
        .connect()
        .await
}

/// Create a pair of mock peers with VirtualTime and custom packet drop policy.
///
/// Useful for testing packet loss scenarios with instant execution.
pub async fn create_peer_pair_with_virtual_time_and_loss(
    channels: Channels,
    delay: Duration,
    time_source: VirtualTime,
    drop_policy: PacketDropPolicy,
) -> PeerPair<VirtualTime> {
    let (peer_a_pub, peer_a, peer_a_addr) = create_mock_peer_with_virtual_time(
        drop_policy.clone(),
        PacketDelayPolicy::Fixed(delay),
        channels.clone(),
        time_source.clone(),
    )
    .await
    .expect("create peer A");

    let (peer_b_pub, peer_b, peer_b_addr) = create_mock_peer_with_virtual_time(
        drop_policy,
        PacketDelayPolicy::Fixed(delay),
        channels.clone(),
        time_source,
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
        channels,
    }
}

/// Create a connected peer pair with VirtualTime and packet loss for resilience testing.
pub async fn create_connected_peers_with_virtual_time_and_loss(
    delay: Duration,
    time_source: VirtualTime,
    drop_policy: PacketDropPolicy,
) -> ConnectedPeerPair<VirtualTime> {
    let channels: Channels = Arc::new(DashMap::new());
    create_peer_pair_with_virtual_time_and_loss(channels, delay, time_source, drop_policy)
        .await
        .connect()
        .await
}

// =============================================================================
// VirtualTime Criterion Measurement
// =============================================================================

/// Custom Criterion measurement using VirtualTime.
///
/// Captures virtual time instead of wall time, enabling instant simulation
/// of high-latency network scenarios. Benchmarks complete in milliseconds
/// regardless of simulated RTT, while reporting realistic throughput.
///
/// ## How It Works
///
/// 1. `start()` captures the current virtual time (nanoseconds)
/// 2. Benchmark code runs, MockSocket advances virtual time on delays
/// 3. `end()` returns the elapsed virtual time
/// 4. Criterion reports throughput as `bytes / virtual_seconds`
///
/// ## Example
///
/// ```rust,ignore
/// let time_source = VirtualTime::new();
/// let mut criterion = Criterion::default()
///     .with_measurement(VirtualTimeMeasurement::new(time_source.clone()));
///
/// // Benchmark runs in ~1ms wall time, reports 640 KB/s for 64KB @ 100ms RTT
/// ```
#[derive(Clone)]
pub struct VirtualTimeMeasurement {
    time_source: VirtualTime,
}

impl VirtualTimeMeasurement {
    /// Create a new VirtualTimeMeasurement with the given time source.
    pub fn new(time_source: VirtualTime) -> Self {
        Self { time_source }
    }

    /// Get the underlying time source for use in benchmarks.
    pub fn time_source(&self) -> &VirtualTime {
        &self.time_source
    }
}

impl Measurement for VirtualTimeMeasurement {
    type Intermediate = u64;
    type Value = Duration;

    fn start(&self) -> Self::Intermediate {
        self.time_source.now_nanos()
    }

    fn end(&self, start: Self::Intermediate) -> Self::Value {
        let end = self.time_source.now_nanos();
        Duration::from_nanos(end.saturating_sub(start))
    }

    fn add(&self, v1: &Self::Value, v2: &Self::Value) -> Self::Value {
        *v1 + *v2
    }

    fn zero(&self) -> Self::Value {
        Duration::ZERO
    }

    fn to_f64(&self, val: &Self::Value) -> f64 {
        val.as_nanos() as f64
    }

    fn formatter(&self) -> &dyn ValueFormatter {
        &VirtualTimeFormatter
    }
}

/// Formatter for VirtualTime measurements.
///
/// Displays values with "(virtual)" suffix to distinguish from wall-clock measurements.
struct VirtualTimeFormatter;

impl ValueFormatter for VirtualTimeFormatter {
    fn scale_values(&self, typical_value: f64, values: &mut [f64]) -> &'static str {
        if typical_value < 1_000.0 {
            "ns (virtual)"
        } else if typical_value < 1_000_000.0 {
            for v in values.iter_mut() {
                *v /= 1_000.0;
            }
            "µs (virtual)"
        } else if typical_value < 1_000_000_000.0 {
            for v in values.iter_mut() {
                *v /= 1_000_000.0;
            }
            "ms (virtual)"
        } else {
            for v in values.iter_mut() {
                *v /= 1_000_000_000.0;
            }
            "s (virtual)"
        }
    }

    fn scale_throughputs(
        &self,
        typical_value: f64,
        throughput: &criterion::Throughput,
        values: &mut [f64],
    ) -> &'static str {
        match throughput {
            criterion::Throughput::Bytes(bytes) => {
                let bytes_per_ns = (*bytes as f64) / typical_value;
                let bytes_per_sec = bytes_per_ns * 1_000_000_000.0;

                if bytes_per_sec < 1024.0 {
                    for v in values.iter_mut() {
                        *v = (*bytes as f64) / *v * 1_000_000_000.0;
                    }
                    "B/s (simulated)"
                } else if bytes_per_sec < 1024.0 * 1024.0 {
                    for v in values.iter_mut() {
                        *v = (*bytes as f64) / *v * 1_000_000_000.0 / 1024.0;
                    }
                    "KiB/s (simulated)"
                } else if bytes_per_sec < 1024.0 * 1024.0 * 1024.0 {
                    for v in values.iter_mut() {
                        *v = (*bytes as f64) / *v * 1_000_000_000.0 / (1024.0 * 1024.0);
                    }
                    "MiB/s (simulated)"
                } else {
                    for v in values.iter_mut() {
                        *v = (*bytes as f64) / *v * 1_000_000_000.0 / (1024.0 * 1024.0 * 1024.0);
                    }
                    "GiB/s (simulated)"
                }
            }
            criterion::Throughput::Elements(elems) => {
                for v in values.iter_mut() {
                    *v = (*elems as f64) / *v * 1_000_000_000.0;
                }
                "elem/s (simulated)"
            }
            // Handle other throughput types (Bits, BytesDecimal, ElementsAndBytes)
            _ => {
                // For other types, just report raw values
                for v in values.iter_mut() {
                    *v = 1.0 / *v * 1_000_000_000.0;
                }
                "ops/s (simulated)"
            }
        }
    }

    fn scale_for_machines(&self, _values: &mut [f64]) -> &'static str {
        "ns"
    }
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
