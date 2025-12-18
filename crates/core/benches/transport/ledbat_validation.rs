//! LEDBAT Validation Benchmarks
//!
//! These benchmarks test LEDBAT congestion control behavior with separate
//! cold start and warm connection measurements.
//!
//! **Design principles:**
//! - Cold start: Create new connection per iteration (measures connection + transfer)
//! - Warm connection: Reuse connection across iterations (measures pure transfer)
//! - Keep OutboundConnectionHandler alive to prevent channel closure
//!
//! Total runtime: ~3-5 minutes

use criterion::{Criterion, Throughput};
use dashmap::DashMap;
use freenet::transport::mock_transport::{create_mock_peer, Channels, PacketDropPolicy};
use freenet::transport::{OutboundConnectionHandler, PeerConnection};
use std::hint::black_box as std_black_box;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Helper struct to keep connection and its peer handler alive together
struct LiveConnection {
    conn: PeerConnection,
    /// Must keep peer alive - it holds the inbound_packet_sender channel
    #[allow(dead_code)]
    peer: OutboundConnectionHandler,
}

/// Cold start benchmark: measures connection establishment + transfer
///
/// Each iteration creates a fresh connection, measuring real cold-start behavior.
/// This is useful for understanding first-message latency.
///
/// Tests multiple message sizes to understand the scaling behavior.
pub fn bench_large_transfer_validation(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("ledbat/cold_start");

    // Test multiple sizes: 1KB, 4KB, 16KB
    // Note: 64KB is skipped due to benchmark timeout issues with larger transfers
    for &(size, name) in &[(1024, "1kb"), (4096, "4kb"), (16384, "16kb")] {
        group.throughput(Throughput::Bytes(size as u64));

        // Cold start: connection created per iteration
        // Use iter() instead of iter_batched() for simpler async handling
        let size_bytes = size; // Capture for async block
        group.bench_function(name, |b| {
            b.to_async(&rt).iter(|| async move {
                let channels: Channels = Arc::new(DashMap::new());
                let message = vec![0xABu8; size_bytes];

                // Create peers fresh each iteration
                let (peer_a_pub, mut peer_a, peer_a_addr) =
                    create_mock_peer(PacketDropPolicy::ReceiveAll, channels.clone())
                        .await
                        .unwrap();

                let (peer_b_pub, mut peer_b, peer_b_addr) =
                    create_mock_peer(PacketDropPolicy::ReceiveAll, channels)
                        .await
                        .unwrap();

                // Connect
                let (conn_a_inner, conn_b_inner) = futures::join!(
                    peer_a.connect(peer_b_pub, peer_b_addr),
                    peer_b.connect(peer_a_pub, peer_a_addr),
                );
                let (conn_a, conn_b) = futures::join!(conn_a_inner, conn_b_inner);
                let (mut conn_a, mut conn_b) = (conn_a.unwrap(), conn_b.unwrap());

                // Transfer
                conn_a.send(message).await.unwrap();
                let received: Vec<u8> = conn_b.recv().await.unwrap();

                // Keep peers alive until transfer completes
                drop(peer_a);
                drop(peer_b);

                assert!(!received.is_empty());
                std_black_box(received);
            });
        });
    }

    group.finish();
}

/// Warm connection benchmark: measures pure transfer throughput
///
/// Connection is established once and reused across iterations.
/// This measures steady-state LEDBAT throughput without connection overhead.
///
/// **Key fix**: Keep OutboundConnectionHandler (peer) alive alongside PeerConnection.
/// The peer holds the inbound_packet_sender channel - dropping it closes the connection.
pub fn bench_1mb_transfer_validation(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("ledbat/warm_connection");

    // Test multiple sizes: 1KB, 4KB, 16KB
    for &(size, name) in &[(1024, "1kb"), (4096, "4kb"), (16384, "16kb")] {
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_function(name, |b| {
            // Create connection once, keep both peers AND connections alive
            let (live_a, live_b) = rt.block_on(async {
                let channels: Channels = Arc::new(DashMap::new());

                let (peer_a_pub, mut peer_a, peer_a_addr) =
                    create_mock_peer(PacketDropPolicy::ReceiveAll, channels.clone())
                        .await
                        .unwrap();

                let (peer_b_pub, mut peer_b, peer_b_addr) =
                    create_mock_peer(PacketDropPolicy::ReceiveAll, channels)
                        .await
                        .unwrap();

                let (conn_a_inner, conn_b_inner) = futures::join!(
                    peer_a.connect(peer_b_pub, peer_b_addr),
                    peer_b.connect(peer_a_pub, peer_a_addr),
                );
                let (conn_a, conn_b) = futures::join!(conn_a_inner, conn_b_inner);
                let (mut conn_a, mut conn_b) = (conn_a.unwrap(), conn_b.unwrap());

                // Warmup: 5 transfers to stabilize LEDBAT cwnd
                for _ in 0..5 {
                    let msg = vec![0xABu8; size];
                    conn_a.send(msg).await.unwrap();
                    let _: Vec<u8> = conn_b.recv().await.unwrap();
                }

                // Return LiveConnection structs that keep peers alive
                let live_a = LiveConnection {
                    conn: conn_a,
                    peer: peer_a,
                };
                let live_b = LiveConnection {
                    conn: conn_b,
                    peer: peer_b,
                };

                (live_a, live_b)
            });

            // Wrap in Arc<Mutex> for sharing across iterations
            let live_a = Arc::new(Mutex::new(live_a));
            let live_b = Arc::new(Mutex::new(live_b));

            let size_bytes = size; // Capture for async block
            b.to_async(&rt).iter(|| {
                let live_a = live_a.clone();
                let live_b = live_b.clone();
                async move {
                    let mut a = live_a.lock().await;
                    let mut b = live_b.lock().await;

                    let message = vec![0xABu8; size_bytes];
                    a.conn.send(message).await.unwrap();
                    let received: Vec<u8> = b.conn.recv().await.unwrap();

                    assert!(!received.is_empty());
                    std_black_box(received);
                }
            });
        });
    }

    group.finish();
}

// NOTE: Benchmarks with 32KB+ transfers timeout during criterion warmup phase.
// This appears to be an interaction between criterion's async benchmarking and
// larger message sizes. The transport layer itself handles large transfers fine
// (verified by unit tests). This needs further investigation.
//
// To properly test throughput approaching the 10 MB/s rate limit, we need either:
// 1. A custom benchmark harness that doesn't use criterion's iter()
// 2. Investigation into why criterion's warmup estimation hangs with large transfers
// 3. Real UDP socket benchmarks instead of mock transport
