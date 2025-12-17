//! Transport Layer Benchmarks (Blackbox)
//!
//! These benchmarks test the actual Freenet transport code (PeerConnection,
//! connection_handler, fast_channel, encryption) with mock sockets instead
//! of real UDP. This tests the real code path without kernel syscall overhead.
//!
//! This is the primary CI benchmark group for detecting transport layer regressions.
//!
//! What's measured:
//! - Message throughput: Full pipeline (serialize → encrypt → channel → decrypt)
//! - fast_channel: Crossbeam-based channel vs tokio::sync::mpsc
//! - Connection establishment: Full handshake timing

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};
use dashmap::DashMap;
use freenet::transport::mock_transport::{create_mock_peer, Channels, PacketDropPolicy};
use std::hint::black_box as std_black_box;
use std::sync::Arc;

/// Benchmark connection establishment between two mock peers.
///
/// This measures the full handshake: key exchange, encryption setup, etc.
pub fn bench_connection_establishment(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("transport/connection");

    group.bench_function("establish", |b| {
        b.to_async(&rt).iter_batched(
            || {
                // Setup: create channel map for this iteration
                Arc::new(DashMap::new()) as Channels
            },
            |channels| async move {
                // Create two peers
                let (peer_a_pub, mut peer_a, peer_a_addr) =
                    create_mock_peer(PacketDropPolicy::ReceiveAll, channels.clone())
                        .await
                        .unwrap();
                let (peer_b_pub, mut peer_b, peer_b_addr) =
                    create_mock_peer(PacketDropPolicy::ReceiveAll, channels)
                        .await
                        .unwrap();

                // Establish connection from both sides (NAT traversal style)
                // connect() is async fn returning Pin<Box<dyn Future<...>>>, so two levels of await
                let (conn_a_inner, conn_b_inner) = futures::join!(
                    peer_a.connect(peer_b_pub, peer_b_addr),
                    peer_b.connect(peer_a_pub, peer_a_addr),
                );
                let (conn_a, conn_b) = futures::join!(conn_a_inner, conn_b_inner);

                // Connection established - drop the connections
                // (the benchmark measures establishment time, not ongoing usage)
                drop(conn_a.unwrap());
                drop(conn_b.unwrap());
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

/// Benchmark message throughput between two connected mock peers.
///
/// This measures the full pipeline: serialize → encrypt → channel → decrypt → deserialize
/// Note: Each iteration includes connection setup for proper benchmark isolation.
pub fn bench_message_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("transport/throughput");

    // Test different message sizes
    for &size in &[64, 256, 1024, 1364] {
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new("bytes", size), &size, |b, &sz| {
            b.to_async(&rt).iter_batched(
                || {
                    // Setup: Create fresh connected peers for each batch
                    let message = vec![0xABu8; sz];
                    (Arc::new(DashMap::new()) as Channels, message)
                },
                |(channels, message)| async move {
                    // Create peers
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

                    // Send and receive message (this is what we're measuring)
                    conn_a.send(message).await.unwrap();
                    let received: Vec<u8> = conn_b.recv().await.unwrap();
                    std_black_box(received);
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}
