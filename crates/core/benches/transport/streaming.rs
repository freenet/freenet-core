//! Transport Streaming Benchmarks - Large message transfers
//!
//! These benchmarks test stream fragmentation, reassembly, and rate limiting
//! for messages larger than a single packet (>1364 bytes).
//!
//! This is critical for measuring the impact of congestion control improvements.

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};
use dashmap::DashMap;
use freenet::transport::mock_transport::{create_mock_peer, Channels, PacketDropPolicy};
use std::hint::black_box as std_black_box;
use std::sync::Arc;

/// Benchmark large message streaming (multi-packet transfers)
///
/// This measures the stream fragmentation and reassembly pipeline with
/// the current rate limiting in place (3 MB/s default).
pub fn bench_stream_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("transport/streaming/throughput");

    // Test STREAM_SIZES: 4KB, 16KB, 64KB
    for &size in &[4096, 16384, 65536] {
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new("rate_limited", size), &size, |b, &sz| {
            b.to_async(&rt).iter_batched(
                || {
                    let message = vec![0xABu8; sz];
                    (Arc::new(DashMap::new()) as Channels, message)
                },
                |(channels, message)| async move {
                    // Create connected peers
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

                    // Send large message (will be fragmented)
                    conn_a.send(message.clone()).await.unwrap();
                    let received: Vec<u8> = conn_b.recv().await.unwrap();

                    // Note: received length may differ slightly due to serialization overhead
                    // Just verify we got data back
                    assert!(!received.is_empty());
                    std_black_box(received);
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

/// Benchmark multiple concurrent streams to same peer
///
/// This measures fairness and aggregate throughput when multiple streams
/// compete for bandwidth.
pub fn bench_concurrent_streams(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("transport/streaming/concurrent");

    // Test 2, 5, 10 concurrent streams
    for num_streams in [2, 5, 10] {
        group.bench_with_input(
            BenchmarkId::new("streams", num_streams),
            &num_streams,
            |b, &n| {
                b.to_async(&rt).iter_batched(
                    || Arc::new(DashMap::new()) as Channels,
                    |channels| async move {
                        // Create connected peers
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

                        // Send n messages concurrently (connection handles concurrency internally)
                        let message = vec![0xABu8; 16384]; // 16KB each

                        // Spawn concurrent send and receive tasks
                        let send_task = tokio::spawn(async move {
                            for _ in 0..n {
                                conn_a.send(message.clone()).await.unwrap();
                            }
                        });

                        let recv_task = tokio::spawn(async move {
                            let mut results = Vec::new();
                            for i in 0..n {
                                let received: Vec<u8> = conn_b.recv().await.unwrap();
                                results.push((i, received.len()));
                            }
                            results
                        });

                        // Wait for both to complete
                        send_task.await.unwrap();
                        let results = recv_task.await.unwrap();

                        std_black_box(results);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}
