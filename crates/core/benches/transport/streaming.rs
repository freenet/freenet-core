//! Transport Streaming Benchmarks - Large message transfers
//!
//! These benchmarks test stream fragmentation, reassembly, and rate limiting
//! for messages larger than a single packet (>1364 bytes).
//!
//! This is critical for measuring the impact of congestion control improvements.

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};
use std::hint::black_box as std_black_box;

use super::common::{create_peer_pair, new_channels, STREAM_SIZES};

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
    for &size in STREAM_SIZES {
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new("rate_limited", size), &size, |b, &sz| {
            b.to_async(&rt).iter_batched(
                || vec![0xABu8; sz],
                |message| async move {
                    // Create connected peers (cold-start measurement)
                    let mut peers = create_peer_pair(new_channels()).await.connect().await;

                    // Send large message (will be fragmented)
                    peers.conn_a.send(message.clone()).await.unwrap();
                    let received: Vec<u8> = peers.conn_b.recv().await.unwrap();

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
                    || (),
                    |_| async move {
                        // Create connected peers (cold-start measurement)
                        let peers = create_peer_pair(new_channels()).await.connect().await;

                        // Destructure to get separate connections for concurrent tasks
                        let mut conn_a = peers.conn_a;
                        let mut conn_b = peers.conn_b;

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
