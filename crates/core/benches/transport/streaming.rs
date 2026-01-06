//! Transport Streaming Benchmarks with VirtualTime - Large message transfers
//!
//! These benchmarks test stream fragmentation, reassembly, and rate limiting
//! for messages larger than a single packet (>1364 bytes).
//!
//! Uses VirtualTime for instant execution of network operations.
//! Expected runtime: ~30 seconds (vs ~5-10 minutes with real time)

use criterion::{BenchmarkId, Criterion, Throughput};
use dashmap::DashMap;
use freenet::simulation::{TimeSource, VirtualTime};
use freenet::transport::mock_transport::{Channels, PacketDelayPolicy, PacketDropPolicy};
use std::hint::black_box as std_black_box;
use std::sync::Arc;
use std::time::Duration;

use super::common::{create_peer_pair_with_virtual_time, spawn_auto_advance_task, STREAM_SIZES};

/// Benchmark large message streaming with VirtualTime (multi-packet transfers)
///
/// This measures the stream fragmentation and reassembly pipeline with
/// instant virtual time execution.
pub fn bench_stream_throughput(c: &mut Criterion) {
    // Use single-threaded runtime for deterministic scheduling with VirtualTime
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let time_source = VirtualTime::new();

    let mut group = c.benchmark_group("transport/streaming/throughput");
    group.sample_size(10);

    // Test STREAM_SIZES: 4KB, 16KB, 64KB
    for &size in STREAM_SIZES {
        group.throughput(Throughput::Bytes(size as u64));

        let ts = time_source.clone();
        group.bench_with_input(BenchmarkId::new("rate_limited", size), &size, |b, &sz| {
            b.to_async(&rt).iter_custom(|iters| {
                let ts = ts.clone();
                async move {
                    // Spawn auto-advance task to prevent deadlocks
                    let _auto_advance = spawn_auto_advance_task(ts.clone());

                    let mut total_virtual_time = Duration::ZERO;

                    for _ in 0..iters {
                        let message = vec![0xABu8; sz];
                        let channels: Channels = Arc::new(DashMap::new());

                        // Create connected peers with VirtualTime
                        let mut peers = create_peer_pair_with_virtual_time(
                            channels,
                            Duration::ZERO, // No delay for baseline measurement
                            ts.clone(),
                        )
                        .await
                        .connect()
                        .await;

                        let start_virtual = ts.now_nanos();

                        // Send large message (will be fragmented)
                        if let Err(e) = peers.conn_a.send(message).await {
                            eprintln!("streaming send failed: {:?}", e);
                            continue;
                        }

                        let received: Vec<u8> = match peers.conn_b.recv().await {
                            Ok(r) => r,
                            Err(e) => {
                                eprintln!("streaming recv failed: {:?}", e);
                                continue;
                            }
                        };

                        let end_virtual = ts.now_nanos();
                        total_virtual_time +=
                            Duration::from_nanos(end_virtual.saturating_sub(start_virtual));

                        std_black_box(received);
                    }

                    total_virtual_time
                }
            });
        });
    }

    group.finish();
}

/// Benchmark multiple concurrent streams with VirtualTime
///
/// This measures fairness and aggregate throughput when multiple streams
/// compete for bandwidth.
pub fn bench_concurrent_streams(c: &mut Criterion) {
    // Use single-threaded runtime for deterministic scheduling with VirtualTime
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let time_source = VirtualTime::new();

    let mut group = c.benchmark_group("transport/streaming/concurrent");
    group.sample_size(10);

    // Test 2, 5, 10 concurrent streams
    for num_streams in [2, 5, 10] {
        let ts = time_source.clone();

        group.bench_with_input(
            BenchmarkId::new("streams", num_streams),
            &num_streams,
            |b, &n| {
                b.to_async(&rt).iter_custom(|iters| {
                    let ts = ts.clone();
                    async move {
                        // Spawn auto-advance task to prevent deadlocks
                        let _auto_advance = spawn_auto_advance_task(ts.clone());

                        let mut total_virtual_time = Duration::ZERO;

                        for _ in 0..iters {
                            let channels: Channels = Arc::new(DashMap::new());

                            // Create connected peers with VirtualTime
                            let peers = create_peer_pair_with_virtual_time(
                                channels,
                                Duration::ZERO,
                                ts.clone(),
                            )
                            .await
                            .connect()
                            .await;

                            let mut conn_a = peers.conn_a;
                            let mut conn_b = peers.conn_b;

                            let message = vec![0xABu8; 16384]; // 16KB each
                            let start_virtual = ts.now_nanos();

                            // Send all messages sequentially (single-threaded runtime)
                            for _ in 0..n {
                                if let Err(e) = conn_a.send(message.clone()).await {
                                    eprintln!("concurrent send failed: {:?}", e);
                                    break;
                                }
                            }

                            // Receive all messages
                            let mut results = Vec::new();
                            for i in 0..n {
                                match conn_b.recv().await {
                                    Ok(received) => results.push((i, received.len())),
                                    Err(e) => {
                                        eprintln!("concurrent recv failed: {:?}", e);
                                        break;
                                    }
                                }
                            }

                            let end_virtual = ts.now_nanos();
                            total_virtual_time +=
                                Duration::from_nanos(end_virtual.saturating_sub(start_virtual));

                            std_black_box(results);
                        }

                        total_virtual_time
                    }
                });
            },
        );
    }

    group.finish();
}
