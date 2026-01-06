//! Transport Streaming Benchmarks - Large message transfers
//!
//! These benchmarks test stream fragmentation, reassembly, and rate limiting
//! for messages larger than a single packet (>1364 bytes).
//!
//! Uses RealTime with MockSocket for fast in-memory transport.
//! Expected runtime: ~1-2 minutes

use criterion::{BenchmarkId, Criterion, Throughput};
use dashmap::DashMap;
use freenet::transport::mock_transport::{Channels, PacketDropPolicy};
use std::hint::black_box as std_black_box;
use std::sync::Arc;
use std::time::Duration;

use super::common::{create_peer_pair, STREAM_SIZES};

/// Benchmark large message streaming (multi-packet transfers)
///
/// This measures the stream fragmentation and reassembly pipeline using
/// in-memory MockSocket transport for fast execution.
pub fn bench_stream_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("transport/streaming/throughput");
    group.sample_size(10);

    // Test STREAM_SIZES: 4KB, 16KB, 64KB
    for &size in STREAM_SIZES {
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new("stream", size), &size, |b, &sz| {
            b.to_async(&rt).iter_custom(|iters| {
                async move {
                    // Create connection ONCE outside the loop to avoid port exhaustion
                    let channels: Channels = Arc::new(DashMap::new());
                    let mut peers = create_peer_pair(channels).await.connect().await;

                    let start = std::time::Instant::now();

                    for _ in 0..iters {
                        let message = vec![0xABu8; sz];

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

                        std_black_box(received);
                    }

                    start.elapsed()
                }
            });
        });
    }

    group.finish();
}

/// Benchmark multiple sequential streams
///
/// This measures aggregate throughput when multiple messages are sent
/// and received in sequence.
pub fn bench_concurrent_streams(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("transport/streaming/concurrent");
    group.sample_size(10);

    // Test 2, 5, 10 sequential transfers per iteration
    for num_streams in [2, 5, 10] {
        group.bench_with_input(
            BenchmarkId::new("streams", num_streams),
            &num_streams,
            |b, &n| {
                b.to_async(&rt).iter_custom(|iters| {
                    async move {
                        // Create connection ONCE outside the loop to avoid port exhaustion
                        let channels: Channels = Arc::new(DashMap::new());
                        let mut peers = create_peer_pair(channels).await.connect().await;

                        let start = std::time::Instant::now();

                        for _ in 0..iters {
                            let message = vec![0xABu8; 16384]; // 16KB each

                            // Send all messages sequentially
                            for _ in 0..n {
                                if let Err(e) = peers.conn_a.send(message.clone()).await {
                                    eprintln!("concurrent send failed: {:?}", e);
                                    break;
                                }
                            }

                            // Receive all messages
                            let mut results = Vec::new();
                            for i in 0..n {
                                match peers.conn_b.recv().await {
                                    Ok(received) => results.push((i, received.len())),
                                    Err(e) => {
                                        eprintln!("concurrent recv failed: {:?}", e);
                                        break;
                                    }
                                }
                            }

                            std_black_box(results);
                        }

                        start.elapsed()
                    }
                });
            },
        );
    }

    group.finish();
}
