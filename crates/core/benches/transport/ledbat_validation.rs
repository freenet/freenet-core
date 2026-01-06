//! LEDBAT Validation Benchmarks
//!
//! These benchmarks test LEDBAT congestion control behavior with separate
//! cold start and warm connection measurements.
//!
//! Uses RealTime with MockSocket for fast in-memory transport.
//! Expected runtime: ~1-2 minutes
//!
//! **Design principles:**
//! - Cold start: Create new connection per iteration (measures connection + transfer)
//! - Warm connection: Reuse connection across iterations (measures pure transfer)
//! - Keep OutboundConnectionHandler alive to prevent channel closure

use criterion::{Criterion, Throughput};
use dashmap::DashMap;
use freenet::transport::mock_transport::Channels;
use std::hint::black_box as std_black_box;
use std::sync::Arc;
use std::time::Duration;

use super::common::{create_peer_pair, SMALL_SIZES};

/// Cold start benchmark: measures connection establishment + transfer
///
/// Creates a connection and performs transfers, measuring total throughput.
/// Uses in-memory MockSocket for fast execution.
pub fn bench_large_transfer_validation(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("ledbat/cold_start");
    group.sample_size(10);

    // Test multiple sizes: 1KB, 4KB, 16KB
    for &(size, name) in SMALL_SIZES {
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_function(name, |b| {
            b.to_async(&rt).iter_custom(|iters| {
                async move {
                    // Create connection ONCE outside the loop to avoid port exhaustion
                    let channels: Channels = Arc::new(DashMap::new());
                    let mut peers = create_peer_pair(channels).await.connect().await;

                    let start = std::time::Instant::now();

                    for _ in 0..iters {
                        let message = vec![0xABu8; size];

                        // Transfer
                        if let Err(e) = peers.conn_a.send(message).await {
                            eprintln!("ledbat_cold send failed: {:?}", e);
                            continue;
                        }
                        match peers.conn_b.recv().await {
                            Ok(received) => {
                                std_black_box(received);
                            }
                            Err(e) => {
                                eprintln!("ledbat_cold recv failed: {:?}", e);
                                continue;
                            }
                        }
                    }

                    start.elapsed()
                }
            });
        });
    }

    group.finish();
}

/// Warm connection benchmark: measures pure transfer throughput
///
/// Connection is established once with warmup, then measures steady-state throughput.
/// Uses in-memory MockSocket for fast execution.
pub fn bench_1mb_transfer_validation(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("ledbat/warm_connection");
    group.sample_size(10);

    // Test multiple sizes: 1KB, 4KB, 16KB
    for &(size, name) in SMALL_SIZES {
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_function(name, |b| {
            b.to_async(&rt).iter_custom(|iters| {
                async move {
                    // Create connection once for this measurement batch
                    let channels: Channels = Arc::new(DashMap::new());
                    let mut peers = create_peer_pair(channels).await.connect().await;

                    // Warmup: 5 transfers to stabilize LEDBAT cwnd
                    for i in 0..5 {
                        let warmup_msg = vec![0xABu8; size];
                        if let Err(e) = peers.conn_a.send(warmup_msg).await {
                            eprintln!("ledbat warmup send {} failed: {:?}", i, e);
                            break;
                        }
                        match peers.conn_b.recv().await {
                            Ok(_) => {}
                            Err(e) => {
                                eprintln!("ledbat warmup recv {} failed: {:?}", i, e);
                                break;
                            }
                        }
                    }

                    let start = std::time::Instant::now();

                    // Measured iterations
                    for _ in 0..iters {
                        let message = vec![0xABu8; size];

                        if let Err(e) = peers.conn_a.send(message).await {
                            eprintln!("ledbat_warm send failed: {:?}", e);
                            continue;
                        }
                        match peers.conn_b.recv().await {
                            Ok(received) => {
                                std_black_box(received);
                            }
                            Err(e) => {
                                eprintln!("ledbat_warm recv failed: {:?}", e);
                                continue;
                            }
                        }
                    }

                    start.elapsed()
                }
            });
        });
    }

    group.finish();
}
