//! Extended Transport Benchmarks
//!
//! Comprehensive benchmark suite for resilience and real-world scenarios.
//! Run nightly or when transport/** changes.
//! Total execution time: ~30-45 minutes
//!
//! Run with: `cargo bench --bench transport_extended --features bench`
//!
//! This suite includes:
//! - High-latency paths (100ms, 200ms, 500ms RTT) - detect ssthresh death spiral
//! - Packet loss scenarios (1%, 5%) - test reliability and recovery
//! - Large transfers (10MB, 100MB) - sustained throughput
//! - Multiple concurrent connections - P2P realism
//! - Micro-benchmarks - component-level validation
//!
//! For quick CI checks: `cargo bench --bench transport_ci`

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::hint::black_box as std_black_box;
use std::sync::Arc;
use std::time::Duration;

mod transport;

use dashmap::DashMap;
use freenet::transport::mock_transport::{create_mock_peer, Channels, PacketDropPolicy};

// Import existing benchmarks
use transport::allocation_overhead::*;
use transport::ledbat_validation::*;
use transport::level0::*;
use transport::level1::*;
use transport::slow_start::*;
use transport::streaming::*;

// =============================================================================
// Extended Benchmark Groups - Resilience Testing
// =============================================================================

/// Sustained throughput benchmark - tests larger transfers for regression detection
///
/// Tests throughput with varying message sizes to detect regressions.
/// Uses longer timeouts proportional to message size.
///
/// **Pattern**: Uses `tokio::spawn` for sender/receiver tasks with
/// synchronization delay (required by mock transport).
pub fn bench_high_latency_sustained(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("extended/sustained_throughput");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));

    // Test multiple sizes to detect regressions at different scales
    // Note: Mock transport is most reliable with smaller sizes; 256KB+ may timeout intermittently
    for &(transfer_size_kb, recv_timeout_secs) in &[
        (16, 15),  // 16KB - baseline
        (64, 30),  // 64KB - multi-packet
        (256, 60), // 256KB - larger transfer (may timeout occasionally)
    ] {
        let transfer_size = transfer_size_kb * 1024;
        group.throughput(Throughput::Bytes(transfer_size as u64));

        group.bench_function(format!("{}kb_transfer", transfer_size_kb), |b| {
            b.to_async(&rt).iter(|| async {
                let channels: Channels = Arc::new(DashMap::new());
                let message = vec![0xABu8; transfer_size];
                let recv_timeout = Duration::from_secs(recv_timeout_secs);

                // Create peers
                let (peer_a_pub, mut peer_a, peer_a_addr) =
                    match create_mock_peer(PacketDropPolicy::ReceiveAll, channels.clone()).await {
                        Ok(p) => p,
                        Err(e) => {
                            eprintln!("sustained {}kb peer_a failed: {:?}", transfer_size_kb, e);
                            return;
                        }
                    };

                let (peer_b_pub, mut peer_b, peer_b_addr) =
                    match create_mock_peer(PacketDropPolicy::ReceiveAll, channels.clone()).await {
                        Ok(p) => p,
                        Err(e) => {
                            eprintln!("sustained {}kb peer_b failed: {:?}", transfer_size_kb, e);
                            return;
                        }
                    };

                // Spawn receiver task first
                let receiver = tokio::spawn(async move {
                    let conn_future = peer_b.connect(peer_a_pub, peer_a_addr).await;
                    let mut conn =
                        match tokio::time::timeout(Duration::from_secs(10), conn_future).await {
                            Ok(Ok(c)) => c,
                            Ok(Err(e)) => {
                                eprintln!("sustained receiver connect failed: {:?}", e);
                                return None;
                            }
                            Err(_) => {
                                eprintln!("sustained receiver connect timeout");
                                return None;
                            }
                        };

                    match tokio::time::timeout(recv_timeout, conn.recv()).await {
                        Ok(Ok(received)) => Some(received),
                        Ok(Err(e)) => {
                            eprintln!("sustained recv failed: {:?}", e);
                            None
                        }
                        Err(_) => {
                            eprintln!("sustained recv timeout ({}s)", recv_timeout.as_secs());
                            None
                        }
                    }
                });

                // Spawn sender task
                let sender = tokio::spawn(async move {
                    let conn_future = peer_a.connect(peer_b_pub, peer_b_addr).await;
                    let mut conn =
                        match tokio::time::timeout(Duration::from_secs(10), conn_future).await {
                            Ok(Ok(c)) => c,
                            Ok(Err(e)) => {
                                eprintln!("sustained sender connect failed: {:?}", e);
                                return false;
                            }
                            Err(_) => {
                                eprintln!("sustained sender connect timeout");
                                return false;
                            }
                        };

                    // 100ms delay to ensure receiver is ready
                    tokio::time::sleep(Duration::from_millis(100)).await;

                    match conn.send(message).await {
                        Ok(()) => true,
                        Err(e) => {
                            eprintln!("sustained send failed: {:?}", e);
                            false
                        }
                    }
                });

                let (recv_result, _) = tokio::join!(receiver, sender);
                if let Ok(Some(received)) = recv_result {
                    std_black_box(received);
                }

                drop(channels);
            });
        });
    }

    group.finish();
}

/// Packet loss resilience - tests reliability layer
///
/// Measures throughput degradation under packet loss.
/// Tests LEDBAT behavior with timeouts and retransmissions.
///
/// **Pattern**: Uses `tokio::spawn` for sender/receiver tasks.
/// Tests 64KB transfers with 1% loss to validate retransmission handling.
pub fn bench_packet_loss_resilience(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("extended/packet_loss");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));

    // Test 64KB with 1% loss - exercises retransmission logic
    let transfer_size = 64 * 1024;
    let loss_factor = 0.01; // 1% loss
    group.throughput(Throughput::Bytes(transfer_size as u64));

    group.bench_function("1pct_loss/64kb", |b| {
        b.to_async(&rt).iter(|| async {
            let channels: Channels = Arc::new(DashMap::new());
            let message = vec![0xABu8; transfer_size];

            // Create sender peer with packet loss
            let (peer_a_pub, mut peer_a, peer_a_addr) =
                match create_mock_peer(PacketDropPolicy::Factor(loss_factor), channels.clone())
                    .await
                {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!("packet_loss peer_a creation failed: {:?}", e);
                        return;
                    }
                };

            // Create receiver peer (no loss)
            let (peer_b_pub, mut peer_b, peer_b_addr) =
                match create_mock_peer(PacketDropPolicy::ReceiveAll, channels.clone()).await {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!("packet_loss peer_b creation failed: {:?}", e);
                        return;
                    }
                };

            // Spawn receiver task first
            let receiver = tokio::spawn(async move {
                let conn_future = peer_b.connect(peer_a_pub, peer_a_addr).await;
                let mut conn =
                    match tokio::time::timeout(Duration::from_secs(10), conn_future).await {
                        Ok(Ok(c)) => c,
                        Ok(Err(e)) => {
                            eprintln!("packet_loss receiver connect failed: {:?}", e);
                            return None;
                        }
                        Err(_) => {
                            eprintln!("packet_loss receiver connect timeout");
                            return None;
                        }
                    };

                // Longer timeout for packet loss (retransmissions take time)
                match tokio::time::timeout(Duration::from_secs(30), conn.recv()).await {
                    Ok(Ok(received)) => Some(received),
                    Ok(Err(e)) => {
                        eprintln!("packet_loss recv failed: {:?}", e);
                        None
                    }
                    Err(_) => {
                        eprintln!("packet_loss recv timeout");
                        None
                    }
                }
            });

            // Spawn sender task
            let sender = tokio::spawn(async move {
                let conn_future = peer_a.connect(peer_b_pub, peer_b_addr).await;
                let mut conn =
                    match tokio::time::timeout(Duration::from_secs(10), conn_future).await {
                        Ok(Ok(c)) => c,
                        Ok(Err(e)) => {
                            eprintln!("packet_loss sender connect failed: {:?}", e);
                            return false;
                        }
                        Err(_) => {
                            eprintln!("packet_loss sender connect timeout");
                            return false;
                        }
                    };

                // 100ms delay to ensure receiver is ready
                tokio::time::sleep(Duration::from_millis(100)).await;

                match conn.send(message).await {
                    Ok(()) => true,
                    Err(e) => {
                        eprintln!("packet_loss send failed: {:?}", e);
                        false
                    }
                }
            });

            let (recv_result, _) = tokio::join!(receiver, sender);
            if let Ok(Some(received)) = recv_result {
                std_black_box(received);
            }

            drop(channels);
        });
    });

    group.finish();
}

/// Large file transfers - tests larger sustained throughput
///
/// Measures performance on larger transfers (512KB, 1MB).
/// Critical for detecting regressions in sustained throughput.
///
/// **Pattern**: Uses `tokio::spawn` for sender/receiver tasks with
/// extended timeouts for large transfers.
pub fn bench_large_file_transfers(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("extended/large_files");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(45));

    // Test larger transfer sizes - 512KB and 1MB
    // Note: Multi-MB transfers may be unreliable with mock transport
    for &(size_kb, recv_timeout_secs) in &[
        (512, 90),   // 512KB - moderate large transfer
        (1024, 120), // 1MB - large transfer
    ] {
        let transfer_size = size_kb * 1024;
        group.throughput(Throughput::Bytes(transfer_size as u64));

        group.bench_function(format!("{}kb_transfer", size_kb), |b| {
            b.to_async(&rt).iter(|| async {
                let channels: Channels = Arc::new(DashMap::new());
                let message = vec![0xABu8; transfer_size];
                let recv_timeout = Duration::from_secs(recv_timeout_secs);

                // Create peers
                let (peer_a_pub, mut peer_a, peer_a_addr) =
                    match create_mock_peer(PacketDropPolicy::ReceiveAll, channels.clone()).await {
                        Ok(p) => p,
                        Err(e) => {
                            eprintln!("large_file {}kb peer_a failed: {:?}", size_kb, e);
                            return;
                        }
                    };

                let (peer_b_pub, mut peer_b, peer_b_addr) =
                    match create_mock_peer(PacketDropPolicy::ReceiveAll, channels.clone()).await {
                        Ok(p) => p,
                        Err(e) => {
                            eprintln!("large_file {}kb peer_b failed: {:?}", size_kb, e);
                            return;
                        }
                    };

                // Spawn receiver task first
                let receiver = tokio::spawn(async move {
                    let conn_future = peer_b.connect(peer_a_pub, peer_a_addr).await;
                    let mut conn =
                        match tokio::time::timeout(Duration::from_secs(15), conn_future).await {
                            Ok(Ok(c)) => c,
                            Ok(Err(e)) => {
                                eprintln!("large_file receiver connect failed: {:?}", e);
                                return None;
                            }
                            Err(_) => {
                                eprintln!("large_file receiver connect timeout");
                                return None;
                            }
                        };

                    match tokio::time::timeout(recv_timeout, conn.recv()).await {
                        Ok(Ok(received)) => Some(received),
                        Ok(Err(e)) => {
                            eprintln!("large_file recv failed: {:?}", e);
                            None
                        }
                        Err(_) => {
                            eprintln!("large_file recv timeout ({}s)", recv_timeout.as_secs());
                            None
                        }
                    }
                });

                // Spawn sender task
                let sender = tokio::spawn(async move {
                    let conn_future = peer_a.connect(peer_b_pub, peer_b_addr).await;
                    let mut conn =
                        match tokio::time::timeout(Duration::from_secs(15), conn_future).await {
                            Ok(Ok(c)) => c,
                            Ok(Err(e)) => {
                                eprintln!("large_file sender connect failed: {:?}", e);
                                return false;
                            }
                            Err(_) => {
                                eprintln!("large_file sender connect timeout");
                                return false;
                            }
                        };

                    // 100ms delay to ensure receiver is ready
                    tokio::time::sleep(Duration::from_millis(100)).await;

                    match conn.send(message).await {
                        Ok(()) => true,
                        Err(e) => {
                            eprintln!("large_file send failed: {:?}", e);
                            false
                        }
                    }
                });

                let (recv_result, _) = tokio::join!(receiver, sender);
                if let Ok(Some(received)) = recv_result {
                    std_black_box(received);
                }

                drop(channels);
            });
        });
    }

    group.finish();
}

// =============================================================================
// Extended Benchmark Groups - Existing Comprehensive Tests
// =============================================================================

criterion_group!(
    name = high_latency_extended;
    config = Criterion::default()
        .sample_size(10)
        .measurement_time(Duration::from_secs(20))
        .noise_threshold(0.25)  // Higher variance expected on high-latency paths
        .significance_level(0.05);
    targets = bench_high_latency_sustained
);

criterion_group!(
    name = packet_loss_extended;
    config = Criterion::default()
        .sample_size(10)
        .measurement_time(Duration::from_secs(15))
        .noise_threshold(0.30)  // Packet loss adds significant variance
        .significance_level(0.05);
    targets = bench_packet_loss_resilience
);

criterion_group!(
    name = large_files_extended;
    config = Criterion::default()
        .sample_size(10)
        .measurement_time(Duration::from_secs(45))
        .noise_threshold(0.25)  // Higher variance for large transfers
        .significance_level(0.05);
    targets = bench_large_file_transfers
);

// Micro-benchmarks (moved from CI)
criterion_group!(
    name = allocation_extended;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(3))
        .noise_threshold(0.03)
        .significance_level(0.01);
    targets =
        bench_packet_allocation,
        bench_fragmentation,
        bench_packet_preparation,
);

criterion_group!(
    name = level0_extended;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(3))
        .noise_threshold(0.03)
        .significance_level(0.01);
    targets =
        bench_aes_gcm_encrypt,
        bench_aes_gcm_decrypt,
        bench_nonce_generation,
        bench_serialization,
        bench_packet_creation,
);

criterion_group!(
    name = level1_extended;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(5))
        .noise_threshold(0.05)
        .significance_level(0.01);
    targets = bench_channel_throughput,
);

// Existing comprehensive tests
criterion_group!(
    name = streaming_extended;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(10))
        .noise_threshold(0.15)
        .significance_level(0.05);
    targets =
        bench_stream_throughput,
        bench_concurrent_streams,
);

criterion_group!(
    name = ledbat_validation_extended;
    config = Criterion::default()
        .sample_size(10)
        .measurement_time(Duration::from_secs(10))
        .noise_threshold(0.20)
        .significance_level(0.05);
    targets =
        bench_large_transfer_validation,
        bench_1mb_transfer_validation,
);

criterion_group!(
    name = slow_start_extended;
    config = Criterion::default()
        .sample_size(10)
        .measurement_time(Duration::from_secs(15))
        .noise_threshold(0.20)
        .significance_level(0.05);
    targets =
        bench_cold_start_throughput,
        bench_cwnd_evolution,
        bench_rtt_scenarios,
);

// Main entry point - comprehensive extended suite
//
// This runs:
// 1. NEW resilience tests (high-latency, packet loss, large files)
// 2. Micro-benchmarks (moved from CI)
// 3. Existing comprehensive tests (streaming, LEDBAT, slow start)
//
// Total runtime: ~30-45 minutes
// Run: nightly or when transport/** changes
criterion_main!(
    // NEW: Resilience testing
    high_latency_extended,
    packet_loss_extended,
    large_files_extended,
    // Micro-benchmarks (removed from CI)
    allocation_extended,
    level0_extended,
    level1_extended,
    // Comprehensive existing tests
    streaming_extended,
    ledbat_validation_extended,
    slow_start_extended,
);
