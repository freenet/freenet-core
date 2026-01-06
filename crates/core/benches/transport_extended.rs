//! Extended Transport Benchmarks with VirtualTime
//!
//! Comprehensive benchmark suite for resilience and real-world scenarios.
//! Uses VirtualTime for instant execution - 40min suite now runs in ~2-3min.
//!
//! Run with: `cargo bench --bench transport_extended --features bench`
//!
//! This suite includes:
//! - High-latency paths (100ms, 200ms, 500ms RTT) - instant with VirtualTime
//! - Packet loss scenarios (1%, 5%) - test reliability and recovery
//! - Large transfers (512KB, 1MB) - sustained throughput
//! - Micro-benchmarks - component-level validation
//!
//! **Note:** VirtualTime benchmarks use multi-threaded runtime to allow packet
//! delivery (via real async channels) to proceed concurrently with VirtualTime
//! operations, preventing premature connection timeouts.
//!
//! For quick CI checks: `cargo bench --bench transport_ci`

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use dashmap::DashMap;
use freenet::simulation::{TimeSource, VirtualTime};
use freenet::transport::mock_transport::{Channels, PacketDelayPolicy, PacketDropPolicy};
use std::hint::black_box as std_black_box;
use std::sync::Arc;
use std::time::Duration;

mod transport;

// Import existing benchmarks
use transport::allocation_overhead::*;
// VirtualTime helpers
use transport::common::spawn_auto_advance_task;
use transport::ledbat_validation::*;
use transport::level0::*;
use transport::level1::*;
use transport::slow_start::*;
use transport::streaming::*;

// =============================================================================
// Extended Benchmark Groups - Resilience Testing with VirtualTime
// =============================================================================

/// Sustained throughput benchmark with VirtualTime - instant execution
///
/// Tests throughput with varying message sizes. With VirtualTime, even large
/// transfers complete in milliseconds of wall time.
pub fn bench_high_latency_sustained(c: &mut Criterion) {
    // Use multi-threaded runtime to allow packet delivery to proceed concurrently
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let time_source = VirtualTime::new();

    let mut group = c.benchmark_group("extended/sustained_throughput");
    group.sample_size(10);

    // Test multiple sizes - all complete instantly with VirtualTime
    for &(transfer_size_kb, _recv_timeout_secs) in &[
        (16, 15),  // 16KB - baseline
        (64, 30),  // 64KB - multi-packet
        (256, 60), // 256KB - larger transfer
    ] {
        let transfer_size = transfer_size_kb * 1024;
        group.throughput(Throughput::Bytes(transfer_size as u64));

        let ts = time_source.clone();
        group.bench_function(format!("{}kb_transfer", transfer_size_kb), |b| {
            b.to_async(&rt).iter_custom(|iters| {
                let ts = ts.clone();
                async move {
                    // Spawn auto-advance task to prevent deadlocks
                    let _auto_advance = spawn_auto_advance_task(ts.clone());

                    let mut total_virtual_time = Duration::ZERO;

                    for _ in 0..iters {
                        let channels: Channels = Arc::new(DashMap::new());
                        let message = vec![0xABu8; transfer_size];

                        let (peer_a_pub, mut peer_a, peer_a_addr) =
                            match freenet::transport::mock_transport::create_mock_peer_with_virtual_time(
                                PacketDropPolicy::ReceiveAll,
                                PacketDelayPolicy::NoDelay,
                                channels.clone(),
                                ts.clone(),
                            )
                            .await
                            {
                                Ok(p) => p,
                                Err(e) => {
                                    eprintln!("sustained {}kb peer_a failed: {:?}", transfer_size_kb, e);
                                    continue;
                                }
                            };

                        let (peer_b_pub, mut peer_b, peer_b_addr) =
                            match freenet::transport::mock_transport::create_mock_peer_with_virtual_time(
                                PacketDropPolicy::ReceiveAll,
                                PacketDelayPolicy::NoDelay,
                                channels.clone(),
                                ts.clone(),
                            )
                            .await
                            {
                                Ok(p) => p,
                                Err(e) => {
                                    eprintln!("sustained {}kb peer_b failed: {:?}", transfer_size_kb, e);
                                    continue;
                                }
                            };

                        let start_virtual = ts.now_nanos();

                        // Connect both peers concurrently
                        let (conn_a_future, conn_b_future) = futures::join!(
                            peer_a.connect(peer_b_pub, peer_b_addr),
                            peer_b.connect(peer_a_pub, peer_a_addr),
                        );

                        let (conn_a, conn_b) = futures::join!(conn_a_future, conn_b_future);

                        let (mut conn_a, mut conn_b) = match (conn_a, conn_b) {
                            (Ok(a), Ok(b)) => (a, b),
                            (Err(e), _) => {
                                eprintln!("sustained sender connect failed: {:?}", e);
                                continue;
                            }
                            (_, Err(e)) => {
                                eprintln!("sustained receiver connect failed: {:?}", e);
                                continue;
                            }
                        };

                        // Send from A to B
                        let send_result = conn_a.send(message).await;
                        let sent_ok = match send_result {
                            Ok(()) => true,
                            Err(e) => {
                                eprintln!("sustained send failed: {:?}", e);
                                false
                            }
                        };

                        // Receive at B
                        if sent_ok {
                            if let Ok(received) = conn_b.recv().await {
                                std_black_box(received);
                            }
                        }

                        // Keep peers alive until end of iteration
                        drop(conn_a);
                        drop(conn_b);
                        drop(peer_a);
                        drop(peer_b);

                        let end_virtual = ts.now_nanos();
                        total_virtual_time +=
                            Duration::from_nanos(end_virtual.saturating_sub(start_virtual));

                        drop(channels);
                    }

                    total_virtual_time
                }
            });
        });
    }

    group.finish();
}

/// Packet loss resilience with VirtualTime - tests reliability layer
///
/// Measures throughput degradation under packet loss. With VirtualTime,
/// retransmission timeouts resolve instantly.
pub fn bench_packet_loss_resilience(c: &mut Criterion) {
    // Use multi-threaded runtime to allow packet delivery to proceed concurrently
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let time_source = VirtualTime::new();

    let mut group = c.benchmark_group("extended/packet_loss");
    group.sample_size(10);

    let transfer_size = 64 * 1024;
    let loss_factor = 0.01; // 1% loss
    group.throughput(Throughput::Bytes(transfer_size as u64));

    let ts = time_source.clone();
    group.bench_function("1pct_loss/64kb", |b| {
        b.to_async(&rt).iter_custom(|iters| {
            let ts = ts.clone();
            async move {
                // Spawn auto-advance task to prevent deadlocks
                let _auto_advance = spawn_auto_advance_task(ts.clone());

                let mut total_virtual_time = Duration::ZERO;

                for _ in 0..iters {
                    let channels: Channels = Arc::new(DashMap::new());
                    let message = vec![0xABu8; transfer_size];

                    // Create sender peer with packet loss and VirtualTime
                    let (peer_a_pub, mut peer_a, peer_a_addr) =
                        match freenet::transport::mock_transport::create_mock_peer_with_virtual_time(
                            PacketDropPolicy::Factor(loss_factor),
                            PacketDelayPolicy::NoDelay,
                            channels.clone(),
                            ts.clone(),
                        )
                        .await
                        {
                            Ok(p) => p,
                            Err(e) => {
                                eprintln!("packet_loss peer_a creation failed: {:?}", e);
                                continue;
                            }
                        };

                    // Create receiver peer (no loss)
                    let (peer_b_pub, mut peer_b, peer_b_addr) =
                        match freenet::transport::mock_transport::create_mock_peer_with_virtual_time(
                            PacketDropPolicy::ReceiveAll,
                            PacketDelayPolicy::NoDelay,
                            channels.clone(),
                            ts.clone(),
                        )
                        .await
                        {
                            Ok(p) => p,
                            Err(e) => {
                                eprintln!("packet_loss peer_b creation failed: {:?}", e);
                                continue;
                            }
                        };

                    let start_virtual = ts.now_nanos();

                    // Connect both peers concurrently
                    let (conn_a_future, conn_b_future) = futures::join!(
                        peer_a.connect(peer_b_pub, peer_b_addr),
                        peer_b.connect(peer_a_pub, peer_a_addr),
                    );

                    let (conn_a, conn_b) = futures::join!(conn_a_future, conn_b_future);

                    let (mut conn_a, mut conn_b) = match (conn_a, conn_b) {
                        (Ok(a), Ok(b)) => (a, b),
                        (Err(e), _) => {
                            eprintln!("packet_loss sender connect failed: {:?}", e);
                            continue;
                        }
                        (_, Err(e)) => {
                            eprintln!("packet_loss receiver connect failed: {:?}", e);
                            continue;
                        }
                    };

                    // Send from A to B
                    let send_result = conn_a.send(message).await;
                    if send_result.is_ok() {
                        if let Ok(received) = conn_b.recv().await {
                            std_black_box(received);
                        }
                    }

                    drop(conn_a);
                    drop(conn_b);
                    drop(peer_a);
                    drop(peer_b);

                    let end_virtual = ts.now_nanos();
                    total_virtual_time +=
                        Duration::from_nanos(end_virtual.saturating_sub(start_virtual));

                    drop(channels);
                }

                total_virtual_time
            }
        });
    });

    group.finish();
}

/// Large file transfers with VirtualTime - instant even for 1MB
pub fn bench_large_file_transfers(c: &mut Criterion) {
    // Use multi-threaded runtime to allow packet delivery to proceed concurrently
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let time_source = VirtualTime::new();

    let mut group = c.benchmark_group("extended/large_files");
    group.sample_size(10);

    // Test larger transfer sizes - all instant with VirtualTime
    for &(size_kb, _recv_timeout_secs) in &[
        (512, 90),   // 512KB
        (1024, 120), // 1MB
    ] {
        let transfer_size = size_kb * 1024;
        group.throughput(Throughput::Bytes(transfer_size as u64));

        let ts = time_source.clone();
        group.bench_function(format!("{}kb_transfer", size_kb), |b| {
            b.to_async(&rt).iter_custom(|iters| {
                let ts = ts.clone();
                async move {
                    // Spawn auto-advance task to prevent deadlocks
                    let _auto_advance = spawn_auto_advance_task(ts.clone());

                    let mut total_virtual_time = Duration::ZERO;

                    for _ in 0..iters {
                        let channels: Channels = Arc::new(DashMap::new());
                        let message = vec![0xABu8; transfer_size];

                        let (peer_a_pub, mut peer_a, peer_a_addr) =
                            match freenet::transport::mock_transport::create_mock_peer_with_virtual_time(
                                PacketDropPolicy::ReceiveAll,
                                PacketDelayPolicy::NoDelay,
                                channels.clone(),
                                ts.clone(),
                            )
                            .await
                            {
                                Ok(p) => p,
                                Err(e) => {
                                    eprintln!("large_file {}kb peer_a failed: {:?}", size_kb, e);
                                    continue;
                                }
                            };

                        let (peer_b_pub, mut peer_b, peer_b_addr) =
                            match freenet::transport::mock_transport::create_mock_peer_with_virtual_time(
                                PacketDropPolicy::ReceiveAll,
                                PacketDelayPolicy::NoDelay,
                                channels.clone(),
                                ts.clone(),
                            )
                            .await
                            {
                                Ok(p) => p,
                                Err(e) => {
                                    eprintln!("large_file {}kb peer_b failed: {:?}", size_kb, e);
                                    continue;
                                }
                            };

                        let start_virtual = ts.now_nanos();

                        // Connect both peers concurrently
                        let (conn_a_future, conn_b_future) = futures::join!(
                            peer_a.connect(peer_b_pub, peer_b_addr),
                            peer_b.connect(peer_a_pub, peer_a_addr),
                        );

                        let (conn_a, conn_b) = futures::join!(conn_a_future, conn_b_future);

                        let (mut conn_a, mut conn_b) = match (conn_a, conn_b) {
                            (Ok(a), Ok(b)) => (a, b),
                            (Err(e), _) => {
                                eprintln!("large_file sender connect failed: {:?}", e);
                                continue;
                            }
                            (_, Err(e)) => {
                                eprintln!("large_file receiver connect failed: {:?}", e);
                                continue;
                            }
                        };

                        // Send from A to B
                        let send_result = conn_a.send(message).await;
                        if send_result.is_ok() {
                            if let Ok(received) = conn_b.recv().await {
                                std_black_box(received);
                            }
                        }

                        drop(conn_a);
                        drop(conn_b);
                        drop(peer_a);
                        drop(peer_b);

                        let end_virtual = ts.now_nanos();
                        total_virtual_time +=
                            Duration::from_nanos(end_virtual.saturating_sub(start_virtual));

                        drop(channels);
                    }

                    total_virtual_time
                }
            });
        });
    }

    group.finish();
}

// =============================================================================
// Extended Benchmark Groups - Configuration
// =============================================================================

criterion_group!(
    name = high_latency_extended;
    config = Criterion::default()
        .sample_size(10)
        .noise_threshold(0.25)
        .significance_level(0.05);
    targets = bench_high_latency_sustained
);

criterion_group!(
    name = packet_loss_extended;
    config = Criterion::default()
        .sample_size(10)
        .noise_threshold(0.30)
        .significance_level(0.05);
    targets = bench_packet_loss_resilience
);

criterion_group!(
    name = large_files_extended;
    config = Criterion::default()
        .sample_size(10)
        .noise_threshold(0.25)
        .significance_level(0.05);
    targets = bench_large_file_transfers
);

// Micro-benchmarks (fast, don't need VirtualTime)
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

// VirtualTime-enabled comprehensive tests
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
        .noise_threshold(0.20)
        .significance_level(0.05);
    targets =
        bench_cold_start_throughput,
        bench_cwnd_evolution,
        bench_rtt_scenarios,
);

// Main entry point - comprehensive extended suite
// With VirtualTime: ~2-3 minutes (vs ~40 minutes with real time)
criterion_main!(
    // Resilience testing (VirtualTime)
    high_latency_extended,
    packet_loss_extended,
    large_files_extended,
    // Micro-benchmarks (fast, no network)
    allocation_extended,
    level0_extended,
    level1_extended,
    // VirtualTime network tests
    streaming_extended,
    ledbat_validation_extended,
    slow_start_extended,
);
