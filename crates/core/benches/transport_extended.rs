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

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use std::hint::black_box as std_black_box;
use std::sync::Arc;
use std::time::Duration;

mod transport;

use dashmap::DashMap;
use freenet::transport::mock_transport::{create_mock_peer, Channels, PacketDropPolicy};
use transport::common::{create_peer_pair_with_delay, new_channels};

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

/// High-latency path throughput - detects ssthresh death spiral
///
/// Tests sustained throughput on intercontinental/satellite paths.
/// Would have caught issue #2578 (ssthresh collapse on high-latency paths).
pub fn bench_high_latency_sustained(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("extended/high_latency");
    group.sample_size(10); // Fewer samples, each runs longer
    group.measurement_time(Duration::from_secs(20));

    // Test RTTs from regional to satellite
    for &rtt_ms in &[100, 200, 500] {
        let one_way_delay = Duration::from_millis(rtt_ms / 2);

        // Transfer sizes: 16KB, 64KB, 256KB
        for &transfer_kb in &[16, 64, 256] {
            let transfer_size = transfer_kb * 1024;
            group.throughput(Throughput::Bytes(transfer_size as u64));

            group.bench_with_input(
                BenchmarkId::new(format!("{}ms_rtt", rtt_ms), transfer_kb),
                &(one_way_delay, transfer_size),
                |b, &(delay, size)| {
                    b.to_async(&rt).iter_batched(
                        || vec![0xABu8; size],
                        |message| async move {
                            // Create peers with latency
                            let mut peers = create_peer_pair_with_delay(new_channels(), delay)
                                .await
                                .connect()
                                .await;

                            // Measured transfer
                            peers.conn_a.send(message).await.unwrap();
                            let received: Vec<u8> = peers.conn_b.recv().await.unwrap();
                            std_black_box(received);
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }

    group.finish();
}

/// Packet loss resilience - tests reliability layer
///
/// Measures throughput degradation under packet loss.
/// Tests LEDBAT behavior with timeouts and retransmissions.
pub fn bench_packet_loss_resilience(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("extended/packet_loss");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    // Test different loss rates
    for &loss_pct in &[1.0, 5.0] {
        let loss_factor = loss_pct / 100.0;

        // 64KB transfer (multiple packets, tests retransmission)
        let transfer_size = 64 * 1024;
        group.throughput(Throughput::Bytes(transfer_size as u64));

        group.bench_with_input(
            BenchmarkId::new("loss", format!("{}pct", loss_pct)),
            &loss_factor,
            |b, &factor| {
                b.to_async(&rt).iter_batched(
                    || vec![0xABu8; transfer_size],
                    |message| async move {
                        let channels = Arc::new(DashMap::new()) as Channels;

                        // Create peers with packet loss
                        let (peer_a_pub, mut peer_a, peer_a_addr) =
                            create_mock_peer(PacketDropPolicy::Factor(factor), channels.clone())
                                .await
                                .unwrap();
                        let (peer_b_pub, mut peer_b, peer_b_addr) =
                            create_mock_peer(PacketDropPolicy::ReceiveAll, channels)
                                .await
                                .unwrap();

                        // Connect (two-level await pattern)
                        let (conn_a_inner, conn_b_inner) = futures::join!(
                            peer_a.connect(peer_b_pub, peer_b_addr),
                            peer_b.connect(peer_a_pub, peer_a_addr),
                        );
                        let (conn_a, conn_b) = futures::join!(conn_a_inner, conn_b_inner);
                        let (mut conn_a, mut conn_b) = (conn_a.unwrap(), conn_b.unwrap());

                        // Measured transfer with packet loss
                        conn_a.send(message).await.unwrap();
                        let received: Vec<u8> = conn_b.recv().await.unwrap();
                        std_black_box(received);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

/// Large file transfers - tests sustained throughput
///
/// Measures performance on contract/delegate distribution sizes.
pub fn bench_large_file_transfers(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("extended/large_files");
    group.sample_size(5); // Even fewer samples for large transfers
    group.measurement_time(Duration::from_secs(30));

    // Test large transfer sizes
    for &size_mb in &[10, 50] {
        let transfer_size = size_mb * 1024 * 1024;
        group.throughput(Throughput::Bytes(transfer_size as u64));

        group.bench_with_input(
            BenchmarkId::new("mb", size_mb),
            &transfer_size,
            |b, &size| {
                b.to_async(&rt).iter_batched(
                    || vec![0xABu8; size],
                    |message| async move {
                        let mut peers = create_peer_pair_with_delay(
                            new_channels(),
                            Duration::from_millis(5), // Slight latency for realism
                        )
                        .await
                        .connect()
                        .await;

                        // Measured large transfer
                        peers.conn_a.send(message).await.unwrap();
                        let received: Vec<u8> = peers.conn_b.recv().await.unwrap();
                        std_black_box(received);
                    },
                    BatchSize::LargeInput,
                );
            },
        );
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
        .sample_size(5)
        .measurement_time(Duration::from_secs(30))
        .noise_threshold(0.20)
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
        bench_warm_connection_throughput,
        bench_cwnd_evolution,
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
