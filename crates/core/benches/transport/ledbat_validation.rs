//! LEDBAT Validation Benchmarks
//!
//! These benchmarks test congestion control behavior under latency.
//!
//! **Design principles:**
//! - Use microsecond delays (100µs-1ms) not millisecond delays (10-100ms)
//!   This tests the same LEDBAT dynamics but 10-100x faster.
//! - Use smaller transfers (32KB-64KB) for faster iterations
//! - Minimal warmup to test cold-start behavior
//!
//! Total runtime: ~5-8 minutes (vs hours with ms delays)

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};
use dashmap::DashMap;
use freenet::transport::mock_transport::{
    create_mock_peer_with_delay, Channels, PacketDelayPolicy, PacketDropPolicy,
};
use std::hint::black_box as std_black_box;
use std::sync::Arc;
use std::time::Duration;

/// Helper to run warmup transfers before measurement
async fn warmup_connection(
    conn_a: &mut freenet::transport::PeerConnection,
    conn_b: &mut freenet::transport::PeerConnection,
    warmup_size: usize,
) {
    // Single warmup transfer to get past slow-start
    let msg = vec![0xABu8; warmup_size];
    conn_a.send(msg).await.unwrap();
    let _: Vec<u8> = conn_b.recv().await.unwrap();
}

/// Validate LEDBAT throughput under various latency conditions
///
/// Tests 64KB transfers with microsecond delays to verify that
/// higher latency = lower throughput (not the reverse anomaly).
pub fn bench_large_transfer_validation(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("ledbat/latency");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));

    // Test with microsecond delays: 100µs, 500µs, 1ms
    // This tests the same LEDBAT dynamics but much faster
    for delay_us in [100, 500, 1000] {
        let delay = Duration::from_micros(delay_us);
        group.throughput(Throughput::Bytes(65536)); // 64KB

        group.bench_with_input(
            BenchmarkId::new("64kb", format!("{}us", delay_us)),
            &delay,
            |b, &delay| {
                b.to_async(&rt).iter_batched(
                    || {
                        let message = vec![0xABu8; 65536]; // 64KB
                        (Arc::new(DashMap::new()) as Channels, message, delay)
                    },
                    |(channels, message, delay)| async move {
                        let delay_policy = PacketDelayPolicy::Fixed(delay);

                        let (peer_a_pub, mut peer_a, peer_a_addr) = create_mock_peer_with_delay(
                            PacketDropPolicy::ReceiveAll,
                            delay_policy.clone(),
                            channels.clone(),
                        )
                        .await
                        .unwrap();
                        let (peer_b_pub, mut peer_b, peer_b_addr) = create_mock_peer_with_delay(
                            PacketDropPolicy::ReceiveAll,
                            delay_policy,
                            channels,
                        )
                        .await
                        .unwrap();

                        let (conn_a_inner, conn_b_inner) = futures::join!(
                            peer_a.connect(peer_b_pub, peer_b_addr),
                            peer_b.connect(peer_a_pub, peer_a_addr),
                        );
                        let (conn_a, conn_b) = futures::join!(conn_a_inner, conn_b_inner);
                        let (mut conn_a, mut conn_b) = (conn_a.unwrap(), conn_b.unwrap());

                        // Single 16KB warmup
                        warmup_connection(&mut conn_a, &mut conn_b, 16384).await;

                        // Measured transfer
                        conn_a.send(message).await.unwrap();
                        let received: Vec<u8> = conn_b.recv().await.unwrap();

                        assert!(!received.is_empty());
                        std_black_box(received);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

/// Validate 128KB transfers - larger but still fast
pub fn bench_1mb_transfer_validation(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("ledbat/large");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(8));

    // Only test 500µs and 1ms delays for large transfers
    for delay_us in [500, 1000] {
        let delay = Duration::from_micros(delay_us);
        group.throughput(Throughput::Bytes(131072)); // 128KB

        group.bench_with_input(
            BenchmarkId::new("128kb", format!("{}us", delay_us)),
            &delay,
            |b, &delay| {
                b.to_async(&rt).iter_batched(
                    || {
                        let message = vec![0xABu8; 131072]; // 128KB
                        (Arc::new(DashMap::new()) as Channels, message, delay)
                    },
                    |(channels, message, delay)| async move {
                        let delay_policy = PacketDelayPolicy::Fixed(delay);

                        let (peer_a_pub, mut peer_a, peer_a_addr) = create_mock_peer_with_delay(
                            PacketDropPolicy::ReceiveAll,
                            delay_policy.clone(),
                            channels.clone(),
                        )
                        .await
                        .unwrap();
                        let (peer_b_pub, mut peer_b, peer_b_addr) = create_mock_peer_with_delay(
                            PacketDropPolicy::ReceiveAll,
                            delay_policy,
                            channels,
                        )
                        .await
                        .unwrap();

                        let (conn_a_inner, conn_b_inner) = futures::join!(
                            peer_a.connect(peer_b_pub, peer_b_addr),
                            peer_b.connect(peer_a_pub, peer_a_addr),
                        );
                        let (conn_a, conn_b) = futures::join!(conn_a_inner, conn_b_inner);
                        let (mut conn_a, mut conn_b) = (conn_a.unwrap(), conn_b.unwrap());

                        // Single 32KB warmup
                        warmup_connection(&mut conn_a, &mut conn_b, 32768).await;

                        // Measured transfer
                        conn_a.send(message).await.unwrap();
                        let received: Vec<u8> = conn_b.recv().await.unwrap();

                        assert!(!received.is_empty());
                        std_black_box(received);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

/// Test congestion behavior with packet loss
pub fn bench_congestion_256kb(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("ledbat/congestion");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    // Test 1% and 5% loss with 500µs delay
    for loss_pct in [1.0, 5.0] {
        let delay_us = 500u64;
        group.throughput(Throughput::Bytes(65536));

        group.bench_with_input(
            BenchmarkId::new("64kb", format!("{}%loss", loss_pct as u32)),
            &loss_pct,
            |b, &loss_pct| {
                b.to_async(&rt).iter_batched(
                    || {
                        let message = vec![0xABu8; 65536];
                        (Arc::new(DashMap::new()) as Channels, message)
                    },
                    |(channels, message)| async move {
                        let drop_policy = PacketDropPolicy::Factor(loss_pct / 100.0);
                        let delay_policy =
                            PacketDelayPolicy::Fixed(Duration::from_micros(delay_us));

                        let (peer_a_pub, mut peer_a, peer_a_addr) = create_mock_peer_with_delay(
                            drop_policy.clone(),
                            delay_policy.clone(),
                            channels.clone(),
                        )
                        .await
                        .unwrap();
                        let (peer_b_pub, mut peer_b, peer_b_addr) =
                            create_mock_peer_with_delay(drop_policy, delay_policy, channels)
                                .await
                                .unwrap();

                        let (conn_a_inner, conn_b_inner) = futures::join!(
                            peer_a.connect(peer_b_pub, peer_b_addr),
                            peer_b.connect(peer_a_pub, peer_a_addr),
                        );
                        let (conn_a, conn_b) = futures::join!(conn_a_inner, conn_b_inner);
                        let (mut conn_a, mut conn_b) = (conn_a.unwrap(), conn_b.unwrap());

                        // Warmup
                        warmup_connection(&mut conn_a, &mut conn_b, 16384).await;

                        // Measured transfer with congestion
                        conn_a.send(message).await.unwrap();
                        let received: Vec<u8> = conn_b.recv().await.unwrap();

                        assert!(!received.is_empty());
                        std_black_box(received);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}
