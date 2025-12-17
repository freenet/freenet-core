//! LEDBAT Validation - Fast benchmarks with larger transfers
//!
//! This validates Opus's hypothesis that the 100ms-faster-than-50ms anomaly
//! is due to statistical variance with small transfers.
//!
//! Uses larger transfers (256KB, 1MB) with warmup to eliminate burst effects.
//! Faster execution: fewer samples, shorter measurement times.

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
    warmup_count: usize,
    warmup_size: usize,
) {
    for _ in 0..warmup_count {
        let msg = vec![0xABu8; warmup_size];
        conn_a.send(msg).await.unwrap();
        let _: Vec<u8> = conn_b.recv().await.unwrap();
    }
}

/// Benchmark with larger transfers to see if pattern holds
///
/// Tests 256KB transfers with 10ms, 50ms, 100ms delays.
/// If 100ms is still faster than 50ms with large transfers, it's real.
/// If pattern corrects (higher delay = slower), it was variance.
pub fn bench_large_transfer_validation(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("ledbat/validation/256kb");
    group.sample_size(20); // Faster: 20 samples instead of 100
    group.measurement_time(Duration::from_secs(8)); // Faster: 8s instead of 15s

    for delay_ms in [10, 50, 100] {
        let delay = Duration::from_millis(delay_ms);
        group.throughput(Throughput::Bytes(262144)); // 256KB

        group.bench_with_input(
            BenchmarkId::new("with_warmup", format!("{}ms", delay_ms)),
            &delay,
            |b, &delay| {
                b.to_async(&rt).iter_batched(
                    || {
                        let message = vec![0xABu8; 262144]; // 256KB
                        (Arc::new(DashMap::new()) as Channels, message, delay)
                    },
                    |(channels, message, delay)| async move {
                        let delay_policy = PacketDelayPolicy::Fixed(delay);

                        let (peer_a_pub, mut peer_a, peer_a_addr) =
                            create_mock_peer_with_delay(
                                PacketDropPolicy::ReceiveAll,
                                delay_policy.clone(),
                                channels.clone(),
                            )
                            .await
                            .unwrap();
                        let (peer_b_pub, mut peer_b, peer_b_addr) =
                            create_mock_peer_with_delay(
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

                        // Warmup: 3 transfers of 64KB each to eliminate initial burst effects
                        warmup_connection(&mut conn_a, &mut conn_b, 3, 65536).await;

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

/// Benchmark 1MB transfers to confirm pattern with very large data
pub fn bench_1mb_transfer_validation(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("ledbat/validation/1mb");
    group.sample_size(10); // Very fast: only 10 samples
    group.measurement_time(Duration::from_secs(10));

    for delay_ms in [10, 50, 100] {
        let delay = Duration::from_millis(delay_ms);
        group.throughput(Throughput::Bytes(1048576)); // 1MB

        group.bench_with_input(
            BenchmarkId::new("with_warmup", format!("{}ms", delay_ms)),
            &delay,
            |b, &delay| {
                b.to_async(&rt).iter_batched(
                    || {
                        let message = vec![0xABu8; 1048576]; // 1MB
                        (Arc::new(DashMap::new()) as Channels, message, delay)
                    },
                    |(channels, message, delay)| async move {
                        let delay_policy = PacketDelayPolicy::Fixed(delay);

                        let (peer_a_pub, mut peer_a, peer_a_addr) =
                            create_mock_peer_with_delay(
                                PacketDropPolicy::ReceiveAll,
                                delay_policy.clone(),
                                channels.clone(),
                            )
                            .await
                            .unwrap();
                        let (peer_b_pub, mut peer_b, peer_b_addr) =
                            create_mock_peer_with_delay(
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

                        // Warmup: 5 transfers to fully stabilize LEDBAT
                        warmup_connection(&mut conn_a, &mut conn_b, 5, 65536).await;

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

/// Quick congestion test with larger transfer
pub fn bench_congestion_256kb(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("ledbat/validation/congestion");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(12));

    for (loss_pct, delay_ms) in [(1.0, 50), (5.0, 100)] {
        group.throughput(Throughput::Bytes(262144));

        group.bench_with_input(
            BenchmarkId::new("256kb", format!("{}%loss_{}ms", loss_pct as u32, delay_ms)),
            &(loss_pct, delay_ms),
            |b, &(loss_pct, delay_ms)| {
                b.to_async(&rt).iter_batched(
                    || {
                        let message = vec![0xABu8; 262144];
                        (Arc::new(DashMap::new()) as Channels, message)
                    },
                    |(channels, message)| async move {
                        let drop_policy = PacketDropPolicy::Factor(loss_pct / 100.0);
                        let delay_policy =
                            PacketDelayPolicy::Fixed(Duration::from_millis(delay_ms));

                        let (peer_a_pub, mut peer_a, peer_a_addr) =
                            create_mock_peer_with_delay(
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
                        warmup_connection(&mut conn_a, &mut conn_b, 2, 65536).await;

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
