//! LEDBAT Validation Benchmarks
//!
//! These benchmarks test warm connection throughput with connection reuse.
//!
//! **Design principles:**
//! - Reuse connections across iterations (avoid connection setup overhead)
//! - Warmup connection before measuring
//! - Measure steady-state LEDBAT throughput
//!
//! Total runtime: ~2-3 minutes

use criterion::{BatchSize, Criterion, Throughput};
use dashmap::DashMap;
use freenet::transport::mock_transport::{create_mock_peer, Channels, PacketDropPolicy};
use std::hint::black_box as std_black_box;
use std::sync::Arc;
use std::time::Duration;

/// Validate warm connection throughput with connection reuse
///
/// Tests 64KB transfers on a pre-warmed connection to measure steady-state
/// LEDBAT throughput without connection establishment overhead.
pub fn bench_large_transfer_validation(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("ledbat/warm");
    // Use defaults from criterion_group config

    // Single benchmark: 64KB transfers on warm connection
    group.throughput(Throughput::Bytes(65536)); // 64KB

    // Note: Connection reuse with iter_batched causes connection closure issues.
    // Using per-iteration connection creation (like transport_ci benchmarks).
    group.bench_function("64kb", |b| {
        b.to_async(&rt).iter_batched(
            || {
                let message = vec![0xABu8; 65536];
                (Arc::new(DashMap::new()) as Channels, message)
            },
            |(channels, message)| async move {
                // Create peers fresh each iteration
                let (peer_a_pub, mut peer_a, peer_a_addr) =
                    create_mock_peer(PacketDropPolicy::ReceiveAll, channels.clone())
                        .await
                        .unwrap();

                let (peer_b_pub, mut peer_b, peer_b_addr) =
                    create_mock_peer(PacketDropPolicy::ReceiveAll, channels)
                        .await
                        .unwrap();

                // Connect
                let (conn_a_inner, conn_b_inner) = futures::join!(
                    peer_a.connect(peer_b_pub, peer_b_addr),
                    peer_b.connect(peer_a_pub, peer_a_addr),
                );
                let (conn_a, conn_b) = futures::join!(conn_a_inner, conn_b_inner);
                let (mut conn_a, mut conn_b) = (conn_a.unwrap(), conn_b.unwrap());

                // Transfer
                conn_a.send(message).await.unwrap();
                let received: Vec<u8> = conn_b.recv().await.unwrap();

                assert!(!received.is_empty());
                std_black_box(received);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

/// Validate 128KB transfers on warm connection
pub fn bench_1mb_transfer_validation(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("ledbat/warm");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(8));
    group.throughput(Throughput::Bytes(131072)); // 128KB

    group.bench_function("128kb", |b| {
        let (conn_a, conn_b) = rt.block_on(async {
            let channels: Channels = Arc::new(DashMap::new());

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

            // Warmup: 3 x 64KB transfers
            for _ in 0..3 {
                let msg = vec![0xABu8; 65536];
                conn_a.send(msg).await.unwrap();
                let _: Vec<u8> = conn_b.recv().await.unwrap();
            }

            (conn_a, conn_b)
        });

        let conn_a = Arc::new(tokio::sync::Mutex::new(conn_a));
        let conn_b = Arc::new(tokio::sync::Mutex::new(conn_b));

        b.to_async(&rt).iter_batched(
            || vec![0xABu8; 131072], // 128KB message
            |message| {
                let conn_a = conn_a.clone();
                let conn_b = conn_b.clone();
                async move {
                    let mut conn_a = conn_a.lock().await;
                    let mut conn_b = conn_b.lock().await;

                    conn_a.send(message).await.unwrap();
                    let received: Vec<u8> = conn_b.recv().await.unwrap();

                    assert!(!received.is_empty());
                    std_black_box(received);
                }
            },
            BatchSize::SmallInput,
        );
    });

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

    // Test 1% loss
    group.throughput(Throughput::Bytes(65536));

    group.bench_function("64kb/1pct_loss", |b| {
        let (conn_a, conn_b) = rt.block_on(async {
            let channels: Channels = Arc::new(DashMap::new());
            let drop_policy = PacketDropPolicy::Factor(0.01); // 1% loss

            let (peer_a_pub, mut peer_a, peer_a_addr) =
                create_mock_peer(drop_policy.clone(), channels.clone())
                    .await
                    .unwrap();

            let (peer_b_pub, mut peer_b, peer_b_addr) =
                create_mock_peer(drop_policy, channels).await.unwrap();

            let (conn_a_inner, conn_b_inner) = futures::join!(
                peer_a.connect(peer_b_pub, peer_b_addr),
                peer_b.connect(peer_a_pub, peer_a_addr),
            );
            let (conn_a, conn_b) = futures::join!(conn_a_inner, conn_b_inner);
            let (mut conn_a, mut conn_b) = (conn_a.unwrap(), conn_b.unwrap());

            // Warmup
            for _ in 0..3 {
                let msg = vec![0xABu8; 32768];
                conn_a.send(msg).await.unwrap();
                let _: Vec<u8> = conn_b.recv().await.unwrap();
            }

            (conn_a, conn_b)
        });

        let conn_a = Arc::new(tokio::sync::Mutex::new(conn_a));
        let conn_b = Arc::new(tokio::sync::Mutex::new(conn_b));

        b.to_async(&rt).iter_batched(
            || vec![0xABu8; 65536],
            |message| {
                let conn_a = conn_a.clone();
                let conn_b = conn_b.clone();
                async move {
                    let mut conn_a = conn_a.lock().await;
                    let mut conn_b = conn_b.lock().await;

                    conn_a.send(message).await.unwrap();
                    let received: Vec<u8> = conn_b.recv().await.unwrap();

                    assert!(!received.is_empty());
                    std_black_box(received);
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}
