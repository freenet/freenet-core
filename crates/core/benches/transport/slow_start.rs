//! Slow Start Validation - Benchmarks to measure cold-start throughput

use criterion::{BatchSize, Criterion, Throughput};
use dashmap::DashMap;
use freenet::transport::mock_transport::{
    create_mock_peer_with_bandwidth, create_mock_peer_with_delay, PacketDelayPolicy,
    PacketDropPolicy,
};
use std::hint::black_box as std_black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Benchmark fresh connection cold-start throughput
/// This measures the actual slow start benefit (or lack thereof)
pub fn bench_cold_start_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("slow_start/cold_start");
    group.sample_size(20); // More samples for variance analysis
    group.measurement_time(Duration::from_secs(30));

    for transfer_size_kb in [256, 512, 1024, 4096] {
        let transfer_size = transfer_size_kb * 1024;
        group.throughput(Throughput::Bytes(transfer_size as u64));

        for delay_ms in [10, 50, 100] {
            group.bench_function(
                format!("{}kb_{}ms_no_warmup", transfer_size_kb, delay_ms),
                |b| {
                    b.to_async(&rt).iter_batched(
                        || {
                            // Setup: create message only, NO connection yet
                            vec![0xABu8; transfer_size]
                        },
                        |message| async move {
                            // Connection creation is PART OF THE MEASUREMENT
                            // This measures real cold-start behavior
                            let delay = Duration::from_millis(delay_ms);
                            let channels = Arc::new(DashMap::new());

                            let (peer_a_pub, mut peer_a, peer_a_addr) =
                                create_mock_peer_with_delay(
                                    PacketDropPolicy::ReceiveAll,
                                    PacketDelayPolicy::Fixed(delay),
                                    channels.clone(),
                                )
                                .await
                                .unwrap();

                            let (peer_b_pub, mut peer_b, peer_b_addr) =
                                create_mock_peer_with_delay(
                                    PacketDropPolicy::ReceiveAll,
                                    PacketDelayPolicy::Fixed(delay),
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

                            // Measured transfer - FIRST transfer on fresh connection
                            conn_a.send(message).await.unwrap();
                            let received: Vec<u8> = conn_b.recv().await.unwrap();

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

/// Benchmark warm connection throughput (for comparison)
pub fn bench_warm_connection_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("slow_start/warm_connection");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(30));

    for transfer_size_kb in [256, 512, 1024, 4096] {
        let transfer_size = transfer_size_kb * 1024;
        group.throughput(Throughput::Bytes(transfer_size as u64));

        for delay_ms in [10, 50, 100] {
            let delay = Duration::from_millis(delay_ms);

            group.bench_function(
                format!("{}kb_{}ms_warm", transfer_size_kb, delay_ms),
                |b| {
                    // Setup connection and warmup OUTSIDE measurement
                    let (conn_a, conn_b) = rt.block_on(async {
                        let channels = Arc::new(DashMap::new());

                        let (peer_a_pub, mut peer_a, peer_a_addr) = create_mock_peer_with_delay(
                            PacketDropPolicy::ReceiveAll,
                            PacketDelayPolicy::Fixed(delay),
                            channels.clone(),
                        )
                        .await
                        .unwrap();

                        let (peer_b_pub, mut peer_b, peer_b_addr) = create_mock_peer_with_delay(
                            PacketDropPolicy::ReceiveAll,
                            PacketDelayPolicy::Fixed(delay),
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

                        // Warmup: 10 x 100KB transfers to stabilize LEDBAT
                        for _ in 0..10 {
                            let msg = vec![0xABu8; 102400];
                            conn_a.send(msg).await.unwrap();
                            let _: Vec<u8> = conn_b.recv().await.unwrap();
                        }

                        (conn_a, conn_b)
                    });

                    let conn_a = Arc::new(tokio::sync::Mutex::new(conn_a));
                    let conn_b = Arc::new(tokio::sync::Mutex::new(conn_b));

                    b.to_async(&rt).iter_batched(
                        || vec![0xABu8; transfer_size],
                        |message| {
                            let conn_a = conn_a.clone();
                            let conn_b = conn_b.clone();
                            async move {
                                let mut conn_a = conn_a.lock().await;
                                let mut conn_b = conn_b.lock().await;

                                conn_a.send(message).await.unwrap();
                                let received: Vec<u8> = conn_b.recv().await.unwrap();
                                std_black_box(received);
                            }
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }
    group.finish();
}

/// Instrumented benchmark that captures cwnd evolution
/// This is for diagnostic purposes, not CI performance tracking
pub fn bench_cwnd_evolution(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("slow_start/cwnd_evolution");
    group.sample_size(10);

    group.bench_function("1mb_100ms_cwnd_trace", |b| {
        b.to_async(&rt).iter(|| async {
            let delay = Duration::from_millis(100);
            let channels = Arc::new(DashMap::new());

            // Create connection with cwnd tracing enabled
            let (peer_a_pub, mut peer_a, peer_a_addr) = create_mock_peer_with_delay(
                PacketDropPolicy::ReceiveAll,
                PacketDelayPolicy::Fixed(delay),
                channels.clone(),
            )
            .await
            .unwrap();

            let (peer_b_pub, mut peer_b, peer_b_addr) = create_mock_peer_with_delay(
                PacketDropPolicy::ReceiveAll,
                PacketDelayPolicy::Fixed(delay),
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

            let message = vec![0xABu8; 1024 * 1024];
            let start = Instant::now();
            conn_a.send(message).await.unwrap();
            let received: Vec<u8> = conn_b.recv().await.unwrap();
            let elapsed = start.elapsed();

            // Log timing (visible with --nocapture)
            let throughput_mbps = (1.0 / elapsed.as_secs_f64()) * 8.0;
            println!("1MB transfer: {:?} ({:.2} Mbps)", elapsed, throughput_mbps);

            std_black_box(received);
        });
    });

    group.finish();
}

/// Benchmark 1MB transfer with high bandwidth limit to test maximum throughput
/// This verifies that with sufficient bandwidth, LEDBAT can achieve >10 MB/s
///
/// TODO: This benchmark is currently #[ignore]d because it requires investigation
/// into why high bandwidth limits don't achieve expected throughput in mock transport.
/// The benchmark is kept for future debugging once we understand the bottleneck.
#[allow(dead_code)]
pub fn bench_high_bandwidth_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("slow_start/high_bandwidth");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));
    group.throughput(Throughput::Bytes(1048576)); // 1MB

    // Test with 100 MB/s bandwidth limit (10x the default)
    let bandwidth_limit_mb = 100;

    group.bench_function(
        format!("1mb_transfer_{}mb_limit", bandwidth_limit_mb),
        |b| {
            b.to_async(&rt).iter(|| async {
                let delay = Duration::from_millis(10); // 10ms RTT
                let channels = Arc::new(DashMap::new());
                let bandwidth_limit = Some(bandwidth_limit_mb * 1_000_000); // Convert to bytes/sec

                // Create connection with high bandwidth limit
                let (peer_a_pub, mut peer_a, peer_a_addr) = create_mock_peer_with_bandwidth(
                    PacketDropPolicy::ReceiveAll,
                    PacketDelayPolicy::Fixed(delay),
                    channels.clone(),
                    bandwidth_limit,
                )
                .await
                .unwrap();

                let (peer_b_pub, mut peer_b, peer_b_addr) = create_mock_peer_with_bandwidth(
                    PacketDropPolicy::ReceiveAll,
                    PacketDelayPolicy::Fixed(delay),
                    channels,
                    bandwidth_limit,
                )
                .await
                .unwrap();

                let (conn_a_inner, conn_b_inner) = futures::join!(
                    peer_a.connect(peer_b_pub, peer_b_addr),
                    peer_b.connect(peer_a_pub, peer_a_addr),
                );
                let (conn_a, conn_b) = futures::join!(conn_a_inner, conn_b_inner);
                let (mut conn_a, mut conn_b) = (conn_a.unwrap(), conn_b.unwrap());

                let message = vec![0xABu8; 1024 * 1024]; // 1MB
                let start = Instant::now();
                conn_a.send(message).await.unwrap();
                let received: Vec<u8> = conn_b.recv().await.unwrap();
                let elapsed = start.elapsed();

                // Log throughput
                let throughput_mbps = (1.0 / elapsed.as_secs_f64()) * 8.0;
                println!(
                    "1MB @ {}MB/s limit, 10ms RTT: {:?} ({:.2} Mbps)",
                    bandwidth_limit_mb, elapsed, throughput_mbps
                );

                std_black_box(received);
            });
        },
    );

    group.finish();
}
