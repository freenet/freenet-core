//! Slow Start Validation - Benchmarks to measure cold-start throughput
//!
//! **Design Principles:**
//! - Use microsecond delays (100µs-1ms) to test LEDBAT dynamics quickly
//! - Millisecond delays (10-100ms) cause benchmarks to take 10+ minutes
//! - Reduced variants: 2 sizes × 2 delays = 4 variants (not 12)
//!
//! Expected runtime: ~3-5 minutes (vs 15+ minutes with ms delays)

use criterion::{BatchSize, Criterion, Throughput};
use dashmap::DashMap;
use freenet::transport::mock_transport::{
    create_mock_peer_with_bandwidth, create_mock_peer_with_delay, PacketDelayPolicy,
    PacketDropPolicy,
};
use std::hint::black_box as std_black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::common::{create_peer_pair_with_delay, new_channels, DELAY_MICROS, TRANSFER_SIZES_KB};

/// Benchmark fresh connection cold-start throughput
/// This measures the actual slow start benefit (or lack thereof)
///
/// Uses microsecond delays to keep benchmark runtime reasonable.
/// Tests same LEDBAT dynamics as millisecond delays but 100x faster.
pub fn bench_cold_start_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("slow_start/cold_start");
    group.sample_size(10); // Reduced from 20 - sufficient for validation
    group.measurement_time(Duration::from_secs(10)); // Reduced from 30s

    // Reduced variants: 2 sizes × 2 delays = 4 benchmarks (was 12)
    for &transfer_size_kb in TRANSFER_SIZES_KB {
        let transfer_size = transfer_size_kb * 1024;
        group.throughput(Throughput::Bytes(transfer_size as u64));

        // Use microsecond delays: 500µs and 2ms (equivalent dynamics to 50ms/200ms)
        for &delay_us in DELAY_MICROS {
            group.bench_function(
                format!("{}kb_{}us_no_warmup", transfer_size_kb, delay_us),
                |b| {
                    b.to_async(&rt).iter_batched(
                        || {
                            // Setup: create message only, NO connection yet
                            vec![0xABu8; transfer_size]
                        },
                        |message| async move {
                            // Connection creation is PART OF THE MEASUREMENT
                            // This measures real cold-start behavior
                            let delay = Duration::from_micros(delay_us);
                            let mut peers = create_peer_pair_with_delay(new_channels(), delay)
                                .await
                                .connect()
                                .await;

                            // Measured transfer - FIRST transfer on fresh connection
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

/// Benchmark warm connection throughput (for comparison)
///
/// Uses microsecond delays and reduced variants for reasonable runtime.
pub fn bench_warm_connection_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("slow_start/warm_connection");
    group.sample_size(10); // Reduced from 20
    group.measurement_time(Duration::from_secs(10)); // Reduced from 30s

    // Reduced variants: 2 sizes × 2 delays = 4 benchmarks (was 12)
    for &transfer_size_kb in TRANSFER_SIZES_KB {
        let transfer_size = transfer_size_kb * 1024;
        group.throughput(Throughput::Bytes(transfer_size as u64));

        // Use microsecond delays: 500µs and 2ms
        for &delay_us in DELAY_MICROS {
            let delay = Duration::from_micros(delay_us);

            group.bench_function(format!("{}kb_{}us_warm", transfer_size_kb, delay_us), |b| {
                // Setup connection and warmup OUTSIDE measurement
                // Keep peers alive to prevent channel closure
                let peers = rt.block_on(async {
                    let mut peers = create_peer_pair_with_delay(new_channels(), delay)
                        .await
                        .connect()
                        .await;

                    // Warmup: 5 x 64KB transfers to stabilize LEDBAT (reduced from 10x100KB)
                    for _ in 0..5 {
                        let msg = vec![0xABu8; 65536];
                        peers.conn_a.send(msg).await.unwrap();
                        let _: Vec<u8> = peers.conn_b.recv().await.unwrap();
                    }

                    peers
                });

                let peers = Arc::new(tokio::sync::Mutex::new(peers));

                b.to_async(&rt).iter_batched(
                    || vec![0xABu8; transfer_size],
                    |message| {
                        let peers = peers.clone();
                        async move {
                            let mut p = peers.lock().await;

                            p.conn_a.send(message).await.unwrap();
                            let received: Vec<u8> = p.conn_b.recv().await.unwrap();
                            std_black_box(received);
                        }
                    },
                    BatchSize::SmallInput,
                );
            });
        }
    }
    group.finish();
}

/// Instrumented benchmark that captures cwnd evolution
/// This is for diagnostic purposes, not CI performance tracking
///
/// Uses 2ms delay (reasonable compromise between realism and speed)
pub fn bench_cwnd_evolution(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("slow_start/cwnd_evolution");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15)); // Allow time for larger transfers

    // Use 256KB instead of 1MB, with 2ms delay (reasonable for diagnostic)
    group.bench_function("256kb_2ms_cwnd_trace", |b| {
        b.to_async(&rt).iter(|| async {
            let delay = Duration::from_millis(2); // 2ms instead of 100ms
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

            let message = vec![0xABu8; 256 * 1024]; // 256KB instead of 1MB
            let start = Instant::now();
            conn_a.send(message).await.unwrap();
            let received: Vec<u8> = conn_b.recv().await.unwrap();
            let elapsed = start.elapsed();

            // Log timing (visible with --nocapture)
            let throughput_mbps = (0.256 / elapsed.as_secs_f64()) * 8.0;
            println!(
                "256KB transfer: {:?} ({:.2} Mbps)",
                elapsed, throughput_mbps
            );

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
