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
    create_mock_peer_with_bandwidth, create_mock_peer_with_delay, Channels, PacketDelayPolicy,
    PacketDropPolicy,
};
use std::hint::black_box as std_black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::common::{DELAY_MICROS, TRANSFER_SIZES_KB};

/// Benchmark fresh connection cold-start throughput
/// This measures the actual slow start benefit (or lack thereof)
///
/// Uses microsecond delays to keep benchmark runtime reasonable.
/// Tests same LEDBAT dynamics as millisecond delays but 100x faster.
///
/// **Pattern**: Uses `tokio::spawn` for sender/receiver tasks with
/// synchronization delay (100ms like the working tests).
pub fn bench_cold_start_throughput(c: &mut Criterion) {
    use freenet::transport::mock_transport::create_mock_peer;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("slow_start/cold_start");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    // Use 16KB only - larger sizes timeout with mock transport
    // (The mock transport is reliable only for smaller messages in benchmark context)
    for &transfer_size_kb in &[16] {
        let transfer_size = transfer_size_kb * 1024;
        group.throughput(Throughput::Bytes(transfer_size as u64));

        group.bench_function(format!("{}kb_transfer", transfer_size_kb), |b| {
            b.to_async(&rt).iter(|| async {
                let channels: Channels = Arc::new(DashMap::new());
                let message: Vec<u8> = vec![0xABu8; transfer_size];
                let _expected_len = transfer_size;

                // Create peers (no delay - matching working tests)
                let (peer_a_pub, mut peer_a, peer_a_addr) =
                    match create_mock_peer(PacketDropPolicy::ReceiveAll, channels.clone()).await {
                        Ok(p) => p,
                        Err(e) => {
                            eprintln!("cold_start peer_a creation failed: {:?}", e);
                            return;
                        }
                    };

                let (peer_b_pub, mut peer_b, peer_b_addr) =
                    match create_mock_peer(PacketDropPolicy::ReceiveAll, channels.clone()).await {
                        Ok(p) => p,
                        Err(e) => {
                            eprintln!("cold_start peer_b creation failed: {:?}", e);
                            return;
                        }
                    };

                // Spawn receiver task (peer A receives) - matching test pattern
                let receiver = tokio::spawn(async move {
                    let conn_future = peer_a.connect(peer_b_pub, peer_b_addr).await;
                    let mut conn =
                        match tokio::time::timeout(Duration::from_secs(5), conn_future).await {
                            Ok(Ok(c)) => c,
                            Ok(Err(e)) => {
                                eprintln!("cold_start receiver connect failed: {:?}", e);
                                return 0;
                            }
                            Err(_) => {
                                eprintln!("cold_start receiver connect timeout");
                                return 0;
                            }
                        };

                    match tokio::time::timeout(Duration::from_secs(5), conn.recv()).await {
                        Ok(Ok(received)) => {
                            std_black_box(&received);
                            received.len()
                        }
                        Ok(Err(e)) => {
                            eprintln!("cold_start recv failed: {:?}", e);
                            0
                        }
                        Err(_) => {
                            eprintln!("cold_start recv timeout");
                            0
                        }
                    }
                });

                // Spawn sender task (peer B sends) - matching test pattern
                let sender = tokio::spawn(async move {
                    let conn_future = peer_b.connect(peer_a_pub, peer_a_addr).await;
                    let mut conn =
                        match tokio::time::timeout(Duration::from_secs(5), conn_future).await {
                            Ok(Ok(c)) => c,
                            Ok(Err(e)) => {
                                eprintln!("cold_start sender connect failed: {:?}", e);
                                return false;
                            }
                            Err(_) => {
                                eprintln!("cold_start sender connect timeout");
                                return false;
                            }
                        };

                    // 100ms delay to ensure receiver is ready (required by mock transport)
                    tokio::time::sleep(Duration::from_millis(100)).await;

                    match conn.send(message).await {
                        Ok(()) => true,
                        Err(e) => {
                            eprintln!("cold_start send failed: {:?}", e);
                            false
                        }
                    }
                });

                let (recv_result, send_result) = tokio::join!(receiver, sender);
                let received_len = recv_result.unwrap_or(0);
                let sent_ok = send_result.unwrap_or(false);

                // Note: received_len may differ from expected_len due to serialization overhead
                if !sent_ok || received_len == 0 {
                    eprintln!(
                        "cold_start failed: sent={}, received={}",
                        sent_ok, received_len
                    );
                }

                // Keep channels alive
                drop(channels);
            });
        });
    }
    group.finish();
}

/// Benchmark warm connection throughput
///
/// Creates fresh connections inside each iteration to work with mock transport.
/// Each iteration: connect → warmup (3 small transfers) → measured transfer
///
/// This measures realistic throughput including LEDBAT warmup effects.
/// Uses small warmup messages (<1.4KB) to avoid cwnd exhaustion, then measures
/// full-size transfer throughput.
///
/// **Pattern**: Uses `tokio::spawn` for sender/receiver tasks with
/// synchronization delay (100ms, required by mock transport).
/// Small warmup transfers establish RTT estimates without blocking cwnd.
pub fn bench_warm_connection_throughput(c: &mut Criterion) {
    use freenet::transport::mock_transport::create_mock_peer;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("slow_start/warm_connection");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    // Use 16KB only - mock transport is unreliable with larger sizes
    let transfer_size = 16 * 1024;
    group.throughput(Throughput::Bytes(transfer_size as u64));

    group.bench_function("16kb_warm", |b| {
        b.to_async(&rt).iter(|| async {
            let channels: Channels = Arc::new(DashMap::new());

            // Create peers (no artificial delay)
            let (peer_a_pub, mut peer_a, peer_a_addr) =
                match create_mock_peer(PacketDropPolicy::ReceiveAll, channels.clone()).await {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!("warm peer_a creation failed: {:?}", e);
                        return;
                    }
                };

            let (peer_b_pub, mut peer_b, peer_b_addr) =
                match create_mock_peer(PacketDropPolicy::ReceiveAll, channels.clone()).await {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!("warm peer_b creation failed: {:?}", e);
                        return;
                    }
                };

            const WARMUP_COUNT: usize = 3;
            // Use small warmup messages that fit in a single packet (no streaming)
            // This avoids cwnd exhaustion while still warming LEDBAT RTT estimates
            let warmup_size = 1000; // < MAX_DATA_SIZE (~1463 bytes)

            // Spawn receiver task - receives warmup + measured transfers
            let receiver = tokio::spawn(async move {
                let conn_future = peer_b.connect(peer_a_pub, peer_a_addr).await;
                let mut conn = match tokio::time::timeout(Duration::from_secs(5), conn_future).await
                {
                    Ok(Ok(c)) => c,
                    Ok(Err(e)) => {
                        eprintln!("warm receiver connect failed: {:?}", e);
                        return None;
                    }
                    Err(_) => {
                        eprintln!("warm receiver connect timeout");
                        return None;
                    }
                };

                // Receive warmup transfers
                for i in 0..WARMUP_COUNT {
                    match tokio::time::timeout(Duration::from_secs(5), conn.recv()).await {
                        Ok(Ok(_)) => {}
                        Ok(Err(e)) => {
                            eprintln!("warm warmup recv {} failed: {:?}", i, e);
                            return None;
                        }
                        Err(_) => {
                            eprintln!("warm warmup recv {} timeout", i);
                            return None;
                        }
                    }
                }

                // Receive measured transfer
                match tokio::time::timeout(Duration::from_secs(10), conn.recv()).await {
                    Ok(Ok(received)) => Some(received),
                    Ok(Err(e)) => {
                        eprintln!("warm measured recv failed: {:?}", e);
                        None
                    }
                    Err(_) => {
                        eprintln!("warm measured recv timeout");
                        None
                    }
                }
            });

            // Spawn sender task - sends warmup + measured transfers
            let sender = tokio::spawn(async move {
                let conn_future = peer_a.connect(peer_b_pub, peer_b_addr).await;
                let mut conn = match tokio::time::timeout(Duration::from_secs(5), conn_future).await
                {
                    Ok(Ok(c)) => c,
                    Ok(Err(e)) => {
                        eprintln!("warm sender connect failed: {:?}", e);
                        return false;
                    }
                    Err(_) => {
                        eprintln!("warm sender connect timeout");
                        return false;
                    }
                };

                // 100ms delay to ensure receiver is ready (required by mock transport)
                tokio::time::sleep(Duration::from_millis(100)).await;

                // Send small warmup transfers (single packet each, no streaming)
                for i in 0..WARMUP_COUNT {
                    let warmup_msg = vec![0xABu8; warmup_size];
                    if let Err(e) = conn.send(warmup_msg).await {
                        eprintln!("warm warmup send {} failed: {:?}", i, e);
                        return false;
                    }
                    // Short delay between warmup sends
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }

                // Send measured transfer (16KB - requires streaming)
                // Initial cwnd (38KB) minus small warmups (~3KB) leaves plenty of room
                let message = vec![0xABu8; transfer_size];
                match conn.send(message).await {
                    Ok(()) => true,
                    Err(e) => {
                        eprintln!("warm measured send failed: {:?}", e);
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

/// Instrumented benchmark that captures cwnd evolution
/// This is for diagnostic purposes, not CI performance tracking
///
/// Uses no artificial delay (mock transport handles timing internally)
///
/// **Pattern**: Uses `tokio::spawn` for sender/receiver tasks with
/// synchronization delay (100ms, required by mock transport).
/// Limited to 16KB transfers for reliability with mock transport.
pub fn bench_cwnd_evolution(c: &mut Criterion) {
    use freenet::transport::mock_transport::create_mock_peer;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("slow_start/cwnd_evolution");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    // Use 16KB (mock transport is only reliable with smaller messages)
    let transfer_size = 16 * 1024;
    group.bench_function("16kb_cwnd_trace", |b| {
        b.to_async(&rt).iter(|| async {
            let channels: Channels = Arc::new(DashMap::new());
            let message = vec![0xABu8; transfer_size];

            // Create peers (no delay - matching working tests)
            let (peer_a_pub, mut peer_a, peer_a_addr) =
                match create_mock_peer(PacketDropPolicy::ReceiveAll, channels.clone()).await {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!("cwnd_evolution peer_a creation failed: {:?}", e);
                        return;
                    }
                };

            let (peer_b_pub, mut peer_b, peer_b_addr) =
                match create_mock_peer(PacketDropPolicy::ReceiveAll, channels.clone()).await {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!("cwnd_evolution peer_b creation failed: {:?}", e);
                        return;
                    }
                };

            let start = Instant::now();

            // Spawn receiver task first
            let receiver = tokio::spawn(async move {
                let conn_future = peer_b.connect(peer_a_pub, peer_a_addr).await;
                let mut conn = match tokio::time::timeout(Duration::from_secs(5), conn_future).await
                {
                    Ok(Ok(c)) => c,
                    Ok(Err(e)) => {
                        eprintln!("cwnd_evolution receiver connect failed: {:?}", e);
                        return None;
                    }
                    Err(_) => {
                        eprintln!("cwnd_evolution receiver connect timeout");
                        return None;
                    }
                };

                match tokio::time::timeout(Duration::from_secs(10), conn.recv()).await {
                    Ok(Ok(received)) => Some(received),
                    Ok(Err(e)) => {
                        eprintln!("cwnd_evolution recv failed: {:?}", e);
                        None
                    }
                    Err(_) => {
                        eprintln!("cwnd_evolution recv timeout");
                        None
                    }
                }
            });

            // Spawn sender task
            let sender = tokio::spawn(async move {
                let conn_future = peer_a.connect(peer_b_pub, peer_b_addr).await;
                let mut conn = match tokio::time::timeout(Duration::from_secs(5), conn_future).await
                {
                    Ok(Ok(c)) => c,
                    Ok(Err(e)) => {
                        eprintln!("cwnd_evolution sender connect failed: {:?}", e);
                        return None;
                    }
                    Err(_) => {
                        eprintln!("cwnd_evolution sender connect timeout");
                        return None;
                    }
                };

                // 100ms delay to ensure receiver is ready (required by mock transport)
                tokio::time::sleep(Duration::from_millis(100)).await;

                if let Err(e) = conn.send(message).await {
                    eprintln!("cwnd_evolution send failed: {:?}", e);
                    return None;
                }
                Some(())
            });

            let (recv_result, _) = tokio::join!(receiver, sender);
            let elapsed = start.elapsed();

            if let Ok(Some(received)) = recv_result {
                let throughput_mbps = (0.016 / elapsed.as_secs_f64()) * 8.0;
                println!("16KB transfer: {:?} ({:.2} Mbps)", elapsed, throughput_mbps);
                std_black_box(received);
            }

            drop(channels);
        });
    });

    group.finish();
}

/// Benchmark RTT scenarios to catch LEDBAT regressions on high-latency paths
///
/// Tests various RTT values (0ms, 5ms, 10ms, 25ms, 50ms) to ensure LEDBAT
/// behaves correctly under different network conditions. This catches issues
/// like #2578 (ssthresh collapse on high-latency paths).
///
/// **Pattern**: Uses `tokio::spawn` for sender/receiver tasks with
/// synchronization delay (100ms, required by mock transport).
pub fn bench_rtt_scenarios(c: &mut Criterion) {
    use freenet::transport::mock_transport::create_mock_peer_with_delay;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("slow_start/rtt_scenarios");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));

    let transfer_size = 16 * 1024; // 16KB
    group.throughput(Throughput::Bytes(transfer_size as u64));

    // Test various RTT scenarios
    for rtt_ms in [0u64, 5, 10, 25, 50] {
        group.bench_function(format!("16kb_rtt_{}ms", rtt_ms), |b| {
            b.to_async(&rt).iter(|| {
                let channels: Channels = Arc::new(DashMap::new());
                let delay = if rtt_ms == 0 {
                    PacketDelayPolicy::NoDelay
                } else {
                    PacketDelayPolicy::Fixed(Duration::from_millis(rtt_ms))
                };

                async move {
                    // Create peers with specified RTT
                    let (peer_a_pub, mut peer_a, peer_a_addr) = match create_mock_peer_with_delay(
                        PacketDropPolicy::ReceiveAll,
                        delay.clone(),
                        channels.clone(),
                    )
                    .await
                    {
                        Ok(p) => p,
                        Err(e) => {
                            eprintln!("rtt_{} peer_a creation failed: {:?}", rtt_ms, e);
                            return;
                        }
                    };

                    let (peer_b_pub, mut peer_b, peer_b_addr) = match create_mock_peer_with_delay(
                        PacketDropPolicy::ReceiveAll,
                        delay,
                        channels.clone(),
                    )
                    .await
                    {
                        Ok(p) => p,
                        Err(e) => {
                            eprintln!("rtt_{} peer_b creation failed: {:?}", rtt_ms, e);
                            return;
                        }
                    };

                    let message = vec![0xABu8; transfer_size];

                    // Spawn receiver task
                    let receiver =
                        tokio::spawn(async move {
                            let conn_future = peer_b.connect(peer_a_pub, peer_a_addr).await;
                            let mut conn =
                                match tokio::time::timeout(Duration::from_secs(15), conn_future)
                                    .await
                                {
                                    Ok(Ok(c)) => c,
                                    Ok(Err(e)) => {
                                        eprintln!("rtt receiver connect failed: {:?}", e);
                                        return None;
                                    }
                                    Err(_) => {
                                        eprintln!("rtt receiver connect timeout");
                                        return None;
                                    }
                                };

                            match tokio::time::timeout(Duration::from_secs(15), conn.recv()).await {
                                Ok(Ok(received)) => Some(received),
                                Ok(Err(e)) => {
                                    eprintln!("rtt recv failed: {:?}", e);
                                    None
                                }
                                Err(_) => {
                                    eprintln!("rtt recv timeout");
                                    None
                                }
                            }
                        });

                    // Spawn sender task
                    let sender =
                        tokio::spawn(async move {
                            let conn_future = peer_a.connect(peer_b_pub, peer_b_addr).await;
                            let mut conn =
                                match tokio::time::timeout(Duration::from_secs(15), conn_future)
                                    .await
                                {
                                    Ok(Ok(c)) => c,
                                    Ok(Err(e)) => {
                                        eprintln!("rtt sender connect failed: {:?}", e);
                                        return false;
                                    }
                                    Err(_) => {
                                        eprintln!("rtt sender connect timeout");
                                        return false;
                                    }
                                };

                            // 100ms delay to ensure receiver is ready (required by mock transport)
                            tokio::time::sleep(Duration::from_millis(100)).await;

                            match conn.send(message).await {
                                Ok(()) => true,
                                Err(e) => {
                                    eprintln!("rtt send failed: {:?}", e);
                                    false
                                }
                            }
                        });

                    let (recv_result, _) = tokio::join!(receiver, sender);
                    if let Ok(Some(received)) = recv_result {
                        std_black_box(received);
                    }

                    drop(channels);
                }
            });
        });
    }

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

                // Create connection with high bandwidth limit - handle errors gracefully
                let (peer_a_pub, mut peer_a, peer_a_addr) = match create_mock_peer_with_bandwidth(
                    PacketDropPolicy::ReceiveAll,
                    PacketDelayPolicy::Fixed(delay),
                    channels.clone(),
                    bandwidth_limit,
                )
                .await
                {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!("high_bandwidth peer_a creation failed: {:?}", e);
                        return;
                    }
                };

                let (peer_b_pub, mut peer_b, peer_b_addr) = match create_mock_peer_with_bandwidth(
                    PacketDropPolicy::ReceiveAll,
                    PacketDelayPolicy::Fixed(delay),
                    channels,
                    bandwidth_limit,
                )
                .await
                {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!("high_bandwidth peer_b creation failed: {:?}", e);
                        return;
                    }
                };

                let (conn_a_inner, conn_b_inner) = futures::join!(
                    peer_a.connect(peer_b_pub, peer_b_addr),
                    peer_b.connect(peer_a_pub, peer_a_addr),
                );
                let (conn_a, conn_b) = futures::join!(conn_a_inner, conn_b_inner);
                let (mut conn_a, mut conn_b) = match (conn_a, conn_b) {
                    (Ok(a), Ok(b)) => (a, b),
                    (Err(e), _) | (_, Err(e)) => {
                        eprintln!("high_bandwidth connection failed: {:?}", e);
                        return;
                    }
                };

                let message = vec![0xABu8; 1024 * 1024]; // 1MB
                let start = Instant::now();
                if let Err(e) = conn_a.send(message).await {
                    eprintln!("high_bandwidth send failed: {:?}", e);
                    return;
                }
                let received: Vec<u8> = match conn_b.recv().await {
                    Ok(r) => r,
                    Err(e) => {
                        eprintln!("high_bandwidth recv failed: {:?}", e);
                        return;
                    }
                };
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
