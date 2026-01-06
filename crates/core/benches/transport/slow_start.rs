//! Slow Start Validation - Benchmarks to measure cold-start throughput
//!
//! **VirtualTime Mode:**
//! All benchmarks use VirtualTime for instant execution of high-RTT scenarios.
//! A 100ms RTT simulation completes in ~1ms wall time while accurately
//! measuring virtual throughput.
//!
//! Expected runtime: ~1-2 minutes (vs 30+ minutes with real-time delays)

use criterion::{Criterion, Throughput};
use dashmap::DashMap;
use freenet::simulation::{TimeSource, VirtualTime};
use freenet::transport::mock_transport::{Channels, PacketDelayPolicy, PacketDropPolicy};
use std::hint::black_box as std_black_box;
use std::sync::Arc;
use std::time::Duration;

use super::common::{create_connected_peers_with_virtual_time, spawn_auto_advance_task};

/// Benchmark fresh connection cold-start throughput with VirtualTime
///
/// Uses VirtualTime for instant execution - 100ms RTT completes in ~1ms wall time.
/// Measures the actual slow start benefit by tracking virtual time elapsed.
pub fn bench_cold_start_throughput(c: &mut Criterion) {
    // Use single-threaded runtime for deterministic scheduling with VirtualTime
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let time_source = VirtualTime::new();

    let mut group = c.benchmark_group("slow_start/cold_start");
    group.sample_size(10);

    // Use 16KB - standard size for cold start measurement
    let transfer_size = 16 * 1024;
    group.throughput(Throughput::Bytes(transfer_size as u64));

    let ts = time_source.clone();
    group.bench_function("16kb_transfer", |b| {
            b.to_async(&rt).iter_custom(|iters| {
                let ts = ts.clone();
                async move {
                    // Spawn auto-advance task to prevent deadlocks when tasks are blocked
                    let _auto_advance = spawn_auto_advance_task(ts.clone());

                    let mut total_virtual_time = Duration::ZERO;

                    for _ in 0..iters {
                        let channels: Channels = Arc::new(DashMap::new());
                        let message: Vec<u8> = vec![0xABu8; transfer_size];

                        // Create peers with VirtualTime (no delay for cold start baseline)
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
                                    eprintln!("cold_start peer_a creation failed: {:?}", e);
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
                                    eprintln!("cold_start peer_b creation failed: {:?}", e);
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
                                eprintln!("cold_start receiver connect failed: {:?}", e);
                                continue;
                            }
                            (_, Err(e)) => {
                                eprintln!("cold_start sender connect failed: {:?}", e);
                                continue;
                            }
                        };

                        // Send from B to A
                        let send_result = conn_b.send(message).await;
                        let sent_ok = match send_result {
                            Ok(()) => true,
                            Err(e) => {
                                eprintln!("cold_start send failed: {:?}", e);
                                false
                            }
                        };

                        // Receive at A
                        let received_len = if sent_ok {
                            match conn_a.recv().await {
                                Ok(received) => {
                                    std_black_box(&received);
                                    received.len()
                                }
                                Err(e) => {
                                    eprintln!("cold_start recv failed: {:?}", e);
                                    0
                                }
                            }
                        } else {
                            0
                        };

                        // Keep peers alive until end of iteration
                        drop(conn_a);
                        drop(conn_b);
                        drop(peer_a);
                        drop(peer_b);

                        let end_virtual = ts.now_nanos();
                        total_virtual_time +=
                            Duration::from_nanos(end_virtual.saturating_sub(start_virtual));

                        if !sent_ok || received_len == 0 {
                            eprintln!(
                                "cold_start failed: sent={}, received={}",
                                sent_ok, received_len
                            );
                        }

                        drop(channels);
                    }

                    total_virtual_time
                }
            });
        });
    group.finish();
}

/// Benchmark warm connection throughput with VirtualTime
///
/// Creates fresh connections, performs warmup transfers, then measures.
/// Uses VirtualTime for instant execution.
pub fn bench_warm_connection_throughput(c: &mut Criterion) {
    // Use single-threaded runtime for deterministic scheduling with VirtualTime
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let time_source = VirtualTime::new();

    let mut group = c.benchmark_group("slow_start/warm_connection");
    group.sample_size(10);

    let transfer_size = 16 * 1024;
    group.throughput(Throughput::Bytes(transfer_size as u64));

    let ts = time_source.clone();
    group.bench_function("16kb_warm", |b| {
        b.to_async(&rt).iter_custom(|iters| {
            let ts = ts.clone();
            async move {
                // Spawn auto-advance task to prevent deadlocks when tasks are blocked
                let _auto_advance = spawn_auto_advance_task(ts.clone());

                let mut total_virtual_time = Duration::ZERO;

                for _ in 0..iters {
                    let channels: Channels = Arc::new(DashMap::new());

                    // Create peers with VirtualTime
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
                                eprintln!("warm peer_a creation failed: {:?}", e);
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
                                eprintln!("warm peer_b creation failed: {:?}", e);
                                continue;
                            }
                        };

                    const WARMUP_COUNT: usize = 3;
                    let warmup_size = 1000; // < MAX_DATA_SIZE

                    let start_virtual = ts.now_nanos();

                    // Spawn receiver task
                    let receiver = tokio::spawn(async move {
                        let conn_future = peer_b.connect(peer_a_pub, peer_a_addr).await;
                        let mut conn = match conn_future.await {
                            Ok(c) => c,
                            Err(e) => {
                                eprintln!("warm receiver connect failed: {:?}", e);
                                return None;
                            }
                        };

                        // Receive warmup transfers
                        for i in 0..WARMUP_COUNT {
                            match conn.recv().await {
                                Ok(_) => {}
                                Err(e) => {
                                    eprintln!("warm warmup recv {} failed: {:?}", i, e);
                                    return None;
                                }
                            }
                        }

                        // Receive measured transfer
                        match conn.recv().await {
                            Ok(received) => Some(received),
                            Err(e) => {
                                eprintln!("warm measured recv failed: {:?}", e);
                                None
                            }
                        }
                    });

                    // Spawn sender task
                    let sender = tokio::spawn(async move {
                        let conn_future = peer_a.connect(peer_b_pub, peer_b_addr).await;
                        let mut conn = match conn_future.await {
                            Ok(c) => c,
                            Err(e) => {
                                eprintln!("warm sender connect failed: {:?}", e);
                                return false;
                            }
                        };

                        // Yield to let receiver start
                        tokio::task::yield_now().await;

                        // Send warmup transfers
                        for i in 0..WARMUP_COUNT {
                            let warmup_msg = vec![0xABu8; warmup_size];
                            if let Err(e) = conn.send(warmup_msg).await {
                                eprintln!("warm warmup send {} failed: {:?}", i, e);
                                return false;
                            }
                            tokio::task::yield_now().await;
                        }

                        // Send measured transfer
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

/// Instrumented benchmark that captures cwnd evolution with VirtualTime
pub fn bench_cwnd_evolution(c: &mut Criterion) {
    // Use single-threaded runtime for deterministic scheduling with VirtualTime
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let time_source = VirtualTime::new();

    let mut group = c.benchmark_group("slow_start/cwnd_evolution");
    group.sample_size(10);

    let transfer_size = 16 * 1024;
    let ts = time_source.clone();

    group.bench_function("16kb_cwnd_trace", |b| {
        b.to_async(&rt).iter_custom(|iters| {
            let ts = ts.clone();
            async move {
                // Spawn auto-advance task to prevent deadlocks when tasks are blocked
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
                                eprintln!("cwnd_evolution peer_a creation failed: {:?}", e);
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
                                eprintln!("cwnd_evolution peer_b creation failed: {:?}", e);
                                continue;
                            }
                        };

                    let start_virtual = ts.now_nanos();

                    let receiver = tokio::spawn(async move {
                        let conn_future = peer_b.connect(peer_a_pub, peer_a_addr).await;
                        let mut conn = match conn_future.await {
                            Ok(c) => c,
                            Err(e) => {
                                eprintln!("cwnd_evolution receiver connect failed: {:?}", e);
                                return None;
                            }
                        };

                        match conn.recv().await {
                            Ok(received) => Some(received),
                            Err(e) => {
                                eprintln!("cwnd_evolution recv failed: {:?}", e);
                                None
                            }
                        }
                    });

                    let sender = tokio::spawn(async move {
                        let conn_future = peer_a.connect(peer_b_pub, peer_b_addr).await;
                        let mut conn = match conn_future.await {
                            Ok(c) => c,
                            Err(e) => {
                                eprintln!("cwnd_evolution sender connect failed: {:?}", e);
                                return None;
                            }
                        };

                        tokio::task::yield_now().await;

                        if let Err(e) = conn.send(message).await {
                            eprintln!("cwnd_evolution send failed: {:?}", e);
                            return None;
                        }
                        Some(())
                    });

                    let (recv_result, _) = tokio::join!(receiver, sender);

                    let end_virtual = ts.now_nanos();
                    let elapsed_virtual =
                        Duration::from_nanos(end_virtual.saturating_sub(start_virtual));
                    total_virtual_time += elapsed_virtual;

                    if let Ok(Some(received)) = recv_result {
                        let throughput_mbps = (0.016 / elapsed_virtual.as_secs_f64()) * 8.0;
                        if iters == 1 {
                            println!(
                                "16KB transfer: {:?} virtual ({:.2} Mbps simulated)",
                                elapsed_virtual, throughput_mbps
                            );
                        }
                        std_black_box(received);
                    }

                    drop(channels);
                }

                total_virtual_time
            }
        });
    });

    group.finish();
}

/// Benchmark RTT scenarios with VirtualTime - instant execution of high-latency paths
///
/// Tests various RTT values (0ms, 5ms, 10ms, 25ms, 50ms) to ensure LEDBAT
/// behaves correctly. With VirtualTime, a 50ms RTT test completes in ~1ms.
pub fn bench_rtt_scenarios(c: &mut Criterion) {
    // Use single-threaded runtime for deterministic scheduling with VirtualTime
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let time_source = VirtualTime::new();

    let mut group = c.benchmark_group("slow_start/rtt_scenarios");
    group.sample_size(10);

    let transfer_size = 16 * 1024;
    group.throughput(Throughput::Bytes(transfer_size as u64));

    // Test various RTT scenarios - all complete instantly with VirtualTime
    for rtt_ms in [0u64, 5, 10, 25, 50] {
        let ts = time_source.clone();

        group.bench_function(format!("16kb_rtt_{}ms", rtt_ms), |b| {
            b.to_async(&rt).iter_custom(|iters| {
                let ts = ts.clone();
                async move {
                    // Spawn auto-advance task to prevent deadlocks when tasks are blocked
                    let _auto_advance = spawn_auto_advance_task(ts.clone());

                    let mut total_virtual_time = Duration::ZERO;

                    for _ in 0..iters {
                        let channels: Channels = Arc::new(DashMap::new());
                        let delay = if rtt_ms == 0 {
                            PacketDelayPolicy::NoDelay
                        } else {
                            PacketDelayPolicy::Fixed(Duration::from_millis(rtt_ms))
                        };

                        let (peer_a_pub, mut peer_a, peer_a_addr) =
                            match freenet::transport::mock_transport::create_mock_peer_with_virtual_time(
                                PacketDropPolicy::ReceiveAll,
                                delay.clone(),
                                channels.clone(),
                                ts.clone(),
                            )
                            .await
                            {
                                Ok(p) => p,
                                Err(e) => {
                                    eprintln!("rtt_{} peer_a creation failed: {:?}", rtt_ms, e);
                                    continue;
                                }
                            };

                        let (peer_b_pub, mut peer_b, peer_b_addr) =
                            match freenet::transport::mock_transport::create_mock_peer_with_virtual_time(
                                PacketDropPolicy::ReceiveAll,
                                delay,
                                channels.clone(),
                                ts.clone(),
                            )
                            .await
                            {
                                Ok(p) => p,
                                Err(e) => {
                                    eprintln!("rtt_{} peer_b creation failed: {:?}", rtt_ms, e);
                                    continue;
                                }
                            };

                        let message = vec![0xABu8; transfer_size];
                        let start_virtual = ts.now_nanos();

                        let receiver = tokio::spawn(async move {
                            let conn_future = peer_b.connect(peer_a_pub, peer_a_addr).await;
                            let mut conn = match conn_future.await {
                                Ok(c) => c,
                                Err(e) => {
                                    eprintln!("rtt receiver connect failed: {:?}", e);
                                    return None;
                                }
                            };

                            match conn.recv().await {
                                Ok(received) => Some(received),
                                Err(e) => {
                                    eprintln!("rtt recv failed: {:?}", e);
                                    None
                                }
                            }
                        });

                        let sender = tokio::spawn(async move {
                            let conn_future = peer_a.connect(peer_b_pub, peer_b_addr).await;
                            let mut conn = match conn_future.await {
                                Ok(c) => c,
                                Err(e) => {
                                    eprintln!("rtt sender connect failed: {:?}", e);
                                    return false;
                                }
                            };

                            tokio::task::yield_now().await;

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

/// Benchmark 1MB transfer with high bandwidth limit to test maximum throughput
#[allow(dead_code)]
pub fn bench_high_bandwidth_throughput(c: &mut Criterion) {
    // Use single-threaded runtime for deterministic scheduling with VirtualTime
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let time_source = VirtualTime::new();

    let mut group = c.benchmark_group("slow_start/high_bandwidth");
    group.sample_size(10);
    group.throughput(Throughput::Bytes(1048576)); // 1MB

    let bandwidth_limit_mb = 100;
    let ts = time_source.clone();

    group.bench_function(
        format!("1mb_transfer_{}mb_limit", bandwidth_limit_mb),
        |b| {
            b.to_async(&rt).iter_custom(|iters| {
                let ts = ts.clone();
                async move {
                    // Spawn auto-advance task to prevent deadlocks when tasks are blocked
                    let _auto_advance = spawn_auto_advance_task(ts.clone());

                    let mut total_virtual_time = Duration::ZERO;

                    for _ in 0..iters {
                        let delay = Duration::from_millis(10); // 10ms RTT

                        // Create peers with VirtualTime and high bandwidth
                        let mut peers =
                            create_connected_peers_with_virtual_time(delay, ts.clone()).await;

                        let message = vec![0xABu8; 1024 * 1024]; // 1MB
                        let start_virtual = ts.now_nanos();

                        if let Err(e) = peers.conn_a.send(message).await {
                            eprintln!("high_bandwidth send failed: {:?}", e);
                            continue;
                        }
                        let received: Vec<u8> = match peers.conn_b.recv().await {
                            Ok(r) => r,
                            Err(e) => {
                                eprintln!("high_bandwidth recv failed: {:?}", e);
                                continue;
                            }
                        };

                        let end_virtual = ts.now_nanos();
                        let elapsed_virtual =
                            Duration::from_nanos(end_virtual.saturating_sub(start_virtual));
                        total_virtual_time += elapsed_virtual;

                        if iters == 1 {
                            let throughput_mbps = (1.0 / elapsed_virtual.as_secs_f64()) * 8.0;
                            println!(
                                "1MB @ {}MB/s limit, 10ms RTT: {:?} virtual ({:.2} Mbps simulated)",
                                bandwidth_limit_mb, elapsed_virtual, throughput_mbps
                            );
                        }

                        std_black_box(received);
                    }

                    total_virtual_time
                }
            });
        },
    );

    group.finish();
}
