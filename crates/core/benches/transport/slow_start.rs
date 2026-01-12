//! Slow Start Validation - Benchmarks to measure cold-start throughput
//!
//! **VirtualTime Mode:**
//! All benchmarks use VirtualTime for instant execution of high-RTT scenarios.
//! A 100ms RTT simulation completes in ~1ms wall time while accurately
//! measuring virtual throughput.
//!
//! Expected runtime: ~1-2 minutes (vs 30+ minutes with real-time delays)
//!
//! **Note:** Uses multi-threaded runtime to allow packet delivery (via real async
//! channels) to proceed concurrently with VirtualTime operations.

use criterion::{Criterion, Throughput};
use dashmap::DashMap;
use freenet::simulation::{TimeSource, VirtualTime};
use freenet::transport::congestion_control::CongestionControlConfig;
use freenet::transport::mock_transport::{
    create_mock_peer_with_congestion_config, Channels, PacketDelayPolicy, PacketDropPolicy,
};
use std::hint::black_box as std_black_box;
use std::sync::Arc;
use std::time::Duration;

use super::common::{
    create_connected_peers_with_configurable_congestion, get_congestion_config,
    print_congestion_config, spawn_auto_advance_task,
};

/// Benchmark fresh connection cold-start throughput with VirtualTime
///
/// Uses VirtualTime for instant execution - 100ms RTT completes in ~1ms wall time.
/// Measures the actual slow start benefit by tracking virtual time elapsed.
///
/// Congestion control algorithm is configurable via `FREENET_CONGESTION_ALGO` env var.
/// Defaults to BBR.
pub fn bench_cold_start_throughput(c: &mut Criterion) {
    // Print which congestion control algorithm is being used
    print_congestion_config();
    let congestion_config = Some(get_congestion_config());

    // Use multi-threaded runtime to allow packet delivery to proceed concurrently
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("slow_start/cold_start");
    group.sample_size(10);

    // Use 16KB - standard size for cold start measurement
    let transfer_size = 16 * 1024;
    group.throughput(Throughput::Bytes(transfer_size as u64));

    let config_clone = congestion_config.clone();
    group.bench_function("16kb_transfer", |b| {
        let config = config_clone.clone();
        b.to_async(&rt).iter_custom(|iters| {
            let config = config.clone();
            async move {
                let mut total_virtual_time = Duration::ZERO;

                for _ in 0..iters {
                    // Create fresh VirtualTime for each iteration to avoid time accumulation
                    let ts = VirtualTime::new();

                    // Spawn auto-advance task BEFORE connection
                    let auto_advance = spawn_auto_advance_task(ts.clone());

                    let channels: Channels = Arc::new(DashMap::new());
                    let message: Vec<u8> = vec![0xABu8; transfer_size];

                    // Create peers with VirtualTime and configurable congestion control
                    let (peer_a_pub, mut peer_a, peer_a_addr) =
                        match create_mock_peer_with_congestion_config(
                            PacketDropPolicy::ReceiveAll,
                            PacketDelayPolicy::NoDelay,
                            channels.clone(),
                            ts.clone(),
                            config.clone(),
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
                        match create_mock_peer_with_congestion_config(
                            PacketDropPolicy::ReceiveAll,
                            PacketDelayPolicy::NoDelay,
                            channels.clone(),
                            ts.clone(),
                            config.clone(),
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

                        auto_advance.abort();
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
///
/// Congestion control algorithm is configurable via `FREENET_CONGESTION_ALGO` env var.
/// Defaults to BBR.
pub fn bench_warm_connection_throughput(c: &mut Criterion) {
    // Get configurable congestion control
    let congestion_config = Some(get_congestion_config());

    // Use multi-threaded runtime to allow packet delivery to proceed concurrently
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("slow_start/warm_connection");
    group.sample_size(10);

    let transfer_size = 16 * 1024;
    group.throughput(Throughput::Bytes(transfer_size as u64));

    let config_clone = congestion_config.clone();
    group.bench_function("16kb_warm", |b| {
        let config = config_clone.clone();
        b.to_async(&rt).iter_custom(|iters| {
            let config = config.clone();
            async move {
                let mut total_virtual_time = Duration::ZERO;

                for _ in 0..iters {
                    // Create fresh VirtualTime for each iteration
                    let ts = VirtualTime::new();
                    let auto_advance = spawn_auto_advance_task(ts.clone());

                    let channels: Channels = Arc::new(DashMap::new());

                    // Create peers with VirtualTime and configurable congestion control
                    let (peer_a_pub, mut peer_a, peer_a_addr) =
                        match create_mock_peer_with_congestion_config(
                            PacketDropPolicy::ReceiveAll,
                            PacketDelayPolicy::NoDelay,
                            channels.clone(),
                            ts.clone(),
                            config.clone(),
                        )
                        .await
                        {
                            Ok(p) => p,
                            Err(e) => {
                                eprintln!("warm peer_a creation failed: {:?}", e);
                                auto_advance.abort();
                                continue;
                            }
                        };

                    let (peer_b_pub, mut peer_b, peer_b_addr) =
                        match create_mock_peer_with_congestion_config(
                            PacketDropPolicy::ReceiveAll,
                            PacketDelayPolicy::NoDelay,
                            channels.clone(),
                            ts.clone(),
                            config.clone(),
                        )
                        .await
                        {
                            Ok(p) => p,
                            Err(e) => {
                                eprintln!("warm peer_b creation failed: {:?}", e);
                                auto_advance.abort();
                                continue;
                            }
                        };

                    const WARMUP_COUNT: usize = 3;
                    let warmup_size = 1000; // < MAX_DATA_SIZE

                    let start_virtual = ts.now_nanos();

                    // Connect both peers concurrently (like cold_start pattern)
                    let (conn_a_future, conn_b_future) = futures::join!(
                        peer_a.connect(peer_b_pub, peer_b_addr),
                        peer_b.connect(peer_a_pub, peer_a_addr),
                    );

                    let (conn_a, conn_b) = futures::join!(conn_a_future, conn_b_future);

                    let (mut conn_a, mut conn_b) = match (conn_a, conn_b) {
                        (Ok(a), Ok(b)) => (a, b),
                        (Err(e), _) => {
                            eprintln!("warm conn_a connect failed: {:?}", e);
                            auto_advance.abort();
                            continue;
                        }
                        (_, Err(e)) => {
                            eprintln!("warm conn_b connect failed: {:?}", e);
                            auto_advance.abort();
                            continue;
                        }
                    };

                    // Send warmup transfers (send then recv for each)
                    let mut warmup_ok = true;
                    for i in 0..WARMUP_COUNT {
                        let warmup_msg = vec![0xABu8; warmup_size];
                        if let Err(e) = conn_a.send(warmup_msg).await {
                            eprintln!("warm warmup send {} failed: {:?}", i, e);
                            warmup_ok = false;
                            break;
                        }
                        match conn_b.recv().await {
                            Ok(_) => {}
                            Err(e) => {
                                eprintln!("warm warmup recv {} failed: {:?}", i, e);
                                warmup_ok = false;
                                break;
                            }
                        }
                    }

                    if !warmup_ok {
                        auto_advance.abort();
                        drop(conn_a);
                        drop(conn_b);
                        drop(peer_a);
                        drop(peer_b);
                        drop(channels);
                        continue;
                    }

                    // Send measured transfer
                    let message = vec![0xABu8; transfer_size];
                    let send_result = conn_a.send(message).await;
                    let sent_ok = match send_result {
                        Ok(()) => true,
                        Err(e) => {
                            eprintln!("warm measured send failed: {:?}", e);
                            false
                        }
                    };

                    // Receive measured transfer
                    let received_len = if sent_ok {
                        match conn_b.recv().await {
                            Ok(received) => {
                                std_black_box(&received);
                                received.len()
                            }
                            Err(e) => {
                                eprintln!("warm measured recv failed: {:?}", e);
                                0
                            }
                        }
                    } else {
                        0
                    };

                    // Keep resources alive until end of iteration
                    drop(conn_a);
                    drop(conn_b);
                    drop(peer_a);
                    drop(peer_b);

                    let end_virtual = ts.now_nanos();
                    total_virtual_time +=
                        Duration::from_nanos(end_virtual.saturating_sub(start_virtual));

                    if !sent_ok || received_len == 0 {
                        eprintln!(
                            "warm failed: sent={}, received={}",
                            sent_ok, received_len
                        );
                    }

                    auto_advance.abort();
                    drop(channels);
                }

                total_virtual_time
            }
        });
    });

    group.finish();
}

/// Instrumented benchmark that captures cwnd evolution with VirtualTime
///
/// Congestion control algorithm is configurable via `FREENET_CONGESTION_ALGO` env var.
pub fn bench_cwnd_evolution(c: &mut Criterion) {
    let congestion_config = Some(get_congestion_config());

    // Use multi-threaded runtime to allow packet delivery to proceed concurrently
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("slow_start/cwnd_evolution");
    group.sample_size(10);

    let transfer_size = 16 * 1024;

    let config_clone = congestion_config.clone();
    group.bench_function("16kb_cwnd_trace", |b| {
        let config = config_clone.clone();
        b.to_async(&rt).iter_custom(|iters| {
            let config = config.clone();
            async move {
                let mut total_virtual_time = Duration::ZERO;

                for _ in 0..iters {
                    // Create fresh VirtualTime for each iteration
                    let ts = VirtualTime::new();
                    let auto_advance = spawn_auto_advance_task(ts.clone());

                    let channels: Channels = Arc::new(DashMap::new());
                    let message: Vec<u8> = vec![0xABu8; transfer_size];

                    let (peer_a_pub, mut peer_a, peer_a_addr) =
                        match create_mock_peer_with_congestion_config(
                            PacketDropPolicy::ReceiveAll,
                            PacketDelayPolicy::NoDelay,
                            channels.clone(),
                            ts.clone(),
                            config.clone(),
                        )
                        .await
                        {
                            Ok(p) => p,
                            Err(e) => {
                                eprintln!("cwnd_evolution peer_a creation failed: {:?}", e);
                                auto_advance.abort();
                                continue;
                            }
                        };

                    let (peer_b_pub, mut peer_b, peer_b_addr) =
                        match create_mock_peer_with_congestion_config(
                            PacketDropPolicy::ReceiveAll,
                            PacketDelayPolicy::NoDelay,
                            channels.clone(),
                            ts.clone(),
                            config.clone(),
                        )
                        .await
                        {
                            Ok(p) => p,
                            Err(e) => {
                                eprintln!("cwnd_evolution peer_b creation failed: {:?}", e);
                                auto_advance.abort();
                                continue;
                            }
                        };

                    let start_virtual = ts.now_nanos();

                    // Connect both peers concurrently (like cold_start pattern)
                    let (conn_a_future, conn_b_future) = futures::join!(
                        peer_a.connect(peer_b_pub, peer_b_addr),
                        peer_b.connect(peer_a_pub, peer_a_addr),
                    );

                    let (conn_a, conn_b) = futures::join!(conn_a_future, conn_b_future);

                    let (mut conn_a, mut conn_b) = match (conn_a, conn_b) {
                        (Ok(a), Ok(b)) => (a, b),
                        (Err(e), _) => {
                            eprintln!("cwnd_evolution conn_a connect failed: {:?}", e);
                            auto_advance.abort();
                            continue;
                        }
                        (_, Err(e)) => {
                            eprintln!("cwnd_evolution conn_b connect failed: {:?}", e);
                            auto_advance.abort();
                            continue;
                        }
                    };

                    // Send from A to B
                    let send_result = conn_a.send(message).await;
                    let sent_ok = match send_result {
                        Ok(()) => true,
                        Err(e) => {
                            eprintln!("cwnd_evolution send failed: {:?}", e);
                            false
                        }
                    };

                    // Receive at B
                    let received = if sent_ok {
                        match conn_b.recv().await {
                            Ok(received) => Some(received),
                            Err(e) => {
                                eprintln!("cwnd_evolution recv failed: {:?}", e);
                                None
                            }
                        }
                    } else {
                        None
                    };

                    // Keep resources alive until end of iteration
                    drop(conn_a);
                    drop(conn_b);
                    drop(peer_a);
                    drop(peer_b);

                    let end_virtual = ts.now_nanos();
                    let elapsed_virtual =
                        Duration::from_nanos(end_virtual.saturating_sub(start_virtual));
                    total_virtual_time += elapsed_virtual;

                    if let Some(received) = received {
                        let throughput_mbps = (0.016 / elapsed_virtual.as_secs_f64()) * 8.0;
                        if iters == 1 {
                            println!(
                                "16KB transfer: {:?} virtual ({:.2} Mbps simulated)",
                                elapsed_virtual, throughput_mbps
                            );
                        }
                        std_black_box(received);
                    }

                    auto_advance.abort();
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
/// Tests various RTT values (0ms, 5ms, 10ms, 25ms, 50ms) to ensure congestion control
/// behaves correctly. With VirtualTime, a 50ms RTT test completes in ~1ms.
///
/// Congestion control algorithm is configurable via `FREENET_CONGESTION_ALGO` env var.
pub fn bench_rtt_scenarios(c: &mut Criterion) {
    let congestion_config = Some(get_congestion_config());

    // Use multi-threaded runtime to allow packet delivery to proceed concurrently
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("slow_start/rtt_scenarios");
    group.sample_size(10);

    let transfer_size = 16 * 1024;
    group.throughput(Throughput::Bytes(transfer_size as u64));

    // Test various RTT scenarios - all complete instantly with VirtualTime
    for rtt_ms in [0u64, 5, 10, 25, 50] {
        let config_clone = congestion_config.clone();
        group.bench_function(format!("16kb_rtt_{}ms", rtt_ms), |b| {
            let config = config_clone.clone();
            b.to_async(&rt).iter_custom(|iters| {
                let config = config.clone();
                async move {
                    let mut total_virtual_time = Duration::ZERO;

                    for _ in 0..iters {
                        // Create fresh VirtualTime for each iteration
                        let ts = VirtualTime::new();
                        let auto_advance = spawn_auto_advance_task(ts.clone());

                        let channels: Channels = Arc::new(DashMap::new());
                        let delay = if rtt_ms == 0 {
                            PacketDelayPolicy::NoDelay
                        } else {
                            PacketDelayPolicy::Fixed(Duration::from_millis(rtt_ms))
                        };
                        let message: Vec<u8> = vec![0xABu8; transfer_size];

                        let (peer_a_pub, mut peer_a, peer_a_addr) =
                            match create_mock_peer_with_congestion_config(
                                PacketDropPolicy::ReceiveAll,
                                delay.clone(),
                                channels.clone(),
                                ts.clone(),
                                config.clone(),
                            )
                            .await
                            {
                                Ok(p) => p,
                                Err(e) => {
                                    eprintln!("rtt_{} peer_a creation failed: {:?}", rtt_ms, e);
                                    auto_advance.abort();
                                    continue;
                                }
                            };

                        let (peer_b_pub, mut peer_b, peer_b_addr) =
                            match create_mock_peer_with_congestion_config(
                                PacketDropPolicy::ReceiveAll,
                                delay,
                                channels.clone(),
                                ts.clone(),
                                config.clone(),
                            )
                            .await
                            {
                                Ok(p) => p,
                                Err(e) => {
                                    eprintln!("rtt_{} peer_b creation failed: {:?}", rtt_ms, e);
                                    auto_advance.abort();
                                    continue;
                                }
                            };

                        let start_virtual = ts.now_nanos();

                        // Connect both peers concurrently (like cold_start pattern)
                        let (conn_a_future, conn_b_future) = futures::join!(
                            peer_a.connect(peer_b_pub, peer_b_addr),
                            peer_b.connect(peer_a_pub, peer_a_addr),
                        );

                        let (conn_a, conn_b) = futures::join!(conn_a_future, conn_b_future);

                        let (mut conn_a, mut conn_b) = match (conn_a, conn_b) {
                            (Ok(a), Ok(b)) => (a, b),
                            (Err(e), _) => {
                                eprintln!("rtt_{} conn_a connect failed: {:?}", rtt_ms, e);
                                auto_advance.abort();
                                continue;
                            }
                            (_, Err(e)) => {
                                eprintln!("rtt_{} conn_b connect failed: {:?}", rtt_ms, e);
                                auto_advance.abort();
                                continue;
                            }
                        };

                        // Send from A to B
                        let send_result = conn_a.send(message).await;
                        let sent_ok = match send_result {
                            Ok(()) => true,
                            Err(e) => {
                                eprintln!("rtt_{} send failed: {:?}", rtt_ms, e);
                                false
                            }
                        };

                        // Receive at B
                        let received_len = if sent_ok {
                            match conn_b.recv().await {
                                Ok(received) => {
                                    std_black_box(&received);
                                    received.len()
                                }
                                Err(e) => {
                                    eprintln!("rtt_{} recv failed: {:?}", rtt_ms, e);
                                    0
                                }
                            }
                        } else {
                            0
                        };

                        // Keep resources alive until end of iteration
                        drop(conn_a);
                        drop(conn_b);
                        drop(peer_a);
                        drop(peer_b);

                        let end_virtual = ts.now_nanos();
                        total_virtual_time +=
                            Duration::from_nanos(end_virtual.saturating_sub(start_virtual));

                        if !sent_ok || received_len == 0 {
                            eprintln!(
                                "rtt_{} failed: sent={}, received={}",
                                rtt_ms, sent_ok, received_len
                            );
                        }

                        auto_advance.abort();
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
///
/// Congestion control algorithm is configurable via `FREENET_CONGESTION_ALGO` env var.
#[allow(dead_code)]
pub fn bench_high_bandwidth_throughput(c: &mut Criterion) {
    // Use multi-threaded runtime to allow packet delivery to proceed concurrently
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("slow_start/high_bandwidth");
    group.sample_size(10);
    group.throughput(Throughput::Bytes(1048576)); // 1MB

    let bandwidth_limit_mb = 100;

    group.bench_function(
        format!("1mb_transfer_{}mb_limit", bandwidth_limit_mb),
        |b| {
            b.to_async(&rt).iter_custom(|iters| {
                async move {
                    let mut total_virtual_time = Duration::ZERO;

                    for _ in 0..iters {
                        // Create fresh VirtualTime for each iteration
                        let ts = VirtualTime::new();
                        let auto_advance = spawn_auto_advance_task(ts.clone());

                        let delay = Duration::from_millis(10); // 10ms RTT

                        // Create peers with VirtualTime and configurable congestion control
                        let mut peers =
                            create_connected_peers_with_configurable_congestion(delay, ts.clone())
                                .await;

                        let message = vec![0xABu8; 1024 * 1024]; // 1MB
                        let start_virtual = ts.now_nanos();

                        if let Err(e) = peers.conn_a.send(message).await {
                            eprintln!("high_bandwidth send failed: {:?}", e);
                            auto_advance.abort();
                            continue;
                        }
                        let received: Vec<u8> = match peers.conn_b.recv().await {
                            Ok(r) => r,
                            Err(e) => {
                                eprintln!("high_bandwidth recv failed: {:?}", e);
                                auto_advance.abort();
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
                        auto_advance.abort();
                    }

                    total_virtual_time
                }
            });
        },
    );

    group.finish();
}
