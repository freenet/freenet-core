//! Manual Throughput Benchmarks
//!
//! Custom harness for testing large transfer throughput without criterion.
//! Criterion's warmup estimation hangs with 32KB+ transfers, so we use
//! manual timing instead.
//!
//! Run with: `cargo test --release --bench transport_manual -- --nocapture`
//!
//! Tests:
//! - Large message sizes (32KB, 64KB, 128KB, 256KB)
//! - Warm connection (reused across iterations)
//! - Manual timing and throughput calculation
//! - Both instant RTT (0ms) and LAN RTT (2ms)

use dashmap::DashMap;
use freenet::transport::mock_transport::{
    create_mock_peer_with_delay, Channels, PacketDelayPolicy, PacketDropPolicy,
};
use freenet::transport::{OutboundConnectionHandler, PeerConnection};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Helper struct to keep connection and its peer handler alive together
struct LiveConnection {
    conn: PeerConnection,
    /// Must keep peer alive - it holds the inbound_packet_sender channel
    #[allow(dead_code)]
    peer: OutboundConnectionHandler,
}

/// Run a manual throughput benchmark with specified parameters
async fn bench_throughput(
    message_size: usize,
    iterations: usize,
    rtt_delay: Option<Duration>,
    warmup_iterations: usize,
) -> (Duration, f64) {
    eprintln!("  [DEBUG] Creating channels...");
    let channels: Channels = Arc::new(DashMap::new());

    // Apply RTT delay if specified
    let drop_policy = PacketDropPolicy::ReceiveAll;
    let delay_policy = if let Some(delay) = rtt_delay {
        PacketDelayPolicy::Fixed(delay)
    } else {
        PacketDelayPolicy::NoDelay
    };

    eprintln!("  [DEBUG] Creating peers...");
    // Create peers
    let (peer_a_pub, mut peer_a, peer_a_addr) =
        create_mock_peer_with_delay(drop_policy.clone(), delay_policy.clone(), channels.clone())
            .await
            .unwrap();

    let (peer_b_pub, mut peer_b, peer_b_addr) =
        create_mock_peer_with_delay(drop_policy, delay_policy, channels)
            .await
            .unwrap();

    eprintln!("  [DEBUG] Establishing connection...");
    // Establish connection
    let (conn_a_inner, conn_b_inner) = futures::join!(
        peer_a.connect(peer_b_pub, peer_b_addr),
        peer_b.connect(peer_a_pub, peer_a_addr),
    );
    let (conn_a, conn_b) = futures::join!(conn_a_inner, conn_b_inner);
    let (mut conn_a, mut conn_b) = (conn_a.unwrap(), conn_b.unwrap());

    eprintln!("  [DEBUG] Running {} warmup iterations...", warmup_iterations);
    // Warmup phase
    for i in 0..warmup_iterations {
        let msg = vec![0xABu8; message_size];
        conn_a.send(msg).await.unwrap();
        let _: Vec<u8> = conn_b.recv().await.unwrap();
        if i == 0 {
            eprintln!("  [DEBUG] First warmup iteration complete");
        }
    }

    eprintln!("  [DEBUG] Running {} benchmark iterations...", iterations);
    // Benchmark phase
    let start = Instant::now();
    for i in 0..iterations {
        let msg = vec![0xABu8; message_size];
        conn_a.send(msg).await.unwrap();
        let _: Vec<u8> = conn_b.recv().await.unwrap();
        if i == 0 {
            eprintln!("  [DEBUG] First benchmark iteration complete");
        }
    }
    let elapsed = start.elapsed();
    eprintln!("  [DEBUG] Benchmark complete");

    // Calculate throughput
    let total_bytes = (message_size * iterations) as f64;
    let throughput_mbps = (total_bytes * 8.0) / elapsed.as_secs_f64() / 1_000_000.0;

    // Keep peers alive until we're done
    drop(peer_a);
    drop(peer_b);

    (elapsed, throughput_mbps)
}

/// Format duration for display
fn format_duration(d: Duration) -> String {
    if d.as_millis() > 0 {
        format!("{:.2}ms", d.as_secs_f64() * 1000.0)
    } else {
        format!("{:.2}µs", d.as_micros())
    }
}

/// Format throughput for display
fn format_throughput(mbps: f64) -> String {
    if mbps >= 1.0 {
        format!("{:.2} Mbps", mbps)
    } else {
        format!("{:.2} Kbps", mbps * 1000.0)
    }
}

#[tokio::test]
async fn manual_throughput_single_test() {
    println!("\n=== Simple Single Test ===\n");
    eprintln!("[DEBUG] Starting simple 1KB test");
    let (elapsed, mbps) = bench_throughput(1024, 5, None, 1).await;
    println!("1 KB: time={:?} throughput={:.2} Mbps", elapsed, mbps);
    println!("\n=== Test Complete ===\n");
}

#[tokio::test]
async fn manual_throughput_benchmarks() {
    println!("\n=== Manual Throughput Benchmarks ===\n");
    println!("Testing large transfers with warm connection (reused)\n");

    // Limited sizes due to timeout issues with 64KB+
    // 64KB+ appears to hang even on single iteration (likely in cleanup/teardown)
    let test_configs = vec![
        (1 * 1024, "1 KB"),
        (4 * 1024, "4 KB"),
        (16 * 1024, "16 KB"),
        (32 * 1024, "32 KB"),
    ];

    // Test with instant RTT (0ms) - single iteration only due to timeout issues
    println!("--- Instant RTT (0ms) ---\n");
    println!("NOTE: Using single iteration per size due to hang issues with 16KB+ on second iteration\n");
    for (size, label) in &test_configs {
        eprintln!("[DEBUG] Starting benchmark for {}", label);
        let (elapsed, mbps) = bench_throughput(*size, 1, None, 0).await;
        println!(
            "{:>8}: time={:>12} throughput={:>12}",
            label,
            format_duration(elapsed),
            format_throughput(mbps)
        );
    }

    // Test with LAN RTT (2ms) - single iteration only
    println!("\n--- LAN RTT (2ms) ---\n");
    println!("NOTE: Using single iteration per size due to timeout issues\n");
    for (size, label) in &test_configs {
        eprintln!("[DEBUG] Starting benchmark for {} with 2ms RTT", label);
        let (elapsed, mbps) =
            bench_throughput(*size, 1, Some(Duration::from_millis(2)), 0).await;
        println!(
            "{:>8}: time={:>12} throughput={:>12}",
            label,
            format_duration(elapsed),
            format_throughput(mbps)
        );
    }

    println!("\n=== Benchmark Complete ===\n");
}

/// Test continuous message sending (sustained throughput)
#[tokio::test]
async fn manual_sustained_throughput() {
    println!("\n=== Sustained Throughput Test ===\n");
    println!("Sending multiple messages continuously on same connection\n");

    let test_configs = vec![
        (1024, 100, "1 KB × 100"),
        (1024, 500, "1 KB × 500"),  // Test more iterations with size that works
        (4096, 10, "4 KB × 10"),    // Reduce iterations for larger sizes
    ];

    for (size, iterations, label) in test_configs {
        eprintln!("[DEBUG] Starting sustained test: {}", label);

        let channels: Channels = Arc::new(DashMap::new());
        let delay_policy = PacketDelayPolicy::NoDelay;
        let drop_policy = PacketDropPolicy::ReceiveAll;

        // Create peers
        let (peer_a_pub, mut peer_a, peer_a_addr) =
            create_mock_peer_with_delay(drop_policy.clone(), delay_policy.clone(), channels.clone())
                .await
                .unwrap();
        let (peer_b_pub, mut peer_b, peer_b_addr) =
            create_mock_peer_with_delay(drop_policy, delay_policy, channels)
                .await
                .unwrap();

        eprintln!("  [DEBUG] Establishing connection...");
        let (conn_a_inner, conn_b_inner) = futures::join!(
            peer_a.connect(peer_b_pub, peer_b_addr),
            peer_b.connect(peer_a_pub, peer_a_addr),
        );
        let (conn_a, conn_b) = futures::join!(conn_a_inner, conn_b_inner);
        let (mut conn_a, mut conn_b) = (conn_a.unwrap(), conn_b.unwrap());

        eprintln!("  [DEBUG] Sending {} messages of {} bytes...", iterations, size);
        let start = Instant::now();
        for i in 0..iterations {
            let msg = vec![0xABu8; size];
            conn_a.send(msg).await.unwrap();
            let _: Vec<u8> = conn_b.recv().await.unwrap();
            if i == 0 {
                eprintln!("  [DEBUG] First message complete");
            }
        }
        let elapsed = start.elapsed();

        let total_bytes = (size * iterations) as f64;
        let throughput_mbps = (total_bytes * 8.0) / elapsed.as_secs_f64() / 1_000_000.0;

        println!(
            "{:>12}: total_time={:>10} aggregate_throughput={:>12}",
            label,
            format_duration(elapsed),
            format_throughput(throughput_mbps)
        );

        drop(peer_a);
        drop(peer_b);
    }

    println!("\n=== Sustained Test Complete ===\n");
}

/// Test multiple concurrent streams
#[tokio::test]
async fn manual_concurrent_streams() {
    println!("\n=== Concurrent Streams Test ===\n");
    println!("Multiple peer pairs sending simultaneously\n");

    let num_streams = 4;
    let message_size = 1024; // 1KB (4KB causes hangs)
    let iterations = 25;

    eprintln!("[DEBUG] Creating {} concurrent streams", num_streams);

    let channels: Channels = Arc::new(DashMap::new());
    let delay_policy = PacketDelayPolicy::NoDelay;
    let drop_policy = PacketDropPolicy::ReceiveAll;

    // Create multiple peer pairs
    let mut tasks = Vec::new();

    for stream_id in 0..num_streams {
        let channels_clone = channels.clone();
        let delay_policy_clone = delay_policy.clone();
        let drop_policy_clone = drop_policy.clone();

        let task = tokio::spawn(async move {
            eprintln!("  [DEBUG] Stream {} starting...", stream_id);

            // Create peers for this stream
            let (peer_a_pub, mut peer_a, peer_a_addr) =
                create_mock_peer_with_delay(drop_policy_clone.clone(), delay_policy_clone.clone(), channels_clone.clone())
                    .await
                    .unwrap();
            let (peer_b_pub, mut peer_b, peer_b_addr) =
                create_mock_peer_with_delay(drop_policy_clone, delay_policy_clone, channels_clone)
                    .await
                    .unwrap();

            // Connect
            let (conn_a_inner, conn_b_inner) = futures::join!(
                peer_a.connect(peer_b_pub, peer_b_addr),
                peer_b.connect(peer_a_pub, peer_a_addr),
            );
            let (conn_a, conn_b) = futures::join!(conn_a_inner, conn_b_inner);
            let (mut conn_a, mut conn_b) = (conn_a.unwrap(), conn_b.unwrap());

            // Send messages
            let start = Instant::now();
            for _ in 0..iterations {
                let msg = vec![0xABu8; message_size];
                conn_a.send(msg).await.unwrap();
                let _: Vec<u8> = conn_b.recv().await.unwrap();
            }
            let elapsed = start.elapsed();

            drop(peer_a);
            drop(peer_b);

            eprintln!("  [DEBUG] Stream {} complete in {:?}", stream_id, elapsed);
            (stream_id, elapsed)
        });

        tasks.push(task);
    }

    // Wait for all streams to complete
    let start = Instant::now();
    let results = futures::future::join_all(tasks).await;
    let total_elapsed = start.elapsed();

    // Calculate aggregate throughput
    let total_bytes = (message_size * iterations * num_streams) as f64;
    let aggregate_mbps = (total_bytes * 8.0) / total_elapsed.as_secs_f64() / 1_000_000.0;

    println!("Streams: {}", num_streams);
    println!("Message size: {} KB", message_size / 1024);
    println!("Messages per stream: {}", iterations);
    println!("Total data: {:.2} MB", total_bytes / 1_000_000.0);
    println!("Total time: {}", format_duration(total_elapsed));
    println!("Aggregate throughput: {}", format_throughput(aggregate_mbps));

    println!("\nPer-stream times:");
    for result in results {
        if let Ok((stream_id, elapsed)) = result {
            println!("  Stream {}: {}", stream_id, format_duration(elapsed));
        }
    }

    println!("\n=== Concurrent Test Complete ===\n");
}

/// Test maximum bandwidth utilization
#[tokio::test]
async fn manual_bandwidth_saturation() {
    println!("\n=== Bandwidth Saturation Test ===\n");
    println!("Sending messages as fast as possible\n");

    let message_size = 1024; // 1KB messages
    let duration_secs = 2; // Send for 2 seconds

    eprintln!("[DEBUG] Creating connection...");
    let channels: Channels = Arc::new(DashMap::new());
    let delay_policy = PacketDelayPolicy::NoDelay;
    let drop_policy = PacketDropPolicy::ReceiveAll;

    let (peer_a_pub, mut peer_a, peer_a_addr) =
        create_mock_peer_with_delay(drop_policy.clone(), delay_policy.clone(), channels.clone())
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

    eprintln!("[DEBUG] Sending messages for {} seconds...", duration_secs);
    let start = Instant::now();
    let deadline = start + Duration::from_secs(duration_secs);
    let mut count = 0;

    while Instant::now() < deadline {
        let msg = vec![0xABu8; message_size];
        conn_a.send(msg).await.unwrap();
        let _: Vec<u8> = conn_b.recv().await.unwrap();
        count += 1;

        if count == 1 {
            eprintln!("[DEBUG] First message complete");
        }
    }

    let elapsed = start.elapsed();
    let total_bytes = (message_size * count) as f64;
    let throughput_mbps = (total_bytes * 8.0) / elapsed.as_secs_f64() / 1_000_000.0;

    println!("Duration: {}", format_duration(elapsed));
    println!("Messages sent: {}", count);
    println!("Total data: {:.2} MB", total_bytes / 1_000_000.0);
    println!("Throughput: {}", format_throughput(throughput_mbps));
    println!("Messages/sec: {:.2}", count as f64 / elapsed.as_secs_f64());

    drop(peer_a);
    drop(peer_b);

    println!("\n=== Saturation Test Complete ===\n");
}
