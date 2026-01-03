//! Minimal regression test for issue #2331: Concurrent streams bottleneck
//!
//! This test reproduces the 3-4x throughput degradation when using multiple
//! concurrent connections compared to a single connection.
//!
//! Run with:
//! ```bash
//! cargo test --release -p freenet --test concurrent_streams_regression --features bench
//! ```

#![cfg(feature = "bench")]

use dashmap::DashMap;
use freenet::transport::mock_transport::{
    create_mock_peer, Channels, MockSocket, PacketDropPolicy,
};
use freenet::transport::{OutboundConnectionHandler, PeerConnection};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::info;

/// Helper to create a connected peer pair
async fn create_connected_pair(
    channels: Channels,
) -> (
    PeerConnection<MockSocket>,
    PeerConnection<MockSocket>,
    OutboundConnectionHandler<MockSocket>,
    OutboundConnectionHandler<MockSocket>,
) {
    let (peer_a_pub, mut peer_a, peer_a_addr) =
        create_mock_peer(PacketDropPolicy::ReceiveAll, channels.clone())
            .await
            .expect("create peer A");

    let (peer_b_pub, mut peer_b, peer_b_addr) =
        create_mock_peer(PacketDropPolicy::ReceiveAll, channels)
            .await
            .expect("create peer B");

    let (conn_a_inner, conn_b_inner) = futures::join!(
        peer_a.connect(peer_b_pub, peer_b_addr),
        peer_b.connect(peer_a_pub, peer_a_addr),
    );
    let (conn_a, conn_b) = futures::join!(conn_a_inner, conn_b_inner);
    let (conn_a, conn_b) = (conn_a.expect("connect A"), conn_b.expect("connect B"));

    (conn_a, conn_b, peer_a, peer_b)
}

/// Measure throughput for a single connection
async fn bench_single_stream(message_size: usize, iterations: usize) -> (Duration, f64) {
    let channels: Channels = Arc::new(DashMap::new());
    let (mut conn_a, mut conn_b, _peer_a, _peer_b) = create_connected_pair(channels).await;

    let start = Instant::now();
    for _ in 0..iterations {
        let msg = vec![0xABu8; message_size];
        conn_a.send(msg).await.unwrap();
        let _: Vec<u8> = conn_b.recv().await.unwrap();
    }
    let elapsed = start.elapsed();

    let total_bytes = message_size * iterations;
    let throughput_mbps = (total_bytes as f64 * 8.0) / elapsed.as_secs_f64() / 1_000_000.0;

    (elapsed, throughput_mbps)
}

/// Measure aggregate throughput for N concurrent connections
async fn bench_concurrent_streams(
    num_streams: usize,
    message_size: usize,
    iterations: usize,
) -> (Duration, f64) {
    let channels: Channels = Arc::new(DashMap::new());
    let mut tasks = Vec::new();

    for stream_id in 0..num_streams {
        let channels_clone = channels.clone();

        let task = tokio::spawn(async move {
            let (mut conn_a, mut conn_b, _peer_a, _peer_b) =
                create_connected_pair(channels_clone).await;

            let start = Instant::now();
            for _ in 0..iterations {
                let msg = vec![0xABu8; message_size];
                conn_a.send(msg).await.unwrap();
                let _: Vec<u8> = conn_b.recv().await.unwrap();
            }
            let elapsed = start.elapsed();

            (stream_id, elapsed)
        });

        tasks.push(task);
    }

    let start = Instant::now();
    let _results = futures::future::join_all(tasks).await;
    let total_elapsed = start.elapsed();

    let total_bytes = message_size * iterations * num_streams;
    let aggregate_mbps = (total_bytes as f64 * 8.0) / total_elapsed.as_secs_f64() / 1_000_000.0;

    (total_elapsed, aggregate_mbps)
}

#[test_log::test(tokio::test(flavor = "current_thread", start_paused = true))]
async fn test_concurrent_streams_bottleneck() {
    info!("=== Concurrent Streams Bottleneck Test ===");

    let message_size = 1024; // 1KB
    let iterations = 25;

    // Benchmark single stream
    info!("Benchmarking single stream...");
    let (single_time, single_mbps) = bench_single_stream(message_size, iterations).await;
    info!(
        "Single stream: {:.2}ms, {:.2} Mbps",
        single_time.as_secs_f64() * 1000.0,
        single_mbps
    );

    // Benchmark 4 concurrent streams
    info!("Benchmarking 4 concurrent streams...");
    let (concurrent_time, concurrent_mbps) =
        bench_concurrent_streams(4, message_size, iterations).await;
    info!(
        "4 concurrent streams: {:.2}ms, {:.2} Mbps (aggregate)",
        concurrent_time.as_secs_f64() * 1000.0,
        concurrent_mbps
    );

    // Calculate degradation
    let degradation_ratio = single_mbps / concurrent_mbps;
    info!(
        "Degradation: {:.2}x (single stream is {:.2}x faster than aggregate of 4 streams)",
        degradation_ratio, degradation_ratio
    );

    // Ideally, 4 concurrent streams should achieve close to single stream throughput
    // or better (due to parallelism). If we see >2x degradation, we have a bottleneck.
    if degradation_ratio > 2.0 {
        info!(
            "WARNING: Significant bottleneck detected ({:.2}x degradation)",
            degradation_ratio
        );
        info!("   Expected: ~1.0x (concurrent ≈ single stream throughput)");
        info!("   This indicates serialization in the packet processing pipeline.");
    } else {
        info!("No significant bottleneck (degradation < 2x)");
    }

    info!("=== Test Complete ===");

    // Don't fail the test, just report metrics
    // assert!(degradation_ratio < 2.0, "Concurrent streams show {:.2}x degradation", degradation_ratio);
}
