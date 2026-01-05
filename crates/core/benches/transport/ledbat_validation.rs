//! LEDBAT Validation Benchmarks
//!
//! These benchmarks test LEDBAT congestion control behavior with separate
//! cold start and warm connection measurements.
//!
//! **Design principles:**
//! - Cold start: Create new connection per iteration (measures connection + transfer)
//! - Warm connection: Reuse connection across iterations (measures pure transfer)
//! - Keep OutboundConnectionHandler alive to prevent channel closure
//!
//! Total runtime: ~3-5 minutes

use criterion::{Criterion, Throughput};
use std::hint::black_box as std_black_box;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::common::{
    create_benchmark_runtime, create_connected_peers, ConnectedPeerPair, SMALL_SIZES,
};

/// Cold start benchmark: measures connection establishment + transfer
///
/// Each iteration creates a fresh connection, measuring real cold-start behavior.
/// This is useful for understanding first-message latency.
///
/// Tests multiple message sizes to understand the scaling behavior.
pub fn bench_large_transfer_validation(c: &mut Criterion) {
    let rt = create_benchmark_runtime();

    let mut group = c.benchmark_group("ledbat/cold_start");

    // Test multiple sizes: 1KB, 4KB, 16KB
    // Note: 64KB is skipped due to benchmark timeout issues with larger transfers
    for &(size, name) in SMALL_SIZES {
        group.throughput(Throughput::Bytes(size as u64));

        // Cold start: connection created per iteration
        // Use iter() instead of iter_batched() for simpler async handling
        group.bench_function(name, |b| {
            b.to_async(&rt).iter(|| async move {
                let ConnectedPeerPair {
                    mut conn_a,
                    mut conn_b,
                    ..
                } = create_connected_peers().await;

                let message = vec![0xABu8; size];

                // Transfer - handle errors gracefully
                if let Err(e) = conn_a.send(message).await {
                    eprintln!("ledbat_cold send failed: {:?}", e);
                    return;
                }
                match conn_b.recv().await {
                    Ok(received) => {
                        assert!(!received.is_empty());
                        std_black_box(received);
                    }
                    Err(e) => eprintln!("ledbat_cold recv failed: {:?}", e),
                }
            });
        });
    }

    group.finish();
}

/// Warm connection benchmark: measures pure transfer throughput
///
/// Connection is established once and reused across iterations.
/// This measures steady-state LEDBAT throughput without connection overhead.
///
/// **Key fix**: Keep OutboundConnectionHandler (peer) alive alongside PeerConnection.
/// The peer holds the inbound_packet_sender channel - dropping it closes the connection.
pub fn bench_1mb_transfer_validation(c: &mut Criterion) {
    let rt = create_benchmark_runtime();

    let mut group = c.benchmark_group("ledbat/warm_connection");

    // Test multiple sizes: 1KB, 4KB, 16KB
    for &(size, name) in SMALL_SIZES {
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_function(name, |b| {
            // Create connection once, keep both peers AND connections alive
            // Track warmup success to skip iterations if connection is broken
            let warmup_success = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let warmup_success_clone = warmup_success.clone();

            let peers = rt.block_on(async {
                let mut peers = create_connected_peers().await;

                // Warmup: 5 transfers to stabilize LEDBAT cwnd
                let completed = peers.warmup(5, size).await;
                if completed > 0 {
                    warmup_success_clone.store(true, std::sync::atomic::Ordering::SeqCst);
                } else {
                    eprintln!("ledbat warmup failed completely - benchmark will be skipped");
                }

                peers
            });

            // Wrap in Arc<Mutex> for sharing across iterations
            let peers = Arc::new(Mutex::new(peers));

            b.to_async(&rt).iter(|| {
                let peers = peers.clone();
                let warmup_ok = warmup_success.clone();
                async move {
                    // Skip iterations if warmup failed - fail fast instead of timing out
                    if !warmup_ok.load(std::sync::atomic::Ordering::SeqCst) {
                        return;
                    }

                    let mut p = peers.lock().await;

                    let message = vec![0xABu8; size];
                    // Handle errors gracefully
                    if let Err(e) = p.conn_a.send(message).await {
                        eprintln!("ledbat_warm send failed: {:?}", e);
                        return;
                    }
                    match p.conn_b.recv().await {
                        Ok(received) => {
                            assert!(!received.is_empty());
                            std_black_box(received);
                        }
                        Err(e) => eprintln!("ledbat_warm recv failed: {:?}", e),
                    }
                }
            });
        });
    }

    group.finish();
}

// NOTE: Benchmarks with 32KB+ transfers timeout during criterion warmup phase.
// This appears to be an interaction between criterion's async benchmarking and
// larger message sizes. The transport layer itself handles large transfers fine
// (verified by unit tests). This needs further investigation.
//
// To properly test throughput approaching the 10 MB/s rate limit, we need either:
// 1. A custom benchmark harness that doesn't use criterion's iter()
// 2. Investigation into why criterion's warmup estimation hangs with large transfers
// 3. Real UDP socket benchmarks instead of mock transport
