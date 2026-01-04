//! CI-Optimized Transport Benchmarks
//!
//! Streamlined benchmark suite focused on what matters: throughput and critical paths.
//! Total execution time: ~5 minutes
//!
//! Run with: `cargo bench --bench transport_ci`
//!
//! This subset includes:
//! - warm_throughput: Sustained bulk transfer (what you care about!)
//! - connection_setup: Cold-start connection establishment
//! - streaming_buffer: Lock-free buffer operations (critical path)
//!
//! What this DOESN'T include (moved to transport_extended):
//! - Micro-benchmarks (AES, serialization, allocation) - don't correlate with throughput
//! - High-latency scenarios - too slow for every PR
//! - Packet loss scenarios - for nightly/transport-change only
//!
//! For comprehensive testing: `cargo bench --bench transport_extended`

use criterion::{criterion_group, criterion_main, Criterion};
use std::time::Duration;

mod transport;

// Import benchmark functions
use transport::blackbox::*;
use transport::slow_start::bench_warm_connection_throughput;
use transport::streaming_buffer::*;

// =============================================================================
// CI Benchmark Groups - Streamlined for What Matters
// =============================================================================

/// Warm connection throughput - measures sustained bulk transfer performance
///
/// This is the PRIMARY metric for max theoretical throughput.
/// Measures MB/s over warm connection with realistic transfer sizes.
criterion_group!(
    name = warm_throughput_ci;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(3))
        .measurement_time(Duration::from_secs(15))  // Longer for stability
        .noise_threshold(0.20)  // 20% - realistic for async on shared runners
        .significance_level(0.05);
    targets = bench_warm_connection_throughput
);

/// Connection establishment - measures cold-start handshake time
///
/// Important for user experience (how long to connect to new peer).
criterion_group!(
    name = connection_setup_ci;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(10))
        .noise_threshold(0.15)  // 15% - connection setup has async variance
        .significance_level(0.05);
    targets = bench_connection_establishment
);

/// Streaming buffer operations - critical path for message reassembly
///
/// Lock-free buffer is in the hot path for all streaming transfers.
/// Should be rock-stable (deterministic operations).
criterion_group!(
    name = streaming_buffer_ci;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(3))
        .noise_threshold(0.03)  // 3% - slightly relaxed from 2%
        .significance_level(0.01);
    targets =
        bench_sequential_insert,
        bench_assemble,
        bench_first_fragment_latency,
);

// Main entry point - streamlined CI suite
//
// Focus on what matters:
// 1. Sustained throughput (what users care about)
// 2. Connection setup (user experience)
// 3. Critical path components (streaming buffer)
//
// Removed from CI (moved to transport_extended):
// - Micro-benchmarks (allocation, crypto, serialization)
// - Component tests (channels, packet creation)
// These don't correlate with real-world throughput
criterion_main!(
    warm_throughput_ci,  // PRIMARY: sustained bulk transfer
    connection_setup_ci, // Cold-start matters for UX
    streaming_buffer_ci, // Critical path component
);
