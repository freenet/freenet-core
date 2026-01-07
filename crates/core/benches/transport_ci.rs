//! CI-Optimized Transport Benchmarks with VirtualTime
//!
//! Streamlined benchmark suite focused on what matters: throughput and critical paths.
//! Uses VirtualTime for instant execution - completes in ~1-2 minutes.
//!
//! Run with: `cargo bench --bench transport_ci`
//!
//! This subset includes:
//! - cold_throughput: Cold-start connection + transfer throughput
//! - warm_throughput: Sustained bulk transfer with LEDBAT warmup
//! - connection_setup: Cold-start connection establishment
//! - streaming_buffer: Lock-free buffer operations (critical path)
//!
//! What this DOESN'T include (moved to transport_extended):
//! - Micro-benchmarks (AES, serialization, allocation) - don't correlate with throughput
//! - High-latency scenarios - for nightly/transport-change only
//! - Packet loss scenarios - for nightly/transport-change only
//!
//! For comprehensive testing: `cargo bench --bench transport_extended`

use criterion::{criterion_group, criterion_main, Criterion};
use std::time::Duration;

mod transport;

// Import benchmark functions
use transport::blackbox::*;
use transport::slow_start::{bench_cold_start_throughput, bench_warm_connection_throughput};
use transport::streaming_buffer::*;

// =============================================================================
// CI Benchmark Groups - VirtualTime for Instant Execution
// =============================================================================

// Cold-start throughput - measures connection establishment + transfer
//
// Each iteration: connect → measured transfer
// With VirtualTime, completes instantly while tracking virtual elapsed time.
criterion_group!(
    name = cold_throughput_ci;
    config = Criterion::default()
        .sample_size(10)
        .noise_threshold(0.20)
        .significance_level(0.05);
    targets = bench_cold_start_throughput
);

// Warm connection throughput - measures sustained transfer after LEDBAT warmup
//
// Each iteration: connect → warmup (3 transfers) → measured transfer
// With VirtualTime, warmup completes instantly.
criterion_group!(
    name = warm_throughput_ci;
    config = Criterion::default()
        .sample_size(10)
        .noise_threshold(0.20)
        .significance_level(0.05);
    targets = bench_warm_connection_throughput
);

// Connection establishment - measures cold-start handshake time
//
// With VirtualTime, handshakes complete instantly while tracking virtual elapsed time.
criterion_group!(
    name = connection_setup_ci;
    config = Criterion::default()
        .sample_size(10)
        .noise_threshold(0.15)
        .significance_level(0.05);
    targets = bench_connection_establishment
);

// Streaming buffer operations - critical path for message reassembly
//
// Lock-free buffer is in the hot path for all streaming transfers.
// These are pure CPU benchmarks, no VirtualTime needed.
criterion_group!(
    name = streaming_buffer_ci;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(3))
        .noise_threshold(0.03)
        .significance_level(0.01);
    targets =
        bench_sequential_insert,
        bench_assemble,
        bench_first_fragment_latency,
);

// Main entry point - streamlined CI suite with VirtualTime
//
// Total execution time: ~1-2 minutes (vs ~5+ minutes with real time)
criterion_main!(
    cold_throughput_ci,  // PRIMARY: cold connection + transfer throughput
    warm_throughput_ci,  // PRIMARY: warmed connection sustained throughput
    connection_setup_ci, // Cold-start matters for UX
    streaming_buffer_ci, // Critical path component (CPU-only, no VirtualTime)
);
