//! CI-Optimized Transport Benchmarks
//!
//! Fast, deterministic benchmarks suitable for CI regression detection.
//! Total execution time: ~8 minutes
//!
//! Run with: `cargo bench --bench transport_ci`
//!
//! This subset includes:
//! - level0: Pure logic (crypto, serialization) - deterministic, <2% noise
//! - level1: Mock I/O (channel throughput) - ~5% noise from async
//! - transport: Full transport pipeline with mock sockets - ~10% noise
//!
//! Not included (too slow or noisy for CI):
//! - level2/level3: Real sockets, kernel-dependent
//! - streaming: Long measurement times (10s per benchmark)
//! - ledbat_validation: Large transfers (256KB, 1MB)
//! - slow_start: Very long (30s measurement time)

use criterion::{criterion_group, criterion_main, Criterion};
use std::time::Duration;

mod transport;

// Import benchmark functions
use transport::blackbox::*;
use transport::level0::*;
use transport::level1::*;

// =============================================================================
// CI Benchmark Groups
// =============================================================================

criterion_group!(
    name = level0_ci;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(3))
        .noise_threshold(0.02)  // 2% - should be rock stable
        .significance_level(0.01);
    targets =
        bench_aes_gcm_encrypt,
        bench_aes_gcm_decrypt,
        bench_nonce_generation,
        bench_serialization,
        bench_packet_creation,
);

criterion_group!(
    name = level1_ci;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(5))
        .noise_threshold(0.05)  // 5% - async adds some variance
        .significance_level(0.01);
    targets =
        bench_channel_throughput,
);

criterion_group!(
    name = transport_ci;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(10))
        .noise_threshold(0.10)  // 10% - some async variance expected
        .significance_level(0.05);
    targets =
        bench_connection_establishment,
        bench_message_throughput,
);

// Main entry point - only CI-friendly benchmarks
criterion_main!(level0_ci, level1_ci, transport_ci);
