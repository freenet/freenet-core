//! LEDBAT Validation Benchmarks
//!
//! Manual benchmarks for validating LEDBAT congestion control behavior.
//! Validates:
//! - Large transfer performance (64KB-256KB with µs delays)
//! - Cold start vs warm connection throughput
//! - cwnd evolution over time
//!
//! Run with: `cargo bench --bench transport_ledbat --features bench`
//!
//! **Expected runtime: ~5-8 minutes** (was 15+ minutes with ms delays)
//!
//! Use when:
//! - Testing congestion control changes
//! - Validating slow start behavior
//! - Measuring cold-start vs steady-state performance

use criterion::{criterion_group, criterion_main, Criterion};
use std::time::Duration;

mod transport;

// Import benchmark functions
use transport::ledbat_validation::*;
use transport::slow_start::*;

// =============================================================================
// LEDBAT Validation Groups
// =============================================================================

criterion_group!(
    name = ledbat_validation;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(10))
        .noise_threshold(0.15)
        .significance_level(0.05);
    targets =
        bench_large_transfer_validation,
        bench_1mb_transfer_validation,
        bench_congestion_256kb,
);

criterion_group!(
    name = slow_start;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(10))
        .noise_threshold(0.15)
        .significance_level(0.05);
    targets =
        bench_cold_start_throughput,
        bench_warm_connection_throughput,
        bench_cwnd_evolution,
        // bench_high_bandwidth_throughput is #[allow(dead_code)] due to OOM
);

// Main entry point - LEDBAT validation benchmarks
criterion_main!(ledbat_validation, slow_start);
