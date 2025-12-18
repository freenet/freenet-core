//! LEDBAT Validation Benchmarks
//!
//! Manual benchmarks for validating LEDBAT congestion control behavior.
//! Validates:
//! - Cold start vs warm connection throughput (1KB, 4KB, 16KB)
//! - Connection reuse speedup (warm connection should be 5-25x faster)
//!
//! Run with: `cargo bench --bench transport_ledbat --features bench`
//!
//! **Expected runtime: ~3-5 minutes**
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
        bench_large_transfer_validation,  // Cold start: 1KB, 4KB, 16KB
        bench_1mb_transfer_validation,    // Warm connection: 1KB, 4KB, 16KB
);

// Main entry point - LEDBAT validation benchmarks
criterion_main!(ledbat_validation);
