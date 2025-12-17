//! Full Transport Benchmark Suite
//!
//! Complete benchmark suite including all levels and categories.
//! This is the most comprehensive test but takes the longest (~78 minutes).
//!
//! Run with: `cargo bench --bench transport_full --features bench_full`
//!
//! Includes:
//! - level0: Pure logic benchmarks
//! - level1: Mock I/O benchmarks
//! - level2: Loopback socket benchmarks (kernel-dependent)
//! - level3: Stress tests (high variance)
//! - transport: Full transport pipeline
//! - streaming: Large message streaming
//! - ledbat_validation: LEDBAT with large transfers
//! - slow_start: Cold-start vs warm throughput

use criterion::{criterion_group, criterion_main, Criterion};
use std::time::Duration;

mod transport;

// Import benchmark functions
use transport::blackbox::*;
use transport::ledbat_validation::*;
use transport::level0::*;
use transport::level1::*;
use transport::level2::*;
use transport::level3::*;
use transport::slow_start::*;
use transport::streaming::*;

// =============================================================================
// Full Benchmark Groups
// =============================================================================

criterion_group!(
    name = level0;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(3))
        .noise_threshold(0.02)
        .significance_level(0.01);
    targets =
        bench_aes_gcm_encrypt,
        bench_aes_gcm_decrypt,
        bench_nonce_generation,
        bench_serialization,
        bench_packet_creation,
);

criterion_group!(
    name = level1;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(5))
        .noise_threshold(0.05)
        .significance_level(0.01);
    targets =
        bench_channel_throughput,
);

criterion_group!(
    name = level2;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(10))
        .noise_threshold(0.10)
        .significance_level(0.05);
    targets =
        bench_udp_syscall,
        bench_udp_burst,
);

criterion_group!(
    name = level3;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(15))
        .sample_size(10)
        .noise_threshold(0.15);
    targets =
        bench_max_send_rate,
);

criterion_group!(
    name = transport;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(10))
        .noise_threshold(0.10)
        .significance_level(0.05);
    targets =
        bench_connection_establishment,
        bench_message_throughput,
);

criterion_group!(
    name = streaming;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(10))
        .noise_threshold(0.10)
        .significance_level(0.05);
    targets =
        bench_stream_throughput,
        bench_concurrent_streams,
);

criterion_group!(
    name = ledbat_validation;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
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
        .measurement_time(Duration::from_secs(30))
        .sample_size(20)
        .noise_threshold(0.15)
        .significance_level(0.05);
    targets =
        bench_cold_start_throughput,
        bench_warm_connection_throughput,
        bench_cwnd_evolution,
);

// Main entry point - all benchmark groups
criterion_main!(
    transport,
    streaming,
    ledbat_validation,
    slow_start,
    level0,
    level1,
    level2,
    level3
);
