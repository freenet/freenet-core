//! Transport Layer Performance Benchmarks
//!
//! This module provides comprehensive benchmarks for the Freenet transport layer.
//! Run with: `cargo bench -p freenet --bench transport_perf`
//!
//! ## Benchmark Levels (from least to most OS-dependent)
//!
//! - **Level 0 (Pure Logic)**: No I/O, measures raw computation (encryption, serialization)
//!   - Completely deterministic, unaffected by OS scheduling
//!   - Run anywhere, including CI
//!
//! - **Level 1 (Mock I/O)**: In-process channels, no syscalls
//!   - Measures protocol overhead without kernel involvement
//!   - Requires multi-threading but no network stack
//!
//! - **Level 2 (Loopback)**: Real sockets on 127.0.0.1
//!   - Includes syscall overhead but no NIC
//!   - Affected by kernel scheduling
//!
//! - **Level 3 (Full Stack)**: Real network interfaces
//!   - Complete measurement including hardware
//!   - Requires controlled environment
//!
//! ## Running Specific Levels
//!
//! ```bash
//! # Level 0 only (CI-safe)
//! cargo bench --bench transport_perf -- "level0/"
//!
//! # Level 1 only (CI-safe)
//! cargo bench --bench transport_perf -- "level1/"
//!
//! # All levels (requires isolated environment)
//! cargo bench --bench transport_perf
//! ```

use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};
use std::time::Duration;

// =============================================================================
// Test Configuration
// =============================================================================

/// Standard message sizes for throughput tests
const MESSAGE_SIZES: &[usize] = &[
    64,           // Tiny: control messages
    512,          // Small: typical requests
    1364,         // Max single packet payload
    4096,         // Small stream: 3 packets
    16384,        // Medium stream: 12 packets
    65536,        // Large stream: ~48 packets
    1048576,      // Very large: 1MB
];

/// Packet counts for batch tests
const PACKET_COUNTS: &[usize] = &[1, 10, 100, 1000];

// =============================================================================
// Level 0: Pure Logic (No I/O, No OS interaction)
// =============================================================================
// These benchmarks measure raw computational overhead with ZERO OS involvement.
// Results are completely deterministic and reproducible across any environment.

mod level0_pure_logic {
    use super::*;
    use aes_gcm::{aead::AeadInPlace, Aes128Gcm, KeyInit};
    use rand::Rng;

    /// Benchmark AES-GCM encryption throughput
    pub fn bench_aes_gcm_encrypt(c: &mut Criterion) {
        let mut group = c.benchmark_group("crypto/aes_gcm_encrypt");

        let key = rand::random::<[u8; 16]>();
        let cipher = Aes128Gcm::new(&key.into());

        for size in MESSAGE_SIZES.iter().filter(|&&s| s <= 1464) {
            group.throughput(Throughput::Bytes(*size as u64));
            group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
                let mut buffer = vec![0u8; size + 16]; // +16 for tag
                let nonce: [u8; 12] = rand::random();

                b.iter(|| {
                    buffer[..size].fill(0xAB);
                    let tag = cipher
                        .encrypt_in_place_detached(&nonce.into(), &[], &mut buffer[..size])
                        .unwrap();
                    black_box(tag);
                });
            });
        }

        group.finish();
    }

    /// Benchmark AES-GCM decryption throughput
    pub fn bench_aes_gcm_decrypt(c: &mut Criterion) {
        let mut group = c.benchmark_group("crypto/aes_gcm_decrypt");

        let key = rand::random::<[u8; 16]>();
        let cipher = Aes128Gcm::new(&key.into());

        for size in MESSAGE_SIZES.iter().filter(|&&s| s <= 1464) {
            group.throughput(Throughput::Bytes(*size as u64));
            group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
                // Pre-encrypt data
                let mut plaintext = vec![0xAB_u8; size];
                let nonce: [u8; 12] = rand::random();
                let tag = cipher
                    .encrypt_in_place_detached(&nonce.into(), &[], &mut plaintext)
                    .unwrap();

                let mut ciphertext = plaintext.clone();

                b.iter(|| {
                    ciphertext.copy_from_slice(&plaintext);
                    cipher
                        .decrypt_in_place_detached(&nonce.into(), &[], &mut ciphertext, &tag)
                        .unwrap();
                    black_box(&ciphertext);
                });
            });
        }

        group.finish();
    }

    /// Benchmark nonce generation: random vs counter
    pub fn bench_nonce_generation(c: &mut Criterion) {
        let mut group = c.benchmark_group("crypto/nonce_generation");

        group.bench_function("random_12bytes", |b| {
            b.iter(|| {
                let nonce: [u8; 12] = rand::random();
                black_box(nonce);
            });
        });

        group.bench_function("counter_12bytes", |b| {
            let mut counter: u128 = 0;
            b.iter(|| {
                counter = counter.wrapping_add(1);
                let nonce: [u8; 12] = counter.to_le_bytes()[..12].try_into().unwrap();
                black_box(nonce);
            });
        });

        group.finish();
    }

    /// Benchmark bincode serialization
    pub fn bench_serialization(c: &mut Criterion) {
        use serde::{Deserialize, Serialize};

        #[derive(Serialize, Deserialize)]
        struct TestMessage {
            packet_id: u32,
            confirm_receipt: Vec<u32>,
            payload: Vec<u8>,
        }

        let mut group = c.benchmark_group("serialization/bincode");

        for size in MESSAGE_SIZES.iter().filter(|&&s| s <= 1364) {
            group.throughput(Throughput::Bytes(*size as u64));

            let msg = TestMessage {
                packet_id: 12345,
                confirm_receipt: vec![1, 2, 3, 4, 5],
                payload: vec![0u8; *size],
            };

            group.bench_with_input(
                BenchmarkId::new("serialize", size),
                &msg,
                |b, msg| {
                    b.iter(|| {
                        let bytes = bincode::serialize(msg).unwrap();
                        black_box(bytes);
                    });
                },
            );

            let serialized = bincode::serialize(&msg).unwrap();
            group.bench_with_input(
                BenchmarkId::new("deserialize", size),
                &serialized,
                |b, bytes| {
                    b.iter(|| {
                        let msg: TestMessage = bincode::deserialize(bytes).unwrap();
                        black_box(msg);
                    });
                },
            );
        }

        group.finish();
    }
}

// =============================================================================
// Level 1: Mock I/O (In-process channels, no syscalls)
// =============================================================================
// These benchmarks use in-process channels instead of real sockets.
// They measure protocol overhead without kernel involvement.
// Affected by: thread scheduling, memory allocation
// NOT affected by: syscalls, network stack, NIC

mod level1_mock_io {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    /// Benchmark mpsc channel throughput (critical path component)
    pub fn bench_mpsc_channel(c: &mut Criterion) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut group = c.benchmark_group("channel/mpsc");

        for buffer_size in [1, 10, 100, 1000] {
            group.throughput(Throughput::Elements(1000));
            group.bench_with_input(
                BenchmarkId::new("buffer", buffer_size),
                &buffer_size,
                |b, &buffer_size| {
                    b.to_async(&rt).iter(|| async move {
                        let (tx, mut rx) = mpsc::channel::<Arc<[u8]>>(buffer_size);
                        let packet: Arc<[u8]> = vec![0u8; 1492].into();

                        let sender = tokio::spawn({
                            let tx = tx.clone();
                            let packet = packet.clone();
                            async move {
                                for _ in 0..1000 {
                                    tx.send(packet.clone()).await.unwrap();
                                }
                                drop(tx);
                            }
                        });

                        let receiver = tokio::spawn(async move {
                            let mut count = 0;
                            while rx.recv().await.is_some() {
                                count += 1;
                            }
                            count
                        });

                        drop(tx);
                        sender.await.unwrap();
                        let count = receiver.await.unwrap();
                        black_box(count);
                    });
                },
            );
        }

        group.finish();
    }

    /// Benchmark syscall overhead simulation
    /// This measures the cost of individual socket operations
    pub fn bench_syscall_overhead(c: &mut Criterion) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut group = c.benchmark_group("io/syscall_overhead");

        // Benchmark UDP socket bind
        group.bench_function("udp_bind", |b| {
            b.to_async(&rt).iter(|| async {
                let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
                black_box(socket);
            });
        });

        // Benchmark loopback send/recv
        group.bench_function("loopback_roundtrip", |b| {
            b.to_async(&rt).iter_custom(|iters| async move {
                let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
                let addr = socket.local_addr().unwrap();
                socket.connect(addr).await.unwrap();

                let send_buf = [0u8; 1400];
                let mut recv_buf = [0u8; 1500];

                let start = std::time::Instant::now();
                for _ in 0..iters {
                    socket.send(&send_buf).await.unwrap();
                    socket.recv(&mut recv_buf).await.unwrap();
                }
                start.elapsed()
            });
        });

        group.finish();
    }
}

// =============================================================================
// Level 2: Loopback (Real sockets, no NIC)
// =============================================================================
// These benchmarks use real UDP sockets on the loopback interface.
// They measure syscall overhead and kernel network stack performance.
// Affected by: syscalls, kernel scheduling, socket buffers
// NOT affected by: NIC, physical network

mod level2_loopback {
    use super::*;

    /// Placeholder for full transport layer throughput benchmark
    /// Requires actual transport module integration
    pub fn bench_transport_throughput(c: &mut Criterion) {
        let mut group = c.benchmark_group("e2e/throughput");
        group.measurement_time(Duration::from_secs(10));

        // TODO: Integrate with actual transport layer
        // This requires setting up two PeerConnections and measuring
        // actual throughput through the full stack

        for size in MESSAGE_SIZES {
            group.throughput(Throughput::Bytes(*size as u64));
            group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &_size| {
                b.iter(|| {
                    // Placeholder - replace with actual transport test
                    black_box(42);
                });
            });
        }

        group.finish();
    }

    /// Placeholder for latency distribution benchmark
    pub fn bench_latency_distribution(c: &mut Criterion) {
        let mut group = c.benchmark_group("e2e/latency");
        group.measurement_time(Duration::from_secs(30));
        group.sample_size(500);

        // TODO: Integrate with actual transport layer
        // Measure RTT for small messages through full stack

        group.bench_function("small_message_rtt", |b| {
            b.iter(|| {
                // Placeholder - replace with actual transport test
                black_box(42);
            });
        });

        group.finish();
    }
}

// =============================================================================
// Level 3: Stress Tests (System limits, real conditions)
// =============================================================================
// These benchmarks push the system to find limits and measure behavior under load.
// Results are highly dependent on hardware and environment configuration.
// For meaningful results: use isolated CPUs, disable frequency scaling.

mod level3_stress {
    use super::*;

    /// Benchmark maximum sustainable packet rate
    pub fn bench_max_pps(c: &mut Criterion) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut group = c.benchmark_group("stress/max_pps");
        group.measurement_time(Duration::from_secs(15));
        group.sample_size(10);

        for packet_count in PACKET_COUNTS {
            group.throughput(Throughput::Elements(*packet_count as u64));
            group.bench_with_input(
                BenchmarkId::from_parameter(packet_count),
                packet_count,
                |b, &count| {
                    b.to_async(&rt).iter(|| async move {
                        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
                        let addr = socket.local_addr().unwrap();
                        socket.connect(addr).await.unwrap();

                        let packet = [0u8; 1400];
                        for _ in 0..count {
                            socket.send(&packet).await.unwrap();
                        }
                        black_box(count);
                    });
                },
            );
        }

        group.finish();
    }
}

// =============================================================================
// Criterion Configuration
// =============================================================================

// Level 0: Pure computation benchmarks (CI-safe, deterministic)
criterion_group!(
    name = level0;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(3))
        .noise_threshold(0.02);  // 2% noise threshold - should be very stable
    targets =
        level0_pure_logic::bench_aes_gcm_encrypt,
        level0_pure_logic::bench_aes_gcm_decrypt,
        level0_pure_logic::bench_nonce_generation,
        level0_pure_logic::bench_serialization,
);

// Level 1: Mock I/O benchmarks (CI-safe, measures protocol logic)
criterion_group!(
    name = level1;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(5))
        .noise_threshold(0.05);  // 5% - some async scheduling variance
    targets =
        level1_mock_io::bench_mpsc_channel,
        level1_mock_io::bench_syscall_overhead,
);

// Level 2: Loopback benchmarks (requires controlled environment)
criterion_group!(
    name = level2;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(10))
        .noise_threshold(0.10);  // 10% - kernel scheduling adds variance
    targets =
        level2_loopback::bench_transport_throughput,
        level2_loopback::bench_latency_distribution,
);

// Level 3: Stress tests (requires isolated CPUs, bare metal preferred)
criterion_group!(
    name = level3;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(15))
        .sample_size(10)
        .noise_threshold(0.15);  // 15% - high variance expected
    targets =
        level3_stress::bench_max_pps,
);

criterion_main!(level0, level1, level2, level3);
