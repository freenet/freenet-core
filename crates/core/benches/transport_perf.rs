//! Transport Layer Performance Benchmarks - Minimal Noise Edition
//!
//! This module provides noise-minimized benchmarks for the Freenet transport layer.
//!
//! ## Noise Reduction Strategies
//!
//! 1. **No tracing**: Compiled out via `--no-default-features`
//! 2. **Pre-allocation**: All buffers allocated before measurement
//! 3. **iter_batched**: Setup separated from hot path
//! 4. **No async in Level 0**: Pure sync code, no runtime overhead
//! 5. **Pinned data**: Reuse buffers to avoid allocation during measurement
//!
//! ## Understanding Benchmark Levels
//!
//! | Level | Measures | Use Case |
//! |-------|----------|----------|
//! | 0 | Absolute cost | "How fast is AES-GCM?" |
//! | 1 | Comparative cost | "Did my change improve throughput?" |
//! | 2 | Syscall overhead | "What's my kernel bottleneck?" |
//! | 3 | System limits | "What's max throughput?" |
//!
//! ### Level 1: Comparative/Differential Measurement
//!
//! Level 1 includes consistent tokio/OS overhead, but this is **bias not noise**.
//! When comparing before/after code changes, the overhead cancels out:
//!
//! ```text
//! Before: 150μs = your_code(X) + tokio_overhead
//! After:  120μs = your_code(Y) + tokio_overhead
//! Delta:  -30μs = improvement from code change
//! ```
//!
//! Use Level 1 for:
//! - A/B testing code changes
//! - Regression detection in CI
//! - Protocol optimization validation
//!
//! ## Running
//!
//! ```bash
//! # Run without tracing feature (critical for noise-free results)
//! cargo bench --bench transport_perf --no-default-features --features "redb,websocket"
//!
//! # Or use the helper script
//! ./scripts/run_benchmarks.sh level0 level1
//!
//! # Compare before/after a change
//! git stash && ./scripts/run_benchmarks.sh level1  # baseline
//! git stash pop && ./scripts/run_benchmarks.sh level1  # with change
//! # Criterion automatically compares to previous run
//! ```
//!
//! ## Benchmark Levels
//!
//! - **Level 0**: Pure computation, zero I/O, zero async, zero allocation in hot path
//! - **Level 1**: Mock I/O - measures code changes through consistent overhead
//! - **Level 2**: Loopback sockets - includes kernel scheduling variance
//! - **Level 3**: Stress tests - highly environment-dependent

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use std::hint::black_box as std_black_box;
use std::time::Duration;

// =============================================================================
// Configuration
// =============================================================================

/// Payload sizes to benchmark (bytes)
const PAYLOAD_SIZES: &[usize] = &[
    64,   // Tiny message
    256,  // Small message
    1024, // 1KB
    1364, // Max single packet (after overhead)
];

/// Large payload sizes for streaming tests
#[allow(dead_code)] // Reserved for future streaming benchmarks
const STREAM_SIZES: &[usize] = &[
    4096,  // 4KB - 3 packets
    16384, // 16KB - ~12 packets
    65536, // 64KB - ~48 packets
];

// =============================================================================
// Level 0: Pure Logic - ZERO noise path
// =============================================================================
//
// These benchmarks have:
// - No async runtime
// - No tracing (even disabled tracing has macro overhead)
// - No allocation in hot path
// - Pre-computed inputs
// - Deterministic operations

mod level0_pure_logic {
    use super::*;
    use aes_gcm::{
        aead::{AeadInPlace, KeyInit},
        Aes128Gcm,
    };

    /// AES-GCM encryption - the core crypto operation
    ///
    /// This measures ONLY the AES-GCM encrypt operation with:
    /// - Pre-allocated buffer (reused)
    /// - Pre-computed key and nonce
    /// - No allocation, no branching, no logging
    pub fn bench_aes_gcm_encrypt(c: &mut Criterion) {
        let mut group = c.benchmark_group("level0/crypto/encrypt");

        // Pre-compute cipher once
        let key: [u8; 16] = [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f,
        ];
        let cipher = Aes128Gcm::new(&key.into());

        // Fixed nonce (in real code this would be random, but we're measuring encrypt speed)
        let nonce: [u8; 12] = [0u8; 12];

        for &size in PAYLOAD_SIZES {
            group.throughput(Throughput::Bytes(size as u64));

            // Pre-allocate buffer with extra space for in-place encryption
            let mut buffer = vec![0xABu8; size];

            group.bench_with_input(BenchmarkId::new("aes128gcm", size), &size, |b, &_size| {
                b.iter(|| {
                    // Reset buffer (minimal overhead - just memset)
                    buffer.fill(0xAB);

                    // The actual operation we're measuring
                    let tag = cipher
                        .encrypt_in_place_detached(
                            (&nonce).into(),
                            &[], // No AAD
                            &mut buffer,
                        )
                        .expect("encryption failed");

                    // Prevent dead code elimination without adding overhead
                    std_black_box(&tag);
                    std_black_box(&buffer);
                });
            });
        }

        group.finish();
    }

    /// AES-GCM decryption
    pub fn bench_aes_gcm_decrypt(c: &mut Criterion) {
        let mut group = c.benchmark_group("level0/crypto/decrypt");

        let key: [u8; 16] = [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f,
        ];
        let cipher = Aes128Gcm::new(&key.into());
        let nonce: [u8; 12] = [0u8; 12];

        for &size in PAYLOAD_SIZES {
            group.throughput(Throughput::Bytes(size as u64));

            // Pre-encrypt data to create valid ciphertext
            let mut plaintext = vec![0xABu8; size];
            let tag = cipher
                .encrypt_in_place_detached((&nonce).into(), &[], &mut plaintext)
                .unwrap();
            let ciphertext_template = plaintext.clone();

            // Working buffer for decryption
            let mut buffer = vec![0u8; size];

            group.bench_with_input(BenchmarkId::new("aes128gcm", size), &size, |b, &_size| {
                b.iter(|| {
                    // Copy ciphertext (simulates receiving packet)
                    buffer.copy_from_slice(&ciphertext_template);

                    // The actual operation
                    cipher
                        .decrypt_in_place_detached((&nonce).into(), &[], &mut buffer, &tag)
                        .expect("decryption failed");

                    std_black_box(&buffer);
                });
            });
        }

        group.finish();
    }

    /// Nonce generation comparison: random vs counter
    ///
    /// Counter-based nonces are much faster and equally secure for our use case
    pub fn bench_nonce_generation(c: &mut Criterion) {
        use std::sync::atomic::{AtomicU64, Ordering};

        let mut group = c.benchmark_group("level0/crypto/nonce");

        // Random nonce generation
        group.bench_function("random", |b| {
            b.iter(|| {
                let nonce: [u8; 12] = rand::random();
                std_black_box(nonce);
            });
        });

        // Counter-based nonce (much faster)
        let counter = AtomicU64::new(0);
        group.bench_function("counter", |b| {
            b.iter(|| {
                let val = counter.fetch_add(1, Ordering::Relaxed);
                let mut nonce = [0u8; 12];
                nonce[..8].copy_from_slice(&val.to_le_bytes());
                std_black_box(nonce);
            });
        });

        // Counter with additional entropy (connection ID in upper bytes)
        group.bench_function("counter_with_connid", |b| {
            let conn_id: u32 = 0x12345678;
            b.iter(|| {
                let val = counter.fetch_add(1, Ordering::Relaxed);
                let mut nonce = [0u8; 12];
                nonce[..8].copy_from_slice(&val.to_le_bytes());
                nonce[8..].copy_from_slice(&conn_id.to_le_bytes());
                std_black_box(nonce);
            });
        });

        group.finish();
    }

    /// Bincode serialization - measures pure serialization overhead
    pub fn bench_serialization(c: &mut Criterion) {
        use serde::{Deserialize, Serialize};

        // Simplified message structure matching SymmetricMessage
        #[derive(Serialize, Deserialize, Clone)]
        struct BenchMessage {
            packet_id: u32,
            confirm_receipt: Vec<u32>,
            payload_type: u8, // Discriminant
            payload: Vec<u8>,
        }

        let mut group = c.benchmark_group("level0/serialization");

        for &size in PAYLOAD_SIZES {
            group.throughput(Throughput::Bytes(size as u64));

            // Pre-create message
            let msg = BenchMessage {
                packet_id: 12345,
                confirm_receipt: vec![1, 2, 3, 4, 5],
                payload_type: 1,
                payload: vec![0xABu8; size],
            };

            // Pre-allocate output buffer
            let mut output_buf = vec![0u8; size + 100]; // Extra for headers

            group.bench_with_input(BenchmarkId::new("serialize", size), &msg, |b, msg| {
                b.iter(|| {
                    // Serialize into pre-allocated buffer
                    let written = bincode::serialize_into(output_buf.as_mut_slice(), msg).is_ok();
                    std_black_box(written);
                    std_black_box(&output_buf);
                });
            });

            // Pre-serialize for deserialization bench
            let serialized = bincode::serialize(&msg).unwrap();

            group.bench_with_input(
                BenchmarkId::new("deserialize", size),
                &serialized,
                |b, data| {
                    b.iter(|| {
                        let msg: BenchMessage = bincode::deserialize(data).unwrap();
                        std_black_box(msg);
                    });
                },
            );
        }

        group.finish();
    }

    /// Combined encrypt + serialize (full packet creation path)
    pub fn bench_packet_creation(c: &mut Criterion) {
        use serde::{Deserialize, Serialize};

        #[derive(Serialize, Deserialize)]
        struct BenchMessage {
            packet_id: u32,
            confirm_receipt: Vec<u32>,
            payload: Vec<u8>,
        }

        let mut group = c.benchmark_group("level0/packet_creation");

        let key: [u8; 16] = [0x42u8; 16];
        let cipher = Aes128Gcm::new(&key.into());
        let nonce: [u8; 12] = [0u8; 12];

        for &size in PAYLOAD_SIZES {
            group.throughput(Throughput::Bytes(size as u64));

            let msg = BenchMessage {
                packet_id: 12345,
                confirm_receipt: vec![1, 2, 3],
                payload: vec![0xABu8; size],
            };

            // Max packet buffer
            let mut packet_buf = vec![0u8; 1500];

            group.bench_with_input(
                BenchmarkId::new("serialize_then_encrypt", size),
                &msg,
                |b, msg| {
                    b.iter(|| {
                        // Step 1: Serialize
                        let serialized_len = bincode::serialized_size(msg).unwrap() as usize;
                        bincode::serialize_into(&mut packet_buf[..serialized_len], msg).unwrap();

                        // Step 2: Encrypt in place
                        let tag = cipher
                            .encrypt_in_place_detached(
                                (&nonce).into(),
                                &[],
                                &mut packet_buf[..serialized_len],
                            )
                            .unwrap();

                        std_black_box(&tag);
                        std_black_box(&packet_buf);
                    });
                },
            );
        }

        group.finish();
    }

    /// Memory copy overhead (baseline for packet handling)
    pub fn bench_memcpy(c: &mut Criterion) {
        let mut group = c.benchmark_group("level0/memcpy");

        for &size in &[64, 256, 1024, 1464, 4096] {
            group.throughput(Throughput::Bytes(size as u64));

            let src = vec![0xABu8; size];
            let mut dst = vec![0u8; size];

            group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &_size| {
                b.iter(|| {
                    dst.copy_from_slice(&src);
                    std_black_box(&dst);
                });
            });
        }

        group.finish();
    }
}

// =============================================================================
// Level 1: Mock I/O - Protocol logic without syscalls
// =============================================================================
//
// Uses tokio channels to simulate network I/O without actual syscalls.
// Measures: async overhead, channel throughput, protocol state machines

mod level1_mock_io {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    /// Channel throughput - critical path for packet routing
    ///
    /// Tests different buffer sizes to find optimal configuration
    pub fn bench_channel_throughput(c: &mut Criterion) {
        // Create runtime ONCE, outside benchmark
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        let mut group = c.benchmark_group("level1/channel/throughput");
        group.throughput(Throughput::Elements(1000));

        // Test various buffer sizes
        for buffer_size in [1, 10, 100, 1000] {
            // Pre-create packet data outside the benchmark loop
            let packet: Arc<[u8]> = vec![0u8; 1492].into();

            group.bench_with_input(
                BenchmarkId::new("buffer", buffer_size),
                &buffer_size,
                |b, &buf_size| {
                    let packet = packet.clone();
                    b.to_async(&rt).iter_batched(
                        // Setup: create channel and clone packet (not measured)
                        || {
                            let p = packet.clone();
                            (mpsc::channel::<Arc<[u8]>>(buf_size), p)
                        },
                        // Routine: send 1000 packets (measured)
                        |((tx, mut rx), packet)| async move {
                            // Spawn receiver
                            let receiver = tokio::spawn(async move {
                                let mut count = 0;
                                while rx.recv().await.is_some() {
                                    count += 1;
                                }
                                count
                            });

                            // Send packets
                            for _ in 0..1000 {
                                tx.send(packet.clone()).await.unwrap();
                            }
                            drop(tx); // Close channel

                            let count = receiver.await.unwrap();
                            std_black_box(count);
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }

        group.finish();
    }

    /// try_send vs send - measures backpressure overhead
    pub fn bench_channel_try_send(c: &mut Criterion) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        let mut group = c.benchmark_group("level1/channel/try_send");

        // Large buffer to avoid backpressure
        let buffer_size = 10000;
        let packet: Arc<[u8]> = vec![0u8; 1492].into();

        group.bench_function("try_send_success", |b| {
            let packet = packet.clone();
            b.to_async(&rt).iter_batched(
                || {
                    let (tx, rx) = mpsc::channel::<Arc<[u8]>>(buffer_size);
                    (tx, rx, packet.clone())
                },
                |(tx, _rx, packet)| async move {
                    for _ in 0..1000 {
                        let _ = tx.try_send(packet.clone());
                    }
                },
                BatchSize::SmallInput,
            );
        });

        group.finish();
    }

    /// Simulated packet routing (what connection_handler does)
    pub fn bench_packet_routing(c: &mut Criterion) {
        use std::collections::HashMap;
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        let mut group = c.benchmark_group("level1/routing");

        // Simulate routing to N peers
        for num_peers in [10, 100, 1000] {
            group.bench_with_input(BenchmarkId::new("peers", num_peers), &num_peers, |b, &n| {
                b.to_async(&rt).iter_batched(
                    || {
                        // Setup: create routing table with N peers
                        let mut routes: HashMap<SocketAddr, mpsc::Sender<Arc<[u8]>>> =
                            HashMap::with_capacity(n);
                        let mut receivers = Vec::with_capacity(n);

                        for i in 0..n {
                            let addr = SocketAddr::new(
                                IpAddr::V4(Ipv4Addr::new(10, 0, (i / 256) as u8, (i % 256) as u8)),
                                8000 + (i as u16),
                            );
                            let (tx, rx) = mpsc::channel(100);
                            routes.insert(addr, tx);
                            receivers.push(rx);
                        }

                        // Target address (middle of range)
                        let target = SocketAddr::new(
                            IpAddr::V4(Ipv4Addr::new(
                                10,
                                0,
                                ((n / 2) / 256) as u8,
                                ((n / 2) % 256) as u8,
                            )),
                            8000 + (n / 2) as u16,
                        );

                        (routes, receivers, target)
                    },
                    |(routes, _receivers, target)| async move {
                        let packet: Arc<[u8]> = vec![0u8; 1492].into();

                        // Route 100 packets
                        for _ in 0..100 {
                            if let Some(tx) = routes.get(&target) {
                                let _ = tx.try_send(packet.clone());
                            }
                        }
                    },
                    BatchSize::SmallInput,
                );
            });
        }

        group.finish();
    }
}

// =============================================================================
// Level 2: Loopback - Real sockets, kernel involved
// =============================================================================
//
// WARNING: Results vary significantly based on:
// - Kernel version and configuration
// - CPU frequency scaling
// - Other system load
// - Socket buffer sizes

mod level2_loopback {
    use super::*;

    /// Raw UDP syscall overhead
    pub fn bench_udp_syscall(c: &mut Criterion) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        let mut group = c.benchmark_group("level2/udp/syscall");

        // Pre-create socket outside the benchmark
        let socket = rt.block_on(async {
            let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let addr = socket.local_addr().unwrap();
            socket.connect(addr).await.unwrap();
            socket
        });
        let socket = std::sync::Arc::new(socket);

        // Single send syscall
        let socket_clone = socket.clone();
        group.bench_function("send_1400b", |b| {
            let socket = socket_clone.clone();
            b.to_async(&rt).iter(|| {
                let socket = socket.clone();
                async move {
                    let buf = [0u8; 1400];
                    socket.send(&buf).await.unwrap();
                }
            });
        });

        // Send + recv roundtrip
        group.bench_function("roundtrip_1400b", |b| {
            let socket = socket.clone();
            b.to_async(&rt).iter(|| {
                let socket = socket.clone();
                async move {
                    let send_buf = [0u8; 1400];
                    let mut recv_buf = [0u8; 1500];
                    socket.send(&send_buf).await.unwrap();
                    socket.recv(&mut recv_buf).await.unwrap();
                }
            });
        });

        group.finish();
    }

    /// Burst send performance (no batching, sequential sends)
    pub fn bench_udp_burst(c: &mut Criterion) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        let mut group = c.benchmark_group("level2/udp/burst");

        // Pre-create socket
        let socket = rt.block_on(async {
            let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let addr = socket.local_addr().unwrap();
            socket.connect(addr).await.unwrap();
            socket
        });
        let socket = std::sync::Arc::new(socket);

        for count in [10, 100, 1000] {
            group.throughput(Throughput::Elements(count as u64));

            let socket = socket.clone();
            group.bench_with_input(BenchmarkId::new("packets", count), &count, |b, &n| {
                let socket = socket.clone();
                b.to_async(&rt).iter(move || {
                    let socket = socket.clone();
                    async move {
                        let buf = [0u8; 1400];
                        for _ in 0..n {
                            socket.send(&buf).await.unwrap();
                        }
                    }
                });
            });
        }

        group.finish();
    }
}

// =============================================================================
// Level 3: Stress Tests
// =============================================================================

mod level3_stress {
    use super::*;

    /// Maximum sustainable send rate
    pub fn bench_max_send_rate(c: &mut Criterion) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        let mut group = c.benchmark_group("level3/stress/send_rate");
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(10));

        // Pre-create socket
        let socket = rt.block_on(async {
            let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let addr = socket.local_addr().unwrap();
            socket.connect(addr).await.unwrap();
            socket
        });
        let socket = std::sync::Arc::new(socket);

        group.bench_function("10k_packets", |b| {
            let socket = socket.clone();
            b.to_async(&rt).iter(|| {
                let socket = socket.clone();
                async move {
                    let buf = [0u8; 1400];
                    for _ in 0..10_000 {
                        let _ = socket.send(&buf).await;
                    }
                }
            });
        });

        group.finish();
    }
}

// =============================================================================
// Criterion Groups - Organized by noise level
// =============================================================================

criterion_group!(
    name = level0;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(3))
        .noise_threshold(0.02)  // 2% - should be rock stable
        .significance_level(0.01);
    targets =
        level0_pure_logic::bench_aes_gcm_encrypt,
        level0_pure_logic::bench_aes_gcm_decrypt,
        level0_pure_logic::bench_nonce_generation,
        level0_pure_logic::bench_serialization,
        level0_pure_logic::bench_packet_creation,
        level0_pure_logic::bench_memcpy,
);

criterion_group!(
    name = level1;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(5))
        .noise_threshold(0.05)  // 5% - async adds some variance
        .significance_level(0.01);
    targets =
        level1_mock_io::bench_channel_throughput,
        level1_mock_io::bench_channel_try_send,
        level1_mock_io::bench_packet_routing,
);

criterion_group!(
    name = level2;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(10))
        .noise_threshold(0.10)  // 10% - kernel scheduling
        .significance_level(0.05);
    targets =
        level2_loopback::bench_udp_syscall,
        level2_loopback::bench_udp_burst,
);

criterion_group!(
    name = level3;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(15))
        .sample_size(10)
        .noise_threshold(0.15);  // 15% - high variance expected
    targets =
        level3_stress::bench_max_send_rate,
);

criterion_main!(level0, level1, level2, level3);
