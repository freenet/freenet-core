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
// Experimental: Packet Size Performance
// =============================================================================
//
// Tests hypothesis: Does larger packet size improve throughput for the same
// number of syscalls? This is critical for understanding whether we should
// pursue jumbo frames or packet coalescing strategies.

mod experimental_packet_size {
    use super::*;

    /// Compare throughput for same syscall count but different packet sizes
    ///
    /// Hypothesis: Larger packets = higher throughput for same syscall overhead
    /// This tests MTU sizes from standard (1400) to jumbo frames (9000)
    pub fn bench_packet_size_throughput(c: &mut Criterion) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        let mut group = c.benchmark_group("experimental/packet_size/throughput");
        group.sample_size(50);
        group.measurement_time(Duration::from_secs(5));

        // Test different packet sizes with SAME syscall count
        // This isolates the effect of packet size on throughput
        const SYSCALL_COUNT: usize = 1000;

        // Packet sizes to test (bytes)
        // 512: Small packets
        // 1400: Standard MTU (typical internet)
        // 4096: 4KB (larger than MTU, would fragment on real network)
        // 8192: 8KB (jumbo-ish, for local/datacenter)
        // Note: On loopback, we can exceed MTU since no fragmentation occurs
        let packet_sizes = [512, 1024, 1400, 2048, 4096, 8192];

        for &packet_size in &packet_sizes {
            // Total data transferred = packet_size * SYSCALL_COUNT
            let total_bytes = (packet_size * SYSCALL_COUNT) as u64;
            group.throughput(Throughput::Bytes(total_bytes));

            let socket = rt.block_on(async {
                let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
                let addr = socket.local_addr().unwrap();
                socket.connect(addr).await.unwrap();
                socket
            });
            let socket = std::sync::Arc::new(socket);

            group.bench_with_input(
                BenchmarkId::new("size_bytes", packet_size),
                &packet_size,
                |b, &size| {
                    let socket = socket.clone();
                    let buf = vec![0xABu8; size];
                    b.to_async(&rt).iter(move || {
                        let socket = socket.clone();
                        let buf = buf.clone();
                        async move {
                            for _ in 0..SYSCALL_COUNT {
                                socket.send(&buf).await.unwrap();
                            }
                        }
                    });
                },
            );
        }

        group.finish();
    }

    /// Measure syscall overhead as a function of packet size
    ///
    /// Shows how much of the send time is syscall overhead vs data transfer
    pub fn bench_syscall_overhead_vs_size(c: &mut Criterion) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        let mut group = c.benchmark_group("experimental/packet_size/syscall_overhead");

        let packet_sizes = [64, 256, 512, 1024, 1400, 2048, 4096, 8192];

        let socket = rt.block_on(async {
            let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let addr = socket.local_addr().unwrap();
            socket.connect(addr).await.unwrap();
            socket
        });
        let socket = std::sync::Arc::new(socket);

        for &packet_size in &packet_sizes {
            // Measure time per syscall (not throughput)
            let socket = socket.clone();
            group.bench_with_input(
                BenchmarkId::new("single_send", packet_size),
                &packet_size,
                |b, &size| {
                    let socket = socket.clone();
                    let buf = vec![0xABu8; size];
                    b.to_async(&rt).iter(move || {
                        let socket = socket.clone();
                        let buf = buf.clone();
                        async move {
                            socket.send(&buf).await.unwrap();
                        }
                    });
                },
            );
        }

        group.finish();
    }

    /// Compare send+recv roundtrip for different packet sizes
    pub fn bench_roundtrip_by_size(c: &mut Criterion) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        let mut group = c.benchmark_group("experimental/packet_size/roundtrip");

        let packet_sizes = [64, 512, 1400, 4096, 8192];

        for &packet_size in &packet_sizes {
            let (socket1, socket2) = rt.block_on(async {
                let s1 = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
                let s2 = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
                let addr1 = s1.local_addr().unwrap();
                let addr2 = s2.local_addr().unwrap();
                s1.connect(addr2).await.unwrap();
                s2.connect(addr1).await.unwrap();
                (s1, s2)
            });
            let socket1 = std::sync::Arc::new(socket1);
            let socket2 = std::sync::Arc::new(socket2);

            group.bench_with_input(
                BenchmarkId::new("bytes", packet_size),
                &packet_size,
                |b, &size| {
                    let s1 = socket1.clone();
                    let s2 = socket2.clone();
                    let send_buf = vec![0xABu8; size];
                    b.to_async(&rt).iter(move || {
                        let s1 = s1.clone();
                        let s2 = s2.clone();
                        let send_buf = send_buf.clone();
                        async move {
                            let mut recv_buf = vec![0u8; size + 100];
                            s1.send(&send_buf).await.unwrap();
                            s2.recv(&mut recv_buf).await.unwrap();
                        }
                    });
                },
            );
        }

        group.finish();
    }
}

// =============================================================================
// Experimental: Tokio Overhead Comparison
// =============================================================================
//
// Tests hypothesis: Can we reduce async runtime overhead by using alternatives
// to tokio channels or by using a different threading model?

mod experimental_tokio_overhead {
    use super::*;
    use std::sync::Arc;

    /// Compare tokio::sync::mpsc vs std::sync::mpsc vs crossbeam
    ///
    /// This helps quantify the overhead of tokio's async channels
    pub fn bench_channel_comparison(c: &mut Criterion) {
        let mut group = c.benchmark_group("experimental/tokio_overhead/channels");
        group.throughput(Throughput::Elements(10000));

        let packet_size = 1492;
        let packet_count = 10000;

        // ========== std::sync::mpsc (blocking) ==========
        group.bench_function("std_mpsc", |b| {
            b.iter_batched(
                || {
                    let (tx, rx) = std::sync::mpsc::channel::<Arc<[u8]>>();
                    let packet: Arc<[u8]> = vec![0u8; packet_size].into();
                    (tx, rx, packet)
                },
                |(tx, rx, packet)| {
                    let receiver = std::thread::spawn(move || {
                        let mut count = 0;
                        while rx.recv().is_ok() {
                            count += 1;
                        }
                        count
                    });

                    for _ in 0..packet_count {
                        tx.send(packet.clone()).unwrap();
                    }
                    drop(tx);

                    let count = receiver.join().unwrap();
                    std_black_box(count);
                },
                BatchSize::SmallInput,
            );
        });

        // ========== std::sync::mpsc with sync_channel (bounded) ==========
        group.bench_function("std_sync_channel_100", |b| {
            b.iter_batched(
                || {
                    let (tx, rx) = std::sync::mpsc::sync_channel::<Arc<[u8]>>(100);
                    let packet: Arc<[u8]> = vec![0u8; packet_size].into();
                    (tx, rx, packet)
                },
                |(tx, rx, packet)| {
                    let receiver = std::thread::spawn(move || {
                        let mut count = 0;
                        while rx.recv().is_ok() {
                            count += 1;
                        }
                        count
                    });

                    for _ in 0..packet_count {
                        tx.send(packet.clone()).unwrap();
                    }
                    drop(tx);

                    let count = receiver.join().unwrap();
                    std_black_box(count);
                },
                BatchSize::SmallInput,
            );
        });

        // ========== crossbeam unbounded ==========
        group.bench_function("crossbeam_unbounded", |b| {
            b.iter_batched(
                || {
                    let (tx, rx) = crossbeam::channel::unbounded::<Arc<[u8]>>();
                    let packet: Arc<[u8]> = vec![0u8; packet_size].into();
                    (tx, rx, packet)
                },
                |(tx, rx, packet)| {
                    let receiver = std::thread::spawn(move || {
                        let mut count = 0;
                        while rx.recv().is_ok() {
                            count += 1;
                        }
                        count
                    });

                    for _ in 0..packet_count {
                        tx.send(packet.clone()).unwrap();
                    }
                    drop(tx);

                    let count = receiver.join().unwrap();
                    std_black_box(count);
                },
                BatchSize::SmallInput,
            );
        });

        // ========== crossbeam bounded ==========
        group.bench_function("crossbeam_bounded_100", |b| {
            b.iter_batched(
                || {
                    let (tx, rx) = crossbeam::channel::bounded::<Arc<[u8]>>(100);
                    let packet: Arc<[u8]> = vec![0u8; packet_size].into();
                    (tx, rx, packet)
                },
                |(tx, rx, packet)| {
                    let receiver = std::thread::spawn(move || {
                        let mut count = 0;
                        while rx.recv().is_ok() {
                            count += 1;
                        }
                        count
                    });

                    for _ in 0..packet_count {
                        tx.send(packet.clone()).unwrap();
                    }
                    drop(tx);

                    let count = receiver.join().unwrap();
                    std_black_box(count);
                },
                BatchSize::SmallInput,
            );
        });

        // ========== tokio::sync::mpsc (async) ==========
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        group.bench_function("tokio_mpsc_100", |b| {
            let packet: Arc<[u8]> = vec![0u8; packet_size].into();
            b.to_async(&rt).iter_batched(
                || {
                    let (tx, rx) = tokio::sync::mpsc::channel::<Arc<[u8]>>(100);
                    (tx, rx, packet.clone())
                },
                |(tx, mut rx, packet)| async move {
                    let receiver = tokio::spawn(async move {
                        let mut count = 0;
                        while rx.recv().await.is_some() {
                            count += 1;
                        }
                        count
                    });

                    for _ in 0..packet_count {
                        tx.send(packet.clone()).await.unwrap();
                    }
                    drop(tx);

                    let count = receiver.await.unwrap();
                    std_black_box(count);
                },
                BatchSize::SmallInput,
            );
        });

        // ========== tokio::sync::mpsc unbounded (async) ==========
        group.bench_function("tokio_mpsc_unbounded", |b| {
            let packet: Arc<[u8]> = vec![0u8; packet_size].into();
            b.to_async(&rt).iter_batched(
                || {
                    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Arc<[u8]>>();
                    (tx, rx, packet.clone())
                },
                |(tx, mut rx, packet)| async move {
                    let receiver = tokio::spawn(async move {
                        let mut count = 0;
                        while rx.recv().await.is_some() {
                            count += 1;
                        }
                        count
                    });

                    for _ in 0..packet_count {
                        tx.send(packet.clone()).unwrap();
                    }
                    drop(tx);

                    let count = receiver.await.unwrap();
                    std_black_box(count);
                },
                BatchSize::SmallInput,
            );
        });

        // ========== crossbeam + std::thread (dedicated thread, no tokio) ==========
        group.bench_function("crossbeam_std_thread", |b| {
            let packet: Arc<[u8]> = vec![0u8; packet_size].into();
            b.iter_batched(
                || {
                    let (tx, rx) = crossbeam::channel::unbounded::<Arc<[u8]>>();
                    (tx, rx, packet.clone())
                },
                |(tx, rx, packet)| {
                    // Dedicated thread consuming from crossbeam channel
                    let receiver = std::thread::spawn(move || {
                        let mut count = 0;
                        while rx.recv().is_ok() {
                            count += 1;
                        }
                        count
                    });

                    for _ in 0..packet_count {
                        tx.send(packet.clone()).unwrap();
                    }
                    drop(tx);

                    let count = receiver.join().unwrap();
                    std_black_box(count);
                },
                BatchSize::SmallInput,
            );
        });

        // ========== crossbeam + spawn_blocking (tokio blocking thread pool) ==========
        group.bench_function("crossbeam_spawn_blocking", |b| {
            let packet: Arc<[u8]> = vec![0u8; packet_size].into();
            b.to_async(&rt).iter_batched(
                || {
                    let (tx, rx) = crossbeam::channel::unbounded::<Arc<[u8]>>();
                    (tx, rx, packet.clone())
                },
                |(tx, rx, packet)| async move {
                    // spawn_blocking runs on tokio's blocking thread pool
                    let receiver = tokio::task::spawn_blocking(move || {
                        let mut count = 0;
                        while rx.recv().is_ok() {
                            count += 1;
                        }
                        count
                    });

                    for _ in 0..packet_count {
                        tx.send(packet.clone()).unwrap();
                    }
                    drop(tx);

                    let count = receiver.await.unwrap();
                    std_black_box(count);
                },
                BatchSize::SmallInput,
            );
        });

        group.finish();
    }

    /// Compare tokio UdpSocket vs std UdpSocket with dedicated threads
    ///
    /// This tests whether tokio's async I/O adds significant overhead
    pub fn bench_socket_overhead(c: &mut Criterion) {
        let mut group = c.benchmark_group("experimental/tokio_overhead/sockets");
        group.sample_size(50);

        let packet_size = 1400;
        const BURST_SIZE: usize = 1000;

        group.throughput(Throughput::Elements(BURST_SIZE as u64));

        // ========== std::net::UdpSocket (blocking, dedicated thread) ==========
        group.bench_function("std_udp_blocking", |b| {
            let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            let addr = socket.local_addr().unwrap();
            socket.connect(addr).unwrap();

            let buf = vec![0xABu8; packet_size];
            b.iter(|| {
                for _ in 0..BURST_SIZE {
                    socket.send(&buf).unwrap();
                }
            });
        });

        // ========== tokio::net::UdpSocket (async) ==========
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        let socket = rt.block_on(async {
            let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let addr = socket.local_addr().unwrap();
            socket.connect(addr).await.unwrap();
            socket
        });
        let socket = std::sync::Arc::new(socket);

        group.bench_function("tokio_udp_async", |b| {
            let socket = socket.clone();
            let buf = vec![0xABu8; packet_size];
            b.to_async(&rt).iter(move || {
                let socket = socket.clone();
                let buf = buf.clone();
                async move {
                    for _ in 0..BURST_SIZE {
                        socket.send(&buf).await.unwrap();
                    }
                }
            });
        });

        // ========== tokio current_thread runtime (single-threaded) ==========
        let rt_single = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let socket_single = rt_single.block_on(async {
            let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let addr = socket.local_addr().unwrap();
            socket.connect(addr).await.unwrap();
            socket
        });
        let socket_single = std::sync::Arc::new(socket_single);

        group.bench_function("tokio_udp_single_thread", |b| {
            let socket = socket_single.clone();
            let buf = vec![0xABu8; packet_size];
            b.to_async(&rt_single).iter(move || {
                let socket = socket.clone();
                let buf = buf.clone();
                async move {
                    for _ in 0..BURST_SIZE {
                        socket.send(&buf).await.unwrap();
                    }
                }
            });
        });

        group.finish();
    }

    /// Thread-per-core model vs tokio work-stealing
    ///
    /// Tests a simplified thread-per-core approach where each "peer" has its own
    /// dedicated thread, vs tokio's work-stealing scheduler
    pub fn bench_threading_model(c: &mut Criterion) {
        let mut group = c.benchmark_group("experimental/tokio_overhead/threading");
        group.sample_size(20);
        group.measurement_time(Duration::from_secs(10));

        let packet_size = 1400;
        let packets_per_peer = 1000;
        let num_peers = 4; // Simulate 4 concurrent peer connections

        group.throughput(Throughput::Elements((packets_per_peer * num_peers) as u64));

        // ========== Thread-per-peer model (dedicated threads) ==========
        group.bench_function("thread_per_peer", |b| {
            b.iter(|| {
                let handles: Vec<_> = (0..num_peers)
                    .map(|_| {
                        std::thread::spawn(move || {
                            let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
                            let addr = socket.local_addr().unwrap();
                            socket.connect(addr).unwrap();
                            let buf = vec![0xABu8; packet_size];

                            for _ in 0..packets_per_peer {
                                socket.send(&buf).unwrap();
                            }
                        })
                    })
                    .collect();

                for h in handles {
                    h.join().unwrap();
                }
            });
        });

        // ========== Tokio work-stealing model ==========
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        group.bench_function("tokio_work_stealing", |b| {
            b.to_async(&rt).iter(|| async {
                let handles: Vec<_> = (0..num_peers)
                    .map(|_| {
                        tokio::spawn(async move {
                            let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
                            let addr = socket.local_addr().unwrap();
                            socket.connect(addr).await.unwrap();
                            let buf = vec![0xABu8; packet_size];

                            for _ in 0..packets_per_peer {
                                socket.send(&buf).await.unwrap();
                            }
                        })
                    })
                    .collect();

                for h in handles {
                    h.await.unwrap();
                }
            });
        });

        // ========== Tokio spawn_blocking (hybrid) ==========
        group.bench_function("tokio_spawn_blocking", |b| {
            b.to_async(&rt).iter(|| async {
                let handles: Vec<_> = (0..num_peers)
                    .map(|_| {
                        tokio::task::spawn_blocking(move || {
                            let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
                            let addr = socket.local_addr().unwrap();
                            socket.connect(addr).unwrap();
                            let buf = vec![0xABu8; packet_size];

                            for _ in 0..packets_per_peer {
                                socket.send(&buf).unwrap();
                            }
                        })
                    })
                    .collect();

                for h in handles {
                    h.await.unwrap();
                }
            });
        });

        group.finish();
    }
}

// =============================================================================
// Experimental: Combined Packet Size + Channel Overhead
// =============================================================================
//
// Tests the combined effect of packet size and channel overhead to simulate
// the real transport layer behavior

mod experimental_combined {
    use super::*;
    use aes_gcm::{aead::AeadInPlace, aead::KeyInit, Aes128Gcm};
    use std::sync::Arc;

    /// Full pipeline: serialize -> encrypt -> channel -> socket
    /// Compares different packet sizes through the full pipeline
    pub fn bench_full_pipeline_by_size(c: &mut Criterion) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        let mut group = c.benchmark_group("experimental/combined/full_pipeline");
        group.sample_size(30);
        group.measurement_time(Duration::from_secs(10));

        const PACKET_COUNT: usize = 1000;

        // Test different payload sizes
        let payload_sizes = [256, 512, 1024, 1364, 2048, 4096];

        let key: [u8; 16] = [0x42u8; 16];
        let cipher = Aes128Gcm::new(&key.into());

        for &payload_size in &payload_sizes {
            let total_bytes = (payload_size * PACKET_COUNT) as u64;
            group.throughput(Throughput::Bytes(total_bytes));

            let cipher = cipher.clone();

            group.bench_with_input(
                BenchmarkId::new("payload_bytes", payload_size),
                &payload_size,
                |b, &size| {
                    let cipher = cipher.clone();
                    b.to_async(&rt).iter_batched(
                        || {
                            // Setup: create channel and socket
                            let (tx, rx) = tokio::sync::mpsc::channel::<Arc<[u8]>>(100);

                            let socket_future = async {
                                let socket =
                                    tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
                                let addr = socket.local_addr().unwrap();
                                socket.connect(addr).await.unwrap();
                                Arc::new(socket)
                            };

                            let socket = tokio::runtime::Handle::current().block_on(socket_future);

                            (tx, rx, socket, cipher.clone())
                        },
                        |(tx, mut rx, socket, cipher)| async move {
                            // Spawn receiver that sends to socket
                            let socket_clone = socket.clone();
                            let receiver = tokio::spawn(async move {
                                let mut count = 0;
                                while let Some(packet) = rx.recv().await {
                                    socket_clone.send(&packet).await.ok();
                                    count += 1;
                                }
                                count
                            });

                            // Sender: serialize, encrypt, send to channel
                            for nonce_counter in 0u64..PACKET_COUNT as u64 {
                                // Allocate buffer for packet
                                let mut packet = vec![0u8; size + 28]; // +28 for nonce+tag

                                // Create nonce
                                let mut nonce = [0u8; 12];
                                nonce[4..].copy_from_slice(&nonce_counter.to_le_bytes());

                                // Copy nonce to packet
                                packet[..12].copy_from_slice(&nonce);

                                // Fill payload with data
                                packet[12..12 + size].fill(0xAB);

                                // Encrypt in place
                                let tag = cipher
                                    .encrypt_in_place_detached(
                                        (&nonce).into(),
                                        &[],
                                        &mut packet[12..12 + size],
                                    )
                                    .unwrap();

                                // Append tag
                                packet[12 + size..12 + size + 16].copy_from_slice(&tag);

                                // Send to channel
                                let packet_arc: Arc<[u8]> = packet.into();
                                tx.send(packet_arc).await.ok();
                            }
                            drop(tx);

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
}

// =============================================================================
// Experimental: Syscall Batching Simulation
// =============================================================================
//
// Tests hypothesis: Can batching multiple packets per syscall improve throughput?
// Simulates the effect of sendmmsg by comparing:
// - Tokio async (one send per await point)
// - Blocking tight loop (simulates sendmmsg effect - no async overhead between sends)
// - Channel-based batching (collect then send)

mod experimental_syscall_batching {
    use super::*;
    use std::net::UdpSocket;
    use std::sync::Arc;

    /// Compare async vs blocking send patterns
    ///
    /// This tests the overhead of async coordination between packet sends:
    /// - Async: Each send() is an await point (potential context switch)
    /// - Blocking: Tight loop with no async overhead between sends
    pub fn bench_sendmmsg_vs_single(c: &mut Criterion) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        let mut group = c.benchmark_group("experimental/syscall_batching/async_vs_blocking");
        group.sample_size(50);

        const PACKET_SIZE: usize = 1400;
        const TOTAL_PACKETS: usize = 1000;

        let total_bytes = (PACKET_SIZE * TOTAL_PACKETS) as u64;
        group.throughput(Throughput::Bytes(total_bytes));

        // ========== Tokio async (one await per send) ==========
        let socket = rt.block_on(async {
            let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let addr = socket.local_addr().unwrap();
            socket.connect(addr).await.unwrap();
            socket
        });
        let socket = Arc::new(socket);

        group.bench_function("tokio_async_per_packet", |b| {
            let socket = socket.clone();
            let buf = vec![0xABu8; PACKET_SIZE];
            b.to_async(&rt).iter(move || {
                let socket = socket.clone();
                let buf = buf.clone();
                async move {
                    for _ in 0..TOTAL_PACKETS {
                        socket.send(&buf).await.unwrap();
                    }
                }
            });
        });

        // ========== Blocking tight loop (simulates sendmmsg effect) ==========
        group.bench_function("blocking_tight_loop", |b| {
            let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
            let addr = socket.local_addr().unwrap();
            socket.connect(addr).unwrap();
            let buf = vec![0xABu8; PACKET_SIZE];

            b.iter(|| {
                for _ in 0..TOTAL_PACKETS {
                    socket.send(&buf).unwrap();
                }
            });
        });

        // ========== spawn_blocking (tokio + blocking I/O) ==========
        group.bench_function("spawn_blocking_loop", |b| {
            b.to_async(&rt).iter(|| async {
                tokio::task::spawn_blocking(move || {
                    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
                    let addr = socket.local_addr().unwrap();
                    socket.connect(addr).unwrap();
                    let buf = vec![0xABu8; PACKET_SIZE];

                    for _ in 0..TOTAL_PACKETS {
                        socket.send(&buf).unwrap();
                    }
                })
                .await
                .unwrap();
            });
        });

        group.finish();
    }

    /// Compare different batching strategies with tokio
    ///
    /// Tests how to integrate batching with async code
    pub fn bench_tokio_with_sendmmsg(c: &mut Criterion) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        let mut group = c.benchmark_group("experimental/syscall_batching/batching_strategies");
        group.sample_size(30);

        const PACKET_SIZE: usize = 1400;
        const TOTAL_PACKETS: usize = 1000;

        let total_bytes = (PACKET_SIZE * TOTAL_PACKETS) as u64;
        group.throughput(Throughput::Bytes(total_bytes));

        // ========== Baseline: collect into Vec, then send all ==========
        group.bench_function("collect_then_send_blocking", |b| {
            b.to_async(&rt).iter(|| async {
                // Simulate collecting packets (like a channel would)
                let packets: Vec<Vec<u8>> = (0..TOTAL_PACKETS)
                    .map(|_| vec![0xABu8; PACKET_SIZE])
                    .collect();

                // Then send all in spawn_blocking
                tokio::task::spawn_blocking(move || {
                    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
                    let addr = socket.local_addr().unwrap();
                    socket.connect(addr).unwrap();

                    for packet in &packets {
                        socket.send(packet).unwrap();
                    }
                })
                .await
                .unwrap();
            });
        });

        // ========== Channel-based: collect batch, then flush ==========
        group.bench_function("channel_batch_flush", |b| {
            const BATCH_SIZE: usize = 100;

            b.to_async(&rt).iter(|| async {
                let (tx, mut rx) = tokio::sync::mpsc::channel::<Arc<[u8]>>(BATCH_SIZE);

                // Sender task
                let sender = tokio::spawn(async move {
                    let packet: Arc<[u8]> = vec![0xABu8; PACKET_SIZE].into();
                    for _ in 0..TOTAL_PACKETS {
                        tx.send(packet.clone()).await.ok();
                    }
                });

                // Receiver: batch then send
                let receiver = tokio::task::spawn_blocking(move || {
                    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
                    let addr = socket.local_addr().unwrap();
                    socket.connect(addr).unwrap();

                    // We need to use a different channel type for spawn_blocking
                    // This is a simplified simulation
                    let rt = tokio::runtime::Handle::current();
                    let mut batch = Vec::with_capacity(BATCH_SIZE);

                    // Try to receive up to BATCH_SIZE packets
                    while let Some(packet) = rt.block_on(rx.recv()) {
                        batch.push(packet);
                        // Drain available packets up to batch size
                        while batch.len() < BATCH_SIZE {
                            match rx.try_recv() {
                                Ok(p) => batch.push(p),
                                Err(_) => break,
                            }
                        }
                        // Send batch
                        for packet in batch.drain(..) {
                            socket.send(&packet).unwrap();
                        }
                    }
                });

                sender.await.unwrap();
                receiver.await.unwrap();
            });
        });

        group.finish();
    }

    /// Measure the overhead of async coordination per packet
    ///
    /// Shows how much time is spent on async machinery vs actual I/O
    pub fn bench_syscall_reduction(c: &mut Criterion) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        let mut group = c.benchmark_group("experimental/syscall_batching/overhead_breakdown");
        group.sample_size(50);

        const PACKET_SIZE: usize = 1400;

        // Test how time per packet changes with different approaches
        let packet_counts = [10, 100, 1000];

        for &count in &packet_counts {
            let total_bytes = (PACKET_SIZE * count) as u64;
            group.throughput(Throughput::Bytes(total_bytes));

            // Async
            let socket = rt.block_on(async {
                let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
                let addr = socket.local_addr().unwrap();
                socket.connect(addr).await.unwrap();
                socket
            });
            let socket = Arc::new(socket);

            group.bench_with_input(BenchmarkId::new("async", count), &count, |b, &n| {
                let socket = socket.clone();
                let buf = vec![0xABu8; PACKET_SIZE];
                b.to_async(&rt).iter(move || {
                    let socket = socket.clone();
                    let buf = buf.clone();
                    async move {
                        for _ in 0..n {
                            socket.send(&buf).await.unwrap();
                        }
                    }
                });
            });

            // Blocking
            let std_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
            let addr = std_socket.local_addr().unwrap();
            std_socket.connect(addr).unwrap();

            group.bench_with_input(BenchmarkId::new("blocking", count), &count, |b, &n| {
                let buf = vec![0xABu8; PACKET_SIZE];
                b.iter(|| {
                    for _ in 0..n {
                        std_socket.send(&buf).unwrap();
                    }
                });
            });
        }

        group.finish();
    }
}

// =============================================================================
// Experimental: True Syscall Batching with sendmmsg (Linux only)
// =============================================================================
//
// This tests ACTUAL syscall batching:
// - send(): 1 syscall per packet (N syscalls for N packets)
// - sendmmsg(): 1 syscall for N packets
//
// This is different from the async_vs_blocking test which still makes N syscalls,
// just without async overhead between them.

#[cfg(target_os = "linux")]
mod experimental_true_sendmmsg {
    use super::*;
    use std::net::{SocketAddr, UdpSocket};
    use std::os::unix::io::AsRawFd;

    /// Wrapper for sendmmsg syscall
    ///
    /// sendmmsg sends multiple messages in a single syscall, reducing
    /// kernel transitions from N to 1.
    unsafe fn sendmmsg_batch(
        fd: i32,
        messages: &mut [libc::mmsghdr],
    ) -> Result<usize, std::io::Error> {
        let ret = libc::sendmmsg(
            fd,
            messages.as_mut_ptr(),
            messages.len() as libc::c_uint,
            0, // flags
        );
        if ret < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(ret as usize)
        }
    }

    /// Compare N send() syscalls vs 1 sendmmsg() syscall
    ///
    /// This is the TRUE test of syscall batching benefit
    pub fn bench_true_sendmmsg(c: &mut Criterion) {
        let mut group = c.benchmark_group("experimental/true_sendmmsg/syscall_count");
        group.sample_size(50);

        const PACKET_SIZE: usize = 1400;

        // Test different batch sizes
        let batch_sizes = [1, 10, 50, 100, 500];

        for &batch_size in &batch_sizes {
            let total_bytes = (PACKET_SIZE * batch_size) as u64;
            group.throughput(Throughput::Bytes(total_bytes));

            // Create socket
            let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
            let addr = socket.local_addr().unwrap();
            socket.connect(addr).unwrap();
            let fd = socket.as_raw_fd();

            // Pre-allocate packet buffers
            let packets: Vec<Vec<u8>> =
                (0..batch_size).map(|_| vec![0xABu8; PACKET_SIZE]).collect();

            // ========== Baseline: N individual send() syscalls ==========
            group.bench_with_input(
                BenchmarkId::new("send_loop", batch_size),
                &batch_size,
                |b, &_n| {
                    b.iter(|| {
                        for packet in &packets {
                            socket.send(packet).unwrap();
                        }
                    });
                },
            );

            // ========== sendmmsg: 1 syscall for N packets ==========
            // Prepare sockaddr
            let sockaddr_in = match addr {
                SocketAddr::V4(v4) => {
                    let mut sa: libc::sockaddr_in = unsafe { std::mem::zeroed() };
                    sa.sin_family = libc::AF_INET as libc::sa_family_t;
                    sa.sin_port = v4.port().to_be();
                    sa.sin_addr.s_addr = u32::from_ne_bytes(v4.ip().octets());
                    sa
                }
                _ => panic!("Expected IPv4"),
            };

            group.bench_with_input(
                BenchmarkId::new("sendmmsg", batch_size),
                &batch_size,
                |b, &n| {
                    // Pre-allocate iovecs and mmsghdr structures
                    let mut iovecs: Vec<[libc::iovec; 1]> = packets
                        .iter()
                        .map(|p| {
                            [libc::iovec {
                                iov_base: p.as_ptr() as *mut libc::c_void,
                                iov_len: p.len(),
                            }]
                        })
                        .collect();

                    b.iter(|| {
                        // Build mmsghdr array
                        let mut msgs: Vec<libc::mmsghdr> = iovecs
                            .iter_mut()
                            .map(|iov| {
                                let mut msg: libc::mmsghdr = unsafe { std::mem::zeroed() };
                                msg.msg_hdr.msg_name =
                                    &sockaddr_in as *const _ as *mut libc::c_void;
                                msg.msg_hdr.msg_namelen =
                                    std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
                                msg.msg_hdr.msg_iov = iov.as_mut_ptr();
                                msg.msg_hdr.msg_iovlen = 1;
                                msg
                            })
                            .collect();

                        // Single syscall for all packets
                        let sent = unsafe { sendmmsg_batch(fd, &mut msgs) }.unwrap();
                        std_black_box(sent);
                        assert_eq!(sent, n);
                    });
                },
            );
        }

        group.finish();
    }

    /// Show throughput scaling with sendmmsg
    ///
    /// Demonstrates how throughput increases with batch size due to
    /// reduced syscall overhead
    pub fn bench_sendmmsg_throughput(c: &mut Criterion) {
        let mut group = c.benchmark_group("experimental/true_sendmmsg/throughput");
        group.sample_size(50);
        group.measurement_time(Duration::from_secs(5));

        const PACKET_SIZE: usize = 1400;
        const TOTAL_BYTES: usize = 1_400_000; // 1.4 MB total

        // Calculate packets needed for consistent total bytes
        let total_packets = TOTAL_BYTES / PACKET_SIZE;
        group.throughput(Throughput::Bytes(TOTAL_BYTES as u64));

        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = socket.local_addr().unwrap();
        socket.connect(addr).unwrap();
        let fd = socket.as_raw_fd();

        let sockaddr_in = match addr {
            SocketAddr::V4(v4) => {
                let mut sa: libc::sockaddr_in = unsafe { std::mem::zeroed() };
                sa.sin_family = libc::AF_INET as libc::sa_family_t;
                sa.sin_port = v4.port().to_be();
                sa.sin_addr.s_addr = u32::from_ne_bytes(v4.ip().octets());
                sa
            }
            _ => panic!("Expected IPv4"),
        };

        // Test different batch sizes (syscalls = total_packets / batch_size)
        let batch_sizes = [1, 10, 50, 100, 500, 1000];

        for &batch_size in &batch_sizes {
            let num_batches = total_packets / batch_size;
            let syscalls = num_batches;

            // Pre-allocate packets for one batch
            let packets: Vec<Vec<u8>> =
                (0..batch_size).map(|_| vec![0xABu8; PACKET_SIZE]).collect();

            let mut iovecs: Vec<[libc::iovec; 1]> = packets
                .iter()
                .map(|p| {
                    [libc::iovec {
                        iov_base: p.as_ptr() as *mut libc::c_void,
                        iov_len: p.len(),
                    }]
                })
                .collect();

            group.bench_with_input(
                BenchmarkId::new(
                    format!("batch_{}_syscalls_{}", batch_size, syscalls),
                    batch_size,
                ),
                &batch_size,
                |b, &_n| {
                    b.iter(|| {
                        for _ in 0..num_batches {
                            let mut msgs: Vec<libc::mmsghdr> = iovecs
                                .iter_mut()
                                .map(|iov| {
                                    let mut msg: libc::mmsghdr = unsafe { std::mem::zeroed() };
                                    msg.msg_hdr.msg_name =
                                        &sockaddr_in as *const _ as *mut libc::c_void;
                                    msg.msg_hdr.msg_namelen =
                                        std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
                                    msg.msg_hdr.msg_iov = iov.as_mut_ptr();
                                    msg.msg_hdr.msg_iovlen = 1;
                                    msg
                                })
                                .collect();

                            let sent = unsafe { sendmmsg_batch(fd, &mut msgs) }.unwrap();
                            std_black_box(sent);
                        }
                    });
                },
            );
        }

        group.finish();
    }

    /// Integration test: sendmmsg with tokio spawn_blocking
    ///
    /// Shows how to use sendmmsg from within tokio
    pub fn bench_tokio_sendmmsg_integration(c: &mut Criterion) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        let mut group = c.benchmark_group("experimental/true_sendmmsg/tokio_integration");
        group.sample_size(30);

        const PACKET_SIZE: usize = 1400;
        const TOTAL_PACKETS: usize = 1000;
        const BATCH_SIZE: usize = 100;

        let total_bytes = (PACKET_SIZE * TOTAL_PACKETS) as u64;
        group.throughput(Throughput::Bytes(total_bytes));

        // ========== Baseline: tokio async send (N syscalls) ==========
        let socket = rt.block_on(async {
            let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let addr = socket.local_addr().unwrap();
            socket.connect(addr).await.unwrap();
            socket
        });
        let socket = std::sync::Arc::new(socket);

        group.bench_function("tokio_async_send", |b| {
            let socket = socket.clone();
            let buf = vec![0xABu8; PACKET_SIZE];
            b.to_async(&rt).iter(move || {
                let socket = socket.clone();
                let buf = buf.clone();
                async move {
                    for _ in 0..TOTAL_PACKETS {
                        socket.send(&buf).await.unwrap();
                    }
                }
            });
        });

        // ========== spawn_blocking + sendmmsg (N/BATCH_SIZE syscalls) ==========
        group.bench_function("spawn_blocking_sendmmsg", |b| {
            b.to_async(&rt).iter(|| async {
                tokio::task::spawn_blocking(move || {
                    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
                    let addr = socket.local_addr().unwrap();
                    socket.connect(addr).unwrap();
                    let fd = socket.as_raw_fd();

                    let sockaddr_in = match addr {
                        SocketAddr::V4(v4) => {
                            let mut sa: libc::sockaddr_in = unsafe { std::mem::zeroed() };
                            sa.sin_family = libc::AF_INET as libc::sa_family_t;
                            sa.sin_port = v4.port().to_be();
                            sa.sin_addr.s_addr = u32::from_ne_bytes(v4.ip().octets());
                            sa
                        }
                        _ => panic!("Expected IPv4"),
                    };

                    // Pre-allocate batch
                    let packets: Vec<Vec<u8>> =
                        (0..BATCH_SIZE).map(|_| vec![0xABu8; PACKET_SIZE]).collect();

                    let mut iovecs: Vec<[libc::iovec; 1]> = packets
                        .iter()
                        .map(|p| {
                            [libc::iovec {
                                iov_base: p.as_ptr() as *mut libc::c_void,
                                iov_len: p.len(),
                            }]
                        })
                        .collect();

                    let num_batches = TOTAL_PACKETS / BATCH_SIZE;
                    for _ in 0..num_batches {
                        let mut msgs: Vec<libc::mmsghdr> = iovecs
                            .iter_mut()
                            .map(|iov| {
                                let mut msg: libc::mmsghdr = unsafe { std::mem::zeroed() };
                                msg.msg_hdr.msg_name =
                                    &sockaddr_in as *const _ as *mut libc::c_void;
                                msg.msg_hdr.msg_namelen =
                                    std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
                                msg.msg_hdr.msg_iov = iov.as_mut_ptr();
                                msg.msg_hdr.msg_iovlen = 1;
                                msg
                            })
                            .collect();

                        unsafe { sendmmsg_batch(fd, &mut msgs) }.unwrap();
                    }
                })
                .await
                .unwrap();
            });
        });

        group.finish();
    }
}

#[cfg(not(target_os = "linux"))]
mod experimental_true_sendmmsg {
    use super::*;

    pub fn bench_true_sendmmsg(_c: &mut Criterion) {
        eprintln!("sendmmsg benchmarks only available on Linux");
    }

    pub fn bench_sendmmsg_throughput(_c: &mut Criterion) {
        eprintln!("sendmmsg benchmarks only available on Linux");
    }

    pub fn bench_tokio_sendmmsg_integration(_c: &mut Criterion) {
        eprintln!("sendmmsg benchmarks only available on Linux");
    }
}

// =============================================================================
// EXPERIMENTAL: Packet Size × Batch Size Interaction
// =============================================================================
// Tests the multiplicative effect of combining large packets with syscall batching.
// Hypothesis: For small packets, syscall overhead dominates (batching helps a lot).
//             For large packets, data transfer dominates (batching helps less relatively).

#[cfg(target_os = "linux")]
mod experimental_size_batch_interaction {
    use super::*;
    use std::net::{SocketAddr, UdpSocket};
    use std::os::unix::io::AsRawFd;

    /// Direct sendmmsg wrapper using libc
    unsafe fn sendmmsg_batch(
        fd: i32,
        messages: &mut [libc::mmsghdr],
    ) -> Result<usize, std::io::Error> {
        let ret = libc::sendmmsg(fd, messages.as_mut_ptr(), messages.len() as libc::c_uint, 0);
        if ret < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(ret as usize)
        }
    }

    /// Set socket buffer size using setsockopt
    fn set_socket_buffer_size(fd: i32, send_size: i32, recv_size: i32) {
        unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                &send_size as *const i32 as *const libc::c_void,
                std::mem::size_of::<i32>() as libc::socklen_t,
            );
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                &recv_size as *const i32 as *const libc::c_void,
                std::mem::size_of::<i32>() as libc::socklen_t,
            );
        }
    }

    /// Core benchmark: Test all combinations of packet size and batch size
    pub fn bench_size_batch_matrix(c: &mut Criterion) {
        let mut group = c.benchmark_group("experimental_size_batch_matrix");
        group.sample_size(50);

        // Packet sizes to test
        let packet_sizes = [512, 1400, 4096, 8192];
        // Batch sizes to test
        let batch_sizes = [1, 10, 50, 100];

        let send_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let recv_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let target_addr = recv_socket.local_addr().unwrap();
        send_socket.set_nonblocking(true).unwrap();
        recv_socket.set_nonblocking(true).unwrap();

        let fd = send_socket.as_raw_fd();
        let recv_fd = recv_socket.as_raw_fd();

        // Increase buffer sizes for large bursts
        set_socket_buffer_size(fd, 16 * 1024 * 1024, 0);
        set_socket_buffer_size(recv_fd, 0, 16 * 1024 * 1024);

        for &packet_size in &packet_sizes {
            let data: Vec<u8> = (0..packet_size).map(|i| (i % 256) as u8).collect();

            for &batch_size in &batch_sizes {
                let total_bytes = packet_size * batch_size;

                group.throughput(Throughput::Bytes(total_bytes as u64));
                group.bench_function(
                    BenchmarkId::new(
                        format!("size_{}_batch_{}", packet_size, batch_size),
                        total_bytes,
                    ),
                    |b| {
                        // Pre-allocate iovecs and message headers
                        let mut iovecs: Vec<libc::iovec> = (0..batch_size)
                            .map(|_| libc::iovec {
                                iov_base: data.as_ptr() as *mut libc::c_void,
                                iov_len: data.len(),
                            })
                            .collect();

                        // Build sockaddr_in
                        let addr_v4 = match target_addr {
                            SocketAddr::V4(a) => a,
                            _ => panic!("Expected IPv4"),
                        };
                        let sockaddr_in = libc::sockaddr_in {
                            sin_family: libc::AF_INET as libc::sa_family_t,
                            sin_port: addr_v4.port().to_be(),
                            sin_addr: libc::in_addr {
                                s_addr: u32::from_ne_bytes(addr_v4.ip().octets()),
                            },
                            sin_zero: [0; 8],
                        };

                        b.iter(|| {
                            let mut msgs: Vec<libc::mmsghdr> = iovecs
                                .iter_mut()
                                .map(|iov| {
                                    let mut msg: libc::mmsghdr = unsafe { std::mem::zeroed() };
                                    msg.msg_hdr.msg_name =
                                        &sockaddr_in as *const _ as *mut libc::c_void;
                                    msg.msg_hdr.msg_namelen =
                                        std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
                                    msg.msg_hdr.msg_iov = iov as *mut libc::iovec;
                                    msg.msg_hdr.msg_iovlen = 1;
                                    msg
                                })
                                .collect();

                            unsafe { sendmmsg_batch(fd, &mut msgs) }.unwrap()
                        });
                    },
                );
            }
        }

        group.finish();
    }

    /// Measure relative improvement: What's the speedup from batching at each packet size?
    pub fn bench_batching_improvement_by_size(c: &mut Criterion) {
        let mut group = c.benchmark_group("experimental_batch_improvement");
        group.sample_size(50);

        let packet_sizes = [512, 1400, 4096, 8192];
        let batch_size = 100; // Compare single-send vs batch-100

        let send_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let recv_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let target_addr = recv_socket.local_addr().unwrap();
        send_socket.set_nonblocking(true).unwrap();

        let fd = send_socket.as_raw_fd();
        let recv_fd = recv_socket.as_raw_fd();

        // Increase buffer sizes for large bursts
        set_socket_buffer_size(fd, 16 * 1024 * 1024, 0);
        set_socket_buffer_size(recv_fd, 0, 16 * 1024 * 1024);

        for &packet_size in &packet_sizes {
            let data: Vec<u8> = (0..packet_size).map(|i| (i % 256) as u8).collect();
            let total_bytes = (packet_size * batch_size) as u64;

            // Baseline: 100 individual send() calls
            group.throughput(Throughput::Bytes(total_bytes));
            group.bench_function(
                BenchmarkId::new(format!("single_sends_{}", packet_size), batch_size),
                |b| {
                    b.iter(|| {
                        for _ in 0..batch_size {
                            let _ = send_socket.send_to(&data, target_addr);
                        }
                    });
                },
            );

            // Optimized: 1 sendmmsg call with 100 packets
            group.bench_function(
                BenchmarkId::new(format!("batched_100_{}", packet_size), batch_size),
                |b| {
                    let mut iovecs: Vec<libc::iovec> = (0..batch_size)
                        .map(|_| libc::iovec {
                            iov_base: data.as_ptr() as *mut libc::c_void,
                            iov_len: data.len(),
                        })
                        .collect();

                    let addr_v4 = match target_addr {
                        SocketAddr::V4(a) => a,
                        _ => panic!("Expected IPv4"),
                    };
                    let sockaddr_in = libc::sockaddr_in {
                        sin_family: libc::AF_INET as libc::sa_family_t,
                        sin_port: addr_v4.port().to_be(),
                        sin_addr: libc::in_addr {
                            s_addr: u32::from_ne_bytes(addr_v4.ip().octets()),
                        },
                        sin_zero: [0; 8],
                    };

                    b.iter(|| {
                        let mut msgs: Vec<libc::mmsghdr> = iovecs
                            .iter_mut()
                            .map(|iov| {
                                let mut msg: libc::mmsghdr = unsafe { std::mem::zeroed() };
                                msg.msg_hdr.msg_name =
                                    &sockaddr_in as *const _ as *mut libc::c_void;
                                msg.msg_hdr.msg_namelen =
                                    std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
                                msg.msg_hdr.msg_iov = iov as *mut libc::iovec;
                                msg.msg_hdr.msg_iovlen = 1;
                                msg
                            })
                            .collect();

                        unsafe { sendmmsg_batch(fd, &mut msgs) }.unwrap()
                    });
                },
            );
        }

        group.finish();
    }

    /// Measure throughput ceiling at each packet size with maximum batching
    pub fn bench_throughput_ceiling(c: &mut Criterion) {
        let mut group = c.benchmark_group("experimental_throughput_ceiling");
        group.sample_size(30);

        // Fixed large batch size to saturate syscall efficiency
        let batch_size = 500;
        let packet_sizes = [512, 1400, 4096, 8192];

        let send_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let recv_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let target_addr = recv_socket.local_addr().unwrap();
        send_socket.set_nonblocking(true).unwrap();

        let fd = send_socket.as_raw_fd();
        let recv_fd = recv_socket.as_raw_fd();

        // Increase buffer sizes for large bursts
        set_socket_buffer_size(fd, 32 * 1024 * 1024, 0);
        set_socket_buffer_size(recv_fd, 0, 32 * 1024 * 1024);

        for &packet_size in &packet_sizes {
            let data: Vec<u8> = (0..packet_size).map(|i| (i % 256) as u8).collect();
            let total_bytes = (packet_size * batch_size) as u64;

            group.throughput(Throughput::Bytes(total_bytes));
            group.bench_function(BenchmarkId::new("max_throughput", packet_size), |b| {
                let mut iovecs: Vec<libc::iovec> = (0..batch_size)
                    .map(|_| libc::iovec {
                        iov_base: data.as_ptr() as *mut libc::c_void,
                        iov_len: data.len(),
                    })
                    .collect();

                let addr_v4 = match target_addr {
                    SocketAddr::V4(a) => a,
                    _ => panic!("Expected IPv4"),
                };
                let sockaddr_in = libc::sockaddr_in {
                    sin_family: libc::AF_INET as libc::sa_family_t,
                    sin_port: addr_v4.port().to_be(),
                    sin_addr: libc::in_addr {
                        s_addr: u32::from_ne_bytes(addr_v4.ip().octets()),
                    },
                    sin_zero: [0; 8],
                };

                b.iter(|| {
                    let mut msgs: Vec<libc::mmsghdr> = iovecs
                        .iter_mut()
                        .map(|iov| {
                            let mut msg: libc::mmsghdr = unsafe { std::mem::zeroed() };
                            msg.msg_hdr.msg_name = &sockaddr_in as *const _ as *mut libc::c_void;
                            msg.msg_hdr.msg_namelen =
                                std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
                            msg.msg_hdr.msg_iov = iov as *mut libc::iovec;
                            msg.msg_hdr.msg_iovlen = 1;
                            msg
                        })
                        .collect();

                    unsafe { sendmmsg_batch(fd, &mut msgs) }.unwrap()
                });
            });
        }

        group.finish();
    }
}

#[cfg(not(target_os = "linux"))]
mod experimental_size_batch_interaction {
    use super::*;

    pub fn bench_size_batch_matrix(_c: &mut Criterion) {
        eprintln!("size×batch interaction benchmarks only available on Linux");
    }

    pub fn bench_batching_improvement_by_size(_c: &mut Criterion) {
        eprintln!("size×batch interaction benchmarks only available on Linux");
    }

    pub fn bench_throughput_ceiling(_c: &mut Criterion) {
        eprintln!("size×batch interaction benchmarks only available on Linux");
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

criterion_group!(
    name = experimental_packet;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(5))
        .noise_threshold(0.10)
        .significance_level(0.05);
    targets =
        experimental_packet_size::bench_packet_size_throughput,
        experimental_packet_size::bench_syscall_overhead_vs_size,
        experimental_packet_size::bench_roundtrip_by_size,
);

criterion_group!(
    name = experimental_tokio;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(5))
        .noise_threshold(0.10)
        .significance_level(0.05);
    targets =
        experimental_tokio_overhead::bench_channel_comparison,
        experimental_tokio_overhead::bench_socket_overhead,
        experimental_tokio_overhead::bench_threading_model,
);

criterion_group!(
    name = experimental_combined;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(10))
        .sample_size(20)
        .noise_threshold(0.15);
    targets =
        experimental_combined::bench_full_pipeline_by_size,
);

criterion_group!(
    name = experimental_batching;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(5))
        .noise_threshold(0.10)
        .significance_level(0.05);
    targets =
        experimental_syscall_batching::bench_sendmmsg_vs_single,
        experimental_syscall_batching::bench_tokio_with_sendmmsg,
        experimental_syscall_batching::bench_syscall_reduction,
);

criterion_group!(
    name = experimental_true_syscall;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(5))
        .noise_threshold(0.10)
        .significance_level(0.05);
    targets =
        experimental_true_sendmmsg::bench_true_sendmmsg,
        experimental_true_sendmmsg::bench_sendmmsg_throughput,
        experimental_true_sendmmsg::bench_tokio_sendmmsg_integration,
);

criterion_group!(
    name = experimental_size_batch;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(5))
        .noise_threshold(0.10)
        .significance_level(0.05);
    targets =
        experimental_size_batch_interaction::bench_size_batch_matrix,
        experimental_size_batch_interaction::bench_batching_improvement_by_size,
        experimental_size_batch_interaction::bench_throughput_ceiling,
);

criterion_main!(
    level0,
    level1,
    level2,
    level3,
    experimental_packet,
    experimental_tokio,
    experimental_combined,
    experimental_batching,
    experimental_true_syscall,
    experimental_size_batch
);
