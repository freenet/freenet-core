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
// Transport Layer Benchmarks (Blackbox)
// =============================================================================
//
// These benchmarks test the actual Freenet transport code (PeerConnection,
// connection_handler, fast_channel, encryption) with mock sockets instead
// of real UDP. This tests the real code path without kernel syscall overhead.
//
// This is the primary CI benchmark group for detecting transport layer regressions.
//
// What's measured:
// - Message throughput: Full pipeline (serialize → encrypt → channel → decrypt)
// - fast_channel: Crossbeam-based channel vs tokio::sync::mpsc
// - Connection establishment: Full handshake timing

mod transport_blackbox {
    use super::*;
    use dashmap::DashMap;
    use freenet::transport::mock_transport::{create_mock_peer, Channels, PacketDropPolicy};
    use std::sync::Arc;

    /// Benchmark connection establishment between two mock peers.
    ///
    /// This measures the full handshake: key exchange, encryption setup, etc.
    pub fn bench_connection_establishment(c: &mut Criterion) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        let mut group = c.benchmark_group("transport/connection");

        group.bench_function("establish", |b| {
            b.to_async(&rt).iter_batched(
                || {
                    // Setup: create channel map for this iteration
                    Arc::new(DashMap::new()) as Channels
                },
                |channels| async move {
                    // Create two peers
                    let (peer_a_pub, mut peer_a, peer_a_addr) =
                        create_mock_peer(PacketDropPolicy::ReceiveAll, channels.clone())
                            .await
                            .unwrap();
                    let (peer_b_pub, mut peer_b, peer_b_addr) =
                        create_mock_peer(PacketDropPolicy::ReceiveAll, channels)
                            .await
                            .unwrap();

                    // Establish connection from both sides (NAT traversal style)
                    // connect() is async fn returning Pin<Box<dyn Future<...>>>, so two levels of await
                    let (conn_a_inner, conn_b_inner) = futures::join!(
                        peer_a.connect(peer_b_pub, peer_b_addr),
                        peer_b.connect(peer_a_pub, peer_a_addr),
                    );
                    let (conn_a, conn_b) = futures::join!(conn_a_inner, conn_b_inner);

                    // Connection established - drop the connections
                    // (the benchmark measures establishment time, not ongoing usage)
                    drop(conn_a.unwrap());
                    drop(conn_b.unwrap());
                },
                BatchSize::SmallInput,
            );
        });

        group.finish();
    }

    /// Benchmark message throughput between two connected mock peers.
    ///
    /// This measures the full pipeline: serialize → encrypt → channel → decrypt → deserialize
    /// Note: Each iteration includes connection setup for proper benchmark isolation.
    pub fn bench_message_throughput(c: &mut Criterion) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        let mut group = c.benchmark_group("transport/throughput");

        // Test different message sizes
        for &size in &[64, 256, 1024, 1364] {
            group.throughput(Throughput::Bytes(size as u64));

            group.bench_with_input(BenchmarkId::new("bytes", size), &size, |b, &sz| {
                b.to_async(&rt).iter_batched(
                    || {
                        // Setup: Create fresh connected peers for each batch
                        let message = vec![0xABu8; sz];
                        (Arc::new(DashMap::new()) as Channels, message)
                    },
                    |(channels, message)| async move {
                        // Create peers
                        let (peer_a_pub, mut peer_a, peer_a_addr) =
                            create_mock_peer(PacketDropPolicy::ReceiveAll, channels.clone())
                                .await
                                .unwrap();
                        let (peer_b_pub, mut peer_b, peer_b_addr) =
                            create_mock_peer(PacketDropPolicy::ReceiveAll, channels)
                                .await
                                .unwrap();

                        // Connect
                        let (conn_a_inner, conn_b_inner) = futures::join!(
                            peer_a.connect(peer_b_pub, peer_b_addr),
                            peer_b.connect(peer_a_pub, peer_a_addr),
                        );
                        let (conn_a, conn_b) = futures::join!(conn_a_inner, conn_b_inner);
                        let (mut conn_a, mut conn_b) = (conn_a.unwrap(), conn_b.unwrap());

                        // Send and receive message (this is what we're measuring)
                        conn_a.send(message).await.unwrap();
                        let received: Vec<u8> = conn_b.recv().await.unwrap();
                        std_black_box(received);
                    },
                    BatchSize::SmallInput,
                );
            });
        }

        group.finish();
    }
}

// =============================================================================
// Transport Streaming Benchmarks - Large message transfers
// =============================================================================
//
// These benchmarks test stream fragmentation, reassembly, and rate limiting
// for messages larger than a single packet (>1364 bytes).
//
// This is critical for measuring the impact of congestion control improvements.

mod transport_streaming {
    use super::*;
    use dashmap::DashMap;
    use freenet::transport::mock_transport::{create_mock_peer, Channels, PacketDropPolicy};
    use std::sync::Arc;

    /// Benchmark large message streaming (multi-packet transfers)
    ///
    /// This measures the stream fragmentation and reassembly pipeline with
    /// the current rate limiting in place (3 MB/s default).
    pub fn bench_stream_throughput(c: &mut Criterion) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        let mut group = c.benchmark_group("transport/streaming/throughput");

        // Test STREAM_SIZES: 4KB, 16KB, 64KB
        for &size in &[4096, 16384, 65536] {
            group.throughput(Throughput::Bytes(size as u64));

            group.bench_with_input(BenchmarkId::new("rate_limited", size), &size, |b, &sz| {
                b.to_async(&rt).iter_batched(
                    || {
                        let message = vec![0xABu8; sz];
                        (Arc::new(DashMap::new()) as Channels, message)
                    },
                    |(channels, message)| async move {
                        // Create connected peers
                        let (peer_a_pub, mut peer_a, peer_a_addr) =
                            create_mock_peer(PacketDropPolicy::ReceiveAll, channels.clone())
                                .await
                                .unwrap();
                        let (peer_b_pub, mut peer_b, peer_b_addr) =
                            create_mock_peer(PacketDropPolicy::ReceiveAll, channels)
                                .await
                                .unwrap();

                        let (conn_a_inner, conn_b_inner) = futures::join!(
                            peer_a.connect(peer_b_pub, peer_b_addr),
                            peer_b.connect(peer_a_pub, peer_a_addr),
                        );
                        let (conn_a, conn_b) = futures::join!(conn_a_inner, conn_b_inner);
                        let (mut conn_a, mut conn_b) = (conn_a.unwrap(), conn_b.unwrap());

                        // Send large message (will be fragmented)
                        conn_a.send(message.clone()).await.unwrap();
                        let received: Vec<u8> = conn_b.recv().await.unwrap();

                        // Note: received length may differ slightly due to serialization overhead
                        // Just verify we got data back
                        assert!(!received.is_empty());
                        std_black_box(received);
                    },
                    BatchSize::SmallInput,
                );
            });
        }

        group.finish();
    }

    /// Benchmark multiple concurrent streams to same peer
    ///
    /// This measures fairness and aggregate throughput when multiple streams
    /// compete for bandwidth.
    pub fn bench_concurrent_streams(c: &mut Criterion) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        let mut group = c.benchmark_group("transport/streaming/concurrent");

        // Test 2, 5, 10 concurrent streams
        for num_streams in [2, 5, 10] {
            group.bench_with_input(
                BenchmarkId::new("streams", num_streams),
                &num_streams,
                |b, &n| {
                    b.to_async(&rt).iter_batched(
                        || Arc::new(DashMap::new()) as Channels,
                        |channels| async move {
                            // Create connected peers
                            let (peer_a_pub, mut peer_a, peer_a_addr) =
                                create_mock_peer(PacketDropPolicy::ReceiveAll, channels.clone())
                                    .await
                                    .unwrap();
                            let (peer_b_pub, mut peer_b, peer_b_addr) =
                                create_mock_peer(PacketDropPolicy::ReceiveAll, channels)
                                    .await
                                    .unwrap();

                            let (conn_a_inner, conn_b_inner) = futures::join!(
                                peer_a.connect(peer_b_pub, peer_b_addr),
                                peer_b.connect(peer_a_pub, peer_a_addr),
                            );
                            let (conn_a, conn_b) = futures::join!(conn_a_inner, conn_b_inner);
                            let (mut conn_a, mut conn_b) = (conn_a.unwrap(), conn_b.unwrap());

                            // Send n messages concurrently (connection handles concurrency internally)
                            let message = vec![0xABu8; 16384]; // 16KB each

                            // Spawn concurrent send and receive tasks
                            let send_task = tokio::spawn(async move {
                                for _ in 0..n {
                                    conn_a.send(message.clone()).await.unwrap();
                                }
                            });

                            let recv_task = tokio::spawn(async move {
                                let mut results = Vec::new();
                                for i in 0..n {
                                    let received: Vec<u8> = conn_b.recv().await.unwrap();
                                    results.push((i, received.len()));
                                }
                                results
                            });

                            // Wait for both to complete
                            send_task.await.unwrap();
                            let results = recv_task.await.unwrap();

                            std_black_box(results);
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }

        group.finish();
    }

    /// Benchmark rate limiting accuracy
    ///
    /// Measures how closely actual throughput matches configured limit.
    /// This validates the current 3 MB/s implementation.
    pub fn bench_rate_limit_accuracy(c: &mut Criterion) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        let mut group = c.benchmark_group("transport/streaming/rate_limit");
        group.measurement_time(Duration::from_secs(10));

        // Send 1MB with rate limiting
        let size = 1_000_000; // 1 MB
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_function("1mb_with_3mbps_limit", |b| {
            b.to_async(&rt).iter_batched(
                || {
                    let message = vec![0xABu8; size];
                    (Arc::new(DashMap::new()) as Channels, message)
                },
                |(channels, message)| async move {
                    let (peer_a_pub, mut peer_a, peer_a_addr) =
                        create_mock_peer(PacketDropPolicy::ReceiveAll, channels.clone())
                            .await
                            .unwrap();
                    let (peer_b_pub, mut peer_b, peer_b_addr) =
                        create_mock_peer(PacketDropPolicy::ReceiveAll, channels)
                            .await
                            .unwrap();

                    let (conn_a_inner, conn_b_inner) = futures::join!(
                        peer_a.connect(peer_b_pub, peer_b_addr),
                        peer_b.connect(peer_a_pub, peer_a_addr),
                    );
                    let (conn_a, conn_b) = futures::join!(conn_a_inner, conn_b_inner);
                    let (mut conn_a, mut conn_b) = (conn_a.unwrap(), conn_b.unwrap());

                    let start = std::time::Instant::now();
                    conn_a.send(message).await.unwrap();
                    let received: Vec<u8> = conn_b.recv().await.unwrap();
                    let elapsed = start.elapsed();

                    // Expected: ~333ms for 1MB at 3 MB/s
                    // Actual will be measured by Criterion
                    std_black_box((received, elapsed));
                },
                BatchSize::SmallInput,
            );
        });

        group.finish();
    }
}

// =============================================================================
// Transport with Delay - Real network conditions
// =============================================================================
//
// These benchmarks test LEDBAT congestion control under realistic network
// conditions by injecting artificial delays into the mock transport.
//
// This validates that LEDBAT properly adapts to queuing delays.

mod transport_with_delay {
    use super::*;
    use dashmap::DashMap;
    use freenet::transport::mock_transport::{
        create_mock_peer_with_delay, Channels, PacketDelayPolicy, PacketDropPolicy,
    };
    use std::sync::Arc;

    /// Benchmark streaming with fixed network delay (simulates LAN/WAN/satellite)
    ///
    /// Tests LEDBAT's ability to measure and adapt to base RTT.
    pub fn bench_stream_with_fixed_delay(c: &mut Criterion) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        let mut group = c.benchmark_group("transport/delay/fixed");
        group.measurement_time(Duration::from_secs(15)); // Longer to observe LEDBAT adaptation

        // Test different network conditions
        for delay_ms in [10, 50, 100] {
            let delay = Duration::from_millis(delay_ms);
            group.throughput(Throughput::Bytes(65536)); // 64KB transfer

            group.bench_with_input(
                BenchmarkId::new("64kb", format!("{}ms", delay_ms)),
                &delay,
                |b, &delay| {
                    b.to_async(&rt).iter_batched(
                        || {
                            let message = vec![0xABu8; 65536];
                            (Arc::new(DashMap::new()) as Channels, message, delay)
                        },
                        |(channels, message, delay)| async move {
                            let delay_policy = PacketDelayPolicy::Fixed(delay);

                            // Create peers with delay
                            let (peer_a_pub, mut peer_a, peer_a_addr) =
                                create_mock_peer_with_delay(
                                    PacketDropPolicy::ReceiveAll,
                                    delay_policy.clone(),
                                    channels.clone(),
                                )
                                .await
                                .unwrap();
                            let (peer_b_pub, mut peer_b, peer_b_addr) =
                                create_mock_peer_with_delay(
                                    PacketDropPolicy::ReceiveAll,
                                    delay_policy,
                                    channels,
                                )
                                .await
                                .unwrap();

                            // Connect
                            let (conn_a_inner, conn_b_inner) = futures::join!(
                                peer_a.connect(peer_b_pub, peer_b_addr),
                                peer_b.connect(peer_a_pub, peer_a_addr),
                            );
                            let (conn_a, conn_b) = futures::join!(conn_a_inner, conn_b_inner);
                            let (mut conn_a, mut conn_b) = (conn_a.unwrap(), conn_b.unwrap());

                            // Send large message (LEDBAT should adapt to delay)
                            let start = std::time::Instant::now();
                            conn_a.send(message).await.unwrap();
                            let received: Vec<u8> = conn_b.recv().await.unwrap();
                            let elapsed = start.elapsed();

                            assert!(!received.is_empty());
                            std_black_box((received, elapsed));
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }

        group.finish();
    }

    /// Benchmark streaming with variable delay (jitter)
    ///
    /// Tests LEDBAT's stability when RTT varies (realistic Internet conditions).
    pub fn bench_stream_with_jitter(c: &mut Criterion) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        let mut group = c.benchmark_group("transport/delay/jitter");
        group.measurement_time(Duration::from_secs(15));

        group.throughput(Throughput::Bytes(65536));

        group.bench_function("64kb_30-70ms", |b| {
            b.to_async(&rt).iter_batched(
                || {
                    let message = vec![0xABu8; 65536];
                    (Arc::new(DashMap::new()) as Channels, message)
                },
                |(channels, message)| async move {
                    let delay_policy = PacketDelayPolicy::Uniform {
                        min: Duration::from_millis(30),
                        max: Duration::from_millis(70),
                    };

                    let (peer_a_pub, mut peer_a, peer_a_addr) = create_mock_peer_with_delay(
                        PacketDropPolicy::ReceiveAll,
                        delay_policy.clone(),
                        channels.clone(),
                    )
                    .await
                    .unwrap();
                    let (peer_b_pub, mut peer_b, peer_b_addr) = create_mock_peer_with_delay(
                        PacketDropPolicy::ReceiveAll,
                        delay_policy,
                        channels,
                    )
                    .await
                    .unwrap();

                    let (conn_a_inner, conn_b_inner) = futures::join!(
                        peer_a.connect(peer_b_pub, peer_b_addr),
                        peer_b.connect(peer_a_pub, peer_a_addr),
                    );
                    let (conn_a, conn_b) = futures::join!(conn_a_inner, conn_b_inner);
                    let (mut conn_a, mut conn_b) = (conn_a.unwrap(), conn_b.unwrap());

                    conn_a.send(message).await.unwrap();
                    let received: Vec<u8> = conn_b.recv().await.unwrap();

                    assert!(!received.is_empty());
                    std_black_box(received);
                },
                BatchSize::SmallInput,
            );
        });

        group.finish();
    }

    /// Benchmark streaming with congestion (delay + packet loss)
    ///
    /// Tests LEDBAT's response to congestion signals (both delay and loss).
    pub fn bench_stream_with_congestion(c: &mut Criterion) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        let mut group = c.benchmark_group("transport/delay/congestion");
        group.measurement_time(Duration::from_secs(20)); // Longer due to retransmissions

        // Test different congestion levels
        for (loss_pct, delay_ms) in [(1.0, 50), (5.0, 100)] {
            group.throughput(Throughput::Bytes(65536));

            group.bench_with_input(
                BenchmarkId::new("64kb", format!("{}%loss_{}ms", loss_pct as u32, delay_ms)),
                &(loss_pct, delay_ms),
                |b, &(loss_pct, delay_ms)| {
                    b.to_async(&rt).iter_batched(
                        || {
                            let message = vec![0xABu8; 65536];
                            (Arc::new(DashMap::new()) as Channels, message)
                        },
                        |(channels, message)| async move {
                            let drop_policy = PacketDropPolicy::Factor(loss_pct / 100.0);
                            let delay_policy =
                                PacketDelayPolicy::Fixed(Duration::from_millis(delay_ms));

                            let (peer_a_pub, mut peer_a, peer_a_addr) =
                                create_mock_peer_with_delay(
                                    drop_policy.clone(),
                                    delay_policy.clone(),
                                    channels.clone(),
                                )
                                .await
                                .unwrap();
                            let (peer_b_pub, mut peer_b, peer_b_addr) =
                                create_mock_peer_with_delay(drop_policy, delay_policy, channels)
                                    .await
                                    .unwrap();

                            let (conn_a_inner, conn_b_inner) = futures::join!(
                                peer_a.connect(peer_b_pub, peer_b_addr),
                                peer_b.connect(peer_a_pub, peer_a_addr),
                            );
                            let (conn_a, conn_b) = futures::join!(conn_a_inner, conn_b_inner);
                            let (mut conn_a, mut conn_b) = (conn_a.unwrap(), conn_b.unwrap());

                            // Send with congestion (LEDBAT should back off)
                            conn_a.send(message).await.unwrap();
                            let received: Vec<u8> = conn_b.recv().await.unwrap();

                            assert!(!received.is_empty());
                            std_black_box(received);
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }

        group.finish();
    }

    /// Benchmark LEDBAT rate adaptation over time
    ///
    /// Sends multiple messages to observe how LEDBAT adjusts cwnd based on queuing delay.
    pub fn bench_ledbat_adaptation(c: &mut Criterion) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        let mut group = c.benchmark_group("transport/delay/ledbat_adaptation");
        group.measurement_time(Duration::from_secs(20));
        group.sample_size(20); // Fewer samples due to longer runtime

        group.bench_function("10_transfers_50ms", |b| {
            b.to_async(&rt).iter_batched(
                || Arc::new(DashMap::new()) as Channels,
                |channels| async move {
                    let delay_policy = PacketDelayPolicy::Fixed(Duration::from_millis(50));

                    let (peer_a_pub, mut peer_a, peer_a_addr) = create_mock_peer_with_delay(
                        PacketDropPolicy::ReceiveAll,
                        delay_policy.clone(),
                        channels.clone(),
                    )
                    .await
                    .unwrap();
                    let (peer_b_pub, mut peer_b, peer_b_addr) = create_mock_peer_with_delay(
                        PacketDropPolicy::ReceiveAll,
                        delay_policy,
                        channels,
                    )
                    .await
                    .unwrap();

                    let (conn_a_inner, conn_b_inner) = futures::join!(
                        peer_a.connect(peer_b_pub, peer_b_addr),
                        peer_b.connect(peer_a_pub, peer_a_addr),
                    );
                    let (conn_a, conn_b) = futures::join!(conn_a_inner, conn_b_inner);
                    let (mut conn_a, mut conn_b) = (conn_a.unwrap(), conn_b.unwrap());

                    // Send 10 messages sequentially - observe LEDBAT learning the network
                    for _ in 0..10 {
                        let message = vec![0xABu8; 16384]; // 16KB each
                        conn_a.send(message).await.unwrap();
                        let received: Vec<u8> = conn_b.recv().await.unwrap();
                        std_black_box(received);
                    }
                },
                BatchSize::SmallInput,
            );
        });

        group.finish();
    }
}

// =============================================================================
// LEDBAT Validation - Fast benchmarks with larger transfers
// =============================================================================
//
// This validates Opus's hypothesis that the 100ms-faster-than-50ms anomaly
// is due to statistical variance with small transfers.
//
// Uses larger transfers (256KB, 1MB) with warmup to eliminate burst effects.
// Faster execution: fewer samples, shorter measurement times.

mod ledbat_validation {
    use super::*;
    use dashmap::DashMap;
    use freenet::transport::mock_transport::{
        create_mock_peer_with_delay, Channels, PacketDelayPolicy, PacketDropPolicy,
    };
    use std::sync::Arc;

    /// Helper to run warmup transfers before measurement
    async fn warmup_connection(
        conn_a: &mut freenet::transport::PeerConnection,
        conn_b: &mut freenet::transport::PeerConnection,
        warmup_count: usize,
        warmup_size: usize,
    ) {
        for _ in 0..warmup_count {
            let msg = vec![0xABu8; warmup_size];
            conn_a.send(msg).await.unwrap();
            let _: Vec<u8> = conn_b.recv().await.unwrap();
        }
    }

    /// Benchmark with larger transfers to see if pattern holds
    ///
    /// Tests 256KB transfers with 10ms, 50ms, 100ms delays.
    /// If 100ms is still faster than 50ms with large transfers, it's real.
    /// If pattern corrects (higher delay = slower), it was variance.
    pub fn bench_large_transfer_validation(c: &mut Criterion) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        let mut group = c.benchmark_group("ledbat/validation/256kb");
        group.sample_size(20); // Faster: 20 samples instead of 100
        group.measurement_time(Duration::from_secs(8)); // Faster: 8s instead of 15s

        for delay_ms in [10, 50, 100] {
            let delay = Duration::from_millis(delay_ms);
            group.throughput(Throughput::Bytes(262144)); // 256KB

            group.bench_with_input(
                BenchmarkId::new("with_warmup", format!("{}ms", delay_ms)),
                &delay,
                |b, &delay| {
                    b.to_async(&rt).iter_batched(
                        || {
                            let message = vec![0xABu8; 262144]; // 256KB
                            (Arc::new(DashMap::new()) as Channels, message, delay)
                        },
                        |(channels, message, delay)| async move {
                            let delay_policy = PacketDelayPolicy::Fixed(delay);

                            let (peer_a_pub, mut peer_a, peer_a_addr) =
                                create_mock_peer_with_delay(
                                    PacketDropPolicy::ReceiveAll,
                                    delay_policy.clone(),
                                    channels.clone(),
                                )
                                .await
                                .unwrap();
                            let (peer_b_pub, mut peer_b, peer_b_addr) =
                                create_mock_peer_with_delay(
                                    PacketDropPolicy::ReceiveAll,
                                    delay_policy,
                                    channels,
                                )
                                .await
                                .unwrap();

                            let (conn_a_inner, conn_b_inner) = futures::join!(
                                peer_a.connect(peer_b_pub, peer_b_addr),
                                peer_b.connect(peer_a_pub, peer_a_addr),
                            );
                            let (conn_a, conn_b) = futures::join!(conn_a_inner, conn_b_inner);
                            let (mut conn_a, mut conn_b) = (conn_a.unwrap(), conn_b.unwrap());

                            // Warmup: 3 transfers of 64KB each to eliminate initial burst effects
                            warmup_connection(&mut conn_a, &mut conn_b, 3, 65536).await;

                            // Measured transfer
                            conn_a.send(message).await.unwrap();
                            let received: Vec<u8> = conn_b.recv().await.unwrap();

                            assert!(!received.is_empty());
                            std_black_box(received);
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }

        group.finish();
    }

    /// Benchmark 1MB transfers to confirm pattern with very large data
    pub fn bench_1mb_transfer_validation(c: &mut Criterion) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        let mut group = c.benchmark_group("ledbat/validation/1mb");
        group.sample_size(10); // Very fast: only 10 samples
        group.measurement_time(Duration::from_secs(10));

        for delay_ms in [10, 50, 100] {
            let delay = Duration::from_millis(delay_ms);
            group.throughput(Throughput::Bytes(1048576)); // 1MB

            group.bench_with_input(
                BenchmarkId::new("with_warmup", format!("{}ms", delay_ms)),
                &delay,
                |b, &delay| {
                    b.to_async(&rt).iter_batched(
                        || {
                            let message = vec![0xABu8; 1048576]; // 1MB
                            (Arc::new(DashMap::new()) as Channels, message, delay)
                        },
                        |(channels, message, delay)| async move {
                            let delay_policy = PacketDelayPolicy::Fixed(delay);

                            let (peer_a_pub, mut peer_a, peer_a_addr) =
                                create_mock_peer_with_delay(
                                    PacketDropPolicy::ReceiveAll,
                                    delay_policy.clone(),
                                    channels.clone(),
                                )
                                .await
                                .unwrap();
                            let (peer_b_pub, mut peer_b, peer_b_addr) =
                                create_mock_peer_with_delay(
                                    PacketDropPolicy::ReceiveAll,
                                    delay_policy,
                                    channels,
                                )
                                .await
                                .unwrap();

                            let (conn_a_inner, conn_b_inner) = futures::join!(
                                peer_a.connect(peer_b_pub, peer_b_addr),
                                peer_b.connect(peer_a_pub, peer_a_addr),
                            );
                            let (conn_a, conn_b) = futures::join!(conn_a_inner, conn_b_inner);
                            let (mut conn_a, mut conn_b) = (conn_a.unwrap(), conn_b.unwrap());

                            // Warmup: 5 transfers to fully stabilize LEDBAT
                            warmup_connection(&mut conn_a, &mut conn_b, 5, 65536).await;

                            // Measured transfer
                            conn_a.send(message).await.unwrap();
                            let received: Vec<u8> = conn_b.recv().await.unwrap();

                            assert!(!received.is_empty());
                            std_black_box(received);
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }

        group.finish();
    }

    /// Quick congestion test with larger transfer
    pub fn bench_congestion_256kb(c: &mut Criterion) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        let mut group = c.benchmark_group("ledbat/validation/congestion");
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(12));

        for (loss_pct, delay_ms) in [(1.0, 50), (5.0, 100)] {
            group.throughput(Throughput::Bytes(262144));

            group.bench_with_input(
                BenchmarkId::new("256kb", format!("{}%loss_{}ms", loss_pct as u32, delay_ms)),
                &(loss_pct, delay_ms),
                |b, &(loss_pct, delay_ms)| {
                    b.to_async(&rt).iter_batched(
                        || {
                            let message = vec![0xABu8; 262144];
                            (Arc::new(DashMap::new()) as Channels, message)
                        },
                        |(channels, message)| async move {
                            let drop_policy = PacketDropPolicy::Factor(loss_pct / 100.0);
                            let delay_policy =
                                PacketDelayPolicy::Fixed(Duration::from_millis(delay_ms));

                            let (peer_a_pub, mut peer_a, peer_a_addr) =
                                create_mock_peer_with_delay(
                                    drop_policy.clone(),
                                    delay_policy.clone(),
                                    channels.clone(),
                                )
                                .await
                                .unwrap();
                            let (peer_b_pub, mut peer_b, peer_b_addr) =
                                create_mock_peer_with_delay(drop_policy, delay_policy, channels)
                                    .await
                                    .unwrap();

                            let (conn_a_inner, conn_b_inner) = futures::join!(
                                peer_a.connect(peer_b_pub, peer_b_addr),
                                peer_b.connect(peer_a_pub, peer_a_addr),
                            );
                            let (conn_a, conn_b) = futures::join!(conn_a_inner, conn_b_inner);
                            let (mut conn_a, mut conn_b) = (conn_a.unwrap(), conn_b.unwrap());

                            // Warmup
                            warmup_connection(&mut conn_a, &mut conn_b, 2, 65536).await;

                            // Measured transfer with congestion
                            conn_a.send(message).await.unwrap();
                            let received: Vec<u8> = conn_b.recv().await.unwrap();

                            assert!(!received.is_empty());
                            std_black_box(received);
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
// Slow Start Validation - Benchmarks to measure cold-start throughput
// =============================================================================

mod slow_start_validation {
    use super::*;
    use dashmap::DashMap;
    use freenet::transport::mock_transport::{
        create_mock_peer_with_delay, PacketDelayPolicy, PacketDropPolicy,
    };
    use std::sync::Arc;
    use std::time::Instant;

    /// Benchmark fresh connection cold-start throughput
    /// This measures the actual slow start benefit (or lack thereof)
    pub fn bench_cold_start_throughput(c: &mut Criterion) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        let mut group = c.benchmark_group("slow_start/cold_start");
        group.sample_size(20); // More samples for variance analysis
        group.measurement_time(Duration::from_secs(30));

        for transfer_size_kb in [256, 512, 1024, 4096] {
            let transfer_size = transfer_size_kb * 1024;
            group.throughput(Throughput::Bytes(transfer_size as u64));

            for delay_ms in [10, 50, 100] {
                group.bench_function(
                    &format!("{}kb_{}ms_no_warmup", transfer_size_kb, delay_ms),
                    |b| {
                        b.to_async(&rt).iter_batched(
                            || {
                                // Setup: create message only, NO connection yet
                                vec![0xABu8; transfer_size]
                            },
                            |message| async move {
                                // Connection creation is PART OF THE MEASUREMENT
                                // This measures real cold-start behavior
                                let delay = Duration::from_millis(delay_ms);
                                let channels = Arc::new(DashMap::new());

                                let (peer_a_pub, mut peer_a, peer_a_addr) =
                                    create_mock_peer_with_delay(
                                        PacketDropPolicy::ReceiveAll,
                                        PacketDelayPolicy::Fixed(delay),
                                        channels.clone(),
                                    )
                                    .await
                                    .unwrap();

                                let (peer_b_pub, mut peer_b, peer_b_addr) =
                                    create_mock_peer_with_delay(
                                        PacketDropPolicy::ReceiveAll,
                                        PacketDelayPolicy::Fixed(delay),
                                        channels,
                                    )
                                    .await
                                    .unwrap();

                                let (conn_a_inner, conn_b_inner) = futures::join!(
                                    peer_a.connect(peer_b_pub, peer_b_addr),
                                    peer_b.connect(peer_a_pub, peer_a_addr),
                                );
                                let (conn_a, conn_b) = futures::join!(conn_a_inner, conn_b_inner);
                                let (mut conn_a, mut conn_b) = (conn_a.unwrap(), conn_b.unwrap());

                                // Measured transfer - FIRST transfer on fresh connection
                                conn_a.send(message).await.unwrap();
                                let received: Vec<u8> = conn_b.recv().await.unwrap();

                                std_black_box(received);
                            },
                            BatchSize::SmallInput,
                        );
                    },
                );
            }
        }
        group.finish();
    }

    /// Benchmark warm connection throughput (for comparison)
    pub fn bench_warm_connection_throughput(c: &mut Criterion) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        let mut group = c.benchmark_group("slow_start/warm_connection");
        group.sample_size(20);
        group.measurement_time(Duration::from_secs(30));

        for transfer_size_kb in [256, 512, 1024, 4096] {
            let transfer_size = transfer_size_kb * 1024;
            group.throughput(Throughput::Bytes(transfer_size as u64));

            for delay_ms in [10, 50, 100] {
                let delay = Duration::from_millis(delay_ms);

                group.bench_function(
                    &format!("{}kb_{}ms_warm", transfer_size_kb, delay_ms),
                    |b| {
                        // Setup connection and warmup OUTSIDE measurement
                        let (conn_a, conn_b) = rt.block_on(async {
                            let channels = Arc::new(DashMap::new());

                            let (peer_a_pub, mut peer_a, peer_a_addr) =
                                create_mock_peer_with_delay(
                                    PacketDropPolicy::ReceiveAll,
                                    PacketDelayPolicy::Fixed(delay),
                                    channels.clone(),
                                )
                                .await
                                .unwrap();

                            let (peer_b_pub, mut peer_b, peer_b_addr) =
                                create_mock_peer_with_delay(
                                    PacketDropPolicy::ReceiveAll,
                                    PacketDelayPolicy::Fixed(delay),
                                    channels,
                                )
                                .await
                                .unwrap();

                            let (conn_a_inner, conn_b_inner) = futures::join!(
                                peer_a.connect(peer_b_pub, peer_b_addr),
                                peer_b.connect(peer_a_pub, peer_a_addr),
                            );
                            let (conn_a, conn_b) = futures::join!(conn_a_inner, conn_b_inner);
                            let (mut conn_a, mut conn_b) = (conn_a.unwrap(), conn_b.unwrap());

                            // Warmup: 10 x 100KB transfers to stabilize LEDBAT
                            for _ in 0..10 {
                                let msg = vec![0xABu8; 102400];
                                conn_a.send(msg).await.unwrap();
                                let _: Vec<u8> = conn_b.recv().await.unwrap();
                            }

                            (conn_a, conn_b)
                        });

                        let conn_a = Arc::new(tokio::sync::Mutex::new(conn_a));
                        let conn_b = Arc::new(tokio::sync::Mutex::new(conn_b));

                        b.to_async(&rt).iter_batched(
                            || vec![0xABu8; transfer_size],
                            |message| {
                                let conn_a = conn_a.clone();
                                let conn_b = conn_b.clone();
                                async move {
                                    let mut conn_a = conn_a.lock().await;
                                    let mut conn_b = conn_b.lock().await;

                                    conn_a.send(message).await.unwrap();
                                    let received: Vec<u8> = conn_b.recv().await.unwrap();
                                    std_black_box(received);
                                }
                            },
                            BatchSize::SmallInput,
                        );
                    },
                );
            }
        }
        group.finish();
    }

    /// Instrumented benchmark that captures cwnd evolution
    /// This is for diagnostic purposes, not CI performance tracking
    pub fn bench_cwnd_evolution(c: &mut Criterion) {
        let rt = tokio::runtime::Runtime::new().unwrap();

        let mut group = c.benchmark_group("slow_start/cwnd_evolution");
        group.sample_size(10);

        group.bench_function("1mb_100ms_cwnd_trace", |b| {
            b.to_async(&rt).iter(|| async {
                let delay = Duration::from_millis(100);
                let channels = Arc::new(DashMap::new());

                // Create connection with cwnd tracing enabled
                let (peer_a_pub, mut peer_a, peer_a_addr) = create_mock_peer_with_delay(
                    PacketDropPolicy::ReceiveAll,
                    PacketDelayPolicy::Fixed(delay),
                    channels.clone(),
                )
                .await
                .unwrap();

                let (peer_b_pub, mut peer_b, peer_b_addr) = create_mock_peer_with_delay(
                    PacketDropPolicy::ReceiveAll,
                    PacketDelayPolicy::Fixed(delay),
                    channels,
                )
                .await
                .unwrap();

                let (conn_a_inner, conn_b_inner) = futures::join!(
                    peer_a.connect(peer_b_pub, peer_b_addr),
                    peer_b.connect(peer_a_pub, peer_a_addr),
                );
                let (conn_a, conn_b) = futures::join!(conn_a_inner, conn_b_inner);
                let (mut conn_a, mut conn_b) = (conn_a.unwrap(), conn_b.unwrap());

                let message = vec![0xABu8; 1024 * 1024];
                let start = Instant::now();
                conn_a.send(message).await.unwrap();
                let received: Vec<u8> = conn_b.recv().await.unwrap();
                let elapsed = start.elapsed();

                // Log timing (visible with --nocapture)
                let throughput_mbps = (1.0 / elapsed.as_secs_f64()) * 8.0;
                println!("1MB transfer: {:?} ({:.2} Mbps)", elapsed, throughput_mbps);

                std_black_box(received);
            });
        });

        group.finish();
    }

    /// Benchmark 1MB transfer with high bandwidth limit to test maximum throughput
    /// This verifies that with sufficient bandwidth, LEDBAT can achieve >10 MB/s
    pub fn bench_high_bandwidth_throughput(c: &mut Criterion) {
        use freenet::transport::mock_transport::create_mock_peer_with_bandwidth;

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        let mut group = c.benchmark_group("slow_start/high_bandwidth");
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(15));
        group.throughput(Throughput::Bytes(1048576)); // 1MB

        // Test with 100 MB/s bandwidth limit (10x the default)
        let bandwidth_limit_mb = 100;

        group.bench_function(
            BenchmarkId::new("1mb_transfer", format!("{}mb_limit", bandwidth_limit_mb)),
            |b| {
                b.to_async(&rt).iter(|| async {
                    let delay = Duration::from_millis(10); // 10ms RTT
                    let channels = Arc::new(DashMap::new());
                    let bandwidth_limit = Some(bandwidth_limit_mb * 1_000_000); // Convert to bytes/sec

                    // Create connection with high bandwidth limit
                    let (peer_a_pub, mut peer_a, peer_a_addr) = create_mock_peer_with_bandwidth(
                        PacketDropPolicy::ReceiveAll,
                        PacketDelayPolicy::Fixed(delay),
                        channels.clone(),
                        bandwidth_limit,
                    )
                    .await
                    .unwrap();

                    let (peer_b_pub, mut peer_b, peer_b_addr) = create_mock_peer_with_bandwidth(
                        PacketDropPolicy::ReceiveAll,
                        PacketDelayPolicy::Fixed(delay),
                        channels,
                        bandwidth_limit,
                    )
                    .await
                    .unwrap();

                    let (conn_a_inner, conn_b_inner) = futures::join!(
                        peer_a.connect(peer_b_pub, peer_b_addr),
                        peer_b.connect(peer_a_pub, peer_a_addr),
                    );
                    let (conn_a, conn_b) = futures::join!(conn_a_inner, conn_b_inner);
                    let (mut conn_a, mut conn_b) = (conn_a.unwrap(), conn_b.unwrap());

                    let message = vec![0xABu8; 1024 * 1024]; // 1MB
                    let start = Instant::now();
                    conn_a.send(message).await.unwrap();
                    let received: Vec<u8> = conn_b.recv().await.unwrap();
                    let elapsed = start.elapsed();

                    // Log throughput
                    let throughput_mbps = (1.0 / elapsed.as_secs_f64()) * 8.0;
                    println!(
                        "1MB @ {}MB/s limit, 10ms RTT: {:?} ({:.2} Mbps)",
                        bandwidth_limit_mb, elapsed, throughput_mbps
                    );

                    std_black_box(received);
                });
            },
        );

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

// Transport layer benchmarks - uses actual transport code with mock I/O
// This is the primary CI benchmark group for regression detection.
criterion_group!(
    name = transport;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(10))
        .noise_threshold(0.10)  // 10% - some async variance expected
        .significance_level(0.05);
    targets =
        transport_blackbox::bench_connection_establishment,
        transport_blackbox::bench_message_throughput,
);

// Streaming benchmarks - large message transfers with rate limiting
// Used to establish baseline before congestion control implementation
criterion_group!(
    name = streaming;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(10))
        .noise_threshold(0.10)  // 10% - async variance expected
        .significance_level(0.05);
    targets =
        transport_streaming::bench_stream_throughput,
        transport_streaming::bench_concurrent_streams,
        transport_streaming::bench_rate_limit_accuracy,
);

// LEDBAT Validation - Fast benchmarks with proper transfer sizes
// Tests LEDBAT behavior with 256KB and 1MB transfers (not misleading 64KB)
criterion_group!(
    name = ledbat_validation;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(10))
        .noise_threshold(0.15)
        .significance_level(0.05);
    targets =
        ledbat_validation::bench_large_transfer_validation,
        ledbat_validation::bench_1mb_transfer_validation,
        ledbat_validation::bench_congestion_256kb,
);

// Slow Start Validation - Measures cold-start vs warm throughput
// These benchmarks validate that slow start + cwnd enforcement improves
// uncongested throughput by allowing faster ramp-up from cold start
criterion_group!(
    name = slow_start;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(30))
        .sample_size(20)
        .noise_threshold(0.15)
        .significance_level(0.05);
    targets =
        slow_start_validation::bench_cold_start_throughput,
        slow_start_validation::bench_warm_connection_throughput,
        slow_start_validation::bench_cwnd_evolution,
        slow_start_validation::bench_high_bandwidth_throughput,
);

// Benchmark groups
//
// CI benchmarks (run on every PR):
// - transport: Full transport pipeline with mock sockets - actual code path testing
// - streaming: Large message transfers - baseline for congestion control improvements
//
// LEDBAT validation benchmarks (run when testing congestion control):
// - ledbat_validation: 256KB/1MB transfers with warmup - validates LEDBAT behavior
//   (Small 64KB benchmarks removed - too dominated by initial burst effects)
//
// Additional benchmarks (run locally or on-demand):
// - level0: Pure logic benchmarks (crypto, serialization) - deterministic, <2% noise
// - level1: Mock I/O benchmarks (channel throughput, routing) - ~5% noise from async
// - level2: Loopback socket benchmarks (UDP syscalls) - ~10% noise from kernel
// - level3: Stress tests (max send rate) - ~15% noise, high variance expected
// - slow_start: Cold-start vs warm throughput - validates slow start effectiveness
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
