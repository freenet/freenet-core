//! Streaming Buffer Benchmarks - Lock-free fragment reassembly
//!
//! **Level 0**: Pure logic benchmarks with no I/O. These measure atomic CAS
//! operations and memory allocations - completely deterministic.
//!
//! See `docs/architecture/transport/benchmarking/methodology.md` for the
//! benchmark level hierarchy.
//!
//! These benchmarks measure the performance of the `LockFreeStreamBuffer`
//! implementation which uses `OnceLock<Bytes>` for lock-free fragment storage.
//!
//! Based on spike validation (issue #1452, PR iduartgomez/freenet-core#204):
//! - Target insert throughput: >2,000 MB/s
//! - Target first-fragment latency: <30μs
//!
//! Run with: `cargo bench --bench transport_ci -- streaming_buffer`

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use std::sync::Arc;

use freenet::transport::peer_connection::streaming_buffer::{
    LockFreeStreamBuffer, FRAGMENT_PAYLOAD_SIZE,
};

/// Stream sizes to benchmark (in bytes)
const STREAM_SIZES: &[usize] = &[
    64 * 1024,       // 64 KB (~45 fragments)
    256 * 1024,      // 256 KB (~180 fragments)
    1024 * 1024,     // 1 MB (~720 fragments)
    4 * 1024 * 1024, // 4 MB (~2880 fragments)
];

/// Number of concurrent inserter threads
const THREAD_COUNTS: &[usize] = &[1, 2, 4, 8];

/// Benchmark single-threaded sequential fragment insertion
///
/// Measures the raw insert throughput without contention.
/// This is the baseline for lock-free performance.
pub fn bench_sequential_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("streaming_buffer/insert/sequential");

    for &size in STREAM_SIZES {
        group.throughput(Throughput::Bytes(size as u64));

        // Pre-create fragments
        let num_fragments = size.div_ceil(FRAGMENT_PAYLOAD_SIZE);
        let fragments: Vec<Bytes> = (0..num_fragments)
            .map(|i| Bytes::from(vec![i as u8; FRAGMENT_PAYLOAD_SIZE]))
            .collect();

        group.bench_with_input(BenchmarkId::new("in_order", size), &size, |b, &sz| {
            b.iter(|| {
                let buffer = LockFreeStreamBuffer::new(sz as u64);
                for (i, frag) in fragments.iter().enumerate() {
                    buffer.insert((i + 1) as u32, frag.clone()).unwrap();
                }
                black_box(buffer.is_complete());
            });
        });

        // Out-of-order insertion (reverse order)
        group.bench_with_input(BenchmarkId::new("reverse_order", size), &size, |b, &sz| {
            b.iter(|| {
                let buffer = LockFreeStreamBuffer::new(sz as u64);
                for (i, frag) in fragments.iter().enumerate().rev() {
                    buffer.insert((i + 1) as u32, frag.clone()).unwrap();
                }
                black_box(buffer.is_complete());
            });
        });
    }

    group.finish();
}

/// Benchmark concurrent fragment insertion
///
/// Measures lock-free performance under contention from multiple threads.
/// This simulates real-world scenarios where fragments arrive from multiple sources.
pub fn bench_concurrent_insert(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("streaming_buffer/insert/concurrent");

    // Use 256 KB for concurrent tests
    let size: usize = 256 * 1024;
    let num_fragments = size.div_ceil(FRAGMENT_PAYLOAD_SIZE);

    group.throughput(Throughput::Bytes(size as u64));

    for &threads in THREAD_COUNTS {
        // Pre-create fragments
        let fragments: Vec<Bytes> = (0..num_fragments)
            .map(|i| Bytes::from(vec![i as u8; FRAGMENT_PAYLOAD_SIZE]))
            .collect();

        group.bench_with_input(
            BenchmarkId::new("threads", threads),
            &threads,
            |b, &thread_count| {
                b.to_async(&rt).iter(|| {
                    let buffer = Arc::new(LockFreeStreamBuffer::new(size as u64));
                    let frags = fragments.clone();

                    async move {
                        let fragments_per_thread = num_fragments / thread_count;
                        let mut handles = Vec::new();

                        for t in 0..thread_count {
                            let buf = Arc::clone(&buffer);
                            let thread_frags = frags.clone();
                            let start = t * fragments_per_thread;
                            let end = if t == thread_count - 1 {
                                num_fragments
                            } else {
                                start + fragments_per_thread
                            };

                            handles.push(tokio::spawn(async move {
                                for i in start..end {
                                    buf.insert((i + 1) as u32, thread_frags[i].clone()).unwrap();
                                }
                            }));
                        }

                        for handle in handles {
                            handle.await.unwrap();
                        }

                        black_box(buffer.is_complete());
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark fragment assembly (concatenation)
///
/// Measures the time to assemble all fragments into a contiguous buffer.
pub fn bench_assemble(c: &mut Criterion) {
    let mut group = c.benchmark_group("streaming_buffer/assemble");

    for &size in STREAM_SIZES {
        group.throughput(Throughput::Bytes(size as u64));

        // Pre-create and populate buffer
        let num_fragments = size.div_ceil(FRAGMENT_PAYLOAD_SIZE);
        let buffer = LockFreeStreamBuffer::new(size as u64);
        for i in 0..num_fragments {
            buffer
                .insert(
                    (i + 1) as u32,
                    Bytes::from(vec![i as u8; FRAGMENT_PAYLOAD_SIZE]),
                )
                .unwrap();
        }

        group.bench_with_input(BenchmarkId::new("complete", size), &buffer, |b, buf| {
            b.iter(|| {
                let assembled = buf.assemble();
                black_box(assembled);
            });
        });
    }

    group.finish();
}

/// Benchmark contiguous iteration
///
/// Measures the performance of iterating over contiguous fragments.
pub fn bench_iter_contiguous(c: &mut Criterion) {
    let mut group = c.benchmark_group("streaming_buffer/iter_contiguous");

    for &size in STREAM_SIZES {
        group.throughput(Throughput::Bytes(size as u64));

        // Pre-create and populate buffer
        let num_fragments = size.div_ceil(FRAGMENT_PAYLOAD_SIZE);
        let buffer = LockFreeStreamBuffer::new(size as u64);
        for i in 0..num_fragments {
            buffer
                .insert(
                    (i + 1) as u32,
                    Bytes::from(vec![i as u8; FRAGMENT_PAYLOAD_SIZE]),
                )
                .unwrap();
        }

        group.bench_with_input(BenchmarkId::new("full", size), &buffer, |b, buf| {
            b.iter(|| {
                let mut total = 0usize;
                for frag in buf.iter_contiguous() {
                    total += frag.len();
                }
                black_box(total);
            });
        });
    }

    group.finish();
}

/// Benchmark first-fragment latency
///
/// Measures the time from receiving the first fragment to it being available.
/// This is critical for streaming applications.
pub fn bench_first_fragment_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("streaming_buffer/latency");

    // Small message - single fragment
    let single_frag = Bytes::from(vec![0xABu8; 1024]);
    group.bench_function("first_fragment_1kb", |b| {
        b.iter(|| {
            let buffer = LockFreeStreamBuffer::new(1024);
            buffer.insert(1, single_frag.clone()).unwrap();
            black_box(buffer.get(1));
        });
    });

    // Full fragment
    let full_frag = Bytes::from(vec![0xABu8; FRAGMENT_PAYLOAD_SIZE]);
    group.bench_function("first_fragment_full", |b| {
        b.iter(|| {
            let buffer = LockFreeStreamBuffer::new(FRAGMENT_PAYLOAD_SIZE as u64);
            buffer.insert(1, full_frag.clone()).unwrap();
            black_box(buffer.get(1));
        });
    });

    group.finish();
}

/// Benchmark duplicate insert handling
///
/// Measures the performance of the idempotent duplicate rejection.
pub fn bench_duplicate_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("streaming_buffer/duplicate");

    let frag = Bytes::from(vec![0xABu8; FRAGMENT_PAYLOAD_SIZE]);

    // Pre-populate buffer
    let buffer = LockFreeStreamBuffer::new((FRAGMENT_PAYLOAD_SIZE * 10) as u64);
    for i in 1..=10 {
        buffer.insert(i, frag.clone()).unwrap();
    }

    group.bench_function("duplicate_insert", |b| {
        b.iter(|| {
            // Try to insert duplicates
            for i in 1..=10 {
                let _ = black_box(buffer.insert(i, frag.clone()));
            }
        });
    });

    group.finish();
}

/// Benchmark buffer creation overhead
///
/// Measures the time to create and initialize a buffer.
pub fn bench_buffer_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("streaming_buffer/creation");

    for &size in STREAM_SIZES {
        group.bench_with_input(BenchmarkId::new("new", size), &size, |b, &sz| {
            b.iter(|| {
                let buffer = LockFreeStreamBuffer::new(sz as u64);
                black_box(buffer.total_fragments());
            });
        });
    }

    group.finish();
}
