# Proposed: Sustained Throughput Benchmark

## What We Should Measure for Max Theoretical Throughput

Instead of:
- ❌ Micro-benchmarks (AES encryption, serialization)
- ❌ Single small messages with connection overhead
- ❌ Per-message latency

Measure:
- ✅ **Sustained bulk transfer** (multi-MB)
- ✅ **Warm connection** (connection setup not part of measurement)
- ✅ **Bytes/second over time** (not latency per message)
- ✅ **Realistic payload sizes** (not 64 bytes)

## Proposed CI Benchmark

```rust
/// Measure sustained throughput over a warm connection
///
/// This is the PRIMARY benchmark for max theoretical throughput.
/// Tests how much data can be pushed through the pipeline per second.
pub fn bench_sustained_throughput_ci(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("transport/sustained_throughput");

    // Conservative settings for CI stability
    group.sample_size(10);  // Fewer samples, each measures more data
    group.measurement_time(Duration::from_secs(15));  // Longer = more stable
    group.warm_up_time(Duration::from_secs(3));
    group.noise_threshold(0.15);  // 15% - realistic for sustained async throughput

    // Test realistic payload sizes
    for &chunk_size in &[16 * 1024, 64 * 1024, 256 * 1024] {  // 16KB, 64KB, 256KB chunks
        let total_transfer = 10 * 1024 * 1024;  // 10 MB total per iteration
        let num_chunks = total_transfer / chunk_size;

        group.throughput(Throughput::Bytes(total_transfer as u64));

        group.bench_with_input(
            BenchmarkId::new("chunk", chunk_size),
            &(chunk_size, num_chunks),
            |b, &(sz, count)| {
                b.to_async(&rt).iter_batched(
                    || {
                        // Setup: Create connection ONCE per batch (not per iteration)
                        (sz, count)
                    },
                    |(chunk_size, num_chunks)| async move {
                        // Create and connect peers (outside measurement)
                        let channels = Arc::new(DashMap::new());
                        let (peer_a_pub, mut peer_a, peer_a_addr) =
                            create_mock_peer(PacketDropPolicy::ReceiveAll, channels.clone())
                                .await.unwrap();
                        let (peer_b_pub, mut peer_b, peer_b_addr) =
                            create_mock_peer(PacketDropPolicy::ReceiveAll, channels)
                                .await.unwrap();

                        let (conn_a, conn_b) = futures::join!(
                            peer_a.connect(peer_b_pub, peer_b_addr),
                            peer_b.connect(peer_a_pub, peer_a_addr),
                        );
                        let (mut conn_a, mut conn_b) = (conn_a.unwrap(), conn_b.unwrap());

                        // Warmup: Send one chunk to establish pipeline state
                        conn_a.send(vec![0xAB; chunk_size]).await.unwrap();
                        let _: Vec<u8> = conn_b.recv().await.unwrap();

                        // === MEASURED SECTION ===
                        // Spawn receiver task (simulate real-world concurrent recv)
                        let recv_task = tokio::spawn(async move {
                            for _ in 0..num_chunks {
                                let _: Vec<u8> = conn_b.recv().await.unwrap();
                            }
                            conn_b  // Return to keep alive
                        });

                        // Send chunks as fast as possible
                        for _ in 0..num_chunks {
                            conn_a.send(vec![0xAB; chunk_size]).await.unwrap();
                        }

                        // Wait for all receives to complete
                        recv_task.await.unwrap();

                        std_black_box(());
                        // === END MEASURED SECTION ===
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }

    group.finish();
}
```

## Why This Is Better

1. **Measures what matters**: Sustained MB/s, not latency per tiny message
2. **Realistic**: Connection already warm, multiple chunks in flight
3. **Stable**: Long measurement time (15s) smooths out async variance
4. **Detects real regressions**: If throughput drops from 10 MB/s to 8 MB/s, that's meaningful

## Configuration for CI

```rust
criterion_group!(
    name = throughput_ci;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(3))
        .measurement_time(Duration::from_secs(15))  // Longer for stability
        .noise_threshold(0.15)  // 15% - realistic for sustained transfer
        .significance_level(0.05);
    targets = bench_sustained_throughput_ci
);
```

## What to Remove from CI

If adding this sustained throughput benchmark, consider removing:

1. **Level 0 micro-benchmarks** (`allocation_ci`, `level0_ci`)
   - AES, serialization, etc. are too stable and don't correlate with real throughput
   - If AES regresses 5%, does it matter if overall throughput is unchanged?

2. **Single-message latency** (`bench_message_throughput`)
   - Including connection setup makes it measure the wrong thing
   - If you care about latency, measure connection establishment separately

3. **Keep**:
   - `streaming_buffer_ci`: Lock-free buffer is critical path for streaming
   - `bench_connection_establishment`: Cold-start time matters for user experience
   - **NEW**: `bench_sustained_throughput_ci`: What you actually care about

## Expected Characteristics

With 10 MB/s default rate limit:
- **Expected throughput**: ~10 MB/s (limited by token bucket)
- **Variance**: 10-15% (async runtime scheduling)
- **Noise threshold**: 15% appropriate
- **Measurement time**: 15s = 150 MB transferred per sample
- **Stability**: Much more stable than per-message latency

If throughput regresses from 10 MB/s → 8.5 MB/s (15% drop), that's a REAL regression.
If AES encryption regresses 5% but throughput unchanged, it's irrelevant.
