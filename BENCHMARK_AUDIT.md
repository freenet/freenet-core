# Benchmark Audit: What Exists vs What Runs in CI

## Summary: The Missing Throughput Benchmarks

**You're absolutely correct** - there ARE throughput benchmarks, but they're **not run in CI**.

## What EXISTS (But Doesn't Run in CI)

### 1. **Stream Throughput** (`transport/streaming.rs`)
```rust
bench_stream_throughput()  // 4KB, 16KB, 64KB streaming transfers
bench_concurrent_streams() // Multiple concurrent streams
```
**What it measures**: Large message fragmentation/reassembly (multi-packet)
**Why excluded from CI**: "Long measurement times (10s per benchmark)" (transport_ci.rs:16)

### 2. **Warm vs Cold Start** (`transport/slow_start.rs`)
```rust
bench_cold_start_throughput()      // Fresh connection, measures slow start
bench_warm_connection_throughput() // Pre-warmed connection
bench_cwnd_evolution()             // Congestion window growth
```
**What it measures**: Throughput on warm connections (what you care about!)
**Transfer sizes**: 4KB, 16KB, 64KB, 256KB
**Why excluded from CI**: "Very long (30s measurement time)" (transport_ci.rs:18)

### 3. **Large Transfer Validation** (`transport/ledbat_validation.rs`)
```rust
bench_large_transfer_validation() // Up to 256KB transfers
bench_1mb_transfer_validation()   // 1MB transfers
```
**What it measures**: Sustained large transfers over LEDBAT
**Why excluded from CI**: "Large transfers (256KB, 1MB)" (transport_ci.rs:17)

### 4. **Max Send Rate** (`transport/level3.rs`)
```rust
bench_max_send_rate() // 10,000 packets as fast as possible
```
**What it measures**: Maximum UDP send rate (raw throughput)
**Why excluded from CI**: Level 3 = stress tests, high variance

### 5. **Manual Throughput** (`transport/manual_throughput.rs`)
```rust
async fn bench_throughput(message_size, iterations, rtt_delay)
```
**What it measures**: Sustained throughput over multiple iterations
**Transfer sizes**: 1KB, 4KB, 16KB, 32KB
**Why excluded from CI**: Manual tests, not in criterion suite

## What RUNS in CI (`transport_ci.rs`)

### Currently in CI:
1. ❌ **`bench_message_throughput`** - Small messages (64-1364 bytes) **with connection setup**
2. ❌ **`bench_channel_throughput`** - Just async channels (not transport)
3. ❌ **Level 0 micro-benchmarks** - AES, serialization, nonce generation
4. ✅ **`bench_connection_establishment`** - Connection setup time (relevant)
5. ✅ **`streaming_buffer_ci`** - Lock-free buffer ops (relevant)

## The Problem

**From `transport_ci.rs:14-18`:**
```rust
//! Not included (too slow or noisy for CI):
//! - level2/level3: Real sockets, kernel-dependent
//! - streaming: Long measurement times (10s per benchmark)
//! - ledbat_validation: Large transfers (256KB, 1MB)
//! - slow_start: Very long (30s measurement time)
```

**Translation**: The benchmarks that measure what you care about (sustained throughput) are excluded because they're "too slow or noisy"!

## Comparison Matrix

| Benchmark | Measures | In CI? | Measures Max Throughput? | Why Excluded |
|-----------|----------|--------|-------------------------|--------------|
| `bench_stream_throughput` | 4-64KB streaming | ❌ | ✅ YES | "Long measurement times" |
| `bench_warm_connection_throughput` | Warm connection transfers | ❌ | ✅ YES | "Very long (30s)" |
| `bench_1mb_transfer_validation` | 1MB transfers | ❌ | ✅ YES | "Large transfers" |
| `bench_max_send_rate` | Max UDP rate | ❌ | ✅ YES | "High variance" |
| `bench_message_throughput` | 64-1364 byte msgs + setup | ✅ | ❌ NO | Measures latency, not throughput |
| `bench_aes_gcm_encrypt` | AES encryption | ✅ | ❌ NO | Component micro-benchmark |

## Key Finding

**The benchmarks that actually measure sustained throughput exist, but are deliberately excluded from CI.**

**Reasons given:**
1. "Too slow" (10-30s measurement time)
2. "Too noisy" (15% variance)
3. "Large transfers"

**But**: If you care about max theoretical throughput, these are EXACTLY what you should measure!

## What We Should Do

### Option 1: Add Existing Benchmarks to CI (Quick Win)

Add `bench_warm_connection_throughput` to CI with relaxed settings:

```rust
// In transport_ci.rs
use transport::slow_start::bench_warm_connection_throughput;

criterion_group!(
    name = warm_throughput_ci;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(3))
        .measurement_time(Duration::from_secs(15))  // Was 30s, reduce for CI
        .sample_size(10)  // Fewer samples
        .noise_threshold(0.20)  // 20% - accept higher variance
        .significance_level(0.05);
    targets = bench_warm_connection_throughput  // ALREADY EXISTS!
);
```

**Impact**:
- Measures what you care about (warm connection throughput)
- Uses existing, battle-tested code
- ~2 minutes added to CI runtime

### Option 2: Run Existing Benchmarks Locally, Not in CI

Accept that:
- CI measures micro-benchmarks for regression detection
- Developers run `cargo bench --bench transport_full` locally for throughput
- CI is for catching obvious regressions, not measuring max throughput

### Option 3: Replace CI Benchmarks Entirely

Remove micro-benchmarks from CI, add throughput:

```rust
criterion_main!(
    warm_throughput_ci,     // Use existing bench_warm_connection_throughput
    connection_setup_ci,    // Keep: cold-start time matters
    streaming_buffer_ci,    // Keep: critical path
);
```

## Recommended Action

**Try Option 1 first:**

1. Add `bench_warm_connection_throughput` to `transport_ci.rs`
2. Reduce measurement time from 30s → 15s
3. Increase noise threshold to 20%
4. Run on a few PRs to see if it's stable enough

**Code change:**
```diff
// transport_ci.rs
+use transport::slow_start::bench_warm_connection_throughput;

+criterion_group!(
+    name = warm_throughput_ci;
+    config = Criterion::default()
+        .warm_up_time(Duration::from_secs(3))
+        .measurement_time(Duration::from_secs(15))
+        .noise_threshold(0.20)
+        .significance_level(0.05);
+    targets = bench_warm_connection_throughput
+);

 criterion_main!(
     allocation_ci,
     level0_ci,
     level1_ci,
     transport_ci,
+    warm_throughput_ci,  // NEW!
     streaming_buffer_ci,
     streaming_buffer_concurrent_ci
 );
```

**Expected outcome:**
- CI now measures actual sustained throughput
- Detects real regressions (10 MB/s → 8 MB/s)
- Higher noise (20%) but measures what matters

## Files to Review

Existing benchmark implementations:
1. **`crates/core/benches/transport/slow_start.rs`** - Warm/cold throughput (ALREADY EXISTS!)
2. **`crates/core/benches/transport/streaming.rs`** - Stream throughput (ALREADY EXISTS!)
3. **`crates/core/benches/transport/ledbat_validation.rs`** - Large transfers (ALREADY EXISTS!)
4. **`crates/core/benches/transport_full.rs`** - Full suite configuration
5. **`crates/core/benches/transport_ci.rs`** - CI suite (what we'd modify)

## Bottom Line

**You don't need to write new benchmarks** - they already exist!

You just need to **add existing throughput benchmarks to the CI suite** with appropriate settings:
- Longer measurement time (15-20s)
- Higher noise threshold (15-20%)
- Fewer samples (10 instead of 100)

The code is already there, battle-tested, and documented. It's just not run in CI because someone decided it was "too slow or noisy."
