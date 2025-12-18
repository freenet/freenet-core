# Transport Benchmark Baseline (December 2024)

**Date:** 2024-12-17
**Branch:** perf/congestion-control
**Commit:** 689e6e7d (docs: on_loss() design decision)
**System:** MacBook Pro (M-series)

## transport_ci Results

The CI benchmark suite tests the fast, deterministic subset of benchmarks.

### Level 0: Pure Logic Benchmarks

| Benchmark | Size | Time | Throughput |
|-----------|------|------|------------|
| **Encrypt (AES-128-GCM)** | 64 B | ~1.4 µs | ~43 MiB/s |
| | 256 B | ~1.9 µs | ~130 MiB/s |
| | 1024 B | ~5.2 µs | ~188 MiB/s |
| | 1364 B | ~6.1 µs | ~213 MiB/s |
| **Decrypt (AES-128-GCM)** | 64 B | ~1.3 µs | ~47 MiB/s |
| | 256 B | ~1.9 µs | ~132 MiB/s |
| | 1024 B | ~10.2 µs | ~96 MiB/s |
| | 1364 B | ~27 µs | ~48 MiB/s |
| **Nonce Generation** | random | varies | - |
| | counter | varies | - |
| **Serialization** | 64 B | ~50 ns | - |
| | 1364 B | ~50 ns | - |
| **Packet Creation** | 64 B | ~2.1 µs | - |
| | 1364 B | ~7.1 µs | - |

### Level 1: Mock I/O Benchmarks

| Benchmark | Buffer Size | Time | Throughput |
|-----------|-------------|------|------------|
| **Channel Throughput** | 1 msg | ~346 ms | ~2.9 Kelem/s |
| | 10 msg | ~35 ms | ~28.6 Kelem/s |
| | 100 msg | ~3.6 ms | ~277 Kelem/s |
| | 1000 msg | ~2.0 ms | ~494 Kelem/s |

### Transport: Integration Benchmarks

#### Cold Start (Connection Per Iteration)

| Benchmark | Payload | Time | Throughput |
|-----------|---------|------|------------|
| **Connection Establish** | - | ~243 ms | - |
| **Message Throughput** | 64 B | ~244 ms | ~262 B/s |
| | 256 B | ~248 ms | ~1.0 KiB/s |
| | 1024 B | ~237 ms | ~4.2 KiB/s |
| | 1364 B | ~245 ms | ~5.4 KiB/s |

#### Warm Connection (Reused Connection)

**Updated 2024-12-18** - Connection reuse fix implemented.

| Benchmark | Payload | Time | Throughput | Speedup vs Cold |
|-----------|---------|------|------------|-----------------|
| **Warm Connection** | 1 KB | ~1.66 ms | ~603 KiB/s | **137x** |

**Why warm connection throughput is much higher:**
- Cold start includes ~220ms connection establishment overhead
- Warm connection measures pure transfer throughput only
- At 1KB message size, per-message overhead (encryption, serialization) still matters

**Why throughput appears lower than 10 MB/s rate limit:**
- Small messages (1KB) have high per-message overhead vs payload
- Mock transport has specific characteristics (instant RTT)
- 64KB+ transfers cause criterion timeout issues (needs investigation)
- Real throughput testing requires larger sustained transfers

## Notes

1. **Regression warnings are expected** - this establishes a new baseline after benchmark reorganization
2. **High variance in some tests** - due to mock I/O and async runtime overhead
3. **Connection establishment dominates** - explains low throughput for small messages
4. **Crypto performance** - AES-GCM is fast for all payload sizes

## Running These Benchmarks

```bash
# CI subset (~8 min)
cargo bench --bench transport_ci --features bench

# LEDBAT validation (~60 min)
cargo bench --bench transport_ledbat --features bench

# Full suite (~78 min, requires bench_full feature)
cargo bench --bench transport_full --features bench,bench_full
```

## Comparison with Previous Baseline

| Metric | Week 0 (3 MB/s limit) | Current (LEDBAT) | Change |
|--------|----------------------|------------------|--------|
| Rate limit | Fixed 3 MB/s | Token bucket 10 MB/s | +233% |
| Initial window | N/A | IW26 (38 KB) | New |
| Slow start | None | RFC 6817 | New |
| cwnd enforcement | None | Active | New |

## See Also

- [Benchmark Methodology](methodology.md)
- [Performance Comparison](../analysis/performance-comparison.md)
- [LEDBAT Design](../design/ledbat-slow-start.md)
