# Transport Layer Baseline (Week 0)

> **Historical Document** - This baseline was captured before implementing congestion control (LEDBAT). It represents the performance with a hardcoded 3 MB/s rate limit.

**Date:** 2025-12-14
**Branch:** perf/congestion-control
**Issue:** https://github.com/freenet/freenet-core/issues/2285
**Baseline saved:** `target/criterion-baseline-pre-cc/`

---

## Current Configuration (Week 0)

- **Rate Limit:** 3 MB/s (3,000,000 bytes/second)
- **Implementation:** Batch-window approach (10ms batches)
- **Location:** `crates/core/src/transport/peer_connection/outbound_stream.rs`

---

## Baseline Benchmark Results

### 1. Stream Throughput (rate_limited)

| Message Size | Time (ms) | Throughput | Notes |
|--------------|-----------|------------|-------|
| 4KB | 244 | 65.6 KiB/s | ~3 packets |
| 16KB | 244 | 65.6 KiB/s | ~12 packets |
| 64KB | 270 | 236.8 KiB/s | ~48 packets |

**Analysis:**
- Small messages (4KB, 16KB) show similar performance
- Larger messages (64KB) achieve ~3.6x better throughput
- Connection setup overhead dominates for small transfers
- Throughput much lower than 3 MB/s limit (includes handshake)

### 2. Concurrent Streams

| # Streams | Time (ms) | Notes |
|-----------|-----------|-------|
| 2 | 255 | 2× 16KB messages |
| 5 | 252 | 5× 16KB messages |
| 10 | 284 | 10× 16KB messages |

**Analysis:**
- Very consistent time (~250-280ms) regardless of stream count
- No obvious starvation or unfairness in current implementation
- Small increase (255ms → 284ms) when going 2→10 streams

### 3. Rate Limit Accuracy (1MB @ 3 MB/s)

| Metric | Value |
|--------|-------|
| Expected Time | ~333ms (1MB / 3MB/s) |
| Actual Time | 297ms (mean) |
| Throughput | 3.21 MiB/s (mean) |
| Deviation | -10.8% (faster than expected) |

**Analysis:**
- Rate limiting is working but slightly permissive
- Actual throughput: 3.21 MiB/s vs 3.0 MiB/s target
- Within acceptable tolerance for baseline
- Mock I/O may have less overhead than real network

---

## Expected Improvements After Congestion Control

**Target:** AIMD-based adaptive rate limiting with slow start

### Expected Improvements

#### 1. Stream Throughput

| Size | Baseline | Expected | Improvement |
|------|----------|----------|-------------|
| 4KB | 65 KiB/s | 500+ KiB/s | 7-10x faster |
| 16KB | 65 KiB/s | 1-2 MiB/s | 15-30x faster |
| 64KB | 237 KiB/s | 5-10 MiB/s | 20-40x faster |

**Rationale:**
- Slow start will probe available bandwidth
- Adaptive rate replaces fixed 3 MB/s ceiling
- Mock I/O has high available bandwidth (no kernel limit)

#### 2. Concurrent Streams

- Time should remain consistent
- Fairness index should improve if measured
- Token bucket will smooth bursts

#### 3. Rate Limit Accuracy

- Dynamic adaptation based on ACKs/loss
- Should match available bandwidth closely
- Better response to congestion

---

## Validation Plan

After implementing Phase 2 (AIMD congestion control):

1. **Re-run benchmarks:**
   ```bash
   cargo bench --bench transport_perf -- streaming
   ```

2. **Criterion will auto-compare to this baseline**

3. **Look for:**
   - ✓ 5-40x throughput improvement (depends on message size)
   - ✓ No regression in concurrent stream time
   - ✓ Adaptive rate matches available bandwidth

4. **Success criteria:**
   - ✓ Stream throughput >1 MiB/s for 16KB+ messages
   - ✓ Concurrent streams remain fair (CoV <0.5)
   - ✓ No panics or errors under load

---

## Notes

- These benchmarks use mock I/O (channels, not real UDP)
- Connection setup included in each iteration (overhead)
- Throughput appears lower than 3 MB/s due to handshake cost
- Real-world improvements may vary based on actual network conditions
- Baseline preserved in: `target/criterion-baseline-pre-cc/`

---

## See Also

- [Performance Comparison](../analysis/performance-comparison.md) - Actual Week 4 results vs this baseline
- [LEDBAT Design](../design/ledbat-slow-start.md) - The congestion control implementation
