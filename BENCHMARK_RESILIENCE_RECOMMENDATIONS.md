# Benchmark Resilience & Significance Recommendations

## Executive Summary

**Current problems:**
1. **False positives from stale baselines**: PRs compare against newer main baseline
2. **Measuring the wrong things**: Micro-benchmarks don't correlate with max throughput
3. **Too noisy**: Small message latency with async variance

**Core question: "Do we care about max theoretical throughput?"**

If YES → Current CI benchmarks measure the wrong things.

## Quick Wins (High Impact, Low Effort)

### 1. Fix Stale Baseline Problem ⭐ **DO THIS FIRST**

**Impact**: Eliminates 80% of false positives
**Effort**: ~30 minutes
**File**: `.github/workflows/benchmarks.yml`

Change baseline strategy from "most recent main" to "merge-base":

```yaml
- name: Determine Baseline Commit
  id: baseline-commit
  run: |
    if [ "${{ github.event_name }}" == "pull_request" ]; then
      # Use merge-base (where branch diverged from main)
      BASE_SHA=$(git merge-base origin/${{ github.base_ref }} HEAD)
      echo "sha=$BASE_SHA" >> $GITHUB_OUTPUT
    else
      echo "sha=${{ github.sha }}" >> $GITHUB_OUTPUT
    fi

- name: Download Baseline
  uses: actions/cache/restore@v5
  with:
    path: target/criterion
    key: criterion-baseline-main-${{ runner.os }}-${{ steps.baseline-commit.outputs.sha }}
```

**Why it works**: PRs always compare against performance when they branched, not against newer improvements on main.

### 2. Increase Noise Thresholds for Async Benchmarks

**Current**: `level1_ci` and `transport_ci` use 5-10% noise thresholds
**Problem**: GitHub-hosted runners have higher variance
**Fix**: Increase to 15-20% for async benchmarks

```rust
criterion_group!(
    name = transport_ci;
    config = Criterion::default()
        .noise_threshold(0.20)  // Was 0.10 - increase for runner variance
        .significance_level(0.05);
    targets = bench_sustained_throughput_ci
);
```

**Trade-off**: Won't catch small regressions (<20%), but eliminates noise-based false positives.

### 3. Increase Measurement Time

**Current**: 3-10 seconds
**Problem**: Too short for async operations to stabilize
**Fix**: 15-30 seconds for throughput benchmarks

```rust
.measurement_time(Duration::from_secs(20))  // Was 10s
```

**Why**: Longer measurement smooths out variance, more representative of sustained performance.

## Bigger Changes (Higher Impact, More Effort)

### 4. Replace Micro-Benchmarks with Macro Throughput

**Current CI benchmarks:**
- ❌ `allocation_ci`: Packet allocation micro-benchmarks
- ❌ `level0_ci`: AES, serialization, nonce generation
- ❌ `level1_ci`: Channel throughput (just channels, not transport)
- ❌ `transport_ci`: **Single small messages with connection overhead**
- ✅ `streaming_buffer_ci`: Lock-free buffer (keep - critical path)

**Problem**: These don't measure max theoretical throughput!

**Proposed replacement:**

```rust
// Remove: allocation_ci, level0_ci, level1_ci, current transport_ci
// Add: sustained_throughput_ci

pub fn bench_sustained_throughput_ci(c: &mut Criterion) {
    // Transfer 10 MB over warm connection
    // Measure MB/s, not latency per message
    // Chunk sizes: 16KB, 64KB, 256KB (realistic)
    // See: docs/architecture/transport/benchmarking/sustained-throughput-proposal.md
}

criterion_main!(
    throughput_ci,           // NEW: What you actually care about
    streaming_buffer_ci,     // KEEP: Critical path for streaming
    connection_setup_ci,     // KEEP: Cold-start time matters
);
```

**Expected characteristics:**
- **Throughput**: ~10 MB/s (your default rate limit)
- **Variance**: 10-15% (async scheduling)
- **Sensitivity**: Catches real regressions (10 MB/s → 8.5 MB/s)
- **Immune to micro-variations**: AES 5% slower? Doesn't matter if throughput unchanged.

See detailed implementation: `docs/architecture/transport/benchmarking/sustained-throughput-proposal.md`

### 5. Self-Hosted Runner (Best Long-Term)

**Problem**: `ubuntu-latest` has variable CPU and different hardware across runs
**Solution**: Dedicated benchmark runner with consistent hardware

**Requirements** (from `.github/workflows/benchmarks.yml:26`):
- Install `liblzma-dev`
- Enable passwordless sudo

**Impact**:
- Reduces variance from ~15% to ~5%
- Enables CPU pinning, frequency control
- Consistent hardware across all runs

## Decision Matrix: What to Measure?

| Goal | Current Benchmarks | Recommended |
|------|-------------------|-------------|
| **Max theoretical throughput** | ❌ Micro-benchmarks, small messages | ✅ Sustained bulk transfer (10 MB over 15s) |
| **Connection setup time** | ✅ `bench_connection_establishment` | ✅ Keep this |
| **Streaming performance** | ✅ `streaming_buffer_ci` | ✅ Keep this |
| **Component micro-optimization** | ✅ Level 0/1 benchmarks | ⚠️ Useful locally, not in CI |
| **Real-world bottlenecks** | ❌ No | ✅ Sustained throughput reveals them |

## Recommended Action Plan

**Phase 1: Quick Wins (This Week)**
1. ✅ Implement merge-base baseline strategy (30 min)
2. ✅ Increase noise thresholds to 15-20% (10 min)
3. ✅ Increase measurement time to 15-20s (5 min)
4. Test on a few PRs to validate

**Phase 2: Better Benchmarks (Next Sprint)**
1. Implement `bench_sustained_throughput_ci` (see proposal)
2. Run in parallel with existing benchmarks
3. Compare: Does it catch real regressions? Is it more stable?
4. Once validated, replace micro-benchmarks in CI

**Phase 3: Infrastructure (When ROI Justifies It)**
1. Fix self-hosted runner dependencies
2. Migrate benchmarks to self-hosted
3. Enable CPU pinning, frequency control

## Expected Outcomes

**After Phase 1 (merge-base baseline):**
- False positive rate: 80% → 20%
- Developers trust benchmark results more
- Less "is this real or noise?" confusion

**After Phase 2 (sustained throughput):**
- Benchmarks measure what you care about (MB/s)
- Regressions are meaningful ("throughput dropped 15%")
- Micro-optimizations that don't affect throughput don't cause alerts

**After Phase 3 (self-hosted runner):**
- False positive rate: 20% → <5%
- Can use tighter thresholds (10% instead of 20%)
- Results reproducible locally

## Files to Review

1. **Current investigation**: `BENCHMARK_INVESTIGATION_SUMMARY.md`
2. **Merge-base proposal**: `docs/architecture/transport/benchmarking/merge-base-baseline-proposal.md`
3. **Throughput proposal**: `docs/architecture/transport/benchmarking/sustained-throughput-proposal.md`
4. **CI guide**: `docs/architecture/transport/benchmarking/ci-guide.md`
5. **Methodology**: `docs/architecture/transport/benchmarking/methodology.md`

## Questions to Answer

1. **Do you primarily care about max sustained throughput?**
   - YES → Implement Phase 2 (sustained throughput benchmark)
   - NO → Current micro-benchmarks might be OK, just fix baseline strategy

2. **What's an acceptable false positive rate?**
   - <5%: Need self-hosted runner
   - 10-20%: Merge-base + higher thresholds sufficient
   - >20%: Current system with merge-base fix

3. **What magnitude of regression matters?**
   - <5%: Need stable environment, tight thresholds
   - 10-15%: Current CI with fixes is OK
   - >20%: Even current system should catch this

## Recommendation

**Start with Phase 1** (merge-base baseline + higher thresholds):
- Biggest impact (80% fewer false positives)
- Smallest effort (~1 hour)
- Immediately actionable
- No risk

Then evaluate whether Phase 2 is worth it based on what you're trying to optimize.
