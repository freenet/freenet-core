# CI Benchmark Guide

**For local benchmarking methodology, see [methodology.md](methodology.md)**

## Overview

This guide explains how to interpret CI benchmark results and handle regression warnings. The CI system runs a subset of benchmarks (`transport_ci`) on PRs that touch transport code.

## What We Measure

### Benchmark Suites

The `transport_ci` benchmark suite includes:

1. **allocation_ci** (2% noise threshold)
   - Packet allocation
   - Fragmentation
   - Packet preparation

2. **level0_ci** (2% noise threshold)
   - AES-GCM encryption/decryption
   - Nonce generation
   - Serialization
   - Packet creation

3. **level1_ci** (5% noise threshold)
   - Channel throughput with async runtime

4. **transport_ci** (10% noise threshold)
   - Connection establishment
   - Message throughput (full pipeline)

5. **streaming_buffer_ci** (2% noise threshold)
   - Sequential insert
   - Assembly
   - Iteration
   - First fragment latency
   - Duplicate insert
   - Buffer creation

6. **streaming_buffer_concurrent_ci** (5% noise threshold)
   - Concurrent insert operations

### Noise Thresholds

Different benchmarks have different expected variance:
- **2%**: Deterministic operations (crypto, serialization, lock-free data structures)
- **5%**: Async operations with some runtime variance
- **10%**: Full transport pipeline with mock I/O

**For running benchmarks locally, see [methodology.md](methodology.md).**

## Interpreting CI Results

### Baseline Comparison

The CI system compares your PR against a baseline from the `main` branch:

1. **Exact match**: Your PR is based on the latest main commit that ran benchmarks
2. **Restored from fallback**: Your PR uses an older baseline from main

**Critical**: If you see "Restored from fallback", check the baseline age in the workflow summary. Recent PRs may have improved performance on main, making your PR appear "regressed" even though you didn't change anything.

### False Positives

Common causes of false benchmark regressions:

#### 1. **Stale Baseline**
- **Symptom**: PR shows regression but you didn't touch performance-critical code
- **Cause**: Recent commits on main improved performance; your PR doesn't have those improvements yet
- **Fix**: Merge or rebase your PR with the latest main and re-run benchmarks

#### 2. **GitHub Runner Variance**
- **Symptom**: Benchmarks run on same commit show different results
- **Cause**: GitHub-hosted runners (`ubuntu-latest`) have variable CPU contention
- **Note**: We use `ubuntu-latest` because the self-hosted runner needs updates (missing liblzma-dev)
- **Mitigation**: Noise thresholds are set conservatively; significance level is 0.01-0.05

#### 3. **Async Runtime Variance**
- **Symptom**: `level1_ci` or `transport_ci` benchmarks show small regressions (~5%)
- **Cause**: Tokio scheduling variance on shared hardware
- **Mitigation**: These benchmarks have higher noise thresholds (5-10%)

### Verifying Real Regressions

If you suspect a real regression:

1. **Check baseline age**: Look at "Baseline Information" in the workflow summary
2. **Merge main**: Bring your branch up-to-date with main
3. **Run locally**: Use `cargo bench` to establish a local baseline and verify
4. **Profile**: Use `cargo flamegraph` or `perf` to identify hotspots

## Benchmark Results Format

The CI system generates three files (available in workflow artifacts):

### 1. `bench_results.json`
Structured JSON output for programmatic analysis:
```json
{
  "results": [
    {
      "name": "level0/crypto/encrypt/aes128gcm/64",
      "time_estimate": "775.44 ns",
      "change_percent": "+2.34%",
      "change_type": "regression",
      "confidence": "p = 0.00 < 0.01",
      "throughput": "82.5 MiB/s"
    }
  ],
  "summary": {
    "total": 42,
    "regressions": 3,
    "improvements": 2,
    "no_change": 37
  }
}
```

### 2. `bench_summary.md`
Markdown summary with tables of regressions and improvements

### 3. `bench_pr_comment.md`
PR comment text (posted automatically if regressions detected)

## When Reviewing PRs with Benchmark Warnings

1. **Check baseline age**: Look for "Restored from fallback" warning in workflow summary
2. **Verify changes**: Do the reported regressions relate to changed code?
3. **Consider variance**: Small changes (<5%) on async benchmarks may be noise
4. **Request investigation**: Ask author to merge main and re-run if uncertain

## Troubleshooting CI Benchmark Issues

### "No baseline found" warning

**Cause**: First benchmark run for this main branch commit, or cache evicted

**Impact**: All benchmarks show as "new" with no comparison data

**Action**: Wait for next commit on main to establish baseline

### CI Runner Variance

**Symptom**: Same code shows different results across runs

**Cause**: GitHub-hosted `ubuntu-latest` runners have variable CPU resources

**Impact**: Noise thresholds are set conservatively (2-10%) to account for this

**Action**: Re-run workflow if results seem suspicious; check baseline age

## Implementation Details

### Baseline Storage

- **Location**: GitHub Actions cache with key `criterion-baseline-main-{OS}-{SHA}`
- **Lifetime**: 7 days (GitHub Actions cache eviction policy)
- **Size**: ~50-100 MB (includes Criterion HTML reports)

### Parser Script

The `scripts/parse_bench_output.py` script parses Criterion output and generates structured reports. It:

1. Extracts benchmark names, times, and change percentages
2. Identifies regressions, improvements, and no-change results
3. Generates markdown tables sorted by severity
4. Creates JSON output for automation

### Workflow Triggers

Benchmarks run when:
- Push to `main` branch (if transport/** or benches/** changed)
- Pull request (if transport/** or benches/** changed)
- Manual workflow dispatch (any branch)

## Potential CI Improvements

Future enhancements being considered:

1. **Self-hosted runner**: Migrate from `ubuntu-latest` to self-hosted for consistent hardware
2. **Better baseline strategy**: Use merge-base commit or store baseline history
3. **Baseline age check**: Warn or skip comparison if baseline is >7 days old
4. **Historical tracking**: Store benchmark history for trend analysis
5. **Benchmark dashboard**: Web UI showing performance trends over time
