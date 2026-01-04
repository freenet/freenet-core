# Benchmark Regression Investigation Summary

**Branch**: `claude/investigate-benchmark-regression-semcj`

## Problem Statement

The project has been consistently getting "Performance Benchmark Regressions Detected" warnings on PRs that don't change performance-related code. The benchmark summaries in GitHub Actions were not showing useful information about what changed.

## Investigation Findings

### 1. What's Being Measured

The `transport_ci` benchmark suite runs on PRs that touch:
- `crates/core/src/transport/**`
- `crates/core/benches/**`
- `.github/workflows/benchmarks.yml`

It measures:
- **allocation_ci**: Packet allocation, fragmentation, preparation (2% noise threshold)
- **level0_ci**: Crypto ops, serialization (2% noise threshold)
- **level1_ci**: Async channel throughput (5% noise threshold)
- **transport_ci**: Connection setup, message throughput (10% noise threshold)
- **streaming_buffer_ci**: Lock-free buffer operations (2% noise threshold)

### 2. Root Causes of False Positives

#### A. Stale Baseline Problem (MAJOR ISSUE)

**How baseline caching works:**
1. Each main branch commit creates a baseline with key: `criterion-baseline-main-{OS}-{SHA}`
2. PRs try to restore baseline with exact SHA match (almost never succeeds)
3. PRs fall back to `restore-keys: criterion-baseline-main-{OS}-` (finds most recent)

**The problem:**
- If commits A, B, C land on main and commit B improves performance
- A PR based on commit A compares against commit C's baseline
- The PR shows as "regressed" even though it didn't change anything
- **This is the primary cause of false positives**

#### B. GitHub Runner Variance

- Benchmarks run on `ubuntu-latest` (shared hardware)
- CPU contention varies between runs
- Comment in workflow says self-hosted runner needs updates (missing liblzma-dev)

#### C. Poor Reporting

- Original workflow used `grep "regressed"` to parse Criterion output
- JavaScript parsing in PR comment was fragile
- No structured output (JSON)
- No indication of baseline age
- No explanation of false positive causes

## Implemented Solutions

### 1. Enhanced Baseline Tracking

**File**: `.github/workflows/benchmarks.yml`

**Changes:**
- Added baseline metadata file (`.baseline_info`) saved with each baseline
- Enhanced "Report baseline status" step to show:
  - Whether exact match or fallback was used
  - Warning when fallback is used (indicates potential stale baseline)
  - Baseline age (last modified timestamp)
  - Baseline metadata (commit SHA, timestamp, workflow run)

**Impact**: Users can now see if they're comparing against a stale baseline

### 2. Structured Benchmark Parser

**File**: `scripts/parse_bench_output.py`

**Features:**
- Parses Criterion text output into structured data
- Generates three output files:
  1. `bench_results.json`: Machine-readable JSON
  2. `bench_summary.md`: Markdown tables for workflow summary
  3. `bench_pr_comment.md`: PR comment text with context

**Output includes:**
- Benchmark name, time, change percentage, confidence
- Sorted tables (worst regressions first)
- Clear categorization (regressions, improvements, no change)
- Summary statistics

**Example output:**
```markdown
## Benchmark Results Summary

- **Total benchmarks**: 42
- **Regressions**: 3 ⚠️
- **Improvements**: 2 🚀
- **No significant change**: 37

### ⚠️ Performance Regressions

| Benchmark | Time | Change | Confidence |
|-----------|------|--------|------------|
| `level0/crypto/encrypt/aes128gcm/256` | 1.2456 µs | **+18.567%** | p = 0.00 < 0.01 |
| `allocation_ci/fragmentation` | 257.89 ns | **+10.567%** | p = 0.00 < 0.01 |
| `transport/connection_establishment` | 125.67 µs | **+5.567%** | p = 0.02 < 0.05 |
```

### 3. Improved PR Comments

**Changes:**
- PR comments now explain false positive causes
- Provide actionable steps to verify real regressions
- Link to workflow summary for full details
- Include warning about stale baselines

**Example PR comment:**
```markdown
## ⚠️ Performance Benchmark Regressions Detected

Found 3 benchmark(s) with performance regressions:
- **`level0/crypto/encrypt/aes128gcm/256`**: +18.567%
- **`allocation_ci/fragmentation`**: +10.567%
- **`transport/connection_establishment`**: +5.567%

### ⚠️ Important: This may be a false positive!

**Common causes of false positives:**
1. **Stale baseline**: If recent PRs improved performance on `main`, this PR
   (which doesn't include those changes) will show as "regressed"
2. **GitHub runner variance**: Benchmarks run on shared runners
3. **Old baseline**: Check baseline age in workflow summary

**To verify if this is a real regression:**
1. Check if recent commits on `main` touched transport or benchmark code
2. Merge `main` into your branch and re-run benchmarks
3. Review the baseline age in the "Download main branch baseline" step
```

### 4. Documentation

**File**: `docs/BENCHMARKING.md`

**Contents:**
- Comprehensive guide to the benchmark system
- Explanation of what's being measured
- How to run benchmarks locally
- How to interpret CI results
- Troubleshooting guide for false positives
- Best practices for making performance changes

## Recommendations

### Immediate Actions (Implemented)

✅ Use structured parser for benchmark output
✅ Show baseline age and metadata in workflow summary
✅ Explain false positive causes in PR comments
✅ Document benchmark system

### Short-term Improvements (Not Implemented)

These would require more extensive changes:

1. **Fix self-hosted runner**
   - Install missing dependencies (liblzma-dev)
   - Enable passwordless sudo
   - Switch back from `ubuntu-latest` to `self-hosted`
   - **Impact**: More consistent hardware, fewer false positives

2. **Improve baseline strategy**
   - Option A: Store baseline history in artifact (not cache)
   - Option B: Always use merge-base commit as baseline
   - Option C: Run benchmarks on both PR and main, compare directly
   - **Impact**: Eliminate stale baseline problem

3. **Add baseline age check**
   - Fail with clear message if baseline is >7 days old
   - Skip regression detection and just show raw numbers
   - **Impact**: Prevent misleading comparisons

### Long-term Enhancements (Future Work)

1. **Historical tracking**: Store benchmark history in database
2. **Trend analysis**: Show performance trends over time
3. **Automatic bisection**: Use `git bisect` to find regression commit
4. **Flamegraph generation**: Auto-generate profiles for regressed benchmarks
5. **Dashboard**: Web UI showing benchmark trends

## Testing

The parser script was tested with sample Criterion output and correctly:
- Extracted benchmark names, times, and changes
- Categorized results (regression, improvement, no change)
- Generated structured JSON, markdown summary, and PR comment
- Handled edge cases (missing data, no baseline)

## Files Changed

1. `.github/workflows/benchmarks.yml`: Enhanced baseline tracking and reporting
2. `scripts/parse_bench_output.py`: New structured parser (executable)
3. `docs/BENCHMARKING.md`: Comprehensive documentation

## Next Steps

1. **Commit and push these changes**
2. **Test on a real PR** to verify improvements
3. **Consider short-term improvements** if false positives persist
4. **Monitor** for a few PRs to validate effectiveness

## Expected Impact

**Before:**
- PRs get vague "Performance has regressed" warnings
- No information about which benchmarks or by how much
- No explanation of why (often false positive)
- Users don't know how to interpret results

**After:**
- Clear tables showing exactly which benchmarks changed and by how much
- Warnings when baseline might be stale
- Explanation of false positive causes
- Actionable steps to verify real regressions
- Structured data for further analysis
- Comprehensive documentation

This should significantly reduce confusion and help developers distinguish real regressions from measurement artifacts.
