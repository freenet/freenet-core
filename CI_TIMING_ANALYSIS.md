# CI Timing Analysis - freenet-core

**Analysis Date:** 2026-01-17
**Workflow:** `.github/workflows/ci.yml`
**Data Source:** Last 20 successful runs on main branch

## Executive Summary

CI times vary significantly based on trigger type:
- **Pull Request runs:** ~23-24 minutes (expensive, full test suite)
- **Merge queue runs:** ~1.5-2 minutes (optimized, fast checks only)
- **Main branch pushes:** ~1.5-2 minutes (optimized, fast checks only)

The Test job dominates PR runtime at **~22-23 minutes** (95% of total PR CI time).

## Detailed Timing Breakdown

### Pull Request Runs (Full Test Suite)

Based on 4 recent successful PR runs:

| Job | Average Time | % of Total | Runner |
|-----|--------------|------------|---------|
| **Test** | 22m 51s | 95.4% | ubicloud-standard-16 |
| six-peer-regression | 3m 59s | 16.6% | ubicloud-standard-16 |
| Clippy | 1m 13s | 5.1% | ubicloud-standard-8 |
| Fmt | 0m 17s | 1.2% | ubuntu-latest |
| Conventional Commits | 0m 10s | 0.7% | ubuntu-latest |

**Total PR runtime:** ~23-24 minutes (jobs run in parallel)

### Merge Queue Runs (Optimized)

| Job | Time | Runner |
|-----|------|---------|
| Clippy | 1m 27s | ubicloud-standard-8 |
| Fmt | 0m 17s | ubuntu-latest |
| Test | SKIPPED | - |
| six-peer-regression | SKIPPED | - |

**Total merge queue runtime:** ~1.5-2 minutes

### Main Branch Push Runs (Optimized)

| Job | Time | Runner |
|-----|------|---------|
| Clippy | 1m 09s | ubicloud-standard-8 |
| Fmt | 0m 16s | ubuntu-latest |
| Test | SKIPPED | - |
| six-peer-regression | SKIPPED | - |

**Total main push runtime:** ~1.5 minutes

## Update: Parallel Test Infrastructure Already Exists!

**Discovery:** The `#[freenet_test]` macro **already has full infrastructure for parallel testing!**

### Evidence:

1. **Thread-safe node allocation** (`crates/core/src/test_utils.rs:1048`):
   ```rust
   pub fn allocate_test_node_block(node_count: usize) -> usize {
       GLOBAL_NODE_INDEX.fetch_add(node_count, std::sync::atomic::Ordering::SeqCst)
   }
   ```
   - Uses atomic counter to allocate unique node index blocks per test
   - Completely thread-safe for parallel test execution

2. **Unique IP addresses per node** (`crates/core/src/test_utils.rs:1061`):
   ```rust
   pub fn test_ip_for_node(node_idx: usize) -> std::net::Ipv4Addr {
       let second_octet = ((node_idx / 254) % 254) + 1;
       let third_octet = (node_idx % 254) + 1;
       std::net::Ipv4Addr::new(127, second_octet as u8, third_octet as u8, 1)
   }
   ```
   - Each node gets a unique loopback IP (127.x.y.1)
   - Supports up to 64,516 unique test nodes
   - No port conflicts even with parallel tests!

3. **Dynamic port allocation** (`crates/freenet-macros/src/codegen.rs:247-249`):
   ```rust
   let network_port = reserve_local_port_on_ip(node_ip)?;
   let ws_port = reserve_local_port_on_ip(node_ip)?;
   ```
   - Ports are reserved dynamically per test
   - No hardcoded ports

4. **Test comment mentions parallelism** (`crates/core/tests/operations.rs:259`):
   ```rust
   // Increased timeout for CI where 8 parallel tests compete for resources
   ```

### Why Tests Run Serially (Currently)

The CI workflow runs with `--test-threads=1` (line 200), but this appears to be **conservative** rather than necessary. The infrastructure is fully designed for parallel execution.

### Recommended Immediate Action

**Phase 1a: Enable Parallel Execution (ZERO CODE CHANGES)**
1. Change CI workflow from `--test-threads=1` to `--test-threads=4`
2. Expected impact: 12-15 min → 3-4 min (75% reduction)
3. Risk: Low (infrastructure already supports it)

If successful, proceed with health-check optimization for further gains.

## Key Observations

### 1. Test Job is the Primary Bottleneck

The Test job accounts for **95.4% of PR CI time** with an average runtime of ~22m 51s. This job runs three distinct test suites:

#### a) Workspace-wide Tests (Parallel, ~5-8 minutes)
```yaml
# Run all tests in parallel using all 16 cores (excludes operations/simulation)
run: cargo test --workspace --no-default-features --features trace,websocket,redb -- --test-threads=16 ...
```
Fast unit and integration tests that can run in parallel.

#### b) Operations Tests (Serial, **~12-15 minutes** - MAJOR BOTTLENECK)
```yaml
# Operations tests (serial)
run: cargo test -p freenet --test operations -- --test-threads=1
```

**Why operations tests are slow:**
- **16 integration tests** in `crates/core/tests/operations.rs`
- Each test spins up real network nodes (gateways + peers) with real WebSocket servers
- **Minimum 260 seconds (4.3 minutes) just for node startup across all tests**
  - Test with longest startup: `test_put_merge_persists_state` (40s)
  - 5 tests with 20s startup each
  - Average startup: 16s per test
- Tests include 52 `tokio::time::sleep()` calls for network stabilization:
  - 5 second sleeps (network propagation)
  - 3 second sleeps (connection stabilization)
  - 2 second sleeps (state persistence)
- **Must run serially** because:
  - Real localhost ports (conflicts if parallel)
  - Real network connections
  - Shared gateway/peer infrastructure

**Estimated breakdown:**
- Node startup: 4.3 minutes (sequential across 16 tests)
- Test execution + sleeps: 8-10 minutes
- **Total: 12-15 minutes** (dominates the Test job)

#### c) Simulation Tests (Serial, ~2-3 minutes)
```yaml
# Simulation tests (serial)
run: cargo test -p freenet --test simulation_integration --test simulation_smoke -- --test-threads=1
```
Deterministic simulation tests that must run serially due to global state (socket registries, RNG, counters).

### 2. six-peer-regression Shows Variability

Runtime varies from 3m 26s to 4m 29s (29% variance), suggesting:
- Docker pull times may vary
- Network conditions affect NAT simulation
- Build caching effectiveness varies

### 3. Existing Optimizations Are Working

The workflow already implements effective cost-saving strategies:
- ✅ Skip Test/six-peer on merge_group (PR already validated)
- ✅ Skip Test/six-peer on main push (PR already validated)
- ✅ Skip six-peer for dependabot PRs
- ✅ Path filtering for docs-only changes
- ✅ Concurrency cancellation for duplicate runs
- ✅ Fast checks (Fmt, Clippy) run in parallel for fail-fast

These optimizations reduce ~75% of CI runs to <2 minutes.

## Cost Analysis (Ubicloud Pricing)

Assuming Ubicloud pricing from workflow comments (~$0.13/run):

### Current Costs Per Merge

1. **PR creation:** Full test suite (~23 mins on ubicloud-standard-16)
2. **Merge queue:** Fast checks only (~1.5 mins on ubicloud-standard-8)
3. **Main push:** Fast checks only (~1.5 mins on ubicloud-standard-8)

**Estimated cost per PR merge:** ~$0.15-0.20 (dominated by PR test run)

### Monthly Projection

Based on recent activity (874 main branch runs):
- Average ~29 merges/day
- Monthly CI costs: ~$130-175/month

The workflow notes estimate 25-30% savings from optimizations, suggesting costs would be ~$175-250/month without the current skip logic.

## Optimization Recommendations

### High Impact (Reduce Test Job Time)

#### 1. Profile Test Suite to Identify Slow Tests

**Action:** Add test timing output to identify outliers
```yaml
- name: Test with timing
  run: cargo test --workspace ... -- --test-threads=16 --report-time
```

**Expected Impact:** Identify tests taking >1 minute for optimization
**Effort:** Low (1-2 hours)

#### 2. Parallelize Operations Tests with Port Isolation

**Current:** Serial execution (`--test-threads=1`) taking ~12-15 minutes
**Root cause:** Tests use real localhost ports that would conflict if run in parallel

**Solution:** Dynamic port allocation per test
```rust
// Instead of hardcoded ports, use OS-assigned ports
let listener = TcpListener::bind("127.0.0.1:0")?;
let ws_api_port = listener.local_addr()?.port();
drop(listener);  // Release for test to bind
```

The `freenet_test` macro could be enhanced to:
1. Auto-allocate unique port ranges per test
2. Set environment variables with port assignments
3. Allow parallel execution with `--test-threads=4` (or more)

**Expected Impact:**
- Running with `--test-threads=4`: 12-15 min → 4-6 min (60% reduction)
- Running with `--test-threads=8`: 12-15 min → 3-4 min (70% reduction)

**Effort:** Medium (1-2 days to refactor `freenet_test` macro + test validation)
**Risk:** Medium (need thorough testing to ensure no port conflicts or race conditions)

#### 2b. Reduce Operations Test Startup Times

**Current:** 260 seconds (4.3 minutes) total startup wait across 16 tests
**Investigation needed:** Are these conservative waits that could be reduced?

**Approaches:**
1. **Health check polling instead of fixed waits:**
   ```rust
   // Instead of: startup_wait_secs = 20
   // Use: wait_until_ready with polling
   for _ in 0..100 {  // 10 second max
       if node_is_ready().await { break; }
       tokio::time::sleep(Duration::from_millis(100)).await;
   }
   ```

2. **Reduce excessive waits:**
   - `test_put_merge_persists_state`: 40s → could 20s suffice?
   - 5 tests with 20s waits → test if 10-15s works
   - Average could go from 16s → 8-10s per test

**Expected Impact:**
- Reduce startup time from 4.3 min → 2-2.5 min (40-50% startup reduction)
- Overall operations test time: 12-15 min → 10-12 min

**Effort:** Medium (2-3 days to implement health checks + test stability)
**Risk:** Low (can be done incrementally per test)

#### 3. Parallelize Simulation Tests (If Possible)

**Current:** Serial execution due to "global state (socket registries, RNG, counters)"
**Investigation needed:** Can we isolate simulation tests?
- Use separate RNG seeds per test
- Isolate socket registries per test
- Consider test-scoped singletons vs. global state

**Expected Impact:** 2-3 minute reduction if parallelizable
**Effort:** High (1-2 days refactoring)

#### 4. Split Test Job into Multiple Jobs

Run operations, simulation, and workspace tests in parallel jobs:

```yaml
test_workspace:
  runs-on: ubicloud-standard-16
  # Fast parallel tests only

test_operations:
  runs-on: ubicloud-standard-4  # Smaller, cheaper if serial
  # Serial operations tests

test_simulation:
  runs-on: ubicloud-standard-4  # Smaller, cheaper if serial
  # Serial simulation tests
```

**Expected Impact:** 5-8 minute reduction (parallel execution)
**Effort:** Low (2-3 hours)
**Cost Impact:** Neutral or slight increase (more runner minutes, but shorter wall time)

### Medium Impact (Optimize six-peer-regression)

#### 5. Cache Docker Images

**Current:** `docker pull alpine:latest` in workflow
**Action:** Use GitHub Actions Docker layer caching

```yaml
- name: Set up Docker Buildx
  uses: docker/setup-buildx-action@v3

- name: Cache Docker layers
  uses: actions/cache@v4
  with:
    path: /tmp/.buildx-cache
    key: ${{ runner.os }}-buildx-${{ github.sha }}
    restore-keys: ${{ runner.os }}-buildx-
```

**Expected Impact:** 30-60 second reduction
**Effort:** Low (1 hour)

#### 6. Investigate six-peer Runtime Variability

**Current:** 3m 26s - 4m 29s (29% variance)
**Action:** Add timing instrumentation to identify variable steps

**Expected Impact:** Better predictability, potential 30s-1m reduction
**Effort:** Medium (3-4 hours)

### Low Impact (Fine-tuning)

#### 7. Clippy Caching Optimization

**Current:** 1m 9s - 1m 27s
**Action:** Verify `rust-cache@v2` is optimal; consider `sccache`

**Expected Impact:** 10-20 second reduction
**Effort:** Low (1-2 hours)

## Recommended Action Plan

### Phase 1: Quick Wins (1-3 days)
1. **Add test timing instrumentation** to identify slowest individual tests
2. **Reduce conservative startup waits** in operations tests (40s → 20s, etc.)
3. **Cache Docker images** for six-peer-regression
4. **Experiment with operations test parallelism** (start with `--test-threads=2`)

**Expected impact:** 3-5 minute reduction in PR CI time
**Effort:** Low-medium (mostly configuration changes)

### Phase 2: Medium-term Optimizations (1-2 weeks)
1. **Implement health-check based readiness** for operations test startup (replace fixed waits)
2. **Refactor `freenet_test` macro** for dynamic port allocation
3. **Enable parallel operations tests** with `--test-threads=4+`
4. **Profile workspace tests** to identify and optimize slowest unit tests
5. **Analyze six-peer runtime variability** and optimize

**Expected impact:** 6-10 minute reduction (operations tests: 12-15 min → 4-6 min)
**Effort:** Medium (requires macro changes and testing)

### Phase 3: Advanced Optimizations (Optional, 2-4 weeks)
1. **Refactor simulation tests** for parallel execution (isolate global state)
2. **Split Test job** into multiple parallel jobs (workspace/operations/simulation)
3. **Consider test sharding** across multiple runners for operations tests
4. **Reduce sleep times** in operations tests (use event-driven waits instead)

**Expected impact:** Additional 3-5 minute reduction
**Target:** ~10-12 minute PR CI time (50-55% improvement from current ~23 minutes)

## Conclusion

Current CI performance shows effective cost optimizations (merge_group and main pushes skip expensive tests), but PR runs at ~23 minutes have clear bottlenecks.

### The Core Problem: Operations Tests

**Operations tests account for 12-15 minutes (52-65%) of the 23-minute PR CI time.**

The root causes are:
1. **Fixed startup waits:** 260 seconds (4.3 minutes) of sequential startup waits
2. **Serial execution:** Tests run one-at-a-time due to port conflicts
3. **Real network operations:** 52 sleep calls for network stabilization (5-second sleeps, etc.)

### High-Impact Opportunities

**Quick wins (1-3 days):**
1. Reduce conservative startup waits (40s → 20s, etc.) → 2 minute savings
2. Experiment with limited parallelism (test-threads=2) → 30-50% reduction
3. Cache Docker images → 30-60 second savings

**Medium-term (1-2 weeks):**
1. Dynamic port allocation in `freenet_test` macro → enables full parallelism
2. Health-check based readiness → replaces fixed waits
3. Result: Operations tests 12-15 min → 4-6 min (60% reduction)

**Realistic target:** Reduce PR CI time from ~23 minutes to ~12-15 minutes (35-45% improvement)

## Appendix: Raw Data

### Sample PR Runs Analyzed

| Run ID | Total Time | Test | six-peer | Clippy |
|--------|-----------|------|----------|--------|
| 21097867048 | 23m 51s | 22m 47s | 3m 26s | 1m 16s |
| 21097188375 | 23m 52s | 23m 19s | 3m 43s | 1m 10s |
| 21096188744 | 23m 14s | 22m 29s | 4m 18s | 1m 10s |
| 21093264751 | 23m 15s | 22m 29s | 4m 29s | 1m 14s |

### Workflow Job Dependency Graph

```
PR Run:
  ├── conventional_commits (ubuntu-latest, parallel)
  ├── fmt_check (ubuntu-latest, parallel)
  └── clippy_check (ubicloud-8, parallel)
       ├── test_all (ubicloud-16, needs: fmt_check)
       └── six_peer_regression (ubicloud-16, needs: fmt_check)

Merge Queue:
  ├── fmt_check (ubuntu-latest)
  └── clippy_check (ubicloud-8)
  # test_all and six_peer_regression SKIPPED

Main Push:
  ├── fmt_check (ubuntu-latest)
  └── clippy_check (ubicloud-8)
  # test_all and six_peer_regression SKIPPED
```
