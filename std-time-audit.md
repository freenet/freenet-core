# Audit: std::time Usage in Production Code

**Issue Reference:** #2684 - Simulation tests failing with non-deterministic behavior

**Context:** Deterministic Simulation Testing (DST) requires using abstractions like `TimeSource` and `VirtualTime` instead of `std::time` or `tokio::time` to ensure reproducible test results.

## Executive Summary

This audit found **extensive usage** of `std::time::Instant::now()` and `tokio::time` in production code throughout the codebase. These usages break determinism in simulation tests and likely contribute to issue #2684.

---

## Critical Production Files Using `std::time::Instant::now()`

### High Priority (Core Runtime Paths)

#### 1. **operations/connect.rs** (Lines: 363, 620, 708, 829, 1017, 1251, 1332, 1462, 1888, 2175, 2182)
- **Usage:** Connection timing, forward attempts, acceptance tracking
- **Impact:** Connection establishment timing is non-deterministic in simulation
- **Fix Required:** Inject `TimeSource` trait

#### 2. **util/backoff.rs** (Lines: 185, 196, 209, 264)
- **Usage:** Exponential backoff timing for retry logic
- **Impact:** Backoff timing non-deterministic, affects retry behavior
- **Fix Required:** Inject `TimeSource` trait

#### 3. **topology/mod.rs** (Lines: 355, 366, 391, 621, 806, 830, 850, 872, 890, 915, 948, 1121, 1163)
- **Usage:**
  - Thread-local static for log throttling (line 355, 366, 391)
  - Source ramp-up timing checks (line 621)
  - Connection evaluation timing (various)
- **Impact:** Topology adjustments and connection decisions non-deterministic
- **Fix Required:**
  - Replace thread-local statics with injected time source
  - Pass `TimeSource` through topology functions

#### 4. **topology/connection_evaluator.rs** (Lines: 62, 74, 85, 96, 106, 114, 125, 138)
- **Usage:** Connection quality evaluation timing
- **Impact:** Connection acceptance/rejection decisions non-deterministic
- **Fix Required:** Inject `TimeSource` trait

#### 5. **ring/mod.rs** (Lines: 964, 990, 1121)
- **Usage:** Backoff cleanup scheduling
- **Impact:** Peer connection management timing non-deterministic
- **Fix Required:** Inject `TimeSource` trait

#### 6. **contract/executor.rs** (Lines: 435, 479)
- **Usage:** Task creation timestamps, timeout checks
- **Impact:** Contract execution timing non-deterministic
- **Fix Required:** Inject `TimeSource` trait

#### 7. **contract/executor/init_tracker.rs** (Lines: 111, 133, 170)
- **Usage:** Contract initialization tracking
- **Impact:** Initialization timeout detection non-deterministic
- **Fix Required:** Inject `TimeSource` trait

#### 8. **operations/orphan_streams.rs** (Lines: 101, 176, 333)
- **Usage:** Stream expiration tracking
- **Impact:** Orphan stream cleanup timing non-deterministic
- **Fix Required:** Inject `TimeSource` trait

### Medium Priority (Infrastructure & Observability)

#### 9. **server/mod.rs** (Line: 283)
- **Usage:** Token expiration cleanup
- **Impact:** Authentication token TTL non-deterministic in tests
- **Fix Required:** Inject `TimeSource` trait

#### 10. **server/http_gateway.rs** (Line: 54)
- **Usage:** Session last-accessed tracking
- **Impact:** Session expiration non-deterministic
- **Fix Required:** Inject `TimeSource` trait

#### 11. **tracing/telemetry.rs** (Lines: 223, 225, 308, 358, 371)
- **Usage:** Rate limiting for telemetry
- **Impact:** Telemetry rate limits non-deterministic (acceptable for observability)
- **Fix Required:** Optional - depends on whether telemetry needs determinism

#### 12. **util/rate_limit_layer.rs** (Line: 63)
- **Usage:** Rate limiting layer
- **Impact:** Rate limits non-deterministic
- **Fix Required:** Inject `TimeSource` trait if used in simulation tests

#### 13. **topology/meter.rs** (Lines: 271, 287, 294, 303, 310, 328, 335, 347, 354, 367, 374)
- **Usage:** Connection quality metering
- **Impact:** Metrics collection non-deterministic
- **Fix Required:** Inject `TimeSource` trait

#### 14. **topology/running_average.rs** (Lines: 59, 83)
- **Usage:** Running average calculations
- **Impact:** Averaging windows non-deterministic
- **Fix Required:** Inject `TimeSource` trait

#### 15. **ring/seeding_cache.rs** (Line: 20, imports only)
- **Usage:** Cache expiration (imports Instant, actual usage in tests)
- **Impact:** Limited - appears to be test code
- **Fix Required:** Review actual usage

#### 16. **ring/connection_manager.rs** (Line: 253)
- **Usage:** Connection evaluation timing
- **Impact:** Connection decisions non-deterministic
- **Fix Required:** Inject `TimeSource` trait

#### 17. **ring/get_subscription_cache.rs** (Line: 21, imports)
- **Usage:** Cache timing
- **Impact:** Cache behavior non-deterministic
- **Fix Required:** Review actual usage and inject `TimeSource`

#### 18. **node/network_bridge/p2p_protoc.rs** (Lines: 1900, 3160)
- **Usage:** Backoff cleanup tracking
- **Impact:** Backoff state management non-deterministic
- **Fix Required:** Inject `TimeSource` trait

---

## Production Files Using `tokio::time`

### High Priority

#### 19. **node/p2p_impl.rs** (Lines: 64, 123)
- **Usage:**
  - `wait_for_min_connections()` - waiting for network bootstrap
  - `run_node()` - uptime tracking
- **Impact:** **CRITICAL** - Bootstrap timing directly affects simulation tests
- **Fix Required:** Use `TimeSource::sleep()` instead

#### 20. **transport/fast_channel.rs** (Lines: 233, 427)
- **Usage:** Backoff sleep, test waiting
- **Impact:** Channel timing non-deterministic
- **Context:** Line 427 is in a test (mod tests), line 233 might be production
- **Fix Required:** Review line 233 context, use `TimeSource::sleep()`

#### 21. **node/mod.rs** (Line: 211)
- **Usage:** Waiting for gateway key conversion
- **Impact:** Gateway initialization timing non-deterministic
- **Fix Required:** Use `TimeSource::sleep()`

#### 22. **bin/freenet.rs** (Lines: 153, 211)
- **Usage:** Periodic cleanup interval, startup delay
- **Impact:** Binary-level timing (less critical for simulation)
- **Fix Required:** Low priority - binary code may not need simulation support

#### 23. **tracing/mod.rs** (Lines: 1630, 1679)
- **Usage:** Timeout and sleep in tracing code
- **Impact:** Tracing infrastructure timing
- **Fix Required:** Low priority - tracing may not need determinism

### Test Code (Reference Only)

- **transport/connection_handler.rs** (Lines: 2526, 3072, 3206, 3511, 3643) - Test helpers
- **transport/ledbat/controller.rs** (Line: 3368) - Tests
- **transport/peer_connection.rs** (Lines: 1367, 1706) - Tests
- **node/testing_impl.rs** (Lines: 1220-1223, 1431, 1732, 1977, 2021, 2023, 2031) - Test infrastructure

---

## Production Files Using `std::time::SystemTime`

These are **generally acceptable** if used only for Unix timestamps (wall-clock time for logging/storage), but should be reviewed:

### Review Required

#### 24. **message.rs** (Line: 654)
- **Usage:** `last_update: Option<std::time::SystemTime>` field
- **Impact:** Message timestamps - review if used in simulation logic
- **Fix Required:** Review usage context

#### 25. **contract/storages/redb.rs** (Lines: 100-101)
- **Usage:** Unix timestamp for storage
- **Impact:** Storage timestamps - acceptable if not used in simulation logic
- **Fix Required:** Review if timestamps affect simulation behavior

#### 26. **tracing/telemetry.rs** (Line: 62)
- **Usage:** Unix epoch timestamp
- **Impact:** Telemetry timestamps - acceptable for observability
- **Fix Required:** None (observability only)

#### 27. **tracing/aof.rs** (Lines: 293, 367, 460)
- **Usage:** Unix timestamps for append-only file
- **Impact:** Trace timestamps - acceptable for observability
- **Fix Required:** None (observability only)

#### 28. **bin/commands/auto_update.rs** (Line: 15)
- **Usage:** Update checking timestamps
- **Impact:** Binary-level feature - not in simulation
- **Fix Required:** None

#### 29. **bin/commands/service.rs** (Lines: 20, 719)
- **Usage:** File timestamps, sleep
- **Impact:** Binary-level service management
- **Fix Required:** None (not used in simulation)

---

## Files Excluded (As Requested)

- **wasm_runtime/** - All files excluded (lines 328 in contract.rs uses Instant, but wasm_runtime not used in DST)
- **simulation/** - These files define the abstractions themselves
- All **test files** and **benchmark files** - Not production code

---

## Recommended Fix Strategy

### Phase 1: Critical Path (Blocks #2684)
1. **operations/connect.rs** - Connection establishment
2. **util/backoff.rs** - Retry logic
3. **node/p2p_impl.rs** - Bootstrap timing (tokio::time)
4. **topology/mod.rs** - Topology decisions

### Phase 2: Core Infrastructure
5. **topology/connection_evaluator.rs** - Connection quality
6. **contract/executor.rs** + **init_tracker.rs** - Contract execution
7. **ring/mod.rs** + **ring/connection_manager.rs** - Ring management
8. **operations/orphan_streams.rs** - Stream cleanup

### Phase 3: Observability & Polish
9. **server/** modules - HTTP gateway
10. **topology/meter.rs** + **running_average.rs** - Metrics
11. **tracing/** modules - Telemetry (if determinism needed)
12. **node/mod.rs** - Gateway initialization

### Implementation Pattern

```rust
// OLD (non-deterministic):
let now = Instant::now();
let elapsed = now.duration_since(start);

// NEW (deterministic):
struct MyComponent<T: TimeSource> {
    time_source: T,
}

impl<T: TimeSource> MyComponent<T> {
    fn check_timeout(&self, start: Instant) -> bool {
        let now = self.time_source.now();
        now.duration_since(start) > self.timeout
    }
}

// For tokio::time::sleep:
// OLD:
tokio::time::sleep(duration).await;

// NEW:
self.time_source.sleep(duration).await;
```

---

## Root Cause Analysis for #2684

The "invalid packet type" errors in simulation tests are likely caused by:

1. **Race conditions** due to non-deterministic timing in:
   - Connection establishment (`operations/connect.rs`)
   - Backoff retry timing (`util/backoff.rs`)
   - Bootstrap synchronization (`node/p2p_impl.rs`)

2. **Timing-dependent state transitions** in:
   - Topology adjustments (`topology/mod.rs`)
   - Connection evaluation (`topology/connection_evaluator.rs`)

3. **Inconsistent timeout behavior** between:
   - Real time (`std::time::Instant::now()`)
   - Virtual time (used in simulation)

---

## Next Steps

1. **Verify working directory and branch** (per AGENTS.md worktree requirements)
2. **Start with Phase 1 files** - most likely to fix #2684
3. **Add TimeSource parameter** to each struct/function
4. **Update call sites** to pass appropriate TimeSource implementation
5. **Run simulation tests** after each file to verify determinism
6. **Measure impact** - track which changes resolve #2684

---

**Generated:** 2026-01-12
**Audit Scope:** Production code only (excluded: tests, benches, wasm_runtime per request)
