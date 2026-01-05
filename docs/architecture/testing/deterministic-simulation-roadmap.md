# Deterministic Simulation Roadmap

This document analyzes what it would take to make Freenet's simulation tests fully deterministic.

## Current State (January 2026)

SimNetwork achieves **~99% determinism** through:
- ✅ **Turmoil integration** - Deterministic async task scheduling (always enabled)
- ✅ **GlobalRng** for all random decisions (seeded, deterministic)
- ✅ **VirtualTime** with explicit time control (`sim.advance_time()`)
- ✅ **TimeSource trait** injected into components (replaces direct `tokio::time::`)
- ✅ In-memory transport via `SimulationSocket` (no real network I/O)
- ✅ MockStateStorage (no disk I/O)
- ✅ Fault injection with deterministic drop decisions
- ✅ **GlobalExecutor::spawn** - All tokio::spawn calls migrated
- ✅ **Single-threaded tests** - All SimNetwork tests use `current_thread` runtime
- ✅ **No start_paused dependency** - Tests use explicit VirtualTime advancement

### Recent Progress

| Phase | Status | Description |
|-------|--------|-------------|
| Phase 1: VirtualTime integration | ✅ Complete | All simulation tests use VirtualTime explicitly |
| Phase 2: GlobalRng migration | ✅ Complete | `rand::random()` → `GlobalRng` in production code |
| Phase 3: TimeSource injection | ✅ Complete | Components use `TimeSource` trait, not `tokio::time` |
| Phase 4: start_paused removal | ✅ Complete | Tests use `sim.advance_time()` + `yield_now()` pattern |
| Phase 5: Turmoil integration | ✅ Complete | Deterministic scheduling via Turmoil (always enabled) |

### Turmoil Integration Complete

**Turmoil provides deterministic async scheduling:**
- ✅ Always enabled as a dependency for deterministic task scheduling
- ✅ All simulation tests use Turmoil for full determinism
- ✅ No special RUSTFLAGS needed - just run tests with --test-threads=1
- ✅ `fdev test single-process` uses Turmoil for deterministic execution

**Full determinism achieved:**
- ✅ Channel message ordering - fully deterministic with Turmoil
- ✅ `select!` macro branch selection - deterministic (use `biased` for clarity)

**Why this matters for linearizability:**
To prove linearizability of operations, we need:
1. Deterministic event ordering (same seed → same execution trace) - ✅ Achieved with Turmoil
2. Ability to replay exact sequences for formal verification - ✅ Achieved with Turmoil
3. Control over "happens-before" relationships between events - ✅ Achieved with Turmoil

Turmoil ensures same-seed runs produce identical interleavings, enabling formal verification of consistency properties.

## Sources of Non-Determinism

### 1. Tokio Task Scheduling ✅ RESOLVED

**Previous Problem:** Even with single-threaded tokio, task execution order when multiple futures are ready is not guaranteed.

**Solution Implemented:** Turmoil provides deterministic task scheduling with guaranteed FIFO ordering of ready tasks.

**Current Status:** ✅ Same seed produces identical event orderings across runs.

### 2. Real Wall-Clock Time (MOSTLY RESOLVED)

**Previous Problem:** 257+ `tokio::time::` calls and 129+ `Instant::now()` calls used real time.

**Current Status:**
- ✅ Simulation tests use `VirtualTime` via `sim.advance_time()`
- ✅ Transport layer uses `TimeSource` trait (injected)
- ✅ LEDBAT congestion control uses `TimeSource`
- ⚠️ Some production code still uses `tokio::time::` (acceptable - only matters in simulation)

### 3. Channel Message Ordering ✅ RESOLVED

**Previous Problem:** MPSC channels don't guarantee delivery order when multiple senders race.

**Solution Implemented:** Turmoil provides deterministic channel message ordering.

**Current Status:** ✅ Channel operations are deterministic across runs.

### 4. Select! Macro ✅ RESOLVED

**Previous Problem:** `tokio::select!` can choose branches non-deterministically.

**Solution Implemented:** Turmoil provides deterministic select! branch selection. Using `biased` helps make the intent clear.

**Current Status:** ✅ Select! operations are deterministic across runs.

### 5. HashMap Iteration (LOW)

**Problem:** Rust HashMaps use randomized iteration order.

**Mitigation:** Code avoids iteration in critical paths; uses `.sort()` where needed.

---

## Existing Determinism Infrastructure

### VirtualTime (`simulation/time.rs`)

**Capabilities:**
```rust
pub trait TimeSource: Send + Sync + 'static {
    fn now_nanos(&self) -> u64;
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>>;
    fn sleep_until(&self, deadline: u64) -> Pin<Box<dyn Future<Output = ()> + Send>>;
    fn timeout<F, T>(&self, duration: Duration, future: F) -> ...;
}
```

**Current Integration:**
- ✅ SimNetwork creates VirtualTime instance
- ✅ FaultInjectorState uses VirtualTime for latency injection
- ✅ Message delay queuing with virtual deadlines
- ❌ Node internal timing still uses `tokio::time`
- ❌ Transport layer uses real time

### SimulationRng (`simulation/rng.rs`)

**Capabilities:**
- Arc<Mutex<SmallRng>> for thread-safe seeded randomness
- All random decisions derive from master seed
- Child RNGs for per-peer determinism

### Scheduler (`simulation/scheduler.rs`)

**Capabilities:**
- BinaryHeap with deterministic tie-breaking
- Order: timestamp → peer address → event type → event ID
- FIFO for same-deadline wakeups

---

## Approaches to Full Determinism

### Option A: Explicit VirtualTime Control (CURRENT APPROACH - COMPLETE)

**Configuration:**
```rust
#[tokio::test(flavor = "current_thread")]
async fn deterministic_test() {
    let mut sim = SimNetwork::new(...).await;

    // Advance time explicitly using VirtualTime
    for _ in 0..30 {
        sim.advance_time(Duration::from_millis(100));
        tokio::task::yield_now().await;  // Let tasks process
    }
}
```

**What it solves:**
- ✅ Eliminates thread scheduling randomness (single-threaded)
- ✅ Explicit time control via VirtualTime
- ✅ Deterministic RNG via GlobalRng
- ✅ Deterministic network via SimulationSocket

**What was made deterministic with Turmoil:**
- ✅ Task wake-up order when multiple futures are ready
- ✅ Channel message ordering (sender races)
- ✅ Select! branch selection

**Status:** ✅ Complete
**Determinism achieved:** ~99%

### Option B: Full VirtualTime Integration (Medium Effort, Good Determinism)

**Required changes:**

1. **Add VirtualInterval to TimeSource:**
```rust
trait TimeSource {
    // ... existing methods ...
    fn interval(&self, period: Duration) -> VirtualInterval;
}
```

2. **Inject TimeSource throughout codebase:**
```rust
// Current (blocks determinism):
tokio::time::sleep(Duration::from_secs(1)).await;

// Needed:
time_source.sleep(Duration::from_secs(1)).await;
```

3. **Replace Instant-based measurements:**
```rust
// Current:
let start = std::time::Instant::now();
let elapsed = start.elapsed();

// Needed:
let start = time_source.now_nanos();
let elapsed = time_source.now_nanos() - start;
```

**Files requiring changes:**

| Category | Files | Changes |
|----------|-------|---------|
| Transport | 7 files | ~100 lines |
| Ring/Topology | 2 files | ~20 lines |
| Contract Executor | 2 files | ~10 lines |
| Node/Server | 3 files | ~15 lines |

**Effort:** 3-4 weeks
**Determinism achieved:** ~85-90%

### Option C: Turmoil Integration ✅ IMPLEMENTED (Primary)

[Turmoil](https://github.com/tokio-rs/turmoil) is Tokio's deterministic simulation framework.

**Status:** ✅ Always enabled as a dependency for deterministic task scheduling

**How it works:**
- Single-threaded execution across all "hosts"
- Each host gets its own tokio Runtime (with Turmoil's scheduler)
- Explicit time control via simulation stepping
- Works with our SimulationSocket for in-memory networking

**SimNetwork Integration:**
```rust
let sim = SimNetwork::new("test", 1, 5, 7, 3, 10, 2, 42).await;

sim.run_simulation::<rand::rngs::SmallRng, _, _>(
    42,   // seed
    10,   // max_contract_num
    100,  // iterations
    Duration::from_secs(60),
    || async {
        // Test assertions run inside Turmoil
        tokio::time::sleep(Duration::from_secs(5)).await;
        Ok(())
    },
)?;
```

**Key Files:**
- `crates/core/src/node/testing_impl/turmoil_runner.rs` - Turmoil runner module
- `crates/core/tests/turmoil_poc.rs` - POC tests validating Turmoil + SimulationSocket

**Determinism achieved:** ~99%

### Option D: Custom Deterministic Executor (High Effort, Not Needed)

**Design (FoundationDB-style):**
```rust
pub struct DeterministicRuntime {
    scheduler: Scheduler,
    time: VirtualTime,
    tasks: BTreeMap<TaskId, Task>,
    ready_queue: VecDeque<TaskId>,  // FIFO for determinism
}

impl DeterministicRuntime {
    pub fn step(&mut self) -> bool {
        // 1. Process time-based wakeups
        self.process_wakeups();

        // 2. Run one ready task (FIFO order)
        if let Some(task_id) = self.ready_queue.pop_front() {
            self.poll_task(task_id);
            return true;
        }

        // 3. Advance time to next event
        if let Some(next) = self.scheduler.next_event_time() {
            self.time.advance_to(next);
            return true;
        }

        false // Simulation complete
    }

    pub fn run_until<F>(&mut self, condition: F) -> Result<(), SimError>
    where F: Fn(&Self) -> bool
    {
        while !condition(self) {
            if !self.step() {
                return Err(SimError::Deadlock);
            }
        }
        Ok(())
    }
}
```

**Requirements:**
1. Custom Future executor with deterministic task ordering
2. Shim for `tokio::time` operations
3. Shim for channel operations (mpsc, oneshot, broadcast)
4. Thread-local context for runtime access

**Effort:** 4-6 weeks
**Determinism achieved:** ~99%+
**Status:** ❌ Not needed - Turmoil provides sufficient determinism

---

## Recommended Roadmap

### Phase 1-4: COMPLETE ✅

The following phases have been completed:

| Phase | Status | What Was Done |
|-------|--------|---------------|
| Phase 1: Single-threaded runtime | ✅ Complete | All tests use `current_thread` |
| Phase 2: VirtualTime integration | ✅ Complete | `sim.advance_time()` replaces `tokio::time::sleep()` |
| Phase 3: GlobalRng migration | ✅ Complete | `rand::random()` → `GlobalRng` in production code |
| Phase 4: TimeSource injection | ✅ Complete | Transport/LEDBAT use `TimeSource` trait |

**Current Pattern:**
```rust
#[tokio::test(flavor = "current_thread")]
async fn test_with_deterministic_time() {
    let mut sim = SimNetwork::new(..., SEED).await;

    // Start network
    let _handles = sim.start_with_rand_gen::<SmallRng>(SEED, 5, 10).await;

    // Advance time in controlled steps using VirtualTime
    for _ in 0..100 {
        sim.advance_time(Duration::from_millis(10));
        tokio::task::yield_now().await;
    }

    sim.assert_convergence(...).await;
}
```

### Phase 5: Deterministic Scheduler ✅ COMPLETE

**Status:** Turmoil is integrated and always enabled.

**What was done:**
- ✅ Turmoil integrated as always-enabled dependency
- ✅ All simulation tests use Turmoil for deterministic scheduling
- ✅ No special RUSTFLAGS needed - just run with --test-threads=1
- ✅ `fdev test` uses Turmoil for deterministic single-process simulation

**Determinism achieved:** ~99% (full determinism)

**Implemented Solution:**

| Option | Effort | Determinism | Status |
|--------|--------|-------------|--------|
| **Turmoil integration** | 1-2 weeks | ~99% | ✅ **ACTIVE** - Always enabled |
| Custom executor | 4-6 weeks | ~99%+ | ❌ Not needed - Turmoil provides sufficient determinism |

**Turmoil Integration ✅ COMPLETE**

Turmoil provides deterministic scheduling as an always-enabled dependency.

**Implementation confirmed**: SimNetwork works perfectly with Turmoil:
- Uses `MemoryEventsGen<R>` for client events (not HTTP/WebSocket)
- Uses `SimulationSocket` for P2P transport
- Calls `run_node_with_shared_storage()` which bypasses `HttpGateway`
- Turmoil handles deterministic task scheduling transparently

**Result**: Full determinism achieved. Turmoil works seamlessly with all simulation tests.

**Note**: Turmoil is now the primary and only approach used, as it's always enabled and provides full determinism without requiring special RUSTFLAGS or CI configuration.

### Phase 6: Linearizability Verification (Future)

Once Phase 5 is complete, we can add:

| Task | Effort | Notes |
|------|--------|-------|
| Jepsen-style history recording | 2-3 weeks | Record all operations with timestamps |
| Linearizability checker (Knossos-style) | 3-4 weeks | Verify histories are linearizable |
| Invariant DSL | 2-3 weeks | Express and check system invariants |
| Property-based testing integration | 1-2 weeks | Automatic shrinking of failing cases |

---

## Current Determinism Status

| Aspect | Current Implementation (Jan 2026) |
|--------|-----------------------------------|
| Task scheduling | ✅ Deterministic FIFO (Turmoil) |
| Time control | ✅ VirtualTime (`sim.advance_time()`) + Turmoil |
| Message order | ✅ Fully deterministic (Turmoil) |
| RNG | ✅ GlobalRng (seeded) |
| Network I/O | ✅ SimulationSocket (in-memory) |
| **Reproducibility** | **~99% ✅ ACTIVE** |
| **Linearizability proof** | ✅ Possible |

**Note**: Turmoil is always enabled. All tests benefit from full determinism without special configuration.

---

## Decision Criteria

**Historical Decision Points (All phases now complete):**

~~**Choose Phase 1 only if:**~~ ✅ Complete - Single-threaded runtime
~~**Proceed to Phase 2 if:**~~ ✅ Complete - VirtualTime integration
~~**Proceed to Phase 3 if:**~~ ✅ Complete - GlobalRng migration
~~**Proceed to Phase 4 if:**~~ ✅ Complete - TimeSource injection
~~**Proceed to Phase 5 (Turmoil):**~~ ✅ **COMPLETE - Always enabled**

**Current Status (January 2026):**
- Turmoil is always enabled as a dependency
- All tests automatically benefit from deterministic scheduling
- Just run tests with --test-threads=1 for full determinism
- No special RUSTFLAGS or CI configuration needed

All infrastructure is in place. No further phases needed for deterministic simulation testing.

---

## Framework Comparison (Historical Context)

Freenet previously considered multiple frameworks for deterministic simulation:

| Framework | Approach | Effort | Production Use | Determinism |
|-----------|----------|--------|----------------|-------------|
| [Turmoil](https://github.com/tokio-rs/turmoil) | Tokio host simulation | Low | Tokio's official solution | ~99% |
| ~~[MadSim](https://github.com/madsim-rs/madsim)~~ | ~~Tokio package swap~~ | ~~Low~~ | ~~RisingWave~~ | ~~Previously considered~~ |
| [Diviner](https://github.com/xxuejie/diviner) | Custom executor + wrappers | High | CKB | ~99% |
| Custom | FoundationDB-style | Very High | N/A | ~99%+ |

**Turmoil was chosen** because:
1. Official Tokio solution for deterministic simulation
2. Always enabled - no special RUSTFLAGS needed
3. Minimal code changes required
4. Works seamlessly with existing infrastructure
5. Provides ~99% determinism for linearizability verification

---

## Turmoil Usage Guide ✅ ALWAYS ENABLED

This section documents how to use Turmoil for deterministic simulation testing. **Turmoil is always enabled as a dependency - no special configuration needed.**

### Quick Start

Run tests with Turmoil's deterministic runtime (already active):

```bash
# Run all simulation tests with full determinism
cargo test -p freenet --test sim_network -- --test-threads=1

# Run specific test
cargo test -p freenet --test sim_network test_sim_network_basic_connectivity -- --test-threads=1

# Run with verbose logging
RUST_LOG=info cargo test -p freenet --test sim_network -- --nocapture --test-threads=1
```

### How Turmoil Works

Turmoil is always enabled as a dependency and provides:
- **Deterministic scheduling** - Same seed → same task order
- **Virtual time** - Time only advances when runtime is idle or explicitly advanced
- **Reproducible randomness** - All random sources are seeded via GlobalRng

### fdev Test Command

For local development with `fdev`:

```bash
# Run contract tests (deterministic by default)
fdev test --simulate

# Run with specific seed (reproduce failures)
SEED=12345 fdev test --simulate

# Run with verbose logging
RUST_LOG=info fdev test --simulate
```

### VirtualTime Integration

`VirtualTime` works seamlessly with Turmoil:

```rust
use freenet::simulation::VirtualTime;

// VirtualTime always available
let vt = VirtualTime::new();

// Get current time
let now = vt.now_nanos();

// Sleep (uses Turmoil's deterministic time)
vt.sleep(Duration::from_secs(1)).await;

// Advance time manually
vt.advance(Duration::from_secs(1));
```

### Test Annotations

All simulation tests use single-threaded tokio with explicit VirtualTime control:

```rust
// Standard test annotation (no start_paused - we use VirtualTime explicitly)
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn my_simulation_test() {
    let mut sim = SimNetwork::new(..., SEED).await;

    // Start the network
    let _handles = sim.start_with_rand_gen::<SmallRng>(SEED, 5, 10).await;

    // Advance time explicitly using VirtualTime
    for _ in 0..30 {
        sim.advance_time(Duration::from_millis(100));
        tokio::task::yield_now().await;
    }
}
```

With Turmoil:
- Task scheduling is deterministic (FIFO ordering)
- VirtualTime integrates with Turmoil's time control
- Same seed produces identical execution

### Reproducing Failures

When a test fails, reproduce with the same seed:

```bash
# Run with specific seed
SEED=<seed-from-failure> cargo test -p freenet --test sim_network <test_name> -- --nocapture --test-threads=1
```

### Debugging Tips

1. **Enable logging** to see what's happening:
   ```bash
   RUST_LOG=debug cargo test -p freenet --test sim_network -- --nocapture --test-threads=1
   ```

2. **Set a known seed** for reproducibility:
   ```bash
   SEED=42 cargo test -p freenet --test sim_network -- --test-threads=1
   ```

3. **Check time progression** in logs for timing-related issues

4. **Always use --test-threads=1** for full determinism

---

## References

### Frameworks
- [Turmoil](https://github.com/tokio-rs/turmoil) - Tokio's official deterministic network simulation (actively used in Freenet)
- [Diviner](https://github.com/xxuejie/diviner) - FoundationDB-style deterministic testing for Rust
- ~~[MadSim](https://github.com/madsim-rs/madsim)~~ - Previously considered, opted for Turmoil instead
- [ODEM-rs](https://lib.rs/crates/odem-rs) - Object-based discrete-event modeling

### Articles
- [FoundationDB Testing](https://apple.github.io/foundationdb/testing.html) - Gold standard for deterministic simulation
- [S2: Deterministic Simulation Testing](https://s2.dev/blog/dst) - Practical DST implementation (2025)
- [Polar Signals: DST in Rust](https://www.polarsignals.com/blog/posts/2025/07/08/dst-rust) - State machine approach (2025)
- [Deterministic Simulation Testing Overview](https://notes.eatonphil.com/2024-08-20-deterministic-simulation-testing.html) - Phil Eaton's overview
