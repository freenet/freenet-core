# Deterministic Simulation Roadmap

This document analyzes what it would take to make Freenet's simulation tests fully deterministic.

## Current State (January 2026)

SimNetwork achieves **~99% determinism** through:
- ✅ **MadSim integration** - Deterministic async task scheduling via `--cfg madsim` (in CI nightly builds)
- ✅ **Turmoil integration** - Deterministic simulation framework (optional, for advanced scenarios)
- ✅ **GlobalRng** for all random decisions (seeded, deterministic)
- ✅ **VirtualTime** with explicit time control (`sim.advance_time()`)
- ✅ **TimeSource trait** injected into components (replaces direct `tokio::time::`)
- ✅ In-memory transport via `SimulationSocket` (no real network I/O)
- ✅ MockStateStorage (no disk I/O)
- ✅ Fault injection with deterministic drop decisions
- ✅ **GlobalExecutor::spawn** - All tokio::spawn calls migrated
- ✅ **Single-threaded tests** - All SimNetwork tests use `current_thread` runtime
- ✅ **No start_paused dependency** - Tests use explicit VirtualTime advancement
- ✅ **CI nightly runs** - simulation-nightly.yml runs with MadSim for full determinism

### Recent Progress

| Phase | Status | Description |
|-------|--------|-------------|
| Phase 1: VirtualTime integration | ✅ Complete | All simulation tests use VirtualTime explicitly |
| Phase 2: GlobalRng migration | ✅ Complete | `rand::random()` → `GlobalRng` in production code |
| Phase 3: TimeSource injection | ✅ Complete | Components use `TimeSource` trait, not `tokio::time` |
| Phase 4: start_paused removal | ✅ Complete | Tests use `sim.advance_time()` + `yield_now()` pattern |
| Phase 5: Turmoil integration | ✅ Complete | Deterministic scheduling via Turmoil (always enabled) |

### MadSim Integration Complete (CI Nightly)

**MadSim provides deterministic async scheduling:**
- ✅ All simulation tests can run with `RUSTFLAGS="--cfg madsim"`
- ✅ CI nightly workflow (simulation-nightly.yml) uses MadSim for full determinism
- ✅ Both MadSim and standard modes tested for comparison
- ✅ Package substitution approach - minimal code changes required
- ✅ `fdev test single-process` works with MadSim

**Turmoil also available for advanced scenarios:**
- ✅ `SimNetwork::run_simulation()` method for Turmoil-based deterministic tests
- ✅ Turmoil hosts model for multi-node scenarios
- ✅ Works alongside MadSim approach

**Remaining minor sources of non-determinism (in standard mode):**
- ⚠️ Channel message ordering (fully deterministic with MadSim)
- ⚠️ `select!` macro branch selection (use `biased` where possible)

**Why this matters for linearizability:**
To prove linearizability of operations, we need:
1. Deterministic event ordering (same seed → same execution trace)
2. Ability to replay exact sequences for formal verification
3. Control over "happens-before" relationships between events

Without a deterministic scheduler, same-seed runs may produce different interleavings, making it impossible to formally verify consistency properties.

## Sources of Non-Determinism

### 1. Tokio Task Scheduling (CRITICAL - REMAINING ISSUE)

**Problem:** Even with single-threaded tokio, task execution order when multiple futures are ready is not guaranteed.

**Impact:** Same seed can produce different event orderings across runs when multiple async tasks are ready to execute simultaneously.

**Solution Required:** Deterministic scheduler (MadSim or custom executor) that guarantees FIFO ordering of ready tasks.

### 2. Real Wall-Clock Time (MOSTLY RESOLVED)

**Previous Problem:** 257+ `tokio::time::` calls and 129+ `Instant::now()` calls used real time.

**Current Status:**
- ✅ Simulation tests use `VirtualTime` via `sim.advance_time()`
- ✅ Transport layer uses `TimeSource` trait (injected)
- ✅ LEDBAT congestion control uses `TimeSource`
- ⚠️ Some production code still uses `tokio::time::` (acceptable - only matters in simulation)

### 3. Channel Message Ordering (MEDIUM)

**Problem:** MPSC channels don't guarantee delivery order when multiple senders race.

**Locations:**
- `testing_impl.rs:55,96,120,193,237,261` - `tokio::sync::mpsc::channel()`
- `testing_impl.rs:470-472` - `watch::channel()`

### 4. Select! Macro (MEDIUM)

**Problem:** `tokio::select!` can choose branches non-deterministically.

**Example from `time.rs:375-379`:**
```rust
tokio::select! {
    biased;  // Helps but doesn't fully solve
    result = future => Some(result),
    _ = sleep => None,
}
```

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

**What remains non-deterministic:**
- ❌ Task wake-up order when multiple futures are ready
- ❌ Channel message ordering (sender races)
- ❌ Select! branch selection

**Status:** ✅ Complete
**Determinism achieved:** ~90%

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

### Option C: MadSim Integration ✅ IMPLEMENTED (Primary)

[MadSim](https://github.com/madsim-rs/madsim) provides deterministic simulation via package substitution.

**Status:** ✅ Integrated and used in CI nightly builds

**How it works:**
- Compile with `RUSTFLAGS="--cfg madsim"` to enable deterministic mode
- MadSim replaces tokio with madsim-tokio at the package level
- All async operations become deterministic
- Same seed produces identical execution traces

### Option D: Turmoil Integration ✅ IMPLEMENTED (Alternative)

[Turmoil](https://github.com/tokio-rs/turmoil) is Tokio's deterministic simulation framework.

**Status:** ✅ Integrated as an optional approach for advanced scenarios

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

### Option E: GlobalExecutor Abstraction + MadSim ✅ IMPLEMENTED

**Status:** ✅ Complete - MadSim is integrated and used in CI

**Implementation:**
- MadSim dependencies added to `Cargo.toml`
- CI nightly workflow uses `RUSTFLAGS="--cfg madsim"` for deterministic runs
- Package substitution approach requires no code changes
- Tests can run in both standard and MadSim modes

**Current Configuration:**
```toml
# Workspace Cargo.toml - patches for MadSim
[patch.crates-io]
getrandom = { git = "https://github.com/madsim-rs/getrandom.git", rev = "e79a7ae" }
quanta = { git = "https://github.com/madsim-rs/quanta.git", branch = "madsim" }

# crates/core/Cargo.toml - MadSim dependencies
[target.'cfg(madsim)'.dependencies]
madsim = "0.2"
madsim-tokio = "0.2"
```

**Running with MadSim:**
```bash
# Run simulation tests with full determinism
RUSTFLAGS="--cfg madsim" cargo test -p freenet --test sim_network -- --test-threads=1

# Run fdev with MadSim
RUSTFLAGS="--cfg madsim" cargo run -p fdev -- test --gateways 1 --nodes 3 --events 10 single-process

# Use specific seed for reproducibility
RUSTFLAGS="--cfg madsim" MADSIM_SEED=0xDEADBEEF cargo test -p freenet --test simulation_integration
```

**CI Integration:**
See `.github/workflows/simulation-nightly.yml` for the nightly deterministic simulation runs.

**Determinism achieved:** ~99% (full determinism in MadSim mode)

---

### Option E: Custom Deterministic Executor (High Effort, Full Determinism)

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

**Status:** MadSim is integrated and used in CI nightly builds. Turmoil is also available as an alternative.

**What was done:**
- ✅ MadSim integrated via package substitution (`--cfg madsim`)
- ✅ CI nightly workflow (simulation-nightly.yml) runs with MadSim
- ✅ Turmoil integrated as alternative approach via `SimNetwork::run_simulation()`
- ✅ Both approaches validated with extensive test coverage
- ✅ `fdev test` works with MadSim for deterministic single-process simulation

**Determinism achieved:** ~99% (full determinism in MadSim mode)

**Implemented Solutions:**

| Option | Effort | Determinism | Status |
|--------|--------|-------------|--------|
| **MadSim integration** | 1-2 weeks | ~99% | ✅ **PRIMARY** - Used in CI, drop-in tokio replacement |
| **Turmoil integration** | 1-2 weeks | ~99% | ✅ **ALTERNATIVE** - Available via `run_simulation()` |
| Custom executor | 4-6 weeks | ~99%+ | ❌ Not needed - MadSim provides sufficient determinism |

**MadSim Integration ✅ COMPLETE**

MadSim provides deterministic scheduling via package substitution (`--cfg madsim`).

**Implementation confirmed**: SimNetwork works perfectly with MadSim because it bypasses the web server:
- Uses `MemoryEventsGen<R>` for client events (not HTTP/WebSocket)
- Uses `SimulationSocket` for P2P transport
- Calls `run_node_with_shared_storage()` which bypasses `HttpGateway`

**Result**: No axum compatibility issues. MadSim works seamlessly with all simulation tests.

**Option B: Custom Deterministic Scheduler (Alternative)**

Build a lightweight FIFO scheduler on top of existing `GlobalExecutor`:

```rust
pub struct DeterministicScheduler {
    ready_queue: VecDeque<Box<dyn Future<Output = ()>>>,
    virtual_time: VirtualTime,
}

impl DeterministicScheduler {
    /// Execute tasks in FIFO order
    pub fn step(&mut self) -> bool {
        if let Some(task) = self.ready_queue.pop_front() {
            // Poll task once
            // If pending, re-queue at back (or based on wakeup)
            true
        } else {
            false
        }
    }
}
```

**Advantages**:
- No external dependencies (no MadSim/axum conflict)
- Full control over task ordering
- Integrates with existing `VirtualTime` and `GlobalRng`
- Works within current `GlobalExecutor` abstraction

**Turmoil Integration ✅ COMPLETE (Alternative Approach)**

[Turmoil](https://github.com/tokio-rs/turmoil) is Tokio's deterministic simulation framework. Implementation validated in `crates/core/tests/turmoil_poc.rs`:

✅ **Turmoil works with our existing infrastructure:**
- Basic async code runs correctly inside Turmoil hosts
- `tokio::sync::mpsc` channels work across Turmoil hosts
- Global registries (like SimulationSocket uses) work correctly
- **Our actual `SimulationSocket` works inside Turmoil hosts**
- Determinism verified: same execution order across runs

**Key Advantage**: No tokio patching required. Turmoil intercepts `tokio::time` automatically.

**Note**: While Turmoil is available, MadSim is the primary approach used in CI for its simplicity and broader ecosystem support.

**Integration Path**:
1. Wrap SimNetwork node startup in `sim.host()` calls
2. Use Turmoil's time instead of (or alongside) VirtualTime
3. Keep SimulationSocket for network (already works)

```rust
#[test]
fn test_with_turmoil() -> turmoil::Result {
    let mut sim = turmoil::Builder::new().build();

    // Each node is a Turmoil host
    sim.host("gateway", || async {
        let socket = SimulationSocket::bind(addr).await?;
        // Node code runs here with deterministic scheduling
        Ok(())
    });

    sim.run()
}
```

**Effort**: 1-2 weeks to adapt SimNetwork to use Turmoil's host model
**Determinism**: ~99% (deterministic scheduling + our existing infrastructure)

### Phase 6: Linearizability Verification (Future)

Once Phase 5 is complete, we can add:

| Task | Effort | Notes |
|------|--------|-------|
| Jepsen-style history recording | 2-3 weeks | Record all operations with timestamps |
| Linearizability checker (Knossos-style) | 3-4 weeks | Verify histories are linearizable |
| Invariant DSL | 2-3 weeks | Express and check system invariants |
| Property-based testing integration | 1-2 weeks | Automatic shrinking of failing cases |

---

## Current vs Target Determinism

| Aspect | Standard Mode (Jan 2026) | With MadSim (CI Nightly) | With Custom Executor |
|--------|--------------------------|--------------------------|----------------------|
| Task scheduling | ⚠️ Single-thread (non-deterministic wake order) | ✅ Deterministic FIFO | ✅ Deterministic FIFO |
| Time control | ✅ VirtualTime (`sim.advance_time()`) | ✅ MadSim time + VirtualTime | ✅ VirtualTime |
| Message order | ⚠️ Partially deterministic | ✅ Fully deterministic | ✅ Fully deterministic |
| RNG | ✅ GlobalRng (seeded) | ✅ Patched getrandom | ✅ GlobalRng |
| Network I/O | ✅ SimulationSocket (in-memory) | ✅ MadSim network + SimulationSocket | ✅ In-memory |
| **Reproducibility** | ~90% | **~99% ✅ ACTIVE** | ~99%+ (not needed) |
| **Linearizability proof** | ❌ Not possible | ✅ Possible | ✅ Possible |

**Note**: MadSim mode is actively used in CI nightly builds. Standard mode is still useful for faster local iteration and comparison testing.

---

## Decision Criteria

**Historical Decision Points (All phases now complete):**

~~**Choose Phase 1 only if:**~~ ✅ Complete - Single-threaded runtime
~~**Proceed to Phase 2 if:**~~ ✅ Complete - VirtualTime integration
~~**Proceed to Phase 3 if:**~~ ✅ Complete - GlobalRng migration
~~**Proceed to Phase 4 if:**~~ ✅ Complete - TimeSource injection
~~**Proceed to Phase 5 (MadSim):**~~ ✅ **COMPLETE - Active in CI**

**Current Recommendation (January 2026):**
- **For CI and reproducible tests**: Use MadSim mode (`RUSTFLAGS="--cfg madsim"`)
- **For local development**: Standard mode is faster, use MadSim when investigating flaky tests
- **For advanced scenarios**: Turmoil available via `SimNetwork::run_simulation()`

All infrastructure is in place. No further phases needed for deterministic simulation testing.

---

## Framework Comparison (2025)

| Framework | Approach | Effort | Production Use | Determinism |
|-----------|----------|--------|----------------|-------------|
| [MadSim](https://github.com/madsim-rs/madsim) | Tokio package swap | Low | RisingWave | ~95% |
| [Turmoil](https://github.com/tokio-rs/turmoil) | Tokio host simulation | Medium | Experimental | ~95% |
| [Diviner](https://github.com/xxuejie/diviner) | Custom executor + wrappers | High | CKB | ~99% |
| Custom | FoundationDB-style | Very High | N/A | ~99%+ |

**MadSim is recommended** because:
1. Minimal code changes (package substitution, not API changes)
2. Battle-tested in RisingWave (distributed streaming database)
3. Patches getrandom/quanta for true reproducibility
4. Existing `GlobalExecutor::spawn` abstraction makes migration straightforward

---

## MadSim Usage Guide ✅ ACTIVE IN CI

This section documents how to use MadSim for deterministic simulation testing. **MadSim is now the primary deterministic testing approach, actively used in CI nightly builds.**

### Quick Start

Run tests with MadSim's deterministic runtime:

```bash
# Run all simulation tests with MadSim (same as CI nightly)
RUSTFLAGS="--cfg madsim" cargo test -p freenet --test sim_network -- --test-threads=1

# Run specific test
RUSTFLAGS="--cfg madsim" cargo test -p freenet --test sim_network test_sim_network_basic_connectivity

# Run with a specific seed for reproducibility (same as CI default)
RUSTFLAGS="--cfg madsim" MADSIM_SEED=0xDEADBEEF cargo test -p freenet --test sim_network
```

### How MadSim Works

When you compile with `--cfg madsim`, Cargo:

1. **Replaces tokio** with `madsim-tokio` (package substitution)
2. **Patches dependencies** like `getrandom` and `quanta` for determinism
3. **Enables VirtualTime delegation** - our VirtualTime delegates to MadSim's time

MadSim provides:
- **Deterministic scheduling** - Same seed → same task order
- **Virtual time** - Time only advances when runtime is idle or explicitly advanced
- **Reproducible randomness** - All random sources are seeded

### CI Configuration

For deterministic CI runs, configure your CI workflow to use MadSim:

```yaml
# .github/workflows/simulation-tests.yml
name: Simulation Tests

on: [push, pull_request]

jobs:
  simulation:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable

      - name: Run simulation tests (deterministic)
        run: |
          RUSTFLAGS="--cfg madsim" cargo test -p freenet --test sim_network -- --test-threads=1
        env:
          MADSIM_SEED: 0xDEADBEEF  # Fixed seed for reproducibility
```

### fdev Test Command

For local development with `fdev`:

```bash
# Run contract tests with MadSim (deterministic)
RUSTFLAGS="--cfg madsim" fdev test --simulate

# Run with specific seed (reproduce failures)
RUSTFLAGS="--cfg madsim" MADSIM_SEED=12345 fdev test --simulate

# Run with verbose logging
RUSTFLAGS="--cfg madsim" RUST_LOG=info fdev test --simulate
```

### VirtualTime Integration

When MadSim is enabled, `VirtualTime` automatically delegates to MadSim's time infrastructure:

```rust
use freenet::simulation::VirtualTime;

// VirtualTime works the same way regardless of MadSim
let vt = VirtualTime::new();

// Get current time (delegates to MadSim when enabled)
let now = vt.now_nanos();

// Sleep (uses MadSim's deterministic time)
vt.sleep(Duration::from_secs(1)).await;

// With MadSim, advance() is a no-op (MadSim auto-advances time)
// Without MadSim, advance() manually steps time
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

When compiled with MadSim:
- `tokio::test` is intercepted by MadSim
- Task scheduling becomes deterministic (FIFO ordering)
- VirtualTime integrates with MadSim's time control

### Reproducing Failures

When a test fails in CI, reproduce locally with the same seed:

```bash
# 1. Get the seed from CI logs (look for "MADSIM_SEED=...")
# 2. Run locally with that seed
RUSTFLAGS="--cfg madsim" MADSIM_SEED=<seed-from-ci> cargo test -p freenet --test sim_network <test_name> -- --nocapture
```

### Debugging Tips

1. **Enable logging** to see what's happening:
   ```bash
   RUSTFLAGS="--cfg madsim" RUST_LOG=debug cargo test ...
   ```

2. **Set a known seed** for reproducibility:
   ```bash
   RUSTFLAGS="--cfg madsim" MADSIM_SEED=42 cargo test ...
   ```

3. **Check time progression** in logs for timing-related issues

4. **Compare with non-MadSim** to isolate MadSim-specific behavior:
   ```bash
   # Without MadSim
   cargo test -p freenet --test sim_network

   # With MadSim
   RUSTFLAGS="--cfg madsim" cargo test -p freenet --test sim_network
   ```

### Cargo Configuration

The following configuration is already set up in `Cargo.toml`:

```toml
# Workspace Cargo.toml - patches for MadSim
[patch.crates-io]
getrandom = { git = "https://github.com/madsim-rs/getrandom.git", rev = "e79a7ae" }
quanta = { git = "https://github.com/madsim-rs/quanta.git", branch = "madsim" }

# crates/core/Cargo.toml - MadSim dependencies
[target.'cfg(madsim)'.dependencies]
madsim = "0.2"
madsim-tokio = "0.2"

# Lint configuration to allow madsim cfg
[lints.rust]
unexpected_cfgs = { level = "warn", check-cfg = ['cfg(madsim)'] }
```

---

## References

### Frameworks
- [MadSim](https://github.com/madsim-rs/madsim) - Recommended: Drop-in tokio replacement with simulation mode
- [Diviner](https://github.com/xxuejie/diviner) - FoundationDB-style deterministic testing for Rust
- [Turmoil](https://github.com/tokio-rs/turmoil) - Tokio's experimental deterministic network simulation
- [ODEM-rs](https://lib.rs/crates/odem-rs) - Object-based discrete-event modeling

### Articles
- [FoundationDB Testing](https://apple.github.io/foundationdb/testing.html) - Gold standard for deterministic simulation
- [S2: Deterministic Simulation Testing](https://s2.dev/blog/dst) - Practical DST implementation (2025)
- [Polar Signals: DST in Rust](https://www.polarsignals.com/blog/posts/2025/07/08/dst-rust) - State machine approach (2025)
- [Deterministic Simulation Testing Overview](https://notes.eatonphil.com/2024-08-20-deterministic-simulation-testing.html) - Phil Eaton's overview
