# Deterministic Simulation Roadmap

This document analyzes what it would take to make Freenet's simulation tests fully deterministic.

## Current State

SimNetwork achieves **partial determinism** through:
- ✅ Seeded RNG for all random decisions
- ✅ VirtualTime abstraction for time control
- ✅ In-memory transport (no real network I/O)
- ✅ MockStateStorage (no disk I/O)
- ✅ Fault injection with deterministic drop decisions

But **full determinism is blocked by**:
- ❌ Multi-threaded tokio scheduler (task ordering varies)
- ❌ Real wall-clock time in 257+ locations
- ❌ Channel message ordering non-deterministic
- ❌ `select!` macro branch selection

## Sources of Non-Determinism

### 1. Tokio Task Scheduling (CRITICAL)

**Problem:** Multi-threaded tokio uses work-stealing scheduler where task execution order is non-deterministic.

**Locations:**
- `testing_impl.rs:1196,1219,1237,1297` - `tokio::time::sleep()` calls
- `testing_impl/in_memory.rs:74,89,121,212,231,262` - `GlobalExecutor::spawn()` calls

**Impact:** Same seed can produce different event orderings across runs.

### 2. Real Wall-Clock Time (HIGH)

**Problem:** 257+ `tokio::time::` calls and 129+ `Instant::now()` calls use real time.

**Major Areas:**
| Area | Files | Calls | Impact |
|------|-------|-------|--------|
| Transport/LEDBAT | 7 files | 155+ | Congestion control timing |
| Contract Executor | 2 files | 36+ | Operation timeouts |
| Ring/Topology | 2 files | 22+ | Peer discovery intervals |
| Client Events | 4 files | 45+ | Streaming timeouts |

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

### Option A: Single-Threaded Tokio (Low Effort, Partial Determinism)

**Configuration:**
```rust
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn deterministic_test() {
    // Time only advances when we say
    tokio::time::advance(Duration::from_millis(100)).await;
}
```

**What it solves:**
- ✅ Eliminates thread scheduling randomness
- ✅ Allows controlled time progression
- ✅ Tasks run sequentially

**What remains non-deterministic:**
- ❌ Message delivery order (channel races)
- ❌ Task wake-up order (multiple futures ready)
- ❌ Select! branch selection

**Effort:** 1-2 days
**Determinism achieved:** ~70-80%

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

### Option C: Turmoil Integration (Medium Effort, High Determinism)

[Turmoil](https://github.com/tokio-rs/turmoil) is Tokio's experimental deterministic simulation framework.

**How it works:**
- Single-threaded execution across all "hosts"
- Each host gets its own tokio Runtime
- Explicit time control via simulation stepping
- Simulated network with tokio::net-compatible types

**API Example:**
```rust
let mut sim = Builder::new().build();

sim.host("server", || async move {
    let listener = TcpListener::bind("0.0.0.0:8080").await?;
    // ...
});

sim.client("test", async move {
    // Test code
});

// Run simulation
sim.run().unwrap();
```

**Challenges for Freenet:**
- Experimental framework (breaking changes possible)
- Requires auditing all dependencies for non-determinism
- Different code path than production
- Limited community adoption

**Effort:** 2-3 weeks for prototype, ongoing maintenance
**Determinism achieved:** ~95%

### Option D: GlobalExecutor Abstraction + MadSim (Recommended)

**Current State:**
```
GlobalExecutor::spawn usage:  27 calls in 10 files (18%)
Direct tokio::spawn usage:   119 calls in 31 files (82%)
```

**Key Insight:** `GlobalExecutor::spawn` already exists as an abstraction point. By:
1. Making it the canonical spawn method
2. Adding a feature flag to swap implementations
3. Using MadSim's package substitution approach

We can achieve high determinism with incremental effort.

**MadSim Approach:**

[MadSim](https://github.com/madsim-rs/madsim) provides drop-in tokio replacement via Cargo:

```toml
# Normal builds use real tokio
[dependencies]
tokio = "1.0"

# Simulation builds swap to madsim-tokio
[target.'cfg(madsim)'.dependencies]
tokio = { version = "1.0", package = "madsim-tokio" }

[target.'cfg(madsim)'.patch.crates-io]
getrandom = { git = "https://github.com/madsim-rs/getrandom.git" }
```

Run with: `RUSTFLAGS="--cfg madsim" cargo test`

**Required Changes for Freenet:**

1. **Canonicalize GlobalExecutor::spawn** (migrate 119 → 0 direct tokio::spawn):

```rust
// Current GlobalExecutor (just wraps tokio)
pub struct GlobalExecutor;

impl GlobalExecutor {
    pub fn spawn<R: Send + 'static>(
        f: impl Future<Output = R> + Send + 'static,
    ) -> tokio::task::JoinHandle<R> {
        tokio::runtime::Handle::current().spawn(f)
    }
}
```

No changes needed to GlobalExecutor itself—MadSim swaps tokio at the package level.

2. **Migrate direct spawns** (119 calls across 31 files):

| Category | Files | Calls | Effort |
|----------|-------|-------|--------|
| Transport | 10 files | 62 | 2-3 days |
| Client Events | 4 files | 15 | 1 day |
| Node/Bridge | 6 files | 24 | 1-2 days |
| Operations | 4 files | 4 | 0.5 day |
| Other | 7 files | 14 | 1 day |

3. **Add madsim dependencies**:
```toml
[target.'cfg(madsim)'.dependencies]
madsim = "0.2"
tokio = { version = "1.0", package = "madsim-tokio" }

[target.'cfg(madsim)'.patch.crates-io]
getrandom = { git = "https://github.com/madsim-rs/getrandom.git" }
quanta = { git = "https://github.com/madsim-rs/quanta.git" }
```

**Alternative: Diviner**

[Diviner](https://github.com/xxuejie/diviner) is a FoundationDB-style executor requiring wrapper types:

```rust
// Diviner requires explicit wrapper types
use diviner::time::sleep;  // Not tokio::time::sleep
use diviner::net::TcpListener;  // Not tokio::net::TcpListener
```

More invasive than MadSim but provides finer control.

**Effort:** 1-2 weeks (primarily mechanical migration)
**Determinism achieved:** ~95%

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

### Phase 1: Quick Wins (1-2 weeks)

| Task | Effort | Impact |
|------|--------|--------|
| Switch SimNetwork tests to `current_thread` | 1 day | Eliminate thread scheduling |
| Add `start_paused = true` to test macro | 1 day | Control time progression |
| Document determinism limits | 1 day | Set expectations |

**Configuration:**
```rust
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_with_better_determinism() {
    let mut sim = SimNetwork::new(..., SEED).await;

    // Advance time in controlled steps
    for _ in 0..100 {
        tokio::time::advance(Duration::from_millis(10)).await;
    }

    sim.assert_convergence(...).await;
}
```

### Phase 2: VirtualTime Integration (3-4 weeks)

| Task | Effort | Files |
|------|--------|-------|
| Implement VirtualInterval | 2-3 days | simulation/time.rs |
| Inject TimeSource into transport | 5-7 days | 7 transport files |
| Replace ring/topology intervals | 3-5 days | 2 ring files |
| Replace executor timeouts | 2-3 days | 2 contract files |
| Integration tests | 3-5 days | New test files |

### Phase 3: GlobalExecutor Canonicalization + MadSim (Recommended, 1-2 weeks)

**This is the recommended approach** - achieves ~95% determinism with primarily mechanical changes.

| Task | Effort | Notes |
|------|--------|-------|
| Migrate transport spawns to GlobalExecutor | 2-3 days | 62 calls in 10 files |
| Migrate client_events spawns | 1 day | 15 calls in 4 files |
| Migrate node/bridge spawns | 1-2 days | 24 calls in 6 files |
| Migrate remaining spawns | 1.5 days | 18 calls in 11 files |
| Add MadSim Cargo configuration | 0.5 day | Cargo.toml changes |
| Validation with seeded tests | 1-2 days | Verify reproducibility |

**Why MadSim over alternatives:**
- Drop-in tokio replacement (no API changes needed)
- Used in production by RisingWave
- Package substitution via Cargo (no code changes after spawn migration)
- Patches getrandom/quanta for full determinism

### Phase 4: Full Determinism (Optional, 4-6 weeks)

Only if Phase 3 insufficient:

| Task | Effort | Notes |
|------|--------|-------|
| Custom deterministic executor | 4-6 weeks | FoundationDB-style |
| Deterministic channel shims | 1-2 weeks | Replace tokio::sync |
| Full replay infrastructure | 1 week | Recording + playback |

---

## Current vs Target Determinism

| Aspect | Current | Phase 1 | Phase 2 | Phase 3 (MadSim) | Phase 4 |
|--------|---------|---------|---------|------------------|---------|
| Task scheduling | ❌ Multi-thread | ✅ Single-thread | ✅ Single-thread | ✅ MadSim executor | ✅ Custom executor |
| Time control | ⚠️ Partial | ✅ Paused | ✅ VirtualTime | ✅ MadSim time | ✅ VirtualTime |
| Message order | ❌ Random | ⚠️ Better | ✅ Scheduled | ✅ MadSim channels | ✅ Deterministic |
| RNG | ✅ Seeded | ✅ Seeded | ✅ Seeded | ✅ Patched getrandom | ✅ Seeded |
| **Reproducibility** | ~40% | ~70% | ~90% | ~95% | ~99% |

---

## Decision Criteria

**Choose Phase 1 only if:**
- Most test flakiness is from thread scheduling
- Debugging doesn't require exact replay
- Quick improvement needed

**Proceed to Phase 2 if:**
- Need to reproduce timing-sensitive bugs
- Message delivery order matters for correctness
- Transport layer bugs need deterministic reproduction

**Proceed to Phase 3 (MadSim - Recommended) if:**
- Want ~95% determinism with minimal code changes
- Need production-proven framework (used by RisingWave)
- Team can invest 1-2 weeks for spawn migration

**Proceed to Phase 4 if:**
- Phase 3 insufficient for bug reproduction
- Need Jepsen-style formal verification
- Need 99%+ reproducibility guarantee

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
