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

### Option D: Custom Deterministic Executor (High Effort, Full Determinism)

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

### Phase 3: Full Determinism (Optional, 4-6 weeks)

Only if Phase 1-2 insufficient:

| Task | Effort | Notes |
|------|--------|-------|
| Evaluate turmoil vs custom | 1 week | Research spike |
| Prototype chosen approach | 2 weeks | Core infrastructure |
| Full integration | 2-3 weeks | Node + transport |
| Testing & validation | 1 week | Verify determinism |

---

## Current vs Target Determinism

| Aspect | Current | Phase 1 | Phase 2 | Phase 3 |
|--------|---------|---------|---------|---------|
| Task scheduling | ❌ Multi-thread | ✅ Single-thread | ✅ Single-thread | ✅ Custom executor |
| Time control | ⚠️ Partial | ✅ Paused | ✅ VirtualTime | ✅ VirtualTime |
| Message order | ❌ Random | ⚠️ Better | ✅ Scheduled | ✅ Deterministic |
| RNG | ✅ Seeded | ✅ Seeded | ✅ Seeded | ✅ Seeded |
| **Reproducibility** | ~40% | ~70% | ~90% | ~99% |

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

**Proceed to Phase 3 if:**
- Phase 2 insufficient for bug reproduction
- Need Jepsen-style formal verification
- Team has capacity for infrastructure investment

---

## References

- [FoundationDB Testing](https://apple.github.io/foundationdb/testing.html) - Gold standard for deterministic simulation
- [Turmoil](https://github.com/tokio-rs/turmoil) - Tokio's deterministic network simulation
- [MadSim](https://github.com/madsim-rs/madsim) - Alternative with libc overrides
- [Deterministic Simulation Testing](https://notes.eatonphil.com/2024-08-20-deterministic-simulation-testing.html) - Overview article
