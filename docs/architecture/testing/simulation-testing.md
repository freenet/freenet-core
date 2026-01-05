# Simulation Testing Architecture

This document describes the deterministic simulation testing framework for Freenet Core.

## Overview

The simulation testing framework enables reproducible testing of the Freenet network by providing:

1. **Seeded randomness** - All random decisions use deterministic RNG
2. **Controlled time** - Virtual time that advances only when explicitly stepped
3. **Fault injection** - Configurable message loss, partitions, and latency
4. **Event capture** - Full visibility into network events for assertions

## Components

### Core Simulation Primitives (`crates/core/src/simulation/`)

| Component | File | Purpose |
|-----------|------|---------|
| `VirtualTime` | `time.rs` | Deterministic time with wakeup scheduling |
| `SimulationRng` | `rng.rs` | Thread-safe seeded RNG |
| `Scheduler` | `scheduler.rs` | Priority-queue event ordering |
| `SimulatedNetwork` | `network.rs` | Deterministic message delivery |
| `FaultConfig` | `fault.rs` | Message loss, partitions, crashes |

### Test Infrastructure (`crates/core/src/node/testing_impl.rs`)

| Component | Purpose |
|-----------|---------|
| `SimNetwork` | Async-based test network with actual nodes |
| `EventChain` | Drives events through the network |
| `TestEventListener` | Captures network events for verification |
| `EventSummary` | Structured event data for assertions |

## Two Simulation Systems

### 1. SimNetwork (Async-Based)

The existing `SimNetwork` uses the tokio async runtime:

```
SimNetwork
    ├── InMemoryTransport (per node)
    │   └── Message channels between nodes
    ├── TestEventListener (shared)
    │   └── Captures all network events
    └── EventChain
        └── Generates Put/Get/Subscribe events
```

**Characteristics:**
- Nodes run as actual async tasks
- Uses real time (`tokio::time`)
- Realistic concurrency behavior
- Non-deterministic event ordering (multi-threaded tokio)

### 2. SimulatedNetwork (Synchronous)

The new `SimulatedNetwork` is purely synchronous:

```
SimulatedNetwork
    ├── Scheduler (event queue)
    │   └── VirtualTime (controlled time)
    ├── FaultConfig
    │   └── Message loss, partitions
    └── Delivered queues (per peer)
```

**Characteristics:**
- Synchronous, event-driven
- Uses VirtualTime (fully controlled)
- Fully deterministic
- Not yet integrated with actual nodes

## Current Determinism Guarantees

### ✅ Fully Deterministic (Turmoil Always Enabled)

| Aspect | Mechanism |
|--------|-----------|
| RNG | `GlobalRng` - seeded, deterministic in simulation mode |
| Time | `VirtualTime` - explicit advancement via `sim.advance_time()` |
| Network | `SimulationSocket` - in-memory, uses VirtualTime |
| Peer label assignment | Derived from master seed |
| Contract generation | Seeded MemoryEventsGen |
| Fault injection | Seeded RNG for drop decisions |
| **Async task ordering** | ✅ Turmoil provides deterministic task scheduling |
| **Channel message order** | ✅ Deterministic with Turmoil |
| **`select!` branches** | ✅ Deterministic with Turmoil (use `biased` for clarity) |

### Full Determinism Achieved

Turmoil is always enabled as a dependency for deterministic scheduling. See [deterministic-simulation-roadmap.md](deterministic-simulation-roadmap.md) for implementation details.

## Known Gaps

### Gap 1: VirtualTime Partially Integrated - PHASE 2 COMPLETE

~~The `VirtualTime` abstraction exists but isn't wired into `SimNetwork`~~ **PARTIAL FIX**

VirtualTime is now integrated into the fault injection system:

```rust
use freenet::simulation::{FaultConfig, VirtualTime};

// Create VirtualTime instance
let vt = VirtualTime::new();

// Configure fault injection with VirtualTime mode
sim.with_fault_injection_virtual_time(
    FaultConfig::builder()
        .latency_range(Duration::from_millis(10)..Duration::from_millis(50))
        .build(),
    vt.clone(),
);

// Messages with latency are queued until VirtualTime advances
vt.advance(Duration::from_millis(100));
let delivered = sim.advance_virtual_time();
```

**What works (Phase 2):**
- VirtualTime integration in FaultInjectorState
- Messages with latency are queued with virtual deadlines
- `advance_virtual_time()` delivers queued messages
- Deterministic latency injection

**Still pending (Phase 3):**
- Replace tokio::time with VirtualTime throughout the codebase
- Full scheduler-based message ordering

### Gap 2: SimulatedNetwork Connected to Nodes ✅ RESOLVED

Fault injection bridge with deterministic RNG, latency support, and network statistics:

```
Current (with bridge):
  Node A → InMemoryTransport.send()
           ↓
           check_delivery(from, to) ← uses FaultInjectorState
           ↓
           FaultInjectorState {
             config: FaultConfig,
             rng: SimulationRng (seeded for determinism),
             virtual_time: Option<VirtualTime>,  // NEW
             pending_deliveries: Vec<PendingDelivery>,  // NEW
             stats: NetworkStats,  // NEW
           }
           ↓
           DeliveryDecision::Deliver | DelayedDelivery | QueuedDelivery | Drop
           ↓
           channel (with optional delay) → InMemoryTransport → Node B

Usage:
  let config = FaultConfig::builder()
      .message_loss_rate(0.1)
      .latency_range(Duration::from_millis(10)..Duration::from_millis(50))
      .partition(partition)
      .build();
  sim.with_fault_injection(config);  // Uses network's seed for determinism

  // Query network statistics
  if let Some(stats) = sim.get_network_stats() {
      println!("Loss rate: {:.1}%", stats.loss_ratio() * 100.0);
      println!("Dropped: {}", stats.total_dropped());
  }
```

**What works:**
- Message loss injection - deterministic with seeded RNG
- Network partitions (blocking messages between peer groups)
- Node crashes (blocking all messages to/from a node)
- Latency injection (real-time or VirtualTime mode)
- Network statistics tracking (Gap T5 - FIXED)

### Gap 3: Event Summary Uses Debug Parsing ✅ RESOLVED

`EventSummary` now has structured `contract_key` and `state_hash` fields:

```rust
pub struct EventSummary {
    pub tx: Transaction,
    pub peer_addr: SocketAddr,
    pub event_kind_name: String,
    pub contract_key: Option<String>,   // Structured field
    pub state_hash: Option<String>,      // Structured field
    pub event_detail: String,            // kept for backwards compatibility
}
```

Helper methods added to `EventKind`:
- `variant_name()` - returns event type name
- `contract_key()` - extracts contract key if applicable
- `state_hash()` - extracts state hash if applicable

### Gap 4: No Direct State Query ⚠️ PARTIAL (Event-based alternative available)

Cannot query `StateStore` from tests directly (nodes run as isolated async tasks).
Event-based state hashes are available as an alternative:

```rust
// Get state hashes for all contracts across all peers
let states = sim.get_contract_state_hashes().await;
// Returns: HashMap<String, HashMap<SocketAddr, String>> (contract_key -> peer -> hash)

for (contract, peer_states) in states {
    println!("Contract {}: {} replicas", contract, peer_states.len());
    for (peer, hash) in peer_states {
        println!("  {}: {}", peer, hash);
    }
}

// Get contract distribution summary
let distribution = sim.get_contract_distribution().await;
// Returns: Vec<ContractDistribution> with replica_count and peers list
```

**Note**: This provides state *hashes* via event observation, not full state content.
Direct StateStore access would require architectural changes (shared state references).

### Gap T4: Operation Completion Tracking - FIXED

**Now Available**: Operation tracking from request to completion:

```rust
// Get full operation summary
let summary = sim.get_operation_summary().await;
println!("Put: {}/{} ({:.1}% success)",
    summary.put.succeeded,
    summary.put.completed(),
    summary.put.success_rate() * 100.0);

// Check overall status
let (completed, pending) = sim.operation_completion_status().await;
println!("{} completed, {} pending", completed, pending);

// Wait for operations to complete
match sim.await_operation_completion(Duration::from_secs(30), Duration::from_millis(500)).await {
    Ok(summary) => println!("All operations completed: {} succeeded", summary.total_succeeded()),
    Err(summary) => println!("Timeout with {} pending", summary.total_requested() - summary.total_completed()),
}

// Assert minimum success rate
sim.assert_operation_success_rate(0.95).await; // Panics if < 95% success

// Operation summary includes:
// - put: requested, succeeded, failed, broadcasts_emitted, broadcasts_received
// - get: requested, succeeded, failed
// - subscribe: requested, succeeded (via SubscribeSuccess), failed (via SubscribeNotFound)
// - update: requested, succeeded, broadcasts_received
// - timeouts: count of timed-out operations
```

## Test Files

| File | Tests |
|------|-------|
| `simulation_integration.rs` | End-to-end determinism tests |
| `simulation_determinism.rs` | Unit tests for primitives |
| `sim_network.rs` | SimNetwork connectivity tests |

## Usage Examples

### Deterministic Replay Test

```rust
#[tokio::test(flavor = "current_thread")]
async fn test_deterministic_replay() {
    const SEED: u64 = 0x1234;

    // Run 1
    let mut sim1 = SimNetwork::new("test", 1, 3, 7, 3, 10, 2, SEED).await;
    let _handles = sim1.start_with_rand_gen::<SmallRng>(SEED, 5, 10).await;

    // Advance time using VirtualTime
    for _ in 0..30 {
        sim1.advance_time(Duration::from_millis(100));
        tokio::task::yield_now().await;
    }
    let events1 = sim1.get_event_counts().await;

    // Run 2 (same seed)
    let mut sim2 = SimNetwork::new("test", 1, 3, 7, 3, 10, 2, SEED).await;
    let _handles = sim2.start_with_rand_gen::<SmallRng>(SEED, 5, 10).await;

    for _ in 0..30 {
        sim2.advance_time(Duration::from_millis(100));
        tokio::task::yield_now().await;
    }
    let events2 = sim2.get_event_counts().await;

    // Same event types should appear
    assert_eq!(events1.keys().collect::<HashSet<_>>(),
               events2.keys().collect::<HashSet<_>>());
}
```

### Fault Injection Test

```rust
#[tokio::test(flavor = "current_thread")]
async fn test_with_fault_injection() {
    let mut sim = SimNetwork::new("fault-test", 1, 3, 7, 3, 10, 2, SEED).await;

    // Configure fault injection with message loss and latency
    sim.with_fault_injection(FaultConfig::builder()
        .message_loss_rate(0.1)  // 10% packet loss
        .latency_range(Duration::from_millis(10)..Duration::from_millis(50))
        .build());

    let _handles = sim.start_with_rand_gen::<SmallRng>(SEED, 5, 10).await;

    // Advance VirtualTime to allow pending messages to be delivered
    for _ in 0..50 {
        sim.advance_time(Duration::from_millis(100));
        tokio::task::yield_now().await;
    }

    // Check network stats
    if let Some(stats) = sim.get_network_stats() {
        println!("Loss rate: {:.1}%", stats.loss_ratio() * 100.0);
    }
}
```

## Future Work

### Completed ✅

1. **VirtualTime** - ✅ DONE: VirtualTime integrated via `sim.advance_time()`
2. **Fault injection bridge** - ✅ DONE: Bridge with deterministic RNG
3. **State query** - ✅ DONE: Event-based state hashes via `get_contract_state_hashes()`
4. **Structured EventSummary** - ✅ DONE: Added typed fields
5. **Operation completion tracking** - ✅ DONE: `get_operation_summary()`, `await_operation_completion()`
6. **Single-threaded mode** - ✅ DONE: All tests use `current_thread`
7. **TimeSource trait** - ✅ DONE: `TimeSource` trait throughout codebase
8. **GlobalRng** - ✅ DONE: `rand::random()` → `GlobalRng`
9. **Turmoil integration** - ✅ DONE: Always enabled for deterministic scheduling

### Recently Completed

9. **Deterministic Scheduler** ✅ COMPLETE
   - Turmoil integrated and always enabled
   - Full determinism achieved (~99%)

### Future Enhancements (Enabled by Determinism)

10. **Linearizability Checker** - Jepsen/Knossos-style operation history verification (now possible)
11. **Property-based Testing** - Integration with proptest/quickcheck with shrinking (now possible)
12. **Invariant Checking DSL** - Declarative system invariants

---

## Test Infrastructure Gaps (fdev & SimNetwork)

This section documents gaps in the current testing infrastructure identified through analysis.

### Gap T1: Connectivity Verification Details

**Current State:**
- `check_partial_connectivity()` uses `event_listener.is_connected()` which checks for Connect events
- This is generally sufficient - Connect events are ground truth for actual connections
- FIXME in `network.rs` refers to a specific reporting detail, not a fundamental issue

**When Event Logging Works Well:**
- Verifying peers connected to each other (Connect events are factual)
- Counting connections per peer
- Checking network formation over time

**Edge Cases Where More Might Be Needed:**
- Multi-hop reachability analysis (can A route to C via B?)
- Ring topology structure validation
- These are protocol verification concerns, not typical test requirements

### Gap T2: Eventual Consistency Tests Don't Assert Convergence - FIXED

~~**Current State:**~~
~~- `test_eventual_consistency_state_hashes()` captures state_hash fields in events~~
~~- Compares final hashes across peers for the same contract key~~
~~- **Explicitly doesn't assert convergence:** "We don't strictly assert 100% consistency"~~

**FIX IMPLEMENTED:** Convergence assertion helpers:

```rust
// Check current convergence state
let result = sim.check_convergence().await;
println!("Converged: {}, Diverged: {}", result.converged.len(), result.diverged.len());

// Wait for convergence with timeout
let result = sim.await_convergence(
    Duration::from_secs(30),
    Duration::from_millis(500),
    1,  // minimum contracts
).await;

// Assert convergence or panic with details
sim.assert_convergence(Duration::from_secs(30), Duration::from_millis(500)).await;

// Get convergence rate
let rate = sim.convergence_rate().await;
assert!(rate >= 0.95, "Expected 95%+ convergence");
```

**What's available now:**
- `check_convergence()` - instant snapshot of convergence state
- `await_convergence()` - poll until converged or timeout
- `assert_convergence()` - assert + panic with detailed diff
- `convergence_rate()` - numeric ratio for monitoring
- `ConvergenceResult`, `ConvergedContract`, `DivergedContract` - structured result types

### Gap T3: State Query Requires Event Inference

**Current State:**
- State verification happens only through event capture
- `node_connectivity()` returns connections from event_listener logs, not actual peer ring state
- No direct query API for peer state

**What's Missing:**
- Test API: `sim.peer_state(peer_label, contract_key) -> Result<WrappedState>`
- Consensus verification: `sim.verify_contract_replicas(contract_key) -> Result<HashSet<StateHash>>`
- State audit trail: `sim.get_state_history(contract_key) -> Vec<(Timestamp, Peer, Hash)>`

### Gap T4: Operations Generated But Not Verified to Complete - FIXED

~~**Current State:**~~
~~- EventChain generates operations but doesn't verify they execute~~
~~- Tests count events captured but don't verify operations succeeded~~
~~- Operations can silently fail and test still passes~~

**Now Available** (see Gap T4 above for usage):
- ✅ `get_operation_summary()` - Full operation tracking
- ✅ `operation_completion_status()` - (completed, pending) counts
- ✅ `await_operation_completion()` - Wait for operations to complete
- ✅ `assert_operation_success_rate()` - Assert minimum success rate
- ✅ Broadcast tracking for Put and Update operations

### Gap T5: Fault Injection Effects Not Measured - FIXED

~~**Current State:**~~
~~- FaultConfig supports message_loss_rate, latency_range, partition, crashed_node~~
~~- Tests verify fault injection doesn't crash the framework, not actual behavior changes~~

**FIX IMPLEMENTED:** Network statistics tracking:

```rust
// Configure fault injection
sim.with_fault_injection(
    FaultConfig::builder()
        .message_loss_rate(0.1)
        .latency_range(Duration::from_millis(10)..Duration::from_millis(50))
        .build()
);

// After running test operations...
if let Some(stats) = sim.get_network_stats() {
    println!("Messages sent: {}", stats.messages_sent);
    println!("Messages delivered: {}", stats.messages_delivered);
    println!("Dropped (loss): {}", stats.messages_dropped_loss);
    println!("Dropped (partition): {}", stats.messages_dropped_partition);
    println!("Dropped (crash): {}", stats.messages_dropped_crash);
    println!("Total dropped: {}", stats.total_dropped());
    println!("Loss ratio: {:.1}%", stats.loss_ratio() * 100.0);
    println!("Average latency: {:?}", stats.average_latency());

    // Verify fault injection worked as expected
    let actual_loss = stats.loss_ratio();
    assert!((actual_loss - 0.1).abs() < 0.05, "Loss rate ~10%");
}

// Reset stats for next phase
sim.reset_network_stats();
```

**What's available now:**
- `NetworkStats` struct with detailed message counters
- `get_network_stats()` - returns current stats
- `reset_network_stats()` - reset for new test phase
- Separate counters for loss, partition, and crash drops
- Latency tracking metrics

### Gap Summary Table

| Gap | Severity | Status | Notes |
|-----|----------|--------|-------|
| T1: Connectivity verification | LOW | Open | Event logging is sufficient for most cases |
| T2: Convergence assertions | HIGH | **FIXED** | `check_convergence()`, `await_convergence()`, `assert_convergence()` |
| T3: Direct state query | MEDIUM | Partial | Event-based state hashes via `get_contract_state_hashes()` |
| T4: Operation completion | MEDIUM | **FIXED** | `get_operation_summary()`, `await_operation_completion()` |
| T5: Fault effect measurement | MEDIUM | **FIXED** | `NetworkStats` with detailed counters |

---

## VirtualTime Integration Plan

Integrating VirtualTime with the fault injection bridge requires bridging the gap between:
- **VirtualTime**: Synchronous, advances only when explicitly stepped via `advance_to()`
- **Tokio runtime**: Async, uses real wall-clock time

### Option A: Queue-Based Delayed Delivery - IMPLEMENTED ✓

Instead of spawning async tasks with `tokio::time::sleep`, queue messages with virtual deadlines:

```rust
pub struct FaultInjectorState {
    pub config: FaultConfig,
    pub rng: SimulationRng,
    pub virtual_time: Option<VirtualTime>,  // IMPLEMENTED
    pub pending_deliveries: Vec<PendingDelivery>,  // IMPLEMENTED
    pub stats: NetworkStats,  // IMPLEMENTED
}

struct PendingDelivery {
    deadline: u64,  // virtual nanos
    msg: MessageOnTransit,
    target_addr: SocketAddr,
}

enum DeliveryDecision {
    Deliver,
    DelayedDelivery(Duration),  // Current: real time
    QueuedDelivery(u64),        // NEW: virtual deadline
    Drop,
}
```

**Changes Required:**

1. **Add VirtualTime to FaultInjectorState** (optional field)
2. **Queue messages when VirtualTime is enabled:**
   ```rust
   if let Some(latency) = config.generate_latency(&rng) {
       if let Some(ref vt) = state.virtual_time {
           let deadline = vt.now_nanos() + latency.as_nanos() as u64;
           state.pending_deliveries.push(PendingDelivery { deadline, msg, target_addr });
           return DeliveryDecision::QueuedDelivery(deadline);
       } else {
           return DeliveryDecision::DelayedDelivery(latency);
       }
   }
   ```

3. **Add time advancement method:**
   ```rust
   impl FaultInjectorState {
       pub fn advance_time(&mut self) -> usize {
           let Some(ref vt) = self.virtual_time else { return 0 };

           let now = vt.now_nanos();
           let mut delivered = 0;

           // Drain messages with deadline <= now
           while let Some(idx) = self.pending_deliveries.iter()
               .position(|p| p.deadline <= now)
           {
               let pending = self.pending_deliveries.remove(idx);
               let registry = PEER_REGISTRY.read().unwrap();
               let _ = registry.send(pending.target_addr, pending.msg);
               delivered += 1;
           }
           delivered
       }
   }
   ```

4. **Add API to SimNetwork:**
   ```rust
   impl SimNetwork {
       /// Enables VirtualTime mode for deterministic latency injection.
       pub fn with_virtual_time(&mut self) {
           // Store VirtualTime instance
       }

       /// Advances virtual time to next pending wakeup and delivers messages.
       pub fn advance_virtual_time(&mut self) -> usize {
           // Advance VirtualTime, deliver pending messages
       }
   }
   ```

### Option B: Tokio's pause/advance API

Tokio provides `tokio::time::pause()` and `tokio::time::advance()`:

```rust
#[tokio::test(start_paused = true)]
async fn test_with_controlled_time() {
    // Time is paused, sleep won't progress until we advance
    let sleep = tokio::time::sleep(Duration::from_secs(100));

    tokio::time::advance(Duration::from_secs(50)).await;
    // sleep still pending

    tokio::time::advance(Duration::from_secs(50)).await;
    // sleep completes
}
```

**Pros:** Works with existing tokio ecosystem
**Cons:** Coarser control than VirtualTime, still uses Duration not precise nanos

### Option C: Full Runtime Replacement (turmoil-style)

Replace the entire runtime with a deterministic simulation runtime:

```rust
// Instead of tokio::spawn
sim_runtime.spawn(async { ... });

// All I/O and time goes through simulation
sim_runtime.advance_until_idle();
```

**Pros:** Complete determinism
**Cons:** Major refactoring, different from production code path

### Recommended Approach

**Phase 1** (Current): Real-time latency injection ✅
- Uses `tokio::time::sleep` for delays
- Deterministic drop decisions via seeded RNG

**Phase 2** (Proposed): Optional VirtualTime mode
- Add `FaultInjectorState.virtual_time: Option<VirtualTime>`
- Queue delayed messages when VirtualTime enabled
- Tests explicitly advance time with `sim.advance_virtual_time()`
- Falls back to real-time when VirtualTime not configured

**Phase 3** (Future): Tokio pause integration
- Use `#[tokio::test(start_paused = true)]` for deterministic tests
- Combine with VirtualTime for message ordering control

### Implementation Estimate

| Task | Effort | Files |
|------|--------|-------|
| Add VirtualTime to FaultInjectorState | Small | `in_memory.rs` |
| Add pending_deliveries queue | Small | `in_memory.rs` |
| Add advance_time() method | Small | `in_memory.rs` |
| Add with_virtual_time() to SimNetwork | Small | `testing_impl.rs` |
| Add tests | Medium | `simulation_integration.rs` |
| Documentation | Small | `simulation-testing.md` |

**Total: ~1-2 days of focused work**

---

## Deterministic Scheduling ✅ ACHIEVED

~~This section explores what it would take to run actual Freenet node code with fully deterministic scheduling.~~

**UPDATE**: Full deterministic scheduling is now implemented via Turmoil!

### Current State (January 2026)

**SimNetwork with Turmoil** (production-ready, always enabled):
- ✅ Runs actual node code
- ✅ Deterministic fault injection (seeded RNG)
- ✅ Network stats and convergence checking
- ✅ **Fully deterministic async scheduling** (Turmoil)
- ✅ **Controlled timing** (VirtualTime + Turmoil)
- ✅ **~99% determinism** - same seed → identical execution

### ✅ Goals Achieved

Running actual Freenet protocol code (operations, state machines, contract execution) with:
1. ✅ Deterministic event ordering (same seed → same execution) **- Turmoil**
2. ✅ Controlled time progression (no wall-clock dependency) **- VirtualTime + Turmoil**
3. ✅ Reproducible test failures **- Always enabled**

### Approaches

#### Approach 1: Single-Threaded Tokio with Paused Time

**Complexity: Low**
**Determinism: Partial**

Use tokio's test utilities for coarse-grained control:

```rust
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn deterministic_test() {
    let mut sim = SimNetwork::new(...).await;
    sim.with_fault_injection(config);

    // Time is paused - we control when it advances
    let handles = sim.start_with_rand_gen::<SmallRng>(SEED, 10, 5).await;

    // Advance time in controlled steps
    for _ in 0..100 {
        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;  // Let tasks run
    }

    sim.assert_convergence(...).await;
}
```

**Limitations:**
- Task execution order within a time slice is still non-deterministic
- Cannot interleave message delivery with precise control
- Good enough for many tests, not for bug reproduction

#### Approach 2: Deterministic Executor (turmoil-style)

**Complexity: High**
**Determinism: Full**

Replace tokio entirely with a custom deterministic runtime:

```rust
// Conceptual design
pub struct DeterministicRuntime {
    scheduler: Scheduler,
    time: VirtualTime,
    tasks: BTreeMap<TaskId, Task>,
    ready_queue: VecDeque<TaskId>,  // FIFO for determinism
}

impl DeterministicRuntime {
    /// Process one task step
    pub fn step(&mut self) -> bool {
        // 1. Check for time-based wakeups
        self.process_wakeups();

        // 2. Run one ready task
        if let Some(task_id) = self.ready_queue.pop_front() {
            self.poll_task(task_id);
            return true;
        }

        // 3. If no tasks ready, advance time to next event
        if let Some(next_time) = self.scheduler.next_event_time() {
            self.time.advance_to(next_time);
            return true;
        }

        false  // Simulation complete
    }

    /// Run until condition or deadlock
    pub fn run_until<F>(&mut self, condition: F) -> Result<(), SimError>
    where
        F: Fn(&Self) -> bool,
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

**Implementation Requirements:**
1. Custom Future executor with deterministic task ordering
2. Shim for `tokio::time` operations (sleep, timeout, interval)
3. Shim for channel operations (mpsc, oneshot, broadcast)
4. Shim for I/O operations (replaced with InMemoryTransport)
5. Thread-local context for runtime access

**Estimated Effort:** 2-4 weeks

**Reference:** [Turmoil](https://github.com/tokio-rs/turmoil) - deterministic testing for distributed systems (actively used in Freenet)

#### Approach 3: Record-Replay

**Complexity: Medium**
**Determinism: Replay only**

Record all non-deterministic decisions during a test run, replay them later:

```rust
pub struct RecordingRuntime {
    inner: tokio::Runtime,
    log: Vec<NonDetEvent>,
}

pub enum NonDetEvent {
    TaskScheduled { task_id: u64, time: Instant },
    ChannelRecv { channel_id: u64, result: Option<Vec<u8>> },
    TimeNow { value: Instant },
}

// During recording, capture all non-deterministic events
// During replay, return recorded values instead of real ones
```

**Limitations:**
- Only reproduces existing failures, doesn't prevent new ones
- Recording overhead
- Log files can be large

### Recommended Path Forward

**Short-term (fdev improvements):**
1. Add fault injection config to fdev CLI ✓ (implementing)
2. Add operation tracking/reporting ✓ (implementing)
3. Add convergence assertions ✓ (implementing)
4. Use single-threaded tokio for better reproducibility

**Medium-term (better determinism):**
1. Add `#[tokio::test(flavor = "current_thread", start_paused = true)]` support
2. Integrate VirtualTime with tokio pause for message ordering
3. Add seeded task ID assignment for consistent ordering

**Long-term (full determinism):**
1. Evaluate turmoil for inspiration
2. Build deterministic executor if needed
3. Or: Accept that current level is "good enough" for most testing

### When Full Determinism Matters

Full determinism is most valuable for:
- **Bug reproduction:** "This failed on CI, let me reproduce locally"
- **Property testing:** "Run 10,000 variations and find edge cases"
- **Formal verification:** "Prove this never deadlocks"

For typical integration testing, SimNetwork + fault injection is sufficient.
