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

### What IS Deterministic

| Aspect | Mechanism |
|--------|-----------|
| RNG seeds | Flow from SimNetwork → peer → transport |
| Noise mode shuffle | Based on message content hash (FNV-1a) |
| Peer label assignment | Derived from master seed |
| Contract generation | Seeded MemoryEventsGen |

### What is NOT Deterministic

| Aspect | Reason | Mitigation |
|--------|--------|------------|
| Tokio scheduling | Multi-threaded async runtime | Assert on event types, not counts |
| Event ordering | Thread scheduling varies | Use HashSet comparisons |
| Timing | Real time varies | VirtualTime exists but not integrated |

## Known Gaps

### Gap 1: VirtualTime Not Integrated

The `VirtualTime` abstraction exists but isn't wired into `SimNetwork`:

```rust
// Current: uses real time
tokio::time::sleep(Duration::from_secs(3)).await;

// Future: would use virtual time
virtual_time.sleep(Duration::from_secs(3)).await;
```

**Required for full integration:**
1. Replace `tokio::time` with `VirtualTime`
2. Use a deterministic async executor
3. Control all timing through the scheduler

### Gap 2: SimulatedNetwork Not Connected to Nodes - MOSTLY FIXED

~~`SimulatedNetwork` provides message routing but doesn't connect to actual nodes~~

**FIX IMPLEMENTED**: A fault injection bridge with deterministic RNG and latency support:

```
Current (with bridge):
  Node A → InMemoryTransport.send()
           ↓
           check_delivery(from, to) ← uses FaultInjectorState
           ↓
           FaultInjectorState {
             config: FaultConfig,
             rng: SimulationRng (seeded for determinism)
           }
           ↓
           DeliveryDecision::Deliver | DelayedDelivery(duration) | Drop
           ↓
           channel (with optional delay) → InMemoryTransport → Node B

Usage:
  let config = FaultConfig::builder()
      .message_loss_rate(0.1)
      .latency_range(Duration::from_millis(10)..Duration::from_millis(50))
      .partition(partition)
      .build();
  sim.with_fault_injection(config);  // Uses network's seed for determinism
```

**What works:**
- Message loss injection - deterministic with seeded RNG
- Network partitions (blocking messages between peer groups)
- Node crashes (blocking all messages to/from a node)
- Latency injection (delays message delivery by configured duration)

**What doesn't work (yet):**
- VirtualTime integration (latency uses real tokio::time::sleep)
- Full scheduler-based message ordering

### Gap 3: Event Summary Uses Debug Parsing - FIXED

~~`EventSummary` extracts fields by parsing debug strings~~ **RESOLVED**

`EventSummary` now has structured `contract_key` and `state_hash` fields:

```rust
pub struct EventSummary {
    pub tx: Transaction,
    pub peer_addr: SocketAddr,
    pub event_kind_name: String,
    pub contract_key: Option<String>,   // NEW: structured field
    pub state_hash: Option<String>,      // NEW: structured field
    pub event_detail: String,            // kept for backwards compatibility
}
```

Helper methods added to `EventKind`:
- `variant_name()` - returns event type name
- `contract_key()` - extracts contract key if applicable
- `state_hash()` - extracts state hash if applicable

### Gap 4: No Direct State Query

Cannot query `StateStore` from tests:

```rust
// Current: only event observation
let summary = sim.get_deterministic_event_summary().await;

// Future: direct state query
let states = sim.get_contract_states(key).await;
// Returns: HashMap<NodeLabel, Option<WrappedState>>
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
#[tokio::test]
async fn test_deterministic_replay() {
    const SEED: u64 = 0x1234;

    // Run 1
    let mut sim1 = SimNetwork::new("test", 1, 3, ..., SEED).await;
    let _handles = sim1.start_with_rand_gen::<SmallRng>(SEED, 5, 10).await;
    let events1 = sim1.get_event_counts().await;

    // Run 2 (same seed)
    let mut sim2 = SimNetwork::new("test", 1, 3, ..., SEED).await;
    let _handles = sim2.start_with_rand_gen::<SmallRng>(SEED, 5, 10).await;
    let events2 = sim2.get_event_counts().await;

    // Same event types should appear
    assert_eq!(events1.keys().collect::<HashSet<_>>(),
               events2.keys().collect::<HashSet<_>>());
}
```

### Fault Injection Test

```rust
#[tokio::test]
async fn test_with_noise() {
    let mut sim = SimNetwork::new("fault-test", 1, 3, ..., SEED).await;
    sim.with_noise(); // Enable deterministic noise mode

    let _handles = sim.start_with_rand_gen::<SmallRng>(SEED, 5, 10).await;

    // Noise mode shuffles ~20% of messages deterministically
    // based on message content hash
}
```

## Future Work

1. **Integrate VirtualTime** - Replace tokio time with VirtualTime
2. **Connect SimulatedNetwork** - Route messages through scheduler
3. **Add StateStore query** - Direct state comparison across peers
4. ~~**Structured EventSummary**~~ - DONE: Added typed fields
5. **Single-threaded mode** - Option for `flavor = "current_thread"`

---

## Test Infrastructure Gaps (fdev & SimNetwork)

This section documents gaps in the current testing infrastructure identified through analysis.

### Gap T1: Connectivity Verification is Event-Based, Not Graph-Based

**Current State:**
- `check_partial_connectivity()` in `testing_impl.rs` relies on `event_listener.is_connected()`
- Only checks if a peer has emitted a "connected" event—not actual network reachability
- Critical FIXME in `network.rs`: "we are getting connectivity check that is not real since peers are not reporting if they are connected or not to other peers"

**What's Missing:**
- Actual peer-graph connectivity verification (can peer A reach peer B through the ring?)
- Deterministic event scheduling for node startup and peer discovery
- Explicit synchronization barriers for topology establishment

### Gap T2: Eventual Consistency Tests Don't Assert Convergence

**Current State:**
- `test_eventual_consistency_state_hashes()` captures state_hash fields in events
- Compares final hashes across peers for the same contract key
- **Explicitly doesn't assert convergence:** "We don't strictly assert 100% consistency"

**What's Missing:**
- Explicit convergence criteria: "all replicas must have state_hash X by time T"
- Convergence timeout that fails tests if not achieved
- Causality tracking: verify that updates are applied in consistent order
- Partition heal verification: "after partition ends, convergence happens within duration D"

### Gap T3: State Query Requires Event Inference

**Current State:**
- State verification happens only through event capture
- `node_connectivity()` returns connections from event_listener logs, not actual peer ring state
- No direct query API for peer state

**What's Missing:**
- Test API: `sim.peer_state(peer_label, contract_key) -> Result<WrappedState>`
- Consensus verification: `sim.verify_contract_replicas(contract_key) -> Result<HashSet<StateHash>>`
- State audit trail: `sim.get_state_history(contract_key) -> Vec<(Timestamp, Peer, Hash)>`

### Gap T4: Operations Generated But Not Verified to Complete

**Current State:**
- EventChain generates operations but doesn't verify they execute
- Tests count events captured but don't verify operations succeeded
- Operations can silently fail and test still passes

**What's Missing:**
- Operation completion tracking: "X out of Y operations completed successfully"
- Response validation: verify Get returns correct state
- Broadcast propagation verification: confirm updates reached all subscribers

### Gap T5: Fault Injection Effects Not Measured

**Current State:**
- FaultConfig supports message_loss_rate, latency_range, partition, crashed_node
- Tests verify fault injection doesn't crash the framework, not actual behavior changes

**What's Missing:**
- Fault observation: `network.stats()` returns dropped/partitioned/latency counts
- Message drop rate verification: measure actual drop ratio vs configured rate
- Recovery verification: "after fault clears, convergence within duration D"

### Gap Summary Table

| Gap | Severity | Current Workaround |
|-----|----------|-------------------|
| T1: Connectivity graph-based | HIGH | Assert on event types only |
| T2: Convergence assertions | HIGH | Manual inspection of logs |
| T3: Direct state query | MEDIUM | Infer from events |
| T4: Operation completion | MEDIUM | Assume success if no crash |
| T5: Fault effect measurement | MEDIUM | Empirical observation |

---

## VirtualTime Integration Plan

Integrating VirtualTime with the fault injection bridge requires bridging the gap between:
- **VirtualTime**: Synchronous, advances only when explicitly stepped via `advance_to()`
- **Tokio runtime**: Async, uses real wall-clock time

### Option A: Queue-Based Delayed Delivery (Recommended)

Instead of spawning async tasks with `tokio::time::sleep`, queue messages with virtual deadlines:

```rust
pub struct FaultInjectorState {
    pub config: FaultConfig,
    pub rng: SimulationRng,
    pub virtual_time: Option<VirtualTime>,
    pub pending_deliveries: Vec<PendingDelivery>,
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
