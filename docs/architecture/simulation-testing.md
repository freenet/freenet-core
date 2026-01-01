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

### Gap 2: SimulatedNetwork Not Connected to Nodes

`SimulatedNetwork` provides message routing but doesn't connect to actual nodes:

```
Current:
  Node A → InMemoryTransport → channel → InMemoryTransport → Node B

Future:
  Node A → SimulatedNetwork → Scheduler → SimulatedNetwork → Node B
```

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
4. **Structured EventSummary** - Proper typed fields instead of parsing
5. **Single-threaded mode** - Option for `flavor = "current_thread"`
