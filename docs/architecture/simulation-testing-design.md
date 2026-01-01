# Simulation Testing Design

This document describes the current state of simulation testing in Freenet Core,
identifies gaps, and outlines the ideal design for deterministic distributed systems testing.

## Current State

### What SimNetwork Actually Tests

The `SimNetwork` infrastructure (`crates/core/src/node/testing_impl.rs`) provides:

1. **In-Memory Transport**: Nodes communicate via channels instead of real UDP/TCP
2. **Fault Injection**: Message loss, latency injection, network partitions
3. **Event Capture**: Records all network events for analysis
4. **Deterministic Seeding**: Same seed produces reproducible random behavior

### Current Test Coverage

| Test File | What It Tests | Assertions |
|-----------|---------------|------------|
| `simulation_integration.rs` | Deterministic replay, fault injection, event capture | Strict |
| `sim_network.rs` | Basic connectivity, peer registration | Lenient (logs warnings) |

### What We're NOT Testing

1. **Node Lifecycle**: Crashes, restarts, graceful shutdown
2. **Node Churn**: Nodes joining/leaving during operations
3. **State Convergence Under Faults**: Do all nodes agree after network heals?
4. **Invariant Checking**: Are protocol invariants maintained at all times?
5. **Linearizability**: Do operations appear atomic and ordered?

---

## The Problem: Non-Deterministic Execution

Current tests run on a real multi-threaded Tokio runtime, which means:

- **Thread scheduling is non-deterministic**: Same test can produce different interleavings
- **Timing-dependent bugs are hard to reproduce**: Race conditions may not trigger consistently
- **No control over "virtual time"**: Can't fast-forward or pause execution
- **Assertions must be probabilistic**: "90% success rate" instead of "exactly this state"

### Example: The InMemoryTransport Drop Bug

We discovered that `InMemoryTransport` gets dropped after some connections complete,
causing subsequent connection attempts to fail. This is a race condition that:
- Sometimes manifests, sometimes doesn't
- Depends on thread scheduling
- Cannot be reliably reproduced with a seed alone

---

## Ideal Simulation Framework Design

### Goal: Deterministic Execution with Full Control

Given the same seed, the simulation should produce **exactly the same sequence of events
and final state**, every time, on any machine.

### Architecture: Event-Driven Simulation

```
┌─────────────────────────────────────────────────────────────┐
│                    Simulation Controller                     │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │ Virtual     │  │ Event       │  │ Fault               │ │
│  │ Clock       │  │ Queue       │  │ Injector            │ │
│  └─────────────┘  └─────────────┘  └─────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Deterministic Executor                    │
│  - Single-threaded execution                                │
│  - Events processed in deterministic order                  │
│  - No real I/O or timers                                    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                         Node Pool                           │
│  ┌────────┐  ┌────────┐  ┌────────┐  ┌────────┐           │
│  │ Node 0 │  │ Node 1 │  │ Node 2 │  │ Node 3 │  ...      │
│  └────────┘  └────────┘  └────────┘  └────────┘           │
└─────────────────────────────────────────────────────────────┘
```

### Key Components

#### 1. Virtual Clock
- No `tokio::time::sleep` or real timers
- All time references go through `VirtualTime`
- Simulation can advance time instantly or in controlled steps

```rust
impl VirtualTime {
    fn now(&self) -> Instant { self.current_time }
    fn advance(&mut self, duration: Duration) { ... }
    fn advance_to_next_event(&mut self) { ... }
}
```

#### 2. Deterministic Event Queue
- All async operations become events in a priority queue
- Events ordered by (virtual_time, sequence_number) for determinism
- Ties broken by insertion order (sequence number)

```rust
struct Event {
    time: VirtualTime,
    sequence: u64,  // For deterministic tie-breaking
    kind: EventKind,
}

enum EventKind {
    MessageDelivery { from: NodeId, to: NodeId, msg: NetMessage },
    TimerFired { node: NodeId, timer_id: u64 },
    NodeCrash { node: NodeId },
    NodeRestart { node: NodeId },
    UserOperation { op: Operation },
}
```

#### 3. Fault Injection (Enhanced)

Current:
- Message loss (random drop)
- Latency (random delay)
- Partitions (block between peers)

Needed:
- **Node crashes**: Immediate termination, state loss
- **Node restarts**: Resume with persisted state only
- **Slow nodes**: One node processes events 10x slower
- **Byzantine behavior**: Node sends conflicting messages
- **Clock skew**: Nodes have different virtual times

#### 4. State Snapshots & Invariant Checking

At any point, we should be able to:

```rust
// Take a snapshot of all node states
let snapshot = sim.snapshot_all_states();

// Check invariants
sim.assert_invariant(|states| {
    // All nodes that have contract X should have the same state
    let contract_states: Vec<_> = states
        .iter()
        .filter_map(|s| s.get_contract(contract_key))
        .collect();

    contract_states.windows(2).all(|w| w[0] == w[1])
});

// Check convergence
sim.assert_eventually(Duration::from_secs(10), |states| {
    // After healing partition, states should converge
    states_are_consistent(states)
});
```

---

## Test Scenarios We Should Support

### 1. Basic Protocol Correctness

```rust
#[test]
fn test_put_get_single_contract() {
    let mut sim = Simulation::new(seed);
    sim.add_gateway();
    sim.add_nodes(5);
    sim.start();

    // Put a contract from node 0
    let contract = sim.node(0).put(contract_data).await;

    // Advance time until operation completes
    sim.advance_until_quiescent();

    // All nodes should be able to get it
    for node in sim.nodes() {
        let state = node.get(contract.key()).await;
        assert_eq!(state, contract_data);
    }
}
```

### 2. Convergence After Network Partition

```rust
#[test]
fn test_convergence_after_partition() {
    let mut sim = Simulation::new(seed);
    sim.add_nodes(6);
    sim.start();

    // Create initial state
    let contract = sim.node(0).put(initial_state).await;
    sim.advance_until_quiescent();

    // Partition: nodes 0-2 vs nodes 3-5
    sim.partition(&[0, 1, 2], &[3, 4, 5]);

    // Both partitions update the contract
    sim.node(0).update(contract.key(), update_a).await;
    sim.node(3).update(contract.key(), update_b).await;
    sim.advance_until_quiescent();

    // Heal partition
    sim.heal_partition();
    sim.advance_until_quiescent();

    // Assert: all nodes have the same state (conflict resolved)
    let states: Vec<_> = sim.nodes()
        .map(|n| n.get(contract.key()))
        .collect();

    assert!(states.windows(2).all(|w| w[0] == w[1]));
}
```

### 3. Node Crash Recovery

```rust
#[test]
fn test_node_crash_recovery() {
    let mut sim = Simulation::new(seed);
    sim.add_nodes(5);
    sim.start();

    // Put contract, ensure it's replicated
    let contract = sim.node(0).put(data).await;
    sim.advance_until_quiescent();

    // Crash nodes 0 and 1 (the ones that might have it)
    sim.crash_node(0);
    sim.crash_node(1);

    // Remaining nodes should still serve the contract
    let state = sim.node(2).get(contract.key()).await;
    assert!(state.is_ok());

    // Restart node 0
    sim.restart_node(0);
    sim.advance_until_quiescent();

    // Node 0 should recover the contract from peers
    let state = sim.node(0).get(contract.key()).await;
    assert_eq!(state, data);
}
```

### 4. Concurrent Updates (Conflict Resolution)

```rust
#[test]
fn test_concurrent_updates_converge() {
    let mut sim = Simulation::new(seed);
    sim.add_nodes(5);
    sim.start();

    let contract = sim.node(0).put(initial).await;
    sim.advance_until_quiescent();

    // Simultaneously update from multiple nodes
    sim.node(0).update(contract.key(), update_0);
    sim.node(1).update(contract.key(), update_1);
    sim.node(2).update(contract.key(), update_2);

    // Don't advance yet - let updates race
    sim.advance_until_quiescent();

    // All nodes should have converged to the same state
    // (whatever the conflict resolution produces)
    sim.assert_all_states_equal(contract.key());
}
```

### 5. Message Loss Resilience

```rust
#[test]
fn test_operation_succeeds_under_message_loss() {
    let mut sim = Simulation::new(seed);
    sim.add_nodes(10);
    sim.start();

    // 20% message loss
    sim.set_message_loss_rate(0.2);

    let contract = sim.node(0).put(data).await;

    // Should eventually succeed despite losses
    sim.advance_until_quiescent_or_timeout(Duration::from_secs(30));

    // At least some nodes should have it
    let replication_count = sim.nodes()
        .filter(|n| n.has_contract(contract.key()))
        .count();

    assert!(replication_count >= 3, "Contract should replicate despite losses");
}
```

### 6. Linearizability Check

```rust
#[test]
fn test_operations_are_linearizable() {
    let mut sim = Simulation::new(seed);
    sim.add_nodes(5);
    sim.start();

    // Enable history recording
    let history = sim.record_history();

    // Perform concurrent operations
    for i in 0..100 {
        let node = sim.random_node();
        node.update(contract.key(), format!("update-{}", i));
    }

    sim.advance_until_quiescent();

    // Check that the history is linearizable
    // (there exists a sequential ordering consistent with all observations)
    assert!(history.is_linearizable());
}
```

---

## Implementation Path

### Phase 1: Deterministic Executor (High Priority)

Replace multi-threaded Tokio with single-threaded, event-driven execution:

1. Create `DeterministicExecutor` that processes one event at a time
2. Replace all `tokio::time::*` with virtual time
3. Replace all `tokio::spawn` with event queue insertions
4. Verify: same seed → same event sequence

### Phase 2: Enhanced Fault Injection

1. Node crash/restart simulation
2. Clock skew simulation
3. Slow node simulation (delayed event processing)
4. Disk/storage failures

### Phase 3: Invariant Framework

1. State snapshot API
2. Invariant assertion DSL
3. Convergence checking helpers
4. Linearizability checker (based on Jepsen's Knossos)

### Phase 4: Property-Based Testing

1. Integration with `proptest` or `quickcheck`
2. Generate random operation sequences
3. Shrinking to find minimal failing cases

---

## References

- [FoundationDB Testing](https://apple.github.io/foundationdb/testing.html) - Gold standard for deterministic simulation
- [Turmoil](https://github.com/tokio-rs/turmoil) - Tokio's deterministic network simulation
- [Jepsen](https://jepsen.io/) - Distributed systems testing methodology
- [TLA+](https://lamport.azurewebsites.net/tla/tla.html) - Formal specification and model checking
- [Maelstrom](https://github.com/jepsen-io/maelstrom) - Workbench for learning distributed systems

---

## Current Gaps Summary

| Gap | Impact | Effort to Fix |
|-----|--------|---------------|
| Non-deterministic execution | Can't reliably reproduce bugs | High (need deterministic executor) |
| No node crash/restart | Can't test recovery | Medium |
| No invariant checking | Can't verify correctness | Medium |
| No linearizability checking | Can't prove consistency | High |
| Race condition in InMemoryTransport | Tests fail intermittently | Medium (fix drop timing) |
