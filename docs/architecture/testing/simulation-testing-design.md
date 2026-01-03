# Simulation Testing Design

This document describes the current state of simulation testing in Freenet Core,
identifies gaps, and outlines the ideal design for deterministic distributed systems testing.

## Quick Start: Using the Test Infrastructure

### Running SimNetwork Tests

```bash
# Run all simulation tests
cargo test -p freenet --test simulation_integration

# Run with logging
RUST_LOG=info cargo test -p freenet --test simulation_integration -- --nocapture

# Run fdev single-process simulation
cargo run -p fdev -- test --gateways 1 --nodes 5 --events 100 single-process
```

### Key Test Patterns

```rust
use freenet::dev_tool::{SimNetwork, FaultConfig, TimeSource};
use std::time::Duration;

#[tokio::test]
async fn example_simulation_test() {
    const SEED: u64 = 0xDEAD_BEEF;

    // Create network - VirtualTime is always enabled
    let mut sim = SimNetwork::new(
        "test-name",
        1,   // gateways
        4,   // nodes
        7,   // ring_max_htl
        3,   // rnd_if_htl_above
        10,  // max_connections
        2,   // min_connections
        SEED,
    ).await;

    // VirtualTime is available immediately
    assert_eq!(sim.virtual_time().now_nanos(), 0);

    // Optional: Configure fault injection (VirtualTime automatically used)
    sim.with_fault_injection(
        FaultConfig::builder()
            .message_loss_rate(0.1)
            .latency_range(Duration::from_millis(10)..Duration::from_millis(50))
            .build()
    );

    // Start the network
    let _handles = sim.start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 5, 10).await;

    // Wait for connectivity
    sim.check_partial_connectivity(Duration::from_secs(30), 0.8)?;

    // Generate events (event_chain now borrows, doesn't consume)
    let mut stream = sim.event_chain(100, None);
    while stream.next().await.is_some() {
        // Advance virtual time and deliver pending messages
        sim.advance_time(Duration::from_millis(100));
    }
    drop(stream);  // Release borrow

    // Verification - sim is still usable!
    let result = sim.check_convergence().await;
    assert!(result.is_converged(), "Contracts should converge");

    let summary = sim.get_operation_summary().await;
    assert!(summary.overall_success_rate() > 0.8, "Operations should mostly succeed");

    // Example: Crash a node and restart it with preserved state
    let all_addrs = sim.all_node_addresses();
    if let Some(label) = all_addrs.keys().next().cloned() {
        // Crash the node (aborts task, blocks messages)
        sim.crash_node(&label);
        assert!(sim.is_node_crashed(&label));

        // Option 1: Simple recovery (unblocks messages, but task stays aborted)
        // sim.recover_node(&label);

        // Option 2: Full restart with persisted state
        if sim.can_restart(&label) {
            let handle = sim.restart_node::<rand::rngs::SmallRng>(&label, 0x5678, 5, 10).await;
            assert!(handle.is_some(), "Restart should succeed");
            assert!(!sim.is_node_crashed(&label), "Restarted node is not crashed");
        }
    }
}
```

### Available Verification APIs

```rust
// Virtual Time (always available)
sim.virtual_time()                          // Get VirtualTime reference
sim.virtual_time().now_nanos()              // Current virtual time
sim.virtual_time().advance(duration)        // Advance time
sim.advance_time(duration)                  // Advance + deliver pending messages
sim.advance_virtual_time()                  // Just deliver pending messages

// Node Lifecycle
sim.crash_node(&label)                      // Abort task + block messages
sim.recover_node(&label)                    // Unblock messages (task stays aborted)
sim.restart_node::<SmallRng>(&label, seed, max_contracts, iters).await  // Full restart with persisted state
sim.is_node_crashed(&label)                 // Check crash status
sim.can_restart(&label)                     // Check if config saved for restart
sim.node_address(&label)                    // Get node's SocketAddr
sim.all_node_addresses()                    // All label -> addr mappings

// Convergence checking
sim.check_convergence().await               // Immediate check
sim.await_convergence(timeout, poll, min)   // Wait for convergence
sim.convergence_rate().await                // Get rate (0.0-1.0)
sim.assert_convergence(timeout, poll)       // Panic if not converged

// Operation tracking
sim.get_operation_summary().await           // Detailed stats
sim.operation_success_rate().await          // Overall rate
sim.operation_completion_status().await     // (completed, pending)
sim.await_operation_completion(timeout, poll)
sim.assert_operation_success_rate(min_rate)

// State inspection
sim.get_contract_state_hashes().await       // contract -> peer -> hash
sim.get_contract_distribution().await       // Where contracts are stored
sim.get_event_counts().await                // Event counts by type
sim.get_deterministic_event_summary().await // Sorted event list

// Network stats (always available with VirtualTime)
sim.get_network_stats()                     // Messages sent/dropped/delayed
sim.reset_network_stats()                   // Reset counters
```

---

## Current State

### What SimNetwork Tests

The `SimNetwork` infrastructure (`crates/core/src/node/testing_impl.rs`) provides:

1. **In-Memory Transport**: Nodes communicate via channels instead of real UDP/TCP
2. **Fault Injection**: Message loss, latency injection, network partitions
3. **Event Capture**: Records all network events for analysis
4. **Deterministic Seeding**: Same seed produces reproducible random behavior
5. **Post-Event Verification**: `event_chain()` now borrows, allowing verification after events complete

### Test Files

| Test File | What It Tests | Status |
|-----------|---------------|--------|
| `simulation_integration.rs` | Deterministic replay, fault injection, event capture, convergence | Active |
| `sim_network.rs` | CI-focused tests with assertions | Active (`#[ignore]` for now) |

### Recent Improvements

1. **`event_chain(&mut self)` refactor**: No longer consumes SimNetwork, enabling post-test verification
2. **Verification APIs**: `check_convergence()`, `get_operation_summary()`, etc.
3. **Fault injection bridge**: `FaultConfig` from simulation module works with `SimNetwork`
4. **fdev verification**: `--check-convergence`, `--min-success-rate`, `--print-summary` flags
5. **VirtualTime always enabled**: `virtual_time()` accessor for time control, `advance_time()` convenience method
6. **Node crash API**: `crash_node()`, `recover_node()`, `is_node_crashed()` for testing failure scenarios

---

## What's Working vs. What Needs Work

### ✅ Working Well

| Feature | Description |
|---------|-------------|
| In-memory transport | Channel-based message passing |
| Seeded RNG | Reproducible random behavior (within limitations) |
| Fault injection | Message loss, latency, partitions |
| Event capture | Full event logging with timestamps |
| Convergence checking | Verify contract state consistency |
| Operation tracking | Success/failure rates per operation type |
| Post-event verification | Access SimNetwork after events complete |
| **VirtualTime (always on)** | Time control via `virtual_time()` and `advance_time()` |
| **Node crash simulation** | `crash_node()` aborts task and blocks messages |
| **Node restart** | `restart_node()` preserves identity (keypair, address) |

### ⚠️ Partially Working

| Feature | Issue | Workaround |
|---------|-------|------------|
| Deterministic execution | Multi-threaded Tokio causes non-determinism | Use lenient assertions (percentages) |
| Gateway registration race | Nodes may start before gateways ready | 3-phase startup with barrier |

### ❌ Not Yet Implemented

| Feature | Impact |
|---------|--------|
| Deterministic executor | Full reproducibility impossible |
| Clock skew simulation | Can't test time-sensitive bugs |
| Invariant checking DSL | Manual assertions only |
| Linearizability checker | Can't prove consistency |
| Property-based test integration | No automatic shrinking |

---

## The Core Problem: Non-Deterministic Execution

Current tests run on multi-threaded Tokio, meaning:

- **Thread scheduling varies**: Same seed can produce different event orderings
- **Race conditions**: Bugs may not reproduce consistently
- **No time control**: Can't fast-forward or step through execution
- **Probabilistic assertions**: "90% success" instead of "exactly this state"

### Why This Matters

A truly deterministic simulator would allow:

```rust
// Dream API:
let mut sim = DeterministicSimulator::new(SEED);
sim.run_until_quiescent();
assert_eq!(sim.contract_state("key"), expected_state);  // Exact match, every time
```

Instead we have:

```rust
// Current reality:
tokio::time::sleep(Duration::from_secs(5)).await;  // Hope this is enough
let rate = sim.convergence_rate().await;
assert!(rate > 0.8, "Should mostly converge");  // Probabilistic
```

---

## Ideal Event-Driven Simulation Architecture

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
│  - Events processed in (time, sequence) order               │
│  - No real I/O or timers                                    │
└─────────────────────────────────────────────────────────────┘
```

### Key Insight: Events, Not Threads

Instead of spawning tasks that run concurrently:

```rust
// Current (non-deterministic):
tokio::spawn(node1.run());
tokio::spawn(node2.run());
// Order depends on thread scheduling
```

Use an event queue with deterministic ordering:

```rust
// Ideal (deterministic):
loop {
    let event = event_queue.pop();  // Ordered by (time, sequence_number)
    match event {
        MessageDelivery { to, msg } => to.receive(msg),
        TimerFired { node, callback } => node.handle_timer(callback),
        NodeCrash { node } => node.crash(),
    }
}
```

---

## Implementation Roadmap

### Phase 1: Better Use of Existing APIs (Current)

- ✅ Refactor `event_chain` to not consume SimNetwork
- ✅ Use `await_convergence()` instead of arbitrary sleeps
- ✅ Add verification to fdev single-process mode
- 🔲 Use VirtualTime in tests (infrastructure exists)
- 🔲 Single-threaded test runtime for more determinism

### Phase 2: Enhanced Fault Injection

- 🔲 Node crash simulation (stop processing, drop state)
- 🔲 Node restart simulation (resume with persisted state)
- 🔲 Slow node simulation (delayed event processing)
- 🔲 Clock skew simulation

### Phase 3: Deterministic Executor (Major Effort)

- 🔲 Replace `tokio::spawn` with event queue insertions
- 🔲 Replace all timers with virtual time
- 🔲 Single-threaded execution loop
- 🔲 Verify: same seed → identical event sequence

### Phase 4: Invariant Framework

- 🔲 State snapshot API (query contract states across all nodes)
- 🔲 Invariant assertion DSL
- 🔲 Linearizability checker (based on Jepsen's Knossos)
- 🔲 Property-based test integration

---

## Test Scenario Examples

### Convergence After Partition

```rust
#[test]
async fn test_convergence_after_partition() {
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
    sim.assert_all_states_equal(contract.key());
}
```

### Node Crash Recovery

```rust
#[test]
async fn test_node_crash_recovery() {
    let mut sim = Simulation::new(seed);
    sim.add_nodes(5);
    sim.start();

    // Put contract, ensure it's replicated
    let contract = sim.node(0).put(data).await;
    sim.advance_until_quiescent();

    // Crash nodes 0 and 1
    sim.crash_node(0);
    sim.crash_node(1);

    // Remaining nodes should still serve the contract
    assert!(sim.node(2).get(contract.key()).await.is_ok());

    // Restart node 0, it should recover from peers
    sim.restart_node(0);
    sim.advance_until_quiescent();
    assert_eq!(sim.node(0).get(contract.key()).await, data);
}
```

---

## References

- [FoundationDB Testing](https://apple.github.io/foundationdb/testing.html) - Gold standard for deterministic simulation
- [Turmoil](https://github.com/tokio-rs/turmoil) - Tokio's deterministic network simulation
- [Jepsen](https://jepsen.io/) - Distributed systems testing methodology
- [TLA+](https://lamport.azurewebsites.net/tla/tla.html) - Formal specification and model checking

---

## Summary: Current Gaps

| Gap | Impact | Effort | Status |
|-----|--------|--------|--------|
| Non-deterministic execution | Can't reliably reproduce all bugs | High | Open |
| ~~No node crash/restart~~ | ~~Can't test recovery~~ | ~~Medium~~ | ✅ Done |
| ~~VirtualTime not used in tests~~ | ~~Missing time-control benefits~~ | ~~Low~~ | ✅ Done |
| ~~In-memory state persistence~~ | ~~Node restart uses disk~~ | ~~Medium~~ | ✅ Done |
| No linearizability checking | Can't prove consistency | High | Open |
| No property-based testing | Manual test case design | Medium | Open |

The infrastructure is solid for what it tests. The main limitation is non-deterministic
execution, which requires either accepting probabilistic assertions or implementing a
full deterministic executor (significant effort, following FoundationDB's approach).

**Recently Completed:**
- VirtualTime always enabled (`virtual_time()`, `advance_time()`)
- Node crash simulation (`crash_node()`)
- Node restart with preserved identity and state (`restart_node()`)
- In-memory state persistence via `MockStateStorage` (Arc-backed, survives restarts)
