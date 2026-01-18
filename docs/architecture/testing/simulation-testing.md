# Simulation Testing

This document describes the deterministic simulation testing framework for Freenet Core.

## Overview

The simulation testing framework enables reproducible testing of the Freenet network by providing:

1. **Deterministic scheduling** - Turmoil provides ~99% deterministic async execution
2. **Seeded randomness** - All random decisions use `GlobalRng`
3. **Controlled time** - `VirtualTime` advances only when explicitly stepped
4. **Fault injection** - Configurable message loss, partitions, and latency
5. **Event capture** - Full visibility into network events for assertions

## Quick Start

### Running Tests

```bash
# Run all simulation tests (Turmoil always enabled)
cargo test -p freenet --features simulation_tests --test sim_network -- --test-threads=1

# Run with logging
RUST_LOG=info cargo test -p freenet --features simulation_tests --test sim_network -- --nocapture --test-threads=1

# Run fdev single-process simulation
cargo run -p fdev -- test --gateways 2 --nodes 10 --events 100 --seed 0xDEADBEEF single-process
```

### Basic Test Pattern

```rust
use freenet::dev_tool::{SimNetwork, FaultConfig};
use std::time::Duration;

#[tokio::test(flavor = "current_thread")]
async fn example_simulation_test() {
    const SEED: u64 = 0xDEAD_BEEF;

    // Create network
    let mut sim = SimNetwork::new(
        "test-name",
        1,    // gateways
        4,    // nodes
        7,    // ring_max_htl
        3,    // rnd_if_htl_above
        10,   // max_connections
        2,    // min_connections
        SEED,
    ).await;

    // Optional: Configure fault injection
    sim.with_fault_injection(
        FaultConfig::builder()
            .message_loss_rate(0.1)
            .latency_range(Duration::from_millis(10)..Duration::from_millis(50))
            .build()
    );

    // Start the network
    let _handles = sim.start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 5, 10).await;

    // Advance time using VirtualTime
    for _ in 0..30 {
        sim.advance_time(Duration::from_millis(100));
        tokio::task::yield_now().await;
    }

    // Wait for connectivity
    sim.check_partial_connectivity(Duration::from_secs(30), 0.8).await
        .expect("Network should connect");

    // Generate events
    let mut stream = sim.event_chain(100, None);
    while { use futures::StreamExt; stream.next().await }.is_some() {
        sim.advance_time(Duration::from_millis(100));
        tokio::task::yield_now().await;
    }
    drop(stream);

    // Verify convergence
    let result = sim.await_convergence(
        Duration::from_secs(30),
        Duration::from_millis(500),
        1,
    ).await;
    assert!(result.is_ok(), "Contracts should converge");
}
```

## Architecture

```
SimNetwork
    ├── InMemoryTransport (per node)
    │   └── Message channels between nodes
    ├── TestEventListener (shared)
    │   └── Captures all network events
    ├── EventChain
    │   └── Generates Put/Get/Subscribe events
    ├── VirtualTime (always enabled)
    │   └── Controlled time progression
    └── FaultInjectorState
        └── Message loss, partitions, latency
```

## Determinism Guarantees

| Aspect | Mechanism |
|--------|-----------|
| Task scheduling | Turmoil - deterministic FIFO ordering |
| RNG | `GlobalRng` - seeded, deterministic |
| Time | `VirtualTime` - explicit advancement |
| Network | `SimulationSocket` - in-memory |
| Peer labels | Derived from master seed |
| Fault injection | Seeded RNG for drop decisions |

**Result:** Same seed produces identical execution (~99% determinism).

## Available APIs

### Time Control

```rust
sim.virtual_time()                    // Get VirtualTime reference
sim.virtual_time().now_nanos()        // Current virtual time
sim.advance_time(duration)            // Advance + deliver pending messages
```

### Node Lifecycle

```rust
sim.crash_node(&label)                // Abort task + block messages
sim.recover_node(&label)              // Unblock messages
sim.restart_node::<SmallRng>(&label, seed, max_contracts, iters).await
sim.is_node_crashed(&label)           // Check crash status
```

### Convergence Checking

```rust
sim.check_convergence().await         // Immediate check
sim.await_convergence(timeout, poll, min_contracts).await
sim.convergence_rate().await          // Get rate (0.0-1.0)
sim.assert_convergence(timeout, poll) // Panic if not converged
```

### Operation Tracking

```rust
sim.get_operation_summary().await     // Detailed stats
sim.operation_success_rate().await    // Overall rate
sim.await_operation_completion(timeout, poll).await
sim.assert_operation_success_rate(min_rate)
```

### Network Statistics

```rust
sim.get_network_stats()               // Messages sent/dropped/delayed
sim.reset_network_stats()             // Reset counters
```

### Quiescence Detection

```rust
sim.await_network_quiescence(
    Duration::from_secs(120),         // Max timeout
    Duration::from_secs(3),           // Quiet period required
    Duration::from_millis(500),       // Poll interval
).await
```

## Fault Injection

```rust
let config = FaultConfig::builder()
    .message_loss_rate(0.15)          // 15% packet loss
    .latency_range(Duration::from_millis(10)..Duration::from_millis(50))
    .build();

sim.with_fault_injection(config);

// After test, check actual fault rates
if let Some(stats) = sim.get_network_stats() {
    println!("Loss rate: {:.1}%", stats.loss_ratio() * 100.0);
    println!("Dropped: {}", stats.total_dropped());
}
```

## Test Files

| File | Purpose |
|------|---------|
| `simulation_integration.rs` | Deterministic replay, fault injection, convergence |
| `simulation_determinism.rs` | Unit tests for simulation primitives |
| `sim_network.rs` | CI-focused tests with strict assertions |

## Nightly Test Suite

The nightly workflow runs these simulation scenarios:

| Test | Nodes | Events | Fault Injection | Success Rate |
|------|-------|--------|-----------------|--------------|
| Medium scale | 50 | 2000 | None | 100% |
| Large scale | 500 | 10000 | None | 100% |
| Fault tolerance | 30 | 1000 | 15% message loss | 80% |
| High latency | 14 | 500 | 50-200ms latency | 95% |

All tests require 100% convergence (eventual consistency).

## Future Work

### Missing Nightly Tests

The following test scenarios exist in `simulation_integration.rs` but are **not yet in the nightly workflow**:

| Test | File | What it does |
|------|------|-------------|
| `test_partition_injection_bridge` | simulation_integration.rs | Partition network, verify messages blocked |
| `test_node_crash_recovery` | simulation_integration.rs | Crash node, verify network handles it |
| `test_node_restart` | simulation_integration.rs | Restart crashed node |
| `test_node_restart_with_state_recovery` | simulation_integration.rs | Restart with MockStateStorage preservation |

### Missing Test Scenarios (not implemented)

| Scenario | Description | Priority |
|----------|-------------|----------|
| **Partition → Heal → Convergence** | Partition network → both sides update → heal → assert all converge to same state | High |
| **Rolling restart with convergence** | Put contracts → crash 2 nodes → verify remaining serve → restart → assert state recovered | High |
| **Multi-step churn** | Continuous crash/restart cycle while operations run, verify eventual consistency | Medium |

### Missing Property Tests

Currently only LEDBAT uses proptest. Expand to:

| Module | Properties to test |
|--------|-------------------|
| `ring/location.rs` | `distance(a,b) == distance(b,a)`, always in [0, 0.5] |
| `operations/*` | Any sequence of put/get/subscribe/update should converge |
| `ring/connection_manager.rs` | State machine invariants (connected → disconnected transitions) |

### Other Enhancements

| Enhancement | Description | Priority |
|-------------|-------------|----------|
| **Linearizability checker** | Jepsen/Knossos-style operation history verification | Medium |
| **Clock skew simulation** | Per-node time offsets | Low |
| **Invariant checking DSL** | Declarative system invariants | Low |

## References

- [FoundationDB Testing](https://apple.github.io/foundationdb/testing.html) - Gold standard for DST
- [Turmoil](https://github.com/tokio-rs/turmoil) - Tokio's deterministic simulation
- [Jepsen](https://jepsen.io/) - Distributed systems testing methodology
