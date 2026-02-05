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

**See working examples in the codebase:**
- `crates/core/tests/simulation_integration.rs` - Complete test suite with strict determinism validation
- `crates/core/tests/simulation_smoke.rs:29-42` - `let_network_run()` time advancement helper
- `crates/core/tests/sim_network.rs` - Fast CI validation tests

**Example pattern:**

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
| `simulation_integration.rs` | Deterministic replay, fault injection, convergence, long-duration tests |
| `simulation_smoke.rs` | Quick smoke tests using plain tokio (non-deterministic) |
| `simulation_determinism.rs` | Unit tests for simulation primitives |
| `sim_network.rs` | CI-focused tests with strict assertions |

## Simulated Time Durations

The simulation framework supports testing across different time scales:

| Test Category | Virtual Time | Wall Clock | Use Case |
|--------------|--------------|------------|----------|
| Quick CI tests | 10-60 seconds | 1-5 seconds | Basic functionality, fast iteration |
| Medium tests | 2-5 minutes | 10-30 seconds | Convergence, fault tolerance |
| Long-duration | **1 hour** | **~3 minutes** | Time-dependent bugs, connection lifecycle |
| Extended nightly | 8+ hours | 1+ hours | Stress testing, resource exhaustion |

### Time Acceleration

Virtual time runs significantly faster than wall clock time due to Turmoil's scheduler:
- **~20x acceleration** for long-duration idle tests (1 hour in ~3 minutes)
- **~100-1000x acceleration** for event-driven tests with minimal idle time

### Long-Duration Test Configuration

For tests simulating extended periods, use the `long_running` configuration:

```rust
TestConfig::long_running("my-long-test", SEED)
    .run()
    .assert_ok()
    .verify_operation_coverage()
    .check_convergence();
```

This configuration:
- 2 gateways, 6 nodes
- 1 hour (3600 seconds) virtual time with events spaced 10s apart
- 360 contract operations distributed across the duration
- Tests for: connection timeout handling, state drift, timer edge cases

Note: Turmoil steps through every virtual millisecond polling all hosts, so virtual
time has a real cost. 3600s virtual ≈ 2.5 min wall clock with 8 hosts.

## Nightly Test Suite

The nightly workflow (`.github/workflows/simulation-nightly.yml`) runs these simulation scenarios.
All fdev tests include realistic network conditions and use 200ms default event spacing.

| Test | Nodes | Events | Virtual Time | Fault Injection |
|------|-------|--------|--------------|-----------------|
| Medium scale (×2 seeds) | 50 | 2000 | ~400s | 10-50ms jitter |
| Fault tolerance | 30 | 1000 | ~200s | 15% loss + 10-50ms jitter |
| High latency | 14 | 500 | ~100s | 50-200ms latency |
| Long-running (Rust test) | 8 | 360 | ~3600s | 10-50ms jitter |
| Large scale | 500 | 10000 | ~2000s | 10-50ms jitter |

All tests require convergence (eventual consistency).

### Long-Running Test Details

The `test_long_running_deterministic` test specifically targets time-dependent bugs:

```bash
# Run manually (requires nightly_tests feature)
cargo test -p freenet --features "simulation_tests,testing,nightly_tests" --test simulation_integration \
  test_long_running_deterministic -- --nocapture --test-threads=1
```

**What it tests:**
- Connection timeout handling over extended virtual time
- Keep-alive and heartbeat mechanisms
- Long-lived contract state consistency
- Timer edge cases (wraparound, scheduling)
- Network partition recovery over time

## Future Work

### Extended Duration Testing

Future improvements for long-duration testing:

| Enhancement | Description | Priority |
|-------------|-------------|----------|
| **24-hour simulation** | Simulate full day of operation | High |
| **Multi-day via checkpoints** | Save/restore simulation state for extended runs | Medium |
| **Adaptive time stepping** | Faster advancement during idle periods | Medium |
| **Memory profiling** | Track resource usage over long simulations | Medium |

### Test Coverage Notes

The following fault injection tests run in **regular CI** (not nightly) via `simulation_smoke.rs`:

| Test | What it tests |
|------|---------------|
| `test_partition_injection_bridge` | Network partitions, verify messages blocked |
| `test_node_crash_recovery` | Crash node, verify network handles it |
| `test_node_restart` | Restart crashed node, verify address preserved |
| `test_fault_injection_bridge` | Message loss injection (50% loss) |
| `test_latency_injection` | Latency injection (100-200ms) |

These run with every PR and don't need nightly scheduling.

### Missing Test Scenarios (not yet implemented)

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
