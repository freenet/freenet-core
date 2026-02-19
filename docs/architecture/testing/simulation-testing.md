# Simulation Testing

This document describes the deterministic simulation testing framework for Freenet Core.

## Overview

The simulation testing framework enables reproducible testing of the Freenet network by providing:

1. **Deterministic scheduling** - Two runners available:
   - **Direct runner** (`run_simulation_direct`) — single `current_thread` + `start_paused(true)` tokio runtime. 100% deterministic. Scales to 500+ nodes.
   - **Turmoil runner** (`run_simulation`) — Turmoil's deterministic scheduler. ~99% deterministic. Better for mid-simulation fault injection via closures.
2. **Seeded randomness** - All random decisions use `GlobalRng`
3. **Controlled time** - `VirtualTime` advances only when explicitly stepped
4. **Fault injection** - Configurable message loss, partitions, and latency
5. **Event capture** - Full visibility into network events for assertions

## Quick Start

### Running Tests

```bash
# Run all simulation tests
cargo test -p freenet --features "simulation_tests,testing" --test simulation_integration

# Run with logging
RUST_LOG=info cargo test -p freenet --features "simulation_tests,testing" --test simulation_integration -- --nocapture

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

| Aspect | Direct Runner | Turmoil Runner |
|--------|---------------|----------------|
| Task scheduling | tokio `current_thread` + `start_paused(true)` — 100% deterministic | Turmoil FIFO — ~99% deterministic |
| RNG | `GlobalRng` - seeded | `GlobalRng` - seeded |
| Time | `VirtualTime` via time driver task | `VirtualTime` - explicit advancement |
| Network | `SimulationSocket` - in-memory | `SimulationSocket` - in-memory |
| Peer labels | Derived from master seed | Derived from master seed |
| Fault injection | Static `FaultConfig` (no mid-sim changes) | `FaultConfig` + closure-based mid-sim injection |
| Scale | 500+ nodes (single runtime) | ~50 nodes (O(n²) link overhead) |

**Result:** Same seed produces identical execution. Direct runner is fully deterministic; Turmoil is ~99%.

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

### Mid-Simulation Fault Injection (Turmoil Runner Only)

For Turmoil-based tests using `run_simulation()`, faults can be injected from the
test closure via the global fault injector registry. **Note:** The direct runner
(`run_simulation_direct`) does not support mid-simulation fault injection — use
static `FaultConfig` for latency jitter and message loss instead.

```rust
let result = sim.run_simulation::<SmallRng, _, _>(
    SEED, 5, 100,
    Duration::from_secs(120),
    Duration::from_millis(500),
    move || async move {
        // Inject partition mid-simulation
        if let Some(injector) = freenet::dev_tool::get_fault_injector("network-name") {
            let mut state = injector.lock().unwrap();
            let partition = Partition::new(side_a, side_b).permanent(0);
            state.config.add_partition(partition);
        }
        tokio::time::sleep(Duration::from_secs(15)).await;

        // Heal the partition
        if let Some(injector) = freenet::dev_tool::get_fault_injector("network-name") {
            let mut state = injector.lock().unwrap();
            state.config.partitions.clear();
        }
        Ok(())
    },
);
```

Available fault operations inside the closure:
- `state.config.add_partition(partition)` — block messages between node groups
- `state.config.partitions.clear()` — heal all partitions
- `state.config.crash_node(addr)` — stop all messages to/from a node
- `state.config.recover_node(&addr)` — restore message delivery

**Important:** `run_simulation()` consumes `self`, so capture node addresses
and the network name *before* calling it.

## Anomaly Detection

The simulation framework includes a `StateVerifier` that analyzes event logs to
detect consistency anomalies. This is used to validate convergence beyond simple
state hash comparison.

### Anomaly Types Detected

| Anomaly | Description |
|---------|-------------|
| `FinalDivergence` | Nodes disagree on final state for a contract |
| `MissingBroadcast` | A state update was not propagated to all subscribers |
| `BroadcastNotApplied` | A node received but did not apply a broadcast |
| `SuspectedPartition` | Message patterns suggest a network partition |
| `StalePeer` | A node missed multiple consecutive updates |
| `StateOscillation` | A node's state flips back and forth |
| `UpdateOrderingAnomaly` | Updates applied in inconsistent order |
| `ZombieTransaction` | A completed transaction is still referenced |
| `BroadcastStorm` | Excessive broadcast messages detected |
| `DeltaSyncFailureCascade` | Cascading failures in delta sync |

### Using Anomaly Detection in Tests

```rust
// After simulation completes, analyze event logs
let report = rt.block_on(async {
    let logs = logs_handle.lock().await;
    let verifier = freenet::tracing::StateVerifier::from_events(logs.clone());
    verifier.verify()
});

// Inspect results
tracing::info!(
    "{} events, {} state, {} contracts, {} anomalies",
    report.total_events, report.state_events,
    report.contracts_analyzed, report.anomalies.len()
);

// Query specific anomaly categories
let divergences = report.divergences();
let stale = report.stale_peers();
let oscillations = report.state_oscillations();
```

The `TestResult::verify_state_report()` method chains anomaly detection onto
any `TestConfig`-based test (non-asserting, logs findings).

### Typical Findings

From production simulation tests, the most common anomalies are:
- **StateOscillation** — dominant pattern; state flip counts of 2–12
- **StalePeer** — 2–11 missed updates per peer, common during fault injection
- **FinalDivergence = 0** — the network consistently self-heals

## Test Files

| File | Purpose |
|------|---------|
| `simulation_integration.rs` | Deterministic replay, fault injection, convergence, anomaly detection, long-duration tests |
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

Virtual time runs significantly faster than wall clock time:
- **Direct runner:** ~29x acceleration for long-duration tests (1 hour virtual in ~2 minutes wall clock). Scales better with node count since there's only one tokio runtime.
- **Turmoil runner:** ~20x acceleration for long-duration idle tests. Degrades at scale due to O(n²) link iteration per tick.
- **Both:** ~100-1000x acceleration for event-driven tests with minimal idle time.

### Long-Duration Test Configuration

For tests simulating extended periods, use the `long_running` configuration:

```rust
TestConfig::long_running("my-long-test", SEED)
    .run_direct()    // Uses the direct runner (single tokio runtime)
    .assert_ok()
    .verify_operation_coverage()
    .check_convergence();
```

This configuration:
- 2 gateways, 6 nodes
- 1 hour (3600 seconds) virtual time with events spaced 10s apart
- 360 contract operations distributed across the duration
- Tests for: connection timeout handling, state drift, timer edge cases

The direct runner completes 3600s virtual time in ~2 minutes wall clock with 8 hosts.
Use `.run_direct()` for long-duration and large-scale tests; use `.run()` (Turmoil)
when mid-simulation fault injection via closures is needed.

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
  test_long_running_deterministic -- --nocapture
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

### Fault Tolerance Integration Tests (Turmoil Runner)

These scenarios use Turmoil's deterministic scheduler with mid-simulation fault
injection via the global fault injector registry. All include anomaly detection.
They use the Turmoil runner because they require mid-simulation fault injection
(partitions, crashes) from closures.

| Test | Description | Results |
|------|-------------|---------|
| `test_partition_heal_convergence` | Partition network → both sides update → heal → verify convergence | 48 state events, 3 contracts, 4 anomalies (oscillation) |
| `test_crash_recover_convergence` | Crash 2 nodes → remaining operate → recover → verify state | 106 state events, 4 contracts, 9 anomalies (oscillation) |
| `test_multi_step_churn` | 3 rounds of crash→wait→recover→wait, verify eventual consistency | 109 state events, 4 contracts, 10 anomalies (oscillation + stale) |

All three converge with 0 `FinalDivergence` despite injected faults.

**Key parameter note:** The `iterations` parameter to `run_simulation()` serves
dual purpose as both the event signal count and the per-peer `gen_event` budget.
Use at least `iterations >= num_peers * 15` to ensure enough budget for contract
creation (only 5% probability per iteration).

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

## Choosing a Runner

| Criterion | Direct Runner | Turmoil Runner |
|-----------|:---:|:---:|
| Large scale (50+ nodes) | **Preferred** | Slow (O(n²) links) |
| Long duration (1hr+ virtual) | **Preferred** | Viable for small node counts |
| Mid-simulation fault injection | Not supported | **Required** |
| fdev CLI tests | **Default** | Not used |
| Nightly tests | **Default** | Not used |
| Determinism verification | **100%** | ~99% |
| Fault tolerance tests (partition/crash/churn) | Not supported | **Required** |

**Rule of thumb:** Use the direct runner unless you need mid-simulation fault injection from a closure.

## References

- [FoundationDB Testing](https://apple.github.io/foundationdb/testing.html) - Gold standard for DST
- [Turmoil](https://github.com/tokio-rs/turmoil) - Tokio's deterministic simulation
- [Jepsen](https://jepsen.io/) - Distributed systems testing methodology
