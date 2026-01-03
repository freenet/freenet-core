# Testing Matrix

This document provides a comprehensive view of all testing strategies in Freenet Core, their coverage, overlaps, and gaps.

## Testing Infrastructure Summary

Freenet Core uses **five distinct testing approaches**, each serving different purposes:

| Approach | Location | Transport | Storage | Determinism | Scale |
|----------|----------|-----------|---------|-------------|-------|
| **Unit Tests** | `src/**/*.rs` | Mocks | Mocks | Full | Single component |
| **`#[freenet_test]` Macro** | `tests/*.rs` | Real UDP (localhost) | SQLite | Non-deterministic | 2-10 nodes |
| **SimNetwork** | `tests/simulation_*.rs` | In-memory channels | MockStateStorage | Seeded RNG | 1-20 nodes |
| **fdev test** | CLI tool | In-memory (single-process) | MockStateStorage | Seeded RNG | 1-50 nodes |
| **freenet-test-network** | `tests/*.rs` | Real UDP (localhost) | SQLite | Non-deterministic | 2-40+ nodes |

---

## Detailed Comparison Matrix

### By Test Type

| What's Tested | Unit | Macro | SimNetwork | fdev | test-network |
|---------------|:----:|:-----:|:----------:|:----:|:------------:|
| **Transport Layer** |
| LEDBAT++ congestion | ✅ 183 | - | - | - | - |
| Streaming buffers | ✅ 73 | - | - | - | - |
| Packet handling | ✅ 47 | - | - | - | - |
| Connection management | ✅ 72 | ✅ | ✅ | ✅ | ✅ |
| **Ring/DHT** |
| Location calculation | ✅ 47 | - | - | - | - |
| Peer proximity | ✅ 68 | - | - | - | - |
| Seeding cache | ✅ 78 | - | - | - | - |
| Ring topology | - | ✅ | ✅ | ✅ | ✅ |
| **Contract Operations** |
| PUT | ✅ mocks | ✅ | ✅ | ✅ | ✅ |
| GET | ✅ mocks | ✅ | ✅ | ✅ | ✅ |
| UPDATE | ✅ mocks | ✅ | ✅ | ✅ | ✅ |
| SUBSCRIBE | ✅ mocks | ✅ | ✅ | ✅ | ✅ |
| Delegation | - | ✅ | - | - | ✅ |
| **State Management** |
| State persistence | ✅ mocks | ✅ | ✅ | ✅ | ✅ |
| State convergence | - | - | ✅ | ✅ | - |
| State size edge cases | - | ✅ 6 | - | - | - |
| **Fault Tolerance** |
| Message loss | - | - | ✅ | ✅ | - |
| Network partitions | - | - | ✅ | ✅ | - |
| Node crashes | - | - | ✅ | - | - |
| Latency injection | - | - | ✅ | ✅ | - |
| **Network Behavior** |
| Peer discovery | - | ✅ | ✅ | ✅ | ✅ |
| Gateway connectivity | - | ✅ | ✅ | ✅ | ✅ |
| Multi-gateway | - | ✅ | ✅ | ✅ | ✅ |
| Connection limits | - | - | - | - | ✅ |
| **Determinism** |
| Seeded replay | - | - | ✅ | ✅ | - |
| VirtualTime | - | - | ✅ | - | - |
| **Scale Testing** |
| 5 nodes | - | ✅ | ✅ | ✅ | ✅ |
| 20 nodes | - | - | ✅ | ✅ | ✅ |
| 40+ nodes | - | - | - | ✅ | ✅ |
| **Application Integration** |
| River app | - | - | - | - | ✅ |
| WebSocket API | - | ✅ | - | - | ✅ |

### By Quality Attribute

| Attribute | Best Approach | Why |
|-----------|---------------|-----|
| **Speed** | Unit tests | No I/O, no network setup |
| **Isolation** | Unit tests | Tests single component |
| **Reproducibility** | SimNetwork | Seeded RNG, VirtualTime |
| **Realism** | test-network | Real processes, real UDP |
| **Fault injection** | SimNetwork/fdev | Built-in FaultConfig |
| **Scale** | test-network | Separate processes, real topology |
| **CI friendliness** | Unit + Macro + SimNetwork | Fast, deterministic |
| **Debugging** | SimNetwork | Event capture, replay |

### By Codebase Area

This matrix shows which testing approaches cover each major codebase area:

| Codebase Area | Path | Unit | Macro | SimNetwork | test-network |
|---------------|------|:----:|:-----:|:----------:|:------------:|
| **Transport Layer** |
| LEDBAT++ congestion | `transport/ledbat.rs` | ✅ 183 | - | - | - |
| Streaming/buffers | `transport/peer_connection/` | ✅ 120 | - | - | - |
| Connection piping | `transport/connection_handler.rs` | ✅ 15 | - | - | - |
| Bandwidth management | `transport/bw.rs` | ✅ 8 | - | - | - |
| **Ring/DHT Layer** |
| Location math | `ring/location.rs` | ✅ 47 | - | - | - |
| Peer key location | `ring/peer_key_location.rs` | ✅ 68 | - | - | - |
| Connection manager | `ring/connection_manager.rs` | ✅ 72 | ✅ | ✅ | ✅ |
| Seeding cache | `ring/seeding.rs` | ✅ 78 | - | - | - |
| **Operations** |
| Connect operation | `operations/connect.rs` | ✅ 12 | ✅ | ✅ | ✅ |
| Put operation | `operations/put.rs` | ✅ 28 | ✅ | ✅ | ✅ |
| Get operation | `operations/get.rs` | ✅ 24 | ✅ | ✅ | ✅ |
| Subscribe operation | `operations/subscribe.rs` | ✅ 18 | ✅ | ✅ | ✅ |
| Update operation | `operations/update.rs` | ✅ 14 | ✅ | ✅ | ✅ |
| **Contract Execution** |
| Executor pool | `contract/executor/` | ✅ 57 | - | - | - |
| WASM runtime | `wasm_runtime/` | ✅ 43 | - | - | - |
| State storage | `wasm_runtime/state_store.rs` | ✅ 12 | ✅ | ✅ | - |
| Mock storage | `wasm_runtime/mock_state_storage.rs` | ✅ 8 | - | ✅ | - |
| **Client/Server** |
| WebSocket API | `server/path_handlers/` | ✅ 1 | ✅ | - | ✅ |
| Client events | `client_events/` | ✅ 12 | ✅ | - | ✅ |
| **Network Bridge** |
| P2P protocol | `network_bridge/p2p_protoc.rs` | ✅ 2 | - | - | - |
| Handshake | `network_bridge/handshake.rs` | ✅ 4 | ✅ | - | ✅ |
| In-memory transport | `transport/in_memory_socket.rs` | - | - | ✅ | - |
| **Node/Config** |
| Node startup | `local_node.rs` | - | ✅ | ✅ | ✅ |
| Config parsing | `config.rs` | ✅ 3 | - | - | - |
| **Simulation** |
| VirtualTime | `simulation/time.rs` | ✅ 5 | - | ✅ | - |
| Fault injection | `simulation/fault.rs` | ✅ 3 | - | ✅ | - |
| SimulationRng | `simulation/rng.rs` | ✅ 4 | - | ✅ | - |
| **Tracing/Telemetry** |
| Event aggregator | `tracing/event_aggregator.rs` | ✅ 3 | - | - | - |
| Test logger | `test_utils.rs` | ✅ 2 | ✅ | ✅ | ✅ |
| **CLI/Binary** |
| Service commands | `bin/service.rs` | ✅ 4 | - | - | - |
| Auto-update | `bin/auto_update.rs` | ✅ 4 | - | - | - |
| fdev testing | `fdev/src/testing.rs` | - | - | ✅ | - |

**Legend:**
- ✅ N = Tested with N unit tests
- ✅ = Tested (integration/e2e)
- `-` = Not tested by this approach

### Coverage Heatmap by Layer

```
Layer              Unit Tests    Integration    Simulation    E2E
─────────────────────────────────────────────────────────────────
Transport          ████████████  ░░░░░░░░░░░░  ░░░░░░░░░░░░  ░░░░
Ring/DHT           ████████████  ████░░░░░░░░  ████░░░░░░░░  ████
Operations         ████████░░░░  ████████████  ████████████  ████
Contract Exec      ████████░░░░  ░░░░░░░░░░░░  ░░░░░░░░░░░░  ░░░░
Client/Server      ██░░░░░░░░░░  ████░░░░░░░░  ░░░░░░░░░░░░  ████
Network Bridge     ██░░░░░░░░░░  ████░░░░░░░░  ░░░░░░░░░░░░  ████
Simulation         ████░░░░░░░░  ░░░░░░░░░░░░  ████████████  ░░░░
─────────────────────────────────────────────────────────────────
█ = Good coverage   ░ = Limited/no coverage
```

**Key Observations:**
1. **Transport layer** - Excellent unit test coverage, but no integration testing (relies on in-memory mocks)
2. **Ring/DHT** - Well covered at unit level; integration tests via SimNetwork
3. **Operations** - Best covered area; tested at all levels
4. **Contract execution** - Good unit tests, but no integration tests for actual WASM execution
5. **Client/Server** - Significant gap; needs more WebSocket API testing
6. **Network bridge** - Light coverage; protocol-level bugs may slip through

---

## Test Count by Category

### Unit Tests (~1,000 total)

| Module | Tests | Key Focus |
|--------|------:|-----------|
| transport/ledbat | 183 | Congestion control math |
| ring/seeding | 78 | Peer seeding cache |
| transport/streaming_buffer | 73 | Buffer management |
| ring/connection_manager | 72 | Connection backoff |
| ring/peer_key_location | 68 | DHT key location |
| transport/streaming | 47 | Stream handling |
| ring/location | 47 | Ring location math |
| operations/* | 96 | Contract operations with mocks |
| contract/executor | 57 | Execution pool |
| wasm_runtime | 43 | WASM metering, storage |

### Integration Tests (80+ tests in 22 files)

| Category | Files | Tests | Infrastructure |
|----------|------:|------:|----------------|
| Isolated node operations | 3 | 13 | `#[freenet_test]` macro |
| Multi-node connectivity | 2 | 7 | `#[freenet_test]` macro |
| SimNetwork simulation | 3 | 30 | Manual tokio::test |
| Real network (test-network) | 8 | 15 | freenet-test-network |
| Regression tests | 5 | 10 | Mixed |
| Examples/utilities | 1 | 9 | `#[freenet_test]` macro |

---

## Overlapping Coverage

These areas are tested by multiple approaches (good for confidence):

| Area | Covered By | Notes |
|------|------------|-------|
| **PUT/GET operations** | Unit (mocks), Macro (real), SimNetwork, fdev, test-network | Well covered at all levels |
| **Peer connectivity** | Macro, SimNetwork, fdev, test-network | Multiple approaches validate topology |
| **State replication** | SimNetwork, fdev | Convergence checking |
| **Gateway behavior** | Macro, SimNetwork, test-network | Connection handling |

---

## Complementary Coverage

These approaches cover different aspects of the same feature:

| Feature | Approach A | Approach B | Complementary Benefit |
|---------|------------|------------|----------------------|
| **Transport** | Unit tests (math) | Integration (real sockets) | Correctness + realism |
| **Convergence** | SimNetwork (deterministic) | test-network (real timing) | Reproducible + realistic |
| **Fault tolerance** | SimNetwork (controlled) | Manual testing | Systematic + exploratory |
| **Scale** | SimNetwork (20 nodes, fast) | test-network (40+ nodes, slow) | Quick validation + realistic load |

---

## Coverage Gaps

### High Priority Gaps

| Gap | Impact | Current Coverage | Recommendation |
|-----|--------|------------------|----------------|
| **WebSocket API endpoints** | 1 test | Client-facing API undertested | Add API integration tests |
| **Client event system** | 12 tests | Event routing, delivery | Add more combinator tests |
| **Network bridge layer** | 19 tests | Protocol-level issues may slip through | Expand handshake/protocol tests |
| **Clock skew simulation** | 0 tests | Time-sensitive bugs undetected | Add to SimNetwork |
| **Linearizability checking** | 0 tests | Can't prove consistency properties | Future: Jepsen-style checker |

### Medium Priority Gaps

| Gap | Impact | Current Coverage | Recommendation |
|-----|--------|------------------|----------------|
| **Tracing/telemetry** | 10 tests | Observability regressions | Add aggregation edge cases |
| **WASM delegate/secrets** | 2 tests | Storage edge cases | Add boundary testing |
| **CLI commands** | 8 tests | Binary behavior | Add CLI integration tests |
| **Database migration** | 2 tests (conditional) | Schema changes | Run in CI with feature |

### Structural Gaps

| Gap | Issue | Recommendation |
|-----|-------|----------------|
| **Property-based testing** | Limited to LEDBAT | Expand proptest to operations |
| **Fuzzing** | Infrastructure exists (arbitrary) | Add contract fuzzing targets |
| **Mutation testing** | Not used | Consider cargo-mutants |

---

## Testing Paradigms: Current vs Potential

This section analyzes what testing paradigms we currently use versus industry best practices we could adopt.

### Currently Using

| Paradigm | Implementation | Coverage | Maturity |
|----------|----------------|----------|----------|
| **Unit Testing** | `#[test]`, `#[tokio::test]` | ~1,000 tests | ✅ Mature |
| **Integration Testing** | `#[freenet_test]` macro, manual setup | ~80 tests | ✅ Mature |
| **Mock-based Testing** | Custom mocks (MockNetworkBridge, MockRing, MockStateStorage) | Extensive | ✅ Mature |
| **Property-based Testing** | `proptest` crate | LEDBAT only (183 tests) | ⚠️ Limited |
| **Simulation Testing** | SimNetwork with seeded RNG, VirtualTime, fault injection | Good | ✅ Mature |
| **Fuzz Testing Infrastructure** | `arbitrary` crate for derive | Infrastructure exists | ⚠️ Underused |
| **End-to-end Testing** | freenet-test-network (real processes) | Scale tests | ✅ Mature |

### Not Yet Using (Industry Best Practices)

| Paradigm | What It Is | Value for Freenet | Effort | Priority |
|----------|------------|-------------------|--------|----------|
| **Deterministic Simulation** | FoundationDB-style single-threaded executor | Full reproducibility of any bug | High | Medium |
| **Linearizability Checking** | Jepsen/Knossos-style consistency proofs | Prove correctness properties | High | Low |
| **Model Checking** | TLA+ / formal specification | Verify protocol design before code | Medium | Low |
| **Mutation Testing** | cargo-mutants | Find untested code paths | Low | High |
| **Chaos Engineering** | Controlled production-like failures | Validate resilience | Medium | Medium |
| **Contract Fuzzing** | AFL/libFuzzer on WASM contracts | Find contract edge cases | Low | High |
| **Snapshot Testing** | Insta crate for output regression | Catch unintended API changes | Low | Medium |
| **Benchmark Regression** | criterion + CI regression detection | Performance guarantees | Low | Medium |
| **Record/Replay** | Capture non-determinism, replay later | Debug production issues | Medium | Medium |

### Paradigm Deep Dive

#### Property-Based Testing (Expand)

**Current state:** Only LEDBAT uses proptest (183 tests for congestion control math).

**Opportunity:** Extend to operations, ring logic, and protocol state machines.

```rust
// Example: Operation sequence testing
use proptest::prelude::*;

fn operation_strategy() -> impl Strategy<Value = Operation> {
    prop_oneof![
        any::<ContractKey>().prop_map(Operation::Get),
        (any::<ContractKey>(), any::<Vec<u8>>()).prop_map(|(k, v)| Operation::Put(k, v)),
        (any::<ContractKey>(), any::<Vec<u8>>()).prop_map(|(k, v)| Operation::Update(k, v)),
    ]
}

proptest! {
    #[test]
    fn operations_converge_regardless_of_order(
        ops in prop::collection::vec(operation_strategy(), 1..50),
        seed in any::<u64>()
    ) {
        // Run operations in different orders, verify same final state
        let result_a = run_operations_ordered(&ops, seed);
        let result_b = run_operations_shuffled(&ops, seed);
        prop_assert_eq!(result_a.final_state, result_b.final_state);
    }

    #[test]
    fn ring_location_properties(loc_a in 0.0..1.0f64, loc_b in 0.0..1.0f64) {
        let dist = ring_distance(loc_a, loc_b);
        prop_assert!(dist >= 0.0 && dist <= 0.5);
        prop_assert_eq!(dist, ring_distance(loc_b, loc_a)); // Symmetric
    }
}
```

**Target modules:**
- `ring/location.rs` - Location arithmetic properties
- `ring/connection_manager.rs` - Connection state machine invariants
- `operations/*` - Operation routing and completion

#### Mutation Testing (Add)

**What it does:** Modifies code (mutants) and checks if tests catch the changes.

**Setup:**
```bash
# Install
cargo install cargo-mutants

# Run on specific package
cargo mutants --package freenet -- --lib

# Run on specific module
cargo mutants --package freenet -- --lib -- ring::location
```

**CI integration (nightly):**
```yaml
mutation-testing:
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4
    - run: cargo install cargo-mutants
    - run: cargo mutants --package freenet -- --lib --timeout 300
```

**Expected findings:** Code paths that tests execute but don't actually verify.

#### Contract Fuzzing (Add)

**What it does:** Generates random/malformed inputs to find crashes and edge cases.

**Setup:**
```bash
# Install
cargo install cargo-fuzz

# Initialize (if not exists)
cargo fuzz init
```

**Fuzz target example:**
```rust
// crates/core/fuzz/fuzz_targets/contract_execution.rs
#![no_main]
use libfuzzer_sys::fuzz_target;
use freenet::contract::{Executor, ContractCode};

fuzz_target!(|data: &[u8]| {
    // Fuzz contract code parsing
    let _ = ContractCode::try_from(data);
});

// crates/core/fuzz/fuzz_targets/state_delta.rs
#![no_main]
use libfuzzer_sys::fuzz_target;
use arbitrary::Arbitrary;

#[derive(Arbitrary, Debug)]
struct FuzzInput {
    current_state: Vec<u8>,
    delta: Vec<u8>,
}

fuzz_target!(|input: FuzzInput| {
    // Fuzz state delta application
    let _ = apply_delta(&input.current_state, &input.delta);
});
```

**Run:**
```bash
cargo +nightly fuzz run contract_execution -- -max_len=65536
```

#### Snapshot Testing (Add)

**What it does:** Captures output and fails if it changes unexpectedly.

**Setup:**
```toml
# Cargo.toml
[dev-dependencies]
insta = { version = "1.40", features = ["json", "yaml"] }
```

**Example:**
```rust
use insta::{assert_json_snapshot, assert_debug_snapshot};

#[test]
fn api_error_response_format() {
    let error = ApiError::ContractNotFound { key: "abc123".into() };
    let response = error.to_response();

    // First run: creates snapshot file
    // Subsequent runs: compares against snapshot
    assert_json_snapshot!(response);
}

#[test]
fn event_summary_format() {
    let event = EventSummary {
        tx: Transaction::new(),
        peer_addr: "127.0.0.1:8080".parse().unwrap(),
        event_kind_name: "Put".into(),
        contract_key: Some("abc123".into()),
        state_hash: Some("deadbeef".into()),
        event_detail: "...".into(),
    };

    assert_debug_snapshot!(event);
}
```

**Review changes:**
```bash
cargo insta review  # Interactive review of snapshot changes
```

#### Deterministic Simulation (Future)

**Current limitation:** Multi-threaded tokio causes non-deterministic task ordering.

**Short-term improvement (low effort):**
```rust
// Add to #[freenet_test] macro options
#[freenet_test(
    nodes = ["gateway", "peer-1"],
    tokio_flavor = "current_thread",
    start_paused = true  // NEW: Enable time control
)]
async fn deterministic_test(ctx: &mut TestContext) -> TestResult {
    // Time only advances explicitly
    tokio::time::advance(Duration::from_secs(1)).await;
    Ok(())
}
```

**Long-term improvement (high effort):** FoundationDB-style deterministic executor.

```rust
// Conceptual design
pub struct DeterministicRuntime {
    scheduler: Scheduler,
    time: VirtualTime,
    tasks: BTreeMap<TaskId, Task>,
    ready_queue: VecDeque<TaskId>,  // FIFO for determinism
}

impl DeterministicRuntime {
    /// Process one task step deterministically
    pub fn step(&mut self) -> bool {
        // 1. Process time-based wakeups
        self.process_wakeups();

        // 2. Run one ready task (FIFO order)
        if let Some(task_id) = self.ready_queue.pop_front() {
            self.poll_task(task_id);
            return true;
        }

        // 3. Advance time to next event
        if let Some(next_time) = self.scheduler.next_event_time() {
            self.time.advance_to(next_time);
            return true;
        }

        false // Simulation complete
    }
}
```

**Reference:** [turmoil](https://github.com/tokio-rs/turmoil) - Tokio's deterministic network simulation

#### Record/Replay (Future)

**What it does:** Records all non-deterministic decisions during test, replays them for debugging.

```rust
pub struct RecordingRuntime {
    inner: tokio::Runtime,
    log: Vec<NonDetEvent>,
}

pub enum NonDetEvent {
    TaskScheduled { task_id: u64, order: u64 },
    ChannelRecv { channel_id: u64, value_hash: u64 },
    TimeNow { nanos: u64 },
    RngOutput { value: u64 },
}

// During recording: capture all non-deterministic events
// During replay: return recorded values instead of real ones
```

**Use case:** "This test failed in CI, let me replay the exact execution locally."

#### Linearizability Checking (Future)

**What it does:** Proves that concurrent operations appear to execute atomically.

**Reference:** [Jepsen](https://jepsen.io/) and [Knossos](https://github.com/jepsen-io/knossos)

```rust
// Conceptual API
let history = run_concurrent_operations(&[
    Op::Put("key1", "value1"),
    Op::Get("key1"),
    Op::Put("key1", "value2"),
    Op::Get("key1"),
]);

// Check if there exists a valid sequential ordering
assert!(is_linearizable(&history, &contract_model));
```

### Adoption Roadmap

#### Phase 1: Quick Wins (1-2 weeks)

| Action | Effort | Impact |
|--------|--------|--------|
| Add mutation testing to CI nightly | 2 hours | Find untested paths |
| Create 3 contract fuzz targets | 1 day | Security coverage |
| Add snapshot tests for API responses | 1 day | Regression protection |
| Extend proptest to ring/location | 2 days | Algorithm correctness |

#### Phase 2: Medium-term (1-2 months)

| Action | Effort | Impact |
|--------|--------|--------|
| Add `start_paused = true` to macro | 3 days | Better determinism |
| Proptest for operation sequences | 1 week | Protocol correctness |
| Benchmark regression in CI | 3 days | Performance protection |
| Chaos engineering in test-network | 1 week | Resilience validation |

#### Phase 3: Long-term (3-6 months)

| Action | Effort | Impact |
|--------|--------|--------|
| Deterministic executor prototype | 4 weeks | Full reproducibility |
| Record/replay infrastructure | 2 weeks | Debug production issues |
| Linearizability checker | 4 weeks | Prove consistency |
| TLA+ model of core protocol | 4 weeks | Formal verification |

### Paradigm Selection Guide

| Scenario | Recommended Paradigm |
|----------|---------------------|
| "Does this algorithm handle edge cases?" | Property-based testing |
| "Did I actually test this code path?" | Mutation testing |
| "Can malformed input crash this?" | Fuzzing |
| "Did this API response change?" | Snapshot testing |
| "Can I reproduce this CI failure?" | Deterministic simulation |
| "Is this concurrent code correct?" | Linearizability checking |
| "Is the protocol design sound?" | TLA+ model checking |

---

## Test Infrastructure Features

### `#[freenet_test]` Macro

```rust
#[freenet_test(
    nodes = ["gateway", "peer-1", "peer-2"],
    gateways = ["gateway"],
    timeout_secs = 180,
    tokio_flavor = "current_thread",
    aggregate_events = "on_failure"
)]
async fn test_example(ctx: &mut TestContext) -> TestResult {
    // Automatic node setup, event logging, failure reporting
    Ok(())
}
```

**Capabilities:**
- Automatic node configuration and startup
- Event log aggregation on failure
- WebSocket client management
- Configurable tokio runtime

**Limitations (blocking more conversions):**
- No node lifecycle control (stop/restart mid-test)
- No custom peer-gateway relationships
- No network topology control

### SimNetwork

```rust
let mut sim = SimNetwork::new("test", 1, 5, 7, 3, 10, 2, SEED).await;
sim.with_fault_injection(FaultConfig::builder()
    .message_loss_rate(0.1)
    .latency_range(10ms..50ms)
    .build());
let handles = sim.start_with_rand_gen::<SmallRng>(SEED, 10, 5).await;
```

**Capabilities:**
- Deterministic seeded RNG
- VirtualTime always enabled
- Fault injection (loss, latency, partitions, crashes)
- Node crash/restart simulation
- Convergence checking APIs
- Operation tracking
- Network statistics

**Limitations:**
- Multi-threaded tokio = non-deterministic task ordering
- Event-based state hashes (not full state access)

### freenet-test-network

```rust
let network = TestNetwork::builder()
    .gateways(2)
    .peers(20)
    .require_connectivity(0.75)
    .preserve_temp_dirs_on_failure(true)
    .build()
    .await?;
let ws_url = network.gateway(0).ws_url();
```

**Capabilities:**
- Real separate processes
- WebSocket API access
- Ring topology snapshots
- Diagnostics collection
- Log aggregation
- Docker NAT support

**Limitations:**
- Slow startup (real processes)
- Non-deterministic (real timing)
- Resource intensive

---

## Recommendations

### Short-term Improvements

1. **Add node lifecycle control to `#[freenet_test]`**
   - Enable `ctx.stop_node()`, `ctx.restart_node()`
   - Would unblock 14+ test conversions

2. **Expand WebSocket API tests**
   - Add endpoint coverage for all v1 handlers
   - Test error responses

3. **Add single-threaded mode to macro**
   - `tokio_flavor = "current_thread"` already supported
   - Add `start_paused = true` for time control

### Medium-term Improvements

1. **Property-based testing for operations**
   - Extend proptest beyond LEDBAT
   - Add operation sequence generators

2. **Expand SimNetwork fault scenarios**
   - Add clock skew simulation
   - Add slow node simulation

3. **CI test tiering**
   - Fast: Unit + macro tests
   - Medium: SimNetwork tests
   - Slow (nightly): test-network + soak tests

### Long-term Improvements

1. **Deterministic executor**
   - Replace tokio scheduler for full reproducibility
   - Would enable exact replay of failures

2. **Invariant checking framework**
   - DSL for contract invariants
   - Automatic verification during tests

3. **Linearizability checker**
   - Jepsen-style consistency proofs
   - Would prove correctness properties

---

## File Reference

### Test Files

| File | Tests | Infrastructure |
|------|------:|----------------|
| `isolated_node_regression.rs` | 4 | Macro |
| `error_notification.rs` | 4 | Macro + manual |
| `edge_case_state_sizes.rs` | 6 | Macro |
| `connectivity.rs` | 4 | Macro |
| `operations.rs` | 18+ | Manual |
| `simulation_integration.rs` | 17 | SimNetwork |
| `simulation_determinism.rs` | 7 | SimNetwork |
| `sim_network.rs` | 6 | SimNetwork |
| `test_network_integration.rs` | 3 | test-network |
| `large_network.rs` | 1 | test-network |
| `twenty_peer_validation.rs` | 3 | test-network |
| `gateway_inbound_identity.rs` | 1 | test-network |

### Documentation

| Document | Purpose |
|----------|---------|
| `simulation-testing.md` | SimNetwork architecture |
| `simulation-testing-design.md` | Design philosophy, roadmap |
| `MACRO_TEST_CONVERSION_STATUS.md` | Macro adoption tracking |
| `debugging/testing-logging-guide.md` | Logging in tests |
