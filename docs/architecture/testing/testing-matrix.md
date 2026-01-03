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
