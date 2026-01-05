# Testing Infrastructure Overview

This directory contains documentation for Freenet Core's testing strategies and infrastructure.

## Documents

| Document | Description |
|----------|-------------|
| [testing-matrix.md](testing-matrix.md) | Comprehensive matrix of all testing approaches, coverage gaps, and paradigm analysis |
| [simulation-testing.md](simulation-testing.md) | SimNetwork architecture and deterministic simulation |
| [simulation-testing-design.md](simulation-testing-design.md) | Design philosophy and roadmap for simulation testing |
| [deterministic-simulation-roadmap.md](deterministic-simulation-roadmap.md) | Analysis of what's needed for full determinism |

## Testing Paradigms

We currently use these testing paradigms:

| Paradigm | Status | Coverage |
|----------|--------|----------|
| Unit Testing | ✅ Mature | ~1,000 tests |
| Integration Testing | ✅ Mature | ~80 tests |
| Mock-based Testing | ✅ Mature | Extensive |
| Simulation Testing | ✅ Mature | SimNetwork with VirtualTime + MadSim |
| Deterministic Time/RNG | ✅ Complete | `VirtualTime`, `GlobalRng` |
| **Deterministic Scheduling** | ✅ **Complete** | **MadSim in CI nightly** |
| Property-based Testing | ⚠️ Limited | LEDBAT only |
| Fuzz Testing | ⚠️ Underused | Infrastructure exists |

### Deterministic Simulation Testing (DST) Status

| Component | Status | Notes |
|-----------|--------|-------|
| VirtualTime | ✅ Complete | `sim.advance_time()` for explicit control |
| GlobalRng | ✅ Complete | Seeded RNG replacing `rand::random()` |
| TimeSource injection | ✅ Complete | Transport/LEDBAT use trait |
| Single-threaded tests | ✅ Complete | All simulation tests use `current_thread` |
| **Deterministic scheduler** | ✅ **Complete** | **MadSim active in CI nightly (~99% determinism)** |
| Turmoil integration | ✅ Complete | Alternative via `run_simulation()` |

Full determinism achieved. See [deterministic-simulation-roadmap.md](deterministic-simulation-roadmap.md) for implementation details.

### ~~Paradigms to Adopt~~ Recently Adopted ✅

| Paradigm | Status | Notes |
|----------|--------|-------|
| ~~Deterministic Scheduler (MadSim)~~ | ✅ **ADOPTED** | **Active in CI nightly** |
| Linearizability Checker | 🔮 Future | Now possible with MadSim |
| Expanded Property Testing | 🔮 Future | Determinism enables this |
| Mutation Testing | 🔮 Future | Low priority |

## Quick Reference: Which Testing Approach to Use

| Scenario | Recommended Approach |
|----------|---------------------|
| Testing a single function/algorithm | Unit tests with `#[test]` |
| Testing transport/ring logic | Unit tests with mocks (MockNetworkBridge, MockRing) |
| Testing contract operations in isolation | `#[freenet_test]` macro with single gateway |
| Testing multi-node connectivity | `#[freenet_test]` macro with multiple nodes |
| Testing fault tolerance (message loss, partitions) | SimNetwork with FaultConfig |
| Testing deterministic replay | SimNetwork with fixed seed |
| Testing at scale (20+ peers) | `freenet-test-network` with TestNetwork |
| Testing River app integration | `freenet-test-network` with riverctl |
| CI quick validation | `fdev test single-process` |
| Soak testing | `large_network.rs` with `--ignored` |

## Test Execution Commands

```bash
# Unit tests
cargo test -p freenet

# Integration tests (macro-based)
cargo test -p freenet --test isolated_node_regression

# SimNetwork simulation tests
cargo test -p freenet --test simulation_integration

# SimNetwork with MadSim (deterministic, same as CI nightly)
RUSTFLAGS="--cfg madsim" cargo test -p freenet --test simulation_integration -- --test-threads=1

# Real network tests (requires feature)
cargo test -p freenet --test test_network_integration --features test-network

# Large scale soak test
cargo test -p freenet --test large_network --features test-network -- --ignored

# fdev CLI testing
cargo run -p fdev -- test --gateways 1 --nodes 5 --events 100 single-process

# fdev with MadSim (deterministic, same as CI nightly)
RUSTFLAGS="--cfg madsim" cargo run -p fdev -- test --gateways 1 --nodes 3 --events 10 --seed 42 single-process
```
