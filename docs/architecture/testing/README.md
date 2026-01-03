# Testing Infrastructure Overview

This directory contains documentation for Freenet Core's testing strategies and infrastructure.

## Documents

| Document | Description |
|----------|-------------|
| [testing-matrix.md](testing-matrix.md) | Comprehensive matrix of all testing approaches, coverage gaps, and paradigm analysis |
| [simulation-testing.md](simulation-testing.md) | SimNetwork architecture and deterministic simulation |
| [simulation-testing-design.md](simulation-testing-design.md) | Design philosophy and roadmap for simulation testing |

## Testing Paradigms

We currently use these testing paradigms:

| Paradigm | Status | Coverage |
|----------|--------|----------|
| Unit Testing | ✅ Mature | ~1,000 tests |
| Integration Testing | ✅ Mature | ~80 tests |
| Mock-based Testing | ✅ Mature | Extensive |
| Simulation Testing | ✅ Mature | SimNetwork |
| Property-based Testing | ⚠️ Limited | LEDBAT only |
| Fuzz Testing | ⚠️ Underused | Infrastructure exists |

Paradigms we should adopt (see [testing-matrix.md](testing-matrix.md#testing-paradigms-current-vs-potential) for details):

| Paradigm | Priority | Effort |
|----------|----------|--------|
| Mutation Testing | High | Low |
| Contract Fuzzing | High | Low |
| Expanded Property Testing | High | Medium |
| Snapshot Testing | Medium | Low |
| Deterministic Simulation | Medium | High |

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

# Real network tests (requires feature)
cargo test -p freenet --test test_network_integration --features test-network

# Large scale soak test
cargo test -p freenet --test large_network --features test-network -- --ignored

# fdev CLI testing
cargo run -p fdev -- test --gateways 1 --nodes 5 --events 100 single-process
```
