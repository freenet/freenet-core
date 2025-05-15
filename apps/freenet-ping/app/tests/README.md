# Freenet Ping App Test Suite

This directory contains integration tests for the Freenet Ping application, focusing on contract state propagation in various network topologies and connectivity scenarios.

## Test Organization

| Test File                  | Scenario                                                                                                                                           |
| -------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `run_app.rs`               | **Multi-node and networking tests.** Contains multiple test functions focusing on basic multi-node functionality and partially connected networks. |
| `run_app_blocked_peers.rs` | **Parameterized blocked peers tests.** A unified implementation of all blocked peers tests with different configurations.                          |

## Parameterized "Blocked Peers" Tests

The `run_app_blocked_peers.rs` file contains a parameterized implementation that consolidates all blocked peers testing variants into a single file. The file provides the following test variants:

| Test Function                       | Description                                                                      |
| ----------------------------------- | -------------------------------------------------------------------------------- |
| `test_ping_blocked_peers`           | **Baseline implementation.** Standard test for indirect propagation via gateway. |
| `test_ping_blocked_peers_simple`    | **Minimal variant.** One round of updates, simplified verification.              |
| `test_ping_blocked_peers_optimized` | **Faster timeouts.** Reduced wait times for quicker test execution.              |
| `test_ping_blocked_peers_improved`  | **Enhanced robustness.** Longer waits and more thorough verification.            |
| `test_ping_blocked_peers_debug`     | **Debugging support.** Verbose logging and frequent state checks.                |
| `test_ping_blocked_peers_reliable`  | **Reliability focus.** Multiple update rounds and refresh updates.               |
| `test_ping_blocked_peers_solution`  | **Reference implementation.** Best practices for indirect propagation.           |

## Test Structure in `run_app.rs`

| Test Function                           | Description                                                      |
| --------------------------------------- | ---------------------------------------------------------------- |
| `test_ping_multi_node`                  | Tests basic contract propagation in a fully-connected network.   |
| `test_ping_application_loop`            | Tests the complete client application running over time.         |
| `test_ping_partially_connected_network` | Tests propagation in a larger network with partial connectivity. |

### Multi-node vs. Partially Connected Tests

- **Multi-node test**: Uses a fully connected network where all nodes can directly communicate with each other
- **Partially connected test**: Creates a larger network with controlled connectivity ratio (50%) between nodes, testing how updates propagate in a more realistic, constrained network topology

## The "Blocked Peers" Test Scenario

The "blocked peers" tests verify that contract state updates can propagate correctly even when direct peer-to-peer connections are blocked:

1. Two regular nodes (Node1 and Node2) are configured to block each other's network addresses
2. A gateway node is connected to both regular nodes
3. All nodes subscribe to the ping contract
4. Each node sends state updates with its own unique identifier
5. Updates must route through the gateway to reach nodes with blocked direct connections
6. The test verifies that all nodes eventually have the same consistent state

This tests Freenet's ability to maintain contract state consistency even when the network topology prevents direct communication between some peers.

## Common Test Infrastructure

The `common/mod.rs` module provides shared infrastructure for tests, including:

- Node and gateway configuration
- Contract deployment helpers
- State comparison utilities
- WebSocket connection management
- State update and verification functions

## Parameterized Testing Approach

The unified approach in `run_app_blocked_peers.rs` offers several advantages:

- **Reduced duplication**: Core test logic is defined once and reused
- **Consistent methodology**: All variants follow the same testing pattern
- **Parameterized configurations**: Tests differ only in timing, logging, and update strategies
- **Easier maintenance**: Changes to the core test logic only need to be made in one place
- **Better test coverage**: Different configurations stress the system in different ways

## Running the Tests

Run all tests with:

```bash
cd apps/freenet-ping
cargo test
```

Run a specific test variant:

```bash
cargo test test_ping_blocked_peers_simple
```

Run the large-scale network test:

```bash
cargo test test_ping_partially_connected_network
```

---
