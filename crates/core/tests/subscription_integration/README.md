# Subscription Integration Test Infrastructure

## Overview

This test infrastructure provides comprehensive testing for Freenet subscription functionality, specifically validating the fixes in PR #1854. It enables multi-node network simulations with detailed logging, monitoring, and debugging capabilities.

## Key Features

- **Multi-node network simulation** with configurable topologies
- **Unified log collection** across all nodes with timeline view
- **Transaction tracing** to follow operations across the network
- **Network monitoring** with connection tracking and message flow visualization
- **Riverctl integration** for testing with the production client
- **GitHub Actions CI** integration for automated testing
- **Detailed test reports** with statistics and diagnostics

## Architecture

### Test Harness (`test_harness.rs`)
- Manages multiple Freenet node instances
- Handles node lifecycle (start, stop, restart)
- Provides WebSocket API connections to each node
- Coordinates test execution

### Network Topologies (`topology.rs`)
- **Linear**: Chain of nodes (GW → N1 → N2 → ... → Nn)
- **Star**: Gateway at center with all nodes connected to it
- **Ring**: Circular topology with gateway as entry point
- **Mesh**: Partially connected network with configurable density
- **Custom**: User-defined topology from adjacency list

### Logging System (`logging.rs`)
- Collects logs from all nodes in real-time
- Creates unified timeline view
- Correlates logs by transaction ID
- Generates sequence diagrams for message flows

### Network Monitor (`monitor.rs`)
- Tracks connections between nodes
- Records message flows with timing
- Detects network partitions
- Monitors contract distribution

## Running Tests

### Prerequisites

```bash
# Required
cargo install cargo-test

# Optional (for riverctl tests)
cargo install riverctl

# Set environment variables
export RUST_LOG=debug
export FREENET_TEST_TIMEOUT=120
```

### Individual Tests

```bash
# Test subscription response routing
cargo test --test subscription_integration test_subscription_response_routing -- --nocapture

# Test optimal location subscriptions
cargo test --test subscription_integration test_optimal_location_subscription -- --nocapture

# Test multiple peer candidates
cargo test --test subscription_integration test_multiple_peer_candidates -- --nocapture

# Comprehensive test suite
cargo test --test subscription_integration test_subscription_fixes_comprehensive -- --nocapture

# Riverctl integration (if installed)
cargo test --test subscription_integration test_riverctl_subscription_flow -- --nocapture --ignored
```

### All Tests

```bash
cargo test --test subscription_integration -- --nocapture
```

## Test Scenarios

### 1. Subscription Response Routing
**Tests**: Transaction ID correlation fix
- Deploys contract on remote node
- Subscribes from gateway
- Verifies response reaches originating client
- Validates `waiting_for_transaction_result` registration

### 2. Optimal Location Subscription
**Tests**: Removal of early return for nodes at optimal location
- Deploys contract to network
- Waits for optimal location placement
- Subscribes from optimal location node
- Verifies subscription succeeds using `k_closest_potentially_caching`

### 3. Multiple Peer Candidates
**Tests**: k=3 peer selection for resilience
- Creates mesh network
- Simulates peer failures
- Verifies subscription attempts multiple candidates
- Confirms success with fallback peers

### 4. Comprehensive Test
**Tests**: All fixes working together
- Multiple simultaneous subscriptions
- Complex network topology
- Verifies no regressions
- Generates detailed reports

## Debugging Failed Tests

### 1. Check Timeline Logs
```bash
cat target/test-logs/timeline.log
```

### 2. Trace Specific Transaction
Look for transaction IDs in the logs:
```
[0.125s] [GW] [DEBUG] Subscribe request tx=abc123
[0.145s] [N1] [DEBUG] Forwarding subscribe tx=abc123
[0.167s] [N3] [DEBUG] Processing subscribe tx=abc123
[0.189s] [N3] [DEBUG] Sending response tx=abc123
[0.210s] [GW] [DEBUG] Received response tx=abc123
```

### 3. Network Visualization
Test output includes:
- Network topology diagram
- Connection status
- Partition detection
- Contract distribution

### 4. Error Analysis
Failed tests generate reports with:
- Operation counts by type
- Log level distribution
- Transaction summaries
- Network statistics

## Adding New Tests

### 1. Create Test Function
```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_my_scenario() -> Result<()> {
    // Setup
    let mut harness = MultiNodeTestHarness::new(
        NetworkTopology::Star { nodes: 5 }
    ).await?;
    harness.start_network().await?;

    // Test logic
    let contract = create_test_contract();
    let key = harness.deploy_contract_on_node(2, contract).await?;

    // Assertions
    TestAssertions::assert_subscription_active(&harness, 0, key).await?;

    // Cleanup
    harness.shutdown().await?;
    Ok(())
}
```

### 2. Custom Assertions
```rust
impl TestAssertions {
    pub async fn assert_my_condition(
        harness: &MultiNodeTestHarness,
        expected: &str,
    ) -> Result<()> {
        // Custom validation logic
    }
}
```

### 3. Network Failures
```rust
// Inject failures for resilience testing
inject_network_failure(
    &mut harness,
    FailureType::NodeCrash("node_3".to_string())
).await?;
```

## CI Integration

Tests run automatically on:
- Push to main/dev branches
- Pull requests
- Manual workflow dispatch

Failed tests upload:
- Complete log timeline
- Test reports
- Network diagnostics

## Performance Considerations

- Tests use fixed seeds for reproducibility
- Network stabilization delays are configurable
- Timeout defaults to 120 seconds
- Parallel test execution supported

## Troubleshooting

### Tests Hang
- Check `FREENET_TEST_TIMEOUT` environment variable
- Verify no port conflicts (tests use random ports)
- Check system resources (each node ~10MB)

### Flaky Tests
- Increase stabilization delays
- Use deterministic network topologies
- Check for race conditions in assertions

### CI Failures
- Review uploaded artifacts
- Check for environment differences
- Verify contract compilation

## Future Improvements

1. **Enhanced Visualization**
   - Interactive network graphs
   - Real-time monitoring dashboard
   - Performance profiling

2. **Failure Injection**
   - Byzantine behavior simulation
   - Network latency injection
   - Packet loss simulation

3. **Load Testing**
   - Concurrent subscription stress tests
   - Large network simulations (100+ nodes)
   - Performance benchmarking

4. **Developer Tools**
   - VS Code test runner integration
   - Test result caching
   - Automatic bisection for regressions

## Contributing

When adding tests for new subscription features:
1. Follow existing test patterns
2. Include comprehensive assertions
3. Add documentation for test scenarios
4. Ensure CI passes before merging
5. Update this README with new test descriptions

## Related Documentation

- [Design Document](design.md) - Detailed architecture design
- [Freenet Core Docs](https://docs.freenet.org)
- [PR #1854](https://github.com/freenet/freenet-core/pull/1854) - Original fixes