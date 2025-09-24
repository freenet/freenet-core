# Subscription Testing Notes

## What We Tried

We attempted to create multi-node integration tests to validate the subscription routing fixes in PR #1854. These tests would spin up multiple Freenet nodes and verify subscription message routing across the network.

## Why It Failed

### 1. Configuration Complexity
- Tests failed with: `invalid type: floating point 127.0, expected struct InlineGwConfig`
- The `base_node_test_config` helper expects specific configuration formats
- Directory structures and paths differ between CI and local environments

### 2. Async/Threading Issues
- Non-Send futures required complex `.boxed_local()` patterns
- `tokio::spawn` couldn't be used due to thread safety constraints
- Race conditions between node startup and test execution

### 3. Too Many Moving Parts
- Each test needs to:
  - Start multiple node processes
  - Wait for network stabilization
  - Deploy contracts
  - Route subscriptions through multiple hops
  - Handle timeouts and retries
- Any failure in these steps causes test failure, even if subscription logic is correct

### 4. Wrong Level of Testing
- We were testing simple code fixes with complex integration tests
- The actual fixes are:
  - Registering transaction IDs for response routing
  - Removing an early return that prevented optimal location subscriptions
  - Using k=3 for peer candidate selection
- These can be validated with focused unit tests

## Current Approach

We've added focused unit tests that directly validate:
1. Transaction ID correlation for response routing
2. Skip list usage for retry logic (k=3 candidates)
3. State transitions through the subscription lifecycle
4. Proper key storage for subscription responses

These unit tests are more maintainable, run faster, and directly test the fixes without the complexity of full network simulation.

## Future Work

Integration testing for subscriptions should be revisited in a separate PR with:
- Purpose-built test infrastructure (not reusing ping app helpers)
- Mock network layer for deterministic testing
- Clear separation between network setup and test logic
- Better debugging output for CI failures