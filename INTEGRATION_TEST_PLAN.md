# Plan: Multi-Peer Integration Tests for PR #1854

## Current Situation
- PR #1854 has the subscription fixes but no integration tests
- Existing `freenet-ping` app has working multi-node test infrastructure
- Previous attempt to add new test framework caused CI failures

## Proposed Solution: Leverage Existing Infrastructure

### Phase 1: Add Subscription Tests to freenet-ping (Immediate)

Create a new test file: `apps/freenet-ping/app/tests/test_subscription_fixes.rs`

This will:
1. Use the existing `common` module infrastructure
2. Work with current CI setup (no new workflows needed)
3. Test all three subscription fixes from PR #1854

#### Test Scenarios

```rust
// Test 1: Subscription Response Routing
#[tokio::test]
async fn test_subscription_response_reaches_client() {
    // Setup 3-node network: Gateway -> Node1 -> Node2
    // Deploy contract on Node2
    // Subscribe from Gateway
    // Verify Gateway receives subscription confirmation
    // This tests the waiting_for_transaction_result fix
}

// Test 2: Optimal Location Subscription
#[tokio::test]
async fn test_optimal_location_can_subscribe() {
    // Setup 5-node network
    // Deploy contract and let it reach optimal location
    // Have optimal node subscribe to its own contract
    // Verify subscription succeeds (no early return)
    // This tests removal of early return in start_subscription_request
}

// Test 3: Multiple Peer Candidates
#[tokio::test]
async fn test_subscription_tries_multiple_peers() {
    // Setup network with some blocked peers
    // Subscribe to contract
    // Verify subscription succeeds despite some peers being unavailable
    // This tests k_closest_potentially_caching with k=3
}
```

### Phase 2: Enhanced Diagnostics (Next Week)

Add diagnostic helpers to track subscription flow:
- Transaction correlation in logs
- Subscription state tracking
- Message flow visualization

### Phase 3: Comprehensive Test Suite (Future)

Once basic tests pass:
- Add stress tests with many simultaneous subscriptions
- Test subscription updates and unsubscribes
- Add riverctl integration when available

## Implementation Steps

### Step 1: Create Basic Test File
```bash
# Create test file using existing patterns
cp apps/freenet-ping/app/tests/run_app.rs \
   apps/freenet-ping/app/tests/test_subscription_fixes.rs
# Then modify for subscription testing
```

### Step 2: Use Existing Helpers
- `base_node_test_config()` - Node configuration
- `connect_ws_client()` - WebSocket connections
- `wait_for_subscribe_response()` - Already exists!
- `collect_node_diagnostics()` - For debugging

### Step 3: Run Tests Locally
```bash
cd apps/freenet-ping
cargo test test_subscription_fixes -- --nocapture
```

### Step 4: Verify in CI
- Tests will run automatically as part of existing CI
- No new workflows needed
- Uses existing test infrastructure

## Key Advantages

1. **Works Today** - No CI configuration needed
2. **Proven Infrastructure** - Already handles multi-node tests
3. **Real Network** - Tests actual node processes, not mocks
4. **Debugging Tools** - Diagnostics already built in
5. **Existing Patterns** - Follows project conventions

## Success Criteria

✅ Tests run in existing CI without changes
✅ All three subscription fixes are validated
✅ Tests are maintainable and debuggable
✅ Can be extended for future subscription features

## Timeline

- **Today**: Create basic test file with 3 scenarios
- **Tomorrow**: Debug and refine tests
- **This Week**: Get tests passing and merge with PR #1854
- **Next Week**: Add enhanced diagnostics if needed

## Why This Approach Works

1. **No New Infrastructure** - Uses what's already there
2. **CI Compatible** - Runs in existing test suite
3. **Battle-tested** - freenet-ping tests already work
4. **Quick to Implement** - Can have working tests today
5. **Easy to Debug** - Logs and diagnostics built in

## Next Action

Create `test_subscription_fixes.rs` with the three test scenarios using the existing freenet-ping test infrastructure.