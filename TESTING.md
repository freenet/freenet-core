# Testing Guidelines for Freenet Core

This document establishes testing standards to prevent regressions in Freenet's distributed network functionality.

## Why These Guidelines Exist

**Real Incident (October 2025)**: A bug in peer-to-peer mesh formation existed for 4 months because:
1. An integration test caught the bug and failed
2. The test was removed instead of being fixed ("test deletion culture")
3. Only a unit test was added, which didn't test end-to-end behavior
4. The bug shipped in production, preventing mesh topology formation

**Lesson**: Tests that fail are telling us something important. Never delete a failing test without understanding the root cause.

## Test-Driven Development (TDD) Requirements

### For Network/Topology/Routing Changes

Any changes to these critical areas MUST follow TDD:
- `crates/core/src/ring/`
- `crates/core/src/topology/`
- `crates/core/src/operations/connect.rs`
- `crates/core/src/node/network_bridge/`

**Process:**
1. Write integration test demonstrating the issue or new behavior
2. Verify test **fails** (Red)
3. Implement the fix
4. Verify test **passes** (Green)
5. Refactor if needed
6. Run full test suite before creating PR

**Example:**
```rust
// tests/connectivity.rs

/// Regression test for issue #XXXX
///
/// Previously, peers with only gateway connections couldn't discover other peers
/// because routing skip_list prevented using the gateway to find peers.
#[tokio::test]
async fn test_mesh_formation_through_gateway() -> TestResult {
    // Test demonstrates the bug exists
    // Implementation fixes it
    // Test now passes
}
```

## Test Deletion Policy

### NEVER Delete a Test Unless:

- [ ] **Root cause fully understood** - Document in GitHub issue with `test-coverage` label
- [ ] **Alternative coverage proven** - Show equivalent or better test exists
- [ ] **Two-developer approval** - Requires review from two different developers
- [ ] **Issue created** - Tag with `test-coverage-gap` explaining why test was removed

### If a Test Fails:

1. **DO NOT** immediately disable or remove it
2. **DO** investigate why it's failing - this is a critical signal
3. **DO** create an issue if you can't fix it immediately
4. **DO** use `#[ignore]` with `TODO-MUST-FIX` comment (blocks commits via pre-commit hook)

```rust
// TODO-MUST-FIX: Test reveals routing issue when peers have few connections
#[tokio::test]
#[ignore = "Investigating routing failure - see TODO-MUST-FIX above"]
async fn test_that_revealed_a_bug() -> TestResult {
    // ...
}
```

## Critical Integration Tests

These tests MUST exist and pass before any release:

### Network Topology Tests (`tests/connectivity.rs`)

- `test_basic_gateway_connectivity` - Verify gateway accepts connections
- `test_gateway_reconnection` - Verify reconnection after disconnect
- `test_three_node_network_connectivity` - **Verify p2p mesh formation** ⚠️ CRITICAL

  This test prevented a 4-month regression when it was wrongly removed.

### Contract Operation Tests (`tests/operations.rs`)

- `test_put_contract` - Verify contract storage
- `test_get_contract` - Verify contract retrieval
- `test_update_contract` - Verify state updates
- `test_subscribe_contract` - Verify subscriptions work

### Connection Maintenance Tests

These verify the network self-heals and maintains optimal topology:

- Periodic maintenance runs (every 60s in production, 2s in tests)
- Topology adjustments create new connections
- Failed connections are pruned
- Network converges to mesh topology

## Pre-Release Testing Checklist

Before ANY release, ALL of these must pass:

```bash
# 1. Unit tests
cd ~/code/freenet/freenet-core/main
cargo test --workspace

# 2. Integration tests (critical - cannot skip)
cargo test --test connectivity --no-fail-fast
cargo test --test operations --no-fail-fast

# 3. Gateway test framework
cd ~/code/freenet/freenet-testing-tools/gateway-testing
python gateway_test_framework.py --local

# 4. Extended stability test (for major releases)
python gateway_test_framework.py --local --extended-stability

# 5. Multi-room stress test (for major releases)
python gateway_test_framework.py --local --multi-room 50
```

**If ANY test fails**: Stop. Investigate. Do not release until fixed.

## Test Organization

### Unit Tests
Location: `crates/*/src/` (alongside code)
- Test individual functions/modules
- Fast execution (<1s per test)
- Mock external dependencies
- Good for logic verification

### Integration Tests
Location: `crates/core/tests/`
- Test end-to-end scenarios
- Real network simulation
- Slower but crucial (<3min per test)
- Cannot be mocked away

**Rule**: Unit tests prove code works in isolation. Integration tests prove it works in reality.

## Writing Good Integration Tests

### Characteristics of a Good Test:

1. **Clear Purpose** - Document what regression it prevents
   ```rust
   /// Regression test for issue #1889
   /// Verifies peers form p2p mesh instead of star topology
   ```

2. **Deterministic** - Same input = same output
   - Use fixed random seeds: `rand::rngs::StdRng::from_seed(...)`
   - Set explicit timeouts
   - Don't depend on timing unless testing timing

3. **Self-Contained** - No dependencies on external state
   - Use `tempfile::tempdir()` for data
   - Bind to `127.0.0.1:0` for random ports
   - Clean up resources

4. **Observable** - Test actual behavior, not implementation
   ```rust
   // Good: Test observable behavior
   assert_eq!(peer_connections.len(), 2, "Should form mesh");

   // Bad: Test implementation details
   assert!(internal_skip_list.contains(&gateway));
   ```

5. **Reasonable Timeout** - Don't wait forever
   ```rust
   tokio::time::timeout(Duration::from_secs(180), test_logic)
   ```

### Test Patterns for Network Code:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_network_behavior() -> TestResult {
    // 1. Setup: Create nodes with known configurations
    let gateway = create_gateway()?;
    let peer1 = create_peer(gateway_info)?;
    let peer2 = create_peer(gateway_info)?;

    // 2. Act: Run nodes and wait for convergence
    tokio::time::sleep(Duration::from_secs(20)).await;

    // 3. Assert: Verify expected network state
    let connections = query_peer_connections(peer1).await?;
    assert!(connections.len() >= 2, "Should form mesh");

    // 4. Cleanup: Graceful shutdown
    disconnect_all().await?;
    Ok(())
}
```

## Code Review Requirements

### For PRs Touching Critical Paths:

Any PR modifying network, topology, or connection code must:

1. **Include Tests**
   - [ ] Integration test demonstrating the change
   - [ ] Test fails without the fix (shown in PR description)
   - [ ] Test passes with the fix

2. **Show Impact**
   ```markdown
   ## Testing

   ### Before (test fails):
   ```
   Network not fully connected after 60s
   ```

   ### After (test passes):
   ```
   ✅ Full mesh connectivity established in 14s
   ```
   ```

3. **Explain Behavior**
   - Describe impact on mesh formation
   - Show connection patterns (diagrams help!)
   - Explain any tradeoffs

4. **Get Reviews**
   - Minimum 2 reviewers for network code
   - At least 1 must be familiar with topology/routing

## Continuous Integration

### GitHub Actions (`.github/workflows/tests.yml`)

```yaml
name: Tests

on: [push, pull_request]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - run: cargo test --workspace

  integration-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - run: cargo test --test connectivity --no-fail-fast
      - run: cargo test --test operations --no-fail-fast

  test-deletion-guard:
    runs-on: ubuntu-latest
    steps:
      - name: Check for test deletions
        run: |
          # Warn if tests were deleted without approval
          git diff origin/main --diff-filter=D -- '*.rs' | grep -q '#\[.*test\]' && \
            echo "::warning::Tests were deleted - ensure proper review process" || true
```

## Pre-Commit Hooks

The repository includes a pre-commit hook that blocks commits containing `TODO-MUST-FIX`:

```bash
# .git/hooks/pre-commit
if git diff --cached | grep -q "TODO-MUST-FIX"; then
    echo "ERROR: Cannot commit code with TODO-MUST-FIX markers"
    echo "Fix the issue or remove the marker before committing"
    exit 1
fi
```

## Common Testing Pitfalls

### ❌ Don't:
- Delete failing tests without understanding root cause
- Only add unit tests for complex distributed behavior
- Rely on timing for correctness (use explicit synchronization)
- Test implementation details instead of observable behavior
- Skip integration tests because they're slow

### ✅ Do:
- Treat test failures as critical signals
- Write integration tests for network/topology changes
- Use timeouts to prevent hanging tests
- Test observable behavior from external perspective
- Run full test suite before every PR

## Resources

- **Gateway Test Framework**: `~/code/freenet/freenet-testing-tools/gateway-testing/README.md`
- **Test Utilities**: `crates/core/src/test_utils/`
- **Example Tests**: `crates/core/tests/connectivity.rs`
- **CI Configuration**: `.github/workflows/tests.yml`

## Questions?

- Ask in #freenet:matrix.org
- Tag PRs with `testing` label for review help
- Create issues with `test-coverage` label for test-related discussions

---

**Remember**: Tests are not bureaucracy. They are the safety net that lets us move fast without breaking things. Respect the tests, and they will save you from 4-month regressions.
