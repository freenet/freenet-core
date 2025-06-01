## Summary

Several ping tests that involve network topology constraints (blocked addresses, partial connectivity) are currently marked as ignored because they have never worked properly. These tests fail during node startup with "channel closed" errors.

## Affected Tests

1. **test_ping_partially_connected_network** (apps/freenet-ping/app/tests/run_app.rs)
   - Creates 3 gateways and 7 regular nodes with partial connectivity (CONNECTIVITY_RATIO = 0.5)
   - Fails with: "Gateway node failed: channel closed"
   - Uses asymmetric blocking (node A blocks B, but B doesn't block A)

2. **test_ping_blocked_peers** (apps/freenet-ping/app/tests/run_app_blocked_peers.rs)
   - 1 gateway and 2 nodes that block each other
   - Fails with: "Failed to read contract code" and "channel closed"
   - Contract loading code is broken (tries to read directory as file)

3. **test_ping_blocked_peers_simple** (apps/freenet-ping/app/tests/run_app_blocked_peers.rs)
   - Simplified version of blocked peers test
   - Same failures as above

## Root Causes

1. **Channel closed errors**: When a node's event listener exits/crashes during startup, it drops the notification channel receiver. Other nodes trying to connect get "channel closed" errors when calling `notify_node_event()`.

2. **Test design issues**:
   - Asymmetric blocking in partially connected network test
   - Contract loading expects compiled WASM but tries to read source directory
   - Tests monitor all nodes and exit immediately if any node fails

3. **Blocking mechanism confusion**: 
   - Tests set up TCP addresses for blocking but P2P connections use UDP
   - Blocking logic needs to be in the connect operation's CheckConnectivity handler

## What Was Attempted

In PR #1615, we attempted to fix these tests but discovered they had never worked since creation (added in commit 82451d7e on May 16, 2025). We reverted all changes and kept these tests marked as ignored.

## Proposed Solution

1. Fix contract loading to use `common::load_contract()` function
2. Implement symmetric blocking (if node i blocks j, then j also blocks i)
3. Add proper blocked address handling in the connect operation
4. Debug why gateway nodes fail during startup in these specific test configurations
5. Consider hardcoding peer connections instead of using CONNECTIVITY_RATIO for more predictable test behavior

## Related PRs

- #1615 - Where these tests were discovered to be broken and marked as ignored