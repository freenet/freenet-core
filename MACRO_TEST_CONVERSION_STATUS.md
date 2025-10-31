# Test Conversion Status: `#[freenet_test]` Macro

## Overview

This document tracks which integration tests have been converted to use the `#[freenet_test]` procedural macro and explains why certain tests were not converted.

## Successfully Converted Tests

### error_notification.rs (3/4 tests converted)

✅ **Converted:**
1. `test_get_error_notification` - GET operation error notification
2. `test_put_error_notification` - PUT operation error notification
3. `test_update_error_notification` - UPDATE operation error notification

❌ **Not Converted:**
- `test_connection_drop_error_notification` - Requires custom peer lifecycle management (selective shutdown)
  - **Reason:** The macro doesn't support stopping individual nodes mid-test

### isolated_node_regression.rs (4/4 tests converted)

✅ **All Converted:**
1. `test_isolated_node_put_get_workflow` - Complete PUT→GET workflow on isolated node
2. `test_concurrent_get_deduplication_race` - Concurrent GET operations
3. `test_isolated_node_local_subscription` - Subscribe operations on isolated node
4. `test_isolated_node_update_operation` - UPDATE operations on isolated nodes

### test_macro_example.rs (7/7 tests - examples)

✅ **All Using Macro:**
1. `test_single_node` - Single gateway node
2. `test_multi_node` - Multiple nodes (gateway + 2 peers)
3. `test_with_event_aggregation` - Event aggregation on failure
4. `test_always_aggregate` - Event aggregation always
5. `test_custom_tokio_config` - Custom tokio worker threads
6. `test_current_thread_runtime` - Single-threaded tokio runtime
7. `test_multiple_gateways` - Multiple gateway nodes (NEW!)

## Tests Not Yet Converted

### operations.rs (0/11 tests converted)

**Tests in this file:**
- `test_put_contract`
- `test_update_contract`
- `test_put_merge_persists_state`
- `test_multiple_clients_subscription`
- `test_get_with_subscribe_flag`
- `test_put_with_subscribe_flag`
- `test_delegate_request`
- `test_gateway_packet_size_change_after_60s`
- `test_production_decryption_error_scenario`
- `test_subscription_introspection`
- `test_update_no_change_notification`

**Why not converted:**
1. **Complex peer-gateway relationships** - Tests set up specific gateway configurations and peer connections
2. **Custom gateway discovery** - Uses `gw_config()` and `base_node_test_config()` helpers with specific network topologies
3. **Multi-phase testing** - Some tests require stopping/starting nodes at specific points
4. **Contract persistence verification** - Tests verify data persists across specific node restarts
5. **Uses `#[test_log::test]`** - Would need to verify macro compatibility

**Future conversion approach:**
- Macro would need to support:
  - Configuring peer gateway lists (which gateways peers should connect to)
  - Mid-test node lifecycle control (stop/restart specific nodes)
  - Or: Create specialized macros for these patterns (e.g., `#[freenet_network_test]`)

### connectivity.rs (0/3 tests converted)

**Tests in this file:**
- `test_gateway_reconnection`
- `test_basic_gateway_connectivity`
- `test_three_node_network_connectivity`

**Why not converted:**
1. **Network topology testing** - These tests specifically test connection establishment and maintenance
2. **Custom disconnect/reconnect logic** - Tests intentionally disconnect and reconnect nodes
3. **Connection state verification** - Tests verify specific connection counts and states
4. **Requires fine-grained control** - Need to control exactly when nodes connect/disconnect

**Future conversion approach:**
- Similar to operations.rs, would need lifecycle control features
- Or: Keep as-is since these are network layer tests, not contract operation tests

### Other Test Files

- `redb_migration.rs` - Database migration tests (no multi-node setup)
- `token_expiration.rs` - Token config tests (no multi-node setup)
- `ubertest.rs` - Large-scale River app test (too specialized/complex)

## Conversion Statistics

- **Total test files analyzed:** 8
- **Files with converted tests:** 3
- **Total tests converted:** 7
- **Total boilerplate lines eliminated:** ~300+

## Macro Features Used in Converted Tests

### Basic Features
- ✅ Single gateway node
- ✅ Multiple peer nodes
- ✅ Multiple gateway nodes (NEW!)
- ✅ Custom timeouts
- ✅ Custom startup wait times
- ✅ Event aggregation (on_failure, always, never)
- ✅ Custom log levels
- ✅ Tokio flavor configuration
- ✅ Tokio worker thread configuration

### TestContext API Used
- ✅ `ctx.node(label)` - Get specific node
- ✅ `ctx.gateway()` - Get first gateway
- ✅ `ctx.gateways()` - Get all gateways (NEW!)
- ✅ `ctx.peers()` - Get all peers (NEW!)
- ✅ `ctx.node_labels()` - Get all node labels
- ✅ `ctx.event_log_path(label)` - Get node event log path
- ✅ `ctx.aggregate_events()` - Aggregate events from all nodes
- ✅ `ctx.generate_failure_report(error)` - Generate comprehensive failure report
- ✅ `ctx.generate_success_summary()` - Generate success summary

## Future Enhancements

### For Conversion of Remaining Tests

1. **Peer Gateway Configuration**
   ```rust
   #[freenet_test(
       nodes = ["gw-1", "gw-2", "peer-1", "peer-2"],
       gateways = ["gw-1", "gw-2"],
       peer_gateways = {
           "peer-1": ["gw-1"],
           "peer-2": ["gw-2"]
       }
   )]
   ```

2. **Node Lifecycle Control**
   ```rust
   async fn test(ctx: &mut TestContext) -> TestResult {
       // Start with nodes running
       ctx.stop_node("peer-1").await?;
       // Test something
       ctx.start_node("peer-1").await?;
       // Test something else
       Ok(())
   }
   ```

3. **Network State Verification**
   ```rust
   async fn test(ctx: &mut TestContext) -> TestResult {
       let conn_count = ctx.connection_count("gateway").await?;
       assert_eq!(conn_count, 2);
       Ok(())
   }
   ```

### Specialized Macros

For network/connectivity tests, consider creating specialized macros:

```rust
#[freenet_network_test(
    topology = "star",  // or "mesh", "chain", etc.
    gateway_count = 1,
    peer_count = 3,
    test_disconnections = true
)]
async fn test(ctx: &mut NetworkTestContext) -> TestResult {
    // Network-specific test context with connection control
    Ok(())
}
```

## Conclusion

The `#[freenet_test]` macro successfully handles:
- ✅ Isolated node tests
- ✅ Simple multi-node tests
- ✅ Tests with multiple gateways
- ✅ Tests with custom tokio configurations
- ✅ Tests with event aggregation

The macro is **not suitable** for:
- ❌ Tests requiring specific peer-gateway connections
- ❌ Tests requiring node lifecycle control (stop/restart)
- ❌ Network topology and connectivity tests
- ❌ Tests with complex custom configurations

For these cases, the existing test infrastructure remains appropriate and should be kept as-is.
