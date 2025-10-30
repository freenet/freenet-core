# Freenet Test Macros

Procedural macros for simplifying Freenet integration tests with automatic node setup, event aggregation, and failure reporting.

## Overview

The `#[freenet_test]` macro automates the boilerplate required for multi-node Freenet integration tests, providing:

- **Automatic node setup** - Gateway and peer nodes configured with temp directories
- **Event log aggregation** - Automatic collection and analysis of events across all nodes
- **Enhanced failure reporting** - Detailed statistics, timelines, and event breakdowns when tests fail
- **Flexible configuration** - Control timeouts, tokio runtime, peer connections, and more
- **Cleanup handling** - Automatic resource cleanup on test completion

## Quick Start

### Basic Test with Gateway

```rust
use freenet::test_utils::TestContext;
use freenet_macros::freenet_test;

#[freenet_test(nodes = ["gateway"])]
async fn test_basic_gateway(ctx: &mut TestContext) -> TestResult {
    let gateway = ctx.gateway()?;
    assert!(gateway.is_gateway);
    assert!(gateway.network_port.is_some());
    Ok(())
}
```

### Multi-Node Test with Auto-Connect

```rust
#[freenet_test(
    nodes = ["gateway", "peer-1", "peer-2"],
    auto_connect_peers = true,
    aggregate_events = "on_failure"
)]
async fn test_network_operations(ctx: &mut TestContext) -> TestResult {
    let gateway = ctx.gateway()?;
    let peers = ctx.peers();

    // All peers are automatically configured to connect to the gateway
    assert_eq!(peers.len(), 2);

    // Your test logic here...
    Ok(())
}
```

## Macro Attributes

### Required Attributes

#### `nodes`
List of node labels to create. Node labels are used for identification in logs and event aggregation.

```rust
#[freenet_test(nodes = ["gateway"])]                    // Single gateway
#[freenet_test(nodes = ["gateway", "peer-1", "peer-2"])]  // Gateway + 2 peers
```

**Default Gateway Selection:**
- By default, the **first node** in the list is the gateway
- All other nodes are peers

### Optional Attributes

#### `gateways`
Explicitly specify which nodes should be gateways (supports multiple gateways).

```rust
#[freenet_test(
    nodes = ["gw-1", "gw-2", "peer-1", "peer-2"],
    gateways = ["gw-1", "gw-2"]  // Both gw-1 and gw-2 are gateways
)]
async fn test_multi_gateway(ctx: &mut TestContext) -> TestResult {
    let gateways = ctx.gateways();
    assert_eq!(gateways.len(), 2);
    Ok(())
}
```

#### `auto_connect_peers`
Automatically configure all peer nodes to connect to all gateway nodes.

```rust
#[freenet_test(
    nodes = ["gateway", "peer-1", "peer-2"],
    auto_connect_peers = true  // Peers auto-connect to gateway
)]
```

**Behavior:**
- When `true`: Peers are pre-configured with gateway connection info
- When `false` (default): You must manually configure peer connections
- Works with multiple gateways (peers connect to all gateways)

#### `aggregate_events`
Control when event aggregation reports are generated.

```rust
#[freenet_test(
    nodes = ["gateway", "peer"],
    aggregate_events = "always"      // Show report on success and failure
)]
#[freenet_test(
    nodes = ["gateway", "peer"],
    aggregate_events = "on_failure"  // Only show report on failure (default)
)]
#[freenet_test(
    nodes = ["gateway", "peer"],
    aggregate_events = "never"       // Never show reports
)]
```

**Values:**
- `"always"` - Generate detailed report for all test runs
- `"on_failure"` (default) - Only generate report when test fails
- `"never"` - Disable event aggregation

**Report Contents:**
- Event statistics by type (Connect, Put, Get, Update, etc.)
- Per-peer event counts
- Chronological timeline with millisecond timestamps
- Visual icons for each event type
- Event details (first 60 chars of Debug output)

#### `timeout_secs`
Maximum time (in seconds) for the test to complete.

```rust
#[freenet_test(
    nodes = ["gateway", "peer"],
    timeout_secs = 120  // Test must complete within 120 seconds
)]
```

**Default:** 60 seconds

#### `startup_wait_secs`
Time (in seconds) to wait for all nodes to start before running test logic.

```rust
#[freenet_test(
    nodes = ["gateway", "peer-1", "peer-2"],
    startup_wait_secs = 15  // Wait 15 seconds for node startup
)]
```

**Default:** 5 seconds

**Note:** Increase this for:
- Tests with many nodes
- Tests that require network connections to establish
- Slow CI environments

#### `tokio_flavor`
Tokio runtime flavor for the test.

```rust
#[freenet_test(
    nodes = ["gateway"],
    tokio_flavor = "multi_thread"    // Multi-threaded runtime
)]
#[freenet_test(
    nodes = ["gateway"],
    tokio_flavor = "current_thread"  // Single-threaded runtime (default)
)]
```

**Values:**
- `"multi_thread"` - Multi-threaded tokio runtime
- `"current_thread"` (default) - Single-threaded runtime

#### `tokio_worker_threads`
Number of worker threads for multi-threaded runtime.

```rust
#[freenet_test(
    nodes = ["gateway", "peer-1", "peer-2"],
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 8  // Use 8 worker threads
)]
```

**Note:** Only valid when `tokio_flavor = "multi_thread"`

#### `log_level`
Logging level for the test.

```rust
#[freenet_test(
    nodes = ["gateway"],
    log_level = "info"   // Info level logging
)]
#[freenet_test(
    nodes = ["gateway"],
    log_level = "debug"  // Debug level logging
)]
```

**Default:** `"info"`

**Values:** `"trace"`, `"debug"`, `"info"`, `"warn"`, `"error"`

## TestContext API

The macro provides a `TestContext` parameter to your test function with these methods:

### Node Access

```rust
// Get a specific node by label
let gateway = ctx.node("gateway")?;

// Get the first gateway node
let gateway = ctx.gateway()?;

// Get all gateway nodes
let gateways = ctx.gateways();

// Get all peer (non-gateway) nodes
let peers = ctx.peers();
```

### NodeInfo Structure

Each node provides:
```rust
pub struct NodeInfo {
    pub label: String,           // Human-readable label (e.g., "gateway")
    pub ws_port: u16,            // WebSocket API port
    pub network_port: Option<u16>, // Network port (Some for gateways, None for peers)
    pub location: f64,           // Node location in ring (0.0 to 1.0)
    pub is_gateway: bool,        // True if this is a gateway node
    pub temp_dir_path: PathBuf,  // Temporary directory for this node
}
```

### Event Log Access

```rust
// Get path to a node's event log file
let log_path = ctx.event_log_path("gateway")?;

// Get all node labels
let labels = ctx.node_labels();

// Aggregate events from all nodes (called automatically on failure)
let aggregator = ctx.aggregate_events().await?;
let all_events = aggregator.get_all_events().await?;
```

## Complete Example

```rust
use freenet::test_utils::{TestContext, make_put, make_get, load_contract, create_empty_todo_list};
use freenet_macros::freenet_test;
use freenet_stdlib::prelude::*;

#[freenet_test(
    nodes = ["gateway", "peer-1", "peer-2"],
    auto_connect_peers = true,
    timeout_secs = 180,
    startup_wait_secs = 15,
    aggregate_events = "on_failure",
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_contract_replication(ctx: &mut TestContext) -> TestResult {
    // Load test contract
    let contract = load_contract("test-contract-integration", vec![].into())?;
    let state = WrappedState::from(create_empty_todo_list());

    // Get peer to perform PUT
    let peer1 = ctx.node("peer-1")?;
    let uri = format!("ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native", peer1.ws_port);
    let (stream, _) = tokio_tungstenite::connect_async(&uri).await?;
    let mut client = WebApi::start(stream);

    // Perform PUT operation
    tracing::info!("Performing PUT operation");
    make_put(&mut client, state.clone(), contract.clone(), false).await?;

    // Wait for PUT response
    match tokio::time::timeout(Duration::from_secs(30), client.recv()).await? {
        Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key })) => {
            assert_eq!(key, contract.key());
            tracing::info!("PUT successful");
        }
        Ok(other) => bail!("Unexpected response: {:?}", other),
        Err(e) => bail!("Error receiving response: {}", e),
    }

    // Verify replication on peer-2
    let peer2 = ctx.node("peer-2")?;
    let uri2 = format!("ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native", peer2.ws_port);
    let (stream2, _) = tokio_tungstenite::connect_async(&uri2).await?;
    let mut client2 = WebApi::start(stream2);

    // Perform GET to verify replication
    tracing::info!("Verifying replication with GET");
    make_get(&mut client2, contract.key(), true, false).await?;

    match tokio::time::timeout(Duration::from_secs(30), client2.recv()).await? {
        Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
            state: recv_state,
            contract: recv_contract,
            ..
        })) => {
            assert_eq!(recv_contract.as_ref().unwrap().key(), contract.key());
            assert_eq!(recv_state, state);
            tracing::info!("Contract successfully replicated");
        }
        Ok(other) => bail!("Unexpected GET response: {:?}", other),
        Err(e) => bail!("Error receiving GET response: {}", e),
    }

    Ok(())
}
```

## Event Aggregation Output

When a test fails with `aggregate_events = "on_failure"` or `"always"`, you get:

```
================================================================================
TEST FAILURE REPORT
================================================================================

Error: Timeout waiting for PUT response

--------------------------------------------------------------------------------
EVENT LOG SUMMARY
--------------------------------------------------------------------------------

📊 Event Statistics:
  Total events: 15

  By type:
    Connect: 3
    Put: 4
    Route: 6
    Ignored: 2

  By peer:
    v6MWKgqK: 8 events
    v6MWKgqJ: 4 events
    v6MWKgqI: 3 events

📅 Event Timeline:
  [     0ms] v6MWKgqK 🔗 Connect(Connected { ... })
  [     5ms] v6MWKgqJ 🔗 Connect(Connected { ... })
  [    10ms] v6MWKgqI 🔗 Connect(Connected { ... })
  [ 11158ms] v6MWKgqK 📤 Put(Request { contract_key: ... })
  [ 11193ms] v6MWKgqJ 🔀 Route(RoutingMessage { ... })
  [ 11245ms] v6MWKgqI 📤 Put(PutSuccess { key: ... })
  [ 11290ms] v6MWKgqJ ⏭️ Ignored
  [ 11325ms] v6MWKgqK 📤 Put(BroadcastReceived { ... })

================================================================================
```

## Best Practices

### 1. Use Descriptive Node Labels

```rust
// Good: Clear identification
#[freenet_test(nodes = ["gateway", "storage-peer", "client-peer"])]

// Less clear
#[freenet_test(nodes = ["node1", "node2", "node3"])]
```

### 2. Set Appropriate Timeouts

```rust
// For simple connectivity tests
#[freenet_test(nodes = ["gateway"], timeout_secs = 30)]

// For complex operations with network propagation
#[freenet_test(
    nodes = ["gateway", "peer-1", "peer-2", "peer-3"],
    timeout_secs = 180
)]
```

### 3. Use Event Aggregation for Debugging

```rust
// During development, always see events
#[freenet_test(
    nodes = ["gateway", "peer"],
    aggregate_events = "always"  // Helpful during debugging
)]

// In CI, only on failure
#[freenet_test(
    nodes = ["gateway", "peer"],
    aggregate_events = "on_failure"  // Keep CI output clean
)]
```

### 4. Increase Startup Wait for Complex Tests

```rust
// Complex multi-node tests need more startup time
#[freenet_test(
    nodes = ["gw-1", "gw-2", "peer-1", "peer-2", "peer-3", "peer-4"],
    gateways = ["gw-1", "gw-2"],
    auto_connect_peers = true,
    startup_wait_secs = 20  // More time for connections to establish
)]
```

### 5. Use Multi-threaded Runtime for Performance Tests

```rust
#[freenet_test(
    nodes = ["gateway", "peer-1", "peer-2", "peer-3"],
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 8  // Better parallelism
)]
```

## Troubleshooting

### Panic Handling (✅ Now Supported!)

**Good News:** The macro now catches panics and generates event aggregation reports!

**How It Works:** The test is spawned in a separate tokio task, allowing the macro to catch panics via `JoinError` and convert them to regular errors before reporting.

**Example:**

```rust
// ✅ Both work now - panic is caught and report is generated!
#[freenet_test(nodes = ["gateway", "peer"])]
async fn test_with_assertion(ctx: &mut TestContext) -> TestResult {
    let value = get_value()?;
    assert_eq!(value, 42);  // Panic is caught - full event report generated!
    Ok(())
}

#[freenet_test(nodes = ["gateway", "peer"])]
async fn test_with_ensure(ctx: &mut TestContext) -> TestResult {
    let value = get_value()?;
    ensure!(value == 42, "Expected 42, got {}", value);  // Also generates report
    Ok(())
}
```

**What You Get on Panic:**
```
================================================================================
TEST FAILURE REPORT
================================================================================

Error: Test panicked: assertion `left == right` failed
  left: 1
 right: 2

📊 Event Statistics:
  Total events: 5
  By type: Connect: 2, ...

📁 Detailed Reports Generated:
  📄 Full event log:     file:///tmp/freenet-test-.../events.md
  📊 Event flow diagram: file:///tmp/freenet-test-.../event-flow.mmd
```

**Best Practices:**
- Both `assert!` and `ensure!` work fine now
- Use `assert!` for simple equality checks
- Use `ensure!` for more descriptive error messages
- Both generate full event reports with timelines and diagrams

### Test Times Out During Startup

**Problem:** Test fails with timeout before your test logic runs

**Solutions:**
1. Increase `startup_wait_secs`:
   ```rust
   #[freenet_test(nodes = ["gateway", "peer"], startup_wait_secs = 15)]
   ```
2. Reduce number of nodes
3. Check for port conflicts

### No Events in Aggregation Report

**Problem:** Event report shows 0 events

**Possible Causes:**
1. Events not flushed before test completion (wait time too short)
2. Event logging not enabled properly
3. Nodes crashed before events were written

**Solutions:**
1. The macro automatically waits 5 seconds for event flushing - this should be sufficient
2. Check node startup logs for errors
3. Use `aggregate_events = "always"` to see events even on success

### Peer Connections Not Establishing

**Problem:** Peers don't connect to gateway

**Solutions:**
1. Use `auto_connect_peers = true` to handle connection setup automatically
2. Increase `startup_wait_secs` to allow more time for connections
3. Check gateway network port is set correctly

### Multi-Gateway Tests Fail

**Problem:** Tests with multiple gateways don't work as expected

**Check:**
1. Verify `gateways` attribute includes all gateway node labels
2. Ensure gateway labels match those in `nodes` list
3. Increase startup wait time for complex topologies

## Implementation Details

### What the Macro Does

The `#[freenet_test]` macro expands to code that:

1. **Sets up logging** - Initializes `TestLogger` with JSON format
2. **Creates node configs** - Generates temp directories and config for each node
3. **Builds nodes** - Initializes nodes and collects event flush handles
4. **Creates TestContext** - Populates context with node information and flush handles
5. **Starts node tasks** - Spawns tasks to run each node
6. **Runs test with timeout** - Executes your test function with a timeout
7. **Waits for event flushing** - Gives 5 seconds for events to flush to disk
8. **Generates reports** - On failure (or always if configured), aggregates events and generates detailed report
9. **Cleans up** - Handles cleanup even on panic

### Generated Code Structure

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_name() -> TestResult {
    // 1. Setup TestLogger
    let _logger = TestLogger::new().with_json().init();

    // 2. Create node configurations
    let (config_0, temp_0) = create_gateway_config();
    let (config_1, temp_1) = create_peer_config(gateway_info);

    // 3. Build nodes and collect flush handles
    let (node_0, flush_handle_0) = build_node(config_0);
    let (node_1, flush_handle_1) = build_node(config_1);

    // 4. Create TestContext
    let mut ctx = TestContext::with_flush_handles(nodes, flush_handles);

    // 5. Start node tasks
    tokio::spawn(async move { node_0.run().await });
    tokio::spawn(async move { node_1.run().await });

    // 6. Run test with timeout
    let result = tokio::time::timeout(
        Duration::from_secs(timeout_secs),
        async {
            tokio::time::sleep(Duration::from_secs(startup_wait_secs)).await;
            test_name_inner(&mut ctx).await
        }
    ).await;

    // 7. Wait for event flushing
    tokio::time::sleep(Duration::from_secs(5)).await;

    // 8. Generate reports if needed
    if result.is_err() {
        eprintln!("{}", ctx.generate_failure_report().await);
    }

    result
}

async fn test_name_inner(ctx: &mut TestContext) -> TestResult {
    // Your test body here
}
```

## Examples

See `/Volumes/PRO-G40/projects/freenet-core/crates/core/tests/test_macro_example.rs` for comprehensive examples of all macro features.

## See Also

- [TESTING.md](/Volumes/PRO-G40/projects/freenet-core/docs/TESTING.md) - General testing guidelines
- [EVENT_AGGREGATOR.md](/Volumes/PRO-G40/projects/freenet-core/docs/EVENT_AGGREGATOR.md) - Event aggregation details
- [testing-logging-guide.md](/Volumes/PRO-G40/projects/freenet-core/docs/debugging/testing-logging-guide.md) - Logging in tests
- [test_macro_example.rs](/Volumes/PRO-G40/projects/freenet-core/crates/core/tests/test_macro_example.rs) - Usage examples

## License

Same as Freenet Core (MIT + Apache 2.0)
