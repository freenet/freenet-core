# Testing and Logging Guide

This guide explains how to use the improved logging capabilities in Freenet Core tests.

## Quick Start

### Running Tests with Logs Only for Failures

By default, tests now use the `test-log` crate to only show logs when tests fail:

```rust
// Old way (deprecated) - shows logs for all tests
#[tokio::test]
async fn my_test() -> TestResult {
    freenet::config::set_logger(Some(LevelFilter::INFO), None);
    // ... test code
}

// New way - only shows logs for failing tests
#[test_log::test(tokio::test)]
async fn my_test() -> TestResult {
    // No need to call set_logger()
    // ... test code
}
```

### Distinguishing Peer Logs in Multi-Peer Tests

Use the `with_peer_id()` function to add peer identification to all logs within a scope:

```rust
use freenet::test_utils::with_peer_id;

#[test_log::test(tokio::test)]
async fn test_multi_peer_network() -> TestResult {
    // Start gateway with peer identification
    let gateway_task = tokio::spawn(async {
        let _span = with_peer_id("gateway");
        tracing::info!("Starting gateway");  // Logs will show peer_id="gateway"
        // ... gateway initialization
    });

    // Start peer 1
    let peer1_task = tokio::spawn(async {
        let _span = with_peer_id("peer-1");
        tracing::info!("Starting peer");  // Logs will show peer_id="peer-1"
        // ... peer initialization
    });

    // Start peer 2
    let peer2_task = tokio::spawn(async {
        let _span = with_peer_id("peer-2");
        tracing::info!("Starting peer");  // Logs will show peer_id="peer-2"
        // ... peer initialization
    });

    // ... test logic
}
```

## Detailed Usage

### 1. Using `test-log` for Test Logging

The `test-log` crate provides better integration with the test framework:

**Benefits:**
- Only shows logs when tests fail (cleaner test output)
- Respects `RUST_LOG` environment variable
- Works with both synchronous and asynchronous tests
- Automatically captures and formats logs

**Usage Examples:**

```rust
// Standard tokio test
#[test_log::test(tokio::test)]
async fn simple_async_test() -> TestResult {
    tracing::info!("This log only appears if the test fails");
    Ok(())
}

// Multi-threaded tokio test
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn complex_async_test() -> TestResult {
    tracing::debug!("Debug logs also work");
    Ok(())
}

// Synchronous test
#[test_log::test]
fn simple_sync_test() -> TestResult {
    tracing::info!("Works for sync tests too");
    Ok(())
}
```

**Controlling Log Levels:**

```bash
# Run tests with default log level (info)
cargo test

# Run tests with debug logs
RUST_LOG=debug cargo test

# Run tests with trace logs for specific modules
RUST_LOG=freenet::operations=trace cargo test

# Run tests with different levels per module
RUST_LOG=info,freenet::node=debug,freenet::operations::get=trace cargo test
```

### 2. Peer Identification in Logs

When running multi-peer tests, it's critical to distinguish which logs come from which peer.

**Important:** The test helper uses the field name `test_node` to avoid conflicts with the production code's `peer` field (which contains the actual cryptographic PeerId).

#### Method 1: Using `with_peer_id()` (Recommended)

```rust
use freenet::test_utils::with_peer_id;

async fn start_gateway(config: Config) -> Result<()> {
    let _span = with_peer_id("gateway");
    tracing::info!("Initializing gateway");
    tracing::debug!("Gateway config: {:?}", config);
    // All logs in this scope will have test_node="gateway"
    Ok(())
}

async fn start_peer(id: usize, config: Config) -> Result<()> {
    let _span = with_peer_id(format!("peer-{}", id));
    tracing::info!("Initializing peer");
    tracing::debug!("Peer config: {:?}", config);
    // All logs in this scope will have test_node="peer-N"
    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_network() -> TestResult {
    let gateway = tokio::spawn(start_gateway(gateway_config));
    let peer1 = tokio::spawn(start_peer(1, peer1_config));
    let peer2 = tokio::spawn(start_peer(2, peer2_config));

    // Wait for initialization
    gateway.await??;
    peer1.await??;
    peer2.await??;

    Ok(())
}
```

**Understanding the log fields:**
- `test_node="gateway"` - Human-readable test label (from `with_peer_id()`)
- `peer=PeerId(...)` - Actual cryptographic peer ID (from production code)

These fields complement each other in logs - you'll often see both!

#### Method 2: Manual Logging with Test Node Label

For more granular control, include the test node label in individual log statements:

```rust
let test_node = "gateway";
tracing::info!(test_node = %test_node, "Starting node");
tracing::debug!(test_node = %test_node, "Connecting to peers");
```

**Note:** Don't use `peer_id` or `peer` in test code manually - these conflict with production logging fields.

### 3. JSON Logging for Structured Output

For better parsing and analysis, especially in CI/CD, use JSON logging:

```bash
# Run tests with JSON formatted logs
FREENET_LOG_FORMAT=json cargo test

# Combine with specific log levels
RUST_LOG=debug FREENET_LOG_FORMAT=json cargo test
```

**JSON Log Format:**

```json
{
  "timestamp": "2025-10-25T12:34:56.789Z",
  "level": "INFO",
  "target": "freenet::node",
  "fields": {
    "message": "Starting gateway",
    "test_node": "gateway",
    "peer": "PeerId(abc123...)"
  },
  "file": "src/node/mod.rs",
  "line": 123
}
```

**Benefits of JSON Logging:**
- Easy parsing in CI/CD pipelines
- Better integration with log aggregation tools (ELK, Loki)
- Structured data for filtering and analysis
- Preserves field types (numbers, booleans, etc.)

### 4. Environment Variables for Test Logging

| Variable | Purpose | Example |
|----------|---------|---------|
| `RUST_LOG` | Control log levels per module | `debug`, `freenet::node=trace` |
| `FREENET_LOG_FORMAT` | Use JSON logging | `json` |
| `FREENET_LOG_TO_STDERR` | Redirect logs to stderr | `1` or `true` |
| `FREENET_DISABLE_LOGS` | Disable all logging | `1` or `true` |

**Combined Usage:**

```bash
# Debug logs in JSON format to stderr
RUST_LOG=debug FREENET_LOG_FORMAT=json FREENET_LOG_TO_STDERR=1 cargo test

# Trace specific modules with JSON
RUST_LOG=freenet::operations::get=trace FREENET_LOG_FORMAT=json cargo test

# Run specific test with detailed logging
RUST_LOG=trace cargo test test_gateway_reconnection
```

## Best Practices

### 1. Use `test-log::test` for All New Tests

**Do:**
```rust
#[test_log::test(tokio::test)]
async fn my_test() -> TestResult {
    tracing::info!("Test started");
    Ok(())
}
```

**Don't:**
```rust
#[tokio::test]
async fn my_test() -> TestResult {
    freenet::config::set_logger(Some(LevelFilter::INFO), None);  // Deprecated
    Ok(())
}
```

### 2. Always Use Peer Identification in Multi-Peer Tests

**Do:**
```rust
async fn start_gateway() -> Result<()> {
    let _span = with_peer_id("gateway");
    tracing::info!("Starting");  // Clear which peer this is
    Ok(())
}
```

**Don't:**
```rust
async fn start_gateway() -> Result<()> {
    tracing::info!("Starting");  // Unclear which peer this log is from
    Ok(())
}
```

### 3. Use Appropriate Log Levels

| Level | Usage | Example |
|-------|-------|---------|
| `error!` | Unexpected failures | `tracing::error!("Failed to connect: {}", err)` |
| `warn!` | Potential issues | `tracing::warn!("Retrying connection attempt {}", n)` |
| `info!` | Important milestones | `tracing::info!("Node started successfully")` |
| `debug!` | Detailed state info | `tracing::debug!("Contract state: {:?}", state)` |
| `trace!` | Very verbose details | `tracing::trace!("Processing message: {:?}", msg)` |

### 4. Include Contextual Fields in Logs

**Good:**
```rust
tracing::info!(
    peer_id = %peer_id,
    contract_key = %key,
    "PUT operation completed"
);
```

**Better:**
```rust
tracing::info!(
    peer_id = %peer_id,
    contract_key = %key,
    duration_ms = elapsed.as_millis(),
    state_size = state.len(),
    "PUT operation completed"
);
```

### 5. Use Spans for Operation Tracking

```rust
async fn process_contract(key: ContractKey) -> Result<()> {
    let span = tracing::info_span!("process_contract", contract_key = %key);
    let _guard = span.enter();

    tracing::debug!("Loading contract");
    // ... load contract

    tracing::debug!("Validating state");
    // ... validate

    tracing::info!("Contract processed successfully");
    Ok(())
}
```

## Common Patterns

### Pattern 1: Multi-Peer Network Test

```rust
use freenet::test_utils::with_peer_id;

#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn test_multi_peer_put_operation() -> TestResult {
    // Gateway setup
    let gateway_task = tokio::spawn(async move {
        let _span = with_peer_id("gateway");
        tracing::info!("Starting gateway");

        let config = create_gateway_config()?;
        let node = start_node(config).await?;

        tracing::info!("Gateway ready");
        Ok(node)
    });

    // Peer setup
    let mut peer_tasks = vec![];
    for i in 1..=3 {
        let task = tokio::spawn(async move {
            let _span = with_peer_id(format!("peer-{}", i));
            tracing::info!("Starting peer");

            let config = create_peer_config(i)?;
            let node = start_node(config).await?;

            tracing::info!("Peer ready");
            Ok(node)
        });
        peer_tasks.push(task);
    }

    // Wait for all nodes
    let gateway = gateway_task.await??;
    for task in peer_tasks {
        task.await??;
    }

    tracing::info!("All nodes started, running test operations");

    // Test operations...

    Ok(())
}
```

### Pattern 2: Testing with JSON Logs in CI

```yaml
# .github/workflows/ci.yml
- name: Run tests with JSON logging
  env:
    RUST_LOG: debug
    FREENET_LOG_FORMAT: json
  run: cargo test --workspace 2>&1 | tee test-output.json

- name: Parse test logs
  run: |
    jq 'select(.level == "ERROR")' test-output.json > errors.json
    if [ -s errors.json ]; then
      echo "Errors found in test run:"
      cat errors.json
      exit 1
    fi
```

### Pattern 3: Debugging Specific Operations

```bash
# Debug only GET operations
RUST_LOG=freenet::operations::get=trace cargo test test_get_operation

# Debug all operations
RUST_LOG=freenet::operations=debug cargo test

# Trace specific test with JSON output
RUST_LOG=trace FREENET_LOG_FORMAT=json cargo test test_gateway_reconnection > test.log
```

## Troubleshooting

### Issue: Logs are always showing (even for passing tests)

**Solution:** Ensure you're using `#[test_log::test(...)]` instead of just `#[tokio::test]`:

```rust
// Wrong - logs always show
#[tokio::test]
async fn my_test() -> TestResult { ... }

// Correct - logs only for failures
#[test_log::test(tokio::test)]
async fn my_test() -> TestResult { ... }
```

### Issue: Can't distinguish logs from different peers

**Solution:** Use `with_peer_id()` at the start of each peer's execution:

```rust
async fn start_peer(id: usize) -> Result<()> {
    let _span = with_peer_id(format!("peer-{}", id));
    // Now all logs will include peer_id
    tracing::info!("Starting");
    Ok(())
}
```

### Issue: Logs are too verbose

**Solution:** Use more restrictive log levels:

```bash
# Only show warnings and errors
RUST_LOG=warn cargo test

# Only show info and above
RUST_LOG=info cargo test
```

### Issue: Need to see logs for a passing test during development

**Solution:** Use `--nocapture` and set `RUST_LOG`:

```bash
# See all logs during test run
RUST_LOG=debug cargo test my_test -- --nocapture

# Or force the test to fail temporarily to see logs
```

## Migration Guide

### Migrating Existing Tests

**Step 1:** Replace test attribute

```diff
- #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
+ #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
  async fn my_test() -> TestResult {
```

**Step 2:** Remove manual logging setup

```diff
  #[test_log::test(tokio::test)]
  async fn my_test() -> TestResult {
-     freenet::config::set_logger(Some(LevelFilter::INFO), None);
-
      // test code...
```

**Step 3:** Add peer identification for multi-peer tests

```diff
+ use freenet::test_utils::with_peer_id;
+
  #[test_log::test(tokio::test)]
  async fn my_test() -> TestResult {
      let gateway_task = tokio::spawn(async {
+         let _span = with_peer_id("gateway");
          // gateway code...
      });

      let peer_task = tokio::spawn(async {
+         let _span = with_peer_id("peer-1");
          // peer code...
      });
```

## Examples

### Example 1: Simple Test with Logs

```rust
#[test_log::test(tokio::test)]
async fn test_contract_put() -> TestResult {
    tracing::info!("Loading test contract");
    let contract = load_contract("test-contract", vec![].into())?;

    tracing::debug!(key = %contract.key(), "Contract loaded");

    // If this fails, all the above logs will be shown
    assert_eq!(contract.key().encoded_code_hash().is_some(), true);

    Ok(())
}
```

**Output on failure:**
```
running 1 test
test test_contract_put ... FAILED

---- test_contract_put stdout ----
2025-10-25T12:34:56.789Z INFO  test_contract_put: Loading test contract
2025-10-25T12:34:56.790Z DEBUG test_contract_put: Contract loaded key=abc123...

failures:
    test_contract_put
```

### Example 2: Multi-Peer Test with Identification

```rust
use freenet::test_utils::with_peer_id;

#[test_log::test(tokio::test)]
async fn test_peer_communication() -> TestResult {
    let gateway = tokio::spawn(async {
        let _span = with_peer_id("gateway");
        tracing::info!("Gateway starting");
        // ... initialization
        tracing::info!("Gateway ready");
        Ok::<_, anyhow::Error>(())
    });

    let peer = tokio::spawn(async {
        let _span = with_peer_id("peer-1");
        tracing::info!("Peer starting");
        // ... initialization
        tracing::info!("Peer ready");
        Ok::<_, anyhow::Error>(())
    });

    gateway.await??;
    peer.await??;

    Ok(())
}
```

**Output on failure (shows both peers clearly):**
```
2025-10-25T12:34:56.789Z INFO  peer{peer_id=gateway}: Gateway starting
2025-10-25T12:34:56.790Z INFO  peer{peer_id=peer-1}: Peer starting
2025-10-25T12:34:56.891Z INFO  peer{peer_id=gateway}: Gateway ready
2025-10-25T12:34:56.892Z ERROR peer{peer_id=peer-1}: Connection failed
```

### Example 3: JSON Logging in CI

```bash
#!/bin/bash
# run-tests-ci.sh

# Enable JSON logging for CI
export FREENET_LOG_FORMAT=json
export RUST_LOG=debug

# Run tests and capture output
cargo test --workspace 2>&1 | tee test-output.jsonl

# Extract errors
jq -s 'map(select(.level == "ERROR"))' test-output.jsonl > errors.json

# Check if any errors occurred
if [ -s errors.json ] && [ "$(cat errors.json)" != "[]" ]; then
    echo "❌ Errors found in test run:"
    jq '.' errors.json
    exit 1
else
    echo "✅ All tests passed without errors"
fi
```

## References

- [test-log crate documentation](https://docs.rs/test-log/)
- [tracing crate documentation](https://docs.rs/tracing/)
- [TESTING.md](../TESTING.md) - General testing guidelines
- [debugging-and-tracing-analysis.md](debugging-and-tracing-analysis.md) - Comprehensive analysis

## Contributing

When adding new tests:
1. Always use `#[test_log::test(...)]`
2. Add peer identification for multi-peer tests
3. Use appropriate log levels
4. Include contextual fields in important logs
5. Document any special logging requirements

---

**Last Updated:** October 25, 2025
**Version:** 1.0
