# Test-Log Analysis and Custom Test Helper Design

## Executive Summary

This document analyzes how `test-log` works internally and proposes a custom test helper that's more configurable for Freenet's needs. The analysis reveals that test-log's limitations stem from its reliance on third-party crates (env_logger/tracing-subscriber) and lack of customization for features like JSON output, per-peer filtering, and custom log capture.

## How test-log Works

### Architecture Overview

test-log is a **procedural macro** that transforms test functions by injecting initialization code before the test runs. It's surprisingly simple:

```rust
// What you write:
#[test_log::test(tokio::test)]
async fn my_test() {
    tracing::info!("test log");
    assert!(true);
}

// What gets generated:
#[tokio::test]
async fn my_test() {
    mod init {
        pub fn init() {
            // Logging/tracing initialization code here
            let _ = ::test_log::tracing_subscriber::FmtSubscriber::builder()
                .with_env_filter(env_filter)
                .with_span_events(span_events)
                .with_test_writer()  // ← KEY: This connects to Rust's test harness
                .try_init();
        }
    }

    init::init();

    // Original test body
    tracing::info!("test log");
    assert!(true);
}
```

### Key Mechanisms

#### 1. Log Capture (Show Only on Failure)

test-log **doesn't implement** log capture itself. It delegates to:

- **For env_logger**: `.is_test(true)` - tells env_logger to write to Rust's test stdout
- **For tracing-subscriber**: `.with_test_writer()` - does the same thing

**How Rust's test harness works**:
- When a test runs, `cargo test` captures all stdout/stderr
- If test passes → output is discarded
- If test fails → output is displayed in the failure section

The logging libraries write to this captured stdout using special test-aware writers:
- `env_logger::Builder::is_test(true)` → uses `std::io::set_output_capture()`
- `tracing_subscriber::fmt::writer::TestWriter` → uses `std::io::set_output_capture()`

#### 2. Configuration

test-log supports minimal configuration:
- `default_log_filter`: Sets the default log level (e.g., "debug", "info")
- `RUST_LOG`: Environment variable for filtering
- `RUST_LOG_SPAN_EVENTS`: For tracing span lifecycle events

**What it doesn't support**:
- JSON formatting (hardcoded to pretty format)
- Custom formatters
- Per-test customization
- Custom writers or output destinations
- Programmatic filtering

#### 3. Code Structure

```
test-log/
├── macros/src/lib.rs         # Procedural macro implementation
│   ├── test()                # Main macro entry point
│   ├── expand_logging_init() # Generates env_logger setup code
│   └── expand_tracing_init() # Generates tracing-subscriber setup code
└── src/lib.rs                # Re-exports the macro
```

The macro generates code that calls:
- `env_logger::builder().is_test(true).try_init()` for log crate
- `tracing_subscriber::FmtSubscriber::builder().with_test_writer().try_init()` for tracing

## Limitations of test-log

### 1. **No JSON Output Support**
- Hardcoded to use pretty formatting
- Can't customize the formatter
- Line 274-277 in macros/src/lib.rs shows it just uses default `FmtSubscriber::builder()`

### 2. **No Per-Test Configuration**
- All tests use the same log level and format
- Can't have different settings for different tests
- Attribute args are limited to just `default_log_filter`

### 3. **No Custom Writers**
- Locked into using `with_test_writer()` which writes to test stdout
- Can't capture logs to a buffer for inspection
- Can't send logs to multiple destinations

### 4. **No Programmatic Access**
- Can't query logs from within tests
- Can't assert on log content
- Can't filter logs programmatically

### 5. **Global Subscriber**
- Uses `try_init()` which sets a global subscriber
- First test to run wins
- Can cause issues with tests that need different configurations

### 6. **Limited Span Support**
- Only supports RUST_LOG_SPAN_EVENTS environment variable
- Can't programmatically configure span events per-test
- No custom span formatting

## Custom Test Helper Design

### Goals

1. **JSON Output Support**: Enable JSON logging in tests
2. **Per-Test Configuration**: Different settings per test
3. **Peer Identification**: Enhanced support for multi-peer tests
4. **Log Inspection**: Ability to programmatically query logs
5. **Flexible Formatting**: Support multiple output formats
6. **Better Error Context**: Capture and display more context on failure

### Proposed Architecture

#### Option 1: Enhanced Macro (Similar to test-log)

Create our own procedural macro that generates more sophisticated initialization code:

```rust
#[freenet_test::test(
    format = "json",           // "json" or "pretty"
    level = "debug",           // log level
    peer_id = "gateway",       // automatic peer identification
    capture_logs = true,       // store logs for inspection
)]
async fn my_test() -> anyhow::Result<()> {
    // Test code
}
```

**Advantages**:
- Clean API similar to test-log
- Can generate complex initialization code
- Type-safe configuration

**Disadvantages**:
- Requires maintaining a proc-macro crate
- More complex to implement
- Harder to debug macro expansion issues

#### Option 2: Function-Based Helper (Recommended)

Create a function-based test helper that manually manages subscribers:

```rust
use freenet::test_utils::TestLogger;

#[tokio::test]
async fn my_test() -> anyhow::Result<()> {
    let _logger = TestLogger::new()
        .with_json()                    // Enable JSON output
        .with_peer_id("gateway")        // Set peer ID
        .with_level("debug")            // Set log level
        .capture_logs()                 // Store logs for inspection
        .init();                        // Initialize (returns guard)

    // Test code
    tracing::info!("this will be in JSON format");

    // Can inspect logs
    assert!(_logger.contains("expected message"));

    Ok(())
}
```

**Advantages**:
- No proc-macro complexity
- Easy to understand and maintain
- Flexible and composable
- Can return values (log handles, guards, etc.)
- Easy to debug

**Disadvantages**:
- Requires manual invocation in each test
- Slightly more verbose than macro approach

#### Option 3: Hybrid Approach

Combine both approaches - use a simple macro that calls function-based helpers:

```rust
#[freenet_test::test]
async fn my_test() -> anyhow::Result<()> {
    // Macro just does basic setup
    // Can still use helpers for custom configuration
    let _logger = TestLogger::current()
        .with_json()
        .with_peer_id("gateway");

    // Test code
}
```

### Implementation Strategy (Option 2 - Recommended)

#### Core Components

##### 1. `TestLogger` Struct

```rust
pub struct TestLogger {
    format: Format,
    level: LevelFilter,
    peer_id: Option<String>,
    capture: bool,
    writer: Box<dyn Write + Send>,
    guard: Option<tracing::subscriber::DefaultGuard>,
    captured_logs: Arc<Mutex<Vec<String>>>,
}

enum Format {
    Pretty,
    Json,
}
```

##### 2. Builder Pattern

```rust
impl TestLogger {
    pub fn new() -> Self {
        Self {
            format: Format::Pretty,
            level: LevelFilter::INFO,
            peer_id: None,
            capture: false,
            writer: Box::new(std::io::stderr()),
            guard: None,
            captured_logs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn with_json(mut self) -> Self {
        self.format = Format::Json;
        self
    }

    pub fn with_level(mut self, level: impl Into<LevelFilter>) -> Self {
        self.level = level.into();
        self
    }

    pub fn with_peer_id(mut self, peer_id: impl Into<String>) -> Self {
        self.peer_id = Some(peer_id.into());
        self
    }

    pub fn capture_logs(mut self) -> Self {
        self.capture = true;
        self
    }
}
```

##### 3. Initialization with Custom Writer

```rust
impl TestLogger {
    pub fn init(mut self) -> Self {
        // Create appropriate writer
        let writer = if self.capture {
            // Use a capturing writer that stores logs in memory
            Arc::new(CapturingWriter::new(self.captured_logs.clone()))
        } else {
            // Use test writer (writes to test stdout)
            TestWriter::default()
        };

        // Build subscriber based on format
        let subscriber = match self.format {
            Format::Pretty => {
                let layer = tracing_subscriber::fmt::layer()
                    .with_writer(writer)
                    .with_level(true)
                    .pretty();

                tracing_subscriber::registry()
                    .with(self.level)
                    .with(layer)
            }
            Format::Json => {
                let layer = tracing_subscriber::fmt::layer()
                    .with_writer(writer)
                    .with_level(true)
                    .json();

                tracing_subscriber::registry()
                    .with(self.level)
                    .with(layer)
            }
        };

        // Add peer ID span if provided
        let subscriber = if let Some(peer_id) = &self.peer_id {
            // Create a span that will be active for the duration
            let span = tracing::info_span!("test_peer", test_node = %peer_id);
            span.enter();
        };

        // Set as default subscriber (returns guard)
        self.guard = Some(tracing::subscriber::set_default(subscriber));

        self
    }
}
```

##### 4. Log Inspection API

```rust
impl TestLogger {
    /// Check if logs contain a specific message
    pub fn contains(&self, message: &str) -> bool {
        if !self.capture {
            panic!("Cannot inspect logs without calling .capture_logs()");
        }

        self.captured_logs
            .lock()
            .unwrap()
            .iter()
            .any(|log| log.contains(message))
    }

    /// Get all captured logs
    pub fn logs(&self) -> Vec<String> {
        if !self.capture {
            panic!("Cannot get logs without calling .capture_logs()");
        }

        self.captured_logs.lock().unwrap().clone()
    }

    /// Get logs matching a filter
    pub fn logs_matching(&self, filter: impl Fn(&str) -> bool) -> Vec<String> {
        self.logs().into_iter().filter(|log| filter(log)).collect()
    }
}
```

##### 5. CapturingWriter Implementation

```rust
struct CapturingWriter {
    buffer: Arc<Mutex<Vec<String>>>,
    line_buffer: Vec<u8>,
}

impl Write for CapturingWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // Also write to test stdout so it shows on failure
        std::io::set_output_capture(Some(Default::default()));

        // Buffer lines
        self.line_buffer.extend_from_slice(buf);

        // When we have complete lines, store them
        while let Some(pos) = self.line_buffer.iter().position(|&b| b == b'\n') {
            let line = String::from_utf8_lossy(&self.line_buffer[..pos]).to_string();
            self.buffer.lock().unwrap().push(line);
            self.line_buffer.drain(..=pos);
        }

        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // Flush any remaining data
        if !self.line_buffer.is_empty() {
            let line = String::from_utf8_lossy(&self.line_buffer).to_string();
            self.buffer.lock().unwrap().push(line);
            self.line_buffer.clear();
        }
        Ok(())
    }
}
```

### Usage Examples

#### Basic Usage with JSON

```rust
#[tokio::test]
async fn test_with_json_logging() -> anyhow::Result<()> {
    let _logger = TestLogger::new()
        .with_json()
        .with_peer_id("gateway")
        .init();

    tracing::info!("Starting test");
    // ... test code

    Ok(())
}
```

#### Multi-Peer Test with Different Loggers

```rust
#[tokio::test]
async fn test_multi_peer() -> anyhow::Result<()> {
    let gateway = async {
        let _logger = TestLogger::new()
            .with_json()
            .with_peer_id("gateway")
            .with_level("debug")
            .init();

        tracing::info!("Gateway starting");
        // ... gateway code
    };

    let peer = async {
        let _logger = TestLogger::new()
            .with_json()
            .with_peer_id("peer-1")
            .with_level("info")
            .init();

        tracing::info!("Peer starting");
        // ... peer code
    };

    tokio::try_join!(gateway, peer)?;
    Ok(())
}
```

#### Log Inspection

```rust
#[tokio::test]
async fn test_with_log_inspection() -> anyhow::Result<()> {
    let logger = TestLogger::new()
        .with_peer_id("test-node")
        .capture_logs()
        .init();

    tracing::info!("Critical operation starting");
    // ... test code that should log something
    tracing::error!("Something went wrong");

    // Assert on log content
    assert!(logger.contains("Critical operation"));
    assert!(logger.contains("Something went wrong"));

    // Get specific logs
    let error_logs = logger.logs_matching(|log| log.contains("ERROR"));
    assert_eq!(error_logs.len(), 1);

    Ok(())
}
```

#### Simple Macro Wrapper (Optional)

For convenience, we could add a simple macro:

```rust
macro_rules! test_with_logging {
    ($test_fn:ident, $($config:tt)*) => {
        #[tokio::test]
        async fn $test_fn() -> anyhow::Result<()> {
            let _logger = TestLogger::new()
                $($config)*
                .init();

            $test_fn_impl().await
        }

        async fn $test_fn_impl() -> anyhow::Result<()> {
            // ... test body
        }
    };
}

// Usage:
test_with_logging!(my_test,
    .with_json()
    .with_peer_id("gateway")
);
```

## Comparison: test-log vs Custom Solution

| Feature | test-log | Custom Solution |
|---------|----------|-----------------|
| JSON Output | ❌ No | ✅ Yes |
| Pretty Output | ✅ Yes | ✅ Yes |
| Per-Test Config | ❌ Limited | ✅ Full |
| Log Capture | ❌ No | ✅ Yes |
| Log Inspection | ❌ No | ✅ Yes |
| Peer ID Support | ⚠️ Manual | ✅ Built-in |
| Custom Writers | ❌ No | ✅ Yes |
| Ease of Use | ✅ Simple | ⚠️ Slightly verbose |
| Maintenance | ✅ External | ⚠️ Internal |
| Flexibility | ❌ Limited | ✅ High |
| Show on Failure | ✅ Yes | ✅ Yes |

## Implementation Plan

### Phase 1: Core Implementation (1-2 days)
1. Create `TestLogger` struct with builder pattern
2. Implement basic initialization with pretty/JSON formats
3. Add peer_id support
4. Integrate with Rust's test writer for "show on failure"

### Phase 2: Advanced Features (1-2 days)
5. Implement log capturing
6. Add log inspection API
7. Create custom writer that both captures and shows on failure
8. Add thread-local storage for current test logger

### Phase 3: Integration (1 day)
9. Update existing tests to use new TestLogger
10. Add examples and documentation
11. Deprecate old test-log usage

### Phase 4: Polish (1 day)
12. Add convenience macros if needed
13. Performance testing
14. Documentation and migration guide

## Recommendations

### Primary Recommendation: Function-Based Helper (Option 2)

Implement the function-based `TestLogger` helper for these reasons:

1. **Immediate Value**: Can implement and use immediately without proc-macro complexity
2. **Flexibility**: Easy to extend with new features
3. **Debuggability**: Easy to step through and understand
4. **Composability**: Can combine with existing macros like `#[tokio::test]`
5. **JSON Support**: Can easily enable JSON output
6. **Log Inspection**: Can programmatically verify log content

### When to Consider Macro Approach (Option 1)

Consider implementing a proc-macro if:
1. The function-based approach becomes too verbose in practice
2. We need compile-time guarantees about test configuration
3. We want to enforce certain patterns across all tests
4. The team prefers attribute-style configuration

### Migration Path

1. **Phase 1**: Implement `TestLogger` alongside existing test-log usage
2. **Phase 2**: Gradually migrate tests that need JSON or special features
3. **Phase 3**: Evaluate if all tests should migrate or if hybrid is acceptable
4. **Phase 4**: Decide if test-log can be removed entirely or kept for simple tests

## Technical Considerations

### Thread Safety
- Use `Arc<Mutex<>>` for shared state
- Consider `thread_local!` for per-thread loggers
- Be careful with global subscriber registration

### Test Isolation
- Each test should create its own logger
- Use guards to ensure cleanup
- Consider using `set_default` instead of `set_global_default`

### Performance
- JSON formatting is slightly slower than pretty
- Log capturing adds memory overhead
- Consider lazy initialization of expensive components

### Compatibility
- Must work with `#[tokio::test]`
- Should work with `#[test]`
- Consider interaction with test-log if used in same crate

## Conclusion

test-log is a simple, well-designed crate that solves the basic problem of "initialize logging in tests" but lacks the flexibility needed for Freenet's requirements. By implementing a custom `TestLogger` helper, we can:

1. ✅ Enable JSON output in tests
2. ✅ Support per-test configuration
3. ✅ Add log inspection capabilities
4. ✅ Enhance peer identification
5. ✅ Maintain "show only on failure" behavior

The function-based approach (Option 2) is recommended for its simplicity, flexibility, and ease of maintenance.
