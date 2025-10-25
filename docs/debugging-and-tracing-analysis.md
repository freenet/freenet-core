# Debugging and Tracing Analysis for Freenet Core

**Date:** October 25, 2025
**Branch:** claude/improve-debugging-tracing-011CUTwcrjFqfCbsTzxDYdwo
**Purpose:** Comprehensive analysis of debugging, tracing, and testing capabilities in Freenet Core

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Testing Infrastructure](#testing-infrastructure)
3. [Tracing and Logging Infrastructure](#tracing-and-logging-infrastructure)
4. [API Debugging Capabilities](#api-debugging-capabilities)
5. [Current State Assessment](#current-state-assessment)
6. [Recommendations for Improvement](#recommendations-for-improvement)
7. [Implementation Priorities](#implementation-priorities)

---

## Executive Summary

Freenet Core has a **mature and comprehensive debugging infrastructure** with:

- **161 test attributes** across the codebase with specialized integration tests
- **Structured tracing** via the `tracing` crate with OpenTelemetry support
- **Three API types** providing runtime diagnostics (WebSocket, HTTP, Query APIs)
- **Custom network event recording** system with append-only file logging
- **Environment-based configuration** for flexible debugging control

**Key Strengths:**
- Well-documented testing practices (TESTING.md, UBERTEST.md)
- Transaction-based tracing for request correlation
- Real-time network diagnostics via WebSocket API
- Comprehensive integration tests covering multi-peer scenarios

**Key Gaps:**
- Limited test output formatting and summarization
- No API documentation (OpenAPI/Swagger specs)
- Distributed tracing (OpenTelemetry) currently disabled
- No unified metrics dashboard
- Inconsistent error context across the codebase

---

## Testing Infrastructure

### Overview

**Test Frameworks:**
- **Rust**: Tokio async tests, standard Rust tests, `testresult` crate
- **JavaScript/TypeScript**: Jest v28+ with ts-jest
- **Test Count**: 161 test attributes (73 tokio async + 88 standard in core)

### Test Output Information

When running `cargo test`, developers receive:

| Information Type | Details | Example |
|-----------------|---------|---------|
| **Async traces** | File/line numbers for async operations | `freenet::node:123 - Request processing` |
| **Timeouts** | Long-running test indicators | Test timeout after 120s |
| **WebSocket logs** | Connection/disconnection events | `WS connected to peer1:7509` |
| **Contract ops** | PUT, GET, UPDATE, SUBSCRIBE results | `PUT contract abc123 → Success` |
| **Network topology** | Peer connections and routing | `Gateway ← peer1, peer2, peer3` |
| **Token lifecycle** | Generation, expiration, cleanup | `Token xyz expires in 300s` |
| **Error notifications** | Operation failures and causes | `Get failed: Contract not found` |

### Key Test Files

**Integration Tests** (`crates/core/tests/`):

1. **`connectivity.rs`** (32KB)
   - Network formation and reconnection scenarios
   - Gateway-peer mesh topology tests
   - Location: `crates/core/tests/connectivity.rs`

2. **`operations.rs`** (110KB)
   - Comprehensive contract operation tests
   - Multi-peer PUT/GET/UPDATE/SUBSCRIBE workflows
   - State consistency validation
   - Location: `crates/core/tests/operations.rs`

3. **`ubertest.rs`** (25KB)
   - Real-world application testing with River (decentralized chat)
   - Configurable peer count (default: 10, env: `UBERTEST_PEER_COUNT`)
   - 5-10 minute runtime for comprehensive validation
   - Requires `riverctl` CLI tool
   - Location: `crates/core/tests/ubertest.rs`

4. **`error_notification.rs`** (24KB)
   - Regression test for issue #1858
   - Validates error delivery to clients
   - Location: `crates/core/tests/error_notification.rs`

5. **`isolated_node_regression.rs`** (30KB)
   - Single-node operation tests
   - Validates local caching without peer network
   - Location: `crates/core/tests/isolated_node_regression.rs`

6. **`token_expiration.rs`** (10KB)
   - WebSocket token lifecycle tests
   - Configurable TTL and cleanup intervals
   - Location: `crates/core/tests/token_expiration.rs`

7. **`redb_migration.rs`** (3.8KB)
   - Database schema migration tests
   - Location: `crates/core/tests/redb_migration.rs`

**Test Utilities** (`crates/core/src/test_utils.rs`):

```rust
// Contract operations
pub async fn make_put(client, state, contract, subscribe) -> Result<()>
pub async fn make_get(client, key, return_contract_code, subscribe) -> Result<()>
pub async fn make_update(client, key, state) -> Result<()>
pub async fn make_subscribe(client, key) -> Result<()>

// Contract loading
pub fn load_contract(name: &str, params) -> Result<ContractContainer>
pub fn load_delegate(name: &str, params) -> Result<DelegateContainer>

// Tracing setup
pub fn with_tracing<T>(f: impl FnOnce() -> T) -> T
```

**Common Test Pattern:**

```rust
use tempfile::tempdir;
use rand::SeedableRng;

// Deterministic random seed
static RNG: LazyLock<Mutex<StdRng>> = LazyLock::new(|| {
    Mutex::new(StdRng::from_seed(*b"consistent_seed_for_tests_12345"))
});

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_operation() -> TestResult {
    // Temporary directory (auto-cleanup)
    let temp_dir = tempfile::tempdir()?;

    // Random port allocation
    let socket = TcpListener::bind("127.0.0.1:0")?;
    let port = socket.local_addr()?.port();

    // Test logic...
    Ok(())
}
```

### Testing Best Practices (from TESTING.md)

1. **TDD for Critical Paths**: Network operations require test-first development
2. **Test Deletion Policy**: Requires 2-developer approval
3. **Pre-Release Checklist**: All integration tests must pass
4. **Deterministic Tests**: Fixed random seeds for reproducibility
5. **Isolated Tests**: Temporary directories, random ports

### CI/CD Configuration

**GitHub Actions** (`.github/workflows/ci.yml`):

```yaml
test_all:
  runs-on: freenet-default-runner
  steps:
    - cargo build --locked
    - cargo test --workspace --no-default-features --features trace,websocket,redb

ubertest:
  needs: test_all
  if: false  # Currently disabled for CI stability
  env:
    UBERTEST_PEER_COUNT: 6  # Reduced from default 10 for CI
  steps:
    - cargo install riverctl
    - cargo test --test ubertest --features trace,websocket,redb
```

---

## Tracing and Logging Infrastructure

### Overview

**Libraries Used:**
- `tracing` v0.1 - Core structured logging framework
- `tracing-subscriber` v0.3 - Subscribers, layers, filters
- `opentelemetry` v0.31 - Distributed tracing support (optional)
- `opentelemetry-jaeger` v0.22 - Jaeger backend (optional, feature: `trace-ot`)
- `console-subscriber` v0.4.1 - Tokio runtime debugging (optional)
- `test-log` v0.2 - Test log capture (dev-dependency)

**Dependencies Location:**
- Workspace: `Cargo.toml:42-46`
- Core crate: `crates/core/Cargo.toml:15-20`

### Configuration

**Initialization:**
- Function: `set_logger()` at `crates/core/src/config/mod.rs:1064`
- Implementation: `init_tracer()` at `crates/core/src/tracing/mod.rs:1264`

**Default Levels:**
```rust
let level = match level {
    Some(l) => l,
    None => {
        #[cfg(any(debug_assertions, test))]
        { LevelFilter::DEBUG }
        #[cfg(not(any(debug_assertions, test)))]
        { LevelFilter::INFO }
    }
};
```

**Custom Directives:**
```rust
EnvFilter::builder()
    .with_default_directive(level.into())
    .parse_lossy("stretto=off,sqlx=error")  // Silence noisy libs
```

**Format Configuration:**
```rust
tracing_subscriber::fmt::layer()
    .with_level(true)                    // Show level (INFO, DEBUG, etc.)
    .pretty()                            // Human-readable format
    .with_file(cfg!(any(debug_assertions, test)))  // File names (debug only)
    .with_line_number(cfg!(any(debug_assertions, test)))  // Line numbers (debug only)
    .with_writer(writer)                 // stdout or stderr
```

### Trace Levels

| Level | Usage | Visibility |
|-------|-------|-----------|
| **TRACE** | Extremely detailed diagnostics | Rarely used, per-module opt-in |
| **DEBUG** | Detailed debugging info | Default for test/debug builds |
| **INFO** | General informational messages | Default for release builds |
| **WARN** | Potentially problematic situations | Always visible |
| **ERROR** | Error messages for failures | Always visible |
| **OFF** | Disable all logging | Manual override only |

### Environment Variables

| Variable | Purpose | Example |
|----------|---------|---------|
| `RUST_LOG` | Control levels and per-module directives | `debug,freenet::node=trace` |
| `LOG_LEVEL` | Set default level via CLI | `INFO`, `DEBUG` |
| `FREENET_DISABLE_LOGS` | Disable all logging | `1` |
| `FREENET_LOG_TO_STDERR` | Redirect to stderr | `1` |
| `FREENET_DISABLE_TRACES` | Disable OpenTelemetry tracing | `1` |
| `FREENET_PEER_ID` | Set peer ID for tracing context | `"node-1"` |
| `TOKIO_CONSOLE` | Enable Tokio console subscriber | `1` |
| `FDEV_NETWORK_METRICS_SERVER_PORT` | Metrics visualization server port | `55010` (default) |

### Custom Network Event Tracing

**Trait:** `NetEventRegister` (at `crates/core/src/tracing/mod.rs:49-118`)

**Implementations:**

1. **EventRegister** - Append-Only File (AOF) logger
   - Location: `crates/core/src/tracing/aof.rs`
   - Batch size: 100 events
   - Max records: 100,000 (production), 10,000 (tests)
   - Auto-truncation for disk space management
   - Binary serialization for efficiency

2. **OTEventRegister** - OpenTelemetry integration
   - Feature: `trace-ot`
   - Currently disabled due to version conflicts
   - Intended for distributed tracing visualization

3. **TestEventListener** - In-memory for tests
   - Stores events in `Vec<NetEvent>`
   - Used for test assertions

**Event Types Recorded:**
- Connection events (Connected, Disconnected)
- Contract operations (PutRequest, GetRequest, UpdateRequest)
- Subscription events (SubscribeRequest, SubscriptionNotification)
- Network routing (RelayedRequest, PeerChange)

### Tracing Usage Patterns

**Basic Logging:**

```rust
// From crates/core/src/operations/get.rs
tracing::debug!(tx = %id, "Requesting get contract {key} @ loc({contract_location})");
tracing::info!(tx = %id, %key, target = %target.peer, "Seek contract");
tracing::warn!(tx = %id, %key, "Contract not found, forwarding to {}", target.peer);
tracing::error!(tx = %id, "Error processing get: {error}");
```

**Span Creation:**

```rust
// From crates/core/src/node/testing_impl.rs
tracing::info_span!("in_mem_gateway", %label)
tracing::info_span!("in_mem_node", %label)
tracing::info_span!(parent: parent_span, "contract_handling")
```

**Instrumentation:**

```rust
// From crates/core/src/node/p2p_impl.rs
task.instrument(tracing::info_span!(
    parent: parent_span.clone(),
    "contract_handling"
))
```

**Transaction-Based Correlation:**

```rust
// Every operation has a transaction ID
let tx = TransactionId::new();
tracing::info!(tx = %tx, "Starting operation");
// ... operation logic ...
tracing::info!(tx = %tx, "Operation completed");
```

### Feature Flags

| Feature | Default | Purpose |
|---------|---------|---------|
| `trace` | ✅ Yes | Enable tracing-subscriber formatted logs |
| `trace-ot` | ❌ No | Enable OpenTelemetry/Jaeger distributed tracing |
| `console-subscriber` | ❌ No | Enable Tokio console runtime debugging |

**Default Features** (`crates/core/Cargo.toml:101`):
```toml
default = ["redb", "trace", "websocket"]
```

### Tokio Console Integration

**Purpose:** Runtime debugging for async Rust applications

**Setup:**
1. Enable feature: `--features console-subscriber`
2. Set environment variable: `TOKIO_CONSOLE=1`
3. Run application
4. Connect with CLI: `tokio-console`

**Provides:**
- Task spawn locations and lifecycle
- Async resource usage (Mutex, RwLock, Semaphore)
- Task wakeup sources and blocking
- Performance bottleneck identification

**Location:** `crates/core/src/tracing/mod.rs:1271-1277`

### OpenTelemetry Integration (Experimental)

**Status:** Currently disabled due to dependency version conflicts

**Intended Capabilities:**
- Distributed tracing across peer network
- Jaeger backend integration
- OTLP exporter support
- Cross-service request correlation

**Location:** `crates/core/src/tracing/mod.rs:1314-1340`

**Re-enabling Requirements:**
- Resolve `opentelemetry` version conflicts
- Test with Jaeger backend
- Document setup and configuration

---

## API Debugging Capabilities

### Overview

Freenet Core provides **three types of APIs** for debugging and diagnostics:

1. **WebSocket API** - Real-time contract operations and subscriptions
2. **HTTP Gateway API** - Contract web interface serving
3. **Diagnostic Query APIs** - System state and network topology queries

### 1. WebSocket API

**Endpoint:** `ws://localhost:7509/v1/contract/command`

**Features:**
- Real-time bidirectional communication
- Two encoding protocols:
  - **Flatbuffers** (default) - Cross-language serialization
  - **Native** (bincode) - Rust-specific, more efficient
- Max message size: 100MB
- Token-based authentication (optional, configurable)

**Message Types:**

| Type | Purpose | Response |
|------|---------|----------|
| `ContractRequest::Put` | Store contract with initial state | `ContractResponse::PutResponse` |
| `ContractRequest::Get` | Retrieve contract and/or state | `ContractResponse::GetResponse` |
| `ContractRequest::Update` | Update contract state | `ContractResponse::UpdateResponse` |
| `ContractRequest::Subscribe` | Subscribe to state changes | `ContractResponse::SubscribeResponse` |
| `ContractRequest::Disconnect` | Clean disconnect | Connection close |

**Debugging Information Available:**

```rust
// Request tracking
pub struct RequestId(Uuid);  // Unique per-request identifier
pub struct ClientId(String); // Persistent client identifier

// Error information
pub enum ErrorKind {
    RequestError(RequestError),
    ProtocolError(ProtocolError),
    // ... more error types
}

pub struct RequestError {
    cause: String,
    kind: ErrorKind,
}
```

**Implementation Location:**
- WebSocket handler: `crates/core/src/client_events/websocket.rs`
- Protocol definitions: `crates/core/src/message.rs`
- Client event processing: `crates/core/src/client_events/mod.rs`

**Example Usage:**

```javascript
// JavaScript client
const ws = new WebSocket('ws://localhost:7509/v1/contract/command');
ws.send(JSON.stringify({
    Put: {
        contract: contractCode,
        state: initialState,
        related_contracts: {}
    }
}));

ws.onmessage = (event) => {
    const response = JSON.parse(event.data);
    console.log('Response:', response);
};
```

### 2. HTTP Gateway API

**Endpoint:** `http://localhost:50509/v1/contract/web/<contract-key>/<path>`

**Purpose:** Serve contract web interfaces (HTML, CSS, JS)

**Features:**
- Automatic token generation per contract
- Cookie-based session management
- Token expiration and cleanup
- Static file serving from contract state

**Token Management:**

```rust
pub struct AuthToken {
    token: String,
    expires_at: SystemTime,
}

// Configuration
pub struct TokenConfig {
    pub token_ttl: Duration,           // Default: 1 hour
    pub cleanup_interval: Duration,    // Default: 5 minutes
}
```

**Debugging Information:**
- Token generation timestamps
- Token expiration times
- Session association (token → client ID)
- Cookie header inspection

**Implementation Location:**
- HTTP gateway: `crates/core/src/server/http_gateway.rs`
- Path handlers: `crates/core/src/server/path_handlers.rs`
- Token management: `crates/core/src/server/token.rs`

### 3. Diagnostic Query APIs

**Access Method:** WebSocket message or CLI tool (`fdev diagnostics`)

**Three Query Types:**

#### A. Connected Peers Query

**Request:**
```rust
ContractRequest::Query(QueryType::ConnectedPeers)
```

**Response:**
```rust
QueryResponse {
    peers: Vec<PeerInfo> {
        peer_id: PeerId,
        location: Option<Location>,  // DHT position (0.0-1.0)
        addresses: Vec<SocketAddr>,
    }
}
```

**Information Provided:**
- Current peer connections
- DHT ring positions
- Network addresses for each peer

#### B. Subscription Info Query

**Request:**
```rust
ContractRequest::Query(QueryType::SubscriptionInfo)
```

**Response:**
```rust
QueryResponse {
    subscriptions: Vec<SubscriptionInfo> {
        client_id: ClientId,
        contract_key: ContractKey,
        subscribed_at: SystemTime,
    }
}
```

**Information Provided:**
- Active client subscriptions
- Contract keys being monitored
- Subscription start times

#### C. Node Diagnostics Query

**Request:**
```rust
ContractRequest::Query(QueryType::NodeDiagnostics {
    network_stats: bool,
    ring_info: bool,
    operation_stats: bool,
    contract_info: bool,
    connection_stats: bool,
    subscription_stats: bool,
    peer_diagnostics: bool,
})
```

**Response Structure:**

```rust
pub struct NodeDiagnostics {
    // Network-level stats
    pub network_stats: Option<NetworkStats> {
        active_connections: usize,
        seeding_contracts: usize,
        cached_contracts: usize,
    },

    // DHT routing table
    pub ring_info: Option<RingInfo> {
        location: Location,
        peers: Vec<PeerKeyLocation>,
    },

    // Operation tracking
    pub operation_stats: Option<OperationStats> {
        active_puts: usize,
        active_gets: usize,
        active_updates: usize,
        active_subscribes: usize,
    },

    // Contract caching
    pub contract_info: Option<Vec<ContractInfo>> {
        key: ContractKey,
        subscribers: usize,
        is_cached: bool,
    },

    // Per-peer connection details
    pub connection_stats: Option<Vec<ConnectionInfo>> {
        peer_id: PeerId,
        connection_time: Duration,
        messages_sent: usize,
        messages_received: usize,
    },

    // Active subscriptions
    pub subscription_stats: Option<Vec<SubscriptionStat>> {
        client_id: ClientId,
        contract_keys: Vec<ContractKey>,
    },

    // Remote peer diagnostics
    pub peer_diagnostics: Option<HashMap<PeerId, NodeDiagnostics>>,
}
```

**CLI Tool:**

```bash
# Query all diagnostics
fdev diagnostics --all

# Query specific categories
fdev diagnostics --network-stats --ring-info

# Query with JSON output
fdev diagnostics --all --format json
```

**Implementation Location:**
- Diagnostics logic: `crates/fdev/src/diagnostics.rs`
- Query handling: `crates/core/src/server/queries.rs`
- CLI tool: `crates/fdev/src/main.rs`

### Network Metrics Server

**Purpose:** Real-time visualization of network metrics

**Endpoint:** `http://localhost:55010/metrics`

**Features:**
- WebSocket-based real-time updates
- Network topology visualization
- Connection event stream
- Peer location tracking

**CLI Tool:**

```bash
# Start metrics server
fdev network-metrics-server --port 55010

# Custom port
fdev network-metrics-server --port 8080
```

**Implementation Location:**
- Metrics server: `crates/fdev/src/network_metrics_server.rs`
- Event streaming: `crates/core/src/tracing/mod.rs` (NetEventRegister)

---

## Current State Assessment

### Strengths

1. **Comprehensive Test Coverage**
   - 161 test attributes across codebase
   - 7 specialized integration tests
   - Real-world application testing (ubertest with River)
   - Deterministic, reproducible tests

2. **Structured Tracing System**
   - Transaction-based correlation
   - Hierarchical spans for nested operations
   - Environment-variable configuration
   - Per-module filtering

3. **Custom Network Event Recording**
   - Append-only file logging
   - Automatic disk space management
   - Binary serialization for efficiency
   - Test-friendly in-memory listener

4. **Rich API Diagnostics**
   - Three query types for different debugging needs
   - Real-time WebSocket communication
   - Token-based security
   - CLI tools for easy access

5. **Documentation**
   - TESTING.md with clear guidelines
   - UBERTEST.md for comprehensive testing
   - Architecture.md explaining system design

### Weaknesses

1. **Test Output Formatting**
   - No structured test result summaries
   - Difficult to parse pass/fail counts
   - No execution time breakdown by test
   - Limited error context in failure messages

2. **API Documentation**
   - No OpenAPI/Swagger specifications
   - Limited examples for external developers
   - No interactive API playground
   - Undocumented error codes and responses

3. **Distributed Tracing**
   - OpenTelemetry integration disabled
   - No cross-peer request visualization
   - Difficult to trace operations across network
   - No Jaeger or Zipkin integration

4. **Metrics Dashboard**
   - Network metrics server has no web UI
   - No historical metrics storage
   - No alerting or anomaly detection
   - Limited visualization options

5. **Error Handling**
   - Inconsistent error context across modules
   - Limited error recovery suggestions
   - No error categorization (transient vs permanent)
   - Missing user-facing error messages

6. **Log Management**
   - No log rotation configuration
   - No centralized log aggregation
   - Append-only file grows unbounded (until truncation)
   - No log shipping to external systems

### Opportunities

1. **Enhanced Test Reporting**
   - JSON test result output
   - HTML test reports with coverage
   - Flamegraphs for performance tests
   - Test trend analysis over time

2. **API Documentation Generation**
   - OpenAPI 3.0 specifications
   - Auto-generated client libraries
   - Interactive Swagger UI
   - Example collections for Postman/Insomnia

3. **Observability Platform**
   - Prometheus metrics export
   - Grafana dashboard templates
   - Jaeger distributed tracing
   - ELK/Loki log aggregation

4. **Developer Experience**
   - VSCode debugging configurations
   - LLDB/GDB pretty-printers for Freenet types
   - Docker Compose for local debugging
   - Mock peer network for fast iteration

5. **Production Monitoring**
   - Health check endpoints
   - Readiness/liveness probes
   - SLA/SLO tracking
   - Incident response playbooks

---

## Recommendations for Improvement

### 1. Enhanced Test Output

**Goal:** Provide structured, parseable test results with rich context

**Implementation:**

```rust
// Test result formatter
pub trait TestResultFormatter {
    fn format_summary(&self, results: &TestResults) -> String;
    fn format_failure(&self, test: &Test, error: &Error) -> String;
}

pub struct JsonFormatter;
impl TestResultFormatter for JsonFormatter {
    fn format_summary(&self, results: &TestResults) -> String {
        serde_json::to_string_pretty(&json!({
            "total": results.total,
            "passed": results.passed,
            "failed": results.failed,
            "duration_ms": results.duration.as_millis(),
            "failures": results.failures.iter().map(|f| {
                json!({
                    "test": f.name,
                    "error": f.error,
                    "file": f.file,
                    "line": f.line,
                })
            }).collect::<Vec<_>>(),
        })).unwrap()
    }
}
```

**Configuration:**

```bash
# Environment variable
export FREENET_TEST_FORMAT=json

# Or CLI flag
cargo test -- --format json --output-file test-results.json
```

**Benefits:**
- CI/CD integration (parse JSON for pass/fail)
- Test trend tracking (store results in database)
- Rich error context (file, line, stack trace)
- Execution time analysis (identify slow tests)

**Files to Modify:**
- `crates/core/src/test_utils.rs` - Add formatter trait
- `.github/workflows/ci.yml` - Use JSON output, store artifacts

### 2. API Documentation (OpenAPI)

**Goal:** Generate interactive API documentation from code

**Implementation:**

```rust
// Use utoipa crate for OpenAPI generation
use utoipa::{OpenApi, ToSchema};

#[derive(OpenApi)]
#[openapi(
    paths(
        handle_put_contract,
        handle_get_contract,
        handle_update_contract,
        handle_subscribe,
    ),
    components(
        schemas(ContractRequest, ContractResponse, ErrorKind)
    ),
    tags(
        (name = "Contracts", description = "Contract operation endpoints")
    )
)]
struct ApiDoc;

// Generate OpenAPI JSON
let openapi = ApiDoc::openapi();
std::fs::write("docs/openapi.json", openapi.to_json()?)?;
```

**Serve with Swagger UI:**

```bash
# Add to HTTP gateway
GET /v1/docs -> Swagger UI
GET /v1/docs/openapi.json -> OpenAPI spec
```

**Benefits:**
- Interactive API testing in browser
- Auto-generated client libraries (TypeScript, Python, Rust)
- Documentation always in sync with code
- Reduced onboarding time for new developers

**Dependencies to Add:**
```toml
[dependencies]
utoipa = "5.0"
utoipa-swagger-ui = "8.0"  # For serving Swagger UI
```

**Files to Modify:**
- `crates/core/src/message.rs` - Add OpenAPI annotations
- `crates/core/src/server/http_gateway.rs` - Serve Swagger UI
- `docs/openapi.json` - Generated OpenAPI spec

### 3. Distributed Tracing (OpenTelemetry)

**Goal:** Re-enable OpenTelemetry for cross-peer request tracing

**Current Issue:** Version conflicts in dependency tree

**Resolution Steps:**

1. Update dependencies to compatible versions:
   ```toml
   [dependencies]
   opentelemetry = "0.27"
   opentelemetry-jaeger = "0.26"
   tracing-opentelemetry = "0.27"
   opentelemetry-otlp = "0.27"
   ```

2. Configure Jaeger exporter:
   ```rust
   // crates/core/src/tracing/mod.rs
   let tracer = opentelemetry_jaeger::new_agent_pipeline()
       .with_service_name("freenet-node")
       .with_endpoint("localhost:6831")
       .install_batch(opentelemetry::runtime::Tokio)?;

   let telemetry_layer = tracing_opentelemetry::layer()
       .with_tracer(tracer);
   ```

3. Propagate trace context across network:
   ```rust
   // crates/core/src/message.rs
   pub struct NetworkMessage {
       pub trace_context: Option<TraceContext>,
       // ... other fields
   }

   // Extract from incoming message
   let parent_context = opentelemetry::global::get_text_map_propagator()
       .extract(&msg.trace_context);

   // Inject into outgoing message
   let mut carrier = HashMap::new();
   opentelemetry::global::get_text_map_propagator()
       .inject_context(&current_context, &mut carrier);
   msg.trace_context = Some(carrier);
   ```

**Visualization:**

```bash
# Run Jaeger locally
docker run -d -p6831:6831/udp -p16686:16686 jaegertracing/all-in-one:latest

# View traces
open http://localhost:16686
```

**Benefits:**
- End-to-end request tracing across peer network
- Latency analysis per operation
- Dependency graph visualization
- Performance bottleneck identification

**Files to Modify:**
- `Cargo.toml` - Update OpenTelemetry versions
- `crates/core/src/tracing/mod.rs:1314-1340` - Re-enable OT integration
- `crates/core/src/message.rs` - Add trace context propagation

### 4. Metrics Dashboard (Prometheus + Grafana)

**Goal:** Provide real-time metrics visualization

**Implementation:**

```rust
// Add prometheus crate
use prometheus::{Encoder, TextEncoder, Counter, Histogram, Gauge};

lazy_static! {
    static ref PUT_REQUESTS: Counter = register_counter!(
        "freenet_put_requests_total",
        "Total PUT requests"
    ).unwrap();

    static ref GET_LATENCY: Histogram = register_histogram!(
        "freenet_get_latency_seconds",
        "GET request latency"
    ).unwrap();

    static ref ACTIVE_CONNECTIONS: Gauge = register_gauge!(
        "freenet_active_connections",
        "Current active connections"
    ).unwrap();
}

// Expose metrics endpoint
async fn metrics_handler() -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = vec![];
    encoder.encode(&metric_families, &mut buffer).unwrap();
    (StatusCode::OK, buffer)
}
```

**Grafana Dashboard JSON:**

```json
{
  "dashboard": {
    "title": "Freenet Node Metrics",
    "panels": [
      {
        "title": "Request Rate",
        "targets": [{
          "expr": "rate(freenet_put_requests_total[5m])"
        }]
      },
      {
        "title": "GET Latency (p95)",
        "targets": [{
          "expr": "histogram_quantile(0.95, freenet_get_latency_seconds)"
        }]
      }
    ]
  }
}
```

**Docker Compose Setup:**

```yaml
version: '3'
services:
  prometheus:
    image: prom/prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml

  grafana:
    image: grafana/grafana
    ports:
      - "3000:3000"
    volumes:
      - ./grafana-dashboards:/var/lib/grafana/dashboards
```

**Benefits:**
- Historical metrics tracking
- Alerting on anomalies
- Capacity planning data
- Performance regression detection

**Dependencies to Add:**
```toml
[dependencies]
prometheus = "0.13"
```

**Files to Create:**
- `crates/core/src/metrics.rs` - Metrics definitions
- `docker/prometheus.yml` - Prometheus configuration
- `docker/grafana-dashboards/freenet.json` - Dashboard definition
- `docker/docker-compose.metrics.yml` - Docker Compose setup

### 5. Improved Error Context

**Goal:** Provide actionable error messages with recovery suggestions

**Implementation:**

```rust
// Enhanced error types
#[derive(Debug, thiserror::Error)]
pub enum FreenetError {
    #[error("Contract not found: {key}")]
    ContractNotFound {
        key: ContractKey,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
        suggestion: String,
    },

    #[error("Network timeout after {duration:?}")]
    NetworkTimeout {
        duration: Duration,
        operation: String,
        suggestion: String,
    },
}

impl FreenetError {
    pub fn contract_not_found(key: ContractKey) -> Self {
        Self::ContractNotFound {
            key,
            source: None,
            suggestion: format!(
                "Contract {} not found in local cache or network. \
                 Try: 1) Check if the contract key is correct, \
                 2) Ensure you're connected to peers, \
                 3) Verify the contract was published successfully.",
                key
            ),
        }
    }

    pub fn suggestion(&self) -> &str {
        match self {
            Self::ContractNotFound { suggestion, .. } => suggestion,
            Self::NetworkTimeout { suggestion, .. } => suggestion,
        }
    }
}
```

**Error Response Format:**

```rust
pub struct ErrorResponse {
    pub error: String,
    pub error_code: String,
    pub suggestion: String,
    pub details: Option<serde_json::Value>,
    pub request_id: RequestId,
}

// Example JSON response
{
    "error": "Contract not found",
    "error_code": "CONTRACT_NOT_FOUND",
    "suggestion": "Check if the contract key is correct...",
    "details": {
        "contract_key": "abc123...",
        "searched_peers": 5
    },
    "request_id": "req-uuid-123"
}
```

**Benefits:**
- Reduced debugging time
- Better user experience
- Self-service problem resolution
- Consistent error handling

**Files to Modify:**
- `crates/core/src/message.rs` - Enhanced error types
- `crates/core/src/operations/*.rs` - Use new error types
- `crates/core/src/client_events/websocket.rs` - Format error responses

### 6. Log Rotation and Management

**Goal:** Prevent unbounded log growth, enable log shipping

**Implementation:**

```rust
// Use tracing-appender for log rotation
use tracing_appender::rolling::{RollingFileAppender, Rotation};

pub fn init_tracer(config: &TracingConfig) -> Result<()> {
    // Daily log rotation
    let file_appender = RollingFileAppender::new(
        Rotation::DAILY,
        &config.log_dir,
        "freenet.log"
    );

    // Keep last 7 days
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    let subscriber = tracing_subscriber::registry()
        .with(fmt::layer().with_writer(non_blocking))
        .with(EnvFilter::from_default_env());

    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}
```

**Configuration:**

```toml
# config.toml
[tracing]
log_dir = "/var/log/freenet"
rotation = "daily"  # or "hourly", "never"
max_files = 7
```

**Log Shipping (Fluent Bit):**

```yaml
# fluent-bit.conf
[INPUT]
    Name tail
    Path /var/log/freenet/*.log
    Parser json

[OUTPUT]
    Name elasticsearch
    Host localhost
    Port 9200
    Index freenet-logs
```

**Benefits:**
- Controlled disk usage
- Long-term log retention (external storage)
- Centralized log analysis
- Compliance with log retention policies

**Dependencies to Add:**
```toml
[dependencies]
tracing-appender = "0.2"
```

**Files to Modify:**
- `crates/core/src/tracing/mod.rs` - Add log rotation
- `crates/core/src/config/mod.rs` - Add log rotation config
- `docker/fluent-bit.conf` - Log shipping configuration

---

## Implementation Priorities

### Phase 1: Quick Wins (1-2 weeks)

**Priority:** High
**Effort:** Low
**Impact:** High

1. **Enhanced Test Output** (3 days)
   - Add JSON formatter for test results
   - Store test results as CI artifacts
   - Create pass/fail summary in GitHub Actions

2. **Improved Error Context** (5 days)
   - Add suggestion field to error types
   - Enhance error messages with recovery steps
   - Document common error codes

3. **Log Rotation** (2 days)
   - Add tracing-appender dependency
   - Configure daily rotation
   - Document log management

### Phase 2: Documentation (2-3 weeks)

**Priority:** High
**Effort:** Medium
**Impact:** High

1. **API Documentation** (1 week)
   - Add utoipa annotations to message types
   - Generate OpenAPI specification
   - Serve Swagger UI on HTTP gateway

2. **Debugging Guide** (1 week)
   - Document tracing configuration
   - Create troubleshooting guide
   - Add common debugging scenarios
   - Document CLI tools (fdev)

### Phase 3: Observability (3-4 weeks)

**Priority:** Medium
**Effort:** High
**Impact:** High

1. **Metrics Dashboard** (2 weeks)
   - Add prometheus metrics
   - Create Grafana dashboards
   - Document metrics and alerting

2. **Distributed Tracing** (2 weeks)
   - Resolve OpenTelemetry version conflicts
   - Re-enable Jaeger integration
   - Add trace context propagation
   - Document Jaeger setup

### Phase 4: Developer Experience (4-6 weeks)

**Priority:** Low
**Effort:** High
**Impact:** Medium

1. **IDE Integration** (1 week)
   - VSCode debugging configurations
   - LLDB pretty-printers for Freenet types
   - Code snippets for common patterns

2. **Local Development Environment** (2 weeks)
   - Docker Compose for multi-peer local network
   - Mock peer network for fast iteration
   - Hot-reload for contract development

3. **Network Visualization UI** (3 weeks)
   - Web-based network topology viewer
   - Real-time connection events
   - Contract propagation visualization

---

## Conclusion

Freenet Core has a **solid foundation for debugging and tracing**, with comprehensive test coverage, structured logging, and rich API diagnostics. The main opportunities for improvement lie in:

1. **Better documentation** (OpenAPI, debugging guides)
2. **Enhanced observability** (metrics, distributed tracing)
3. **Improved developer experience** (test output, error messages, local dev environment)

By implementing the recommendations in this document, we can significantly improve the debugging experience for both developers and operators, leading to faster development cycles and more reliable production deployments.

### Key Metrics for Success

- **Test output**: JSON format available, CI stores artifacts
- **API documentation**: OpenAPI spec available, Swagger UI accessible
- **Error context**: 90% of errors include recovery suggestions
- **Log management**: Rotation enabled, max 7 days local retention
- **Metrics**: Prometheus endpoint available, 3+ Grafana dashboards
- **Distributed tracing**: OpenTelemetry re-enabled, Jaeger integration working
- **Developer onboarding**: New developers can debug issues in <1 week

### Next Steps

1. Review this analysis with the team
2. Prioritize recommendations based on team needs
3. Create GitHub issues for each improvement
4. Assign owners and timelines
5. Track progress in project board

---

**Document Version:** 1.0
**Last Updated:** October 25, 2025
**Authors:** Claude (AI Assistant)
**Review Status:** Pending team review
