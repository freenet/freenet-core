# Freenet Subscription Integration Test Infrastructure

## Goals
1. Test peer behavior in actual multi-node networks
2. Enable convenient debugging for humans and AI agents
3. Provide unified timeline logs from multiple instances
4. Rigorously test the subscription fixes in PR #1854

## Architecture Components

### 1. Test Harness Core (`test_harness.rs`)
```rust
pub struct MultiNodeTestHarness {
    supervisor: TestSupervisor,
    nodes: Vec<NodeInstance>,
    log_collector: UnifiedLogCollector,
    network_monitor: NetworkMonitor,
    test_contracts: HashMap<ContractKey, TestContract>,
}

pub struct NodeInstance {
    id: PeerId,
    config: NodeConfig,
    ws_client: WebApi,
    process: Child,
    log_path: PathBuf,
    location: Location,
    is_gateway: bool,
}

pub struct UnifiedLogCollector {
    timeline: BTreeMap<Instant, LogEntry>,
    node_streams: HashMap<PeerId, LogStream>,
    correlation_ids: HashMap<Transaction, Vec<LogEntry>>,
}
```

### 2. Network Topology Builder (`topology.rs`)
```rust
pub enum NetworkTopology {
    /// Linear chain: GW -> N1 -> N2 -> ... -> Nn
    Linear { nodes: usize },

    /// Star: GW in center, all nodes connect to GW
    Star { nodes: usize },

    /// Ring: Nodes form a ring with GW as entry point
    Ring { nodes: usize },

    /// Mesh: Partially connected mesh with configurable density
    Mesh { nodes: usize, connectivity: f64 },

    /// Custom topology from adjacency list
    Custom { adjacency: HashMap<PeerId, Vec<PeerId>> },
}
```

### 3. Test Scenarios (`scenarios/mod.rs`)

#### Subscription Routing Fix Test
```rust
// Tests that subscription responses reach the originating client
// Validates fix for missing waiting_for_transaction_result
async fn test_subscription_response_routing() {
    let harness = MultiNodeTestHarness::new(NetworkTopology::Linear { nodes: 5 });

    // Deploy contract on node 3
    let contract = harness.deploy_contract_on_node(3, test_contract()).await;

    // Subscribe from gateway (node 0)
    let subscription = harness.subscribe_from_node(0, contract.key).await;

    // Verify subscription response reaches gateway
    harness.assert_subscription_received(0, subscription.id).await;

    // Check logs for proper transaction correlation
    harness.verify_transaction_correlation(subscription.id).await;
}
```

#### Optimal Location Subscription Test
```rust
// Tests that nodes at optimal location can subscribe to contracts
// Validates removal of early return in start_subscription_request
async fn test_optimal_location_subscription() {
    let harness = MultiNodeTestHarness::new(NetworkTopology::Ring { nodes: 10 });

    // Deploy contract and wait for it to reach optimal location
    let contract = harness.deploy_contract_with_location(test_location()).await;
    let optimal_node = harness.get_node_at_location(test_location());

    // Trigger subscription from optimal node
    harness.trigger_subscription_at_node(optimal_node, contract.key).await;

    // Verify subscription succeeds using k_closest_potentially_caching
    harness.assert_subscription_active(optimal_node, contract.key).await;
}
```

#### Multiple Peer Candidates Test
```rust
// Tests k_closest_potentially_caching with k=3
// Validates that subscription tries multiple peers on failure
async fn test_multiple_peer_candidates() {
    let harness = MultiNodeTestHarness::new(NetworkTopology::Mesh {
        nodes: 20,
        connectivity: 0.3
    });

    // Deploy contract
    let contract = harness.deploy_contract().await;

    // Make first two candidate peers unavailable
    harness.block_peers(&[1, 2]).await;

    // Subscribe should succeed with third candidate
    let subscription = harness.subscribe_with_monitoring(contract.key).await;

    // Verify it tried first two and succeeded with third
    harness.assert_peer_sequence_attempted(
        subscription.id,
        &[peer_1, peer_2, peer_3]
    ).await;
}
```

### 4. Log Collection and Analysis (`logging.rs`)

```rust
pub struct LogEntry {
    timestamp: Instant,
    node_id: PeerId,
    level: Level,
    message: String,
    transaction_id: Option<Transaction>,
    operation_type: Option<OperationType>,
    fields: HashMap<String, String>,
}

impl UnifiedLogCollector {
    /// Collects logs from all nodes in real-time
    pub async fn start_collection(&mut self) {
        for node in &self.nodes {
            self.spawn_log_tail(node).await;
        }
    }

    /// Creates unified timeline view
    pub fn get_timeline(&self) -> String {
        let mut output = String::new();
        for (instant, entry) in &self.timeline {
            writeln!(output, "[{:?}] [{}] {}: {}",
                instant, entry.node_id, entry.level, entry.message);
        }
        output
    }

    /// Traces a specific transaction across all nodes
    pub fn trace_transaction(&self, tx: Transaction) -> TransactionTrace {
        TransactionTrace {
            id: tx,
            events: self.correlation_ids.get(&tx).cloned().unwrap_or_default(),
            nodes_involved: self.get_nodes_for_transaction(tx),
            duration: self.get_transaction_duration(tx),
        }
    }
}
```

### 5. Network Monitoring (`monitor.rs`)

```rust
pub struct NetworkMonitor {
    connections: HashMap<(PeerId, PeerId), ConnectionStatus>,
    message_flows: Vec<MessageFlow>,
    contract_locations: HashMap<ContractKey, Vec<PeerId>>,
}

pub struct MessageFlow {
    from: PeerId,
    to: PeerId,
    message_type: String,
    transaction_id: Transaction,
    timestamp: Instant,
}

impl NetworkMonitor {
    /// Visualize message flow for debugging
    pub fn generate_sequence_diagram(&self, tx: Transaction) -> String {
        // Generates PlantUML or Mermaid diagram
    }

    /// Check network partitions
    pub fn detect_partitions(&self) -> Vec<Vec<PeerId>> {
        // Returns disconnected components
    }
}
```

### 6. Riverctl Integration (`riverctl.rs`)

```rust
pub struct RiverctlClient {
    process: Child,
    api_endpoint: String,
    working_dir: PathBuf,
}

impl RiverctlClient {
    /// Spawn riverctl connected to a test node
    pub async fn connect_to_node(node: &NodeInstance) -> Result<Self> {
        let mut cmd = Command::new("riverctl");
        cmd.arg("--api").arg(&node.ws_endpoint());
        cmd.arg("--verbose");
        let process = cmd.spawn()?;

        Ok(RiverctlClient {
            process,
            api_endpoint: node.ws_endpoint(),
            working_dir: env::temp_dir(),
        })
    }

    /// Execute riverctl commands
    pub async fn execute(&mut self, command: &str) -> Result<String> {
        // Send command to riverctl process
        // Parse and return output
    }

    /// Test contract deployment via riverctl
    pub async fn deploy_contract(&mut self, contract_path: &Path) -> Result<ContractKey> {
        let output = self.execute(&format!("contract deploy {}", contract_path.display())).await?;
        // Parse contract key from output
    }

    /// Subscribe to contract updates via riverctl
    pub async fn subscribe(&mut self, key: ContractKey) -> Result<()> {
        self.execute(&format!("contract subscribe {}", key)).await?;
        Ok(())
    }
}

/// High-level riverctl test scenarios
pub async fn test_riverctl_subscription_flow() {
    let harness = MultiNodeTestHarness::new(NetworkTopology::Star { nodes: 5 });

    // Connect riverctl to gateway
    let mut riverctl = RiverctlClient::connect_to_node(&harness.gateway()).await?;

    // Deploy contract via riverctl
    let contract = riverctl.deploy_contract(&test_contract_path()).await?;

    // Subscribe via riverctl
    riverctl.subscribe(contract).await?;

    // Update contract from another node
    harness.update_contract_from_node(2, contract, delta).await?;

    // Verify riverctl receives update notification
    let updates = riverctl.execute("contract updates").await?;
    assert!(updates.contains(&contract.to_string()));
}
```

### 7. Test Utilities (`utils.rs`)

```rust
/// Waits for contract to propagate to expected nodes
pub async fn wait_for_propagation(
    harness: &MultiNodeTestHarness,
    contract: ContractKey,
    expected_nodes: usize,
    timeout: Duration,
) -> Result<()> {
    // Poll nodes until contract is found
}

/// Injects failures for resilience testing
pub async fn inject_network_failure(
    harness: &mut MultiNodeTestHarness,
    failure_type: FailureType,
) {
    match failure_type {
        FailureType::NodeCrash(peer_id) => { /* kill process */ },
        FailureType::NetworkPartition(peers) => { /* block connections */ },
        FailureType::SlowNetwork(latency) => { /* add latency */ },
    }
}

/// Check if riverctl is available
pub fn riverctl_available() -> bool {
    Command::new("riverctl")
        .arg("--version")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}
```

### 7. GitHub Actions Integration (`.github/workflows/integration-tests.yml`)

```yaml
name: Integration Tests
on: [push, pull_request]
jobs:
  subscription-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Install riverctl (optional)
        run: cargo install riverctl --force

      - name: Run subscription integration tests
        run: |
          cargo test --test subscription_integration --features integration-tests
        env:
          RUST_LOG: debug
          FREENET_TEST_TIMEOUT: 120

      - name: Upload test logs
        if: failure()
        uses: actions/upload-artifact@v3
        with:
          name: test-logs
          path: target/test-logs/

      - name: Generate test report
        run: cargo run --bin generate-test-report
```

## Implementation Plan

### Phase 1: Core Infrastructure (Week 1)
- [ ] Implement MultiNodeTestHarness
- [ ] Create UnifiedLogCollector with timeline view
- [ ] Build basic network topologies (Linear, Star)
- [ ] Set up process management for multiple nodes

### Phase 2: Test Scenarios (Week 1-2)
- [ ] Implement subscription_response_routing test
- [ ] Implement optimal_location_subscription test
- [ ] Implement multiple_peer_candidates test
- [ ] Add contract state verification helpers

### Phase 3: Monitoring & Debugging (Week 2)
- [ ] Build NetworkMonitor with connection tracking
- [ ] Add transaction tracing across nodes
- [ ] Create sequence diagram generation
- [ ] Implement failure injection utilities

### Phase 4: CI Integration (Week 2-3)
- [ ] Configure GitHub Actions workflow
- [ ] Add test parallelization
- [ ] Implement log artifact collection
- [ ] Create test report generation

## Key Features

### 1. Deterministic Testing
- Fixed seed for reproducible tests
- Controlled network topologies
- Predictable peer locations

### 2. Comprehensive Logging
- Unified timeline across all nodes
- Transaction correlation
- Automatic log collection on failure

### 3. Visual Debugging
- Sequence diagrams for message flows
- Network topology visualization
- Contract propagation maps

### 4. Failure Injection
- Node crashes
- Network partitions
- Latency simulation
- Byzantine behavior

### 5. Performance Metrics
- Operation latency tracking
- Message count analysis
- Network overhead measurement

## Success Criteria

1. **Correctness**: All subscription fixes in PR #1854 have corresponding tests
2. **Debuggability**: Failed tests produce clear, actionable logs
3. **Reliability**: Tests run consistently in CI without flakes
4. **Performance**: Full test suite runs in under 5 minutes
5. **Maintainability**: New test scenarios can be added easily

## Example Test Output

```
Running subscription integration tests...

Test: subscription_response_routing
  ✓ Network initialized (5 nodes, linear topology)
  ✓ Contract deployed at node 3
  ✓ Subscription initiated from gateway
  ✓ Response received at gateway (latency: 125ms)
  ✓ Transaction correlation verified

Test: optimal_location_subscription
  ✓ Network initialized (10 nodes, ring topology)
  ✓ Contract deployed at optimal location
  ✗ Subscription failed at optimal node

    Timeline:
    [0.001s] [GW] Starting subscription for contract_abc
    [0.012s] [N5] Received subscribe request
    [0.013s] [N5] ERROR: Early return due to optimal location

    Suggested fix: Remove early return in start_subscription_request

Test Summary: 1 passed, 1 failed
Logs saved to: target/test-logs/run-2024-01-24-123456/
```

## Next Steps

1. Review and refine design with team
2. Start implementation with Phase 1
3. Use PR #1854 as test subject
4. Iterate based on test results
5. Document patterns for future test development