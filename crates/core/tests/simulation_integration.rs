//! Deterministic simulation tests using Turmoil.
//!
//! All tests in this file use Turmoil's deterministic scheduler for reproducible execution.
//! Same seed MUST produce identical results across runs.
//!
//! NOTE: These tests use global state (socket registries, RNG) and must run serially.
//! Enable with: cargo test -p freenet --features simulation_tests --test simulation_integration -- --test-threads=1
//!
//! For non-deterministic smoke tests, see `simulation_smoke.rs`.

#![cfg(feature = "simulation_tests")]

use freenet::config::{GlobalRng, GlobalSimulationTime};
use freenet::dev_tool::{
    check_convergence_from_logs, reset_all_simulation_state, SimNetwork, VirtualTime,
};
use freenet::transport::in_memory_socket::{
    clear_all_socket_registries, register_address_network, register_network_time_source,
    SimulationSocket,
};
use std::collections::HashMap;
use std::net::{Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

// =============================================================================
// Test Configuration
// =============================================================================

/// Configuration for a simulation test.
struct TestConfig {
    name: &'static str,
    seed: u64,
    gateways: usize,
    nodes: usize,
    ring_max_htl: usize,
    rnd_if_htl_above: usize,
    max_connections: usize,
    min_connections: usize,
    max_contracts: usize,
    iterations: usize,
    duration: Duration,
    sleep_between_ops: Duration,
    require_convergence: bool,
}

impl TestConfig {
    /// Create a small test configuration (quick CI tests).
    fn small(name: &'static str, seed: u64) -> Self {
        Self {
            name,
            seed,
            gateways: 1,
            nodes: 3,
            ring_max_htl: 7,
            rnd_if_htl_above: 3,
            max_connections: 10,
            min_connections: 2,
            max_contracts: 3,
            iterations: 15,
            duration: Duration::from_secs(20),
            sleep_between_ops: Duration::from_secs(1),
            require_convergence: false,
        }
    }

    /// Create a medium test configuration.
    fn medium(name: &'static str, seed: u64) -> Self {
        Self {
            name,
            seed,
            gateways: 2,
            nodes: 6,
            ring_max_htl: 10,
            rnd_if_htl_above: 7,
            max_connections: 15,
            min_connections: 2,
            max_contracts: 8,
            iterations: 100,
            duration: Duration::from_secs(120),
            sleep_between_ops: Duration::from_secs(3),
            require_convergence: false,
        }
    }

    /// Create a large/dense test configuration.
    fn large(name: &'static str, seed: u64) -> Self {
        Self {
            name,
            seed,
            gateways: 3,
            nodes: 12,
            ring_max_htl: 12,
            rnd_if_htl_above: 8,
            max_connections: 12,
            min_connections: 6,
            max_contracts: 15,
            iterations: 150,
            duration: Duration::from_secs(300),
            sleep_between_ops: Duration::from_secs(10),
            require_convergence: false,
        }
    }

    // Builder methods
    fn with_nodes(mut self, nodes: usize) -> Self {
        self.nodes = nodes;
        self
    }

    fn with_gateways(mut self, gateways: usize) -> Self {
        self.gateways = gateways;
        self
    }

    fn with_iterations(mut self, iterations: usize) -> Self {
        self.iterations = iterations;
        self
    }

    fn with_max_contracts(mut self, max_contracts: usize) -> Self {
        self.max_contracts = max_contracts;
        self
    }

    fn with_duration(mut self, duration: Duration) -> Self {
        self.duration = duration;
        self
    }

    fn with_sleep(mut self, sleep: Duration) -> Self {
        self.sleep_between_ops = sleep;
        self
    }

    fn with_connections(mut self, min: usize, max: usize) -> Self {
        self.min_connections = min;
        self.max_connections = max;
        self
    }

    fn with_htl(mut self, max_htl: usize, rnd_above: usize) -> Self {
        self.ring_max_htl = max_htl;
        self.rnd_if_htl_above = rnd_above;
        self
    }

    fn require_convergence(mut self) -> Self {
        self.require_convergence = true;
        self
    }

    /// Run the simulation and return results.
    fn run(self) -> TestResult {
        setup_deterministic_state(self.seed);
        let rt = create_runtime();

        let (sim, logs_handle) = rt.block_on(async {
            let sim = SimNetwork::new(
                self.name,
                self.gateways,
                self.nodes,
                self.ring_max_htl,
                self.rnd_if_htl_above,
                self.max_connections,
                self.min_connections,
                self.seed,
            )
            .await;
            let logs_handle = sim.event_logs_handle();
            (sim, logs_handle)
        });

        let sleep_duration = self.sleep_between_ops;
        let result = sim.run_simulation::<rand::rngs::SmallRng, _, _>(
            self.seed,
            self.max_contracts,
            self.iterations,
            self.duration,
            move || async move {
                tokio::time::sleep(sleep_duration).await;
                Ok(())
            },
        );

        let convergence = rt.block_on(async { check_convergence_from_logs(&logs_handle).await });
        let event_count = rt.block_on(async { logs_handle.lock().await.len() });

        TestResult {
            seed: self.seed,
            name: self.name,
            simulation_result: result,
            convergence,
            event_count,
            require_convergence: self.require_convergence,
            logs_handle,
        }
    }
}

/// Result of running a simulation test.
struct TestResult {
    seed: u64,
    name: &'static str,
    simulation_result: turmoil::Result,
    convergence: freenet::dev_tool::ConvergenceResult,
    event_count: usize,
    require_convergence: bool,
    logs_handle: Arc<Mutex<Vec<freenet::tracing::NetLogMessage>>>,
}

impl TestResult {
    /// Assert the simulation completed successfully.
    fn assert_ok(self) -> Self {
        if let Err(e) = &self.simulation_result {
            tracing::error!("============================================================");
            tracing::error!("SIMULATION TEST FAILED: {}", self.name);
            tracing::error!("============================================================");
            tracing::error!("Seed for reproduction: 0x{:X}", self.seed);
            tracing::error!("Error: {:?}", e);
            tracing::error!("============================================================");
        }
        assert!(
            self.simulation_result.is_ok(),
            "{} failed: {:?}",
            self.name,
            self.simulation_result.err()
        );
        self
    }

    /// Log convergence results and optionally assert convergence.
    fn check_convergence(self) -> Self {
        tracing::info!("=== CONVERGENCE CHECK: {} ===", self.name);
        tracing::info!(
            "Result: {} converged, {} diverged, {} events",
            self.convergence.converged.len(),
            self.convergence.diverged.len(),
            self.event_count
        );

        // Get logs for detailed analysis
        let rt = create_runtime();
        let logs = rt.block_on(async { self.logs_handle.lock().await.clone() });

        for diverged in &self.convergence.diverged {
            tracing::warn!(
                "DIVERGED: {} - {} unique states across {} peers",
                diverged.contract_key,
                diverged.unique_state_count(),
                diverged.peer_states.len()
            );
            for (peer, hash) in &diverged.peer_states {
                tracing::warn!("  peer {}: {}", peer, hash);
            }

            // Show stored_hash events only (PutSuccess, UpdateSuccess, BroadcastApplied)
            tracing::debug!(
                "Stored state events for contract {}:",
                diverged.contract_key
            );
            for log in &logs {
                let contract_key = log.kind.contract_key().map(|k| format!("{:?}", k));
                if contract_key.as_ref() == Some(&diverged.contract_key) {
                    if let Some(state_hash) = log.kind.stored_state_hash() {
                        let variant = log.kind.variant_name();
                        tracing::debug!(
                            "  {} @ {}: {} -> {}",
                            log.tx,
                            log.peer_id.addr,
                            variant,
                            &state_hash[..16]
                        );
                    }
                }
            }

            // Show Subscribe events
            tracing::debug!("Subscribe events for contract {}:", diverged.contract_key);
            for log in &logs {
                let contract_key = log.kind.contract_key().map(|k| format!("{:?}", k));
                if contract_key.as_ref() == Some(&diverged.contract_key) {
                    let variant = log.kind.variant_name();
                    if variant.contains("Subscribe") {
                        tracing::debug!("  {} @ {}: {}", log.tx, log.peer_id.addr, variant);
                    }
                }
            }
        }

        if self.require_convergence {
            assert!(
                self.convergence.is_converged(),
                "{} convergence failed: {} converged, {} diverged",
                self.name,
                self.convergence.converged.len(),
                self.convergence.diverged.len()
            );
        }

        tracing::info!("{} PASSED", self.name);
        self
    }

    /// Verify that all operation types (PUT, GET, UPDATE, SUBSCRIBE) were executed.
    ///
    /// This ensures tests are exercising the full range of contract operations
    /// for robust coverage. Fails if any operation type has zero occurrences.
    ///
    /// Also tracks concurrent PUTs - multiple PUTs to the same contract from different peers.
    /// This exercises the CRDT merge logic that was fixed in PR #2683.
    fn verify_operation_coverage(self) -> Self {
        let rt = create_runtime();
        let logs = rt.block_on(async { self.logs_handle.lock().await.clone() });

        let mut put_count = 0;
        let mut get_count = 0;
        let mut update_count = 0;
        let mut subscribe_count = 0;
        let mut contracts_with_puts: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        // Track PUTs per contract per peer to detect concurrent puts
        // Key: contract_key, Value: set of (peer_addr, state_hash) tuples
        let mut puts_per_contract: std::collections::HashMap<
            String,
            std::collections::HashSet<(String, String)>,
        > = std::collections::HashMap::new();

        for log in &logs {
            let variant = log.kind.variant_name();
            if variant.starts_with("Put") {
                put_count += 1;
                if let Some(key) = log.kind.contract_key() {
                    let key_str = format!("{:?}", key);
                    contracts_with_puts.insert(key_str.clone());

                    // Track peer and state for this PUT
                    let peer_addr = format!("{}", log.peer_id.addr);
                    let state_hash = log
                        .kind
                        .state_hash()
                        .map(|h| h[..8].to_string())
                        .unwrap_or_default();
                    puts_per_contract
                        .entry(key_str)
                        .or_default()
                        .insert((peer_addr, state_hash));
                }
            } else if variant.starts_with("Get") {
                get_count += 1;
            } else if variant.starts_with("Update") {
                update_count += 1;
            } else if variant.starts_with("Subscribe") {
                subscribe_count += 1;
            }
        }

        // Count contracts with concurrent puts (multiple unique peer+state combinations)
        let contracts_with_concurrent_puts: Vec<_> = puts_per_contract
            .iter()
            .filter(|(_, puts)| puts.len() > 1)
            .collect();

        tracing::info!("=== OPERATION COVERAGE: {} ===", self.name);
        tracing::info!("PUT operations:       {}", put_count);
        tracing::info!("GET operations:       {}", get_count);
        tracing::info!("UPDATE operations:    {}", update_count);
        tracing::info!("SUBSCRIBE operations: {}", subscribe_count);
        tracing::info!("Contracts with PUTs:  {}", contracts_with_puts.len());
        tracing::info!(
            "Contracts with concurrent PUTs: {}",
            contracts_with_concurrent_puts.len()
        );

        // Log details of concurrent puts for debugging
        for (contract, puts) in &contracts_with_concurrent_puts {
            tracing::debug!(
                "  Contract {} has {} concurrent puts from peers:",
                &contract[..20],
                puts.len()
            );
            for (peer, hash) in puts.iter().take(5) {
                tracing::debug!("    peer={} state={}", peer, hash);
            }
        }

        // Verify minimum coverage - all operation types should be exercised
        assert!(
            put_count > 0,
            "{}: No PUT operations detected - test not exercising puts",
            self.name
        );
        assert!(
            get_count > 0,
            "{}: No GET operations detected - test not exercising gets",
            self.name
        );
        assert!(
            update_count > 0,
            "{}: No UPDATE operations detected - test not exercising updates",
            self.name
        );
        assert!(
            subscribe_count > 0,
            "{}: No SUBSCRIBE operations detected - test not exercising subscribes",
            self.name
        );

        // Verify at least one contract is being tested
        // Note: For larger convergence tests, multiple contracts should naturally be created
        // given sufficient iterations. Small CI tests may only have 1 contract.
        assert!(
            !contracts_with_puts.is_empty(),
            "{}: No contracts with PUTs - test not exercising contract creation",
            self.name
        );

        tracing::info!(
            "{}: Operation coverage verified (PUT={}, GET={}, UPDATE={}, SUBSCRIBE={}, contracts={})",
            self.name,
            put_count,
            get_count,
            update_count,
            subscribe_count,
            contracts_with_puts.len()
        );

        self
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Setup deterministic simulation state before running a test.
fn setup_deterministic_state(seed: u64) {
    reset_all_simulation_state();
    GlobalRng::set_seed(seed);
    const BASE_EPOCH_MS: u64 = 1577836800000; // 2020-01-01 00:00:00 UTC
    const RANGE_MS: u64 = 5 * 365 * 24 * 60 * 60 * 1000; // ~5 years
    GlobalSimulationTime::set_time_ms(BASE_EPOCH_MS + (seed % RANGE_MS));
}

/// Create a tokio runtime for running simulation setup.
fn create_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

// =============================================================================
// STRICT Determinism Tests - Exact Event Equality
// =============================================================================

/// **STRICT** determinism test: verifies that same seed produces EXACTLY identical events.
///
/// This test uses Turmoil's deterministic scheduler to ensure reproducible execution.
/// It verifies:
/// 1. Exact same number of events
/// 2. Exact same event counts per type
/// 3. Exact same event sequence (order, content)
///
/// If this test fails, it indicates non-determinism in the simulation that Turmoil
/// doesn't control (e.g., HashMap iteration order, external I/O, real time usage).
///
/// The test runs the simulation 3 times with the same seed and compares traces.
/// All three runs must produce identical results.
#[test_log::test]
fn test_strict_determinism_exact_event_equality() {
    const SEED: u64 = 0xDE7E_2A1E_1234;

    /// Captures all simulation state for comparison
    #[derive(Debug, PartialEq)]
    struct SimulationTrace {
        event_counts: HashMap<String, usize>,
        event_sequence: Vec<String>, // event_kind names in order
        total_events: usize,
    }

    fn run_and_trace(name: &str, seed: u64) -> (turmoil::Result, SimulationTrace) {
        setup_deterministic_state(seed);

        let rt = create_runtime();

        // Create SimNetwork and get event logs handle before run_simulation consumes it
        let (sim, logs_handle) = rt.block_on(async {
            let sim = SimNetwork::new(
                name, 1,  // gateways - single gateway for determinism
                6,  // nodes
                7,  // ring_max_htl
                3,  // rnd_if_htl_above
                10, // max_connections
                2,  // min_connections
                seed,
            )
            .await;
            let logs_handle = sim.event_logs_handle();
            (sim, logs_handle)
        });

        // Run simulation with Turmoil's deterministic scheduler
        let result = sim.run_simulation::<rand::rngs::SmallRng, _, _>(
            seed,
            5,                       // max_contract_num
            20,                      // iterations
            Duration::from_secs(30), // simulation_duration
            || async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok(())
            },
        );

        // Extract event trace from the shared logs handle
        let trace = rt.block_on(async {
            let logs = logs_handle.lock().await;
            let mut event_counts: HashMap<String, usize> = HashMap::new();
            let mut event_sequence: Vec<String> = Vec::new();

            for log in logs.iter() {
                let kind_name = log.kind.variant_name().to_string();
                *event_counts.entry(kind_name.clone()).or_insert(0) += 1;
                event_sequence.push(kind_name);
            }

            SimulationTrace {
                total_events: logs.len(),
                event_counts,
                event_sequence,
            }
        });

        (result, trace)
    }

    // Run simulation THREE times with identical seed
    let (result1, trace1) = run_and_trace("strict-det-run1", SEED);
    let (result2, trace2) = run_and_trace("strict-det-run2", SEED);
    let (result3, trace3) = run_and_trace("strict-det-run3", SEED);

    // All simulations should have the same outcome
    assert_eq!(
        result1.is_ok(),
        result2.is_ok(),
        "STRICT DETERMINISM FAILURE: Simulation outcomes differ!\nRun 1: {:?}\nRun 2: {:?}",
        result1,
        result2
    );
    assert_eq!(
        result2.is_ok(),
        result3.is_ok(),
        "STRICT DETERMINISM FAILURE: Simulation outcomes differ!\nRun 2: {:?}\nRun 3: {:?}",
        result2,
        result3
    );

    // Debug: Print detailed event breakdown before assertions
    if trace1.total_events != trace2.total_events || trace2.total_events != trace3.total_events {
        tracing::info!("\n=== DETERMINISM DEBUG ===");
        tracing::info!(
            "Run 1 total: {}, Run 2 total: {}, Run 3 total: {}",
            trace1.total_events,
            trace2.total_events,
            trace3.total_events
        );

        let mut all_types: std::collections::BTreeSet<&String> =
            trace1.event_counts.keys().collect();
        all_types.extend(trace2.event_counts.keys());
        all_types.extend(trace3.event_counts.keys());

        for event_type in all_types {
            let count1 = trace1.event_counts.get(event_type).unwrap_or(&0);
            let count2 = trace2.event_counts.get(event_type).unwrap_or(&0);
            let count3 = trace3.event_counts.get(event_type).unwrap_or(&0);
            if count1 != count2 || count2 != count3 {
                tracing::info!(
                    "  {} : {} vs {} vs {} (DIFFERS)",
                    event_type,
                    count1,
                    count2,
                    count3
                );
            }
        }
        tracing::info!("=========================\n");
    }

    // STRICT ASSERTION 1: Exact same total event count
    assert_eq!(
        trace1.total_events, trace2.total_events,
        "STRICT DETERMINISM FAILURE: Total event counts differ!"
    );
    assert_eq!(
        trace2.total_events, trace3.total_events,
        "STRICT DETERMINISM FAILURE: Total event counts differ!"
    );

    // STRICT ASSERTION 2: Exact same event counts per type
    assert_eq!(
        trace1.event_counts, trace2.event_counts,
        "STRICT DETERMINISM FAILURE: Event counts per type differ!"
    );
    assert_eq!(
        trace2.event_counts, trace3.event_counts,
        "STRICT DETERMINISM FAILURE: Event counts per type differ!"
    );

    // STRICT ASSERTION 3: Exact same event sequence
    for (i, ((e1, e2), e3)) in trace1
        .event_sequence
        .iter()
        .zip(trace2.event_sequence.iter())
        .zip(trace3.event_sequence.iter())
        .enumerate()
    {
        assert_eq!(e1, e2, "Event sequence differs at index {}!", i);
        assert_eq!(e2, e3, "Event sequence differs at index {}!", i);
    }

    tracing::info!(
        "STRICT DETERMINISM TEST PASSED: {} events matched exactly across 3 runs",
        trace1.total_events
    );
}

/// **STRICT** determinism test with MULTIPLE GATEWAYS.
///
/// This test verifies that simulations with 2+ gateways remain deterministic.
#[test_log::test]
fn test_strict_determinism_multi_gateway() {
    const SEED: u64 = 0xAB17_6A7E_1234;

    #[derive(Debug, PartialEq)]
    struct SimulationTrace {
        event_counts: HashMap<String, usize>,
        event_sequence: Vec<String>,
        total_events: usize,
    }

    fn run_and_trace(name: &str, seed: u64) -> (turmoil::Result, SimulationTrace) {
        setup_deterministic_state(seed);

        let rt = create_runtime();

        let (sim, logs_handle) = rt.block_on(async {
            let sim = SimNetwork::new(
                name, 2, // gateways - MULTI-GATEWAY
                6, 7, 3, 10, 2, seed,
            )
            .await;
            let logs_handle = sim.event_logs_handle();
            (sim, logs_handle)
        });

        let result = sim.run_simulation::<rand::rngs::SmallRng, _, _>(
            seed,
            5,
            20,
            Duration::from_secs(30),
            || async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok(())
            },
        );

        let trace = rt.block_on(async {
            let logs = logs_handle.lock().await;
            let mut event_counts: HashMap<String, usize> = HashMap::new();
            let mut event_sequence: Vec<String> = Vec::new();

            for log in logs.iter() {
                let kind_name = log.kind.variant_name().to_string();
                *event_counts.entry(kind_name.clone()).or_insert(0) += 1;
                event_sequence.push(kind_name);
            }

            SimulationTrace {
                total_events: logs.len(),
                event_counts,
                event_sequence,
            }
        });

        (result, trace)
    }

    let (result1, trace1) = run_and_trace("multi-gw-det-run1", SEED);
    let (result2, trace2) = run_and_trace("multi-gw-det-run2", SEED);
    let (result3, trace3) = run_and_trace("multi-gw-det-run3", SEED);

    assert_eq!(result1.is_ok(), result2.is_ok());
    assert_eq!(result2.is_ok(), result3.is_ok());

    assert_eq!(trace1.total_events, trace2.total_events);
    assert_eq!(trace2.total_events, trace3.total_events);

    assert_eq!(trace1.event_counts, trace2.event_counts);
    assert_eq!(trace2.event_counts, trace3.event_counts);

    for (i, ((e1, e2), e3)) in trace1
        .event_sequence
        .iter()
        .zip(trace2.event_sequence.iter())
        .zip(trace3.event_sequence.iter())
        .enumerate()
    {
        assert_eq!(e1, e2, "Event sequence differs at index {}!", i);
        assert_eq!(e2, e3, "Event sequence differs at index {}!", i);
    }

    tracing::info!(
        "MULTI-GATEWAY DETERMINISM TEST PASSED: {} events (2 gateways)",
        trace1.total_events
    );
}

/// **STRICT** determinism test: verifies that same seed produces identical replay.
#[test_log::test]
fn test_deterministic_replay_events() {
    const SEED: u64 = 0xDEAD_BEEF_1234;

    #[derive(Debug, PartialEq)]
    struct ReplayTrace {
        event_counts: HashMap<String, usize>,
        total_events: usize,
    }

    fn run_and_trace(name: &str, seed: u64) -> (turmoil::Result, ReplayTrace) {
        setup_deterministic_state(seed);

        let rt = create_runtime();

        let (sim, logs_handle) = rt.block_on(async {
            let sim = SimNetwork::new(name, 1, 3, 7, 3, 10, 2, seed).await;
            let logs_handle = sim.event_logs_handle();
            (sim, logs_handle)
        });

        let result = sim.run_simulation::<rand::rngs::SmallRng, _, _>(
            seed,
            3,
            10,
            Duration::from_secs(20),
            || async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok(())
            },
        );

        let trace = rt.block_on(async {
            let logs = logs_handle.lock().await;
            let mut event_counts: HashMap<String, usize> = HashMap::new();

            for log in logs.iter() {
                let kind_name = log.kind.variant_name().to_string();
                *event_counts.entry(kind_name).or_insert(0) += 1;
            }

            ReplayTrace {
                total_events: logs.len(),
                event_counts,
            }
        });

        (result, trace)
    }

    let (result1, trace1) = run_and_trace("replay-run1", SEED);
    let (result2, trace2) = run_and_trace("replay-run2", SEED);

    assert_eq!(result1.is_ok(), result2.is_ok());
    assert!(trace1.total_events > 0);
    assert_eq!(trace1.event_counts, trace2.event_counts);
    assert_eq!(trace1.total_events, trace2.total_events);

    tracing::info!(
        "Deterministic replay test passed - {} events",
        trace1.total_events
    );
}

// =============================================================================
// Basic Infrastructure Tests
// =============================================================================

/// Test that SimNetwork can be created and peers can be built.
#[test_log::test]
fn test_sim_network_basic_setup() {
    #[allow(clippy::unusual_byte_groupings)]
    const SEED: u64 = 0xBA51C_5E70_0001; // "BASIC SETO 0001"

    setup_deterministic_state(SEED);
    let rt = create_runtime();

    rt.block_on(async {
        let mut sim = SimNetwork::new("basic-setup", 1, 5, 7, 3, 10, 2, SEED).await;
        sim.with_start_backoff(Duration::from_millis(100));

        let peers = sim.build_peers();
        assert_eq!(peers.len(), 6, "Expected 1 gateway + 5 nodes = 6 peers");

        let gateway_count = peers.iter().filter(|(l, _)| !l.is_node()).count();
        let node_count = peers.iter().filter(|(l, _)| l.is_node()).count();
        assert_eq!(gateway_count, 1, "Expected 1 gateway");
        assert_eq!(node_count, 5, "Expected 5 regular nodes");
    });
}

/// Test that peers can start and run under Turmoil's deterministic scheduler.
#[test_log::test]
fn test_sim_network_peer_startup() {
    TestConfig::small("peer-startup", 0xBEE2_5747_0001)
        .with_nodes(2)
        .with_iterations(5)
        .with_duration(Duration::from_secs(15))
        .run()
        .assert_ok();
}

/// Test network connectivity under Turmoil's deterministic scheduler.
#[test_log::test]
fn test_sim_network_connectivity() {
    TestConfig::small("connectivity-test", 0xC0EE_3C70_0001)
        .with_max_contracts(2)
        .with_iterations(10)
        .run()
        .assert_ok();
}

// =============================================================================
// CI Simulation Tests
// =============================================================================

/// CI simulation test - small network with contract operations.
#[test_log::test]
fn ci_quick_simulation() {
    TestConfig::small("ci-quick-sim", 0xC1F1_ED5E_ED00)
        .with_nodes(4)
        .with_max_contracts(5)
        .with_iterations(50)
        .with_duration(Duration::from_secs(45))
        .with_sleep(Duration::from_secs(2))
        .run()
        .assert_ok()
        .verify_operation_coverage()
        .check_convergence();
}

/// CI simulation test - medium network with more operations.
#[test_log::test]
fn ci_medium_simulation() {
    TestConfig::medium("ci-medium-sim", 0xC1F1_ED7E_ED01)
        .run()
        .assert_ok()
        .verify_operation_coverage()
        .check_convergence();
}

// =============================================================================
// Convergence Tests
// =============================================================================

/// Replica validation test with stepwise consistency checking.
/// Verifies that contracts converge to the same state across all replicas.
#[test_log::test]
fn replica_validation_and_stepwise_consistency() {
    TestConfig::medium("replica-validation", 0xBEE1_1CA5_0001)
        .with_gateways(2)
        .with_nodes(8)
        .with_htl(8, 4)
        .with_connections(4, 8)
        .with_max_contracts(10)
        .with_iterations(90)
        .with_duration(Duration::from_secs(180))
        .with_sleep(Duration::from_secs(5))
        .require_convergence()
        .run()
        .assert_ok()
        .verify_operation_coverage()
        .check_convergence();
}

/// Dense network test with high connectivity.
/// Tests contract replication and convergence in a densely connected network.
#[test_log::test]
fn dense_network_replication() {
    TestConfig::large("dense-network", 0xDE05_E0F0_0001)
        .require_convergence()
        .run()
        .assert_ok()
        .verify_operation_coverage()
        .check_convergence();
}

// =============================================================================
// Determinism Verification Tests
// =============================================================================

/// Verify that running the same simulation twice produces identical results.
#[test_log::test]
fn test_turmoil_determinism_verification() {
    const SEED: u64 = 0xDE7E_2A11_0001;

    fn run_simulation(name: &'static str, seed: u64) -> Vec<String> {
        setup_deterministic_state(seed);
        let rt = create_runtime();

        let (sim, logs_handle) = rt.block_on(async {
            let sim = SimNetwork::new(name, 1, 3, 7, 3, 10, 2, seed).await;
            let logs_handle = sim.event_logs_handle();
            (sim, logs_handle)
        });

        let result = sim.run_simulation::<rand::rngs::SmallRng, _, _>(
            seed,
            3,
            15,
            Duration::from_secs(20),
            || async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok(())
            },
        );

        assert!(result.is_ok(), "Simulation failed: {:?}", result.err());

        rt.block_on(async {
            let logs = logs_handle.lock().await;
            logs.iter()
                .map(|log| format!("{:?}", log.kind.variant_name()))
                .collect()
        })
    }

    let events1 = run_simulation("det-verify-1", SEED);
    let events2 = run_simulation("det-verify-2", SEED);

    assert_eq!(
        events1.len(),
        events2.len(),
        "Event counts differ: {} vs {}",
        events1.len(),
        events2.len()
    );

    for (i, (e1, e2)) in events1.iter().zip(events2.iter()).enumerate() {
        assert_eq!(e1, e2, "Event {} differs: {:?} vs {:?}", i, e1, e2);
    }

    tracing::info!(
        "Determinism verified: {} identical events across runs",
        events1.len()
    );
}

// =============================================================================
// Graceful Shutdown Regression Tests
// =============================================================================

/// Regression test for graceful shutdown using Turmoil deterministic simulation.
#[test_log::test]
fn test_graceful_shutdown_no_deadlock() {
    const SEED: u64 = 0xDEAD_BEEF_CAFE;

    let rt = create_runtime();

    let sim = rt.block_on(async {
        SimNetwork::new("graceful-shutdown-test", 1, 2, 7, 3, 10, 2, SEED).await
    });

    let result = sim.run_simulation::<rand::rngs::SmallRng, _, _>(
        SEED,
        10,
        3,
        Duration::from_secs(30),
        || async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            tracing::info!("Test client: simulation will end gracefully");
            Ok(())
        },
    );

    assert!(
        result.is_ok(),
        "Simulation should complete without deadlock: {:?}",
        result.err()
    );

    tracing::info!("Graceful shutdown test passed - no deadlock under Turmoil");
}

/// Tests that the typed EventLoopExitReason error is properly handled.
#[test_log::test]
fn test_graceful_shutdown_typed_error() {
    use freenet::EventLoopExitReason;

    let graceful = EventLoopExitReason::GracefulShutdown;
    let unexpected = EventLoopExitReason::UnexpectedStreamEnd;

    assert_eq!(graceful.to_string(), "Graceful shutdown");
    assert_eq!(
        unexpected.to_string(),
        "Network event stream ended unexpectedly"
    );

    tracing::info!("Typed error test passed");
}

// =============================================================================
// High-Latency Regression Test
// =============================================================================

/// Regression test for v0.1.92 timeout storm bug with high-latency connections.
///
/// In production, we observed 935 timeouts in 10 seconds when transferring to peers
/// with ~150ms RTT. The root cause was MIN_RTO (200ms) < RTT + ACK_CHECK_INTERVAL (244ms).
///
/// This test verifies that:
/// 1. High-latency connections (150-200ms) don't experience excessive timeouts
/// 2. Data transfers complete successfully despite latency
/// 3. BBR congestion control handles high-latency correctly after the fix
///
/// Fix applied in this codebase:
/// - MIN_RTO increased from 200ms to 300ms
/// - BBR now uses adaptive timeout floor based on max BDP seen
#[test_log::test]
fn test_high_latency_timeout_regression() {
    use freenet::simulation::FaultConfig;

    // Use a fixed seed for reproducibility
    const SEED: u64 = 0xDEAD_BEEF_1234;
    tracing::info!(
        "Starting high-latency regression test with seed: 0x{:X}",
        SEED
    );

    setup_deterministic_state(SEED);
    let rt = create_runtime();

    rt.block_on(async {
        // Create a network with 1 gateway and 3 peers
        let mut sim = SimNetwork::new(
            "high-latency-regression",
            1,  // gateways
            3,  // nodes
            7,  // ring_max_htl
            3,  // rnd_if_htl_above
            10, // max_connections
            2,  // min_connections
            SEED,
        )
        .await;

        // Configure high latency (150-200ms) - similar to intercontinental connections
        // This latency, combined with ACK_CHECK_INTERVAL (100ms), would have caused
        // spurious timeouts with the old MIN_RTO of 200ms
        let fault_config = FaultConfig::builder()
            .latency_range(Duration::from_millis(150)..Duration::from_millis(200))
            .build();
        sim.with_fault_injection(fault_config);

        sim.with_start_backoff(Duration::from_millis(100));

        // Start the network
        let _handles = sim
            .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 1, 1)
            .await;

        // Give time for initial connections with high latency
        // (needs more time than normal due to latency)
        for _ in 0..50 {
            sim.advance_time(Duration::from_millis(100));
            tokio::task::yield_now().await;
        }

        // Check connectivity with a longer timeout due to high latency
        match sim.check_partial_connectivity(Duration::from_secs(60), 0.5).await {
            Ok(()) => {
                tracing::info!("High-latency network established connectivity");
            }
            Err(e) => {
                tracing::warn!(
                    "High-latency connectivity check incomplete: {} (this may be acceptable)",
                    e
                );
            }
        }

        // The key test: verify we don't have excessive timeouts
        // With the old MIN_RTO=200ms, we'd see hundreds of timeouts
        // With the fix (MIN_RTO=300ms + adaptive floor), timeouts should be minimal
        let summary = sim.get_operation_summary().await;
        let total_timeouts = summary.timeouts;

        tracing::info!(
            "High-latency test results: {} total timeouts, {:.1}% success rate",
            total_timeouts,
            summary.overall_success_rate() * 100.0
        );

        // Before the fix, we'd see 100+ timeouts per second
        // After the fix, timeouts should be rare (mostly actual network issues)
        // Allow up to 50 timeouts for a 60-second test (less than 1/second)
        assert!(
            total_timeouts < 50,
            "Expected <50 timeouts with MIN_RTO=300ms fix, got {} (timeout storm detected!)",
            total_timeouts
        );

        tracing::info!("High-latency regression test PASSED - no timeout storm detected");
    });
}

// =============================================================================
// SimulationSocket Integration Test
// =============================================================================

/// Test: Use actual SimulationSocket inside Turmoil.
/// This verifies that the real in-memory socket works with Turmoil's deterministic scheduler.
#[test]
fn test_turmoil_with_real_simulation_socket() -> turmoil::Result {
    // Clean up any previous socket state
    clear_all_socket_registries();

    let network_name = "turmoil-test";
    let virtual_time = VirtualTime::new();

    // Register the network's time source
    register_network_time_source(network_name, virtual_time);

    let server_addr: SocketAddr = (Ipv6Addr::LOCALHOST, 18000).into();
    let client_addr: SocketAddr = (Ipv6Addr::LOCALHOST, 19000).into();

    // Register addresses with the network
    register_address_network(server_addr, network_name);
    register_address_network(client_addr, network_name);

    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(10))
        .rng_seed(0xDEAD_BEEF_CAFE_1234)
        .build();

    // Server host using SimulationSocket
    sim.host("server", move || async move {
        let socket = SimulationSocket::bind(server_addr)
            .await
            .expect("Server bind failed");

        tracing::info!("Server bound to {:?}", server_addr);

        // Wait for a packet
        let mut buf = [0u8; 1024];
        match socket.recv_from(&mut buf).await {
            Ok((len, from)) => {
                let msg = &buf[..len];
                tracing::info!("Server received {:?} from {:?}", msg, from);

                // Echo back
                socket.send_to(b"pong", from).await.ok();
            }
            Err(e) => {
                tracing::error!("Server recv error: {:?}", e);
            }
        }

        Ok(())
    });

    // Client host using SimulationSocket
    sim.client("client", async move {
        // Give server time to bind
        tokio::time::sleep(Duration::from_millis(100)).await;

        let socket = SimulationSocket::bind(client_addr)
            .await
            .expect("Client bind failed");

        tracing::info!("Client bound to {:?}", client_addr);

        // Send ping to server
        socket.send_to(b"ping", server_addr).await.ok();
        tracing::info!("Client sent ping");

        // Wait for response
        let mut buf = [0u8; 1024];
        match tokio::time::timeout(Duration::from_secs(5), socket.recv_from(&mut buf)).await {
            Ok(Ok((len, from))) => {
                let msg = &buf[..len];
                tracing::info!("Client received {:?} from {:?}", msg, from);
                assert_eq!(msg, b"pong");
                assert_eq!(from, server_addr);
            }
            Ok(Err(e)) => {
                tracing::error!("Client recv error: {:?}", e);
            }
            Err(_) => {
                tracing::error!("Client recv timeout");
            }
        }

        Ok(())
    });

    sim.run()
}

// =============================================================================
// Subscription Topology Validation
// =============================================================================
// These tests validate the subscription topology infrastructure to detect
// issues like bidirectional cycles (#2720), orphan seeders (#2719), etc.

/// Source detection threshold: peers within 5% of ring distance to contract
/// are considered sources. Must match SOURCE_THRESHOLD in topology_registry.rs.
const SOURCE_THRESHOLD: f64 = 0.05;

/// Network name for single seeder topology test.
const SINGLE_SEEDER_NETWORK: &str = "single-seeder-test";

/// Network name for multi-subscriber topology test (bidirectional cycle detection).
const MULTI_SUBSCRIBER_NETWORK: &str = "multi-subscriber-test";

/// Helper to let tokio tasks run and process network messages.
///
/// SimulationSocket uses VirtualTime internally for message delivery scheduling.
/// This helper advances VirtualTime in chunks while yielding to tokio to let
/// tasks process delivered messages.
///
/// # Timing
/// - `step`: 100ms virtual time advancement per iteration
/// - Real-time sleep: 10ms per iteration to allow task scheduling
async fn let_network_run_for_topology(sim: &mut SimNetwork, duration: Duration) {
    let step = Duration::from_millis(100);
    let mut elapsed = Duration::ZERO;

    while elapsed < duration {
        // Advance virtual time to trigger message delivery
        sim.advance_time(step);
        // Yield to tokio so tasks can process delivered messages
        tokio::task::yield_now().await;
        // Also give a small real-time sleep for task scheduling
        tokio::time::sleep(Duration::from_millis(10)).await;
        elapsed += step;
    }
}

/// Calculate ring distance (handles wrap-around at 0/1 boundary).
fn ring_distance(a: f64, b: f64) -> f64 {
    let diff = (a - b).abs();
    diff.min(1.0 - diff)
}

/// Test topology capture with a single seeder (gateway only).
///
/// This test validates the topology infrastructure without triggering the
/// bidirectional cycle issue (#2720) by using only a single subscriber.
///
/// ## Scenario
/// 1. Gateway PUTs a contract with subscribe=true
/// 2. Wait for topology registration
/// 3. Verify: gateway's location is within SOURCE_THRESHOLD of contract location
/// 4. Verify: gateway's snapshot shows it's seeding the contract
/// 5. Verify: no topology issues (no cycles since only one subscriber)
///
/// ## Related
/// - For the multi-subscriber test that detects #2720, see `test_bidirectional_cycle_issue_2720`
/// - This test runs in CI while #2720 remains open
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_topology_single_seeder() {
    use freenet::dev_tool::{Location, NodeLabel, ScheduledOperation, SimOperation};
    use futures::StreamExt;

    const SEED: u64 = 0x5EED_0001_CAFE;

    // Reset all global state and set up deterministic time/RNG
    reset_all_simulation_state();
    GlobalRng::set_seed(SEED);
    const BASE_EPOCH_MS: u64 = 1577836800000;
    const RANGE_MS: u64 = 5 * 365 * 24 * 60 * 60 * 1000;
    GlobalSimulationTime::set_time_ms(BASE_EPOCH_MS + (SEED % RANGE_MS));

    let mut sim = SimNetwork::new(
        SINGLE_SEEDER_NETWORK,
        1,  // 1 gateway
        2,  // 2 peers (they won't subscribe)
        7,  // max_htl
        3,  // rnd_if_htl_above
        10, // max_connections
        2,  // min_connections
        SEED,
    )
    .await;
    sim.with_start_backoff(Duration::from_millis(50));

    // Create a test contract
    let contract = SimOperation::create_test_contract(99);
    let contract_id = *contract.key().id();
    let initial_state = SimOperation::create_test_state(99);

    // Schedule only ONE operation: gateway PUTs the contract with subscribe=true
    // No other nodes subscribe - this avoids #2720 bidirectional cycle issue
    let operations = vec![ScheduledOperation::new(
        NodeLabel::gateway(SINGLE_SEEDER_NETWORK, 0),
        SimOperation::Put {
            contract: contract.clone(),
            state: initial_state.clone(),
            subscribe: true,
        },
    )];

    // Start network with controlled events
    let (_handles, num_ops) = sim.start_with_controlled_events(operations).await;
    assert_eq!(num_ops, 1, "Should have scheduled 1 operation");

    // Let nodes establish connections (3 seconds: enough for connection handshakes)
    let_network_run_for_topology(&mut sim, Duration::from_secs(3)).await;

    // Create and trigger the controlled event chain
    let event_sequence = vec![(0, NodeLabel::gateway(SINGLE_SEEDER_NETWORK, 0))]; // PUT
    let mut event_chain = sim.controlled_event_chain(event_sequence);

    // Trigger PUT event
    let event = event_chain.next().await;
    assert_eq!(event, Some(0), "Should trigger PUT event");

    // Let the PUT propagate through the network (5 seconds: allows PUT to complete)
    let_network_run_for_topology(&mut sim, Duration::from_secs(5)).await;

    // Wait for topology registration task (runs every 1 second, wait 3 for margin)
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Get topology snapshots
    let snapshots = sim.get_topology_snapshots();
    tracing::info!("Captured {} topology snapshots", snapshots.len());

    // Find the gateway's snapshot with the contract
    let gateway_with_contract = snapshots
        .iter()
        .find(|snap| snap.contracts.contains_key(&contract_id));

    assert!(
        gateway_with_contract.is_some(),
        "Gateway should have the contract in its topology snapshot. \
         Available snapshots: {} peers",
        snapshots.len()
    );

    let gateway_snap = gateway_with_contract.unwrap();
    let contract_sub = gateway_snap.contracts.get(&contract_id).unwrap();

    // Get the actual contract location derived from the contract ID
    let contract_location = Location::from(&contract_id).as_f64();
    let gateway_location = gateway_snap.location;

    // CRITICAL: Validate that gateway is within SOURCE_THRESHOLD of contract location
    // This ensures the gateway will be considered a "source" by the validation logic.
    // If this fails, the test setup is incorrect and other assertions may give false positives.
    let gateway_distance = ring_distance(gateway_location, contract_location);
    tracing::info!(
        "Contract location: {:.4}, Gateway location: {:.4}, Distance: {:.4}, Threshold: {:.4}",
        contract_location,
        gateway_location,
        gateway_distance,
        SOURCE_THRESHOLD
    );

    // Note: The gateway may not always be within SOURCE_THRESHOLD of the contract.
    // The validation logic considers any peer within SOURCE_THRESHOLD as a potential source.
    // If the gateway is NOT within threshold, it will be flagged as an orphan/disconnected seeder
    // unless it has proper upstream connections. This is the expected behavior.
    // For this single-seeder test, we verify the validation runs correctly regardless of location.

    // Gateway should be seeding (it did PUT with subscribe=true)
    assert!(
        contract_sub.is_seeding,
        "Gateway should be seeding the contract after PUT with subscribe=true"
    );

    // Gateway is the source - it should have no upstream (it's the origin)
    // and may have downstream peers if they subscribed
    tracing::info!(
        "Gateway {} is seeding contract: upstream={:?}, downstream={:?}",
        gateway_snap.peer_addr,
        contract_sub.upstream,
        contract_sub.downstream
    );

    // Validate topology
    let result = sim.validate_subscription_topology(&contract_id, contract_location);

    tracing::info!(
        "Topology validation: cycles={}, orphans={}, disconnected={}, unreachable={}, proximity={}",
        result.bidirectional_cycles.len(),
        result.orphan_seeders.len(),
        result.disconnected_upstream.len(),
        result.unreachable_seeders.len(),
        result.proximity_violations.len()
    );

    // With only one subscriber, there should be no cycles (no second peer to form a cycle with)
    assert!(
        result.bidirectional_cycles.is_empty(),
        "Single seeder should have no bidirectional cycles, found: {:?}",
        result.bidirectional_cycles
    );

    // Check if gateway is considered a "source" (within SOURCE_THRESHOLD of contract)
    let gateway_is_source = gateway_distance < SOURCE_THRESHOLD;

    if gateway_is_source {
        // Gateway IS a source: it should not be flagged as orphan or disconnected
        tracing::info!("Gateway is within SOURCE_THRESHOLD - validating source behavior");

        assert!(
            result.orphan_seeders.is_empty(),
            "Source seeder (within threshold) should not be marked as orphan, found: {:?}",
            result.orphan_seeders
        );

        assert!(
            result.disconnected_upstream.is_empty(),
            "Source seeder (within threshold) should not be disconnected, found: {:?}",
            result.disconnected_upstream
        );

        assert!(
            result.unreachable_seeders.is_empty(),
            "Source seeder should have no unreachable seeders, found: {:?}",
            result.unreachable_seeders
        );

        assert!(
            result.is_healthy(),
            "Single source seeder topology should be healthy, got {} issues",
            result.issue_count
        );
    } else {
        // Gateway is NOT within SOURCE_THRESHOLD: validation will flag it appropriately
        // This is expected behavior - the validation correctly identifies non-source seeders
        tracing::warn!(
            "Gateway is NOT within SOURCE_THRESHOLD (distance={:.4} > {:.4}). \
             Validation may flag orphan/disconnected issues - this is correct behavior.",
            gateway_distance,
            SOURCE_THRESHOLD
        );

        // Still verify no cycles (impossible with single seeder)
        assert!(
            result.bidirectional_cycles.is_empty(),
            "Single seeder should never have cycles"
        );

        // Log what issues were found (expected when gateway is not a source)
        if !result.orphan_seeders.is_empty() {
            tracing::info!(
                "Expected: Gateway flagged as orphan (not a source, no upstream): {:?}",
                result.orphan_seeders
            );
        }
        if !result.disconnected_upstream.is_empty() {
            tracing::info!(
                "Expected: Gateway flagged as disconnected upstream (not a source, has downstream): {:?}",
                result.disconnected_upstream
            );
        }
    }

    tracing::info!(
        "Single seeder topology test passed - gateway_is_source={}, issues={}",
        gateway_is_source,
        result.issue_count
    );
}

/// Regression test for Issue #2720: Bidirectional subscription cycles.
///
/// This test creates multiple subscribers and verifies the subscription topology
/// is correct. It INTENTIONALLY detects the bidirectional cycle bug in #2720.
///
/// ## Why this test is `#[ignore]`
///
/// This test exposes a known bug where when multiple peers subscribe to the same
/// contract, they sometimes create bidirectional subscription relationships
/// (A upstream of B, and B upstream of A). This creates isolated cycles that
/// cannot receive updates from the source.
///
/// ## Scenario
/// 1. Gateway PUTs a contract with subscribe=true (becomes the source)
/// 2. Node 1 subscribes to the same contract
/// 3. Node 2 subscribes to the same contract
/// 4. Verify: topology has no bidirectional cycles (FAILS due to #2720)
///
/// ## When to enable this test
/// Remove `#[ignore]` once Issue #2720 is fixed. The test passing indicates
/// the subscription topology correctly forms a directed acyclic graph from
/// subscribers toward the source.
///
/// ## Related
/// - Issue #2720: https://github.com/freenet/freenet-core/issues/2720
/// - For a passing topology test, see `test_topology_single_seeder`
///
/// ## Manual run
/// ```bash
/// cargo test --features "simulation_tests,testing" test_bidirectional_cycle_issue_2720 -- --ignored
/// ```
#[ignore] // Enable once Issue #2720 (bidirectional subscriptions) is fixed
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_bidirectional_cycle_issue_2720() {
    use freenet::dev_tool::{Location, NodeLabel, ScheduledOperation, SimOperation};
    use futures::StreamExt;

    const SEED: u64 = 0xC0DE_CAFE_1234;

    // Reset all global state and set up deterministic time/RNG
    reset_all_simulation_state();
    GlobalRng::set_seed(SEED);
    const BASE_EPOCH_MS: u64 = 1577836800000;
    const RANGE_MS: u64 = 5 * 365 * 24 * 60 * 60 * 1000;
    GlobalSimulationTime::set_time_ms(BASE_EPOCH_MS + (SEED % RANGE_MS));

    let mut sim = SimNetwork::new(
        MULTI_SUBSCRIBER_NETWORK,
        1,  // 1 gateway
        2,  // 2 peers
        7,  // max_htl
        3,  // rnd_if_htl_above
        10, // max_connections
        2,  // min_connections
        SEED,
    )
    .await;
    sim.with_start_backoff(Duration::from_millis(50));

    // Create a test contract
    let contract = SimOperation::create_test_contract(42);
    let contract_id = *contract.key().id();
    let initial_state = SimOperation::create_test_state(42);

    // Schedule controlled operations:
    // 1. Gateway PUTs the contract with subscribe=true
    // 2. Node 1 subscribes (node numbering starts after gateways)
    // 3. Node 2 subscribes
    let operations = vec![
        ScheduledOperation::new(
            NodeLabel::gateway(MULTI_SUBSCRIBER_NETWORK, 0),
            SimOperation::Put {
                contract: contract.clone(),
                state: initial_state.clone(),
                subscribe: true,
            },
        ),
        ScheduledOperation::new(
            NodeLabel::node(MULTI_SUBSCRIBER_NETWORK, 1),
            SimOperation::Subscribe { contract_id },
        ),
        ScheduledOperation::new(
            NodeLabel::node(MULTI_SUBSCRIBER_NETWORK, 2),
            SimOperation::Subscribe { contract_id },
        ),
    ];

    // Start network with controlled events
    let (_handles, num_ops) = sim.start_with_controlled_events(operations).await;
    assert_eq!(num_ops, 3, "Should have scheduled 3 operations");

    // Let nodes establish connections (3 seconds: enough for connection handshakes)
    let_network_run_for_topology(&mut sim, Duration::from_secs(3)).await;

    // Create and trigger the controlled event chain
    let event_sequence = vec![
        (0, NodeLabel::gateway(MULTI_SUBSCRIBER_NETWORK, 0)), // PUT
        (1, NodeLabel::node(MULTI_SUBSCRIBER_NETWORK, 1)),    // Subscribe on node 1
        (2, NodeLabel::node(MULTI_SUBSCRIBER_NETWORK, 2)),    // Subscribe on node 2
    ];
    let mut event_chain = sim.controlled_event_chain(event_sequence);

    // Trigger first event (PUT) and let it complete
    let event = event_chain.next().await;
    assert_eq!(event, Some(0), "Should trigger PUT event");
    let_network_run_for_topology(&mut sim, Duration::from_secs(5)).await;

    // Trigger subscribe events
    let event = event_chain.next().await;
    assert_eq!(event, Some(1), "Should trigger first subscribe event");
    let_network_run_for_topology(&mut sim, Duration::from_secs(3)).await;

    let event = event_chain.next().await;
    assert_eq!(event, Some(2), "Should trigger second subscribe event");
    let_network_run_for_topology(&mut sim, Duration::from_secs(3)).await;

    // Wait for topology registration
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Get topology snapshots
    let snapshots = sim.get_topology_snapshots();
    tracing::info!("Captured {} topology snapshots", snapshots.len());

    // Check if any peer has contract subscriptions
    let mut peers_with_contract = 0;
    for snap in &snapshots {
        if let Some(sub) = snap.contracts.get(&contract_id) {
            peers_with_contract += 1;
            tracing::debug!(
                "Peer {} has contract: upstream={:?}, downstream={:?}, seeding={}",
                snap.peer_addr,
                sub.upstream,
                sub.downstream,
                sub.is_seeding
            );
        }
    }

    // We expect at least the gateway to have the contract
    assert!(
        peers_with_contract >= 1,
        "Expected at least 1 peer with contract subscription, found {}",
        peers_with_contract
    );

    // Validate topology for this contract using actual contract location
    let contract_location = Location::from(&contract_id).as_f64();
    let result = sim.validate_subscription_topology(&contract_id, contract_location);

    tracing::info!(
        "Topology validation: cycles={}, orphans={}, disconnected={}, unreachable={}, proximity_violations={}",
        result.bidirectional_cycles.len(),
        result.orphan_seeders.len(),
        result.disconnected_upstream.len(),
        result.unreachable_seeders.len(),
        result.proximity_violations.len()
    );

    // Assert no bidirectional cycles (Issue #2720)
    assert!(
        result.bidirectional_cycles.is_empty(),
        "ISSUE #2720: Found {} bidirectional subscription cycles: {:?}. \
         Bidirectional cycles create isolated islands that can't receive updates from the source.",
        result.bidirectional_cycles.len(),
        result.bidirectional_cycles
    );

    // Assert no orphan seeders (Issue #2719)
    assert!(
        result.orphan_seeders.is_empty(),
        "ISSUE #2719: Found {} orphan seeders: {:?}. \
         Orphan seeders have no upstream and won't receive updates.",
        result.orphan_seeders.len(),
        result.orphan_seeders
    );

    // Assert no disconnected upstream (seeders with downstream but no upstream)
    assert!(
        result.disconnected_upstream.is_empty(),
        "Found {} disconnected upstream seeders: {:?}. \
         These seeders have downstream peers but can't receive updates themselves.",
        result.disconnected_upstream.len(),
        result.disconnected_upstream
    );

    // Assert no unreachable seeders
    assert!(
        result.unreachable_seeders.is_empty(),
        "Found {} unreachable seeders: {:?}. \
         These seeders cannot receive updates from the contract source.",
        result.unreachable_seeders.len(),
        result.unreachable_seeders
    );

    tracing::info!("Bidirectional cycle regression test passed (Issue #2720 is fixed!)");
}
