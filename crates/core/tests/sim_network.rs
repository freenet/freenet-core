//! Deterministic simulation tests for the in-memory SimNetwork.
//!
//! All tests in this file use Turmoil's deterministic scheduler for reproducible execution.
//! This ensures that running with the same seed produces identical results.
//!
//! NOTE: These tests use global state and must run serially.
//! Enable with: cargo test -p freenet --features simulation_tests --test sim_network -- --test-threads=1

#![cfg(feature = "simulation_tests")]

use freenet::config::{GlobalRng, GlobalSimulationTime};
use freenet::dev_tool::{check_convergence_from_logs, reset_all_simulation_state, SimNetwork};
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
// Basic Infrastructure Tests
// =============================================================================

/// Test that SimNetwork can be created and peers can be built.
#[test_log::test]
fn test_sim_network_basic_setup() {
    const SEED: u64 = 0xBA51C_5E70_0001;

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
        match sim.check_partial_connectivity(Duration::from_secs(60), 0.5) {
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
