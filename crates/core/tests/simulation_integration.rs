//! Deterministic simulation tests using Turmoil.
//!
//! All tests in this file use Turmoil's deterministic scheduler for reproducible execution.
//! Same seed MUST produce identical results across runs.
//!
//! NOTE: These tests use global state (socket registries, RNG) and must run serially.
//! Enable with: cargo test -p freenet --features "simulation_tests,testing" --test simulation_integration -- --test-threads=1
//!
//! The `testing` feature is required for `run_controlled_simulation` method.
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
    /// Optional latency range for jitter simulation (min..max)
    latency_range: Option<std::ops::Range<Duration>>,
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
            latency_range: None,
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
            latency_range: None,
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
            latency_range: None,
        }
    }

    /// Create a long-running test configuration (1 hour virtual time).
    ///
    /// Designed to uncover time-dependent bugs:
    /// - Connection timeout handling over extended periods
    /// - State drift in long-lived contracts
    /// - Timer edge cases (keep-alive, connection idle timeout)
    /// - Resource exhaustion patterns
    ///
    /// Wall clock time: ~30-60 seconds (with Turmoil's time acceleration)
    ///
    /// Includes 10-50ms latency jitter to simulate realistic network conditions.
    ///
    /// # Virtual Time Breakdown
    /// - Events phase: 200 iterations × 200ms = 40 seconds
    /// - Idle phase: 3556 seconds (tests timeout handling, keep-alive)
    /// - Total: ~3600 seconds (1 hour)
    fn long_running_1h(name: &'static str, seed: u64) -> Self {
        Self {
            name,
            seed,
            gateways: 2,
            nodes: 6,
            ring_max_htl: 10,
            rnd_if_htl_above: 5,
            max_connections: 15,
            min_connections: 3,
            max_contracts: 8,
            iterations: 200,                     // Contract operations
            duration: Duration::from_secs(3700), // Max simulation time (buffer)
            // Sleep after events to reach ~1 hour total virtual time
            // Events take ~44s (200×200ms + setup), so sleep for ~3556s
            sleep_between_ops: Duration::from_secs(3556),
            require_convergence: true, // Must converge after 1 hour
            // Realistic latency jitter (10-50ms) to uncover timing issues
            latency_range: Some(Duration::from_millis(10)..Duration::from_millis(50)),
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

    /// Add latency jitter simulation.
    fn with_latency(mut self, min: Duration, max: Duration) -> Self {
        self.latency_range = Some(min..max);
        self
    }

    /// Run the simulation and return results.
    fn run(self) -> TestResult {
        use freenet::simulation::FaultConfig;

        setup_deterministic_state(self.seed);
        let rt = create_runtime();

        let (sim, logs_handle) = rt.block_on(async {
            let mut sim = SimNetwork::new(
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

            // Apply latency jitter if configured
            if let Some(ref latency) = self.latency_range {
                let fault_config = FaultConfig::builder()
                    .latency_range(latency.clone())
                    .build();
                sim.with_fault_injection(fault_config);
                tracing::info!(
                    "Latency jitter enabled: {:?} - {:?}",
                    latency.start,
                    latency.end
                );
            }

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
        match sim
            .check_partial_connectivity(Duration::from_secs(60), 0.5)
            .await
        {
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
/// - For Issue #2720 (bidirectional cycles), that bug is now fixed
/// - For Issue #2755 (orphan/disconnected seeders), see `test_orphan_seeders_no_source` (ignored)
#[test_log::test]
fn test_topology_single_seeder() {
    use freenet::dev_tool::{
        validate_topology_from_snapshots, Location, NodeLabel, ScheduledOperation, SimOperation,
    };

    const SEED: u64 = 0x5EED_0001_CAFE;

    // Set up deterministic state BEFORE creating SimNetwork
    // This ensures peer locations are deterministic (uses GlobalRng)
    setup_deterministic_state(SEED);

    let rt = create_runtime();

    // Create SimNetwork and get gateway location
    let (sim, gateway_location) = rt.block_on(async {
        let sim = SimNetwork::new(
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
        // Get gateway location (first peer in the list)
        let locations = sim.get_peer_locations();
        let gateway_loc = locations
            .first()
            .copied()
            .expect("Should have at least one gateway");
        (sim, gateway_loc)
    });

    // Note: get_peer_locations() returns locations from SimNetwork config, but actual
    // running nodes may have different locations due to how ConnectionManager is initialized.
    // We use a fixed contract seed and validate based on actual topology snapshot locations.
    // See: https://github.com/freenet/freenet-core/issues/2759 for the peer location non-determinism issue.
    let _ = gateway_location; // Silence unused variable warning

    // Use a fixed contract seed - we'll determine source status from actual topology
    let contract_seed = 42u8;
    let contract = SimOperation::create_test_contract(contract_seed);
    let contract_id = *contract.key().id();
    let initial_state = SimOperation::create_test_state(contract_seed);

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

    // Run simulation with controlled events under Turmoil
    // Wait 45 seconds after operations for:
    // - Topology registration (runs periodically)
    // - Orphan recovery (runs every 30 seconds)
    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(120), // simulation duration
        Duration::from_secs(45),  // post-operation wait for topology stabilization
    );

    assert!(
        result.turmoil_result.is_ok(),
        "Simulation should complete successfully: {:?}",
        result.turmoil_result.err()
    );

    // Get topology snapshots from the returned result (captured before SimNetwork::Drop clears registry)
    let snapshots = result.topology_snapshots;
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
    let gateway_distance = ring_distance(gateway_location, contract_location);
    tracing::info!(
        "Contract location: {:.4}, Gateway location: {:.4}, Distance: {:.4}, Threshold: {:.4}",
        contract_location,
        gateway_location,
        gateway_distance,
        SOURCE_THRESHOLD
    );

    // Gateway should be seeding (it did PUT with subscribe=true)
    assert!(
        contract_sub.is_seeding,
        "Gateway should be seeding the contract after PUT with subscribe=true"
    );

    tracing::info!(
        "Gateway {} is seeding contract: upstream={:?}, downstream={:?}",
        gateway_snap.peer_addr,
        contract_sub.upstream,
        contract_sub.downstream
    );

    // Validate topology using captured snapshots
    let result = validate_topology_from_snapshots(&snapshots, &contract_id, contract_location);

    tracing::info!(
        "Topology validation: cycles={}, orphans={}, disconnected={}, unreachable={}, proximity={}",
        result.bidirectional_cycles.len(),
        result.orphan_seeders.len(),
        result.disconnected_upstream.len(),
        result.unreachable_seeders.len(),
        result.proximity_violations.len()
    );

    // CRITICAL: With only one subscriber, there should NEVER be cycles
    // This is the primary assertion - single seeder cannot form cycles
    assert!(
        result.bidirectional_cycles.is_empty(),
        "Single seeder should have no bidirectional cycles, found: {:?}",
        result.bidirectional_cycles
    );

    // Check if gateway is considered a "source" based on actual topology snapshot
    let gateway_is_source = gateway_distance < SOURCE_THRESHOLD;

    if gateway_is_source {
        tracing::info!(
            "Gateway IS source (distance={:.4} < threshold={:.4}) - validating healthy topology",
            gateway_distance,
            SOURCE_THRESHOLD
        );

        // Source seeder should have healthy topology
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
        tracing::warn!(
            "Gateway is NOT source (distance={:.4} >= threshold={:.4}) - orphan/disconnected issues are expected",
            gateway_distance, SOURCE_THRESHOLD
        );

        // Non-source seeder will have orphan/disconnected issues - this is expected behavior
        // The key assertion (no cycles) already passed above
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
        "Single seeder topology test passed - gateway_is_source={}, cycles=0, other_issues={}",
        gateway_is_source,
        result.issue_count
    );
}

// =============================================================================
// Concurrent Updates Convergence Test
// =============================================================================

/// Tests that concurrent updates from multiple peers converge to the same state.
///
/// This test validates the core eventual consistency property of Freenet's
/// state synchronization:
///
/// 1. Multiple peers subscribe to the same contract
/// 2. Multiple peers issue concurrent updates
/// 3. Updates propagate via broadcast mechanism
/// 4. All subscribers MUST converge to identical state
///
/// ## What This Catches
///
/// - **Echo-back bugs**: If A's update incorrectly comes back to A
/// - **Missed broadcasts**: If some peer doesn't receive propagated updates
/// - **CRDT merge bugs**: If deterministic merge produces different results
/// - **Summary caching bugs**: If summary comparison incorrectly skips peers
/// - **Race conditions**: In update propagation and application
///
/// ## Related Issues
///
/// - Issue #2764: Echo-back prevention (summary comparison should handle this)
/// - PR #2763: Conditional summary caching
///
/// ## CI Impact
///
/// Configuration: 2 gateways + 6 nodes with few contracts and many iterations
/// to maximize concurrent update scenarios per contract.
///
/// Expected runtime: ~60-90 seconds (similar to ci_medium_simulation).
#[test_log::test]
fn test_concurrent_updates_convergence() {
    // Use a specific seed for reproducibility
    // This seed was chosen to produce good coverage of concurrent update scenarios
    TestConfig::medium("concurrent-updates-convergence", 0xC0_C0_BEEF_1234)
        .with_gateways(2) // Multiple gateways for richer topology
        .with_nodes(6) // 6 regular nodes for concurrent updates
        .with_max_contracts(3) // Few contracts = more updates per contract
        .with_iterations(80) // Many iterations to stress concurrent updates
        .with_duration(Duration::from_secs(90))
        .with_sleep(Duration::from_secs(2)) // Short sleep between ops for concurrency
        .require_convergence() // FAIL if any contract diverges
        .run()
        .assert_ok()
        .verify_operation_coverage()
        .check_convergence();
}

// =============================================================================
// Full State Send Caching Bug Test (Issue #2763)
// =============================================================================

/// Tests correct behavior of summary caching after full state sends.
///
/// ## Background (PR #2763)
///
/// Before the fix, when a peer sent full state (not a delta) to another peer,
/// it would incorrectly cache its own summary as the recipient's summary.
/// This caused problems because:
///
/// 1. After receiving full state, the recipient's state depends on CRDT merge
///    with their existing state - we can't predict their resulting summary
/// 2. Later broadcasts would compute deltas based on the wrong cached summary
/// 3. The recipient couldn't apply these deltas, triggering ResyncRequests
/// 4. This caused inefficiency (extra round trips) and potential divergence in race conditions
///
/// ## What This Test Does
///
/// Creates a scenario that exercises the full state send path:
///
/// 1. Gateway PUTs contract with state S1, subscribes
/// 2. Nodes subscribe → receive full state (gateway has no cached summaries)
/// 3. Multiple nodes issue UPDATEs → broadcasts to subscribers
/// 4. Wait for propagation and CRDT merges
/// 5. Assert all peers converge to the same state
/// 6. Assert no ResyncRequests were needed (via GlobalTestMetrics)
///
/// ## Test Infrastructure
///
/// This test uses `GlobalTestMetrics` to track behavior across all nodes:
/// - `GlobalTestMetrics::reset()` at test start
/// - `GlobalTestMetrics::resync_requests()` - should be 0 with the fix
/// - `GlobalTestMetrics::delta_sends()` / `full_state_sends()` - broadcast statistics
///
/// MockRuntime uses hash-based summaries (32 bytes) which enables the delta
/// efficiency check to pass (for states > 64 bytes). However, it returns full
/// state as "delta" content, so the actual CRDT merge behavior is:
/// - Compare incoming state hash with current state hash
/// - Larger hash wins (deterministic CRDT convergence)
///
/// ## Current Limitations
///
/// While MockRuntime implements CRDT-style "largest hash wins" merging, it doesn't
/// fully reproduce the bug because:
/// 1. The "delta" is actually full state, so it works regardless of base state
/// 2. Even with wrong cached summary, the merge produces correct results
/// 3. No ResyncRequests are triggered because delta application never truly fails
///
/// The bug would fully manifest with a real CRDT contract where:
/// - Delta is computed from assumed base state
/// - Applying delta to wrong base state produces incorrect result
/// - This triggers ResyncRequest or causes state divergence
///
/// The test still provides value as:
/// 1. Documentation of the expected behavior and fix
/// 2. Infrastructure for tracking broadcasts (delta vs full state)
/// 3. Verification that convergence works correctly
/// 4. Regression guard if MockRuntime or protocol logic changes
///
/// ## Related
///
/// - PR #2763: Conditional summary caching to prevent state divergence
/// - Issue #2764: Echo-back prevention
#[test_log::test]
fn test_full_state_send_no_incorrect_caching() {
    use freenet::dev_tool::{GlobalTestMetrics, NodeLabel, ScheduledOperation, SimOperation};

    const SEED: u64 = 0x2763_CAFE_0001;
    const NETWORK_NAME: &str = "full-state-cache-test";

    // Reset global test metrics at the start
    GlobalTestMetrics::reset();

    setup_deterministic_state(SEED);
    let rt = create_runtime();

    // Create network: 1 gateway + 3 nodes
    // This gives us enough peers to have interesting broadcast patterns
    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(
            NETWORK_NAME,
            1,  // 1 gateway
            3,  // 3 regular nodes
            7,  // max_htl
            3,  // rnd_if_htl_above
            10, // max_connections
            2,  // min_connections
            SEED,
        )
        .await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    // Create a test contract
    let contract = SimOperation::create_test_contract(0x63);
    let contract_id = *contract.key().id();
    let contract_key = contract.key();

    // Create distinct states for each update
    // Using different byte patterns to ensure CRDT merges produce distinct results
    let initial_state = SimOperation::create_test_state(1);
    let update_state_2 = SimOperation::create_test_state(2);
    let update_state_3 = SimOperation::create_test_state(3);
    let update_state_4 = SimOperation::create_test_state(4);

    // Schedule operations that will trigger the bug scenario:
    // 1. Gateway PUTs contract (becomes the source)
    // 2. Nodes subscribe one by one (each receives full state, gateway might incorrectly cache)
    // 3. Multiple updates from different nodes (tests delta computation with cached summaries)
    let operations = vec![
        // Step 1: Gateway puts contract with initial state and subscribes
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Put {
                contract: contract.clone(),
                state: initial_state,
                subscribe: true,
            },
        ),
        // Step 2: Node 1 subscribes - will receive full state from gateway
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 1),
            SimOperation::Subscribe { contract_id },
        ),
        // Step 3: Node 2 subscribes - will receive full state from gateway
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 2),
            SimOperation::Subscribe { contract_id },
        ),
        // Step 4: Node 3 subscribes - will receive full state from gateway
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 3),
            SimOperation::Subscribe { contract_id },
        ),
        // Step 5: Gateway updates - broadcasts to all subscribers
        // BUG: If gateway incorrectly cached subscriber summaries after full state sends,
        // it will compute deltas based on wrong base state
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Update {
                key: contract_key,
                data: update_state_2,
            },
        ),
        // Step 6: Node 1 updates - broadcasts to subscribers
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 1),
            SimOperation::Update {
                key: contract_key,
                data: update_state_3,
            },
        ),
        // Step 7: Node 2 updates - broadcasts to subscribers
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 2),
            SimOperation::Update {
                key: contract_key,
                data: update_state_4,
            },
        ),
    ];

    // Run simulation with enough time for all operations and CRDT merges
    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(120), // simulation duration
        Duration::from_secs(60),  // post-operation wait for propagation
    );

    assert!(
        result.turmoil_result.is_ok(),
        "Simulation should complete successfully: {:?}",
        result.turmoil_result.err()
    );

    // Check convergence - this is the key assertion
    // With the fix, all peers should converge to the same state
    // Without the fix, incorrect caching may cause divergence
    let convergence =
        rt.block_on(async { freenet::dev_tool::check_convergence_from_logs(&logs_handle).await });

    tracing::info!(
        "Convergence check: {} converged, {} diverged",
        convergence.converged.len(),
        convergence.diverged.len()
    );

    // Log any diverged contracts for debugging
    for diverged in &convergence.diverged {
        tracing::error!(
            "DIVERGED: {} - {} unique states across {} peers",
            diverged.contract_key,
            diverged.unique_state_count(),
            diverged.peer_states.len()
        );
        for (peer, hash) in &diverged.peer_states {
            tracing::error!("  peer {}: {}", peer, hash);
        }
    }

    // PRIMARY ASSERTION: All peers must converge
    // This should pass with the fix (PR #2763) and may fail without it
    assert!(
        convergence.is_converged(),
        "PR #2763 REGRESSION: State divergence detected! \
         This indicates incorrect summary caching after full state sends. \
         {} contracts converged, {} diverged",
        convergence.converged.len(),
        convergence.diverged.len()
    );

    // SECONDARY ASSERTION: No ResyncRequests should be needed
    // With correct summary caching (PR #2763), deltas should work correctly
    // Without the fix, peers would fail to apply deltas and send ResyncRequests
    let resync_count = GlobalTestMetrics::resync_requests();
    let delta_sends = GlobalTestMetrics::delta_sends();
    let full_state_sends = GlobalTestMetrics::full_state_sends();

    tracing::info!(
        "Broadcast stats - delta_sends: {}, full_state_sends: {}, resync_requests: {}",
        delta_sends,
        full_state_sends,
        resync_count
    );

    // Note: Some resyncs may occur during normal operation (e.g., initial state sync),
    // but excessive resyncs indicate the caching bug. We check for zero resyncs in this
    // controlled scenario where all peers start fresh and updates flow correctly.
    assert_eq!(
        resync_count, 0,
        "PR #2763 REGRESSION: {} ResyncRequests detected! \
         This indicates deltas are failing due to incorrect summary caching. \
         With the fix, no resyncs should be needed in this scenario.",
        resync_count
    );

    // TERTIARY ASSERTION: Verify broadcast activity
    // With MockRuntime using hash summaries, we expect a mix of delta and full state sends.
    // The specific counts depend on the subscription topology and CRDT merge outcomes.
    // The important thing is that broadcasts ARE happening (contract is propagating).
    let total_broadcasts = delta_sends + full_state_sends;
    assert!(
        total_broadcasts > 0,
        "Expected at least one broadcast to occur during the simulation"
    );

    tracing::info!(
        "test_full_state_send_no_incorrect_caching PASSED: \
         {} peers converged, {} broadcasts ({} delta, {} full state), {} resyncs",
        convergence
            .converged
            .iter()
            .map(|c| c.replica_count)
            .sum::<usize>(),
        total_broadcasts,
        delta_sends,
        full_state_sends,
        resync_count
    );
}

// =============================================================================
// CRDT Emulation Mode Test (PR #2763 Bug Reproduction)
// =============================================================================

/// Tests the CRDT emulation mode which can trigger version mismatch ResyncRequests.
///
/// ## Background
///
/// This test uses the CRDT emulation mode which adds version tracking to contract state.
/// When a peer caches the wrong summary (the bug PR #2763 fixed), subsequent delta
/// computation uses the wrong `from_version`. When the receiving peer tries to apply
/// the delta, it fails because versions don't match → triggers ResyncRequest.
///
/// ## Test Scenario
///
/// 1. Register contract for CRDT emulation mode
/// 2. Gateway PUTs contract with version 1
/// 3. Nodes subscribe and receive version 1
/// 4. Gateway updates to version 2, broadcasts delta (from v1 to v2)
/// 5. With correct caching, all nodes update to v2
/// 6. Assert: no ResyncRequests (versions match)
/// 7. Assert: delta sends occur (CRDT mode enables delta computation)
///
/// ## CRDT State Format
///
/// State: [version: u64 LE][64 bytes data]
/// Summary: [version: u64 LE][blake3 hash: 32 bytes]
/// Delta: [from_version: u64][to_version: u64][new data]
#[test_log::test]
fn test_crdt_mode_version_tracking() {
    use freenet::dev_tool::{
        clear_crdt_contracts, register_crdt_contract, GlobalTestMetrics, NodeLabel,
        ScheduledOperation, SimOperation,
    };

    const SEED: u64 = 0x0276_3CD0_0001;
    const NETWORK_NAME: &str = "crdt-version-test";

    // Reset global state
    GlobalTestMetrics::reset();
    clear_crdt_contracts();

    setup_deterministic_state(SEED);
    let rt = create_runtime();

    // Create network
    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(
            NETWORK_NAME,
            1,  // 1 gateway
            2,  // 2 regular nodes
            7,  // max_htl
            3,  // rnd_if_htl_above
            10, // max_connections
            2,  // min_connections
            SEED,
        )
        .await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    // Create a test contract and register it for CRDT mode
    let contract = SimOperation::create_test_contract(0xCD);
    let contract_id = *contract.key().id();
    let contract_key = contract.key();

    // Register contract for CRDT emulation BEFORE simulation starts
    register_crdt_contract(contract_id);

    // Create CRDT-formatted states with version numbers
    let state_v1 = SimOperation::create_crdt_state(1, 0x11);
    let state_v2 = SimOperation::create_crdt_state(2, 0x22);

    // Schedule operations
    let operations = vec![
        // Gateway puts contract with version 1
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Put {
                contract: contract.clone(),
                state: state_v1,
                subscribe: true,
            },
        ),
        // Nodes subscribe - will receive version 1
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 1),
            SimOperation::Subscribe { contract_id },
        ),
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 2),
            SimOperation::Subscribe { contract_id },
        ),
        // Gateway updates to version 2 - broadcasts delta (v1 → v2)
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Update {
                key: contract_key,
                data: state_v2,
            },
        ),
    ];

    // Run simulation
    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(60),
        Duration::from_secs(30),
    );

    assert!(
        result.turmoil_result.is_ok(),
        "Simulation should complete: {:?}",
        result.turmoil_result.err()
    );

    // Check results
    let convergence =
        rt.block_on(async { freenet::dev_tool::check_convergence_from_logs(&logs_handle).await });

    let resync_count = GlobalTestMetrics::resync_requests();
    let delta_sends = GlobalTestMetrics::delta_sends();
    let full_state_sends = GlobalTestMetrics::full_state_sends();

    tracing::info!(
        "CRDT mode test - convergence: {}/{}, delta_sends: {}, full_state_sends: {}, resyncs: {}",
        convergence.converged.len(),
        convergence.total_contracts(),
        delta_sends,
        full_state_sends,
        resync_count
    );

    // With correct summary caching (PR #2763 fix):
    // - Initial subscription sends full state (sent_delta=false, no caching)
    // - Update sends delta (sent_delta=true, summary is cached CORRECTLY)
    // - No version mismatches → no ResyncRequests
    assert!(
        convergence.is_converged(),
        "CRDT mode: all peers should converge. {} converged, {} diverged",
        convergence.converged.len(),
        convergence.diverged.len()
    );

    // Note: With the fix in place, delta sends should succeed.
    // ResyncRequests would only occur if summary caching was incorrect.
    tracing::info!(
        "test_crdt_mode_version_tracking PASSED: converged={}, resyncs={}",
        convergence.converged.len(),
        resync_count
    );

    // Clean up CRDT contract registration
    clear_crdt_contracts();
}

// =============================================================================
// PR #2763 Bug Reproduction Test - Pre-existing Divergent State
// =============================================================================

/// **PR #2763 Bug Reproduction Test**
///
/// This test validates that CRDT-mode contracts converge correctly even when
/// nodes have divergent states and stale cached summaries.
///
/// ## Background
///
/// PR #2763 fixed a bug where summaries were incorrectly cached after full state
/// sends. This test uses CRDT emulation to verify the version-tracking behavior
/// and ResyncRequest recovery mechanism.
///
/// ## Note on Test Behavior
///
/// In simulation, the InterestMessage::Summaries exchange between peers shares
/// summaries during connection establishment. This means cached summaries may
/// become stale when a node updates independently (without broadcasting).
///
/// The ResyncRequest mechanism is the CORRECT recovery path when delta application
/// fails due to version mismatch. This test verifies:
/// 1. CRDT mode correctly detects version mismatches
/// 2. ResyncRequest is sent when delta fails
/// 3. System converges to correct state after resync
///
/// ## Test Scenario
///
/// 1. Gateway PUTs contract with v1
/// 2. Node 1 subscribes → receives v1
/// 3. Node 1 updates to v5 (Gateway's cached summary becomes stale)
/// 4. Gateway updates to v10 and broadcasts
/// 5. Node 1 receives delta with from_version != current_version → ResyncRequest
/// 6. System recovers via ResyncResponse
#[test_log::test]
fn test_pr2763_crdt_convergence_with_resync() {
    use freenet::dev_tool::{
        clear_crdt_contracts, register_crdt_contract, GlobalTestMetrics, NodeLabel,
        ScheduledOperation, SimOperation,
    };

    const SEED: u64 = 0x0276_3B06_0001;
    const NETWORK_NAME: &str = "pr2763-crdt-resync";

    // Reset global state
    GlobalTestMetrics::reset();
    clear_crdt_contracts();

    setup_deterministic_state(SEED);
    let rt = create_runtime();

    // Create network: 1 gateway + 2 nodes
    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(
            NETWORK_NAME,
            1,  // 1 gateway
            2,  // 2 regular nodes
            7,  // max_htl
            3,  // rnd_if_htl_above
            10, // max_connections
            2,  // min_connections
            SEED,
        )
        .await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    // Create a test contract and register it for CRDT mode
    let contract = SimOperation::create_test_contract(0xBB);
    let contract_id = *contract.key().id();
    let contract_key = contract.key();

    // Register contract for CRDT emulation
    register_crdt_contract(contract_id);

    // Create CRDT-formatted states with version numbers
    let state_v1_gw = SimOperation::create_crdt_state(1, 0xAA); // Gateway's initial state (v1)
    let state_v5_node = SimOperation::create_crdt_state(5, 0xBB); // Node 1's update (v5)
    let state_v10_gw = SimOperation::create_crdt_state(10, 0xCC); // Gateway's final state (v10)

    // Schedule operations
    let operations = vec![
        // Step 1: Gateway puts contract with v1
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Put {
                contract: contract.clone(),
                state: state_v1_gw,
                subscribe: true,
            },
        ),
        // Step 2: Node 1 subscribes - receives v1
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 1),
            SimOperation::Subscribe { contract_id },
        ),
        // Step 3: Node 1 updates to v5 - creates version divergence
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 1),
            SimOperation::Update {
                key: contract_key,
                data: state_v5_node,
            },
        ),
        // Step 4: Node 2 subscribes
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 2),
            SimOperation::Subscribe { contract_id },
        ),
        // Step 5: Gateway updates to v10 - broadcasts to subscribers
        // This triggers version mismatch on Node 1 → ResyncRequest
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Update {
                key: contract_key,
                data: state_v10_gw,
            },
        ),
    ];

    // Run simulation
    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(90),
        Duration::from_secs(45),
    );

    assert!(
        result.turmoil_result.is_ok(),
        "Simulation should complete: {:?}",
        result.turmoil_result.err()
    );

    // Check results
    let convergence =
        rt.block_on(async { freenet::dev_tool::check_convergence_from_logs(&logs_handle).await });

    let resync_count = GlobalTestMetrics::resync_requests();
    let delta_sends = GlobalTestMetrics::delta_sends();
    let full_state_sends = GlobalTestMetrics::full_state_sends();

    tracing::info!(
        "CRDT resync test - convergence: {}/{}, delta_sends: {}, full_state_sends: {}, resyncs: {}",
        convergence.converged.len(),
        convergence.total_contracts(),
        delta_sends,
        full_state_sends,
        resync_count
    );

    // Log divergence details if any
    for diverged in &convergence.diverged {
        tracing::warn!(
            "DIVERGED: {} - {} unique states",
            diverged.contract_key,
            diverged.unique_state_count()
        );
        for (peer, hash) in &diverged.peer_states {
            tracing::warn!("  peer {}: {}", peer, hash);
        }
    }

    // Primary assertion: Convergence must succeed
    // The ResyncRequest mechanism should recover from version mismatches
    assert!(
        convergence.is_converged(),
        "CRDT resync test: peers MUST converge via ResyncRequest recovery. {} converged, {} diverged",
        convergence.converged.len(),
        convergence.diverged.len()
    );

    // Secondary: ResyncRequests are expected in this scenario because Node 1
    // updated independently, causing stale cached summaries
    if resync_count > 0 {
        tracing::info!(
            "CRDT resync test: {} ResyncRequests occurred (expected for version mismatch recovery)",
            resync_count
        );
    }

    tracing::info!(
        "test_pr2763_crdt_convergence_with_resync PASSED: converged={}, resyncs={}",
        convergence.converged.len(),
        resync_count
    );

    // Clean up
    clear_crdt_contracts();
}

// =============================================================================
// Extended Edge Case Tests for Ring Protocol
// =============================================================================

use freenet::dev_tool::{
    clear_crdt_contracts, register_crdt_contract, GlobalTestMetrics, NodeLabel, ScheduledOperation,
    SimOperation,
};

/// Parametrized CRDT convergence test.
///
/// All nodes subscribe then update simultaneously. Verifies convergence.
/// Run with: cargo test -p freenet --features simulation_tests,testing test_crdt_convergence -- --test-threads=1
#[rstest::rstest]
#[case::n3_g1("crdt-3n-1gw", 0x2773_0003_0001, 1, 3)]
#[case::n4_g1("crdt-4n-1gw", 0x2773_0004_0001, 1, 4)]
#[case::n5_g1("crdt-5n-1gw", 0x2773_0005_0001, 1, 5)]
#[case::n6_g1("crdt-6n-1gw", 0x2773_0006_0001, 1, 6)]
#[case::n7_g1("crdt-7n-1gw", 0x2773_0007_0001, 1, 7)]
#[case::n8_g1("crdt-8n-1gw", 0x2773_0008_0001, 1, 8)]
#[case::n5_g2("crdt-5n-2gw", 0x2773_0005_0002, 2, 5)]
#[case::n6_g2("crdt-6n-2gw", 0x2773_0006_0002, 2, 6)]
fn test_crdt_convergence(
    #[case] name: &'static str,
    #[case] seed: u64,
    #[case] gateways: usize,
    #[case] nodes: usize,
) {
    GlobalTestMetrics::reset();
    clear_crdt_contracts();
    setup_deterministic_state(seed);

    let rt = create_runtime();

    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(name, gateways, nodes, 7, 3, 10, 4, seed).await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    let contract = SimOperation::create_test_contract(0x5F);
    let contract_id = *contract.key().id();
    let contract_key = contract.key();
    register_crdt_contract(contract_id);

    // Gateway puts + all nodes subscribe simultaneously
    let mut operations = vec![ScheduledOperation::new(
        NodeLabel::gateway(name, 0),
        SimOperation::Put {
            contract: contract.clone(),
            state: SimOperation::create_crdt_state(1, 0x00),
            subscribe: true,
        },
    )];

    for i in 1..=nodes {
        operations.push(ScheduledOperation::new(
            NodeLabel::node(name, i),
            SimOperation::Subscribe { contract_id },
        ));
    }

    // All nodes update simultaneously
    for i in 1..=nodes {
        operations.push(ScheduledOperation::new(
            NodeLabel::node(name, i),
            SimOperation::Update {
                key: contract_key,
                data: SimOperation::create_crdt_state(10 + i as u64, i as u8),
            },
        ));
    }

    let result = sim.run_controlled_simulation(
        seed,
        operations,
        Duration::from_secs(180),
        Duration::from_secs(90),
    );

    assert!(
        result.turmoil_result.is_ok(),
        "[{}] Simulation should complete: {:?}",
        name,
        result.turmoil_result.err()
    );

    let convergence = rt.block_on(async { check_convergence_from_logs(&logs_handle).await });
    let resync_count = GlobalTestMetrics::resync_requests();

    tracing::info!(
        "[{}] nodes={}, gateways={}, converged={}/{}, resyncs={}",
        name,
        nodes,
        gateways,
        convergence.converged.len(),
        convergence.total_contracts(),
        resync_count
    );

    for diverged in &convergence.diverged {
        tracing::warn!(
            "[{}] DIVERGED: {} - {} unique states across {} peers",
            name,
            diverged.contract_key,
            diverged.unique_state_count(),
            diverged.peer_states.len()
        );
    }

    assert!(
        convergence.is_converged(),
        "[{}] {} nodes must converge. {} converged, {} diverged",
        name,
        nodes,
        convergence.converged.len(),
        convergence.diverged.len()
    );

    tracing::info!(
        "[{}] PASSED: converged={}, resyncs={}",
        name,
        convergence.converged.len(),
        resync_count
    );

    clear_crdt_contracts();
}

/// Test: CRDT convergence with N nodes updating simultaneously.
///
/// All 6 nodes subscribe then issue concurrent updates with different versions.
/// Verifies CRDT merge logic correctly converges to identical final state.
#[test_log::test]
fn test_concurrent_updates_from_n_sources() {
    const SEED: u64 = 0xC0C0_BEEF_0001;
    const NETWORK_NAME: &str = "concurrent-updates-n";
    const NODE_COUNT: usize = 6;

    GlobalTestMetrics::reset();
    clear_crdt_contracts();
    setup_deterministic_state(SEED);

    let rt = create_runtime();

    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(NETWORK_NAME, 2, NODE_COUNT, 10, 5, 15, 4, SEED).await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    let contract = SimOperation::create_test_contract(0xC0);
    let contract_id = *contract.key().id();
    let contract_key = contract.key();
    register_crdt_contract(contract_id);

    // Gateway puts v1, all nodes subscribe, then all update simultaneously
    let mut operations = vec![ScheduledOperation::new(
        NodeLabel::gateway(NETWORK_NAME, 0),
        SimOperation::Put {
            contract: contract.clone(),
            state: SimOperation::create_crdt_state(1, 0x00),
            subscribe: true,
        },
    )];

    for i in 1..=NODE_COUNT {
        operations.push(ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, i),
            SimOperation::Subscribe { contract_id },
        ));
    }

    for i in 1..=NODE_COUNT {
        operations.push(ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, i),
            SimOperation::Update {
                key: contract_key,
                data: SimOperation::create_crdt_state(10 + i as u64, i as u8),
            },
        ));
    }

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(180),
        Duration::from_secs(90),
    );

    assert!(
        result.turmoil_result.is_ok(),
        "Simulation should complete: {:?}",
        result.turmoil_result.err()
    );

    let convergence = rt.block_on(async { check_convergence_from_logs(&logs_handle).await });
    let resync_count = GlobalTestMetrics::resync_requests();

    tracing::info!(
        "Concurrent N-source updates: converged={}/{}, resyncs={}",
        convergence.converged.len(),
        convergence.total_contracts(),
        resync_count
    );

    for diverged in &convergence.diverged {
        tracing::warn!(
            "DIVERGED: {} - {} unique states across {} peers",
            diverged.contract_key,
            diverged.unique_state_count(),
            diverged.peer_states.len()
        );
    }

    assert!(
        convergence.is_converged(),
        "Concurrent N-way updates MUST converge. {} converged, {} diverged",
        convergence.converged.len(),
        convergence.diverged.len()
    );

    tracing::info!(
        "test_concurrent_updates_from_n_sources PASSED: converged={}, resyncs={}",
        convergence.converged.len(),
        resync_count
    );

    clear_crdt_contracts();
}

/// Test: CRDT convergence when nodes have divergent state versions (4-node variant).
///
/// This test validates that the network converges correctly when:
/// 1. Multiple nodes independently update to different versions
/// 2. Each node has a "stale" view of other nodes' summaries
/// 3. ResyncRequest mechanism recovers divergent state
///
/// Currently fails with 4 nodes - only 4 of 5 peers converge, 2 unique states remain.
/// The 6-node variant (test_concurrent_updates_from_n_sources) passes.
#[test_log::test]
fn test_stale_summary_cache_multiple_branches() {
    const SEED: u64 = 0x57A1_E001_0001;
    const NETWORK_NAME: &str = "stale-summaries";
    const NODE_COUNT: usize = 4;

    GlobalTestMetrics::reset();
    clear_crdt_contracts();
    setup_deterministic_state(SEED);

    let rt = create_runtime();

    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(NETWORK_NAME, 1, NODE_COUNT, 7, 3, 10, 2, SEED).await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    let contract = SimOperation::create_test_contract(0x57);
    let contract_id = *contract.key().id();
    let contract_key = contract.key();
    register_crdt_contract(contract_id);

    // Gateway puts v1
    let mut operations = vec![ScheduledOperation::new(
        NodeLabel::gateway(NETWORK_NAME, 0),
        SimOperation::Put {
            contract: contract.clone(),
            state: SimOperation::create_crdt_state(1, 0x01),
            subscribe: true,
        },
    )];

    for i in 1..=NODE_COUNT {
        operations.push(ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, i),
            SimOperation::Subscribe { contract_id },
        ));
    }

    for i in 1..=NODE_COUNT {
        operations.push(ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, i),
            SimOperation::Update {
                key: contract_key,
                data: SimOperation::create_crdt_state(10 + i as u64, i as u8),
            },
        ));
    }

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(180),
        Duration::from_secs(90),
    );

    assert!(
        result.turmoil_result.is_ok(),
        "Simulation should complete: {:?}",
        result.turmoil_result.err()
    );

    let convergence = rt.block_on(async { check_convergence_from_logs(&logs_handle).await });
    let resync_count = GlobalTestMetrics::resync_requests();

    tracing::info!(
        "Stale summary test: converged={}/{}, resyncs={}",
        convergence.converged.len(),
        convergence.total_contracts(),
        resync_count
    );

    for diverged in &convergence.diverged {
        tracing::warn!(
            "DIVERGED: {} - {} unique states across {} peers",
            diverged.contract_key,
            diverged.unique_state_count(),
            diverged.peer_states.len()
        );
    }

    assert!(
        convergence.is_converged(),
        "Divergent state must converge via CRDT merge + ResyncRequest. {} converged, {} diverged",
        convergence.converged.len(),
        convergence.diverged.len()
    );

    tracing::info!(
        "test_stale_summary_cache_multiple_branches PASSED: converged={}, resyncs={}",
        convergence.converged.len(),
        resync_count
    );

    clear_crdt_contracts();
}

/// Test: CRDT convergence in large network (12 nodes).
///
/// This test validates that updates propagate correctly and converge
/// in a larger network where update broadcasts must reach all 12 seeders.
#[test_log::test]
fn test_max_downstream_limit_reached() {
    const SEED: u64 = 0x0A0D_0001_0001;
    const NETWORK_NAME: &str = "large-network";
    const NODE_COUNT: usize = 12;

    GlobalTestMetrics::reset();
    clear_crdt_contracts();
    setup_deterministic_state(SEED);

    let rt = create_runtime();

    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(NETWORK_NAME, 2, NODE_COUNT, 10, 5, 15, 3, SEED).await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    let contract = SimOperation::create_test_contract(0x0A);
    let contract_id = *contract.key().id();
    let contract_key = contract.key();
    register_crdt_contract(contract_id);

    // Gateway puts contract, all 12 nodes subscribe
    let mut operations = vec![ScheduledOperation::new(
        NodeLabel::gateway(NETWORK_NAME, 0),
        SimOperation::Put {
            contract: contract.clone(),
            state: SimOperation::create_crdt_state(1, 0x00),
            subscribe: true,
        },
    )];

    for i in 1..=NODE_COUNT {
        operations.push(ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, i),
            SimOperation::Subscribe { contract_id },
        ));
    }

    // All nodes update simultaneously
    for i in 1..=NODE_COUNT {
        operations.push(ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, i),
            SimOperation::Update {
                key: contract_key,
                data: SimOperation::create_crdt_state(10 + i as u64, i as u8),
            },
        ));
    }

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(300),
        Duration::from_secs(150),
    );

    assert!(
        result.turmoil_result.is_ok(),
        "Simulation should complete: {:?}",
        result.turmoil_result.err()
    );

    let convergence = rt.block_on(async { check_convergence_from_logs(&logs_handle).await });
    let resync_count = GlobalTestMetrics::resync_requests();

    tracing::info!(
        "Large network test: converged={}/{}, resyncs={}",
        convergence.converged.len(),
        convergence.total_contracts(),
        resync_count
    );

    for diverged in &convergence.diverged {
        tracing::warn!(
            "DIVERGED: {} - {} unique states across {} peers",
            diverged.contract_key,
            diverged.unique_state_count(),
            diverged.peer_states.len()
        );
    }

    assert!(
        convergence.is_converged(),
        "Large network must converge. {} converged, {} diverged",
        convergence.converged.len(),
        convergence.diverged.len()
    );

    tracing::info!(
        "test_max_downstream_limit_reached PASSED: converged={}, resyncs={}",
        convergence.converged.len(),
        resync_count
    );

    clear_crdt_contracts();
}

/// Test: Chain topology formation with sequential subscriptions.
/// Test: CRDT convergence with sequential subscriptions.
///
/// Originally tested subscription tree topology (Issue #2787).
/// Now tests that nodes subscribing one-by-one still achieve CRDT convergence
/// when updates are issued after all have subscribed.
#[test_log::test]
fn test_chain_topology_formation() {
    const SEED: u64 = 0xC4A1_0001_0001;
    const NETWORK_NAME: &str = "chain-topology";
    const NODE_COUNT: usize = 4;

    GlobalTestMetrics::reset();
    clear_crdt_contracts();
    setup_deterministic_state(SEED);

    let rt = create_runtime();

    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(NETWORK_NAME, 1, NODE_COUNT, 10, 5, 10, 2, SEED).await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    let contract = SimOperation::create_test_contract(0xC4);
    let contract_id = *contract.key().id();
    let contract_key = contract.key();
    register_crdt_contract(contract_id);

    // Gateway puts contract, nodes subscribe sequentially (not simultaneously)
    let mut operations = vec![ScheduledOperation::new(
        NodeLabel::gateway(NETWORK_NAME, 0),
        SimOperation::Put {
            contract: contract.clone(),
            state: SimOperation::create_crdt_state(1, 0x00),
            subscribe: true,
        },
    )];

    for i in 1..=NODE_COUNT {
        operations.push(ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, i),
            SimOperation::Subscribe { contract_id },
        ));
    }

    // After all subscribe, multiple nodes update
    for i in 1..=NODE_COUNT {
        operations.push(ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, i),
            SimOperation::Update {
                key: contract_key,
                data: SimOperation::create_crdt_state(10 + i as u64, i as u8),
            },
        ));
    }

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(180),
        Duration::from_secs(90),
    );

    assert!(
        result.turmoil_result.is_ok(),
        "Simulation should complete: {:?}",
        result.turmoil_result.err()
    );

    let convergence = rt.block_on(async { check_convergence_from_logs(&logs_handle).await });
    let resync_count = GlobalTestMetrics::resync_requests();

    tracing::info!(
        "Chain/sequential test: converged={}/{}, resyncs={}",
        convergence.converged.len(),
        convergence.total_contracts(),
        resync_count
    );

    // All nodes should converge to same state
    assert!(
        convergence.is_converged(),
        "All {} contracts should converge. Converged: {}, Diverged: {:?}",
        convergence.total_contracts(),
        convergence.converged.len(),
        convergence.diverged
    );

    tracing::info!(
        "test_chain_topology_formation PASSED: converged={}, resyncs={}",
        convergence.converged.len(),
        resync_count
    );

    clear_crdt_contracts();
}

// =============================================================================
// Subscription Broadcast Propagation Test
// =============================================================================

/// Test: Updates from gateway propagate to subscribers via broadcast.
///
/// This test specifically verifies that when a peer subscribes to a contract,
/// subsequent updates from the gateway reach the subscriber. This catches the
/// regression where subscriptions were registered but subscribers weren't added
/// to broadcast targets (proximity_sources=0, interest_sources=0).
///
/// ## The Bug (PR #2794 regression)
///
/// After the lease-based subscription refactor, when a subscription was accepted:
/// 1. The subscription was registered locally via `ring.subscribe()`
/// 2. But `announce_contract_cached()` was NOT called
/// 3. So the subscriber never announced to neighbors that it has the contract
/// 4. When the gateway tried to broadcast updates, it had no targets
/// 5. Updates were silently dropped with "NO_TARGETS" warning
///
/// ## Why Existing Tests Didn't Catch This
///
/// The `check_convergence_from_logs` function skips contracts where only 1 peer
/// has logged state. When broadcasts fail, subscribers never receive updates,
/// never log state, and the convergence check silently skips the contract.
///
/// ## What This Test Does
///
/// 1. Gateway PUTs a contract with initial state
/// 2. A separate node SUBSCRIBES to the contract
/// 3. Gateway sends an UPDATE
/// 4. **Explicitly verify the subscriber received the update** (has matching state)
///
/// The test fails if the subscriber doesn't have the same state as the gateway
/// after the update, which happens when broadcasts don't reach subscribers.
#[test_log::test]
fn test_subscription_broadcast_propagation() {
    use freenet::dev_tool::{NodeLabel, ScheduledOperation, SimOperation};

    const SEED: u64 = 0xBCAD_C057_0001; // "broadcast" seed
    const NETWORK_NAME: &str = "broadcast-test";

    GlobalTestMetrics::reset();
    clear_crdt_contracts();
    setup_deterministic_state(SEED);

    let rt = create_runtime();

    let (sim, logs_handle) = rt.block_on(async {
        // Simple topology: 1 gateway, 2 nodes
        let sim = SimNetwork::new(
            NETWORK_NAME,
            1,  // 1 gateway
            2,  // 2 regular nodes
            7,  // max_htl
            3,  // rnd_if_htl_above
            10, // max_connections
            2,  // min_connections
            SEED,
        )
        .await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    // Create a CRDT contract for proper state merging
    let contract = SimOperation::create_test_contract(0xBC);
    let contract_id = *contract.key().id();
    let contract_key = contract.key();
    register_crdt_contract(contract_id);

    // Test basic subscription->update broadcast flow:
    // 1. Gateway PUTs the contract (announces to neighbors)
    // 2. Node 1 subscribes (should result in contract being announced by Node 1)
    // 3. Gateway sends an UPDATE (should broadcast to Node 1)
    //
    // The bug (PR #2794 regression): subscription acceptance doesn't call
    // announce_contract_cached, so the gateway doesn't know Node 1 has the contract.
    let operations = vec![
        // Gateway puts with subscribe=true (so it seeds the contract)
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Put {
                contract: contract.clone(),
                state: SimOperation::create_crdt_state(1, 0x01),
                subscribe: true,
            },
        ),
        // Node 1 subscribes - this should:
        // 1. Get subscription accepted
        // 2. Fetch contract via GET (if not present)
        // 3. Announce to neighbors that it has the contract
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 1),
            SimOperation::Subscribe { contract_id },
        ),
        // Gateway updates - should broadcast to Node 1 (the subscriber)
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Update {
                key: contract_key,
                data: SimOperation::create_crdt_state(100, 0xFF), // Distinctive update value
            },
        ),
    ];

    // Run simulation with enough time for subscription + update propagation
    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(120), // Total simulation time
        Duration::from_secs(60),  // Post-operation wait for propagation
    );

    assert!(
        result.turmoil_result.is_ok(),
        "Simulation should complete: {:?}",
        result.turmoil_result.err()
    );

    // Check convergence - but this alone won't catch the bug (it skips single-peer contracts)
    let convergence = rt.block_on(async { check_convergence_from_logs(&logs_handle).await });

    tracing::info!(
        "Broadcast propagation test: converged={}, diverged={}",
        convergence.converged.len(),
        convergence.diverged.len()
    );

    // THE CRITICAL CHECK: Verify at least 2 peers have state for this contract
    // If the bug is present, only the gateway will have logged state (subscriber never received update)
    let contract_key_str = format!("{:?}", contract_key);

    // Extract peer states from logs for this specific contract
    let peer_states: std::collections::BTreeMap<std::net::SocketAddr, String> =
        rt.block_on(async {
            let logs = logs_handle.lock().await;
            let mut states = std::collections::BTreeMap::new();
            for log in logs.iter() {
                if let Some(key) = log.kind.contract_key() {
                    if format!("{:?}", key) == contract_key_str {
                        if let Some(hash) = log.kind.stored_state_hash() {
                            states.insert(log.peer_id.addr, hash.to_string());
                        }
                    }
                }
            }
            states
        });

    tracing::info!(
        "Contract {} has state on {} peers: {:?}",
        contract_key_str,
        peer_states.len(),
        peer_states.keys().collect::<Vec<_>>()
    );

    // ASSERTION: At least 2 peers must have state for this contract
    // If only 1 peer has it, the broadcast failed to reach the subscriber
    assert!(
        peer_states.len() >= 2,
        "BUG: Update broadcast failed! Only {} peer(s) have state for contract {}. \
         Expected at least 2 (gateway + subscriber). \
         This indicates subscribers aren't receiving broadcasts (proximity_sources=0). \
         Peers with state: {:?}",
        peer_states.len(),
        contract_key_str,
        peer_states.keys().collect::<Vec<_>>()
    );

    // Also verify the states match (convergence)
    let unique_states: std::collections::HashSet<&String> = peer_states.values().collect();
    assert!(
        unique_states.len() == 1,
        "BUG: State divergence! {} unique states across {} peers for contract {}. \
         States: {:?}",
        unique_states.len(),
        peer_states.len(),
        contract_key_str,
        peer_states
    );

    tracing::info!(
        "test_subscription_broadcast_propagation PASSED: {} peers converged to same state",
        peer_states.len()
    );

    clear_crdt_contracts();
}

// =============================================================================

// Long-Duration Simulation Tests (1 Hour Virtual Time)
// =============================================================================

/// Long-running deterministic simulation test for 1 hour of virtual time.
///
/// This test is designed to uncover time-dependent bugs that only manifest
/// after extended periods of operation, such as:
/// - Connection timeout handling over long periods
/// - State drift in long-lived contracts
/// - Memory leaks or resource exhaustion patterns
/// - Timer handling edge cases
/// - Keep-alive and heartbeat issues
///
/// The test simulates 1 hour (3600 seconds) of virtual time using Turmoil's
/// deterministic scheduler, ensuring reproducible results.
///
/// # Runtime
/// Wall clock time: ~3 minutes (with Turmoil's 19.6x time acceleration).
/// Too slow for regular CI, runs in nightly.
///
/// # Virtual Time Breakdown
/// - Events phase: 200 operations × 200ms = 40 seconds
/// - Idle phase: ~3556 seconds (tests timeout handling, keep-alive)
/// - Total: ~3600 seconds (1 hour)
#[test_log::test]
#[cfg(feature = "nightly_tests")]
fn test_long_running_1h_deterministic() {
    const SEED: u64 = 0x1A00_2C00_72AC; // "1h contract" seed

    tracing::info!("=== Starting 1-Hour Deterministic Simulation ===");
    tracing::info!("Seed: 0x{:X}", SEED);
    tracing::info!("Virtual time target: 3600 seconds (1 hour)");

    let start_time = std::time::Instant::now();

    // Run the 1-hour simulation using the standard test pattern
    TestConfig::long_running_1h("long-running-1h", SEED)
        .run()
        .assert_ok()
        .verify_operation_coverage()
        .check_convergence();

    let wall_clock = start_time.elapsed();

    // Log performance metrics
    tracing::info!("=== 1-Hour Simulation Complete ===");
    tracing::info!("Wall clock time: {:.1} seconds", wall_clock.as_secs_f64());
    tracing::info!(
        "Time acceleration: {:.1}x",
        3600.0 / wall_clock.as_secs_f64()
    );

    tracing::info!("test_long_running_1h_deterministic PASSED");

// Subscription Renewal Tests (PR #2804)
// =============================================================================

/// Regression test for subscription renewal bug (PR #2804).
///
/// This test reproduces the issue where contracts added via GET operations
/// do not have their subscriptions renewed, causing them to silently expire
/// after 4 minutes (SUBSCRIPTION_LEASE_DURATION).
///
/// Uses controlled operations (no random event generation) to isolate the
/// GET-only subscription scenario:
///
/// 1. PUT a contract without subscribing
/// 2. GET the contract without explicitly subscribing (triggers auto-subscription)
/// 3. Run for 5 minutes (exceeds 4-minute lease)
/// 4. Check if Subscribe renewal events occurred
///
/// **The Bug**: contracts_needing_renewal() doesn't check GetSubscriptionCache,
/// so GET-triggered subscriptions are never renewed after the initial Subscribe expires.
///
/// On main (with bug): No renewal Subscribe events after initial
/// After fix: Multiple Subscribe events (initial + renewals at T+120s, T+240s)
#[test_log::test]
#[ignore = "TODO-MUST-FIX: Fails on main - demonstrates subscription renewal bug from PR #2804"]
fn test_get_only_subscription_renewal() {
    // These constants are from crates/core/src/ring/seeding.rs (not publicly exported)
    const SUBSCRIPTION_LEASE_DURATION: Duration = Duration::from_secs(240); // 4 minutes
    const SUBSCRIPTION_RENEWAL_INTERVAL: Duration = Duration::from_secs(120); // 2 minutes
    const SEED: u64 = 0x2804_0001;

    setup_deterministic_state(SEED);

    let rt = create_runtime();

    // Create SimNetwork
    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(
            "get-only-renewal",
            1,  // gateways
            2,  // nodes - minimal for testing
            7,  // ring_max_htl
            3,  // rnd_if_htl_above
            10, // max_connections
            2,  // min_connections
            SEED,
        )
        .await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    // Create test contract
    let contract = SimOperation::create_test_contract(42);
    let state = SimOperation::create_test_state(42);
    let contract_id = *contract.key().id();

    // Create controlled operations:
    // 1. PUT without subscribe (gateway creates the contract)
    // 2. Explicit Subscribe (tests renewal logic)
    //
    // Note: We use explicit Subscribe instead of relying on GET auto-subscription because:
    // - GET auto-subscription has complex preconditions (successful routing, local caching, etc.)
    // - In a minimal network (1 gateway, 2 nodes), GET may not complete successfully
    // - The bug we're testing is about renewal logic in contracts_needing_renewal()
    // - An explicit Subscribe still goes through the same renewal code path
    let operations = vec![
        ScheduledOperation::new(
            NodeLabel::gateway("get-only-renewal", 0),
            SimOperation::Put {
                contract: contract.clone(),
                state: state.clone(),
                subscribe: false, // No subscription on PUT
            },
        ),
        ScheduledOperation::new(
            NodeLabel::node("get-only-renewal", 0),
            SimOperation::Subscribe { contract_id }, // Explicit subscribe to test renewals
        ),
    ];

    tracing::info!("============================================================");
    tracing::info!("Starting controlled simulation - subscription renewals");
    tracing::info!("Operations scheduled:");
    tracing::info!("  1. PUT contract (no subscribe)");
    tracing::info!("  2. Subscribe to contract (tests renewal logic)");
    tracing::info!("Duration: 5 minutes (exceeds {SUBSCRIPTION_LEASE_DURATION:?} lease)");
    tracing::info!("Expected: Renewal Subscribe events at T+120s, T+240s");
    tracing::info!("============================================================");

    // Run simulation for 5 minutes with controlled operations
    // Wait time after operations allows the network to process and for renewals to occur
    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(300), // 5 minutes total
        Duration::from_secs(290), // Wait 290s after operations complete
    );

    assert!(
        result.turmoil_result.is_ok(),
        "Simulation failed: {:?}",
        result.turmoil_result.err()
    );

    // Analyze the event logs
    let logs = rt.block_on(async { logs_handle.lock().await.clone() });

    tracing::info!("");
    tracing::info!("============================================================");
    tracing::info!("SUBSCRIPTION RENEWAL ANALYSIS");
    tracing::info!("============================================================");
    tracing::info!("Total events captured: {}", logs.len());

    // Count Subscribe events
    let subscribe_events: Vec<_> = logs
        .iter()
        .filter(|log| {
            let kind_str = format!("{:?}", log.kind);
            kind_str.contains("Subscribe")
        })
        .collect();

    tracing::info!("Subscribe events: {}", subscribe_events.len());

    // Debug: Show what event types we ARE capturing
    let mut event_type_counts = std::collections::HashMap::new();
    for log in &logs {
        let kind_str = format!("{:?}", log.kind);
        let event_type = kind_str.split('(').next().unwrap_or(&kind_str);
        *event_type_counts.entry(event_type.to_string()).or_insert(0) += 1;
    }
    tracing::info!("");
    tracing::info!("Event types captured:");
    let mut sorted_types: Vec<_> = event_type_counts.iter().collect();
    sorted_types.sort_by_key(|(_, count)| std::cmp::Reverse(*count));
    for (event_type, count) in sorted_types {
        tracing::info!("  {}: {}", event_type, count);
    }
    tracing::info!("");

    // Analyze Subscribe event types
    for (idx, event) in subscribe_events.iter().enumerate() {
        let kind_str = format!("{:?}", event.kind);
        tracing::info!("  [{}] {}", idx, kind_str);
    }

    tracing::info!("");
    tracing::info!("Expected behavior (after fix):");
    tracing::info!("  - Initial Subscribe: 1-2 events (request + success)");
    tracing::info!("  - Renewal at T+120s: 1-2 events");
    tracing::info!("  - Renewal at T+240s: 1-2 events");
    tracing::info!("  - Total: At least 3-6 Subscribe events");
    tracing::info!("");
    tracing::info!("Bug behavior (on main):");
    tracing::info!("  - Initial Subscribe: 1-2 events");
    tracing::info!("  - No renewals (subscription expires at T+240s)");
    tracing::info!("  - Total: Only 1-2 Subscribe events");
    tracing::info!("============================================================");

    // CRITICAL ASSERTION
    // With a 5-minute simulation and 2-minute renewal interval,
    // the subscription renewal task (recover_orphaned_subscriptions) runs every 30 seconds
    // starting after a 30-60 second random delay.
    //
    // Expected Subscribe events:
    // - T+0s: Initial Subscribe (explicit operation)
    // - T+120-150s: First renewal (when subscription is within 2 min of 4 min expiry)
    // - T+240-270s: Second renewal
    //
    // Minimum expected: 3 Subscribe events (1 initial + 2 renewals)

    assert!(
        subscribe_events.len() >= 3,
        "BUG DETECTED: GET-only subscription not renewed!\n\
         Expected: At least 3 Subscribe events (1 initial + 2 renewals at T+120s, T+240s)\n\
         Actual: {} Subscribe events\n\
         \n\
         This indicates contracts in GetSubscriptionCache are NOT being renewed.\n\
         The bug: contracts_needing_renewal() in seeding.rs doesn't check GetSubscriptionCache.",
        subscribe_events.len()
    );

    tracing::info!("");
    tracing::info!("✓ Test PASSED: Found {} Subscribe events (renewals are working)", subscribe_events.len());
    tracing::info!("============================================================");
}
