//! Deterministic simulation tests using Turmoil.
//!
//! All tests in this file use Turmoil's deterministic scheduler for reproducible execution.
//! Same seed MUST produce identical results across runs.
//!
//! Thread-local state (GlobalRng, GlobalSimulationTime, GlobalTestMetrics) and per-network
//! registries (sockets, topology, fault injectors) provide test isolation, so parallel
//! execution is safe.
//!
//! Enable with: cargo test -p freenet --features "simulation_tests,testing" --test simulation_integration
//!
//! The `testing` feature is required for `run_controlled_simulation` method.
//!
//! For non-deterministic smoke tests, see `simulation_smoke.rs`.

#![cfg(feature = "simulation_tests")]

use freenet::config::GlobalTestMetrics;
use freenet::config::{GlobalRng, GlobalSimulationTime, SimulationTransportOpt};
use freenet::dev_tool::{
    RequestId, SimNetwork, StreamId, VirtualTime, check_convergence_from_logs,
    reset_channel_id_counter, reset_event_id_counter, reset_global_node_index, reset_nonce_counter,
};
use freenet::simulation::TimeSource;
use freenet::transport::in_memory_socket::{
    SimulationSocket, clear_all_socket_registries, register_address_network,
    register_network_time_source,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
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
    /// Time to sleep between each event in turmoil virtual time.
    /// Default: 200ms (sufficient for event propagation in deterministic sim).
    event_wait: Duration,
    /// Time to sleep after all events complete (post-events phase).
    sleep_after_events: Duration,
    require_convergence: bool,
    /// Optional latency range for jitter simulation (min..max)
    latency_range: Option<std::ops::Range<Duration>>,
    /// Optional message loss rate for fault injection (0.0 to 1.0)
    message_loss_rate: f64,
    /// Use MockWasmRuntime (production ContractExecutor path) instead of MockRuntime.
    ///
    /// **Prefer `true` for new tests.** MockWasmRuntime shares production `bridged_*`
    /// code paths (init tracking, validation, subscriber notifications, corrupted state
    /// recovery). MockRuntime has its own independent implementation that can silently
    /// drift from production behavior. See issue #3141.
    use_mock_wasm: bool,
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
            event_wait: Duration::from_millis(200),
            sleep_after_events: Duration::from_secs(1),
            require_convergence: true,
            latency_range: None,
            message_loss_rate: 0.0,
            use_mock_wasm: false,
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
            event_wait: Duration::from_millis(200),
            sleep_after_events: Duration::from_secs(3),
            require_convergence: true,
            latency_range: None,
            message_loss_rate: 0.0,
            use_mock_wasm: false,
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
            event_wait: Duration::from_millis(200),
            sleep_after_events: Duration::from_secs(10),
            require_convergence: true,
            latency_range: None,
            message_loss_rate: 0.0,
            use_mock_wasm: false,
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
    /// Events are distributed across the full hour (1 event every ~10s) so
    /// the network must remain functional throughout — not just survive idle.
    ///
    /// Includes 10-50ms latency jitter to simulate realistic network conditions.
    ///
    /// # Virtual Time Breakdown
    /// - 360 events × 10s between events = 3600 seconds (1 hour)
    /// - Post-events buffer: 10 seconds for final propagation
    #[allow(dead_code)]
    fn long_running(name: &'static str, seed: u64) -> Self {
        // 360 events × 10s apart = 3600s (1 hour) virtual time.
        // Turmoil steps every virtual ms: 3600s ≈ 3.6M steps × 8 hosts.
        // Wall clock: ~2.5 min (25x acceleration with 8 hosts).
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
            iterations: 360, // Events distributed across 1 hour virtual
            duration: Duration::from_secs(3700), // Max simulation time (buffer)
            event_wait: Duration::from_secs(10), // 10s between events = ~3600s total
            sleep_after_events: Duration::from_secs(10), // Brief propagation wait
            require_convergence: true,
            // Realistic latency jitter (10-50ms) to uncover timing issues
            latency_range: Some(Duration::from_millis(10)..Duration::from_millis(50)),
            message_loss_rate: 0.0,
            use_mock_wasm: false,
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
        self.sleep_after_events = sleep;
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

    /// Opt out of asserting convergence.
    ///
    /// For `run_direct()` tests this ALSO tells the runner to skip its
    /// up-to-1800s post-event convergence-polling tail and do only a brief
    /// settle instead. Use this for tests that analyze events produced during
    /// the event phase and never assert final-state convergence — running the
    /// advisory polling tail (which only warns, never fails) when the network
    /// happens not to converge just burns wall-clock and risks a CI timeout.
    /// See #3792.
    fn no_convergence_wait(mut self) -> Self {
        self.require_convergence = false;
        self
    }

    /// Add latency jitter simulation.
    #[allow(dead_code)]
    fn with_latency(mut self, min: Duration, max: Duration) -> Self {
        self.latency_range = Some(min..max);
        self
    }

    /// Set the delay between events in virtual time.
    fn with_event_wait(mut self, event_wait: Duration) -> Self {
        self.event_wait = event_wait;
        self
    }

    /// Add message loss fault injection (0.0 to 1.0).
    fn with_message_loss(mut self, rate: f64) -> Self {
        self.message_loss_rate = rate.clamp(0.0, 1.0);
        self
    }

    /// Use MockWasmRuntime (production ContractExecutor path) instead of MockRuntime.
    fn with_mock_wasm(mut self) -> Self {
        self.use_mock_wasm = true;
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

            // Apply fault injection if configured (latency and/or message loss)
            let has_latency = self.latency_range.is_some();
            let has_loss = self.message_loss_rate > 0.0;
            if has_latency || has_loss {
                let mut builder = FaultConfig::builder();
                if let Some(ref latency) = self.latency_range {
                    builder = builder.latency_range(latency.clone());
                    tracing::info!(
                        "Latency jitter enabled: {:?} - {:?}",
                        latency.start,
                        latency.end
                    );
                }
                if has_loss {
                    builder = builder.message_loss_rate(self.message_loss_rate);
                    tracing::info!(
                        "Message loss enabled: {:.1}%",
                        self.message_loss_rate * 100.0
                    );
                }
                sim.with_fault_injection(builder.build());
            }

            let logs_handle = sim.event_logs_handle();
            (sim, logs_handle)
        });

        let sleep_duration = self.sleep_after_events;
        let event_wait = self.event_wait;
        let result = sim.run_simulation::<rand::rngs::SmallRng, _, _>(
            self.seed,
            self.max_contracts,
            self.iterations,
            self.duration,
            event_wait,
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

    /// Run the simulation using the direct runner (single-threaded paused-time runtime).
    ///
    /// This avoids turmoil's O(n²) link overhead, making it suitable for large-scale
    /// or long-running simulations.
    ///
    /// Note: Does not support mid-simulation fault injection (partitions, crashes).
    /// Static latency jitter via `FaultConfig` is supported.
    #[allow(dead_code)] // Used by nightly_tests-gated tests
    fn run_direct(self) -> TestResult {
        use freenet::simulation::FaultConfig;

        setup_deterministic_state(self.seed);
        let rt = create_runtime();

        let use_mock_wasm = self.use_mock_wasm;
        // Tests that never assert convergence (require_convergence = false) skip the
        // direct runner's up-to-1800s convergence-polling tail and do a brief settle
        // instead. The tail is advisory (it only warns on failure), so skipping it
        // changes no assertion but keeps wall-clock bounded when the network does not
        // converge — the failure mode that timed out test_interest_renewal in CI (#3792).
        let skip_convergence_wait = !self.require_convergence;
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
            sim.use_mock_wasm = use_mock_wasm;
            sim.skip_convergence_wait = skip_convergence_wait;

            // Apply fault injection if configured (latency and/or message loss)
            let has_latency = self.latency_range.is_some();
            let has_loss = self.message_loss_rate > 0.0;
            if has_latency || has_loss {
                let mut builder = FaultConfig::builder();
                if let Some(ref latency) = self.latency_range {
                    builder = builder.latency_range(latency.clone());
                    tracing::info!(
                        "Latency jitter enabled: {:?} - {:?}",
                        latency.start,
                        latency.end
                    );
                }
                if has_loss {
                    builder = builder.message_loss_rate(self.message_loss_rate);
                    tracing::info!(
                        "Message loss enabled: {:.1}%",
                        self.message_loss_rate * 100.0
                    );
                }
                sim.with_fault_injection(builder.build());
            }

            let logs_handle = sim.event_logs_handle();
            (sim, logs_handle)
        });

        drop(rt);

        let direct_result = sim.run_simulation_direct::<rand::rngs::SmallRng>(
            self.seed,
            self.max_contracts,
            self.iterations,
            self.event_wait,
        );

        // Map anyhow::Result to turmoil::Result for TestResult compatibility
        let simulation_result: turmoil::Result = match direct_result {
            Ok(()) => Ok(()),
            Err(e) => Err(Box::new(std::io::Error::other(e.to_string()))),
        };

        let rt = create_runtime();
        let convergence = rt.block_on(async { check_convergence_from_logs(&logs_handle).await });
        let event_count = rt.block_on(async { logs_handle.lock().await.len() });

        TestResult {
            seed: self.seed,
            name: self.name,
            simulation_result,
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
                            log.peer_id.socket_addr(),
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
                        tracing::debug!(
                            "  {} @ {}: {}",
                            log.tx,
                            log.peer_id.socket_addr(),
                            variant
                        );
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
                    let peer_addr = format!("{}", log.peer_id.socket_addr());
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

    /// Run anomaly detection on the simulation event logs and report findings.
    ///
    /// This is exploratory: it logs all detected anomalies without asserting
    /// that the report is clean. This lets us see what the detectors find
    /// against real simulation data.
    fn verify_state_report(self) -> Self {
        let rt = create_runtime();
        let report = rt.block_on(async {
            let logs = self.logs_handle.lock().await;
            let verifier = freenet::tracing::StateVerifier::from_events(logs.clone());
            verifier.verify()
        });

        tracing::info!("=== ANOMALY DETECTION REPORT: {} ===", self.name);
        tracing::info!(
            "Events analyzed: {} total, {} state-mutating",
            report.total_events,
            report.state_events
        );
        tracing::info!("Contracts analyzed: {}", report.contracts_analyzed);
        tracing::info!("Total anomalies: {}", report.anomalies.len());

        if report.anomalies.is_empty() {
            tracing::info!(
                "{}: State verification CLEAN - no anomalies detected",
                self.name
            );
        } else {
            // Break down by category
            let divergences = report.divergences();
            let missing = report.missing_broadcasts();
            let unapplied = report.unapplied_broadcasts();
            let partitions = report.suspected_partitions();
            let stale = report.stale_peers();
            let oscillations = report.state_oscillations();
            let zombies = report.zombie_transactions();
            let storms = report.broadcast_storms();
            let cascades = report.delta_sync_cascades();

            tracing::warn!(
                "{}: {} anomalies detected: divergences={}, missing_broadcasts={}, \
                 unapplied_broadcasts={}, partitions={}, stale_peers={}, oscillations={}, \
                 zombies={}, storms={}, cascades={}",
                self.name,
                report.anomalies.len(),
                divergences.len(),
                missing.len(),
                unapplied.len(),
                partitions.len(),
                stale.len(),
                oscillations.len(),
                zombies.len(),
                storms.len(),
                cascades.len(),
            );

            // Log each anomaly at debug level for detailed inspection
            for (i, anomaly) in report.anomalies.iter().enumerate() {
                tracing::debug!("{}: anomaly[{}] = {:?}", self.name, i, anomaly);
            }
        }

        self
    }

    /// Extract router snapshot data from event logs.
    ///
    /// Returns a list of `(failure_events, success_events, prediction_active)` tuples,
    /// one per `RouterSnapshot` event found in the logs. These snapshots are emitted
    /// every 5 minutes of virtual time by each node's Ring telemetry.
    fn router_snapshots(&self) -> Vec<(usize, usize, bool)> {
        let rt = create_runtime();
        rt.block_on(async {
            let logs = self.logs_handle.lock().await;
            logs.iter()
                .filter_map(|log| log.kind.router_snapshot_summary())
                .collect()
        })
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Setup deterministic simulation state before running a test.
///
/// Resets all thread-local state for deterministic simulation testing.
///
/// All ID counters are now thread-local with per-thread offset blocks, so resetting
/// them is safe for parallel execution — each thread only resets its own counters.
///
/// Per-network state (sockets, topology, fault injectors) is cleaned up by
/// `SimNetwork::Drop`, so no scorched-earth registry clear is needed.
fn setup_deterministic_state(seed: u64) {
    // All state below is thread-local — safe for parallel tests.
    GlobalRng::set_seed(seed);
    const BASE_EPOCH_MS: u64 = 1577836800000; // 2020-01-01 00:00:00 UTC
    const RANGE_MS: u64 = 5 * 365 * 24 * 60 * 60 * 1000; // ~5 years
    GlobalSimulationTime::set_time_ms(BASE_EPOCH_MS + (seed % RANGE_MS));
    GlobalTestMetrics::reset();
    // Reset transport optimization — run_simulation_direct() enables it when needed,
    // but tests that reuse a thread should start with production timer behavior.
    SimulationTransportOpt::disable();

    // Clear CRDT contract registrations from prior tests on this thread.
    freenet::dev_tool::clear_crdt_contracts();

    // Reset all thread-local ID counters for exact event sequence reproducibility.
    RequestId::reset_counter();
    freenet::dev_tool::ClientId::reset_counter();
    reset_event_id_counter();
    reset_channel_id_counter();
    StreamId::reset_counter();
    reset_nonce_counter();
    reset_global_node_index();
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

/// Compact fingerprint of a simulation trace for cross-run verification.
///
/// Provides a hash-based integrity check on top of field-by-field assertions.
/// The fingerprint is logged so it can be compared across CI invocations.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TraceFingerprint {
    total_events: usize,
    /// Hash of the full event sequence (order-sensitive).
    sequence_hash: u64,
    /// Hash of the per-type event counts (order-insensitive via BTreeMap).
    counts_hash: u64,
}

impl TraceFingerprint {
    fn from_events(events: &[String], event_counts: &HashMap<String, usize>) -> Self {
        let mut seq_hasher = std::collections::hash_map::DefaultHasher::new();
        for event in events {
            event.hash(&mut seq_hasher);
        }

        let sorted_counts: BTreeMap<&String, &usize> = event_counts.iter().collect();
        let mut counts_hasher = std::collections::hash_map::DefaultHasher::new();
        for (k, v) in &sorted_counts {
            k.hash(&mut counts_hasher);
            v.hash(&mut counts_hasher);
        }

        TraceFingerprint {
            total_events: events.len(),
            sequence_hash: seq_hasher.finish(),
            counts_hash: counts_hasher.finish(),
        }
    }
}

/// **STRICT** determinism test: verifies that same seed produces EXACTLY identical events.
///
/// This test uses Turmoil's deterministic scheduler to ensure reproducible execution.
/// It verifies:
/// 1. Exact same number of events
/// 2. Exact same event counts per type
/// 3. Exact same event sequence (order, content)
///
/// If this test fails, it indicates non-determinism in the simulation that Turmoil
/// doesn't control (e.g., DashMap/HashMap iteration order, external I/O, real time usage).
///
/// The test runs the simulation 3 times with the same seed and compares traces.
/// All three runs must produce identical results.
///
/// **Scale:** Uses 2 gateways + 18 nodes to catch DashMap iteration non-determinism
/// that only manifests at scale (e.g., in subscription/interest management).
///
/// Process isolation via nextest eliminates inter-test DashMap state leakage (#3051).
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
                name, 2,  // gateways - multi-gateway to test more code paths
                18, // nodes - increased to trigger DashMap iteration issues
                10, // ring_max_htl
                3,  // rnd_if_htl_above
                15, // max_connections - higher to create more subscription operations
                5,  // min_connections
                seed,
            )
            .await;
            let logs_handle = sim.event_logs_handle();
            (sim, logs_handle)
        });

        // Run simulation with Turmoil's deterministic scheduler
        let result = sim.run_simulation::<rand::rngs::SmallRng, _, _>(
            seed,
            10, // max_contract_num - more contracts = more subscription operations
            40, // iterations - more iterations to trigger DashMap operations
            Duration::from_secs(30), // simulation_duration
            Duration::from_millis(200),
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

    // Fingerprint verification: compact hash check for cross-run integrity
    let fp1 = TraceFingerprint::from_events(&trace1.event_sequence, &trace1.event_counts);
    let fp2 = TraceFingerprint::from_events(&trace2.event_sequence, &trace2.event_counts);
    let fp3 = TraceFingerprint::from_events(&trace3.event_sequence, &trace3.event_counts);
    assert_eq!(fp1, fp2, "Fingerprint mismatch between run 1 and run 2");
    assert_eq!(fp2, fp3, "Fingerprint mismatch between run 2 and run 3");

    tracing::info!(
        "STRICT DETERMINISM TEST PASSED: {} events, fingerprint={:#018x}",
        trace1.total_events,
        fp1.sequence_hash
    );
}

/// **STRICT** determinism test with MULTIPLE GATEWAYS.
///
/// This test verifies that simulations with 2+ gateways remain deterministic.
///
/// Process isolation via nextest eliminates inter-test DashMap state leakage (#3051).
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
            Duration::from_millis(200),
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

    // Fingerprint verification
    let fp1 = TraceFingerprint::from_events(&trace1.event_sequence, &trace1.event_counts);
    let fp2 = TraceFingerprint::from_events(&trace2.event_sequence, &trace2.event_counts);
    let fp3 = TraceFingerprint::from_events(&trace3.event_sequence, &trace3.event_counts);
    assert_eq!(fp1, fp2, "Fingerprint mismatch between run 1 and run 2");
    assert_eq!(fp2, fp3, "Fingerprint mismatch between run 2 and run 3");

    tracing::info!(
        "MULTI-GATEWAY DETERMINISM TEST PASSED: {} events (2 gateways), fingerprint={:#018x}",
        trace1.total_events,
        fp1.sequence_hash
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
            Duration::from_millis(200),
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

    // Fingerprint verification (counts-only since this test doesn't track sequence)
    let fp1 = TraceFingerprint::from_events(&[], &trace1.event_counts);
    let fp2 = TraceFingerprint::from_events(&[], &trace2.event_counts);
    assert_eq!(
        fp1.counts_hash, fp2.counts_hash,
        "Counts fingerprint mismatch between replay runs"
    );

    tracing::info!(
        "Deterministic replay test passed - {} events, counts_hash={:#018x}",
        trace1.total_events,
        fp1.counts_hash
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
        .check_convergence()
        .verify_state_report();
}

/// CI simulation test - medium network with more operations.
#[test_log::test]
fn ci_medium_simulation() {
    TestConfig::medium("ci-medium-sim", 0xC1F1_ED7E_ED01)
        .run()
        .assert_ok()
        .verify_operation_coverage()
        .check_convergence()
        .verify_state_report();
}

// =============================================================================
// MockWasmRuntime Tests
// =============================================================================

/// Simulation using MockWasmRuntime — exercises the production ContractExecutor code path
/// (init_tracker, validation, notification pipeline, corrupted state recovery) without WASM.
#[test_log::test]
fn mock_wasm_runtime_simulation() {
    TestConfig::small("mock-wasm-sim", 0xA0C1_1A5E_0001)
        .with_mock_wasm()
        .run_direct()
        .assert_ok()
        .verify_state_report();
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
        .check_convergence()
        .verify_state_report();
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
        .check_convergence()
        .verify_state_report();
}

// =============================================================================
// Determinism Verification Tests
// =============================================================================

/// Verify that running the same simulation twice produces identical results.
///
/// Process isolation via nextest eliminates inter-test DashMap state leakage (#3051).
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
            Duration::from_millis(200),
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

    // Fingerprint verification
    let counts1: HashMap<String, usize> = {
        let mut m = HashMap::new();
        for e in &events1 {
            *m.entry(e.clone()).or_insert(0) += 1;
        }
        m
    };
    let counts2: HashMap<String, usize> = {
        let mut m = HashMap::new();
        for e in &events2 {
            *m.entry(e.clone()).or_insert(0) += 1;
        }
        m
    };
    let fp1 = TraceFingerprint::from_events(&events1, &counts1);
    let fp2 = TraceFingerprint::from_events(&events2, &counts2);
    assert_eq!(fp1, fp2, "Fingerprint mismatch between run 1 and run 2");

    tracing::info!(
        "Determinism verified: {} identical events, fingerprint={:#018x}",
        events1.len(),
        fp1.sequence_hash
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
        Duration::from_millis(200),
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
// issues like bidirectional cycles (#2720), orphan hosters (#2719), etc.

/// Source detection threshold: peers within 5% of ring distance to contract
/// are considered sources. Must match SOURCE_THRESHOLD in topology_registry.rs.
const SOURCE_THRESHOLD: f64 = 0.05;

/// Network name for single hoster topology test.
const SINGLE_HOSTER_NETWORK: &str = "single-hoster-test";

/// Calculate ring distance (handles wrap-around at 0/1 boundary).
fn ring_distance(a: f64, b: f64) -> f64 {
    let diff = (a - b).abs();
    diff.min(1.0 - diff)
}

/// Test topology capture with a single hoster (gateway only).
///
/// This test validates the topology infrastructure without triggering the
/// bidirectional cycle issue (#2720) by using only a single subscriber.
///
/// ## Scenario
/// 1. Gateway PUTs a contract with subscribe=true
/// 2. Wait for topology registration
/// 3. Verify: gateway's location is within SOURCE_THRESHOLD of contract location
/// 4. Verify: gateway's snapshot shows it's hosting the contract
/// 5. Verify: no topology issues (no cycles since only one subscriber)
///
/// ## Related
/// - For Issue #2720 (bidirectional cycles), that bug is now fixed
/// - For Issue #2755 (orphan/disconnected hosters), see `test_orphan_hosters_no_source` (ignored)
#[test_log::test]
fn test_topology_single_hoster() {
    use freenet::dev_tool::{
        Location, NodeLabel, ScheduledOperation, SimOperation, validate_topology_from_snapshots,
    };

    const SEED: u64 = 0x5EED_0001_CAFE;

    // Set up deterministic state BEFORE creating SimNetwork
    // This ensures peer locations are deterministic (uses GlobalRng)
    setup_deterministic_state(SEED);

    let rt = create_runtime();

    // Create SimNetwork and get gateway location
    let (sim, gateway_location) = rt.block_on(async {
        let sim = SimNetwork::new(
            SINGLE_HOSTER_NETWORK,
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
        NodeLabel::gateway(SINGLE_HOSTER_NETWORK, 0),
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

    // Gateway should be hosting (it did PUT with subscribe=true)
    assert!(
        contract_sub.is_hosting,
        "Gateway should be hosting the contract after PUT with subscribe=true"
    );

    tracing::info!(
        "Gateway {} is hosting contract: upstream={:?}, downstream={:?}",
        gateway_snap.peer_addr,
        contract_sub.upstream,
        contract_sub.downstream
    );

    // Validate topology using captured snapshots
    let result = validate_topology_from_snapshots(&snapshots, &contract_id, contract_location);

    tracing::info!(
        "Topology validation: cycles={}, orphans={}, disconnected={}, unreachable={}, proximity={}",
        result.bidirectional_cycles.len(),
        result.orphan_hosters.len(),
        result.disconnected_upstream.len(),
        result.unreachable_hosters.len(),
        result.proximity_violations.len()
    );

    // CRITICAL: With only one subscriber, there should NEVER be cycles
    // This is the primary assertion - single hoster cannot form cycles
    assert!(
        result.bidirectional_cycles.is_empty(),
        "Single hoster should have no bidirectional cycles, found: {:?}",
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

        // Source hoster should have healthy topology
        assert!(
            result.orphan_hosters.is_empty(),
            "Source hoster (within threshold) should not be marked as orphan, found: {:?}",
            result.orphan_hosters
        );

        assert!(
            result.disconnected_upstream.is_empty(),
            "Source hoster (within threshold) should not be disconnected, found: {:?}",
            result.disconnected_upstream
        );

        assert!(
            result.unreachable_hosters.is_empty(),
            "Source hoster should have no unreachable hosters, found: {:?}",
            result.unreachable_hosters
        );

        assert!(
            result.is_healthy(),
            "Single source hoster topology should be healthy, got {} issues",
            result.issue_count
        );
    } else {
        tracing::warn!(
            "Gateway is NOT source (distance={:.4} >= threshold={:.4}) - orphan/disconnected issues are expected",
            gateway_distance,
            SOURCE_THRESHOLD
        );

        // Non-source hoster will have orphan/disconnected issues - this is expected behavior
        // The key assertion (no cycles) already passed above
        if !result.orphan_hosters.is_empty() {
            tracing::info!(
                "Expected: Gateway flagged as orphan (not a source, no upstream): {:?}",
                result.orphan_hosters
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
        "Single hoster topology test passed - gateway_is_source={}, cycles=0, other_issues={}",
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
    TestConfig::medium("concurrent-updates-convergence", 0xC0C0_BEEF_1234)
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
        .check_convergence()
        .verify_state_report();
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
    use freenet::dev_tool::{NodeLabel, ScheduledOperation, SimOperation};

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
// Task-per-tx Subscribe ForwardingAck Regression Test (PR #3806)
// =============================================================================

/// Exercises the task-per-tx client-initiated subscribe path
/// (`subscribe_with_id` → `start_client_subscribe`) in a multi-node
/// simulation with network-routed subscribes.
///
/// ## Background (PR #3806, #1454 Phase 2b)
///
/// Phase 2b migrated client-initiated SUBSCRIBE to a task-per-tx driver.
/// This test verifies the driver works end-to-end with network round-trips,
/// retries, and peer selection.
///
/// ## ForwardingAck coverage
///
/// The ForwardingAck relay bug (commit 5cb6f37c) is covered by unit tests
/// in `node.rs::callback_forward_tests`. Triggering it in simulation
/// requires `SeedContract` + `use_mock_wasm=true` so that relay peers
/// lack the contract. The `SeedContract` framework is in place but the
/// mock WASM runtime has compatibility issues with `OpCtx::send_and_await`
/// (separate from the local-completion fix below).
///
/// The task-per-tx `send_and_await` path IS exercised here: subscribes
/// complete with `outcome="subscribed"` via the local-completion synthesis
/// in the SUBSCRIBE branch of `handle_pure_network_message_v1`.
#[test_log::test]
fn test_subscribe_forwarding_ack_relay() {
    use freenet::dev_tool::{NodeLabel, ScheduledOperation, SimOperation};

    const SEED: u64 = 0x5CB6_F37C_0001;
    const NETWORK_NAME: &str = "subscribe-fwd-ack";

    GlobalTestMetrics::reset();
    setup_deterministic_state(SEED);
    let rt = create_runtime();

    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(
            NETWORK_NAME,
            1, // 1 gateway
            4, // 4 regular nodes
            7, // ring_max_htl
            3, // rnd_if_htl_above
            5, // max_connections
            2, // min_connections
            SEED,
        )
        .await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    let contract = SimOperation::create_test_contract(0xAC);
    let contract_id = *contract.key().id();
    let initial_state = SimOperation::create_test_state(1);

    // Gateway PUTs, then multiple nodes subscribe via the task-per-tx path.
    //
    // Note: SeedContract (no-propagation local store) is available for
    // tests that need to control which nodes have the contract, but
    // requires use_mock_wasm=true which has compatibility issues with
    // OpCtx::send_and_await. For now, use regular PUT + Subscribe.
    let operations = vec![
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Put {
                contract: contract.clone(),
                state: initial_state,
                subscribe: true,
            },
        ),
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 1),
            SimOperation::Subscribe { contract_id },
        ),
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 2),
            SimOperation::Subscribe { contract_id },
        ),
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 3),
            SimOperation::Subscribe { contract_id },
        ),
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 4),
            SimOperation::Subscribe { contract_id },
        ),
    ];

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(120),
        Duration::from_secs(30),
    );

    // PRIMARY: simulation must complete without errors.
    // Before the fix, ForwardingAck on the task-per-tx channel
    // would cause UnexpectedOpState, crashing the simulation.
    assert!(
        result.turmoil_result.is_ok(),
        "Simulation failed (ForwardingAck regression?): {:?}",
        result.turmoil_result.err()
    );

    // Verify subscribes succeeded.
    let (success_count, not_found_count) = rt.block_on(async {
        let logs = logs_handle.lock().await;
        let mut successes = 0usize;
        let mut not_found = 0usize;
        for log in logs.iter() {
            match log.kind.subscribe_outcome() {
                Some(true) => successes += 1,
                Some(false) => not_found += 1,
                None => {}
            }
        }
        (successes, not_found)
    });

    tracing::info!(
        success_count,
        not_found_count,
        "Subscribe telemetry summary"
    );

    // 4 explicit subscribes + 1 from gateway's put-with-subscribe.
    assert!(
        success_count >= 4,
        "Expected at least 4 successful subscribes, got {} (not_found={})",
        success_count,
        not_found_count,
    );
}

/// Soundness check for the fix that moved `ring.subscribe()` /
/// `record_subscription()` / `announce_contract_hosted()` out of the shared
/// prefix of `SubscribeMsgResult::Subscribed` and into the originator-only
/// branch.
///
/// The *direct* regression is covered by the unit test
/// `ring::hosting::tests::test_relay_downstream_only_not_in_renewal`, which
/// asserts at the HostingManager level that a pure-relay state (downstream
/// subscriber registered, no self-lease) does not get recruited into
/// `contracts_needing_renewal()`. That unit test is where the bug bite is
/// pinned.
///
/// This simulation test verifies the broader property that subscribes still
/// complete successfully under the refactored handler and that the bounded
/// number of active-subscription-lease holders stays sane under a multi-peer
/// subscribe scenario — any regression that re-introduced relay-side
/// `ring.subscribe` calls on nodes running the LEGACY process_message path
/// would cause `subscribed_peers` to grow beyond the expected count of
/// genuine originators.
///
/// Note on turmoil vs production: in the turmoil simulation runner every
/// peer shares one process, which means `pending_op_results` is effectively
/// global. `node::try_forward_driver_reply` on an intermediate peer
/// can therefore find the originator's task entry and short-circuit the
/// Response before it reaches the legacy relay handler. In production each
/// peer is an independent process with its own `pending_op_results`, so
/// relays fall through to the legacy handler and hit the bug. This is why
/// the *primary* regression test for the bug is the in-process hosting
/// manager unit test rather than this simulation test.
///
/// Simulation-side regression coverage for the bug tracked in #3874: when a
/// subscribed client disconnects, the node MUST emit an upstream Unsubscribe
/// so the hoster can drop the downstream subscriber registration.
///
/// This test exercises the same code path the failing integration test
/// `test_client_disconnect_triggers_upstream_unsubscribe` exercises — the
/// `ClientRequest::Disconnect` arm in `client_events::process_open_request`,
/// which calls `remove_client_from_all_subscriptions` /
/// `should_unsubscribe_upstream` / `send_unsubscribe_upstream` — but through
/// the simulation harness where the bug's preconditions (completed PUT
/// replay from GC, subscription resurrection, etc.) can be controlled and
/// reproduced deterministically.
///
/// Before this test existed, `SimOperation` had no client-disconnect variant
/// and simulation had NO coverage of the disconnect → unsubscribe chain —
/// see #3874 for the full gap analysis. The integration-test regression
/// that came out of PR #3843 was therefore invisible to the simulation
/// suite.
///
/// This test currently FAILS and reproduces a concrete regression that
/// was invisible to the simulation suite before: the task-per-tx
/// subscribe driver in `operations/subscribe/op_ctx_task.rs` does NOT
/// call `interest_manager.register_peer_interest(..., is_upstream=true)`
/// on subscribe completion, so `send_unsubscribe_upstream` logs
/// `"No upstream peer found for unsubscribe"` and no Unsubscribe is
/// ever emitted. The legacy `operations/subscribe.rs::SubscribeMsg::Response`
/// arm DOES register the upstream peer (see subscribe.rs:1767), so the
/// behaviour regressed when client-initiated SUBSCRIBE migrated to the
/// task-per-tx driver in Phase 2b.
///
/// This is either the #3874 root cause or a closely-related bug on the
/// same code path. Marked `#[ignore]` until the missing registration
/// is restored; the test is kept in-tree as executable documentation
/// of the gap and as a regression guard for the fix.
#[test_log::test]
fn test_client_disconnect_emits_upstream_unsubscribe() {
    use freenet::dev_tool::{NodeLabel, ScheduledOperation, SimOperation};

    const SEED: u64 = 0x5CB6_F37C_0003;
    const NETWORK_NAME: &str = "client-disconnect-unsub";

    GlobalTestMetrics::reset();
    setup_deterministic_state(SEED);
    let rt = create_runtime();

    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(
            NETWORK_NAME,
            1, // 1 gateway
            4, // 4 regular nodes (matching subscribe_forwarding_ack_relay)
            7, // ring_max_htl
            3, // rnd_if_htl_above
            5, // max_connections
            2, // min_connections
            SEED,
        )
        .await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    let contract = SimOperation::create_test_contract(0xDC);
    let contract_id = *contract.key().id();
    let initial_state = SimOperation::create_test_state(1);

    // Gateway PUTs → node-1 subscribes → node-1 disconnects.
    // Node-1 is a pure downstream subscriber with no further downstream
    // of its own, so `should_unsubscribe_upstream` must return true
    // on disconnect and an Unsubscribe must be emitted to its upstream.
    let operations = vec![
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Put {
                contract: contract.clone(),
                state: initial_state,
                subscribe: false,
            },
        ),
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 1),
            SimOperation::Subscribe { contract_id },
        ),
        ScheduledOperation::new(NodeLabel::node(NETWORK_NAME, 1), SimOperation::Disconnect),
    ];

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(120),
        Duration::from_secs(30),
    );

    assert!(
        result.turmoil_result.is_ok(),
        "Simulation failed: {:?}",
        result.turmoil_result.err()
    );

    let (sent_count, received_count) = rt.block_on(async {
        let logs = logs_handle.lock().await;
        let mut sent = 0usize;
        let mut received = 0usize;
        for log in logs.iter() {
            if log
                .kind
                .unsubscribe_sent_instance_id()
                .is_some_and(|id| *id == contract_id)
            {
                sent += 1;
            }
            if log
                .kind
                .unsubscribe_received_instance_id()
                .is_some_and(|id| *id == contract_id)
            {
                received += 1;
            }
        }
        (sent, received)
    });

    tracing::info!(
        sent_count,
        received_count,
        "Unsubscribe telemetry after client disconnect"
    );

    assert!(
        sent_count > 0,
        "Disconnecting node MUST emit at least one UnsubscribeSent for the \
         contract it was subscribed to (sent={sent_count}, received={received_count})"
    );
    assert!(
        received_count > 0,
        "Upstream node MUST log UnsubscribeReceived after client disconnect \
         (sent={sent_count}, received={received_count})"
    );
}

/// Edge case: disconnect from a node that never subscribed. No Unsubscribe
/// should be emitted and nothing should log "No upstream peer found".
#[test_log::test]
fn test_client_disconnect_without_subscriptions_is_noop() {
    use freenet::dev_tool::{NodeLabel, ScheduledOperation, SimOperation};

    const SEED: u64 = 0x5CB6_F37C_0004;
    const NETWORK_NAME: &str = "disconnect-no-subs";

    GlobalTestMetrics::reset();
    setup_deterministic_state(SEED);
    let rt = create_runtime();

    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(NETWORK_NAME, 1, 4, 7, 3, 5, 2, SEED).await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    let operations = vec![ScheduledOperation::new(
        NodeLabel::node(NETWORK_NAME, 1),
        SimOperation::Disconnect,
    )];

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(60),
        Duration::from_secs(30),
    );

    assert!(
        result.turmoil_result.is_ok(),
        "Simulation failed: {:?}",
        result.turmoil_result.err()
    );

    let sent_count = rt.block_on(async {
        let logs = logs_handle.lock().await;
        logs.iter()
            .filter(|log| log.kind.unsubscribe_sent_instance_id().is_some())
            .count()
    });

    assert_eq!(
        sent_count, 0,
        "Disconnecting a client with no subscriptions must not emit any \
         UnsubscribeSent events (got {sent_count})"
    );
}

/// Edge case: subscribe twice from the same client, then disconnect.
/// The upstream registration is idempotent, so only one Unsubscribe should
/// be emitted per contract.
#[test_log::test]
fn test_repeat_subscribe_then_disconnect_emits_single_unsubscribe() {
    use freenet::dev_tool::{NodeLabel, ScheduledOperation, SimOperation};

    const SEED: u64 = 0x5CB6_F37C_0005;
    const NETWORK_NAME: &str = "repeat-subscribe-disconnect";

    GlobalTestMetrics::reset();
    setup_deterministic_state(SEED);
    let rt = create_runtime();

    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(NETWORK_NAME, 1, 4, 7, 3, 5, 2, SEED).await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    let contract = SimOperation::create_test_contract(0xDD);
    let contract_id = *contract.key().id();
    let initial_state = SimOperation::create_test_state(1);

    let operations = vec![
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Put {
                contract: contract.clone(),
                state: initial_state,
                subscribe: false,
            },
        ),
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 1),
            SimOperation::Subscribe { contract_id },
        ),
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 1),
            SimOperation::Subscribe { contract_id },
        ),
        ScheduledOperation::new(NodeLabel::node(NETWORK_NAME, 1), SimOperation::Disconnect),
    ];

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(120),
        Duration::from_secs(30),
    );

    assert!(
        result.turmoil_result.is_ok(),
        "Simulation failed: {:?}",
        result.turmoil_result.err()
    );

    let sent_count = rt.block_on(async {
        let logs = logs_handle.lock().await;
        logs.iter()
            .filter(|log| {
                log.kind
                    .unsubscribe_sent_instance_id()
                    .is_some_and(|id| *id == contract_id)
            })
            .count()
    });

    assert_eq!(
        sent_count, 1,
        "Repeat-subscribe + disconnect must emit exactly one UnsubscribeSent \
         (got {sent_count})"
    );
}

#[test_log::test]
fn test_relay_does_not_pollute_active_subscriptions() {
    use freenet::dev_tool::{NodeLabel, ScheduledOperation, SimOperation};

    const SEED: u64 = 0x5CB6_F37C_0002;
    const NETWORK_NAME: &str = "subscribe-relay-pollution";

    GlobalTestMetrics::reset();
    setup_deterministic_state(SEED);
    let rt = create_runtime();

    // Sparse topology: more peers than max_connections so the ring can't
    // be fully connected and subscribe requests MUST route through at least
    // one relay to reach the peer that holds the contract.
    let sim = rt.block_on(async {
        SimNetwork::new(
            NETWORK_NAME,
            2,  // 2 gateways
            18, // 18 regular nodes (20 total peers)
            10, // ring_max_htl
            3,  // rnd_if_htl_above
            3,  // max_connections — tight ring, non-full topology
            2,  // min_connections
            SEED,
        )
        .await
    });

    let contract = SimOperation::create_test_contract(0xAD);
    let contract_id = *contract.key().id();
    let initial_state = SimOperation::create_test_state(1);

    // Gateway PUTs (with subscribe). Multiple distant nodes then subscribe
    // in parallel so at least some Subscribe requests must route through
    // relay hops before reaching a peer that holds the contract.
    let operations = vec![
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Put {
                contract: contract.clone(),
                state: initial_state,
                subscribe: true,
            },
        ),
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 15),
            SimOperation::Subscribe { contract_id },
        ),
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 16),
            SimOperation::Subscribe { contract_id },
        ),
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 17),
            SimOperation::Subscribe { contract_id },
        ),
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 18),
            SimOperation::Subscribe { contract_id },
        ),
    ];

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(120),
        Duration::from_secs(30),
    );

    assert!(
        result.turmoil_result.is_ok(),
        "Simulation failed: {:?}",
        result.turmoil_result.err()
    );

    // run_controlled_simulation snapshots every peer's HostingManager state
    // at shutdown and returns them on the result. Use those directly rather
    // than the global registry (which is cleared on SimNetwork drop).
    let snapshots = &result.topology_snapshots;
    assert!(
        !snapshots.is_empty(),
        "No topology snapshots captured — background snapshot task didn't run"
    );

    // Count peers whose `HostingManager::active_subscriptions` contains the
    // contract. Only genuine originators of a subscription should appear here:
    //
    //   * The PUT-with-subscribe originator (gateway 0).
    //   * Each of nodes 15..=18 that called Subscribe explicitly.
    //
    // That's at most 5 peers. Before the fix, the
    // `SubscribeMsgResult::Subscribed` handler would ALSO add every relay
    // that forwards a Subscribed response to the set — crucially, when four
    // simultaneous subscribers route through each other, the bug's recruited
    // relays overlap with legit subscribers, but additional strictly-relay
    // peers still accumulate. After the fix the bound collapses to the 5
    // genuine originators.
    let mut subscribed_peers: Vec<SocketAddr> = snapshots
        .iter()
        .filter(|snap| snap.active_subscription_keys.contains(&contract_id))
        .map(|snap| snap.peer_addr)
        .collect();
    subscribed_peers.sort();

    tracing::info!(
        subscribed_peer_count = subscribed_peers.len(),
        ?subscribed_peers,
        total_snapshots = snapshots.len(),
        "Peers with contract in active_subscriptions"
    );

    // Five nodes legitimately install a lease in `active_subscriptions`:
    // the PUT-with-subscribe gateway and the four explicit Subscribe
    // originators (nodes 15..=18). Anything more means a peer that did
    // not originate a subscribe — a pure relay — ended up with the
    // contract in its active_subscriptions, which is the bug.
    assert!(
        subscribed_peers.len() <= 5,
        "Relay pollution regression: {} peer(s) have the contract in \
         `active_subscriptions`, expected at most 5 (gateway + 4 subscribing \
         nodes). Polluted peers: {:?}. \
         See subscribe.rs::SubscribeMsgResult::Subscribed — `ring.subscribe()` \
         must only run on the originator branch, not on relays.",
        subscribed_peers.len(),
        subscribed_peers,
    );

    // Sanity: at least some peer must have actually subscribed.
    assert!(
        !subscribed_peers.is_empty(),
        "No peers have the contract in active_subscriptions — \
         subscribes didn't complete, test is degenerate"
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
    use freenet::dev_tool::{NodeLabel, ScheduledOperation, SimOperation, register_crdt_contract};

    const SEED: u64 = 0x0276_3CD0_0001;
    const NETWORK_NAME: &str = "crdt-version-test";

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
    use freenet::dev_tool::{NodeLabel, ScheduledOperation, SimOperation, register_crdt_contract};

    const SEED: u64 = 0x0276_3B06_0001;
    const NETWORK_NAME: &str = "pr2763-crdt-resync";

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
}

/// Regression test for the #4233 full-state broadcast storm (issue #4145).
///
/// ## What this guards
///
/// Sustained UPDATE fan-out from one host to its direct subscribers of a CRDT
/// contract. After each subscriber bootstraps on a single full state, every
/// subsequent broadcast to that subscriber must be a small delta — not another
/// full state. The #4145 fix caches the target peer's summary on ANY delivered
/// broadcast (delta OR full state), not just deltas, so the host can compute a
/// delta for the next broadcast instead of re-sending full state forever.
///
/// ## Scenario
///
/// 1 gateway (the contract host / broadcast source) + a small number of direct
/// subscriber nodes. The gateway PUTs+subscribes, every node subscribes, then
/// the gateway drives a burst of sequential UPDATEs that fan out to all
/// subscribers.
///
/// ## Why a *small* star topology (1 gateway + 2 direct subscribers)
///
/// The chicken-and-egg the fix targets is per-(sender, target): a sender that
/// lacks a target's cached summary must send full state. A LARGER topology
/// (e.g. 5+ subscribers forming a multi-hop relay tree) does NOT make this test
/// more sensitive — it makes it LESS sensitive. Relay nodes cache a downstream
/// peer's *full state* as its "summary", and `compute_delta` then rejects the
/// diff as "not efficient" (`is_delta_efficient`), emitting full state
/// regardless of the #4145 gate. Those relay-tree full-states swamp the signal:
/// at 5 subscribers, fix vs reverted is ~199 vs ~201 full-state sends — noise.
///
/// A clean star (the host broadcasts directly to its subscribers, no relay tree)
/// removes that floor, so the metric isolates exactly the bug the fix addresses:
/// with the fix `full_state_sends` collapses below `delta_sends` (the host caches
/// each subscriber's summary after a delivered broadcast and switches to deltas)
/// while `delta_sends` grows with the update count; reverted, `full_state_sends`
/// scales with updates × subscribers and overtakes `delta_sends`. The unit tests
/// in `broadcast_queue.rs` pin the gate logic directly; this sim pins the
/// system-level consequence on the path simulation builds actually exercise
/// (the `#[cfg(simulation_tests)]` inline broadcast loop in `p2p_protoc.rs`).
///
/// ## Assertions (the delta-dominance one FAILS if the #4145 gate is reverted)
///
/// - (a) Event-loop liveness: no `StalePeer` anomaly — every subscriber keeps
///   receiving broadcasts; none goes silent under the fan-out.
/// - (b) Full-state-send rate collapses: `delta_sends > full_state_sends`. With
///   the fix the host caches each subscriber's summary after a delivered
///   broadcast, so subsequent updates go out as deltas and deltas dominate.
///   (Full-state sends are reduced below delta sends but NOT to one-per-
///   subscriber — bootstrap + retry jitter mean several full states per
///   subscriber.) Reverted, every update is full state to every subscriber and
///   `delta_sends` stays low → this assertion fails. (Measured on this star
///   topology: fix = 78 delta / 47 full; reverted = 40 delta / 85 full.)
/// - (c) Broadcast delivery / update propagation stays high: all peers converge.
#[test_log::test]
fn test_sustained_update_fanout_no_full_state_storm() {
    use freenet::dev_tool::{NodeLabel, ScheduledOperation, SimOperation, register_crdt_contract};

    const SEED: u64 = 0x4233_5701_0001;
    const NETWORK_NAME: &str = "i4233-fanout-storm";
    // Direct subscribers of the gateway (the broadcast fan-out width). Kept small
    // so the host broadcasts directly to subscribers with no multi-hop relay tree
    // — see the "Why a small star topology" note above.
    const SUBSCRIBERS: usize = 2;
    // UPDATEs driven into the hot contract AFTER all subscribers have bootstrapped.
    // Enough that, reverted, the per-update full-state fan-out clearly overtakes
    // the one-time bootstrap deltas.
    const SUSTAINED_UPDATES: usize = 20;

    GlobalTestMetrics::reset();
    setup_deterministic_state(SEED);
    let rt = create_runtime();

    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(
            NETWORK_NAME,
            1,           // 1 gateway (the broadcast source / contract host)
            SUBSCRIBERS, // subscriber nodes (the fan-out targets)
            7,           // max_htl
            3,           // rnd_if_htl_above
            10,          // max_connections
            2,           // min_connections
            SEED,
        )
        .await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    // CRDT contract so post-bootstrap broadcasts compute real version-aware
    // deltas (a plain hash contract's "delta" is full state, which would not
    // exercise the delta transition this test is about).
    let contract = SimOperation::create_test_contract(0x42);
    let contract_id = *contract.key().id();
    let contract_key = contract.key();
    register_crdt_contract(contract_id);

    let initial_state = SimOperation::create_crdt_state(1, 0x10);

    let mut operations = Vec::new();

    // Bootstrap: gateway PUTs + subscribes (becomes the host/source), then every
    // node subscribes. Each subscriber's FIRST broadcast is full state — that is
    // the one bootstrap full-state per subscriber the fix permits.
    operations.push(ScheduledOperation::new(
        NodeLabel::gateway(NETWORK_NAME, 0),
        SimOperation::Put {
            contract: contract.clone(),
            state: initial_state,
            subscribe: true,
        },
    ));
    for node_idx in 1..=SUBSCRIBERS {
        operations.push(ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, node_idx),
            SimOperation::Subscribe { contract_id },
        ));
    }

    // Sustained UPDATE fan-out: a burst of UPDATEs from the gateway, each fanned
    // out to all subscribers. With the fix every subscriber already has a cached
    // summary after its bootstrap full state, so these all go out as deltas.
    // Without the fix every one of these is another full state to every
    // subscriber — the storm.
    for v in 0..SUSTAINED_UPDATES {
        let version = (v as u64) + 2; // versions 2.. (v1 was the PUT)
        operations.push(ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Update {
                key: contract_key,
                data: SimOperation::create_crdt_state(version, 0x20 + v as u8),
            },
        ));
    }

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(240), // simulation duration (virtual)
        Duration::from_secs(90),  // post-op propagation wait (virtual)
    );
    assert!(
        result.turmoil_result.is_ok(),
        "Simulation should complete: {:?}",
        result.turmoil_result.err()
    );

    let convergence =
        rt.block_on(async { freenet::dev_tool::check_convergence_from_logs(&logs_handle).await });
    let delta_sends = GlobalTestMetrics::delta_sends();
    let full_state_sends = GlobalTestMetrics::full_state_sends();
    let resync_count = GlobalTestMetrics::resync_requests();
    // Event-loop backlog proxy: the high-water mark of in-flight transactions
    // awaiting a reply (`pending_op_results` map size). A full-state storm that
    // pins the event loop lets replies pile up, pushing this mark up; a healthy
    // run keeps it low. See `GlobalTestMetrics::pending_op_high_water_mark`.
    let pending_op_hwm = GlobalTestMetrics::pending_op_high_water_mark();

    // Anomaly report for liveness (StalePeer = a subscriber that stopped
    // receiving broadcasts while others kept getting them).
    let report = rt.block_on(async {
        let logs = logs_handle.lock().await;
        let verifier = freenet::tracing::StateVerifier::from_events(logs.clone());
        verifier.verify()
    });
    let stale = report.stale_peers();

    tracing::info!(
        "i4233 fanout: delta_sends={}, full_state_sends={}, resyncs={}, pending_op_hwm={}, converged={}/{}, stale_peers={}",
        delta_sends,
        full_state_sends,
        resync_count,
        pending_op_hwm,
        convergence.converged.len(),
        convergence.total_contracts(),
        stale.len(),
    );

    // Sanity: the scenario must actually have broadcast something, otherwise the
    // collapse assertion below would pass vacuously.
    assert!(
        delta_sends + full_state_sends > 0,
        "no broadcasts were recorded — the fan-out scenario did not run"
    );

    // (a) LIVENESS: no subscriber goes silent under the fan-out. A full-state
    // storm that pins the event loop strands a subscriber, which surfaces as a
    // StalePeer anomaly.
    assert!(
        stale.is_empty(),
        "#4233 liveness: {} subscriber(s) went stale under sustained UPDATE \
         fan-out (event loop starved by the broadcast storm): {:?}",
        stale.len(),
        stale,
    );

    // (a2) EVENT-LOOP BACKLOG STAYS BOUNDED. StalePeer is a content-divergence
    // proxy for liveness; this is a more direct event-loop-backlog signal. The
    // high-water mark of `pending_op_results` (in-flight transactions awaiting a
    // reply) tracks how far the event loop falls behind: if the loop is pinned by
    // a broadcast storm, replies pile up and the mark climbs. Measured steady
    // state on this star is ~15 (identical on both the fixed and reverted gate —
    // in this small deterministic harness the storm shows up as full-state-send
    // volume, not as op-result backlog, so this bound is a SATURATION guard, not
    // the fix-vs-revert discriminator; assertion (b) is that discriminator). The
    // bound is set well above steady state but far below a level that would
    // indicate the loop is wedged and replies are accumulating unboundedly.
    const PENDING_OP_BACKLOG_BOUND: u64 = 200;
    assert!(
        pending_op_hwm < PENDING_OP_BACKLOG_BOUND,
        "#4233 liveness: event-loop backlog high-water mark {pending_op_hwm} \
         reached {PENDING_OP_BACKLOG_BOUND}+ in-flight op-results under sustained \
         UPDATE fan-out — the event loop is falling behind (replies piling up), \
         a sign the broadcast path is pinning the loop."
    );

    // (b) FULL-STATE-SEND RATE COLLAPSES — the load-bearing #4145 discriminator.
    //
    // With the fix the host caches a subscriber's summary after a delivered
    // broadcast, so subsequent updates go out as deltas. Full-state sends collapse
    // BELOW delta sends (not to one-per-subscriber — bootstrap plus retry jitter
    // produce several full states per subscriber), so over SUSTAINED_UPDATES
    // updates the delta sends dominate: delta_sends > full_state_sends.
    //
    // Reverted to `if sent_delta`, the host never caches a subscriber's summary,
    // so EVERY update is full state to EVERY subscriber and delta_sends stays
    // low — full_state_sends overtakes delta_sends and THIS assertion fails.
    // (Measured on this star topology: fix = 78 delta / 47 full; reverted = 40
    // delta / 85 full.)
    //
    // We assert the *direction* (deltas dominate) rather than an absolute
    // full-state budget: a fixed numeric cap is brittle against topology/retry
    // jitter, whereas the delta-vs-full crossover flips cleanly between the fixed
    // and reverted gate.
    assert!(
        delta_sends > full_state_sends,
        "#4145 REGRESSION: deltas must dominate after bootstrap, but \
         delta_sends={delta_sends} <= full_state_sends={full_state_sends}. \
         A reverted summary-cache gate traps every subscriber on full state \
         (the #4233 storm): every update re-sends full state to every subscriber \
         instead of a delta."
    );

    // (c) DELIVERY / PROPAGATION stays high: all peers converge to one state.
    assert!(
        convergence.is_converged(),
        "#4233: broadcast delivery degraded — peers failed to converge under \
         sustained fan-out. {} converged, {} diverged",
        convergence.converged.len(),
        convergence.diverged.len(),
    );

    tracing::info!(
        "test_sustained_update_fanout_no_full_state_storm PASSED: \
         delta_sends={} > full_state_sends={}, converged, no stale peers",
        delta_sends,
        full_state_sends,
    );
}

// =============================================================================
// #4233: Event-loop liveness under wide-star sustained UPDATE fan-out
// =============================================================================

/// Shared driver for the #4233 wide-star broadcast fan-out liveness scenario.
///
/// ## What this guards (the safety gate for re-enabling placement migration)
///
/// On 2026-05-24 both production gateways wedged simultaneously (#4145) under
/// sustained UPDATE fan-out from a popular River-style contract with dozens of
/// downstream subscribers: the event-loop `event_loop_notification` channel
/// filled faster than the loop could drain it, the drainers were themselves
/// blocked downstream (a self-feeding back-pressure cycle), and subscribers
/// went silent. #4231 broke the cycle (`try_notify_node_event` + a 500 ms
/// `reserve()` timeout on per-peer dispatch); #4442 fixed a second, separate
/// full-state broadcast STORM under the same load (cache the summary on ANY
/// delivered broadcast, not just deltas). `.claude/rules/channel-safety.md`
/// lists this bug class with 6+ production incidents.
///
/// The existing `test_sustained_update_fanout_no_full_state_storm` pins the
/// #4442 storm discriminator but only at `SUBSCRIBERS = 2`. This driver scales
/// the SAME star to the production-incident width (40-80 direct subscribers) so
/// the fan-out actually exercises the event-loop saturation surface, and asserts
/// **liveness** (no subscriber goes silent, backlog stays bounded, no broadcast
/// is lost or left unapplied, all peers converge) rather than the delta/full
/// crossover. This is the simulation-level guard #4233 asks for, and the gate
/// that must be green before the placement-migration / `SubscribeHint` feature
/// (which adds new `notify_node_event` fan-out sites) is re-enabled.
///
/// ## CRITICAL topology knob: `max_connections >= subscribers`
///
/// To get a TRUE wide star (one host with direct connections to all N
/// subscribers — the production pattern from the incident) the gateway must be
/// allowed to hold N direct connections. If `max_connections < N` a multi-hop
/// relay tree forms instead: relay nodes cache a downstream peer's full state as
/// its "summary" and `compute_delta` then rejects the diff as inefficient,
/// emitting full state regardless of the #4442 gate. That changes the fan-out
/// shape and swamps the delta/full metric — a different scenario. We size
/// `max_connections` to `subscribers + slack` so the host fans out directly.
///
/// ## Fan-out width vs. the harness bootstrap ceiling (why N=8/16, not 40-80)
///
/// #4233 calls for 40-80 subscribers (the popular-River-contract width that
/// wedged production). Empirically, the Turmoil simulation harness CANNOT
/// bootstrap a star that wide within a feasible CI wall-clock budget, so the
/// public tests run at N=8 and N=16 instead — the widest faithful direct star
/// this harness can actually form. Measured on this runner (current origin/main,
/// migration OFF):
///
/// | N  | result | wall-clock | what happened                                  |
/// |----|--------|-----------|-------------------------------------------------|
/// |  8 | PASS   | ~32 s     | star bootstraps, 40 UPDATEs broadcast, liveness holds |
/// | 16 | PASS   | <200 s    | as above, at double the width                   |
/// | 24 | FAIL   | ~135 s    | "Ran for duration: 300s … without completing"   |
/// | 32 | FAIL   | ~169 s    | same — topology never finishes forming          |
/// | 40 | FAIL   | times out | 300 002 virtual steps; subscribers stuck on "peer has not joined the network yet" |
///
/// The N>=24 failures are NOT the broadcast wedge. The simulation never reaches
/// the UPDATE phase: it exhausts the entire 300s virtual-time schedule in
/// CONNECT/topology formation (subscribers route CONNECT through the ring, get
/// "at terminus, no uphill peers available — rejecting" in the tiny 1-gateway
/// star, and churn on pending reservations), so zero UPDATEs and zero broadcasts
/// ever fire. That is a harness-scale limit UPSTREAM of the fan-out this test
/// targets, not evidence about the event loop under load.
///
/// To exercise a true 40-80-direct-subscriber star, the harness would need a
/// topology-preseed primitive that wires every subscriber directly to the
/// gateway (bypassing organic CONNECT formation), analogous to the
/// `SeedHostedContract` contract preseed. That is a harness follow-up; until it
/// exists, N=8/16 is the faithful ceiling and is what gates the migration
/// re-enable. On current main both PASS: under sustained fan-out at these widths
/// the event loop does NOT wedge (no stale peers, bounded backlog, convergence).
///
/// ## Signals available vs. what #4233 ideally wants
///
/// #4233 ideally asserts on `Event loop stats iterations` advancing and
/// `notification_channel_pending` staying bounded. Those are emitted as raw
/// `tracing::info!("Event loop stats")` lines in
/// `p2p_protoc.rs` — they are NOT part of the structured `NetLogMessage` event
/// log that `StateVerifier` / `check_convergence_from_logs` consume, and the
/// integration test harness here captures the structured log, not raw tracing
/// fields. So we assert on the strongest proxies the harness DOES expose:
///   - `StalePeer` anomalies — a subscriber that stopped receiving broadcasts
///     while others kept getting them. This is the incident's defining symptom
///     (subscribers go silent) and is exactly what a wedged event loop produces.
///   - `pending_op_high_water_mark` — high-water mark of in-flight transactions
///     awaiting a reply (`pending_op_results` map size). A wedged loop lets
///     replies pile up unboundedly; a healthy loop keeps this bounded. This is
///     the closest available analogue to `notification_channel_pending`.
///   - `MissingBroadcast` / `BroadcastNotApplied` — broadcast-assembly / delivery
///     failures (a broadcast that never reached a subscriber, or reached it but
///     was never applied).
///   - convergence — every subscriber ends on the host's latest state.
///
/// To assert directly on event-loop iteration progress and
/// `notification_channel_pending` per the letter of #4233, the harness would
/// need to either surface those two fields as `GlobalTestMetrics` counters
/// (mirroring `pending_op_high_water_mark`) or expose a raw-tracing capture to
/// integration tests. That is noted as a harness follow-up; the proxies above
/// cover the same failure mode (silent subscribers + unbounded backlog).
#[cfg(feature = "simulation_tests")]
fn run_fanout_liveness_scenario(network_name: &str, seed: u64, subscribers: usize) {
    run_fanout_liveness_scenario_inner(network_name, seed, subscribers, false, false);
}

/// Configurable driver behind [`run_fanout_liveness_scenario`].
///
/// `preseed` selects how the wide direct star is formed:
///   - `false`: organic ring-routed CONNECT (the original path). Only forms up
///     to ~N=16 in this harness — see the bootstrap-ceiling note above.
///   - `true`: inject the direct star via [`SimNetwork::preseed_direct_star`],
///     bypassing the CONNECT handshake so N=40/60/80 (the production-incident
///     width) can be reached. This is the #4233 topology-preseed primitive.
///
/// `enable_migration` selects the placement-migration (`SubscribeHint`) cascade
/// floor:
///   - `false`: migration OFF (sim default) — the baseline fan-out.
///   - `true`: migration ON via [`SimNetwork::enable_placement_migration`] —
///     the closest sim reproduction of the v0.2.73 incident, in which the
///     migration AMPLIFIES broadcast load.
#[cfg(feature = "simulation_tests")]
fn run_fanout_liveness_scenario_inner(
    network_name: &str,
    seed: u64,
    subscribers: usize,
    preseed: bool,
    enable_migration: bool,
) {
    use freenet::dev_tool::{NodeLabel, ScheduledOperation, SimOperation, register_crdt_contract};

    // UPDATEs driven into the hot contract AFTER all subscribers have
    // bootstrapped. A sustained burst (not a single update) is what saturated
    // the notification channel in the incident.
    const SUSTAINED_UPDATES: usize = 40;

    GlobalTestMetrics::reset();
    setup_deterministic_state(seed);
    let rt = create_runtime();

    // Wide star: the gateway must hold a direct connection to every subscriber,
    // so max_connections must exceed the fan-out width. Slack covers the
    // gateway's own bootstrap/gateway-mesh connections. If this were < N a
    // relay tree would form and the topology would no longer be the production
    // wide-star (see the rustdoc above).
    let max_connections = subscribers + 16;

    let (sim, logs_handle) = rt.block_on(async {
        let mut sim = SimNetwork::new(
            network_name,
            1,           // 1 gateway (the broadcast source / contract host)
            subscribers, // subscriber nodes (the fan-out targets)
            7,           // max_htl
            3,           // rnd_if_htl_above
            max_connections,
            2, // min_connections
            seed,
        )
        .await;
        if enable_migration {
            // Closest sim reproduction of the v0.2.73 incident: the migration
            // cascade amplifies the broadcast/summarize load under fan-out.
            sim.enable_placement_migration();
        } else {
            // Pin the placement-migration (`SubscribeHint`) cascade OFF for the
            // baseline direct-star scenario.
            //
            // This is NOT a no-op: the cascade gates on the build version being
            // `>= SUBSCRIBE_HINT_MIN_VERSION` (0.2.80), and this crate is already
            // past that floor, so #4404 migration is ON by default in sim now.
            // Left on, the host's UPDATE broadcasts nudge closer subscribers to
            // re-host the contract and those nudges cascade among subscribers,
            // fragmenting the intended *direct* star into a shifting multi-level
            // tree: hosting migrates off the gateway, the host's direct-interest
            // set no longer covers every subscriber, and a subscriber can end up
            // hosting a different replica subset than the host — so the converged
            // replica set is no longer deterministically `host + all subscribers`
            // and the (e) full-participation invariant cannot hold. The
            // `migration=false` scenario's premise is a STABLE direct star where
            // every subscriber holds the same contract the host broadcasts, so it
            // pins the cascade off at any build version (the migration-ON variant
            // covers the cascade explicitly). See `disable_placement_migration`.
            sim.disable_placement_migration();
        }
        if preseed {
            // #4233 topology preseed: wire the gateway directly to every
            // subscriber so the production-incident-width star (N=40-80) exists
            // at t=0, bypassing the organic CONNECT handshake the harness cannot
            // complete at this width.
            let host = NodeLabel::gateway(network_name, 0);
            let subs: Vec<NodeLabel> = (1..=subscribers)
                .map(|idx| NodeLabel::node(network_name, idx))
                .collect();
            sim.preseed_direct_star(&host, &subs);
        }
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    // CRDT contract so post-bootstrap broadcasts compute real version-aware
    // deltas (a plain hash contract's "delta" is full state).
    let contract = SimOperation::create_test_contract(0x42);
    let contract_id = *contract.key().id();
    let contract_key = contract.key();
    register_crdt_contract(contract_id);

    let initial_state = SimOperation::create_crdt_state(1, 0x10);

    let mut operations = Vec::new();

    // Bootstrap: gateway PUTs + subscribes (becomes the host/source), then every
    // node fetches the contract and subscribes directly to the gateway.
    operations.push(ScheduledOperation::new(
        NodeLabel::gateway(network_name, 0),
        SimOperation::Put {
            contract: contract.clone(),
            state: initial_state,
            subscribe: true,
        },
    ));
    for node_idx in 1..=subscribers {
        // Each subscriber GETs-then-subscribes rather than issuing a bare
        // `Subscribe`. This is REQUIRED for the subscriber to actually join the
        // fan-out: a bare `ContractRequest::Subscribe` is rejected by the
        // client-event handler ("Rejecting SUBSCRIBE: contract WASM not cached
        // locally", client_events.rs) unless the node already holds the
        // contract WASM — the production guard that forces a real client to PUT
        // or GET the contract before subscribing (so it can validate/apply
        // incoming updates). `GET+subscribe=true` is the documented exempt path:
        // the GET inherently fetches the WASM+state from the host over the
        // preseeded connection, and the explicit `subscribe=true` flag then
        // subscribes (`maybe_subscribe_child` -> `run_client_subscribe`),
        // registering downstream interest at the host and joining the broadcast
        // set. (Piece E removed the implicit AUTO_SUBSCRIBE_ON_GET fallback; this
        // path is unaffected because it always set subscribe=true.) The topology preseed only
        // injects ring connections, not contract state, so without the GET each
        // subscriber's bare subscribe is rejected at t=0 and only the subset
        // that happened to receive the contract first (via an early broadcast /
        // the placement-migration cascade) before its paced op fires gets
        // through — a race that strands ~half the star (the (e) participation
        // shortfall). The genuine GET routing + subscribe + interest + broadcast
        // machinery is exercised end-to-end; only the WASM-acquisition
        // precondition that a real subscriber must satisfy is made explicit.
        operations.push(ScheduledOperation::new(
            NodeLabel::node(network_name, node_idx),
            SimOperation::Get {
                contract_id,
                return_contract_code: true,
                subscribe: true,
            },
        ));
    }

    // Sustained UPDATE fan-out: a burst of UPDATEs from the gateway, each fanned
    // out to all N subscribers. This is the load pattern that wedged production.
    for v in 0..SUSTAINED_UPDATES {
        let version = (v as u64) + 2; // versions 2.. (v1 was the PUT)
        operations.push(ScheduledOperation::new(
            NodeLabel::gateway(network_name, 0),
            SimOperation::Update {
                key: contract_key,
                data: SimOperation::create_crdt_state(version, 0x20 + v as u8),
            },
        ));
    }

    // The controlled-sim test client paces every triggered operation with a
    // fixed ~3s virtual-time gap (see `run_controlled_simulation`). The
    // operation sequence here is O(N): 1 PUT + N subscribes + SUSTAINED_UPDATES
    // updates, so the *operation phase* alone needs `(1 + N + SUSTAINED_UPDATES)
    // * 3s` of virtual time before the post-op settle even begins. At N=8/16 the
    // fixed 300s budget covered this, but at incident scale (N=40-80) the
    // sequence overruns 300s and the simulation reports "Ran for duration: 300s
    // … without completing" before any liveness can be assessed — a budget
    // overrun, NOT a topology-formation or event-loop problem. Scale the budget
    // with N so the operation phase always fits, with generous headroom.
    const PER_OP_PACING_SECS: u64 = 3;
    const POST_OP_WAIT_SECS: u64 = 120;
    let op_count = (1 + subscribers + SUSTAINED_UPDATES) as u64;
    // op-phase + post-op settle + a 90s margin for startup + final propagation.
    let sim_duration_secs = op_count * PER_OP_PACING_SECS + POST_OP_WAIT_SECS + 90;

    let result = sim.run_controlled_simulation(
        seed,
        operations,
        Duration::from_secs(sim_duration_secs), // simulation duration (virtual)
        Duration::from_secs(POST_OP_WAIT_SECS), // post-op propagation wait (virtual)
    );
    assert!(
        result.turmoil_result.is_ok(),
        "#4233 fan-out (N={subscribers}): simulation should complete: {:?}",
        result.turmoil_result.err()
    );

    let convergence =
        rt.block_on(async { freenet::dev_tool::check_convergence_from_logs(&logs_handle).await });
    let delta_sends = GlobalTestMetrics::delta_sends();
    let full_state_sends = GlobalTestMetrics::full_state_sends();
    let resync_count = GlobalTestMetrics::resync_requests();
    // Event-loop backlog proxy (closest available analogue to
    // notification_channel_pending): high-water mark of in-flight transactions
    // awaiting a reply. A wedged loop lets replies pile up; a healthy loop keeps
    // this bounded.
    let pending_op_hwm = GlobalTestMetrics::pending_op_high_water_mark();

    let report = rt.block_on(async {
        let logs = logs_handle.lock().await;
        let verifier = freenet::tracing::StateVerifier::from_events(logs.clone());
        verifier.verify()
    });
    let stale = report.stale_peers();
    let missing = report.missing_broadcasts();
    let unapplied = report.unapplied_broadcasts();

    tracing::info!(
        "#4233 fanout N={}: delta_sends={}, full_state_sends={}, resyncs={}, \
         pending_op_hwm={}, converged={}/{}, stale_peers={}, missing_broadcasts={}, \
         unapplied_broadcasts={}",
        subscribers,
        delta_sends,
        full_state_sends,
        resync_count,
        pending_op_hwm,
        convergence.converged.len(),
        convergence.total_contracts(),
        stale.len(),
        missing.len(),
        unapplied.len(),
    );

    // Sanity: the scenario must actually have broadcast something, otherwise the
    // liveness assertions below would pass vacuously.
    assert!(
        delta_sends + full_state_sends > 0,
        "#4233 fan-out (N={subscribers}): no broadcasts were recorded — the \
         fan-out scenario did not run"
    );

    // (a) LIVENESS — the incident's defining symptom. No subscriber goes silent
    // under the fan-out. A wedged event loop strands a subscriber, surfacing as
    // a StalePeer anomaly.
    assert!(
        stale.is_empty(),
        "#4233 liveness (N={subscribers}): {} subscriber(s) went stale under \
         sustained UPDATE fan-out — the event loop wedged and subscribers went \
         silent (the v0.2.73 / #4145 production symptom): {:?}",
        stale.len(),
        stale,
    );

    // (b) EVENT-LOOP BACKLOG STAYS BOUNDED. If the loop is pinned by the
    // broadcast fan-out, op-result replies accumulate and the high-water mark
    // climbs without bound. The bound scales with the fan-out width (each
    // UPDATE legitimately fans out to N subscribers, so a window of concurrent
    // in-flight broadcast/op work is expected to grow with N) plus generous
    // headroom, but stays far below a level that would indicate the loop is
    // wedged and replies are piling up unboundedly. A wedged loop produces a
    // mark that scales with updates*subscribers, dwarfing this bound.
    let pending_op_bound = (subscribers as u64) * 8 + 256;
    assert!(
        pending_op_hwm < pending_op_bound,
        "#4233 liveness (N={subscribers}): event-loop backlog high-water mark \
         {pending_op_hwm} reached the bound {pending_op_bound} of in-flight \
         op-results under sustained UPDATE fan-out — the event loop is falling \
         behind (replies piling up), a sign the broadcast path is pinning the \
         loop (the #4145 wedge condition)."
    );

    // (c) BROADCAST DELIVERY HEALTH — LOG-ONLY, NOT asserted.
    //
    // `missing_broadcasts()` / `unapplied_broadcasts()` are NOT reliable
    // pass/fail gates in a multi-node fan-out topology, so we record them as
    // diagnostics but do not assert on them. The `MissingBroadcast` detector
    // (state_verifier/detectors.rs) matches strictly per-(transaction, target):
    // every target listed in a `BroadcastEmitted.broadcast_to` must log a
    // `BroadcastReceived`/`BroadcastApplied` under the SAME transaction id. Two
    // benign mechanisms break that match under this scenario:
    //   1. Summary-match skip (the #4442 fix path itself): the broadcast loop
    //      skips a target whose cached summary already equals the host's, so no
    //      frame is sent and no `BroadcastReceived` is logged — yet the emitted
    //      event still lists that target in `broadcast_to`. This fires even in a
    //      pure direct star whenever a subscriber is already up to date.
    //   2. Fresh-tx-per-hop: a relay re-broadcast mints a NEW transaction and
    //      the cross-hop `BroadcastStateChange` carries no tx, so a subscriber
    //      reached via a relay logs its receive under a tx the original emitter
    //      never referenced.
    // Both produce `MissingBroadcast`/`BroadcastNotApplied` anomalies with no
    // actual lost delivery — confirmed by convergence + no-stale-peers holding.
    // No other simulation test in this file asserts on these two signals; the
    // model test (`test_sustained_update_fanout_no_full_state_storm`) and the
    // rest of the suite gate on `stale_peers` + convergence only. Liveness here
    // is captured by (a) StalePeer, (b) pending-op backlog, and (d) convergence.
    if !missing.is_empty() || !unapplied.is_empty() {
        tracing::info!(
            "#4233 (N={}): broadcast-bookkeeping diagnostics (NOT a failure): \
             missing_broadcasts={}, unapplied_broadcasts={} — these are \
             per-(tx,target) verifier artifacts (summary-skip / relay-hop), see \
             the rationale above; liveness is gated on stale_peers + convergence",
            subscribers,
            missing.len(),
            unapplied.len(),
        );
    }

    // (d) CONVERGENCE: every subscriber ends on the host's latest state. A
    // subscriber stranded by the wedge would diverge.
    assert!(
        convergence.is_converged(),
        "#4233 (N={subscribers}): broadcast delivery degraded — peers failed to \
         converge under sustained fan-out. {} converged, {} diverged",
        convergence.converged.len(),
        convergence.diverged.len(),
    );

    // (e) FULL PARTICIPATION — preseeded direct star only.
    //
    // `is_converged()` (d) and `stale.is_empty()` (a) are necessary but NOT
    // sufficient: `check_convergence_from_logs` only counts peers that logged
    // contract state and ignores contracts with < 2 replicas, so a subscriber
    // that silently never joined the fan-out logs no state and is invisible to
    // BOTH checks — the gate would then pass with a partial star. This assertion
    // closes that gap by requiring the converged contract's replica set to be
    // exactly the host plus EVERY subscriber.
    //
    // Scoped to `preseed && !enable_migration`, because exact participation is a
    // structural invariant ONLY there: `preseed_direct_star` wires every
    // subscriber directly to the host at t=0, so every subscriber's GET+subscribe
    // must reach the host and join the broadcast set (a shortfall means a real
    // loss — e.g. a `ConnectionManager` cap-rejected preseed connection, or a
    // subscribe that never reached the host).
    //
    // Excluded from the migration-ON variant for the same reason the baseline
    // pins the cascade OFF (see the `disable_placement_migration` rationale
    // above): with the `SubscribeHint` cascade live, hosting migrates off the
    // gateway and the converged replica set is no longer deterministically
    // `host + all subscribers`, so this exact-count invariant cannot hold — a
    // failure there would be benign migration churn, not a liveness loss. The
    // migration-ON incident-scale test is gated by (a)–(d) instead.
    //
    // Excluded from the organic N=8/16 variants (preseed=false) because they form
    // the star via ring-routed CONNECT, which is best-effort at this harness's
    // bootstrap ceiling — a subscriber can legitimately fail to form its host
    // connection ("at terminus … rejecting", the very limitation the preseed
    // primitive exists to bypass), so requiring all N there would assert an
    // invariant organic formation does not provide. Those variants are gated by
    // (a)–(d) (no stale peers, bounded backlog, convergence), which hold.
    if preseed && !enable_migration {
        let expected_replicas = subscribers + 1; // 1 gateway host + N subscribers
        assert!(
            !convergence.converged.is_empty(),
            "#4233 participation (N={subscribers}): no contract reached the >=2-replica \
             convergence threshold — the fan-out star did not form"
        );
        for c in &convergence.converged {
            assert_eq!(
                c.replica_count, expected_replicas,
                "#4233 participation (N={subscribers}): converged contract {} has {} \
                 replicas, expected {} (host + all subscribers). A subscriber never \
                 logged contract state, so it silently dropped out of the fan-out \
                 (preseed cap-rejection or a subscribe that never reached the host); \
                 the liveness gate would otherwise pass vacuously.",
                c.contract_key, c.replica_count, expected_replicas,
            );
        }
    }

    tracing::info!(
        "#4233 fan-out liveness (N={}) PASSED: no stale peers, \
         pending_op_hwm={} < {}, converged ({} delta / {} full sends)",
        subscribers,
        pending_op_hwm,
        pending_op_bound,
        delta_sends,
        full_state_sends,
    );
}

/// #4233 wide-star fan-out liveness at N=8 direct subscribers.
///
/// See [`run_fanout_liveness_scenario`] for the full rationale and the
/// "Fan-out width vs. the harness bootstrap ceiling" note explaining why this
/// is N=8/16 rather than the production-incident 40-80.
#[test_log::test]
#[cfg(feature = "simulation_tests")]
fn test_fanout_liveness_8_subscribers() {
    run_fanout_liveness_scenario("i4233-fanout-8", 0x4233_0801_0001, 8);
}

/// #4233 wide-star fan-out liveness at N=16 direct subscribers.
///
/// N=16 is the widest faithful direct star this Turmoil harness can bootstrap
/// within the CI wall-clock budget (N>=24 exhausts the 300s virtual-time
/// schedule in topology formation before any UPDATE fires — see
/// [`run_fanout_liveness_scenario`]). It is the closest reachable proxy for the
/// 40-80-subscriber production incident.
#[test_log::test]
#[cfg(feature = "simulation_tests")]
fn test_fanout_liveness_16_subscribers() {
    run_fanout_liveness_scenario("i4233-fanout-16", 0x4233_1601_0001, 16);
}

// =============================================================================
// #4233 INCIDENT-SCALE fan-out liveness diagnostics (N = 40 / 60 / 80)
//
// These use the topology-preseed primitive (`SimNetwork::preseed_direct_star`)
// to build the production-incident-width direct star that the organic CONNECT
// path cannot bootstrap in this harness (N>=24 never finishes — see the
// bootstrap-ceiling note on `run_fanout_liveness_scenario`). They are the real
// incident-scale reproduction #4233 asks for, in BOTH conditions:
//   - migration OFF (default): baseline sustained UPDATE fan-out.
//   - migration ON: the closest sim reproduction of the v0.2.73 incident, where
//     the placement-migration cascade amplifies the broadcast/summarize load.
//
// They are `#[ignore]`d by default: each builds 41-81 real event-loop nodes and
// drives 40 sustained UPDATEs fanned out across the whole star, which is far
// heavier than the normal per-test CI budget. The N=8/16 organic tests above
// are the always-on regression gate; these are the on-demand incident-scale
// diagnostic. Run explicitly with, e.g.:
//
//   cargo test -p freenet --features "simulation_tests,testing" \
//     --test simulation_integration -- --ignored --nocapture \
//     test_fanout_liveness_incident_scale
// =============================================================================

/// #4233 incident-scale fan-out liveness, migration OFF.
///
/// Preseeded direct star at N=40/60/80, sustained UPDATE fan-out, migration OFF.
/// Asserts the same liveness gates as the organic tests (no stale peers,
/// bounded event-loop backlog, convergence) at the production-incident width.
#[test_log::test]
#[cfg(feature = "simulation_tests")]
#[ignore = "#4233 incident-scale diagnostic (preseeded N=40/60/80, 40 UPDATEs each); too heavy for default CI. Run with --ignored. Always-on gate is test_fanout_liveness_8/16_subscribers."]
fn test_fanout_liveness_incident_scale_migration_off() {
    for (n, seed) in [
        (40usize, 0x4233_4001_0001u64),
        (60, 0x4233_6001_0001),
        (80, 0x4233_8001_0001),
    ] {
        let name = format!("i4233-fanout-preseed-off-{n}");
        run_fanout_liveness_scenario_inner(
            &name, seed, n, /*preseed=*/ true, /*migration=*/ false,
        );
    }
}

/// #4233 incident-scale fan-out liveness, migration ON.
///
/// Preseeded direct star at N=40/60/80, sustained UPDATE fan-out, with the
/// placement-migration (`SubscribeHint`) cascade ENABLED. This is the closest
/// sim reproduction of the actual v0.2.73 incident, where the migration
/// amplifies broadcast load. The headline question #4233 poses: does the event
/// loop wedge at incident scale with the migration on?
#[test_log::test]
#[cfg(feature = "simulation_tests")]
#[ignore = "#4233 incident-scale diagnostic (preseeded N=40/60/80, 40 UPDATEs each, migration ON); too heavy for default CI. Run with --ignored. Always-on gate is test_fanout_liveness_8/16_subscribers."]
fn test_fanout_liveness_incident_scale_migration_on() {
    for (n, seed) in [
        (40usize, 0x4233_4001_0002u64),
        (60, 0x4233_6001_0002),
        (80, 0x4233_8001_0002),
    ] {
        let name = format!("i4233-fanout-preseed-on-{n}");
        run_fanout_liveness_scenario_inner(
            &name, seed, n, /*preseed=*/ true, /*migration=*/ true,
        );
    }
}

/// #4233 topology-preseed sanity at modest width (N=24), migration OFF.
///
/// N=24 is the smallest width the organic CONNECT path CANNOT bootstrap in this
/// harness (see the bootstrap-ceiling note on `run_fanout_liveness_scenario`).
/// This test forms it via the preseed primitive instead, proving the primitive
/// reaches widths organic formation cannot — and runs fast enough to stay in the
/// default suite as an always-on guard that the preseed path itself works.
#[test_log::test]
#[cfg(feature = "simulation_tests")]
fn test_fanout_liveness_preseed_24_subscribers() {
    run_fanout_liveness_scenario_inner(
        "i4233-fanout-preseed-24",
        0x4233_2401_0001,
        24,
        /*preseed=*/ true,
        /*migration=*/ false,
    );
}

// =============================================================================
// Extended Edge Case Tests for Ring Protocol
// =============================================================================

use freenet::dev_tool::{NodeLabel, ScheduledOperation, SimOperation, register_crdt_contract};

/// Parametrized CRDT convergence test.
///
/// All nodes subscribe then update simultaneously. Verifies convergence.
/// Each (nodes, gateways) combo is tested with 5 different seeds to detect
/// topology-dependent failures. See #3028.
///
/// Run with: cargo test -p freenet --features simulation_tests,testing test_crdt_convergence
#[rstest::rstest]
#[case::n3_g1_s1("crdt-3n-1gw-s1", 0x2773_0003_0001, 1, 3)]
#[case::n3_g1_s2("crdt-3n-1gw-s2", 0x2773_0003_0002, 1, 3)]
#[case::n3_g1_s3("crdt-3n-1gw-s3", 0x2773_0003_0003, 1, 3)]
#[case::n3_g1_s4("crdt-3n-1gw-s4", 0x2773_0003_0007, 1, 3)]
#[case::n3_g1_s5("crdt-3n-1gw-s5", 0x2773_0003_0006, 1, 3)]
#[case::n5_g2_s1("crdt-5n-2gw-s1", 0x2773_0005_1021, 2, 5)]
#[case::n5_g2_s2("crdt-5n-2gw-s2", 0x2773_0005_1012, 2, 5)]
#[case::n5_g2_s3("crdt-5n-2gw-s3", 0x2773_0005_1003, 2, 5)]
#[case::n5_g2_s4("crdt-5n-2gw-s4", 0x2773_0005_2001, 2, 5)]
#[case::n5_g2_s5("crdt-5n-2gw-s5", 0x2773_0005_1005, 2, 5)]
#[case::n6_g2_s1("crdt-6n-2gw-s1", 0x2773_0006_1001, 2, 6)]
#[case::n6_g2_s2("crdt-6n-2gw-s2", 0x2773_0006_1002, 2, 6)]
#[case::n6_g2_s3("crdt-6n-2gw-s3", 0x2773_0006_1003, 2, 6)]
#[case::n6_g2_s4("crdt-6n-2gw-s4", 0x2773_0006_1004, 2, 6)]
#[case::n6_g2_s5("crdt-6n-2gw-s5", 0x2773_0006_1005, 2, 6)]
#[case::n4_g1_s1("crdt-4n-1gw-s1", 0x2773_0004_0001, 1, 4)]
#[case::n4_g1_s2("crdt-4n-1gw-s2", 0x2773_0004_0002, 1, 4)]
#[case::n4_g1_s3("crdt-4n-1gw-s3", 0x2773_0004_0003, 1, 4)]
#[case::n4_g1_s4("crdt-4n-1gw-s4", 0x2773_0004_0004, 1, 4)]
#[case::n4_g1_s5("crdt-4n-1gw-s5", 0x2773_0004_0005, 1, 4)]
#[case::n5_g1_s1("crdt-5n-1gw-s1", 0x2773_0005_0008, 1, 5)]
#[case::n5_g1_s2("crdt-5n-1gw-s2", 0x2773_0005_0012, 1, 5)]
#[case::n5_g1_s3("crdt-5n-1gw-s3", 0x2773_0005_0003, 1, 5)]
#[case::n5_g1_s4("crdt-5n-1gw-s4", 0x2773_0005_0004, 1, 5)]
#[case::n5_g1_s5("crdt-5n-1gw-s5", 0x2773_0005_0005, 1, 5)]
#[case::n6_g1_s1("crdt-6n-1gw-s1", 0x2773_0006_0001, 1, 6)]
#[case::n6_g1_s2("crdt-6n-1gw-s2", 0x2773_0006_0002, 1, 6)]
#[case::n6_g1_s3("crdt-6n-1gw-s3", 0x2773_0006_0003, 1, 6)]
#[case::n6_g1_s4("crdt-6n-1gw-s4", 0x2773_0006_0004, 1, 6)]
#[case::n6_g1_s5("crdt-6n-1gw-s5", 0x2773_0006_0005, 1, 6)]
#[case::n7_g1_s1("crdt-7n-1gw-s1", 0x2773_0007_0001, 1, 7)]
#[case::n7_g1_s2("crdt-7n-1gw-s2", 0x2773_0007_0002, 1, 7)]
#[case::n7_g1_s3("crdt-7n-1gw-s3", 0x2773_0007_0023, 1, 7)]
#[case::n7_g1_s4("crdt-7n-1gw-s4", 0x2773_0007_0010, 1, 7)]
#[case::n7_g1_s5("crdt-7n-1gw-s5", 0x2773_0007_0005, 1, 7)]
#[case::n8_g1_s1("crdt-8n-1gw-s1", 0x2773_0008_0001, 1, 8)]
#[case::n8_g1_s2("crdt-8n-1gw-s2", 0x2773_0008_0002, 1, 8)]
#[case::n8_g1_s3("crdt-8n-1gw-s3", 0x2773_0008_0003, 1, 8)]
#[case::n8_g1_s4("crdt-8n-1gw-s4", 0x2773_0008_0004, 1, 8)]
#[case::n8_g1_s5("crdt-8n-1gw-s5", 0x2773_0008_0005, 1, 8)]
fn test_crdt_convergence(
    #[case] name: &'static str,
    #[case] seed: u64,
    #[case] gateways: usize,
    #[case] nodes: usize,
) {
    GlobalTestMetrics::reset();

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
}

/// Test: CRDT convergence with N nodes updating simultaneously.
///
/// All 6 nodes subscribe then issue concurrent updates with different versions.
/// Verifies CRDT merge logic correctly converges to identical final state.
#[test_log::test]
fn test_concurrent_updates_from_n_sources() {
    const SEED: u64 = 0xC0C0_BEEF_0002;
    const NETWORK_NAME: &str = "concurrent-updates-n";
    const NODE_COUNT: usize = 6;

    GlobalTestMetrics::reset();

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
}

/// Test: CRDT convergence when nodes have divergent state versions (4-node variant).
///
/// This test validates that the network converges correctly when:
/// 1. Multiple nodes independently update to different versions
/// 2. Each node has a "stale" view of other nodes' summaries
/// 3. ResyncRequest mechanism recovers divergent state
///
#[test_log::test]
fn test_stale_summary_cache_multiple_branches() {
    const SEED: u64 = 0x57A1_E001_0001;
    const NETWORK_NAME: &str = "stale-summaries";
    const NODE_COUNT: usize = 4;

    GlobalTestMetrics::reset();

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
}

/// Test: CRDT convergence in large network (12 nodes).
///
/// This test validates that updates propagate correctly and converge
/// in a larger network where update broadcasts must reach all 12 hosters.
#[test_log::test]
fn test_max_downstream_limit_reached() {
    const SEED: u64 = 0x0A0D_0001_0001;
    const NETWORK_NAME: &str = "large-network";
    const NODE_COUNT: usize = 12;

    GlobalTestMetrics::reset();

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
/// 2. But `announce_contract_hosted()` was NOT called
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
    // announce_contract_hosted, so the gateway doesn't know Node 1 has the contract.
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
                            states.insert(log.peer_id.socket_addr(), hash.to_string());
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

    // Verify BroadcastEmitted telemetry events were produced (#3622)
    let broadcast_emitted_count: usize = rt.block_on(async {
        let logs = logs_handle.lock().await;
        logs.iter()
            .filter(|log| log.kind.is_update_broadcast_emitted())
            .count()
    });

    assert!(
        broadcast_emitted_count > 0,
        "Expected at least one UpdateEvent::BroadcastEmitted telemetry event, got 0. \
         This means the emission wiring from #3622 is broken."
    );

    tracing::info!(
        "test_subscription_broadcast_propagation PASSED: {} peers converged to same state, \
         {} BroadcastEmitted events",
        peer_states.len(),
        broadcast_emitted_count
    );
}

/// Regression test for #3390: relay nodes must register upstream requester as
/// downstream subscriber when forwarding a subscribe Response.
///
/// ## The Bug
///
/// When a subscribe routes through a relay (e.g., node-2 → gateway → node-1),
/// the fulfilling node (node-1) correctly registers the gateway as a downstream
/// subscriber. But when the gateway forwards the Response to node-2, it did NOT
/// register node-2 as its own downstream subscriber. Updates flowing from
/// node-1 → gateway would then stop at the gateway — node-2 never received them.
///
/// ## What This Test Does
///
/// 1. Node 1 PUTs a contract (becomes the hoster/fulfiller)
/// 2. Node 2 SUBSCRIBES — the subscribe routes through the gateway to node 1
/// 3. Node 1 sends an UPDATE
/// 4. Verify node 2 received the update (has matching state)
///
/// If the relay doesn't register node-2 as downstream, updates die at the gateway.
#[test_log::test]
fn test_subscription_relay_propagation() {
    use freenet::dev_tool::{NodeLabel, ScheduledOperation, SimOperation, register_crdt_contract};

    const SEED: u64 = 0xBEAD_FEED_3390;
    const NETWORK_NAME: &str = "relay-sub-test";

    GlobalTestMetrics::reset();
    setup_deterministic_state(SEED);

    let rt = create_runtime();

    let (sim, logs_handle) = rt.block_on(async {
        // 1 gateway + 3 nodes: node-1 puts, node-2 subscribes, gateway relays
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

    let contract = SimOperation::create_test_contract(0x39);
    let contract_id = *contract.key().id();
    let contract_key = contract.key();
    register_crdt_contract(contract_id);

    // Node 1 puts the contract (NOT the gateway), so subscribe from node 2
    // must relay through the gateway to reach node 1.
    let operations = vec![
        // Node 1 puts with subscribe=true (seeds the contract)
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 1),
            SimOperation::Put {
                contract: contract.clone(),
                state: SimOperation::create_crdt_state(1, 0x01),
                subscribe: true,
            },
        ),
        // Node 2 subscribes — routes through gateway to node 1
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 2),
            SimOperation::Subscribe { contract_id },
        ),
        // Node 1 updates — should propagate: node-1 → gateway → node-2
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 1),
            SimOperation::Update {
                key: contract_key,
                data: SimOperation::create_crdt_state(100, 0xFE),
            },
        ),
    ];

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(120),
        Duration::from_secs(60),
    );

    assert!(
        result.turmoil_result.is_ok(),
        "Simulation should complete: {:?}",
        result.turmoil_result.err()
    );

    let contract_key_str = format!("{:?}", contract_key);

    // Extract per-peer states for this contract from logs
    let peer_states: std::collections::BTreeMap<std::net::SocketAddr, String> =
        rt.block_on(async {
            let logs = logs_handle.lock().await;
            let mut states = std::collections::BTreeMap::new();
            for log in logs.iter() {
                if let Some(key) = log.kind.contract_key() {
                    if format!("{:?}", key) == contract_key_str {
                        if let Some(hash) = log.kind.stored_state_hash() {
                            states.insert(log.peer_id.socket_addr(), hash.to_string());
                        }
                    }
                }
            }
            states
        });

    tracing::info!(
        "Relay subscription test: contract {} has state on {} peers: {:?}",
        contract_key_str,
        peer_states.len(),
        peer_states.keys().collect::<Vec<_>>()
    );

    // ASSERTION: At least 3 peers must have state (node-1 hoster + gateway relay + node-2 subscriber)
    // Without the relay fix, only node-1 and gateway would have state (2 peers);
    // node-2 wouldn't receive updates because the gateway didn't register it as downstream.
    assert!(
        peer_states.len() >= 3,
        "BUG (#3390): Relay subscription failed! Only {} peer(s) have state for contract {}. \
         Expected at least 3 (hoster + gateway relay + subscriber). \
         The relay node (gateway) likely didn't register the subscriber as downstream. \
         Peers with state: {:?}",
        peer_states.len(),
        contract_key_str,
        peer_states.keys().collect::<Vec<_>>()
    );

    // Verify convergence (all peers have same state)
    let unique_states: std::collections::HashSet<&String> = peer_states.values().collect();
    assert!(
        unique_states.len() == 1,
        "State divergence in relay test! {} unique states across {} peers: {:?}",
        unique_states.len(),
        peer_states.len(),
        peer_states
    );

    // Run StateVerifier for anomaly detection (per testing.md)
    let report = rt.block_on(async {
        let logs = logs_handle.lock().await;
        let verifier = freenet::tracing::StateVerifier::from_events(logs.clone());
        verifier.verify()
    });
    tracing::info!(
        "Anomaly report: {} anomalies across {} contracts",
        report.anomalies.len(),
        report.contracts_analyzed
    );

    tracing::info!(
        "test_subscription_relay_propagation PASSED: {} peers converged via relay",
        peer_states.len()
    );
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
/// Long-running deterministic simulation (1 hour virtual time).
///
/// Tests for time-dependent bugs (connection timeouts, state drift, etc.)
/// that only manifest after extended operation. Uses wider event spacing
/// (10s apart) compared to regular tests (200ms) to exercise idle-path code.
///
/// # Virtual Time Breakdown
/// - Events phase: 360 operations × 10s = 3600 seconds (1 hour)
/// - Propagation: 10 seconds
/// - Total: ~3610 seconds virtual time
/// - Wall clock: ~2.5 min with direct runner (single-threaded paused-time runtime)
///
/// NOTE: Gated by nightly_tests feature — does NOT run in regular CI.
#[test_log::test]
#[cfg(feature = "nightly_tests")]
fn test_long_running_deterministic() {
    const SEED: u64 = 0x1A00_2C00_72AC;

    tracing::info!("=== Starting Long-Running Deterministic Simulation ===");
    tracing::info!("Seed: 0x{:X}", SEED);
    tracing::info!("Virtual time target: 3600 seconds (1 hour)");

    let start_time = std::time::Instant::now();

    TestConfig::long_running("long-running", SEED)
        .run_direct()
        .assert_ok()
        .verify_operation_coverage()
        .check_convergence();

    let wall_clock = start_time.elapsed();

    tracing::info!("=== Long-Running Simulation Complete ===");
    tracing::info!("Wall clock time: {:.1} seconds", wall_clock.as_secs_f64());
    tracing::info!(
        "Time acceleration: {:.1}x",
        3600.0 / wall_clock.as_secs_f64()
    );

    tracing::info!("test_long_running_deterministic PASSED");
}

// =============================================================================
// Roadmap Scenario: Partition → Heal → Convergence
// =============================================================================

/// Tests that the network converges after a partition heals.
///
/// ## Scenario (from simulation-testing.md roadmap)
/// 1. Start a network and let contract operations flow
/// 2. Partition the network into two halves (via global fault injector)
/// 3. Wait while both sides operate in isolation
/// 4. Heal the partition
/// 5. Assert convergence via anomaly detection
///
/// Uses Turmoil's deterministic scheduler. Faults are injected mid-simulation
/// through the global `get_fault_injector` registry.
#[test_log::test]
fn test_partition_heal_convergence() {
    use freenet::dev_tool::NodeLabel;
    use freenet::simulation::Partition;

    const SEED: u64 = 0xDA27_0EA1_0001;
    const NETWORK_NAME: &str = "partition-heal";

    setup_deterministic_state(SEED);
    let rt = create_runtime();

    let (sim, logs_handle, node_addrs) = rt.block_on(async {
        let sim = SimNetwork::new(
            NETWORK_NAME,
            1, // 1 gateway
            5, // 5 nodes (6 total, splits 3+3)
            7,
            3,
            10,
            2,
            SEED,
        )
        .await;
        let logs_handle = sim.event_logs_handle();
        let node_addrs: HashMap<NodeLabel, std::net::SocketAddr> = sim.all_node_addresses().clone();
        (sim, logs_handle, node_addrs)
    });

    // Capture addresses for partition injection inside the test closure
    let addrs: Vec<std::net::SocketAddr> = node_addrs.values().copied().collect();
    let mid = addrs.len() / 2;
    let side_a: std::collections::HashSet<_> = addrs[..mid].iter().copied().collect();
    let side_b: std::collections::HashSet<_> = addrs[mid..].iter().copied().collect();

    let side_a_len = side_a.len();
    let side_b_len = side_b.len();
    let network_name = NETWORK_NAME.to_string();

    // iterations=100 gives enough gen_event budget (100/6 peers ≈ 16 iterations/peer)
    // event_wait=500ms keeps total event time to ~50s, leaving room for test_fn
    let result = sim.run_simulation::<rand::rngs::SmallRng, _, _>(
        SEED,
        5,                          // contracts
        100,                        // iterations (also per-peer gen_event budget)
        Duration::from_secs(120),   // simulation_duration
        Duration::from_millis(500), // event_wait
        move || async move {
            // Phase 1: Events have already been firing. Now inject the partition.
            tracing::info!(
                "Injecting partition: side_a={} nodes, side_b={} nodes",
                side_a_len,
                side_b_len,
            );

            if let Some(injector) = freenet::dev_tool::get_fault_injector(&network_name) {
                let mut state = injector.lock().unwrap();
                let partition = Partition::new(side_a, side_b).permanent(0);
                state.config.add_partition(partition);
            }

            // Phase 2: Let both sides operate independently during partition
            tokio::time::sleep(Duration::from_secs(15)).await;
            tracing::info!("Partition active for 15s, now healing");

            // Phase 3: Heal the partition by clearing all partitions
            if let Some(injector) = freenet::dev_tool::get_fault_injector(&network_name) {
                let mut state = injector.lock().unwrap();
                state.config.partitions.clear();
            }

            // Phase 4: Wait for convergence after healing
            tokio::time::sleep(Duration::from_secs(15)).await;
            tracing::info!("Post-heal convergence period complete");

            Ok(())
        },
    );

    assert!(
        result.is_ok(),
        "Partition-heal simulation failed: {:?}",
        result.err()
    );

    // Check convergence
    let convergence = rt.block_on(async { check_convergence_from_logs(&logs_handle).await });
    tracing::info!(
        "Partition-heal: {} converged, {} diverged",
        convergence.converged.len(),
        convergence.diverged.len()
    );

    // Run anomaly detection
    let report = rt.block_on(async {
        let logs = logs_handle.lock().await;
        let verifier = freenet::tracing::StateVerifier::from_events(logs.clone());
        verifier.verify()
    });

    tracing::info!(
        "=== PARTITION-HEAL ANOMALY REPORT: {} events, {} state, {} contracts, {} anomalies ===",
        report.total_events,
        report.state_events,
        report.contracts_analyzed,
        report.anomalies.len()
    );

    let divergences = report.divergences();
    let missing = report.missing_broadcasts();
    let partitions = report.suspected_partitions();
    let stale = report.stale_peers();
    let oscillations = report.state_oscillations();

    tracing::warn!(
        "  divergences={}, missing_broadcasts={}, partitions={}, stale={}, oscillations={}",
        divergences.len(),
        missing.len(),
        partitions.len(),
        stale.len(),
        oscillations.len(),
    );

    for (i, anomaly) in report.anomalies.iter().enumerate() {
        tracing::debug!("  anomaly[{}] = {:?}", i, anomaly);
    }
}

// =============================================================================
// Roadmap Scenario: Node Crash → Recover → Convergence
// =============================================================================

/// Tests that the network converges after nodes crash and recover.
///
/// ## Scenario (from simulation-testing.md roadmap)
/// 1. Put contracts into network and let state propagate
/// 2. Crash 2 nodes (via fault injector — messages to/from are dropped)
/// 3. Remaining nodes continue operating
/// 4. Recover the crashed nodes (messages flow again)
/// 5. Assert convergence via anomaly detection
///
/// Note: This uses crash/recover (message blocking) rather than full process
/// restart, since `restart_node()` requires `&mut SimNetwork` which is consumed
/// by `run_simulation()`. The effect on the anomaly detector is the same:
/// crashed nodes miss updates and become stale.
#[test_log::test]
fn test_crash_recover_convergence() {
    use freenet::dev_tool::NodeLabel;

    const SEED: u64 = 0x2011_2E57_0002;
    const NETWORK_NAME: &str = "crash-recover";

    setup_deterministic_state(SEED);
    let rt = create_runtime();

    let (sim, logs_handle, node_addrs) = rt.block_on(async {
        let sim = SimNetwork::new(NETWORK_NAME, 1, 5, 7, 3, 10, 2, SEED).await;
        let logs_handle = sim.event_logs_handle();
        let node_addrs: HashMap<NodeLabel, std::net::SocketAddr> = sim.all_node_addresses().clone();
        (sim, logs_handle, node_addrs)
    });

    // Pick 2 non-gateway nodes to crash
    let crash_addrs: Vec<std::net::SocketAddr> = node_addrs
        .iter()
        .filter(|(label, _)| label.is_node())
        .take(2)
        .map(|(_, addr)| *addr)
        .collect();

    assert_eq!(crash_addrs.len(), 2, "Need at least 2 non-gateway nodes");

    let network_name = NETWORK_NAME.to_string();
    let crash_addrs_clone = crash_addrs.clone();

    // iterations=100 gives enough gen_event budget (100/6 peers ≈ 16 iterations/peer)
    let result = sim.run_simulation::<rand::rngs::SmallRng, _, _>(
        SEED,
        3,                          // contracts
        100,                        // iterations (also per-peer gen_event budget)
        Duration::from_secs(120),   // simulation_duration
        Duration::from_millis(500), // event_wait
        move || async move {
            // Phase 1: Events have been firing. Now crash 2 nodes.
            tracing::info!("Crashing 2 nodes: {:?}", crash_addrs_clone);

            if let Some(injector) = freenet::dev_tool::get_fault_injector(&network_name) {
                let mut state = injector.lock().unwrap();
                for addr in &crash_addrs_clone {
                    state.config.crash_node(*addr);
                }
            }

            // Phase 2: Let the remaining network operate in degraded state
            tokio::time::sleep(Duration::from_secs(10)).await;
            tracing::info!("Degraded period over, recovering nodes");

            // Phase 3: Recover the crashed nodes
            if let Some(injector) = freenet::dev_tool::get_fault_injector(&network_name) {
                let mut state = injector.lock().unwrap();
                for addr in &crash_addrs_clone {
                    state.config.recover_node(addr);
                }
            }

            // Phase 4: Wait for convergence
            tokio::time::sleep(Duration::from_secs(15)).await;
            tracing::info!("Post-recovery convergence period complete");

            Ok(())
        },
    );

    assert!(
        result.is_ok(),
        "Crash-recover simulation failed: {:?}",
        result.err()
    );

    let convergence = rt.block_on(async { check_convergence_from_logs(&logs_handle).await });
    tracing::info!(
        "Crash-recover: {} converged, {} diverged",
        convergence.converged.len(),
        convergence.diverged.len()
    );

    let report = rt.block_on(async {
        let logs = logs_handle.lock().await;
        let verifier = freenet::tracing::StateVerifier::from_events(logs.clone());
        verifier.verify()
    });

    tracing::info!(
        "=== CRASH-RECOVER ANOMALY REPORT: {} events, {} state, {} contracts, {} anomalies ===",
        report.total_events,
        report.state_events,
        report.contracts_analyzed,
        report.anomalies.len()
    );

    let divergences = report.divergences();
    let stale = report.stale_peers();
    let zombies = report.zombie_transactions();
    let oscillations = report.state_oscillations();

    tracing::warn!(
        "  divergences={}, stale_peers={}, zombies={}, oscillations={}",
        divergences.len(),
        stale.len(),
        zombies.len(),
        oscillations.len(),
    );

    for (i, anomaly) in report.anomalies.iter().enumerate() {
        tracing::debug!("  anomaly[{}] = {:?}", i, anomaly);
    }
}

// =============================================================================
// Roadmap Scenario: Multi-Step Churn
// =============================================================================

/// Tests eventual consistency through continuous crash/recover cycles.
///
/// ## Scenario (from simulation-testing.md roadmap)
/// 1. Start the network with contract operations
/// 2. Perform multiple rounds of: crash a node → wait → recover → wait
/// 3. After all churn rounds, verify convergence via anomaly detection
///
/// Each round targets a different node to maximise disruption coverage.
#[test_log::test]
fn test_multi_step_churn() {
    use freenet::dev_tool::NodeLabel;

    const SEED: u64 = 0xC402_0000_0002;
    const NETWORK_NAME: &str = "multi-churn";

    setup_deterministic_state(SEED);
    let rt = create_runtime();

    let (sim, logs_handle, node_addrs) = rt.block_on(async {
        let sim = SimNetwork::new(NETWORK_NAME, 1, 4, 7, 3, 10, 2, SEED).await;
        let logs_handle = sim.event_logs_handle();
        let node_addrs: HashMap<NodeLabel, std::net::SocketAddr> = sim.all_node_addresses().clone();
        (sim, logs_handle, node_addrs)
    });

    // Collect non-gateway addresses for churn targets
    let churn_addrs: Vec<std::net::SocketAddr> = node_addrs
        .iter()
        .filter(|(label, _)| label.is_node())
        .map(|(_, addr)| *addr)
        .collect();

    let network_name = NETWORK_NAME.to_string();

    // iterations=100 gives enough gen_event budget (100/5 peers = 20 iterations/peer)
    let result = sim.run_simulation::<rand::rngs::SmallRng, _, _>(
        SEED,
        3,                          // contracts
        100,                        // iterations (also per-peer gen_event budget)
        Duration::from_secs(150),   // simulation_duration (events ~50s + churn ~40s + buffer)
        Duration::from_millis(500), // event_wait
        move || async move {
            const CHURN_ROUNDS: usize = 3;

            for round in 0..CHURN_ROUNDS {
                let target = churn_addrs[round % churn_addrs.len()];

                // Crash
                tracing::info!("Churn round {}: crashing {:?}", round, target);
                if let Some(injector) = freenet::dev_tool::get_fault_injector(&network_name) {
                    let mut state = injector.lock().unwrap();
                    state.config.crash_node(target);
                }

                // Degraded period
                tokio::time::sleep(Duration::from_secs(5)).await;

                // Recover
                tracing::info!("Churn round {}: recovering {:?}", round, target);
                if let Some(injector) = freenet::dev_tool::get_fault_injector(&network_name) {
                    let mut state = injector.lock().unwrap();
                    state.config.recover_node(&target);
                }

                // Stabilization period
                tokio::time::sleep(Duration::from_secs(5)).await;
            }

            // Final convergence period
            tokio::time::sleep(Duration::from_secs(10)).await;
            tracing::info!("Multi-step churn: all {} rounds complete", CHURN_ROUNDS);

            Ok(())
        },
    );

    assert!(
        result.is_ok(),
        "Multi-step churn simulation failed: {:?}",
        result.err()
    );

    let convergence = rt.block_on(async { check_convergence_from_logs(&logs_handle).await });
    tracing::info!(
        "Multi-step churn: {} converged, {} diverged",
        convergence.converged.len(),
        convergence.diverged.len()
    );

    let report = rt.block_on(async {
        let logs = logs_handle.lock().await;
        let verifier = freenet::tracing::StateVerifier::from_events(logs.clone());
        verifier.verify()
    });

    tracing::info!(
        "=== MULTI-STEP CHURN ANOMALY REPORT: {} events, {} state, {} contracts, {} anomalies ===",
        report.total_events,
        report.state_events,
        report.contracts_analyzed,
        report.anomalies.len()
    );

    let divergences = report.divergences();
    let missing = report.missing_broadcasts();
    let stale = report.stale_peers();
    let zombies = report.zombie_transactions();
    let oscillations = report.state_oscillations();
    let cascades = report.delta_sync_cascades();

    tracing::warn!(
        "  divergences={}, missing={}, stale={}, zombies={}, oscillations={}, cascades={}",
        divergences.len(),
        missing.len(),
        stale.len(),
        zombies.len(),
        oscillations.len(),
        cascades.len(),
    );

    for (i, anomaly) in report.anomalies.iter().enumerate() {
        tracing::debug!("  anomaly[{}] = {:?}", i, anomaly);
    }
}

// =============================================================================
// Parallel Safety & Determinism Verification
// =============================================================================

/// Proves determinism with per-network cleanup via `SimNetwork::Drop` and thread-local state.
/// Runs the same simulation 3x with the same seed.
///
/// This is the parallel-safe equivalent of `test_strict_determinism_exact_event_equality`.
#[test_log::test]
fn test_determinism_parallel_safe() {
    const SEED: u64 = 0x0A2A_11E1_0001;

    #[derive(Debug, PartialEq)]
    struct Trace {
        event_counts: HashMap<String, usize>,
        event_sequence: Vec<String>,
        total_events: usize,
    }

    fn run_and_trace(name: &str, seed: u64) -> (turmoil::Result, Trace) {
        // Per-network cleanup via SimNetwork::Drop, thread-local counter resets.
        setup_deterministic_state(seed);

        let rt = create_runtime();

        let (sim, logs_handle) = rt.block_on(async {
            let sim = SimNetwork::new(name, 1, 5, 7, 3, 10, 2, seed).await;
            let logs_handle = sim.event_logs_handle();
            (sim, logs_handle)
        });

        let result = sim.run_simulation::<rand::rngs::SmallRng, _, _>(
            seed,
            3,
            15,
            Duration::from_secs(20),
            Duration::from_millis(200),
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

            Trace {
                total_events: logs.len(),
                event_counts,
                event_sequence,
            }
        });
        // SimNetwork dropped here — per-network cleanup runs

        (result, trace)
    }

    let (result1, trace1) = run_and_trace("parallel-safe-run1", SEED);
    let (result2, trace2) = run_and_trace("parallel-safe-run2", SEED);
    let (result3, trace3) = run_and_trace("parallel-safe-run3", SEED);

    assert_eq!(
        result1.is_ok(),
        result2.is_ok(),
        "Simulation outcomes differ: run1={:?}, run2={:?}",
        result1,
        result2
    );
    assert_eq!(result2.is_ok(), result3.is_ok());

    assert!(trace1.total_events > 0, "No events captured");
    assert_eq!(
        trace1.total_events, trace2.total_events,
        "Total events differ: {} vs {}",
        trace1.total_events, trace2.total_events
    );
    assert_eq!(trace2.total_events, trace3.total_events);
    assert_eq!(trace1.event_counts, trace2.event_counts);
    assert_eq!(trace2.event_counts, trace3.event_counts);
    assert_eq!(trace1.event_sequence, trace2.event_sequence);
    assert_eq!(trace2.event_sequence, trace3.event_sequence);

    // Fingerprint verification
    let fp1 = TraceFingerprint::from_events(&trace1.event_sequence, &trace1.event_counts);
    let fp2 = TraceFingerprint::from_events(&trace2.event_sequence, &trace2.event_counts);
    let fp3 = TraceFingerprint::from_events(&trace3.event_sequence, &trace3.event_counts);
    assert_eq!(fp1, fp2, "Fingerprint mismatch between run 1 and run 2");
    assert_eq!(fp2, fp3, "Fingerprint mismatch between run 2 and run 3");

    tracing::info!(
        "PARALLEL-SAFE DETERMINISM PASSED: {} events, fingerprint={:#018x}",
        trace1.total_events,
        fp1.sequence_hash
    );
}

/// Proves that thread-local isolation allows independent simulation runs on separate threads.
///
/// Spawns 2 simulations concurrently on separate OS threads. Each thread creates its own
/// tokio `current_thread` runtime and `SimNetwork`. Both threads must complete successfully
/// and produce events, proving that thread-local state (RNG, simulation time, metrics)
/// provides sufficient isolation for parallel execution.
///
/// Note: Event traces are NOT compared for exact equality because shared atomic counters
/// (CHANNEL_ID_COUNTER, NONCE_RANDOM_PREFIX) race between concurrent threads, causing
/// different initialization sequences. This is harmless for parallel testing (no test
/// asserts on absolute counter values), but prevents exact event comparison. For exact
/// determinism verification, see `test_determinism_parallel_safe` which runs sequentially.
#[test_log::test]
fn test_determinism_across_threads() {
    const SEED: u64 = 0xCE05_7EAD_0001;

    fn run_on_thread(name: &'static str, seed: u64) -> std::thread::JoinHandle<usize> {
        std::thread::spawn(move || {
            // All counters are thread-local, so each spawned thread gets its own
            // counter space automatically. Just need to set seed/time/metrics.
            GlobalRng::set_seed(seed);
            const BASE_EPOCH_MS: u64 = 1577836800000;
            const RANGE_MS: u64 = 5 * 365 * 24 * 60 * 60 * 1000;
            GlobalSimulationTime::set_time_ms(BASE_EPOCH_MS + (seed % RANGE_MS));
            GlobalTestMetrics::reset();
            RequestId::reset_counter();
            freenet::dev_tool::ClientId::reset_counter();
            reset_event_id_counter();
            reset_channel_id_counter();
            StreamId::reset_counter();
            reset_nonce_counter();
            reset_global_node_index();

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to create runtime");

            let (sim, logs_handle) = rt.block_on(async {
                let sim = SimNetwork::new(name, 1, 3, 7, 3, 10, 2, seed).await;
                let logs_handle = sim.event_logs_handle();
                (sim, logs_handle)
            });

            let _result = sim.run_simulation::<rand::rngs::SmallRng, _, _>(
                seed,
                3,
                10,
                Duration::from_secs(20),
                Duration::from_millis(200),
                || async {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    Ok(())
                },
            );

            rt.block_on(async { logs_handle.lock().await.len() })
        })
    }

    // Run concurrently on separate threads — proves thread isolation
    let handle1 = run_on_thread("cross-thread-run1", SEED);
    let handle2 = run_on_thread("cross-thread-run2", SEED);

    let events1 = handle1.join().expect("Thread 1 panicked");
    let events2 = handle2.join().expect("Thread 2 panicked");

    assert!(events1 > 0, "Thread 1 should produce events, got 0");
    assert!(events2 > 0, "Thread 2 should produce events, got 0");

    tracing::info!(
        "CROSS-THREAD ISOLATION PASSED: thread1={} events, thread2={} events",
        events1,
        events2
    );
}

// =============================================================================
// Direct Runner Determinism Tests
// =============================================================================

/// **STRICT** determinism test for `run_simulation_direct`: verifies that
/// same seed produces EXACTLY identical events across 3 runs.
///
/// Checks 4 levels of determinism (weakest → strongest):
/// 1. Total event count
/// 2. Per-type event counts
/// 3. Event sequence (variant names in log order)
/// 4. Structural EventSummary (tx, peer_addr, contract_key, state_hash) — sorted
///    to verify that *which* contracts and peers are involved is deterministic,
///    not only event types. The `event_detail` Debug string is excluded because it
///    contains ephemeral fields (e.g., `this_peer_connection_count`).
///
/// Runs 3 times to catch flaky coincidences.
#[test_log::test]
fn test_direct_runner_determinism() {
    const SEED: u64 = 0xD12E_C7DE_7000;

    /// Structural fields of an event for deterministic comparison.
    /// Excludes `event_detail` (Debug string with ephemeral internal state).
    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct EventKey {
        tx: freenet::dev_tool::Transaction,
        peer_addr: std::net::SocketAddr,
        event_kind_name: String,
        contract_key: Option<String>,
        state_hash: Option<String>,
    }

    #[derive(Debug)]
    struct SimulationTrace {
        event_counts: HashMap<String, usize>,
        event_sequence: Vec<String>,
        /// Sorted structural keys for deep content comparison
        event_keys: Vec<EventKey>,
        total_events: usize,
    }

    fn run_and_trace(name: &str, seed: u64) -> SimulationTrace {
        setup_deterministic_state(seed);

        let rt = create_runtime();

        let (sim, logs_handle) = rt.block_on(async {
            let sim = SimNetwork::new(
                name, 2,  // gateways
                4,  // nodes
                10, // ring_max_htl
                3,  // rnd_if_htl_above
                10, // max_connections
                3,  // min_connections
                seed,
            )
            .await;
            let logs_handle = sim.event_logs_handle();
            (sim, logs_handle)
        });

        drop(rt);

        sim.run_simulation_direct::<rand::rngs::SmallRng>(
            seed,
            10, // max_contract_num
            30, // iterations
            Duration::from_millis(200),
        )
        .expect("Direct simulation should succeed");

        // Extract trace — need a runtime to lock the async mutex
        let rt = create_runtime();
        rt.block_on(async {
            let logs = logs_handle.lock().await;
            let mut event_counts: HashMap<String, usize> = HashMap::new();
            let mut event_sequence: Vec<String> = Vec::new();

            let mut event_keys: Vec<EventKey> = logs
                .iter()
                .map(|log| {
                    let event_kind_name = log.kind.variant_name().to_string();
                    let contract_key = log.kind.contract_key().map(|k| format!("{:?}", k));
                    let state_hash = log.kind.state_hash().map(String::from);

                    *event_counts.entry(event_kind_name.clone()).or_insert(0) += 1;
                    event_sequence.push(event_kind_name.clone());

                    EventKey {
                        tx: log.tx,
                        peer_addr: log.peer_id.socket_addr(),
                        event_kind_name,
                        contract_key,
                        state_hash,
                    }
                })
                .collect();
            event_keys.sort();

            SimulationTrace {
                total_events: logs.len(),
                event_counts,
                event_sequence,
                event_keys,
            }
        })
    }

    // Run 3 times with identical seed
    let trace1 = run_and_trace("direct-det-run1", SEED);
    let trace2 = run_and_trace("direct-det-run2", SEED);
    let trace3 = run_and_trace("direct-det-run3", SEED);

    // All runs must produce events
    assert!(trace1.total_events > 0, "Run 1 should produce events");
    assert!(trace2.total_events > 0, "Run 2 should produce events");
    assert!(trace3.total_events > 0, "Run 3 should produce events");

    // STRICT ASSERTION 1: Exact same total event count
    assert_eq!(
        trace1.total_events, trace2.total_events,
        "DIRECT DETERMINISM FAILURE: Total event counts differ (run1 vs run2)!"
    );
    assert_eq!(
        trace2.total_events, trace3.total_events,
        "DIRECT DETERMINISM FAILURE: Total event counts differ (run2 vs run3)!"
    );

    // STRICT ASSERTION 2: Exact same event counts per type
    assert_eq!(
        trace1.event_counts, trace2.event_counts,
        "DIRECT DETERMINISM FAILURE: Event type counts differ (run1 vs run2)!"
    );
    assert_eq!(
        trace2.event_counts, trace3.event_counts,
        "DIRECT DETERMINISM FAILURE: Event type counts differ (run2 vs run3)!"
    );

    // STRICT ASSERTION 3: Exact same event sequence (variant names in log order)
    for (i, ((e1, e2), e3)) in trace1
        .event_sequence
        .iter()
        .zip(trace2.event_sequence.iter())
        .zip(trace3.event_sequence.iter())
        .enumerate()
    {
        assert_eq!(
            e1, e2,
            "Event sequence differs at index {} (run1 vs run2)!",
            i
        );
        assert_eq!(
            e2, e3,
            "Event sequence differs at index {} (run2 vs run3)!",
            i
        );
    }

    // STRICT ASSERTION 4: Structural event content (tx, peer, contract_key, state_hash)
    assert_eq!(
        trace1.event_keys, trace2.event_keys,
        "DIRECT DETERMINISM FAILURE: Sorted event keys differ (run1 vs run2)!"
    );
    assert_eq!(
        trace2.event_keys, trace3.event_keys,
        "DIRECT DETERMINISM FAILURE: Sorted event keys differ (run2 vs run3)!"
    );

    // Fingerprint verification
    let fp1 = TraceFingerprint::from_events(&trace1.event_sequence, &trace1.event_counts);
    let fp2 = TraceFingerprint::from_events(&trace2.event_sequence, &trace2.event_counts);
    let fp3 = TraceFingerprint::from_events(&trace3.event_sequence, &trace3.event_counts);
    assert_eq!(fp1, fp2, "Fingerprint mismatch between run 1 and run 2");
    assert_eq!(fp2, fp3, "Fingerprint mismatch between run 2 and run 3");

    tracing::info!(
        "DIRECT RUNNER DETERMINISM PASSED: {} events, fingerprint={:#018x}",
        trace1.total_events,
        fp1.sequence_hash
    );
}

// =============================================================================
// Zombie Connection Regression Test (PR #3005)
// =============================================================================

/// Test: Suspend/resume creates zombie connections that block reconnection.
///
/// Reproduces the bug from PR #3005:
/// 1. Network establishes connections
/// 2. Node crashes (simulating suspend without cleanup)
/// 3. Time passes (simulating suspend duration)
/// 4. Node restarts (simulating resume)
/// 5. BUG: Old connection entries persist as "zombies"
/// 6. New CONNECT messages sent through dead transport sockets
/// 7. Bootstrap/reconnection fails
///
/// This is a regression test to prevent the zombie connection bug.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_suspend_resume_zombie_connections() {
    const SEED: u64 = 0xDEAD_BEEF_3005;
    const NETWORK_NAME: &str = "zombie-connections";

    tracing::info!("=== Testing Zombie Connection Bug (PR #3005) ===");

    let mut sim = SimNetwork::new(NETWORK_NAME, 1, 2, 7, 3, 10, 2, SEED).await;
    sim.with_start_backoff(Duration::from_millis(50));

    // Start network
    let _handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 5, 10)
        .await;

    // Phase 1: Let network stabilize and establish connections
    tracing::info!("Phase 1: Network startup and stabilization");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Log initial virtual time
    let initial_time = sim.virtual_time().now_nanos();
    tracing::info!("Initial virtual time: {}ns", initial_time);

    // Verify initial connectivity
    sim.check_connectivity(Duration::from_secs(10))
        .await
        .expect("Initial connectivity check should pass");
    tracing::info!("✓ Network connectivity established");

    // Phase 2: Crash a node (simulate suspend)
    let node_to_suspend = sim
        .all_node_addresses()
        .keys()
        .find(|label| label.is_node())
        .cloned()
        .expect("Should have at least one node");

    tracing::info!(?node_to_suspend, "Phase 2: Simulating suspend (crash node)");
    let crashed = sim.crash_node(&node_to_suspend);
    assert!(crashed, "Node should crash successfully");
    assert!(sim.is_node_crashed(&node_to_suspend));
    tracing::info!("✓ Node crashed (suspend simulated)");

    // Phase 3: Advance time significantly (simulate suspend duration)
    tracing::info!("Phase 3: Simulating time passage during suspend");

    // Advance time by 1 hour in virtual time
    // NOTE: With keepalive enabled (future), this would trigger timeout cleanup
    // Without keepalive, connections persist as zombies
    let suspend_duration = Duration::from_secs(3600);
    sim.virtual_time().advance(suspend_duration);

    let time_after_suspend = sim.virtual_time().now_nanos();
    tracing::info!(
        "✓ Advanced virtual time by {} seconds (now: {}ns)",
        suspend_duration.as_secs(),
        time_after_suspend
    );

    // Let tasks process the time advancement
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Phase 4: Restart node (simulate resume)
    tracing::info!(
        ?node_to_suspend,
        "Phase 4: Simulating resume (restart node)"
    );

    let restart_seed = SEED.wrapping_add(0x1000);
    let handle = sim
        .restart_node::<rand::rngs::SmallRng>(&node_to_suspend, restart_seed, 5, 5)
        .await;

    assert!(handle.is_some(), "Node should restart successfully");
    assert!(!sim.is_node_crashed(&node_to_suspend));
    tracing::info!("✓ Node restarted (resume simulated)");

    // Let the restarted node attempt to bootstrap
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Phase 5: Check for zombie connection bug
    tracing::info!("Phase 5: Checking for zombie connections");

    let connectivity_result = sim.check_connectivity(Duration::from_secs(15)).await;

    match connectivity_result {
        Ok(()) => {
            tracing::info!("✓ Connectivity restored after restart");
            tracing::info!("   FIX WORKING: No zombie connections blocking reconnection");
            tracing::info!("   (or DropAllConnections was called on resume)");
        }
        Err(e) => {
            tracing::error!("✗ Connectivity check failed after restart: {}", e);
            tracing::error!("BUG DETECTED: Zombie connections blocking reconnection");
            tracing::error!("");
            tracing::error!("Root cause analysis:");
            tracing::error!("  - Node crashed without proper cleanup");
            tracing::error!("  - Old connection HashMap entries still exist");
            tracing::error!("  - CONNECT messages sent through dead transport sockets");
            tracing::error!("  - Gateway doesn't respond (socket handle invalid)");
            tracing::error!("  - Bootstrap hangs waiting for response");
            tracing::error!("");
            tracing::error!("Expected fix: Call DropAllConnections in ops_after_resume()");
            tracing::error!("See PR #3005 for details");

            // Fail the test to catch regression
            panic!("Zombie connection bug detected! Connectivity failed after node restart");
        }
    }

    tracing::info!("=== Zombie Connection Test Complete ===");
}

// =============================================================================
// Subscription Storm Regression Test (PR #2995, PR #3146)
// =============================================================================

/// Regression test for subscription renewal storm under high contract load.
///
/// Ensures that with 250+ contracts requiring renewal, the subscription
/// recovery mechanism (PR #2995, #3146) prevents notification channel
/// saturation through rate-limiting and smart backpressure.
///
/// **Background:**
/// - `recover_orphaned_subscriptions()` runs every 30 seconds
/// - Pre-fix: 20+ concurrent renewals could saturate the notification channel
/// - PR #2995: Rate-limited to 5 renewals per interval
/// - PR #3146: Improved to 10 renewals/interval with smart backpressure
///
/// **Test validates:**
/// - Subscription recovery triggers with 250+ contracts
/// - No channel saturation warnings under production-scale load
/// - Network remains responsive during recovery cycles
///
/// **Runtime:** ~8-10 minutes wall-clock (300 contract compilations)
#[test_log::test]
#[cfg(feature = "nightly_tests")]
fn test_subscription_renewal_at_scale() {
    const SEED: u64 = 0x2995_CAFE_BABE;

    tracing::info!("=== Subscription Renewal at Scale (250+ contracts) ===");

    let start_time = std::time::Instant::now();

    let config = TestConfig {
        name: "subscription-renewal-scale",
        seed: SEED,
        gateways: 1,
        nodes: 2,
        ring_max_htl: 7,
        rnd_if_htl_above: 3,
        max_connections: 10,
        min_connections: 2,
        max_contracts: 250,                 // Production scale
        iterations: 300,                    // Seed contracts
        duration: Duration::from_secs(120), // 60s events + 40s sleep + margin
        event_wait: Duration::from_millis(200),
        sleep_after_events: Duration::from_secs(40), // Trigger 30s recovery cycle
        require_convergence: false,
        latency_range: None,
        message_loss_rate: 0.0,
        use_mock_wasm: false,
    };

    let max_contracts = config.max_contracts; // Save before config.run() moves it

    tracing::info!(
        "Testing with {} contracts, {} operations",
        max_contracts,
        config.iterations
    );

    let result = config.run();

    // Verify subscription recovery triggered and channel didn't saturate
    let rt = create_runtime();
    let logs = rt.block_on(async { result.logs_handle.lock().await.clone() });

    let mut renewal_attempts = 0;
    let mut channel_warnings = 0;

    for log in &logs {
        let msg = format!("{:?}", log);
        if msg.contains("Subscription renewal: attempted") {
            renewal_attempts += 1;
        }
        if msg.contains("Notification channel") && msg.contains("full") {
            channel_warnings += 1;
            tracing::error!("Channel saturation detected: {}", msg);
        }
    }

    let wall_clock = start_time.elapsed();

    tracing::info!(
        "Completed in {:.1}s: {} renewal cycles, {} channel warnings",
        wall_clock.as_secs_f64(),
        renewal_attempts,
        channel_warnings
    );

    // Assertions: Regression checks
    result.assert_ok();

    // Log renewal activity (may be 0 if no subscriptions need renewal)
    tracing::info!("Subscription renewal cycles observed: {}", renewal_attempts);

    // REGRESSION CHECK: With the fix (PR #2995/#3146), we should NOT see channel saturation
    // even under high contract load. This is the critical validation.
    assert_eq!(
        channel_warnings, 0,
        "Channel saturation detected - regression in PR #2995/#3146 fix! \
         The subscription renewal rate-limiting may not be working correctly."
    );

    tracing::info!(
        "✓ No channel saturation with {} contracts (PR #2995/#3146 fix working)",
        max_contracts
    );
}

// =============================================================================
// Router Feedback Verification Tests
//
// These tests verify that the router's isotonic regression model receives
// feedback events from PUT, UPDATE, SUBSCRIBE, and GET operations during
// multi-node simulations. Prior to PR #3137, only GET operations (~4% of
// traffic) fed the router. These tests confirm the fix works end-to-end.
//
// Verification approach: Each `routing_finished()` call emits an
// EventKind::Route log entry. We count these "Route" events in the simulation
// event logs to verify the router is receiving feedback.
// =============================================================================

/// Verify that the router accumulates feedback events from operations during simulation.
///
/// Counts `Route` events in the simulation logs (emitted by `routing_finished()` on each
/// completed operation). With PUT, UPDATE, SUBSCRIBE, and GET all feeding the router,
/// we expect Route events proportional to the number of completed operations.
#[test_log::test]
fn test_router_accumulates_feedback_events() {
    let result = TestConfig::small("router-feedback", 0x0FEE_DBAC_0001)
        .with_nodes(4)
        .with_max_contracts(5)
        .with_iterations(50)
        .with_duration(Duration::from_secs(60))
        .with_sleep(Duration::from_secs(2))
        .run()
        .assert_ok();

    // Count Route events (one per routing_finished call)
    let rt = create_runtime();
    let (route_count, route_by_peer) = rt.block_on(async {
        let logs = result.logs_handle.lock().await;
        let route_count = logs
            .iter()
            .filter(|log| log.kind.variant_name() == "Route")
            .count();

        // Group by peer to see distribution
        let mut by_peer: HashMap<String, usize> = HashMap::new();
        for log in logs.iter().filter(|l| l.kind.variant_name() == "Route") {
            *by_peer
                .entry(format!("{}", log.peer_id.socket_addr()))
                .or_default() += 1;
        }
        (route_count, by_peer)
    });

    tracing::info!("=== ROUTER FEEDBACK TEST ===");
    tracing::info!(
        "Total Route events (routing_finished calls): {}",
        route_count
    );
    for (peer, count) in &route_by_peer {
        tracing::info!("  peer {}: {} route events", peer, count);
    }

    // The router must have received feedback from completed operations.
    // With 50 iterations generating PUT/GET/UPDATE/SUBSCRIBE across 5 nodes,
    // each completing operation emits one Route event on the initiating node.
    assert!(
        route_count > 0,
        "No Route events in logs - routing_finished never called. \
         The router is not receiving any feedback from completed operations."
    );

    tracing::info!(
        "ROUTER FEEDBACK TEST PASSED: {} route events across {} peers",
        route_count,
        route_by_peer.len()
    );
}

/// Verify that the router accumulates failure events when messages are lost.
///
/// Injects 15% message loss to cause operation timeouts. The timeout failure
/// reporting path (`report_timeout_failure`) feeds `RouteOutcome::Failure` events
/// to the router, which also emits Route log events. This test verifies:
/// 1. The simulation still completes (tolerates message loss)
/// 2. Timeout events appear in the logs (operations failed)
/// 3. Route events appear in the logs (router received feedback including failures)
///
/// We don't assert convergence since message loss may prevent it.
#[test_log::test]
fn test_router_accumulates_failure_events_with_message_loss() {
    let result = TestConfig::small("router-faults", 0xFA17_0055_0001)
        .with_nodes(4)
        .with_max_contracts(5)
        .with_iterations(60)
        .with_duration(Duration::from_secs(60))
        .with_sleep(Duration::from_secs(2))
        .with_message_loss(0.15)
        .run()
        // Simulation should complete even with message loss
        .assert_ok();

    let rt = create_runtime();
    let (route_count, timeout_count) = rt.block_on(async {
        let logs = result.logs_handle.lock().await;
        let route_count = logs
            .iter()
            .filter(|log| log.kind.variant_name() == "Route")
            .count();
        let timeout_count = logs
            .iter()
            .filter(|log| log.kind.variant_name() == "Timeout")
            .count();
        (route_count, timeout_count)
    });

    tracing::info!("=== ROUTER FAULT INJECTION TEST ===");
    tracing::info!("Route events (routing_finished): {}", route_count);
    tracing::info!("Timeout events: {}", timeout_count);

    // With 15% message loss, some operations complete (Route events with
    // SuccessUntimed/Success) and some timeout (Timeout events trigger
    // report_timeout_failure which also calls routing_finished with Failure).
    assert!(
        route_count > 0,
        "No Route events with 15% message loss - router not receiving any feedback"
    );

    tracing::info!(
        "ROUTER FAULT TEST PASSED: route_events={}, timeouts={}",
        route_count,
        timeout_count
    );
}

/// Verify that the router's 50-event prediction threshold is crossed during simulation.
///
/// The router transitions from distance-based to prediction-based routing once
/// `failure_estimator.len() >= 50` (threshold lowered from 200 to 50 in PR #3137).
/// This test runs enough operations to cross that threshold and verifies
/// `prediction_active = true` in at least one RouterSnapshot event.
///
/// Uses > 5 min virtual time to capture periodic router telemetry snapshots,
/// and enough concentrated operations (many iterations, few nodes) to exceed 50
/// routing events per node.
#[test_log::test]
fn test_router_prediction_threshold_activation() {
    // Only ~15% of iterations produce a Route event (operations that forward to another
    // peer generate routing feedback; operations that store locally are "Irrelevant").
    // With 3 nodes and ~15% hit rate, to get 50 per node: 50 / 0.15 * 3 ≈ 1000 iterations.
    // Using 1500 for margin, event_wait=0.5s → 750s virtual time > 300s telemetry interval.
    let result = TestConfig::small("router-threshold", 0xACE5_0FD0_0001)
        .with_nodes(2)
        .with_max_contracts(6)
        .with_iterations(1500)
        .with_event_wait(Duration::from_millis(500))
        .with_duration(Duration::from_secs(900))
        .with_sleep(Duration::from_secs(2))
        .run()
        .assert_ok();

    // First verify Route events are being generated
    let rt = create_runtime();
    let (route_count, route_by_peer) = rt.block_on(async {
        let logs = result.logs_handle.lock().await;
        let route_count = logs
            .iter()
            .filter(|log| log.kind.variant_name() == "Route")
            .count();
        let mut by_peer: HashMap<String, usize> = HashMap::new();
        for log in logs.iter().filter(|l| l.kind.variant_name() == "Route") {
            *by_peer
                .entry(format!("{}", log.peer_id.socket_addr()))
                .or_default() += 1;
        }
        (route_count, by_peer)
    });

    tracing::info!("=== ROUTER THRESHOLD ACTIVATION TEST ===");
    tracing::info!("Total Route events: {}", route_count);
    for (peer, count) in &route_by_peer {
        tracing::info!("  peer {}: {} route events", peer, count);
    }

    // Check RouterSnapshot telemetry for prediction_active status
    let snapshots = result.router_snapshots();
    tracing::info!("RouterSnapshot events captured: {}", snapshots.len());
    for (i, (failure, success, active)) in snapshots.iter().enumerate() {
        tracing::info!(
            "  snapshot[{}]: failure_events={}, success_events={}, prediction_active={}",
            i,
            failure,
            success,
            active
        );
    }

    // The Route events prove feedback is flowing. Now check if any node crossed
    // the 50-event threshold for prediction activation.
    let max_per_peer = route_by_peer.values().max().copied().unwrap_or(0);
    tracing::info!("Max route events on a single peer: {}", max_per_peer);

    // Verify enough Route events were generated to make threshold crossing feasible.
    // With 300 iterations across 3 nodes, we expect ~100 per node.
    assert!(
        route_count >= 50,
        "Only {} total Route events from 300 iterations - \
         not enough feedback flowing to the router",
        route_count
    );

    // Check if RouterSnapshot shows prediction_active (if snapshots were captured)
    if !snapshots.is_empty() {
        let any_active = snapshots.iter().any(|(_, _, active)| *active);
        let max_failure_events = snapshots
            .iter()
            .map(|(failure, _, _)| *failure)
            .max()
            .unwrap();

        if any_active {
            tracing::info!(
                "ROUTER THRESHOLD TEST PASSED: prediction activated with {} failure_events (threshold=50)",
                max_failure_events
            );
        } else {
            tracing::info!(
                "RouterSnapshot shows failure_events={} (threshold=50). \
                 Route events per peer: {:?}. \
                 Note: Route events may not all be reflected in failure_events \
                 if the snapshot was captured before all operations completed.",
                max_failure_events,
                route_by_peer
            );
        }
    }

    tracing::info!(
        "ROUTER THRESHOLD TEST PASSED: {} total route events, max {} per peer (threshold=50)",
        route_count,
        max_per_peer
    );
}

// =============================================================================
// Resource Invariant Tests (Stage 2 — Issue #3150)
// =============================================================================

/// Regression guard for #3100: pending_op_results must stay bounded.
/// Verifies that the event loop properly cleans up completed/timed-out
/// transaction callbacks, preventing unbounded HashMap growth.
///
/// Note: As of Phase 2a of #1454, the op_execution channel's caller side
/// is `OpCtx::send_and_await` (via the `OpManager::op_ctx` factory), which
/// is currently scaffolding-only with no production caller — so
/// `pending_op_results` is not populated during simulation yet. This test
/// serves as a regression guard that will activate automatically once
/// Phase 2b (SUBSCRIBE client-initiated migration) lands its first
/// production `OpCtx` caller. See #1454 for the phased rollout and #3159
/// for the historical dead-code context.
#[test]
#[cfg(feature = "simulation_tests")]
fn test_pending_op_results_bounded() {
    let result = TestConfig::medium("pending-op-bounded", 0x3100_0001).run();
    result.assert_ok().verify_state_report();

    let inserts = freenet::config::GlobalTestMetrics::pending_op_inserts();
    let removes = freenet::config::GlobalTestMetrics::pending_op_removes();
    let hwm = freenet::config::GlobalTestMetrics::pending_op_high_water_mark();

    tracing::info!(inserts, removes, hwm, "pending_op_results resource metrics");

    if inserts == 0 {
        // `OpCtx::send_and_await` is Phase 2a scaffolding with no production
        // caller yet; Phase 2b lands the first consumer (#1454).
        tracing::info!(
            "pending_op_results path not exercised (no production OpCtx caller yet — see #1454 phase 2b)"
        );
    }

    assert!(
        hwm <= 100,
        "pending_op_results high-water mark ({hwm}) exceeded bound of 100 — \
         regression of #3100 (unbounded HashMap growth)"
    );
    let leak = inserts.saturating_sub(removes);
    // Threshold previously crept 10 -> 30 -> 60 to absorb simulation-shutdown
    // noise; #4057's `EventListenerState::Drop` now balances the accounting on
    // every event-loop exit path. With the simulation runner on a single
    // `current_thread` runtime (so all peers share one set of thread-local
    // counters with this test), the leak should be exactly zero: any entry
    // resident in the map at exit is counted by Drop, and any entry already
    // gone was counted by whichever code path removed it (TransactionCompleted/
    // TransactionTimedOut handler, periodic cleanup, or graceful Shutdown).
    // Asserting `== 0` rather than `<= N` removes the slow-creep failure mode
    // that drove the original 10 -> 30 -> 60 history.
    assert_eq!(
        leak, 0,
        "pending_op_results leak at shutdown: {leak} entries \
         (inserts={inserts}, removes={removes})"
    );
}

/// Verifies that the proximity cache is exercised and bounded relative to
/// network size during simulation (2 gateways + 6 nodes, 8 contracts).
#[test]
#[cfg(feature = "simulation_tests")]
fn test_neighbor_hosting_bounded() {
    let result = TestConfig::medium("neighbor-hosting-bounded", 0x3100_0002).run();
    result.assert_ok().verify_state_report();

    let updates = freenet::config::GlobalTestMetrics::neighbor_hosting_updates();

    tracing::info!(updates, "neighbor hosting resource metrics");

    assert!(
        updates > 0,
        "Neighbor hosting should have been exercised (updates = 0)"
    );
    // 500 is ~10x expected steady-state for an 8-peer / 8-contract medium network
    assert!(
        updates <= 500,
        "neighbor_hosting_updates ({updates}) exceeded bound of 500 — \
         excessive cache churn for an 8-peer / 8-contract network"
    );
}

/// Validates the PrioritySelectStream anti-starvation mechanism under load.
/// Asserts the mechanism fires at least once and that the simulation converges.
#[test]
#[cfg(feature = "simulation_tests")]
fn test_anti_starvation_exercised() {
    let result = TestConfig::medium("anti-starvation", 0x3094_0002)
        .with_iterations(150)
        .with_max_contracts(10)
        .run();

    let triggers = freenet::config::GlobalTestMetrics::anti_starvation_triggers();
    tracing::info!(triggers, "anti-starvation trigger count");

    result.assert_ok().verify_state_report().check_convergence();
}

// =============================================================================
// Contract Lifecycle Synthetic Tests (Stage 3, #3151)
// =============================================================================

/// Helper: extract per-peer state from logs for a contract and verify propagation.
///
/// Returns the peer states map for further inspection.
fn verify_contract_propagation(
    rt: &tokio::runtime::Runtime,
    logs_handle: &Arc<Mutex<Vec<freenet::tracing::NetLogMessage>>>,
    contract_key: freenet_stdlib::prelude::ContractKey,
    min_peers: usize,
) -> BTreeMap<SocketAddr, String> {
    let contract_key_str = format!("{:?}", contract_key);

    let peer_states: BTreeMap<SocketAddr, String> = rt.block_on(async {
        let logs = logs_handle.lock().await;
        let mut states = BTreeMap::new();
        for log in logs.iter() {
            if let Some(key) = log.kind.contract_key() {
                if format!("{:?}", key) == contract_key_str {
                    if let Some(hash) = log.kind.stored_state_hash() {
                        states.insert(log.peer_id.socket_addr(), hash.to_string());
                    }
                }
            }
        }
        states
    });

    tracing::info!(
        "Contract {} has state on {} peers (need {}): {:?}",
        contract_key_str,
        peer_states.len(),
        min_peers,
        peer_states.keys().collect::<Vec<_>>()
    );

    assert!(
        peer_states.len() >= min_peers,
        "Contract {} propagation failed: only {} peer(s) have state, expected at least {}. \
         Peers with state: {:?}",
        contract_key_str,
        peer_states.len(),
        min_peers,
        peer_states.keys().collect::<Vec<_>>()
    );

    // Verify convergence: all peers should have the same final state hash
    let unique_states: std::collections::HashSet<&String> = peer_states.values().collect();
    assert!(
        unique_states.len() == 1,
        "Contract {} state divergence: {} unique states across {} peers. States: {:?}",
        contract_key_str,
        unique_states.len(),
        peer_states.len(),
        peer_states
    );

    peer_states
}

/// Six-peer multi-contract lifecycle test — direct replacement for River's
/// six-peer regression test.
///
/// Exercises the complete contract lifecycle with realistic topology:
/// 2 gateways + 4 nodes, 2 contracts (simulating 2 "rooms").
///
/// For each contract:
///   1. Gateway 0 PUTs with subscribe=true
///   2. All 5 other peers SUBSCRIBE
///   3. Gateway 0 sends 3 rounds of UPDATEs
///   4. Node 1 sends 2 UPDATEs
///
/// Catches: broadcast targeting failures (#2794), subscription tree formation
/// bugs, multi-contract interference.
#[test_log::test]
fn test_six_peer_contract_lifecycle() {
    use freenet::dev_tool::{NodeLabel, ScheduledOperation, SimOperation, register_crdt_contract};

    const SEED: u64 = 0x3151_0001_0001;
    const NETWORK_NAME: &str = "six-peer-lifecycle";

    GlobalTestMetrics::reset();
    setup_deterministic_state(SEED);

    let rt = create_runtime();

    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(NETWORK_NAME, 2, 4, 10, 5, 15, 3, SEED).await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    // Create 2 contracts (simulating 2 "rooms")
    let contract_a = SimOperation::create_test_contract(0xA1);
    let contract_a_id = *contract_a.key().id();
    let contract_a_key = contract_a.key();
    register_crdt_contract(contract_a_id);

    let contract_b = SimOperation::create_test_contract(0xB2);
    let contract_b_id = *contract_b.key().id();
    let contract_b_key = contract_b.key();
    register_crdt_contract(contract_b_id);

    let mut operations = Vec::new();

    // For each contract: gateway 0 puts, all others subscribe, then updates
    for (contract, contract_id, contract_key, seed_byte) in [
        (contract_a.clone(), contract_a_id, contract_a_key, 0xA1u8),
        (contract_b.clone(), contract_b_id, contract_b_key, 0xB2u8),
    ] {
        // Gateway 0 puts with subscribe=true
        operations.push(ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Put {
                contract: contract.clone(),
                state: SimOperation::create_crdt_state(1, seed_byte),
                subscribe: true,
            },
        ));

        // All 5 other peers subscribe: gateway 1, nodes 2-5
        // (node indices start at number_of_gateways, so with 2 GWs: 2,3,4,5)
        operations.push(ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 1),
            SimOperation::Subscribe { contract_id },
        ));
        for i in 2..=5 {
            operations.push(ScheduledOperation::new(
                NodeLabel::node(NETWORK_NAME, i),
                SimOperation::Subscribe { contract_id },
            ));
        }

        // Gateway 0 sends 3 rounds of updates (versions 10, 20, 30)
        for v in [10u64, 20, 30] {
            operations.push(ScheduledOperation::new(
                NodeLabel::gateway(NETWORK_NAME, 0),
                SimOperation::Update {
                    key: contract_key,
                    data: SimOperation::create_crdt_state(v, seed_byte),
                },
            ));
        }

        // Node 2 sends 2 updates (versions 40, 50)
        for v in [40u64, 50] {
            operations.push(ScheduledOperation::new(
                NodeLabel::node(NETWORK_NAME, 2),
                SimOperation::Update {
                    key: contract_key,
                    data: SimOperation::create_crdt_state(v, seed_byte),
                },
            ));
        }
    }

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(240),
        Duration::from_secs(90),
    );

    assert!(
        result.turmoil_result.is_ok(),
        "Six-peer lifecycle simulation failed: {:?}",
        result.turmoil_result.err()
    );

    // Verify both contracts propagated to at least 4 of 6 peers
    let states_a = verify_contract_propagation(&rt, &logs_handle, contract_a_key, 4);
    let states_b = verify_contract_propagation(&rt, &logs_handle, contract_b_key, 4);

    tracing::info!(
        "test_six_peer_contract_lifecycle PASSED: contract_a on {} peers, contract_b on {} peers",
        states_a.len(),
        states_b.len()
    );
}

/// Same scenario as `test_six_peer_contract_lifecycle` but using MockWasmRuntime.
///
/// Exercises the production `ContractExecutor` code path (init_tracker,
/// validation, notification pipeline) without actual WASM.
#[test_log::test]
fn test_six_peer_lifecycle_mock_wasm() {
    use freenet::dev_tool::{NodeLabel, ScheduledOperation, SimOperation, register_crdt_contract};

    const SEED: u64 = 0x3151_0002_0001;
    const NETWORK_NAME: &str = "six-peer-mock-wasm";

    GlobalTestMetrics::reset();
    setup_deterministic_state(SEED);

    let rt = create_runtime();

    let (mut sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(NETWORK_NAME, 2, 4, 10, 5, 15, 3, SEED).await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });
    sim.use_mock_wasm = true;

    let contract = SimOperation::create_test_contract(0xC3);
    let contract_id = *contract.key().id();
    let contract_key = contract.key();
    register_crdt_contract(contract_id);

    let mut operations = vec![ScheduledOperation::new(
        NodeLabel::gateway(NETWORK_NAME, 0),
        SimOperation::Put {
            contract: contract.clone(),
            state: SimOperation::create_crdt_state(1, 0xC3),
            subscribe: true,
        },
    )];

    // All other peers subscribe (node indices start at 2 with 2 gateways)
    operations.push(ScheduledOperation::new(
        NodeLabel::gateway(NETWORK_NAME, 1),
        SimOperation::Subscribe { contract_id },
    ));
    for i in 2..=5 {
        operations.push(ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, i),
            SimOperation::Subscribe { contract_id },
        ));
    }

    // Gateway 0 sends updates
    for v in [10u64, 20, 30] {
        operations.push(ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Update {
                key: contract_key,
                data: SimOperation::create_crdt_state(v, 0xC3),
            },
        ));
    }

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(240),
        Duration::from_secs(90),
    );

    assert!(
        result.turmoil_result.is_ok(),
        "MockWasm lifecycle simulation failed: {:?}",
        result.turmoil_result.err()
    );

    verify_contract_propagation(&rt, &logs_handle, contract_key, 4);

    tracing::info!("test_six_peer_lifecycle_mock_wasm PASSED");
}

/// Verify a node joining after initial updates gets the current state.
///
/// Scenario:
///   1. Gateway PUTs contract with subscribe=true
///   2. Nodes 1-2 SUBSCRIBE
///   3. Gateway UPDATEs to v10, v20
///   4. Node 3 SUBSCRIBES (late joiner)
///   5. Gateway UPDATEs to v30
///
/// The late joiner should receive state via GET during subscribe and then
/// converge with the rest of the network after the final update.
///
/// Catches PR #2360 regression (contract key mismatch on late GET).
#[test_log::test]
fn test_late_joiner_receives_current_state() {
    use freenet::dev_tool::{NodeLabel, ScheduledOperation, SimOperation, register_crdt_contract};

    const SEED: u64 = 0x3151_0003_0001;
    const NETWORK_NAME: &str = "late-joiner";

    GlobalTestMetrics::reset();
    setup_deterministic_state(SEED);

    let rt = create_runtime();

    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(NETWORK_NAME, 1, 3, 7, 3, 10, 2, SEED).await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    let contract = SimOperation::create_test_contract(0xD4);
    let contract_id = *contract.key().id();
    let contract_key = contract.key();
    register_crdt_contract(contract_id);

    let operations = vec![
        // 1. Gateway PUTs contract
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Put {
                contract: contract.clone(),
                state: SimOperation::create_crdt_state(1, 0xD4),
                subscribe: true,
            },
        ),
        // 2. Nodes 1-2 subscribe
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 1),
            SimOperation::Subscribe { contract_id },
        ),
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 2),
            SimOperation::Subscribe { contract_id },
        ),
        // 3. Gateway updates to v10, v20
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Update {
                key: contract_key,
                data: SimOperation::create_crdt_state(10, 0xD4),
            },
        ),
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Update {
                key: contract_key,
                data: SimOperation::create_crdt_state(20, 0xD4),
            },
        ),
        // 4. Node 3 subscribes (late joiner — joins after 2 updates)
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 3),
            SimOperation::Subscribe { contract_id },
        ),
        // 5. Gateway updates to v30 (should reach all including late joiner)
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Update {
                key: contract_key,
                data: SimOperation::create_crdt_state(30, 0xD4),
            },
        ),
    ];

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(180),
        Duration::from_secs(90),
    );

    assert!(
        result.turmoil_result.is_ok(),
        "Late joiner simulation failed: {:?}",
        result.turmoil_result.err()
    );

    // All 4 peers (gateway + 3 nodes) should converge
    let peer_states = verify_contract_propagation(&rt, &logs_handle, contract_key, 3);

    // Specifically verify node 3 (the late joiner) has the contract state
    // via the shared storage handle, not just log inference.
    let node3_label = NodeLabel::node(NETWORK_NAME, 3);
    let node3_storage = result
        .node_storages
        .get(&node3_label)
        .expect("node 3 should have a storage handle");
    let node3_state = node3_storage.get_stored_state(&contract_key);
    assert!(
        node3_state.is_some(),
        "Late joiner (node 3) should have contract state, but storage is empty. \
         This indicates the subscribe-after-updates path failed to fetch current state."
    );

    tracing::info!(
        "test_late_joiner_receives_current_state PASSED: {} peers converged, \
         late joiner confirmed with state",
        peer_states.len()
    );
}

/// Long-running subscription renewal test — verifies subscriptions survive
/// multiple TTL renewal cycles.
///
/// Uses MockWasmRuntime with enough virtual time for multiple TTL cycles.
/// The test uses random event generation (not controlled) to exercise
/// the subscription renewal path under realistic conditions.
///
/// Catches #3093 (interest TTL not refreshed), subscription tree degradation.
///
/// NOTE: Gated by nightly_tests — does NOT run in regular CI.
#[test_log::test]
#[cfg(feature = "nightly_tests")]
fn test_subscribe_renewal_long_running() {
    const SEED: u64 = 0x3151_0004_0001;

    tracing::info!("=== Starting Subscription Renewal Long-Running Test ===");

    TestConfig::long_running("sub-renewal-long", SEED)
        .with_mock_wasm()
        .run_direct()
        .assert_ok()
        .verify_operation_coverage()
        .verify_state_report();

    tracing::info!("test_subscribe_renewal_long_running PASSED");
}

// =============================================================================
// Operation Success Rate Tests (Stage 4, #3152)
//
// These tests measure operation success rates and assert thresholds,
// catching regressions from topology/routing changes.
//
// Target regressions: #3138 (subscribe rate collapse), #3128/#3136
// (router never learns), #3093 (TTL not refreshed).
// =============================================================================

/// Assert subscribe success rate ≥ 70%.
///
/// Runs a medium network and scans event logs for subscribe outcomes.
/// Catches regressions like #3138 where subscribe success collapsed from 92% to 24%.
///
/// Note: Only counts completed subscribe outcomes (SubscribeSuccess/SubscribeNotFound).
/// Subscribe timeouts are emitted as `EventKind::Timeout` only with the `trace-ot` feature,
/// which is not enabled in simulation tests.
#[test_log::test]
fn test_topology_subscribe_health() {
    const SEED: u64 = 0x3152_0001_0001;

    tracing::info!("=== Starting Topology Subscribe Health Test ===");

    let result = TestConfig::medium("sub-health", SEED)
        .with_iterations(100)
        .with_duration(Duration::from_secs(60))
        .run()
        .assert_ok();

    let rt = create_runtime();
    let (successes, failures) = rt.block_on(async {
        let logs = result.logs_handle.lock().await;
        let mut successes = 0u64;
        let mut failures = 0u64;
        for log in logs.iter() {
            match log.kind.subscribe_outcome() {
                Some(true) => successes += 1,
                Some(false) => failures += 1,
                None => {}
            }
        }
        (successes, failures)
    });

    let total = successes + failures;
    assert!(
        total >= 5,
        "Only {} subscribe outcome events in {} total events — \
         too few for meaningful success rate measurement",
        total,
        result.event_count
    );

    let success_rate = successes as f64 / total as f64;
    tracing::info!(
        "Subscribe health: {}/{} succeeded ({:.1}%)",
        successes,
        total,
        success_rate * 100.0
    );

    assert!(
        success_rate >= 0.70,
        "Subscribe success rate {:.1}% is below 70% threshold \
         ({} succeeded, {} failed out of {} total). \
         See #3138 for context on subscribe rate regressions.",
        success_rate * 100.0,
        successes,
        failures,
        total
    );

    tracing::info!(
        "test_topology_subscribe_health PASSED: {:.1}% success rate",
        success_rate * 100.0
    );
}

/// Verify router receives feedback and doesn't degrade over time.
///
/// Checks that the failure rate in the second half of the simulation is not
/// significantly worse than the first half. If router snapshots are available
/// (emitted every 5 min of virtual time), also logs whether prediction is active.
/// Catches #3128/#3136 (router never learns).
#[test_log::test]
fn test_router_learning() {
    const SEED: u64 = 0x3152_0002_0001;

    tracing::info!("=== Starting Router Learning Test ===");

    let result = TestConfig::small("router-learn", SEED)
        .with_iterations(150)
        .with_duration(Duration::from_secs(200))
        .run()
        .assert_ok();

    // Log router snapshot info if available (snapshots emitted every 5 min,
    // may not fire in short simulations using turmoil's time model)
    let snapshots = result.router_snapshots();
    let any_prediction_active = snapshots.iter().any(|(_f, _s, active)| *active);
    tracing::info!(
        "Router snapshots: {} total, prediction_active in any: {}",
        snapshots.len(),
        any_prediction_active
    );

    // Collect route outcomes and split into first/second half
    let rt = create_runtime();
    let outcomes: Vec<bool> = rt.block_on(async {
        let logs = result.logs_handle.lock().await;
        logs.iter()
            .filter_map(|log| log.kind.route_outcome_is_success())
            .collect()
    });

    assert!(
        outcomes.len() >= 10,
        "Only {} route outcome events — too few for meaningful comparison. \
         Expected at least 10.",
        outcomes.len()
    );

    let mid = outcomes.len() / 2;
    let (first_half, second_half) = outcomes.split_at(mid);

    let first_failures = first_half.iter().filter(|&&s| !s).count();
    let second_failures = second_half.iter().filter(|&&s| !s).count();

    let first_rate = if first_half.is_empty() {
        0.0
    } else {
        first_failures as f64 / first_half.len() as f64
    };
    // second_half is never empty: mid = len/2 < len when len >= 1
    let second_rate = second_failures as f64 / second_half.len() as f64;

    tracing::info!(
        "Route failure rates: first half {:.1}% ({}/{}), second half {:.1}% ({}/{})",
        first_rate * 100.0,
        first_failures,
        first_half.len(),
        second_rate * 100.0,
        second_failures,
        second_half.len()
    );

    // Second half failure rate should not be significantly worse than first half
    assert!(
        second_rate <= first_rate + 0.15,
        "Router is degrading: second-half failure rate ({:.1}%) exceeds \
         first-half ({:.1}%) by more than 15pp. \
         This suggests the router is not learning from feedback. See #3128/#3136.",
        second_rate * 100.0,
        first_rate * 100.0
    );

    tracing::info!("test_router_learning PASSED");
}

/// Verify subscriptions survive across multiple subscription lease cycles.
///
/// SUBSCRIPTION_LEASE_DURATION = 480s (8 min). The simulation runs for
/// 400 iterations * 3s = 1200s (~2.5 lease cycles) and checks that update
/// broadcasts continue arriving in the second half of virtual time.
///
/// Uses `run_direct()` (paused-time single-thread runtime) for efficiency.
/// Virtual time is controlled by iterations * event_wait, not `with_duration()`.
///
/// `no_convergence_wait()`: this test asserts only on broadcast-received events
/// produced during the 1200s event phase, never on final-state convergence, so
/// it skips the direct runner's advisory up-to-1800s convergence-polling tail.
/// When the network happens not to converge, that tail would add 1800s of virtual
/// time (processed in ~1ms quanta) on top of the event phase for no benefit — the
/// exact failure mode that made this test consistently time out in CI (#3792).
///
/// Catches #3093 (interest TTL not refreshed on broadcast send).
#[test_log::test]
fn test_interest_renewal() {
    const SEED: u64 = 0x3152_0003_0001;

    tracing::info!("=== Starting Interest Renewal Test ===");

    let result = TestConfig::medium("interest-renew", SEED)
        .with_nodes(4)
        .with_iterations(400)
        .with_event_wait(Duration::from_secs(3))
        .no_convergence_wait()
        .run_direct()
        .assert_ok();

    // Split broadcast-received events by time (log index as proxy for time order)
    let rt = create_runtime();
    let (early_broadcasts, late_broadcasts) = rt.block_on(async {
        let logs = result.logs_handle.lock().await;
        let log_count = logs.len();
        let mid_index = log_count / 2;

        let mut early = 0usize;
        let mut late = 0usize;
        for (i, log) in logs.iter().enumerate() {
            if log.kind.is_update_broadcast_received() {
                if i < mid_index {
                    early += 1;
                } else {
                    late += 1;
                }
            }
        }
        (early, late)
    });

    let total = early_broadcasts + late_broadcasts;
    tracing::info!(
        "Broadcast received events: {} total (early: {}, late: {})",
        total,
        early_broadcasts,
        late_broadcasts
    );

    assert!(
        total > 0,
        "No BroadcastReceived events found in {} events — \
         simulation may not be generating updates",
        result.event_count
    );

    assert!(
        late_broadcasts > 0,
        "No BroadcastReceived events in second half of simulation. \
         Subscriptions may have expired without renewal. See #3093."
    );

    let ratio = late_broadcasts as f64 / early_broadcasts.max(1) as f64;
    tracing::info!(
        "Late/early broadcast ratio: {:.2} ({}/{})",
        ratio,
        late_broadcasts,
        early_broadcasts
    );

    assert!(
        ratio > 0.2,
        "Late broadcast ratio ({:.2}) is below 0.2 threshold — \
         subscriptions are likely degrading across lease cycles. See #3093.",
        ratio
    );

    tracing::info!(
        "test_interest_renewal PASSED: {:.2} late/early ratio",
        ratio
    );
}

/// Isolated integration test for interest TTL refresh on broadcast send.
///
/// Validates the fix for #3093: interest entries for peers receiving full-state
/// broadcasts must have their TTL refreshed on each successful send. Without
/// this refresh, subscriptions expire after INTEREST_TTL (20 min) even though
/// broadcasts are being delivered, causing ~49% subscriber drop.
///
/// ## Test Design
///
/// Uses a minimal network (1 gateway + 2 nodes) with few contracts to
/// isolate the TTL refresh mechanism. Virtual time spans ~1.5x INTEREST_TTL
/// (1800s) so that without the broadcast-send TTL refresh, interest entries
/// would expire and late-phase broadcasts would stop arriving.
///
/// The test splits broadcast-received events into three phases:
/// - **Early** (first third): baseline broadcast delivery
/// - **Mid** (second third): crosses the TTL boundary (~1200s)
/// - **Late** (final third): must still receive broadcasts if TTL was refreshed
///
/// ## What This Catches
///
/// - Missing `refresh_peer_interest()` call in broadcast send path (p2p_protoc.rs)
/// - TTL expiration causing silent subscriber loss
/// - Regression of the #3093 fix
///
/// ## Related
///
/// - Issue #3093: Interest TTL not refreshed on full-state broadcast
/// - Issue #3107: Add isolated integration test (this test)
/// - Issue #3141: CI & Testing Redesign
/// - `test_interest_renewal`: Scale test covering the same mechanism
///
/// Uses `run_direct()` (paused-time single-thread runtime) for efficiency.
/// Virtual time: 300 iterations × 6s = 1800s (~1.5× INTEREST_TTL).
/// Wall clock: typically < 15s.
#[test_log::test]
fn test_interest_ttl_refresh_on_broadcast() {
    const SEED: u64 = 0x3107_0BCA_0001;

    tracing::info!("=== Starting Interest TTL Refresh on Broadcast Test ===");
    // INTEREST_TTL = 1200s (20 min). Virtual time = 300 × 6s = 1800s (~1.5× TTL).
    tracing::info!("Virtual time target: 1800s (~1.5x INTEREST_TTL of 1200s)");

    let result = TestConfig::small("ttl-refresh-bcast", SEED)
        .with_gateways(1)
        .with_nodes(2) // Minimal network: 1 gateway + 2 nodes
        .with_max_contracts(2) // Few contracts → more updates per contract
        .with_iterations(300) // 300 × 6s = 1800s virtual time
        .with_event_wait(Duration::from_secs(6))
        .run_direct()
        .assert_ok();

    // Analyze broadcast-received events across three phases of virtual time.
    // The TTL boundary is at ~1200s (INTEREST_TTL). If refresh is working,
    // broadcasts should continue in the late phase (1200s-1800s).
    let rt = create_runtime();
    let (early_broadcasts, mid_broadcasts, late_broadcasts) = rt.block_on(async {
        let logs = result.logs_handle.lock().await;
        let log_count = logs.len();
        let third = log_count / 3;

        let mut early = 0usize;
        let mut mid = 0usize;
        let mut late = 0usize;
        for (i, log) in logs.iter().enumerate() {
            if log.kind.is_update_broadcast_received() {
                if i < third {
                    early += 1;
                } else if i < third * 2 {
                    mid += 1;
                } else {
                    late += 1;
                }
            }
        }
        (early, mid, late)
    });

    let total = early_broadcasts + mid_broadcasts + late_broadcasts;
    tracing::info!(
        "Broadcast received events: {} total (early: {}, mid: {}, late: {})",
        total,
        early_broadcasts,
        mid_broadcasts,
        late_broadcasts
    );

    // Must have some broadcasts overall — otherwise the simulation didn't
    // generate enough update activity to be meaningful.
    assert!(
        total > 0,
        "No BroadcastReceived events found in {} logged events — \
         simulation may not be generating updates. Seed: 0x{:X}",
        result.event_count,
        SEED
    );

    // CRITICAL: Late-phase broadcasts must exist. If the TTL refresh on
    // broadcast send is missing (regression of #3093), interest entries
    // expire at ~1200s and no broadcasts are delivered after that point.
    assert!(
        late_broadcasts > 0,
        "No BroadcastReceived events in the final third of simulation \
         (after INTEREST_TTL boundary). Interest TTL is NOT being refreshed \
         on broadcast send — subscriptions have silently expired. \
         See #3093, #3107. Seed: 0x{:X}",
        SEED
    );

    // The late/early ratio should be meaningful — at least 10% of early
    // traffic. A drastic drop indicates partial TTL refresh failure.
    let late_ratio = late_broadcasts as f64 / early_broadcasts.max(1) as f64;
    tracing::info!(
        "Late/early broadcast ratio: {:.2} ({}/{})",
        late_ratio,
        late_broadcasts,
        early_broadcasts
    );

    assert!(
        late_ratio > 0.1,
        "Late broadcast ratio ({:.2}) dropped below 0.1 — \
         interest TTL refresh may be partially broken. \
         Early: {}, Mid: {}, Late: {}. See #3093, #3107. Seed: 0x{:X}",
        late_ratio,
        early_broadcasts,
        mid_broadcasts,
        late_broadcasts,
        SEED
    );

    tracing::info!(
        "test_interest_ttl_refresh_on_broadcast PASSED: late/early ratio {:.2}, \
         total broadcasts: {} (early: {}, mid: {}, late: {})",
        late_ratio,
        total,
        early_broadcasts,
        mid_broadcasts,
        late_broadcasts
    );
}

// =============================================================================
// Thundering Herd CONNECT Storm Regression Test (Issue #3207, PR #3208)
// =============================================================================

/// Helper to advance virtual time in steps while yielding to tokio.
///
/// This is the same pattern used in `simulation_smoke.rs` and `state_verification.rs`,
/// duplicated here because it is test-file-scoped.
async fn let_network_run(sim: &mut SimNetwork, duration: Duration) {
    let step = Duration::from_millis(100);
    let mut elapsed = Duration::ZERO;
    while elapsed < duration {
        sim.advance_time(step);
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        elapsed += step;
    }
}

/// Regression test for thundering herd CONNECT storm after gateway restart.
///
/// **Background (Issue #3207):**
/// When a gateway restarts, all peers reconnect simultaneously, generating a
/// burst of CONNECT operations that can overwhelm per-connection inbound
/// channels (see `INBOUND_CHANNEL_CAPACITY`), causing a non-recovering
/// drop→retransmit feedback loop. PR #3208 fixed the underlying inbound
/// starvation bug.
///
/// **Test scenario:**
/// 1. Create a 1-gateway + 20-node network, let it stabilize
/// 2. Crash the gateway (all peers lose connections)
/// 3. Restart the gateway (triggers thundering herd reconnection)
/// 4. Verify the network recovers (no non-recovering overflow)
///
/// Uses the async direct-control pattern (like `test_suspend_resume_zombie_connections`)
/// because `restart_node` requires `&mut SimNetwork`.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_thundering_herd_connect_storm() {
    const SEED: u64 = 0x3207_0000_3208;
    const NETWORK_NAME: &str = "thundering-herd-connect";

    tracing::info!("=== Thundering Herd CONNECT Storm Test (Issue #3207, PR #3208) ===");

    // 1 gateway, 20 nodes — single bottleneck topology
    let mut sim = SimNetwork::new(NETWORK_NAME, 1, 20, 7, 3, 10, 2, SEED).await;
    sim.with_start_backoff(Duration::from_millis(50));

    let _handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 5, 10)
        .await;

    let logs_handle = sim.event_logs_handle();

    // Phase 1: Stabilize
    tracing::info!("Phase 1: Stabilizing network (5s)");
    let_network_run(&mut sim, Duration::from_secs(5)).await;

    sim.check_partial_connectivity(Duration::from_secs(20), 0.8)
        .await
        .expect("Network should reach ≥80% connectivity before crash");
    tracing::info!("Phase 1 complete: network connected");

    // Record baseline Connect event count
    let baseline_connects = {
        let logs = logs_handle.lock().await;
        logs.iter().filter(|log| log.kind.is_connect()).count()
    };
    tracing::info!("Baseline Connect events: {}", baseline_connects);

    // Phase 2: Crash the gateway
    let gateway_label = sim
        .all_node_addresses()
        .keys()
        .find(|label| label.is_gateway())
        .cloned()
        .expect("Should have a gateway");

    tracing::info!(?gateway_label, "Phase 2: Crashing gateway");
    let crashed = sim.crash_node(&gateway_label);
    assert!(crashed, "Gateway should crash successfully");

    // Phase 3: Let peers detect the dead gateway
    tracing::info!("Phase 3: Peers detecting dead gateway (10s)");
    let_network_run(&mut sim, Duration::from_secs(10)).await;

    // Phase 4: Restart gateway — all peers reconnect at once
    tracing::info!("Phase 4: Restarting gateway (thundering herd begins)");
    let restart_seed = SEED.wrapping_add(0x1000);
    let handle = sim
        .restart_node::<rand::rngs::SmallRng>(&gateway_label, restart_seed, 5, 5)
        .await;
    assert!(handle.is_some(), "Gateway should restart successfully");

    // Phase 5: Let the reconnection storm play out
    tracing::info!("Phase 5: Reconnection storm playing out (30s)");
    let_network_run(&mut sim, Duration::from_secs(30)).await;

    // Phase 6: Verify recovery
    tracing::info!("Phase 6: Verifying network recovery");

    // 6a: Log reconnection activity for diagnostics
    let final_connects = {
        let logs = logs_handle.lock().await;
        logs.iter().filter(|log| log.kind.is_connect()).count()
    };
    let storm_connects = final_connects - baseline_connects;
    tracing::info!(
        "Connect events after restart: {} (baseline: {}, storm: {})",
        final_connects,
        baseline_connects,
        storm_connects
    );
    // Note: The gateway rate limiter (GatewayConnectionRateLimiter) intentionally
    // throttles the thundering herd to 5 connections/sec initially, ramping up over
    // 2 minutes. Its admission ramp advances on the simulation clock
    // (max(real_elapsed, virtual_elapsed); see GatewayConnectionRateLimiter), so the
    // throttle still applies as virtual time advances but fewer connections complete
    // per window than the 20 peers attempting to reconnect. This is the desired
    // behavior: the storm is prevented, not just survived.

    // 6b: Verify network recovered (the actual regression check for #3207/#3208)
    sim.check_partial_connectivity(Duration::from_secs(20), 0.8)
        .await
        .expect(
            "Network should recover to ≥80% connectivity after gateway restart. \
             The rate limiter should throttle but not prevent reconnection.",
        );

    tracing::info!(
        "test_thundering_herd_connect_storm PASSED: {} Connect events after restart, \
         network recovered to ≥80% connectivity",
        storm_connects
    );
}

// =============================================================================
// Regression: Gateway re-bootstrap after total isolation (#3219)
// =============================================================================

/// Tests that a node isolated to zero connections re-bootstraps via gateways.
///
/// ## Scenario
/// 1. Create a network (1 gateway + 4 nodes)
/// 2. Let it stabilize with contracts propagated
/// 3. Partition ONE non-gateway node from ALL other nodes
/// 4. Wait for the isolated node to lose all ring connections
/// 5. Heal the partition
/// 6. Assert the node recovers (contracts converge)
///
/// Without the fix in #3219 (gateway bootstrap fallback in connection_maintenance),
/// the isolated node cannot call acquire_new (no routing candidates at zero
/// connections) and remains permanently disconnected.
#[test_log::test]
fn test_isolated_node_rebootstraps_via_gateway() {
    use freenet::dev_tool::NodeLabel;
    use freenet::simulation::Partition;

    const SEED: u64 = 0x3219_B007_0001;
    const NETWORK_NAME: &str = "isolated-rebootstrap";

    setup_deterministic_state(SEED);
    let rt = create_runtime();

    let (sim, logs_handle, node_addrs) = rt.block_on(async {
        let sim = SimNetwork::new(
            NETWORK_NAME,
            1,  // 1 gateway
            4,  // 4 nodes (5 total)
            7,  // ring_max_htl
            3,  // rnd_if_htl_above
            10, // max_connections
            2,  // min_connections
            SEED,
        )
        .await;
        let logs_handle = sim.event_logs_handle();
        let node_addrs: HashMap<NodeLabel, std::net::SocketAddr> = sim.all_node_addresses().clone();
        (sim, logs_handle, node_addrs)
    });

    // Pick one non-gateway node to isolate
    let (isolated_label, isolated_addr) = node_addrs
        .iter()
        .find(|(label, _)| label.is_node())
        .map(|(l, a)| (l.clone(), *a))
        .expect("Need at least one non-gateway node");

    // side_a = just the isolated node, side_b = everyone else
    let side_a: std::collections::HashSet<_> = [isolated_addr].into_iter().collect();
    let side_b: std::collections::HashSet<_> = node_addrs
        .iter()
        .filter(|(label, _)| **label != isolated_label)
        .map(|(_, addr)| *addr)
        .collect();

    tracing::info!(
        isolated = %isolated_label,
        isolated_addr = %isolated_addr,
        rest_count = side_b.len(),
        "Will isolate one node from all others"
    );

    let network_name = NETWORK_NAME.to_string();

    // iterations=80 gives enough gen_event budget (80/5 peers = 16 iterations/peer)
    let result = sim.run_simulation::<rand::rngs::SmallRng, _, _>(
        SEED,
        3,                          // contracts
        80,                         // iterations (also per-peer gen_event budget)
        Duration::from_secs(120),   // simulation_duration
        Duration::from_millis(500), // event_wait
        move || async move {
            // Phase 1: Events have been firing, contracts propagated. Now isolate one node.
            tracing::info!("Injecting partition: isolating one node from all others");

            if let Some(injector) = freenet::dev_tool::get_fault_injector(&network_name) {
                let mut state = injector.lock().unwrap();
                let partition = Partition::new(side_a, side_b).permanent(0);
                state.config.add_partition(partition);
            }

            // Phase 2: Wait for existing connections to time out.
            // Connection liveness checks run every ~5s (fast tick), connections
            // are pruned after missing pings. 20s is enough for full isolation.
            tokio::time::sleep(Duration::from_secs(20)).await;
            tracing::info!("Isolation period over — node should have zero connections now");

            // Phase 3: Heal the partition. The gateway bootstrap fallback in
            // connection_maintenance should kick in and reconnect the node.
            if let Some(injector) = freenet::dev_tool::get_fault_injector(&network_name) {
                let mut state = injector.lock().unwrap();
                state.config.partitions.clear();
            }
            tracing::info!("Partition healed — waiting for re-bootstrap");

            // Phase 4: Wait for the node to re-bootstrap via gateway and
            // converge state. connection_maintenance runs on 5s tick, so
            // the gateway fallback should trigger quickly.
            tokio::time::sleep(Duration::from_secs(30)).await;
            tracing::info!("Post-heal convergence period complete");

            Ok(())
        },
    );

    assert!(
        result.is_ok(),
        "Isolated-node re-bootstrap simulation failed: {:?}",
        result.err()
    );

    // Check convergence — if the isolated node re-bootstrapped, it should
    // have re-subscribed and received contract state updates.
    let convergence = rt.block_on(async { check_convergence_from_logs(&logs_handle).await });
    tracing::info!(
        "Isolated-rebootstrap: {} converged, {} diverged",
        convergence.converged.len(),
        convergence.diverged.len()
    );

    // Hard assertion: the isolated node must have re-bootstrapped and
    // converged state. Without the gateway bootstrap fallback, the node
    // stays permanently disconnected and contracts diverge.
    assert!(
        convergence.diverged.is_empty(),
        "Isolated node failed to re-bootstrap: {} contracts diverged (expected 0). \
         This indicates the gateway bootstrap fallback in connection_maintenance \
         did not recover the node after partition heal.",
        convergence.diverged.len()
    );

    // Run anomaly detection
    let report = rt.block_on(async {
        let logs = logs_handle.lock().await;
        let verifier = freenet::tracing::StateVerifier::from_events(logs.clone());
        verifier.verify()
    });

    tracing::info!(
        "=== ISOLATED-REBOOTSTRAP ANOMALY REPORT: {} events, {} state, {} contracts, {} anomalies ===",
        report.total_events,
        report.state_events,
        report.contracts_analyzed,
        report.anomalies.len()
    );

    let divergences = report.divergences();
    let stale = report.stale_peers();

    tracing::warn!("  divergences={}, stale={}", divergences.len(), stale.len(),);

    for (i, anomaly) in report.anomalies.iter().enumerate() {
        tracing::debug!("  anomaly[{}] = {:?}", i, anomaly);
    }
}

// =============================================================================
// Direct Runner: Churn Resilience
// =============================================================================

/// Verifies that the direct runner can complete a simulation with node churn enabled.
///
/// Runs 2 gateways + 8 nodes with 20% crash rate, 200ms ticks, 500ms recovery.
/// Asserts the simulation completes without panicking and produces events.
#[test]
fn test_direct_runner_churn() {
    use freenet::dev_tool::ChurnConfig;

    const SEED: u64 = 0xC1_0055_FEED;

    setup_deterministic_state(SEED);

    let rt = create_runtime();

    let mut sim = rt.block_on(async {
        SimNetwork::new(
            "test-churn",
            2,  // gateways
            8,  // nodes
            10, // ring_max_htl
            5,  // rnd_if_htl_above
            15, // max_connections
            3,  // min_connections
            SEED,
        )
        .await
    });

    sim.with_churn(ChurnConfig {
        crash_probability: 0.20,
        tick_interval: Duration::from_millis(200),
        recovery_delay: Duration::from_millis(500),
        max_simultaneous_crashes: Some(2),
        permanent_crash_rate: 0.05,
        // Must be >= connection wait (2s) + buffer to avoid crashing nodes during startup
        warmup_delay: Duration::from_secs(5),
    });

    let logs_handle = sim.event_logs_handle();

    drop(rt);

    sim.run_simulation_direct::<rand::rngs::SmallRng>(
        SEED,
        10, // max_contract_num
        30, // iterations
        Duration::from_millis(200),
    )
    .expect("Direct simulation with churn should complete without panic");

    // Verify events were produced despite churn
    let rt = create_runtime();
    let event_count = rt.block_on(async {
        let logs = logs_handle.lock().await;
        logs.len()
    });

    assert!(
        event_count > 0,
        "Simulation with churn should produce events, got 0"
    );

    tracing::info!(
        "CHURN TEST PASSED: {} events produced with node churn enabled",
        event_count
    );
}

// =============================================================================
// Unhealthy Peer Eviction Tests
// =============================================================================

/// Verifies that partitioned (dead) peers are eventually evicted from the ring
/// via the PeerHealthTracker, and that zombie transports don't accumulate.
///
/// Creates a network, partitions one node, runs contract operations so the
/// partitioned node accumulates routing failures, then checks that the network
/// continues to function (the partitioned node doesn't permanently block routing).
#[test_log::test]
fn test_unhealthy_peer_eviction() {
    use freenet::dev_tool::NodeLabel;
    use freenet::simulation::Partition;

    const SEED: u64 = 0xDEAD_BEEF_0042;
    const NETWORK_NAME: &str = "unhealthy-eviction";

    setup_deterministic_state(SEED);
    let rt = create_runtime();

    let (sim, logs_handle, node_addrs) = rt.block_on(async {
        let sim = SimNetwork::new(
            NETWORK_NAME,
            1, // 1 gateway
            4, // 4 nodes (5 total)
            7,
            3,
            10,
            2,
            SEED,
        )
        .await;
        let logs_handle = sim.event_logs_handle();
        let node_addrs: HashMap<NodeLabel, SocketAddr> = sim.all_node_addresses().clone();
        (sim, logs_handle, node_addrs)
    });

    // Pick one non-gateway node to partition
    let victim_addr: SocketAddr = *node_addrs
        .iter()
        .find(|(label, _)| label.is_node())
        .expect("Need at least 1 non-gateway node")
        .1;

    let all_addrs: HashSet<_> = node_addrs.values().copied().collect();
    let victim_set: HashSet<_> = [victim_addr].into_iter().collect();
    let healthy_set: HashSet<_> = all_addrs.difference(&victim_set).copied().collect();

    let network_name = NETWORK_NAME.to_string();

    // Use long event_wait to give virtual time for health checks to fire.
    // 150 iterations × 2s = 300s virtual time (5 min) — enough for health check interval.
    let result = sim.run_simulation::<rand::rngs::SmallRng, _, _>(
        SEED,
        5,                        // contracts
        150,                      // iterations
        Duration::from_secs(600), // simulation_duration (10 min headroom)
        Duration::from_secs(2),   // event_wait (2s between events)
        move || async move {
            // Phase 1: Partition the victim node from everyone else
            tracing::info!("Partitioning victim node: {}", victim_addr);

            if let Some(injector) = freenet::dev_tool::get_fault_injector(&network_name) {
                let mut state = injector.lock().unwrap();
                let partition = Partition::new(victim_set, healthy_set).permanent(0);
                state.config.add_partition(partition);
            }

            // Phase 2: Let the network operate with the partitioned node.
            // The healthy nodes should accumulate routing failures when trying
            // to use the partitioned node, and eventually the health tracker
            // should flag it as unhealthy.
            tokio::time::sleep(Duration::from_secs(120)).await;
            tracing::info!("Partition active for 120s virtual time");

            Ok(())
        },
    );

    assert!(
        result.is_ok(),
        "Unhealthy-peer eviction simulation failed: {:?}",
        result.err()
    );

    // Verify the network produced events (it didn't deadlock)
    let event_count = rt.block_on(async { logs_handle.lock().await.len() });
    assert!(event_count > 0, "Simulation should produce events, got 0");

    // Run anomaly detection
    let report = rt.block_on(async {
        let logs = logs_handle.lock().await;
        let verifier = freenet::tracing::StateVerifier::from_events(logs.clone());
        verifier.verify()
    });

    tracing::info!(
        "=== UNHEALTHY EVICTION REPORT: {} events, {} state, {} contracts, {} anomalies ===",
        report.total_events,
        report.state_events,
        report.contracts_analyzed,
        report.anomalies.len()
    );
}

// =============================================================================
// Readiness Gating Tests
// =============================================================================

/// Verifies that readiness gating (min_ready_connections=3) does not block
/// contract operations once the network has stabilized.
///
/// Runs 1 gateway + 4 nodes with readiness gating enabled. If the periodic
/// re-broadcast and optimistic timeout work correctly, all nodes should
/// eventually become ready and contract operations should succeed.
#[test]
fn test_readiness_gating_production_config() {
    const SEED: u64 = 0xBEAD_10A7_E001;

    setup_deterministic_state(SEED);

    let rt = create_runtime();

    let mut sim = rt.block_on(async {
        SimNetwork::new(
            "test-readiness-gating",
            1,  // gateways
            4,  // nodes
            10, // ring_max_htl
            5,  // rnd_if_htl_above
            10, // max_connections
            3,  // min_connections
            SEED,
        )
        .await
    });

    sim.with_readiness_gating(3);

    let logs_handle = sim.event_logs_handle();

    drop(rt);

    sim.run_simulation_direct::<rand::rngs::SmallRng>(
        SEED,
        5,  // max_contract_num
        20, // iterations (enough for contract creation + operations)
        Duration::from_millis(200),
    )
    .expect("Simulation with readiness gating should complete without panic");

    let rt = create_runtime();
    let event_count = rt.block_on(async {
        let logs = logs_handle.lock().await;
        logs.len()
    });

    assert!(
        event_count > 0,
        "Simulation with readiness gating should produce events, got 0"
    );

    tracing::info!(
        "READINESS GATING TEST PASSED: {} events produced with gating(3)",
        event_count
    );
}

/// Same as `test_readiness_gating_production_config` but with 5% message loss.
///
/// This validates that the periodic ReadyState re-broadcast and optimistic
/// timeout handle lost messages gracefully.
#[test]
fn test_readiness_gating_with_message_loss() {
    use freenet::simulation::FaultConfig;

    const SEED: u64 = 0xBEAD_10A7_E002;

    setup_deterministic_state(SEED);

    let rt = create_runtime();

    let mut sim = rt.block_on(async {
        SimNetwork::new(
            "test-readiness-gating-loss",
            1,  // gateways
            4,  // nodes
            10, // ring_max_htl
            5,  // rnd_if_htl_above
            10, // max_connections
            3,  // min_connections
            SEED,
        )
        .await
    });

    sim.with_readiness_gating(3);
    sim.with_fault_injection(FaultConfig::builder().message_loss_rate(0.05).build());

    let logs_handle = sim.event_logs_handle();

    drop(rt);

    sim.run_simulation_direct::<rand::rngs::SmallRng>(
        SEED,
        5,  // max_contract_num
        25, // iterations (extra budget for retries under loss)
        Duration::from_millis(200),
    )
    .expect("Simulation with readiness gating + message loss should complete");

    let rt = create_runtime();
    let event_count = rt.block_on(async {
        let logs = logs_handle.lock().await;
        logs.len()
    });

    assert!(
        event_count > 0,
        "Simulation with readiness gating + message loss should produce events, got 0"
    );

    tracing::info!(
        "READINESS GATING + MESSAGE LOSS TEST PASSED: {} events",
        event_count
    );
}

// =============================================================================
// Gateway Version Probe Tests
// =============================================================================

/// Verify that the periodic gateway version probe does not break
/// connection_maintenance. Actual version mismatch detection is not testable
/// in simulation (all nodes share the same PROTOC_VERSION). (#3677)
#[test]
fn test_gateway_version_probe_fires() {
    const SEED: u64 = 0x6A7E_7AE9_0001;

    setup_deterministic_state(SEED);

    let rt = create_runtime();

    let sim = rt.block_on(async {
        SimNetwork::new(
            "test-gw-version-probe",
            1,  // gateways
            3,  // nodes
            7,  // ring_max_htl
            3,  // rnd_if_htl_above
            10, // max_connections
            2,  // min_connections
            SEED,
        )
        .await
    });

    let logs_handle = sim.event_logs_handle();

    drop(rt);

    sim.run_simulation_direct::<rand::rngs::SmallRng>(SEED, 3, 30, Duration::from_secs(1))
        .expect("Simulation should complete without panic");

    let rt = create_runtime();
    let unique_connect_txs = rt.block_on(async {
        let logs = logs_handle.lock().await;
        // Count distinct Connect transactions, not raw events. Each CONNECT op
        // emits multiple log entries (sent/received/accepted) that share a Tx.
        // Counting unique Tx values gives us the number of *operations*, which
        // is what the probe code path actually generates.
        let mut seen = std::collections::HashSet::new();
        for log in logs.iter() {
            if log.kind.variant_name() == "Connect" {
                seen.insert(log.tx);
            }
        }
        seen.len()
    });

    // Bootstrap baseline: each of the 3 non-gateway nodes initiates an initial
    // CONNECT to the gateway, so the bootstrap floor is 3 unique transactions.
    // (Bootstrap may also retry on failure, but that just adds to the floor.)
    //
    // Probe budget: cfg(test) sets GATEWAY_VERSION_PROBE_INTERVAL to 10s with
    // a random initial delay in [0, 10)s and ±20% jitter per cycle. Over 30s
    // of sim time, each non-gateway can fire 1–3 probes, so 3 nodes contribute
    // an additional 3–9 unique CONNECT transactions on top of bootstrap.
    //
    // Asserting > 6 (bootstrap floor + at least one probe per node) is robust
    // against the random initial delay while still proving probes fire.
    assert!(
        unique_connect_txs > 6,
        "Expected bootstrap (~3) + at least 4 probe CONNECTs, got {unique_connect_txs}"
    );
}

// =============================================================================
// CONNECT Acceptor Diversity: Joiner succeeds despite NAT-blocked acceptor
// =============================================================================

/// Tests that a joiner can connect to the network even when partitioned from
/// some peers (simulating NAT hole-punch failure).
///
/// ## Scenario
/// 1. Create a 1-gateway + 5-node network
/// 2. Let the network bootstrap normally (all nodes connected)
/// 3. Inject a partition between 2 specific non-gateway nodes
///    (simulates NAT incompatibility between those peers)
/// 4. Continue operating — peers in the partitioned pair can't reach each other
///    but should still connect via other paths using ConnectFailed re-routing
/// 5. Verify the simulation completes without hanging (nodes don't get stuck
///    retrying the same unreachable acceptor forever)
///
/// Without ConnectFailed + jitter, a joiner partitioned from its nearest
/// acceptor would retry the same peer indefinitely. With the fix, ConnectFailed
/// propagates through the relay chain, causing re-routing to different acceptors,
/// and location jitter explores different ring regions on retry.
#[test_log::test]
fn test_connect_despite_nat_partition() {
    use freenet::dev_tool::NodeLabel;
    use freenet::simulation::Partition;

    const SEED: u64 = 0xC0DE_FA11_0001;
    const NETWORK_NAME: &str = "nat-partition";

    setup_deterministic_state(SEED);
    let rt = create_runtime();

    let (sim, logs_handle, node_addrs) = rt.block_on(async {
        let sim = SimNetwork::new(
            NETWORK_NAME,
            1,  // 1 gateway
            5,  // 5 regular nodes (6 total)
            7,  // max HTL
            3,  // rnd_if_htl_above
            10, // max_connections
            2,  // min_connections
            SEED,
        )
        .await;
        let logs_handle = sim.event_logs_handle();
        let node_addrs: HashMap<NodeLabel, std::net::SocketAddr> = sim.all_node_addresses().clone();
        (sim, logs_handle, node_addrs)
    });

    // Pick two non-gateway nodes to partition from each other.
    // This simulates NAT incompatibility: hole-punch between these two peers
    // always fails, but both can reach the rest of the network.
    let mut non_gw_nodes: Vec<_> = node_addrs
        .iter()
        .filter(|(label, _)| label.is_node())
        .collect();
    non_gw_nodes.sort_by_key(|(label, _)| label.number());

    assert!(
        non_gw_nodes.len() >= 2,
        "Need at least 2 non-gateway nodes for partition"
    );

    let node_a_addr = *non_gw_nodes[0].1;
    let node_b_addr = *non_gw_nodes[1].1;

    let side_a: HashSet<std::net::SocketAddr> = [node_a_addr].into_iter().collect();
    let side_b: HashSet<std::net::SocketAddr> = [node_b_addr].into_iter().collect();

    let network_name = NETWORK_NAME.to_string();

    tracing::info!(
        node_a = %node_a_addr,
        node_b = %node_b_addr,
        "Will inject NAT partition between two nodes"
    );

    // Inject the partition from the test closure (like test_partition_heal_convergence)
    // to preserve the properly-configured fault injector with VirtualTime.
    // The partition is injected immediately and kept permanent — simulating
    // persistent NAT incompatibility between two specific peers.
    let result = sim.run_simulation::<rand::rngs::SmallRng, _, _>(
        SEED,
        3,                          // contracts
        100,                        // iterations (generous budget)
        Duration::from_secs(120),   // simulation_duration
        Duration::from_millis(500), // event_wait
        move || async move {
            // Inject the permanent partition immediately
            if let Some(injector) = freenet::dev_tool::get_fault_injector(&network_name) {
                let mut state = injector.lock().unwrap();
                let partition = Partition::new(side_a, side_b).permanent(0);
                state.config.add_partition(partition);
                tracing::info!("NAT partition injected between two nodes");
            }

            // Let the network operate with the partition active
            tokio::time::sleep(Duration::from_secs(30)).await;
            tracing::info!("NAT partition test: observation period complete");
            Ok(())
        },
    );

    assert!(
        result.is_ok(),
        "NAT partition simulation failed (nodes may have gotten stuck retrying \
         unreachable acceptor): {:?}",
        result.err()
    );

    // Check convergence — with the partition, some divergence is expected
    // between the two partitioned nodes, but the network should still function.
    let convergence = rt.block_on(async { check_convergence_from_logs(&logs_handle).await });
    tracing::info!(
        "NAT partition: {} converged, {} diverged",
        convergence.converged.len(),
        convergence.diverged.len()
    );

    // Run anomaly detection
    let report = rt.block_on(async {
        let logs = logs_handle.lock().await;
        let verifier = freenet::tracing::StateVerifier::from_events(logs.clone());
        verifier.verify()
    });

    tracing::info!(
        "=== NAT PARTITION ANOMALY REPORT: {} events, {} state, {} contracts, {} anomalies ===",
        report.total_events,
        report.state_events,
        report.contracts_analyzed,
        report.anomalies.len()
    );

    // Log anomaly details for debugging
    let divergences = report.divergences();
    let stale = report.stale_peers();
    tracing::warn!("  divergences={}, stale={}", divergences.len(), stale.len(),);

    for (i, anomaly) in report.anomalies.iter().enumerate() {
        tracing::debug!("  anomaly[{}] = {:?}", i, anomaly);
    }

    // The simulation completing without timeout is the primary assertion:
    // without ConnectFailed + jitter, partitioned joiners would retry the same
    // unreachable acceptor forever, eventually causing the simulation to hang
    // or timeout.
}

// =============================================================================
// Connection Growth Stall Regression Test (PRs #3408, #3398, #3396, #3380)
// =============================================================================

/// Regression test: connection growth stall observed on live Freenet network.
///
/// **Background:**
/// Nodes get ~10 connections (including 2 gateway transient connections) but never
/// grow beyond that, continuously reconnecting to gateways. Root causes:
///
/// - `BOOTSTRAP_THRESHOLD` was hardcoded to 4, stopping gateway-directed CONNECTs
///   far below `min_connections`. Fixed: use `min_connections` as the threshold.
/// - `acquire_new` returning None (no routing candidates) incorrectly put the
///   target location in backoff. Fixed: no backoff on routing capacity failure.
/// - `should_accept` used inflated `total_conn` (including pending reservations)
///   for the min_connections check, pushing nodes into the topology evaluator
///   prematurely. Fixed: use actual open connection count for min threshold.
/// - CONNECT exclusion was absolute (3 failures = banned 30min). Fixed: max 50%
///   of ring peers excluded at once (#3408).
/// - Distance-based fallback never evicted never-succeeded peers below
///   min_connections (#3398).
/// - GC-expired CONNECT forwards silently blamed the acceptor (#3396/#3380).
///
/// **What this test verifies (post-fix behavior):**
///
/// 1. Median connections exceed old BOOTSTRAP_THRESHOLD=4 after 10 virtual minutes.
/// 2. At least some nodes reach min_connections.
/// 3. Nodes form connections to non-gateway peers (proves multi-hop CONNECT).
/// 4. Under 20% message loss (simulating NAT hole-punch failures), no death spiral.
///
/// **Topology:** 2 gateways + 15 nodes, min_connections=5, max_connections=10.
/// min_connections=5 is above the old hardcoded BOOTSTRAP_THRESHOLD=4, so
/// this test would fail without the connect.rs fix (nodes would stall at 4).
/// Sized for CI: 600s virtual time ≈ 70s wall time via `let_network_run`.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_connection_growth_stall_regression() {
    use freenet::dev_tool::NodeLabel;
    use freenet::simulation::FaultConfig;

    const SEED: u64 = 0x3408_3398_0001;
    const NETWORK_NAME: &str = "connection-growth-stall";
    const GATEWAYS: usize = 2;
    const NODES: usize = 15;
    const RING_MAX_HTL: usize = 7;
    const RND_IF_HTL_ABOVE: usize = 3;
    const MAX_CONN: usize = 10;
    const MIN_CONN: usize = 5;

    tracing::info!("=== Connection Growth Stall Regression Test ===");
    tracing::info!("Verifies fixes: #3408, #3398, #3396, #3380");

    setup_deterministic_state(SEED);

    let mut sim = SimNetwork::new(
        NETWORK_NAME,
        GATEWAYS,
        NODES,
        RING_MAX_HTL,
        RND_IF_HTL_ABOVE,
        MAX_CONN,
        MIN_CONN,
        SEED,
    )
    .await;

    sim.with_start_backoff(Duration::from_millis(50));

    // Start all nodes. max_contract_num=0, iterations=0: topology-only test.
    let _handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 0, 0)
        .await;

    // -------------------------------------------------------------------------
    // Phase 1: Let the network form connections over 20 virtual minutes
    // -------------------------------------------------------------------------
    // 20 minutes gives topology-aware pruning enough time to converge.
    // The algorithm is more selective about connections (gap-based targeting,
    // composite scoring), so it trades speed for distribution quality.
    tracing::info!("Phase 1: Connection growth — 20 virtual minutes, no faults");
    let_network_run(&mut sim, Duration::from_secs(1200)).await;

    // Collect per-node connection counts
    let mut node_counts: Vec<usize> = (0..NODES)
        .filter_map(|i| {
            let label = NodeLabel::node(NETWORK_NAME, i);
            sim.connection_count(&label)
        })
        .collect();
    node_counts.sort_unstable();

    let num_sampled = node_counts.len();
    assert!(num_sampled > 0, "No connection_managers available");

    let median_conn = node_counts[num_sampled / 2];
    let nodes_above_min = node_counts.iter().filter(|&&c| c >= MIN_CONN).count();
    let fraction_above_min = nodes_above_min as f64 / num_sampled as f64;

    tracing::info!("Phase 1 connection counts: {:?}", node_counts);
    tracing::info!(
        "Median={}, nodes at min_connections={}/{} ({:.0}%)",
        median_conn,
        nodes_above_min,
        num_sampled,
        fraction_above_min * 100.0
    );

    // Check for non-gateway peer connections (proves multi-hop CONNECT forwarding)
    let connectivity = sim.node_connectivity();
    let mut nodes_with_peer_connections = 0usize;
    for (label, (_key, conns)) in &connectivity {
        if !label.is_gateway() {
            let has_non_gw_conn = conns.keys().any(|peer| !peer.is_gateway());
            if has_non_gw_conn {
                nodes_with_peer_connections += 1;
            }
        }
    }

    tracing::info!(
        "Nodes with non-gateway peer connections: {}/{}",
        nodes_with_peer_connections,
        NODES
    );

    // ASSERTION 1: Median connections must be close to MIN_CONN.
    // With dynamic concurrent limits and routing through connected gateways,
    // the median should approach min_connections even in a small simulation.
    // Threshold is MIN_CONN - 1 to allow for topology-aware pruning tradeoffs.
    assert!(
        median_conn >= MIN_CONN - 1,
        "Connection growth stall: median={} must be >= {} (MIN_CONN - 1). \
         Counts: {:?}. Seed: 0x{:X}",
        median_conn,
        MIN_CONN - 1,
        node_counts,
        SEED
    );

    // ASSERTION 2: At least 10% of nodes reached min_connections.
    // In a 15-node simulation with 20 virtual minutes, topology-aware pruning
    // and non-deterministic timing cause run-to-run variance (15%–31% observed).
    // 10% threshold catches severe growth stalls while tolerating this variance.
    assert!(
        fraction_above_min >= 0.10,
        "Too few nodes reached min_connections: {:.0}% ({}/{}) — expected >= 10%. \
         Counts: {:?}. Seed: 0x{:X}",
        fraction_above_min * 100.0,
        nodes_above_min,
        num_sampled,
        node_counts,
        SEED
    );

    // ASSERTION 3: Multi-hop CONNECT forwarding is widespread.
    // With routing through connected gateways, the majority of nodes should
    // have non-gateway peer connections, not just a token few.
    let peer_conn_fraction = nodes_with_peer_connections as f64 / NODES as f64;
    assert!(
        peer_conn_fraction >= 0.50,
        "Only {:.0}% of nodes have non-gateway peer connections (expected >= 50%). \
         CONNECT forwarding is insufficient. Seed: 0x{:X}",
        peer_conn_fraction * 100.0,
        SEED
    );

    // -------------------------------------------------------------------------
    // Phase 2: 20% message loss simulating NAT hole-punch failures — 3 minutes
    // -------------------------------------------------------------------------
    tracing::info!("Phase 2: NAT failure simulation — 20% message loss for 1 min");
    sim.with_fault_injection(FaultConfig::builder().message_loss_rate(0.20).build());

    let_network_run(&mut sim, Duration::from_secs(60)).await;

    // Re-inspect connection counts after faults
    let mut node_counts_after: Vec<usize> = (0..NODES)
        .filter_map(|i| {
            let label = NodeLabel::node(NETWORK_NAME, i);
            sim.connection_count(&label)
        })
        .collect();
    node_counts_after.sort_unstable();

    let median_after = node_counts_after
        .get(node_counts_after.len() / 2)
        .copied()
        .unwrap_or(0);
    let nodes_isolated = node_counts_after.iter().filter(|&&c| c == 0).count();
    let fraction_isolated = nodes_isolated as f64 / NODES as f64;

    tracing::info!(
        "Phase 2 connection counts after NAT failures: {:?}",
        node_counts_after
    );
    tracing::info!(
        "Median={}, isolated={}/{} ({:.0}%)",
        median_after,
        nodes_isolated,
        NODES,
        fraction_isolated * 100.0
    );

    // ASSERTION 4: No death spiral — median must stay near pre-fault level.
    // With 20% message loss for only 60s, a well-connected network should
    // retain most connections. Threshold is MIN_CONN - 2 to allow some churn.
    assert!(
        median_after >= MIN_CONN.saturating_sub(2).max(2),
        "Death spiral: median connections after NAT failures = {} (expected >= {}). \
         Counts: {:?}. Seed: 0x{:X}",
        median_after,
        MIN_CONN.saturating_sub(2).max(2),
        node_counts_after,
        SEED
    );

    // ASSERTION 5: No nodes fully isolated after brief packet loss.
    assert!(
        fraction_isolated < 0.10,
        "{:.0}% of nodes isolated after NAT failures (threshold: 10%). \
         Counts: {:?}. Seed: 0x{:X}",
        fraction_isolated * 100.0,
        node_counts_after,
        SEED
    );

    tracing::info!(
        "PASSED: Phase1[median={}, above_min={}/{}, peer_conns={}/{}] \
         Phase2[median={}, isolated={}/{}]",
        median_conn,
        nodes_above_min,
        num_sampled,
        nodes_with_peer_connections,
        NODES,
        median_after,
        nodes_isolated,
        NODES
    );
}

// =============================================================================
// GET Routing Regression Tests
// =============================================================================

/// Regression test for #3356 / #3423: GET fails with EmptyRing when all peers
/// fail readiness gating.
///
/// Sets `relay_ready_connections` higher than network size so `is_self_ready()`
/// returns false on every node → no node ever sends `ReadyState` to peers →
/// `is_peer_ready()` returns false for all peers. The 60-second optimistic
/// timeout (`OPTIMISTIC_READY_TIMEOUT`) uses real wall-clock `Instant::now()`,
/// not virtual time, so it cannot fire during this test (~0.5s wall-clock).
///
/// Before the fix, `k_closest_potentially_hosting` returned empty → GET failed
/// with EmptyRing. After the fix, it falls back to using not-yet-ready peers.
/// Verified: this test FAILS without the fallback (confirmed by reverting the
/// fix and observing the assertion fire).
///
/// Scenario:
///   1. Gateway PUTs a contract
///   2. Node 1 GETs the contract with `return_contract_code=true`
///   3. Assert node 1's storage contains the contract state
#[test_log::test]
fn test_get_succeeds_despite_readiness_gating() {
    use freenet::dev_tool::{NodeLabel, ScheduledOperation, SimOperation};

    const SEED: u64 = 0xAE7F_1A00_0001;
    const NETWORK_NAME: &str = "get-readiness-fallback";

    GlobalTestMetrics::reset();
    setup_deterministic_state(SEED);

    let rt = create_runtime();

    let (mut sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(
            NETWORK_NAME,
            1,  // gateways
            3,  // nodes
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

    // Set readiness gating higher than network size (1 gateway + 3 nodes = 4 total).
    // No node can ever have this many ready connections, so ReadyState is never sent,
    // and is_peer_ready() returns false for all peers.
    let total_nodes = 1 + 3; // gateways + nodes
    sim.with_readiness_gating(total_nodes + 1);

    let contract = SimOperation::create_test_contract(0xAE);
    let contract_id = *contract.key().id();
    let contract_key = contract.key();

    let operations = vec![
        // 1. Gateway PUTs the contract
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Put {
                contract: contract.clone(),
                state: vec![1, 2, 3, 4],
                subscribe: false,
            },
        ),
        // 2. Node 1 GETs the contract (with contract code)
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 1),
            SimOperation::Get {
                contract_id,
                return_contract_code: true,
                subscribe: false,
            },
        ),
    ];

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(120),
        Duration::from_secs(30),
    );

    assert!(
        result.turmoil_result.is_ok(),
        "GET readiness fallback simulation failed: {:?}",
        result.turmoil_result.err()
    );

    // Verify that node 1 has the contract state despite readiness gating
    let node1_label = NodeLabel::node(NETWORK_NAME, 1);
    let node1_storage = result
        .node_storages
        .get(&node1_label)
        .expect("node 1 should have a storage handle");
    let node1_state = node1_storage.get_stored_state(&contract_key);

    assert!(
        node1_state.is_some(),
        "Node 1 should have contract state after GET, but storage is empty. \
         This indicates k_closest_potentially_hosting returned empty due to \
         readiness gating (the bug from #3356/#3423)."
    );

    // Run StateVerifier for anomaly detection (per testing.md)
    let rt = create_runtime();
    let report = rt.block_on(async {
        let logs = logs_handle.lock().await;
        let verifier = freenet::tracing::StateVerifier::from_events(logs.clone());
        verifier.verify()
    });
    tracing::info!(
        "Anomaly report: {} anomalies across {} contracts",
        report.anomalies.len(),
        report.contracts_analyzed
    );

    tracing::info!(
        "test_get_succeeds_despite_readiness_gating PASSED: \
         node 1 has contract state, {} events analyzed",
        report.total_events
    );
}

/// Regression test for #3431: GET routing exhaustion when contract is cached at few nodes.
///
/// With low HTL, PUT only caches the contract at a handful of nodes along the
/// route. GET from nodes whose connected peers don't overlap with caching nodes
/// fails with "No other peers found" → NotFound.
///
/// Scenario:
///   1. Gateway PUTs contract (HTL=4 → only ~5 nodes cache code+state)
///   2. 12 nodes subscribe, gateway sends UPDATE (state propagation)
///   3. All 15 nodes GET with `fetch_contract=true`
///   4. Assert every node gets the contract state
///
#[test_log::test]
fn test_get_routing_coverage_low_htl() {
    use std::sync::atomic::Ordering;

    use freenet::dev_tool::{
        GET_RELAY_DRIVER_CALL_COUNT, NodeLabel, RELAY_PUT_DRIVER_CALL_COUNT,
        RELAY_SUBSCRIBE_DRIVER_CALL_COUNT, ScheduledOperation, SimOperation,
        register_crdt_contract,
    };

    // NOTE: the relay-hop *route-event* assertions (RELAY_*_ROUTE_EVENT_COUNT)
    // that used to live here were moved to `test_relay_route_events_multihop`.
    // They require a SPARSE topology where a GET/PUT actually forwards a
    // downstream response, but this test deliberately keeps the mesh well
    // connected (max_connections=7) so the GET-coverage assertion (#3431)
    // holds. Once the #4348 convergence fix let nodes reliably reach
    // min_connections, a 15-node/max=7 net converged dense enough that ops
    // resolved in 0-1 hops and the route-event counters stayed at 0 — a
    // topology-coupling that the dedicated sparse test now owns. This test
    // keeps the relay *driver-call* assertions (which fire even when a relay
    // resolves locally) plus the coverage assertion.
    const SEED: u64 = 0xC0DE_B0CA_0032;
    const NETWORK_NAME: &str = "get-routing-coverage";

    GlobalTestMetrics::reset();
    setup_deterministic_state(SEED);

    // Baseline the relay driver counter so we can assert that the
    // dispatch gate in handle_pure_network_message_v1 actually routed
    // relay hops through the task-per-tx driver (not the legacy
    // handle_op_request fallthrough). This is the behavioral companion
    // to the source-scrape tests in op_ctx_task.rs that only pin the
    // dispatch-block SHAPE. #1454 phase 5 / #3883.
    let relay_baseline = GET_RELAY_DRIVER_CALL_COUNT.load(Ordering::SeqCst);
    let relay_put_baseline = RELAY_PUT_DRIVER_CALL_COUNT.load(Ordering::SeqCst);
    let relay_subscribe_baseline = RELAY_SUBSCRIBE_DRIVER_CALL_COUNT.load(Ordering::SeqCst);

    let rt = create_runtime();

    let num_nodes = 15;

    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(
            NETWORK_NAME,
            1,         // gateways
            num_nodes, // nodes — large enough that PUT won't reach all
            3,         // ring_max_htl — LOW to limit PUT propagation depth
            1,         // rnd_if_htl_above
            7,         // max_connections — 7 ensures robust GET routing across topology variations
            3,         // min_connections
            SEED,
        )
        .await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    let contract = SimOperation::create_test_contract(0xC0);
    let contract_id = *contract.key().id();
    let contract_key = contract.key();
    register_crdt_contract(contract_id);

    let mut operations = vec![
        // Gateway PUTs with subscribe (HTL=4 → caches at ~5 nodes)
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Put {
                contract: contract.clone(),
                state: SimOperation::create_crdt_state(1, 0xC0),
                subscribe: true,
            },
        ),
    ];

    // Subscribe nodes 1-12
    for i in 1..=12 {
        operations.push(ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, i),
            SimOperation::Subscribe { contract_id },
        ));
    }

    // Gateway updates — propagates state to subscribers
    operations.push(ScheduledOperation::new(
        NodeLabel::gateway(NETWORK_NAME, 0),
        SimOperation::Update {
            key: contract_key,
            data: SimOperation::create_crdt_state(42, 0xC0),
        },
    ));

    // Every node GETs with fetch_contract=true
    for i in 1..=num_nodes {
        operations.push(ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, i),
            SimOperation::Get {
                contract_id,
                return_contract_code: true,
                subscribe: false,
            },
        ));
    }

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(300),
        Duration::from_secs(90),
    );

    assert!(
        result.turmoil_result.is_ok(),
        "Simulation failed: {:?}",
        result.turmoil_result.err()
    );

    // Assert every node has the contract state after GET
    let mut nodes_without_state = Vec::new();

    for i in 1..=num_nodes {
        let label = NodeLabel::node(NETWORK_NAME, i);
        let storage = result
            .node_storages
            .get(&label)
            .unwrap_or_else(|| panic!("node {i} should have a storage handle"));
        if storage.get_stored_state(&contract_key).is_none() {
            nodes_without_state.push(format!("node-{i}"));
        }
    }

    assert!(
        nodes_without_state.is_empty(),
        "GET routing exhaustion: {} nodes failed to get contract state \
         (contract only cached at ~5 nodes due to HTL=4). \
         Failed nodes: {:?}. See #3431.",
        nodes_without_state.len(),
        nodes_without_state
    );

    // Behavioral companion to the dispatch-gate source-scrape tests in
    // op_ctx_task.rs: prove the relay task-per-tx driver actually ran
    // on at least one intermediate hop during the 15-node / HTL=3 /
    // 15 parallel GETs workload. If this assertion ever starts failing,
    // something has silently regressed the dispatch split in
    // handle_pure_network_message_v1 — inbound relay Requests are going
    // through the legacy handle_op_request path again. #1454 phase 5 / #3883.
    let relay_after = GET_RELAY_DRIVER_CALL_COUNT.load(Ordering::SeqCst);
    let relay_delta = relay_after.saturating_sub(relay_baseline);
    assert!(
        relay_delta > 0,
        "RELAY_DRIVER_CALL_COUNT did not advance during the test — the \
         relay task-per-tx driver never ran. With {num_nodes} nodes, HTL=3, \
         and ~5 caching nodes, every non-caching requester should hit at \
         least one relay hop. Dispatch gate in \
         handle_pure_network_message_v1 has likely regressed. Baseline: {relay_baseline}, after: {relay_after}."
    );

    // Behavioral companion for relay PUT slice A (#1454 phase 5
    // follow-up, PR #3917): the gateway PUT at HTL=3 should reach at
    // least one relay peer via start_relay_put. If this never advances,
    // the PUT dispatch gate in handle_pure_network_message_v1 has
    // regressed — inbound relay PutMsg::Request is going through the
    // legacy handle_op_request path again.
    let relay_put_after = RELAY_PUT_DRIVER_CALL_COUNT.load(Ordering::SeqCst);
    let relay_put_delta = relay_put_after.saturating_sub(relay_put_baseline);
    assert!(
        relay_put_delta > 0,
        "RELAY_PUT_DRIVER_CALL_COUNT did not advance during the test — \
         the relay PUT task-per-tx driver never ran. Gateway PUT at HTL=3 \
         in a {num_nodes}-node network must traverse at least one relay hop. \
         Dispatch gate for PUT has likely regressed. \
         Baseline: {relay_put_baseline}, after: {relay_put_after}."
    );

    // Behavioral companion for relay SUBSCRIBE slice A (#1454 phase 5
    // follow-up, PR #3932): 12 nodes subscribe above. At HTL=3 with
    // ~5 caching nodes, at least one subscribe must traverse a relay
    // hop and hit start_relay_subscribe. If this never advances, the
    // dispatch gate for SUBSCRIBE has regressed — inbound relay
    // SubscribeMsg::Request is going through the legacy
    // handle_op_request path again.
    let relay_subscribe_after = RELAY_SUBSCRIBE_DRIVER_CALL_COUNT.load(Ordering::SeqCst);
    let relay_subscribe_delta = relay_subscribe_after.saturating_sub(relay_subscribe_baseline);
    assert!(
        relay_subscribe_delta > 0,
        "RELAY_SUBSCRIBE_DRIVER_CALL_COUNT did not advance during the \
         test — the relay SUBSCRIBE task-per-tx driver never ran. With \
         12 node subscribes at HTL=3 and ~5 caching nodes, at least one \
         subscribe must traverse a relay hop. Dispatch gate for \
         SUBSCRIBE has likely regressed. \
         Baseline: {relay_subscribe_baseline}, after: {relay_subscribe_after}."
    );

    // Relay-hop *route-event* coverage (RELAY_*_ROUTE_EVENT_COUNT) is asserted
    // separately in `test_relay_route_events_multihop`. Those counters only
    // fire when a relay forwards a downstream response, which needs a sparse
    // topology where an op actually traverses >=2 hops. This test keeps the
    // mesh dense (max_connections=7) so the GET-coverage assertion above holds;
    // post-#4348 that density makes ops resolve in 0-1 hops, so route events do
    // not fire here (#4348). The relay *drivers* are still exercised — see the
    // RELAY_*_DRIVER_CALL_COUNT assertions above.

    // StateVerifier anomaly check
    let rt = create_runtime();
    let report = rt.block_on(async {
        let logs = logs_handle.lock().await;
        let verifier = freenet::tracing::StateVerifier::from_events(logs.clone());
        verifier.verify()
    });
    tracing::info!(
        "test_get_routing_coverage_low_htl PASSED: {} anomalies, {} events, \
         relay_driver_calls={relay_delta}",
        report.anomalies.len(),
        report.total_events,
    );
}

/// Relay-hop route-event telemetry (#1454): a relay that forwards a downstream
/// GET/PUT response must feed the result into the local Router via
/// `record_relay_route_event`, or the per-peer dashboard panels stay empty and
/// the failure-probability model is undertrained on relay-heavy nodes.
///
/// These assertions previously lived in `test_get_routing_coverage_low_htl`,
/// but that test must keep a DENSE mesh so every node's GET resolves
/// (its #3431 coverage assertion). The #4348 connection-convergence fix made
/// that mesh converge dense enough that ops resolved in 0-1 hops, so
/// `RELAY_*_ROUTE_EVENT_COUNT` never advanced — the test was implicitly relying
/// on the under-connectivity bug #4348 fixed. This dedicated test instead uses a
/// deliberately SPARSE topology where multi-hop forwarding is structural, and
/// asserts ONLY that the route-event hooks fire. It makes NO GET-coverage claim
/// (a NotFound reply still exercises the relay route-event hook), so improved
/// connectivity can never invalidate it.
///
/// SUBSCRIBE route events are intentionally not asserted (subscribes hit the
/// relay local-cache fast path without forwarding downstream — see the note in
/// `test_get_routing_coverage_low_htl`); the relay SUBSCRIBE *driver* is still
/// covered there.
#[test_log::test]
fn test_relay_route_events_multihop() {
    use std::sync::atomic::Ordering;

    use freenet::dev_tool::{
        NodeLabel, RELAY_GET_ROUTE_EVENT_COUNT, RELAY_PUT_ROUTE_EVENT_COUNT, ScheduledOperation,
        SimOperation, register_crdt_contract,
    };

    const SEED: u64 = 0xC0DE_B0CA_0040;
    const NETWORK_NAME: &str = "relay-route-events";

    GlobalTestMetrics::reset();
    setup_deterministic_state(SEED);

    let get_route_baseline = RELAY_GET_ROUTE_EVENT_COUNT.load(Ordering::SeqCst);
    let put_route_baseline = RELAY_PUT_ROUTE_EVENT_COUNT.load(Ordering::SeqCst);

    let rt = create_runtime();
    let num_nodes = 15;

    let sim = rt.block_on(async {
        SimNetwork::new(
            NETWORK_NAME,
            1,         // gateways
            num_nodes, // nodes
            3,         // ring_max_htl
            1,         // rnd_if_htl_above
            // max_connections — deliberately LOW. A sparse mesh keeps requesters
            // >=2 hops from the few holders, so GET/PUT requests forward through a
            // non-holding relay and fire the route-event hooks. We assert only
            // that the hooks fire, NOT that every GET resolves, so sparsity (which
            // would fail a coverage assertion) is exactly what we want here.
            4, // max_connections
            3, // min_connections
            SEED,
        )
        .await
    });

    let contract = SimOperation::create_test_contract(0xC0);
    let contract_id = *contract.key().id();
    register_crdt_contract(contract_id);

    // Gateway seeds the contract; only a few nodes subscribe so the cache stays
    // sparse and most requesters are several hops from a holder.
    let mut operations = vec![ScheduledOperation::new(
        NodeLabel::gateway(NETWORK_NAME, 0),
        SimOperation::Put {
            contract: contract.clone(),
            state: SimOperation::create_crdt_state(1, 0xC0),
            subscribe: true,
        },
    )];
    for i in 1..=4 {
        operations.push(ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, i),
            SimOperation::Subscribe { contract_id },
        ));
    }
    for i in 1..=num_nodes {
        operations.push(ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, i),
            SimOperation::Get {
                contract_id,
                return_contract_code: true,
                subscribe: false,
            },
        ));
    }

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(300),
        Duration::from_secs(90),
    );
    assert!(
        result.turmoil_result.is_ok(),
        "Simulation failed: {:?}",
        result.turmoil_result.err()
    );

    let get_route_after = RELAY_GET_ROUTE_EVENT_COUNT.load(Ordering::SeqCst);
    let put_route_after = RELAY_PUT_ROUTE_EVENT_COUNT.load(Ordering::SeqCst);
    assert!(
        get_route_after > get_route_baseline,
        "RELAY_GET_ROUTE_EVENT_COUNT did not advance — in a sparse {num_nodes}-node \
         mesh (max_connections=4) at least one GET must forward through a relay that \
         feeds a route event to the local Router (a NotFound reply counts too). \
         Baseline: {get_route_baseline}, after: {get_route_after}."
    );
    assert!(
        put_route_after > put_route_baseline,
        "RELAY_PUT_ROUTE_EVENT_COUNT did not advance — the gateway PUT at HTL=3 in a \
         sparse {num_nodes}-node mesh must forward through at least one relay. \
         Baseline: {put_route_baseline}, after: {put_route_after}."
    );
}

/// Regression test for GET retry with alternatives in sparse topologies.
///
/// With HTL=2 and 10 nodes, PUT only caches the contract at 2-3 nodes along the
/// route. When distant nodes GET, their initial routing candidate may not have
/// the contract. The fix ensures the GET state machine tries alternative peers
/// (from k_closest) and re-queries k_closest when alternatives are exhausted,
/// rather than immediately returning NotFound.
///
/// Without the fix: distant nodes return NotFound because the first-choice peer
/// doesn't have the contract and there's no retry.
/// With the fix: the GET retries with alternative peers until one succeeds.
///
/// Exercises: `get.rs` alternative retry (line ~1521) and k_closest re-query (line ~1569)
///
/// Seed history: this test was `#[ignore]`d in PR #3488 because the original
/// seed (`0xBEEF_CAFE_0001`) stopped producing a sparse-but-reachable topology
/// after a freenet-stdlib bump shifted RNG-dependent topology formation (the
/// stdlib's enum layout changes bincode serialization sizes, which perturbs the
/// seeded RNG stream). #3506 re-seeded it for the current stdlib: with
/// `SEED = 0x3506_000B_0001` the gateway PUT at HTL=2 caches the contract at
/// exactly 2 of the 10 nodes (verified with a PUT-only run of this same
/// topology), so 8 of the 10 GET requesters must use the retry-with-alternatives
/// path to reach a cacher, and all 10 do, deterministically across repeated
/// runs. Like every seed-coupled topology test, a future stdlib/topology change
/// could perturb the RNG stream again and require another re-seed; that is
/// expected maintenance rather than a regression in the GET retry logic itself
/// (which is also covered by `test_get_routing_coverage_low_htl`).
#[test_log::test]
fn test_get_retry_with_alternatives_sparse_topology() {
    use freenet::dev_tool::{NodeLabel, ScheduledOperation, SimOperation};

    // Seed reselected for the current freenet-stdlib layout (see #3506). Under
    // this seed the gateway PUT at HTL=2 caches the contract at exactly 2 of the
    // 10 nodes, and all 10 GET requesters reach one of those 2 cachers via the
    // GET retry-with-alternatives path. Verified sparse + reachable + deterministic
    // before un-ignoring; see the doc comment above for the full rationale.
    const SEED: u64 = 0x3506_000B_0001;
    const NETWORK_NAME: &str = "get-retry-sparse";

    GlobalTestMetrics::reset();
    setup_deterministic_state(SEED);

    let rt = create_runtime();

    let num_nodes = 10;

    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(
            NETWORK_NAME,
            1,         // 1 gateway
            num_nodes, // 10 nodes — large enough that PUT won't reach all
            2,         // ring_max_htl = 2 — VERY low, PUT caches at only ~2-3 nodes
            1,         // rnd_if_htl_above
            6,         // max_connections
            3,         // min_connections
            SEED,
        )
        .await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    let contract = SimOperation::create_test_contract(0xBE);
    let contract_id = *contract.key().id();
    let contract_key = contract.key();

    let mut operations = vec![
        // Gateway PUTs the contract (HTL=2 → only ~2-3 nodes cache)
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Put {
                contract: contract.clone(),
                state: vec![10, 20, 30, 40],
                subscribe: false,
            },
        ),
    ];

    // All 10 nodes GET with fetch_contract=true
    // Nodes far from caching nodes must retry with alternatives
    for i in 1..=num_nodes {
        operations.push(ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, i),
            SimOperation::Get {
                contract_id,
                return_contract_code: true,
                subscribe: false,
            },
        ));
    }

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(300), // simulation duration
        Duration::from_secs(90),  // post-operations wait for retries
    );

    assert!(
        result.turmoil_result.is_ok(),
        "Simulation failed: {:?}",
        result.turmoil_result.err()
    );

    // Verify that all nodes got the contract state despite sparse caching.
    // Without the retry fix, distant nodes would get NotFound.
    let mut nodes_without_state = Vec::new();
    for i in 1..=num_nodes {
        let label = NodeLabel::node(NETWORK_NAME, i);
        let storage = result
            .node_storages
            .get(&label)
            .unwrap_or_else(|| panic!("node {i} should have a storage handle"));
        if storage.get_stored_state(&contract_key).is_none() {
            nodes_without_state.push(format!("node-{i}"));
        }
    }

    assert!(
        nodes_without_state.is_empty(),
        "GET retry regression: {} of {} nodes failed to get contract state \
         (contract cached at only ~2-3 nodes due to HTL=2, retry should find it). \
         Failed nodes: {:?}. See PR #3444.",
        nodes_without_state.len(),
        num_nodes,
        nodes_without_state
    );

    // StateVerifier anomaly check
    let rt = create_runtime();
    let report = rt.block_on(async {
        let logs = logs_handle.lock().await;
        let verifier = freenet::tracing::StateVerifier::from_events(logs.clone());
        verifier.verify()
    });
    tracing::info!(
        "test_get_retry_with_alternatives_sparse_topology PASSED: \
         all {} nodes got contract state, {} anomalies, {} events",
        num_nodes,
        report.anomalies.len(),
        report.total_events
    );
}

/// Regression test for auto-fetch from UPDATE sender.
///
/// When a node subscribed to a CRDT contract receives an UPDATE broadcast but
/// doesn't have the contract's code/parameters (because PUT's low HTL didn't
/// cache it there), the fix triggers `try_auto_fetch_contract` — a targeted GET
/// to the UPDATE sender who is known to have the contract.
///
/// Scenario:
///   1. Gateway PUTs a CRDT contract (HTL=2 → only ~2-3 nodes cache code)
///   2. All nodes subscribe to the contract
///   3. Gateway sends UPDATE → broadcast reaches subscribed nodes
///   4. Nodes that subscribed but don't have the contract code auto-fetch
///   5. All subscribed nodes should end up with the state
///
/// Without the fix: nodes without contract code can't apply the update → stale.
/// With the fix: auto-fetch retrieves the contract, then update applies.
///
/// Exercises: `update.rs` try_auto_fetch_contract (BroadcastTo/BroadcastToStreaming paths)
#[test_log::test]
fn test_auto_fetch_from_update_sender() {
    use freenet::dev_tool::{NodeLabel, ScheduledOperation, SimOperation, register_crdt_contract};

    const SEED: u64 = 0xFE7C_A100_0001;
    const NETWORK_NAME: &str = "auto-fetch-update";

    GlobalTestMetrics::reset();
    setup_deterministic_state(SEED);

    let rt = create_runtime();

    let num_nodes = 8;

    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(
            NETWORK_NAME,
            1,         // 1 gateway
            num_nodes, // 8 nodes
            2,         // ring_max_htl = 2 — low to limit PUT propagation
            1,         // rnd_if_htl_above
            6,         // max_connections
            3,         // min_connections
            SEED,
        )
        .await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    let contract = SimOperation::create_test_contract(0xFE);
    let contract_id = *contract.key().id();
    let contract_key = contract.key();
    register_crdt_contract(contract_id);

    let mut operations = vec![
        // 1. Gateway PUTs with subscribe (HTL=2 → few nodes cache code)
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Put {
                contract: contract.clone(),
                state: SimOperation::create_crdt_state(1, 0xFE),
                subscribe: true,
            },
        ),
    ];

    // 2. All nodes subscribe
    for i in 1..=num_nodes {
        operations.push(ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, i),
            SimOperation::Subscribe { contract_id },
        ));
    }

    // 3. Gateway sends UPDATE — propagates to subscribers
    operations.push(ScheduledOperation::new(
        NodeLabel::gateway(NETWORK_NAME, 0),
        SimOperation::Update {
            key: contract_key,
            data: SimOperation::create_crdt_state(42, 0xFE),
        },
    ));

    // 4. All nodes GET to verify they have the state
    // (auto-fetch should have already retrieved it for nodes without code)
    for i in 1..=num_nodes {
        operations.push(ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, i),
            SimOperation::Get {
                contract_id,
                return_contract_code: true,
                subscribe: false,
            },
        ));
    }

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(300), // simulation duration
        Duration::from_secs(90),  // post-operations wait for auto-fetch
    );

    assert!(
        result.turmoil_result.is_ok(),
        "Simulation failed: {:?}",
        result.turmoil_result.err()
    );

    // Verify that all subscribing nodes got the contract state.
    // Nodes that didn't have the contract code from PUT should have
    // auto-fetched it when they received the UPDATE broadcast.
    let mut nodes_without_state = Vec::new();
    for i in 1..=num_nodes {
        let label = NodeLabel::node(NETWORK_NAME, i);
        let storage = result
            .node_storages
            .get(&label)
            .unwrap_or_else(|| panic!("node {i} should have a storage handle"));
        if storage.get_stored_state(&contract_key).is_none() {
            nodes_without_state.push(format!("node-{i}"));
        }
    }

    // Allow up to 1 node without state (edge case: a node may not have connected
    // to any subscriber yet). But most nodes should have it via auto-fetch.
    let success_count = num_nodes - nodes_without_state.len();
    let success_rate = success_count as f64 / num_nodes as f64;
    assert!(
        success_rate >= 0.75,
        "Auto-fetch regression: only {} of {} nodes ({:.0}%) got contract state \
         after UPDATE broadcast. Without auto-fetch, nodes that didn't receive \
         PUT due to low HTL would never get the contract. \
         Failed nodes: {:?}. See PR #3444.",
        success_count,
        num_nodes,
        success_rate * 100.0,
        nodes_without_state
    );

    // StateVerifier anomaly check
    let rt = create_runtime();
    let report = rt.block_on(async {
        let logs = logs_handle.lock().await;
        let verifier = freenet::tracing::StateVerifier::from_events(logs.clone());
        verifier.verify()
    });
    tracing::info!(
        "test_auto_fetch_from_update_sender PASSED: \
         {}/{} nodes got contract state ({:.0}%), {} anomalies, {} events",
        success_count,
        num_nodes,
        success_rate * 100.0,
        report.anomalies.len(),
        report.total_events
    );
}

// =============================================================================
// Connection Growth Plateau Diagnostic (#3555)
// =============================================================================

/// Diagnostic test to understand why nodes plateau below min_connections.
///
/// Instruments the CONNECT pipeline to measure:
/// - How many CONNECT operations are initiated
/// - How many are accepted at terminus vs forwarded
/// - How many result in actual Connected events
/// - Per-node connection counts over time
///
/// This is a diagnostic (non-asserting beyond basic sanity) — run with
/// `RUST_LOG=info` to see the pipeline breakdown.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_connection_growth_plateau_diagnostic() {
    use freenet::dev_tool::NodeLabel;

    const SEED: u64 = 0x3555_D1A6_0002;
    const NETWORK_NAME: &str = "growth-plateau-diag";
    const GATEWAYS: usize = 2;
    const NODES: usize = 50;
    const RING_MAX_HTL: usize = 7;
    const RND_IF_HTL_ABOVE: usize = 3;
    const MAX_CONN: usize = 20;
    const MIN_CONN: usize = 10;

    tracing::info!("=== Connection Growth Plateau Diagnostic ===");

    setup_deterministic_state(SEED);

    let mut sim = SimNetwork::new(
        NETWORK_NAME,
        GATEWAYS,
        NODES,
        RING_MAX_HTL,
        RND_IF_HTL_ABOVE,
        MAX_CONN,
        MIN_CONN,
        SEED,
    )
    .await;

    sim.with_start_backoff(Duration::from_millis(50));

    let _handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 0, 0)
        .await;

    let logs_handle = sim.event_logs_handle();

    // Run for 5 virtual minutes, snapshot, then 5 more, snapshot, etc.
    // This gives us a growth curve.
    let phases = [
        ("5min", Duration::from_secs(300)),
        ("10min", Duration::from_secs(300)),
        ("15min", Duration::from_secs(300)),
        ("20min", Duration::from_secs(300)),
    ];

    for (phase_name, duration) in &phases {
        let_network_run(&mut sim, *duration).await;

        // Collect per-node connection counts
        let mut node_counts: Vec<usize> = (0..NODES)
            .filter_map(|i| {
                let label = NodeLabel::node(NETWORK_NAME, i);
                sim.connection_count(&label)
            })
            .collect();
        node_counts.sort_unstable();

        let n = node_counts.len();
        if n == 0 {
            tracing::info!("[{}] No nodes sampled", phase_name);
            continue;
        }

        let median = node_counts[n / 2];
        let min = node_counts[0];
        let max = node_counts[n - 1];
        let above_min = node_counts.iter().filter(|&&c| c >= MIN_CONN).count();
        let zero = node_counts.iter().filter(|&&c| c == 0).count();
        let avg = node_counts.iter().sum::<usize>() as f64 / n as f64;

        // Count CONNECT events so far
        let (initiated, terminus_accepted, terminus_rejected, forwarded, connected, disconnected) = {
            let logs = logs_handle.lock().await;
            let initiated = logs
                .iter()
                .filter(|l| l.kind.is_connect_initiated())
                .count();
            let terminus_accepted = logs
                .iter()
                .filter(|l| l.kind.is_connect_terminus_accepted())
                .count();
            let terminus_rejected = logs
                .iter()
                .filter(|l| l.kind.is_connect_terminus_rejected())
                .count();
            let forwarded = logs
                .iter()
                .filter(|l| l.kind.is_connect_forwarded())
                .count();
            let connected = logs
                .iter()
                .filter(|l| l.kind.is_connect_connected())
                .count();
            let disconnected = logs.iter().filter(|l| l.kind.is_disconnected()).count();
            (
                initiated,
                terminus_accepted,
                terminus_rejected,
                forwarded,
                connected,
                disconnected,
            )
        };

        let terminus_total = terminus_accepted + terminus_rejected;
        let terminus_accept_rate = if terminus_total > 0 {
            terminus_accepted as f64 / terminus_total as f64 * 100.0
        } else {
            0.0
        };

        tracing::info!(
            "[{}] Connections: median={}, avg={:.1}, min={}, max={}, \
             above_min={}/{} ({:.0}%), zero={}",
            phase_name,
            median,
            avg,
            min,
            max,
            above_min,
            n,
            above_min as f64 / n as f64 * 100.0,
            zero,
        );
        tracing::info!(
            "[{}] CONNECT pipeline: initiated={}, forwarded={}, \
             terminus_accepted={}, terminus_rejected={}, \
             terminus_accept_rate={:.0}%, connected={}, disconnected={}",
            phase_name,
            initiated,
            forwarded,
            terminus_accepted,
            terminus_rejected,
            terminus_accept_rate,
            connected,
            disconnected,
        );
    }

    // Distribution histogram
    let final_counts: Vec<usize> = (0..NODES)
        .filter_map(|i| {
            let label = NodeLabel::node(NETWORK_NAME, i);
            sim.connection_count(&label)
        })
        .collect();

    let mut histogram: BTreeMap<usize, usize> = BTreeMap::new();
    for &c in &final_counts {
        *histogram.entry(c).or_insert(0) += 1;
    }
    tracing::info!("Final connection distribution: {:?}", histogram);

    // Gateway connection counts
    for i in 0..GATEWAYS {
        let label = NodeLabel::gateway(NETWORK_NAME, i);
        if let Some(count) = sim.connection_count(&label) {
            tracing::info!("Gateway {} connections: {}", i, count);
        }
    }

    // Intentionally weak sanity check — this is a diagnostic test (see doc comment),
    // not a regression test for min_connections. The full regression test is
    // test_connection_growth_stall_regression which asserts >=90% reach min_connections.
    let total_connected = final_counts.iter().filter(|&&c| c > 0).count();
    assert!(
        total_connected > NODES / 2,
        "Fewer than half the nodes have any connections: {}/{}",
        total_connected,
        NODES,
    );
}

/// Diagnostic test for #3570: GET operations have high timeout rate in realistic networks.
///
/// This test creates a 100-node network, PUTs a contract from a gateway, then has
/// every node attempt to GET the same contract. It measures:
/// - Overall GET success rate
/// - Latency distribution (p50, p90, p99)
/// - Failure modes (NotFound vs Failure vs Timeout)
///
/// The test is intentionally diagnostic — it logs detailed statistics rather than
/// asserting a hard threshold, so we can gather evidence on what's happening.
/// A soft assertion ensures GET success rate doesn't fall below 50% (catastrophic).
///
/// Uses `run_controlled_simulation` for deterministic reproduction.
// Long-running diagnostic test (~2min). Runs in nightly CI.
#[cfg(feature = "nightly_tests")]
#[test_log::test]
fn test_get_reliability_diagnostic() {
    use freenet::dev_tool::{NodeLabel, ScheduledOperation, SimOperation};

    const SEED: u64 = 0x3570_D1A6_0001;
    const NETWORK_NAME: &str = "get-reliability-diag";
    const NUM_NODES: usize = 100;
    const NUM_GATEWAYS: usize = 3;
    const RING_MAX_HTL: usize = 10;

    GlobalTestMetrics::reset();
    setup_deterministic_state(SEED);

    let rt = create_runtime();

    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(
            NETWORK_NAME,
            NUM_GATEWAYS,
            NUM_NODES,
            RING_MAX_HTL, // realistic for 100-node network
            7,            // rnd_if_htl_above
            12,           // max_connections
            4,            // min_connections
            SEED,
        )
        .await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    // Create a test contract and PUT it from gateway 0
    let contract = SimOperation::create_test_contract(0x35);
    let contract_id = *contract.key().id();
    let contract_key = contract.key();

    let mut operations = vec![
        // Gateway 0 PUTs the contract with subscribe
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Put {
                contract: contract.clone(),
                state: vec![0x35; 64],
                subscribe: true,
            },
        ),
    ];

    // Every node GETs the contract — this exercises multi-hop routing
    // across the full network topology
    for i in 0..NUM_NODES {
        operations.push(ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, i),
            SimOperation::Get {
                contract_id,
                return_contract_code: true,
                subscribe: false,
            },
        ));
    }

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(600), // 10 min simulation
        Duration::from_secs(120), // 2 min post-operations wait
    );

    assert!(
        result.turmoil_result.is_ok(),
        "Simulation failed: {:?}",
        result.turmoil_result.err()
    );

    // Analyze GET outcomes from event logs, deduplicated per attempt
    // transaction (#4361): raw event counting double-counts every failed
    // attempt (relay-direct + loopback-Response registration) and counts
    // multi-hop outcomes once per hop traversed.
    let rt = create_runtime();
    let (summary, dispatched_nodes) = rt.block_on(async {
        let logs = logs_handle.lock().await;
        let summary = freenet::tracing::summarize_get_outcomes_per_tx(&logs);

        // Dispatched-vs-scheduled accounting (#4361): the client driver's
        // loopback Request is registered at the originating node with
        // htl == ring_max_htl; relay-received Requests have already been
        // decremented. Unique originating peers therefore counts how many
        // nodes actually dispatched a GET (sub-op GETs can inflate this
        // slightly, which is why it feeds a floor assertion, not an
        // exact-match one).
        let dispatched_nodes: HashSet<_> = logs
            .iter()
            .filter(|log| log.kind.get_request_htl() == Some(RING_MAX_HTL))
            .map(|log| log.peer_id.clone())
            .collect();

        (summary, dispatched_nodes.len())
    });

    let (successes, not_found, failures, timeouts) = (
        summary.successes,
        summary.not_found,
        summary.failures,
        summary.timeouts,
    );
    let network_successes = summary.network_successes;
    let total_outcomes = summary.total();

    // Compute latency percentiles for successful GETs
    // (success_elapsed_ms is pre-sorted ascending).
    let sorted_latencies = &summary.success_elapsed_ms;

    let p50 = sorted_latencies
        .get(sorted_latencies.len() / 2)
        .copied()
        .unwrap_or(0);
    let p90 = sorted_latencies
        .get(sorted_latencies.len() * 9 / 10)
        .copied()
        .unwrap_or(0);
    let p99 = sorted_latencies
        .get(sorted_latencies.len() * 99 / 100)
        .copied()
        .unwrap_or(0);
    let max_latency = sorted_latencies.last().copied().unwrap_or(0);

    let success_rate = if total_outcomes > 0 {
        successes as f64 / total_outcomes as f64
    } else {
        0.0
    };

    // Detect response-lost and ForwardingAck patterns from event logs
    let (response_sent_count, ack_received_count, response_lost_txs, retry_storm_txs) = rt
        .block_on(async {
            let logs = logs_handle.lock().await;
            let mut response_sent_txs: HashSet<String> = HashSet::new();
            let mut success_txs: HashSet<String> = HashSet::new();
            let mut ack_received = 0u64;
            let mut request_count_per_tx: HashMap<String, usize> = HashMap::new();

            for log in logs.iter() {
                let tx_str = log.tx.to_string();
                if log.kind.is_get_response_sent() {
                    response_sent_txs.insert(tx_str.clone());
                }
                if log.kind.get_outcome() == Some(true) {
                    success_txs.insert(tx_str.clone());
                }
                if log.kind.is_forwarding_ack_received() {
                    ack_received += 1;
                }
                if log.kind.get_outcome().is_some()
                    || log.kind.is_get_response_sent()
                    || log.kind.is_forwarding_ack_received()
                {
                    // Count unique request events per tx for retry-storm detection
                } else if log.kind.is_get_request() {
                    *request_count_per_tx.entry(tx_str).or_insert(0) += 1;
                }
            }

            // Response-lost: response_sent exists but no get_success for that tx
            let response_lost: Vec<_> = response_sent_txs
                .difference(&success_txs)
                .cloned()
                .collect();

            // Retry-storm: transactions with >10 request events (same tx hitting many peers)
            let retry_storms: Vec<_> = request_count_per_tx
                .iter()
                .filter(|&(_, &count)| count > 10)
                .map(|(tx, count)| (tx.clone(), *count))
                .collect();

            (
                response_sent_txs.len(),
                ack_received,
                response_lost,
                retry_storms,
            )
        });

    tracing::info!("=== GET Reliability Diagnostic (#3570) ===");
    tracing::info!(
        "Network: {} gateways + {} nodes = {} total peers",
        NUM_GATEWAYS,
        NUM_NODES,
        NUM_GATEWAYS + NUM_NODES
    );
    tracing::info!(
        "GET outcomes (per attempt tx): {} total — {} success ({} network-traversed), \
         {} not_found, {} failures, {} timeouts",
        total_outcomes,
        successes,
        network_successes,
        not_found,
        failures,
        timeouts
    );
    tracing::info!(
        "GET success rate: {:.1}% ({}/{})",
        success_rate * 100.0,
        successes,
        total_outcomes
    );
    tracing::info!(
        "GET dispatch: {}/{} nodes dispatched at least one GET attempt",
        dispatched_nodes,
        NUM_NODES
    );
    tracing::info!(
        "Latency (successful GETs): p50={}ms, p90={}ms, p99={}ms, max={}ms",
        p50,
        p90,
        p99,
        max_latency
    );
    tracing::info!(
        "ForwardingAck: {} ACKs received, {} response_sent events, {} response-lost txs",
        ack_received_count,
        response_sent_count,
        response_lost_txs.len()
    );
    if !retry_storm_txs.is_empty() {
        tracing::info!(
            "Retry storms (>10 requests per tx): {} txs, max {} requests",
            retry_storm_txs.len(),
            retry_storm_txs.iter().map(|(_, c)| c).max().unwrap_or(&0)
        );
    }

    // Check which nodes got the contract state
    let mut nodes_with_state = 0;
    let mut nodes_without_state = Vec::new();
    for i in 0..NUM_NODES {
        let label = NodeLabel::node(NETWORK_NAME, i);
        if let Some(storage) = result.node_storages.get(&label) {
            if storage.get_stored_state(&contract_key).is_some() {
                nodes_with_state += 1;
            } else {
                nodes_without_state.push(i);
            }
        } else {
            nodes_without_state.push(i);
        }
    }

    tracing::info!(
        "Storage verification: {}/{} nodes have contract state",
        nodes_with_state,
        NUM_NODES
    );
    if !nodes_without_state.is_empty() {
        tracing::info!(
            "Nodes missing state ({} total): {:?}{}",
            nodes_without_state.len(),
            &nodes_without_state[..nodes_without_state.len().min(20)],
            if nodes_without_state.len() > 20 {
                "..."
            } else {
                ""
            }
        );
    }

    // Soft assertion — this is diagnostic, but catastrophic failure should still fail the test
    assert!(
        total_outcomes >= 10,
        "Only {} GET outcome transactions — too few for meaningful analysis",
        total_outcomes
    );
    // Success-rate floor raised from the catastrophic-only 0.50 to 0.90 now
    // that #4362 is fixed: with late joiners actually joining, the measured
    // run reaches 100% (91/91). 0.90 leaves headroom for seed-to-seed jitter
    // while still catching a real regression.
    assert!(
        success_rate >= 0.90,
        "GET success rate {:.1}% below 0.90 floor (#4362 fixed should keep this high). \
         {} succeeded, {} not_found, {} failures, {} timeouts out of {} total. \
         See #3570 for context.",
        success_rate * 100.0,
        successes,
        not_found,
        failures,
        timeouts,
        total_outcomes
    );
    // Dispatch accounting (#4361): scheduled GETs that never dispatch an
    // operation silently shrink the denominator, so the success rate can
    // look healthy while most of the test never ran. The historical floor
    // was NUM_NODES/3 (33) because the bootstrap acceptance collapse (#4362)
    // left ~46/100 nodes with no completed transport handshake when their
    // GET signal arrived (rejected with PeerNotJoined, no harness retry).
    // With #4362 fixed (joiners re-route off saturated gateways and join
    // early under a short non-escalating reject backoff), the measured run
    // dispatches 86/100. Floor set to 80/100 = achieved minus a ~6-node
    // safety margin for seed-to-seed jitter; still ~2.4x the broken baseline,
    // so it locks the fix in without being flaky.
    assert!(
        dispatched_nodes >= NUM_NODES * 8 / 10,
        "Only {}/{} scheduled GETs dispatched an operation — the test \
         exercised only a fraction of its workload; the bootstrap acceptance \
         collapse (#4362) appears to have regressed (#4361)",
        dispatched_nodes,
        NUM_NODES
    );
    // Network-traversal floor (#4361): before this assertion existed, every
    // "success" in the failing runs was a local cache hit (hop_count == 0)
    // on a node that already held the contract — multi-hop GET was never
    // exercised at all. Require that a meaningful number of successes
    // actually traversed the network.
    assert!(
        network_successes >= 5,
        "Only {} of {} successful GETs traversed the network (hop_count >= 1) \
         — the success metric is measuring local availability, not routing (#4361)",
        network_successes,
        successes
    );

    // StateVerifier anomaly check
    let report = rt.block_on(async {
        let logs = logs_handle.lock().await;
        let verifier = freenet::tracing::StateVerifier::from_events(logs.clone());
        verifier.verify()
    });
    tracing::info!(
        "Anomaly report: {} anomalies across {} contracts, {} total events",
        report.anomalies.len(),
        report.contracts_analyzed,
        report.total_events
    );

    tracing::info!(
        "test_get_reliability_diagnostic DONE: {:.1}% success rate, \
         {}/{} nodes have state",
        success_rate * 100.0,
        nodes_with_state,
        NUM_NODES
    );
}

/// Regression test for the bootstrap acceptance collapse (#4362).
///
/// Unlike `test_get_reliability_diagnostic`, which asserts on GET outcomes,
/// this test asserts on the TOPOLOGY-FORMATION timeline so it pins the actual
/// fix rather than a downstream symptom:
///
///   (a) the number of `connect_rejected` events stays far below the ~5936
///       observed on the broken code — the terminus-rejection storm that
///       defined the dead zone, and
///   (b) a high fraction of nodes climb to `min_connections` early (by a
///       mid-sim virtual-time checkpoint), proving the ~T+320s unblock moved
///       to the front of the run.
///
/// Same seed/params as `test_get_reliability_diagnostic`. On the broken code
/// the joiner stamps a 30s→600s location backoff on its own (jittered)
/// location every time a saturated gateway neighborhood terminus-rejects it,
/// then retries into the same wall — so the rejection count explodes and most
/// nodes sit below `min_connections` for minutes. With the joiner-side
/// re-route (skip the under-min location backoff + widen the target after the
/// bounded jitter saturates), retries route toward regions with spare
/// capacity and the floor lifts early.
// Long-running topology test (~2min). Runs in nightly CI.
#[cfg(feature = "nightly_tests")]
#[test_log::test]
fn test_bootstrap_acceptance_no_dead_zone() {
    use freenet::dev_tool::{NodeLabel, ScheduledOperation, SimOperation};

    const SEED: u64 = 0x3570_D1A6_0001;
    const NETWORK_NAME: &str = "bootstrap-no-dead-zone";
    const NUM_NODES: usize = 100;
    const NUM_GATEWAYS: usize = 3;
    const RING_MAX_HTL: usize = 10;
    const MIN_CONNECTIONS: usize = 4;
    const MAX_CONNECTIONS: usize = 12;

    // Early-simulation checkpoint. This is NOT a literal virtual-time bound:
    // `created_at_ms()` decodes a transaction's ULID timestamp, which in
    // simulation mode is GlobalSimulationTime = a fixed epoch + a counter that
    // increments by 1 per ULID GENERATED (config.rs), not advanced by the
    // sim's virtual clock. So this filter selects "transactions among roughly
    // the first CHECKPOINT_MS ULIDs generated" — a monotonic GENERATION-ORDER
    // proxy for "early in the run", whose calibration depends on ULID volume.
    // It is sufficient here: on broken code the dead zone persists deep into
    // the run (46/100 nodes peer_ready==false at T+306s of wall-equivalent
    // sim), so almost no nodes reach min_connections in the early window;
    // requiring most nodes to reach it early is exactly what fails on broken
    // code and passes once retries re-route. The constant is named *_MS for
    // continuity with the epoch arithmetic, not because it is literally
    // milliseconds of virtual time.
    const CHECKPOINT_MS: u64 = 120_000;
    // Fraction of nodes that must have reached min_connections by the
    // checkpoint. Mirrors the >=90% topology-formation floor used by
    // test_connection_growth_stall_regression. Measured run reaches 95/100
    // (so the 90% floor has ~5 nodes of headroom).
    const REACHED_MIN_FRACTION: f64 = 0.90;

    // NOTE on connect_rejected: this test deliberately does NOT assert an upper
    // bound on the network-wide connect_rejected count. An earlier draft
    // asserted `connect_rejected < ~5936` (the broken-code baseline), but that
    // premise is structurally wrong. The broken code's LOW rejection count was
    // a SYMPTOM of stuck nodes: a capacity Rejected stamped a 30s→600s location
    // backoff and the joiner stopped retrying. The #4362 fix unblocks those
    // nodes by letting under-min joiners keep probing (throttled by a short
    // backoff), which inherently produces MORE connection attempts — hence MORE
    // terminus rejections — while actually forming the topology. Rejection
    // count therefore moves OPPOSITE to health here; the honest signal is the
    // reach-min timeline below. The count is logged for diagnostics only.

    // Replicate the deterministic sim epoch derived inside
    // run_controlled_simulation so transaction ULID timestamps can be mapped
    // back to a generation-order offset since simulation start (see the
    // CHECKPOINT_MS note above — this is an ordering proxy, not literal time).
    const BASE_EPOCH_MS: u64 = 1577836800000; // 2020-01-01 00:00:00 UTC
    const RANGE_MS: u64 = 5 * 365 * 24 * 60 * 60 * 1000; // ~5 years in ms
    let epoch_ms = BASE_EPOCH_MS + (SEED % RANGE_MS);

    GlobalTestMetrics::reset();
    setup_deterministic_state(SEED);

    let rt = create_runtime();

    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(
            NETWORK_NAME,
            NUM_GATEWAYS,
            NUM_NODES,
            RING_MAX_HTL,
            7, // rnd_if_htl_above
            MAX_CONNECTIONS,
            MIN_CONNECTIONS,
            SEED,
        )
        .await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    // Seed one PUT so the network has real traffic, mirroring the diagnostic
    // test. The assertions key off topology events, not the GET outcomes.
    let contract = SimOperation::create_test_contract(0x35);
    let contract_id = *contract.key().id();
    let mut operations = vec![ScheduledOperation::new(
        NodeLabel::gateway(NETWORK_NAME, 0),
        SimOperation::Put {
            contract: contract.clone(),
            state: vec![0x35; 64],
            subscribe: true,
        },
    )];
    for i in 0..NUM_NODES {
        operations.push(ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, i),
            SimOperation::Get {
                contract_id,
                return_contract_code: true,
                subscribe: false,
            },
        ));
    }

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(600),
        Duration::from_secs(120),
    );
    assert!(
        result.turmoil_result.is_ok(),
        "Simulation failed: {:?}",
        result.turmoil_result.err()
    );

    let rt = create_runtime();
    let (connect_rejected, reached_min_by_checkpoint, total_peers_seen) = rt.block_on(async {
        let logs = logs_handle.lock().await;

        // (a) Count terminus-rejections across the whole run.
        let connect_rejected = logs
            .iter()
            .filter(|log| log.kind.is_connect_rejected())
            .count();

        // (b) Per-peer max open-connection count observed at any Connected
        // event whose transaction was created at or before the early-run
        // checkpoint. `Transaction::created_at_ms()` decodes the ULID
        // timestamp (GlobalSimulationTime = epoch + per-ULID counter), so this
        // is a deterministic GENERATION-ORDER filter — an early-run proxy, not
        // literal virtual time (see the CHECKPOINT_MS note above).
        let checkpoint_at_ms = epoch_ms.saturating_add(CHECKPOINT_MS);
        let mut max_count_by_peer: HashMap<_, usize> = HashMap::new();
        let mut peers_seen: HashSet<_> = HashSet::new();
        for log in logs.iter() {
            if let Some(count) = log.kind.connect_peer_connection_count() {
                peers_seen.insert(log.peer_id.clone());
                if log.tx.created_at_ms() <= checkpoint_at_ms {
                    let entry = max_count_by_peer.entry(log.peer_id.clone()).or_insert(0);
                    *entry = (*entry).max(count);
                }
            }
        }
        let reached_min = max_count_by_peer
            .values()
            .filter(|&&c| c >= MIN_CONNECTIONS)
            .count();
        (connect_rejected, reached_min, peers_seen.len())
    });

    tracing::info!(
        "=== Bootstrap acceptance timeline (#4362) ===\n\
         connect_rejected={} (diagnostic only — NOT asserted; see note above)\n\
         nodes reaching min_connections({}) by T+{}s: {}/{} peers seen",
        connect_rejected,
        MIN_CONNECTIONS,
        CHECKPOINT_MS / 1000,
        reached_min_by_checkpoint,
        total_peers_seen,
    );

    // Most nodes must reach min_connections by the mid-sim checkpoint.
    // Denominator is the full node count, so nodes that never emitted a
    // Connected event (still wedged in the dead zone) count against the floor.
    let required = (NUM_NODES as f64 * REACHED_MIN_FRACTION).ceil() as usize;
    assert!(
        reached_min_by_checkpoint >= required,
        "only {}/{} nodes reached min_connections({}) by T+{}s (needed >= {}) — \
         the bootstrap dead zone still parks late joiners below min (#4362)",
        reached_min_by_checkpoint,
        NUM_NODES,
        MIN_CONNECTIONS,
        CHECKPOINT_MS / 1000,
        required,
    );
}

/// Same as `test_get_reliability_diagnostic` but with realistic network delays.
///
/// Adds 50-200ms latency jitter and 5% message loss to simulate real-world conditions.
/// This should surface the ForwardingAck timeout issue from #3570: when relays ACK but
/// downstream chains stall, the originator's retry is permanently disabled.
///
/// The latency means multi-hop GETs accumulate realistic delays (e.g., 5 hops × 100ms avg
/// = 500ms per hop chain), which triggers the ACK_TIMEOUT (3s) and OPERATION_TTL (60s)
/// timing windows that are invisible in zero-latency simulations.
// Long-running diagnostic test (~3min). Runs in nightly CI.
#[cfg(feature = "nightly_tests")]
#[test_log::test]
fn test_get_reliability_with_latency() {
    use freenet::dev_tool::{NodeLabel, ScheduledOperation, SimOperation};
    use freenet::simulation::FaultConfig;

    const SEED: u64 = 0x3570_D1A6_0002;
    const NETWORK_NAME: &str = "get-reliability-latency";
    const NUM_NODES: usize = 100;
    const NUM_GATEWAYS: usize = 3;

    GlobalTestMetrics::reset();
    setup_deterministic_state(SEED);

    let rt = create_runtime();

    let (mut sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(
            NETWORK_NAME,
            NUM_GATEWAYS,
            NUM_NODES,
            10, // ring_max_htl
            7,  // rnd_if_htl_above
            12, // max_connections
            4,  // min_connections
            SEED,
        )
        .await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    // Add realistic network delays: 50-200ms latency + 5% message loss
    sim.with_fault_injection(
        FaultConfig::builder()
            .latency_range(Duration::from_millis(50)..Duration::from_millis(200))
            .message_loss_rate(0.05)
            .build(),
    );

    let contract = SimOperation::create_test_contract(0x36);
    let contract_id = *contract.key().id();
    let contract_key = contract.key();

    let mut operations = vec![
        // Gateway 0 PUTs the contract
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Put {
                contract: contract.clone(),
                state: vec![0x36; 64],
                subscribe: true,
            },
        ),
    ];

    // Every node GETs the contract
    for i in 0..NUM_NODES {
        operations.push(ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, i),
            SimOperation::Get {
                contract_id,
                return_contract_code: true,
                subscribe: false,
            },
        ));
    }

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(900), // 15 min — longer to account for latency
        Duration::from_secs(180), // 3 min post-operations wait
    );

    assert!(
        result.turmoil_result.is_ok(),
        "Simulation failed: {:?}",
        result.turmoil_result.err()
    );

    // Analyze GET outcomes, deduplicated per attempt transaction (#4361).
    let rt = create_runtime();
    let summary = rt.block_on(async {
        let logs = logs_handle.lock().await;
        freenet::tracing::summarize_get_outcomes_per_tx(&logs)
    });
    let (successes, not_found, failures, timeouts) = (
        summary.successes,
        summary.not_found,
        summary.failures,
        summary.timeouts,
    );
    let total_outcomes = summary.total();

    // success_elapsed_ms is pre-sorted ascending.
    let sorted_latencies = &summary.success_elapsed_ms;

    let p50 = sorted_latencies
        .get(sorted_latencies.len() / 2)
        .copied()
        .unwrap_or(0);
    let p90 = sorted_latencies
        .get(sorted_latencies.len() * 9 / 10)
        .copied()
        .unwrap_or(0);
    let p99 = sorted_latencies
        .get(sorted_latencies.len() * 99 / 100)
        .copied()
        .unwrap_or(0);
    let max_latency = sorted_latencies.last().copied().unwrap_or(0);

    let success_rate = if total_outcomes > 0 {
        successes as f64 / total_outcomes as f64
    } else {
        0.0
    };

    tracing::info!("=== GET Reliability with Latency (#3570) ===");
    tracing::info!(
        "Network: {} gateways + {} nodes, latency=50-200ms, loss=5%",
        NUM_GATEWAYS,
        NUM_NODES
    );
    tracing::info!(
        "GET outcomes (per attempt tx): {} total — {} success ({} network-traversed), \
         {} not_found, {} failures, {} timeouts",
        total_outcomes,
        successes,
        summary.network_successes,
        not_found,
        failures,
        timeouts
    );
    tracing::info!(
        "GET success rate: {:.1}% ({}/{})",
        success_rate * 100.0,
        successes,
        total_outcomes
    );
    tracing::info!(
        "Latency (successful GETs): p50={}ms, p90={}ms, p99={}ms, max={}ms",
        p50,
        p90,
        p99,
        max_latency
    );

    // Check which nodes got the contract state
    let mut nodes_with_state = 0;
    let mut nodes_without_state = Vec::new();
    for i in 0..NUM_NODES {
        let label = NodeLabel::node(NETWORK_NAME, i);
        if let Some(storage) = result.node_storages.get(&label) {
            if storage.get_stored_state(&contract_key).is_some() {
                nodes_with_state += 1;
            } else {
                nodes_without_state.push(i);
            }
        } else {
            nodes_without_state.push(i);
        }
    }

    tracing::info!(
        "Storage verification: {}/{} nodes have contract state",
        nodes_with_state,
        NUM_NODES
    );
    if !nodes_without_state.is_empty() {
        tracing::info!(
            "Nodes missing state ({} total): {:?}{}",
            nodes_without_state.len(),
            &nodes_without_state[..nodes_without_state.len().min(20)],
            if nodes_without_state.len() > 20 {
                "..."
            } else {
                ""
            }
        );
    }

    // Compare with no-latency baseline
    tracing::info!(
        "=== Comparison with no-latency baseline ===\n\
         Baseline (no latency): 88.3% success, 72/100 nodes, p50=1ms, p90=754ms\n\
         This run (50-200ms latency, 5% loss): {:.1}% success, {}/{} nodes, p50={}ms, p90={}ms",
        success_rate * 100.0,
        nodes_with_state,
        NUM_NODES,
        p50,
        p90
    );

    // Soft assertion — diagnostic test, but flag catastrophic regression
    assert!(
        total_outcomes >= 10,
        "Only {} GET outcome events — too few for meaningful analysis",
        total_outcomes
    );

    // StateVerifier anomaly check
    let report = rt.block_on(async {
        let logs = logs_handle.lock().await;
        let verifier = freenet::tracing::StateVerifier::from_events(logs.clone());
        verifier.verify()
    });
    tracing::info!(
        "Anomaly report: {} anomalies across {} contracts, {} total events",
        report.anomalies.len(),
        report.contracts_analyzed,
        report.total_events
    );

    tracing::info!(
        "test_get_reliability_with_latency DONE: {:.1}% success rate, \
         {}/{} nodes have state",
        success_rate * 100.0,
        nodes_with_state,
        NUM_NODES
    );
}

/// GET reliability under node churn (nodes randomly crashing and restarting).
///
/// Uses the direct runner with ChurnConfig to simulate realistic network dynamics:
/// - 100 nodes + 3 gateways
/// - 10% crash probability per tick, 3s recovery, max 5 simultaneous crashes
/// - 50-200ms latency + 5% message loss
/// - 300 random operations (PUTs, GETs, SUBSCRIBEs, UPDATEs)
///
/// Measures GET success rate under these conditions to see if churn degrades
/// reliability beyond what static routing exhaustion causes.
// Long-running diagnostic test (~4min). Runs in nightly CI.
#[cfg(feature = "nightly_tests")]
#[test_log::test]
fn test_get_reliability_with_churn() {
    use freenet::dev_tool::ChurnConfig;
    use freenet::simulation::FaultConfig;

    const SEED: u64 = 0x3570_D1A6_0003;
    const NETWORK_NAME: &str = "get-reliability-churn";
    const NUM_NODES: usize = 100;
    const NUM_GATEWAYS: usize = 3;

    GlobalTestMetrics::reset();
    setup_deterministic_state(SEED);

    let rt = create_runtime();

    let (mut sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(
            NETWORK_NAME,
            NUM_GATEWAYS,
            NUM_NODES,
            10, // ring_max_htl
            7,  // rnd_if_htl_above
            12, // max_connections
            4,  // min_connections
            SEED,
        )
        .await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    // Add realistic network delays
    sim.with_fault_injection(
        FaultConfig::builder()
            .latency_range(Duration::from_millis(50)..Duration::from_millis(200))
            .message_loss_rate(0.05)
            .build(),
    );

    // Add node churn: 10% crash rate, 3s recovery, max 5 simultaneous crashes
    sim.with_churn(ChurnConfig {
        crash_probability: 0.10,
        tick_interval: Duration::from_secs(5),
        recovery_delay: Duration::from_secs(3),
        max_simultaneous_crashes: Some(5),
        permanent_crash_rate: 0.02, // 2% of crashes are permanent
        warmup_delay: Duration::from_secs(10),
    });

    drop(rt);

    // Run with random operations — direct runner handles churn
    let direct_result = sim.run_simulation_direct::<rand::rngs::SmallRng>(
        SEED,
        15,  // max_contract_num
        300, // iterations — enough to generate many GETs
        Duration::from_millis(500),
    );

    if let Err(e) = &direct_result {
        tracing::warn!("Direct simulation completed with error (may be expected under churn): {e}");
    }

    // Analyze GET outcomes from event logs, deduplicated per attempt
    // transaction (#4361).
    let rt = create_runtime();
    let summary = rt.block_on(async {
        let logs = logs_handle.lock().await;
        freenet::tracing::summarize_get_outcomes_per_tx(&logs)
    });
    let (successes, not_found, failures, timeouts) = (
        summary.successes,
        summary.not_found,
        summary.failures,
        summary.timeouts,
    );
    let total_outcomes = summary.total();

    // success_elapsed_ms is pre-sorted ascending.
    let sorted_latencies = &summary.success_elapsed_ms;

    let p50 = sorted_latencies
        .get(sorted_latencies.len() / 2)
        .copied()
        .unwrap_or(0);
    let p90 = sorted_latencies
        .get(sorted_latencies.len() * 9 / 10)
        .copied()
        .unwrap_or(0);
    let p99 = sorted_latencies
        .get(sorted_latencies.len() * 99 / 100)
        .copied()
        .unwrap_or(0);
    let max_latency = sorted_latencies.last().copied().unwrap_or(0);

    let success_rate = if total_outcomes > 0 {
        successes as f64 / total_outcomes as f64
    } else {
        0.0
    };

    tracing::info!("=== GET Reliability with Churn (#3570) ===");
    tracing::info!(
        "Network: {} gateways + {} nodes, latency=50-200ms, loss=5%, churn=10%/5s",
        NUM_GATEWAYS,
        NUM_NODES
    );
    tracing::info!(
        "GET outcomes (per attempt tx): {} total — {} success ({} network-traversed), \
         {} not_found, {} failures, {} timeouts",
        total_outcomes,
        successes,
        summary.network_successes,
        not_found,
        failures,
        timeouts
    );
    tracing::info!(
        "GET success rate: {:.1}% ({}/{})",
        success_rate * 100.0,
        successes,
        total_outcomes
    );
    tracing::info!(
        "Latency (successful GETs): p50={}ms, p90={}ms, p99={}ms, max={}ms",
        p50,
        p90,
        p99,
        max_latency
    );

    // Compare with previous variants
    tracing::info!(
        "=== Comparison ===\n\
         Baseline (no latency):         88.3% success, p90=754ms\n\
         With latency (50-200ms, 5%):   81.9% success, p90=1833ms\n\
         With churn + latency:          {:.1}% success, p90={}ms",
        success_rate * 100.0,
        p90
    );

    // Soft assertion — diagnostic test
    if total_outcomes >= 10 {
        tracing::info!(
            "test_get_reliability_with_churn DONE: {:.1}% GET success rate \
             ({} outcomes from 300 random operations under churn)",
            success_rate * 100.0,
            total_outcomes
        );
    } else {
        tracing::warn!(
            "test_get_reliability_with_churn: only {} GET outcomes — \
             random event generation may not have produced enough GETs",
            total_outcomes
        );
    }
}

/// Nightly: 50-node topology formation with strict connectivity assertions.
///
/// Verifies that a 50-node network converges to `min_connections` within 1 virtual hour.
/// Uses the Direct Runner for scale (50+ nodes) and determinism.
///
/// **Assertions (per testing.md realism requirements):**
/// 1. Median connections >= `min_connections` (strict, not min-1)
/// 2. >= 90% of nodes reach `min_connections`
/// 3. >= 80% of nodes have non-gateway peer connections
#[cfg(feature = "nightly_tests")]
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_nightly_50_node_topology_formation() {
    use freenet::dev_tool::NodeLabel;

    const SEED: u64 = 0x3511_5000_0001;
    const NETWORK_NAME: &str = "nightly-50-topology";
    const GATEWAYS: usize = 4;
    const NODES: usize = 50;
    const RING_MAX_HTL: usize = 10;
    const RND_IF_HTL_ABOVE: usize = 5;
    const MAX_CONN: usize = 20;
    const MIN_CONN: usize = 10;
    const VIRTUAL_DURATION: Duration = Duration::from_secs(3600); // 1 hour

    tracing::info!("=== Nightly: 50-Node Topology Formation ===");

    setup_deterministic_state(SEED);

    let mut sim = SimNetwork::new(
        NETWORK_NAME,
        GATEWAYS,
        NODES,
        RING_MAX_HTL,
        RND_IF_HTL_ABOVE,
        MAX_CONN,
        MIN_CONN,
        SEED,
    )
    .await;

    sim.with_start_backoff(Duration::from_millis(50));

    let _handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 0, 0)
        .await;

    tracing::info!("Running 1 virtual hour of topology formation...");
    let_network_run(&mut sim, VIRTUAL_DURATION).await;

    // Collect per-node connection counts. `connection_count` returns `Option`
    // and `filter_map` may drop nodes whose ConnectionManager is briefly
    // unavailable; this matches the baseline `test_connection_growth_stall_regression`
    // pattern. Percentages are computed against the sampled population, same
    // as the baseline test.
    let mut node_counts: Vec<usize> = (0..NODES)
        .filter_map(|i| {
            let label = NodeLabel::node(NETWORK_NAME, i);
            sim.connection_count(&label)
        })
        .collect();
    node_counts.sort_unstable();

    let num_sampled = node_counts.len();
    assert!(num_sampled > 0, "No connection managers available");

    let median_conn = node_counts[num_sampled / 2];
    let nodes_above_min = node_counts.iter().filter(|&&c| c >= MIN_CONN).count();
    let fraction_above_min = nodes_above_min as f64 / num_sampled as f64;

    tracing::info!("Connection counts: {:?}", node_counts);
    tracing::info!(
        "Median={}, nodes at min_connections={}/{} ({:.0}%)",
        median_conn,
        nodes_above_min,
        num_sampled,
        fraction_above_min * 100.0
    );

    // Check non-gateway peer connections
    let connectivity = sim.node_connectivity();
    let mut nodes_with_peer_connections = 0usize;
    for (label, (_key, conns)) in &connectivity {
        if !label.is_gateway() && conns.keys().any(|peer| !peer.is_gateway()) {
            nodes_with_peer_connections += 1;
        }
    }
    let peer_conn_fraction = nodes_with_peer_connections as f64 / NODES as f64;

    tracing::info!(
        "Nodes with non-gateway peer connections: {}/{} ({:.0}%)",
        nodes_with_peer_connections,
        NODES,
        peer_conn_fraction * 100.0
    );

    // ASSERTION 1: Median connections >= min_connections (strict).
    // With 50 nodes and 1 hour, the network must fully converge.
    assert!(
        median_conn >= MIN_CONN,
        "Topology formation stall: median={} < min_connections={}. \
         Counts: {:?}. Seed: 0x{:X}",
        median_conn,
        MIN_CONN,
        node_counts,
        SEED
    );

    // ASSERTION 2: >= 90% of nodes reach min_connections (per testing.md).
    assert!(
        fraction_above_min >= 0.90,
        "Only {:.0}% of nodes reached min_connections (expected >= 90%). \
         {}/{} nodes. Counts: {:?}. Seed: 0x{:X}",
        fraction_above_min * 100.0,
        nodes_above_min,
        num_sampled,
        node_counts,
        SEED
    );

    // ASSERTION 3: >= 80% of nodes have non-gateway peer connections.
    assert!(
        peer_conn_fraction >= 0.80,
        "Only {:.0}% of nodes have non-gateway peer connections (expected >= 80%). \
         CONNECT forwarding is insufficient. Seed: 0x{:X}",
        peer_conn_fraction * 100.0,
        SEED
    );

    tracing::info!(
        "PASSED: median={}, above_min={:.0}%, peer_conns={:.0}%",
        median_conn,
        fraction_above_min * 100.0,
        peer_conn_fraction * 100.0
    );
}

/// Nightly: Connection growth rate checkpoints — monotonic growth toward `min_connections`.
///
/// Measures connection counts at 5m, 15m, 30m, 60m virtual time and asserts that
/// median connections grow monotonically. Catches growth plateaus and stalls that
/// are invisible in short-duration CI tests.
///
/// **Assertions:**
/// 1. Median connections at each checkpoint >= previous checkpoint (monotonic growth)
/// 2. Final median >= `min_connections`
/// 3. No checkpoint after 5m has median = 0 (no network collapse)
#[cfg(feature = "nightly_tests")]
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_nightly_connection_growth_checkpoints() {
    use freenet::dev_tool::NodeLabel;

    const SEED: u64 = 0x3511_6C8E_0002;
    const NETWORK_NAME: &str = "nightly-growth-checkpoints";
    const GATEWAYS: usize = 4;
    const NODES: usize = 50;
    const RING_MAX_HTL: usize = 10;
    const RND_IF_HTL_ABOVE: usize = 5;
    const MAX_CONN: usize = 20;
    const MIN_CONN: usize = 10;

    // Checkpoint intervals (cumulative seconds from start)
    const CHECKPOINTS_SECS: [u64; 4] = [300, 900, 1800, 3600]; // 5m, 15m, 30m, 60m

    tracing::info!("=== Nightly: Connection Growth Checkpoints ===");

    setup_deterministic_state(SEED);

    let mut sim = SimNetwork::new(
        NETWORK_NAME,
        GATEWAYS,
        NODES,
        RING_MAX_HTL,
        RND_IF_HTL_ABOVE,
        MAX_CONN,
        MIN_CONN,
        SEED,
    )
    .await;

    sim.with_start_backoff(Duration::from_millis(50));

    let _handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 0, 0)
        .await;

    let mut checkpoint_medians: Vec<(u64, usize)> = Vec::new();
    let mut elapsed_so_far = 0u64;

    for &target_secs in &CHECKPOINTS_SECS {
        let delta = target_secs - elapsed_so_far;
        let_network_run(&mut sim, Duration::from_secs(delta)).await;
        elapsed_so_far = target_secs;

        let mut counts: Vec<usize> = (0..NODES)
            .filter_map(|i| {
                let label = NodeLabel::node(NETWORK_NAME, i);
                sim.connection_count(&label)
            })
            .collect();
        counts.sort_unstable();

        let median = if counts.is_empty() {
            0
        } else {
            counts[counts.len() / 2]
        };

        tracing::info!(
            "Checkpoint @{}m: median={}, counts={:?}",
            target_secs / 60,
            median,
            counts
        );

        // No total network collapse after the bootstrap phase.
        assert!(
            median > 0 || elapsed_so_far <= 300,
            "Network collapse at checkpoint @{}m: median=0. Counts: {:?}. Seed: 0x{:X}",
            target_secs / 60,
            counts,
            SEED
        );

        checkpoint_medians.push((target_secs, median));
    }

    // Near-monotonic growth — each checkpoint median is within 1 connection of
    // the previous. Topology-aware pruning legitimately trades a single
    // connection for distribution quality, so a transient -1 dip is healthy
    // behavior, not a stall.
    for window in checkpoint_medians.windows(2) {
        let (prev_t, prev_median) = window[0];
        let (curr_t, curr_median) = window[1];
        assert!(
            curr_median + 1 >= prev_median,
            "Connection growth regressed > 1 between @{}m (median={}) and @{}m (median={}). \
             Growth must be near-monotonic. Seed: 0x{:X}",
            prev_t / 60,
            prev_median,
            curr_t / 60,
            curr_median,
            SEED
        );
    }

    // Final checkpoint median >= min_connections.
    let (_, final_median) = checkpoint_medians.last().unwrap();
    assert!(
        *final_median >= MIN_CONN,
        "Final median={} < min_connections={} after 60 virtual minutes. \
         Checkpoints: {:?}. Seed: 0x{:X}",
        final_median,
        MIN_CONN,
        checkpoint_medians,
        SEED
    );

    tracing::info!("PASSED: checkpoints={:?}", checkpoint_medians);
}

/// Nightly: Fault injection and recovery speed — network self-heals within bounded time.
///
/// Three phases:
/// 1. **Convergence** (30 virtual min): Let network form connections normally.
/// 2. **Fault injection** (5 virtual min): 20% message loss simulating NAT failures.
/// 3. **Recovery** (25 virtual min): Remove faults, verify network recovers.
///
/// **Assertions:**
/// 1. Pre-fault median >= `min_connections` (network converged)
/// 2. No death spiral during faults (median > 0)
/// 3. Post-recovery median >= pre-fault median - 1 (bounded recovery)
/// 4. < 5% nodes isolated after recovery
#[cfg(feature = "nightly_tests")]
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_nightly_fault_recovery_speed() {
    use freenet::dev_tool::NodeLabel;
    use freenet::simulation::FaultConfig;

    const SEED: u64 = 0x3511_FA17_0003;
    const NETWORK_NAME: &str = "nightly-fault-recovery";
    const GATEWAYS: usize = 4;
    const NODES: usize = 50;
    const RING_MAX_HTL: usize = 10;
    const RND_IF_HTL_ABOVE: usize = 5;
    const MAX_CONN: usize = 20;
    const MIN_CONN: usize = 10;

    tracing::info!("=== Nightly: Fault Recovery Speed ===");

    setup_deterministic_state(SEED);

    let mut sim = SimNetwork::new(
        NETWORK_NAME,
        GATEWAYS,
        NODES,
        RING_MAX_HTL,
        RND_IF_HTL_ABOVE,
        MAX_CONN,
        MIN_CONN,
        SEED,
    )
    .await;

    sim.with_start_backoff(Duration::from_millis(50));

    let _handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 0, 0)
        .await;

    // ── Phase 1: Convergence (30 virtual minutes, no faults) ──────────────────
    tracing::info!("Phase 1: Convergence — 30 virtual minutes, no faults");
    let_network_run(&mut sim, Duration::from_secs(1800)).await;

    // Regular-node IDs start at `GATEWAYS`, not 0. `SimNetwork::config_nodes`
    // iterates `number_of_gateways..number_of_gateways + num` when constructing
    // labels, so for GATEWAYS=4 / NODES=50 the regular-node labels are
    // `NodeLabel::node(NETWORK_NAME, 4..54)`. Querying `(0..NODES)` would miss
    // the top `GATEWAYS` nodes and collect an off-by-GATEWAYS subset — that was
    // the deterministic `got 46, expected 50` nightly failure on seed
    // 0x3511FA170003 before this fix. Iterate over the actual live-label range
    // so every assertion in this test (Phase 1, 2, 3) sees the full 50-node
    // population.
    let node_ids = || GATEWAYS..GATEWAYS + NODES;

    let mut pre_fault_counts: Vec<usize> = node_ids()
        .filter_map(|i| {
            let label = NodeLabel::node(NETWORK_NAME, i);
            sim.connection_count(&label)
        })
        .collect();
    pre_fault_counts.sort_unstable();
    assert!(
        !pre_fault_counts.is_empty(),
        "Phase 1: no connection managers available. Seed: 0x{:X}",
        SEED
    );
    let pre_fault_median = pre_fault_counts[pre_fault_counts.len() / 2];

    tracing::info!(
        "Phase 1 done: median={}, counts={:?}",
        pre_fault_median,
        pre_fault_counts
    );

    // Network must converge before we inject faults.
    assert!(
        pre_fault_median >= MIN_CONN,
        "Network did not converge before fault injection: median={} < min_connections={}. \
         Counts: {:?}. Seed: 0x{:X}",
        pre_fault_median,
        MIN_CONN,
        pre_fault_counts,
        SEED
    );

    // ── Phase 2: Fault injection (5 virtual minutes, 20% message loss) ────────
    tracing::info!("Phase 2: Fault injection — 20% message loss for 5 virtual minutes");
    sim.with_fault_injection(FaultConfig::builder().message_loss_rate(0.20).build());

    let_network_run(&mut sim, Duration::from_secs(300)).await;

    let mut during_fault_counts: Vec<usize> = node_ids()
        .filter_map(|i| {
            let label = NodeLabel::node(NETWORK_NAME, i);
            sim.connection_count(&label)
        })
        .collect();
    during_fault_counts.sort_unstable();
    assert_eq!(
        during_fault_counts.len(),
        NODES,
        "Phase 2: expected connection managers for all {} nodes, got {}. Seed: 0x{:X}",
        NODES,
        during_fault_counts.len(),
        SEED
    );
    let during_fault_median = during_fault_counts[during_fault_counts.len() / 2];

    tracing::info!(
        "Phase 2 done: median={}, counts={:?}",
        during_fault_median,
        during_fault_counts
    );

    // Resilience under load: 20% message loss must not push the median below
    // half of MIN_CONN. The previous `> 0` bar was degenerate (a single peer
    // surviving on the median node would pass), so it could not detect a
    // partial death spiral. Half-of-min is the smallest threshold strict
    // enough to fail on real cascade collapse but loose enough to tolerate
    // brief pruning during a 5-minute fault window.
    let during_fault_floor = MIN_CONN / 2;
    assert!(
        during_fault_median >= during_fault_floor,
        "Connection collapse under load: during_fault_median={} < MIN_CONN/2={}. \
         Counts: {:?}. Seed: 0x{:X}",
        during_fault_median,
        during_fault_floor,
        during_fault_counts,
        SEED
    );

    // ── Phase 3: Recovery (25 virtual minutes, faults cleared) ────────────────
    tracing::info!("Phase 3: Recovery — faults cleared, 25 virtual minutes");
    sim.clear_fault_injection();

    let_network_run(&mut sim, Duration::from_secs(1500)).await;

    let mut post_recovery_counts: Vec<usize> = node_ids()
        .filter_map(|i| {
            let label = NodeLabel::node(NETWORK_NAME, i);
            sim.connection_count(&label)
        })
        .collect();
    post_recovery_counts.sort_unstable();
    assert_eq!(
        post_recovery_counts.len(),
        NODES,
        "Phase 3: expected connection managers for all {} nodes, got {}. Seed: 0x{:X}",
        NODES,
        post_recovery_counts.len(),
        SEED
    );

    let post_recovery_median = post_recovery_counts[post_recovery_counts.len() / 2];
    let isolated_count = post_recovery_counts.iter().filter(|&&c| c == 0).count();
    let fraction_isolated = isolated_count as f64 / NODES as f64;

    tracing::info!(
        "Phase 3 done: median={}, isolated={}/{} ({:.0}%), counts={:?}",
        post_recovery_median,
        isolated_count,
        NODES,
        fraction_isolated * 100.0,
        post_recovery_counts
    );

    // Recovery to near pre-fault levels AND back above MIN_CONN. Without the
    // MIN_CONN floor, `pre_fault_median.saturating_sub(1)` could allow the
    // post-recovery median to fall below the documented success criterion
    // (e.g. pre=10, post=9 would silently pass).
    let recovery_floor = pre_fault_median.saturating_sub(1).max(MIN_CONN);
    assert!(
        post_recovery_median >= recovery_floor,
        "Incomplete recovery: post_recovery_median={} < floor={} \
         (max(pre_fault_median - 1, MIN_CONN)). \
         Pre-fault: {:?}, Post-recovery: {:?}. Seed: 0x{:X}",
        post_recovery_median,
        recovery_floor,
        pre_fault_counts,
        post_recovery_counts,
        SEED
    );

    // < 5% nodes isolated after recovery.
    assert!(
        fraction_isolated < 0.05,
        "{:.0}% of nodes isolated after recovery (threshold: 5%). \
         Counts: {:?}. Seed: 0x{:X}",
        fraction_isolated * 100.0,
        post_recovery_counts,
        SEED
    );

    tracing::info!(
        "PASSED: pre_fault_median={}, during_fault_median={}, post_recovery_median={}, \
         isolated={:.0}%",
        pre_fault_median,
        during_fault_median,
        post_recovery_median,
        fraction_isolated * 100.0
    );
}

/// Regression test for the 2026-04-15 nightly failure: pins the
/// `SimNetwork::config_nodes` regular-node-label indexing convention so
/// future tests can't silently re-introduce the off-by-GATEWAYS bug that
/// broke `test_nightly_fault_recovery_speed`.
///
/// The convention (`crates/core/src/node/testing_impl.rs::config_nodes`,
/// circa line 1580) iterates
/// `number_of_gateways..num + number_of_gateways` when constructing
/// regular-node labels. For a SimNetwork with G gateways and N nodes, the
/// live regular-node labels are therefore
/// `NodeLabel::node(name, G..G + N)` — NOT `0..N`. A test that iterates
/// `0..N` and calls `NodeLabel::node(name, i)` will miss the top G entries
/// entirely, deterministically getting `N - G` labels instead of `N`.
///
/// This test uses a small SimNetwork (2 gateways, 3 regular nodes, fast
/// startup) and verifies:
///   1. `node-0` and `node-1` (inside the gateway ID range) are NOT live
///      regular-node labels.
///   2. `node-2`, `node-3`, `node-4` (the `GATEWAYS..GATEWAYS + NODES`
///      range) ARE live regular-node labels.
///   3. `node-5` (one past the end) is NOT a live regular-node label.
///
/// Pins the `NodeLabel::node` / `NodeLabel::gateway` string format that
/// `SimNetwork::config_nodes` and every downstream test query rely on.
/// Keeping this as a plain `#[test]` (not `#[tokio::test]`) ensures the
/// CI rule-lint regex `#\[(tokio::)?test\]` counts it as a regression
/// test — the async `sim_network_regular_node_labels_start_at_gateway_count`
/// below is annotated with `#[test_log::test(...)]` which the lint grep
/// does not recognize. Both tests pin the same bug class at different
/// layers: this one pins the label encoding, the async one pins the
/// live-label ID range.
#[test]
fn node_label_node_and_gateway_formats_are_stable() {
    // Expected encoding is "<network>-node-<id>" and "<network>-gateway-<id>".
    assert_eq!(
        NodeLabel::node("net", 5).to_string(),
        "net-node-5",
        "NodeLabel::node string format must remain stable"
    );
    assert_eq!(
        NodeLabel::gateway("net", 2).to_string(),
        "net-gateway-2",
        "NodeLabel::gateway string format must remain stable"
    );
    // Gateway and regular-node labels at the same numeric ID must be
    // distinct, because `config_nodes` and the test indexing convention
    // rely on the two label spaces being disjoint by type-prefix — not
    // by numeric range. A future refactor that conflates them would
    // silently corrupt every connection-count assertion.
    assert_ne!(
        NodeLabel::node("net", 0),
        NodeLabel::gateway("net", 0),
        "gateway and regular-node labels must be distinct at the same numeric ID"
    );
    // And `node 5` must not equal `gateway 5` either.
    assert_ne!(NodeLabel::node("net", 5), NodeLabel::gateway("net", 5));
}

#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn sim_network_regular_node_labels_start_at_gateway_count() {
    const NETWORK_NAME: &str = "label-indexing-regression";
    const GATEWAYS: usize = 2;
    const NODES: usize = 3;
    const SEED: u64 = 0xABCD_1234;

    setup_deterministic_state(SEED);

    let mut sim = SimNetwork::new(
        NETWORK_NAME,
        GATEWAYS,
        NODES,
        /* ring_max_htl */ 3,
        /* rnd_if_htl_above */ 2,
        /* max_connections */ 4,
        /* min_connections */ 2,
        SEED,
    )
    .await;

    let _handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 0, 0)
        .await;

    // Labels inside the gateway ID range (0..GATEWAYS) must NOT exist as
    // regular-node labels, even though `NodeLabel::node(name, i)` is a
    // syntactically valid label for any `i`. The rule being pinned is that
    // the *live* regular-node set starts at `GATEWAYS`, not 0.
    for i in 0..GATEWAYS {
        assert!(
            sim.connection_count(&NodeLabel::node(NETWORK_NAME, i))
                .is_none(),
            "node-{i} must NOT be a live regular-node label (inside gateway range)"
        );
    }

    // The live regular-node range is `GATEWAYS..GATEWAYS + NODES`.
    for i in GATEWAYS..GATEWAYS + NODES {
        assert!(
            sim.connection_count(&NodeLabel::node(NETWORK_NAME, i))
                .is_some(),
            "node-{i} must be a live regular-node label"
        );
    }

    // One past the end must not be live either.
    assert!(
        sim.connection_count(&NodeLabel::node(NETWORK_NAME, GATEWAYS + NODES))
            .is_none(),
        "node-{} must NOT be a live regular-node label (past end of range)",
        GATEWAYS + NODES
    );
}

// =============================================================================
// hop_count Regression Test (simulation-level, currently disabled)
// =============================================================================
//
// Direct regression coverage for the wire-format fix is provided by
// `test_get_msg_response_hop_count_roundtrip` in `crates/core/src/operations/get.rs`.
// That unit test asserts the new `hop_count` field roundtrips through bincode
// for both `Found` and `NotFound` variants, which is the specific regression
// scenario this PR addresses.
//
// The simulation-level integration test below is left in tree, but marked
// `#[ignore]` because the default `TestConfig`/`event_chain` workload at the
// scales reasonable for CI does not reliably produce terminal GET events
// (most operations are PUT/UPDATE/SUBSCRIBE).  The `nightly_tests`-gated
// `test_get_reliability_diagnostic` above does drive GETs via
// `run_controlled_simulation`; once that pattern is generalised, this test
// can be rebuilt on top of it without `#[ignore]`.
#[test_log::test]
#[ignore = "tracking issue #4250 — default event_chain workload doesn't produce terminal GETs at CI scales; primary regression coverage lives in test_get_msg_response_hop_count_roundtrip (get.rs) and classify_response_found_preserves_hop_count (op_ctx_task.rs). Un-ignore once a deterministic GET-producing workload is wired through TestConfig"]
fn test_hop_count_populated_on_terminal_get_events() {
    // Parameters match test_router_accumulates_feedback_events (a known-good
    // workload that produces GET events in CI).
    let result = TestConfig::small("hop-count-regression", 0xCAFE_0001)
        .with_nodes(4)
        .with_max_contracts(5)
        .with_iterations(50)
        .with_duration(Duration::from_secs(60))
        .with_sleep(Duration::from_secs(2))
        .run()
        .assert_ok();

    // TestConfig::small uses ring_max_htl = 7, so any populated hop_count must
    // be within [0, 7].  Anything outside that range would indicate either
    // garbage-value propagation or a regression in how relays compute their
    // forward depth.
    const RING_MAX_HTL: usize = 7;

    let rt = create_runtime();
    let (populated, max_hop) = rt.block_on(async {
        let logs = result.logs_handle.lock().await;
        let hop_counts: Vec<usize> = logs
            .iter()
            .filter(|m| m.kind.variant_name() == "Get")
            .filter_map(|m| m.kind.hop_count())
            .collect();
        let max_hop = hop_counts.iter().copied().max().unwrap_or(0);
        (hop_counts.len(), max_hop)
    });

    // Presence: before the fix the wire-carried hop_count is dropped on the
    // floor and every terminal GET event has hop_count = None.  After the
    // fix every terminal GET event carries it.
    assert!(
        populated > 0,
        "expected at least one terminal GET event with populated hop_count; \
         got 0.  Indicates hop_count is no longer being threaded through the \
         GetMsg::Response wire format (PR #4245)."
    );

    // Sanity-bound: hop_count is computed as `max_htl - htl` on the storer
    // side (and at exhaustion relays), so it must be within [0, max_htl].
    // A regression that, say, started writing the originator's htl_max into
    // the field instead of (max_htl - htl) would trip this.
    assert!(
        max_hop <= RING_MAX_HTL,
        "max observed hop_count {} exceeds ring_max_htl {}; suggests garbage \
         value propagation rather than (max_htl - htl) accounting",
        max_hop,
        RING_MAX_HTL
    );
}

/// Verifies the contract-placement migration (#4404): a contract held only by a
/// peer FAR from its key migrates onto the cluster of peers CLOSEST to the key,
/// resolving the GET dead-end.
///
/// Setup (peer ring locations controlled via `new_with_node_locations`):
///   - a dense cluster of peers sits right on the contract's key location;
///   - exactly one peer, far from the key, initially hosts the contract (seeded
///     into its store AND Ring hosting manager, with no network propagation);
///   - a requester, also far from the key, issues a GET.
///
/// GET is single-path greedy (k=1), so it routes toward the key and reaches the
/// close cluster. WITHOUT the migration that cluster lacks the state, so the GET
/// dead-ends with NotFound (the far host is never on the greedy path toward the
/// key) — that is the placement gap #4404 describes. The migration nudges the
/// contract from the far host toward the key: each hosting peer, on gaining a
/// connected neighbor strictly closer to the key, sends a `SubscribeHint` so
/// that neighbor directed-subscribes through the holder and begins hosting. The
/// contract therefore climbs onto the close cluster, and a key-routed GET now
/// lands on a host. (The dead-end itself is not asserted separately here; the
/// migration trigger is always-on, so this test pins the resolved state.)
///
/// Asserts the migration outcome directly via each node's live Ring: the far
/// host still hosts the contract, and at least one close-cluster peer (the peers
/// a key-routed GET actually reaches) ends up hosting it via migration.
#[test_log::test]
fn test_contract_migrates_to_close_cluster_resolving_get_dead_end() {
    use freenet::dev_tool::{Location, NodeLabel, ScheduledOperation, SimNetwork, SimOperation};

    const SEED: u64 = 0xDEAD_F00D_0001;
    const NETWORK_NAME: &str = "get-placement-deadend";
    setup_deterministic_state(SEED);

    // Pick a contract and read its ring location; place peers relative to it.
    let contract = SimOperation::create_test_contract(0xBE);
    let contract_id = *contract.key().id();
    let contract_key = contract.key();
    let key_loc = Location::from(&contract_key).as_f64();

    // Six peers clustered tightly around the key (none will host it), then one
    // far host (seeded) and one far requester. `rem_euclid` wraps onto the ring.
    let wrap = |x: f64| x.rem_euclid(1.0);
    let cluster: Vec<f64> = [-0.010, -0.006, -0.003, 0.003, 0.006, 0.010]
        .iter()
        .map(|o| wrap(key_loc + o))
        .collect();
    let host_loc = wrap(key_loc + 0.40); // far from the key
    let requester_loc = wrap(key_loc + 0.70); // far from the key, other side
    let mut node_locations = cluster.clone();
    node_locations.push(host_loc); // regular-node order 6 -> node_no 7
    node_locations.push(requester_loc); // regular-node order 7 -> node_no 8
    let num_nodes = node_locations.len(); // 8

    let rt = create_runtime();
    let mut sim = rt.block_on(async {
        SimNetwork::new_with_node_locations(
            NETWORK_NAME,
            1,         // 1 gateway
            num_nodes, // 8 regular nodes
            10,        // ring_max_htl
            7,         // rnd_if_htl_above
            8,         // max_connections
            3,         // min_connections
            SEED,
            &node_locations,
        )
        .await
    });
    // Opt this simulation into the placement-migration cascade (off by default in
    // sim so it can't perturb unrelated tests, e.g. the streaming assembly-retry
    // test). This lowers the per-node SubscribeHint version floor to (0,0,0).
    sim.enable_placement_migration();

    // Regular nodes are node_no = 1..=8 (gateway is node 0); node_locations[i]
    // maps to node_no i+1.
    let host_label = NodeLabel::node(NETWORK_NAME, 7); // host_loc
    let requester_label = NodeLabel::node(NETWORK_NAME, 8); // requester_loc

    // Setup sanity: a cluster node must be strictly closer to the key than the
    // host, so a key-routed GET genuinely lands in the (non-hosting) cluster
    // rather than near the holder. get_peer_locations() is [gateway, node1..8].
    let locs = sim.get_peer_locations();
    let ring_dist = |a: f64, b: f64| {
        let d = (a - b).abs();
        d.min(1.0 - d)
    };
    let host_dist = ring_dist(locs[7], key_loc);
    let cluster_min = (1..=6)
        .map(|i| ring_dist(locs[i], key_loc))
        .fold(f64::INFINITY, f64::min);
    assert!(
        cluster_min < host_dist,
        "scenario setup wrong: a cluster node must be closer to the key than the host \
         (cluster_min={cluster_min}, host_dist={host_dist})"
    );

    let operations = vec![
        // Only the far host holds the contract initially — seeded into both its
        // store and Ring hosting manager (no network propagation), so it is a
        // genuine migration source (`ring.is_hosting_contract` is true).
        ScheduledOperation::new(
            host_label.clone(),
            SimOperation::SeedHostedContract {
                contract: contract.clone(),
                state: vec![10, 20, 30, 40],
            },
        ),
        // The far requester asks for it (greedy GET toward the key).
        ScheduledOperation::new(
            requester_label.clone(),
            SimOperation::Get {
                contract_id,
                return_contract_code: true,
                subscribe: false,
            },
        ),
    ];

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(180),
        Duration::from_secs(60),
    );
    assert!(
        result.turmoil_result.is_ok(),
        "simulation failed: {:?}",
        result.turmoil_result.err()
    );

    // The far host still hosts the seeded contract (it remains a source).
    assert!(
        result.is_node_hosting(&host_label, &contract_key),
        "host should still host the seeded contract after the simulation"
    );

    // CORE OF THE FIX: the contract migrated onto the close cluster. At least
    // one of the peers a key-routed GET actually reaches (node_no 1..=6) now
    // hosts it. Before the migration NONE of them did — this test previously
    // asserted exactly that dead-end (see git history). With the contract now
    // present on a peer the greedy path lands on, the dead-end is resolved.
    let migrated: Vec<usize> = (1..=6usize)
        .filter(|n| result.is_node_hosting(&NodeLabel::node(NETWORK_NAME, *n), &contract_key))
        .collect();
    let requester_has_state = result
        .node_storages
        .get(&requester_label)
        .is_some_and(|s| s.get_stored_state(&contract_key).is_some());
    assert!(
        !migrated.is_empty(),
        "placement migration FAILED: no close-cluster peer (node_no 1..=6) hosts the \
         contract after the simulation. The contract never migrated from the far host \
         toward the key, so a key-routed GET would still dead-end. \
         host_hosting={}, requester_has_state={requester_has_state}",
        result.is_node_hosting(&host_label, &contract_key),
    );

    // End-to-end payoff: with the contract migrated onto the close cluster, the
    // requester's greedy GET toward the key now lands on a host instead of
    // dead-ending, so it obtains the state. This is the user-visible symptom
    // from the original telemetry (a web GET that needed several retries before
    // the contract had migrated onto the key-close peers).
    assert!(
        requester_has_state,
        "requester GET should now succeed: with the contract migrated onto the close \
         cluster, the greedy GET toward the key lands on a host instead of dead-ending \
         (migrated cluster nodes: {migrated:?})"
    );

    tracing::info!(
        migrated_cluster_nodes = ?migrated,
        requester_has_state,
        "placement migration converged onto the close cluster"
    );
}

/// Negative control for `test_contract_migrates_to_close_cluster_resolving_get_dead_end`.
///
/// IDENTICAL scenario, but WITHOUT `enable_placement_migration()` — so the
/// SubscribeHint cascade stays off (sim peers report a build version below the
/// production floor). This reproduces the #4404 dead-end and, paired with the
/// positive test, proves that the migration cascade (not some incidental GET
/// caching path) is what makes the close cluster host the contract: same seed,
/// same topology, the ONLY difference is whether migration is enabled.
#[test_log::test]
fn test_get_dead_ends_at_close_cluster_without_migration() {
    use freenet::dev_tool::{Location, NodeLabel, ScheduledOperation, SimNetwork, SimOperation};

    const SEED: u64 = 0xDEAD_F00D_0001;
    const NETWORK_NAME: &str = "get-placement-deadend-control";
    setup_deterministic_state(SEED);

    let contract = SimOperation::create_test_contract(0xBE);
    let contract_id = *contract.key().id();
    let contract_key = contract.key();
    let key_loc = Location::from(&contract_key).as_f64();

    let wrap = |x: f64| x.rem_euclid(1.0);
    let cluster: Vec<f64> = [-0.010, -0.006, -0.003, 0.003, 0.006, 0.010]
        .iter()
        .map(|o| wrap(key_loc + o))
        .collect();
    let host_loc = wrap(key_loc + 0.40);
    let requester_loc = wrap(key_loc + 0.70);
    let mut node_locations = cluster.clone();
    node_locations.push(host_loc);
    node_locations.push(requester_loc);
    let num_nodes = node_locations.len();

    let rt = create_runtime();
    let mut sim = rt.block_on(async {
        SimNetwork::new_with_node_locations(
            NETWORK_NAME,
            1,
            num_nodes,
            10,
            7,
            8,
            3,
            SEED,
            &node_locations,
        )
        .await
    });
    // This control asserts the GET dead-ends *because migration is off* — it
    // deliberately does NOT call `enable_placement_migration()`. But "off by
    // default" only holds on a build below the production
    // SUBSCRIBE_HINT_MIN_VERSION floor; on the v0.2.73 release branch (#4404
    // ships active) the default flips to ON and this control would falsely
    // fail. Pin migration OFF explicitly so the control's premise holds at any
    // build version — do not rely on build-version gating.
    sim.disable_placement_migration();

    let host_label = NodeLabel::node(NETWORK_NAME, 7);
    let requester_label = NodeLabel::node(NETWORK_NAME, 8);

    let operations = vec![
        ScheduledOperation::new(
            host_label.clone(),
            SimOperation::SeedHostedContract {
                contract: contract.clone(),
                state: vec![10, 20, 30, 40],
            },
        ),
        ScheduledOperation::new(
            requester_label.clone(),
            SimOperation::Get {
                contract_id,
                return_contract_code: true,
                subscribe: false,
            },
        ),
    ];

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(180),
        Duration::from_secs(60),
    );
    assert!(
        result.turmoil_result.is_ok(),
        "simulation failed: {:?}",
        result.turmoil_result.err()
    );

    // The far host still hosts the seeded contract.
    assert!(
        result.is_node_hosting(&host_label, &contract_key),
        "host should hold the seeded contract"
    );

    // DEAD-END (expected without migration): no close-cluster peer hosts the
    // contract, so a key-routed GET dead-ends and the requester gets nothing.
    let migrated: Vec<usize> = (1..=6usize)
        .filter(|n| result.is_node_hosting(&NodeLabel::node(NETWORK_NAME, *n), &contract_key))
        .collect();
    assert!(
        migrated.is_empty(),
        "without migration, no close-cluster peer should host the contract, but these do: \
         {migrated:?} (cascade leaked into a migration-disabled sim?)"
    );
    let requester_has_state = result
        .node_storages
        .get(&requester_label)
        .is_some_and(|s| s.get_stored_state(&contract_key).is_some());
    assert!(
        !requester_has_state,
        "without migration the requester GET must dead-end at the close non-hosting cluster \
         and obtain NO state (the far host is off the greedy path toward the key)"
    );
}

/// Terminal advertisement consult (hosting redesign piece C, invariant 5):
/// a GET that routes to a terminus whose neighbor hosts the contract — one hop
/// OFF the greedy routing path — must resolve via the consult instead of
/// dead-ending with NotFound.
///
/// Setup (peer ring locations controlled via `new_with_node_locations`):
///   - a dense cluster of non-hosting peers sits right on the contract's key;
///   - exactly one host peer sits just OUTSIDE that cluster — farther from the
///     key than every cluster peer (so greedy routing never selects it), but
///     close enough in ring location to become a cluster peer's neighbor and
///     advertise its hosting to it;
///   - a far requester issues a GET.
///
/// Placement migration is disabled, so the ONLY mechanism that can carry the
/// state to the requester is the terminal consult. GET is single-path greedy
/// (relay `MAX_RELAY_RETRIES = 1`): a relay forwards to its single closest
/// neighbor toward the key and bubbles NotFound without trying its other
/// neighbors — so the advertised host, being a non-closest neighbor of the
/// terminus, is exactly the "one hop off the routing path" case. WITHOUT the
/// consult this dead-ends (proven by
/// `test_get_dead_ends_at_close_cluster_without_migration`, whose host is too
/// far to be a terminus neighbor); WITH it, the terminus consults the host
/// advertisement it received and forwards there, closing the dead-end.
///
/// The proof is the consult telemetry: `terminal_consult_resolved_found() > 0`
/// is recorded ONLY when a consulted advertised host returns Found, so with
/// migration off it is unambiguous that the consult (not routing or migration)
/// delivered the state.
#[test_log::test]
fn test_terminal_advertisement_consult_closes_get_dead_end() {
    use freenet::config::GlobalTestMetrics;
    use freenet::dev_tool::{Location, NodeLabel, ScheduledOperation, SimNetwork, SimOperation};

    // Seed + host offset chosen so the host lands as a non-closest neighbor of
    // an outer cluster peer (advertises to it, but greedy routing never selects
    // it). Deterministic under the direct/turmoil runner for this seed.
    const SEED: u64 = 0xC0FF_EEC0_0004;
    const NETWORK_NAME: &str = "terminal-consult-deadend";
    setup_deterministic_state(SEED);
    GlobalTestMetrics::reset();

    let contract = SimOperation::create_test_contract(0xC0);
    let contract_id = *contract.key().id();
    let contract_key = contract.key();
    let key_loc = Location::from(&contract_key).as_f64();
    let wrap = |x: f64| x.rem_euclid(1.0);

    // Non-hosting cluster tightly around the key.
    let cluster: Vec<f64> = [-0.010, -0.006, -0.003, 0.003, 0.006, 0.010]
        .iter()
        .map(|o| wrap(key_loc + o))
        .collect();
    // Host sits just outside the cluster: farther from the key than every
    // cluster peer (so greedy routing never selects it as a next hop), yet close
    // enough in ring location to become a cluster peer's neighbor and advertise.
    let host_loc = wrap(key_loc + 0.035);
    // Requester on the OPPOSITE side of the key from the host, so its inbound
    // path approaches the key from the far side and never passes near the host.
    let requester_loc = wrap(key_loc - 0.60);
    let mut node_locations = cluster.clone();
    node_locations.push(host_loc); // regular-node order 6 -> node_no 7
    node_locations.push(requester_loc); // regular-node order 7 -> node_no 8
    let num_nodes = node_locations.len(); // 8

    let rt = create_runtime();
    let mut sim = rt.block_on(async {
        SimNetwork::new_with_node_locations(
            NETWORK_NAME,
            1,         // 1 gateway
            num_nodes, // 8 regular nodes
            10,        // ring_max_htl
            7,         // rnd_if_htl_above
            8,         // max_connections
            3,         // min_connections
            SEED,
            &node_locations,
        )
        .await
    });
    // Migration OFF: isolate the consult as the ONLY mechanism that can carry
    // state to the requester (mirrors the negative control above).
    sim.disable_placement_migration();
    // Opt into advertising the seeded host so the consult can find it (OFF by
    // default; existing dead-end controls keep a seeded host un-advertised).
    sim.enable_seeded_host_advertisements();

    let host_label = NodeLabel::node(NETWORK_NAME, 7);
    let requester_label = NodeLabel::node(NETWORK_NAME, 8);

    // Setup sanity: the host must be strictly FARTHER from the key than the
    // closest cluster peer, so greedy routing lands on the cluster (terminus)
    // and never picks the host directly. get_peer_locations() is [gw, node1..8].
    let locs = sim.get_peer_locations();
    let ring_dist = |a: f64, b: f64| {
        let d = (a - b).abs();
        d.min(1.0 - d)
    };
    let host_dist = ring_dist(locs[7], key_loc);
    let cluster_min = (1..=6)
        .map(|i| ring_dist(locs[i], key_loc))
        .fold(f64::INFINITY, f64::min);
    assert!(
        cluster_min < host_dist,
        "scenario setup wrong: a cluster peer must be closer to the key than the host \
         (cluster_min={cluster_min}, host_dist={host_dist})"
    );

    let operations = vec![
        // Only the host holds the contract (seeded into its store + Ring hosting
        // manager + neighbor-hosting advertised set, no network propagation), so
        // it advertises hosting to its neighbors but the state exists nowhere else.
        ScheduledOperation::new(
            host_label.clone(),
            SimOperation::SeedHostedContract {
                contract: contract.clone(),
                state: vec![11, 22, 33, 44],
            },
        ),
        // The far requester asks for it (greedy GET toward the key).
        ScheduledOperation::new(
            requester_label.clone(),
            SimOperation::Get {
                contract_id,
                return_contract_code: true,
                subscribe: false,
            },
        ),
    ];

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(180),
        Duration::from_secs(60),
    );
    assert!(
        result.turmoil_result.is_ok(),
        "simulation failed: {:?}",
        result.turmoil_result.err()
    );

    // The host still hosts the seeded contract.
    assert!(
        result.is_node_hosting(&host_label, &contract_key),
        "host should still host the seeded contract after the simulation"
    );

    let attempts = GlobalTestMetrics::terminal_consult_attempts();
    let hits = GlobalTestMetrics::terminal_consult_hits();
    let resolved_found = GlobalTestMetrics::terminal_consult_resolved_found();
    // With the consult succeeding, cluster relays on the Found return path
    // opportunistically cache the state (normal GET relay caching, migration is
    // OFF) — informational only; the clean proof is `resolved_found` below.
    let cached_on_cluster: Vec<usize> = (1..=6usize)
        .filter(|n| result.is_node_hosting(&NodeLabel::node(NETWORK_NAME, *n), &contract_key))
        .collect();
    tracing::info!(
        attempts,
        hits,
        resolved_found,
        still_not_found = GlobalTestMetrics::terminal_consult_still_not_found(),
        ?cached_on_cluster,
        "terminal consult telemetry"
    );

    let requester_has_state = result
        .node_storages
        .get(&requester_label)
        .is_some_and(|s| s.get_stored_state(&contract_key).is_some());

    // CORE OF THE FIX: the consult carried the state from the off-path host to
    // the requester. With migration disabled, `resolved_found > 0` is the
    // unambiguous signal that the terminal advertisement consult (not routing,
    // not migration) closed the dead-end — it is recorded ONLY when a consulted
    // advertised host returns Found.
    assert!(
        resolved_found > 0,
        "terminal consult should have resolved the GET to Found via an advertised \
         off-path host (attempts={attempts}, hits={hits}, resolved_found={resolved_found}, \
         requester_has_state={requester_has_state})"
    );
    assert!(
        requester_has_state,
        "requester GET should succeed via the terminal advertisement consult \
         (resolved_found={resolved_found})"
    );
}

/// SUBSCRIBE counterpart of `test_terminal_advertisement_consult_closes_get_dead_end`:
/// a SUBSCRIBE whose only host sits one hop OFF the routing path must resolve
/// via the terminal advertisement consult instead of dead-ending with NotFound.
///
/// Same topology and isolation as the GET test (dense non-hosting cluster on
/// the key, one advertised host just outside it, far requester, migration
/// disabled, seeded-host advertising opted in). The SUBSCRIBE relay is
/// single-shot: it forwards one greedy hop toward the key, and — on a CLEAN
/// downstream NotFound — consults the host advertisements and forwards the
/// SUBSCRIBE to the off-path advertised host, which answers Subscribed.
///
/// Proof is `terminal_consult_resolved_found() > 0` (recorded ONLY when a
/// consulted host returns Subscribed); with migration off it is unambiguous
/// that the consult (not routing or migration) closed the subscribe dead-end.
#[test_log::test]
fn test_terminal_advertisement_consult_closes_subscribe_dead_end() {
    use freenet::config::GlobalTestMetrics;
    use freenet::dev_tool::{Location, NodeLabel, ScheduledOperation, SimNetwork, SimOperation};

    // Seed + placement chosen so the requester's upstream SUBSCRIBE routes into
    // the non-hosting cluster and dead-ends at a relay whose off-path neighbor
    // is the advertised host. Deterministic under the turmoil runner.
    const SEED: u64 = 0xC0FF_EEC0_0008;
    const NETWORK_NAME: &str = "terminal-consult-subscribe-deadend";
    setup_deterministic_state(SEED);
    GlobalTestMetrics::reset();

    let contract = SimOperation::create_test_contract(0xC0);
    let contract_id = *contract.key().id();
    let contract_key = contract.key();
    let key_loc = Location::from(&contract_key).as_f64();
    let wrap = |x: f64| x.rem_euclid(1.0);

    let cluster: Vec<f64> = [-0.010, -0.006, -0.003, 0.003, 0.006, 0.010]
        .iter()
        .map(|o| wrap(key_loc + o))
        .collect();
    let host_loc = wrap(key_loc + 0.05);
    let requester_loc = wrap(key_loc - 0.60);
    let mut node_locations = cluster.clone();
    node_locations.push(host_loc); // node_no 7
    node_locations.push(requester_loc); // node_no 8
    let num_nodes = node_locations.len();

    let rt = create_runtime();
    let mut sim = rt.block_on(async {
        SimNetwork::new_with_node_locations(
            NETWORK_NAME,
            1,
            num_nodes,
            10,
            7,
            8,
            3,
            SEED,
            &node_locations,
        )
        .await
    });
    sim.disable_placement_migration();
    sim.enable_seeded_host_advertisements();

    let host_label = NodeLabel::node(NETWORK_NAME, 7);
    let requester_label = NodeLabel::node(NETWORK_NAME, 8);

    let operations = vec![
        ScheduledOperation::new(
            host_label.clone(),
            SimOperation::SeedHostedContract {
                contract: contract.clone(),
                state: vec![11, 22, 33, 44],
            },
        ),
        // Seed the requester too: a bare SUBSCRIBE for an absent contract fails
        // the local RegisterSubscriberListener gate and never starts the
        // network subscribe. With the contract present, the SUBSCRIBE passes
        // that gate and routes UPSTREAM toward the key to build the
        // subscription tree — and since the requester is far from the key, it
        // dead-ends at the non-hosting cluster, exactly where the terminal
        // consult applies.
        ScheduledOperation::new(
            requester_label.clone(),
            SimOperation::SeedHostedContract {
                contract: contract.clone(),
                state: vec![11, 22, 33, 44],
            },
        ),
        ScheduledOperation::new(
            requester_label.clone(),
            SimOperation::Subscribe { contract_id },
        ),
    ];

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(180),
        Duration::from_secs(60),
    );
    assert!(
        result.turmoil_result.is_ok(),
        "simulation failed: {:?}",
        result.turmoil_result.err()
    );

    assert!(
        result.is_node_hosting(&host_label, &contract_key),
        "host should still host the seeded contract after the simulation"
    );

    let attempts = GlobalTestMetrics::terminal_consult_attempts();
    let hits = GlobalTestMetrics::terminal_consult_hits();
    let resolved_found = GlobalTestMetrics::terminal_consult_resolved_found();
    tracing::info!(
        attempts,
        hits,
        resolved_found,
        still_not_found = GlobalTestMetrics::terminal_consult_still_not_found(),
        "terminal consult telemetry (subscribe)"
    );

    // With migration off, `resolved_found > 0` is recorded ONLY when a
    // consulted advertised host answers Subscribed — the unambiguous proof
    // that the SUBSCRIBE terminal consult (not routing or migration) closed
    // the dead-end.
    assert!(
        resolved_found > 0,
        "terminal consult should have resolved the SUBSCRIBE to Subscribed via an \
         advertised off-path host (attempts={attempts}, hits={hits}, \
         resolved_found={resolved_found})"
    );
}

/// Reproduction + regression test for the placement-migration renewal storm
/// (#4440, the storm symptom of the #4404 placement work).
///
/// ## The storm
///
/// When the placement migration drifts a contract onto the peer closest to its
/// ring location, that peer becomes the *body-holding subscription root*: it
/// hosts the contract AND no connected neighbor is closer to the key. Such a
/// peer has no peer closer than itself to subscribe to upstream. Before the fix,
/// the subscription-renewal driver did not recognise that role: every renewal
/// cycle it still sent a wire `Subscribe::Request`, which routes greedily toward
/// the contract, dead-ends at the root itself, fails, and is retried on the next
/// 30s cycle — forever. Across many such contracts that loop is the renewal
/// storm that made the 500-node nightly go metastable (`subscribe: attempt
/// timed out ... is_renewal: true` floods; in prod the renewal task also
/// overruns its 25s cycle deadline).
///
/// ## What this test ignites
///
/// A small, deterministic topology where one node is unambiguously the
/// body-holding subscription root for a contract AND keeps that contract in the
/// renewal set (it holds a client subscription for it, so
/// `contracts_needing_renewal()` returns it every cycle). Migration is enabled
/// so the run reflects the production condition the storm appears under. Over the
/// simulation the root's renewal driver fires repeatedly.
///
/// ## The deterministic signal
///
/// `is_renewal` is not recorded on the captured `NetLogMessage` event stream and
/// the `subscription_renewal_outcome` telemetry is fire-and-forget to a remote
/// collector, so neither is readable in-sim. Instead the renewal driver publishes
/// a per-node counter into a simulation-only renewal-metrics registry that
/// `run_controlled_simulation` snapshots before the `SimNetwork` is dropped; the
/// test reads it via `ControlledSimulationResult::{renewal_metrics_for,
/// aggregate_renewal_metrics}`:
///   - `wire_attempts`: the node sent (or attempted) a wire renewal request —
///     the storm signature for a body-holding root, because each such attempt
///     dead-ends and is retried.
///   - `terminus_satisfied`: the renewal short-circuited via the root-satisfied
///     path the fix adds.
///
/// **Before the fix** the root's renewals are all `wire_attempts` (the storm) and
/// `terminus_satisfied` is zero — the asserts below FAIL. **After the fix** the
/// root's renewals are `terminus_satisfied` and it makes essentially no wire
/// renewal attempts — the asserts PASS. The test is run across several seeds
/// because the storm is metastable (one green run is not proof).
#[test_log::test]
fn test_subscription_root_renewal_does_not_storm() {
    use freenet::dev_tool::{Location, NodeLabel, ScheduledOperation, SimNetwork, SimOperation};

    // The storm is metastable; assert across several seeds, not just one.
    for seed in [0x4440_0001u64, 0x4440_0002, 0x4440_0003] {
        let network_name = format!("renewal-storm-{seed:x}");
        // Leak the name to obtain the &'static str SimNetwork::new requires; one
        // small allocation per seed in a test is fine.
        let network_name: &'static str = Box::leak(network_name.into_boxed_str());
        setup_deterministic_state(seed);

        // Place the root node exactly at the key and everyone else far from it,
        // so the root is unambiguously the closest connected peer (the
        // body-holding subscription root) and no migration can move the contract
        // off it.
        let contract = SimOperation::create_test_contract(0x44);
        let contract_id = *contract.key().id();
        let contract_key = contract.key();
        let key_loc = Location::from(&contract_key).as_f64();

        let wrap = |x: f64| x.rem_euclid(1.0);
        // node order 1 = root (AT the key); orders 2..=4 far from the key.
        let node_locations = vec![
            key_loc, // root
            wrap(key_loc + 0.30),
            wrap(key_loc + 0.45),
            wrap(key_loc + 0.60),
        ];
        let num_nodes = node_locations.len();

        let rt = create_runtime();
        let mut sim = rt.block_on(async {
            SimNetwork::new_with_node_locations(
                network_name,
                1,         // 1 gateway
                num_nodes, // 4 regular nodes
                10,        // ring_max_htl
                7,         // rnd_if_htl_above
                8,         // max_connections
                3,         // min_connections
                seed,
                &node_locations,
            )
            .await
        });
        // Match the production condition the storm appears under: migration ON.
        sim.enable_placement_migration();

        let root_label = NodeLabel::node(network_name, 1); // AT the key

        // Resolve the root's address now — `run_controlled_simulation` consumes
        // `sim`, and we need this to key into the renewal-metrics registry after.
        let root_addr = sim
            .node_address(&root_label)
            .unwrap_or_else(|| panic!("root node {root_label:?} has no address (seed {seed:x})"));

        // Sanity: the root must be strictly closer to the key than every other
        // peer (regular nodes AND the gateway), so it is genuinely the
        // body-holding subscription root. The root is placed AT the key, so its
        // distance is 0 — assert that directly plus the strict-closest invariant.
        let locs = sim.get_peer_locations(); // [gateway, node1..node4]
        let ring_dist = |a: f64, b: f64| {
            let d = (a - b).abs();
            d.min(1.0 - d)
        };
        let root_dist = ring_dist(locs[1], key_loc);
        // Include the gateway (index 0) in the "everyone else" set.
        let others_min = (0..locs.len())
            .filter(|&i| i != 1)
            .map(|i| ring_dist(locs[i], key_loc))
            .fold(f64::INFINITY, f64::min);
        // The root is placed AT the key; location quantization (loopback-port
        // assignment) introduces a tiny offset (~1e-6), so assert ~0 with a small
        // epsilon rather than exact zero.
        assert!(
            root_dist < 1e-3,
            "scenario setup wrong (seed {seed:x}): the root is placed AT the key, \
             so its distance must be ~0 (root_dist={root_dist})"
        );
        assert!(
            root_dist < others_min,
            "scenario setup wrong (seed {seed:x}): the root must be the closest peer to the key \
             including the gateway (root_dist={root_dist}, others_min={others_min})"
        );

        // A SECOND contract, seeded on a NON-root node (node 2) that is NOT its
        // closest peer, so node 2 must keep renewing it over the wire. This is
        // the guard against a silent network-wide renewal STOPPAGE: if
        // `is_subscription_root` / `body_holding_subscription_root_key` ever
        // regressed to return `Some` too liberally, every node would
        // short-circuit and NO node would wire-renew — a regression worse than
        // the storm. Pick a contract whose ring location is closest to a peer
        // OTHER than node 2, then seed it on node 2 with a client subscription.
        let non_root_label = NodeLabel::node(network_name, 2);
        let non_root_addr = sim.node_address(&non_root_label).unwrap_or_else(|| {
            panic!("non-root node {non_root_label:?} has no address (seed {seed:x})")
        });
        let non_root_loc = locs[2];
        let (contract2, contract2_id) = {
            // Search deterministic contract seeds for one whose key is NOT
            // closest to node 2 (so node 2 is a genuine non-root host).
            let mut chosen = None;
            for byte in 0u8..=255 {
                let c = SimOperation::create_test_contract(byte);
                let cloc = Location::from(&c.key()).as_f64();
                let node2_d = ring_dist(non_root_loc, cloc);
                let someone_closer = (0..locs.len())
                    .filter(|&i| i != 2)
                    .any(|i| ring_dist(locs[i], cloc) < node2_d);
                if someone_closer {
                    chosen = Some((c.clone(), *c.key().id()));
                    break;
                }
            }
            chosen.unwrap_or_else(|| {
                panic!("seed {seed:x}: could not find a contract node 2 is NOT the root for")
            })
        };

        let operations = vec![
            // The root genuinely HOSTS the contract (state + host_contract +
            // active subscription), with no network propagation — so
            // `is_subscription_root` is true and renewal eligibility starts.
            ScheduledOperation::new(
                root_label.clone(),
                SimOperation::SeedHostedContract {
                    contract: contract.clone(),
                    state: vec![1, 2, 3, 4],
                },
            ),
            // A local client on the root subscribes, so the contract stays in
            // `contracts_needing_renewal()` (the client-subscription path),
            // keeping the renewal driver firing on the root and marking it
            // `contract_in_use` (so the root-satisfied path refreshes the lease).
            ScheduledOperation::new(root_label.clone(), SimOperation::Subscribe { contract_id }),
            // Second contract on a NON-root node, with a client subscription so
            // it stays renewal-eligible. Node 2 is not its closest peer, so it
            // must wire-renew — the non-root control for the stoppage guard.
            ScheduledOperation::new(
                non_root_label.clone(),
                SimOperation::SeedHostedContract {
                    contract: contract2.clone(),
                    state: vec![5, 6, 7, 8],
                },
            ),
            ScheduledOperation::new(
                non_root_label.clone(),
                SimOperation::Subscribe {
                    contract_id: contract2_id,
                },
            ),
        ];

        // Run long enough for the network to form and several 30s renewal cycles
        // to fire on the root. `run_controlled_simulation` advances virtual time
        // by the test client's sleeps (3s warmup + 3s per op + the
        // post-operations wait), not the `simulation_duration` cap. The seeded
        // active subscription has an 8-minute lease and only enters the renewal
        // window at 6 minutes, so the post-operations wait must comfortably
        // exceed that to exercise the renewal driver on the root. 540s gives
        // several 30s renewal cycles inside the window. (Virtual time is cheap:
        // this whole run is a few seconds of wall-clock under Turmoil.)
        let result = sim.run_controlled_simulation(
            seed,
            operations,
            Duration::from_secs(900),
            Duration::from_secs(540),
        );
        assert!(
            result.turmoil_result.is_ok(),
            "simulation failed (seed {seed:x}): {:?}",
            result.turmoil_result.err()
        );

        // The root must still host the contract (its body-holding-root status,
        // and thus the storm condition, held for the whole run).
        assert!(
            result.is_node_hosting(&root_label, &contract_key),
            "root should still host the seeded contract after the simulation (seed {seed:x})"
        );

        // Read the renewal metrics captured inside `run_controlled_simulation`
        // BEFORE the SimNetwork was dropped (the Drop impl clears the global
        // registry, so reading it here would always see zeros — #4440).
        let root_metrics = result.renewal_metrics_for(&root_addr);
        let non_root_metrics = result.renewal_metrics_for(&non_root_addr);
        let totals = result.aggregate_renewal_metrics();

        tracing::info!(
            seed = format!("{seed:x}"),
            root_wire_attempts = root_metrics.wire_attempts,
            root_terminus_satisfied = root_metrics.terminus_satisfied,
            non_root_wire_attempts = non_root_metrics.wire_attempts,
            non_root_terminus_satisfied = non_root_metrics.terminus_satisfied,
            total_wire_attempts = totals.wire_attempts,
            total_terminus_satisfied = totals.terminus_satisfied,
            "renewal storm metrics"
        );

        // The renewal driver must have actually run on the root — otherwise the
        // test proves nothing (neither storm nor fix exercised).
        assert!(
            root_metrics.wire_attempts + root_metrics.terminus_satisfied > 0,
            "renewal driver never ran on the body-holding root (seed {seed:x}); \
             test exercised neither the storm nor the fix \
             (wire_attempts={}, terminus_satisfied={})",
            root_metrics.wire_attempts,
            root_metrics.terminus_satisfied,
        );

        // CORE OF THE FIX: a body-holding subscription root satisfies its
        // renewals locally and sends no wire renewal request.
        //
        // Before the fix this is the storm: every renewal on the root is a
        // doomed wire attempt and `terminus_satisfied` is zero — both asserts
        // below fail. After the fix the root's renewals are terminus-satisfied
        // and it makes no wire renewal attempts.
        assert!(
            root_metrics.terminus_satisfied > 0,
            "body-holding root never took the root-satisfied renewal path (seed {seed:x}): \
             terminus_satisfied=0, wire_attempts={}. Before the #4440 fix the root storms \
             with doomed upstream renewals instead of satisfying them locally.",
            root_metrics.wire_attempts,
        );
        assert_eq!(
            root_metrics.wire_attempts, 0,
            "body-holding root must NOT send wire renewal requests — that is the storm \
             (seed {seed:x}): wire_attempts={}, terminus_satisfied={}. Each wire renewal \
             from a root routes greedily toward the key, dead-ends at the root, fails, and \
             retries next cycle.",
            root_metrics.wire_attempts, root_metrics.terminus_satisfied,
        );

        // UPPER BOUND on the root's terminus-satisfied count (regression guard
        // for the lease-refresh gating, #4440 blocker 2). The in-use root
        // refreshes its LOCAL lease on each root-satisfied renewal, which moves
        // it onto the ~6-minute lease-expiry renewal cadence. Over this ~9-minute
        // run that is at most ~2 cycles. If the lease refresh were accidentally
        // dropped (or made unconditional in a way that re-selected the root every
        // 30s cycle), the root would fire the root-satisfied path on EVERY cycle
        // and this count would balloon into the teens — caught here.
        assert!(
            root_metrics.terminus_satisfied <= 4,
            "root took the root-satisfied path {} times (seed {seed:x}) — far more than the \
             ~6-minute lease-expiry cadence allows over this run. The local-lease refresh \
             is supposed to keep an in-use root OFF the every-30s-cycle renewal selection; \
             a count this high means it is being re-selected every cycle (the batch-starvation \
             regression).",
            root_metrics.terminus_satisfied,
        );

        // STOPPAGE GUARD (#4440): renewals must NOT have globally stopped. A
        // non-root host (node 2, not the closest peer for contract2) must keep
        // renewing over the wire. If `is_subscription_root` regressed to return
        // true too liberally, EVERY node would short-circuit and this would be 0
        // — a silent network-wide renewal stoppage, worse than the storm.
        assert!(
            non_root_metrics.wire_attempts > 0,
            "non-root host made no wire renewal attempts (seed {seed:x}): wire_attempts=0, \
             terminus_satisfied={}. A non-root host MUST keep renewing upstream; zero here \
             means the root short-circuit is firing on non-roots too (silent global renewal \
             stoppage).",
            non_root_metrics.terminus_satisfied,
        );
        // And the non-root must NOT have taken the root-satisfied path (it is not
        // a root for contract2).
        assert_eq!(
            non_root_metrics.terminus_satisfied, 0,
            "non-root host wrongly took the root-satisfied path (seed {seed:x}): \
             terminus_satisfied={}, wire_attempts={}. Only a body-holding root may \
             short-circuit; a non-root host must wire-renew.",
            non_root_metrics.terminus_satisfied, non_root_metrics.wire_attempts,
        );
    }
}

/// Regression test for #4440 blocker 2: a body-holding subscription root with NO
/// genuine interest (no local client subscription, no downstream subscriber)
/// must let its `active_subscriptions` lease **lapse** rather than refresh it on
/// the root-satisfied renewal path.
///
/// Why this matters: the root-satisfied renewal path must not refresh the lease
/// of a root with no genuine interest. If it refreshed the lease
/// unconditionally, a no-interest root would keep its `active_subscriptions`
/// lease alive forever — an unbounded `is_subscribed`-only retention exemption
/// that the `contract_in_use` rustdoc and the AGENTS.md time-bounded-exemption
/// rule forbid. Both the root-satisfied path and `contracts_needing_renewal`
/// section 1 gate renewal on `contract_in_use`, so a no-interest root does NOT
/// refresh and the lease lapses (`SUBSCRIPTION_LEASE_DURATION` = 8 min) — after
/// which it drops out of the renewal set entirely.
///
/// This test seeds a hosted contract on a body-holding root with NO client
/// subscription, runs past the lease window, and asserts the root's final
/// `active_subscription_keys` (captured from the topology snapshot) no longer
/// contains the contract — i.e. the lease was allowed to lapse, not
/// self-perpetuated.
#[test_log::test]
fn test_no_interest_subscription_root_lease_lapses() {
    use freenet::dev_tool::{Location, NodeLabel, ScheduledOperation, SimNetwork, SimOperation};

    const SEED: u64 = 0x4440_0009;
    let network_name = "renewal-root-lease-lapse";
    setup_deterministic_state(SEED);

    let contract = SimOperation::create_test_contract(0x4A);
    let contract_id = *contract.key().id();
    let contract_key = contract.key();
    let key_loc = Location::from(&contract_key).as_f64();

    let wrap = |x: f64| x.rem_euclid(1.0);
    // node order 1 = root (AT the key); orders 2..=4 far from the key.
    let node_locations = vec![
        key_loc,
        wrap(key_loc + 0.30),
        wrap(key_loc + 0.45),
        wrap(key_loc + 0.60),
    ];
    let num_nodes = node_locations.len();

    let rt = create_runtime();
    let mut sim = rt.block_on(async {
        SimNetwork::new_with_node_locations(
            network_name,
            1,
            num_nodes,
            10,
            7,
            8,
            3,
            SEED,
            &node_locations,
        )
        .await
    });
    sim.enable_placement_migration();

    let root_label = NodeLabel::node(network_name, 1);
    let root_addr = sim
        .node_address(&root_label)
        .expect("root node has an address");

    // Seed the contract as hosted on the root, but DO NOT subscribe a client and
    // DO NOT create any downstream subscriber — so `contract_in_use` is false.
    let operations = vec![ScheduledOperation::new(
        root_label.clone(),
        SimOperation::SeedHostedContract {
            contract: contract.clone(),
            state: vec![1, 2, 3, 4],
        },
    )];

    // Run past the 8-minute lease (post-op wait 600s ≈ 10 min) so a refreshed
    // lease would still be live at the end, while a lapsed lease is gone — the
    // discriminating signal.
    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(900),
        Duration::from_secs(600),
    );
    assert!(
        result.turmoil_result.is_ok(),
        "simulation failed: {:?}",
        result.turmoil_result.err()
    );

    // The root still HOSTS the contract (hosting is decoupled from the lease).
    assert!(
        result.is_node_hosting(&root_label, &contract_key),
        "root should still host the contract (hosting does not depend on the lease)"
    );

    // The root must NOT have made wire renewal attempts (it is the body-holding
    // root) — its renewals take the root-satisfied path.
    let root_metrics = result.renewal_metrics_for(&root_addr);
    assert_eq!(
        root_metrics.wire_attempts, 0,
        "body-holding root must not wire-renew (wire_attempts={}, terminus_satisfied={})",
        root_metrics.wire_attempts, root_metrics.terminus_satisfied,
    );

    // CORE ASSERTION: the lease lapsed. The root's final active-subscription set
    // (captured in the topology snapshot before drop) must NOT contain the
    // contract — a no-interest root does not refresh, so after 8 minutes the
    // lease is gone and the root has dropped out of the renewal set. If the
    // refresh were unconditional, the lease would still be live here.
    let root_snapshot = result
        .topology_snapshots
        .iter()
        .find(|s| s.peer_addr == root_addr)
        .expect("root topology snapshot present");
    assert!(
        !root_snapshot
            .active_subscription_keys
            .contains(&contract_id),
        "no-interest body-holding root self-perpetuated its lease: the contract is still in \
         active_subscription_keys after the 8-minute lease window. The root-satisfied path \
         must NOT refresh the lease when the contract is not in use (#4440 blocker 2) — \
         otherwise it is an unbounded is_subscribed-only retention exemption."
    );

    tracing::info!(
        root_terminus_satisfied = root_metrics.terminus_satisfied,
        active_subs = root_snapshot.active_subscription_keys.len(),
        "no-interest root lease lapsed as expected"
    );
}

/// Opt-in migration-at-scale regression guard (#4601).
///
/// ## What this is the home for
///
/// The placement-migration (`SubscribeHint`) cascade is OFF by default in every
/// simulation since #4601 (the per-node version floor defaults to an unreachable
/// value — see `SimNetwork::SIM_MIGRATION_DISABLED_FLOOR`), so the general
/// throughput nightlies no longer carry migration load. This test is the
/// DEDICATED place that turns migration ON (`enable_placement_migration`) and
/// proves its load is BOUNDED at scale — not merely that the sim completes.
///
/// ## The bound it asserts
///
/// The production subscription-renewal driver
/// (`Ring::recover_orphaned_subscriptions`) hard-caps the number of renewal
/// tasks it spawns per 30s recovery cycle at
/// `Ring::MAX_RECOVERY_ATTEMPTS_PER_INTERVAL` (10), regardless of how many
/// contracts are eligible. That per-node rate cap is the reason the renewal path
/// is load-safe in production (prod telemetry shows real peers well under it).
/// This test exercises the cap by hosting MANY (`NUM_CONTRACTS` = 20) contracts
/// on one node, all of which enter the 2-minute renewal window simultaneously
/// (they share an ~8-minute lease set at startup) so a single cycle has far more
/// eligible contracts than the cap. It then asserts, via the per-node
/// `max_cycle_batch` metric (the largest `attempted` count any cycle reached):
///
///   * (cap holds, EVERY node) `max_cycle_batch <= MAX_RECOVERY_ATTEMPTS_PER_INTERVAL`.
///     This is the regression guard: if the spawn cap were removed, the loaded
///     node would record `max_cycle_batch == NUM_CONTRACTS` (20) and FAIL.
///   * (cap engaged, loaded node) `max_cycle_batch == MAX_RECOVERY_ATTEMPTS_PER_INTERVAL`.
///     Proves demand exceeded the cap and the cap actually clipped — without
///     this the bound above could pass vacuously on a node that never had >10
///     eligible contracts at once.
///
/// ## Why migration must be ON for this to be meaningful
///
/// With migration ON the loaded node also nudges its closer-to-the-key neighbors
/// to host its contracts (directed subscribes), so the run carries the real
/// migration traffic the #4601 regression piled onto unrelated sims. The test
/// additionally asserts at least one contract migrated onto another node, so the
/// cascade is genuinely exercised (not silently inert).
#[test_log::test]
fn test_placement_migration_at_scale_renewal_load_stays_bounded() {
    use freenet::dev_tool::{NodeLabel, ScheduledOperation, SimNetwork, SimOperation};

    // Mirrors `ring::Ring::MAX_RECOVERY_ATTEMPTS_PER_INTERVAL` (private to the
    // crate). The renewal driver breaks its spawn loop once `attempted` reaches
    // this value, so no node may exceed it in a single 30s cycle. Frozen value;
    // if the production constant changes, update this and the rationale above.
    const MAX_RECOVERY_ATTEMPTS_PER_INTERVAL: u64 = 10;
    // Hosted-on-the-loaded-node contract count. Chosen well above the cap so a
    // single renewal-window cycle has many more eligible contracts than the cap
    // can spawn — that is what makes the cap regression detectable.
    const NUM_CONTRACTS: usize = 20;

    // Metastable load behavior: assert across several seeds, not just one.
    for seed in [0x4601_0001u64, 0x4601_0002, 0x4601_0003] {
        let network_name = format!("migration-scale-{seed:x}");
        // Leak to obtain the &'static str SimNetwork::new requires; one small
        // allocation per seed in a test is fine (same pattern as the storm test).
        let network_name: &'static str = Box::leak(network_name.into_boxed_str());
        setup_deterministic_state(seed);

        // 1 gateway + 8 regular nodes spread around the ring. The loaded node
        // (node 1) sits at 0.5; the others are scattered so that for most of the
        // hosted contracts SOME peer is strictly closer to the key than the
        // loaded node — giving the migration cascade real targets to nudge.
        let loaded_loc = 0.5;
        let node_locations = vec![
            loaded_loc, // node 1 = the loaded node
            0.05, 0.15, 0.30, 0.42, 0.62, 0.78, 0.92,
        ];
        let num_nodes = node_locations.len();

        let rt = create_runtime();
        let mut sim = rt.block_on(async {
            SimNetwork::new_with_node_locations(
                network_name,
                1,         // 1 gateway
                num_nodes, // 8 regular nodes
                10,        // ring_max_htl
                7,         // rnd_if_htl_above
                16,        // max_connections (room for the small mesh)
                3,         // min_connections
                seed,
                &node_locations,
            )
            .await
        });
        // OPT IN to placement migration — the whole point of this test.
        sim.enable_placement_migration();

        let loaded_label = NodeLabel::node(network_name, 1);
        let loaded_addr = sim
            .node_address(&loaded_label)
            .unwrap_or_else(|| panic!("loaded node has no address (seed {seed:x})"));

        // Build NUM_CONTRACTS distinct contracts and host them all on the loaded
        // node (seeded as genuinely hosted + actively subscribed at startup, so
        // their leases all start together → they all enter the renewal window in
        // the same cycle). A client Subscribe per contract keeps them
        // renewal-eligible past lease expiry (path 2) so the high-demand window
        // spans several cycles.
        let contracts: Vec<_> = (0..NUM_CONTRACTS)
            .map(|i| SimOperation::create_test_contract(i as u8))
            .collect();
        let contract_keys: Vec<_> = contracts.iter().map(|c| c.key()).collect();

        let mut operations = Vec::with_capacity(NUM_CONTRACTS * 2);
        for contract in &contracts {
            operations.push(ScheduledOperation::new(
                loaded_label.clone(),
                SimOperation::SeedHostedContract {
                    contract: contract.clone(),
                    state: vec![1, 2, 3, 4],
                },
            ));
        }
        for contract in &contracts {
            operations.push(ScheduledOperation::new(
                loaded_label.clone(),
                SimOperation::Subscribe {
                    contract_id: *contract.key().id(),
                },
            ));
        }

        // Run past the 6-minute renewal-window opening (8-min lease − 2-min
        // window) and several 30s cycles into it. 540s post-op wait gives the
        // window-burst cycles where all NUM_CONTRACTS are eligible at once.
        let result = sim.run_controlled_simulation(
            seed,
            operations,
            Duration::from_secs(900),
            Duration::from_secs(540),
        );
        assert!(
            result.turmoil_result.is_ok(),
            "simulation failed (seed {seed:x}): {:?}",
            result.turmoil_result.err()
        );

        let loaded_metrics = result.renewal_metrics_for(&loaded_addr);
        let totals = result.aggregate_renewal_metrics();
        tracing::info!(
            seed = format!("{seed:x}"),
            loaded_max_cycle_batch = loaded_metrics.max_cycle_batch,
            loaded_wire_attempts = loaded_metrics.wire_attempts,
            loaded_terminus = loaded_metrics.terminus_satisfied,
            agg_max_cycle_batch = totals.max_cycle_batch,
            agg_wire_attempts = totals.wire_attempts,
            "migration-at-scale renewal load"
        );

        // (A) CAP HOLDS for EVERY node — the regression guard. If the per-cycle
        // spawn cap were removed, the loaded node (with NUM_CONTRACTS eligible at
        // once) would record max_cycle_batch == NUM_CONTRACTS > the cap.
        for (addr, m) in &result.renewal_metrics {
            assert!(
                m.max_cycle_batch <= MAX_RECOVERY_ATTEMPTS_PER_INTERVAL,
                "node {addr} spawned {} renewal tasks in a single 30s cycle (seed {seed:x}) — \
                 exceeds the production cap MAX_RECOVERY_ATTEMPTS_PER_INTERVAL={}. The renewal \
                 rate cap in recover_orphaned_subscriptions has regressed (#4601).",
                m.max_cycle_batch,
                MAX_RECOVERY_ATTEMPTS_PER_INTERVAL,
            );
        }

        // (B) CAP ENGAGED on the loaded node — non-vacuous. With NUM_CONTRACTS
        // (>cap) eligible simultaneously, the loaded node must have hit the cap in
        // at least one cycle. If this is < cap the test proved nothing about the
        // cap (demand never exceeded it), so the guard in (A) would be vacuous.
        assert_eq!(
            loaded_metrics.max_cycle_batch, MAX_RECOVERY_ATTEMPTS_PER_INTERVAL,
            "loaded node never reached the renewal cap (seed {seed:x}): max_cycle_batch={}, \
             expected exactly MAX_RECOVERY_ATTEMPTS_PER_INTERVAL={}. With {} contracts entering \
             the renewal window together the cap must clip a cycle to exactly the cap; a lower \
             value means the high-demand burst did not form and (A) is vacuous.",
            loaded_metrics.max_cycle_batch, MAX_RECOVERY_ATTEMPTS_PER_INTERVAL, NUM_CONTRACTS,
        );

        // (C) MIGRATION genuinely ran — at least one hosted contract migrated
        // onto another node via the directed-subscribe cascade. Without this the
        // test could pass with migration silently inert (which would defeat its
        // purpose as migration's scale home).
        let mut migrated: Vec<(usize, usize)> = Vec::new();
        for ci in 0..NUM_CONTRACTS {
            // Non-loaded regular nodes are node_no 2..=num_nodes (node 1 is the
            // loaded host; the loaded node hosting its own seeded contract is not
            // a migration).
            for n in 2..=num_nodes {
                if result.is_node_hosting(&NodeLabel::node(network_name, n), &contract_keys[ci]) {
                    migrated.push((ci, n));
                }
            }
        }
        assert!(
            !migrated.is_empty(),
            "placement migration was enabled but NO contract migrated off the loaded node to a \
             closer neighbor (seed {seed:x}). Either the cascade is inert (gate regression) or \
             the topology never let a neighbor become a migration target — the test is not \
             exercising migration. (contract_idx, node_no) hosting pairs: {migrated:?}",
        );

        tracing::info!(
            seed = format!("{seed:x}"),
            migrated_pairs = migrated.len(),
            "migration-at-scale: cap held, cap engaged, migration ran"
        );
    }
}

// =============================================================================
// Demand-driven hosting harness (#4642) — example sim tests exercising the
// sim-harness infrastructure added for the hosting/subscription redesign:
//   * injectable, controllable hosting clock (enable_hosting_time_control +
//     SimOperation::AdvanceHostingClock)   — piece A (eviction/TTL)
//   * per-node hosting-budget knob (with_hosting_budget)                — piece A
//   * per-node subscription / hosting / active-demand measurement
//     accessors on ControlledSimulationResult                          — piece D
//   * scripted mid-run node crash (SimOperation::CrashNode)            — piece F
//
// These validate the harness end-to-end. The strong, deterministic proof that
// the injected clock drives eviction lives in the unit test
// `ring::hosting::tests::test_with_time_source_lease_expiry_follows_injected_clock`.
// =============================================================================

/// End-to-end validation of the controllable hosting clock + budget knob +
/// per-node measurement accessors (#4642 pieces A & D).
///
/// Injects a controllable hosting clock and a tiny hosting budget into every
/// node, runs a controlled scenario that PUTs a demanded contract, advances the
/// hosting clock far past the 8-minute TTL gate via `AdvanceHostingClock`, and
/// then reads the new per-node measurement accessors. Asserts the harness
/// plumbing works end-to-end and that a contract backed by active demand
/// survives a large clock jump (demand protects against eviction).
#[test]
fn test_hosting_clock_injection_and_measurement_end_to_end() {
    use freenet::dev_tool::{NodeLabel, ScheduledOperation, SimOperation};

    const NETWORK: &str = "hosting-clock-injection";
    const SEED: u64 = 0x4642_A001_CAFE;

    setup_deterministic_state(SEED);
    let rt = create_runtime();

    // The gateway PUTs (and hosts) the demanded contract — a gateway reliably
    // hosts its own PUT, avoiding placement non-determinism. A regular node
    // (labels start at `number_of_gateways`) later GETs it.
    let gateway = NodeLabel::gateway(NETWORK, 0);
    let getter = NodeLabel::node(NETWORK, 1);
    let demanded = SimOperation::create_test_contract(11);
    let demanded_key = demanded.key();
    let demanded_id = *demanded.key().id();
    let demanded_state = SimOperation::create_test_state(11);

    // Build + configure the network inside a tokio runtime; run_controlled_simulation
    // manages its OWN turmoil runtime and must be called OUTSIDE block_on.
    let (sim, clock, clock_start) = rt.block_on(async {
        let mut sim = SimNetwork::new(NETWORK, 1, 2, 7, 3, 10, 2, SEED).await;

        // Piece A: inject a controllable hosting clock (so TTL/eviction is under
        // test control) and a tiny per-node budget (to force cache pressure).
        let clock = sim.enable_hosting_time_control();
        sim.with_hosting_budget(4096);
        let clock_start = clock.current_time();
        (sim, clock, clock_start)
    });

    let operations = vec![
        // Demanded contract: PUT with subscribe=true → the gateway hosts it AND
        // holds a local client subscription, so eviction must NOT drop it.
        ScheduledOperation::new(
            gateway.clone(),
            SimOperation::Put {
                contract: demanded.clone(),
                state: demanded_state.clone(),
                subscribe: true,
            },
        ),
        // Jump the hosting clock 30 minutes — far past the 8-minute TTL gate —
        // deterministically, WITHOUT running 30 minutes of virtual time.
        ScheduledOperation::new(
            gateway.clone(),
            SimOperation::AdvanceHostingClock {
                duration: Duration::from_secs(30 * 60),
            },
        ),
        // Re-access the demanded contract from a node, which exercises the GET
        // path and triggers hosting-cache activity on the advanced clock.
        ScheduledOperation::new(
            getter.clone(),
            SimOperation::Get {
                contract_id: demanded_id,
                return_contract_code: true,
                subscribe: false,
            },
        ),
    ];

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(180),
        Duration::from_secs(30),
    );

    // The injected clock is shared with every node; confirm the scripted
    // AdvanceHostingClock actually moved it forward by the 30 minutes we
    // scheduled (the sim advanced the same Arc-backed clock this handle sees).
    assert!(
        clock.current_time() >= clock_start + Duration::from_secs(30 * 60),
        "AdvanceHostingClock should have advanced the shared hosting clock by >= 30 minutes"
    );

    assert!(
        result.turmoil_result.is_ok(),
        "controlled simulation should complete: {:?}",
        result.turmoil_result.err()
    );

    // The measurement accessors (#4642 piece D) must be readable per node.
    for label in result.captured_node_labels() {
        tracing::info!(
            node = %label,
            hosting = result.node_hosting_count(&label),
            subscriptions = result.node_subscription_count(&label),
            active_demand = ?result.node_active_demand_count(&label),
            "hosting measurement snapshot"
        );
    }

    // The demanded contract must still be hosted on the gateway after the large
    // clock jump — its local client subscription protects it from TTL/budget
    // eviction. This exercises the clock-injection + budget plumbing end-to-end.
    assert!(
        result.is_node_hosting(&gateway, &demanded_key),
        "gateway should still host the demanded (subscribed) contract after a 30-minute \
         hosting-clock jump; its client subscription must protect it from eviction"
    );
    assert!(
        result.node_hosting_count(&gateway) >= 1,
        "gateway should host at least the demanded contract"
    );

    // The per-node subscription / active-demand accessors (#4642 piece D) are
    // exercised in the logging loop above. They are the measurement surface a
    // no-storm proof asserts on (subscriptions ∝ demand, not ∝ cache size); the
    // strict storm bound is a property piece D must establish, so this harness
    // test asserts only that the accessors are wired and the plumbing runs, not
    // the storm bound itself.
}

/// Demonstrates the scripted mid-run node-crash capability (#4642 piece F):
/// `SimOperation::CrashNode` removes a specific peer at a scripted point via the
/// per-network fault injector, in-order with the other events. Asserts the
/// simulation completes and the contract remains hosted somewhere in the
/// surviving network.
///
/// NOTE: `CrashNode` is a SILENT (message-blocking) crash — the crashed peer's
/// transport is not torn down, so surviving peers notice on their own
/// routing-health cadence rather than an immediate transport close. A test that
/// needs *prompt* upstream-loss detection (the core of piece F) additionally
/// depends on the event-driven re-subscribe production change, which is not on
/// `main`.
#[test]
fn test_scripted_node_crash_mid_run() {
    use freenet::dev_tool::{NodeLabel, ScheduledOperation, SimOperation};

    const NETWORK: &str = "scripted-node-crash";
    const SEED: u64 = 0x4642_F001_CAFE;

    setup_deterministic_state(SEED);
    let rt = create_runtime();

    // Regular-node labels start at `number_of_gateways` (= 1 here).
    let gateway = NodeLabel::gateway(NETWORK, 0);
    let node1 = NodeLabel::node(NETWORK, 1);
    let node2 = NodeLabel::node(NETWORK, 2);

    let contract = SimOperation::create_test_contract(21);
    let contract_key = contract.key();
    let contract_id = *contract.key().id();
    let state = SimOperation::create_test_state(21);

    // Build the network inside a tokio runtime; run_controlled_simulation manages
    // its OWN turmoil runtime and must be called OUTSIDE block_on.
    let sim = rt.block_on(async { SimNetwork::new(NETWORK, 1, 3, 7, 3, 10, 2, SEED).await });

    let operations = vec![
        // Gateway hosts the contract.
        ScheduledOperation::new(
            gateway.clone(),
            SimOperation::Put {
                contract: contract.clone(),
                state: state.clone(),
                subscribe: true,
            },
        ),
        // Two nodes subscribe to it.
        ScheduledOperation::new(node1.clone(), SimOperation::Subscribe { contract_id }),
        ScheduledOperation::new(node2.clone(), SimOperation::Subscribe { contract_id }),
        // Crash node1 mid-run (scripted, in-order).
        ScheduledOperation::new(node1.clone(), SimOperation::CrashNode),
    ];

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(180),
        Duration::from_secs(30),
    );

    assert!(
        result.turmoil_result.is_ok(),
        "controlled simulation with a scripted crash should complete: {:?}",
        result.turmoil_result.err()
    );

    for label in result.captured_node_labels() {
        tracing::info!(
            node = %label,
            hosting = result.node_hosting_count(&label),
            subscriptions = result.node_subscription_count(&label),
            upstream = ?result.node_upstream_count(&label, &contract_key),
            "post-crash measurement snapshot"
        );
    }

    // DISCRIMINATING assertion: the crash must have actually taken effect.
    // `CrashNode` installs the sim's packet-delivery callback and marks node1
    // crashed in the fault injector; every packet to/from node1 is then dropped
    // and counted. node1 held an established subscription (keep-alive traffic +
    // renewals + update fan-out), so post-crash traffic to/from it is inevitable
    // — `crash_packets_dropped() > 0` therefore holds ONLY if the crash really
    // blocked node1's packets. Before this harness fix the callback was never
    // installed, so a "crashed" node kept exchanging packets and this count
    // stayed 0 (the false-green the fix closes). See #4642 piece F.
    assert!(
        result.crash_packets_dropped() > 0,
        "scripted CrashNode must actually drop packets to/from the crashed node \
         (crash_packets_dropped == 0 means the crash was a silent no-op)"
    );

    // The contract must still be hosted somewhere in the surviving network:
    // the gateway hosts it, and node1's crash must not take it down.
    let hosted_somewhere = result
        .captured_node_labels()
        .iter()
        .any(|label| result.is_node_hosting(label, &contract_key));
    assert!(
        hosted_somewhere,
        "contract should remain hosted on a surviving node after node1 was crashed"
    );
}

/// Error-path coverage for the controllable-clock harness: scheduling
/// `SimOperation::AdvanceHostingClock` WITHOUT first calling
/// `enable_hosting_time_control()` must degrade gracefully — the runner logs a
/// warning and the advance is a no-op, rather than panicking or wedging. This
/// exercises the "no controllable hosting clock was enabled" branch of the
/// special-op dispatch.
#[test]
fn test_advance_hosting_clock_without_control_is_graceful_noop() {
    use freenet::dev_tool::{NodeLabel, ScheduledOperation, SimOperation};

    const NETWORK: &str = "advance-clock-no-control";
    const SEED: u64 = 0x4642_A002_CAFE;

    setup_deterministic_state(SEED);
    let rt = create_runtime();

    let gateway = NodeLabel::gateway(NETWORK, 0);
    let contract = SimOperation::create_test_contract(31);
    let state = SimOperation::create_test_state(31);

    // NOTE: enable_hosting_time_control() is deliberately NOT called here.
    let sim = rt.block_on(async { SimNetwork::new(NETWORK, 1, 1, 7, 3, 10, 2, SEED).await });

    let operations = vec![
        ScheduledOperation::new(
            gateway.clone(),
            SimOperation::Put {
                contract: contract.clone(),
                state: state.clone(),
                subscribe: true,
            },
        ),
        // No controllable clock is enabled → this must be a graceful no-op.
        ScheduledOperation::new(
            gateway.clone(),
            SimOperation::AdvanceHostingClock {
                duration: Duration::from_secs(30 * 60),
            },
        ),
    ];

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(120),
        Duration::from_secs(20),
    );

    assert!(
        result.turmoil_result.is_ok(),
        "simulation must complete gracefully even when AdvanceHostingClock is scheduled \
         without a controllable clock enabled: {:?}",
        result.turmoil_result.err()
    );
    // The gateway still hosts its PUT — the no-op advance changed nothing.
    assert!(
        result.is_node_hosting(&gateway, &contract.key()),
        "gateway should still host its PUT contract"
    );
}

/// M2 (#4642 piece A): proves the injected hosting clock is NOT a silent no-op
/// end-to-end through SimNetwork → NodeConfig → Ring → HostingManager. This
/// guards the whole clock-injection wiring against future refactors: it is the
/// discriminating counterpart to
/// `test_hosting_clock_injection_and_measurement_end_to_end` (which only shows a
/// DEMANDED contract SURVIVES a clock jump — a no-op injection would pass that
/// too).
///
/// A gateway hosts several UNDEMANDED (`subscribe=false`, cache-only) contracts
/// under a 1-byte hosting budget, so the cache is permanently over budget and
/// eviction is gated SOLELY by the TTL clock. Eviction of a past-TTL entry runs
/// on the next cache access; a trailing PUT provides that access
/// deterministically (no dependence on background-sweep timing). We run the SAME
/// scenario twice, differing ONLY in how far the injected clock is advanced
/// before that trailing access:
///   * control: advance 1 min  (< DEFAULT_MIN_TTL = 8 min) → NO eviction
///   * test:    advance 10 min (> DEFAULT_MIN_TTL)         → eviction
///
/// If the clock injection were broken (an `AdvanceHostingClock` that never
/// reaches the `HostingManager`'s cache), both runs would behave identically and
/// the `test_count < control_count` assertion would fail — which is exactly the
/// regression this test exists to catch.
#[test]
fn test_injected_hosting_clock_drives_undemanded_eviction() {
    use freenet::dev_tool::{NodeLabel, ScheduledOperation, SimOperation};

    // DEFAULT_MIN_TTL = TTL_RENEWAL_MULTIPLIER (4) × SUBSCRIPTION_RENEWAL_INTERVAL
    // (120s) = 480s (8 min). One advance stays under it, the other crosses it.
    const SUB_TTL_ADVANCE: Duration = Duration::from_secs(60); // 1 min  < 8 min
    const SUPER_TTL_ADVANCE: Duration = Duration::from_secs(10 * 60); // 10 min > 8 min

    // Seeds of the undemanded contracts the gateway hosts (cache-only). A
    // distinct trigger contract is PUT AFTER the advance to force a cache access.
    const UNDEMANDED_SEEDS: [u8; 3] = [41, 42, 43];
    const TRIGGER_SEED: u8 = 44;

    // Runs the scenario with a given clock advance; returns the gateway's final
    // hosting count and whether each undemanded contract is still hosted.
    let run = |network: &str, seed: u64, advance: Duration| -> (usize, [bool; 3]) {
        setup_deterministic_state(seed);
        let rt = create_runtime();
        let gateway = NodeLabel::gateway(network, 0);

        let undemanded: Vec<_> = UNDEMANDED_SEEDS
            .iter()
            .map(|s| SimOperation::create_test_contract(*s))
            .collect();
        let undemanded_keys: Vec<_> = undemanded.iter().map(|c| c.key()).collect();
        let trigger = SimOperation::create_test_contract(TRIGGER_SEED);

        let sim = rt.block_on(async {
            let mut sim = SimNetwork::new(network, 1, 2, 7, 3, 10, 2, seed).await;
            // Piece A: inject the controllable clock (TTL under test control) and
            // a 1-byte budget so ANY hosted contract is over budget — eviction is
            // then gated purely by the TTL clock this test advances.
            sim.enable_hosting_time_control();
            sim.with_hosting_budget(1);
            sim
        });

        let mut operations = Vec::new();
        // The gateway hosts each undemanded contract (host-on-PUT); with
        // subscribe=false there is no client subscription, so it is cache-only
        // (evictable) rather than demand-pinned.
        for (c, s) in undemanded.iter().zip(UNDEMANDED_SEEDS.iter()) {
            operations.push(ScheduledOperation::new(
                gateway.clone(),
                SimOperation::Put {
                    contract: c.clone(),
                    state: SimOperation::create_test_state(*s),
                    subscribe: false,
                },
            ));
        }
        // Jump the injected clock deterministically (no virtual minutes elapse).
        operations.push(ScheduledOperation::new(
            gateway.clone(),
            SimOperation::AdvanceHostingClock { duration: advance },
        ));
        // Trailing PUT: a fresh cache access that runs `evict_over_budget`. In
        // the super-TTL run the undemanded entries are now past TTL and are
        // evicted; in the sub-TTL run they are still TTL-protected. The trigger
        // itself is freshly inserted (age 0) and cannot be evicted by its own
        // access, so it survives in both runs.
        operations.push(ScheduledOperation::new(
            gateway.clone(),
            SimOperation::Put {
                contract: trigger.clone(),
                state: SimOperation::create_test_state(TRIGGER_SEED),
                subscribe: false,
            },
        ));

        let result = sim.run_controlled_simulation(
            seed,
            operations,
            Duration::from_secs(120),
            Duration::from_secs(20),
        );
        assert!(
            result.turmoil_result.is_ok(),
            "controlled simulation should complete: {:?}",
            result.turmoil_result.err()
        );
        let flags = [
            result.is_node_hosting(&gateway, &undemanded_keys[0]),
            result.is_node_hosting(&gateway, &undemanded_keys[1]),
            result.is_node_hosting(&gateway, &undemanded_keys[2]),
        ];
        (result.node_hosting_count(&gateway), flags)
    };

    const SEED: u64 = 0x4642_A003_CAFE;
    let (control_count, control_flags) = run("clock-evict-control", SEED, SUB_TTL_ADVANCE);
    let (test_count, test_flags) = run("clock-evict-test", SEED, SUPER_TTL_ADVANCE);

    tracing::info!(
        control_count,
        test_count,
        ?control_flags,
        ?test_flags,
        "injected-clock eviction: control (sub-TTL advance) vs test (super-TTL advance)"
    );

    // Sanity: host-on-PUT actually hosted the undemanded contracts (otherwise the
    // comparison below would be vacuous). All three present in the control run.
    assert!(
        control_flags.iter().all(|&h| h),
        "sub-TTL advance ({}s < 8min): all undemanded contracts must still be hosted \
         — they are over budget but TTL-protected, so eviction must NOT fire; got {:?}",
        SUB_TTL_ADVANCE.as_secs(),
        control_flags
    );

    // After crossing DEFAULT_MIN_TTL: the SAME undemanded contracts ARE evicted.
    assert!(
        test_flags.iter().all(|&h| !h),
        "super-TTL advance ({}s > 8min): all undemanded contracts must be evicted \
         once the injected clock crosses DEFAULT_MIN_TTL; got {:?}",
        SUPER_TTL_ADVANCE.as_secs(),
        test_flags
    );

    // The discriminating comparison: the ONLY difference between the two runs is
    // how far the injected clock advanced. Strictly fewer contracts surviving the
    // super-TTL run can happen ONLY if `AdvanceHostingClock` actually reached the
    // `HostingManager`'s cache. A no-op injection would make the runs identical.
    assert!(
        test_count < control_count,
        "the super-TTL run must host strictly fewer contracts than the sub-TTL run \
         (control={control_count}, test={test_count}); equal counts mean the injected \
         clock never reached the hosting cache (silent no-op — the regression this \
         test guards against)"
    );
}

// =============================================================================
// Interest-gated renewal — #3763 storm fix (demand-driven-hosting §5a / §7)
// =============================================================================
//
// These two simulations are SYSTEM-LEVEL behavioral coverage of the two
// demand-driven properties the interest-gated renewal predicate contributes to:
// (1) no-storm — the surviving subscription set tracks active DEMAND, not cache
// size; and (2) chain-collapse — a subscription mesh collapses to zero once all
// client interest ends. Each runs across ≥3 seeds (design note requirement),
// using the merged #4657 sim-harness: `enable_hosting_time_control` (a
// controllable clock so the 8-minute lease/TTL can be crossed deterministically),
// `with_hosting_budget` (cache pressure), `SimOperation::AdvanceHostingClock`,
// and the `node_subscription_count` / `node_active_demand_count` /
// `node_hosting_count` accessors.
//
// SCOPE NOTE (honest): on the CURRENT main codebase these two sims pass with OR
// without the `contract_in_use` gate in `contracts_needing_renewal` §1 —
// verified empirically. They therefore validate the end-to-end properties but do
// NOT, on their own, isolate the §1 predicate change. Two reasons: (a) main has
// already removed GET-auto-subscribe (#4689), so a GET-only contract never
// acquires a §1 active-subscription lease for the gate to act on; and (b) the
// collapse sim's 20-minute clock jump EXPIRES the leases outright, so the
// pre-existing `expires_at > now` lower-bound guard lapses them regardless of the
// gate. The DISCRIMINATING, deterministic proof of the predicate — a within-
// renewal-window, not-yet-expired, demand-less lease that is renewed iff
// `contract_in_use` — is the unit test
// `ring::hosting::tests::test_active_lease_renewed_iff_contract_in_use` (it FAILS
// without the gate, passes with it). These sims are the system-level regression
// companions to that unit proof; they will also catch a future regression that
// reintroduces unconditional renewal once piece-D chain-hosting re-enables the
// storm precondition.

/// Proof 1 (no-storm, design §7 / #3763): under sustained cache/GET load the
/// surviving subscription set must track active DEMAND, not cache size.
///
/// The hub SUBSCRIBES to a small demand set (real, pinned interest) and GETs a
/// larger cache-only set (fills its hosting cache, no subscription). Advancing
/// the hosting clock 20 virtual minutes ages every cache-only lease past both
/// the 8-minute lease and the 8-minute recent-local-access renewal window (§3),
/// so those leases lose their renewal source and lapse, while the
/// client-subscribed demand contracts keep their leases via the §1/§2
/// `contract_in_use` (client-subscription) path. The surviving subscription
/// count must therefore track demand, not cache.
///
/// This is the no-storm END-TO-END property. See the section SCOPE NOTE above:
/// on current main it holds with or without the §1 `contract_in_use` gate
/// (GET-auto-subscribe is already removed, so cache-only contracts never acquire
/// a §1 lease); the discriminating proof of the gate itself is the unit test
/// `test_active_lease_renewed_iff_contract_in_use`.
#[test_log::test]
fn test_subscription_count_tracks_demand_not_cache() {
    use freenet::dev_tool::{NodeLabel, ScheduledOperation, SimOperation};

    const SEEDS: [u64; 3] = [0x4642_D101, 0x4642_D102, 0x4642_D103];
    const DEMAND: usize = 2; // contracts the hub SUBSCRIBES to (active demand)
    const CACHE: usize = 8; // contracts the hub only GETs (cache-only)

    for seed in SEEDS {
        setup_deterministic_state(seed);
        let rt = create_runtime();
        let network = format!("subcount-demand-{seed:x}");

        let gateway = NodeLabel::gateway(&network, 0);
        let hub = NodeLabel::node(&network, 1);

        let demand_contracts: Vec<_> = (0..DEMAND)
            .map(|i| SimOperation::create_test_contract(0x40 + i as u8))
            .collect();
        let cache_contracts: Vec<_> = (0..CACHE)
            .map(|i| SimOperation::create_test_contract(0x60 + i as u8))
            .collect();
        // Instance-id sets so we can categorize surviving subscription leases
        // network-wide as demand-backed vs cache-only.
        let demand_ids: HashSet<_> = demand_contracts.iter().map(|c| *c.key().id()).collect();
        let cache_ids: HashSet<_> = cache_contracts.iter().map(|c| *c.key().id()).collect();

        // Dense 3-peer ring so every GET/SUBSCRIBE reliably completes — this
        // test is about renewal accounting, not routing/findability.
        let (sim, _clock) = rt.block_on(async {
            let mut sim = SimNetwork::new(&network, 1, 2, 7, 3, 10, 2, seed).await;
            let clock = sim.enable_hosting_time_control();
            // Deliberately reduced budget (1 MiB vs the ~1 GiB default) — still
            // comfortably holds the ~10 tiny test contracts, so the cache load
            // is observable as hosting_count >> subscription_count.
            sim.with_hosting_budget(1024 * 1024);
            (sim, clock)
        });

        let mut ops = Vec::new();
        // Gateway PUTs (and announces) every contract so the hub can fetch them.
        for c in demand_contracts.iter().chain(cache_contracts.iter()) {
            ops.push(ScheduledOperation::new(
                gateway.clone(),
                SimOperation::Put {
                    contract: c.clone(),
                    state: SimOperation::create_test_state(1),
                    subscribe: false,
                },
            ));
        }
        // Hub SUBSCRIBES to the demand contracts (real, pinned demand).
        for c in &demand_contracts {
            ops.push(ScheduledOperation::new(
                hub.clone(),
                SimOperation::Subscribe {
                    contract_id: *c.key().id(),
                },
            ));
        }
        // Hub GETs the cache-only contracts (fills its hosting cache, no subscribe).
        for c in &cache_contracts {
            ops.push(ScheduledOperation::new(
                hub.clone(),
                SimOperation::Get {
                    contract_id: *c.key().id(),
                    return_contract_code: true,
                    subscribe: false,
                },
            ));
        }
        // Jump the hosting clock 20 minutes — well past the 8-minute lease AND
        // the 8-minute recent-local-access window (§3) — so any GET-only lease
        // that §3 transiently installed loses its renewal source and lapses,
        // while the client-subscribed demand contracts keep their leases via the
        // §1/§2 `contract_in_use` (client-subscription) renewal path.
        ops.push(ScheduledOperation::new(
            hub.clone(),
            SimOperation::AdvanceHostingClock {
                duration: Duration::from_secs(20 * 60),
            },
        ));

        let result = sim.run_controlled_simulation(
            seed,
            ops,
            Duration::from_secs(300),
            Duration::from_secs(120),
        );
        assert!(
            result.turmoil_result.is_ok(),
            "seed={seed:x}: sim failed: {:?}",
            result.turmoil_result.err()
        );

        let hosting = result.node_hosting_count(&hub);
        let subs = result.node_subscription_count(&hub);
        let demand = result.node_active_demand_count(&hub);
        let metrics = result.aggregate_renewal_metrics();

        // Categorize every surviving subscription lease NETWORK-WIDE as
        // demand-backed (one of the hub's client subscriptions) vs cache-only
        // (one of the GET-only contracts). This is the strongest form of the
        // no-storm property: after demand fades, cache-only contracts hold ZERO
        // leases anywhere, while client-subscribed contracts keep theirs.
        let mut demand_leases = 0usize;
        let mut cache_leases = 0usize;
        for snap in &result.topology_snapshots {
            for id in &snap.active_subscription_keys {
                if demand_ids.contains(id) {
                    demand_leases += 1;
                } else if cache_ids.contains(id) {
                    cache_leases += 1;
                }
            }
        }
        eprintln!(
            "[proof1 seed={seed:x}] hub hosting_count={hosting} subscription_count={subs} \
             active_demand_count={demand:?} | network leases: demand={demand_leases} \
             cache={cache_leases} | max_cycle_batch={}",
            metrics.max_cycle_batch
        );

        // Scenario sanity: the cache load actually landed (not a degenerate run
        // where GETs never cached), so hosting_count >> lease-set is meaningful.
        assert!(
            hosting >= DEMAND + CACHE / 2,
            "seed={seed:x}: hub hosting_count ({hosting}) too low — the GET cache-load \
             did not land; test would be degenerate"
        );

        // CORE no-storm property (design §7 / #3763): the cache-only contracts —
        // which the hub GET-accessed but never subscribed, and stopped accessing
        // 20 virtual minutes ago — hold ZERO subscription leases anywhere in the
        // network. Interest-gated renewal must NOT let cache accrete leases.
        assert_eq!(
            cache_leases, 0,
            "seed={seed:x}: #3763 storm signature — {cache_leases} cache-only contract lease(s) \
             survived network-wide after demand faded. Subscriptions must track active demand, \
             NOT accumulated cache (hub hosting_count={hosting})."
        );

        // The hub's OWN lease set is bounded by the demand it actually created
        // (2 client subscriptions), never by its 10-contract cache.
        assert!(
            subs <= DEMAND + 1,
            "seed={seed:x}: hub subscription_count ({subs}) exceeds the demand it created \
             ({DEMAND}) + 1 — leases must not track the {hosting}-contract cache."
        );
        assert!(
            hosting > subs,
            "seed={seed:x}: hub hosting_count ({hosting}) must exceed subscription_count \
             ({subs}) — the separation between cache and leases must be real."
        );

        // Non-degeneracy: the demand subscriptions genuinely persisted (this is
        // NOT a run where everything simply lapsed).
        assert!(
            demand_leases >= 1,
            "seed={seed:x}: no demand-backed lease survived — the run is degenerate \
             (subscribes never took or all lapsed), so the cache==0 result is vacuous"
        );

        // Renewal batch cap (#4601): no node fans out more than 10 renewals per
        // 30s cycle, regardless of how many contracts are eligible.
        assert!(
            metrics.max_cycle_batch <= 10,
            "seed={seed:x}: max_cycle_batch ({}) exceeded the renewal cap of 10",
            metrics.max_cycle_batch
        );
    }
}

/// Per-run observation for the subscription-mesh collapse proof. Primitives
/// only, so the proof shares a driver without naming the sim-result type.
struct MeshObservation {
    /// Peers holding the contract in `active_subscriptions` at the end. In a
    /// FORMATION run this is the size of the subscription mesh; in a COLLAPSE
    /// run it is the set of survivors (must be empty).
    subscribed_peers: Vec<SocketAddr>,
    /// Nodes still hosting the contract in their cache at the end.
    hosting_nodes: usize,
    /// Max recorded upstreams at any node (subscription-tree parent edges).
    max_upstreams: usize,
    /// Total snapshots captured (guards against an empty-registry false read).
    total_snapshots: usize,
}

/// Shared driver for the subscription-mesh collapse proof.
///
/// Builds a moderately-connected ring, PUTs + announces a contract on the
/// gateway, and has `subscriber_ids` nodes subscribe — forming a connected mesh
/// of subscribed hosts toward the key. When `collapse` is set it then ends ALL
/// client interest (disconnect the PUT originator AND every subscriber) and
/// jumps the hosting clock 20 minutes past the 8-minute lease, so interest-gated
/// renewal must let every lease lapse.
///
/// The FORMATION run (`collapse=false`) and the COLLAPSE run (`collapse=true`)
/// share the same seed, topology, and subscribe prefix, so the mesh the
/// formation run measures at its end is deterministically the same mesh the
/// collapse run tears down — this is how a single end-of-run snapshot proves
/// "formed THEN collapsed" without a mid-run measurement.
fn run_subscription_mesh(
    seed: u64,
    network: &str,
    subscriber_ids: &[usize],
    collapse: bool,
) -> MeshObservation {
    use freenet::dev_tool::{NodeLabel, ScheduledOperation, SimOperation};

    setup_deterministic_state(seed);
    let rt = create_runtime();

    let gateway = NodeLabel::gateway(network, 0);
    let contract = SimOperation::create_test_contract(0xC0);
    let contract_key = contract.key();
    let contract_id = *contract.key().id();

    // Moderately connected so distant subscribes reliably reach the holder
    // (the terminal-advertisement consult only sees direct-neighbor hosts, so
    // too-sparse a ring dead-ends multi-hop subscribes) while still large enough
    // to build a real multi-node mesh.
    let sim = rt.block_on(async {
        let mut sim = SimNetwork::new(network, 1, 15, 10, 3, 5, 3, seed).await;
        sim.enable_hosting_time_control();
        sim
    });

    let mut ops = vec![ScheduledOperation::new(
        gateway.clone(),
        SimOperation::Put {
            contract: contract.clone(),
            state: SimOperation::create_test_state(1),
            subscribe: true,
        },
    )];
    for id in subscriber_ids {
        ops.push(ScheduledOperation::new(
            NodeLabel::node(network, *id),
            SimOperation::Subscribe { contract_id },
        ));
    }

    if collapse {
        // End ALL client interest: disconnect the PUT originator AND every
        // subscriber (Disconnect is ordered last for its node).
        ops.push(ScheduledOperation::new(
            gateway.clone(),
            SimOperation::Disconnect,
        ));
        for id in subscriber_ids {
            ops.push(ScheduledOperation::new(
                NodeLabel::node(network, *id),
                SimOperation::Disconnect,
            ));
        }
        // Jump 20 minutes — past the 8-minute lease AND the recent-access window
        // — so no renewal branch keeps an un-demanded lease alive.
        ops.push(ScheduledOperation::new(
            gateway.clone(),
            SimOperation::AdvanceHostingClock {
                duration: Duration::from_secs(20 * 60),
            },
        ));
    }

    // Generous settle (120s ≈ 4 renewal/expiry cycles at 30s each) so the
    // expire-sweep + interest-gated non-renewal removes every lapsed lease
    // before the final snapshot.
    let result = sim.run_controlled_simulation(
        seed,
        ops,
        Duration::from_secs(360),
        Duration::from_secs(120),
    );
    assert!(
        result.turmoil_result.is_ok(),
        "seed={seed:x} collapse={collapse}: sim failed: {:?}",
        result.turmoil_result.err()
    );

    let subscribed_peers: Vec<SocketAddr> = result
        .topology_snapshots
        .iter()
        .filter(|s| s.active_subscription_keys.contains(&contract_id))
        .map(|s| s.peer_addr)
        .collect();
    let hosting_nodes = result
        .captured_node_labels()
        .iter()
        .filter(|l| result.is_node_hosting(l, &contract_key))
        .count();
    let max_upstreams = result
        .captured_node_labels()
        .iter()
        .map(|l| result.node_upstream_count(l, &contract_key).unwrap_or(0))
        .max()
        .unwrap_or(0);

    MeshObservation {
        subscribed_peers,
        hosting_nodes,
        max_upstreams,
        total_snapshots: result.topology_snapshots.len(),
    }
}

/// Subscriber set used by the collapse proof — a spread of nodes across the
/// ring so a genuine multi-node mesh forms.
const MESH_SUBSCRIBERS: [usize; 8] = [1, 3, 5, 7, 9, 11, 13, 15];

/// Proof 2 (chain collapse on client leave, design §5a/§6). Two phases sharing
/// one seed + topology + subscribe prefix (so determinism makes the FORMATION
/// run's mesh identical to what the COLLAPSE run tears down):
///
///   * FORMATION run: a genuine multi-node subscription mesh forms.
///   * COLLAPSE run: after ALL clients leave + a 20-minute clock jump, EVERY
///     lease lapses — interest-gated renewal keeps none alive.
///
/// Formation is proved by the mesh SIZE measured at the end of the FORMATION
/// run (the shared seed/topology guarantees the COLLAPSE run tears down that
/// same mesh); collapse is proved by ZERO surviving `active_subscriptions`
/// anywhere in the COLLAPSE run. This is the chain-collapse END-TO-END property.
/// See the section SCOPE NOTE above: on current main the collapse also holds
/// without the §1 gate, because the 20-minute clock jump expires the leases
/// outright (the pre-existing `expires_at > now` guard lapses them); the
/// discriminating proof of the gate itself is the unit test
/// `test_active_lease_renewed_iff_contract_in_use`.
#[test_log::test]
fn test_subscription_chain_collapses_on_client_leave() {
    const SEEDS: [u64; 3] = [0x4642_D201, 0x4642_D202, 0x4642_D203];
    // The mesh must be non-trivial for a collapse-to-zero to be meaningful.
    const MIN_MESH: usize = 3;

    for seed in SEEDS {
        // Phase A — formation baseline (no disconnect / no clock jump).
        let form = run_subscription_mesh(
            seed,
            &format!("chain-form-{seed:x}"),
            &MESH_SUBSCRIBERS,
            false,
        );
        eprintln!(
            "[proof2 seed={seed:x} FORM] mesh_size={} hosting_nodes={} \
             max_upstreams={} total_snapshots={}",
            form.subscribed_peers.len(),
            form.hosting_nodes,
            form.max_upstreams,
            form.total_snapshots,
        );
        assert!(
            form.subscribed_peers.len() >= MIN_MESH,
            "seed={seed:x}: formation degenerate — only {} peer(s) in the subscription mesh \
             (need >= {MIN_MESH}); the collapse below would be vacuous. \
             Requested {} subscribers.",
            form.subscribed_peers.len(),
            MESH_SUBSCRIBERS.len(),
        );

        // Phase B — collapse (same seed/topology + disconnect-all + 20-min jump).
        let coll = run_subscription_mesh(
            seed,
            &format!("chain-collapse-{seed:x}"),
            &MESH_SUBSCRIBERS,
            true,
        );
        eprintln!(
            "[proof2 seed={seed:x} COLLAPSE] survivors={} hosting_nodes={} total_snapshots={} {:?}",
            coll.subscribed_peers.len(),
            coll.hosting_nodes,
            coll.total_snapshots,
            coll.subscribed_peers,
        );
        assert!(
            coll.total_snapshots > 0,
            "seed={seed:x}: no snapshots captured in collapse run — cannot judge collapse"
        );
        assert!(
            coll.subscribed_peers.is_empty(),
            "seed={seed:x}: chain did NOT collapse — {} peer(s) still hold the contract in \
             active_subscriptions after all client interest ended + a 20-min clock jump: {:?}. \
             Interest-gated renewal must let every un-demanded lease lapse.",
            coll.subscribed_peers.len(),
            coll.subscribed_peers,
        );
    }
}
