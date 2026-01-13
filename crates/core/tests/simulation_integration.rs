//! Deterministic simulation tests using Turmoil.
//!
//! These tests use `run_simulation()` which runs under Turmoil's deterministic scheduler.
//! Same seed MUST produce identical results across runs.
//!
//! NOTE: These tests use global state (socket registries, RNG) and must run serially.
//! Enable with: cargo test -p freenet --features simulation_tests --test simulation_integration -- --test-threads=1
//!
//! For non-deterministic smoke tests, see `simulation_smoke.rs`.

#![cfg(feature = "simulation_tests")]

use std::collections::HashMap;
use std::time::Duration;

// =============================================================================
// STRICT Determinism Test - Exact Event Equality
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
    use freenet::dev_tool::SimNetwork;

    const SEED: u64 = 0xDE7E_2A1E_1234;

    /// Captures all simulation state for comparison
    #[derive(Debug, PartialEq)]
    struct SimulationTrace {
        event_counts: HashMap<String, usize>,
        event_sequence: Vec<String>, // event_kind names in order
        total_events: usize,
    }

    fn run_and_trace(name: &str, seed: u64) -> (turmoil::Result, SimulationTrace) {
        use freenet::config::{GlobalRng, GlobalSimulationTime};

        // Reset all global simulation state for determinism
        freenet::dev_tool::reset_all_simulation_state();

        // Set seed BEFORE SimNetwork::new() since it uses GlobalRng for keypair generation
        GlobalRng::set_seed(seed);
        // Derive epoch from seed for deterministic ULID generation
        const BASE_EPOCH_MS: u64 = 1577836800000; // 2020-01-01 00:00:00 UTC
        const RANGE_MS: u64 = 5 * 365 * 24 * 60 * 60 * 1000; // ~5 years
        GlobalSimulationTime::set_time_ms(BASE_EPOCH_MS + (seed % RANGE_MS));

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

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

// =============================================================================
// STRICT Determinism Test - Multi-Gateway
// =============================================================================

/// **STRICT** determinism test with MULTIPLE GATEWAYS.
///
/// This test verifies that simulations with 2+ gateways remain deterministic.
#[test_log::test]
fn test_strict_determinism_multi_gateway() {
    use freenet::dev_tool::SimNetwork;

    const SEED: u64 = 0xAB17_6A7E_1234;

    #[derive(Debug, PartialEq)]
    struct SimulationTrace {
        event_counts: HashMap<String, usize>,
        event_sequence: Vec<String>,
        total_events: usize,
    }

    fn run_and_trace(name: &str, seed: u64) -> (turmoil::Result, SimulationTrace) {
        use freenet::config::{GlobalRng, GlobalSimulationTime};

        freenet::dev_tool::reset_all_simulation_state();
        GlobalRng::set_seed(seed);
        const BASE_EPOCH_MS: u64 = 1577836800000;
        const RANGE_MS: u64 = 5 * 365 * 24 * 60 * 60 * 1000;
        GlobalSimulationTime::set_time_ms(BASE_EPOCH_MS + (seed % RANGE_MS));

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

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

// =============================================================================
// Deterministic Replay Events
// =============================================================================

/// **STRICT** determinism test: verifies that same seed produces identical replay.
#[test_log::test]
fn test_deterministic_replay_events() {
    use freenet::config::{GlobalRng, GlobalSimulationTime};
    use freenet::dev_tool::SimNetwork;

    const SEED: u64 = 0xDEAD_BEEF_1234;

    #[derive(Debug, PartialEq)]
    struct ReplayTrace {
        event_counts: HashMap<String, usize>,
        total_events: usize,
    }

    fn run_and_trace(name: &str, seed: u64) -> (turmoil::Result, ReplayTrace) {
        freenet::dev_tool::reset_all_simulation_state();
        GlobalRng::set_seed(seed);
        const BASE_EPOCH_MS: u64 = 1577836800000;
        const RANGE_MS: u64 = 5 * 365 * 24 * 60 * 60 * 1000;
        GlobalSimulationTime::set_time_ms(BASE_EPOCH_MS + (seed % RANGE_MS));

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

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
// Graceful Shutdown Regression Tests
// =============================================================================

/// Regression test for graceful shutdown using Turmoil deterministic simulation.
#[test_log::test]
fn test_graceful_shutdown_no_deadlock() {
    use freenet::dev_tool::SimNetwork;

    const SEED: u64 = 0xDEAD_BEEF_CAFE;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

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
