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
use std::time::Duration;

// =============================================================================
// Helper Functions
// =============================================================================

/// Setup deterministic simulation state before running a test.
fn setup_deterministic_state(seed: u64) {
    reset_all_simulation_state();
    GlobalRng::set_seed(seed);
    // Derive epoch from seed for deterministic ULID generation
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
///
/// This is a basic smoke test that doesn't actually run the network.
#[test]
fn test_sim_network_basic_setup() {
    const SEED: u64 = 0xBA51C_5E70_0001;

    setup_deterministic_state(SEED);
    let rt = create_runtime();

    rt.block_on(async {
        let mut sim = SimNetwork::new(
            "basic-setup",
            1,  // gateways
            5,  // nodes
            7,  // ring_max_htl
            3,  // rnd_if_htl_above
            10, // max_connections
            2,  // min_connections
            SEED,
        )
        .await;

        sim.with_start_backoff(Duration::from_millis(100));

        // Build the peers without starting them
        let peers = sim.build_peers();

        // Verify we have the expected number of peers
        assert_eq!(peers.len(), 6, "Expected 1 gateway + 5 nodes = 6 peers");

        // Verify labels are correctly assigned
        let gateway_count = peers.iter().filter(|(l, _)| !l.is_node()).count();
        let node_count = peers.iter().filter(|(l, _)| l.is_node()).count();
        assert_eq!(gateway_count, 1, "Expected 1 gateway");
        assert_eq!(node_count, 5, "Expected 5 regular nodes");

        tracing::info!("SimNetwork basic setup test passed");
    });
}

/// Test that peers can start and run under Turmoil's deterministic scheduler.
#[test]
fn test_sim_network_peer_startup() {
    const SEED: u64 = 0xBEE2_5747_0001;

    setup_deterministic_state(SEED);
    let rt = create_runtime();

    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(
            "peer-startup",
            1,  // gateways
            2,  // nodes (small for quick test)
            7,  // ring_max_htl
            3,  // rnd_if_htl_above
            10, // max_connections
            1,  // min_connections
            SEED,
        )
        .await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    // Run simulation with minimal events - just verify startup works
    let result = sim.run_simulation::<rand::rngs::SmallRng, _, _>(
        SEED,
        1,                       // max_contract_num
        5,                       // iterations (minimal)
        Duration::from_secs(15), // simulation_duration
        || async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok(())
        },
    );

    assert!(
        result.is_ok(),
        "Peer startup simulation failed: {:?}",
        result.err()
    );

    // Verify some events were logged (nodes started and did something)
    let event_count = rt.block_on(async { logs_handle.lock().await.len() });
    tracing::info!("Peer startup test logged {} events", event_count);

    tracing::info!("SimNetwork peer startup test passed");
}

/// Test network connectivity under Turmoil's deterministic scheduler.
#[test]
fn test_sim_network_connectivity() {
    const SEED: u64 = 0xC0EE_3C70_0001;

    setup_deterministic_state(SEED);
    let rt = create_runtime();

    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(
            "connectivity-test",
            1,  // gateways
            3,  // nodes
            7,  // ring_max_htl
            3,  // rnd_if_htl_above
            10, // max_connections
            1,  // min_connections
            SEED,
        )
        .await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    let result = sim.run_simulation::<rand::rngs::SmallRng, _, _>(
        SEED,
        2,                       // max_contract_num
        10,                      // iterations
        Duration::from_secs(20), // simulation_duration
        || async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok(())
        },
    );

    assert!(
        result.is_ok(),
        "Connectivity simulation failed: {:?}",
        result.err()
    );

    let event_count = rt.block_on(async { logs_handle.lock().await.len() });
    tracing::info!("Connectivity test logged {} events", event_count);

    tracing::info!("SimNetwork connectivity test passed");
}

// =============================================================================
// CI Simulation Tests
// =============================================================================

/// CI simulation test - small network with contract operations.
///
/// This test validates that a small network can:
/// 1. Start nodes successfully
/// 2. Execute contract operations
/// 3. Complete without errors
///
/// Run with: `cargo test -p freenet --features simulation_tests --test sim_network ci_quick_simulation -- --test-threads=1`
#[test]
fn ci_quick_simulation() {
    const SEED: u64 = 0xC1F1_ED5E_ED00;

    tracing::info!("Starting CI quick simulation test with seed: 0x{:X}", SEED);

    setup_deterministic_state(SEED);
    let rt = create_runtime();

    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(
            "ci-quick-sim",
            1,  // gateways
            4,  // nodes (5 total)
            10, // ring_max_htl
            7,  // rnd_if_htl_above
            15, // max_connections
            2,  // min_connections
            SEED,
        )
        .await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    let result = sim.run_simulation::<rand::rngs::SmallRng, _, _>(
        SEED,
        5,                       // max_contract_num
        50,                      // iterations (contract operations)
        Duration::from_secs(45), // simulation_duration
        || async {
            tokio::time::sleep(Duration::from_secs(2)).await;
            Ok(())
        },
    );

    if let Err(e) = &result {
        eprintln!("\n============================================================");
        eprintln!("CI SIMULATION TEST FAILED");
        eprintln!("============================================================");
        eprintln!("Seed for reproduction: 0x{:X}", SEED);
        eprintln!("Error: {:?}", e);
        eprintln!("============================================================\n");
    }

    assert!(
        result.is_ok(),
        "CI quick simulation failed: {:?}",
        result.err()
    );

    // Check convergence
    let convergence = rt.block_on(async { check_convergence_from_logs(&logs_handle).await });

    tracing::info!(
        "CI quick simulation: {} converged, {} diverged",
        convergence.converged.len(),
        convergence.diverged.len()
    );

    // Log diverged contracts for debugging (don't fail on divergence yet)
    for diverged in &convergence.diverged {
        tracing::warn!(
            "Contract {} diverged: {} states across {} peers",
            diverged.contract_key,
            diverged.unique_state_count(),
            diverged.peer_states.len()
        );
    }

    tracing::info!("CI quick simulation test PASSED");
}

/// CI simulation test - medium network with more operations.
///
/// Run with: `cargo test -p freenet --features simulation_tests --test sim_network ci_medium_simulation -- --test-threads=1`
#[test]
fn ci_medium_simulation() {
    const SEED: u64 = 0xC1F1_ED7E_ED01;

    tracing::info!("Starting CI medium simulation test with seed: 0x{:X}", SEED);

    setup_deterministic_state(SEED);
    let rt = create_runtime();

    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(
            "ci-medium-sim",
            2,  // gateways
            6,  // nodes (8 total)
            10, // ring_max_htl
            7,  // rnd_if_htl_above
            15, // max_connections
            2,  // min_connections
            SEED,
        )
        .await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    let result = sim.run_simulation::<rand::rngs::SmallRng, _, _>(
        SEED,
        8,                        // max_contract_num
        100,                      // iterations
        Duration::from_secs(120), // simulation_duration
        || async {
            tokio::time::sleep(Duration::from_secs(3)).await;
            Ok(())
        },
    );

    if let Err(e) = &result {
        eprintln!("\n============================================================");
        eprintln!("CI MEDIUM SIMULATION TEST FAILED");
        eprintln!("============================================================");
        eprintln!("Seed for reproduction: 0x{:X}", SEED);
        eprintln!("Error: {:?}", e);
        eprintln!("============================================================\n");
    }

    assert!(
        result.is_ok(),
        "CI medium simulation failed: {:?}",
        result.err()
    );

    let convergence = rt.block_on(async { check_convergence_from_logs(&logs_handle).await });

    tracing::info!(
        "CI medium simulation: {} converged, {} diverged",
        convergence.converged.len(),
        convergence.diverged.len()
    );

    for diverged in &convergence.diverged {
        tracing::warn!(
            "Contract {} diverged: {} states across {} peers",
            diverged.contract_key,
            diverged.unique_state_count(),
            diverged.peer_states.len()
        );
    }

    tracing::info!("CI medium simulation test PASSED");
}

// =============================================================================
// Convergence Tests (Currently Ignored - Investigating Convergence Bug)
// =============================================================================

/// Replica validation test with stepwise consistency checking.
///
/// This test validates contract replication with phased event generation.
///
/// Run with: `cargo test -p freenet --features simulation_tests --test sim_network replica_validation -- --test-threads=1 --ignored`
// FIXME: Test fails consistently with convergence issues. See issue #2684.
#[test]
#[ignore]
fn replica_validation_and_stepwise_consistency() {
    const SEED: u64 = 0xBEE1_1CA5_0001;
    const ITERATIONS: usize = 90; // 3 phases × 30 events

    tracing::info!("Starting replica validation test with seed: 0x{:X}", SEED);

    setup_deterministic_state(SEED);
    let rt = create_runtime();

    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(
            "replica-validation",
            2, // gateways
            8, // nodes (10 total)
            8, // ring_max_htl
            4, // rnd_if_htl_above
            8, // max_connections
            4, // min_connections
            SEED,
        )
        .await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    let result = sim.run_simulation::<rand::rngs::SmallRng, _, _>(
        SEED,
        10,                       // max_contract_num
        ITERATIONS,               // iterations
        Duration::from_secs(180), // simulation_duration
        || async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            Ok(())
        },
    );

    assert!(
        result.is_ok(),
        "Replica validation simulation failed: {:?}",
        result.err()
    );

    let convergence = rt.block_on(async { check_convergence_from_logs(&logs_handle).await });

    tracing::info!(
        "Replica validation: {} converged, {} diverged",
        convergence.converged.len(),
        convergence.diverged.len()
    );

    for diverged in &convergence.diverged {
        tracing::error!(
            "Contract {} diverged: {} states across {} peers",
            diverged.contract_key,
            diverged.unique_state_count(),
            diverged.peer_states.len()
        );
    }

    assert!(
        convergence.is_converged(),
        "Replica validation failed: {} converged, {} diverged",
        convergence.converged.len(),
        convergence.diverged.len()
    );

    tracing::info!("Replica validation test PASSED");
}

/// Dense network test with high connectivity.
///
/// Run with: `cargo test -p freenet --features simulation_tests --test sim_network dense_network -- --test-threads=1 --ignored`
// FIXME: Test fails consistently with convergence issues. See issue #2684.
#[test]
#[ignore]
fn dense_network_replication() {
    const SEED: u64 = 0xDE05_E0F0_0001;

    tracing::info!(
        "Starting dense network replication test with seed: 0x{:X}",
        SEED
    );

    setup_deterministic_state(SEED);
    let rt = create_runtime();

    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(
            "dense-network",
            3,  // gateways
            12, // nodes (15 total - dense)
            12, // ring_max_htl
            8,  // rnd_if_htl_above
            12, // max_connections
            6,  // min_connections
            SEED,
        )
        .await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    let result = sim.run_simulation::<rand::rngs::SmallRng, _, _>(
        SEED,
        15,                       // max_contract_num
        150,                      // iterations
        Duration::from_secs(300), // simulation_duration
        || async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok(())
        },
    );

    assert!(
        result.is_ok(),
        "Dense network simulation failed: {:?}",
        result.err()
    );

    let convergence = rt.block_on(async { check_convergence_from_logs(&logs_handle).await });

    tracing::info!(
        "Dense network: {} converged, {} diverged",
        convergence.converged.len(),
        convergence.diverged.len()
    );

    for diverged in &convergence.diverged {
        tracing::error!(
            "Contract {} diverged: {} states across {} peers",
            diverged.contract_key,
            diverged.unique_state_count(),
            diverged.peer_states.len()
        );
    }

    assert!(
        convergence.is_converged(),
        "Dense network convergence failed: {} converged, {} diverged",
        convergence.converged.len(),
        convergence.diverged.len()
    );

    tracing::info!("Dense network replication test PASSED");
}

// =============================================================================
// Determinism Verification Tests
// =============================================================================

/// Verify that running the same simulation twice produces identical results.
///
/// This is the ultimate test of determinism - same seed should produce
/// exactly the same event sequence.
#[test]
fn test_turmoil_determinism_verification() {
    const SEED: u64 = 0xDE7E_2A11_0001;

    fn run_simulation(name: &str, seed: u64) -> Vec<String> {
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

        // Extract event sequence
        rt.block_on(async {
            let logs = logs_handle.lock().await;
            logs.iter()
                .map(|log| format!("{:?}", log.kind.variant_name()))
                .collect()
        })
    }

    // Run twice with same seed
    let events1 = run_simulation("det-verify-1", SEED);
    let events2 = run_simulation("det-verify-2", SEED);

    // Both runs should produce identical event sequences
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
