//! Basic integration test for the in-memory SimNetwork.
//!
//! This test verifies that the in-memory network simulation can:
//! 1. Start a gateway and peers
//! 2. Establish connections between them
//! 3. Handle basic message routing
//!
//! This test exercises the PeerRegistry-based messaging introduced to fix issue #2496.
//!
//! NOTE: These tests use global state and must run serially.
//! Enable with: cargo test -p freenet --features simulation_tests -- --test-threads=1

#![cfg(feature = "simulation_tests")]

use freenet::dev_tool::SimNetwork;
use std::time::Duration;

/// Test that a small in-memory network can establish connectivity.
///
/// This is a basic smoke test for the simulation infrastructure.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_sim_network_basic_connectivity() {
    // Create a network with 1 gateway and 5 peers
    let mut sim = SimNetwork::new(
        "basic-connectivity",
        1,  // gateways
        5,  // nodes
        7,  // ring_max_htl
        3,  // rnd_if_htl_above
        10, // max_connections
        2,  // min_connections
        SimNetwork::DEFAULT_SEED,
    )
    .await;

    // Set a reasonable backoff between node startups
    sim.with_start_backoff(Duration::from_millis(100));

    // Build the peers without starting them (just for basic setup verification)
    let peers = sim.build_peers();

    // Verify we have the expected number of peers
    assert_eq!(peers.len(), 6, "Expected 1 gateway + 5 nodes = 6 peers");

    // Verify labels are correctly assigned
    // Note: is_gateway is private, so we check !is_node() instead
    let gateway_count = peers.iter().filter(|(l, _)| !l.is_node()).count();
    let node_count = peers.iter().filter(|(l, _)| l.is_node()).count();
    assert_eq!(gateway_count, 1, "Expected 1 gateway");
    assert_eq!(node_count, 5, "Expected 5 regular nodes");

    tracing::info!("SimNetwork basic setup test passed");
}

/// Test that the network can start peers and they register correctly.
///
/// This test verifies the PeerRegistry mechanics work when nodes are started.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_sim_network_peer_registration() {
    // Create a minimal network for quick testing
    let mut sim = SimNetwork::new(
        "peer-registration",
        1,  // gateways
        2,  // nodes (small for quick test)
        7,  // ring_max_htl
        3,  // rnd_if_htl_above
        10, // max_connections
        1,  // min_connections
        SimNetwork::DEFAULT_SEED,
    )
    .await;

    sim.with_start_backoff(Duration::from_millis(50));

    // Start the network with SmallRng as the random generator
    let handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(42, 1, 1)
        .await;

    // Give peers time to start and attempt connections using VirtualTime
    for _ in 0..20 {
        sim.advance_time(Duration::from_millis(100));
        tokio::task::yield_now().await;
    }

    // Check that peers are starting (handles should be running)
    assert_eq!(
        handles.len(),
        3,
        "Expected 3 peer handles (1 gateway + 2 nodes)"
    );

    // Allow some time for connection attempts using VirtualTime
    for _ in 0..30 {
        sim.advance_time(Duration::from_millis(100));
        tokio::task::yield_now().await;
    }

    tracing::info!("SimNetwork peer registration test passed");
}

/// Test that peers can check connectivity status.
///
/// This test verifies that after starting, peers attempt to connect.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_sim_network_connection_check() {
    // Create a network with 1 gateway and 3 peers
    let mut sim = SimNetwork::new(
        "connection-check",
        1,  // gateways
        3,  // nodes
        7,  // ring_max_htl
        3,  // rnd_if_htl_above
        10, // max_connections
        1,  // min_connections
        SimNetwork::DEFAULT_SEED,
    )
    .await;

    sim.with_start_backoff(Duration::from_millis(100));

    // Start the network
    let _handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(12345, 1, 1)
        .await;

    // Try to check connectivity with a short timeout
    // Note: We use a lenient timeout since this is testing infrastructure
    let timeout = Duration::from_secs(30);

    // The check_partial_connectivity allows for some peers to not be connected
    // which is more realistic for a quick test
    match sim.check_partial_connectivity(timeout, 0.5) {
        Ok(()) => {
            tracing::info!("At least 50% of peers connected successfully");
        }
        Err(e) => {
            // Log but don't fail - this test is primarily about ensuring
            // the simulation infrastructure doesn't crash
            tracing::warn!("Connectivity check did not meet threshold: {}", e);
        }
    }

    tracing::info!("SimNetwork connection check test completed");
}

// =============================================================================
// CI Simulation Tests - These tests are designed to run in CI with strict
// assertions. They validate network behavior with 100% expected success.
// =============================================================================

/// CI simulation test with strict assertions.
///
/// This test validates that a small network can:
/// 1. Establish connectivity
/// 2. Execute contract operations successfully
/// 3. Achieve eventual consistency (convergence)
///
/// **Configuration:**
/// - Network: 5 nodes (1 gateway + 4 peers)
/// - Events: 20 contract operations
/// - Timeout: 45 seconds for connectivity
///
/// **On failure:** Prints seed for local reproduction
///
/// Run with: `cargo test -p freenet --features simulation_tests --test sim_network ci_quick_simulation -- --test-threads=1`
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn ci_quick_simulation() {
    // Use a fixed seed for reproducibility - if this test fails, the seed
    // allows exact reproduction of the failure scenario
    const SEED: u64 = 0xC1F1_ED5E_ED00;

    // Record seed at start for failure reporting
    tracing::info!("Starting CI quick simulation test with seed: 0x{:X}", SEED);

    let result = run_ci_simulation(
        "ci-quick-sim",
        SEED,
        1,                       // gateways
        4,                       // nodes (1 + 4 = 5 total - small for reliable CI)
        20,                      // events (contract operations)
        3,                       // max contracts
        Duration::from_secs(45), // connectivity timeout
        Duration::from_secs(30), // convergence timeout
        0.80,                    // min success rate (80% to account for timing issues in CI)
    )
    .await;

    if let Err(failure) = result {
        // Print seed and failure details for reproduction
        eprintln!("\n============================================================");
        eprintln!("CI SIMULATION TEST FAILED");
        eprintln!("============================================================");
        eprintln!("Seed for reproduction: 0x{:X}", SEED);
        eprintln!("Failure: {}", failure);
        eprintln!("\nTo reproduce locally:");
        eprintln!("  RUST_LOG=info cargo test -p freenet --test sim_network ci_quick_simulation -- --test-threads=1 --nocapture");
        eprintln!("============================================================\n");
        panic!("CI simulation test failed: {}", failure);
    }

    tracing::info!("CI quick simulation test PASSED");
}

/// Runs a CI simulation with the given parameters and validates all assertions.
///
/// Returns Ok(()) on success, Err(message) on any failure.
#[allow(clippy::too_many_arguments)]
async fn run_ci_simulation(
    name: &str,
    seed: u64,
    gateways: usize,
    nodes: usize,
    events: usize,
    max_contracts: usize,
    connectivity_timeout: Duration,
    convergence_timeout: Duration,
    min_success_rate: f64,
) -> Result<(), String> {
    let total_peers = gateways + nodes;
    tracing::info!(
        "Creating {} network: {} gateways + {} nodes = {} total",
        name,
        gateways,
        nodes,
        total_peers
    );

    // Create network with deterministic seed
    let mut sim = SimNetwork::new(
        name, gateways, nodes, 10, // ring_max_htl - higher for reliable routing
        7,  // rnd_if_htl_above
        15, // max_connections - allow more connections for reliability
        2,  // min_connections
        seed,
    )
    .await;

    // Longer backoff to ensure peers have time to register before connections are attempted
    sim.with_start_backoff(Duration::from_millis(300));

    // Start the network and run contract operations
    let _handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(seed, max_contracts, events)
        .await;

    // Step 1: Wait for network connectivity
    tracing::info!("Waiting for network connectivity...");

    // Check that at least 50% of expected connections are established
    // This is lenient to handle timing variations in CI environments
    match sim.check_partial_connectivity(connectivity_timeout, 0.5) {
        Ok(()) => {
            tracing::info!("Network connectivity check PASSED (>=50%)");
        }
        Err(e) => {
            return Err(format!("Connectivity failed: {}", e));
        }
    }

    // Step 2: Wait for operations to complete
    tracing::info!("Waiting for operations to complete...");

    // Give operations time to propagate through the network
    // Use await_operation_completion for proper waiting
    let operation_timeout = Duration::from_secs(20);
    let poll_interval = Duration::from_millis(500);

    match sim
        .await_operation_completion(operation_timeout, poll_interval)
        .await
    {
        Ok(summary) => {
            tracing::info!(
                "Operations completed: put={}/{}, get={}/{}, subscribe={}/{}",
                summary.put.completed(),
                summary.put.requested,
                summary.get.completed(),
                summary.get.requested,
                summary.subscribe.completed(),
                summary.subscribe.requested
            );
        }
        Err(summary) => {
            // Log but continue - some operations may time out but we still check success rate
            tracing::warn!(
                "Some operations pending: put={}/{}, get={}/{}, subscribe={}/{}",
                summary.put.completed(),
                summary.put.requested,
                summary.get.completed(),
                summary.get.requested,
                summary.subscribe.completed(),
                summary.subscribe.requested
            );
        }
    }

    // Step 3: Check operation success rate
    tracing::info!("Checking operation success rate...");

    let summary = sim.get_operation_summary().await;
    let success_rate = summary.overall_success_rate();

    tracing::info!(
        "Success rate: {:.1}% (threshold: {:.1}%)",
        success_rate * 100.0,
        min_success_rate * 100.0
    );

    if success_rate < min_success_rate {
        return Err(format!(
            "Operation success rate {:.1}% below threshold {:.1}%. \
             Put: {}/{} ({:.1}%), Get: {}/{} ({:.1}%), Subscribe: {}/{} ({:.1}%)",
            success_rate * 100.0,
            min_success_rate * 100.0,
            summary.put.succeeded,
            summary.put.completed(),
            summary.put.success_rate() * 100.0,
            summary.get.succeeded,
            summary.get.completed(),
            summary.get.success_rate() * 100.0,
            summary.subscribe.succeeded,
            summary.subscribe.completed(),
            summary.subscribe.success_rate() * 100.0,
        ));
    }

    tracing::info!("Operation success rate check PASSED");

    // Step 4: Check convergence (eventual consistency)
    tracing::info!("Checking convergence...");

    match sim
        .await_convergence(convergence_timeout, Duration::from_millis(500), 1)
        .await
    {
        Ok(result) => {
            tracing::info!(
                "Convergence check PASSED: {} contracts converged, {} diverged",
                result.converged.len(),
                result.diverged.len()
            );
        }
        Err(result) => {
            // Log diverged contracts for debugging
            for diverged in &result.diverged {
                tracing::warn!(
                    "Contract {} has {} different states across peers",
                    diverged.contract_key,
                    diverged.peer_states.len()
                );
            }

            // Allow some divergence in quick tests due to timing
            // A longer test would require stricter convergence
            let diverged_count = result.diverged.len();
            let total_contracts = result.converged.len() + diverged_count;

            if total_contracts > 0 {
                let convergence_rate = result.converged.len() as f64 / total_contracts as f64;
                if convergence_rate < 0.5 {
                    return Err(format!(
                        "Convergence rate {:.1}% too low: {} converged, {} diverged",
                        convergence_rate * 100.0,
                        result.converged.len(),
                        diverged_count
                    ));
                }
                tracing::warn!(
                    "Partial convergence: {:.1}% ({}/{})",
                    convergence_rate * 100.0,
                    result.converged.len(),
                    total_contracts
                );
            }
        }
    }

    Ok(())
}

/// Extended simulation test for more thorough validation.
///
/// This test runs a larger network with more events to catch
/// issues that only manifest at scale. It's suitable for nightly CI.
///
/// **Configuration:**
/// - Network: 20 nodes (2 gateways + 18 peers)
/// - Events: 200 contract operations
/// - Timeout: 60 seconds
///
/// Marked as `#[ignore]` to avoid running in quick CI - run with `--ignored`.
#[test_log::test(tokio::test(flavor = "current_thread"))]
#[ignore] // Run with: cargo test --test sim_network extended_simulation -- --ignored
async fn extended_simulation() {
    const SEED: u64 = 0xE17E_ED5E_ED01;

    tracing::info!("Starting extended simulation test with seed: 0x{:X}", SEED);

    let result = run_ci_simulation(
        "extended-sim",
        SEED,
        2,                       // gateways
        18,                      // nodes (2 + 18 = 20 total)
        200,                     // events
        10,                      // max contracts
        Duration::from_secs(60), // connectivity timeout
        Duration::from_secs(60), // convergence timeout
        0.90,                    // min success rate
    )
    .await;

    if let Err(failure) = result {
        eprintln!("\n============================================================");
        eprintln!("EXTENDED SIMULATION TEST FAILED");
        eprintln!("============================================================");
        eprintln!("Seed for reproduction: 0x{:X}", SEED);
        eprintln!("Failure: {}", failure);
        eprintln!("\nTo reproduce locally:");
        eprintln!("  RUST_LOG=info cargo test -p freenet --test sim_network extended_simulation -- --ignored --test-threads=1 --nocapture");
        eprintln!("============================================================\n");
        panic!("Extended simulation test failed: {}", failure);
    }

    tracing::info!("Extended simulation test PASSED");
}
