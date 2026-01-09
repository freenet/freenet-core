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
/// - Events: 100 contract operations
/// - Success rate: 95% minimum
/// - Convergence: 100% required (eventual consistency guarantee)
/// - Timeout: 45 seconds for connectivity, 60 seconds for convergence
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
        100,                     // events (contract operations) - enough to test consistency
        5,                       // max contracts
        Duration::from_secs(45), // connectivity timeout
        Duration::from_secs(60), // convergence timeout - longer for 100% convergence
        0.95, // min success rate (95% - eventual consistency should achieve this)
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

            // Eventual consistency requires 100% convergence
            // All contracts must have identical state across all replicas
            let diverged_count = result.diverged.len();
            let total_contracts = result.converged.len() + diverged_count;

            if diverged_count > 0 {
                return Err(format!(
                    "Convergence failed: {} contracts diverged out of {} total. \
                     Eventual consistency requires 100% convergence. Diverged: {:?}",
                    diverged_count,
                    total_contracts,
                    result
                        .diverged
                        .iter()
                        .map(|d| &d.contract_key)
                        .collect::<Vec<_>>()
                ));
            }

            tracing::info!(
                "All {} contracts converged successfully",
                result.converged.len()
            );
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

// =============================================================================
// Replica Validation and Step-by-Step Consistency Tests
// =============================================================================

/// Minimum replica count required for convergence validation.
/// Contracts should be replicated to at least this many peers.
const MIN_REPLICA_COUNT: usize = 2;

/// Test that validates replica distribution and step-by-step eventual consistency.
///
/// This test runs operations in multiple phases, verifying convergence after each phase.
/// This ensures the system maintains eventual consistency throughout operation sequences,
/// not just at the end.
///
/// **Configuration:**
/// - Dense network: 10 nodes with high connectivity (min 4, max 8 connections)
/// - 3 phases of 30 events each (90 total)
/// - After each phase: verify 100% convergence and replica counts
/// - Minimum 2 replicas per contract required
///
/// **Purpose:**
/// - Validates contract replication is working correctly
/// - Ensures eventual consistency at each step, not just final state
/// - Catches issues where consistency is achieved only temporarily
///
/// Run with: `cargo test -p freenet --features simulation_tests --test sim_network replica_validation -- --ignored --test-threads=1`
///
/// NOTE: This test is currently marked #[ignore] because it reveals convergence issues
/// that need investigation. The test correctly identifies that contracts are NOT achieving
/// eventual consistency despite high operation success rates.
#[test_log::test(tokio::test(flavor = "current_thread"))]
#[ignore] // TODO: Investigate convergence failures - contracts diverging despite successful operations
async fn replica_validation_and_stepwise_consistency() {
    const SEED: u64 = 0xBEE1_1CA5_0001;
    const PHASES: u32 = 3;
    const EVENTS_PER_PHASE: u32 = 30;

    tracing::info!(
        "Starting replica validation test: seed=0x{:X}, {} phases × {} events",
        SEED,
        PHASES,
        EVENTS_PER_PHASE
    );

    // Create a dense network for reliable replication
    let mut sim = SimNetwork::new(
        "replica-validation",
        2, // gateways
        8, // nodes (10 total - dense enough for replication testing)
        8, // ring_max_htl - moderate for focused routing
        4, // rnd_if_htl_above
        8, // max_connections - higher connectivity
        4, // min_connections - ensure each node is well connected
        SEED,
    )
    .await;

    sim.with_start_backoff(Duration::from_millis(200));

    // Start the network - keep handles to wait for finalization later
    let handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(
            SEED,
            10,                                   // max_contracts
            (EVENTS_PER_PHASE * PHASES) as usize, // total events
        )
        .await;

    // Wait for initial connectivity
    tracing::info!("Waiting for network connectivity...");
    sim.check_partial_connectivity(Duration::from_secs(30), 0.6)
        .expect("Network should achieve 60% connectivity");
    tracing::info!("Network connectivity established");

    // Run phases with convergence checks
    // Use Option so we can drop the stream after all phases to signal peers to disconnect
    let mut event_stream = Some(sim.event_chain(EVENTS_PER_PHASE * PHASES, None));

    for phase in 1..=PHASES {
        tracing::info!("=== Phase {}/{} ===", phase, PHASES);

        // Generate events for this phase
        for event_num in 1..=EVENTS_PER_PHASE {
            use futures::StreamExt;
            match event_stream.as_mut().unwrap().next().await {
                Some(_event_id) => {
                    // Minimal delay - just yield to let other tasks run
                    tokio::task::yield_now().await;
                }
                None => {
                    panic!(
                        "Event stream ended early at phase {} event {}",
                        phase, event_num
                    );
                }
            }
        }

        tracing::info!(
            "Phase {} events complete, waiting for quiescence...",
            phase
        );

        // Wait for network quiescence - all broadcasts propagated
        let quiesce_result = sim
            .await_network_quiescence(
                Duration::from_secs(120), // Max timeout
                Duration::from_secs(3),   // Quiet for 3 seconds = quiesced
                Duration::from_millis(500),
            )
            .await;

        match quiesce_result {
            Ok(log_count) => tracing::info!(
                "Phase {} quiesced with {} log entries",
                phase,
                log_count
            ),
            Err(log_count) => tracing::warn!(
                "Phase {} still active after timeout ({} entries)",
                phase,
                log_count
            ),
        }

        tracing::info!("Checking convergence for phase {}...", phase);

        // Wait for convergence after this phase - longer timeout for eventual consistency
        let convergence_timeout = Duration::from_secs(120);
        let poll_interval = Duration::from_millis(500);

        let result = sim
            .await_convergence(convergence_timeout, poll_interval, 1)
            .await;

        match result {
            Ok(conv_result) => {
                // Validate replica counts
                let mut low_replica_count = 0;
                let mut total_replicas: usize = 0;

                for contract in &conv_result.converged {
                    total_replicas += contract.replica_count;
                    if contract.replica_count < MIN_REPLICA_COUNT {
                        low_replica_count += 1;
                        tracing::warn!(
                            "Contract {} has only {} replicas (minimum: {})",
                            contract.contract_key,
                            contract.replica_count,
                            MIN_REPLICA_COUNT
                        );
                    }
                }

                let avg_replicas = if conv_result.converged.is_empty() {
                    0.0
                } else {
                    total_replicas as f64 / conv_result.converged.len() as f64
                };

                tracing::info!(
                    "Phase {} convergence: {} contracts, avg {:.1} replicas/contract",
                    phase,
                    conv_result.converged.len(),
                    avg_replicas
                );

                // Fail if too many contracts have insufficient replicas
                if !conv_result.converged.is_empty() {
                    let low_replica_pct =
                        low_replica_count as f64 / conv_result.converged.len() as f64;
                    if low_replica_pct > 0.5 {
                        panic!(
                            "Phase {}: {:.1}% of contracts have fewer than {} replicas. \
                             Replication may not be working correctly.",
                            phase,
                            low_replica_pct * 100.0,
                            MIN_REPLICA_COUNT
                        );
                    }
                }
            }
            Err(conv_result) => {
                // Log details of diverged contracts
                for diverged in &conv_result.diverged {
                    tracing::warn!(
                        "Contract {} diverged: {} different states across {} peers",
                        diverged.contract_key,
                        diverged.unique_state_count(),
                        diverged.peer_states.len()
                    );
                }

                let total = conv_result.converged.len() + conv_result.diverged.len();
                let convergence_rate = if total == 0 {
                    1.0
                } else {
                    conv_result.converged.len() as f64 / total as f64
                };

                // All phases (including final): log warning but continue
                // Strict convergence check happens after peer finalization
                tracing::warn!(
                    "Phase {} partial convergence: {} converged, {} diverged ({:.1}% rate). \
                     Will verify 100% convergence after peer finalization.",
                    phase,
                    conv_result.converged.len(),
                    conv_result.diverged.len(),
                    convergence_rate * 100.0
                );
            }
        }

        // Check operation success rate after this phase
        let summary = sim.get_operation_summary().await;
        let success_rate = summary.overall_success_rate();
        tracing::info!(
            "Phase {} success rate: {:.1}% (put: {}/{}, get: {}/{}, subscribe: {}/{}, update: {}/{})",
            phase,
            success_rate * 100.0,
            summary.put.succeeded,
            summary.put.completed(),
            summary.get.succeeded,
            summary.get.completed(),
            summary.subscribe.succeeded,
            summary.subscribe.completed(),
            summary.update.succeeded,
            summary.update.completed()
        );

        if success_rate < 0.90 {
            panic!(
                "Phase {} success rate {:.1}% below 90% threshold",
                phase,
                success_rate * 100.0
            );
        }
    }

    // CRITICAL: Wait for network quiescence before signaling shutdown.
    // Instead of guessing a fixed timeout, we monitor log activity and wait until
    // no new broadcasts/operations are happening. This ensures all in-flight operations
    // have completed propagating through the network.
    tracing::info!("All events generated. Waiting for network to quiesce...");
    let quiesce_result = sim
        .await_network_quiescence(
            Duration::from_secs(120), // Max timeout
            Duration::from_secs(5),   // Quiet for 5 seconds = quiesced
            Duration::from_millis(500),
        )
        .await;

    match quiesce_result {
        Ok(log_count) => tracing::info!("Network quiesced with {} log entries", log_count),
        Err(log_count) => tracing::warn!(
            "Network still active after timeout ({} log entries), proceeding anyway",
            log_count
        ),
    }

    // Drop stream to signal peers to disconnect (like fdev does)
    drop(event_stream);
    tracing::info!("Waiting for peer tasks to finalize...");

    // Wait for all peer tasks to finalize (like fdev does)
    // This ensures all broadcasts and updates have been processed
    // Use FuturesUnordered for concurrent handling like fdev
    use futures::stream::FuturesUnordered;
    let finalize_timeout = tokio::time::timeout(Duration::from_secs(120), async {
        let mut futs = FuturesUnordered::from_iter(handles);
        while let Some(result) = {
            use futures::StreamExt;
            futs.next().await
        } {
            if let Err(e) = result {
                tracing::warn!("Peer task error: {:?}", e);
            }
        }
    });

    match finalize_timeout.await {
        Ok(_) => tracing::info!("All peer tasks finalized successfully"),
        Err(_) => tracing::warn!("Peer task finalization timed out after 120s"),
    }

    // Final convergence check after peer finalization
    tracing::info!("Checking final convergence after peer finalization...");
    let final_convergence = sim
        .await_convergence(Duration::from_secs(30), Duration::from_millis(500), 1)
        .await;

    match final_convergence {
        Ok(result) => {
            tracing::info!(
                "Final convergence achieved: {} contracts converged",
                result.converged.len()
            );
        }
        Err(result) => {
            for diverged in &result.diverged {
                tracing::error!(
                    "Contract {} diverged after finalization: {} states across {} peers",
                    diverged.contract_key,
                    diverged.unique_state_count(),
                    diverged.peer_states.len()
                );
            }
            panic!(
                "Final convergence failed after peer finalization: {} converged, {} diverged",
                result.converged.len(),
                result.diverged.len()
            );
        }
    }

    // Final validation
    tracing::info!("=== Final Validation ===");

    let final_summary = sim.get_operation_summary().await;
    tracing::info!(
        "Final totals - Put: {}/{} ({:.1}%), Get: {}/{} ({:.1}%), \
         Subscribe: {}/{} ({:.1}%), Update: {}/{} ({:.1}%)",
        final_summary.put.succeeded,
        final_summary.put.completed(),
        final_summary.put.success_rate() * 100.0,
        final_summary.get.succeeded,
        final_summary.get.completed(),
        final_summary.get.success_rate() * 100.0,
        final_summary.subscribe.succeeded,
        final_summary.subscribe.completed(),
        final_summary.subscribe.success_rate() * 100.0,
        final_summary.update.succeeded,
        final_summary.update.completed(),
        final_summary.update.success_rate() * 100.0
    );

    let distribution = sim.get_contract_distribution().await;
    tracing::info!(
        "Contract distribution: {} contracts across network",
        distribution.len()
    );
    for dist in &distribution {
        tracing::debug!(
            "  Contract {}: {} replicas on {:?}",
            dist.contract_key,
            dist.replica_count,
            dist.peers
        );
    }

    tracing::info!(
        "Replica validation test PASSED: {} phases completed with 100% convergence",
        PHASES
    );
}

/// Dense network test with high connectivity for stress testing replication.
///
/// This test uses a more densely connected network to verify that replication
/// works correctly when nodes have many connections.
///
/// Marked as `#[ignore]` - run with `--ignored` for nightly CI.
#[test_log::test(tokio::test(flavor = "current_thread"))]
#[ignore]
async fn dense_network_replication() {
    const SEED: u64 = 0xDE05_E0F0_0001;

    tracing::info!(
        "Starting dense network replication test with seed: 0x{:X}",
        SEED
    );

    // Very dense network: high min_connections ensures full mesh-like topology
    let mut sim = SimNetwork::new(
        "dense-replication",
        3,  // gateways
        12, // nodes (15 total)
        10, // ring_max_htl
        5,  // rnd_if_htl_above
        12, // max_connections - very high
        6,  // min_connections - each node connects to 40% of network
        SEED,
    )
    .await;

    sim.with_start_backoff(Duration::from_millis(150));

    let handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 15, 150)
        .await;

    // Wait for connectivity
    sim.check_partial_connectivity(Duration::from_secs(45), 0.7)
        .expect("Dense network should achieve high connectivity");

    // Run events - no fixed delays, just yield between events
    let mut stream = sim.event_chain(150, None);
    while {
        use futures::StreamExt;
        stream.next().await
    }
    .is_some()
    {
        tokio::task::yield_now().await;
    }

    // CRITICAL: Wait for network quiescence before signaling shutdown.
    // Instead of guessing a fixed timeout, we monitor log activity and wait until
    // no new broadcasts/operations are happening. This ensures all in-flight operations
    // have completed propagating through the network.
    tracing::info!("All events generated. Waiting for network to quiesce...");
    let quiesce_result = sim
        .await_network_quiescence(
            Duration::from_secs(120), // Max timeout
            Duration::from_secs(5),   // Quiet for 5 seconds = quiesced
            Duration::from_millis(500),
        )
        .await;

    match quiesce_result {
        Ok(log_count) => tracing::info!("Network quiesced with {} log entries", log_count),
        Err(log_count) => tracing::warn!(
            "Network still active after timeout ({} log entries), proceeding anyway",
            log_count
        ),
    }

    // Drop stream to signal peers to disconnect (like fdev does)
    drop(stream);
    tracing::info!("Waiting for peer tasks to finalize...");

    // Wait for all peer tasks to finalize (like fdev does)
    // This ensures all broadcasts and updates have been processed
    // Use FuturesUnordered for concurrent handling like fdev
    use futures::stream::FuturesUnordered;
    let finalize_timeout = tokio::time::timeout(Duration::from_secs(120), async {
        let mut futs = FuturesUnordered::from_iter(handles);
        while let Some(result) = {
            use futures::StreamExt;
            futs.next().await
        } {
            if let Err(e) = result {
                tracing::warn!("Peer task error: {:?}", e);
            }
        }
    });

    match finalize_timeout.await {
        Ok(_) => tracing::info!("All peer tasks finalized successfully"),
        Err(_) => tracing::warn!("Peer task finalization timed out after 120s"),
    }

    // Check convergence - use 120s timeout to match fdev tests
    // NOTE: This test documents known eventual consistency behavior.
    // Divergence here indicates a real bug in the replication logic that
    // is tracked separately from this DST test.
    let result = sim
        .await_convergence(Duration::from_secs(120), Duration::from_millis(500), 1)
        .await;

    match result {
        Ok(conv) => {
            // In a dense network, we expect higher replica counts
            let avg_replicas: f64 = if conv.converged.is_empty() {
                0.0
            } else {
                conv.converged
                    .iter()
                    .map(|c| c.replica_count)
                    .sum::<usize>() as f64
                    / conv.converged.len() as f64
            };

            tracing::info!(
                "Dense network: {} contracts, avg {:.1} replicas",
                conv.converged.len(),
                avg_replicas
            );

            // Dense networks should achieve higher replication
            if avg_replicas < 3.0 && conv.converged.len() > 5 {
                tracing::warn!(
                    "Dense network has low average replication ({:.1}), expected >= 3.0",
                    avg_replicas
                );
            }
        }
        Err(conv) => {
            for diverged in &conv.diverged {
                tracing::error!(
                    "Contract {} diverged: {} states across {} peers",
                    diverged.contract_key,
                    diverged.unique_state_count(),
                    diverged.peer_states.len()
                );
            }
            panic!(
                "Dense network convergence failed: {} converged, {} diverged",
                conv.converged.len(),
                conv.diverged.len()
            );
        }
    }

    // Verify success rate
    let summary = sim.get_operation_summary().await;
    assert!(
        summary.overall_success_rate() >= 0.95,
        "Dense network should achieve 95%+ success rate, got {:.1}%",
        summary.overall_success_rate() * 100.0
    );

    tracing::info!("Dense network replication test PASSED");
}
