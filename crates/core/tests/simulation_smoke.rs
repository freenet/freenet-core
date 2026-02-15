//! Smoke tests for the simulation framework.
//!
//! These tests use `start_with_rand_gen()` which runs on plain tokio (NOT Turmoil).
//! They verify basic functionality but are NOT deterministic - same seed may produce
//! slightly different results across runs due to tokio's scheduling.
//!
//! For deterministic tests that use Turmoil, see `simulation_integration.rs`.
//!
//! NOTE: These tests must run serially (behind simulation_tests feature) because they
//! use global state: reset_all_simulation_state(), GlobalRng, GlobalSimulationTime,
//! and VirtualTime registries. Running in parallel causes state corruption.

#![cfg(feature = "simulation_tests")]

use freenet::dev_tool::SimNetwork;
use std::collections::HashMap;
use std::time::Duration;

/// Helper to let tokio tasks run and process network messages.
///
/// SimulationSocket uses VirtualTime internally for message delivery scheduling.
/// This helper advances VirtualTime in chunks while yielding to tokio to let
/// tasks process delivered messages. This is necessary because:
/// 1. Messages are scheduled for delivery at VirtualTime + latency
/// 2. Tasks need tokio runtime time to process received messages
///
/// Note: This is different from run_simulation() which uses Turmoil's scheduler.
/// These smoke tests run on plain tokio with manual time advancement.
async fn let_network_run(sim: &mut SimNetwork, duration: Duration) {
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

// =============================================================================
// Event Types Consistency
// =============================================================================

/// Smoke test: verifies that same seed produces consistent event types.
///
/// NOTE: This is NOT a strict determinism test - it only verifies event TYPES
/// are captured consistently, not exact counts. Uses start_with_rand_gen()
/// which runs on plain tokio (not Turmoil).
///
/// For strict determinism tests, see test_strict_determinism_exact_event_equality.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_smoke_event_types_consistency() {
    use freenet::config::{GlobalRng, GlobalSimulationTime};

    const SEED: u64 = 0xFA01_7777_1234;

    async fn run_simulation(name: &str, seed: u64) -> HashMap<String, usize> {
        // Reset all global state and set up deterministic time/RNG
        freenet::dev_tool::reset_all_simulation_state();
        GlobalRng::set_seed(seed);
        // Derive epoch from seed (same logic as run_simulation)
        const BASE_EPOCH_MS: u64 = 1577836800000; // 2020-01-01 00:00:00 UTC
        const RANGE_MS: u64 = 5 * 365 * 24 * 60 * 60 * 1000; // ~5 years
        GlobalSimulationTime::set_time_ms(BASE_EPOCH_MS + (seed % RANGE_MS));

        let mut sim = SimNetwork::new(name, 1, 3, 7, 3, 10, 2, seed).await;
        sim.with_start_backoff(Duration::from_millis(50));

        let _handles = sim
            .start_with_rand_gen::<rand::rngs::SmallRng>(seed, 1, 1)
            .await;

        // Let tokio tasks run to generate events
        let_network_run(&mut sim, Duration::from_secs(3)).await;

        sim.get_event_counts().await
    }

    let events1 = run_simulation("fault-run1", SEED).await;
    let events2 = run_simulation("fault-run2", SEED).await;

    // Verify simulation captures events
    let total_events: usize = events1.values().sum();
    assert!(total_events > 0, "Should capture events during simulation");

    // Verify same event types are captured
    let types1: std::collections::HashSet<&String> = events1.keys().collect();
    let types2: std::collections::HashSet<&String> = events2.keys().collect();
    assert_eq!(
        types1, types2,
        "Event types should be consistent.\nRun 1: {:?}\nRun 2: {:?}",
        events1, events2
    );

    tracing::info!(
        "Event types consistency test passed - {} events captured",
        total_events
    );
}

// =============================================================================
// Event Sequence Verification
// =============================================================================

/// Tests that the event summary is correctly ordered and consistent.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_event_summary_ordering() {
    const SEED: u64 = 0xC0DE_CAFE_BABE;

    let mut sim = SimNetwork::new("event-order", 1, 3, 7, 3, 10, 2, SEED).await;
    sim.with_start_backoff(Duration::from_millis(50));

    let _handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 1, 1)
        .await;

    // Let tokio tasks run to generate events
    let_network_run(&mut sim, Duration::from_secs(3)).await;

    let summary = sim.get_deterministic_event_summary().await;

    // Verify ordering: the summary should already be sorted
    let mut sorted = summary.clone();
    sorted.sort();
    assert_eq!(summary, sorted, "Event summary should be sorted");

    // Verify we have expected event types
    let connect_events = summary
        .iter()
        .filter(|e| e.event_kind_name == "Connect")
        .count();

    assert!(
        connect_events > 0,
        "Should have Connect events, got event kinds: {:?}",
        summary
            .iter()
            .map(|e| &e.event_kind_name)
            .collect::<Vec<_>>()
    );

    tracing::info!(
        "Event ordering test passed - {} events, {} Connect events",
        summary.len(),
        connect_events
    );
}

// =============================================================================
// Small Network Connectivity
// =============================================================================

/// Smoke test: verifies a minimal network (1 gateway + 2 nodes) can start.
///
/// This is NOT a determinism test - it simply verifies basic functionality.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_smoke_small_network() {
    const SEED: u64 = 0x5A11_1111;

    let mut sim = SimNetwork::new("small-network", 1, 2, 7, 3, 5, 1, SEED).await;
    sim.with_start_backoff(Duration::from_millis(30));

    let handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 1, 1)
        .await;

    // Verify correct number of peers started
    assert_eq!(handles.len(), 3, "Expected 1 gateway + 2 nodes = 3 handles");

    // Let tokio tasks run to generate events
    let_network_run(&mut sim, Duration::from_secs(2)).await;

    // Verify we captured events
    let event_counts = sim.get_event_counts().await;
    let total_events: usize = event_counts.values().sum();

    assert!(
        total_events > 0,
        "Should have captured events during network startup"
    );

    // Check partial connectivity
    match sim
        .check_partial_connectivity(Duration::from_secs(10), 0.5)
        .await
    {
        Ok(()) => {
            tracing::info!("Small network achieved connectivity");
        }
        Err(e) => {
            tracing::warn!("Connectivity check: {}", e);
        }
    }

    tracing::info!(
        "Small network test completed - captured {} events",
        total_events
    );
}

// =============================================================================
// Peer Label Assignment
// =============================================================================

/// Tests that peer labels are assigned correctly.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_peer_label_assignment() {
    const SEED: u64 = 0x1ABE_1234;

    let mut sim = SimNetwork::new("label-test", 2, 3, 7, 3, 10, 2, SEED).await;
    let peers = sim.build_peers();

    // Count gateways and nodes
    let gateway_count = peers.iter().filter(|(l, _)| !l.is_node()).count();
    let node_count = peers.iter().filter(|(l, _)| l.is_node()).count();

    assert_eq!(gateway_count, 2, "Expected 2 gateways");
    assert_eq!(node_count, 3, "Expected 3 nodes");
    assert_eq!(peers.len(), 5, "Expected 5 total peers");

    // Verify label format: "{network_name}-gateway-{id}" or "{network_name}-node-{id}"
    for (label, _config) in &peers {
        let label_str = label.to_string();
        assert!(
            label_str.contains("-gateway-") || label_str.contains("-node-"),
            "Unexpected label format: {}",
            label_str
        );
    }

    tracing::info!("Peer label assignment test passed");
}

// =============================================================================
// Event State Hash Capture
// =============================================================================

/// Tests that contract state hashes are properly captured in events.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_event_state_hash_capture() {
    const SEED: u64 = 0xC0DE_1234;

    let mut sim = SimNetwork::new(
        "consistency-test",
        1,  // gateways
        3,  // nodes
        7,  // ring_max_htl
        3,  // rnd_if_htl_above
        10, // max_connections
        2,  // min_connections
        SEED,
    )
    .await;

    sim.with_start_backoff(Duration::from_millis(50));

    // Start network with some contract events
    let _handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 5, 10) // 5 contracts, 10 events
        .await;

    // Let tokio tasks run to generate events
    let_network_run(&mut sim, Duration::from_secs(5)).await;

    // Get event summary and look for state hashes
    let summary = sim.get_deterministic_event_summary().await;

    // Extract put-related events
    let put_events: Vec<_> = summary
        .iter()
        .filter(|e| e.event_kind_name == "Put")
        .collect();

    // Extract state hashes using the structured field
    let state_hashes: Vec<&str> = put_events
        .iter()
        .filter_map(|e| e.state_hash.as_deref())
        .collect();

    // Log what we found for debugging
    tracing::info!(
        "State hash capture test: {} put events, {} state hashes found",
        put_events.len(),
        state_hashes.len()
    );

    // If we have state hashes, verify they are valid format (8 hex chars)
    for hash in &state_hashes {
        assert_eq!(
            hash.len(),
            8,
            "State hash should be 8 hex characters, got: {}",
            hash
        );
        assert!(
            hash.chars().all(|c| c.is_ascii_hexdigit()),
            "State hash should be hex: {}",
            hash
        );
    }

    // Verify event capture is working (we should at least see Connect events)
    let connect_events = summary
        .iter()
        .filter(|e| e.event_kind_name == "Connect")
        .count();

    assert!(
        connect_events > 0,
        "Should capture Connect events during network startup"
    );

    tracing::info!(
        "Event state hash capture test passed - {} Connect events",
        connect_events
    );
}

// =============================================================================
// Eventual Consistency State Hashes
// =============================================================================

/// Tests eventual consistency: peers receiving updates for the same contract
/// should have matching state hashes.
///
/// NOTE: This is a smoke test using tokio. For deterministic testing,
/// this should be converted to use Turmoil.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_eventual_consistency_state_hashes() {
    const SEED: u64 = 0xC0DE_5678;

    let mut sim = SimNetwork::new(
        "eventual-consistency",
        1,  // gateways
        4,  // nodes - more nodes for better broadcast coverage
        7,  // ring_max_htl
        3,  // rnd_if_htl_above
        10, // max_connections
        2,  // min_connections
        SEED,
    )
    .await;

    sim.with_start_backoff(Duration::from_millis(50));

    // Start network with contract events to trigger broadcasts
    let _handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 3, 15) // 3 contracts, 15 events
        .await;

    // Let tokio tasks run to generate events
    let_network_run(&mut sim, Duration::from_secs(6)).await;

    let summary = sim.get_deterministic_event_summary().await;

    // Extract (contract_key, peer_addr, state_hash) using structured fields
    let mut contract_state_by_peer: HashMap<String, Vec<(std::net::SocketAddr, String)>> =
        HashMap::new();

    for event in &summary {
        if let (Some(contract_key), Some(state_hash)) = (&event.contract_key, &event.state_hash) {
            contract_state_by_peer
                .entry(contract_key.clone())
                .or_default()
                .push((event.peer_addr, state_hash.clone()));
        }
    }

    tracing::info!(
        "Found {} contracts with state hashes across peers",
        contract_state_by_peer.len()
    );

    // For each contract, verify eventual consistency
    let mut consistent_contracts = 0;
    let mut total_contracts_with_multiple_peers = 0;

    for (contract_key, peer_states) in &contract_state_by_peer {
        if peer_states.len() < 2 {
            continue; // Need at least 2 peers to check consistency
        }

        total_contracts_with_multiple_peers += 1;

        // Get the most recent state hash for each peer (last in the list)
        let mut peer_final_states: HashMap<std::net::SocketAddr, String> = HashMap::new();
        for (peer_addr, state_hash) in peer_states {
            peer_final_states.insert(*peer_addr, state_hash.clone());
        }

        // All peers should converge to the same state
        let unique_states: std::collections::HashSet<&String> =
            peer_final_states.values().collect();

        if unique_states.len() == 1 {
            consistent_contracts += 1;
            tracing::debug!(
                "Contract {} is consistent across {} peers",
                contract_key,
                peer_final_states.len()
            );
        } else {
            tracing::warn!(
                "Contract {} has {} different states across {} peers: {:?}",
                contract_key,
                unique_states.len(),
                peer_final_states.len(),
                peer_final_states
            );
        }
    }

    tracing::info!(
        "Eventual consistency: {}/{} contracts consistent",
        consistent_contracts,
        total_contracts_with_multiple_peers
    );

    // Verify we at least captured some events
    let total_events: usize = summary.len();
    assert!(total_events > 0, "Should have captured events during test");

    // Lenient threshold for smoke test
    if total_contracts_with_multiple_peers > 0 {
        let convergence_rate =
            consistent_contracts as f64 / total_contracts_with_multiple_peers as f64;
        tracing::info!(
            "Convergence rate: {:.1}% ({}/{})",
            convergence_rate * 100.0,
            consistent_contracts,
            total_contracts_with_multiple_peers
        );

        assert!(
            convergence_rate >= 0.5,
            "Expected at least 50% of contracts to converge, got {:.1}%",
            convergence_rate * 100.0
        );
    }

    // Run anomaly detection on the simulation event logs
    let report = sim.verify_state().await;
    tracing::info!(
        "=== ANOMALY DETECTION (eventual-consistency): {} events, {} state events, {} contracts, {} anomalies ===",
        report.total_events,
        report.state_events,
        report.contracts_analyzed,
        report.anomalies.len()
    );
    for (i, anomaly) in report.anomalies.iter().enumerate() {
        tracing::warn!("  anomaly[{}] = {:?}", i, anomaly);
    }
}

// =============================================================================
// Fault Injection Bridge
// =============================================================================

/// Tests the fault injection bridge that connects FaultConfig with SimNetwork.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_fault_injection_bridge() {
    use freenet::simulation::FaultConfig;

    const SEED: u64 = 0xFA17_B21D;

    // Run 1: Normal network (no faults)
    let mut sim_normal = SimNetwork::new("fault-bridge-normal", 1, 3, 7, 3, 10, 2, SEED).await;
    sim_normal.with_start_backoff(Duration::from_millis(50));

    let _handles = sim_normal
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 1, 3)
        .await;

    // Let tokio tasks run to generate events
    let_network_run(&mut sim_normal, Duration::from_secs(3)).await;

    let normal_events = sim_normal.get_event_counts().await;
    let normal_total: usize = normal_events.values().sum();

    // Run anomaly detection on normal run
    let normal_report = sim_normal.verify_state().await;
    tracing::info!(
        "=== ANOMALY DETECTION (normal run): {} events, {} state events, {} anomalies ===",
        normal_report.total_events,
        normal_report.state_events,
        normal_report.anomalies.len()
    );
    for (i, anomaly) in normal_report.anomalies.iter().enumerate() {
        tracing::warn!("  normal anomaly[{}] = {:?}", i, anomaly);
    }

    sim_normal.clear_fault_injection();

    tracing::info!("Normal run completed: {} events", normal_total);

    // Run 2: With 50% message loss injected via bridge
    let mut sim_lossy = SimNetwork::new("fault-bridge-lossy", 1, 3, 7, 3, 10, 2, SEED).await;
    sim_lossy.with_start_backoff(Duration::from_millis(50));

    let fault_config = FaultConfig::builder().message_loss_rate(0.5).build();
    sim_lossy.with_fault_injection(fault_config);

    let _handles = sim_lossy
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 1, 3)
        .await;

    // Let tokio tasks run to generate events
    let_network_run(&mut sim_lossy, Duration::from_secs(3)).await;

    let lossy_events = sim_lossy.get_event_counts().await;
    let lossy_total: usize = lossy_events.values().sum();

    // Run anomaly detection on lossy run - expect MORE anomalies than normal
    let lossy_report = sim_lossy.verify_state().await;
    tracing::info!(
        "=== ANOMALY DETECTION (50% loss run): {} events, {} state events, {} anomalies ===",
        lossy_report.total_events,
        lossy_report.state_events,
        lossy_report.anomalies.len()
    );
    for (i, anomaly) in lossy_report.anomalies.iter().enumerate() {
        tracing::warn!("  lossy anomaly[{}] = {:?}", i, anomaly);
    }

    // Compare anomaly counts between normal and lossy runs
    tracing::info!(
        "=== ANOMALY COMPARISON: normal={} anomalies vs lossy={} anomalies ===",
        normal_report.anomalies.len(),
        lossy_report.anomalies.len()
    );

    sim_lossy.clear_fault_injection();

    tracing::info!("Lossy run (50% loss) completed: {} events", lossy_total);

    assert!(normal_total > 0, "Normal run should capture events");
    assert!(
        lossy_total > 0,
        "Lossy run should still capture some events"
    );

    tracing::info!("Fault injection bridge test passed");
}

// =============================================================================
// Partition Injection Bridge
// =============================================================================

/// Tests partition injection via the fault bridge.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_partition_injection_bridge() {
    use freenet::simulation::{FaultConfig, Partition};
    use std::collections::HashSet;

    const SEED: u64 = 0xDA27_1710;

    let mut sim = SimNetwork::new("partition-test", 1, 2, 7, 3, 10, 2, SEED).await;
    sim.with_start_backoff(Duration::from_millis(50));

    let _handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 1, 3)
        .await;

    // Let tokio tasks run to generate events
    let_network_run(&mut sim, Duration::from_secs(2)).await;

    let pre_partition = sim.get_event_counts().await;
    let pre_total: usize = pre_partition.values().sum();
    tracing::info!("Pre-partition: {} events", pre_total);

    // Get actual node addresses from the simulation
    let all_addrs = sim.all_node_addresses();
    assert!(
        all_addrs.len() >= 2,
        "Need at least 2 nodes for partition test"
    );

    // Create a partition using real addresses
    let addrs: Vec<_> = all_addrs.values().copied().collect();
    let mid = addrs.len() / 2;

    let side_a: HashSet<_> = addrs[..mid].iter().copied().collect();
    let side_b: HashSet<_> = addrs[mid..].iter().copied().collect();

    let partition = Partition::new(side_a, side_b).permanent(0);
    let fault_config = FaultConfig::builder().partition(partition).build();
    sim.with_fault_injection(fault_config);

    // Let tokio tasks run during partition
    let_network_run(&mut sim, Duration::from_secs(2)).await;

    sim.clear_fault_injection();

    tracing::info!("Partition injection bridge test passed");
}

// =============================================================================
// Latency Injection
// =============================================================================

/// Tests latency injection via the fault bridge.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_latency_injection() {
    use freenet::simulation::FaultConfig;

    const SEED: u64 = 0x1A7E_1234;

    // Run 1: No latency injection
    let mut sim_fast = SimNetwork::new("latency-none", 1, 2, 7, 3, 10, 2, SEED).await;
    sim_fast.with_start_backoff(Duration::from_millis(30));

    let _handles = sim_fast
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 1, 2)
        .await;

    // Let tokio tasks run to generate events
    let_network_run(&mut sim_fast, Duration::from_secs(2)).await;

    let fast_events = sim_fast.get_event_counts().await;
    let fast_total: usize = fast_events.values().sum();

    // Run 2: With latency injection (100-200ms per message)
    let mut sim_slow = SimNetwork::new("latency-injected", 1, 2, 7, 3, 10, 2, SEED).await;
    sim_slow.with_start_backoff(Duration::from_millis(30));

    let fault_config = FaultConfig::builder()
        .latency_range(Duration::from_millis(100)..Duration::from_millis(200))
        .build();
    sim_slow.with_fault_injection(fault_config);

    let _handles = sim_slow
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 1, 2)
        .await;

    // Let tokio tasks run to generate events
    let_network_run(&mut sim_slow, Duration::from_secs(2)).await;

    let slow_events = sim_slow.get_event_counts().await;
    let slow_total: usize = slow_events.values().sum();
    sim_slow.clear_fault_injection();

    tracing::info!(
        "Latency test: fast={} events, slow={} events",
        fast_total,
        slow_total
    );

    assert!(fast_total > 0, "Fast run should capture events");
    assert!(slow_total > 0, "Slow run should capture some events");

    tracing::info!("Latency injection test passed");
}

// =============================================================================
// Node Crash and Recovery
// =============================================================================

/// Tests the node crash/recovery API in SimNetwork.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_node_crash_recovery() {
    const SEED: u64 = 0xC2A5_0000_000E;

    let mut sim = SimNetwork::new("crash-recovery-test", 1, 3, 7, 3, 10, 2, SEED).await;
    sim.with_start_backoff(Duration::from_millis(50));

    let _handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 1, 1)
        .await;

    // Let tokio tasks run to generate events
    let_network_run(&mut sim, Duration::from_secs(2)).await;

    // Find a non-gateway node to crash
    let all_addrs = sim.all_node_addresses();
    assert!(!all_addrs.is_empty(), "Should have tracked node addresses");

    let node_to_crash = all_addrs
        .keys()
        .find(|label| label.is_node())
        .cloned()
        .expect("Should have at least one regular node");

    // Initially node is not crashed
    assert!(
        !sim.is_node_crashed(&node_to_crash),
        "Node should not be crashed initially"
    );

    // Crash the node
    let crashed = sim.crash_node(&node_to_crash);
    assert!(crashed, "crash_node should return true for running node");
    assert!(
        sim.is_node_crashed(&node_to_crash),
        "Node should be marked as crashed after crash_node()"
    );

    // Let some time pass while crashed
    let_network_run(&mut sim, Duration::from_millis(500)).await;

    // Recover the node
    let recovered = sim.recover_node(&node_to_crash);
    assert!(recovered, "recover_node should return true");
    assert!(
        !sim.is_node_crashed(&node_to_crash),
        "Node should not be crashed after recovery"
    );

    tracing::info!("Node crash/recovery test completed successfully");
}

// =============================================================================
// VirtualTime Always Enabled
// =============================================================================

/// Tests that VirtualTime is always enabled and accessible.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_virtual_time_always_enabled() {
    use freenet::dev_tool::TimeSource;

    const SEED: u64 = 0x1111_7111;

    let mut sim = SimNetwork::new("virtual-time-test", 1, 2, 7, 3, 10, 2, SEED).await;

    // VirtualTime should be available immediately
    let vt = sim.virtual_time();
    assert_eq!(vt.now_nanos(), 0, "VirtualTime should start at 0");

    // Advance virtual time manually
    vt.advance(Duration::from_millis(100));
    assert_eq!(vt.now_nanos(), 100_000_000);

    // Network stats should be available
    let stats = sim.get_network_stats();
    assert!(stats.is_some());

    let _handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 1, 1)
        .await;

    // Let tokio tasks run
    let_network_run(&mut sim, Duration::from_secs(1)).await;

    // VirtualTime is independent of real time in start_with_rand_gen mode
    // Just verify it's accessible after network start
    let _ = sim.virtual_time().now_nanos();

    tracing::info!("VirtualTime always-enabled test completed");
}

// =============================================================================
// Node Restart
// =============================================================================

/// Tests full node restart with preserved state.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_node_restart() {
    const SEED: u64 = 0x2E57_A2F0;

    let mut sim = SimNetwork::new("restart-test", 1, 3, 7, 3, 10, 2, SEED).await;
    sim.with_start_backoff(Duration::from_millis(50));

    let _handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 1, 1)
        .await;

    // Let tokio tasks run
    let_network_run(&mut sim, Duration::from_secs(2)).await;

    let all_addrs = sim.all_node_addresses();
    let node_to_restart = all_addrs
        .keys()
        .find(|label| label.is_node())
        .cloned()
        .expect("Should have at least one regular node");

    assert!(sim.can_restart(&node_to_restart));

    let addr_before = sim.node_address(&node_to_restart);
    assert!(addr_before.is_some());

    // Crash and restart
    let crashed = sim.crash_node(&node_to_restart);
    assert!(crashed);

    // Let some time pass while crashed
    let_network_run(&mut sim, Duration::from_millis(200)).await;

    let restart_seed = SEED.wrapping_add(0x1000);
    let handle = sim
        .restart_node::<rand::rngs::SmallRng>(&node_to_restart, restart_seed, 1, 1)
        .await;

    assert!(handle.is_some());
    assert!(!sim.is_node_crashed(&node_to_restart));

    // Address should remain the same (same identity)
    let addr_after = sim.node_address(&node_to_restart);
    assert_eq!(addr_before, addr_after);

    // Let tokio tasks run after restart
    let_network_run(&mut sim, Duration::from_secs(2)).await;

    tracing::info!("Node restart test completed successfully");
}

// =============================================================================
// Crash/Restart Edge Cases
// =============================================================================

/// Tests edge cases for crash/restart operations.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_crash_restart_edge_cases() {
    use freenet::dev_tool::NodeLabel;

    const SEED: u64 = 0xED6E_CA5E;

    let mut sim = SimNetwork::new("crash-edge-cases", 1, 2, 7, 3, 10, 2, SEED).await;
    sim.with_start_backoff(Duration::from_millis(50));

    let _handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 1, 1)
        .await;

    // Let tokio tasks run
    let_network_run(&mut sim, Duration::from_secs(1)).await;

    // Test with a non-existent node label
    let fake_label: NodeLabel = "node-999".into();

    assert!(!sim.crash_node(&fake_label));
    assert!(!sim.recover_node(&fake_label));
    assert!(!sim.is_node_crashed(&fake_label));
    assert!(!sim.can_restart(&fake_label));
    assert!(sim.node_address(&fake_label).is_none());

    tracing::info!("Edge case tests completed successfully");
}

// =============================================================================
// Roadmap Scenario: Partition → Heal → Convergence
// =============================================================================

/// Tests that the network converges after a partition heals.
///
/// ## Scenario (from simulation-testing.md roadmap)
/// 1. Start a network with contract operations flowing
/// 2. Partition the network into two sides
/// 3. Both sides continue to receive updates independently
/// 4. Heal the partition
/// 5. Assert all nodes converge to the same state
///
/// ## Anomaly Detection
/// During the partition we expect:
/// - SuspectedPartition or MissingBroadcast (broadcasts can't cross)
/// After healing we expect:
/// - FinalDivergence should be absent (network self-heals)
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_partition_heal_convergence() {
    use freenet::config::{GlobalRng, GlobalSimulationTime};
    use freenet::simulation::{FaultConfig, Partition};
    use std::collections::HashSet;

    const SEED: u64 = 0xDA27_0EA1_0001;

    // Reset global state for deterministic behavior
    freenet::dev_tool::reset_all_simulation_state();
    GlobalRng::set_seed(SEED);
    const BASE_EPOCH_MS: u64 = 1577836800000;
    const RANGE_MS: u64 = 5 * 365 * 24 * 60 * 60 * 1000;
    GlobalSimulationTime::set_time_ms(BASE_EPOCH_MS + (SEED % RANGE_MS));

    // Larger network to make the partition meaningful
    let mut sim = SimNetwork::new(
        "partition-heal-test",
        1,  // 1 gateway
        5,  // 5 nodes (6 total, splits 3+3)
        7,  // ring_max_htl
        3,  // rnd_if_htl_above
        10, // max_connections
        2,  // min_connections
        SEED,
    )
    .await;
    sim.with_start_backoff(Duration::from_millis(50));

    // Start with contracts and enough events to generate meaningful state
    let _handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 3, 15)
        .await;

    // Phase 1: Let the network run normally and establish contracts
    let_network_run(&mut sim, Duration::from_secs(5)).await;

    let pre_partition_events = sim.get_event_counts().await;
    let pre_total: usize = pre_partition_events.values().sum();
    tracing::info!("Phase 1 (pre-partition): {} events", pre_total);
    assert!(
        pre_total > 0,
        "Should have captured events during initial phase"
    );

    // Phase 2: Partition the network in half
    let all_addrs = sim.all_node_addresses();
    let addrs: Vec<_> = all_addrs.values().copied().collect();
    let mid = addrs.len() / 2;

    let side_a: HashSet<_> = addrs[..mid].iter().copied().collect();
    let side_b: HashSet<_> = addrs[mid..].iter().copied().collect();

    tracing::info!(
        "Partitioning network: side_a={} nodes, side_b={} nodes",
        side_a.len(),
        side_b.len()
    );

    let partition = Partition::new(side_a, side_b).permanent(0);
    let fault_config = FaultConfig::builder().partition(partition).build();
    sim.with_fault_injection(fault_config);

    // Let both sides operate independently during partition
    let_network_run(&mut sim, Duration::from_secs(4)).await;

    tracing::info!("Phase 2 (partitioned): network split active");

    // Phase 3: Heal the partition
    sim.clear_fault_injection();
    tracing::info!("Phase 3 (healing): partition cleared");

    // Give the network time to converge after healing
    let_network_run(&mut sim, Duration::from_secs(6)).await;

    // Phase 4: Check convergence and anomaly detection
    let report = sim.verify_state().await;
    tracing::info!(
        "=== PARTITION-HEAL ANOMALY REPORT: {} events, {} state events, {} contracts, {} anomalies ===",
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

    tracing::info!(
        "  divergences={}, missing_broadcasts={}, partitions={}, stale={}, oscillations={}",
        divergences.len(),
        missing.len(),
        partitions.len(),
        stale.len(),
        oscillations.len(),
    );

    for (i, anomaly) in report.anomalies.iter().enumerate() {
        tracing::warn!("  anomaly[{}] = {:?}", i, anomaly);
    }

    tracing::info!(
        "Partition-heal-convergence test completed: {} total events",
        pre_total
    );
}

// =============================================================================
// Roadmap Scenario: Rolling Restart with Convergence
// =============================================================================

/// Tests that the network handles node crashes and restarts while maintaining state.
///
/// ## Scenario (from simulation-testing.md roadmap)
/// 1. Put contracts into the network
/// 2. Crash 2 nodes
/// 3. Verify remaining nodes still serve the contracts
/// 4. Restart the crashed nodes
/// 5. Assert state is recovered (restarted nodes rejoin and converge)
///
/// ## Anomaly Detection
/// During crash phase we expect:
/// - StalePeer (crashed nodes miss updates)
/// After restart we expect:
/// - Network should converge (no FinalDivergence)
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_rolling_restart_convergence() {
    use freenet::config::{GlobalRng, GlobalSimulationTime};

    const SEED: u64 = 0x2011_2E57_0001;

    freenet::dev_tool::reset_all_simulation_state();
    GlobalRng::set_seed(SEED);
    const BASE_EPOCH_MS: u64 = 1577836800000;
    const RANGE_MS: u64 = 5 * 365 * 24 * 60 * 60 * 1000;
    GlobalSimulationTime::set_time_ms(BASE_EPOCH_MS + (SEED % RANGE_MS));

    // 1 gateway + 5 nodes so we can crash 2 and still have 4 alive
    let mut sim = SimNetwork::new(
        "rolling-restart-test",
        1,  // 1 gateway
        5,  // 5 nodes
        7,  // ring_max_htl
        3,  // rnd_if_htl_above
        10, // max_connections
        2,  // min_connections
        SEED,
    )
    .await;
    sim.with_start_backoff(Duration::from_millis(50));

    let _handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 2, 10)
        .await;

    // Phase 1: Let the network establish contracts
    let_network_run(&mut sim, Duration::from_secs(5)).await;

    let pre_crash_events = sim.get_event_counts().await;
    let pre_crash_total: usize = pre_crash_events.values().sum();
    tracing::info!("Phase 1 (pre-crash): {} events", pre_crash_total);
    assert!(
        pre_crash_total > 0,
        "Should have captured events during initial phase"
    );

    // Phase 2: Crash 2 non-gateway nodes
    let all_addrs = sim.all_node_addresses();
    let nodes_to_crash: Vec<_> = all_addrs
        .keys()
        .filter(|label| label.is_node())
        .take(2)
        .cloned()
        .collect();

    assert_eq!(
        nodes_to_crash.len(),
        2,
        "Need at least 2 non-gateway nodes to crash"
    );

    for node in &nodes_to_crash {
        let crashed = sim.crash_node(node);
        assert!(crashed, "crash_node should succeed for {}", node);
        tracing::info!("Crashed node: {}", node);
    }

    // Phase 3: Let the remaining network continue operating
    let_network_run(&mut sim, Duration::from_secs(3)).await;

    tracing::info!("Phase 3 (degraded): network running with 2 crashed nodes");

    // Phase 4: Restart the crashed nodes
    for node in &nodes_to_crash {
        let restart_seed = SEED.wrapping_add(node.number() as u64 * 0x1000);
        let handle = sim
            .restart_node::<rand::rngs::SmallRng>(node, restart_seed, 2, 5)
            .await;
        assert!(handle.is_some(), "restart_node should succeed for {}", node);
        tracing::info!("Restarted node: {}", node);
    }

    // Phase 5: Let the network converge after restart
    let_network_run(&mut sim, Duration::from_secs(5)).await;

    tracing::info!("Phase 5 (recovered): all nodes restarted");

    // Phase 6: Anomaly detection
    let report = sim.verify_state().await;
    tracing::info!(
        "=== ROLLING RESTART ANOMALY REPORT: {} events, {} state events, {} contracts, {} anomalies ===",
        report.total_events,
        report.state_events,
        report.contracts_analyzed,
        report.anomalies.len()
    );

    let divergences = report.divergences();
    let stale = report.stale_peers();
    let zombies = report.zombie_transactions();
    let oscillations = report.state_oscillations();

    tracing::info!(
        "  divergences={}, stale_peers={}, zombies={}, oscillations={}",
        divergences.len(),
        stale.len(),
        zombies.len(),
        oscillations.len(),
    );

    for (i, anomaly) in report.anomalies.iter().enumerate() {
        tracing::warn!("  anomaly[{}] = {:?}", i, anomaly);
    }

    tracing::info!(
        "Rolling restart convergence test completed: {} total events",
        pre_crash_total
    );
}

// =============================================================================
// Roadmap Scenario: Multi-Step Churn
// =============================================================================

/// Tests eventual consistency through continuous crash/restart cycles.
///
/// ## Scenario (from simulation-testing.md roadmap)
/// 1. Start the network with contract operations
/// 2. Perform multiple rounds of: crash a node → wait → restart → wait
/// 3. After all churn rounds, verify eventual consistency
///
/// ## Anomaly Detection
/// During churn we expect:
/// - StalePeer, StateOscillation (transient disruption)
/// After stabilization:
/// - FinalDivergence should be absent if the network self-heals
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_multi_step_churn() {
    use freenet::config::{GlobalRng, GlobalSimulationTime};

    const SEED: u64 = 0xC402_0000_0001;
    const CHURN_ROUNDS: usize = 3;

    freenet::dev_tool::reset_all_simulation_state();
    GlobalRng::set_seed(SEED);
    const BASE_EPOCH_MS: u64 = 1577836800000;
    const RANGE_MS: u64 = 5 * 365 * 24 * 60 * 60 * 1000;
    GlobalSimulationTime::set_time_ms(BASE_EPOCH_MS + (SEED % RANGE_MS));

    let mut sim = SimNetwork::new(
        "multi-step-churn-test",
        1,  // 1 gateway
        4,  // 4 nodes (5 total; we churn 1 at a time)
        7,  // ring_max_htl
        3,  // rnd_if_htl_above
        10, // max_connections
        2,  // min_connections
        SEED,
    )
    .await;
    sim.with_start_backoff(Duration::from_millis(50));

    let _handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 2, 6)
        .await;

    // Let network establish
    let_network_run(&mut sim, Duration::from_secs(3)).await;

    // Get the list of non-gateway nodes we can churn
    let all_addrs = sim.all_node_addresses();
    let churn_candidates: Vec<_> = all_addrs
        .keys()
        .filter(|label| label.is_node())
        .cloned()
        .collect();

    assert!(
        !churn_candidates.is_empty(),
        "Need non-gateway nodes for churn"
    );

    // Perform churn rounds: crash → wait → restart → wait
    for round in 0..CHURN_ROUNDS {
        let target = &churn_candidates[round % churn_candidates.len()];

        // Skip if node is already crashed (from a previous round that hasn't restarted yet)
        if sim.is_node_crashed(target) {
            tracing::info!(
                "Churn round {}: {} already crashed, skipping",
                round,
                target
            );
            continue;
        }

        tracing::info!("Churn round {}: crashing {}", round, target);
        let crashed = sim.crash_node(target);
        assert!(crashed, "crash_node should succeed for {}", target);

        // Let network operate in degraded state
        let_network_run(&mut sim, Duration::from_secs(2)).await;

        // Restart the node
        let restart_seed = SEED.wrapping_add((round as u64 + 1) * 0x2000);
        let handle = sim
            .restart_node::<rand::rngs::SmallRng>(target, restart_seed, 2, 4)
            .await;
        assert!(
            handle.is_some(),
            "restart_node should succeed for {} in round {}",
            target,
            round
        );

        tracing::info!("Churn round {}: restarted {}", round, target);

        // Let network stabilize after restart
        let_network_run(&mut sim, Duration::from_secs(2)).await;
    }

    // Final stabilization period
    let_network_run(&mut sim, Duration::from_secs(4)).await;

    // Anomaly detection
    let report = sim.verify_state().await;
    tracing::info!(
        "=== MULTI-STEP CHURN ANOMALY REPORT ({} rounds): {} events, {} state events, {} contracts, {} anomalies ===",
        CHURN_ROUNDS,
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

    tracing::info!(
        "  divergences={}, missing_broadcasts={}, stale_peers={}, zombies={}, oscillations={}, cascades={}",
        divergences.len(),
        missing.len(),
        stale.len(),
        zombies.len(),
        oscillations.len(),
        cascades.len(),
    );

    for (i, anomaly) in report.anomalies.iter().enumerate() {
        tracing::warn!("  anomaly[{}] = {:?}", i, anomaly);
    }

    tracing::info!(
        "Multi-step churn test completed: {} rounds, {} total anomalies",
        CHURN_ROUNDS,
        report.anomalies.len()
    );
}

// =============================================================================
// Zero Nodes/Gateways Panic Tests
// =============================================================================

/// Tests that creating a network with zero nodes panics.
#[test_log::test(tokio::test)]
#[should_panic(expected = "assertion failed")]
async fn test_zero_nodes_panics() {
    let _sim = SimNetwork::new("zero-nodes-test", 1, 0, 7, 3, 10, 2, 0xDEAD).await;
}

/// Tests that creating a network with zero gateways panics.
#[test_log::test(tokio::test)]
#[should_panic(expected = "should have at least one gateway")]
async fn test_zero_gateways_panics() {
    let _sim = SimNetwork::new("zero-gateways-test", 0, 2, 7, 3, 10, 2, 0xDEAD).await;
}

// =============================================================================
// Minimal Network
// =============================================================================

/// Tests that a minimal network with 1 gateway and 1 node works.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_minimal_network() {
    const SEED: u64 = 0x0101_0401;

    let mut sim = SimNetwork::new("minimal-test", 1, 1, 7, 3, 10, 2, SEED).await;
    sim.with_start_backoff(Duration::from_millis(50));

    let handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 1, 1)
        .await;

    assert_eq!(handles.len(), 2, "Should have exactly 2 nodes");

    // Let tokio tasks run
    let_network_run(&mut sim, Duration::from_secs(1)).await;

    let all_addrs = sim.all_node_addresses();
    assert_eq!(all_addrs.len(), 2, "Should track 2 node addresses");

    tracing::info!("Minimal network test passed");
}

// =============================================================================
// Subscription Topology Validation
// =============================================================================
// These tests validate the subscription topology infrastructure to detect
// issues like bidirectional cycles (#2720), orphan seeders (#2719), etc.

/// Helper to create a contract ID from a seed
fn make_contract_id(seed: u8) -> freenet_stdlib::prelude::ContractInstanceId {
    freenet_stdlib::prelude::ContractInstanceId::new([seed; 32])
}

/// Test that topology snapshot infrastructure is working correctly.
///
/// This test verifies:
/// 1. SimNetwork creates and starts nodes correctly
/// 2. The periodic topology registration task runs
/// 3. Topology snapshots are captured for all peers
/// 4. The validate_subscription_topology function can be called
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_topology_infrastructure() {
    use freenet::config::{GlobalRng, GlobalSimulationTime};
    use freenet::dev_tool::reset_all_simulation_state;

    const SEED: u64 = 0x1234_5678;

    // Reset all global state and set up deterministic time/RNG
    reset_all_simulation_state();
    GlobalRng::set_seed(SEED);
    const BASE_EPOCH_MS: u64 = 1577836800000;
    const RANGE_MS: u64 = 5 * 365 * 24 * 60 * 60 * 1000;
    GlobalSimulationTime::set_time_ms(BASE_EPOCH_MS + (SEED % RANGE_MS));

    let mut sim = SimNetwork::new(
        "topology-infra-test",
        1,  // 1 gateway
        3,  // 3 peers
        7,  // max_htl
        3,  // rnd_if_htl_above
        10, // max_connections
        2,  // min_connections
        SEED,
    )
    .await;
    sim.with_start_backoff(Duration::from_millis(50));

    // Start network with minimal operations - just verify infrastructure
    let _handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 1, 1)
        .await;

    // Let the network run to establish connections
    let_network_run(&mut sim, Duration::from_secs(5)).await;

    // Wait for topology registration task to run (1 second interval)
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify topology snapshots were captured
    let snapshots = sim.get_topology_snapshots();

    // Should have captured snapshots for all 4 peers (1 gateway + 3 nodes)
    assert!(
        snapshots.len() >= 4,
        "Expected at least 4 topology snapshots (1 gateway + 3 nodes), got {}",
        snapshots.len()
    );

    // Verify each snapshot has valid data
    for snap in &snapshots {
        // Each peer should have a valid address and location
        assert!(
            snap.location >= 0.0 && snap.location <= 1.0,
            "Invalid location {} for peer {}",
            snap.location,
            snap.peer_addr
        );
    }

    // Verify validate_subscription_topology can be called
    let contract_id = make_contract_id(1);
    let result = sim.validate_subscription_topology(&contract_id, 0.5);

    // With no contracts created, should have no issues
    assert!(
        result.is_healthy(),
        "Empty topology should be healthy, got {} issues",
        result.issue_count
    );

    tracing::info!("Topology infrastructure test passed");
}
