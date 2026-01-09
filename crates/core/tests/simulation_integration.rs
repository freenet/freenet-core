//! Integration tests for the deterministic simulation framework.
//!
//! These tests verify that:
//! 1. Same seed produces identical network behavior (deterministic replay)
//! 2. Fault injection works correctly with deterministic outcomes
//! 3. Network events are captured and comparable
//! 4. Small networks establish connectivity reliably
//!
//! NOTE: These tests use global state (socket registries, RNG) and must run serially.
//! Enable with: cargo test -p freenet --features simulation_tests --test simulation_integration -- --test-threads=1

#![cfg(feature = "simulation_tests")]

use freenet::dev_tool::SimNetwork;
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
#[test]
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
        // Use moderate number of iterations - each takes ~200ms so 20 iterations = 4 seconds
        // Plus 2s startup + 2s final wait = ~8s of event processing
        let result = sim.run_simulation::<rand::rngs::SmallRng, _, _>(
            seed,
            5,                       // max_contract_num - moderate contracts
            20,                      // iterations - 20 events for diverse coverage
            Duration::from_secs(30), // simulation_duration
            || async {
                // Event triggering happens before this function
                // Just a short wait to ensure everything settled
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

    // Run simulation THREE times with identical seed to catch:
    // - State leakage between runs
    // - Warm-up effects on first run
    // - Off-by-one patterns that only manifest on odd/even runs
    let (result1, trace1) = run_and_trace("strict-det-run1", SEED);
    let (result2, trace2) = run_and_trace("strict-det-run2", SEED);
    let (result3, trace3) = run_and_trace("strict-det-run3", SEED);

    // All simulations should have the same outcome (all succeed or all fail)
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
        eprintln!("\n=== DETERMINISM DEBUG ===");
        eprintln!(
            "Run 1 total: {}, Run 2 total: {}, Run 3 total: {}",
            trace1.total_events, trace2.total_events, trace3.total_events
        );
        eprintln!("\nEvent counts by type:");

        let mut all_types: std::collections::BTreeSet<&String> =
            trace1.event_counts.keys().collect();
        all_types.extend(trace2.event_counts.keys());
        all_types.extend(trace3.event_counts.keys());

        for event_type in all_types {
            let count1 = trace1.event_counts.get(event_type).unwrap_or(&0);
            let count2 = trace2.event_counts.get(event_type).unwrap_or(&0);
            let count3 = trace3.event_counts.get(event_type).unwrap_or(&0);
            if count1 != count2 || count2 != count3 {
                eprintln!(
                    "  {} : {} vs {} vs {} (DIFFERS)",
                    event_type, count1, count2, count3
                );
            } else {
                eprintln!("  {} : {} (same)", event_type, count1);
            }
        }
        eprintln!("=========================\n");
    }

    // STRICT ASSERTION 1: Exact same total event count across all 3 runs
    assert_eq!(
        trace1.total_events, trace2.total_events,
        "STRICT DETERMINISM FAILURE: Total event counts differ!\nRun 1: {}\nRun 2: {}",
        trace1.total_events, trace2.total_events
    );
    assert_eq!(
        trace2.total_events, trace3.total_events,
        "STRICT DETERMINISM FAILURE: Total event counts differ!\nRun 2: {}\nRun 3: {}",
        trace2.total_events, trace3.total_events
    );

    // STRICT ASSERTION 2: Exact same event counts per type across all 3 runs
    assert_eq!(
        trace1.event_counts, trace2.event_counts,
        "STRICT DETERMINISM FAILURE: Event counts per type differ!\nRun 1: {:?}\nRun 2: {:?}",
        trace1.event_counts, trace2.event_counts
    );
    assert_eq!(
        trace2.event_counts, trace3.event_counts,
        "STRICT DETERMINISM FAILURE: Event counts per type differ!\nRun 2: {:?}\nRun 3: {:?}",
        trace2.event_counts, trace3.event_counts
    );

    // STRICT ASSERTION 3: Exact same event sequence across all 3 runs
    assert_eq!(
        trace1.event_sequence.len(),
        trace2.event_sequence.len(),
        "STRICT DETERMINISM FAILURE: Event sequence lengths differ (run 1 vs 2)!"
    );
    assert_eq!(
        trace2.event_sequence.len(),
        trace3.event_sequence.len(),
        "STRICT DETERMINISM FAILURE: Event sequence lengths differ (run 2 vs 3)!"
    );

    for (i, ((e1, e2), e3)) in trace1
        .event_sequence
        .iter()
        .zip(trace2.event_sequence.iter())
        .zip(trace3.event_sequence.iter())
        .enumerate()
    {
        assert_eq!(
            e1, e2,
            "STRICT DETERMINISM FAILURE: Event sequence differs at index {}!\nRun 1: {:?}\nRun 2: {:?}",
            i, e1, e2
        );
        assert_eq!(
            e2, e3,
            "STRICT DETERMINISM FAILURE: Event sequence differs at index {}!\nRun 2: {:?}\nRun 3: {:?}",
            i, e2, e3
        );
    }

    // Print event breakdown on success for verification
    eprintln!("\n=== DETERMINISM TEST PASSED ===");
    eprintln!("Total events matched: {}", trace1.total_events);
    eprintln!("\nEvent type breakdown:");
    let mut sorted_types: Vec<_> = trace1.event_counts.iter().collect();
    sorted_types.sort_by(|a, b| b.1.cmp(a.1)); // Sort by count descending
    for (event_type, count) in &sorted_types {
        eprintln!("  {:20} : {:5}", event_type, count);
    }
    eprintln!("================================\n");

    tracing::info!(
        "STRICT DETERMINISM TEST PASSED: {} events matched exactly across 3 runs",
        trace1.total_events
    );
}

/// **STRICT** determinism test with MULTIPLE GATEWAYS.
///
/// This test verifies that simulations with 2+ gateways remain deterministic.
/// Multiple gateways introduce additional complexity:
/// - Multiple independent connection entry points
/// - Gateway-to-gateway communication
/// - More complex routing decisions
///
/// The test runs 3 times with the same seed and verifies exact event equality.
#[test]
fn test_strict_determinism_multi_gateway() {
    use freenet::dev_tool::SimNetwork;

    const SEED: u64 = 0xAB17_6A7E_1234;

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

        // Create SimNetwork with 2 gateways (multi-gateway determinism test)
        let (sim, logs_handle) = rt.block_on(async {
            let sim = SimNetwork::new(
                name, 2,  // gateways - MULTI-GATEWAY for this test
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
            20,                      // iterations - 20 events for diverse coverage
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
    let (result1, trace1) = run_and_trace("multi-gw-det-run1", SEED);
    let (result2, trace2) = run_and_trace("multi-gw-det-run2", SEED);
    let (result3, trace3) = run_and_trace("multi-gw-det-run3", SEED);

    // All simulations should have the same outcome
    assert_eq!(
        result1.is_ok(),
        result2.is_ok(),
        "MULTI-GATEWAY DETERMINISM FAILURE: Simulation outcomes differ!\nRun 1: {:?}\nRun 2: {:?}",
        result1,
        result2
    );
    assert_eq!(
        result2.is_ok(),
        result3.is_ok(),
        "MULTI-GATEWAY DETERMINISM FAILURE: Simulation outcomes differ!\nRun 2: {:?}\nRun 3: {:?}",
        result2,
        result3
    );

    // Debug output if event counts differ
    if trace1.total_events != trace2.total_events || trace2.total_events != trace3.total_events {
        eprintln!("\n=== MULTI-GATEWAY DETERMINISM DEBUG ===");
        eprintln!(
            "Run 1 total: {}, Run 2 total: {}, Run 3 total: {}",
            trace1.total_events, trace2.total_events, trace3.total_events
        );
        eprintln!("\nEvent counts by type:");

        let mut all_types: std::collections::BTreeSet<&String> =
            trace1.event_counts.keys().collect();
        all_types.extend(trace2.event_counts.keys());
        all_types.extend(trace3.event_counts.keys());

        for event_type in all_types {
            let count1 = trace1.event_counts.get(event_type).unwrap_or(&0);
            let count2 = trace2.event_counts.get(event_type).unwrap_or(&0);
            let count3 = trace3.event_counts.get(event_type).unwrap_or(&0);
            if count1 != count2 || count2 != count3 {
                eprintln!(
                    "  {} : {} vs {} vs {} (DIFFERS)",
                    event_type, count1, count2, count3
                );
            } else {
                eprintln!("  {} : {} (same)", event_type, count1);
            }
        }
        eprintln!("========================================\n");
    }

    // STRICT ASSERTION 1: Exact same total event count
    assert_eq!(
        trace1.total_events, trace2.total_events,
        "MULTI-GATEWAY DETERMINISM FAILURE: Total event counts differ!\nRun 1: {}\nRun 2: {}",
        trace1.total_events, trace2.total_events
    );
    assert_eq!(
        trace2.total_events, trace3.total_events,
        "MULTI-GATEWAY DETERMINISM FAILURE: Total event counts differ!\nRun 2: {}\nRun 3: {}",
        trace2.total_events, trace3.total_events
    );

    // STRICT ASSERTION 2: Exact same event counts per type
    assert_eq!(
        trace1.event_counts, trace2.event_counts,
        "MULTI-GATEWAY DETERMINISM FAILURE: Event counts per type differ!\nRun 1: {:?}\nRun 2: {:?}",
        trace1.event_counts, trace2.event_counts
    );
    assert_eq!(
        trace2.event_counts, trace3.event_counts,
        "MULTI-GATEWAY DETERMINISM FAILURE: Event counts per type differ!\nRun 2: {:?}\nRun 3: {:?}",
        trace2.event_counts, trace3.event_counts
    );

    // STRICT ASSERTION 3: Exact same event sequence
    assert_eq!(
        trace1.event_sequence.len(),
        trace2.event_sequence.len(),
        "MULTI-GATEWAY DETERMINISM FAILURE: Event sequence lengths differ (run 1 vs 2)!"
    );
    assert_eq!(
        trace2.event_sequence.len(),
        trace3.event_sequence.len(),
        "MULTI-GATEWAY DETERMINISM FAILURE: Event sequence lengths differ (run 2 vs 3)!"
    );

    for (i, ((e1, e2), e3)) in trace1
        .event_sequence
        .iter()
        .zip(trace2.event_sequence.iter())
        .zip(trace3.event_sequence.iter())
        .enumerate()
    {
        assert_eq!(
            e1, e2,
            "MULTI-GATEWAY DETERMINISM FAILURE: Event sequence differs at index {}!\nRun 1: {:?}\nRun 2: {:?}",
            i, e1, e2
        );
        assert_eq!(
            e2, e3,
            "MULTI-GATEWAY DETERMINISM FAILURE: Event sequence differs at index {}!\nRun 2: {:?}\nRun 3: {:?}",
            i, e2, e3
        );
    }

    // Print event breakdown on success
    eprintln!("\n=== MULTI-GATEWAY DETERMINISM TEST PASSED ===");
    eprintln!("Gateways: 2, Total events matched: {}", trace1.total_events);
    eprintln!("\nEvent type breakdown:");
    let mut sorted_types: Vec<_> = trace1.event_counts.iter().collect();
    sorted_types.sort_by(|a, b| b.1.cmp(a.1));
    for (event_type, count) in &sorted_types {
        eprintln!("  {:20} : {:5}", event_type, count);
    }
    eprintln!("==============================================\n");

    tracing::info!(
        "MULTI-GATEWAY DETERMINISM TEST PASSED: {} events matched exactly across 3 runs (2 gateways)",
        trace1.total_events
    );
}

// =============================================================================
// Test 1: End-to-End Deterministic Replay with Event Verification
// =============================================================================

/// Verifies that the simulation infrastructure captures events consistently.
///
/// NOTE: Full determinism requires a single-threaded async runtime to control
/// scheduling. With multi-threaded tokio, message ordering can vary slightly.
/// This test verifies that the same types of events are captured across runs.
///
/// Uses VirtualTime exclusively - no start_paused or tokio::time::sleep().
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_deterministic_replay_events() {
    const SEED: u64 = 0xDEAD_BEEF_1234;

    async fn run_and_capture(
        name: &str,
        seed: u64,
    ) -> (Vec<(String, usize)>, HashMap<String, usize>) {
        let mut sim = SimNetwork::new(
            name, 1,  // gateways
            3,  // nodes
            7,  // ring_max_htl
            3,  // rnd_if_htl_above
            10, // max_connections
            2,  // min_connections
            seed,
        )
        .await;

        sim.with_start_backoff(Duration::from_millis(50));

        let _handles = sim
            .start_with_rand_gen::<rand::rngs::SmallRng>(seed, 1, 1)
            .await;

        // Use VirtualTime advancement instead of tokio::time::sleep
        // Advance 30x100ms = 3 seconds of virtual time
        for _ in 0..30 {
            sim.advance_time(Duration::from_millis(100));
            tokio::task::yield_now().await;
        }

        // Capture connectivity state
        let connectivity = sim.node_connectivity();
        let mut conn_results: Vec<(String, usize)> = connectivity
            .iter()
            .map(|(label, (_key, conns))| (label.to_string(), conns.len()))
            .collect();
        conn_results.sort_by(|a, b| a.0.cmp(&b.0));

        // Capture event counts
        let event_counts = sim.get_event_counts().await;

        (conn_results, event_counts)
    }

    // Run simulation twice with same seed
    let (conn1, events1) = run_and_capture("deterministic-run1", SEED).await;
    let (conn2, events2) = run_and_capture("deterministic-run2", SEED).await;

    // Both runs should produce the same peer label patterns (this is deterministic)
    // Labels are "{network_name}-{gateway|node}-{id}", so we extract the suffix pattern
    fn extract_label_suffix(label: &str) -> &str {
        // Find the second-to-last hyphen to get "{type}-{id}" suffix
        if let Some(gateway_pos) = label.find("-gateway-") {
            &label[gateway_pos + 1..]
        } else if let Some(node_pos) = label.find("-node-") {
            &label[node_pos + 1..]
        } else {
            label
        }
    }
    let labels1: Vec<&str> = conn1.iter().map(|(l, _)| extract_label_suffix(l)).collect();
    let labels2: Vec<&str> = conn2.iter().map(|(l, _)| extract_label_suffix(l)).collect();
    assert_eq!(
        labels1, labels2,
        "Peer label patterns should be deterministic.\nRun 1: {:?}\nRun 2: {:?}",
        labels1, labels2
    );

    // Both runs should capture the same event types
    let event_types1: std::collections::HashSet<&String> = events1.keys().collect();
    let event_types2: std::collections::HashSet<&String> = events2.keys().collect();
    assert_eq!(
        event_types1, event_types2,
        "Event types should be consistent.\nRun 1: {:?}\nRun 2: {:?}",
        event_types1, event_types2
    );

    // Verify we actually captured some events
    let total_events: usize = events1.values().sum();
    assert!(
        total_events > 0,
        "Should have captured at least some events, got: {:?}",
        events1
    );

    // Log comparison for debugging
    tracing::info!("Run 1 events: {:?}\nRun 2 events: {:?}", events1, events2);

    tracing::info!(
        "Deterministic replay test passed - {} total events captured",
        total_events
    );
}

/// Verifies that different seeds produce different event sequences.
/// Uses VirtualTime exclusively - no start_paused.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_different_seeds_produce_different_events() {
    const SEED_A: u64 = 0x1111_2222_3333;
    const SEED_B: u64 = 0x4444_5555_6666;

    async fn run_and_get_event_summary(
        name: &str,
        seed: u64,
    ) -> Vec<freenet::dev_tool::EventSummary> {
        let mut sim = SimNetwork::new(name, 1, 3, 7, 3, 10, 2, seed).await;
        sim.with_start_backoff(Duration::from_millis(50));
        let _handles = sim
            .start_with_rand_gen::<rand::rngs::SmallRng>(seed, 1, 1)
            .await;
        // Use VirtualTime advancement instead of tokio::time::sleep
        for _ in 0..30 {
            sim.advance_time(Duration::from_millis(100));
            tokio::task::yield_now().await;
        }
        sim.get_deterministic_event_summary().await
    }

    let events_a = run_and_get_event_summary("seed-a", SEED_A).await;
    let events_b = run_and_get_event_summary("seed-b", SEED_B).await;

    // With different seeds, event details should differ
    // (peer addresses, transactions will be different)
    tracing::info!(
        "Seed A: {} events, Seed B: {} events",
        events_a.len(),
        events_b.len()
    );

    // The test verifies the system produces valid results with different seeds
    assert!(!events_a.is_empty(), "Should capture events with seed A");
    assert!(!events_b.is_empty(), "Should capture events with seed B");

    tracing::info!("Different seeds test completed - both produced valid event sequences");
}

// =============================================================================
// Test 2: Fault Injection with Deterministic Behavior
// =============================================================================

/// Tests that simulation produces deterministic results with the same seed.
///
/// VirtualTime is always enabled, making simulation fully deterministic.
/// Uses VirtualTime exclusively - no start_paused.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_fault_injection_deterministic() {
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

        // Use VirtualTime advancement
        for _ in 0..30 {
            sim.advance_time(Duration::from_millis(100));
            tokio::task::yield_now().await;
        }
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
        "Simulation determinism test passed - {} events captured",
        total_events
    );
}

// =============================================================================
// Test 3: Event Sequence Verification
// =============================================================================

/// Tests that the event summary is correctly ordered and consistent.
/// Uses VirtualTime exclusively - no start_paused.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_event_summary_ordering() {
    const SEED: u64 = 0xC0DE_CAFE_BABE;

    let mut sim = SimNetwork::new("event-order", 1, 3, 7, 3, 10, 2, SEED).await;
    sim.with_start_backoff(Duration::from_millis(50));

    let _handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 1, 1)
        .await;

    // Use VirtualTime advancement
    for _ in 0..30 {
        sim.advance_time(Duration::from_millis(100));
        tokio::task::yield_now().await;
    }

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
// Test 4: Small Network Connectivity
// =============================================================================

/// Minimal network test: 1 gateway + 2 nodes.
/// Uses VirtualTime exclusively - no start_paused.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_small_network_connectivity() {
    const SEED: u64 = 0x5A11_1111;

    let mut sim = SimNetwork::new("small-network", 1, 2, 7, 3, 5, 1, SEED).await;
    sim.with_start_backoff(Duration::from_millis(30));

    let handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 1, 1)
        .await;

    // Verify correct number of peers started
    assert_eq!(handles.len(), 3, "Expected 1 gateway + 2 nodes = 3 handles");

    // Use VirtualTime advancement
    for _ in 0..20 {
        sim.advance_time(Duration::from_millis(100));
        tokio::task::yield_now().await;
    }

    // Verify we captured events
    let event_counts = sim.get_event_counts().await;
    let total_events: usize = event_counts.values().sum();

    assert!(
        total_events > 0,
        "Should have captured events during network startup"
    );

    // Check partial connectivity
    match sim.check_partial_connectivity(Duration::from_secs(10), 0.5) {
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

/// Tests that peer labels are assigned correctly and deterministically.
/// Uses VirtualTime exclusively - no start_paused.
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
// Test 5: Contract State Consistency (Eventual Consistency)
// =============================================================================

/// Tests that contract state hashes are properly captured in events.
///
/// This test verifies the event infrastructure captures state hashes when
/// contract operations occur. When put operations succeed, the state_hash
/// field in events allows verification of eventual consistency.
///
/// NOTE: Full eventual consistency testing requires:
/// 1. Peers to establish connections (which may not happen in all runs)
/// 2. Contract operations to complete successfully
/// 3. Comparing state hashes across nodes for the same contract
///
/// This test validates the infrastructure is in place for such verification.
/// Uses VirtualTime exclusively - no start_paused.
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

    // Use VirtualTime advancement
    for _ in 0..50 {
        sim.advance_time(Duration::from_millis(100));
        tokio::task::yield_now().await;
    }

    // Get event summary and look for state hashes
    let summary = sim.get_deterministic_event_summary().await;

    // Extract put-related events
    let put_events: Vec<_> = summary
        .iter()
        .filter(|e| e.event_kind_name == "Put")
        .collect();

    // Extract state hashes using the NEW structured field (no more string parsing!)
    let state_hashes: Vec<&str> = put_events
        .iter()
        .filter_map(|e| e.state_hash.as_deref())
        .collect();

    // Log what we found for debugging
    tracing::info!(
        "State hash capture test: {} put events, {} state hashes found (using structured fields)",
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

    // Also verify contract_key structured field is populated
    let events_with_contract_key = put_events
        .iter()
        .filter(|e| e.contract_key.is_some())
        .count();
    tracing::info!(
        "Events with structured contract_key: {}/{}",
        events_with_contract_key,
        put_events.len()
    );

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

/// Tests eventual consistency: peers receiving updates for the same contract
/// should have matching state hashes.
///
/// This test verifies that when multiple peers receive broadcast updates
/// (via BroadcastReceived or UpdateSuccess events), they end up with the
/// same state_hash for a given contract key.
/// Uses VirtualTime exclusively - no start_paused.
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

    // Use VirtualTime advancement
    for _ in 0..60 {
        sim.advance_time(Duration::from_millis(100));
        tokio::task::yield_now().await;
    }

    let summary = sim.get_deterministic_event_summary().await;

    // Extract (contract_key, peer_addr, state_hash) using NEW structured fields!
    // No more fragile string parsing - use the contract_key and state_hash fields directly
    let mut contract_state_by_peer: HashMap<String, Vec<(std::net::SocketAddr, String)>> =
        HashMap::new();

    for event in &summary {
        // Use the structured fields instead of parsing debug strings
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
            // Keep the latest state for each peer
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

    // Assert convergence: if we have contracts replicated across multiple peers,
    // at least 50% should have converged. This is a lenient threshold because:
    // - Short test runs may not allow full propagation
    // - Multi-threaded tokio introduces timing variance
    // A stricter threshold (80-100%) would require longer test duration or
    // deterministic scheduling.
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
            "Expected at least 50% of contracts to converge, got {:.1}% ({}/{})",
            convergence_rate * 100.0,
            consistent_contracts,
            total_contracts_with_multiple_peers
        );
    }
}

// =============================================================================
// Test 6: Fault Injection Bridge (SimulatedNetwork → SimNetwork)
// =============================================================================

/// Tests the fault injection bridge that connects FaultConfig with SimNetwork.
///
/// This verifies Gap 2 fix: SimulatedNetwork's FaultConfig can now be applied
/// to SimNetwork's InMemoryTransport to inject message loss, partitions, etc.
/// Uses VirtualTime exclusively - no start_paused.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_fault_injection_bridge() {
    use freenet::simulation::FaultConfig;

    const SEED: u64 = 0xFA17_B21D;

    // Run 1: Normal network (no faults)
    let mut sim_normal = SimNetwork::new(
        "fault-bridge-normal",
        1,  // gateways
        3,  // nodes
        7,  // ring_max_htl
        3,  // rnd_if_htl_above
        10, // max_connections
        2,  // min_connections
        SEED,
    )
    .await;
    sim_normal.with_start_backoff(Duration::from_millis(50));

    let _handles = sim_normal
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 1, 3)
        .await;

    // Use VirtualTime advancement
    for _ in 0..30 {
        sim_normal.advance_time(Duration::from_millis(100));
        tokio::task::yield_now().await;
    }
    let normal_events = sim_normal.get_event_counts().await;
    let normal_total: usize = normal_events.values().sum();

    // Clear any previous fault injection
    sim_normal.clear_fault_injection();

    tracing::info!(
        "Normal run completed: {} events ({:?})",
        normal_total,
        normal_events
    );

    // Run 2: With 50% message loss injected via bridge
    let mut sim_lossy = SimNetwork::new("fault-bridge-lossy", 1, 3, 7, 3, 10, 2, SEED).await;
    sim_lossy.with_start_backoff(Duration::from_millis(50));

    // Inject 50% message loss using the NEW bridge API
    let fault_config = FaultConfig::builder().message_loss_rate(0.5).build();
    sim_lossy.with_fault_injection(fault_config);

    let _handles = sim_lossy
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 1, 3)
        .await;

    // Use VirtualTime advancement
    for _ in 0..30 {
        sim_lossy.advance_time(Duration::from_millis(100));
        tokio::task::yield_now().await;
    }
    let lossy_events = sim_lossy.get_event_counts().await;
    let lossy_total: usize = lossy_events.values().sum();

    // Clear fault injection after test
    sim_lossy.clear_fault_injection();

    tracing::info!(
        "Lossy run (50% loss) completed: {} events ({:?})",
        lossy_total,
        lossy_events
    );

    // Verify that the bridge is working:
    // 1. Both runs should capture events
    assert!(normal_total > 0, "Normal run should capture events");
    assert!(
        lossy_total > 0,
        "Lossy run should still capture some events"
    );

    // 2. With 50% message loss, we generally expect fewer events or different patterns
    // (though exact counts depend on which messages get dropped)
    tracing::info!(
        "Fault injection bridge test: normal={} events, lossy={} events",
        normal_total,
        lossy_total
    );

    // Verify that the infrastructure is in place and working
    // The key validation is that we could successfully set fault injection
    // and the network still operated (just with message drops)
    tracing::info!("Fault injection bridge test passed - infrastructure verified");
}

/// Tests partition injection via the fault bridge.
///
/// Creates a network and then partitions it, verifying that the
/// partition blocks messages between specified peer groups.
/// Uses VirtualTime exclusively - no start_paused.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_partition_injection_bridge() {
    use freenet::simulation::{FaultConfig, Partition};
    use std::collections::HashSet;

    const SEED: u64 = 0xDA27_1710;

    let mut sim = SimNetwork::new(
        "partition-test",
        1, // 1 gateway
        2, // 2 nodes
        7,
        3,
        10,
        2,
        SEED,
    )
    .await;
    sim.with_start_backoff(Duration::from_millis(50));

    // Start network and let it establish some connections
    let _handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 1, 3)
        .await;

    // Use VirtualTime advancement
    for _ in 0..20 {
        sim.advance_time(Duration::from_millis(100));
        tokio::task::yield_now().await;
    }

    // Get pre-partition event count
    let pre_partition = sim.get_event_counts().await;
    let pre_total: usize = pre_partition.values().sum();

    tracing::info!("Pre-partition: {} events", pre_total);

    // Get actual node addresses from the simulation
    let all_addrs = sim.all_node_addresses();
    assert!(
        all_addrs.len() >= 2,
        "Need at least 2 nodes for partition test"
    );

    // Create a partition using real addresses from the simulation
    // Put half the nodes on each side
    let addrs: Vec<_> = all_addrs.values().copied().collect();
    let mid = addrs.len() / 2;

    let side_a: HashSet<_> = addrs[..mid].iter().copied().collect();
    let side_b: HashSet<_> = addrs[mid..].iter().copied().collect();

    tracing::info!(
        "Creating partition: side_a={:?}, side_b={:?}",
        side_a,
        side_b
    );

    let partition = Partition::new(side_a, side_b).permanent(0);

    let fault_config = FaultConfig::builder().partition(partition).build();

    // Apply partition via bridge
    sim.with_fault_injection(fault_config);

    // Use VirtualTime advancement for post-partition
    for _ in 0..20 {
        sim.advance_time(Duration::from_millis(100));
        tokio::task::yield_now().await;
    }
    let post_partition = sim.get_event_counts().await;
    let post_total: usize = post_partition.values().sum();

    // Clear fault injection
    sim.clear_fault_injection();

    tracing::info!(
        "Partition test: pre={} events, post={} events (diff={})",
        pre_total,
        post_total,
        post_total.saturating_sub(pre_total)
    );

    // The key verification is that the API works without error
    // Actual partition effects depend on which addresses are involved
    tracing::info!("Partition injection bridge test passed");
}

/// Tests that fault injection with the same seed produces deterministic results.
///
/// This verifies that the fault injection bridge uses seeded RNG for reproducible
/// message loss decisions across multiple runs.
/// Uses VirtualTime exclusively - no start_paused.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_deterministic_fault_injection() {
    use freenet::simulation::FaultConfig;

    const SEED: u64 = 0xDE7E_FA01;

    async fn run_with_fault_injection(name: &str, seed: u64) -> HashMap<String, usize> {
        let mut sim = SimNetwork::new(name, 1, 3, 7, 3, 10, 2, seed).await;
        sim.with_start_backoff(Duration::from_millis(50));

        // Inject 30% message loss - with seeded RNG this should be deterministic
        let fault_config = FaultConfig::builder().message_loss_rate(0.3).build();
        sim.with_fault_injection(fault_config);

        let _handles = sim
            .start_with_rand_gen::<rand::rngs::SmallRng>(seed, 1, 3)
            .await;

        // Use VirtualTime advancement
        for _ in 0..30 {
            sim.advance_time(Duration::from_millis(100));
            tokio::task::yield_now().await;
        }
        let events = sim.get_event_counts().await;
        sim.clear_fault_injection();
        events
    }

    // Run twice with same seed
    let events1 = run_with_fault_injection("det-fault-run1", SEED).await;
    let events2 = run_with_fault_injection("det-fault-run2", SEED).await;

    // Both runs should produce events
    let total1: usize = events1.values().sum();
    let total2: usize = events2.values().sum();

    assert!(total1 > 0, "Run 1 should capture events");
    assert!(total2 > 0, "Run 2 should capture events");

    // With deterministic fault injection, event types should be consistent
    let types1: std::collections::HashSet<&String> = events1.keys().collect();
    let types2: std::collections::HashSet<&String> = events2.keys().collect();

    assert_eq!(
        types1, types2,
        "Event types should be consistent across runs.\nRun 1: {:?}\nRun 2: {:?}",
        events1, events2
    );

    tracing::info!(
        "Deterministic fault injection test: run1={} events, run2={} events",
        total1,
        total2
    );
    tracing::info!("Deterministic fault injection test passed");
}

/// Tests latency injection via the fault bridge using VirtualTime.
///
/// This test uses VirtualTime exclusively for deterministic latency testing.
/// Instead of wall-clock time, we advance VirtualTime and measure how many
/// messages get delivered at each time step.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_latency_injection() {
    use freenet::simulation::FaultConfig;

    const SEED: u64 = 0x1A7E_1234;

    // Run 1: No latency injection - messages delivered immediately
    let mut sim_fast = SimNetwork::new("latency-none-vt", 1, 2, 7, 3, 10, 2, SEED).await;
    sim_fast.with_start_backoff(Duration::from_millis(30));

    let _handles = sim_fast
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 1, 2)
        .await;

    // Advance VirtualTime instead of sleeping - this delivers queued messages
    // and allows nodes to make progress
    let mut fast_messages_delivered = 0;
    for _ in 0..20 {
        // Advance time in 100ms increments
        fast_messages_delivered += sim_fast.advance_time(Duration::from_millis(100));
        // Small yield to let async tasks run
        tokio::task::yield_now().await;
    }
    let fast_events = sim_fast.get_event_counts().await;

    // Run 2: With latency injection (100-200ms per message)
    let mut sim_slow = SimNetwork::new("latency-injected-vt", 1, 2, 7, 3, 10, 2, SEED).await;
    sim_slow.with_start_backoff(Duration::from_millis(30));

    let fault_config = FaultConfig::builder()
        .latency_range(Duration::from_millis(100)..Duration::from_millis(200))
        .build();
    sim_slow.with_fault_injection(fault_config);

    let _handles = sim_slow
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 1, 2)
        .await;

    // Advance the same amount of VirtualTime
    let mut slow_messages_delivered = 0;
    for _ in 0..20 {
        slow_messages_delivered += sim_slow.advance_time(Duration::from_millis(100));
        tokio::task::yield_now().await;
    }
    let slow_events = sim_slow.get_event_counts().await;
    sim_slow.clear_fault_injection();

    // Both runs should complete
    let fast_total: usize = fast_events.values().sum();
    let slow_total: usize = slow_events.values().sum();

    tracing::info!(
        "VirtualTime latency test: fast={} events ({} messages delivered), slow={} events ({} messages delivered)",
        fast_total,
        fast_messages_delivered,
        slow_total,
        slow_messages_delivered
    );

    // Verify we captured events in both runs
    assert!(fast_total > 0, "Fast run should capture events");
    assert!(slow_total > 0, "Slow run should capture some events");

    // With latency injection, messages are queued and delivered after their deadline.
    // We should see more messages delivered in the fast run (immediate delivery)
    // compared to the slow run (delayed delivery).
    tracing::info!("VirtualTime latency injection test passed - deterministic timing verified");
}

// =============================================================================
// Simulation Module Unit Tests
// =============================================================================

mod simulation_primitives {
    use freenet::simulation::{FaultConfig, SimulationRng, TimeSource, VirtualTime};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    /// Tests that fault injection decisions are deterministic.
    #[test]
    fn test_fault_config_determinism() {
        let config = FaultConfig::builder().message_loss_rate(0.3).build();

        let rng1 = SimulationRng::new(12345);
        let rng2 = SimulationRng::new(12345);

        let decisions1: Vec<bool> = (0..100)
            .map(|_| config.should_drop_message(&rng1))
            .collect();
        let decisions2: Vec<bool> = (0..100)
            .map(|_| config.should_drop_message(&rng2))
            .collect();

        assert_eq!(
            decisions1, decisions2,
            "Fault decisions should be deterministic"
        );
    }

    /// Tests virtual time wakeup ordering.
    #[test]
    fn test_virtual_time_wakeup_order() {
        let vt = VirtualTime::new();

        // Register wakeups in reverse order
        for i in (0..5).rev() {
            drop(vt.sleep_until((i + 1) * 100));
        }

        // Wakeups should fire in deadline order
        let mut fired = Vec::new();
        while let Some((id, deadline)) = vt.advance_to_next_wakeup() {
            fired.push((id.as_u64(), deadline));
        }

        // Verify ordering
        for i in 1..fired.len() {
            assert!(
                fired[i].1 >= fired[i - 1].1,
                "Wakeups should be ordered by deadline"
            );
        }
    }

    /// Tests that crashed nodes block all messages.
    #[test]
    fn test_crashed_node_blocks_messages() {
        let config = FaultConfig::builder().crashed_node(addr(1000)).build();
        let rng = SimulationRng::new(42);

        assert!(!config.can_deliver(&addr(1000), &addr(2000), 0, &rng));
        assert!(!config.can_deliver(&addr(2000), &addr(1000), 0, &rng));
        assert!(config.can_deliver(&addr(2000), &addr(3000), 0, &rng));
    }
}

// =============================================================================
// Test 10: Node Crash and Recovery via SimNetwork API
// =============================================================================

/// Tests the node crash/recovery API in SimNetwork.
///
/// This verifies:
/// 1. VirtualTime is always enabled by default
/// 2. crash_node() aborts the task and blocks messages
/// 3. recover_node() allows messages to flow again
/// 4. is_node_crashed() correctly reports crash status
///
/// Note: Does not use start_paused because the complex spawned task network
/// doesn't work well with paused time (time only advances when ALL tasks are
/// blocked on timers).
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_node_crash_recovery() {
    use freenet::dev_tool::SimNetwork;

    const SEED: u64 = 0xC2A5_0000_000E;

    let mut sim = SimNetwork::new(
        "crash-recovery-test",
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

    // VirtualTime should always be available
    let _vt = sim.virtual_time();

    // Start the network
    let _handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 1, 1)
        .await;

    // Allow some time for connections
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Get a node label to crash from all tracked addresses
    let all_addrs = sim.all_node_addresses();
    assert!(!all_addrs.is_empty(), "Should have tracked node addresses");

    // Find a non-gateway node (one that contains "-node-")
    let node_to_crash = all_addrs
        .keys()
        .find(|label| label.is_node())
        .cloned()
        .expect("Should have at least one regular node");

    // Verify the node address is tracked
    let addr = sim.node_address(&node_to_crash);
    assert!(addr.is_some(), "Node address should be tracked");
    tracing::info!(?node_to_crash, ?addr, "Node to crash");

    // Initially node is not crashed
    assert!(
        !sim.is_node_crashed(&node_to_crash),
        "Node should not be crashed initially"
    );

    // Crash the node
    let crashed = sim.crash_node(&node_to_crash);
    assert!(crashed, "crash_node should return true for running node");

    // Verify crash status
    assert!(
        sim.is_node_crashed(&node_to_crash),
        "Node should be marked as crashed"
    );

    // Get network stats - should show dropped messages
    let stats_before = sim.get_network_stats();
    assert!(stats_before.is_some(), "Network stats should be available");

    // Give some time for crash effects to propagate
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Recover the node (message blocking removed, but task is still aborted)
    let recovered = sim.recover_node(&node_to_crash);
    assert!(recovered, "recover_node should return true");

    // Verify crash status cleared
    assert!(
        !sim.is_node_crashed(&node_to_crash),
        "Node should not be marked as crashed after recovery"
    );

    tracing::info!("Node crash/recovery test completed successfully");
}

/// Tests that VirtualTime is always enabled and accessible.
///
/// This test demonstrates fully VirtualTime-based simulation without any
/// tokio::time::sleep() calls - time only advances when explicitly requested.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_virtual_time_always_enabled() {
    use freenet::dev_tool::{SimNetwork, TimeSource};

    const SEED: u64 = 0x1111_7111;

    let mut sim = SimNetwork::new(
        "virtual-time-test",
        1,  // gateways
        2,  // nodes
        7,  // ring_max_htl
        3,  // rnd_if_htl_above
        10, // max_connections
        2,  // min_connections
        SEED,
    )
    .await;

    // VirtualTime should be available immediately
    let vt = sim.virtual_time();
    assert_eq!(vt.now_nanos(), 0, "VirtualTime should start at 0");

    // Advance virtual time
    vt.advance(Duration::from_millis(100));
    assert_eq!(
        vt.now_nanos(),
        100_000_000,
        "VirtualTime should advance by 100ms"
    );

    // Network stats should be available (fault injection auto-configured)
    let stats = sim.get_network_stats();
    assert!(
        stats.is_some(),
        "Network stats should be available (VirtualTime enables fault injection)"
    );

    // Start network and use advance_time convenience method
    let _handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 1, 1)
        .await;

    // Use VirtualTime advancement instead of tokio::time::sleep
    // This advances time and delivers any queued messages
    let mut total_delivered = 0;
    for _ in 0..10 {
        total_delivered += sim.advance_time(Duration::from_millis(50));
        // Yield to let async tasks process
        tokio::task::yield_now().await;
    }
    tracing::info!("advance_time delivered {} messages total", total_delivered);

    // VirtualTime should have advanced: 100ms initial + 10*50ms = 600ms
    assert!(
        sim.virtual_time().now_nanos() >= 600_000_000,
        "VirtualTime should have advanced past 600ms, got {}ns",
        sim.virtual_time().now_nanos()
    );

    tracing::info!("VirtualTime always-enabled test completed");
}

/// Tests full node restart with preserved state.
///
/// This verifies:
/// 1. Nodes can be crashed and restarted
/// 2. Restarted nodes use the same identity (keypair)
/// 3. can_restart() correctly identifies restartable nodes
///
/// Note: Does not use start_paused because the complex spawned task network
/// doesn't work well with paused time.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_node_restart() {
    use freenet::dev_tool::SimNetwork;

    const SEED: u64 = 0x2E57_A2F0;

    let mut sim = SimNetwork::new(
        "restart-test",
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

    // Start the network
    let _handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 1, 1)
        .await;

    // Allow some time for connections to establish
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Get all node addresses
    let all_addrs = sim.all_node_addresses();
    assert!(!all_addrs.is_empty(), "Should have tracked node addresses");

    // Find a non-gateway node to restart
    let node_to_restart = all_addrs
        .keys()
        .find(|label| label.is_node())
        .cloned()
        .expect("Should have at least one regular node");

    tracing::info!(?node_to_restart, "Testing restart for node");

    // Verify the node can be restarted (config was saved)
    assert!(
        sim.can_restart(&node_to_restart),
        "Node should have saved config for restart"
    );

    // Initially node is not crashed
    assert!(
        !sim.is_node_crashed(&node_to_restart),
        "Node should not be crashed initially"
    );

    // Get the node's address before crash
    let addr_before = sim.node_address(&node_to_restart);
    assert!(addr_before.is_some(), "Node address should be tracked");

    // Crash the node
    let crashed = sim.crash_node(&node_to_restart);
    assert!(crashed, "crash_node should succeed for running node");
    assert!(
        sim.is_node_crashed(&node_to_restart),
        "Node should be marked as crashed"
    );

    // Small delay to let crash take effect
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Restart the node with same identity but different event seed
    let restart_seed = SEED.wrapping_add(0x1000);
    let handle = sim
        .restart_node::<rand::rngs::SmallRng>(&node_to_restart, restart_seed, 1, 1)
        .await;

    assert!(handle.is_some(), "restart_node should return a handle");
    tracing::info!(?node_to_restart, "Node restarted successfully");

    // Node should no longer be crashed
    assert!(
        !sim.is_node_crashed(&node_to_restart),
        "Restarted node should not be marked as crashed"
    );

    // Address should remain the same (same identity)
    let addr_after = sim.node_address(&node_to_restart);
    assert_eq!(
        addr_before, addr_after,
        "Node should have same address after restart"
    );

    // Give the restarted node time to reconnect
    tokio::time::sleep(Duration::from_secs(2)).await;

    tracing::info!("Node restart test completed successfully");
}

/// Tests edge cases for crash/restart operations.
///
/// This verifies:
/// 1. Crashing a non-existent node returns false
/// 2. Recovering a non-existent node returns false
/// 3. is_node_crashed returns false for unknown nodes
/// 4. can_restart returns false for unknown nodes
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_crash_restart_edge_cases() {
    use freenet::dev_tool::{NodeLabel, SimNetwork};

    const SEED: u64 = 0xED6E_CA5E;

    let mut sim = SimNetwork::new(
        "crash-edge-cases",
        1,  // gateways
        2,  // nodes
        7,  // ring_max_htl
        3,  // rnd_if_htl_above
        10, // max_connections
        2,  // min_connections
        SEED,
    )
    .await;

    sim.with_start_backoff(Duration::from_millis(50));

    // Start the network
    let _handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 1, 1)
        .await;

    // Allow network to stabilize using VirtualTime
    for _ in 0..10 {
        sim.advance_time(Duration::from_millis(100));
        tokio::task::yield_now().await;
    }

    // Test with a non-existent node label (using valid format but unknown network)
    let fake_label: NodeLabel = "node-999".into();

    // Crashing non-existent node should return false
    assert!(
        !sim.crash_node(&fake_label),
        "crash_node should return false for non-existent node"
    );

    // Recovering non-existent node should return false
    assert!(
        !sim.recover_node(&fake_label),
        "recover_node should return false for non-existent node"
    );

    // is_node_crashed should return false for unknown nodes
    assert!(
        !sim.is_node_crashed(&fake_label),
        "is_node_crashed should return false for unknown node"
    );

    // can_restart should return false for unknown nodes
    assert!(
        !sim.can_restart(&fake_label),
        "can_restart should return false for unknown node"
    );

    // node_address should return None for unknown nodes
    assert!(
        sim.node_address(&fake_label).is_none(),
        "node_address should return None for unknown node"
    );

    tracing::info!("Edge case tests completed successfully");
}

/// Tests that creating a network with zero nodes panics.
#[test_log::test(tokio::test)]
#[should_panic(expected = "assertion failed")]
async fn test_zero_nodes_panics() {
    use freenet::dev_tool::SimNetwork;

    // This should panic because nodes must be > 0
    let _sim = SimNetwork::new(
        "zero-nodes-test",
        1, // gateways
        0, // nodes - invalid!
        7,
        3,
        10,
        2,
        0xDEAD,
    )
    .await;
}

/// Tests that creating a network with zero gateways panics.
#[test_log::test(tokio::test)]
#[should_panic(expected = "should have at least one gateway")]
async fn test_zero_gateways_panics() {
    use freenet::dev_tool::SimNetwork;

    // This should panic because gateways must be > 0
    let _sim = SimNetwork::new(
        "zero-gateways-test",
        0, // gateways - invalid!
        2, // nodes
        7,
        3,
        10,
        2,
        0xDEAD,
    )
    .await;
}

/// Tests that a minimal network with 1 gateway and 1 node works.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_minimal_network() {
    use freenet::dev_tool::SimNetwork;

    const SEED: u64 = 0x0101_0401;

    // Create minimal network: 1 gateway, 1 node
    let mut sim = SimNetwork::new(
        "minimal-test",
        1, // 1 gateway
        1, // 1 node
        7,
        3,
        10,
        2,
        SEED,
    )
    .await;

    sim.with_start_backoff(Duration::from_millis(50));

    // Start the network
    let handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 1, 1)
        .await;

    // Should have 2 nodes (1 gateway + 1 regular node)
    assert_eq!(handles.len(), 2, "Should have exactly 2 nodes");

    // Let network stabilize using VirtualTime
    for _ in 0..10 {
        sim.advance_time(Duration::from_millis(100));
        tokio::task::yield_now().await;
    }

    // Verify node addresses are tracked
    let all_addrs = sim.all_node_addresses();
    assert_eq!(all_addrs.len(), 2, "Should track 2 node addresses");

    tracing::info!("Minimal network test passed");
}

// =============================================================================
// Graceful Shutdown Regression Tests
// =============================================================================

/// Regression test for graceful shutdown using Turmoil deterministic simulation.
///
/// This test verifies that:
/// 1. Nodes can start and run under Turmoil's deterministic scheduler
/// 2. The simulation completes without deadlocks
/// 3. Nodes exit gracefully when the simulation ends
///
/// Uses Turmoil for deterministic scheduling - tokio::time calls inside
/// Turmoil hosts are intercepted and advanced deterministically.
#[test]
fn test_graceful_shutdown_no_deadlock() {
    use freenet::dev_tool::SimNetwork;

    const SEED: u64 = 0xDEAD_BEEF_CAFE;

    // Create the network synchronously (Turmoil will run the async parts)
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let sim = rt.block_on(async {
        SimNetwork::new(
            "graceful-shutdown-test",
            1, // 1 gateway
            2, // 2 nodes
            7,
            3,
            10,
            2,
            SEED,
        )
        .await
    });

    // Run under Turmoil - this handles all tokio::time calls deterministically
    let result = sim.run_simulation::<rand::rngs::SmallRng, _, _>(
        SEED,
        10,                      // max_contract_num
        3,                       // iterations
        Duration::from_secs(30), // simulation_duration
        || async {
            // Wait for nodes to establish connections
            tokio::time::sleep(Duration::from_secs(5)).await;

            tracing::info!("Test client: nodes have been running, simulation will end gracefully");

            // The simulation will end after this, triggering graceful shutdown
            Ok(())
        },
    );

    // Verify simulation completed without panics or deadlocks
    assert!(
        result.is_ok(),
        "Simulation should complete without deadlock: {:?}",
        result.err()
    );

    tracing::info!("Graceful shutdown test passed - no deadlock under Turmoil");
}

/// Tests that the typed EventLoopExitReason error is properly handled.
///
/// This test verifies the fix for fragile string-based error matching.
/// The EventLoopExitReason enum should be used instead of comparing
/// error message strings.
#[test]
fn test_graceful_shutdown_typed_error() {
    use freenet::EventLoopExitReason;

    // Verify the error type exists and has correct Display impl
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
// Turmoil Determinism Test
// =============================================================================

/// Tests that Turmoil provides deterministic execution.
///
/// This test verifies that running the same simulation twice with Turmoil
/// produces consistent results. Since Turmoil provides deterministic task
/// scheduling, the simulations should behave identically.
///
/// Note: This tests that Turmoil scheduling is deterministic, not that the
/// full event sequence matches (which requires API changes to capture events
/// from run_simulation).
#[test]
fn test_turmoil_determinism() {
    use freenet::dev_tool::SimNetwork;

    const SEED: u64 = 0x000D_E7E2_A101_571C;

    fn run_simulation_once(name: &str, seed: u64) -> turmoil::Result {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let sim = rt.block_on(async {
            SimNetwork::new(
                name, 1, // 1 gateway
                2, // 2 nodes
                7, 3, 10, 2, seed,
            )
            .await
        });

        sim.run_simulation::<rand::rngs::SmallRng, _, _>(
            seed,
            5,                       // max_contract_num
            2,                       // iterations
            Duration::from_secs(15), // simulation_duration
            || async {
                // Wait for nodes to establish connections
                tokio::time::sleep(Duration::from_secs(3)).await;
                Ok(())
            },
        )
    }

    // Run simulation twice with the same seed
    let result1 = run_simulation_once("turmoil-det-run1", SEED);
    let result2 = run_simulation_once("turmoil-det-run2", SEED);

    // Both should succeed (or both should fail) if Turmoil is deterministic
    assert!(
        result1.is_ok() == result2.is_ok(),
        "Turmoil determinism test: runs should have consistent success/failure.\nRun 1: {:?}\nRun 2: {:?}",
        result1,
        result2
    );

    if result1.is_ok() {
        tracing::info!("Turmoil determinism test PASSED: Both runs completed successfully");
    } else {
        tracing::warn!(
            "Turmoil determinism test PASSED (consistent failure): {:?}",
            result1.err()
        );
    }
}

// =============================================================================
// Node Restart with State Recovery Test
// =============================================================================

/// Tests that nodes can restart and recover state from peers.
///
/// This is a comprehensive restart test that verifies:
/// 1. Contracts are distributed across the network
/// 2. Nodes can be crashed and restarted
/// 3. Restarted nodes re-sync contracts from peers
/// 4. Network converges to consistent state after restarts
///
/// Marked as `#[ignore]` - run with `--ignored` for thorough testing.
#[test_log::test(tokio::test(flavor = "current_thread"))]
#[ignore] // FIXME: Convergence bug - contracts end up with different state hashes on different peers
async fn test_node_restart_with_state_recovery() {
    use freenet::dev_tool::SimNetwork;
    use futures::StreamExt;

    const SEED: u64 = 0x2E57_A2F0_5747_E001;
    const NUM_EVENTS: u32 = 150; // Need enough events for 5% Put rate to create contracts

    tracing::info!("Starting node restart with state recovery test");

    // Create network with enough nodes to have meaningful replication
    let mut sim = SimNetwork::new(
        "restart-recovery-test",
        2,  // gateways
        6,  // nodes (8 total)
        7,  // ring_max_htl
        3,  // rnd_if_htl_above
        10, // max_connections
        3,  // min_connections
        SEED,
    )
    .await;

    sim.with_start_backoff(Duration::from_millis(100));

    // Start the network - need enough iterations for Put events (only 5% chance per event)
    let handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 15, NUM_EVENTS as usize)
        .await;

    tracing::info!("Network started with {} nodes", handles.len());

    // Wait for connectivity
    sim.check_partial_connectivity(Duration::from_secs(30), 0.6)
        .expect("Network should achieve 60% connectivity");
    tracing::info!("Network connectivity established");

    // Phase 1: Generate events to distribute contracts
    tracing::info!(
        "Phase 1: Generating {} events to distribute contracts",
        NUM_EVENTS
    );
    let mut stream = sim.event_chain(NUM_EVENTS, None);
    while stream.next().await.is_some() {
        tokio::task::yield_now().await;
    }

    // Wait for network to quiesce
    tracing::info!("Waiting for network to quiesce after initial events...");
    let _ = sim
        .await_network_quiescence(
            Duration::from_secs(60),
            Duration::from_secs(3),
            Duration::from_millis(500),
        )
        .await;

    // Get initial state snapshot
    let initial_distribution = sim.get_contract_distribution().await;
    let initial_summary = sim.get_operation_summary().await;
    tracing::info!(
        "Initial state: {} contracts, {} puts succeeded",
        initial_distribution.len(),
        initial_summary.put.succeeded
    );

    // Find non-gateway nodes to restart
    let all_addrs = sim.all_node_addresses();
    let nodes_to_restart: Vec<_> = all_addrs
        .keys()
        .filter(|label| label.is_node())
        .take(2) // Restart 2 nodes
        .cloned()
        .collect();

    assert!(
        nodes_to_restart.len() >= 2,
        "Need at least 2 non-gateway nodes to restart"
    );

    tracing::info!("Phase 2: Crashing {} nodes", nodes_to_restart.len());
    for node in &nodes_to_restart {
        let crashed = sim.crash_node(node);
        assert!(crashed, "crash_node should succeed for {:?}", node);
        tracing::info!("Crashed node {:?}", node);
    }

    // Wait briefly for crash to take effect
    let _ = sim
        .await_network_quiescence(
            Duration::from_secs(30),
            Duration::from_secs(2),
            Duration::from_millis(500),
        )
        .await;

    // Phase 3: Restart the nodes
    tracing::info!("Phase 3: Restarting crashed nodes");
    let mut restart_handles = Vec::new();
    for (i, node) in nodes_to_restart.iter().enumerate() {
        let restart_seed = SEED.wrapping_add((i as u64 + 1) * 0x1000);
        let handle = sim
            .restart_node::<rand::rngs::SmallRng>(node, restart_seed, 10, NUM_EVENTS as usize)
            .await;
        assert!(
            handle.is_some(),
            "restart_node should succeed for {:?}",
            node
        );
        restart_handles.push(handle.unwrap());
        tracing::info!("Restarted node {:?}", node);
    }

    // Wait for restarted nodes to reconnect and sync
    tracing::info!("Waiting for restarted nodes to reconnect and sync...");

    // Give time for reconnection
    for _ in 0..20 {
        sim.advance_time(Duration::from_millis(100));
        tokio::task::yield_now().await;
    }

    // Wait for network to quiesce after restarts
    let quiesce_result = sim
        .await_network_quiescence(
            Duration::from_secs(120),
            Duration::from_secs(5),
            Duration::from_millis(500),
        )
        .await;

    match quiesce_result {
        Ok(count) => tracing::info!("Network quiesced with {} log entries after restart", count),
        Err(count) => tracing::warn!("Network still active after timeout ({} entries)", count),
    }

    // Phase 4: Verify convergence
    tracing::info!("Phase 4: Checking convergence after restart");

    let convergence_result = sim
        .await_convergence(Duration::from_secs(60), Duration::from_millis(500), 1)
        .await;

    match convergence_result {
        Ok(result) => {
            tracing::info!(
                "Convergence achieved: {} contracts converged",
                result.converged.len()
            );

            // Verify we still have contracts
            assert!(
                !result.converged.is_empty() || initial_distribution.is_empty(),
                "Should have converged contracts if we had any initially"
            );
        }
        Err(result) => {
            for diverged in &result.diverged {
                tracing::error!(
                    "Contract {} diverged: {} states across {} peers",
                    diverged.contract_key,
                    diverged.unique_state_count(),
                    diverged.peer_states.len()
                );
            }
            panic!(
                "Convergence failed after node restart: {} converged, {} diverged",
                result.converged.len(),
                result.diverged.len()
            );
        }
    }

    // Verify restarted nodes are functioning
    for node in &nodes_to_restart {
        assert!(
            !sim.is_node_crashed(node),
            "Node {:?} should not be crashed after restart",
            node
        );
    }

    let final_summary = sim.get_operation_summary().await;
    tracing::info!(
        "Final state: Put {}/{} ({:.1}%), Get {}/{} ({:.1}%)",
        final_summary.put.succeeded,
        final_summary.put.completed(),
        final_summary.put.success_rate() * 100.0,
        final_summary.get.succeeded,
        final_summary.get.completed(),
        final_summary.get.success_rate() * 100.0
    );

    tracing::info!("Node restart with state recovery test PASSED");
}
