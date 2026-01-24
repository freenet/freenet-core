//! Regression test for subscription renewal bug (PR #2804).
//!
//! This test reproduces the issue where contracts added via GET operations
//! do not have their subscriptions renewed, causing them to silently expire
//! after 4 minutes (SUBSCRIPTION_LEASE_DURATION).
//!
//! The bug exists on `main` branch and should be fixed in the `unified-hosting` branch.

#![cfg(all(feature = "simulation_tests", feature = "testing"))]

use freenet::config::{GlobalRng, GlobalSimulationTime};
use freenet::dev_tool::{reset_all_simulation_state, NodeLabel, ScheduledOperation, SimNetwork, SimOperation};
use std::time::Duration;

// These constants are from crates/core/src/ring/seeding.rs
// but the ring module is not publicly exported
const SUBSCRIPTION_LEASE_DURATION: Duration = Duration::from_secs(240); // 4 minutes
const SUBSCRIPTION_RENEWAL_INTERVAL: Duration = Duration::from_secs(120); // 2 minutes

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

/// Test that contracts accessed ONLY via GET operations have their subscriptions renewed.
///
/// This test uses controlled operations (no random event generation) to isolate
/// the GET-only subscription scenario:
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
    // 2. GET without subscribe (node accesses it, triggering auto-subscription)
    let operations = vec![
        ScheduledOperation::new(
            NodeLabel::gateway("get-only-renewal", 0),
            SimOperation::Put {
                contract: contract.clone(),
                state: state.clone(),
                subscribe: false, // Critical: no explicit subscription on PUT
            },
        ),
        ScheduledOperation::new(
            NodeLabel::node("get-only-renewal", 0),
            SimOperation::Get {
                contract_id,
                return_contract_code: true,
                subscribe: false, // Critical: no explicit subscription on GET either
            },
        ),
    ];

    tracing::info!("============================================================");
    tracing::info!("Starting controlled simulation - GET-only subscriptions");
    tracing::info!("Operations scheduled:");
    tracing::info!("  1. PUT contract (no subscribe)");
    tracing::info!("  2. GET contract (no explicit subscribe - auto-subscription only)");
    tracing::info!("Duration: 5 minutes (exceeds {SUBSCRIPTION_LEASE_DURATION:?} lease)");
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

    // Analyze Subscribe event types
    let mut initial_subscribe_count = 0;
    let mut renewal_subscribe_count = 0;

    for (idx, event) in subscribe_events.iter().enumerate() {
        let kind_str = format!("{:?}", event.kind);
        tracing::info!("  [{}] {}", idx, kind_str);

        // Try to determine if this is initial or renewal
        // Initial Subscribe would happen shortly after GET
        // Renewals would happen at T+120s, T+240s intervals
        if kind_str.contains("Request") || kind_str.contains("Success") {
            // This is an approximation - ideally we'd check timestamps
            if idx == 0 {
                initial_subscribe_count += 1;
            } else {
                renewal_subscribe_count += 1;
            }
        }
    }

    tracing::info!("");
    tracing::info!("Subscribe event breakdown:");
    tracing::info!("  Initial subscriptions: ~{}", initial_subscribe_count);
    tracing::info!("  Renewal subscriptions: ~{}", renewal_subscribe_count);
    tracing::info!("");
    tracing::info!("Expected behavior:");
    tracing::info!("  - Initial Subscribe after GET: 1 event");
    tracing::info!("  - Renewals at T+120s, T+240s: 2+ events");
    tracing::info!("  - Total expected: 3+ Subscribe events");
    tracing::info!("");
    tracing::info!("Bug behavior (on main):");
    tracing::info!("  - Initial Subscribe after GET: 1 event");
    tracing::info!("  - No renewals (GetSubscriptionCache not checked)");
    tracing::info!("  - Total: Only 1 Subscribe event");
    tracing::info!("============================================================");

    // CRITICAL ASSERTION
    // With a 5-minute simulation and 2-minute renewal interval,
    // we should see:
    // - T+0s: Initial Subscribe (after GET triggers auto-subscription)
    // - T+120s: First renewal
    // - T+240s: Second renewal
    // - T+360s would be third but simulation ends at T+300s
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

/// Baseline test that verifies the simulation infrastructure works.
///
/// This test uses random event generation (which includes explicit Subscribe operations)
/// and should always pass. It serves as a sanity check that the simulation framework
/// is functioning correctly.
#[test_log::test]
fn test_subscription_baseline_with_random_events() {
    const SEED: u64 = 0x2804_0000;

    setup_deterministic_state(SEED);

    let rt = create_runtime();

    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(
            "subscription-baseline",
            1, 3, 7, 3, 10, 2,
            SEED,
        )
        .await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    // Run with random events (includes explicit Subscribe operations)
    let result = sim.run_simulation::<rand::rngs::SmallRng, _, _>(
        SEED,
        2,
        30,
        Duration::from_secs(180),
        || async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok(())
        },
    );

    assert!(result.is_ok(), "Baseline simulation failed: {:?}", result.err());

    let logs = rt.block_on(async { logs_handle.lock().await.clone() });
    let subscribe_count = logs.iter().filter(|log| {
        format!("{:?}", log.kind).contains("Subscribe")
    }).count();

    tracing::info!("Baseline test: {} events, {} Subscribe events", logs.len(), subscribe_count);

    // Just verify we captured events
    assert!(logs.len() > 0, "Should have captured some events");
}
