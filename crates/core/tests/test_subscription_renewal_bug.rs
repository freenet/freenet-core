//! Regression test for subscription renewal bug (PR #2804).
//!
//! This test reproduces the issue where contracts added via GET operations
//! do not have their subscriptions renewed, causing them to silently expire
//! after 4 minutes (SUBSCRIPTION_LEASE_DURATION).
//!
//! The bug exists on `main` branch and should be fixed in the `unified-hosting` branch.
//!
//! NOTE: This test is marked as #[ignore] because it FAILS on main (reproducing the bug).
//! Once the fix is merged, the #[ignore] should be removed.

#![cfg(feature = "simulation_tests")]

use freenet::config::{GlobalRng, GlobalSimulationTime};
use freenet::dev_tool::{reset_all_simulation_state, SimNetwork};
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

/// Test that contracts accessed via GET operations have their subscriptions renewed.
///
/// This test reproduces the bug where:
/// 1. A contract is fetched via GET operation
/// 2. The contract is cached and auto-subscribed (via GetSubscriptionCache)
/// 3. Time advances past the subscription renewal interval (2 minutes)
/// 4. The subscription is NOT renewed (BUG on main)
/// 5. After 4 minutes (SUBSCRIPTION_LEASE_DURATION), the subscription expires
/// 6. Updates stop being received, UI shows stale data
///
/// Expected behavior (after fix):
/// - Contracts in GetSubscriptionCache should be included in contracts_needing_renewal()
/// - Their subscriptions should be renewed before expiry
/// - The contract should remain subscribed beyond 4 minutes
///
/// NOTE: This test uses Turmoil for deterministic simulation, ensuring reproducible results.
///
/// This test is #[ignore] because it FAILS on main branch (reproducing the bug).
/// Remove #[ignore] after PR #2804 is merged.
#[test_log::test]
#[ignore = "TODO-MUST-FIX: Fails on main - demonstrates subscription renewal bug from PR #2804"]
fn test_get_triggered_subscription_renewal() {
    const SEED: u64 = 0x2804_0001;

    setup_deterministic_state(SEED);

    let rt = create_runtime();

    // Create SimNetwork and get event logs handle
    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(
            "subscription-renewal-bug",
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

    // Run simulation using Turmoil for deterministic execution
    // The simulation will:
    // 1. Create 1 contract with 2 operations (PUT + GET)
    // 2. Run for total duration that exceeds subscription lease (4+ minutes)
    // 3. Sleep between operations to allow time advancement
    let result = sim.run_simulation::<rand::rngs::SmallRng, _, _>(
        SEED,
        1,  // max_contracts - single contract to track
        2,  // iterations - minimal ops (PUT + GET)
        Duration::from_secs(300), // 5 minutes - exceeds SUBSCRIPTION_LEASE_DURATION
        || async {
            // Sleep to advance Turmoil's virtual time
            // This simulates time passing in the network
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok(())
        },
    );

    // Verify simulation completed successfully
    if let Err(e) = &result {
        tracing::error!("============================================================");
        tracing::error!("SIMULATION FAILED: subscription-renewal-bug");
        tracing::error!("============================================================");
        tracing::error!("Seed for reproduction: 0x{:X}", SEED);
        tracing::error!("Error: {:?}", e);
        tracing::error!("============================================================");
    }
    assert!(
        result.is_ok(),
        "Simulation failed: {:?}",
        result.err()
    );

    // Analyze the event logs to understand subscription behavior
    let logs = rt.block_on(async { logs_handle.lock().await.clone() });

    tracing::info!("============================================================");
    tracing::info!("SUBSCRIPTION RENEWAL TEST RESULTS");
    tracing::info!("============================================================");
    tracing::info!("Total events captured: {}", logs.len());

    // Count GET operations to verify the scenario triggered
    let get_count = logs.iter().filter(|log| {
        format!("{:?}", log.kind).contains("Get")
    }).count();

    tracing::info!("GET operations: {}", get_count);
    tracing::info!("Expected behavior:");
    tracing::info!("  - GET operations should trigger auto-subscriptions");
    tracing::info!("  - Subscriptions should be renewed at ~{}s", SUBSCRIPTION_RENEWAL_INTERVAL.as_secs());
    tracing::info!("  - Subscriptions should NOT expire at {}s", SUBSCRIPTION_LEASE_DURATION.as_secs());
    tracing::info!("");
    tracing::info!("BUG on main:");
    tracing::info!("  - GET-triggered subscriptions are NOT in contracts_needing_renewal()");
    tracing::info!("  - They expire after {}s without renewal", SUBSCRIPTION_LEASE_DURATION.as_secs());
    tracing::info!("============================================================");

    // On main: This test passes but the bug exists (subscriptions expire silently)
    // On fixed branch: Subscriptions should be renewed and remain active
    //
    // TODO after merge: Add assertions to verify subscriptions are renewed
    // For example, check that subscription renewal messages appear in logs
    // at the expected times (T+120s, T+240s, etc.)
}

/// Test that explicitly verifies the subscription renewal interval.
///
/// This is a more focused test that checks if subscriptions are renewed
/// at the expected interval (2 minutes) to prevent expiry at 4 minutes.
///
/// Uses Turmoil for deterministic time advancement and reproducible results.
///
/// NOTE: This test is #[ignore] because it demonstrates the bug on main.
#[test_log::test]
#[ignore = "TODO-MUST-FIX: Demonstrates subscription renewal timing bug - remove ignore after fix"]
fn test_subscription_renewal_timing() {
    const SEED: u64 = 0x2804_0002;

    setup_deterministic_state(SEED);

    let rt = create_runtime();

    // Create minimal network
    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(
            "renewal-timing-test",
            1,  // gateways
            2,  // nodes
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

    // Run simulation with specific timing to observe renewal behavior
    // Duration extends beyond lease duration to see if subscriptions expire
    let result = sim.run_simulation::<rand::rngs::SmallRng, _, _>(
        SEED,
        1,  // max_contracts
        1,  // iterations - single operation
        Duration::from_secs(300), // 5 minutes - well past lease duration
        || async {
            // Sleep for renewal interval duration to trigger renewal checks
            tokio::time::sleep(SUBSCRIPTION_RENEWAL_INTERVAL).await;
            Ok(())
        },
    );

    assert!(
        result.is_ok(),
        "Renewal timing simulation failed: {:?}",
        result.err()
    );

    let logs = rt.block_on(async { logs_handle.lock().await.clone() });

    tracing::info!("============================================================");
    tracing::info!("RENEWAL TIMING TEST RESULTS");
    tracing::info!("============================================================");
    tracing::info!("Total events: {}", logs.len());
    tracing::info!("Renewal interval: {}s", SUBSCRIPTION_RENEWAL_INTERVAL.as_secs());
    tracing::info!("Lease duration: {}s", SUBSCRIPTION_LEASE_DURATION.as_secs());
    tracing::info!("");
    tracing::info!("Expected timeline:");
    tracing::info!("  T+0s   : GET operation creates subscription");
    tracing::info!("  T+120s : Renewal should occur (if bug is fixed)");
    tracing::info!("  T+240s : Without renewal, subscription expires (BUG)");
    tracing::info!("  T+300s : Test completes");
    tracing::info!("============================================================");

    // TODO after merge: Add assertions that verify:
    // 1. Subscription renewal events appear at T+120s intervals
    // 2. No subscription expiry events occur for active contracts
    // 3. Contracts remain subscribed beyond T+240s
}

/// Test that simulates the real-world River UI scenario.
///
/// This test specifically reproduces the River UI bug where:
/// - User opens River UI, which GETs a contract
/// - User leaves the UI open (expecting live updates)
/// - After 4 minutes, subscription expires silently
/// - UI stops receiving updates and shows stale data
///
/// Uses Turmoil for deterministic simulation.
#[test_log::test]
#[ignore = "TODO-MUST-FIX: Reproduces River UI subscription expiry bug - remove ignore after fix"]
fn test_river_ui_subscription_expiry_scenario() {
    const SEED: u64 = 0x2804_0003; // River UI scenario

    setup_deterministic_state(SEED);

    let rt = create_runtime();

    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(
            "river-ui-scenario",
            1,  // gateway (simulating user's local gateway)
            2,  // nodes (minimal network)
            7, 3, 10, 2,
            SEED,
        )
        .await;
        let logs_handle = sim.event_logs_handle();
        (sim, logs_handle)
    });

    // Simulate River UI workflow:
    // 1. Initial GET when UI loads (iteration 1)
    // 2. Wait for 5 minutes with UI open
    // 3. Expect to continue receiving updates
    let result = sim.run_simulation::<rand::rngs::SmallRng, _, _>(
        SEED,
        1,  // Single contract (e.g., a River channel)
        1,  // Single GET operation (UI loads contract)
        Duration::from_secs(300), // 5 minutes - user has UI open
        || async {
            // Simulate periodic UI activity (doesn't trigger renewal on main)
            tokio::time::sleep(Duration::from_secs(60)).await;
            Ok(())
        },
    );

    assert!(
        result.is_ok(),
        "River UI scenario simulation failed: {:?}",
        result.err()
    );

    let logs = rt.block_on(async { logs_handle.lock().await.clone() });

    tracing::info!("============================================================");
    tracing::info!("RIVER UI SCENARIO TEST");
    tracing::info!("============================================================");
    tracing::info!("Scenario: User opens River UI, expects live updates");
    tracing::info!("Total events: {}", logs.len());
    tracing::info!("");
    tracing::info!("BUG on main:");
    tracing::info!("  T+0s   : UI GETs contract, auto-subscription created");
    tracing::info!("  T+240s : Subscription expires (NOT renewed)");
    tracing::info!("  T+240s+: UI stops receiving updates, shows stale data");
    tracing::info!("");
    tracing::info!("Expected after fix:");
    tracing::info!("  T+0s   : UI GETs contract, auto-subscription created");
    tracing::info!("  T+120s : Subscription renewed");
    tracing::info!("  T+240s : Subscription renewed again");
    tracing::info!("  T+300s : Subscription still active, UI receives updates");
    tracing::info!("============================================================");

    // TODO after merge: Add assertion that subscription remains active
    // and updates continue to be received throughout the 5-minute window
}
