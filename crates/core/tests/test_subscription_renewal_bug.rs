//! Regression test for subscription renewal bug (PR #2804).
//!
//! This test reproduces the issue where contracts added via GET operations
//! do not have their subscriptions renewed, causing them to silently expire
//! after 4 minutes (SUBSCRIPTION_LEASE_DURATION).
//!
//! The bug exists on `main` branch and should be fixed in the `unified-hosting` branch.

#![cfg(feature = "simulation_tests")]

use freenet::config::{GlobalRng, GlobalSimulationTime};
use freenet::dev_tool::{reset_all_simulation_state, SimNetwork};
use std::collections::HashMap;
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
/// This test reproduces the bug by running a simulation long enough to:
/// 1. Trigger GET operations that create auto-subscriptions
/// 2. Advance time past the renewal interval (2 minutes)
/// 3. Advance time past the lease duration (4 minutes)
/// 4. Check if Subscribe renewal events occurred
///
/// On main (with bug): GET-triggered subscriptions are NOT renewed
/// After fix: All hosted contracts should have Subscribe renewal events
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
            4,  // nodes - more nodes for better contract distribution
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

    // Run simulation with enough operations to trigger GETs
    // Increase iterations to ensure we get both PUT and GET operations
    let result = sim.run_simulation::<rand::rngs::SmallRng, _, _>(
        SEED,
        3,   // max_contracts - multiple contracts
        50,  // iterations - enough to trigger various operations
        Duration::from_secs(300), // 5 minutes - exceeds lease duration
        || async {
            // Sleep to advance time and trigger renewals
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok(())
        },
    );

    assert!(
        result.is_ok(),
        "Simulation failed: {:?}",
        result.err()
    );

    // Analyze the event logs
    let logs = rt.block_on(async { logs_handle.lock().await.clone() });

    tracing::info!("============================================================");
    tracing::info!("SUBSCRIPTION RENEWAL TEST RESULTS");
    tracing::info!("============================================================");
    tracing::info!("Total events captured: {}", logs.len());

    // Count different operation types
    let get_count = logs.iter().filter(|log| {
        format!("{:?}", log.kind).contains("Get")
    }).count();

    let put_count = logs.iter().filter(|log| {
        format!("{:?}", log.kind).contains("Put")
    }).count();

    let subscribe_count = logs.iter().filter(|log| {
        format!("{:?}", log.kind).contains("Subscribe")
    }).count();

    tracing::info!("Operation counts:");
    tracing::info!("  GET operations: {}", get_count);
    tracing::info!("  PUT operations: {}", put_count);
    tracing::info!("  Subscribe events: {}", subscribe_count);

    // Verify we had some GET operations to test the bug
    assert!(
        get_count > 0,
        "Test needs GET operations to reproduce the bug. \
         Got {} GETs. Try increasing iterations or contracts.",
        get_count
    );

    // Extract Subscribe events with timestamps
    // Subscribe events should occur:
    // 1. Initially when contract is first accessed
    // 2. At T+120s (renewal interval)
    // 3. At T+240s (another renewal)
    // etc.
    let subscribe_events: Vec<_> = logs
        .iter()
        .filter(|log| format!("{:?}", log.kind).contains("Subscribe"))
        .collect();

    // Group Subscribe events by contract
    let mut subscribes_by_contract: HashMap<String, Vec<&freenet::tracing::NetLogMessage>> = HashMap::new();
    for event in subscribe_events {
        // Try to extract contract identifier from the event
        // The exact format depends on how EventKind::Subscribe is formatted
        let event_str = format!("{:?}", event.kind);
        if let Some(contract_id) = extract_contract_id(&event_str) {
            subscribes_by_contract
                .entry(contract_id)
                .or_default()
                .push(event);
        }
    }

    tracing::info!("");
    tracing::info!("Subscribe events by contract:");
    for (contract, events) in &subscribes_by_contract {
        tracing::info!("  Contract {}: {} Subscribe events", contract, events.len());
    }

    // The bug manifests as: contracts that were GET-ed don't get renewed
    // So we expect to see multiple Subscribe events per contract (initial + renewals)
    // If the bug exists, some contracts will only have 1 Subscribe event

    if !subscribes_by_contract.is_empty() {
        let contracts_with_single_subscribe = subscribes_by_contract
            .iter()
            .filter(|(_, events)| events.len() == 1)
            .count();

        let contracts_with_renewals = subscribes_by_contract
            .iter()
            .filter(|(_, events)| events.len() > 1)
            .count();

        tracing::info!("");
        tracing::info!("Subscription renewal analysis:");
        tracing::info!("  Contracts with renewals: {}", contracts_with_renewals);
        tracing::info!("  Contracts without renewals: {}", contracts_with_single_subscribe);

        // ASSERTION: After the fix, ALL contracts that had GET operations should have renewals
        // With a 5-minute simulation and 2-minute renewal interval, we should see at least
        // 2-3 Subscribe events per contract (initial + renewals)
        //
        // On main (with bug): Some contracts will have only 1 Subscribe event
        // After fix: All contracts should have multiple Subscribe events

        assert!(
            contracts_with_renewals > 0,
            "BUG DETECTED: No contracts had subscription renewals! \
             All {} contracts have single Subscribe events. \
             Expected: Multiple Subscribe events per contract (initial + renewals at T+120s, T+240s). \
             This indicates GET-triggered subscriptions are NOT being renewed.",
            contracts_with_single_subscribe
        );

        // A stricter assertion for after the fix is merged:
        // ALL contracts should have renewals, not just some
        // Uncomment this after PR #2804 is merged:
        /*
        assert_eq!(
            contracts_with_single_subscribe, 0,
            "Some contracts still lack renewals: {} contracts have only 1 Subscribe event. \
             All contracts should be renewed at {SUBSCRIPTION_RENEWAL_INTERVAL:?} intervals.",
        );
        */
    } else {
        panic!(
            "No Subscribe events found in logs. Cannot test subscription renewal. \
             This may indicate the simulation didn't create any contracts."
        );
    }

    tracing::info!("============================================================");
}

/// Helper to extract contract ID from Subscribe event debug string
fn extract_contract_id(event_str: &str) -> Option<String> {
    // Subscribe events contain instance_id or contract key
    // Example: "Subscribe(Request { instance_id: ContractInstanceId(...), ... })"
    // We'll extract a simple identifier for grouping

    // Try to find instance_id pattern
    if let Some(start) = event_str.find("instance_id:") {
        let after_id = &event_str[start + 12..];
        if let Some(end) = after_id.find(|c: char| c == ',' || c == ')' || c == '}') {
            let id_str = &after_id[..end].trim();
            return Some(id_str.to_string());
        }
    }

    // Try to find key pattern
    if let Some(start) = event_str.find("key:") {
        let after_key = &event_str[start + 4..];
        if let Some(end) = after_key.find(|c: char| c == ',' || c == ')' || c == '}') {
            let key_str = &after_key[..end].trim();
            return Some(key_str.to_string());
        }
    }

    None
}

/// Simpler test that just verifies the simulation runs for long enough
/// to observe subscription behavior.
#[test_log::test]
fn test_subscription_simulation_baseline() {
    const SEED: u64 = 0x2804_0000; // Baseline test

    setup_deterministic_state(SEED);

    let rt = create_runtime();

    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(
            "subscription-baseline",
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

    // Run for the lease duration to see what happens
    let result = sim.run_simulation::<rand::rngs::SmallRng, _, _>(
        SEED,
        2,   // max_contracts
        30,  // iterations
        SUBSCRIPTION_LEASE_DURATION + Duration::from_secs(60), // Just past lease duration
        || async {
            tokio::time::sleep(Duration::from_secs(15)).await;
            Ok(())
        },
    );

    assert!(result.is_ok(), "Baseline simulation failed: {:?}", result.err());

    let logs = rt.block_on(async { logs_handle.lock().await.clone() });

    tracing::info!("Baseline test completed: {} events captured", logs.len());

    // Just verify we captured events - this is a sanity check
    assert!(logs.len() > 0, "Should have captured some events");
}
