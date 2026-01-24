//! Debug test for subscription renewal mechanism
//!
//! This test adds extensive logging to understand why renewal Subscribe events
//! aren't appearing in simulation tests.

#![cfg(feature = "simulation_tests")]

use freenet::config::{GlobalRng, GlobalSimulationTime};
use freenet::dev_tool::{reset_all_simulation_state, NodeLabel, ScheduledOperation, SimNetwork, SimOperation};
use std::time::Duration;

fn setup_deterministic_state(seed: u64) {
    reset_all_simulation_state();
    GlobalRng::set_seed(seed);
    const BASE_EPOCH_MS: u64 = 1577836800000;
    const RANGE_MS: u64 = 5 * 365 * 24 * 60 * 60 * 1000;
    GlobalSimulationTime::set_time_ms(BASE_EPOCH_MS + (seed % RANGE_MS));
}

fn create_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime")
}

/// Debug test with extensive logging to track:
/// 1. GET subscription storage in GetSubscriptionCache
/// 2. Renewal task execution
/// 3. contracts_needing_renewal() calls
/// 4. Subscribe event emission
#[test_log::test]
#[ignore = "Debug test - run manually with --nocapture to see detailed logs"]
fn debug_subscription_renewal_mechanism() {
    const SEED: u64 = 0x2804_DE60;

    setup_deterministic_state(SEED);

    let rt = create_runtime();

    tracing::info!("╔════════════════════════════════════════════════════════════════╗");
    tracing::info!("║ DEBUG TEST: Subscription Renewal Mechanism                     ║");
    tracing::info!("╚════════════════════════════════════════════════════════════════╝");
    tracing::info!("");
    tracing::info!("This test tracks:");
    tracing::info!("  1. GET subscription storage (GetSubscriptionCache)");
    tracing::info!("  2. Renewal task execution (every 30s after 30-60s delay)");
    tracing::info!("  3. contracts_needing_renewal() calls");
    tracing::info!("  4. Subscribe event emission");
    tracing::info!("");
    tracing::info!("Expected timeline:");
    tracing::info!("  T+0s: PUT contract at gateway");
    tracing::info!("  T+0s: GET at node-3 -> stores in GetSubscriptionCache");
    tracing::info!("  T+30-60s: First renewal task run (initial random delay)");
    tracing::info!("  T+60-90s: Second renewal task run");
    tracing::info!("  T+90-120s: Third renewal task run");
    tracing::info!("  T+120-150s: Fourth renewal task run <- should see Subscribe events");
    tracing::info!("");

    // Create small network first to test local subscription path
    let (sim, logs_handle) = rt.block_on(async {
        let sim = SimNetwork::new(
            "debug-renewal",
            1,  // gateways
            2,  // nodes - small network to see local subscription behavior
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

    let contract = SimOperation::create_test_contract(42);
    let state = SimOperation::create_test_state(42);
    let contract_id = *contract.key().id();

    tracing::info!("Contract ID: {}", contract_id);
    tracing::info!("Contract Key: {:?}", contract.key());
    tracing::info!("");

    let operations = vec![
        ScheduledOperation::new(
            NodeLabel::gateway("debug-renewal", 0),
            SimOperation::Put {
                contract: contract.clone(),
                state: state.clone(),
                subscribe: false,
            },
        ),
        ScheduledOperation::new(
            NodeLabel::node("debug-renewal", 1),
            SimOperation::Get {
                contract_id,
                return_contract_code: false,
                subscribe: true,
            },
        ),
    ];

    tracing::info!("Starting simulation (180 seconds)...");
    tracing::info!("");

    // Run for 3 minutes to see multiple renewal cycles
    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(180),
        Duration::from_secs(170),
    );

    assert!(
        result.turmoil_result.is_ok(),
        "Simulation failed: {:?}",
        result.turmoil_result.err()
    );

    tracing::info!("");
    tracing::info!("╔════════════════════════════════════════════════════════════════╗");
    tracing::info!("║ TRACE LOG ANALYSIS - Searching for GetSubscriptionCache logs   ║");
    tracing::info!("╚════════════════════════════════════════════════════════════════╝");
    tracing::info!("");
    tracing::info!("Looking for key log messages:");
    tracing::info!("  1. 'Recorded subscription in GetSubscriptionCache' (GET operation)");
    tracing::info!("  2. 'checked active_subscriptions and client_subscriptions' (renewal)");
    tracing::info!("  3. Absence of GET contracts in renewal checks (demonstrates bug)");
    tracing::info!("");

    let logs = rt.block_on(async { logs_handle.lock().await.clone() });

    tracing::info!("Total events captured: {}", logs.len());

    // Group events by type
    let mut event_type_counts = std::collections::HashMap::new();
    for log in &logs {
        let kind_str = format!("{:?}", log.kind);
        let event_type = kind_str.split('(').next().unwrap_or(&kind_str);
        *event_type_counts.entry(event_type.to_string()).or_insert(0) += 1;
    }

    tracing::info!("");
    tracing::info!("Event type distribution:");
    let mut sorted_types: Vec<_> = event_type_counts.iter().collect();
    sorted_types.sort_by_key(|(_, count)| std::cmp::Reverse(*count));
    for (event_type, count) in sorted_types {
        tracing::info!("  {:20} : {}", event_type, count);
    }

    // Focus on Subscribe events
    let subscribe_events: Vec<_> = logs
        .iter()
        .filter(|log| {
            let kind_str = format!("{:?}", log.kind);
            kind_str.contains("Subscribe")
        })
        .collect();

    tracing::info!("");
    tracing::info!("Subscribe events found: {}", subscribe_events.len());

    if subscribe_events.is_empty() {
        tracing::warn!("");
        tracing::warn!("⚠ NO SUBSCRIBE EVENTS FOUND!");
        tracing::warn!("");
        tracing::warn!("This means either:");
        tracing::warn!("  1. GET completed locally (no network Subscribe)");
        tracing::warn!("  2. Subscribe events aren't being logged");
        tracing::warn!("  3. GetSubscriptionCache subscription didn't trigger Subscribe");
        tracing::warn!("");
        tracing::warn!("Check logs above for:");
        tracing::warn!("  - 'created child subscription operation' (from GET)");
        tracing::warn!("  - 'started subscription' (confirms Subscribe was triggered)");
        tracing::warn!("  - 'Local subscription completed' (Subscribe went local path)");
        tracing::warn!("  - 'forwarding Request to target peer' (Subscribe went network path)");
    } else {
        tracing::info!("");
        tracing::info!("Subscribe event details:");
        for (idx, event) in subscribe_events.iter().enumerate() {
            let kind_str = format!("{:?}", event.kind);
            tracing::info!("  [{}] {}", idx, kind_str);
        }

        // Extract timestamps
        let mut subscribe_timestamps = Vec::new();
        for event in &subscribe_events {
            let kind_str = format!("{:?}", event.kind);
            if let Some(ts_start) = kind_str.find("timestamp: ") {
                let ts_str = &kind_str[ts_start + 11..];
                if let Some(ts_end) = ts_str.find(&[' ', ',', '}'][..]) {
                    if let Ok(ts) = ts_str[..ts_end].parse::<u64>() {
                        subscribe_timestamps.push(ts);
                    }
                }
            }
        }

        if !subscribe_timestamps.is_empty() {
            subscribe_timestamps.sort();
            let min_ts = subscribe_timestamps[0];

            tracing::info!("");
            tracing::info!("Subscribe event timeline (relative to first):");
            for ts in &subscribe_timestamps {
                let relative_s = (*ts - min_ts) / 1000;
                tracing::info!("  T+{}s", relative_s);
            }

            // Check for renewal events (should be around T+120s)
            let has_renewal = subscribe_timestamps.iter().any(|ts| {
                let elapsed_s = (*ts - min_ts) / 1000;
                elapsed_s >= 100 && elapsed_s <= 150
            });

            tracing::info!("");
            if has_renewal {
                tracing::info!("✓ RENEWAL DETECTED: Subscribe events found around T+120s!");
            } else {
                tracing::warn!("⚠ NO RENEWAL: All Subscribe events at T+0s, none at T+120s");
                tracing::warn!("This confirms the bug: GetSubscriptionCache not checked by renewal task");
            }
        }
    }

    tracing::info!("");
    tracing::info!("╔════════════════════════════════════════════════════════════════╗");
    tracing::info!("║ SUMMARY - What to Look For in Logs Above                       ║");
    tracing::info!("╚════════════════════════════════════════════════════════════════╝");
    tracing::info!("");
    tracing::info!("To verify GetSubscriptionCache is being used:");
    tracing::info!("  ✓ Search for: 'Recorded subscription in GetSubscriptionCache'");
    tracing::info!("    → This confirms GET operation stored subscription");
    tracing::info!("");
    tracing::info!("To verify the bug (renewal doesn't check GetSubscriptionCache):");
    tracing::info!("  ✓ Search for: 'contracts_needing_renewal: checked active_subscriptions'");
    tracing::info!("    → This message confirms the function doesn't check GetSubscriptionCache");
    tracing::info!("  ✓ Look for: renewal_count=0 in above message");
    tracing::info!("    → Zero contracts found for renewal (bug: GET subscriptions ignored)");
    tracing::info!("");
    tracing::info!("Expected behavior:");
    tracing::info!("  • GET operation at T+0s stores contract in GetSubscriptionCache");
    tracing::info!("  • Renewal task runs every 30s after initial delay");
    tracing::info!("  • BUT renewal task doesn't find GET subscriptions (the bug)");
    tracing::info!("  • Result: No renewal Subscribe events appear");
    tracing::info!("");
    tracing::info!("╔════════════════════════════════════════════════════════════════╗");
    tracing::info!("║ Test complete - analyze logs above                             ║");
    tracing::info!("╚════════════════════════════════════════════════════════════════╝");
}
