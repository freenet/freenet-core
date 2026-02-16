//! Integration tests for the state verification system.
//!
//! Tests the SimNetwork.verify_state() and StateVerifier API with realistic event
//! logs generated from simulation runs.

#![cfg(feature = "simulation_tests")]

use freenet::config::{GlobalRng, GlobalSimulationTime};
use freenet::dev_tool::SimNetwork;
use std::time::Duration;

async fn let_network_run(sim: &mut SimNetwork, duration: Duration) {
    let step = Duration::from_millis(100);
    let mut elapsed = Duration::ZERO;

    while elapsed < duration {
        sim.advance_time(step);
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        elapsed += step;
    }
}

/// Verify that verify_state() returns a report after a simulation run
/// and correctly counts events and contracts.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_verify_state_produces_report() {
    const SEED: u64 = 0xBEEF_CAFE_0001;
    const BASE_EPOCH_MS: u64 = 1577836800000;
    const RANGE_MS: u64 = 5 * 365 * 24 * 60 * 60 * 1000;

    GlobalRng::set_seed(SEED);
    GlobalSimulationTime::set_time_ms(BASE_EPOCH_MS + (SEED % RANGE_MS));
    GlobalRng::reset_thread_index_counter();

    let mut sim = SimNetwork::new("verify-state-test", 1, 3, 7, 3, 10, 2, SEED).await;
    sim.with_start_backoff(Duration::from_millis(50));

    let _handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 1, 1)
        .await;

    let_network_run(&mut sim, Duration::from_secs(3)).await;

    let report = sim.verify_state().await;

    // The report should have analyzed events from the simulation
    assert!(
        report.total_events > 0,
        "Expected events from simulation, got 0"
    );

    // Display should produce non-empty output
    let display = report.display();
    assert!(
        display.contains("State Verification Report"),
        "Report display should contain header"
    );

    tracing::info!(
        "verify_state report: {} events, {} state events, {} contracts, {} anomalies",
        report.total_events,
        report.state_events,
        report.contracts_analyzed,
        report.anomalies.len()
    );
}

/// Verify that the VerificationReport filter methods work correctly
/// on a report from a real simulation.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_verify_state_filter_methods() {
    const SEED: u64 = 0xBEEF_CAFE_0002;
    const BASE_EPOCH_MS: u64 = 1577836800000;
    const RANGE_MS: u64 = 5 * 365 * 24 * 60 * 60 * 1000;

    GlobalRng::set_seed(SEED);
    GlobalSimulationTime::set_time_ms(BASE_EPOCH_MS + (SEED % RANGE_MS));
    GlobalRng::reset_thread_index_counter();

    let mut sim = SimNetwork::new("verify-filter-test", 1, 3, 7, 3, 10, 2, SEED).await;
    sim.with_start_backoff(Duration::from_millis(50));

    let _handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, 1, 1)
        .await;

    let_network_run(&mut sim, Duration::from_secs(3)).await;

    let report = sim.verify_state().await;

    // Filter methods should not panic and should return subsets
    let divergences = report.divergences();
    let missing = report.missing_broadcasts();
    let unapplied = report.unapplied_broadcasts();
    let partitions = report.suspected_partitions();
    let stale = report.stale_peers();
    let oscillations = report.state_oscillations();
    let zombies = report.zombie_transactions();
    let storms = report.broadcast_storms();
    let cascades = report.delta_sync_cascades();

    let total_filtered = divergences.len()
        + missing.len()
        + unapplied.len()
        + partitions.len()
        + stale.len()
        + oscillations.len()
        + zombies.len()
        + storms.len()
        + cascades.len();

    // The sum of filtered anomalies should be <= total (some anomaly types
    // like UnexpectedStateChange, OneWayPropagation, UpdateOrdering, and
    // SubscriptionAsymmetry aren't covered by the named filters).
    assert!(
        total_filtered <= report.anomalies.len(),
        "Filtered anomalies ({}) should be <= total ({})",
        total_filtered,
        report.anomalies.len()
    );

    tracing::info!(
        "Filter test: {} total anomalies, {} via named filters",
        report.anomalies.len(),
        total_filtered
    );
}
