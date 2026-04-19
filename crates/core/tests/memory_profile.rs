//! Memory profiling for typical Freenet node usage.
//!
//! Spins up a local in-memory network with nodes that interact continuously
//! via random operations (PUT/GET/SUBSCRIBE/UPDATE) and samples process RSS
//! plus operation metrics at regular intervals.
//!
//! These tests are marked `#[ignore]` — run them explicitly:
//!
//!   cargo test -p freenet --features "simulation_tests,testing" \
//!     --test memory_profile -- --ignored --nocapture
//!
//! The output is a time-series table showing RSS vs. operational metrics,
//! suitable for spotting memory leaks, sizing nodes, and regression baselining.

#![cfg(feature = "simulation_tests")]

use freenet::config::{GlobalRng, GlobalSimulationTime, GlobalTestMetrics, SimulationTransportOpt};
use freenet::dev_tool::{
    ClientId, RequestId, SimNetwork, StreamId, reset_channel_id_counter, reset_event_id_counter,
    reset_global_node_index, reset_nonce_counter,
};
use std::time::{Duration, Instant};

// =============================================================================
// Snapshot types
// =============================================================================

#[derive(Debug, Clone)]
struct MemorySnapshot {
    elapsed_secs: f64,
    rss_kb: u64,
    op_inserts: u64,
    op_removes: u64,
    op_hwm: u64,
    delta_sends: u64,
    full_state_sends: u64,
    log_events: usize,
}

impl MemorySnapshot {
    fn capture(elapsed_secs: f64, log_events: usize) -> Self {
        let rss_kb = read_rss_kb();
        let inserts = GlobalTestMetrics::pending_op_inserts();
        let removes = GlobalTestMetrics::pending_op_removes();
        MemorySnapshot {
            elapsed_secs,
            rss_kb,
            op_inserts: inserts,
            op_removes: removes,
            op_hwm: GlobalTestMetrics::pending_op_high_water_mark(),
            delta_sends: GlobalTestMetrics::delta_sends(),
            full_state_sends: GlobalTestMetrics::full_state_sends(),
            log_events,
        }
    }

    fn in_flight(&self) -> u64 {
        self.op_inserts.saturating_sub(self.op_removes)
    }
}

// =============================================================================
// Helpers
// =============================================================================

/// Read process RSS from /proc/self/status (Linux).
/// Returns 0 on non-Linux or if the read fails.
fn read_rss_kb() -> u64 {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|l| l.starts_with("VmRSS:"))
                    .and_then(|l| l.split_whitespace().nth(1))
                    .and_then(|v| v.parse().ok())
            })
            .unwrap_or(0)
    }
    #[cfg(not(target_os = "linux"))]
    0
}

fn setup_test_state(seed: u64) {
    GlobalRng::set_seed(seed);
    const BASE_EPOCH_MS: u64 = 1577836800000; // 2020-01-01 00:00:00 UTC
    const RANGE_MS: u64 = 5 * 365 * 24 * 60 * 60 * 1000;
    GlobalSimulationTime::set_time_ms(BASE_EPOCH_MS + (seed % RANGE_MS));
    GlobalTestMetrics::reset();
    SimulationTransportOpt::disable();
    freenet::dev_tool::clear_crdt_contracts();
    RequestId::reset_counter();
    ClientId::reset_counter();
    reset_event_id_counter();
    reset_channel_id_counter();
    StreamId::reset_counter();
    reset_nonce_counter();
    reset_global_node_index();
}

fn print_profile_report(config: &str, snapshots: &[MemorySnapshot]) {
    println!();
    println!("{}", "═".repeat(115));
    println!("  MEMORY PROFILE: {config}");
    println!("{}", "═".repeat(115));
    println!(
        "{:>8}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}  {:>10}  {:>10}  {:>8}",
        "t(s)",
        "RSS(MB)",
        "ΔRSS(MB)",
        "InFlight",
        "TotalOps",
        "PeakOps",
        "DltSends",
        "FllSends",
        "LogEvts"
    );
    println!("{}", "─".repeat(115));

    let baseline = snapshots.first().map(|s| s.rss_kb).unwrap_or(0);
    for snap in snapshots {
        let delta_mb = (snap.rss_kb as i64 - baseline as i64) as f64 / 1024.0;
        println!(
            "{:>8.1}  {:>8.1}  {:>+8.1}  {:>9}  {:>9}  {:>9}  {:>10}  {:>10}  {:>8}",
            snap.elapsed_secs,
            snap.rss_kb as f64 / 1024.0,
            delta_mb,
            snap.in_flight(),
            snap.op_inserts,
            snap.op_hwm,
            snap.delta_sends,
            snap.full_state_sends,
            snap.log_events,
        );
    }

    if let (Some(first), Some(last)) = (snapshots.first(), snapshots.last()) {
        let rss_growth_mb = (last.rss_kb as i64 - first.rss_kb as i64) as f64 / 1024.0;
        let peak_rss_mb = snapshots.iter().map(|s| s.rss_kb).max().unwrap_or(0) as f64 / 1024.0;
        let ops_completed = last.op_removes;
        let runtime_s = last.elapsed_secs;

        println!("{}", "─".repeat(115));
        println!(
            "  Peak RSS: {peak_rss_mb:.1} MB  |  RSS growth: {rss_growth_mb:+.1} MB  \
             |  Ops completed: {ops_completed}  |  Runtime: {runtime_s:.0}s"
        );

        if ops_completed > 0 && rss_growth_mb > 0.0 {
            let kb_per_op = (rss_growth_mb * 1024.0) / ops_completed as f64;
            println!("  Approx. RSS growth per completed op: {kb_per_op:.1} KB");
        }
    }
    println!("{}", "═".repeat(115));
}

// =============================================================================
// Profiling test: typical node usage with continuous interaction
// =============================================================================

/// Profile memory usage for a local in-memory Freenet network under load.
///
/// Creates a network of nodes that continuously exchange contracts and updates
/// via random PUT/GET/SUBSCRIBE/UPDATE operations, then profiles RSS + operation
/// metrics over time.  Produces a time-series table and a brief summary.
///
/// Run:
///   cargo test -p freenet --features "simulation_tests,testing" \
///     --test memory_profile -- --ignored --nocapture profile_node_memory_typical
#[ignore = "memory profiling — run manually with --ignored --nocapture"]
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn profile_node_memory_typical() {
    const SEED: u64 = 0xBA5E_111E_0001;

    // Network parameters — tuned to keep the test under ~2 minutes wall-clock.
    const GATEWAYS: usize = 1;
    const NODES: usize = 7;
    const MAX_CONTRACTS: usize = 10;
    const ITERATIONS: usize = 300; // event budget per node

    // Profiling cadence
    const PROFILE_DURATION: Duration = Duration::from_secs(90);
    const SAMPLE_INTERVAL: Duration = Duration::from_secs(3);

    // Virtual-time advance per real-time step (drives message delivery).
    const VTIME_STEP: Duration = Duration::from_millis(200);
    // Real-time sleep between vtime advances (yields to node tasks).
    const REAL_STEP: Duration = Duration::from_millis(50);

    let config = format!(
        "{GATEWAYS} gateway + {NODES} nodes, {MAX_CONTRACTS} contracts, {ITERATIONS} events/node"
    );

    setup_test_state(SEED);

    let mut sim = SimNetwork::new(
        "mem-profile",
        GATEWAYS,
        NODES,
        10, // ring_max_htl
        5,  // rnd_if_htl_above
        15, // max_connections
        3,  // min_connections
        SEED,
    )
    .await;
    sim.with_start_backoff(Duration::from_millis(50));

    let logs_handle = sim.event_logs_handle();

    // Baseline — before any nodes start.
    let baseline_log_events = logs_handle.lock().await.len();
    let mut snapshots = vec![MemorySnapshot::capture(0.0, baseline_log_events)];

    // Start all nodes with random event generation.
    let _handles = sim
        .start_with_rand_gen::<rand::rngs::SmallRng>(SEED, MAX_CONTRACTS, ITERATIONS)
        .await;

    let wall_start = Instant::now();
    let mut last_sample = wall_start;

    while wall_start.elapsed() < PROFILE_DURATION {
        // Advance virtual time so scheduled messages get delivered.
        sim.advance_time(VTIME_STEP);
        // Yield to tokio so node tasks process those messages.
        tokio::task::yield_now().await;
        tokio::time::sleep(REAL_STEP).await;

        if last_sample.elapsed() >= SAMPLE_INTERVAL {
            let elapsed = wall_start.elapsed().as_secs_f64();
            let log_events = logs_handle.lock().await.len();
            snapshots.push(MemorySnapshot::capture(elapsed, log_events));
            last_sample = Instant::now();
        }
    }

    // Final snapshot.
    {
        let elapsed = wall_start.elapsed().as_secs_f64();
        let log_events = logs_handle.lock().await.len();
        snapshots.push(MemorySnapshot::capture(elapsed, log_events));
    }

    print_profile_report(&config, &snapshots);
}

// =============================================================================
// Scaling profile: memory cost per node
// =============================================================================

/// Profile RSS at three network scales to estimate per-node memory cost.
///
/// Runs three simulations sequentially (small / medium / large) and reports
/// baseline RSS (after setup, before operations) and post-simulation RSS for
/// each.  Divides the delta by the node count to approximate per-node cost.
///
/// Run:
///   cargo test -p freenet --features "simulation_tests,testing" \
///     --test memory_profile -- --ignored --nocapture profile_node_memory_scaling
#[ignore = "memory profiling — run manually with --ignored --nocapture"]
#[test_log::test]
fn profile_node_memory_scaling() {
    #[derive(Debug)]
    struct ScaleResult {
        label: &'static str,
        peers: usize,
        rss_before_kb: u64,
        rss_after_setup_kb: u64,
        rss_after_sim_kb: u64,
        op_inserts: u64,
        op_removes: u64,
        log_events: usize,
    }

    fn run_scale(
        name: &'static str,
        label: &'static str,
        gateways: usize,
        nodes: usize,
        max_contracts: usize,
        iterations: usize,
        seed: u64,
    ) -> ScaleResult {
        use rand::rngs::SmallRng;

        setup_test_state(seed);
        let peers = gateways + nodes;

        let rss_before_kb = read_rss_kb();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build runtime");

        // Phase 1: create and start the network.
        let (mut sim, logs_handle) = rt.block_on(async {
            let mut sim = SimNetwork::new(name, gateways, nodes, 10, 5, 15, 3, seed).await;
            sim.with_start_backoff(Duration::from_millis(50));
            let logs = sim.event_logs_handle();

            let _handles = sim
                .start_with_rand_gen::<SmallRng>(seed, max_contracts, iterations)
                .await;
            (sim, logs)
        });

        let rss_after_setup_kb = read_rss_kb();

        // Phase 2: drive message delivery until events exhaust or 30s elapses.
        const RUN_SECS: u64 = 30;
        rt.block_on(async {
            let wall_start = Instant::now();
            let run_limit = Duration::from_secs(RUN_SECS);
            while wall_start.elapsed() < run_limit {
                sim.advance_time(Duration::from_millis(200));
                tokio::task::yield_now().await;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });

        let (rss_after_sim_kb, log_events) = rt.block_on(async {
            let rss = read_rss_kb();
            let evts = logs_handle.lock().await.len();
            (rss, evts)
        });

        ScaleResult {
            label,
            peers,
            rss_before_kb,
            rss_after_setup_kb,
            rss_after_sim_kb,
            op_inserts: GlobalTestMetrics::pending_op_inserts(),
            op_removes: GlobalTestMetrics::pending_op_removes(),
            log_events,
        }
    }

    const BASE_SEED: u64 = 0xBA5E_5CA1_0001;

    let results = [
        run_scale("scale-s", "g1n2 (3 peers)", 1, 2, 3, 60, BASE_SEED),
        run_scale("scale-m", "g1n5 (6 peers)", 1, 5, 6, 120, BASE_SEED + 1),
        run_scale("scale-l", "g2n8 (10 peers)", 2, 8, 10, 200, BASE_SEED + 2),
    ];

    println!();
    println!("{}", "═".repeat(110));
    println!("  NODE MEMORY SCALING PROFILE");
    println!("{}", "═".repeat(110));
    println!(
        "{:<18}  {:>6}  {:>10}  {:>12}  {:>11}  {:>9}  {:>9}  {:>8}",
        "Config",
        "Peers",
        "Pre(MB)",
        "PostSetup(MB)",
        "PostSim(MB)",
        "OpsIssued",
        "OpsOK",
        "LogEvts"
    );
    println!("{}", "─".repeat(110));

    for r in &results {
        println!(
            "{:<18}  {:>6}  {:>9.1}  {:>12.1}  {:>11.1}  {:>9}  {:>9}  {:>8}",
            r.label,
            r.peers,
            r.rss_before_kb as f64 / 1024.0,
            r.rss_after_setup_kb as f64 / 1024.0,
            r.rss_after_sim_kb as f64 / 1024.0,
            r.op_inserts,
            r.op_removes,
            r.log_events,
        );
    }

    // Rough per-node memory estimate between scale-s and scale-l.
    if results.len() >= 2 {
        let small = &results[0];
        let large = &results[results.len() - 1];
        let peer_delta = large.peers.saturating_sub(small.peers);
        if peer_delta > 0 {
            let setup_delta_kb = large.rss_after_setup_kb as i64 - small.rss_after_setup_kb as i64;
            let sim_delta_kb = large.rss_after_sim_kb as i64 - small.rss_after_sim_kb as i64;
            println!("{}", "─".repeat(110));
            println!(
                "  Per-node estimate ({} → {} peers, Δ{} nodes):",
                small.peers, large.peers, peer_delta
            );
            println!(
                "    After setup: {:+.1} MB/node",
                setup_delta_kb as f64 / 1024.0 / peer_delta as f64
            );
            println!(
                "    After simulation: {:+.1} MB/node",
                sim_delta_kb as f64 / 1024.0 / peer_delta as f64
            );
        }
    }
    println!("{}", "═".repeat(110));
}
