//! Simulation coverage for NeighborHosting anti-entropy (#5157).
//!
//! Two questions, answered on the same deterministic network:
//!
//! 1. **Accuracy.** After the network goes quiet, does every peer's view of
//!    each neighbour's hosted set equal that neighbour's actual hosted set?
//!    The view picks UPDATE broadcast targets (`neighbors_with_contract`), so a
//!    stale entry is a phantom target or a lost update. The scenario includes a
//!    node that is silently crashed while others host new contracts, so some
//!    announcements are lost and only the periodic anti-entropy can repair the
//!    views.
//! 2. **Bytes.** How many NeighborHosting bytes does the network send during a
//!    steady-state window in which nothing changes?
//!
//! The module is named `sim_e2e_tests` because CI's simulation job selects
//! in-crate simulation tests with
//! `-E 'kind(test) | (kind(lib) & test(sim_e2e_tests))'`; a module with any other
//! name compiles and never runs (#4301).

use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use freenet_stdlib::prelude::ContractInstanceId;

use crate::config::GlobalTestMetrics;
use crate::node::testing_impl::{
    ControlledSimulationResult, NodeLabel, ScheduledOperation, SimNetwork, SimOperation,
};

/// Regular (non-gateway) nodes. With `MAX_CONNECTIONS` this large every peer is
/// connected to every other, so each node has a view of every other node.
const NODES: usize = 4;
const MAX_CONNECTIONS: usize = 10;
const MIN_CONNECTIONS: usize = 2;

/// How long after the last scripted operation the simulation keeps running.
/// Long enough for the quiet window below plus several 300 s anti-entropy
/// cycles.
const POST_OPERATIONS_WAIT: Duration = Duration::from_secs(3_000);

/// The quiet window starts this long after the last announce was sent. One
/// anti-entropy cycle is 300 s and its sends are spread across the cycle, so
/// 700 s guarantees every peer has completed at least one full cycle after the
/// last change and any repair that cycle triggered has been sent.
const QUIET_AFTER_LAST_ANNOUNCE: Duration = Duration::from_secs(700);

/// Reset every piece of per-thread simulation state the harness relies on, as
/// `router::sim_e2e_tests::reset_simulation_state` does. It must run before
/// `SimNetwork::new`, because node keypairs are generated there.
fn reset_simulation_state(seed: u64) {
    use crate::config::{GlobalRng, GlobalSimulationTime, SimulationTransportOpt};

    GlobalRng::set_seed(seed);
    const BASE_EPOCH_MS: u64 = 1577836800000; // 2020-01-01 00:00:00 UTC
    const RANGE_MS: u64 = 5 * 365 * 24 * 60 * 60 * 1000; // ~5 years
    GlobalSimulationTime::set_time_ms(BASE_EPOCH_MS + (seed % RANGE_MS));
    GlobalTestMetrics::reset();
    SimulationTransportOpt::disable();
    // Without this a connection with no traffic is dropped by the 120 s idle
    // timeout and re-established, which clears and re-learns every view about
    // every 125 s and makes a steady-state window impossible. The direct runner
    // enables it for every simulation for the same reason.
    crate::config::SimulationIdleTimeout::enable();
    crate::contract::clear_crdt_contracts();
    crate::client_events::RequestId::reset_counter();
    crate::client_events::ClientId::reset_counter();
    crate::contract::reset_event_id_counter();
    crate::node::reset_channel_id_counter();
    crate::transport::StreamId::reset_counter();
    crate::transport::reset_nonce_counter();
    crate::test_utils::reset_global_node_index();
}

/// What one scenario run measured.
#[derive(Debug)]
struct ScenarioOutcome {
    /// NeighborHosting bytes and message counts per variant stem, over the
    /// quiet window only.
    quiet_bytes: BTreeMap<&'static str, u64>,
    quiet_msgs: BTreeMap<&'static str, u64>,
    /// NeighborHosting bytes per variant stem over the whole run.
    total_bytes: BTreeMap<&'static str, u64>,
    /// Length of the quiet window in virtual seconds.
    quiet_secs: u64,
    /// (viewer, subject) pairs whose view was compared.
    pairs_checked: usize,
    /// One line per pair whose view differs from the subject's hosted set.
    mismatches: Vec<String>,
    /// Advertised contracts summed over all nodes at the end, so a test can
    /// assert the scenario actually put something in the sets.
    hosted_total: usize,
}

impl ScenarioOutcome {
    #[allow(dead_code)]
    fn quiet_total_bytes(&self) -> u64 {
        self.quiet_bytes.values().sum()
    }
}

/// Run the scenario once and return what it measured.
///
/// `tag` keeps network names distinct: lib tests run on parallel threads and the
/// simulation registries are keyed by network name.
fn run_scenario(tag: &str, seed: u64, with_crash: bool) -> ScenarioOutcome {
    reset_simulation_state(seed);
    let network = format!("nh-digest-{tag}-{seed:x}");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let mut sim = rt.block_on(async {
        SimNetwork::new(
            &network,
            1,
            NODES,
            7,
            3,
            MAX_CONNECTIONS,
            MIN_CONNECTIONS,
            seed,
        )
        .await
    });
    // The production `ContractExecutor` path.
    sim.use_mock_wasm = true;
    sim.wait_for_join_convergence_before_ops(1.0, Duration::from_secs(120));
    sim.with_controlled_op_interval(Duration::from_secs(2));
    configure_network(&mut sim);

    let contracts: Vec<_> = (0..8u8)
        .map(|i| SimOperation::create_test_contract(0x50 + i))
        .collect();
    let gateway = NodeLabel::gateway(&network, 0);
    let node = |i: usize| NodeLabel::node(&network, i);
    let put = |label: NodeLabel, idx: usize| {
        ScheduledOperation::new(
            label,
            SimOperation::Put {
                contract: contracts[idx].clone(),
                state: SimOperation::create_test_state(idx as u8),
                subscribe: true,
            },
        )
    };
    let subscribe = |label: NodeLabel, idx: usize| {
        ScheduledOperation::new(
            label,
            SimOperation::Subscribe {
                contract_id: *contracts[idx].key().id(),
            },
        )
    };

    // Initial hosting, announced to everyone.
    let mut operations = vec![
        put(node(1), 0),
        put(node(2), 1),
        put(node(3), 2),
        put(gateway.clone(), 3),
        subscribe(node(4), 0),
        subscribe(node(4), 3),
    ];
    if with_crash {
        // Node 3 is silently crashed while others host new contracts, so the
        // announcements for contracts 4 and 5 never reach it and its own
        // announcements (contract 6) never reach anyone. Only periodic
        // anti-entropy can repair those views.
        operations.extend([
            ScheduledOperation::new(node(3), SimOperation::CrashNode),
            put(node(1), 4),
            put(node(2), 5),
            put(node(3), 6),
            ScheduledOperation::new(node(3), SimOperation::RecoverNode),
            subscribe(node(3), 1),
            put(node(4), 7),
        ]);
    } else {
        operations.extend([
            put(node(1), 4),
            put(node(2), 5),
            put(node(3), 6),
            subscribe(node(3), 1),
            put(node(4), 7),
        ]);
    }

    let sim_duration = Duration::from_secs(POST_OPERATIONS_WAIT.as_secs() + 400);
    let result =
        sim.run_controlled_simulation(seed, operations, sim_duration, POST_OPERATIONS_WAIT);
    assert!(
        result.turmoil_result.is_ok(),
        "[{network}] controlled simulation must complete: {:?}",
        result.turmoil_result.err()
    );

    measure(&network, &result, !with_crash)
}

/// Hook for the scenario's network configuration. The baseline (legacy-only)
/// build needs none beyond turning the send trace on; the digest build also
/// sets the version floor per mode.
fn configure_network(sim: &mut SimNetwork) {
    GlobalTestMetrics::enable_neighbor_hosting_trace();
    let _ = sim;
}

/// `require_quiet`: the run must contain a window with no announces. A run with
/// a crashed-and-recovered node does not: the recovered node's connections keep
/// being re-established for the rest of the run (a periodic ~8-id reciprocal
/// announce every ~125 s), so it is used for view accuracy only.
fn measure(
    network: &str,
    result: &ControlledSimulationResult,
    require_quiet: bool,
) -> ScenarioOutcome {
    let samples = GlobalTestMetrics::neighbor_hosting_sends();
    assert!(
        !samples.is_empty(),
        "[{network}] no NeighborHosting send was recorded: the trace is not wired into the \
         path the simulation sends on, so every bytes figure below would be a vacuous zero"
    );
    let last = samples.last().map(|s| s.at).expect("non-empty");
    {
        let t0 = samples[0].at;
        let timeline: Vec<String> = samples
            .iter()
            .map(|s| {
                format!(
                    "{}s:{}:{}",
                    s.at.saturating_duration_since(t0).as_secs(),
                    s.kind,
                    s.bytes
                )
            })
            .collect();
        eprintln!("5157 timeline [{network}]: {} sends", timeline.len());
    }
    let last_announce = samples
        .iter()
        .rfind(|s| s.kind == "announce")
        .map(|s| s.at)
        .expect("the scenario hosts contracts, so announces must have been sent");
    let quiet_start = last_announce + QUIET_AFTER_LAST_ANNOUNCE;
    assert!(
        !require_quiet || last > quiet_start + Duration::from_secs(600),
        "[{network}] the simulation ended {}s after the last announce; the quiet window needs \
         at least 600s after its {}s lead-in",
        last.saturating_duration_since(last_announce).as_secs(),
        QUIET_AFTER_LAST_ANNOUNCE.as_secs(),
    );

    let mut quiet_bytes: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut quiet_msgs: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut total_bytes: BTreeMap<&'static str, u64> = BTreeMap::new();
    for s in &samples {
        *total_bytes.entry(s.kind).or_default() += s.bytes;
        if s.at >= quiet_start {
            *quiet_bytes.entry(s.kind).or_default() += s.bytes;
            *quiet_msgs.entry(s.kind).or_default() += 1;
        }
    }
    let quiet_secs = last.saturating_duration_since(quiet_start).as_secs();

    // View accuracy: every viewer's set for each subject vs the subject's own.
    let mut by_key = Vec::new();
    for (label, ring) in &result.node_rings {
        let manager = ring
            .neighbor_hosting_for_test()
            .unwrap_or_else(|| panic!("[{network}] {label} published no NeighborHostingManager"));
        by_key.push((
            label.clone(),
            (*ring.connection_manager.pub_key).clone(),
            manager,
        ));
    }
    let mut pairs_checked = 0usize;
    let mut mismatches = Vec::new();
    let mut hosted_total = 0usize;
    for (_, _, manager) in &by_key {
        hosted_total += manager.advertised_ids().len();
    }
    for (viewer, _, vm) in &by_key {
        for peer in vm.tracked_peers() {
            let Some((subject, _, sm)) = by_key.iter().find(|(_, key, _)| *key == peer) else {
                continue;
            };
            let view: HashSet<ContractInstanceId> = vm.contracts_for_peer(&peer);
            let actual = sm.advertised_ids();
            pairs_checked += 1;
            if view != actual {
                let missing: Vec<_> = actual.difference(&view).map(|id| id.to_string()).collect();
                let phantom: Vec<_> = view.difference(&actual).map(|id| id.to_string()).collect();
                mismatches.push(format!(
                    "{viewer} view of {subject}: missing {missing:?}, phantom {phantom:?}"
                ));
            }
        }
    }

    ScenarioOutcome {
        quiet_bytes,
        quiet_msgs,
        total_bytes,
        quiet_secs,
        pairs_checked,
        mismatches,
        hosted_total,
    }
}

/// Baseline on unmodified code (#5157): every anti-entropy cycle ships each
/// neighbour's full hosted set, so the steady-state window carries a steady
/// stream of `response` bytes even though nothing changes.
#[test]
fn baseline_legacy_steady_state_bytes_and_view_accuracy() {
    let out = run_scenario("baseline", 0x5157_0001, false);
    eprintln!("5157 BASELINE outcome: {out:#?}");

    assert!(
        out.hosted_total >= 8,
        "premise: the scenario must leave contracts hosted ({} advertised across all nodes)",
        out.hosted_total
    );
    assert!(
        out.pairs_checked >= NODES * 2,
        "premise: peers must hold views of each other ({} pairs checked)",
        out.pairs_checked
    );
    assert!(
        out.quiet_bytes.get("response").copied().unwrap_or(0) > 0,
        "premise: the legacy anti-entropy must ship responses in the quiet window"
    );
    assert!(
        out.mismatches.is_empty(),
        "after quiescence every view must equal the subject's hosted set: {:#?}",
        out.mismatches
    );
}

/// Baseline accuracy under message loss (#5157): a node that is silently
/// crashed while others host new contracts misses announcements, and only the
/// periodic full-set exchange can repair the views.
#[test]
fn baseline_legacy_views_heal_after_a_crashed_node_misses_announcements() {
    let out = run_scenario("baseline-crash", 0x5157_0002, true);
    eprintln!("5157 BASELINE (crash) outcome: {out:#?}");
    assert!(
        out.pairs_checked >= NODES * 2,
        "premise: peers must hold views of each other ({} pairs checked)",
        out.pairs_checked
    );
    assert!(
        out.mismatches.is_empty(),
        "views must heal after the crashed node recovers: {:#?}",
        out.mismatches
    );
}
