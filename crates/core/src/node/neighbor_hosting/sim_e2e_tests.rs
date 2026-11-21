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

/// Which anti-entropy a scenario runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Every node on the new code with the digest gate open (the simulation
    /// default).
    Digest,
    /// The kill switch: the gate is closed on every node and every responder
    /// answers a legacy request in full, which is what a network of pre-#5157
    /// peers does.
    Legacy,
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
fn run_scenario(
    tag: &str,
    seed: u64,
    with_crash: bool,
    mode: Mode,
    extra_contracts: usize,
    ack_version: bool,
) -> ScenarioOutcome {
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
    configure_network(&mut sim, mode, ack_version);

    let contracts: Vec<_> = (0..(8 + extra_contracts) as u8)
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
    // Larger hosted sets, so a full response is the size it is in the field
    // (tens of ids per neighbour or more) rather than a single packet's worth.
    for i in 0..extra_contracts {
        operations.push(put(node(1 + i % NODES), 8 + i));
    }
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
///
/// `ack_version` makes a regular node learn its gateway's version, as every
/// node does in production since 0.2.120. Without it the node -> gateway link
/// has an unknown version, so the gate fails closed and that pair runs the
/// legacy exchange, which a new responder throttles to one full reply per hour.
fn configure_network(sim: &mut SimNetwork, mode: Mode, ack_version: bool) {
    GlobalTestMetrics::enable_neighbor_hosting_trace();
    if ack_version {
        sim.enable_gateway_ack_version();
    }
    if mode == Mode::Legacy {
        sim.disable_neighbor_hosting_digest();
    }
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
    for (label, key, m) in &by_key { eprintln!("RCV KEY {label} {key:?} hosts={} tracked={:?}", m.advertised_ids().len(), m.tracked_peers().iter().map(|p| format!("{p:?}")).collect::<Vec<_>>()); }
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
                let missing = actual.difference(&view).count();
                let phantom = view.difference(&actual).count();
                mismatches.push(format!(
                    "{viewer} view of {subject}: view has {} ids, subject hosts {}, \
                     {missing} missing, {phantom} phantom",
                    view.len(),
                    actual.len()
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
    let out = run_scenario("baseline", 0x5157_0001, false, Mode::Legacy, 0, false);
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
    let out = run_scenario("baseline-crash", 0x5157_0002, true, Mode::Legacy, 0, false);
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

/// How many extra contracts the comparison scenario hosts on top of the
/// baseline's eight, so a node's set is about 24 ids and a full response is
/// ~800 bytes.
const COMPARISON_EXTRA_CONTRACTS: usize = 16;

fn quiet(out: &ScenarioOutcome, kind: &str) -> (u64, u64) {
    (
        out.quiet_msgs.get(kind).copied().unwrap_or(0),
        out.quiet_bytes.get(kind).copied().unwrap_or(0),
    )
}

/// The headline #5157 assertion: on the same seed and network, NeighborHosting
/// bytes in a window with no hosting changes fall by at least 90% against the
/// forced-legacy run, and views are exactly as accurate in both.
///
/// What carries the weight: the bytes ratio fails if the responder stops
/// sending `HostingStateUnchanged` (it would then answer every digest request
/// with the full set) and if the heartbeat stops sending digest requests; the
/// accuracy assertions fail if a digest mismatch stops replacing a stale view.
#[test]
fn digest_steady_state_bytes_are_at_least_90_percent_below_forced_legacy() {
    let seed = 0x5157_0003;
    let legacy = run_scenario(
        "cmp-legacy",
        seed,
        false,
        Mode::Legacy,
        COMPARISON_EXTRA_CONTRACTS,
        true,
    );
    let digest = run_scenario(
        "cmp-digest",
        seed,
        false,
        Mode::Digest,
        COMPARISON_EXTRA_CONTRACTS,
        true,
    );
    eprintln!("5157 CMP legacy: {legacy:#?}");
    eprintln!("5157 CMP digest: {digest:#?}");

    // Premises: both runs saw the same quiet-window length class, hosted the
    // same number of contracts and used the path they claim to.
    assert_eq!(legacy.hosted_total, digest.hosted_total, "same hosted sets");
    assert!(legacy.hosted_total >= 5 * 16, "{}", legacy.hosted_total);
    assert!(quiet(&legacy, "response").1 > 0, "legacy ships full sets");
    assert_eq!(quiet(&legacy, "digest_request").0, 0);
    assert_eq!(quiet(&legacy, "unchanged").0, 0);
    assert!(
        quiet(&digest, "digest_request").0 > 0,
        "digest requests are sent"
    );
    assert!(quiet(&digest, "unchanged").0 > 0, "and answered Unchanged");
    assert_eq!(
        quiet(&digest, "request").0,
        0,
        "no legacy request is sent when every peer is on the new code"
    );

    // Compare per virtual second, since the two runs' quiet windows differ in
    // length by a few seconds.
    let legacy_rate = legacy.quiet_total_bytes() as f64 / legacy.quiet_secs as f64;
    let digest_rate = digest.quiet_total_bytes() as f64 / digest.quiet_secs as f64;
    assert!(
        digest_rate <= legacy_rate * 0.10,
        "steady-state NeighborHosting bytes/s must fall by at least 90%: \
         legacy {legacy_rate:.2} B/s, digest {digest_rate:.2} B/s"
    );

    // Accuracy is not traded for bytes.
    assert!(legacy.pairs_checked >= NODES * 2 && digest.pairs_checked >= NODES * 2);
    assert!(legacy.mismatches.is_empty(), "{:#?}", legacy.mismatches);
    assert!(digest.mismatches.is_empty(), "{:#?}", digest.mismatches);
}

/// View accuracy under message loss, in both modes (#5157). A node silently
/// crashed while others host new contracts misses announcements, and the views
/// must be repaired by anti-entropy: in digest mode by the digest mismatch that
/// triggers a full replacement.
#[test]
fn views_heal_after_a_crashed_node_in_both_modes() {
    for (mode, tag) in [(Mode::Digest, "heal-digest"), (Mode::Legacy, "heal-legacy")] {
        let out = run_scenario(tag, 0x5157_0004, true, mode, 4, true);
        assert!(
            out.pairs_checked >= NODES * 2,
            "[{mode:?}] premise: {} pairs checked",
            out.pairs_checked
        );
        assert!(
            out.mismatches.is_empty(),
            "[{mode:?}] views must heal after the crashed node recovers: {:#?}",
            out.mismatches
        );
    }
}
