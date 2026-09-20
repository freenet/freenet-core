//! Simulation coverage for the hierarchical routing estimator (#4485).
//!
//! # Why this file exists
//!
//! Until this module was written, nothing in `crates/core/tests/` or anywhere
//! else had ever ROUTED with the hierarchical estimator enabled.
//! `FREENET_ROUTING_HIERARCHICAL` and [`force_hierarchical_routing`] appeared
//! only in `crates/core/src/router.rs` and its own unit tests, so the estimator
//! had been exercised by unit tests and by offline replay of recorded gateway
//! data, and by nothing that carries a packet. The repo's
//! `.claude/rules/test-philosophy.md` asks a routing change to assert
//! simulation health metrics; the promotion gate for flipping the flag on by
//! default (`routing-soak-gate/PLAN-v2.md` section 6, added 2026-09-17) turns
//! that into a condition: at least one simulation case must route with the flag
//! enabled over a workload with REPEATED contracts, asserting GET and subscribe
//! success rates and subscription-tree formation.
//!
//! This module is that case, run as an A/B: the SAME workload and seed with the
//! estimator ON and OFF, so the assertion is a comparison rather than an
//! absolute threshold picked from one run.
//!
//! # Why it lives in the lib rather than `crates/core/tests/`
//!
//! Three things it needs are `pub(crate)`: [`force_hierarchical_routing`],
//! [`super::RouterSnapshotInfo`] (which carries the contract-term counters), and
//! `ControlledSimulationResult::node_rings`. More importantly, the flag's test
//! override is a THREAD-LOCAL, and the flag's production path is a process-wide
//! `OnceLock` over an environment variable. An integration test could only set
//! the env var, which fixes one value for the whole process and so cannot run
//! both arms — exactly the cross-test-interference shape
//! `.claude/rules/testing.md` warns about, benign under `nextest`'s
//! process-per-test and not under plain `cargo test`. The thread-local reaches
//! every simulated node because `run_controlled_simulation` drives Turmoil on
//! the calling thread, so both arms can run in one process with no env var at
//! all. `hierarchical_estimator_is_actually_active_under_the_flag` pins that the
//! override really does reach the routers, so a future change that moves node
//! execution off this thread fails here instead of silently measuring the OFF
//! arm twice.
//!
//! The module is named `sim_e2e_tests` for a mechanical reason: CI's simulation
//! job selects in-crate simulation tests with
//! `-E 'kind(test) | (kind(lib) & test(sim_e2e_tests))'`, so a module with any
//! other name would compile and never run (#4301 is the precedent for that
//! failure).
//!
//! # The workload, and why it is shaped this way
//!
//! Two properties matter and neither is automatic.
//!
//! **Repeated contracts.** The contract term keys a second hierarchy by the
//! contract location's bits. A workload that draws a fresh contract per event
//! never replicates a `(contract, peer)` cell, so the term cannot activate at
//! all — which is precisely how the #5655 synthetic bake-off ended up vacuous
//! (`PLAN-v2.md` section 6 records the correction). Every request here is drawn
//! from a small fixed pool, so each contract is asked for repeatedly by several
//! peers.
//!
//! **Failures that differ BY CONTRACT.** A clean network produces only success
//! residuals, `tau2_contract` computes to zero, and the term is inert even with
//! a replicated table. Generating failures is less obvious than it looks: an
//! attempt that returns `NotFound` for a contract with no proof of existence is
//! dropped UNTRAINED (`route_attempt.rs`'s `AmbiguousNotFoundPolicy::Untrained`),
//! so simply asking for contracts that were never PUT produces no failure
//! labels whatsoever. What does produce them is a contract that EXISTS but is
//! held by exactly one distant node: peers tried on the way answer `NotFound`,
//! the operation later fetches state from a remote peer,
//! `RouteAttemptRecorder::contract_exists` fires, and every pending `NotFound`
//! becomes a real `Failure` label attributed to that peer on that contract.
//! That is the shape the contract term exists for, so [`SCARCE_CONTRACTS`] are
//! seeded on a single node and asked for repeatedly by everyone else.
//!
//! # What this test does NOT establish
//!
//! It is a simulation of a handful of nodes over a few hundred events. It
//! cannot speak to the gate's offline measures (M1/M2/M3 in `PLAN-v2.md`),
//! which are scored on recorded gateway traffic, and it cannot resolve small
//! differences: the margins below are chosen from the observed seed-to-seed
//! spread of the OFF arm, which is what bounds what a run of this size can see.
//! It answers one narrow question the gate asks and nothing else — does routing
//! with the estimator on still form a network, serve reads and build a
//! subscription tree, and does the contract term ever engage.

use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use freenet_stdlib::prelude::{ContractInstanceId, ContractKey};

use crate::node::testing_impl::{
    ControlledSimulationResult, NodeLabel, ScheduledOperation, SimNetwork, SimOperation,
};
use crate::tracing::event_kind::{EventKind, GetEvent, GetTerminalOutcome};

/// Contracts PUT through the network and subscribed, so they end up held in
/// several places and their reads mostly succeed.
const REPLICATED_CONTRACTS: u8 = 4;

/// Contracts seeded on ONE node only. Reads for these travel, collect
/// `NotFound`s from peers that do not hold them, and then succeed — which is
/// what turns those `NotFound`s into trained `Failure` labels (see the module
/// docs). These are what give the contract term something to explain away.
const SCARCE_CONTRACTS: u8 = 3;

/// Contracts held by exactly TWO nodes, so a read for them travels but has two
/// possible terminals. Between them and [`SCARCE_CONTRACTS`] the workload
/// spans three replication levels, which is what gives the contract term a
/// between-contract contrast to find at all.
const TWIN_CONTRACTS: u8 = 2;

/// Rounds of the read workload. Each round issues, from every non-gateway node,
/// one GET and one SUBSCRIBE against a replicated contract and one GET against
/// a scarce one, so every contract is asked for by every peer many times over.
const ROUNDS: usize = 12;

/// Regular (non-gateway) nodes.
const NODES: usize = 7;

/// Seeds the A/B runs over. Three is few, and the spread across them is
/// reported rather than hidden: it is what the non-inferiority margins are
/// derived from.
const SEEDS: [u64; 3] = [0x5EED_0001, 0x5EED_0002, 0x5EED_0003];

/// Contract-id seed space is split so a replicated contract and a scarce one
/// can never collide.
const REPLICATED_SEED_BASE: u8 = 0x10;
const SCARCE_SEED_BASE: u8 = 0x40;
const TWIN_SEED_BASE: u8 = 0x70;

/// Everything one arm of the A/B produced.
#[derive(Debug, Clone, Default)]
struct ArmMetrics {
    /// Client-visible GET outcomes for the widely-replicated contracts, one
    /// per client operation, taken from `GetEvent::ClientTerminal` (the
    /// authoritative terminal: the inline `GetSuccess` path misses local
    /// serves and streamed successes entirely, which is why the first version
    /// of this test read 0/0 here while the workload was plainly running).
    replicated_get_ok: u64,
    replicated_get_total: u64,
    /// The same for the single-holder contracts.
    scarce_get_ok: u64,
    scarce_get_total: u64,
    /// The same for the two-holder contracts.
    twin_get_ok: u64,
    twin_get_total: u64,
    /// Successful client GETs that actually traversed the network
    /// (`hop_count >= 1`), over both classes. A run whose GETs are all served
    /// locally exercises no routing at all, so this is reported beside the
    /// rates rather than left implicit.
    network_get_successes: u64,
    /// Terminal SUBSCRIBE outcomes, all contracts.
    subscribe_ok: u64,
    subscribe_total: u64,
    /// Subscription tree: `(contract, node)` pairs where the node ended the run
    /// actually receiving updates for that contract. Read from the live `Ring`s,
    /// not from the logs, so it is a state assertion rather than an event count.
    subscription_edges: u64,
    /// Nodes hosting each replicated contract at the end of the run, summed.
    hosting_edges: u64,
    /// Virtual-time latency, in ms, of the first terminal success in
    /// transaction-generation order. See [`first_success_latency_ms`] for what
    /// this ordering is and is not.
    first_success_latency_ms: Option<u64>,
    /// Terminal successes generated before the first one succeeded, in the same
    /// ordering. The "time to first successful operation" the test-philosophy
    /// rule asks for, expressed in operations because the log's wall-clock
    /// `datetime` is `Utc::now()` and so is not deterministic.
    ops_before_first_success: u64,
    /// Route events the routers ingested, network-wide.
    route_successes: u64,
    route_failures: u64,
    /// Contract-term counters, summed over every node's router
    /// ([`super::RouterSnapshotInfo`]).
    contract_estimable_refits: u64,
    contract_den_below_two_refits: u64,
    contract_effects_applied: u64,
    contract_forecast_offsets: u64,
    /// Largest `qualifying_contracts` seen on any node at the end of the run.
    /// The term needs at least two before `tau2_contract` can be non-zero.
    contract_qualifying_contracts_max: u64,
    /// Every node's `tau2_contract` at its last refit, for nodes that had one.
    contract_tau2: Vec<f64>,
    /// Contracts held in the failure stage's contract table, summed.
    contracts_tracked: u64,
    /// Events in the failure stage's window, summed. Zero means the estimator
    /// was not being computed at all.
    failure_events: u64,
    /// Nodes reporting `hierarchical_routing_enabled`. Proves the thread-local
    /// override reached the routers.
    nodes_routing_hierarchically: u64,
    nodes_total: u64,
}

impl ArmMetrics {
    fn replicated_get_rate(&self) -> f64 {
        rate(self.replicated_get_ok, self.replicated_get_total)
    }

    fn scarce_get_rate(&self) -> f64 {
        rate(self.scarce_get_ok, self.scarce_get_total)
    }

    fn twin_get_rate(&self) -> f64 {
        rate(self.twin_get_ok, self.twin_get_total)
    }

    fn subscribe_rate(&self) -> f64 {
        rate(self.subscribe_ok, self.subscribe_total)
    }

    fn contract_term_activated(&self) -> bool {
        self.contract_effects_applied > 0 || self.contract_forecast_offsets > 0
    }
}

fn rate(ok: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        ok as f64 / total as f64
    }
}

fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}

/// The workload, identical for both arms and both derived only from the network
/// name, so nothing about it can differ between the ON and OFF runs.
///
/// Returns the operations, then the replicated, scarce and twin contract keys.
fn build_workload(
    network: &str,
) -> (
    Vec<ScheduledOperation>,
    Vec<ContractKey>,
    Vec<ContractKey>,
    Vec<ContractKey>,
) {
    let gateway = NodeLabel::gateway(network, 0);
    // Node labels are 1-indexed and start after the gateways.
    let nodes: Vec<NodeLabel> = (1..=NODES).map(|n| NodeLabel::node(network, n)).collect();

    let mut operations = Vec::new();
    let mut replicated = Vec::new();
    let mut scarce = Vec::new();
    let mut twin = Vec::new();

    // Replicated contracts: PUT from the gateway with subscribe, so they
    // propagate and their reads mostly succeed.
    for i in 0..REPLICATED_CONTRACTS {
        let contract = SimOperation::create_test_contract(REPLICATED_SEED_BASE + i);
        replicated.push(contract.key());
        operations.push(ScheduledOperation::new(
            gateway.clone(),
            SimOperation::Put {
                contract,
                state: SimOperation::create_test_state(REPLICATED_SEED_BASE + i),
                subscribe: true,
            },
        ));
    }

    // Scarce contracts: seeded as a genuine host on ONE node, never PUT through
    // the network. Reads for them must travel, and the peers tried on the way
    // answer NotFound — which becomes a trained Failure once the operation
    // proves the contract exists.
    let holder = nodes[NODES - 3].clone();
    for i in 0..SCARCE_CONTRACTS {
        let contract = SimOperation::create_test_contract(SCARCE_SEED_BASE + i);
        scarce.push(contract.key());
        operations.push(ScheduledOperation::new(
            holder.clone(),
            SimOperation::SeedHostedContract {
                contract,
                state: SimOperation::create_test_state(SCARCE_SEED_BASE + i),
            },
        ));
    }

    // Twin contracts: seeded on the last two nodes and nowhere else, so a read
    // for them travels but has two possible terminals.
    //
    // An earlier version of this class CRASHED both holders part-way through
    // the run, on the theory that reads timing out at both would look like a
    // dead contract. Measured, it did the opposite: every-hop placement had
    // already left copies along the earlier successful routes, so the reads
    // kept succeeding (72/72 after the crash), and what the crash actually
    // produced was two bad PEERS. The estimator attributed it to them, which
    // is correct and is exactly what the leave-one-out is for -- and
    // `effects_applied` fell from 19 to 1 against the uncrashed workload. A
    // crashed peer is not a dead contract; do not re-add the crash expecting
    // one.
    let twin_holders: Vec<NodeLabel> = nodes[NODES - 2..].to_vec();
    for i in 0..TWIN_CONTRACTS {
        let contract = SimOperation::create_test_contract(TWIN_SEED_BASE + i);
        twin.push(contract.key());
        for holder in &twin_holders {
            operations.push(ScheduledOperation::new(
                holder.clone(),
                SimOperation::SeedHostedContract {
                    contract: contract.clone(),
                    state: SimOperation::create_test_state(TWIN_SEED_BASE + i),
                },
            ));
        }
    }

    // The read workload. Every requester asks for every contract repeatedly, so
    // each contract accumulates several peers in the contract table rather than
    // one — which is what `CONTRACT_MIN_OTHER_PEERS` requires before the term
    // can adjust anything.
    for round in 0..ROUNDS {
        for (n, node) in nodes.iter().enumerate() {
            let replicated_key = replicated[(round + n) % replicated.len()];
            let scarce_key = scarce[(round + n) % scarce.len()];

            operations.push(ScheduledOperation::new(
                node.clone(),
                SimOperation::Get {
                    contract_id: *replicated_key.id(),
                    return_contract_code: true,
                    subscribe: false,
                },
            ));
            operations.push(ScheduledOperation::new(
                node.clone(),
                SimOperation::Subscribe {
                    contract_id: *replicated_key.id(),
                },
            ));
            // The scarce read is issued by everyone except the node that holds
            // it, which would serve it locally and route nothing.
            if *node != holder {
                operations.push(ScheduledOperation::new(
                    node.clone(),
                    SimOperation::Get {
                        contract_id: *scarce_key.id(),
                        return_contract_code: true,
                        subscribe: false,
                    },
                ));
            }
        }

        // Twin-contract reads, from every node that is not itself a holder.
        for (n, node) in nodes.iter().enumerate() {
            if twin_holders.contains(node) {
                continue;
            }
            let twin_key = twin[(round + n) % twin.len()];
            operations.push(ScheduledOperation::new(
                node.clone(),
                SimOperation::Get {
                    contract_id: *twin_key.id(),
                    return_contract_code: true,
                    subscribe: false,
                },
            ));
        }
        operations.push(ScheduledOperation::new(
            gateway.clone(),
            SimOperation::Get {
                contract_id: *twin[round % twin.len()].id(),
                return_contract_code: true,
                subscribe: false,
            },
        ));

        // And once from the gateway, which is connected to every node and so
        // is the router most likely to accumulate the THREE distinct present
        // peers per contract that `CONTRACT_MIN_OTHER_PEERS` requires before
        // the contract term can adjust or offset anything.
        let scarce_key = scarce[round % scarce.len()];
        operations.push(ScheduledOperation::new(
            gateway.clone(),
            SimOperation::Get {
                contract_id: *scarce_key.id(),
                return_contract_code: true,
                subscribe: false,
            },
        ));
    }

    (operations, replicated, scarce, twin)
}

/// Latency of the first successful GET, and how many terminal GET events
/// preceded it, in transaction-GENERATION order.
///
/// The ordering is `Transaction::created_at_ms`, which under
/// `GlobalSimulationTime` is a deterministic monotonic counter incremented once
/// per ULID generated — an ordering proxy, NOT a virtual-clock reading (its own
/// rustdoc says so). The returned latency IS virtual milliseconds: `elapsed_ms`
/// comes from `Transaction::elapsed`, which reads
/// `GlobalSimulationTime::read_time_ms`. The log's `datetime` field is
/// `Utc::now()` and is deliberately not used anywhere here, so nothing in this
/// module depends on wall-clock time.
///
/// Restricted to GETs because only the inline GET terminals carry an
/// `elapsed_ms` this module can reach through a public accessor; mixing in
/// SUBSCRIBE terminals gave a first event with no latency at all, which read
/// as "nothing ever succeeded".
fn first_get_success(logs: &[crate::tracing::NetLogMessage]) -> (Option<u64>, u64) {
    let mut terminal: Vec<(u64, bool, Option<u64>)> = logs
        .iter()
        .filter_map(|log| {
            let ok = log.kind.get_outcome()?;
            Some((log.tx.created_at_ms(), ok, log.kind.get_elapsed_ms()))
        })
        .collect();
    terminal.sort_by_key(|(created, _, _)| *created);

    let mut preceding = 0u64;
    for (_, ok, elapsed) in &terminal {
        if *ok {
            return (*elapsed, preceding);
        }
        preceding += 1;
    }
    (None, preceding)
}

/// Reset every piece of per-thread simulation state the harness relies on.
///
/// This is `setup_deterministic_state` from
/// `crates/core/tests/simulation_integration.rs`, which the in-crate governance
/// simulation does not call because it runs one simulation per test. This module
/// runs SEVERAL on one thread, and two of them are supposed to be the same run
/// with one flag flipped, so without this the second arm inherits the first
/// arm's RNG and counter state and the A/B compares two different networks.
/// All of it is thread-local, so parallel tests stay isolated.
///
/// It must run BEFORE `SimNetwork::new`, not just before the simulation: node
/// transport keypairs are generated during construction.
fn reset_simulation_state(seed: u64) {
    use crate::config::{
        GlobalRng, GlobalSimulationTime, GlobalTestMetrics, SimulationTransportOpt,
    };

    GlobalRng::set_seed(seed);
    const BASE_EPOCH_MS: u64 = 1577836800000; // 2020-01-01 00:00:00 UTC
    const RANGE_MS: u64 = 5 * 365 * 24 * 60 * 60 * 1000; // ~5 years
    GlobalSimulationTime::set_time_ms(BASE_EPOCH_MS + (seed % RANGE_MS));
    GlobalTestMetrics::reset();
    SimulationTransportOpt::disable();
    crate::contract::clear_crdt_contracts();
    crate::client_events::RequestId::reset_counter();
    crate::client_events::ClientId::reset_counter();
    crate::contract::reset_event_id_counter();
    crate::node::reset_channel_id_counter();
    crate::transport::StreamId::reset_counter();
    crate::transport::reset_nonce_counter();
    crate::test_utils::reset_global_node_index();
}

/// Run one arm: the whole workload with the estimator forced on or off.
///
/// `tag` distinguishes the callers. Lib tests run on parallel threads, the
/// simulation registries are keyed by network NAME, and two tests running the
/// same seed and arm would otherwise share one key and read each other's nodes.
fn run_arm(tag: &str, seed: u64, hierarchical: bool) -> ArmMetrics {
    // The override is a thread-local and Turmoil drives every simulated node on
    // THIS thread, so it reaches all of them. `nodes_routing_hierarchically`
    // below is the check that this is still true.
    let _flag = super::force_hierarchical_routing(hierarchical);
    // Both arms must start from identical thread-local state, or they are not
    // the same network with one flag flipped.
    reset_simulation_state(seed);

    let network = format!(
        "hier-sim-{tag}-{}-{seed:x}",
        if hierarchical { "on" } else { "off" }
    );
    let (operations, replicated, scarce, twin) = build_workload(&network);

    // Each scheduled operation consumes 3 virtual seconds in the controlled
    // runner, so the wall must exceed startup + 3 * ops + the post-op settle.
    let op_seconds = 3 * operations.len() as u64;
    let post_op_wait = Duration::from_secs(60);
    let sim_duration = Duration::from_secs(op_seconds + 180);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let mut sim = rt.block_on(async {
        SimNetwork::new(
            &network, 1, // gateways
            NODES, 7,  // max_htl
            3,  // rnd_if_htl_above
            10, // max_connections
            2,  // min_connections
            seed,
        )
        .await
    });
    // The production `ContractExecutor` path, as the repo's testing rules ask
    // for; `MockRuntime` has its own implementation that can drift (#3141).
    sim.use_mock_wasm = true;
    // Start the read workload against a FORMED ring in both arms. Without this
    // the early rounds fail for topology reasons that have nothing to do with
    // the estimator, which adds seed noise to exactly the rates being compared.
    sim.wait_for_join_convergence_before_ops(1.0, Duration::from_secs(120));

    let logs_handle = sim.event_logs_handle();
    let result = sim.run_controlled_simulation(seed, operations, sim_duration, post_op_wait);

    assert!(
        result.turmoil_result.is_ok(),
        "[{network}] controlled simulation must complete: {:?}",
        result.turmoil_result.err()
    );

    let logs = rt.block_on(async { logs_handle.lock().await.clone() });
    collect_metrics(&network, &logs, &result, &replicated, &scarce, &twin)
}

fn collect_metrics(
    network: &str,
    logs: &[crate::tracing::NetLogMessage],
    result: &ControlledSimulationResult,
    replicated: &[ContractKey],
    scarce: &[ContractKey],
    twin: &[ContractKey],
) -> ArmMetrics {
    let mut metrics = ArmMetrics::default();

    let replicated_ids: HashSet<ContractInstanceId> =
        replicated.iter().map(|key| *key.id()).collect();
    let scarce_ids: HashSet<ContractInstanceId> = scarce.iter().map(|key| *key.id()).collect();
    let twin_ids: HashSet<ContractInstanceId> = twin.iter().map(|key| *key.id()).collect();

    // Client GET outcomes, one per client operation, deduplicated per
    // transaction exactly as `crate::tracing::summarize_client_get_outcomes`
    // does. That helper is not used directly because it does not split by
    // contract, and the split is the whole point here: the replicated
    // contracts measure ordinary read health while the scarce ones measure
    // reads that must actually travel.
    let mut per_tx: std::collections::HashMap<
        crate::message::Transaction,
        (GetTerminalOutcome, Option<usize>, ContractInstanceId),
    > = std::collections::HashMap::new();
    for log in logs {
        if let EventKind::Get(GetEvent::ClientTerminal {
            outcome,
            is_sub_op,
            hop_count,
            instance_id,
            ..
        }) = &log.kind
        {
            // Sub-op GETs (repair, renewal, related-fetch) are not client
            // demand and are excluded, as the production summary excludes them.
            if *is_sub_op {
                continue;
            }
            per_tx.insert(log.tx, (*outcome, *hop_count, *instance_id));
        }
        if let Some(ok) = log.kind.subscribe_outcome() {
            metrics.subscribe_total += 1;
            metrics.subscribe_ok += u64::from(ok);
        }
    }
    for (outcome, hop_count, instance_id) in per_tx.values() {
        let ok = matches!(outcome, GetTerminalOutcome::Success);
        if ok && hop_count.unwrap_or(0) >= 1 {
            metrics.network_get_successes += 1;
        }
        if replicated_ids.contains(instance_id) {
            metrics.replicated_get_total += 1;
            metrics.replicated_get_ok += u64::from(ok);
        } else if scarce_ids.contains(instance_id) {
            metrics.scarce_get_total += 1;
            metrics.scarce_get_ok += u64::from(ok);
        } else if twin_ids.contains(instance_id) {
            metrics.twin_get_total += 1;
            metrics.twin_get_ok += u64::from(ok);
        }
    }

    let (latency, preceding) = first_get_success(logs);
    metrics.first_success_latency_ms = latency;
    metrics.ops_before_first_success = preceding;

    let (failures, successes) = result.aggregate_route_outcome_totals();
    metrics.route_failures = failures;
    metrics.route_successes = successes;

    let labels = result.captured_node_labels();
    metrics.nodes_total = labels.len() as u64;

    for label in &labels {
        for key in replicated.iter().chain(scarce.iter()) {
            if result.node_is_receiving_updates(label, key) {
                metrics.subscription_edges += 1;
            }
        }
        for key in replicated {
            if result.is_node_hosting(label, key) {
                metrics.hosting_edges += 1;
            }
        }

        let Some(ring) = result.node_rings.get(label) else {
            continue;
        };
        let snapshot = ring.router.read().snapshot();
        if snapshot.hierarchical_routing_enabled {
            metrics.nodes_routing_hierarchically += 1;
        }
        metrics.contract_estimable_refits += snapshot.hierarchical_contract_estimable_refits;
        metrics.contract_den_below_two_refits +=
            snapshot.hierarchical_contract_den_below_two_refits;
        metrics.contract_effects_applied += snapshot.hierarchical_contract_effects_applied;
        metrics.contract_forecast_offsets += snapshot.hierarchical_contract_forecast_offsets;
        metrics.contract_qualifying_contracts_max = metrics
            .contract_qualifying_contracts_max
            .max(snapshot.hierarchical_contract_qualifying_contracts);
        metrics.contracts_tracked += snapshot.hierarchical_contracts as u64;
        metrics.failure_events += snapshot.hierarchical_failure_events as u64;
        if let Some(tau2) = snapshot.hierarchical_contract_tau2 {
            metrics.contract_tau2.push(tau2);
        }
    }

    eprintln!(
        "[{network}] replicated_get {}/{} ({:.3}) scarce_get {}/{} ({:.3}) \
         twin_get {}/{} ({:.3}) net_ok {} \
         subscribe {}/{} ({:.3}) \
         sub_edges {} hosting_edges {} first_success {:?}ms after {} terminals \
         route ok/fail {}/{} | hierarchical nodes {}/{} failure_events {} contracts {} \
         estimable_refits {} den<2 {} effects {} offsets {} qualifying_max {} tau2 {:?}",
        metrics.replicated_get_ok,
        metrics.replicated_get_total,
        metrics.replicated_get_rate(),
        metrics.scarce_get_ok,
        metrics.scarce_get_total,
        metrics.scarce_get_rate(),
        metrics.twin_get_ok,
        metrics.twin_get_total,
        metrics.twin_get_rate(),
        metrics.network_get_successes,
        metrics.subscribe_ok,
        metrics.subscribe_total,
        metrics.subscribe_rate(),
        metrics.subscription_edges,
        metrics.hosting_edges,
        metrics.first_success_latency_ms,
        metrics.ops_before_first_success,
        metrics.route_successes,
        metrics.route_failures,
        metrics.nodes_routing_hierarchically,
        metrics.nodes_total,
        metrics.failure_events,
        metrics.contracts_tracked,
        metrics.contract_estimable_refits,
        metrics.contract_den_below_two_refits,
        metrics.contract_effects_applied,
        metrics.contract_forecast_offsets,
        metrics.contract_qualifying_contracts_max,
        metrics.contract_tau2,
    );

    metrics
}

/// Run both arms over every seed once.
///
/// One test, not three, and deliberately: CI runs each test in its own
/// `nextest` process, so three tests asserting on the same A/B would run the
/// simulations three times over. The assertion blocks below carry their own
/// messages, so a failure still names its cause.
fn run_ab() -> BTreeMap<u64, (ArmMetrics, ArmMetrics)> {
    SEEDS
        .iter()
        .map(|&seed| {
            let off = run_arm("ab", seed, false);
            let on = run_arm("ab", seed, true);
            (seed, (off, on))
        })
        .collect()
}

fn column(
    runs: &BTreeMap<u64, (ArmMetrics, ArmMetrics)>,
    on: bool,
    f: fn(&ArmMetrics) -> f64,
) -> Vec<f64> {
    runs.values()
        .map(|(o, n)| if on { f(n) } else { f(o) })
        .collect()
}

#[test]
fn hierarchical_routing_simulation_ab() {
    let runs = run_ab();

    // ---------------------------------------------------------------
    // 1. The override really did reach every simulated router.
    //
    // Without this the whole comparison could silently be the OFF arm run
    // twice, and that failure looks exactly like a clean pass. The override is
    // a thread-local and Turmoil must drive every node on the calling thread;
    // if node execution ever moves off that thread, this is what says so.
    // ---------------------------------------------------------------
    for (seed, (off, on)) in &runs {
        assert!(
            on.nodes_total > 0,
            "seed {seed:x}: no node published its Ring"
        );
        assert_eq!(
            on.nodes_routing_hierarchically, on.nodes_total,
            "seed {seed:x}: the hierarchical override reached only {} of {} routers, so the ON \
             arm is not an ON arm",
            on.nodes_routing_hierarchically, on.nodes_total
        );
        assert_eq!(
            off.nodes_routing_hierarchically, 0,
            "seed {seed:x}: the OFF arm routed hierarchically on {} routers, so the control is \
             not a control",
            off.nodes_routing_hierarchically
        );
        assert!(
            on.failure_events > 0,
            "seed {seed:x}: the estimator routed but its failure stage learned nothing, so \
             nothing it forecast was informed by this run"
        );
    }

    // ---------------------------------------------------------------
    // 2. Both arms formed a network and served reads.
    //
    // A comparison between two broken arms is not evidence. These floors are
    // deliberately loose: they say the workload ran, not that it ran well.
    // ---------------------------------------------------------------
    for (seed, (off, on)) in &runs {
        for (arm, m) in [("off", off), ("on", on)] {
            assert!(
                m.replicated_get_total >= 10,
                "seed {seed:x} arm {arm}: only {} client GETs for replicated contracts",
                m.replicated_get_total
            );
            assert!(
                m.scarce_get_total >= 10,
                "seed {seed:x} arm {arm}: only {} client GETs for scarce contracts",
                m.scarce_get_total
            );
            assert!(
                m.twin_get_total >= 10,
                "seed {seed:x} arm {arm}: only {} client GETs for twin-held contracts",
                m.twin_get_total
            );
            assert!(
                m.subscribe_total >= 10,
                "seed {seed:x} arm {arm}: only {} terminal SUBSCRIBEs",
                m.subscribe_total
            );
            assert!(
                m.subscription_edges > 0,
                "seed {seed:x} arm {arm}: no subscription tree formed"
            );
            assert!(
                m.network_get_successes > 0,
                "seed {seed:x} arm {arm}: every successful GET was served locally, so this run \
                 exercised no routing at all"
            );
            assert!(
                m.first_success_latency_ms.is_some(),
                "seed {seed:x} arm {arm}: no GET ever succeeded"
            );
        }
    }

    // ---------------------------------------------------------------
    // 3. Non-inferiority, per metric, against the OFF arm.
    //
    // MARGIN PROVENANCE. Each margin is the OFF arm's own seed-to-seed spread
    // (max minus min over `SEEDS`) plus a floor. That is the smallest
    // difference a run of this size can tell from seed noise: a tighter margin
    // would fail on noise, a looser one would assert nothing. These are
    // NON-INFERIORITY bars, not superiority bars — the estimator is not
    // expected to improve simulated success rates, only not to damage them.
    // Re-derive them if `SEEDS`, `ROUNDS` or the network size change, and say
    // so here when you do.
    // ---------------------------------------------------------------
    let checks: [(&str, fn(&ArmMetrics) -> f64, bool); 6] = [
        (
            "replicated GET success rate",
            |m| m.replicated_get_rate(),
            true,
        ),
        ("scarce GET success rate", |m| m.scarce_get_rate(), true),
        ("twin-held GET success rate", |m| m.twin_get_rate(), true),
        ("subscribe success rate", |m| m.subscribe_rate(), true),
        ("subscription edges", |m| m.subscription_edges as f64, false),
        (
            "network-traversed GET successes",
            |m| m.network_get_successes as f64,
            false,
        ),
    ];

    eprintln!("[ab] metric | off per seed | on per seed | off mean | on mean");
    let mut failures = Vec::new();
    for (name, metric, is_rate) in checks {
        let off = column(&runs, false, metric);
        let on = column(&runs, true, metric);
        let spread = off.iter().cloned().fold(f64::NEG_INFINITY, f64::max)
            - off.iter().cloned().fold(f64::INFINITY, f64::min);
        let margin = if is_rate {
            (spread + 0.05).min(0.5)
        } else {
            // A count, so the floor is a share of the OFF mean rather than an
            // absolute on [0, 1].
            (spread + 0.10 * mean(&off)).max(1.0)
        };
        let (off_mean, on_mean) = (mean(&off), mean(&on));
        eprintln!(
            "[ab] {name} | {off:?} | {on:?} | {off_mean:.3} | {on_mean:.3} | margin {margin:.3}"
        );
        if on_mean < off_mean - margin {
            failures.push(format!(
                "{name}: on {on_mean:.3} vs off {off_mean:.3}, worse by more than the {margin:.3} \
                 noise margin (off per seed {off:?}, on per seed {on:?})"
            ));
        }
    }
    eprintln!(
        "[ab] first-GET-success latency ms | off {:?} | on {:?}",
        runs.values()
            .map(|(o, _)| o.first_success_latency_ms)
            .collect::<Vec<_>>(),
        runs.values()
            .map(|(_, n)| n.first_success_latency_ms)
            .collect::<Vec<_>>(),
    );
    eprintln!(
        "[ab] terminal GETs before first success | off {:?} | on {:?}",
        runs.values()
            .map(|(o, _)| o.ops_before_first_success)
            .collect::<Vec<_>>(),
        runs.values()
            .map(|(_, n)| n.ops_before_first_success)
            .collect::<Vec<_>>(),
    );
    assert!(
        failures.is_empty(),
        "hierarchical routing is worse than legacy beyond the noise margin:\n{}",
        failures.join("\n")
    );

    // ---------------------------------------------------------------
    // 4. Contract term: a REPORT, plus the one thing that must hold.
    //
    // Whether the term engages depends on the traffic clearing several
    // preconditions at once — a replicated (contract, peer) table, at least two
    // qualifying contracts, and a between-contract contrast in failure
    // residuals larger than the noise the estimator subtracts. A simulation of
    // this size is not guaranteed to reach them, and pretending otherwise is
    // the failure this block exists to prevent: the counters are printed so a
    // reader can see for themselves. The assertion pins only that the contract
    // table was POPULATED, which is what says the workload's contracts really
    // did repeat at a single router. If the term itself never activates, this
    // case is measuring the horizon menu and the cost path and NOT the contract
    // term, and the printed line says so in those words.
    // ---------------------------------------------------------------
    for (seed, (_, on)) in &runs {
        eprintln!(
            "[contract-term] seed {seed:x}: tracked {} contracts, estimable_refits {}, \
             den<2 refits {}, effects_applied {}, forecast_offsets {}, qualifying_max {}, \
             tau2 {:?}, route ok/fail {}/{}",
            on.contracts_tracked,
            on.contract_estimable_refits,
            on.contract_den_below_two_refits,
            on.contract_effects_applied,
            on.contract_forecast_offsets,
            on.contract_qualifying_contracts_max,
            on.contract_tau2,
            on.route_successes,
            on.route_failures,
        );
        assert!(
            on.contracts_tracked > 0,
            "seed {seed:x}: the failure stage's contract table is empty, so the workload's \
             contracts did not repeat at any single router and the mechanism cannot be \
             exercised by this workload at all"
        );
        if !on.contract_term_activated() {
            eprintln!(
                "[contract-term] seed {seed:x}: NOT ACTIVATED — this run measured the horizon \
                 menu and the cost path only, not the contract term."
            );
        }
    }
}
