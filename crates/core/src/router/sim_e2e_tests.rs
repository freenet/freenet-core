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
//! both arms. That is the cross-test-interference shape
//! `.claude/rules/testing.md` warns about, benign under `nextest`'s
//! process-per-test and not under plain `cargo test`. The thread-local reaches
//! every simulated node because `run_controlled_simulation` drives Turmoil on
//! the calling thread, so both arms can run in one process with no env var at
//! all.
//!
//! That thread-local is load-bearing, so a change that moves node execution off
//! the calling thread must FAIL here rather than silently measure the OFF arm
//! twice. The assertion that achieves it is `off.failure_events == 0` in
//! [`assert_seed`], NOT the `nodes_routing_hierarchically` counter beside it:
//! the counter is read on this thread and so is a readback of the thread-local
//! itself, while `failure_events` is estimator state written by whatever thread
//! ran the node. Section 1 of [`assert_seed`] spells out why the difference
//! matters, and it matters more since the process default became ON, because a
//! fall-through now lands on the estimator rather than off it.
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
//! all, which is precisely how the #5655 synthetic bake-off ended up vacuous
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
//! differences: the margins below are floors sized from the largest per-seed
//! arm-to-arm difference measured over four passes, which is what bounds what a
//! run of this size can see.
//! It answers one narrow question the gate asks and nothing else: does routing
//! with the estimator on still form a network, serve reads and build a
//! subscription tree, and does the contract term ever engage.

use std::collections::HashSet;
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
/// `NotFound`s from peers that do not hold them, and then succeed, which is
/// what turns those `NotFound`s into trained `Failure` labels (see the module
/// docs). These are what give the contract term something to explain away.
const SCARCE_CONTRACTS: u8 = 3;

/// Contracts held by exactly TWO nodes, so a read for them travels but has two
/// possible terminals. Between them and [`SCARCE_CONTRACTS`] the workload
/// spans three replication levels, which is what gives the contract term a
/// between-contract contrast to find at all.
const TWIN_CONTRACTS: u8 = 2;

/// Rounds of the read workload. Each round issues, from every non-gateway node,
/// one GET and one SUBSCRIBE against a replicated contract and one GET each
/// against a scarce and a twin-held one, so every contract is asked for by
/// every peer many times over.
const ROUNDS: usize = 10;

/// Regular (non-gateway) nodes.
const NODES: usize = 9;

/// Connection cap, and the reason it is well below the peer count.
///
/// At `max_connections = 10` with eight peers every node is connected to every
/// other, so a read reaches any holder in one hop and routing never has a
/// decision to make. Measured on exactly that configuration: every GET class
/// returned 1.000 in both arms, `failure_events` were plentiful but almost all
/// successes, and the contract term stayed inert. A cap of five over ten peers
/// leaves a genuine topology, so reads for a single-holder contract travel,
/// collect `NotFound`s on the way, and produce the trained failure labels the
/// term needs.
const MAX_CONNECTIONS: usize = 5;
const MIN_CONNECTIONS: usize = 2;

/// Seeds the A/B runs over, ONE PER TEST. Three is few, and the spread across
/// them is reported rather than hidden.
///
/// Each seed is asserted on its own rather than pooled into a mean. Pooling
/// let one seed collapse while the other two absorbed it: see the margin
/// provenance note in [`assert_seed`].
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
    /// Terminal SUBSCRIBE outcomes, all contracts, ONE PER TRANSACTION.
    subscribe_ok: u64,
    subscribe_total: u64,
    /// Raw terminal-SUBSCRIBE log events, before the per-transaction
    /// deduplication above. Reported, never gated on: the gap between this and
    /// `subscribe_total` is the size of the route-length weighting the
    /// deduplication removes, and printing it is what stops that bias
    /// reappearing unnoticed.
    subscribe_raw_events: u64,
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
    // answer NotFound, which becomes a trained Failure once the operation
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
    // one, which is what `CONTRACT_MIN_OTHER_PEERS` requires before the term
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
/// per ULID generated: an ordering proxy, NOT a virtual-clock reading (its own
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

    // The controlled runner's default settle between scheduled operations is 3
    // virtual seconds. These operations run against an already-formed ring and
    // finish in about a millisecond of virtual time, so 3 s buys nothing and
    // costs a third of the wall clock: at the default this case took 708 s for
    // six simulations. One second keeps the operations well separated and
    // brings that down. Both arms use the same interval, so it cannot bias the
    // comparison.
    let op_interval = Duration::from_secs(1);
    let op_seconds = operations.len() as u64;
    let post_op_wait = Duration::from_secs(60);
    let sim_duration = Duration::from_secs(op_seconds + 180);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let mut sim = rt.block_on(async {
        SimNetwork::new(
            &network,
            1, // gateways
            NODES,
            7, // max_htl
            3, // rnd_if_htl_above
            MAX_CONNECTIONS,
            MIN_CONNECTIONS,
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
    sim.with_controlled_op_interval(op_interval);

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

    // Terminal SUBSCRIBE outcomes, also deduplicated per transaction, and for a
    // sharper reason than tidiness.
    //
    // `SubscribeEvent::SubscribeSuccess` is emitted at EVERY node that
    // establishes the subscription as the response bubbles back toward the
    // requester, while `SubscribeTimeout` is emitted once, at the originator.
    // Counting raw events therefore weights successes by route length and
    // failures not at all. Route length is precisely what the routing model
    // under test changes, so the raw rate is confounded by the variable this
    // A/B exists to hold everything else constant against: a model that routes
    // over longer paths would score a HIGHER subscribe rate for that reason
    // alone. Measured, not assumed; the printed `raw` count beside the
    // deduplicated one is the evidence.
    //
    // Last write wins, exactly as the GET path above and
    // `crate::tracing::summarize_client_get_outcomes` do. The logs are in
    // emission order and the response travels back toward the originator, so
    // the last terminal recorded for a transaction is the one nearest the
    // client. Deliberately NOT a "success beats failure" precedence rule:
    // that would count a transaction as a success when a downstream node
    // established a subscription but the originator still timed out, which is
    // an overstatement in the same direction as the bias being removed.
    let mut subscribe_per_tx: std::collections::HashMap<crate::message::Transaction, bool> =
        std::collections::HashMap::new();

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
            metrics.subscribe_raw_events += 1;
            subscribe_per_tx.insert(log.tx, ok);
        }
    }

    metrics.subscribe_total = subscribe_per_tx.len() as u64;
    metrics.subscribe_ok = subscribe_per_tx.values().filter(|ok| **ok).count() as u64;

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
         subscribe {}/{} ({:.3}) raw_subscribe_events {} \
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
        metrics.subscribe_raw_events,
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

/// Run both arms over ONE seed.
///
/// SPLIT PER SEED, and the reason is worth keeping because the earlier form of
/// this file argued the opposite. A single test running all three seeds
/// measured 177.741s on the CI runner against the `ci` profile's 240s cap
/// (`.config/nextest.toml`, `slow-timeout = { period = "120s",
/// terminate-after = 2 }`), so 1.35x headroom, and 267-278s locally. That is
/// less headroom than the three simulation tests in that same file's #5176
/// block, which were given an override at ~1.6x. The `Simulation` job runs on
/// `merge_group` as well as `pull_request`, and `retries = 2` means a breach
/// costs three full runs, so a systematically slower runner would stall the
/// merge queue rather than red one PR.
///
/// Splitting divides the work instead of buying more time: one seed's two arms
/// per test, so each is about a third of the runtime with real headroom under
/// the DEFAULT cap and no override to maintain, nextest runs the three in
/// parallel, and a failure names its seed instead of the whole case.
///
/// The earlier comment here warned that three tests would run the simulations
/// three times over. That was true of three tests each asserting on the same
/// full A/B; it is not true of three tests each owning one seed.
fn run_seed(seed: u64) -> (ArmMetrics, ArmMetrics) {
    let off = run_arm("ab", seed, false);
    let on = run_arm("ab", seed, true);
    (off, on)
}

/// One metric read off an arm. Named rather than written inline so the
/// non-inferiority table below stays inside `clippy::type_complexity`.
type Metric = fn(&ArmMetrics) -> f64;

/// A gated metric: its name, how to read it, and whether it lives on `[0, 1]`
/// (which decides how its noise margin is floored).
type Check = (&'static str, Metric, bool);

/// Everything asserted about one seed's pair of arms.
fn assert_seed(seed: u64, off: &ArmMetrics, on: &ArmMetrics) {
    // ---------------------------------------------------------------
    // 1. The override really did reach every simulated router.
    //
    // Without this the whole comparison could silently be the OFF arm run
    // twice, and that failure looks exactly like a clean pass.
    //
    // `nodes_routing_hierarchically` ALONE cannot establish that, and it is
    // important not to believe it does. It comes from `Router::snapshot()`,
    // which evaluates `hierarchical_routing_enabled()` at call time, and under
    // `cfg(test)` that reads the CALLING thread's `TEST_HIERARCHICAL_OVERRIDE`.
    // `collect_metrics` runs on the test thread inside `run_arm`'s guard
    // scope, so that counter is a readback of the thread-local this thread just
    // set, multiplied by the node count. It says nothing about the threads the
    // routers actually ran on. If turmoil ever polls host futures off the
    // calling thread, those routers fall through to the PROCESS default, which
    // is now ON, and both arms would route hierarchically while both of those
    // assertions still passed: permanently green, comparing ON against ON.
    //
    // `failure_events` is what closes that hole, because it crosses threads.
    // It reads `hierarchical[0].window_events`, estimator state populated by
    // `add_event` on whichever thread ran it. With the flag off and no
    // `FREENET_ROUTING_DATASET` the estimator is never fed at all (see
    // `hierarchical_computed`), so the OFF arm must show exactly zero. If the
    // OFF arm's routers silently fell through to the process default, this is
    // the assertion that fails.
    // ---------------------------------------------------------------
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
    assert_eq!(
        off.failure_events, 0,
        "seed {seed:x}: the OFF arm's failure stage ingested {} events, so its routers were \
         computing the hierarchical estimator. Either the estimator is being fed with the \
         flag off, or the OFF arm's nodes ran on a thread that did not carry the override \
         and fell through to the process default (which is ON). Both make this an ON-vs-ON \
         comparison that every other assertion here would pass.",
        off.failure_events
    );

    // ---------------------------------------------------------------
    // 2. Both arms formed a network and served reads.
    //
    // A comparison between two broken arms is not evidence. These floors are
    // deliberately loose: they say the workload ran, not that it ran well.
    // ---------------------------------------------------------------
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
            "seed {seed:x} arm {arm}: only {} terminal SUBSCRIBE transactions",
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

    // ---------------------------------------------------------------
    // 3. Non-inferiority, per metric, WITHIN this seed.
    //
    // MARGIN PROVENANCE. Each margin is a floor alone: 0.05 for a rate, and
    // for a count the larger of 1 and a tenth of the OFF arm's value. It is
    // the paired ON-vs-OFF difference on the SAME seed that is compared
    // against it.
    //
    // This replaced an earlier cross-seed form whose margin was the OFF arm's
    // seed-to-seed spread PLUS these same floors, compared against the two
    // arms' MEANS. That had two defects, and dropping it fixes both. The
    // spread was the wrong noise model: the arms share seeds, so seed-to-seed
    // variation cancels in the paired difference and adding it only widened
    // the bar, to the point where a metric with a large spread could not fail
    // the check at all (subscription edges carried a margin of 7.967 of which
    // 7.0 was spread). And comparing MEANS let one seed collapse while the
    // other two absorbed it: OFF [0.95, 0.95, 0.95] against ON
    // [0.95, 0.95, 0.80] sits exactly on the bar and passes, hiding a
    // 15-point regression on a third of the runs.
    //
    // The floors are measured, not invented. Across four passes of the shipped
    // configuration the largest per-seed ON-vs-OFF difference on any rate was
    // 0.0077 (subscribe) against this 0.05 floor, and on subscription edges it
    // was 2 of 70 against a floor of 7.0. Every other metric differed by zero.
    // These are NON-INFERIORITY bars, not superiority bars: the estimator is
    // not expected to improve simulated success rates, only not to damage
    // them. Re-derive them if `SEEDS`, `ROUNDS` or the network size change,
    // and say so here when you do.
    // ---------------------------------------------------------------
    let checks: [Check; 6] = [
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

    eprintln!("[ab] seed {seed:x}: metric | off | on | margin");
    let mut failures = Vec::new();
    for (name, metric, is_rate) in checks {
        let (off_value, on_value) = (metric(off), metric(on));
        let margin = if is_rate {
            0.05
        } else {
            (0.10 * off_value).max(1.0)
        };
        eprintln!(
            "[ab] seed {seed:x}: {name} | {off_value:.3} | {on_value:.3} | margin {margin:.3}"
        );
        if on_value < off_value - margin {
            failures.push(format!(
                "{name}: on {on_value:.3} vs off {off_value:.3}, worse by more than the \
                 {margin:.3} noise margin"
            ));
        }
    }
    eprintln!(
        "[ab] seed {seed:x}: first-GET-success latency ms | off {:?} | on {:?}",
        off.first_success_latency_ms, on.first_success_latency_ms
    );
    eprintln!(
        "[ab] seed {seed:x}: terminal GETs before first success | off {} | on {}",
        off.ops_before_first_success, on.ops_before_first_success
    );
    assert!(
        failures.is_empty(),
        "seed {seed:x}: hierarchical routing is worse than legacy beyond the noise margin:\n{}",
        failures.join("\n")
    );

    // ---------------------------------------------------------------
    // 4. Contract term: a REPORT, plus the one thing that must hold.
    //
    // Whether the term engages depends on the traffic clearing several
    // preconditions at once: a replicated (contract, peer) table, at least two
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
            "[contract-term] seed {seed:x}: NOT ACTIVATED. This run measured the horizon \
             menu and the cost path only, not the contract term."
        );
    }
}

#[test]
fn hierarchical_routing_simulation_ab_seed_0001() {
    let seed = SEEDS[0];
    let (off, on) = run_seed(seed);
    assert_seed(seed, &off, &on);
}

#[test]
fn hierarchical_routing_simulation_ab_seed_0002() {
    let seed = SEEDS[1];
    let (off, on) = run_seed(seed);
    assert_seed(seed, &off, &on);
}

#[test]
fn hierarchical_routing_simulation_ab_seed_0003() {
    let seed = SEEDS[2];
    let (off, on) = run_seed(seed);
    assert_seed(seed, &off, &on);
}
