//! Large-N topology model guarding peer degree against uptime creep (#5814).
//!
//! Every model node owns a real `ConnectionManager` (and through it a real
//! `TopologyManager`), so acceptance (`should_accept`), targets, swaps and
//! pruning (`adjust_topology`), the add/concurrency budgets, the lattice
//! probe's hit/miss test (`record_lattice_probe_result`) and its scheduling
//! (`LatticeProbeScheduler`) are production code. Only the plumbing is
//! modelled: the CONNECT walk (greedy toward the target over the model graph
//! with the joiner's connections and recently-failed addresses pre-excluded,
//! near-terminus probabilistic acceptance, terminus acceptance, bounded uphill
//! retry), connection establishment (instant, but failing at random like a
//! hole punch, which records the failed address and re-routes), peer
//! restarts, and the maintenance-loop tick (production tick and backoff
//! constants). Not modelled: transport and NAT details, latency, score-based
//! next-hop choice, traffic (the bandwidth meter stays empty, i.e. a lightly loaded network,
//! which is where nothing prunes below `max_connections`), location backoff
//! and router learning.
//!
//! Before #5814 the route-to-self probe re-fired every few minutes forever and
//! every acceptor was kept, so a converged peer gained a non-lattice link per
//! probe and degree climbed with uptime. The test runs the same seed under the
//! old discovery and the production scheduler: the old arm must reproduce the
//! creep (so the model is sensitive to it), and the new arm must hold degree
//! near the low-usage band without losing lattice coverage.

use super::super::{
    LatticeProbeProgress, LatticeProbeScheduler, LatticeSides, lattice_probe_progress,
};
use super::*;
use crate::topology::TopologyAdjustment;
use crate::topology::rate::Rate;
use crate::transport::{TransportKeypair, TransportPublicKey};
use std::collections::{BTreeSet, BinaryHeap, HashSet};

// Mirrors of production constants (connect.rs / ring.rs, non-test values).
const NEAR_TERMINUS_DISTANCE: f64 = 0.05;
const NEAR_TERMINUS_ACCEPT_PROB: f64 = 0.6;
const UPHILL_BUDGET: u8 = 8;
const MAX_HTL: u8 = 10;
const CHECK_TICK: f64 = 60.0;
const FAST_TICK: f64 = 5.0;
const FAST_TICK_BACKOFF_THRESHOLD: u32 = 6;
// The lattice probe intervals are the production ones.
use super::super::lattice_probe_timing::{
    RECHECK_MAX as LATTICE_RECHECK_MAX, RECHECK_MIN as LATTICE_RECHECK_MIN,
    RETRY_MAX as LATTICE_RETRY_MAX, RETRY_MIN as LATTICE_RETRY_MIN, TAU_MAX as LATTICE_TAU_MAX,
    TAU0 as LATTICE_TAU0,
};
/// Chance that an accepted connection fails to establish (hole punch).
const CONNECT_FAILURE_RATE: f64 = 0.2;
const DEFERRED_SWAP_DROP_TTL: f64 = 120.0;

const MIN_CONNECTIONS: usize = super::super::Ring::DEFAULT_MIN_CONNECTIONS;
const MAX_CONNECTIONS: usize = super::super::Ring::DEFAULT_MAX_CONNECTIONS;

const PORT_BASE: u16 = 10_000;

fn node_index(addr: SocketAddr) -> usize {
    usize::from(addr.port() - PORT_BASE)
}

struct Node {
    cm: ConnectionManager,
    addr: SocketAddr,
    loc: Location,
    pending: BTreeSet<Location>,
    deferred_drops: Vec<(SocketAddr, f64)>,
    last_count: usize,
    no_progress_ticks: u32,
    probe: Probe,
    /// When this peer next restarts (drops every link and rejoins), if churn is on.
    restart_at: f64,
}

/// How the route-to-self lattice probe is scheduled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Discovery {
    /// Pre-#5814: re-fire on a backoff capped at tau_max, forever.
    Continuous,
    /// Production `LatticeProbeScheduler`: sleep once a probe misses.
    Production,
}

enum Probe {
    Continuous {
        next_at: f64,
        attempt: u32,
        last: Option<LatticeSides>,
    },
    Production(LatticeProbeScheduler),
}

impl Probe {
    fn new(discovery: Discovery) -> Self {
        match discovery {
            Discovery::Continuous => Probe::Continuous {
                next_at: 0.0,
                attempt: 0,
                last: None,
            },
            Discovery::Production => Probe::Production(LatticeProbeScheduler::new(
                Instant::now(),
                Default::default(),
                ExponentialBackoff::new(LATTICE_TAU0, LATTICE_TAU_MAX),
                ExponentialBackoff::new(LATTICE_RECHECK_MIN, LATTICE_RECHECK_MAX),
                ExponentialBackoff::new(LATTICE_RETRY_MIN, LATTICE_RETRY_MAX),
            )),
        }
    }
}

/// Mean degree and lattice coverage at one sample time.
#[derive(Debug, Clone, Copy)]
struct Sample {
    degree: f64,
    /// Fraction of peers connected to BOTH their true ring successor and
    /// predecessor.
    coverage: f64,
}

struct Model {
    nodes: Vec<Node>,
    key: TransportPublicKey,
    discovery: Discovery,
    /// Mean time between restarts of one peer, if peers restart.
    mean_uptime_secs: Option<f64>,
    /// Node indices in ring order.
    ring_order: Vec<usize>,
}

fn new_cm(
    key: &TransportPublicKey,
    addr: SocketAddr,
    loc: Location,
    gateway: bool,
) -> ConnectionManager {
    ConnectionManager::init(
        Rate::new_per_second(1_000_000.0),
        Rate::new_per_second(1_000_000.0),
        MIN_CONNECTIONS,
        MAX_CONNECTIONS,
        7,
        (
            key.clone(),
            Some(addr),
            AtomicU64::new(u64::from_le_bytes(loc.as_f64().to_le_bytes())),
        ),
        gateway,
        10,
        Duration::from_secs(60),
        0,
    )
}

/// The pre-#5814 change classifier, frozen for the old-discovery arm: a side
/// that widened (nearest dropped, a farther one remains) was a plateau, not a
/// regression, so it did not reset the backoff.
fn old_lattice_probe_progress(
    prev: Option<LatticeSides>,
    curr: LatticeSides,
) -> LatticeProbeProgress {
    let mut progress = lattice_probe_progress(prev, curr);
    if let Some(prev) = prev {
        let lost = |p: Option<f64>, c: Option<f64>| p.is_some() && c.is_none();
        progress.regressed = lost(prev.succ, curr.succ) || lost(prev.pred, curr.pred);
    }
    progress
}

fn exp_sample(mean: f64) -> f64 {
    -mean * (1.0 - GlobalRng::random_range(0.0..1.0_f64)).ln()
}

impl Model {
    fn new(n: usize, discovery: Discovery, mean_uptime_secs: Option<f64>) -> Self {
        let key = TransportKeypair::new().public().clone();
        let mut used = HashSet::new();
        let mut nodes = Vec::with_capacity(n + 1);
        while nodes.len() < n + 1 {
            let mut b = [0u8; 4];
            GlobalRng::fill_bytes(&mut b);
            b[0] = 1 + b[0] % 200; // avoid loopback / link-local / multicast
            // The port carries the node index (the location ignores it), so
            // an address maps back to its node without a hash lookup.
            let addr = SocketAddr::from((b, PORT_BASE + nodes.len() as u16));
            let loc = Location::from_address(&addr);
            if !used.insert(loc.as_f64().to_bits()) {
                continue;
            }
            nodes.push(Node {
                cm: new_cm(&key, addr, loc, nodes.is_empty()), // node 0 is the gateway
                addr,
                loc,
                pending: BTreeSet::new(),
                deferred_drops: Vec::new(),
                last_count: 0,
                no_progress_ticks: 0,
                probe: Probe::new(discovery),
                restart_at: mean_uptime_secs.map_or(f64::INFINITY, exp_sample),
            });
        }
        let mut ring_order: Vec<usize> = (0..nodes.len()).collect();
        ring_order.sort_by(|a, b| nodes[*a].loc.as_f64().total_cmp(&nodes[*b].loc.as_f64()));
        Self {
            nodes,
            key,
            discovery,
            mean_uptime_secs,
            ring_order,
        }
    }

    /// Peer `i` restarts: every link drops, and it comes back with fresh state
    /// and rejoins through the gateway on its next tick.
    fn restart(&mut self, i: usize, now: f64) {
        for n in self.neighbors(i) {
            self.unlink(i, n);
        }
        let (addr, loc) = (self.nodes[i].addr, self.nodes[i].loc);
        let node = &mut self.nodes[i];
        node.cm = new_cm(&self.key, addr, loc, false);
        node.pending.clear();
        node.deferred_drops.clear();
        node.last_count = 0;
        node.no_progress_ticks = 0;
        node.probe = Probe::new(self.discovery);
        node.restart_at = now + self.mean_uptime_secs.map_or(f64::INFINITY, exp_sample);
    }

    fn neighbors(&self, i: usize) -> Vec<usize> {
        self.nodes[i]
            .cm
            .connected_peer_addrs()
            .into_iter()
            .map(node_index)
            .collect()
    }

    fn link(&self, a: usize, b: usize) {
        // `a` accepted `b` through should_accept (which reserved the slot).
        self.nodes[a].cm.add_connection(
            self.nodes[b].loc,
            self.nodes[b].addr,
            self.key.clone(),
            true,
        );
        if !self.nodes[b]
            .cm
            .has_connection_or_pending(self.nodes[a].addr)
        {
            self.nodes[b].cm.add_connection(
                self.nodes[a].loc,
                self.nodes[a].addr,
                self.key.clone(),
                false,
            );
        }
    }

    fn unlink(&self, a: usize, b: usize) {
        self.nodes[a].cm.prune_alive_connection(self.nodes[b].addr);
        self.nodes[b].cm.prune_alive_connection(self.nodes[a].addr);
    }

    /// Relay `r` considers accepting joiner `j`. On acceptance the link forms,
    /// unless establishing it fails (then the joiner records the failed address
    /// and the walk re-routes), and a lattice probe's acceptor that is not a
    /// lattice edge is reported as a miss for the probe's generation (the
    /// production CONNECT driver).
    fn try_accept(&self, r: usize, j: usize, probe_generation: Option<u64>) -> bool {
        if !self.nodes[r]
            .cm
            .should_accept(self.nodes[j].loc, self.nodes[j].addr)
        {
            return false;
        }
        if GlobalRng::random_range(0.0..1.0) < CONNECT_FAILURE_RATE {
            self.nodes[r]
                .cm
                .prune_in_transit_connection(self.nodes[j].addr);
            self.nodes[j].cm.record_failed_addr(self.nodes[r].addr);
            self.classify(r, j, probe_generation, false);
            return false;
        }
        self.link(r, j);
        self.classify(r, j, probe_generation, true);
        true
    }

    /// The production CONNECT driver's lattice-probe classification.
    fn classify(&self, r: usize, j: usize, probe_generation: Option<u64>, connected: bool) {
        if let Some(generation) = probe_generation {
            self.nodes[j].cm.record_lattice_probe_result(
                self.nodes[r].loc,
                self.nodes[r].addr,
                generation,
                connected,
                // Model peers stay far below max_connections.
                false,
            );
        }
    }

    /// Modelled CONNECT from `j` toward `target`, entering at `first_hop`.
    fn connect(&self, j: usize, target: Location, first_hop: usize, probe_generation: Option<u64>) {
        let mut visited = vec![false; self.nodes.len()];
        for n in self.neighbors(j) {
            visited[n] = true;
        }
        for addr in self.nodes[j].cm.recently_failed_addrs() {
            visited[node_index(addr)] = true;
        }
        visited[j] = true;
        let dist = |i: usize| self.nodes[i].loc.distance(target).as_f64();
        let mut r = first_hop;
        let mut ttl = MAX_HTL;
        let mut uphill = UPHILL_BUDGET;
        loop {
            visited[r] = true;
            let next = if ttl > 0 {
                self.neighbors(r)
                    .into_iter()
                    .filter(|n| !visited[*n] && dist(*n) <= dist(r))
                    .min_by(|a, b| dist(*a).total_cmp(&dist(*b)))
            } else {
                None
            };
            if let Some(n) = next {
                let d = dist(r);
                if d < NEAR_TERMINUS_DISTANCE
                    && GlobalRng::random_range(0.0..1.0)
                        < NEAR_TERMINUS_ACCEPT_PROB * (1.0 - d / NEAR_TERMINUS_DISTANCE)
                {
                    self.try_accept(r, j, probe_generation);
                }
                r = n;
                ttl -= 1;
                continue;
            }
            if self.try_accept(r, j, probe_generation) || uphill == 0 || ttl < 2 {
                return;
            }
            let cands: Vec<usize> = self
                .neighbors(r)
                .into_iter()
                .filter(|n| !visited[*n])
                .collect();
            let close: Vec<usize> = cands
                .iter()
                .copied()
                .filter(|n| dist(*n) < NEAR_TERMINUS_DISTANCE * 2.0)
                .collect();
            let pool = if close.is_empty() { cands } else { close };
            let Some(u) = pool
                .into_iter()
                .min_by(|a, b| dist(*a).total_cmp(&dist(*b)))
            else {
                return;
            };
            uphill -= 1;
            ttl -= 1;
            r = u;
        }
    }

    /// One `connection_maintenance` iteration for node `i`; returns the delay
    /// until the next one.
    fn tick(&mut self, i: usize, now: f64) -> f64 {
        if i != 0 && now >= self.nodes[i].restart_at {
            self.restart(i, now);
        }
        let min = self.nodes[i].cm.min_connections;
        let max = self.nodes[i].cm.max_connections;
        let count = self.nodes[i].cm.connection_count();

        // Isolated: join through the gateway, then CONNECT toward own location.
        if count == 0 && i != 0 {
            if self.nodes[0]
                .cm
                .should_accept(self.nodes[i].loc, self.nodes[i].addr)
            {
                self.link(0, i);
            }
            self.connect(i, self.nodes[i].loc, 0, None);
        }

        // Route-to-self lattice probe (ring.rs), queued just before the drain.
        if self.nodes[i].cm.nn_lattice_active() {
            let node = &mut self.nodes[i];
            let sides = LatticeSides {
                succ: node.cm.nearest_lattice_neighbor_dist(true),
                pred: node.cm.nearest_lattice_neighbor_dist(false),
            };
            let backoff = ExponentialBackoff::new(LATTICE_TAU0, LATTICE_TAU_MAX);
            let jitter = GlobalRng::random_range(0.8..=1.2);
            let fire = match &mut node.probe {
                Probe::Continuous {
                    next_at,
                    attempt,
                    last,
                } => {
                    let progress = old_lattice_probe_progress(*last, sides);
                    *last = Some(sides);
                    if progress.improved || progress.regressed {
                        *attempt = 0;
                        *next_at = now;
                    }
                    let fire = now >= *next_at;
                    if fire {
                        *next_at = now + backoff.delay(*attempt).as_secs_f64() * jitter;
                        *attempt = attempt.saturating_add(1);
                    }
                    fire
                }
                Probe::Production(scheduler) => scheduler
                    .tick_for(&node.cm, Instant::now(), || jitter)
                    .fired
                    .is_some(),
            };
            if fire {
                node.pending.insert(node.loc);
            }
        }

        // Drain queued targets, bounded by the production concurrency cap.
        let max_concurrent = super::super::calculate_max_concurrent_connections(count, min);
        let mut active = 0;
        while let Some(target) = self.nodes[i].pending.pop_first() {
            if active >= max_concurrent {
                self.nodes[i].pending.insert(target);
                break;
            }
            let first = self.neighbors(i).into_iter().min_by(|a, b| {
                let da = self.nodes[*a].loc.distance(target).as_f64();
                let db = self.nodes[*b].loc.distance(target).as_f64();
                da.total_cmp(&db)
            });
            let Some(first) = first else { continue };
            // Production tags the drained own-location target as the lattice
            // probe, with the scheduler's generation (ring.rs
            // `lattice_probe_target`). The old discovery recorded no misses.
            let probe_generation = match &self.nodes[i].probe {
                Probe::Production(scheduler)
                    if target == self.nodes[i].loc && self.nodes[i].cm.nn_lattice_active() =>
                {
                    Some(scheduler.generation())
                }
                Probe::Production(_) | Probe::Continuous { .. } => None,
            };
            self.connect(i, target, first, probe_generation);
            active += 1;
        }

        // Production topology decision.
        let current = self.nodes[i].cm.connection_count();
        let neighbor_locations = self.nodes[i].cm.get_connections_by_location();
        let adjustment = self.nodes[i].cm.topology_manager.write().adjust_topology(
            &neighbor_locations,
            &Some(self.nodes[i].loc),
            Instant::now(),
            current,
        );
        match adjustment {
            TopologyAdjustment::AddConnections(targets) => {
                let allowed = super::super::calculate_allowed_connection_additions(
                    current,
                    self.nodes[i].pending.len(),
                    min,
                    max,
                    targets.len(),
                );
                self.nodes[i]
                    .pending
                    .extend(targets.into_iter().take(allowed));
            }
            TopologyAdjustment::RemoveConnections(peers) => {
                for p in peers {
                    if let Some(addr) = p.socket_addr() {
                        self.unlink(i, node_index(addr));
                    }
                }
            }
            TopologyAdjustment::SwapConnection {
                remove,
                add_location,
            } => {
                if let Some(addr) = remove.socket_addr() {
                    self.nodes[i].pending.insert(add_location);
                    if !self.nodes[i].deferred_drops.iter().any(|(a, _)| *a == addr) {
                        self.nodes[i].deferred_drops.push((addr, now));
                    }
                }
            }
            TopologyAdjustment::NoChange => {}
        }

        // Deferred swap drops.
        self.nodes[i]
            .deferred_drops
            .retain(|(_, q)| now - *q < DEFERRED_SWAP_DROP_TTL);
        if !self.nodes[i].deferred_drops.is_empty() {
            let k = super::super::deferred_swap_drops_to_execute(
                self.nodes[i].cm.connection_count(),
                min,
                self.nodes[i].deferred_drops.len(),
            );
            let drops: Vec<_> = self.nodes[i].deferred_drops.drain(..k).collect();
            for (addr, _) in drops {
                self.unlink(i, node_index(addr));
            }
        }

        // Next tick: fast with adaptive backoff below min, else steady.
        let jitter = GlobalRng::random_range(0.8..=1.2);
        let node = &mut self.nodes[i];
        if current < min {
            if current != node.last_count {
                node.no_progress_ticks = 0;
            } else {
                node.no_progress_ticks = node.no_progress_ticks.saturating_add(1);
            }
            node.last_count = current;
            let mult = if node.no_progress_ticks <= FAST_TICK_BACKOFF_THRESHOLD {
                1.0
            } else {
                let excess = node.no_progress_ticks - FAST_TICK_BACKOFF_THRESHOLD;
                f64::from(
                    2u32.saturating_pow(excess)
                        .min((CHECK_TICK / FAST_TICK) as u32),
                )
            };
            FAST_TICK * mult * jitter
        } else {
            node.no_progress_ticks = 0;
            node.last_count = current;
            CHECK_TICK
        }
    }

    fn sample(&self) -> Sample {
        let peers = self.nodes.len() - 1;
        let degree = self.nodes[1..]
            .iter()
            .map(|x| x.cm.connection_count() as f64)
            .sum::<f64>()
            / peers as f64;
        let len = self.ring_order.len();
        let covered = (0..len)
            .filter(|k| {
                let i = self.ring_order[*k];
                if i == 0 {
                    return false;
                }
                let succ = self.nodes[self.ring_order[(k + 1) % len]].addr;
                let pred = self.nodes[self.ring_order[(k + len - 1) % len]].addr;
                let held = self.nodes[i].cm.connected_peer_addrs();
                held.contains(&succ) && held.contains(&pred)
            })
            .count();
        Sample {
            degree,
            coverage: covered as f64 / peers as f64,
        }
    }
}

/// Runs the model for `MODEL_HORIZON_SECS` of virtual time and samples it
/// every `SAMPLE_SECS`.
async fn run(seed: u64, discovery: Discovery, mean_uptime_secs: Option<f64>) -> Vec<Sample> {
    let _seed = GlobalRng::seed_guard(seed);
    let mut model = Model::new(MODEL_PEERS, discovery, mean_uptime_secs);

    // Maintenance ticks (time, node), earliest first; joins spread over 5 min.
    #[derive(PartialEq)]
    struct Ev(f64, usize);
    impl Eq for Ev {}
    impl PartialOrd for Ev {
        fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(o))
        }
    }
    impl Ord for Ev {
        fn cmp(&self, o: &Self) -> std::cmp::Ordering {
            o.0.total_cmp(&self.0).then(o.1.cmp(&self.1))
        }
    }
    let mut queue = BinaryHeap::new();
    queue.push(Ev(0.0, 0));
    for i in 1..=MODEL_PEERS {
        queue.push(Ev(300.0 * i as f64 / MODEL_PEERS as f64, i));
    }

    let mut now = 0.0_f64;
    let mut samples = Vec::new();
    let mut next_sample = SAMPLE_SECS;
    while let Some(Ev(t, i)) = queue.pop() {
        while next_sample <= t && next_sample <= MODEL_HORIZON_SECS {
            samples.push(model.sample());
            next_sample += SAMPLE_SECS;
        }
        if t > MODEL_HORIZON_SECS {
            break;
        }
        if t > now {
            tokio::time::advance(Duration::from_secs_f64(t - now)).await;
            now = t;
        }
        let interval = model.tick(i, now);
        queue.push(Ev(now + interval, i));
    }
    samples
}

/// Runs [`run`] on its own thread with a paused-clock current-thread runtime,
/// so arms proceed in parallel without sharing thread-local state.
fn spawn_arm(
    seed: u64,
    discovery: Discovery,
    mean_uptime_secs: Option<f64>,
) -> std::thread::JoinHandle<Vec<Sample>> {
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .expect("runtime")
            .block_on(run(seed, discovery, mean_uptime_secs))
    })
}

const MODEL_PEERS: usize = 1000;
const MODEL_HORIZON_SECS: f64 = 3.0 * 3600.0;
const SAMPLE_SECS: f64 = 1800.0;
const MODEL_SEED: u64 = 0x5814;

/// Peers restart (drop every link and rejoin) with exponentially distributed
/// uptimes of this mean, so the lattice must also be repaired, not just built.
const MEAN_UPTIME_SECS: f64 = 4.0 * 3600.0;

/// #5814 regression: at production limits (min 25 / max 200) on a lightly
/// loaded network with peer churn and failing connections, mean degree must
/// stay near the low-usage band (2 * min = 50) instead of climbing with
/// uptime, and the lattice must stay nearly as complete as under the old
/// always-on discovery. The old arm runs on the same seed and must reproduce
/// the creep, which shows the model can see it.
#[test]
fn lattice_probe_does_not_grow_degree_with_uptime() {
    let old = spawn_arm(MODEL_SEED, Discovery::Continuous, Some(MEAN_UPTIME_SECS));
    let new = spawn_arm(MODEL_SEED, Discovery::Production, Some(MEAN_UPTIME_SECS));
    let old = old.join().expect("old arm panicked");
    let new = new.join().expect("new arm panicked");
    let degree = |s: &[Sample]| s.iter().map(|x| x.degree).collect::<Vec<_>>();
    let (old_degree, new_degree) = (degree(&old), degree(&new));
    // Lattice coverage, averaged after bootstrap (the first sample).
    let coverage =
        |s: &[Sample]| s[1..].iter().map(|x| x.coverage).sum::<f64>() / (s.len() - 1) as f64;
    let (old_coverage, new_coverage) = (coverage(&old), coverage(&new));
    eprintln!("old discovery, mean degree every 30 min: {old_degree:.1?}");
    eprintln!("new discovery, mean degree every 30 min: {new_degree:.1?}");
    eprintln!("mean lattice coverage: old {old_coverage:.3}, new {new_coverage:.3}");

    let band = (2 * MIN_CONNECTIONS) as f64;
    // Growth after bootstrap has settled (from the second sample to the end).
    let growth = |s: &[f64]| s[s.len() - 1] - s[1];
    let (old_end, new_end) = (
        old_degree[old_degree.len() - 1],
        new_degree[new_degree.len() - 1],
    );
    assert!(
        old_end > band * 1.5,
        "the old arm must reproduce the creep (model sensitivity): {old_degree:.1?}"
    );
    assert!(
        new_end < band * 1.3,
        "mean degree crept to {new_end:.1}, low-usage band is {band}: {new_degree:.1?}"
    );
    assert!(
        growth(&new_degree) < growth(&old_degree) / 3.0,
        "degree still climbs with uptime: new {new_degree:.1?}, old {old_degree:.1?}"
    );
    // Sleeping must not cost lattice coverage at this failure rate: measured
    // -0.1 to +0.3 points against the old discovery across seeds (at a 60%
    // failure rate, -1.2 to -3.4). Turning the probe off costs ~3 points.
    assert!(
        new_coverage > 0.96 && new_coverage >= old_coverage - 0.015,
        "lattice coverage fell: new {new_coverage:.3}, old {old_coverage:.3}"
    );
}
