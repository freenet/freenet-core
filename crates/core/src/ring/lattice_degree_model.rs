//! Large-N topology model guarding peer degree against uptime creep (#5814).
//!
//! Every model node owns a real `ConnectionManager` (and through it a real
//! `TopologyManager`), so acceptance (`should_accept`), targets, swaps and
//! pruning (`adjust_topology`), the add/concurrency budgets and the lattice
//! probe's keep/drop decision (`keeps_lattice_probe_acceptor`) are production
//! code. Only the plumbing is modelled: the CONNECT walk (greedy toward the target over
//! the model graph with the joiner's connections pre-excluded, near-terminus
//! probabilistic acceptance, terminus acceptance, bounded uphill retry),
//! instant connection establishment, and the maintenance-loop scheduling
//! (production tick, backoff and lattice-probe cadence constants). Not
//! modelled: transport, NAT, failures, traffic (the bandwidth meter stays empty,
//! i.e. a lightly loaded network, which is where nothing prunes below
//! `max_connections`), location backoff and router learning.
//!
//! The production route-to-self probe lands on the nearest UNCONNECTED peer
//! every few minutes forever. Before #5814 every acceptor was kept, so each
//! probe that found nothing closer still added a link and degree climbed with
//! uptime. The test runs the same seed with and without the keep/drop gate:
//! the ungated arm must reproduce the creep (so the model is sensitive to it)
//! and the gated arm must stay in the low-usage band.

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
const LATTICE_TAU0: Duration = Duration::from_secs(5);
const LATTICE_TAU_MAX: Duration = Duration::from_secs(300);
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
    probe_next_at: f64,
    probe_attempt: u32,
    probe_last: Option<super::super::LatticeSides>,
}

struct Model {
    nodes: Vec<Node>,
    key: TransportPublicKey,
    /// Whether a lattice probe's acceptors go through the production keep/drop
    /// gate. `false` reproduces the pre-#5814 behavior (keep every acceptor).
    probe_gate: bool,
}

impl Model {
    fn new(n: usize, probe_gate: bool) -> Self {
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
            let cm = ConnectionManager::init(
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
                nodes.is_empty(), // node 0 is the gateway
                10,
                Duration::from_secs(60),
                0,
            );
            nodes.push(Node {
                cm,
                addr,
                loc,
                pending: BTreeSet::new(),
                deferred_drops: Vec::new(),
                last_count: 0,
                no_progress_ticks: 0,
                probe_next_at: 0.0,
                probe_attempt: 0,
                probe_last: None,
            });
        }
        Self {
            nodes,
            key,
            probe_gate,
        }
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

    /// Relay `r` considers accepting joiner `j`; on acceptance the link forms,
    /// and a lattice probe's acceptor the production gate rejects is dropped
    /// again (the CONNECT driver's connect-then-drop).
    fn try_accept(&self, r: usize, j: usize, lattice_probe: bool) -> bool {
        if !self.nodes[r]
            .cm
            .should_accept(self.nodes[j].loc, self.nodes[j].addr)
        {
            return false;
        }
        self.link(r, j);
        if lattice_probe
            && self.probe_gate
            && !self.nodes[j]
                .cm
                .keeps_lattice_probe_acceptor(self.nodes[r].loc)
        {
            self.unlink(r, j);
        }
        true
    }

    /// Modelled CONNECT from `j` toward `target`, entering at `first_hop`.
    fn connect(&self, j: usize, target: Location, first_hop: usize, lattice_probe: bool) {
        let mut visited = vec![false; self.nodes.len()];
        for n in self.neighbors(j) {
            visited[n] = true;
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
                    self.try_accept(r, j, lattice_probe);
                }
                r = n;
                ttl -= 1;
                continue;
            }
            if self.try_accept(r, j, lattice_probe) || uphill == 0 || ttl < 2 {
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
            self.connect(i, self.nodes[i].loc, 0, false);
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
            // probe (ring.rs `lattice_probe_target`).
            let lattice_probe = target == self.nodes[i].loc && self.nodes[i].cm.nn_lattice_active();
            self.connect(i, target, first, lattice_probe);
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

        // Route-to-self lattice probe, production cadence (ring.rs).
        if self.nodes[i].cm.nn_lattice_active() {
            let node = &mut self.nodes[i];
            let curr = super::super::LatticeSides {
                succ: node.cm.nearest_lattice_neighbor_dist(true),
                pred: node.cm.nearest_lattice_neighbor_dist(false),
            };
            let progress = super::super::lattice_probe_progress(node.probe_last, curr);
            if progress.improved || progress.regressed {
                node.probe_attempt = 0;
                node.probe_next_at = now;
            }
            if now >= node.probe_next_at {
                node.pending.insert(node.loc);
                let backoff = ExponentialBackoff::new(LATTICE_TAU0, LATTICE_TAU_MAX);
                let jitter = GlobalRng::random_range(0.8..=1.2);
                node.probe_next_at = now + backoff.delay(node.probe_attempt).as_secs_f64() * jitter;
                node.probe_attempt = node.probe_attempt.saturating_add(1);
            }
            node.probe_last = Some(curr);
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

    /// Mean degree over the non-gateway nodes.
    fn mean_degree(&self) -> f64 {
        let peers = &self.nodes[1..];
        peers
            .iter()
            .map(|x| x.cm.connection_count() as f64)
            .sum::<f64>()
            / peers.len() as f64
    }
}

/// Runs the model for `total_secs` of virtual time and returns the mean peer
/// degree at each multiple of `sample_secs`.
async fn run(n: usize, seed: u64, total_secs: f64, sample_secs: f64, probe_gate: bool) -> Vec<f64> {
    let _seed = GlobalRng::seed_guard(seed);
    let mut model = Model::new(n, probe_gate);

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
    for i in 1..=n {
        queue.push(Ev(300.0 * i as f64 / n as f64, i));
    }

    let mut now = 0.0_f64;
    let mut samples = Vec::new();
    let mut next_sample = sample_secs;
    while let Some(Ev(t, i)) = queue.pop() {
        while next_sample <= t && next_sample <= total_secs {
            samples.push(model.mean_degree());
            next_sample += sample_secs;
        }
        if t > total_secs {
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
/// so the two arms proceed in parallel without sharing thread-local state.
fn spawn_arm(seed: u64, probe_gate: bool) -> std::thread::JoinHandle<Vec<f64>> {
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .expect("runtime")
            .block_on(run(
                MODEL_PEERS,
                seed,
                MODEL_HORIZON_SECS,
                SAMPLE_SECS,
                probe_gate,
            ))
    })
}

const MODEL_PEERS: usize = 1000;
const MODEL_HORIZON_SECS: f64 = 3.0 * 3600.0;
const SAMPLE_SECS: f64 = 1800.0;
const MODEL_SEED: u64 = 0x5814;

/// #5814 regression: at production limits (min 25 / max 200) on a lightly
/// loaded network, mean peer degree must stay near the low-usage band
/// (2 * min = 50) instead of climbing with uptime. The ungated arm (every
/// probe acceptor kept, the pre-#5814 behavior) runs on the same seed and must
/// reproduce the creep, which shows the model can see it.
#[test]
fn lattice_probe_does_not_grow_degree_with_uptime() {
    let gated = spawn_arm(MODEL_SEED, true);
    let ungated = spawn_arm(MODEL_SEED, false);
    let gated = gated.join().expect("gated arm panicked");
    let ungated = ungated.join().expect("ungated arm panicked");
    eprintln!("gated mean degree every 30 min: {gated:.1?}");
    eprintln!("ungated mean degree every 30 min: {ungated:.1?}");

    let band = (2 * MIN_CONNECTIONS) as f64;
    // Growth over the second half of the run, after bootstrap has settled.
    let late_growth = |s: &[f64]| s[s.len() - 1] - s[s.len() / 2 - 1];
    let (gated_end, ungated_end) = (gated[gated.len() - 1], ungated[ungated.len() - 1]);
    assert!(
        ungated_end > band * 1.5 && late_growth(&ungated) > 20.0,
        "the ungated arm must reproduce the creep (model sensitivity): {ungated:.1?}"
    );
    assert!(
        gated_end < band * 1.25,
        "mean degree crept to {gated_end:.1}, low-usage band is {band}: {gated:.1?}"
    );
    assert!(
        late_growth(&gated) < late_growth(&ungated) / 5.0,
        "degree still climbs with uptime: gated {gated:.1?}, ungated {ungated:.1?}"
    );
}
