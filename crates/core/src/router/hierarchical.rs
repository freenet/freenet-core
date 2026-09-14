//! Hierarchical empirical-Bayes routing estimator (#4485), run in shadow.
//!
//! # Why this exists
//!
//! The legacy prediction stack composes three independently-motivated pieces:
//! a global isotonic distance curve fitted over the last 500 events, a per-peer
//! EWMA offset (alpha 0.1), and a Renegade k-NN prediction blended in with a
//! fixed weight that ramps to 50%. None of the weights is derived from how much
//! evidence stands behind each piece, so a peer seen twice is corrected as
//! confidently as a peer seen two thousand times, and the 50% blend applies
//! whether Renegade is right or wrong for this network.
//!
//! This estimator replaces all three with one model whose every weight is
//! estimated from the data. It was selected on a synthetic bake-off
//! (`exp/estimator-bakeoff`, commit e26a92c1d, estimator "H* on EB-shrunk long
//! curve"): the only candidate that was not materially worse than legacy in any
//! of 24 scenarios on both an original and a held-out confirmation seed set,
//! and materially better in 21 of them. That evidence is synthetic; this module
//! is shadow code so the same comparison can be made on real traffic before
//! anything acts on it. Routing uses it only under `FREENET_ROUTING_HIERARCHICAL`.
//!
//! # The model, per stage
//!
//! Each stage models one target on its own scale: failure as a probability
//! (additive, clamped to `[0, 1]`), response time and transfer speed in natural
//! log (so a composed prediction is multiplicative in seconds or bytes/s).
//!
//! 1. **Prior curve.** Isotonic (PAV) fit of the target on ring distance over
//!    the last [`WINDOW_EVENTS`] events, refit every [`REFIT_EVERY`] events.
//!    Each PAV block is then shrunk toward the pooled mean by
//!    `B = tau2 / (tau2 + s2 / w_block)` (method of moments for `tau2`, pooled
//!    residual variance for `s2`) and PAV is re-run so the result stays
//!    monotone. Without the shrinkage a block resting on five events reads one
//!    failure as 20%.
//! 2. **Residuals against the CURRENT curve.** Every refit re-derives
//!    `r = y - g(d)` for the whole window. Residuals stored against the curve as
//!    it stood when each event arrived model a curve that no longer exists.
//! 3. **Hierarchy root -> peer -> (peer, contract band).** Each node holds
//!    exponentially-forgotten `(n, sum r, sum r^2)`. Variance components come
//!    from method of moments at refit; a prediction descends the levels with a
//!    normal-normal update (`B = P / (P + V)`), so a node with little evidence
//!    contributes little and a missing node passes its level's variance down.
//! 4. **Forgetting horizon chosen online.** One hierarchy per horizon in
//!    [`HORIZONS_HOURS`]; the one with the lowest decayed prequential squared
//!    loss predicts. Every horizon is scored before the event is learned.
//!
//! # Where production differs from the reference, and why
//!
//! - **Time is wall-clock hours, not event count.** The reference assumed 60
//!   events per hour, so its 1.5h horizon was 90 events. Production event rates
//!   span orders of magnitude, and what the horizons exist to track (a peer
//!   degrading, a region of the ring going bad) happens in wall-clock time. The
//!   horizon is chosen by prequential loss, so a horizon that is wrong for a
//!   node's rate is simply not selected. On a busy node the window, not the
//!   horizon, is what bounds memory: 10k events at 10k/h is one hour, so every
//!   horizon then sees about the same data, which is harmless. On a quiet node
//!   (tens of events per hour) the horizons behave as the reference's did.
//! - **Variance components are frozen between refits.** The reference
//!   recomputed them for every prediction, which is `O(nodes)` per candidate.
//!   Here they are recomputed at refit, i.e. at most [`REFIT_EVERY`] events
//!   stale, exactly as stale as the curve they describe. Node counts and means
//!   stay live, so a new peer's evidence is used from its first event.
//! - **Decay is stored epoch-scaled.** Sums are kept as
//!   `sum_i exp((t_i - epoch)/h) x_i`, so reading a node at `now` is one
//!   multiplication and an add needs no per-node timestamp. The same algebra
//!   keeps the squared child counts the variance formulas need as running sums.
//!   Each refit rebases the epoch to `now`; an add that would push the scale
//!   past [`REBASE_EXPONENT`] forces a refit first.
//! - **No attribute level.** The router holds no cheap per-peer attribute at
//!   `add_event` time (connection age, version and gateway status live behind
//!   connection-manager locks). The level is skipped, which the reference's
//!   `attribute: false` configuration also supports. Future work: feed one in
//!   once the routing dataset shows which attribute carries signal.
//! - **The log-scale curves are not floored at zero.** The reference clamped
//!   every curve value at 0 because its timing target was log-milliseconds,
//!   which is non-negative in practice. Production records seconds and bytes/s,
//!   whose logs are routinely negative, so only the failure stage is clamped.
//! - **Peers are bounded.** Peers churn, so the peer table is capped at
//!   [`MAX_PEERS`] with batched least-recently-used eviction (entries are
//!   refreshed by every event, so refusing newcomers would starve them — see
//!   `.claude/rules/code-style.md`). Evictions are counted. An evicted peer's
//!   events still inform the curve and the root; they no longer form a peer node.
//!
//! # Cost
//!
//! A prediction is one hash lookup, one binary search over the curve's blocks
//! and a constant amount of arithmetic for the selected horizon. A refit is
//! linear in the window: the window is kept sorted by distance with an in-place
//! merge of the events since the last refit, so PAV never sorts, and the
//! hierarchy rebuild touches each event once per horizon with no hashing.

use std::collections::HashMap;
use std::hash::Hash;

use super::routing_predictor::{RoutingOutcome, wall_clock_hours};
use crate::ring::{Location, PeerKeyLocation};

/// Events the prior curve and the hierarchy are fitted over.
pub(crate) const WINDOW_EVENTS: usize = 10_000;

/// Learned events between refits: the legacy isotonic estimator's own refit
/// cadence at saturation, kept from the reference.
pub(crate) const REFIT_EVERY: usize = 50;

/// Below this many windowed events every event triggers a refit, so a cold
/// stage acquires a curve immediately. Refits are trivially cheap at this size.
const EAGER_REFIT_BELOW: usize = 100;

/// A curve needs at least this many points before it is worth fitting.
const MIN_CURVE_POINTS: usize = 5;

/// Contract-location bands per peer: `band = floor(8 * contract_location)`.
pub(crate) const BANDS: usize = 8;
const _: () = assert!(BANDS.is_power_of_two(), "band masking needs a power of two");

/// Forgetting horizons, in hours. `None` forgets nothing inside the window.
/// Powers of four down from 24h, as in the reference.
pub(crate) const HORIZONS_HOURS: [Option<f64>; 4] = [None, Some(24.0), Some(6.0), Some(1.5)];

/// Forgetting of the horizon selector's accumulated loss, in hours.
const SELECTOR_FORGETTING_HOURS: f64 = 24.0;

/// Peers tracked per stage. Production nodes run `max_connections = 200`;
/// this leaves room for churn inside one window without evicting live peers.
pub(crate) const MAX_PEERS: usize = 512;

/// Largest decay exponent an epoch-scaled weight may carry before a refit
/// rebases it. `e^30` keeps every weight far from overflow and precision loss.
const REBASE_EXPONENT: f64 = 30.0;

/// Below this effective count a node carries no usable evidence. Guards the
/// variance formulas against a count that decay has underflowed toward zero.
const MIN_EFFECTIVE_COUNT: f64 = 1e-12;

/// Floor on the pooled within-cell variance, as in the reference.
const MIN_SIGMA2: f64 = 1e-9;

// ---------------------------------------------------------------------------
// Targets
// ---------------------------------------------------------------------------

/// What a stage predicts, which fixes its curve direction and output range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Target {
    /// Failure probability. Rises with distance; clamped to `[0, 1]`.
    Failure,
    /// Natural log of time to response start in seconds. Rises with distance.
    LogResponseTime,
    /// Natural log of transfer speed in bytes/s. Falls with distance.
    LogTransferSpeed,
}

impl Target {
    fn ascending(self) -> bool {
        match self {
            Target::Failure | Target::LogResponseTime => true,
            Target::LogTransferSpeed => false,
        }
    }

    /// Map a composed value onto the target's range.
    fn finish(self, value: f64) -> f64 {
        match self {
            Target::Failure => value.clamp(0.0, 1.0),
            Target::LogResponseTime | Target::LogTransferSpeed => value,
        }
    }
}

// ---------------------------------------------------------------------------
// Prior curve
// ---------------------------------------------------------------------------

/// A weighted point, or a PAV block of them.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Block {
    x: f64,
    y: f64,
    w: f64,
}

/// A monotone piecewise-linear curve.
///
/// Built and read with exactly the semantics of `pav_regression` 0.7, which the
/// reference used: equal-`x` points are ordered by descending `y` so they pool,
/// a violating neighbour is pooled into the incoming point, and queries outside
/// the block range extrapolate along the line through the end block and the
/// centroid of the input. Reimplemented rather than called because the crate
/// re-sorts its input on every fit, and the window here is already sorted;
/// `curve_matches_pav_regression` pins the equivalence.
#[derive(Debug, Clone)]
struct Curve {
    /// Blocks in ascending `x`.
    blocks: Vec<Block>,
    /// Weighted centroid of the input points.
    centroid: (f64, f64),
}

impl Curve {
    /// PAV over points already sorted by `(x asc, y desc)`.
    fn pav(points: impl Iterator<Item = Block>, ascending: bool) -> Option<Curve> {
        let mut blocks: Vec<Block> = Vec::new();
        let (mut sum_x, mut sum_y, mut sum_w) = (0.0, 0.0, 0.0);
        for point in points {
            sum_x += point.x * point.w;
            sum_y += point.y * point.w;
            sum_w += point.w;
            let mut point = point;
            while let Some(&last) = blocks.last() {
                let violates = if ascending {
                    last.y >= point.y
                } else {
                    last.y <= point.y
                };
                if !violates {
                    break;
                }
                blocks.pop();
                let total = point.w + last.w;
                point = Block {
                    x: (point.x * point.w + last.x * last.w) / total,
                    y: (point.y * point.w + last.y * last.w) / total,
                    w: total,
                };
            }
            blocks.push(point);
        }
        if blocks.is_empty() || sum_w <= 0.0 {
            return None;
        }
        Some(Curve {
            blocks,
            centroid: (sum_x / sum_w, sum_y / sum_w),
        })
    }

    /// The reference's EB-shrunk curve over `window`, sorted by `(x, -y)`.
    fn fit_shrunk(window: &[Event], ascending: bool) -> Option<Curve> {
        if window.len() < MIN_CURVE_POINTS {
            return None;
        }
        let fit = Curve::pav(
            window.iter().map(|event| Block {
                x: event.distance,
                y: event.y,
                w: 1.0,
            }),
            ascending,
        )?;
        let blocks = &fit.blocks;
        let total: f64 = blocks.iter().map(|block| block.w).sum();
        if blocks.len() < 2 || total <= 0.0 {
            return Some(fit);
        }
        let grand = blocks.iter().map(|block| block.w * block.y).sum::<f64>() / total;
        let (mut ss, mut sw) = (0.0, 0.0);
        let mut cursor = 0;
        for event in window {
            if let Some(fitted) = fit.value_sorted(event.distance, &mut cursor) {
                ss += (event.y - fitted).powi(2);
                sw += 1.0;
            }
        }
        let s2 = if sw > 0.0 { ss / sw } else { 0.0 };
        let tau2 = (blocks
            .iter()
            .map(|block| (block.y - grand).powi(2) - s2 / block.w)
            .sum::<f64>()
            / blocks.len() as f64)
            .max(0.0);
        let shrunk = blocks.iter().map(|block| {
            let factor = if tau2 > 0.0 {
                tau2 / (tau2 + s2 / block.w)
            } else {
                0.0
            };
            Block {
                x: block.x,
                y: grand + factor * (block.y - grand),
                w: block.w,
            }
        });
        Curve::pav(shrunk, ascending).or(Some(fit))
    }

    fn value(&self, x: f64) -> Option<f64> {
        let mut cursor = self.blocks.partition_point(|block| block.x <= x);
        self.value_sorted(x, &mut cursor)
    }

    /// [`Self::value`] for a non-decreasing sequence of queries: `cursor` is the
    /// number of blocks at or left of the previous query (start at 0), advanced
    /// in place, so a pass over the sorted window is linear, not `n log b`.
    fn value_sorted(&self, x: f64, cursor: &mut usize) -> Option<f64> {
        if !x.is_finite() {
            return None;
        }
        let blocks = &self.blocks;
        while *cursor < blocks.len() && blocks[*cursor].x <= x {
            *cursor += 1;
        }
        let centroid = Block {
            x: self.centroid.0,
            y: self.centroid.1,
            w: 1.0,
        };
        let value = match blocks.len() {
            0 => return None,
            1 => blocks[0].y,
            len => {
                let above = *cursor;
                if above == 0 {
                    interpolate(blocks[0], centroid, x)
                } else if above == len {
                    interpolate(centroid, blocks[len - 1], x)
                } else {
                    interpolate(blocks[above - 1], blocks[above], x)
                }
            }
        };
        value.is_finite().then_some(value)
    }
}

fn interpolate(a: Block, b: Block, x: f64) -> f64 {
    a.y + (b.y - a.y) * ((x - a.x) / (b.x - a.x))
}

// ---------------------------------------------------------------------------
// Hierarchy
// ---------------------------------------------------------------------------

/// Epoch-scaled forgotten moments: true value at `now` is the stored value
/// times the level's `scale(now)`. Means are scale-free.
#[derive(Debug, Clone, Copy, Default)]
struct Moments {
    n: f64,
    sum: f64,
    sumsq: f64,
}

impl Moments {
    fn add(&mut self, weight: f64, value: f64) {
        self.n += weight;
        self.sum += weight * value;
        self.sumsq += weight * value * value;
    }

    fn mean(&self) -> f64 {
        self.sum / self.n
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct PeerNode {
    peer: Moments,
    cells: [Moments; BANDS],
    /// `sum over bands of cells[b].n^2`, epoch-scaled squared.
    sq_cells: f64,
}

/// Method-of-moments variance components, fixed at refit.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Components {
    /// Pooled within-cell variance.
    sigma2: f64,
    /// Between-peer variance of peer effects around the root.
    tau2_peer: f64,
    /// Between-cell variance of cell effects around their peer.
    tau2_cell: f64,
}

/// One hierarchy, forgetting at one horizon.
#[derive(Debug, Clone)]
struct Level {
    horizon_hours: Option<f64>,
    epoch: f64,
    root: Moments,
    /// `sum over peers of peer.n^2`.
    sq_peers: f64,
    /// `sum over all cells of cell.n^2`.
    sq_cells: f64,
    /// Indexed by peer slot.
    nodes: Vec<PeerNode>,
    components: Option<Components>,
}

impl Level {
    fn new(horizon_hours: Option<f64>) -> Level {
        Level {
            horizon_hours,
            epoch: 0.0,
            root: Moments::default(),
            sq_peers: 0.0,
            sq_cells: 0.0,
            nodes: Vec::new(),
            components: None,
        }
    }

    fn reset(&mut self, epoch: f64) {
        self.epoch = epoch;
        self.root = Moments::default();
        self.sq_peers = 0.0;
        self.sq_cells = 0.0;
        for node in &mut self.nodes {
            *node = PeerNode::default();
        }
        self.components = None;
    }

    /// Elapsed decay exponent since the epoch.
    fn exponent(&self, time: f64) -> f64 {
        self.horizon_hours
            .map_or(0.0, |hours| (time - self.epoch) / hours)
    }

    /// Epoch-scaled weight of an event at `time`.
    fn weight(&self, time: f64) -> f64 {
        self.exponent(time).exp()
    }

    /// Factor converting stored values into true values at `now`.
    fn scale(&self, now: f64) -> f64 {
        (-self.exponent(now).max(0.0)).exp()
    }

    fn add(&mut self, slot: Option<usize>, band: usize, weight: f64, residual: f64) {
        self.root.add(weight, residual);
        let Some(slot) = slot else {
            return;
        };
        if self.nodes.len() <= slot {
            self.nodes.resize(slot + 1, PeerNode::default());
        }
        let node = &mut self.nodes[slot];
        let before = node.peer.n;
        node.peer.add(weight, residual);
        self.sq_peers += 2.0 * before * weight + weight * weight;
        let cell = &mut node.cells[band];
        let before = cell.n;
        cell.add(weight, residual);
        let delta = 2.0 * before * weight + weight * weight;
        node.sq_cells += delta;
        self.sq_cells += delta;
    }

    /// Accumulate a whole prepared window into a freshly reset level. The
    /// squared-count sums are left for `recount_squares`, which recomputes them
    /// exactly anyway, so this loop does only the moment updates.
    fn rebuild(&mut self, prepared: &mut [Prepared], weighting: Weighting) {
        let mut root = Moments::default();
        let nodes = &mut self.nodes;
        for event in prepared.iter_mut() {
            let weight = match weighting {
                Weighting::Unit => 1.0,
                Weighting::PowerOfPrevious(power) => {
                    event.weight = event.weight.powi(power);
                    event.weight
                }
                Weighting::Decay { hours, now } => {
                    event.weight = ((event.time - now) / hours).exp();
                    event.weight
                }
            };
            let weighted = weight * event.residual;
            let weighted_sq = weighted * event.residual;
            root.n += weight;
            root.sum += weighted;
            root.sumsq += weighted_sq;
            if let Some(node) = nodes.get_mut(event.slot as usize) {
                node.peer.n += weight;
                node.peer.sum += weighted;
                node.peer.sumsq += weighted_sq;
                // `band < BANDS` by construction; the mask spares a bounds check.
                let cell = &mut node.cells[event.band as usize & (BANDS - 1)];
                cell.n += weight;
                cell.sum += weighted;
                cell.sumsq += weighted_sq;
            }
        }
        self.root = root;
    }

    fn evict(&mut self, slot: usize) {
        if let Some(node) = self.nodes.get_mut(slot) {
            self.sq_peers -= node.peer.n * node.peer.n;
            self.sq_cells -= node.sq_cells;
            *node = PeerNode::default();
        }
    }

    /// Recompute the squared-count sums exactly, removing any drift the
    /// incremental updates and evictions accumulated.
    fn recount_squares(&mut self) {
        self.sq_peers = 0.0;
        self.sq_cells = 0.0;
        for node in &mut self.nodes {
            self.sq_peers += node.peer.n * node.peer.n;
            node.sq_cells = node.cells.iter().map(|cell| cell.n * cell.n).sum();
            self.sq_cells += node.sq_cells;
        }
    }

    /// The reference's `snapshot` variance components, evaluated at the epoch
    /// (a refit always rebases the epoch to now, so stored counts are true).
    fn compute_components(&self) -> Option<Components> {
        let (mut ss, mut df) = (0.0, 0.0);
        for node in &self.nodes {
            for cell in &node.cells {
                if cell.n >= 2.0 {
                    ss += (cell.sumsq - cell.sum * cell.sum / cell.n).max(0.0);
                    df += cell.n - 1.0;
                }
            }
        }
        if df < 2.0 {
            return None;
        }
        let sigma2 = (ss / df).max(MIN_SIGMA2);
        let base = self.root.mean();
        if !base.is_finite() {
            return None;
        }

        let (mut acc, mut count) = (0.0, 0.0);
        for node in &self.nodes {
            for cell in &node.cells {
                if cell.n >= 2.0 {
                    acc += (cell.mean() - node.peer.mean()).powi(2)
                        - sigma2 * (1.0 / cell.n - 1.0 / node.peer.n).max(0.0);
                    count += 1.0;
                }
            }
        }
        let tau2_cell = if count > 0.0 {
            (acc / count).max(0.0)
        } else {
            0.0
        };

        let (mut acc, mut count) = (0.0, 0.0);
        for node in &self.nodes {
            let peer = node.peer;
            if peer.n < 2.0 {
                continue;
            }
            let noise = tau2_cell * node.sq_cells / (peer.n * peer.n) + sigma2 / peer.n;
            acc += (peer.mean() - base).powi(2) - noise;
            count += 1.0;
        }
        let tau2_peer = if count > 0.0 {
            (acc / count).max(0.0)
        } else {
            0.0
        };

        let components = Components {
            sigma2,
            tau2_peer,
            tau2_cell,
        };
        [sigma2, tau2_peer, tau2_cell]
            .iter()
            .all(|value| value.is_finite())
            .then_some(components)
    }

    /// Posterior mean residual at a query, descending root -> peer -> cell with
    /// the reference's normal-normal update. `0.0` with no components.
    fn residual(&self, slot: Option<usize>, band: usize, now: f64) -> f64 {
        let Some(components) = self.components else {
            return 0.0;
        };
        let scale = self.scale(now);
        // `(mean, noise)` of a node, or `None` where the reference had none.
        let step = |(mu, v): (f64, f64), node: Option<(f64, f64)>, tau2: f64| match node {
            Some((mean, noise)) if tau2 > 0.0 => {
                let prior = tau2 + v;
                let b = prior / (prior + noise);
                (mu + b * (mean - mu), b * noise)
            }
            _ => (mu, v + tau2),
        };

        let mut state = (0.0, 0.0);
        let root_n = self.root.n * scale;
        if root_n >= 2.0 {
            let n2 = self.root.n * self.root.n;
            let mean = self.root.mean();
            let noise = components.sigma2 / root_n
                + components.tau2_peer * self.sq_peers.max(0.0) / n2
                + components.tau2_cell * self.sq_cells.max(0.0) / n2;
            let tau2_root = (mean * mean - noise).max(0.0);
            state = step(state, Some((mean, noise)), tau2_root);
        }

        let node = slot.and_then(|slot| self.nodes.get(slot));
        let peer = node.and_then(|node| {
            let n = node.peer.n * scale;
            (n > MIN_EFFECTIVE_COUNT).then(|| {
                let noise = components.tau2_cell * node.sq_cells.max(0.0)
                    / (node.peer.n * node.peer.n)
                    + components.sigma2 / n;
                (node.peer.mean(), noise)
            })
        });
        state = step(state, peer, components.tau2_peer);

        let cell = node.and_then(|node| {
            let cell = node.cells[band];
            let n = cell.n * scale;
            (n > MIN_EFFECTIVE_COUNT).then(|| (cell.mean(), components.sigma2 / n))
        });
        state = step(state, cell, components.tau2_cell);

        if state.0.is_finite() { state.0 } else { 0.0 }
    }
}

// ---------------------------------------------------------------------------
// Peer table
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct Slot {
    generation: u32,
    last_used: u64,
    occupied: bool,
}

/// Peer key -> dense slot, bounded with batched LRU eviction.
#[derive(Debug, Clone)]
struct PeerTable<K> {
    index: HashMap<K, usize>,
    keys: Vec<Option<K>>,
    slots: Vec<Slot>,
    free: Vec<usize>,
    capacity: usize,
    use_clock: u64,
    evictions: u64,
}

impl<K: Hash + Eq + Clone> PeerTable<K> {
    fn new(capacity: usize) -> Self {
        PeerTable {
            index: HashMap::new(),
            keys: Vec::new(),
            slots: Vec::new(),
            free: Vec::new(),
            capacity: capacity.clamp(1, u32::MAX as usize - 1),
            use_clock: 0,
            evictions: 0,
        }
    }

    fn lookup(&self, key: &K) -> Option<usize> {
        self.index.get(key).copied()
    }

    fn generation(&self, slot: usize) -> Option<u32> {
        self.slots
            .get(slot)
            .filter(|slot| slot.occupied)
            .map(|slot| slot.generation)
    }

    /// Slot for `key`, marking it used. Returns the evicted slots, which the
    /// caller must clear from every level before using the returned slot.
    fn touch(&mut self, key: &K) -> (usize, u32, Vec<usize>) {
        self.use_clock += 1;
        if let Some(&slot) = self.index.get(key) {
            self.slots[slot].last_used = self.use_clock;
            return (slot, self.slots[slot].generation, Vec::new());
        }
        let mut evicted = Vec::new();
        if self.index.len() >= self.capacity {
            evicted = self.evict_batch();
        }
        let slot = match self.free.pop() {
            Some(slot) => slot,
            None => {
                self.keys.push(None);
                self.slots.push(Slot {
                    generation: 0,
                    last_used: 0,
                    occupied: false,
                });
                self.slots.len() - 1
            }
        };
        self.keys[slot] = Some(key.clone());
        self.slots[slot].occupied = true;
        self.slots[slot].last_used = self.use_clock;
        self.index.insert(key.clone(), slot);
        (slot, self.slots[slot].generation, evicted)
    }

    /// Evict the least-recently-used `capacity / 64` (at least one) peers.
    ///
    /// A batch, not one: the scan is linear in the capacity, and on a node whose
    /// churn keeps the table full, one eviction per new peer would be a scan
    /// per event.
    fn evict_batch(&mut self) -> Vec<usize> {
        let batch = (self.capacity / 64).max(1);
        let mut occupied: Vec<(u64, usize)> = self
            .slots
            .iter()
            .enumerate()
            .filter(|(_, slot)| slot.occupied)
            .map(|(index, slot)| (slot.last_used, index))
            .collect();
        let batch = batch.min(occupied.len());
        if batch == 0 {
            return Vec::new();
        }
        if batch < occupied.len() {
            occupied.select_nth_unstable(batch - 1);
        }
        let victims: Vec<usize> = occupied[..batch].iter().map(|&(_, slot)| slot).collect();
        for &slot in &victims {
            if let Some(key) = self.keys[slot].take() {
                self.index.remove(&key);
            }
            let entry = &mut self.slots[slot];
            entry.occupied = false;
            entry.generation = entry.generation.wrapping_add(1);
            self.free.push(slot);
        }
        self.evictions += victims.len() as u64;
        victims
    }
}

// ---------------------------------------------------------------------------
// Stage
// ---------------------------------------------------------------------------

/// One learned event.
#[derive(Debug, Clone, Copy)]
struct Event {
    distance: f64,
    y: f64,
    time: f64,
    seq: u64,
    slot: u32,
    generation: u32,
    band: u8,
}

/// Window order: ascending distance, then descending `y` so equal-distance
/// points pool in PAV exactly as `pav_regression` pools them.
fn window_order(a: &Event, b: &Event) -> std::cmp::Ordering {
    a.distance
        .total_cmp(&b.distance)
        .then_with(|| b.y.total_cmp(&a.y))
}

fn band_of(contract_location: f64) -> usize {
    if !contract_location.is_finite() {
        return 0;
    }
    ((contract_location * BANDS as f64).floor().max(0.0) as usize).min(BANDS - 1)
}

/// A windowed event reduced to what a hierarchy rebuild needs.
#[derive(Debug, Clone, Copy)]
struct Prepared {
    residual: f64,
    time: f64,
    /// Scratch: the epoch-scaled weight at the level being rebuilt.
    weight: f64,
    /// `u32::MAX` when the event's peer has since been evicted. Never a valid
    /// index: slots are bounded by the peer table's capacity.
    slot: u32,
    band: u8,
}

/// How a level weights prepared events during a rebuild.
#[derive(Debug, Clone, Copy)]
enum Weighting {
    /// No forgetting.
    Unit,
    /// The previous level's weight raised to this power.
    PowerOfPrevious(i32),
    Decay {
        hours: f64,
        now: f64,
    },
}

/// Exact small-integer ratio of two decay rates, if there is one, so a faster
/// horizon's weight is a power of a slower one's instead of another `exp`.
fn integer_rate_ratio(slower_hours: f64, faster_hours: f64) -> Option<i32> {
    let ratio = slower_hours / faster_hours;
    let rounded = ratio.round();
    ((ratio - rounded).abs() < 1e-9 && (2.0..=16.0).contains(&rounded)).then_some(rounded as i32)
}

/// Read-only state of a stage, for the dashboard.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct StageDiagnostics {
    pub window_events: usize,
    pub peers: usize,
    pub peer_evictions: u64,
    pub refits: u64,
    /// Inputs refused as non-finite or out of range, never learned.
    pub rejected: u64,
    /// Forgetting horizon currently predicting; `None` forgets nothing.
    pub selected_horizon_hours: Option<f64>,
    /// Whether a curve exists, i.e. whether the stage predicts at all.
    pub active: bool,
    /// Windowed events at the last refit whose peer had been evicted, so they
    /// informed the curve and root but no peer node.
    pub orphaned_at_last_refit: usize,
}

/// One target's estimator.
#[derive(Debug, Clone)]
pub(crate) struct Stage<K> {
    target: Target,
    window_capacity: usize,
    /// Events as of the last refit, in [`window_order`].
    sorted: Vec<Event>,
    /// Events learned since the last refit, in arrival order.
    fresh: Vec<Event>,
    next_seq: u64,
    since_refit: usize,
    curve: Option<Curve>,
    peers: PeerTable<K>,
    levels: Vec<Level>,
    /// Decayed prequential squared loss per horizon.
    loss: Vec<f64>,
    loss_time: Option<f64>,
    /// Latest time seen. Time never runs backwards inside a stage: a wall clock
    /// that steps back is read as "no time has passed".
    clock: f64,
    refits: u64,
    rejected: u64,
    orphaned_at_last_refit: usize,
    /// Reused rebuild buffer, so a refit allocates nothing in steady state.
    prepared: Vec<Prepared>,
}

impl<K: Hash + Eq + Clone> Stage<K> {
    pub(crate) fn new(target: Target) -> Self {
        Self::with_limits(target, WINDOW_EVENTS, MAX_PEERS)
    }

    pub(crate) fn with_limits(target: Target, window_capacity: usize, max_peers: usize) -> Self {
        Stage {
            target,
            window_capacity: window_capacity.max(MIN_CURVE_POINTS),
            sorted: Vec::new(),
            fresh: Vec::new(),
            next_seq: 0,
            since_refit: 0,
            curve: None,
            peers: PeerTable::new(max_peers),
            levels: HORIZONS_HOURS.iter().map(|&h| Level::new(h)).collect(),
            loss: vec![0.0; HORIZONS_HOURS.len()],
            loss_time: None,
            clock: f64::NEG_INFINITY,
            refits: 0,
            rejected: 0,
            orphaned_at_last_refit: 0,
            prepared: Vec::new(),
        }
    }

    fn effective_time(&self, now: f64) -> f64 {
        if now.is_finite() {
            now.max(self.clock)
        } else if self.clock.is_finite() {
            self.clock
        } else {
            0.0
        }
    }

    /// Index of the horizon currently predicting: strict argmin of loss, seeded
    /// at the no-forgetting horizon so ties keep it. Every horizon's loss is
    /// decayed by the same factor, so comparing stored sums is exact.
    fn selected(&self) -> usize {
        let mut best = 0;
        for index in 1..self.loss.len() {
            if self.loss[index] < self.loss[best] {
                best = index;
            }
        }
        best
    }

    /// Prior curve value and per-horizon residuals at a query, before
    /// finishing. `None` without a curve.
    fn components_at(
        &self,
        peer: &K,
        contract_location: f64,
        distance: f64,
        now: f64,
    ) -> Option<(f64, Vec<f64>)> {
        let prior = self.target.finish(self.curve.as_ref()?.value(distance)?);
        let slot = self.peers.lookup(peer);
        let band = band_of(contract_location);
        let residuals = self
            .levels
            .iter()
            .map(|level| level.residual(slot, band, now))
            .collect();
        Some((prior, residuals))
    }

    /// Prediction on the target's scale (probability, or log seconds / log
    /// bytes per second). `None` until the stage has a curve.
    ///
    /// `O(1)` in the window: one hash lookup, one binary search over the
    /// curve's blocks, and the selected horizon's arithmetic.
    pub(crate) fn predict(
        &self,
        peer: &K,
        contract_location: f64,
        distance: f64,
        now: f64,
    ) -> Option<f64> {
        let now = self.effective_time(now);
        let prior = self.target.finish(self.curve.as_ref()?.value(distance)?);
        let slot = self.peers.lookup(peer);
        let band = band_of(contract_location);
        let residual = self.levels[self.selected()].residual(slot, band, now);
        let value = self.target.finish(prior + residual);
        value.is_finite().then_some(value)
    }

    /// Learn one outcome, returning the forecast made for it BEFORE learning
    /// it. Every horizon is scored on that same pre-learning state.
    pub(crate) fn observe(
        &mut self,
        peer: &K,
        contract_location: f64,
        distance: f64,
        y: f64,
        now: f64,
    ) -> Option<f64> {
        let valid_y = match self.target {
            Target::Failure => (0.0..=1.0).contains(&y),
            Target::LogResponseTime | Target::LogTransferSpeed => y.is_finite(),
        };
        if !valid_y || !distance.is_finite() {
            self.rejected += 1;
            return None;
        }
        let now = self.effective_time(now);
        self.clock = now;

        let mut forecast = None;
        let mut prior_used = None;
        if let Some((prior, residuals)) = self.components_at(peer, contract_location, distance, now)
        {
            let selected = residuals[self.selected()];
            let value = self.target.finish(prior + selected);
            forecast = value.is_finite().then_some(value);
            self.score(&residuals, y - prior, now);
            prior_used = Some(prior);
        }

        let (slot, generation, evicted) = self.peers.touch(peer);
        for victim in evicted {
            for level in &mut self.levels {
                level.evict(victim);
            }
        }
        let band = band_of(contract_location);
        let event = Event {
            distance,
            y,
            time: now,
            seq: self.next_seq,
            slot: slot as u32,
            generation,
            band: band as u8,
        };
        self.next_seq += 1;
        self.fresh.push(event);
        self.since_refit += 1;

        let mut must_rebase = false;
        if let Some(prior) = prior_used {
            for level in &mut self.levels {
                if level.exponent(now) > REBASE_EXPONENT {
                    must_rebase = true;
                    break;
                }
            }
            if !must_rebase {
                for level in &mut self.levels {
                    let weight = level.weight(now);
                    level.add(Some(slot), band, weight, y - prior);
                }
            }
        }

        let windowed = self.sorted.len() + self.fresh.len();
        if windowed < EAGER_REFIT_BELOW || self.since_refit >= REFIT_EVERY || must_rebase {
            self.refit(now);
        }
        forecast
    }

    fn score(&mut self, residuals: &[f64], actual_residual: f64, now: f64) {
        let factor = self.loss_time.map_or(1.0, |then| {
            (-(now - then).max(0.0) / SELECTOR_FORGETTING_HOURS).exp()
        });
        for (loss, predicted) in self.loss.iter_mut().zip(residuals) {
            let error = (predicted - actual_residual).powi(2);
            if error.is_finite() {
                *loss = *loss * factor + error;
            } else {
                *loss *= factor;
            }
        }
        self.loss_time = Some(now);
    }

    /// Merge fresh events into the sorted window, drop expired ones, refit the
    /// curve, and rebuild every hierarchy against it. Linear in the window.
    fn refit(&mut self, now: f64) {
        self.refits += 1;
        self.since_refit = 0;
        self.merge_fresh();
        if let Some(curve) = Curve::fit_shrunk(&self.sorted, self.target.ascending()) {
            self.curve = Some(curve);
        }
        for level in &mut self.levels {
            level.reset(now);
        }
        if self.prepare() {
            self.rebuild_levels(now);
        }
    }

    /// Fold events learned since the last refit into the sorted window and drop
    /// events that have left it.
    fn merge_fresh(&mut self) {
        let oldest_live = self.next_seq.saturating_sub(self.window_capacity as u64);
        self.sorted.retain(|event| event.seq >= oldest_live);
        self.fresh.retain(|event| event.seq >= oldest_live);
        self.fresh.sort_unstable_by(window_order);
        // In-place merge from the back: no second window-sized buffer.
        let existing = self.sorted.len();
        let incoming = self.fresh.len();
        if incoming > 0 {
            let filler = self.fresh[0];
            self.sorted.resize(existing + incoming, filler);
            let (mut i, mut j, mut write) = (existing, incoming, existing + incoming);
            while j > 0 {
                if i > 0 && window_order(&self.sorted[i - 1], &self.fresh[j - 1]).is_gt() {
                    self.sorted[write - 1] = self.sorted[i - 1];
                    i -= 1;
                } else {
                    self.sorted[write - 1] = self.fresh[j - 1];
                    j -= 1;
                }
                write -= 1;
            }
            self.fresh.clear();
        }
    }

    /// Re-anchor every windowed residual on the current curve. `false` without
    /// a curve.
    fn prepare(&mut self) -> bool {
        let Some(curve) = self.curve.as_ref() else {
            return false;
        };
        // One pass to re-anchor every residual on the new curve (linear, since
        // the window is sorted by distance), then one tight pass per level.
        // Level-at-a-time keeps each level's node table hot in cache, and lets
        // a faster horizon's weight be an integer power of the slower one's
        // (24h -> 6h -> 1.5h are powers of four) rather than a fresh `exp`.
        let mut orphaned = 0;
        let mut cursor = 0;
        self.prepared.clear();
        for event in &self.sorted {
            let Some(value) = curve.value_sorted(event.distance, &mut cursor) else {
                continue;
            };
            let live = self.peers.generation(event.slot as usize) == Some(event.generation);
            if !live {
                orphaned += 1;
            }
            self.prepared.push(Prepared {
                residual: event.y - self.target.finish(value),
                time: event.time,
                weight: 1.0,
                slot: if live { event.slot } else { u32::MAX },
                band: event.band,
            });
        }
        self.orphaned_at_last_refit = orphaned;

        true
    }

    /// Accumulate the prepared window into every (already reset) level and
    /// recompute its variance components.
    fn rebuild_levels(&mut self, now: f64) {
        let slots = self.peers.slots.len();
        let mut previous_hours: Option<f64> = None;
        for level in &mut self.levels {
            if level.nodes.len() < slots {
                level.nodes.resize(slots, PeerNode::default());
            }
            let weighting = match level.horizon_hours {
                None => Weighting::Unit,
                Some(hours) => {
                    let weighting =
                        match previous_hours.and_then(|slower| integer_rate_ratio(slower, hours)) {
                            Some(power) => Weighting::PowerOfPrevious(power),
                            None => Weighting::Decay { hours, now },
                        };
                    previous_hours = Some(hours);
                    weighting
                }
            };
            level.rebuild(&mut self.prepared, weighting);
        }
        for level in &mut self.levels {
            level.recount_squares();
            level.components = level.compute_components();
        }
    }

    pub(crate) fn diagnostics(&self) -> StageDiagnostics {
        StageDiagnostics {
            window_events: self.sorted.len() + self.fresh.len(),
            peers: self.peers.index.len(),
            peer_evictions: self.peers.evictions,
            refits: self.refits,
            rejected: self.rejected,
            selected_horizon_hours: HORIZONS_HOURS[self.selected()],
            active: self.curve.is_some(),
            orphaned_at_last_refit: self.orphaned_at_last_refit,
        }
    }
}

// ---------------------------------------------------------------------------
// The router's three stages
// ---------------------------------------------------------------------------

/// What the estimator forecast for an event before learning it.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct Observed {
    pub failure: Option<f64>,
    /// Forecast of `ln(seconds)` to response start, made for every event
    /// whether or not it turned out to be timed, so timing can be evaluated
    /// offline against the timed subset.
    pub log_response_time: Option<f64>,
}

/// A routing estimate on the router's own units.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct Estimate {
    pub failure_probability: Option<f64>,
    pub time_to_response_start_secs: Option<f64>,
    pub transfer_speed_bps: Option<f64>,
}

/// The estimator for all three router stages, on a shared relative clock.
#[derive(Clone)]
pub(crate) struct HierarchicalRouting {
    failure: Stage<PeerKeyLocation>,
    response_time: Stage<PeerKeyLocation>,
    transfer_speed: Stage<PeerKeyLocation>,
    /// Wall-clock hours at construction; times are relative to this, following
    /// the Renegade predictor's convention.
    reference_time_hours: f64,
}

impl std::fmt::Debug for HierarchicalRouting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HierarchicalRouting")
            .field("failure", &self.failure.diagnostics())
            .field("response_time", &self.response_time.diagnostics())
            .field("transfer_speed", &self.transfer_speed.diagnostics())
            .finish()
    }
}

impl HierarchicalRouting {
    pub(crate) fn new() -> Self {
        HierarchicalRouting {
            failure: Stage::new(Target::Failure),
            response_time: Stage::new(Target::LogResponseTime),
            transfer_speed: Stage::new(Target::LogTransferSpeed),
            reference_time_hours: wall_clock_hours(),
        }
    }

    /// Estimator time for a wall-clock reading in hours.
    pub(crate) fn time_at(&self, wall_clock_hours: f64) -> f64 {
        wall_clock_hours - self.reference_time_hours
    }

    /// Learn one routing outcome, returning the forecasts made before it.
    pub(crate) fn observe_at(
        &mut self,
        peer: &PeerKeyLocation,
        contract_location: Location,
        distance: f64,
        outcome: &RoutingOutcome,
        time: f64,
    ) -> Observed {
        let contract = contract_location.as_f64();
        let log_response_time = self.response_time.predict(peer, contract, distance, time);
        let failure = self.failure.observe(
            peer,
            contract,
            distance,
            if outcome.success { 0.0 } else { 1.0 },
            time,
        );
        if let Some(seconds) = outcome.time_to_response_start_secs {
            // `ln` of zero or a negative is not a learnable time; `observe`
            // counts the non-finite result as rejected.
            let log = if seconds > 0.0 {
                seconds.ln()
            } else {
                f64::NAN
            };
            self.response_time
                .observe(peer, contract, distance, log, time);
        }
        if let Some(speed) = outcome.transfer_speed_bps {
            let log = if speed > 0.0 { speed.ln() } else { f64::NAN };
            self.transfer_speed
                .observe(peer, contract, distance, log, time);
        }
        Observed {
            failure,
            log_response_time,
        }
    }

    pub(crate) fn estimate_at(
        &self,
        peer: &PeerKeyLocation,
        contract_location: Location,
        distance: f64,
        time: f64,
    ) -> Estimate {
        let contract = contract_location.as_f64();
        Estimate {
            failure_probability: self.failure.predict(peer, contract, distance, time),
            time_to_response_start_secs: self
                .response_time
                .predict(peer, contract, distance, time)
                .map(f64::exp)
                .filter(|seconds| seconds.is_finite()),
            transfer_speed_bps: self
                .transfer_speed
                .predict(peer, contract, distance, time)
                .map(f64::exp)
                .filter(|speed| speed.is_finite()),
        }
    }

    pub(crate) fn diagnostics(&self) -> [StageDiagnostics; 3] {
        [
            self.failure.diagnostics(),
            self.response_time.diagnostics(),
            self.transfer_speed.diagnostics(),
        ]
    }
}

#[cfg(test)]
mod tests;
