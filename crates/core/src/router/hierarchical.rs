//! Hierarchical empirical-Bayes routing estimator (#4485).
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
//! This estimator replaces all of that with one model whose every weight is
//! estimated from the data. It is the intended REPLACEMENT for the legacy stack
//! (Renegade, the per-peer EWMA, the fixed blend and the residual correction),
//! which is removed in a later PR after a gateway soak. Until then it reaches
//! routing only under `FREENET_ROUTING_HIERARCHICAL`, and is computed at all only
//! when that flag is on or the routing dataset is being recorded.
//!
//! # Provenance, and what has changed since
//!
//! The structure was selected on a synthetic bake-off (`exp/estimator-bakeoff`,
//! commit e26a92c1d, "H* on EB-shrunk long curve"). That bake-off validated a
//! model WITH an attribute level, on a different traffic shape, and with the
//! reference's estimators for the curve shrinkage and the variance components.
//! A statistical review of the port found those estimators biased, and they have
//! been replaced here (see "Estimators" below).
//!
//! **The shipped formulas are the ones re-validated on
//! `exp/estimator-bakeoff` commit 390470ef1** (`recoverability::bakeoff::
//! revalidate`, row 7: no attribute level, frozen components, corrections 1-3,
//! expectation timing, standard descent). Over 53 scenarios, including
//! production-like ones (200 peers, Zipf, 90% home band, 600 events/h), its
//! worst natural-units ratio against legacy was 0.971 (confirmation seeds) and
//! 1.002 (original seeds). Neither half alone passes: the corrections with
//! median timing reach 1.44, and the uncorrected shape 1.60. The margin is
//! thin, drifted peers (`pt.drift`) and narrow targeted pairs are its weak
//! subsets, and expectation timing assumes approximately lognormal residuals,
//! which [`ResidualShape`] exposes for checking on real traffic.
//!
//! A second re-validation, **`exp/estimator-bakeoff` commit 5d86af8a2**
//! (`revalidate_live_sums_guard_and_speed{,_confirmation_seeds}` and
//! `live_root_sums_match_frozen_at_refit_and_diverge_after`), covers the parts
//! row 7 did not: live root squared-count sums (row 7b) and live sums plus the
//! leave-one-out cancellation guard (row 7c) both match row 7, worst ratio
//! 1.002 / 0.971, including quiet-node (6 events/h) and long-window runs at
//! production cadence (10k window, refit every `max(50, window/100)`). The
//! transfer-speed stage, with ties pooled by PAV direction, gave expected
//! transfer times 0.05-0.51x legacy's. Caveats that remain: the 1.002 parity
//! case, the drifted-peer subset of `pt.drift` (1.23 / 1.16), and tie
//! pooling's own effect, which was not isolated.
//!
//! Deliberate differences from that reference, each from the review brief:
//! the horizon selector scores the clamped forecast (identical for the log
//! stages; differs for failure only when a forecast clamps at 0 or 1); log
//! stages are bounded (below) and need [`MIN_CURVE_POINTS_LOG`] points; the log
//! curves are not floored at zero; and peers are bounded.
//!
//! # The model, per stage
//!
//! Each stage models one target on its own scale: failure as a probability
//! (additive, clamped to `[0, 1]`), response time and transfer speed in natural
//! log.
//!
//! 1. **Prior curve.** Isotonic (PAV) fit of the target on ring distance over
//!    the last [`WINDOW_EVENTS`] events. Each PAV block mean is shrunk toward the
//!    pooled mean and PAV is re-run over the shrunk blocks, so the result stays
//!    monotone.
//! 2. **Residuals against the CURRENT curve.** Every refit re-derives
//!    `r = y - g(d)` for the whole window. Residuals stored against the curve as
//!    it stood when each event arrived model a curve that no longer exists.
//! 3. **Hierarchy root -> peer -> (peer, contract band).** Each node holds
//!    exponentially-forgotten `(n, sum w^2, sum r, sum r^2)`. A prediction
//!    descends the levels with a normal-normal update (`B = P / (P + V)`), so a
//!    node with little evidence contributes little and a missing node passes its
//!    level's variance down.
//! 4. **Forgetting horizon chosen online.** One hierarchy per horizon in
//!    [`HORIZONS_HOURS`]; the one with the lowest decayed prequential squared
//!    loss of its FINISHED forecast predicts. Every horizon is scored before the
//!    event is learned.
//!
//! # Estimators
//!
//! - **Curve shrinkage** is the one-way random-effects method of moments for
//!   unequal group sizes, over the PAV blocks as groups: `s2` is the pooled
//!   within-block variance about the block means,
//!   `tau2 = max(0, [sum w_b (ybar_b - g)^2 - (k-1) s2] / [W - sum w_b^2 / W])`,
//!   and each block moves toward the pool by `B_b = tau2 / (tau2 + s2 / w_b)`.
//!   What this does and does not give: a block resting on a handful of events
//!   moves most of the way to the pool when the between-block variance is small
//!   relative to its own sampling noise, and a real monotone trend backed by
//!   well-populated blocks survives. It is not a guarantee about any single
//!   block; `tau2` is estimated from the same blocks it is shrinking.
//! - **Variance components** use leave-one-out contrasts: a cell is compared
//!   with the mean of its peer's OTHER cells, and a peer with the mean of every
//!   OTHER event, so a child never sits inside the mean it is compared against.
//!   The earlier child-vs-parent form was biased low whenever one child
//!   dominates its parent, which is the normal case under routing locality (a
//!   peer mostly sees contracts near its own location). A peer seen in only one
//!   band contributes nothing to `tau2_cell`: it has no other cell to contrast.
//! - **Decayed evidence is counted by Kish effective size**, `n_eff = n^2 / sum
//!   w^2`, in the sampling variance of every mean, in the degrees of freedom of
//!   `sigma2`, and in the minimum-evidence gates. Counting the raw weight sum
//!   instead overstates the evidence of a short horizon by about 2x, which drove
//!   the between-group variance estimates to zero at low event rates.
//! - **The descent uses each node's full mean**, not a leave-one-out mean. The
//!   re-validation rejected leave-one-out evidence at the descent (worst ratio
//!   1.18 on `pt.drift`); leave-one-out is used for the variance components only.
//!
//! # Timing and speed return expectations
//!
//! The router's cost formula adds seconds and divides bytes by speed, so the
//! stages return what those operations need, not medians: response time
//! `E[T] = exp(mu + (sigma2 + v)/2)` and the effective transfer speed
//! `exp(mu - (sigma2 + v)/2)`, whose reciprocal is `E[1/speed]`, so
//! `bytes / speed = bytes * E[1/speed]`. `v` is the posterior variance of the
//! peer's own mean. Keeping it in is deliberate: an unknown or rarely-seen peer
//! carries more variance, so it is priced as slower, which is the cold-peer
//! penalty the router wants rather than an optimism it would have to unlearn.
//!
//! # Where production differs from the reference, and why
//!
//! - **Time comes from the router's injected `TimeSource`**, as hours since the
//!   router was built, not event count and not the host wall clock. Ring passes
//!   its `InstantTimeSrc`, which reads tokio's clock, so horizons advance under
//!   a paused tokio runtime (the direct simulation runner); they do not follow
//!   a hosting-only time override. Router-level tests inject a mock clock. The horizon is chosen by
//!   prequential loss, so one that does not suit a node's event rate is simply
//!   not selected; on a busy node the window binds before any horizon does.
//!   A node whose count has decayed to [`NODE_MIN`] by the time of a query is
//!   treated as absent, so predictions take the query time too.
//! - **Variance components are frozen between refits**, exactly as stale as the
//!   curve they describe. Node means stay live, and so do the squared-count
//!   sums in the root's noise: row 7 froze those at refit along with the
//!   components. Live sums are consistent with the live root mean they
//!   describe, and re-validated as row 7b (5d86af8a2) with no loss.
//! - **Leave-one-out rests are summed, not subtracted.** A cell's rest is the
//!   sum of its peer's other cells, and a peer holding all but a millionth of
//!   the root is skipped, because under decay the reference's subtraction
//!   cancels to zero and biases `tau2` upward.
//! - **Equal distances pool on the descending (speed) curve too**; see
//!   `window_order`.
//! - **Decay is stored epoch-scaled.** Sums are kept as
//!   `sum_i exp((t_i - epoch)/h) x_i`; the noise terms a prediction reads are
//!   ratios in which the scale cancels. Each refit rebases the epoch to `now`;
//!   an add that would push the scale past [`REBASE_EXPONENT`] forces a refit.
//! - **No attribute level.** The router holds no cheap per-peer attribute at
//!   `add_event` time. Future work, once the routing dataset shows which one
//!   carries signal.
//! - **Log stages are bounded.** Beyond the fitted distance range a log curve
//!   holds its end value instead of extrapolating along the centroid line, and
//!   every log prediction is clamped to the window's observed range widened by
//!   [`LOG_PREDICTION_MARGIN`].
//! - **Peers are bounded** by [`peer_capacity`], derived from the configured
//!   connection cap, with batched least-recently-used eviction (entries are
//!   refreshed by every event, so refusing newcomers would starve them — see
//!   `.claude/rules/code-style.md`). Evictions are counted and exported. An
//!   evicted peer's events still inform the curve and the root.
//!
//! # Cost
//!
//! A prediction is one hash lookup, one binary search over the curve's blocks
//! and a constant amount of arithmetic for the selected horizon. A refit is
//! linear in the window, and once the window is full runs every
//! [`refit_interval`] events.

use std::collections::HashMap;
use std::hash::Hash;

use super::routing_predictor::RoutingOutcome;
use crate::ring::{Location, PeerKeyLocation};

/// Events the prior curve and the hierarchy are fitted over.
pub(crate) const WINDOW_EVENTS: usize = 10_000;

/// Learned events between refits while the window is filling: the legacy
/// isotonic estimator's own refit cadence, kept from the reference.
pub(crate) const REFIT_EVERY: usize = 50;

/// Once the window is full, refit after this share of it has turned over.
///
/// A refit is linear in the window and runs under the router's write lock, so
/// its cadence is what sets the lock-hold budget. One percent of a full window
/// changes the curve and the variance components by about one percent, which
/// is well inside their own sampling error, so refitting more often buys
/// nothing measurable.
const REFIT_TURNOVER_DIVISOR: usize = 100;

/// Below this many windowed events every event triggers a refit, so a cold
/// stage acquires a curve immediately. Refits are trivially cheap at this size.
const EAGER_REFIT_BELOW: usize = 100;

/// Minimum points before the failure curve is fitted: the legacy isotonic
/// estimator's own `MIN_POINTS_FOR_REGRESSION`. A failure curve is clamped to
/// `[0, 1]`, so a sparse one cannot produce an out-of-range estimate.
const MIN_CURVE_POINTS_FAILURE: usize = 5;

/// Minimum points before a log-scale (timing, speed) curve is fitted.
///
/// Log response times and speeds are unbounded, and a log curve's error
/// becomes a multiplicative error in the router's cost formula. At a typical
/// log-space spread of 0.5-1.0, 30 events put the standard error of a pooled
/// mean at 0.1-0.2 (a factor of about 1.1-1.2) and give the within-block
/// variance roughly 25 degrees of freedom, which is where its own relative
/// error falls below 30%. Five points, the failure floor, would allow a factor
/// of two.
const MIN_CURVE_POINTS_LOG: usize = 30;

/// Log-scale predictions are clamped to the observed target range widened by
/// this much on each side: a factor of two beyond anything the window has seen.
pub(crate) const LOG_PREDICTION_MARGIN: f64 = std::f64::consts::LN_2;

/// Contract-location bands per peer: `band = floor(8 * contract_location)`.
pub(crate) const BANDS: usize = 8;
const _: () = assert!(BANDS.is_power_of_two(), "band masking needs a power of two");

/// Forgetting horizons, in hours. `None` forgets nothing inside the window.
/// Powers of four down from 24h, as in the reference.
pub(crate) const HORIZONS_HOURS: [Option<f64>; HORIZONS] = [None, Some(24.0), Some(6.0), Some(1.5)];
pub(crate) const HORIZONS: usize = 4;

/// Forgetting of the horizon selector's accumulated loss, in hours.
const SELECTOR_FORGETTING_HOURS: f64 = 24.0;

/// Floor on the peer table, for nodes configured with very few connections.
const MIN_PEER_CAPACITY: usize = 64;

/// Largest decay exponent an epoch-scaled weight may carry before a refit
/// rebases it. `e^30` keeps every weight far from overflow and precision loss.
const REBASE_EXPONENT: f64 = 30.0;

/// Minimum Kish effective sample size for a node to count as replicated.
const MIN_EFFECTIVE_N: f64 = 2.0;

/// A node whose decayed count is at or below this carries no evidence and is
/// treated as absent, as in the validated reference (`NODE_MIN`).
const NODE_MIN: f64 = 1e-12;

/// A peer is left out of `tau2_peer` when the rest of the root holds less than
/// this share of its weight: the leave-one-out sums would be cancellation noise.
const ROOT_REST_FLOOR: f64 = 1e-6;

/// Floor on the pooled within-cell variance, as in the reference.
const MIN_SIGMA2: f64 = 1e-9;

/// Response times below this are floored before taking the log.
///
/// The legacy multiplicative estimator ingests a 0 s response as-is; a log
/// stage cannot. A sub-millisecond time to first response is below anything
/// the router can act on, so it is recorded as one millisecond and counted.
pub(crate) const MIN_RESPONSE_SECS: f64 = 1e-3;

/// Peer-table capacity for a node with this connection cap.
///
/// Every peer that appears in the event stream is, or was, a connection. Twice
/// the cap lets the whole connection set turn over once inside a window before
/// an active peer can be evicted; the floor covers tiny configurations.
pub(crate) fn peer_capacity(max_connections: usize) -> usize {
    max_connections.saturating_mul(2).max(MIN_PEER_CAPACITY)
}

/// Events between refits for a window of this size and fill.
pub(crate) fn refit_interval(window_capacity: usize, windowed: usize) -> usize {
    if windowed >= window_capacity {
        REFIT_EVERY.max(window_capacity / REFIT_TURNOVER_DIVISOR)
    } else {
        REFIT_EVERY
    }
}

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

    fn is_log(self) -> bool {
        match self {
            Target::Failure => false,
            Target::LogResponseTime | Target::LogTransferSpeed => true,
        }
    }

    fn min_curve_points(self) -> usize {
        if self.is_log() {
            MIN_CURVE_POINTS_LOG
        } else {
            MIN_CURVE_POINTS_FAILURE
        }
    }

    fn valid(self, y: f64) -> bool {
        match self {
            Target::Failure => (0.0..=1.0).contains(&y),
            Target::LogResponseTime | Target::LogTransferSpeed => y.is_finite(),
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
/// PAV and interpolation follow `pav_regression` 0.7 (queries outside the block
/// range extrapolate along the line through the end block and the input
/// centroid), pinned by `curve_matches_pav_regression`. One deliberate
/// difference: equal-`x` points are ordered to pool in the curve's direction
/// (see [`window_order`]), where the crate orders them by descending `y` for
/// both. Reimplemented because the crate re-sorts its input on every fit and
/// the window here is already sorted.
#[derive(Debug, Clone)]
struct Curve {
    /// Blocks in ascending `x`.
    blocks: Vec<Block>,
    /// Weighted centroid of the input points.
    centroid: (f64, f64),
    /// Hold the end values beyond the block range instead of extrapolating.
    flat_ends: bool,
}

impl Curve {
    /// PAV over points already in [`window_order`] for `ascending`: `x`
    /// ascending, ties by `y` descending for an ascending curve and ascending
    /// for a descending one.
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
            flat_ends: false,
        })
    }

    /// The shrunk curve over `window` (in [`window_order`] for the target), using the one-way
    /// random-effects method of moments over the PAV blocks (module docs).
    fn fit_shrunk(window: &[Event], target: Target) -> Option<Curve> {
        if window.len() < target.min_curve_points() {
            return None;
        }
        let ascending = target.ascending();
        let mut fit = Curve::pav(
            window.iter().map(|event| Block {
                x: event.distance,
                y: event.y,
                w: 1.0,
            }),
            ascending,
        )?;
        fit.flat_ends = target.is_log();
        let k = fit.blocks.len();
        let n = window.len();
        if n <= k {
            // No replication inside any block: noise and signal cannot be told
            // apart, so there is nothing principled to shrink by.
            return Some(fit);
        }
        let sum_y2: f64 = window.iter().map(|event| event.y * event.y).sum();
        let explained: f64 = fit.blocks.iter().map(|b| b.w * b.y * b.y).sum();
        let s2 = (sum_y2 - explained).max(0.0) / (n - k) as f64;
        if k < 2 {
            return Some(fit);
        }
        let total: f64 = fit.blocks.iter().map(|b| b.w).sum();
        let grand = fit.blocks.iter().map(|b| b.w * b.y).sum::<f64>() / total;
        let between: f64 = fit.blocks.iter().map(|b| b.w * (b.y - grand).powi(2)).sum();
        let denominator = total - fit.blocks.iter().map(|b| b.w * b.w).sum::<f64>() / total;
        let tau2 = if denominator > 0.0 {
            ((between - (k - 1) as f64 * s2) / denominator).max(0.0)
        } else {
            0.0
        };
        let shrunk = fit.blocks.iter().map(|b| {
            let factor = if tau2 > 0.0 {
                tau2 / (tau2 + s2 / b.w)
            } else {
                0.0
            };
            Block {
                x: b.x,
                y: grand + factor * (b.y - grand),
                w: b.w,
            }
        });
        let mut shrunk = Curve::pav(shrunk, ascending)?;
        shrunk.flat_ends = fit.flat_ends;
        Some(shrunk)
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
                    if self.flat_ends {
                        blocks[0].y
                    } else {
                        interpolate(blocks[0], centroid, x)
                    }
                } else if above == len {
                    if self.flat_ends {
                        blocks[len - 1].y
                    } else {
                        interpolate(centroid, blocks[len - 1], x)
                    }
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

/// Epoch-scaled forgotten moments. Every quantity read from them is a ratio in
/// which the epoch scale cancels (`n^2 / w2`, `w2 / n^2`, means).
#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct Moments {
    /// `sum w`.
    n: f64,
    /// `sum w^2`.
    w2: f64,
    sum: f64,
    sumsq: f64,
}

impl Moments {
    fn add(&mut self, weight: f64, value: f64) {
        self.n += weight;
        self.w2 += weight * weight;
        self.sum += weight * value;
        self.sumsq += weight * value * value;
    }

    fn mean(&self) -> f64 {
        self.sum / self.n
    }

    /// Kish effective sample size.
    fn effective_n(&self) -> f64 {
        if self.w2 > 0.0 {
            self.n * self.n / self.w2
        } else {
            0.0
        }
    }

    fn replicated(&self) -> bool {
        self.effective_n() >= MIN_EFFECTIVE_N
    }

    /// Sampling variance of the mean per unit single-observation variance:
    /// `sum w^2 / (sum w)^2 = 1 / n_eff`.
    fn mean_variance_factor(&self) -> f64 {
        self.w2 / (self.n * self.n)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct PeerNode {
    peer: Moments,
    cells: [Moments; BANDS],
    /// `sum over bands of cells[b].n^2`.
    sq_cells: f64,
}

/// Method-of-moments variance components, fixed at refit.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Components {
    /// Pooled within-cell variance.
    sigma2: f64,
    /// Between-peer variance of peer effects.
    tau2_peer: f64,
    /// Between-cell variance of cell effects within a peer.
    tau2_cell: f64,
}

/// Posterior of the residual at a query.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Posterior {
    mean: f64,
    variance: f64,
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
    /// `sum w^2` of windowed events whose peer has been evicted, counted as
    /// singleton peers and cells in `sq_peers` and `sq_cells` so the root's
    /// noise does not understate their contribution. Set at rebuild.
    orphan_w2: f64,
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
            orphan_w2: 0.0,
            nodes: Vec::new(),
            components: None,
        }
    }

    fn reset(&mut self, epoch: f64) {
        self.epoch = epoch;
        self.root = Moments::default();
        self.sq_peers = 0.0;
        self.sq_cells = 0.0;
        self.orphan_w2 = 0.0;
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
        let cell = &mut node.cells[band & (BANDS - 1)];
        let before = cell.n;
        cell.add(weight, residual);
        let delta = 2.0 * before * weight + weight * weight;
        node.sq_cells += delta;
        self.sq_cells += delta;
    }

    /// Accumulate a whole prepared window into a freshly reset level. The
    /// squared-count sums are left for `recount_squares`.
    fn rebuild(&mut self, prepared: &mut [Prepared], weighting: Weighting) {
        let mut root = Moments::default();
        let mut orphan_w2 = 0.0;
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
            root.add(weight, event.residual);
            if let Some(node) = nodes.get_mut(event.slot as usize) {
                node.peer.add(weight, event.residual);
                node.cells[event.band as usize & (BANDS - 1)].add(weight, event.residual);
            } else {
                orphan_w2 += weight * weight;
            }
        }
        self.root = root;
        self.orphan_w2 = orphan_w2;
    }

    /// Drop a peer's node. Its events stay in the root (they are still in the
    /// window) and become orphans, each a singleton peer and cell in the root's
    /// squared-count sums, exactly as the next rebuild will count them. Taking
    /// the node's squares out without adding the singletons back would
    /// understate the root's noise until then.
    fn evict(&mut self, slot: usize) {
        if let Some(node) = self.nodes.get_mut(slot) {
            let singletons = node.peer.w2;
            self.sq_peers += singletons - node.peer.n * node.peer.n;
            self.sq_cells += singletons - node.sq_cells;
            self.orphan_w2 += singletons;
            *node = PeerNode::default();
        }
    }

    /// Recompute the squared-count sums exactly, removing any drift the
    /// incremental updates and evictions accumulated.
    fn recount_squares(&mut self) {
        self.sq_peers = self.orphan_w2;
        self.sq_cells = self.orphan_w2;
        for node in &mut self.nodes {
            self.sq_peers += node.peer.n * node.peer.n;
            node.sq_cells = node.cells.iter().map(|cell| cell.n * cell.n).sum();
            self.sq_cells += node.sq_cells;
        }
    }

    /// Variance components by method of moments on leave-one-out contrasts,
    /// with decayed evidence counted by Kish effective size (module docs).
    fn compute_components(&self) -> Option<Components> {
        let (mut ss, mut df) = (0.0, 0.0);
        for node in &self.nodes {
            for cell in &node.cells {
                if cell.replicated() {
                    ss += (cell.sumsq - cell.sum * cell.sum / cell.n).max(0.0);
                    df += cell.n - cell.w2 / cell.n;
                }
            }
        }
        if df < 2.0 {
            return None;
        }
        let sigma2 = (ss / df).max(MIN_SIGMA2);

        // tau2_cell: each replicated cell against the mean of its peer's other
        // cells. Weighted by the design factor of that contrast so peers whose
        // other cells are thin do not dominate.
        //
        // The rest is summed from the other (at most seven) cells directly, not
        // taken as `peer - cell`. Under decay a stale band's weights can fall
        // below ~1e-8 of the active band's, and the subtraction then cancels to
        // exactly zero in `w2` and `n^2`, removing the rest's sampling noise from
        // the contrast and biasing `tau2_cell` upward.
        let (mut acc, mut den) = (0.0, 0.0);
        for node in &self.nodes {
            for (index, cell) in node.cells.iter().enumerate() {
                if !cell.replicated() {
                    continue;
                }
                let mut rest = Moments::default();
                let mut rest_sq = 0.0;
                for (other_index, other) in node.cells.iter().enumerate() {
                    if other_index != index {
                        rest.n += other.n;
                        rest.w2 += other.w2;
                        rest.sum += other.sum;
                        rest_sq += other.n * other.n;
                    }
                }
                if rest.n <= NODE_MIN {
                    continue;
                }
                let contrast = cell.mean() - rest.mean();
                let noise = sigma2 * (cell.mean_variance_factor() + rest.w2 / (rest.n * rest.n));
                acc += contrast * contrast - noise;
                den += 1.0 + rest_sq / (rest.n * rest.n);
            }
        }
        let tau2_cell = if den > 0.0 { (acc / den).max(0.0) } else { 0.0 };

        // tau2_peer: each replicated peer against the mean of every other
        // windowed event (including events whose peer has been evicted).
        let root = self.root;
        let (mut acc, mut den) = (0.0, 0.0);
        for node in &self.nodes {
            let peer = node.peer;
            if !peer.replicated() {
                continue;
            }
            let rest = root.n - peer.n;
            // A peer holding all but a sliver of the root's weight leaves a rest
            // whose `w2` and squared counts are differences of nearly equal
            // numbers; skip it rather than read cancellation noise as signal.
            if rest <= NODE_MIN || rest < ROOT_REST_FLOOR * root.n {
                continue;
            }
            let rest_w2 = (root.w2 - peer.w2).max(0.0);
            let contrast = peer.mean() - (root.sum - peer.sum) / rest;
            let noise = tau2_cell
                * (node.sq_cells / (peer.n * peer.n)
                    + (self.sq_cells - node.sq_cells).max(0.0) / (rest * rest))
                + sigma2 * (peer.mean_variance_factor() + rest_w2 / (rest * rest));
            acc += contrast * contrast - noise;
            den += 1.0 + (self.sq_peers - peer.n * peer.n).max(0.0) / (rest * rest);
        }
        let tau2_peer = if den > 0.0 { (acc / den).max(0.0) } else { 0.0 };

        [sigma2, tau2_peer, tau2_cell]
            .iter()
            .all(|value| value.is_finite())
            .then_some(Components {
                sigma2,
                tau2_peer,
                tau2_cell,
            })
    }

    /// Factor converting stored counts into counts decayed to `now`.
    fn scale(&self, now: f64) -> f64 {
        (-self.exponent(now).max(0.0)).exp()
    }

    /// Posterior of the residual at a query, descending root -> peer -> cell
    /// with a normal-normal update. `None` without components.
    ///
    /// Matches the validated reference (`exp/estimator-bakeoff` 390470ef1,
    /// `HierC::predict` with standard descent): a node is present when its
    /// count decayed to `now` exceeds [`NODE_MIN`], and its noise uses the Kish
    /// factor, which the decay scale cancels out of.
    fn residual(&self, slot: Option<usize>, band: usize, now: f64) -> Option<Posterior> {
        let components = self.components?;
        let scale = self.scale(now);
        // `(mean, noise)` of a node, or `None` where there is no node.
        let step = |(mu, v): (f64, f64), node: Option<(f64, f64)>, tau2: f64| match node {
            Some((mean, noise)) if tau2 > 0.0 => {
                let prior = tau2 + v;
                let b = prior / (prior + noise);
                (mu + b * (mean - mu), b * noise)
            }
            _ => (mu, v + tau2),
        };

        let mut state = (0.0, 0.0);
        let root = self.root;
        if root.n * scale > NODE_MIN {
            let n2 = root.n * root.n;
            let mean = root.mean();
            let noise = components.sigma2 * root.mean_variance_factor()
                + components.tau2_peer * self.sq_peers.max(0.0) / n2
                + components.tau2_cell * self.sq_cells.max(0.0) / n2;
            let tau2_root = (mean * mean - noise).max(0.0);
            state = step(state, Some((mean, noise)), tau2_root);
        }

        let node = slot.and_then(|slot| self.nodes.get(slot));
        let peer = node.and_then(|node| {
            let peer = node.peer;
            (peer.n * scale > NODE_MIN && peer.w2 > 0.0).then(|| {
                let noise = components.tau2_cell * node.sq_cells.max(0.0) / (peer.n * peer.n)
                    + components.sigma2 * peer.mean_variance_factor();
                (peer.mean(), noise)
            })
        });
        state = step(state, peer, components.tau2_peer);

        let cell = node.and_then(|node| {
            let cell = node.cells[band & (BANDS - 1)];
            (cell.n * scale > NODE_MIN && cell.w2 > 0.0)
                .then(|| (cell.mean(), components.sigma2 * cell.mean_variance_factor()))
        });
        state = step(state, cell, components.tau2_cell);

        (state.0.is_finite() && state.1.is_finite()).then_some(Posterior {
            mean: state.0,
            variance: state.1.max(0.0),
        })
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

    /// Evictions per batch: `capacity / 64`, at least one.
    fn batch(&self) -> usize {
        (self.capacity / 64).max(1)
    }

    /// Slot for `key`, marking it used. Evicted slots are appended to
    /// `evicted`; the caller must clear them from every level before using the
    /// returned slot.
    fn touch(&mut self, key: &K, evicted: &mut Vec<usize>) -> (usize, u32) {
        self.use_clock += 1;
        if let Some(&slot) = self.index.get(key) {
            self.slots[slot].last_used = self.use_clock;
            return (slot, self.slots[slot].generation);
        }
        if self.index.len() >= self.capacity {
            self.evict_batch(evicted);
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
        (slot, self.slots[slot].generation)
    }

    /// Evict the least-recently-used batch of peers.
    ///
    /// A batch, not one: the scan is linear in the capacity, and on a node whose
    /// churn keeps the table full, one eviction per new peer would be a scan
    /// per event.
    fn evict_batch(&mut self, evicted: &mut Vec<usize>) {
        let mut occupied: Vec<(u64, usize)> = self
            .slots
            .iter()
            .enumerate()
            .filter(|(_, slot)| slot.occupied)
            .map(|(index, slot)| (slot.last_used, index))
            .collect();
        let batch = self.batch().min(occupied.len());
        if batch == 0 {
            return;
        }
        if batch < occupied.len() {
            occupied.select_nth_unstable(batch - 1);
        }
        for &(_, slot) in &occupied[..batch] {
            if let Some(key) = self.keys[slot].take() {
                self.index.remove(&key);
            }
            let entry = &mut self.slots[slot];
            entry.occupied = false;
            entry.generation = entry.generation.wrapping_add(1);
            self.free.push(slot);
            evicted.push(slot);
        }
        self.evictions += batch as u64;
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

/// Window order: ascending distance, with equal distances ordered so that PAV
/// pools them into one block.
///
/// PAV merges a point into the previous block when it violates the curve's
/// direction, so equal-`x` points must arrive in violating order: descending
/// `y` for an ascending curve (as `pav_regression` orders them) and ascending
/// `y` for a descending one. `pav_regression` uses descending `y` for both,
/// which leaves ties on a descending curve as separate blocks; this does not.
fn window_order(ascending: bool) -> impl Fn(&Event, &Event) -> std::cmp::Ordering + Copy {
    move |a: &Event, b: &Event| {
        a.distance.total_cmp(&b.distance).then_with(|| {
            if ascending {
                b.y.total_cmp(&a.y)
            } else {
                a.y.total_cmp(&b.y)
            }
        })
    }
}

fn band_of(contract_location: f64) -> usize {
    if !contract_location.is_finite() {
        return 0;
    }
    ((contract_location * BANDS as f64).floor().max(0.0) as usize).min(BANDS - 1)
}

/// Size bounds the published memory budget is computed from (window
/// `WINDOW_EVENTS x EVENT_BYTES`, refit buffer `WINDOW_EVENTS x PREPARED_BYTES`,
/// levels `HORIZONS x peer capacity x PEER_NODE_BYTES`). Enforced at compile
/// time, so a field that grows a hot struct cannot silently grow the budget.
const EVENT_BYTES: usize = 48;
const PREPARED_BYTES: usize = 32;
const PEER_NODE_BYTES: usize = 296;
const LEVEL_BYTES: usize = 136;
const _: () = {
    assert!(std::mem::size_of::<Event>() <= EVENT_BYTES);
    assert!(std::mem::size_of::<Prepared>() <= PREPARED_BYTES);
    assert!(std::mem::size_of::<PeerNode>() <= PEER_NODE_BYTES);
    assert!(std::mem::size_of::<Level>() <= LEVEL_BYTES);
};

/// A windowed event reduced to what a hierarchy rebuild needs.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Prepared {
    residual: f64,
    time: f64,
    /// Scratch: the epoch-scaled weight at the level being rebuilt.
    weight: f64,
    /// `u32::MAX` when the event's peer has since been evicted. Never a valid
    /// index: slots are bounded by the peer table's capacity.
    slot: u32,
    band: u8,
}

/// Buffers a refit needs, owned once and shared by every stage so no stage
/// keeps a window-sized allocation of its own between refits.
#[derive(Debug, Clone, Default)]
pub(crate) struct Scratch {
    prepared: Vec<Prepared>,
    evicted: Vec<usize>,
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

/// Shape of the within-cell log residuals, the check on the lognormal
/// assumption behind expectation timing.
///
/// `E[T] = exp(mu + sigma2 / 2)` is exact only when log response times are
/// normal around their cell mean. Skewness and excess kurtosis of those
/// deviations are both zero for a normal; a heavy right tail (occasional very
/// slow responses) shows up as positive skew and kurtosis, and means the
/// expectation understates the mean. Measured on the no-forgetting level, over
/// events whose peer is still tracked, at every refit.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct ResidualShape {
    pub events: usize,
    pub skewness: Option<f64>,
    pub excess_kurtosis: Option<f64>,
}

impl ResidualShape {
    /// Fewest within-cell deviations before a shape is reported.
    const MIN_EVENTS: usize = 30;

    /// Fewest events a cell needs to contribute. Deviations about a small
    /// cell's own mean are strongly non-normal even for normal data (a
    /// two-event cell's two deviations are always equal and opposite), which
    /// would read as spurious negative kurtosis.
    const MIN_CELL_EVENTS: f64 = 10.0;

    fn measure(prepared: &[Prepared], level: &Level) -> ResidualShape {
        Self::measure_with(prepared, level, Self::MIN_CELL_EVENTS)
    }

    /// [`Self::measure`] with an explicit per-cell minimum, so the two guards
    /// (the minimum and the `sqrt(n/(n-1))` rescaling) can each be tested with
    /// the other out of the way.
    fn measure_with(prepared: &[Prepared], level: &Level, min_cell_events: f64) -> ResidualShape {
        let (mut n, mut m2, mut m3, mut m4) = (0usize, 0.0, 0.0, 0.0);
        for event in prepared {
            let Some(node) = level.nodes.get(event.slot as usize) else {
                continue;
            };
            let cell = node.cells[event.band as usize & (BANDS - 1)];
            let cell_n = cell.effective_n();
            if cell_n < min_cell_events.max(3.0) {
                continue;
            }
            // Deviations about a cell's own mean shrink its moments: the second
            // by (n-1)/n and the third by (n-1)(n-2)/n^2. Each is reweighted by
            // the inverse (the unbiased k-statistic factors), so cells of
            // different sizes pool on one scale and a small cell's skewness is
            // not understated. The fourth moment uses the second-moment
            // rescaling only; its exact correction (the h-statistic) needs
            // per-cell second moments too, so excess kurtosis still reads a
            // little low in small cells (5.1 against a truth of 6 at n = 10 on
            // exponential data), which is small beside the >1 departures the
            // verdict looks for.
            let raw = event.residual - cell.mean();
            let e = raw * (cell_n / (cell_n - 1.0)).sqrt();
            let e2 = e * e;
            n += 1;
            m2 += e2;
            m3 += raw * raw * raw * cell_n * cell_n / ((cell_n - 1.0) * (cell_n - 2.0));
            m4 += e2 * e2;
        }
        if n < Self::MIN_EVENTS || m2 <= 0.0 {
            return ResidualShape {
                events: n,
                ..ResidualShape::default()
            };
        }
        let count = n as f64;
        let variance = m2 / count;
        ResidualShape {
            events: n,
            skewness: Some((m3 / count) / variance.powf(1.5)).filter(|v| v.is_finite()),
            excess_kurtosis: Some((m4 / count) / (variance * variance) - 3.0)
                .filter(|v| v.is_finite()),
        }
    }
}

/// A stage's forecast on its own scale.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Forecast {
    /// Probability, or the log-scale location `mu`.
    pub value: f64,
    /// Predictive variance of a single log observation, `sigma2 + v_post`.
    /// Zero for the failure stage, which does not use it.
    pub spread: f64,
}

/// Read-only state of a stage, for the dashboard.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct StageDiagnostics {
    pub window_events: usize,
    pub peers: usize,
    pub peer_capacity: usize,
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
    /// Pooled within-cell variance of the selected horizon (log units for the
    /// timing stages), the `sigma2` expectation timing adds to the log mean.
    pub residual_sigma2: Option<f64>,
    /// Lognormality check for the log stages; default for failure.
    pub residual_shape: ResidualShape,
}

/// One target's estimator.
#[derive(Debug, Clone)]
pub(crate) struct Stage<K> {
    target: Target,
    window_capacity: usize,
    /// Events as of the last refit, in [`window_order`] for the target.
    sorted: Vec<Event>,
    /// Events learned since the last refit, in arrival order.
    fresh: Vec<Event>,
    next_seq: u64,
    since_refit: usize,
    curve: Option<Curve>,
    /// Smallest and largest target in the window at the last refit.
    observed_range: (f64, f64),
    peers: PeerTable<K>,
    levels: [Level; HORIZONS],
    /// Decayed prequential squared loss per horizon.
    loss: [f64; HORIZONS],
    loss_time: Option<f64>,
    /// Latest time seen. Time never runs backwards inside a stage: a clock that
    /// steps back is read as "no time has passed".
    clock: f64,
    refits: u64,
    rejected: u64,
    orphaned_at_last_refit: usize,
    /// Shape of the log residuals within cells at the last refit (log stages).
    residual_shape: ResidualShape,
}

impl<K: Hash + Eq + Clone> Stage<K> {
    pub(crate) fn new(target: Target, peer_capacity: usize) -> Self {
        Self::with_limits(target, WINDOW_EVENTS, peer_capacity)
    }

    pub(crate) fn with_limits(
        target: Target,
        window_capacity: usize,
        peer_capacity: usize,
    ) -> Self {
        Stage {
            target,
            window_capacity: window_capacity.max(target.min_curve_points()),
            sorted: Vec::new(),
            fresh: Vec::new(),
            next_seq: 0,
            since_refit: 0,
            curve: None,
            observed_range: (f64::NEG_INFINITY, f64::INFINITY),
            peers: PeerTable::new(peer_capacity),
            levels: HORIZONS_HOURS.map(Level::new),
            loss: [0.0; HORIZONS],
            loss_time: None,
            clock: f64::NEG_INFINITY,
            refits: 0,
            rejected: 0,
            orphaned_at_last_refit: 0,
            residual_shape: ResidualShape::default(),
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
        for index in 1..HORIZONS {
            if self.loss[index] < self.loss[best] {
                best = index;
            }
        }
        best
    }

    /// Map a composed value onto the target's range: `[0, 1]` for failure, the
    /// window's observed range widened by [`LOG_PREDICTION_MARGIN`] for a log
    /// stage.
    pub(crate) fn bound(&self, value: f64) -> f64 {
        match self.target {
            Target::Failure => value.clamp(0.0, 1.0),
            Target::LogResponseTime | Target::LogTransferSpeed => {
                let (low, high) = self.observed_range;
                value.clamp(low - LOG_PREDICTION_MARGIN, high + LOG_PREDICTION_MARGIN)
            }
        }
    }

    fn prior(&self, distance: f64) -> Option<f64> {
        Some(self.bound(self.curve.as_ref()?.value(distance)?))
    }

    /// The forecast one horizon makes at a query, given the prior.
    fn forecast_with(
        &self,
        level: usize,
        slot: Option<usize>,
        band: usize,
        prior: f64,
        now: f64,
    ) -> Forecast {
        let posterior = self.levels[level].residual(slot, band, now);
        let value = self.bound(prior + posterior.map_or(0.0, |p| p.mean));
        // `sigma2 + v_post` from the horizon's components, as the reference
        // does; before components exist there is no spread, so the forecast is
        // the median until the first replicated refit.
        let spread = match self.target {
            Target::Failure => 0.0,
            Target::LogResponseTime | Target::LogTransferSpeed => {
                posterior.map_or(0.0, |p| p.variance)
                    + self.levels[level].components.map_or(0.0, |c| c.sigma2)
            }
        };
        Forecast { value, spread }
    }

    /// Forecast on the target's scale. `None` until the stage has a curve.
    ///
    /// `O(1)` in the window: one hash lookup, one binary search over the
    /// curve's blocks, and the selected horizon's arithmetic.
    pub(crate) fn predict(
        &self,
        peer: &K,
        contract_location: f64,
        distance: f64,
        now: f64,
    ) -> Option<Forecast> {
        let now = self.effective_time(now);
        let prior = self.prior(distance)?;
        let forecast = self.forecast_with(
            self.selected(),
            self.peers.lookup(peer),
            band_of(contract_location),
            prior,
            now,
        );
        (forecast.value.is_finite() && forecast.spread.is_finite()).then_some(forecast)
    }

    /// Learn one outcome, returning the forecast made for it BEFORE learning
    /// it. Every horizon is scored on that same pre-learning state.
    pub(crate) fn observe(
        &mut self,
        scratch: &mut Scratch,
        peer: &K,
        contract_location: f64,
        distance: f64,
        y: f64,
        now: f64,
    ) -> Option<Forecast> {
        if !self.target.valid(y) || !distance.is_finite() {
            self.rejected += 1;
            return None;
        }
        let now = self.effective_time(now);
        self.clock = now;
        let band = band_of(contract_location);

        let mut forecast = None;
        let prior = self.prior(distance);
        if let Some(prior) = prior {
            let slot = self.peers.lookup(peer);
            let forecasts: [Forecast; HORIZONS] =
                std::array::from_fn(|level| self.forecast_with(level, slot, band, prior, now));
            let selected = forecasts[self.selected()];
            forecast =
                (selected.value.is_finite() && selected.spread.is_finite()).then_some(selected);
            self.score(&forecasts, y, now);
        }

        scratch.evicted.clear();
        let (slot, generation) = self.peers.touch(peer, &mut scratch.evicted);
        for &victim in &scratch.evicted {
            for level in &mut self.levels {
                level.evict(victim);
            }
        }
        self.fresh.push(Event {
            distance,
            y,
            time: now,
            seq: self.next_seq,
            slot: slot as u32,
            generation,
            band: band as u8,
        });
        self.next_seq += 1;
        self.since_refit += 1;

        let mut must_rebase = false;
        if let Some(prior) = prior {
            must_rebase = self
                .levels
                .iter()
                .any(|level| level.exponent(now) > REBASE_EXPONENT);
            if !must_rebase {
                for level in &mut self.levels {
                    let weight = level.weight(now);
                    level.add(Some(slot), band, weight, y - prior);
                }
            }
        }

        let windowed = self.sorted.len() + self.fresh.len();
        if windowed < EAGER_REFIT_BELOW
            || self.since_refit >= refit_interval(self.window_capacity, windowed)
            || must_rebase
        {
            self.refit(scratch, now);
        }
        forecast
    }

    /// Score every horizon's FINISHED forecast, i.e. the clamped value the
    /// router would act on, not the unclamped residual.
    fn score(&mut self, forecasts: &[Forecast; HORIZONS], y: f64, now: f64) {
        let factor = self.loss_time.map_or(1.0, |then| {
            (-(now - then).max(0.0) / SELECTOR_FORGETTING_HOURS).exp()
        });
        for (loss, forecast) in self.loss.iter_mut().zip(forecasts) {
            let error = (forecast.value - y).powi(2);
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
    fn refit(&mut self, scratch: &mut Scratch, now: f64) {
        self.refits += 1;
        self.since_refit = 0;
        self.merge_fresh();
        if let Some(curve) = Curve::fit_shrunk(&self.sorted, self.target) {
            self.curve = Some(curve);
        }
        self.observed_range = self
            .sorted
            .iter()
            .fold((f64::INFINITY, f64::NEG_INFINITY), |(low, high), event| {
                (low.min(event.y), high.max(event.y))
            });
        for level in &mut self.levels {
            level.reset(now);
        }
        if self.prepare(&mut scratch.prepared) {
            self.rebuild_levels(&mut scratch.prepared, now);
        }
        scratch.prepared.clear();
    }

    /// Fold events learned since the last refit into the sorted window and drop
    /// events that have left it.
    fn merge_fresh(&mut self) {
        let oldest_live = self.next_seq.saturating_sub(self.window_capacity as u64);
        self.sorted.retain(|event| event.seq >= oldest_live);
        self.fresh.retain(|event| event.seq >= oldest_live);
        let order = window_order(self.target.ascending());
        self.fresh.sort_unstable_by(order);
        // In-place merge from the back: no second window-sized buffer.
        let existing = self.sorted.len();
        let incoming = self.fresh.len();
        if incoming > 0 {
            let filler = self.fresh[0];
            // Exact, so the window's allocation tops out at its bound instead of
            // the next power of two (16k slots for a 10k window).
            self.sorted.reserve_exact(incoming);
            self.sorted.resize(existing + incoming, filler);
            let (mut i, mut j, mut write) = (existing, incoming, existing + incoming);
            while j > 0 {
                if i > 0 && order(&self.sorted[i - 1], &self.fresh[j - 1]).is_gt() {
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

    /// Re-anchor every windowed residual on the current curve, linearly, since
    /// the window is sorted by distance. `false` without a curve.
    fn prepare(&mut self, prepared: &mut Vec<Prepared>) -> bool {
        let Some(curve) = self.curve.as_ref() else {
            return false;
        };
        let mut orphaned = 0;
        let mut cursor = 0;
        prepared.clear();
        prepared.reserve_exact(self.sorted.len());
        for event in &self.sorted {
            let Some(value) = curve.value_sorted(event.distance, &mut cursor) else {
                continue;
            };
            let live = self.peers.generation(event.slot as usize) == Some(event.generation);
            if !live {
                orphaned += 1;
            }
            prepared.push(Prepared {
                residual: event.y - self.bound(value),
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
    /// recompute its variance components. One tight pass per level keeps each
    /// level's node table hot in cache, and lets a faster horizon's weight be an
    /// integer power of the slower one's (24h -> 6h -> 1.5h are powers of four).
    fn rebuild_levels(&mut self, prepared: &mut [Prepared], now: f64) {
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
            level.rebuild(prepared, weighting);
        }
        for level in &mut self.levels {
            level.recount_squares();
            level.components = level.compute_components();
        }
        if self.target.is_log() {
            self.residual_shape = ResidualShape::measure(prepared, &self.levels[0]);
        }
    }

    pub(crate) fn diagnostics(&self) -> StageDiagnostics {
        StageDiagnostics {
            window_events: self.sorted.len() + self.fresh.len(),
            peers: self.peers.index.len(),
            peer_capacity: self.peers.capacity,
            peer_evictions: self.peers.evictions,
            refits: self.refits,
            rejected: self.rejected,
            selected_horizon_hours: HORIZONS_HOURS[self.selected()],
            active: self.curve.is_some(),
            orphaned_at_last_refit: self.orphaned_at_last_refit,
            residual_sigma2: self.levels[self.selected()].components.map(|c| c.sigma2),
            residual_shape: self.residual_shape,
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
    /// The timing and speed estimates routing would have acted on (expected
    /// seconds, effective bytes/s), made for every event whether or not it
    /// turned out to be timed, so timing can be scored against the timed subset.
    pub estimate: Estimate,
}

/// A routing estimate in the router's own units.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct Estimate {
    pub failure_probability: Option<f64>,
    /// Expected time to response start, `E[T]`, in seconds.
    pub time_to_response_start_secs: Option<f64>,
    /// Effective transfer speed in bytes/s: the reciprocal of `E[1/speed]`, so
    /// that `bytes / speed` is the expected transfer time.
    pub transfer_speed_bps: Option<f64>,
}

/// Minimum estimator hours between two saturation log lines.
const SATURATION_LOG_INTERVAL_HOURS: f64 = 1.0;

/// The estimator for all three router stages.
#[derive(Clone)]
pub(crate) struct HierarchicalRouting {
    failure: Stage<PeerKeyLocation>,
    response_time: Stage<PeerKeyLocation>,
    transfer_speed: Stage<PeerKeyLocation>,
    scratch: Scratch,
    /// Timed successes whose response time was floored to [`MIN_RESPONSE_SECS`].
    floored_response_times: u64,
    /// Successes that carried no transfer-speed sample (zero payload or zero
    /// duration). Not rejections: there was nothing to learn.
    non_speed_samples: u64,
    last_saturation_log: Option<f64>,
    evictions_at_last_log: u64,
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
    /// An estimator whose peer tables hold [`peer_capacity`]`(max_connections)`.
    pub(crate) fn new(max_connections: usize) -> Self {
        let capacity = peer_capacity(max_connections);
        HierarchicalRouting {
            failure: Stage::new(Target::Failure, capacity),
            response_time: Stage::new(Target::LogResponseTime, capacity),
            transfer_speed: Stage::new(Target::LogTransferSpeed, capacity),
            scratch: Scratch::default(),
            floored_response_times: 0,
            non_speed_samples: 0,
            last_saturation_log: None,
            evictions_at_last_log: 0,
        }
    }

    /// Learn one routing outcome at estimator time `time` (hours), returning
    /// the forecasts made before it.
    pub(crate) fn observe_at(
        &mut self,
        peer: &PeerKeyLocation,
        contract_location: Location,
        distance: f64,
        outcome: &RoutingOutcome,
        time: f64,
    ) -> Observed {
        let contract = contract_location.as_f64();
        let estimate = self.estimate(peer, contract_location, distance, time);
        let failure = self
            .failure
            .observe(
                &mut self.scratch,
                peer,
                contract,
                distance,
                if outcome.success { 0.0 } else { 1.0 },
                time,
            )
            .map(|forecast| forecast.value);
        if let Some(seconds) = outcome.time_to_response_start_secs {
            let seconds = if (0.0..MIN_RESPONSE_SECS).contains(&seconds) {
                self.floored_response_times += 1;
                MIN_RESPONSE_SECS
            } else {
                seconds
            };
            // Negative or non-finite times are refused and counted by `observe`.
            self.response_time.observe(
                &mut self.scratch,
                peer,
                contract,
                distance,
                seconds.ln(),
                time,
            );
        }
        match outcome.transfer_speed_bps {
            // A zero speed is a zero-byte payload (SUBSCRIBE), which legacy also
            // skips: not a sample, so not a rejection either.
            Some(speed) if speed > 0.0 => {
                self.transfer_speed.observe(
                    &mut self.scratch,
                    peer,
                    contract,
                    distance,
                    speed.ln(),
                    time,
                );
            }
            Some(_) => self.non_speed_samples += 1,
            None => {
                if outcome.time_to_response_start_secs.is_some() {
                    self.non_speed_samples += 1;
                }
            }
        }
        self.log_saturation(time);
        Observed { failure, estimate }
    }

    /// Info-level, rate-limited notice that the peer tables are evicting live
    /// entries. Evictions are the signal that `max_connections` headroom is too
    /// small for this node's churn.
    fn log_saturation(&mut self, time: f64) {
        let evictions = self.total_evictions();
        if evictions == self.evictions_at_last_log {
            return;
        }
        let due = self
            .last_saturation_log
            .is_none_or(|then| time - then >= SATURATION_LOG_INTERVAL_HOURS);
        if !due {
            return;
        }
        tracing::info!(
            evictions_total = evictions,
            evictions_since_last_notice = evictions - self.evictions_at_last_log,
            peer_capacity = self.failure.peers.capacity,
            "hierarchical routing estimator: peer table full, evicting least-recently-used peers"
        );
        self.last_saturation_log = Some(time);
        self.evictions_at_last_log = evictions;
    }

    pub(crate) fn total_evictions(&self) -> u64 {
        self.failure.peers.evictions
            + self.response_time.peers.evictions
            + self.transfer_speed.peers.evictions
    }

    /// Estimate every stage in router units, at estimator time `time` (hours).
    pub(crate) fn estimate(
        &self,
        peer: &PeerKeyLocation,
        contract_location: Location,
        distance: f64,
        time: f64,
    ) -> Estimate {
        let contract = contract_location.as_f64();
        Estimate {
            failure_probability: self
                .failure
                .predict(peer, contract, distance, time)
                .map(|forecast| forecast.value),
            // The expectation's log, `mu +- spread/2`, is bounded like `mu`
            // itself: a noisy early `tau2` can give an unknown peer a very large
            // posterior variance, and unbounded it would price that peer as
            // effectively unroutable. See `LOG_PREDICTION_MARGIN`.
            time_to_response_start_secs: self
                .response_time
                .predict(peer, contract, distance, time)
                .map(|f| self.response_time.bound(f.value + f.spread / 2.0).exp())
                .filter(|seconds| seconds.is_finite()),
            transfer_speed_bps: self
                .transfer_speed
                .predict(peer, contract, distance, time)
                .map(|f| self.transfer_speed.bound(f.value - f.spread / 2.0).exp())
                .filter(|speed| speed.is_finite() && *speed > 0.0),
        }
    }

    pub(crate) fn diagnostics(&self) -> [StageDiagnostics; 3] {
        [
            self.failure.diagnostics(),
            self.response_time.diagnostics(),
            self.transfer_speed.diagnostics(),
        ]
    }

    pub(crate) fn non_speed_samples(&self) -> u64 {
        self.non_speed_samples
    }

    pub(crate) fn floored_response_times(&self) -> u64 {
        self.floored_response_times
    }
}

#[cfg(test)]
mod tests;
