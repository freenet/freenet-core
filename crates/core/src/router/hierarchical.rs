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
//! which is removed in a later PR. Until then both stacks exist: this one
//! routes by default, `FREENET_ROUTING_HIERARCHICAL=0` (or any other value that
//! resolves off) puts a node back on the legacy stack, and on such a node this
//! estimator is computed at all only while the routing dataset is being
//! recorded.
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
//! Two LATER differences, added after that re-validation and NOT covered by
//! it, which is where the next reader will look for them: the per-stage
//! forgetting menus ([`FAILURE_HORIZONS_HOURS`] against
//! [`LOG_HORIZONS_HOURS`]), and the contract term described below. The
//! bake-off's harness draws a fresh contract per event, so it cannot exercise
//! the contract term at all; see "Provenance" above for what that means for
//! the condition it satisfies.
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
//! 4. **Forgetting horizon chosen online.** One hierarchy per horizon in the
//!    stage's menu ([`FAILURE_HORIZONS_HOURS`] for failure,
//!    [`LOG_HORIZONS_HOURS`] for timing and speed); the one with the lowest
//!    decayed prequential squared loss of its FINISHED forecast predicts. Every
//!    horizon is scored before the event is learned.
//! 5. **Contract term (failure stage only).** See below.
//!
//! # Contract term
//!
//! A failure on a contract that nobody can serve is not evidence about the peer
//! that was asked. Learned as if it were, it raises that peer's forecasts for
//! every OTHER contract: in the soak's storm hours (#4485, #5700) peers whose
//! SUBSCRIBEs all succeeded were forecast to fail 24% of the time, against 3%
//! for clean peers. The failure stage therefore models the residual as
//! `r = c_contract + u_peer + v_(peer, band) + e`:
//!
//! - **Contract table.** A second small hierarchy, contract -> (contract,
//!   peer), keyed by the contract location's bits, with at most
//!   [`CONTRACT_ENTRIES`] peers per contract and [`CONTRACT_CAPACITY`]
//!   contracts (batched LRU), forgetting at [`CONTRACT_HORIZON_HOURS`]. Its
//!   variance components mirror the peer levels' (leave-one-out contrasts,
//!   Kish counting); an entry counts only while its decayed count is at least
//!   [`CONTRACT_PRESENCE`].
//! - **Explain-away.** The raw residual goes into the contract table. What the
//!   root, peer and cell levels learn is `r` minus the contract effect from
//!   OTHER peers only (at least [`CONTRACT_MIN_OTHER_PEERS`] present). Leaving
//!   the peer out is what keeps a peer that fails every contract charged to
//!   itself: its own failures never explain themselves away. The curve is
//!   still fitted on the raw target.
//! - **Refit.** The table is rebuilt against the new curve and every windowed
//!   residual re-adjusted, so the first failures on a dead contract are cleared
//!   from the peer levels at the next refit. An event whose contract has no
//!   present evidence keeps its last adjustment.
//! - **Forecast.** A failure forecast adds the contract effect from ALL present
//!   peers, at every horizon, before the `[0, 1]` bound; without it, the model
//!   forecasts a dead contract's failures low once they are explained away.
//!   The effect is candidate-INDEPENDENT: the same number is added to every
//!   candidate at a given moment, so it never changes which candidate has the
//!   higher failure PROBABILITY. It does NOT follow that it cannot change
//!   which peer is SELECTED, and an earlier version of this note claimed that
//!   it could not. Routing ranks by expected total cost, which on the timed
//!   branch is `t + transfer + 3 * t * p` (`router.rs`), so a shared effect
//!   `d` moves candidate `i`'s cost by `3 * t_i * d`, which differs across
//!   candidates whenever their response times differ: a positive effect
//!   favours the faster-responding peer, a negative one the slower. Measured
//!   on the tuning window, the mean effect is about -0.013 to -0.066 at
//!   healthy instants and +0.098 to +0.187 at degraded ones, so this
//!   re-weighting happens in ordinary operation and not only on dead
//!   contracts. Pinned by
//!   `a_shared_contract_effect_reweights_peers_that_differ_in_response_time`.
//!   The shared form is therefore preferred over the leave-one-peer-out form
//!   on measured ranking behaviour, not on candidate-independence: the
//!   leave-one-out form ranked the FAILING peer lower in 45 of 53 tuning-window
//!   pairs, which is a direct ranking error, while the shared form's effect on
//!   ranking is this indirect re-weighting. The bar is one peer higher than
//!   the learning bar (at least `CONTRACT_MIN_OTHER_PEERS + 1` including the
//!   candidate); see [`CONTRACT_MIN_OTHER_PEERS`] for what that asymmetry
//!   costs. Routing ranks peers by the forecast before the bound
//!   ([`ranking_failure_probability`]), so a large shared effect that clamps
//!   several peers at 1 does not erase the differences between them.
//!
//! Measured offline on the recorded gateway soak (2026-09-17), with constants
//! tuned on its first part only and scored once on the rest under the
//! pre-registered gate (`routing-soak-gate/PLAN-v2.md`), and re-scored after
//! the 2026-09-17 review fixes changed the model: failure Brier against
//! legacy 0.989 [0.923, 1.064] where the estimator without the term scored
//! 1.322, and the excess false-alarm forecast on successes of recently
//! storm-tainted peers 0.068 [0.018, 0.125] against 0.183. **The gate's
//! verdict on that scoring was INSUFFICIENT, not PASS**: the Brier check is
//! secondary, the pollution measure's confidence interval did not resolve
//! against its bar, and ranking within a contract was not resolvable on that
//! data at all. So the term is not validated; it is measured, on a gate that
//! could not decide. Three further limits of that measurement, from the
//! 2026-09-17 review:
//!
//! - the #5655 synthetic bake-off condition holds VACUOUSLY. That harness
//!   draws a fresh contract per event, so no `(contract, peer)` cell is ever
//!   replicated, the components are never estimable, and the term never
//!   activates. It is evidence that the term is inert there, not that it is
//!   harmless where it acts.
//! - M2 scores only forecasts that are too high on successes, so an
//!   over-correction reads as an improvement, and M3's interval cannot resolve
//!   a level shift of the size the explain-away coverage asymmetry implies.
//!   The size of that shift was measured three times and the first two
//!   answers were wrong about DIFFERENT things, so the sequence is recorded.
//!   A first measurement gave +0.00167 per event and was reported as
//!   contradicting the review's estimate of about +0.024. It did not: it
//!   covered only the LIVE learn path, which is a small fraction of the
//!   adjustment an event ends up carrying. Measured over the last refit's
//!   whole prepared window, which is the population the mechanism actually
//!   reaches, the net amount subtracted per event is **+0.0235 on the gate
//!   window** and **-0.0331 on the gateway-1 tuning events**, both the same
//!   order as the review's figure, which is therefore corroborated in
//!   magnitude. The SIGN differs between the two windows, so no single signed
//!   number describes both. See [`CONTRACT_MIN_OTHER_PEERS`] for the
//!   outcome-conditional means and for the finding that the coverage
//!   asymmetry itself reverses between windows.
//! - the gate replays the recorded CLAMPED forecast, and the unbounded value
//!   is recorded nowhere, so no offline tool can reproduce the router's order
//!   among candidates whose forecasts all clamp.
//!
//! The numbers above are the re-scored ones; see the PR for the full row, the
//! measured coverage asymmetry the review found and the fixes did not close,
//! and the isolated measurement of the failure horizon menu.
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
//! A prediction is one hash lookup in the peer table, one binary search over
//! the curve's blocks, a constant amount of arithmetic for the selected
//! horizon, and for the failure stage a second hash lookup plus a scan of one
//! contract's [`CONTRACT_ENTRIES`] entries for the shared effect. That second
//! lookup is per CANDIDATE although its value is one number per decision;
//! hoisting it needs an estimator API that takes the contract once per
//! decision rather than once per candidate, which is left for a follow-up.
//! The number in tree is `routing_estimate_cost_per_candidate`, which measures
//! the whole three-stage estimate WITH the term active and prints it; an
//! earlier version of this note quoted about 15 microseconds per decision at
//! 200 candidates for the redundant lookup alone, which was a reviewer's
//! estimate that nothing here produces, and it is withdrawn.
//!
//! A refit is linear in the window, plus one sort of it for the contract
//! table's rebuild, and once the window is full runs every [`refit_interval`]
//! events.

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

/// Forgetting horizons of the response-time and transfer-speed stages, in
/// hours. `None` forgets nothing inside the window. Powers of four down from
/// 24h, as in the reference.
pub(crate) const LOG_HORIZONS_HOURS: [Option<f64>; HORIZONS] =
    [None, Some(24.0), Some(6.0), Some(1.5)];

/// Forgetting horizons of the failure stage, in hours.
///
/// Shorter than [`LOG_HORIZONS_HOURS`]: on the recorded gateway soak (#4485,
/// 2026-09-15/16) the failure stage's accuracy gap against legacy came from
/// bursts of failures that a 1.5 h memory was too slow to follow. The menu was
/// chosen on the soak's first half (horizon sweep, 2026-09-15) and validated on
/// held-out data and the synthetic bake-off (2026-09-16). It keeps `None` and
/// 1.5 h because the prequential selector falls back to them on quiet nodes,
/// where the short levels hold almost no evidence. The timing stages keep the
/// longer menu: the same short menu there failed the bake-off's quiet timing
/// scenarios and caused ranking churn against healthy peers.
pub(crate) const FAILURE_HORIZONS_HOURS: [Option<f64>; HORIZONS] =
    [None, Some(1.5), Some(0.25), Some(0.05)];

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

/// Smallest within-cell variance a Bernoulli outcome can credibly have after
/// `n_eff` effective observations.
///
/// A cell whose observations are unanimous has a sample variance of exactly 0,
/// and for a 0/1 target that is not evidence that the underlying probability is
/// degenerate: it is what a moderate `p` looks like in a small sample. The
/// Laplace-smoothed rate after `k` successes in `n` trials is `(k+1)/(n+2)`, so
/// a unanimous sample gives `p~ = (n+1)/(n+2)` and a variance of
/// `p~(1-p~) = (n+1)/(n+2)^2`, which is bounded above by and close to
/// `1/(n+2)`. That is the form used here: parameter-free, monotone decreasing
/// in evidence, and it needs nothing the entries do not already carry.
///
/// What it buys, which is the whole point: the noise a query subtracts is
/// `sigma2 * w2/n^2 + ...`, so it falls as `1 / n_eff`. Two barely-replicated
/// cells give a floor of about 0.25 and a heavily shrunk effect, while a dead
/// contract with several peers and many events each gives a floor of about 0.1
/// against a much larger `n_eff`, leaving the effect close to full strength.
///
/// Note where the leverage actually is, because an earlier version of this
/// paragraph put it in the wrong place: the FLOOR itself differs by only about
/// 2.5x between those regimes (0.25 at `n_eff` 2 against 0.1 at `n_eff` 8).
/// The order of magnitude is in the NOISE term, because `w2/n^2` falls with
/// evidence as well, and it is their product that separates the two cases.
/// Pinned in both regimes by
/// `the_evidence_floor_shrinks_thin_evidence_and_spares_a_dead_contract`,
/// which measures the surviving fraction of the raw mean rather than the floor.
fn bernoulli_variance_floor(effective_n: f64) -> f64 {
    // NaN or a non-positive effective size means no usable evidence, so take
    // the Bernoulli maximum. Written with `is_finite` rather than a negated
    // comparison so clippy's partial-order lint is satisfied.
    //
    // UNREACHABLE from the only current caller, which calls this per entry
    // only after `replicated()` has already required an effective size of at
    // least `MIN_EFFECTIVE_N`. It is kept because the function is a named,
    // documented formula rather than an inlined expression, and returning the
    // Bernoulli maximum is the right answer for a caller that has no evidence
    // at all. It is not dead code to be deleted on a coverage argument, and it
    // is deliberately not pinned by a test, because a test of it would assert
    // only that this branch exists.
    if !effective_n.is_finite() || effective_n <= 0.0 {
        return 0.25;
    }
    1.0 / (effective_n + 2.0)
}

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

    /// The forgetting horizons this target's stage selects among.
    fn horizons(self) -> [Option<f64>; HORIZONS] {
        match self {
            Target::Failure => FAILURE_HORIZONS_HOURS,
            Target::LogResponseTime | Target::LogTransferSpeed => LOG_HORIZONS_HOURS,
        }
    }

    /// Whether this target's stage carries a contract term. Only failure: a
    /// dead contract is a failure-rate phenomenon, and the timing stages must
    /// stay bit-identical to the estimator they were validated as.
    fn has_contract_term(self) -> bool {
        match self {
            Target::Failure => true,
            Target::LogResponseTime | Target::LogTransferSpeed => false,
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
// Contract term (failure stage)
// ---------------------------------------------------------------------------

/// Per-peer entries kept for one contract.
///
/// Measured on the recorded gateway soak (#4485, 2026-09-17): distinct peers
/// per contract over a whole file were p90 2-5, p99 6-12, max 17. A ninth peer
/// replaces the entry with the smallest decayed weight, and only when its own
/// weight is larger. Fixed-size entries make the count bound a byte bound.
const CONTRACT_ENTRIES: usize = 8;

/// Contracts tracked, with batched least-recently-used eviction.
///
/// Note for whoever reads the eviction counter: a contract enters the table on
/// `touch`, which happens for EVERY learned event before the residual is
/// computed, so a contract whose event was then rejected (no curve yet, or a
/// rebase forced a refit first) still occupies a slot and can still be
/// evicted. On a cold node the eviction count therefore includes contracts
/// that never contributed anything.
///
/// Measured on the same soak: at most 214 distinct contracts in one hour and
/// 1,259 in a whole file (about a day). An evicted contract loses only its
/// explanation: its windowed events keep the adjustment they last had.
const CONTRACT_CAPACITY: usize = 1024;

/// An entry counts as present when its count decayed to the query time is at
/// least this, about three contract horizons after its last event.
///
/// The Kish factor is scale-free, so without a presence cut a single event from
/// hours ago would count as full-strength evidence about a contract's CURRENT
/// state. Fixed in the offline prototype (2026-09-17) before tuning.
///
/// Note for tests: the cut compares a stored count against `scale(now)`, which
/// is 1 when no time has passed, so a test built on a clock that never
/// advances cannot exercise it at all and is not coverage of the forgetting
/// behaviour. Router-level tests inject a mock clock and mostly leave it
/// still; the tests that do exercise the cut
/// (`the_presence_cut_stops_stale_evidence_from_explaining_a_contract`,
/// `an_event_outside_the_presence_window_keeps_its_stored_adjustment`) drive
/// the stage directly with advancing estimator hours.
const CONTRACT_PRESENCE: f64 = 0.05;

/// Forgetting horizon of the contract table, in hours.
///
/// Tuned with [`CONTRACT_MIN_OTHER_PEERS`] on the soak's tuning window only
/// (half 1 before 2026-09-16 00:00 CDT), by a selection rule written before any
/// tuning run, over {0.25, 0.5, 1.5, 6} h x {1, 2} peers; frozen before the
/// gate window was scored (2026-09-17, `PLAN-v2` gate).
const CONTRACT_HORIZON_HOURS: f64 = 0.5;

/// Other peers that must be present on a contract before a peer's learned
/// residual is adjusted for it. Tuned with [`CONTRACT_HORIZON_HOURS`].
///
/// The forecast's shared effect needs `CONTRACT_MIN_OTHER_PEERS + 1` present
/// peers (see [`ContractTable::shared_effect`]), which is NOT the same
/// evidence bar, and an earlier version of this comment said it was. Learning
/// excludes the acting peer and so counts 2 others; the forecast includes
/// every present peer and so counts 3. That leaves a band in which a residual
/// is explained away for learning while the forecast never gets the
/// explanation back. Measured with the gate's own dead-contract definition on
/// the tuning streams: of gateway-1's 63 dead-contract failures all 63 were
/// adjusted for learning and 18 (29%) were eligible for the forecast offset;
/// of gateway-2's 5, all were adjusted and none was eligible. In that band the
/// model is in the "explain away only" configuration, which the offline gate
/// measured at failure Brier 1.758 [1.213, 2.529] and rejected. Two other
/// forms were considered and neither is free: applying the shared offset
/// wherever the learning adjustment applied re-introduces a leave-one-out
/// asymmetry at the forecast, and raising the learning bar to 3 others cuts
/// the mechanism's coverage further. The choice between them needs a measured
/// comparison and is left open (#5700).
///
/// What the bar buys, and what it costs in coverage: the failure rate falls
/// monotonically with present-peer count on the recorded soak (0.503 at zero
/// present peers, 0.041 at four or more), so requiring two other present peers
/// is what stops a single bad peer reading as a dead contract. The price is
/// that 83.5% of gateway-1's failures and 95.4% of gateway-2's are on
/// contracts that can never be adjusted or offset. The mechanism therefore
/// covers a small share of the failure population, not the population.
///
/// **A SECOND, INDEPENDENT REASON THE TERM IS SILENT, which is not part of
/// that figure.** Measured 2026-09-18, and it had been written down nowhere:
/// *a contract whose peers DISAGREE sharply contributes its spread to
/// `tau2_peer`, not to `tau2_contract`, so the term produces no effect for
/// that contract unless OTHER contracts supply the between-contract
/// variance.* A table holding only a contract where one peer fails and two
/// succeed gives `tau2_contract` exactly 0 with two qualifying contracts and a
/// pooled `tau2_peer` of 0.2275: the noise the estimator subtracts exceeds the
/// between-contract contrast, and `effect` then returns `None` for every
/// query. That is the estimator behaving as designed, but note which case it
/// removes, because it is the unlucky one: one peer failing a contract other
/// peers serve is exactly the shape the leave-one-out exists for.
///
/// It is a DIFFERENT gate from the present-peer bar above, so it compounds
/// rather than overlaps: the 83.5% and 95.4% figures count failures whose
/// contract never reaches [`CONTRACT_MIN_OTHER_PEERS`] present peers, while
/// this one applies to the remainder, where the peers ARE present. How much of
/// that remainder it removes is NOT measured: the counters for it
/// (`den_below_two_refits`, and `tau2_contract` reaching exactly zero) are
/// per-REFIT and cannot be converted to a share of failure events. So the true
/// coverage is at most what the present-peer bar leaves, and by an unmeasured
/// margin less.
///
/// **What the per-refit counters say about it**, which is the part that IS
/// measured. Over 796 refits on the four recorded streams, 595 were estimable;
/// of those, 158 had fewer than two qualifying contracts and a further 152 had
/// `den >= 2` with `tau2_contract` computing to exactly zero. **So the term
/// could act on 285 of 796 refits, 35.8%.** Do not quote the estimable count
/// on its own: "estimable at 595 refits" reads as the term working everywhere
/// and it is not. Read `estimable_refits` with `den_below_two_refits` beside
/// it, and remember the remaining gap (an estimable refit with `den >= 2`
/// whose `tau2_contract` is still zero) is visible only as a zero `tau2`.
///
/// **The coverage asymmetry is a property of the WINDOW's traffic, not of the
/// mechanism.** This is the most important thing measured about it, and it was
/// found only by re-running the counters on the gate streams rather than the
/// tuning streams. Pooled over the four gate streams, the adjustment lands on
/// 12.1% of successes and **23.9% of failures**, a ratio of 0.51x; on the
/// tuning window the same counters give 22.4% and 14.9%, a ratio of 1.51x the
/// other way. Per stream the direction ranges from 0.16x (failures adjusted
/// six times as often as successes) to 3.12x. So a figure measured on one
/// window does not describe another, and the round-1 finding is correct about
/// the tuning window and does not carry to the gate window. That is not
/// evidence the mechanism is unbiased: a coverage difference in either
/// direction shifts what the peer levels learn.
///
/// **The number the bar asymmetry should be judged on.** An earlier version of
/// this paragraph gave -0.010665 over 3,537 successes and +0.128886 over 343
/// failures, and compared their differential against a net shift of +0.00167.
/// Every one of those figures came from the LIVE learn path, which is the
/// population `c7d8a3965` retracted for covering only a small fraction of the
/// adjustment an event ends up carrying; leaving the conditional means on that
/// population while retracting the net was inconsistent, and the comparison
/// overstated itself by about 15x. Re-measured over the LAST REFIT's prepared
/// window, which is the population the mechanism actually reaches, pooled over
/// the four gate streams at the shipping model:
///
/// - successes: the adjustment is non-zero on 17.3% of them, mean **-0.012622**
///   over all of them;
/// - failures: non-zero on 44.4%, mean **+0.357436** over all of them;
/// - outcome differential **+0.370058**, against a NET amount subtracted per
///   event of **+0.026273**, so the differential is about **14x** the net, not
///   the two orders of magnitude previously claimed.
///
/// The levels learn `residual - effect`, so an outcome-correlated adjustment
/// attenuates the slope of learned residual against true failure rate, and
/// that is the harm this bar is judged on. The net shift is the least harmful
/// component by comparison: it moves every peer equally, is partly returned by
/// `shared_effect`, and M2 cancels it exactly by construction. These are still
/// upper bounds at the level input. Counter-evidence, which cuts the other way
/// and must be reported with them: on the gate window the candidate's
/// clean-peer success mean is 0.0258 against legacy's 0.0268, so the pedestal
/// is not detectable at the forecast there.
///
/// **And do not cite M3's PASS as reassurance about the bar asymmetry.** The
/// asymmetry lands on the OPENING event of each contract episode, because a
/// contract reaches three present entries only after the third peer's first
/// event while learning needs two others, which is exactly the "dead
/// contract's FIRST failures" the refit exists to clear. Those events are in
/// the rejected explain-away-only configuration, and M3 is arithmetically
/// incapable of seeing them: 63 events in 15,141 rows is 0.42%, so a penalty
/// of that configuration's size moves the Brier ratio by about 0.003 against a
/// confidence-interval half-width of 0.07.
const CONTRACT_MIN_OTHER_PEERS: usize = 2;

/// One peer's forgotten residual moments on one contract.
#[derive(Debug, Clone, Copy, PartialEq)]
struct ContractEntry {
    /// `u32::MAX` for an unused entry. Never a valid peer slot: slots are
    /// bounded by the peer table's capacity.
    peer_slot: u32,
    peer_generation: u32,
    moments: Moments,
}

impl Default for ContractEntry {
    fn default() -> Self {
        ContractEntry {
            peer_slot: u32::MAX,
            peer_generation: 0,
            moments: Moments::default(),
        }
    }
}

impl ContractEntry {
    fn used(&self) -> bool {
        self.peer_slot != u32::MAX
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct ContractNode {
    entries: [ContractEntry; CONTRACT_ENTRIES],
}

/// Method-of-moments variance components of the contract table, fixed at refit.
#[derive(Debug, Clone, Copy, PartialEq)]
struct ContractComponents {
    /// Pooled within-(contract, peer) variance.
    sigma2: f64,
    /// Between-peer variance within a contract.
    tau2_peer: f64,
    /// Between-contract variance of contract effects about the curve.
    tau2_contract: f64,
    /// Whether [`bernoulli_variance_floor`] was the BINDING value for
    /// `sigma2`, rather than the measured `ss / df`. Exported as a per-refit
    /// count, because it is the signal that tells a future reader whether the
    /// reasoning behind the floor still holds on their traffic: on the
    /// recorded gateway streams it bound on 595 of 595 estimable refits, and
    /// on 285 of the 285 of those on which `tau2_contract` was non-zero, so
    /// the figure does not depend on whether the inert refits are counted. If
    /// traffic ever makes the measured value bind instead, that must be
    /// visible without a replay rig.
    floor_bound: bool,
    /// Whether fewer than two contracts qualified, so `tau2_contract` is zero
    /// and every `effect` query on this refit returns `None`. An estimable
    /// refit that produced nothing.
    den_below_two: bool,
    /// Contracts that qualified for `tau2_contract` (the `den` of its average)
    /// and present entries that informed any component. Diagnostic only, never
    /// read by the model. `tau2_contract` has NO minimum group count, so
    /// without these a value resting on ONE contract reads the same as one
    /// resting on eighty, and the one-contract regime is the measured reality
    /// on quiet traffic and in every stage-level test that draws a unique
    /// contract per event.
    qualifying_contracts: u64,
    qualifying_entries: u64,
}

/// Which entries of a contract a query reads.
#[derive(Debug, Clone, Copy)]
enum ContractQuery {
    /// Every present peer except this one (slot, generation): the adjustment
    /// of that peer's own learned residual.
    LeaveOut(Option<(u32, u32)>),
    /// Every present peer: the effect a forecast adds, identical for every
    /// candidate peer.
    Shared,
}

/// Contract -> (contract, peer) hierarchy at one forgetting horizon (module
/// docs, "Contract term").
#[derive(Debug, Clone)]
struct ContractTable {
    epoch: f64,
    table: PeerTable<u64>,
    /// Indexed by contract slot.
    nodes: Vec<ContractNode>,
    components: Option<ContractComponents>,
    evicted: Vec<usize>,
    /// Live residuals not recorded because every entry of their contract held
    /// a larger weight. The events still train the levels and the curve.
    refused: u64,
    /// `(contract, peer)` pairs the LAST refit dropped because their contract
    /// had more than [`CONTRACT_ENTRIES`] peers in the window and they were
    /// not among the heaviest.
    ///
    /// A GAUGE, not a total, and deliberately so: a persistently over-full
    /// contract contributes the same drops at EVERY refit, about a hundred
    /// times over a full window, so a cumulative count would read roughly 100x
    /// the number of distinct pairs ever dropped, next to `refused`, which is
    /// once per event. Reset at each refit.
    pairs_refused_last_refit: u64,
    /// Live entries DISPLACED by a heavier newcomer, whose accumulated moments
    /// were discarded. Counted rather than merged: merging would attribute the
    /// displaced peer's residuals to the newcomer, which is the attribution
    /// error the whole term exists to avoid. This is the only
    /// evidence-destroying path with no repair, and between refits it can push
    /// a contract's present-entry count below the minimum and switch the term
    /// off for that contract for up to one refit interval.
    displaced: u64,
    /// Refits after which the variance components were estimable.
    ///
    /// NOT on its own an "is the term doing anything" signal: `effect` also
    /// returns `None` per query below the present-peer bar, and on the recorded
    /// soak most failures are on contracts that never reach it. Read it with
    /// `effects_applied` and `forecast_offsets`, which count what moved.
    estimable_refits: u64,
    /// Learned residuals actually adjusted by a contract effect, counted only
    /// where the BOUNDED value was non-zero, so an effect the bound took to
    /// zero does not advance it. Before the 2026-09-18 round-3 review this
    /// incremented whenever an effect was available, which made it an
    /// availability count wearing the name of an application count, and that
    /// is routine rather than rare: a failure event on a mostly-successful
    /// contract gets a negative effect that the bound clamps to zero.
    ///
    /// SCOPE, which the name does not carry: this counts the LIVE learn path
    /// only. `apply_contract_term` re-adjusts every windowed event at each
    /// refit and increments nothing, so this is not the total number of
    /// residuals the term has ever moved.
    effects_applied: u64,
    /// Scored forecasts that actually carried a contract offset. Counted at
    /// learn time, where every event produces a forecast, because the routing
    /// query path takes `&self` under a read lock and cannot count.
    forecast_offsets: u64,
    /// Estimable refits at which [`bernoulli_variance_floor`] was the binding
    /// value for `sigma2` rather than the measured within-cell variance.
    floor_bound_refits: u64,
    /// Estimable refits at which fewer than two contracts qualified, so
    /// `tau2_contract` was zero and the term produced NOTHING although the
    /// refit counted as estimable. The gate's own frequency: without it a
    /// reader cannot tell an estimable refit that acted from one that did not.
    den_below_two_refits: u64,
}

impl ContractTable {
    fn new() -> Self {
        ContractTable {
            epoch: 0.0,
            table: PeerTable::new(CONTRACT_CAPACITY),
            nodes: Vec::new(),
            components: None,
            evicted: Vec::new(),
            refused: 0,
            pairs_refused_last_refit: 0,
            displaced: 0,
            estimable_refits: 0,
            effects_applied: 0,
            forecast_offsets: 0,
            floor_bound_refits: 0,
            den_below_two_refits: 0,
        }
    }

    fn exponent(&self, time: f64) -> f64 {
        (time - self.epoch) / CONTRACT_HORIZON_HOURS
    }

    fn weight(&self, time: f64) -> f64 {
        self.exponent(time).exp()
    }

    fn scale(&self, now: f64) -> f64 {
        (-self.exponent(now).max(0.0)).exp()
    }

    fn reset(&mut self, epoch: f64) {
        self.epoch = epoch;
        for node in &mut self.nodes {
            *node = ContractNode::default();
        }
        self.components = None;
    }

    /// Slot and generation for a contract key, marking it used. Evicted
    /// contracts' nodes are cleared.
    fn touch(&mut self, key: u64) -> (usize, u32) {
        self.evicted.clear();
        let (slot, generation) = self.table.touch(&key, &mut self.evicted);
        for &victim in &self.evicted {
            if let Some(node) = self.nodes.get_mut(victim) {
                *node = ContractNode::default();
            }
        }
        if self.nodes.len() <= slot {
            self.nodes.resize(slot + 1, ContractNode::default());
        }
        (slot, generation)
    }

    fn live(&self, slot: u32, generation: u32) -> bool {
        self.table.generation(slot as usize) == Some(generation)
    }

    /// Add a raw residual to (contract, peer). A peer beyond
    /// [`CONTRACT_ENTRIES`] replaces the entry with the smallest weight, but
    /// only if its own weight is larger; otherwise the residual is refused and
    /// `false` returned.
    #[must_use]
    fn add(&mut self, slot: usize, peer: (u32, u32), weight: f64, residual: f64) -> bool {
        if self.nodes.len() <= slot {
            self.nodes.resize(slot + 1, ContractNode::default());
        }
        let mut displaced = false;
        let entries = &mut self.nodes[slot].entries;
        let index = match entries
            .iter()
            .position(|e| e.used() && (e.peer_slot, e.peer_generation) == peer)
        {
            Some(index) => index,
            None => {
                let index = match entries.iter().position(|e| !e.used()) {
                    Some(index) => index,
                    None => {
                        let mut smallest = 0;
                        for index in 1..CONTRACT_ENTRIES {
                            if entries[index].moments.n < entries[smallest].moments.n {
                                smallest = index;
                            }
                        }
                        if entries[smallest].moments.n >= weight {
                            return false;
                        }
                        displaced = true;
                        smallest
                    }
                };
                entries[index] = ContractEntry {
                    peer_slot: peer.0,
                    peer_generation: peer.1,
                    moments: Moments::default(),
                };
                index
            }
        };
        entries[index].moments.add(weight, residual);
        self.displaced += u64::from(displaced);
        true
    }

    /// Write one contract's entries from already-accumulated per-peer moments,
    /// keeping the [`CONTRACT_ENTRIES`] heaviest, and return how many pairs
    /// were dropped.
    ///
    /// This is the REFIT admission rule, and it is deliberately not [`add`]'s.
    /// `add` fills first-come and then displaces only when one incoming
    /// event's weight exceeds an incumbent's whole accumulated count, so which
    /// peers a contract keeps depends on the order events arrive in. At refit
    /// the window is iterated in [`window_order`], which is ascending DISTANCE,
    /// so a contract with more peers than entries kept a distance-biased
    /// subset, its retained residuals were systematically smaller (the failure
    /// curve rises with distance), and a displaced incumbent was zeroed rather
    /// than merged, which left entries below `replicated()` and could drive
    /// `df < 2` and disable the term. Keeping the heaviest pairs is
    /// independent of iteration order, keeps every event of the pairs it
    /// keeps, and keeps the pairs with the most present evidence, which is
    /// what the variance components need.
    ///
    /// `pairs` is sorted in place. Ties in weight are broken by peer slot, so
    /// the result does not depend on the order pairs were accumulated in.
    ///
    /// A KNOWN asymmetry this creates, stated because it is invisible at the
    /// query site: a peer whose pair was dropped here has no entry, so
    /// `ContractQuery::LeaveOut` excludes nothing for it and it receives the
    /// effect of ALL the kept eight, while a peer that was kept receives the
    /// leave-one-out effect of the other seven. Two classes of peer on one
    /// contract are therefore estimated by slightly different estimators. The
    /// dropped peer is by construction the lightest, so the difference is
    /// small, and the count of drops is exported
    /// (`pairs_refused_last_refit`), but it is not zero and it is not
    /// corrected.
    fn rebuild_node(&mut self, slot: usize, pairs: &mut [(u32, u32, Moments)]) -> usize {
        if self.nodes.len() <= slot {
            self.nodes.resize(slot + 1, ContractNode::default());
        }
        let entries = &mut self.nodes[slot].entries;
        *entries = [ContractEntry::default(); CONTRACT_ENTRIES];
        if pairs.len() > CONTRACT_ENTRIES {
            pairs.sort_unstable_by(|a, b| b.2.n.total_cmp(&a.2.n).then(a.0.cmp(&b.0)));
        }
        let kept = pairs.len().min(CONTRACT_ENTRIES);
        for (entry, &(peer_slot, peer_generation, moments)) in
            entries.iter_mut().zip(pairs.iter().take(kept))
        {
            *entry = ContractEntry {
                peer_slot,
                peer_generation,
                moments,
            };
        }
        pairs.len() - kept
    }

    /// Drop every entry belonging to a peer the peer table has just evicted.
    ///
    /// Without this the entries survive under the old `(slot, generation)`,
    /// `ContractQuery::LeaveOut` excludes only an exact match, and a
    /// reconnecting peer commonly regains the same slot with a new generation
    /// (`PeerTable::touch` pushes the freed slot onto a LIFO free list). Its
    /// own earlier failures would then count as OTHER-peer evidence and toward
    /// [`CONTRACT_MIN_OTHER_PEERS`], so the peer would explain away its own
    /// failures, which is exactly what leaving the peer out exists to prevent.
    /// One pass over the nodes per eviction batch, not per evicted peer.
    /// Counterpart of [`Level::evict`], which does the same bookkeeping for
    /// the peer levels.
    ///
    /// COST, because this runs on the LEARN path under the router's write lock
    /// and the lock measurement in the PR covers the refit rather than this.
    /// The bound is `nodes.len() * CONTRACT_ENTRIES * evicted.len()`
    /// comparisons, so at a full table and the largest batch a peer table of
    /// capacity 4096 produces (`capacity / 64` = 64) it is 1024 * 8 * 64, about
    /// 500k integer comparisons. In production `max_connections` is 200, so
    /// the peer capacity is 400 and the batch is 6, giving about 49k. It runs
    /// only when the peer table is full and a newcomer arrives, and the early
    /// return below means a node with no contract nodes pays nothing. It has
    /// not been measured directly; that gap is stated in the PR rather than
    /// papered over.
    fn evict_peers(&mut self, evicted: &[usize]) {
        if evicted.is_empty() || self.nodes.is_empty() {
            return;
        }
        for node in &mut self.nodes {
            for entry in &mut node.entries {
                if entry.used() && evicted.contains(&(entry.peer_slot as usize)) {
                    *entry = ContractEntry::default();
                }
            }
        }
    }

    /// Variance components by method of moments over the entries present at
    /// the last rebuild (the epoch is the rebuild time, so stored counts are
    /// counts at refit), mirroring [`Level::compute_components`] with peers in
    /// place of bands and contracts in place of peers.
    ///
    /// `tau2_contract` contrasts each contract's mean with ZERO, not with a
    /// grand mean: residuals are defined against the curve, so the curve is the
    /// prior for a contract. A grand mean at a short horizon would be pulled up
    /// by the same dead contracts it is meant to measure.
    fn compute_components(&self) -> Option<ContractComponents> {
        let present = |e: &ContractEntry| e.used() && e.moments.n >= CONTRACT_PRESENCE;
        let (mut ss, mut df) = (0.0, 0.0);
        let mut floor_acc = 0.0;
        let mut entries_present = 0u64;
        for node in &self.nodes {
            for e in node.entries.iter().filter(|e| present(e)) {
                entries_present += 1;
                if e.moments.replicated() {
                    let cell_df = e.moments.n - e.moments.w2 / e.moments.n;
                    ss += (e.moments.sumsq - e.moments.sum * e.moments.sum / e.moments.n).max(0.0);
                    df += cell_df;
                    floor_acc += cell_df * bernoulli_variance_floor(e.moments.effective_n());
                }
            }
        }
        if df < 2.0 {
            return None;
        }
        // WHY `sigma2` IS FLOORED, and the direction of the error it costs.
        // Settled 2026-09-17 after the round-2 review (finding F) and the
        // measurements that answered it; keep this argument with the code.
        //
        // The review first proposed REFUSING whenever `ss / df` lands on the
        // `MIN_SIGMA2` sentinel. That was implemented and measured, and it
        // switches the term off on its own target case: a contract several
        // peers fail unanimously at one distance has IDENTICAL residuals in
        // every cell, so `ss` is exactly 0 by construction, and the whole
        // contract-term suite went dark. `ss = 0` cannot distinguish "no
        // within-cell information" from "unanimous evidence".
        //
        // What separates them is evidence QUANTITY, which is where the noise
        // term already looks. The failure target is a PROBABILITY, so an
        // outcome's within-cell variance is `p(1-p)` and never zero, and
        // unanimity in a small sample is what a moderate `p` looks like.
        // `MIN_SIGMA2` is a numerical sentinel standing in for that
        // statistical quantity, which is why it behaved so badly.
        // [`bernoulli_variance_floor`] replaces it. `max(measured, floor)`
        // keeps the empirical estimate available: if a cell ever holds many
        // MIXED outcomes, `ss / df` rises above the floor and binds instead.
        //
        // MEASURED on the recorded gateway streams: the floor is the binding
        // value on 595 of 595 estimable refits, with the measured `ss / df`
        // 23 to 623 times smaller. Note that 595 is the ESTIMABLE count and
        // not the count of refits on which the term could act, which is 285 of
        // 796 (see [`CONTRACT_MIN_OTHER_PEERS`]); restricting to those 285 the
        // floor still bound on all of them, so the conclusion does not rest on
        // pooling the inert refits. That is a statement about the DATA, not a
        // flaw: `ss / df` estimates a within-(contract, peer) variance from
        // cells that are mostly unanimous and mostly tiny, so it converges on
        // zero for a reason unrelated to the true variance, and that
        // structural zero is what made the unshrunk path reachable.
        // `floor_bound_refits` exports the frequency, so a future reader can
        // see whether this still holds on their traffic without a rig.
        //
        // THE ERROR HAS A DIRECTION, and a reader needs to know it: for a
        // genuinely dead contract the true Bernoulli variance goes to zero as
        // `p` goes to 1, while the Laplace floor stays at `1/(n_eff + 2)`. So
        // the floor OVER-shrinks exactly the case the term exists for. The
        // size is tolerable at realistic evidence, which is what
        // `the_evidence_floor_shrinks_thin_evidence_and_spares_a_dead_contract`
        // pins: at six peers with eight events each the effect keeps 99.7% of
        // the raw mean, against 92.3% for two barely-replicated cells.
        // The failure target is a PROBABILITY, so the within-cell variance of
        // an outcome is `p(1-p)`, never zero: unanimity in a small sample is
        // the expected appearance of a moderate `p`, not evidence of a
        // degenerate one. `MIN_SIGMA2` is a numerical sentinel standing in for
        // that statistical quantity, which is why it behaves so badly here (a
        // contract several peers fail unanimously at one distance has
        // IDENTICAL residuals, so `ss` is exactly 0 by construction). Floor it
        // on evidence QUANTITY instead, which is what separates the two cases:
        // the degenerate one is a couple of barely-replicated cells crossing
        // `df >= 2` in a table of any size, the target one is several peers
        // each seen repeatedly on one contract. See
        // [`bernoulli_variance_floor`]. Contract table only: the timing stages
        // model logs rather than probabilities and have no contract table.
        //
        // EVERY SITE OF THIS SHAPE, and which one this change touches. The
        // round-3 review was right that the sentence above explains the timing
        // stages and not the failure stage's own peer levels, so here is the
        // enumeration the repo's rules ask for.
        //
        // 1. THIS site, `ContractTable::compute_components`'s `sigma2`.
        //    FIXED, by [`bernoulli_variance_floor`].
        // 2. `Level::compute_components`'s `sigma2`, which the failure
        //    stage's peer and (peer, band) levels use. Same 0/1 target, same
        //    structural zero from unanimous cells, still floored at the
        //    `MIN_SIGMA2` numerical sentinel. NOT FIXED, and the reason is
        //    NOT that the argument fails to apply: it applies exactly. It is
        //    that those levels are the estimator the #5655 bake-off and every
        //    number in the promotion gate were measured on, so changing their
        //    variance floor changes the baseline this PR is judged against and
        //    belongs in its own change with its own re-score. Recorded on
        //    #5700 rather than left as an unstated choice.
        // 3. The same `Level::compute_components` reached by the response-time
        //    and transfer-speed stages. NOT FIXED and should not be: those
        //    targets are logs of positive quantities, so there is no Bernoulli
        //    variance to floor on, and the timing forecasts are required to
        //    stay bit-identical.
        // 4. `tau2_peer` below, gated at `den > 0.0` while `tau2_contract` is
        //    gated at `den >= 2.0`. The argument for the tighter gate ("a
        //    between-group variance estimated from one group is not a
        //    variance") does apply to a `tau2_peer` resting on a single
        //    leave-one-out contrast. NOT FIXED here, for two reasons worth
        //    stating separately: tightening it changes behaviour and so
        //    invalidates the gate row this PR reports, and the two `den`s are
        //    not the same quantity (this one sums `1 + rest_sq/rest.n^2` over
        //    every replicated present entry in the WHOLE table, not one term
        //    per contract), so "at least two" is not the same threshold and
        //    the right value needs measuring rather than copying. Because it
        //    is a table-wide sum, a single-contrast `den` is reachable only
        //    when the entire table holds one replicated present entry, which
        //    is the same warm-up regime as the `den >= 2.0` gate below.
        //    Recorded on #5700 as a decision for the lead, not silently kept.
        let floor = floor_acc / df;
        let measured = ss / df;
        let sigma2 = measured.max(floor);
        // Written as `<=` rather than as a negated `>`, so clippy's
        // partial-order lint is satisfied; a NaN measured value also reads as
        // floor-bound, which is the right answer because `max` then returns
        // the floor.
        let floor_bound = !measured.is_finite() || measured <= floor;

        // tau2_peer: each replicated entry against the SUM of the other present
        // entries of its contract (leave-one-out, summed rather than
        // subtracted; see `Level::compute_components`).
        let (mut acc, mut den) = (0.0, 0.0);
        for node in &self.nodes {
            for (index, e) in node.entries.iter().enumerate() {
                if !present(e) || !e.moments.replicated() {
                    continue;
                }
                let mut rest = Moments::default();
                let mut rest_sq = 0.0;
                for (other_index, other) in node.entries.iter().enumerate() {
                    if other_index != index && present(other) {
                        rest.n += other.moments.n;
                        rest.w2 += other.moments.w2;
                        rest.sum += other.moments.sum;
                        rest_sq += other.moments.n * other.moments.n;
                    }
                }
                if rest.n <= NODE_MIN {
                    continue;
                }
                let contrast = e.moments.mean() - rest.mean();
                let noise =
                    sigma2 * (e.moments.mean_variance_factor() + rest.w2 / (rest.n * rest.n));
                acc += contrast * contrast - noise;
                den += 1.0 + rest_sq / (rest.n * rest.n);
            }
        }
        let tau2_peer = if den > 0.0 { (acc / den).max(0.0) } else { 0.0 };

        let (mut acc, mut den) = (0.0, 0.0);
        for node in &self.nodes {
            let mut total = Moments::default();
            let mut sq = 0.0;
            for e in node.entries.iter().filter(|e| present(e)) {
                total.n += e.moments.n;
                total.w2 += e.moments.w2;
                total.sum += e.moments.sum;
                sq += e.moments.n * e.moments.n;
            }
            if !total.replicated() {
                continue;
            }
            let n2 = total.n * total.n;
            let mean = total.mean();
            let noise = sigma2 * total.w2 / n2 + tau2_peer * sq / n2;
            acc += mean * mean - noise;
            den += 1.0;
        }
        // At least TWO qualifying contracts, for the same reason as the
        // `df >= 2.0` gate two blocks above: a between-group variance
        // estimated from ONE group is not a variance, it is that group's own
        // mean, and it then un-shrinks every other contract's effect.
        //
        // WHAT THIS COSTS, measured PER REFIT over the recorded gateway files
        // rather than on average, because a gate is a threshold and an average
        // hides it. `den < 2` fires on 62 of gw1-h1's 283 refits (21.9%) and
        // 96 of gw1-rc's 236 (40.7%), and never on the two gw2 streams. Every
        // firing is CONSECUTIVE FROM THE FIRST REFIT and none recurs
        // afterwards, with `den` exactly 1 throughout and never 0. So this is
        // a WARM-UP property: the term is off for a freshly started node's
        // first 62 to 96 refits, several thousand events, and then never
        // again. During that window the node genuinely holds fewer than two
        // qualifying contracts, so the alternative is a between-contract
        // variance estimated from one contract, which is the defect this
        // gates. It sits alongside the estimator's other warm-up rules
        // ([`MIN_CURVE_POINTS_FAILURE`], [`EAGER_REFIT_BELOW`]), and every
        // node loses this state on restart in any case.
        //
        // An earlier version of this comment said "on the recorded gateway
        // streams this costs nothing (52 to 76 qualifying contracts
        // measured)". 52.0 is gw1-h1's MEAN `den`, so that was right about the
        // steady state and silent about the warm-up.
        //
        // WHAT IS MEASURED, AND WHAT IS NOT GUARANTEED. On the four recorded
        // streams every firing happened to be consecutive from the first refit
        // and none recurred. That is an OBSERVATION about those streams, on
        // two of the four, and NOT a property of the gate: `den` is recomputed
        // from the current window at every refit, so a peer relaying one hot
        // contract, or a node quiet enough that only one contract clears the
        // replication bar in a window, sits at `den = 1` indefinitely with the
        // term off. Do not read "warm-up" as a bound on how long this lasts.
        //
        // `den_below_two_refits` counts the firings and `qualifying_contracts`
        // reports the current value, so both the frequency and the regime are
        // observable on a live node without a replay rig. Note that a firing
        // still counts as an ESTIMABLE refit: `components` is `Some` with
        // `tau2_contract` at zero, and `effect` then returns `None` for every
        // query, so the term produced nothing on those refits although
        // `estimable_refits` and `floor_bound_refits` both advanced. The three
        // counters have to be read together; `estimable_refits` alone
        // overstates what the term did.
        let tau2_contract = if den >= 2.0 {
            (acc / den).max(0.0)
        } else {
            0.0
        };

        [sigma2, tau2_peer, tau2_contract]
            .iter()
            .all(|value| value.is_finite())
            .then_some(ContractComponents {
                sigma2,
                tau2_peer,
                tau2_contract,
                floor_bound,
                den_below_two: den < 2.0,
                qualifying_contracts: den as u64,
                qualifying_entries: entries_present,
            })
    }

    /// Shrunk contract effect at `now` from the entries `query` selects, or
    /// `None` without components, without between-contract variance, or with
    /// fewer than `min_peers` present entries.
    ///
    /// `noise = sigma2 * w2/n^2 + tau2_peer * sum_q n_q^2 / n^2`. The `tau2_peer`
    /// term keeps one peer with many failures from reading as a dead contract:
    /// evidence from a single peer carries at least `tau2_peer` of noise however
    /// many events it has.
    fn effect(
        &self,
        slot: Option<usize>,
        query: ContractQuery,
        now: f64,
        min_peers: usize,
    ) -> Option<f64> {
        let components = self.components?;
        if components.tau2_contract <= 0.0 {
            return None;
        }
        let node = self.nodes.get(slot?)?;
        let scale = self.scale(now);
        let (mut n, mut w2, mut sum, mut sq, mut count) = (0.0, 0.0, 0.0, 0.0, 0usize);
        for e in &node.entries {
            if !e.used() {
                continue;
            }
            if let ContractQuery::LeaveOut(Some(peer)) = query {
                if (e.peer_slot, e.peer_generation) == peer {
                    continue;
                }
            }
            if e.moments.n * scale < CONTRACT_PRESENCE {
                continue;
            }
            count += 1;
            n += e.moments.n;
            w2 += e.moments.w2;
            sum += e.moments.sum;
            sq += e.moments.n * e.moments.n;
        }
        if count < min_peers || n <= 0.0 || w2 <= 0.0 {
            return None;
        }
        let n2 = n * n;
        let noise = components.sigma2 * w2 / n2 + components.tau2_peer * sq / n2;
        let shrink = components.tau2_contract / (components.tau2_contract + noise);
        let value = shrink * sum / n;
        value.is_finite().then_some(value)
    }

    /// Adjustment for `peer`'s own residual on the contract in `slot`.
    fn leave_out_effect(
        &self,
        slot: Option<usize>,
        peer: Option<(u32, u32)>,
        now: f64,
    ) -> Option<f64> {
        self.effect(
            slot,
            ContractQuery::LeaveOut(peer),
            now,
            CONTRACT_MIN_OTHER_PEERS,
        )
    }

    /// Effect a forecast adds for the contract with location bits `key`.
    ///
    /// The same value for every candidate peer, so it never changes which
    /// candidate has the higher failure PROBABILITY. It can still change which
    /// peer the router SELECTS, because the cost formula multiplies the
    /// probability by each candidate's own response time; see the module docs,
    /// "Contract term", Forecast, for the arithmetic and the measured
    /// magnitudes. A leave-one-out effect here would reorder the probabilities
    /// themselves, and in the wrong direction: it excludes the failing peer's
    /// own failures and includes the succeeding peer's successes, which on the
    /// tuning window ranked the failing peer LOWER in 45 of 53 pairs.
    fn shared_effect(&self, key: u64, now: f64) -> Option<f64> {
        let slot = self.table.lookup(&key)?;
        self.effect(
            Some(slot),
            ContractQuery::Shared,
            now,
            CONTRACT_MIN_OTHER_PEERS + 1,
        )
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
    /// Contract-table slot and generation; `u32::MAX` for a stage without a
    /// contract term.
    contract_slot: u32,
    contract_generation: u32,
    /// The contract effect last subtracted from this event's residual before
    /// it entered the levels. Kept when a later refit finds no present
    /// evidence for the contract (module docs, "Contract term").
    ///
    /// `f32` to keep [`Event`] inside its byte budget. The rounding is about
    /// 6e-8 relative, four orders below the smallest effect that changes a
    /// forecast, but it is a ROUNDING: two refits that compute the same `f64`
    /// effect store the same `f32`, while `stored != 0.0` in the sign-flip
    /// guard tests the rounded value, so an effect below about 1e-45 stores as
    /// 0 and is treated as "never adjusted". That is far below
    /// [`CONTRACT_PRESENCE`]-scale effects and is noted rather than guarded.
    adjustment: f32,
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
///
/// The failure stage's contract table adds at most `CONTRACT_CAPACITY x
/// CONTRACT_NODE_BYTES` (327,680 bytes) of nodes, plus its key table (a
/// `HashMap<u64, usize>` and per-slot bookkeeping, about 80 KB at capacity).
/// The shared [`Scratch`] also carries the contract rebuild's two buffers, at
/// most `WINDOW_EVENTS x 24` bytes (240 KB) of events and one entry per
/// `(contract, peer)` pair of the contract being written. Those are held once
/// per router, not once per stage, and only the failure stage fills them.
const EVENT_BYTES: usize = 56;
const PREPARED_BYTES: usize = 40;
const PEER_NODE_BYTES: usize = 296;
const LEVEL_BYTES: usize = 136;
const CONTRACT_NODE_BYTES: usize = 320;
const _: () = {
    assert!(std::mem::size_of::<Event>() <= EVENT_BYTES);
    assert!(std::mem::size_of::<Prepared>() <= PREPARED_BYTES);
    assert!(std::mem::size_of::<PeerNode>() <= PEER_NODE_BYTES);
    assert!(std::mem::size_of::<Level>() <= LEVEL_BYTES);
    assert!(std::mem::size_of::<ContractNode>() <= CONTRACT_NODE_BYTES);
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
    /// Live contract-table slot, or `u32::MAX`.
    contract_slot: u32,
    /// Index of the source event in the sorted window, where a refit stores
    /// the event's new adjustment.
    source: u32,
    band: u8,
}

/// Buffers a refit needs, owned once and shared by every stage so no stage
/// keeps a window-sized allocation of its own between refits.
#[derive(Debug, Clone, Default)]
pub(crate) struct Scratch {
    prepared: Vec<Prepared>,
    evicted: Vec<usize>,
    /// One entry per windowed event the contract-table rebuild admits:
    /// `(contract_slot << 32 | peer_slot, weight, residual)`. Sorted by key
    /// so that a contract's events, and within a contract a peer's events,
    /// are contiguous.
    contract_events: Vec<(u64, f64, f64)>,
    /// One `(peer_slot, peer_generation, moments)` per peer of the contract
    /// currently being written.
    contract_pairs: Vec<(u32, u32, Moments)>,
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
    /// `value` before the stage's output bound. For failure this can leave
    /// `[0, 1]`; it is what peers are RANKED by (see
    /// [`ranking_failure_probability`]), so a forecast that clamps at 1 for two
    /// peers does not tie them. Equal to `value` wherever the bound does not
    /// bind.
    pub unbounded: f64,
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
    /// Contracts held by the failure stage's contract table; 0 for a stage
    /// without one.
    pub contracts: usize,
    /// Contracts evicted from that table.
    pub contract_evictions: u64,
    /// Residuals the contract table did not record LIVE because every entry
    /// of their contract held a larger weight.
    pub contract_residuals_refused: u64,
    /// `(contract, peer)` pairs the LAST refit dropped for being outside
    /// their contract's heaviest [`CONTRACT_ENTRIES`]. A GAUGE, not a total.
    pub contract_pairs_refused_last_refit: u64,
    /// Live entries displaced by a heavier newcomer, whose accumulated moments
    /// were discarded. The only evidence-destroying path with no repair.
    pub contract_entries_displaced: u64,
    /// Refits after which the contract term's variance components were
    /// estimable. Necessary for the term to do anything and NOT sufficient:
    /// `effect` also returns nothing per query below the present-peer bar.
    pub contract_estimable_refits: u64,
    /// Learned residuals actually adjusted by a contract effect, and scored
    /// forecasts that actually carried a contract offset. These, not
    /// `contract_estimable_refits`, distinguish a term that worked from one
    /// that was estimable and moved nothing.
    ///
    /// `contract_effects_applied` counts only where the bound left a non-zero
    /// adjustment, and only on the LIVE learn path: the per-refit re-adjustment
    /// of the whole window is not counted anywhere.
    pub contract_effects_applied: u64,
    pub contract_forecast_offsets: u64,
    /// Estimable refits at which the Bernoulli evidence floor was the binding
    /// value for `sigma2` rather than the measured within-cell variance. On
    /// the recorded gateway streams this equals `contract_estimable_refits`;
    /// a reader whose traffic differs should be able to see that here.
    pub contract_floor_bound_refits: u64,
    /// Estimable refits on which fewer than two contracts qualified, so
    /// `tau2_contract` was zero and the term produced nothing. Subtract this
    /// from `contract_estimable_refits` for the refits that could act.
    pub contract_den_below_two_refits: u64,
    /// Contracts that qualified for `tau2_contract` at the last refit, and
    /// present entries that informed the components.
    pub contract_qualifying_contracts: u64,
    pub contract_qualifying_entries: u64,
    /// Between-contract variance of the contract term at the last refit;
    /// `None` when the components are not estimable. The term produces no
    /// effect at all while this is absent or zero.
    pub contract_tau2: Option<f64>,
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
    /// The contract term, for a target that has one.
    contracts: Option<Box<ContractTable>>,
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
            levels: target.horizons().map(Level::new),
            loss: [0.0; HORIZONS],
            loss_time: None,
            clock: f64::NEG_INFINITY,
            refits: 0,
            rejected: 0,
            orphaned_at_last_refit: 0,
            residual_shape: ResidualShape::default(),
            contracts: target
                .has_contract_term()
                .then(|| Box::new(ContractTable::new())),
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
        let unbounded = prior + posterior.map_or(0.0, |p| p.mean);
        let value = self.bound(unbounded);
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
        Forecast {
            value,
            unbounded,
            spread,
        }
    }

    /// The prior a forecast descends from: the curve at `distance`, plus the
    /// contract's shared effect where the stage has a contract term and the
    /// contract has enough present evidence.
    fn forecast_prior(&self, prior: f64, contract_location: f64, now: f64) -> f64 {
        match self
            .contracts
            .as_ref()
            .and_then(|table| table.shared_effect(contract_location.to_bits(), now))
        {
            Some(effect) => prior + effect,
            None => prior,
        }
    }

    /// Forecast on the target's scale. `None` until the stage has a curve.
    ///
    /// `O(1)` in the window: a hash lookup in the peer table, one binary
    /// search over the curve's blocks, the selected horizon's arithmetic, and
    /// for a stage with a contract term a second hash lookup plus a scan of
    /// one contract's [`CONTRACT_ENTRIES`] entries.
    pub(crate) fn predict(
        &self,
        peer: &K,
        contract_location: f64,
        distance: f64,
        now: f64,
    ) -> Option<Forecast> {
        let now = self.effective_time(now);
        let prior = self.forecast_prior(self.prior(distance)?, contract_location, now);
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
            let offset_prior = self.forecast_prior(prior, contract_location, now);
            if offset_prior != prior {
                if let Some(table) = self.contracts.as_mut() {
                    table.forecast_offsets += 1;
                }
            }
            let prior = offset_prior;
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
        if let Some(table) = self.contracts.as_mut() {
            table.evict_peers(&scratch.evicted);
        }
        let (contract_slot, contract_generation) = match self.contracts.as_mut() {
            Some(table) => {
                let (contract_slot, contract_generation) = table.touch(contract_location.to_bits());
                (contract_slot as u32, contract_generation)
            }
            None => (u32::MAX, 0),
        };
        self.fresh.push(Event {
            distance,
            y,
            time: now,
            seq: self.next_seq,
            slot: slot as u32,
            generation,
            contract_slot,
            contract_generation,
            adjustment: 0.0,
            band: band as u8,
        });
        self.next_seq += 1;
        self.since_refit += 1;

        let mut must_rebase = false;
        if let Some(prior) = prior {
            must_rebase = self
                .levels
                .iter()
                .any(|level| level.exponent(now) > REBASE_EXPONENT)
                || self
                    .contracts
                    .as_ref()
                    .is_some_and(|table| table.exponent(now) > REBASE_EXPONENT);
            if !must_rebase {
                let residual = y - prior;
                let mut adjusted = residual;
                if let Some(table) = self.contracts.as_mut() {
                    let peer_id = (slot as u32, generation);
                    // The effect from OTHER peers as the table stands, before
                    // this event joins it.
                    if let Some(effect) =
                        table.leave_out_effect(Some(contract_slot as usize), Some(peer_id), now)
                    {
                        // The RAW effect is stored, so a later refit can see
                        // the estimate this event was explained by; the BOUND
                        // is applied where it is used.
                        let bounded = explaining_bound(effect, residual);
                        adjusted = residual - bounded;
                        // Counted only when the residual actually MOVED. An
                        // effect the bound takes to zero (one that opposes the
                        // residual's sign) leaves the levels learning exactly
                        // what they would have learned without the term, and
                        // counting it made this an availability count, which
                        // is what `ring.md` tells the reader it is not.
                        // `forecast_offsets` already counts only what moved.
                        if bounded != 0.0 {
                            table.effects_applied += 1;
                        }
                        if let Some(event) = self.fresh.last_mut() {
                            event.adjustment = effect as f32;
                        }
                    }
                    let weight = table.weight(now);
                    if !table.add(contract_slot as usize, peer_id, weight, residual) {
                        table.refused += 1;
                    }
                }
                for level in &mut self.levels {
                    let weight = level.weight(now);
                    level.add(Some(slot), band, weight, adjusted);
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
            let Scratch {
                prepared,
                contract_events,
                contract_pairs,
                ..
            } = scratch;
            self.apply_contract_term(prepared, contract_events, contract_pairs, now);
            self.rebuild_levels(prepared, now);
        }
        scratch.prepared.clear();
        scratch.contract_events.clear();
        scratch.contract_pairs.clear();
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
        for (source, event) in self.sorted.iter().enumerate() {
            let Some(value) = curve.value_sorted(event.distance, &mut cursor) else {
                continue;
            };
            let live = self.peers.generation(event.slot as usize) == Some(event.generation);
            if !live {
                orphaned += 1;
            }
            let contract_live = self
                .contracts
                .as_ref()
                .is_some_and(|table| table.live(event.contract_slot, event.contract_generation));
            prepared.push(Prepared {
                residual: event.y - self.bound(value),
                time: event.time,
                weight: 1.0,
                slot: if live { event.slot } else { u32::MAX },
                contract_slot: if contract_live {
                    event.contract_slot
                } else {
                    u32::MAX
                },
                source: source as u32,
                band: event.band,
            });
        }
        self.orphaned_at_last_refit = orphaned;
        true
    }

    /// Rebuild the contract table from the prepared window against the new
    /// curve, recompute its components, and re-adjust every prepared residual
    /// before the levels are rebuilt from them. Linear in the window, plus one
    /// sort of it.
    ///
    /// Only events whose peer and contract are both still tracked enter the
    /// table. Admission is by accumulated weight, not by the order the window
    /// is iterated in (see [`ContractTable::rebuild_node`]).
    ///
    /// Each event INSIDE the presence window is then adjusted by its peer's
    /// leave-out effect as of this refit, which clears a dead contract's FIRST
    /// failures (learned before any other peer had failed there) from the peer
    /// levels. THREE cases keep the adjustment the event last had instead:
    ///
    /// - the contract has no present evidence from other peers. Otherwise a
    ///   storm's failures would be charged back to the peers once the storm's
    ///   evidence decays out of the table, while the no-forgetting level still
    ///   holds them.
    /// - the EVENT's own decayed weight is below [`CONTRACT_PRESENCE`], i.e.
    ///   it is older than about three contract horizons (1.50 h at a 0.5 h
    ///   horizon). Such an event contributes essentially nothing to the
    ///   estimate it would be re-scored against, so re-scoring it applies a
    ///   contract state estimated from the last 1.5 h at full strength to an
    ///   event from long before it. Measured on the recorded gateway soak at
    ///   one instant: 364 of 593 windowed events on contracts with three or
    ///   more present peers were older than that, 326 of them successes. The
    ///   alternative considered, weighting the adjustment by the event's own
    ///   decayed weight, was rejected because it un-explains a storm's older
    ///   failures as they age, which is what the kept adjustment exists to
    ///   prevent.
    /// - the event's contract SLOT has been evicted and reused, so `prepare`
    ///   reports it as having no contract and it cannot be adjusted by
    ///   whatever now occupies the slot.
    ///
    /// And one case keeps it while still recomputing everything else: an
    /// effect whose SIGN is opposite to the stored one, which means the
    /// contract has changed regime since the event (see the sign-flip guard
    /// below, and [`explaining_bound`]).
    fn apply_contract_term(
        &mut self,
        prepared: &mut [Prepared],
        events: &mut Vec<(u64, f64, f64)>,
        pairs: &mut Vec<(u32, u32, Moments)>,
        now: f64,
    ) {
        let Stage {
            contracts,
            peers,
            sorted,
            ..
        } = self;
        let Some(table) = contracts.as_mut() else {
            return;
        };
        table.reset(now);
        table.pairs_refused_last_refit = 0;
        events.clear();
        for event in prepared.iter() {
            if event.contract_slot == u32::MAX || event.slot == u32::MAX {
                continue;
            }
            if peers.generation(event.slot as usize).is_none() {
                continue;
            }
            let weight = ((event.time - now) / CONTRACT_HORIZON_HOURS).exp();
            let key = (u64::from(event.contract_slot) << 32) | u64::from(event.slot);
            events.push((key, weight, event.residual));
        }
        // Only live peers were pushed, so one generation per slot: the key's
        // low half identifies the pair on its own.
        events.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let mut index = 0;
        while index < events.len() {
            let contract_slot = (events[index].0 >> 32) as usize;
            pairs.clear();
            while index < events.len() && (events[index].0 >> 32) as usize == contract_slot {
                // The WHOLE key, not its low half: two adjacent contracts can
                // hold the same peer slot, and matching on the peer alone
                // would merge the second contract's events into the first.
                let key = events[index].0;
                let peer_slot = key as u32;
                let generation = peers
                    .generation(peer_slot as usize)
                    .expect("only live peers were pushed");
                let mut moments = Moments::default();
                while index < events.len() && events[index].0 == key {
                    moments.add(events[index].1, events[index].2);
                    index += 1;
                }
                pairs.push((peer_slot, generation, moments));
            }
            table.pairs_refused_last_refit += table.rebuild_node(contract_slot, pairs) as u64;
        }
        table.components = table.compute_components();
        if let Some(components) = table.components {
            table.estimable_refits += 1;
            table.floor_bound_refits += u64::from(components.floor_bound);
            table.den_below_two_refits += u64::from(components.den_below_two);
        }
        for event in prepared.iter_mut() {
            let Some(source) = sorted.get_mut(event.source as usize) else {
                continue;
            };
            let peer = (event.slot != u32::MAX)
                .then(|| {
                    peers
                        .generation(event.slot as usize)
                        .map(|g| (event.slot, g))
                })
                .flatten();
            let present = ((event.time - now) / CONTRACT_HORIZON_HOURS).exp() >= CONTRACT_PRESENCE;
            let fresh = (present && event.contract_slot != u32::MAX)
                .then(|| table.leave_out_effect(Some(event.contract_slot as usize), peer, now))
                .flatten();
            let stored = f64::from(source.adjustment);
            let adjustment = match fresh {
                // THE RULE, stated generally: an event keeps the adjustment
                // from its own era whenever the fresh estimate no longer
                // agrees in sign with it. A refit that disagrees in sign is
                // describing a different regime from the one the event was
                // learned in, and re-scoring the event against a state it
                // never saw would move the levels on evidence the event does
                // not carry.
                //
                // An earlier version of this comment named one cause, a
                // contract recovering while the event is still inside the
                // presence window, and that is NOT the case the guard usually
                // fires on, which the round-3 review was right to flag.
                // `stored` is overwritten at every same-sign refit and refits
                // run every [`REFIT_EVERY`] events, so a recovering contract's
                // effect can already be near zero by the refit at which its
                // sign finally turns over.
                //
                // MEASURED, on the four recorded gateway streams at
                // `9ad70135b`, because the review asked what the guard does to
                // the population it actually reaches. 238 firings out of
                // 1,546,012 refit-loop visits (0.0154%), and the review's
                // reading holds for two thirds of them and not for the rest:
                //
                // - `stored > 0` with a fresh `effect < 0`: 159 firings
                //   (66.8%). The frozen value has mean magnitude 0.0114 and
                //   median 0.0055, SMALLER than the fresh effect it displaces
                //   (mean 0.0273). Here the guard is blocking a fresh effect
                //   rather than preserving an era value, which is the review's
                //   point.
                // - `stored < 0` with a fresh `effect > 0`: 79 firings
                //   (33.2%). The frozen value has mean magnitude 0.0657 and
                //   median 0.0708, LARGER than the fresh effect (mean 0.0603),
                //   so for this third the frozen value is not near zero.
                //
                // Mean signed change to what the peer levels learn, positive
                // meaning the levels learn LESS than without the guard:
                // +0.0273 for the first group and -0.0477 for the second, a
                // pooled net of +0.58 over all 238 firings. EVERY firing is on
                // a SUCCESS event (negative residual); the guard did not fire
                // once on a failure on any stream. The per-visit figures do
                // NOT capture the guard's effect on later refits through what
                // `stored` then holds, which would need a counterfactual
                // replay. Full tables in the PR.
                //
                // The scenario test pins the large-value case, which is the
                // guard's best case and, on this evidence, its minority one.
                //
                // `<= 0.0` rather than `< 0.0`: an effect of exactly zero is
                // not agreement, and overwriting a non-zero stored value with
                // it would erase the era estimate the rule exists to keep.
                // Reachable only through an exactly-zero decayed residual sum,
                // since `effect` returns `None` rather than `Some(0.0)` when
                // `tau2_contract` is zero. MEASURED at zero firings on all
                // four recorded streams, so on that traffic this is a
                // robustness change and not a behaviour change. A zero on four
                // streams is not a proof of unreachability.
                Some(effect) if stored != 0.0 && effect * stored <= 0.0 => stored,
                Some(effect) => {
                    source.adjustment = effect as f32;
                    effect
                }
                None => stored,
            };
            event.residual -= explaining_bound(adjustment, event.residual);
        }
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
            selected_horizon_hours: self.target.horizons()[self.selected()],
            active: self.curve.is_some(),
            orphaned_at_last_refit: self.orphaned_at_last_refit,
            residual_sigma2: self.levels[self.selected()].components.map(|c| c.sigma2),
            residual_shape: self.residual_shape,
            contracts: self
                .contracts
                .as_ref()
                .map_or(0, |table| table.table.index.len()),
            contract_evictions: self
                .contracts
                .as_ref()
                .map_or(0, |table| table.table.evictions),
            contract_residuals_refused: self.contracts.as_ref().map_or(0, |table| table.refused),
            contract_pairs_refused_last_refit: self
                .contracts
                .as_ref()
                .map_or(0, |table| table.pairs_refused_last_refit),
            contract_entries_displaced: self.contracts.as_ref().map_or(0, |table| table.displaced),
            contract_estimable_refits: self
                .contracts
                .as_ref()
                .map_or(0, |table| table.estimable_refits),
            contract_effects_applied: self
                .contracts
                .as_ref()
                .map_or(0, |table| table.effects_applied),
            contract_forecast_offsets: self
                .contracts
                .as_ref()
                .map_or(0, |table| table.forecast_offsets),
            contract_floor_bound_refits: self
                .contracts
                .as_ref()
                .map_or(0, |table| table.floor_bound_refits),
            contract_den_below_two_refits: self
                .contracts
                .as_ref()
                .map_or(0, |table| table.den_below_two_refits),
            contract_qualifying_contracts: self
                .contracts
                .as_ref()
                .and_then(|table| table.components)
                .map_or(0, |c| c.qualifying_contracts),
            contract_qualifying_entries: self
                .contracts
                .as_ref()
                .and_then(|table| table.components)
                .map_or(0, |c| c.qualifying_entries),
            contract_tau2: self
                .contracts
                .as_ref()
                .and_then(|table| table.components)
                .map(|components| components.tau2_contract),
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
    /// What routing ranks peers by on failure: [`ranking_failure_probability`]
    /// of the unbounded forecast. Equal to `failure_probability` wherever the
    /// `[0, 1]` bound does not bind.
    pub failure_ranking: Option<f64>,
    /// Expected time to response start, `E[T]`, in seconds.
    pub time_to_response_start_secs: Option<f64>,
    /// Effective transfer speed in bytes/s: the reciprocal of `E[1/speed]`, so
    /// that `bytes / speed` is the expected transfer time.
    pub transfer_speed_bps: Option<f64>,
}

/// The part of `residual` a contract adjustment is allowed to remove.
///
/// Explaining away may neutralise an event's evidence but must never INVERT
/// it: a failure must not be learned as evidence of success, nor a success as
/// evidence of failure. So the adjustment is clamped into the closed interval
/// between 0 and the event's own residual, and the adjusted residual therefore
/// keeps the residual's sign or is exactly zero.
///
/// Two defects this bounds, both from the 2026-09-17 round-2 review:
///
/// - a contract that RECOVERS while an event is still inside the presence
///   window makes `leave_out_effect` return a value of the OPPOSITE sign,
///   dominated by the fresh successes. Subtracting that from a storm-era
///   failure charges the peer MORE than its own raw residual, which is worse
///   than not having the term at all. The sign-flip guard in
///   [`Stage::apply_contract_term`] is the first line against this; the bound
///   is the backstop, and it also covers the live path, where there is no
///   stored value to compare against.
/// - a stored adjustment is an absolute offset in residual space, while
///   `prepare` re-anchors every residual on the CURRENT curve at each refit,
///   so a frozen adjustment and the residual it is subtracted from drift onto
///   different baselines. The bound does not remove that drift; it stops the
///   drift from inverting an event's evidence, which is the harmful end of it.
///   Storing the adjustment curve-relative would remove it and is the
///   alternative not taken here, because it changes what the stored value
///   means on every path at once.
fn explaining_bound(adjustment: f64, residual: f64) -> f64 {
    // BOTH sides are checked, not only the adjustment. `f64::clamp` asserts
    // `min <= max`, so a NaN residual would panic in the branches below rather
    // than return a harmless zero. No caller can currently pass one (a
    // residual is `y - prior` with a finite validated `y`), but this function
    // reads as total and a later caller will assume it is.
    if !adjustment.is_finite() || !residual.is_finite() {
        return 0.0;
    }
    if residual >= 0.0 {
        adjustment.clamp(0.0, residual)
    } else {
        adjustment.clamp(residual, 0.0)
    }
}

/// Slope of [`ranking_failure_probability`] above 1.
///
/// Any positive slope keeps the order of the unbounded forecasts. A small one
/// keeps the router's expected-cost formula within `1e-6` per unit of
/// overshoot of the value it would compute from the clamped probability, so
/// the ranking changes only where the clamp would otherwise create a tie. It
/// is far above the resolution of `f64` at the cost formula's magnitudes.
pub(crate) const RANKING_OVERSHOOT_SLOPE: f64 = 1e-6;

/// Failure value routing ranks peers by: the probability clamped to `[0, 1]`,
/// plus [`RANKING_OVERSHOOT_SLOPE`] times any overshoot ABOVE 1.
///
/// Non-decreasing in the unbounded forecast, and strictly increasing above 1,
/// so two peers whose forecasts both clamp at 1 (a contract whose shared
/// effect is large) are still ordered by the evidence against each peer,
/// instead of tying and falling back to distance order. It stays within a
/// hair of the clamped probability, which matters where the router multiplies
/// it by a response time: using the raw unbounded value there would let a
/// forecast below `-1/3` make a SLOWER peer cheaper. The clamped probability
/// stays what is reported and recorded.
///
/// The DOWNWARD overshoot is deliberately dropped rather than carried with the
/// same slope. Carrying it returned a small negative value for the normal case
/// of a good peer, and the router's no-timing cost branch is
/// `failure * multiplier`, so the cost went negative: the dashboard's
/// expected-total-time formatter prints "N/A" outside `0..1e9`, and negative
/// expected times reached the routing dataset and telemetry. A negative value
/// also cannot be the tie this mechanism exists for, which is at 1. What is
/// given up is real and should be stated rather than explained away: on the
/// gate window 3,805 of 15,848 scored rows (24%) have an unbounded forecast
/// below 0 and now tie at exactly 0, against 231 that clamp at 1. On the TIMED
/// branch the remaining terms of the cost formula still separate those peers;
/// on the no-timing branch, which is the common one because the timing stage
/// needs 30 timed successes, there are no remaining terms and they tie
/// outright, falling back to the caller's order. The alternative, carrying the
/// downward overshoot at the same slope, makes the cost NEGATIVE on that same
/// branch, which the dashboard renders as "N/A" and which reaches the routing
/// dataset; that was measured and is the worse of the two.
pub(crate) fn ranking_failure_probability(unbounded: f64) -> f64 {
    let clamped = unbounded.clamp(0.0, 1.0);
    clamped + RANKING_OVERSHOOT_SLOPE * (unbounded - clamped).max(0.0)
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
    /// The MONOTONE refusal total at the last notice, and the only value the
    /// `_since_last_notice` delta may be computed against.
    refused_at_last_log: u64,
    /// The composite saturation reading at the last notice, used ONLY to decide
    /// whether anything changed. It is NOT monotone, because
    /// [`ContractTable::pairs_refused_last_refit`] is a gauge that resets at
    /// every refit, so subtracting it from a later total underflows. Keeping
    /// the two apart is the fix for that; see [`Self::log_saturation`].
    saturation_at_last_log: u64,
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
            refused_at_last_log: 0,
            saturation_at_last_log: 0,
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
    /// entries, or that the contract table is refusing residuals. Evictions are
    /// the signal that `max_connections` headroom is too small for this node's
    /// churn; refusals are contracts with more peers than
    /// [`CONTRACT_ENTRIES`], whose residuals train the levels and the curve but
    /// not the contract table. Both are counted in release builds, so a node
    /// discarding evidence is not reading as a node with nothing to discard.
    fn log_saturation(&mut self, time: f64) {
        let evictions = self.total_evictions();
        // Read the counters directly. `diagnostics()` builds a 15-field struct
        // and walks the peer index, and this runs per learned event under the
        // router's write lock.
        let (refused, refused_last_refit, displaced) =
            self.failure.contracts.as_ref().map_or((0, 0, 0), |table| {
                (
                    table.refused,
                    table.pairs_refused_last_refit,
                    table.displaced,
                )
            });
        // Gate on every saturation the message names, not only evictions and
        // live refusals: a node whose only saturation is refit drops, or
        // displacement of live entries, would otherwise stay silent while the
        // message advertises "contract entries full".
        //
        // `saturation` is a CHANGE DETECTOR, not a total, and the two must not
        // be confused. `refused_last_refit` is a gauge that
        // [`Stage::refit`] resets at every refit, so `saturation` can FALL, and
        // subtracting a stored `saturation` from a later `refused` underflows:
        // in a debug or test build that is a panic inside `observe` under the
        // router's write lock, and in release it wraps to about 1.8e19 in a
        // telemetry field. The monotone total is tracked separately in
        // `refused_at_last_log` and is the only value the delta below is
        // computed against. Pinned by
        // `a_second_saturation_notice_reports_a_real_delta_and_does_not_underflow`.
        // The monotone total is checked as well as the composite, because a
        // composite that happens to be unchanged does not mean nothing
        // happened: one new live refusal against a gauge one lower leaves
        // `saturation` identical and would suppress a notice about a real
        // refusal. Pinned by
        // `a_new_live_refusal_is_not_suppressed_by_a_falling_refit_gauge`.
        let saturation = refused + refused_last_refit + displaced;
        if evictions == self.evictions_at_last_log
            && refused == self.refused_at_last_log
            && saturation == self.saturation_at_last_log
        {
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
            contract_residuals_refused_total = refused,
            contract_residuals_refused_since_last_notice = refused - self.refused_at_last_log,
            contract_pairs_refused_last_refit = refused_last_refit,
            contract_entries_displaced_total = displaced,
            contract_entries = CONTRACT_ENTRIES,
            "hierarchical routing estimator: peer table full, evicting least-recently-used \
             peers, or contract entries full"
        );
        self.last_saturation_log = Some(time);
        self.evictions_at_last_log = evictions;
        self.refused_at_last_log = refused;
        self.saturation_at_last_log = saturation;
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
        let failure = self.failure.predict(peer, contract, distance, time);
        Estimate {
            failure_probability: failure.map(|forecast| forecast.value),
            failure_ranking: failure
                .map(|forecast| ranking_failure_probability(forecast.unbounded))
                .filter(|ranking| ranking.is_finite()),
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
