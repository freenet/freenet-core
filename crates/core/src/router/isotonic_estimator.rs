use crate::ring::{Distance, Location, PeerKeyLocation};
use pav_regression::IsotonicRegression;
use pav_regression::Point;
use serde::Serialize;
use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};

const MIN_POINTS_FOR_REGRESSION: usize = 5;

/// Maximum number of raw data points retained by the global regression.
/// Once reached, each new point evicts the oldest.
const MAX_REGRESSION_POINTS: usize = 500;

/// Percentage of the current window that must turn over before
/// [`IsotonicEstimator::refit_if_stale`] rebuilds the fit.
///
/// Compared as `events_since_refit * 100 > len * REFIT_STALENESS_PERCENT` — a
/// multiplication rather than a division, so the threshold is exact with no
/// truncation. Over a saturated window (`MAX_REGRESSION_POINTS`) this earns a
/// refit every 51st event, rare enough that the O(n log n) refit amortises to a
/// handful of operations per event.
///
/// Since #5658 the refit no longer repairs the GLOBAL curve, which is rebuilt
/// exactly on every event (see `IsotonicEstimator::sorted_points`). What it still
/// does is re-anchor every peer's EWMA to the current curve and prune peers that
/// left the window; see [`IsotonicEstimator::refit_if_stale`].
///
/// This is the WHOLE cadence, not a lower bound on it. [`IsotonicEstimator::add_event`]
/// evaluates the trigger inline, and it is the only writer of both
/// `events_since_refit` and `raw_events`, so a refit happens on the very event
/// that earns it. Until #4811 the trigger was instead polled by
/// `Ring::refit_router_periodically`'s 5-minute tick, making the realised cadence
/// `min(5 minutes, this)`: above ~10 events/min the tick bound instead, and a
/// window could turn over entirely between refits — precisely where drift is
/// worst. (That stayed a documentation caveat rather than a live fault only
/// because ~10 events/min is ~40x the observed production median of ~14-16
/// events/hour.)
///
/// A SLOW estimator loses nothing by the move, which is worth stating because the
/// opposite is the natural guess. The old tick did not refit on a timer: it called
/// `refit_stale_estimators` -> `refit_if_stale`, which returns early unless
/// `refit_due()` — THIS predicate — already holds. So the tick was a poll of the
/// same condition, and an estimator that had not earned turnover was skipped by it
/// exactly as it is skipped now. The trigger condition is unchanged; only the
/// latency between earning a refit and performing it changed, from "up to 5
/// minutes" to zero. Nothing that used to be refit no longer is.
///
/// That the timer was never the real trigger is also why dropping it costs no
/// accuracy: staleness is produced by `add_event`, so an estimator receiving no
/// events is not going stale, and refitting it on a timer would rebuild an
/// identical fit from identical data.
const REFIT_STALENESS_PERCENT: usize = 10;

/// EWMA smoothing factor for per-peer adjustments.
/// Alpha = 0.1 gives a half-life of ~6.6 events, meaning the influence of an
/// observation drops below 50% after about 7 newer observations.
const EWMA_ALPHA: f64 = 0.1;

/// Floor for the global base in [`AdjustmentMode::Multiplicative`]. The global
/// regression can extrapolate slightly negative near the edges of its data range;
/// clamping that base to exactly `0.0` before a multiplicative adjustment would
/// annihilate the peer's factor (`0 * exp(adj) == 0`), predicting a slow peer as
/// instant. A tiny positive floor keeps `base * exp(adj)` ordered by the per-peer
/// factor. It is far below any real response time (1 ns) / transfer rate, so it
/// only affects the degenerate `global <= 0` region.
const MULTIPLICATIVE_MIN_BASE: f64 = 1e-9;

#[cfg(test)]
thread_local! {
    /// Test builds only: whether this thread expects `resync_window` to run.
    /// Unset, a resync panics. That covers this crate's lib unit tests, on the
    /// thread that reaches the resync. It does not cover the integration tests
    /// under `crates/core/tests` (the simulations link the library without
    /// `cfg(test)`, so there a resync only warns), nor a resync inside a
    /// spawned task whose `JoinHandle` nothing checks. That is narrower than
    /// the `debug_assert!` it replaced, which fired in every debug build. A
    /// test that corrupts a window on purpose sets it.
    static RESYNC_EXPECTED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// `IsotonicEstimator` provides outcome estimation for a given action, such as
/// retrieving the state of a contract, based on the distance between the peer
/// and the contract. It uses an isotonic regression model from the `pav.rs`
/// library to estimate the outcome based on the distance between the peer and
/// the contract, but then also tracks an adjustment for each peer based on the
/// outcome of the peer's previous requests.
///
/// The global regression uses a rolling window: once `MAX_REGRESSION_POINTS`
/// raw points have been accumulated, each new point evicts the oldest and the
/// fit is rebuilt over the window. Per-peer adjustments use an
/// exponentially-weighted moving average (EWMA) so recent events have more
/// influence than old ones.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct IsotonicEstimator {
    /// The fit over the window, kept current on every event under
    /// [`FitPolicy::EveryEvent`]. Under [`FitPolicy::OnRead`] it is never built
    /// and stays empty. Private so that nothing outside this module can read it
    /// directly and get that empty fit: every reader goes through
    /// [`Self::current_fit`]. `Serialize` writes it as it stands, so an OnRead
    /// estimator would serialize an empty fit; nothing serializes an estimator
    /// today.
    global_regression: IsotonicRegression<f64>,
    pub peer_adjustments: HashMap<PeerKeyLocation, Adjustment>,
    /// When the global fit is built. See [`FitPolicy`].
    #[serde(skip)]
    fit_policy: FitPolicy,
    /// How many times `sorted_points` was found out of step with `raw_events`
    /// and rebuilt from it. Always 0 unless the window's bookkeeping has a bug;
    /// see [`Self::resync_window`].
    #[serde(skip)]
    window_resyncs: u64,
    /// How per-peer adjustments combine with the global estimate. See
    /// [`AdjustmentMode`].
    #[serde(skip)]
    adjustment_mode: AdjustmentMode,
    /// Raw input events in insertion order. When len exceeds
    /// `MAX_REGRESSION_POINTS`, the oldest is evicted.
    ///
    /// Retains the whole [`IsotonicEvent`], not just its `(distance, result)`
    /// point, because [`Self::refit`] must rebuild `peer_adjustments` too — and
    /// that needs peer identity. Keeping only points would make a refit a
    /// PARTIAL rebuild: the global curve would move while every peer's EWMA
    /// stayed anchored to the old curve, and evicted peers would never be
    /// pruned from `peer_adjustments`.
    #[serde(skip)]
    raw_events: VecDeque<IsotonicEvent>,
    /// The `(distance, result)` points of `raw_events`, kept in the order
    /// `pav_regression` sorts its input. Under [`FitPolicy::EveryEvent`],
    /// `global_regression` is rebuilt from this on every event; under
    /// [`FitPolicy::OnRead`], [`Self::current_fit`] builds from it when read.
    ///
    /// WHY NOT `add_points` / `remove_points` (#5658). `pav_regression` 0.7.0's
    /// incremental maintenance is approximate, and not slightly: `remove_points`
    /// subtracts the evicted point from the CLOSEST pooled aggregate by x, which
    /// need not be the aggregate that point was pooled into. When it is the wrong
    /// one, the subtraction produces an aggregate no data could — observed on a
    /// failure-probability fit as `(distance = -0.062, failure = -5.0)`. Routing
    /// read that fit, and every peer's EWMA was trained against it, for up to 50
    /// events until the next refit. Measured on failure-shaped data (binary
    /// outcomes, 500-point window, refit every 51st event): 19% of states held an
    /// out-of-range aggregate, and interpolation was off by as much as 2.16.
    /// `add_points` is not exact on its own either: it re-pools the already
    /// pooled blocks, so its result depends on insertion order.
    ///
    /// For the cost of the rebuild, and why the ordering matters to it, see
    /// [`SortedWindow`].
    #[serde(skip)]
    sorted_points: SortedWindow,
    /// Monotonic direction of the fit, retained so [`Self::refit`] can rebuild
    /// the same way [`Self::new_with_mode`] built it.
    #[serde(skip)]
    estimator_type: EstimatorType,
    /// Events added via [`Self::add_event`] since the last refit. Drives the
    /// staleness trigger; see [`Self::refit_if_stale`].
    #[serde(skip)]
    events_since_refit: usize,
}

/// When an [`IsotonicEstimator`] builds its global fit.
///
/// The fit is a full PAV rebuild over the window (see [`SortedWindow`]), and it
/// runs inside `Router::add_event`, under `ring.router.write()`. Six estimators
/// can take one route event, but routing only reads three of them. The other
/// three (`Router::per_op_*`) feed the dashboard snapshot, which is built on
/// the five-minute telemetry cadence and on each load of a peer-detail page, so
/// rebuilding them on every event paid for a fit that was rarely read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum FitPolicy {
    /// Rebuild the fit and update the per-peer EWMA on every event, and refit
    /// the peer adjustments as the window turns over. For estimators that
    /// routing reads.
    #[default]
    EveryEvent,
    /// Keep only the window; fit it when a reader asks (see
    /// [`IsotonicEstimator::current_fit`]). The fit is the same exact batch fit over the
    /// same window, so readers see the identical curve. There are no per-peer
    /// adjustments: the EWMA is trained against the fit as it stood at each
    /// event, which this policy never builds. For estimators nothing routes on.
    ///
    /// Reading computes a fresh fit and caches nothing, so it needs only
    /// `&self`. That keeps the dashboard snapshot on `ring.router.read()` with
    /// no interior mutability to reason about, at the cost of one fit per read.
    /// [`IsotonicEstimator::sampled_curve_and_range`] keeps the snapshot to one
    /// fit per estimator.
    OnRead,
}

/// How a per-peer adjustment combines with the global isotonic estimate.
///
/// The per-peer adjustment is an EWMA that corrects the global distance→outcome
/// fit for a specific peer. Whether that correction is best expressed as an
/// absolute offset or a scaling factor depends on the target:
///
/// - **Additive** (`global + adjustment`): the EWMA averages absolute residuals
///   `observed - global`. Correct for a bounded target such as failure
///   probability, where "this peer fails 0.05 more often" is the natural unit.
/// - **Multiplicative** (`global * exp(adjustment)`): the EWMA averages log
///   ratios `ln(observed) - ln(global)` (a geometric mean of `observed/global`).
///   Correct for an unbounded, heavy-tailed, multiplicative-scale target such as
///   response time. Telemetry over 5.3 days / 631 peers showed a peer's deviation
///   from the global response-time curve is a near-constant *ratio*, not a
///   constant offset (log-residuals are level-independent for ~88% of peers,
///   96% observation-weighted, whereas additive residuals grow with the level).
///   It also makes a negative estimate impossible by construction: `global >= 0`
///   and `exp(_) > 0`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum AdjustmentMode {
    #[default]
    Additive,
    Multiplicative,
}

impl AdjustmentMode {
    /// The per-event training residual fed to the peer's EWMA, or `None` when the
    /// event cannot be expressed in this mode's space. Multiplicative requires a
    /// strictly-positive observation and global estimate (`ln` is undefined at or
    /// below zero); such events are skipped rather than corrupting the EWMA with
    /// `NaN`/`-inf`.
    pub(crate) fn residual(self, observed: f64, global: f64) -> Option<f64> {
        match self {
            AdjustmentMode::Additive => Some(observed - global),
            AdjustmentMode::Multiplicative => {
                if observed > 0.0 && global > 0.0 {
                    Some(observed.ln() - global.ln())
                } else {
                    None
                }
            }
        }
    }

    /// Combine a global estimate with a peer's smoothed adjustment value. The
    /// neutral adjustment is `0.0` in both modes (`global + 0` and
    /// `global * e^0 = global`), so callers can pass `0.0` for a peer that has no
    /// usable adjustment yet. Shared by the router and the dashboard so both
    /// render the identical peer-adjusted value.
    pub(crate) fn apply(self, global: f64, adjustment: f64) -> f64 {
        match self {
            AdjustmentMode::Additive => global + adjustment,
            AdjustmentMode::Multiplicative => global * adjustment.exp(),
        }
    }

    /// Clamp the global estimate to a non-negative base before applying the
    /// adjustment. The regression can extrapolate slightly negative near its
    /// data-range edges; both modes must start from a non-negative base.
    ///
    /// Additive floors at exactly `0.0` (`0 + adj` still carries the peer's
    /// absolute correction). Multiplicative floors at a tiny POSITIVE value
    /// instead: flooring to `0.0` would make `0 * exp(adj) == 0` and erase the
    /// peer's learned factor entirely — a consistently-slow peer would be
    /// predicted as instant. The tiny floor keeps `base * exp(adj)` ordered by the
    /// per-peer factor in that degenerate region while staying far below any real
    /// value, so normal (positive-base) predictions are unchanged.
    fn floor_base(self, global: f64) -> f64 {
        match self {
            AdjustmentMode::Additive => global.max(0.0),
            AdjustmentMode::Multiplicative => global.max(MULTIPLICATIVE_MIN_BASE),
        }
    }
}

impl IsotonicEstimator {
    // Minimum sample size before we apply per-peer adjustments; keeps peer curves from being
    // dominated by sparse/noisy data.
    const ADJUSTMENT_PRIOR_SIZE: u64 = 10;

    /// Creates a new `IsotonicEstimator` from a list of historical events, using
    /// [`AdjustmentMode::Additive`] per-peer adjustments.
    pub fn new<I>(history: I, estimator_type: EstimatorType) -> Self
    where
        I: IntoIterator<Item = IsotonicEvent>,
    {
        Self::new_with_mode(history, estimator_type, AdjustmentMode::Additive)
    }

    /// Like [`new`](Self::new) but selects how per-peer adjustments combine with
    /// the global estimate. The mode must be fixed at construction because the
    /// per-peer EWMA is trained on residuals computed in that mode's space (see
    /// [`AdjustmentMode::residual`]); it cannot be changed afterwards without
    /// recomputing every peer's history.
    pub fn new_with_mode<I>(
        history: I,
        estimator_type: EstimatorType,
        adjustment_mode: AdjustmentMode,
    ) -> Self
    where
        I: IntoIterator<Item = IsotonicEvent>,
    {
        Self::new_with_policy(
            history,
            estimator_type,
            adjustment_mode,
            FitPolicy::EveryEvent,
        )
    }

    /// An estimator that keeps its window current but fits it only when read
    /// ([`FitPolicy::OnRead`]). For estimators nothing routes on, such as the
    /// router's per-operation dashboard curves.
    ///
    /// `adjustment_mode` is recorded so the estimator reports the same mode as
    /// its routing counterpart, but it has no effect: this policy keeps no
    /// per-peer adjustments.
    pub fn new_fit_on_read<I>(
        history: I,
        estimator_type: EstimatorType,
        adjustment_mode: AdjustmentMode,
    ) -> Self
    where
        I: IntoIterator<Item = IsotonicEvent>,
    {
        Self::new_with_policy(history, estimator_type, adjustment_mode, FitPolicy::OnRead)
    }

    fn new_with_policy<I>(
        history: I,
        estimator_type: EstimatorType,
        adjustment_mode: AdjustmentMode,
        fit_policy: FitPolicy,
    ) -> Self
    where
        I: IntoIterator<Item = IsotonicEvent>,
    {
        let mut all_events: Vec<IsotonicEvent> = history.into_iter().collect();

        // If history exceeds the window, keep only the most recent events.
        // Both the regression points and the peer adjustment deltas are computed
        // from the same windowed subset to avoid stale-data bias.
        if all_events.len() > MAX_REGRESSION_POINTS {
            all_events.drain(..all_events.len() - MAX_REGRESSION_POINTS);
        }

        let raw_events: VecDeque<IsotonicEvent> = all_events.into();
        let sorted_points = SortedWindow::from_events(&raw_events);
        let (global_regression, peer_adjustments) = match fit_policy {
            FitPolicy::EveryEvent => {
                let global_regression = Self::fit_points(sorted_points.as_slice(), estimator_type)
                    .expect("Failed to create isotonic regression");
                let peer_adjustments =
                    Self::anchor_peer_adjustments(&raw_events, &global_regression, adjustment_mode);
                (global_regression, peer_adjustments)
            }
            FitPolicy::OnRead => (
                Self::fit_points(&[], estimator_type)
                    .expect("an empty fit without intersect_origin cannot fail"),
                HashMap::new(),
            ),
        };

        IsotonicEstimator {
            global_regression,
            peer_adjustments,
            fit_policy,
            window_resyncs: 0,
            adjustment_mode,
            raw_events,
            sorted_points,
            estimator_type,
            events_since_refit: 0,
        }
    }

    /// Build the global regression over `points`, in this estimator's direction.
    fn fit_points(
        points: &[Point<f64>],
        estimator_type: EstimatorType,
    ) -> Result<IsotonicRegression<f64>, pav_regression::isotonic_regression::IsotonicRegressionError>
    {
        match estimator_type {
            EstimatorType::Positive => IsotonicRegression::new_ascending(points),
            EstimatorType::Negative => IsotonicRegression::new_descending(points),
        }
    }

    /// Every peer's adjustment in `events`, re-derived from scratch against
    /// `global_regression`.
    ///
    /// Shared by [`Self::new_with_mode`] and [`Self::refit`]. A peer's
    /// `Adjustment` is an EWMA of residuals measured AGAINST the global curve
    /// (see [`AdjustmentMode::residual`]), so once the curve has moved the
    /// residuals must be re-derived, or every peer is corrected relative to a
    /// curve that no longer exists. Rebuilding from `events` also bounds
    /// `peer_adjustments` to peers present in the current window — without
    /// that, the map would grow one entry per peer ever seen (an unbounded
    /// per-key collection driven by remote peers) and a returning peer would
    /// have its stale adjustment applied at full weight, since
    /// `Adjustment::effective_count` has no time decay.
    fn anchor_peer_adjustments(
        events: &VecDeque<IsotonicEvent>,
        global_regression: &IsotonicRegression<f64>,
        adjustment_mode: AdjustmentMode,
    ) -> HashMap<PeerKeyLocation, Adjustment> {
        let mut peer_adjustments: HashMap<PeerKeyLocation, Adjustment> = HashMap::new();

        if global_regression.len() >= Self::ADJUSTMENT_PRIOR_SIZE as usize {
            let mut peer_events: HashMap<&PeerKeyLocation, Vec<&IsotonicEvent>> = HashMap::new();
            for event in events {
                peer_events.entry(&event.peer).or_default().push(event);
            }

            for (peer_location, peer_history) in peer_events {
                let mut adjustment = Adjustment::new();
                // Seed with ADJUSTMENT_PRIOR_SIZE phantom neutral observations
                // so peers with few real observations are shrunk toward zero.
                adjustment.effective_count = Self::ADJUSTMENT_PRIOR_SIZE as f64;

                for event in peer_history {
                    let global_estimate = global_regression
                        .interpolate(event.route_distance().as_f64())
                        .expect("Regression should always produce an estimate");
                    if let Some(delta) = adjustment_mode.residual(event.result, global_estimate) {
                        adjustment.add(delta);
                    }
                }
                peer_adjustments.insert(peer_location.clone(), adjustment);
            }
        }

        peer_adjustments
    }

    /// Re-anchor every peer's adjustment to the current curve, but only once
    /// more than `REFIT_STALENESS_PERCENT` percent of the window has turned
    /// over since the last refit. Returns whether a refit ran.
    ///
    /// WHY THIS EXISTS: `add_event` keeps the global curve exact (#5658), but it
    /// updates each peer's EWMA only with the residual against the curve AS IT
    /// STOOD at that event, and never removes a peer. As the window turns over
    /// the curve moves under those residuals, and peers that left the window
    /// keep their entries. Refitting re-derives every peer's adjustment against
    /// the current curve and bounds the map to the window.
    ///
    /// Until #5658 this was also what repaired the global curve itself, which
    /// was maintained with `pav_regression`'s approximate `add_points` /
    /// `remove_points` and could be badly wrong between refits — see
    /// `sorted_points`.
    ///
    /// It is deliberately driven by DATA TURNOVER rather than a timer: a refit
    /// on an idle router is pure waste (nothing changed), while a busy router
    /// earns one quickly. This also replaces the previous mechanism, which
    /// rebuilt the whole `Router` from the on-disk event log every 5 minutes —
    /// that log never receives relay-recorded events
    /// (`operations::record_relay_route_event` feeds the in-memory router only),
    /// so the rebuild silently discarded them and reset the model faster than it
    /// could learn. `raw_events` is the complete corpus by construction: every
    /// `add_event` lands here regardless of whether it came from an originator
    /// or a relay hop. See issue #4808.
    ///
    /// The curve itself is already exact when this runs; see
    /// [`Self::anchor_peer_adjustments`] for why the adjustments must follow it.
    ///
    /// Called from [`Self::add_event`] only. It is deliberately NOT public: a
    /// caller polling it could never observe a stale estimator, because
    /// `add_event` clears staleness before it returns (see there for why the
    /// check belongs on the write path).
    fn refit_if_stale(&mut self) -> bool {
        if !self.refit_due() {
            return false;
        }
        self.refit();
        true
    }

    /// Test-only view of [`Self::refit_due`], for guards in other modules
    /// (`router`, `operations::connect`) that pin the #4811 invariant: once
    /// [`Self::add_event`] returns, the estimator is never left stale.
    ///
    /// Reading the predicate rather than counting refits is deliberate — it
    /// asserts the property that matters (no outstanding staleness) rather than
    /// the mechanism, and it fails loudly if the `refit_if_stale` call in
    /// `add_event` is ever dropped.
    #[cfg(test)]
    pub(crate) fn is_stale_for_test(&self) -> bool {
        self.refit_due()
    }

    /// Whether enough of the window has turned over to justify a refit.
    fn refit_due(&self) -> bool {
        if self.raw_events.len() < MIN_POINTS_FOR_REGRESSION {
            // Below this the regression refuses to estimate anyway
            // (`estimate_retrieval_time`), so a refit buys nothing.
            return false;
        }
        self.events_since_refit * 100 > self.raw_events.len() * REFIT_STALENESS_PERCENT
    }

    /// Unconditionally re-derive every peer's adjustment from `raw_events`
    /// against the current global curve, and prune peers no longer in the
    /// window.
    ///
    /// The curve is NOT re-fitted here. `add_event_incremental` rebuilds it
    /// from the window on every event, so it is already the batch fit over
    /// `raw_events`, and re-fitting would reproduce it for nothing. Anchoring to
    /// the curve routing actually reads is also the right semantics even in the
    /// unreachable case where that rebuild failed and kept a previous fit: the
    /// adjustments must correct the curve `estimate_retrieval_time` uses.
    ///
    /// Equivalent to constructing a fresh estimator over the current window —
    /// which is exactly what the pre-#4808 periodic `Router::new(&history)`
    /// rebuild achieved, except that it read an on-disk log missing every
    /// relay-recorded event, whereas `raw_events` is complete by construction.
    fn refit(&mut self) {
        self.peer_adjustments = Self::anchor_peer_adjustments(
            &self.raw_events,
            &self.global_regression,
            self.adjustment_mode,
        );
        self.events_since_refit = 0;
    }

    /// Adds a new event to the estimator, refitting it if this event pushes the
    /// window past the staleness threshold ([`Self::refit_if_stale`]).
    ///
    /// WHY THE REFIT LIVES HERE (#4811). This method is the sole writer of both
    /// inputs to the staleness trigger — `events_since_refit` and `raw_events` —
    /// so it is the only moment staleness can change. Evaluating the trigger here
    /// therefore catches every transition into staleness at the instant it
    /// happens, and makes "the estimator is never stale once `add_event` returns"
    /// an invariant rather than something a poller converges on.
    ///
    /// Until #4811 the trigger was polled by `Ring::refit_router_periodically`'s
    /// 5-minute tick instead. That was a pull where a push is natural, and it
    /// paid for the privilege three times over:
    ///
    /// - It could only ever refit LATER than this does, never sooner, so it
    ///   widened the drift window it existed to close (see
    ///   [`REFIT_STALENESS_PERCENT`]).
    /// - It could only refit estimators reachable from `Router`, so
    ///   `operations::connect::ConnectForwardEstimator` — which lives outside
    ///   `Router`, behind its own lock — silently drifted forever. Refitting on
    ///   the write path fixes that with no wiring: every estimator refits itself,
    ///   whoever owns it.
    /// - It kept a `task_monitor`-registered (hence node-fatal) task alive to
    ///   poll an in-memory counter.
    ///
    /// COST. Every call rebuilds the global regression from the sorted window
    /// (see `sorted_points` and [`SortedWindow`]), and every ~51st call on a
    /// saturated window also re-anchors the peer adjustments. Measured with
    /// `sorted_window_rebuild_cost_by_shape` in a release build, 500-point
    /// window, 32 peers, refit amortised in, on a shared 16-core machine at load
    /// average 20-38 (so treat these as upper-ish figures): **12-59µs per call**
    /// across runs, for both all-distinct and 20 repeated distances. The
    /// pre-#5658 path, `add_points` / `remove_points` with no rebuild, measured
    /// 3-4µs, and additionally paid a full curve fit on every refit. The
    /// rebuild is the price of a fit that is exact on every event rather than
    /// repaired every 51. A [`FitPolicy::OnRead`] estimator skips the rebuild
    /// and pays only for the window's sorted insert and remove: 0.3µs per call
    /// in the same runs.
    ///
    /// In context, this is not what sets the router's write-lock hold time.
    /// `Router::add_event` feeds this to its three routing estimators (its
    /// three per-operation dashboard estimators fit on read), under
    /// `ring.router.write()`. On a saturated router the whole `add_event`
    /// measured 2.0-8.7ms per event across passes in release on the same
    /// machine, and within every pass the isotonic estimators took under 2% of
    /// it. (The ranges come from different passes, so they are not to be divided
    /// into each other.) Nearly all of the rest is the
    /// Renegade predictor: in a perf profile about 22% of samples were under
    /// `PredictionStage::train` (renegade's `get_optimal_k`) and about 11% in
    /// its kNN sort, with no isotonic function above 3% (#5662).
    ///
    /// The figures this paragraph used to quote (~39µs per `add_event`, ~150µs
    /// per refit, from #4811) no longer reproduce on this build and have been
    /// dropped rather than carried forward.
    ///
    /// LOCK SAFETY. The fit takes no locks and does no I/O — it is pure
    /// computation over `self` — so it cannot deadlock or re-enter a caller that
    /// already holds one (`Router::add_event` is called under
    /// `ring.router.write()`; `ConnectForwardEstimator::record` under its own
    /// `RwLock`). It only extends a critical section the caller already holds.
    ///
    /// Precisely: that "no locks" claim covers the fit, not every line
    /// reachable from `add_event`. The rebuild's error arm in
    /// `add_event_incremental` calls `tracing::warn!`, and in release builds the
    /// per-callsite rate limiter (`util/rate_limit_layer.rs`) does a DashMap
    /// lookup, whose shard guard is a real `parking_lot` lock (it is compiled out
    /// under `cfg(test)`, so no test would surface it). That is not a deadlock
    /// risk — nothing reachable from that shard guard takes `ring.router` or
    /// `connect_forward_estimator` back, so there is no cycle — and it is inert
    /// today because the error arm is unreachable (see there). Anyone making
    /// that arm reachable must re-check this paragraph. The same holds for the
    /// warn in `resync_window`, which runs only if the window's bookkeeping has
    /// a bug.
    pub fn add_event(&mut self, event: IsotonicEvent) {
        self.add_event_incremental(event);

        // Re-anchor the peer adjustments once this event has turned over enough
        // of the window. Runs last so the incremental state above is complete on
        // the ~50/51 events that do not earn a refit; on the one that does,
        // `refit` re-derives every peer's adjustment from `raw_events`. (After
        // a resync, `add_event_incremental` has already refitted, which leaves
        // nothing stale for this check.)
        self.refit_if_stale();
    }

    /// The incremental half of [`Self::add_event`]: extend the window by one
    /// event, rebuild the global fit over it, and update this peer's EWMA,
    /// WITHOUT considering a scheduled refit. The one refit it does make is
    /// after a resync: it re-anchors the peer adjustments to the repaired fit
    /// at once (the end of the body), which also resets `events_since_refit`.
    ///
    /// Split out so tests can observe the states BETWEEN refits, which is what
    /// routing reads on ~50 of every 51 events.
    /// `incremental_fit_matches_batch_after_every_event` pins that the global
    /// curve is exact in those states (#5658), and
    /// `refit_prunes_peers_that_fell_out_of_the_window` that the peer map is not
    /// bounded without a refit. Through `add_event` neither could be seen: it
    /// refits as it goes, which is the point of #4811.
    fn add_event_incremental(&mut self, event: IsotonicEvent) {
        let route_distance = event.route_distance();

        // Extend the window by one point, evict the oldest if it is full, and
        // rebuild the global fit from the window. The rebuild is what keeps the
        // fit exact: see `sorted_points` for why the library's incremental
        // `add_points` / `remove_points` cannot be used here (#5658).
        self.sorted_points
            .insert(route_distance.as_f64(), event.result);
        self.raw_events.push_back(event.clone());

        let mut evicted_in_step = true;
        if self.raw_events.len() > MAX_REGRESSION_POINTS {
            if let Some(oldest) = self.raw_events.pop_front() {
                evicted_in_step = self
                    .sorted_points
                    .remove(oldest.route_distance().as_f64(), oldest.result);
            }
        }
        let resynced = !evicted_in_step || self.sorted_points.len() != self.raw_events.len();
        if resynced {
            self.resync_window();
        }

        if self.fit_policy == FitPolicy::OnRead {
            // Nothing routes on this estimator: keep the window, and leave the
            // fit to whoever reads it. With no EWMA there is nothing for a refit
            // to re-anchor, so `events_since_refit` stays 0 and `refit_due` is
            // never true.
            return;
        }
        self.events_since_refit += 1;

        match Self::fit_points(self.sorted_points.as_slice(), self.estimator_type) {
            Ok(regression) => self.global_regression = regression,
            // Unreachable today: `new_ascending`/`new_descending` pass
            // `intersect_origin: false`, and the sole error variant
            // (`NegativePointWithIntersectOrigin`) is only produced when that
            // flag is true. Empty input succeeds. Handled rather than
            // `.expect()`-ed so that a future pav_regression change cannot turn
            // a fit failure into a panic on the relay hot path; a fit one event
            // stale beats a dead node. If a reachable error variant is ever
            // added, rate-limit this warn: as written it would fire on every
            // event.
            Err(error) => tracing::warn!(
                %error,
                events = self.raw_events.len(),
                "Isotonic fit failed; keeping previous fit"
            ),
        }

        if self.global_regression.len() >= Self::ADJUSTMENT_PRIOR_SIZE as usize {
            let global_estimate = self
                .global_regression
                .interpolate(route_distance.as_f64())
                .unwrap();

            if let Some(delta) = self.adjustment_mode.residual(event.result, global_estimate) {
                self.peer_adjustments
                    .entry(event.peer)
                    .or_default()
                    .add(delta);
            }
        }

        // After a resync the EWMAs had been trained against fits over the wrong
        // window for as long as the desync lasted. Re-anchor them to the repaired
        // fit now, not at the next scheduled refit, so a detected bookkeeping bug
        // leaves no residue.
        if resynced {
            self.refit();
        }
    }

    pub fn estimate_retrieval_time(
        &self,
        peer: &PeerKeyLocation,
        contract_location: Location,
    ) -> Result<f64, EstimationError> {
        let fit = self.current_fit();
        if fit.len() < MIN_POINTS_FOR_REGRESSION {
            return Err(EstimationError::InsufficientData);
        }

        let peer_location = peer.location().ok_or(EstimationError::InsufficientData)?;
        let distance: f64 = contract_location.distance(peer_location).as_f64();

        let global_estimate = fit
            .interpolate(distance)
            .ok_or(EstimationError::InsufficientData)?;

        // Regression can sometimes produce negative estimates. Floor the base
        // non-negative before applying the per-peer adjustment; in multiplicative
        // mode the floor is a tiny positive value so the peer's factor is not
        // annihilated (see `AdjustmentMode::floor_base`).
        let global_estimate = self.adjustment_mode.floor_base(global_estimate);

        let adjusted_estimate =
            self.peer_adjustments
                .get(peer)
                .map_or(global_estimate, |peer_adjustment| {
                    let should_use_peer_adjustment =
                        peer_adjustment.effective_count >= MIN_POINTS_FOR_REGRESSION as f64;
                    if should_use_peer_adjustment {
                        self.adjustment_mode
                            .apply(global_estimate, peer_adjustment.value())
                    } else {
                        global_estimate
                    }
                });

        // The per-peer adjustment is applied *after* the global clamp above. In
        // additive mode it can be negative (a peer faster / more reliable than the
        // global fit); in multiplicative mode `global * exp(_)` is already >= 0.
        // Re-clamp either way so the per-peer estimate can never go below zero —
        // these targets (response time, transfer rate, failure probability) are
        // all physically non-negative, and a negative prediction would both
        // distort routing cost formulas and render below the x-axis on the
        // dashboard's "Peer-adjusted" curve. The failure path applies the same
        // clamp downstream (see `predict_routing_outcome`).
        Ok(adjusted_estimate.max(0.0))
    }

    /// Number of points the global fit is over: the window's size.
    pub(crate) fn len(&self) -> usize {
        match self.fit_policy {
            FitPolicy::EveryEvent => self.global_regression.len(),
            // Counted without fitting. Equal to the fit's `len()` by
            // construction: the fit is over exactly these points.
            FitPolicy::OnRead => self.sorted_points.len(),
        }
    }

    /// The global fit over the current window.
    ///
    /// Under [`FitPolicy::EveryEvent`] that is the fit `add_event` keeps current,
    /// borrowed. Under [`FitPolicy::OnRead`] it is built here, from the sorted
    /// window, and owned by the caller: the same exact batch fit, computed when
    /// asked for rather than on every event. Nothing is cached, so this takes
    /// `&self` and a reader holding only `ring.router.read()` can call it.
    ///
    /// Named `current_fit`, not `fit`: until #5662 `fit` was the refit that
    /// rebuilt the peer adjustments, and an old reference to that should not
    /// silently resolve to this.
    fn current_fit(&self) -> Cow<'_, IsotonicRegression<f64>> {
        match self.fit_policy {
            FitPolicy::EveryEvent => Cow::Borrowed(&self.global_regression),
            FitPolicy::OnRead => {
                match Self::fit_points(self.sorted_points.as_slice(), self.estimator_type) {
                    Ok(fit) => Cow::Owned(fit),
                    // Unreachable for the same reason as in
                    // `add_event_incremental`. `global_regression` is the empty
                    // fit here, so a reader sees no data rather than a panic.
                    Err(_) => Cow::Borrowed(&self.global_regression),
                }
            }
        }
    }

    /// Rebuild `sorted_points` from `raw_events`, which is the source of truth.
    ///
    /// Called only when the two are found out of step, which the bookkeeping in
    /// `add_event_incremental` should make impossible. Before #5662 a missed
    /// removal was only a `debug_assert!`: in a release build the window kept the
    /// stale point for good, and every later fit was over the wrong data. This
    /// repairs it on the event that notices, at the cost of one sort, and
    /// `window_resyncs` records that it happened so tests can require zero.
    ///
    /// The warn cannot flood: a resync restores the invariant, so a second one
    /// needs a second bookkeeping bug. For an EveryEvent estimator,
    /// `add_event_incremental` then re-anchors the peer adjustments.
    ///
    /// In lib unit tests it panics unless the thread set `RESYNC_EXPECTED`, so
    /// a bookkeeping bug fails any such test that reaches it on the test
    /// thread, not just the ones that read `window_resyncs`. See
    /// `RESYNC_EXPECTED` for what that does not cover.
    fn resync_window(&mut self) {
        #[cfg(test)]
        assert!(
            RESYNC_EXPECTED.with(std::cell::Cell::get),
            "isotonic sorted window out of step with its events (window {}, sorted {}): \
             a bookkeeping bug. A test that corrupts a window on purpose must set \
             RESYNC_EXPECTED.",
            self.raw_events.len(),
            self.sorted_points.len()
        );
        self.window_resyncs += 1;
        tracing::warn!(
            window = self.raw_events.len(),
            sorted = self.sorted_points.len(),
            resyncs = self.window_resyncs,
            "Isotonic sorted window out of step with its events; rebuilt it"
        );
        self.sorted_points = SortedWindow::from_events(&self.raw_events);
    }

    /// When this estimator builds its global fit.
    #[cfg(test)]
    pub(crate) fn fit_policy(&self) -> FitPolicy {
        self.fit_policy
    }

    /// The raw events in the rolling window, oldest first. Test-only.
    #[cfg(test)]
    pub(crate) fn raw_events_for_test(&self) -> impl Iterator<Item = &IsotonicEvent> {
        self.raw_events.iter()
    }

    /// The per-peer adjustment mode this estimator was constructed with.
    ///
    /// Was `cfg(test)` while only the router test `estimators_use_intended_adjustment_modes`
    /// needed it. Now real API: the residual correction has to express its target
    /// in the same space this estimator adjusts in, so it asks rather than
    /// duplicating the per-target decision and letting the two drift apart.
    pub(crate) fn adjustment_mode(&self) -> AdjustmentMode {
        self.adjustment_mode
    }

    /// The global distance→outcome estimate, without this peer's EWMA adjustment.
    ///
    /// Exists so the dashboard can attribute a prediction across its layers
    /// (distance only → peer-adjusted → corrected) and score each separately.
    /// Without it there is no way to tell which layer is doing the work, which is
    /// the question an operator actually has.
    pub(crate) fn estimate_global(
        &self,
        peer: &PeerKeyLocation,
        contract_location: Location,
    ) -> Result<f64, EstimationError> {
        let fit = self.current_fit();
        if fit.len() < MIN_POINTS_FOR_REGRESSION {
            return Err(EstimationError::InsufficientData);
        }
        let peer_location = peer.location().ok_or(EstimationError::InsufficientData)?;
        let distance: f64 = contract_location.distance(peer_location).as_f64();
        let global_estimate = fit
            .interpolate(distance)
            .ok_or(EstimationError::InsufficientData)?;
        Ok(self.adjustment_mode.floor_base(global_estimate).max(0.0))
    }

    /// Return the x-range of actual regression data points, or (0, 0) if empty.
    pub(crate) fn data_x_range(&self) -> (f64, f64) {
        Self::fit_x_range(&self.current_fit())
    }

    fn fit_x_range(fit: &IsotonicRegression<f64>) -> (f64, f64) {
        let sorted = fit.get_points_sorted();
        match (sorted.first(), sorted.last()) {
            (Some(first), Some(last)) => (*first.x(), *last.x()),
            _ => (0.0, 0.0),
        }
    }

    /// Sample the regression's `interpolate()` across the full distance range [0, 0.5],
    /// clamping outputs to `[y_clamp_min, y_clamp_max]`. This produces the actual
    /// predictions the estimator would make, including centroid-based extrapolation
    /// beyond the data range.
    ///
    /// Returns `(sampled_points, data_x_min, data_x_max)` where `data_x_min/max`
    /// are the bounds of the actual regression data (for distinguishing interpolation
    /// from extrapolation in charts).
    /// Requires `num_samples >= 2` to produce a meaningful curve.
    pub(crate) fn sampled_curve(
        &self,
        y_clamp_min: f64,
        y_clamp_max: f64,
        num_samples: usize,
    ) -> Vec<(f64, f64)> {
        if num_samples < 2 {
            return Vec::new();
        }
        Self::sample_fit(&self.current_fit(), y_clamp_min, y_clamp_max, num_samples)
    }

    /// [`Self::sampled_curve`] and [`Self::data_x_range`] from ONE fit.
    ///
    /// Under [`FitPolicy::OnRead`] each of those builds its own fit, so a caller
    /// that wants both would pay for two. The router's dashboard snapshot wants
    /// both for every per-operation estimator, under `ring.router.read()`, on
    /// the telemetry cadence and on every peer-detail page load.
    pub(crate) fn sampled_curve_and_range(
        &self,
        y_clamp_min: f64,
        y_clamp_max: f64,
        num_samples: usize,
    ) -> (Vec<(f64, f64)>, (f64, f64)) {
        let fit = self.current_fit();
        (
            Self::sample_fit(&fit, y_clamp_min, y_clamp_max, num_samples),
            Self::fit_x_range(&fit),
        )
    }

    fn sample_fit(
        fit: &IsotonicRegression<f64>,
        y_clamp_min: f64,
        y_clamp_max: f64,
        num_samples: usize,
    ) -> Vec<(f64, f64)> {
        if num_samples < 2 || fit.get_points().is_empty() {
            return Vec::new();
        }
        (0..num_samples)
            .filter_map(|i| {
                let x = (i as f64 / (num_samples - 1) as f64) * 0.5;
                fit.interpolate(x)
                    .map(|y| (x, y.clamp(y_clamp_min, y_clamp_max)))
            })
            .collect()
    }

    /// Downsampled raw `(distance, outcome)` observations for visualization, in
    /// insertion order, at most `max` points (evenly strided across the retained
    /// window). These are the actual events the isotonic fit is computed from, so
    /// the dashboard can show the scatter behind the curve. Empty when no data.
    pub(crate) fn sampled_raw_points(&self, max: usize) -> Vec<(f64, f64)> {
        let n = self.raw_events.len();
        if n == 0 || max == 0 {
            return Vec::new();
        }
        if n <= max {
            return self
                .raw_events
                .iter()
                .map(|e| (e.route_distance().as_f64(), e.result))
                .collect();
        }
        let stride = n as f64 / max as f64;
        (0..max)
            .map(|i| {
                let idx = ((i as f64 * stride) as usize).min(n - 1);
                let event = &self.raw_events[idx];
                (event.route_distance().as_f64(), event.result)
            })
            .collect()
    }
}

/// The global regression's input window, held in exactly the order
/// `pav_regression` sorts its input: distance ascending, then result
/// DESCENDING among equal distances (see `isotonic()` in pav_regression 0.7.0,
/// which does this for both directions).
///
/// # Why the order must match the library's, not just be "sorted by distance"
///
/// `IsotonicRegression::new_*` sorts whatever it is given, with a stable sort.
/// Handing it input already in its own order lets that sort finish in one
/// linear pass; handing it input sorted by distance alone does not whenever
/// distances repeat, and repeated distances are the normal case here — every
/// event for the same peer and contract lands at the same distance. Measured
/// in isolation (release, 500-point window, 20 distinct distances): 28.5µs per
/// rebuild ordered by distance alone, 10.3µs in the library's order.
///
/// # Signed zeros
///
/// Coordinates are stored with `-0.0` normalised to `+0.0`, in both `insert`
/// and `remove`. The library compares with `partial_cmp`, under which the two
/// are equal; `total_cmp`, used here so the order is total, puts `-0.0` first.
/// Normalising makes the two comparisons agree on every non-NaN value, so this
/// window stays in the library's order, and makes `remove` find a point
/// whichever zero it was written with. A route distance is an `abs()` or
/// `1.0 - abs()`, so `-0.0` should not arrive in practice; this is cheap
/// insurance rather than a known path.
#[derive(Debug, Clone, Default)]
struct SortedWindow {
    points: Vec<Point<f64>>,
}

impl SortedWindow {
    /// Build a window from `events`, sorting once.
    fn from_events<'a>(events: impl IntoIterator<Item = &'a IsotonicEvent>) -> Self {
        let mut points: Vec<Point<f64>> = events
            .into_iter()
            .map(|event| canonical_point(event.route_distance().as_f64(), event.result))
            .collect();
        points.sort_by(library_order);
        SortedWindow { points }
    }

    /// Insert `(x, y)`, after any points it compares equal to.
    fn insert(&mut self, x: f64, y: f64) {
        let point = canonical_point(x, y);
        let index = self
            .points
            .partition_point(|existing| library_order(existing, &point).is_le());
        self.points.insert(index, point);
    }

    /// Remove one point equal to `(x, y)`, returning whether one was found.
    ///
    /// Points with identical coordinates are interchangeable to the fit, so
    /// which of several duplicates goes does not matter. A missing point is a
    /// bookkeeping bug in the caller rather than a runtime condition; the
    /// caller answers it by rebuilding the window (see
    /// `IsotonicEstimator::resync_window`).
    #[must_use]
    fn remove(&mut self, x: f64, y: f64) -> bool {
        let point = canonical_point(x, y);
        let index = self
            .points
            .partition_point(|existing| library_order(existing, &point).is_lt());
        match self.points.get(index) {
            Some(existing) if library_order(existing, &point).is_eq() => {
                self.points.remove(index);
                true
            }
            _ => false,
        }
    }

    fn len(&self) -> usize {
        self.points.len()
    }

    fn as_slice(&self) -> &[Point<f64>] {
        &self.points
    }
}

/// `(x, y)` with each `-0.0` replaced by `+0.0`. See [`SortedWindow`].
fn canonical_point(x: f64, y: f64) -> Point<f64> {
    // IEEE 754: -0.0 + 0.0 == +0.0 under round-to-nearest; every other value,
    // NaN included, is unchanged.
    Point::new(x + 0.0, y + 0.0)
}

/// `pav_regression`'s input order: x ascending, then y descending. Total
/// (`total_cmp`) so that sorting and binary search are well defined; agrees
/// with the library's `partial_cmp` on canonical, non-NaN points.
fn library_order(a: &Point<f64>, b: &Point<f64>) -> std::cmp::Ordering {
    a.x().total_cmp(b.x()).then_with(|| b.y().total_cmp(a.y()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EstimatorType {
    /// Where the estimated value is expected to increase as distance increases
    Positive,
    /// Where the estimated value is expected to decrease as distance increases
    Negative,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum EstimationError {
    #[error("Insufficient data for estimation")]
    InsufficientData,
}

/// A routing event is a single request to a peer for a contract, and some value indicating
/// the result of the request, such as the time it took to retrieve the contract.
#[derive(Debug, Clone)]
pub(crate) struct IsotonicEvent {
    pub peer: PeerKeyLocation,
    pub contract_location: Location,
    /// The result of the routing event, which is used to train the estimator, typically the time
    /// but could also represent request success as 0.0 and failure as 1.0, and then be used
    /// to predict the probability of success.
    pub result: f64,
}

impl IsotonicEvent {
    fn route_distance(&self) -> Distance {
        let peer_location = self
            .peer
            .location()
            .ok_or(EstimationError::InsufficientData)
            .expect("IsotonicEvent should always carry a peer location");
        self.contract_location.distance(peer_location)
    }
}

/// Per-peer adjustment using an exponentially-weighted moving average (EWMA).
///
/// Each new observation is blended with the running average:
///   smoothed = alpha * new_value + (1 - alpha) * smoothed
///
/// `effective_count` tracks the decayed sample size so callers can decide
/// whether the peer has enough data to be trusted.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct Adjustment {
    smoothed: f64,
    effective_count: f64,
    #[serde(skip)]
    alpha: f64,
}

impl Default for Adjustment {
    fn default() -> Self {
        Self::new()
    }
}

impl Adjustment {
    fn new() -> Self {
        Self {
            smoothed: 0.0,
            effective_count: 0.0,
            alpha: EWMA_ALPHA,
        }
    }

    fn add(&mut self, value: f64) {
        if self.effective_count < 1.0 {
            // First real observation — set directly rather than blending with zero.
            self.smoothed = value;
        } else {
            self.smoothed = self.alpha * value + (1.0 - self.alpha) * self.smoothed;
        }
        // Decay the effective count and add 1 for this new observation.
        self.effective_count = 1.0 + (1.0 - self.alpha) * self.effective_count;
    }

    /// EWMA smoothed adjustment value.
    pub(crate) fn value(&self) -> f64 {
        self.smoothed
    }

    /// Effective number of events contributing to this adjustment (decayed).
    pub(crate) fn event_count(&self) -> u64 {
        self.effective_count.round() as u64
    }
}

// Tests

#[cfg(test)]
mod tests {

    use super::*;
    use tracing::debug;

    #[test]
    fn test_positive_peer_time_estimator() {
        let mut events = Vec::new();
        for _ in 0..100 {
            let peer = PeerKeyLocation::random();
            if peer.location().is_none() {
                debug!("Peer location is none for {peer:?}");
            }
            let contract_location = Location::random();
            events.push(simulate_positive_request(peer, contract_location));
        }

        let (training_events, testing_events) = events.split_at(events.len() / 2);

        let estimator =
            IsotonicEstimator::new(training_events.iter().cloned(), EstimatorType::Positive);

        let mut errors = Vec::new();
        for event in testing_events {
            let estimated_time = estimator
                .estimate_retrieval_time(&event.peer, event.contract_location)
                .unwrap();
            let actual_time = event.result;
            let error = (estimated_time - actual_time).abs();
            errors.push(error);
        }

        let average_error = errors.iter().sum::<f64>() / errors.len() as f64;
        debug!("Average error: {average_error}");
        // Threshold 0.02 to avoid flaky failures from random seed variation
        assert!(average_error < 0.02);
    }

    #[test]
    fn test_negative_peer_time_estimator() {
        let mut events = Vec::new();
        for _ in 0..100 {
            let peer = PeerKeyLocation::random();
            if peer.location().is_none() {
                debug!("Peer location is none for {peer:?}");
            }
            let contract_location = Location::random();
            events.push(simulate_negative_request(peer, contract_location));
        }

        let (training_events, testing_events) = events.split_at(events.len() / 2);

        let estimator =
            IsotonicEstimator::new(training_events.iter().cloned(), EstimatorType::Negative);

        let mut errors = Vec::new();
        for event in testing_events {
            let estimated_time = estimator
                .estimate_retrieval_time(&event.peer, event.contract_location)
                .unwrap();
            let actual_time = event.result;
            let error = (estimated_time - actual_time).abs();
            errors.push(error);
        }

        let average_error = errors.iter().sum::<f64>() / errors.len() as f64;
        debug!("Average error: {average_error}");
        // Threshold 0.02 to avoid flaky failures from random seed variation
        assert!(average_error < 0.02);
    }

    #[test]
    fn test_peer_adjustment_cannot_produce_negative_estimate() {
        // A strongly-negative per-peer EWMA adjustment (a peer far faster / more
        // reliable than the global fit) must not drive the estimate below zero:
        // these targets are physically non-negative, and a negative estimate both
        // distorts routing and renders below the x-axis on the dashboard chart.
        let mut events = Vec::new();
        for _ in 0..100 {
            let peer = PeerKeyLocation::random();
            let contract_location = Location::random();
            events.push(simulate_positive_request(peer, contract_location));
        }
        let mut estimator = IsotonicEstimator::new(events, EstimatorType::Positive);

        // Overwrite one peer's adjustment with a large negative value and enough
        // effective observations that it is actually applied.
        let fast_peer = PeerKeyLocation::random();
        let mut adjustment = Adjustment::new();
        for _ in 0..10 {
            adjustment.add(-1000.0);
        }
        assert!(
            adjustment.effective_count >= MIN_POINTS_FOR_REGRESSION as f64,
            "adjustment must have enough effective observations to be applied"
        );
        assert!(
            adjustment.value() < -1.0,
            "adjustment must be strongly negative"
        );
        estimator
            .peer_adjustments
            .insert(fast_peer.clone(), adjustment);

        // Estimate at the peer's own location (distance 0 → smallest global value).
        let contract_location = fast_peer.location().unwrap();
        let estimate = estimator
            .estimate_retrieval_time(&fast_peer, contract_location)
            .expect("estimator has enough data to produce an estimate");

        assert!(
            estimate >= 0.0,
            "per-peer adjusted estimate must be clamped to a non-negative value, got {estimate}"
        );
    }

    #[test]
    fn adjustment_mode_residual_and_apply() {
        // Additive: residual is the absolute difference; apply adds it back.
        assert_eq!(
            AdjustmentMode::Additive.residual(7.0, 10.0),
            Some(-3.0),
            "additive residual is observed - global"
        );
        assert_eq!(AdjustmentMode::Additive.apply(10.0, -3.0), 7.0);

        // Multiplicative: residual is the log-ratio; apply scales by exp().
        let r = AdjustmentMode::Multiplicative
            .residual(20.0, 10.0)
            .expect("positive inputs");
        assert!(
            (r - std::f64::consts::LN_2).abs() < 1e-12,
            "multiplicative residual of 20/10 must be ln(2), got {r}"
        );
        let applied = AdjustmentMode::Multiplicative.apply(10.0, std::f64::consts::LN_2);
        assert!(
            (applied - 20.0).abs() < 1e-9,
            "applying ln(2) to 10 must give 20, got {applied}"
        );

        // Multiplicative is undefined for non-positive inputs → skipped (None).
        assert_eq!(AdjustmentMode::Multiplicative.residual(0.0, 10.0), None);
        assert_eq!(AdjustmentMode::Multiplicative.residual(5.0, 0.0), None);
        assert_eq!(AdjustmentMode::Multiplicative.residual(-1.0, 10.0), None);

        // Neutral adjustment (0.0) is a no-op in both modes.
        assert_eq!(AdjustmentMode::Additive.apply(42.0, 0.0), 42.0);
        assert_eq!(AdjustmentMode::Multiplicative.apply(42.0, 0.0), 42.0);

        // Multiplicative can never produce a negative estimate, even for a huge
        // negative log-adjustment (the additive failure mode this design avoids).
        assert!(AdjustmentMode::Multiplicative.apply(10.0, -1000.0) >= 0.0);
    }

    #[test]
    fn multiplicative_estimate_scales_global_by_geometric_factor() {
        // Build a multiplicative-mode estimator with a real global fit.
        let mut events = Vec::new();
        for _ in 0..100 {
            let peer = PeerKeyLocation::random();
            let contract_location = Location::random();
            events.push(simulate_positive_request(peer, contract_location));
        }
        let mut estimator = IsotonicEstimator::new_with_mode(
            events,
            EstimatorType::Positive,
            AdjustmentMode::Multiplicative,
        );

        // A peer whose log-adjustment is ln(2): it is consistently ~2x the global.
        let peer = PeerKeyLocation::random();
        let mut adjustment = Adjustment::new();
        for _ in 0..10 {
            adjustment.add(std::f64::consts::LN_2);
        }
        assert!(adjustment.effective_count >= MIN_POINTS_FOR_REGRESSION as f64);
        estimator.peer_adjustments.insert(peer.clone(), adjustment);

        // At the peer's own location distance is 0; compare against global * 2.
        let contract_location = peer.location().unwrap();
        let global = estimator
            .global_regression
            .interpolate(0.0)
            .unwrap()
            .max(0.0);
        let estimate = estimator
            .estimate_retrieval_time(&peer, contract_location)
            .expect("enough data");
        let expected = global * 2.0;
        assert!(
            (estimate - expected).abs() <= 1e-6 + expected * 1e-9,
            "multiplicative estimate must be global*exp(ln2)=2*global ({expected}), got {estimate}"
        );

        // A strongly-negative log-adjustment scales toward (but not below) zero.
        let mut neg = Adjustment::new();
        for _ in 0..10 {
            neg.add(-1000.0);
        }
        estimator.peer_adjustments.insert(peer.clone(), neg);
        let est_neg = estimator
            .estimate_retrieval_time(&peer, contract_location)
            .expect("enough data");
        assert!(
            (0.0..1e-3).contains(&est_neg),
            "global*exp(-1000) must round to ~0 and stay non-negative, got {est_neg}"
        );
    }

    #[test]
    fn multiplicative_adjustment_via_add_event_orders_peers_by_ratio() {
        // End-to-end: build the multiplicative adjustment through the production
        // path — `add_event` -> `residual` (log-ratio) -> EWMA -> `apply` — rather
        // than hand-inserting an `Adjustment`. A slow peer (results ~2.0) must end
        // up predicted slower than a fast peer (results ~0.5) at the same distance.
        // This is the path a sign error or `ln`/`exp` inversion would corrupt.
        let mut estimator = IsotonicEstimator::new_with_mode(
            std::iter::empty(),
            EstimatorType::Positive,
            AdjustmentMode::Multiplicative,
        );
        for _ in 0..40 {
            estimator.add_event(IsotonicEvent {
                peer: PeerKeyLocation::random(),
                contract_location: Location::random(),
                result: 1.0,
            });
        }
        let fast_peer = PeerKeyLocation::random();
        let slow_peer = PeerKeyLocation::random();
        for _ in 0..20 {
            estimator.add_event(IsotonicEvent {
                peer: fast_peer.clone(),
                contract_location: Location::random(),
                result: 0.5,
            });
            estimator.add_event(IsotonicEvent {
                peer: slow_peer.clone(),
                contract_location: Location::random(),
                result: 2.0,
            });
        }
        // Both query at distance 0 (contract at own location) → identical global
        // base, so the ordering is decided purely by each peer's learned factor.
        let est_fast = estimator
            .estimate_retrieval_time(&fast_peer, fast_peer.location().unwrap())
            .expect("enough data");
        let est_slow = estimator
            .estimate_retrieval_time(&slow_peer, slow_peer.location().unwrap())
            .expect("enough data");
        assert!(
            est_slow > est_fast && est_fast > 0.0,
            "slow peer (2.0) must estimate higher than fast peer (0.5): slow={est_slow} fast={est_fast}"
        );
    }

    #[test]
    fn multiplicative_skips_non_positive_observations() {
        // A zero/negative response time can't be expressed in log space; `residual`
        // returns None and `add_event` must skip the peer entirely rather than
        // poisoning its EWMA with NaN/-inf.
        let mut estimator = IsotonicEstimator::new_with_mode(
            std::iter::empty(),
            EstimatorType::Positive,
            AdjustmentMode::Multiplicative,
        );
        for _ in 0..15 {
            estimator.add_event(IsotonicEvent {
                peer: PeerKeyLocation::random(),
                contract_location: Location::random(),
                result: 1.0,
            });
        }
        let peer = PeerKeyLocation::random();
        estimator.add_event(IsotonicEvent {
            peer: peer.clone(),
            contract_location: Location::random(),
            result: 0.0,
        });
        assert!(
            !estimator.peer_adjustments.contains_key(&peer),
            "a non-positive observation must be skipped in multiplicative mode (no adjustment entry created)"
        );
    }

    #[test]
    fn floor_base_preserves_multiplicative_factor_at_degenerate_base() {
        // Additive floors at exactly 0; multiplicative floors at a tiny positive
        // value so a degenerate (clamped) base does not annihilate the peer factor.
        assert_eq!(AdjustmentMode::Additive.floor_base(-1.0), 0.0);
        assert_eq!(AdjustmentMode::Additive.floor_base(5.0), 5.0);
        assert_eq!(AdjustmentMode::Multiplicative.floor_base(5.0), 5.0);
        let base = AdjustmentMode::Multiplicative.floor_base(-1.0);
        assert!(
            base > 0.0,
            "multiplicative base must stay positive, got {base}"
        );

        // The fix: at a clamped base, a slow peer (adj>0) is still predicted slower
        // than a fast peer (adj<0). Flooring to 0 (the bug) would tie both at 0.
        let slow = AdjustmentMode::Multiplicative.apply(base, 0.7);
        let fast = AdjustmentMode::Multiplicative.apply(base, -0.7);
        assert!(
            slow > fast && fast > 0.0,
            "multiplicative ordering must survive a degenerate base: slow={slow} fast={fast}"
        );
        assert_eq!(
            AdjustmentMode::Multiplicative.apply(0.0, 0.7),
            0.0,
            "sanity: a literal-0 base would annihilate the factor — the bug floor_base avoids"
        );
    }

    #[test]
    fn test_adjustment_ewma_recency() {
        let mut adj = Adjustment::new();

        // Feed 100 events with value 10.0 (simulating a "bad" period)
        for _ in 0..100 {
            adj.add(10.0);
        }
        let after_bad = adj.value();
        assert!(
            (after_bad - 10.0).abs() < 0.01,
            "EWMA should converge to 10.0, got {after_bad}"
        );

        // Now feed 20 events with value 0.0 (simulating recovery)
        for _ in 0..20 {
            adj.add(0.0);
        }
        let after_recovery = adj.value();
        // (1-0.1)^20 ≈ 0.12, so ~88% of the old 10.0 has decayed.
        assert!(
            after_recovery < 2.0,
            "EWMA should reflect recent 0.0 events after 20 observations, got {after_recovery}"
        );
    }

    #[test]
    fn test_adjustment_ewma_first_observation() {
        let mut adj = Adjustment {
            alpha: 0.5,
            ..Adjustment::new()
        };
        adj.add(5.0);
        assert_eq!(adj.value(), 5.0, "First observation should be set directly");
        assert_eq!(adj.event_count(), 1);

        adj.add(3.0);
        // alpha * 3.0 + (1-alpha) * 5.0 = 0.5 * 3.0 + 0.5 * 5.0 = 4.0
        assert!(
            (adj.value() - 4.0).abs() < 1e-10,
            "Second observation should blend via EWMA"
        );
    }

    #[test]
    fn test_rolling_window_eviction() {
        let peer = PeerKeyLocation::random();
        let contract = Location::random();

        let mut estimator = IsotonicEstimator::new(std::iter::empty(), EstimatorType::Positive);

        // Add more events than MAX_REGRESSION_POINTS
        for i in 0..(MAX_REGRESSION_POINTS + 100) {
            estimator.add_event(IsotonicEvent {
                peer: peer.clone(),
                contract_location: contract,
                result: i as f64,
            });
        }

        assert!(
            estimator.raw_events.len() <= MAX_REGRESSION_POINTS,
            "Raw points should be bounded, got {}",
            estimator.raw_events.len()
        );

        let result = estimator.estimate_retrieval_time(&peer, contract);
        assert!(
            result.is_ok(),
            "Estimator should produce estimates after eviction"
        );
    }

    #[test]
    fn sampled_raw_points_downsamples_and_bounds() {
        let peer = PeerKeyLocation::random();
        let contract = Location::random();
        let mut estimator = IsotonicEstimator::new(std::iter::empty(), EstimatorType::Positive);

        // No data yet.
        assert!(estimator.sampled_raw_points(50).is_empty());

        for i in 0..40 {
            estimator.add_event(IsotonicEvent {
                peer: peer.clone(),
                contract_location: contract,
                result: i as f64,
            });
        }

        // Fewer than the cap -> every retained point is returned.
        assert_eq!(estimator.sampled_raw_points(100).len(), 40);
        // More than the cap -> downsampled to exactly the cap.
        let strided = estimator.sampled_raw_points(10);
        assert_eq!(strided.len(), 10);
        // Striding preserves insertion order (outcomes were added monotonically
        // 0..40), so a returned-first-before-returned-newest check catches a
        // regression that returned the first N points or reversed the window.
        assert!(
            strided.first().unwrap().1 < strided.last().unwrap().1,
            "downsample should span oldest..newest in order, got {strided:?}",
        );
        // Degenerate cap.
        assert!(estimator.sampled_raw_points(0).is_empty());
    }

    #[test]
    fn test_estimator_adapts_to_regime_change() {
        let peer = PeerKeyLocation::random();
        let contract = Location::new(0.0);

        let mut estimator = IsotonicEstimator::new(std::iter::empty(), EstimatorType::Positive);

        // Phase 1: build up global regression with several peers at value 100.0
        let peers: Vec<PeerKeyLocation> = (0..5).map(|_| PeerKeyLocation::random()).collect();
        for _ in 0..30 {
            for p in &peers {
                estimator.add_event(IsotonicEvent {
                    peer: p.clone(),
                    contract_location: contract,
                    result: 100.0,
                });
            }
        }
        // Target peer is "slow" — higher than average
        for _ in 0..20 {
            estimator.add_event(IsotonicEvent {
                peer: peer.clone(),
                contract_location: contract,
                result: 200.0,
            });
        }

        let estimate_before = estimator
            .estimate_retrieval_time(&peer, contract)
            .unwrap_or(0.0);

        // Phase 2: peer becomes "fast"
        for _ in 0..20 {
            estimator.add_event(IsotonicEvent {
                peer: peer.clone(),
                contract_location: contract,
                result: 50.0,
            });
        }

        let estimate_after = estimator
            .estimate_retrieval_time(&peer, contract)
            .unwrap_or(0.0);

        assert!(
            estimate_after < estimate_before,
            "Estimate should decrease after peer improves: before={estimate_before}, after={estimate_after}"
        );
    }

    /// Deterministic per-peer noise derived from the public key hash.
    fn peer_noise(peer: &PeerKeyLocation) -> f64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        format!("{}", peer.pub_key()).hash(&mut hasher);
        (hasher.finish() as u8) as f64
    }

    fn simulate_request(
        peer: PeerKeyLocation,
        contract_location: Location,
        result_fn: impl FnOnce(f64) -> f64,
    ) -> IsotonicEvent {
        let distance = peer
            .location()
            .unwrap()
            .distance(contract_location)
            .as_f64();
        let result = result_fn(distance) + peer_noise(&peer);
        IsotonicEvent {
            peer,
            contract_location,
            result,
        }
    }

    /// Build `count` positive events with distinct random peers.
    fn positive_events(count: usize) -> Vec<IsotonicEvent> {
        (0..count)
            .map(|_| simulate_positive_request(PeerKeyLocation::random(), Location::random()))
            .collect()
    }

    /// An event whose `route_distance()` is EXACTLY `distance`, built by placing
    /// the contract at ring offset `distance` from `peer`.
    ///
    /// A peer's location is derived from its address and cannot be set, and
    /// `route_distance()` is `contract.distance(peer_location)`, which folds
    /// `|a-b|` around 0.5. So a test that picks contract locations directly has no
    /// control over the regression's x-axis — the random peer location turns it
    /// into a tent transform of the intended value. Deriving the contract FROM the
    /// peer inverts that: for `distance` in `[0.0, 0.5]`, `route_distance()` is
    /// `distance` exactly, whatever the peer happens to be.
    fn event_at_distance(peer: &PeerKeyLocation, distance: f64, result: f64) -> IsotonicEvent {
        let peer_location = peer
            .location()
            .expect("PeerKeyLocation::random always yields a known address");
        let contract_location = Location::new((peer_location.as_f64() + distance).rem_euclid(1.0));
        IsotonicEvent {
            peer: peer.clone(),
            contract_location,
            result,
        }
    }

    /// The trigger is `events_since_refit * 100 > len * 10`, where `len` is the
    /// window AFTER the adds. Seeded with 100, adding k gives len = 100 + k, so
    /// the boundary is the smallest k with `100k > 10(100 + k)`, i.e. k > 11.11 —
    /// k = 12 fires, k = 11 does not. Pinning BOTH sides matters: bracketing only
    /// 10 and 20 leaves `>` vs `>=` (and several off-by-ones) undetected.
    ///
    /// This side pins the HOLD-OFF: `add_event` must not refit eagerly. Since
    /// #4811 the trigger is evaluated inside `add_event`, so an over-eager trigger
    /// would refit on the relay hot path every time, which is exactly the cost the
    /// amortisation argument rules out.
    #[test]
    fn add_event_holds_off_just_below_the_turnover_boundary() {
        let mut estimator = IsotonicEstimator::new(positive_events(100), EstimatorType::Positive);
        assert_eq!(estimator.events_since_refit, 0, "constructor starts fresh");

        for event in positive_events(11) {
            estimator.add_event(event);
        }
        assert!(
            !estimator.is_stale_for_test(),
            "11 new events over a 111-event window is 9.9% — under the trigger"
        );
        assert_eq!(
            estimator.events_since_refit, 11,
            "a held-off refit must not clear the counter, or turnover never \
             accumulates — 11 adds must still be pending, un-refit"
        );
    }

    /// The firing side of the boundary above, and the core #4811 pin: the 12th
    /// `add_event` — the one that earns the refit — performs it ITSELF, with no
    /// poller involved.
    ///
    /// Before #4811 this could only be observed by a caller polling
    /// `refit_if_stale()`, and the refit landed whenever that caller next ran
    /// (`Ring::refit_router_periodically`'s 5-minute tick). Now it is synchronous
    /// with the event that earns it: `events_since_refit` is back to 0 by the time
    /// `add_event` returns. Mutation check: deleting the `refit_if_stale()` call
    /// from `add_event` leaves the counter at 12 and fails both assertions.
    #[test]
    fn add_event_refits_inline_at_the_turnover_boundary() {
        let mut estimator = IsotonicEstimator::new(positive_events(100), EstimatorType::Positive);
        for event in positive_events(11) {
            estimator.add_event(event);
        }
        assert_eq!(
            estimator.events_since_refit, 11,
            "sanity: 11 adds are pending and un-refit (see the hold-off guard)"
        );

        // The 12th add crosses 12*100 > 112*10 and must refit on the spot.
        estimator.add_event(positive_events(1).pop().expect("one event"));

        assert_eq!(
            estimator.events_since_refit, 0,
            "the add that earns the refit must perform it inline, resetting the \
             turnover counter before it returns (#4811)"
        );
        assert!(
            !estimator.is_stale_for_test(),
            "an estimator must never be left stale once add_event returns"
        );
    }

    #[test]
    fn add_event_refits_inline_at_exactly_min_points() {
        // Pins the `< MIN_POINTS_FOR_REGRESSION` guard's boundary: one fewer must
        // hold off (covered by `add_event_never_refits_below_min_points`), exactly
        // MIN must be allowed through — and, since #4811, by `add_event` itself.
        let mut estimator = IsotonicEstimator::new(std::iter::empty(), EstimatorType::Positive);
        for event in positive_events(MIN_POINTS_FOR_REGRESSION) {
            estimator.add_event(event);
        }
        assert_eq!(
            estimator.events_since_refit, 0,
            "at exactly MIN_POINTS_FOR_REGRESSION the guard must allow the refit, \
             and add_event must have performed it"
        );
        assert!(!estimator.is_stale_for_test());
    }

    #[test]
    fn refit_rebuilds_peer_adjustments_against_the_new_curve() {
        // A refit moves `global_regression`. Every `Adjustment` is an EWMA of
        // residuals measured AGAINST that curve, so leaving them untouched would
        // correct each peer relative to a curve that no longer exists. The batch
        // constructor rebuilds both together; a refit must too.
        //
        // Pinned by equivalence: after a refit, the estimator must match a batch
        // build over the same window in the thing routing actually consumes —
        // `estimate_retrieval_time` — not merely in the global curve.
        let seed = positive_events(150);

        let mut incremental = IsotonicEstimator::new(std::iter::empty(), EstimatorType::Positive);
        for event in seed.iter().cloned() {
            incremental.add_event(event);
        }
        incremental.refit();

        let batch = IsotonicEstimator::new(seed.clone(), EstimatorType::Positive);

        assert_eq!(
            incremental.peer_adjustments.len(),
            batch.peer_adjustments.len(),
            "a refit must produce the same peer set as a batch build over the \
             same window — otherwise peer_adjustments is not bounded by the window"
        );

        for event in &seed {
            let refit_est =
                incremental.estimate_retrieval_time(&event.peer, event.contract_location);
            let batch_est = batch.estimate_retrieval_time(&event.peer, event.contract_location);
            match (refit_est, batch_est) {
                (Ok(a), Ok(b)) => assert!(
                    (a - b).abs() < 1e-9,
                    "refit and batch disagree on the peer-adjusted estimate: {a} vs {b}"
                ),
                (Err(_), Err(_)) => {}
                (a, b) => panic!("refit/batch disagree on estimability: {a:?} vs {b:?}"),
            }
        }
    }

    #[test]
    fn refit_prunes_peers_that_fell_out_of_the_window() {
        // `raw_events` evicts at MAX_REGRESSION_POINTS, but `peer_adjustments`
        // only ever inserts on the incremental path. Without a rebuild the map
        // would grow one entry per peer EVER seen — an unbounded per-key
        // collection driven by remote peers (see .claude/rules/code-style.md) —
        // and a returning peer's stale adjustment would apply at full weight,
        // since Adjustment::effective_count has no time decay.
        let mut estimator = IsotonicEstimator::new(std::iter::empty(), EstimatorType::Positive);

        // Fill the window, then push it fully over with a disjoint peer set.
        //
        // Deliberately via `add_event_incremental`, NOT `add_event`: since #4811
        // `add_event` refits inline, and a refit is precisely what prunes the map.
        // Feeding through `add_event` would prune as it went and destroy this
        // test's premise — the unbounded growth it exists to demonstrate.
        for event in positive_events(MAX_REGRESSION_POINTS) {
            estimator.add_event_incremental(event);
        }
        for event in positive_events(MAX_REGRESSION_POINTS) {
            estimator.add_event_incremental(event);
        }
        assert_eq!(
            estimator.raw_events.len(),
            MAX_REGRESSION_POINTS,
            "sanity: the event window stays capped"
        );
        assert!(
            estimator.peer_adjustments.len() > MAX_REGRESSION_POINTS,
            "sanity: the incremental path really does accumulate evicted peers \
             (got {})",
            estimator.peer_adjustments.len()
        );

        estimator.refit();
        assert!(
            estimator.peer_adjustments.len() <= MAX_REGRESSION_POINTS,
            "a refit must bound peer_adjustments to peers in the current window \
             (got {} for a {}-event window)",
            estimator.peer_adjustments.len(),
            MAX_REGRESSION_POINTS
        );
    }

    #[test]
    fn add_event_never_refits_below_min_points() {
        // Under MIN_POINTS_FOR_REGRESSION the regression refuses to estimate at
        // all, so refitting buys nothing and must not thrash. Every one of these
        // adds is past the turnover percentage (k*100 > k*10 for any k >= 1), so
        // only the MIN_POINTS guard holds them off — which makes this the guard's
        // real pin now that `add_event` evaluates the trigger on every call.
        let mut estimator = IsotonicEstimator::new(std::iter::empty(), EstimatorType::Positive);
        for event in positive_events(MIN_POINTS_FOR_REGRESSION - 1) {
            estimator.add_event(event);
        }
        assert!(!estimator.is_stale_for_test());
        assert_eq!(
            estimator.events_since_refit,
            MIN_POINTS_FOR_REGRESSION - 1,
            "below MIN_POINTS_FOR_REGRESSION no refit may run, so the turnover \
             counter must still be carrying every add"
        );
    }

    #[test]
    fn add_event_preserves_every_observation_across_its_inline_refits() {
        // The regression this guards: a refit must never DISCARD data. The bug it
        // replaces rebuilt the router from an on-disk log that lacked relay
        // events, so the model shrank on every pass (#4808).
        //
        // Since #4811 the refits happen inside these `add_event` calls (50 adds
        // over a 200-event seed crosses the trigger twice), so this now pins the
        // no-loss property across the real, inline mechanism rather than across a
        // hand-invoked one.
        let mut estimator = IsotonicEstimator::new(positive_events(200), EstimatorType::Positive);
        let before = estimator.len();

        for event in positive_events(50) {
            estimator.add_event(event);
        }

        assert_eq!(
            estimator.len(),
            before + 50,
            "add_event must preserve every observation across the refits it \
             performs, not shrink the model"
        );
        assert_eq!(
            estimator.raw_events.len(),
            250,
            "the refit rebuilds from raw_events; it must not disturb the window"
        );
    }

    /// Regression test for #5658: the global fit must equal a batch fit over the
    /// current window after EVERY event, including every eviction.
    ///
    /// Until #5658 the window was maintained with `pav_regression`'s
    /// `add_points` / `remove_points`, which are approximate (0.7.0):
    /// `remove_points` subtracts the evicted point from the CLOSEST pooled
    /// aggregate by x, which need not be the aggregate it was pooled into. Once
    /// it lands in the wrong one, the subtraction yields an aggregate no data
    /// could produce — observed while diagnosing the #5658 flaky router test, a
    /// failure-probability curve holding the point `(distance = -0.062,
    /// failure = -5.0)`, and a peer EWMA of +18 learned against it. The periodic
    /// refit repaired that only every 51st event, and routing read the corrupted
    /// fit in between: on failure-shaped data like the first stream below,
    /// roughly one state in five carried an out-of-range aggregate, with
    /// interpolation errors up to 2.16 in probability units.
    ///
    /// The previous version of this test asserted the OPPOSITE (that the
    /// incremental fit drifts, as a tripwire for the refit's existence) and
    /// checked only `drift > 1e-9`, so it never measured how large the drift was.
    ///
    /// Streams cover both directions, and the two shapes where the window's
    /// bookkeeping is easiest to get wrong: many points at one distance with
    /// different results (so removing "a point at that distance" is not the
    /// same as removing THE point), and a window where every distance is equal.
    ///
    /// # What "matches" means
    ///
    /// A fit is its pooled blocks plus a centroid (the mean of every point, used
    /// to extrapolate past the ends), and `interpolate` is a pure function of
    /// the two. Both are compared directly, to a relative tolerance of 1e-12:
    /// the centroid is a sum, and the window hands points to the library in
    /// sorted order while the reference gets them in arrival order, so the two
    /// can differ in the last bit.
    ///
    /// Interpolated values are deliberately NOT compared. An earlier version
    /// did, at fixed distances, and failed 58 of 200 runs on the
    /// single-distance descending stream with the blocks bit-identical: with
    /// every point at one distance, blocks and centroid sit within an ulp of
    /// each other, interpolating divides by that ulp, and a one-ulp difference
    /// in the centroid became a difference of 0.5 in estimates of order 1e15 (or
    /// NaN, or inf). Those values are garbage either way; that the estimator can
    /// produce them from a degenerate window is #5665.
    #[test]
    fn incremental_fit_matches_batch_after_every_event() {
        let peer = PeerKeyLocation::random();

        // Deterministic arithmetic rather than an RNG, so a failure reproduces.
        // Every stream runs well past MAX_REGRESSION_POINTS so eviction is
        // exercised in steady state.
        let long = MAX_REGRESSION_POINTS * 3;
        // Binary outcomes whose rate rises with distance: the shape the router's
        // failure estimator sees, and the one that produced the corrupt aggregates.
        let failure_shaped: Vec<(f64, f64)> = (0..long)
            .map(|i| {
                let x = ((i * 7919) % 500) as f64 / 1000.0; // [0.0, 0.499]
                let threshold = 1 + (x * 10.0) as usize; // failure rate 10%..60%
                let y = if (i * 31) % 10 < threshold { 1.0 } else { 0.0 };
                (x, y)
            })
            .collect();
        // Adversarially non-monotonic, so PAV must pool heavily.
        let non_monotonic: Vec<(f64, f64)> = (0..(MAX_REGRESSION_POINTS + 200))
            .map(|i| {
                let x = (i % 50) as f64 / 100.0;
                let y = if i % 3 == 0 { 1.0 - x } else { x };
                (x, y)
            })
            .collect();
        // A rate that falls with distance, noisy enough to pool: the
        // transfer-rate estimator's shape, for the Negative direction.
        let falling: Vec<(f64, f64)> = (0..long)
            .map(|i| {
                let x = ((i * 3571) % 500) as f64 / 1000.0;
                let noise = ((i * 17) % 11) as f64 * 40.0;
                (x, 1000.0 - 1500.0 * x + noise)
            })
            .collect();
        // Twenty distances, each carrying several different results.
        let repeated_distance: Vec<(f64, f64)> = (0..long)
            .map(|i| {
                let x = ((i * 7) % 20) as f64 / 40.0; // [0.0, 0.475]
                let y = ((i * 13) % 7) as f64 / 6.0;
                (x, y)
            })
            .collect();
        // One distance for the entire window.
        let single_distance: Vec<(f64, f64)> = (0..long)
            .map(|i| (0.25, ((i * 13) % 7) as f64 / 6.0))
            .collect();

        let cases = [
            ("failure-shaped", EstimatorType::Positive, &failure_shaped),
            ("non-monotonic", EstimatorType::Positive, &non_monotonic),
            ("falling", EstimatorType::Negative, &falling),
            (
                "repeated-distance/asc",
                EstimatorType::Positive,
                &repeated_distance,
            ),
            (
                "repeated-distance/desc",
                EstimatorType::Negative,
                &repeated_distance,
            ),
            (
                "single-distance/asc",
                EstimatorType::Positive,
                &single_distance,
            ),
            (
                "single-distance/desc",
                EstimatorType::Negative,
                &single_distance,
            ),
        ];

        for (label, estimator_type, stream) in cases {
            let (y_min, y_max) = stream
                .iter()
                .fold((f64::INFINITY, f64::NEG_INFINITY), |(lo, hi), &(_, y)| {
                    (lo.min(y), hi.max(y))
                });
            // Through `add_event_incremental`, NOT `add_event`: the latter also
            // refits every 51st event, which would mask exactly the between-refit
            // states this pins.
            let mut estimator = IsotonicEstimator::new(std::iter::empty(), estimator_type);
            let mut window: VecDeque<Point<f64>> = VecDeque::new();
            let close = |a: f64, b: f64, tolerance: f64| {
                (a - b).abs() <= tolerance * (1.0 + a.abs().max(b.abs()))
            };
            for (index, &(x, y)) in stream.iter().enumerate() {
                let event = event_at_distance(&peer, x, y);
                // The reference is built from the distance the estimator will
                // compute, not from `x`: placing a contract at a ring offset
                // round-trips through floating point.
                window.push_back(Point::new(event.route_distance().as_f64(), y));
                if window.len() > MAX_REGRESSION_POINTS {
                    window.pop_front();
                }
                estimator.add_event_incremental(event);

                let batch = match estimator_type {
                    EstimatorType::Positive => {
                        IsotonicRegression::new_ascending(window.make_contiguous())
                    }
                    EstimatorType::Negative => {
                        IsotonicRegression::new_descending(window.make_contiguous())
                    }
                }
                .expect("a fit without intersect_origin cannot fail");

                let got = estimator.global_regression.get_points_sorted();
                let want = batch.get_points_sorted();
                let blocks_match = got.len() == want.len()
                    && got.iter().zip(&want).all(|(g, w)| {
                        close(*g.x(), *w.x(), 1e-12)
                            && close(*g.y(), *w.y(), 1e-12)
                            && close(g.weight(), w.weight(), 1e-12)
                    });
                assert!(
                    blocks_match,
                    "{label}: after event {index} the incremental fit's blocks differ \
                     from a batch fit over the same window; routing reads this fit \
                     between refits.\n  incremental: {got:?}\n  batch:       {want:?}"
                );

                let (Some(got_centroid), Some(want_centroid)) = (
                    estimator.global_regression.get_centroid_point(),
                    batch.get_centroid_point(),
                ) else {
                    panic!("{label}: a non-empty fit must have a centroid");
                };
                assert!(
                    close(*got_centroid.x(), *want_centroid.x(), 1e-12)
                        && close(*got_centroid.y(), *want_centroid.y(), 1e-12),
                    "{label}: after event {index} the centroid differs beyond rounding: \
                     {got_centroid:?} vs {want_centroid:?}"
                );

                for point in &got {
                    assert!(
                        (0.0..=0.5).contains(point.x()) && (y_min..=y_max).contains(point.y()),
                        "{label}: after event {index} the fit holds the aggregate \
                         ({}, {}), which no point in the window could produce",
                        point.x(),
                        point.y()
                    );
                }
            }
            assert_eq!(estimator.raw_events.len(), MAX_REGRESSION_POINTS);
            assert_eq!(
                estimator.sorted_points.as_slice().len(),
                MAX_REGRESSION_POINTS,
                "{label}: the sorted window must track raw_events exactly"
            );
            // The self-heal would mask a bookkeeping bug from every assertion
            // above, so require that it never ran. This is the tripwire the
            // `debug_assert!` in `SortedWindow::remove` used to be.
            assert_eq!(
                estimator.window_resyncs, 0,
                "{label}: the sorted window fell out of step with raw_events"
            );
        }
    }

    /// The window's two bookkeeping invariants, checked directly rather than
    /// through a fit that might happen to mask them.
    ///
    /// 1. Order is `pav_regression`'s own: distance ascending, result DESCENDING
    ///    among equal distances. This is spelled out here independently of
    ///    `library_order`, so changing that function to (say) distance-only
    ///    order fails the test instead of redefining what it checks. Getting it
    ///    wrong costs no correctness, only the linear-time sort, which is why
    ///    nothing else would notice.
    /// 2. `-0.0` and `+0.0` are one value: a point inserted with either zero is
    ///    stored as `+0.0` and removed by either spelling.
    #[test]
    fn sorted_window_keeps_library_order_and_normalises_signed_zero() {
        let mut window = SortedWindow::default();
        let inserted: Vec<(f64, f64)> = vec![
            (0.2, 0.0),
            (0.1, 1.0),
            (0.2, 1.0),
            (-0.0, 0.5),
            (0.0, 0.25),
            (0.2, 0.5),
            (0.0, -0.0),
            (-0.0, 1.0),
            (0.1, 0.0),
            (0.2, 1.0),
        ];
        for &(x, y) in &inserted {
            window.insert(x, y);
            assert!(
                in_library_order(window.as_slice()),
                "after inserting ({x}, {y}) the window left pav_regression's order: {:?}",
                window.as_slice()
            );
        }
        for point in window.as_slice() {
            assert!(
                !point.x().is_sign_negative() && !point.y().is_sign_negative(),
                "signed zero must be normalised on insert, found ({}, {})",
                point.x(),
                point.y()
            );
        }

        // Remove with the OPPOSITE zero to the one each point was inserted with,
        // in an order unrelated to insertion. Every removal must find its point.
        let flip = |v: f64| if v == 0.0 { -v } else { v };
        for &(x, y) in inserted.iter().rev() {
            assert!(
                window.remove(flip(x), flip(y)),
                "removing ({}, {}) missed; the window holds {:?}",
                flip(x),
                flip(y),
                window.as_slice()
            );
            assert!(in_library_order(window.as_slice()));
        }
        assert!(
            window.as_slice().is_empty(),
            "every inserted point must be removable, left {:?}",
            window.as_slice()
        );

        // Removing among duplicates at one distance must take the matching
        // result, not merely the first point at that distance.
        let mut window = SortedWindow::default();
        for y in [1.0, 0.0, 0.5] {
            window.insert(0.3, y);
        }
        assert!(window.remove(0.3, 0.0));
        assert!(
            !window.remove(0.3, 0.25),
            "a point that was never inserted must be reported missing"
        );
        let remaining: Vec<f64> = window.as_slice().iter().map(|p| *p.y()).collect();
        assert_eq!(remaining, vec![1.0, 0.5]);
    }

    /// `pav_regression`'s input order, written out independently of
    /// `library_order` so that changing that function (to distance-only order,
    /// say) fails the tests that use this instead of redefining what they check.
    fn in_library_order(points: &[Point<f64>]) -> bool {
        points.windows(2).all(|pair| {
            let (a, b) = (&pair[0], &pair[1]);
            a.x() < b.x() || (a.x() == b.x() && a.y() >= b.y())
        })
    }

    fn coordinates(points: &[Point<f64>]) -> Vec<(f64, f64)> {
        points.iter().map(|p| (*p.x(), *p.y())).collect()
    }

    /// `SortedWindow::from_events` builds the constructor's window (and every
    /// resync's) with a sort rather than by insertion, so it needs its own pin:
    /// the insertion path's test says nothing about it. It must produce the
    /// library's order, and exactly the window that inserting the same events
    /// one at a time produces.
    ///
    /// The events repeat distances with differing results, arriving in an order
    /// unrelated to the sorted one, which is the case where "sorted by distance"
    /// and "in the library's order" differ.
    #[test]
    fn from_events_builds_the_library_order_that_inserting_does() {
        let peer = PeerKeyLocation::random();
        let events: Vec<IsotonicEvent> = (0..60)
            .map(|i| {
                let x = ((i * 7) % 5) as f64 / 10.0;
                let y = ((i * 11) % 4) as f64 / 3.0;
                event_at_distance(&peer, x, y)
            })
            .collect();

        let built = SortedWindow::from_events(&events);
        assert!(
            in_library_order(built.as_slice()),
            "from_events left pav_regression's order: {:?}",
            built.as_slice()
        );

        let mut inserted = SortedWindow::default();
        for event in &events {
            inserted.insert(event.route_distance().as_f64(), event.result);
        }
        assert_eq!(
            coordinates(built.as_slice()),
            coordinates(inserted.as_slice()),
            "from_events and one-at-a-time insertion must build the same window"
        );

        // And the constructor, which is where `from_events` is used.
        let estimator = IsotonicEstimator::new(events, EstimatorType::Positive);
        assert!(in_library_order(estimator.sorted_points.as_slice()));
    }

    /// A window that has fallen out of step with `raw_events` is rebuilt from
    /// it on the next event, rather than carrying the error forever.
    ///
    /// Three corruptions, each made directly, since correct bookkeeping cannot
    /// produce any of them. Each is caught by a different part of the check:
    ///
    /// - A point replaced: the window is the right size but holds a point
    ///   `raw_events` does not. Nothing looks wrong until that point's event is
    ///   evicted and its removal misses. A miss leaves the window one point
    ///   long, so the length comparison fires as well.
    /// - A point replaced and another dropped: the missed removal leaves the
    ///   lengths EQUAL, so only the miss itself can notice. Without this case
    ///   the `!evicted_in_step` half of the check could be deleted unnoticed.
    /// - A point too many, below capacity: no eviction happens at all, so only
    ///   the length comparison can notice.
    ///
    /// After the rebuild, the EWMAs, trained against fits over the wrong window,
    /// must be re-anchored to the repaired fit.
    ///
    /// The test sets `RESYNC_EXPECTED`; any other test that drives a window out
    /// of step panics instead (see `resync_window`).
    ///
    /// Mutation checks: without the `resync_window()` call the stray point stays
    /// in the window and in the fit, and every case fails; without the miss
    /// check the second case fails; without the re-anchor the EWMA assertions
    /// fail.
    #[test]
    fn a_desynced_sorted_window_is_rebuilt_from_its_events() {
        RESYNC_EXPECTED.with(|expected| expected.set(true));
        let peer = PeerKeyLocation::random();
        // Every distance distinct, so removing the oldest point cannot be
        // satisfied by a duplicate of it elsewhere in the window.
        let event = |i: usize| {
            let x = (i % (2 * MAX_REGRESSION_POINTS)) as f64 / (4 * MAX_REGRESSION_POINTS) as f64;
            event_at_distance(&peer, x, (i % 3) as f64 / 2.0)
        };
        const STRAY: (f64, f64) = (0.499_9, 99.0);

        type Corruption = fn(&mut IsotonicEstimator);
        let cases: [(&str, usize, Corruption); 3] = [
            (
                "point replaced, caught at its eviction",
                MAX_REGRESSION_POINTS,
                |estimator| {
                    let oldest = estimator
                        .raw_events
                        .front()
                        .expect("window is full")
                        .clone();
                    assert!(
                        estimator
                            .sorted_points
                            .remove(oldest.route_distance().as_f64(), oldest.result)
                    );
                    estimator.sorted_points.insert(STRAY.0, STRAY.1);
                },
            ),
            (
                "point replaced and another dropped, so only the miss shows",
                MAX_REGRESSION_POINTS,
                |estimator| {
                    let oldest = estimator
                        .raw_events
                        .front()
                        .expect("window is full")
                        .clone();
                    let newest = estimator.raw_events.back().expect("window is full").clone();
                    for event in [&oldest, &newest] {
                        assert!(
                            estimator
                                .sorted_points
                                .remove(event.route_distance().as_f64(), event.result)
                        );
                    }
                    estimator.sorted_points.insert(STRAY.0, STRAY.1);
                },
            ),
            (
                "point too many, below capacity",
                MAX_REGRESSION_POINTS - 10,
                |estimator| {
                    estimator.sorted_points.insert(STRAY.0, STRAY.1);
                },
            ),
        ];

        for (label, fill, corrupt) in cases {
            let mut estimator = IsotonicEstimator::new(std::iter::empty(), EstimatorType::Positive);
            for i in 0..fill {
                estimator.add_event_incremental(event(i));
            }
            corrupt(&mut estimator);
            assert_eq!(estimator.window_resyncs, 0, "{label}: sanity");

            estimator.add_event_incremental(event(fill));

            assert_eq!(
                estimator.window_resyncs, 1,
                "{label}: the desync must be noticed on the next event"
            );
            assert_eq!(
                coordinates(estimator.sorted_points.as_slice()),
                coordinates(SortedWindow::from_events(&estimator.raw_events).as_slice()),
                "{label}: the window must be rebuilt from raw_events"
            );
            let batch = IsotonicRegression::new_ascending(estimator.sorted_points.as_slice())
                .expect("a fit without intersect_origin cannot fail");
            assert_eq!(
                coordinates(&estimator.global_regression.get_points_sorted()),
                coordinates(&batch.get_points_sorted()),
                "{label}: the fit must be over the repaired window"
            );
            assert!(
                estimator
                    .global_regression
                    .get_points()
                    .iter()
                    .all(|point| *point.y() <= 1.0),
                "{label}: the stray point must be gone from the fit"
            );

            // The EWMAs were trained against fits over the wrong window while it
            // was out of step: the resync must re-anchor them to the repaired fit.
            assert_eq!(
                estimator.events_since_refit, 0,
                "{label}: the resync must re-anchor the peer adjustments"
            );
            let anchored = IsotonicEstimator::anchor_peer_adjustments(
                &estimator.raw_events,
                &estimator.global_regression,
                estimator.adjustment_mode,
            );
            let summary = |adjustments: &HashMap<PeerKeyLocation, Adjustment>| {
                let mut rows: Vec<(String, f64, f64)> = adjustments
                    .iter()
                    .map(|(peer, a)| (format!("{peer:?}"), a.smoothed, a.effective_count))
                    .collect();
                rows.sort_by(|a, b| a.0.cmp(&b.0));
                rows
            };
            assert_eq!(
                summary(&estimator.peer_adjustments),
                summary(&anchored),
                "{label}: the peer adjustments must be the ones anchored to the repaired fit"
            );

            // Back in step: the next event must not resync again.
            estimator.add_event_incremental(event(fill + 1));
            assert_eq!(estimator.window_resyncs, 1, "{label}: resynced twice");
        }
    }

    /// An estimator that fits on read must show its readers exactly the curve
    /// one that fits on every event shows, from the same events. The router's
    /// per-operation dashboard curves are built this way (#5662), so this is
    /// what makes that change invisible on the dashboard.
    ///
    /// Compared with `==`, not a tolerance: both fit the same sorted window with
    /// the same function, so any difference at all is a bug. Checked at points
    /// through warm-up, saturation and steady-state eviction, in both
    /// directions. Mutation check: returning the stored (never built) fit from
    /// `fit()` makes every on-read reader see an empty curve and fails this.
    #[test]
    fn fit_on_read_shows_the_same_curve_as_fitting_every_event() {
        let peer = PeerKeyLocation::random();
        let contract = event_at_distance(&peer, 0.2, 0.0).contract_location;
        let total = 2 * MAX_REGRESSION_POINTS + 37;
        for estimator_type in [EstimatorType::Positive, EstimatorType::Negative] {
            let mut every = IsotonicEstimator::new(std::iter::empty(), estimator_type);
            let mut on_read = IsotonicEstimator::new_fit_on_read(
                std::iter::empty(),
                estimator_type,
                AdjustmentMode::Additive,
            );
            for i in 0..total {
                let x = ((i * 7919) % 500) as f64 / 1000.0;
                let y = if (i * 31) % 10 < 1 + (x * 10.0) as usize {
                    1.0
                } else {
                    0.0
                };
                let event = event_at_distance(&peer, x, y);
                every.add_event(event.clone());
                on_read.add_event(event);

                if i < 12 || i % 97 == 0 || i + 1 == total {
                    let label = format!("{estimator_type:?} after event {i}");
                    assert_eq!(on_read.len(), every.len(), "{label}: len");
                    assert_eq!(
                        on_read.data_x_range(),
                        every.data_x_range(),
                        "{label}: data range"
                    );
                    assert_eq!(
                        on_read.sampled_curve(0.0, 1.0, 50),
                        every.sampled_curve(0.0, 1.0, 50),
                        "{label}: sampled curve"
                    );
                    assert_eq!(
                        on_read.estimate_global(&peer, contract),
                        every.estimate_global(&peer, contract),
                        "{label}: global estimate"
                    );
                    assert_eq!(
                        on_read.sampled_curve_and_range(0.0, 1.0, 50),
                        (every.sampled_curve(0.0, 1.0, 50), every.data_x_range()),
                        "{label}: curve and range from one fit"
                    );
                }
            }
            assert!(
                !every.sampled_curve(0.0, 1.0, 50).is_empty(),
                "sanity: the reference must have a curve, or the comparison is vacuous"
            );
            assert!(
                on_read.global_regression.is_empty(),
                "the on-read estimator must never build the stored fit"
            );
            assert!(
                on_read.peer_adjustments.is_empty() && on_read.events_since_refit == 0,
                "the on-read estimator keeps no per-peer state and owes no refit"
            );
        }
    }

    /// Prints the per-event cost of `add_event` on a saturated window for the
    /// two shapes that matter to the rebuild: all-distinct distances, and the
    /// repeated distances real traffic produces (one peer, one contract, one
    /// distance). Not an assertion — timings in a debug test binary say little
    /// about a release node — but it keeps the comparison one command away:
    /// `cargo test -p freenet --lib --release sorted_window_rebuild_cost -- --nocapture`.
    ///
    /// The deterministic half of the property it reports on, that the window
    /// stays in the order that makes the rebuild's sort linear, is pinned by
    /// `sorted_window_keeps_library_order_and_normalises_signed_zero`.
    ///
    /// Each shape is timed under both [`FitPolicy`] values on the same events,
    /// in the same process, so machine load affects both alike: the difference
    /// is what the router's per-operation dashboard estimators stopped paying
    /// per event when they moved to fitting on read (#5662).
    #[test]
    fn sorted_window_rebuild_cost_by_shape() {
        let peers: Vec<PeerKeyLocation> = (0..32).map(|_| PeerKeyLocation::random()).collect();
        let measured = 5 * MAX_REGRESSION_POINTS;
        for (label, distinct_distances) in [("distinct distances", 0usize), ("20 distances", 20)] {
            let event = |i: usize| {
                let x = if distinct_distances == 0 {
                    ((i * 7919) % 100_000) as f64 / 200_000.0
                } else {
                    (i % distinct_distances) as f64 / (2 * distinct_distances) as f64
                };
                let y = if (i * 31) % 10 < 2 { 1.0 } else { 0.0 };
                event_at_distance(&peers[i % peers.len()], x, y)
            };
            type Build = fn() -> IsotonicEstimator;
            let policies: [(&str, Build); 2] = [
                ("fit every event, refit amortised in", || {
                    IsotonicEstimator::new(std::iter::empty(), EstimatorType::Positive)
                }),
                ("fit on read", || {
                    IsotonicEstimator::new_fit_on_read(
                        std::iter::empty(),
                        EstimatorType::Positive,
                        AdjustmentMode::Additive,
                    )
                }),
            ];
            for (policy, build) in policies {
                let mut estimator = build();
                for i in 0..MAX_REGRESSION_POINTS {
                    estimator.add_event(event(i));
                }
                let events: Vec<IsotonicEvent> = (MAX_REGRESSION_POINTS
                    ..MAX_REGRESSION_POINTS + measured)
                    .map(event)
                    .collect();
                let start = std::time::Instant::now();
                for event in events {
                    estimator.add_event(event);
                }
                let per_event = start.elapsed().as_secs_f64() * 1e6 / measured as f64;
                eprintln!(
                    "isotonic add_event, saturated {MAX_REGRESSION_POINTS}-point window, \
                     {label}, {policy}: {per_event:.1}us/event"
                );
                assert_eq!(
                    estimator.sorted_points.as_slice().len(),
                    MAX_REGRESSION_POINTS
                );
            }
        }
    }

    /// Both places the regression is built — the constructor and the per-event
    /// rebuild — must use the estimator's direction: fitting a Negative
    /// (transfer-rate) estimator as ascending would silently invert every
    /// estimate.
    ///
    /// NOTE ON THE ASSERTION. The obvious check — `near >= far` — is VACUOUS
    /// and was proven so in review: PAV over data that violates its assumed
    /// direction at every pair pools everything into ONE aggregate, i.e. a flat
    /// line, and a flat line satisfies `near >= far` by equality. Fitting these
    /// points ascending yields exactly that, so the weak assertion passes on
    /// the very bug it is meant to catch. So assert a STRICT decrease AND that
    /// the fit did not collapse to a single block. Both fail if the direction
    /// is wrong.
    ///
    /// Until #5658 this was `refit_respects_descending_estimator_direction`;
    /// the refit no longer builds the curve.
    #[test]
    fn fit_respects_descending_estimator_direction() {
        let peer = PeerKeyLocation::random();
        let events: Vec<IsotonicEvent> = (0..120)
            .map(|i| {
                let x = (i % 40) as f64 / 100.0; // [0.0, 0.39]
                event_at_distance(&peer, x, 1.0 - x) // strictly decreasing in distance
            })
            .collect();

        let constructed = IsotonicEstimator::new(events.clone(), EstimatorType::Negative);
        let mut incremental = IsotonicEstimator::new(std::iter::empty(), EstimatorType::Negative);
        for event in events {
            incremental.add_event_incremental(event);
        }

        for (label, estimator) in [("constructor", &constructed), ("per-event", &incremental)] {
            let near = estimator
                .global_regression
                .interpolate(0.05)
                .expect("fit must estimate within its data range");
            let far = estimator
                .global_regression
                .interpolate(0.35)
                .expect("fit must estimate within its data range");
            assert!(
                near > far + 1e-6,
                "{label}: a descending fit must STRICTLY decrease with distance \
                 (near={near}, far={far}); equality means PAV pooled everything \
                 into one flat aggregate, which is what fitting the wrong direction does"
            );
            assert!(
                estimator.global_regression.get_points().len() > 1,
                "{label}: the fit collapsed to a single aggregate — the hallmark of \
                 PAV run against its data's actual direction"
            );
        }
    }

    fn simulate_positive_request(
        peer: PeerKeyLocation,
        contract_location: Location,
    ) -> IsotonicEvent {
        simulate_request(peer, contract_location, |d| d.powf(0.5))
    }

    fn simulate_negative_request(
        peer: PeerKeyLocation,
        contract_location: Location,
    ) -> IsotonicEvent {
        simulate_request(peer, contract_location, |d| (100.0 - d).powf(0.5))
    }

    #[test]
    fn test_sampled_curve_empty_estimator() {
        let estimator = IsotonicEstimator::new(std::iter::empty(), EstimatorType::Positive);
        let curve = estimator.sampled_curve(0.0, 1.0, 50);
        assert!(
            curve.is_empty(),
            "Empty estimator should produce empty curve"
        );
        assert_eq!(estimator.data_x_range(), (0.0, 0.0));
    }

    #[test]
    fn test_sampled_curve_clamping() {
        // Create an ascending estimator where extrapolation could exceed [0, 1]
        let peer = PeerKeyLocation::random();
        let events: Vec<IsotonicEvent> = (0..50)
            .map(|i| {
                let x = i as f64 / 100.0; // distances 0.0 to 0.49
                IsotonicEvent {
                    peer: peer.clone(),
                    contract_location: Location::new(x),
                    result: x * 3.0, // values 0.0 to 1.47 -- will exceed 1.0 clamp
                }
            })
            .collect();
        let estimator = IsotonicEstimator::new(events, EstimatorType::Positive);
        let curve = estimator.sampled_curve(0.0, 1.0, 50);

        assert!(!curve.is_empty());
        for &(_, y) in &curve {
            assert!(y >= 0.0, "y should be >= 0, got {y}");
            assert!(y <= 1.0, "y should be <= 1.0 (clamped), got {y}");
        }
    }

    #[test]
    fn test_sampled_curve_covers_full_range() {
        let peer = PeerKeyLocation::random();
        let events: Vec<IsotonicEvent> = (0..20)
            .map(|i| IsotonicEvent {
                peer: peer.clone(),
                contract_location: Location::new(0.1 + i as f64 * 0.01),
                result: i as f64,
            })
            .collect();
        let estimator = IsotonicEstimator::new(events, EstimatorType::Positive);
        let curve = estimator.sampled_curve(0.0, f64::INFINITY, 50);

        assert_eq!(curve.len(), 50);
        // First point should be at x=0.0, last at x=0.5
        assert!((curve[0].0 - 0.0).abs() < 1e-10);
        assert!((curve[49].0 - 0.5).abs() < 1e-10);
    }

    #[test]
    fn test_data_x_range_reflects_actual_data() {
        let peer = PeerKeyLocation::random();
        let events: Vec<IsotonicEvent> = vec![
            IsotonicEvent {
                peer: peer.clone(),
                contract_location: Location::new(0.1),
                result: 1.0,
            },
            IsotonicEvent {
                peer: peer.clone(),
                contract_location: Location::new(0.3),
                result: 2.0,
            },
        ];
        let estimator = IsotonicEstimator::new(events, EstimatorType::Positive);
        let (lo, hi) = estimator.data_x_range();

        // Data range should approximately match the distances we fed in
        // (exact values depend on PeerKeyLocation's random location)
        assert!((0.0..=0.5).contains(&lo));
        assert!(hi >= lo);
        assert!(hi <= 0.5);
    }

    #[test]
    fn test_sampled_curve_guard_against_low_samples() {
        let estimator = IsotonicEstimator::new(std::iter::empty(), EstimatorType::Positive);
        // num_samples < 2 should return empty without panicking
        let curve = estimator.sampled_curve(0.0, 1.0, 0);
        assert!(curve.is_empty());
        let curve = estimator.sampled_curve(0.0, 1.0, 1);
        assert!(curve.is_empty());
    }
}
