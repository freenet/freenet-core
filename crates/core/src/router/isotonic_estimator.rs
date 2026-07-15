use crate::ring::{Distance, Location, PeerKeyLocation};
use pav_regression::IsotonicRegression;
use pav_regression::Point;
use serde::Serialize;
use std::collections::{HashMap, VecDeque};

const MIN_POINTS_FOR_REGRESSION: usize = 5;

/// Maximum number of raw data points retained by the global regression.
/// Once reached, each new point evicts the oldest via `remove_points`.
const MAX_REGRESSION_POINTS: usize = 500;

/// Percentage of the current window that must turn over before
/// [`IsotonicEstimator::refit_if_stale`] rebuilds the fit.
///
/// Compared as `events_since_refit * 100 > len * REFIT_STALENESS_PERCENT` — a
/// multiplication rather than a division, so the threshold is exact with no
/// truncation. Over a saturated window (`MAX_REGRESSION_POINTS`) this earns a
/// refit every 51st event: frequent enough that incremental PAV drift stays
/// small, rare enough that the O(n log n) refit amortises to a handful of
/// operations per event.
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

/// `IsotonicEstimator` provides outcome estimation for a given action, such as
/// retrieving the state of a contract, based on the distance between the peer
/// and the contract. It uses an isotonic regression model from the `pav.rs`
/// library to estimate the outcome based on the distance between the peer and
/// the contract, but then also tracks an adjustment for each peer based on the
/// outcome of the peer's previous requests.
///
/// The global regression uses a rolling window: once `MAX_REGRESSION_POINTS`
/// raw points have been accumulated, each new point evicts the oldest via
/// `remove_points`. Per-peer adjustments use an exponentially-weighted moving
/// average (EWMA) so recent events have more influence than old ones.

#[derive(Debug, Clone, Serialize)]
pub(crate) struct IsotonicEstimator {
    pub global_regression: IsotonicRegression<f64>,
    pub peer_adjustments: HashMap<PeerKeyLocation, Adjustment>,
    /// How per-peer adjustments combine with the global estimate. See
    /// [`AdjustmentMode`].
    #[serde(skip)]
    adjustment_mode: AdjustmentMode,
    /// Raw input events in insertion order. When len exceeds
    /// `MAX_REGRESSION_POINTS`, the oldest is evicted via `remove_points`.
    ///
    /// Retains the whole [`IsotonicEvent`], not just its `(distance, result)`
    /// point, because [`Self::refit`] must rebuild `peer_adjustments` too — and
    /// that needs peer identity. Keeping only points would make a refit a
    /// PARTIAL rebuild: the global curve would move while every peer's EWMA
    /// stayed anchored to the old curve, and evicted peers would never be
    /// pruned from `peer_adjustments`.
    #[serde(skip)]
    raw_events: VecDeque<IsotonicEvent>,
    /// Monotonic direction of the fit, retained so [`Self::refit`] can rebuild
    /// the same way [`Self::new_with_mode`] built it.
    #[serde(skip)]
    estimator_type: EstimatorType,
    /// Events added via [`Self::add_event`] since the last refit. Drives the
    /// staleness trigger; see [`Self::refit_if_stale`].
    #[serde(skip)]
    events_since_refit: usize,
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
    fn residual(self, observed: f64, global: f64) -> Option<f64> {
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
        let mut all_events: Vec<IsotonicEvent> = history.into_iter().collect();

        // If history exceeds the window, keep only the most recent events.
        // Both the regression points and the peer adjustment deltas are computed
        // from the same windowed subset to avoid stale-data bias.
        if all_events.len() > MAX_REGRESSION_POINTS {
            all_events.drain(..all_events.len() - MAX_REGRESSION_POINTS);
        }

        let raw_events: VecDeque<IsotonicEvent> = all_events.into();
        let (global_regression, peer_adjustments) =
            Self::fit(&raw_events, estimator_type, adjustment_mode)
                .expect("Failed to create isotonic regression");

        IsotonicEstimator {
            global_regression,
            peer_adjustments,
            adjustment_mode,
            raw_events,
            estimator_type,
            events_since_refit: 0,
        }
    }

    /// Fit `events` from scratch: the global isotonic regression, plus every
    /// peer's adjustment re-anchored to that fit.
    ///
    /// Shared by [`Self::new_with_mode`] and [`Self::refit`] so that a refit is a
    /// TRUE batch rebuild rather than a partial one. Both halves must be rebuilt
    /// together: a peer's `Adjustment` is an EWMA of residuals measured AGAINST
    /// the global curve (see [`AdjustmentMode::residual`]), so refitting the
    /// curve without re-deriving the residuals would leave every peer corrected
    /// relative to a curve that no longer exists. Rebuilding from `events` also
    /// bounds `peer_adjustments` to peers present in the current window —
    /// without that, the map would grow one entry per peer ever seen (an
    /// unbounded per-key collection driven by remote peers) and a returning peer
    /// would have its stale adjustment applied at full weight, since
    /// `Adjustment::effective_count` has no time decay.
    fn fit(
        events: &VecDeque<IsotonicEvent>,
        estimator_type: EstimatorType,
        adjustment_mode: AdjustmentMode,
    ) -> Result<
        (
            IsotonicRegression<f64>,
            HashMap<PeerKeyLocation, Adjustment>,
        ),
        pav_regression::isotonic_regression::IsotonicRegressionError,
    > {
        let points: Vec<Point<f64>> = events
            .iter()
            .map(|event| Point::new(event.route_distance().as_f64(), event.result))
            .collect();

        let global_regression = match estimator_type {
            EstimatorType::Positive => IsotonicRegression::new_ascending(&points),
            EstimatorType::Negative => IsotonicRegression::new_descending(&points),
        }?;

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

        Ok((global_regression, peer_adjustments))
    }

    /// Refit `global_regression` from scratch over `raw_points`, but only once
    /// more than `REFIT_STALENESS_NUMERATOR/REFIT_STALENESS_DENOMINATOR` of the
    /// window has turned over since the last refit. Returns whether a refit ran.
    ///
    /// WHY THIS EXISTS: `add_event` maintains the fit incrementally
    /// (`add_points` / `remove_points`). Incremental pool-adjacent-violators
    /// maintenance is an approximation — the pooled blocks it leaves depend on
    /// insertion order, so the fit drifts from the one a batch PAV pass over the
    /// same points would produce. Refitting restores the exact fit.
    ///
    /// It is deliberately driven by DATA TURNOVER rather than a timer: a refit
    /// on an idle router is pure waste (nothing changed), while a busy router
    /// earns one quickly. This also replaces the previous mechanism, which
    /// rebuilt the whole `Router` from the on-disk event log every 5 minutes —
    /// that log never receives relay-recorded events
    /// (`operations::record_relay_route_event` feeds the in-memory router only),
    /// so the rebuild silently discarded them and reset the model faster than it
    /// could learn. `raw_points` is the complete corpus by construction: every
    /// `add_event` lands here regardless of whether it came from an originator
    /// or a relay hop. See issue #4808.
    ///
    /// Rebuilds BOTH halves of the estimator — see [`Self::fit`] for why the
    /// global curve and the per-peer adjustments cannot be refit independently.
    ///
    /// Called from [`Self::add_event`] only. It is deliberately NOT public: a
    /// caller polling it could never observe a stale estimator, because
    /// `add_event` clears staleness before it returns (see there for why the
    /// check belongs on the write path).
    fn refit_if_stale(&mut self) -> bool {
        if !self.refit_due() {
            return false;
        }
        self.refit()
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

    /// Unconditionally rebuild the estimator from `raw_events`. Returns whether
    /// the rebuild succeeded.
    ///
    /// Equivalent to constructing a fresh estimator over the current window —
    /// which is exactly what the pre-#4808 periodic `Router::new(&history)`
    /// rebuild achieved, except that it read an on-disk log missing every
    /// relay-recorded event, whereas `raw_events` is complete by construction.
    fn refit(&mut self) -> bool {
        match Self::fit(&self.raw_events, self.estimator_type, self.adjustment_mode) {
            Ok((global_regression, peer_adjustments)) => {
                self.global_regression = global_regression;
                self.peer_adjustments = peer_adjustments;
                self.events_since_refit = 0;
                true
            }
            Err(error) => {
                // Unreachable today: `new_ascending`/`new_descending` pass
                // `intersect_origin: false`, and the sole error variant
                // (`NegativePointWithIntersectOrigin`) is only produced when that
                // flag is true. Empty input succeeds. Handled rather than
                // `.expect()`-ed so that a future pav_regression change cannot
                // turn a fit failure into a dead routing path — a slightly
                // drifted regression beats a panicking node.
                //
                // `events_since_refit` is deliberately NOT reset, so the next
                // poll retries rather than waiting for another full turnover.
                tracing::warn!(
                    %error,
                    events = self.raw_events.len(),
                    "Isotonic refit failed; keeping previous fit"
                );
                false
            }
        }
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
    /// COST. The refit is amortised over the events that earn it: one O(n log n)
    /// batch fit per ~51 events on a saturated window, on a path that already
    /// pays O(k log k) twice per call (`add_points` and `remove_points` each
    /// clone, sort, and re-run PAV). Measured on this estimator at a saturated
    /// 500-point window (release build, 20-50 distinct peers — a realistic
    /// neighbour set): one refit costs ~150µs, `add_event` costs ~39µs without it,
    /// and the amortised overhead is ~1.5-5.7µs/event, i.e. **+4-14% on a call
    /// that was already the expensive part**. Note #4808's oft-quoted "~27µs per
    /// saturated refit" does not reproduce — it is ~5x optimistic — but the
    /// conclusion it supported survives the correction, because what lands on the
    /// hot path is the amortised few µs, not the whole fit.
    ///
    /// It takes no locks and does no I/O — it is pure computation over `self` —
    /// so it cannot deadlock or re-enter a caller that already holds one
    /// (`Router::add_event` is called under `ring.router.write()`;
    /// `ConnectForwardEstimator::record` under its own `RwLock`). It only extends
    /// a critical section the caller already holds.
    pub fn add_event(&mut self, event: IsotonicEvent) {
        self.add_event_incremental(event);

        // Restore the exact batch fit once this event has turned over enough of
        // the window. Runs last so the incremental state above is complete and
        // correct on the ~50/51 events that do not earn a refit; on the one that
        // does, `refit` rebuilds both halves from `raw_events` and supersedes it.
        self.refit_if_stale();
    }

    /// The incremental half of [`Self::add_event`]: extend the window by one
    /// event and patch the existing fit in place, WITHOUT considering a refit.
    ///
    /// Split out because this is the half that DRIFTS. Keeping it callable on its
    /// own is what lets `incremental_fit_drifts_from_batch_and_refit_repairs_it`
    /// and `refit_prunes_peers_that_fell_out_of_the_window` build a genuinely
    /// refit-free fit to measure against a batch build — the tripwire that
    /// justifies the refit existing at all, and the proof that pruning is needed.
    /// Through `add_event` they no longer can: it repairs the drift as it goes,
    /// which is the entire point of #4811.
    fn add_event_incremental(&mut self, event: IsotonicEvent) {
        let route_distance = event.route_distance();
        let point = Point::new(route_distance.as_f64(), event.result);

        // Add the new point to the regression and the raw-event FIFO. This keeps
        // the fit usable immediately; `refit_if_stale` later restores the exact
        // batch fit once enough of the window has turned over.
        self.global_regression.add_points(&[point]);
        self.raw_events.push_back(event.clone());
        self.events_since_refit += 1;

        // Evict the oldest event if the window is full.
        if self.raw_events.len() > MAX_REGRESSION_POINTS {
            if let Some(oldest) = self.raw_events.pop_front() {
                let oldest_point = Point::new(oldest.route_distance().as_f64(), oldest.result);
                self.global_regression.remove_points(&[oldest_point]);
            }
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
    }

    pub fn estimate_retrieval_time(
        &self,
        peer: &PeerKeyLocation,
        contract_location: Location,
    ) -> Result<f64, EstimationError> {
        if self.global_regression.len() < MIN_POINTS_FOR_REGRESSION {
            return Err(EstimationError::InsufficientData);
        }

        let peer_location = peer.location().ok_or(EstimationError::InsufficientData)?;
        let distance: f64 = contract_location.distance(peer_location).as_f64();

        let global_estimate = self
            .global_regression
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

    pub(crate) fn len(&self) -> usize {
        self.global_regression.len()
    }

    /// The per-peer adjustment mode this estimator was constructed with.
    /// Test-only: used to pin each estimator's mode (see the router test
    /// `estimators_use_intended_adjustment_modes`). When the dashboard is wired to
    /// read the mode (the #4547 follow-up), drop the `cfg(test)` to make it real API.
    #[cfg(test)]
    pub(crate) fn adjustment_mode(&self) -> AdjustmentMode {
        self.adjustment_mode
    }

    /// Return the x-range of actual regression data points, or (0, 0) if empty.
    pub(crate) fn data_x_range(&self) -> (f64, f64) {
        let sorted = self.global_regression.get_points_sorted();
        if sorted.is_empty() {
            return (0.0, 0.0);
        }
        (*sorted.first().unwrap().x(), *sorted.last().unwrap().x())
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
        if num_samples < 2 || self.global_regression.get_points_sorted().is_empty() {
            return Vec::new();
        }

        let mut points = Vec::with_capacity(num_samples);
        for i in 0..num_samples {
            let x = (i as f64 / (num_samples - 1) as f64) * 0.5;
            if let Some(y) = self.global_regression.interpolate(x) {
                points.push((x, y.clamp(y_clamp_min, y_clamp_max)));
            }
        }

        points
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
        assert!(incremental.refit(), "refit must succeed");

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

        assert!(estimator.refit(), "refit must succeed");
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

    #[test]
    fn incremental_fit_drifts_from_batch_and_refit_repairs_it() {
        // This test justifies the refit's existence, and is a tripwire: if
        // `pav_regression` ever makes the incremental path exact, the first
        // assertion fails and `refit_if_stale` can be deleted.
        //
        // Why drift happens (pav_regression 0.7.0):
        //   - `add_points` re-runs `isotonic()` over `self.points`, which are the
        //     already-POOLED blocks, not the raw inputs. Pooling is lossy, so the
        //     result depends on insertion order.
        //   - `remove_points` is explicitly approximate: it subtracts the evicted
        //     point's influence from the CLOSEST aggregate by x, which need not be
        //     the aggregate that point actually contributed to.
        // Eviction is the worse of the two, so drive the window past
        // MAX_REGRESSION_POINTS to exercise it — the steady state of a busy peer.
        //
        // The regression's x-axis is controlled exactly via `event_at_distance`
        // (see its docs): the contract is placed at ring offset x FROM the peer,
        // so `route_distance() == x` whatever the random peer location is. Without
        // that, `Location::distance`'s fold turns the intended geometry into a
        // peer-dependent tent transform and the data below is not what it reads as.
        let peer = PeerKeyLocation::random();
        let make = |x: f64, y: f64| event_at_distance(&peer, x, y);

        // Deliberately non-monotonic in y so PAV must pool, with enough points to
        // force eviction of the oldest.
        let events: Vec<IsotonicEvent> = (0..(MAX_REGRESSION_POINTS + 200))
            .map(|i| {
                let x = (i % 50) as f64 / 100.0; // spread over [0.0, 0.49]
                let y = if i % 3 == 0 { 1.0 - x } else { x }; // violates monotonicity
                make(x, y)
            })
            .collect();

        // Fed through `add_event_incremental`, NOT `add_event`: since #4811 the
        // latter refits inline, so it cannot produce the un-repaired fit this test
        // needs to measure drift against. That is the fix working, not a reason to
        // weaken the tripwire — the incremental path is still what runs between
        // refits, and it is still what drifts.
        let mut incremental = IsotonicEstimator::new(std::iter::empty(), EstimatorType::Positive);
        for event in events.iter().cloned() {
            incremental.add_event_incremental(event);
        }

        // The batch reference: exactly what the estimator's own constructor builds
        // from the same windowed corpus.
        let batch = IsotonicEstimator::new(events, EstimatorType::Positive);

        let sample = |est: &IsotonicEstimator| -> Vec<f64> {
            (0..=49)
                .map(|s| {
                    est.global_regression
                        .interpolate(s as f64 / 100.0)
                        .unwrap_or(f64::NAN)
                })
                .collect()
        };

        let drift: f64 = sample(&incremental)
            .iter()
            .zip(sample(&batch).iter())
            .map(|(a, b)| (a - b).abs())
            .sum();
        assert!(
            drift > 1e-9,
            "expected the incremental fit to drift from batch (drift={drift}); if this \
             now holds exactly, pav_regression became exact and refit_if_stale is dead code"
        );

        incremental.refit();
        let repaired: f64 = sample(&incremental)
            .iter()
            .zip(sample(&batch).iter())
            .map(|(a, b)| (a - b).abs())
            .sum();
        assert!(
            repaired < 1e-9,
            "refit must restore the exact batch fit (residual={repaired}, was {drift})"
        );
    }

    #[test]
    fn refit_respects_descending_estimator_direction() {
        // A refit must rebuild with the SAME monotonic direction it was created
        // with: rebuilding a Negative (transfer-rate) estimator as ascending would
        // silently invert every estimate.
        //
        // NOTE ON THE ASSERTION. The obvious check — `near >= far` — is VACUOUS
        // and was proven so in review: PAV over data that violates its assumed
        // direction at every pair pools everything into ONE aggregate, i.e. a flat
        // line, and a flat line satisfies `near >= far` by equality. Fitting these
        // points ascending yields exactly that, so the weak assertion passes on
        // the very bug it is meant to catch.
        //
        // So assert a STRICT decrease AND that the fit did not collapse to a
        // single block. Both fail if the direction is wrong.
        let peer = PeerKeyLocation::random();
        let events: Vec<IsotonicEvent> = (0..120)
            .map(|i| {
                let x = (i % 40) as f64 / 100.0; // [0.0, 0.39]
                event_at_distance(&peer, x, 1.0 - x) // strictly decreasing in distance
            })
            .collect();
        let mut estimator = IsotonicEstimator::new(events, EstimatorType::Negative);
        assert!(estimator.refit(), "refit must succeed");

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
            "a descending fit must STRICTLY decrease with distance after refit \
             (near={near}, far={far}); equality means PAV pooled everything into \
             one flat aggregate, which is what fitting the wrong direction does"
        );
        assert!(
            estimator.global_regression.get_points().len() > 1,
            "the fit collapsed to a single aggregate — the hallmark of PAV run \
             against its data's actual direction"
        );
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
