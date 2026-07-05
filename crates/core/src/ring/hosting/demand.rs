//! Proximity-prior demand estimator for demand-driven hosting eviction (A3).
//!
//! Piece A3 of the demand-driven hosting redesign (freenet/freenet-core#4642).
//! The demand-ordered (Greedy-Dual) eviction policy in [`super::cache`] orders contracts by
//! `keep_score = eviction_floor + predicted_demand(X)`. This module supplies
//! `predicted_demand(X)`: a per-contract estimate of X's read-request rate at
//! this peer.
//!
//! The estimate blends two terms, **outside** the regression:
//!
//! ```text
//! predicted_demand(d) = w(n) * g0(d) + (1 - w(n)) * fit(d)
//! ```
//!
//! - `g0(d)` — an **analytic cold-start prior**: a smooth, strictly-decreasing
//!   function of the ring distance `d` between this peer and the contract's key
//!   ([`distance_prior`]). Contracts near this peer's location are, on average,
//!   requested more often (routing gravity), so demand falls off with distance.
//!   This makes near-key contracts score higher **from the very first read**,
//!   before any samples exist — the cold-start behavior that fixes the
//!   under-retention of near-key contracts.
//! - `fit(d)` — the **learned aggregate estimate**: a descending isotonic
//!   (PAVA) regression over `(distance, observed_read_rate)` samples this peer
//!   collects from its own reads, reusing the same machinery the router uses for
//!   its distance -> outcome curves ([`crate::router`] / `pav_regression`). A
//!   rolling window bounds the retained points so the curve tracks the peer's
//!   recent demand geometry rather than its whole history.
//! - `w(n)` — a blend weight that **decays in the retained sample count `n`**
//!   ([`blend_weight`]): `1.0` at `n = 0` (pure analytic prior) and `-> 0` as
//!   `n` grows (the learned fit takes over), with a smooth hand-off and no
//!   threshold discontinuity. Note the *value* hand-off is governed by the
//!   magnitude ratio of the two terms, not `n` alone: `g0` is anchored at
//!   `NEUTRAL_DEMAND = 1.0` while `fit` is in reads/sec, which for a low-traffic
//!   peer is often `<< 1`. When the observed rate is that small, the prior's
//!   distance slope can persist in the eviction *ordering* even at large `n`
//!   (the neutral-scale prior term keeps dominating the tiny fit term). The
//!   ordering DIRECTION stays sane throughout — both `g0` and `fit` are
//!   non-increasing in distance, so their blend is too — and the per-contract
//!   own-rate blend that fully retires the aggregate distance slope is A4.
//!
//! The blend is deliberately computed **outside** the PAVA fit — the prior is
//! NOT seeded as synthetic points into the regression. Seeding would make the
//! prior's absolute scale load-bearing (it would have to be calibrated against
//! real rates) and the FIFO rolling window would evict the synthetic seeds in a
//! step, reintroducing exactly the cold-start cliff this design removes. Keeping
//! it a separate blended term means only the *ratio* g0(near)/g0(far) matters,
//! never its absolute scale.
//!
//! Only RATIOS matter to eviction ordering (`keep_score` is compared, never
//! interpreted as an absolute rate), so no absolute calibration is required.
//!
//! The per-contract blend toward each contract's OWN observed read rate (as
//! opposed to this aggregate distance -> rate relationship) remains deferred to
//! A4.

use pav_regression::{IsotonicRegression, Point};
use std::collections::VecDeque;

/// Maximum raw `(distance, rate)` points retained. Once reached, each new
/// observation evicts the oldest (FIFO rolling window), mirroring the router's
/// `MAX_REGRESSION_POINTS`. Keeps the fit tracking recent demand and bounds
/// memory + refit cost.
const MAX_PRIOR_POINTS: usize = 500;

/// Demand returned only when no ring distance is available (this peer's own
/// location is unknown) or the requested distance is non-finite. Positive and
/// uniform: with equal demand for every contract, eviction reduces to
/// Greedy-Dual floor-ordering with a last-read tiebreak. Must be `> 0` so that
/// an abandoned contract (whose `keep_score` is dropped to the current
/// `eviction_floor`, i.e. zero demand credit) sorts below a still-credited one.
/// Also the anchor for the cold-start prior at zero distance
/// (`g0(0) == NEUTRAL_DEMAND`), so a near-key contract keeps the old neutral
/// credit and only farther contracts are discounted.
pub(crate) const NEUTRAL_DEMAND: f64 = 1.0;

/// Falloff rate of the analytic cold-start prior
/// `g0(d) = NEUTRAL_DEMAND * exp(-DISTANCE_PRIOR_DECAY * d)` over ring distance
/// `d in [0, 0.5]`. Only the RATIO `g0(near)/g0(far)` feeds eviction ordering,
/// so the exact rate is a soft choice: `4.0` gives a ~7x near-to-far demand
/// ratio across the half-ring (`g0(0) = 1.0` down to `g0(0.5) = e^-2 ≈ 0.135`),
/// a mild monotone gravity toward the key that a handful of real samples
/// quickly overrides.
const DISTANCE_PRIOR_DECAY: f64 = 4.0;

/// Pseudo-observation weight of the analytic prior. With `n` retained real
/// samples the prior's blend weight is `PRIOR_PSEUDO_COUNT / (PRIOR_PSEUDO_COUNT
/// plus n)` (see [`blend_weight`]): the analytic prior counts as this many real
/// observations. At `n = PRIOR_PSEUDO_COUNT` prior and fit weigh equally; past
/// that the learned fit dominates. Set to the sample count the old hard gate
/// required before it trusted the fit at all, so "enough data to trust the fit"
/// and "prior no longer dominates" coincide.
const PRIOR_PSEUDO_COUNT: f64 = 5.0;

/// Analytic cold-start demand prior: expected read demand as a smooth,
/// strictly-decreasing function of ring distance to the contract key. Closer to
/// the key -> more routing gravity -> higher expected read demand. Anchored so
/// `g0(0) == NEUTRAL_DEMAND`; distance is clamped to the valid `[0, 0.5]`
/// ring-distance range defensively. Always strictly positive.
fn distance_prior(distance: f64) -> f64 {
    let d = distance.clamp(0.0, 0.5);
    NEUTRAL_DEMAND * (-DISTANCE_PRIOR_DECAY * d).exp()
}

/// Blend weight on the analytic prior given `n` retained real samples:
/// `PRIOR_PSEUDO_COUNT / (PRIOR_PSEUDO_COUNT + n)`. `1.0` at `n = 0`, strictly
/// decreasing in `n`, and `-> 0` as `n` grows, so the estimate slides smoothly
/// from the pure distance prior (cold) to the pure learned fit (warm).
fn blend_weight(n: usize) -> f64 {
    PRIOR_PSEUDO_COUNT / (PRIOR_PSEUDO_COUNT + n as f64)
}

/// Per-peer proximity-prior demand estimator.
///
/// Wraps a descending isotonic regression over `(distance, read_rate)` samples.
/// `predict(distance)` gives the expected read rate at that ring distance, used
/// as `predicted_demand` in the demand-ordered `keep_score`.
pub(crate) struct ProximityPrior {
    /// Descending isotonic fit: expected read rate as a non-increasing function
    /// of ring distance to the contract key.
    regression: IsotonicRegression<f64>,
    /// Raw input points in insertion order. When the length exceeds
    /// [`MAX_PRIOR_POINTS`], the oldest is evicted via `remove_points` so the
    /// fit tracks a bounded recent window.
    raw_points: VecDeque<Point<f64>>,
}

impl ProximityPrior {
    /// Create an empty estimator. With no observations,
    /// [`predict`](Self::predict) returns the pure analytic distance prior
    /// [`distance_prior`] (monotone-decreasing in distance), and slides toward
    /// the learned fit as observations accrue.
    pub(crate) fn new() -> Self {
        // `new_descending` on an empty slice yields an empty regression; the
        // router relies on the same empty-construction path.
        let empty: [Point<f64>; 0] = [];
        let regression = IsotonicRegression::new_descending(&empty)
            .expect("empty descending isotonic regression is always constructible");
        Self {
            regression,
            raw_points: VecDeque::new(),
        }
    }

    /// Record one observed `(distance, read_rate)` sample. `distance` is the
    /// ring distance in `[0, 0.5]`; `read_rate` is a non-negative reads/second
    /// estimate for a contract at that distance. Non-finite or negative inputs
    /// are ignored (they would corrupt the fit).
    pub(crate) fn observe(&mut self, distance: f64, read_rate: f64) {
        if !distance.is_finite() || !read_rate.is_finite() || distance < 0.0 || read_rate < 0.0 {
            return;
        }
        let point = Point::new(distance, read_rate);
        self.regression.add_points(&[point]);
        self.raw_points.push_back(point);
        if self.raw_points.len() > MAX_PRIOR_POINTS {
            if let Some(oldest) = self.raw_points.pop_front() {
                // `remove_points` correctly drops the sample even after PAVA has
                // pooled it into a merged block: pav_regression's `remove_points`
                // subtracts the removed point's weighted influence from the
                // nearest aggregate AND decrements the centroid exactly (see its
                // rustdoc), so the oldest observation's contribution is removed,
                // not retained. This is the same incremental rolling-window
                // mechanism the router's `IsotonicEstimator` relies on in
                // production; a full rebuild from `raw_points` here would be
                // O(n log n) per observation for no correctness gain.
                self.regression.remove_points(&[oldest]);
            }
        }
    }

    /// Predict the read-demand for a contract at ring `distance` from this peer.
    ///
    /// Blends the analytic cold-start prior [`distance_prior`] with the learned
    /// isotonic fit by a sample-count-decayed [`blend_weight`]:
    /// `w(n) * g0(d) + (1 - w(n)) * fit(d)`. Cold (`n = 0`) this is the pure
    /// distance prior; warm it is the pure fit. When there is no usable fit yet
    /// (no retained points, or the regression cannot interpolate) it is the pure
    /// prior. `NEUTRAL_DEMAND` is returned only for a non-finite `distance` (the
    /// guard that keeps a NaN out of the eviction sort). Always finite and
    /// strictly positive: `g0 > 0` and `fit >= 0`, and `w(n) > 0`, so the blend
    /// `w*g0 + (1-w)*fit >= w*g0 > 0`.
    ///
    /// # Accepted tradeoff (A3 intermediate)
    ///
    /// At cold start (and for a low-traffic peer whose observed rates stay
    /// `<< NEUTRAL_DEMAND`; see the module docs) eviction ordering is effectively
    /// **distance-only** — demand-blind, because the per-contract observed-read-
    /// rate signal (A4) is deferred. Concretely: a FAR contract that is actually
    /// GET-hot can be out-scored, and once past `min_ttl` evicted, in favor of an
    /// unread NEAR contract that merely sits closer to this peer's key. This is
    /// the spec-accepted intermediate state, not a bug:
    ///
    /// - It is **bounded by `min_ttl`** — the anti-thrash retention floor kept
    ///   deliberately per the hosting design (`hosting-invariants.md`, invariant
    ///   3 / #4441): nothing evicts a contract within `min_ttl` of its last read,
    ///   so a genuinely hot far contract keeps being refreshed and survives.
    /// - It is **fully resolved when A4 lands** (per-contract own-observed-rate
    ///   blend) together with in-flight op-pinning, at which point real read
    ///   demand overrides the distance prior for any contract with evidence.
    ///
    /// The design contract for this piece is exactly "distance prior + `min_ttl`
    /// until A4" (`.claude/rules/hosting-invariants.md`, piece A / #4642). Do NOT
    /// try to close the gap here by reaching for a per-contract counter — that is
    /// A4's job and belongs with its telemetry and op-pinning.
    pub(crate) fn predict(&self, distance: f64) -> f64 {
        if !distance.is_finite() {
            return NEUTRAL_DEMAND;
        }
        let g0 = distance_prior(distance);
        // Blend OUTSIDE the regression: never seed the analytic prior as
        // synthetic points into the PAVA fit (that would make its absolute scale
        // load-bearing and the FIFO window would evict the seeds in a step). The
        // prior and the fit are combined by a weight that decays in the retained
        // sample count, so the estimate slides smoothly from prior to fit.
        let w = blend_weight(self.raw_points.len());
        match self.fit(distance) {
            Some(fit) => w * g0 + (1.0 - w) * fit,
            None => g0,
        }
    }

    /// The learned aggregate estimate at `distance`: the descending isotonic fit
    /// over retained `(distance, rate)` samples, floored at `0.0` (the fit can
    /// extrapolate slightly negative near the data-range edges). `None` when
    /// there is no usable fit yet — no retained points, or the regression cannot
    /// interpolate — in which case [`predict`](Self::predict) uses the pure
    /// analytic prior.
    fn fit(&self, distance: f64) -> Option<f64> {
        if self.raw_points.is_empty() {
            return None;
        }
        match self.regression.interpolate(distance) {
            Some(rate) if rate.is_finite() => Some(rate.max(0.0)),
            _ => None,
        }
    }

    /// Number of retained raw observations (test/introspection).
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.raw_points.len()
    }
}

impl Default for ProximityPrior {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// COLD START (no samples): the estimate is the analytic distance prior,
    /// which is strictly decreasing in ring distance — a near-key contract
    /// predicts MORE read-demand than a far-key one from the very first moment.
    /// This is the cold-start drift fix: before, every distance returned a flat
    /// `NEUTRAL_DEMAND`, so near-key contracts were under-retained.
    #[test]
    fn cold_prior_is_monotone_decreasing_in_distance() {
        let prior = ProximityPrior::new();
        assert_eq!(prior.len(), 0);

        // Sweep the valid ring-distance range and assert non-increasing +
        // strictly positive.
        let mut prev = f64::INFINITY;
        let mut x = 0.0;
        while x <= 0.5 + 1e-9 {
            let y = prior.predict(x);
            assert!(y > 0.0, "cold prior must stay positive: g0({x}) = {y}");
            assert!(
                y <= prev + 1e-12,
                "cold prior must be non-increasing in distance: g0({x}) = {y} > previous {prev}"
            );
            prev = y;
            x += 0.05;
        }

        // A near contract must predict STRICTLY more demand than a far one at
        // cold start — the property the flat-neutral behavior lacked.
        assert!(
            prior.predict(0.02) > prior.predict(0.45),
            "near-key cold demand must exceed far-key: near={}, far={}",
            prior.predict(0.02),
            prior.predict(0.45),
        );
    }

    /// As real samples accumulate the learned fit dominates and the analytic
    /// prior's slope washes out (blend weight → 0). A FLAT observed rate across
    /// all distances drives the near-vs-far spread from the prior's large cold
    /// slope down toward ~0 (the flat fit) — which can only happen if the
    /// prior's weight decays as `n` grows.
    #[test]
    fn warm_observations_wash_out_the_sloped_cold_prior() {
        const FLAT_RATE: f64 = 5.0;

        // Cold: a fresh prior carries a real near>far slope from the distance
        // prior.
        let cold = ProximityPrior::new();
        let cold_spread = cold.predict(0.02) - cold.predict(0.45);
        assert!(
            cold_spread > 0.3,
            "cold prior must carry a near>far slope, got spread {cold_spread}"
        );

        // Warm: many flat-rate samples spanning the ring → ~flat fit → near and
        // far predictions converge.
        let mut warm = ProximityPrior::new();
        for i in 0..200 {
            let d = 0.01 + (i % 49) as f64 / 100.0; // 0.01..=0.49
            warm.observe(d, FLAT_RATE);
        }
        let near = warm.predict(0.02);
        let far = warm.predict(0.45);
        let warm_spread = (near - far).abs();
        assert!(
            warm_spread < 0.25,
            "warm flat-rate fit must wash out the prior slope: near={near}, far={far}, spread={warm_spread}"
        );
        assert!(
            near > 3.0 && far > 3.0,
            "warm predictions must track the observed rate {FLAT_RATE}: near={near}, far={far}"
        );
    }

    /// The prediction slides SMOOTHLY from the analytic prior toward the fit as
    /// samples accrue — no flat "neutral" plateau, no discontinuous jump. With a
    /// flat observed rate above the far-distance prior value, the far-distance
    /// prediction strictly INCREASES with each of the first few samples, because
    /// the prior's blend weight strictly shrinks each step.
    #[test]
    fn prediction_transitions_smoothly_from_prior_to_fit() {
        const FLAT_RATE: f64 = 5.0;
        let mut prior = ProximityPrior::new();

        let p0 = prior.predict(0.45); // cold: pure prior at far distance (small)
        prior.observe(0.20, FLAT_RATE);
        let p1 = prior.predict(0.45);
        prior.observe(0.25, FLAT_RATE);
        let p2 = prior.predict(0.45);
        prior.observe(0.30, FLAT_RATE);
        let p3 = prior.predict(0.45);

        assert!(
            p0 < p1 && p1 < p2 && p2 < p3,
            "far-distance prediction must climb monotonically toward the fit as \
             samples accrue: p0={p0}, p1={p1}, p2={p2}, p3={p3}"
        );
        assert!(
            p3 < FLAT_RATE,
            "still short of the full fit after only 3 samples (prior still weighted): p3={p3}"
        );
    }

    /// The blend weight is `1.0` cold, strictly decreasing in the retained
    /// sample count, and decays toward `0` as samples accumulate — the property
    /// that hands control from the analytic prior to the learned fit.
    #[test]
    fn blend_weight_decays_with_sample_count() {
        assert_eq!(blend_weight(0), 1.0, "prior owns the estimate at n = 0");

        // Strictly decreasing across a growing sample count.
        let mut prev = f64::INFINITY;
        for n in [0usize, 1, 2, 5, 10, 25, 50, 100, 250, 500] {
            let w = blend_weight(n);
            assert!(w > 0.0 && w <= 1.0, "weight out of (0, 1]: w({n}) = {w}");
            assert!(
                w < prev,
                "weight must strictly decrease: w({n}) = {w} >= {prev}"
            );
            prev = w;
        }

        // Equal weighting exactly at the pseudo-count, and negligible far past
        // the sample window.
        assert!(
            (blend_weight(PRIOR_PSEUDO_COUNT as usize) - 0.5).abs() < 1e-9,
            "prior and fit weigh equally at n = PRIOR_PSEUDO_COUNT"
        );
        assert!(
            blend_weight(MAX_PRIOR_POINTS) < 0.02,
            "prior weight must be negligible once the sample window is full: {}",
            blend_weight(MAX_PRIOR_POINTS)
        );
    }

    /// The fitted prior is monotone NON-INCREASING in distance: closer contracts
    /// predict at least as much demand as farther ones. This is the defining
    /// property of the proximity prior and the reason a descending isotonic fit
    /// is used.
    #[test]
    fn fitted_prior_is_monotone_non_increasing() {
        let mut prior = ProximityPrior::new();
        // Near contracts see high rates, far contracts low rates, with noise.
        let samples = [
            (0.02, 20.0),
            (0.05, 18.0),
            (0.05, 22.0), // noise around the near band
            (0.10, 12.0),
            (0.15, 9.0),
            (0.20, 7.0),
            (0.30, 3.0),
            (0.40, 2.0),
            (0.45, 1.0),
            (0.48, 0.5),
        ];
        for (d, r) in samples {
            prior.observe(d, r);
        }
        assert_eq!(prior.len(), samples.len());

        // Sample the curve WITHIN the observed data range [0.05, 0.45] (where
        // interpolation between the non-increasing fitted points is guaranteed
        // monotone) and assert non-increasing. Both blended terms — the analytic
        // prior and the descending fit — are non-increasing in distance, so
        // their blend is too. Extrapolation beyond the data edges is exercised
        // separately by `prediction_is_non_negative_out_of_range`.
        let mut prev = f64::INFINITY;
        let mut x = 0.05;
        while x <= 0.45 + 1e-9 {
            let y = prior.predict(x);
            assert!(
                y <= prev + 1e-9,
                "prior must be non-increasing in distance: g({x}) = {y} > previous {prev}"
            );
            prev = y;
            x += 0.02;
        }

        // A near contract must predict strictly more demand than a far one.
        assert!(
            prior.predict(0.05) > prior.predict(0.45),
            "near-key demand must exceed far-key demand: near={}, far={}",
            prior.predict(0.05),
            prior.predict(0.45),
        );
    }

    /// The rolling window bounds retained points; the estimator keeps producing
    /// finite, non-negative predictions after eviction.
    #[test]
    fn rolling_window_is_bounded() {
        let mut prior = ProximityPrior::new();
        for i in 0..(MAX_PRIOR_POINTS + 100) {
            let d = (i % 50) as f64 / 100.0; // distances 0.0..0.49
            prior.observe(d, (50 - (i % 50)) as f64);
        }
        assert!(
            prior.len() <= MAX_PRIOR_POINTS,
            "raw points must be bounded, got {}",
            prior.len()
        );
        let y = prior.predict(0.1);
        assert!(y.is_finite() && y >= 0.0, "prediction must stay valid: {y}");
    }

    /// Garbage inputs (NaN, negatives) are dropped rather than corrupting the
    /// fit.
    #[test]
    fn invalid_observations_are_ignored() {
        let mut prior = ProximityPrior::new();
        prior.observe(f64::NAN, 5.0);
        prior.observe(0.1, f64::INFINITY);
        prior.observe(-0.1, 5.0);
        prior.observe(0.1, -5.0);
        assert_eq!(prior.len(), 0, "no invalid point should be retained");
        // With no retained points the estimate is the pure cold prior; it must
        // stay finite and positive.
        let cold = prior.predict(0.1);
        assert!(
            cold.is_finite() && cold > 0.0,
            "empty prior must return a finite positive estimate, got {cold}"
        );
    }

    /// A non-finite `distance` (NaN or ±∞) short-circuits to `NEUTRAL_DEMAND`
    /// rather than propagating through `distance_prior`/the fit. This is the
    /// guard that keeps a NaN out of `keep_score` — a NaN there would poison the
    /// `total_cmp` eviction sort (every comparison against NaN is `Greater` under
    /// `total_cmp`, silently corrupting the victim order). Holds cold (no
    /// samples, pure prior) and the check lives in `predict` itself, so it holds
    /// warm too.
    #[test]
    fn predict_guards_non_finite_distance() {
        let cold = ProximityPrior::new();
        assert_eq!(cold.predict(f64::NAN), NEUTRAL_DEMAND);
        assert_eq!(cold.predict(f64::INFINITY), NEUTRAL_DEMAND);
        assert_eq!(cold.predict(f64::NEG_INFINITY), NEUTRAL_DEMAND);

        // Same guard once the estimator is warm (has a usable fit).
        let mut warm = ProximityPrior::new();
        for i in 0..20 {
            warm.observe(0.01 * i as f64, 5.0);
        }
        assert_eq!(warm.predict(f64::NAN), NEUTRAL_DEMAND);
        assert_eq!(warm.predict(f64::INFINITY), NEUTRAL_DEMAND);
        assert_eq!(warm.predict(f64::NEG_INFINITY), NEUTRAL_DEMAND);
    }

    /// Predicted demand is always non-negative, even at distances beyond the
    /// observed data range where the isotonic fit may extrapolate.
    #[test]
    fn prediction_is_non_negative_out_of_range() {
        let mut prior = ProximityPrior::new();
        for i in 0..10 {
            // All samples clustered near the origin; predict far away.
            prior.observe(0.01 * i as f64, 5.0 - 0.4 * i as f64);
        }
        assert!(prior.predict(0.5) >= 0.0);
        assert!(prior.predict(0.49) >= 0.0);
    }
}
