//! Proximity-prior demand estimator for demand-driven hosting eviction (A3).
//!
//! Piece A3 of the demand-driven hosting redesign (freenet/freenet-core#4642).
//! The demand-ordered (Greedy-Dual) eviction policy in [`super::cache`] orders contracts by
//! `keep_score = eviction_floor + predicted_demand(X)`. This module supplies
//! `predicted_demand(X)`: a per-contract estimate of X's read-request rate at
//! this peer.
//!
//! For A3 the estimate is the **proximity prior only** — `g(distance-to-key)`,
//! a monotone non-increasing function of the ring distance between this peer
//! and the contract's key. Contracts near this peer's location are, on average,
//! requested more often (routing gravity), so demand falls off with distance.
//! The blend toward each contract's OWN observed read rate is deferred to A4
//! (see `docs/design/hosting-eviction.md`); this estimator only tracks the
//! aggregate distance -> rate relationship.
//!
//! The prior is fit with the same isotonic-regression (PAVA) machinery the
//! router uses for its distance -> outcome curves ([`crate::router`] /
//! `pav_regression`), deliberately reused rather than re-implemented. Points are
//! `(distance, observed_read_rate)` samples this peer collects from its own
//! reads; the fit is **descending** (rate non-increasing in distance). A rolling
//! window bounds the retained points, so the curve tracks the peer's recent
//! demand geometry rather than its whole history.
//!
//! Only RATIOS matter to eviction ordering (`keep_score` is compared, never
//! interpreted as an absolute rate), so no absolute calibration is required.

use pav_regression::{IsotonicRegression, Point};
use std::collections::VecDeque;

/// Minimum retained sample count before the fitted prior is trusted. Below this
/// the estimator returns [`NEUTRAL_DEMAND`] for every distance, which makes
/// `keep_score` degenerate to `eviction_floor + NEUTRAL_DEMAND` for all
/// contracts — i.e. pure Greedy-Dual aging with a recency tiebreak, the sane
/// cold-start behavior before any demand geometry is known.
const MIN_POINTS_FOR_PRIOR: usize = 5;

/// Maximum raw `(distance, rate)` points retained. Once reached, each new
/// observation evicts the oldest (FIFO rolling window), mirroring the router's
/// `MAX_REGRESSION_POINTS`. Keeps the fit tracking recent demand and bounds
/// memory + refit cost.
const MAX_PRIOR_POINTS: usize = 500;

/// Demand returned when the prior has insufficient data (or cannot interpolate).
/// Positive and uniform: with equal demand for every contract, eviction reduces
/// to Greedy-Dual floor-ordering with a last-read tiebreak. Must be `> 0` so
/// that an abandoned contract (whose `keep_score` is dropped to the current
/// `eviction_floor`, i.e. zero demand credit) sorts below a still-credited one.
pub(crate) const NEUTRAL_DEMAND: f64 = 1.0;

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
    /// Create an empty estimator. Until [`MIN_POINTS_FOR_PRIOR`] observations
    /// accrue, [`predict`](Self::predict) returns [`NEUTRAL_DEMAND`].
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
    /// Returns [`NEUTRAL_DEMAND`] until [`MIN_POINTS_FOR_PRIOR`] samples have
    /// accrued, or if the regression cannot interpolate. Otherwise returns the
    /// fitted rate, floored at `0.0` (the descending fit can extrapolate
    /// slightly negative near the data-range edges).
    pub(crate) fn predict(&self, distance: f64) -> f64 {
        if self.raw_points.len() < MIN_POINTS_FOR_PRIOR || !distance.is_finite() {
            return NEUTRAL_DEMAND;
        }
        match self.regression.interpolate(distance) {
            Some(rate) if rate.is_finite() => rate.max(0.0),
            _ => NEUTRAL_DEMAND,
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

    /// With no data the estimator is neutral for every distance, so eviction
    /// degenerates to floor-ordering + recency (the cold-start contract).
    #[test]
    fn empty_prior_is_neutral_everywhere() {
        let prior = ProximityPrior::new();
        assert_eq!(prior.len(), 0);
        assert_eq!(prior.predict(0.0), NEUTRAL_DEMAND);
        assert_eq!(prior.predict(0.25), NEUTRAL_DEMAND);
        assert_eq!(prior.predict(0.5), NEUTRAL_DEMAND);
    }

    /// Below the minimum sample count the fit is not trusted yet.
    #[test]
    fn prior_stays_neutral_below_min_points() {
        let mut prior = ProximityPrior::new();
        for i in 0..(MIN_POINTS_FOR_PRIOR - 1) {
            prior.observe(0.1 * i as f64, 10.0 - i as f64);
        }
        assert!(prior.len() < MIN_POINTS_FOR_PRIOR);
        assert_eq!(prior.predict(0.0), NEUTRAL_DEMAND);
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
        assert!(prior.len() >= MIN_POINTS_FOR_PRIOR);

        // Sample the curve WITHIN the observed data range [0.05, 0.45] (where
        // interpolation between the non-increasing fitted points is guaranteed
        // monotone) and assert non-increasing. Extrapolation beyond the data
        // edges is exercised separately by `prediction_is_non_negative_out_of_range`.
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
        assert_eq!(prior.predict(0.1), NEUTRAL_DEMAND);
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
