//! Kernel-weighted residual correction with empirical-Bayes shrinkage.
//!
//! This module holds the pure math behind the routing predictor's correction
//! layer. It is deliberately free of any dependency on `renegade-ml`, the ring,
//! or the router, so every property below is unit-testable in isolation.
//!
//! # Why a kernel, and why this particular effective-sample-size
//!
//! The correction answers "how much should I trust a k-NN estimate for *this*
//! query?". Two ingredients:
//!
//! 1. **Gaussian kernel weights** `w_i = exp(-d_i² / 2h²)`. `renegade-ml`'s own
//!    `weighted_mean` uses `w = 1/d`, which is scale-free: it has no notion of
//!    "far", so a query whose nearest observation is on the other side of the
//!    feature space still produces a confident-looking number. A kernel needs a
//!    length scale `h`, and that is exactly what lets "far" mean something.
//!
//! 2. **`n_eff` as kernel mass** `Σ w_i`, not Kish's `(Σw)² / Σw²`. Kish's
//!    effective sample size is invariant to uniform scaling of the weights, so a
//!    neighbourhood of five uniformly-*distant* points still reports `n_eff ≈ 5`
//!    — which defeats the entire purpose. Kernel mass is the Nadaraya-Watson
//!    denominator: a neighbour at distance 0 counts as one full observation, one
//!    at `2h` counts 0.135, one at `4h` counts 0.0003. So a query with no
//!    genuinely nearby evidence has `n_eff → 0`.
//!
//! # Shrinkage
//!
//! Under the hierarchical model `r_i ~ N(r, σ²_noise)`, `r ~ N(0, σ²_signal)`,
//! the posterior mean of the correction is exactly `λ · r̂` with
//!
//! ```text
//! λ = n_eff / (n_eff + κ),    κ = σ²_noise / σ²_signal
//! ```
//!
//! This is the Bayes estimator for that model rather than a heuristic, and it
//! gives the two properties the previous fixed-weight blend lacked:
//!
//! - **Neutral when uninformed.** `n_eff → 0 ⟹ λ → 0 ⟹ prediction = base`,
//!   exactly. No cap is needed to bound the harm of an uninformed prediction,
//!   because an uninformed prediction now contributes nothing. This is what
//!   makes the old `MAX_RENEGADE_WEIGHT` ceiling unnecessary rather than merely
//!   mistuned.
//! - **Full correction when informed.** Dense nearby evidence drives `λ → 1`, so
//!   a peer genuinely dropping requests for one contract gets the *whole*
//!   correction — which a permanent 50% ceiling forbids by construction.
//!
//! `κ` is not chosen by hand: [`ShrinkageSelector`] keeps a small grid and
//! selects by measured prequential loss, so the derivation fixes the *shape* and
//! the data fixes the *value*.

/// Candidate `κ` values. Spans "trust a single nearby observation" (0.5) to
/// "demand tens of observations before correcting much" (32).
const KAPPA_GRID: [f64; 7] = [0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0];

/// Multiple of the residual standard deviation at which a multiplicative
/// (log-space) correction is clamped. `exp()` is unbounded, so an unclamped
/// log-correction can turn one bad residual into an arbitrarily large estimate.
/// Three sigma is a data-derived bound rather than a magic absolute constant.
const LOG_CORRECTION_SIGMA_CLAMP: f64 = 3.0;

/// Number of stored points sampled when estimating the kernel bandwidth.
/// The bandwidth is a global length scale, so a sample is sufficient and keeps
/// the estimate O(S log n) rather than O(n log n) per training round.
pub(crate) const BANDWIDTH_SAMPLE_POINTS: usize = 128;

/// A kernel-weighted estimate of the residual at one query point.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct KernelEstimate {
    /// Nadaraya-Watson weighted mean of the neighbours' residuals.
    pub residual: f64,
    /// Kernel mass `Σ w_i` — the effective number of observations supporting
    /// `residual`. Zero means "no nearby evidence"; see the module docs for why
    /// this is kernel mass rather than Kish's ratio.
    pub n_eff: f64,
}

/// Kernel-weight a set of `(distance, residual)` neighbours.
///
/// Returns `None` when the neighbour set is empty, when `bandwidth` is not a
/// positive finite number, or when the kernel mass underflows to zero — all of
/// which mean "no usable evidence", and all of which the caller must treat as
/// "leave the base estimate alone".
///
/// Non-finite neighbours are skipped rather than allowed to poison the mean: a
/// single NaN residual would otherwise make every downstream prediction NaN, and
/// a NaN failure probability propagates into the router's cost comparator.
pub(crate) fn kernel_estimate(
    neighbors: &[(f64, f64)],
    bandwidth: f64,
) -> Option<KernelEstimate> {
    if neighbors.is_empty() || !bandwidth.is_finite() || bandwidth <= 0.0 {
        return None;
    }

    let two_h_squared = 2.0 * bandwidth * bandwidth;
    let mut weight_sum = 0.0f64;
    let mut value_sum = 0.0f64;

    for &(distance, residual) in neighbors {
        if !distance.is_finite() || !residual.is_finite() || distance < 0.0 {
            continue;
        }
        let weight = (-(distance * distance) / two_h_squared).exp();
        if !weight.is_finite() {
            continue;
        }
        weight_sum += weight;
        value_sum += weight * residual;
    }

    if !(weight_sum > 0.0) || !weight_sum.is_finite() {
        return None;
    }

    let residual = value_sum / weight_sum;
    if !residual.is_finite() {
        return None;
    }

    Some(KernelEstimate {
        residual,
        n_eff: weight_sum,
    })
}

/// Estimate the kernel bandwidth as the median of per-point k-th-nearest-neighbour
/// distances.
///
/// This is the feature-space length scale at which "k neighbours" stops being a
/// local neighbourhood, so it adapts to however densely the observations happen
/// to be distributed instead of pinning a constant. `samples` is consumed
/// (sorted in place) and non-finite or non-positive entries are ignored.
///
/// Returns `None` when no usable sample remains, which the caller must treat as
/// "no bandwidth yet, so no correction".
pub(crate) fn estimate_bandwidth(samples: &mut Vec<f64>) -> Option<f64> {
    samples.retain(|d| d.is_finite() && *d > 0.0);
    if samples.is_empty() {
        return None;
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = samples.len() / 2;
    let median = if samples.len() % 2 == 0 {
        (samples[mid - 1] + samples[mid]) / 2.0
    } else {
        samples[mid]
    };
    if median.is_finite() && median > 0.0 {
        Some(median)
    } else {
        None
    }
}

/// Online selection of the shrinkage parameter `κ` by prequential squared loss.
///
/// Each candidate `κ` in [`KAPPA_GRID`] accumulates the squared error its
/// shrunk correction *would have* made. `lambda` uses the current best. This is
/// standard online model selection: asymptotically no worse than the best fixed
/// `κ` in the grid, at a cost of one `f64` per candidate.
///
/// Scoring the correction against the actual residual is equivalent to scoring
/// the final prediction against the actual outcome, since
/// `(base + λr̂) − actual = λr̂ − (actual − base)`.
#[derive(Debug, Clone)]
pub(crate) struct ShrinkageSelector {
    /// Accumulated squared error per candidate, parallel to [`KAPPA_GRID`].
    squared_error: [f64; KAPPA_GRID.len()],
    /// Number of scored corrections.
    scored: u64,
    /// Welford accumulators for the spread of observed residuals, used to bound
    /// multiplicative corrections.
    residual_mean: f64,
    residual_m2: f64,
    residual_count: u64,
}

impl Default for ShrinkageSelector {
    fn default() -> Self {
        Self::new()
    }
}

impl ShrinkageSelector {
    pub(crate) fn new() -> Self {
        Self {
            squared_error: [0.0; KAPPA_GRID.len()],
            scored: 0,
            residual_mean: 0.0,
            residual_m2: 0.0,
            residual_count: 0,
        }
    }

    /// The currently-best `κ`. Before any correction has been scored there is no
    /// evidence to choose on, so this returns the middle of the grid — a
    /// deliberately cautious starting point, since a too-large `κ` only delays
    /// the correction whereas a too-small one applies it before it is earned.
    pub(crate) fn kappa(&self) -> f64 {
        if self.scored == 0 {
            return KAPPA_GRID[KAPPA_GRID.len() / 2];
        }
        let mut best_index = 0;
        for index in 1..KAPPA_GRID.len() {
            if self.squared_error[index] < self.squared_error[best_index] {
                best_index = index;
            }
        }
        KAPPA_GRID[best_index]
    }

    /// Shrinkage factor `λ = n_eff / (n_eff + κ)` for the given evidence mass.
    ///
    /// Guaranteed to land in `[0, 1]`, and to be exactly `0.0` for zero or
    /// non-finite evidence so that a caller can rely on `apply(base, 0.0) == base`.
    pub(crate) fn lambda(&self, n_eff: f64) -> f64 {
        if !n_eff.is_finite() || n_eff <= 0.0 {
            return 0.0;
        }
        let kappa = self.kappa();
        let lambda = n_eff / (n_eff + kappa);
        if lambda.is_finite() {
            lambda.clamp(0.0, 1.0)
        } else {
            0.0
        }
    }

    /// Score every candidate `κ` against a realised residual, and fold that
    /// residual into the spread estimate.
    pub(crate) fn record(&mut self, n_eff: f64, predicted_residual: f64, actual_residual: f64) {
        if !actual_residual.is_finite() {
            return;
        }

        self.residual_count += 1;
        let delta = actual_residual - self.residual_mean;
        self.residual_mean += delta / self.residual_count as f64;
        self.residual_m2 += delta * (actual_residual - self.residual_mean);

        if !n_eff.is_finite() || n_eff <= 0.0 || !predicted_residual.is_finite() {
            return;
        }

        for (index, kappa) in KAPPA_GRID.iter().enumerate() {
            let lambda = n_eff / (n_eff + kappa);
            let error = lambda * predicted_residual - actual_residual;
            if error.is_finite() {
                self.squared_error[index] += error * error;
            }
        }
        self.scored += 1;
    }

    /// Standard deviation of observed residuals, once there are enough to make
    /// one meaningful.
    pub(crate) fn residual_sigma(&self) -> Option<f64> {
        if self.residual_count < 2 {
            return None;
        }
        let variance = self.residual_m2 / (self.residual_count - 1) as f64;
        if variance.is_finite() && variance > 0.0 {
            Some(variance.sqrt())
        } else {
            None
        }
    }

    /// Bound a log-space correction to `±3σ` of the observed residual spread.
    ///
    /// Only used for the multiplicative stages: `base * exp(correction)` is
    /// unbounded above, so one pathological residual could otherwise dominate a
    /// routing decision. Returns the correction unchanged while there is no
    /// usable spread estimate yet (in which case `λ` is still near zero anyway).
    pub(crate) fn clamp_log_correction(&self, correction: f64) -> f64 {
        match self.residual_sigma() {
            Some(sigma) => {
                let bound = LOG_CORRECTION_SIGMA_CLAMP * sigma;
                correction.clamp(-bound, bound)
            }
            None => correction,
        }
    }

    pub(crate) fn scored(&self) -> u64 {
        self.scored
    }
}

/// Prequential accuracy for one prediction layer, scored against the
/// climatological base rate rather than an absolute threshold.
///
/// # Why skill rather than raw Brier
///
/// For a binary outcome with base rate `p̄`, a constant predictor that always
/// says `p̄` scores `p̄(1−p̄)`. At the ~1% failure rate a production gateway
/// actually sees that is 0.0099 — which an absolute scale reading "< 0.05
/// excellent" grades as excellent. That is not a hypothetical: it is exactly how
/// a routing model with *negative* skill went unnoticed for six months, because
/// the dashboard graded rarity and called it accuracy.
///
/// The skill score divides that out:
///
/// ```text
/// skill = 1 − brier / (p̄(1−p̄))
/// ```
///
/// Zero means "no better than assuming the base rate", one means perfect, and
/// **negative means worse than assuming nothing** — the reading that matters and
/// that an absolute threshold cannot express.
#[derive(Debug, Clone, Default)]
pub(crate) struct SkillTracker {
    squared_error: f64,
    outcome_sum: f64,
    count: u64,
}

impl SkillTracker {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn record(&mut self, predicted: f64, actual: f64) {
        if !predicted.is_finite() || !actual.is_finite() {
            return;
        }
        let error = predicted - actual;
        self.squared_error += error * error;
        self.outcome_sum += actual;
        self.count += 1;
    }

    pub(crate) fn count(&self) -> u64 {
        self.count
    }

    /// Mean squared error — the Brier score for a probabilistic binary forecast.
    pub(crate) fn brier(&self) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        Some(self.squared_error / self.count as f64)
    }

    /// Observed base rate over the scored window.
    pub(crate) fn base_rate(&self) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        Some(self.outcome_sum / self.count as f64)
    }

    /// Brier score a constant base-rate forecast would have achieved. This is the
    /// baseline every layer has to beat to be worth its cost.
    pub(crate) fn climatology_brier(&self) -> Option<f64> {
        let base = self.base_rate()?;
        let value = base * (1.0 - base);
        if value > 0.0 { Some(value) } else { None }
    }

    /// `1 − brier / climatology`. `None` when the window holds no variation to
    /// score against (an all-success or all-failure window), where skill is
    /// genuinely undefined rather than zero.
    pub(crate) fn skill(&self) -> Option<f64> {
        let brier = self.brier()?;
        let climatology = self.climatology_brier()?;
        let skill = 1.0 - brier / climatology;
        skill.is_finite().then_some(skill)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `h` such that the numbers below are easy to reason about.
    const H: f64 = 1.0;

    #[test]
    fn kernel_estimate_averages_coincident_neighbours_exactly() {
        // Three neighbours all at distance 0 => each contributes weight 1.
        let estimate = kernel_estimate(&[(0.0, 0.2), (0.0, 0.4), (0.0, 0.6)], H).unwrap();
        assert!((estimate.residual - 0.4).abs() < 1e-12);
        assert!(
            (estimate.n_eff - 3.0).abs() < 1e-12,
            "k coincident neighbours must give n_eff = k, got {}",
            estimate.n_eff
        );
    }

    #[test]
    fn n_eff_is_kernel_mass_not_kish_ratio() {
        // Five neighbours, all far (4h). Kish's (Σw)²/Σw² would report ~5 here
        // because it is scale-invariant; kernel mass must report ~0.
        let far: Vec<(f64, f64)> = (0..5).map(|_| (4.0 * H, 1.0)).collect();
        let estimate = kernel_estimate(&far, H).unwrap();
        let kish = 5.0; // by construction, equal weights => Kish n_eff == count
        assert!(
            estimate.n_eff < 0.01,
            "kernel mass must collapse for uniformly distant neighbours, got {} \
             (Kish would report {kish})",
            estimate.n_eff
        );
    }

    #[test]
    fn n_eff_decays_monotonically_with_distance() {
        let mut previous = f64::INFINITY;
        for step in 0..40 {
            let distance = step as f64 * 0.25;
            let estimate = kernel_estimate(&[(distance, 0.5)], H).unwrap();
            assert!(
                estimate.n_eff <= previous + 1e-12,
                "n_eff must be non-increasing in distance: {} > {} at d={distance}",
                estimate.n_eff,
                previous
            );
            previous = estimate.n_eff;
        }
        assert!(
            previous < 1e-9,
            "n_eff must approach zero far from the data, got {previous}"
        );
    }

    #[test]
    fn one_near_neighbour_dominates_many_far_ones() {
        let mut neighbors = vec![(0.0, 1.0)];
        neighbors.extend((0..20).map(|_| (5.0 * H, -1.0)));
        let estimate = kernel_estimate(&neighbors, H).unwrap();
        assert!(
            (estimate.n_eff - 1.0).abs() < 0.01,
            "n_eff should be ~1 for one near plus many far, got {}",
            estimate.n_eff
        );
        assert!(
            estimate.residual > 0.99,
            "the near neighbour must dominate the mean, got {}",
            estimate.residual
        );
    }

    #[test]
    fn kernel_estimate_rejects_unusable_input() {
        assert!(kernel_estimate(&[], H).is_none());
        assert!(kernel_estimate(&[(0.0, 1.0)], 0.0).is_none());
        assert!(kernel_estimate(&[(0.0, 1.0)], -1.0).is_none());
        assert!(kernel_estimate(&[(0.0, 1.0)], f64::NAN).is_none());
        // Every neighbour unusable => None rather than NaN.
        assert!(kernel_estimate(&[(f64::NAN, 1.0), (0.0, f64::NAN)], H).is_none());
    }

    #[test]
    fn kernel_estimate_skips_non_finite_neighbours_without_poisoning() {
        let estimate =
            kernel_estimate(&[(0.0, 1.0), (f64::NAN, 5.0), (0.0, f64::INFINITY)], H).unwrap();
        assert!(
            estimate.residual.is_finite(),
            "a NaN neighbour must not make the estimate non-finite"
        );
        assert!((estimate.residual - 1.0).abs() < 1e-12);
    }

    #[test]
    fn bandwidth_is_median_of_samples() {
        let mut samples = vec![0.4, 0.1, 0.3, 0.2, 0.5];
        assert_eq!(estimate_bandwidth(&mut samples), Some(0.3));

        let mut even = vec![0.2, 0.4];
        assert_eq!(estimate_bandwidth(&mut even), Some(0.30000000000000004));
    }

    #[test]
    fn bandwidth_ignores_unusable_samples() {
        let mut samples = vec![f64::NAN, 0.0, -1.0, f64::INFINITY, 0.5];
        assert_eq!(estimate_bandwidth(&mut samples), Some(0.5));

        let mut all_bad = vec![f64::NAN, 0.0, -3.0];
        assert_eq!(estimate_bandwidth(&mut all_bad), None);

        let mut empty: Vec<f64> = Vec::new();
        assert_eq!(estimate_bandwidth(&mut empty), None);
    }

    #[test]
    fn lambda_is_zero_without_evidence() {
        let selector = ShrinkageSelector::new();
        assert_eq!(selector.lambda(0.0), 0.0);
        assert_eq!(selector.lambda(-1.0), 0.0);
        assert_eq!(selector.lambda(f64::NAN), 0.0);
        assert_eq!(selector.lambda(f64::INFINITY), 0.0);
    }

    #[test]
    fn lambda_is_bounded_and_increasing_in_evidence() {
        let selector = ShrinkageSelector::new();
        let mut previous = 0.0;
        for step in 1..500 {
            let n_eff = step as f64 * 0.5;
            let lambda = selector.lambda(n_eff);
            assert!(
                (0.0..=1.0).contains(&lambda),
                "lambda must stay in [0,1], got {lambda}"
            );
            assert!(
                lambda >= previous - 1e-12,
                "lambda must be non-decreasing in n_eff"
            );
            previous = lambda;
        }
        assert!(
            previous > 0.85,
            "lambda must approach 1 with abundant evidence, got {previous}"
        );
    }

    #[test]
    fn lambda_equals_half_at_n_eff_equal_kappa() {
        let selector = ShrinkageSelector::new();
        let kappa = selector.kappa();
        assert!((selector.lambda(kappa) - 0.5).abs() < 1e-12);
    }

    #[test]
    fn kappa_selection_prefers_small_kappa_when_residuals_are_learnable() {
        // Residual is a clean, reproducible signal: the best policy is to apply
        // the correction aggressively, i.e. a small kappa.
        let mut selector = ShrinkageSelector::new();
        for _ in 0..2_000 {
            selector.record(4.0, 0.3, 0.3);
        }
        assert!(
            selector.kappa() <= 1.0,
            "a perfectly predictable residual should select an aggressive kappa, got {}",
            selector.kappa()
        );
    }

    #[test]
    fn kappa_selection_prefers_large_kappa_when_residuals_are_noise() {
        // The predicted residual carries no information about the actual one, so
        // the best policy is to shrink hard.
        let mut selector = ShrinkageSelector::new();
        let mut state = 12_345u64;
        for _ in 0..4_000 {
            // xorshift for a deterministic pseudo-random sign
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let actual = if state % 2 == 0 { 0.5 } else { -0.5 };
            selector.record(4.0, 0.5, actual);
        }
        assert!(
            selector.kappa() >= 8.0,
            "pure noise should select a conservative kappa, got {}",
            selector.kappa()
        );
    }

    #[test]
    fn residual_sigma_tracks_spread() {
        let mut selector = ShrinkageSelector::new();
        assert_eq!(selector.residual_sigma(), None);
        selector.record(1.0, 0.0, 1.0);
        assert_eq!(selector.residual_sigma(), None, "one sample has no spread");
        for value in [-1.0, 1.0, -1.0, 1.0, -1.0] {
            selector.record(1.0, 0.0, value);
        }
        let sigma = selector.residual_sigma().expect("spread after six samples");
        assert!(
            (sigma - 1.0).abs() < 0.2,
            "sigma of ±1 samples should be ~1, got {sigma}"
        );
    }

    #[test]
    fn log_correction_clamp_bounds_to_three_sigma() {
        let mut selector = ShrinkageSelector::new();
        for value in [-1.0, 1.0, -1.0, 1.0, -1.0, 1.0] {
            selector.record(1.0, 0.0, value);
        }
        let sigma = selector.residual_sigma().unwrap();
        let clamped = selector.clamp_log_correction(1_000.0);
        assert!(
            (clamped - 3.0 * sigma).abs() < 1e-9,
            "a huge correction must clamp to +3 sigma, got {clamped}"
        );
        assert!((selector.clamp_log_correction(-1_000.0) + 3.0 * sigma).abs() < 1e-9);
        // Inside the bound, untouched.
        assert_eq!(selector.clamp_log_correction(0.25), 0.25);
    }

    #[test]
    fn log_correction_clamp_is_identity_without_spread_estimate() {
        let selector = ShrinkageSelector::new();
        assert_eq!(selector.clamp_log_correction(42.0), 42.0);
    }

    #[test]
    fn record_ignores_non_finite_actuals() {
        let mut selector = ShrinkageSelector::new();
        selector.record(1.0, 0.0, f64::NAN);
        selector.record(1.0, 0.0, f64::INFINITY);
        assert_eq!(selector.scored(), 0);
        assert_eq!(selector.residual_sigma(), None);
    }

    #[test]
    fn skill_is_zero_for_a_constant_base_rate_forecast() {
        // 100 events at a 10% base rate, predicting exactly the base rate every
        // time. This is the definition of zero skill and it must read as zero,
        // however flattering the absolute Brier looks.
        let mut tracker = SkillTracker::new();
        for index in 0..100 {
            let actual = if index < 10 { 1.0 } else { 0.0 };
            tracker.record(0.10, actual);
        }
        let skill = tracker.skill().expect("skill is defined for a mixed window");
        assert!(
            skill.abs() < 1e-9,
            "a climatology forecast must score exactly zero skill, got {skill}"
        );
    }

    /// The measured production case. Reproducing it here is the regression test
    /// for the reason this was missed: an absolute "< 0.05 excellent" grade on a
    /// rare event flatters a forecast that is *worse* than assuming nothing.
    #[test]
    fn absolute_brier_can_look_excellent_while_skill_is_negative() {
        // gateway 1's recent window: 200 predictions, 2 real failures neither of
        // which was predicted, plus 1 full-confidence false alarm.
        let mut tracker = SkillTracker::new();
        tracker.record(1.0, 0.0); // false alarm at p=1.0
        tracker.record(0.0, 1.0); // missed failure
        tracker.record(0.0, 1.0); // missed failure
        for _ in 0..197 {
            tracker.record(0.0, 0.0);
        }

        let brier = tracker.brier().unwrap();
        let skill = tracker.skill().unwrap();

        assert!(
            brier < 0.05,
            "this window's absolute Brier lands in the old 'excellent' band ({brier})"
        );
        assert!(
            skill < 0.0,
            "yet its skill must be negative — worse than assuming nothing — got {skill}"
        );
    }

    #[test]
    fn skill_is_one_for_a_perfect_forecast() {
        let mut tracker = SkillTracker::new();
        for index in 0..100 {
            let actual = if index < 25 { 1.0 } else { 0.0 };
            tracker.record(actual, actual);
        }
        assert!((tracker.skill().unwrap() - 1.0).abs() < 1e-9);
    }

    #[test]
    fn skill_is_undefined_without_variation_to_score_against() {
        // An all-success window has climatology Brier 0, so skill is genuinely
        // undefined rather than zero or infinite.
        let mut tracker = SkillTracker::new();
        for _ in 0..50 {
            tracker.record(0.02, 0.0);
        }
        assert_eq!(tracker.base_rate(), Some(0.0));
        assert_eq!(tracker.climatology_brier(), None);
        assert_eq!(tracker.skill(), None);
        assert!(tracker.brier().is_some(), "the Brier score is still defined");
    }

    #[test]
    fn skill_tracker_is_empty_before_any_record() {
        let tracker = SkillTracker::new();
        assert_eq!(tracker.count(), 0);
        assert_eq!(tracker.brier(), None);
        assert_eq!(tracker.base_rate(), None);
        assert_eq!(tracker.skill(), None);
    }

    #[test]
    fn skill_tracker_ignores_non_finite_samples() {
        let mut tracker = SkillTracker::new();
        tracker.record(f64::NAN, 0.0);
        tracker.record(0.5, f64::INFINITY);
        assert_eq!(tracker.count(), 0);
    }

    /// The property the whole design rests on: with no evidence the correction
    /// must be exactly zero, so `base + λ·r̂` is bit-for-bit `base`.
    #[test]
    fn no_evidence_leaves_base_bit_for_bit_unchanged() {
        let selector = ShrinkageSelector::new();
        for base in [0.0f64, 0.017, 0.5, 0.999, 1.0, 12.5, 1e6] {
            // Far-field neighbours: kernel mass underflows, so either the estimate
            // is None or lambda is zero. Both must leave base untouched.
            let estimate = kernel_estimate(&[(50.0, 0.9)], 1.0);
            let correction = match estimate {
                Some(e) => selector.lambda(e.n_eff) * e.residual,
                None => 0.0,
            };
            assert_eq!(
                base + correction,
                base,
                "far-field correction must be exactly neutral for base {base}"
            );
        }
    }
}
