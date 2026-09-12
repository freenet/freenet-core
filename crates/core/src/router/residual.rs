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

use super::isotonic_estimator::AdjustmentMode;

/// Candidate `κ` values. Spans "trust a single nearby observation" (0.5) to
/// "demand tens of observations before correcting much" (32).
const KAPPA_GRID: [f64; 7] = [0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0];

/// Multiple of the residual standard deviation at which a multiplicative
/// (log-space) correction is clamped. `exp()` is unbounded, so an unclamped
/// log-correction can turn one bad residual into an arbitrarily large estimate.
/// Three sigma is a data-derived bound rather than a magic absolute constant.
const LOG_CORRECTION_SIGMA_CLAMP: f64 = 3.0;

/// Multipliers applied to the estimated length scale to form bandwidth
/// candidates.
///
/// A single bandwidth set to the data's typical spacing is NOT a safe default,
/// and measuring it is the whole reason this grid exists. The median k-th
/// neighbour distance makes the kernel exactly as coarse as the data is sparse,
/// so `exp(-d^2/2h^2)` is ~1 for nearly every neighbour, `n_eff` is roughly
/// constant everywhere, and the correction degenerates into a global smoother
/// that can never localise to structure finer than the typical spacing.
///
/// Measured on the recoverability harness before this grid existed: a
/// peer x contract effect scored `captured = -0.33` and the learning curve was
/// FLAT from 500 to 4000 events. The correction helped slightly (14% error
/// reduction) while recovering none of the structure it exists to find, which
/// is exactly the failure a relative comparison against the old blend would
/// have called a success.
///
/// So the bandwidth is selected by the same prequential loss that selects
/// `kappa`: the derivation fixes the shape, measurement fixes both values.
pub(crate) const BANDWIDTH_MULTIPLIERS: [f64; 6] = [0.05, 0.1, 0.25, 0.5, 1.0, 2.0];

/// Events over which a candidate's accumulated loss decays to ~37% of its
/// weight.
///
/// Without decay, `squared_error` accumulates from process start forever and
/// `best()` is an argmin over the whole lifetime — so the new evidence needed to
/// overturn a long-standing lead grows without bound as a node stays up. A
/// gateway running for weeks would become progressively less able to change its
/// mind about `kappa` or the bandwidth, which is the opposite of what a
/// component justified as "measurement fixes the value" should do. Flagged in
/// review of #5642; same spirit as this repo's TTL-bounded GC exemptions.
const LOSS_FORGETTING_EVENTS: f64 = 20_000.0;

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
pub(crate) fn kernel_estimate(neighbors: &[(f64, f64)], bandwidth: f64) -> Option<KernelEstimate> {
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

    if !weight_sum.is_finite() || weight_sum <= 0.0 {
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
    /// Accumulated squared error per (bandwidth, kappa) candidate, indexed
    /// `[bandwidth][kappa]` over [`BANDWIDTH_MULTIPLIERS`] and [`KAPPA_GRID`].
    squared_error: [[f64; KAPPA_GRID.len()]; BANDWIDTH_MULTIPLIERS.len()],
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
            squared_error: [[0.0; KAPPA_GRID.len()]; BANDWIDTH_MULTIPLIERS.len()],
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
        KAPPA_GRID[self.best().1]
    }

    /// Index of the currently-best bandwidth multiplier.
    pub(crate) fn bandwidth_index(&self) -> usize {
        self.best().0
    }

    /// The currently-best bandwidth multiplier.
    pub(crate) fn bandwidth_multiplier(&self) -> f64 {
        BANDWIDTH_MULTIPLIERS[self.best().0]
    }

    /// Best `(bandwidth index, kappa index)` by accumulated prequential loss.
    ///
    /// Before anything has been scored there is no evidence to choose on, so
    /// this returns the middle of each grid — deliberately cautious, since a
    /// too-large `kappa` only delays the correction whereas a too-small one
    /// applies it before it is earned.
    fn best(&self) -> (usize, usize) {
        // Seeded with the cautious midpoint and improved on only STRICTLY, so a
        // fully-tied grid keeps the default rather than collapsing to index
        // (0, 0) — the narrowest bandwidth and smallest kappa, i.e. the most
        // aggressive combination in the grid.
        //
        // Ties are not hypothetical. Before a stage has a bandwidth, every
        // `record` scores an all-`None` estimate array, which adds the SAME loss
        // to every candidate while still incrementing `scored`. Without a strict
        // comparison the selector would leave warm-up already committed to the
        // most aggressive settings, on the strength of evidence that
        // distinguished nothing.
        let mut best = (BANDWIDTH_MULTIPLIERS.len() / 2, KAPPA_GRID.len() / 2);
        if self.scored == 0 {
            return best;
        }
        for bandwidth in 0..BANDWIDTH_MULTIPLIERS.len() {
            for kappa in 0..KAPPA_GRID.len() {
                if self.squared_error[bandwidth][kappa] < self.squared_error[best.0][best.1] {
                    best = (bandwidth, kappa);
                }
            }
        }
        best
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
    /// Score every `(bandwidth, kappa)` candidate against a realised residual.
    ///
    /// `estimates` is parallel to [`BANDWIDTH_MULTIPLIERS`]; a `None` entry means
    /// that bandwidth produced no usable estimate, which is scored as "no
    /// correction" rather than skipped — a candidate that abstains still has to
    /// answer for the residual it declined to predict, or abstaining would look
    /// free.
    pub(crate) fn record(
        &mut self,
        estimates: &[Option<KernelEstimate>; BANDWIDTH_MULTIPLIERS.len()],
        actual_residual: f64,
    ) {
        if !actual_residual.is_finite() {
            return;
        }

        self.residual_count += 1;
        let delta = actual_residual - self.residual_mean;
        self.residual_mean += delta / self.residual_count as f64;
        self.residual_m2 += delta * (actual_residual - self.residual_mean);

        // Decay before accumulating, so distant history fades and a regime
        // change can be recognised within a bounded number of events.
        let retention = 1.0 - 1.0 / LOSS_FORGETTING_EVENTS;
        for row in self.squared_error.iter_mut() {
            for cell in row.iter_mut() {
                *cell *= retention;
            }
        }

        for (bandwidth_index, estimate) in estimates.iter().enumerate() {
            let (n_eff, predicted) = match estimate {
                Some(estimate) if estimate.n_eff.is_finite() && estimate.residual.is_finite() => {
                    (estimate.n_eff, estimate.residual)
                }
                _ => (0.0, 0.0),
            };
            for (kappa_index, kappa) in KAPPA_GRID.iter().enumerate() {
                let lambda = if n_eff > 0.0 {
                    n_eff / (n_eff + kappa)
                } else {
                    0.0
                };
                let error = lambda * predicted - actual_residual;
                if error.is_finite() {
                    self.squared_error[bandwidth_index][kappa_index] += error * error;
                }
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

/// Combine the learned correction with the per-peer EWMA acting as its
/// low-evidence prior.
///
/// ```text
/// prediction = global ⊕ [ (1−λ)·ewma + λ·r̂ ]
/// ```
///
/// # Why the EWMA has to be here
///
/// The correction learns residuals of the GLOBAL curve, which is correct — the
/// EWMA's own noise in the target is unlearnable and was measured to wreck
/// recovery. But composing the *prediction* with the global curve alone made
/// "neutral when uninformed" mean **fall back to distance only**, discarding a
/// per-peer signal that works today.
///
/// That is not a tail case. The residual stages start empty on every node (the
/// batch reload deliberately records no residuals, to avoid leaking future
/// outcomes into past ones), so it is every node after a restart, and every
/// unfamiliar (peer, contract) query forever.
///
/// Neutral should mean "fall back to the best estimate available WITHOUT the
/// correction", and that is the peer-adjusted one. So λ now arbitrates between
/// two priors rather than between a prior and nothing: the EWMA holds where
/// there is no evidence, the correction takes over as evidence accrues. Same
/// shrinkage logic, one level up.
///
/// The consequence that matters: there is no regime where this is worse than
/// the current default. With no evidence it IS the current peer-adjusted
/// estimate (minus the fixed-weight blend, which measured negative skill); with
/// evidence it is the correction. That is what makes turning it on safe.
pub(crate) fn compose_with_prior(
    mode: AdjustmentMode,
    global: f64,
    peer_adjusted: f64,
    lambda: f64,
    correction: f64,
) -> f64 {
    // The EWMA's own adjustment, recovered in the mode's own space — additive
    // offset or log-ratio — so this works for either without special-casing.
    // A `None` means the mode cannot express this pair — multiplicative space
    // with a non-positive value. Falling back to a prior of 0.0 would silently
    // compose against the BARE GLOBAL curve, i.e. exactly the pre-correction
    // behaviour this function exists to prevent, breaking the lambda=0
    // guarantee in the one regime nobody tests. Return the peer-adjusted
    // estimate: that IS the answer when no correction is expressible.
    let Some(prior) = mode.residual(peer_adjusted, global) else {
        return peer_adjusted;
    };
    let lambda = if lambda.is_finite() {
        lambda.clamp(0.0, 1.0)
    } else {
        0.0
    };
    let correction = if correction.is_finite() {
        correction
    } else {
        0.0
    };
    let combined = (1.0 - lambda) * prior + correction;
    if !combined.is_finite() {
        return peer_adjusted;
    }
    let composed = mode.apply(global, combined);
    if composed.is_finite() {
        composed
    } else {
        peer_adjusted
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

    /// Score one `(n_eff, predicted)` pair against every bandwidth candidate.
    ///
    /// These tests are about `kappa` selection, so they hold the bandwidth
    /// dimension constant and let the grid collapse to a single row.
    fn record_uniform(selector: &mut ShrinkageSelector, n_eff: f64, predicted: f64, actual: f64) {
        let estimate = Some(KernelEstimate {
            residual: predicted,
            n_eff,
        });
        selector.record(&[estimate; BANDWIDTH_MULTIPLIERS.len()], actual);
    }

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
            record_uniform(&mut selector, 4.0, 0.3, 0.3);
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
            record_uniform(&mut selector, 4.0, 0.5, actual);
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
        record_uniform(&mut selector, 1.0, 0.0, 1.0);
        assert_eq!(selector.residual_sigma(), None, "one sample has no spread");
        for value in [-1.0, 1.0, -1.0, 1.0, -1.0] {
            record_uniform(&mut selector, 1.0, 0.0, value);
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
            record_uniform(&mut selector, 1.0, 0.0, value);
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
        record_uniform(&mut selector, 1.0, 0.0, f64::NAN);
        record_uniform(&mut selector, 1.0, 0.0, f64::INFINITY);
        assert_eq!(selector.scored(), 0);
        assert_eq!(selector.residual_sigma(), None);
    }

    #[test]
    fn no_evidence_composes_to_exactly_the_peer_adjusted_estimate() {
        // The property that makes enabling this safe: with lambda 0 the result
        // is today's peer-adjusted estimate, NOT the bare global curve.
        for (global, adjusted) in [(0.10f64, 0.18f64), (0.02, 0.01), (0.5, 0.5)] {
            let composed = compose_with_prior(AdjustmentMode::Additive, global, adjusted, 0.0, 0.0);
            assert!(
                (composed - adjusted).abs() < 1e-12,
                "lambda=0 must fall back to the peer-adjusted estimate, got {composed} \
                 for global {global} / adjusted {adjusted}"
            );
        }
    }

    #[test]
    fn full_evidence_composes_to_the_global_curve_plus_the_correction() {
        let composed = compose_with_prior(AdjustmentMode::Additive, 0.10, 0.18, 1.0, 0.25);
        assert!(
            (composed - 0.35).abs() < 1e-12,
            "lambda=1 must drop the prior entirely, got {composed}"
        );
    }

    #[test]
    fn partial_evidence_interpolates_between_the_two_priors() {
        // global 0.10, ewma prior +0.08, correction 0.25 at lambda 0.5
        // => 0.10 + 0.5*0.08 + 0.25 = 0.39
        let composed = compose_with_prior(AdjustmentMode::Additive, 0.10, 0.18, 0.5, 0.25);
        assert!((composed - 0.39).abs() < 1e-12, "got {composed}");
    }

    #[test]
    fn composition_works_in_multiplicative_space() {
        // global 100, adjusted 200 => log-ratio prior ln(2)
        // lambda 0 must recover 200 exactly.
        let composed = compose_with_prior(AdjustmentMode::Multiplicative, 100.0, 200.0, 0.0, 0.0);
        assert!(
            (composed - 200.0).abs() < 1e-9,
            "multiplicative lambda=0 must recover the peer-adjusted value, got {composed}"
        );
        // lambda 1 with no correction returns to the global curve.
        let composed = compose_with_prior(AdjustmentMode::Multiplicative, 100.0, 200.0, 1.0, 0.0);
        assert!((composed - 100.0).abs() < 1e-9, "got {composed}");
    }

    #[test]
    fn composition_survives_unusable_inputs() {
        // A non-finite lambda or correction must degrade to the safe prior
        // rather than propagate into the router's cost comparator.
        for lambda in [f64::NAN, f64::INFINITY, -1.0] {
            let composed = compose_with_prior(AdjustmentMode::Additive, 0.1, 0.18, lambda, 0.0);
            assert!(composed.is_finite(), "lambda {lambda} produced {composed}");
        }
        let composed = compose_with_prior(AdjustmentMode::Additive, 0.1, 0.18, 0.5, f64::NAN);
        assert!((composed - 0.14).abs() < 1e-12, "got {composed}");
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
        let skill = tracker
            .skill()
            .expect("skill is defined for a mixed window");
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
        assert!(
            tracker.brier().is_some(),
            "the Brier score is still defined"
        );
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
    ///
    /// Covers BOTH far-field routes, because they are different code paths and
    /// an earlier version of this test only reached one of them. At `d = 50h`
    /// the weight underflows to exactly `0.0`, so `kernel_estimate` returns
    /// `None` and the `Some` arm — the arm carrying `lambda` and the
    /// multiplication this test claims to exercise — was dead. The `d = 10h`
    /// case keeps the kernel mass denormal-but-nonzero (`~2e-22`), so the
    /// estimate is `Some`, `lambda` really is consulted, and the product really
    /// does have to vanish.
    #[test]
    fn no_evidence_leaves_base_bit_for_bit_unchanged() {
        let selector = ShrinkageSelector::new();

        // Route 1: kernel mass survives as a denormal, so the Some arm runs.
        let near_zero = kernel_estimate(&[(10.0, 0.9)], 1.0)
            .expect("at d = 10h the kernel mass is tiny but non-zero");
        assert!(
            near_zero.n_eff > 0.0 && near_zero.n_eff < 1e-20,
            "this case must exercise the Some arm with negligible evidence, \
             got n_eff {}",
            near_zero.n_eff
        );

        // Route 2: kernel mass underflows entirely.
        assert!(
            kernel_estimate(&[(50.0, 0.9)], 1.0).is_none(),
            "at d = 50h the weight underflows and there is no estimate at all"
        );

        let correction = selector.lambda(near_zero.n_eff) * near_zero.residual;
        assert!(
            correction.abs() < 1e-20,
            "a negligible-evidence correction must be negligible, got {correction}"
        );

        // For any base at a scale a probability or a latency actually occupies,
        // the correction is absorbed exactly.
        for base in [0.017f64, 0.5, 0.999, 1.0, 12.5, 1e6] {
            assert_eq!(
                base + correction,
                base,
                "a Some-but-negligible correction must be exactly neutral for base {base}"
            );
        }

        // Precision about the invariant, because this test found the original
        // claim to be slightly overstated: at base EXACTLY 0.0 there is no
        // mantissa to absorb the denormal, so it survives as ~4e-23 rather than
        // vanishing. That is harmless — it is a failure probability of 4e-23 —
        // but the honest statement for this route is "negligible", not
        // "bit-for-bit". Bit-for-bit holds on the far-field route below, where
        // the correction is exactly 0.0.
        assert!(
            (0.0 + correction).abs() < 1e-20,
            "at base 0.0 the correction survives as a denormal; it must at least \
             stay negligible, got {correction}"
        );

        // Route 2, the true far field: exactly zero, so bit-for-bit for EVERY
        // base including 0.0.
        for base in [0.0f64, 0.017, 0.5, 1.0, 1e6] {
            assert_eq!(base + 0.0, base);
        }
    }

    /// The loss accumulator must be able to change its mind about a candidate
    /// when the regime shifts, rather than being anchored by distant history.
    #[test]
    fn selection_can_change_its_mind_after_a_regime_change() {
        let mut selector = ShrinkageSelector::new();

        // Regime 1: residuals are pure noise, so shrink hard.
        let mut state = 99u64;
        for _ in 0..40_000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let actual = if state % 2 == 0 { 0.5 } else { -0.5 };
            record_uniform(&mut selector, 4.0, 0.5, actual);
        }
        let noisy_kappa = selector.kappa();
        assert!(
            noisy_kappa >= 8.0,
            "a long noise regime should select a conservative kappa, got {noisy_kappa}"
        );

        // Regime 2: the residual becomes perfectly predictable. The selector must
        // follow within a bounded number of events rather than being held by the
        // 40k events of history above.
        for _ in 0..40_000 {
            record_uniform(&mut selector, 4.0, 0.3, 0.3);
        }
        let learnable_kappa = selector.kappa();
        assert!(
            learnable_kappa < noisy_kappa,
            "the selector must adapt to a new regime: kappa stayed at \
             {learnable_kappa} after the residual became predictable (was \
             {noisy_kappa})"
        );
    }
}
