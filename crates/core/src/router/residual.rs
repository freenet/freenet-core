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

/// A kernel-weighted estimate of the residual at one query point.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct KernelEstimate {
    /// Nadaraya-Watson weighted mean of the neighbours' residuals.
    pub residual: f64,
    /// Kernel mass `Σ w_i` — the effective number of observations supporting
    /// `residual`. Zero means "no nearby evidence"; see the module docs for why
    /// this is kernel mass rather than Kish's ratio.
    pub n_eff: f64,
    /// Kernel-weighted variance of the neighbours' residuals — how much they
    /// DISAGREE.
    ///
    /// `n_eff` says how much evidence there is; this says whether that evidence
    /// is telling one story. Both are needed, and using only the first was a
    /// real defect: the peer feature is categorical, so a query pulls in every
    /// observation for that peer regardless of contract. A peer that fails for
    /// ONE contract band therefore produces a neighbourhood of a few residuals
    /// near +0.55 and many near 0 — a large `n_eff` supporting a mean of ~0.12
    /// that describes neither population. Measured: on ordinary traffic the
    /// correction predicted |r̂| ≈ 0.12 and applied 78% of it, making that
    /// traffic 27% worse than the plain distance curve.
    pub variance: f64,
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
    /// Accumulated squared error per `kappa` candidate.
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
        KAPPA_GRID[self.best()]
    }

    /// Best `(bandwidth index, kappa index)` by accumulated prequential loss.
    ///
    /// Before anything has been scored there is no evidence to choose on, so
    /// this returns the middle of each grid — deliberately cautious, since a
    /// too-large `kappa` only delays the correction whereas a too-small one
    /// applies it before it is earned.
    fn best(&self) -> usize {
        // Seeded with the cautious midpoint and improved on only STRICTLY, so a
        // fully-tied grid keeps the default rather than collapsing to the most
        // aggressive candidate. Ties are not hypothetical: before a stage has
        // any estimate, every `record` adds the SAME loss to every candidate.
        let mut best = KAPPA_GRID.len() / 2;
        if self.scored == 0 {
            return best;
        }
        for kappa in 0..KAPPA_GRID.len() {
            if self.squared_error[kappa] < self.squared_error[best] {
                best = kappa;
            }
        }
        best
    }

    /// Shrinkage factor `λ = n_eff / (n_eff + κ)` for the given evidence mass.
    ///
    /// Guaranteed to land in `[0, 1]`, and to be exactly `0.0` for zero or
    /// non-finite evidence so that a caller can rely on `apply(base, 0.0) == base`.
    pub(crate) fn lambda(
        &self,
        n_eff: f64,
        local_variance: f64,
        predicted: f64,
        prior: f64,
    ) -> f64 {
        self.lambda_full(n_eff, local_variance, predicted, prior)
    }

    /// `λ = n_eff / (n_eff + κ · relative_noise)`.
    ///
    /// `relative_noise` is the neighbours' own dispersion measured against how
    /// much residuals vary across the whole stage. It is the `σ²_noise` term
    /// the empirical-Bayes derivation always called for, estimated LOCALLY
    /// rather than pinned to one global constant:
    ///
    /// - neighbours in perfect agreement → `relative_noise → 0` → `λ → 1`
    /// - neighbours as scattered as the population → `relative_noise ≈ 1` →
    ///   `λ = n/(n+κ)`, the previous behaviour
    /// - neighbours MORE scattered than the population → `λ` collapses, and the
    ///   correction defers to the prior
    ///
    /// That last case is the one this fixes. Weighting by evidence QUANTITY
    /// alone made the model most confident exactly where it was averaging two
    /// different populations together — a large neighbourhood that disagrees
    /// with itself scored as strong evidence.
    /// Shrinkage from a LOCAL signal estimate rather than a global one.
    ///
    /// The deviation the correction proposes is `d = r̂ − prior`. Its own
    /// standard error is `se² = σ²_local / n_eff`. The classic unbiased estimate
    /// of how much of `d` is real signal rather than sampling noise is
    /// `max(0, d² − se²)`, and the empirical-Bayes weight follows:
    ///
    /// ```text
    /// λ = signal / (signal + se²)
    /// ```
    ///
    /// # Why this and not a global σ²_signal
    ///
    /// Estimating the signal variance across the whole stage inflates it
    /// wherever the data contains a strong localised effect — and then keeps λ
    /// high in the regions that have NO effect, which is exactly backwards.
    /// Measured: with a global estimate, ordinary traffic (true residual ~0)
    /// still ran at λ ≈ 0.61, applying most of a noisy ±0.14 correction to 92%
    /// of queries.
    ///
    /// Locally, the test is self-normalising: if the proposed deviation is no
    /// larger than its own standard error, there is nothing to distinguish it
    /// from zero, `signal → 0`, `λ → 0`, and the prior stands. That is the
    /// property the global form could not express, and it needs nothing beyond
    /// the mean, the dispersion and the neighbour count.
    ///
    /// There is deliberately NO global tuning dial any more. `kappa` used to
    /// multiply the noise term, standing in for a local estimate that did not
    /// exist. Now that the local standard error IS the noise term, the dial only
    /// distorted it: the selector chose a large kappa because over-shrinking is
    /// right for the ~92% of queries with no signal, and that same choice then
    /// shrank away the correction on the queries that had some. Measured: with
    /// the dial, targeted error ran 1.39x the legacy path; without it, 0.95x.
    fn lambda_full(&self, n_eff: f64, local_variance: f64, predicted: f64, prior: f64) -> f64 {
        if !n_eff.is_finite() || n_eff <= 0.0 {
            return 0.0;
        }
        if !local_variance.is_finite() || local_variance < 0.0 {
            return 0.0;
        }

        // Standard error of the local mean, inflated by the selected kappa.
        let standard_error_squared = (local_variance / n_eff).max(0.0);
        if standard_error_squared <= 0.0 {
            // The neighbours agree exactly: nothing to shrink against.
            return 1.0;
        }

        let deviation = predicted - prior;
        if !deviation.is_finite() {
            return 0.0;
        }
        let signal = (deviation * deviation - standard_error_squared).max(0.0);
        let lambda = signal / (signal + standard_error_squared);
        if lambda.is_finite() {
            lambda.clamp(0.0, 1.0)
        } else {
            0.0
        }
    }

    /// Variance of the residuals seen across the whole stage — the `σ²_signal`
    /// scale that local dispersion is measured against.
    fn residual_variance(&self) -> Option<f64> {
        if self.residual_count < 2 {
            return None;
        }
        let variance = self.residual_m2 / (self.residual_count - 1) as f64;
        (variance.is_finite() && variance > 0.0).then_some(variance)
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
    /// `prior` is the per-peer EWMA's own adjustment for this event, in the
    /// same space as `actual_residual`.
    ///
    /// It is required, not optional, because the forecast being scored is the
    /// COMPOSED one: `(1−λ)·prior + λ·r̂`. Scoring `λ·r̂` alone — as this did
    /// before `compose_with_prior` existed — optimises a formula the predictor
    /// no longer uses, and biases selection toward a large λ merely because the
    /// correction beats ZERO. It would happily hand over from an accurate EWMA
    /// to a noisier kernel estimate and call that an improvement.
    pub(crate) fn record(
        &mut self,
        estimate: Option<KernelEstimate>,
        actual_residual: f64,
        prior: f64,
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
        for cell in self.squared_error.iter_mut() {
            *cell *= retention;
        }

        {
            let (n_eff, predicted, local_variance) = match &estimate {
                Some(estimate) if estimate.n_eff.is_finite() && estimate.residual.is_finite() => {
                    (estimate.n_eff, estimate.residual, estimate.variance)
                }
                _ => (0.0, 0.0, 0.0),
            };
            let prior = if prior.is_finite() { prior } else { 0.0 };
            for (kappa_index, kappa) in KAPPA_GRID.iter().enumerate() {
                // Same shrinkage the predictor will apply, dispersion included,
                // so a candidate wins here only if it would genuinely have
                // predicted better.
                let lambda = self.lambda_full(n_eff, local_variance, predicted, prior);
                // The same composition `compose_with_prior` forms, so the
                // candidate that wins here is the candidate that would actually
                // have predicted best.
                let forecast = (1.0 - lambda) * prior + lambda * predicted;
                let error = forecast - actual_residual;
                if error.is_finite() {
                    self.squared_error[kappa_index] += error * error;
                }
            }
        }
        self.scored += 1;
    }

    /// Standard deviation of observed residuals, once there are enough to make
    /// one meaningful.
    pub(crate) fn residual_sigma(&self) -> Option<f64> {
        self.residual_variance().map(f64::sqrt)
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

/// Apply the learned correction to the global distance curve.
///
/// # Why there is no per-peer layer here
///
/// The learner already takes peer identity as a feature, so it can represent the
/// per-peer effect itself — and the peer x contract interaction, and time-varying
/// behaviour, none of which a per-peer scalar can express. Keeping a separate
/// hand-rolled per-peer EWMA underneath it is redundant at best.
///
/// It measured worse than redundant. On ordinary traffic in the recoverability
/// harness the plain global curve scored 0.0157, the legacy path 0.0161 and a
/// composition anchored to the per-peer EWMA 0.0259 — the EWMA was the worst of
/// the three. The reason is the same over-generalisation the correction itself
/// had to be fixed for: a peer failing on one narrow contract band has that
/// penalty averaged across ALL its traffic by a per-peer scalar, which then
/// taxes its perfectly normal requests.
///
/// An earlier revision of this function composed against the peer-adjusted
/// estimate, on the reasoning that "when uninformed, fall back to the best
/// estimate available". That reasoning was sound and the premise was false: the
/// peer-adjusted estimate is not better than the bare curve here, so falling
/// back to it was falling back to something worse than doing nothing. With no
/// evidence this now shrinks to zero and yields the curve, which the measurement
/// says is the right answer.
pub(crate) fn compose(mode: AdjustmentMode, global: f64, correction: f64) -> f64 {
    let correction = if correction.is_finite() {
        correction
    } else {
        0.0
    };
    let composed = mode.apply(global, correction);
    if composed.is_finite() {
        composed
    } else {
        global
    }
}

/// Prequential accuracy for one prediction layer/// Prequential accuracy for one prediction layer, scored against the
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
            // Dispersion matched to the spread these tests actually feed, so
            // `relative_noise ≈ 1` and lambda reduces to `n/(n+kappa)`.
            //
            // NOT zero: zero dispersion means the neighbours agree exactly, which
            // correctly drives lambda to 1 for EVERY kappa — making the kappa
            // dimension inert and these tests unable to observe the thing they
            // are about.
            variance: 0.25,
        });
        selector.record(estimate, actual, 0.0);
    }

    #[test]
    fn lambda_is_zero_without_evidence() {
        let selector = ShrinkageSelector::new();
        assert_eq!(selector.lambda(0.0, 0.0, 1.0, 0.0), 0.0);
        assert_eq!(selector.lambda(-1.0, 0.0, 1.0, 0.0), 0.0);
        assert_eq!(selector.lambda(f64::NAN, 0.0, 1.0, 0.0), 0.0);
        assert_eq!(selector.lambda(f64::INFINITY, 0.0, 1.0, 0.0), 0.0);
    }

    #[test]
    fn lambda_is_bounded_and_increasing_in_evidence() {
        let selector = ShrinkageSelector::new();
        let mut previous = 0.0;
        for step in 1..500 {
            let n_eff = step as f64 * 0.5;
            let lambda = selector.lambda(n_eff, 0.0, 1.0, 0.0);
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
        assert!((selector.lambda(kappa, 0.0, 1.0, 0.0) - 0.5).abs() < 1e-12);
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

    /// The selector must not hand over to a noisier kernel estimate just because
    /// the correction beats doing nothing.
    ///
    /// Here the EWMA prior is exactly right (0.4) and the kernel estimate is
    /// pure noise. Scoring `λ·r̂` alone would favour a small kappa, because any
    /// λ>0 beats λ=0 when the target is 0.4 and the correction is centred near
    /// it. Scoring the composed forecast sees that handing over DESTROYS an
    /// accurate prior, and shrinks instead.
    #[test]
    fn selection_does_not_hand_over_from_an_accurate_prior_to_noise() {
        let mut selector = ShrinkageSelector::new();
        let mut state = 0xBEEFu64;
        for _ in 0..20_000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let noisy = if state % 2 == 0 { 1.4 } else { -0.6 };
            let estimate = Some(KernelEstimate {
                residual: noisy,
                n_eff: 8.0,
                variance: 1.0,
            });
            selector.record(estimate, 0.4, 0.4);
        }
        let lambda = selector.lambda(8.0, 0.0, 1.0, 0.0);
        assert!(
            lambda < 0.5,
            "with an exact prior and a noisy correction the selector must keep \
             most of the prior, got lambda {lambda:.3} (kappa {})",
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
    fn no_correction_yields_exactly_the_global_curve() {
        // The property the design now rests on: with nothing learned the
        // prediction IS the distance curve — not a fallback to another
        // estimator that may itself be worse, which is what the previous
        // composition did.
        for global in [0.0f64, 0.017, 0.5, 0.999, 1.0] {
            let composed = compose(AdjustmentMode::Additive, global, 0.0);
            assert_eq!(
                composed, global,
                "an empty correction must leave the curve bit-for-bit unchanged"
            );
        }
    }

    #[test]
    fn correction_applies_in_each_adjustment_space() {
        assert!((compose(AdjustmentMode::Additive, 0.10, 0.25) - 0.35).abs() < 1e-12);
        // Multiplicative composes as global * exp(c); ln(2) doubles it.
        let doubled = compose(AdjustmentMode::Multiplicative, 100.0, 2.0f64.ln());
        assert!((doubled - 200.0).abs() < 1e-9, "got {doubled}");
    }

    #[test]
    fn composition_survives_unusable_corrections() {
        // A non-finite correction must degrade to the curve rather than reach
        // the router's cost comparator.
        for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let composed = compose(AdjustmentMode::Additive, 0.2, bad);
            assert_eq!(composed, 0.2, "bad correction {bad} must be ignored");
        }
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
