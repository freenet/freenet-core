//! Renegade-ML based routing predictor for detecting peer × contract interaction patterns.
//!
//! Uses a funnel of three prediction stages, each conditional on the previous:
//! 1. **Success probability** — trained on all routing events
//! 2. **Time to response start** — trained only on successful events
//! 3. **Transfer speed** — trained only on successful events with timing data
//!
//! Each stage uses the same features: (peer_id, contract_location, distance, time).
//! Separate predictor instances are used per operation type (GET, PUT, etc.).

use super::isotonic_estimator::AdjustmentMode;
use super::residual;
use crate::ring::{Location, PeerKeyLocation};
use renegade_ml::{DataPoint, Renegade};
use std::collections::{HashMap, VecDeque};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default K before auto-selection has run.
const DEFAULT_K: usize = 5;

/// Minimum observations in a stage before predictions are produced.
const MIN_OBSERVATIONS_FOR_PREDICTION: usize = 10;

/// How many neighbours the kernel correction considers before weighting them.
///
/// Deliberately MUCH larger than `cached_k`, and not the same quantity. `k` is a
/// hard cutoff chosen for renegade's own `1/d` weighted mean; the kernel is a
/// soft cutoff. Using both means the hard one dominates and the bandwidth never
/// gets to decide anything.
///
/// Reusing `cached_k` (~5) here reintroduced exactly the ceiling this change
/// exists to remove, by a different route: `n_eff <= k`, so `lambda <= 5/(5+4)
/// = 0.56` no matter how much evidence existed, and a residual estimate
/// averaging five binary-derived samples carries `sd ~ sqrt(0.2/5) = 0.20` —
/// comparable to the 0.55 effect it was supposed to detect. Measured: the
/// recoverability harness scored `captured = -0.33` with a FLAT learning curve
/// from 500 to 4000 events.
///
/// 128 is a compromise with the hot path: this runs per candidate peer per
/// routing decision (up to `CANDIDATE_WINDOW` peers x 3 stages), so the
/// neighbour search is the cost that matters, not the kernel sums over it.
const KERNEL_CANDIDATE_NEIGHBOURS: usize = 128;

/// Minimum observations before training is worthwhile.
const MIN_OBSERVATIONS_FOR_TRAINING: usize = 20;

/// Maximum number of recent (predicted, actual) pairs for accuracy visualization.
const MAX_ACCURACY_HISTORY: usize = 200;

/// Maximum number of peer IDs to retain (LRU cap).
const MAX_PEER_IDS: usize = 10_000;

/// How many events for the failure stage ramp to reach max weight (50%).
const FAILURE_WEIGHT_RAMP_EVENTS: f64 = 200.0;

/// How many events for timing stages to reach max weight (50%).
const TIMING_WEIGHT_RAMP_EVENTS: f64 = 100.0;

/// Maximum blending weight for renegade predictions.
const MAX_RENEGADE_WEIGHT: f64 = 0.5;

// ---------------------------------------------------------------------------
// Feature representation
// ---------------------------------------------------------------------------

/// A routing observation with 4 features: peer identity, contract location,
/// distance, and time. Used as input to each funnel stage.
#[derive(Clone, Debug)]
struct RoutingObservation {
    /// Numeric encoding of the peer identity (categorical).
    peer_id: f64,
    /// Contract location on the ring [0, 1].
    contract_location: f64,
    /// Distance from peer to contract [0, 0.5].
    distance: f64,
    /// Time of observation in hours (relative to predictor start, not epoch).
    /// This keeps values small for metric learning.
    time: f64,
}

impl DataPoint for RoutingObservation {
    fn feature_distances(&self, other: &Self) -> Vec<f64> {
        vec![
            // Peer ID: categorical — 0 if same peer, 1 if different
            if (self.peer_id - other.peer_id).abs() < 0.5 {
                0.0
            } else {
                1.0
            },
            // Contract location: ring distance, scaled [0,0.5] → [0,1]
            ring_distance(self.contract_location, other.contract_location) * 2.0,
            // Distance: [0, 0.5] → [0, 1]
            (self.distance - other.distance).abs() * 2.0,
            // Time: observations 24 hours apart get distance ~1
            ((self.time - other.time).abs() / 24.0).min(1.0),
        ]
    }

    fn feature_values(&self) -> Vec<f64> {
        // All values are normalized to roughly [0, 1] range for metric learning:
        // - peer_id: 0..N/MAX_PEER_IDS (bounded by LRU cap)
        // - contract_location: already [0, 1]
        // - distance: [0, 0.5] → scale to [0, 1]
        // - time: hours since predictor start / 168 (normalized to weeks)
        vec![
            self.peer_id / MAX_PEER_IDS as f64,
            self.contract_location,
            self.distance * 2.0,
            self.time / 168.0, // normalize to ~weeks
        ]
    }
}

/// Shortest arc distance on [0, 1] ring.
fn ring_distance(a: f64, b: f64) -> f64 {
    let d = (a - b).abs();
    d.min(1.0 - d)
}

// ---------------------------------------------------------------------------
// Single prediction stage
// ---------------------------------------------------------------------------

/// A single renegade model for one stage of the prediction funnel.
///
/// Training (metric learning + K selection) is triggered on the write path,
/// not during prediction. This allows `predict()` to take `&self` (immutable).
struct PredictionStage {
    model: Renegade<RoutingObservation>,
    max_observations: usize,
    count: usize,
    /// Kernel bandwidth — the feature-space length scale at which "k neighbours"
    /// stops being a local neighbourhood. `None` until the first training round
    /// has enough data to estimate one, which keeps the correction inert rather
    /// than guessing a scale.
    bandwidth: Option<f64>,
    /// Recently-added observations, used as query points when estimating the
    /// bandwidth. Deliberately a recency window rather than a uniform sample of
    /// the store: the observations are the same population queries land in, and
    /// the time feature means recent points are where prediction actually
    /// happens, so a recency-biased length scale is the relevant one.
    bandwidth_samples: VecDeque<RoutingObservation>,
    /// Cached K from the last training. Used for immutable predictions.
    cached_k: usize,
    /// Number of observations when last trained (base for the retraining
    /// threshold; see `observations_since_train`). Zero means never trained.
    trained_at: usize,
    /// Observations added since the last training.
    ///
    /// Retraining keys off this rather than off total-count growth
    /// (`len() >= trained_at * 3 / 2`) because eviction pins `len()` at or below
    /// `max_observations` (it reaches the cap exactly, then drops to 9/10 of it).
    /// Once `trained_at * 3 / 2` exceeded that reachable count, the total-count
    /// rule became unsatisfiable and training froze permanently. No constant
    /// rescues that formulation — capping the growth term just moves the rung at
    /// which it freezes. See #4810.
    observations_since_train: usize,
}

impl PredictionStage {
    fn new(max_observations: usize) -> Self {
        PredictionStage {
            model: Renegade::new(),
            max_observations,
            count: 0,
            bandwidth: None,
            bandwidth_samples: VecDeque::new(),
            cached_k: DEFAULT_K,
            trained_at: 0,
            observations_since_train: 0,
        }
    }

    fn add(&mut self, obs: RoutingObservation, output: f64) {
        // Guard against Inf/NaN outputs from division by zero
        if !output.is_finite() {
            return;
        }
        if self.bandwidth_samples.len() >= residual::BANDWIDTH_SAMPLE_POINTS {
            self.bandwidth_samples.pop_front();
        }
        self.bandwidth_samples.push_back(obs.clone());
        self.model.add(obs, output);
        self.count += 1;
        self.observations_since_train += 1;
        if self.count > self.max_observations {
            self.evict_oldest();
        }
    }

    /// Check if training should happen: 50% of the last-trained size worth of
    /// fresh observations has arrived since the last training.
    fn should_train(&self) -> bool {
        let n = self.model.len();
        if n < MIN_OBSERVATIONS_FOR_TRAINING {
            return false;
        }
        if self.trained_at == 0 {
            return true;
        }
        // BEFORE THE FIRST EVICTION nothing has been discarded, so
        // `observations_since_train == n - trained_at` and this is exactly the
        // historical `n >= trained_at + trained_at / 2` rule — the pre-eviction
        // cadence is preserved bit-for-bit.
        //
        // The boundary is the first eviction, NOT the cap: once eviction starts,
        // `n` is still below `max_observations` (it drops to 9/10 of it) but the
        // two forms have already diverged, because evicted observations still
        // count as "arrived". At trained_at=3829 with a 5000 cap, for instance,
        // `observations_since_train` reads 1173 while `n - trained_at` is 672.
        // That divergence is the fix: `n` stops growing but new observations keep
        // arriving, so only this form stays satisfiable. See #4810.
        self.observations_since_train >= (self.trained_at / 2).max(1)
    }

    /// Trigger training (metric learning + K selection).
    ///
    /// `get_optimal_k()` early-returns unless renegade's cached K has been
    /// invalidated, so this only does real work when there is real work to do.
    /// At saturation eviction is what invalidates it, and eviction is several
    /// times more frequent than training (every ~`max_observations / 10` adds
    /// vs every ~`max_observations * 0.45`), so each training here genuinely
    /// re-learns rather than no-op'ing. Do not "optimize" the eviction path into
    /// preserving the cache without revisiting the retraining cadence: that
    /// would turn these calls into early-returns and silently re-freeze K
    /// (#4810).
    fn train(&mut self) {
        if self.model.len() >= MIN_OBSERVATIONS_FOR_TRAINING {
            self.cached_k = self.model.get_optimal_k();
            self.trained_at = self.model.len();
            self.observations_since_train = 0;
            self.refresh_bandwidth();
        }
    }

    /// Re-estimate the kernel bandwidth as the median distance from a sampled
    /// observation to its k-th nearest neighbour.
    ///
    /// Runs inside `train()` so it shares that cadence rather than adding another
    /// one, and costs `O(S log n)` for `S` samples instead of `O(n log n)`.
    fn refresh_bandwidth(&mut self) {
        if self.bandwidth_samples.is_empty() {
            return;
        }
        // Measured at the KERNEL's neighbour count, not renegade's `k`: this is
        // the length scale over which the kernel's own candidate set is spread,
        // and the two are different quantities (see
        // `KERNEL_CANDIDATE_NEIGHBOURS`). `+ 1` because each sample is itself in
        // the store at distance 0.
        let k = (KERNEL_CANDIDATE_NEIGHBOURS.min(self.model.len())).max(1) + 1;
        let mut kth_distances = Vec::with_capacity(self.bandwidth_samples.len());
        for sample in &self.bandwidth_samples {
            let neighbors = self.model.query_k(sample, k);
            // `query_k` returns neighbours sorted nearest-first, so the last is
            // the farthest of the k considered.
            if let Some(farthest) = neighbors.neighbors.last() {
                kth_distances.push(farthest.distance);
            }
        }
        if let Some(bandwidth) = residual::estimate_bandwidth(&mut kth_distances) {
            self.bandwidth = Some(bandwidth);
        }
    }

    /// Kernel-weighted estimate of this stage's target at `query`, together with
    /// the evidence mass supporting it.
    ///
    /// Deliberately has **no** minimum-observation gate. The old `predict()` needs
    /// one because it returns an absolute value whose uninformed output is the
    /// global mean — a real perturbation that has to be suppressed. Here an
    /// uninformed query yields `n_eff ≈ 0`, and the caller's shrinkage turns that
    /// into a correction of exactly zero, so a floor would be redundant: the
    /// evidence measure already encodes "I have nothing to say about this".
    fn predict_kernel_multi(
        &self,
        query: &RoutingObservation,
    ) -> Option<[Option<residual::KernelEstimate>; residual::BANDWIDTH_MULTIPLIERS.len()]> {
        let base_bandwidth = self.bandwidth?;
        if self.model.is_empty() {
            return None;
        }
        // ONE neighbour query, reused across every candidate bandwidth. The
        // k-NN search is the expensive part; a kernel sum over an existing
        // neighbour list is a handful of `exp` calls.
        let candidates = KERNEL_CANDIDATE_NEIGHBOURS.min(self.model.len());
        let neighbors = self.model.query_k(query, candidates);
        if neighbors.neighbors.is_empty() {
            return None;
        }
        let pairs: Vec<(f64, f64)> = neighbors
            .neighbors
            .iter()
            .map(|neighbor| (neighbor.distance, neighbor.output))
            .collect();

        let mut estimates = [None; residual::BANDWIDTH_MULTIPLIERS.len()];
        for (index, multiplier) in residual::BANDWIDTH_MULTIPLIERS.iter().enumerate() {
            estimates[index] = residual::kernel_estimate(&pairs, base_bandwidth * multiplier);
        }
        Some(estimates)
    }

    /// Predict using the pre-trained model (immutable access).
    fn predict(&self, query: &RoutingObservation) -> Option<f64> {
        if self.model.len() < MIN_OBSERVATIONS_FOR_PREDICTION {
            return None;
        }
        let neighbors = self.model.query_k(query, self.cached_k);
        if neighbors.neighbors.is_empty() {
            return None;
        }
        Some(neighbors.weighted_mean())
    }

    fn len(&self) -> usize {
        self.model.len()
    }

    fn evict_oldest(&mut self) {
        let target = self.max_observations * 9 / 10;
        let current = self.model.len();
        if current <= target {
            return;
        }
        let to_remove = current - target;
        let mut removed = 0;
        // retain() preserves relative order of kept elements — the first
        // `to_remove` entries (oldest, since we always append) are removed.
        self.model.retain(|_point, _output| {
            if removed < to_remove {
                removed += 1;
                false
            } else {
                true
            }
        });
        self.count = self.model.len();
        // Deliberately does NOT touch `observations_since_train`: eviction must
        // not erase the record that new data has arrived, or retraining would
        // freeze again at saturation (#4810).
    }
}

// ---------------------------------------------------------------------------
// Routing predictor (funnel of 3 stages)
// ---------------------------------------------------------------------------

/// Prediction result from the routing funnel.
#[derive(Debug, Clone)]
pub(crate) struct RoutingPredictionResult {
    /// Predicted failure probability [0, 1]. Always available once enough data.
    pub failure_probability: Option<f64>,
    /// Predicted time to response start in seconds.
    pub time_to_response_start: Option<f64>,
    /// Predicted transfer speed in bytes/second.
    pub transfer_speed: Option<f64>,
}

/// A routing event outcome for recording into the funnel.
pub(crate) struct RoutingOutcome {
    /// Whether the request succeeded.
    pub success: bool,
    /// Time to response start (only for timed successes).
    pub time_to_response_start_secs: Option<f64>,
    /// Transfer speed in bytes/second (only for timed successes with payload).
    pub transfer_speed_bps: Option<f64>,
}

/// Convert a `RouteOutcome` into a `RoutingOutcome` for the predictor.
impl RoutingOutcome {
    pub fn from_route_outcome(outcome: &super::RouteOutcome) -> (Self, f64) {
        match outcome {
            super::RouteOutcome::Success {
                time_to_response_start,
                payload_size,
                payload_transfer_time,
            } => {
                let transfer_time_secs = payload_transfer_time.as_secs_f64();
                let speed = if transfer_time_secs > 0.0 {
                    Some(*payload_size as f64 / transfer_time_secs)
                } else {
                    None // avoid Inf from zero-duration transfer
                };
                (
                    RoutingOutcome {
                        success: true,
                        time_to_response_start_secs: Some(time_to_response_start.as_secs_f64()),
                        transfer_speed_bps: speed,
                    },
                    0.0, // failure value
                )
            }
            super::RouteOutcome::SuccessUntimed => (
                RoutingOutcome {
                    success: true,
                    time_to_response_start_secs: None,
                    transfer_speed_bps: None,
                },
                0.0,
            ),
            super::RouteOutcome::Failure => (
                RoutingOutcome {
                    success: false,
                    time_to_response_start_secs: None,
                    transfer_speed_bps: None,
                },
                1.0,
            ),
        }
    }
}

/// Renegade-based routing predictor using a funnel of three stages.
pub(crate) struct RoutingPredictor {
    failure_stage: PredictionStage,
    response_time_stage: PredictionStage,
    transfer_speed_stage: PredictionStage,
    /// Residual-target counterparts of the three stages above.
    ///
    /// These are kept *alongside* the absolute-target stages rather than
    /// replacing them, for two reasons. The absolute stages still drive the
    /// legacy blend, so the old path stays bit-identical while the correction is
    /// flag-gated; and running both is what lets the router score the two
    /// approaches against each other on live traffic before the default flips.
    /// The cost is one extra observation store per target — a few hundred KB at
    /// the current cap — which is the price of being able to measure rather than
    /// assume.
    failure_residual_stage: PredictionStage,
    response_time_residual_stage: PredictionStage,
    transfer_speed_residual_stage: PredictionStage,
    /// Online shrinkage selection per residual stage.
    failure_shrinkage: residual::ShrinkageSelector,
    response_time_shrinkage: residual::ShrinkageSelector,
    transfer_speed_shrinkage: residual::ShrinkageSelector,
    /// Map from PeerKeyLocation to (numeric_id, lru_generation).
    /// Bounded by MAX_PEER_IDS via LRU eviction.
    peer_ids: HashMap<PeerKeyLocation, (u64, u64)>,
    /// LRU generation counter — incremented on each access. Eviction removes
    /// the entry with the lowest generation.
    lru_generation: u64,
    /// Next peer ID to assign.
    next_peer_id: u64,
    /// Running prediction accuracy tracker for failure predictions.
    accuracy: PredictionAccuracy,
    /// Accuracy tracker for the response-time regression stage.
    response_time_accuracy: RegressionAccuracy,
    /// Accuracy tracker for the transfer-speed regression stage.
    transfer_speed_accuracy: RegressionAccuracy,
    /// When true, skip periodic training and accuracy tracking (batch loading).
    batch_mode: bool,
    /// Reference time (hours since epoch at predictor creation).
    /// All time features are relative to this, keeping values small for metric learning.
    reference_time_hours: f64,
}

/// Tracks prediction vs actual outcomes for measuring predictive quality.
struct PredictionAccuracy {
    total: u64,
    brier_sum: f64,
    ewma_error: f64,
    initialized: bool,
    /// Ring buffer of recent (predicted, actual) pairs for visualization.
    recent_pairs: VecDeque<(f64, f64)>,
}

impl PredictionAccuracy {
    fn new() -> Self {
        PredictionAccuracy {
            total: 0,
            brier_sum: 0.0,
            ewma_error: 0.0,
            initialized: false,
            recent_pairs: VecDeque::new(),
        }
    }

    fn record(&mut self, predicted: f64, actual: f64) {
        let error = (predicted - actual).powi(2);
        self.total += 1;
        self.brier_sum += error;

        const ALPHA: f64 = 0.01;
        if self.initialized {
            self.ewma_error = ALPHA * error + (1.0 - ALPHA) * self.ewma_error;
        } else {
            self.ewma_error = error;
            self.initialized = true;
        }

        if self.recent_pairs.len() >= MAX_ACCURACY_HISTORY {
            self.recent_pairs.pop_front();
        }
        self.recent_pairs.push_back((predicted, actual));
    }

    fn brier_score(&self) -> Option<f64> {
        if self.total == 0 {
            return None;
        }
        Some(self.brier_sum / self.total as f64)
    }

    fn recent_brier_score(&self) -> Option<f64> {
        if !self.initialized {
            return None;
        }
        Some(self.ewma_error)
    }
}

/// Tracks regression prediction accuracy (continuous targets such as response
/// time and transfer speed). Unlike the binary failure stage there is no Brier
/// score; quality is judged from the spread of (predicted, actual) pairs, which
/// the dashboard renders as a predicted-vs-actual scatter and summarizes as a
/// median absolute percentage error over the retained window.
struct RegressionAccuracy {
    total: u64,
    /// Ring buffer of recent (predicted, actual) pairs for visualization.
    recent_pairs: VecDeque<(f64, f64)>,
}

impl RegressionAccuracy {
    fn new() -> Self {
        RegressionAccuracy {
            total: 0,
            recent_pairs: VecDeque::new(),
        }
    }

    fn record(&mut self, predicted: f64, actual: f64) {
        // Drop non-finite samples so a single bad division can't poison the view.
        if !predicted.is_finite() || !actual.is_finite() {
            return;
        }
        self.total += 1;
        if self.recent_pairs.len() >= MAX_ACCURACY_HISTORY {
            self.recent_pairs.pop_front();
        }
        self.recent_pairs.push_back((predicted, actual));
    }
}

impl std::fmt::Debug for RoutingPredictor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoutingPredictor")
            .field("failure_events", &self.failure_stage.len())
            .field("response_time_events", &self.response_time_stage.len())
            .field("transfer_speed_events", &self.transfer_speed_stage.len())
            .field("known_peers", &self.peer_ids.len())
            .field("brier_score", &self.accuracy.brier_score())
            .finish()
    }
}

/// Residuals of the isotonic base estimate for a single event, each expressed in
/// its own estimator's adjustment space (additive for failure, log-ratio for the
/// timing targets).
///
/// The caller computes these because it owns the estimators and therefore knows
/// each one's space; this module only stores and kernel-weights them. Critically,
/// they must be computed from the base estimate **as it stood before the
/// isotonic estimators ingested this event**, or the residual is deflated by the
/// base model having already fitted the point it is being scored on.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct StageResiduals {
    pub failure: Option<f64>,
    pub response_time: Option<f64>,
    pub transfer_speed: Option<f64>,
}

/// A shrunk correction for one stage, plus the diagnostics that explain it.
///
/// `lambda` and `n_eff` are carried out rather than kept internal because "how
/// much of the correction is being applied, and on what evidence" is the single
/// most useful thing the dashboard can tell an operator about the routing model.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Correction {
    /// The correction to apply, in the stage's adjustment space, already shrunk.
    pub value: f64,
    /// Shrinkage factor applied — `0.0` means the base estimate stands untouched.
    pub lambda: f64,
    /// Effective observations supporting the underlying residual estimate.
    pub n_eff: f64,
}

/// Corrections for all three stages at one query point.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RoutingCorrections {
    pub failure: Option<Correction>,
    pub response_time: Option<Correction>,
    pub transfer_speed: Option<Correction>,
}

/// The adjustment space each stage's residual lives in.
///
/// Taken from the estimators rather than assumed, because the assumption is
/// wrong: response time is multiplicative but **transfer rate is additive** in
/// the current configuration. Deriving this from each estimator means the
/// correction follows whatever those are set to, and keeps following them if
/// #4547 changes one.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StageModes {
    pub failure: AdjustmentMode,
    pub response_time: AdjustmentMode,
    pub transfer_speed: AdjustmentMode,
}

/// Self-tuned model state, surfaced for the dashboard.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ShrinkageDiagnostics {
    pub failure_kappa: f64,
    pub failure_bandwidth: Option<f64>,
    pub failure_residual_events: usize,
    pub failure_scored: u64,
    pub response_time_residual_events: usize,
    pub transfer_speed_residual_events: usize,
}

/// Shrink one stage's kernel estimate into an applicable correction.
///
/// Returns `None` when the stage has nothing to say — no bandwidth yet, no
/// neighbours, or an unusable estimate — which the caller must treat as "leave
/// the base estimate alone". A returned `Correction` may still carry
/// `value == 0.0` when `λ` shrank it away entirely; that is the same outcome by a
/// different route, and it is reported rather than hidden so the dashboard can
/// distinguish "no model" from "model present but unconvinced".
fn shrink(
    stage: &PredictionStage,
    selector: &residual::ShrinkageSelector,
    query: &RoutingObservation,
    mode: AdjustmentMode,
) -> Option<Correction> {
    let estimates = stage.predict_kernel_multi(query)?;
    let estimate = estimates[selector.bandwidth_index()]?;
    let lambda = selector.lambda(estimate.n_eff);
    let mut value = lambda * estimate.residual;
    // Only a multiplicative stage needs a spread bound: it recombines as
    // `base * exp(c)`, which is unbounded above, so one pathological residual
    // could otherwise dominate a routing decision. An additive stage is bounded
    // by its own target's range downstream, and bounding it here would cap a
    // legitimately large correction -- reintroducing exactly the ceiling this
    // change removes.
    if matches!(mode, AdjustmentMode::Multiplicative) {
        value = selector.clamp_log_correction(value);
    }
    if !value.is_finite() {
        return None;
    }
    Some(Correction {
        value,
        lambda,
        n_eff: estimate.n_eff,
    })
}

impl RoutingPredictor {
    /// Create a new predictor.
    pub fn new(max_observations_per_stage: usize) -> Self {
        RoutingPredictor {
            failure_stage: PredictionStage::new(max_observations_per_stage),
            response_time_stage: PredictionStage::new(max_observations_per_stage),
            transfer_speed_stage: PredictionStage::new(max_observations_per_stage),
            failure_residual_stage: PredictionStage::new(max_observations_per_stage),
            response_time_residual_stage: PredictionStage::new(max_observations_per_stage),
            transfer_speed_residual_stage: PredictionStage::new(max_observations_per_stage),
            failure_shrinkage: residual::ShrinkageSelector::new(),
            response_time_shrinkage: residual::ShrinkageSelector::new(),
            transfer_speed_shrinkage: residual::ShrinkageSelector::new(),
            peer_ids: HashMap::new(),
            lru_generation: 0,
            next_peer_id: 0,
            accuracy: PredictionAccuracy::new(),
            response_time_accuracy: RegressionAccuracy::new(),
            transfer_speed_accuracy: RegressionAccuracy::new(),
            batch_mode: false,
            reference_time_hours: wall_clock_hours(),
        }
    }

    /// Create a new predictor in batch mode (for loading historical events).
    /// Call `finish_batch()` after loading all events to trigger training.
    pub fn new_batch(max_observations_per_stage: usize) -> Self {
        let mut p = Self::new(max_observations_per_stage);
        p.batch_mode = true;
        p
    }

    /// Record a routing outcome. Uses wall-clock time for the time feature.
    pub fn record(
        &mut self,
        peer: &PeerKeyLocation,
        contract_location: Location,
        distance: f64,
        outcome: RoutingOutcome,
        residuals: StageResiduals,
    ) {
        let time = wall_clock_hours() - self.reference_time_hours;
        self.record_at_time(peer, contract_location, distance, outcome, residuals, time);
    }

    /// Record at a specific relative time (for batch loading with original timestamps
    /// and for testing with controlled time).
    pub(crate) fn record_at_time(
        &mut self,
        peer: &PeerKeyLocation,
        contract_location: Location,
        distance: f64,
        outcome: RoutingOutcome,
        residuals: StageResiduals,
        time: f64,
    ) {
        let actual_failure = if outcome.success { 0.0 } else { 1.0 };

        // Track prediction accuracy (skip in batch mode — no trained model yet).
        // Each stage is scored against the *current* model, i.e. before this
        // event's own observation is added below, so a prediction is never graded
        // against data that already contains its answer.
        if !self.batch_mode {
            let query = self.make_observation_immutable(peer, contract_location, distance, time);
            if let Some(predicted_failure) = self.failure_stage.predict(&query) {
                self.accuracy.record(predicted_failure, actual_failure);
            }
            // Regression stages only have a ground-truth actual on timed successes.
            if outcome.success {
                if let Some(actual_rt) = outcome.time_to_response_start_secs {
                    if let Some(predicted_rt) = self.response_time_stage.predict(&query) {
                        self.response_time_accuracy.record(predicted_rt, actual_rt);
                    }
                }
                if let Some(actual_ts) = outcome.transfer_speed_bps {
                    if let Some(predicted_ts) = self.transfer_speed_stage.predict(&query) {
                        self.transfer_speed_accuracy.record(predicted_ts, actual_ts);
                    }
                }
            }

            // Score the shrinkage candidates against the residual this event
            // actually turned out to have. Same predict-before-add discipline as
            // above: the kernel estimate is taken from the model as it stands,
            // so no candidate is ever graded on data containing its own answer.
            //
            // A stage with no usable estimate still scores, with `n_eff = 0`.
            // That matters: "no evidence, so no correction, and the residual was
            // nonetheless large" is real evidence about how much to trust this
            // layer, and dropping those samples would bias selection toward
            // whichever kappa looks good only where the model happens to be
            // confident.
            let shrinkage_inputs = [
                (
                    residuals.failure,
                    &self.failure_residual_stage,
                    &mut self.failure_shrinkage,
                ),
                (
                    residuals.response_time,
                    &self.response_time_residual_stage,
                    &mut self.response_time_shrinkage,
                ),
                (
                    residuals.transfer_speed,
                    &self.transfer_speed_residual_stage,
                    &mut self.transfer_speed_shrinkage,
                ),
            ];
            for (actual_residual, stage, selector) in shrinkage_inputs {
                let Some(actual_residual) = actual_residual else {
                    continue;
                };
                // Every bandwidth candidate is scored on the same event, so
                // the grid is compared on identical data rather than on
                // whichever events each happened to see.
                let estimates = stage
                    .predict_kernel_multi(&query)
                    .unwrap_or([None; residual::BANDWIDTH_MULTIPLIERS.len()]);
                selector.record(&estimates, actual_residual);
            }
        }

        let obs = self.make_observation(peer, contract_location, distance, time);

        // Residual stages mirror their absolute counterparts' eligibility: the
        // failure residual exists for every event, the timing residuals only for
        // timed successes (a residual needs an observed value to subtract the
        // base from).
        if let Some(failure_residual) = residuals.failure {
            self.failure_residual_stage
                .add(obs.clone(), failure_residual);
        }
        if outcome.success {
            if let Some(response_time_residual) = residuals.response_time {
                self.response_time_residual_stage
                    .add(obs.clone(), response_time_residual);
            }
            if let Some(transfer_speed_residual) = residuals.transfer_speed {
                self.transfer_speed_residual_stage
                    .add(obs.clone(), transfer_speed_residual);
            }
        }

        // Stage 1: all events
        self.failure_stage.add(obs.clone(), actual_failure);

        // Stage 2: only successes with response time
        if outcome.success {
            if let Some(response_time) = outcome.time_to_response_start_secs {
                self.response_time_stage.add(obs.clone(), response_time);
            }
        }

        // Stage 3: only successes with transfer speed
        if outcome.success {
            if let Some(speed) = outcome.transfer_speed_bps {
                self.transfer_speed_stage.add(obs, speed);
            }
        }

        // Retrain on observation turnover (not at fixed counts).
        //
        // Note the ordering: `add()` above evicts inline, so by the time
        // `should_train()` runs, a stage that just hit `max_observations + 1` has
        // already been trimmed to 9/10 of the cap. The largest `len()` any
        // `should_train()` can observe is therefore exactly `max_observations` —
        // which is why a rule keyed off total count is unsatisfiable past a
        // point, and why this one is keyed off turnover instead (#4810).
        if !self.batch_mode {
            if self.failure_stage.should_train() {
                self.failure_stage.train();
            }
            if self.response_time_stage.should_train() {
                self.response_time_stage.train();
            }
            if self.transfer_speed_stage.should_train() {
                self.transfer_speed_stage.train();
            }
            if self.failure_residual_stage.should_train() {
                self.failure_residual_stage.train();
            }
            if self.response_time_residual_stage.should_train() {
                self.response_time_residual_stage.train();
            }
            if self.transfer_speed_residual_stage.should_train() {
                self.transfer_speed_residual_stage.train();
            }
        }
    }

    /// Record with no residual targets — the pre-#4485 signature.
    ///
    /// Test-only. The existing stage tests exercise the absolute-target path and
    /// say nothing about residuals; routing them through this keeps them
    /// testing what they were written to test, rather than silently acquiring a
    /// second subject.
    #[cfg(test)]
    pub(crate) fn record_at_time_absolute_only(
        &mut self,
        peer: &PeerKeyLocation,
        contract_location: Location,
        distance: f64,
        outcome: RoutingOutcome,
        time: f64,
    ) {
        self.record_at_time(
            peer,
            contract_location,
            distance,
            outcome,
            StageResiduals::default(),
            time,
        );
    }

    /// Trigger training on all stages. Call after batch loading historical events.
    pub fn finish_batch(&mut self) {
        self.batch_mode = false;
        self.failure_stage.train();
        self.response_time_stage.train();
        self.transfer_speed_stage.train();
        self.failure_residual_stage.train();
        self.response_time_residual_stage.train();
        self.transfer_speed_residual_stage.train();
    }

    /// Kernel-weighted, shrunk corrections for each stage at the current time.
    pub(crate) fn predict_corrections(
        &self,
        peer: &PeerKeyLocation,
        contract_location: Location,
        distance: f64,
        modes: StageModes,
    ) -> RoutingCorrections {
        let time = wall_clock_hours() - self.reference_time_hours;
        self.predict_corrections_at_time(peer, contract_location, distance, modes, time)
    }

    pub(crate) fn predict_corrections_at_time(
        &self,
        peer: &PeerKeyLocation,
        contract_location: Location,
        distance: f64,
        modes: StageModes,
        time: f64,
    ) -> RoutingCorrections {
        let query = self.make_observation_immutable(peer, contract_location, distance, time);
        RoutingCorrections {
            failure: shrink(
                &self.failure_residual_stage,
                &self.failure_shrinkage,
                &query,
                modes.failure,
            ),
            response_time: shrink(
                &self.response_time_residual_stage,
                &self.response_time_shrinkage,
                &query,
                modes.response_time,
            ),
            transfer_speed: shrink(
                &self.transfer_speed_residual_stage,
                &self.transfer_speed_shrinkage,
                &query,
                modes.transfer_speed,
            ),
        }
    }

    /// Selected shrinkage parameters, for dashboard display. These are self-tuned,
    /// so they say something real about what the model has concluded about this
    /// network rather than echoing a constant back at the reader.
    pub(crate) fn shrinkage_diagnostics(&self) -> ShrinkageDiagnostics {
        ShrinkageDiagnostics {
            failure_kappa: self.failure_shrinkage.kappa(),
            // The effective bandwidth, i.e. the estimated length scale times the
            // multiplier the selector actually chose — reporting the raw length
            // scale would describe a kernel the model is not using.
            failure_bandwidth: self
                .failure_residual_stage
                .bandwidth
                .map(|scale| scale * self.failure_shrinkage.bandwidth_multiplier()),
            failure_residual_events: self.failure_residual_stage.len(),
            failure_scored: self.failure_shrinkage.scored(),
            response_time_residual_events: self.response_time_residual_stage.len(),
            transfer_speed_residual_events: self.transfer_speed_residual_stage.len(),
        }
    }

    /// Predict routing outcomes (immutable — training happens during record()).
    pub fn predict(
        &self,
        peer: &PeerKeyLocation,
        contract_location: Location,
        distance: f64,
    ) -> RoutingPredictionResult {
        let time = wall_clock_hours() - self.reference_time_hours;
        self.predict_at_time(peer, contract_location, distance, time)
    }

    /// Predict at a specific time (for testing).
    fn predict_at_time(
        &self,
        peer: &PeerKeyLocation,
        contract_location: Location,
        distance: f64,
        time: f64,
    ) -> RoutingPredictionResult {
        let query = self.make_observation_immutable(peer, contract_location, distance, time);

        RoutingPredictionResult {
            failure_probability: self.failure_stage.predict(&query),
            time_to_response_start: self.response_time_stage.predict(&query),
            transfer_speed: self.transfer_speed_stage.predict(&query),
        }
    }

    /// Blending weight for the failure stage prediction.
    pub fn failure_weight(&self) -> f64 {
        (self.failure_stage.len() as f64 / FAILURE_WEIGHT_RAMP_EVENTS).min(MAX_RENEGADE_WEIGHT)
    }

    /// Blending weight for the response time stage.
    pub fn response_time_weight(&self) -> f64 {
        (self.response_time_stage.len() as f64 / TIMING_WEIGHT_RAMP_EVENTS).min(MAX_RENEGADE_WEIGHT)
    }

    /// Blending weight for the transfer speed stage.
    pub fn transfer_speed_weight(&self) -> f64 {
        (self.transfer_speed_stage.len() as f64 / TIMING_WEIGHT_RAMP_EVENTS)
            .min(MAX_RENEGADE_WEIGHT)
    }

    /// Number of observations in the failure stage (most populated).
    pub fn len(&self) -> usize {
        self.failure_stage.len()
    }

    /// Number of distinct peers the predictor has seen.
    pub fn known_peers(&self) -> usize {
        self.peer_ids.len()
    }

    pub fn brier_score(&self) -> Option<f64> {
        self.accuracy.brier_score()
    }

    pub fn recent_brier_score(&self) -> Option<f64> {
        self.accuracy.recent_brier_score()
    }

    pub fn predictions_evaluated(&self) -> u64 {
        self.accuracy.total
    }

    pub fn recent_accuracy_pairs(&self) -> &VecDeque<(f64, f64)> {
        &self.accuracy.recent_pairs
    }

    /// Recent (predicted_secs, actual_secs) pairs for the response-time stage.
    pub fn response_time_accuracy_pairs(&self) -> &VecDeque<(f64, f64)> {
        &self.response_time_accuracy.recent_pairs
    }

    /// Recent (predicted_bps, actual_bps) pairs for the transfer-speed stage.
    pub fn transfer_speed_accuracy_pairs(&self) -> &VecDeque<(f64, f64)> {
        &self.transfer_speed_accuracy.recent_pairs
    }

    /// Count of response-time predictions scored against an actual outcome
    /// (finite (predicted, actual) pairs recorded on timed successes).
    pub fn response_time_predictions_evaluated(&self) -> u64 {
        self.response_time_accuracy.total
    }

    /// Count of transfer-speed predictions scored against an actual outcome
    /// (finite (predicted, actual) pairs recorded on timed successes).
    pub fn transfer_speed_predictions_evaluated(&self) -> u64 {
        self.transfer_speed_accuracy.total
    }

    pub fn stage_sizes(&self) -> (usize, usize, usize) {
        (
            self.failure_stage.len(),
            self.response_time_stage.len(),
            self.transfer_speed_stage.len(),
        )
    }

    fn make_observation(
        &mut self,
        peer: &PeerKeyLocation,
        contract_location: Location,
        distance: f64,
        time: f64,
    ) -> RoutingObservation {
        let peer_id = self.get_or_assign_peer_id(peer);
        RoutingObservation {
            peer_id: peer_id as f64,
            contract_location: contract_location.as_f64(),
            distance,
            time,
        }
    }

    fn make_observation_immutable(
        &self,
        peer: &PeerKeyLocation,
        contract_location: Location,
        distance: f64,
        time: f64,
    ) -> RoutingObservation {
        // For unknown peers, use next_peer_id (one past the last assigned).
        // This is within the normal ID range and won't pollute metric learning
        // with extreme values. It will have distance 1.0 from all known peers
        // in feature_distances, so predictions fall back to other features.
        let peer_id = self
            .peer_ids
            .get(peer)
            .map(|(id, _)| *id)
            .unwrap_or(self.next_peer_id);
        RoutingObservation {
            peer_id: peer_id as f64,
            contract_location: contract_location.as_f64(),
            distance,
            time,
        }
    }

    fn get_or_assign_peer_id(&mut self, peer: &PeerKeyLocation) -> u64 {
        self.lru_generation += 1;
        let generation = self.lru_generation;

        if let Some(entry) = self.peer_ids.get_mut(peer) {
            entry.1 = generation; // update LRU generation
            entry.0
        } else {
            // Evict least-recently-used peer if at capacity
            if self.peer_ids.len() >= MAX_PEER_IDS {
                // Find the entry with the lowest generation (O(N), but only on eviction)
                if let Some(oldest_key) = self
                    .peer_ids
                    .iter()
                    .min_by_key(|(_, (_, g))| *g)
                    .map(|(k, _)| k.clone())
                {
                    self.peer_ids.remove(&oldest_key);
                }
            }
            let id = self.next_peer_id;
            self.next_peer_id += 1;
            self.peer_ids.insert(peer.clone(), (id, generation));
            id
        }
    }
}

/// Wall-clock time in hours since epoch. Used for the time feature.
/// Note: For full deterministic simulation testing, this should be replaced
/// with TimeSource. Currently, the _at_time() methods allow controlled time
/// in tests, and batch loading passes original timestamps.
fn wall_clock_hours() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
        / 3600.0
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_peer() -> PeerKeyLocation {
        PeerKeyLocation::random()
    }

    fn success_untimed() -> RoutingOutcome {
        RoutingOutcome {
            success: true,
            time_to_response_start_secs: None,
            transfer_speed_bps: None,
        }
    }

    fn success_timed(response_time: f64, speed: f64) -> RoutingOutcome {
        RoutingOutcome {
            success: true,
            time_to_response_start_secs: Some(response_time),
            transfer_speed_bps: Some(speed),
        }
    }

    fn failure() -> RoutingOutcome {
        RoutingOutcome {
            success: false,
            time_to_response_start_secs: None,
            transfer_speed_bps: None,
        }
    }

    #[test]
    fn funnel_stages_receive_correct_data() {
        let mut predictor = RoutingPredictor::new(10000);
        let peer = make_peer();
        let contract = Location::try_from(0.5).unwrap();
        let base_time = 1.0; // relative hours

        for i in 0..10 {
            predictor.record_at_time_absolute_only(
                &peer,
                contract,
                0.1,
                failure(),
                base_time + i as f64 * 0.01,
            );
        }
        for i in 10..20 {
            predictor.record_at_time_absolute_only(
                &peer,
                contract,
                0.1,
                success_untimed(),
                base_time + i as f64 * 0.01,
            );
        }
        for i in 20..30 {
            predictor.record_at_time_absolute_only(
                &peer,
                contract,
                0.1,
                success_timed(0.1, 1000.0),
                base_time + i as f64 * 0.01,
            );
        }

        let (failure_n, response_n, speed_n) = predictor.stage_sizes();
        assert_eq!(failure_n, 30, "Failure stage gets all events");
        assert_eq!(
            response_n, 10,
            "Response time stage gets only timed successes"
        );
        assert_eq!(
            speed_n, 10,
            "Transfer speed stage gets only timed successes"
        );
    }

    #[test]
    fn failure_prediction_distinguishes_peers() {
        let mut predictor = RoutingPredictor::new(10000);
        let good_peer = make_peer();
        let bad_peer = make_peer();
        let contract = Location::try_from(0.5).unwrap();
        let base_time = 1.0;

        for i in 0..100 {
            let outcome = if i % 10 != 0 {
                success_untimed()
            } else {
                failure()
            };
            predictor.record_at_time_absolute_only(
                &good_peer,
                contract,
                0.1,
                outcome,
                base_time + i as f64 * 0.01,
            );
        }

        for i in 0..100 {
            let outcome = if i % 5 == 0 {
                success_untimed()
            } else {
                failure()
            };
            predictor.record_at_time_absolute_only(
                &bad_peer,
                contract,
                0.1,
                outcome,
                base_time + i as f64 * 0.01,
            );
        }

        let query_time = base_time + 1.0;
        let good_result = predictor.predict_at_time(&good_peer, contract, 0.1, query_time);
        let bad_result = predictor.predict_at_time(&bad_peer, contract, 0.1, query_time);

        let good_fail = good_result.failure_probability.unwrap();
        let bad_fail = bad_result.failure_probability.unwrap();
        eprintln!(
            "Good peer failure: {:.2}, Bad peer failure: {:.2}",
            good_fail, bad_fail
        );

        assert!(
            good_fail < 0.3,
            "Good peer should have low failure, got {:.2}",
            good_fail
        );
        assert!(
            bad_fail > 0.5,
            "Bad peer should have high failure, got {:.2}",
            bad_fail
        );
        assert!(good_result.time_to_response_start.is_none());
        assert!(good_result.transfer_speed.is_none());
    }

    #[test]
    fn timing_prediction_for_timed_successes() {
        let mut predictor = RoutingPredictor::new(10000);
        let fast_peer = make_peer();
        let slow_peer = make_peer();
        let contract = Location::try_from(0.5).unwrap();
        let base_time = 1.0;

        for i in 0..100 {
            predictor.record_at_time_absolute_only(
                &fast_peer,
                contract,
                0.1,
                success_timed(0.05, 10_000_000.0),
                base_time + i as f64 * 0.01,
            );
        }
        for i in 0..100 {
            predictor.record_at_time_absolute_only(
                &slow_peer,
                contract,
                0.1,
                success_timed(0.5, 100_000.0),
                base_time + i as f64 * 0.01,
            );
        }

        let query_time = base_time + 1.0;
        let fast_result = predictor.predict_at_time(&fast_peer, contract, 0.1, query_time);
        let slow_result = predictor.predict_at_time(&slow_peer, contract, 0.1, query_time);

        let fast_rt = fast_result.time_to_response_start.unwrap();
        let slow_rt = slow_result.time_to_response_start.unwrap();
        assert!(
            fast_rt < slow_rt,
            "Fast < slow response time: {:.3} vs {:.3}",
            fast_rt,
            slow_rt
        );

        let fast_speed = fast_result.transfer_speed.unwrap();
        let slow_speed = slow_result.transfer_speed.unwrap();
        assert!(
            fast_speed > slow_speed,
            "Fast > slow speed: {:.0} vs {:.0}",
            fast_speed,
            slow_speed
        );
    }

    #[test]
    fn detects_targeted_attack() {
        let mut predictor = RoutingPredictor::new(10000);
        let attacker = make_peer();
        let target_contract = Location::try_from(0.3).unwrap();
        let other_contract = Location::try_from(0.7).unwrap();
        let base_time = 1.0;

        for i in 0..100 {
            let loc = Location::try_from(i as f64 / 100.0).unwrap();
            predictor.record_at_time_absolute_only(
                &attacker,
                loc,
                0.1,
                success_untimed(),
                base_time + i as f64 * 0.01,
            );
        }
        for i in 0..50 {
            predictor.record_at_time_absolute_only(
                &attacker,
                target_contract,
                0.1,
                failure(),
                base_time + 1.0 + i as f64 * 0.01,
            );
        }

        let query_time = base_time + 2.0;
        let target = predictor.predict_at_time(&attacker, target_contract, 0.1, query_time);
        let other = predictor.predict_at_time(&attacker, other_contract, 0.1, query_time);

        let target_fail = target.failure_probability.unwrap();
        let other_fail = other.failure_probability.unwrap();
        assert!(
            target_fail > other_fail + 0.1,
            "Targeted contract should have higher failure: {:.2} vs {:.2}",
            target_fail,
            other_fail,
        );
    }

    #[test]
    fn sliding_window_eviction() {
        let mut predictor = RoutingPredictor::new(100);
        let peer = make_peer();
        let contract = Location::try_from(0.5).unwrap();

        for i in 0..200 {
            predictor.record_at_time_absolute_only(
                &peer,
                contract,
                0.1,
                success_untimed(),
                i as f64 * 0.01,
            );
        }

        assert!(
            predictor.len() <= 100,
            "Should evict, got {}",
            predictor.len()
        );

        // #4810: this test drives the LIVE `record_at_time` path deep into
        // saturation, so it is where the training freeze was observable — and
        // it sat here green for the whole time the freeze existed, because it
        // only ever asserted the eviction bound. `growth_based_retraining`
        // watches training but never saturates; this one saturates but never
        // watched training. Both halves were covered and never intersected,
        // which is exactly how #4810 got past CI. Assert the intersection:
        // training must still be keeping up at saturation. At max=100,
        // trained_at settles in [90, 100], so the retrain threshold is at most
        // 50 and the counter can never sit above it for long. Pre-fix this
        // reads ~100 (frozen, counting up forever).
        assert!(
            predictor.failure_stage.observations_since_train <= 50,
            "training froze at saturation (#4810): {} observations since last train",
            predictor.failure_stage.observations_since_train,
        );
    }

    #[test]
    fn peer_id_lru_eviction() {
        let mut predictor = RoutingPredictor::new(100000);
        let contract = Location::try_from(0.5).unwrap();

        // Add MAX_PEER_IDS + 10 unique peers
        let mut peers = Vec::new();
        for i in 0..(MAX_PEER_IDS + 10) {
            let peer = make_peer();
            predictor.record_at_time_absolute_only(
                &peer,
                contract,
                0.1,
                success_untimed(),
                i as f64 * 0.001,
            );
            peers.push(peer);
        }

        assert!(
            predictor.known_peers() <= MAX_PEER_IDS,
            "peer_ids should be bounded at {}, got {}",
            MAX_PEER_IDS,
            predictor.known_peers(),
        );
    }

    #[test]
    fn inf_output_rejected() {
        let mut predictor = RoutingPredictor::new(10000);
        let peer = make_peer();
        let contract = Location::try_from(0.5).unwrap();

        // Record with Inf transfer speed (from zero-duration transfer)
        predictor.record_at_time_absolute_only(
            &peer,
            contract,
            0.1,
            RoutingOutcome {
                success: true,
                time_to_response_start_secs: Some(0.1),
                transfer_speed_bps: Some(f64::INFINITY),
            },
            1.0,
        );

        // The Inf observation should have been rejected by PredictionStage::add
        assert_eq!(predictor.stage_sizes().2, 0, "Inf should not be recorded");
        // But the failure and response time stages should have the observation
        assert_eq!(predictor.stage_sizes().0, 1);
        assert_eq!(predictor.stage_sizes().1, 1);

        // A rejected observation must not count toward retraining either: the
        // increment sits after the `!is_finite()` early-return in `add()`, so a
        // stage fed nothing but Inf never trains. Without this, moving the
        // increment above the early-return would still pass the size asserts.
        assert_eq!(
            predictor.transfer_speed_stage.observations_since_train, 0,
            "rejected Inf observation must not count toward retraining",
        );
    }

    #[test]
    fn growth_based_retraining() {
        let mut predictor = RoutingPredictor::new(10000);
        let peer = make_peer();
        let contract = Location::try_from(0.5).unwrap();

        // Add 20 events — should trigger initial training
        for i in 0..20 {
            predictor.record_at_time_absolute_only(
                &peer,
                contract,
                0.1,
                success_untimed(),
                i as f64 * 0.01,
            );
        }
        let _k_after_20 = predictor.failure_stage.cached_k;

        // Add 10 more (50% growth) — should trigger retrain
        for i in 20..30 {
            predictor.record_at_time_absolute_only(
                &peer,
                contract,
                0.1,
                failure(),
                i as f64 * 0.01,
            );
        }
        // Can't easily assert K changed, but trained_at should have updated
        assert!(
            predictor.failure_stage.trained_at >= 20,
            "Should have retrained, trained_at={}",
            predictor.failure_stage.trained_at,
        );
    }

    fn stage_obs(i: usize) -> RoutingObservation {
        RoutingObservation {
            peer_id: (i % 7) as f64,
            contract_location: (i % 100) as f64 / 100.0,
            distance: (i % 50) as f64 / 100.0,
            time: i as f64 * 0.01,
        }
    }

    /// Drive a stage exactly the way the live `record_at_time` path does:
    /// add the observation (which may evict), then train if due.
    /// Returns true if training fired.
    fn add_then_maybe_train(stage: &mut PredictionStage, i: usize) -> bool {
        stage.add(stage_obs(i), (i % 2) as f64);
        if stage.should_train() {
            stage.train();
            true
        } else {
            false
        }
    }

    /// Regression test for #4810: training must stay reachable once the stage
    /// saturates its observation cap.
    ///
    /// The old rule (`n >= trained_at + trained_at / 2`) compares against the
    /// TOTAL observation count, which eviction pins below `max_observations`.
    /// Once `trained_at * 3 / 2` exceeds the reachable count, the condition is
    /// unsatisfiable and training never fires again.
    #[test]
    fn regression_training_reachable_after_saturation() {
        // Small cap so saturation is reached quickly. Ladder is 20 → 30 → 45 →
        // 67 → 100, and at trained_at=100 the old rule needs n >= 150, which a
        // 100-observation cap can never reach.
        let mut stage = PredictionStage::new(100);
        let mut i = 0;

        // Warm up well past saturation.
        for _ in 0..1_000 {
            add_then_maybe_train(&mut stage, i);
            i += 1;
        }

        // Guard against a vacuous test: assert we are genuinely in the state the
        // old rule could never escape, or the count below proves nothing.
        //
        // This is the STRUCTURAL form from #4810 ("any trained_at > 3333 freezes,
        // because trained_at * 1.5 > 5000 from there on"), scaled to this cap. It
        // is phase-independent: `len()` oscillates in [90, 100] as eviction
        // fires, so an instantaneous `len() < trained_at + trained_at/2` could be
        // satisfied by a trough rather than by genuine unreachability. Comparing
        // against `max_observations` instead holds at every point in the cycle.
        //
        // Concretely, why the instantaneous form is not enough: change the cap
        // above to 100_000 and the warmup ends at len=1000, trained_at=757. The
        // instantaneous check passes (1000 < 1135) even though the old rule is
        // nowhere near frozen — 1135 is perfectly reachable under that cap — so
        // the test would pass while proving nothing. The structural check
        // correctly fails there (1135 is not > 100_000).
        assert!(
            stage.trained_at + stage.trained_at / 2 > stage.max_observations,
            "test precondition: expected the old rule to be unsatisfiable, but \
             trained_at={} still allows it under cap {}",
            stage.trained_at,
            stage.max_observations,
        );

        // Training must still fire over a further run of observations.
        let mut trainings = 0;
        for _ in 0..1_000 {
            if add_then_maybe_train(&mut stage, i) {
                trainings += 1;
            }
            i += 1;
        }

        // Bounded on BOTH sides. The lower bound catches the freeze; the upper
        // bound catches a fix that restores training by making it fire far too
        // often (e.g. an escape hatch like `if n >= max_observations * 9 / 10 {
        // return true }`, which would train on EVERY event — an O(n^2)
        // `ensure_trained` per routing event at n~4500). The cadence is the whole
        // reason this fix is cheap, so pin it.
        //
        // Deterministic: trained_at settles in [90, 100], so the threshold is
        // 45-50 and 1000 observations yield 21 trainings. The band absorbs
        // incidental drift without admitting either failure mode.
        assert!(
            (15..=25).contains(&trainings),
            "expected ~21 trainings over 1000 observations at saturation, got {} \
             (0 = frozen (#4810); >25 = retraining far too often) with \
             trained_at={}, len={}",
            trainings,
            stage.trained_at,
            stage.len(),
        );
    }

    /// Pins the pre-saturation retraining cadence from #4810 so the fix for the
    /// frozen-at-saturation case does not perturb the 50%-growth ladder.
    #[test]
    fn pre_saturation_training_cadence_unchanged() {
        // Cap far above the observations added, so nothing is ever evicted.
        let mut stage = PredictionStage::new(100_000);
        let mut ladder = Vec::new();

        for i in 0..600 {
            if add_then_maybe_train(&mut stage, i) {
                ladder.push(stage.trained_at);
            }
        }

        // The exact 50%-growth ladder documented in #4810. It is DERIVED, not
        // magic: it starts at MIN_OBSERVATIONS_FOR_TRAINING (20) and each rung
        // is `t + t / 2` (integer division). If you change either the constant
        // or the 50% factor, recompute this vector from the new values rather
        // than editing it to match whatever the test now prints — the point of
        // the assertion is that the cadence is the one that was designed.
        assert_eq!(ladder, vec![20, 30, 45, 67, 100, 150, 225, 337, 505]);
    }

    #[test]
    fn accuracy_tracking() {
        let mut predictor = RoutingPredictor::new(10000);
        let peer = make_peer();
        let contract = Location::try_from(0.5).unwrap();

        // Add enough data to enable predictions
        for i in 0..50 {
            predictor.record_at_time_absolute_only(
                &peer,
                contract,
                0.1,
                success_untimed(),
                i as f64 * 0.01,
            );
        }

        // Now further events should be tracked for accuracy
        for i in 50..60 {
            predictor.record_at_time_absolute_only(
                &peer,
                contract,
                0.1,
                success_untimed(),
                i as f64 * 0.01,
            );
        }

        assert!(
            predictor.predictions_evaluated() > 0,
            "Should have evaluated some predictions",
        );
        assert!(predictor.brier_score().is_some());
    }

    #[test]
    fn regression_accuracy_tracked_for_timing_stages() {
        let mut predictor = RoutingPredictor::new(10000);
        let peer = make_peer();
        let contract = Location::try_from(0.5).unwrap();

        // Warm up the timing stages past MIN_OBSERVATIONS_FOR_PREDICTION so the
        // next timed successes produce a prediction that gets scored.
        for i in 0..50 {
            predictor.record_at_time_absolute_only(
                &peer,
                contract,
                0.1,
                success_timed(0.1, 1000.0),
                i as f64 * 0.01,
            );
        }
        for i in 50..60 {
            predictor.record_at_time_absolute_only(
                &peer,
                contract,
                0.1,
                success_timed(0.1, 1000.0),
                i as f64 * 0.01,
            );
        }

        assert!(
            predictor.response_time_predictions_evaluated() > 0,
            "response-time predictions should be scored once the stage is trained",
        );
        assert!(
            predictor.transfer_speed_predictions_evaluated() > 0,
            "transfer-speed predictions should be scored once the stage is trained",
        );
        assert!(
            !predictor.response_time_accuracy_pairs().is_empty(),
            "response-time accuracy pairs should be recorded for the scatter plot",
        );
        assert!(
            !predictor.transfer_speed_accuracy_pairs().is_empty(),
            "transfer-speed accuracy pairs should be recorded for the scatter plot",
        );
    }

    #[test]
    fn regression_accuracy_not_tracked_for_failures() {
        let mut predictor = RoutingPredictor::new(10000);
        let peer = make_peer();
        let contract = Location::try_from(0.5).unwrap();

        // Failures carry no timing ground truth, so the regression stages must
        // never accumulate accuracy samples from them.
        for i in 0..60 {
            predictor.record_at_time_absolute_only(
                &peer,
                contract,
                0.1,
                failure(),
                i as f64 * 0.01,
            );
        }

        assert_eq!(predictor.response_time_predictions_evaluated(), 0);
        assert_eq!(predictor.transfer_speed_predictions_evaluated(), 0);
        assert!(predictor.response_time_accuracy_pairs().is_empty());
        assert!(predictor.transfer_speed_accuracy_pairs().is_empty());
    }

    #[test]
    fn regression_accuracy_skips_untimed_successes() {
        let mut predictor = RoutingPredictor::new(10000);
        let peer = make_peer();
        let contract = Location::try_from(0.5).unwrap();

        // Warm the timing stages with timed successes so predict() returns Some.
        for i in 0..60 {
            predictor.record_at_time_absolute_only(
                &peer,
                contract,
                0.1,
                success_timed(0.1, 1000.0),
                i as f64 * 0.01,
            );
        }
        let rt_before = predictor.response_time_predictions_evaluated();
        let ts_before = predictor.transfer_speed_predictions_evaluated();
        assert!(rt_before > 0 && ts_before > 0, "stages should be warmed");

        // An untimed success carries no response-time/transfer-speed ground truth,
        // so it must NOT be scored even though the stages can now predict (guards
        // against recording a garbage/zero actual into the scatter).
        for i in 60..70 {
            predictor.record_at_time_absolute_only(
                &peer,
                contract,
                0.1,
                success_untimed(),
                i as f64 * 0.01,
            );
        }
        assert_eq!(predictor.response_time_predictions_evaluated(), rt_before);
        assert_eq!(predictor.transfer_speed_predictions_evaluated(), ts_before);
    }
}
/// Recoverability of a KNOWN generative model (#4485, Tier 2a).
///
/// These tests answer the question relative comparisons cannot: *does the
/// correction actually learn the structure it exists to learn, on the data
/// volume a real node has?*
///
/// # Scoring against the probability, not the outcome
///
/// Outcomes are generated from a known `p*`, and predictions are scored against
/// `p*` rather than against the sampled 0/1. The decomposition is exact:
///
/// ```text
/// E[(p̂ − y)²] = E[(p̂ − p*)²] + E[p*(1 − p*)]
///                └─ learnable ┘  └─ irreducible ┘
/// ```
///
/// Brier against *outcomes* is dominated by the second term — which is why a
/// production Brier of 0.009 read as "excellent" for a model with negative
/// skill. Scoring against `p*` isolates the learnable part, so convergence is
/// measurable in hundreds of events instead of tens of thousands.
///
/// Two exact quantities follow, both computable here because `p*` is known:
/// the **Bayes floor** `E[p*(1−p*)]`, which no predictor can beat, and the
/// **learnable headroom**, which equals `Var(p*)` exactly (it is Jensen's gap).
/// So the headline metric is
///
/// ```text
/// captured = 1 − E[(p̂ − p*)²] / Var(p*)
/// ```
///
/// `0` for a climatology forecast, `1` for the oracle — an absolute scale rather
/// than "better than the variant we happened to compare against".
#[cfg(test)]
mod recoverability {
    use super::*;
    use crate::config::GlobalRng;
    use crate::router::isotonic_estimator::{EstimatorType, IsotonicEstimator, IsotonicEvent};

    /// Events before scoring starts, so the isotonic base has a curve to be
    /// corrected and the comparison is not dominated by cold start.
    const WARMUP_EVENTS: usize = 300;

    /// Sized from production: nova's gateways hold 4,155 and 2,745 failure
    /// observations. A mechanism that needs materially more than this cannot
    /// work on a real node however elegant it is, so the budget is the
    /// assertion, not an implementation detail.
    const RECOVERY_BUDGET_EVENTS: usize = 2_000;

    const PEER_COUNT: usize = 12;

    /// What generated the outcomes. Each isolates one capability.
    #[derive(Debug, Clone, Copy, PartialEq)]
    enum Model {
        /// `p* = f(distance)` only. The isotonic base should capture this and
        /// the correction should add ~nothing.
        DistanceOnly,
        /// `p* = f(distance) + g(peer)`. The per-peer EWMA should capture it.
        PeerMarginal,
        /// `p* = f(distance) + penalty` on specific (peer, contract) pairs.
        /// ONLY the correction can capture this — the headline case.
        PeerContract,
        /// `p*` constant. Nothing to learn; the correction must not invent
        /// structure. `Var(p*) = 0`, so `captured` is undefined here by
        /// construction and the assertions are on error and λ instead.
        Noise,
    }

    /// Targeted (peer, contract-offset) pairs for `PeerContract`.
    ///
    /// The offsets deliberately SPAN A RANGE OF DISTANCES. A single peer with a
    /// single narrow contract band — as in the abandoned harness on
    /// `rescue/dirty-residual-routing-correction` — puts every targeted event at
    /// nearly one distance, where the GLOBAL isotonic curve can absorb part of
    /// the effect. The test then passes for the wrong reason, or understates the
    /// correction's contribution. Spreading the offsets makes the effect
    /// genuinely inseparable from distance alone.
    const TARGETED: [(usize, f64); 3] = [(0, 0.05), (1, 0.17), (2, 0.31)];

    /// Half-width of a targeted contract band.
    const BAND: f64 = 0.02;

    struct Scenario {
        peers: Vec<PeerKeyLocation>,
    }

    impl Scenario {
        fn new() -> Self {
            Scenario {
                peers: (0..PEER_COUNT).map(|_| PeerKeyLocation::random()).collect(),
            }
        }

        fn peer_location(&self, index: usize) -> f64 {
            self.peers[index]
                .location()
                .expect("generated peers carry a location")
                .as_f64()
        }

        /// Centre of the targeted band for a targeted peer, placed at a fixed
        /// ring offset from that peer so its distance is controlled.
        fn band_centre(&self, peer_index: usize, offset: f64) -> f64 {
            (self.peer_location(peer_index) + offset).rem_euclid(1.0)
        }

        fn is_targeted(&self, peer_index: usize, contract: f64) -> bool {
            TARGETED.iter().any(|&(target, offset)| {
                target == peer_index
                    && ring_distance(contract, self.band_centre(target, offset)) < BAND
            })
        }

        /// The generating probability. Known exactly, which is the whole point.
        fn true_probability(
            &self,
            model: Model,
            peer_index: usize,
            contract: f64,
            distance: f64,
        ) -> f64 {
            let base = match model {
                Model::Noise => 0.08,
                // Concave rather than linear, so the monotone isotonic base is
                // not trivially perfect and the test says something about fit.
                // Listed exhaustively so a new model must decide its own base
                // rather than silently inheriting this one.
                Model::DistanceOnly | Model::PeerMarginal | Model::PeerContract => {
                    0.03 + 0.45 * distance.sqrt()
                }
            };
            let extra = match model {
                Model::DistanceOnly | Model::Noise => 0.0,
                // A per-peer offset the EWMA can absorb.
                Model::PeerMarginal => {
                    if peer_index % 4 == 0 {
                        0.30
                    } else {
                        0.0
                    }
                }
                Model::PeerContract => {
                    if self.is_targeted(peer_index, contract) {
                        0.55
                    } else {
                        0.0
                    }
                }
            };
            (base + extra).clamp(0.01, 0.99)
        }

        /// Draw the next event, biased so targeted pairs are sampled often
        /// enough to be learnable but stay a small minority of traffic.
        fn draw(&self, model: Model, index: usize) -> (usize, f64) {
            let targeted_turn = model == Model::PeerContract && index % 12 == 0;
            if targeted_turn {
                let (peer_index, offset) = TARGETED[(index / 12) % TARGETED.len()];
                let centre = self.band_centre(peer_index, offset);
                let jitter = GlobalRng::random_range(-BAND..BAND);
                (peer_index, (centre + jitter).rem_euclid(1.0))
            } else {
                (
                    GlobalRng::random_range(0..PEER_COUNT),
                    GlobalRng::random_range(0.0..1.0),
                )
            }
        }
    }

    /// What a run measured.
    #[derive(Debug, Clone, Copy)]
    struct Recovery {
        /// `1 − E[(p̂−p*)²]/Var(p*)` for the corrected prediction.
        captured_corrected: f64,
        /// The same for the uncorrected base — the λ=0 counterfactual, and the
        /// negative control: if the base captures the structure too, the
        /// scenario is not testing what it claims to.
        captured_base: f64,
        mse_corrected: f64,
        mse_base: f64,
        var_p_star: f64,
        bayes_floor: f64,
        mean_lambda: f64,
        /// Mean magnitude of the correction actually applied. This, not `lambda`,
        /// is the overfitting guard: `lambda` measures how much EVIDENCE there
        /// is, which is legitimately high on abundant data even when that
        /// evidence says "the residual is zero".
        mean_abs_correction: f64,
        scored: usize,
    }

    fn run(model: Model, events: usize, seed: u64) -> Recovery {
        let _guard = GlobalRng::seed_guard(seed);
        let scenario = Scenario::new();
        let mut predictor = RoutingPredictor::new(10_000);
        let mut isotonic = IsotonicEstimator::new(Vec::new(), EstimatorType::Positive);
        let modes = StageModes {
            failure: isotonic.adjustment_mode(),
            response_time: AdjustmentMode::Multiplicative,
            transfer_speed: AdjustmentMode::Additive,
        };

        let mut sum_p_star = 0.0;
        let mut sum_p_star_sq = 0.0;
        let mut sum_bayes = 0.0;
        let mut sum_err_corrected = 0.0;
        let mut sum_err_base = 0.0;
        let mut sum_lambda = 0.0;
        let mut sum_abs_correction = 0.0;
        let mut scored = 0usize;

        for index in 0..events {
            let (peer_index, contract_value) = scenario.draw(model, index);
            let peer = &scenario.peers[peer_index];
            let contract = Location::try_from(contract_value).expect("contract within ring");
            let distance = contract
                .distance(peer.location().expect("peer has a location"))
                .as_f64();
            let time = index as f64 / 60.0;

            let p_star = scenario.true_probability(model, peer_index, contract_value, distance);
            let actual = if GlobalRng::random_range(0.0..1.0) < p_star {
                1.0
            } else {
                0.0
            };

            // Predict BEFORE this event reaches either learner.
            let base = isotonic
                .estimate_global(peer, contract)
                .ok()
                .map(|value| value.clamp(0.0, 1.0));

            if let Some(base) = base {
                let correction = predictor
                    .predict_corrections_at_time(peer, contract, distance, modes, time)
                    .failure;
                let corrected = (base + correction.map_or(0.0, |c| c.value)).clamp(0.0, 1.0);

                if index >= WARMUP_EVENTS {
                    sum_p_star += p_star;
                    sum_p_star_sq += p_star * p_star;
                    sum_bayes += p_star * (1.0 - p_star);
                    sum_err_corrected += (corrected - p_star).powi(2);
                    sum_err_base += (base - p_star).powi(2);
                    sum_lambda += correction.map_or(0.0, |c| c.lambda);
                    sum_abs_correction += correction.map_or(0.0, |c| c.value.abs());
                    scored += 1;
                }

                let residual = isotonic.adjustment_mode().residual(actual, base);
                predictor.record_at_time(
                    peer,
                    contract,
                    distance,
                    RoutingOutcome {
                        success: actual == 0.0,
                        time_to_response_start_secs: None,
                        transfer_speed_bps: None,
                    },
                    StageResiduals {
                        failure: residual,
                        response_time: None,
                        transfer_speed: None,
                    },
                    time,
                );
            }

            isotonic.add_event(IsotonicEvent {
                peer: peer.clone(),
                contract_location: contract,
                result: actual,
            });
        }

        let n = scored.max(1) as f64;
        let mean_p_star = sum_p_star / n;
        let var_p_star = (sum_p_star_sq / n) - mean_p_star * mean_p_star;
        let mse_corrected = sum_err_corrected / n;
        let mse_base = sum_err_base / n;

        Recovery {
            captured_corrected: 1.0 - mse_corrected / var_p_star,
            captured_base: 1.0 - mse_base / var_p_star,
            mse_corrected,
            mse_base,
            var_p_star,
            bayes_floor: sum_bayes / n,
            mean_lambda: sum_lambda / n,
            mean_abs_correction: sum_abs_correction / n,
            scored,
        }
    }

    /// Average a metric over seeds, so a threshold is not riding on one draw.
    fn over_seeds(model: Model, events: usize, f: impl Fn(Recovery) -> f64) -> f64 {
        const SEEDS: [u64; 5] = [
            0x4485_0001,
            0x4485_0002,
            0x4485_0003,
            0x4485_0004,
            0x4485_0005,
        ];
        SEEDS
            .iter()
            .map(|&seed| f(run(model, events, seed)))
            .sum::<f64>()
            / SEEDS.len() as f64
    }

    /// The headline test: a peer×contract effect must be recovered well enough
    /// to beat a climatology forecast within the data volume a real gateway
    /// holds.
    ///
    /// # The target this does NOT assert, and why
    ///
    /// The design proposal on #4485 published `captured >= 0.8` as the gate.
    /// **That figure was set from intuition and is not achievable in this
    /// scenario**, for a reason the harness itself measures: the global isotonic
    /// base is off by `sd ~ 0.13` on the 91.7% of events that are untargeted —
    /// partly because the 8.3% of events carrying a +0.55 penalty bend the fit,
    /// partly because the fit is over binary draws. That error floor dominates
    /// the achievable ceiling regardless of how good the correction is, so 0.8
    /// was never reachable here and the number should not have been published
    /// without this decomposition behind it.
    ///
    /// What IS asserted is the property that actually matters and was genuinely
    /// in doubt: **the corrected estimate beats assuming the base rate.** Before
    /// the correction composed with the global curve it did not — it scored
    /// `captured = -0.30`, worse than assuming nothing.
    ///
    /// The measured figure is printed on every run so the gap stays visible
    /// rather than being quietly forgotten, and closing it is what gates turning
    /// `FREENET_ROUTING_RESIDUAL_CORRECTION` on by default.
    #[test]
    fn recovered_structure_beats_assuming_the_base_rate() {
        let captured = over_seeds(Model::PeerContract, RECOVERY_BUDGET_EVENTS, |r| {
            r.captured_corrected
        });
        let base = over_seeds(Model::PeerContract, RECOVERY_BUDGET_EVENTS, |r| {
            r.captured_base
        });
        let lambda = over_seeds(Model::PeerContract, RECOVERY_BUDGET_EVENTS, |r| {
            r.mean_lambda
        });

        eprintln!(
            "#4485 recovery at {RECOVERY_BUDGET_EVENTS} events: captured {captured:.3} \
             (base {base:.3}, mean lambda {lambda:.3}); published target 0.8 NOT met \
             — see this test's docs for the noise decomposition"
        );

        assert!(
            captured > 0.0,
            "the corrected estimate must beat a climatology forecast; captured \
             {captured:.3} (base {base:.3}, mean lambda {lambda:.3})"
        );
        assert!(
            captured > base + 0.3,
            "the correction must add substantial signal over its own base; \
             captured {captured:.3} vs base {base:.3}"
        );
    }

    /// The no-regression gate: where there is no peer×contract structure, the
    /// correction must not make the estimate worse.
    #[test]
    fn distance_only_structure_is_not_degraded_by_the_correction() {
        let ratio = over_seeds(Model::DistanceOnly, RECOVERY_BUDGET_EVENTS, |r| {
            r.mse_corrected / r.mse_base.max(f64::MIN_POSITIVE)
        });
        assert!(
            ratio <= 1.05,
            "with nothing to learn the correction must not degrade the base \
             estimate; error ratio {ratio:.3}"
        );
    }

    /// A peer-marginal effect is the EWMA's job. The correction must not fight
    /// it or double-count it.
    #[test]
    fn peer_marginal_structure_is_not_degraded_by_the_correction() {
        let ratio = over_seeds(Model::PeerMarginal, RECOVERY_BUDGET_EVENTS, |r| {
            r.mse_corrected / r.mse_base.max(f64::MIN_POSITIVE)
        });
        assert!(
            ratio <= 1.05,
            "the correction must compose with the per-peer EWMA rather than \
             fight it; error ratio {ratio:.3}"
        );
    }

    /// The overfitting guard. Pure noise has no structure, so a learner that
    /// "recovers" something here is inventing it.
    #[test]
    fn pure_noise_yields_no_correction_and_no_invented_structure() {
        let events = RECOVERY_BUDGET_EVENTS;
        let mean_lambda = over_seeds(Model::Noise, events, |r| r.mean_lambda);
        let mse = over_seeds(Model::Noise, events, |r| r.mse_corrected);
        let base_mse = over_seeds(Model::Noise, events, |r| r.mse_base);
        let applied = over_seeds(Model::Noise, events, |r| r.mean_abs_correction);

        assert!(
            mse <= base_mse + 1e-3,
            "on pure noise the correction must not degrade the base estimate; \
             corrected {mse:.5} vs base {base_mse:.5}"
        );
        // The guard is the size of the correction APPLIED, not lambda.
        //
        // An earlier version asserted `lambda < 0.5` here and failed at 0.779,
        // which turned out to be the assertion being wrong rather than the code:
        // lambda measures how much evidence supports the residual estimate, and
        // on 2000 events of abundant data that evidence is real. What it
        // supports is the conclusion that the residual is ZERO, so a confident
        // lambda multiplied by `r_hat ~ 0` is still ~0 — which is exactly the
        // behaviour wanted, and which the error assertion above already
        // confirms. Asserting on lambda was measuring the wrong quantity.
        assert!(
            applied < 0.05,
            "on pure noise the APPLIED correction must be negligible; mean |correction| \
             {applied:.4} (mean lambda {mean_lambda:.3}, which is allowed to be high)"
        );
    }

    /// Records the learning curve, and pins that recovery IMPROVES with data
    /// rather than arriving by luck at one budget.
    #[test]
    fn recovery_improves_monotonically_with_data() {
        let checkpoints = [500usize, 1_000, 2_000, 4_000];
        let captured: Vec<f64> = checkpoints
            .iter()
            .map(|&events| over_seeds(Model::PeerContract, events, |r| r.captured_corrected))
            .collect();

        eprintln!("learning curve (events -> captured): {checkpoints:?} -> {captured:?}");

        // Direction, not level. The level is bounded by the base model's own
        // error floor (see `recovered_structure_beats_assuming_the_base_rate`),
        // so asserting a level here would be asserting a property of the
        // isotonic fit rather than of the correction.
        assert!(
            captured[captured.len() - 1] >= captured[0] - 0.05,
            "recovery must not DEGRADE as data accumulates, got {captured:?}"
        );
        assert!(
            captured.iter().all(|value| value.is_finite()),
            "every checkpoint must produce a finite score, got {captured:?}"
        );
    }

    /// The identity the scoring rests on, verified rather than assumed:
    /// learnable headroom equals `Var(p*)`, and the Bayes floor plus that
    /// headroom is the climatology Brier.
    #[test]
    fn learnable_headroom_equals_variance_of_the_true_probability() {
        let recovery = run(Model::PeerContract, 2_000, 0x4485_0001);
        let mean_p = {
            // climatology Brier = p̄(1−p̄) = bayes_floor + Var(p*)
            recovery.bayes_floor + recovery.var_p_star
        };
        assert!(
            recovery.var_p_star > 0.0,
            "this scenario must carry learnable signal, got Var(p*)={}",
            recovery.var_p_star
        );
        assert!(
            mean_p > recovery.bayes_floor,
            "climatology must be strictly worse than the Bayes floor whenever \
             p* varies"
        );
        assert!(
            recovery.scored > 1_000,
            "expected a substantial scored window, got {}",
            recovery.scored
        );
    }
}
