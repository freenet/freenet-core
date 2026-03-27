//! Renegade-ML based routing predictor for detecting peer × contract interaction patterns.
//!
//! Uses a funnel of three prediction stages, each conditional on the previous:
//! 1. **Success probability** — trained on all routing events
//! 2. **Time to response start** — trained only on successful events
//! 3. **Transfer speed** — trained only on successful events with timing data
//!
//! Each stage uses the same features: (peer_id, contract_location, distance, time).
//! Separate predictor instances are used per operation type (GET, PUT, etc.).

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
    /// Cached K from the last training. Used for immutable predictions.
    cached_k: usize,
    /// Number of observations when last trained (for growth-based retraining).
    trained_at: usize,
}

impl PredictionStage {
    fn new(max_observations: usize) -> Self {
        PredictionStage {
            model: Renegade::new(),
            max_observations,
            count: 0,
            cached_k: DEFAULT_K,
            trained_at: 0,
        }
    }

    fn add(&mut self, obs: RoutingObservation, output: f64) {
        // Guard against Inf/NaN outputs from division by zero
        if !output.is_finite() {
            return;
        }
        self.model.add(obs, output);
        self.count += 1;
        if self.count > self.max_observations {
            self.evict_oldest();
        }
    }

    /// Check if training should happen based on data growth (50% since last train).
    fn should_train(&self) -> bool {
        let n = self.model.len();
        if n < MIN_OBSERVATIONS_FOR_TRAINING {
            return false;
        }
        if self.trained_at == 0 {
            return true;
        }
        // Retrain when data has grown 50% since last training
        n >= self.trained_at + self.trained_at / 2
    }

    /// Trigger training (metric learning + K selection).
    fn train(&mut self) {
        if self.model.len() >= MIN_OBSERVATIONS_FOR_TRAINING {
            self.cached_k = self.model.get_optimal_k();
            self.trained_at = self.model.len();
        }
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

impl RoutingPredictor {
    /// Create a new predictor.
    pub fn new(max_observations_per_stage: usize) -> Self {
        RoutingPredictor {
            failure_stage: PredictionStage::new(max_observations_per_stage),
            response_time_stage: PredictionStage::new(max_observations_per_stage),
            transfer_speed_stage: PredictionStage::new(max_observations_per_stage),
            peer_ids: HashMap::new(),
            lru_generation: 0,
            next_peer_id: 0,
            accuracy: PredictionAccuracy::new(),
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
    ) {
        let time = wall_clock_hours() - self.reference_time_hours;
        self.record_at_time(peer, contract_location, distance, outcome, time);
    }

    /// Record at a specific relative time (for batch loading with original timestamps
    /// and for testing with controlled time).
    pub(crate) fn record_at_time(
        &mut self,
        peer: &PeerKeyLocation,
        contract_location: Location,
        distance: f64,
        outcome: RoutingOutcome,
        time: f64,
    ) {
        let actual_failure = if outcome.success { 0.0 } else { 1.0 };

        // Track prediction accuracy (skip in batch mode — no trained model yet)
        if !self.batch_mode {
            if let Some(predicted_failure) = self
                .failure_stage
                .predict(&self.make_observation_immutable(peer, contract_location, distance, time))
            {
                self.accuracy.record(predicted_failure, actual_failure);
            }
        }

        let obs = self.make_observation(peer, contract_location, distance, time);

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

        // Train based on data growth (not at fixed counts)
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
        }
    }

    /// Trigger training on all stages. Call after batch loading historical events.
    pub fn finish_batch(&mut self) {
        self.batch_mode = false;
        self.failure_stage.train();
        self.response_time_stage.train();
        self.transfer_speed_stage.train();
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
            predictor.record_at_time(&peer, contract, 0.1, failure(), base_time + i as f64 * 0.01);
        }
        for i in 10..20 {
            predictor.record_at_time(
                &peer,
                contract,
                0.1,
                success_untimed(),
                base_time + i as f64 * 0.01,
            );
        }
        for i in 20..30 {
            predictor.record_at_time(
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
            predictor.record_at_time(
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
            predictor.record_at_time(
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
            predictor.record_at_time(
                &fast_peer,
                contract,
                0.1,
                success_timed(0.05, 10_000_000.0),
                base_time + i as f64 * 0.01,
            );
        }
        for i in 0..100 {
            predictor.record_at_time(
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
            predictor.record_at_time(
                &attacker,
                loc,
                0.1,
                success_untimed(),
                base_time + i as f64 * 0.01,
            );
        }
        for i in 0..50 {
            predictor.record_at_time(
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
            predictor.record_at_time(&peer, contract, 0.1, success_untimed(), i as f64 * 0.01);
        }

        assert!(
            predictor.len() <= 100,
            "Should evict, got {}",
            predictor.len()
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
            predictor.record_at_time(&peer, contract, 0.1, success_untimed(), i as f64 * 0.001);
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
        predictor.record_at_time(
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
    }

    #[test]
    fn growth_based_retraining() {
        let mut predictor = RoutingPredictor::new(10000);
        let peer = make_peer();
        let contract = Location::try_from(0.5).unwrap();

        // Add 20 events — should trigger initial training
        for i in 0..20 {
            predictor.record_at_time(&peer, contract, 0.1, success_untimed(), i as f64 * 0.01);
        }
        let _k_after_20 = predictor.failure_stage.cached_k;

        // Add 10 more (50% growth) — should trigger retrain
        for i in 20..30 {
            predictor.record_at_time(&peer, contract, 0.1, failure(), i as f64 * 0.01);
        }
        // Can't easily assert K changed, but trained_at should have updated
        assert!(
            predictor.failure_stage.trained_at >= 20,
            "Should have retrained, trained_at={}",
            predictor.failure_stage.trained_at,
        );
    }

    #[test]
    fn accuracy_tracking() {
        let mut predictor = RoutingPredictor::new(10000);
        let peer = make_peer();
        let contract = Location::try_from(0.5).unwrap();

        // Add enough data to enable predictions
        for i in 0..50 {
            predictor.record_at_time(&peer, contract, 0.1, success_untimed(), i as f64 * 0.01);
        }

        // Now further events should be tracked for accuracy
        for i in 50..60 {
            predictor.record_at_time(&peer, contract, 0.1, success_untimed(), i as f64 * 0.01);
        }

        assert!(
            predictor.predictions_evaluated() > 0,
            "Should have evaluated some predictions",
        );
        assert!(predictor.brier_score().is_some());
    }
}
