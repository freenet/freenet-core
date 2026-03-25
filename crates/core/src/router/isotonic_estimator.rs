use crate::ring::{Distance, Location, PeerKeyLocation};
use pav_regression::IsotonicRegression;
use pav_regression::Point;
use serde::Serialize;
use std::collections::{HashMap, VecDeque};

const MIN_POINTS_FOR_REGRESSION: usize = 5;

/// Maximum number of raw data points retained by the global regression.
/// Once reached, each new point evicts the oldest via `remove_points`.
const MAX_REGRESSION_POINTS: usize = 500;

/// EWMA smoothing factor for per-peer adjustments.
/// Alpha = 0.1 gives a half-life of ~6.6 events, meaning the influence of an
/// observation drops below 50% after about 7 newer observations.
const EWMA_ALPHA: f64 = 0.1;

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
    /// Raw input points in insertion order. When len exceeds
    /// `MAX_REGRESSION_POINTS`, the oldest is evicted via `remove_points`.
    #[serde(skip)]
    raw_points: VecDeque<Point<f64>>,
}

impl IsotonicEstimator {
    // Minimum sample size before we apply per-peer adjustments; keeps peer curves from being
    // dominated by sparse/noisy data.
    const ADJUSTMENT_PRIOR_SIZE: u64 = 10;

    /// Creates a new `IsotonicEstimator` from a list of historical events.
    pub fn new<I>(history: I, estimator_type: EstimatorType) -> Self
    where
        I: IntoIterator<Item = IsotonicEvent>,
    {
        let mut all_points = VecDeque::new();
        let mut peer_events: HashMap<PeerKeyLocation, Vec<IsotonicEvent>> = HashMap::new();

        for event in history {
            let point = Point::new(event.route_distance().as_f64(), event.result);
            all_points.push_back(point);
            peer_events
                .entry(event.peer.clone())
                .or_default()
                .push(event);
        }

        // If history exceeds the window, keep only the most recent points.
        while all_points.len() > MAX_REGRESSION_POINTS {
            all_points.pop_front();
        }

        let points_vec: Vec<Point<f64>> = all_points.iter().cloned().collect();

        let global_regression = match estimator_type {
            EstimatorType::Positive => IsotonicRegression::new_ascending(&points_vec),
            EstimatorType::Negative => IsotonicRegression::new_descending(&points_vec),
        }
        .expect("Failed to create isotonic regression");

        let adjustment_prior_size = Self::ADJUSTMENT_PRIOR_SIZE;
        let global_regression_big_enough =
            global_regression.len() >= adjustment_prior_size as usize;

        let mut peer_adjustments: HashMap<PeerKeyLocation, Adjustment> = HashMap::new();

        if global_regression_big_enough {
            for (peer_location, events) in peer_events.iter() {
                let mut adjustment = Adjustment::new();
                // Seed with ADJUSTMENT_PRIOR_SIZE phantom neutral observations
                // so peers with few real observations are shrunk toward zero.
                adjustment.effective_count = adjustment_prior_size as f64;

                for event in events {
                    let global_estimate = global_regression
                        .interpolate(event.route_distance().as_f64())
                        .expect("Regression should always produce an estimate");
                    let delta = event.result - global_estimate;
                    adjustment.add(delta);
                }
                peer_adjustments.insert(peer_location.clone(), adjustment);
            }
        }

        IsotonicEstimator {
            global_regression,
            peer_adjustments,
            raw_points: all_points,
        }
    }

    /// Adds a new event to the estimator.
    pub fn add_event(&mut self, event: IsotonicEvent) {
        let route_distance = event.route_distance();
        let point = Point::new(route_distance.as_f64(), event.result);

        // Add the new point to the regression and raw-point FIFO.
        self.global_regression.add_points(&[point]);
        self.raw_points.push_back(point);

        // Evict the oldest point if the window is full.
        if self.raw_points.len() > MAX_REGRESSION_POINTS {
            if let Some(oldest) = self.raw_points.pop_front() {
                self.global_regression.remove_points(&[oldest]);
            }
        }

        let adjustment_prior_size = Self::ADJUSTMENT_PRIOR_SIZE;
        if self.global_regression.len() >= adjustment_prior_size as usize {
            let adjustment = event.result
                - self
                    .global_regression
                    .interpolate(route_distance.as_f64())
                    .unwrap();

            self.peer_adjustments
                .entry(event.peer)
                .or_default()
                .add(adjustment);
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

        // Regression can sometimes produce negative estimates
        let global_estimate = global_estimate.max(0.0);

        Ok(self
            .peer_adjustments
            .get(peer)
            .map_or(global_estimate, |peer_adjustment| {
                let should_use_peer_adjustment =
                    peer_adjustment.effective_count >= MIN_POINTS_FOR_REGRESSION as f64;
                global_estimate
                    + if should_use_peer_adjustment {
                        peer_adjustment.value()
                    } else {
                        0.0
                    }
            }))
    }

    pub(crate) fn len(&self) -> usize {
        self.global_regression.len()
    }

    /// Extract the global regression curve as `(x, y)` pairs, sorted by x.
    pub(crate) fn curve_points(&self) -> Vec<(f64, f64)> {
        self.global_regression
            .get_points_sorted()
            .into_iter()
            .map(|p| (*p.x(), *p.y()))
            .collect()
    }
}

#[derive(Debug, Clone)]
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

    fn value(&self) -> f64 {
        self.smoothed
    }

    /// EWMA smoothed adjustment value.
    pub(crate) fn mean(&self) -> f64 {
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
            estimator.raw_points.len() <= MAX_REGRESSION_POINTS,
            "Raw points should be bounded, got {}",
            estimator.raw_points.len()
        );

        let result = estimator.estimate_retrieval_time(&peer, contract);
        assert!(
            result.is_ok(),
            "Estimator should produce estimates after eviction"
        );
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

    fn simulate_positive_request(
        peer: PeerKeyLocation,
        contract_location: Location,
    ) -> IsotonicEvent {
        let distance: f64 = peer
            .location()
            .unwrap()
            .distance(contract_location)
            .as_f64();

        let key_hash = {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            format!("{}", peer.pub_key()).hash(&mut hasher);
            hasher.finish()
        };
        let result = distance.powf(0.5) + (key_hash as u8) as f64;
        IsotonicEvent {
            peer,
            contract_location,
            result,
        }
    }

    fn simulate_negative_request(
        peer: PeerKeyLocation,
        contract_location: Location,
    ) -> IsotonicEvent {
        let distance: f64 = peer
            .location()
            .unwrap()
            .distance(contract_location)
            .as_f64();

        let key_hash = {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            format!("{}", peer.pub_key()).hash(&mut hasher);
            hasher.finish()
        };
        let result = (100.0 - distance).powf(0.5) + (key_hash as u8) as f64;
        IsotonicEvent {
            peer,
            contract_location,
            result,
        }
    }
}
