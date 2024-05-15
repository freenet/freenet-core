use crate::ring::{Distance, Location, PeerKeyLocation};
use pav_regression::pav::{IsotonicRegression, Point};
use serde::Serialize;
use std::collections::HashMap;

const MIN_POINTS_FOR_REGRESSION: usize = 5;

/// `IsotonicEstimator`  provides outcome estimation for a given action, such as
/// retrieving the state of a contract, based on the distance between the peer
/// and the contract. It uses an isotonic regression model from the `pav.rs`
/// library to estimate the outcome based on the distance between the peer and
/// the contract, but then also tracks an adjustment for each peer based on the
/// outcome of the peer's previous requests.

#[derive(Debug, Clone, Serialize)]
pub(super) struct IsotonicEstimator {
    pub global_regression: IsotonicRegression,
    pub peer_adjustments: HashMap<PeerKeyLocation, Adjustment>,
}

impl IsotonicEstimator {
    // Define a constant for the adjustment prior size.
    const ADJUSTMENT_PRIOR_SIZE: u64 = 10;

    /// Creates a new `PeerOutcomeEstimator` from a list of historical events.
    pub fn new<I>(history: I, estimator_type: EstimatorType) -> Self
    where
        I: IntoIterator<Item = IsotonicEvent>,
    {
        let mut all_points = Vec::new();

        let mut peer_events: HashMap<PeerKeyLocation, Vec<IsotonicEvent>> = HashMap::new();

        for event in history {
            let point = Point::new(event.route_distance().as_f64(), event.result.clone());

            all_points.push(point);
            peer_events
                .entry(event.peer.clone())
                .or_default()
                .push(event);
        }

        let global_regression = match estimator_type {
            EstimatorType::Positive => IsotonicRegression::new_ascending(&all_points),
            EstimatorType::Negative => IsotonicRegression::new_descending(&all_points),
        };

        let adjustment_prior_size = 20;
        let global_regression_big_enough_to_estimate_peer_adjustments =
            global_regression.len() >= adjustment_prior_size;

        let mut peer_adjustments: HashMap<PeerKeyLocation, Adjustment> = HashMap::new();

        if global_regression_big_enough_to_estimate_peer_adjustments {
            // Use the constant defined earlier.
            let adjustment_prior_size = Self::ADJUSTMENT_PRIOR_SIZE;

            // Use more descriptive variable names.
            for (peer_location, events) in peer_events.iter() {
                let mut event_count: u64 = adjustment_prior_size;
                let mut total_adjustment: f64 = 0.0;
                for event in events {
                    let global_estimate_from_distance = global_regression
                        .interpolate(event.route_distance().as_f64())
                        .expect("Regression should always produce an estimate");
                    let peer_adjustment = event.result - global_estimate_from_distance;

                    event_count += 1;
                    total_adjustment += peer_adjustment;
                }
                peer_adjustments.insert(
                    peer_location.clone(),
                    Adjustment {
                        sum: total_adjustment,
                        count: event_count,
                    },
                );
            }
        }

        IsotonicEstimator {
            global_regression,
            peer_adjustments,
        }
    }

    /// Adds a new event to the estimator.
    pub fn add_event(&mut self, event: IsotonicEvent) {
        let route_distance = event.route_distance();

        let point = Point::new(route_distance.as_f64(), event.result);

        self.global_regression.add_points(&[point]);

        let adjustment_prior_size = 20;
        let global_regression_big_enough_to_estimate_peer_adjustments =
            self.global_regression.len() >= adjustment_prior_size;

        if global_regression_big_enough_to_estimate_peer_adjustments {
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
        // Check if there are enough data points that the model won't produce
        // garbage output, but users of this class must implement their own checks
        // to ensure that the model is sufficiently accurate as this is an
        // extremely low bar.
        if self.global_regression.len() < MIN_POINTS_FOR_REGRESSION {
            return Err(EstimationError::InsufficientData);
        }

        let distance: f64 = contract_location.distance(peer.location.unwrap()).as_f64();

        let global_estimate = self.global_regression.interpolate(distance).unwrap();

        // Regression can sometimes produce negative estimates
        let global_estimate = global_estimate.max(0.0);

        Ok(self
            .peer_adjustments
            .get(peer)
            .map_or(global_estimate, |peer_adjustment| {
                let should_use_peer_adjustment =
                    peer_adjustment.count >= MIN_POINTS_FOR_REGRESSION as u64;
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
}

pub(super) enum EstimatorType {
    /// Where the estimated value is expected to increase as distance increases
    Positive,
    /// Where the estimated value is expected to decrease as distance increases
    Negative,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub(super) enum EstimationError {
    #[error("Insufficient data for estimation")]
    InsufficientData,
}

/// A routing event is a single request to a peer for a contract, and some value indicating
/// the result of the request, such as the time it took to retrieve the contract.
#[derive(Debug, Clone)]
pub(super) struct IsotonicEvent {
    pub peer: PeerKeyLocation,
    pub contract_location: Location,
    /// The result of the routing event, which is used to train the estimator, typically the time
    /// but could also represent request success as 0.0 and failure as 1.0, and then be used
    /// to predict the probability of success.
    pub result: f64,
}

impl IsotonicEvent {
    fn route_distance(&self) -> Distance {
        self.contract_location.distance(self.peer.location.unwrap())
    }
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct Adjustment {
    sum: f64,
    count: u64,
}

impl Default for Adjustment {
    fn default() -> Self {
        Self::new()
    }
}

impl Adjustment {
    fn new() -> Self {
        Self { sum: 0.0, count: 0 }
    }

    fn add(&mut self, value: f64) {
        self.sum += value;
        self.count += 1;
    }

    fn value(&self) -> f64 {
        self.sum / self.count as f64
    }
}

// Tests

#[cfg(test)]
mod tests {

    use super::*;

    // This test `test_peer_time_estimator` checks the accuracy of the `RoutingOutcomeEstimator` struct's
    // `estimate_retrieval_time()` method. It generates a list of 100 random events, where each event
    // represents a simulated request made by a random `PeerId` at a random `Location` to retrieve data
    // from a contract at another random `Location`. Each event is created by calling the `simulate_request()`
    // helper function which calculates the distance between the `Peer` and the `Contract`, then estimates
    // the retrieval time based on the distance and some random factor. The list of events is then split
    // into two sets: a training set and a testing set.
    //
    // The `RoutingOutcomeEstimator` is then instantiated using the training set, and the `estimate_retrieval_time()`
    // method is called for each event in the testing set. The estimated retrieval time is compared to the
    // actual retrieval time recorded in the event, and the error between the two is calculated. The average
    // error across all events is then calculated, and the test passes if the average error is less than 0.01.
    // If the error is greater than or equal to 0.01, the test fails.

    #[test]
    fn test_positive_peer_time_estimator() {
        // Generate a list of random events
        let mut events = Vec::new();
        for _ in 0..100 {
            let peer = PeerKeyLocation::random();
            if peer.location.is_none() {
                println!("Peer location is none for {peer:?}");
            }
            let contract_location = Location::random();
            events.push(simulate_positive_request(peer, contract_location));
        }

        // Split the events into two sets
        let (training_events, testing_events) = events.split_at(events.len() / 2);

        // Create a new estimator from the training set
        let estimator =
            IsotonicEstimator::new(training_events.iter().cloned(), EstimatorType::Positive);

        // Test the estimator on the testing set, recording the errors
        let mut errors = Vec::new();
        for event in testing_events {
            let estimated_time = estimator
                .estimate_retrieval_time(&event.peer, event.contract_location)
                .unwrap();
            let actual_time = event.result;
            let error = (estimated_time - actual_time).abs();
            errors.push(error);
        }

        // Check that the errors are small
        let average_error = errors.iter().sum::<f64>() / errors.len() as f64;
        println!("Average error: {}", average_error);
        assert!(average_error < 0.01);
    }

    #[test]
    fn test_negative_peer_time_estimator() {
        // Generate a list of random events
        let mut events = Vec::new();
        for _ in 0..100 {
            let peer = PeerKeyLocation::random();
            if peer.location.is_none() {
                println!("Peer location is none for {peer:?}");
            }
            let contract_location = Location::random();
            events.push(simulate_negative_request(peer, contract_location));
        }

        // Split the events into two sets
        let (training_events, testing_events) = events.split_at(events.len() / 2);

        // Create a new estimator from the training set
        let estimator =
            IsotonicEstimator::new(training_events.iter().cloned(), EstimatorType::Negative);

        // Test the estimator on the testing set, recording the errors
        let mut errors = Vec::new();
        for event in testing_events {
            let estimated_time = estimator
                .estimate_retrieval_time(&event.peer, event.contract_location)
                .unwrap();
            let actual_time = event.result;
            let error = (estimated_time - actual_time).abs();
            errors.push(error);
        }

        // Check that the errors are small
        let average_error = errors.iter().sum::<f64>() / errors.len() as f64;
        println!("Average error: {}", average_error);
        assert!(average_error < 0.01);
    }

    fn simulate_positive_request(
        peer: PeerKeyLocation,
        contract_location: Location,
    ) -> IsotonicEvent {
        let distance: f64 = peer
            .location
            .clone()
            .unwrap()
            .distance(contract_location)
            .as_f64();

        let result = distance.powf(0.5) + peer.peer.clone().to_bytes()[0] as f64;
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
            .location
            .clone()
            .unwrap()
            .distance(contract_location)
            .as_f64();

        let result = (100.0 - distance).powf(0.5) + peer.peer.clone().to_bytes()[0] as f64;
        IsotonicEvent {
            peer,
            contract_location,
            result,
        }
    }
}
