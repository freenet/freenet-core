use std::collections::HashMap;
use locutus_core::{libp2p::PeerId, Location};
use pav_regression::pav::{IsotonicRegression, Point};

const MIN_POINTS_FOR_REGRESSION: usize = 10;

/// `RoutingOutcomeEstimator` is a Rust struct that provides outcome estimation for a given action,
/// such as retrieving the state of a contract, based on the distance between the peer and the contract.
/// It uses an isotonic regression model from the `pav.rs` library to predict the outcome, represented
/// as an `f64` where lower values indicate better outcomes and higher values indicate worse outcomes.
///
/// The estimator is trained on historical data and assumes that the outcome is proportional to the
/// distance between the peer and the contract, with longer distances resulting in worse outcomes,
/// assuming all other factors are equal. Each peer has its own regression model, which is used only
/// if it has enough data points for accurate predictions. Otherwise, the global regression model
/// is used.

pub struct RoutingOutcomeEstimator {
    global_regression: IsotonicRegression,
    peer_regressions: HashMap<PeerId, IsotonicRegression>,
}

impl RoutingOutcomeEstimator {
    /// Creates a new `PeerTimeEstimator` from a list of historical events.
    pub fn new<I>(history: I) -> Self
    where
        I: IntoIterator<Item = RoutingEvent>,
    {
        let mut all_points = Vec::new();
        let mut peer_points: HashMap<PeerId, Vec<Point>> = HashMap::new();

        for event in history {
            let point = Point::new(
                event.peer_location.distance(&event.contract_location).into(),
                event.result,
            );

            all_points.push(point);
            peer_points.entry(event.peer).or_default().push(point);
        }

        let global_regression = IsotonicRegression::new_ascending(&all_points);

        let peer_regressions = peer_points
            .into_iter()
            .map(|(peer, points)| {
                let regression = IsotonicRegression::new_ascending(&points);
                (peer, regression)
            })
            .collect();

        RoutingOutcomeEstimator {
            global_regression,
            peer_regressions,
        }
    }

    /// Adds a new event to the estimator.
    pub fn add_event(&mut self, event: RoutingEvent) {
        let point = Point::new(
            event.peer_location.distance(&event.contract_location).into(),
            event.result,
        );

        self.global_regression.add_points(&[point]);

        self.peer_regressions
            .entry(event.peer)
            .or_insert_with(|| IsotonicRegression::new_ascending(&[point]))
            .add_points(&[point]);
    }

    pub fn estimate_retrieval_time(&self, peer: PeerId, distance: f64) -> Option<f64> {
        if let Some(regression) = self.peer_regressions.get(&peer) {
            if regression.len() > MIN_POINTS_FOR_REGRESSION {
                return Some(regression.interpolate(distance));
            }
        }
    
        if self.global_regression.len() > MIN_POINTS_FOR_REGRESSION {
            return Some(self.global_regression.interpolate(distance));
        }
    
        None
    }
    
}

/// A routing event is a single request to a peer for a contract, and some value indicating
/// the result of the request, such as the time it took to retrieve the contract.
#[derive(Debug, Clone)]
pub struct RoutingEvent {
    peer: PeerId,
    peer_location: Location,
    contract_location: Location,
    /// The result of the routing event, which is used to train the estimator, typically the time
    /// but could also represent request success as 0.0 and failure as 1.0, and then be used
    /// to predict the probability of success.
    result: f64,
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
    fn test_peer_time_estimator() {
        // Generate a list of random events
        let mut events = Vec::new();
        for _ in 0..100 {
            let peer = PeerId::random();
            let peer_location = Location::random();
            let contract_location = Location::random();
            events.push(simulate_request(peer, peer_location, contract_location));
        }

        // Split the events into two sets
        let (training_events, testing_events) = events.split_at(events.len() / 2);

        // Create a new estimator from the training set
        let estimator = RoutingOutcomeEstimator::new(training_events.iter().cloned());
        
        // Test the estimator on the testing set, recording the errors
        let mut errors = Vec::new();
        for event in testing_events {
            let distance = event.contract_location.distance(&event.peer_location).into();
            let estimated_time = estimator.estimate_retrieval_time(event.peer, distance);
            assert!(estimated_time.is_some());
            let estimated_time = estimated_time.unwrap();
            let actual_time = event.result;
            let error = (estimated_time - actual_time).abs();
            errors.push(error);
        }

        // Check that the errors are small
        let average_error = errors.iter().sum::<f64>() / errors.len() as f64;
        assert!(average_error < 0.01);
    }

    fn simulate_request(peer: PeerId, peer_location: Location, contract_location: Location) -> RoutingEvent {
        let distance: f64 = peer_location.distance(&contract_location).into();

        let result = distance.powf(0.5) + peer.to_bytes()[0] as f64;
        RoutingEvent {
            peer,
            peer_location,
            contract_location,
            result,
        }
    }
}
