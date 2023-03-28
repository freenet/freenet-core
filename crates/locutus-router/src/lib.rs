use std::{collections::HashMap, time::Duration};

use locutus_core::{libp2p::PeerId, Location};
use pav_regression::{
    pav::{IsotonicRegression, Point},
};

const MIN_PEER_POINTS_FOR_REGRESSION: usize = 10;

pub struct PeerTimeEstimator {
    global_regression: IsotonicRegression,
    peer_regressions: HashMap<PeerId, IsotonicRegression>,
}

impl PeerTimeEstimator {
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
            peer_points.entry(event.peer).or_insert_with(Vec::new).push(point);
        }

        let global_regression = IsotonicRegression::new_ascending(all_points.as_slice());

        let peer_regressions = peer_points
            .into_iter()
            .filter(|(_, points)| points.len() > MIN_PEER_POINTS_FOR_REGRESSION)
            .map(|(peer, points)| {
                let regression = IsotonicRegression::new_ascending(points.as_slice());
                (peer, regression)
            })
            .collect();

        PeerTimeEstimator {
            global_regression,
            peer_regressions,
        }
    }

    pub fn add_event(&mut self, event: RoutingEvent) {
        let point = Point::new(
            event.peer_location.distance(&event.contract_location).into(),
            event.result,
        );

        self.global_regression.add_points(&[point]);

        if let Some(regression) = self.peer_regressions.get_mut(&event.peer) {
            regression.add_points(&[point]);
        } else {
            let regression = IsotonicRegression::new_ascending(&[point]);
            self.peer_regressions.insert(event.peer, regression);
        }
    }

    pub fn estimate_retrieval_time(&self, peer: PeerId, distance: f64) -> Option<f64> {
        if let Some(regression) = self.peer_regressions.get(&peer) {
            let y = regression.interpolate(distance);
            Some(y)
        } else if self.global_regression.len() > MIN_PEER_POINTS_FOR_REGRESSION {
            let y = self.global_regression.interpolate(distance);
            Some(y)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct RoutingEvent {
    peer: PeerId,
    peer_location: Location,
    contract_location: Location,
    result: f64,
}

// Tests

#[cfg(test)]
mod tests {

    use super::*;

    fn simulate_request(peer: PeerId, peer_location: Location, contract_location: Location) -> RoutingEvent {
        let distance : f64 = peer_location.distance(&contract_location).into();

        let result = distance.powf(0.5) + peer.to_bytes()[0] as f64;
        let ev = RoutingEvent {
            peer,
            peer_location,
            contract_location,
            result,
        };
        println!("Event: {ev:?}");
        ev
    }

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
        let (training_events, testing_events) = events.split_at(events.len()/2);

        // Create a new estimator from the training set
        let estimator = PeerTimeEstimator::new(training_events.iter().cloned());

        // Test the estimator on the testing set, recording the errors
        let mut errors = Vec::new();
        for event in testing_events {
            let estimated_time = estimator.estimate_retrieval_time(event.peer, event.contract_location.distance(&event.peer_location).into());
            assert!(estimated_time.is_some());
            let estimated_time = estimated_time.unwrap();
            let actual_time = event.result;
            // print estimated and actual
            println!("Estimated: {estimated_time:?}, Actual: {actual_time:?}");
            let error = (estimated_time - actual_time).abs();
        }
 
        println!("Error: {errors:?}"); 

        // Check that the errors are small
        let average_error = errors.iter().sum::<i64>() as f64 / errors.len() as f64;
        assert!(average_error < -100.0);

    }
}
