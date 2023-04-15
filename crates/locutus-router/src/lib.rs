mod isotonic_estimator;

use locutus_core::{ring::PeerKeyLocation, Location};
use isotonic_estimator::{IsotonicEstimator, PeerRoutingEvent};
use serde::Serialize;
use std::time::Duration;

#[derive(Debug, Clone, Serialize)]
pub struct Router {
    pub time_estimator: IsotonicEstimator,
    pub transfer_rate_estimator : IsotonicEstimator,
    pub success_estimator: IsotonicEstimator,
}

impl Router {
    pub fn new(history: &[RouteEvent]) -> Self {
        let success_outcomes: Vec<PeerRoutingEvent> = history
            .iter()
            .map(|re| PeerRoutingEvent {
                peer: re.peer,
                contract_location: re.contract_location,
                result: match re.outcome {
                    RouteOutcome::Success {
                        time_to_response_start: _,
                        payload_size: _,
                        payload_transfer_time: _,
                    } => 0.0,
                    RouteOutcome::Failure => 1.0,
                },
            })
            .collect();

        let success_durations: Vec<PeerRoutingEvent> = history
            .iter()
            .filter_map(|re| {
                if let RouteOutcome::Success {
                    time_to_response_start,
                    payload_size: _,
                    payload_transfer_time: _,
                } = re.outcome
                {
                    Some(PeerRoutingEvent {
                        peer: re.peer,
                        contract_location: re.contract_location,
                        result: time_to_response_start.as_secs_f64(),
                    })
                } else {
                    None
                }
            })
            .collect();

        Router {
            time_estimator: IsotonicEstimator::new(success_durations),
            success_estimator: IsotonicEstimator::new(success_outcomes),
        }
    }

    pub fn add_event(&mut self, event: RouteEvent) {
        match event.outcome {
            RouteOutcome::Success {
                time_to_response_start,
                payload_size: _,
                payload_transfer_time: _,
            } => {
                self.time_estimator.add_event(PeerRoutingEvent {
                    peer: event.peer,
                    contract_location: event.contract_location,
                    result: time_to_response_start.as_secs_f64(),
                });
                self.success_estimator.add_event(PeerRoutingEvent {
                    peer: event.peer,
                    contract_location: event.contract_location,
                    result: 0.0,
                });
            }
            RouteOutcome::Failure => {
                self.success_estimator.add_event(PeerRoutingEvent {
                    peer: event.peer,
                    contract_location: event.contract_location,
                    result: 1.0,
                });
            }
        }
    }

    pub fn select_peer(
        &self,
        peers: &[PeerKeyLocation],
        contract_location: Location,
    ) -> Option<PeerKeyLocation> {
        if !self.has_sufficient_historical_data() {
            // Find the peer with the minimum distance to the contract location
            return peers
                .iter()
                .filter_map(|peer| {
                    peer.location
                        .map(|loc| (peer, contract_location.distance(&loc)))
                })
                .min_by_key(|&(_, distance)| distance)
                .map(|(peer, _)| *peer);
        }

        // Find the peer with the minimum predicted routing outcome time
        // TODO: Should also consider success probability
        let best_peer = peers
            .iter()
            .map(|peer: &PeerKeyLocation| {
                let t = self.predict_routing_outcome(*peer, contract_location);
                (peer, routing_prediction_to_expected_time(t))
            })
            .min_by_key(|&(_, time)| time)
            .map(|(peer, _)| *peer);

        best_peer
    }

    pub fn predict_routing_outcome(
        &self,
        peer: PeerKeyLocation,
        contract_location: Location,
    ) -> RoutingPrediction {
        let time_estimate = self
            .time_estimator
            .estimate_retrieval_time(&peer, contract_location);
        let success_estimate = self
            .success_estimator
            .estimate_retrieval_time(&peer, contract_location);

        RoutingPrediction {
            time: Duration::from_secs_f64(time_estimate.unwrap()),
            failure_probability: success_estimate.unwrap(),
        }
    }

    fn has_sufficient_historical_data(&self) -> bool {
        let minimum_historical_data_for_global_prediction = 200;
        self.time_estimator.len() >= minimum_historical_data_for_global_prediction
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct RoutingPrediction {
    pub time: Duration,
    pub failure_probability: f64,
    pub xfer_speed: TransferSpeed,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct TransferSpeed {bytes_per_second : f64 }

impl TransferSpeed {
    pub fn new(bytes : usize, duration: Duration) -> Self {
        TransferSpeed { bytes_per_second : bytes as f64 / duration.as_secs_f64() }
    }
}

impl RoutingPrediction {
    fn to_expected_time(&self) -> Duration {
        /*
         * This is a fairly naive approach, assuming that the cost of a failure is a multiple of the cost of success
         * and that 1000 bytes are transferred.
         */
        let failure_cost_multiplier = 3.0;
        let expected_bytes_transferred = 1000.0;
        let expected_time =
            self.time.as_secs_f64() + 
            (expected_bytes_transferred / self.xfer_speed.bytes_per_second) +
                (self.time.as_secs_f64() * self.failure_probability * failure_cost_multiplier);
    
        Duration::from_secs_f64(expected_time)
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct RouteEvent {
    peer: PeerKeyLocation,
    contract_location: Location,
    outcome: RouteOutcome,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub enum RouteOutcome {
    Success {
        time_to_response_start: Duration,
        payload_size: usize,
        payload_transfer_time: Duration,
    },
    Failure,
}

#[cfg(test)]
mod tests {

    use locutus_core::ring::Distance;

    use super::*;

    #[test]
    fn before_data_select_closest() {
        // Create 5 random peers and put them in an array
        let mut peers = vec![];
        for _ in 0..5 {
            let peer = PeerKeyLocation::random();
            peers.push(peer);
        }

        // Create a router with no historical data
        let router = Router::new(&[]);

        for _ in 0..10 {
            let contract_location = Location::random();
            let best = router.select_peer(&peers, contract_location).unwrap();
            let best_distance = best.location.unwrap().distance(&contract_location);
            for peer in &peers {
                if peer != &best {
                    let distance = peer.location.unwrap().distance(&contract_location);
                    assert!(distance >= best_distance);
                }
            }
        }
    }

    #[test]
    fn test_request_time() {
        // Define constants for the number of peers, number of events, and number of test iterations.
        const NUM_PEERS: usize = 25;
        const NUM_EVENTS: usize = 1000;
        const NUM_TEST_ITERATIONS: usize = 100;
    
        // Create `NUM_PEERS` random peers and put them in a vector.
        let peers: Vec<PeerKeyLocation> = (0..NUM_PEERS).map(|_| PeerKeyLocation::random()).collect();
    
        // Create a router with no historical data.
        let mut router = Router::new(&[]);
    
        // Add `NUM_EVENTS` events to the router.
        for _ in 0..NUM_EVENTS {
            let contract_location = Location::random();
            let closest = router.select_peer(&peers, contract_location).unwrap();
            let closest_distance = closest.location.unwrap().distance(&contract_location);
    
            // Force the time to be proportional to the distance.
            let time = dist_to_time_simulated(closest_distance);
            let failure_prob = dist_to_failure_prob_simulated(closest_distance);
    
            let outcome = if rand::random::<f64>() < failure_prob {
                RouteOutcome::Failure
            } else {
                RouteOutcome::Success {
                    time_to_response_start: time,
                    payload_size: 100,
                    payload_transfer_time: Duration::from_secs_f64(1.0),
                }
            };
    
            router.add_event(RouteEvent {
                peer: closest,
                contract_location,
                outcome,
            });
        }
    
        // Test the router's prediction accuracy for `NUM_TEST_ITERATIONS` iterations.
        for _ in 0..NUM_TEST_ITERATIONS {
            let contract_location = Location::random();
            let best = router.select_peer(&peers, contract_location).unwrap();
            let predicted_best = router.predict_routing_outcome(best, contract_location);
            let simulated_best_time =
                dist_to_time_simulated(best.location.unwrap().distance(&contract_location));
            let simulated_best_failure_prob =
                dist_to_failure_prob_simulated(best.location.unwrap().distance(&contract_location));
    
            // Assert that the predicted time is close to the simulated time.
            assert!(
                (predicted_best.time.as_secs_f64() - simulated_best_time.as_secs_f64()).abs() < 0.1
            );
    
            // Get the peer that has the actual best time.
            let mut best_peer: Option<(PeerKeyLocation, f64)> = None;
            for peer in &peers {
                let distance = peer.location.unwrap().distance(&contract_location);
                let failure_prob = dist_to_failure_prob_simulated(distance);
                let simulated_time = dist_to_time_simulated(distance).as_secs_f64() * (1.0 + failure_prob * 3.0);
                if best_peer.map_or(true, |(_, time)| simulated_time < time) {
                    best_peer = Some((*peer, simulated_time));
                }
            }
    
            let predicted_best_score = predicted_best.time.as_secs_f64() * (1.0 + simulated_best_failure_prob * 3.0);

            let (best_peer, best_peer_time_provided) = best_peer.unwrap();
            let best_peer_failure_prob = dist_to_failure_prob_simulated(best_peer.location.unwrap().distance(&contract_location));
            let best_peer_score_provided = best_peer_time_provided * (1.0 + best_peer_failure_prob * 3.0);
    
            // Assert that the predicted best score is close to the provided best score.
            assert!(
                (predicted_best_score - best_peer_score_provided).abs() < 0.001,
                "Actual pred best score: {:?}, provided pred best score: {:?}",
                predicted_best_score,
                best_peer_score_provided
            );
        }
    }
    

    fn dist_to_failure_prob_simulated(dist: Distance) -> f64 {
        dist.as_f64() * 2.0
    }

    fn dist_to_time_simulated(dist: Distance) -> Duration {
        Duration::from_secs_f64(dist.as_f64() + 2.8)
    }
}
