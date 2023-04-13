mod routing_outcome_estimator;

use locutus_core::{ring::PeerKeyLocation, Location};
use rand::seq::SliceRandom;
use routing_outcome_estimator::{PeerOutcomeEstimator, PeerRoutingEvent};
use serde::Serialize;
use std::time::Duration;

#[derive(Debug, Clone, Serialize)]
pub struct Router {
    pub time_estimator: PeerOutcomeEstimator,
    pub success_estimator: PeerOutcomeEstimator,
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
            time_estimator: PeerOutcomeEstimator::new(success_durations),
            success_estimator: PeerOutcomeEstimator::new(success_outcomes),
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

fn routing_prediction_to_expected_time(prediction: RoutingPrediction) -> Duration {
    let time_if_success = prediction.time.as_secs_f64();
    let failure_probability = prediction.failure_probability;
    /*
     * This is a fairly naive approach, assuming that the cost of a failure is a multiple of the cost of success.
     */
    let failure_cost_multiplier = 3.0;
    let expected_time =
        time_if_success + (time_if_success * failure_probability * failure_cost_multiplier);

    Duration::from_secs_f64(expected_time)
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct RoutingPrediction {
    pub time: Duration,
    pub failure_probability: f64,
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
        // Create 5 random peers and put them in an array
        let mut peers = vec![];
        for _ in 0..25 {
            let peer = PeerKeyLocation::random();
            peers.push(peer);
        }

        // Create a router with no historical data
        let mut router = Router::new(&[]);

        // Add some events to the router
        for _ in 0..1000 {
            let contract_location = Location::random();

            let closest = router
                .select_peer(peers.as_slice(), contract_location)
                .unwrap();
            let closest_distance: Distance = closest.location.unwrap().distance(&contract_location);

            // Force the time to be proportional to the distance
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

        // Dump Router
        println!("Router success estimator global regression:\n{:#?}", router.success_estimator.global_regression);

        for _ in 0..100 {
            let contract_location = Location::random();
            let best = router.select_peer(&peers, contract_location).unwrap();
            let predicted_best = router.predict_routing_outcome(best, contract_location);
            let simulated_best_time =
                dist_to_time_simulated(best.location.unwrap().distance(&contract_location));
            let simulated_best_failure_prob =
                dist_to_failure_prob_simulated(best.location.unwrap().distance(&contract_location));

            println!(
                "Predicted best: {predicted_best:?}, simulated best time: {simulated_best_time:?}, simulated best failure prob: {simulated_best_failure_prob:?}");

            assert!((predicted_best.time.as_secs_f64() - simulated_best_time.as_secs_f64()).abs() < 0.01);

            // Get the peer that has the actual best time
            let mut best_peer: Option<(PeerKeyLocation, f64)> = None;
            for peer in &peers {
                let distance = peer.location.unwrap().distance(&contract_location);
                let failure_prob = dist_to_failure_prob_simulated(distance);
                let simulated_time = dist_to_time_simulated(distance).as_secs_f64() * (1.0 + failure_prob * 3.0);
                if best_peer.is_none() || simulated_time < best_peer.unwrap().1 {
                    best_peer = Some((*peer, simulated_time));
                }
            }

            assert_eq!(best, best_peer.unwrap().0);
            assert!((predicted_best.time.as_secs_f64() - best_peer.unwrap().1).abs() < 0.001);
        }
    }

    fn dist_to_failure_prob_simulated(dist: Distance) -> f64 {
        dist.as_f64() * 2.0
    }

    fn dist_to_time_simulated(dist: Distance) -> Duration {
        Duration::from_secs_f64(dist.as_f64() + 2.8)
    }
}
