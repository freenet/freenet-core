mod routing_outcome_estimator;

use std::time::Duration;

use locutus_core::{
    ring::{Distance, PeerKeyLocation},
    Location,
};
use routing_outcome_estimator::{PeerOutcomeEstimator, PeerRoutingEvent};
use serde::Serialize;

pub struct Router {
    pub time_estimator: PeerOutcomeEstimator,
    pub success_estimator: PeerOutcomeEstimator,
}

impl Router {
    pub fn new<I>(history: &[RouteEvent]) -> Self {
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

fn routing_prediction_to_expected_time(prediction : RoutingPrediction) -> Duration {
    let time_if_success = prediction.time.as_secs_f64();
    let failure_probability = prediction.failure_probability;
    /*
     * This is a fairly naive approach, assuming that the cost of a failure is a multiple of the cost of success.
     */
    let failure_cost_multiplier = 3.0;
    let expected_time = time_if_success + (time_if_success * failure_probability * failure_cost_multiplier);

    Duration::from_secs_f64(expected_time)
}

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

    use super::*;
/* 
    #[test]
    fn simulate_requests() {
        // Create 5 random peers and put them in an array
        let mut peers = vec![];
        for _ in 0..5 {
            let peer = PeerKeyLocation::random();
            peers.push(peer);
        }

        // Generate 50 random requests and use them to create a Router
        let mut history: Vec<RouteEvent> = vec![];
        for _ in 0..50 {
            let event = RouteEvent {
                peer: *peers.choose(&mut rand::thread_rng()).unwrap(),
                contract_location: Location::random(),
                outcome: RouteOutcome::Success {
                    time_to_response_start: Duration::from_millis(rand::random::<u64>() % 100),
                    payload_size: 100,
                    payload_transfer_time: Duration::from_millis(rand::random::<u64>() % 100),
                },
            };
        }
    } */
}
