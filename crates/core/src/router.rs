#![allow(dead_code)] // FIXME: remove this when this module is integrated with the rest of the codebase
mod isotonic_estimator;
mod util;

use crate::ring::{Location, PeerKeyLocation};
use isotonic_estimator::{EstimatorType, IsotonicEstimator, IsotonicEvent};
use serde::Serialize;
use std::{fmt, time::Duration};
use util::{Mean, TransferSpeed};

#[derive(Debug, Clone, Serialize)]
pub(crate) struct Router {
    response_start_time_estimator: IsotonicEstimator,
    transfer_rate_estimator: IsotonicEstimator,
    failure_estimator: IsotonicEstimator,
    mean_transfer_size: Mean,
}

impl Router {
    pub fn new(history: &[RouteEvent]) -> Self {
        let failure_outcomes: Vec<IsotonicEvent> = history
            .iter()
            .map(|re| IsotonicEvent {
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

        let success_durations: Vec<IsotonicEvent> = history
            .iter()
            .filter_map(|re| {
                if let RouteOutcome::Success {
                    time_to_response_start,
                    payload_size: _,
                    payload_transfer_time: _,
                } = re.outcome
                {
                    Some(IsotonicEvent {
                        peer: re.peer,
                        contract_location: re.contract_location,
                        result: time_to_response_start.as_secs_f64(),
                    })
                } else {
                    None
                }
            })
            .collect();

        let transfer_rates: Vec<IsotonicEvent> = history
            .iter()
            .filter_map(|re| {
                if let RouteOutcome::Success {
                    time_to_response_start: _,
                    payload_size,
                    payload_transfer_time,
                } = re.outcome
                {
                    Some(IsotonicEvent {
                        peer: re.peer,
                        contract_location: re.contract_location,
                        result: payload_size as f64 / payload_transfer_time.as_secs_f64(),
                    })
                } else {
                    None
                }
            })
            .collect();

        let mut mean_transfer_size = Mean::new();

        // Add some initial data so this produces sensible results with low or no historical data
        mean_transfer_size.add_with_count(1000.0, 10);

        for event in history {
            if let RouteOutcome::Success {
                time_to_response_start: _,
                payload_size,
                payload_transfer_time: _,
            } = event.outcome
            {
                mean_transfer_size.add(payload_size as f64);
            }
        }

        Router {
            // Positive because we expect time to increase as distance increases
            response_start_time_estimator: IsotonicEstimator::new(
                success_durations,
                EstimatorType::Positive,
            ),
            // Positive because we expect failure probability to increase as distance increase
            failure_estimator: IsotonicEstimator::new(failure_outcomes, EstimatorType::Positive),
            // Negative because we expect transfer rate to decrease as distance increases
            transfer_rate_estimator: IsotonicEstimator::new(
                transfer_rates,
                EstimatorType::Negative,
            ),
            mean_transfer_size,
        }
    }

    pub fn add_event(&mut self, event: RouteEvent) {
        match event.outcome {
            RouteOutcome::Success {
                time_to_response_start,
                payload_size,
                payload_transfer_time,
            } => {
                self.response_start_time_estimator.add_event(IsotonicEvent {
                    peer: event.peer,
                    contract_location: event.contract_location,
                    result: time_to_response_start.as_secs_f64(),
                });
                self.failure_estimator.add_event(IsotonicEvent {
                    peer: event.peer,
                    contract_location: event.contract_location,
                    result: 0.0,
                });
                let transfer_rate_event = IsotonicEvent {
                    peer: event.peer,
                    contract_location: event.contract_location,
                    result: payload_size as f64 / payload_transfer_time.as_secs_f64(),
                };
                self.mean_transfer_size.add(payload_size as f64);

                self.transfer_rate_estimator.add_event(transfer_rate_event);
            }
            RouteOutcome::Failure => {
                self.failure_estimator.add_event(IsotonicEvent {
                    peer: event.peer,
                    contract_location: event.contract_location,
                    result: 1.0,
                });
            }
        }
    }

    pub fn select_peer<'a>(
        &self,
        peers: impl IntoIterator<Item = &'a PeerKeyLocation>,
        contract_location: &Location,
    ) -> Option<&'a PeerKeyLocation> {
        if !self.has_sufficient_historical_data() {
            // Find the peer with the minimum distance to the contract location,
            // ignoring peers with no location
            peers
                .into_iter()
                .filter_map(|peer| {
                    peer.location
                        .map(|loc| (peer, contract_location.distance(loc)))
                })
                .min_by_key(|&(_, distance)| distance)
                .map(|(peer, _)| peer)
        } else {
            // Find the peer with the minimum predicted routing outcome time
            peers
                .into_iter()
                .map(|peer: &PeerKeyLocation| {
                    let t = self
                        .predict_routing_outcome(peer, contract_location)
                        .expect(
                            "Should always be Ok when has_sufficient_historical_data() is true",
                        );
                    (peer, t.time_to_response_start)
                })
                // Required because f64 doesn't implement Ord
                .min_by(|&(_, time1), &(_, time2)| {
                    time1
                        .partial_cmp(&time2)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .map(|(peer, _)| peer)
        }
    }

    fn predict_routing_outcome(
        &self,
        peer: &PeerKeyLocation,
        contract_location: &Location,
    ) -> Result<RoutingPrediction, RoutingError> {
        if !self.has_sufficient_historical_data() {
            return Err(RoutingError::InsufficientDataError);
        }

        let time_to_response_start_estimate = self
            .response_start_time_estimator
            .estimate_retrieval_time(peer, contract_location)
            .map_err(|e| {
                RoutingError::EstimationError(format!(
                    "Response Start Time Estimation failed: {}",
                    e
                ))
            })?;
        let failure_estimate = self
            .failure_estimator
            .estimate_retrieval_time(peer, contract_location)
            .map_err(|e| {
                RoutingError::EstimationError(format!("Failure Estimation failed: {}", e))
            })?;
        let transfer_rate_estimate = self
            .transfer_rate_estimator
            .estimate_retrieval_time(peer, contract_location)
            .map_err(|e| {
                RoutingError::EstimationError(format!("Transfer Rate Estimation failed: {}", e))
            })?;

        // This is a fairly naive approach, assuming that the cost of a failure is a multiple
        // of the cost of success.
        let failure_cost_multiplier = 3.0;

        let expected_total_time = time_to_response_start_estimate
            + (self.mean_transfer_size.compute() / transfer_rate_estimate)
            + (time_to_response_start_estimate * failure_estimate * failure_cost_multiplier);

        Ok(RoutingPrediction {
            failure_probability: failure_estimate,
            xfer_speed: TransferSpeed {
                bytes_per_second: transfer_rate_estimate,
            },
            time_to_response_start: time_to_response_start_estimate,
            expected_total_time,
        })
    }

    fn has_sufficient_historical_data(&self) -> bool {
        let minimum_historical_data_for_global_prediction = 200;
        self.response_start_time_estimator.len() >= minimum_historical_data_for_global_prediction
    }
}

#[derive(Debug)]
enum RoutingError {
    InsufficientDataError,
    EstimationError(String),
}

impl fmt::Display for RoutingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RoutingError::InsufficientDataError => write!(f, "Insufficient data provided"),
            RoutingError::EstimationError(err_msg) => write!(f, "Estimation error: {}", err_msg),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
struct RoutingPrediction {
    failure_probability: f64,
    xfer_speed: TransferSpeed,
    time_to_response_start: f64,
    expected_total_time: f64,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub(crate) struct RouteEvent {
    pub peer: PeerKeyLocation,
    pub contract_location: Location,
    pub outcome: RouteOutcome,
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
    use rand::Rng;

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
            // Pass a reference to the `peers` vector
            let best = router.select_peer(&peers, &contract_location).unwrap();
            let best_distance = best.location.unwrap().distance(contract_location);
            for peer in &peers {
                // Dereference `best` when making the comparison
                if *peer != *best {
                    let distance = peer.location.unwrap().distance(contract_location);
                    assert!(distance >= best_distance);
                }
            }
        }
    }

    #[test]
    fn test_request_time() {
        // Define constants for the number of peers, number of events, and number of test iterations.
        const NUM_PEERS: usize = 25;
        const NUM_EVENTS: usize = 40000;

        // Create `NUM_PEERS` random peers and put them in a vector.
        let peers: Vec<PeerKeyLocation> =
            (0..NUM_PEERS).map(|_| PeerKeyLocation::random()).collect();

        // Create NUM_EVENTS random events
        let mut events = vec![];
        let mut rng = rand::thread_rng();
        for _ in 0..NUM_EVENTS {
            let peer = peers[rng.gen_range(0..NUM_PEERS)];
            let contract_location = Location::random();
            let simulated_prediction = simulate_prediction(&mut rng, peer, contract_location);
            let event = RouteEvent {
                peer,
                contract_location,
                outcome: if rng.gen_range(0.0..1.0) > simulated_prediction.failure_probability {
                    RouteOutcome::Success {
                        time_to_response_start: Duration::from_secs_f64(
                            simulated_prediction.time_to_response_start,
                        ),
                        payload_size: 1000,
                        payload_transfer_time: Duration::from_secs_f64(
                            1000.0 / simulated_prediction.xfer_speed.bytes_per_second,
                        ),
                    }
                } else {
                    RouteOutcome::Failure
                },
            };
            events.push(event);
        }

        // Split events into two vectors, one for training and one for testing.
        let (training_events, testing_events) = events.split_at(NUM_EVENTS - 100);

        // Train the router with the training events.
        let router = Router::new(training_events);

        // Test the router with the testing events.
        for event in testing_events {
            let truth = simulate_prediction(&mut rng, event.peer, event.contract_location);

            let prediction = router
                .predict_routing_outcome(&event.peer, &event.contract_location)
                .unwrap();

            // Verify that the prediction is within 0.01 of the truth

            let response_start_time_error =
                (prediction.time_to_response_start - truth.time_to_response_start).abs();
            assert!(
                response_start_time_error < 0.01,
                "response_start_time: Prediction: {}, Truth: {}, Error: {}",
                prediction.time_to_response_start,
                truth.time_to_response_start,
                response_start_time_error
            );

            let failure_probability_error =
                (prediction.failure_probability - truth.failure_probability).abs();
            assert!(
                failure_probability_error < 0.3,
                "failure_probability: Prediction: {}, Truth: {}, Error: {}",
                prediction.failure_probability,
                truth.failure_probability,
                failure_probability_error
            );

            let transfer_speed_error =
                (prediction.xfer_speed.bytes_per_second - truth.xfer_speed.bytes_per_second).abs();
            assert!(
                transfer_speed_error < 0.01,
                "transfer_speed: Prediction: {}, Truth: {}, Error: {}",
                prediction.xfer_speed.bytes_per_second,
                truth.xfer_speed.bytes_per_second,
                transfer_speed_error
            );
        }
    }

    fn simulate_prediction(
        random: &mut rand::rngs::ThreadRng,
        peer: PeerKeyLocation,
        contract_location: Location,
    ) -> RoutingPrediction {
        let distance = peer.location.unwrap().distance(contract_location);
        let time_to_response_start = 2.0 * distance.as_f64();
        let failure_prob = distance.as_f64();
        let transfer_speed = 100.0 - (100.0 * distance.as_f64());
        let payload_size = random.gen_range(100..1000);
        let transfer_time = transfer_speed * (payload_size as f64);
        RoutingPrediction {
            failure_probability: failure_prob,
            xfer_speed: TransferSpeed {
                bytes_per_second: transfer_speed,
            },
            time_to_response_start,
            expected_total_time: time_to_response_start + transfer_time,
        }
    }
}
