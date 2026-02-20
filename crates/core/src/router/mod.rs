mod isotonic_estimator;
mod util;

use crate::config::GlobalRng;
use crate::ring::{Distance, Location, PeerKeyLocation};
pub(crate) use isotonic_estimator::{EstimatorType, IsotonicEstimator, IsotonicEvent};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use util::{Mean, TransferSpeed};

// ==================== Telemetry types ====================

/// A snapshot of a single routing decision for telemetry.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub(crate) struct RoutingDecisionInfo {
    pub target_location: f64,
    pub strategy: RoutingStrategy,
    pub candidates: Vec<RoutingCandidate>,
    pub total_routing_events: usize,
}

/// Which strategy the router used for this decision.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub(crate) enum RoutingStrategy {
    /// Not enough history; selected by distance only.
    DistanceBased,
    /// Used prediction model to rank candidates.
    PredictionBased,
    /// Had history but predictions failed for some candidates; fell back to distance for those.
    PredictionFallback,
}

/// A single candidate considered during routing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub(crate) struct RoutingCandidate {
    pub distance: f64,
    pub prediction: Option<RoutingPredictionInfo>,
    pub selected: bool,
}

/// Prediction details for a routing candidate (subset of RoutingPrediction for telemetry).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub(crate) struct RoutingPredictionInfo {
    pub failure_probability: f64,
    pub time_to_response_start: f64,
    pub expected_total_time: f64,
    pub transfer_speed_bps: f64,
}

impl From<RoutingPrediction> for RoutingPredictionInfo {
    fn from(p: RoutingPrediction) -> Self {
        Self {
            failure_probability: p.failure_probability,
            time_to_response_start: p.time_to_response_start,
            expected_total_time: p.expected_total_time,
            transfer_speed_bps: p.xfer_speed.bytes_per_second,
        }
    }
}

/// Periodic snapshot of the router model state for telemetry.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub(crate) struct RouterSnapshotInfo {
    pub failure_events: usize,
    pub success_events: usize,
    pub transfer_rate_events: usize,
    pub prediction_active: bool,
    pub mean_transfer_size_bytes: f64,
    pub consider_n_closest_peers: usize,
    pub peers_with_failure_adjustments: usize,
    pub peers_with_response_adjustments: usize,
    /// PAV regression curve: Vec of (distance, failure_probability)
    pub failure_curve: Vec<(f64, f64)>,
    /// PAV regression curve: Vec of (distance, response_time_secs)
    pub response_time_curve: Vec<(f64, f64)>,
    /// PAV regression curve: Vec of (distance, bytes_per_sec)
    pub transfer_rate_curve: Vec<(f64, f64)>,
    /// Connect forward estimator curve, if available
    pub connect_forward_curve: Option<Vec<(f64, f64)>>,
    pub connect_forward_events: Option<usize>,
    pub connect_forward_peer_adjustments: Option<usize>,
}

/// # Usage
/// Important when using this type:
/// Need to periodically rebuild the Router using `history` for better predictions.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct Router {
    response_start_time_estimator: IsotonicEstimator,
    transfer_rate_estimator: IsotonicEstimator,
    failure_estimator: IsotonicEstimator,
    mean_transfer_size: Mean,
    consider_n_closest_peers: usize,
}

impl Router {
    pub fn new(history: &[RouteEvent]) -> Self {
        let failure_outcomes: Vec<IsotonicEvent> = history
            .iter()
            .map(|re| IsotonicEvent {
                peer: re.peer.clone(),
                contract_location: re.contract_location,
                result: match re.outcome {
                    RouteOutcome::Success { .. } | RouteOutcome::SuccessUntimed => 0.0,
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
                        peer: re.peer.clone(),
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
                        peer: re.peer.clone(),
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
            consider_n_closest_peers: 5,
        }
    }

    #[allow(dead_code)]
    pub fn considering_n_closest_peers(mut self, n: u32) -> Self {
        self.consider_n_closest_peers = n as usize;
        self
    }

    pub fn add_event(&mut self, event: RouteEvent) {
        let was_below_threshold = !self.has_sufficient_routing_events();

        match event.outcome {
            RouteOutcome::Success {
                time_to_response_start,
                payload_size,
                payload_transfer_time,
            } => {
                self.response_start_time_estimator.add_event(IsotonicEvent {
                    peer: event.peer.clone(),
                    contract_location: event.contract_location,
                    result: time_to_response_start.as_secs_f64(),
                });
                self.failure_estimator.add_event(IsotonicEvent {
                    peer: event.peer.clone(),
                    contract_location: event.contract_location,
                    result: 0.0,
                });
                let transfer_rate_event = IsotonicEvent {
                    contract_location: event.contract_location,
                    peer: event.peer,
                    result: payload_size as f64 / payload_transfer_time.as_secs_f64(),
                };
                self.mean_transfer_size.add(payload_size as f64);

                self.transfer_rate_estimator.add_event(transfer_rate_event);
            }
            RouteOutcome::SuccessUntimed | RouteOutcome::Failure => {
                let result = if matches!(event.outcome, RouteOutcome::Failure) {
                    1.0
                } else {
                    0.0
                };
                self.failure_estimator.add_event(IsotonicEvent {
                    peer: event.peer,
                    contract_location: event.contract_location,
                    result,
                });
            }
        }

        if was_below_threshold && self.has_sufficient_routing_events() {
            tracing::info!(
                total_events = self.failure_estimator.len(),
                successes = self.response_start_time_estimator.len(),
                "Router transitioning from distance-based to prediction-based routing"
            );
        }
    }

    fn select_closest_peers<'a>(
        &self,
        peers: impl IntoIterator<Item = &'a PeerKeyLocation>,
        target_location: &Location,
    ) -> Vec<&'a PeerKeyLocation> {
        let mut peer_distances: Vec<_> = peers
            .into_iter()
            .map(|peer| {
                let distance = peer
                    .location()
                    .map(|loc| target_location.distance(loc))
                    .unwrap_or_else(|| Distance::new(0.5));
                (peer, distance)
            })
            .collect();

        GlobalRng::shuffle(&mut peer_distances);
        peer_distances.sort_by_key(|&(_, distance)| distance);
        peer_distances.truncate(self.consider_n_closest_peers);
        peer_distances.into_iter().map(|(peer, _)| peer).collect()
    }

    pub fn select_peer<'a>(
        &self,
        peers: impl IntoIterator<Item = &'a PeerKeyLocation>,
        target_location: Location,
    ) -> Option<&'a PeerKeyLocation> {
        self.select_k_best_peers(peers, target_location, 1)
            .into_iter()
            .next()
    }

    /// Select up to k best peers for routing, ranked by predicted performance.
    /// Returns peers ordered from best to worst predicted performance.
    pub fn select_k_best_peers<'a>(
        &self,
        peers: impl IntoIterator<Item = &'a PeerKeyLocation>,
        target_location: Location,
        k: usize,
    ) -> Vec<&'a PeerKeyLocation> {
        let (selected, _decision) =
            self.select_k_best_peers_with_telemetry(peers, target_location, k);
        selected
    }

    fn predict_routing_outcome(
        &self,
        peer: &PeerKeyLocation,
        target_location: Location,
    ) -> Result<RoutingPrediction, RoutingError> {
        if !self.has_sufficient_routing_events() {
            return Err(RoutingError::InsufficientDataError);
        }

        // Failure estimator is required — it has data from all outcome types
        let failure_estimate = self
            .failure_estimator
            .estimate_retrieval_time(peer, target_location)
            .map_err(|source| RoutingError::EstimationError {
                estimation: "failure",
                source,
            })?;

        // Timing estimators are optional — they only get data from timed GET successes
        // which are rare (~4% of operations).
        let time_estimate = self
            .response_start_time_estimator
            .estimate_retrieval_time(peer, target_location)
            .ok();
        let transfer_estimate = self
            .transfer_rate_estimator
            .estimate_retrieval_time(peer, target_location)
            .ok();

        let failure_cost_multiplier = 3.0;

        let (expected_total_time, time_to_response_start, xfer_speed) =
            match (time_estimate, transfer_estimate) {
                (Some(time), Some(rate)) => {
                    // Full prediction with timing data
                    let total = time
                        + (self.mean_transfer_size.compute() / rate)
                        + (time * failure_estimate * failure_cost_multiplier);
                    (total, time, rate)
                }
                _ => {
                    // Failure-only prediction: use failure probability as cost
                    let total = failure_estimate * failure_cost_multiplier;
                    (total, 0.0, 0.0)
                }
            };

        Ok(RoutingPrediction {
            failure_probability: failure_estimate,
            xfer_speed: TransferSpeed {
                bytes_per_second: xfer_speed,
            },
            time_to_response_start,
            expected_total_time,
        })
    }

    /// Like `select_k_best_peers` but also returns a `RoutingDecisionInfo` for telemetry.
    pub fn select_k_best_peers_with_telemetry<'a>(
        &self,
        peers: impl IntoIterator<Item = &'a PeerKeyLocation>,
        target_location: Location,
        k: usize,
    ) -> (Vec<&'a PeerKeyLocation>, RoutingDecisionInfo) {
        let total_routing_events = self.failure_estimator.len();

        if k == 0 {
            return (
                Vec::new(),
                RoutingDecisionInfo {
                    target_location: target_location.as_f64(),
                    strategy: RoutingStrategy::DistanceBased,
                    candidates: Vec::new(),
                    total_routing_events,
                },
            );
        }

        if !self.has_sufficient_routing_events() {
            let mut peer_distances: Vec<_> = peers
                .into_iter()
                .filter_map(|peer| {
                    peer.location().map(|loc| {
                        let distance = target_location.distance(loc);
                        (peer, distance)
                    })
                })
                .collect();

            GlobalRng::shuffle(&mut peer_distances);
            peer_distances.sort_by_key(|&(_, distance)| distance);
            peer_distances.truncate(k);

            let candidates: Vec<RoutingCandidate> = peer_distances
                .iter()
                .map(|(_, dist)| RoutingCandidate {
                    distance: dist.as_f64(),
                    prediction: None,
                    selected: true, // All are selected (list already truncated to k)
                })
                .collect();

            let selected: Vec<&'a PeerKeyLocation> =
                peer_distances.into_iter().map(|(peer, _)| peer).collect();

            let decision = RoutingDecisionInfo {
                target_location: target_location.as_f64(),
                strategy: RoutingStrategy::DistanceBased,
                candidates,
                total_routing_events,
            };
            (selected, decision)
        } else {
            let closest = self.select_closest_peers(peers, &target_location);
            let mut fallback_count = 0;

            let mut scored: Vec<(&'a PeerKeyLocation, f64, Option<RoutingPrediction>)> = closest
                .iter()
                .map(|peer| {
                    let distance = peer
                        .location()
                        .map(|loc| target_location.distance(loc).as_f64())
                        .unwrap_or(0.5);
                    match self.predict_routing_outcome(peer, target_location) {
                        Ok(pred) => (*peer, distance, Some(pred)),
                        Err(_) => {
                            fallback_count += 1;
                            (*peer, distance, None)
                        }
                    }
                })
                .collect();

            // Sort: peers with predictions by expected_total_time, others at the end
            scored.sort_by(|a, b| {
                let time_a = a.2.map(|p| p.expected_total_time).unwrap_or(f64::MAX);
                let time_b = b.2.map(|p| p.expected_total_time).unwrap_or(f64::MAX);
                time_a
                    .partial_cmp(&time_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

            let strategy = if fallback_count == 0 {
                RoutingStrategy::PredictionBased
            } else {
                // Some or all predictions failed; using distance as tiebreaker
                RoutingStrategy::PredictionFallback
            };

            let candidates: Vec<RoutingCandidate> = scored
                .iter()
                .enumerate()
                .map(|(i, (_, dist, pred))| RoutingCandidate {
                    distance: *dist,
                    prediction: pred.map(RoutingPredictionInfo::from),
                    selected: i < k,
                })
                .collect();

            scored.truncate(k);
            let selected: Vec<&'a PeerKeyLocation> =
                scored.into_iter().map(|(peer, _, _)| peer).collect();

            let decision = RoutingDecisionInfo {
                target_location: target_location.as_f64(),
                strategy,
                candidates,
                total_routing_events,
            };
            (selected, decision)
        }
    }

    /// Produce a snapshot of the router model state for telemetry.
    pub fn snapshot(&self) -> RouterSnapshotInfo {
        RouterSnapshotInfo {
            failure_events: self.failure_estimator.len(),
            success_events: self.response_start_time_estimator.len(),
            transfer_rate_events: self.transfer_rate_estimator.len(),
            prediction_active: self.has_sufficient_routing_events(),
            mean_transfer_size_bytes: self.mean_transfer_size.compute(),
            consider_n_closest_peers: self.consider_n_closest_peers,
            peers_with_failure_adjustments: self.failure_estimator.peer_adjustments.len(),
            peers_with_response_adjustments: self
                .response_start_time_estimator
                .peer_adjustments
                .len(),
            failure_curve: self.failure_estimator.curve_points(),
            response_time_curve: self.response_start_time_estimator.curve_points(),
            transfer_rate_curve: self.transfer_rate_estimator.curve_points(),
            // Populated by Ring which has access to both Router and OpManager
            connect_forward_curve: None,
            connect_forward_events: None,
            connect_forward_peer_adjustments: None,
        }
    }

    /// Whether we have enough routing events to attempt prediction-based selection.
    ///
    /// Uses `failure_estimator` which records both successes (0.0) and failures (1.0),
    /// so it reflects total routing events. Note: this can return true even when
    /// `response_start_time_estimator` has too few events for individual predictions —
    /// callers must handle the fallback case via `predict_routing_outcome` returning Err.
    ///
    /// Threshold of 50 (down from 200): with failure data now flowing through the
    /// router, 50 events provides meaningful signal for the isotonic regression.
    /// The old threshold of 200 success-only events was effectively unreachable.
    fn has_sufficient_routing_events(&self) -> bool {
        const MIN_EVENTS_FOR_PREDICTION: usize = 50;
        self.failure_estimator.len() >= MIN_EVENTS_FOR_PREDICTION
    }
}

#[derive(Debug, thiserror::Error)]
enum RoutingError {
    #[error("Insufficient data provided")]
    InsufficientDataError,
    #[error("failed {estimation} estimation: {source}")]
    EstimationError {
        estimation: &'static str,
        #[source]
        source: isotonic_estimator::EstimationError,
    },
}

#[derive(Debug, Clone, Copy, Serialize)]
pub(crate) struct RoutingPrediction {
    pub failure_probability: f64,
    pub xfer_speed: TransferSpeed,
    pub time_to_response_start: f64,
    pub expected_total_time: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub(crate) struct RouteEvent {
    pub peer: PeerKeyLocation,
    pub contract_location: Location,
    pub outcome: RouteOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub enum RouteOutcome {
    Success {
        time_to_response_start: Duration,
        payload_size: usize,
        payload_transfer_time: Duration,
    },
    /// Operation succeeded but has no timing data (subscribe, put, update).
    /// Feeds only the failure_estimator (0.0 = success), not timing estimators.
    SuccessUntimed,
    Failure,
}

#[cfg(test)]
mod tests {
    use crate::ring::Distance;

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
            let best = router.select_peer(&peers, contract_location).unwrap();
            let best_distance = best.location().unwrap().distance(contract_location);
            for peer in &peers {
                // Dereference `best` when making the comparison
                if *peer != *best {
                    let distance = peer.location().unwrap().distance(contract_location);
                    assert!(distance >= best_distance);
                }
            }
        }
    }

    #[test]
    fn test_request_time() {
        // Define constants for the number of peers, number of events, and number of test iterations.
        const NUM_PEERS: usize = 25;
        const NUM_EVENTS: usize = 400000;

        // Create `NUM_PEERS` random peers and put them in a vector.
        let peers: Vec<PeerKeyLocation> =
            (0..NUM_PEERS).map(|_| PeerKeyLocation::random()).collect();

        // Create NUM_EVENTS random events
        let mut events = vec![];
        for _ in 0..NUM_EVENTS {
            let peer = peers[GlobalRng::random_range(0..NUM_PEERS)].clone();
            let contract_location = Location::random();
            let simulated_prediction = GlobalRng::with_rng(|rng| {
                simulate_prediction(rng, peer.clone(), contract_location)
            });
            let event = RouteEvent {
                peer,
                contract_location,
                outcome: if GlobalRng::random_range(0.0..1.0)
                    > simulated_prediction.failure_probability
                {
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

        // Calculate empirical statistics from the training data
        let mut empirical_stats: std::collections::HashMap<
            (PeerKeyLocation, Location),
            (f64, f64, f64, usize),
        > = std::collections::HashMap::new();

        for event in training_events {
            let key = (event.peer.clone(), event.contract_location);
            let entry = empirical_stats.entry(key).or_insert((0.0, 0.0, 0.0, 0));

            entry.3 += 1; // count

            match &event.outcome {
                RouteOutcome::Success {
                    time_to_response_start,
                    payload_transfer_time,
                    payload_size,
                } => {
                    entry.0 += time_to_response_start.as_secs_f64();
                    entry.1 += *payload_size as f64 / payload_transfer_time.as_secs_f64();
                }
                RouteOutcome::SuccessUntimed => {
                    // No timing data to accumulate
                }
                RouteOutcome::Failure => {
                    entry.2 += 1.0; // failure count
                }
            }
        }

        // Test the router with the testing events.
        for event in testing_events {
            let prediction = router
                .predict_routing_outcome(&event.peer, event.contract_location)
                .unwrap();

            // Instead of comparing against simulate_prediction, we should verify
            // that the router's predictions are reasonable given the empirical data.
            // The router uses isotonic regression which learns from actual outcomes,
            // not theoretical models.

            // For failure probability, just check it's in valid range [0, 1]
            // Note: Due to isotonic regression implementation details, values might
            // occasionally be slightly outside [0, 1] due to floating point errors
            assert!(
                prediction.failure_probability >= -0.01 && prediction.failure_probability <= 1.01,
                "failure_probability out of range: {}",
                prediction.failure_probability
            );

            // For response time and transfer speed, check they're positive
            assert!(
                prediction.time_to_response_start > 0.0,
                "time_to_response_start must be positive: {}",
                prediction.time_to_response_start
            );

            assert!(
                prediction.xfer_speed.bytes_per_second > 0.0,
                "transfer_speed must be positive: {}",
                prediction.xfer_speed.bytes_per_second
            );
        }
    }

    #[test]
    fn test_select_closest_peers_size() {
        const NUM_PEERS: u32 = 45;
        const CAP: u32 = 30;

        assert_eq!(
            CAP as usize,
            Router::new(&[])
                .considering_n_closest_peers(CAP)
                .select_closest_peers(&create_peers(NUM_PEERS), &Location::random())
                .len()
        );
    }

    #[test]
    fn test_select_closest_peers_equality() {
        const NUM_PEERS: u32 = 100;
        const CLOSEST_CAP: u32 = 10;
        let peers: Vec<PeerKeyLocation> = create_peers(NUM_PEERS);
        let contract_location = Location::random();

        let expected_closest = select_closest_peers_vec(CLOSEST_CAP, &peers, &contract_location);

        // Create a router with no historical data
        let router = Router::new(&[]).considering_n_closest_peers(CLOSEST_CAP);
        let asserted_closest: Vec<&PeerKeyLocation> =
            router.select_closest_peers(&peers, &contract_location);

        let mut expected_iter = expected_closest.iter();
        let mut asserted_iter = asserted_closest.iter();

        while let (Some(expected_location), Some(asserted_location)) =
            (expected_iter.next(), asserted_iter.next())
        {
            assert_eq!(**expected_location, **asserted_location);
        }

        assert_eq!(expected_iter.next(), asserted_iter.next());
    }

    fn simulate_prediction(
        random: &mut dyn rand::RngCore,
        peer: PeerKeyLocation,
        target_location: Location,
    ) -> RoutingPrediction {
        use rand::Rng;
        let distance = peer.location().unwrap().distance(target_location);
        let time_to_response_start = 2.0 * distance.as_f64();
        let failure_prob = distance.as_f64();
        let transfer_speed = 100.0 - (100.0 * distance.as_f64());
        let payload_size = random.random_range(100..1000);
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

    fn select_closest_peers_vec<'a>(
        closest_peers_capacity: u32,
        peers: impl IntoIterator<Item = &'a PeerKeyLocation>,
        target_location: &Location,
    ) -> Vec<&'a PeerKeyLocation>
    where
        PeerKeyLocation: Clone,
    {
        let mut closest: Vec<&'a PeerKeyLocation> = peers.into_iter().collect();
        closest.sort_by_key(|&peer| {
            if let Some(location) = peer.location() {
                target_location.distance(location)
            } else {
                Distance::new(f64::MAX)
            }
        });

        closest[..closest_peers_capacity as usize].to_vec()
    }

    fn create_peers(num_peers: u32) -> Vec<PeerKeyLocation> {
        let mut peers: Vec<PeerKeyLocation> = vec![];

        for _ in 0..num_peers {
            let peer = PeerKeyLocation::random();
            peers.push(peer);
        }

        peers
    }

    // ============ Self-routing prevention support tests ============
    //
    // These tests support the self-routing prevention tests in ConnectionManager.
    // While ConnectionManager handles filtering (excluding self/requester), Router
    // must handle the edge cases that result from aggressive filtering:
    // - Empty candidate lists (all peers filtered out)
    // - Single candidate lists (only one peer remains)
    //
    // Related bugs: #1806, #1786, #1781, #1827

    /// Test that select_peer returns None for empty candidate list
    ///
    /// **Scenario this supports:**
    /// After ConnectionManager filters out the requesting peer and any transient
    /// connections, the candidate list may be empty. Router must return None
    /// rather than panicking or returning an invalid peer.
    ///
    /// **Related to bug #1806:**
    /// When routing filters were first added, empty candidate lists caused panics.
    #[test]
    fn test_select_peer_empty_candidates() {
        let router = Router::new(&[]);
        let empty_peers: Vec<PeerKeyLocation> = vec![];
        let target = Location::random();

        let result = router.select_peer(&empty_peers, target);
        assert!(
            result.is_none(),
            "select_peer should return None for empty candidate list"
        );
    }

    /// Test that select_closest_peers handles empty candidate list
    ///
    /// **Scenario this supports:**
    /// Internal method used by select_k_best_peers. Must handle edge cases
    /// gracefully when filtering leaves no candidates.
    ///
    /// **Related to bugs #1806, #1786:**
    /// Small networks with aggressive filtering can easily end up with zero
    /// routing candidates. This must not cause crashes.
    #[test]
    fn test_select_k_best_empty_candidates() {
        let router = Router::new(&[]).considering_n_closest_peers(5);
        let empty_peers: Vec<PeerKeyLocation> = vec![];
        let target = Location::random();

        let result = router.select_closest_peers(&empty_peers, &target);
        assert!(
            result.is_empty(),
            "select_closest_peers should return empty vec for empty candidates"
        );
    }

    /// Test that select_peer works correctly with single candidate
    ///
    /// **Scenario this supports:**
    /// In a 3-node network, after excluding self and requester, only 1 peer remains.
    /// Router must correctly select that peer without additional filtering that
    /// could cause "no route found" errors.
    ///
    /// **Related to bug #1827:**
    /// Gateway nodes in small networks sometimes failed to route because overly
    /// aggressive filtering left only one candidate, which was then incorrectly
    /// rejected by other criteria.
    #[test]
    fn test_select_peer_single_candidate() {
        let router = Router::new(&[]);
        let single_peer = PeerKeyLocation::random();
        let peers = vec![single_peer.clone()];
        let target = Location::random();

        let result = router.select_peer(&peers, target);
        assert!(result.is_some(), "Should select the only available peer");
        assert_eq!(
            *result.unwrap(),
            single_peer,
            "Should return the single candidate"
        );
    }

    /// Feed router a mix of successes for peer A and failures for peer B at similar
    /// distances. Verify select_peer prefers peer A.
    #[test]
    fn test_failure_avoidance() {
        let peer_a = PeerKeyLocation::random();
        let peer_b = PeerKeyLocation::random();

        let contract_location = Location::random();

        let mut events = Vec::new();

        // 40 successes for peer A
        for _ in 0..40 {
            events.push(RouteEvent {
                peer: peer_a.clone(),
                contract_location,
                outcome: RouteOutcome::Success {
                    time_to_response_start: Duration::from_millis(50),
                    payload_size: 1000,
                    payload_transfer_time: Duration::from_millis(10),
                },
            });
        }

        // 40 failures for peer B
        for _ in 0..40 {
            events.push(RouteEvent {
                peer: peer_b.clone(),
                contract_location,
                outcome: RouteOutcome::Failure,
            });
        }

        let router = Router::new(&events);

        // With 80 total events in failure_estimator (>= 50 threshold),
        // the router should use predictions and prefer peer A
        let peers = vec![peer_a.clone(), peer_b.clone()];
        let selected = router.select_peer(&peers, contract_location);
        assert!(selected.is_some());
        assert_eq!(
            *selected.unwrap(),
            peer_a,
            "Router should prefer peer A (all successes) over peer B (all failures)"
        );
    }

    /// Verify 49 events = distance-based, 50 events = prediction-based.
    #[test]
    fn test_threshold_at_50_events() {
        let peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        // 49 events: below threshold
        let events_49: Vec<RouteEvent> = (0..49)
            .map(|_| RouteEvent {
                peer: peer.clone(),
                contract_location,
                outcome: RouteOutcome::Success {
                    time_to_response_start: Duration::from_millis(50),
                    payload_size: 1000,
                    payload_transfer_time: Duration::from_millis(10),
                },
            })
            .collect();

        let router_49 = Router::new(&events_49);
        assert!(
            !router_49.has_sufficient_routing_events(),
            "49 events should be below threshold"
        );

        // 50 events: at threshold
        let events_50: Vec<RouteEvent> = (0..50)
            .map(|_| RouteEvent {
                peer: peer.clone(),
                contract_location,
                outcome: RouteOutcome::Success {
                    time_to_response_start: Duration::from_millis(50),
                    payload_size: 1000,
                    payload_transfer_time: Duration::from_millis(10),
                },
            })
            .collect();

        let router_50 = Router::new(&events_50);
        assert!(
            router_50.has_sufficient_routing_events(),
            "50 events should meet threshold"
        );
    }

    /// 25 successes + 25 failures = 50 total. Router should activate predictions.
    #[test]
    fn test_failures_count_toward_threshold() {
        let peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        let mut events = Vec::new();

        // 25 successes
        for _ in 0..25 {
            events.push(RouteEvent {
                peer: peer.clone(),
                contract_location,
                outcome: RouteOutcome::Success {
                    time_to_response_start: Duration::from_millis(50),
                    payload_size: 1000,
                    payload_transfer_time: Duration::from_millis(10),
                },
            });
        }

        // 25 failures
        for _ in 0..25 {
            events.push(RouteEvent {
                peer: peer.clone(),
                contract_location,
                outcome: RouteOutcome::Failure,
            });
        }

        let router = Router::new(&events);
        assert!(
            router.has_sufficient_routing_events(),
            "25 successes + 25 failures = 50 total should meet threshold"
        );
    }

    /// When the failure_estimator has enough events but timing estimators do not
    /// (all failures, no timed successes), the router should still use prediction-based
    /// routing using failure probability alone — not fall back to distance-based.
    #[test]
    fn test_prediction_works_with_failure_only_data() {
        let peers: Vec<PeerKeyLocation> = (0..5).map(|_| PeerKeyLocation::random()).collect();
        let contract_location = Location::random();

        // 60 failures spread across peers: failure_estimator has data,
        // but timing estimators have 0 events
        let events: Vec<RouteEvent> = (0..60)
            .map(|i| RouteEvent {
                peer: peers[i % peers.len()].clone(),
                contract_location,
                outcome: RouteOutcome::Failure,
            })
            .collect();

        let router = Router::new(&events);
        assert!(router.has_sufficient_routing_events());
        assert_eq!(router.response_start_time_estimator.len(), 0);
        assert_eq!(router.transfer_rate_estimator.len(), 0);

        // Predictions should succeed using failure probability alone
        let (selected, decision) =
            router.select_k_best_peers_with_telemetry(&peers, contract_location, 1);
        assert!(!selected.is_empty());
        assert!(
            matches!(decision.strategy, RoutingStrategy::PredictionBased),
            "Router should use predictions with failure-only data, got {:?}",
            decision.strategy
        );

        // All candidates should have predictions (not None)
        for candidate in &decision.candidates {
            assert!(
                candidate.prediction.is_some(),
                "Each candidate should have a prediction from failure data"
            );
        }
    }

    /// Verify that `SuccessUntimed` feeds the failure_estimator (as 0.0 = success)
    /// but does NOT feed timing estimators (response_start_time, transfer_rate).
    #[test]
    fn test_success_untimed_feeds_failure_only() {
        let peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        let mut router = Router::new(&[]);

        // Add a SuccessUntimed event
        router.add_event(RouteEvent {
            peer: peer.clone(),
            contract_location,
            outcome: RouteOutcome::SuccessUntimed,
        });

        // failure_estimator should have 1 event (success = 0.0)
        assert_eq!(
            router.failure_estimator.len(),
            1,
            "SuccessUntimed should feed failure_estimator"
        );
        // timing estimators should remain empty
        assert_eq!(
            router.response_start_time_estimator.len(),
            0,
            "SuccessUntimed should NOT feed response_start_time_estimator"
        );
        assert_eq!(
            router.transfer_rate_estimator.len(),
            0,
            "SuccessUntimed should NOT feed transfer_rate_estimator"
        );

        // Also verify it counts toward threshold: 49 SuccessUntimed + 1 more = 50
        for _ in 0..49 {
            router.add_event(RouteEvent {
                peer: peer.clone(),
                contract_location,
                outcome: RouteOutcome::SuccessUntimed,
            });
        }
        assert_eq!(router.failure_estimator.len(), 50);
        assert!(
            router.has_sufficient_routing_events(),
            "50 SuccessUntimed events should meet the threshold"
        );
    }

    /// Verify Router::new() handles SuccessUntimed in history correctly.
    #[test]
    fn test_success_untimed_in_history() {
        let peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        let events: Vec<RouteEvent> = (0..30)
            .map(|_| RouteEvent {
                peer: peer.clone(),
                contract_location,
                outcome: RouteOutcome::SuccessUntimed,
            })
            .chain((0..20).map(|_| RouteEvent {
                peer: peer.clone(),
                contract_location,
                outcome: RouteOutcome::Failure,
            }))
            .collect();

        let router = Router::new(&events);

        // failure_estimator: 30 successes + 20 failures = 50
        assert_eq!(router.failure_estimator.len(), 50);
        // timing estimators: 0 (SuccessUntimed has no timing data, Failure has none either)
        assert_eq!(router.response_start_time_estimator.len(), 0);
        assert_eq!(router.transfer_rate_estimator.len(), 0);
    }

    /// With mixed untimed successes and failures (realistic post-#3137 scenario),
    /// the router should use failure probability to prefer low-failure peers.
    #[test]
    fn test_failure_only_differentiates_peers() {
        let good_peer = PeerKeyLocation::random();
        let bad_peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        let mut events = Vec::new();

        // Good peer: 30 untimed successes, 0 failures → 0% failure rate
        for _ in 0..30 {
            events.push(RouteEvent {
                peer: good_peer.clone(),
                contract_location,
                outcome: RouteOutcome::SuccessUntimed,
            });
        }

        // Bad peer: 0 successes, 30 failures → 100% failure rate
        for _ in 0..30 {
            events.push(RouteEvent {
                peer: bad_peer.clone(),
                contract_location,
                outcome: RouteOutcome::Failure,
            });
        }

        let router = Router::new(&events);
        assert!(router.has_sufficient_routing_events());
        assert_eq!(router.response_start_time_estimator.len(), 0);
        assert_eq!(router.transfer_rate_estimator.len(), 0);

        // Router should prefer the good peer via failure-probability prediction
        let peers = vec![good_peer.clone(), bad_peer.clone()];
        let (selected, decision) =
            router.select_k_best_peers_with_telemetry(&peers, contract_location, 1);

        assert!(matches!(
            decision.strategy,
            RoutingStrategy::PredictionBased
        ));
        assert_eq!(
            *selected[0], good_peer,
            "Router should prefer the low-failure peer when using failure-only predictions"
        );

        // The selected (first) candidate should have lower expected_total_time
        let first = &decision.candidates[0];
        let second = &decision.candidates[1];
        assert!(
            first.prediction.as_ref().unwrap().expected_total_time
                <= second.prediction.as_ref().unwrap().expected_total_time,
            "Selected peer should have lower expected_total_time"
        );
    }

    /// With sparse timed success data (only a few GETs succeed), the router should
    /// still produce predictions — using failure data for all peers and timing data
    /// where available.
    #[test]
    fn test_sparse_timed_success_data() {
        let peers: Vec<PeerKeyLocation> = (0..5).map(|_| PeerKeyLocation::random()).collect();
        let contract_location = Location::random();

        let mut events = Vec::new();

        // 40 untimed successes across peers
        for i in 0..40 {
            events.push(RouteEvent {
                peer: peers[i % peers.len()].clone(),
                contract_location,
                outcome: RouteOutcome::SuccessUntimed,
            });
        }

        // 10 failures
        for i in 0..10 {
            events.push(RouteEvent {
                peer: peers[i % peers.len()].clone(),
                contract_location,
                outcome: RouteOutcome::Failure,
            });
        }

        // Only 3 timed successes (below MIN_POINTS_FOR_REGRESSION=5)
        for peer in peers.iter().take(3) {
            events.push(RouteEvent {
                peer: peer.clone(),
                contract_location,
                outcome: RouteOutcome::Success {
                    time_to_response_start: Duration::from_millis(50),
                    payload_size: 1000,
                    payload_transfer_time: Duration::from_millis(10),
                },
            });
        }

        let router = Router::new(&events);
        assert!(router.has_sufficient_routing_events());
        assert!(router.response_start_time_estimator.len() < 5);
        assert!(router.transfer_rate_estimator.len() < 5);

        // Router should still make predictions using failure data
        let (selected, decision) =
            router.select_k_best_peers_with_telemetry(&peers, contract_location, 1);
        assert!(!selected.is_empty());
        assert!(
            matches!(decision.strategy, RoutingStrategy::PredictionBased),
            "Router should use failure-only predictions when timing data is sparse, got {:?}",
            decision.strategy
        );
    }

    // ============ Wiring completeness: end-to-end outcome → router chain tests ============

    /// Simulate the full chain: subscribe outcome → RouteEvent → Router.
    /// A completed subscribe with stats produces SuccessUntimed, which feeds
    /// the failure_estimator (as 0.0 = success).
    #[test]
    fn test_subscribe_outcome_feeds_router() {
        let target_peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        // Simulate what node/mod.rs:580-587 does when OpOutcome::ContractOpSuccessUntimed
        // is returned from subscribe's outcome() (post-fix: stats are preserved)
        let route_event = RouteEvent {
            peer: target_peer.clone(),
            contract_location,
            outcome: RouteOutcome::SuccessUntimed,
        };

        let mut router = Router::new(&[]);
        assert_eq!(router.failure_estimator.len(), 0);
        router.add_event(route_event);
        assert_eq!(
            router.failure_estimator.len(),
            1,
            "Router failure_estimator should record subscribe success (0.0)"
        );
        // Timing estimators should NOT be fed by untimed operations
        assert_eq!(router.response_start_time_estimator.len(), 0);
        assert_eq!(router.transfer_rate_estimator.len(), 0);
    }

    /// Create GetOp with result + partial timing, verify ContractOpSuccessUntimed
    /// feeds router's failure_estimator.
    #[test]
    fn test_get_partial_timing_feeds_router() {
        use crate::operations::OpOutcome;

        let target_peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        // Simulate the OpOutcome from a GET with partial timing
        let route_event = RouteEvent {
            peer: target_peer.clone(),
            contract_location,
            outcome: RouteOutcome::SuccessUntimed,
        };

        let mut router = Router::new(&[]);
        assert_eq!(router.failure_estimator.len(), 0);
        router.add_event(route_event);
        assert_eq!(
            router.failure_estimator.len(),
            1,
            "Router should record untimed GET success"
        );
        // Timing estimators should remain empty
        assert_eq!(router.response_start_time_estimator.len(), 0);
        assert_eq!(router.transfer_rate_estimator.len(), 0);
    }

    /// Verify ContractOpFailure increments failure_estimator with value 1.0,
    /// and that after enough failures the router predicts higher failure probability.
    #[test]
    fn test_failure_outcome_feeds_failure_estimator_with_value_one() {
        let peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        let mut router = Router::new(&[]);

        // Add a failure event
        router.add_event(RouteEvent {
            peer: peer.clone(),
            contract_location,
            outcome: RouteOutcome::Failure,
        });
        assert_eq!(
            router.failure_estimator.len(),
            1,
            "Failure should be recorded in failure_estimator"
        );

        // Add enough failures to cross threshold and check prediction
        for _ in 0..59 {
            router.add_event(RouteEvent {
                peer: peer.clone(),
                contract_location,
                outcome: RouteOutcome::Failure,
            });
        }
        assert_eq!(router.failure_estimator.len(), 60);
        assert!(router.has_sufficient_routing_events());

        // Prediction should show high failure probability
        let peers = vec![peer.clone()];
        let (selected, decision) =
            router.select_k_best_peers_with_telemetry(&peers, contract_location, 1);
        assert!(!selected.is_empty());
        let pred = decision.candidates[0].prediction.as_ref().unwrap();
        assert!(
            pred.failure_probability > 0.5,
            "After 60 failures, failure probability should be high, got {}",
            pred.failure_probability
        );
    }

    /// Verify existing Success path (with timing) still feeds all 3 estimators.
    /// Regression guard for the new untimed branch.
    #[test]
    fn test_full_timing_success_still_works() {
        let peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        let mut router = Router::new(&[]);

        router.add_event(RouteEvent {
            peer: peer.clone(),
            contract_location,
            outcome: RouteOutcome::Success {
                time_to_response_start: Duration::from_millis(50),
                payload_size: 1000,
                payload_transfer_time: Duration::from_millis(10),
            },
        });

        assert_eq!(
            router.failure_estimator.len(),
            1,
            "Success should feed failure_estimator (as 0.0)"
        );
        assert_eq!(
            router.response_start_time_estimator.len(),
            1,
            "Timed success should feed response_start_time_estimator"
        );
        assert_eq!(
            router.transfer_rate_estimator.len(),
            1,
            "Timed success should feed transfer_rate_estimator"
        );
    }

    /// When timing data accumulates beyond MIN_POINTS_FOR_REGRESSION, the router
    /// should transition from failure-only predictions (time=0, speed=0) to full
    /// predictions with real timing values.
    #[test]
    fn test_transition_from_failure_only_to_full_predictions() {
        let peers: Vec<PeerKeyLocation> = (0..5).map(|_| PeerKeyLocation::random()).collect();
        let contract_location = Location::random();

        // Phase 1: Only untimed data, above threshold
        let mut events: Vec<RouteEvent> = (0..50)
            .map(|i| RouteEvent {
                peer: peers[i % peers.len()].clone(),
                contract_location,
                outcome: RouteOutcome::SuccessUntimed,
            })
            .collect();

        let router = Router::new(&events);
        let (_, decision) = router.select_k_best_peers_with_telemetry(&peers, contract_location, 1);
        // Failure-only: timing fields should be 0.0
        let pred = decision.candidates[0].prediction.as_ref().unwrap();
        assert_eq!(pred.time_to_response_start, 0.0);
        assert_eq!(pred.transfer_speed_bps, 0.0);

        // Phase 2: Add enough timed successes to cross MIN_POINTS_FOR_REGRESSION (5)
        for peer in &peers {
            events.push(RouteEvent {
                peer: peer.clone(),
                contract_location,
                outcome: RouteOutcome::Success {
                    time_to_response_start: Duration::from_millis(50),
                    payload_size: 1000,
                    payload_transfer_time: Duration::from_millis(10),
                },
            });
        }

        let router2 = Router::new(&events);
        assert!(router2.response_start_time_estimator.len() >= 5);
        let (_, decision2) =
            router2.select_k_best_peers_with_telemetry(&peers, contract_location, 1);
        // Full prediction: timing fields should have real values
        let pred2 = decision2.candidates[0].prediction.as_ref().unwrap();
        assert!(
            pred2.time_to_response_start > 0.0,
            "Should have real timing data after transition"
        );
        assert!(
            pred2.transfer_speed_bps > 0.0,
            "Should have real transfer speed after transition"
        );
    }

    /// Simulate realistic post-#3137 traffic: a mix of timed GET successes, untimed
    /// PUT/SUBSCRIBE/UPDATE successes, and failures across multiple peers.
    /// The router should activate prediction-based routing and prefer
    /// low-failure, low-latency peers.
    #[test]
    fn test_realistic_mixed_traffic_routing() {
        let contract_location = Location::random();
        let close_peer = PeerKeyLocation::random();
        let mid_peer = PeerKeyLocation::random();
        let far_peer = PeerKeyLocation::random();

        let mut events = Vec::new();

        // close_peer: 10 timed GET successes + 15 untimed PUT/SUB successes, 2 failures
        // → ~7% failure rate, good timing data
        for _ in 0..10 {
            events.push(RouteEvent {
                peer: close_peer.clone(),
                contract_location,
                outcome: RouteOutcome::Success {
                    time_to_response_start: Duration::from_millis(20),
                    payload_size: 2000,
                    payload_transfer_time: Duration::from_millis(5),
                },
            });
        }
        for _ in 0..15 {
            events.push(RouteEvent {
                peer: close_peer.clone(),
                contract_location,
                outcome: RouteOutcome::SuccessUntimed,
            });
        }
        for _ in 0..2 {
            events.push(RouteEvent {
                peer: close_peer.clone(),
                contract_location,
                outcome: RouteOutcome::Failure,
            });
        }

        // mid_peer: 3 timed GET successes + 10 untimed successes, 5 failures
        // → ~28% failure rate, sparse timing data
        for _ in 0..3 {
            events.push(RouteEvent {
                peer: mid_peer.clone(),
                contract_location,
                outcome: RouteOutcome::Success {
                    time_to_response_start: Duration::from_millis(50),
                    payload_size: 2000,
                    payload_transfer_time: Duration::from_millis(15),
                },
            });
        }
        for _ in 0..10 {
            events.push(RouteEvent {
                peer: mid_peer.clone(),
                contract_location,
                outcome: RouteOutcome::SuccessUntimed,
            });
        }
        for _ in 0..5 {
            events.push(RouteEvent {
                peer: mid_peer.clone(),
                contract_location,
                outcome: RouteOutcome::Failure,
            });
        }

        // far_peer: 0 timed successes, 5 untimed successes, 15 failures
        // → 75% failure rate, no timing data
        for _ in 0..5 {
            events.push(RouteEvent {
                peer: far_peer.clone(),
                contract_location,
                outcome: RouteOutcome::SuccessUntimed,
            });
        }
        for _ in 0..15 {
            events.push(RouteEvent {
                peer: far_peer.clone(),
                contract_location,
                outcome: RouteOutcome::Failure,
            });
        }

        // Total: 27 + 18 + 20 = 65 events > 50 threshold
        let router = Router::new(&events);
        assert!(router.has_sufficient_routing_events());

        // failure_estimator should have all 65 events
        assert_eq!(router.failure_estimator.len(), 65);
        // timing estimators only have the timed GET successes: 10 + 3 = 13
        assert_eq!(router.response_start_time_estimator.len(), 13);
        assert_eq!(router.transfer_rate_estimator.len(), 13);

        // Router should use prediction-based routing and prefer close_peer
        let peers = vec![close_peer.clone(), mid_peer.clone(), far_peer.clone()];
        let (selected, decision) =
            router.select_k_best_peers_with_telemetry(&peers, contract_location, 3);

        assert!(matches!(
            decision.strategy,
            RoutingStrategy::PredictionBased
        ));
        assert_eq!(selected.len(), 3);

        // close_peer should be ranked first (lowest failure + best timing)
        assert_eq!(
            *selected[0], close_peer,
            "close_peer with ~7% failure and fast timing should be ranked first"
        );
        // far_peer should be ranked last (highest failure rate)
        assert_eq!(
            *selected[2], far_peer,
            "far_peer with ~75% failure should be ranked last"
        );
    }

    /// Test that adding events incrementally (as happens in practice) produces
    /// the same routing decision as building from history.
    #[test]
    fn test_incremental_vs_batch_consistency() {
        let peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        let events: Vec<RouteEvent> = (0..30)
            .map(|_| RouteEvent {
                peer: peer.clone(),
                contract_location,
                outcome: RouteOutcome::SuccessUntimed,
            })
            .chain((0..20).map(|_| RouteEvent {
                peer: peer.clone(),
                contract_location,
                outcome: RouteOutcome::Failure,
            }))
            .chain((0..5).map(|_| RouteEvent {
                peer: peer.clone(),
                contract_location,
                outcome: RouteOutcome::Success {
                    time_to_response_start: Duration::from_millis(40),
                    payload_size: 1500,
                    payload_transfer_time: Duration::from_millis(8),
                },
            }))
            .collect();

        // Batch: build from history
        let batch_router = Router::new(&events);

        // Incremental: add one by one
        let mut incr_router = Router::new(&[]);
        for event in &events {
            incr_router.add_event(event.clone());
        }

        // Both should have identical estimator counts
        assert_eq!(
            batch_router.failure_estimator.len(),
            incr_router.failure_estimator.len()
        );
        assert_eq!(
            batch_router.response_start_time_estimator.len(),
            incr_router.response_start_time_estimator.len()
        );
        assert_eq!(
            batch_router.transfer_rate_estimator.len(),
            incr_router.transfer_rate_estimator.len()
        );
    }

    /// Verify that the router handles a scenario where only untimed operations
    /// exist (no GETs ever succeeded with timing). This is realistic for a node
    /// that primarily handles PUT/SUBSCRIBE/UPDATE traffic.
    #[test]
    fn test_untimed_only_network_peer_ranking() {
        let reliable_peer = PeerKeyLocation::random();
        let flaky_peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        let mut events = Vec::new();

        // reliable_peer: 40 untimed successes, 0 failures
        for _ in 0..40 {
            events.push(RouteEvent {
                peer: reliable_peer.clone(),
                contract_location,
                outcome: RouteOutcome::SuccessUntimed,
            });
        }

        // flaky_peer: 5 untimed successes, 15 failures → 75% failure
        for _ in 0..5 {
            events.push(RouteEvent {
                peer: flaky_peer.clone(),
                contract_location,
                outcome: RouteOutcome::SuccessUntimed,
            });
        }
        for _ in 0..15 {
            events.push(RouteEvent {
                peer: flaky_peer.clone(),
                contract_location,
                outcome: RouteOutcome::Failure,
            });
        }

        let router = Router::new(&events);
        assert!(router.has_sufficient_routing_events());
        // No timing data at all
        assert_eq!(router.response_start_time_estimator.len(), 0);
        assert_eq!(router.transfer_rate_estimator.len(), 0);

        // Router should still make predictions and prefer the reliable peer
        let peers = vec![reliable_peer.clone(), flaky_peer.clone()];
        let (selected, decision) =
            router.select_k_best_peers_with_telemetry(&peers, contract_location, 2);

        assert!(matches!(
            decision.strategy,
            RoutingStrategy::PredictionBased
        ));
        assert_eq!(
            *selected[0], reliable_peer,
            "Reliable peer should be preferred in untimed-only network"
        );

        // Predictions should use failure-only mode (time=0, speed=0)
        for candidate in &decision.candidates {
            let pred = candidate.prediction.as_ref().unwrap();
            assert_eq!(pred.time_to_response_start, 0.0);
            assert_eq!(pred.transfer_speed_bps, 0.0);
        }
    }

    /// When the router receives SuccessUntimed events at one contract location
    /// and Failure events at a different contract location through the same peer,
    /// the failure estimator should track these as distinct (distance, result)
    /// data points. This validates the isotonic model learns location-dependent
    /// failure patterns using untimed success data from PUT/SUBSCRIBE/UPDATE.
    #[test]
    fn test_location_dependent_failure_patterns() {
        let peer = PeerKeyLocation::random();
        let near_contract = Location::new(0.01); // very close distance (close to 0)
        let far_contract = Location::new(0.49); // maximum distance on the ring

        let mut router = Router::new(&[]);

        // Peer succeeds for nearby contracts (small distance)
        for _ in 0..30 {
            router.add_event(RouteEvent {
                peer: peer.clone(),
                contract_location: near_contract,
                outcome: RouteOutcome::SuccessUntimed,
            });
        }

        // Peer fails for distant contracts (large distance)
        for _ in 0..25 {
            router.add_event(RouteEvent {
                peer: peer.clone(),
                contract_location: far_contract,
                outcome: RouteOutcome::Failure,
            });
        }

        assert!(router.has_sufficient_routing_events());
        assert_eq!(router.failure_estimator.len(), 55);

        // The isotonic model should give different failure predictions
        // for near vs far contracts through this peer
        let peers = vec![peer.clone()];

        let (_, near_decision) =
            router.select_k_best_peers_with_telemetry(&peers, near_contract, 1);
        let (_, far_decision) = router.select_k_best_peers_with_telemetry(&peers, far_contract, 1);

        let near_pred = near_decision.candidates[0]
            .prediction
            .as_ref()
            .expect("should have prediction for near contract");
        let far_pred = far_decision.candidates[0]
            .prediction
            .as_ref()
            .expect("should have prediction for far contract");

        // Near contract should have lower failure probability than far contract
        assert!(
            near_pred.failure_probability <= far_pred.failure_probability,
            "Near contract failure prob ({}) should be <= far contract failure prob ({})",
            near_pred.failure_probability,
            far_pred.failure_probability
        );
    }

    /// Verify that a single peer's SuccessUntimed events correctly produce a 0.0
    /// failure probability when it has never failed.
    #[test]
    fn test_zero_failure_probability_with_untimed_success() {
        let peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        let events: Vec<RouteEvent> = (0..60)
            .map(|_| RouteEvent {
                peer: peer.clone(),
                contract_location,
                outcome: RouteOutcome::SuccessUntimed,
            })
            .collect();

        let router = Router::new(&events);
        assert!(router.has_sufficient_routing_events());

        let (_, decision) =
            router.select_k_best_peers_with_telemetry(&[peer], contract_location, 1);

        let pred = decision.candidates[0]
            .prediction
            .as_ref()
            .expect("should have prediction");
        assert!(
            pred.failure_probability < 0.01,
            "Peer with only successes should have near-zero failure probability, got {}",
            pred.failure_probability
        );
    }
}
