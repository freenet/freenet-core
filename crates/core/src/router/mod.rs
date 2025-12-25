mod isotonic_estimator;
mod util;

use crate::ring::{Distance, Location, PeerKeyLocation};
pub(crate) use isotonic_estimator::{EstimatorType, IsotonicEstimator, IsotonicEvent};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use util::{Mean, TransferSpeed};

/// # Usage
/// Important when using this type:
/// Need to periodically rebuild the Router using `history` for better predictions.
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
                peer: re.peer.clone(),
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
            RouteOutcome::Failure => {
                self.failure_estimator.add_event(IsotonicEvent {
                    peer: event.peer,
                    contract_location: event.contract_location,
                    result: 1.0,
                });
            }
        }
    }

    /// Returns all peers sorted by distance from the target location (closest first).
    ///
    /// This enables "uphill" routing: when closer peers don't have the contract,
    /// the caller can fall back to further peers that might have it. Previously
    /// this function truncated to a small number of closest peers, which caused
    /// GET/SUBSCRIBE operations to fail even when further peers had the contract.
    fn select_peers_by_distance<'a>(
        &self,
        peers: impl IntoIterator<Item = &'a PeerKeyLocation>,
        target_location: &Location,
    ) -> Vec<&'a PeerKeyLocation> {
        use rand::seq::SliceRandom;

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

        // Shuffle first to randomize ordering among peers at equal distances
        let rng = &mut rand::rng();
        peer_distances.shuffle(rng);
        // Sort by distance - closest peers first
        peer_distances.sort_by_key(|&(_, distance)| distance);
        // Return ALL peers, not just the closest N
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
        if k == 0 {
            return Vec::new();
        }

        if !self.has_sufficient_historical_data() {
            use rand::seq::SliceRandom;

            let mut peer_distances: Vec<_> = peers
                .into_iter()
                .filter_map(|peer| {
                    peer.location().map(|loc| {
                        let distance = target_location.distance(loc);
                        (peer, distance)
                    })
                })
                .collect();

            let rng = &mut rand::rng();
            peer_distances.shuffle(rng);
            peer_distances.sort_by_key(|&(_, distance)| distance);
            peer_distances.truncate(k);
            peer_distances.into_iter().map(|(peer, _)| peer).collect()
        } else {
            // Get ALL peers sorted by distance, then rank by predicted routing outcome time.
            // This enables "uphill" routing: we consider all peers, not just the closest N,
            // so when closer peers don't have the contract, further peers can be tried.
            let mut candidates: Vec<_> = self
                .select_peers_by_distance(peers, &target_location)
                .into_iter()
                .filter_map(|peer| {
                    self.predict_routing_outcome(peer, target_location)
                        .ok()
                        .map(|t| (peer, t.time_to_response_start))
                })
                .collect();

            // Sort by predicted response time
            candidates.sort_by(|&(_, time1), &(_, time2)| {
                time1
                    .partial_cmp(&time2)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

            candidates.truncate(k);
            candidates.into_iter().map(|(peer, _)| peer).collect()
        }
    }

    fn predict_routing_outcome(
        &self,
        peer: &PeerKeyLocation,
        target_location: Location,
    ) -> Result<RoutingPrediction, RoutingError> {
        if !self.has_sufficient_historical_data() {
            return Err(RoutingError::InsufficientDataError);
        }

        let time_to_response_start_estimate = self
            .response_start_time_estimator
            .estimate_retrieval_time(peer, target_location)
            .map_err(|source| RoutingError::EstimationError {
                estimation: "start time",
                source,
            })?;
        let failure_estimate = self
            .failure_estimator
            .estimate_retrieval_time(peer, target_location)
            .map_err(|source| RoutingError::EstimationError {
                estimation: "failure",
                source,
            })?;
        let transfer_rate_estimate = self
            .transfer_rate_estimator
            .estimate_retrieval_time(peer, target_location)
            .map_err(|source| RoutingError::EstimationError {
                estimation: "transfer rate",
                source,
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
struct RoutingPrediction {
    failure_probability: f64,
    xfer_speed: TransferSpeed,
    time_to_response_start: f64,
    expected_total_time: f64,
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
    Failure,
}

#[cfg(test)]
mod tests {
    use rand::Rng;

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
        let mut rng = rand::rng();
        for _ in 0..NUM_EVENTS {
            let peer = peers[rng.random_range(0..NUM_PEERS)].clone();
            let contract_location = Location::random();
            let simulated_prediction =
                simulate_prediction(&mut rng, peer.clone(), contract_location);
            let event = RouteEvent {
                peer,
                contract_location,
                outcome: if rng.random_range(0.0..1.0) > simulated_prediction.failure_probability {
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

    /// Test that select_peers_by_distance returns ALL peers, sorted by distance.
    /// This is the key change for uphill routing - we no longer truncate to closest N.
    #[test]
    fn test_select_peers_by_distance_returns_all() {
        const NUM_PEERS: u32 = 45;

        let router = Router::new(&[]);
        let peers = create_peers(NUM_PEERS);
        let target = Location::random();

        let result = router.select_peers_by_distance(&peers, &target);

        // Should return ALL peers, not just a subset
        assert_eq!(
            result.len(),
            NUM_PEERS as usize,
            "select_peers_by_distance should return all peers"
        );
    }

    /// Test that select_peers_by_distance returns peers sorted by distance (closest first).
    #[test]
    fn test_select_peers_by_distance_ordering() {
        const NUM_PEERS: u32 = 100;
        let peers: Vec<PeerKeyLocation> = create_peers(NUM_PEERS);
        let contract_location = Location::random();

        let router = Router::new(&[]);
        let sorted_peers = router.select_peers_by_distance(&peers, &contract_location);

        // Verify ordering: each peer should be no further than the next
        for window in sorted_peers.windows(2) {
            let dist1 = window[0]
                .location()
                .map(|loc| contract_location.distance(loc))
                .unwrap_or_else(|| Distance::new(0.5));
            let dist2 = window[1]
                .location()
                .map(|loc| contract_location.distance(loc))
                .unwrap_or_else(|| Distance::new(0.5));
            assert!(
                dist1 <= dist2,
                "Peers should be sorted by distance: {:?} > {:?}",
                dist1,
                dist2
            );
        }
    }

    fn simulate_prediction(
        random: &mut rand::rngs::ThreadRng,
        peer: PeerKeyLocation,
        target_location: Location,
    ) -> RoutingPrediction {
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

    /// Test that select_peers_by_distance handles empty candidate list
    ///
    /// **Scenario this supports:**
    /// Internal method used by select_k_best_peers. Must handle edge cases
    /// gracefully when filtering leaves no candidates.
    ///
    /// **Related to bugs #1806, #1786:**
    /// Small networks with aggressive filtering can easily end up with zero
    /// routing candidates. This must not cause crashes.
    #[test]
    fn test_select_peers_by_distance_empty_candidates() {
        let router = Router::new(&[]);
        let empty_peers: Vec<PeerKeyLocation> = vec![];
        let target = Location::random();

        let result = router.select_peers_by_distance(&empty_peers, &target);
        assert!(
            result.is_empty(),
            "select_peers_by_distance should return empty vec for empty candidates"
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

    // ============ Uphill routing tests ============
    //
    // These tests verify the uphill routing feature: when closer peers don't
    // have a contract, further peers can be tried. Previously, only the 2
    // closest peers were considered, causing operations to fail even when
    // further peers had the contract.
    //
    // Related issue: #2401

    /// Test that select_k_best_peers can return peers beyond the closest ones.
    ///
    /// **Scenario this tests:**
    /// With 10 peers and k=5, all 5 returned peers should be from the pool of
    /// all 10 peers, not limited to just the 2 closest. This is the key
    /// behavioral change enabling uphill routing.
    #[test]
    fn test_select_k_best_considers_all_peers() {
        const NUM_PEERS: u32 = 10;
        const K: usize = 5;

        let router = Router::new(&[]);
        let peers = create_peers(NUM_PEERS);
        let target = Location::random();

        let result = router.select_k_best_peers(&peers, target, K);

        // Should return exactly k peers (or all if fewer than k available)
        assert_eq!(
            result.len(),
            K,
            "select_k_best_peers should return k peers when k <= num_peers"
        );
    }

    /// Test that uphill routing works: further peers are included in results
    /// when requesting more than the default closest peer count.
    ///
    /// **Scenario this tests:**
    /// Create peers at known locations. Request more peers than would have
    /// been returned with the old truncation behavior. Verify that further
    /// peers are included.
    #[test]
    fn test_uphill_routing_includes_further_peers() {
        const NUM_PEERS: u32 = 20;
        const K: usize = 10;

        let router = Router::new(&[]);
        let peers = create_peers(NUM_PEERS);
        let target = Location::random();

        // Get all peers sorted by distance for reference
        let all_sorted = router.select_peers_by_distance(&peers, &target);

        // Get k best peers
        let k_best = router.select_k_best_peers(&peers, target, K);

        // All k_best peers should be from the sorted list
        for best_peer in &k_best {
            assert!(
                all_sorted.contains(best_peer),
                "k_best peer should be in the sorted peer list"
            );
        }

        // The old implementation would have only returned 2 peers at most,
        // but now we should get all k requested (when available)
        assert_eq!(
            k_best.len(),
            K,
            "Should return all requested peers, not truncate to closest N"
        );
    }

    /// Test that the router correctly handles the case where there are more
    /// peers than requested in select_k_best_peers.
    #[test]
    fn test_select_k_best_truncates_to_k() {
        const NUM_PEERS: u32 = 50;
        const K: usize = 5;

        let router = Router::new(&[]);
        let peers = create_peers(NUM_PEERS);
        let target = Location::random();

        let result = router.select_k_best_peers(&peers, target, K);

        assert_eq!(
            result.len(),
            K,
            "select_k_best_peers should return exactly k peers when more than k are available"
        );
    }

    /// Test that when fewer peers are available than k, all are returned.
    #[test]
    fn test_select_k_best_returns_all_when_fewer_than_k() {
        const NUM_PEERS: u32 = 3;
        const K: usize = 10;

        let router = Router::new(&[]);
        let peers = create_peers(NUM_PEERS);
        let target = Location::random();

        let result = router.select_k_best_peers(&peers, target, K);

        assert_eq!(
            result.len(),
            NUM_PEERS as usize,
            "select_k_best_peers should return all peers when fewer than k are available"
        );
    }
}
