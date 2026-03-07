//! # Topology Management
//!
//! This module manages peer connection decisions: when to add connections, when to remove them,
//! and whether to accept incoming connection requests.
//!
//! ## Connection Target Selection
//!
//! Outbound connection targets are selected using **gap-based targeting**: the center of
//! the largest gap in the node's current connection distribution in log-distance space.
//! This makes target selection adaptive — nodes converge to the ideal 1/d distribution
//! faster by actively seeking the most deficient distance range. See
//! `small_world_rand::gap_target`.
//!
//! When the node has no existing connections or its own location is unknown (during very
//! early bootstrap), the fallback is random Kleinberg 1/d sampling via inverse CDF:
//! `d = d_min * (d_max/d_min)^U` where U ~ Uniform(0,1).
//!
//! ## When to Add/Remove/Swap
//!
//! - Below `min_connections` → add connections
//! - At/above `min_connections`, resource usage < 50% → add connections
//! - At/above `min_connections`, resource usage > 90% → remove connections
//! - Above `max_connections` → remove connections
//! - Whenever not removing and not resource-constrained → consider topology swap
//!
//! ## Topology Swaps
//!
//! When at steady state (resource usage 50-90%), the manager checks whether
//! replacing a connection would improve the Kleinberg distribution. The swap
//! probability is proportional to how much the largest gap in log-distance space
//! exceeds the expected gap for an ideal distribution (`ln(k)/k` for k connections).
//! This makes swaps self-limiting: frequent when topology is poor, rare when it's
//! good. The connection to drop is the one with the fewest routed requests
//! (least useful to the routing algorithm).
//!
//! ## Accepting Incoming Connections
//!
//! All inbound connection candidates are scored by the **Kleinberg gap score**: the
//! candidate's minimum distance to its nearest neighbor in log-distance space. Candidates
//! that fill the largest gap in the distribution score highest. See
//! `small_world_rand::kleinberg_score`.
//!
//! Two acceptance policies use this score:
//!
//! 1. **Below min_connections** (`should_accept` in connection_manager): Probabilistic
//!    acceptance with a 50% floor. Higher gap scores get higher acceptance probability.
//!    This shapes the distribution during bootstrap without blocking it.
//!
//! 2. **At/above min_connections**: The gap score is fed into the [`ConnectionEvaluator`]
//!    which rate-limits acceptance by picking the best candidate from recent arrivals.
//!
//! Below `KLEINBERG_FILTER_MIN_CONNECTIONS` (3), all connections are accepted
//! unconditionally to avoid blocking initial bootstrap.
//!
//! ## Removing Connections
//!
//! When resource usage is high or over max_connections:
//! 1. Calculate value-per-usage ratio for each peer: `request_count / bandwidth_used`
//! 2. Remove the peer with the worst ratio (least useful for bandwidth consumed)
//! 3. Fallback: if no peer qualifies, remove the most distant peer on the ring

use crate::{message::TransactionType, ring::Location};
use connection_evaluator::ConnectionEvaluator;
use meter::Meter;
use outbound_request_counter::OutboundRequestCounter;
use request_density_tracker::{CachedDensityMap, RequestDensityTracker};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use tokio::time::Instant;
use tracing::{debug, event, info, span, trace, warn, Level};

pub mod connection_evaluator;
mod constants;
pub(crate) mod meter;
pub(crate) mod outbound_request_counter;
pub(crate) mod rate;
pub mod request_density_tracker;
pub(crate) mod running_average;
pub(crate) mod small_world_rand;

use crate::ring::{Connection, PeerKeyLocation};
use crate::topology::meter::{AttributionSource, ResourceType};
use crate::topology::rate::{Rate, RateProportion};
use constants::*;
use request_density_tracker::DensityMapError;

/// Manages peer connection topology: adding, removing, and evaluating connections.
///
/// New connection targets are sampled from Kleinberg's 1/d distribution centered
/// on the peer's own ring location (see [`small_world_rand::kleinberg_target`]).
///
/// The manager uses a [`ConnectionEvaluator`] to evaluate whether an incoming
/// connection candidate is better than all other candidates seen within a time
/// window, and a [`RequestDensityTracker`] to score candidates by request density
/// at their location.
pub(crate) struct TopologyManager {
    limits: Limits,
    meter: Meter,
    source_creation_times: BTreeMap<AttributionSource, Instant>,
    slow_connection_evaluator: ConnectionEvaluator,
    fast_connection_evaluator: ConnectionEvaluator,
    request_density_tracker: RequestDensityTracker,
    pub(crate) outbound_request_counter: OutboundRequestCounter,
    /// Must be updated when new neighbors are discovered.
    cached_density_map: CachedDensityMap,
    connection_acquisition_strategy: ConnectionAcquisitionStrategy,
}

impl TopologyManager {
    /// Create a new TopologyManager specifying the peer's own Location
    pub(crate) fn new(limits: Limits) -> Self {
        TopologyManager {
            meter: Meter::new_with_window_size(100),
            limits,
            source_creation_times: BTreeMap::new(),
            slow_connection_evaluator: ConnectionEvaluator::new(
                SLOW_CONNECTION_EVALUATOR_WINDOW_DURATION,
            ),
            fast_connection_evaluator: ConnectionEvaluator::new(
                FAST_CONNECTION_EVALUATOR_WINDOW_DURATION,
            ),
            request_density_tracker: RequestDensityTracker::new(
                REQUEST_DENSITY_TRACKER_WINDOW_SIZE,
            ),
            cached_density_map: CachedDensityMap::new(),
            outbound_request_counter: OutboundRequestCounter::new(
                OUTBOUND_REQUEST_COUNTER_WINDOW_SIZE,
            ),
            connection_acquisition_strategy: ConnectionAcquisitionStrategy::Fast,
        }
    }

    pub(crate) fn refresh_cache(
        &mut self,
        neighbor_locations: &BTreeMap<Location, Vec<Connection>>,
    ) -> Result<(), DensityMapError> {
        self.cached_density_map
            .set(&self.request_density_tracker, neighbor_locations)?;
        Ok(())
    }

    /// Record a request and the location it's targeting
    pub(crate) fn record_request(
        &mut self,
        recipient: PeerKeyLocation,
        target: Location,
        request_type: TransactionType,
    ) {
        debug!(
            request_type = %request_type,
            recipient = %recipient,
            target_location = %target,
            "Recording request sent to peer"
        );

        self.request_density_tracker.sample(target);
        self.outbound_request_counter.record_request(recipient);
    }

    /// Evaluate a pre-computed score through the connection evaluator.
    ///
    /// Used by `should_accept` to feed the Kleinberg gap score through the
    /// same windowed comparison mechanism that rate-limits connection acceptance.
    pub(crate) fn evaluate_new_connection_with_score(
        &mut self,
        score: f64,
        current_time: Instant,
    ) -> bool {
        match self.connection_acquisition_strategy {
            ConnectionAcquisitionStrategy::Slow => {
                self.fast_connection_evaluator
                    .record_only_with_current_time(score, current_time);
                self.slow_connection_evaluator
                    .record_and_eval_with_current_time(score, current_time)
            }
            ConnectionAcquisitionStrategy::Fast => {
                self.slow_connection_evaluator
                    .record_only_with_current_time(score, current_time);
                self.fast_connection_evaluator
                    .record_and_eval_with_current_time(score, current_time)
            }
        }
    }

    #[cfg(test)]
    /// Get the ideal location for a new connection based on current neighbors and request density
    fn get_best_candidate_location(
        &self,
        this_peer_location: &Location,
    ) -> Result<Location, DensityMapError> {
        let density_map = self
            .cached_density_map
            .get()
            .ok_or(DensityMapError::EmptyNeighbors)?;

        let best_location = match density_map.get_max_density() {
            Ok(location) => {
                debug!(location = %location, "Max density found");
                location
            }
            Err(_) => {
                debug!(
                    fallback_location = %this_peer_location,
                    "An error occurred while getting max density, falling back to random location"
                );
                *this_peer_location
            }
        };
        Ok(best_location)
    }

    #[cfg(test)]
    pub(self) fn update_limits(&mut self, limits: Limits) {
        self.limits = limits;
    }

    /// Report the use of a resource with multiple attribution sources, splitting the usage
    /// evenly between the sources.
    /// This should be done in the lowest-level functions that consume the resource, taking
    /// an AttributionMeter as a parameter. This will be useful for contracts with multiple
    /// subscribers - where the responsibility should be split evenly among the subscribers.
    #[allow(dead_code)] // todo: maybe use this
    pub(crate) fn report_split_resource_usage(
        &mut self,
        attributions: &[AttributionSource],
        resource: ResourceType,
        value: f64,
        at_time: Instant,
    ) {
        let split_value = value / attributions.len() as f64;
        for attribution in attributions {
            self.report_resource_usage(attribution, resource, split_value, at_time);
        }
    }

    #[allow(dead_code)] // fixme: use this
    pub(crate) fn report_resource_usage(
        &mut self,
        attribution: &AttributionSource,
        resource: ResourceType,
        amount: f64,
        at_time: Instant,
    ) {
        if let Some(creation_time) = self.source_creation_times.get(attribution) {
            if at_time < *creation_time {
                self.source_creation_times
                    .insert(attribution.clone(), at_time);
            }
        } else {
            self.source_creation_times
                .insert(attribution.clone(), at_time);
        }

        self.meter.report(attribution, resource, amount, at_time);
    }

    /// Record an outbound request to a peer, along with the target Location of that request
    pub(crate) fn report_outbound_request(&mut self, peer: PeerKeyLocation, target: Location) {
        self.request_density_tracker.sample(target);
        self.outbound_request_counter.record_request(peer);
    }

    /// Calculate total usage for a resource type extrapolating usage for resources that
    /// are younger than [SOURCE_RAMP_UP_DURATION]
    fn extrapolated_usage(&mut self, resource_type: &ResourceType, now: Instant) -> Usage {
        let function_span = span!(Level::DEBUG, "extrapolated_usage_function");
        let _enter = function_span.enter();

        let mut total_usage: Rate = Rate::new_per_second(0.0);
        let mut usage_per_source: BTreeMap<AttributionSource, Rate> = BTreeMap::new();

        // Step 1: Collect data
        let collect_data_span = span!(Level::DEBUG, "collect_data");
        let _collect_data_guard = collect_data_span.enter();
        debug!("Collecting data from source_creation_times");
        let mut usage_data = Vec::new();
        for (source, creation_time) in self.source_creation_times.iter() {
            let ramping_up = now.duration_since(*creation_time) <= SOURCE_RAMP_UP_DURATION;
            debug!(
                "Source: {:?}, Creation time: {:?}, Ramping up: {}",
                source, creation_time, ramping_up
            );
            usage_data.push((source.clone(), ramping_up));
        }
        drop(_collect_data_guard);

        // Step 2: Process data
        let process_data_span = span!(Level::DEBUG, "process_data");
        let _process_data_guard = process_data_span.enter();
        debug!("Processing data for usage calculation");
        for (source, ramping_up) in usage_data {
            let usage_rate: Option<Rate> = if ramping_up {
                debug!("Source {:?} is ramping up", source);
                self.meter.get_adjusted_usage_rate(resource_type, now)
            } else {
                debug!("Source {:?} is not ramping up", source);
                self.attributed_usage_rate(&source, resource_type, now)
            };
            debug!("Usage rate for source {:?}: {:?}", source, usage_rate);
            total_usage += usage_rate.unwrap_or(Rate::new_per_second(0.0));
            usage_per_source.insert(source, usage_rate.unwrap_or(Rate::new_per_second(0.0)));
        }
        drop(_process_data_guard);

        debug!("Total usage: {:?}", total_usage);
        debug!("Usage per source: {:?}", usage_per_source);

        Usage {
            total: total_usage,
            per_source: usage_per_source,
        }
    }

    pub(crate) fn attributed_usage_rate(
        &mut self,
        source: &AttributionSource,
        resource_type: &ResourceType,
        now: Instant,
    ) -> Option<Rate> {
        self.meter.attributed_usage_rate(source, resource_type, now)
    }

    /// Determine whether to add or remove connections based on current connection
    /// count and resource usage.
    ///
    /// When adding connections, targets are selected using gap-based targeting:
    /// the center of the largest gap in the node's connection distribution in
    /// log-distance space (see `small_world_rand::gap_target`).
    /// When own location is unknown, random targets are used as fallback.
    pub(crate) fn adjust_topology(
        &mut self,
        neighbor_locations: &BTreeMap<Location, Vec<Connection>>,
        my_location: &Option<Location>,
        at_time: Instant,
        current_connections: usize,
    ) -> TopologyAdjustment {
        // Below min_connections: add connections to reach the minimum.
        if current_connections < self.limits.min_connections {
            let needed = self.limits.min_connections - current_connections;

            // With 5+ connections, use gap-based targeting
            if current_connections >= DENSITY_SELECTION_THRESHOLD {
                let locations = Self::sample_targets(my_location, neighbor_locations, needed);
                return TopologyAdjustment::AddConnections(locations);
            }

            let locations = bootstrap_target_locations(my_location, current_connections, needed);

            #[cfg(debug_assertions)]
            if current_connections == 0 {
                thread_local! {
                    static LAST_LOG: std::cell::RefCell<Instant> = std::cell::RefCell::new(Instant::now());
                }
                if LAST_LOG.with(|last_log| {
                    last_log.borrow().elapsed() > std::time::Duration::from_secs(10)
                }) {
                    LAST_LOG.with(|last_log| {
                        tracing::trace!(
                            minimum_num_peers_hard_limit = self.limits.min_connections,
                            num_peers = current_connections,
                            to_add = needed,
                            "Bootstrap: adding first connection at own location"
                        );
                        *last_log.borrow_mut() = Instant::now();
                    });
                }
            }

            return TopologyAdjustment::AddConnections(locations);
        }

        // At/above min_connections: use resource usage to decide.
        let increase_usage_if_below: RateProportion =
            RateProportion::new(MINIMUM_DESIRED_RESOURCE_USAGE_PROPORTION);
        let decrease_usage_if_above: RateProportion =
            RateProportion::new(MAXIMUM_DESIRED_RESOURCE_USAGE_PROPORTION);

        let (resource_type, usage_proportion) = self.calculate_usage_proportion(at_time);

        // Track whether we should suppress topology swaps. Swaps should NOT
        // fire when resource usage is high and we're pinned at min_connections
        // (Codex review catch: don't churn connections under load).
        let mut suppress_swap = false;

        let adjustment: anyhow::Result<TopologyAdjustment> =
            if current_connections > self.limits.max_connections {
                debug!(
                    current_connections,
                    max_connections = self.limits.max_connections,
                    "Above max connections, removing"
                );
                self.update_connection_acquisition_strategy(ConnectionAcquisitionStrategy::Slow);
                Ok(self.select_connections_to_remove(&resource_type, at_time))
            } else if usage_proportion < increase_usage_if_below {
                debug!(
                    resource_type = ?resource_type,
                    usage_proportion = ?usage_proportion,
                    "Resource usage below threshold, adding connection"
                );
                self.update_connection_acquisition_strategy(ConnectionAcquisitionStrategy::Fast);
                let locations = Self::sample_targets(my_location, neighbor_locations, 1);
                Ok(TopologyAdjustment::AddConnections(locations))
            } else if usage_proportion > decrease_usage_if_above {
                if current_connections <= self.limits.min_connections {
                    debug!(
                        current_connections,
                        min_connections = self.limits.min_connections,
                        "Resource usage high but at min_connections — not removing"
                    );
                    suppress_swap = true;
                    Ok(TopologyAdjustment::NoChange)
                } else {
                    debug!(
                        resource_type = ?resource_type,
                        usage_proportion = ?usage_proportion,
                        "Resource usage above threshold, removing connection"
                    );
                    Ok(self.select_connections_to_remove(&resource_type, at_time))
                }
            } else {
                Ok(TopologyAdjustment::NoChange)
            };

        // Enforce max-connections cap: if we're still over after the main logic,
        // use fallback removal (drop the most distant peer).
        if current_connections > self.limits.max_connections {
            let mut adj = adjustment.unwrap_or(TopologyAdjustment::NoChange);
            if matches!(adj, TopologyAdjustment::NoChange) {
                if let Some(peer) = select_fallback_peer_to_drop(neighbor_locations, my_location) {
                    info!(
                        current_connections,
                        max_connections = self.limits.max_connections,
                        peer = %peer,
                        "Enforcing max-connections cap via fallback removal"
                    );
                    adj = TopologyAdjustment::RemoveConnections(vec![peer]);
                } else {
                    warn!(
                        current_connections,
                        max_connections = self.limits.max_connections,
                        "Over capacity but no removable peer found"
                    );
                }
            }
            return adj;
        }

        let adj = adjustment.unwrap_or(TopologyAdjustment::NoChange);

        // Topology swap: when not removing connections and not under resource
        // pressure at min_connections, check whether replacing the least-routed
        // connection would improve the Kleinberg distribution. The swap's own
        // min_connections guard (in maybe_swap_connection) prevents swaps during
        // bootstrap. When the main logic returns AddConnections, the swap can
        // override it — the node already has min_connections so the swap's
        // topology improvement is more valuable than growing beyond min.
        if !suppress_swap && !matches!(adj, TopologyAdjustment::RemoveConnections(_)) {
            let swap =
                self.maybe_swap_connection(my_location, neighbor_locations, current_connections);
            if !matches!(swap, TopologyAdjustment::NoChange) {
                return swap;
            }
        }

        adj
    }

    /// Sample `count` target locations using gap-based targeting.
    ///
    /// Targets the center of the largest gap in the node's current connection
    /// distribution in log-distance space. Falls back to random Kleinberg 1/d
    /// sampling when own location is unknown or there are no existing connections.
    ///
    /// Note: when `count > 1`, all targets may cluster in the same gap since
    /// the distance snapshot is not updated between iterations. This is
    /// self-correcting: after connections succeed, subsequent calls to
    /// `adjust_topology` will see filled gaps and target different ranges.
    fn sample_targets(
        my_location: &Option<Location>,
        neighbor_locations: &BTreeMap<Location, Vec<Connection>>,
        count: usize,
    ) -> Vec<Location> {
        match my_location {
            Some(loc) => {
                let distances: Vec<f64> = neighbor_locations
                    .keys()
                    .map(|nloc| loc.distance(*nloc).as_f64())
                    .collect();
                (0..count)
                    .map(|_| small_world_rand::gap_target(*loc, distances.iter().copied()))
                    .collect()
            }
            None => (0..count).map(|_| Location::random()).collect(),
        }
    }

    fn calculate_usage_proportion(&mut self, at_time: Instant) -> (ResourceType, RateProportion) {
        let mut usage_rate_per_type = BTreeMap::new();
        for resource_type in ResourceType::all() {
            let usage = self.extrapolated_usage(&resource_type, at_time);
            let proportion = usage.total.proportion_of(&self.limits.get(&resource_type));
            usage_rate_per_type.insert(resource_type, proportion);
        }

        let max_usage_rate = usage_rate_per_type
            .iter()
            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(Ordering::Equal))
            .unwrap();

        (*max_usage_rate.0, *max_usage_rate.1)
    }

    fn update_connection_acquisition_strategy(
        &mut self,
        new_strategy: ConnectionAcquisitionStrategy,
    ) {
        self.connection_acquisition_strategy = new_strategy;
    }

    fn select_connections_to_remove(
        &mut self,
        exceeded_usage_for_resource_type: &ResourceType,
        at_time: Instant,
    ) -> TopologyAdjustment {
        let function_span = span!(Level::INFO, "remove_connections");
        let _enter = function_span.enter();

        let mut worst: Option<(PeerKeyLocation, f64)> = None;

        for (source, source_usage) in self
            .meter
            .get_usage_rates(exceeded_usage_for_resource_type, at_time)
        {
            let loop_span = span!(Level::DEBUG, "source_loop", ?source);
            let _loop_enter = loop_span.enter();

            event!(Level::DEBUG, "Checking source");

            if let Some(creation_time) = self.source_creation_times.get(&source) {
                if Instant::now().duration_since(*creation_time) <= SOURCE_RAMP_UP_DURATION {
                    event!(Level::DEBUG, "Source is in ramp-up time, skipping");
                    continue;
                }
            } else {
                event!(Level::DEBUG, "No creation time for source, skipping");
                continue;
            }

            match source {
                AttributionSource::Peer(peer) => {
                    let peer_span = span!(Level::DEBUG, "peer_processing", ?peer);
                    let _peer_enter = peer_span.enter();

                    let request_count =
                        self.outbound_request_counter.get_request_count(&peer) as f64;

                    let value_per_usage = request_count / source_usage.per_second();

                    event!(
                        Level::DEBUG,
                        request_count = request_count,
                        usage = source_usage.per_second(),
                        value_per_usage = value_per_usage
                    );

                    if let Some((_, worst_value_per_usage)) = worst {
                        if value_per_usage < worst_value_per_usage {
                            worst = Some((peer, value_per_usage));
                            event!(Level::DEBUG, "Found a worse peer");
                        }
                    } else {
                        worst = Some((peer, value_per_usage));
                        event!(Level::DEBUG, "Setting initial worst peer");
                    }
                }
                _ => {
                    event!(Level::DEBUG, "Non-peer source, skipping");
                }
            }
        }

        if let Some((peer, _)) = worst {
            event!(Level::INFO, action = "Recommend peer for removal", peer = ?peer);
            TopologyAdjustment::RemoveConnections(vec![peer])
        } else {
            event!(Level::WARN, "Couldn't find a suitable peer to remove");
            TopologyAdjustment::NoChange
        }
    }

    /// Probabilistically decide whether to swap a connection to improve topology.
    ///
    /// Computes the largest gap in the current connection distribution in
    /// log-distance space, compares it to the expected gap for an ideal
    /// Kleinberg distribution, and triggers a swap with probability
    /// proportional to the excess. The connection to drop is the one with
    /// the fewest routed requests (least useful to the routing algorithm).
    fn maybe_swap_connection(
        &self,
        my_location: &Option<Location>,
        neighbor_locations: &BTreeMap<Location, Vec<Connection>>,
        current_connections: usize,
    ) -> TopologyAdjustment {
        if current_connections < self.limits.min_connections {
            return TopologyAdjustment::NoChange;
        }

        let Some(&my_loc) = my_location.as_ref() else {
            return TopologyAdjustment::NoChange;
        };

        let distances: Vec<f64> = neighbor_locations
            .keys()
            .map(|nloc| my_loc.distance(*nloc).as_f64())
            .collect();

        let largest_gap = small_world_rand::largest_gap_size(distances.iter().copied());

        // Expected largest gap for k uniform points on [0,1]: ~ln(k)/k.
        // Use the actual number of distinct distances in the gap analysis,
        // not the total connection count, since BTreeMap keys are deduplicated.
        let k = distances.len() as f64;
        let expected_gap = k.ln() / k;

        // Swap probability proportional to how much the gap exceeds expected.
        // When gap == expected: excess = 0, no swap.
        // When gap == 2x expected: excess = 1.0, swap at MAX_SWAP_PROB_PER_TICK.
        let excess = ((largest_gap / expected_gap) - 1.0).clamp(0.0, 1.0);
        let swap_prob = excess * MAX_SWAP_PROB_PER_TICK;

        if swap_prob <= 0.0 {
            return TopologyAdjustment::NoChange;
        }

        let roll: f64 = crate::config::GlobalRng::random_range(0.0..1.0);
        if roll >= swap_prob {
            trace!(
                largest_gap,
                expected_gap,
                excess,
                swap_prob,
                roll,
                "Topology swap check: not triggered"
            );
            return TopologyAdjustment::NoChange;
        }

        // Find the least-routed peer to drop.
        let least_routed = neighbor_locations
            .values()
            .flatten()
            .min_by_key(|conn| {
                self.outbound_request_counter
                    .get_request_count(&conn.location)
            })
            .map(|conn| conn.location.clone());

        let Some(remove) = least_routed else {
            return TopologyAdjustment::NoChange;
        };

        // Target the largest gap for the replacement connection.
        let add_location = small_world_rand::gap_target(my_loc, distances.iter().copied());

        info!(
            largest_gap,
            expected_gap,
            excess,
            swap_prob,
            remove_peer = %remove,
            add_target = %add_location,
            "Topology swap triggered: replacing least-routed peer with gap-targeted connection"
        );

        TopologyAdjustment::SwapConnection {
            remove,
            add_location,
        }
    }
}

/// Select target locations for bootstrap (0 to DENSITY_SELECTION_THRESHOLD-1 connections).
///
/// - **0 connections**: single target at own location (or random if unknown).
/// - **1..DENSITY_SELECTION_THRESHOLD-1**: own location + evenly spaced ring targets.
///   Distinct locations ensure all targets survive BTreeSet deduplication in the caller.
fn bootstrap_target_locations(
    my_location: &Option<Location>,
    current_connections: usize,
    needed: usize,
) -> Vec<Location> {
    debug_assert!(current_connections < DENSITY_SELECTION_THRESHOLD);

    if current_connections == 0 {
        // First connection targets own location to help clustering
        return match my_location {
            Some(location) => vec![*location],
            None => vec![Location::random()],
        };
    }

    // Spread targets: first at own location, rest evenly spaced around the ring
    match my_location {
        Some(location) => {
            let mut locations = Vec::with_capacity(needed);
            locations.push(*location);
            for i in 1..needed {
                let offset = i as f64 / needed as f64;
                locations.push(Location::new_rounded(location.as_f64() + offset));
            }
            locations
        }
        None => (0..needed).map(|_| Location::random()).collect(),
    }
}

fn select_fallback_peer_to_drop(
    neighbor_locations: &BTreeMap<Location, Vec<Connection>>,
    my_location: &Option<Location>,
) -> Option<PeerKeyLocation> {
    let mut candidate: Option<(PeerKeyLocation, f64)> = None;
    for (loc, conns) in neighbor_locations.iter() {
        for conn in conns {
            let score = match my_location {
                Some(me) => me.distance(*loc).as_f64(),
                None => 0.0,
            };
            if let Some((_, best_score)) = &mut candidate {
                if score > *best_score {
                    *best_score = score;
                    candidate = Some((conn.location.clone(), score));
                }
            } else {
                candidate = Some((conn.location.clone(), score));
            }
        }
    }
    candidate.map(|(peer, _)| peer)
}

#[derive(PartialEq, Debug, Clone, Copy)]
pub(crate) enum ConnectionAcquisitionStrategy {
    /// Acquire new connections slowly, be picky
    Slow,

    /// Acquire new connections aggressively, be less picky
    Fast,
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_topology() {
        const NUM_REQUESTS: usize = 5_000;
        let mut topology_manager = TopologyManager::new(Limits {
            max_upstream_bandwidth: Rate::new_per_second(1000.0),
            max_downstream_bandwidth: Rate::new_per_second(1000.0),
            min_connections: 5,
            max_connections: 200,
        });
        let mut current_neighbors = std::collections::BTreeMap::new();

        // Insert neighbors from 0.0 to 0.9
        for i in 0..10 {
            current_neighbors.insert(Location::new(i as f64 / 10.0), vec![]);
        }

        let this_peer_location = Location::new(0.39);

        // Simulate a bunch of random requests clustered around 0.35
        for _ in 0..NUM_REQUESTS {
            let requested_location = random_location(&random_location(&this_peer_location));
            // todo: Is PeerKeyLocation unimportant for this test?
            topology_manager.record_request(
                PeerKeyLocation::random(),
                requested_location,
                TransactionType::Get,
            );
        }

        topology_manager
            .cached_density_map
            .set(
                &topology_manager.request_density_tracker,
                &current_neighbors,
            )
            .unwrap();

        let best_candidate_location = topology_manager
            .get_best_candidate_location(&this_peer_location)
            .unwrap();
        // Should be half way between 0.3 and 0.4 as that is where the most requests were.
        assert_eq!(best_candidate_location, Location::new(0.35));

        // Find the location with the highest density score across 0.0 to 1.0
        let mut best_score = 0.0;
        let mut best_location = Location::new(0.0);
        for i in 0..100 {
            let candidate_location = Location::new(i as f64 / 100.0);
            let score = topology_manager
                .cached_density_map
                .get()
                .unwrap()
                .get_density_at(candidate_location)
                .unwrap();
            if score > best_score {
                best_score = score;
                best_location = candidate_location;
            }
        }

        // Best location should be 0.4 as that is closest to 0.39 which is the peer's location and
        // the request epicenter
        assert_eq!(best_location, Location::new(0.4));
    }

    /// Generates a random location that is close to the current peer location with a small
    /// world distribution.
    fn random_location(this_peer_location: &Location) -> Location {
        use crate::config::GlobalRng;
        tracing::debug!("Generating random location");
        let distance = small_world_rand::test_utils::random_link_distance(Distance::new(0.01));
        let location_f64 = if GlobalRng::random_bool(0.5) {
            this_peer_location.as_f64() - distance.as_f64()
        } else {
            this_peer_location.as_f64() + distance.as_f64()
        };
        let location_f64 = location_f64.rem_euclid(1.0);
        Location::new(location_f64)
    }

    use super::*;
    use crate::ring::Distance;
    use std::time::Duration;

    #[test_log::test]
    fn test_resource_manager_report() {
        // Create a TopologyManager with arbitrary limits
        let limits = Limits {
            max_upstream_bandwidth: Rate::new_per_second(1000.0),
            max_downstream_bandwidth: Rate::new_per_second(1000.0),
            max_connections: 200,
            min_connections: 5,
        };
        let mut resource_manager = TopologyManager::new(limits);

        // Report some usage and test that the total and attributed usage are updated
        let attribution = AttributionSource::Peer(PeerKeyLocation::random());
        let now = Instant::now();
        resource_manager.report_resource_usage(
            &attribution,
            ResourceType::InboundBandwidthBytes,
            100.0,
            now,
        );
        assert_eq!(
            resource_manager
                .meter
                .attributed_usage_rate(&attribution, &ResourceType::InboundBandwidthBytes, now)
                .unwrap()
                .per_second(),
            100.0
        );
    }

    #[test_log::test]
    fn test_remove_connections() {
        let mut resource_manager = setup_topology_manager(1000.0);
        // Need 6+ peers because the removal guard prevents dropping below min_connections (5).
        // With 6 peers: current_connections=6 > min_connections=5, so removal is allowed.
        let peers = generate_random_peers(6);
        // Total bw usage will be way higher than the limit of 1000
        let bw_usage_by_peer = vec![1000, 1100, 1200, 2000, 1600, 1300];
        // Report usage from outside the ramp-up time window so it isn't ignored
        let report_time = Instant::now() - SOURCE_RAMP_UP_DURATION - Duration::from_secs(30);
        report_resource_usage(
            &mut resource_manager,
            &peers,
            &bw_usage_by_peer,
            report_time,
        );
        let requests_per_peer = vec![20, 19, 18, 9, 9, 15];
        report_outbound_requests(&mut resource_manager, &peers, &requests_per_peer);
        let worst_ix = find_worst_peer(&peers, &bw_usage_by_peer, &requests_per_peer);
        assert_eq!(worst_ix, 3);
        let worst_peer = &peers[worst_ix];
        let mut neighbor_locations = BTreeMap::new();
        for peer in &peers {
            neighbor_locations.insert(peer.location().unwrap(), vec![]);
        }

        let adjustment = resource_manager.adjust_topology(
            &neighbor_locations,
            &None,
            Instant::now(),
            peers.len(),
        );
        match adjustment {
            TopologyAdjustment::RemoveConnections(peers) => {
                assert_eq!(peers.len(), 1);
                assert_eq!(peers[0], *worst_peer);
            }
            TopologyAdjustment::AddConnections(_)
            | TopologyAdjustment::NoChange
            | TopologyAdjustment::SwapConnection { .. } => {
                panic!("Expected to remove a peer, adjustment was {adjustment:?}")
            }
        }
    }

    #[test_log::test]
    fn test_add_connections() {
        let mut resource_manager = setup_topology_manager(1000.0);
        // Generate 5 peers with locations specified in a vec!
        let peers: Vec<PeerKeyLocation> = generate_random_peers(5);

        // Total bw usage will be way lower than MINIMUM_DESIRED_RESOURCE_USAGE_PROPORTION, triggering
        // the TopologyManager to add a connection
        let bw_usage_by_peer = vec![10, 20, 30, 25, 30];
        // Report usage from outside the ramp-up time window so it isn't ignored
        let report_time = Instant::now() - SOURCE_RAMP_UP_DURATION - Duration::from_secs(30);
        report_resource_usage(
            &mut resource_manager,
            &peers,
            &bw_usage_by_peer,
            report_time,
        );
        let requests_per_peer = vec![20, 19, 18, 9, 9];
        report_outbound_requests(&mut resource_manager, &peers, &requests_per_peer);

        let mut neighbor_locations = BTreeMap::new();
        for peer in &peers {
            neighbor_locations.insert(peer.location().unwrap(), vec![]);
        }

        let adjustment = resource_manager.adjust_topology(
            &neighbor_locations,
            &None,
            Instant::now(),
            peers.len(),
        );

        match adjustment {
            TopologyAdjustment::AddConnections(locations) => {
                assert_eq!(locations.len(), 1);
            }
            TopologyAdjustment::RemoveConnections(_)
            | TopologyAdjustment::NoChange
            | TopologyAdjustment::SwapConnection { .. } => {
                panic!("Expected to add a connection, adjustment was {adjustment:?}")
            }
        }
    }

    // Test with no adjustment because the usage is within acceptable bounds
    #[test_log::test]
    fn test_no_adjustment() {
        let mut resource_manager = setup_topology_manager(1000.0);
        let peers = generate_random_peers(5);
        // Total bw usage 750/1000 = 75%, within the 50-90% "no change" band
        let bw_usage_by_peer = vec![150, 200, 100, 100, 200];
        // Report usage from outside the ramp-up time window so it isn't ignored
        let report_time = Instant::now() - SOURCE_RAMP_UP_DURATION - Duration::from_secs(30);
        report_resource_usage(
            &mut resource_manager,
            &peers,
            &bw_usage_by_peer,
            report_time,
        );
        let requests_per_peer = vec![20, 19, 18, 9, 9];
        report_outbound_requests(&mut resource_manager, &peers, &requests_per_peer);

        let mut neighbor_locations = BTreeMap::new();
        for peer in &peers {
            neighbor_locations.insert(peer.location().unwrap(), vec![]);
        }

        let adjustment =
            resource_manager.adjust_topology(&neighbor_locations, &None, report_time, peers.len());

        match adjustment {
            TopologyAdjustment::NoChange => {}
            TopologyAdjustment::AddConnections(_)
            | TopologyAdjustment::RemoveConnections(_)
            | TopologyAdjustment::SwapConnection { .. } => {
                panic!("Expected no adjustment, adjustment was {adjustment:?}")
            }
        }
    }

    // Test that connections are never removed when at or below min_connections,
    // even when resource usage is high. This prevents topology destabilization
    // in small networks where every connection is critical.
    #[test_log::test]
    fn test_no_removal_at_min_connections() {
        // Use min_connections = 5 to test the inner guard in the removal branch.
        let limits = Limits {
            max_upstream_bandwidth: Rate::new_per_second(100000.0),
            max_downstream_bandwidth: Rate::new_per_second(1000.0),
            max_connections: 200,
            min_connections: 5,
        };
        let mut resource_manager = TopologyManager::new(limits);
        let peers = generate_random_peers(5);
        // Very high bandwidth usage (way above the 90% threshold)
        let bw_usage_by_peer = vec![2000, 2000, 2000, 2000, 2000];
        let report_time = Instant::now() - SOURCE_RAMP_UP_DURATION - Duration::from_secs(30);
        report_resource_usage(
            &mut resource_manager,
            &peers,
            &bw_usage_by_peer,
            report_time,
        );
        let requests_per_peer = vec![5, 5, 5, 5, 5];
        report_outbound_requests(&mut resource_manager, &peers, &requests_per_peer);

        let mut neighbor_locations = BTreeMap::new();
        for peer in &peers {
            neighbor_locations.insert(peer.location().unwrap(), vec![]);
        }

        // At min_connections (5) with 5 connections:
        // - current(5) >= min(5), enters resource evaluation
        // - High usage triggers removal branch
        // - Inner guard: current(5) <= min(5) → NoChange (not removal)
        let adjustment = resource_manager.adjust_topology(
            &neighbor_locations,
            &None,
            Instant::now(),
            5, // exactly at min_connections
        );
        assert!(
            !matches!(adjustment, TopologyAdjustment::RemoveConnections(_)),
            "Should not remove connections when at min_connections, got {adjustment:?}"
        );

        // Below min_connections should add, not remove
        let adjustment = resource_manager.adjust_topology(
            &neighbor_locations,
            &None,
            Instant::now(),
            3, // below min_connections
        );
        match adjustment {
            TopologyAdjustment::AddConnections(_) => {}
            TopologyAdjustment::RemoveConnections(_)
            | TopologyAdjustment::NoChange
            | TopologyAdjustment::SwapConnection { .. } => {
                panic!("Expected AddConnections when below min, got {adjustment:?}")
            }
        }

        // With 6 connections and min=5: passes threshold, enters resource eval.
        // Since current(6) > min(5), the inner guard allows removal.
        let adjustment = resource_manager.adjust_topology(
            &neighbor_locations,
            &None,
            Instant::now(),
            6, // above min_connections
        );
        assert!(
            matches!(adjustment, TopologyAdjustment::RemoveConnections(_)),
            "Should allow removal when above min_connections, got {adjustment:?}"
        );
    }

    // Test with no peers: bootstrap should target own location
    #[test_log::test]
    fn test_no_peers() {
        let mut resource_manager = setup_topology_manager(1000.0);
        let neighbor_locations = BTreeMap::new();
        let my_location = Location::new(0.5);

        let adjustment = resource_manager.adjust_topology(
            &neighbor_locations,
            &Some(my_location),
            Instant::now(),
            0,
        );

        match adjustment {
            TopologyAdjustment::AddConnections(v) => {
                // Zero connections: bootstrap returns single target at own location
                assert_eq!(v.len(), 1);
                assert_eq!(
                    v[0], my_location,
                    "First bootstrap target should be own location"
                );
            }
            TopologyAdjustment::RemoveConnections(_)
            | TopologyAdjustment::NoChange
            | TopologyAdjustment::SwapConnection { .. } => {
                panic!("Expected AddConnections, but was: {adjustment:?}")
            }
        }
    }

    // Test that resource-based addition uses gap-based targets biased toward own location.
    #[test_log::test]
    fn test_resource_based_add_uses_gap_targets() {
        let _guard = crate::config::GlobalRng::seed_guard(0xBEEF_CAFE);
        let mut resource_manager = setup_topology_manager(1000.0);
        let peers: Vec<PeerKeyLocation> = generate_random_peers(6);
        // Low bandwidth to trigger "add connections" path
        let bw_usage_by_peer = vec![5, 5, 5, 5, 5, 5];
        let report_time = Instant::now() - SOURCE_RAMP_UP_DURATION - Duration::from_secs(30);
        report_resource_usage(
            &mut resource_manager,
            &peers,
            &bw_usage_by_peer,
            report_time,
        );
        let mut neighbor_locations = BTreeMap::new();
        for peer in &peers {
            neighbor_locations.insert(peer.location().unwrap(), vec![]);
        }

        let my_location = peers[0].location().unwrap();
        let mut close_count = 0;
        let trials = 20;
        for _ in 0..trials {
            let adjustment = resource_manager.adjust_topology(
                &neighbor_locations,
                &Some(my_location),
                Instant::now(),
                peers.len(),
            );

            match adjustment {
                TopologyAdjustment::AddConnections(locations) => {
                    assert_eq!(locations.len(), 1);
                    let dist_to_me = my_location.distance(locations[0]).as_f64();
                    if dist_to_me < 0.3 {
                        close_count += 1;
                    }
                }
                TopologyAdjustment::RemoveConnections(_)
                | TopologyAdjustment::NoChange
                | TopologyAdjustment::SwapConnection { .. } => {
                    panic!("Expected AddConnections, got {adjustment:?}")
                }
            }
        }
        // Kleinberg 1/d should produce mostly close targets
        assert!(
            close_count > trials / 2,
            "Expected most targets near my_location, got {close_count}/{trials} close"
        );
    }

    fn setup_topology_manager(max_downstream_rate: f64) -> TopologyManager {
        let limits = Limits {
            // This won't be used
            max_upstream_bandwidth: Rate::new_per_second(100000.0),
            max_downstream_bandwidth: Rate::new_per_second(max_downstream_rate),
            max_connections: 200,
            min_connections: 5,
        };
        TopologyManager::new(limits)
    }

    fn generate_random_peers(num_peers: usize) -> Vec<PeerKeyLocation> {
        let mut peers: Vec<PeerKeyLocation> =
            (0..num_peers).map(|_| PeerKeyLocation::random()).collect();
        // Sort by location so tests can make index-based assumptions about location ordering
        peers.sort_by(|a, b| {
            a.location()
                .unwrap()
                .partial_cmp(&b.location().unwrap())
                .unwrap()
        });
        peers
    }

    fn report_resource_usage(
        resource_manager: &mut TopologyManager,
        peers: &[PeerKeyLocation],
        bw_usage_by_peer: &[usize],
        up_to_time: Instant,
    ) {
        for (i, peer) in peers.iter().enumerate() {
            // Report usage for the last 10 minutes, which is 2X longer than the ramp-up time
            for seconds in 1..600 {
                let report_time = up_to_time - Duration::from_secs(600 - seconds);
                tracing::trace!(
                    "Reporting {} bytes of inbound bandwidth for peer {:?} at {:?}",
                    bw_usage_by_peer[i],
                    peer,
                    report_time
                );
                resource_manager.report_resource_usage(
                    &AttributionSource::Peer(peer.clone()),
                    ResourceType::InboundBandwidthBytes,
                    bw_usage_by_peer[i] as f64,
                    report_time,
                );
            }
        }
    }

    fn report_outbound_requests(
        resource_manager: &mut TopologyManager,
        peers: &[PeerKeyLocation],
        requests_per_peer: &[usize],
    ) {
        for (i, requests) in requests_per_peer.iter().enumerate() {
            for _ in 0..*requests {
                // For simplicity we'll just assume that the target location of the request is the
                // neighboring peer's own location
                resource_manager
                    .report_outbound_request(peers[i].clone(), peers[i].location().unwrap());
            }
        }
    }

    fn find_worst_peer(
        peers: &[PeerKeyLocation],
        bw_usage_by_peer: &[usize],
        requests_per_peer: &[usize],
    ) -> usize {
        let mut values = vec![];
        for ix in 0..peers.len() {
            let peer = peers[ix].clone();
            let value = requests_per_peer[ix] as f64 / bw_usage_by_peer[ix] as f64;
            values.push(value);
            debug!(
                "Peer {:?} has value {}/{} = {}",
                peer, requests_per_peer[ix], bw_usage_by_peer[ix], value
            );
        }
        let mut worst_ix = 0;
        for (ix, value) in values.iter().enumerate() {
            if *value < values[worst_ix] {
                worst_ix = ix;
            }
        }
        worst_ix
    }

    #[test]
    fn test_update_limits() {
        let limits = Limits {
            max_upstream_bandwidth: Rate::new_per_second(1000.0),
            max_downstream_bandwidth: Rate::new_per_second(1000.0),
            max_connections: 200,
            min_connections: 5,
        };
        let mut topology_manager = TopologyManager::new(limits);

        let new_limits = Limits {
            max_upstream_bandwidth: Rate::new_per_second(2000.0),
            max_downstream_bandwidth: Rate::new_per_second(2000.0),
            max_connections: 200,
            min_connections: 5,
        };
        topology_manager.update_limits(new_limits);

        assert_eq!(
            topology_manager.limits.max_upstream_bandwidth,
            Rate::new_per_second(2000.0)
        );
        assert_eq!(
            topology_manager.limits.max_downstream_bandwidth,
            Rate::new_per_second(2000.0)
        );
    }

    // Test that below DENSITY_SELECTION_THRESHOLD, bootstrap targets are evenly spaced.
    #[test_log::test]
    fn test_below_threshold_uses_bootstrap_targets() {
        let limits = Limits {
            max_upstream_bandwidth: Rate::new_per_second(1000.0),
            max_downstream_bandwidth: Rate::new_per_second(1000.0),
            max_connections: 200,
            min_connections: 25,
        };
        let mut topology_manager = TopologyManager::new(limits);

        let mut neighbor_locations = BTreeMap::new();
        let peer = PeerKeyLocation::random();
        neighbor_locations.insert(peer.location().unwrap(), vec![]);

        let my_location = Location::new(0.5);
        let adjustment = topology_manager.adjust_topology(
            &neighbor_locations,
            &Some(my_location),
            Instant::now(),
            1,
        );

        match adjustment {
            TopologyAdjustment::AddConnections(locations) => {
                // Should request 24 more connections to reach min of 25
                assert_eq!(locations.len(), 24);

                // First target should be own location
                assert_eq!(locations[0], my_location);

                // All locations must be distinct
                let as_set: std::collections::BTreeSet<Location> =
                    locations.iter().copied().collect();
                assert_eq!(
                    as_set.len(),
                    locations.len(),
                    "All bootstrap targets must be distinct"
                );
            }
            TopologyAdjustment::RemoveConnections(_)
            | TopologyAdjustment::NoChange
            | TopologyAdjustment::SwapConnection { .. } => {
                panic!("Expected AddConnections, got {adjustment:?}")
            }
        }
    }

    // Test that at/above DENSITY_SELECTION_THRESHOLD, Kleinberg targets are used.
    #[test_log::test]
    fn test_above_threshold_uses_kleinberg_targets() {
        let _guard = crate::config::GlobalRng::seed_guard(0xBEEF_CAFE);
        let limits = Limits {
            max_upstream_bandwidth: Rate::new_per_second(1000.0),
            max_downstream_bandwidth: Rate::new_per_second(1000.0),
            max_connections: 200,
            min_connections: 25,
        };
        let mut topology_manager = TopologyManager::new(limits);

        let mut neighbor_locations = BTreeMap::new();
        for _ in 0..5 {
            let peer = PeerKeyLocation::random();
            neighbor_locations.insert(peer.location().unwrap(), vec![]);
        }

        let my_location = Location::new(0.5);
        let adjustment = topology_manager.adjust_topology(
            &neighbor_locations,
            &Some(my_location),
            Instant::now(),
            5, // at DENSITY_SELECTION_THRESHOLD
        );

        match adjustment {
            TopologyAdjustment::AddConnections(locations) => {
                // Should request 20 more connections to reach min of 25
                assert_eq!(locations.len(), 20);

                // Kleinberg 1/d bias: majority should be short-distance
                let short_count = locations
                    .iter()
                    .filter(|loc| my_location.distance(**loc).as_f64() < 0.1)
                    .count();
                assert!(
                    short_count > locations.len() / 3,
                    "Expected short-distance bias, got {short_count}/{} close",
                    locations.len()
                );
            }
            TopologyAdjustment::RemoveConnections(_)
            | TopologyAdjustment::NoChange
            | TopologyAdjustment::SwapConnection { .. } => {
                panic!("Expected AddConnections, got {adjustment:?}")
            }
        }
    }

    // Test that needing 1 more connection below threshold produces a bootstrap target.
    #[test_log::test]
    fn test_single_bootstrap_target() {
        let limits = Limits {
            max_upstream_bandwidth: Rate::new_per_second(1000.0),
            max_downstream_bandwidth: Rate::new_per_second(1000.0),
            max_connections: 200,
            min_connections: 5,
        };
        let mut topology_manager = TopologyManager::new(limits);

        let mut neighbor_locations = BTreeMap::new();
        for _ in 0..4 {
            let peer = PeerKeyLocation::random();
            neighbor_locations.insert(peer.location().unwrap(), vec![]);
        }

        let my_location = Location::new(0.25);
        let adjustment = topology_manager.adjust_topology(
            &neighbor_locations,
            &Some(my_location),
            Instant::now(),
            4,
        );

        match adjustment {
            TopologyAdjustment::AddConnections(locations) => {
                // 4 connections, need 1 more: bootstrap returns own location
                assert_eq!(locations.len(), 1);
                assert_eq!(locations[0], my_location);
            }
            TopologyAdjustment::RemoveConnections(_)
            | TopologyAdjustment::NoChange
            | TopologyAdjustment::SwapConnection { .. } => {
                panic!("Expected AddConnections, got {adjustment:?}")
            }
        }
    }

    // Test that bootstrap targets wrap correctly near the ring boundary (1.0 wraps to 0.0).
    #[test_log::test]
    fn test_bootstrap_targets_wrap_near_boundary() {
        let limits = Limits {
            max_upstream_bandwidth: Rate::new_per_second(1000.0),
            max_downstream_bandwidth: Rate::new_per_second(1000.0),
            max_connections: 200,
            min_connections: 5,
        };
        let mut topology_manager = TopologyManager::new(limits);

        let mut neighbor_locations = BTreeMap::new();
        let peer = PeerKeyLocation::random();
        neighbor_locations.insert(peer.location().unwrap(), vec![]);

        let my_location = Location::new(0.9);
        let adjustment = topology_manager.adjust_topology(
            &neighbor_locations,
            &Some(my_location),
            Instant::now(),
            1,
        );

        match adjustment {
            TopologyAdjustment::AddConnections(locations) => {
                // All locations must be valid (in [0, 1)) and distinct
                for loc in &locations {
                    let v = loc.as_f64();
                    assert!(
                        (0.0..1.0).contains(&v),
                        "Location {v} outside valid ring range [0, 1)"
                    );
                }
                let as_set: std::collections::BTreeSet<Location> =
                    locations.iter().copied().collect();
                assert_eq!(
                    as_set.len(),
                    locations.len(),
                    "All bootstrap targets must be distinct even when wrapping"
                );
            }
            TopologyAdjustment::RemoveConnections(_)
            | TopologyAdjustment::NoChange
            | TopologyAdjustment::SwapConnection { .. } => {
                panic!("Expected AddConnections, got {adjustment:?}")
            }
        }
    }

    // Test that when no location is known, we fall back to random locations
    #[test_log::test]
    fn test_no_location_falls_back_to_random() {
        let limits = Limits {
            max_upstream_bandwidth: Rate::new_per_second(1000.0),
            max_downstream_bandwidth: Rate::new_per_second(1000.0),
            max_connections: 200,
            min_connections: 25,
        };
        let mut topology_manager = TopologyManager::new(limits);

        let mut neighbor_locations = BTreeMap::new();
        let peer = PeerKeyLocation::random();
        neighbor_locations.insert(peer.location().unwrap(), vec![]);

        let adjustment =
            topology_manager.adjust_topology(&neighbor_locations, &None, Instant::now(), 1);

        match adjustment {
            TopologyAdjustment::AddConnections(locations) => {
                assert_eq!(locations.len(), 24);

                // Random locations should produce diverse values
                let unique_locations: std::collections::HashSet<_> = locations.iter().collect();
                assert!(
                    unique_locations.len() > 1,
                    "Random fallback should produce diverse locations, got {} unique out of {}",
                    unique_locations.len(),
                    locations.len()
                );
            }
            TopologyAdjustment::RemoveConnections(_)
            | TopologyAdjustment::NoChange
            | TopologyAdjustment::SwapConnection { .. } => {
                panic!("Expected AddConnections, got {adjustment:?}")
            }
        }
    }

    #[test]
    fn test_bootstrap_zero_connections_with_location() {
        let my_loc = Location::new(0.4);
        let locations = bootstrap_target_locations(&Some(my_loc), 0, 5);
        assert_eq!(
            locations.len(),
            1,
            "Zero connections should produce exactly one target"
        );
        assert_eq!(locations[0], my_loc, "Should target own location");
    }

    #[test]
    fn test_bootstrap_zero_connections_without_location() {
        let locations = bootstrap_target_locations(&None, 0, 5);
        assert_eq!(
            locations.len(),
            1,
            "Zero connections should produce exactly one target"
        );
    }

    #[test]
    fn test_bootstrap_few_connections_with_location() {
        let my_loc = Location::new(0.2);
        let needed = 4;
        let locations = bootstrap_target_locations(&Some(my_loc), 1, needed);
        assert_eq!(locations.len(), needed);

        // First target should be own location
        assert_eq!(locations[0], my_loc);

        // All targets should be distinct
        let unique: std::collections::HashSet<_> = locations.iter().collect();
        assert_eq!(
            unique.len(),
            needed,
            "All bootstrap targets should be distinct"
        );

        // Targets should be evenly spaced (offsets 0/4, 1/4, 2/4, 3/4)
        for (i, loc) in locations.iter().enumerate().skip(1) {
            let expected_offset = i as f64 / needed as f64;
            let expected = Location::new_rounded(my_loc.as_f64() + expected_offset);
            assert_eq!(*loc, expected, "Target {i} should be evenly spaced");
        }
    }

    #[test]
    fn test_bootstrap_few_connections_without_location() {
        let needed = 3;
        let locations = bootstrap_target_locations(&None, 2, needed);
        assert_eq!(locations.len(), needed);

        // All should be random but valid locations
        let unique: std::collections::HashSet<_> = locations.iter().collect();
        assert!(
            unique.len() > 1,
            "Random locations should generally be distinct"
        );
    }

    /// Test that topology swaps trigger when connections are clustered
    /// (large gap deviation) and don't trigger when well-distributed.
    #[test_log::test]
    fn test_topology_swap_triggers_on_clustered_connections() {
        let _guard = crate::config::GlobalRng::seed_guard(0xABCD_1234);
        let limits = Limits {
            max_upstream_bandwidth: Rate::new_per_second(100000.0),
            max_downstream_bandwidth: Rate::new_per_second(100000.0),
            max_connections: 200,
            min_connections: 10,
        };
        let mut tm = TopologyManager::new(limits);

        // Create 10 peers all clustered at very similar short distances from us.
        // This creates a huge gap in the long-distance part of log-space,
        // which should trigger swaps.
        let my_location = Location::new(0.5);
        let mut neighbor_locations = BTreeMap::new();
        for i in 0..10 {
            // All within distance 0.02 of us — heavily clustered
            let offset = 0.01 + (i as f64 * 0.001);
            let loc = Location::new(my_location.as_f64() + offset);
            let peer = PeerKeyLocation::random();
            tm.outbound_request_counter.record_request(peer.clone());
            neighbor_locations
                .entry(loc)
                .or_insert_with(Vec::new)
                .push(Connection::new(peer));
        }

        // Test maybe_swap_connection directly (bypasses resource meter).
        let mut swap_count = 0;
        let trials = 100;
        for _ in 0..trials {
            let adjustment = tm.maybe_swap_connection(&Some(my_location), &neighbor_locations, 10);
            if matches!(adjustment, TopologyAdjustment::SwapConnection { .. }) {
                swap_count += 1;
            }
        }
        assert!(
            swap_count > 0,
            "Expected at least one swap with clustered connections, got 0/{trials}"
        );
        // With max 10% probability per tick and a large gap, expect roughly 5-15 swaps
        assert!(
            swap_count < trials / 2,
            "Too many swaps ({swap_count}/{trials}) — probability should be capped"
        );
    }

    /// Test that topology swaps don't trigger when connections are well-distributed.
    #[test_log::test]
    fn test_topology_swap_no_trigger_when_well_distributed() {
        let _guard = crate::config::GlobalRng::seed_guard(0x600D_7090);
        let limits = Limits {
            max_upstream_bandwidth: Rate::new_per_second(100000.0),
            max_downstream_bandwidth: Rate::new_per_second(100000.0),
            max_connections: 200,
            min_connections: 10,
        };
        let mut tm = TopologyManager::new(limits);

        // Create 10 peers evenly distributed in log-distance space.
        let my_location = Location::new(0.5);
        let d_at = |u: f64| 0.01_f64 * (0.5_f64 / 0.01).powf(u); // D_MIN * (D_MAX/D_MIN)^u
        let mut neighbor_locations = BTreeMap::new();
        for i in 0..10 {
            let u = (i as f64 + 0.5) / 10.0; // evenly spaced in [0, 1]
            let dist = d_at(u);
            let loc = Location::new_rounded(my_location.as_f64() + dist);
            let peer = PeerKeyLocation::random();
            tm.outbound_request_counter.record_request(peer.clone());
            neighbor_locations
                .entry(loc)
                .or_insert_with(Vec::new)
                .push(Connection::new(peer));
        }

        // Test maybe_swap_connection directly — with well-distributed connections,
        // largest gap ≈ expected gap, so swap probability should be near zero.
        let mut swap_count = 0;
        let trials = 100;
        for _ in 0..trials {
            let adjustment = tm.maybe_swap_connection(&Some(my_location), &neighbor_locations, 10);
            if matches!(adjustment, TopologyAdjustment::SwapConnection { .. }) {
                swap_count += 1;
            }
        }
        assert!(
            swap_count <= 3,
            "Well-distributed topology should rarely trigger swaps, got {swap_count}/{trials}"
        );
    }

    /// Test that the swap selects the least-routed peer for removal.
    #[test_log::test]
    fn test_topology_swap_removes_least_routed() {
        let _guard = crate::config::GlobalRng::seed_guard(0x1EA5_70FE);
        let limits = Limits {
            max_upstream_bandwidth: Rate::new_per_second(100000.0),
            max_downstream_bandwidth: Rate::new_per_second(100000.0),
            max_connections: 200,
            min_connections: 5,
        };
        let mut tm = TopologyManager::new(limits);

        let my_location = Location::new(0.5);
        let mut neighbor_locations = BTreeMap::new();

        // Create 6 clustered peers. Give most of them high request counts
        // but one peer gets zero requests.
        let mut least_routed_peer = None;
        for i in 0..6 {
            let loc = Location::new(my_location.as_f64() + 0.01 + (i as f64 * 0.001));
            let peer = PeerKeyLocation::random();
            if i == 3 {
                // This peer gets no requests — should be the drop candidate
                least_routed_peer = Some(peer.clone());
            } else {
                // Give other peers many requests
                for _ in 0..50 {
                    tm.outbound_request_counter.record_request(peer.clone());
                }
            }
            neighbor_locations
                .entry(loc)
                .or_insert_with(Vec::new)
                .push(Connection::new(peer));
        }

        // Test maybe_swap_connection directly.
        let expected_drop = least_routed_peer.unwrap();
        let mut found_swap = false;
        for _ in 0..200 {
            let adjustment = tm.maybe_swap_connection(&Some(my_location), &neighbor_locations, 6);
            if let TopologyAdjustment::SwapConnection { remove, .. } = adjustment {
                assert_eq!(remove, expected_drop, "Should drop the least-routed peer");
                found_swap = true;
                break;
            }
        }
        assert!(found_swap, "Should have triggered a swap within 200 trials");
    }
}

#[derive(Debug, Clone)]
pub(crate) enum TopologyAdjustment {
    AddConnections(Vec<Location>),
    RemoveConnections(Vec<PeerKeyLocation>),
    /// Replace the least-routed connection with a new one targeting the largest gap.
    /// This allows the topology to converge toward the ideal Kleinberg distribution
    /// even after reaching the target connection count.
    SwapConnection {
        remove: PeerKeyLocation,
        add_location: Location,
    },
    NoChange,
}

struct Usage {
    total: Rate,
    #[allow(unused)]
    per_source: BTreeMap<AttributionSource, Rate>,
}

pub(crate) struct Limits {
    pub max_upstream_bandwidth: Rate,
    pub max_downstream_bandwidth: Rate,
    pub min_connections: usize,
    pub max_connections: usize,
}

impl Limits {
    pub fn get(&self, resource_type: &ResourceType) -> Rate {
        match resource_type {
            ResourceType::OutboundBandwidthBytes => self.max_upstream_bandwidth,
            ResourceType::InboundBandwidthBytes => self.max_downstream_bandwidth,
        }
    }
}
