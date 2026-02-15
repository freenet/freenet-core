//! Connection strategies: current Freenet algorithm and proposed small-world improvement.

use crate::network::{ring_distance, PeerContext};
use rand::Rng;

/// Dispatch enum for connection strategies.
pub enum Strategy {
    Current(Current),
    SmallWorld(SmallWorld),
}

impl Strategy {
    pub fn select_target(&self, ctx: &PeerContext, rng: &mut rand::rngs::ThreadRng) -> f64 {
        match self {
            Strategy::Current(s) => s.select_target(ctx, rng),
            Strategy::SmallWorld(s) => s.select_target(ctx, rng),
        }
    }

    pub fn accept_connection(
        &self,
        ctx: &PeerContext,
        candidate_loc: f64,
        min_connections: usize,
        max_connections: usize,
        rng: &mut rand::rngs::ThreadRng,
    ) -> bool {
        match self {
            Strategy::Current(s) => {
                s.accept_connection(ctx, candidate_loc, min_connections, max_connections, rng)
            }
            Strategy::SmallWorld(s) => {
                s.accept_connection(ctx, candidate_loc, min_connections, max_connections, rng)
            }
        }
    }

    pub fn select_drop(&self, ctx: &PeerContext, rng: &mut rand::rngs::ThreadRng) -> Option<usize> {
        match self {
            Strategy::Current(s) => s.select_drop(ctx, rng),
            Strategy::SmallWorld(s) => s.select_drop(ctx, rng),
        }
    }

    /// Fraction of connection attempts that should use direct introduction (sorted
    /// location lookup) instead of routing through existing topology. Models gateway
    /// introductions or peer exchange protocol.
    pub fn introduction_rate(&self) -> f64 {
        match self {
            Strategy::Current(_) => 0.0,
            Strategy::SmallWorld(_) => 0.5,
        }
    }
}

// ---------------------------------------------------------------------------
// Current Freenet algorithm (as of v0.1.128)
// ---------------------------------------------------------------------------

const DENSITY_SELECTION_THRESHOLD: usize = 5;

/// Reimplementation of the current Freenet connection selection algorithm.
pub struct Current;

impl Default for Current {
    fn default() -> Self {
        Self
    }
}

impl Current {
    /// Build a density map from request history, attributing each request to the nearest
    /// neighbor. Returns sorted vec of (location, count) pairs.
    fn build_density_map(ctx: &PeerContext) -> Vec<(f64, u32)> {
        if ctx.neighbor_locations.is_empty() {
            return Vec::new();
        }

        let mut sorted_locs: Vec<f64> = ctx.neighbor_locations.clone();
        sorted_locs.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let mut counts: Vec<u32> = vec![0; sorted_locs.len()];

        for &req in &ctx.recent_requests {
            let nearest_idx = sorted_locs
                .iter()
                .enumerate()
                .min_by(|(_, a), (_, b)| {
                    ring_distance(**a, req)
                        .partial_cmp(&ring_distance(**b, req))
                        .unwrap()
                })
                .map(|(i, _)| i)
                .unwrap();
            counts[nearest_idx] += 1;
        }

        sorted_locs.into_iter().zip(counts).collect()
    }

    /// Get density-weighted target: score = density/distance, stochastic sampling.
    fn density_weighted_target(
        ctx: &PeerContext,
        density_map: &[(f64, u32)],
        rng: &mut impl Rng,
    ) -> Option<f64> {
        if density_map.len() < 2 {
            return None;
        }

        const MIN_DISTANCE: f64 = 0.001;
        let mut candidates: Vec<(f64, f64)> = Vec::new();

        for i in 0..density_map.len() {
            let next = (i + 1) % density_map.len();
            let (loc_a, count_a) = density_map[i];
            let (loc_b, count_b) = density_map[next];

            let combined_density = (count_a + count_b) as f64;
            if combined_density == 0.0 {
                continue;
            }

            // Midpoint on ring
            let midpoint = if (loc_b - loc_a).abs() < 0.5 {
                (loc_a + loc_b) / 2.0
            } else {
                let mid = (loc_a + loc_b + 1.0) / 2.0;
                if mid >= 1.0 {
                    mid - 1.0
                } else {
                    mid
                }
            };

            let dist = ring_distance(ctx.location, midpoint).max(MIN_DISTANCE);
            let score = combined_density / dist;
            candidates.push((midpoint, score));
        }

        if candidates.is_empty() {
            return None;
        }

        let total: f64 = candidates.iter().map(|(_, s)| s).sum();
        if total <= 0.0 {
            return None;
        }
        let threshold = rng.random::<f64>() * total;
        let mut cumulative = 0.0;
        for (midpoint, score) in &candidates {
            cumulative += score;
            if cumulative >= threshold {
                return Some(*midpoint);
            }
        }
        candidates.last().map(|(m, _)| *m)
    }

    fn select_target(&self, ctx: &PeerContext, rng: &mut impl Rng) -> f64 {
        if ctx.connection_count < DENSITY_SELECTION_THRESHOLD {
            return ctx.location;
        }

        let density_map = Self::build_density_map(ctx);
        if let Some(target) = Self::density_weighted_target(ctx, &density_map, rng) {
            target
        } else {
            rng.random()
        }
    }

    fn accept_connection(
        &self,
        ctx: &PeerContext,
        _candidate_loc: f64,
        _min_connections: usize,
        max_connections: usize,
        _rng: &mut impl Rng,
    ) -> bool {
        ctx.connection_count < max_connections
    }

    fn select_drop(&self, ctx: &PeerContext, _rng: &mut impl Rng) -> Option<usize> {
        ctx.connection_ids
            .iter()
            .zip(ctx.neighbor_locations.iter())
            .max_by(|(_, loc_a), (_, loc_b)| {
                ring_distance(ctx.location, **loc_a)
                    .partial_cmp(&ring_distance(ctx.location, **loc_b))
                    .unwrap()
            })
            .map(|(id, _)| *id)
    }
}

// ---------------------------------------------------------------------------
// Proposed small-world strategy
// ---------------------------------------------------------------------------

/// Proposed strategy: Kleinberg 1/d link generation + peer introduction mechanism
/// + nearest-neighbor protection + redundancy-based drop selection.
pub struct SmallWorld;

impl Default for SmallWorld {
    fn default() -> Self {
        Self
    }
}

impl SmallWorld {
    /// Generate a random link distance following Kleinberg's d^{-1} distribution.
    fn random_link_distance(rng: &mut impl Rng) -> f64 {
        let d_min: f64 = 0.001;
        let d_max: f64 = 0.5;
        let u: f64 = rng.random();
        d_min * (d_max / d_min).powf(u)
    }

    fn kleinberg_target(location: f64, rng: &mut impl Rng) -> f64 {
        let dist = Self::random_link_distance(rng);
        if rng.random::<bool>() {
            (location + dist) % 1.0
        } else {
            (location - dist + 1.0) % 1.0
        }
    }

    fn select_target(&self, ctx: &PeerContext, rng: &mut impl Rng) -> f64 {
        // First priority: connect to nearest neighbors if not connected
        for &(id, loc) in &ctx.nearest_peers {
            if !ctx.connection_ids.contains(&id) {
                return loc;
            }
        }

        Self::kleinberg_target(ctx.location, rng)
    }

    fn accept_connection(
        &self,
        ctx: &PeerContext,
        _candidate_loc: f64,
        _min_connections: usize,
        max_connections: usize,
        _rng: &mut impl Rng,
    ) -> bool {
        // Simple capacity-based acceptance. The topology is shaped by target
        // selection (Kleinberg 1/d), not by acceptance filtering. Filtering
        // here would destroy the carefully-chosen distance distribution.
        ctx.connection_count < max_connections
    }

    fn select_drop(&self, ctx: &PeerContext, _rng: &mut impl Rng) -> Option<usize> {
        let nearest_3: std::collections::HashSet<usize> =
            ctx.nearest_peers.iter().map(|(id, _)| *id).collect();

        // Collect droppable connections with their distances, sorted by distance
        let mut droppable: Vec<(usize, f64)> = ctx
            .connection_ids
            .iter()
            .zip(ctx.neighbor_locations.iter())
            .filter(|(id, _)| !nearest_3.contains(id))
            .map(|(&id, &loc)| (id, ring_distance(ctx.location, loc)))
            .collect();

        if droppable.is_empty() {
            return None;
        }

        droppable.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        // Drop the most redundant connection: the one with the smallest distance
        // gap to its neighbor in distance-space. This preserves distance diversity
        // (keeping both short and long-range links) instead of always killing the
        // longest connection.
        if droppable.len() >= 2 {
            let mut most_redundant_idx = 0;
            let mut min_gap = f64::MAX;
            for i in 0..droppable.len() - 1 {
                let gap = droppable[i + 1].1 - droppable[i].1;
                if gap < min_gap {
                    min_gap = gap;
                    // Drop the further one of the redundant pair (it's more replaceable)
                    most_redundant_idx = i + 1;
                }
            }
            Some(droppable[most_redundant_idx].0)
        } else {
            Some(droppable[0].0)
        }
    }
}
