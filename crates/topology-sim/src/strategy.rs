//! Connection strategies: current Freenet algorithm and proposed small-world improvement.
//!
//! ## SmallWorld strategy — theoretical basis
//!
//! Based on Kleinberg (2000): on a 1D ring with n nodes, if each node's long-range
//! contacts follow a d^{-1} distance distribution, greedy routing succeeds in
//! O(log²n) hops. Any other exponent gives polynomial routing time.
//!
//! **Design decisions and their justifications:**
//!
//! 1. **Target selection: Kleinberg 1/d** — Proven optimal for greedy routing on a ring.
//!    Inverse CDF sampling: d = d_min × (d_max/d_min)^U, U ~ Uniform(0,1).
//!
//! 2. **d_min = distance to k-th nearest protected neighbor** — The k nearest ring
//!    neighbors are deterministically connected (nearest-neighbor protection). Kleinberg
//!    long-range links should cover distances beyond this protected zone. Using the
//!    actual observed distance (not a multiplier) scales naturally with network density
//!    (k-th nearest ≈ k/n on a uniform ring).
//!
//! 3. **d_max = 0.5** — Maximum possible ring distance. Not a parameter.
//!
//! 4. **k = 3 nearest-neighbor protection** — Minimum 2 for ring connectivity (left +
//!    right neighbor). k=3 adds one layer of redundancy. Protected peers are never
//!    dropped, ensuring local connectivity.
//!
//! 5. **Capacity-only acceptance** — The topology is shaped entirely by target selection
//!    (Kleinberg 1/d). Acceptance filtering would distort the distribution.
//!
//! 6. **Log-distance redundancy drop** — The 1/d distribution is uniform in log-distance
//!    space (CDF ∝ ln d). Ideal connections are equally spaced in log(d). Drop the
//!    connection with the smallest log-distance gap to its neighbor — it contributes
//!    least to the distribution's coverage of distance scales.
//!
//! 7. **Introduction (gateway-assisted discovery)** — Kleinberg's model assumes contacts
//!    drawn directly from 1/d. In practice, discovery happens via routing (which depends
//!    on existing topology) or external mechanisms (gateways, peer exchange). During
//!    bootstrap (below min_connections), routing through sparse connections is unreliable,
//!    so introduction (modeling gateway discovery) is used. At steady state, routing
//!    through the established 1/d topology is used, with introduction as fallback when
//!    routing fails. This is self-regulating: bad topology → routing fails → more
//!    introductions → better topology → routing succeeds → fewer introductions.
//!
//! ## Simulation model limitations
//!
//! The introduction mechanism (`find_peer_near`) uses a global sorted index, which is
//! an upper bound on what a real gateway can achieve. The `Current` strategy does not
//! use introduction (matching current Freenet behavior), making the comparison asymmetric
//! in discovery capability. The SmallWorld results demonstrate what is achievable *given*
//! a gateway that can place connections near Kleinberg-sampled targets — the production
//! gateway protocol must approximate this capability for the results to transfer.

use crate::network::{ring_distance, PeerContext};
use rand::Rng;

/// Dispatch enum for connection strategies.
pub enum Strategy {
    Current(Current),
    SmallWorld(SmallWorld),
}

impl Strategy {
    pub fn select_target(&self, ctx: &PeerContext, rng: &mut rand::rngs::StdRng) -> f64 {
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
        rng: &mut rand::rngs::StdRng,
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

    pub fn select_drop(&self, ctx: &PeerContext, rng: &mut rand::rngs::StdRng) -> Option<usize> {
        match self {
            Strategy::Current(s) => s.select_drop(ctx, rng),
            Strategy::SmallWorld(s) => s.select_drop(ctx, rng),
        }
    }

    /// Whether this strategy uses introduction (gateway-assisted discovery) during
    /// bootstrap (below min_connections) and as fallback when routing fails.
    pub fn uses_introduction(&self) -> bool {
        match self {
            Strategy::Current(_) => false,
            Strategy::SmallWorld(_) => true,
        }
    }
}

// ---------------------------------------------------------------------------
// Current Freenet algorithm (as of v0.1.128)
// ---------------------------------------------------------------------------

const DENSITY_SELECTION_THRESHOLD: usize = 5;

/// Reimplementation of the current Freenet connection selection algorithm.
pub struct Current;

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

/// Proposed strategy based on Kleinberg (2000). See module-level documentation
/// for theoretical justification of each design decision.
pub struct SmallWorld;

impl SmallWorld {
    /// Generate a random link distance following Kleinberg's d^{-1} distribution.
    ///
    /// d_min is set to the distance to the k-th protected nearest neighbor.
    /// This is principled: the nearest-k peers are connected via deterministic
    /// protection, so Kleinberg sampling starts just beyond that boundary.
    /// On a uniform ring, the k-th nearest is at ~k/n, which scales naturally
    /// with network density.
    ///
    /// d_max = 0.5 is the maximum possible ring distance.
    fn random_link_distance(protection_boundary: f64, rng: &mut impl Rng) -> f64 {
        // d_min matches the protection boundary — Kleinberg long-range links
        // start just beyond the nearest-3 protected zone, with no gap.
        let d_min: f64 = protection_boundary.max(1e-8);
        let d_max: f64 = 0.5;
        // Inverse CDF of 1/d distribution on [d_min, d_max]:
        // CDF(d) = ln(d/d_min) / ln(d_max/d_min)
        // Inverse: d = d_min * (d_max/d_min)^u, u ~ Uniform(0,1)
        let u: f64 = rng.random();
        d_min * (d_max / d_min).powf(u)
    }

    fn kleinberg_target(location: f64, protection_boundary: f64, rng: &mut impl Rng) -> f64 {
        let dist = Self::random_link_distance(protection_boundary, rng);
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

        // Protection boundary: distance to the k-th (furthest) protected neighbor.
        // Kleinberg long-range links start beyond this boundary.
        let protection_boundary = ctx
            .nearest_peers
            .last()
            .map(|(_, loc)| ring_distance(ctx.location, *loc))
            .unwrap_or(0.001);

        Self::kleinberg_target(ctx.location, protection_boundary, rng)
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

        // Drop the most redundant connection using LOG-distance gaps.
        //
        // The ideal Kleinberg 1/d distribution is uniform in log-distance space
        // (since P(d) ∝ 1/d means CDF ∝ ln(d)). So connections should be equally
        // spaced in log(d). The most redundant connection is the one with the
        // smallest log-distance gap to its neighbor — it contributes least to
        // the distribution's coverage of distance scales.
        if droppable.len() >= 2 {
            let mut most_redundant_idx = 0;
            let mut min_log_gap = f64::MAX;
            for i in 0..droppable.len() - 1 {
                let log_gap = droppable[i + 1].1.ln() - droppable[i].1.ln();
                if log_gap < min_log_gap {
                    min_log_gap = log_gap;
                    // Either peer in the redundant pair could be dropped; we pick the
                    // further one as an arbitrary but consistent tie-breaker.
                    most_redundant_idx = i + 1;
                }
            }
            Some(droppable[most_redundant_idx].0)
        } else {
            Some(droppable[0].0)
        }
    }
}
