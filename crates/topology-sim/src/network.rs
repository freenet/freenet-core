//! Core network simulation: peers on a ring forming and maintaining connections.

use crate::strategy::Strategy;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashSet;

/// A peer in the simulated network.
pub struct Peer {
    pub id: usize,
    pub location: f64,
    pub connections: HashSet<usize>,
    /// Sliding window of recent request target locations (for density tracking).
    pub recent_requests: Vec<f64>,
}

impl Peer {
    fn new(id: usize, location: f64) -> Self {
        Self {
            id,
            location,
            connections: HashSet::new(),
            recent_requests: Vec::new(),
        }
    }
}

/// Ring distance on [0, 1).
pub fn ring_distance(a: f64, b: f64) -> f64 {
    let d = (a - b).abs();
    d.min(1.0 - d)
}

pub struct Network {
    pub peers: Vec<Peer>,
    pub min_connections: usize,
    pub max_connections: usize,
    /// Sorted peer locations for efficient nearest-neighbor lookups.
    sorted_locations: Vec<(f64, usize)>,
    strategy: Strategy,
    rng: StdRng,
}

impl Network {
    pub fn new(
        num_peers: usize,
        min_connections: usize,
        max_connections: usize,
        strategy: Strategy,
        seed: u64,
    ) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut peers: Vec<Peer> = (0..num_peers)
            .map(|id| Peer::new(id, rng.random::<f64>()))
            .collect();

        // Sort by location for consistent ordering
        peers.sort_by(|a, b| a.location.partial_cmp(&b.location).unwrap());
        for (i, p) in peers.iter_mut().enumerate() {
            p.id = i;
        }

        let sorted_locations: Vec<(f64, usize)> =
            peers.iter().map(|p| (p.location, p.id)).collect();

        // Bootstrap: each peer joins through a gateway and gets routed to 2 random
        // initial connections.
        for i in 1..num_peers {
            let mut targets: Vec<usize> = (0..i).collect();
            for j in (1..targets.len()).rev() {
                let k = rng.random_range(0..=j);
                targets.swap(j, k);
            }
            for &t in targets.iter().take(2) {
                peers[i].connections.insert(t);
                peers[t].connections.insert(i);
            }
        }

        Self {
            peers,
            min_connections,
            max_connections,
            sorted_locations,
            strategy,
            rng,
        }
    }

    /// One simulation tick: generate requests, seek connections, maintain topology.
    pub fn tick(&mut self) {
        // 1. Generate random request traffic to build density maps
        self.generate_requests();

        // 2. Each peer below min_connections seeks new connections
        self.seek_connections();

        // 3. Peers at/above min_connections try to improve topology via replacement
        self.improve_connections();

        // 4. Maintenance: peers above max drop connections
        self.drop_excess_connections();
    }

    fn generate_requests(&mut self) {
        let n = self.peers.len();
        for i in 0..n {
            let num_requests = 2 + self.rng.random_range(0..3u32);
            for _ in 0..num_requests {
                let target: f64 = self.rng.random();
                self.peers[i].recent_requests.push(target);
            }
            let max_window = 200;
            let len = self.peers[i].recent_requests.len();
            if len > max_window {
                self.peers[i].recent_requests.drain(0..len - max_window);
            }
        }
    }

    fn seek_connections(&mut self) {
        let n = self.peers.len();
        let uses_intro = self.strategy.uses_introduction();
        let mut new_connections: Vec<(usize, usize)> = Vec::new();

        for i in 0..n {
            let current_conns = self.peers[i].connections.len();
            if current_conns >= self.max_connections {
                continue;
            }

            // Below min: seek aggressively (multiple attempts per tick).
            // Above min but below max: seek occasionally (simulation rate knob,
            // not a design parameter — controls convergence speed, not steady state).
            let attempts = if current_conns < self.min_connections {
                ((self.min_connections - current_conns) / 2).clamp(1, 3)
            } else if self.rng.random::<f64>() < 0.05 {
                1
            } else {
                continue;
            };
            for _ in 0..attempts {
                let ctx = self.build_peer_context(i);
                let target_loc = self.strategy.select_target(&ctx, &mut self.rng);

                // Connection discovery mechanism:
                // - Below min_connections: use introduction (models gateway-assisted
                //   bootstrap — a joining peer discovers contacts via the gateway's
                //   global knowledge, not through its own sparse connections).
                // - Above min_connections: route through existing topology, with
                //   introduction as fallback if routing fails to find a candidate.
                //   This is self-regulating: bad topology → routing fails → more
                //   introductions → better topology → routing succeeds → fewer
                //   introductions.
                let target_peer = if uses_intro && current_conns < self.min_connections {
                    self.find_peer_near(i, target_loc)
                } else {
                    self.route_to_peer(i, target_loc).or_else(|| {
                        if uses_intro {
                            self.find_peer_near(i, target_loc)
                        } else {
                            None
                        }
                    })
                };

                if let Some(target_peer) = target_peer {
                    let target_ctx = self.build_peer_context(target_peer);
                    if self.strategy.accept_connection(
                        &target_ctx,
                        self.peers[i].location,
                        self.min_connections,
                        self.max_connections,
                        &mut self.rng,
                    ) {
                        new_connections.push((i, target_peer));
                    }
                }
            }
        }

        for (a, b) in new_connections {
            if self.peers[a].connections.len() < self.max_connections
                && self.peers[b].connections.len() < self.max_connections
            {
                self.peers[a].connections.insert(b);
                self.peers[b].connections.insert(a);
            }
        }
    }

    /// Active topology improvement: peers at/above min_connections periodically
    /// replace their most redundant connection with a Kleinberg-sampled one.
    ///
    /// No improvement threshold is needed: select_drop identifies the connection
    /// contributing least to the 1/d distribution (smallest log-distance gap),
    /// and select_target draws from the ideal 1/d distribution. Replacing
    /// redundancy with a fresh Kleinberg sample improves distribution fit by
    /// construction.
    ///
    /// Improvement uses introduction (gateway/peer exchange) rather than routing
    /// for peer discovery. Rationale: improvement exists specifically to maintain
    /// the 1/d distribution quality. Routing through a partially-converged topology
    /// provides approximate placement; introduction provides precise placement near
    /// the sampled target. For distribution maintenance, precision matters.
    fn improve_connections(&mut self) {
        let n = self.peers.len();
        let uses_intro = self.strategy.uses_introduction();
        let mut replacements: Vec<(usize, usize, usize)> = Vec::new(); // (peer, drop, add)

        for i in 0..n {
            if self.peers[i].connections.len() < self.min_connections {
                continue;
            }

            // Simulation rate knob: controls convergence speed, not steady-state
            // topology. In real Freenet, improvement runs during periodic maintenance.
            if self.rng.random::<f64>() > 0.1 {
                continue;
            }

            let ctx = self.build_peer_context(i);

            if let Some(drop_id) = self.strategy.select_drop(&ctx, &mut self.rng) {
                let target_loc = self.strategy.select_target(&ctx, &mut self.rng);

                // Use introduction for precise 1/d placement. Improvement's purpose
                // is distribution quality, so we use the precise mechanism.
                // For strategies without introduction, fall back to routing.
                let candidate = if uses_intro {
                    self.find_peer_near(i, target_loc)
                } else {
                    self.route_to_peer(i, target_loc)
                };

                if let Some(candidate) = candidate {
                    if candidate != drop_id {
                        let target_ctx = self.build_peer_context(candidate);
                        if self.strategy.accept_connection(
                            &target_ctx,
                            self.peers[i].location,
                            self.min_connections,
                            self.max_connections,
                            &mut self.rng,
                        ) {
                            replacements.push((i, drop_id, candidate));
                        }
                    }
                }
            }
        }

        for (peer, drop, add) in replacements {
            if !self.peers[peer].connections.contains(&add) {
                self.peers[peer].connections.remove(&drop);
                self.peers[drop].connections.remove(&peer);
                self.peers[peer].connections.insert(add);
                self.peers[add].connections.insert(peer);
            }
        }
    }

    fn drop_excess_connections(&mut self) {
        let n = self.peers.len();
        for i in 0..n {
            while self.peers[i].connections.len() > self.max_connections {
                let ctx = self.build_peer_context(i);
                let to_drop = self.strategy.select_drop(&ctx, &mut self.rng);
                if let Some(drop_id) = to_drop {
                    self.peers[i].connections.remove(&drop_id);
                    self.peers[drop_id].connections.remove(&i);
                } else {
                    break;
                }
            }
        }
    }

    /// Simulate the CONNECT routing process: greedy-forward through existing topology
    /// toward `target_loc` with a TTL scaled to network size.
    fn route_to_peer(&self, from_id: usize, target_loc: f64) -> Option<usize> {
        let ttl = ((self.peers.len() as f64).log2() as usize * 2).max(10);
        let mut current = from_id;
        let mut visited: HashSet<usize> = HashSet::new();
        visited.insert(from_id);
        let mut best_candidate = None;
        let mut best_dist = f64::MAX;

        for _ in 0..ttl {
            let next = self.peers[current]
                .connections
                .iter()
                .filter(|&&id| !visited.contains(&id) && id != from_id)
                .min_by(|&&a, &&b| {
                    ring_distance(self.peers[a].location, target_loc)
                        .partial_cmp(&ring_distance(self.peers[b].location, target_loc))
                        .unwrap()
                })
                .copied();

            match next {
                Some(next_id) => {
                    visited.insert(next_id);
                    let d = ring_distance(self.peers[next_id].location, target_loc);
                    if d < best_dist && !self.peers[from_id].connections.contains(&next_id) {
                        best_dist = d;
                        best_candidate = Some(next_id);
                    }
                    current = next_id;
                }
                None => break,
            }
        }

        best_candidate.or_else(|| {
            if current != from_id && !self.peers[from_id].connections.contains(&current) {
                Some(current)
            } else {
                None
            }
        })
    }

    /// Find a peer near `target_loc` using the sorted location index. Models
    /// gateway introduction: the gateway knows about peers at various locations
    /// and can introduce us to one near our target.
    fn find_peer_near(&self, from_id: usize, target_loc: f64) -> Option<usize> {
        // Check enough candidates to guarantee finding a non-connected peer.
        // With degree d, we need at most d+1 candidates (pigeonhole principle).
        let degree = self.peers[from_id].connections.len();
        let k = (degree + 1).min(self.peers.len() - 1);
        let results = self.k_nearest(target_loc, k, from_id);
        results
            .into_iter()
            .find(|(id, _)| !self.peers[from_id].connections.contains(id))
            .map(|(id, _)| id)
    }

    /// Find the k nearest peers to `location` using binary search on sorted locations.
    /// Returns Vec of (peer_id, distance). Much faster than scanning all peers.
    pub fn k_nearest(&self, location: f64, k: usize, exclude: usize) -> Vec<(usize, f64)> {
        let n = self.sorted_locations.len();
        if n <= 1 {
            return Vec::new();
        }

        // Binary search for insertion point
        let pos = self
            .sorted_locations
            .partition_point(|&(loc, _)| loc < location);

        let mut result = Vec::with_capacity(k);
        let mut left = if pos > 0 { pos - 1 } else { n - 1 };
        let mut right = pos % n;
        let mut added = HashSet::new();
        let mut iterations = 0;
        let max_iterations = 2 * n;

        while result.len() < k && iterations < max_iterations {
            iterations += 1;
            let (loc_l, id_l) = self.sorted_locations[left];
            let (loc_r, id_r) = self.sorted_locations[right];
            let dl = ring_distance(location, loc_l);
            let dr = ring_distance(location, loc_r);

            if dl <= dr {
                if id_l != exclude && !added.contains(&id_l) {
                    result.push((id_l, dl));
                    added.insert(id_l);
                }
                left = if left > 0 { left - 1 } else { n - 1 };
            } else {
                if id_r != exclude && !added.contains(&id_r) {
                    result.push((id_r, dr));
                    added.insert(id_r);
                }
                right = (right + 1) % n;
            }

            if added.len() >= n - 1 {
                break;
            }
        }

        result
    }

    pub fn build_peer_context(&self, peer_id: usize) -> PeerContext {
        let peer = &self.peers[peer_id];
        let neighbor_locs: Vec<f64> = peer
            .connections
            .iter()
            .map(|&id| self.peers[id].location)
            .collect();

        let nearest_peers: Vec<(usize, f64)> = self
            .k_nearest(peer.location, 3, peer_id)
            .into_iter()
            .map(|(id, _dist)| (id, self.peers[id].location))
            .collect();

        PeerContext {
            location: peer.location,
            connection_count: peer.connections.len(),
            neighbor_locations: neighbor_locs,
            recent_requests: peer.recent_requests.clone(),
            connection_ids: peer.connections.iter().copied().collect(),
            nearest_peers,
        }
    }
}

/// Context passed to strategy functions for decision-making.
pub struct PeerContext {
    pub location: f64,
    pub connection_count: usize,
    pub neighbor_locations: Vec<f64>,
    pub recent_requests: Vec<f64>,
    pub connection_ids: Vec<usize>,
    /// Nearest peers on the ring: (id, location), pre-computed via O(log n) lookup.
    pub nearest_peers: Vec<(usize, f64)>,
}
