//! Core network simulation: peers on a ring forming and maintaining connections.

use crate::strategy::Strategy;
use rand::Rng;
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
    strategy: Strategy,
    rng: rand::rngs::ThreadRng,
    tick_count: usize,
}

impl Network {
    pub fn new(
        num_peers: usize,
        min_connections: usize,
        max_connections: usize,
        strategy: Strategy,
    ) -> Self {
        let mut rng = rand::rng();
        let mut peers: Vec<Peer> = (0..num_peers)
            .map(|id| Peer::new(id, rng.random::<f64>()))
            .collect();

        // Sort by location for efficient neighbor lookups
        peers.sort_by(|a, b| a.location.partial_cmp(&b.location).unwrap());
        // Reassign IDs to match sorted order
        for (i, p) in peers.iter_mut().enumerate() {
            p.id = i;
        }

        Self {
            peers,
            min_connections,
            max_connections,
            strategy,
            rng,
            tick_count: 0,
        }
    }

    /// One simulation tick: generate requests, seek connections, maintain topology.
    pub fn tick(&mut self) {
        self.tick_count += 1;

        // 1. Generate random request traffic to build density maps
        self.generate_requests();

        // 2. Each peer below min_connections seeks new connections
        self.seek_connections();

        // 3. Maintenance: peers above max drop connections
        self.drop_excess_connections();
    }

    fn generate_requests(&mut self) {
        let n = self.peers.len();
        // Each peer gets a few random request targets per tick
        for i in 0..n {
            let num_requests = 2 + self.rng.random_range(0..3u32);
            for _ in 0..num_requests {
                let target: f64 = self.rng.random();
                self.peers[i].recent_requests.push(target);
            }
            // Keep sliding window bounded
            let max_window = 200;
            let len = self.peers[i].recent_requests.len();
            if len > max_window {
                self.peers[i].recent_requests.drain(0..len - max_window);
            }
        }
    }

    fn seek_connections(&mut self) {
        let n = self.peers.len();
        // Collect connection targets first, then apply (borrow-checker friendly)
        let mut new_connections: Vec<(usize, usize)> = Vec::new();

        for i in 0..n {
            if self.peers[i].connections.len() >= self.min_connections {
                continue;
            }

            // Build context for strategy
            let ctx = self.build_peer_context(i);
            let target_loc = self.strategy.select_target(&ctx, &mut self.rng);

            // Find the best unconnected peer near target_loc
            if let Some(target_peer) = self.find_peer_near(i, target_loc) {
                // Check if target accepts the connection
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

        // Apply connections
        for (a, b) in new_connections {
            if self.peers[a].connections.len() < self.max_connections
                && self.peers[b].connections.len() < self.max_connections
            {
                self.peers[a].connections.insert(b);
                self.peers[b].connections.insert(a);
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

    /// Find the closest unconnected peer to `target_loc`, excluding `self_id`.
    fn find_peer_near(&self, self_id: usize, target_loc: f64) -> Option<usize> {
        self.peers
            .iter()
            .filter(|p| p.id != self_id && !self.peers[self_id].connections.contains(&p.id))
            .min_by(|a, b| {
                ring_distance(a.location, target_loc)
                    .partial_cmp(&ring_distance(b.location, target_loc))
                    .unwrap()
            })
            .map(|p| p.id)
    }

    /// Build a strategy context for a given peer.
    pub fn build_peer_context(&self, peer_id: usize) -> PeerContext {
        let peer = &self.peers[peer_id];
        let neighbor_locs: Vec<f64> = peer
            .connections
            .iter()
            .map(|&id| self.peers[id].location)
            .collect();

        PeerContext {
            peer_id,
            location: peer.location,
            connection_count: peer.connections.len(),
            neighbor_locations: neighbor_locs,
            recent_requests: peer.recent_requests.clone(),
            connection_ids: peer.connections.iter().copied().collect(),
            all_peer_locations: self.peers.iter().map(|p| p.location).collect(),
        }
    }
}

/// Context passed to strategy functions for decision-making.
pub struct PeerContext {
    pub peer_id: usize,
    pub location: f64,
    pub connection_count: usize,
    pub neighbor_locations: Vec<f64>,
    pub recent_requests: Vec<f64>,
    pub connection_ids: Vec<usize>,
    pub all_peer_locations: Vec<f64>,
}
