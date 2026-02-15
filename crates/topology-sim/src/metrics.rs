//! Topology quality metrics: distance distribution, routing efficiency, neighbor connectivity.

use crate::network::{ring_distance, Network};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashSet;

pub struct HistogramBin {
    pub lo: f64,
    pub hi: f64,
    pub pct: f64,
}

pub struct Metrics {
    pub total_connections: usize,
    pub avg_degree: f64,
    pub avg_distance: f64,
    pub short_pct: f64,
    pub long_pct: f64,
    pub nearest_3_pct: f64,
    pub greedy_success_pct: f64,
    pub avg_greedy_hops: f64,
    pub histogram: Vec<HistogramBin>,
}

pub fn compute(net: &Network, seed: u64) -> Metrics {
    let n = net.peers.len();

    // Collect all unique connection distances
    let mut distances: Vec<f64> = Vec::new();
    let mut seen: HashSet<(usize, usize)> = HashSet::new();
    let mut total_connections = 0;

    for peer in &net.peers {
        for &conn in &peer.connections {
            let key = if peer.id < conn {
                (peer.id, conn)
            } else {
                (conn, peer.id)
            };
            if seen.insert(key) {
                distances.push(ring_distance(peer.location, net.peers[conn].location));
                total_connections += 1;
            }
        }
    }

    let avg_degree = if n > 0 {
        net.peers.iter().map(|p| p.connections.len()).sum::<usize>() as f64 / n as f64
    } else {
        0.0
    };

    let avg_distance = if distances.is_empty() {
        0.0
    } else {
        distances.iter().sum::<f64>() / distances.len() as f64
    };

    let pct_matching = |predicate: fn(&f64) -> bool| -> f64 {
        if distances.is_empty() {
            0.0
        } else {
            distances.iter().filter(|&d| predicate(d)).count() as f64 / distances.len() as f64
                * 100.0
        }
    };
    let short_pct = pct_matching(|&d| d < 0.1);
    let long_pct = pct_matching(|&d| d > 0.3);

    // Nearest-3 connectivity
    let nearest_3_pct = compute_nearest_3(net);

    // Greedy routing
    let (greedy_success_pct, avg_greedy_hops) = compute_greedy_routing(net, seed);

    // Histogram
    let bins: Vec<(f64, f64)> = (0..10)
        .map(|i| (i as f64 * 0.05, (i + 1) as f64 * 0.05))
        .collect();
    let histogram = bins
        .iter()
        .map(|&(lo, hi)| {
            // Use <= for the last bin to include d=0.5 (max ring distance)
            let count = distances
                .iter()
                .filter(|&&d| d >= lo && if hi >= 0.5 { d <= hi } else { d < hi })
                .count();
            let pct = if distances.is_empty() {
                0.0
            } else {
                count as f64 / distances.len() as f64 * 100.0
            };
            HistogramBin { lo, hi, pct }
        })
        .collect();

    Metrics {
        total_connections,
        avg_degree,
        avg_distance,
        short_pct,
        long_pct,
        nearest_3_pct,
        greedy_success_pct,
        avg_greedy_hops,
        histogram,
    }
}

/// What fraction of peers are connected to their 3 nearest ring neighbors?
fn compute_nearest_3(net: &Network) -> f64 {
    let n = net.peers.len();
    if n < 4 {
        return 0.0;
    }

    let mut connected = 0;
    let mut checked = 0;

    for peer in &net.peers {
        let nearest = net.k_nearest(peer.location, 3, peer.id);
        for (id, _) in &nearest {
            checked += 1;
            if peer.connections.contains(id) {
                connected += 1;
            }
        }
    }

    if checked == 0 {
        0.0
    } else {
        connected as f64 / checked as f64 * 100.0
    }
}

/// Greedy routing: from random source to random target, always forward to the
/// neighbor closest to the target. Measure success rate and hop count.
fn compute_greedy_routing(net: &Network, seed: u64) -> (f64, f64) {
    let n = net.peers.len();
    if n < 10 {
        return (0.0, 0.0);
    }

    let num_trials = (n * 2).min(2000);
    let mut rng = StdRng::seed_from_u64(seed.wrapping_add(1));
    let mut successes = 0;
    let mut total_hops: usize = 0;
    let max_hops = (n as f64).log2() as usize * 5; // generous limit

    for _ in 0..num_trials {
        let src = rng.random_range(0..n);
        let mut dst = rng.random_range(0..n);
        while dst == src {
            dst = rng.random_range(0..n);
        }

        let target_loc = net.peers[dst].location;
        let mut current = src;
        let mut hops = 0;
        let mut visited: HashSet<usize> = HashSet::new();

        loop {
            if current == dst {
                successes += 1;
                total_hops += hops;
                break;
            }

            if hops >= max_hops || visited.contains(&current) {
                break; // Failed
            }

            visited.insert(current);
            hops += 1;

            // Greedily forward to neighbor closest to target
            let next = net.peers[current]
                .connections
                .iter()
                .min_by(|&&a, &&b| {
                    ring_distance(net.peers[a].location, target_loc)
                        .partial_cmp(&ring_distance(net.peers[b].location, target_loc))
                        .unwrap()
                })
                .copied();

            match next {
                Some(next_id)
                    if ring_distance(net.peers[next_id].location, target_loc)
                        < ring_distance(net.peers[current].location, target_loc) =>
                {
                    current = next_id;
                }
                _ => break, // Stuck - no neighbor is closer
            }
        }
    }

    let success_pct = successes as f64 / num_trials as f64 * 100.0;
    let avg_hops = if successes > 0 {
        total_hops as f64 / successes as f64
    } else {
        0.0
    };

    (success_pct, avg_hops)
}
