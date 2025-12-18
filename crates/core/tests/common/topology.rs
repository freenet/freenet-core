//! Topology analysis utilities for validating small-world network properties.
//!
//! A small-world network should exhibit:
//! 1. High clustering coefficient (peers connected to nearby peers)
//! 2. Short average path length
//! 3. Connections primarily to peers with similar ring locations

use anyhow::Result;
use freenet_test_network::TestNetwork;
use std::collections::{HashMap, HashSet};

/// Analysis results for network topology.
#[derive(Debug, Clone)]
pub struct TopologyAnalysis {
    /// Number of peers in the network.
    pub peer_count: usize,
    /// Average number of connections per peer.
    pub avg_connections: f64,
    /// Minimum connections observed.
    pub min_connections: usize,
    /// Maximum connections observed.
    pub max_connections: usize,
    /// Number of peers with zero connections (isolated).
    pub isolated_peers: usize,
    /// Average ring distance between connected peers (0.0-0.5 scale).
    pub avg_ring_distance: f64,
    /// Percentage of connections that are "local" (within 0.2 ring distance).
    pub local_connection_ratio: f64,
    /// Whether the network appears to have small-world properties.
    pub is_small_world: bool,
    /// Number of unique ring locations (should equal peer_count for proper operation).
    pub unique_locations: usize,
    /// Whether peer locations are diverse (not all identical).
    /// If false, indicates a bug in location assignment.
    pub locations_diverse: bool,
    /// Detailed connection info per peer: peer_id -> (ring_location, connected_peer_ids).
    pub peer_details: HashMap<String, PeerTopologyInfo>,
}

/// Topology information for a single peer.
#[derive(Debug, Clone)]
pub struct PeerTopologyInfo {
    pub ring_location: Option<f64>,
    pub connected_peers: Vec<String>,
    pub connection_count: usize,
}

/// Analyze the network topology for small-world properties.
///
/// This queries each peer for its connected peers and ring location,
/// then computes metrics indicating whether the topology exhibits
/// small-world characteristics (lots of local connections).
pub async fn analyze_small_world_topology(network: &TestNetwork) -> Result<TopologyAnalysis> {
    let diagnostics = network.collect_diagnostics().await?;

    let mut peer_details = HashMap::new();
    let mut total_connections = 0usize;
    let mut min_connections = usize::MAX;
    let mut max_connections = 0usize;
    let mut isolated_peers = 0usize;

    // Collect connection info for all peers (gateways + regular peers)
    for peer_diag in &diagnostics.peers {
        let conn_count = peer_diag.connected_peer_ids.len();
        total_connections += conn_count;
        min_connections = min_connections.min(conn_count);
        max_connections = max_connections.max(conn_count);
        if conn_count == 0 {
            isolated_peers += 1;
        }

        // Parse the location string to f64 if available
        let ring_location = peer_diag
            .location
            .as_ref()
            .and_then(|s| s.parse::<f64>().ok());

        peer_details.insert(
            peer_diag.peer_id.clone(),
            PeerTopologyInfo {
                ring_location,
                connected_peers: peer_diag.connected_peer_ids.clone(),
                connection_count: conn_count,
            },
        );
    }

    let peer_count = diagnostics.peers.len();
    let avg_connections = if peer_count > 0 {
        total_connections as f64 / peer_count as f64
    } else {
        0.0
    };

    if min_connections == usize::MAX {
        min_connections = 0;
    }

    // Compute ring distance metrics
    let (avg_ring_distance, local_connection_ratio) = compute_ring_distance_metrics(&peer_details);

    // Count unique locations (truncate to avoid floating point comparison issues)
    let peers_with_location = peer_details
        .values()
        .filter(|info| info.ring_location.is_some())
        .count();
    let unique_locs: HashSet<_> = peer_details
        .values()
        .filter_map(|info| info.ring_location)
        .map(|loc| (loc * 10000.0).round() as i64) // Truncate to 4 decimal places
        .collect();
    let unique_locations = unique_locs.len();

    // Each peer with a unique external IP should have a unique ring location.
    // If all peers have the same location (unique_locations == 1 but peers > 1),
    // that indicates a bug in location assignment from observed addresses.
    let locations_diverse = unique_locations == peers_with_location || peers_with_location <= 1;

    // A network is considered "small-world" if:
    // 1. Most peers have connections (low isolation)
    // 2. Connections are predominantly local (high local_connection_ratio)
    // 3. Average ring distance is small
    // 4. Locations are diverse (not a bug where all peers have same location)
    let isolation_ratio = isolated_peers as f64 / peer_count.max(1) as f64;
    let is_small_world = isolation_ratio < 0.1 // Less than 10% isolated
        && local_connection_ratio > 0.5 // More than 50% local connections
        && avg_ring_distance < 0.3 // Average distance less than 0.3
        && locations_diverse; // Locations must be diverse

    Ok(TopologyAnalysis {
        peer_count,
        avg_connections,
        min_connections,
        max_connections,
        isolated_peers,
        avg_ring_distance,
        local_connection_ratio,
        is_small_world,
        unique_locations,
        locations_diverse,
        peer_details,
    })
}

/// Compute ring distance metrics between connected peers.
fn compute_ring_distance_metrics(peer_details: &HashMap<String, PeerTopologyInfo>) -> (f64, f64) {
    let mut total_distance = 0.0;
    let mut local_connections = 0usize;
    let mut total_measured_connections = 0usize;

    for info in peer_details.values() {
        let Some(my_location) = info.ring_location else {
            continue;
        };

        for connected_id in &info.connected_peers {
            let Some(connected_info) = peer_details.get(connected_id) else {
                continue;
            };
            let Some(their_location) = connected_info.ring_location else {
                continue;
            };

            let distance = ring_distance(my_location, their_location);
            total_distance += distance;
            total_measured_connections += 1;

            // "Local" connection: within 0.2 ring distance
            if distance < 0.2 {
                local_connections += 1;
            }
        }
    }

    let avg_distance = if total_measured_connections > 0 {
        total_distance / total_measured_connections as f64
    } else {
        0.5 // Default to max if no data
    };

    let local_ratio = if total_measured_connections > 0 {
        local_connections as f64 / total_measured_connections as f64
    } else {
        0.0
    };

    (avg_distance, local_ratio)
}

/// Compute the ring distance between two locations (0.0 to 0.5 scale).
/// Ring locations are in [0.0, 1.0), and distance wraps around.
fn ring_distance(a: f64, b: f64) -> f64 {
    let diff = (a - b).abs();
    diff.min(1.0 - diff)
}

/// Print a summary of the topology analysis.
pub fn print_topology_summary(analysis: &TopologyAnalysis) {
    println!("--- Topology Analysis ---");
    println!("Peers: {}", analysis.peer_count);
    println!(
        "Connections: avg={:.2}, min={}, max={}",
        analysis.avg_connections, analysis.min_connections, analysis.max_connections
    );
    println!("Isolated peers: {}", analysis.isolated_peers);
    println!(
        "Ring distance: avg={:.3}, local_ratio={:.1}%",
        analysis.avg_ring_distance,
        analysis.local_connection_ratio * 100.0
    );
    println!(
        "Location diversity: {}/{} unique ({})",
        analysis.unique_locations,
        analysis.peer_count,
        if analysis.locations_diverse {
            "OK"
        } else {
            "BUG: all peers have same location!"
        }
    );
    println!(
        "Small-world topology: {}",
        if analysis.is_small_world { "YES" } else { "NO" }
    );
    println!("--- End Topology Analysis ---");
}

/// Detailed topology dump showing each peer's connections.
pub fn print_topology_details(analysis: &TopologyAnalysis) {
    println!("--- Detailed Topology ---");
    let mut peers: Vec<_> = analysis.peer_details.iter().collect();
    peers.sort_by(|a, b| {
        let loc_a = a.1.ring_location.unwrap_or(0.0);
        let loc_b = b.1.ring_location.unwrap_or(0.0);
        loc_a.partial_cmp(&loc_b).unwrap()
    });

    for (peer_id, info) in peers {
        let loc_str = info
            .ring_location
            .map(|l| format!("{:.4}", l))
            .unwrap_or_else(|| "N/A".to_string());
        println!(
            "{} (loc={}): {} connections",
            peer_id, loc_str, info.connection_count
        );
        for conn in &info.connected_peers {
            if let Some(conn_info) = analysis.peer_details.get(conn) {
                let conn_loc = conn_info
                    .ring_location
                    .map(|l| format!("{:.4}", l))
                    .unwrap_or_else(|| "N/A".to_string());
                let distance = match (info.ring_location, conn_info.ring_location) {
                    (Some(a), Some(b)) => format!("{:.4}", ring_distance(a, b)),
                    _ => "N/A".to_string(),
                };
                println!("  -> {} (loc={}, dist={})", conn, conn_loc, distance);
            } else {
                println!("  -> {} (unknown)", conn);
            }
        }
    }
    println!("--- End Detailed Topology ---");
}
