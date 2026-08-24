//! One-off measurement harness for freenet-core#5147 ("covered-set" broadcast
//! suppression). This is NOT a regression test — it does not assert anything.
//! It builds real Freenet ring topologies via the existing simulation
//! framework (organic CONNECT, no synthetic graph model) and dumps the
//! resulting connectivity graph (which peer is connected to which) to JSON
//! so a follow-up analysis script can compute the achievable suppression
//! rate of a covered-peer-set broadcast fix without implementing the wire
//! change itself.
//!
//! Run with:
//!   FREENET_5147_OUT_DIR=/tmp/freenet-5147-graphs cargo test -p freenet \
//!     --features "simulation_tests,testing" --test broadcast_5147_topology_dump \
//!     -- --test-threads=1 --nocapture --ignored
//!
//! `--ignored` because this is a data-collection run, not part of the
//! regular suite, and is slow (many large topologies). Defaults to writing
//! under /tmp/freenet-5147-graphs if FREENET_5147_OUT_DIR is unset.

#![cfg(feature = "simulation_tests")]

use freenet::config::{GlobalRng, GlobalSimulationTime, GlobalTestMetrics, SimulationTransportOpt};
use freenet::dev_tool::{NodeLabel, SimNetwork};
use std::collections::HashMap;
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

fn setup_deterministic_state(seed: u64) {
    GlobalRng::set_seed(seed);
    const BASE_EPOCH_MS: u64 = 1577836800000;
    const RANGE_MS: u64 = 5 * 365 * 24 * 60 * 60 * 1000;
    GlobalSimulationTime::set_time_ms(BASE_EPOCH_MS + (seed % RANGE_MS));
    GlobalTestMetrics::reset();
    SimulationTransportOpt::disable();
    freenet::dev_tool::clear_crdt_contracts();
}

fn create_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn out_dir() -> PathBuf {
    let dir = std::env::var("FREENET_5147_OUT_DIR")
        .unwrap_or_else(|_| "/tmp/freenet-5147-graphs".to_string());
    std::fs::create_dir_all(&dir).expect("create out dir");
    PathBuf::from(dir)
}

/// One topology configuration to build and dump.
struct TopoConfig {
    /// File-safe short name, embeds all params for traceability.
    name: &'static str,
    total_nodes: usize,
    max_connections: usize,
    min_connections: usize,
    seed: u64,
}

/// Build a network of `total_nodes` peers (1 gateway + rest), let CONNECT
/// topology converge with NO contract operations at all (pure ring
/// formation), then dump the resulting connectivity graph as JSON:
/// `{"nodes": ["gw:0", "n:1", ...], "edges": [["gw:0","n:1"], ...]}`.
fn build_and_dump(cfg: &TopoConfig) {
    setup_deterministic_state(cfg.seed);
    let rt = create_runtime();

    let network_name: String = format!("i5147-{}", cfg.name);
    // A SINGLE gateway is a documented harness bottleneck, not a real-network
    // limit: run_fanout_liveness_scenario's doc comment (simulation_integration.rs)
    // records that a 1-gateway star empirically plateaus around N~16-24 within
    // a feasible sim wall-clock budget regardless of node count or duration,
    // because every CONNECT funnels through one admission-limited peer. A
    // first attempt at this harness hit exactly that: n80/n160 sparse configs
    // both converged to exactly 16 connected nodes. Scale gateway count with
    // network size (~1 per 15 nodes) so bootstrap has multiple entry points,
    // matching how real Freenet networks are actually structured (multiple
    // gateways), not a synthetic workaround.
    let gateways = (cfg.total_nodes / 15).max(1);
    let nodes = cfg.total_nodes.saturating_sub(gateways);

    let sim = rt.block_on(async {
        SimNetwork::new(
            &network_name,
            gateways,
            nodes,
            8,  // ring_max_htl: generous so CONNECT can route across a larger ring
            3,  // rnd_if_htl_above
            cfg.max_connections,
            cfg.min_connections,
            cfg.seed,
        )
        .await
    });

    // Capture the label -> addr map BEFORE run_controlled_simulation consumes `sim`.
    let addr_to_label: HashMap<SocketAddr, NodeLabel> = sim
        .all_node_addresses()
        .iter()
        .map(|(label, addr)| (*addr, label.clone()))
        .collect();

    // Give CONNECT plenty of virtual time to converge for larger networks.
    // No SimOperation at all: startup bootstrap alone drives peers to
    // min_connections; we only need the resulting ring, not any contract op.
    // (Bumped from an initial 60+3n/20s-wait budget after a 24-node smoke run
    // left 8/24 nodes with zero captured connections — under-converged.)
    let sim_duration = Duration::from_secs(120 + (cfg.total_nodes as u64) * 5);
    let result = sim.run_controlled_simulation(
        cfg.seed,
        Vec::new(), // no scheduled operations — pure topology formation
        sim_duration,
        Duration::from_secs(30),
    );

    assert!(
        result.turmoil_result.is_ok(),
        "{}: simulation did not complete: {:?}",
        cfg.name,
        result.turmoil_result.err()
    );

    let mut edges: Vec<(String, String)> = Vec::new();
    let mut node_names: Vec<String> = Vec::new();
    let mut connected_count = 0usize;
    for label in result.captured_node_labels() {
        node_names.push(label.to_string());
        let open = result.node_open_connections(&label);
        if open > 0 {
            connected_count += 1;
        }
        // node_rings backed accessor: real Ring connections at end of run.
        for addr in dump_neighbor_addrs(&result, &label) {
            if let Some(peer_label) = addr_to_label.get(&addr) {
                // Undirected edge; store both endpoints, dedup below.
                let a = label.to_string();
                let b = peer_label.to_string();
                if a < b {
                    edges.push((a, b));
                } else {
                    edges.push((b, a));
                }
            }
        }
    }
    edges.sort();
    edges.dedup();
    node_names.sort();

    eprintln!(
        "[5147] {}: total_nodes={} connected={} edges={} max_conn={} min_conn={}",
        cfg.name,
        node_names.len(),
        connected_count,
        edges.len(),
        cfg.max_connections,
        cfg.min_connections,
    );

    let json = serde_json::json!({
        "name": cfg.name,
        "seed": cfg.seed,
        "total_nodes": cfg.total_nodes,
        "max_connections": cfg.max_connections,
        "min_connections": cfg.min_connections,
        "connected_nodes": connected_count,
        "nodes": node_names,
        "edges": edges,
    });

    let path = out_dir().join(format!("{}.json", cfg.name));
    let mut f = std::fs::File::create(&path).expect("create json file");
    f.write_all(serde_json::to_string_pretty(&json).unwrap().as_bytes())
        .expect("write json");
    eprintln!("[5147] wrote {}", path.display());
}

// Thin wrapper so the accessor name change is a single edit point if the
// underlying testing_impl API evolves.
fn dump_neighbor_addrs(
    result: &freenet::dev_tool::ControlledSimulationResult,
    label: &NodeLabel,
) -> Vec<SocketAddr> {
    result.node_neighbor_addrs(label)
}

/// Small smoke config, cheap to run in normal CI-adjacent verification of
/// this harness (NOT the real data collection — see the `#[ignore]`d
/// `dump_all_5147_topologies` for that).
#[test]
fn dump_smoke_topology() {
    let cfg = TopoConfig {
        name: "smoke-n24-c10-5",
        total_nodes: 24,
        max_connections: 10,
        min_connections: 5,
        seed: 0x5147_0000_0001,
    };
    build_and_dump(&cfg);
}

/// Validates the multi-gateway fix (see `build_and_dump`'s `gateways` comment)
/// actually gets past the single-gateway ~16-node plateau before committing
/// to the full (slow) matrix.
#[test]
#[ignore]
fn dump_smoke_topology_n80_multigateway() {
    let cfg = TopoConfig {
        name: "smoke-n80-c10-5-multigw",
        total_nodes: 80,
        max_connections: 10,
        min_connections: 5,
        seed: 0x5147_0000_0002,
    };
    build_and_dump(&cfg);
}

/// Priority pair for the degree-curve extension into the fleet-realistic
/// range (median 16-17, mean 24.7, p90 58 broadcast targets). Run standalone
/// (not via the full 7-config matrix) so these land first.
#[test]
#[ignore]
fn dump_high_degree_n80_n160() {
    let configs = [
        TopoConfig { name: "n80-c60-30-s1", total_nodes: 80, max_connections: 60, min_connections: 30, seed: 0x5147_4001 },
        TopoConfig { name: "n160-c60-30-s1", total_nodes: 160, max_connections: 60, min_connections: 30, seed: 0x5147_4002 },
    ];
    for cfg in &configs {
        build_and_dump(cfg);
    }
}

/// The real data-collection matrix for the #5147 suppression measurement.
/// Sizes/degrees chosen to span the fleet fan-out distribution (median 17,
/// mean 24.7, p90 58) while keeping the co-host SUBSET a small fraction of
/// the total network (so a co-host's connections include non-co-host peers
/// too, matching get_broadcast_targets_update's per-node/direct-neighbour
/// scoping — see update.rs:359 rustdoc).
#[test]
#[ignore]
fn dump_all_5147_topologies() {
    let configs = [
        // -- sparse (min=5,max=10): thin co-host meshes --
        TopoConfig { name: "n80-c10-5-s1", total_nodes: 80, max_connections: 10, min_connections: 5, seed: 0x5147_1001 },
        TopoConfig { name: "n160-c10-5-s1", total_nodes: 160, max_connections: 10, min_connections: 5, seed: 0x5147_1003 },
        // -- medium (min=10,max=20) --
        TopoConfig { name: "n80-c20-10-s1", total_nodes: 80, max_connections: 20, min_connections: 10, seed: 0x5147_2001 },
        TopoConfig { name: "n160-c20-10-s1", total_nodes: 160, max_connections: 20, min_connections: 10, seed: 0x5147_2003 },
        // -- dense (min=20,max=40): matches near production ceiling for sim scale --
        TopoConfig { name: "n160-c40-20-s1", total_nodes: 160, max_connections: 40, min_connections: 20, seed: 0x5147_3001 },
        // -- higher (min=30,max=60): reaching toward fleet p90 (58) in the
        // INDUCED co-host-subgraph degree, esp. combined with clustered
        // co-host subset selection in the analysis step --
        TopoConfig { name: "n80-c60-30-s1", total_nodes: 80, max_connections: 60, min_connections: 30, seed: 0x5147_4001 },
        TopoConfig { name: "n160-c60-30-s1", total_nodes: 160, max_connections: 60, min_connections: 30, seed: 0x5147_4002 },
    ];
    for cfg in &configs {
        build_and_dump(cfg);
    }
}
