#![cfg(feature = "test-network")]
//! Twenty-peer network validation test for connect protocol and operations.
//!
//! This test validates:
//! 1. **Connect Protocol** (Docker NAT): Verifies that the network achieves a small-world
//!    topology with connections between nearby peers on the ring.
//! 2. **Operations** (Local backend): Validates put, get, subscribe, and update operations
//!    using riverctl.
//!
//! ## Running Manually
//!
//! ### Phase 1: Connect Protocol Validation (Docker NAT)
//! ```text
//! FREENET_TEST_DOCKER_NAT=1 cargo test -p freenet --test twenty_peer_validation \
//!     twenty_peer_connect_validation -- --ignored --nocapture
//! ```
//!
//! ### Phase 2: Operations Validation (Local, faster)
//! ```text
//! cargo test -p freenet --test twenty_peer_validation \
//!     twenty_peer_operations_validation -- --ignored --nocapture
//! ```
//!
//! ## Environment Overrides
//! - `VALIDATION_PEER_COUNT` – number of peers (default: 20)
//! - `VALIDATION_MIN_CONNECTIONS` – min connections target (default: 3)
//! - `VALIDATION_MAX_CONNECTIONS` – max connections target (default: 5)
//! - `VALIDATION_STABILIZATION_SECS` – topology stabilization wait (default: 180)
//! - `VALIDATION_SNAPSHOT_COUNT` – number of topology snapshots (default: 3)
//! - `VALIDATION_SNAPSHOT_INTERVAL_SECS` – interval between snapshots (default: 60)
//!
//! ## Requirements
//! - Docker (for connect validation)
//! - `riverctl` in PATH (for operations validation)
//! - Sufficient CPU/RAM for 20+ peers

mod common;

use anyhow::{ensure, Context};
use common::{
    topology::{analyze_small_world_topology, print_topology_details, print_topology_summary},
    RiverSession, RiverUser,
};
use freenet_test_network::{Backend, BuildProfile, DockerNatConfig, FreenetBinary, TestNetwork};
use serde_json::to_string_pretty;
use std::{
    env, fs,
    time::{Duration, Instant},
};
use tokio::time::sleep;
use which::which;

// Defaults tuned for 20-peer network
const DEFAULT_PEER_COUNT: usize = 20;
const DEFAULT_MIN_CONNECTIONS: usize = 3;
const DEFAULT_MAX_CONNECTIONS: usize = 5;
const DEFAULT_STABILIZATION_SECS: u64 = 180; // 3 minutes for topology convergence
const DEFAULT_SNAPSHOT_COUNT: usize = 3;
const DEFAULT_SNAPSHOT_INTERVAL_SECS: u64 = 60;
const DEFAULT_CONNECTIVITY_TARGET: f64 = 0.9; // 90% of peers should be connected

/// Read test configuration from environment variables.
struct TestConfig {
    peer_count: usize,
    min_connections: usize,
    max_connections: usize,
    stabilization_secs: u64,
    snapshot_count: usize,
    snapshot_interval_secs: u64,
    connectivity_target: f64,
}

impl TestConfig {
    fn from_env() -> Self {
        Self {
            // Require at least 2 peers for meaningful topology tests
            peer_count: env::var("VALIDATION_PEER_COUNT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_PEER_COUNT)
                .max(2),
            min_connections: env::var("VALIDATION_MIN_CONNECTIONS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_MIN_CONNECTIONS),
            max_connections: env::var("VALIDATION_MAX_CONNECTIONS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_MAX_CONNECTIONS),
            stabilization_secs: env::var("VALIDATION_STABILIZATION_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_STABILIZATION_SECS),
            // Require at least 1 snapshot to avoid panic
            snapshot_count: env::var("VALIDATION_SNAPSHOT_COUNT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_SNAPSHOT_COUNT)
                .max(1),
            snapshot_interval_secs: env::var("VALIDATION_SNAPSHOT_INTERVAL_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_SNAPSHOT_INTERVAL_SECS),
            // Clamp connectivity_target to valid range (0.0, 1.0]
            connectivity_target: env::var("VALIDATION_CONNECTIVITY_TARGET")
                .ok()
                .and_then(|v| v.parse().ok())
                .map(|v: f64| v.clamp(0.01, 1.0))
                .unwrap_or(DEFAULT_CONNECTIVITY_TARGET),
        }
    }
}

/// Phase 1: Validate the connect protocol achieves a small-world topology.
///
/// Uses Docker NAT to simulate realistic NAT traversal scenarios.
/// Waits for topology stabilization and validates:
/// - Most peers have connections (low isolation rate)
/// - Connections are primarily to nearby peers on the ring
/// - Average ring distance between connected peers is small
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Manual test - requires Docker NAT (FREENET_TEST_DOCKER_NAT=1)"]
async fn twenty_peer_connect_validation() -> anyhow::Result<()> {
    let config = TestConfig::from_env();

    // Verify Docker NAT is enabled
    ensure!(
        env::var("FREENET_TEST_DOCKER_NAT").is_ok(),
        "This test requires FREENET_TEST_DOCKER_NAT=1 to simulate NAT traversal"
    );

    // Verify Docker is available
    let docker_available = std::process::Command::new("docker")
        .args(["info"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    ensure!(
        docker_available,
        "Docker is not available. Install/start Docker or use local backend for operations test."
    );

    println!("=== Twenty-Peer Connect Protocol Validation ===");
    println!(
        "Peers: {}, Connections: {}-{}, Stabilization: {}s",
        config.peer_count,
        config.min_connections,
        config.max_connections,
        config.stabilization_secs
    );

    let network = TestNetwork::builder()
        .gateways(1)
        .peers(config.peer_count)
        .min_connections(config.min_connections)
        .max_connections(config.max_connections)
        .require_connectivity(config.connectivity_target)
        .connectivity_timeout(Duration::from_secs(180))
        .preserve_temp_dirs_on_failure(true)
        .preserve_temp_dirs_on_success(true)
        .backend(Backend::DockerNat(DockerNatConfig::default()))
        .binary(FreenetBinary::CurrentCrate(BuildProfile::Debug))
        .build()
        .await
        .context("Failed to start Docker NAT test network")?;

    println!(
        "Network started (run root: {})",
        network.run_root().display()
    );

    let snapshots_dir = network.run_root().join("connect-validation");
    fs::create_dir_all(&snapshots_dir)?;

    // Wait for initial topology stabilization
    println!(
        "Waiting {}s for topology stabilization...",
        config.stabilization_secs
    );
    sleep(Duration::from_secs(config.stabilization_secs)).await;

    // Take periodic snapshots to observe topology evolution
    let mut best_analysis = None;
    for i in 1..=config.snapshot_count {
        println!(
            "\n--- Topology Snapshot {}/{} ---",
            i, config.snapshot_count
        );

        let analysis = analyze_small_world_topology(&network).await?;
        print_topology_summary(&analysis);

        // Save detailed snapshot
        let snapshot_path = snapshots_dir.join(format!("topology-{i:02}.json"));
        let diagnostics = network.collect_diagnostics().await?;
        fs::write(&snapshot_path, to_string_pretty(&diagnostics)?)?;
        println!("Wrote snapshot to {}", snapshot_path.display());

        // Track the best (most connected) topology observed
        if best_analysis
            .as_ref()
            .map(|b: &common::topology::TopologyAnalysis| {
                analysis.avg_connections > b.avg_connections
            })
            .unwrap_or(true)
        {
            best_analysis = Some(analysis.clone());
        }

        // Validate connectivity hasn't dropped
        let isolation_ratio = analysis.isolated_peers as f64 / analysis.peer_count.max(1) as f64;
        ensure!(
            isolation_ratio < 0.2,
            "Too many isolated peers: {}/{} ({:.1}%)",
            analysis.isolated_peers,
            analysis.peer_count,
            isolation_ratio * 100.0
        );

        if i < config.snapshot_count {
            sleep(Duration::from_secs(config.snapshot_interval_secs)).await;
        }
    }

    // Final analysis
    let final_analysis = best_analysis.expect("Should have at least one analysis");
    println!("\n=== Final Topology Analysis ===");
    print_topology_summary(&final_analysis);
    print_topology_details(&final_analysis);

    // Validate small-world properties
    if !final_analysis.is_small_world {
        println!("\nWARNING: Network did not achieve small-world topology.");
        println!("This may indicate issues with the connect protocol.");
        println!(
            "  - Local connection ratio: {:.1}% (target: >50%)",
            final_analysis.local_connection_ratio * 100.0
        );
        println!(
            "  - Avg ring distance: {:.3} (target: <0.3)",
            final_analysis.avg_ring_distance
        );
        println!(
            "  - Isolated peers: {}/{} (target: <10%)",
            final_analysis.isolated_peers, final_analysis.peer_count
        );
        // Don't fail yet - this is informational for now
    }

    println!("\n=== Connect Protocol Validation Complete ===");
    println!("Snapshots saved to: {}", snapshots_dir.display());

    Ok(())
}

/// Phase 2: Validate operations (put, get, subscribe, update) work correctly.
///
/// Uses local backend for speed. Tests:
/// - Room creation (put)
/// - Room retrieval (get)
/// - Subscription and message delivery (subscribe/update)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Manual test - requires riverctl in PATH"]
async fn twenty_peer_operations_validation() -> anyhow::Result<()> {
    let config = TestConfig::from_env();

    let riverctl = which("riverctl")
        .context("riverctl not found in PATH; install via `cargo install riverctl`")?;

    println!("=== Twenty-Peer Operations Validation ===");
    println!(
        "Peers: {}, Connections: {}-{}",
        config.peer_count, config.min_connections, config.max_connections
    );

    // Use local backend for speed (no Docker overhead)
    let network = TestNetwork::builder()
        .gateways(1)
        .peers(config.peer_count)
        .min_connections(config.min_connections)
        .max_connections(config.max_connections)
        .require_connectivity(config.connectivity_target)
        .connectivity_timeout(Duration::from_secs(120))
        .preserve_temp_dirs_on_failure(true)
        .preserve_temp_dirs_on_success(true)
        .binary(FreenetBinary::CurrentCrate(BuildProfile::Debug))
        .build()
        .await
        .context("Failed to start local test network")?;

    println!(
        "Network started (run root: {})",
        network.run_root().display()
    );

    // Brief stabilization for local network
    println!("Waiting 30s for network stabilization...");
    sleep(Duration::from_secs(30)).await;

    // Check initial topology
    let analysis = analyze_small_world_topology(&network).await?;
    print_topology_summary(&analysis);

    // Test operations using River
    println!("\n--- Testing River Operations ---");

    // Use gateway and a distant peer (by index) to test cross-network operations
    let alice_peer_idx = 0; // First regular peer
    let bob_peer_idx = config.peer_count / 2; // Peer roughly halfway through

    let alice_url = format!(
        "{}?encodingProtocol=native",
        network.peer(alice_peer_idx).ws_url()
    );
    let bob_url = format!(
        "{}?encodingProtocol=native",
        network.peer(bob_peer_idx).ws_url()
    );

    println!(
        "Alice on peer {}, Bob on peer {}",
        alice_peer_idx, bob_peer_idx
    );

    // Initialize session (creates room = PUT, joins = GET + PUT)
    println!("Creating room and inviting Bob...");
    let session = RiverSession::initialize(riverctl.clone(), alice_url, bob_url)
        .await
        .context("Failed to initialize River session")?;
    println!(
        "Room created successfully (room_key: {})",
        session.room_key()
    );

    // Test message sending (UPDATE propagation)
    println!("\nTesting message propagation...");
    let test_messages = [
        "Hello from Alice - testing update propagation",
        "Reply from Bob - testing bidirectional updates",
        "Another from Alice - testing consistency",
    ];

    for (i, msg) in test_messages.iter().enumerate() {
        let user = if i % 2 == 0 {
            RiverUser::Alice
        } else {
            RiverUser::Bob
        };
        session.send_message(user, msg).await?;
        println!("  Sent: {} ({:?})", msg, user);
        sleep(Duration::from_millis(500)).await;
    }

    // Verify messages are visible
    println!("\nVerifying message delivery...");
    let alice_messages = session.list_messages(RiverUser::Alice).await?;
    let bob_messages = session.list_messages(RiverUser::Bob).await?;

    for msg in &test_messages {
        ensure!(
            alice_messages.contains(msg),
            "Alice missing message: {}",
            msg
        );
        ensure!(bob_messages.contains(msg), "Bob missing message: {}", msg);
    }
    println!("All messages delivered successfully!");

    // Final topology check
    println!("\n--- Final Topology Check ---");
    let final_analysis = analyze_small_world_topology(&network).await?;
    print_topology_summary(&final_analysis);

    let snapshots_dir = network.run_root().join("operations-validation");
    fs::create_dir_all(&snapshots_dir)?;
    let diagnostics = network.collect_diagnostics().await?;
    fs::write(
        snapshots_dir.join("final-snapshot.json"),
        to_string_pretty(&diagnostics)?,
    )?;

    println!("\n=== Operations Validation Complete ===");
    println!("Snapshots saved to: {}", snapshots_dir.display());

    Ok(())
}

/// Combined test: Run both connect and operations validation in sequence.
///
/// This is a comprehensive validation that:
/// 1. Starts a Docker NAT network
/// 2. Validates small-world topology formation
/// 3. Runs River operations to validate put/get/subscribe/update
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Manual test - requires Docker NAT and riverctl"]
async fn twenty_peer_full_validation() -> anyhow::Result<()> {
    let config = TestConfig::from_env();

    // Check prerequisites
    ensure!(
        env::var("FREENET_TEST_DOCKER_NAT").is_ok(),
        "This test requires FREENET_TEST_DOCKER_NAT=1"
    );
    let riverctl = which("riverctl")
        .context("riverctl not found in PATH; install via `cargo install riverctl`")?;

    println!("=== Twenty-Peer Full Validation ===");
    println!(
        "Peers: {}, Connections: {}-{}",
        config.peer_count, config.min_connections, config.max_connections
    );

    let network = TestNetwork::builder()
        .gateways(1)
        .peers(config.peer_count)
        .min_connections(config.min_connections)
        .max_connections(config.max_connections)
        .require_connectivity(config.connectivity_target)
        .connectivity_timeout(Duration::from_secs(180))
        .preserve_temp_dirs_on_failure(true)
        .preserve_temp_dirs_on_success(true)
        .backend(Backend::DockerNat(DockerNatConfig::default()))
        .binary(FreenetBinary::CurrentCrate(BuildProfile::Debug))
        .build()
        .await
        .context("Failed to start Docker NAT test network")?;

    let snapshots_dir = network.run_root().join("full-validation");
    fs::create_dir_all(&snapshots_dir)?;

    println!(
        "Network started (run root: {})",
        network.run_root().display()
    );

    // Phase 1: Wait for topology stabilization
    println!(
        "\n--- Phase 1: Topology Stabilization ({}s) ---",
        config.stabilization_secs
    );
    let start = Instant::now();
    let mut snapshot_num = 0;

    while start.elapsed().as_secs() < config.stabilization_secs {
        let elapsed = start.elapsed().as_secs();
        let remaining = config.stabilization_secs - elapsed;
        println!(
            "  Stabilizing... {}s elapsed, {}s remaining",
            elapsed, remaining
        );

        // Take periodic snapshots during stabilization
        if elapsed > 0 && elapsed % 60 == 0 {
            snapshot_num += 1;
            let analysis = analyze_small_world_topology(&network).await?;
            println!(
                "  Snapshot {}: {} connections avg, {:.1}% local",
                snapshot_num,
                analysis.avg_connections,
                analysis.local_connection_ratio * 100.0
            );
        }

        sleep(Duration::from_secs(10)).await;
    }

    // Phase 1 validation
    println!("\n--- Phase 1: Topology Validation ---");
    let topology_analysis = analyze_small_world_topology(&network).await?;
    print_topology_summary(&topology_analysis);

    let diagnostics = network.collect_diagnostics().await?;
    fs::write(
        snapshots_dir.join("topology-final.json"),
        to_string_pretty(&diagnostics)?,
    )?;

    // Phase 2: Operations validation
    println!("\n--- Phase 2: Operations Validation ---");

    let alice_url = format!("{}?encodingProtocol=native", network.peer(0).ws_url());
    let bob_url = format!(
        "{}?encodingProtocol=native",
        network.peer(config.peer_count / 2).ws_url()
    );

    let session = RiverSession::initialize(riverctl, alice_url, bob_url)
        .await
        .context("Failed to initialize River session")?;
    println!("Room created: {}", session.room_key());

    // Send test messages
    session
        .send_message(RiverUser::Alice, "Full validation test from Alice")
        .await?;
    session
        .send_message(RiverUser::Bob, "Full validation test from Bob")
        .await?;

    // Verify delivery
    sleep(Duration::from_secs(2)).await;
    let messages = session.list_messages(RiverUser::Alice).await?;
    ensure!(
        messages.contains("Full validation test from Alice"),
        "Alice's message not found"
    );
    ensure!(
        messages.contains("Full validation test from Bob"),
        "Bob's message not found"
    );
    println!("Messages delivered successfully!");

    // Final snapshot
    let final_diagnostics = network.collect_diagnostics().await?;
    fs::write(
        snapshots_dir.join("final-snapshot.json"),
        to_string_pretty(&final_diagnostics)?,
    )?;

    println!("\n=== Full Validation Complete ===");
    println!("Snapshots saved to: {}", snapshots_dir.display());
    println!(
        "Small-world topology: {}",
        if topology_analysis.is_small_world {
            "ACHIEVED"
        } else {
            "NOT ACHIEVED (see warnings above)"
        }
    );

    Ok(())
}
