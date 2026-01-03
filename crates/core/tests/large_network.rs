#![cfg(feature = "test-network")]
//! Large-scale soak test using `freenet-test-network`.
//!
//! This test intentionally spins up a sizable network (2 gateways + N peers) and exercises the
//! cluster for several minutes while capturing diagnostics snapshots and running River client
//! workflows via `riverctl`.
//!
//! ## Running Manually
//! ```text
//! cargo test -p freenet --test large_network -- --ignored
//! ```
//! Environment overrides:
//! - `SOAK_PEER_COUNT` – number of non-gateway peers (default: 38).
//! - `SOAK_SNAPSHOT_INTERVAL_SECS` – seconds between diagnostics snapshots (default: 60).
//! - `SOAK_SNAPSHOT_ITERATIONS` – number of snapshots to capture (default: 5).
//! - `SOAK_CONNECTIVITY_TARGET` – minimum ratio of peers that must report >=1 connection (default: 0.75).
//!
//! Requirements:
//! - `riverctl` must be installed and in PATH (`cargo install riverctl`).
//! - Enough CPU/RAM to host ~40 peers locally.
//!
//! The snapshots are stored under the network's `run_root()/large-soak/` directory for later
//! inspection or visualization.

mod common;

use anyhow::{ensure, Context};
use common::{RiverSession, RiverUser};
use freenet_test_network::{BuildProfile, FreenetBinary, TestNetwork};
use serde_json::to_string_pretty;
use std::{
    env, fs,
    time::{Duration, Instant},
};
use tokio::time::sleep;
use tracing::info;
use which::which;

const DEFAULT_PEER_COUNT: usize = 38;
const DEFAULT_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(60);
const DEFAULT_SNAPSHOT_ITERATIONS: usize = 5;
const DEFAULT_SNAPSHOT_WARMUP: Duration = Duration::from_secs(60);
const DEFAULT_CONNECTIVITY_TARGET: f64 = 0.75;
const DEFAULT_MIN_CONNECTIONS: usize = 5;
const DEFAULT_MAX_CONNECTIONS: usize = 7;

#[test_log::test(tokio::test(flavor = "current_thread", start_paused = true))]
#[ignore = "Large soak test - run manually (see file header for instructions)"]
async fn large_network_soak() -> anyhow::Result<()> {
    let peer_count = env::var("SOAK_PEER_COUNT")
        .ok()
        .and_then(|val| val.parse().ok())
        .unwrap_or(DEFAULT_PEER_COUNT);
    let snapshot_interval = env::var("SOAK_SNAPSHOT_INTERVAL_SECS")
        .ok()
        .and_then(|val| val.parse().ok())
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_SNAPSHOT_INTERVAL);
    let snapshot_iterations = env::var("SOAK_SNAPSHOT_ITERATIONS")
        .ok()
        .and_then(|val| val.parse().ok())
        .unwrap_or(DEFAULT_SNAPSHOT_ITERATIONS);
    let connectivity_target = env::var("SOAK_CONNECTIVITY_TARGET")
        .ok()
        .and_then(|val| val.parse::<f64>().ok())
        .unwrap_or(DEFAULT_CONNECTIVITY_TARGET);
    let snapshot_warmup = env::var("SOAK_SNAPSHOT_WARMUP_SECS")
        .ok()
        .and_then(|val| val.parse().ok())
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_SNAPSHOT_WARMUP);
    let min_connections = env::var("SOAK_MIN_CONNECTIONS")
        .ok()
        .and_then(|val| val.parse().ok())
        .unwrap_or(DEFAULT_MIN_CONNECTIONS);
    let max_connections = env::var("SOAK_MAX_CONNECTIONS")
        .ok()
        .and_then(|val| val.parse().ok())
        .unwrap_or(DEFAULT_MAX_CONNECTIONS);

    let network = TestNetwork::builder()
        .gateways(2)
        .peers(peer_count)
        .min_connections(min_connections)
        .max_connections(max_connections)
        .require_connectivity(connectivity_target)
        .connectivity_timeout(Duration::from_secs(120))
        .preserve_temp_dirs_on_failure(true)
        .preserve_temp_dirs_on_success(true)
        .binary(FreenetBinary::CurrentCrate(BuildProfile::Debug))
        .build()
        .await
        .context("failed to start soak test network")?;

    info!(
        "Started soak network with {} gateways and {} peers (run root: {})",
        2,
        peer_count,
        network.run_root().display()
    );
    info!(
        "Min connections: {}, max connections: {} (override via SOAK_MIN_CONNECTIONS / SOAK_MAX_CONNECTIONS)",
        min_connections, max_connections
    );

    let riverctl_path = which("riverctl")
        .context("riverctl not found in PATH; install via `cargo install riverctl`")?;

    let alice_url = format!("{}?encodingProtocol=native", network.gateway(0).ws_url());
    let bob_url = format!("{}?encodingProtocol=native", network.peer(0).ws_url());
    let session = RiverSession::initialize(riverctl_path, alice_url, bob_url).await?;

    let snapshots_dir = network.run_root().join("large-soak");
    fs::create_dir_all(&snapshots_dir)?;

    // Allow topology maintenance to run before the first snapshot.
    info!(
        "Waiting {:?} before first snapshot to allow topology maintenance to converge",
        snapshot_warmup
    );
    sleep(snapshot_warmup).await;

    let mut iteration = 0usize;
    let mut next_tick = Instant::now();
    while iteration < snapshot_iterations {
        iteration += 1;
        let snapshot = network.collect_diagnostics().await?;
        let snapshot_path = snapshots_dir.join(format!("snapshot-{iteration:02}.json"));
        fs::write(&snapshot_path, to_string_pretty(&snapshot)?)?;

        // Also capture ring topology for visualizing evolution over time.
        let ring_snapshot = network.ring_snapshot().await?;
        let ring_path = snapshots_dir.join(format!("ring-{iteration:02}.json"));
        fs::write(&ring_path, to_string_pretty(&ring_snapshot)?)?;

        let healthy = snapshot
            .peers
            .iter()
            .filter(|peer| peer.error.is_none() && !peer.connected_peer_ids.is_empty())
            .count();
        let ratio = healthy as f64 / snapshot.peers.len().max(1) as f64;
        info!(
            "Snapshot {iteration}/{snapshot_iterations}: {:.1}% peers healthy ({} / {}), wrote {}",
            ratio * 100.0,
            healthy,
            snapshot.peers.len(),
            snapshot_path.display()
        );
        ensure!(
            ratio >= connectivity_target,
            "Connectivity dropped below {:.0}% (actual: {:.1}%). Inspect {}",
            connectivity_target * 100.0,
            ratio * 100.0,
            snapshot_path.display()
        );

        // Exercise River application flows to ensure contracts stay responsive.
        session
            .send_message(
                RiverUser::Alice,
                &format!("Large soak heartbeat {} from Alice", iteration),
            )
            .await?;
        session
            .send_message(
                RiverUser::Bob,
                &format!("Large soak heartbeat {} from Bob", iteration),
            )
            .await?;
        session.list_messages(RiverUser::Alice).await?;

        next_tick += snapshot_interval;
        let now = Instant::now();
        if next_tick > now {
            sleep(next_tick - now).await;
        }
    }

    info!(
        "Large network soak complete; inspect {} for diagnostics snapshots",
        snapshots_dir.display()
    );
    Ok(())
}
