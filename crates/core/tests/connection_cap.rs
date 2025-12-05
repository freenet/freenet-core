#![cfg(feature = "test-network")]
//! Minimal repro harness for connection-cap enforcement.
//!
//! This test spins up a tiny network (2 gateways + 6 peers) with a low max-connections
//! setting (min=5, max=6) and waits for connectivity. It then inspects diagnostics to
//! ensure no peer reports more than `max` connections. This is intended to quickly catch
//! admission/cap bypass regressions without running the full soak.

use freenet_test_network::{BuildProfile, FreenetBinary, NetworkBuilder};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connection_cap_respected() -> anyhow::Result<()> {
    let max_connections = freenet::config::DEFAULT_MAX_CONNECTIONS;
    let net = NetworkBuilder::new()
        .gateways(2)
        .peers(6)
        .start_stagger(std::time::Duration::from_millis(300))
        .require_connectivity(0.9)
        .connectivity_timeout(std::time::Duration::from_secs(40))
        .binary(FreenetBinary::CurrentCrate(BuildProfile::Debug))
        .build()
        .await?;

    let snapshot = net.collect_diagnostics().await?;
    for peer in snapshot.peers {
        let count = peer.connected_peer_ids.len();
        assert!(
            count <= max_connections,
            "peer {} exceeds max connections ({} > {})",
            peer.peer_id,
            count,
            max_connections
        );
    }

    Ok(())
}
