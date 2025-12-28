#![cfg(feature = "test-network")]
//! Regression test: gateways must register inbound peers under their real identities
//! rather than collapsing multiple connections under a placeholder.
//!
//! This targets the bug where `HandshakeEvent::InboundEstablished` arrived with `peer=None`
//! and the bridge synthesized a PeerId with its own pubkey, never re-keying to the remote.
//! The result: gateways had zero routable neighbors even though peers connected successfully.

use std::time::Duration;

use freenet_test_network::{BuildProfile, FreenetBinary, TestNetwork};

/// Gateways should expose at least one non-gateway neighbor in their ring snapshot after peers join.
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn gateway_records_real_peer_ids_on_inbound() -> anyhow::Result<()> {
    // Small, fast network: 1 gateway + 2 peers.
    let network = TestNetwork::builder()
        .gateways(1)
        .peers(2)
        .require_connectivity(0.5) // Allow startup even if the bug is present; assertions below enforce correctness.
        .connectivity_timeout(Duration::from_secs(20))
        .preserve_temp_dirs_on_failure(true)
        .binary(FreenetBinary::CurrentCrate(BuildProfile::Debug))
        .build()
        .await?;

    // Give topology maintenance a moment to run.
    tokio::time::sleep(Duration::from_secs(5)).await;

    let snapshots = network.ring_snapshot().await?;
    let gateways: Vec<_> = snapshots.iter().filter(|p| p.is_gateway).collect();
    assert_eq!(
        gateways.len(),
        1,
        "expected exactly one gateway in the snapshot"
    );

    let gateway = gateways[0];
    assert!(
        !gateway.connections.is_empty(),
        "gateway should report at least one peer connection, found none"
    );
    assert!(
        gateway
            .connections
            .iter()
            .all(|peer_id| peer_id != &gateway.id),
        "gateway connections must reference remote peers, not itself"
    );

    Ok(())
}
