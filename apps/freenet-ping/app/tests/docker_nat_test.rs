//! Docker NAT integration test for contract operations.
//!
//! Validates that contract PUT, SUBSCRIBE, UPDATE, and GET work correctly
//! when peers are behind NAT (Docker-simulated). This replaces the six-peer
//! River regression test with a lighter, self-contained freenet-ping test.
//!
//! Requires: FREENET_TEST_DOCKER_NAT=1, FREENET_BINARY_PATH, Docker daemon.

mod common;

use anyhow::Result;
use common::{
    connect_ws_with_retry, deploy_contract, get_contract_state, subscribe_to_contract,
    update_contract_state,
};
use freenet_ping_types::{Ping, PingContractOptions};
use freenet_test_network::{FreenetBinary, NetworkBuilder, TestNetwork};
use std::time::Duration;

/// Test contract operations (PUT -> SUBSCRIBE -> UPDATE -> GET) across a
/// Docker NAT network with 1 gateway and 3 peers.
///
/// This is the primary CI validation for NAT hole-punching. It proves that:
/// 1. Peers behind NAT can connect to the gateway
/// 2. Contract deployment propagates across NAT boundaries
/// 3. Subscriptions work across NAT
/// 4. State updates are received by subscribing peers
#[ignore] // Only runs when FREENET_TEST_DOCKER_NAT=1
#[tokio::test]
async fn test_contract_operations_via_docker_nat() -> Result<()> {
    // Require Docker NAT env var — fail loudly if someone removes #[ignore]
    // and runs this without Docker.
    if std::env::var("FREENET_TEST_DOCKER_NAT").is_err() {
        panic!(
            "FREENET_TEST_DOCKER_NAT must be set. \
             This test requires Docker for NAT simulation."
        );
    }

    let binary_path = std::env::var("FREENET_BINARY_PATH")
        .expect("FREENET_BINARY_PATH must point to the freenet binary");

    // Build a 1-gateway + 3-peer Docker NAT network.
    // Backend::default() reads FREENET_TEST_DOCKER_NAT and creates DockerNat.
    let network: TestNetwork = NetworkBuilder::new()
        .gateways(1)
        .peers(3)
        .binary(FreenetBinary::Path(binary_path.into()))
        .connectivity_timeout(Duration::from_secs(120))
        .build()
        .await
        .expect("Failed to build Docker NAT network");

    // --- Connect WebSocket clients to gateway and one peer ---
    let gw_url = format!("{}?encodingProtocol=native", network.gateway(0).ws_url());
    let peer_url = format!("{}?encodingProtocol=native", network.peer(0).ws_url());

    let mut gw_client = connect_ws_with_retry(&gw_url, "gateway", 30).await?;
    let mut peer_client = connect_ws_with_retry(&peer_url, "peer0", 30).await?;

    // --- Deploy contract from gateway (PUT + subscribe) ---
    let options = PingContractOptions::default();
    let initial_state = Ping::default();
    let contract_key = deploy_contract(&mut gw_client, initial_state, &options, true).await?;
    tracing::info!("Contract deployed: {contract_key}");

    // --- Subscribe from peer ---
    subscribe_to_contract(&mut peer_client, contract_key).await?;
    tracing::info!("Peer subscribed to contract");

    // Allow subscription propagation across the network
    tokio::time::sleep(Duration::from_secs(15)).await;

    // --- Update state from gateway ---
    // Ping::insert() adds a timestamped entry under the given peer name.
    let mut update = Ping::default();
    update.insert("nat-test-node".to_string());
    update_contract_state(&mut gw_client, contract_key, update).await?;
    tracing::info!("State update sent from gateway");

    // Allow update propagation
    tokio::time::sleep(Duration::from_secs(10)).await;

    // --- Verify peer received the update ---
    let peer_state = get_contract_state(&mut peer_client, contract_key, false).await?;
    assert!(
        peer_state.contains_key("nat-test-node"),
        "Peer should have received the 'nat-test-node' key via subscription. \
         Got keys: {:?}",
        peer_state.keys().collect::<Vec<_>>()
    );

    tracing::info!("Docker NAT contract operations test passed");
    Ok(())
}
