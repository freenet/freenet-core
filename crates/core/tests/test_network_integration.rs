#![cfg(feature = "test-network")]
//! Integration test demonstrating freenet-test-network usage
//!
//! This shows how much simpler tests become with the test-network crate

use freenet_stdlib::client_api::WebApi;
use freenet_test_network::TestNetwork;
use testresult::TestResult;
use tokio_tungstenite::connect_async;
use tracing::info;

// Helper to get or create network
async fn get_network() -> TestNetwork {
    TestNetwork::builder()
        .gateways(1)
        .peers(2)
        .binary(freenet_test_network::FreenetBinary::CurrentCrate(
            freenet_test_network::BuildProfile::Debug,
        ))
        .build()
        .await
        .expect("Failed to start test network")
}

#[test_log::test(tokio::test)]
async fn test_network_connectivity() -> TestResult {
    let network = get_network().await;

    // Just verify we can connect to all peers
    let gw_url = format!("{}?encodingProtocol=native", network.gateway(0).ws_url());
    let (stream, _) = connect_async(&gw_url).await?;
    let _gw_client = WebApi::start(stream);

    let peer_url = format!("{}?encodingProtocol=native", network.peer(0).ws_url());
    let (stream, _) = connect_async(&peer_url).await?;
    let _peer_client = WebApi::start(stream);

    info!("Successfully connected to gateway and peer");
    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_multiple_connections() -> TestResult {
    let network = get_network().await;

    // Each test gets its own connections - no conflicts
    let url1 = format!("{}?encodingProtocol=native", network.gateway(0).ws_url());
    let (stream1, _) = connect_async(&url1).await?;
    let _client1 = WebApi::start(stream1);

    let url2 = format!("{}?encodingProtocol=native", network.peer(0).ws_url());
    let (stream2, _) = connect_async(&url2).await?;
    let _client2 = WebApi::start(stream2);

    let url3 = format!("{}?encodingProtocol=native", network.peer(1).ws_url());
    let (stream3, _) = connect_async(&url3).await?;
    let _client3 = WebApi::start(stream3);

    info!("Multiple WebSocket connections work");
    Ok(())
}

/// Verify that all peers have ring locations after network startup.
///
/// This test catches bugs where ObservedAddress is not properly sent to peers,
/// which would leave them without a ring location (loc=N/A).
///
/// See PR #2333 for context on why this validation is important.
#[test_log::test(tokio::test)]
async fn test_all_peers_have_ring_locations() -> TestResult {
    use std::time::Duration;
    use tokio::time::sleep;

    let network = get_network().await;

    // Give network time to fully establish connections and exchange locations
    sleep(Duration::from_secs(5)).await;

    let diagnostics = network.collect_diagnostics().await?;

    let mut missing_locations = Vec::new();
    for peer in &diagnostics.peers {
        let has_location = peer
            .location
            .as_ref()
            .map(|s| !s.is_empty() && s != "N/A")
            .unwrap_or(false);

        if !has_location {
            missing_locations.push(peer.peer_id.clone());
        }
    }

    assert!(
        missing_locations.is_empty(),
        "Peers missing ring locations (ObservedAddress may not be working): {:?}",
        missing_locations
    );

    info!("All {} peers have ring locations", diagnostics.peers.len());
    Ok(())
}
