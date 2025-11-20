//! Integration test demonstrating freenet-test-network usage
//!
//! This shows how much simpler tests become with the test-network crate

use freenet_stdlib::client_api::WebApi;
use freenet_test_network::TestNetwork;
use testresult::TestResult;
use tokio_tungstenite::connect_async;

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

#[tokio::test]
async fn test_network_connectivity() -> TestResult {
    let network = get_network().await;

    // Just verify we can connect to all peers
    let gw_url = format!("{}?encodingProtocol=native", network.gateway(0).ws_url());
    let (stream, _) = connect_async(&gw_url).await?;
    let _gw_client = WebApi::start(stream);

    let peer_url = format!("{}?encodingProtocol=native", network.peer(0).ws_url());
    let (stream, _) = connect_async(&peer_url).await?;
    let _peer_client = WebApi::start(stream);

    println!("✓ Successfully connected to gateway and peer");
    Ok(())
}

#[tokio::test]
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

    println!("✓ Multiple WebSocket connections work");
    Ok(())
}
