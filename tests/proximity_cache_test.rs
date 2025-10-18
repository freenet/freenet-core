use std::time::Duration;
use freenet::dev_tool::TestNetwork;
use freenet_stdlib::client_api::{ClientRequest, HostResponse, NodeQuery, QueryResponse};

#[tokio::test(flavor = "multi_thread")]
async fn test_proximity_cache_query() -> Result<(), Box<dyn std::error::Error>> {
    // Start a small test network
    let mut network = TestNetwork::builder()
        .with_num_gateways(1)
        .with_num_nodes(3)
        .build()
        .await;

    // Wait for network to stabilize
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Get a client for one of the nodes
    let mut client = network.client(0).await?;

    // Send proximity cache query
    client
        .send(ClientRequest::NodeQueries(NodeQuery::ProximityCacheInfo))
        .await?;

    // Receive the response
    let response = client.recv().await?;

    // Verify we get the correct response type
    match response {
        HostResponse::QueryResponse(QueryResponse::ProximityCache(info)) => {
            println!("✓ Successfully queried proximity cache");
            println!("  My cache entries: {}", info.my_cache.len());
            println!("  Neighbor caches: {}", info.neighbor_caches.len());
            println!("  Cache announces sent: {}", info.stats.cache_announces_sent);
            println!("  Cache announces received: {}", info.stats.cache_announces_received);
            println!("  Updates via proximity: {}", info.stats.updates_via_proximity);
            println!("  Updates via subscription: {}", info.stats.updates_via_subscription);
            Ok(())
        }
        _ => {
            panic!("Expected ProximityCache response, got: {:?}", response);
        }
    }
}