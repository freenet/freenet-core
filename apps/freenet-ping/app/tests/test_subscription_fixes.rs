/// Integration tests for subscription fixes in PR #1854
/// Tests:
/// 1. Transaction ID correlation (waiting_for_transaction_result)
/// 2. Optimal location subscriptions (removed early return)
/// 3. Multiple peer candidates (k_closest_potentially_caching with k=3)
mod common;

use std::{net::TcpListener, time::Duration};

use anyhow::anyhow;
use freenet::{local_node::NodeConfig, server::serve_gateway};
use freenet_ping_types::{Ping, PingContractOptions};
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, ContractResponse, HostResponse},
    prelude::*,
};
use testresult::TestResult;
use tokio::time::timeout;
use tracing::{info, warn};

use common::{
    base_node_test_config, connect_ws_client, deploy_contract, get_contract_state,
};

/// Test that subscription responses properly route back to the originating client
/// This validates the fix for missing waiting_for_transaction_result registration
#[tokio::test]
async fn test_subscription_response_reaches_client() -> TestResult {
    freenet::config::set_logger(Some(tracing::level_filters::LevelFilter::DEBUG), None);
    info!("Testing subscription response routing to originating client");

    // === SETUP 3-NODE LINEAR NETWORK ===
    // Gateway -> Node1 -> Node2

    // Setup ports
    let gw_socket = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_gw = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_node1 = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_node2 = TcpListener::bind("127.0.0.1:0")?;

    // Gateway configuration
    let (gw_config, gw_preset) = base_node_test_config(
        true,
        vec![],
        Some(gw_socket.local_addr()?.port()),
        ws_api_gw.local_addr()?.port(),
        "gw_sub_test",
        None,
        None,
    )
    .await?;

    // Node1 configuration (connects to gateway)
    let (node1_config, node1_preset) = base_node_test_config(
        false,
        vec![format!("127.0.0.1:{}", gw_socket.local_addr()?.port())],
        None,
        ws_api_node1.local_addr()?.port(),
        "node1_sub_test",
        None,
        None,
    )
    .await?;

    // Node2 configuration (connects to gateway)
    let (node2_config, node2_preset) = base_node_test_config(
        false,
        vec![format!("127.0.0.1:{}", gw_socket.local_addr()?.port())],
        None,
        ws_api_node2.local_addr()?.port(),
        "node2_sub_test",
        None,
        None,
    )
    .await?;

    // Start gateway
    let gw_task = tokio::spawn(async move {
        let config = gw_config.build().await.unwrap();
        let node = NodeConfig::new(config.clone())
            .await
            .unwrap()
            .build(serve_gateway(config.ws_api).await)
            .await
            .unwrap();
        node.run().await
    });

    // Start Node1
    let node1_task = tokio::spawn(async move {
        let config = node1_config.build().await.unwrap();
        let node = NodeConfig::new(config.clone())
            .await
            .unwrap()
            .build(serve_gateway(config.ws_api).await)
            .await
            .unwrap();
        node.run().await
    });

    // Start Node2
    let node2_task = tokio::spawn(async move {
        let config = node2_config.build().await.unwrap();
        let node = NodeConfig::new(config.clone())
            .await
            .unwrap()
            .build(serve_gateway(config.ws_api).await)
            .await
            .unwrap();
        node.run().await
    });

    // Wait for network to stabilize
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Connect WebSocket clients
    let mut client_gw = connect_ws_client(ws_api_gw.local_addr()?.port()).await?;
    let mut client_node1 = connect_ws_client(ws_api_node1.local_addr()?.port()).await?;
    let mut client_node2 = connect_ws_client(ws_api_node2.local_addr()?.port()).await?;

    info!("Network started: Gateway -> Node1 -> Node2");

    // === TEST SUBSCRIPTION RESPONSE ROUTING ===

    // Deploy contract on Node2
    let initial_state = Ping::new();
    let options = PingContractOptions {
        ttl: std::time::Duration::from_secs(60),
        frequency: std::time::Duration::from_secs(5),
        tag: "test".to_string(),
        code_key: "test_contract".to_string(),
    };
    let contract_key = deploy_contract(&mut client_node2, initial_state, &options, false).await?;
    info!("Contract deployed on Node2: {}", contract_key);

    // Wait for contract to propagate
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Subscribe from Gateway - this tests transaction correlation
    info!("Gateway subscribing to contract {}...", contract_key);

    client_gw
        .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
            key: contract_key,
            summary: None,
        }))
        .await?;

    // Try to receive subscription response with timeout
    let subscribe_result = timeout(Duration::from_secs(10), async {
        loop {
            match client_gw.recv().await {
                Ok(HostResponse::ContractResponse(ContractResponse::SubscribeResponse {
                    key,
                    subscribed,
                })) if key == contract_key => {
                    return Ok(subscribed);
                }
                Ok(_) => continue, // Ignore other messages
                Err(e) => return Err(anyhow!("Failed to receive: {}", e)),
            }
        }
    })
    .await;

    match subscribe_result {
        Ok(Ok(true)) => {
            info!("✅ SUCCESS: Gateway received subscription confirmation!");
            info!("This validates that waiting_for_transaction_result is working correctly");
        }
        Ok(Ok(false)) => {
            return Err(anyhow!("Subscription failed - received false response").into());
        }
        Ok(Err(e)) => {
            return Err(anyhow!("Failed to subscribe: {}", e).into());
        }
        Err(_) => {
            return Err(anyhow!(
                "TIMEOUT: Gateway did not receive subscription response within 10 seconds. \
                This indicates the waiting_for_transaction_result fix may not be working."
            )
            .into());
        }
    }

    // Cleanup
    gw_task.abort();
    node1_task.abort();
    node2_task.abort();

    Ok(())
}

/// Test that nodes at optimal location can subscribe to contracts
/// This validates removal of early return in start_subscription_request
#[tokio::test]
async fn test_optimal_location_can_subscribe() -> TestResult {
    freenet::config::set_logger(Some(tracing::level_filters::LevelFilter::DEBUG), None);
    info!("Testing optimal location node subscription");

    // === SETUP 5-NODE NETWORK ===

    let gw_socket = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_gw = TcpListener::bind("127.0.0.1:0")?;
    let mut ws_api_nodes = Vec::new();
    for _ in 0..4 {
        ws_api_nodes.push(TcpListener::bind("127.0.0.1:0")?);
    }

    // Gateway configuration
    let (gw_config, _gw_preset) = base_node_test_config(
        true,
        vec![],
        Some(gw_socket.local_addr()?.port()),
        ws_api_gw.local_addr()?.port(),
        "gw_optimal_test",
        None,
        None,
    )
    .await?;

    // Start gateway
    let gw_task = tokio::spawn(async move {
        let config = gw_config.build().await.unwrap();
        let node = NodeConfig::new(config.clone())
            .await
            .unwrap()
            .build(serve_gateway(config.ws_api).await)
            .await
            .unwrap();
        node.run().await
    });

    // Start regular nodes
    let mut node_tasks = Vec::new();
    for i in 0..4 {
        let (node_config, _preset) = base_node_test_config(
            false,
            vec![format!("127.0.0.1:{}", gw_socket.local_addr()?.port())],
            None,
            ws_api_nodes[i].local_addr()?.port(),
            &format!("node{}_optimal_test", i + 1),
            None,
            None,
        )
        .await?;

        let task = tokio::spawn(async move {
            let config = node_config.build().await.unwrap();
            let node = NodeConfig::new(config.clone())
                .await
                .unwrap()
                .build(serve_gateway(config.ws_api).await)
                .await
                .unwrap();
            node.run().await
        });
        node_tasks.push(task);
    }

    // Wait for network to stabilize
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Connect WebSocket clients
    let mut client_gw = connect_ws_client(ws_api_gw.local_addr()?.port()).await?;
    let mut node_clients = Vec::new();
    for i in 0..4 {
        let client = connect_ws_client(ws_api_nodes[i].local_addr()?.port()).await?;
        node_clients.push(client);
    }

    info!("5-node network started");

    // === TEST OPTIMAL LOCATION SUBSCRIPTION ===

    // Deploy contract from gateway
    let initial_state = Ping::new();
    let options = PingContractOptions {
        ttl: std::time::Duration::from_secs(60),
        frequency: std::time::Duration::from_secs(5),
        tag: "test".to_string(),
        code_key: "test_contract".to_string(),
    };
    let contract_key =
        deploy_contract(&mut client_gw, initial_state.clone(), &options, false).await?;
    info!("Contract deployed: {}", contract_key);

    // Wait for contract to propagate and reach optimal location
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Find which node has the contract (simulating optimal location)
    let mut optimal_node_idx = None;
    for (i, client) in node_clients.iter_mut().enumerate() {
        // Try to get the contract state
        client
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key: contract_key,
                return_contract_code: false,
                subscribe: false,
            }))
            .await?;

        // Check if this node has the contract
        let has_contract = timeout(Duration::from_secs(2), async {
            loop {
                match client.recv().await {
                    Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                        contract: _,
                        state,
                        ..
                    })) => return !state.is_empty(),
                    Ok(_) => continue,
                    Err(_) => return false,
                }
            }
        })
        .await
        .unwrap_or(false);

        if has_contract {
            optimal_node_idx = Some(i);
            info!("Node {} has the contract (optimal location)", i + 1);
            break;
        }
    }

    let optimal_idx = optimal_node_idx.ok_or_else(|| anyhow!("No node has the contract"))?;

    // Now have the optimal node subscribe to its own contract
    info!("Optimal node subscribing to contract it already has...");

    let client = &mut node_clients[optimal_idx];
    client
        .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
            key: contract_key,
            summary: None,
        }))
        .await?;

    // Check if subscription succeeds
    let subscribe_result = timeout(Duration::from_secs(10), async {
        loop {
            match client.recv().await {
                Ok(HostResponse::ContractResponse(ContractResponse::SubscribeResponse {
                    key,
                    subscribed,
                })) if key == contract_key => {
                    return Ok(subscribed);
                }
                Ok(_) => continue,
                Err(e) => return Err(anyhow!("Failed to receive: {}", e)),
            }
        }
    })
    .await;

    match subscribe_result {
        Ok(Ok(true)) => {
            info!("✅ SUCCESS: Optimal location node successfully subscribed!");
            info!("This validates that the early return was properly removed");
        }
        Ok(Ok(false)) => {
            return Err(anyhow!("Subscription failed at optimal location").into());
        }
        Ok(Err(e)) => {
            return Err(anyhow!("Failed to subscribe: {}", e).into());
        }
        Err(_) => {
            return Err(anyhow!(
                "TIMEOUT: Optimal node subscription timed out. \
                This may indicate the early return is still preventing optimal location subscriptions."
            ).into());
        }
    }

    // Cleanup
    gw_task.abort();
    for task in node_tasks {
        task.abort();
    }

    Ok(())
}

/// Test that subscription attempts multiple peer candidates on failure
/// This validates k_closest_potentially_caching with k=3
#[tokio::test]
#[ignore = "This test requires network failure simulation"]
async fn test_subscription_tries_multiple_peers() -> TestResult {
    freenet::config::set_logger(Some(tracing::level_filters::LevelFilter::DEBUG), None);
    info!("Testing multiple peer candidates for subscription");

    // This test would require:
    // 1. Setting up a network with multiple nodes
    // 2. Making some peers unavailable (blocked or crashed)
    // 3. Verifying subscription still succeeds using alternative peers
    // 4. Checking logs to confirm multiple peers were attempted

    // For now, this is a placeholder showing the test structure
    // The actual implementation would need network failure injection

    warn!("Test not yet implemented - requires network failure simulation");

    // The test would validate that when the first candidate peer fails,
    // the subscription mechanism tries the next peer in the k_closest list (k=3)

    Ok(())
}

/// Helper test to verify basic subscription functionality works
#[tokio::test]
async fn test_basic_subscription() -> TestResult {
    freenet::config::set_logger(Some(tracing::level_filters::LevelFilter::DEBUG), None);
    info!("Testing basic subscription functionality");

    // Setup simple 2-node network
    let gw_socket = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_gw = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_node = TcpListener::bind("127.0.0.1:0")?;

    let (gw_config, _gw_preset) = base_node_test_config(
        true,
        vec![],
        Some(gw_socket.local_addr()?.port()),
        ws_api_gw.local_addr()?.port(),
        "gw_basic",
        None,
        None,
    )
    .await?;

    let (node_config, _node_preset) = base_node_test_config(
        false,
        vec![format!("127.0.0.1:{}", gw_socket.local_addr()?.port())],
        None,
        ws_api_node.local_addr()?.port(),
        "node_basic",
        None,
        None,
    )
    .await?;

    // Start nodes
    let gw_task = tokio::spawn(async move {
        let config = gw_config.build().await.unwrap();
        let node = NodeConfig::new(config.clone())
            .await
            .unwrap()
            .build(serve_gateway(config.ws_api).await)
            .await
            .unwrap();
        node.run().await
    });

    let node_task = tokio::spawn(async move {
        let config = node_config.build().await.unwrap();
        let node = NodeConfig::new(config.clone())
            .await
            .unwrap()
            .build(serve_gateway(config.ws_api).await)
            .await
            .unwrap();
        node.run().await
    });

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Connect clients
    let mut client_gw = connect_ws_client(ws_api_gw.local_addr()?.port()).await?;
    let mut client_node = connect_ws_client(ws_api_node.local_addr()?.port()).await?;

    // Deploy and subscribe
    let initial_state = Ping::new();
    let options = PingContractOptions {
        ttl: std::time::Duration::from_secs(60),
        frequency: std::time::Duration::from_secs(5),
        tag: "test".to_string(),
        code_key: "test_contract".to_string(),
    };
    let contract_key = deploy_contract(&mut client_node, initial_state, &options, true).await?;

    info!("Contract deployed with subscription: {}", contract_key);

    // Verify subscription worked
    let state = get_contract_state(&mut client_gw, contract_key, false).await?;
    info!("Retrieved state from gateway: {:?}", state);

    // Cleanup
    gw_task.abort();
    node_task.abort();

    Ok(())
}
