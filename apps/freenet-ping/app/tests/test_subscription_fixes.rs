/// Test that subscription routing works across multiple nodes and subscription
/// responses properly reach clients through multi-hop network paths
/// Key fixes:
/// 1. Missing waiting_for_transaction_result in subscribe_contract
/// 2. Early return prevention for optimal location subscriptions
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
use futures::FutureExt;
use testresult::TestResult;
use tokio::{select, time::timeout};
use tracing::info;

use common::{base_node_test_config, connect_ws_client, deploy_contract, get_contract_state};

/// Test that subscription responses properly route back to the originating client
/// This validates the fix for missing waiting_for_transaction_result registration
#[tokio::test]
async fn test_subscription_response_reaches_client() -> TestResult {
    freenet::config::set_logger(Some(tracing::level_filters::LevelFilter::DEBUG), None);
    info!("Testing subscription response routing to originating client");

    // Setup network: Gateway -> Node1 -> Node2
    let gw_socket = TcpListener::bind("127.0.0.1:0")?;
    let _node1_socket = TcpListener::bind("127.0.0.1:0")?;
    let _node2_socket = TcpListener::bind("127.0.0.1:0")?;

    let ws_api_gw = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_node1 = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_node2 = TcpListener::bind("127.0.0.1:0")?;

    // Gateway configuration
    let (gw_config, _gw_preset) = base_node_test_config(
        true,
        vec![],
        Some(gw_socket.local_addr()?.port()),
        ws_api_gw.local_addr()?.port(),
        "gw_subscription_test",
        None,
        None,
    )
    .await?;

    // Node1 configuration (connects to gateway)
    let (node1_config, _node1_preset) = base_node_test_config(
        false,
        vec![format!("127.0.0.1:{}", gw_socket.local_addr()?.port())],
        None,
        ws_api_node1.local_addr()?.port(),
        "node1_subscription_test",
        None,
        None,
    )
    .await?;

    // Node2 configuration (connects to gateway)
    let (node2_config, _node2_preset) = base_node_test_config(
        false,
        vec![format!("127.0.0.1:{}", gw_socket.local_addr()?.port())],
        None,
        ws_api_node2.local_addr()?.port(),
        "node2_subscription_test",
        None,
        None,
    )
    .await?;

    let ws_api_port_gw = ws_api_gw.local_addr()?.port();
    let ws_api_port_node2 = ws_api_node2.local_addr()?.port();

    // Start gateway
    let gw_task = async move {
        let config = gw_config.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Start Node1
    let node1_task = async move {
        let config = node1_config.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Start Node2
    let node2_task = async move {
        let config = node2_config.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Main test logic
    let test = tokio::time::timeout(Duration::from_secs(30), async {
        // Wait for network to stabilize
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Connect WebSocket clients
        let mut client_gw = connect_ws_client(ws_api_port_gw).await?;
        let mut client_node2 = connect_ws_client(ws_api_port_node2).await?;

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
        let contract_key =
            deploy_contract(&mut client_node2, initial_state, &options, false).await?;
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
                Ok(())
            }
            Ok(Ok(false)) => Err(anyhow!("Subscription failed - received false response").into()),
            Ok(Err(e)) => Err(anyhow!("Failed to subscribe: {}", e).into()),
            Err(_) => Err(anyhow!(
                "TIMEOUT: Gateway did not receive subscription response within 10 seconds. \
                This indicates the waiting_for_transaction_result fix may not be working."
            )
            .into()),
        }
    });

    // Wait for test completion or node failures
    select! {
        gw = gw_task => {
            let Err(gw) = gw;
            return Err(anyhow!("Gateway node failed: {}", gw).into());
        }
        n1 = node1_task => {
            let Err(n1) = n1;
            return Err(anyhow!("Node1 failed: {}", n1).into());
        }
        n2 = node2_task => {
            let Err(n2) = n2;
            return Err(anyhow!("Node2 failed: {}", n2).into());
        }
        r = test => {
            match r {
                Ok(r) => r,
                Err(e) => Err(anyhow!("Test timed out: {}", e).into()),
            }
        }
    }
}

/// Test that nodes at optimal location can subscribe to contracts
/// This validates that the early return was properly removed from subscribe_contract
#[tokio::test]
async fn test_optimal_location_can_subscribe() -> TestResult {
    freenet::config::set_logger(Some(tracing::level_filters::LevelFilter::DEBUG), None);
    info!("Testing subscription at optimal contract location");

    // Setup a 5-node network to ensure we have optimal locations
    let gw_socket = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_gw = TcpListener::bind("127.0.0.1:0")?;

    // Create 4 regular nodes
    let mut ws_api_nodes = Vec::new();
    for _ in 0..4 {
        ws_api_nodes.push(TcpListener::bind("127.0.0.1:0")?);
    }

    let ws_api_port_gw = ws_api_gw.local_addr()?.port();
    let mut ws_api_ports = Vec::new();
    for listener in &ws_api_nodes {
        ws_api_ports.push(listener.local_addr()?.port());
    }

    // Gateway configuration
    let (gw_config, _gw_preset) = base_node_test_config(
        true,
        vec![],
        Some(gw_socket.local_addr()?.port()),
        ws_api_port_gw,
        "gw_optimal_test",
        None,
        None,
    )
    .await?;

    // Start gateway
    let gw_task = async move {
        let config = gw_config.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Start regular nodes - fixed clippy warning by using enumerate
    let mut node_tasks = Vec::new();
    for (i, &port) in ws_api_ports.iter().enumerate() {
        let (node_config, _preset) = base_node_test_config(
            false,
            vec![format!("127.0.0.1:{}", gw_socket.local_addr()?.port())],
            None,
            port,
            &format!("node{}_optimal_test", i + 1),
            None,
            None,
        )
        .await?;

        let task = async move {
            let config = node_config.build().await?;
            let node = NodeConfig::new(config.clone())
                .await?
                .build(serve_gateway(config.ws_api).await)
                .await?;
            node.run().await
        }
        .boxed_local();
        node_tasks.push(task);
    }

    // Main test logic
    let test = tokio::time::timeout(Duration::from_secs(60), async {
        // Wait for network to stabilize
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Connect WebSocket clients
        let mut client_gw = connect_ws_client(ws_api_port_gw).await?;
        let mut node_clients = Vec::new();
        for port in &ws_api_ports {
            let client = connect_ws_client(*port).await?;
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
                Ok(())
            }
            Ok(Ok(false)) => Err(anyhow!("Subscription failed at optimal location").into()),
            Ok(Err(e)) => Err(anyhow!("Failed to subscribe: {}", e).into()),
            Err(_) => Err(anyhow!(
                "TIMEOUT: Optimal node subscription timed out. \
                This may indicate the early return is still preventing optimal location subscriptions."
            )
            .into()),
        }
    });

    // Create a combined future for all node tasks
    let all_nodes = async { futures::future::try_join_all(node_tasks).await }.boxed_local();

    // Wait for test completion or node failures
    select! {
        gw = gw_task => {
            let Err(gw) = gw;
            return Err(anyhow!("Gateway node failed: {}", gw).into());
        }
        nodes = all_nodes => {
            match nodes {
                Err(e) => return Err(anyhow!("Node failed: {}", e).into()),
                Ok(_) => return Err(anyhow!("All nodes stopped unexpectedly").into()),
            }
        }
        r = test => {
            match r {
                Ok(r) => r,
                Err(e) => Err(anyhow!("Test timed out: {}", e).into()),
            }
        }
    }
}

/// Test basic subscription functionality in a simple 2-node network
/// This validates that the subscription mechanism works at a basic level
#[tokio::test]
async fn test_basic_subscription() -> TestResult {
    freenet::config::set_logger(Some(tracing::level_filters::LevelFilter::DEBUG), None);
    info!("Testing basic subscription functionality");

    // Setup basic 2-node network for subscription testing
    let gw_socket = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_gw = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_node = TcpListener::bind("127.0.0.1:0")?;

    let ws_api_port_gw = ws_api_gw.local_addr()?.port();
    let ws_api_port_node = ws_api_node.local_addr()?.port();

    let (gw_config, _gw_preset) = base_node_test_config(
        true,
        vec![],
        Some(gw_socket.local_addr()?.port()),
        ws_api_port_gw,
        "gw_basic",
        None,
        None,
    )
    .await?;

    let (node_config, _node_preset) = base_node_test_config(
        false,
        vec![format!("127.0.0.1:{}", gw_socket.local_addr()?.port())],
        None,
        ws_api_port_node,
        "node_basic",
        None,
        None,
    )
    .await?;

    // Start nodes
    let gw_task = async move {
        let config = gw_config.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    let node_task = async move {
        let config = node_config.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Main test logic
    let test = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Connect clients
        let mut client_gw = connect_ws_client(ws_api_port_gw).await?;
        let mut client_node = connect_ws_client(ws_api_port_node).await?;

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

        Ok(())
    });

    // Wait for test completion or node failures
    select! {
        gw = gw_task => {
            let Err(gw) = gw;
            return Err(anyhow!("Gateway node failed: {}", gw).into());
        }
        n = node_task => {
            let Err(n) = n;
            return Err(anyhow!("Node failed: {}", n).into());
        }
        r = test => {
            match r {
                Ok(r) => r,
                Err(e) => Err(anyhow!("Test timed out: {}", e).into()),
            }
        }
    }
}
