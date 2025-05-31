/// Test to reproduce the get operation failure in small, poorly connected networks
/// This simulates the real network conditions where contracts can't be retrieved
mod common;

use anyhow::{anyhow, Result};
use common::{test_packet_loss_errors_connection, TestNodeBuilder};
use freenet::client_api::{ClientRequest, ContractRequest, HostResponse};
use freenet_ping_types::run_test;
use futures::StreamExt;
use std::time::Duration;
use testresult::TestResult;
use tokio::time::timeout;
use tracing::level_filters::LevelFilter;

#[tokio::test(flavor = "multi_thread")]
async fn test_small_network_get_failure() -> TestResult {
    freenet::config::set_logger(Some(LevelFilter::DEBUG), None);

    /*
     * This test simulates the real network issue where:
     * 1. A small number of peers (matching production)
     * 2. Poor connectivity between peers
     * 3. One peer publishes a contract
     * 4. Other peers try to GET the contract but fail
     */

    // Small network like production
    const NUM_GATEWAYS: usize = 1;
    const NUM_NODES: usize = 3; // Total 4 peers like in production

    println!("🔧 Testing get operation in small, poorly connected network");
    println!(
        "   Simulating production conditions with {} total peers",
        NUM_GATEWAYS + NUM_NODES
    );

    // Create gateway
    let gateway = TestNodeBuilder::new(0).gateway().build()?;

    // Create nodes with limited connectivity
    // Node 1 connects only to gateway
    let node1 = TestNodeBuilder::new(1).connect_to(&gateway).build()?;

    // Node 2 connects only to gateway (no connection to node1)
    let node2 = TestNodeBuilder::new(2).connect_to(&gateway).build()?;

    // Node 3 connects to node1 but NOT gateway or node2
    // This creates a chain: gateway <- node1 <- node3
    //                              <- node2
    let node3 = TestNodeBuilder::new(3).connect_to(&node1).build()?;

    println!("Network topology:");
    println!("  Gateway(0)");
    println!("    ├── Node1(1)");
    println!("    │     └── Node3(3)");
    println!("    └── Node2(2)");
    println!();
    println!("Note: Node2 and Node3 can only reach each other through gateway/node1");

    // Start all nodes
    let mut futures = vec![];
    futures.push(run_test(gateway.node, async { Ok(()) }));
    futures.push(run_test(node1.node, async { Ok(()) }));
    futures.push(run_test(node2.node, async { Ok(()) }));
    futures.push(run_test(node3.node, async { Ok(()) }));

    // Allow network to stabilize
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Node1 publishes a contract
    println!("📤 Node1 publishing ping contract...");
    let contract = common::build_ping_contract(&[1])?;
    let contract_key = contract.key();

    node1
        .client
        .send(ClientRequest::ContractOp(ContractRequest::Put {
            contract: contract.clone(),
            state: vec![].into(),
            related_contracts: Default::default(),
            subscribe: false,
        }))
        .await?;

    // Wait for put response
    match timeout(Duration::from_secs(10), node1.client.recv()).await {
        Ok(Ok(HostResponse::ContractResponse(response))) => {
            println!("✅ Node1 successfully published contract: {:?}", response);
        }
        _ => {
            return Err(anyhow!("Failed to get put response").into());
        }
    }

    // Give time for any propagation
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Now Node3 (furthest from Node1) tries to GET the contract
    println!("📥 Node3 attempting to GET the contract...");
    println!("   (Node3 is 2 hops away from Node1 through the network)");

    node3
        .client
        .send(ClientRequest::ContractOp(ContractRequest::Get {
            key: contract_key,
            return_contract_code: true,
            subscribe: false,
        }))
        .await?;

    // Wait for get response with longer timeout
    let start = std::time::Instant::now();
    match timeout(Duration::from_secs(30), node3.client.recv()).await {
        Ok(Ok(HostResponse::ContractResponse(response))) => {
            println!("Get response after {:?}: {:?}", start.elapsed(), response);
            match response {
                freenet::client_api::ContractResponse::GetResponse { key, state, .. }
                    if key == contract_key && state.is_some() =>
                {
                    println!("✅ Node3 successfully retrieved the contract!");
                    // This would be surprising given the issue
                }
                _ => {
                    println!("❌ Node3 failed to retrieve the contract");
                    println!("   This reproduces the production issue!");
                    return Err(anyhow!("Contract not found - reproducing production issue").into());
                }
            }
        }
        Ok(Ok(other)) => {
            println!("Unexpected response: {:?}", other);
        }
        Ok(Err(e)) => {
            println!("❌ Error during get: {}", e);
            return Err(anyhow!("Get operation failed: {}", e).into());
        }
        Err(_) => {
            println!("❌ Timeout waiting for get response after 30s");
            return Err(anyhow!("Get operation timed out").into());
        }
    }

    // Also test Node2 trying to get
    println!("📥 Node2 attempting to GET the contract...");
    node2
        .client
        .send(ClientRequest::ContractOp(ContractRequest::Get {
            key: contract_key,
            return_contract_code: true,
            subscribe: false,
        }))
        .await?;

    match timeout(Duration::from_secs(30), node2.client.recv()).await {
        Ok(Ok(HostResponse::ContractResponse(response))) => {
            println!("Node2 get response: {:?}", response);
        }
        _ => {
            println!("❌ Node2 also failed to retrieve the contract");
        }
    }

    Ok(())
}
