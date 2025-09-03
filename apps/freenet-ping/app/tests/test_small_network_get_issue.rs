/// Test to reproduce the get operation failure in small, poorly connected networks
/// This simulates the real network conditions where contracts can't be retrieved
mod common;

use std::{net::TcpListener, path::PathBuf, time::Duration};

use anyhow::anyhow;
use freenet::{local_node::NodeConfig, server::serve_gateway};
use freenet_ping_types::{Ping, PingContractOptions};
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, ContractResponse, HostResponse, WebApi},
    prelude::*,
};
use futures::FutureExt;
use testresult::TestResult;
use tokio::{select, time::timeout};
use tokio_tungstenite::connect_async;
use tracing::{level_filters::LevelFilter, span, Instrument, Level};

use common::{base_node_test_config, gw_config_from_path, APP_TAG, PACKAGE_DIR, PATH_TO_CONTRACT};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "Test has reliability issues in CI - PUT operations timeout and gateway crashes"]
async fn test_small_network_get_failure() -> TestResult {
    freenet::config::set_logger(Some(LevelFilter::DEBUG), None);

    /*
     * This test simulates the real network issue where:
     * 1. A small number of peers (matching production)
     * 2. Poor connectivity between peers
     * 3. Gateway publishes a contract (central topology position)
     * 4. Node2 tries to GET the contract through the gateway
     *
     * Without backtracking, the GET would fail.
     * With backtracking, it should succeed.
     */

    // Small network like production
    const NUM_GATEWAYS: usize = 1;
    const NUM_NODES: usize = 3; // Total 4 peers like in production

    println!("🔧 Testing get operation in small, poorly connected network");
    println!(
        "   Simulating production conditions with {} total peers",
        NUM_GATEWAYS + NUM_NODES
    );

    // Setup network sockets for the gateway
    let network_socket_gw = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_gw = TcpListener::bind("127.0.0.1:0")?;

    // Setup API sockets for nodes
    let ws_api_port_socket_node1 = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_node2 = TcpListener::bind("127.0.0.1:0")?;

    // Configure gateway node
    let (config_gw, preset_cfg_gw) = base_node_test_config(
        true,
        vec![],
        Some(network_socket_gw.local_addr()?.port()),
        ws_api_port_socket_gw.local_addr()?.port(),
        "gw_small_network_get",
        None,
        None,
    )
    .await?;
    let public_port = config_gw.network_api.public_port.unwrap();
    let path = preset_cfg_gw.temp_dir.path().to_path_buf();
    let config_info = gw_config_from_path(public_port, &path)?;
    let serialized_gateway = serde_json::to_string(&config_info)?;
    let _ws_api_port_gw = config_gw.ws_api.ws_api_port.unwrap();

    // Configure Node 1 (connects to gateway)
    let (config_node1, preset_cfg_node1) = base_node_test_config(
        false,
        vec![serialized_gateway.clone()],
        None,
        ws_api_port_socket_node1.local_addr()?.port(),
        "node1_small_network_get",
        None,
        None,
    )
    .await?;
    let ws_api_port_node1 = config_node1.ws_api.ws_api_port.unwrap();

    // Configure Node 2 (connects to gateway)
    let (config_node2, preset_cfg_node2) = base_node_test_config(
        false,
        vec![serialized_gateway],
        None,
        ws_api_port_socket_node2.local_addr()?.port(),
        "node2_small_network_get",
        None,
        None,
    )
    .await?;
    let ws_api_port_node2 = config_node2.ws_api.ws_api_port.unwrap();

    // Free the sockets
    std::mem::drop(network_socket_gw);
    std::mem::drop(ws_api_port_socket_gw);
    std::mem::drop(ws_api_port_socket_node1);
    std::mem::drop(ws_api_port_socket_node2);

    println!("Network topology:");
    println!("  Gateway");
    println!("    ├── Node1");
    println!("    └── Node2");
    println!();
    println!("Note: Nodes are only connected through the gateway");

    // Start all nodes
    let gateway_future = async {
        let config = config_gw.build().await?;
        let mut node_config = NodeConfig::new(config.clone()).await?;
        // Set reasonable connection limits for small test network
        node_config.min_number_of_connections(2);
        node_config.max_number_of_connections(10);
        let node = node_config
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .instrument(span!(Level::INFO, "gateway"))
    .boxed_local();

    let node1_future = async {
        let config = config_node1.build().await?;
        let mut node_config = NodeConfig::new(config.clone()).await?;
        // Set reasonable connection limits for small test network
        node_config.min_number_of_connections(2);
        node_config.max_number_of_connections(10);
        let node = node_config
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .instrument(span!(Level::INFO, "node1"))
    .boxed_local();

    let node2_future = async {
        let config = config_node2.build().await?;
        let mut node_config = NodeConfig::new(config.clone()).await?;
        // Set reasonable connection limits for small test network
        node_config.min_number_of_connections(2);
        node_config.max_number_of_connections(10);
        let node = node_config
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .instrument(span!(Level::INFO, "node2"))
    .boxed_local();

    let test = timeout(Duration::from_secs(120), async {
        // Wait for nodes to start up
        println!("Waiting for nodes to start up...");
        tokio::time::sleep(Duration::from_secs(15)).await;
        println!("✓ Nodes should be up and have basic connectivity");

        // TODO: The connection maintenance task runs every 60 seconds by default,
        // which is too slow for tests. This causes the first GET operation to take
        // 11+ seconds as connections are established on-demand.
        //
        // Proper fix: Make connection acquisition more aggressive during startup,
        // or make the maintenance interval configurable for tests.

        // Connect to gateway first
        let uri_gw =
            format!("ws://127.0.0.1:{_ws_api_port_gw}/v1/contract/command?encodingProtocol=native");
        let (stream_gw, _) = connect_async(&uri_gw).await?;
        let mut client_gw = WebApi::start(stream_gw);

        // Connect to nodes
        let uri_node1 = format!(
            "ws://127.0.0.1:{ws_api_port_node1}/v1/contract/command?encodingProtocol=native"
        );
        let (stream_node1, _) = connect_async(&uri_node1).await?;
        let mut client_node1 = WebApi::start(stream_node1);

        let uri_node2 = format!(
            "ws://127.0.0.1:{ws_api_port_node2}/v1/contract/command?encodingProtocol=native"
        );
        let (stream_node2, _) = connect_async(&uri_node2).await?;
        let mut client_node2 = WebApi::start(stream_node2);

        // Load the ping contract
        let path_to_code = PathBuf::from(PACKAGE_DIR).join(PATH_TO_CONTRACT);
        println!("Loading contract code from {}", path_to_code.display());

        // Create ping contract options
        let ping_options = PingContractOptions {
            frequency: Duration::from_secs(2),
            ttl: Duration::from_secs(60),
            tag: APP_TAG.to_string(),
            code_key: "".to_string(),
        };

        let params = Parameters::from(serde_json::to_vec(&ping_options).unwrap());
        let container = common::load_contract(&path_to_code, params)?;
        let contract_key = container.key();

        // Node1 publishes the contract (more typical scenario)
        println!("📤 Node1 publishing ping contract...");

        let ping = Ping::default();
        let serialized = serde_json::to_vec(&ping)?;
        let wrapped_state = WrappedState::new(serialized);

        client_node1
            .send(ClientRequest::ContractOp(ContractRequest::Put {
                contract: container.clone(),
                state: wrapped_state.clone(),
                related_contracts: RelatedContracts::new(),
                subscribe: false,
            }))
            .await?;

        // Wait for put response
        println!("Waiting for put response...");
        match timeout(Duration::from_secs(30), client_node1.recv()).await {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key })))
                if key == contract_key =>
            {
                println!("✅ Node1 successfully published contract");
            }
            Ok(Ok(resp)) => {
                println!("Got unexpected response: {resp:?}");
                // Continue anyway - maybe the contract is already there
            }
            Ok(Err(e)) => {
                println!("Error receiving put response: {e:?}");
                return Err(anyhow!("Failed to get put response - error: {}", e));
            }
            Err(_) => {
                println!("Timeout waiting for put response");
                // Continue anyway - maybe the contract is already there
            }
        }

        // Give time for any propagation
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Debug: Check if contract is cached on each node
        println!("🔍 Checking which nodes have the contract cached...");

        // Try GET from Gateway
        client_gw
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key: contract_key,
                return_contract_code: false,
                subscribe: false,
            }))
            .await?;

        match timeout(Duration::from_secs(2), client_gw.recv()).await {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse { .. }))) => {
                println!("   ✓ Gateway has the contract cached");
            }
            _ => {
                println!("   ✗ Gateway does NOT have the contract cached");
            }
        }

        // First, let's do a second GET from Gateway to see if it's faster
        println!("🔍 Testing second GET from Gateway (should be fast if WASM is the issue)...");
        let gw_get2_start = std::time::Instant::now();
        client_gw
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key: contract_key,
                return_contract_code: true,
                subscribe: false,
            }))
            .await?;

        match timeout(Duration::from_secs(5), client_gw.recv()).await {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse { .. }))) => {
                println!(
                    "   ✓ Gateway second GET took {}ms",
                    gw_get2_start.elapsed().as_millis()
                );
            }
            _ => {
                println!("   ✗ Gateway second GET failed/timed out");
            }
        }

        // Now Node2 tries to GET the contract
        let get_start = std::time::Instant::now();
        println!("📥 Node2 attempting to GET the contract...");
        println!("   Contract key: {contract_key}");

        client_node2
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key: contract_key,
                return_contract_code: true,
                subscribe: false,
            }))
            .await?;
        println!(
            "   GET request sent ({}ms after start)",
            get_start.elapsed().as_millis()
        );

        // Wait for get response with longer timeout
        let start = std::time::Instant::now();
        match timeout(Duration::from_secs(45), client_node2.recv()).await {
            Ok(Ok(HostResponse::ContractResponse(response))) => {
                println!("Get response after {:?}: {:?}", start.elapsed(), response);
                match response {
                    ContractResponse::GetResponse { key, state, .. } if key == contract_key => {
                        if !state.is_empty() {
                            println!("✅ Node2 successfully retrieved the contract!");
                            println!("   The backtracking implementation is working!");
                        } else {
                            println!("❌ Node2 failed to retrieve the contract (empty state)");
                            println!("   This would have reproduced the production issue!");
                            return Err(anyhow!("GET returned empty state"));
                        }
                    }
                    _ => {
                        println!("❌ Node2 failed to retrieve the contract");
                        println!("   This would have reproduced the production issue!");
                        return Err(anyhow!("GET failed"));
                    }
                }
            }
            Ok(Ok(_)) => {
                println!("Got unexpected response type");
                return Err(anyhow!("Unexpected response type"));
            }
            Ok(Err(e)) => {
                println!("❌ Error during get: {e}");
                return Err(anyhow!("Get operation failed: {}", e));
            }
            Err(_) => {
                println!("❌ Timeout waiting for get response after 45s");
                return Err(anyhow!("Get operation timed out"));
            }
        }

        // Test second GET after connections are established
        println!("\n🔍 Testing second GET from Node2 after connections are established...");
        tokio::time::sleep(Duration::from_secs(5)).await;

        let get2_start = std::time::Instant::now();
        client_node2
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key: contract_key,
                return_contract_code: false,
                subscribe: false,
            }))
            .await?;
        println!("   Second GET request sent");

        match timeout(Duration::from_secs(10), client_node2.recv()).await {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                key,
                state,
                ..
            }))) if key == contract_key => {
                println!(
                    "✅ Second GET completed in {}ms (vs 13s for first GET!)",
                    get2_start.elapsed().as_millis()
                );
                if state.is_empty() {
                    return Err(anyhow!("Second GET returned empty state"));
                }
            }
            Ok(Ok(resp)) => {
                println!("Got unexpected response: {resp:?}");
                return Err(anyhow!("Unexpected response type"));
            }
            Ok(Err(e)) => {
                println!("❌ Error during second get: {e}");
                return Err(anyhow!("Second GET operation failed: {}", e));
            }
            Err(_) => {
                println!("❌ Timeout waiting for second get response after 10s");
                return Err(anyhow!("Second GET operation timed out"));
            }
        }

        Ok::<_, anyhow::Error>(())
    })
    .instrument(span!(Level::INFO, "test_small_network_get_failure"));

    // Wait for test completion or node failures
    select! {
        r = gateway_future => {
            match r {
                Err(e) => return Err(anyhow!("Gateway node stopped unexpectedly: {}", e).into()),
                Ok(_) => return Err(anyhow!("Gateway node stopped unexpectedly").into()),
            }
        }
        r = node1_future => {
            match r {
                Err(e) => return Err(anyhow!("Node1 stopped unexpectedly: {}", e).into()),
                Ok(_) => return Err(anyhow!("Node1 stopped unexpectedly").into()),
            }
        }
        r = node2_future => {
            match r {
                Err(e) => return Err(anyhow!("Node2 stopped unexpectedly: {}", e).into()),
                Ok(_) => return Err(anyhow!("Node2 stopped unexpectedly").into()),
            }
        }
        r = test => {
            match r {
                Err(e) => return Err(anyhow!("Test timed out: {}", e).into()),
                Ok(Ok(_)) => {
                    println!("Test completed successfully!");
                    // Give nodes time to process remaining operations before shutdown
                    tokio::time::sleep(Duration::from_secs(3)).await;
                },
                Ok(Err(e)) => return Err(anyhow!("Test failed: {}", e).into()),
            }
        }
    }

    // Clean up
    drop(preset_cfg_gw);
    drop(preset_cfg_node1);
    drop(preset_cfg_node2);

    Ok(())
}
