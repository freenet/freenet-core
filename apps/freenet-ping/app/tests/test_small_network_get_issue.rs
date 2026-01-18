mod common;

use std::{
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    time::Duration,
};

use freenet::{local_node::NodeConfig, server::serve_gateway, test_utils::test_ip_for_node};
use freenet_ping_types::{Ping, PingContractOptions};
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, ContractResponse, HostResponse, WebApi},
    prelude::*,
};
use futures::FutureExt;
use tokio::{select, time::timeout};
use tracing::{span, Instrument, Level};

use common::{
    base_node_test_config_with_ip, connect_async_with_config, gw_config_from_path_with_ip,
    ws_config, APP_TAG, PACKAGE_DIR, PATH_TO_CONTRACT,
};

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_small_network_get_failure() -> anyhow::Result<()> {
    const NUM_GATEWAYS: usize = 1;
    const NUM_NODES: usize = 3;

    println!("🔧 Testing get operation in small, poorly connected network");
    println!(
        "   Simulating production conditions with {} total peers",
        NUM_GATEWAYS + NUM_NODES
    );

    // Use unique IPs for each node
    let gw_ip = test_ip_for_node(0);
    let node1_ip = test_ip_for_node(1);
    let node2_ip = test_ip_for_node(2);

    let network_socket_gw = TcpListener::bind(SocketAddr::new(gw_ip.into(), 0))?;
    let ws_api_port_socket_gw = TcpListener::bind(SocketAddr::new(gw_ip.into(), 0))?;
    let ws_api_port_socket_node1 = TcpListener::bind(SocketAddr::new(node1_ip.into(), 0))?;
    let ws_api_port_socket_node2 = TcpListener::bind(SocketAddr::new(node2_ip.into(), 0))?;
    let (config_gw, preset_cfg_gw) = base_node_test_config_with_ip(
        true,
        vec![],
        Some(network_socket_gw.local_addr()?.port()),
        ws_api_port_socket_gw.local_addr()?.port(),
        "gw_small_network_get",
        None,
        None,
        Some(gw_ip),
    )
    .await?;
    let public_port = config_gw.network_api.public_port.unwrap();
    let path = preset_cfg_gw.temp_dir.path().to_path_buf();
    let config_info = gw_config_from_path_with_ip(public_port, &path, gw_ip)?;
    let serialized_gateway = serde_json::to_string(&config_info)?;
    let ws_api_port_gw = config_gw.ws_api.ws_api_port.unwrap();

    let (config_node1, preset_cfg_node1) = base_node_test_config_with_ip(
        false,
        vec![serialized_gateway.clone()],
        None,
        ws_api_port_socket_node1.local_addr()?.port(),
        "node1_small_network_get",
        None,
        None,
        Some(node1_ip),
    )
    .await?;
    let ws_api_port_node1 = config_node1.ws_api.ws_api_port.unwrap();

    let (config_node2, preset_cfg_node2) = base_node_test_config_with_ip(
        false,
        vec![serialized_gateway],
        None,
        ws_api_port_socket_node2.local_addr()?.port(),
        "node2_small_network_get",
        None,
        None,
        Some(node2_ip),
    )
    .await?;
    let ws_api_port_node2 = config_node2.ws_api.ws_api_port.unwrap();

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

    let gateway_future = async {
        let config = config_gw.build().await?;
        let mut node_config = NodeConfig::new(config.clone()).await?;
        node_config.min_number_of_connections(2);
        node_config.max_number_of_connections(10);
        let node = node_config
            .build(serve_gateway(config.ws_api).await?)
            .await?;
        node.run().await
    }
    .instrument(span!(Level::INFO, "gateway"))
    .boxed_local();

    let node1_future = async {
        let config = config_node1.build().await?;
        let mut node_config = NodeConfig::new(config.clone()).await?;
        node_config.min_number_of_connections(2);
        node_config.max_number_of_connections(10);
        let node = node_config
            .build(serve_gateway(config.ws_api).await?)
            .await?;
        node.run().await
    }
    .instrument(span!(Level::INFO, "node1"))
    .boxed_local();

    let node2_future = async {
        let config = config_node2.build().await?;
        let mut node_config = NodeConfig::new(config.clone()).await?;
        node_config.min_number_of_connections(2);
        node_config.max_number_of_connections(10);
        let node = node_config
            .build(serve_gateway(config.ws_api).await?)
            .await?;
        node.run().await
    }
    .instrument(span!(Level::INFO, "node2"))
    .boxed_local();

    let test = timeout(Duration::from_secs(180), async {
        println!("Waiting for nodes to start up...");
        tokio::time::sleep(Duration::from_secs(15)).await;
        println!("✓ Nodes should be up and have basic connectivity");

        // TODO: The connection maintenance task runs every 60 seconds by default,
        // which is too slow for tests. This causes the first GET operation to take
        // 11+ seconds as connections are established on-demand.
        //
        // Proper fix: Make connection acquisition more aggressive during startup,
        // or make the maintenance interval configurable for tests.

        let uri_gw =
            format!("ws://{gw_ip}:{ws_api_port_gw}/v1/contract/command?encodingProtocol=native");
        let (stream_gw, _) = connect_async_with_config(&uri_gw, Some(ws_config()), false).await?;
        let mut client_gw = WebApi::start(stream_gw);

        let uri_node1 = format!(
            "ws://{node1_ip}:{ws_api_port_node1}/v1/contract/command?encodingProtocol=native"
        );
        let (stream_node1, _) =
            connect_async_with_config(&uri_node1, Some(ws_config()), false).await?;
        let mut client_node1 = WebApi::start(stream_node1);

        let uri_node2 = format!(
            "ws://{node2_ip}:{ws_api_port_node2}/v1/contract/command?encodingProtocol=native"
        );
        let (stream_node2, _) =
            connect_async_with_config(&uri_node2, Some(ws_config()), false).await?;
        let mut client_node2 = WebApi::start(stream_node2);

        let path_to_code = PathBuf::from(PACKAGE_DIR).join(PATH_TO_CONTRACT);
        println!("Loading contract code from {}", path_to_code.display());

        let ping_options = PingContractOptions {
            frequency: Duration::from_secs(2),
            ttl: Duration::from_secs(60),
            tag: APP_TAG.to_string(),
            code_key: "".to_string(),
        };

        let params = Parameters::from(serde_json::to_vec(&ping_options).unwrap());
        let container = common::load_contract(&path_to_code, params)?;
        let contract_key = container.key();

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

        println!("Waiting for put response...");
        match timeout(Duration::from_secs(90), client_node1.recv()).await {
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
                anyhow::bail!("Failed to get put response - error: {}", e);
            }
            Err(_) => {
                println!("⚠️  Timeout waiting for put response after 90s");
                println!("   This may indicate issues with:");
                println!("   - Network connectivity in small network topology");
                println!("   - PUT operation propagation delays");
                println!("   - Gateway stability under timeout conditions");
                anyhow::bail!("PUT operation timed out after 90 seconds");
            }
        }

        tokio::time::sleep(Duration::from_secs(5)).await;

        println!("🔍 Checking which nodes have the contract cached...");

        client_gw
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key: *contract_key.id(),
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

        println!("🔍 Testing second GET from Gateway (should be fast if WASM is the issue)...");
        let gw_get2_start = std::time::Instant::now();
        client_gw
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key: *contract_key.id(),
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

        let get_start = std::time::Instant::now();
        println!("📥 Node2 attempting to GET the contract...");
        println!("   Contract key: {contract_key}");

        client_node2
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key: *contract_key.id(),
                return_contract_code: true,
                subscribe: false,
            }))
            .await?;
        println!(
            "   GET request sent ({}ms after start)",
            get_start.elapsed().as_millis()
        );

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
                            anyhow::bail!("GET returned empty state");
                        }
                    }
                    _ => {
                        println!("❌ Node2 failed to retrieve the contract");
                        println!("   This would have reproduced the production issue!");
                        anyhow::bail!("GET failed");
                    }
                }
            }
            Ok(Ok(_)) => {
                println!("Got unexpected response type");
                anyhow::bail!("Unexpected response type");
            }
            Ok(Err(e)) => {
                println!("❌ Error during get: {e}");
                anyhow::bail!("Get operation failed: {}", e);
            }
            Err(_) => {
                println!("❌ Timeout waiting for get response after 45s");
                anyhow::bail!("Get operation timed out");
            }
        }

        println!("\n🔍 Testing second GET from Node2 after connections are established...");
        tokio::time::sleep(Duration::from_secs(5)).await;

        let get2_start = std::time::Instant::now();
        client_node2
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key: *contract_key.id(),
                return_contract_code: true, // Same as first GET for consistency
                subscribe: false,
            }))
            .await?;

        // Second GET timeout - with rate limiting fix (#2546), this should complete quickly
        match timeout(Duration::from_secs(45), client_node2.recv()).await {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                key,
                state,
                ..
            }))) if key == contract_key => {
                println!(
                    "✅ Second GET completed in {}ms",
                    get2_start.elapsed().as_millis()
                );
                if state.is_empty() {
                    anyhow::bail!("Second GET returned empty state");
                }
            }
            Ok(Ok(resp)) => {
                println!("Got unexpected response: {resp:?}");
                anyhow::bail!("Unexpected response type");
            }
            Ok(Err(e)) => {
                println!("❌ Error during second get: {e}");
                anyhow::bail!("Second GET operation failed: {}", e);
            }
            Err(_) => {
                println!("❌ Timeout waiting for second get response after 45s");
                anyhow::bail!("Second GET operation timed out");
            }
        }

        Ok::<_, anyhow::Error>(())
    })
    .instrument(span!(Level::INFO, "test_small_network_get_failure"));

    select! {
        r = gateway_future => {
            match r {
                Err(e) => anyhow::bail!("Gateway node stopped unexpectedly: {}", e),
                Ok(_) => anyhow::bail!("Gateway node stopped unexpectedly"),
            }
        }
        r = node1_future => {
            match r {
                Err(e) => anyhow::bail!("Node1 stopped unexpectedly: {}", e),
                Ok(_) => anyhow::bail!("Node1 stopped unexpectedly"),
            }
        }
        r = node2_future => {
            match r {
                Err(e) => anyhow::bail!("Node2 stopped unexpectedly: {}", e),
                Ok(_) => anyhow::bail!("Node2 stopped unexpectedly"),
            }
        }
        r = test => {
            match r {
                Err(e) => anyhow::bail!("Test timed out: {}", e),
                Ok(Ok(_)) => {
                    println!("Test completed successfully!");
                    tokio::time::sleep(Duration::from_secs(3)).await;
                },
                Ok(Err(e)) => anyhow::bail!("Test failed: {}", e),
            }
        }
    }

    drop(preset_cfg_gw);
    drop(preset_cfg_node1);
    drop(preset_cfg_node2);

    Ok(())
}
