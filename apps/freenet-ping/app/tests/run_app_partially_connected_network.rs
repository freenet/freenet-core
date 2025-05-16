//! # Partially Connected Network Test
//!
//! This test verifies how subscription propagation works in a partially connected network.
//! Unlike the blocked peers tests (which focus on a simple topology with two nodes
//! blocking each other), this test creates a more complex network with:
//!
//! - Multiple gateway nodes (default: 3)
//! - Multiple regular nodes (default: 7)
//! - Controlled connectivity ratio between regular nodes
//! - A deterministic approach to create partial connectivity
//!
//! The test evaluates propagation success across this complex topology and
//! provides statistical analysis of the results.

mod common;

use std::{net::TcpListener, path::PathBuf, time::Duration};

use anyhow::anyhow;
use freenet::{local_node::NodeConfig, server::serve_gateway};
use freenet_ping_app::ping_client::wait_for_put_response;
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
#[ignore = "fix me"]
async fn test_ping_partially_connected_network() -> TestResult {
    /*
     * This test verifies how subscription propagation works in a partially connected network.
     *
     * Parameters:
     * - NUM_GATEWAYS: Number of gateway nodes to create (default: 3)
     * - NUM_REGULAR_NODES: Number of regular nodes to create (default: 7)
     * - CONNECTIVITY_RATIO: Percentage of connectivity between regular nodes (0.0-1.0)
     *
     * Network Topology:
     * - Creates a network with specified number of gateway and regular nodes
     * - ALL regular nodes connect to ALL gateways (full gateway connectivity)
     * - Regular nodes have partial connectivity to other regular nodes based on CONNECTIVITY_RATIO
     * - Uses a deterministic approach to create partial connectivity between regular nodes
     *
     * Test procedure:
     * 1. Configures and starts all nodes with the specified topology
     * 2. One node publishes the ping contract
     * 3. Tracks which nodes successfully access the contract
     * 4. Subscribes available nodes to the contract
     * 5. Sends an update from one node
     * 6. Verifies update propagation across the network
     * 7. Analyzes results to detect potential subscription propagation issues
     */

    // Network configuration parameters
    const NUM_GATEWAYS: usize = 3;
    const NUM_REGULAR_NODES: usize = 7;
    const CONNECTIVITY_RATIO: f64 = 0.5; // Controls connectivity between regular nodes

    // Configure logging
    freenet::config::set_logger(Some(LevelFilter::DEBUG), None);
    tracing::info!(
        "Starting test with {} gateways and {} regular nodes (connectivity ratio: {})",
        NUM_GATEWAYS,
        NUM_REGULAR_NODES,
        CONNECTIVITY_RATIO
    );

    // Setup network sockets for the gateways
    let mut gateway_sockets = Vec::with_capacity(NUM_GATEWAYS);
    let mut ws_api_gateway_sockets = Vec::with_capacity(NUM_GATEWAYS);

    for _ in 0..NUM_GATEWAYS {
        gateway_sockets.push(TcpListener::bind("127.0.0.1:0")?);
        ws_api_gateway_sockets.push(TcpListener::bind("127.0.0.1:0")?);
    }

    // Setup API sockets for regular nodes
    let mut ws_api_node_sockets = Vec::with_capacity(NUM_REGULAR_NODES);
    let mut regular_node_addresses = Vec::with_capacity(NUM_REGULAR_NODES);

    // First, bind all sockets to get addresses for later blocking
    for _ in 0..NUM_REGULAR_NODES {
        let socket = TcpListener::bind("127.0.0.1:0")?;
        // Store the address for later use in blocked_addresses
        regular_node_addresses.push(socket.local_addr()?);
        ws_api_node_sockets.push(TcpListener::bind("127.0.0.1:0")?);
    }

    // Configure gateway nodes
    let mut gateway_info = Vec::new();
    let mut ws_api_ports_gw = Vec::new();

    // Build configurations and keep temp directories alive
    let mut gateway_configs = Vec::with_capacity(NUM_GATEWAYS);
    let mut gateway_presets = Vec::with_capacity(NUM_GATEWAYS);

    for i in 0..NUM_GATEWAYS {
        let (cfg, preset) = base_node_test_config(
            true,
            vec![],
            Some(gateway_sockets[i].local_addr()?.port()),
            ws_api_gateway_sockets[i].local_addr()?.port(),
            &format!("gw_partial_{}", i), // data_dir_suffix
            None,                         // base_tmp_dir
            None,                         // No blocked addresses for gateways
        )
        .await?;

        let public_port = cfg.network_api.public_port.unwrap();
        let path = preset.temp_dir.path().to_path_buf();
        let config_info = gw_config_from_path(public_port, &path)?;

        tracing::info!("Gateway {} data dir: {:?}", i, preset.temp_dir.path());
        ws_api_ports_gw.push(cfg.ws_api.ws_api_port.unwrap());
        gateway_info.push(config_info);
        gateway_configs.push(cfg);
        gateway_presets.push(preset);
    }

    // Serialize gateway info for nodes to use
    let serialized_gateways: Vec<String> = gateway_info
        .iter()
        .map(|info| serde_json::to_string(info).unwrap())
        .collect();

    // Configure regular nodes with partial connectivity to OTHER regular nodes
    let mut ws_api_ports_nodes = Vec::new();
    let mut node_connections = Vec::new();

    // Build configurations and keep temp directories alive
    let mut node_configs = Vec::with_capacity(NUM_REGULAR_NODES);
    let mut node_presets = Vec::with_capacity(NUM_REGULAR_NODES);

    for (i, listener) in ws_api_node_sockets
        .iter()
        .enumerate()
        .take(NUM_REGULAR_NODES)
    {
        // Determine which other regular nodes this node should block
        let mut blocked_addresses = Vec::new();
        for (j, &addr) in regular_node_addresses.iter().enumerate() {
            if i == j {
                continue; // Skip self
            }

            // Use a deterministic approach based on node indices
            // If the result is >= than CONNECTIVITY_RATIO, block the connection
            let should_block = (i * j) % 100 >= (CONNECTIVITY_RATIO * 100.0) as usize;
            if should_block {
                blocked_addresses.push(addr);
            }
        }

        // Count effective connections to other regular nodes
        let effective_connections = (NUM_REGULAR_NODES - 1) - blocked_addresses.len();
        node_connections.push(effective_connections);

        let (cfg, preset) = base_node_test_config(
            false,
            serialized_gateways.clone(), // All nodes connect to all gateways
            None,                        // Regular nodes pick their own network port or use default
            listener.local_addr()?.port(),
            &format!("node_partial_{}", i),  // data_dir_suffix
            None,                            // base_tmp_dir
            Some(blocked_addresses.clone()), // Use blocked_addresses for regular nodes
        )
        .await?;

        tracing::info!(
            "Node {} data dir: {:?} - Connected to {} other regular nodes (blocked: {})",
            i,
            preset.temp_dir.path(),
            effective_connections,
            blocked_addresses.len()
        );

        ws_api_ports_nodes.push(cfg.ws_api.ws_api_port.unwrap());
        node_configs.push(cfg);
        node_presets.push(preset);
    }

    // Free ports to avoid binding errors
    std::mem::drop(gateway_sockets);
    std::mem::drop(ws_api_gateway_sockets);
    std::mem::drop(ws_api_node_sockets);

    // Start the first gateway node
    let gateway_future = {
        let config = gateway_configs.remove(0);
        async {
            let config = config.build().await?;
            let node = NodeConfig::new(config.clone())
                .await?
                .build(serve_gateway(config.ws_api).await)
                .await?;
            node.run().await
        }
        .boxed_local()
    };

    // Start one regular node
    let regular_node_future = {
        let config = node_configs.remove(0);
        async {
            let config = config.build().await?;
            let node = NodeConfig::new(config.clone())
                .await?
                .build(serve_gateway(config.ws_api).await)
                .await?;
            node.run().await
        }
        .boxed_local()
    };

    let test = tokio::time::timeout(Duration::from_secs(240), async {
        // Wait for nodes to start up
        tokio::time::sleep(Duration::from_secs(15)).await;

        // Connect to all nodes
        let mut gateway_clients = Vec::with_capacity(NUM_GATEWAYS);
        for (i, port) in ws_api_ports_gw.iter().enumerate() {
            let uri = format!(
                "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
                port
            );
            let (stream, _) = connect_async(&uri).await?;
            let client = WebApi::start(stream);
            gateway_clients.push(client);
            tracing::info!("Connected to gateway {}", i);
        }

        let mut node_clients = Vec::with_capacity(NUM_REGULAR_NODES);
        for (i, port) in ws_api_ports_nodes.iter().enumerate() {
            let uri = format!(
                "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
                port
            );
            let (stream, _) = connect_async(&uri).await?;
            let client = WebApi::start(stream);
            node_clients.push(client);
            tracing::info!("Connected to regular node {}", i);
        }

        // Log the node connectivity
        tracing::info!("Node connectivity setup:");
        for (i, num_connections) in node_connections.iter().enumerate() {
            tracing::info!("Node {} is connected to all {} gateways and {} other regular nodes",
                          i, NUM_GATEWAYS, num_connections);
        }

        // Load the ping contract
        let path_to_code = PathBuf::from(PACKAGE_DIR).join(PATH_TO_CONTRACT);
        tracing::info!(path=%path_to_code.display(), "loading contract code");
        let code = std::fs::read(path_to_code)
            .ok()
            .ok_or_else(|| anyhow!("Failed to read contract code"))?;
        let code_hash = CodeHash::from_code(&code);

        // Create ping contract options
        let ping_options = PingContractOptions {
            frequency: Duration::from_secs(3),
            ttl: Duration::from_secs(60),
            tag: APP_TAG.to_string(),
            code_key: code_hash.to_string(),
        };

        let params = Parameters::from(serde_json::to_vec(&ping_options).unwrap());
        let container = ContractContainer::try_from((code, &params))?;
        let contract_key = container.key();

        // Choose a node to publish the contract
        let publisher_idx = 0;
        tracing::info!("Node {} will publish the contract", publisher_idx);

        // Publisher node puts the contract
        let ping = Ping::default();
        let serialized = serde_json::to_vec(&ping)?;
        let wrapped_state = WrappedState::new(serialized);

        let publisher = &mut node_clients[publisher_idx];
        publisher
            .send(ClientRequest::ContractOp(ContractRequest::Put {
                contract: container.clone(),
                state: wrapped_state.clone(),
                related_contracts: RelatedContracts::new(),
                subscribe: false,
            }))
            .await?;

        // Wait for put response on publisher
        let key = timeout(
            Duration::from_secs(30),
            wait_for_put_response(publisher, &contract_key),
        )
        .await
        .map_err(|_| anyhow!("Put request timed out"))?
        .map_err(anyhow::Error::msg)?;

        tracing::info!(key=%key, "Publisher node {} put ping contract successfully!", publisher_idx);

        // All nodes try to get the contract to see which have access
        let mut nodes_with_contract = [false; NUM_REGULAR_NODES];
        let mut get_requests = Vec::with_capacity(NUM_REGULAR_NODES);

        for (i, client) in node_clients.iter_mut().enumerate() {
            client
                .send(ClientRequest::ContractOp(ContractRequest::Get {
                    key: contract_key,
                    return_contract_code: true,
                    subscribe: false,
                }))
                .await?;
            get_requests.push(i);
        }

        // Track gateways with the contract
        let mut gateways_with_contract = [false; NUM_GATEWAYS];
        let mut gw_get_requests = Vec::with_capacity(NUM_GATEWAYS);

        for (i, client) in gateway_clients.iter_mut().enumerate() {
            client
                .send(ClientRequest::ContractOp(ContractRequest::Get {
                    key: contract_key,
                    return_contract_code: true,
                    subscribe: false,
                }))
                .await?;
            gw_get_requests.push(i);
        }

        // Process all get responses with a timeout
        let total_timeout = Duration::from_secs(30);
        let start = std::time::Instant::now();

        while !get_requests.is_empty() || !gw_get_requests.is_empty() {
            if start.elapsed() > total_timeout {
                tracing::warn!("Timeout waiting for get responses, continuing with test");
                break;
            }

            // Check regular nodes
            let mut i = 0;
            while i < get_requests.len() {
                let node_idx = get_requests[i];
                let client = &mut node_clients[node_idx];

                match timeout(Duration::from_millis(500), client.recv()).await {
                    Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse { key, .. }))) => {
                        if key == contract_key {
                            tracing::info!("Node {} successfully got the contract", node_idx);
                            nodes_with_contract[node_idx] = true;
                            get_requests.remove(i);
                            continue;
                        }
                    }
                    Ok(Ok(_)) => {},
                    Ok(Err(e)) => {
                        tracing::warn!("Error receiving from node {}: {}", node_idx, e);
                        get_requests.remove(i);
                        continue;
                    }
                    Err(_) => {}
                }
                i += 1;
            }

            // Check gateways
            let mut i = 0;
            while i < gw_get_requests.len() {
                let gw_idx = gw_get_requests[i];
                let client = &mut gateway_clients[gw_idx];

                match timeout(Duration::from_millis(500), client.recv()).await {
                    Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse { key, .. }))) => {
                        if key == contract_key {
                            tracing::info!("Gateway {} successfully got the contract", gw_idx);
                            gateways_with_contract[gw_idx] = true;
                            gw_get_requests.remove(i);
                            continue;
                        }
                    }
                    Ok(Ok(_)) => {},
                    Ok(Err(e)) => {
                        tracing::warn!("Error receiving from gateway {}: {}", gw_idx, e);
                        gw_get_requests.remove(i);
                        continue;
                    }
                    Err(_) => {}
                }
                i += 1;
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Log initial contract distribution
        tracing::info!("Initial contract distribution:");
        tracing::info!("Gateways with contract: {}/{}",
                      gateways_with_contract.iter().filter(|&&x| x).count(),
                      NUM_GATEWAYS);
        tracing::info!("Regular nodes with contract: {}/{}",
                      nodes_with_contract.iter().filter(|&&x| x).count(),
                      NUM_REGULAR_NODES);

        // All nodes with the contract subscribe to it
        let mut subscribed_nodes = [false; NUM_REGULAR_NODES];
        let mut subscription_requests = Vec::new();

        for (i, has_contract) in nodes_with_contract.iter().enumerate() {
            if *has_contract {
                node_clients[i]
                    .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
                        key: contract_key,
                        summary: None,
                    }))
                    .await?;
                subscription_requests.push(i);
            }
        }

        // Also subscribe gateways
        let mut subscribed_gateways = [false; NUM_GATEWAYS];
        let mut gw_subscription_requests = Vec::new();

        for (i, has_contract) in gateways_with_contract.iter().enumerate() {
            if *has_contract {
                gateway_clients[i]
                    .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
                        key: contract_key,
                        summary: None,
                    }))
                    .await?;
                gw_subscription_requests.push(i);
            }
        }

        // Process subscription responses with a timeout
        let start = std::time::Instant::now();
        let total_timeout = Duration::from_secs(30);

        while !subscription_requests.is_empty() || !gw_subscription_requests.is_empty() {
            if start.elapsed() > total_timeout {
                tracing::warn!("Timeout waiting for subscription responses, continuing with test");
                break;
            }

            // Check regular nodes
            let mut i = 0;
            while i < subscription_requests.len() {
                let node_idx = subscription_requests[i];
                let client = &mut node_clients[node_idx];

                match timeout(Duration::from_millis(500), client.recv()).await {
                    Ok(Ok(HostResponse::ContractResponse(ContractResponse::SubscribeResponse { key, subscribed, .. }))) => {
                        if key == contract_key {
                            tracing::info!("Node {} subscription result: {}", node_idx, subscribed);
                            subscribed_nodes[node_idx] = subscribed;
                            subscription_requests.remove(i);
                            continue;
                        }
                    }
                    Ok(Ok(_)) => {},
                    Ok(Err(e)) => {
                        tracing::warn!("Error receiving from node {}: {}", node_idx, e);
                        subscription_requests.remove(i);
                        continue;
                    }
                    Err(_) => {}
                }
                i += 1;
            }

            // Check gateways
            let mut i = 0;
            while i < gw_subscription_requests.len() {
                let gw_idx = gw_subscription_requests[i];
                let client = &mut gateway_clients[gw_idx];

                match timeout(Duration::from_millis(500), client.recv()).await {
                    Ok(Ok(HostResponse::ContractResponse(ContractResponse::SubscribeResponse { key, subscribed, .. }))) => {
                        if key == contract_key {
                            tracing::info!("Gateway {} subscription result: {}", gw_idx, subscribed);
                            subscribed_gateways[gw_idx] = subscribed;
                            gw_subscription_requests.remove(i);
                            continue;
                        }
                    }
                    Ok(Ok(_)) => {},
                    Ok(Err(e)) => {
                        tracing::warn!("Error receiving from gateway {}: {}", gw_idx, e);
                        gw_subscription_requests.remove(i);
                        continue;
                    }
                    Err(_) => {}
                }
                i += 1;
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Log subscription results
        tracing::info!("Initial subscription results:");
        tracing::info!("Subscribed gateways: {}/{} (with contract: {})",
                      subscribed_gateways.iter().filter(|&&x| x).count(),
                      NUM_GATEWAYS,
                      gateways_with_contract.iter().filter(|&&x| x).count());
        tracing::info!("Subscribed regular nodes: {}/{} (with contract: {})",
                      subscribed_nodes.iter().filter(|&&x| x).count(),
                      NUM_REGULAR_NODES,
                      nodes_with_contract.iter().filter(|&&x| x).count());

        // Choose one subscribed node to send an update
        let updater_indices = subscribed_nodes.iter()
            .enumerate()
            .filter_map(|(i, &subscribed)| if subscribed { Some(i) } else { None })
            .collect::<Vec<_>>();

        if updater_indices.is_empty() {
            return Err(anyhow!("No subscribed nodes to send updates!"));
        }

        let updater_idx = updater_indices[0];
        tracing::info!("Node {} will send an update", updater_idx);

        // Create a unique tag for the updater
        let update_tag = format!("ping-from-node-{}", updater_idx);

        // Send the update
        let mut update_ping = Ping::default();
        update_ping.insert(update_tag.clone());

        tracing::info!("Node {} sending update with tag: {}", updater_idx, update_tag);
        node_clients[updater_idx]
            .send(ClientRequest::ContractOp(ContractRequest::Update {
                key: contract_key,
                data: UpdateData::Delta(StateDelta::from(serde_json::to_vec(&update_ping).unwrap())),
            }))
            .await?;

        // Wait for the update to propagate through the network
        tracing::info!("Waiting for update to propagate...");
        tokio::time::sleep(Duration::from_secs(20)).await;

        // Check which nodes received the update
        let mut nodes_received_update = [false; NUM_REGULAR_NODES];
        let mut get_state_requests = Vec::new();

        for (i, subscribed) in subscribed_nodes.iter().enumerate() {
            if *subscribed {
                node_clients[i]
                    .send(ClientRequest::ContractOp(ContractRequest::Get {
                        key: contract_key,
                        return_contract_code: false,
                        subscribe: false,
                    }))
                    .await?;
                get_state_requests.push(i);
            }
        }

        // Also check gateways
        let mut gateways_received_update = [false; NUM_GATEWAYS];
        let mut gw_get_state_requests = Vec::new();

        for (i, subscribed) in subscribed_gateways.iter().enumerate() {
            if *subscribed {
                gateway_clients[i]
                    .send(ClientRequest::ContractOp(ContractRequest::Get {
                        key: contract_key,
                        return_contract_code: false,
                        subscribe: false,
                    }))
                    .await?;
                gw_get_state_requests.push(i);
            }
        }

        // Process get state responses with a timeout
        let start = std::time::Instant::now();
        let total_timeout = Duration::from_secs(30);

        while !get_state_requests.is_empty() || !gw_get_state_requests.is_empty() {
            if start.elapsed() > total_timeout {
                tracing::warn!("Timeout waiting for get state responses, finalizing test");
                break;
            }

            // Check regular nodes
            let mut i = 0;
            while i < get_state_requests.len() {
                let node_idx = get_state_requests[i];
                let client = &mut node_clients[node_idx];

                match timeout(Duration::from_millis(500), client.recv()).await {
                    Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse { key, state, .. }))) => {
                        if key == contract_key {
                            // Deserialize state and check for the update tag
                            match serde_json::from_slice::<Ping>(&state) {
                                Ok(ping_state) => {
                                    let has_update = ping_state.get(&update_tag).is_some();
                                    tracing::info!("Node {} has update: {}", node_idx, has_update);

                                    if has_update {
                                        let timestamps = ping_state.get(&update_tag).unwrap();
                                        tracing::info!("Node {} has {} timestamps for tag {}",
                                                      node_idx,
                                                      timestamps.len(),
                                                      update_tag);
                                    }

                                    nodes_received_update[node_idx] = has_update;
                                }
                                Err(e) => {
                                    tracing::warn!("Failed to deserialize state from node {}: {}", node_idx, e);
                                }
                            }
                            get_state_requests.remove(i);
                            continue;
                        }
                    }
                    Ok(Ok(_)) => {},
                    Ok(Err(e)) => {
                        tracing::warn!("Error receiving from node {}: {}", node_idx, e);
                        get_state_requests.remove(i);
                        continue;
                    }
                    Err(_) => {}
                }
                i += 1;
            }

            // Check gateways
            let mut i = 0;
            while i < gw_get_state_requests.len() {
                let gw_idx = gw_get_state_requests[i];
                let client = &mut gateway_clients[gw_idx];

                match timeout(Duration::from_millis(500), client.recv()).await {
                    Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse { key, state, .. }))) => {
                        if key == contract_key {
                            // Deserialize state and check for the update tag
                            match serde_json::from_slice::<Ping>(&state) {
                                Ok(ping_state) => {
                                    let has_update = ping_state.get(&update_tag).is_some();
                                    tracing::info!("Gateway {} has update: {}", gw_idx, has_update);

                                    if has_update {
                                        let timestamps = ping_state.get(&update_tag).unwrap();
                                        tracing::info!("Gateway {} has {} timestamps for tag {}",
                                                      gw_idx,
                                                      timestamps.len(),
                                                      update_tag);
                                    }

                                    gateways_received_update[gw_idx] = has_update;
                                }
                                Err(e) => {
                                    tracing::warn!("Failed to deserialize state from gateway {}: {}", gw_idx, e);
                                }
                            }
                            gw_get_state_requests.remove(i);
                            continue;
                        }
                    }
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        tracing::warn!("Error receiving from gateway {}: {}", gw_idx, e);
                        gw_get_state_requests.remove(i);
                        continue;
                    }
                    Err(_) => {}
                }
                i += 1;
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Analyze update propagation results
        tracing::info!("Final update propagation results:");

        // Summary for gateways
        let subscribed_gw_count = subscribed_gateways.iter().filter(|&&x| x).count();
        let updated_gw_count = gateways_received_update.iter().filter(|&&x| x).count();

        tracing::info!("Gateways: {}/{} subscribed received the update ({:.1}%)",
                      updated_gw_count,
                      subscribed_gw_count,
                      if subscribed_gw_count > 0 {
                          (updated_gw_count as f64 / subscribed_gw_count as f64) * 100.0
                      } else {
                          0.0
                      });

        // Summary for regular nodes
        let subscribed_node_count = subscribed_nodes.iter().filter(|&&x| x).count();
        let updated_node_count = nodes_received_update.iter().filter(|&&x| x).count();

        tracing::info!("Regular nodes: {}/{} subscribed received the update ({:.1}%)",
                      updated_node_count,
                      subscribed_node_count,
                      if subscribed_node_count > 0 {
                          (updated_node_count as f64 / subscribed_node_count as f64) * 100.0
                      } else {
                          0.0
                      });

        // Check nodes that didn't receive updates
        for (node_idx, (subscribed, updated)) in subscribed_nodes.iter().zip(nodes_received_update.iter()).enumerate() {
            if *subscribed && !updated {
                tracing::warn!("Node {} was subscribed but did not receive the update!", node_idx);

                // Get the node connectivity info
                let connections = node_connections[node_idx];
                tracing::warn!("Node {} is connected to {} other regular nodes", node_idx, connections);
            }
        }

        // Verify that updates have propagated to at least some nodes
        assert!(
            updated_node_count > 0,
            "No nodes received the update, subscription propagation failed"
        );

        // Verify that if we have multiple gateways, at least some received the update
        if NUM_GATEWAYS > 1 && subscribed_gw_count > 1 {
            assert!(
                updated_gw_count > 0,
                "No gateways received the update, gateway subscription propagation failed"
            );
        }

        // Calculate and assert a minimum expected update propagation rate
        let min_expected_rate = 0.5; // At least 50% of subscribed nodes should get updates

        let actual_rate = if subscribed_node_count > 0 {
            updated_node_count as f64 / subscribed_node_count as f64
        } else {
            0.0
        };

        assert!(
            actual_rate >= min_expected_rate,
            "Update propagation rate too low: {:.1}% (expected at least {:.1}%)",
            actual_rate * 100.0,
            min_expected_rate * 100.0
        );
        tracing::info!("Subscription propagation test completed successfully!");
        Ok::<_, anyhow::Error>(())
    })
    .instrument(span!(Level::INFO, "test_ping_partially_connected_network"));

    // Wait for test completion or node failures
    select! {
        r = gateway_future => {
            let Err(err) = r;
            return Err(anyhow!("Gateway node failed: {}", err).into());
        }
        r = regular_node_future => {
            let Err(err) = r;
            return Err(anyhow!("Regular node failed: {}", err).into());
        }
        r = test => {
            r??;
        }
    }

    // Keep presets alive until here
    tracing::debug!(
        "Test complete, dropping {} gateway presets and {} node presets",
        gateway_presets.len(),
        node_presets.len()
    );

    Ok(())
}
