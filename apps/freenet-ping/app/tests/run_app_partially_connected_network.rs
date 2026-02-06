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

use std::{
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    time::Duration,
};

use anyhow::anyhow;
use freenet::{local_node::NodeConfig, server::serve_gateway, test_utils::test_ip_for_node};
use freenet_ping_app::ping_client::wait_for_put_response;
use freenet_ping_types::{Ping, PingContractOptions};
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, ContractResponse, HostResponse},
    prelude::*,
};
use futures::FutureExt;
use tokio::{select, time::timeout};
use tracing::{span, Instrument, Level};

use common::{
    base_node_test_config_with_ip, connect_ws_with_retry, gw_config_from_path_with_ip,
    wait_for_node_connected, APP_TAG, PACKAGE_DIR, PATH_TO_CONTRACT,
};

/// Test for subscription propagation in a partially connected network.
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_ping_partially_connected_network() -> anyhow::Result<()> {
    /*
     * This test verifies how subscription propagation works in a partially connected network.
     *
     * Parameters:
     * - NUM_GATEWAYS: Number of gateway nodes to create (default: 1)
     * - NUM_REGULAR_NODES: Number of regular nodes to create (default: 7)
     * - CONNECTIVITY_RATIO: Percentage of connectivity between regular nodes (0.0-1.0)
     *
     * Network Topology:
     * - Creates a network with specified number of gateway and regular nodes
     * - ALL regular nodes connect to ALL gateways (full gateway connectivity)
     * - Regular nodes have partial connectivity to other regular nodes based on CONNECTIVITY_RATIO
     * - Uses a deterministic approach to create partial connectivity between regular nodes
     */

    std::env::set_var("FREENET_RUNTIME_POOL_SIZE", "2");

    // Network configuration parameters
    const NUM_GATEWAYS: usize = 1;
    const NUM_REGULAR_NODES: usize = 7;
    const CONNECTIVITY_RATIO: f64 = 0.3;

    println!(
        "Starting test with {} gateways and {} regular nodes (connectivity ratio: {})",
        NUM_GATEWAYS, NUM_REGULAR_NODES, CONNECTIVITY_RATIO
    );

    // Use varied loopback IPs for unique ring locations
    // This is essential because ring locations are derived from IP addresses.
    // Gateways use indices 0..NUM_GATEWAYS, nodes use NUM_GATEWAYS..NUM_GATEWAYS+NUM_REGULAR_NODES
    let gateway_ips: Vec<_> = (0..NUM_GATEWAYS).map(test_ip_for_node).collect();
    let node_ips: Vec<_> = (0..NUM_REGULAR_NODES)
        .map(|i| test_ip_for_node(NUM_GATEWAYS + i))
        .collect();

    // Reserve network ports for gateways on their varied IPs
    let mut gateway_network_sockets = Vec::with_capacity(NUM_GATEWAYS);
    let mut gateway_network_ports = Vec::with_capacity(NUM_GATEWAYS);
    let mut ws_api_gateway_sockets = Vec::with_capacity(NUM_GATEWAYS);

    for (i, &ip) in gateway_ips.iter().enumerate() {
        let network_socket = TcpListener::bind(SocketAddr::new(ip.into(), 0))?;
        let port = network_socket.local_addr()?.port();
        gateway_network_ports.push(port);
        gateway_network_sockets.push(network_socket);
        // WebSocket always uses localhost
        ws_api_gateway_sockets.push(TcpListener::bind("127.0.0.1:0")?);
        println!("Gateway {} will use {}:{}", i, ip, port);
    }

    // Reserve network ports for regular nodes on their varied IPs
    // Store the actual network addresses for blocking
    let mut node_network_sockets = Vec::with_capacity(NUM_REGULAR_NODES);
    let mut node_network_ports = Vec::with_capacity(NUM_REGULAR_NODES);
    let mut node_network_addrs = Vec::with_capacity(NUM_REGULAR_NODES);
    let mut ws_api_node_sockets = Vec::with_capacity(NUM_REGULAR_NODES);

    for (i, &ip) in node_ips.iter().enumerate() {
        let network_socket = TcpListener::bind(SocketAddr::new(ip.into(), 0))?;
        let port = network_socket.local_addr()?.port();
        let addr = SocketAddr::new(ip.into(), port);
        node_network_ports.push(port);
        node_network_addrs.push(addr);
        node_network_sockets.push(network_socket);
        // WebSocket always uses localhost
        ws_api_node_sockets.push(TcpListener::bind("127.0.0.1:0")?);
        println!("Node {} will use {} ({})", i, addr, ip);
    }

    // Configure gateway nodes
    let mut gateway_info = Vec::new();
    let mut ws_api_ports_gw = Vec::new();

    // Build configurations and keep temp directories alive
    let mut gateway_configs = Vec::with_capacity(NUM_GATEWAYS);
    let mut gateway_presets = Vec::with_capacity(NUM_GATEWAYS);

    let dir_suffix = "partial";

    for i in 0..NUM_GATEWAYS {
        let gw_ip = gateway_ips[i];
        let (cfg, preset) = base_node_test_config_with_ip(
            true,
            vec![],
            Some(gateway_network_ports[i]),
            ws_api_gateway_sockets[i].local_addr()?.port(),
            &format!("gw_{}_{i}", dir_suffix),
            None,
            None,        // No blocked addresses for gateways
            Some(gw_ip), // Use varied IP for unique ring location
        )
        .await?;

        let public_port = cfg.network_api.public_port.unwrap();
        let path = preset.temp_dir.path().to_path_buf();
        let config_info = gw_config_from_path_with_ip(public_port, &path, gw_ip)?;

        println!(
            "Gateway {} data dir: {:?} (IP: {})",
            i,
            preset.temp_dir.path(),
            gw_ip
        );
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

    for i in 0..NUM_REGULAR_NODES {
        let node_ip = node_ips[i];

        // Determine which other regular nodes this node should block
        // Use the ACTUAL network addresses we reserved earlier
        let mut blocked_addresses = Vec::new();
        for (j, &addr) in node_network_addrs.iter().enumerate() {
            if i == j {
                continue; // Skip self
            }

            // Use a deterministic hash-based approach to achieve target connectivity ratio
            // Create a deterministic value from the ordered pair (min(i,j), max(i,j))
            let (a, b) = if i < j { (i, j) } else { (j, i) };
            let hash_value = (a * 17 + b * 31 + a * b * 7) % 100;
            let should_block = hash_value >= (CONNECTIVITY_RATIO * 100.0) as usize;
            if should_block {
                blocked_addresses.push(addr);
            }
        }

        // Count effective connections to other regular nodes
        let effective_connections = (NUM_REGULAR_NODES - 1) - blocked_addresses.len();
        node_connections.push(effective_connections);

        let (cfg, preset) = base_node_test_config_with_ip(
            false,
            serialized_gateways.clone(),
            Some(node_network_ports[i]), // Use reserved network port
            ws_api_node_sockets[i].local_addr()?.port(),
            &format!("node_{}_{i}", dir_suffix),
            None,
            Some(blocked_addresses.clone()),
            Some(node_ip), // Use varied IP for unique ring location
        )
        .await?;

        println!(
            "Node {} data dir: {:?} (IP: {}) - Connected to {} other nodes (blocked: {})",
            i,
            preset.temp_dir.path(),
            node_ip,
            effective_connections,
            blocked_addresses.len()
        );

        ws_api_ports_nodes.push(cfg.ws_api.ws_api_port.unwrap());
        node_configs.push(cfg);
        node_presets.push(preset);
    }

    // Free ports to avoid binding errors
    std::mem::drop(gateway_network_sockets);
    std::mem::drop(ws_api_gateway_sockets);
    std::mem::drop(node_network_sockets);
    std::mem::drop(ws_api_node_sockets);

    // Start all gateway nodes
    let mut gateway_futures = Vec::with_capacity(NUM_GATEWAYS);
    for _i in 0..NUM_GATEWAYS {
        let config = gateway_configs.remove(0);
        let gateway_future = async move {
            let config = config.build().await?;
            let node = NodeConfig::new(config.clone()).await?;
            let gateway_service = serve_gateway(config.ws_api).await?;
            let node = node.build(gateway_service).await?;
            node.run().await
        }
        .boxed_local();
        gateway_futures.push(gateway_future);
    }

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Start all regular nodes
    let mut regular_node_futures = Vec::with_capacity(NUM_REGULAR_NODES);
    for _i in 0..NUM_REGULAR_NODES {
        let config = node_configs.remove(0);
        let regular_node_future = async move {
            let config = config.build().await?;
            let node = NodeConfig::new(config.clone()).await?;
            let gateway_service = serve_gateway(config.ws_api).await?;
            let node = node.build(gateway_service).await?;
            node.run().await
        }
        .boxed_local();
        regular_node_futures.push(regular_node_future);
    }

    let test = tokio::time::timeout(Duration::from_secs(300), async {
        // Small initial wait for nodes to start binding ports
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Connect to all nodes with retry logic
        // Use the correct IPs for each node (WebSocket servers bind to node IPs, not 127.0.0.1)
        let mut gateway_clients = Vec::with_capacity(NUM_GATEWAYS);
        for (i, port) in ws_api_ports_gw.iter().enumerate() {
            let ip = gateway_ips[i];
            let uri = format!(
                "ws://{ip}:{port}/v1/contract/command?encodingProtocol=native"
            );
            let client = connect_ws_with_retry(&uri, &format!("Gateway{i}"), 60).await?;
            gateway_clients.push(client);
            println!("Connected to gateway {i}");
        }

        let mut node_clients = Vec::with_capacity(NUM_REGULAR_NODES);
        for (i, port) in ws_api_ports_nodes.iter().enumerate() {
            let ip = node_ips[i];
            let uri = format!(
                "ws://{ip}:{port}/v1/contract/command?encodingProtocol=native"
            );
            let client = connect_ws_with_retry(&uri, &format!("Node{i}"), 60).await?;
            node_clients.push(client);
            println!("Connected to regular node {i}");
        }

        // Wait for nodes to connect to the network
        println!("Waiting for nodes to connect to the network...");
        for (i, client) in node_clients.iter_mut().enumerate() {
            wait_for_node_connected(client, &format!("Node{i}"), 1, 120).await?;
        }
        println!("All nodes connected to the network!");

        // Log the node connectivity
        println!("Node connectivity setup:");
        for (i, num_connections) in node_connections.iter().enumerate() {
            println!(
                "Node {i} is connected to all {NUM_GATEWAYS} gateways and {num_connections} other regular nodes"
            );
        }

        // Load the ping contract using load_contract which compiles it at test execution time
        let path_to_code = PathBuf::from(PACKAGE_DIR).join(PATH_TO_CONTRACT);
        println!("loading contract code from {}", path_to_code.display());

        // Create ping contract options
        let ping_options = PingContractOptions {
            frequency: Duration::from_secs(2),
            ttl: Duration::from_secs(60),
            tag: APP_TAG.to_string(),
            code_key: "".to_string(), // Will be set by load_contract
        };

        let params = Parameters::from(serde_json::to_vec(&ping_options).unwrap());
        let container = common::load_contract(&path_to_code, params)?;
        let contract_key = container.key();

        // Choose a node to publish the contract
        let publisher_idx = 0;
        println!("Node {publisher_idx} will publish the contract");

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
                blocking_subscribe: false,
            }))
            .await?;

        // Wait for put response on publisher
        let _key = timeout(
            Duration::from_secs(30),
            wait_for_put_response(publisher, &contract_key),
        )
        .await
        .map_err(|_| anyhow!("Put request timed out"))?
        .map_err(anyhow::Error::msg)?;

        println!(
            "Publisher node {publisher_idx} put ping contract successfully!"
        );
        // wait for put propagation
        tokio::time::sleep(Duration::from_secs(5)).await;

        // All nodes try to get the contract to see which have access
        let mut nodes_with_contract = [false; NUM_REGULAR_NODES];
        let mut get_requests = Vec::with_capacity(NUM_REGULAR_NODES);

        for (i, client) in node_clients.iter_mut().enumerate() {
            client
                .send(ClientRequest::ContractOp(ContractRequest::Get {
                    key: *contract_key.id(),
                    return_contract_code: true,
                    subscribe: false,
                    blocking_subscribe: false,
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
                    key: *contract_key.id(),
                    return_contract_code: true,
                    subscribe: false,
                    blocking_subscribe: false,
                }))
                .await?;
            gw_get_requests.push(i);
        }

        // Process all get responses with a timeout
        let total_timeout = Duration::from_secs(30);
        let start = std::time::Instant::now();

        while !get_requests.is_empty() || !gw_get_requests.is_empty() {
            if start.elapsed() > total_timeout {
                println!("Timeout waiting for get responses, continuing with test");
                break;
            }

            // Check regular nodes
            let mut i = 0;
            while i < get_requests.len() {
                let node_idx = get_requests[i];
                let client = &mut node_clients[node_idx];

                match timeout(Duration::from_millis(500), client.recv()).await {
                    Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                        key,
                        ..
                    }))) => {
                        if key == contract_key {
                            println!("Node {node_idx} successfully got the contract");
                            nodes_with_contract[node_idx] = true;
                            get_requests.remove(i);
                            continue;
                        }
                    }
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        println!("Error receiving from node {node_idx}: {e}");
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
                    Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                        key,
                        ..
                    }))) => {
                        if key == contract_key {
                            println!("Gateway {gw_idx} successfully got the contract");
                            gateways_with_contract[gw_idx] = true;
                            gw_get_requests.remove(i);
                            continue;
                        }
                    }
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        println!("Error receiving from gateway {gw_idx}: {e}");
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
        println!("Initial contract distribution:");
        println!(
            "Gateways with contract: {}/{}",
            gateways_with_contract.iter().filter(|&&x| x).count(),
            NUM_GATEWAYS
        );
        println!(
            "Regular nodes with contract: {}/{}",
            nodes_with_contract.iter().filter(|&&x| x).count(),
            NUM_REGULAR_NODES
        );

        // For nodes that still don't have the contract, make additional attempts
        let mut final_get_requests = Vec::new();
        for (i, has_contract) in nodes_with_contract.iter().enumerate() {
            if !has_contract {
                println!(
                    "Node {i} still doesn't have contract, making final attempt to get it"
                );
                node_clients[i]
                    .send(ClientRequest::ContractOp(ContractRequest::Get {
                        key: *contract_key.id(),
                        return_contract_code: true,
                        subscribe: false,
                        blocking_subscribe: false,
                    }))
                    .await?;
                final_get_requests.push(i);
            }
        }

        // Process final get responses with a timeout
        let start = std::time::Instant::now();
        let total_timeout = Duration::from_secs(30);

        while !final_get_requests.is_empty() {
            if start.elapsed() > total_timeout {
                println!("Timeout waiting for final get responses, continuing with subscriptions");
                break;
            }

            let mut i = 0;
            while i < final_get_requests.len() {
                let node_idx = final_get_requests[i];
                let client = &mut node_clients[node_idx];

                match timeout(Duration::from_millis(500), client.recv()).await {
                    Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                        key,
                        ..
                    }))) => {
                        if key == contract_key {
                            println!(
                                "Node {node_idx} successfully got the contract on final attempt"
                            );
                            nodes_with_contract[node_idx] = true;
                            final_get_requests.remove(i);
                            continue;
                        }
                    }
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        println!(
                            "Error receiving from node {node_idx} on final get attempt: {e}"
                        );
                        final_get_requests.remove(i);
                        continue;
                    }
                    Err(_) => {}
                }
                i += 1;
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Log final contract distribution
        println!("Final contract distribution after all get attempts:");
        println!(
            "Regular nodes with contract: {}/{}",
            nodes_with_contract.iter().filter(|&&x| x).count(),
            NUM_REGULAR_NODES
        );

        // ALL nodes with the contract subscribe to it (including those that got it in any attempt)
        let mut subscribed_nodes = [false; NUM_REGULAR_NODES];
        let mut subscription_requests = Vec::new();

        for (i, has_contract) in nodes_with_contract.iter().enumerate() {
            if *has_contract {
                println!("Node {i} has contract, subscribing to it");
                node_clients[i]
                    .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
                        key: *contract_key.id(),
                        summary: None,
                    }))
                    .await?;
                subscription_requests.push(i);
            } else {
                println!("Node {i} does not have contract, cannot subscribe");
            }
        }

        // Also subscribe gateways that have the contract
        let mut subscribed_gateways = [false; NUM_GATEWAYS];
        let mut gw_subscription_requests = Vec::new();

        for (i, has_contract) in gateways_with_contract.iter().enumerate() {
            if *has_contract {
                println!("Gateway {i} has contract, subscribing to it");
                gateway_clients[i]
                    .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
                        key: *contract_key.id(),
                        summary: None,
                    }))
                    .await?;
                gw_subscription_requests.push(i);
            } else {
                println!("Gateway {i} does not have contract, cannot subscribe");
            }
        }

        // Process subscription responses with a timeout
        let start = std::time::Instant::now();
        let total_timeout = Duration::from_secs(30);

        while !subscription_requests.is_empty() || !gw_subscription_requests.is_empty() {
            if start.elapsed() > total_timeout {
                println!("Timeout waiting for subscription responses, continuing with test");
                break;
            }

            // Check regular nodes
            let mut i = 0;
            while i < subscription_requests.len() {
                let node_idx = subscription_requests[i];
                let client = &mut node_clients[node_idx];

                match timeout(Duration::from_millis(500), client.recv()).await {
                    Ok(Ok(HostResponse::ContractResponse(
                        ContractResponse::SubscribeResponse {
                            key, subscribed, ..
                        },
                    ))) => {
                        if key == contract_key {
                            println!("Node {node_idx} subscription result: {subscribed}");
                            subscribed_nodes[node_idx] = subscribed;
                            subscription_requests.remove(i);
                            continue;
                        }
                    }
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        println!("Error receiving from node {node_idx}: {e}");
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
                    Ok(Ok(HostResponse::ContractResponse(
                        ContractResponse::SubscribeResponse {
                            key, subscribed, ..
                        },
                    ))) => {
                        if key == contract_key {
                            println!("Gateway {gw_idx} subscription result: {subscribed}");
                            subscribed_gateways[gw_idx] = subscribed;
                            gw_subscription_requests.remove(i);
                            continue;
                        }
                    }
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        println!("Error receiving from gateway {gw_idx}: {e}");
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
        println!("Initial subscription results:");
        println!(
            "Subscribed gateways: {}/{} (with contract: {})",
            subscribed_gateways.iter().filter(|&&x| x).count(),
            NUM_GATEWAYS,
            gateways_with_contract.iter().filter(|&&x| x).count()
        );
        println!(
            "Subscribed regular nodes: {}/{} (with contract: {})",
            subscribed_nodes.iter().filter(|&&x| x).count(),
            NUM_REGULAR_NODES,
            nodes_with_contract.iter().filter(|&&x| x).count()
        );

        // Choose one subscribed node to send an update
        let updater_indices = subscribed_nodes
            .iter()
            .enumerate()
            .filter_map(|(i, &subscribed)| if subscribed { Some(i) } else { None })
            .collect::<Vec<_>>();

        if updater_indices.is_empty() {
            return Err(anyhow!("No subscribed nodes to send updates!"));
        }

        let updater_idx = updater_indices[0];
        println!("Node {updater_idx} will send an update");

        // Create a unique tag for the updater
        let update_tag = format!("ping-from-node-{updater_idx}");

        // Send the update
        let mut update_ping = Ping::default();
        update_ping.insert(update_tag.clone());

        println!(
            "Node {updater_idx} sending update with tag: {update_tag}"
        );
        node_clients[updater_idx]
            .send(ClientRequest::ContractOp(ContractRequest::Update {
                key: contract_key,
                data: UpdateData::Delta(StateDelta::from(
                    serde_json::to_vec(&update_ping).unwrap(),
                )),
            }))
            .await?;

        // Wait for the update to propagate through the network
        println!("Waiting for update to propagate...");
        tokio::time::sleep(Duration::from_secs(20)).await;

        // Check which nodes received the update
        let mut nodes_received_update = [false; NUM_REGULAR_NODES];
        let mut get_state_requests = Vec::new();

        for (i, subscribed) in subscribed_nodes.iter().enumerate() {
            if *subscribed {
                node_clients[i]
                    .send(ClientRequest::ContractOp(ContractRequest::Get {
                        key: *contract_key.id(),
                        return_contract_code: false,
                        subscribe: false,
                        blocking_subscribe: false,
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
                        key: *contract_key.id(),
                        return_contract_code: false,
                        subscribe: false,
                        blocking_subscribe: false,
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
                println!("Timeout waiting for get state responses, finalizing test");
                break;
            }

            // Check regular nodes
            let mut i = 0;
            while i < get_state_requests.len() {
                let node_idx = get_state_requests[i];
                let client = &mut node_clients[node_idx];

                match timeout(Duration::from_millis(500), client.recv()).await {
                    Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                        key,
                        state,
                        ..
                    }))) => {
                        if key == contract_key {
                            // Deserialize state and check for the update tag
                            match serde_json::from_slice::<Ping>(&state) {
                                Ok(ping_state) => {
                                    let has_update = ping_state.get(&update_tag).is_some();
                                    println!("Node {node_idx} has update: {has_update}");

                                    if has_update {
                                        let timestamps = ping_state.get(&update_tag).unwrap();
                                        println!(
                                            "Node {} has {} timestamps for tag {}",
                                            node_idx,
                                            timestamps.len(),
                                            update_tag
                                        );
                                    }

                                    nodes_received_update[node_idx] = has_update;
                                }
                                Err(e) => {
                                    println!(
                                        "Failed to deserialize state from node {node_idx}: {e}"
                                    );
                                }
                            }
                            get_state_requests.remove(i);
                            continue;
                        }
                    }
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        println!("Error receiving from node {node_idx}: {e}");
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
                    Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                        key,
                        state,
                        ..
                    }))) => {
                        if key == contract_key {
                            // Deserialize state and check for the update tag
                            match serde_json::from_slice::<Ping>(&state) {
                                Ok(ping_state) => {
                                    let has_update = ping_state.get(&update_tag).is_some();
                                    println!("Gateway {gw_idx} has update: {has_update}");

                                    if has_update {
                                        let timestamps = ping_state.get(&update_tag).unwrap();
                                        println!(
                                            "Gateway {} has {} timestamps for tag {}",
                                            gw_idx,
                                            timestamps.len(),
                                            update_tag
                                        );
                                    }

                                    gateways_received_update[gw_idx] = has_update;
                                }
                                Err(e) => {
                                    println!(
                                        "Failed to deserialize state from gateway {gw_idx}: {e}"
                                    );
                                }
                            }
                            gw_get_state_requests.remove(i);
                            continue;
                        }
                    }
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        println!("Error receiving from gateway {gw_idx}: {e}");
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
        println!("Final update propagation results:");

        // Summary for gateways
        let subscribed_gw_count = subscribed_gateways.iter().filter(|&&x| x).count();
        let updated_gw_count = gateways_received_update.iter().filter(|&&x| x).count();

        println!(
            "Gateways: {}/{} subscribed received the update ({:.1}%)",
            updated_gw_count,
            subscribed_gw_count,
            if subscribed_gw_count > 0 {
                (updated_gw_count as f64 / subscribed_gw_count as f64) * 100.0
            } else {
                0.0
            }
        );

        // Summary for regular nodes
        let subscribed_node_count = subscribed_nodes.iter().filter(|&&x| x).count();
        let updated_node_count = nodes_received_update.iter().filter(|&&x| x).count();

        println!(
            "Regular nodes: {}/{} subscribed received the update ({:.1}%)",
            updated_node_count,
            subscribed_node_count,
            if subscribed_node_count > 0 {
                (updated_node_count as f64 / subscribed_node_count as f64) * 100.0
            } else {
                0.0
            }
        );

        // Check nodes that didn't receive updates
        for (node_idx, (subscribed, updated)) in subscribed_nodes
            .iter()
            .zip(nodes_received_update.iter())
            .enumerate()
        {
            if *subscribed && !updated {
                println!(
                    "Node {node_idx} was subscribed but did not receive the update!"
                );

                // Get the node connectivity info
                let connections = node_connections[node_idx];
                println!(
                    "Node {node_idx} is connected to {connections} other regular nodes"
                );
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
        println!("Subscription propagation test completed successfully!");
        Ok::<_, anyhow::Error>(())
    })
    .instrument(span!(Level::INFO, "test_ping_partially_connected_network"));

    // Wait for test completion or node failures
    let mut all_futures = Vec::new();
    all_futures.extend(gateway_futures);
    all_futures.extend(regular_node_futures);

    // Use a loop with select_all to handle multiple futures
    let mut test_future = Box::pin(test);

    loop {
        let select_all_futures = futures::future::select_all(all_futures);

        select! {
            (result, _index, remaining) = select_all_futures => {
                match result {
                    Err(err) => {
                        return Err(anyhow!("Node failed: {}", err));
                    }
                    Ok(_) => {
                        // A node completed successfully, continue with remaining nodes
                        all_futures = remaining;
                        if all_futures.is_empty() {
                            // All nodes completed successfully, but test should still be running
                            println!("All nodes completed before test finished");
                            break;
                        }
                    }
                }
            }
            r = &mut test_future => {
                // Test completed
                r??;
                break;
            }
        }
    }

    // Keep presets alive until here
    println!(
        "Test complete, dropping {} gateway presets and {} node presets",
        gateway_presets.len(),
        node_presets.len()
    );

    Ok(())
}
