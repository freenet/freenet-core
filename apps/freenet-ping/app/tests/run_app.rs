mod common;

use std::{
    net::{Ipv4Addr, TcpListener},
    path::PathBuf,
    time::Duration,
};

use anyhow::anyhow;
use freenet::{local_node::NodeConfig, server::serve_gateway};
use freenet_ping_types::{Ping, PingContractOptions};
use freenet_stdlib::{
    client_api::{
        ClientRequest, ContractRequest, HostResponse, NodeDiagnosticsConfig, NodeQuery,
        QueryResponse, WebApi,
    },
    prelude::*,
};
use futures::FutureExt;
use rand::SeedableRng;
use testresult::TestResult;
use tokio::{select, time::sleep, time::timeout};
use tracing::{span, Instrument, Level};

use common::{
    base_node_test_config_with_rng, connect_async_with_config, gw_config_from_path_with_rng,
    ws_config, APP_TAG, PACKAGE_DIR, PATH_TO_CONTRACT,
};
use freenet_ping_app::ping_client::{
    run_ping_client, wait_for_get_response, wait_for_put_response, wait_for_subscribe_response,
    PingStats,
};

/// Helper function to collect diagnostics from all nodes for debugging update propagation issues
async fn collect_node_diagnostics(
    clients: &mut [&mut freenet_stdlib::client_api::WebApi],
    node_names: &[&str],
    contract_key: ContractKey,
    phase: &str,
) -> anyhow::Result<()> {
    use std::time::Duration;

    println!("=== NODE DIAGNOSTICS: {phase} ===");

    // Create diagnostic config with features enabled
    let config = NodeDiagnosticsConfig {
        include_node_info: true,
        include_network_info: true,
        include_subscriptions: true,
        contract_keys: vec![contract_key],
        include_system_metrics: true,
        include_detailed_peer_info: true,
        include_subscriber_peer_ids: true,
    };

    for (i, (client, node_name)) in clients.iter_mut().zip(node_names.iter()).enumerate() {
        println!("Collecting diagnostics from {node_name} (index {i})...");

        // Send diagnostic request
        match client
            .send(ClientRequest::NodeQueries(NodeQuery::NodeDiagnostics {
                config: config.clone(),
            }))
            .await
        {
            Ok(_) => {
                // Keep receiving messages until we get a diagnostic response (ignore everything else)
                loop {
                    match timeout(Duration::from_secs(5), client.recv()).await {
                        Ok(Ok(HostResponse::QueryResponse(QueryResponse::NodeDiagnostics(
                            response,
                        )))) => {
                            println!("--- {} DIAGNOSTICS ---", node_name.to_uppercase());

                            // Node info
                            if let Some(node_info) = &response.node_info {
                                println!(
                                    "📍 Node: {} ({})",
                                    node_info.peer_id,
                                    if node_info.is_gateway {
                                        "Gateway"
                                    } else {
                                        "Client"
                                    }
                                );
                            }

                            // Simplified peer information
                            if !response.connected_peers_detailed.is_empty() {
                                println!(
                                    "🔗 Connected to {} peer(s):",
                                    response.connected_peers_detailed.len()
                                );
                                for peer in &response.connected_peers_detailed {
                                    println!("  - Peer: {} ({})", peer.peer_id, peer.address);
                                }
                            } else if let Some(network_info) = &response.network_info {
                                println!(
                                    "🔗 Connected to {} peer(s):",
                                    network_info.connected_peers.len()
                                );
                                for (peer_id, addr) in &network_info.connected_peers {
                                    println!("  - {peer_id} at {addr}");
                                }
                            }

                            // Subscription info
                            if !response.subscriptions.is_empty() {
                                println!("Subscriptions:");
                                for sub in &response.subscriptions {
                                    println!("  - Contract: {}", sub.contract_key);
                                    println!("    Client ID: {}", sub.client_id);
                                }
                            } else {
                                println!("No active subscriptions");
                            }

                            // Contract states
                            if !response.contract_states.is_empty() {
                                println!(
                                    "📋 Contract States ({} contract(s)):",
                                    response.contract_states.len()
                                );
                                for (key, state) in &response.contract_states {
                                    println!("  📄 Contract: {key}");
                                    println!("     {} subscribers", state.subscribers);

                                    // Show subscriber peer IDs (NODOS SUSCRITOS)
                                    if !state.subscriber_peer_ids.is_empty() {
                                        println!(
                                            "     👥 Subscribed nodes: [{}]",
                                            state.subscriber_peer_ids.join(", ")
                                        );
                                    } else {
                                        println!("     👥 No subscribers");
                                    }
                                }
                            }

                            // System metrics (only show meaningful info)
                            if let Some(metrics) = &response.system_metrics {
                                println!("Network metrics:");
                                println!("  - Active connections: {}", metrics.active_connections);
                                println!("  - Seeding contracts: {}", metrics.seeding_contracts);
                            }

                            println!("--- END {} DIAGNOSTICS ---", node_name.to_uppercase());
                            break; // Got the diagnostic response, exit the loop
                        }
                        Ok(Ok(_other_message)) => {
                            // Ignore any other message (UpdateNotifications, etc.) and keep waiting
                            continue;
                        }
                        Ok(Err(e)) => {
                            println!("ERROR: {node_name} returned error: {e}");
                            break;
                        }
                        Err(_) => {
                            println!("ERROR: {node_name} diagnostic request timed out");
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                println!("ERROR: Failed to send diagnostic request to {node_name}: {e}");
            }
        }

        println!();
    }

    println!("=== END DIAGNOSTICS: {phase} ===\n");
    Ok(())
}

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_node_diagnostics_query() -> TestResult {
    // Setup network sockets for the gateway and client node
    let network_socket_gw = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_gw = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_node = TcpListener::bind("127.0.0.1:0")?;

    // Configure nodes with fixed seed for deterministic testing
    let test_seed = *b"diagnostics_test_seed_1234567890";
    let mut test_rng = rand::rngs::StdRng::from_seed(test_seed);

    println!("Testing NodeDiagnostics query functionality");
    println!("Using deterministic test seed: {test_seed:?}");

    // Configure gateway node
    let (config_gw, preset_cfg_gw) = base_node_test_config_with_rng(
        true,
        vec![],
        Some(network_socket_gw.local_addr()?.port()),
        ws_api_port_socket_gw.local_addr()?.port(),
        "gw_diagnostics", // data_dir_suffix
        None,             // base_tmp_dir
        None,             // blocked_addresses
        None,             // bind_ip
        &mut test_rng,
    )
    .await?;
    let public_port = config_gw.network_api.public_port.unwrap();
    let path = preset_cfg_gw.temp_dir.path().to_path_buf();
    let config_gw_info =
        gw_config_from_path_with_rng(public_port, &path, &mut test_rng, Ipv4Addr::LOCALHOST)?;
    let ws_api_port_gw = config_gw.ws_api.ws_api_port.unwrap();

    // Configure client node
    let (config_node, preset_cfg_node) = base_node_test_config_with_rng(
        false,
        vec![serde_json::to_string(&config_gw_info)?],
        None,
        ws_api_port_socket_node.local_addr()?.port(),
        "node_diagnostics", // data_dir_suffix
        None,               // base_tmp_dir
        None,               // blocked_addresses
        None,               // bind_ip
        &mut test_rng,
    )
    .await?;
    let ws_api_port_node = config_node.ws_api.ws_api_port.unwrap();

    println!("Gateway node data dir: {:?}", preset_cfg_gw.temp_dir.path());
    println!(
        "Client node data dir: {:?}",
        preset_cfg_node.temp_dir.path()
    );
    println!("Gateway location: {:?}", config_gw.network_api.location);
    println!(
        "Client node location: {:?}",
        config_node.network_api.location
    );

    // Free ports so they don't fail on initialization
    std::mem::drop(network_socket_gw);
    std::mem::drop(ws_api_port_socket_gw);
    std::mem::drop(ws_api_port_socket_node);

    // Start gateway node
    let gateway_node = async {
        let config = config_gw.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await?)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Start client node
    let client_node = async move {
        let config = config_node.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await?)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Main test logic
    let test = tokio::time::timeout(Duration::from_secs(120), async {
        // Wait for nodes to start up and connect
        tokio::time::sleep(Duration::from_secs(15)).await;

        // Connect to both nodes
        let uri_gw = format!(
            "ws://127.0.0.1:{ws_api_port_gw}/v1/contract/command?encodingProtocol=native"
        );
        let uri_node = format!(
            "ws://127.0.0.1:{ws_api_port_node}/v1/contract/command?encodingProtocol=native"
        );

        let (stream_gw, _) = connect_async_with_config(&uri_gw, Some(ws_config()), false).await?;
        let (stream_node, _) =
            connect_async_with_config(&uri_node, Some(ws_config()), false).await?;

        let mut client_gw = WebApi::start(stream_gw);
        let mut client_node = WebApi::start(stream_node);

        println!("=== TESTING NODE DIAGNOSTICS QUERIES ===");

        println!("Test 1: Querying diagnostics from gateway...");
        let config_basic = NodeDiagnosticsConfig {
            include_node_info: true,
            include_network_info: true,
            include_subscriptions: false,
            contract_keys: vec![],
            include_system_metrics: true,
            include_detailed_peer_info: true,
            include_subscriber_peer_ids: false,
        };

        client_gw
            .send(ClientRequest::NodeQueries(NodeQuery::NodeDiagnostics {
                config: config_basic.clone()
            }))
            .await?;

        match timeout(Duration::from_secs(10), client_gw.recv()).await {
            Ok(Ok(HostResponse::QueryResponse(QueryResponse::NodeDiagnostics(response)))) => {
                println!("✓ Gateway diagnostics received successfully!");

                // Validate response structure
                if let Some(node_info) = &response.node_info {
                    println!("  - Node ID: {}", node_info.peer_id);
                    println!("  - Is Gateway: {}", node_info.is_gateway);
                    if let Some(location) = &node_info.location {
                        println!("  - Location: {location}");
                    }
                    if let Some(listening_address) = &node_info.listening_address {
                        println!("  - Listening Address: {listening_address}");
                    }
                    assert!(node_info.is_gateway, "Gateway node should report is_gateway=true");
                } else {
                    return Err(anyhow!("Gateway diagnostics missing node_info"));
                }

                if let Some(network_info) = &response.network_info {
                    println!("  - Active connections: {}", network_info.active_connections);
                    println!("  - Connected peers: {}", network_info.connected_peers.len());
                } else {
                    return Err(anyhow!("Gateway diagnostics missing network_info"));
                }

                if let Some(metrics) = &response.system_metrics {
                    println!("  - Active connections: {}", metrics.active_connections);
                    println!("  - Seeding contracts: {}", metrics.seeding_contracts);
                } else {
                    return Err(anyhow!("Gateway diagnostics missing system_metrics"));
                }
            }
            Ok(Ok(response)) => {
                return Err(anyhow!("Gateway returned unexpected response: {:?}", response));
            }
            Ok(Err(e)) => {
                return Err(anyhow!("Gateway returned error: {}", e));
            }
            Err(_) => {
                return Err(anyhow!("Gateway diagnostic request timed out"));
            }
        }

        // Test 2: Basic node diagnostics from client node
        println!("\nTest 2: Querying basic diagnostics from client node...");

        client_node
            .send(ClientRequest::NodeQueries(NodeQuery::NodeDiagnostics {
                config: config_basic.clone()
            }))
            .await?;

        match timeout(Duration::from_secs(10), client_node.recv()).await {
            Ok(Ok(HostResponse::QueryResponse(QueryResponse::NodeDiagnostics(response)))) => {
                println!("✓ Client node diagnostics received successfully!");

                if let Some(node_info) = &response.node_info {
                    println!("  - Node ID: {}", node_info.peer_id);
                    println!("  - Is Gateway: {}", node_info.is_gateway);
                    if let Some(location) = &node_info.location {
                        println!("  - Location: {location}");
                    }
                    if let Some(listening_address) = &node_info.listening_address {
                        println!("  - Listening Address: {listening_address}");
                    }
                    assert!(!node_info.is_gateway, "Client node should report is_gateway=false");
                } else {
                    return Err(anyhow!("Client node diagnostics missing node_info"));
                }

                if let Some(network_info) = &response.network_info {
                    println!("  - Active connections: {}", network_info.active_connections);
                    println!("  - Connected peers: {}", network_info.connected_peers.len());
                    // Note: Client might not be connected immediately, this is acceptable
                    if network_info.connected_peers.is_empty() {
                        println!("  - Note: Client not yet connected to gateway (expected during startup)");
                    }
                }

                if let Some(metrics) = &response.system_metrics {
                    println!("  - Active connections: {}", metrics.active_connections);
                    println!("  - Seeding contracts: {}", metrics.seeding_contracts);
                    println!("  - System metrics collection working");
                }
            }
            Ok(Ok(response)) => {
                return Err(anyhow!("Client node returned unexpected response: {:?}", response));
            }
            Ok(Err(e)) => {
                return Err(anyhow!("Client node returned error: {}", e));
            }
            Err(_) => {
                return Err(anyhow!("Client node diagnostic request timed out"));
            }
        }

        // Test 3: Full diagnostics including subscriptions and operations
        println!("\nTest 3: Querying full diagnostics...");
        let config_full = NodeDiagnosticsConfig {
            include_node_info: true,
            include_network_info: true,
            include_subscriptions: true,
            contract_keys: vec![],
            include_system_metrics: true,
            include_detailed_peer_info: true,
            include_subscriber_peer_ids: true,
        };

        client_gw
            .send(ClientRequest::NodeQueries(NodeQuery::NodeDiagnostics {
                config: config_full.clone()
            }))
            .await?;

        match timeout(Duration::from_secs(10), client_gw.recv()).await {
            Ok(Ok(HostResponse::QueryResponse(QueryResponse::NodeDiagnostics(response)))) => {
                println!("✓ Full diagnostics received successfully!");
                println!("  - Subscriptions: {}", response.subscriptions.len());
                println!("  - Contract states: {}", response.contract_states.len());
                println!("  - Connected peers detailed: {}", response.connected_peers_detailed.len());

                // All fields should be present in full diagnostics
                assert!(response.node_info.is_some(), "Full diagnostics should include node_info");
                assert!(response.network_info.is_some(), "Full diagnostics should include network_info");
                assert!(response.system_metrics.is_some(), "Full diagnostics should include system_metrics");
            }
            Ok(Ok(response)) => {
                return Err(anyhow!("Full diagnostics returned unexpected response: {:?}", response));
            }
            Ok(Err(e)) => {
                return Err(anyhow!("Full diagnostics returned error: {}", e));
            }
            Err(_) => {
                return Err(anyhow!("Full diagnostics request timed out"));
            }
        }

        println!("\n=== ALL DIAGNOSTICS TESTS PASSED! ===");
        println!("✓ Gateway basic diagnostics working");
        println!("✓ Client node basic diagnostics working");
        println!("✓ Full diagnostics functionality working");
        println!("✓ Node info validation passed");
        println!("✓ Network connectivity validation passed");
        println!("✓ System metrics validation passed");

        Ok::<_, anyhow::Error>(())
    })
    .instrument(span!(Level::INFO, "test_node_diagnostics_query"));

    // Wait for test completion or node failures
    select! {
        gw = gateway_node => {
            let Err(gw) = gw;
            return Err(anyhow!("Gateway node failed: {}", gw).into());
        }
        n = client_node => {
            let Err(n) = n;
            return Err(anyhow!("Client node failed: {}", n).into());
        }
        r = test => {
            r??;
        }
    }

    Ok(())
}

#[ignore = "this test currently fails and we are workign on fixing it"]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_ping_multi_node() -> TestResult {
    // Setup network sockets for the gateway
    let network_socket_gw = TcpListener::bind("127.0.0.1:0")?;

    // Setup API sockets for all three nodes
    let ws_api_port_socket_gw = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_node1 = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_node2 = TcpListener::bind("127.0.0.1:0")?;

    // Configure gateway node with fixed seed for deterministic testing
    let test_seed = *b"app_loop_test_seed_0123456789012";
    let mut test_rng = rand::rngs::StdRng::from_seed(test_seed);

    println!("Using deterministic test seed: {test_seed:?}");
    println!("Test RNG initial state configured for deterministic network topology");

    // Configure gateway node
    let (config_gw, preset_cfg_gw, config_gw_info) = {
        let (cfg, preset) = base_node_test_config_with_rng(
            true,
            vec![],
            Some(network_socket_gw.local_addr()?.port()),
            ws_api_port_socket_gw.local_addr()?.port(),
            "gw_multi_node", // data_dir_suffix
            None,            // base_tmp_dir
            None,            // blocked_addresses
            None,            // bind_ip
            &mut test_rng,
        )
        .await?;
        let public_port = cfg.network_api.public_port.unwrap();
        let path = preset.temp_dir.path().to_path_buf();
        (
            cfg,
            preset,
            gw_config_from_path_with_rng(public_port, &path, &mut test_rng, Ipv4Addr::LOCALHOST)?,
        )
    };
    let ws_api_port_gw = config_gw.ws_api.ws_api_port.unwrap();

    // Configure client node 1
    let (config_node1, preset_cfg_node1) = base_node_test_config_with_rng(
        false,
        vec![serde_json::to_string(&config_gw_info)?],
        None,
        ws_api_port_socket_node1.local_addr()?.port(),
        "node1_multi_node", // data_dir_suffix
        None,               // base_tmp_dir
        None,               // blocked_addresses
        None,               // bind_ip
        &mut test_rng,
    )
    .await?;
    let ws_api_port_node1 = config_node1.ws_api.ws_api_port.unwrap();

    // Configure client node 2
    let (config_node2, preset_cfg_node2) = base_node_test_config_with_rng(
        false,
        vec![serde_json::to_string(&config_gw_info)?],
        None,
        ws_api_port_socket_node2.local_addr()?.port(),
        "node2_multi_node", // data_dir_suffix
        None,               // base_tmp_dir
        None,               // blocked_addresses
        None,               // bind_ip
        &mut test_rng,
    )
    .await?;
    let ws_api_port_node2 = config_node2.ws_api.ws_api_port.unwrap();

    // Log data directories and ring locations for debugging
    println!("Gateway node data dir: {:?}", preset_cfg_gw.temp_dir.path());
    println!("Node 1 data dir: {:?}", preset_cfg_node1.temp_dir.path());
    println!("Node 2 data dir: {:?}", preset_cfg_node2.temp_dir.path());

    // Log ring locations for network topology analysis
    println!("=== RING TOPOLOGY ANALYSIS ===");
    println!("Gateway location: {:?}", config_gw.network_api.location);
    println!("Node 1 location: {:?}", config_node1.network_api.location);
    println!("Node 2 location: {:?}", config_node2.network_api.location);

    // Calculate distances in the ring
    if let (Some(gw_loc), Some(n1_loc), Some(n2_loc)) = (
        config_gw.network_api.location,
        config_node1.network_api.location,
        config_node2.network_api.location,
    ) {
        let gw_to_n1_dist = (gw_loc - n1_loc).abs().min(1.0 - (gw_loc - n1_loc).abs());
        let gw_to_n2_dist = (gw_loc - n2_loc).abs().min(1.0 - (gw_loc - n2_loc).abs());
        let n1_to_n2_dist = (n1_loc - n2_loc).abs().min(1.0 - (n1_loc - n2_loc).abs());

        println!("Ring distances:");
        println!("  Gateway ↔ Node1: {gw_to_n1_dist:.6}");
        println!("  Gateway ↔ Node2: {gw_to_n2_dist:.6}");
        println!("  Node1 ↔ Node2: {n1_to_n2_dist:.6}");

        // Warn about potentially problematic distances
        let max_distance_threshold = 0.4; // Arbitrary threshold for "far" nodes
        if n1_to_n2_dist > max_distance_threshold {
            tracing::warn!(
                "Node1 and Node2 are far apart in the ring (distance: {:.6})",
                n1_to_n2_dist
            );
            tracing::warn!("This may cause update propagation issues!");
        }

        if gw_to_n1_dist > max_distance_threshold {
            tracing::warn!(
                "Gateway and Node1 are far apart (distance: {:.6})",
                gw_to_n1_dist
            );
        }

        if gw_to_n2_dist > max_distance_threshold {
            tracing::warn!(
                "Gateway and Node2 are far apart (distance: {:.6})",
                gw_to_n2_dist
            );
        }
    }
    println!("==============================");

    // Free ports so they don't fail on initialization
    std::mem::drop(network_socket_gw);
    std::mem::drop(ws_api_port_socket_gw);
    std::mem::drop(ws_api_port_socket_node1);
    std::mem::drop(ws_api_port_socket_node2);

    // Start gateway node
    let gateway_node = async {
        let config = config_gw.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await?)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Start client node 1
    let node1 = async move {
        let config = config_node1.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await?)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Start client node 2
    let node2 = async {
        let config = config_node2.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await?)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Main test logic
    let test = tokio::time::timeout(Duration::from_secs(450), async {
        // Wait for nodes to start up
        tokio::time::sleep(Duration::from_secs(10)).await;

        // Connect to all three nodes
        let uri_gw = format!(
            "ws://127.0.0.1:{ws_api_port_gw}/v1/contract/command?encodingProtocol=native"
        );
        let uri_node1 = format!(
            "ws://127.0.0.1:{ws_api_port_node1}/v1/contract/command?encodingProtocol=native"
        );
        let uri_node2 = format!(
            "ws://127.0.0.1:{ws_api_port_node2}/v1/contract/command?encodingProtocol=native"
        );

        let (stream_gw, _) = connect_async_with_config(&uri_gw, Some(ws_config()), false).await?;
        let (stream_node1, _) =
            connect_async_with_config(&uri_node1, Some(ws_config()), false).await?;
        let (stream_node2, _) =
            connect_async_with_config(&uri_node2, Some(ws_config()), false).await?;

        let mut client_gw = WebApi::start(stream_gw);
        let mut client_node1 = WebApi::start(stream_node1);
        let mut client_node2 = WebApi::start(stream_node2);

        // Load the ping contract
        let path_to_code = PathBuf::from(PACKAGE_DIR).join(PATH_TO_CONTRACT);
        println!("loading contract code: {}", path_to_code.display());

        // First compile to get the code hash, then create proper options
        let temp_options = PingContractOptions {
            frequency: Duration::from_secs(5),
            ttl: Duration::from_secs(30),
            tag: APP_TAG.to_string(),
            code_key: "".to_string(),
        };
        let temp_params = Parameters::from(serde_json::to_vec(&temp_options).unwrap());
        let container = common::load_contract(&path_to_code, temp_params)?;

        // Now get the actual code hash and create proper options
        let code_hash = CodeHash::from_code(container.data());
        let ping_options = PingContractOptions {
            frequency: Duration::from_secs(5),
            ttl: Duration::from_secs(30),
            tag: APP_TAG.to_string(),
            code_key: code_hash.to_string(),
        };
        let params = Parameters::from(serde_json::to_vec(&ping_options).unwrap());
        let container = common::load_contract(&path_to_code, params)?;
        let contract_key = container.key();

        // Step 1: Gateway node puts the contrac
        println!("Gateway node putting contract...");
        let wrapped_state = {
            let ping = Ping::default();
            let serialized = serde_json::to_vec(&ping)?;
            WrappedState::new(serialized)
        };

        client_gw
            .send(ClientRequest::ContractOp(ContractRequest::Put {
                contract: container.clone(),
                state: wrapped_state.clone(),
                related_contracts: RelatedContracts::new(),
                subscribe: false,
            }))
            .await?;

        // Wait for put response on gateway
        let key = wait_for_put_response(&mut client_gw, &contract_key)
            .await
            .map_err(anyhow::Error::msg)?;
        println!("Gateway: put ping contract successfully! key={key}");

        // Step 2: Node 1 gets the contrac
        println!("Node 1 getting contract...");
        client_node1
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key: *contract_key.id(),
                return_contract_code: true,
                subscribe: false,
            }))
            .await?;

        // Wait for get response on node 1
        let node1_state = wait_for_get_response(&mut client_node1, &contract_key)
            .await
            .map_err(anyhow::Error::msg)?;
        println!("Node 1: got contract with {} entries", node1_state.len());

        // Step 3: Node 2 gets the contrac
        println!("Node 2 getting contract...");
        client_node2
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key: *contract_key.id(),
                return_contract_code: true,
                subscribe: false,
            }))
            .await?;

        // Wait for get response on node 2
        let node2_state = wait_for_get_response(&mut client_node2, &contract_key)
            .await
            .map_err(anyhow::Error::msg)?;
        println!("Node 2: got contract with {} entries", node2_state.len());

        // Step 4: All nodes subscribe to the contract
        println!("=== SUBSCRIPTION PHASE ===");
        println!("All nodes subscribing to contract: {contract_key}");

        // Gateway subscribes
        println!("Gateway attempting subscription...");
        client_gw
            .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
                key: *contract_key.id(),
                summary: None,
            }))
            .await?;
        wait_for_subscribe_response(&mut client_gw, &contract_key)
            .await
            .map_err(anyhow::Error::msg)?;
        println!("Gateway: subscribed successfully!");

        // Node 1 subscribes
        println!("Node 1 attempting subscription...");
        client_node1
            .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
                key: *contract_key.id(),
                summary: None,
            }))
            .await?;
        wait_for_subscribe_response(&mut client_node1, &contract_key)
            .await
            .map_err(anyhow::Error::msg)?;
        println!("Node 1: subscribed successfully!");

        // Node 2 subscribes
        println!("Node 2 attempting subscription...");
        client_node2
            .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
                key: *contract_key.id(),
                summary: None,
            }))
            .await?;
        wait_for_subscribe_response(&mut client_node2, &contract_key)
            .await
            .map_err(anyhow::Error::msg)?;
        println!("Node 2: subscribed successfully!");

        println!("=== ALL SUBSCRIPTIONS COMPLETED ===");
        println!("All nodes are now subscribed and should receive all updates regardless of ring location");

        // Add a delay to ensure subscription system is fully active
        println!("Waiting 5 seconds for subscription system to stabilize...");
        sleep(Duration::from_secs(5)).await;

        // Collect diagnostics after subscription phase
        println!("=== AFTER SUBSCRIPTIONS DIAGNOSTICS ===");
        {
            let mut clients_for_diagnostics = vec![&mut client_gw, &mut client_node1, &mut client_node2];
            let node_names = ["Gateway", "Node1", "Node2"];
            let _ = collect_node_diagnostics(&mut clients_for_diagnostics, &node_names, contract_key, "AFTER SUBSCRIPTIONS").await;
        }

        // Step 5: All nodes send multiple updates to build history for eventual consistency testing

        // Create different tags for each node
        let gw_tag = "ping-from-gw".to_string();
        let node1_tag = "ping-from-node1".to_string();
        let node2_tag = "ping-from-node2".to_string();

        // Each node will send multiple pings to build history
        let ping_rounds = 3;
        println!("Each node will send {ping_rounds} pings to build history");

        for round in 1..=ping_rounds {
            println!("=== ROUND {round} UPDATE CYCLE ===");

            // Gateway sends update with its tag
            let mut gw_ping = Ping::default();
            gw_ping.insert(gw_tag.clone());
            let gw_timestamp = gw_ping.get(&gw_tag).and_then(|v| v.first()).unwrap();
            println!("Gateway sending update: tag='{gw_tag}', timestamp={gw_timestamp} (round {round})");
            client_gw
                .send(ClientRequest::ContractOp(ContractRequest::Update {
                    key: contract_key,
                    data: UpdateData::Delta(StateDelta::from(serde_json::to_vec(&gw_ping).unwrap())),
                }))
                .await?;

            // Node 1 sends update with its tag
            let mut node1_ping = Ping::default();
            node1_ping.insert(node1_tag.clone());
            let node1_timestamp = node1_ping.get(&node1_tag).and_then(|v| v.first()).unwrap();
            println!("Node 1 sending update: tag='{node1_tag}', timestamp={node1_timestamp} (round {round})");
            client_node1
                .send(ClientRequest::ContractOp(ContractRequest::Update {
                    key: contract_key,
                    data: UpdateData::Delta(StateDelta::from(serde_json::to_vec(&node1_ping).unwrap())),
                }))
                .await?;

            // Node 2 sends update with its tag
            let mut node2_ping = Ping::default();
            node2_ping.insert(node2_tag.clone());
            let node2_timestamp = node2_ping.get(&node2_tag).and_then(|v| v.first()).unwrap();
            println!("Node 2 sending update: tag='{node2_tag}', timestamp={node2_timestamp} (round {round})");
            client_node2
                .send(ClientRequest::ContractOp(ContractRequest::Update {
                    key: contract_key,
                    data: UpdateData::Delta(StateDelta::from(serde_json::to_vec(&node2_ping).unwrap())),
                }))
                .await?;

            // Small delay between rounds to ensure distinct timestamps
            println!("Waiting 30ms before next round...");
            sleep(Duration::from_millis(30)).await;
        }

        println!("=== ALL UPDATES SENT, WAITING FOR PROPAGATION ===");

        // Wait for updates to propagate across the network - longer wait to ensure eventual consistency
        println!("Waiting for updates to propagate across the network...");
        sleep(Duration::from_secs(30)).await;

        // Collect diagnostics after propagation wait period
        {
            let mut clients_for_diagnostics = vec![&mut client_gw, &mut client_node1, &mut client_node2];
            let node_names = ["Gateway", "Node1", "Node2"];
            let _ = collect_node_diagnostics(&mut clients_for_diagnostics, &node_names, contract_key, "AFTER PROPAGATION WAIT").await;
        }

        // Request the current state from all nodes
        println!("Querying all nodes for current state...");

        client_gw
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key: *contract_key.id(),
                return_contract_code: false,
                subscribe: false,
            }))
            .await?;

        client_node1
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key: *contract_key.id(),
                return_contract_code: false,
                subscribe: false,
            }))
            .await?;

        client_node2
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key: *contract_key.id(),
                return_contract_code: false,
                subscribe: false,
            }))
            .await?;

        // Receive and deserialize the states from all nodes
        let final_state_gw = wait_for_get_response(&mut client_gw, &contract_key)
            .await
            .map_err(anyhow::Error::msg)?;
        let final_state_node1 = wait_for_get_response(&mut client_node1, &contract_key)
            .await
            .map_err(anyhow::Error::msg)?;

        let final_state_node2 = wait_for_get_response(&mut client_node2, &contract_key)
            .await
            .map_err(anyhow::Error::msg)?;

        // Log the final state from each node
        println!("Gateway final state: {final_state_gw}");
        println!("Node 1 final state: {final_state_node1}");
        println!("Node 2 final state: {final_state_node2}");

        // Final diagnostic collection to understand the issue
        {
            let mut clients_for_diagnostics = vec![&mut client_gw, &mut client_node1, &mut client_node2];
            let node_names = ["Gateway", "Node1", "Node2"];
            let _ = collect_node_diagnostics(&mut clients_for_diagnostics, &node_names, contract_key, "FINAL STATE ANALYSIS").await;
        }

        // Show detailed comparison of ping history per tag
        println!("===== DETAILED PROPAGATION ANALYSIS =====");

        let tags = vec![gw_tag.clone(), node1_tag.clone(), node2_tag.clone()];
        let mut all_histories_match = true;
        let mut propagation_matrix = std::collections::HashMap::new();

        // First, analyze what each node received
        println!("=== PROPAGATION MATRIX ===");
        let nodes = [("Gateway", &final_state_gw), ("Node1", &final_state_node1), ("Node2", &final_state_node2)];

        for (node_name, state) in &nodes {
            println!("{} received tags: {:?}", node_name, state.keys().collect::<Vec<_>>());
            for tag in &tags {
                let has_tag = state.contains_key(tag);
                let count = state.get(tag).map(|v| v.len()).unwrap_or(0);
                println!("  {node_name} has '{tag}': {has_tag} (count: {count})");
                propagation_matrix.insert((node_name.to_string(), tag.clone()), (has_tag, count));
            }
        }

        // Analyze cross-propagation success
        println!("=== CROSS-PROPAGATION ANALYSIS ===");

        // Gateway should have all tags
        let gw_has_node1 = final_state_gw.contains_key(&node1_tag);
        let gw_has_node2 = final_state_gw.contains_key(&node2_tag);

        // Node1 should have gateway and node2 tags
        let node1_has_gw = final_state_node1.contains_key(&gw_tag);
        let node1_has_node2 = final_state_node1.contains_key(&node2_tag);

        // Node2 should have gateway and node1 tags
        let node2_has_gw = final_state_node2.contains_key(&gw_tag);
        let node2_has_node1 = final_state_node2.contains_key(&node1_tag);

        println!("Cross-propagation success rates:");
        println!("  Gateway ← Node1: {gw_has_node1} | Gateway ← Node2: {gw_has_node2}");
        println!("  Node1 ← Gateway: {node1_has_gw} | Node1 ← Node2: {node1_has_node2}");
        println!("  Node2 ← Gateway: {node2_has_gw} | Node2 ← Node1: {node2_has_node1}");

        // Calculate overall propagation success
        let total_propagation_attempts = 6; // 3 nodes × 2 cross-propagations each
        let successful_propagations = [gw_has_node1, gw_has_node2, node1_has_gw,
                                     node1_has_node2, node2_has_gw, node2_has_node1]
                                     .iter().filter(|&&x| x).count();
        let propagation_rate = successful_propagations as f64 / total_propagation_attempts as f64;

        println!("Overall propagation success: {}/{} ({:.1}%)",
                      successful_propagations, total_propagation_attempts, propagation_rate * 100.0);

        for tag in &tags {
            println!("=== DETAILED ANALYSIS FOR TAG '{tag}' ===");

            // Get the vector of timestamps for this tag from each node
            let gw_history = final_state_gw.get(tag).cloned().unwrap_or_default();
            let node1_history = final_state_node1.get(tag).cloned().unwrap_or_default();
            let node2_history = final_state_node2.get(tag).cloned().unwrap_or_default();

            // Log which nodes have this tag
            println!("Tag '{tag}' presence:");
            println!("  Gateway: {} (count: {})", !gw_history.is_empty(), gw_history.len());
            println!("  Node1: {} (count: {})", !node1_history.is_empty(), node1_history.len());
            println!("  Node2: {} (count: {})", !node2_history.is_empty(), node2_history.len());

            // Histories should be non-empty if eventual consistency worked
            if gw_history.is_empty() || node1_history.is_empty() || node2_history.is_empty() {
                tracing::warn!("Tag '{}' missing from one or more nodes!", tag);
                if gw_history.is_empty() { tracing::warn!("Gateway missing '{}'", tag); }
                if node1_history.is_empty() { tracing::warn!("Node1 missing '{}'", tag); }
                if node2_history.is_empty() { tracing::warn!("Node2 missing '{}'", tag); }
                all_histories_match = false;
                continue;
            }

            // Log the number of entries in each history
            println!("  - Gateway: {} entries", gw_history.len());
            println!("  - Node 1:  {} entries", node1_history.len());
            println!("  - Node 2:  {} entries", node2_history.len());

            // Check if the histories have the same length
            if gw_history.len() != node1_history.len() || gw_history.len() != node2_history.len() {
                tracing::warn!("Different number of history entries for tag '{}'!", tag);
                all_histories_match = false;
                continue;
            }

            // Compare the actual timestamp vectors element by elemen
            let mut timestamps_match = true;
            for i in 0..gw_history.len() {
                if gw_history[i] != node1_history[i] || gw_history[i] != node2_history[i] {
                    timestamps_match = false;
                    tracing::warn!(
                        "Timestamp mismatch at position {}:\n  - Gateway: {}\n  - Node 1:  {}\n  - Node 2:  {}",
                        i, gw_history[i], node1_history[i], node2_history[i]
                    );
                }
            }

            if timestamps_match {
                println!("History for tag '{tag}' is identical across all nodes!");
            } else {
                tracing::warn!("History timestamps for tag '{}' differ between nodes!", tag);
                all_histories_match = false;
            }
        }

        println!("=================================================");

        // Final diagnosis before assertion
        if !all_histories_match {
            tracing::error!("PROPAGATION FAILURE DIAGNOSIS:");
            tracing::error!("Overall propagation rate: {:.1}%", propagation_rate * 100.0);

            if propagation_rate < 0.5 {
                tracing::error!("SEVERE: Less than 50% of updates propagated");
                tracing::error!("This is a BUG - all subscribed nodes MUST receive updates!");
                tracing::error!("Possible causes:");
                tracing::error!("1. Bug in subscription notification system");
                tracing::error!("2. Network connectivity failure");
                tracing::error!("3. Updates sent before subscriptions fully active");
                tracing::error!("4. Configuration issues (skip_load_from_network, etc.)");
            } else if propagation_rate < 0.8 {
                tracing::error!("MODERATE: 50-80% of updates propagated");
                tracing::error!("Still problematic - subscribed nodes should receive ALL updates");
                tracing::error!("This suggests partial failure in notification system");
            } else {
                tracing::error!("PARTIAL: >80% propagated but timestamp mismatches");
                tracing::error!("Updates reached nodes but content differs - timing or merge issues");
            }

            // More detailed failure analysis
            if !gw_has_node1 && !gw_has_node2 {
                tracing::error!("Gateway received NO updates from client nodes!");
                tracing::error!("This suggests client→gateway propagation failure");
            }

            if !node1_has_node2 && !node2_has_node1 {
                tracing::error!("No peer-to-peer propagation between client nodes!");
                tracing::error!("This is expected if they only connect through gateway");
            }

            if gw_has_node1 && gw_has_node2 && (!node1_has_gw || !node2_has_gw) {
                tracing::error!("Gateway received updates but failed to propagate them!");
                tracing::error!("This suggests gateway→client propagation failure");
            }
        }

        // Final assertion for eventual consistency
        assert!(
            all_histories_match,
            "Eventual consistency test failed: Ping histories are not identical across all nodes\n\
             Propagation success rate: {:.1}% ({}/{})\n\
             Check logs above for detailed diagnosis of the failure",
            propagation_rate * 100.0, successful_propagations, total_propagation_attempts
        );

        println!("Eventual consistency test PASSED - all nodes have identical ping histories!");

        Ok::<_, anyhow::Error>(())
    })
    .instrument(span!(Level::INFO, "test_ping_multi_node"));

    // Wait for test completion or node failures
    select! {
        gw = gateway_node => {
            let Err(gw) = gw;
            return Err(anyhow!("Gateway node failed: {}", gw).into());
        }
        n1 = node1 => {
            let Err(n1) = n1;
            return Err(anyhow!("Node 1 failed: {}", n1).into());
        }
        n2 = node2 => {
            let Err(n2) = n2;
            return Err(anyhow!("Node 2 failed: {}", n2).into());
        }
        r = test => {
            r??;
        }
    }

    Ok(())
}

#[ignore = "this test currently fails and we are workign on fixing it"]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_ping_application_loop() -> TestResult {
    // Setup network sockets for the gateway
    let network_socket_gw = TcpListener::bind("127.0.0.1:0")?;

    // Setup API sockets for all three nodes
    let ws_api_port_socket_gw = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_node1 = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_node2 = TcpListener::bind("127.0.0.1:0")?;

    // Configure nodes with fixed seed for deterministic testing
    let test_seed = *b"app_loop_test_seed_0123456789012";
    let mut test_rng = rand::rngs::StdRng::from_seed(test_seed);

    tracing::info!(
        "Using deterministic test seed for app loop: {:?}",
        test_seed
    );
    tracing::info!("Test RNG initial state configured for deterministic network topology");

    // Configure gateway node
    let (config_gw, preset_cfg_gw, config_gw_info) = {
        let (cfg, preset) = base_node_test_config_with_rng(
            true,
            vec![],
            Some(network_socket_gw.local_addr()?.port()),
            ws_api_port_socket_gw.local_addr()?.port(),
            "gw_app_loop", // data_dir_suffix
            None,          // base_tmp_dir
            None,          // blocked_addresses
            None,          // bind_ip
            &mut test_rng,
        )
        .await?;
        let public_port = cfg.network_api.public_port.unwrap();
        let path = preset.temp_dir.path().to_path_buf();
        (
            cfg,
            preset,
            gw_config_from_path_with_rng(public_port, &path, &mut test_rng, Ipv4Addr::LOCALHOST)?,
        )
    };
    let ws_api_port_gw = config_gw.ws_api.ws_api_port.unwrap();

    // Configure client node 1
    let (config_node1, preset_cfg_node1) = base_node_test_config_with_rng(
        false,
        vec![serde_json::to_string(&config_gw_info)?],
        None,
        ws_api_port_socket_node1.local_addr()?.port(),
        "node1_app_loop", // data_dir_suffix
        None,             // base_tmp_dir
        None,             // blocked_addresses
        None,             // bind_ip
        &mut test_rng,
    )
    .await?;
    let ws_api_port_node1 = config_node1.ws_api.ws_api_port.unwrap();

    // Configure client node 2
    let (config_node2, preset_cfg_node2) = base_node_test_config_with_rng(
        false,
        vec![serde_json::to_string(&config_gw_info)?],
        None,
        ws_api_port_socket_node2.local_addr()?.port(),
        "node2_app_loop", // data_dir_suffix
        None,             // base_tmp_dir
        None,             // blocked_addresses
        None,             // bind_ip
        &mut test_rng,
    )
    .await?;
    let ws_api_port_node2 = config_node2.ws_api.ws_api_port.unwrap();

    // Log data directories and locations for debugging
    tracing::info!("Gateway node data dir: {:?}", preset_cfg_gw.temp_dir.path());
    tracing::info!("Node 1 data dir: {:?}", preset_cfg_node1.temp_dir.path());
    tracing::info!("Node 2 data dir: {:?}", preset_cfg_node2.temp_dir.path());
    tracing::info!(
        "App loop - Gateway location: {:?}",
        config_gw.network_api.location
    );
    tracing::info!(
        "App loop - Node 1 location: {:?}",
        config_node1.network_api.location
    );
    tracing::info!(
        "App loop - Node 2 location: {:?}",
        config_node2.network_api.location
    );

    // Free ports so they don't fail on initialization
    std::mem::drop(network_socket_gw);
    std::mem::drop(ws_api_port_socket_gw);
    std::mem::drop(ws_api_port_socket_node1);
    std::mem::drop(ws_api_port_socket_node2);

    // Start gateway node
    let gateway_node = async {
        let config = config_gw.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await?)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Start client node 1
    let node1 = async move {
        let config = config_node1.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await?)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Start client node 2
    let node2 = async {
        let config = config_node2.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await?)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Main test logic
    let test = tokio::time::timeout(Duration::from_secs(180), async {
        // Wait for nodes to start up
        tokio::time::sleep(Duration::from_secs(10)).await;

        // Connect to all three nodes
        let uri_gw =
            format!("ws://127.0.0.1:{ws_api_port_gw}/v1/contract/command?encodingProtocol=native");
        let uri_node1 = format!(
            "ws://127.0.0.1:{ws_api_port_node1}/v1/contract/command?encodingProtocol=native"
        );
        let uri_node2 = format!(
            "ws://127.0.0.1:{ws_api_port_node2}/v1/contract/command?encodingProtocol=native"
        );

        let (stream_gw, _) = connect_async_with_config(&uri_gw, Some(ws_config()), false).await?;
        let (stream_node1, _) =
            connect_async_with_config(&uri_node1, Some(ws_config()), false).await?;
        let (stream_node2, _) =
            connect_async_with_config(&uri_node2, Some(ws_config()), false).await?;

        let mut client_gw = WebApi::start(stream_gw);
        let mut client_node1 = WebApi::start(stream_node1);
        let mut client_node2 = WebApi::start(stream_node2);

        // Load the ping contract
        let path_to_code = PathBuf::from(PACKAGE_DIR).join(PATH_TO_CONTRACT);
        println!("loading contract code: {}", path_to_code.display());

        // First compile to get the code hash, then create proper options
        let temp_options = PingContractOptions {
            frequency: Duration::from_secs(3),
            ttl: Duration::from_secs(30),
            tag: APP_TAG.to_string(),
            code_key: "".to_string(),
        };
        let temp_params = Parameters::from(serde_json::to_vec(&temp_options).unwrap());
        let container = common::load_contract(&path_to_code, temp_params)?;

        // Now get the actual code hash and create proper options for all nodes
        let code_hash = CodeHash::from_code(container.data());

        let gw_options = PingContractOptions {
            frequency: Duration::from_secs(3),
            ttl: Duration::from_secs(30),
            tag: APP_TAG.to_string(),
            code_key: code_hash.to_string(),
        };

        let node1_options = PingContractOptions {
            frequency: Duration::from_secs(3),
            ttl: Duration::from_secs(30),
            tag: APP_TAG.to_string(),
            code_key: code_hash.to_string(),
        };

        let node2_options = PingContractOptions {
            frequency: Duration::from_secs(3),
            ttl: Duration::from_secs(30),
            tag: APP_TAG.to_string(),
            code_key: code_hash.to_string(),
        };

        let params = Parameters::from(serde_json::to_vec(&gw_options).unwrap());
        let container = common::load_contract(&path_to_code, params)?;
        let contract_key = container.key();

        // Step 1: Gateway node puts the contrac
        println!("Gateway node putting contract...");
        let ping = Ping::default();
        let serialized = serde_json::to_vec(&ping)?;
        let wrapped_state = WrappedState::new(serialized);

        client_gw
            .send(ClientRequest::ContractOp(ContractRequest::Put {
                contract: container.clone(),
                state: wrapped_state.clone(),
                related_contracts: RelatedContracts::new(),
                subscribe: false,
            }))
            .await?;

        // Wait for put response on gateway
        let key = wait_for_put_response(&mut client_gw, &contract_key)
            .await
            .map_err(anyhow::Error::msg)?;
        println!("Gateway: put ping contract successfully! key={key}");

        // Step 2: Node 1 gets the contrac
        println!("Node 1 getting contract...");
        client_node1
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key: *contract_key.id(),
                return_contract_code: true,
                subscribe: false,
            }))
            .await?;

        // Wait for get response on node 1
        let node1_state = wait_for_get_response(&mut client_node1, &contract_key)
            .await
            .map_err(anyhow::Error::msg)?;
        println!("Node 1: got contract with {} entries", node1_state.len());

        // Step 3: Node 2 gets the contrac
        println!("Node 2 getting contract...");
        client_node2
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key: *contract_key.id(),
                return_contract_code: true,
                subscribe: false,
            }))
            .await?;

        // Wait for get response on node 2
        let node2_state = wait_for_get_response(&mut client_node2, &contract_key)
            .await
            .map_err(anyhow::Error::msg)?;
        println!("Node 2: got contract with {} entries", node2_state.len());

        // Step 4: Subscribe all clients to the contrac
        // Gateway subscribes
        client_gw
            .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
                key: *contract_key.id(),
                summary: None,
            }))
            .await?;
        wait_for_subscribe_response(&mut client_gw, &contract_key)
            .await
            .map_err(anyhow::Error::msg)?;
        println!("Gateway: subscribed successfully!");

        // Node 1 subscribes
        client_node1
            .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
                key: *contract_key.id(),
                summary: None,
            }))
            .await?;
        wait_for_subscribe_response(&mut client_node1, &contract_key)
            .await
            .map_err(anyhow::Error::msg)?;
        println!("Node 1: subscribed successfully!");

        // Node 2 subscribes
        client_node2
            .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
                key: *contract_key.id(),
                summary: None,
            }))
            .await?;
        wait_for_subscribe_response(&mut client_node2, &contract_key)
            .await
            .map_err(anyhow::Error::msg)?;
        println!("Node 2: subscribed successfully!");

        // Step 5: Run the ping clients on all nodes simultaneously
        // Create channels for controlled shutdown
        let (gw_shutdown_tx, gw_shutdown_rx) = tokio::sync::oneshot::channel();
        let (node1_shutdown_tx, node1_shutdown_rx) = tokio::sync::oneshot::channel();
        let (node2_shutdown_tx, node2_shutdown_rx) = tokio::sync::oneshot::channel();

        // Clone clients for the handle functions
        let mut client_gw_clone = client_gw;
        let mut client_node1_clone = client_node1;
        let mut client_node2_clone = client_node2;

        // Set up test duration - short enough for testing but long enough to see interactions
        let test_duration = Duration::from_secs(30);

        // Start all ping clients
        let gw_handle = tokio::spawn(async move {
            let mut local_state = Ping::default();
            run_ping_client(
                &mut client_gw_clone,
                contract_key,
                gw_options,
                "gateway".into(),
                &mut local_state,
                Some(gw_shutdown_rx),
                Some(test_duration),
            )
            .await
        });

        let node1_handle = tokio::spawn(async move {
            let mut local_state = Ping::default();
            run_ping_client(
                &mut client_node1_clone,
                contract_key,
                node1_options,
                "node1".into(),
                &mut local_state,
                Some(node1_shutdown_rx),
                Some(test_duration),
            )
            .await
        });

        let node2_handle = tokio::spawn(async move {
            let mut local_state = Ping::default();
            run_ping_client(
                &mut client_node2_clone,
                contract_key,
                node2_options,
                "node2".into(),
                &mut local_state,
                Some(node2_shutdown_rx),
                Some(test_duration),
            )
            .await
        });

        // Wait for test duration plus a small buffer
        tokio::time::sleep(test_duration + Duration::from_secs(15)).await;

        // Signal all clients to shut down if they haven't already
        let _ = gw_shutdown_tx.send(());
        let _ = node1_shutdown_tx.send(());
        let _ = node2_shutdown_tx.send(());

        // Wait for all clients to complete and get their stats
        let gw_stats = gw_handle.await?.map_err(anyhow::Error::msg)?;
        let node1_stats = node1_handle.await?.map_err(anyhow::Error::msg)?;
        let node2_stats = node2_handle.await?.map_err(anyhow::Error::msg)?;

        // Log ping statistics
        tracing::info!("Gateway sent {} pings", gw_stats.sent_count);
        tracing::info!("Node 1 sent {} pings", node1_stats.sent_count);
        tracing::info!("Node 2 sent {} pings", node2_stats.sent_count);

        // Verify that each node saw updates from other nodes
        assert!(
            gw_stats.received_counts.contains_key("node1"),
            "Gateway didn't receive pings from node 1"
        );
        assert!(
            gw_stats.received_counts.contains_key("node2"),
            "Gateway didn't receive pings from node 2"
        );

        assert!(
            node1_stats.received_counts.contains_key("gateway"),
            "Node 1 didn't receive pings from gateway"
        );
        assert!(
            node1_stats.received_counts.contains_key("node2"),
            "Node 1 didn't receive pings from node 2"
        );

        assert!(
            node2_stats.received_counts.contains_key("gateway"),
            "Node 2 didn't receive pings from gateway"
        );
        assert!(
            node2_stats.received_counts.contains_key("node1"),
            "Node 2 didn't receive pings from node 1"
        );

        // Check that each node received a reasonable number of pings
        let check_ping_counts = |name: &str, stats: &PingStats| {
            for (source, count) in &stats.received_counts {
                tracing::info!("{} received {} pings from {}", name, count, source);
                assert!(*count > 0, "{name} received no pings from {source}");
            }
        };

        check_ping_counts("Gateway", &gw_stats);
        check_ping_counts("Node 1", &node1_stats);
        check_ping_counts("Node 2", &node2_stats);

        tracing::info!("All ping clients successfully sent and received pings!");

        Ok::<_, anyhow::Error>(())
    })
    .instrument(span!(Level::INFO, "test_ping_application_loop"));

    // Wait for test completion or node failures
    select! {
        gw = gateway_node => {
            let Err(gw) = gw;
            return Err(anyhow!("Gateway node failed: {}", gw).into());
        }
        n1 = node1 => {
            let Err(n1) = n1;
            return Err(anyhow!("Node 1 failed: {}", n1).into());
        }
        n2 = node2 => {
            let Err(n2) = n2;
            return Err(anyhow!("Node 2 failed: {}", n2).into());
        }
        r = test => {
            r??;
        }
    }

    Ok(())
}
