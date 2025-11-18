//! # Blocked Peers Tests
//!
//! This file implements a parameterized test framework for the "blocked peers" scenario
//! in Freenet, where direct peer-to-peer connections between certain nodes are intentionally
//! blocked. The tests verify that contract state updates can propagate via gateway nodes
//! when direct connections aren't available.
//!
//! ## Parameterized Testing Approach
//!
//! This module uses a single parameterized test implementation with different configuration
//! profiles. Each test variant shares the same core logic but differs in aspects such as:
//!
//! - Timeouts and delays
//! - Update rounds and wait periods
//! - Logging verbosity
//! - Verification approaches
//! - Subscription strategies
//!
//! ## Test Variants Included
//!
//! - `test_ping_blocked_peers`: Baseline implementation
//! - `test_ping_blocked_peers_simple`: Minimalist approach with one round of updates
//! - `test_ping_blocked_peers_solution`: Reference implementation with best practices
//!
//! ## Test Scenario
//!
//! Each test follows the same general flow:
//! 1. Set up a gateway node and two regular nodes (Node1 and Node2)
//! 2. Configure Node1 to block Node2's address, and Node2 to block Node1's address
//! 3. Deploy the ping contract on the gateway
//! 4. Have all nodes subscribe to the contract
//! 5. Each node sends updates with its own identifier tag
//! 6. Verify that all nodes eventually receive all updates, despite the blocks
//! 7. Confirm that all nodes converge to the same final state
//!
//! This demonstrates that even when direct peer-to-peer connections are blocked,
//! contract updates can propagate via gateway nodes.

mod common;

use std::{
    net::{Ipv4Addr, SocketAddr, TcpListener},
    time::Duration,
};

use anyhow::anyhow;
use common::{
    base_node_test_config, get_all_ping_states, gw_config_from_path, ping_states_equal, APP_TAG,
    PACKAGE_DIR, PATH_TO_CONTRACT,
};
use freenet::{config::set_logger, local_node::NodeConfig, server::serve_gateway};
use freenet_ping_app::ping_client::{
    wait_for_get_response, wait_for_put_response, wait_for_subscribe_response,
};
use freenet_ping_types::{Ping, PingContractOptions};
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, WebApi},
    prelude::*,
};
use futures::FutureExt;
use testresult::TestResult;
use tokio::{select, time::sleep};
use tokio_tungstenite::connect_async;
use tracing::{level_filters::LevelFilter, span, Instrument, Level};

/// Configuration for blocked peers test variants
#[derive(Debug, Clone)]
struct BlockedPeersConfig {
    /// Name of the test variant for logging and data directory naming
    test_name: &'static str,
    /// Initial wait time for nodes to start up
    initial_wait: Duration,
    /// Timeout for put/get/subscribe operations
    operation_timeout: Duration,
    /// Number of update rounds to send
    update_rounds: usize,
    /// Time to wait between update rounds
    update_wait: Duration,
    /// Time to wait after all updates before verification
    propagation_wait: Duration,
    /// Whether to use verbose logging (for debug variant)
    verbose_logging: bool,
    /// Strategy for regular check intervals during update propagation
    check_interval: Option<Duration>, // None means no regular checks
    /// Whether to send additional "refresh" updates after initial ones
    send_refresh_updates: bool,
    /// Whether to send final updates if propagation isn't complete
    send_final_updates: bool,
    /// Whether to subscribe immediately in get/put operations
    subscribe_immediately: bool,
}

/// Runs a blocked peers test with the provided configuration
async fn run_blocked_peers_test(config: BlockedPeersConfig) -> TestResult {
    if config.verbose_logging {
        std::env::set_var(
            "RUST_LOG",
            "debug,freenet::operations::subscribe=trace,freenet::contract=trace",
        );
    }
    set_logger(Some(LevelFilter::DEBUG), None);

    tracing::info!("Starting {} blocked peers test...", config.test_name);

    // Network setup
    let network_socket_gw = TcpListener::bind("127.0.0.1:0")?;
    let gw_network_port = network_socket_gw.local_addr()?.port();

    let ws_api_port_socket_gw = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_node1 = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_node2 = TcpListener::bind("127.0.0.1:0")?;

    let network_socket_node1 = TcpListener::bind("127.0.0.1:0")?;
    let node1_network_port = network_socket_node1.local_addr()?.port();
    let node1_network_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), node1_network_port);

    let network_socket_node2 = TcpListener::bind("127.0.0.1:0")?;
    let node2_network_port = network_socket_node2.local_addr()?.port();
    let node2_network_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), node2_network_port);

    // Configure gateway
    let (config_gw, preset_cfg_gw, config_gw_info) = {
        let (cfg, preset) = base_node_test_config(
            true, // is_gateway
            vec![],
            Some(gw_network_port),
            ws_api_port_socket_gw.local_addr()?.port(),
            &format!("gw_{}", config.test_name),
            None,
            None, // Gateway doesn't block any peers
        )
        .await?;
        let public_port = cfg.network_api.public_port.unwrap();
        let path = preset.temp_dir.path().to_path_buf();
        (cfg, preset, gw_config_from_path(public_port, &path)?)
    };
    let ws_api_port_gw = config_gw.ws_api.ws_api_port.unwrap();

    // Configure Node1 (blocks Node2)
    let (config_node1, preset_cfg_node1) = base_node_test_config(
        false, // is_gateway
        vec![serde_json::to_string(&config_gw_info)?],
        Some(node1_network_port),
        ws_api_port_socket_node1.local_addr()?.port(),
        &format!("node1_{}", config.test_name),
        None,
        Some(vec![node2_network_addr]), // Node1 blocks Node2
    )
    .await?;
    let ws_api_port_node1 = config_node1.ws_api.ws_api_port.unwrap();

    // Configure Node2 (blocks Node1)
    let (config_node2, preset_cfg_node2) = base_node_test_config(
        false, // is_gateway
        vec![serde_json::to_string(&config_gw_info)?],
        Some(node2_network_port),
        ws_api_port_socket_node2.local_addr()?.port(),
        &format!("node2_{}", config.test_name),
        None,
        Some(vec![node1_network_addr]), // Node2 blocks Node1
    )
    .await?;
    let ws_api_port_node2 = config_node2.ws_api.ws_api_port.unwrap();

    tracing::info!("Gateway node data dir: {:?}", preset_cfg_gw.temp_dir.path());
    tracing::info!("Node 1 data dir: {:?}", preset_cfg_node1.temp_dir.path());
    tracing::info!("Node 2 data dir: {:?}", preset_cfg_node2.temp_dir.path());
    tracing::info!("Node 1 blocks: {:?}", node2_network_addr);
    tracing::info!("Node 2 blocks: {:?}", node1_network_addr);

    // Free socket resources
    std::mem::drop(network_socket_gw);
    std::mem::drop(ws_api_port_socket_gw);
    std::mem::drop(network_socket_node1);
    std::mem::drop(ws_api_port_socket_node1);
    std::mem::drop(network_socket_node2);
    std::mem::drop(ws_api_port_socket_node2);

    // Start nodes
    let gateway_node = async {
        let config = config_gw.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    let node1 = async {
        let config = config_node1.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    let node2 = async {
        let config = config_node2.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Create a span with static name and test variant as a field
    let span = span!(
        Level::INFO,
        "test_ping_blocked_peers",
        variant = config.test_name
    );

    // Main test
    let test = tokio::time::timeout(Duration::from_secs(300), async {
        tracing::info!(
            "Waiting for nodes to start up ({:?})...",
            config.initial_wait
        );
        tokio::time::sleep(config.initial_wait).await;

        // Connect to nodes via WebSocket
        let uri_gw =
            format!("ws://127.0.0.1:{ws_api_port_gw}/v1/contract/command?encodingProtocol=native");
        let uri_node1 = format!(
            "ws://127.0.0.1:{ws_api_port_node1}/v1/contract/command?encodingProtocol=native"
        );
        let uri_node2 = format!(
            "ws://127.0.0.1:{ws_api_port_node2}/v1/contract/command?encodingProtocol=native"
        );

        tracing::info!("Connecting to Gateway at {}", uri_gw);
        let (stream_gw, _) = connect_async(&uri_gw).await?;
        let mut client_gw = WebApi::start(stream_gw);

        tracing::info!("Connecting to Node1 at {}", uri_node1);
        let (stream_node1, _) = connect_async(&uri_node1).await?;
        let mut client_node1 = WebApi::start(stream_node1);

        tracing::info!("Connecting to Node2 at {}", uri_node2);
        let (stream_node2, _) = connect_async(&uri_node2).await?;
        let mut client_node2 = WebApi::start(stream_node2);

        // Compile/load contract code (same helper used by other app tests)
        let path_to_code = std::path::PathBuf::from(PACKAGE_DIR).join(PATH_TO_CONTRACT);
        tracing::info!(path = %path_to_code.display(), "Loading contract code");

        // First compile to compute the code hash, then rebuild options with the correct code_key
        let temp_options = PingContractOptions {
            frequency: Duration::from_secs(3),
            ttl: Duration::from_secs(30),
            tag: APP_TAG.to_string(),
            code_key: String::new(),
        };
        let temp_params = Parameters::from(serde_json::to_vec(&temp_options).unwrap());
        let temp_container = common::load_contract(&path_to_code, temp_params)?;
        let code_hash = CodeHash::from_code(temp_container.data());

        let ping_options = PingContractOptions {
            frequency: Duration::from_secs(3),
            ttl: Duration::from_secs(30),
            tag: APP_TAG.to_string(),
            code_key: code_hash.to_string(),
        };
        let params = Parameters::from(serde_json::to_vec(&ping_options).unwrap());
        let container = common::load_contract(&path_to_code, params)?;
        let contract_key = container.key();

        // Gateway puts the contract
        tracing::info!("Gateway node putting contract...");
        let ping = Ping::default();
        let serialized = serde_json::to_vec(&ping)?;
        let wrapped_state = WrappedState::new(serialized);

        client_gw
            .send(ClientRequest::ContractOp(ContractRequest::Put {
                contract: container.clone(),
                state: wrapped_state.clone(),
                related_contracts: RelatedContracts::new(),
                subscribe: config.subscribe_immediately,
            }))
            .await?;

        let key = tokio::time::timeout(
            config.operation_timeout,
            wait_for_put_response(&mut client_gw, &contract_key),
        )
        .await
        .map_err(|_| anyhow!("Gateway put request timed out"))?
        .map_err(anyhow::Error::msg)?;
        tracing::info!(key=%key, "Gateway: put ping contract successfully!");

        // Node1 gets the contract
        tracing::info!("Node 1 getting contract...");
        client_node1
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key: contract_key,
                return_contract_code: true,
                subscribe: config.subscribe_immediately,
            }))
            .await?;

        let node1_state = tokio::time::timeout(
            config.operation_timeout,
            wait_for_get_response(&mut client_node1, &contract_key),
        )
        .await
        .map_err(|_| anyhow!("Node1 get request timed out"))?
        .map_err(anyhow::Error::msg)?;
        tracing::info!("Node 1: got contract with {} entries", node1_state.len());

        // Node2 gets the contract
        tracing::info!("Node 2 getting contract...");
        client_node2
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key: contract_key,
                return_contract_code: true,
                subscribe: config.subscribe_immediately,
            }))
            .await?;

        let node2_state = tokio::time::timeout(
            config.operation_timeout,
            wait_for_get_response(&mut client_node2, &contract_key),
        )
        .await
        .map_err(|_| anyhow!("Node2 get request timed out"))?
        .map_err(anyhow::Error::msg)?;
        tracing::info!("Node 2: got contract with {} entries", node2_state.len());

        // Subscribe to the contract if not already subscribed
        if !config.subscribe_immediately {
            tracing::info!("All nodes subscribing to contract...");

            // Gateway subscribes
            client_gw
                .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
                    key: contract_key,
                    summary: None,
                }))
                .await?;
            tokio::time::timeout(
                config.operation_timeout,
                wait_for_subscribe_response(&mut client_gw, &contract_key),
            )
            .await
            .map_err(|_| anyhow!("Gateway subscribe request timed out"))?
            .map_err(anyhow::Error::msg)?;
            tracing::info!("Gateway: subscribed successfully!");

            // Node1 subscribes
            client_node1
                .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
                    key: contract_key,
                    summary: None,
                }))
                .await?;
            tokio::time::timeout(
                config.operation_timeout,
                wait_for_subscribe_response(&mut client_node1, &contract_key),
            )
            .await
            .map_err(|_| anyhow!("Node1 subscribe request timed out"))?
            .map_err(anyhow::Error::msg)?;
            tracing::info!("Node 1: subscribed successfully!");

            // Node2 subscribes
            client_node2
                .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
                    key: contract_key,
                    summary: None,
                }))
                .await?;
            tokio::time::timeout(
                config.operation_timeout,
                wait_for_subscribe_response(&mut client_node2, &contract_key),
            )
            .await
            .map_err(|_| anyhow!("Node2 subscribe request timed out"))?
            .map_err(anyhow::Error::msg)?;
            tracing::info!("Node 2: subscribed successfully!");
        }

        if config.verbose_logging {
            tracing::info!("Waiting for subscriptions to be fully established...");
            sleep(Duration::from_secs(5)).await;
        }

        // Define tags for each node
        let gw_tag = "ping-from-gw".to_string();
        let node1_tag = "ping-from-node1".to_string();
        let node2_tag = "ping-from-node2".to_string();

        // Track whether nodes have seen each other's updates
        let mut gw_seen_node1 = false;
        let mut gw_seen_node2 = false;
        let mut node1_seen_gw = false;
        let mut node1_seen_node2 = false;
        let mut node2_seen_gw = false;
        let mut node2_seen_node1 = false;

        // Initial updates from each node
        tracing::info!("Sending initial updates from each node...");

        // Gateway sends update
        let mut gw_ping = Ping::default();
        gw_ping.insert(gw_tag.clone());
        tracing::info!("Gateway sending update with tag: {}", gw_tag);
        client_gw
            .send(ClientRequest::ContractOp(ContractRequest::Update {
                key: contract_key,
                data: UpdateData::Delta(StateDelta::from(serde_json::to_vec(&gw_ping).unwrap())),
            }))
            .await?;

        // Node1 sends update
        let mut node1_ping = Ping::default();
        node1_ping.insert(node1_tag.clone());
        tracing::info!("Node 1 sending update with tag: {}", node1_tag);
        client_node1
            .send(ClientRequest::ContractOp(ContractRequest::Update {
                key: contract_key,
                data: UpdateData::Delta(StateDelta::from(serde_json::to_vec(&node1_ping).unwrap())),
            }))
            .await?;

        // Node2 sends update
        let mut node2_ping = Ping::default();
        node2_ping.insert(node2_tag.clone());
        tracing::info!("Node 2 sending update with tag: {}", node2_tag);
        client_node2
            .send(ClientRequest::ContractOp(ContractRequest::Update {
                key: contract_key,
                data: UpdateData::Delta(StateDelta::from(serde_json::to_vec(&node2_ping).unwrap())),
            }))
            .await?;

        // Wait for initial updates to propagate
        tracing::info!("Waiting for initial updates to propagate...");
        sleep(Duration::from_secs(5)).await;

        // Regular checks at intervals if enabled
        if let Some(interval) = config.check_interval {
            for i in 1..=5 {
                let (state_gw, state_node1, state_node2) = get_all_ping_states(
                    &mut client_gw,
                    &mut client_node1,
                    &mut client_node2,
                    contract_key,
                )
                .await?;

                let gw_seen_node1_now = state_gw.contains_key(&node1_tag);
                let gw_seen_node2_now = state_gw.contains_key(&node2_tag);
                let node1_seen_gw_now = state_node1.contains_key(&gw_tag);
                let node1_seen_node2_now = state_node1.contains_key(&node2_tag);
                let node2_seen_gw_now = state_node2.contains_key(&gw_tag);
                let node2_seen_node1_now = state_node2.contains_key(&node1_tag);

                gw_seen_node1 |= gw_seen_node1_now;
                gw_seen_node2 |= gw_seen_node2_now;
                node1_seen_gw |= node1_seen_gw_now;
                node1_seen_node2 |= node1_seen_node2_now;
                node2_seen_gw |= node2_seen_gw_now;
                node2_seen_node1 |= node2_seen_node1_now;

                tracing::info!("Check {}: Update propagation status:", i);
                tracing::info!("Gateway state: {:?}", state_gw);
                tracing::info!("Node 1 state: {:?}", state_node1);
                tracing::info!("Node 2 state: {:?}", state_node2);
                tracing::info!("Gateway seen Node1: {}", gw_seen_node1);
                tracing::info!("Gateway seen Node2: {}", gw_seen_node2);
                tracing::info!("Node1 seen Gateway: {}", node1_seen_gw);
                tracing::info!("Node1 seen Node2: {}", node1_seen_node2);
                tracing::info!("Node2 seen Gateway: {}", node2_seen_gw);
                tracing::info!("Node2 seen Node1: {}", node2_seen_node1);

                if gw_seen_node1
                    && gw_seen_node2
                    && node1_seen_gw
                    && node1_seen_node2
                    && node2_seen_gw
                    && node2_seen_node1
                {
                    tracing::info!("All updates have propagated after {} checks!", i);
                    break;
                }

                sleep(interval).await;

                if i == 3 && config.send_refresh_updates {
                    tracing::info!(
                        "Some updates still missing after {} checks, sending refresh updates...",
                        i
                    );

                    let mut gw_ping_refresh = Ping::default();
                    let gw_refresh_tag = format!("{gw_tag}-refresh");
                    gw_ping_refresh.insert(gw_refresh_tag.clone());
                    tracing::info!("Gateway sending refresh update: {}", gw_refresh_tag);
                    client_gw
                        .send(ClientRequest::ContractOp(ContractRequest::Update {
                            key: contract_key,
                            data: UpdateData::Delta(StateDelta::from(
                                serde_json::to_vec(&gw_ping_refresh).unwrap(),
                            )),
                        }))
                        .await?;

                    let mut node1_ping_refresh = Ping::default();
                    let node1_refresh_tag = format!("{node1_tag}-refresh");
                    node1_ping_refresh.insert(node1_refresh_tag.clone());
                    tracing::info!("Node1 sending refresh update: {}", node1_refresh_tag);
                    client_node1
                        .send(ClientRequest::ContractOp(ContractRequest::Update {
                            key: contract_key,
                            data: UpdateData::Delta(StateDelta::from(
                                serde_json::to_vec(&node1_ping_refresh).unwrap(),
                            )),
                        }))
                        .await?;

                    let mut node2_ping_refresh = Ping::default();
                    let node2_refresh_tag = format!("{node2_tag}-refresh");
                    node2_ping_refresh.insert(node2_refresh_tag.clone());
                    tracing::info!("Node2 sending refresh update: {}", node2_refresh_tag);
                    client_node2
                        .send(ClientRequest::ContractOp(ContractRequest::Update {
                            key: contract_key,
                            data: UpdateData::Delta(StateDelta::from(
                                serde_json::to_vec(&node2_ping_refresh).unwrap(),
                            )),
                        }))
                        .await?;
                }
            }
        } else {
            // Multi-round updates
            for i in 1..=config.update_rounds {
                // Gateway update
                let mut gw_ping_refresh = Ping::default();
                let gw_refresh_tag = format!("{gw_tag}-round{i}");
                gw_ping_refresh.insert(gw_refresh_tag.clone());
                tracing::info!("Gateway sending round {} update: {}", i, gw_refresh_tag);
                client_gw
                    .send(ClientRequest::ContractOp(ContractRequest::Update {
                        key: contract_key,
                        data: UpdateData::Delta(StateDelta::from(
                            serde_json::to_vec(&gw_ping_refresh).unwrap(),
                        )),
                    }))
                    .await?;

                // Node1 update
                let mut node1_ping_refresh = Ping::default();
                let node1_refresh_tag = format!("{node1_tag}-round{i}");
                node1_ping_refresh.insert(node1_refresh_tag.clone());
                tracing::info!("Node1 sending round {} update: {}", i, node1_refresh_tag);
                client_node1
                    .send(ClientRequest::ContractOp(ContractRequest::Update {
                        key: contract_key,
                        data: UpdateData::Delta(StateDelta::from(
                            serde_json::to_vec(&node1_ping_refresh).unwrap(),
                        )),
                    }))
                    .await?;

                // Node2 update
                let mut node2_ping_refresh = Ping::default();
                let node2_refresh_tag = format!("{node2_tag}-round{i}");
                node2_ping_refresh.insert(node2_refresh_tag.clone());
                tracing::info!("Node2 sending round {} update: {}", i, node2_refresh_tag);
                client_node2
                    .send(ClientRequest::ContractOp(ContractRequest::Update {
                        key: contract_key,
                        data: UpdateData::Delta(StateDelta::from(
                            serde_json::to_vec(&node2_ping_refresh).unwrap(),
                        )),
                    }))
                    .await?;

                sleep(config.update_wait).await;
            }

            tracing::info!(
                "Waiting for updates to propagate ({:?})...",
                config.propagation_wait
            );
            sleep(config.propagation_wait).await;
        }

        // Check states after updates
        let (state_gw, state_node1, state_node2) = get_all_ping_states(
            &mut client_gw,
            &mut client_node1,
            &mut client_node2,
            contract_key,
        )
        .await?;

        // Check if updates have been seen
        gw_seen_node1 = gw_seen_node1 || state_gw.contains_key(&node1_tag);
        gw_seen_node2 = gw_seen_node2 || state_gw.contains_key(&node2_tag);
        node1_seen_gw = node1_seen_gw || state_node1.contains_key(&gw_tag);
        node1_seen_node2 = node1_seen_node2 || state_node1.contains_key(&node2_tag);
        node2_seen_gw = node2_seen_gw || state_node2.contains_key(&gw_tag);
        node2_seen_node1 = node2_seen_node1 || state_node2.contains_key(&node1_tag);

        tracing::info!("Update propagation status:");
        tracing::info!("Gateway state: {:?}", state_gw);
        tracing::info!("Node 1 state: {:?}", state_node1);
        tracing::info!("Node 2 state: {:?}", state_node2);
        tracing::info!("Gateway seen Node1: {}", gw_seen_node1);
        tracing::info!("Gateway seen Node2: {}", gw_seen_node2);
        tracing::info!("Node1 seen Gateway: {}", node1_seen_gw);
        tracing::info!("Node1 seen Node2: {}", node1_seen_node2);
        tracing::info!("Node2 seen Gateway: {}", node2_seen_gw);
        tracing::info!("Node2 seen Node1: {}", node2_seen_node1);

        // Check if we need final updates when propagation is incomplete
        if config.send_final_updates
            && (!gw_seen_node1
                || !gw_seen_node2
                || !node1_seen_gw
                || !node1_seen_node2
                || !node2_seen_gw
                || !node2_seen_node1)
        {
            tracing::info!("Some updates still missing, sending final round of updates...");

            // Gateway final update
            let mut gw_ping_final = Ping::default();
            let gw_final_tag = format!("{gw_tag}-final");
            gw_ping_final.insert(gw_final_tag.clone());
            tracing::info!("Gateway sending final update: {}", gw_final_tag);
            client_gw
                .send(ClientRequest::ContractOp(ContractRequest::Update {
                    key: contract_key,
                    data: UpdateData::Delta(StateDelta::from(
                        serde_json::to_vec(&gw_ping_final).unwrap(),
                    )),
                }))
                .await?;

            // Node1 final update
            let mut node1_ping_final = Ping::default();
            let node1_final_tag = format!("{node1_tag}-final");
            node1_ping_final.insert(node1_final_tag.clone());
            tracing::info!("Node1 sending final update: {}", node1_final_tag);
            client_node1
                .send(ClientRequest::ContractOp(ContractRequest::Update {
                    key: contract_key,
                    data: UpdateData::Delta(StateDelta::from(
                        serde_json::to_vec(&node1_ping_final).unwrap(),
                    )),
                }))
                .await?;

            // Node2 final update
            let mut node2_ping_final = Ping::default();
            let node2_final_tag = format!("{node2_tag}-final");
            node2_ping_final.insert(node2_final_tag.clone());
            tracing::info!("Node2 sending final update: {}", node2_final_tag);
            client_node2
                .send(ClientRequest::ContractOp(ContractRequest::Update {
                    key: contract_key,
                    data: UpdateData::Delta(StateDelta::from(
                        serde_json::to_vec(&node2_ping_final).unwrap(),
                    )),
                }))
                .await?;

            tracing::info!("Waiting for final updates to propagate (15 seconds)...");
            sleep(Duration::from_secs(15)).await;

            // Check final states
            let (state_gw, state_node1, state_node2) = get_all_ping_states(
                &mut client_gw,
                &mut client_node1,
                &mut client_node2,
                contract_key,
            )
            .await?;

            // Check if updates have been seen (including final tags)
            gw_seen_node1 = gw_seen_node1
                || state_gw.contains_key(&node1_tag)
                || state_gw.contains_key(&node1_final_tag);
            gw_seen_node2 = gw_seen_node2
                || state_gw.contains_key(&node2_tag)
                || state_gw.contains_key(&node2_final_tag);
            node1_seen_gw = node1_seen_gw
                || state_node1.contains_key(&gw_tag)
                || state_node1.contains_key(&gw_final_tag);
            node1_seen_node2 = node1_seen_node2
                || state_node1.contains_key(&node2_tag)
                || state_node1.contains_key(&node2_final_tag);
            node2_seen_gw = node2_seen_gw
                || state_node2.contains_key(&gw_tag)
                || state_node2.contains_key(&gw_final_tag);
            node2_seen_node1 = node2_seen_node1
                || state_node2.contains_key(&node1_tag)
                || state_node2.contains_key(&node1_final_tag);

            tracing::info!("Final update propagation status:");
            tracing::info!("Gateway state: {:?}", state_gw);
            tracing::info!("Node 1 state: {:?}", state_node1);
            tracing::info!("Node 2 state: {:?}", state_node2);
            tracing::info!("Gateway seen Node1: {}", gw_seen_node1);
            tracing::info!("Gateway seen Node2: {}", gw_seen_node2);
            tracing::info!("Node1 seen Gateway: {}", node1_seen_gw);
            tracing::info!("Node1 seen Node2: {}", node1_seen_node2);
            tracing::info!("Node2 seen Gateway: {}", node2_seen_gw);
            tracing::info!("Node2 seen Node1: {}", node2_seen_node1);
        }

        // Verify that all updates have been seen
        assert!(gw_seen_node1, "Gateway did not see Node1's update");
        assert!(gw_seen_node2, "Gateway did not see Node2's update");
        assert!(node1_seen_gw, "Node1 did not see Gateway's update");
        assert!(
            node1_seen_node2,
            "Node1 did not see Node2's update through Gateway"
        );
        assert!(node2_seen_gw, "Node2 did not see Gateway's update");
        assert!(
            node2_seen_node1,
            "Node2 did not see Node1's update through Gateway"
        );

        // Verify that all nodes have consistent states
        assert!(
            ping_states_equal(&state_gw, &state_node1),
            "Gateway and Node1 have different state content"
        );
        assert!(
            ping_states_equal(&state_gw, &state_node2),
            "Gateway and Node2 have different state content"
        );
        assert!(
            ping_states_equal(&state_node1, &state_node2),
            "Node1 and Node2 have different state content"
        );

        tracing::info!("All nodes have successfully received updates through the gateway!");
        tracing::info!(
            "Test passed: updates propagated correctly despite blocked direct connections"
        );

        Ok::<_, anyhow::Error>(())
    })
    .instrument(span);

    // Wait for completion or error
    select! {
        gw = gateway_node => {
            let Err(gw) = gw;
            Err(anyhow!("Gateway node failed: {}", gw).into())
        }
        n1 = node1 => {
            let Err(n1) = n1;
            Err(anyhow!("Node 1 failed: {}", n1).into())
        }
        n2 = node2 => {
            let Err(n2) = n2;
            Err(anyhow!("Node 2 failed: {}", n2).into())
        }
        t = test => {
            match t {
                Ok(Ok(())) => {
                    tracing::info!("Test completed successfully!");
                    Ok(())
                }
                Ok(Err(e)) => {
                    tracing::error!("Test failed: {}", e);
                    Err(e.into())
                }
                Err(_) => {
                    tracing::error!("Test timed out!");
                    Err(anyhow!("Test timed out").into())
                }
            }
        }
    }
}

/// Standard blocked peers test (baseline)
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_ping_blocked_peers() -> TestResult {
    run_blocked_peers_test(BlockedPeersConfig {
        test_name: "baseline",
        initial_wait: Duration::from_secs(10),
        operation_timeout: Duration::from_secs(20),
        update_rounds: 3,
        update_wait: Duration::from_secs(5),
        propagation_wait: Duration::from_secs(8),
        verbose_logging: false,
        check_interval: None,
        send_refresh_updates: false,
        send_final_updates: true,
        subscribe_immediately: false,
    })
    .await
}

/// Simple blocked peers test
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_ping_blocked_peers_simple() -> TestResult {
    run_blocked_peers_test(BlockedPeersConfig {
        test_name: "simple",
        initial_wait: Duration::from_secs(10),
        operation_timeout: Duration::from_secs(15),
        update_rounds: 1, // Only one round of updates
        update_wait: Duration::from_secs(3),
        propagation_wait: Duration::from_secs(10), // Longer wait for simpler flow
        verbose_logging: false,
        check_interval: None,
        send_refresh_updates: false,
        send_final_updates: false,
        subscribe_immediately: true,
    })
    .await
}

// Note: Redundant tests (optimized, improved, debug, reliable) were removed
// as they only varied in non-functional aspects like timeouts and logging

/// Solution/reference implementation for blocked peers
#[tokio::test(flavor = "multi_thread")]
#[ignore = "fix me"]
async fn test_ping_blocked_peers_solution() -> TestResult {
    run_blocked_peers_test(BlockedPeersConfig {
        test_name: "solution",
        initial_wait: Duration::from_secs(12),
        operation_timeout: Duration::from_secs(25),
        update_rounds: 2,
        update_wait: Duration::from_secs(4),
        propagation_wait: Duration::from_secs(12),
        verbose_logging: false,
        check_interval: Some(Duration::from_secs(3)), // Regular check intervals
        send_refresh_updates: true,
        send_final_updates: true,
        subscribe_immediately: true,
    })
    .await
}
