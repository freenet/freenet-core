mod common;

use std::{net::TcpListener, path::PathBuf, time::Duration};

use anyhow::anyhow;
use freenet::{local_node::NodeConfig, server::serve_gateway};
use freenet_ping_types::{Ping, PingContractOptions};
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, ContractResponse, HostResponse, WebApi},
    prelude::*,
};
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use testresult::TestResult;
use tokio::{select, time::sleep, time::timeout};
use tokio_tungstenite::connect_async;
use tracing::{level_filters::LevelFilter, span, Instrument, Level};

use common::{base_node_test_config, gw_config_from_path, APP_TAG, PACKAGE_DIR, PATH_TO_CONTRACT};
use freenet_ping_app::ping_client::{
    run_ping_client, wait_for_get_response, wait_for_put_response, wait_for_subscribe_response,
    PingStats,
};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "fix me"]
async fn test_ping_multi_node() -> TestResult {
    freenet::config::set_logger(Some(LevelFilter::DEBUG), None);

    // Setup network sockets for the gateway
    let network_socket_gw = TcpListener::bind("127.0.0.1:0")?;

    // Setup API sockets for all three nodes
    let ws_api_port_socket_gw = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_node1 = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_node2 = TcpListener::bind("127.0.0.1:0")?;

    // Configure gateway node
    let (config_gw, preset_cfg_gw, config_gw_info) = {
        let (cfg, preset) = base_node_test_config(
            true,
            vec![],
            Some(network_socket_gw.local_addr()?.port()),
            ws_api_port_socket_gw.local_addr()?.port(),
            "gw_multi_node", // data_dir_suffix
            None,            // base_tmp_dir
            None,            // blocked_addresses
        )
        .await?;
        let public_port = cfg.network_api.public_port.unwrap();
        let path = preset.temp_dir.path().to_path_buf();
        (cfg, preset, gw_config_from_path(public_port, &path)?)
    };
    let ws_api_port_gw = config_gw.ws_api.ws_api_port.unwrap();

    // Configure client node 1
    let (config_node1, preset_cfg_node1) = base_node_test_config(
        false,
        vec![serde_json::to_string(&config_gw_info)?],
        None,
        ws_api_port_socket_node1.local_addr()?.port(),
        "node1_multi_node", // data_dir_suffix
        None,               // base_tmp_dir
        None,               // blocked_addresses
    )
    .await?;
    let ws_api_port_node1 = config_node1.ws_api.ws_api_port.unwrap();

    // Configure client node 2
    let (config_node2, preset_cfg_node2) = base_node_test_config(
        false,
        vec![serde_json::to_string(&config_gw_info)?],
        None,
        ws_api_port_socket_node2.local_addr()?.port(),
        "node2_multi_node", // data_dir_suffix
        None,               // base_tmp_dir
        None,               // blocked_addresses
    )
    .await?;
    let ws_api_port_node2 = config_node2.ws_api.ws_api_port.unwrap();

    // Log data directories for debugging
    tracing::info!("Gateway node data dir: {:?}", preset_cfg_gw.temp_dir.path());
    tracing::info!("Node 1 data dir: {:?}", preset_cfg_node1.temp_dir.path());
    tracing::info!("Node 2 data dir: {:?}", preset_cfg_node2.temp_dir.path());

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
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Start client node 1
    let node1 = async move {
        let config = config_node1.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Start client node 2
    let node2 = async {
        let config = config_node2.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Main test logic
    let test = tokio::time::timeout(Duration::from_secs(120), async {
        // Wait for nodes to start up
        tokio::time::sleep(Duration::from_secs(10)).await;

        // Connect to all three nodes
        let uri_gw = format!(
            "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
            ws_api_port_gw
        );
        let uri_node1 = format!(
            "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
            ws_api_port_node1
        );
        let uri_node2 = format!(
            "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
            ws_api_port_node2
        );

        let (stream_gw, _) = connect_async(&uri_gw).await?;
        let (stream_node1, _) = connect_async(&uri_node1).await?;
        let (stream_node2, _) = connect_async(&uri_node2).await?;

        let mut client_gw = WebApi::start(stream_gw);
        let mut client_node1 = WebApi::start(stream_node1);
        let mut client_node2 = WebApi::start(stream_node2);

        // FIXME: this is error prone, rebuild the contract each time there are changes in the code
        // (add a build.rs script to the contracts/ping crate)
        let path_to_code = PathBuf::from(PACKAGE_DIR).join(PATH_TO_CONTRACT);
        tracing::info!(path=%path_to_code.display(), "loading contract code");
        let code = std::fs::read(path_to_code)
            .ok()
            .ok_or_else(|| anyhow!("Failed to read contract code"))?;
        let code_hash = CodeHash::from_code(&code);
        tracing::info!(code_hash=%code_hash, "loaded contract code");

        // Load the ping contrac
        let ping_options = PingContractOptions {
            frequency: Duration::from_secs(5),
            ttl: Duration::from_secs(30),
            tag: APP_TAG.to_string(),
            code_key: code_hash.to_string(),
        };
        let params = Parameters::from(serde_json::to_vec(&ping_options).unwrap());
        let container = ContractContainer::try_from((code, &params))?;
        let contract_key = container.key();

        // Step 1: Gateway node puts the contrac
        tracing::info!("Gateway node putting contract...");
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
        tracing::info!(key=%key, "Gateway: put ping contract successfully!");

        // Step 2: Node 1 gets the contrac
        tracing::info!("Node 1 getting contract...");
        client_node1
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key: contract_key,
                return_contract_code: true,
                subscribe: false,
            }))
            .await?;

        // Wait for get response on node 1
        let node1_state = wait_for_get_response(&mut client_node1, &contract_key)
            .await
            .map_err(anyhow::Error::msg)?;
        tracing::info!("Node 1: got contract with {} entries", node1_state.len());

        // Step 3: Node 2 gets the contrac
        tracing::info!("Node 2 getting contract...");
        client_node2
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key: contract_key,
                return_contract_code: true,
                subscribe: false,
            }))
            .await?;

        // Wait for get response on node 2
        let node2_state = wait_for_get_response(&mut client_node2, &contract_key)
            .await
            .map_err(anyhow::Error::msg)?;
        tracing::info!("Node 2: got contract with {} entries", node2_state.len());

        // Step 4: All nodes subscribe to the contrac
        tracing::info!("All nodes subscribing to contract...");

        // Gateway subscribes
        client_gw
            .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
                key: contract_key,
                summary: None,
            }))
            .await?;
        wait_for_subscribe_response(&mut client_gw, &contract_key)
            .await
            .map_err(anyhow::Error::msg)?;
        tracing::info!("Gateway: subscribed successfully!");

        // Node 1 subscribes
        client_node1
            .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
                key: contract_key,
                summary: None,
            }))
            .await?;
        wait_for_subscribe_response(&mut client_node1, &contract_key)
            .await
            .map_err(anyhow::Error::msg)?;
        tracing::info!("Node 1: subscribed successfully!");

        // Node 2 subscribes
        client_node2
            .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
                key: contract_key,
                summary: None,
            }))
            .await?;
        wait_for_subscribe_response(&mut client_node2, &contract_key)
            .await
            .map_err(anyhow::Error::msg)?;
        tracing::info!("Node 2: subscribed successfully!");

        // Step 5: All nodes send multiple updates to build history for eventual consistency testing

        // Create different tags for each node
        let gw_tag = "ping-from-gw".to_string();
        let node1_tag = "ping-from-node1".to_string();
        let node2_tag = "ping-from-node2".to_string();

        // Each node will send multiple pings to build history
        let ping_rounds = 5;
        tracing::info!("Each node will send {} pings to build history", ping_rounds);

        for round in 1..=ping_rounds {
            // Gateway sends update with its tag
            let mut gw_ping = Ping::default();
            gw_ping.insert(gw_tag.clone());
            tracing::info!("Gateway sending update with tag: {} (round {})", gw_tag, round);
            client_gw
                .send(ClientRequest::ContractOp(ContractRequest::Update {
                    key: contract_key,
                    data: UpdateData::Delta(StateDelta::from(serde_json::to_vec(&gw_ping).unwrap())),
                }))
                .await?;

            // Node 1 sends update with its tag
            let mut node1_ping = Ping::default();
            node1_ping.insert(node1_tag.clone());
            tracing::info!("Node 1 sending update with tag: {} (round {})", node1_tag, round);
            client_node1
                .send(ClientRequest::ContractOp(ContractRequest::Update {
                    key: contract_key,
                    data: UpdateData::Delta(StateDelta::from(serde_json::to_vec(&node1_ping).unwrap())),
                }))
                .await?;

            // Node 2 sends update with its tag
            let mut node2_ping = Ping::default();
            node2_ping.insert(node2_tag.clone());
            tracing::info!("Node 2 sending update with tag: {} (round {})", node2_tag, round);
            client_node2
                .send(ClientRequest::ContractOp(ContractRequest::Update {
                    key: contract_key,
                    data: UpdateData::Delta(StateDelta::from(serde_json::to_vec(&node2_ping).unwrap())),
                }))
                .await?;

            // Small delay between rounds to ensure distinct timestamps
            sleep(Duration::from_millis(200)).await;
        }

        // Wait for updates to propagate across the network - longer wait to ensure eventual consistency
        tracing::info!("Waiting for updates to propagate across the network...");
        sleep(Duration::from_secs(30)).await;

        // Request the current state from all nodes
        tracing::info!("Querying all nodes for current state...");

        client_gw
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key: contract_key,
                return_contract_code: false,
                subscribe: false,
            }))
            .await?;

        client_node1
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key: contract_key,
                return_contract_code: false,
                subscribe: false,
            }))
            .await?;

        client_node2
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key: contract_key,
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
        tracing::info!("Gateway final state: {}", final_state_gw);
        tracing::info!("Node 1 final state: {}", final_state_node1);
        tracing::info!("Node 2 final state: {}", final_state_node2);

        // Show detailed comparison of ping history per tag
        tracing::info!("===== Detailed comparison of ping history =====");

        let tags = vec![gw_tag.clone(), node1_tag.clone(), node2_tag.clone()];
        let mut all_histories_match = true;

        for tag in &tags {
            tracing::info!("Checking history for tag '{}':", tag);

            // Get the vector of timestamps for this tag from each node
            let gw_history = final_state_gw.get(tag).cloned().unwrap_or_default();
            let node1_history = final_state_node1.get(tag).cloned().unwrap_or_default();
            let node2_history = final_state_node2.get(tag).cloned().unwrap_or_default();

            // Histories should be non-empty if eventual consistency worked
            if gw_history.is_empty() || node1_history.is_empty() || node2_history.is_empty() {
                tracing::warn!("⚠️ Tag '{}' missing from one or more nodes!", tag);
                all_histories_match = false;
                continue;
            }

            // Log the number of entries in each history
            tracing::info!("  - Gateway: {} entries", gw_history.len());
            tracing::info!("  - Node 1:  {} entries", node1_history.len());
            tracing::info!("  - Node 2:  {} entries", node2_history.len());

            // Check if the histories have the same length
            if gw_history.len() != node1_history.len() || gw_history.len() != node2_history.len() {
                tracing::warn!("⚠️ Different number of history entries for tag '{}'!", tag);
                all_histories_match = false;
                continue;
            }

            // Compare the actual timestamp vectors element by elemen
            let mut timestamps_match = true;
            for i in 0..gw_history.len() {
                if gw_history[i] != node1_history[i] || gw_history[i] != node2_history[i] {
                    timestamps_match = false;
                    tracing::warn!(
                        "⚠️ Timestamp mismatch at position {}:\n  - Gateway: {}\n  - Node 1:  {}\n  - Node 2:  {}",
                        i, gw_history[i], node1_history[i], node2_history[i]
                    );
                }
            }

            if timestamps_match {
                tracing::info!("  ✅ History for tag '{}' is identical across all nodes!", tag);
            } else {
                tracing::warn!("  ⚠️ History timestamps for tag '{}' differ between nodes!", tag);
                all_histories_match = false;
            }
        }

        tracing::info!("=================================================");

        // Final assertion for eventual consistency
        assert!(
            all_histories_match,
            "Eventual consistency test failed: Ping histories are not identical across all nodes"
        );

        tracing::info!("✅ Eventual consistency test PASSED - all nodes have identical ping histories!");

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

#[tokio::test(flavor = "multi_thread")]
#[ignore = "fix me"]
async fn test_ping_application_loop() -> TestResult {
    freenet::config::set_logger(Some(LevelFilter::DEBUG), None);

    // Setup network sockets for the gateway
    let network_socket_gw = TcpListener::bind("127.0.0.1:0")?;

    // Setup API sockets for all three nodes
    let ws_api_port_socket_gw = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_node1 = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_node2 = TcpListener::bind("127.0.0.1:0")?;

    // Configure gateway node
    let (config_gw, preset_cfg_gw, config_gw_info) = {
        let (cfg, preset) = base_node_test_config(
            true,
            vec![],
            Some(network_socket_gw.local_addr()?.port()),
            ws_api_port_socket_gw.local_addr()?.port(),
            "gw_app_loop", // data_dir_suffix
            None,          // base_tmp_dir
            None,          // blocked_addresses
        )
        .await?;
        let public_port = cfg.network_api.public_port.unwrap();
        let path = preset.temp_dir.path().to_path_buf();
        (cfg, preset, gw_config_from_path(public_port, &path)?)
    };
    let ws_api_port_gw = config_gw.ws_api.ws_api_port.unwrap();

    // Configure client node 1
    let (config_node1, preset_cfg_node1) = base_node_test_config(
        false,
        vec![serde_json::to_string(&config_gw_info)?],
        None,
        ws_api_port_socket_node1.local_addr()?.port(),
        "node1_app_loop", // data_dir_suffix
        None,             // base_tmp_dir
        None,             // blocked_addresses
    )
    .await?;
    let ws_api_port_node1 = config_node1.ws_api.ws_api_port.unwrap();

    // Configure client node 2
    let (config_node2, preset_cfg_node2) = base_node_test_config(
        false,
        vec![serde_json::to_string(&config_gw_info)?],
        None,
        ws_api_port_socket_node2.local_addr()?.port(),
        "node2_app_loop", // data_dir_suffix
        None,             // base_tmp_dir
        None,             // blocked_addresses
    )
    .await?;
    let ws_api_port_node2 = config_node2.ws_api.ws_api_port.unwrap();

    // Log data directories for debugging
    tracing::info!("Gateway node data dir: {:?}", preset_cfg_gw.temp_dir.path());
    tracing::info!("Node 1 data dir: {:?}", preset_cfg_node1.temp_dir.path());
    tracing::info!("Node 2 data dir: {:?}", preset_cfg_node2.temp_dir.path());

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
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Start client node 1
    let node1 = async move {
        let config = config_node1.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Start client node 2
    let node2 = async {
        let config = config_node2.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Main test logic
    let test = tokio::time::timeout(Duration::from_secs(180), async {
        // Wait for nodes to start up
        tokio::time::sleep(Duration::from_secs(10)).await;

        // Connect to all three nodes
        let uri_gw = format!(
            "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
            ws_api_port_gw
        );
        let uri_node1 = format!(
            "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
            ws_api_port_node1
        );
        let uri_node2 = format!(
            "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
            ws_api_port_node2
        );

        let (stream_gw, _) = connect_async(&uri_gw).await?;
        let (stream_node1, _) = connect_async(&uri_node1).await?;
        let (stream_node2, _) = connect_async(&uri_node2).await?;

        let mut client_gw = WebApi::start(stream_gw);
        let mut client_node1 = WebApi::start(stream_node1);
        let mut client_node2 = WebApi::start(stream_node2);

        // Load the ping contrac
        let path_to_code = PathBuf::from(PACKAGE_DIR).join(PATH_TO_CONTRACT);
        tracing::info!(path=%path_to_code.display(), "loading contract code");
        let code = std::fs::read(path_to_code)
            .ok()
            .ok_or_else(|| anyhow!("Failed to read contract code"))?;
        let code_hash = CodeHash::from_code(&code);

        // Create ping contract options for each node with different tags
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
        let container = ContractContainer::try_from((code, &params))?;
        let contract_key = container.key();

        // Step 1: Gateway node puts the contrac
        tracing::info!("Gateway node putting contract...");
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
        tracing::info!(key=%key, "Gateway: put ping contract successfully!");

        // Step 2: Node 1 gets the contrac
        tracing::info!("Node 1 getting contract...");
        client_node1
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key: contract_key,
                return_contract_code: true,
                subscribe: false,
            }))
            .await?;

        // Wait for get response on node 1
        let node1_state = wait_for_get_response(&mut client_node1, &contract_key)
            .await
            .map_err(anyhow::Error::msg)?;
        tracing::info!("Node 1: got contract with {} entries", node1_state.len());

        // Step 3: Node 2 gets the contrac
        tracing::info!("Node 2 getting contract...");
        client_node2
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key: contract_key,
                return_contract_code: true,
                subscribe: false,
            }))
            .await?;

        // Wait for get response on node 2
        let node2_state = wait_for_get_response(&mut client_node2, &contract_key)
            .await
            .map_err(anyhow::Error::msg)?;
        tracing::info!("Node 2: got contract with {} entries", node2_state.len());

        // Step 4: Subscribe all clients to the contrac
        // Gateway subscribes
        client_gw
            .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
                key: contract_key,
                summary: None,
            }))
            .await?;
        wait_for_subscribe_response(&mut client_gw, &contract_key)
            .await
            .map_err(anyhow::Error::msg)?;
        tracing::info!("Gateway: subscribed successfully!");

        // Node 1 subscribes
        client_node1
            .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
                key: contract_key,
                summary: None,
            }))
            .await?;
        wait_for_subscribe_response(&mut client_node1, &contract_key)
            .await
            .map_err(anyhow::Error::msg)?;
        tracing::info!("Node 1: subscribed successfully!");

        // Node 2 subscribes
        client_node2
            .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
                key: contract_key,
                summary: None,
            }))
            .await?;
        wait_for_subscribe_response(&mut client_node2, &contract_key)
            .await
            .map_err(anyhow::Error::msg)?;
        tracing::info!("Node 2: subscribed successfully!");

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
                assert!(*count > 0, "{} received no pings from {}", name, source);
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

#[tokio::test(flavor = "multi_thread")]
#[ignore = "Test has never worked - gateway nodes fail on startup with channel closed errors"]
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
    for i in 0..NUM_REGULAR_NODES {
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
        let data_dir_suffix = format!("gw_{}", i);

        let (cfg, preset) = base_node_test_config(
            true,
            vec![],
            Some(gateway_sockets[i].local_addr()?.port()),
            ws_api_gateway_sockets[i].local_addr()?.port(),
            &data_dir_suffix,
            None, // base_tmp_dir
            None, // No blocked addresses for gateways
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

    for i in 0..NUM_REGULAR_NODES {
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

        let data_dir_suffix = format!("node_{}", i);

        let (cfg, preset) = base_node_test_config(
            false,
            serialized_gateways.clone(), // All nodes connect to all gateways
            None,
            ws_api_node_sockets[i].local_addr()?.port(),
            &data_dir_suffix,
            None,
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

    // Start all gateway nodes
    let mut gateway_futures = FuturesUnordered::new();
    for (_i, config) in gateway_configs.into_iter().enumerate() {
        let gateway_future = async move {
            let config = config.build().await?;
            let node = NodeConfig::new(config.clone())
                .await?
                .build(serve_gateway(config.ws_api).await)
                .await?;
            node.run().await
        }
        .boxed_local();
        gateway_futures.push(gateway_future);
    }

    // Start all regular nodes
    let mut regular_node_futures = FuturesUnordered::new();
    for (_i, config) in node_configs.into_iter().enumerate() {
        let regular_node_future = async move {
            let config = config.build().await?;
            let node = NodeConfig::new(config.clone())
                .await?
                .build(serve_gateway(config.ws_api).await)
                .await?;
            node.run().await
        }
        .boxed_local();
        tokio::time::sleep(Duration::from_secs(2)).await;
        regular_node_futures.push(regular_node_future);
    }

    // Create a future that will complete if any gateway fails
    let mut gateway_monitor = gateway_futures.into_future();
    let mut regular_node_monitor = regular_node_futures.into_future();

    let test = tokio::time::timeout(Duration::from_secs(240), async {
        // Wait for nodes to start up
        tracing::info!("Waiting for nodes to start up...");
        tokio::time::sleep(Duration::from_secs(30)).await;
        tracing::info!("Proceeding to connect to nodes...");

        // Connect to all nodes with retry logic
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

        let mut publisher = &mut node_clients[publisher_idx];
        publisher
            .send(ClientRequest::ContractOp(ContractRequest::Put {
                contract: container.clone(),
                state: wrapped_state.clone(),
                related_contracts: RelatedContracts::new(),
                subscribe: false,
            }))
            .await?;

        // Wait for put response on publisher
        let key = wait_for_put_response(publisher, &contract_key)
            .await
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
                    Ok(Ok(_)) => {},
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
        (r, _remaining) = &mut gateway_monitor => {
            if let Some(r) = r {
                match r {
                    Err(err) => return Err(anyhow!("Gateway node failed: {}", err).into()),
                    Ok(_) => panic!("Gateway node unexpectedly terminated successfully"),
                }
            }
        }
        (r, _remaining) = &mut regular_node_monitor => {
            if let Some(r) = r {
                match r {
                    Err(err) => return Err(anyhow!("Regular node failed: {}", err).into()),
                    Ok(_) => panic!("Regular node unexpectedly terminated successfully"),
                }
            }
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
