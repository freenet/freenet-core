use std::{
    net::{Ipv4Addr, TcpListener},
    path::PathBuf,
    time::Duration,
};

use anyhow::anyhow;
use freenet::{
    config::{ConfigArgs, InlineGwConfig, NetworkArgs, SecretArgs, WebsocketApiArgs},
    dev_tool::TransportKeypair,
    local_node::NodeConfig,
    server::serve_gateway,
};
use freenet_ping_types::{Ping, PingContractOptions};
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, WebApi},
    prelude::*,
};
use futures::FutureExt;
use rand::{random, Rng, SeedableRng};
use testresult::TestResult;
use tokio::{select, time::sleep};
use tokio_tungstenite::connect_async;
use tracing::{level_filters::LevelFilter, span, Instrument, Level};

use freenet_ping_app::ping_client::{
    run_ping_client, wait_for_get_response, wait_for_put_response, wait_for_subscribe_response,
    PingStats,
};

static RNG: once_cell::sync::Lazy<std::sync::Mutex<rand::rngs::StdRng>> =
    once_cell::sync::Lazy::new(|| {
        std::sync::Mutex::new(rand::rngs::StdRng::from_seed(
            *b"0102030405060708090a0b0c0d0e0f10",
        ))
    });

struct PresetConfig {
    temp_dir: tempfile::TempDir,
}

async fn base_node_test_config(
    is_gateway: bool,
    gateways: Vec<String>,
    public_port: Option<u16>,
    ws_api_port: u16,
    // New parameter to specify addresses this node should block
    blocked_addresses: Option<Vec<std::net::SocketAddr>>,
) -> anyhow::Result<(ConfigArgs, PresetConfig)> {
    if is_gateway {
        assert!(public_port.is_some());
    }

    let temp_dir = tempfile::tempdir()?;
    let key = TransportKeypair::new_with_rng(&mut *RNG.lock().unwrap());
    let transport_keypair = temp_dir.path().join("private.pem");
    key.save(&transport_keypair)?;
    key.public().save(temp_dir.path().join("public.pem"))?;
    let config = ConfigArgs {
        ws_api: WebsocketApiArgs {
            address: Some(Ipv4Addr::LOCALHOST.into()),
            ws_api_port: Some(ws_api_port),
        },
        network_api: NetworkArgs {
            public_address: Some(Ipv4Addr::LOCALHOST.into()),
            public_port,
            is_gateway,
            skip_load_from_network: true,
            gateways: Some(gateways),
            location: Some(RNG.lock().unwrap().gen()),
            ignore_protocol_checking: true,
            address: Some(Ipv4Addr::LOCALHOST.into()),
            network_port: public_port,
            bandwidth_limit: None,
            // Assuming the new field 'blocked_addresses' is added to NetworkArgs
            // and it takes Option<Vec<SocketAddr>>
            blocked_addresses,
        },
        config_paths: {
            freenet::config::ConfigPathsArgs {
                config_dir: Some(temp_dir.path().to_path_buf()),
                data_dir: Some(temp_dir.path().to_path_buf()),
            }
        },
        secrets: SecretArgs {
            transport_keypair: Some(transport_keypair),
            ..Default::default()
        },
        ..Default::default()
    };
    Ok((config, PresetConfig { temp_dir }))
}

fn gw_config(port: u16, path: &std::path::Path) -> anyhow::Result<InlineGwConfig> {
    Ok(InlineGwConfig {
        address: (Ipv4Addr::LOCALHOST, port).into(),
        location: Some(random()),
        public_key_path: path.join("public.pem"),
    })
}

const PACKAGE_DIR: &str = env!("CARGO_MANIFEST_DIR");
const PATH_TO_CONTRACT: &str = "../contracts/ping/build/freenet/freenet_ping_contract";

const APP_TAG: &str = "ping-app";

#[tokio::test(flavor = "multi_thread")]
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
            None, // blocked_addresses
        )
        .await?;
        let public_port = cfg.network_api.public_port.unwrap();
        let path = preset.temp_dir.path().to_path_buf();
        (cfg, preset, gw_config(public_port, &path)?)
    };
    let ws_api_port_gw = config_gw.ws_api.ws_api_port.unwrap();

    // Configure client node 1
    let (config_node1, preset_cfg_node1) = base_node_test_config(
        false,
        vec![serde_json::to_string(&config_gw_info)?],
        None,
        ws_api_port_socket_node1.local_addr()?.port(),
        None, // blocked_addresses
    )
    .await?;
    let ws_api_port_node1 = config_node1.ws_api.ws_api_port.unwrap();

    // Configure client node 2
    let (config_node2, preset_cfg_node2) = base_node_test_config(
        false,
        vec![serde_json::to_string(&config_gw_info)?],
        None,
        ws_api_port_socket_node2.local_addr()?.port(),
        None, // blocked_addresses
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

        // Load the ping contract
        let ping_options = PingContractOptions {
            frequency: Duration::from_secs(5),
            ttl: Duration::from_secs(30),
            tag: APP_TAG.to_string(),
            code_key: code_hash.to_string(),
        };
        let params = Parameters::from(serde_json::to_vec(&ping_options).unwrap());
        let container = ContractContainer::try_from((code, &params))?;
        let contract_key = container.key();

        // Step 1: Gateway node puts the contract
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

        // Step 2: Node 1 gets the contract
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

        // Step 3: Node 2 gets the contract
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

        // Step 4: All nodes subscribe to the contract
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
            // Compare the actual timestamp vectors element by element
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
            None, // blocked_addresses
        )
        .await?;
        let public_port = cfg.network_api.public_port.unwrap();
        let path = preset.temp_dir.path().to_path_buf();
        (cfg, preset, gw_config(public_port, &path)?)
    };
    let ws_api_port_gw = config_gw.ws_api.ws_api_port.unwrap();

    // Configure client node 1
    let (config_node1, preset_cfg_node1) = base_node_test_config(
        false,
        vec![serde_json::to_string(&config_gw_info)?],
        None,
        ws_api_port_socket_node1.local_addr()?.port(),
        None, // blocked_addresses
    )
    .await?;
    let ws_api_port_node1 = config_node1.ws_api.ws_api_port.unwrap();

    // Configure client node 2
    let (config_node2, preset_cfg_node2) = base_node_test_config(
        false,
        vec![serde_json::to_string(&config_gw_info)?],
        None,
        ws_api_port_socket_node2.local_addr()?.port(),
        None, // blocked_addresses
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

        // Load the ping contract
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

        // Step 1: Gateway node puts the contract
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

        // Step 2: Node 1 gets the contract
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

        // Step 3: Node 2 gets the contract
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

        // Step 4: Subscribe all clients to the contract
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
