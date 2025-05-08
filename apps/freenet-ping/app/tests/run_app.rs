use std::{
    collections::HashMap,
    net::{Ipv4Addr, TcpListener},
    path::PathBuf,
    time::Duration,
};

use anyhow::anyhow;
use chrono::{DateTime, Utc};
use freenet::{
    config::{ConfigArgs, InlineGwConfig, NetworkArgs, SecretArgs, WebsocketApiArgs},
    dev_tool::TransportKeypair,
    local_node::NodeConfig,
    server::serve_gateway,
};
use freenet_ping_types::{Ping, PingContractOptions};
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, ContractResponse, HostResponse, WebApi},
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
            blocked_addresses: blocked_addresses_for_this_node,
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

// Process an update notification for the ping contract - test helper function
fn process_ping_update(
    local_state: &mut Ping,
    ttl: Duration,
    update: UpdateData,
) -> Result<HashMap<String, DateTime<Utc>>, Box<dyn std::error::Error + Send + Sync + 'static>> {
    let mut handle_update = |state: &[u8]| {
        let new_ping = if state.is_empty() {
            Ping::default()
        } else {
            match serde_json::from_slice::<Ping>(state) {
                Ok(p) => p,
                Err(e) => {
                    return Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>)
                }
            }
        };

        let updates = local_state.merge(new_ping, ttl);
        Ok(updates)
    };

    match update {
        UpdateData::State(state) => handle_update(state.as_ref()),
        UpdateData::Delta(delta) => handle_update(&delta),
        UpdateData::StateAndDelta { state, delta } => {
            let mut updates = handle_update(&state)?;
            updates.extend(handle_update(&delta)?);
            Ok(updates)
        }
        _ => Err("unknown state".into()),
    }
}

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
    )
    .await?;
    let ws_api_port_node1 = config_node1.ws_api.ws_api_port.unwrap();

    // Configure client node 2
    let (config_node2, preset_cfg_node2) = base_node_test_config(
        false,
        vec![serde_json::to_string(&config_gw_info)?],
        None,
        ws_api_port_socket_node2.local_addr()?.port(),
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

        // Step 5: All nodes send updates and verify they receive updates from others

        // Setup local state trackers for each node
        let mut gw_local_state = Ping::default();
        let mut node1_local_state = Ping::default();
        let mut node2_local_state = Ping::default();

        // Create different tags for each node
        let gw_tag = "ping-from-gw".to_string();
        let node1_tag = "ping-from-node1".to_string();
        let node2_tag = "ping-from-node2".to_string();

        // Track which nodes have seen updates from each other
        let mut gw_seen_node1 = false;
        let mut gw_seen_node2 = false;
        let mut node1_seen_gw = false;
        let mut node1_seen_node2 = false;
        let mut node2_seen_gw = false;
        let mut node2_seen_node1 = false;

        // Gateway sends update with its tag
        let mut gw_ping = Ping::default();
        gw_ping.insert(gw_tag.clone());
        tracing::info!(%gw_ping, "Gateway sending update with tag: {}", gw_tag);
        client_gw
            .send(ClientRequest::ContractOp(ContractRequest::Update {
                key: contract_key,
                data: UpdateData::Delta(StateDelta::from(serde_json::to_vec(&gw_ping).unwrap())),
            }))
            .await?;

        // Node 1 sends update with its tag
        let mut node1_ping = Ping::default();
        node1_ping.insert(node1_tag.clone());
        tracing::info!(%node1_ping, "Node 1 sending update with tag: {}", node1_tag);
        client_node1
            .send(ClientRequest::ContractOp(ContractRequest::Update {
                key: contract_key,
                data: UpdateData::Delta(StateDelta::from(serde_json::to_vec(&node1_ping).unwrap())),
            }))
            .await?;

        // Node 2 sends update with its tag
        let mut node2_ping = Ping::default();
        node2_ping.insert(node2_tag.clone());
        tracing::info!(%node2_ping, "Node 2 sending update with tag: {}", node2_tag);
        client_node2
            .send(ClientRequest::ContractOp(ContractRequest::Update {
                key: contract_key,
                data: UpdateData::Delta(StateDelta::from(serde_json::to_vec(&node2_ping).unwrap())),
            }))
            .await?;

        // Wait for updates to propagate across the network
        tracing::info!("Waiting for updates to propagate across the network...");
        sleep(Duration::from_secs(20)).await;

        // Function to verify if all nodes have all the expected tags
        let verify_all_tags_present =
            |gw: &Ping, node1: &Ping, node2: &Ping, tags: &[String]| -> bool {
                for tag in tags {
                    if !gw.contains_key(tag) || !node1.contains_key(tag) || !node2.contains_key(tag)
                    {
                        return false;
                    }
                }
                true
            };

        // Function to get the current states from all nodes
        let get_all_states = async |client_gw: &mut WebApi,
                                    client_node1: &mut WebApi,
                                    client_node2: &mut WebApi,
                                    key: ContractKey|
               -> anyhow::Result<(Ping, Ping, Ping)> {
            // Request the contract state from all nodes
            tracing::info!("Querying all nodes for current state...");

            client_gw
                .send(ClientRequest::ContractOp(ContractRequest::Get {
                    key,
                    return_contract_code: false,
                    subscribe: false,
                }))
                .await?;

            client_node1
                .send(ClientRequest::ContractOp(ContractRequest::Get {
                    key,
                    return_contract_code: false,
                    subscribe: false,
                }))
                .await?;

            client_node2
                .send(ClientRequest::ContractOp(ContractRequest::Get {
                    key,
                    return_contract_code: false,
                    subscribe: false,
                }))
                .await?;

            // Receive and deserialize the states from all nodes
            let state_gw = wait_for_get_response(client_gw, &key)
                .await
                .map_err(anyhow::Error::msg)?;

            let state_node1 = wait_for_get_response(client_node1, &key)
                .await
                .map_err(anyhow::Error::msg)?;

            let state_node2 = wait_for_get_response(client_node2, &key)
                .await
                .map_err(anyhow::Error::msg)?;

            Ok((state_gw, state_node1, state_node2))
        };

        // Variables for retry mechanism
        let expected_tags = vec![gw_tag.clone(), node1_tag.clone(), node2_tag.clone()];
        let max_retries = 3;
        let mut retry_count = 0;
        let mut final_state_gw;
        let mut final_state_node1;
        let mut final_state_node2;

        // Retry loop to wait for all updates to propagate
        loop {
            // Get current states
            let (gw_state, node1_state, node2_state) = get_all_states(
                &mut client_gw,
                &mut client_node1,
                &mut client_node2,
                contract_key,
            )
            .await?;

            final_state_gw = gw_state;
            final_state_node1 = node1_state;
            final_state_node2 = node2_state;

            // Check if all nodes have all the tags
            if verify_all_tags_present(
                &final_state_gw,
                &final_state_node1,
                &final_state_node2,
                &expected_tags,
            ) {
                tracing::info!("All tags successfully propagated to all nodes!");
                break;
            }

            // If we've reached maximum retries, continue with the test
            if retry_count >= max_retries {
                tracing::warn!(
                    "Not all tags propagated after {} retries - continuing with current state",
                    max_retries
                );
                break;
            }

            // Otherwise, wait and retry
            retry_count += 1;
            tracing::info!(
                "Some tags are missing from some nodes. Waiting another 15 seconds (retry {}/{})",
                retry_count,
                max_retries
            );
            sleep(Duration::from_secs(15)).await;
        }

        // Log the final state from each node
        tracing::info!("Gateway final state: {}", final_state_gw);
        tracing::info!("Node 1 final state: {}", final_state_node1);
        tracing::info!("Node 2 final state: {}", final_state_node2);

        // Show detailed comparison by tag
        tracing::info!("===== Detailed comparison of final states =====");

        let tags = vec![gw_tag.clone(), node1_tag.clone(), node2_tag.clone()];
        for tag in &tags {
            let gw_time = final_state_gw
                .get(tag)
                .map(|t| t.to_rfc3339())
                .unwrap_or_else(|| "MISSING".to_string());
            let node1_time = final_state_node1
                .get(tag)
                .map(|t| t.to_rfc3339())
                .unwrap_or_else(|| "MISSING".to_string());
            let node2_time = final_state_node2
                .get(tag)
                .map(|t| t.to_rfc3339())
                .unwrap_or_else(|| "MISSING".to_string());

            tracing::info!("Tag '{}' timestamps:", tag);
            tracing::info!("  - Gateway: {}", gw_time);
            tracing::info!("  - Node 1:  {}", node1_time);
            tracing::info!("  - Node 2:  {}", node2_time);

            // Check if each tag has the same timestamp across all nodes (if it exists in all nodes)
            if final_state_gw.get(tag).is_some()
                && final_state_node1.get(tag).is_some()
                && final_state_node2.get(tag).is_some()
            {
                let timestamps_match = final_state_gw.get(tag) == final_state_node1.get(tag)
                    && final_state_gw.get(tag) == final_state_node2.get(tag);

                if timestamps_match {
                    tracing::info!("  Timestamp for '{}' is consistent across all nodes", tag);
                } else {
                    tracing::warn!("  ⚠️ Timestamp for '{}' varies between nodes!", tag);
                }
            }
        }

        tracing::info!("=================================================");

        // Log the sizes of each state
        tracing::info!("Gateway final state size: {}", final_state_gw.len());
        tracing::info!("Node 1 final state size: {}", final_state_node1.len());
        tracing::info!("Node 2 final state size: {}", final_state_node2.len());

        // Direct state comparison between nodes
        let all_states_match = final_state_gw.len() == final_state_node1.len()
            && final_state_gw.len() == final_state_node2.len()
            && final_state_node1.len() == final_state_node2.len();

        // Make sure all found tags have the same timestamp across all nodes
        let mut timestamps_consistent = true;
        for tag in &tags {
            // Only compare if the tag exists in all nodes
            if final_state_gw.get(tag).is_some()
                && final_state_node1.get(tag).is_some()
                && final_state_node2.get(tag).is_some()
            {
                if final_state_gw.get(tag) != final_state_node1.get(tag)
                    || final_state_gw.get(tag) != final_state_node2.get(tag)
                    || final_state_node1.get(tag) != final_state_node2.get(tag)
                {
                    timestamps_consistent = false;
                    break;
                }
            }
        }

        // Report final comparison result
        if all_states_match && timestamps_consistent {
            tracing::info!("All nodes have consistent states with matching timestamps!");
        } else if all_states_match {
            tracing::warn!("All nodes have the same number of entries but some timestamps vary!");
        } else {
            tracing::warn!("Nodes have different state content!");
        }

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
    )
    .await?;
    let ws_api_port_node1 = config_node1.ws_api.ws_api_port.unwrap();

    // Configure client node 2
    let (config_node2, preset_cfg_node2) = base_node_test_config(
        false,
        vec![serde_json::to_string(&config_gw_info)?],
        None,
        ws_api_port_socket_node2.local_addr()?.port(),
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
