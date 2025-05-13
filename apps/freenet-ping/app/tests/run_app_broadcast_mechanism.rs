use std::{
    fmt::Debug,
    net::{Ipv4Addr, SocketAddr, TcpListener},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
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
use futures::{future::BoxFuture, FutureExt};
use rand::{random, Rng, SeedableRng};
use testresult::TestResult;
use tokio::{task::LocalSet, time::sleep};
use tokio_tungstenite::connect_async;
use tracing::info;

use freenet_ping_app::ping_client::{
    wait_for_get_response, wait_for_put_response, wait_for_subscribe_response,
};

const MAX_UPDATE_RETRIES: usize = 8;
const INITIAL_DELAY_MS: u64 = 500;
const MAX_DELAY_MS: u64 = 15000;
const PROPAGATION_CHECK_INTERVAL_MS: u64 = 1000;
const MAX_PROPAGATION_CHECKS: usize = 10;
const PACKAGE_DIR: &str = env!("CARGO_MANIFEST_DIR");
const PATH_TO_CONTRACT: &str = "../contracts/ping/build/freenet/freenet_ping_contract";
const APP_TAG: &str = "ping-app";

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
    blocked_addresses: Option<Vec<SocketAddr>>,
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

#[derive(Debug, Clone)]
struct LogEntry {
    timestamp: SystemTime,
    node_name: String,
    message: String,
}

struct ChronologicalLogger {
    entries: Arc<Mutex<Vec<LogEntry>>>,
}

impl ChronologicalLogger {
    fn new() -> Self {
        Self {
            entries: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn log(&self, node_name: &str, message: &str) {
        let entry = LogEntry {
            timestamp: SystemTime::now(),
            node_name: node_name.to_string(),
            message: message.to_string(),
        };

        if let Ok(mut entries) = self.entries.lock() {
            entries.push(entry);
        }
    }

    fn print_logs(&self) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

            println!("\n=== CHRONOLOGICAL LOGS FROM ALL NODES ===");
            for entry in entries.iter() {
                let timestamp = entry
                    .timestamp
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default();
                println!(
                    "[{}.{:03}] [{}] {}",
                    timestamp.as_secs(),
                    timestamp.subsec_millis(),
                    entry.node_name,
                    entry.message
                );
            }
            println!("=== END OF LOGS ===\n");
        }
    }
}

#[tokio::test]
async fn test_ping_broadcast_mechanism() -> TestResult {
    freenet::config::set_logger(Some(tracing::level_filters::LevelFilter::DEBUG), None);

    let logger = Arc::new(ChronologicalLogger::new());
    let gateway_logger = logger.clone();
    let node1_logger = logger.clone();
    let node2_logger = logger.clone();

    let network_socket_gw = TcpListener::bind("127.0.0.1:0")?;
    let gw_network_port = network_socket_gw.local_addr()?.port();
    let _gw_network_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), gw_network_port);

    let ws_api_port_socket_gw = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_node1 = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_node2 = TcpListener::bind("127.0.0.1:0")?;

    let network_socket_node1 = TcpListener::bind("127.0.0.1:0")?;
    let node1_network_port = network_socket_node1.local_addr()?.port();
    let node1_network_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), node1_network_port);

    let network_socket_node2 = TcpListener::bind("127.0.0.1:0")?;
    let node2_network_port = network_socket_node2.local_addr()?.port();
    let node2_network_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), node2_network_port);

    let (config_gw, _preset_cfg_gw, config_gw_info) = {
        let (cfg, preset) = base_node_test_config(
            true,
            vec![],
            Some(gw_network_port),
            ws_api_port_socket_gw.local_addr()?.port(),
            None, // Gateway doesn't block any peers
        )
        .await?;
        let public_port = cfg.network_api.public_port.unwrap();
        let path = preset.temp_dir.path().to_path_buf();
        (cfg, preset, gw_config(public_port, &path)?)
    };
    let ws_api_port_gw = config_gw.ws_api.ws_api_port.unwrap();
    gateway_logger.log(
        "Gateway",
        &format!("Gateway WS API port: {}", ws_api_port_gw),
    );

    let (config_node1, _preset_cfg_node1) = base_node_test_config(
        false,
        vec![serde_json::to_string(&config_gw_info)?],
        Some(node1_network_port),
        ws_api_port_socket_node1.local_addr()?.port(),
        Some(vec![node2_network_addr]), // Node1 blocks Node2
    )
    .await?;
    let ws_api_port_node1 = config_node1.ws_api.ws_api_port.unwrap();
    node1_logger.log(
        "Node1",
        &format!("Node1 WS API port: {}", ws_api_port_node1),
    );
    node1_logger.log("Node1", &format!("Node1 blocks: {:?}", node2_network_addr));

    let (config_node2, _preset_cfg_node2) = base_node_test_config(
        false,
        vec![serde_json::to_string(&config_gw_info)?],
        Some(node2_network_port),
        ws_api_port_socket_node2.local_addr()?.port(),
        Some(vec![node1_network_addr]), // Node2 blocks Node1
    )
    .await?;
    let ws_api_port_node2 = config_node2.ws_api.ws_api_port.unwrap();
    node2_logger.log(
        "Node2",
        &format!("Node2 WS API port: {}", ws_api_port_node2),
    );
    node2_logger.log("Node2", &format!("Node2 blocks: {:?}", node1_network_addr));

    std::mem::drop(network_socket_gw);
    std::mem::drop(network_socket_node1);
    std::mem::drop(network_socket_node2);
    std::mem::drop(ws_api_port_socket_gw);
    std::mem::drop(ws_api_port_socket_node1);
    std::mem::drop(ws_api_port_socket_node2);

    let gateway_node = async {
        let config = config_gw.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    let node1 = async move {
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

    let local = LocalSet::new();

    local.spawn_local(gateway_node);
    local.spawn_local(node1);
    local.spawn_local(node2);

    tokio::task::spawn_local(async {
        local.await;
    });

    sleep(Duration::from_secs(10)).await;

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

    let client_gw = WebApi::start(stream_gw);
    let client_node1 = WebApi::start(stream_node1);
    let client_node2 = WebApi::start(stream_node2);

    let path_to_code = PathBuf::from(PACKAGE_DIR).join(PATH_TO_CONTRACT);
    tracing::info!(path=%path_to_code.display(), "loading contract code");
    let code = std::fs::read(path_to_code)
        .ok()
        .ok_or_else(|| anyhow!("Failed to read contract code"))?;
    let code_hash = CodeHash::from_code(&code);
    tracing::info!(code_hash=%code_hash, "loaded contract code");

    let ping_options = PingContractOptions {
        frequency: Duration::from_secs(5),
        ttl: Duration::from_secs(30),
        tag: APP_TAG.to_string(),
        code_key: code_hash.to_string(),
    };
    let params = Parameters::from(serde_json::to_vec(&ping_options).unwrap());
    let container = ContractContainer::try_from((code, &params))?;
    let contract_key = container.key();

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

    let key = wait_for_put_response(&mut client_gw, &contract_key)
        .await
        .map_err(anyhow::Error::msg)?;
    gateway_logger.log(
        "Gateway",
        &format!("Deployed ping contract with key: {}", key),
    );

    client_gw
        .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
            key: contract_key,
            summary: None,
        }))
        .await?;
    wait_for_subscribe_response(&mut client_gw, &contract_key)
        .await
        .map_err(anyhow::Error::msg)?;
    gateway_logger.log("Gateway", &format!("Subscribed to contract: {}", key));

    client_node1
        .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
            key: contract_key,
            summary: None,
        }))
        .await?;
    wait_for_subscribe_response(&mut client_node1, &contract_key)
        .await
        .map_err(anyhow::Error::msg)?;
    node1_logger.log("Node1", &format!("Subscribed to contract: {}", key));

    client_node2
        .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
            key: contract_key,
            summary: None,
        }))
        .await?;
    wait_for_subscribe_response(&mut client_node2, &contract_key)
        .await
        .map_err(anyhow::Error::msg)?;
    node2_logger.log("Node2", &format!("Subscribed to contract: {}", key));

    sleep(Duration::from_secs(5)).await;

    let ws_api_port_gw_copy = ws_api_port_gw;
    let ws_api_port_node1_copy = ws_api_port_node1;
    let ws_api_port_node2_copy = ws_api_port_node2;

    let get_all_states =
        move |key: ContractKey| -> BoxFuture<'static, anyhow::Result<(Ping, Ping, Ping)>> {
            let key = key.clone();
            let ws_api_port_gw = ws_api_port_gw_copy;
            let ws_api_port_node1 = ws_api_port_node1_copy;
            let ws_api_port_node2 = ws_api_port_node2_copy;

            Box::pin(async move {
                info!("Querying all nodes for current state...");

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

                let state_gw = wait_for_get_response(&mut client_gw, &key)
                    .await
                    .map_err(anyhow::Error::msg)?;

                let state_node1 = wait_for_get_response(&mut client_node1, &key)
                    .await
                    .map_err(anyhow::Error::msg)?;

                let state_node2 = wait_for_get_response(&mut client_node2, &key)
                    .await
                    .map_err(anyhow::Error::msg)?;

                Ok((state_gw, state_node1, state_node2))
            })
        };

    let send_update_and_check_propagation = |source_node: &str,
                                             key: &ContractKey,
                                             ws_api_port: u16,
                                             logger: Arc<ChronologicalLogger>|
     -> BoxFuture<'static, anyhow::Result<bool>> {
        let key = *key;
        let source_node = source_node.to_string();
        let logger = logger.clone();
        let source_uri = format!(
            "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
            ws_api_port
        );

        Box::pin(async move {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            let update_id = format!("update-{}-{}", source_node, timestamp);

            logger.log(
                &source_node,
                &format!("Sending update with ID: {}", update_id),
            );

            let (stream_source, _) = connect_async(&source_uri).await?;
            let mut source_client = WebApi::start(stream_source);

            let mut delay_ms = INITIAL_DELAY_MS;
            let mut success = false;

            let mut ping = Ping::default();
            ping.insert(update_id.clone());

            for attempt in 1..=MAX_UPDATE_RETRIES {
                match source_client
                    .send(ClientRequest::ContractOp(ContractRequest::Update {
                        key,
                        data: UpdateData::Delta(StateDelta::from(
                            serde_json::to_vec(&ping).unwrap(),
                        )),
                    }))
                    .await
                {
                    Ok(_) => {
                        logger.log(
                            &source_node,
                            &format!("Update sent successfully on attempt {}", attempt),
                        );
                        success = true;
                        break;
                    }
                    Err(e) => {
                        logger.log(
                            &source_node,
                            &format!("Update attempt {} failed: {}", attempt, e),
                        );

                        if e.to_string().contains("connection") {
                            let (new_stream, _) = match connect_async(&source_uri).await {
                                Ok(s) => {
                                    logger.log(&source_node, "Reconnected successfully");
                                    s
                                }
                                Err(e) => {
                                    logger
                                        .log(&source_node, &format!("Reconnection failed: {}", e));
                                    delay_ms = (delay_ms * 2).min(MAX_DELAY_MS);
                                    sleep(Duration::from_millis(delay_ms)).await;
                                    continue;
                                }
                            };
                            source_client = WebApi::start(new_stream);
                        }

                        delay_ms = (delay_ms * 2).min(MAX_DELAY_MS);
                        sleep(Duration::from_millis(delay_ms)).await;
                    }
                }
            }

            if !success {
                logger.log(
                    &source_node,
                    "Failed to send update after all retry attempts",
                );
                return Ok(false);
            }

            let gw_uri = format!(
                "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
                ws_api_port_gw
            );
            let node1_uri = format!(
                "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
                ws_api_port_node1
            );
            let node2_uri = format!(
                "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
                ws_api_port_node2
            );

            let (stream_gw, _) = connect_async(&gw_uri).await?;
            let (stream_node1, _) = connect_async(&node1_uri).await?;
            let (stream_node2, _) = connect_async(&node2_uri).await?;

            let mut client_gw = WebApi::start(stream_gw);
            let mut client_node1 = WebApi::start(stream_node1);
            let mut client_node2 = WebApi::start(stream_node2);

            for check in 1..=MAX_PROPAGATION_CHECKS {
                logger.log(
                    "Test",
                    &format!("Propagation check {} for update {}", check, update_id),
                );

                sleep(Duration::from_millis(PROPAGATION_CHECK_INTERVAL_MS)).await;

                let states_result = get_all_states(key).await;

                match states_result {
                    Ok((gw_ping, node1_ping, node2_ping)) => {
                        let gw_has_update = gw_ping.contains_key(&update_id);
                        let node1_has_update = node1_ping.contains_key(&update_id);
                        let node2_has_update = node2_ping.contains_key(&update_id);

                        logger.log(
                            "Test",
                            &format!(
                                "Update {} propagation status - Gateway: {}, Node1: {}, Node2: {}",
                                update_id, gw_has_update, node1_has_update, node2_has_update
                            ),
                        );

                        if gw_has_update && node1_has_update && node2_has_update {
                            logger.log(
                                "Test",
                                &format!(
                                    "Update {} successfully propagated to all nodes",
                                    update_id
                                ),
                            );
                            return Ok(true);
                        }
                    }
                    Err(e) => {
                        logger.log("Test", &format!("Error checking propagation: {}", e));
                    }
                }
            }

            logger.log(
                "Test",
                &format!(
                    "Update {} failed to propagate to all nodes after {} checks",
                    update_id, MAX_PROPAGATION_CHECKS
                ),
            );

            Ok(false)
        })
    };

    let node1_to_node2_result = send_update_and_check_propagation(
        "Node1",
        &contract_key,
        ws_api_port_node1,
        node1_logger.clone(),
    )
    .await?;

    let node2_to_node1_result = send_update_and_check_propagation(
        "Node2",
        &contract_key,
        ws_api_port_node2,
        node2_logger.clone(),
    )
    .await?;

    let gateway_to_nodes_result = send_update_and_check_propagation(
        "Gateway",
        &contract_key,
        ws_api_port_gw,
        gateway_logger.clone(),
    )
    .await?;

    logger.print_logs();

    assert!(
        !node1_to_node2_result || !node2_to_node1_result || !gateway_to_nodes_result,
        "Expected at least one update propagation test to fail, but all succeeded"
    );

    Ok(())
}
