use std::{
    net::{Ipv4Addr, SocketAddr, TcpListener},
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::{anyhow, Result};
use freenet::{
    config::{ConfigArgs, InlineGwConfig, NetworkArgs, SecretArgs, WebsocketApiArgs},
    dev_tool::TransportKeypair,
    local_node::NodeConfig,
    server::serve_gateway,
};
use freenet_ping_types::Ping;
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, ContractResponse, HostResponse, WebApi},
    prelude::*,
};
use futures::{future::BoxFuture, FutureExt};
use rand::{random, Rng, SeedableRng};
use testresult::TestResult;
use tokio::{
    select,
    sync::{mpsc, Mutex},
    time::sleep,
};
use tokio_tungstenite::connect_async;
use tracing::{span, Instrument, Level};
use tracing_subscriber::EnvFilter;

use freenet_ping_app::ping_client::{
    wait_for_get_response, wait_for_put_response, wait_for_subscribe_response,
};

const MAX_UPDATE_RETRIES: u32 = 5;
const BASE_DELAY_MS: u64 = 500;
const MAX_TEST_DURATION_SECS: u64 = 300;

#[derive(Debug)]
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

static RNG: once_cell::sync::Lazy<std::sync::Mutex<rand::rngs::StdRng>> =
    once_cell::sync::Lazy::new(|| {
        std::sync::Mutex::new(rand::rngs::StdRng::from_seed(
            *b"0102030405060708090a0b0c0d0e0f10",
        ))
    });

const PACKAGE_DIR: &str = env!("CARGO_MANIFEST_DIR");
const PATH_TO_CONTRACT: &str = "../contracts/ping/build/freenet/freenet_ping_contract";
const APP_TAG: &str = "ping-app";

struct LogEntry {
    timestamp: std::time::SystemTime,
    source: String,
    message: String,
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ping_blocked_peers_solution() -> TestResult {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("debug,freenet::operations::subscribe=trace,freenet::contract=trace,freenet::operations::update=trace")
    });
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();

    let (log_tx, mut log_rx) = mpsc::channel::<LogEntry>(1000);

    let log_task = tokio::spawn(async move {
        let mut logs = Vec::new();
        while let Some(log) = log_rx.recv().await {
            logs.push(log);
            logs.sort_by_key(|log| log.timestamp);

            if logs.len() > 10 {
                println!("--- CHRONOLOGICAL LOGS (LAST 10) ---");
                for log in logs.iter().rev().take(10).rev() {
                    println!(
                        "[{}] {}: {}",
                        humantime::format_rfc3339(log.timestamp),
                        log.source,
                        log.message
                    );
                }
                println!("----------------------------------");
            }
        }
    });

    let create_logger = |source: String, log_tx: mpsc::Sender<LogEntry>| {
        move |message: String| {
            let log_entry = LogEntry {
                timestamp: std::time::SystemTime::now(),
                source: source.clone(),
                message,
            };
            let _ = log_tx.try_send(log_entry);
        }
    };

    let test = async {
        let log_tx_clone = log_tx.clone();
        let log_gateway = create_logger("Gateway".to_string(), log_tx_clone.clone());
        let log_node1 = create_logger("Node1".to_string(), log_tx_clone.clone());
        let log_node2 = create_logger("Node2".to_string(), log_tx_clone);

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let ws_port_gw = rng.gen_range(40000..50000);
        let ws_port_node1 = rng.gen_range(50001..60000);
        let ws_port_node2 = rng.gen_range(60001..70000);
        let network_port_gw = rng.gen_range(30000..40000);
        let network_port_node1 = rng.gen_range(20000..30000);
        let network_port_node2 = rng.gen_range(10000..20000);

        let temp_dir = tempfile::tempdir()?;
        let gw_dir = tempfile::tempdir_in(temp_dir.path())?;
        let node1_dir = tempfile::tempdir_in(temp_dir.path())?;
        let node2_dir = tempfile::tempdir_in(temp_dir.path())?;

        let network_socket_gw = TcpListener::bind("127.0.0.1:0")?;
        let gw_network_port = network_socket_gw.local_addr()?.port();
        let gw_network_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), gw_network_port);

        let ws_api_port_socket_gw = TcpListener::bind("127.0.0.1:0")?;
        let ws_api_port_socket_node1 = TcpListener::bind("127.0.0.1:0")?;
        let ws_api_port_socket_node2 = TcpListener::bind("127.0.0.1:0")?;

        let network_socket_node1 = TcpListener::bind("127.0.0.1:0")?;
        let node1_network_port = network_socket_node1.local_addr()?.port();
        let node1_network_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), node1_network_port);

        let network_socket_node2 = TcpListener::bind("127.0.0.1:0")?;
        let node2_network_port = network_socket_node2.local_addr()?.port();
        let node2_network_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), node2_network_port);

        let node1_peer_id = format!("node1-{}", node1_network_port);
        let node2_peer_id = format!("node2-{}", node2_network_port);

        log_gateway(format!("Gateway node port: {}", gw_network_port));
        log_node1(format!("Node 1 port: {}", node1_network_port));
        log_node2(format!("Node 2 port: {}", node2_network_port));
        log_node1(format!("Node 1 blocks: {:?}", node2_network_addr));
        log_node2(format!("Node 2 blocks: {:?}", node1_network_addr));

        let (config_gw, preset_cfg_gw, config_gw_info) = {
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

        let (config_node1, preset_cfg_node1) = base_node_test_config(
            false,
            vec![serde_json::to_string(&config_gw_info)?],
            Some(node1_network_port),
            ws_api_port_socket_node1.local_addr()?.port(),
            Some(vec![node2_network_addr]), // Node1 blocks Node2
        )
        .await?;
        let ws_api_port_node1 = config_node1.ws_api.ws_api_port.unwrap();

        let (config_node2, preset_cfg_node2) = base_node_test_config(
            false,
            vec![serde_json::to_string(&config_gw_info)?],
            Some(node2_network_port),
            ws_api_port_socket_node2.local_addr()?.port(),
            Some(vec![node1_network_addr]), // Node2 blocks Node1
        )
        .await?;
        let ws_api_port_node2 = config_node2.ws_api.ws_api_port.unwrap();

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

        log_node1(format!("Updating node1 to block node2: {}", node2_peer_id));

        log_gateway("Waiting for nodes to connect to the gateway".to_string());
        sleep(Duration::from_secs(5)).await;

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

        log_gateway(format!("Connecting to Gateway at {}", uri_gw));
        let (stream_gw, _) = connect_async(&uri_gw).await?;
        let mut client_gw = WebApi::start(stream_gw);

        log_node1(format!("Connecting to Node1 at {}", uri_node1));
        let (stream_node1, _) = connect_async(&uri_node1).await?;
        let mut client_node1 = WebApi::start(stream_node1);

        log_node2(format!("Connecting to Node2 at {}", uri_node2));
        let (stream_node2, _) = connect_async(&uri_node2).await?;
        let mut client_node2 = WebApi::start(stream_node2);

        let client_gw = Arc::new(Mutex::new(client_gw));
        let client_node1 = Arc::new(Mutex::new(client_node1));
        let client_node2 = Arc::new(Mutex::new(client_node2));

        log_gateway("Deploying ping contract on gateway node".to_string());
        let mut client_gw_lock = client_gw.lock().await;

        let ping = Ping::default();
        let state = serde_json::to_vec(&ping)?;

        let path_to_code = PathBuf::from(PACKAGE_DIR).join(PATH_TO_CONTRACT);
        log_gateway(format!(
            "Loading contract code from {}",
            path_to_code.display()
        ));
        let code = std::fs::read(path_to_code)
            .ok()
            .ok_or_else(|| anyhow!("Failed to read contract code"))?;
        let code_hash = CodeHash::from_code(&code);
        log_gateway(format!("Loaded contract code with hash: {}", code_hash));

        let ping = Ping::default();
        let serialized = serde_json::to_vec(&ping)?;
        let wrapped_state = WrappedState::new(serialized);
        let params = Parameters::from(vec![]);
        let container = ContractContainer::try_from((code, &params))?;
        let contract_key = container.key();

        log_gateway(format!(
            "Deploying ping contract with key: {}",
            contract_key
        ));

        client_gw_lock
            .send(ClientRequest::ContractOp(ContractRequest::Put {
                contract: container.clone(),
                state: wrapped_state.clone(),
                related_contracts: RelatedContracts::new(),
                subscribe: false,
            }))
            .await?;

        let key = wait_for_put_response(&mut client_gw_lock, &contract_key)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        log_gateway(format!("Ping contract deployed with key: {}", key));
        drop(client_gw_lock);

        log_node1(format!("Subscribing node1 to the contract: {}", key));
        let mut client_node1_lock = client_node1.lock().await;
        client_node1_lock
            .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
                key: contract_key,
                summary: None,
            }))
            .await?;
        let response = wait_for_subscribe_response(&mut client_node1_lock, &contract_key)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;
        log_node1(format!("Node1 subscription response: {:?}", response));
        drop(client_node1_lock);

        log_node2(format!("Subscribing node2 to the contract: {}", key));
        let mut client_node2_lock = client_node2.lock().await;
        client_node2_lock
            .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
                key: contract_key,
                summary: None,
            }))
            .await?;
        let response = wait_for_subscribe_response(&mut client_node2_lock, &contract_key)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;
        log_node2(format!("Node2 subscription response: {:?}", response));
        drop(client_node2_lock);

        log_gateway("Waiting for subscriptions to propagate".to_string());
        sleep(Duration::from_secs(5)).await;

        let get_all_states = |client_gw: &Arc<Mutex<WebApi>>,
                              client_node1: &Arc<Mutex<WebApi>>,
                              client_node2: &Arc<Mutex<WebApi>>,
                              key: ContractKey,
                              log_gateway: &dyn Fn(String),
                              log_node1: &dyn Fn(String),
                              log_node2: &dyn Fn(String)|
         -> BoxFuture<'_, anyhow::Result<(Ping, Ping, Ping)>> {
            Box::pin(async move {
                log_gateway("Querying all nodes for current state...".to_string());

                let client_gw_clone = client_gw.clone();
                let client_node1_clone = client_node1.clone();
                let client_node2_clone = client_node2.clone();

                let gw_state_future = tokio::spawn(async move {
                    let mut client = client_gw_clone.lock().await;
                    client
                        .send(ClientRequest::ContractOp(ContractRequest::Get {
                            key,
                            return_contract_code: false,
                            subscribe: false,
                        }))
                        .await?;

                    let response = client.recv().await?;

                    if let HostResponse::ContractResponse(ContractResponse::GetResponse {
                        state,
                        ..
                    }) = response
                    {
                        let state = serde_json::from_slice::<Ping>(&state)?;
                        Ok(state)
                    } else {
                        Err(anyhow::anyhow!("Failed to get state from gateway"))
                    }
                });

                let node1_state_future = tokio::spawn(async move {
                    let mut client = client_node1_clone.lock().await;
                    client
                        .send(ClientRequest::ContractOp(ContractRequest::Get {
                            key,
                            return_contract_code: false,
                            subscribe: false,
                        }))
                        .await?;

                    let response = client.recv().await?;

                    if let HostResponse::ContractResponse(ContractResponse::GetResponse {
                        state,
                        ..
                    }) = response
                    {
                        let state = serde_json::from_slice::<Ping>(&state)?;
                        Ok(state)
                    } else {
                        Err(anyhow::anyhow!("Failed to get state from node1"))
                    }
                });

                let node2_state_future = tokio::spawn(async move {
                    let mut client = client_node2_clone.lock().await;
                    client
                        .send(ClientRequest::ContractOp(ContractRequest::Get {
                            key,
                            return_contract_code: false,
                            subscribe: false,
                        }))
                        .await?;

                    let response = client.recv().await?;

                    if let HostResponse::ContractResponse(ContractResponse::GetResponse {
                        state,
                        ..
                    }) = response
                    {
                        let state = serde_json::from_slice::<Ping>(&state)?;
                        Ok(state)
                    } else {
                        Err(anyhow::anyhow!("Failed to get state from node2"))
                    }
                });

                let gw_result = gw_state_future.await??;
                let node1_result = node1_state_future.await??;
                let node2_result = node2_state_future.await??;

                log_gateway(format!("Gateway state: {:?}", gw_result));
                log_node1(format!("Node1 state: {:?}", node1_result));
                log_node2(format!("Node2 state: {:?}", node2_result));

                Ok((gw_result, node1_result, node2_result))
            })
        };

        let update_with_retry = |client: &Arc<Mutex<WebApi>>,
                                 node_name: &str,
                                 key: ContractKey,
                                 name: String,
                                 timestamp: u64,
                                 log_gateway: &dyn Fn(String),
                                 log_node1: &dyn Fn(String),
                                 log_node2: &dyn Fn(String)|
         -> BoxFuture<'_, anyhow::Result<()>> {
            Box::pin(async move {
                let logger = match node_name {
                    "Gateway" => log_gateway,
                    "Node1" => log_node1,
                    "Node2" => log_node2,
                    _ => log_gateway,
                };

                for retry in 0..MAX_UPDATE_RETRIES {
                    logger(format!(
                        "Updating contract on {} (attempt {})",
                        node_name,
                        retry + 1
                    ));

                    let delay = BASE_DELAY_MS * (2_u64.pow(retry as u32));

                    let mut client_lock = client.lock().await;

                    let mut ping = Ping::default();
                    ping.insert(format!("{}:{}", name.clone(), timestamp));
                    let state = serde_json::to_vec(&ping)?;

                    client_lock
                        .send(ClientRequest::ContractOp(ContractRequest::Update {
                            key,
                            data: UpdateData::State(state.into()),
                        }))
                        .await?;

                    let response = wait_for_put_response(&mut client_lock, &key)
                        .await
                        .map_err(|e| anyhow::anyhow!("{}", e))?;
                    logger(format!("{} update response: {:?}", node_name, response));
                    drop(client_lock);

                    sleep(Duration::from_millis(delay)).await;

                    let (gw_state, node1_state, node2_state) = get_all_states(
                        &client_gw,
                        &client_node1,
                        &client_node2,
                        key,
                        log_gateway,
                        log_node1,
                        log_node2,
                    )
                    .await?;

                    let update_propagated = match node_name {
                        "Gateway" => {
                            gw_state.contains_key(&name)
                                && node1_state.contains_key(&name)
                                && node2_state.contains_key(&name)
                        }
                        "Node1" => {
                            gw_state.contains_key(&name)
                                && node1_state.contains_key(&name)
                                && node2_state.contains_key(&name)
                        }
                        "Node2" => {
                            gw_state.contains_key(&name)
                                && node1_state.contains_key(&name)
                                && node2_state.contains_key(&name)
                        }
                        _ => false,
                    };

                    if update_propagated {
                        logger(format!(
                            "Update from {} successfully propagated to all nodes",
                            node_name
                        ));
                        return Ok(());
                    }

                    logger(format!(
                        "Update from {} not fully propagated, retrying...",
                        node_name
                    ));
                }

                Err(anyhow::anyhow!(
                    "Failed to propagate update from {} after {} retries",
                    node_name,
                    MAX_UPDATE_RETRIES
                ))
            })
        };

        log_gateway("Testing update propagation from Gateway to Node1 and Node2".to_string());
        let gateway_update_result = update_with_retry(
            &client_gw,
            "Gateway",
            key,
            "Gateway Update".to_string(),
            42,
            &log_gateway,
            &log_node1,
            &log_node2,
        )
        .await;

        log_node1("Testing update propagation from Node1 to Gateway and Node2".to_string());
        let node1_update_result = update_with_retry(
            &client_node1,
            "Node1",
            key,
            "Node1 Update".to_string(),
            43,
            &log_gateway,
            &log_node1,
            &log_node2,
        )
        .await;

        log_node2("Testing update propagation from Node2 to Gateway and Node1".to_string());
        let node2_update_result = update_with_retry(
            &client_node2,
            "Node2",
            key,
            "Node2 Update".to_string(),
            44,
            &log_gateway,
            &log_node1,
            &log_node2,
        )
        .await;

        let all_updates_successful = gateway_update_result.is_ok()
            && node1_update_result.is_ok()
            && node2_update_result.is_ok();

        if all_updates_successful {
            log_gateway("All updates successfully propagated!".to_string());
        } else {
            if let Err(e) = &gateway_update_result {
                log_gateway(format!("Gateway update error: {}", e));
            }
            if let Err(e) = &node1_update_result {
                log_node1(format!("Node1 update error: {}", e));
            }
            if let Err(e) = &node2_update_result {
                log_node2(format!("Node2 update error: {}", e));
            }
        }

        assert!(
            gateway_update_result.is_ok(),
            "Gateway update failed to propagate"
        );
        assert!(
            node1_update_result.is_ok(),
            "Node1 update failed to propagate"
        );
        assert!(
            node2_update_result.is_ok(),
            "Node2 update failed to propagate"
        );

        drop(log_tx);

        Ok::<_, anyhow::Error>(())
    };

    let instrumented_test = test.instrument(span!(Level::INFO, "test_ping_blocked_peers_solution"));

    let result = select! {
        result = instrumented_test => result.map_err(|e| e.into()),
        _ = sleep(Duration::from_secs(MAX_TEST_DURATION_SECS)) => {
            Err("Test timed out".into())
        }
    };

    let _ = log_task.await;

    result
}
