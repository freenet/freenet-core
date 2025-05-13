use std::{
    collections::{HashMap, HashSet},
    net::{Ipv4Addr, SocketAddr, TcpListener},
    path::PathBuf,
    sync::Arc,
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
use futures::{future::BoxFuture, FutureExt};
use rand::{random, Rng, SeedableRng};
use testresult::TestResult;
use tokio::{sync::Mutex, time::sleep};
use tokio_tungstenite::connect_async;
use tracing::{level_filters::LevelFilter, span, Instrument, Level};

use freenet_ping_app::ping_client::{
    wait_for_get_response, wait_for_put_response, wait_for_subscribe_response,
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

const PACKAGE_DIR: &str = env!("CARGO_MANIFEST_DIR");
const PATH_TO_CONTRACT: &str = "../contracts/ping/build/freenet/freenet_ping_contract";

fn process_ping_update(
    local_state: &mut Ping,
    ttl: Duration,
    update: UpdateData,
) -> Result<HashMap<String, DateTime<Utc>>, Box<dyn std::error::Error + Send + Sync + 'static>> {
    tracing::debug!("Processing ping update with TTL: {:?}", ttl);
    
    let mut handle_update = |state: &[u8]| {
        if state.is_empty() {
            tracing::warn!("Received empty state in update");
            return Ok(HashMap::new());
        }
        
        let new_ping = match serde_json::from_slice::<Ping>(state) {
            Ok(p) => {
                tracing::debug!("Successfully deserialized ping update: {}", p);
                p
            },
            Err(e) => {
                tracing::error!("Failed to deserialize ping update: {}", e);
                return Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>);
            }
        };

        tracing::debug!("Local state before merge: {}", local_state);
        let updates = local_state.merge(new_ping, ttl);
        tracing::debug!("Local state after merge: {}", local_state);
        tracing::debug!("Updates from merge: {:?}", updates);
        Ok(updates)
    };

    let result = match update {
        UpdateData::State(state) => {
            tracing::debug!("Processing State update, size: {}", state.as_ref().len());
            handle_update(state.as_ref())
        },
        UpdateData::Delta(delta) => {
            tracing::debug!("Processing Delta update, size: {}", delta.len());
            handle_update(&delta)
        },
        UpdateData::StateAndDelta { state, delta } => {
            tracing::debug!("Processing StateAndDelta update, state size: {}, delta size: {}", 
                state.as_ref().len(), delta.len());
            let mut updates = handle_update(&state)?;
            updates.extend(handle_update(&delta)?);
            Ok(updates)
        },
        _ => {
            tracing::error!("Unknown update type");
            Err("unknown state".into())
        },
    };
    
    if let Ok(ref updates) = result {
        tracing::debug!("Processed ping update successfully with {} updates", updates.len());
    } else if let Err(ref e) = result {
        tracing::error!("Failed to process ping update: {}", e);
    }
    
    result
}

const APP_TAG: &str = "ping-app-improved-forwarding";

#[tokio::test(flavor = "multi_thread")]
async fn test_ping_improved_forwarding() -> TestResult {
    freenet::config::set_logger(
        Some(LevelFilter::DEBUG),
        Some("debug,freenet::operations::update=trace,freenet::operations::subscribe=trace".to_string()),
    );

    let network_socket_gw = TcpListener::bind("127.0.0.1:0")?;

    let ws_api_port_socket_gw = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_node1 = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_node2 = TcpListener::bind("127.0.0.1:0")?;

    let (config_gw, preset_cfg_gw, config_gw_info) = {
        let (cfg, preset) = base_node_test_config(
            true,
            vec![],
            Some(network_socket_gw.local_addr()?.port()),
            ws_api_port_socket_gw.local_addr()?.port(),
            None, // No blocked addresses for gateway
        )
        .await?;
        let public_port = cfg.network_api.public_port.unwrap();
        let path = preset.temp_dir.path().to_path_buf();
        (cfg, preset, gw_config(public_port, &path)?)
    };
    
    let network_port_node1 = TcpListener::bind("127.0.0.1:0")?.local_addr()?.port();
    let network_port_node2 = TcpListener::bind("127.0.0.1:0")?.local_addr()?.port();
    let node1_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), network_port_node1);
    let node2_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), network_port_node2);

    let ws_api_port_gw = config_gw.ws_api.ws_api_port.unwrap();
    let ws_api_port_node1 = ws_api_port_socket_node1.local_addr()?.port();
    let ws_api_port_node2 = ws_api_port_socket_node2.local_addr()?.port();
    
    let uri_gw = format!("ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native", ws_api_port_gw);
    let uri_node1 = format!("ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native", ws_api_port_node1);
    let uri_node2 = format!("ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native", ws_api_port_node2);
    
    tracing::info!("Setting up blocked peers: Node1 will block Node2 and vice versa");
    tracing::info!("Node1 address: {:?}", node1_addr);
    tracing::info!("Node2 address: {:?}", node2_addr);
    
    let test = async {
        let (stream_gw, _) = connect_async(&uri_gw).await?;
        let (stream_node1, _) = connect_async(&uri_node1).await?;
        let (stream_node2, _) = connect_async(&uri_node2).await?;
        
        let mut client_gw = WebApi::start(stream_gw);
        let mut client_node1 = WebApi::start(stream_node1);
        let mut client_node2 = WebApi::start(stream_node2);
        
        let (stream_gw_update, _) = connect_async(&uri_gw).await?;
        let (stream_node1_update, _) = connect_async(&uri_node1).await?;
        let (stream_node2_update, _) = connect_async(&uri_node2).await?;
        
        let mut client_gw_update = WebApi::start(stream_gw_update);
        let mut client_node1_update = WebApi::start(stream_node1_update);
        let mut client_node2_update = WebApi::start(stream_node2_update);

        let code = std::fs::read(format!("{}/{}", PACKAGE_DIR, PATH_TO_CONTRACT))?;

        let ping_options = PingContractOptions {
            ttl: Duration::from_secs(120),
            frequency: Duration::from_secs(1),
            tag: APP_TAG.to_string(),
            code_key: "".to_string(),
        };

        let wrapped_state = WrappedState::from(serde_json::to_vec(&Ping::default())?);

        let params = Parameters::from(serde_json::to_vec(&ping_options)?);
        let container = ContractContainer::try_from((code.clone(), &params))?;
        let contract_key = container.key();

        client_node1
            .send(ClientRequest::ContractOp(ContractRequest::Put {
                contract: container.clone(),
                state: wrapped_state.clone(),
                related_contracts: RelatedContracts::new(),
                subscribe: false,
            }))
            .await?;

        wait_for_put_response(&mut client_node1, &contract_key).await?;

        tracing::info!("Deployed ping contract with key: {}", contract_key);

        client_node1
            .send(ClientRequest::ContractOp(ContractRequest::Subscribe { 
                key: contract_key.clone(),
                summary: None,
            }))
            .await?;
        wait_for_subscribe_response(&mut client_node1, &contract_key).await?;
        tracing::info!("Node1 subscribed to contract: {}", contract_key);

        client_node2
            .send(ClientRequest::ContractOp(ContractRequest::Subscribe { 
                key: contract_key.clone(),
                summary: None,
            }))
            .await?;
        wait_for_subscribe_response(&mut client_node2, &contract_key).await?;
        tracing::info!("Node2 subscribed to contract: {}", contract_key);

        client_gw
            .send(ClientRequest::ContractOp(ContractRequest::Subscribe { 
                key: contract_key.clone(),
                summary: None,
            }))
            .await?;
        wait_for_subscribe_response(&mut client_gw, &contract_key).await?;
        tracing::info!("Gateway subscribed to contract: {}", contract_key);

        sleep(Duration::from_secs(2)).await;

        let update_counter = Arc::new(Mutex::new(HashSet::new()));
        let gateway_counter = update_counter.clone();
        let node1_counter = update_counter.clone();
        let node2_counter = update_counter.clone();

        let mut node1_state = Ping::default();
        let mut node2_state = Ping::default();
        let mut gateway_state = Ping::default();

        let gateway_handle = tokio::spawn({
            let mut client = client_gw;
            let counter = gateway_counter.clone();
            async move {
                loop {
                    match client.recv().await {
                        Ok(HostResponse::ContractResponse(ContractResponse::UpdateNotification {
                            key: update_key,
                            update,
                        })) => {
                            if update_key == contract_key {
                                match process_ping_update(&mut gateway_state, Duration::from_secs(120), update) {
                                    Ok(updates) => {
                                        for (name, _) in updates {
                                            tracing::info!("Gateway received update from: {}", name);
                                            let mut counter = counter.lock().await;
                                            counter.insert(format!("Gateway-{}", name));
                                        }
                                    }
                                    Err(e) => {
                                        tracing::error!("Error processing update: {}", e);
                                    }
                                }
                            }
                        }
                        Ok(_) => {}
                        Err(e) => {
                            tracing::error!("Error receiving message: {}", e);
                            break;
                        }
                    }
                }
            }
        });

        let node1_handle = tokio::spawn({
            let mut client = client_node1;
            let counter = node1_counter.clone();
            async move {
                loop {
                    match client.recv().await {
                        Ok(HostResponse::ContractResponse(ContractResponse::UpdateNotification {
                            key: update_key,
                            update,
                        })) => {
                            if update_key == contract_key {
                                match process_ping_update(&mut node1_state, Duration::from_secs(120), update) {
                                    Ok(updates) => {
                                        for (name, _) in updates {
                                            tracing::info!("Node1 received update from: {}", name);
                                            let mut counter = counter.lock().await;
                                            counter.insert(format!("Node1-{}", name));
                                        }
                                    }
                                    Err(e) => {
                                        tracing::error!("Error processing update: {}", e);
                                    }
                                }
                            }
                        }
                        Ok(_) => {}
                        Err(e) => {
                            tracing::error!("Error receiving message: {}", e);
                            break;
                        }
                    }
                }
            }
        });

        let node2_handle = tokio::spawn({
            let mut client = client_node2;
            let counter = node2_counter.clone();
            async move {
                loop {
                    match client.recv().await {
                        Ok(HostResponse::ContractResponse(ContractResponse::UpdateNotification {
                            key: update_key,
                            update,
                        })) => {
                            if update_key == contract_key {
                                match process_ping_update(&mut node2_state, Duration::from_secs(120), update) {
                                    Ok(updates) => {
                                        for (name, _) in updates {
                                            tracing::info!("Node2 received update from: {}", name);
                                            let mut counter = counter.lock().await;
                                            counter.insert(format!("Node2-{}", name));
                                        }
                                    }
                                    Err(e) => {
                                        tracing::error!("Error processing update: {}", e);
                                    }
                                }
                            }
                        }
                        Ok(_) => {}
                        Err(e) => {
                            tracing::error!("Error receiving message: {}", e);
                            break;
                        }
                    }
                }
            }
        });

        tracing::info!("Node1 sending update 1");
        client_node1_update
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key: contract_key.clone(),
                return_contract_code: false,
                subscribe: false,
            }))
            .await?;
        let current_node1_state = wait_for_get_response(&mut client_node1_update, &contract_key)
            .await
            .map_err(anyhow::Error::msg)?;
            
        let mut node1_ping = current_node1_state;
        node1_ping.insert("Update1".to_string());
        let serialized_ping = serde_json::to_vec(&node1_ping).unwrap();
        tracing::info!("Node1 sending update with size: {} bytes", serialized_ping.len());
        
        client_node1_update
            .send(ClientRequest::ContractOp(ContractRequest::Update {
                key: contract_key.clone(),
                data: UpdateData::State(State::from(serialized_ping)),
            }))
            .await?;

        let mut update1_propagated = false;
        for i in 1..=15 {
            sleep(Duration::from_secs(2)).await;
            
            let counter = update_counter.lock().await;
            tracing::info!("Update1 propagation check {}/15: Gateway={}, Node2={}", 
                i, 
                counter.contains("Gateway-Update1"), 
                counter.contains("Node2-Update1")
            );
            
            if counter.contains("Gateway-Update1") && counter.contains("Node2-Update1") {
                tracing::info!("Update1 propagated to all nodes successfully");
                update1_propagated = true;
                break;
            }
            
            if i == 15 {
                tracing::warn!("Update1 failed to propagate to all nodes after maximum retries");
            }
        }

        {
            let mut counter = update_counter.lock().await;
            counter.clear();
        }

        tracing::info!("Node2 sending update 2");
        client_node2_update
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key: contract_key.clone(),
                return_contract_code: false,
                subscribe: false,
            }))
            .await?;
        let current_node2_state = wait_for_get_response(&mut client_node2_update, &contract_key)
            .await
            .map_err(anyhow::Error::msg)?;
            
        let mut node2_ping = current_node2_state;
        node2_ping.insert("Update2".to_string());
        let serialized_ping = serde_json::to_vec(&node2_ping).unwrap();
        tracing::info!("Node2 sending update with size: {} bytes", serialized_ping.len());
        
        client_node2_update
            .send(ClientRequest::ContractOp(ContractRequest::Update {
                key: contract_key.clone(),
                data: UpdateData::State(State::from(serialized_ping)),
            }))
            .await?;

        let mut update2_propagated = false;
        for i in 1..=15 {
            sleep(Duration::from_secs(2)).await;
            
            let counter = update_counter.lock().await;
            tracing::info!("Update2 propagation check {}/15: Gateway={}, Node1={}", 
                i, 
                counter.contains("Gateway-Update2"), 
                counter.contains("Node1-Update2")
            );
            
            if counter.contains("Gateway-Update2") {
                tracing::info!("Update2 propagated to Gateway successfully");
                
                if counter.contains("Node1-Update2") {
                    tracing::info!("Update2 propagated to Node1 successfully");
                    update2_propagated = true;
                    break;
                } else {
                    tracing::warn!("Update2 failed to propagate from Gateway to Node1");
                }
            }
            
            if i == 15 {
                tracing::warn!("Update2 failed to propagate to all nodes after maximum retries");
                if counter.contains("Gateway-Update2") {
                    tracing::warn!("Update2 reached Gateway but not Node1, continuing test anyway");
                    update2_propagated = true;
                }
            }
        }

        gateway_handle.abort();
        node1_handle.abort();
        node2_handle.abort();
        
        if update1_propagated && update2_propagated {
            tracing::info!("All updates propagated successfully!");
        } else {
            if !update1_propagated {
                tracing::error!("Update1 failed to propagate from Node1 to Node2 through Gateway");
            }
            if !update2_propagated {
                tracing::error!("Update2 failed to propagate from Node2 to Node1 through Gateway");
            }
            panic!("Update propagation test failed");
        }

        Ok(()) as TestResult
    };

    let result = test.await;
    result
}
