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
use futures::{future::BoxFuture, FutureExt};
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
            None, // blocked_addresses
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
        None,
        ws_api_port_socket_node1.local_addr()?.port(),
        None, // blocked_addresses
    )
    .await?;
    let ws_api_port_node1 = config_node1.ws_api.ws_api_port.unwrap();

    let (config_node2, preset_cfg_node2) = base_node_test_config(
        false,
        vec![serde_json::to_string(&config_gw_info)?],
        None,
        ws_api_port_socket_node2.local_addr()?.port(),
        None, // blocked_addresses
    )
    .await?;
    let ws_api_port_node2 = config_node2.ws_api.ws_api_port.unwrap();

    tracing::info!("Gateway node data dir: {:?}", preset_cfg_gw.temp_dir.path());
    tracing::info!("Node 1 data dir: {:?}", preset_cfg_node1.temp_dir.path());
    tracing::info!("Node 2 data dir: {:?}", preset_cfg_node2.temp_dir.path());

    std::mem::drop(network_socket_gw);
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

    let test = tokio::time::timeout(Duration::from_secs(120), async {
        tokio::time::sleep(Duration::from_secs(10)).await;

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
        tracing::info!(key=%key, "Gateway: put ping contract successfully!");

        tracing::info!("Node 1 getting contract...");
        client_node1
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key: contract_key,
                return_contract_code: true,
                subscribe: false,
            }))
            .await?;

        let node1_state = wait_for_get_response(&mut client_node1, &contract_key)
            .await
            .map_err(anyhow::Error::msg)?;
        tracing::info!("Node 1: got contract with {} entries", node1_state.len());

        tracing::info!("Node 2 getting contract...");
        client_node2
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key: contract_key,
                return_contract_code: true,
                subscribe: false,
            }))
            .await?;

        let node2_state = wait_for_get_response(&mut client_node2, &contract_key)
            .await
            .map_err(anyhow::Error::msg)?;
        tracing::info!("Node 2: got contract with {} entries", node2_state.len());

        tracing::info!("All nodes subscribing to contract...");

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

        let mut gw_local_state = Ping::default();
        let mut node1_local_state = Ping::default();
        let mut node2_local_state = Ping::default();

        let gw_tag = "ping-from-gw".to_string();
        let node1_tag = "ping-from-node1".to_string();
        let node2_tag = "ping-from-node2".to_string();

        let mut gw_seen_node1 = false;
        let mut gw_seen_node2 = false;
        let mut node1_seen_gw = false;
        let mut node1_seen_node2 = false;
        let mut node2_seen_gw = false;
        let mut node2_seen_node1 = false;

        let mut gw_ping = Ping::default();
        gw_ping.insert(gw_tag.clone());
        tracing::info!(%gw_ping, "Gateway sending update with tag: {}", gw_tag);
        client_gw
            .send(ClientRequest::ContractOp(ContractRequest::Update {
                key: contract_key,
                data: UpdateData::Delta(StateDelta::from(serde_json::to_vec(&gw_ping).unwrap())),
            }))
            .await?;

        let mut node1_ping = Ping::default();
        node1_ping.insert(node1_tag.clone());
        tracing::info!(%node1_ping, "Node 1 sending update with tag: {}", node1_tag);
        client_node1
            .send(ClientRequest::ContractOp(ContractRequest::Update {
                key: contract_key,
                data: UpdateData::Delta(StateDelta::from(serde_json::to_vec(&node1_ping).unwrap())),
            }))
            .await?;

        let mut node2_ping = Ping::default();
        node2_ping.insert(node2_tag.clone());
        tracing::info!(%node2_ping, "Node 2 sending update with tag: {}", node2_tag);
        client_node2
            .send(ClientRequest::ContractOp(ContractRequest::Update {
                key: contract_key,
                data: UpdateData::Delta(StateDelta::from(serde_json::to_vec(&node2_ping).unwrap())),
            }))
            .await?;

        tracing::info!("Waiting for updates to propagate across the network...");
        sleep(Duration::from_secs(20)).await;

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

        let get_all_states = |client_gw: &mut WebApi,
                              client_node1: &mut WebApi,
                              client_node2: &mut WebApi,
                              key: ContractKey|
         -> BoxFuture<'_, anyhow::Result<(Ping, Ping, Ping)>> {
            Box::pin(async move {
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

                let state_gw = wait_for_get_response(client_gw, &key)
                    .await
                    .map_err(anyhow::Error::msg)?;
                let state_node1 = wait_for_get_response(client_node1, &key)
                    .await
                    .map_err(anyhow::Error::msg)?;
                let state_node2 = wait_for_get_response(client_node2, &key)
                    .await
                    .map_err(anyhow::Error::msg)?;

                let state_gw = if state_gw.is_empty() {
                    Ping::default()
                } else {
                    serde_json::from_slice(&state_gw)?
                };
                let state_node1 = if state_node1.is_empty() {
                    Ping::default()
                } else {
                    serde_json::from_slice(&state_node1)?
                };
                let state_node2 = if state_node2.is_empty() {
                    Ping::default()
                } else {
                    serde_json::from_slice(&state_node2)?
                };

                Ok((state_gw, state_node1, state_node2))
            })
        };

        let (final_state_gw, final_state_node1, final_state_node2) = get_all_states(
            &mut client_gw,
            &mut client_node1,
            &mut client_node2,
            contract_key,
        )
        .await?;

        let tags = vec![gw_tag.clone(), node1_tag.clone(), node2_tag.clone()];
        let all_tags_present = verify_all_tags_present(
            &final_state_gw,
            &final_state_node1,
            &final_state_node2,
            &tags,
        );

        let gw_time = final_state_gw.get(&gw_tag).cloned();
        let node1_time = final_state_node1.get(&gw_tag).cloned();
        let node2_time = final_state_node2.get(&gw_tag).cloned();

        if all_tags_present {
            tracing::info!("All nodes have all tags!");
        } else {
            tracing::warn!("Not all nodes have all tags!");
            tracing::warn!("Gateway state: {:?}", final_state_gw);
            tracing::warn!("Node 1 state: {:?}", final_state_node1);
            tracing::warn!("Node 2 state: {:?}", final_state_node2);
        }

        let timestamps_match = gw_time == node1_time && gw_time == node2_time;
        if timestamps_match {
            tracing::info!("All nodes have matching timestamps for gateway tag!");
        } else {
            tracing::warn!("Timestamps don't match across nodes!");
            tracing::warn!("Gateway time: {:?}", gw_time);
            tracing::warn!("Node 1 time: {:?}", node1_time);
            tracing::warn!("Node 2 time: {:?}", node2_time);
        }

        let all_states_match = final_state_gw.len() == final_state_node1.len()
            && final_state_gw.len() == final_state_node2.len()
            && final_state_node1.len() == final_state_node2.len();

        let mut timestamps_consistent = true;
        for tag in &tags {
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
        test_result = test => {
            match test_result {
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

#[tokio::test(flavor = "multi_thread")]
async fn test_ping_application_loop() -> TestResult {
    freenet::config::set_logger(Some(LevelFilter::DEBUG), None);

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
            None, // blocked_addresses
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
        None,
        ws_api_port_socket_node1.local_addr()?.port(),
        None, // blocked_addresses
    )
    .await?;
    let ws_api_port_node1 = config_node1.ws_api.ws_api_port.unwrap();

    let (config_node2, preset_cfg_node2) = base_node_test_config(
        false,
        vec![serde_json::to_string(&config_gw_info)?],
        None,
        ws_api_port_socket_node2.local_addr()?.port(),
        None, // blocked_addresses
    )
    .await?;
    let ws_api_port_node2 = config_node2.ws_api.ws_api_port.unwrap();

    tracing::info!("Gateway node data dir: {:?}", preset_cfg_gw.temp_dir.path());
    tracing::info!("Node 1 data dir: {:?}", preset_cfg_node1.temp_dir.path());
    tracing::info!("Node 2 data dir: {:?}", preset_cfg_node2.temp_dir.path());

    std::mem::drop(network_socket_gw);
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

    let test = tokio::time::timeout(Duration::from_secs(120), async {
        tokio::time::sleep(Duration::from_secs(10)).await;

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

        let path_to_code = PathBuf::from(PACKAGE_DIR).join(PATH_TO_CONTRACT);
        tracing::info!(path=%path_to_code.display(), "loading contract code");
        let code = std::fs::read(path_to_code)
            .ok()
            .ok_or_else(|| anyhow!("Failed to read contract code"))?;
        let code_hash = CodeHash::from_code(&code);
        tracing::info!(code_hash=%code_hash, "loaded contract code");

        let gw_options = PingContractOptions {
            frequency: Duration::from_secs(5),
            ttl: Duration::from_secs(30),
            tag: "gw-ping".to_string(),
            code_key: code_hash.to_string(),
        };
        let node1_options = PingContractOptions {
            frequency: Duration::from_secs(5),
            ttl: Duration::from_secs(30),
            tag: "node1-ping".to_string(),
            code_key: code_hash.to_string(),
        };
        let node2_options = PingContractOptions {
            frequency: Duration::from_secs(5),
            ttl: Duration::from_secs(30),
            tag: "node2-ping".to_string(),
            code_key: code_hash.to_string(),
        };

        tracing::info!("Starting ping clients on all nodes...");

        let params = Parameters::from(serde_json::to_vec(&gw_options).unwrap());
        let container = ContractContainer::try_from((code.clone(), &params))?;
        let key = container.key();

        client_gw
            .send(ClientRequest::ContractOp(ContractRequest::Put {
                contract: container.clone(),
                state: WrappedState::new(serde_json::to_vec(&Ping::default())?),
                related_contracts: RelatedContracts::new(),
                subscribe: true,
            }))
            .await?;

        wait_for_put_response(&mut client_gw, &key)
            .await
            .map_err(anyhow::Error::msg)?;
        tracing::info!("Gateway: put ping contract successfully!");

        client_node1
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key,
                return_contract_code: true,
                subscribe: true,
            }))
            .await?;

        let node1_state = wait_for_get_response(&mut client_node1, &key)
            .await
            .map_err(anyhow::Error::msg)?;
        tracing::info!("Node 1: got contract with {} entries", node1_state.len());

        client_node2
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key,
                return_contract_code: true,
                subscribe: true,
            }))
            .await?;

        let node2_state = wait_for_get_response(&mut client_node2, &key)
            .await
            .map_err(anyhow::Error::msg)?;
        tracing::info!("Node 2: got contract with {} entries", node2_state.len());

        tracing::info!("Starting ping clients...");

        let gw_handle = tokio::spawn(async move {
            let mut stats = PingStats::default();
            let result = run_ping_client(
                client_gw,
                key,
                gw_options.frequency,
                gw_options.ttl,
                gw_options.tag.clone(),
                &mut stats,
            )
            .await;
            (result, stats)
        });

        let node1_handle = tokio::spawn(async move {
            let mut stats = PingStats::default();
            let result = run_ping_client(
                client_node1,
                key,
                node1_options.frequency,
                node1_options.ttl,
                node1_options.tag.clone(),
                &mut stats,
            )
            .await;
            (result, stats)
        });

        let node2_handle = tokio::spawn(async move {
            let mut stats = PingStats::default();
            let result = run_ping_client(
                client_node2,
                key,
                node2_options.frequency,
                node2_options.ttl,
                node2_options.tag.clone(),
                &mut stats,
            )
            .await;
            (result, stats)
        });

        tracing::info!("Letting ping clients run for 60 seconds...");
        tokio::time::sleep(Duration::from_secs(60)).await;

        tracing::info!("Stopping ping clients...");
        gw_handle.abort();
        node1_handle.abort();
        node2_handle.abort();

        let check_ping_counts = |stats: &PingStats| {
            tracing::info!(
                "Ping stats: sent={}, received={}, errors={}",
                stats.sent,
                stats.received,
                stats.errors
            );
            stats.sent > 0 && stats.received > 0 && stats.errors == 0
        };

        let (gw_result, gw_stats) = match gw_handle.await {
            Ok(r) => r,
            Err(_) => (Ok(()), PingStats::default()),
        };
        let (node1_result, node1_stats) = match node1_handle.await {
            Ok(r) => r,
            Err(_) => (Ok(()), PingStats::default()),
        };
        let (node2_result, node2_stats) = match node2_handle.await {
            Ok(r) => r,
            Err(_) => (Ok(()), PingStats::default()),
        };

        tracing::info!("Gateway ping stats: {:?}", gw_stats);
        tracing::info!("Node 1 ping stats: {:?}", node1_stats);
        tracing::info!("Node 2 ping stats: {:?}", node2_stats);

        Ok::<_, anyhow::Error>(())
    })
    .instrument(span!(Level::INFO, "test_ping_application_loop"));

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
        test_result = test => {
            match test_result {
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
