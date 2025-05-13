//
//

use std::{
    net::{Ipv4Addr, SocketAddr, TcpListener},
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
const APP_TAG: &str = "ping-app";

fn ping_states_equal(a: &Ping, b: &Ping) -> bool {
    if a.len() != b.len() {
        return false;
    }

    for key in a.keys() {
        if !b.contains_key(key) {
            return false;
        }
    }

    true
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ping_blocked_peers() -> TestResult {
    freenet::config::set_logger(Some(LevelFilter::DEBUG), None);

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

    tracing::info!("Gateway node data dir: {:?}", preset_cfg_gw.temp_dir.path());
    tracing::info!("Node 1 data dir: {:?}", preset_cfg_node1.temp_dir.path());
    tracing::info!("Node 2 data dir: {:?}", preset_cfg_node2.temp_dir.path());
    tracing::info!("Node 1 blocks: {:?}", node2_network_addr);
    tracing::info!("Node 2 blocks: {:?}", node1_network_addr);

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

    let test = tokio::time::timeout(Duration::from_secs(300), async {
        tracing::info!("Waiting for nodes to start up and establish connections...");
        tokio::time::sleep(Duration::from_secs(15)).await;

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

        tracing::info!("Connecting to Gateway at {}", uri_gw);
        let (stream_gw, _) = connect_async(&uri_gw).await?;
        tracing::info!("Connecting to Node1 at {}", uri_node1);
        let (stream_node1, _) = connect_async(&uri_node1).await?;
        tracing::info!("Connecting to Node2 at {}", uri_node2);
        let (stream_node2, _) = connect_async(&uri_node2).await?;

        let mut client_gw = WebApi::start(stream_gw);
        let mut client_node1 = WebApi::start(stream_node1);
        let mut client_node2 = WebApi::start(stream_node2);

        let path_to_code = PathBuf::from(PACKAGE_DIR).join(PATH_TO_CONTRACT);
        tracing::info!(path=%path_to_code.display(), "Loading contract code");
        let code = std::fs::read(path_to_code)
            .ok()
            .ok_or_else(|| anyhow!("Failed to read contract code"))?;
        let code_hash = CodeHash::from_code(&code);
        tracing::info!(code_hash=%code_hash, "Loaded contract code");

        let ping_options = PingContractOptions {
            frequency: Duration::from_secs(5),
            ttl: Duration::from_secs(60), // Increased TTL for more reliability
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

        let key = tokio::time::timeout(
            Duration::from_secs(30),
            wait_for_put_response(&mut client_gw, &contract_key),
        )
        .await
        .map_err(|_| anyhow!("Gateway put request timed out"))?
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

        let node1_state = tokio::time::timeout(
            Duration::from_secs(30),
            wait_for_get_response(&mut client_node1, &contract_key),
        )
        .await
        .map_err(|_| anyhow!("Node1 get request timed out"))?
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

        let node2_state = tokio::time::timeout(
            Duration::from_secs(30),
            wait_for_get_response(&mut client_node2, &contract_key),
        )
        .await
        .map_err(|_| anyhow!("Node2 get request timed out"))?
        .map_err(anyhow::Error::msg)?;

        tracing::info!("Node 2: got contract with {} entries", node2_state.len());

        tracing::info!("All nodes subscribing to contract...");

        client_gw
            .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
                key: contract_key,
                summary: None,
            }))
            .await?;

        tokio::time::timeout(
            Duration::from_secs(30),
            wait_for_subscribe_response(&mut client_gw, &contract_key),
        )
        .await
        .map_err(|_| anyhow!("Gateway subscribe request timed out"))?
        .map_err(anyhow::Error::msg)?;

        tracing::info!("Gateway: subscribed successfully!");

        client_node1
            .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
                key: contract_key,
                summary: None,
            }))
            .await?;

        tokio::time::timeout(
            Duration::from_secs(30),
            wait_for_subscribe_response(&mut client_node1, &contract_key),
        )
        .await
        .map_err(|_| anyhow!("Node1 subscribe request timed out"))?
        .map_err(anyhow::Error::msg)?;

        tracing::info!("Node 1: subscribed successfully!");

        client_node2
            .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
                key: contract_key,
                summary: None,
            }))
            .await?;

        tokio::time::timeout(
            Duration::from_secs(30),
            wait_for_subscribe_response(&mut client_node2, &contract_key),
        )
        .await
        .map_err(|_| anyhow!("Node2 subscribe request timed out"))?
        .map_err(anyhow::Error::msg)?;

        tracing::info!("Node 2: subscribed successfully!");

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
        tracing::info!("Gateway sending update with tag: {}", gw_tag);
        client_gw
            .send(ClientRequest::ContractOp(ContractRequest::Update {
                key: contract_key,
                data: UpdateData::Delta(StateDelta::from(serde_json::to_vec(&gw_ping).unwrap())),
            }))
            .await?;

        let mut node1_ping = Ping::default();
        node1_ping.insert(node1_tag.clone());
        tracing::info!("Node 1 sending update with tag: {}", node1_tag);
        client_node1
            .send(ClientRequest::ContractOp(ContractRequest::Update {
                key: contract_key,
                data: UpdateData::Delta(StateDelta::from(serde_json::to_vec(&node1_ping).unwrap())),
            }))
            .await?;

        let mut node2_ping = Ping::default();
        node2_ping.insert(node2_tag.clone());
        tracing::info!("Node 2 sending update with tag: {}", node2_tag);
        client_node2
            .send(ClientRequest::ContractOp(ContractRequest::Update {
                key: contract_key,
                data: UpdateData::Delta(StateDelta::from(serde_json::to_vec(&node2_ping).unwrap())),
            }))
            .await?;

        async fn get_all_states(
            client_gw: &mut WebApi,
            client_node1: &mut WebApi,
            client_node2: &mut WebApi,
            key: ContractKey,
        ) -> anyhow::Result<(Ping, Ping, Ping)> {
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

            let state_gw = tokio::time::timeout(
                Duration::from_secs(20),
                wait_for_get_response(client_gw, &key),
            )
            .await
            .map_err(|_| anyhow!("Gateway get request timed out"))?;

            let state_node1 = tokio::time::timeout(
                Duration::from_secs(20),
                wait_for_get_response(client_node1, &key),
            )
            .await
            .map_err(|_| anyhow!("Node1 get request timed out"))?;

            let state_node2 = tokio::time::timeout(
                Duration::from_secs(20),
                wait_for_get_response(client_node2, &key),
            )
            .await
            .map_err(|_| anyhow!("Node2 get request timed out"))?;

            let ping_gw = state_gw.map_err(|e| anyhow!("Failed to get gateway state: {}", e))?;
            let ping_node1 =
                state_node1.map_err(|e| anyhow!("Failed to get node1 state: {}", e))?;
            let ping_node2 =
                state_node2.map_err(|e| anyhow!("Failed to get node2 state: {}", e))?;

            Ok((ping_gw, ping_node1, ping_node2))
        }

        tracing::info!("Implementing robust update propagation strategy...");

        tracing::info!("Waiting for initial updates to propagate...");
        sleep(Duration::from_secs(8)).await;

        for i in 1..=3 {
            // Reduced from 5 to 3 rounds to speed up test
            let mut gw_ping_refresh = Ping::default();
            let gw_refresh_tag = format!("{}-refresh-{}", gw_tag, i);
            gw_ping_refresh.insert(gw_refresh_tag.clone());
            tracing::info!("Gateway sending refresh update {}: {}", i, gw_refresh_tag);
            client_gw
                .send(ClientRequest::ContractOp(ContractRequest::Update {
                    key: contract_key,
                    data: UpdateData::Delta(StateDelta::from(
                        serde_json::to_vec(&gw_ping_refresh).unwrap(),
                    )),
                }))
                .await?;

            let mut node1_ping_refresh = Ping::default();
            let node1_refresh_tag = format!("{}-refresh-{}", node1_tag, i);
            node1_ping_refresh.insert(node1_refresh_tag.clone());
            tracing::info!("Node1 sending refresh update {}: {}", i, node1_refresh_tag);
            client_node1
                .send(ClientRequest::ContractOp(ContractRequest::Update {
                    key: contract_key,
                    data: UpdateData::Delta(StateDelta::from(
                        serde_json::to_vec(&node1_ping_refresh).unwrap(),
                    )),
                }))
                .await?;

            let mut node2_ping_refresh = Ping::default();
            let node2_refresh_tag = format!("{}-refresh-{}", node2_tag, i);
            node2_ping_refresh.insert(node2_refresh_tag.clone());
            tracing::info!("Node2 sending refresh update {}: {}", i, node2_refresh_tag);
            client_node2
                .send(ClientRequest::ContractOp(ContractRequest::Update {
                    key: contract_key,
                    data: UpdateData::Delta(StateDelta::from(
                        serde_json::to_vec(&node2_ping_refresh).unwrap(),
                    )),
                }))
                .await?;

            sleep(Duration::from_secs(5)).await;
        }

        tracing::info!("Waiting for all updates to propagate...");
        sleep(Duration::from_secs(8)).await;

        let (state_gw, state_node1, state_node2) = get_all_states(
            &mut client_gw,
            &mut client_node1,
            &mut client_node2,
            contract_key,
        )
        .await?;

        gw_seen_node1 = gw_seen_node1 || state_gw.contains_key(&node1_tag);
        gw_seen_node2 = gw_seen_node2 || state_gw.contains_key(&node2_tag);
        node1_seen_gw = node1_seen_gw || state_node1.contains_key(&gw_tag);
        node1_seen_node2 = node1_seen_node2 || state_node1.contains_key(&node2_tag);
        node2_seen_gw = node2_seen_gw || state_node2.contains_key(&gw_tag);
        node2_seen_node1 = node2_seen_node1 || state_node2.contains_key(&node1_tag);

        tracing::info!("After initial updates:");
        tracing::info!("Gateway state: {:?}", state_gw);
        tracing::info!("Node 1 state: {:?}", state_node1);
        tracing::info!("Node 2 state: {:?}", state_node2);
        tracing::info!("Gateway seen Node1: {}", gw_seen_node1);
        tracing::info!("Gateway seen Node2: {}", gw_seen_node2);
        tracing::info!("Node1 seen Gateway: {}", node1_seen_gw);
        tracing::info!("Node1 seen Node2: {}", node1_seen_node2);
        tracing::info!("Node2 seen Gateway: {}", node2_seen_gw);
        tracing::info!("Node2 seen Node1: {}", node2_seen_node1);

        tracing::info!("Waiting longer for updates to propagate through the gateway...");
        sleep(Duration::from_secs(15)).await; // Reduced from 20 to 15 seconds

        let (state_gw, state_node1, state_node2) = get_all_states(
            &mut client_gw,
            &mut client_node1,
            &mut client_node2,
            contract_key,
        )
        .await?;

        gw_seen_node1 = gw_seen_node1 || state_gw.contains_key(&node1_tag);
        gw_seen_node2 = gw_seen_node2 || state_gw.contains_key(&node2_tag);
        node1_seen_gw = node1_seen_gw || state_node1.contains_key(&gw_tag);
        node1_seen_node2 = node1_seen_node2 || state_node1.contains_key(&node2_tag);
        node2_seen_gw = node2_seen_gw || state_node2.contains_key(&gw_tag);
        node2_seen_node1 = node2_seen_node1 || state_node2.contains_key(&node1_tag);

        tracing::info!("After waiting longer:");
        tracing::info!("Gateway state: {:?}", state_gw);
        tracing::info!("Node 1 state: {:?}", state_node1);
        tracing::info!("Node 2 state: {:?}", state_node2);
        tracing::info!("Gateway seen Node1: {}", gw_seen_node1);
        tracing::info!("Gateway seen Node2: {}", gw_seen_node2);
        tracing::info!("Node1 seen Gateway: {}", node1_seen_gw);
        tracing::info!("Node1 seen Node2: {}", node1_seen_node2);
        tracing::info!("Node2 seen Gateway: {}", node2_seen_gw);
        tracing::info!("Node2 seen Node1: {}", node2_seen_node1);

        if !gw_seen_node1
            || !gw_seen_node2
            || !node1_seen_gw
            || !node1_seen_node2
            || !node2_seen_gw
            || !node2_seen_node1
        {
            tracing::info!("Some updates still missing, sending final round of updates...");

            let mut gw_ping_final = Ping::default();
            let gw_final_tag = format!("{}-final", gw_tag);
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

            let mut node1_ping_final = Ping::default();
            let node1_final_tag = format!("{}-final", node1_tag);
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

            let mut node2_ping_final = Ping::default();
            let node2_final_tag = format!("{}-final", node2_tag);
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

            tracing::info!("Waiting for final updates to propagate (25 seconds)...");
            sleep(Duration::from_secs(25)).await; // Reduced from 30 to 25 seconds

            let (state_gw, state_node1, state_node2) = get_all_states(
                &mut client_gw,
                &mut client_node1,
                &mut client_node2,
                contract_key,
            )
            .await?;

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

            tracing::info!("After final updates:");
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
    .instrument(span!(Level::INFO, "test_ping_blocked_peers"));

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
