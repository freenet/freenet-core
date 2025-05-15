#![allow(unused)]
use std::{
    collections::HashSet,
    net::{Ipv4Addr, SocketAddr, TcpListener},
    path::{Path, PathBuf},
    sync::Mutex,
    time::Duration,
};

use anyhow::{anyhow, Context, Result};
use freenet::{
    config::{ConfigArgs, InlineGwConfig, NetworkArgs, SecretArgs, WebsocketApiArgs},
    dev_tool::TransportKeypair,
    local_node::NodeConfig,
    server::serve_gateway,
};
use freenet_ping_app::ping_client::{
    wait_for_get_response, wait_for_put_response, wait_for_subscribe_response,
};
use freenet_ping_types::{Ping, PingContractOptions};
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, ContractResponse, HostResponse, WebApi},
    prelude::*,
};
use futures::{future::BoxFuture, FutureExt};
use rand::{Rng, SeedableRng};
use tokio::{select, time::sleep};
use tokio_tungstenite::connect_async;
use tracing::{info, span, Instrument, Level};

pub static RNG: once_cell::sync::Lazy<Mutex<rand::rngs::StdRng>> =
    once_cell::sync::Lazy::new(|| {
        Mutex::new(rand::rngs::StdRng::from_seed(
            *b"0102030405060708090a0b0c0d0e0f10",
        ))
    });

#[derive(Debug)]
pub struct PresetConfig {
    pub temp_dir: tempfile::TempDir,
}

pub fn get_free_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

pub fn get_free_socket_addr() -> Result<SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?)
}

#[allow(clippy::too_many_arguments)]
pub async fn base_node_test_config(
    is_gateway: bool,
    gateways: Vec<String>,
    public_port: Option<u16>,
    ws_api_port: u16,
    data_dir_suffix: &str,
    base_tmp_dir: Option<&Path>,
    blocked_addresses: Option<Vec<SocketAddr>>,
) -> Result<(ConfigArgs, PresetConfig)> {
    if is_gateway {
        assert!(public_port.is_some());
    }

    let temp_dir = if let Some(base) = base_tmp_dir {
        tempfile::tempdir_in(base)?
    } else {
        tempfile::Builder::new().prefix(data_dir_suffix).tempdir()?
    };

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
            network_port: public_port, // if None, node will pick a free one or use default
            bandwidth_limit: None,
            blocked_addresses,
        },
        config_paths: freenet::config::ConfigPathsArgs {
            config_dir: Some(temp_dir.path().to_path_buf()),
            data_dir: Some(temp_dir.path().to_path_buf()),
        },
        secrets: SecretArgs {
            transport_keypair: Some(transport_keypair),
            ..Default::default()
        },
        ..Default::default()
    };
    Ok((config, PresetConfig { temp_dir }))
}

pub fn gw_config_from_path(port: u16, path: &Path) -> Result<InlineGwConfig> {
    Ok(InlineGwConfig {
        address: (Ipv4Addr::LOCALHOST, port).into(),
        location: Some(RNG.lock().unwrap().gen()),
        public_key_path: path.join("public.pem"),
    })
}

pub fn ping_states_equal(a: &Ping, b: &Ping) -> bool {
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

pub const PACKAGE_DIR: &str = env!("CARGO_MANIFEST_DIR");
pub const PATH_TO_CONTRACT: &str = "../contracts/ping/build/freenet/freenet_ping_contract";
pub const APP_TAG: &str = "ping-app";

pub async fn connect_ws_client(ws_port: u16) -> Result<WebApi> {
    let uri = format!(
        "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
        ws_port
    );
    let (stream, _) = connect_async(&uri).await?;
    Ok(WebApi::start(stream))
}

pub async fn deploy_contract(
    client: &mut WebApi,
    initial_ping_state: Ping,
    options: &PingContractOptions,
    subscribe: bool,
) -> Result<ContractKey> {
    let path_to_code = PathBuf::from(PACKAGE_DIR).join(PATH_TO_CONTRACT);
    let code = std::fs::read(path_to_code)?;
    let params = Parameters::from(serde_json::to_vec(options)?);
    let container = ContractContainer::try_from((code, &params))?;
    let contract_key = container.key();

    let wrapped_state = WrappedState::new(serde_json::to_vec(&initial_ping_state)?);

    client
        .send(ClientRequest::ContractOp(ContractRequest::Put {
            contract: container,
            state: wrapped_state,
            related_contracts: RelatedContracts::new(),
            subscribe,
        }))
        .await?;
    wait_for_put_response(client, &contract_key)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to deploy contract: {}", e))
}

pub async fn subscribe_to_contract(client: &mut WebApi, key: ContractKey) -> Result<()> {
    client
        .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
            key,
            summary: None,
        }))
        .await?;
    wait_for_subscribe_response(client, &key)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to subscribe to contract: {}", e))
}

pub async fn get_contract_state(
    client: &mut WebApi,
    key: ContractKey,
    fetch_contract: bool,
) -> Result<Ping> {
    client
        .send(ClientRequest::ContractOp(ContractRequest::Get {
            key,
            return_contract_code: fetch_contract,
            subscribe: false,
        }))
        .await?;
    wait_for_get_response(client, &key)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get contract state: {}", e))
}

pub async fn update_contract_state(
    client: &mut WebApi,
    key: ContractKey,
    delta: Ping,
) -> Result<()> {
    let delta_bytes = serde_json::to_vec(&delta)?;
    client
        .send(ClientRequest::ContractOp(ContractRequest::Update {
            key,
            data: UpdateData::Delta(StateDelta::from(delta_bytes)),
        }))
        .await?;
    // Note: Update typically doesn't have a direct response confirming the update itself,
    // propagation is checked by subsequent Gets or via subscription updates.
    Ok(())
}

pub async fn get_all_ping_states(
    client_gw: &mut WebApi,
    client_node1: &mut WebApi,
    client_node2: &mut WebApi,
    key: ContractKey,
) -> Result<(Ping, Ping, Ping)> {
    tracing::debug!("Querying all nodes for current state (key: {})...", key);

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
        Duration::from_secs(15),
        wait_for_get_response(client_gw, &key),
    )
    .await
    .map_err(|_| anyhow!("Gateway get request timed out"))?;

    let state_node1 = tokio::time::timeout(
        Duration::from_secs(15),
        wait_for_get_response(client_node1, &key),
    )
    .await
    .map_err(|_| anyhow!("Node1 get request timed out"))?;

    let state_node2 = tokio::time::timeout(
        Duration::from_secs(15),
        wait_for_get_response(client_node2, &key),
    )
    .await
    .map_err(|_| anyhow!("Node2 get request timed out"))?;

    let ping_gw = state_gw.map_err(|e| anyhow!("Failed to get gateway state: {}", e))?;
    let ping_node1 = state_node1.map_err(|e| anyhow!("Failed to get node1 state: {}", e))?;
    let ping_node2 = state_node2.map_err(|e| anyhow!("Failed to get node2 state: {}", e))?;

    tracing::debug!(
        "Received states: GW: {:?}, N1: {:?}, N2: {:?}",
        ping_gw.keys(),
        ping_node1.keys(),
        ping_node2.keys()
    );

    Ok((ping_gw, ping_node1, ping_node2))
}
