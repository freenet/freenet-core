use std::{
    collections::HashSet,
    net::{Ipv4Addr, SocketAddr, TcpListener},
    path::PathBuf,
    time::Duration,
};

use anyhow::{anyhow, Context, Result};
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
use rand::{Rng, SeedableRng};
use tokio::{select, time::sleep};
use tracing::{info, span, Instrument, Level};

pub fn base_node_test_config(
    data_dir: PathBuf,
    ws_api_port: u16,
    network_port: u16,
    blocked_peers: Vec<PeerId>,
) -> NodeConfig {
    let mut rng = rand::rngs::StdRng::from_entropy();
    let seed: [u8; 32] = rng.gen();

    let mut config = NodeConfig::new(data_dir, seed);

    config.network.ws_api_port = ws_api_port;
    config.network.port = network_port;
    config.network.blocked_peers = blocked_peers;

    config.network.connect_timeout = Duration::from_secs(5);
    config.network.operation_timeout = Duration::from_secs(10);

    config
}

pub fn gw_config(data_dir: PathBuf, ws_api_port: u16, network_port: u16) -> NodeConfig {
    base_node_test_config(data_dir, ws_api_port, network_port, vec![])
}

pub fn ping_states_equal(a: &Ping, b: &Ping) -> bool {
    let a_set: HashSet<_> = a.get_all().into_iter().collect();
    let b_set: HashSet<_> = b.get_all().into_iter().collect();
    a_set == b_set
}

pub async fn get_all_states(
    client_gw: &mut WebApi,
    client_node1: &mut WebApi,
    client_node2: &mut WebApi,
    key: ContractKey,
) -> Result<(Ping, Ping, Ping)> {
    info!("Querying all nodes for current state...");

    let response = client_gw
        .request(ClientRequest::Contract(ContractRequest::Get { key }))
        .await
        .context("Failed to get gateway state")?;

    let state_gw = if let ContractResponse::State { state, .. } = response {
        serde_json::from_slice(&state).context("Failed to deserialize gateway state")?
    } else {
        anyhow::bail!("Unexpected response from gateway");
    };

    let response = client_node1
        .request(ClientRequest::Contract(ContractRequest::Get { key }))
        .await
        .context("Failed to get node 1 state")?;

    let state_node1 = if let ContractResponse::State { state, .. } = response {
        serde_json::from_slice(&state).context("Failed to deserialize node 1 state")?
    } else {
        anyhow::bail!("Unexpected response from node 1");
    };

    let response = client_node2
        .request(ClientRequest::Contract(ContractRequest::Get { key }))
        .await
        .context("Failed to get node 2 state")?;

    let state_node2 = if let ContractResponse::State { state, .. } = response {
        serde_json::from_slice(&state).context("Failed to deserialize node 2 state")?
    } else {
        anyhow::bail!("Unexpected response from node 2");
    };

    Ok((state_gw, state_node1, state_node2))
}

pub async fn setup_test_network(
    temp_dir: &std::path::Path,
) -> Result<(
    BoxFuture<'static, ()>,
    BoxFuture<'static, ()>,
    BoxFuture<'static, ()>,
    PeerId,
    PeerId,
    PeerId,
)> {
    let gw_dir = temp_dir.join("gateway");
    let node1_dir = temp_dir.join("node1");
    let node2_dir = temp_dir.join("node2");

    std::fs::create_dir_all(&gw_dir)?;
    std::fs::create_dir_all(&node1_dir)?;
    std::fs::create_dir_all(&node2_dir)?;

    let gw_ws_port = 50510;
    let gw_network_port = 50511;
    let node1_ws_port = 50512;
    let node1_network_port = 50513;
    let node2_ws_port = 50514;
    let node2_network_port = 50515;

    let gw_config = gw_config(gw_dir.clone(), gw_ws_port, gw_network_port);
    let gateway_peer_id = gw_config.identity.peer_id();

    let node1_config =
        base_node_test_config(node1_dir.clone(), node1_ws_port, node1_network_port, vec![]);
    let node1_peer_id = node1_config.identity.peer_id();

    let node2_config =
        base_node_test_config(node2_dir.clone(), node2_ws_port, node2_network_port, vec![]);
    let node2_peer_id = node2_config.identity.peer_id();

    let gateway_node = {
        let config = gw_config;
        let node = Node::new(config);
        info!("Starting gateway node");
        node.run().await
    }
    .boxed_local();

    let node1 = {
        let config = base_node_test_config(
            node1_dir,
            node1_ws_port,
            node1_network_port,
            vec![node2_peer_id],
        );

        let node = Node::new(config);
        info!("Starting node 1");
        node.run().await
    }
    .boxed_local();

    let node2 = {
        let config = base_node_test_config(
            node2_dir,
            node2_ws_port,
            node2_network_port,
            vec![node1_peer_id],
        );

        let node = Node::new(config);
        info!("Starting node 2");
        node.run().await
    }
    .boxed_local();

    Ok((
        gateway_node,
        node1,
        node2,
        gateway_peer_id,
        node1_peer_id,
        node2_peer_id,
    ))
}

pub async fn connect_to_nodes(
    gw_ws_port: u16,
    node1_ws_port: u16,
    node2_ws_port: u16,
) -> Result<(WebApi, WebApi, WebApi)> {
    let uri_gw = format!("ws://127.0.0.1:{}", gw_ws_port);
    let uri_node1 = format!("ws://127.0.0.1:{}", node1_ws_port);
    let uri_node2 = format!("ws://127.0.0.1:{}", node2_ws_port);

    let client_gw = WebApi::connect(&uri_gw).await?;
    let client_node1 = WebApi::connect(&uri_node1).await?;
    let client_node2 = WebApi::connect(&uri_node2).await?;

    info!("Connected to all nodes");

    Ok((client_gw, client_node1, client_node2))
}

pub async fn deploy_ping_contract(
    client_gw: &mut WebApi,
    code: Vec<u8>,
    ping_options: PingOptions,
    initial_state: Ping,
) -> Result<ContractKey> {
    info!("Deploying ping contract on gateway...");

    let response = client_gw
        .request(ClientRequest::Contract(ContractRequest::Deploy {
            code,
            state: serde_json::to_vec(&initial_state)?,
            options: serde_json::to_vec(&ping_options)?,
        }))
        .await?;

    let key = if let ContractResponse::Deployed { key } = response {
        key
    } else {
        anyhow::bail!("Failed to deploy contract");
    };

    info!("Contract deployed with key: {}", key);
    Ok(key)
}

pub async fn subscribe_to_contract(
    client: &mut WebApi,
    key: ContractKey,
    node_name: &str,
) -> Result<Ping> {
    info!("{} subscribing to contract...", node_name);
    let response = client
        .request(ClientRequest::Contract(ContractRequest::Subscribe { key }))
        .await?;

    let state = if let ContractResponse::Subscribed { state, .. } = response {
        let ping: Ping = serde_json::from_slice(&state)?;
        ping
    } else {
        anyhow::bail!("Failed to subscribe {} to contract", node_name);
    };

    info!("{} subscribed to contract", node_name);
    Ok(state)
}

pub async fn send_update(
    client: &mut WebApi,
    key: ContractKey,
    ping: Ping,
    node_name: &str,
) -> Result<()> {
    info!("Sending update from {}...", node_name);

    client
        .request(ClientRequest::Contract(ContractRequest::Update {
            key,
            data: UpdateData::Delta(StateDelta::from(serde_json::to_vec(&ping)?)),
        }))
        .await?;

    info!("Update sent from {}", node_name);
    Ok(())
}
