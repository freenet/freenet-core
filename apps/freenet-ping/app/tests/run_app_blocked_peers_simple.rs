
use std::{
    collections::HashSet,
    path::PathBuf,
    time::Duration,
};

use anyhow::Context;
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, ContractResponse, HostResponse, WebApi},
    prelude::*,
};
use futures::{future::BoxFuture, FutureExt};
use rand::{Rng, SeedableRng};
type TestResult = Result<(), Box<dyn std::error::Error>>;
use tokio::{select, time::sleep};
use tracing::{info, span, Level, Instrument};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use freenet_ping_contract::{Ping, PingOptions};

#[derive(Debug, Clone)]
struct PresetConfig {}

fn base_node_test_config(
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

fn gw_config(data_dir: PathBuf, ws_api_port: u16, network_port: u16) -> NodeConfig {
    base_node_test_config(data_dir, ws_api_port, network_port, vec![])
}

fn ping_states_equal(a: &Ping, b: &Ping) -> bool {
    let a_set: HashSet<_> = a.get_all().into_iter().collect();
    let b_set: HashSet<_> = b.get_all().into_iter().collect();
    a_set == b_set
}

#[tokio::test]
async fn test_ping_blocked_peers_simple() -> TestResult {
    let subscriber = tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env());
    let _guard = subscriber.set_default();

    let temp_dir = tempfile::tempdir()?;
    let gw_dir = temp_dir.path().join("gateway");
    let node1_dir = temp_dir.path().join("node1");
    let node2_dir = temp_dir.path().join("node2");
    
    std::fs::create_dir_all(&gw_dir)?;
    std::fs::create_dir_all(&node1_dir)?;
    std::fs::create_dir_all(&node2_dir)?;

    let gw_ws_port = 50510;
    let gw_network_port = 50511;
    let node1_ws_port = 50512;
    let node1_network_port = 50513;
    let node2_ws_port = 50514;
    let node2_network_port = 50515;

    let gateway_node = {
        let config = gw_config(gw_dir, gw_ws_port, gw_network_port);
        let node = Node::new(config);
        info!("Starting gateway node");
        node.run().await
    }
    .boxed_local();

    let gateway_peer_id = {
        let uri = format!("ws://127.0.0.1:{}", gw_ws_port);
        let mut client = WebApi::connect(&uri).await?;
        let response = client.request(ClientRequest::Host).await?;
        
        if let HostResponse::Info(info) = response {
            info.peer_id
        } else {
            anyhow::bail!("Failed to get gateway peer ID");
        }
    };
    
    info!("Gateway peer ID: {}", gateway_peer_id);

    let node1 = {
        let node2_peer_id = {
            let config = base_node_test_config(
                node2_dir.clone(),
                node2_ws_port,
                node2_network_port,
                vec![],
            );
            config.identity.peer_id()
        };
        
        info!("Node 2 peer ID: {}", node2_peer_id);
        
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
        let node1_peer_id = {
            let config = base_node_test_config(
                node1_dir.clone(),
                node1_ws_port,
                node1_network_port,
                vec![],
            );
            config.identity.peer_id()
        };
        
        info!("Node 1 peer ID: {}", node1_peer_id);
        
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

    let test = tokio::time::timeout(Duration::from_secs(180), async {
        info!("Waiting for nodes to start up and establish connections...");
        tokio::time::sleep(Duration::from_secs(10)).await;

        let uri_gw = format!("ws://127.0.0.1:{}", gw_ws_port);
        let uri_node1 = format!("ws://127.0.0.1:{}", node1_ws_port);
        let uri_node2 = format!("ws://127.0.0.1:{}", node2_ws_port);
        
        let mut client_gw = WebApi::connect(&uri_gw).await?;
        let mut client_node1 = WebApi::connect(&uri_node1).await?;
        let mut client_node2 = WebApi::connect(&uri_node2).await?;
        
        info!("Connected to all nodes");

        info!("Deploying ping contract on gateway...");
        let code = include_bytes!("../../contracts/ping/build/freenet/freenet_ping_contract");
        
        let ping_options = PingOptions {
            update_frequency_ms: 1000,
            ttl_ms: 5000,
        };
        
        let mut wrapped_state = Ping::default();
        let gw_tag = "gateway-initial";
        wrapped_state.insert(gw_tag.to_string());
        
        let response = client_gw
            .request(ClientRequest::Contract(ContractRequest::Deploy {
                code: code.to_vec(),
                state: serde_json::to_vec(&wrapped_state)?,
                options: serde_json::to_vec(&ping_options)?,
            }))
            .await?;
        
        let key = if let ContractResponse::Deployed { key } = response {
            key
        } else {
            anyhow::bail!("Failed to deploy contract");
        };
        
        info!("Contract deployed with key: {}", key);
        
        info!("Node 1 subscribing to contract...");
        let response = client_node1
            .request(ClientRequest::Contract(ContractRequest::Subscribe { key }))
            .await?;
        
        let node1_state = if let ContractResponse::Subscribed { state, .. } = response {
            let ping: Ping = serde_json::from_slice(&state)?;
            ping
        } else {
            anyhow::bail!("Failed to subscribe node 1 to contract");
        };
        
        info!("Node 1 subscribed to contract");
        
        info!("Node 2 subscribing to contract...");
        let response = client_node2
            .request(ClientRequest::Contract(ContractRequest::Subscribe { key }))
            .await?;
        
        let node2_state = if let ContractResponse::Subscribed { state, .. } = response {
            let ping: Ping = serde_json::from_slice(&state)?;
            ping
        } else {
            anyhow::bail!("Failed to subscribe node 2 to contract");
        };
        
        info!("Node 2 subscribed to contract");
        
        info!("Verifying initial state...");
        assert!(
            ping_states_equal(&wrapped_state, &node1_state),
            "Node 1 initial state doesn't match gateway state"
        );
        assert!(
            ping_states_equal(&wrapped_state, &node2_state),
            "Node 2 initial state doesn't match gateway state"
        );
        
        let get_all_states = |client_gw: &mut WebApi,
                              client_node1: &mut WebApi,
                              client_node2: &mut WebApi,
                              key: ContractKey| -> BoxFuture<'_, anyhow::Result<(Ping, Ping, Ping)>> {
            Box::pin(async move {
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
            })
        };
        
        info!("Testing update propagation through gateway...");
        
        info!("Sending update from node 1...");
        let mut node1_ping = Ping::default();
        let node1_tag = "node1-update";
        node1_ping.insert(node1_tag.to_string());
        
        client_node1
            .request(ClientRequest::Contract(ContractRequest::Update {
                key,
                data: UpdateData::Delta(StateDelta::from(serde_json::to_vec(&node1_ping)?)),
            }))
            .await?;
        
        info!("Sending update from node 2...");
        let mut node2_ping = Ping::default();
        let node2_tag = "node2-update";
        node2_ping.insert(node2_tag.to_string());
        
        client_node2
            .request(ClientRequest::Contract(ContractRequest::Update {
                key,
                data: UpdateData::Delta(StateDelta::from(serde_json::to_vec(&node2_ping)?)),
            }))
            .await?;
        
        info!("Waiting for updates to propagate...");
        sleep(Duration::from_secs(15)).await;
        
        let (state_gw, state_node1, state_node2) = get_all_states(
            &mut client_gw,
            &mut client_node1,
            &mut client_node2,
            key,
        )
        .await?;
        
        info!("Gateway state: {:?}", state_gw.get_all());
        info!("Node 1 state: {:?}", state_node1.get_all());
        info!("Node 2 state: {:?}", state_node2.get_all());
        
        let gw_seen_node1 = state_gw.get_all().contains(&node1_tag.to_string());
        let gw_seen_node2 = state_gw.get_all().contains(&node2_tag.to_string());
        let node1_seen_gw = state_node1.get_all().contains(&gw_tag.to_string());
        let node1_seen_node2 = state_node1.get_all().contains(&node2_tag.to_string());
        let node2_seen_gw = state_node2.get_all().contains(&gw_tag.to_string());
        let node2_seen_node1 = state_node2.get_all().contains(&node1_tag.to_string());
        
        info!("Gateway seen Node1: {}", gw_seen_node1);
        info!("Gateway seen Node2: {}", gw_seen_node2);
        info!("Node1 seen Gateway: {}", node1_seen_gw);
        info!("Node1 seen Node2: {}", node1_seen_node2);
        info!("Node2 seen Gateway: {}", node2_seen_gw);
        info!("Node2 seen Node1: {}", node2_seen_node1);
        
        if !gw_seen_node1 || !gw_seen_node2 || !node1_seen_node2 || !node2_seen_node1 {
            info!("Some updates didn't propagate, sending final updates...");
            
            let mut gw_ping_final = Ping::default();
            let gw_final_tag = "gateway-final";
            gw_ping_final.insert(gw_final_tag.to_string());
            
            client_gw
                .request(ClientRequest::Contract(ContractRequest::Update {
                    key,
                    data: UpdateData::Delta(StateDelta::from(serde_json::to_vec(&gw_ping_final)?)),
                }))
                .await?;
            
            info!("Waiting for final updates to propagate (20 seconds)...");
            sleep(Duration::from_secs(20)).await;
            
            let (state_gw, state_node1, state_node2) = get_all_states(
                &mut client_gw,
                &mut client_node1,
                &mut client_node2,
                key,
            )
            .await?;
            
            info!("Final Gateway state: {:?}", state_gw.get_all());
            info!("Final Node 1 state: {:?}", state_node1.get_all());
            info!("Final Node 2 state: {:?}", state_node2.get_all());
            
            assert!(
                ping_states_equal(&state_gw, &state_node1),
                "Gateway and Node 1 states don't match after final update"
            );
            assert!(
                ping_states_equal(&state_gw, &state_node2),
                "Gateway and Node 2 states don't match after final update"
            );
        } else {
            assert!(
                ping_states_equal(&state_gw, &state_node1),
                "Gateway and Node 1 states don't match"
            );
            assert!(
                ping_states_equal(&state_gw, &state_node2),
                "Gateway and Node 2 states don't match"
            );
        }
        
        info!("Test completed successfully!");
        Ok::<_, anyhow::Error>(())
    })
    .instrument(span!(Level::INFO, "test_ping_blocked_peers_simple"));

    select! {
        result = test => {
            match result {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(e)) => {
                    tracing::error!("Test failed: {}", e);
                    Err(e.into())
                }
                Err(_) => {
                    tracing::error!("Test timed out!");
                    Err(anyhow::anyhow!("Test timed out").into())
                }
            }
        }
        _ = gateway_node => {
            tracing::error!("Gateway node failed");
            Err(anyhow::anyhow!("Gateway node failed").into())
        }
        _ = node1 => {
            tracing::error!("Node 1 failed");
            Err(anyhow::anyhow!("Node 1 failed").into())
        }
        _ = node2 => {
            tracing::error!("Node 2 failed");
            Err(anyhow::anyhow!("Node 2 failed").into())
        }
    }
}
