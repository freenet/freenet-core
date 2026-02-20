//! # Blocked Peers Test
//!
//! Tests that contract updates propagate via gateway when direct peer connections are blocked.
//! Node1 and Node2 block each other, so updates must propagate through the Gateway.
//!
//! Uses a polling loop that continuously re-sends updates and checks propagation,
//! making the test resilient to transient subscription registration failures
//! (see subscribe.rs where interest registration can silently fail if peer_key
//! lookup fails during the subscription handshake).

mod common;

use std::{
    net::{SocketAddr, TcpListener},
    time::Duration,
};

use anyhow::anyhow;
use common::{
    base_node_test_config_with_ip, get_all_ping_states, gw_config_from_path_with_ip,
    wait_for_node_connected, APP_TAG, PACKAGE_DIR, PATH_TO_CONTRACT,
};
use freenet::{
    local_node::NodeConfig,
    server::serve_client_api,
    test_utils::{allocate_test_node_block, test_ip_for_node},
};
use freenet_ping_app::ping_client::{
    wait_for_get_response, wait_for_put_response, wait_for_subscribe_response,
};
use freenet_ping_types::{Ping, PingContractOptions};
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, WebApi},
    prelude::*,
};
use futures::FutureExt;
use tokio::{select, time::sleep};
use tracing::{span, Instrument, Level};

use common::{connect_async_with_config, ws_config};

/// Maximum number of retries when port allocation fails
const MAX_PORT_RETRY_ATTEMPTS: usize = 5;

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_ping_blocked_peers() -> anyhow::Result<()> {
    let mut last_error: Option<anyhow::Error> = None;

    for attempt in 1..=MAX_PORT_RETRY_ATTEMPTS {
        match run_blocked_peers_test(attempt).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                let error_str = format!("{:#}", e);
                if error_str.contains("Address already in use")
                    || error_str.contains("os error 98")
                    || error_str.contains("os error 48")
                {
                    tracing::warn!(
                        attempt,
                        MAX_PORT_RETRY_ATTEMPTS,
                        "Port collision detected, retrying: {}",
                        error_str
                    );
                    last_error = Some(e);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                return Err(e);
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        anyhow!(
            "Port allocation failed after {} attempts",
            MAX_PORT_RETRY_ATTEMPTS
        )
    }))
}

async fn run_blocked_peers_test(attempt: usize) -> anyhow::Result<()> {
    tracing::info!(
        "Starting blocked peers test (attempt {}/{})...",
        attempt,
        MAX_PORT_RETRY_ATTEMPTS
    );

    // Network setup - use globally unique loopback IPs to avoid conflicts with
    // other tests running in parallel on CI.
    let base_node_idx = allocate_test_node_block(3);
    let gw_ip = test_ip_for_node(base_node_idx);
    let node1_ip = test_ip_for_node(base_node_idx + 1);
    let node2_ip = test_ip_for_node(base_node_idx + 2);

    let network_socket_gw = TcpListener::bind(SocketAddr::new(gw_ip.into(), 0))?;
    let gw_network_port = network_socket_gw.local_addr()?.port();

    let ws_api_port_socket_gw = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_node1 = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_node2 = TcpListener::bind("127.0.0.1:0")?;

    let network_socket_node1 = TcpListener::bind(SocketAddr::new(node1_ip.into(), 0))?;
    let node1_network_port = network_socket_node1.local_addr()?.port();
    let node1_network_addr = SocketAddr::new(node1_ip.into(), node1_network_port);

    let network_socket_node2 = TcpListener::bind(SocketAddr::new(node2_ip.into(), 0))?;
    let node2_network_port = network_socket_node2.local_addr()?.port();
    let node2_network_addr = SocketAddr::new(node2_ip.into(), node2_network_port);

    let dir_suffix = if attempt > 1 {
        format!("blocked_peers_{}", attempt)
    } else {
        "blocked_peers".to_string()
    };

    // Configure gateway
    let (config_gw, preset_cfg_gw, config_gw_info) = {
        let (cfg, preset) = base_node_test_config_with_ip(
            true,
            vec![],
            Some(gw_network_port),
            ws_api_port_socket_gw.local_addr()?.port(),
            &format!("gw_{}", dir_suffix),
            None,
            None,
            Some(gw_ip),
        )
        .await?;
        let public_port = cfg.network_api.public_port.unwrap();
        let path = preset.temp_dir.path().to_path_buf();
        (
            cfg,
            preset,
            gw_config_from_path_with_ip(public_port, &path, gw_ip)?,
        )
    };

    let ws_api_port_gw = config_gw.ws_api.ws_api_port.unwrap();

    // Configure Node1 (blocks Node2)
    let (config_node1, preset_cfg_node1) = base_node_test_config_with_ip(
        false,
        vec![serde_json::to_string(&config_gw_info)?],
        Some(node1_network_port),
        ws_api_port_socket_node1.local_addr()?.port(),
        &format!("node1_{}", dir_suffix),
        None,
        Some(vec![node2_network_addr]),
        Some(node1_ip),
    )
    .await?;
    let ws_api_port_node1 = config_node1.ws_api.ws_api_port.unwrap();

    // Configure Node2 (blocks Node1)
    let (config_node2, preset_cfg_node2) = base_node_test_config_with_ip(
        false,
        vec![serde_json::to_string(&config_gw_info)?],
        Some(node2_network_port),
        ws_api_port_socket_node2.local_addr()?.port(),
        &format!("node2_{}", dir_suffix),
        None,
        Some(vec![node1_network_addr]),
        Some(node2_ip),
    )
    .await?;
    let ws_api_port_node2 = config_node2.ws_api.ws_api_port.unwrap();

    tracing::info!("Gateway data dir: {:?}", preset_cfg_gw.temp_dir.path());
    tracing::info!("Node1 data dir: {:?}", preset_cfg_node1.temp_dir.path());
    tracing::info!("Node2 data dir: {:?}", preset_cfg_node2.temp_dir.path());

    // Free socket resources before starting nodes (race window handled by retry loop)
    std::mem::drop(network_socket_gw);
    std::mem::drop(ws_api_port_socket_gw);
    std::mem::drop(network_socket_node1);
    std::mem::drop(ws_api_port_socket_node1);
    std::mem::drop(network_socket_node2);
    std::mem::drop(ws_api_port_socket_node2);

    // Start nodes
    let gateway_node = async {
        let config = config_gw.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_client_api(config.ws_api).await?)
            .await?;
        node.run().await
    }
    .boxed_local();

    let node1 = async {
        let config = config_node1.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_client_api(config.ws_api).await?)
            .await?;
        node.run().await
    }
    .boxed_local();

    let node2 = async {
        let config = config_node2.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_client_api(config.ws_api).await?)
            .await?;
        node.run().await
    }
    .boxed_local();

    let span = span!(Level::INFO, "test_ping_blocked_peers");

    let test = tokio::time::timeout(Duration::from_secs(300), async {
        tracing::info!("Waiting 25s for nodes to start up...");
        sleep(Duration::from_secs(25)).await;

        // Connect WebSocket clients
        let uri_gw =
            format!("ws://{gw_ip}:{ws_api_port_gw}/v1/contract/command?encodingProtocol=native");
        let uri_node1 = format!(
            "ws://{node1_ip}:{ws_api_port_node1}/v1/contract/command?encodingProtocol=native"
        );
        let uri_node2 = format!(
            "ws://{node2_ip}:{ws_api_port_node2}/v1/contract/command?encodingProtocol=native"
        );

        tracing::info!("Connecting to Gateway at {}", uri_gw);
        let (stream_gw, _) = connect_async_with_config(&uri_gw, Some(ws_config()), false).await?;
        let mut client_gw = WebApi::start(stream_gw);

        tracing::info!("Connecting to Node1 at {}", uri_node1);
        let (stream_node1, _) =
            connect_async_with_config(&uri_node1, Some(ws_config()), false).await?;
        let mut client_node1 = WebApi::start(stream_node1);

        tracing::info!("Connecting to Node2 at {}", uri_node2);
        let (stream_node2, _) =
            connect_async_with_config(&uri_node2, Some(ws_config()), false).await?;
        let mut client_node2 = WebApi::start(stream_node2);

        // Wait for ring connections (180s for slow CI runners)
        tracing::info!("Waiting for nodes to join the ring...");
        wait_for_node_connected(&mut client_node1, "Node1", 1, 180).await?;
        wait_for_node_connected(&mut client_node2, "Node2", 1, 180).await?;
        tracing::info!("All nodes connected!");

        // Deploy contract on gateway
        let path_to_code = std::path::PathBuf::from(PACKAGE_DIR).join(PATH_TO_CONTRACT);

        let temp_options = PingContractOptions {
            frequency: Duration::from_secs(3),
            ttl: Duration::from_secs(30),
            tag: APP_TAG.to_string(),
            code_key: String::new(),
        };
        let temp_params = Parameters::from(serde_json::to_vec(&temp_options).unwrap());
        let temp_container = common::load_contract(&path_to_code, temp_params)?;
        let code_hash = CodeHash::from_code(temp_container.data());

        let ping_options = PingContractOptions {
            frequency: Duration::from_secs(3),
            ttl: Duration::from_secs(30),
            tag: APP_TAG.to_string(),
            code_key: code_hash.to_string(),
        };
        let params = Parameters::from(serde_json::to_vec(&ping_options).unwrap());
        let container = common::load_contract(&path_to_code, params)?;
        let contract_key = container.key();

        tracing::info!("Gateway putting contract...");
        client_gw
            .send(ClientRequest::ContractOp(ContractRequest::Put {
                contract: container.clone(),
                state: WrappedState::new(serde_json::to_vec(&Ping::default())?),
                related_contracts: RelatedContracts::new(),
                subscribe: false,
                blocking_subscribe: false,
            }))
            .await?;

        tokio::time::timeout(
            Duration::from_secs(60),
            wait_for_put_response(&mut client_gw, &contract_key),
        )
        .await
        .map_err(|_| anyhow!("Gateway put timed out"))?
        .map_err(anyhow::Error::msg)?;
        tracing::info!("Gateway: contract deployed!");

        // Node1 and Node2 get the contract
        for (client, name) in [(&mut client_node1, "Node1"), (&mut client_node2, "Node2")] {
            tracing::info!("{} getting contract...", name);
            client
                .send(ClientRequest::ContractOp(ContractRequest::Get {
                    key: *contract_key.id(),
                    return_contract_code: true,
                    subscribe: false,
                    blocking_subscribe: false,
                }))
                .await?;

            tokio::time::timeout(
                Duration::from_secs(60),
                wait_for_get_response(client, &contract_key),
            )
            .await
            .map_err(|_| anyhow!("{} get timed out", name))?
            .map_err(anyhow::Error::msg)?;
            tracing::info!("{}: got contract!", name);
        }

        // Subscribe all nodes
        tracing::info!("All nodes subscribing...");
        for (client, name) in [
            (&mut client_gw as &mut WebApi, "Gateway"),
            (&mut client_node1, "Node1"),
            (&mut client_node2, "Node2"),
        ] {
            client
                .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
                    key: *contract_key.id(),
                    summary: None,
                }))
                .await?;
            tokio::time::timeout(
                Duration::from_secs(60),
                wait_for_subscribe_response(client, &contract_key),
            )
            .await
            .map_err(|_| anyhow!("{} subscribe timed out", name))?
            .map_err(anyhow::Error::msg)?;
            tracing::info!("{}: subscribed!", name);
        }

        // Wait for subscription interest to be registered at the home node
        sleep(Duration::from_secs(5)).await;

        // Tags for each node's updates
        let gw_tag = "ping-from-gw";
        let node1_tag = "ping-from-node1";
        let node2_tag = "ping-from-node2";

        // Polling loop: send updates, check propagation, re-subscribe if stuck.
        // This handles the case where subscription interest registration silently
        // fails (subscribe.rs:705-716 - peer_key lookup can return None if the
        // peer isn't fully registered yet).
        let poll_interval = Duration::from_secs(5);
        let max_polls: u32 = 24; // 120s total

        for poll in 1..=max_polls {
            // Send an update from each node
            send_update(&mut client_gw, contract_key, gw_tag).await?;
            send_update(&mut client_node1, contract_key, node1_tag).await?;
            send_update(&mut client_node2, contract_key, node2_tag).await?;

            sleep(poll_interval).await;

            // Check current state on all nodes
            let (state_gw, state_node1, state_node2) = get_all_ping_states(
                &mut client_gw,
                &mut client_node1,
                &mut client_node2,
                contract_key,
            )
            .await?;

            let gw_has_n1 = state_gw.contains_key(node1_tag);
            let gw_has_n2 = state_gw.contains_key(node2_tag);
            let n1_has_gw = state_node1.contains_key(gw_tag);
            let n1_has_n2 = state_node1.contains_key(node2_tag);
            let n2_has_gw = state_node2.contains_key(gw_tag);
            let n2_has_n1 = state_node2.contains_key(node1_tag);

            tracing::info!(
                "Poll {}/{}: GW[n1={},n2={}] N1[gw={},n2={}] N2[gw={},n1={}]",
                poll,
                max_polls,
                gw_has_n1,
                gw_has_n2,
                n1_has_gw,
                n1_has_n2,
                n2_has_gw,
                n2_has_n1,
            );

            if gw_has_n1 && gw_has_n2 && n1_has_gw && n1_has_n2 && n2_has_gw && n2_has_n1 {
                tracing::info!("All updates propagated after {} polls!", poll);
                return Ok(());
            }

            // Re-subscribe every 6 polls (30s) in case subscription interest
            // was silently lost during the initial handshake
            if poll % 6 == 0 {
                tracing::info!("Re-subscribing all nodes (poll {})...", poll);
                for client in [&mut client_gw, &mut client_node1, &mut client_node2] {
                    let _ = client
                        .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
                            key: *contract_key.id(),
                            summary: None,
                        }))
                        .await;
                }
                // Give subscriptions time to propagate before next update round
                sleep(Duration::from_secs(3)).await;
            }
        }

        // Propagation failed - get final state for diagnostics
        let (state_gw, state_node1, state_node2) = get_all_ping_states(
            &mut client_gw,
            &mut client_node1,
            &mut client_node2,
            contract_key,
        )
        .await?;

        Err(anyhow!(
            "Updates did not fully propagate after {} polls.\n\
             Gateway keys: {:?}\n\
             Node1 keys: {:?}\n\
             Node2 keys: {:?}",
            max_polls,
            state_gw.keys().collect::<Vec<_>>(),
            state_node1.keys().collect::<Vec<_>>(),
            state_node2.keys().collect::<Vec<_>>(),
        ))
    })
    .instrument(span);

    select! {
        gw = gateway_node => {
            let Err(gw) = gw;
            Err(anyhow!("Gateway node failed: {}", gw))
        }
        n1 = node1 => {
            let Err(n1) = n1;
            Err(anyhow!("Node1 failed: {}", n1))
        }
        n2 = node2 => {
            let Err(n2) = n2;
            Err(anyhow!("Node2 failed: {}", n2))
        }
        t = test => {
            match t {
                Ok(Ok(())) => {
                    tracing::info!("Test passed!");
                    Ok(())
                }
                Ok(Err(e)) => {
                    tracing::error!("Test failed: {}", e);
                    Err(e)
                }
                Err(_) => {
                    tracing::error!("Test timed out!");
                    Err(anyhow!("Test timed out"))
                }
            }
        }
    }
}

async fn send_update(
    client: &mut WebApi,
    contract_key: ContractKey,
    tag: &str,
) -> anyhow::Result<()> {
    let mut ping = Ping::default();
    ping.insert(tag.to_string());
    client
        .send(ClientRequest::ContractOp(ContractRequest::Update {
            key: contract_key,
            data: UpdateData::Delta(StateDelta::from(serde_json::to_vec(&ping).unwrap())),
        }))
        .await?;
    Ok(())
}
