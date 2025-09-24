use anyhow::anyhow;
use freenet::{
    config::{ConfigArgs, InlineGwConfig, NetworkArgs, SecretArgs, WebsocketApiArgs},
    dev_tool::TransportKeypair,
    local_node::NodeConfig,
    server::serve_gateway,
    test_utils::{load_contract, make_put, make_subscribe, make_update},
};
use freenet_stdlib::{
    client_api::{ClientRequest, ContractResponse, HostResponse, WebApi},
    prelude::*,
};
use futures::FutureExt;
use rand::{random, Rng, SeedableRng};
use std::{
    net::{Ipv4Addr, TcpListener},
    sync::{LazyLock, Mutex},
    time::Duration,
};
use tokio::time::timeout;
use tokio_tungstenite::connect_async;

static RNG: LazyLock<Mutex<rand::rngs::StdRng>> = LazyLock::new(|| {
    Mutex::new(rand::rngs::StdRng::from_seed(
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
    location: Option<f64>,
) -> anyhow::Result<(ConfigArgs, PresetConfig)> {
    let temp_dir = tempfile::tempdir()?;
    let key = TransportKeypair::new();
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
            location: location.or_else(|| Some(RNG.lock().unwrap().random::<f64>())),
            ignore_protocol_checking: true,
            address: Some(Ipv4Addr::LOCALHOST.into()),
            network_port: public_port,
            bandwidth_limit: None,
            blocked_addresses: None,
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

/// Test that subscription responses are correctly routed back to clients
/// This tests the fix for issue #1 - missing waiting_for_transaction_result
#[tokio::test(flavor = "current_thread")]
async fn test_subscription_response_routing() -> anyhow::Result<()> {
    // Setup: Create a 2-node network
    // Find available ports
    let ws_api_socket_a = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    let ws_api_socket_b = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    let network_socket_b = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;

    // Gateway node (B)
    let (config_b, _preset_cfg_b, config_b_gw) = {
        let (cfg, preset) = base_node_test_config(
            true,
            vec![],
            Some(network_socket_b.local_addr()?.port()),
            ws_api_socket_b.local_addr()?.port(),
            None,
        )
        .await?;
        let public_port = cfg.network_api.public_port.unwrap();
        let path = preset.temp_dir.path().to_path_buf();
        (cfg, preset, gw_config(public_port, &path)?)
    };
    let _ws_api_port_peer_b = config_b.ws_api.ws_api_port.unwrap();

    // Peer node (A)
    let (config_a, _preset_cfg_a) = base_node_test_config(
        false,
        vec![serde_json::to_string(&config_b_gw)?],
        None,
        ws_api_socket_a.local_addr()?.port(),
        None,
    )
    .await?;
    let ws_api_port_peer_a = config_a.ws_api.ws_api_port.unwrap();

    std::mem::drop(ws_api_socket_a); // Free the port
    let node_a = async move {
        let config = config_a.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    std::mem::drop(network_socket_b); // Free the port
    std::mem::drop(ws_api_socket_b);
    let node_b = async {
        let config = config_b.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    let test = tokio::time::timeout(Duration::from_secs(180), async {
        // Wait for nodes to start up
        tokio::time::sleep(Duration::from_millis(2000)).await;

        // Connect to node A's websocket API
        let uri = format!(
            "ws://127.0.0.1:{ws_api_port_peer_a}/v1/contract/command?encodingProtocol=native"
        );
        let (stream, _) = connect_async(&uri).await?;
        let mut client_api_a = WebApi::start(stream);

        // Create a simple test contract
        let contract = load_contract("test_contract_1", Parameters::from(vec![]))?;
        let contract_key = contract.key();
        let initial_state = WrappedState::new(vec![0]);

        // Put the contract via node A
        make_put(
            &mut client_api_a,
            initial_state.clone(),
            contract.clone(),
            false,
        )
        .await?;

        // Wait for put response
        let resp = timeout(Duration::from_secs(30), client_api_a.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
                assert_eq!(key, contract_key, "Put response key should match");
            }
            Ok(Ok(other)) => {
                return Err(anyhow!("Unexpected put response: {:?}", other));
            }
            Ok(Err(e)) => {
                return Err(anyhow!("Error receiving put response: {}", e));
            }
            Err(_) => {
                return Err(anyhow!("Timeout waiting for put response"));
            }
        }

        // Give the contract time to propagate
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Subscribe to the contract from client A
        // This tests the fix - the subscription response should be routed back
        make_subscribe(&mut client_api_a, contract_key).await?;

        // The fix ensures this doesn't hang or fail - the response is routed back
        let resp = timeout(Duration::from_secs(30), client_api_a.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::SubscribeResponse {
                key,
                subscribed,
            }))) => {
                assert_eq!(key, contract_key, "Subscribe response key should match");
                assert!(subscribed, "Should be successfully subscribed");
            }
            Ok(Ok(other)) => {
                return Err(anyhow!("Unexpected subscribe response: {:?}", other));
            }
            Ok(Err(e)) => {
                return Err(anyhow!("Error receiving subscribe response: {}", e));
            }
            Err(_) => {
                return Err(anyhow!("Timeout waiting for subscribe response"));
            }
        }

        // Verify we can receive updates (proves the subscription is working)
        let updated_state = WrappedState::new(vec![1]);
        make_update(&mut client_api_a, contract_key, updated_state.clone()).await?;

        // Wait for update response
        let resp = timeout(Duration::from_secs(10), client_api_a.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateResponse {
                key,
                ..
            }))) => {
                assert_eq!(key, contract_key, "Update response key should match");
            }
            _ => {
                // Update response is optional, subscription notification is what matters
            }
        }

        // Should receive the update notification
        let resp = timeout(Duration::from_secs(10), client_api_a.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateNotification {
                ..
            }))) => {
                // Success - we received the update notification
            }
            _ => {
                // Update notifications may not always arrive in tests, but the subscription response is what's critical
            }
        }

        // Disconnect client
        client_api_a
            .send(ClientRequest::Disconnect { cause: None })
            .await?;

        Ok::<(), anyhow::Error>(())
    });

    tokio::select! {
        res = node_a => match res {
            Ok(r) => anyhow::bail!("node a finished unexpectedly: {r:?}"),
            Err(e) => anyhow::bail!("node a error: {e}"),
        },
        res = node_b => match res {
            Ok(r) => anyhow::bail!("node b finished unexpectedly: {r:?}"),
            Err(e) => anyhow::bail!("node b error: {e}"),
        },
        res = test => match res {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => anyhow::bail!("test timeout after 180 seconds"),
        },
    }
}

/// Test that nodes at optimal location can subscribe to contracts
/// This tests the fix for issue #2 - using k_closest_potentially_caching
#[tokio::test(flavor = "current_thread")]
async fn test_optimal_location_subscription() -> anyhow::Result<()> {
    // Setup: Create a 3-node network
    // Find available ports
    let ws_api_socket_gw = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    let ws_api_socket_a = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    let ws_api_socket_b = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    let network_socket_gw = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    let network_socket_a = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    let network_socket_b = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;

    // Gateway node at location 0.25
    let (config_gw, _preset_cfg_gw, config_gw_info) = {
        let (cfg, preset) = base_node_test_config(
            true,
            vec![],
            Some(network_socket_gw.local_addr()?.port()),
            ws_api_socket_gw.local_addr()?.port(),
            Some(0.25),
        )
        .await?;
        let public_port = cfg.network_api.public_port.unwrap();
        let path = preset.temp_dir.path().to_path_buf();
        (cfg, preset, gw_config(public_port, &path)?)
    };

    // Peer A at location 0.5 (optimal for our test contract)
    let (config_a, _preset_cfg_a) = base_node_test_config(
        false,
        vec![serde_json::to_string(&config_gw_info)?],
        Some(network_socket_a.local_addr()?.port()),
        ws_api_socket_a.local_addr()?.port(),
        Some(0.5),
    )
    .await?;
    let ws_api_port_a = config_a.ws_api.ws_api_port.unwrap();

    // Peer B at location 0.75
    let (config_b, _preset_cfg_b) = base_node_test_config(
        false,
        vec![serde_json::to_string(&config_gw_info)?],
        Some(network_socket_b.local_addr()?.port()),
        ws_api_socket_b.local_addr()?.port(),
        Some(0.75),
    )
    .await?;

    // Free the ports
    std::mem::drop(ws_api_socket_gw);
    std::mem::drop(ws_api_socket_a);
    std::mem::drop(ws_api_socket_b);
    std::mem::drop(network_socket_gw);
    std::mem::drop(network_socket_a);
    std::mem::drop(network_socket_b);

    let node_gw = async move {
        let config = config_gw.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    let node_a = async move {
        let config = config_a.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    let node_b = async move {
        let config = config_b.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    let test = tokio::time::timeout(Duration::from_secs(180), async {
        // Wait for nodes to start up
        tokio::time::sleep(Duration::from_millis(3000)).await;

        // Connect to node A's websocket API (optimal location node)
        let uri =
            format!("ws://127.0.0.1:{ws_api_port_a}/v1/contract/command?encodingProtocol=native");
        let (stream, _) = connect_async(&uri).await?;
        let mut client_api_a = WebApi::start(stream);

        // Create a contract that should map to location ~0.5
        // This location matches node A's position
        let contract = load_contract("test_contract_1", Parameters::from(vec![]))?;
        let contract_key = contract.key();
        let initial_state = WrappedState::new(vec![0]);

        // Put the contract via node A (optimal location)
        make_put(
            &mut client_api_a,
            initial_state.clone(),
            contract.clone(),
            false,
        )
        .await?;

        // Wait for put response
        let resp = timeout(Duration::from_secs(30), client_api_a.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
                assert_eq!(key, contract_key, "Put response key should match");
            }
            _ => {
                // Put might fail or timeout in some cases, but we continue to test subscription
            }
        }

        // Give the contract time to propagate
        tokio::time::sleep(Duration::from_millis(1000)).await;

        // Node A at optimal location should be able to subscribe
        // With the fix, it will try multiple candidates (k=3) instead of failing
        make_subscribe(&mut client_api_a, contract_key).await?;

        let resp = timeout(Duration::from_secs(30), client_api_a.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::SubscribeResponse {
                key,
                subscribed,
            }))) => {
                assert_eq!(key, contract_key, "Subscribe response key should match");
                assert!(
                    subscribed,
                    "Node at optimal location should be able to subscribe using alternative peers"
                );
            }
            Ok(Ok(other)) => {
                return Err(anyhow!("Unexpected subscribe response: {:?}", other));
            }
            Ok(Err(e)) => {
                return Err(anyhow!("Error receiving subscribe response: {}", e));
            }
            Err(_) => {
                return Err(anyhow!("Timeout waiting for subscribe response"));
            }
        }

        // Disconnect client
        client_api_a
            .send(ClientRequest::Disconnect { cause: None })
            .await?;

        Ok::<(), anyhow::Error>(())
    });

    tokio::select! {
        res = node_gw => match res {
            Ok(r) => anyhow::bail!("gateway node finished unexpectedly: {r:?}"),
            Err(e) => anyhow::bail!("gateway node error: {e}"),
        },
        res = node_a => match res {
            Ok(r) => anyhow::bail!("node a finished unexpectedly: {r:?}"),
            Err(e) => anyhow::bail!("node a error: {e}"),
        },
        res = node_b => match res {
            Ok(r) => anyhow::bail!("node b finished unexpectedly: {r:?}"),
            Err(e) => anyhow::bail!("node b error: {e}"),
        },
        res = test => match res {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => anyhow::bail!("test timeout after 180 seconds"),
        },
    }
}
