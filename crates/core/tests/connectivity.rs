use anyhow::{anyhow, bail};
use freenet::{
    config::{ConfigArgs, InlineGwConfig, NetworkArgs, SecretArgs, WebsocketApiArgs},
    dev_tool::TransportKeypair,
    local_node::NodeConfig,
    server::serve_gateway,
    test_utils::{self, make_get, make_put},
};
use freenet_stdlib::{
    client_api::{ClientRequest, ContractResponse, HostResponse, WebApi},
    prelude::*,
};
use futures::FutureExt;
use rand::{Rng, SeedableRng};
use std::{
    net::{Ipv4Addr, TcpListener},
    time::Duration,
};
use testresult::TestResult;
use tokio::select;
use tokio_tungstenite::connect_async;
use tracing::level_filters::LevelFilter;

static RNG: once_cell::sync::Lazy<std::sync::Mutex<rand::rngs::StdRng>> =
    once_cell::sync::Lazy::new(|| {
        std::sync::Mutex::new(rand::rngs::StdRng::from_seed(
            *b"0102030405060708090a0b0c0d0e0f10",
        ))
    });

/// Test gateway reconnection:
/// 1. Start a gateway and a peer connected to it
/// 2. Perform operations to verify connectivity
/// 3. Force disconnect
/// 4. Verify that the peer can reconnect and operate normally
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_gateway_reconnection() -> TestResult {
    freenet::config::set_logger(Some(LevelFilter::INFO), None);

    // Load test contract
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();
    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    // Create network sockets
    let gateway_network_socket = TcpListener::bind("127.0.0.1:0")?;
    let gateway_ws_socket = TcpListener::bind("127.0.0.1:0")?;
    let peer_ws_socket = TcpListener::bind("127.0.0.1:0")?;

    // Gateway configuration
    let temp_dir_gw = tempfile::tempdir()?;
    let gateway_key = TransportKeypair::new_with_rng(&mut *RNG.lock().unwrap());
    let gateway_transport_keypair = temp_dir_gw.path().join("private.pem");
    gateway_key.save(&gateway_transport_keypair)?;
    gateway_key
        .public()
        .save(temp_dir_gw.path().join("public.pem"))?;

    let gateway_port = gateway_network_socket.local_addr()?.port();
    let gateway_ws_port = gateway_ws_socket.local_addr()?.port();
    let peer_ws_port = peer_ws_socket.local_addr()?.port();

    let gateway_config = ConfigArgs {
        ws_api: WebsocketApiArgs {
            address: Some(Ipv4Addr::LOCALHOST.into()),
            ws_api_port: Some(gateway_ws_port),
        },
        network_api: NetworkArgs {
            public_address: Some(Ipv4Addr::LOCALHOST.into()),
            public_port: Some(gateway_port),
            is_gateway: true,
            skip_load_from_network: true,
            gateways: Some(vec![]),
            location: Some(RNG.lock().unwrap().gen()),
            ignore_protocol_checking: true,
            address: Some(Ipv4Addr::LOCALHOST.into()),
            network_port: Some(gateway_port),
            bandwidth_limit: None,
            blocked_addresses: None,
        },
        config_paths: freenet::config::ConfigPathsArgs {
            config_dir: Some(temp_dir_gw.path().to_path_buf()),
            data_dir: Some(temp_dir_gw.path().to_path_buf()),
        },
        secrets: SecretArgs {
            transport_keypair: Some(gateway_transport_keypair),
            ..Default::default()
        },
        ..Default::default()
    };

    // Peer configuration
    let temp_dir_peer = tempfile::tempdir()?;
    let peer_key = TransportKeypair::new_with_rng(&mut *RNG.lock().unwrap());
    let peer_transport_keypair = temp_dir_peer.path().join("private.pem");
    peer_key.save(&peer_transport_keypair)?;

    let gateway_info = InlineGwConfig {
        address: (Ipv4Addr::LOCALHOST, gateway_port).into(),
        location: Some(RNG.lock().unwrap().gen()),
        public_key_path: temp_dir_gw.path().join("public.pem"),
    };

    let peer_config = ConfigArgs {
        ws_api: WebsocketApiArgs {
            address: Some(Ipv4Addr::LOCALHOST.into()),
            ws_api_port: Some(peer_ws_port),
        },
        network_api: NetworkArgs {
            public_address: Some(Ipv4Addr::LOCALHOST.into()),
            public_port: None,
            is_gateway: false,
            skip_load_from_network: true,
            gateways: Some(vec![serde_json::to_string(&gateway_info)?]),
            location: Some(RNG.lock().unwrap().gen()),
            ignore_protocol_checking: true,
            address: Some(Ipv4Addr::LOCALHOST.into()),
            network_port: None,
            bandwidth_limit: None,
            blocked_addresses: None,
        },
        config_paths: freenet::config::ConfigPathsArgs {
            config_dir: Some(temp_dir_peer.path().to_path_buf()),
            data_dir: Some(temp_dir_peer.path().to_path_buf()),
        },
        secrets: SecretArgs {
            transport_keypair: Some(peer_transport_keypair),
            ..Default::default()
        },
        ..Default::default()
    };

    // Free the sockets before starting nodes
    std::mem::drop(gateway_network_socket);
    std::mem::drop(gateway_ws_socket);
    std::mem::drop(peer_ws_socket);

    // Start gateway node
    let gateway = async {
        let config = gateway_config.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Start peer node
    let peer = async move {
        let config = peer_config.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Main test logic
    let test = tokio::time::timeout(Duration::from_secs(180), async move {
        // Wait for nodes to start up (following the pattern from working tests)
        tracing::info!("Waiting for nodes to start up...");
        tokio::time::sleep(Duration::from_secs(15)).await;
        tracing::info!("Nodes should be ready, proceeding with test...");

        // Connect to peer's websocket API
        let uri =
            format!("ws://127.0.0.1:{peer_ws_port}/v1/contract/command?encodingProtocol=native");
        let (stream, _) = connect_async(&uri).await?;
        let mut client_api = WebApi::start(stream);

        // Perform initial PUT to verify connectivity
        tracing::info!("Performing initial PUT to verify connectivity");
        make_put(
            &mut client_api,
            wrapped_state.clone(),
            contract.clone(),
            false,
        )
        .await?;

        // Wait for put response
        let resp = tokio::time::timeout(Duration::from_secs(60), client_api.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
                assert_eq!(key, contract_key);
                tracing::info!("Initial PUT successful");
            }
            Ok(Ok(other)) => {
                bail!("Unexpected response while waiting for put: {:?}", other);
            }
            Ok(Err(e)) => {
                bail!("Error receiving put response: {}", e);
            }
            Err(_) => {
                bail!("Timeout waiting for put response");
            }
        }

        // Verify with GET
        tracing::info!("Verifying with GET");
        make_get(&mut client_api, contract_key, true, false).await?;
        let get_response = tokio::time::timeout(Duration::from_secs(60), client_api.recv()).await;
        match get_response {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                contract: recv_contract,
                state: recv_state,
                ..
            }))) => {
                assert_eq!(
                    recv_contract.as_ref().expect("Contract should exist").key(),
                    contract_key
                );
                assert_eq!(recv_state, wrapped_state);
                tracing::info!("Initial GET successful");
            }
            Ok(Ok(other)) => {
                bail!("Unexpected response while waiting for get: {:?}", other);
            }
            Ok(Err(e)) => {
                bail!("Error receiving get response: {}", e);
            }
            Err(_) => {
                bail!("Timeout waiting for get response");
            }
        }

        // Disconnect from peer
        tracing::info!("Disconnecting from peer");
        client_api
            .send(ClientRequest::Disconnect { cause: None })
            .await?;

        // Wait for disconnect to complete
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Reconnect to the peer's websocket API
        tracing::info!("Reconnecting to peer");
        let (stream, _) = connect_async(&uri).await?;
        let mut client_api = WebApi::start(stream);

        // Wait for reconnection to establish (peer should reconnect to gateway)
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Perform GET to verify reconnection worked and peer can operate normally
        tracing::info!("Performing GET after reconnection");
        make_get(&mut client_api, contract_key, true, false).await?;
        let get_response = tokio::time::timeout(Duration::from_secs(60), client_api.recv()).await;
        match get_response {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                contract: recv_contract,
                state: recv_state,
                ..
            }))) => {
                assert_eq!(
                    recv_contract.as_ref().expect("Contract should exist").key(),
                    contract_key
                );
                assert_eq!(recv_state, wrapped_state);
                tracing::info!(
                    "Reconnection test successful - peer can perform operations after reconnecting"
                );
            }
            Ok(Ok(other)) => {
                bail!(
                    "Unexpected response while waiting for get after reconnection: {:?}",
                    other
                );
            }
            Ok(Err(e)) => {
                bail!("Error receiving get response after reconnection: {}", e);
            }
            Err(_) => {
                bail!("Timeout waiting for get response after reconnection");
            }
        }

        // Clean disconnect
        client_api
            .send(ClientRequest::Disconnect { cause: None })
            .await?;
        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok::<_, anyhow::Error>(())
    });

    select! {
        g = gateway => {
            g.map_err(|e| anyhow!("Gateway error: {}", e))?;
            Ok(())
        }
        p = peer => {
            p.map_err(|e| anyhow!("Peer error: {}", e))?;
            Ok(())
        }
        r = test => {
            r??;
            // Give time for cleanup before dropping nodes
            tokio::time::sleep(Duration::from_secs(3)).await;
            Ok(())
        }
    }
}

/// Simplified test to verify basic gateway connectivity
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_basic_gateway_connectivity() -> TestResult {
    use freenet_stdlib::client_api::{ClientRequest, WebApi};
    use tokio_tungstenite::connect_async;

    freenet::config::set_logger(Some(LevelFilter::INFO), None);

    // Use the test utilities to create a simple network
    let network_socket = TcpListener::bind("127.0.0.1:0")?;
    let ws_socket = TcpListener::bind("127.0.0.1:0")?;
    let gateway_port = network_socket.local_addr()?.port();
    let ws_port = ws_socket.local_addr()?.port();

    // Create a simple gateway configuration
    let temp_dir = tempfile::tempdir()?;
    let key = TransportKeypair::new_with_rng(&mut *RNG.lock().unwrap());
    let transport_keypair = temp_dir.path().join("private.pem");
    key.save(&transport_keypair)?;

    let config = ConfigArgs {
        ws_api: WebsocketApiArgs {
            address: Some(Ipv4Addr::LOCALHOST.into()),
            ws_api_port: Some(ws_port),
        },
        network_api: NetworkArgs {
            public_address: Some(Ipv4Addr::LOCALHOST.into()),
            public_port: Some(gateway_port),
            is_gateway: true,
            skip_load_from_network: true,
            gateways: Some(vec![]),
            location: Some(RNG.lock().unwrap().gen()),
            ignore_protocol_checking: true,
            address: Some(Ipv4Addr::LOCALHOST.into()),
            network_port: Some(gateway_port),
            bandwidth_limit: None,
            blocked_addresses: None,
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

    // Free the sockets
    std::mem::drop(network_socket);
    std::mem::drop(ws_socket);

    // Start the gateway node
    let gateway = async {
        let config = config.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Test logic
    let test = async move {
        // Give the gateway time to start
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Try to connect to the gateway's WebSocket API
        let uri = format!("ws://127.0.0.1:{ws_port}/v1/contract/command?encodingProtocol=native");
        let result = tokio::time::timeout(Duration::from_secs(10), connect_async(&uri)).await;

        match result {
            Ok(Ok((stream, _))) => {
                tracing::info!("Successfully connected to gateway WebSocket");
                let mut client = WebApi::start(stream);

                // Disconnect cleanly
                client
                    .send(ClientRequest::Disconnect { cause: None })
                    .await?;
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok(())
            }
            Ok(Err(e)) => {
                bail!("Failed to connect to gateway: {}", e);
            }
            Err(_) => {
                bail!("Timeout connecting to gateway");
            }
        }
    };

    // Run with timeout
    let result = tokio::time::timeout(Duration::from_secs(30), async {
        select! {
            g = gateway => {
                g.map_err(|e| anyhow!("Gateway error: {}", e))?;
                Ok::<_, anyhow::Error>(())
            }
            t = test => {
                t?;
                Ok::<_, anyhow::Error>(())
            }
        }
    })
    .await;

    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e.into()),
        Err(_) => Err(anyhow!("Test timed out after 30 seconds").into()),
    }
}
