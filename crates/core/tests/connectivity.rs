use anyhow::{anyhow, bail};
use freenet::{
    config::{ConfigArgs, InlineGwConfig, NetworkArgs, SecretArgs, WebsocketApiArgs},
    dev_tool::TransportKeypair,
    local_node::NodeConfig,
    server::serve_gateway,
    test_utils::{self, make_get, make_put, with_peer_id},
};
use freenet_stdlib::{
    client_api::{ClientRequest, ContractResponse, HostResponse, WebApi},
    prelude::*,
};
use futures::FutureExt;
use rand::{Rng, SeedableRng};
use std::{
    net::{Ipv4Addr, TcpListener},
    sync::{LazyLock, Mutex},
    time::Duration,
};
use testresult::TestResult;
use tokio::select;
use tokio_tungstenite::connect_async;

static RNG: LazyLock<Mutex<rand::rngs::StdRng>> = LazyLock::new(|| {
    Mutex::new(rand::rngs::StdRng::from_seed(
        *b"connectivity_test_seed0123456789",
    ))
});

/// Test gateway reconnection:
/// 1. Start a gateway and a peer connected to it
/// 2. Perform operations to verify connectivity
/// 3. Force disconnect
/// 4. Verify that the peer can reconnect and operate normally
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_gateway_reconnection() -> TestResult {
    use freenet::test_utils::TestLogger;

    let _logger = TestLogger::new()
        .with_json()
        .with_level("info")
        .init();

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
    let gateway_key = TransportKeypair::new();
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
            token_ttl_seconds: None,
            token_cleanup_interval_seconds: None,
        },
        network_api: NetworkArgs {
            public_address: Some(Ipv4Addr::LOCALHOST.into()),
            public_port: Some(gateway_port),
            is_gateway: true,
            skip_load_from_network: true,
            gateways: Some(vec![]),
            location: Some(RNG.lock().unwrap().random()),
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
    let peer_key = TransportKeypair::new();
    let peer_transport_keypair = temp_dir_peer.path().join("private.pem");
    peer_key.save(&peer_transport_keypair)?;

    let gateway_info = InlineGwConfig {
        address: (Ipv4Addr::LOCALHOST, gateway_port).into(),
        location: Some(RNG.lock().unwrap().random()),
        public_key_path: temp_dir_gw.path().join("public.pem"),
    };

    let peer_config = ConfigArgs {
        ws_api: WebsocketApiArgs {
            address: Some(Ipv4Addr::LOCALHOST.into()),
            ws_api_port: Some(peer_ws_port),
            token_ttl_seconds: None,
            token_cleanup_interval_seconds: None,
        },
        network_api: NetworkArgs {
            public_address: Some(Ipv4Addr::LOCALHOST.into()),
            public_port: None,
            is_gateway: false,
            skip_load_from_network: true,
            gateways: Some(vec![serde_json::to_string(&gateway_info)?]),
            location: Some(RNG.lock().unwrap().random()),
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

    // Start gateway node with peer identification
    let gateway = async {
        let _span = with_peer_id("gateway");
        tracing::info!("Starting gateway node");
        let config = gateway_config.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        tracing::info!("Gateway node running");
        node.run().await
    }
    .boxed_local();

    // Start peer node with peer identification
    let peer = async move {
        let _span = with_peer_id("peer-1");
        tracing::info!("Starting peer node");
        let config = peer_config.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        tracing::info!("Peer node running");
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
    use freenet::test_utils::TestLogger;
    use tokio_tungstenite::connect_async;

    let _logger = TestLogger::new()
        .with_json()
        .with_level("info")
        .init();

    // Use the test utilities to create a simple network
    let network_socket = TcpListener::bind("127.0.0.1:0")?;
    let ws_socket = TcpListener::bind("127.0.0.1:0")?;
    let gateway_port = network_socket.local_addr()?.port();
    let ws_port = ws_socket.local_addr()?.port();

    // Create a simple gateway configuration
    let temp_dir = tempfile::tempdir()?;
    let key = TransportKeypair::new();
    let transport_keypair = temp_dir.path().join("private.pem");
    key.save(&transport_keypair)?;

    let config = ConfigArgs {
        ws_api: WebsocketApiArgs {
            address: Some(Ipv4Addr::LOCALHOST.into()),
            ws_api_port: Some(ws_port),
            token_ttl_seconds: None,
            token_cleanup_interval_seconds: None,
        },
        network_api: NetworkArgs {
            public_address: Some(Ipv4Addr::LOCALHOST.into()),
            public_port: Some(gateway_port),
            is_gateway: true,
            skip_load_from_network: true,
            gateways: Some(vec![]),
            location: Some(RNG.lock().unwrap().random()),
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

    // Start the gateway node with peer identification
    let gateway = async {
        let _span = with_peer_id("gateway");
        tracing::info!("Starting gateway node");
        let config = config.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        tracing::info!("Gateway node running");
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

/// Test three-node network connectivity with full mesh formation
/// This test verifies that a network of 3 nodes (1 gateway + 2 peers) can:
/// 1. Establish connections to form a full mesh
/// 2. Successfully perform PUT/GET operations across the network
///
/// # Port Configuration for P2P Mesh
///
/// For peers to participate in P2P mesh connectivity, they must have BOTH
/// `public_address` AND `public_port` configured. This ensures the peer's
/// PeerId is set from config (see config/mod.rs:242-251).
///
/// ## Port Types
///
/// - **network_port**: The local port the peer binds to for listening
/// - **public_port**: The external port peers should connect to
///   - In localhost tests (no NAT): public_port = network_port
///   - In production with NAT: public_port = router's external port
///
/// ## How It Works
///
/// ### Localhost Tests (this test)
/// 1. Peer binds UDP socket to network_port (e.g., 53425)
/// 2. When sending to gateway, UDP uses bound port as source (53425)
/// 3. Gateway sees source port 53425 in handshake
/// 4. Gateway sends back "your external address is 127.0.0.1:53425"
/// 5. Peer's PeerId is already set from config with public_port=53425
/// 6. Other peers connect directly to 127.0.0.1:53425 ✅
///
/// ### Real P2P Network (with NAT)
/// 1. Peer behind NAT binds to network_port (e.g., 8080)
/// 2. Peer sets public_port to router's external port (e.g., 54321)
/// 3. Router forwards external port 54321 → internal port 8080
/// 4. When peer sends to gateway, NAT translates:
///    - Source: 192.168.1.100:8080 → PublicIP:54321
/// 5. Gateway sees source as PublicIP:54321
/// 6. Peer's PeerId is set from config: PublicIP:54321
/// 7. Other peers connect to PublicIP:54321
/// 8. Router forwards to peer's internal 192.168.1.100:8080 ✅
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_three_node_network_connectivity() -> TestResult {
    use freenet::test_utils::TestLogger;
    use freenet_stdlib::client_api::{NodeQuery, QueryResponse};
    use std::collections::HashSet;

    let _logger = TestLogger::new()
        .with_json()
        .with_level("info")
        .init();

    // Load test contract
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();
    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    // Create network sockets
    let gateway_network_socket = TcpListener::bind("127.0.0.1:0")?;
    let gateway_ws_socket = TcpListener::bind("127.0.0.1:0")?;
    let peer1_ws_socket = TcpListener::bind("127.0.0.1:0")?;
    let peer2_ws_socket = TcpListener::bind("127.0.0.1:0")?;

    // Gateway configuration
    let temp_dir_gw = tempfile::tempdir()?;
    let gateway_key = TransportKeypair::new();
    let gateway_transport_keypair = temp_dir_gw.path().join("private.pem");
    gateway_key.save(&gateway_transport_keypair)?;
    gateway_key
        .public()
        .save(temp_dir_gw.path().join("public.pem"))?;

    let gateway_port = gateway_network_socket.local_addr()?.port();
    let gateway_ws_port = gateway_ws_socket.local_addr()?.port();
    let peer1_ws_port = peer1_ws_socket.local_addr()?.port();
    let peer2_ws_port = peer2_ws_socket.local_addr()?.port();

    // Generate a single consistent location for the gateway
    let gateway_location = RNG.lock().unwrap().random();

    let gateway_config = ConfigArgs {
        ws_api: WebsocketApiArgs {
            address: Some(Ipv4Addr::LOCALHOST.into()),
            ws_api_port: Some(gateway_ws_port),
            token_ttl_seconds: None,
            token_cleanup_interval_seconds: None,
        },
        network_api: NetworkArgs {
            public_address: Some(Ipv4Addr::LOCALHOST.into()),
            public_port: Some(gateway_port),
            is_gateway: true,
            skip_load_from_network: true,
            gateways: Some(vec![]),
            location: Some(gateway_location),
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

    // Gateway info for peers - use the SAME location as gateway config
    let gateway_info = InlineGwConfig {
        address: (Ipv4Addr::LOCALHOST, gateway_port).into(),
        location: Some(gateway_location),
        public_key_path: temp_dir_gw.path().join("public.pem"),
    };

    // First peer configuration
    let temp_dir_peer1 = tempfile::tempdir()?;
    let peer1_key = TransportKeypair::new();
    let peer1_transport_keypair = temp_dir_peer1.path().join("private.pem");
    peer1_key.save(&peer1_transport_keypair)?;

    // Allocate network port for peer1 to enable P2P mesh connectivity
    // In localhost tests, peers must bind to a specific port so the gateway
    // can see their listening address and facilitate P2P connections
    let peer1_network_socket = TcpListener::bind("127.0.0.1:0")?;
    let peer1_port = peer1_network_socket.local_addr()?.port();

    let peer1_config = ConfigArgs {
        ws_api: WebsocketApiArgs {
            address: Some(Ipv4Addr::LOCALHOST.into()),
            ws_api_port: Some(peer1_ws_port),
            token_ttl_seconds: None,
            token_cleanup_interval_seconds: None,
        },
        network_api: NetworkArgs {
            public_address: Some(Ipv4Addr::LOCALHOST.into()),
            public_port: Some(peer1_port),
            is_gateway: false,
            skip_load_from_network: true,
            gateways: Some(vec![serde_json::to_string(&gateway_info)?]),
            location: Some(RNG.lock().unwrap().random()),
            ignore_protocol_checking: true,
            address: Some(Ipv4Addr::LOCALHOST.into()),
            network_port: Some(peer1_port),
            bandwidth_limit: None,
            blocked_addresses: None,
        },
        config_paths: freenet::config::ConfigPathsArgs {
            config_dir: Some(temp_dir_peer1.path().to_path_buf()),
            data_dir: Some(temp_dir_peer1.path().to_path_buf()),
        },
        secrets: SecretArgs {
            transport_keypair: Some(peer1_transport_keypair),
            ..Default::default()
        },
        ..Default::default()
    };

    // Second peer configuration
    let temp_dir_peer2 = tempfile::tempdir()?;
    let peer2_key = TransportKeypair::new();
    let peer2_transport_keypair = temp_dir_peer2.path().join("private.pem");
    peer2_key.save(&peer2_transport_keypair)?;

    // Allocate network port for peer2 to enable P2P mesh connectivity
    let peer2_network_socket = TcpListener::bind("127.0.0.1:0")?;
    let peer2_port = peer2_network_socket.local_addr()?.port();

    let peer2_config = ConfigArgs {
        ws_api: WebsocketApiArgs {
            address: Some(Ipv4Addr::LOCALHOST.into()),
            ws_api_port: Some(peer2_ws_port),
            token_ttl_seconds: None,
            token_cleanup_interval_seconds: None,
        },
        network_api: NetworkArgs {
            public_address: Some(Ipv4Addr::LOCALHOST.into()),
            public_port: Some(peer2_port),
            is_gateway: false,
            skip_load_from_network: true,
            gateways: Some(vec![serde_json::to_string(&gateway_info)?]),
            location: Some(RNG.lock().unwrap().random()),
            ignore_protocol_checking: true,
            address: Some(Ipv4Addr::LOCALHOST.into()),
            network_port: Some(peer2_port),
            bandwidth_limit: None,
            blocked_addresses: None,
        },
        config_paths: freenet::config::ConfigPathsArgs {
            config_dir: Some(temp_dir_peer2.path().to_path_buf()),
            data_dir: Some(temp_dir_peer2.path().to_path_buf()),
        },
        secrets: SecretArgs {
            transport_keypair: Some(peer2_transport_keypair),
            ..Default::default()
        },
        ..Default::default()
    };

    // Free the sockets before starting nodes
    std::mem::drop(gateway_network_socket);
    std::mem::drop(gateway_ws_socket);
    std::mem::drop(peer1_network_socket);
    std::mem::drop(peer1_ws_socket);
    std::mem::drop(peer2_network_socket);
    std::mem::drop(peer2_ws_socket);

    // Start gateway node with peer identification
    let gateway = async {
        let _span = with_peer_id("gateway");
        tracing::info!("Starting gateway node");
        let config = gateway_config.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        tracing::info!("Gateway node running");
        node.run().await
    }
    .boxed_local();

    // Start first peer node with peer identification
    let peer1 = async move {
        let _span = with_peer_id("peer-1");
        tokio::time::sleep(Duration::from_secs(5)).await;
        tracing::info!("Starting peer 1 node");
        let config = peer1_config.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        tracing::info!("Peer 1 node running");
        node.run().await
    }
    .boxed_local();

    // Start second peer node with peer identification
    let peer2 = async move {
        let _span = with_peer_id("peer-2");
        tokio::time::sleep(Duration::from_secs(10)).await;
        tracing::info!("Starting peer 2 node");
        let config = peer2_config.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        tracing::info!("Peer 2 node running");
        node.run().await
    }
    .boxed_local();

    // Main test logic
    let test = tokio::time::timeout(Duration::from_secs(180), async move {
        // Wait for all nodes to start and connect
        tracing::info!("Waiting for nodes to start and establish connections...");
        tokio::time::sleep(Duration::from_secs(20)).await;

        // Connect to websockets
        let uri_gw =
            format!("ws://127.0.0.1:{gateway_ws_port}/v1/contract/command?encodingProtocol=native");
        let (stream_gw, _) = connect_async(&uri_gw).await?;
        let mut client_gw = WebApi::start(stream_gw);

        let uri1 =
            format!("ws://127.0.0.1:{peer1_ws_port}/v1/contract/command?encodingProtocol=native");
        let (stream1, _) = connect_async(&uri1).await?;
        let mut client1 = WebApi::start(stream1);

        let uri2 =
            format!("ws://127.0.0.1:{peer2_ws_port}/v1/contract/command?encodingProtocol=native");
        let (stream2, _) = connect_async(&uri2).await?;
        let mut client2 = WebApi::start(stream2);

        // Retry loop to wait for full mesh connectivity
        const MAX_RETRIES: usize = 30;
        const RETRY_DELAY: Duration = Duration::from_secs(2);
        let mut retry_count = 0;

        loop {
            retry_count += 1;
            if retry_count > MAX_RETRIES {
                bail!(
                    "Failed to establish full mesh connectivity after {} seconds",
                    MAX_RETRIES * 2
                );
            }

            tracing::info!(
                "Attempt {}/{}: Querying all nodes for connected peers...",
                retry_count,
                MAX_RETRIES
            );

            // Query each node for connections
            client_gw
                .send(ClientRequest::NodeQueries(NodeQuery::ConnectedPeers))
                .await?;
            let gw_resp = tokio::time::timeout(Duration::from_secs(10), client_gw.recv()).await?;
            let gw_peers = match gw_resp {
                Ok(HostResponse::QueryResponse(QueryResponse::ConnectedPeers { peers })) => peers,
                Ok(other) => bail!("Unexpected response from gateway: {:?}", other),
                Err(e) => bail!("Error receiving gateway response: {}", e),
            };

            client1
                .send(ClientRequest::NodeQueries(NodeQuery::ConnectedPeers))
                .await?;
            let peer1_resp = tokio::time::timeout(Duration::from_secs(10), client1.recv()).await?;
            let peer1_peers = match peer1_resp {
                Ok(HostResponse::QueryResponse(QueryResponse::ConnectedPeers { peers })) => peers,
                Ok(other) => bail!("Unexpected response from peer1: {:?}", other),
                Err(e) => bail!("Error receiving peer1 response: {}", e),
            };

            client2
                .send(ClientRequest::NodeQueries(NodeQuery::ConnectedPeers))
                .await?;
            let peer2_resp = tokio::time::timeout(Duration::from_secs(10), client2.recv()).await?;
            let peer2_peers = match peer2_resp {
                Ok(HostResponse::QueryResponse(QueryResponse::ConnectedPeers { peers })) => peers,
                Ok(other) => bail!("Unexpected response from peer2: {:?}", other),
                Err(e) => bail!("Error receiving peer2 response: {}", e),
            };

            tracing::info!("  - Gateway has {} connections", gw_peers.len());
            tracing::info!("  - Peer1 has {} connections", peer1_peers.len());
            tracing::info!("  - Peer2 has {} connections", peer2_peers.len());

            // Check for full mesh (each node connected to the other two)
            if gw_peers.len() >= 2 && peer1_peers.len() >= 2 && peer2_peers.len() >= 2 {
                let gw_peer_addrs: HashSet<_> = gw_peers.iter().map(|p| p.1).collect();
                let peer1_peer_addrs: HashSet<_> = peer1_peers.iter().map(|p| p.1).collect();
                let peer2_peer_addrs: HashSet<_> = peer2_peers.iter().map(|p| p.1).collect();

                let fully_connected = gw_peer_addrs.len() == 2
                    && peer1_peer_addrs.len() == 2
                    && peer2_peer_addrs.len() == 2;

                if fully_connected {
                    tracing::info!("✅ Full mesh connectivity established!");
                    break;
                }
            }

            tracing::info!("Network not fully connected yet, waiting...");
            tokio::time::sleep(RETRY_DELAY).await;
        }

        // Verify functionality with PUT/GET
        tracing::info!("Verifying network functionality with PUT/GET operations");

        make_put(&mut client1, wrapped_state.clone(), contract.clone(), false).await?;
        let resp = tokio::time::timeout(Duration::from_secs(60), client1.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
                assert_eq!(key, contract_key);
                tracing::info!("Peer1 successfully performed PUT");
            }
            Ok(Ok(other)) => bail!("Unexpected PUT response: {:?}", other),
            Ok(Err(e)) => bail!("Error receiving PUT response: {}", e),
            Err(_) => bail!("Timeout waiting for PUT response"),
        }

        make_get(&mut client2, contract_key, true, false).await?;
        let get_response = tokio::time::timeout(Duration::from_secs(60), client2.recv()).await;
        match get_response {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                contract: recv_contract,
                state: recv_state,
                ..
            }))) => {
                assert_eq!(recv_contract.as_ref().unwrap().key(), contract_key);
                assert_eq!(recv_state, wrapped_state);
                tracing::info!("✅ Peer2 successfully retrieved data from network");
            }
            Ok(Ok(other)) => bail!("Unexpected GET response: {:?}", other),
            Ok(Err(e)) => bail!("Error receiving GET response: {}", e),
            Err(_) => bail!("Timeout waiting for GET response"),
        }

        // Clean disconnect
        client_gw
            .send(ClientRequest::Disconnect { cause: None })
            .await?;
        client1
            .send(ClientRequest::Disconnect { cause: None })
            .await?;
        client2
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
        p1 = peer1 => {
            p1.map_err(|e| anyhow!("Peer1 error: {}", e))?;
            Ok(())
        }
        p2 = peer2 => {
            p2.map_err(|e| anyhow!("Peer2 error: {}", e))?;
            Ok(())
        }
        r = test => {
            r??;
            tokio::time::sleep(Duration::from_secs(3)).await;
            Ok(())
        }
    }
}
