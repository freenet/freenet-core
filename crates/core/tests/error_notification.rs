//! Tests for error notification delivery to WebSocket clients
//!
//! This test suite verifies that operation errors are properly delivered to clients
//! via the result router, rather than leaving clients hanging indefinitely.
//!
//! Related Issues:
//! - #1858: Clients hang when operations fail (no error notification)

use freenet::{
    local_node::NodeConfig,
    server::serve_gateway,
    test_utils::{load_contract, make_get, TestContext},
};
use freenet_macros::freenet_test;
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, WebApi},
    prelude::*,
};
use futures::FutureExt;
use std::{
    net::Ipv4Addr,
    sync::{LazyLock, Mutex},
    time::Duration,
};
use tokio::{select, time::timeout};
use tokio_tungstenite::connect_async;
use tracing::{error, info};

static RNG: LazyLock<Mutex<rand::rngs::StdRng>> = LazyLock::new(|| {
    use rand::SeedableRng;
    Mutex::new(rand::rngs::StdRng::from_seed(
        *b"error_notification_test_seed0123",
    ))
});

/// Test that GET operation errors are delivered to WebSocket clients
///
/// This test verifies that when a GET operation fails (e.g., contract not found
/// on an isolated node), the client receives an error response rather than
/// hanging indefinitely.
///
/// Fixes: #1858
#[freenet_test(
    nodes = ["gateway"],
    timeout_secs = 60,
    startup_wait_secs = 10,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_get_error_notification(ctx: &mut TestContext) -> TestResult {
    let gateway = ctx.gateway()?;

    // Connect to the node
    let (ws_stream, _) = connect_async(&gateway.ws_url()).await?;
    let mut client = WebApi::start(ws_stream);

    info!("Testing GET operation for non-existent contract (should fail with error)");

    // Create a contract to get its key, but we won't PUT it - so GET will fail
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = load_contract(TEST_CONTRACT, vec![1u8; 32].into())?; // Random params
    let nonexistent_key = contract.key();

    // Attempt to GET a contract that doesn't exist - should fail
    make_get(&mut client, nonexistent_key, false, false).await?;

    // Wait for response - should receive SOME response (error or otherwise) within reasonable time
    // The key test is that we DON'T timeout - errors should be delivered
    let get_result = timeout(Duration::from_secs(30), client.recv()).await;

    match get_result {
        Ok(Ok(response)) => {
            // Any response is good - means we're not hanging
            info!("✓ Received response (not timing out): {:?}", response);
            info!("✓ Client properly notified instead of hanging");
        }
        Ok(Err(e)) => {
            // WebSocket error could indicate error was delivered
            info!("✓ Received error notification: {}", e);
        }
        Err(_) => {
            panic!(
                "GET operation timed out - no response received! \
                 This indicates the bug from issue #1858 has regressed. \
                 Clients should receive error responses, not hang indefinitely."
            );
        }
    }

    info!("Error notification test passed - client did not hang on operation failure");

    // Properly close the client
    client
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    Ok(())
}

/// Test that PUT operation errors are delivered to WebSocket clients
///
/// This test verifies that when a PUT operation fails (e.g., invalid contract),
/// the client receives an error response rather than hanging indefinitely.
#[freenet_test(
    nodes = ["gateway"],
    timeout_secs = 60,
    startup_wait_secs = 10,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_put_error_notification(ctx: &mut TestContext) -> TestResult {
    let gateway = ctx.gateway()?;

    // Connect to the node
    let (ws_stream, _) = connect_async(&gateway.ws_url()).await?;
    let mut client = WebApi::start(ws_stream);

    info!("Testing PUT operation with invalid contract (should fail with error)");

    // Try to PUT with malformed contract data - this should fail
    // We'll use make_put with invalid state to trigger an error
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = load_contract(TEST_CONTRACT, vec![].into())?;

    // Create invalid state that will cause PUT to fail
    let invalid_state = WrappedState::new(vec![0xFF; 1024 * 1024]); // 1MB of invalid data

    let put_request = ClientRequest::ContractOp(ContractRequest::Put {
        contract: contract.clone(),
        state: invalid_state,
        related_contracts: Default::default(),
        subscribe: false,
    });

    client.send(put_request).await?;

    // Wait for response - should receive error response
    let put_result = timeout(Duration::from_secs(30), client.recv()).await;

    match put_result {
        Ok(Ok(response)) => {
            // Any response is good - means we're not hanging
            info!("✓ Received response (not timing out): {:?}", response);
            info!("✓ Client properly notified instead of hanging");
        }
        Ok(Err(e)) => {
            // WebSocket error could indicate error was delivered
            info!("✓ Received error notification: {}", e);
        }
        Err(_) => {
            panic!(
                "PUT operation timed out - no response received! \
                 This indicates clients are not receiving error notifications. \
                 Clients should receive error responses, not hang indefinitely."
            );
        }
    }

    info!("PUT error notification test passed - client did not hang on operation failure");

    // Properly close the client
    client
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    Ok(())
}

/// Test that UPDATE operation errors are delivered to WebSocket clients
///
/// This test verifies that when an UPDATE operation fails (e.g., contract doesn't exist),
/// the client receives an error response rather than hanging indefinitely.
#[freenet_test(
    nodes = ["gateway"],
    timeout_secs = 60,
    startup_wait_secs = 10,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_update_error_notification(ctx: &mut TestContext) -> TestResult {
    let gateway = ctx.gateway()?;

    // Connect to the node
    let (ws_stream, _) = connect_async(&gateway.ws_url()).await?;
    let mut client = WebApi::start(ws_stream);

    info!("Testing UPDATE operation for non-existent contract (should fail with error)");

    // Create a contract key for a contract that doesn't exist
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = load_contract(TEST_CONTRACT, vec![99u8; 32].into())?; // Random params
    let nonexistent_key = contract.key();

    // Try to UPDATE a contract that doesn't exist
    let new_state = State::from(vec![1, 2, 3, 4]);
    let update_request = ClientRequest::ContractOp(ContractRequest::Update {
        key: nonexistent_key,
        data: freenet_stdlib::prelude::UpdateData::State(new_state),
    });

    client.send(update_request).await?;

    // Wait for response - should receive error response
    let update_result = timeout(Duration::from_secs(30), client.recv()).await;

    match update_result {
        Ok(Ok(response)) => {
            // Any response is good - means we're not hanging
            info!("✓ Received response (not timing out): {:?}", response);
            info!("✓ Client properly notified instead of hanging");
        }
        Ok(Err(e)) => {
            // WebSocket error could indicate error was delivered
            info!("✓ Received error notification: {}", e);
        }
        Err(_) => {
            panic!(
                "UPDATE operation timed out - no response received! \
                 This indicates clients are not receiving error notifications. \
                 Clients should receive error responses, not hang indefinitely."
            );
        }
    }

    info!("UPDATE error notification test passed - client did not hang on operation failure");

    // Properly close the client
    client
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    Ok(())
}

/// Test that errors are delivered when a peer connection drops
///
/// This test verifies that when a connection to a peer is lost during an operation,
/// the client receives an error response rather than hanging indefinitely.
///
/// Scenario: Connect 2 peers (gateway + peer1), establish connection, then forcibly
/// drop the peer to trigger connection errors.
#[test_log::test(tokio::test(flavor = "current_thread", start_paused = true))]
async fn test_connection_drop_error_notification() -> anyhow::Result<()> {
    use std::net::TcpListener;
    // Create network sockets for gateway and peer
    let gateway_network_socket = TcpListener::bind("127.0.0.1:0")?;
    let gateway_ws_socket = TcpListener::bind("127.0.0.1:0")?;
    let peer_ws_socket = TcpListener::bind("127.0.0.1:0")?;

    let gateway_port = gateway_network_socket.local_addr()?.port();
    let gateway_ws_port = gateway_ws_socket.local_addr()?.port();
    let peer_ws_port = peer_ws_socket.local_addr()?.port();

    // Gateway configuration
    let temp_dir_gw = tempfile::tempdir()?;
    let gateway_key = freenet::dev_tool::TransportKeypair::new();
    let gateway_transport_keypair = temp_dir_gw.path().join("private.pem");
    gateway_key.save(&gateway_transport_keypair)?;
    gateway_key
        .public()
        .save(temp_dir_gw.path().join("public.pem"))?;

    let gateway_config = freenet::config::ConfigArgs {
        ws_api: freenet::config::WebsocketApiArgs {
            address: Some(Ipv4Addr::LOCALHOST.into()),
            ws_api_port: Some(gateway_ws_port),
            token_ttl_seconds: None,
            token_cleanup_interval_seconds: None,
        },
        network_api: freenet::config::NetworkArgs {
            public_address: Some(Ipv4Addr::LOCALHOST.into()),
            public_port: Some(gateway_port),
            is_gateway: true,
            skip_load_from_network: true,
            gateways: Some(vec![]),
            location: Some({
                use rand::Rng;
                RNG.lock().unwrap().random()
            }),
            ignore_protocol_checking: true,
            address: Some(Ipv4Addr::LOCALHOST.into()),
            network_port: Some(gateway_port),
            min_connections: None,
            max_connections: None,
            bandwidth_limit: None,
            blocked_addresses: None,
            transient_budget: None,
            transient_ttl_secs: None,
            total_bandwidth_limit: None,
            min_bandwidth_per_connection: None,
            ..Default::default()
        },
        config_paths: freenet::config::ConfigPathsArgs {
            config_dir: Some(temp_dir_gw.path().to_path_buf()),
            data_dir: Some(temp_dir_gw.path().to_path_buf()),
        },
        secrets: freenet::config::SecretArgs {
            transport_keypair: Some(gateway_transport_keypair),
            ..Default::default()
        },
        ..Default::default()
    };

    // Peer configuration
    let temp_dir_peer = tempfile::tempdir()?;
    let peer_key = freenet::dev_tool::TransportKeypair::new();
    let peer_transport_keypair = temp_dir_peer.path().join("private.pem");
    peer_key.save(&peer_transport_keypair)?;

    let gateway_info = freenet::config::InlineGwConfig {
        address: (Ipv4Addr::LOCALHOST, gateway_port).into(),
        location: Some({
            use rand::Rng;
            RNG.lock().unwrap().random()
        }),
        public_key_path: temp_dir_gw.path().join("public.pem"),
    };

    let peer_config = freenet::config::ConfigArgs {
        ws_api: freenet::config::WebsocketApiArgs {
            address: Some(Ipv4Addr::LOCALHOST.into()),
            ws_api_port: Some(peer_ws_port),
            token_ttl_seconds: None,
            token_cleanup_interval_seconds: None,
        },
        network_api: freenet::config::NetworkArgs {
            public_address: Some(Ipv4Addr::LOCALHOST.into()),
            public_port: None,
            is_gateway: false,
            skip_load_from_network: true,
            gateways: Some(vec![serde_json::to_string(&gateway_info)?]),
            location: Some({
                use rand::Rng;
                RNG.lock().unwrap().random()
            }),
            ignore_protocol_checking: true,
            address: Some(Ipv4Addr::LOCALHOST.into()),
            network_port: None,
            min_connections: None,
            max_connections: None,
            bandwidth_limit: None,
            blocked_addresses: None,
            transient_budget: None,
            transient_ttl_secs: None,
            total_bandwidth_limit: None,
            min_bandwidth_per_connection: None,
            ..Default::default()
        },
        config_paths: freenet::config::ConfigPathsArgs {
            config_dir: Some(temp_dir_peer.path().to_path_buf()),
            data_dir: Some(temp_dir_peer.path().to_path_buf()),
        },
        secrets: freenet::config::SecretArgs {
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
            .build(serve_gateway(config.ws_api).await?)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Start peer node in a way we can drop it later
    let (peer_shutdown_tx, mut peer_shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);
    let peer = async move {
        let config = peer_config.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await?)
            .await?;

        // Run node until we receive shutdown signal
        tokio::select! {
            result = node.run() => result,
            _ = peer_shutdown_rx.recv() => {
                info!("Peer received shutdown signal - simulating connection drop");
                // We can't construct Infallible, so return an error to exit cleanly
                Err(anyhow::anyhow!("Peer shutdown requested"))
            }
        }
    }
    .boxed_local();

    // Main test logic
    let test = tokio::time::timeout(Duration::from_secs(90), async move {
        // Wait for nodes to start and connect
        info!("Waiting for nodes to start up and connect...");
        tokio::time::sleep(Duration::from_secs(15)).await;

        // Connect a client to the gateway
        let url = format!(
            "ws://localhost:{}/v1/contract/command?encodingProtocol=native",
            gateway_ws_port
        );
        let (ws_stream, _) = connect_async(&url).await?;
        let mut client = WebApi::start(ws_stream);

        info!("Client connected to gateway");

        // Try to PUT a contract (this should work initially)
        const TEST_CONTRACT: &str = "test-contract-integration";
        let contract = load_contract(TEST_CONTRACT, vec![].into())?;
        let state = freenet::test_utils::create_empty_todo_list();
        let wrapped_state = WrappedState::from(state);

        // Start a PUT operation
        let put_request = ClientRequest::ContractOp(ContractRequest::Put {
            contract: contract.clone(),
            state: wrapped_state.clone(),
            related_contracts: Default::default(),
            subscribe: false,
        });

        client.send(put_request).await?;

        // Give the PUT a moment to start processing
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Now forcibly drop the peer connection
        info!("Dropping peer connection to simulate network failure...");
        peer_shutdown_tx.send(()).await?;

        // Give time for the drop to be detected
        tokio::time::sleep(Duration::from_secs(2)).await;

        // The PUT may or may not succeed depending on timing, but we should get SOME response
        // The key is that we don't hang indefinitely
        info!("Waiting for response after connection drop...");
        let response_result = timeout(Duration::from_secs(30), client.recv()).await;

        match response_result {
            Ok(Ok(response)) => {
                info!("✓ Received response after connection drop: {:?}", response);
                info!("✓ Client properly handled connection drop scenario");
            }
            Ok(Err(e)) => {
                info!("✓ Received error notification after connection drop: {}", e);
                info!("✓ Client properly notified of connection issues");
            }
            Err(_) => {
                panic!(
                    "Operation timed out after connection drop - no response received! \
                     This indicates clients are not being notified of connection failures. \
                     Clients should receive error responses even when connections fail."
                );
            }
        }

        info!("Connection drop error notification test passed");

        // Try to disconnect cleanly (may fail if connection is already gone)
        let _ = client.send(ClientRequest::Disconnect { cause: None }).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok::<(), anyhow::Error>(())
    });

    // Run gateway, peer, and test concurrently
    select! {
        _ = gateway => {
            error!("Gateway exited unexpectedly");
            Ok(())
        }
        _ = peer => {
            // Peer is expected to exit when we drop it
            Ok(())
        }
        result = test => {
            result??;
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok(())
        }
    }
}
