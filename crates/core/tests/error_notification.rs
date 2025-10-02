//! Tests for error notification delivery to WebSocket clients
//!
//! This test suite verifies that operation errors are properly delivered to clients
//! via the result router, rather than leaving clients hanging indefinitely.
//!
//! Related Issues:
//! - #1858: Clients hang when operations fail (no error notification)

use freenet::{
    config::{ConfigArgs, NetworkArgs, SecretArgs, WebsocketApiArgs},
    dev_tool::TransportKeypair,
    local_node::NodeConfig,
    server::serve_gateway,
    test_utils::{load_contract, make_get},
};
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
use tracing::error;

static RNG: LazyLock<Mutex<rand::rngs::StdRng>> = LazyLock::new(|| {
    use rand::SeedableRng;
    Mutex::new(rand::rngs::StdRng::from_seed(
        *b"error_notification_test_seed0123",
    ))
});

/// Helper to create a node configuration for testing
async fn create_test_node_config(
    is_gateway: bool,
    ws_api_port: u16,
    network_port: Option<u16>,
) -> anyhow::Result<(ConfigArgs, tempfile::TempDir)> {
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
            public_port: network_port,
            is_gateway,
            skip_load_from_network: true,
            gateways: Some(vec![]), // Empty gateways for isolated node
            location: Some({
                use rand::Rng;
                RNG.lock().unwrap().random()
            }),
            ignore_protocol_checking: true,
            address: Some(Ipv4Addr::LOCALHOST.into()),
            network_port,
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

    Ok((config, temp_dir))
}

/// Test that GET operation errors are delivered to WebSocket clients
///
/// This test verifies that when a GET operation fails (e.g., contract not found
/// on an isolated node), the client receives an error response rather than
/// hanging indefinitely.
///
/// Fixes: #1858
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_get_error_notification() -> anyhow::Result<()> {
    freenet::config::set_logger(Some(tracing::level_filters::LevelFilter::INFO), None);

    // Start a single isolated node (no peers)
    let ws_port = 50900;
    let network_port = 50901;
    let (config, _temp_dir) = create_test_node_config(true, ws_port, Some(network_port)).await?;

    // Start the node
    let node_handle = {
        let config = config.clone();
        async move {
            let built_config = config.build().await?;
            let node = NodeConfig::new(built_config.clone())
                .await?
                .build(serve_gateway(built_config.ws_api).await)
                .await?;
            node.run().await
        }
        .boxed_local()
    };

    // Run the test with timeout
    let test_result = timeout(Duration::from_secs(60), async {
        // Give node time to start
        println!("Waiting for node to start up...");
        tokio::time::sleep(Duration::from_secs(10)).await;
        println!("Node should be ready, proceeding with test...");

        // Connect to the node
        let url = format!(
            "ws://localhost:{}/v1/contract/command?encodingProtocol=native",
            ws_port
        );
        let (ws_stream, _) = connect_async(&url).await?;
        let mut client = WebApi::start(ws_stream);

        println!("Testing GET operation for non-existent contract (should fail with error)");

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
                println!("✓ Received response (not timing out): {:?}", response);
                println!("✓ Client properly notified instead of hanging");
            }
            Ok(Err(e)) => {
                // WebSocket error could indicate error was delivered
                println!("✓ Received error notification: {}", e);
            }
            Err(_) => {
                panic!(
                    "GET operation timed out - no response received! \
                     This indicates the bug from issue #1858 has regressed. \
                     Clients should receive error responses, not hang indefinitely."
                );
            }
        }

        println!("Error notification test passed - client did not hang on operation failure");

        // Properly close the client
        client
            .send(ClientRequest::Disconnect { cause: None })
            .await?;
        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok::<(), anyhow::Error>(())
    });

    // Run node and test concurrently
    select! {
        _ = node_handle => {
            error!("Node exited unexpectedly");
            panic!("Node should not exit during test");
        }
        result = test_result => {
            result??;
            // Give time for cleanup
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    Ok(())
}

/// Test that PUT operation errors are delivered to WebSocket clients
///
/// This test verifies that when a PUT operation fails (e.g., invalid contract),
/// the client receives an error response rather than hanging indefinitely.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_put_error_notification() -> anyhow::Result<()> {
    freenet::config::set_logger(Some(tracing::level_filters::LevelFilter::INFO), None);

    // Start a single isolated node (no peers)
    let ws_port = 50910;
    let network_port = 50911;
    let (config, _temp_dir) = create_test_node_config(true, ws_port, Some(network_port)).await?;

    // Start the node
    let node_handle = {
        let config = config.clone();
        async move {
            let built_config = config.build().await?;
            let node = NodeConfig::new(built_config.clone())
                .await?
                .build(serve_gateway(built_config.ws_api).await)
                .await?;
            node.run().await
        }
        .boxed_local()
    };

    // Run the test with timeout
    let test_result = timeout(Duration::from_secs(60), async {
        // Give node time to start
        println!("Waiting for node to start up...");
        tokio::time::sleep(Duration::from_secs(10)).await;
        println!("Node should be ready, proceeding with test...");

        // Connect to the node
        let url = format!(
            "ws://localhost:{}/v1/contract/command?encodingProtocol=native",
            ws_port
        );
        let (ws_stream, _) = connect_async(&url).await?;
        let mut client = WebApi::start(ws_stream);

        println!("Testing PUT operation with invalid contract (should fail with error)");

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
                println!("✓ Received response (not timing out): {:?}", response);
                println!("✓ Client properly notified instead of hanging");
            }
            Ok(Err(e)) => {
                // WebSocket error could indicate error was delivered
                println!("✓ Received error notification: {}", e);
            }
            Err(_) => {
                panic!(
                    "PUT operation timed out - no response received! \
                     This indicates clients are not receiving error notifications. \
                     Clients should receive error responses, not hang indefinitely."
                );
            }
        }

        println!("PUT error notification test passed - client did not hang on operation failure");

        // Properly close the client
        client
            .send(ClientRequest::Disconnect { cause: None })
            .await?;
        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok::<(), anyhow::Error>(())
    });

    // Run node and test concurrently
    select! {
        _ = node_handle => {
            error!("Node exited unexpectedly");
            panic!("Node should not exit during test");
        }
        result = test_result => {
            result??;
            // Give time for cleanup
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    Ok(())
}

/// Test that UPDATE operation errors are delivered to WebSocket clients
///
/// This test verifies that when an UPDATE operation fails (e.g., contract doesn't exist),
/// the client receives an error response rather than hanging indefinitely.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_update_error_notification() -> anyhow::Result<()> {
    freenet::config::set_logger(Some(tracing::level_filters::LevelFilter::INFO), None);

    // Start a single isolated node (no peers)
    let ws_port = 50920;
    let network_port = 50921;
    let (config, _temp_dir) = create_test_node_config(true, ws_port, Some(network_port)).await?;

    // Start the node
    let node_handle = {
        let config = config.clone();
        async move {
            let built_config = config.build().await?;
            let node = NodeConfig::new(built_config.clone())
                .await?
                .build(serve_gateway(built_config.ws_api).await)
                .await?;
            node.run().await
        }
        .boxed_local()
    };

    // Run the test with timeout
    let test_result = timeout(Duration::from_secs(60), async {
        // Give node time to start
        println!("Waiting for node to start up...");
        tokio::time::sleep(Duration::from_secs(10)).await;
        println!("Node should be ready, proceeding with test...");

        // Connect to the node
        let url = format!(
            "ws://localhost:{}/v1/contract/command?encodingProtocol=native",
            ws_port
        );
        let (ws_stream, _) = connect_async(&url).await?;
        let mut client = WebApi::start(ws_stream);

        println!("Testing UPDATE operation for non-existent contract (should fail with error)");

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
                println!("✓ Received response (not timing out): {:?}", response);
                println!("✓ Client properly notified instead of hanging");
            }
            Ok(Err(e)) => {
                // WebSocket error could indicate error was delivered
                println!("✓ Received error notification: {}", e);
            }
            Err(_) => {
                panic!(
                    "UPDATE operation timed out - no response received! \
                     This indicates clients are not receiving error notifications. \
                     Clients should receive error responses, not hang indefinitely."
                );
            }
        }

        println!(
            "UPDATE error notification test passed - client did not hang on operation failure"
        );

        // Properly close the client
        client
            .send(ClientRequest::Disconnect { cause: None })
            .await?;
        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok::<(), anyhow::Error>(())
    });

    // Run node and test concurrently
    select! {
        _ = node_handle => {
            error!("Node exited unexpectedly");
            panic!("Node should not exit during test");
        }
        result = test_result => {
            result??;
            // Give time for cleanup
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    Ok(())
}
