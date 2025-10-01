//! Regression tests for isolated node functionality
//!
//! These tests ensure that isolated nodes (nodes with no peer connections) operate correctly:
//! 1. PUT operations cache contracts locally without network timeouts
//! 2. GET operations retrieve from local cache without self-routing attempts
//! 3. Complete PUT→GET workflow functions properly on isolated nodes

use freenet::{
    config::{ConfigArgs, NetworkArgs, SecretArgs, WebsocketApiArgs},
    dev_tool::TransportKeypair,
    local_node::NodeConfig,
    server::serve_gateway,
    test_utils::{load_contract, make_get, make_put, make_update},
};
use freenet_stdlib::{
    client_api::{ClientRequest, ContractResponse, HostResponse, WebApi},
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
        *b"regression_test_seed_01234567890",
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

/// Test complete PUT-then-GET workflow on isolated node
///
/// This regression test verifies isolated nodes operate correctly:
/// - PUT operations cache contracts locally without network timeouts (PR #1781)
/// - GET operations retrieve from local cache without self-routing attempts (PR #1806)
/// - Complete workflow functions properly without peer connections
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_isolated_node_put_get_workflow() -> anyhow::Result<()> {
    freenet::config::set_logger(Some(tracing::level_filters::LevelFilter::INFO), None);

    // Start a single isolated node (no peers)
    let ws_port = 50700;
    let network_port = 50701;
    let (config, _temp_dir) = create_test_node_config(true, ws_port, Some(network_port)).await?;

    // Load test contract and state
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();
    let initial_state = freenet::test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

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
        // Give node time to start - critical for proper initialization
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

        println!("Step 1: Performing PUT operation to cache contract locally");

        // Perform PUT operation - this should cache the contract locally
        let put_start = std::time::Instant::now();
        make_put(&mut client, wrapped_state.clone(), contract.clone(), false).await?;

        // Wait for PUT response
        let put_result = timeout(Duration::from_secs(30), client.recv()).await;
        let put_elapsed = put_start.elapsed();

        match put_result {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
                assert_eq!(key, contract_key);
                println!("PUT operation successful in {:?}", put_elapsed);
            }
            Ok(Ok(other)) => {
                panic!("Unexpected PUT response: {:?}", other);
            }
            Ok(Err(e)) => {
                panic!("PUT operation failed: {}", e);
            }
            Err(_) => {
                panic!("PUT operation timed out");
            }
        }

        println!("Contract verified in local cache");

        println!("Step 2: Performing GET operation using local cache");

        // Now perform GET operation - should use local cache without self-routing
        let get_start = std::time::Instant::now();
        make_get(&mut client, contract_key, true, false).await?;

        // Wait for GET response
        let get_result = timeout(Duration::from_secs(10), client.recv()).await;
        let get_elapsed = get_start.elapsed();

        // REGRESSION TEST: Verify GET completed quickly using local cache
        assert!(
            get_elapsed < Duration::from_secs(5),
            "GET from local cache should be fast, not hanging on self-routing. Elapsed: {:?}",
            get_elapsed
        );

        // Verify GET succeeded with correct data
        match get_result {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                contract: recv_contract,
                state: recv_state,
                ..
            }))) => {
                assert_eq!(
                    recv_contract
                        .as_ref()
                        .expect("Contract should be present")
                        .key(),
                    contract_key
                );
                assert_eq!(recv_state, wrapped_state);
                println!(
                    "GET operation successful from local cache in {:?}",
                    get_elapsed
                );
            }
            Ok(Ok(other)) => {
                panic!("Unexpected GET response: {:?}", other);
            }
            Ok(Err(e)) => {
                panic!("GET operation failed: {}", e);
            }
            Err(_) => {
                panic!("GET operation timed out");
            }
        }

        println!("PUT-then-GET workflow completed successfully without self-routing");

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

/// Test UPDATE operation on isolated node
///
/// This regression test verifies UPDATE operations on isolated nodes:
/// - PUT operation caches contract locally
/// - UPDATE operation updates the contract state
/// - UPDATE returns UpdateResponse without timeout (issue #1884)
/// - GET operation retrieves updated state
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_isolated_node_update_operation() -> anyhow::Result<()> {
    freenet::config::set_logger(Some(tracing::level_filters::LevelFilter::DEBUG), None);

    // Start a single isolated node (no peers)
    let ws_port = 50702;
    let network_port = 50703;
    let (config, _temp_dir) = create_test_node_config(true, ws_port, Some(network_port)).await?;

    // Load test contract and state
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();
    let initial_state = freenet::test_utils::create_empty_todo_list();
    let wrapped_initial_state = WrappedState::from(initial_state);

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

        println!("Step 1: Performing PUT operation to cache contract locally");

        // Perform PUT operation - this caches the contract locally
        let put_start = std::time::Instant::now();
        make_put(
            &mut client,
            wrapped_initial_state.clone(),
            contract.clone(),
            false,
        )
        .await?;

        // Wait for PUT response
        let put_result = timeout(Duration::from_secs(30), client.recv()).await;
        let put_elapsed = put_start.elapsed();

        match put_result {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
                assert_eq!(key, contract_key);
                println!("PUT operation successful in {:?}", put_elapsed);
            }
            Ok(Ok(other)) => {
                panic!("Unexpected PUT response: {:?}", other);
            }
            Ok(Err(e)) => {
                panic!("PUT operation failed: {}", e);
            }
            Err(_) => {
                panic!("PUT operation timed out");
            }
        }

        println!("Step 2: Performing UPDATE operation with new state");

        // Create updated state (add a todo item)
        let updated_state = freenet::test_utils::create_todo_list_with_item("Test task");
        let wrapped_updated_state = WrappedState::from(updated_state);

        // Perform UPDATE operation
        let update_start = std::time::Instant::now();
        make_update(&mut client, contract_key, wrapped_updated_state.clone()).await?;

        // Wait for UPDATE response
        let update_result = timeout(Duration::from_secs(15), client.recv()).await;
        let update_elapsed = update_start.elapsed();

        // REGRESSION TEST: Verify UPDATE completed quickly without timeout (issue #1884)
        // The bug causes UPDATE to timeout after 10 seconds, so we check it completes in < 10 seconds
        assert!(
            update_elapsed < Duration::from_secs(10),
            "UPDATE should complete quickly on isolated node, not timeout. Elapsed: {:?}",
            update_elapsed
        );

        // Verify UPDATE succeeded
        match update_result {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateResponse {
                key,
                ..
            }))) => {
                assert_eq!(key, contract_key);
                println!("UPDATE operation successful in {:?}", update_elapsed);
            }
            Ok(Ok(other)) => {
                panic!("Unexpected UPDATE response: {:?}", other);
            }
            Ok(Err(e)) => {
                panic!("UPDATE operation failed: {}", e);
            }
            Err(_) => {
                panic!("UPDATE operation timed out (this is the bug in issue #1884)");
            }
        }

        println!("Step 3: Performing GET operation to verify updated state");

        // Verify the state was updated by performing a GET
        let get_start = std::time::Instant::now();
        make_get(&mut client, contract_key, true, false).await?;

        let get_result = timeout(Duration::from_secs(10), client.recv()).await;
        let get_elapsed = get_start.elapsed();

        match get_result {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                state: recv_state,
                ..
            }))) => {
                // Parse both states to verify the tasks were updated correctly
                // Note: UPDATE operations may modify the version number, so we check the tasks array
                let recv_str = String::from_utf8_lossy(recv_state.as_ref());
                println!("Received state after UPDATE: {}", recv_str);

                // Verify the state contains the expected task
                assert!(
                    recv_str.contains("\"title\":\"Test task\""),
                    "State should contain the updated task 'Test task'"
                );
                assert!(
                    recv_str.contains("\"tasks\":["),
                    "State should have tasks array"
                );

                // Verify it's not the empty state
                assert!(
                    !recv_str.contains("\"tasks\":[]"),
                    "Tasks array should not be empty after update"
                );

                println!(
                    "GET operation successful, state correctly updated in {:?}",
                    get_elapsed
                );
            }
            Ok(Ok(other)) => {
                panic!("Unexpected GET response: {:?}", other);
            }
            Ok(Err(e)) => {
                panic!("GET operation failed: {}", e);
            }
            Err(_) => {
                panic!("GET operation timed out");
            }
        }

        println!("PUT-UPDATE-GET workflow completed successfully on isolated node");

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
