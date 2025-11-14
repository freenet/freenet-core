use anyhow::{anyhow, bail, ensure};
use freenet::{
    config::{ConfigArgs, InlineGwConfig, NetworkArgs, SecretArgs, WebsocketApiArgs},
    dev_tool::TransportKeypair,
    local_node::NodeConfig,
    server::serve_gateway,
    test_utils::{
        self, load_delegate, make_get, make_put, make_subscribe, make_update,
        verify_contract_exists, TestContext,
    },
};
use freenet_macros::freenet_test;
use freenet_stdlib::{
    client_api::{ClientRequest, ContractResponse, HostResponse, QueryResponse, WebApi},
    prelude::*,
};
use futures::FutureExt;
use rand::{random, Rng, SeedableRng};
use serde::Deserialize;
use std::{
    net::{Ipv4Addr, TcpListener},
    path::Path,
    sync::{LazyLock, Mutex},
    time::Duration,
};
use testresult::TestResult;
use tokio::select;
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
) -> anyhow::Result<(ConfigArgs, PresetConfig)> {
    const _DEFAULT_RATE_LIMIT: usize = 1024 * 1024 * 10; // 10 MB/s

    if is_gateway {
        assert!(public_port.is_some());
    }

    let temp_dir = tempfile::tempdir()?;
    let key = TransportKeypair::new();
    let transport_keypair = temp_dir.path().join("private.pem");
    key.save(&transport_keypair)?;
    key.public().save(temp_dir.path().join("public.pem"))?;
    let config = ConfigArgs {
        ws_api: WebsocketApiArgs {
            address: Some(Ipv4Addr::LOCALHOST.into()),
            ws_api_port: Some(ws_api_port),
            token_ttl_seconds: None,
            token_cleanup_interval_seconds: None,
        },
        network_api: NetworkArgs {
            public_address: Some(Ipv4Addr::LOCALHOST.into()),
            public_port,
            is_gateway,
            skip_load_from_network: true,
            gateways: Some(gateways),
            location: Some(RNG.lock().unwrap().random()),
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

fn gw_config(port: u16, path: &Path) -> anyhow::Result<InlineGwConfig> {
    Ok(InlineGwConfig {
        address: (Ipv4Addr::LOCALHOST, port).into(),
        location: Some(random()),
        public_key_path: path.join("public.pem"),
    })
}

async fn get_contract(
    client: &mut WebApi,
    key: ContractKey,
    temp_dir: impl AsRef<Path>,
) -> anyhow::Result<(ContractContainer, WrappedState)> {
    make_get(client, key, true, false).await?;
    loop {
        let resp = tokio::time::timeout(Duration::from_secs(30), client.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                key,
                contract: Some(contract),
                state,
            }))) => {
                verify_contract_exists(temp_dir.as_ref(), key).await?;
                return Ok((contract, state));
            }
            Ok(Ok(other)) => {
                tracing::warn!("unexpected response while waiting for get: {:?}", other);
            }
            Ok(Err(e)) => {
                bail!("Error receiving get response: {}", e);
            }
            Err(_) => {
                bail!("Timeout waiting for get response");
            }
        }
    }
}

async fn send_put_with_retry(
    client: &mut WebApi,
    state: WrappedState,
    contract: ContractContainer,
    description: &str,
    expected_key: Option<ContractKey>,
) -> anyhow::Result<()> {
    const MAX_ATTEMPTS: usize = 3;
    for attempt in 1..=MAX_ATTEMPTS {
        tracing::info!("Sending {} (attempt {attempt}/{MAX_ATTEMPTS})", description);

        make_put(client, state.clone(), contract.clone(), false).await?;

        match tokio::time::timeout(Duration::from_secs(120), client.recv()).await {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
                if let Some(expected) = expected_key {
                    ensure!(
                        key == expected,
                        "{} returned unexpected contract key (expected {}, got {})",
                        description,
                        expected,
                        key
                    );
                }
                tracing::info!("{description} succeeded on attempt {attempt}");
                return Ok(());
            }
            Ok(Ok(other)) => {
                tracing::warn!(
                    "{} attempt {attempt} returned unexpected response: {:?}",
                    description,
                    other
                );
            }
            Ok(Err(e)) => {
                tracing::warn!(
                    "{} attempt {attempt} failed while receiving response: {}",
                    description,
                    e
                );
            }
            Err(_) => {
                tracing::warn!(
                    "{} attempt {attempt} timed out waiting for response",
                    description
                );
            }
        }

        if attempt == MAX_ATTEMPTS {
            bail!("{description} failed after {MAX_ATTEMPTS} attempts");
        }

        // Drain any stray responses/errors before retrying to keep the client state clean.
        loop {
            match tokio::time::timeout(Duration::from_millis(200), client.recv()).await {
                Ok(Ok(resp)) => {
                    tracing::warn!(
                        "Discarding stray response prior to retrying {}: {:?}",
                        description,
                        resp
                    );
                }
                Ok(Err(err)) => {
                    tracing::warn!(
                        "Discarding stray error prior to retrying {}: {}",
                        description,
                        err
                    );
                }
                Err(_) => break,
            }
        }

        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    unreachable!("send_put_with_retry loop should always return or bail");
}

/// Test PUT operation across two peers (gateway and peer)
#[freenet_test(
    nodes = ["gateway", "peer-a"],
    auto_connect_peers = true,
    timeout_secs = 180,
    startup_wait_secs = 15,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4,
    aggregate_events = "always"
)]
async fn test_put_contract(ctx: &mut TestContext) -> TestResult {
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    let peer_a = ctx.node("peer-a")?;
    let gateway = ctx.node("gateway")?;
    let ws_api_port_peer_a = peer_a.ws_port;
    let ws_api_port_peer_b = gateway.ws_port;

    tracing::info!("Node A (peer-a) ws_port: {}", ws_api_port_peer_a);
    tracing::info!("Node B (gateway) ws_port: {}", ws_api_port_peer_b);

    // Give extra time for peer to connect to gateway
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Connect to node A's websocket API
    let uri =
        format!("ws://127.0.0.1:{ws_api_port_peer_a}/v1/contract/command?encodingProtocol=native");
    let (stream, _) = connect_async(&uri).await?;
    let mut client_api_a = WebApi::start(stream);

    make_put(
        &mut client_api_a,
        wrapped_state.clone(),
        contract.clone(),
        false,
    )
    .await?;

    // Wait for put response (increased timeout for CI environments)
    tracing::info!("Waiting for PUT response...");
    let resp = tokio::time::timeout(Duration::from_secs(120), client_api_a.recv()).await;
    match resp {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
            tracing::info!("PUT successful for contract: {}", key);
            assert_eq!(key, contract_key);
        }
        Ok(Ok(other)) => {
            tracing::warn!("unexpected response while waiting for put: {:?}", other);
        }
        Ok(Err(e)) => {
            bail!("Error receiving put response: {}", e);
        }
        Err(_) => {
            bail!("Timeout waiting for put response after 120 seconds");
        }
    }

    {
        // Wait for get response from node A
        tracing::info!("getting contract from A");
        let (response_contract, response_state) =
            get_contract(&mut client_api_a, contract_key, &gateway.temp_dir_path).await?;
        let response_key = response_contract.key();

        // Verify the responses
        assert_eq!(response_key, contract_key);
        assert_eq!(response_contract, contract);
        assert_eq!(response_state, wrapped_state);
    }

    {
        // Connect to node B's websocket API
        let uri = format!(
            "ws://127.0.0.1:{ws_api_port_peer_b}/v1/contract/command?encodingProtocol=native"
        );
        let (stream, _) = connect_async(&uri).await?;
        let mut client_api_b = WebApi::start(stream);

        // Wait for get response from node B
        let (response_contract, response_state) =
            get_contract(&mut client_api_b, contract_key, &gateway.temp_dir_path).await?;
        let response_key = response_contract.key();

        // Verify the responses
        assert_eq!(response_key, contract_key);
        assert_eq!(response_contract, contract);
        assert_eq!(response_state, wrapped_state);

        // Properly close the client
        client_api_b
            .send(ClientRequest::Disconnect { cause: None })
            .await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Close the first client as well
    client_api_a
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    Ok(())
}

#[freenet_test(
    nodes = ["gateway", "peer-a"],
    auto_connect_peers = true,
    timeout_secs = 180,
    startup_wait_secs = 20,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_update_contract(ctx: &mut TestContext) -> TestResult {
    // Load test contract
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    // Create initial state with empty todo list
    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    let peer_a = ctx.node("peer-a")?;
    let gateway = ctx.node("gateway")?;
    let ws_api_port = peer_a.ws_port;

    // Log data directories for debugging
    tracing::info!("Node A (peer-a) data dir: {:?}", peer_a.temp_dir_path);
    tracing::info!("Node B (gw) data dir: {:?}", gateway.temp_dir_path);

    // Give extra time for peer to connect to gateway
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Connect to node A websocket API
    let uri = format!("ws://127.0.0.1:{ws_api_port}/v1/contract/command?encodingProtocol=native");
    let (stream, _) = connect_async(&uri).await?;
    let mut client_api_a = WebApi::start(stream);

    // Put contract with initial state
    make_put(
        &mut client_api_a,
        wrapped_state.clone(),
        contract.clone(),
        false,
    )
    .await?;

    // Wait for put response (increased timeout for CI environments)
    tracing::info!("Waiting for PUT response...");
    let resp = tokio::time::timeout(Duration::from_secs(120), client_api_a.recv()).await;
    match resp {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
            tracing::info!("PUT successful for contract: {}", key);
            assert_eq!(key, contract_key, "Contract key mismatch in PUT response");
        }
        Ok(Ok(other)) => {
            tracing::warn!("unexpected response while waiting for put: {:?}", other);
        }
        Ok(Err(e)) => {
            bail!("Error receiving put response: {}", e);
        }
        Err(_) => {
            bail!("Timeout waiting for put response after 120 seconds");
        }
    }

    // Create a new to-do list by deserializing the current state, adding a task, and serializing it back
    let mut todo_list: test_utils::TodoList = serde_json::from_slice(wrapped_state.as_ref())
        .unwrap_or_else(|_| test_utils::TodoList {
            tasks: Vec::new(),
            version: 0,
        });

    // Add a task directly to the list
    todo_list.tasks.push(test_utils::Task {
        id: 1,
        title: "Implement contract".to_string(),
        description: "Create a smart contract for the todo list".to_string(),
        completed: false,
        priority: 3,
    });

    // Serialize the updated list back to bytes
    let updated_bytes = serde_json::to_vec(&todo_list).unwrap();
    let updated_state = WrappedState::from(updated_bytes);

    let expected_version_after_update = todo_list.version + 1;

    make_update(&mut client_api_a, contract_key, updated_state.clone()).await?;

    // Wait for update response
    let resp = tokio::time::timeout(Duration::from_secs(30), client_api_a.recv()).await;
    match resp {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateResponse {
            key,
            summary: _,
        }))) => {
            assert_eq!(
                key, contract_key,
                "Contract key mismatch in UPDATE response"
            );
        }
        Ok(Ok(other)) => {
            bail!("unexpected response while waiting for update: {:?}", other);
        }
        Ok(Err(e)) => {
            bail!("Client A: Error receiving update response: {}", e);
        }
        Err(_) => {
            bail!("Client A: Timeout waiting for update response");
        }
    }

    // Verify the updated state with GET
    {
        // Wait for get response from node A
        let (response_contract, response_state) =
            get_contract(&mut client_api_a, contract_key, &gateway.temp_dir_path).await?;

        assert_eq!(
            response_contract.key(),
            contract_key,
            "Contract key mismatch in GET response"
        );
        assert_eq!(
            response_contract, contract,
            "Contract content mismatch in GET response"
        );

        // Compare the deserialized updated content
        let response_todo_list: test_utils::TodoList =
            serde_json::from_slice(response_state.as_ref())
                .expect("Failed to deserialize response state");

        let expected_todo_list: test_utils::TodoList =
            serde_json::from_slice(updated_state.as_ref())
                .expect("Failed to deserialize expected state");

        assert_eq!(
            response_todo_list.version, expected_version_after_update,
            "Version should match"
        );

        assert_eq!(
            response_todo_list.tasks.len(),
            expected_todo_list.tasks.len(),
            "Number of tasks should match"
        );

        // Verify that the task exists and has the correct values
        assert_eq!(response_todo_list.tasks.len(), 1, "Should have one task");
        assert_eq!(response_todo_list.tasks[0].id, 1, "Task ID should be 1");
        assert_eq!(
            response_todo_list.tasks[0].title, "Implement contract",
            "Task title should match"
        );

        tracing::info!(
            "Successfully verified updated state for contract {}",
            contract_key
        );

        // Print states for debugging
        tracing::debug!(
            "Response state: {:?}, Expected state: {:?}",
            response_todo_list,
            expected_todo_list
        );
    }

    Ok(())
}

/// Test that a second PUT to an already cached contract persists the merged state.
/// This is a regression test for issue #1995.
#[freenet_test(
    nodes = ["gateway", "peer-a"],
    auto_connect_peers = true,
    timeout_secs = 180,
    startup_wait_secs = 15,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_put_merge_persists_state(ctx: &mut TestContext) -> TestResult {
    // Load test contract
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    // Create initial state with empty todo list
    let initial_state = test_utils::create_empty_todo_list();
    let initial_wrapped_state = WrappedState::from(initial_state);

    let peer_a = ctx.node("peer-a")?;
    let gateway = ctx.node("gateway")?;
    let ws_api_port_peer_a = peer_a.ws_port;
    let ws_api_port_peer_b = gateway.ws_port;

    tracing::info!("Node A data dir: {:?}", peer_a.temp_dir_path);
    tracing::info!("Node B (gw) data dir: {:?}", gateway.temp_dir_path);

    // Give extra time for peer to connect to gateway
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Connect to node A's websocket API
    let uri =
        format!("ws://127.0.0.1:{ws_api_port_peer_a}/v1/contract/command?encodingProtocol=native");
    let (stream, _) = connect_async(&uri).await?;
    let mut client_api_a = WebApi::start(stream);

    send_put_with_retry(
        &mut client_api_a,
        initial_wrapped_state.clone(),
        contract.clone(),
        "first PUT (cache seed)",
        Some(contract_key),
    )
    .await?;

    // Wait a bit to ensure state is fully cached
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Create updated state with more data (simulating a state merge)
    let mut updated_todo_list: test_utils::TodoList =
        serde_json::from_slice(initial_wrapped_state.as_ref()).unwrap();

    // Add multiple tasks to make the state larger
    for i in 1..=5 {
        updated_todo_list.tasks.push(test_utils::Task {
            id: i,
            title: format!("Task {}", i),
            description: format!("Description for task {}", i),
            completed: false,
            priority: i as u8,
        });
    }

    let updated_bytes = serde_json::to_vec(&updated_todo_list).unwrap();
    let updated_wrapped_state = WrappedState::from(updated_bytes);

    tracing::info!(
        "Initial state size: {} bytes, Updated state size: {} bytes",
        initial_wrapped_state.as_ref().len(),
        updated_wrapped_state.as_ref().len()
    );

    send_put_with_retry(
        &mut client_api_a,
        updated_wrapped_state.clone(),
        contract.clone(),
        "second PUT (merge)",
        Some(contract_key),
    )
    .await?;

    // Wait a bit to ensure the merge and persistence completes
    tokio::time::sleep(Duration::from_secs(2)).await;

    // The key test: GET from gateway to verify it persisted the merged state
    // This is the bug from issue #1995 - gateway receives PUT for already-cached
    // contract, merges state, but doesn't persist it
    let uri =
        format!("ws://127.0.0.1:{ws_api_port_peer_b}/v1/contract/command?encodingProtocol=native");
    let (stream, _) = connect_async(&uri).await?;
    let mut client_api_gateway = WebApi::start(stream);

    tracing::info!("Getting contract from gateway to verify merged state was persisted...");
    let (response_contract_gw, response_state_gw) = get_contract(
        &mut client_api_gateway,
        contract_key,
        &gateway.temp_dir_path,
    )
    .await?;

    assert_eq!(response_contract_gw.key(), contract_key);

    let response_todo_list_gw: test_utils::TodoList =
        serde_json::from_slice(response_state_gw.as_ref())
            .expect("Failed to deserialize state from gateway");

    tracing::info!(
        "Gateway returned state with {} tasks, size {} bytes",
        response_todo_list_gw.tasks.len(),
        response_state_gw.as_ref().len()
    );

    // This is the key assertion for issue #1995:
    // Gateway received a PUT for an already-cached contract, merged the states,
    // and should have PERSISTED the merged state (not just computed it)
    assert_eq!(
        response_todo_list_gw.tasks.len(),
        5,
        "Gateway should return merged state with 5 tasks (issue #1995: merged state must be persisted)"
    );

    // Verify the state size matches as additional confirmation
    assert_eq!(
        response_state_gw.as_ref().len(),
        updated_wrapped_state.as_ref().len(),
        "Gateway state size should match the updated state"
    );

    tracing::info!(
        "✓ Test passed: Gateway correctly persisted merged state after second PUT (issue #1995 fixed)"
    );

    // Cleanup
    client_api_a
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    client_api_gateway
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    Ok(())
}

// This test is disabled due to race conditions in subscription propagation logic.
// The test expects multiple clients across different nodes to receive subscription updates,
// but the PUT caching refactor (commits 2cd337b5-0d432347) changed the subscription semantics.
// Re-enabled after recent fixes to subscription logic - previously exhibited race conditions.
// If this test becomes flaky again, see issue #1798 for historical context.
// Ignored again due to recurring flakiness - fails intermittently with timeout waiting for
// cross-node subscription notifications (Client 3 timeout). See issue #1798.
#[ignore]
#[freenet_test(
    nodes = ["gateway", "node-a", "node-b"],
    auto_connect_peers = true,
    timeout_secs = 600,
    startup_wait_secs = 40,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_multiple_clients_subscription(ctx: &mut TestContext) -> TestResult {
    // Load test contract
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    // Create initial state with empty todo list
    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    // Get node information from context
    let node_a = ctx.node("node-a")?;
    let node_b = ctx.node("node-b")?;
    let gateway = ctx.node("gateway")?;
    let ws_api_port_a = node_a.ws_port;
    let ws_api_port_b = node_b.ws_port;

    // Log data directories for debugging
    tracing::info!("Node A data dir: {:?}", node_a.temp_dir_path);
    tracing::info!("Gateway data dir: {:?}", gateway.temp_dir_path);
    tracing::info!("Node B data dir: {:?}", node_b.temp_dir_path);

    // Give extra time for peers to connect to gateway
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Connect first client to node A's websocket API
    tracing::info!("Starting WebSocket connections after 40s startup wait");
    let start_time = std::time::Instant::now();
    let uri_a =
        format!("ws://127.0.0.1:{ws_api_port_a}/v1/contract/command?encodingProtocol=native");
    let (stream1, _) = connect_async(&uri_a).await?;
    let mut client_api1_node_a = WebApi::start(stream1);

    // Connect second client to node A's websocket API
    let (stream2, _) = connect_async(&uri_a).await?;
    let mut client_api2_node_a = WebApi::start(stream2);

    // Connect third client to node C's websocket API (different node)
    let uri_c =
        format!("ws://127.0.0.1:{ws_api_port_b}/v1/contract/command?encodingProtocol=native");
    let (stream3, _) = connect_async(&uri_c).await?;
    let mut client_api_node_b = WebApi::start(stream3);

    // First client puts contract with initial state (without subscribing)
    tracing::info!(
        "Client 1: Starting PUT operation (elapsed: {:?})",
        start_time.elapsed()
    );
    make_put(
        &mut client_api1_node_a,
        wrapped_state.clone(),
        contract.clone(),
        false, // subscribe=false - no automatic subscription
    )
    .await?;

    // Wait for put response
    loop {
        let resp = tokio::time::timeout(Duration::from_secs(120), client_api1_node_a.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
                assert_eq!(key, contract_key, "Contract key mismatch in PUT response");
                tracing::info!(
                    "Client 1: PUT completed successfully (elapsed: {:?})",
                    start_time.elapsed()
                );
                break;
            }
            Ok(Ok(other)) => {
                tracing::warn!("unexpected response while waiting for put: {:?}", other);
            }
            Ok(Err(e)) => {
                bail!("Error receiving put response: {}", e);
            }
            Err(_) => {
                bail!("Timeout waiting for put response");
            }
        }
    }

    // Explicitly subscribe client 1 to the contract using make_subscribe
    make_subscribe(&mut client_api1_node_a, contract_key).await?;

    // Wait for subscribe response
    loop {
        let resp = tokio::time::timeout(Duration::from_secs(30), client_api1_node_a.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::SubscribeResponse {
                key,
                subscribed,
            }))) => {
                assert_eq!(
                    key, contract_key,
                    "Contract key mismatch in SUBSCRIBE response"
                );
                assert!(subscribed, "Failed to subscribe to contract");
                tracing::info!("Client 1: Successfully subscribed to contract {}", key);
                break;
            }
            Ok(Ok(other)) => {
                tracing::warn!(
                    "Client 1: unexpected response while waiting for subscribe: {:?}",
                    other
                );
            }
            Ok(Err(e)) => {
                bail!("Client 1: Error receiving subscribe response: {}", e);
            }
            Err(_) => {
                bail!("Client 1: Timeout waiting for subscribe response");
            }
        }
    }

    // Second client gets the contract (without subscribing)
    make_get(&mut client_api2_node_a, contract_key, true, false).await?;

    // Wait for get response on second client
    loop {
        let resp = tokio::time::timeout(Duration::from_secs(30), client_api2_node_a.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                key,
                contract: Some(_),
                state: _,
            }))) => {
                assert_eq!(key, contract_key, "Contract key mismatch in GET response");
                break;
            }
            Ok(Ok(other)) => {
                tracing::warn!("unexpected response while waiting for get: {:?}", other);
            }
            Ok(Err(e)) => {
                bail!("Error receiving get response: {}", e);
            }
            Err(_) => {
                bail!("Timeout waiting for get response");
            }
        }
    }

    // Explicitly subscribe client 2 to the contract using make_subscribe
    make_subscribe(&mut client_api2_node_a, contract_key).await?;

    // Wait for subscribe response
    loop {
        let resp = tokio::time::timeout(Duration::from_secs(30), client_api2_node_a.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::SubscribeResponse {
                key,
                subscribed,
            }))) => {
                assert_eq!(
                    key, contract_key,
                    "Contract key mismatch in SUBSCRIBE response"
                );
                assert!(subscribed, "Failed to subscribe to contract");
                tracing::info!("Client 2: Successfully subscribed to contract {}", key);
                break;
            }
            Ok(Ok(other)) => {
                tracing::warn!(
                    "Client 2: unexpected response while waiting for subscribe: {:?}",
                    other
                );
            }
            Ok(Err(e)) => {
                bail!("Client 2: Error receiving subscribe response: {}", e);
            }
            Err(_) => {
                bail!("Client 2: Timeout waiting for subscribe response");
            }
        }
    }

    // Third client gets the contract from node C (without subscribing)
    // Add delay to allow contract to propagate from Node A to Node B/C
    tracing::info!("Waiting 5 seconds for contract to propagate across nodes...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    tracing::info!(
        "Client 3: Sending GET request for contract {} to Node B",
        contract_key
    );
    let get_start = std::time::Instant::now();
    make_get(&mut client_api_node_b, contract_key, true, false).await?;

    // Wait for get response on third client
    // Note: Contract propagation from Node A to Node B can take 5-10s locally, longer in CI
    loop {
        let resp = tokio::time::timeout(Duration::from_secs(60), client_api_node_b.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                key,
                contract: Some(_),
                state: _,
            }))) => {
                let elapsed = get_start.elapsed();
                tracing::info!("Client 3: Received GET response after {:?}", elapsed);
                assert_eq!(
                    key, contract_key,
                    "Contract key mismatch in GET response for client 3"
                );
                break;
            }
            Ok(Ok(other)) => {
                tracing::warn!(
                    "Client 3: unexpected response while waiting for get: {:?}",
                    other
                );
            }
            Ok(Err(e)) => {
                bail!("Client 3: Error receiving get response: {}", e);
            }
            Err(_) => {
                let elapsed = get_start.elapsed();
                bail!("Client 3: Timeout waiting for get response after {:?}. Contract may not have propagated from Node A to Node B", elapsed);
            }
        }
    }

    // Explicitly subscribe client 3 to the contract using make_subscribe
    make_subscribe(&mut client_api_node_b, contract_key).await?;

    // Wait for subscribe response
    loop {
        let resp = tokio::time::timeout(Duration::from_secs(60), client_api_node_b.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::SubscribeResponse {
                key,
                subscribed,
            }))) => {
                assert_eq!(
                    key, contract_key,
                    "Contract key mismatch in SUBSCRIBE response for client 3"
                );
                assert!(subscribed, "Failed to subscribe to contract for client 3");
                tracing::info!("Client 3: Successfully subscribed to contract {}", key);
                break;
            }
            Ok(Ok(other)) => {
                tracing::warn!(
                    "Client 3: unexpected response while waiting for subscribe: {:?}",
                    other
                );
            }
            Ok(Err(e)) => {
                bail!("Client 3: Error receiving subscribe response: {}", e);
            }
            Err(_) => {
                bail!("Client 3: Timeout waiting for subscribe response");
            }
        }
    }

    tracing::info!("All clients subscribed, proceeding with UPDATE operation");

    // Create a new to-do list by deserializing the current state, adding a task, and serializing it back
    let mut todo_list: test_utils::TodoList = serde_json::from_slice(wrapped_state.as_ref())
        .unwrap_or_else(|_| test_utils::TodoList {
            tasks: Vec::new(),
            version: 0,
        });

    // Add a task directly to the list
    todo_list.tasks.push(test_utils::Task {
        id: 1,
        title: "Test multiple clients".to_string(),
        description: "Verify that update notifications are received by multiple clients"
            .to_string(),
        completed: false,
        priority: 5,
    });

    // Serialize the updated list back to bytes
    let updated_bytes = serde_json::to_vec(&todo_list).unwrap();
    let updated_state = WrappedState::from(updated_bytes);

    // First client updates the contract
    make_update(&mut client_api1_node_a, contract_key, updated_state.clone()).await?;

    // Wait for update response and notifications on all clients
    let mut client1_received_notification = false;
    let mut client2_received_notification = false;
    let mut client_node_b_received_notification = false;
    let mut received_update_response = false;

    // Expected task after update
    let expected_task = test_utils::Task {
        id: 1,
        title: "Test multiple clients".to_string(),
        description: "Verify that update notifications are received by multiple clients"
            .to_string(),
        completed: false,
        priority: 5,
    };

    let start_time = std::time::Instant::now();
    while start_time.elapsed() < Duration::from_secs(90)
        && (!received_update_response
            || !client1_received_notification
            || !client2_received_notification
            || !client_node_b_received_notification)
    {
        // Check for messages on client 1
        if !received_update_response || !client1_received_notification {
            let resp =
                tokio::time::timeout(Duration::from_secs(1), client_api1_node_a.recv()).await;
            match resp {
                Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateResponse {
                    key,
                    summary: _,
                }))) => {
                    assert_eq!(
                        key, contract_key,
                        "Contract key mismatch in UPDATE response"
                    );
                    tracing::info!("Client 1: Received update response for contract {}", key);
                    received_update_response = true;
                }
                Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateNotification {
                    key,
                    update,
                }))) => {
                    assert_eq!(
                        key, contract_key,
                        "Contract key mismatch in UPDATE notification for client 1"
                    );

                    // Verify update content
                    match update {
                        UpdateData::State(state) => {
                            let received_todo_list: test_utils::TodoList =
                                serde_json::from_slice(state.as_ref())
                                    .expect("Failed to deserialize state from update notification");

                            assert_eq!(received_todo_list.tasks.len(), 1, "Should have one task");
                            assert_eq!(
                                received_todo_list.tasks[0].id, expected_task.id,
                                "Task ID should match"
                            );
                            assert_eq!(
                                received_todo_list.tasks[0].title, expected_task.title,
                                "Task title should match"
                            );
                            assert_eq!(
                                received_todo_list.tasks[0].description, expected_task.description,
                                "Task description should match"
                            );
                            assert_eq!(
                                received_todo_list.tasks[0].completed, expected_task.completed,
                                "Task completed status should match"
                            );
                            assert_eq!(
                                received_todo_list.tasks[0].priority, expected_task.priority,
                                "Task priority should match"
                            );

                            tracing::info!("Client 1: Successfully verified update content");
                        }
                        _ => {
                            tracing::warn!(
                                "Client 1: Received unexpected update type: {:?}",
                                update
                            );
                        }
                    }

                    tracing::info!(
                        "✅ Client 1: Successfully received update notification for contract {}",
                        key
                    );
                    client1_received_notification = true;
                }
                Ok(Ok(other)) => {
                    tracing::debug!("Client 1: Received unexpected response: {:?}", other);
                }
                Ok(Err(e)) => {
                    tracing::debug!("Client 1: Error receiving response: {}", e);
                }
                Err(_) => {
                    // Timeout is expected, just continue
                }
            }
        }

        // Check for notification on client 2
        if !client2_received_notification {
            let resp =
                tokio::time::timeout(Duration::from_secs(1), client_api2_node_a.recv()).await;
            match resp {
                Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateNotification {
                    key,
                    update,
                }))) => {
                    assert_eq!(
                        key, contract_key,
                        "Contract key mismatch in UPDATE notification for client 2"
                    );

                    // Verify update content
                    match update {
                        UpdateData::State(state) => {
                            let received_todo_list: test_utils::TodoList =
                                serde_json::from_slice(state.as_ref())
                                    .expect("Failed to deserialize state from update notification");

                            assert_eq!(received_todo_list.tasks.len(), 1, "Should have one task");
                            assert_eq!(
                                received_todo_list.tasks[0].id, expected_task.id,
                                "Task ID should match"
                            );
                            assert_eq!(
                                received_todo_list.tasks[0].title, expected_task.title,
                                "Task title should match"
                            );
                            assert_eq!(
                                received_todo_list.tasks[0].description, expected_task.description,
                                "Task description should match"
                            );
                            assert_eq!(
                                received_todo_list.tasks[0].completed, expected_task.completed,
                                "Task completed status should match"
                            );
                            assert_eq!(
                                received_todo_list.tasks[0].priority, expected_task.priority,
                                "Task priority should match"
                            );

                            tracing::info!("Client 2: Successfully verified update content");
                        }
                        _ => {
                            tracing::warn!(
                                "Client 2: Received unexpected update type: {:?}",
                                update
                            );
                        }
                    }

                    tracing::info!(
                        "✅ Client 2: Successfully received update notification for contract {}",
                        key
                    );
                    client2_received_notification = true;
                }
                Ok(Ok(other)) => {
                    tracing::debug!("Client 2: Received unexpected response: {:?}", other);
                }
                Ok(Err(e)) => {
                    tracing::debug!("Client 2: Error receiving response: {}", e);
                }
                Err(_) => {
                    // Timeout is expected, just continue
                }
            }
        }

        // Check for notification on client 3 (on different node)
        if !client_node_b_received_notification {
            let resp = tokio::time::timeout(Duration::from_secs(1), client_api_node_b.recv()).await;
            match resp {
                Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateNotification {
                    key,
                    update,
                }))) => {
                    assert_eq!(
                        key, contract_key,
                        "Contract key mismatch in UPDATE notification for client 3"
                    );

                    // Verify update content
                    match update {
                        UpdateData::State(state) => {
                            let received_todo_list: test_utils::TodoList =
                                serde_json::from_slice(state.as_ref())
                                    .expect("Failed to deserialize state from update notification");

                            assert_eq!(received_todo_list.tasks.len(), 1, "Should have one task");
                            assert_eq!(
                                received_todo_list.tasks[0].id, expected_task.id,
                                "Task ID should match"
                            );
                            assert_eq!(
                                received_todo_list.tasks[0].title, expected_task.title,
                                "Task title should match"
                            );
                            assert_eq!(
                                received_todo_list.tasks[0].description, expected_task.description,
                                "Task description should match"
                            );
                            assert_eq!(
                                received_todo_list.tasks[0].completed, expected_task.completed,
                                "Task completed status should match"
                            );
                            assert_eq!(
                                received_todo_list.tasks[0].priority, expected_task.priority,
                                "Task priority should match"
                            );

                            tracing::info!(
                                "Client 3: Successfully verified update content (cross-node)"
                            );
                        }
                        _ => {
                            tracing::warn!(
                                "Client 3: Received unexpected update type: {:?}",
                                update
                            );
                        }
                    }

                    tracing::info!(
                        "✅ Client 3: Successfully received update notification for contract {} (cross-node)",
                        key
                    );
                    client_node_b_received_notification = true;
                }
                Ok(Ok(other)) => {
                    tracing::debug!("Client 3: Received unexpected response: {:?}", other);
                }
                Ok(Err(e)) => {
                    tracing::debug!("Client 3: Error receiving response: {}", e);
                }
                Err(_) => {
                    // Timeout is expected, just continue
                }
            }
        }

        // Small delay before trying again
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Assert that we received the update response and all clients received notifications
    assert!(
        received_update_response,
        "Did not receive update response within timeout period"
    );
    assert!(
        client1_received_notification,
        "Client 1 did not receive update notification within timeout period"
    );
    assert!(
        client2_received_notification,
        "Client 2 did not receive update notification within timeout period"
    );
    assert!(
        client_node_b_received_notification,
        "Client 3 did not receive update notification within timeout period (cross-node)"
    );

    // Properly close all clients
    client_api1_node_a
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    client_api2_node_a
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    client_api_node_b
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    Ok(())
}

#[freenet_test(
    nodes = ["gateway", "node-a"],
    auto_connect_peers = true,
    timeout_secs = 120,
    startup_wait_secs = 20,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_get_with_subscribe_flag(ctx: &mut TestContext) -> TestResult {
    // Load test contract
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    // Create initial state with empty todo list
    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    let node_a = ctx.node("node-a")?;
    let gateway = ctx.node("gateway")?;
    let ws_api_port_a = node_a.ws_port;

    // Log data directories for debugging
    tracing::info!("Node A data dir: {:?}", node_a.temp_dir_path);
    tracing::info!("Node B (gw) data dir: {:?}", gateway.temp_dir_path);

    // Give extra time for peer to connect to gateway
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Connect first client to node A's websocket API (for putting the contract)
    let uri_a =
        format!("ws://127.0.0.1:{ws_api_port_a}/v1/contract/command?encodingProtocol=native");
    let (stream1, _) = connect_async(&uri_a).await?;
    let mut client_api1_node_a = WebApi::start(stream1);

    tracing::info!("Client 1: Put contract with initial state");

    // First client puts contract with initial state (without subscribing)
    make_put(
        &mut client_api1_node_a,
        wrapped_state.clone(),
        contract.clone(),
        false, // subscribe=false
    )
    .await?;

    // Wait for put response (increased timeout for CI environments)
    let resp = tokio::time::timeout(Duration::from_secs(45), client_api1_node_a.recv()).await;
    match resp {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
            assert_eq!(key, contract_key, "Contract key mismatch in PUT response");
        }
        Ok(Ok(other)) => {
            bail!("unexpected response while waiting for put: {:?}", other);
        }
        Ok(Err(e)) => {
            bail!("Client 1: Error receiving put response: {}", e);
        }
        Err(_) => {
            bail!("Client 1: Timeout waiting for put response");
        }
    }

    tracing::warn!("Client 1: Successfully put contract {}", contract_key);

    // Connect second client to node A's websocket API (for getting with auto-subscribe)
    let (stream2, _) = connect_async(&uri_a).await?;
    let mut client_api2_node_a = WebApi::start(stream2);

    // Second client gets the contract with auto-subscribe
    make_get(&mut client_api2_node_a, contract_key, true, true).await?;

    // Wait for get response on second client
    let resp = tokio::time::timeout(Duration::from_secs(30), client_api2_node_a.recv()).await;
    match resp {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
            key,
            contract: Some(_),
            state: _,
        }))) => {
            assert_eq!(key, contract_key, "Contract key mismatch in GET response");
        }
        Ok(Ok(other)) => {
            bail!("unexpected response while waiting for get: {:?}", other);
        }
        Ok(Err(e)) => {
            bail!("Client 2: Error receiving get response: {}", e);
        }
        Err(_) => {
            bail!("Client 2: Timeout waiting for get response");
        }
    }

    // Create a new to-do list by deserializing the current state, adding a task, and serializing it back
    let mut todo_list: test_utils::TodoList = serde_json::from_slice(wrapped_state.as_ref())
        .unwrap_or_else(|_| test_utils::TodoList {
            tasks: Vec::new(),
            version: 0,
        });

    // Add a task directly to the list
    todo_list.tasks.push(test_utils::Task {
        id: 1,
        title: "Test auto-subscribe with GET".to_string(),
        description: "Verify that auto-subscribe works with GET operation".to_string(),
        completed: false,
        priority: 5,
    });

    // Serialize the updated list back to bytes
    let updated_bytes = serde_json::to_vec(&todo_list).unwrap();
    let updated_state = WrappedState::from(updated_bytes);

    // First client updates the contract
    make_update(&mut client_api1_node_a, contract_key, updated_state.clone()).await?;

    // Wait for update response
    let resp = tokio::time::timeout(Duration::from_secs(30), client_api1_node_a.recv()).await;
    match resp {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateResponse {
            key,
            summary: _,
        }))) => {
            assert_eq!(
                key, contract_key,
                "Contract key mismatch in UPDATE response"
            );
        }
        Ok(Ok(other)) => {
            bail!("unexpected response while waiting for update: {:?}", other);
        }
        Ok(Err(e)) => {
            bail!("Client 1: Error receiving update response: {}", e);
        }
        Err(_) => {
            bail!("Client 1: Timeout waiting for update response");
        }
    }

    // Expected task after update
    let expected_task = test_utils::Task {
        id: 1,
        title: "Test auto-subscribe with GET".to_string(),
        description: "Verify that auto-subscribe works with GET operation".to_string(),
        completed: false,
        priority: 5,
    };

    // Wait for update notification on client 2 (should be auto-subscribed)
    let mut client2_node_a_received_notification = false;

    // Try for up to 30 seconds to receive the notification
    let start_time = std::time::Instant::now();
    while start_time.elapsed() < Duration::from_secs(30) && !client2_node_a_received_notification {
        let resp = tokio::time::timeout(Duration::from_secs(1), client_api2_node_a.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateNotification {
                key,
                update,
            }))) => {
                assert_eq!(
                    key, contract_key,
                    "Contract key mismatch in UPDATE notification for client 2"
                );

                // Verify update content
                match update {
                    UpdateData::State(state) => {
                        let received_todo_list: test_utils::TodoList =
                            serde_json::from_slice(state.as_ref())
                                .expect("Failed to deserialize state from update notification");

                        assert_eq!(received_todo_list.tasks.len(), 1, "Should have one task");
                        assert_eq!(
                            received_todo_list.tasks[0].id, expected_task.id,
                            "Task ID should match"
                        );
                        assert_eq!(
                            received_todo_list.tasks[0].title, expected_task.title,
                            "Task title should match"
                        );
                        assert_eq!(
                            received_todo_list.tasks[0].description, expected_task.description,
                            "Task description should match"
                        );
                        assert_eq!(
                            received_todo_list.tasks[0].completed, expected_task.completed,
                            "Task completed status should match"
                        );
                        assert_eq!(
                            received_todo_list.tasks[0].priority, expected_task.priority,
                            "Task priority should match"
                        );

                        tracing::info!("Client 1: Successfully verified update content");
                    }
                    _ => {
                        tracing::warn!("Client 1: Received unexpected update type: {:?}", update);
                    }
                }
                client2_node_a_received_notification = true;
                break;
            }
            Ok(Ok(other)) => {
                bail!("unexpected response while waiting for update: {:?}", other);
            }
            Ok(Err(e)) => {
                tracing::error!("Client 2: Timeout waiting for update: {}", e);
            }
            Err(_) => {
                tracing::error!("Client 2: Timeout waiting for update response");
            }
        }

        // Small delay before trying again
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Assert that client 1 received the notification (proving auto-subscribe worked)
    assert!(
            client2_node_a_received_notification,
            "Client 2 did not receive update notification within timeout period (auto-subscribe via GET failed)"
        );

    Ok(())
}

// FIXME Update notification is not received
#[freenet_test(
    nodes = ["gateway", "node-a"],
    auto_connect_peers = true,
    timeout_secs = 180,
    startup_wait_secs = 20,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_put_with_subscribe_flag(ctx: &mut TestContext) -> TestResult {
    // Load test contract
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    // Create initial state with empty todo list
    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    let node_a = ctx.node("node-a")?;
    let gateway = ctx.node("gateway")?;
    let ws_api_port_a = node_a.ws_port;

    // Log data directories for debugging
    tracing::info!("Node A data dir: {:?}", node_a.temp_dir_path);
    tracing::info!("Gateway data dir: {:?}", gateway.temp_dir_path);

    // Give extra time for peer to connect to gateway
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Connect first client to node A's websocket API (for putting with auto-subscribe)
    let uri_a =
        format!("ws://127.0.0.1:{ws_api_port_a}/v1/contract/command?encodingProtocol=native");
    let (stream1, _) = connect_async(&uri_a).await?;
    let mut client_api1 = WebApi::start(stream1);

    // Connect second client to node A's websocket API (for updating the contract)
    let (stream2, _) = connect_async(&uri_a).await?;
    let mut client_api2 = WebApi::start(stream2);

    // First client puts contract with initial state and auto-subscribes
    make_put(
        &mut client_api1,
        wrapped_state.clone(),
        contract.clone(),
        true, // subscribe=true for auto-subscribe
    )
    .await?;

    // Wait for put response
    let mut put_response_received = false;
    let start = std::time::Instant::now();
    while !put_response_received && start.elapsed() < Duration::from_secs(30) {
        let resp = tokio::time::timeout(Duration::from_secs(5), client_api1.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
                assert_eq!(key, contract_key, "Contract key mismatch in PUT response");
                put_response_received = true;
            }
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::SubscribeResponse {
                key,
                subscribed: _,
            }))) => {
                bail!(
                    "Client 1: Received unexpected SubscribeResponse for contract {key} - \
                         sub-operations should not send client notifications"
                );
            }
            Ok(Ok(other)) => {
                tracing::debug!(
                    "Client 1: Received non-PUT response while waiting for PUT: {:?}",
                    other
                );
                // Continue waiting - might receive other messages before PUT response
            }
            Ok(Err(e)) => {
                tracing::error!("Client 1: Error receiving put response: {}", e);
                bail!("WebSocket error while waiting for PUT response: {}", e);
            }
            Err(_) => {
                // Timeout on recv - continue looping with outer timeout check
                tracing::debug!(
                    "Client 1: No message received in 5s, continuing to wait for PUT response"
                );
            }
        }
    }

    if !put_response_received {
        bail!("Client 1: Did not receive PUT response within 30 seconds");
    }

    // Second client gets the contract (without subscribing)
    make_get(&mut client_api2, contract_key, true, false).await?;

    // Wait for get response on second client
    let mut get_response_received = false;
    let start = std::time::Instant::now();
    while !get_response_received && start.elapsed() < Duration::from_secs(30) {
        let resp = tokio::time::timeout(Duration::from_secs(5), client_api2.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                key,
                contract: Some(_),
                state: _,
            }))) => {
                assert_eq!(key, contract_key, "Contract key mismatch in GET response");
                get_response_received = true;
            }
            Ok(Ok(other)) => {
                tracing::debug!(
                    "Client 2: Received non-GET response while waiting for GET: {:?}",
                    other
                );
                // Continue waiting - might receive other messages before GET response
            }
            Ok(Err(e)) => {
                tracing::error!("Client 2: Error receiving get response: {}", e);
                bail!("WebSocket error while waiting for GET response: {}", e);
            }
            Err(_) => {
                // Timeout on recv - continue looping with outer timeout check
                tracing::debug!(
                    "Client 2: No message received in 5s, continuing to wait for GET response"
                );
            }
        }
    }

    if !get_response_received {
        bail!("Client 2: Did not receive GET response within 30 seconds");
    }

    // Create a new to-do list by deserializing the current state, adding a task, and serializing it back
    let mut todo_list: test_utils::TodoList = serde_json::from_slice(wrapped_state.as_ref())
        .unwrap_or_else(|_| test_utils::TodoList {
            tasks: Vec::new(),
            version: 0,
        });

    // Add a task directly to the list
    todo_list.tasks.push(test_utils::Task {
        id: 1,
        title: "Test auto-subscribe with PUT".to_string(),
        description: "Verify that auto-subscribe works with PUT operation".to_string(),
        completed: false,
        priority: 5,
    });

    // Serialize the updated list back to bytes
    let updated_bytes = serde_json::to_vec(&todo_list).unwrap();
    let updated_state = WrappedState::from(updated_bytes);

    // Second client updates the contract
    tracing::info!("Client 2: Updating contract to trigger notification");
    make_update(&mut client_api2, contract_key, updated_state.clone()).await?;

    // Wait for update response
    let mut update_response_received = false;
    let start = std::time::Instant::now();
    while !update_response_received && start.elapsed() < Duration::from_secs(30) {
        let resp = tokio::time::timeout(Duration::from_secs(5), client_api2.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateResponse {
                key,
                summary: _,
            }))) => {
                assert_eq!(
                    key, contract_key,
                    "Contract key mismatch in UPDATE response"
                );
                update_response_received = true;
            }
            Ok(Ok(other)) => {
                tracing::debug!(
                    "Client 2: Received non-UPDATE response while waiting for UPDATE: {:?}",
                    other
                );
                // Continue waiting - might receive other messages before UPDATE response
            }
            Ok(Err(e)) => {
                tracing::error!("Client 2: Error receiving update response: {}", e);
                bail!("WebSocket error while waiting for UPDATE response: {}", e);
            }
            Err(_) => {
                // Timeout on recv - continue looping with outer timeout check
                tracing::debug!(
                    "Client 2: No message received in 5s, continuing to wait for UPDATE response"
                );
            }
        }
    }

    if !update_response_received {
        bail!("Client 2: Did not receive UPDATE response within 30 seconds");
    }

    // Expected task after update
    let expected_task = test_utils::Task {
        id: 1,
        title: "Test auto-subscribe with PUT".to_string(),
        description: "Verify that auto-subscribe works with PUT operation".to_string(),
        completed: false,
        priority: 5,
    };

    // Wait for update notification on client 1 (should be auto-subscribed from PUT)
    let mut client1_received_notification = false;

    // Try for up to 30 seconds to receive the notification
    let start_time = std::time::Instant::now();
    while start_time.elapsed() < Duration::from_secs(30) && !client1_received_notification {
        let resp = tokio::time::timeout(Duration::from_secs(1), client_api1.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateNotification {
                key,
                update,
            }))) => {
                assert_eq!(
                    key, contract_key,
                    "Contract key mismatch in UPDATE notification for client 1"
                );

                // Verify update content
                match update {
                    UpdateData::State(state) => {
                        let received_todo_list: test_utils::TodoList =
                            serde_json::from_slice(state.as_ref())
                                .expect("Failed to deserialize state from update notification");

                        assert_eq!(received_todo_list.tasks.len(), 1, "Should have one task");
                        assert_eq!(
                            received_todo_list.tasks[0].id, expected_task.id,
                            "Task ID should match"
                        );
                        assert_eq!(
                            received_todo_list.tasks[0].title, expected_task.title,
                            "Task title should match"
                        );
                        assert_eq!(
                            received_todo_list.tasks[0].description, expected_task.description,
                            "Task description should match"
                        );
                        assert_eq!(
                            received_todo_list.tasks[0].completed, expected_task.completed,
                            "Task completed status should match"
                        );
                        assert_eq!(
                            received_todo_list.tasks[0].priority, expected_task.priority,
                            "Task priority should match"
                        );

                        tracing::info!("Client 1: Successfully verified update content");
                    }
                    _ => {
                        tracing::warn!("Client 1: Received unexpected update type: {:?}", update);
                    }
                }
                client1_received_notification = true;
                break;
            }
            Ok(Ok(other)) => {
                tracing::debug!("Client 1: Received non-notification response while waiting for update notification: {:?}", other);
                // Continue waiting - might receive other messages before notification
            }
            Ok(Err(e)) => {
                tracing::error!("Client 1: Error receiving update notification: {}", e);
                bail!(
                    "WebSocket error while waiting for update notification: {}",
                    e
                );
            }
            Err(_) => {
                // Timeout on recv - this is expected, just continue looping
                tracing::debug!("Client 1: No message received in 1s, continuing to wait for update notification");
            }
        }

        // Small delay before trying again
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Assert that client 1 received the notification (proving auto-subscribe worked)
    assert!(
        client1_received_notification,
        "Client 1 did not receive update notification within timeout period (auto-subscribe via PUT failed)"
    );

    Ok(())
}

#[freenet_test(
    nodes = ["gateway", "client-node"],
    auto_connect_peers = true,
    timeout_secs = 180,
    startup_wait_secs = 20,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_delegate_request(ctx: &mut TestContext) -> TestResult {
    const TEST_DELEGATE: &str = "test-delegate-integration";

    // Configure environment variables for optimized release build
    std::env::set_var("CARGO_PROFILE_RELEASE_LTO", "true");
    std::env::set_var("CARGO_PROFILE_RELEASE_CODEGEN_UNITS", "1");
    std::env::set_var("CARGO_PROFILE_RELEASE_STRIP", "true");

    // Load delegate (moving this outside the async block)
    let params = Parameters::from(vec![]);
    let delegate = load_delegate(TEST_DELEGATE, params.clone())?;
    let delegate_key = delegate.key().clone();

    let client_node = ctx.node("client-node")?;
    let gateway = ctx.node("gateway")?;
    let ws_api_port_client = client_node.ws_port;

    // Log data directories for debugging
    tracing::info!("Client node data dir: {:?}", client_node.temp_dir_path);
    tracing::info!("Gateway node data dir: {:?}", gateway.temp_dir_path);

    // Give extra time for peer to connect to gateway
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Connect to the client node's WebSocket API
    let uri =
        format!("ws://127.0.0.1:{ws_api_port_client}/v1/contract/command?encodingProtocol=native");
    let (stream, _) = connect_async(&uri).await?;
    let mut client = WebApi::start(stream);

    // Register the delegate in the node
    client
        .send(ClientRequest::DelegateOp(
            freenet_stdlib::client_api::DelegateRequest::RegisterDelegate {
                delegate: delegate.clone(),
                cipher: freenet_stdlib::client_api::DelegateRequest::DEFAULT_CIPHER,
                nonce: freenet_stdlib::client_api::DelegateRequest::DEFAULT_NONCE,
            },
        ))
        .await?;

    // Wait for registration response
    let resp = tokio::time::timeout(Duration::from_secs(10), client.recv()).await??;
    match resp {
        HostResponse::DelegateResponse { key, values: _ } => {
            assert_eq!(
                key, delegate_key,
                "Delegate key mismatch in register response"
            );
            tracing::info!("Successfully registered delegate with key: {key}");
        }
        other => {
            bail!(
                "Unexpected response while waiting for register: {:?}",
                other
            );
        }
    }

    // Create message for the delegate
    use serde::{Deserialize, Serialize};
    #[derive(Debug, Serialize, Deserialize)]
    enum InboundAppMessage {
        TestRequest(String),
    }

    let app_id = ContractInstanceId::new([0; 32]);
    let request_data = "test-request-data".to_string();
    let payload = bincode::serialize(&InboundAppMessage::TestRequest(request_data.clone()))?;
    let app_msg = ApplicationMessage::new(app_id, payload);

    // Send request to the delegate
    client
        .send(ClientRequest::DelegateOp(
            freenet_stdlib::client_api::DelegateRequest::ApplicationMessages {
                key: delegate_key.clone(),
                params: params.clone(),
                inbound: vec![InboundDelegateMsg::ApplicationMessage(app_msg)],
            },
        ))
        .await?;

    // Wait for delegate response
    let resp = tokio::time::timeout(Duration::from_secs(10), client.recv()).await??;

    match resp {
        HostResponse::DelegateResponse {
            key,
            values: outbound,
        } => {
            assert_eq!(key, delegate_key, "Delegate key mismatch in response");

            assert!(!outbound.is_empty(), "No output messages from delegate");

            let app_msg = match &outbound[0] {
                OutboundDelegateMsg::ApplicationMessage(msg) => msg,
                other => bail!("Expected ApplicationMessage, got {:?}", other),
            };

            assert!(app_msg.processed, "Message not marked as processed");

            #[derive(Debug, Deserialize)]
            enum OutboundAppMessage {
                TestResponse(String, Vec<u8>),
            }

            let response: OutboundAppMessage = bincode::deserialize(&app_msg.payload)?;

            match response {
                OutboundAppMessage::TestResponse(text, data) => {
                    assert_eq!(
                        text,
                        format!("Processed: {request_data}"),
                        "Response text doesn't match expected format"
                    );
                    assert_eq!(
                        data,
                        vec![4, 5, 6],
                        "Response data doesn't match expected value"
                    );

                    tracing::info!("Successfully received and verified delegate response");
                }
            }
        }
        other => {
            bail!(
                "Unexpected response while waiting for delegate response: {:?}",
                other
            );
        }
    }

    Ok(())
}

/// Ensure a client-only peer receives PutResponse when the contract is seeded on a third hop.
///
/// This test verifies that PUT responses are properly routed back through forwarding peers,
/// even when the contract is stored on a node that is multiple hops away from the client.
///
/// Network topology:
/// - peer-a (client): Far from contract location
/// - gateway: Intermediate node
/// - peer-c (target): Close to contract location
///
/// Expected flow:
/// 1. peer-a sends PUT → routes through gateway → stored on peer-c
/// 2. peer-c sends PUT response → routes back through gateway → received by peer-a
#[freenet_test(
    nodes = ["gateway", "peer-a", "peer-c"],
    gateways = ["gateway"],
    auto_connect_peers = true,
    timeout_secs = 240,
    startup_wait_secs = 15,
    aggregate_events = "on_failure",
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_put_contract_three_hop_returns_response(ctx: &mut TestContext) -> TestResult {
    use freenet::dev_tool::Location;

    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();
    let contract_location = Location::from(&contract_key);

    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    // Get node information
    let gateway = ctx.node("gateway")?;
    let peer_a = ctx.node("peer-a")?;
    let peer_c = ctx.node("peer-c")?;

    // Note: We cannot modify node locations after they're created with the macro,
    // so this test will use random locations. The original test had specific location
    // requirements to ensure proper three-hop routing. For now, we'll proceed with
    // the test and it should still validate PUT response routing.

    tracing::info!("Node A data dir: {:?}", peer_a.temp_dir_path);
    tracing::info!("Gateway node data dir: {:?}", gateway.temp_dir_path);
    tracing::info!("Node C data dir: {:?}", peer_c.temp_dir_path);
    tracing::info!("Contract location: {}", contract_location.as_f64());

    // Connect to peer A's WebSocket API
    let uri_a = format!(
        "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
        peer_a.ws_port
    );
    let (stream_a, _) = connect_async(&uri_a).await?;
    let mut client_api_a = WebApi::start(stream_a);

    // Send PUT from peer A
    make_put(
        &mut client_api_a,
        wrapped_state.clone(),
        contract.clone(),
        false,
    )
    .await?;

    // Wait for PUT response from peer A
    tracing::info!("Waiting for PUT response from peer A...");
    let resp = tokio::time::timeout(Duration::from_secs(120), client_api_a.recv()).await;
    match resp {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
            tracing::info!("PUT successful for contract: {}", key);
            assert_eq!(key, contract_key);
        }
        Ok(Ok(other)) => {
            bail!("Unexpected response while waiting for put: {:?}", other);
        }
        Ok(Err(e)) => {
            bail!("Error receiving put response: {}", e);
        }
        Err(_) => {
            bail!("Timeout waiting for put response after 120 seconds");
        }
    }

    // Verify contract can be retrieved from peer C
    let uri_c = format!(
        "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
        peer_c.ws_port
    );
    let (stream_c, _) = connect_async(&uri_c).await?;
    let mut client_api_c = WebApi::start(stream_c);
    let (response_contract, response_state) =
        get_contract(&mut client_api_c, contract_key, &peer_c.temp_dir_path).await?;
    assert_eq!(response_contract, contract);
    assert_eq!(response_state, wrapped_state);

    client_api_c
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Clean disconnect from peer A
    client_api_a
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify contract can be retrieved from gateway
    let uri_b = format!(
        "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
        gateway.ws_port
    );
    let (stream_b, _) = connect_async(&uri_b).await?;
    let mut client_api_b = WebApi::start(stream_b);
    let (gw_contract, gw_state) =
        get_contract(&mut client_api_b, contract_key, &gateway.temp_dir_path).await?;
    assert_eq!(gw_contract, contract);
    assert_eq!(gw_state, wrapped_state);
    client_api_b
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    Ok(())
}

#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
#[ignore = "Long-running test (90s) - needs update for new keep-alive constants"]
async fn test_gateway_packet_size_change_after_60s() -> TestResult {
    // Load test contract
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    // Create initial state
    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    // Create network sockets
    let network_socket_gw1 = TcpListener::bind("127.0.0.1:0")?;
    let network_socket_gw2 = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_client = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_gw1 = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_gw2 = TcpListener::bind("127.0.0.1:0")?;

    // Configure first gateway node
    let (config_gw1, preset_cfg_gw1, config_gw1_info) = {
        let (cfg, preset) = base_node_test_config(
            true,
            vec![],
            Some(network_socket_gw1.local_addr()?.port()),
            ws_api_port_socket_gw1.local_addr()?.port(),
        )
        .await?;
        let public_port = cfg.network_api.public_port.unwrap();
        let path = preset.temp_dir.path().to_path_buf();
        (cfg, preset, gw_config(public_port, &path)?)
    };

    // Configure second gateway node (connects to first gateway)
    let (config_gw2, preset_cfg_gw2, config_gw2_info) = {
        let (cfg, preset) = base_node_test_config(
            true,
            vec![serde_json::to_string(&config_gw1_info)?], // Connect to gateway 1
            Some(network_socket_gw2.local_addr()?.port()),
            ws_api_port_socket_gw2.local_addr()?.port(),
        )
        .await?;
        let public_port = cfg.network_api.public_port.unwrap();
        let path = preset.temp_dir.path().to_path_buf();
        (cfg, preset, gw_config(public_port, &path)?)
    };

    // Configure client node (connects via gateway 2)
    let (config_client, preset_cfg_client) = base_node_test_config(
        false,
        vec![serde_json::to_string(&config_gw2_info)?],
        None,
        ws_api_port_socket_client.local_addr()?.port(),
    )
    .await?;
    let ws_api_port_client = config_client.ws_api.ws_api_port.unwrap();

    // Log data directories
    tracing::info!(
        "Client node data dir: {:?}",
        preset_cfg_client.temp_dir.path()
    );
    tracing::info!(
        "Gateway 1 node data dir: {:?}",
        preset_cfg_gw1.temp_dir.path()
    );
    tracing::info!(
        "Gateway 2 node data dir: {:?}",
        preset_cfg_gw2.temp_dir.path()
    );

    // Free ports
    std::mem::drop(ws_api_port_socket_client);
    std::mem::drop(network_socket_gw1);
    std::mem::drop(network_socket_gw2);
    std::mem::drop(ws_api_port_socket_gw1);
    std::mem::drop(ws_api_port_socket_gw2);

    // Start gateway 1 node
    let node_gw1 = async {
        let config = config_gw1.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Start gateway 2 node (connects to gateway 1)
    let node_gw2 = async {
        let config = config_gw2.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Start client node
    let node_client = async move {
        let config = config_client.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    let test = tokio::time::timeout(Duration::from_secs(180), async {
        // Wait for nodes to start (gateways need to connect to each other)
        tokio::time::sleep(Duration::from_secs(20)).await;

        // Connect to client node
        let uri = format!(
            "ws://127.0.0.1:{ws_api_port_client}/v1/contract/command?encodingProtocol=native"
        );
        let (stream, _) = connect_async(&uri).await?;
        let mut client = WebApi::start(stream);

        // Put contract
        make_put(&mut client, wrapped_state.clone(), contract.clone(), false).await?;

        // Wait for put response
        let resp = tokio::time::timeout(Duration::from_secs(30), client.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
                assert_eq!(key, contract_key);
                tracing::info!("Successfully put contract");
            }
            _ => {
                bail!("Failed to put contract");
            }
        }

        // Now keep the connection alive for 90 seconds, sending periodic GET requests
        tracing::info!("Starting packet size change test - monitoring for 75 seconds");
        let start_time = std::time::Instant::now();
        let mut get_count = 0;
        let mut error_count = 0;

        while start_time.elapsed() < Duration::from_secs(75) {
            // Send a GET request every 5 seconds for more frequent monitoring
            tokio::time::sleep(Duration::from_secs(5)).await;
            get_count += 1;

            let elapsed = start_time.elapsed();
            tracing::info!("Sending GET request #{} at {:?}", get_count, elapsed);

            // Log if we're past the 60-second mark where errors typically start
            if elapsed > Duration::from_secs(60) {
                tracing::warn!("Past 60-second mark - monitoring for packet size changes");
            }

            make_get(&mut client, contract_key, false, false).await?;

            // Try to receive response with a shorter timeout
            match tokio::time::timeout(Duration::from_secs(10), client.recv()).await {
                Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                    key,
                    ..
                }))) => {
                    assert_eq!(key, contract_key);
                    tracing::info!("GET request #{} succeeded", get_count);
                }
                Ok(Ok(other)) => {
                    tracing::warn!(
                        "GET request #{} unexpected response: {:?}",
                        get_count,
                        other
                    );
                    error_count += 1;
                }
                Ok(Err(e)) => {
                    tracing::error!("GET request #{} error: {}", get_count, e);
                    error_count += 1;
                }
                Err(_) => {
                    tracing::error!("GET request #{} timed out", get_count);
                    error_count += 1;
                }
            }
        }

        tracing::info!(
            "Long-running test completed: {} GET requests, {} errors",
            get_count,
            error_count
        );

        // The test passes if we don't crash with decryption errors
        // In production, decryption errors would cause the connection to fail
        if error_count > get_count / 2 {
            bail!("Too many errors during long-running connection test");
        }

        Ok::<_, anyhow::Error>(())
    });

    // Wait for test completion or node failures
    select! {
        gw1 = node_gw1 => {
            let Err(e) = gw1;
            return Err(anyhow!("Gateway 1 node failed: {}", e).into())
        }
        gw2 = node_gw2 => {
            let Err(e) = gw2;
            return Err(anyhow!("Gateway 2 node failed: {}", e).into())
        }
        client = node_client => {
            let Err(e) = client;
            return Err(anyhow!("Client node failed: {}", e).into())
        }
        r = test => {
            r??;
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    Ok(())
}

#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
#[ignore = "Long-running test (75s) - run with --ignored flag"]
async fn test_production_decryption_error_scenario() -> TestResult {
    // This test attempts to reproduce the exact production scenario:
    // 1. Client connects to gateway (vega)
    // 2. Connection works fine for ~60 seconds with 48-byte packets
    // 3. After 60 seconds, 256-byte packets arrive that fail to decrypt

    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    // Create sockets
    let network_socket_gw = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_client = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_gw = TcpListener::bind("127.0.0.1:0")?;

    // Configure gateway (simulating vega)
    let (config_gw, preset_cfg_gw, config_gw_info) = {
        let (cfg, preset) = base_node_test_config(
            true,
            vec![],
            Some(network_socket_gw.local_addr()?.port()),
            ws_api_port_socket_gw.local_addr()?.port(),
        )
        .await?;
        let public_port = cfg.network_api.public_port.unwrap();
        let path = preset.temp_dir.path().to_path_buf();
        (cfg, preset, gw_config(public_port, &path)?)
    };

    // Configure client node
    let (config_client, preset_cfg_client) = base_node_test_config(
        false,
        vec![serde_json::to_string(&config_gw_info)?],
        None,
        ws_api_port_socket_client.local_addr()?.port(),
    )
    .await?;
    let ws_api_port_client = config_client.ws_api.ws_api_port.unwrap();

    tracing::info!(
        "Client node data dir: {:?}",
        preset_cfg_client.temp_dir.path()
    );
    tracing::info!("Gateway node data dir: {:?}", preset_cfg_gw.temp_dir.path());

    // Free ports
    std::mem::drop(ws_api_port_socket_client);
    std::mem::drop(network_socket_gw);
    std::mem::drop(ws_api_port_socket_gw);

    // Start nodes
    let node_gw = async {
        let config = config_gw.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    let node_client = async move {
        let config = config_client.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    let test = tokio::time::timeout(Duration::from_secs(90), async {
        // Wait for nodes to start
        tokio::time::sleep(Duration::from_secs(15)).await;

        // Connect to client node
        let uri = format!(
            "ws://127.0.0.1:{ws_api_port_client}/v1/contract/command?encodingProtocol=native"
        );
        let (stream, _) = connect_async(&uri).await?;
        let mut client = WebApi::start(stream);

        // Put contract
        make_put(&mut client, wrapped_state.clone(), contract.clone(), false).await?;

        // Wait for put response
        let resp = tokio::time::timeout(Duration::from_secs(30), client.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
                assert_eq!(key, contract_key);
                tracing::info!("Successfully put contract");
            }
            _ => {
                bail!("Failed to put contract");
            }
        }

        // Monitor connection for 75 seconds
        tracing::info!("Starting production scenario simulation - monitoring for 75 seconds");
        let start_time = std::time::Instant::now();
        let mut last_success_time = start_time;
        let mut error_count = 0;
        let mut success_count = 0;

        while start_time.elapsed() < Duration::from_secs(75) {
            tokio::time::sleep(Duration::from_secs(3)).await;

            let elapsed = start_time.elapsed();

            // Try a GET request
            make_get(&mut client, contract_key, false, false).await?;

            match tokio::time::timeout(Duration::from_secs(5), client.recv()).await {
                Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                    key,
                    ..
                }))) => {
                    assert_eq!(key, contract_key);
                    success_count += 1;
                    last_success_time = std::time::Instant::now();
                    tracing::info!(
                        "GET succeeded at {:?} (success #{})",
                        elapsed,
                        success_count
                    );
                }
                Ok(Ok(other)) => {
                    error_count += 1;
                    tracing::error!("GET unexpected response at {:?}: {:?}", elapsed, other);
                }
                Ok(Err(e)) => {
                    error_count += 1;
                    tracing::error!("GET error at {:?}: {}", elapsed, e);
                }
                Err(_) => {
                    error_count += 1;
                    tracing::error!("GET timeout at {:?}", elapsed);
                }
            }

            // Log status around the critical 60-second mark
            if elapsed > Duration::from_secs(58) && elapsed < Duration::from_secs(65) {
                tracing::warn!(
                    "Critical period - elapsed: {:?}, errors: {}, last success: {:?} ago",
                    elapsed,
                    error_count,
                    std::time::Instant::now().duration_since(last_success_time)
                );
            }
        }

        tracing::info!(
            "Test completed: {} successes, {} errors",
            success_count,
            error_count
        );

        // In production, all requests fail after ~60 seconds
        // For now, we just log the results to see if we can reproduce the pattern

        Ok::<_, anyhow::Error>(())
    });

    // Wait for test completion or node failures
    select! {
        gw = node_gw => {
            let Err(e) = gw;
            return Err(anyhow!("Gateway node failed: {}", e).into())
        }
        client = node_client => {
            let Err(e) = client;
            return Err(anyhow!("Client node failed: {}", e).into())
        }
        r = test => {
            r??;
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    Ok(())
}

// Helper functions for future full subscription testing
#[allow(dead_code)]
async fn wait_for_put_response(
    client: &mut WebApi,
    expected_key: &ContractKey,
) -> Result<ContractKey, anyhow::Error> {
    let resp = timeout(Duration::from_secs(30), client.recv()).await??;
    match resp {
        HostResponse::ContractResponse(ContractResponse::PutResponse { key }) => {
            if &key != expected_key {
                bail!(
                    "Put response key mismatch: expected {}, got {}",
                    expected_key,
                    key
                );
            }
            Ok(key)
        }
        other => {
            bail!("Unexpected response while waiting for put: {:?}", other);
        }
    }
}

#[allow(dead_code)]
async fn wait_for_subscribe_response(
    client: &mut WebApi,
    expected_key: &ContractKey,
) -> Result<(), anyhow::Error> {
    let resp = timeout(Duration::from_secs(10), client.recv()).await??;
    match resp {
        HostResponse::ContractResponse(ContractResponse::SubscribeResponse { key, .. }) => {
            if &key != expected_key {
                bail!(
                    "Subscribe response key mismatch: expected {}, got {}",
                    expected_key,
                    key
                );
            }
            Ok(())
        }
        other => {
            bail!(
                "Unexpected response while waiting for subscribe: {:?}",
                other
            );
        }
    }
}

#[freenet_test(
    nodes = ["gateway", "peer-node"],
    auto_connect_peers = true,
    timeout_secs = 180,
    startup_wait_secs = 10,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_subscription_introspection(ctx: &mut TestContext) -> TestResult {
    // Load test contract - not used in this simplified test
    const TEST_CONTRACT: &str = "test-contract-integration";
    let _contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;

    // Create initial state - not used in this simplified test
    let _initial_state = test_utils::create_empty_todo_list();

    let gateway = ctx.node("gateway")?;
    let peer_node = ctx.node("peer-node")?;
    let ws_api_port_gw = gateway.ws_port;
    let ws_api_port_node = peer_node.ws_port;

    tracing::info!("Gateway data dir: {:?}", gateway.temp_dir_path);
    tracing::info!("Node data dir: {:?}", peer_node.temp_dir_path);

    // Give extra time for peer to connect to gateway
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Connect to gateway websocket API
    let uri_gw =
        format!("ws://127.0.0.1:{ws_api_port_gw}/v1/contract/command?encodingProtocol=native");
    let (stream_gw, _) = connect_async(&uri_gw).await?;
    let mut client_gw = WebApi::start(stream_gw);

    // Connect to node websocket API
    let uri_node =
        format!("ws://127.0.0.1:{ws_api_port_node}/v1/contract/command?encodingProtocol=native");
    let (stream_node, _) = connect_async(&uri_node).await?;
    let _client_node = WebApi::start(stream_node);

    // First just test that we can query subscription info
    tracing::info!("Testing basic subscription query without any subscriptions");

    // Query subscription info from gateway
    tracing::info!("Querying subscription info from gateway");
    client_gw
        .send(ClientRequest::NodeQueries(
            freenet_stdlib::client_api::NodeQuery::SubscriptionInfo,
        ))
        .await?;

    // Wait for subscription info response
    let resp = timeout(Duration::from_secs(5), client_gw.recv()).await??;

    match resp {
        HostResponse::QueryResponse(QueryResponse::NetworkDebug(info)) => {
            tracing::info!("Gateway subscription info:");
            tracing::info!("  Connected peers: {:?}", info.connected_peers);
            tracing::info!("  Total subscriptions: {}", info.subscriptions.len());

            // Should be empty since we haven't subscribed to anything
            assert!(
                info.subscriptions.is_empty(),
                "Expected no subscriptions initially"
            );
            tracing::info!("Test passed - query subscription info works");
        }
        other => {
            bail!("Unexpected response: {:?}", other);
        }
    }

    Ok(())
}

#[freenet_test(
    nodes = ["gateway", "peer-a"],
    auto_connect_peers = true,
    timeout_secs = 180,
    startup_wait_secs = 20,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_update_no_change_notification(ctx: &mut TestContext) -> TestResult {
    // Load test contract that properly handles NoChange
    const TEST_CONTRACT: &str = "test-contract-update-nochange";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    // Create initial state - a simple state that we can update
    #[derive(serde::Serialize, serde::Deserialize)]
    struct SimpleState {
        value: String,
        counter: u64,
    }

    let initial_state = SimpleState {
        value: "initial".to_string(),
        counter: 1,
    };
    let initial_state_bytes = serde_json::to_vec(&initial_state)?;
    let wrapped_state = WrappedState::from(initial_state_bytes);

    let peer_a = ctx.node("peer-a")?;
    let gateway = ctx.node("gateway")?;
    let ws_api_port = peer_a.ws_port;

    // Log data directories for debugging
    tracing::info!("Node A data dir: {:?}", peer_a.temp_dir_path);
    tracing::info!("Node B (gw) data dir: {:?}", gateway.temp_dir_path);

    // Give extra time for peer to connect to gateway
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Connect to node A websocket API
    let uri = format!("ws://127.0.0.1:{ws_api_port}/v1/contract/command?encodingProtocol=native");
    let (stream, _) = connect_async(&uri).await?;
    let mut client_api_a = WebApi::start(stream);

    // Put contract with initial state
    make_put(
        &mut client_api_a,
        wrapped_state.clone(),
        contract.clone(),
        false,
    )
    .await?;

    // Wait for put response
    let resp = tokio::time::timeout(Duration::from_secs(30), client_api_a.recv()).await;
    match resp {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
            assert_eq!(key, contract_key, "Contract key mismatch in PUT response");
        }
        Ok(Ok(other)) => {
            tracing::warn!("unexpected response while waiting for put: {:?}", other);
        }
        Ok(Err(e)) => {
            bail!("Error receiving put response: {}", e);
        }
        Err(_) => {
            bail!("Timeout waiting for put response");
        }
    }

    // Now update with the EXACT SAME state (should trigger UpdateNoChange)
    tracing::info!("Sending UPDATE with identical state to trigger UpdateNoChange");
    make_update(&mut client_api_a, contract_key, wrapped_state.clone()).await?;

    // Wait for update response - THIS SHOULD NOT TIMEOUT
    let resp = tokio::time::timeout(Duration::from_secs(30), client_api_a.recv()).await;
    match resp {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateResponse {
            key,
            summary: _,
        }))) => {
            assert_eq!(
                key, contract_key,
                "Contract key mismatch in UPDATE response"
            );
            tracing::info!("SUCCESS: Received UpdateResponse for no-change update");
        }
        Ok(Ok(other)) => {
            bail!("Unexpected response while waiting for update: {:?}", other);
        }
        Ok(Err(e)) => {
            bail!("Error receiving update response: {}", e);
        }
        Err(_) => {
            // This is where the test will currently fail
            bail!("TIMEOUT waiting for update response - UpdateNoChange bug: client not notified when update results in no state change");
        }
    }

    Ok(())
}
