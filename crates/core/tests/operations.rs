use anyhow::{anyhow, bail};
use freenet::{
    config::{ConfigArgs, InlineGwConfig, NetworkArgs, SecretArgs, WebsocketApiArgs},
    dev_tool::TransportKeypair,
    local_node::NodeConfig,
    server::serve_gateway,
    test_utils::{
        self, load_delegate, make_get, make_put, make_subscribe, make_update,
        verify_contract_exists,
    },
};
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
    time::Duration,
};
use testresult::TestResult;
use tokio::select;
use tokio::time::timeout;
use tokio_tungstenite::connect_async;
use tracing::{level_filters::LevelFilter, span, Instrument, Level};

static RNG: once_cell::sync::Lazy<std::sync::Mutex<rand::rngs::StdRng>> =
    once_cell::sync::Lazy::new(|| {
        std::sync::Mutex::new(rand::rngs::StdRng::from_seed(
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
    let key = TransportKeypair::new_with_rng(&mut *RNG.lock().unwrap());
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
            location: Some(RNG.lock().unwrap().gen()),
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
    temp_dir: &tempfile::TempDir,
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
                verify_contract_exists(temp_dir.path(), key).await?;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_put_contract() -> TestResult {
    freenet::config::set_logger(Some(LevelFilter::INFO), None);
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    let network_socket_b = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_a = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_b = TcpListener::bind("127.0.0.1:0")?;

    let (config_b, preset_cfg_b, config_b_gw) = {
        let (cfg, preset) = base_node_test_config(
            true,
            vec![],
            Some(network_socket_b.local_addr()?.port()),
            ws_api_port_socket_b.local_addr()?.port(),
        )
        .await?;
        let public_port = cfg.network_api.public_port.unwrap();
        let path = preset.temp_dir.path().to_path_buf();
        (cfg, preset, gw_config(public_port, &path)?)
    };
    let ws_api_port_peer_b = config_b.ws_api.ws_api_port.unwrap();

    let (config_a, preset_cfg_a) = base_node_test_config(
        false,
        vec![serde_json::to_string(&config_b_gw)?],
        None,
        ws_api_port_socket_a.local_addr()?.port(),
    )
    .await?;
    let ws_api_port_peer_a = config_a.ws_api.ws_api_port.unwrap();

    tracing::info!("Node A data dir: {:?}", preset_cfg_b.temp_dir.path());
    tracing::info!("Node B data dir: {:?}", preset_cfg_a.temp_dir.path());

    std::mem::drop(ws_api_port_socket_a); // Free the port so it does not fail on initialization
    let node_a = async move {
        let config = config_a.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    std::mem::drop(network_socket_b); // Free the port so it does not fail on initialization
    std::mem::drop(ws_api_port_socket_b);
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
        tracing::info!("Waiting for nodes to start up...");
        tokio::time::sleep(Duration::from_secs(15)).await;
        tracing::info!("Nodes should be ready, proceeding with test...");

        // Connect to node A's websocket API
        let uri = format!(
            "ws://127.0.0.1:{ws_api_port_peer_a}/v1/contract/command?encodingProtocol=native"
        );
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
                get_contract(&mut client_api_a, contract_key, &preset_cfg_b.temp_dir).await?;
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
                get_contract(&mut client_api_b, contract_key, &preset_cfg_b.temp_dir).await?;
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

        Ok::<_, anyhow::Error>(())
    });

    select! {
        a = node_a => {
            let Err(a) = a;
            return Err(anyhow!(a).into());
        }
        b = node_b => {
            let Err(b) = b;
            return Err(anyhow!(b).into());
        }
        r = test => {
            r??;
            // Give time for cleanup before dropping nodes
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_update_contract() -> TestResult {
    freenet::config::set_logger(Some(LevelFilter::INFO), None);

    // Load test contract
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    // Create initial state with empty todo list
    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    // Create network sockets
    let network_socket_b = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_a = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_b = TcpListener::bind("127.0.0.1:0")?;

    // Configure gateway node B
    let (config_b, preset_cfg_b, config_b_gw) = {
        let (cfg, preset) = base_node_test_config(
            true,
            vec![],
            Some(network_socket_b.local_addr()?.port()),
            ws_api_port_socket_b.local_addr()?.port(),
        )
        .await?;
        let public_port = cfg.network_api.public_port.unwrap();
        let path = preset.temp_dir.path().to_path_buf();
        (cfg, preset, gw_config(public_port, &path)?)
    };

    // Configure client node A
    let (config_a, preset_cfg_a) = base_node_test_config(
        false,
        vec![serde_json::to_string(&config_b_gw)?],
        None,
        ws_api_port_socket_a.local_addr()?.port(),
    )
    .await?;
    let ws_api_port = config_a.ws_api.ws_api_port.unwrap();

    // Log data directories for debugging
    tracing::info!("Node A data dir: {:?}", preset_cfg_a.temp_dir.path());
    tracing::info!("Node B (gw) data dir: {:?}", preset_cfg_b.temp_dir.path());

    // Free ports so they don't fail on initialization
    std::mem::drop(ws_api_port_socket_a);
    std::mem::drop(network_socket_b);
    std::mem::drop(ws_api_port_socket_b);

    // Start node A (client)
    let node_a = async move {
        let config = config_a.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Start node B (gateway)
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
        tokio::time::sleep(Duration::from_secs(20)).await; // Increased sleep duration

        // Connect to node A websocket API
        let uri =
            format!("ws://127.0.0.1:{ws_api_port}/v1/contract/command?encodingProtocol=native");
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
                get_contract(&mut client_api_a, contract_key, &preset_cfg_b.temp_dir).await?;

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

        Ok::<_, anyhow::Error>(())
    });

    // Wait for test completion or node failures
    select! {
        a = node_a => {
            let Err(a) = a;
            return Err(anyhow!("Node A failed: {}", a).into());
        }
        b = node_b => {
            let Err(b) = b;
            return Err(anyhow!("Node B failed: {}", b).into());
        }
        r = test => {
            r??;
            // Keep nodes alive for pending operations to complete
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multiple_clients_subscription() -> TestResult {
    freenet::config::set_logger(Some(LevelFilter::INFO), None);

    // Load test contract
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    // Create initial state with empty todo list
    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    // Create network sockets
    let network_socket_b = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_a = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_b = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_c = TcpListener::bind("127.0.0.1:0")?; // Socket for node C (second client)

    // Configure gateway node
    let (config_gw, preset_cfg_b, gw_cfg) = {
        let (cfg, preset) = base_node_test_config(
            true,
            vec![],
            Some(network_socket_b.local_addr()?.port()),
            ws_api_port_socket_b.local_addr()?.port(),
        )
        .await?;
        let public_port = cfg.network_api.public_port.unwrap();
        let path = preset.temp_dir.path().to_path_buf();
        (cfg, preset, gw_config(public_port, &path)?)
    };

    // Configure client node A
    let (config_a, preset_cfg_a) = base_node_test_config(
        false,
        vec![serde_json::to_string(&gw_cfg)?],
        None,
        ws_api_port_socket_a.local_addr()?.port(),
    )
    .await?;
    let ws_api_port_a = config_a.ws_api.ws_api_port.unwrap();

    // Configure client node B (second client node)
    let (config_b, preset_cfg_c) = base_node_test_config(
        false,
        vec![serde_json::to_string(&gw_cfg)?],
        None,
        ws_api_port_socket_c.local_addr()?.port(),
    )
    .await?;
    let ws_api_port_b = config_b.ws_api.ws_api_port.unwrap();

    // Log data directories for debugging
    tracing::info!("Node A data dir: {:?}", preset_cfg_a.temp_dir.path());
    tracing::info!("Node B (gw) data dir: {:?}", preset_cfg_b.temp_dir.path());
    tracing::info!("Node C data dir: {:?}", preset_cfg_c.temp_dir.path());

    // Free ports so they don't fail on initialization
    std::mem::drop(ws_api_port_socket_a);
    std::mem::drop(network_socket_b);
    std::mem::drop(ws_api_port_socket_b);
    std::mem::drop(ws_api_port_socket_c);

    // Start node A (first client)
    let node_a = async move {
        let config = config_a.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Start GW node
    let node_gw = async {
        let config = config_gw.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Start node B (second client)
    let node_b = async {
        let config = config_b.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    let test = tokio::time::timeout(Duration::from_secs(600), async {
        // Wait for nodes to start up - CI environments need more time
        tokio::time::sleep(Duration::from_secs(40)).await;

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
            let resp =
                tokio::time::timeout(Duration::from_secs(120), client_api1_node_a.recv()).await;
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
            let resp =
                tokio::time::timeout(Duration::from_secs(30), client_api1_node_a.recv()).await;
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
            let resp =
                tokio::time::timeout(Duration::from_secs(30), client_api2_node_a.recv()).await;
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
            let resp =
                tokio::time::timeout(Duration::from_secs(30), client_api2_node_a.recv()).await;
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
            let resp =
                tokio::time::timeout(Duration::from_secs(60), client_api_node_b.recv()).await;
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
            let resp =
                tokio::time::timeout(Duration::from_secs(60), client_api_node_b.recv()).await;
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
                    Ok(Ok(HostResponse::ContractResponse(
                        ContractResponse::UpdateNotification { key, update },
                    ))) => {
                        assert_eq!(
                            key, contract_key,
                            "Contract key mismatch in UPDATE notification for client 1"
                        );

                        // Verify update content
                        match update {
                            UpdateData::State(state) => {
                                let received_todo_list: test_utils::TodoList =
                                    serde_json::from_slice(state.as_ref()).expect(
                                        "Failed to deserialize state from update notification",
                                    );

                                assert_eq!(
                                    received_todo_list.tasks.len(),
                                    1,
                                    "Should have one task"
                                );
                                assert_eq!(
                                    received_todo_list.tasks[0].id, expected_task.id,
                                    "Task ID should match"
                                );
                                assert_eq!(
                                    received_todo_list.tasks[0].title, expected_task.title,
                                    "Task title should match"
                                );
                                assert_eq!(
                                    received_todo_list.tasks[0].description,
                                    expected_task.description,
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
                    Ok(Ok(HostResponse::ContractResponse(
                        ContractResponse::UpdateNotification { key, update },
                    ))) => {
                        assert_eq!(
                            key, contract_key,
                            "Contract key mismatch in UPDATE notification for client 2"
                        );

                        // Verify update content
                        match update {
                            UpdateData::State(state) => {
                                let received_todo_list: test_utils::TodoList =
                                    serde_json::from_slice(state.as_ref()).expect(
                                        "Failed to deserialize state from update notification",
                                    );

                                assert_eq!(
                                    received_todo_list.tasks.len(),
                                    1,
                                    "Should have one task"
                                );
                                assert_eq!(
                                    received_todo_list.tasks[0].id, expected_task.id,
                                    "Task ID should match"
                                );
                                assert_eq!(
                                    received_todo_list.tasks[0].title, expected_task.title,
                                    "Task title should match"
                                );
                                assert_eq!(
                                    received_todo_list.tasks[0].description,
                                    expected_task.description,
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
                let resp =
                    tokio::time::timeout(Duration::from_secs(1), client_api_node_b.recv()).await;
                match resp {
                    Ok(Ok(HostResponse::ContractResponse(
                        ContractResponse::UpdateNotification { key, update },
                    ))) => {
                        assert_eq!(
                            key, contract_key,
                            "Contract key mismatch in UPDATE notification for client 3"
                        );

                        // Verify update content
                        match update {
                            UpdateData::State(state) => {
                                let received_todo_list: test_utils::TodoList =
                                    serde_json::from_slice(state.as_ref()).expect(
                                        "Failed to deserialize state from update notification",
                                    );

                                assert_eq!(
                                    received_todo_list.tasks.len(),
                                    1,
                                    "Should have one task"
                                );
                                assert_eq!(
                                    received_todo_list.tasks[0].id, expected_task.id,
                                    "Task ID should match"
                                );
                                assert_eq!(
                                    received_todo_list.tasks[0].title, expected_task.title,
                                    "Task title should match"
                                );
                                assert_eq!(
                                    received_todo_list.tasks[0].description,
                                    expected_task.description,
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

        Ok::<_, anyhow::Error>(())
    });

    // Wait for test completion or node failures
    select! {
        a = node_a => {
            let Err(a) = a;
            return Err(anyhow!("Node A failed: {}", a).into());
        }
        b = node_gw => {
            let Err(b) = b;
            return Err(anyhow!("Node B failed: {}", b).into());
        }
        c = node_b => {
            let Err(c) = c;
            return Err(anyhow!("Node C failed: {}", c).into());
        }
        r = test => {
            r??;
            // Give time for cleanup before dropping nodes
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_get_with_subscribe_flag() -> TestResult {
    freenet::config::set_logger(Some(LevelFilter::INFO), None);

    // Load test contract
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    // Create initial state with empty todo list
    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    // Create network sockets
    let network_socket_b = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_a = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_b = TcpListener::bind("127.0.0.1:0")?;

    // Configure gateway node B
    let (config_b, preset_cfg_b, config_b_gw) = {
        let (cfg, preset) = base_node_test_config(
            true,
            vec![],
            Some(network_socket_b.local_addr()?.port()),
            ws_api_port_socket_b.local_addr()?.port(),
        )
        .await?;
        let public_port = cfg.network_api.public_port.unwrap();
        let path = preset.temp_dir.path().to_path_buf();
        (cfg, preset, gw_config(public_port, &path)?)
    };

    // Configure client node A
    let (config_a, preset_cfg_a) = base_node_test_config(
        false,
        vec![serde_json::to_string(&config_b_gw)?],
        None,
        ws_api_port_socket_a.local_addr()?.port(),
    )
    .await?;
    let ws_api_port_a = config_a.ws_api.ws_api_port.unwrap();

    // Log data directories for debugging
    tracing::info!("Node A data dir: {:?}", preset_cfg_a.temp_dir.path());
    tracing::info!("Node B (gw) data dir: {:?}", preset_cfg_b.temp_dir.path());

    // Free ports so they don't fail on initialization
    std::mem::drop(ws_api_port_socket_a);
    std::mem::drop(network_socket_b);
    std::mem::drop(ws_api_port_socket_b);

    // Start node A (client)
    let node_a = async move {
        let config = config_a.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Start node B (gateway)
    let node_b = async {
        let config = config_b.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    let test = tokio::time::timeout(Duration::from_secs(120), async {
        // Wait for nodes to start up (extra time for CI with limited resources)
        tokio::time::sleep(Duration::from_secs(20)).await;

        // Connect first client to node A's websocket API (for putting the contract)
        let uri_a = format!(
            "ws://127.0.0.1:{ws_api_port_a}/v1/contract/command?encodingProtocol=native"
        );
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
                            tracing::warn!(
                                "Client 1: Received unexpected update type: {:?}",
                                update
                            );
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

        Ok::<_, anyhow::Error>(())
    }).instrument(span!(Level::INFO, "test_get_with_subscribe_flag"));

    // Wait for test completion or node failures
    select! {
        a = node_a => {
            let Err(a) = a;
            return Err(anyhow!("Node A failed: {}", a).into());
        }
        b = node_b => {
            let Err(b) = b;
            return Err(anyhow!("Node B failed: {}", b).into());
        }
        r = test => {
            r??;
            // Keep nodes alive for pending operations to complete
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    Ok(())
}

// FIXME Update notification is not received
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_put_with_subscribe_flag() -> TestResult {
    freenet::config::set_logger(Some(LevelFilter::INFO), None);

    // Load test contract
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    // Create initial state with empty todo list
    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    // Create network sockets
    let network_socket_b = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_a = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_b = TcpListener::bind("127.0.0.1:0")?;

    // Configure gateway node B
    let (config_b, preset_cfg_b, config_b_gw) = {
        let (cfg, preset) = base_node_test_config(
            true,
            vec![],
            Some(network_socket_b.local_addr()?.port()),
            ws_api_port_socket_b.local_addr()?.port(),
        )
        .await?;
        let public_port = cfg.network_api.public_port.unwrap();
        let path = preset.temp_dir.path().to_path_buf();
        (cfg, preset, gw_config(public_port, &path)?)
    };

    // Configure client node A
    let (config_a, preset_cfg_a) = base_node_test_config(
        false,
        vec![serde_json::to_string(&config_b_gw)?],
        None,
        ws_api_port_socket_a.local_addr()?.port(),
    )
    .await?;
    let ws_api_port_a = config_a.ws_api.ws_api_port.unwrap();

    // Log data directories for debugging
    tracing::info!("Node A data dir: {:?}", preset_cfg_a.temp_dir.path());
    tracing::info!("Node B (gw) data dir: {:?}", preset_cfg_b.temp_dir.path());

    // Free ports so they don't fail on initialization
    std::mem::drop(ws_api_port_socket_a);
    std::mem::drop(network_socket_b);
    std::mem::drop(ws_api_port_socket_b);

    // Start node A (client)
    let node_a = async move {
        let config = config_a.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Start node B (gateway)
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
        tokio::time::sleep(Duration::from_secs(20)).await;

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
        loop {
            let resp = tokio::time::timeout(Duration::from_secs(30), client_api1.recv()).await;
            match resp {
                Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
                    assert_eq!(key, contract_key, "Contract key mismatch in PUT response");
                    break;
                }
                Ok(Ok(other)) => {
                    bail!("Contract key mismatch in PUT response: {:?}", other);
                }
                Ok(Err(e)) => {
                    tracing::error!("Client 1: Error receiving put response: {}", e);
                }
                Err(_) => {
                    tracing::error!("Client 1: Error receiving put response");
                }
            }
        }

        // Second client gets the contract (without subscribing)
        make_get(&mut client_api2, contract_key, true, false).await?;

        // Wait for get response on second client
        loop {
            let resp = tokio::time::timeout(Duration::from_secs(30), client_api2.recv()).await;
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
                    bail!("unexpected response while waiting for get: {:?}", other);
                }
                Ok(Err(e)) => {
                    tracing::error!("Client 2: Error receiving get response: {}", e);
                }
                Err(_) => {
                    tracing::error!("Client 2: Error receiving get response");
                }
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
        loop {
            let resp = tokio::time::timeout(Duration::from_secs(30), client_api2.recv()).await;
            match resp {
                Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateResponse {
                    key,
                    summary: _,
                }))) => {
                    assert_eq!(
                        key, contract_key,
                        "Contract key mismatch in UPDATE response"
                    );
                    break;
                }
                Ok(Ok(other)) => {
                    bail!("unexpected response while waiting for update: {:?}", other);
                }
                Ok(Err(e)) => {
                    tracing::error!("Client 2: Error receiving update response: {}", e);
                }
                Err(_) => {
                    tracing::error!("Client 2: Error receiving update response");
                }
            }
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
                            tracing::warn!(
                                "Client 1: Received unexpected update type: {:?}",
                                update
                            );
                        }
                    }
                    client1_received_notification = true;
                    break;
                }
                Ok(Ok(other)) => {
                    bail!("unexpected response while waiting for update: {:?}", other);
                }
                Ok(Err(e)) => {
                    tracing::error!("Client 2: Error receiving update response: {}", e);
                }
                Err(_) => {
                    tracing::error!("Client 2: Error receiving update response");
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

        Ok::<_, anyhow::Error>(())
    });

    // Wait for test completion or node failures
    select! {
        a = node_a => {
            let Err(a) = a;
            return Err(anyhow!("Node A failed: {}", a).into());
        }
        b = node_b => {
            let Err(b) = b;
            return Err(anyhow!("Node B failed: {}", b).into());
        }
        r = test => {
            r??;
            // Keep nodes alive for pending operations to complete
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_delegate_request() -> TestResult {
    freenet::config::set_logger(Some(LevelFilter::INFO), None);
    const TEST_DELEGATE: &str = "test-delegate-integration";

    // Configure environment variables for optimized release build
    std::env::set_var("CARGO_PROFILE_RELEASE_LTO", "true");
    std::env::set_var("CARGO_PROFILE_RELEASE_CODEGEN_UNITS", "1");
    std::env::set_var("CARGO_PROFILE_RELEASE_STRIP", "true");

    // Load delegate (moving this outside the async block)
    let params = Parameters::from(vec![]);
    let delegate = load_delegate(TEST_DELEGATE, params.clone())?;
    let delegate_key = delegate.key().clone();

    // Create sockets for ports
    let network_socket_gw = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_client = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_gw = TcpListener::bind("127.0.0.1:0")?;

    // Configure gateway node
    let (config_gw, preset_cfg_gw, gw_cfg) = {
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
        vec![serde_json::to_string(&gw_cfg)?],
        None,
        ws_api_port_socket_client.local_addr()?.port(),
    )
    .await?;
    let ws_api_port_client = config_client.ws_api.ws_api_port.unwrap();

    // Log data directories for debugging
    tracing::info!(
        "Client node data dir: {:?}",
        preset_cfg_client.temp_dir.path()
    );
    tracing::info!("Gateway node data dir: {:?}", preset_cfg_gw.temp_dir.path());

    // Free ports so they don't fail on initialization
    std::mem::drop(ws_api_port_socket_client);
    std::mem::drop(network_socket_gw);
    std::mem::drop(ws_api_port_socket_gw);

    // Start gateway node
    let node_gw = async {
        let config = config_gw.build().await?;
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

    // Wait for the nodes to start and run the test
    let test = tokio::time::timeout(Duration::from_secs(180), async {
        // Wait for nodes to start up
        tokio::time::sleep(Duration::from_secs(20)).await;

        // Connect to the client node's WebSocket API
        let uri = format!(
            "ws://127.0.0.1:{ws_api_port_client}/v1/contract/command?encodingProtocol=native"
        );
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
                println!("Successfully registered delegate with key: {key}");
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

                        println!("Successfully received and verified delegate response");
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
            // Keep nodes alive for pending operations to complete
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Long-running test (90s) - needs update for new keep-alive constants"]
async fn test_gateway_packet_size_change_after_60s() -> TestResult {
    freenet::config::set_logger(Some(LevelFilter::DEBUG), None);

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Long-running test (75s) - run with --ignored flag"]
async fn test_production_decryption_error_scenario() -> TestResult {
    freenet::config::set_logger(Some(LevelFilter::DEBUG), None);

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_subscription_introspection() -> TestResult {
    freenet::config::set_logger(Some(LevelFilter::DEBUG), None);

    // Load test contract - not used in this simplified test
    const TEST_CONTRACT: &str = "test-contract-integration";
    let _contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;

    // Create initial state - not used in this simplified test
    let _initial_state = test_utils::create_empty_todo_list();

    // Setup network sockets
    let network_socket_gw = TcpListener::bind("127.0.0.1:0")?;
    let network_socket_node = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_gw = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_node = TcpListener::bind("127.0.0.1:0")?;

    // Configure gateway node
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
    let ws_api_port_gw = config_gw.ws_api.ws_api_port.unwrap();

    // Configure regular node
    let (config_node, preset_cfg_node) = base_node_test_config(
        false,
        vec![serde_json::to_string(&config_gw_info)?],
        Some(network_socket_node.local_addr()?.port()),
        ws_api_port_socket_node.local_addr()?.port(),
    )
    .await?;
    let ws_api_port_node = config_node.ws_api.ws_api_port.unwrap();

    tracing::info!("Gateway data dir: {:?}", preset_cfg_gw.temp_dir.path());
    tracing::info!("Node data dir: {:?}", preset_cfg_node.temp_dir.path());

    // Free ports
    std::mem::drop(network_socket_gw);
    std::mem::drop(network_socket_node);
    std::mem::drop(ws_api_port_socket_gw);
    std::mem::drop(ws_api_port_socket_node);

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

    let node_regular = async {
        let config = config_node.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    let test = tokio::time::timeout(Duration::from_secs(180), async {
        // Wait for nodes to start and connect
        tokio::time::sleep(Duration::from_secs(10)).await;

        // Connect to gateway websocket API
        let uri_gw =
            format!("ws://127.0.0.1:{ws_api_port_gw}/v1/contract/command?encodingProtocol=native");
        let (stream_gw, _) = connect_async(&uri_gw).await?;
        let mut client_gw = WebApi::start(stream_gw);

        // Connect to node websocket API
        let uri_node = format!(
            "ws://127.0.0.1:{ws_api_port_node}/v1/contract/command?encodingProtocol=native"
        );
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

        Ok::<_, anyhow::Error>(())
    });

    // Run test with timeout
    select! {
        gw = node_gw => {
            let Err(e) = gw;
            return Err(anyhow!("Gateway node failed: {}", e).into())
        }
        node = node_regular => {
            let Err(e) = node;
            return Err(anyhow!("Regular node failed: {}", e).into())
        }
        r = test => {
            r??
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_update_no_change_notification() -> TestResult {
    freenet::config::set_logger(Some(LevelFilter::INFO), None);

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

    // Create network sockets
    let network_socket_b = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_a = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_b = TcpListener::bind("127.0.0.1:0")?;

    // Configure gateway node B
    let (config_b, preset_cfg_b, config_b_gw) = {
        let (cfg, preset) = base_node_test_config(
            true,
            vec![],
            Some(network_socket_b.local_addr()?.port()),
            ws_api_port_socket_b.local_addr()?.port(),
        )
        .await?;
        let public_port = cfg.network_api.public_port.unwrap();
        let path = preset.temp_dir.path().to_path_buf();
        (cfg, preset, gw_config(public_port, &path)?)
    };

    // Configure client node A
    let (config_a, preset_cfg_a) = base_node_test_config(
        false,
        vec![serde_json::to_string(&config_b_gw)?],
        None,
        ws_api_port_socket_a.local_addr()?.port(),
    )
    .await?;
    let ws_api_port = config_a.ws_api.ws_api_port.unwrap();

    // Log data directories for debugging
    tracing::info!("Node A data dir: {:?}", preset_cfg_a.temp_dir.path());
    tracing::info!("Node B (gw) data dir: {:?}", preset_cfg_b.temp_dir.path());

    // Free ports so they don't fail on initialization
    std::mem::drop(ws_api_port_socket_a);
    std::mem::drop(network_socket_b);
    std::mem::drop(ws_api_port_socket_b);

    // Start node A (client)
    let node_a = async move {
        let config = config_a.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Start node B (gateway)
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
        tokio::time::sleep(Duration::from_secs(20)).await;

        // Connect to node A websocket API
        let uri =
            format!("ws://127.0.0.1:{ws_api_port}/v1/contract/command?encodingProtocol=native");
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

        Ok::<(), anyhow::Error>(())
    });

    select! {
        a = node_a => {
            let Err(a) = a;
            return Err(anyhow!(a).into());
        }
        b = node_b => {
            let Err(b) = b;
            return Err(anyhow!(b).into());
        }
        r = test => {
            r??;
            // Give time for cleanup before dropping nodes
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    Ok(())
}
