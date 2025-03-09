use crate::test_utils::verify_contract_exists;
use anyhow::{anyhow, bail};
use freenet::{
    config::{ConfigArgs, InlineGwConfig, NetworkArgs, SecretArgs, WebsocketApiArgs},
    dev_tool::TransportKeypair,
    local_node::NodeConfig,
    server::serve_gateway,
};
use freenet_stdlib::{
    client_api::{ContractResponse, HostResponse, WebApi},
    prelude::*,
};
use futures::FutureExt;
use rand::{random, Rng, SeedableRng};
use std::{
    net::{Ipv4Addr, TcpListener},
    path::Path,
    time::Duration,
};
use test_utils::{make_get, make_put, make_update};
use testresult::TestResult;
use tokio::select;
use tokio_tungstenite::connect_async;
use tracing::level_filters::LevelFilter;

mod test_utils;

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
        },
        config_paths: {
            let mut args = freenet::config::ConfigPathsArgs::default();
            args.config_dir = Some(temp_dir.path().to_path_buf());
            args.data_dir = Some(temp_dir.path().to_path_buf());
            args
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
    make_get(client, key, true).await?;
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

#[tokio::test(flavor = "multi_thread")]
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

    println!("Node A data dir: {:?}", preset_cfg_b.temp_dir.path());
    println!("Node B data dir: {:?}", preset_cfg_a.temp_dir.path());

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

    let test = tokio::time::timeout(Duration::from_secs(60), async {
        // Wait for nodes to start up
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Connect to node A's websocket API
        let uri = format!(
            "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
            ws_api_port_peer_a
        );
        let (stream, _) = connect_async(&uri).await?;
        let mut client_api_a = WebApi::start(stream);

        make_put(&mut client_api_a, wrapped_state.clone(), contract.clone()).await?;

        // Wait for put response
        loop {
            let resp = tokio::time::timeout(Duration::from_secs(30), client_api_a.recv()).await;
            match resp {
                Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
                    assert_eq!(key, contract_key);
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
                "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
                ws_api_port_peer_b
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
        }

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
    println!("Node A data dir: {:?}", preset_cfg_a.temp_dir.path());
    println!("Node B (gw) data dir: {:?}", preset_cfg_b.temp_dir.path());

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

    let test = tokio::time::timeout(Duration::from_secs(60), async {
        // Wait for nodes to start up
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Connect to node A websocket API
        let uri = format!(
            "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
            ws_api_port
        );
        let (stream, _) = connect_async(&uri).await?;
        let mut client_api_a = WebApi::start(stream);

        // Put contract with initial state
        make_put(&mut client_api_a, wrapped_state.clone(), contract.clone()).await?;

        // Wait for put response
        loop {
            let resp = tokio::time::timeout(Duration::from_secs(30), client_api_a.recv()).await;
            match resp {
                Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
                    assert_eq!(key, contract_key, "Contract key mismatch in PUT response");
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
        loop {
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
                    break;
                }
                Ok(Ok(other)) => {
                    tracing::warn!("unexpected response while waiting for update: {:?}", other);
                }
                Ok(Err(e)) => {
                    bail!("Error receiving update response: {}", e);
                }
                Err(_) => {
                    bail!("Timeout waiting for update response");
                }
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
        }
    }

    Ok(())
}
