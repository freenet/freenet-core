use anyhow::{anyhow, bail};
use freenet::{
    config::{ConfigArgs, InlineGwConfig, NetworkArgs, WebsocketApiArgs},
    dev_tool::TransportKeypair,
    local_node::NodeConfig,
    server::serve_gateway,
};
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, ContractResponse, HostResponse, WebApi},
    prelude::*,
};
use futures::FutureExt;
use rand::{random, Rng, SeedableRng};
use std::{
    net::{Ipv4Addr, TcpListener},
    path::Path,
    time::Duration,
};
use test_utils::{make_put, make_update, make_get};
use testresult::TestResult;
use tokio::select;
use tokio_tungstenite::connect_async;
use tracing::level_filters::LevelFilter;
use crate::test_utils::verify_contract_exists;

mod test_utils;

static RNG: once_cell::sync::Lazy<std::sync::Mutex<rand::rngs::StdRng>> =
    once_cell::sync::Lazy::new(|| {
        std::sync::Mutex::new(rand::rngs::StdRng::from_seed(
            *b"0102030405060708090a0b0c0d0e0f10",
        ))
    });

async fn base_test_config(
    is_gateway: bool,
    gateways: Vec<String>,
    public_port: Option<u16>,
    ws_api_port: u16,
) -> anyhow::Result<ConfigArgs> {
    let network_port = if public_port.is_none() {
        (!is_gateway)
            .then(|| Ok::<_, anyhow::Error>(TcpListener::bind("127.0.0.1:0")?.local_addr()?.port()))
            .transpose()?
    } else {
        public_port
    };
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
            network_port,
            bandwidth_limit: None,
        },
        ..Default::default()
    };
    Ok(config)
}

fn gw_config(port: u16, path: &Path) -> anyhow::Result<(InlineGwConfig, TransportKeypair)> {
    // generate key and store it in a temp file
    let key = TransportKeypair::new_with_rng(&mut *RNG.lock().unwrap());
    key.public().save(path)?;
    Ok((
        InlineGwConfig {
            address: (Ipv4Addr::LOCALHOST, port).into(),
            location: Some(random()),
            public_key_path: path.into(),
        },
        key,
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_put_contract() -> TestResult {
    freenet::config::set_logger(Some(LevelFilter::TRACE), None);
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    let node_b_tmp_dir = tempfile::tempdir()?;
    let node_b_pub_key = node_b_tmp_dir.path().join("pub_key.pem");
    let reserved_listener = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_a = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_b = TcpListener::bind("127.0.0.1:0")?;
    let (gw_config, gw_keypair) =
        gw_config(reserved_listener.local_addr()?.port(), &node_b_pub_key)?;
    let gw_loc = gw_config.location;
    let node_a_tmp_dir = tempfile::tempdir()?;
    println!("Node A data dir: {:?}", node_a_tmp_dir.path());
    println!("Node B data dir: {:?}", node_b_tmp_dir.path());

    let mut config_a = base_test_config(
        false,
        vec![serde_json::to_string(&gw_config)?],
        None,
        ws_api_port_a.local_addr()?.port(),
    )
    .await?;
    config_a.config_paths.config_dir = Some(node_a_tmp_dir.path().to_path_buf());
    config_a.config_paths.data_dir = Some(node_a_tmp_dir.path().to_path_buf());

    let ws_api_port = config_a.ws_api.ws_api_port.unwrap();
    std::mem::drop(ws_api_port_a); // Free the port so it does not fail on initialization
    let node_a = async move {
        let config = config_a.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    let mut config_b = base_test_config(
        true,
        vec![],
        Some(gw_config.address.port()),
        ws_api_port_b.local_addr()?.port(),
    )
    .await?;
    config_b.network_api.location = gw_loc;
    let keypair_file = node_b_tmp_dir.path().join("keypair.pem");
    gw_keypair.save(&keypair_file)?;
    config_b.secrets.transport_keypair = Some(keypair_file);
    config_b.network_api.is_gateway = true;
    config_b.config_paths.config_dir = Some(node_b_tmp_dir.path().to_path_buf());
    config_b.config_paths.data_dir = Some(node_b_tmp_dir.path().to_path_buf());
    std::mem::drop(reserved_listener); // Free the port so it does not fail on initialization
    std::mem::drop(ws_api_port_b);
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
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Connect to node A's websocket API
        let uri = format!(
            "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
            ws_api_port
        );
        let (stream, _) = connect_async(&uri).await?;
        let mut client = WebApi::start(stream);

        // Create a test contract and state
        let state = WrappedState::new(vec![]);
        make_put(&mut client, state.clone(), contract.clone()).await?;

        // Wait for put response
        loop {
            let resp = tokio::time::timeout(Duration::from_secs(30), client.recv()).await;
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

        // Send get request
        make_get(&mut client, contract_key, true).await?;

        // Wait for get response
        let (response_key, response_contract, response_state) = loop {
            let resp = tokio::time::timeout(Duration::from_secs(30), client.recv()).await;
            match resp {
                Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                    key,
                    contract: Some(contract),
                    state,
                }))) => {
                    verify_contract_exists(node_a_tmp_dir.path(), contract_key).await?;
                    break (key, contract, state);
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
        };

        // Verify the responses
        assert_eq!(response_key, contract_key);
        assert_eq!(response_contract, contract);
        assert_eq!(response_state, state);

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
    freenet::config::set_logger(Some(LevelFilter::TRACE), None);
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    let node_b_tmp_dir = tempfile::tempdir()?;
    let node_b_pub_key = node_b_tmp_dir.path().join("pub_key.pem");
    let reserved_listener = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_a = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_b = TcpListener::bind("127.0.0.1:0")?;
    let (gw_config, gw_keypair) =
        gw_config(reserved_listener.local_addr()?.port(), &node_b_pub_key)?;
    let gw_loc = gw_config.location;
    let node_a_tmp_dir = tempfile::tempdir()?;
    println!("Node A data dir: {:?}", node_a_tmp_dir.path());
    println!("Node B data dir: {:?}", node_b_tmp_dir.path());

    let mut config_a = base_test_config(
        false,
        vec![serde_json::to_string(&gw_config)?],
        None,
        ws_api_port_a.local_addr()?.port(),
    )
    .await?;
    config_a.config_paths.config_dir = Some(node_a_tmp_dir.path().to_path_buf());
    config_a.config_paths.data_dir = Some(node_a_tmp_dir.path().to_path_buf());

    let ws_api_port = config_a.ws_api.ws_api_port.unwrap();
    std::mem::drop(ws_api_port_a); // Free the port so it does not fail on initialization
    let node_a = async move {
        let config = config_a.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    let mut config_b = base_test_config(
        true,
        vec![],
        Some(gw_config.address.port()),
        ws_api_port_b.local_addr()?.port(),
    )
    .await?;
    config_b.network_api.location = gw_loc;
    let keypair_file = node_b_tmp_dir.path().join("keypair.pem");
    gw_keypair.save(&keypair_file)?;
    config_b.secrets.transport_keypair = Some(keypair_file);
    config_b.network_api.is_gateway = true;
    config_b.config_paths.config_dir = Some(node_b_tmp_dir.path().to_path_buf());
    config_b.config_paths.data_dir = Some(node_b_tmp_dir.path().to_path_buf());
    std::mem::drop(reserved_listener); // Free the port so it does not fail on initialization
    std::mem::drop(ws_api_port_b);
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
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Connect to node A's websocket API
        let uri = format!(
            "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
            ws_api_port
        );
        let (stream, _) = connect_async(&uri).await?;
        let mut client = WebApi::start(stream);

        // First put the contract with initial state
        let initial_state = WrappedState::new(vec![1, 2, 3]);
        make_put(&mut client, initial_state.clone(), contract.clone()).await?;

        // Wait for put response
        loop {
            let resp = tokio::time::timeout(Duration::from_secs(30), client.recv()).await;
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

        // Now update the contract state
        let updated_state = WrappedState::new(vec![4, 5, 6]);
        make_update(&mut client, contract_key, updated_state.clone()).await?;

        // Wait for update response
        let summary = loop {
            let resp = tokio::time::timeout(Duration::from_secs(30), client.recv()).await;
            match resp {
                Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateResponse { key, summary }))) => {
                    assert_eq!(key, contract_key);
                    break summary;
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
        };

        // Verify the update by getting the contract state
        make_get(&mut client, contract_key, true).await?;

        // Wait for get response and verify state
        let (response_key, response_contract, response_state) = loop {
            let resp = tokio::time::timeout(Duration::from_secs(30), client.recv()).await;
            match resp {
                Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                    key,
                    contract: Some(contract),
                    state,
                }))) => {
                    verify_contract_exists(node_a_tmp_dir.path(), contract_key).await?;
                    break (key, contract, state);
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
        };

        // Verify the responses
        assert_eq!(response_key, contract_key);
        assert_eq!(response_contract, contract);
        assert_eq!(response_state, updated_state);
        assert_eq!(summary, StateSummary::from(updated_state.as_ref().to_vec()));

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
