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
use tracing::{level_filters::LevelFilter, span, Instrument, Level};

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
#[ignore = "Test is flaky and needs investigation - hangs in CI"]
async fn test_gateway_reconnection() -> TestResult {
    freenet::config::set_logger(Some(LevelFilter::INFO), None);

    // Create sockets for ports
    let gateway_network_socket = TcpListener::bind("127.0.0.1:0")?;
    let gateway_ws_socket = TcpListener::bind("127.0.0.1:0")?;
    let peer_ws_socket = TcpListener::bind("127.0.0.1:0")?;

    let gateway_public_port = gateway_network_socket.local_addr()?.port();
    let gateway_ws_port = gateway_ws_socket.local_addr()?.port();
    let peer_ws_port = peer_ws_socket.local_addr()?.port();

    // Setup temp directories for nodes
    let gateway_temp_dir = tempfile::tempdir()?;
    let peer_temp_dir = tempfile::tempdir()?;

    // Configure gateway
    let gateway_key = TransportKeypair::new_with_rng(&mut *RNG.lock().unwrap());
    let gateway_transport_keypair = gateway_temp_dir.path().join("private.pem");
    gateway_key.save(&gateway_transport_keypair)?;
    gateway_key
        .public()
        .save(gateway_temp_dir.path().join("public.pem"))?;

    // Create empty gateways.toml file for gateway
    std::fs::write(
        gateway_temp_dir.path().join("gateways.toml"),
        "gateways = []",
    )?;

    let gateway_config = ConfigArgs {
        ws_api: WebsocketApiArgs {
            address: Some(Ipv4Addr::LOCALHOST.into()),
            ws_api_port: Some(gateway_ws_port),
        },
        network_api: NetworkArgs {
            public_address: Some(Ipv4Addr::LOCALHOST.into()),
            public_port: Some(gateway_public_port),
            is_gateway: true,
            skip_load_from_network: true,
            gateways: Some(vec![]),
            location: Some(RNG.lock().unwrap().gen()),
            ignore_protocol_checking: true,
            address: Some(Ipv4Addr::LOCALHOST.into()),
            network_port: Some(gateway_public_port),
            bandwidth_limit: None,
            blocked_addresses: None,
        },
        config_paths: {
            freenet::config::ConfigPathsArgs {
                config_dir: Some(gateway_temp_dir.path().to_path_buf()),
                data_dir: Some(gateway_temp_dir.path().to_path_buf()),
            }
        },
        secrets: SecretArgs {
            transport_keypair: Some(gateway_transport_keypair),
            ..Default::default()
        },
        ..Default::default()
    };

    // Configure peer
    let peer_key = TransportKeypair::new_with_rng(&mut *RNG.lock().unwrap());
    let peer_transport_keypair = peer_temp_dir.path().join("private.pem");
    peer_key.save(&peer_transport_keypair)?;
    peer_key
        .public()
        .save(peer_temp_dir.path().join("public.pem"))?;

    // Create gateways.toml file for peer with gateway info
    let gateway_info = format!(
        r#"[[gateways]]
address = "127.0.0.1:{}"
public_key_path = "{}"
"#,
        gateway_public_port,
        gateway_temp_dir.path().join("public.pem").display()
    );
    std::fs::write(peer_temp_dir.path().join("gateways.toml"), gateway_info)?;

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
            gateways: Some(vec![serde_json::to_string(&InlineGwConfig {
                address: (Ipv4Addr::LOCALHOST, gateway_public_port).into(),
                location: Some(RNG.lock().unwrap().gen()),
                public_key_path: gateway_temp_dir.path().join("public.pem"),
            })?]),
            location: Some(RNG.lock().unwrap().gen()),
            ignore_protocol_checking: true,
            address: Some(Ipv4Addr::LOCALHOST.into()),
            network_port: None,
            bandwidth_limit: None,
            blocked_addresses: None,
        },
        config_paths: {
            freenet::config::ConfigPathsArgs {
                config_dir: Some(peer_temp_dir.path().to_path_buf()),
                data_dir: Some(peer_temp_dir.path().to_path_buf()),
            }
        },
        secrets: SecretArgs {
            transport_keypair: Some(peer_transport_keypair),
            ..Default::default()
        },
        ..Default::default()
    };

    // Create test contract
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();
    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    // Start gateway node
    std::mem::drop(gateway_network_socket);
    std::mem::drop(gateway_ws_socket);
    let gateway = async {
        let config = gateway_config.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .instrument(span!(Level::DEBUG, "gateway"))
    .boxed_local();

    // Start peer node
    std::mem::drop(peer_ws_socket);
    let peer = async move {
        let config = peer_config.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .instrument(span!(Level::DEBUG, "peer"))
    .boxed_local();

    // Main test logic
    let test = async move {
        // Give nodes time to start up and connect
        tokio::time::sleep(Duration::from_secs(5)).await;

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
        make_get(&mut client_api, contract_key, false, false).await?;
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
        make_get(&mut client_api, contract_key, false, false).await?;
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
    };

    select! {
        g = gateway => {
            g.map_err(|e| anyhow!("Gateway error: {}", e))?;
        }
        p = peer => {
            p.map_err(|e| anyhow!("Peer error: {}", e))?;
        }
        r = test => {
            r?;
            // Give time for cleanup before dropping nodes
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    Ok(())
}
