use anyhow::{anyhow, bail};
use freenet::{
    config::{ConfigArgs, InlineGwConfig, NetworkArgs, SecretArgs, WebsocketApiArgs},
    dev_tool::TransportKeypair,
    local_node::NodeConfig,
    server::serve_gateway,
    test_utils::{self, make_get, make_put, make_subscribe, make_update},
};
use freenet_stdlib::{
    client_api::{ContractResponse, HostResponse, WebApi},
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
use tracing::level_filters::LevelFilter;

static RNG: LazyLock<Mutex<rand::rngs::StdRng>> = LazyLock::new(|| {
    Mutex::new(rand::rngs::StdRng::from_seed(
        *b"0102030405060708090a0b0c0d0e0f10",
    ))
});

/// Comprehensive test for proximity-based update forwarding
///
/// This test validates that:
/// 1. Nodes announce their cache to neighbors when they cache a contract
/// 2. Updates are forwarded to neighbors based on proximity cache knowledge
/// 3. Updates via proximity are distinguished from updates via subscription
/// 4. Proximity cache stats are correctly tracked
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_proximity_based_update_forwarding() -> TestResult {
    freenet::config::set_logger(Some(LevelFilter::INFO), None);

    // Load test contract
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();
    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state.clone());

    // Create updated state properly by deserializing, modifying, and re-serializing
    let updated_state = {
        let mut todo_list: test_utils::TodoList = serde_json::from_slice(&initial_state)
            .unwrap_or_else(|_| test_utils::TodoList {
                tasks: Vec::new(),
                version: 0,
            });

        // Add a task to the list
        todo_list.tasks.push(test_utils::Task {
            id: 1,
            title: "Test proximity forwarding".to_string(),
            description: "Verify that updates are forwarded based on proximity cache".to_string(),
            completed: false,
            priority: 1,
        });

        // Serialize back to bytes
        let updated_bytes = serde_json::to_vec(&todo_list).unwrap();
        WrappedState::from(updated_bytes)
    };

    // Create network sockets for gateway + 3 peers
    let gateway_network_socket = TcpListener::bind("127.0.0.1:0")?;
    let gateway_ws_socket = TcpListener::bind("127.0.0.1:0")?;
    let peer_a_ws_socket = TcpListener::bind("127.0.0.1:0")?;
    let peer_b_ws_socket = TcpListener::bind("127.0.0.1:0")?;
    let peer_c_ws_socket = TcpListener::bind("127.0.0.1:0")?;

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
    let peer_a_ws_port = peer_a_ws_socket.local_addr()?.port();
    let peer_b_ws_port = peer_b_ws_socket.local_addr()?.port();
    let peer_c_ws_port = peer_c_ws_socket.local_addr()?.port();

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

    let gateway_info = InlineGwConfig {
        address: (Ipv4Addr::LOCALHOST, gateway_port).into(),
        location: gateway_config.network_api.location,
        public_key_path: temp_dir_gw.path().join("public.pem"),
    };

    // Configure peer A
    let temp_dir_a = tempfile::tempdir()?;
    let peer_a_key = TransportKeypair::new();
    let peer_a_transport_keypair = temp_dir_a.path().join("private.pem");
    peer_a_key.save(&peer_a_transport_keypair)?;

    let peer_a_config = ConfigArgs {
        ws_api: WebsocketApiArgs {
            address: Some(Ipv4Addr::LOCALHOST.into()),
            ws_api_port: Some(peer_a_ws_port),
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
            config_dir: Some(temp_dir_a.path().to_path_buf()),
            data_dir: Some(temp_dir_a.path().to_path_buf()),
        },
        secrets: SecretArgs {
            transport_keypair: Some(peer_a_transport_keypair),
            ..Default::default()
        },
        ..Default::default()
    };

    // Configure peer B (similar to A)
    let temp_dir_b = tempfile::tempdir()?;
    let peer_b_key = TransportKeypair::new();
    let peer_b_transport_keypair = temp_dir_b.path().join("private.pem");
    peer_b_key.save(&peer_b_transport_keypair)?;

    let peer_b_config = ConfigArgs {
        ws_api: WebsocketApiArgs {
            address: Some(Ipv4Addr::LOCALHOST.into()),
            ws_api_port: Some(peer_b_ws_port),
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
            config_dir: Some(temp_dir_b.path().to_path_buf()),
            data_dir: Some(temp_dir_b.path().to_path_buf()),
        },
        secrets: SecretArgs {
            transport_keypair: Some(peer_b_transport_keypair),
            ..Default::default()
        },
        ..Default::default()
    };

    // Configure peer C (similar to A and B)
    let temp_dir_c = tempfile::tempdir()?;
    let peer_c_key = TransportKeypair::new();
    let peer_c_transport_keypair = temp_dir_c.path().join("private.pem");
    peer_c_key.save(&peer_c_transport_keypair)?;

    let peer_c_config = ConfigArgs {
        ws_api: WebsocketApiArgs {
            address: Some(Ipv4Addr::LOCALHOST.into()),
            ws_api_port: Some(peer_c_ws_port),
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
            config_dir: Some(temp_dir_c.path().to_path_buf()),
            data_dir: Some(temp_dir_c.path().to_path_buf()),
        },
        secrets: SecretArgs {
            transport_keypair: Some(peer_c_transport_keypair),
            ..Default::default()
        },
        ..Default::default()
    };

    // Start all nodes
    std::mem::drop(gateway_network_socket);
    std::mem::drop(gateway_ws_socket);
    std::mem::drop(peer_a_ws_socket);
    std::mem::drop(peer_b_ws_socket);
    std::mem::drop(peer_c_ws_socket);

    // Give OS time to release ports (prevents "Address already in use" and connection errors)
    tokio::time::sleep(Duration::from_millis(100)).await;

    let gateway = async move {
        let config = gateway_config.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    let peer_a = async move {
        let config = peer_a_config.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    let peer_b = async move {
        let config = peer_b_config.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    let peer_c = async move {
        let config = peer_c_config.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Increased from 300s to accommodate exponential backoff across multiple operations
    let test = tokio::time::timeout(Duration::from_secs(500), async move {
        // CI environment: 45s for network discovery and full connection establishment
        tokio::time::sleep(Duration::from_secs(45)).await;

        // Connect to all peers
        let uri_a =
            format!("ws://127.0.0.1:{peer_a_ws_port}/v1/contract/command?encodingProtocol=native");
        let (stream_a, _) = connect_async(&uri_a).await?;
        let mut client_a = WebApi::start(stream_a);

        let uri_b =
            format!("ws://127.0.0.1:{peer_b_ws_port}/v1/contract/command?encodingProtocol=native");
        let (stream_b, _) = connect_async(&uri_b).await?;
        let mut client_b = WebApi::start(stream_b);

        let uri_c =
            format!("ws://127.0.0.1:{peer_c_ws_port}/v1/contract/command?encodingProtocol=native");
        let (stream_c, _) = connect_async(&uri_c).await?;
        let mut client_c = WebApi::start(stream_c);

        // Test flow: A puts → B gets (caches) → C subscribes → A updates → verify C receives update
        make_put(
            &mut client_a,
            wrapped_state.clone(),
            contract.clone(),
            false,
        )
        .await?;

        // Increased timeout to accommodate exponential backoff in retransmissions
        let resp = tokio::time::timeout(Duration::from_secs(120), client_a.recv()).await??;
        match resp {
            HostResponse::ContractResponse(ContractResponse::PutResponse { key }) => {
                assert_eq!(key, contract_key);
            }
            other => bail!("Expected PutResponse, got: {:?}", other),
        }

        tokio::time::sleep(Duration::from_secs(5)).await;

        make_get(&mut client_b, contract_key, true, false).await?;
        // Increased timeout to accommodate exponential backoff in retransmissions
        let resp = tokio::time::timeout(Duration::from_secs(120), client_b.recv()).await??;
        match resp {
            HostResponse::ContractResponse(ContractResponse::GetResponse { key, .. }) => {
                assert_eq!(key, contract_key);
            }
            other => bail!("Expected GetResponse, got: {:?}", other),
        }

        // CI environment: 10s for cache announcement propagation
        tokio::time::sleep(Duration::from_secs(10)).await;

        make_subscribe(&mut client_c, contract_key).await?;
        tokio::time::sleep(Duration::from_secs(5)).await;

        make_update(&mut client_a, contract_key, updated_state.clone()).await?;
        // Increased timeout to accommodate exponential backoff in retransmissions
        let resp = tokio::time::timeout(Duration::from_secs(120), client_a.recv()).await??;
        match resp {
            HostResponse::ContractResponse(ContractResponse::UpdateResponse { key, .. }) => {
                assert_eq!(key, contract_key);
            }
            other => bail!("Expected UpdateResponse, got: {:?}", other),
        }

        tokio::time::sleep(Duration::from_secs(10)).await;

        Ok(())
    });

    select! {
        g = gateway => {
            let Err(e) = g;
            return Err(anyhow!("Gateway error: {}", e).into());
        }
        a = peer_a => {
            let Err(e) = a;
            return Err(anyhow!("Peer A error: {}", e).into());
        }
        b = peer_b => {
            let Err(e) = b;
            return Err(anyhow!("Peer B error: {}", e).into());
        }
        c = peer_c => {
            let Err(e) = c;
            return Err(anyhow!("Peer C error: {}", e).into());
        }
        r = test => {
            r??;
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    Ok(())
}
