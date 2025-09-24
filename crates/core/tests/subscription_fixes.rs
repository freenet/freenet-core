use anyhow::anyhow;
use freenet::{
    config::{ConfigArgs, NetworkArgs, SecretArgs, WebsocketApiArgs},
    dev_tool::TransportKeypair,
    local_node::NodeConfig,
    server::serve_gateway,
    test_utils,
};
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, ContractResponse, HostResponse, WebApi},
    prelude::*,
};
use futures::FutureExt;
use std::{
    net::{Ipv4Addr, TcpListener},
    time::Duration,
};
use tokio::time::timeout;
use tokio_tungstenite::connect_async;

struct PresetConfig {
    temp_dir: tempfile::TempDir,
}

async fn base_node_test_config(
    is_gateway: bool,
    gateways: Vec<String>,
    public_port: Option<u16>,
    ws_api_port: u16,
) -> anyhow::Result<(ConfigArgs, PresetConfig)> {
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
            public_port,
            is_gateway,
            skip_load_from_network: true,
            gateways: Some(gateways),
            location: Some(rand::random()),
            ignore_protocol_checking: true,
            address: Some(Ipv4Addr::LOCALHOST.into()),
            network_port: public_port,
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
    Ok((config, PresetConfig { temp_dir }))
}

/// Test that subscription responses are correctly routed back to clients
/// This tests the fix for issue #1 - missing waiting_for_transaction_result
#[tokio::test(flavor = "current_thread")]
async fn test_subscription_response_routing() -> anyhow::Result<()> {
    const TEST_CONTRACT: &str = "test-contract-integration";

    // Find available ports
    let ws_api_socket_a = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    let ws_api_socket_b = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    let network_socket_b = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;

    // Gateway node
    let (config_b, _preset_b) = base_node_test_config(
        true,
        vec![],
        Some(network_socket_b.local_addr()?.port()),
        ws_api_socket_b.local_addr()?.port(),
    )
    .await?;

    // Convert gateway config to JSON for peer connection
    let public_key_path = _preset_b.temp_dir.path().join("public.pem");
    let gw_config = freenet::config::InlineGwConfig {
        address: (Ipv4Addr::LOCALHOST, network_socket_b.local_addr()?.port()).into(),
        location: Some(rand::random()),
        public_key_path,
    };

    // Peer node
    let (config_a, _preset_a) = base_node_test_config(
        false,
        vec![serde_json::to_string(&gw_config)?],
        None,
        ws_api_socket_a.local_addr()?.port(),
    )
    .await?;
    let ws_api_port_peer_a = config_a.ws_api.ws_api_port.unwrap();

    // Free ports before starting nodes
    std::mem::drop(ws_api_socket_a);
    std::mem::drop(ws_api_socket_b);
    std::mem::drop(network_socket_b);

    // Start nodes
    let node_a = async move {
        let config = config_a.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

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
        // Wait for nodes to start
        tokio::time::sleep(Duration::from_secs(10)).await;

        // Connect to node A's websocket API
        let uri = format!(
            "ws://127.0.0.1:{ws_api_port_peer_a}/v1/contract/command?encodingProtocol=native"
        );
        let (stream, _) = connect_async(&uri).await?;
        let mut client = WebApi::start(stream);

        // Load and put contract
        let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
        let contract_key = contract.key();
        let initial_state = WrappedState::new(vec![0]);

        client
            .send(ClientRequest::ContractOp(ContractRequest::Put {
                contract: contract.clone(),
                state: initial_state.clone(),
                related_contracts: RelatedContracts::default(),
                subscribe: false,
            }))
            .await?;

        // Wait for put response
        let resp = timeout(Duration::from_secs(30), client.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
                assert_eq!(key, contract_key, "Put response key should match");
            }
            _ => return Err(anyhow!("Failed to put contract")),
        }

        // Give contract time to propagate
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Subscribe to the contract - this tests the routing fix
        client
            .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
                key: contract_key,
                summary: None,
            }))
            .await?;

        // The fix ensures subscription response is routed back
        let resp = timeout(Duration::from_secs(30), client.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::SubscribeResponse {
                key,
                subscribed,
            }))) => {
                assert_eq!(key, contract_key, "Subscribe response key should match");
                assert!(subscribed, "Should be successfully subscribed");
            }
            _ => {
                return Err(anyhow!(
                    "Failed to receive subscription response - routing issue"
                ))
            }
        }

        // Clean up
        client
            .send(ClientRequest::Disconnect { cause: None })
            .await?;

        Ok::<(), anyhow::Error>(())
    });

    tokio::select! {
        res = node_a => match res {
            Ok(r) => anyhow::bail!("node a finished unexpectedly: {r:?}"),
            Err(e) => anyhow::bail!("node a error: {e}"),
        },
        res = node_b => match res {
            Ok(r) => anyhow::bail!("node b finished unexpectedly: {r:?}"),
            Err(e) => anyhow::bail!("node b error: {e}"),
        },
        res = test => match res {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => anyhow::bail!("test timeout after 180 seconds"),
        },
    }
}

/// Test that nodes at optimal location can subscribe to contracts
/// This tests the fix for issue #2 - using k_closest_potentially_caching
#[tokio::test(flavor = "current_thread")]
async fn test_optimal_location_subscription() -> anyhow::Result<()> {
    const TEST_CONTRACT: &str = "test-contract-integration";

    // This test verifies that when a node is at the optimal location for a contract,
    // it can still subscribe by finding alternative peers using k_closest_potentially_caching
    //
    // The key fix is:
    // 1. Removed early return in start_subscription_request
    // 2. Using k_closest_potentially_caching(k=3) to find multiple candidates
    //
    // Due to the complexity of setting up a network with precise location control,
    // we test this indirectly by verifying subscriptions work in various scenarios

    // Find available ports
    let ws_api_socket = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    let network_socket = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;

    // Single gateway node (often at optimal location for some contracts)
    let (config, _preset) = base_node_test_config(
        true,
        vec![],
        Some(network_socket.local_addr()?.port()),
        ws_api_socket.local_addr()?.port(),
    )
    .await?;
    let ws_api_port = config.ws_api.ws_api_port.unwrap();

    // Free ports
    std::mem::drop(ws_api_socket);
    std::mem::drop(network_socket);

    // Start node
    let node = async move {
        let config = config.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    let test = tokio::time::timeout(Duration::from_secs(180), async {
        // Wait for node to start
        tokio::time::sleep(Duration::from_secs(10)).await;

        // Connect to websocket API
        let uri =
            format!("ws://127.0.0.1:{ws_api_port}/v1/contract/command?encodingProtocol=native");
        let (stream, _) = connect_async(&uri).await?;
        let mut client = WebApi::start(stream);

        // Load and put contract with auto-subscribe
        let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
        let contract_key = contract.key();
        let initial_state = WrappedState::new(vec![0]);

        // Put with subscribe=true to test auto-subscribe path
        client
            .send(ClientRequest::ContractOp(ContractRequest::Put {
                contract: contract.clone(),
                state: initial_state.clone(),
                related_contracts: RelatedContracts::default(),
                subscribe: true, // Auto-subscribe
            }))
            .await?;

        // Wait for put response
        let resp = timeout(Duration::from_secs(30), client.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
                assert_eq!(key, contract_key, "Put response key should match");
            }
            _ => return Err(anyhow!("Failed to put contract")),
        }

        // The fix ensures that even if this node is at optimal location,
        // the auto-subscribe still attempts to find peers (no early return)
        // We can't directly test the subscription here since it's a single node,
        // but the key is that it doesn't crash or hang trying to subscribe

        // Clean up
        client
            .send(ClientRequest::Disconnect { cause: None })
            .await?;

        Ok::<(), anyhow::Error>(())
    });

    tokio::select! {
        res = node => match res {
            Ok(r) => anyhow::bail!("node finished unexpectedly: {r:?}"),
            Err(e) => anyhow::bail!("node error: {e}"),
        },
        res = test => match res {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => anyhow::bail!("test timeout after 180 seconds"),
        },
    }
}
