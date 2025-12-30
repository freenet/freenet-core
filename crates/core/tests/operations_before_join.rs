//! Tests for operations attempted before peer join completes.
//!
//! Verifies that PUT, GET, and SUBSCRIBE operations return PEER_NOT_JOINED error
//! when the peer hasn't joined the network yet.

use freenet::{
    local_node::NodeConfig,
    server::serve_gateway,
    test_utils::{load_contract, make_get, make_put, make_subscribe},
};
use freenet_stdlib::{client_api::WebApi, prelude::*};
use futures::FutureExt;
use std::{
    net::{Ipv4Addr, TcpListener},
    sync::{LazyLock, Mutex},
    time::Duration,
};
use tokio::{select, time::timeout};
use tokio_tungstenite::connect_async;
use tracing::{error, info};

static RNG: LazyLock<Mutex<rand::rngs::StdRng>> = LazyLock::new(|| {
    use rand::SeedableRng;
    Mutex::new(rand::rngs::StdRng::from_seed(
        *b"ops_before_join_test_seed_12345!",
    ))
});

async fn expect_peer_not_joined(client: &mut WebApi, op_name: &str) -> anyhow::Result<()> {
    let response = timeout(Duration::from_secs(10), client.recv()).await;

    let error_msg = match response {
        Ok(Err(client_error)) => format!("{:?}", client_error),
        Ok(Ok(resp)) => {
            return Err(anyhow::anyhow!(
                "{} unexpectedly succeeded before join: {:?}",
                op_name,
                resp
            ));
        }
        Err(_) => return Err(anyhow::anyhow!("{}: timeout - no response", op_name)),
    };

    assert!(
        error_msg.contains("PEER_NOT_JOINED"),
        "{}: expected PEER_NOT_JOINED, got: {}",
        op_name,
        error_msg
    );
    info!("{}: got PEER_NOT_JOINED as expected", op_name);
    Ok(())
}

async fn connect_to_ws(port: u16) -> anyhow::Result<WebApi> {
    let url = format!(
        "ws://localhost:{}/v1/contract/command?encodingProtocol=native",
        port
    );

    for attempt in 0..10 {
        match connect_async(&url).await {
            Ok((ws_stream, _)) => return Ok(WebApi::start(ws_stream)),
            Err(_) if attempt < 9 => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            Err(e) => return Err(anyhow::anyhow!("Failed to connect: {}", e)),
        }
    }
    unreachable!()
}

#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn test_operations_blocked_before_join() -> anyhow::Result<()> {
    let gateway_network_socket = TcpListener::bind("127.0.0.1:0")?;
    let gateway_ws_socket = TcpListener::bind("127.0.0.1:0")?;
    let peer_ws_socket = TcpListener::bind("127.0.0.1:0")?;

    let gateway_port = gateway_network_socket.local_addr()?.port();
    let gateway_ws_port = gateway_ws_socket.local_addr()?.port();
    let peer_ws_port = peer_ws_socket.local_addr()?.port();

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

    std::mem::drop(gateway_network_socket);
    std::mem::drop(gateway_ws_socket);
    std::mem::drop(peer_ws_socket);

    let (start_gateway_tx, start_gateway_rx) = tokio::sync::oneshot::channel::<()>();

    let peer_cfg = peer_config.build().await?;
    let peer_ws_server = serve_gateway(peer_cfg.ws_api).await?;
    let peer_node = NodeConfig::new(peer_cfg.clone())
        .await?
        .build(peer_ws_server)
        .await?;

    let peer = async move { peer_node.run().await }.boxed_local();

    let gw_config = gateway_config.build().await?;
    let gateway = async move {
        let _ = start_gateway_rx.await;
        info!("Starting gateway...");

        let node = NodeConfig::new(gw_config.clone())
            .await?
            .build(serve_gateway(gw_config.ws_api).await?)
            .await?;
        node.run().await
    }
    .boxed_local();

    let test = tokio::time::timeout(Duration::from_secs(90), async move {
        tokio::time::sleep(Duration::from_millis(200)).await;

        let mut client = connect_to_ws(peer_ws_port).await?;
        info!("Connected to peer (gateway NOT started yet)");

        let contract = load_contract("test-contract-integration", vec![].into())?;
        let key = contract.key();
        let state = WrappedState::from(freenet::test_utils::create_empty_todo_list());

        info!("Testing operations before join...");

        make_put(&mut client, state.clone(), contract.clone(), false).await?;
        expect_peer_not_joined(&mut client, "PUT").await?;

        make_get(&mut client, key, false, false).await?;
        expect_peer_not_joined(&mut client, "GET").await?;

        make_subscribe(&mut client, key).await?;
        expect_peer_not_joined(&mut client, "SUBSCRIBE").await?;

        info!("All operations correctly rejected before join");

        info!("Starting gateway...");
        let _ = start_gateway_tx.send(());

        info!("Waiting for peer to join network...");
        tokio::time::sleep(Duration::from_secs(15)).await;

        info!("Sending PUT after join...");
        let contract2 = load_contract("test-contract-integration", vec![1].into())?;
        make_put(&mut client, state, contract2, false).await?;

        let response = timeout(Duration::from_secs(30), client.recv()).await;

        match response {
            Ok(Ok(_)) => info!("PUT succeeded after join"),
            Ok(Err(e)) => {
                let err_str = format!("{:?}", e);
                if err_str.contains("PEER_NOT_JOINED") {
                    return Err(anyhow::anyhow!(
                        "Still getting PEER_NOT_JOINED after 15s wait - join may have failed"
                    ));
                }
                return Err(anyhow::anyhow!("Unexpected error after join: {}", err_str));
            }
            Err(_) => return Err(anyhow::anyhow!("Timeout waiting for PUT response")),
        }

        info!("Test completed successfully");
        Ok::<(), anyhow::Error>(())
    });

    select! {
        _ = gateway => {
            error!("Gateway exited unexpectedly");
            Ok(())
        }
        _ = peer => {
            error!("Peer exited unexpectedly");
            Ok(())
        }
        result = test => {
            result??;
            Ok(())
        }
    }
}
