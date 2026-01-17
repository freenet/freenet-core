//! Minimal test to debug connection timing issues

mod common;

use std::{
    net::{SocketAddr, TcpListener},
    time::{Duration, Instant},
};

use freenet::{local_node::NodeConfig, server::serve_gateway, test_utils::test_ip_for_node};
use freenet_stdlib::client_api::WebApi;
use futures::FutureExt;
use tokio::{select, time::timeout};
use tracing::{span, Instrument, Level};

use common::{
    base_node_test_config_with_ip, connect_async_with_config, gw_config_from_path_with_ip,
    ws_config,
};

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_connection_timing() -> anyhow::Result<()> {
    println!("🔧 Testing connection timing with 2 nodes");

    // Use unique IPs for each node to ensure unique ring locations
    let gw_ip = test_ip_for_node(0);
    let node1_ip = test_ip_for_node(1);

    // Setup only 2 nodes to minimize complexity
    let network_socket_gw = TcpListener::bind(SocketAddr::new(gw_ip.into(), 0))?;
    let ws_api_port_socket_gw = TcpListener::bind(SocketAddr::new(gw_ip.into(), 0))?;
    let ws_api_port_socket_node1 = TcpListener::bind(SocketAddr::new(node1_ip.into(), 0))?;

    // Configure gateway
    let (config_gw, preset_cfg_gw) = base_node_test_config_with_ip(
        true,
        vec![],
        Some(network_socket_gw.local_addr()?.port()),
        ws_api_port_socket_gw.local_addr()?.port(),
        "gw_timing_test",
        None,
        None,
        Some(gw_ip),
    )
    .await?;
    let public_port = config_gw.network_api.public_port.unwrap();
    let path = preset_cfg_gw.temp_dir.path().to_path_buf();
    let config_info = gw_config_from_path_with_ip(public_port, &path, gw_ip)?;
    let serialized_gateway = serde_json::to_string(&config_info)?;
    let ws_api_port_gw = config_gw.ws_api.ws_api_port.unwrap();

    // Configure Node 1
    let (config_node1, _preset_cfg_node1) = base_node_test_config_with_ip(
        false,
        vec![serialized_gateway],
        None,
        ws_api_port_socket_node1.local_addr()?.port(),
        "node1_timing_test",
        None,
        None,
        Some(node1_ip),
    )
    .await?;
    let ws_api_port_node1 = config_node1.ws_api.ws_api_port.unwrap();

    // Free the sockets
    std::mem::drop(network_socket_gw);
    std::mem::drop(ws_api_port_socket_gw);
    std::mem::drop(ws_api_port_socket_node1);

    // Start nodes
    let gateway_future = async {
        let config = config_gw.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await?)
            .await?;
        node.run().await
    }
    .instrument(span!(Level::INFO, "gateway"))
    .boxed_local();

    let node1_future = async {
        let config = config_node1.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await?)
            .await?;
        node.run().await
    }
    .instrument(span!(Level::INFO, "node1"))
    .boxed_local();

    let test = timeout(Duration::from_secs(60), async {
        println!("⏳ Waiting for nodes to start (10s)...");
        tokio::time::sleep(Duration::from_secs(10)).await;

        // Simple ping test - just establish websocket connections
        println!("📡 Testing WebSocket connections...");

        let ws_start = Instant::now();
        let uri_gw =
            format!("ws://{gw_ip}:{ws_api_port_gw}/v1/contract/command?encodingProtocol=native");
        let (stream_gw, _) = connect_async_with_config(&uri_gw, Some(ws_config()), false).await?;
        let _client_gw = WebApi::start(stream_gw);
        println!(
            "   ✓ Gateway WebSocket connected in {}ms",
            ws_start.elapsed().as_millis()
        );

        let ws_start = Instant::now();
        let uri_node1 = format!(
            "ws://{node1_ip}:{ws_api_port_node1}/v1/contract/command?encodingProtocol=native"
        );
        let (stream_node1, _) =
            connect_async_with_config(&uri_node1, Some(ws_config()), false).await?;
        let _client_node1 = WebApi::start(stream_node1);
        println!(
            "   ✓ Node1 WebSocket connected in {}ms",
            ws_start.elapsed().as_millis()
        );

        // Now wait and watch for UDP connection attempts
        println!("\n⏳ Waiting 20s to observe P2P connection establishment...");
        tokio::time::sleep(Duration::from_secs(20)).await;

        println!("✅ Test completed - check logs for connection timing");
        Ok::<_, anyhow::Error>(())
    })
    .instrument(span!(Level::INFO, "connection_timing_test"));

    select! {
        r = gateway_future => {
            r?;
            anyhow::bail!("Gateway stopped unexpectedly");
        }
        r = node1_future => {
            r?;
            anyhow::bail!("Node1 stopped unexpectedly");
        }
        r = test => {
            r??;
        }
    }

    Ok(())
}
