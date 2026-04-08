//! Minimal test to debug connection timing issues

mod common;

use std::{
    net::{SocketAddr, TcpListener},
    time::{Duration, Instant},
};

use freenet::server::serve_client_api;
use futures::FutureExt;
use tokio::{select, time::timeout};
use tracing::{Instrument, Level, span};

use common::{
    allocate_test_node_block, base_node_test_config_with_ip, connect_ws_with_retry,
    gw_config_from_path_with_ip, test_ip_for_node, test_node_config, wait_for_node_connected,
};

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_connection_timing() -> anyhow::Result<()> {
    println!("🔧 Testing connection timing with 2 nodes");

    // Use unique IPs for each node to avoid collisions with parallel tests
    let base_node_idx = allocate_test_node_block(2);
    let gw_ip = test_ip_for_node(base_node_idx);
    let node1_ip = test_ip_for_node(base_node_idx + 1);

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

    // Start both nodes immediately — no startup sleep needed.
    // connect_ws_with_retry polls until the WebSocket server is ready.
    let gateway_future = async {
        let config = config_gw.build().await?;
        let node = test_node_config(config.clone())
            .await?
            .build(serve_client_api(config.ws_api).await?)
            .await?;
        node.run().await
    }
    .instrument(span!(Level::INFO, "gateway"))
    .boxed_local();

    let node1_future = async {
        let config = config_node1.build().await?;
        let node = test_node_config(config.clone())
            .await?
            .build(serve_client_api(config.ws_api).await?)
            .await?;
        node.run().await
    }
    .instrument(span!(Level::INFO, "node1"))
    .boxed_local();

    let test = timeout(Duration::from_secs(60), async {
        // Connect with retry logic — polls until WebSocket server accepts
        let uri_gw =
            format!("ws://{gw_ip}:{ws_api_port_gw}/v1/contract/command?encodingProtocol=native");
        let uri_node1 = format!(
            "ws://{node1_ip}:{ws_api_port_node1}/v1/contract/command?encodingProtocol=native"
        );

        let ws_start = Instant::now();
        let mut _client_gw = connect_ws_with_retry(&uri_gw, "Gateway", 30).await?;
        println!(
            "   ✓ Gateway WebSocket connected in {}ms",
            ws_start.elapsed().as_millis()
        );

        let ws_start = Instant::now();
        let mut client_node1 = connect_ws_with_retry(&uri_node1, "Node1", 30).await?;
        println!(
            "   ✓ Node1 WebSocket connected in {}ms",
            ws_start.elapsed().as_millis()
        );

        // Poll until Node1 has at least 1 ring connection to the gateway
        println!("\n⏳ Waiting for P2P connection establishment...");
        wait_for_node_connected(&mut client_node1, "Node1", 1, 30).await?;
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
