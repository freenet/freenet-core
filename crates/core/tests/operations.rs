use anyhow::bail;
use freenet::{
    config::{ConfigArgs, InlineGwConfig, NetworkArgs, WebsocketApiArgs},
    dev_tool::TransportKeypair,
    local_node::NodeConfig,
    server::serve_gateway,
};
use rand::random;
use std::{
    net::{Ipv4Addr, TcpListener},
    path::Path,
    time::Duration,
};

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
            location: Some(rand::random()),
            ignore_protocol_checking: true,
            address: Some(Ipv4Addr::LOCALHOST.into()),
            network_port,
        },
        ..Default::default()
    };
    Ok(config)
}

fn gw_config(port: u16, path: &Path) -> anyhow::Result<(InlineGwConfig, TransportKeypair)> {
    // generate key and store it in a temp file
    let key = TransportKeypair::new();
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

#[tokio::test]
async fn test_get_contract() -> anyhow::Result<()> {
    let node_b_tmp_dir = tempfile::tempdir()?;
    let node_b_pub_key = node_b_tmp_dir.path().join("pub_key.pem");
    let reserved_listener = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_a = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_b = TcpListener::bind("127.0.0.1:0")?;
    let (gw_config, gw_keypair) =
        gw_config(reserved_listener.local_addr()?.port(), &node_b_pub_key)?;
    let gw_loc = gw_config.location;

    let node_a_tmp_dir = tempfile::tempdir()?;
    let mut config_a = base_test_config(
        false,
        vec![serde_json::to_string(&gw_config)?],
        None,
        ws_api_port_a.local_addr()?.port(),
    )
    .await?;
    config_a.config_paths.config_dir = Some(node_a_tmp_dir.path().to_path_buf());
    config_a.config_paths.data_dir = Some(node_a_tmp_dir.path().to_path_buf());

    std::mem::drop(ws_api_port_a); // Free the port so it does not fail on initialization
    let node_a = async move {
        let config = config_a.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    };

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
    };

    let test = tokio::time::timeout(Duration::from_secs(1), async {
        // TODO: setup test with a client from freenet_stdlib::client_api::regular
        tokio::time::sleep(Duration::from_secs(10)).await;
        Ok::<_, anyhow::Error>(())
    });
    tokio::select! {
        a = node_a => {
            let Err(a) = a;
            bail!(a);
        }
        b = node_b => {
            let Err(b) = b;
            bail!(b);
        }
        r = test => {
            r??;
        }
    }

    Ok(())
}
