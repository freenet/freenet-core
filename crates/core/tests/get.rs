use anyhow::bail;
use freenet::{
    config::{ConfigArgs, InlineGwConfig, NetworkArgs, WebsocketApiArgs},
    dev_tool::{Location, TransportKeypair},
    local_node::NodeConfig,
    server::serve_gateway,
};
use rand::random;
use std::{
    net::{Ipv4Addr, TcpListener},
    time::Duration,
};

async fn base_test_config(
    is_gateway: bool,
    gateways: Vec<String>,
    public_port: Option<u16>,
) -> anyhow::Result<ConfigArgs> {
    let network_port = if public_port.is_none() {
        (!is_gateway)
            .then(|| Ok::<_, anyhow::Error>(TcpListener::bind("127.0.0.1:0")?.local_addr()?.port()))
            .transpose()?
    } else {
        public_port
    };
    let mut config = ConfigArgs::default();
    config.ws_api = WebsocketApiArgs {
        address: Some(Ipv4Addr::LOCALHOST.into()),
        ws_api_port: Some(50000),
    };
    config.network_api = NetworkArgs {
        public_address: Some(Ipv4Addr::LOCALHOST.into()),
        public_port,
        is_gateway,
        skip_load_from_network: true,
        gateways: Some(gateways),
        location: Some(rand::random()),
        ignore_protocol_checking: true,
        address: Some(Ipv4Addr::LOCALHOST.into()),
        network_port,
    };
    Ok(config)
}

fn gw_config(port: u16) -> InlineGwConfig {
    // generate key and store it in a temp file
    let key = TransportKeypair::new();
    // TODO: store
    InlineGwConfig {
        address: (Ipv4Addr::LOCALHOST, port).into(),
        public_key_path: todo!(),
        location: Some(random()),
    }
}
#[tokio::test]
async fn integration_test_get_contract() -> anyhow::Result<()> {
    let reserved_listener = TcpListener::bind("127.0.0.1:0")?;
    let gw_config = gw_config(reserved_listener.local_addr()?.port());
    let gw_loc = gw_config.location.clone();
    let config_a = base_test_config(false, vec![serde_json::to_string(&gw_config)?], None).await?;
    let node_a = async move {
        let config = config_a.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await?;
        Ok::<(), anyhow::Error>(())
    };

    let config_b = base_test_config(true, vec![], Some(gw_config.address.port())).await?;
    std::mem::drop(reserved_listener); // Free the port so it does not fail on initialization
    let node_b = async {
        let config = config_b.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await?;
        Ok::<(), anyhow::Error>(())
    };

    let test = tokio::time::timeout(Duration::from_secs(1), async {
        // TODO: setup test with a client from freenet_stdlib::client_api::regular
        Ok::<_, anyhow::Error>(())
    });
    tokio::select! {
        a = node_a => {
            bail!(a.unwrap_err());
        }
        b = node_b => {
            bail!(b.unwrap_err());
        }
        r = test => {
            r??;
        }
    }

    Ok(())
}
