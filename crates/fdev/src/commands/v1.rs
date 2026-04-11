use super::*;

pub(super) async fn start_api_client(cfg: BaseConfig) -> anyhow::Result<WebApi> {
    let url = if let Some(node_url) = &cfg.node_url {
        node_url.clone()
    } else {
        let mode = cfg.mode;
        let address = cfg.address;
        let target = match mode {
            OperationMode::Local => {
                if !address.is_loopback() {
                    return Err(anyhow::anyhow!(
                        "invalid ip: {address}, expecting a loopback ip address in local mode"
                    ));
                }
                SocketAddr::new(address, cfg.port)
            }
            OperationMode::Network => SocketAddr::new(address, cfg.port),
        };
        format!("ws://{target}/v1/contract/command?encodingProtocol=native")
    };

    let (stream, _) = tokio_tungstenite::connect_async(&url).await.map_err(|e| {
        tracing::error!(err=%e);
        anyhow::anyhow!("fail to connect to the host({url}): {e}")
    })?;

    Ok(WebApi::start(stream))
}

pub(super) async fn execute_command(
    request: ClientRequest<'static>,
    api_client: &mut WebApi,
) -> anyhow::Result<()> {
    api_client.send(request).await?;
    Ok(())
}
