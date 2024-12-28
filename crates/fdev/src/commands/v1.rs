use super::*;

pub(super) async fn start_api_client(cfg: BaseConfig) -> anyhow::Result<WebApi> {
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

    let (stream, _) = tokio_tungstenite::connect_async(&format!(
        "ws://{}/v1/contract/command?encodingProtocol=native",
        target
    ))
    .await
    .map_err(|e| {
        tracing::error!(err=%e);
        anyhow::anyhow!(format!("fail to connect to the host({target}): {e}"))
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
