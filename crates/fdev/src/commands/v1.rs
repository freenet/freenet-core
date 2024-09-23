use super::*;

pub(super) async fn execute_command(
    request: ClientRequest<'static>,
    other: BaseConfig,
    address: IpAddr,
    port: u16,
) -> anyhow::Result<()> {
    let mode = other.mode;

    let target = match mode {
        OperationMode::Local => {
            if !address.is_loopback() {
                return Err(anyhow::anyhow!(
                    "invalid ip: {address}, expecting a loopback ip address in local mode"
                ));
            }
            SocketAddr::new(address, port)
        }
        OperationMode::Network => SocketAddr::new(address, port),
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

    WebApi::start(stream)
        .send(request)
        .await
        .map_err(Into::into)
}
