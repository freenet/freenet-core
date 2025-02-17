use super::*;
use crate::websocket::convert_websocket_stream;

impl AppState {
    pub async fn new_v1(config: &ExecutorConfig) -> anyhow::Result<Self> {
        let target: SocketAddr = (config.address, config.port).into();
        let stream = tokio_tungstenite::connect_async(&format!(
            "ws://{}/v1/contract/command?encodingProtocol=native",
            target
        ))
        .await
        .map_err(|e| {
            tracing::error!(err=%e);
            anyhow::anyhow!(format!("fail to connect to the host({target}): {e}"))
        })?;

        Ok(AppState {
            local_node: Arc::new(RwLock::new(WebApi::start(convert_websocket_stream(stream.0)))),
            config: config.clone(),
        })
    }
}
