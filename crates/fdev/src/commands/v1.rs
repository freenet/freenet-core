use freenet_stdlib::client_api::{ContractResponse, HostResponse};

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
        "ws://{target}/v1/contract/command?encodingProtocol=native"
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

    // Wait for the server's response before closing the connection
    let response = api_client
        .recv()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to receive response: {e}"))?;

    match response {
        HostResponse::ContractResponse(contract_response) => match contract_response {
            ContractResponse::PutResponse { key } => {
                tracing::info!(%key, "Contract published successfully");
                Ok(())
            }
            ContractResponse::UpdateResponse { key, summary } => {
                tracing::info!(%key, ?summary, "Contract updated successfully");
                Ok(())
            }
            other => {
                tracing::warn!(?other, "Unexpected contract response");
                Ok(())
            }
        },
        HostResponse::DelegateResponse { key, values } => {
            tracing::info!(%key, response_count = values.len(), "Delegate registered successfully");
            Ok(())
        }
        HostResponse::Ok => {
            tracing::info!("Operation completed successfully");
            Ok(())
        }
        HostResponse::QueryResponse(query_response) => {
            tracing::info!(?query_response, "Query response received");
            Ok(())
        }
        _ => {
            tracing::warn!(?response, "Unexpected response type");
            Ok(())
        }
    }
}
