use freenet::dev_tool::ClientId;
use freenet_stdlib::client_api::{ClientRequest, ContractRequest, ContractResponse, HostResponse};

use crate::CommandReceiver;

use super::{state::AppState, LocalNodeCliConfig};

pub(super) async fn wasm_runtime(
    _config: LocalNodeCliConfig,
    mut command_receiver: CommandReceiver,
    mut app: AppState,
) -> Result<(), anyhow::Error> {
    loop {
        let req = command_receiver.recv().await;
        let dc = execute_command(
            req.ok_or_else(|| anyhow::anyhow!("channel closed"))?,
            &mut app,
        )
        .await?;
        if dc {
            break;
        }
    }
    Ok(())
}

async fn execute_command(
    req: ClientRequest<'static>,
    app: &mut AppState,
) -> Result<bool, anyhow::Error> {
    let node = &mut *app.local_node.write().await;
    match req {
        ClientRequest::ContractOp(op) => match op {
            req @ ContractRequest::Put { .. } => {
                match node.handle_request(ClientId::FIRST, req.into(), None).await {
                    Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key })) => {
                        println!("valid put for {key}");
                    }
                    Err(err) => {
                        println!("error: {err}");
                    }
                    _ => unreachable!(),
                }
            }
            req @ ContractRequest::Update { .. } => {
                match node.handle_request(ClientId::FIRST, req.into(), None).await {
                    Ok(HostResponse::ContractResponse(ContractResponse::UpdateResponse {
                        key,
                        summary,
                    })) => {
                        println!("valid update for {key}, state summary:");
                        app.printout_deser(summary.as_ref())?;
                    }
                    Err(err) => {
                        println!("error: {err}");
                    }
                    _ => unreachable!(),
                }
            }
            ContractRequest::Get {
                key,
                fetch_contract: contract,
            } => {
                match node
                    .handle_request(
                        ClientId::FIRST,
                        ContractRequest::Get {
                            key: key.clone(),
                            fetch_contract: contract,
                        }
                        .into(),
                        None,
                    )
                    .await
                {
                    Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                        state,
                        ..
                    })) => {
                        println!("current state for {key}:");
                        app.printout_deser(state.as_ref())?;
                    }
                    Err(err) => {
                        println!("error: {err}");
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        },
        _ => {
            tracing::error!("op not supported");
            anyhow::bail!("op not support");
        }
    }
    Ok(false)
}
