use locutus_core::{ClientId, ClientRequest, ContractRequest, ContractResponse, HostResponse};

use crate::{CommandReceiver, DynError};

use super::{state::AppState, LocalNodeCliConfig};

pub(super) async fn wasm_runtime(
    _config: LocalNodeCliConfig,
    mut command_receiver: CommandReceiver,
    mut app: AppState,
) -> Result<(), DynError> {
    loop {
        let req = command_receiver.recv().await;
        let dc = execute_command(req.ok_or("channel closed")?, &mut app).await?;
        if dc {
            break;
        }
    }
    Ok(())
}

async fn execute_command(
    req: ClientRequest<'static>,
    app: &mut AppState,
) -> Result<bool, DynError> {
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
                        todo!()
                    }
                    Err(err) => {
                        println!("error: {err}");
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        },
        ClientRequest::ComponentOp(op) => {
            match node.handle_request(ClientId::FIRST, op.into(), None).await {
                Ok(_res) => todo!(),
                Err(either::Either::Left(err)) => {
                    println!("error: {err}");
                }
                Err(either::Either::Right(err)) => {
                    println!("error: {err}");
                }
            }
        }
        ClientRequest::Disconnect { .. } => return Ok(true),
    }
    Ok(false)
}
