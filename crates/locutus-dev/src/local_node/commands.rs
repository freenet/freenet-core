use locutus_core::{ClientId, ClientRequest, HostResponse};

use crate::{config::LocalNodeCliConfig, CommandReceiver, DynError};

use super::state::AppState;

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

#[allow(unused, clippy::diverging_sub_expression)]
async fn execute_command(req: ClientRequest, app: &mut AppState) -> Result<bool, DynError> {
    let node = &mut *app.local_node.write().await;
    match req {
        req @ ClientRequest::Put { .. } => {
            match node.handle_request(ClientId::FIRST, req, None).await {
                Ok(HostResponse::PutResponse { key }) => {
                    println!("valid put for {key}");
                }
                Err(err) => {
                    println!("error: {err}");
                }
                _ => unreachable!(),
            }
        }
        req @ ClientRequest::Update { .. } => {
            match node.handle_request(ClientId::FIRST, req, None).await {
                Ok(HostResponse::UpdateResponse { key, summary }) => {
                    println!("valid update for {key}, state summary:");
                    app.printout_deser(summary.as_ref())?;
                }
                Err(err) => {
                    println!("error: {err}");
                }
                _ => unreachable!(),
            }
        }
        ClientRequest::Get {
            key,
            fetch_contract: contract,
        } => {
            match node
                .handle_request(
                    ClientId::FIRST,
                    ClientRequest::Get {
                        key,
                        fetch_contract: contract,
                    },
                    None,
                )
                .await
            {
                Ok(HostResponse::GetResponse {
                    contract, state, ..
                }) => {
                    println!("current state for {key}:");
                    app.printout_deser(state.as_ref())?;
                }
                Err(err) => {
                    println!("error: {err}");
                }
                _ => unreachable!(),
            }
        }
        ClientRequest::Disconnect { .. } => return Ok(true),
        _ => unreachable!(),
    }
    Ok(false)
}
