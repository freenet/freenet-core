use locutus_node::{ClientRequest, HostResponse};

use crate::{config::Config, state::AppState, CommandReceiver, DynError};

pub async fn wasm_runtime(
    _config: Config,
    mut command_receiver: CommandReceiver,
    mut app: AppState,
) -> Result<(), DynError> {
    loop {
        tokio::select! {
            req = command_receiver.recv() => {
                execute_command(req.ok_or("channel closed")?, &mut app)?;
            }
            interrupt = tokio::signal::ctrl_c() => {
                interrupt?;
                break;
            }
        }
    }
    Ok(())
}

#[allow(unused, clippy::diverging_sub_expression)]
fn execute_command(req: ClientRequest, app: &mut AppState) -> Result<(), DynError> {
    let node = &mut *app.local_node.write();
    match req {
        req @ ClientRequest::Put { .. } => match node.handle_request(req) {
            Ok(HostResponse::PutResponse(key)) => {
                println!("valid put for {key}");
            }
            Err(err) => {
                println!("error: {err}");
            }
            _ => unreachable!(),
        },
        req @ ClientRequest::Update { .. } => match node.handle_request(req) {
            Ok(HostResponse::UpdateResponse { key, summary }) => {
                println!("valid update for {key}, state summary:");
                app.printout_deser(summary.as_ref())?;
            }
            Err(err) => {
                println!("error: {err}");
            }
            _ => unreachable!(),
        },
        ClientRequest::Get { key, contract } => {
            match node.handle_request(ClientRequest::Get { key, contract }) {
                Ok(HostResponse::GetResponse { contract, state }) => {
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
    }
    Ok(())
}
