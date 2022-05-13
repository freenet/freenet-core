use locutus_node::{ClientRequest, HostResponse};
use locutus_runtime::prelude::*;

use crate::{config::Config, state::AppState, CommandReceiver, DynError};

pub async fn wasm_runtime(
    config: Config,
    mut command_receiver: CommandReceiver,
    mut app: AppState,
) -> Result<(), DynError> {
    let tmp_path = std::env::temp_dir().join("locutus");
    let mut contract_store =
        ContractStore::new(tmp_path.join("contracts"), config.max_contract_size);

    let contract = WrappedContract::try_from((&*config.contract, vec![].into()))?;
    contract_store.store_contract(contract)?;
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
            _ => unimplemented!(),
        },
        req @ ClientRequest::Update { .. } => match node.handle_request(req) {
            Ok(HostResponse::UpdateResponse { key, summary }) => {
                println!("valid update for {key}, state summary:");
                app.printout_deser(summary.as_ref())?;
            }
            Err(err) => {
                println!("error: {err}");
            }
            _ => unimplemented!(),
        },
        ClientRequest::Get { key, contract } => {
            match node.handle_request(ClientRequest::Get { key, contract }) {
                Ok(HostResponse::GetResponse { contract, state }) => {
                    println!("valid update for {key}, state:");
                    app.printout_deser(state.as_ref())?;
                }
                Err(err) => {
                    println!("error: {err}");
                }
                _ => unimplemented!(),
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}
