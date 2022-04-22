use locutus_node::ClientRequest;
use locutus_runtime::prelude::*;

use crate::CommandReceiver;

const DEFAULT_MAX_SIZE: i64 = 50 * 1024 * 1024;

pub async fn wasm_runtime(
    mut command_receiver: CommandReceiver,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let tmp_path = std::env::temp_dir().join("locutus");
    let contract_max_size = std::env::var("LOCUTUS_MAX_CONTRACT_SIZE")
        .map(|s| s.parse())
        .ok()
        .transpose()?
        .unwrap_or(DEFAULT_MAX_SIZE);
    let contract_store = ContractStore::new(tmp_path.join("contracts"), contract_max_size);
    let mut runtime = Runtime::build(contract_store, false)?;
    loop {
        tokio::select! {
            req = command_receiver.recv() => {
                execute_command(&mut runtime, req.ok_or("channel closed")?)?;
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
fn execute_command(
    runtime: &mut Runtime,
    req: ClientRequest,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    match req {
        ClientRequest::Put { contract, state } => {
            let parameters = todo!();
            let delta = todo!();
            runtime.update_state(&contract.key(), parameters, (&*state).into(), delta)?;
        }
        ClientRequest::Get { key, .. } => {
            let parameters = todo!();
            let state = todo!();
            let summary = todo!();
            runtime.get_state_delta(&key, parameters, state, summary)?;
        }
        _ => unreachable!(),
    }
    Ok(())
}
