use locutus_node::ClientRequest;
use locutus_runtime::prelude::*;
use locutus_stdlib::prelude::Parameters;

use crate::{state::AppState, Cli, CommandReceiver, DynError};

pub(crate) async fn wasm_runtime(
    config: Cli,
    mut command_receiver: CommandReceiver,
    mut app: AppState,
) -> Result<(), DynError> {
    let tmp_path = std::env::temp_dir().join("locutus");
    let mut contract_store =
        ContractStore::new(tmp_path.join("contracts"), config.max_contract_size);

    let contract = WrappedContract::try_from((&*config.contract, vec![].into()))?;
    contract_store.store_contract(contract)?;
    let mut runtime = Runtime::build(contract_store, false)?;
    loop {
        tokio::select! {
            req = command_receiver.recv() => {
                execute_command(&mut runtime, req.ok_or("channel closed")?, &mut app)?;
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
    app: &mut AppState,
) -> Result<(), DynError> {
    match req {
        ClientRequest::Put {
            contract,
            state,
            parameters,
        } => match runtime.validate_state(contract.key(), parameters, State::from(&*state)) {
            Ok(valid) => app.printout_deser(format!("valid put: {valid}").as_bytes())?,
            Err(err) => {
                println!("error: {err}");
            }
        },
        ClientRequest::Update { key, delta } => {
            let state = app.load_state()?;
            match runtime.update_state(&key, vec![].into(), state.clone(), delta.left().unwrap()) {
                Ok(new_state) => {
                    app.printout_deser(&*state)?;
                    let state = WrappedState::new(new_state.into_owned());
                    app.put(state);
                }
                Err(err) => {
                    println!("error: {err}");
                }
            }
        }
        ClientRequest::Get { key, .. } => {
            let state = app.load_state()?;
            let parameters: Parameters = vec![].into();
            let summary = runtime.summarize_state(&key, parameters.clone(), state.clone())?;
            match runtime.get_state_delta(&key, parameters, state, summary) {
                Ok(delta_output) => {
                    app.printout_deser(&*delta_output)?;
                    println!("finished writing delta result from get");
                }
                Err(err) => {
                    println!("error: {err}");
                }
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}
