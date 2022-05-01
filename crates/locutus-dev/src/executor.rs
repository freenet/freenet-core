use std::{fs::File, io::Write};

use locutus_node::ClientRequest;
use locutus_runtime::prelude::*;

use crate::{Cli, CommandReceiver, DeserializationFmt};

pub(crate) async fn wasm_runtime(
    config: Cli,
    mut command_receiver: CommandReceiver,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let tmp_path = std::env::temp_dir().join("locutus");
    let contract_store = ContractStore::new(tmp_path.join("contracts"), config.max_contract_size);
    let mut runtime = Runtime::build(contract_store, false)?;
    loop {
        tokio::select! {
            req = command_receiver.recv() => {
                execute_command(&mut runtime, req.ok_or("channel closed")?, &config)?;
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
    config: &Cli,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    match req {
        ClientRequest::Put { contract, state } => {
            let parameters = todo!();
            let delta = todo!();
            match runtime.update_state(&contract.key(), parameters, (&*state).into(), delta) {
                Ok(new_state) => printout_deser(config, &*state)?,
                Err(err) => {
                    println!("error: {err}");
                }
            }
        }
        ClientRequest::Get { key, .. } => {
            let parameters = todo!();
            let state = todo!();
            let summary = todo!();
            match runtime.get_state_delta(&key, parameters, state, summary) {
                Ok(delta_output) => printout_deser(config, &*delta_output)?,
                Err(err) => {
                    println!("error: {err}");
                }
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}

fn printout_deser<R: AsRef<[u8]> + ?Sized>(config: &Cli, data: &R) -> Result<(), std::io::Error> {
    fn write_res(config: &Cli, pprinted: &str) -> Result<(), std::io::Error> {
        if let Some(p) = &config.output_file {
            let mut f = File::create(p)?;
            f.write_all(pprinted.as_bytes())?;
        } else if config.terminal_output {
            println!("{pprinted}");
        }
        Ok(())
    }

    #[cfg(feature = "json")]
    {
        if let Some(DeserializationFmt::Json) = config.deser_format {
            let deser: serde_json::Value = serde_json::from_slice(data.as_ref())?;
            let pp = serde_json::to_string_pretty(&deser)?;
            write_res(config, &*pp)?;
        }
    }
    #[cfg(feature = "messagepack")]
    {
        if let Some(DeserializationFmt::MessagePack) = &config.deser_format {
            let deser = rmpv::decode::read_value(&mut data.as_ref())
                .map_err(|_err| std::io::ErrorKind::InvalidData)?;
            let pp = format!("{deser}");
            write_res(config, &*pp)?;
        }
    }
    Ok(())
}
