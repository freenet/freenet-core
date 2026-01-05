use std::borrow::Cow;

use clap::Parser;
use freenet_stdlib::client_api::ClientRequest;

mod build;
mod commands;
mod config;
mod diagnostics;
mod inspect;
pub(crate) mod network_metrics_server;
mod new_package;
mod query;
mod testing;
mod util;
mod wasm_runtime;

use crate::{
    build::build_package,
    commands::{put, update},
    config::{Config, SubCommand},
    inspect::inspect,
    new_package::create_new_package,
    wasm_runtime::run_local_executor,
};

type CommandReceiver = tokio::sync::mpsc::Receiver<ClientRequest<'static>>;
type CommandSender = tokio::sync::mpsc::Sender<ClientRequest<'static>>;

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("Configuration error: {0}")]
    MissConfiguration(Cow<'static, str>),
    #[error("Command failed: {0}")]
    CommandFailed(&'static str),
}

fn main() -> anyhow::Result<()> {
    let config = Config::parse();
    if !config.sub_command.is_child() {
        freenet::config::set_logger(None, None);
    }

    // Handle deterministic test mode before creating tokio runtime
    // (Turmoil creates its own runtime internally)
    if let SubCommand::Test(ref test_config) = config.sub_command {
        if test_config.deterministic {
            return testing::run_deterministic_test(test_config);
        }
    }

    let tokio_rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    tokio_rt.block_on(async move {
        let cwd = std::env::current_dir()?;
        let r = match config.sub_command {
            SubCommand::WasmRuntime(local_node_config) => {
                run_local_executor(local_node_config).await
            }
            SubCommand::Build(build_tool_config) => build_package(build_tool_config, &cwd),
            SubCommand::Inspect(inspect_config) => inspect(inspect_config),
            SubCommand::Init(init_pckg_config) => create_new_package(init_pckg_config),
            SubCommand::New(new_pckg_config) => create_new_package(new_pckg_config.into()),
            SubCommand::Publish(publish_config) => put(publish_config, config.additional).await,
            SubCommand::Execute(cmd_config) => match cmd_config.command {
                config::NodeCommand::Put(put_config) => put(put_config, config.additional).await,
                config::NodeCommand::Update(update_config) => {
                    update(update_config, config.additional).await
                }
                config::NodeCommand::GetContractId(get_contract_id_config) => {
                    commands::get_contract_id(get_contract_id_config).await
                }
            },
            SubCommand::Test(test_config) => testing::test_framework(test_config).await,
            SubCommand::NetworkMetricsServer(server_config) => {
                let (server, _) = crate::network_metrics_server::start_server(&server_config).await;
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = server => {}
                }
                Ok(())
            }
            SubCommand::Query {} => {
                query::query(config.additional).await?;
                Ok(())
            }
            SubCommand::Diagnostics { contract_keys } => {
                diagnostics::diagnostics(config.additional, contract_keys).await?;
                Ok(())
            }
            SubCommand::GetContractId(get_contract_id_config) => {
                commands::get_contract_id(get_contract_id_config).await
            }
        };
        // todo: make all commands return concrete `thiserror` compatible errors so we can use anyhow
        r.map_err(|e| anyhow::format_err!(e))
    })
}
