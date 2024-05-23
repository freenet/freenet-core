use std::borrow::Cow;

use clap::Parser;
use freenet_stdlib::client_api::ClientRequest;

mod build;
mod commands;
mod config;
mod inspect;
pub(crate) mod network_metrics_server;
mod new_package;
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

fn main() -> Result<(), anyhow::Error> {
    let tokio_rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let config = Config::parse();
    if !config.sub_command.is_child() {
        freenet::config::set_logger(None);
    }
    tokio_rt.block_on(async move {
        let cwd = std::env::current_dir()?;
        let r = match config.sub_command {
            SubCommand::WasmRuntime(local_node_config) => {
                run_local_executor(local_node_config).await
            }
            SubCommand::Build(build_tool_config) => build_package(build_tool_config, &cwd),
            SubCommand::Inspect(inspect_config) => inspect(inspect_config),
            SubCommand::New(new_pckg_config) => create_new_package(new_pckg_config),
            SubCommand::Publish(publish_config) => put(publish_config, config.additional).await,
            SubCommand::Execute(cmd_config) => match cmd_config.command {
                config::NodeCommand::Put(put_config) => put(put_config, config.additional).await,
                config::NodeCommand::Update(update_config) => {
                    update(update_config, config.additional).await
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
        };
        // todo: make all commands return concrete `thiserror` compatible errors so we can use anyhow
        r.map_err(|e| anyhow::format_err!(e))
    })
}
