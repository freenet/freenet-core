use std::borrow::Cow;

use clap::Parser;
use freenet_stdlib::client_api::ClientRequest;

mod build;
mod commands;
mod config;
mod inspect;
mod local_node;
mod new_package;
mod util;

use crate::{
    build::build_package,
    commands::{put, update},
    config::{Config, SubCommand},
    inspect::inspect,
    local_node::run_local_node_client,
    new_package::create_new_package,
};

type CommandReceiver = tokio::sync::mpsc::Receiver<ClientRequest<'static>>;
type CommandSender = tokio::sync::mpsc::Sender<ClientRequest<'static>>;
type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("Configuration error: {0}")]
    MissConfiguration(Cow<'static, str>),
    #[error("Command failed: {0}")]
    CommandFailed(&'static str),
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    freenet::config::set_logger();
    let cwd = std::env::current_dir()?;
    let config = Config::parse();
    freenet::config::Config::set_op_mode(config.additional.mode);
    let r = match config.sub_command {
        SubCommand::RunLocal(local_node_config) => run_local_node_client(local_node_config).await,
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
    };
    // todo: make all commands return concrete `thiserror` compatible errors so we can use anyhow
    r.map_err(|e| anyhow::format_err!(e))
}
