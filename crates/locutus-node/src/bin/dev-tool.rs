use clap::Parser;
use locutus_dev::{
    build::build_package,
    commands::{put, update},
    config::{Config, SubCommand},
    inspect::inspect,
    local_node::run_local_node_client,
    new_package::create_new_package,
};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let cwd = std::env::current_dir()?;
    let config = Config::parse();
    match config.sub_command {
        SubCommand::RunLocal(local_node_config) => run_local_node_client(local_node_config).await,
        SubCommand::Build(build_tool_config) => build_package(build_tool_config, &cwd),
        SubCommand::Inspect(inspect_config) => inspect(inspect_config),
        SubCommand::New(new_pckg_config) => create_new_package(new_pckg_config),
        SubCommand::Publish(publish_config) => put(publish_config, config.additional).await,
        SubCommand::Execute(cmd_config) => match cmd_config.command {
            locutus_dev::config::NodeCommand::Put(put_config) => {
                put(put_config, config.additional).await
            }
            locutus_dev::config::NodeCommand::Update(update_config) => {
                update(update_config, config.additional).await
            }
        },
    }
}
