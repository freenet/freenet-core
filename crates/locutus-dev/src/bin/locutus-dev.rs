use clap::Parser;
use locutus_dev::{
    build_tool::build_package,
    config::{Config, SubCommand},
    local_node::run_local_node_client,
    new_pckg::create_new_package,
};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let cwd = std::env::current_dir()?;
    match Config::parse().sub_command {
        SubCommand::RunLocal(local_node_config) => run_local_node_client(local_node_config).await,
        SubCommand::Build(build_tool_config) => build_package(build_tool_config, &cwd),
        SubCommand::New(new_pckg_config) => create_new_package(new_pckg_config),
    }
}
