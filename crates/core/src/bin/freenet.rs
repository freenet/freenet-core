use clap::Parser;
use freenet::{
    config::{Config, ConfigArgs},
    local_node::{Executor, OperationMode},
    server::{local_node::run_local_node, network_node::run_network_node},
};
use std::net::SocketAddr;

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

async fn run(config: Config) -> Result<(), DynError> {
    match config.mode {
        OperationMode::Local => run_local(config).await,
        OperationMode::Network => run_network(config).await,
    }
}

async fn run_local(config: Config) -> Result<(), DynError> {
    let port = config.gateway.port;
    let ip = config.gateway.address;
    let executor = Executor::from_config(&config, None).await?;
    let socket: SocketAddr = (ip, port).into();
    run_local_node(executor, socket).await
}

async fn run_network(config: Config) -> Result<(), DynError> {
    let port = config.gateway.port;
    let ip = config.gateway.address;
    run_network_node(config, (ip, port).into()).await
}

fn main() -> Result<(), DynError> {
    freenet::config::set_logger(None);
    let config = ConfigArgs::parse().build()?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(run(config))?;
    Ok(())
}
