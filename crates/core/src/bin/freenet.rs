use anyhow::Context;
use clap::Parser;
use freenet::{
    config::{Config, ConfigArgs},
    dev_tool::NodeConfig,
    local_node::{Executor, OperationMode},
    server::{local_node::run_local_node, network_node::run_network_node},
};
use std::{net::SocketAddr, sync::Arc};

async fn run(config: Config) -> anyhow::Result<()> {
    match config.mode {
        OperationMode::Local => run_local(config).await,
        OperationMode::Network => run_network(config).await,
    }
}

async fn run_local(config: Config) -> anyhow::Result<()> {
    tracing::info!("Starting freenet node in local mode");
    let port = config.ws_api.port;
    let ip = config.ws_api.address;
    let executor = Executor::from_config(Arc::new(config), None)
        .await
        .map_err(anyhow::Error::msg)?;

    let socket: SocketAddr = (ip, port).into();
    run_local_node(executor, socket)
        .await
        .map_err(anyhow::Error::msg)
}

async fn run_network(config: Config) -> anyhow::Result<()> {
    tracing::info!("Starting freenet node in network mode");

    let node_config = NodeConfig::new(config)
        .await
        .with_context(|| "failed while loading node config")?;
    run_network_node(node_config).await
}

fn main() -> anyhow::Result<()> {
    freenet::config::set_logger(None);
    let config = ConfigArgs::parse().build()?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(
            std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1),
        )
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(run(config))?;
    Ok(())
}
