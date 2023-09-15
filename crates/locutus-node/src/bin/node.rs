use clap::Parser;
use locutus_core::NodeConfig;
use locutus_core::{Executor, OperationMode};
use std::net::SocketAddr;
use tracing::metadata::LevelFilter;
use tracing_subscriber::EnvFilter;

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

async fn run(config: NodeConfig) -> Result<(), DynError> {
    match config.mode {
        OperationMode::Local => run_local(config).await,
        OperationMode::Network => Err("network mode not yet enabled".into()),
    }
}

async fn run_local(config: NodeConfig) -> Result<(), DynError> {
    let port = config.port;
    let ip = config.address;
    let executor = Executor::from_config(config).await?;
    let socket: SocketAddr = (ip, port).into();
    locutus::local_node::run_local_node(executor, socket).await
}

fn main() -> Result<(), DynError> {
    tracing_subscriber::fmt()
        .with_level(true)
        .with_file(true)
        .with_line_number(true)
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();
    let config = NodeConfig::parse();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(run(config))?;
    Ok(())
}
