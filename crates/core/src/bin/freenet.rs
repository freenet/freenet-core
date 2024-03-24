use clap::Parser;
use freenet::local_node::{Executor, OperationMode, PeerCliConfig};
use std::net::SocketAddr;

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

async fn run(config: PeerCliConfig) -> Result<(), DynError> {
    match config.mode {
        OperationMode::Local => run_local(config).await,
        OperationMode::Network => Err("network mode not yet enabled".into()),
    }
}

async fn run_local(config: PeerCliConfig) -> Result<(), DynError> {
    let port = config.port;
    let ip = config.address;
    freenet::config::Config::set_op_mode(OperationMode::Local);
    let executor = Executor::from_config(config, None).await?;
    let socket: SocketAddr = (ip, port).into();
    freenet::server::local_node::run_local_node(executor, socket).await
}

fn main() -> Result<(), DynError> {
    freenet::config::set_logger(None);
    let config = PeerCliConfig::parse();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(run(config))?;
    Ok(())
}
