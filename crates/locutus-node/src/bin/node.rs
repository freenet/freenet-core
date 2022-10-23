use std::path::PathBuf;

use clap::Parser;
use locutus_core::{
    locutus_runtime::{ContractStore, StateStore},
    Config, ContractExecutor, SqlitePool,
};
use locutus_dev::config::OperationMode;
use tracing::metadata::LevelFilter;
use tracing_subscriber::EnvFilter;

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

const MAX_SIZE: i64 = 10 * 1024 * 1024;
const MAX_MEM_CACHE: u32 = 10_000_000;

async fn run(config: NodeConfig) -> Result<(), DynError> {
    match config.mode {
        OperationMode::Local => run_local(config).await,
        OperationMode::Network => Err("network mode not yet enabled".into()),
    }
}

async fn run_local(config: NodeConfig) -> Result<(), DynError> {
    let contract_dir = config
        .contract_data_dir
        .unwrap_or_else(|| Config::get_conf().config_paths.local_contracts_dir());
    let contract_store = ContractStore::new(contract_dir, MAX_SIZE)?;
    let state_store = StateStore::new(SqlitePool::new().await?, MAX_MEM_CACHE).unwrap();
    let executor = ContractExecutor::new(contract_store, state_store, || {
        locutus_core::util::set_cleanup_on_exit().unwrap();
    })
    .await?;
    locutus::local_node::run_local_node(executor).await
}

fn main() -> Result<(), DynError> {
    tracing_subscriber::fmt()
        .with_level(true)
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

#[derive(clap::Parser, Clone)]
struct NodeConfig {
    /// Node operation mode.
    #[clap(value_enum, default_value_t=OperationMode::Local)]
    mode: OperationMode,
    /// Overrides the default data directory where Locutus contract files are stored.
    contract_data_dir: Option<PathBuf>,
}
