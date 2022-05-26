//! Serves a new contract so is available for browsing.

use std::{fs::File, io::Read, path::PathBuf, sync::Arc};

use locutus_dev::ContractStore;
use locutus_node::{either::Either, SqlitePool, WrappedState};
use locutus_runtime::{ContractCode, StateStore, WrappedContract};

const MAX_SIZE: i64 = 10 * 1024 * 1024;
const MAX_MEM_CACHE: u32 = 10_000_000;

pub fn test_state() -> Result<WrappedState, std::io::Error> {
    const CRATE_DIR: &str = env!("CARGO_MANIFEST_DIR");
    let path = PathBuf::from(CRATE_DIR).join("examples/encoded_state");
    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;
    Ok(WrappedState::new(bytes))
}

pub fn test_contract() -> Result<WrappedContract<'static>, std::io::Error> {
    const CRATE_DIR: &str = env!("CARGO_MANIFEST_DIR");
    let path = PathBuf::from(CRATE_DIR).join("examples/test_web_contract.wasm");
    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;
    Ok(WrappedContract::new(
        Arc::new(ContractCode::from(bytes)),
        [].as_ref().into(),
    ))
}

#[cfg(feature = "local")]
async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let state = test_state()?;
    let contract = test_contract()?;
    log::info!("loaded contract {} in local node", contract.key().encode());
    let tmp_path = std::env::temp_dir().join("locutus");
    let contract_store = ContractStore::new(tmp_path.join("contracts"), MAX_SIZE);
    let state_store = StateStore::new(SqlitePool::new().await?, MAX_MEM_CACHE).unwrap();
    let mut local_node =
        locutus_dev::LocalNode::new(contract_store.clone(), state_store.clone()).await?;
    if let Err(err) = local_node
        .handle_request(locutus_node::ClientRequest::Put { contract, state })
        .await
    {
        match err {
            Either::Left(err) => log::error!("req error: {err}"),
            Either::Right(err) => log::error!("other error: {err}"),
        }
    }
    http_gw::local_node::set_local_node(local_node, contract_store, state_store).await
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    #[cfg(not(feature = "local"))]
    {
        panic!("only allowed if local feature is enabled");
    }
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    // env_logger::Builder::from_default_env()
    //     .format_module_path(true)
    //     .filter_level(log::LevelFilter::Info)
    //     .init();

    #[allow(unused_variables)]
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    #[cfg(feature = "local")]
    {
        rt.block_on(run())?;
    }

    Ok(())
}
