use locutus_node::SqlitePool;
use locutus_runtime::StateStore;
use tracing::metadata::LevelFilter;
use tracing_subscriber::util::SubscriberInitExt;

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

const MAX_SIZE: i64 = 10 * 1024 * 1024;
const MAX_MEM_CACHE: u32 = 10_000_000;

#[cfg(feature = "local")]
async fn run() -> Result<(), DynError> {
    use locutus_dev::ContractStore;
    let tmp_path = std::env::temp_dir().join("locutus");
    let contract_store = ContractStore::new(tmp_path.join("contracts"), MAX_SIZE);
    let state_store = StateStore::new(SqlitePool::new().await?, MAX_MEM_CACHE).unwrap();
    let local_node =
        locutus_dev::LocalNode::new(contract_store.clone(), state_store.clone()).await?;
    http_gw::local_node::set_local_node(local_node, contract_store, state_store).await
}

fn main() -> Result<(), DynError> {
    #[cfg(not(feature = "local"))]
    {
        panic!("only allowed if local feature is enabled");
    }

    let sub = tracing_subscriber::fmt()
        .with_max_level(LevelFilter::DEBUG)
        .with_level(true)
        .finish();
    sub.init();

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
