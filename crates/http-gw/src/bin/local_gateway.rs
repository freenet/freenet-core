use tracing::metadata::LevelFilter;
use tracing_subscriber::util::SubscriberInitExt;

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[cfg(feature = "local")]
const MAX_SIZE: i64 = 10 * 1024 * 1024;
#[cfg(feature = "local")]
const MAX_MEM_CACHE: u32 = 10_000_000;

#[cfg(feature = "local")]
async fn run() -> Result<(), DynError> {
    use locutus_node::SqlitePool;
    use locutus_runtime::{ContractStore, StateStore};
    let tmp_path = std::env::temp_dir().join("locutus");
    let contract_store = ContractStore::new(tmp_path.join("contracts"), MAX_SIZE);
    let state_store = StateStore::new(SqlitePool::new().await?, MAX_MEM_CACHE).unwrap();
    let local_node = locutus_dev::local_node::LocalNode::new(
        contract_store.clone(),
        state_store.clone(),
        || {
            locutus_dev::util::set_cleanup_on_exit().unwrap();
        },
    )
    .await?;
    http_gw::local_node::run_local_node(local_node).await
}

#[allow(unreachable_code)]
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
