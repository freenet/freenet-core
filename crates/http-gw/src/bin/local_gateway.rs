use tracing::metadata::LevelFilter;
use tracing_subscriber::util::SubscriberInitExt;

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

fn main() -> Result<(), DynError> {
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
        rt.block_on(async move {
            let local_node = http_gw::local_node::config_node().await?;
            http_gw::local_node::set_local_node(local_node).await
        })?;
    }
    #[cfg(not(feature = "local"))]
    {
        panic!("only allowed if local feature is enabled");
    }
    Ok(())
}
