use anyhow::Context;
use clap::Parser;
use freenet::{
    config::{Config, ConfigArgs},
    local_node::{Executor, NodeConfig, OperationMode},
    run_local_node, run_network_node,
    server::serve_gateway,
};
use std::sync::Arc;

/// Count threads with "tokio-runtime" in their name by reading /proc/self/task
fn count_tokio_threads() -> usize {
    let Ok(entries) = std::fs::read_dir("/proc/self/task") else {
        return 0;
    };
    entries
        .filter_map(|e| e.ok())
        .filter_map(|entry| {
            let comm_path = entry.path().join("comm");
            std::fs::read_to_string(comm_path).ok()
        })
        .filter(|name| name.contains("tokio-runtime"))
        .count()
}

/// Build metadata embedded at compile time
mod build_info {
    pub const VERSION: &str = env!("CARGO_PKG_VERSION");
    pub const GIT_COMMIT: &str = env!("GIT_COMMIT_HASH");
    pub const GIT_DIRTY: &str = env!("GIT_DIRTY");
    pub const BUILD_TIMESTAMP: &str = env!("BUILD_TIMESTAMP");
}

async fn run(config: Config) -> anyhow::Result<()> {
    // Log build info on startup - critical for correlating logs with code version
    tracing::info!(
        version = build_info::VERSION,
        git_commit = %format!("{}{}", build_info::GIT_COMMIT, build_info::GIT_DIRTY),
        build_timestamp = build_info::BUILD_TIMESTAMP,
        "Freenet node starting"
    );

    match config.mode {
        OperationMode::Local => run_local(config).await,
        OperationMode::Network => run_network(config).await,
    }
}

async fn run_local(config: Config) -> anyhow::Result<()> {
    tracing::info!("Starting freenet node in local mode");
    let socket = config.ws_api;

    let executor = Executor::from_config(Arc::new(config), None)
        .await
        .map_err(anyhow::Error::msg)?;

    run_local_node(executor, socket)
        .await
        .map_err(anyhow::Error::msg)
}

async fn run_network(config: Config) -> anyhow::Result<()> {
    tracing::info!("Starting freenet node in network mode");

    // Thread monitor for diagnosing thread explosion issue
    tokio::spawn(async {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static BASELINE: AtomicUsize = AtomicUsize::new(0);
        let expected = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1);
        let initial = count_tokio_threads();
        BASELINE.store(initial, Ordering::SeqCst);
        tracing::info!(target: "freenet::diagnostics::thread_explosion", initial, expected, "Thread monitor started");
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            let current = count_tokio_threads();
            let baseline = BASELINE.load(Ordering::SeqCst);
            if current > baseline + 10 {
                tracing::error!(target: "freenet::diagnostics::thread_explosion", current, baseline, "THREAD EXPLOSION");
            } else if current > baseline {
                tracing::warn!(target: "freenet::diagnostics::thread_explosion", current, baseline, "Thread count increased");
            }
        }
    });

    let clients = serve_gateway(config.ws_api)
        .await
        .with_context(|| "failed to start HTTP/WebSocket gateway")?;
    tracing::info!("Initializing node configuration");

    let node_config = NodeConfig::new(config)
        .await
        .with_context(|| "failed while loading node config")?;

    let node = node_config
        .build(clients)
        .await
        .with_context(|| "failed while building the node")?;

    run_network_node(node).await
}

fn main() -> anyhow::Result<()> {
    freenet::config::set_logger(None, None);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(
            std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1),
        )
        .enable_all()
        .build()
        .unwrap();
    let config = ConfigArgs::parse();
    rt.block_on(async move {
        if config.version {
            println!("Freenet version: {}", config.current_version());
            return Ok(());
        }
        let config = config.build().await?;
        run(config).await
    })?;
    Ok(())
}
