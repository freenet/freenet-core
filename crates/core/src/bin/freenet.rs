use anyhow::Context;
use clap::Parser;
use freenet::{
    config::{Config, ConfigArgs},
    local_node::{Executor, NodeConfig, OperationMode},
    run_local_node, run_network_node,
    server::serve_gateway,
};
use std::collections::HashSet;
use std::sync::Arc;

/// Thread info for diagnostics
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ThreadInfo {
    tid: u64,
    name: String,
}

/// Get all tokio runtime threads with their TIDs and names
/// Looks for both "freenet-main" (our main runtime) and "tokio-runtime" (rogue runtimes)
fn get_tokio_threads() -> Vec<ThreadInfo> {
    let Ok(entries) = std::fs::read_dir("/proc/self/task") else {
        return Vec::new();
    };
    entries
        .filter_map(|e| e.ok())
        .filter_map(|entry| {
            let tid: u64 = entry.file_name().to_str()?.parse().ok()?;
            let comm_path = entry.path().join("comm");
            let name = std::fs::read_to_string(comm_path).ok()?.trim().to_string();
            // Match our main runtime threads OR any rogue tokio runtime threads
            if name.contains("freenet-main") || name.contains("tokio-runtime") {
                Some(ThreadInfo { tid, name })
            } else {
                None
            }
        })
        .collect()
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

    let executor = Executor::from_config_local(Arc::new(config))
        .await
        .map_err(anyhow::Error::msg)?;

    run_local_node(executor, socket)
        .await
        .map_err(anyhow::Error::msg)
}

async fn run_network(config: Config) -> anyhow::Result<()> {
    tracing::info!("Starting freenet node in network mode");

    // Thread monitor for diagnosing thread explosion issue
    // Enhanced to track individual thread TIDs and detect exactly when new threads appear
    tokio::spawn(async {
        let expected = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1);

        // Track known thread TIDs
        let initial_threads = get_tokio_threads();
        let mut known_tids: HashSet<u64> = initial_threads.iter().map(|t| t.tid).collect();
        let baseline = initial_threads.len();

        // Log initial thread TIDs for reference
        let initial_tid_range = if !initial_threads.is_empty() {
            let min_tid = initial_threads.iter().map(|t| t.tid).min().unwrap();
            let max_tid = initial_threads.iter().map(|t| t.tid).max().unwrap();
            format!("{}-{}", min_tid, max_tid)
        } else {
            "none".to_string()
        };

        tracing::info!(
            target: "freenet::diagnostics::thread_explosion",
            initial = baseline,
            expected,
            tid_range = %initial_tid_range,
            "Thread monitor started (checking every 10s)"
        );

        // Check every 10 seconds for faster detection
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));

        loop {
            interval.tick().await;

            let current_threads = get_tokio_threads();
            let current_count = current_threads.len();
            let current_tids: HashSet<u64> = current_threads.iter().map(|t| t.tid).collect();

            // Find newly created threads
            let new_tids: Vec<u64> = current_tids.difference(&known_tids).copied().collect();

            if !new_tids.is_empty() {
                // New threads detected! Log detailed info
                let new_threads: Vec<_> = current_threads
                    .iter()
                    .filter(|t| new_tids.contains(&t.tid))
                    .collect();

                // Check if this looks like a new runtime (batch of consecutive TIDs)
                let mut sorted_new_tids = new_tids.clone();
                sorted_new_tids.sort();
                let is_batch = sorted_new_tids.len() > 1 && {
                    let first = sorted_new_tids[0];
                    let last = sorted_new_tids[sorted_new_tids.len() - 1];
                    // Consecutive or near-consecutive TIDs suggest batch creation
                    (last - first) < (sorted_new_tids.len() as u64 * 2)
                };

                let new_tid_range = if !sorted_new_tids.is_empty() {
                    format!(
                        "{}-{}",
                        sorted_new_tids[0],
                        sorted_new_tids[sorted_new_tids.len() - 1]
                    )
                } else {
                    "none".to_string()
                };

                // Get thread names (should all be tokio-runtime-w or similar)
                let thread_names: Vec<_> = new_threads.iter().map(|t| t.name.as_str()).collect();
                let unique_names: HashSet<_> = thread_names.iter().copied().collect();

                if current_count > baseline + 10 {
                    tracing::error!(
                        target: "freenet::diagnostics::thread_explosion",
                        current = current_count,
                        baseline,
                        new_thread_count = new_tids.len(),
                        new_tid_range = %new_tid_range,
                        is_batch_creation = is_batch,
                        thread_names = ?unique_names,
                        "THREAD EXPLOSION - new runtime likely created"
                    );
                } else {
                    tracing::warn!(
                        target: "freenet::diagnostics::thread_explosion",
                        current = current_count,
                        baseline,
                        new_thread_count = new_tids.len(),
                        new_tid_range = %new_tid_range,
                        is_batch_creation = is_batch,
                        thread_names = ?unique_names,
                        "New tokio threads detected"
                    );
                }

                // Update known TIDs
                known_tids.extend(new_tids);
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
        // Name threads to distinguish main runtime from any rogue runtimes
        // Rogue runtimes would use default "tokio-runtime-w" name
        .thread_name("freenet-main")
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
