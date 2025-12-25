use anyhow::Context;
use clap::{Parser, Subcommand};
use freenet::{
    config::{Config, ConfigArgs},
    local_node::{Executor, NodeConfig, OperationMode},
    run_local_node, run_network_node,
    server::serve_gateway,
};
use std::sync::Arc;

mod commands;
use commands::{service::ServiceCommand, update::UpdateCommand};

/// Freenet - A distributed, decentralized, and censorship-resistant platform
#[derive(Parser, Debug)]
#[command(name = "freenet")]
#[command(about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    #[command(flatten)]
    config: ConfigArgs,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the node in network mode (default if no subcommand specified)
    Network {
        #[command(flatten)]
        config: ConfigArgs,
    },
    /// Run the node in local mode
    Local {
        #[command(flatten)]
        config: ConfigArgs,
    },
    /// Manage the Freenet system service
    #[command(subcommand)]
    Service(ServiceCommand),
    /// Update Freenet to the latest version
    Update(UpdateCommand),
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

fn run_node(config_args: ConfigArgs) -> anyhow::Result<()> {
    freenet::config::set_logger(None, None);

    if config_args.version {
        println!(
            "Freenet version: {} ({}{})",
            config_args.current_version(),
            build_info::GIT_COMMIT,
            build_info::GIT_DIRTY
        );
        println!("Build timestamp: {}", build_info::BUILD_TIMESTAMP);
        return Ok(());
    }

    // Calculate blocking threads: use CLI arg, or default (2x CPU cores, clamped to 4-32)
    let max_blocking_threads = config_args.max_blocking_threads.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| (n.get() * 2).clamp(4, 32))
            .unwrap_or(8)
    });

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(
            std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1),
        )
        .max_blocking_threads(max_blocking_threads)
        // Name threads to distinguish main runtime from any rogue runtimes
        // Rogue runtimes would use default "tokio-runtime-w" name
        .thread_name("freenet-main")
        .enable_all()
        .build()
        .unwrap();

    tracing::info!(
        max_blocking_threads,
        "Tokio runtime configured with bounded blocking thread pool"
    );

    rt.block_on(async move {
        let config = config_args.build().await?;
        run(config).await
    })?;
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Some(Command::Service(cmd)) => cmd.run(),
        Some(Command::Update(cmd)) => cmd.run(build_info::VERSION),
        Some(Command::Network { mut config }) => {
            config.mode = Some(OperationMode::Network);
            run_node(config)
        }
        Some(Command::Local { mut config }) => {
            config.mode = Some(OperationMode::Local);
            run_node(config)
        }
        None => {
            // Default behavior: run with the config from top-level args
            run_node(cli.config)
        }
    }
}
