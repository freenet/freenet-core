use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use reqwest::Client as HttpClient;
use tokio::sync::Mutex;

use freenet_release_agent::{
    config::Config,
    server::{AppState, build_router, serve},
    updater::Updater,
};

#[derive(Parser)]
#[command(version, about = "HTTP agent that triggers gateway updates on release")]
struct Cli {
    #[arg(
        short,
        long,
        env = "FREENET_RELEASE_AGENT_CONFIG",
        default_value = "/etc/freenet-release-agent/config.toml"
    )]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,freenet_release_agent=info".into()),
        )
        .init();

    let cli = Cli::parse();
    let config = Config::from_path(&cli.config)?;
    let secret = config.load_secret()?;

    let http = HttpClient::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("build reqwest client")?;

    let updater = Updater {
        command: config.update_command.clone(),
        dry_run: config.dry_run,
    };

    let listen_addr = config.listen_addr;
    let state = AppState {
        config: Arc::new(config),
        secret: Arc::new(secret),
        http,
        updater,
        last_update_attempt: Arc::new(Mutex::new(None)),
    };

    serve(listen_addr, build_router(state)).await
}
