use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use reqwest::Client as HttpClient;
use tokio::sync::Mutex;

use freenet_release_agent::{
    announcer::Announcer,
    config::Config,
    github::GitHubLatest,
    server::{AppState, build_router, serve},
    updater::Updater,
    version::{ServiceHealthCache, VersionCache},
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

    let updater = Updater::new_with_sudo(config.update_command.clone(), config.dry_run);
    let announcer = Announcer::new_with_sudo(
        config.river_announce_command.clone(),
        config.dry_run,
        config.river_announce_user.clone(),
    );

    let listen_addr = config.listen_addr;
    let latest_source = Arc::new(GitHubLatest {
        client: http,
        repo: config.github_repo.clone(),
    });
    let state = AppState {
        config: Arc::new(config),
        secret: Arc::new(secret),
        latest_source,
        updater,
        announcer,
        version_cache: VersionCache::new(),
        service_health_cache: ServiceHealthCache::new(),
        systemctl_path: PathBuf::from("systemctl"),
        last_update_attempt: Arc::new(Mutex::new(None)),
        last_announce_attempt: Arc::new(Mutex::new(None)),
        update_in_flight: Arc::new(std::sync::atomic::AtomicBool::new(false)),
    };

    serve(listen_addr, build_router(state)).await
}
