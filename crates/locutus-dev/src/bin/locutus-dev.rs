use clap::Parser;
use tracing_subscriber::EnvFilter;

use locutus_dev::{
    package_manager::package_state, set_cleanup_on_exit, user_fn_handler, wasm_runtime, AppState,
    Config, LocalNodeConfig, SubCommand,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    match Config::parse().sub_command {
        SubCommand::RunLocal(local_node_config) => run_local_node_client(local_node_config).await,
        SubCommand::Build(package_manager_config) => package_state(package_manager_config),
    }
}

async fn run_local_node_client(
    config: LocalNodeConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    if config.disable_tui_mode {
        return Err("TUI mode not yet implemented".into());
    }

    if config.clean_exit {
        set_cleanup_on_exit()?;
    }

    let app_state = AppState::new(&config).await?;
    let (sender, receiver) = tokio::sync::mpsc::channel(100);
    let runtime = tokio::task::spawn(wasm_runtime(config.clone(), receiver, app_state.clone()));
    let user_fn = user_fn_handler(config, sender, app_state);
    tokio::select! {
        res = runtime => { res?? }
        res = user_fn => { res? }
    };
    println!("Shutdown...");
    Ok(())
}
