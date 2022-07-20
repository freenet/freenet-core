use clap::Parser;
use tracing_subscriber::EnvFilter;

use locutus_dev::set_cleanup_on_exit;
use locutus_dev::{user_fn_handler, wasm_runtime, AppState, Config};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Config::parse();
    if cli.disable_tui_mode {
        return Err("CLI mode not yet implemented".into());
    }

    if cli.clean_exit {
        set_cleanup_on_exit();
    }

    let app_state = AppState::new(&cli).await?;
    let (sender, receiver) = tokio::sync::mpsc::channel(100);
    let runtime = tokio::task::spawn(wasm_runtime(cli.clone(), receiver, app_state.clone()));
    let user_fn = user_fn_handler(cli, sender, app_state);
    tokio::select! {
        res = runtime => { res?? }
        res = user_fn => { res? }
    };
    println!("Shutdown...");
    Ok(())
}
