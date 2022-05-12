use clap::Parser;

use locutus_dev::{user_fn_handler, wasm_runtime, AppState, Config};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let cli = Config::parse();
    if cli.disable_tui_mode {
        return Err("CLI mode not yet implemented".into());
    }
    let app_state = AppState::new(&cli)?;
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
