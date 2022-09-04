use crate::config::LocalNodeCliConfig;

mod commands;
mod state;
mod user_events;

pub async fn run_local_node_client(
    config: LocalNodeCliConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    if config.disable_tui_mode {
        return Err("TUI mode not yet implemented".into());
    }

    if config.clean_exit {
        locutus_core::util::set_cleanup_on_exit()?;
    }

    let app_state = state::AppState::new(&config).await?;
    let (sender, receiver) = tokio::sync::mpsc::channel(100);
    let runtime = tokio::task::spawn(commands::wasm_runtime(
        config.clone(),
        receiver,
        app_state.clone(),
    ));
    let user_fn = user_events::user_fn_handler(config, sender, app_state);
    tokio::select! {
        res = runtime => { res?? }
        res = user_fn => { res? }
    };
    println!("Shutdown...");
    Ok(())
}
