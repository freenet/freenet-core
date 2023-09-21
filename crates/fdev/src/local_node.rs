use std::path::PathBuf;

use clap::ArgGroup;

mod commands;
mod state;
mod user_events;

const DEFAULT_MAX_CONTRACT_SIZE: i64 = 50 * 1024 * 1024;

pub async fn run_local_node_client(
    config: LocalNodeCliConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    if config.disable_tui_mode {
        return Err("TUI mode not yet implemented".into());
    }

    if config.clean_exit {
        freenet::util::set_cleanup_on_exit()?;
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

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
pub enum DeserializationFmt {
    Json,
    #[cfg(feature = "messagepack")]
    MessagePack,
}

/// A CLI utility for testing out contracts against a Locutus local node.
#[derive(clap::Parser, Clone)]
#[clap(name = "Locutus Local Development Node Environment")]
#[clap(author = "The Freenet Project Inc.")]
#[clap(group(
    ArgGroup::new("output")
        .required(true)
        .args(&["output-file", "terminal-output"])
))]
pub struct LocalNodeCliConfig {
    /// Cleanups all state which was created locally during execution
    #[clap(long, requires = "fmt")]
    pub(crate) clean_exit: bool,
    /// Path to the contract to be loaded.
    #[clap(value_parser)]
    pub(crate) contract: PathBuf,
    /// Path to the file containing the parameters for this contract. If not set the default parameters will be empty.
    #[clap(long = "parameters", value_parser)]
    pub(crate) params: Option<PathBuf>,
    /// Path to the input file to read from on command.
    #[clap(short, long, value_parser, value_name = "INPUT_FILE")]
    pub(crate) input_file: PathBuf,
    /// Deserialization format, requires feature flags enabled.
    #[arg(
        short,
        long = "deserialization-format",
        value_enum,
        group = "fmt",
        value_name = "FORMAT"
    )]
    pub(crate) ser_format: Option<DeserializationFmt>,
    /// Disable TUI mode (run only though CLI commands)
    #[clap(long)]
    pub(crate) disable_tui_mode: bool,
    /// Path to output file
    #[clap(short, long, value_parser, value_name = "OUTPUT_FILE")]
    pub(crate) output_file: Option<PathBuf>,
    /// Terminal output
    #[clap(long, requires = "fmt")]
    pub(crate) terminal_output: bool,
    /// Max contract size
    #[clap(long, env = "LOCUTUS_MAX_CONTRACT_SIZE", default_value_t = DEFAULT_MAX_CONTRACT_SIZE)]
    pub(crate) max_contract_size: i64,
}
