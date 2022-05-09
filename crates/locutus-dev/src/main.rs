use std::path::PathBuf;

use clap::{ArgGroup, Parser};
use locutus_node::ClientRequest;

use crate::state::AppState;

mod executor;
mod state;
mod user_events;

type CommandReceiver = tokio::sync::mpsc::Receiver<ClientRequest>;
type CommandSender = tokio::sync::mpsc::Sender<ClientRequest>;
type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

const DEFAULT_MAX_CONTRACT_SIZE: i64 = 50 * 1024 * 1024;

#[derive(clap::ArgEnum, Clone, Copy, Debug)]
enum DeserializationFmt {
    #[cfg(feature = "json")]
    Json,
    #[cfg(feature = "messagepack")]
    MessagePack,
}

/// A CLI utility for testing out contracts against a Locutus local node.
#[derive(clap::Parser, Clone)]
#[clap(name = "Locutus Contract Development Environment")]
#[clap(author = "The Freenet Project Inc.")]
#[clap(version = "0.0.1")]
#[clap(group(
    ArgGroup::new("output")
        .required(true)
        .args(&["output-file", "terminal-output"])
))]
struct Cli {
    /// Path to the contract to be loaded
    #[clap(parse(from_os_str))]
    contract: PathBuf,
    /// Path to the input file to read from on command
    #[clap(short, long, parse(from_os_str), value_name = "INPUT_FILE")]
    input_file: PathBuf,
    /// Deserialization format, requires feature flags enabled.
    #[clap(short, long, arg_enum, group = "fmt", value_name = "FORMAT")]
    deser_format: Option<DeserializationFmt>,
    /// Disable TUI mode (run only though CLI commands)
    #[clap(long)]
    disable_tui_mode: bool,
    /// Path to output file
    #[clap(short, long, parse(from_os_str), value_name = "OUTPUT_FILE")]
    output_file: Option<PathBuf>,
    /// Terminal output
    #[clap(long, requires = "fmt")]
    terminal_output: bool,
    /// Max contract size
    #[clap(long, env = "LOCUTUS_MAX_CONTRACT_SIZE", default_value_t = DEFAULT_MAX_CONTRACT_SIZE)]
    max_contract_size: i64,
}

#[tokio::main]
async fn main() -> Result<(), DynError> {
    let cli = Cli::parse();
    if cli.disable_tui_mode {
        return Err("CLI mode not yet implemented".into());
    }
    let app_state = AppState::new(&cli)?;

    let (sender, receiver) = tokio::sync::mpsc::channel(100);
    let runtime = tokio::task::spawn(executor::wasm_runtime(
        cli.clone(),
        receiver,
        app_state.clone(),
    ));
    let user_fn = user_events::user_fn_handler(cli, sender, app_state);
    tokio::select! {
        res = runtime => { res?? }
        res = user_fn => { res? }
    };
    println!("Shutdown...");
    Ok(())
}
