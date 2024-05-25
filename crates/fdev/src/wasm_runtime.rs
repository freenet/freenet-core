use std::{
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
};

use clap::ArgGroup;
use freenet::{config::ConfigPathsArgs, dev_tool::OperationMode};

mod commands;
mod state;
mod user_events;

const DEFAULT_MAX_CONTRACT_SIZE: i64 = 50 * 1024 * 1024;

pub async fn run_local_executor(config: ExecutorConfig) -> Result<(), anyhow::Error> {
    if config.disable_tui_mode {
        anyhow::bail!("TUI mode not yet implemented");
    }

    if config.clean_exit {
        // FIXME: potentially not cleaning up the correct directory
        freenet::util::set_cleanup_on_exit(None)?;
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
}

/// An interactive runtime for WASM for testing contracts and delegates development.
#[derive(clap::Parser, Clone)]
#[clap(name = "Freenet Local Development Node Environment")]
#[clap(author = "The Freenet Project Inc.")]
#[clap(group(
    ArgGroup::new("output")
        .required(true)
        .args(&["output-file", "terminal-output"])
))]
pub struct ExecutorConfig {
    /// Cleanups all state which was created locally during execution
    #[clap(long, requires = "fmt")]
    pub(crate) clean_exit: bool,
    /// Path to the contract to be loaded.
    #[clap(flatten)]
    pub(crate) paths: ConfigPathsArgs,
    /// The ip address of freenet node to update the contract to. If the node is running in local mode,
    /// The default value is `127.0.0.1`
    #[arg(short, long, default_value_t = IpAddr::V4(Ipv4Addr::LOCALHOST))]
    pub(crate) address: IpAddr,
    /// The port of the running local freenet node.
    #[arg(short, long, default_value = "50509")]
    pub(crate) port: u16,
    /// Node operation mode.
    #[clap(value_enum, default_value_t = OperationMode::Local, env = "MODE")]
    pub(crate) mode: OperationMode,
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
    #[clap(long, env = "FREENET_MAX_CONTRACT_SIZE", default_value_t = DEFAULT_MAX_CONTRACT_SIZE)]
    pub(crate) max_contract_size: i64,
}
